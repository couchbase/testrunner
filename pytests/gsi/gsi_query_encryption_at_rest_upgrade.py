"""
gsi_query_encryption_at_rest_upgrade.py: Combined GSI + Query encryption-at-rest upgrade tests.

Tests validate that:
  * GSI on-disk files (shards, snapshots, etc.) are encrypted post-upgrade.
  * Query completed-requests (rlstream.*), history (local_request_log.*), FFDC
    (query_ffdc_MAN_*) and spill/backfill files are encrypted post-upgrade.
  * Log encryption at rest is enabled in addition to bucket encryption.
  * Indexes survive the upgrade.
  * Mixed-mode behaviour is correct: when the cluster has bucket encryption on,
    nodes whose running version is >= EAR_FILE_MIN_VERSION encrypt GSI/Query files
    on disk; nodes still on an older version do not.

Version semantics:
  * < 8.0   : no encryption-at-rest support at all.
  * 8.0.x   : bucket encryption (KEK + per-bucket toggle) supported, BUT
              GSI / Query file-level encryption is NOT implemented.
  * >= 8.1  : full GSI + Query file-level encryption.

Two test variants differ only in upgrade mechanic:
  * test_offline_upgrade_with_encryption_at_rest:
      per-node stop_server -> install upgrade build -> start
  * test_online_upgrade_with_encryption_at_rest:
      per-node rebalance-out -> install upgrade build -> rebalance-in
"""

import concurrent.futures
import itertools
import json
import threading
import time

from couchbase_helper.query_definitions import QueryDefinition
from .upgrade_gsi import UpgradeSecondaryIndex
from pytests.tuqquery.n1ql_encryption_helpers import N1QLEncryptionHelpers
from membase.api.rest_client import RestConnection
from lib.membase.helper.encryption_at_rest_helper import EncryptionUtil
from remote.remote_util import RemoteMachineShellConnection


class GSIQueryEncryptionAtRestUpgrade(UpgradeSecondaryIndex):
    """GSI + Query encryption-at-rest upgrade tests with mixed-mode validation."""

    EAR_FILE_MIN_VERSION = (8, 1)       # GSI / Query files encrypt at >= 8.1
    BUCKET_EAR_MIN_VERSION = (8, 0)     # bucket-level encryption toggle works at >= 8.0

    # ------------------------------------------------------------------ setUp / tearDown

    def setUp(self):
        super(GSIQueryEncryptionAtRestUpgrade, self).setUp()
        self.encryption_helper = N1QLEncryptionHelpers(self.log)
        self.log.info("==============  GSIQueryEncryptionAtRestUpgrade setUp ==============")
        self.test_bucket = self.input.param("bucket_name", "test_bucket")
        # Drop any auto-created buckets so we own bucket lifecycle in the test.
        try:
            self.rest.delete_all_buckets()
            self.sleep(10, "Wait after delete_all_buckets")
        except Exception as e:
            self.log.warning(f"delete_all_buckets failed (continuing): {e}")

        self.initial_supports_bucket_ear = (
            self._version_tuple(self.initial_version) >= self.BUCKET_EAR_MIN_VERSION
        )
        self.initial_supports_file_ear = (
            self._version_tuple(self.initial_version) >= self.EAR_FILE_MIN_VERSION
        )
        self.target_supports_file_ear = (
            self._version_tuple(self.upgrade_to) >= self.EAR_FILE_MIN_VERSION
        )
        self.log.info(
            f"[setUp] initial={self.initial_version} "
            f"(bucket_ear={self.initial_supports_bucket_ear}, file_ear={self.initial_supports_file_ear}) "
            f"-> upgrade_to={self.upgrade_to} (file_ear={self.target_supports_file_ear})"
        )
        self.encryption_key_id = None
        self._install_tools()

    def _install_tools(self):
        """
        Ensure xxd is installed on every cluster node.

        xxd ships in vim-common on Debian/Ubuntu and on RHEL/CentOS/Fedora.
        The check is skipped for nodes where xxd is already present so
        repeated suite runs are fast.
        """
        for node in self.servers:
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                info = shell.extract_remote_info()
                distro = (info.distribution_type or "").lower()

                out, _ = shell.execute_command("which xxd 2>/dev/null")
                if out and out[0].strip():
                    self.log.info(f"xxd already present on {node.ip}: {out[0].strip()}")
                    continue

                self.log.info(f"Installing xxd on {node.ip} (distro={distro})")
                if "ubuntu" in distro or "debian" in distro:
                    install_out, _ = shell.execute_command(
                        "apt-get install -y xxd 2>&1"
                    )
                elif any(d in distro for d in ("centos", "rhel", "fedora", "amazon")):
                    install_out, _ = shell.execute_command(
                        "yum install -y vim-common 2>&1 || dnf install -y vim-common 2>&1"
                    )
                else:
                    install_out, _ = shell.execute_command(
                        "apt-get install -y xxd 2>&1 || yum install -y vim-common 2>&1"
                    )
                self.log.info(f"Install output on {node.ip}: {' '.join(install_out or [])}")

                verify_out, _ = shell.execute_command("which xxd 2>/dev/null")
                if verify_out and verify_out[0].strip():
                    self.log.info(f"xxd installed successfully on {node.ip}: {verify_out[0].strip()}")
                else:
                    self.log.warning(f"xxd installation may have failed on {node.ip}")
            finally:
                shell.disconnect()

    def tearDown(self):
        # Best-effort restore of query admin settings so subsequent tests start clean.
        try:
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )
            for node in query_nodes:
                try:
                    self.rest.set_completed_requests_collection_duration(node, 1000)
                    self.rest.set_completed_stream_size(node, 0)
                except Exception:
                    pass
        except Exception:
            pass
        super(GSIQueryEncryptionAtRestUpgrade, self).tearDown()

    # ------------------------------------------------------------------ version helpers

    def _enable_other_encryption_at_rest(self):
        """Enable 'other' encryption at rest (covers query spill / backfill files).
        This is a new 8.1 feature and must be called after the cluster is fully
        upgraded before spill-file encryption can be validated."""
        if getattr(self, "other_encryption_at_rest_id", None):
            self.log.info(
                f"[setUp] other encryption at rest already enabled with key "
                f"{self.other_encryption_at_rest_id}"
            )
            return
        self.log.info("[setUp] Enabling other encryption at rest")
        result = self.encryption_util.setup_encryption_at_rest(
            cluster_master=self.master,
            bypass_encryption_func=self.bypass_encryption_setting,
            enable_other_encryption_at_rest=True,
            KMIP_for_other_encryption=self.KMIP_for_other_encryption,
            secret_rotation_interval=self.secret_rotation_interval,
            other_dekLifetime=self.other_dekLifetime,
            other_dekRotationInterval=self.other_dekRotationInterval,
        )
        self.encryption_util.set_encryption_ids(self, result)
        self.assertTrue(
            getattr(self, "other_encryption_at_rest_id", None),
            "Failed to enable other encryption at rest",
        )
        self.log.info(
            f"[setUp] other encryption at rest enabled with key "
            f"{self.other_encryption_at_rest_id}"
        )

    def _enable_log_encryption_at_rest(self):
        """Enable log encryption at rest for the cluster."""
        if getattr(self, "log_encryption_at_rest_id", None):
            self.log.info(
                f"[setUp] log encryption at rest already enabled with key "
                f"{self.log_encryption_at_rest_id}"
            )
            return
        self.log.info("[setUp] Enabling log encryption at rest")
        result = self.encryption_util.setup_encryption_at_rest(
            cluster_master=self.master,
            bypass_encryption_func=self.bypass_encryption_setting,
            enable_log_encryption_at_rest=True,
            secret_rotation_interval=self.secret_rotation_interval,
            log_dekLifetime=self.log_dekLifetime,
            log_dekRotationInterval=self.log_dekRotationInterval,
        )
        self.encryption_util.set_encryption_ids(self, result)
        self.assertTrue(
            getattr(self, "log_encryption_at_rest_id", None),
            "Failed to enable log encryption at rest",
        )
        self.log.info(
            f"[setUp] log encryption at rest enabled with key "
            f"{self.log_encryption_at_rest_id}"
        )

    @staticmethod
    def _version_tuple(version_str):
        """'8.0.0-3777' -> (8, 0, 0). Build suffix ignored. Missing parts -> 0."""
        if not version_str:
            return (0, 0, 0)
        base = str(version_str).split('-')[0]
        parts = base.split('.')
        out = []
        for p in parts[:3]:
            try:
                out.append(int(p))
            except (TypeError, ValueError):
                out.append(0)
        while len(out) < 3:
            out.append(0)
        return tuple(out)

    def _node_running_version(self, node):
        try:
            return self._version_tuple(RestConnection(node).get_complete_version())
        except Exception as e:
            self.log.warning(f"_node_running_version failed for {node.ip}: {e}")
            return (0, 0, 0)

    # ------------------------------------------------------------------ KEK / bucket

    def _create_kek(self):
        """Create a server-managed AES-256 KEK with bucket-encryption usage. Returns id."""
        params = EncryptionUtil.create_secret_params(
            secret_type="cb-server-managed-aes-key-256",
            name="gsi-query-upgrade-kek",
            usage=["bucket-encryption"],
        )
        self.log.info(f"Creating KEK: {params}")
        status, response = self.rest.create_secret(params)
        if not status:
            raise Exception(f"create_secret failed: {response}")
        if isinstance(response, (bytes, bytearray)):
            response = json.loads(response)
        key_id = response.get("id") if isinstance(response, dict) else None
        if key_id is None:
            raise Exception(f"create_secret returned no id: {response}")
        self.log.info(f"Created KEK id={key_id}")
        self.encryption_key_id = key_id
        return key_id

    def _enable_bucket_encryption(self, bucket, key_id):
        status, response = self.rest.enable_bucket_encryption(bucket, key_id)
        if not status:
            raise Exception(f"enable_bucket_encryption failed: {response}")
        self.log.info(f"Bucket '{bucket}' encryption enabled with key id {key_id}")
        self.sleep(5, "Wait for bucket encryption setting to apply")

    # ------------------------------------------------------------------ cluster prep

    def _prepare_cluster(self, enable_bucket_encryption_now, enable_log_encryption_now):
        """
        Create the test bucket, optionally enable encryption, load 10k Hotel docs into
        the default scope/collection AND 10k into a non-default scope/collection,
        create Plasma indexes on both, wait online, run a baseline SELECT sweep.

        Returns (select_queries_list, namespaces_list).
        """
        self.log.info(f"[prepare] Creating bucket '{self.test_bucket}'")
        bucket_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size, replicas=self.num_replicas,
            bucket_type=self.bucket_type, enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
        )
        self.cluster.create_standard_bucket(
            name=self.test_bucket, port=11222, bucket_params=bucket_params,
        )
        self.sleep(10, "Wait after bucket creation")

        if enable_bucket_encryption_now:
            self._create_kek()
            self._enable_bucket_encryption(self.test_bucket, self.encryption_key_id)
        else:
            self.log.info(
                "[prepare] Skipping bucket encryption (cluster version below "
                f"{self.BUCKET_EAR_MIN_VERSION})"
            )

        if enable_log_encryption_now:
            self._enable_log_encryption_at_rest()
        else:
            self.log.info(
                "[prepare] Skipping log encryption (cluster version below "
                f"{self.BUCKET_EAR_MIN_VERSION})"
            )

        # 10k docs into a custom scope/collection AND 10k into _default._default.
        self.namespaces = []
        self.prepare_collection_for_indexing(
            num_scopes=1, num_collections=1,
            num_of_docs_per_collection=self.num_of_docs_per_collection,
            json_template=self.json_template,
            bucket_name=self.test_bucket,
            load_default_coll=True,
        )
        self.log.info(f"[prepare] Namespaces created: {self.namespaces}")
        self.assertGreaterEqual(
            len(self.namespaces), 2,
            f"Expected default + non-default namespaces, got {self.namespaces}",
        )

        # Plasma indexes across both namespaces.
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        select_queries = set()
        for i, ns in enumerate(self.namespaces):
            tail = "".join(ns.split(':')[1].split('.'))
            prefix = f"enc_{i+1}_{tail}"
            defs = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, skip_primary=False,
            )
            select_queries.update(
                self.gsi_util_obj.get_select_queries(definition_list=defs, namespace=ns)
            )
            create_q = self.gsi_util_obj.get_create_index_list(
                definition_list=defs, namespace=ns,
                num_replica=self.num_index_replica,
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=create_q, database=ns, query_node=query_node,
            )
            self.log.info(f"[prepare] Created {len(create_q)} indexes on {ns}")

        self.wait_until_indexes_online(timeout=600)
        baseline_queries = list(select_queries)
        self._run_select_scans(baseline_queries)
        return baseline_queries, list(self.namespaces)

    # ------------------------------------------------------------------ vector bucket prep

    def _prepare_vector_bucket(self):
        """Create a vector bucket, load 10k MSMARCOSiftEmbeddingProduct docs,
        create 3 dense BHIVE indexes (no scalar, no sparse), wait online,
        run a baseline SELECT sweep. Only call when initial_version >= 8.0.

        Returns (vector_namespaces, vector_queries, vector_bucket_name).
        """
        vector_bucket = f"{self.test_bucket}_vector"
        self.log.info(f"[prepare] Creating vector bucket '{vector_bucket}'")
        bucket_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size, replicas=self.num_replicas,
            bucket_type=self.bucket_type, enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
        )
        self.cluster.create_standard_bucket(
            name=vector_bucket, port=11222, bucket_params=bucket_params,
        )
        self.sleep(10, "Wait after vector bucket creation")

        if self.initial_supports_bucket_ear:
            self._enable_bucket_encryption(vector_bucket, self.encryption_key_id)

        self.prepare_collection_for_indexing(
            num_scopes=1, num_collections=1,
            num_of_docs_per_collection=10000,
            json_template="MSMARCOSiftEmbeddingProduct",
            bucket_name=vector_bucket,
            load_default_coll=True,
        )
        vector_namespaces = [ns for ns in self.namespaces if vector_bucket in ns]
        self.log.info(f"[prepare] Vector namespaces: {vector_namespaces}")
        self.assertGreaterEqual(
            len(vector_namespaces), 2,
            "Expected default + non-default vector namespaces",
        )

        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        vector_queries = set()
        for i, ns in enumerate(vector_namespaces):
            tail = "".join(ns.split(':')[1].split('.'))
            # 1 BHIVE dense index
            bhive_prefix = f"vec_bhive_{i+1}_{tail}"
            bhive_defs = self.gsi_util_obj.get_index_definition_list(
                dataset="MSMARCOSiftEmbeddingProduct",
                prefix=bhive_prefix,
                bhive_index=True,
                array_indexes=False,
                similarity="L2_SQUARED",
                description_dimension=128,
            )[:1]
            # 1 composite dense index
            comp_prefix = f"vec_comp_{i+1}_{tail}"
            comp_defs = self.gsi_util_obj.get_index_definition_list(
                dataset="MSMARCOSiftEmbeddingProduct",
                prefix=comp_prefix,
                bhive_index=False,
                array_indexes=False,
                similarity="L2_SQUARED",
                description_dimension=128,
            )[:1]
            all_defs = bhive_defs + comp_defs
            self.log.info(
                f"[prepare] Creating {len(all_defs)} vector indexes on {ns} "
                f"({len(bhive_defs)} BHIVE + {len(comp_defs)} composite)"
            )
            vector_queries.update(
                self.gsi_util_obj.get_select_queries(definition_list=all_defs, namespace=ns)
            )
            for idx_def in all_defs:
                is_bhive = getattr(idx_def, "bhive_index", False)
                create_q = self.gsi_util_obj.get_create_index_list(
                    definition_list=[idx_def], namespace=ns,
                    num_replica=self.num_index_replica,
                    bhive_index=is_bhive,
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_q, database=ns, query_node=query_node,
                )
            self.log.info(f"[prepare] Created {len(bhive_defs) + len(comp_defs)} vector indexes on {ns}")

        self.wait_until_indexes_online(timeout=600)
        vector_query_list = list(vector_queries)
        self._run_select_scans(vector_query_list)
        return vector_namespaces, vector_query_list, vector_bucket

    def _capture_item_counts(self):
        """Return a {index_name: count} map from the indexer /stats endpoint.
        Filters out _system indexes."""
        index_counts = self.get_item_counts_from_index_stats()
        counts = {}
        for entry in index_counts:
            name, count = entry["name"], entry["count"]
            if "_system:" in name:
                continue
            counts[name] = count
        self.log.info(
            f"[capture-counts] {len(counts)} indexes: " +
            ", ".join(f"{k}={v}" for k, v in sorted(counts.items())[:10]) +
            ("..." if len(counts) > 10 else "")
        )
        return counts

    def _validate_item_counts(self, pre_counts):
        """Fetch current index stats and assert every index still has at least the
        same item count that was recorded pre-upgrade."""
        current = self.get_item_counts_from_index_stats()
        post_counts = {}
        for entry in current:
            name, count = entry["name"], entry["count"]
            if "_system:" in name:
                continue
            post_counts[name] = count
        mismatches = []
        for name, pre_count in sorted(pre_counts.items()):
            post_count = post_counts.get(name)
            if post_count is None:
                mismatches.append(f"  {name}: missing from post-upgrade stats")
            elif post_count < pre_count:
                mismatches.append(
                    f"  {name}: count dropped from {pre_count} to {post_count}"
                )
        if mismatches:
            self.fail(
                "[validate-counts] Index item counts degraded after upgrade:\n"
                + "\n".join(mismatches)
            )
        self.log.info(f"[validate-counts] {len(pre_counts)} indexes OK")

    # ------------------------------------------------------------------ query helpers (ported)

    def _set_query_completed_settings(self, stream_size=500, threshold=0):
        """Apply completed-stream-size + completed-threshold on every query node."""
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True,
        )
        self.log.info(
            f"Setting completed-stream-size={stream_size}, completed-threshold={threshold} "
            f"on {len(query_nodes)} query node(s)"
        )
        for node in query_nodes:
            try:
                self.rest.set_completed_stream_size(node, stream_size)
                self.rest.set_completed_requests_collection_duration(node, threshold)
            except Exception as e:
                self.log.warning(f"set query settings on {node.ip} failed: {e}")
        self.sleep(10, "Wait for query settings to propagate")

    def _run_select_scans(self, select_queries, query_nodes=None):
        """Run every SELECT query on every query node. Tolerant of per-query failures
        (a mid-upgrade query node may be transiently unavailable)."""
        if not query_nodes:
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True,
            )
        if not select_queries:
            return
        self.log.info(
            f"Running {len(select_queries)} SELECT(s) on {len(query_nodes)} node(s)"
        )
        for node in query_nodes:
            for q in select_queries:
                try:
                    self.run_cbq_query(query=q, server=node, verbose=False)
                except Exception as e:
                    self.log.warning(f"SELECT on {node.ip} failed: {e}")

    def _local_request_log_files_exist(self, query_nodes):
        for node in query_nodes:
            try:
                log_path = self.encryption_helper.get_log_path(node)
            except Exception:
                continue
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
                )
            finally:
                shell.disconnect()
            if [l.strip() for l in out if l.strip()]:
                return True
        return False

    def _generate_completed_requests_and_history(self, query_nodes, select_queries):
        """Concurrent SELECT load until local_request_log.* archives appear on disk
        (or until we hit the iteration cap). Ported from
        n1ql_encryption_at_rest._generate_concurrent_load_until_archive."""
        MAX_ITER, BATCHES = 20, 5
        nodes = list(query_nodes) if query_nodes else [self.master]
        node_cycle = itertools.cycle(nodes)
        qlist = list(select_queries)
        n = max(len(qlist), 1)
        self.log.info(
            f"[history] Driving load: up to {MAX_ITER} iters x {BATCHES} batches "
            f"x {n} queries (across {len(nodes)} node(s))"
        )

        def _run_one(q, node):
            try:
                return self.run_cbq_query(query=q, server=node, verbose=False)
            except Exception:
                return None

        for it in range(1, MAX_ITER + 1):
            for _ in range(BATCHES):
                with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
                    futs = [ex.submit(_run_one, q, next(node_cycle)) for q in qlist]
                    concurrent.futures.wait(futs)
            if self._local_request_log_files_exist(query_nodes):
                self.log.info(
                    f"[history] local_request_log.* archives found after iter {it}"
                )
                return
        self.log.warning(
            f"[history] No local_request_log.* archives after {MAX_ITER} iters; continuing"
        )

    def _dispatch_background_query_batch(self, select_queries, query_nodes,
                                          workers=5):
        """Run select_queries x query_nodes concurrently in a background thread.

        Returns the thread so callers can join it. Failures inside individual
        queries are swallowed — these batches are load generators, not assertions.
        Ported from n1ql_encryption_at_rest._dispatch_background_query_batch.
        """
        queries_list = list(select_queries)
        nodes = list(query_nodes)
        pairs = [(q, n) for n in nodes for q in queries_list]

        def _runner():
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(self.run_cbq_query, query=q, server=n, verbose=False)
                    for q, n in pairs
                ]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        self.log.debug(f"[bg-query] failed: {e}")

        t = threading.Thread(target=_runner, daemon=True, name="bg-query-batch")
        t.start()
        return t

    def _trigger_ffdc_and_poll(self, query_nodes, expected_key_ids=None, label=""):
        """Trigger FFDC on every query node, poll up to 180s for files to appear.
        Returns (results dict, normalised per-node key id map)."""
        if isinstance(expected_key_ids, dict):
            node_keys = expected_key_ids
        else:
            node_keys = {n.ip: (expected_key_ids or []) for n in query_nodes}
        for node in query_nodes:
            try:
                status, response = self.rest.trigger_query_ffdc(node)
                if status:
                    self.log.info(f"{label}FFDC triggered on {node.ip}")
                else:
                    self.log.warning(f"{label}FFDC trigger failed on {node.ip}: {response}")
            except Exception as e:
                self.log.warning(f"{label}FFDC trigger exception on {node.ip}: {e}")

        deadline = time.time() + 180
        results = {}
        while time.time() < deadline:
            results = {}
            for node in query_nodes:
                try:
                    partial = self.encryption_helper.verify_query_ffdc_files_encrypted(
                        [node], node_keys.get(node.ip, []),
                    )
                    results.update(partial)
                except Exception as e:
                    self.log.warning(f"{label}verify_query_ffdc_files on {node.ip} failed: {e}")
            if sum(len(v) for v in results.values()) > 0:
                break
            self.sleep(15, f"{label}Waiting for FFDC files...")
        return results, node_keys

    def _get_query_in_use_key_ids(self, category=None):
        """Returns {node_ip: [key_id, ...]} from each query node.

        Args:
            category: if None, returns the union of all categories (legacy
                behavior). If one of "log" / "other", returns ONLY that
                category's keys — required by per-category re-encryption polls,
                since the endpoint response shape is
                ``{"keys.in_use": {"log": [...], "other": [...]}}`` and mixing
                them would mask the very change we're waiting for.
        """
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True,
        )
        out = {}
        for node in query_nodes:
            try:
                status, enc = self.rest.get_query_in_use_encryption_keys(node)
                if not status:
                    out[node.ip] = []
                    continue
                if isinstance(enc, (bytes, bytearray)):
                    enc = json.loads(enc)
                in_use = enc.get("keys.in_use", {}) if isinstance(enc, dict) else {}
                if category is None:
                    keys = [k for ids in in_use.values() for k in ids if k]
                else:
                    keys = [k for k in (in_use.get(category) or []) if k]
                out[node.ip] = keys
            except Exception as e:
                self.log.warning(f"get_query_in_use_encryption_keys on {node.ip} failed: {e}")
                out[node.ip] = []
        return out

    def _validate_query_keys_in_use_endpoint(self, label=""):
        """Return query-service in-use key IDs per node from
        GET /admin/encryption_at_rest on every query node (8.1+).

        Returns ``{node_ip: [key_id, ...]}``.  When log or other encryption is
        enabled cluster-side, the corresponding per-category keys must be
        non-empty; when a category is NOT enabled its keys must be empty.

        Skips per-node when the node's running version is < EAR_FILE_MIN_VERSION
        because the per-category endpoint is an 8.1+ feature.
        """
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True,
        )
        log_enabled = bool(getattr(self, "log_encryption_at_rest_id", None))
        other_enabled = bool(getattr(self, "other_encryption_at_rest_id", None))
        self.log.info(
            f"{label}keys-in-use expectations: log_enabled={log_enabled}, "
            f"other_enabled={other_enabled}"
        )
        node_key_ids = {}
        for node in query_nodes:
            ver = self._node_running_version(node)
            if ver < self.EAR_FILE_MIN_VERSION:
                self.log.info(
                    f"{label}{node.ip} on {ver} < {self.EAR_FILE_MIN_VERSION} — "
                    "skipping keys-in-use endpoint check"
                )
                continue
            status, enc_response = self.rest.get_query_in_use_encryption_keys(node)
            self.assertTrue(
                status,
                f"{label}admin/encryption_at_rest failed on {node.ip}: {enc_response}",
            )
            if isinstance(enc_response, (bytes, bytearray)):
                enc_response = json.loads(enc_response)
            keys_in_use = enc_response.get("keys.in_use", {})
            all_key_ids = [k for ids in keys_in_use.values() for k in ids if k]
            self.log.info(
                f"{label}Query service in-use key IDs on {node.ip} ({ver}): "
                f"{keys_in_use} -> {all_key_ids}"
            )
            node_key_ids[node.ip] = all_key_ids
            log_ids = [k for k in (keys_in_use.get("log") or []) if k]
            other_ids = [k for k in (keys_in_use.get("other") or []) if k]
            if log_enabled:
                self.assertTrue(
                    len(log_ids) > 0,
                    f"{label}{node.ip}: log encryption is enabled but "
                    "endpoint reports no in-use log DEKs",
                )
            else:
                self.assertTrue(
                    len(log_ids) == 0,
                    f"{label}{node.ip}: log encryption is NOT enabled but "
                    f"endpoint reports in-use log DEKs {log_ids}",
                )
            # TODO: Re-enable once MB-72285 is fixed — post-upgrade the
            # /admin/encryption_at_rest endpoint does not return other keys in use.
            # https://jira.issues.couchbase.com/browse/MB-72285
            # if other_enabled:
            #     self.assertTrue(
            #         len(other_ids) > 0,
            #         f"{label}{node.ip}: other encryption is enabled but "
            #         "endpoint reports no in-use other DEKs",
            #     )
            # else:
            #     self.assertTrue(
            #         len(other_ids) == 0,
            #         f"{label}{node.ip}: other encryption is NOT enabled but "
            #         f"endpoint reports in-use other DEKs {other_ids}",
            #     )
        return node_key_ids

    def _get_indexer_in_use_key_ids(self, index_nodes):
        """Union of in-use key IDs across all index nodes (sorted list)."""
        keys = set()
        for node in index_nodes:
            try:
                rest = RestConnection(node)
                status, response = rest.get_indexer_in_use_encryption_keys()
                if not status:
                    continue
                self._extract_indexer_key_ids(response, keys)
            except Exception as e:
                self.log.warning(f"get_indexer_in_use_encryption_keys on {node.ip} failed: {e}")
        return sorted(keys)

    @staticmethod
    def _extract_indexer_key_ids(response, out_set):
        """Walk a (possibly nested) indexer GetInUseKeys response and collect IDs."""
        if isinstance(response, list):
            for k in response:
                if k:
                    out_set.add(str(k))
            return
        if isinstance(response, dict):
            for v in response.values():
                GSIQueryEncryptionAtRestUpgrade._extract_indexer_key_ids(v, out_set)

    # ------------------------------------------------------------------ spill / backfill

    def _run_spill_and_backfill_workload(self, query_nodes, namespace):
        """Issue scan-backfill queries on the loaded data to produce scan-results* files.

        Mirrors the query-service scan-backfill workload used in
        n1ql_encryption_at_rest.py: project a wide row, scan on country,
        and cap the request buffer so the index scan backfills to disk.
        Per-query failures are tolerated (mid-upgrade nodes may bounce).
        """
        scan_cap = self.input.param("backfill_scan_cap", 128)
        item_count = self.input.param("num_backfill_docs", 100000)
        backfill_query = (
            f"SELECT meta().id AS id, name, country, city, state, address, "
            f"description, reviews FROM {namespace} "
            f"WHERE country IS NOT NULL LIMIT {item_count}"
        )
        query_params = {"scan_cap": scan_cap}
        self.log.info(
            f"[spill] backfill query (scan_cap={scan_cap}, item_count={item_count}): "
            f"{backfill_query[:150]}"
        )
        for node in query_nodes:
            for _ in range(3):
                try:
                    self.run_cbq_query(
                        query=backfill_query,
                        server=node,
                        query_params=query_params,
                        verbose=False,
                    )
                except Exception as e:
                    self.log.warning(f"[spill] query on {node.ip} failed: {e}")

    # ------------------------------------------------------------------ index map

    def _capture_index_identity_set(self):
        """Return {(bucket, scope, collection, index_name)} via the indexer's
        GET :9102/getIndexStatus endpoint (RestConnection.get_indexer_metadata).

        This avoids N1QL system:indexes which depends on the query node having a
        fully synced KV collection-manifest — something that can take minutes after
        an offline node restart and causes spurious 'Scope not found' failures.
        The indexer REST endpoint is authoritative and has no such dependency."""
        try:
            index_nodes = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True,
            )
            index_node = index_nodes[0] if index_nodes else None
        except Exception:
            index_node = None
        if index_node is None:
            self.log.warning("[index_map] No index nodes found — returning empty set")
            return set()
        try:
            index_meta = RestConnection(index_node).get_indexer_metadata()
        except Exception as e:
            self.log.warning(f"[index_map] get_indexer_metadata on {index_node.ip} failed: {e}")
            return set()
        ids = set()
        for item in index_meta.get('status', []):
            ids.add((
                item.get('bucket'),
                item.get('scope'),
                item.get('collection'),
                item.get('name'),
            ))
        self.log.info(f"[index_map] {len(ids)} index identity entries captured")
        return ids

    # ------------------------------------------------------------------ mixed-mode validation

    def _assert_node_gsi_files_plaintext(self, node, label=""):
        """Sample *.data files under @2i on this node and confirm they're NOT encrypted.
        Used to validate that a pre-EAR_FILE_MIN_VERSION node is producing plaintext."""
        rest = RestConnection(node)
        storage_dir = f"{rest.get_index_path()}/@2i"
        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            out, _ = shell.execute_command(
                f"find {storage_dir} -type f -name '*.data' 2>/dev/null | head -5"
            )
        finally:
            shell.disconnect()
        sample = [l.strip() for l in out if l.strip()]
        if not sample:
            self.log.warning(
                f"{label}{node.ip}: no *.data files under {storage_dir} — skipping plaintext check"
            )
            return
        for fpath in sample:
            is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                node, fpath
            )
            if is_enc:
                self.fail(
                    f"{label}{node.ip}: file {fpath} is ENCRYPTED but expected PLAINTEXT "
                    f"(node running {self._node_running_version(node)}, below {self.EAR_FILE_MIN_VERSION}): "
                    f"{details}"
                )
        self.log.info(
            f"{label}{node.ip}: sampled {len(sample)} *.data files — all PLAINTEXT (expected)"
        )

    def _assert_query_log_files_state(self, node, expect_encrypted, label=""):
        """Assert rlstream.* / local_request_log.* on this node match the expected
        encryption state. Empty file-set -> warn (test indeterminate)."""
        try:
            results = self.encryption_helper.verify_query_log_files_encrypted([node])
        except Exception as e:
            self.log.warning(f"{label}verify_query_log_files on {node.ip} failed: {e}")
            return
        node_result = results.get(node.ip, {}) if isinstance(results, dict) else {}
        all_files = {
            **node_result.get("rlstream", {}),
            **node_result.get("local_request_log", {}),
        }
        if not all_files:
            self.log.warning(
                f"{label}{node.ip}: no query log files found — skipping (indeterminate)"
            )
            return
        bad = []
        for fpath, r in all_files.items():
            actual = bool(r.get("encrypted", False))
            if actual != expect_encrypted:
                bad.append(f"  {fpath}: encrypted={actual} expected={expect_encrypted}")
        if bad:
            self.fail(
                f"{label}{node.ip}: query log file state mismatch "
                f"(expected encrypted={expect_encrypted}):\n" + "\n".join(bad)
            )
        self.log.info(
            f"{label}{node.ip}: {len(all_files)} query log file(s) state OK "
            f"(encrypted={expect_encrypted})"
        )

    def _validate_mixed_mode(self, upgraded_index_nodes, pending_index_nodes,
                             upgraded_query_nodes, pending_query_nodes,
                             encrypted_buckets):
        """Mid-upgrade checkpoint: create extra indexes, drive workload, then assert
        per-node GSI encryption state matches each node's running version.

        Returns a list of failure strings; never raises — caller defers failures to
        the end of the test so the upgrade can complete regardless."""
        errors = []
        self.log.info("=" * 80)
        self.log.info(
            f"[mixed-mode] upgraded="
            f"{[n.ip for n in upgraded_index_nodes + upgraded_query_nodes]} "
            f"pending="
            f"{[n.ip for n in pending_index_nodes + pending_query_nodes]}"
        )
        self.log.info("=" * 80)
        self.sleep(120, "[mixed-mode] Post-upgrade checkpoint settle")
        # 1. Add a small index on each existing namespace (placement up to indexer).
        extra_queries = set()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        for i, ns in enumerate(self.namespaces[:2]):
            try:
                idx_def = QueryDefinition(
                    index_name=f"mixed_mode_idx_{i+1}",
                    index_fields=["country"],
                )
                create_q = idx_def.generate_index_create_query(
                    namespace=ns, defer_build=False,
                )
                self.run_cbq_query(query=create_q, server=query_node)
                extra_queries.add(
                    f"SELECT country FROM {ns} WHERE country IS NOT NULL LIMIT 100"
                )
            except Exception as e:
                self.log.warning(f"[mixed-mode] extra index on {ns} failed: {e}")
        try:
            self.wait_until_indexes_online(timeout=300)
        except Exception as e:
            self.log.warning(f"[mixed-mode] wait_until_indexes_online: {e}")

        # 2. Workload against ALL query nodes (both upgraded and pending).
        all_query_nodes = upgraded_query_nodes + pending_query_nodes
        self._run_select_scans(extra_queries, all_query_nodes)
        if self.namespaces:
            self._run_spill_and_backfill_workload(all_query_nodes, self.namespaces[0])

        # 3. Per-index-node GSI state.
        if upgraded_index_nodes:
            index_keys = self._get_indexer_in_use_key_ids(upgraded_index_nodes)
        else:
            index_keys = []
        for node in upgraded_index_nodes:
            ver = self._node_running_version(node)
            self.log.info(f"[mixed-mode] index {node.ip} ({ver}): expect ENCRYPTED")
            try:
                self.validate_engine_encryption(
                    'plasma', [node],
                    expected_key_id=index_keys,
                    encrypted_bucket_names=encrypted_buckets,
                    fail_on_error=True,
                )
                self.log.info(f"[mixed-mode] index {node.ip}: GSI files ENCRYPTED OK")
            except AssertionError as e:
                msg = f"[mixed-mode] index {node.ip} GSI encryption assertion failed: {e}"
                self.log.error(msg)
                errors.append(msg)
        for node in pending_index_nodes:
            ver = self._node_running_version(node)
            self.log.info(f"[mixed-mode] index {node.ip} ({ver}): expect PLAINTEXT")
            try:
                self._assert_node_gsi_files_plaintext(node, label="[mixed-mode] ")
            except AssertionError as e:
                msg = f"[mixed-mode] index {node.ip} plaintext assertion failed: {e}"
                self.log.error(msg)
                errors.append(msg)

        # 4. Sanity scan to confirm cluster is still serving.
        self._run_select_scans(extra_queries, all_query_nodes)

        if errors:
            self.log.error(
                f"[mixed-mode] {len(errors)} failure(s) — will be reported at end of test"
            )
        else:
            self.log.info("[mixed-mode] checkpoint PASSED")
        return errors

    # ------------------------------------------------------------------ per-node upgrade

    def _upgrade_node(self, node, mode):
        """Upgrade a single node, online or offline.

        Mirrors the per-node branches in upgrade_gsi.upgrade_and_validate
        (lines 3820-3861). Captures the node's service list BEFORE doing
        anything, so the online branch can rebalance it back in with the
        same services.
        """
        node_rest = RestConnection(node)
        node_info = f"{node.ip}:{node.port}"
        try:
            node_services_list = node_rest.get_nodes_services()[node_info]
        except Exception:
            # If the node info is keyed differently, fall back to scanning the map.
            services_map = node_rest.get_nodes_services()
            node_services_list = None
            for key, val in services_map.items():
                if key.startswith(node.ip + ":"):
                    node_services_list = val
                    break
            if node_services_list is None:
                raise Exception(f"Could not resolve services for {node.ip} from {services_map}")
        node_services_str = [",".join(node_services_list)]
        self.log.info(f"[upgrade] {node.ip} (services={node_services_list}) via {mode}")

        cluster_profile = "provisioned"
        if (self._version_tuple(self.initial_version) >= (7, 6)
                or self._version_tuple(self.upgrade_to) >= (8, 0)):
            cluster_profile = None

        if mode == 'offline':
            self.rest.update_autofailover_settings(True, 300)
            remote = RemoteMachineShellConnection(node)
            try:
                remote.stop_server()
            finally:
                remote.disconnect()
            upgrade_th = self._async_update(
                self.upgrade_to, [node], cluster_profile=cluster_profile,
            )
            for th in upgrade_th:
                th.join()
            self.sleep(120, f"[upgrade] post-offline-install settle on {node.ip}")
        elif mode == 'online':
            self.log.info(f"[upgrade] rebalancing out {node.ip}")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], [node],
            )
            rebalance.result()
            self.log.info(f"[upgrade] installing {self.upgrade_to} on {node.ip}")
            upgrade_th = self._async_update(
                upgrade_version=self.upgrade_to, servers=[node],
                cluster_profile=cluster_profile,
            )
            for th in upgrade_th:
                th.join()
            self.sleep(120, f"[upgrade] post-online-install settle on {node.ip}")
            active = [n for n in self.servers[:self.nodes_init] if n.ip != node.ip]
            self.log.info(
                f"[upgrade] rebalancing in {node.ip} with services {node_services_str}"
            )
            rebalance = self.cluster.async_rebalance(
                active, [node], [], services=node_services_str,
            )
            rebalance.result()
            self.sleep(15, "wait after rebalance-in")
        else:
            raise ValueError(f"Unknown upgrade mode: {mode}")
        self.log.info(f"[upgrade] {node.ip} completed")

    # ------------------------------------------------------------------ post-upgrade helpers

    @staticmethod
    def _collect_empty_key_entries(response, _path=""):
        """Walk a GetInUseKeys response tree and return a list of human-readable
        path strings for every empty (falsy) key ID found.

        An empty key ID means the corresponding store has not yet been re-encrypted
        with the current active DEK — it is still pending or was never encrypted."""
        entries = []
        if isinstance(response, list):
            for k in response:
                if not k:
                    entries.append(f"empty key under [{_path}]")
        elif isinstance(response, dict):
            for bucket_label, val in response.items():
                child = bucket_label if not _path else f"{_path}/{bucket_label}"
                entries.extend(
                    GSIQueryEncryptionAtRestUpgrade._collect_empty_key_entries(val, child)
                )
        return entries

    def _wait_for_full_gsi_encryption(self, index_nodes, encrypted_buckets,
                                      timeout=1200, label=""):
        """Poll GetInUseKeys on every index node until no empty key-ID entries remain.

        After a force re-encryption (DEK drop), the indexer assigns a new DEK to each
        store as it re-encrypts it.  While a store is still pending it reports an empty
        key ID.  Once all stores have been re-encrypted every entry in the
        GetInUseKeys response is a non-empty UUID — that is the completion signal.

        Fails with a clear message after *timeout* seconds."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_done = True
            pending = []
            for node in index_nodes:
                try:
                    rest = RestConnection(node)
                    status, response = rest.get_indexer_in_use_encryption_keys()
                    if not status:
                        all_done = False
                        pending.append(f"{node.ip}: GetInUseKeys returned status=False")
                        continue
                    empty_entries = self._collect_empty_key_entries(response)
                    if empty_entries:
                        all_done = False
                        for entry in empty_entries[:3]:
                            pending.append(f"{node.ip}: {entry}")
                except Exception as e:
                    self.log.warning(
                        f"{label}{node.ip}: get_indexer_in_use_encryption_keys failed: {e}"
                    )
                    all_done = False
                    pending.append(f"{node.ip}: REST error")
                    continue
            if all_done:
                self.log.info(
                    f"{label}All GSI stores fully encrypted (no empty key IDs in GetInUseKeys)"
                )
                return
            preview = pending[:5]
            suffix = f" ... (+{len(pending) - 5} more)" if len(pending) > 5 else ""
            self.log.info(
                f"{label}{len(pending)} store(s) still re-encrypting: "
                f"{preview}{suffix}"
            )
            self.sleep(30, f"{label}Waiting for GSI re-encryption to complete")
        self.fail(
            f"{label}GSI re-encryption did not complete within {timeout}s. "
            f"Still pending: {pending}"
        )

    def _wait_for_no_empty_keys_in_use(self, index_nodes, query_nodes,
                                       encrypted_buckets, timeout=1200, label=""):
        """Pre-flight gate before any file-encryption validation.

        Polls GetInUseKeys on every relevant service until NONE of the enabled
        encryption categories report an empty key ID:

          * bucket encryption (GSI)   — if `encrypted_buckets` is non-empty,
            every indexer store must report a non-empty key ID
            (delegates to `_wait_for_full_gsi_encryption`).
          * log encryption (Query)    — if `self.log_encryption_at_rest_id` is
            set, `keys.in_use["log"]` on every query node must be non-empty
            and contain no empty strings.
          * other encryption (Query)  — if `self.other_encryption_at_rest_id`
            is set, `keys.in_use["other"]` must be non-empty/non-empty-strings.

        An "empty key" (empty string or empty list under a category) means the
        store has not yet been re-encrypted with the current active DEK —
        validating files at that point would race the encryptor and produce
        spurious failures.

        Polls every 30 s up to `timeout` seconds; fails clearly on timeout."""
        log_enabled = bool(getattr(self, "log_encryption_at_rest_id", None))
        other_enabled = bool(getattr(self, "other_encryption_at_rest_id", None))
        bucket_enabled = bool(encrypted_buckets)
        self.log.info(
            f"{label}Waiting (up to {timeout}s) for no empty keys in use: "
            f"bucket={bucket_enabled}, log={log_enabled}, other={other_enabled}"
        )

        if bucket_enabled and index_nodes:
            try:
                self._wait_for_full_gsi_encryption(
                    index_nodes, encrypted_buckets, timeout=timeout,
                    label=f"{label}[bucket-keys] ",
                )
            except AssertionError as e:
                # Empty bucket key IDs after timeout — log and continue to
                # file-encryption validation instead of failing the test.
                self.log.warning(
                    f"{label}[bucket-keys] empty key IDs still present after "
                    f"{timeout}s; proceeding to file validation anyway: {e}"
                )

        if not (log_enabled or other_enabled) or not query_nodes:
            return

        deadline = time.time() + timeout
        while time.time() < deadline:
            pending = []
            for node in query_nodes:
                ver = self._node_running_version(node)
                if ver < self.EAR_FILE_MIN_VERSION:
                    continue
                try:
                    status, enc = self.rest.get_query_in_use_encryption_keys(node)
                    if not status:
                        pending.append(f"{node.ip}: keys-in-use status=False")
                        continue
                    if isinstance(enc, (bytes, bytearray)):
                        enc = json.loads(enc)
                    keys_in_use = (
                        enc.get("keys.in_use", {}) if isinstance(enc, dict) else {}
                    )
                    for cat, enabled in (("log", log_enabled), ("other", other_enabled)):
                        if not enabled:
                            continue
                        ids = keys_in_use.get(cat) or []
                        if not ids:
                            pending.append(
                                f"{node.ip}: '{cat}' has no in-use key IDs"
                            )
                            continue
                        empties = [i for i, k in enumerate(ids) if not k]
                        if empties:
                            pending.append(
                                f"{node.ip}: '{cat}' has empty key id(s) at "
                                f"indices {empties} (full list: {ids})"
                            )
                except Exception as e:
                    pending.append(f"{node.ip}: keys-in-use error: {e}")

            if not pending:
                self.log.info(
                    f"{label}All query categories report non-empty in-use key IDs"
                )
                return
            preview = pending[:5]
            suffix = f" ... (+{len(pending) - 5} more)" if len(pending) > 5 else ""
            self.log.info(
                f"{label}{len(pending)} query store(s) still report empty keys: "
                f"{preview}{suffix}"
            )
            self.sleep(30, f"{label}Waiting for non-empty query keys-in-use")

        # Do NOT fail the test on timeout — empty key IDs at this point may
        # simply mean re-encryption is still in flight. Log a warning and let
        # the file-encryption validation proceed so we still get coverage.
        self.log.warning(
            f"{label}Query keys-in-use still report empty keys after {timeout}s; "
            f"proceeding to file validation anyway. Still pending: {pending}"
        )

    def _create_post_upgrade_indexes(self, query_nodes):
        """Create a fresh set of indexes with a 'post_upg' prefix after full upgrade.
        Returns a set of SELECT queries that target those indexes."""
        post_queries = set()
        query_node = query_nodes[0] if query_nodes else self.get_nodes_from_services_map(
            service_type="n1ql"
        )
        for i, ns in enumerate(self.namespaces):
            try:
                tail = "".join(ns.split(':')[1].split('.'))
                bucket = ns.split(':')[1].split('.')[0]
                is_vector = "_vector" in bucket
                if is_vector:
                    dataset = "MSMARCOSiftEmbeddingProduct"
                    # 1 BHIVE dense
                    bhive_defs = self.gsi_util_obj.get_index_definition_list(
                        dataset=dataset,
                        prefix=f"post_upg_vec_bhive_{i+1}_{tail}",
                        bhive_index=True, array_indexes=False,
                        similarity="L2_SQUARED", description_dimension=128,
                    )[:1]
                    # 1 composite dense
                    comp_defs = self.gsi_util_obj.get_index_definition_list(
                        dataset=dataset,
                        prefix=f"post_upg_vec_comp_{i+1}_{tail}",
                        bhive_index=False, array_indexes=False,
                        similarity="L2_SQUARED", description_dimension=128,
                    )[:1]
                    defs = bhive_defs + comp_defs
                else:
                    prefix = f"post_upg_{i+1}_{tail}"
                    defs = self.gsi_util_obj.get_index_definition_list(
                        dataset=self.json_template, prefix=prefix, skip_primary=True,
                    )
                post_queries.update(
                    self.gsi_util_obj.get_select_queries(definition_list=defs, namespace=ns)
                )
                for idx_def in defs:
                    is_bhive = getattr(idx_def, "bhive_index", False)
                    create_q = self.gsi_util_obj.get_create_index_list(
                        definition_list=[idx_def], namespace=ns,
                        num_replica=self.num_index_replica,
                        bhive_index=is_bhive,
                    )
                    self.gsi_util_obj.create_gsi_indexes(
                        create_queries=create_q, database=ns, query_node=query_node,
                    )
                self.log.info(
                    f"[post-upgrade] Created {len(defs)} post-upgrade indexes on {ns}"
                )
            except Exception as e:
                self.log.warning(
                    f"[post-upgrade] post-upgrade index creation on {ns} failed: {e}"
                )
        try:
            self.wait_until_indexes_online(timeout=600)
            self.log.info("[post-upgrade] Post-upgrade indexes online")
        except Exception as e:
            self.log.warning(f"[post-upgrade] wait_until_indexes_online: {e}")
        return post_queries

    def _snapshot_query_log_file_paths(self, query_nodes):
        """Return {node_ip: set(fpath)} for all rlstream.* / local_request_log.* files
        currently on disk. Used to isolate files created after the snapshot."""
        snapshot = {}
        for node in query_nodes:
            try:
                res = self.encryption_helper.verify_query_log_files_encrypted([node])
            except Exception:
                snapshot[node.ip] = set()
                continue
            node_result = res.get(node.ip, {}) if isinstance(res, dict) else {}
            files = set()
            for cat in ("rlstream", "local_request_log"):
                files.update(node_result.get(cat, {}).keys())
            snapshot[node.ip] = files
        return snapshot

    def _snapshot_ffdc_file_paths(self, query_nodes):
        """Return {node_ip: set(fpath)} for all query_ffdc_MAN_* files currently on disk."""
        snapshot = {}
        for node in query_nodes:
            try:
                res = self.encryption_helper.verify_query_ffdc_files_encrypted([node], [])
            except Exception:
                snapshot[node.ip] = set()
                continue
            node_result = res.get(node.ip, {}) if isinstance(res, dict) else {}
            snapshot[node.ip] = (
                set(node_result.keys()) if isinstance(node_result, dict) else set()
            )
        return snapshot

    def _force_reencrypt_bucket_and_wait(self, bucket, index_nodes, timeout=300,
                                         label=""):
        """Drop all DEKs for *bucket* (POST /controller/dropEncryptionAtRestDeks/bucket/<bucket>)
        and poll until the indexer reports key IDs that differ from the baseline.
        Returns the new sorted list of in-use key IDs."""
        baseline = set(self._get_indexer_in_use_key_ids(index_nodes))
        self.log.info(
            f"{label}Triggering force re-encryption for bucket '{bucket}' "
            f"(baseline key IDs: {sorted(baseline)})"
        )
        status, response = self.rest.trigger_data_reencryption(bucket)
        if not status:
            raise Exception(f"trigger_data_reencryption failed: {response}")
        self.log.info(f"{label}Bucket re-encryption triggered: {response}")

        deadline = time.time() + timeout
        while time.time() < deadline:
            current = set(self._get_indexer_in_use_key_ids(index_nodes))
            if current != baseline:
                new_ids = sorted(current)
                self.log.info(
                    f"{label}New indexer key IDs detected: {new_ids} "
                    f"(baseline was {sorted(baseline)})"
                )
                return new_ids
            self.sleep(15, f"{label}Waiting for new indexer key IDs after bucket re-encryption")
        self.fail(
            f"{label}No new indexer key IDs appeared within {timeout}s after "
            f"triggering bucket re-encryption (baseline: {sorted(baseline)})"
        )

    def _force_reencrypt_logs_and_wait(self, query_nodes, timeout=300, label=""):
        """Drop all log DEKs (POST /controller/dropEncryptionAtRestDeks/log) and poll
        until any query node reports a change in the ``log`` category specifically.
        Returns the new {node_ip: [log_key_id, ...]} map.

        Best-effort: if the baseline is empty on every node (no log keys to drop)
        or the timeout elapses without a change, logs a warning and returns the
        last observed map instead of failing the test. File-encryption validation
        downstream is the real signal of correctness."""
        baseline = self._get_query_in_use_key_ids(category="log")
        self.log.info(
            f"{label}Triggering force log re-encryption "
            f"(baseline log key IDs per node: {baseline})"
        )
        if not any(baseline.get(n.ip) for n in query_nodes):
            self.log.warning(
                f"{label}Baseline log key IDs are empty on every query node — "
                "nothing to re-encrypt; skipping trigger + wait."
            )
            return baseline
        status, response = self.rest.trigger_log_reencryption()
        if not status:
            self.log.warning(
                f"{label}trigger_log_reencryption returned status=False: {response}; "
                "proceeding without waiting for new key IDs."
            )
            return baseline
        self.log.info(f"{label}Log re-encryption triggered: {response}")

        deadline = time.time() + timeout
        current = baseline
        while time.time() < deadline:
            current = self._get_query_in_use_key_ids(category="log")
            if any(
                set(current.get(node.ip, [])) != set(baseline.get(node.ip, []))
                for node in query_nodes
            ):
                self.log.info(
                    f"{label}New log key IDs detected: {current} "
                    f"(baseline was {baseline})"
                )
                return current
            self.sleep(15, f"{label}Waiting for new log key IDs after log re-encryption")
        self.log.warning(
            f"{label}No new log key IDs appeared within {timeout}s after "
            f"triggering log re-encryption (baseline: {baseline}); "
            "continuing — file-encryption validation will catch real failures."
        )
        return current

    def _force_reencrypt_other_and_wait(self, query_nodes, timeout=300, label=""):
        """Drop all other-DEKs (POST /controller/dropEncryptionAtRestDeks/other) and poll
        until any query node reports a change in the ``other`` category specifically.
        Returns the new {node_ip: [other_key_id, ...]} map.

        Best-effort: if the baseline is empty on every node (no other keys to drop)
        or the timeout elapses without a change, logs a warning and returns the
        last observed map instead of failing the test. File-encryption validation
        downstream is the real signal of correctness."""
        baseline = self._get_query_in_use_key_ids(category="other")
        self.log.info(
            f"{label}Triggering force other re-encryption "
            f"(baseline other key IDs per node: {baseline})"
        )
        if not any(baseline.get(n.ip) for n in query_nodes):
            self.log.warning(
                f"{label}Baseline other key IDs are empty on every query node — "
                "nothing to re-encrypt; skipping trigger + wait."
            )
            return baseline
        status, response = self.rest.trigger_other_reencryption()
        if not status:
            self.log.warning(
                f"{label}trigger_other_reencryption returned status=False: {response}; "
                "proceeding without waiting for new key IDs."
            )
            return baseline
        self.log.info(f"{label}Other re-encryption triggered: {response}")

        deadline = time.time() + timeout
        current = baseline
        while time.time() < deadline:
            current = self._get_query_in_use_key_ids(category="other")
            if any(
                set(current.get(node.ip, [])) != set(baseline.get(node.ip, []))
                for node in query_nodes
            ):
                self.log.info(
                    f"{label}New other key IDs detected: {current} "
                    f"(baseline was {baseline})"
                )
                return current
            self.sleep(15, f"{label}Waiting for new other key IDs after other re-encryption")
        self.log.warning(
            f"{label}No new other key IDs appeared within {timeout}s after "
            f"triggering other re-encryption (baseline: {baseline}); "
            "continuing — file-encryption validation will catch real failures."
        )
        return current

    def _trigger_ffdc_and_wait_for_new(self, query_nodes, pre_snapshot, query_keys,
                                       label=""):
        """Trigger FFDC on every query node, then poll until files NOT present in
        pre_snapshot appear on disk (up to 180 s). Returns {node_ip: {fpath: result}}
        containing only newly created files."""
        for node in query_nodes:
            try:
                status, response = self.rest.trigger_query_ffdc(node)
                if status:
                    self.log.info(f"{label}FFDC triggered on {node.ip}")
                else:
                    self.log.warning(f"{label}FFDC trigger failed on {node.ip}: {response}")
            except Exception as e:
                self.log.warning(f"{label}FFDC trigger exception on {node.ip}: {e}")

        deadline = time.time() + 180
        new_results = {}
        while time.time() < deadline:
            all_results = {}
            for node in query_nodes:
                try:
                    partial = self.encryption_helper.verify_query_ffdc_files_encrypted(
                        [node], query_keys.get(node.ip, []),
                    )
                    all_results.update(partial)
                except Exception as e:
                    self.log.warning(
                        f"{label}verify_query_ffdc_files on {node.ip} failed: {e}"
                    )
            new_results = {}
            for node in query_nodes:
                existing = pre_snapshot.get(node.ip, set())
                node_result = (
                    all_results.get(node.ip, {}) if isinstance(all_results, dict) else {}
                )
                new_files = (
                    {fp: r for fp, r in node_result.items() if fp not in existing}
                    if isinstance(node_result, dict) else {}
                )
                if new_files:
                    new_results[node.ip] = new_files
            if new_results:
                self.log.info(
                    f"{label}New FFDC files found: "
                    f"{sum(len(v) for v in new_results.values())}"
                )
                break
            self.sleep(15, f"{label}Waiting for new FFDC files...")
        return new_results

    def _collect_query_encryption_failures(self, query_nodes, expected_key_ids, label=""):
        """Return a list of query request-log / FFDC file failures."""
        failures = []
        log_results = {}
        ffdc_results = {}
        for node in query_nodes:
            node_keys = (
                expected_key_ids.get(node.ip, [])
                if isinstance(expected_key_ids, dict)
                else (expected_key_ids or [])
            )
            log_results.update(
                self.encryption_helper.verify_query_log_files_encrypted([node], node_keys)
            )
        for node_ip, node_result in log_results.items():
            for category in ("rlstream", "local_request_log"):
                for fpath, result in node_result.get(category, {}).items():
                    if result.get("transient"):
                        continue
                    if not result.get("encrypted"):
                        failures.append(
                            f"  [{node_ip}] {category} {fpath}: NOT encrypted: "
                            f"{result.get('details')}"
                        )
                    elif result.get("key_id_found") is False:
                        failures.append(
                            f"  [{node_ip}] {category} {fpath}: header missing expected key ID"
                        )

        for node in query_nodes:
            node_keys = (
                expected_key_ids.get(node.ip, [])
                if isinstance(expected_key_ids, dict)
                else (expected_key_ids or [])
            )
            ffdc_results.update(
                self.encryption_helper.verify_query_ffdc_files_encrypted([node], node_keys)
            )
        for node_ip, node_result in ffdc_results.items():
            for fpath, result in node_result.items():
                if not result.get("encrypted"):
                    failures.append(
                        f"  [{node_ip}] FFDC {fpath}: NOT encrypted: {result.get('details')}"
                    )
                elif result.get("key_id_found") is False:
                    failures.append(
                        f"  [{node_ip}] FFDC {fpath}: header missing expected key ID"
                    )
        if failures:
            self.log.error(
                f"{label}Query encryption validation found {len(failures)} failure(s)"
            )
        return failures

    def _collect_gsi_encryption_failures(
        self, index_nodes, encrypted_buckets, expected_key_id, query_node=None, label=""
    ):
        """Return a list of GSI file-encryption failures."""
        results = self.validate_engine_encryption(
            "plasma",
            index_nodes,
            expected_key_id=expected_key_id,
            encrypted_bucket_names=encrypted_buckets,
            query_node=query_node,
            fail_on_error=False,
        )
        failures = []
        for category, node_results in results.items():
            if category == "_overall" or not isinstance(node_results, dict):
                continue
            for node_ip, result in node_results.items():
                if not isinstance(result, dict):
                    continue
                for failed_file in result.get("failed_files", []) or []:
                    if isinstance(failed_file, (list, tuple)) and failed_file:
                        file_path = failed_file[0]
                        details = failed_file[1] if len(failed_file) > 1 else ""
                    else:
                        file_path = str(failed_file)
                        details = ""
                    failures.append(
                        f"  [{node_ip}] {category} {file_path}: {details}"
                    )
        if failures:
            self.log.error(
                f"{label}GSI encryption validation found {len(failures)} failure(s)"
            )
        return failures

    def _validate_indexer_keys_in_use_endpoint(self, label=""):
        """Check indexer GetInUseKeys on every index node before post-upgrade enablement.

        On 8.0->8.1 paths that had bucket encryption enabled pre-upgrade, the
        endpoint must report at least one in-use key on every index node. The
        method never fails the test directly; it returns a list of error strings
        so the caller can defer the failure until the end of the run.
        """
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True,
        )
        errors = []
        for node in index_nodes:
            try:
                status, response = RestConnection(node).get_indexer_in_use_encryption_keys()
            except Exception as e:
                msg = f"{label}{node.ip}: get_indexer_in_use_encryption_keys failed: {e}"
                self.log.error(msg)
                errors.append(msg)
                continue

            if not status:
                msg = (
                    f"{label}{node.ip}: get_indexer_in_use_encryption_keys returned "
                    f"status=False: {response}"
                )
                self.log.error(msg)
                errors.append(msg)
                continue

            key_ids = set()
            self._extract_indexer_key_ids(response, key_ids)
            resolved = sorted(key_ids)
            self.log.info(
                f"{label}Indexer in-use key IDs on {node.ip}: {resolved or '[]'}"
            )

            if self.initial_supports_bucket_ear and self.target_supports_file_ear and not resolved:
                msg = (
                    f"{label}{node.ip}: bucket encryption was enabled before upgrade "
                    "but GetInUseKeys returned no key IDs after reaching 8.1"
                )
                self.log.error(msg)
                errors.append(msg)

        return errors

    def _restart_indexer_on_all_nodes(self, label=""):
        """Restart indexer service on every index node at the very end of the test."""
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True,
        )
        errors = []
        for node in index_nodes:
            remote = RemoteMachineShellConnection(node, verbose=False)
            try:
                self.log.info(f"{label}Restarting indexer on {node.ip}")
                remote.stop_indexer()
                self.sleep(5, f"{label}Waiting for indexer to stop on {node.ip}")
                remote.start_indexer()
                self.sleep(10, f"{label}Waiting for indexer to restart on {node.ip}")
            except Exception as e:
                msg = f"{label}{node.ip}: indexer restart failed: {e}"
                self.log.error(msg)
                errors.append(msg)
            finally:
                remote.disconnect()
        return errors

    # ------------------------------------------------------------------ spill file validation

    def _verify_spill_files_encrypted(self, query_nodes, label=""):
        """Find av_spill_* / scan-results* files produced by ORDER-BY spill workloads
        on each query node and assert every file carries the Couchbase Encrypted
        magic bytes. This exercises the 'other encryption' path (8.1+).

        Failures are hard-errors. If no spill files are found on any node a warning
        is logged but the method does not fail (the workload may not have created
        files large enough to spill to disk on this cluster)."""
        search_dirs = [
            "/opt/couchbase/var/lib/couchbase/query/backfill",
            "/tmp",
        ]
        patterns = ["av_spill_*", "scan-results*"]

        all_failures = []
        total_checked = 0

        for node in query_nodes:
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                spill_files = []
                for d in search_dirs:
                    for pat in patterns:
                        out, _ = shell.execute_command(
                            f"find {d} -maxdepth 3 -name '{pat}' -type f 2>/dev/null"
                        )
                        spill_files.extend(l.strip() for l in out if l.strip())
            finally:
                shell.disconnect()

            if not spill_files:
                self.log.warning(
                    f"{label}{node.ip}: no spill/backfill files found — "
                    "other encryption not verified for this node"
                )
                continue

            self.log.info(
                f"{label}{node.ip}: checking {len(spill_files)} spill/backfill file(s)"
            )
            for fpath in spill_files:
                total_checked += 1
                is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                    node, fpath
                )
                if is_enc:
                    self.log.debug(f"{label}{node.ip} {fpath}: encrypted OK")
                else:
                    all_failures.append(f"  {node.ip} {fpath}: NOT encrypted: {details}")

        if all_failures:
            self.fail(
                f"{label}Spill/backfill files not encrypted "
                "(other encryption regression):\n" + "\n".join(all_failures)
            )
        if total_checked == 0:
            self.log.warning(
                f"{label}No spill/backfill files found on any query node — "
                "other encryption path not exercised"
            )
        else:
            self.log.info(
                f"{label}{total_checked} spill/backfill file(s) encrypted OK"
            )

    # ------------------------------------------------------------------ post-upgrade

    def _validate_full_post_upgrade(self, pre_index_identity, encrypted_buckets,
                                    namespaces, baseline_queries):
        """All-nodes-upgraded validation."""
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True,
        )
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True,
        )

        # 1. Index identity diff (no indexes lost).
        # After an offline upgrade the N1QL node restarts and must re-sync the KV
        # collection-manifest cache before non-default scopes become visible to
        # system:indexes. wait_until_indexes_online alone is not sufficient — if the
        # scope is not yet visible it returns immediately on a partial view.
        # The 120 s sleep gives the manifest cache time to warm up first.
        self.sleep(120, "[post-upgrade] Waiting for KV collection-manifest cache to sync "
                        "before checking index status")
        try:
            self.wait_until_indexes_online(timeout=600)
        except Exception as e:
            self.log.warning(f"[post-upgrade] wait_until_indexes_online: {e}")
        post_index_identity = self._capture_index_identity_set()
        missing = pre_index_identity - post_index_identity
        if missing:
            self.fail(f"[post-upgrade] {len(missing)} indexes lost: {sorted(missing)}")
        self.log.info(
            f"[post-upgrade] index_map OK ({len(post_index_identity)} entries, 0 lost)"
        )

        # 2. Baseline scans still succeed.
        self._run_select_scans(baseline_queries, query_nodes)
        self.log.info("[post-upgrade] baseline scans OK")

        # 2.5 Pre-flight: poll GetInUseKeys (bucket / log / other — whichever
        # categories are enabled) until no empty key IDs remain anywhere.
        # Validating files while any store still has an empty key would race the
        # encryptor and produce spurious "NOT encrypted" failures.
        self._wait_for_no_empty_keys_in_use(
            index_nodes, query_nodes, encrypted_buckets,
            timeout=1200, label="[post-upgrade pre-flight] ",
        )

        # 3. GSI file encryption — full sweep across all index nodes.
        # STEP 7c already triggered force re-encryption, so ALL index files on disk
        # (including those created before encryption was enabled on pre-8.0 upgrade
        # paths) have been re-encrypted with the current active key IDs.
        index_keys = self._get_indexer_in_use_key_ids(index_nodes)
        self.assertGreater(
            len(index_keys), 0,
            "[post-upgrade] no indexer in-use key IDs found — encryption not active?",
        )
        validation_failures = []
        gsi_failures = self._collect_gsi_encryption_failures(
            index_nodes, encrypted_buckets, index_keys, query_node=query_nodes[0] if query_nodes else None,
            label="[post-upgrade] "
        )
        validation_failures.extend(f"[GSI] {line}" for line in gsi_failures)
        if not gsi_failures:
            self.log.info(
                f"[post-upgrade] GSI encryption validation PASSED with keys={index_keys}"
            )

        # 4. Create post-upgrade indexes and run queries.
        self.log.info("[post-upgrade] Creating post-upgrade indexes")
        post_upgrade_queries = self._create_post_upgrade_indexes(query_nodes)
        all_queries = list(baseline_queries) + list(post_upgrade_queries)
        self._run_select_scans(all_queries, query_nodes)
        self.log.info("[post-upgrade] Post-upgrade index scans OK")

        # 5. Query artifact encryption checks.
        #
        # Two distinct goals, sequenced deliberately:
        #   5a. Pre-existing files: STEP 7c triggered a force re-encryption of all
        #       query log and FFDC files that existed before or during the upgrade.
        #       Verify *those* files are encrypted here — before generating new ones —
        #       so a re-encryption failure is caught separately from a new-file failure.
        #   5b-5e. New files: generate a fresh batch of completed-requests, history,
        #       spill and FFDC files and assert every newly created file is encrypted,
        #       confirming the cluster continues to encrypt under normal write paths.

        self.log.info(
            "[post-upgrade] 5a: Verifying pre-existing query log/FFDC files are encrypted "
            "(STEP 7c force re-encryption guarantee)"
        )
        query_keys = self._get_query_in_use_key_ids()
        query_failures = self._collect_query_encryption_failures(
            query_nodes, query_keys, label="[post-upgrade] "
        )
        validation_failures.extend(f"[Query] {line}" for line in query_failures)
        if not query_failures:
            self.log.info("[post-upgrade] Query log/FFDC file encryption PASSED")

        # 5b. Snapshot the current set of file paths so new-file validation can
        #     isolate only the files generated in the steps below.
        pre_load_log_snap = self._snapshot_query_log_file_paths(query_nodes)
        pre_ffdc_snap = self._snapshot_ffdc_file_paths(query_nodes)

        # 5c. Generate completed-requests / history / spill files.
        self.log.info("[post-upgrade] 5c: Generating new query artifacts")
        self._set_query_completed_settings(stream_size=500, threshold=0)
        self.log.info("[post-upgrade] 5c: Running 5-minute background query load to fill rlstream...")
        bg_thread = self._dispatch_background_query_batch(
            all_queries, query_nodes, workers=5
        )
        self.sleep(300, "Running background queries for 5 minutes")
        bg_thread.join(timeout=30)
        self.log.info("[post-upgrade] 5c: Background load complete, setting completed-threshold=720000...")
        self._set_query_completed_settings(stream_size=500, threshold=720000)
        self.sleep(720, "Waiting 12 minutes for local_request_log.* files to be created")
        self.log.info("[post-upgrade] 5c: Wait complete - local_request_log.* should be created")
        if namespaces:
            self._run_spill_and_backfill_workload(query_nodes, namespaces[0])

        # 5d. Validate spill / backfill files — exercises the 'other encryption' path.
        self.log.info(
            "[post-upgrade] 5d: Validating spill/backfill files (other encryption)"
        )
        try:
            self._verify_spill_files_encrypted(query_nodes, label="[post-upgrade] ")
        except Exception as e:
            validation_failures.append(f"[Query] spill/backfill files: {e}")

        # Refresh key IDs after load generation in case a DEK rotation occurred.
        query_keys = self._get_query_in_use_key_ids()

        # 5e. Trigger FFDC and wait for new files to appear (not in pre_ffdc_snap).
        new_ffdc_results = self._trigger_ffdc_and_wait_for_new(
            query_nodes, pre_ffdc_snap, query_keys, label="[post-upgrade] ",
        )

        # 5f. Validate new rlstream.* / local_request_log.* files.
        self.log.info("[post-upgrade] 5f: Validating new query log files")
        for node in query_nodes:
            existing = pre_load_log_snap.get(node.ip, set())
            ids = query_keys.get(node.ip, [])
            try:
                # Watcher variant: polls for rlstream/local_request_log files
                # for ~30s and validates each as soon as it appears, marking
                # files that vanish or are empty mid-check as transient. This
                # avoids a TOCTOU failure when rlstream.* gets rotated between
                # find and xxd.
                res = self.encryption_helper.verify_query_log_files_encrypted_with_watcher(
                    [node], ids, watch_seconds=30, poll_interval=3,
                )
            except Exception as e:
                validation_failures.append(
                    f"[Query] {node.ip}: verify_query_log_files failed: {e}"
                )
                continue
            node_result = res.get(node.ip, {}) if isinstance(res, dict) else {}
            failures = []
            new_files = 0
            for cat in ("rlstream", "local_request_log"):
                for fpath, r in node_result.get(cat, {}).items():
                    if fpath in existing:
                        continue
                    if r.get("transient"):
                        continue
                    new_files += 1
                    if not r.get("encrypted"):
                        failures.append(
                            f"  {cat} {fpath}: NOT encrypted: {r.get('details')}"
                        )
                    elif ids and not r.get("key_id_found", True):
                        failures.append(
                            f"  {cat} {fpath}: header missing key ID {ids}"
                        )
            if failures:
                validation_failures.extend(
                    f"[Query] {node.ip}: {line}" for line in failures
                )
            if new_files == 0:
                self.log.warning(
                    f"[post-upgrade] {node.ip}: no new rlstream/local_request_log "
                    "files found to validate"
                )
            else:
                self.log.info(
                    f"[post-upgrade] {node.ip}: {new_files} new query log file(s) "
                    f"encrypted OK with key IDs {ids}"
                )

        # 5g. Validate new FFDC files.
        self.log.info("[post-upgrade] 5g: Validating new FFDC files")
        total_new_ffdc = 0
        ffdc_failures = []
        for node in query_nodes:
            node_new = new_ffdc_results.get(node.ip, {})
            for fpath, r in node_new.items():
                total_new_ffdc += 1
                if not r.get("encrypted"):
                    ffdc_failures.append(
                        f"  {node.ip} {fpath}: NOT encrypted: {r.get('details')}"
                    )
                elif query_keys.get(node.ip) and not r.get("key_id_found", True):
                    ffdc_failures.append(
                        f"  {node.ip} {fpath}: header missing key ID "
                        f"{query_keys[node.ip]}"
                    )
        if ffdc_failures:
            validation_failures.extend(f"[Query] {line}" for line in ffdc_failures)
        self.assertGreater(
            total_new_ffdc, 0,
            "[post-upgrade] No new FFDC files produced after post-upgrade trigger",
        )
        self.log.info(
            f"[post-upgrade] {total_new_ffdc} new FFDC file(s) encrypted OK"
        )
        self.log.info("[post-upgrade] Post-upgrade query artifact encryption PASSED")

        if validation_failures:
            # Extract bare file paths from failure lines so the failed-files
            # set is easy to spot in CI logs without parsing the full diag.
            failed_paths = []
            seen_paths = set()
            for line in validation_failures:
                for tok in line.split():
                    if tok.startswith("/") and tok not in seen_paths:
                        # Trim trailing punctuation like ':' that follows paths
                        clean = tok.rstrip(":,;")
                        if clean and clean not in seen_paths:
                            seen_paths.add(clean)
                            failed_paths.append(clean)
            header = [
                "[post-upgrade] Encryption validation FAILED",
                f"  Failed file count: {len(failed_paths)}",
                "  Failed files:",
            ]
            if failed_paths:
                header.extend(f"    - {p}" for p in failed_paths)
            else:
                header.append("    (no file paths parsed from failures)")
            header.append("")
            header.append("Full failure details (GSI + Query):")
            self.fail("\n".join(header) + "\n" + "\n".join(validation_failures))

    # ------------------------------------------------------------------ test runner

    def _run_upgrade_test(self, mode):
        encrypted_buckets = [self.test_bucket]
        deferred_failures = []

        # STEP 1-2: Cluster preparation
        self.log.info("=" * 80)
        self.log.info(f"[STEP 1] Preparing cluster (initial_version={self.initial_version})")
        baseline_queries, namespaces = self._prepare_cluster(
            enable_bucket_encryption_now=self.initial_supports_bucket_ear,
            enable_log_encryption_now=self.initial_supports_bucket_ear,
        )
        query_nodes_initial = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True,
        )

        # STEP 2.5: Conditional vector bucket (dense BHIVE indexes, 8.0+ only)
        if self._version_tuple(self.initial_version) >= (8, 0):
            self.log.info("[STEP 2.5] Creating vector bucket with dense BHIVE indexes")
            vector_namespaces, vector_queries, vector_bucket = self._prepare_vector_bucket()
            encrypted_buckets.append(vector_bucket)
            baseline_queries.extend(vector_queries)
            namespaces.extend(vector_namespaces)
        else:
            self.log.info(
                "[STEP 2.5] Skipping vector bucket (initial version < 8.0)"
            )

        # STEP 3: Query settings + drive workload to produce rlstream + history + spill + FFDC
        self.log.info("[STEP 3] Driving pre-upgrade query workload")
        self._set_query_completed_settings(stream_size=500, threshold=0)
        self.log.info("[STEP 3] Running 5-minute background query load to fill rlstream...")
        bg_thread = self._dispatch_background_query_batch(
            baseline_queries, query_nodes_initial, workers=5
        )
        self.sleep(300, "Running background queries for 5 minutes")
        bg_thread.join(timeout=30)
        self.log.info("[STEP 3] Background load complete, setting completed-threshold=720000...")
        self._set_query_completed_settings(stream_size=500, threshold=720000)
        self.sleep(720, "Waiting 12 minutes for local_request_log.* files to be created")
        self.log.info("[STEP 3] Wait complete - local_request_log.* should be created")
        self._run_spill_and_backfill_workload(query_nodes_initial, namespaces[0])
        pre_query_keys = (
            self._get_query_in_use_key_ids() if self.initial_supports_file_ear else {}
        )
        self._trigger_ffdc_and_poll(
            query_nodes_initial,
            expected_key_ids=pre_query_keys,
            label="[pre-upgrade] ",
        )

        pre_upgrade_failures = []
        if self.initial_supports_file_ear and self.log_encryption_at_rest_id:
            self.log.info(
                "[STEP 3a] Pre-upgrade: validating request log and FFDC encryption"
            )
            pre_upgrade_failures.extend(
                f"[Query] {line}"
                for line in self._collect_query_encryption_failures(
                    query_nodes_initial, pre_query_keys, label="[STEP 3a] "
                )
            )
        else:
            self.log.info(
                "[STEP 3a] Pre-upgrade query file encryption check skipped — "
                "file-level query encryption is not supported before 8.1"
            )

        # STEP 4: Pre-upgrade GSI file validation
        # need to check for bucket encryption being enabled for releases > 8.1
        if self.initial_supports_file_ear:
            index_nodes_initial = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True,
            )
            pre_index_keys = self._get_indexer_in_use_key_ids(index_nodes_initial)
            self.log.info(
                f"[STEP 4] Pre-upgrade: file EAR supported — validating GSI files "
                f"are encrypted with {pre_index_keys}"
            )
            pre_upgrade_failures.extend(
                f"[GSI] {line}"
                for line in self._collect_gsi_encryption_failures(
                    index_nodes_initial, encrypted_buckets, pre_index_keys,
                    query_node=query_nodes_initial[0] if query_nodes_initial else None,
                    label="[STEP 4] ",
                )
            )
        else:
            self.log.info(
                f"[STEP 4] Pre-upgrade: cluster on {self.initial_version} — file EAR not "
                f"supported; expecting PLAINTEXT (mixed-mode + post-upgrade checks will assert)"
            )

        if pre_upgrade_failures:
            self.fail(
                "[pre-upgrade] Encryption validation FAILED — both GSI and Query "
                "files are listed below:\n" + "\n".join(pre_upgrade_failures)
            )

        # STEP 5: Snapshot index identities and item counts for all namespaces
        pre_index_identity = self._capture_index_identity_set()
        pre_item_counts = self._capture_item_counts()

        # STEP 6: Upgrade nodes one by one with mixed-mode checkpoint in the middle
        kv_nodes = self.get_nodes_from_services_map(
            service_type="kv", get_all_nodes=True,
        )
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True,
        )
        n1ql_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True,
        )
        self.assertGreaterEqual(len(kv_nodes), 2, "Need >=2 KV nodes")
        self.assertGreaterEqual(len(index_nodes), 2, "Need >=2 index nodes")
        self.assertGreaterEqual(len(n1ql_nodes), 2, "Need >=2 N1QL nodes")

        # Upgrade order:
        #   1. All KV nodes (no EAR file semantics; upgrade them all before the checkpoint)
        #   2. First index + first N1QL node  → then take the mixed-mode checkpoint
        #   3. All remaining index + N1QL nodes
        # Using slices (not literal [1]) so clusters with >2 nodes per service are covered.
        pre_mixed = list(kv_nodes) + [index_nodes[0], n1ql_nodes[0]]
        post_mixed = list(index_nodes[1:]) + list(n1ql_nodes[1:])

        self.log.info(f"[STEP 6] Upgrading pre-mixed-mode nodes: {[n.ip for n in pre_mixed]}")
        for node in pre_mixed:
            self._upgrade_node(node, mode)

        # Mixed-mode checkpoint — only interesting when target supports file EAR but
        # initial does not (i.e. 8.0 -> 8.1, or pre-8.0 -> 8.1).
        if self.target_supports_file_ear and not self.initial_supports_file_ear:
            if self.initial_supports_bucket_ear:
                # Bucket encryption was on the whole time — clean mixed-mode case.
                self.log.info(
                    "[STEP 6.5] Mixed-mode checkpoint (bucket EAR pre-upgrade, file EAR mid-flight)"
                )
                mixed_errors = self._validate_mixed_mode(
                    upgraded_index_nodes=[index_nodes[0]],
                    pending_index_nodes=list(index_nodes[1:]),
                    upgraded_query_nodes=[n1ql_nodes[0]],
                    pending_query_nodes=list(n1ql_nodes[1:]),
                    encrypted_buckets=encrypted_buckets,
                )
                deferred_failures.extend(mixed_errors)
            else:
                self.log.info(
                    "[STEP 6.5] Mixed-mode checkpoint skipped — bucket encryption not enabled "
                    "yet (came from pre-8.0); will enable post-upgrade and validate at STEP 8"
                )
        else:
            self.log.info(
                "[STEP 6.5] Mixed-mode checkpoint skipped — version combo not interesting "
                f"(initial_file_ear={self.initial_supports_file_ear}, "
                f"target_file_ear={self.target_supports_file_ear})"
            )
        self.log.info(f"[STEP 6] Upgrading post-mixed-mode nodes: {[n.ip for n in post_mixed]}")
        for node in post_mixed:
            self._upgrade_node(node, mode)

        self.log.info("[STEP 6.8] Manual-check pause complete; resuming automated validation")

        # STEP 6.9: keys-in-use endpoint check on 8.1 before any post-upgrade
        # enablement. On 7.x->8.1 paths encryption is still off here; on 8.0->8.1
        # paths log encryption was on from the start. Either way the endpoint must
        # report a coherent answer on every query and index node now that they're on 8.1.
        if self.target_supports_file_ear:
            self._validate_query_keys_in_use_endpoint(
                label="[STEP 6.9 pre-enablement] ",
            )
            deferred_failures.extend(
                self._validate_indexer_keys_in_use_endpoint(
                    label="[STEP 6.9 pre-enablement] ",
                )
            )

        # STEP 7a: Enable bucket + log encryption for pre-8.0 starting versions
        # (e.g. 7.2→8.1 or 7.6→8.1). Both must be active before the post-upgrade
        # new-file validation can assert that query log files are encrypted.
        if not self.initial_supports_bucket_ear:
            self.log.info(
                f"[STEP 7a] Enabling bucket and log encryption post-upgrade "
                f"(cluster on {self.upgrade_to})"
            )
            self._create_kek()
            self._enable_bucket_encryption(self.test_bucket, self.encryption_key_id)
            self._enable_log_encryption_at_rest()
            self.sleep(60, "Wait for indexer + query to start using the new keys")

        # STEP 7b: Enable other encryption at rest on all upgrade paths that land on
        # 8.1+. This is a new 8.1 feature required for spill / backfill file
        # encryption and is not enabled during cluster preparation regardless of the
        # initial version.
        if self.target_supports_file_ear:
            self.log.info(
                "[STEP 7b] Enabling other encryption at rest "
                "(required for spill/backfill file encryption)"
            )
            self._enable_other_encryption_at_rest()
            self.sleep(30, "Wait for other encryption to apply")

        # STEP 7c: Force re-encrypt everything now that all nodes are on 8.1+ and
        # all encryption types are active. This guarantees that index files and log
        # files created before encryption was enabled (pre-8.0 upgrade paths) are
        # re-encrypted on disk before the final validation runs.
        if self.target_supports_file_ear:
            self.log.info(
                "[STEP 7c] Force re-encrypting all GSI index files and query log files"
            )
            index_nodes_reenc = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True,
            )
            query_nodes_reenc = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True,
            )
            self._force_reencrypt_bucket_and_wait(
                self.test_bucket, index_nodes_reenc,
                timeout=300, label="[STEP 7c] ",
            )
            self._force_reencrypt_logs_and_wait(
                query_nodes_reenc, timeout=300, label="[STEP 7c] ",
            )
            self._force_reencrypt_other_and_wait(
                query_nodes_reenc, timeout=300, label="[STEP 7c] ",
            )
            self.log.info(
                "[STEP 7c] Re-encryption triggered — waiting for all GSI stores "
                "to reach encryption_status='encrypted'"
            )
            self._wait_for_full_gsi_encryption(
                index_nodes_reenc, encrypted_buckets,
                timeout=1200, label="[STEP 7c] ",
            )
            self.log.info("[STEP 7c] Force re-encryption complete")

        # STEP 7d: keys-in-use endpoint check on 8.1 after encryption is fully on.
        # Every query node must now report non-empty key IDs matching the active
        # log-encryption key.
        if self.target_supports_file_ear:
            self._validate_query_keys_in_use_endpoint(
                label="[STEP 7d post-enablement] ",
            )

        # STEP 8: Full post-upgrade validation
        self.log.info("[STEP 8] Running full post-upgrade validation")
        try:
            self._validate_full_post_upgrade(
                pre_index_identity, encrypted_buckets, namespaces, baseline_queries,
            )
        except AssertionError as e:
            deferred_failures.append(f"[STEP 8] post-upgrade validation failed: {e}")
        except Exception as e:
            deferred_failures.append(f"[STEP 8] post-upgrade validation error: {e}")

        # STEP 8.5: Validate item counts for all namespaces
        self.log.info("[STEP 8.5] Validating item counts across upgrade")
        try:
            self._validate_item_counts(pre_item_counts)
        except AssertionError as e:
            deferred_failures.append(f"[STEP 8.5] item count mismatch: {e}")
        except Exception as e:
            deferred_failures.append(f"[STEP 8.5] item count validation error: {e}")

        # STEP 9: Final indexer restart on every node before test exit/failure.
        restart_errors = self._restart_indexer_on_all_nodes(
            label="[STEP 9 final restart] ",
        )
        deferred_failures.extend(restart_errors)

        if deferred_failures:
            failure_summary = "\n".join(deferred_failures)
            self.fail(
                f"[DONE] {mode} upgrade test FAILED — "
                f"{len(deferred_failures)} mixed-mode failure(s):\n{failure_summary}"
            )
        self.log.info(f"[DONE] {mode} upgrade test PASSED")

    # ------------------------------------------------------------------ simple upgrade with workload

    def _run_simple_upgrade_with_workload(self, mode):
        """Simplified upgrade test with continuous KV mutations and query workload.

        Creates a single bucket, loads docs, builds scalar + composite + BHIVE
        indexes, then upgrades all cluster nodes while running continuous kv
        mutations and query scans in background threads.  Skips every
        encryption validation — this is purely a workload resilience test.

        Args:
            mode: 'offline' or 'online' — passed through to _upgrade_node.
        """
        self.log.info("=" * 80)
        self.log.info(
            f"STARTING TEST: simple {mode} upgrade with background workload "
            f"({self.initial_version} -> {self.upgrade_to})"
        )
        self.log.info("=" * 80)

        # -- bucket + data -------------------------------------------------
        self.log.info("[STEP 1] Creating bucket and loading data...")
        self.bucket_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size,
            replicas=self.num_replicas, bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
        )
        self.cluster.create_standard_bucket(
            name=self.test_bucket, port=11222, bucket_params=self.bucket_params,
        )
        self.sleep(10, "Wait after bucket creation")

        self.prepare_collection_for_indexing(
            num_scopes=1, num_collections=1,
            num_of_docs_per_collection=self.num_of_docs_per_collection,
            json_template=self.json_template,
            bucket_name=self.test_bucket,
            load_default_coll=True,
        )
        self.sleep(10)

        # -- indexes -------------------------------------------------------
        self.log.info("[STEP 2] Creating indexes...")
        select_queries = set()

        scalar_queries = self.create_index_in_batches(
            num_batches=1, replica_count=1, scalar=True,
            dataset=self.json_template, bhive=False,
        )
        select_queries.update(scalar_queries)

        if self._version_tuple(self.initial_version) >= (8, 0):
            composite_queries = self.create_index_in_batches(
                num_batches=1, replica_count=1, scalar=False,
                dataset=self.json_template, bhive=False,
            )
            bhive_queries = self.create_index_in_batches(
                num_batches=1, replica_count=1, scalar=False,
                dataset=self.json_template, bhive=True,
                skip_extra_indexes=False,
            )
            select_queries.update(composite_queries)
            select_queries.update(bhive_queries)

        self.wait_until_indexes_online()
        self.sleep(120)

        # -- continuous workload + upgrade --------------------------------
        self.log.info("[STEP 3] Starting continuous scan + mutation threads...")
        event = threading.Event()
        self.run_continous_query = True
        scan_thread = threading.Thread(
            target=self._run_queries_continously,
            args=(select_queries, False),
            name="simple_upg_scan",
        )
        scan_thread.start()

        mutation_thread = threading.Thread(
            target=self.perform_continuous_kv_mutations,
            args=(event,),
            kwargs={"timeout": 4800},
            name="simple_upg_mutation",
        )
        mutation_thread.start()

        try:
            self.log.info(
                f"[STEP 4] Upgrading all nodes ({mode})..."
            )
            kv_nodes = self.get_nodes_from_services_map(
                service_type="kv", get_all_nodes=True,
            )
            index_nodes = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True,
            )
            n1ql_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True,
            )
            for node in list(kv_nodes) + list(index_nodes) + list(n1ql_nodes):
                self._upgrade_node(node, mode)

            self.update_master_node()
        finally:
            self.run_continous_query = False
            event.set()
            scan_thread.join()
            mutation_thread.join()

        # -- post-upgrade validation --------------------------------------
        self.log.info("[STEP 5] Post-upgrade validation...")
        self.wait_until_indexes_online()

        self.create_index_in_batches(
            num_batches=1, replica_count=1, scalar=True,
            dataset=self.json_template,
        )
        self.wait_until_indexes_online()

        self._run_select_scans(select_queries)

        # -- restart indexer on all nodes ----------------------------------
        try:
            self.log.info("[STEP 6] Killing indexer process on all index nodes...")
            self._kill_indexer_on_all_nodes(step_prefix="[STEP 6] ")
            self.log.info("[STEP 6] PASSED - Indexer processes killed on all nodes")
        except Exception as e:
            self.log.warning(f"[STEP 6] WARNING - Failed to kill indexer on some nodes: {str(e)}")

        self.log.info(
            f"[DONE] simple {mode} upgrade test with workload PASSED "
            f"({self.initial_version} -> {self.upgrade_to})"
        )

    # ------------------------------------------------------------------ tests

    def test_offline_upgrade_with_encryption_at_rest(self):
        """Offline upgrade: per-node stop_server -> install -> start. Cluster sees the
        node bounce; remaining nodes serve traffic. Mixed-mode checkpoint between the
        first GSI/N1QL upgrade and the second."""
        if self.input.param("simple_upgrade_with_workload", False):
            self._run_simple_upgrade_with_workload(mode='offline')
            return
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_offline_upgrade_with_encryption_at_rest")
        self.log.info("=" * 80)
        self.dcp_rebalance = self.input.param("dcp_rebalance", False)
        if self.dcp_rebalance:
            self.disable_shard_based_rebalance()
            self.sleep(10)
        self._run_upgrade_test(mode='offline')

    def test_online_upgrade_with_encryption_at_rest(self):
        """Online upgrade: per-node rebalance-out -> install -> rebalance-in (with the
        same service list). Mixed-mode checkpoint between the first GSI/N1QL upgrade
        and the second."""
        if self.input.param("simple_upgrade_with_workload", False):
            self._run_simple_upgrade_with_workload(mode='online')
            return
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_online_upgrade_with_encryption_at_rest")
        self.log.info("=" * 80)
        self.dcp_rebalance = self.input.param("dcp_rebalance", False)
        if self.dcp_rebalance:
            self.disable_shard_based_rebalance()
            self.sleep(10)
        self._run_upgrade_test(mode='online')
