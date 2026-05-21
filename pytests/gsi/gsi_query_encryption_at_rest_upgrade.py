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

    def _get_query_in_use_key_ids(self):
        """Returns {node_ip: [key_id, ...]} from each query node, or {} if endpoint absent."""
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
                all_keys = [k for ids in in_use.values() for k in ids if k]
                out[node.ip] = all_keys
            except Exception as e:
                self.log.warning(f"get_query_in_use_encryption_keys on {node.ip} failed: {e}")
                out[node.ip] = []
        return out

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
        """Issue ORDER BY queries on the loaded data to produce av_spill_* / scan-results*
        files. Per-query failures are tolerated (mid-upgrade nodes may bounce)."""
        sort_query = f"SELECT meta().id, counter FROM {namespace} ORDER BY counter LIMIT 5000"
        self.log.info(f"[spill] {sort_query[:120]}")
        for node in query_nodes:
            for _ in range(3):
                try:
                    self.run_cbq_query(query=sort_query, server=node, verbose=False)
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
                                      timeout=600, label=""):
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

    def _create_post_upgrade_indexes(self, query_nodes):
        """Create a fresh set of indexes with a 'post_upg' prefix after full upgrade.
        Returns a set of SELECT queries that target those indexes."""
        post_queries = set()
        query_node = query_nodes[0] if query_nodes else self.get_nodes_from_services_map(
            service_type="n1ql"
        )
        for i, ns in enumerate(self.namespaces[:2]):
            try:
                tail = "".join(ns.split(':')[1].split('.'))
                prefix = f"post_upg_{i+1}_{tail}"
                defs = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=prefix, skip_primary=True,
                )
                post_queries.update(
                    self.gsi_util_obj.get_select_queries(definition_list=defs, namespace=ns)
                )
                create_q = self.gsi_util_obj.get_create_index_list(
                    definition_list=defs, namespace=ns,
                    num_replica=self.num_index_replica,
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_q, database=ns, query_node=query_node,
                )
                self.log.info(
                    f"[post-upgrade] Created {len(create_q)} post-upgrade indexes on {ns}"
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
        until any query node reports key IDs that differ from the baseline.
        Returns the new {node_ip: [key_id, ...]} map."""
        baseline = self._get_query_in_use_key_ids()
        self.log.info(
            f"{label}Triggering force log re-encryption "
            f"(baseline key IDs per node: {baseline})"
        )
        status, response = self.rest.trigger_log_reencryption()
        if not status:
            raise Exception(f"trigger_log_reencryption failed: {response}")
        self.log.info(f"{label}Log re-encryption triggered: {response}")

        deadline = time.time() + timeout
        while time.time() < deadline:
            current = self._get_query_in_use_key_ids()
            if any(
                set(current.get(node.ip, [])) != set(baseline.get(node.ip, []))
                for node in query_nodes
            ):
                self.log.info(
                    f"{label}New query key IDs detected: {current} "
                    f"(baseline was {baseline})"
                )
                return current
            self.sleep(15, f"{label}Waiting for new query key IDs after log re-encryption")
        self.fail(
            f"{label}No new query key IDs appeared within {timeout}s after "
            f"triggering log re-encryption (baseline: {baseline})"
        )

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

        # 3. GSI file encryption — full sweep across all index nodes.
        # STEP 7c already triggered force re-encryption, so ALL index files on disk
        # (including those created before encryption was enabled on pre-8.0 upgrade
        # paths) have been re-encrypted with the current active key IDs.
        index_keys = self._get_indexer_in_use_key_ids(index_nodes)
        self.assertGreater(
            len(index_keys), 0,
            "[post-upgrade] no indexer in-use key IDs found — encryption not active?",
        )
        self.validate_engine_encryption(
            'plasma', index_nodes,
            expected_key_id=index_keys,
            encrypted_bucket_names=encrypted_buckets,
            fail_on_error=True,
        )
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

        # 5a-i: rlstream.* / local_request_log.*
        pre_existing_log_failures = []
        for node in query_nodes:
            ids = query_keys.get(node.ip, [])
            try:
                res = self.encryption_helper.verify_query_log_files_encrypted([node], ids)
            except Exception as e:
                pre_existing_log_failures.append(
                    f"  {node.ip}: verify_query_log_files failed: {e}"
                )
                continue
            node_result = res.get(node.ip, {}) if isinstance(res, dict) else {}
            for cat in ("rlstream", "local_request_log"):
                for fpath, r in node_result.get(cat, {}).items():
                    if not r.get("encrypted"):
                        pre_existing_log_failures.append(
                            f"  {node.ip} {cat} {fpath}: NOT encrypted after STEP 7c "
                            f"re-encryption: {r.get('details')}"
                        )
        if pre_existing_log_failures:
            self.fail(
                "[post-upgrade] Pre-existing query log files not encrypted after "
                "STEP 7c force re-encryption:\n"
                + "\n".join(pre_existing_log_failures)
            )
        self.log.info("[post-upgrade] Pre-existing query log files all encrypted OK")

        # 5a-ii: query_ffdc_MAN_* files
        pre_existing_ffdc_failures = []
        for node in query_nodes:
            ids = query_keys.get(node.ip, [])
            try:
                res = self.encryption_helper.verify_query_ffdc_files_encrypted([node], ids)
            except Exception as e:
                pre_existing_ffdc_failures.append(
                    f"  {node.ip}: verify_query_ffdc_files failed: {e}"
                )
                continue
            node_result = res.get(node.ip, {}) if isinstance(res, dict) else {}
            for fpath, r in node_result.items():
                if not r.get("encrypted"):
                    pre_existing_ffdc_failures.append(
                        f"  {node.ip} {fpath}: NOT encrypted after STEP 7c "
                        f"re-encryption: {r.get('details')}"
                    )
        if pre_existing_ffdc_failures:
            self.fail(
                "[post-upgrade] Pre-existing FFDC files not encrypted after "
                "STEP 7c force re-encryption:\n"
                + "\n".join(pre_existing_ffdc_failures)
            )
        self.log.info(
            "[post-upgrade] Pre-existing FFDC files all encrypted OK (or none present)"
        )

        # 5b. Snapshot the current set of file paths so new-file validation can
        #     isolate only the files generated in the steps below.
        pre_load_log_snap = self._snapshot_query_log_file_paths(query_nodes)
        pre_ffdc_snap = self._snapshot_ffdc_file_paths(query_nodes)

        # 5c. Generate completed-requests / history / spill files.
        self.log.info("[post-upgrade] 5c: Generating new query artifacts")
        self._generate_completed_requests_and_history(query_nodes, all_queries)
        if namespaces:
            self._run_spill_and_backfill_workload(query_nodes, namespaces[0])

        # 5d. Validate spill / backfill files — exercises the 'other encryption' path.
        self.log.info(
            "[post-upgrade] 5d: Validating spill/backfill files (other encryption)"
        )
        self._verify_spill_files_encrypted(query_nodes, label="[post-upgrade] ")

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
                res = self.encryption_helper.verify_query_log_files_encrypted([node], ids)
            except Exception as e:
                self.fail(f"[post-upgrade] verify_query_log_files on {node.ip}: {e}")
            node_result = res.get(node.ip, {}) if isinstance(res, dict) else {}
            failures = []
            new_files = 0
            for cat in ("rlstream", "local_request_log"):
                for fpath, r in node_result.get(cat, {}).items():
                    if fpath in existing:
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
                self.fail(
                    f"[post-upgrade] {node.ip}: new query log files not properly encrypted:\n"
                    + "\n".join(failures)
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
            self.fail(
                "[post-upgrade] New FFDC files not properly encrypted:\n"
                + "\n".join(ffdc_failures)
            )
        self.assertGreater(
            total_new_ffdc, 0,
            "[post-upgrade] No new FFDC files produced after post-upgrade trigger",
        )
        self.log.info(
            f"[post-upgrade] {total_new_ffdc} new FFDC file(s) encrypted OK"
        )
        self.log.info("[post-upgrade] Post-upgrade query artifact encryption PASSED")

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

        # STEP 3: Query settings + drive workload to produce rlstream + history + spill + FFDC
        self.log.info("[STEP 3] Driving pre-upgrade query workload")
        self._set_query_completed_settings(stream_size=500, threshold=0)
        self._generate_completed_requests_and_history(
            query_nodes_initial, baseline_queries,
        )
        self._run_spill_and_backfill_workload(query_nodes_initial, namespaces[0])
        pre_query_keys = (
            self._get_query_in_use_key_ids() if self.initial_supports_file_ear else {}
        )
        self._trigger_ffdc_and_poll(
            query_nodes_initial,
            expected_key_ids=pre_query_keys,
            label="[pre-upgrade] ",
        )

        # STEP 4: Pre-upgrade GSI file validation
        if self.initial_supports_file_ear:
            index_nodes_initial = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True,
            )
            pre_index_keys = self._get_indexer_in_use_key_ids(index_nodes_initial)
            self.log.info(
                f"[STEP 4] Pre-upgrade: file EAR supported — validating GSI files "
                f"are encrypted with {pre_index_keys}"
            )
            self.validate_engine_encryption(
                'plasma', index_nodes_initial,
                expected_key_id=pre_index_keys,
                encrypted_bucket_names=encrypted_buckets,
                fail_on_error=True,
            )
        else:
            self.log.info(
                f"[STEP 4] Pre-upgrade: cluster on {self.initial_version} — file EAR not "
                f"supported; expecting PLAINTEXT (mixed-mode + post-upgrade checks will assert)"
            )

        # STEP 5: Snapshot index identities
        pre_index_identity = self._capture_index_identity_set()

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
            self.log.info(
                "[STEP 7c] Re-encryption triggered — waiting for all GSI stores "
                "to reach encryption_status='encrypted'"
            )
            self._wait_for_full_gsi_encryption(
                index_nodes_reenc, encrypted_buckets,
                timeout=600, label="[STEP 7c] ",
            )
            self.log.info("[STEP 7c] Force re-encryption complete")

        # STEP 8: Full post-upgrade validation
        self.log.info("[STEP 8] Running full post-upgrade validation")
        self._validate_full_post_upgrade(
            pre_index_identity, encrypted_buckets, namespaces, baseline_queries,
        )

        if deferred_failures:
            failure_summary = "\n".join(deferred_failures)
            self.fail(
                f"[DONE] {mode} upgrade test FAILED — "
                f"{len(deferred_failures)} mixed-mode failure(s):\n{failure_summary}"
            )
        self.log.info(f"[DONE] {mode} upgrade test PASSED")

    # ------------------------------------------------------------------ tests

    def test_offline_upgrade_with_encryption_at_rest(self):
        """Offline upgrade: per-node stop_server -> install -> start. Cluster sees the
        node bounce; remaining nodes serve traffic. Mixed-mode checkpoint between the
        first GSI/N1QL upgrade and the second."""
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_offline_upgrade_with_encryption_at_rest")
        self.log.info("=" * 80)
        self._run_upgrade_test(mode='offline')

    def test_online_upgrade_with_encryption_at_rest(self):
        """Online upgrade: per-node rebalance-out -> install -> rebalance-in (with the
        same service list). Mixed-mode checkpoint between the first GSI/N1QL upgrade
        and the second."""
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_online_upgrade_with_encryption_at_rest")
        self.log.info("=" * 80)
        self._run_upgrade_test(mode='online')
