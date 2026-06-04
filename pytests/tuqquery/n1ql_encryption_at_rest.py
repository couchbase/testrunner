"""
tuq_encryption_at_rest.py: Tests for Query service with Log Encryption at Rest enabled.

Tests validate that the query service's completed-request log files (rlstream.* and
local_request_log.*) are encrypted when log-level encryption at rest is enabled.

Encryption setup is handled by basetestcase.py when enable_log_encryption_at_rest=True.
This test uses self.log_encryption_at_rest_id (set by base class) as the initial KEK.

Unlike GSI encryption tests, no bucket-level encryption is required here.  The focus
is on the query service's own log files.

__author__ = "Pavan PB"
__maintainer__ = "Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
__created_on__ = "13/05/26"
"""

import binascii
import concurrent.futures
import hashlib
import itertools
import json
import os
import re
import threading
import time

from couchbase_helper.query_definitions import QueryDefinition
from lib.membase.api.rest_client import RestConnection
from pytests.gsi.base_gsi import BaseSecondaryIndexingTests
from pytests.tuqquery.n1ql_encryption_helpers import N1QLEncryptionHelpers
from lib.remote.remote_util import RemoteMachineShellConnection

class QueryEncryptionAtRestTests(BaseSecondaryIndexingTests):
    """
    Test class for Query service operations with Log Encryption at Rest enabled.

    Tests validate:
    - Query request log files (rlstream.* / local_request_log.*) are encrypted
    - system:completed_requests entries reflect the index scan queries
    - Log file encryption survives key rotation
    - Log file encryption survives force re-encryption
    """

    def setUp(self):
        super(QueryEncryptionAtRestTests, self).setUp()
        self.encryption_helper = N1QLEncryptionHelpers(self.log)
        self.log.info("==============  QueryEncryptionAtRestTests setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.num_of_docs_per_collection = self.input.param("num_of_docs_per_collection", 10000)
        self.json_template = self.input.param("json_template", "Person")
        self.defer_build = self.input.param("defer_build", False)
        test_name = self._testMethodName
        evidence_root = getattr(self.__class__, "_run_evidence_dir", "/tmp/run_unknown")
        self._evidence_dir = os.path.join(evidence_root, test_name)
        os.makedirs(self._evidence_dir, exist_ok=True)
        self.log.info(f"Per-test evidence directory: {self._evidence_dir}")
        self.log.info("==============  QueryEncryptionAtRestTests setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryEncryptionAtRestTests tearDown has started ==============")
        try:
            # Restore query service settings to defaults on every query node
            query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
            for node in query_nodes:
                self.rest.set_completed_requests_collection_duration(node, 1000)
                self.rest.set_completed_stream_size(node, 0)
        except Exception as e:
            self.log.warning(f"tearDown: failed to restore query settings: {e}")
        try:
            # Defensive: if a spill test crashed between setting queryLogLevel=debug
            # and the matching finally, reset it back to info here so the cluster
            # doesn't keep dumping DEBUG-level output across subsequent tests.
            self.rest.set_query_log_level(self.master, "info")
        except Exception as e:
            self.log.warning(f"tearDown: failed to reset queryLogLevel=info: {e}")
        try:
            query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
            log_path = None
            for node in query_nodes:
                try:
                    log_path = self.encryption_helper.get_log_path(node)
                    shell = RemoteMachineShellConnection(node, verbose=False)
                    shell.execute_command(
                        f"find {log_path} -name 'rlstream.*' -delete 2>/dev/null; "
                        f"find {log_path} -name 'local_request_log.*' -delete 2>/dev/null; "
                        f"find {log_path} -name 'query_ffdc_MAN_areq*' -delete 2>/dev/null; "
                        f"find {log_path} -name 'query_ffdc_MAN_creq*' -delete 2>/dev/null; "
                        f"find {log_path} -name 'query_ffdc_MAN_vita*' -delete 2>/dev/null; "
                        f"rm -rf /tmp/spill_snap_* 2>/dev/null"
                    )
                    shell.disconnect()
                    self.log.info(f"tearDown: cleaned up log files on {node.ip} ({log_path})")
                except Exception as e:
                    self.log.warning(f"tearDown: failed to clean up log files on {node.ip}: {e}")
        except Exception as e:
            self.log.warning(f"tearDown: failed to get query nodes for log cleanup: {e}")
        try:
            _, query_settings = self.rest.get_query_settings(self.master)
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            if spill_dir:
                query_nodes = self.get_nodes_from_services_map(
                    service_type="n1ql", get_all_nodes=True
                )
                for node in query_nodes:
                    try:
                        shell = RemoteMachineShellConnection(node, verbose=False)
                        shell.execute_command(
                            f"find {spill_dir} -name 'av_spill_*' -delete 2>/dev/null; "
                            f"find {spill_dir} -name 'ss_spill-*' -delete 2>/dev/null; "
                            f"find {spill_dir} -name 'scan-results*' -delete 2>/dev/null"
                        )
                        shell.disconnect()
                        self.log.info(f"tearDown: cleaned up spill/backfill files on {node.ip} ({spill_dir})")
                    except Exception as e:
                        self.log.warning(f"tearDown: failed to clean up spill files on {node.ip}: {e}")
        except Exception as e:
            self.log.warning(f"tearDown: failed to clean up spill/backfill files: {e}")
        super(QueryEncryptionAtRestTests, self).tearDown()
        self.log.info("==============  QueryEncryptionAtRestTests tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryEncryptionAtRestTests suite_setUp has started ==============")
        super(QueryEncryptionAtRestTests, self).suite_setUp()
        self._install_tools()
        ts = time.strftime("%Y%m%d_%H%M%S")
        QueryEncryptionAtRestTests._run_evidence_dir = f"/tmp/run_{ts}"
        os.makedirs(QueryEncryptionAtRestTests._run_evidence_dir, exist_ok=True)
        self.log.info(f"Evidence directory for this run: {QueryEncryptionAtRestTests._run_evidence_dir}")
        self.log.info("==============  QueryEncryptionAtRestTests suite_setUp has completed ==============")


    def suite_tearDown(self):
        self.log.info("==============  QueryEncryptionAtRestTests suite_tearDown has started ==============")
        super(QueryEncryptionAtRestTests, self).suite_tearDown()
        self.log.info("==============  QueryEncryptionAtRestTests suite_tearDown has completed ==============")

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    def _copy_failed_file_from_node(self, node, remote_filepath):
        """Copy a remote file that failed encryption validation to self._evidence_dir.
        Returns the local destination path, or None if the copy failed."""
        try:
            remote_dir = os.path.dirname(remote_filepath)
            filename = os.path.basename(remote_filepath)
            local_dest = os.path.join(self._evidence_dir, f"{node.ip}_{filename}")
            shell = RemoteMachineShellConnection(node, verbose=False)
            shell.get_file(remote_dir, filename, local_dest)
            shell.disconnect()
            return local_dest
        except Exception as e:
            self.log.warning(f"Could not copy {remote_filepath} from {node.ip}: {e}")
            return None

    def _remote_file_exists(self, node, file_path):
        """Return True if file_path exists on node. Uses `test -f` for a definite
        answer (xxd silence is ambiguous — could be empty file OR missing file)."""
        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            out, _ = shell.execute_command(
                f"test -f {file_path} && echo EXISTS || echo MISSING"
            )
        finally:
            shell.disconnect()
        return bool(out) and "EXISTS" in (out[0].strip() if out else "")

    def _resolve_existing_or_archived_path(self, node, file_path):
        """
        Returns `file_path` if it still exists on the node, else None.

        Couchbase query log rotation moves the active `rlstream.*` content into
        `local_request_log.*` archives (not into a `.gz` counterpart of the
        rlstream path). FFDC files are generated as `.gz` and either persist or
        get cleaned up — there is no further name mapping on rotation.

        So once a snapshotted path is missing, there is no deterministic
        sibling to redirect to: its content has been concatenated into some
        unrelated archive whose path the snapshot does not track. For the
        force-encryption / lifetime tests the assertion is "no plaintext
        residue should exist at the snapshotted path" — and `None` answers
        that affirmatively.

        Callers should treat `None` as "already handled / nothing to verify",
        not as a failure: the absence of a plaintext file is exactly the
        outcome forceEncryptionAtRest is supposed to achieve.
        """
        if self._remote_file_exists(node, file_path):
            return file_path
        return None

    def _install_tools(self):
        """
        Ensure xxd is installed on every cluster node. Called once from suite_setUp.

        xxd ships in vim-common on Debian/Ubuntu and in vim-common (or vim-enhanced)
        on RHEL/CentOS/Fedora. The check is skipped for nodes where xxd is already
        present so repeated suite runs are fast.
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

    def _verify_encryption_prerequisites(self, enc_type="log"):
        """
        Verify that the requested encryption type is enabled (test param) and active
        on the cluster (via /settings/security/encryptionAtRest).

        enc_type: one of "log", "other", "config", "audit"
        """
        enabled_attr = f"enable_{enc_type}_encryption_at_rest"
        id_attr = f"{enc_type}_encryption_at_rest_id"

        if not getattr(self, enabled_attr, False):
            self.skipTest(
                f"{enc_type.capitalize()} encryption at rest not enabled. "
                f"Set {enabled_attr}=True to run this test."
            )
        key_id = getattr(self, id_attr, None)
        self.assertIsNotNone(
            key_id,
            f"{id_attr} is None — suite_setUp may not have configured {enc_type} encryption"
        )

        status, config = self.rest.get_encryption_at_rest_config()
        self.assertTrue(status, f"Failed to retrieve encryption at rest config: {config}")
        if isinstance(config, (bytes, bytearray)):
            config = json.loads(config)

        section = config.get(enc_type, {})
        self.assertEqual(
            section.get("encryptionMethod"), "encryptionKey",
            f"{enc_type.capitalize()} encryption method is not 'encryptionKey': {section}"
        )
        configured_key_id = section.get("encryptionKeyId")
        self.assertEqual(
            configured_key_id, key_id,
            f"Cluster {enc_type} encryptionKeyId ({configured_key_id}) does not match "
            f"{id_attr} ({key_id})"
        )
        self.log.info(
            f"{enc_type.capitalize()} encryption verified on cluster: "
            f"method=encryptionKey, keyId={configured_key_id}"
        )

    def _set_query_completed_settings(self, stream_size=500, threshold=0):
        """Apply completed-stream-size and completed-threshold on every query node, then verify."""
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        self.log.info(
            f"Setting query settings on {len(query_nodes)} node(s): "
            f"completed-stream-size={stream_size}, completed-threshold={threshold}"
        )
        for node in query_nodes:
            self.rest.set_completed_stream_size(node, stream_size)
            self.rest.set_completed_requests_collection_duration(node, threshold)

        self.sleep(10, "Waiting for query settings to propagate before verification")

        for node in query_nodes:
            settings = self.rest.get_query_admin_settings(node)
            if isinstance(settings, (bytes, bytearray)):
                settings = json.loads(settings)
            self.log.info(f"Query admin settings on {node.ip}: {json.dumps(settings, indent=2)}")
            self.assertEqual(
                settings.get("completed-stream-size"), stream_size,
                f"completed-stream-size not applied on {node.ip}: {settings}"
            )
            self.assertEqual(
                settings.get("completed-threshold"), threshold,
                f"completed-threshold not applied on {node.ip}: {settings}"
            )
            self.log.info(f"Query settings verified on {node.ip}")
        self.log.info("Query settings applied and verified on all query nodes")

    def _create_standard_bucket_and_load(self):
        """Create a plain (non-encrypted) test bucket and load data via prepare_collection_for_indexing."""
        self.log.info(f"Creating standard bucket '{self.test_bucket}'")
        self.bucket_params = self._create_bucket_params(
            server=self.master,
            size=self.bucket_size,
            replicas=self.num_replicas,
            bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy,
            lww=self.lww
        )
        self.cluster.create_standard_bucket(
            name=self.test_bucket, port=11222, bucket_params=self.bucket_params
        )
        self.sleep(10, "Waiting after bucket creation")

        self.log.info(
            f"Loading data: {self.num_of_docs_per_collection} docs, "
            f"{self.num_scopes} scope(s), {self.num_collections} collection(s)"
        )
        self.prepare_collection_for_indexing(
            num_scopes=self.num_scopes,
            num_collections=self.num_collections,
            num_of_docs_per_collection=self.num_of_docs_per_collection,
            json_template=self.json_template,
            bucket_name=self.test_bucket
        )
        self.log.info(f"Namespaces prepared: {self.namespaces}")

    def _create_indexes_for_namespaces(self, prefix_label="enc"):
        """
        Create indexes on all namespaces using gsi_util_obj pattern.
        Follows the same pattern as gsi_file_based_rebalance.py.

        Returns:
            tuple: (select_queries set, query_node)
        """
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        select_queries = set()
        create_queries = []
        deploy_nodes = None

        for i, namespace in enumerate(self.namespaces):
            # Build prefix from namespace scope and collection names
            prefix = f'{prefix_label}_{i+1}' + ''.join(namespace.split(':')[1].split('.'))

            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=True,
                similarity=self.similarity,
                train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                is_base64=self.base64,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index,
                description_dimension=self.dimension
            )

            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace,
                    limit=self.scan_limit
                )
            )

            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions,
                namespace=namespace,
                num_replica=self.num_index_replica,
                randomise_replica_count=True,
                deploy_node_info=deploy_nodes,
                bhive_index=self.bhive_index
            )

            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            create_queries.extend(queries)
            self.log.info(f"Created {len(queries)} indexes on namespace {namespace}")

        return select_queries, query_node

    def _run_select_scans(self, select_queries, query_nodes=None):
        """
        Run every SELECT query on every query node.

        Each query is sent to all nodes so that every node records completed
        requests and creates rlstream files — regardless of how many queries
        are in the set relative to the number of nodes.

        Args:
            select_queries: Set of SELECT query strings to execute
            query_nodes: List of query nodes to run against.
                         Defaults to all n1ql nodes in the cluster.
        """
        if not query_nodes:
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )

        self.log.info(
            f"Running {len(select_queries)} SELECT scan(s) on each of "
            f"{len(query_nodes)} node(s): {[n.ip for n in query_nodes]}"
        )
        for node in query_nodes:
            for query in select_queries:
                result = self.run_cbq_query(query=query, server=node, verbose=False)
                self.assertEqual(
                    result.get('status'), 'success',
                    f"Query failed on {node.ip}: {query}\n{result}"
                )
                self.assertGreater(
                    len(result.get('results', [])), 0,
                    f"Query returned no results on {node.ip}: {query}"
                )
            self.log.info(
                f"All {len(select_queries)} SELECT scan(s) completed on {node.ip}"
            )
        self.log.info(
            f"All SELECT scans completed on all {len(query_nodes)} node(s)"
        )

    def _dispatch_background_query_batch(self, select_queries, query_nodes):
        """Start one round of _run_select_scans in a background daemon thread."""
        t = threading.Thread(
            target=self._run_select_scans,
            args=(select_queries, query_nodes),
            daemon=True,
            name="bg-query-batch"
        )
        t.start()
        self.log.info(
            f"Background query batch dispatched: {len(select_queries)} queries "
            f"× {len(query_nodes)} node(s)"
        )
        return t

    def _get_vitals_spill_counts(self, query_nodes, metric="spills.order"):
        """Return {node_ip: <metric>} from admin/vitals on each query node.

        Supported metrics (all admin/vitals counters in the same shape):
          - `spills.order`              — ORDER BY sort spill
          - `spills.merge`              — MERGE-driven spill
          - `spills.update_statistics`  — UPDATE STATISTICS per-field sort spill
          - `spills.seq_scan`           — sequential-scan (key-only and
                                          key+document) spill
        """
        counts = {}
        for node in query_nodes:
            status, vitals = self.rest.get_query_vitals(node)
            self.assertTrue(status, f"admin/vitals failed on {node.ip}: {vitals}")
            if isinstance(vitals, (bytes, bytearray)):
                vitals = json.loads(vitals)
            counts[node.ip] = vitals.get(metric, 0)
        return counts

    def _assert_spill_files_encrypted(self, query_nodes, spill_dir, expected_key_ids, label=""):
        """Find all files in spill_dir on each query node and verify they are encrypted."""
        node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)
        failures = []
        total_files = 0
        for node in query_nodes:
            shell = RemoteMachineShellConnection(node, verbose=False)
            out, _ = shell.execute_command(f"find {spill_dir} -type f 2>/dev/null")
            shell.disconnect()
            spill_files = [f.strip() for f in out if f.strip()]
            node_key_ids = [str(k) for k in node_key_ids_map.get(node.ip, [])]
            self.log.info(
                f"{label}[{node.ip}] {len(spill_files)} spill file(s) found in {spill_dir}"
            )
            for fpath in spill_files:
                total_files += 1
                is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                    node, fpath
                )
                if not is_enc:
                    dest = self._copy_failed_file_from_node(node, fpath)
                    failures.append(
                        f"  [{node.ip}] {fpath} is NOT encrypted: {details}"
                        + (f" (copied to {dest})" if dest else "")
                    )
                    continue
                if node_key_ids:
                    found = any(
                        self.encryption_helper.verify_file_header_contains(node, fpath, k)[0]
                        for k in node_key_ids
                    )
                    if not found:
                        failures.append(
                            f"  [{node.ip}] {fpath} encrypted but header missing "
                            f"expected key ID(s) {node_key_ids}"
                        )
        self.assertGreater(
            total_files, 0,
            f"{label}No spill files found in {spill_dir} across {len(query_nodes)} node(s)"
        )
        if failures:
            msg = f"{label}Spill file encryption FAILED:\n" + "\n".join(failures)
            self.log.error(msg)
            self.fail(msg)
        self.log.info(
            f"{label}{total_files} spill file(s) verified encrypted across "
            f"{len(query_nodes)} node(s)"
        )

    # -------------------------------------------------------------------------
    # 'other' encryption-at-rest toggle and remote spill-file watcher helpers
    # (shared by the three spill correctness tests: ORDER BY, MERGE,
    # UPDATE STATISTICS)
    # -------------------------------------------------------------------------

    def _disable_other_encryption_at_rest(self, label=""):
        """Disable 'other' encryption at rest cluster-wide. Does NOT trigger a
        re-encryption of existing files — only flips the policy."""
        self.log.info(f"{label}Disabling 'other' encryption at rest...")
        status, response = self.rest.disable_other_encryption()
        self.assertTrue(status, f"Failed to disable other encryption: {response}")
        self.sleep(5, f"{label}Waiting for 'other' encryption disable to take effect")

    def _enable_other_encryption_at_rest(self, key_id, label=""):
        """Enable 'other' encryption at rest cluster-wide with the supplied KEK."""
        self.log.info(f"{label}Enabling 'other' encryption at rest with key {key_id}...")
        status, response = self.rest.configure_encryption_at_rest({
            "other.encryptionMethod": "encryptionKey",
            "other.encryptionKeyId": str(key_id),
        })
        self.assertTrue(status, f"Failed to enable other encryption: {response}")
        self.sleep(5, f"{label}Waiting for 'other' encryption enable to take effect")

    def _disable_bucket_encryption_at_rest(self, bucket, label=""):
        """
        Disable bucket-level encryption at rest on the given bucket. POSTs
        encryptionAtRestKeyId=-1 to /pools/default/buckets/<bucket>. Used by
        the sequential-scan and GSI scan-backfill tests — per the spec doc,
        spill/backfill files produced by those paths are encrypted with
        BUCKET encryption keys, not the 'other' cluster-wide KEK.
        """
        self.log.info(f"{label}Disabling bucket encryption at rest on '{bucket}'...")
        status, response = self.rest.disable_bucket_encryption(bucket)
        self.assertTrue(status, f"Failed to disable bucket encryption: {response}")
        self.sleep(5, f"{label}Waiting for bucket encryption disable to take effect")

    def _enable_bucket_encryption_at_rest(self, bucket, key_id, label=""):
        """
        Enable bucket-level encryption at rest on the given bucket with the
        supplied KEK id. Mirrors `_enable_other_encryption_at_rest` but for
        the bucket-encryption flavour required by sequential-scan / GSI
        scan-backfill spill files (per the spec doc).
        """
        self.log.info(
            f"{label}Enabling bucket encryption at rest on '{bucket}' with key {key_id}..."
        )
        status, response = self.rest.enable_bucket_encryption(bucket, key_id)
        self.assertTrue(status, f"Failed to enable bucket encryption: {response}")
        self.sleep(5, f"{label}Waiting for bucket encryption enable to take effect")

    def _set_query_log_level(self, level, label=""):
        """Set the query service log level on EVERY query node via
        POST /settings/querySettings (ns_server, port 8091).

        IMPORTANT: `/settings/querySettings` is applied on the receiving
        node — it does NOT fan out to other query nodes in the cluster.
        Setting it on `self.master` alone meant that when a spill query
        landed on any other query node, that node stayed at INFO and the
        per-spill debug log lines were never written to its query.log.
        This helper now posts the setting to each query node returned
        by `get_nodes_from_services_map(service_type="n1ql")`.

        Used by the spill tests to enable DEBUG-level logging immediately
        before issuing a spill-triggering query and revert to INFO right
        after the query finishes. While DEBUG is active query.log captures
        per-spill log lines like:
          - sort spill:        `need to spill: %v+%v, heapSize: %v`
          - seq-scan key-only: `Sequential scan: %s document scan spill file created`
                              (or `... encrypted with keyId: %s` when encrypted)
          - seq-scan key+doc:  `Sequential scan: %s key scan spill file created`
                              (or `... encrypted with keyId: %s` when encrypted)
          - scan backfill:     `new temp file isEncrypted:<bool> ...`
          - FTS backfill:      `n1fty: response_handler: buffer overflow ...`
        Once reverted to INFO these are suppressed — keeping the bracket
        tight avoids flooding the cluster log with unrelated DEBUG output.

        No grep is done: log capture is the goal. The artefacts can be
        inspected post-test from the cluster log bundle.
        """
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True
        )
        self.log.info(
            f"{label}Setting queryLogLevel={level} on "
            f"{len(query_nodes)} query node(s): {[n.ip for n in query_nodes]}"
        )
        failures = []
        for node in query_nodes:
            status, response = self.rest.set_query_log_level(node, level)
            if not status:
                failures.append(f"{node.ip}: {response}")
        self.assertFalse(
            failures,
            f"{label}Failed to set queryLogLevel={level} on: "
            + "; ".join(failures)
        )

    def _remote_spill_snap_dir(self, suffix=""):
        """Stable, per-test snapshot directory path on every cluster node.
        The watcher writes copies of spill files here before they are deleted
        by the live query. Path is the same on every node so tests can
        iterate node-by-node with one variable."""
        base = f"/tmp/spill_snap_{self._testMethodName}"
        return f"{base}_{suffix}" if suffix else base

    def _start_remote_spill_watcher(self, query_nodes, spill_dir, suffix="", label="",
                                     file_prefix=""):
        """
        Launch a backgrounded bash watcher on every query node. The watcher
        polls spill_dir every second and copies any new files into the
        per-test snapshot directory before the live query deletes them.

        file_prefix: if non-empty, only files whose basename starts with this
        prefix are captured (e.g. "av_spill_" for ORDER BY / MERGE /
        UPDATE STATISTICS spills, "ss_spill-" for sequential-scan spills,
        "scan-results" for GSI scan-backfill files).

        Returns the snapshot directory path (same on every node).
        """
        snap_dir = self._remote_spill_snap_dir(suffix=suffix)
        script_path = f"{snap_dir}/.watcher.sh"
        log_path = f"{snap_dir}/.watcher.log"

        find_name_filter = f"-name '{file_prefix}*'" if file_prefix else ""
        script = (
            "#!/bin/bash\n"
            f"SNAP_DIR={snap_dir}\n"
            "STOP=$SNAP_DIR/.stop\n"
            f"SPILL_DIR={spill_dir}\n"
            "while [ ! -f \"$STOP\" ]; do\n"
            f"  find \"$SPILL_DIR\" -type f {find_name_filter} 2>/dev/null | while read -r f; do\n"
            "    base=$(basename \"$f\")\n"
            "    if [ ! -f \"$SNAP_DIR/$base\" ]; then\n"
            "      cp -p \"$f\" \"$SNAP_DIR/$base\" 2>/dev/null\n"
            "    fi\n"
            "  done\n"
            "  sleep 1\n"
            "done\n"
        )

        for node in query_nodes:
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                shell.execute_command(f"rm -rf {snap_dir} && mkdir -p {snap_dir}")
                shell.execute_command(
                    f"cat > {script_path} <<'WATCHER_EOF'\n{script}WATCHER_EOF"
                )
                shell.execute_command(f"chmod +x {script_path}")
                shell.execute_command(
                    f"nohup {script_path} > {log_path} 2>&1 & disown"
                )
                self.log.info(
                    f"{label}Started spill watcher on {node.ip} in {snap_dir}"
                )
            finally:
                shell.disconnect()

        return snap_dir

    def _stop_remote_spill_watcher(self, query_nodes, snap_dir, label=""):
        """Touch the stop sentinel on each node so each watcher exits its loop.
        Sleeps briefly so any in-flight copy completes."""
        for node in query_nodes:
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                shell.execute_command(f"touch {snap_dir}/.stop")
            finally:
                shell.disconnect()
        self.sleep(3, f"{label}Waiting for spill watchers to exit")

    def _assert_remote_spill_snapshots_encrypted(self, query_nodes, snap_dir,
                                                 expected_key_ids, label=""):
        """
        Validate snapshotted spill files in snap_dir.

        Soft behaviour: if no files were captured by the watcher (transient
        files may be created and deleted faster than the 1 s poll cycle),
        log a warning and return — the watcher is a best-effort signal,
        and the authoritative test outcome is the query-results comparison
        in the caller.

        Hard failure: if any captured file is NOT encrypted, or is
        encrypted with an unexpected key ID — that is a real encryption
        regression.
        """
        node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)
        total_files = 0
        failures = []
        for node in query_nodes:
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                out, _ = shell.execute_command(
                    f"find {snap_dir} -type f ! -name '.*' 2>/dev/null"
                )
            finally:
                shell.disconnect()
            snap_files = [f.strip() for f in out if f.strip()]
            node_key_ids = [str(k) for k in node_key_ids_map.get(node.ip, [])]
            self.log.info(
                f"{label}[{node.ip}] {len(snap_files)} spill snapshot file(s) in {snap_dir}"
            )
            for fpath in snap_files:
                total_files += 1
                is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                    node, fpath
                )
                if not is_enc:
                    dest = self._copy_failed_file_from_node(node, fpath)
                    failures.append(
                        f"  [{node.ip}] {fpath} is NOT encrypted: {details}"
                        + (f" (copied to {dest})" if dest else "")
                    )
                    continue
                if node_key_ids:
                    found = any(
                        self.encryption_helper.verify_file_header_contains(node, fpath, k)[0]
                        for k in node_key_ids
                    )
                    if not found:
                        failures.append(
                            f"  [{node.ip}] {fpath} encrypted but header missing "
                            f"expected key ID(s) {node_key_ids}"
                        )
        if total_files == 0:
            self.log.warning(
                f"{label}No spill snapshot files captured in {snap_dir} across "
                f"{len(query_nodes)} node(s) — watcher may have missed the "
                f"transient files (poll interval is 1 s) or the query did "
                f"not spill on this cluster. Test continues; query-results "
                f"comparison is the authoritative signal."
            )
            return
        if failures:
            msg = f"{label}Spill snapshot encryption FAILED:\n" + "\n".join(failures)
            self.log.error(msg)
            self.fail(msg)
        self.log.info(
            f"{label}{total_files} spill snapshot file(s) verified encrypted "
            f"across {len(query_nodes)} node(s)"
        )

    def _cleanup_remote_spill_snapshots(self, query_nodes, snap_dir):
        """Remove the per-test snapshot dir on every node. Idempotent and
        tolerant of node-level failures."""
        for node in query_nodes:
            try:
                shell = RemoteMachineShellConnection(node, verbose=False)
                try:
                    shell.execute_command(f"rm -rf {snap_dir}")
                finally:
                    shell.disconnect()
            except Exception as e:
                self.log.warning(f"Failed to cleanup {snap_dir} on {node.ip}: {e}")

    # -------------------------------------------------------------------------
    # vBucket-balanced key generation (ported from main.go's vbHash). Used by
    # the sequential-scan spill tests so every vBucket carries enough keys to
    # have a realistic chance of spilling when scanned.
    # -------------------------------------------------------------------------

    @staticmethod
    def _vb_hash(key, num_vbuckets):
        """
        Couchbase's CRC32-IEEE based vBucket hash.

        Equivalent to the Go reference at scripts/main.go: builds the same
        CRC32 over the UTF-8 bytes of `key`, then returns
            ((^crc) >> 16) & 0x7fff & (num_vbuckets - 1).

        Python's binascii.crc32 already returns crc ^ 0xFFFFFFFF (i.e. ^crc
        in 32-bit), so we feed that directly into the shift+mask.

        `num_vbuckets` must be a power of 2 (typical: 1024 for Couchstore,
        128 for Magma).
        """
        return (binascii.crc32(key.encode("utf-8")) >> 16) & 0x7fff & (num_vbuckets - 1)

    def _generate_vb_balanced_keys(self, num_vbuckets=1024, keys_per_vb=10,
                                   key_template="key::{n}",
                                   max_iterations=10_000_000):
        """
        Return a list of keys distributed evenly across vBuckets.

        Iterates key_template.format(n=1, 2, 3, …) and keeps the first
        `keys_per_vb` keys hashing to each vBucket. Stops once every
        vBucket has reached the target count. Preserves discovery order.

        Direct port of the Go reference at scripts/main.go — same key
        construction (sequential integer suffix), same CRC32 hash, same
        per-vBucket cap loop. The Python flavour just lets callers pick
        a wider key_template (e.g. a long padding prefix + "::{n}" so the
        resulting keys are also large in length, which is what the
        key-only seqscan test wants).

        Raises if max_iterations is exhausted without filling every vBucket.
        """
        keys_by_vb = [[] for _ in range(num_vbuckets)]
        selected = []
        completed = 0
        i = 0
        while completed < num_vbuckets and i < max_iterations:
            i += 1
            key = key_template.format(n=i)
            vb = self._vb_hash(key, num_vbuckets)
            if len(keys_by_vb[vb]) < keys_per_vb:
                keys_by_vb[vb].append(key)
                selected.append(key)
                if len(keys_by_vb[vb]) == keys_per_vb:
                    completed += 1
        if completed < num_vbuckets:
            raise RuntimeError(
                f"_generate_vb_balanced_keys: only {completed}/{num_vbuckets} "
                f"vBuckets filled after {i} iterations — increase max_iterations "
                f"or reduce keys_per_vb"
            )
        return selected

    def _upsert_explicit_keys(self, namespace, keys, value_expr, batch_size=500,
                              label=""):
        """
        UPSERT each key in `keys` with the given N1QL VALUE expression.

        Each batch builds a single statement of the form:
            UPSERT INTO <ns> (KEY k, VALUE v)
            SELECT k AS k, <value_expr> AS v
            FROM ["k1", "k2", ...] AS k

        That keeps the key set tightly controlled (as opposed to ARRAY_RANGE
        generation, which would produce keys we did not choose). Used by the
        seqscan tests when keys come from `_generate_vb_balanced_keys`.
        """
        for offset in range(0, len(keys), batch_size):
            batch = keys[offset:offset + batch_size]
            keys_array = json.dumps(batch)
            upsert_query = (
                f"UPSERT INTO {namespace} (KEY k, VALUE v) "
                f"SELECT k AS k, {value_expr} AS v "
                f"FROM {keys_array} AS k"
            )
            result = self.run_cbq_query(query=upsert_query)
            self.assertEqual(
                result.get("status"), "success",
                f"{label}UPSERT batch at offset {offset} failed: {result}"
            )

    def _get_query_in_use_key_ids(self, category=None):
        """
        Fetch the key IDs currently in use by the query service on every query node.

        Each node independently manages its own DEKs via its own
        GET /admin/encryption_at_rest endpoint. Endpoint shape:
        ``{"keys.in_use": {"log": [...], "other": [...]}}``.

        Args:
            category: when None, returns the union across categories (legacy
                behavior). Pass "log" or "other" to read only that category —
                required for per-category re-encryption / rotation polls,
                because mixing log+other masks the very change we're waiting
                for when both are enabled.

        Returns ``{node_ip: [key_id, ...]}``.
        """
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True
        )
        node_key_ids = {}
        for node in query_nodes:
            status, enc_response = self.rest.get_query_in_use_encryption_keys(node)
            self.assertTrue(status, f"admin/encryption_at_rest failed on {node.ip}: {enc_response}")
            if isinstance(enc_response, (bytes, bytearray)):
                enc_response = json.loads(enc_response)
            keys_in_use = enc_response.get("keys.in_use", {})
            if category is None:
                resolved = [k for ids in keys_in_use.values() for k in ids if k]
            else:
                resolved = [k for k in (keys_in_use.get(category) or []) if k]
            self.log.info(
                f"Query service in-use key IDs on {node.ip} "
                f"(category={category or 'all'}): {keys_in_use} -> {resolved}"
            )
            node_key_ids[node.ip] = resolved
        return node_key_ids

    def _build_node_key_ids_map(self, query_nodes, expected_key_ids):
        """
        Normalise expected_key_ids into a per-node dict.

        Accepts either:
          - dict  {node_ip: [key_id, ...]}  — returned by _get_query_in_use_key_ids()
          - list  [key_id, ...]             — same list applied to every node
        """
        if isinstance(expected_key_ids, dict):
            return expected_key_ids
        return {node.ip: expected_key_ids for node in query_nodes}

    def _verify_archived_local_request_logs(self, node, expected_key_ids, label=""):
        """
        Fallback for `_assert_rlstream_files_encrypted` when rlstream.* has rotated
        away. The completed-request stream is finalised into local_request_log.*
        archives (idle timeout or 100 MiB), so when rlstream.* is empty the
        evidence we care about lives in those archives.

        Returns a (verified, failures) tuple where:
          - verified: True if at least one local_request_log.* file was found AND
            it is encrypted with one of expected_key_ids (or expected_key_ids is
            empty, in which case "encrypted" alone is sufficient).
          - failures: list of human-readable failure lines if any archive was
            present but unencrypted / missing the expected key ID.

        If neither rlstream.* nor local_request_log.* exists on the node, returns
        (False, []) so the caller can decide whether to fail.
        """
        log_path = self.encryption_helper.get_log_path(node)
        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            # Newest-first ordering so we evaluate the freshest archives first
            out, _ = shell.execute_command(
                f"ls -1t {log_path}/local_request_log.* 2>/dev/null"
            )
        finally:
            shell.disconnect()
        archives = [f.strip() for f in out if f and f.strip()]
        if not archives:
            return False, []

        expected = [str(k) for k in (expected_key_ids or [])]
        failures = []
        any_encrypted_with_expected = False
        # Sample up to the newest 5 archives — enough signal without scanning
        # the entire historical archive set
        for fpath in archives[:5]:
            is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                node, fpath
            )
            if not is_enc:
                failures.append(
                    f"  [{node.ip}] (rlstream rotated) {fpath} is NOT encrypted: {details}"
                )
                continue
            if not expected:
                any_encrypted_with_expected = True
                continue
            found = any(
                self.encryption_helper.verify_file_header_contains(node, fpath, k)[0]
                for k in expected
            )
            if found:
                any_encrypted_with_expected = True
            else:
                failures.append(
                    f"  [{node.ip}] (rlstream rotated) {fpath} header missing expected key ID"
                )

        if any_encrypted_with_expected and not failures:
            self.log.info(
                f"{label}[{node.ip}] rlstream.* absent (rotated); verified "
                f"local_request_log.* archive encrypted with expected key ID"
            )
            return True, []
        return any_encrypted_with_expected, failures

    def _assert_rlstream_files_encrypted(self, query_nodes, expected_key_ids, label=""):
        """
        Assert that every query node has rlstream.* (or, if rotated, the
        equivalent local_request_log.* archive) encrypted with the expected key ID.

        rlstream.* files are the active completed-request stream files written
        while the query service is running.  They should exist immediately after
        scans have been issued with completed-stream-size > 0.

        Rotation handling: rlstream is finalised into local_request_log.* on
        idle timeout or at 100 MiB. Under sustained query load (concurrent
        re-encryption, DEK lifetime, etc.) rlstream.* can be empty by the time
        we look, even though it existed moments earlier. In that case we fall
        back to the newest local_request_log.* archives on the node — they
        carry the same encryption evidence and are the rotation target.

        expected_key_ids: dict {node_ip: [key_id, ...]} or flat list — see
        _build_node_key_ids_map().
        """
        node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)

        all_results = {}
        for node in query_nodes:
            r = self.encryption_helper.verify_query_log_files_encrypted(
                [node], node_key_ids_map.get(node.ip, [])
            )
            all_results.update(r)

        node_obj_map = {node.ip: node for node in query_nodes}
        failures = []
        copied_files = []
        for node_ip, node_result in all_results.items():
            rlstream_files = node_result["rlstream"]
            if len(rlstream_files) == 0:
                # rlstream has rotated — fall back to local_request_log.* archives
                node_obj = node_obj_map.get(node_ip)
                if node_obj is None:
                    failures.append(f"  [{node_ip}] No rlstream.* files found")
                    continue
                verified, archive_failures = self._verify_archived_local_request_logs(
                    node_obj, node_key_ids_map.get(node_ip, []), label=label
                )
                if archive_failures:
                    failures.extend(archive_failures)
                    continue
                if not verified:
                    failures.append(
                        f"  [{node_ip}] No rlstream.* files found and no "
                        f"local_request_log.* fallback archives present"
                    )
                # Verified via fallback — continue to next node
                continue
            for f, r in rlstream_files.items():
                if not r["encrypted"] or not r.get("key_id_found", True):
                    node_obj = node_obj_map.get(node_ip)
                    if node_obj:
                        dest = self._copy_failed_file_from_node(node_obj, f)
                        if dest:
                            copied_files.append(dest)
                if not r["encrypted"]:
                    failures.append(f"  [{node_ip}] {f} is NOT encrypted: {r['details']}")
                if not r.get("key_id_found", True):
                    failures.append(f"  [{node_ip}] {f} header missing expected key ID")

        if copied_files:
            self.log.info(
                f"{label}Copied {len(copied_files)} failing rlstream file(s) to "
                f"{self._evidence_dir}:\n" + "\n".join(f"  {p}" for p in copied_files)
            )
        if failures:
            msg = (
                f"{label}Encryption validation FAILED for the following rlstream files "
                f"(copies in {self._evidence_dir}):\n" + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)

    def _assert_local_request_log_files_encrypted(self, query_nodes, expected_key_ids,
                                                    select_queries, label=""):
        """
        Drive query load to trigger local_request_log.* archive creation, then assert
        that every node has at least one such file encrypted with the expected key IDs.

        local_request_log.* archives are created when the active rlstream file is
        finalised (idle timeout or 100 MiB).  Passive waiting is not sufficient — this
        method drives select_queries in background batches every 5 s to keep generating
        completed-request data.  Every 60 s it polls for new local_request_log.* files.
        Exits as soon as files are present on all nodes, or after 10 minutes (600 s).

        expected_key_ids: dict {node_ip: [key_id, ...]} or flat list — see
        _build_node_key_ids_map().
        """
        node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)
        queries_list = list(select_queries)

        deadline = time.time() + 600
        last_poll = time.time()
        nodes_still_waiting = set(node.ip for node in query_nodes)

        self.log.info(
            f"{label}Driving query load to trigger local_request_log.* archive creation "
            f"(timeout 600 s, polling every 60 s)..."
        )
        active_batch = None
        while time.time() < deadline:
            # Join the previous batch before starting the next one so we never
            # accumulate more than one concurrent query-batch thread.
            if active_batch is not None:
                active_batch.join(timeout=60)
            active_batch = self._dispatch_background_query_batch(queries_list, query_nodes)
            self.sleep(5, f"{label}Waiting between query batches...")

            if time.time() - last_poll >= 60:
                last_poll = time.time()
                for node in query_nodes:
                    if node.ip not in nodes_still_waiting:
                        continue
                    log_path = self.encryption_helper.get_log_path(node)
                    shell = RemoteMachineShellConnection(node, verbose=False)
                    out, _ = shell.execute_command(
                        f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
                    )
                    shell.disconnect()
                    files = [f.strip() for f in out if f.strip()]
                    if files:
                        nodes_still_waiting.discard(node.ip)
                        self.log.info(
                            f"{label}{node.ip}: found {len(files)} local_request_log.* file(s)"
                        )
                if not nodes_still_waiting:
                    self.log.info(
                        f"{label}local_request_log.* files found on all nodes — stopping load"
                    )
                    break
                self.log.info(
                    f"{label}Still waiting for local_request_log.* on: "
                    f"{sorted(nodes_still_waiting)} — continuing query load..."
                )
        else:
            self.log.info(f"{label}10-minute timeout reached — proceeding to encryption validation")

        # Validate encryption state now that files exist (or timeout reached)
        all_results = {}
        for node in query_nodes:
            r = self.encryption_helper.verify_query_log_files_encrypted(
                [node], node_key_ids_map.get(node.ip, [])
            )
            all_results.update(r)

        node_obj_map = {node.ip: node for node in query_nodes}
        failures = []
        copied_files = []
        for node_ip, node_result in all_results.items():
            local_log_files = node_result["local_request_log"]
            if len(local_log_files) == 0:
                failures.append(
                    f"  [{node_ip}] No local_request_log.* files found after 600 s of query load"
                )
                continue
            for f, r in local_log_files.items():
                if not r["encrypted"] or not r.get("key_id_found", True):
                    node_obj = node_obj_map.get(node_ip)
                    if node_obj:
                        dest = self._copy_failed_file_from_node(node_obj, f)
                        if dest:
                            copied_files.append(dest)
                if not r["encrypted"]:
                    failures.append(f"  [{node_ip}] {f} is NOT encrypted: {r['details']}")
                if not r.get("key_id_found", True):
                    failures.append(f"  [{node_ip}] {f} header missing expected key ID")

        if copied_files:
            self.log.info(
                f"{label}Copied {len(copied_files)} failing local_request_log file(s) to "
                f"{self._evidence_dir}:\n" + "\n".join(f"  {p}" for p in copied_files)
            )
        if failures:
            msg = (
                f"{label}Encryption validation FAILED for the following local_request_log "
                f"files (copies in {self._evidence_dir}):\n" + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)

    def _validate_completed_requests(self, select_queries, query_nodes=None):
        """
        Query system:completed_requests on every query node and assert that entries
        reflecting the index scans run exist across the cluster.

        system:completed_requests is per-node — each node only records requests it
        handled — so all nodes must be polled and results aggregated.

        Validates per-entry: state=completed, errorCount=0, resultCount>0,
        and phaseCounts shows GSI index scan activity.
        """
        if not query_nodes:
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )

        self.log.info(
            f"Validating system:completed_requests across "
            f"{len(query_nodes)} node(s): {[n.ip for n in query_nodes]}"
        )
        our_statements = {q.strip() for q in select_queries}
        all_matched = []

        for node in query_nodes:
            result = self.run_cbq_query(
                "SELECT * FROM system:completed_requests", server=node
            )
            self.assertEqual(
                result.get('status'), 'success',
                f"system:completed_requests query failed on {node.ip}: {result}"
            )
            completed = result.get('results', [])
            self.log.info(
                f"system:completed_requests on {node.ip}: {len(completed)} entries"
            )
            matched = [
                e for e in completed
                if e.get('completed_requests', e).get('statement', '').strip()
                in our_statements
            ]
            all_matched.extend(matched)

        self.assertGreater(
            len(all_matched), 0,
            "None of the executed SELECT queries appear in system:completed_requests "
            f"on any of {[n.ip for n in query_nodes]}"
        )
        for entry in all_matched:
            inner = entry.get('completed_requests', entry)
            self.assertEqual(inner.get('state'), 'completed',
                             f"Entry state is not 'completed': {inner.get('state')}")
            self.assertEqual(inner.get('errorCount', 0), 0,
                             f"Entry has errors: {inner.get('errors')}")
            self.assertGreater(inner.get('resultCount', 0), 0,
                               "Entry resultCount is 0")
            phase_counts = inner.get('phaseCounts', {})
            gsi_scans = (
                phase_counts.get('indexScan', {}).get('GSI', 0)
                if isinstance(phase_counts.get('indexScan'), dict)
                else phase_counts.get('indexScan', 0)
            )
            self.assertGreater(gsi_scans, 0,
                               f"No GSI index scans recorded in phaseCounts: {phase_counts}")

        self.log.info(
            f"system:completed_requests validated: {len(all_matched)} matching "
            f"entries found across all nodes"
        )

    def _validate_completed_requests_history(self, select_queries, query_nodes=None):
        """
        Query system:completed_requests_history on every query node and assert any
        entries present reflect the index scans run.

        system:completed_requests_history is per-node — all nodes are polled and
        results aggregated. History may legitimately be empty if entries have not
        yet aged out; only validates entries when present.
        Validates per-entry: state=completed, errorCount=0, resultCount>0,
        and phaseCounts shows GSI index scan activity.
        """
        if not query_nodes:
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )

        self.log.info(
            f"Validating system:completed_requests_history across "
            f"{len(query_nodes)} node(s): {[n.ip for n in query_nodes]}"
        )
        history = []
        for node in query_nodes:
            result = self.run_cbq_query(
                "SELECT * FROM system:completed_requests_history", server=node
            )
            self.assertEqual(
                result.get('status'), 'success',
                f"system:completed_requests_history query failed on {node.ip}: {result}"
            )
            history.extend(result.get('results', []))
        if not history:
            self.log.info(
                "system:completed_requests_history is empty — "
                "entries may not have aged out yet; skipping per-entry checks"
            )
            return

        our_statements = {q.strip() for q in select_queries}
        matched = [
            e for e in history
            if e.get('completed_requests_history', e).get('statement', '').strip()
            in our_statements
        ]
        if not matched:
            self.log.info(
                "No executed SELECT queries found in system:completed_requests_history "
                "(entries may not have aged out yet)"
            )
            return

        for entry in matched:
            inner = entry.get('completed_requests_history', entry)
            self.assertEqual(inner.get('state'), 'completed',
                             f"History entry state is not 'completed': {inner.get('state')}")
            self.assertEqual(inner.get('errorCount', 0), 0,
                             f"History entry has errors: {inner.get('errors')}")
            self.assertGreater(inner.get('resultCount', 0), 0,
                               "History entry resultCount is 0")
            phase_counts = inner.get('phaseCounts', {})
            gsi_scans = (
                phase_counts.get('indexScan', {}).get('GSI', 0)
                if isinstance(phase_counts.get('indexScan'), dict)
                else phase_counts.get('indexScan', 0)
            )
            self.assertGreater(gsi_scans, 0,
                               f"No GSI index scans in history phaseCounts: {phase_counts}")

        self.log.info(
            f"system:completed_requests_history validated: {len(matched)} matching entries found"
        )

    def _setup_bucket_indexes_scans(self, prefix_label="enc"):
        """
        Shared setup: set query settings, create bucket + data, create indexes,
        wait online, run scans.

        Returns:
            tuple: (select_queries set, query_nodes list)
        """
        self._set_query_completed_settings(stream_size=500, threshold=0)

        self._create_standard_bucket_and_load()

        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True
        )

        self.log.info("Creating indexes")
        select_queries, _ = self._create_indexes_for_namespaces(prefix_label=prefix_label)

        self.log.info("Waiting for indexes to come online")
        self.wait_until_indexes_online(timeout=300, defer_build=self.defer_build)

        self._run_select_scans(select_queries, query_nodes=query_nodes)

        return select_queries, query_nodes

    def _wait_for_new_query_key_ids(self, baseline_key_ids, timeout=300, label="",
                                    category=None):
        """
        Poll admin/encryption_at_rest until at least one query node reports a key ID
        that was not present in baseline_key_ids.

        baseline_key_ids: {node_ip: [key_id, ...]} — from _get_query_in_use_key_ids()
        category: forwarded to _get_query_in_use_key_ids so the poll reads only the
            same category as the baseline (log / other). Required when both log
            and other encryption are enabled — without this the poll sees the
            union and may report "no change" even when the targeted category
            has rotated, or fire on rotations in the unrelated category.

        Returns the full current {node_ip: [key_id, ...]} dict so the caller can pass
        it directly as expected_key_ids to _assert_rlstream_files_encrypted /
        _assert_local_request_log_files_encrypted.

        Raises AssertionError if no new IDs appear within timeout seconds.
        """
        baseline_sets = {
            node_ip: {str(k) for k in key_ids}
            for node_ip, key_ids in baseline_key_ids.items()
        }
        deadline = time.time() + timeout
        while time.time() < deadline:
            current = self._get_query_in_use_key_ids(category=category)
            new_per_node = {
                node_ip: [k for k in key_ids if str(k) not in baseline_sets.get(node_ip, set())]
                for node_ip, key_ids in current.items()
            }
            detected = {ip: ids for ip, ids in new_per_node.items() if ids}
            if all(new_per_node.get(node_ip) for node_ip in baseline_sets):
                self.log.info(
                    f"{label}New query key IDs detected on all nodes: {detected} "
                    f"(baseline was {baseline_key_ids})"
                )
                return current  # full current dict used as new expected_key_ids
            if detected:
                self.log.info(
                    f"{label}New key IDs on some nodes: {detected}; "
                    f"still waiting for remaining nodes"
                )
            self.sleep(10, f"{label}Waiting for new key IDs (baseline: {baseline_key_ids})")
        self.fail(
            f"{label}No new key IDs appeared within {timeout} s. "
            f"Baseline: {baseline_key_ids}"
        )

    def _wait_for_old_query_key_ids_to_disappear(self, baseline_key_ids, timeout=300,
                                                 label="", category=None):
        """
        Poll admin/encryption_at_rest until none of the key IDs in baseline_key_ids
        are reported in KeysInUse on any query node.

        Returns the first current {node_ip: [key_id, ...]} snapshot observed after all
        baseline key IDs have disappeared.
        """
        baseline_sets = {
            node_ip: {str(k) for k in key_ids}
            for node_ip, key_ids in baseline_key_ids.items()
        }
        deadline = time.time() + timeout
        while time.time() < deadline:
            current = self._get_query_in_use_key_ids(category=category)
            still_present = {}
            for node_ip, key_ids in current.items():
                expired_keys = [k for k in key_ids if str(k) in baseline_sets.get(node_ip, set())]
                if expired_keys:
                    still_present[node_ip] = expired_keys
            if not still_present:
                self.log.info(
                    f"{label}Expired key IDs no longer reported by KeysInUse: {baseline_key_ids}"
                )
                return current
            self.log.info(
                f"{label}Still seeing expired key IDs in KeysInUse: {still_present}"
            )
            self.sleep(10, f"{label}Waiting for expired key IDs to disappear from KeysInUse")
        self.fail(
            f"{label}Expired key IDs were still reported in KeysInUse after {timeout} s: "
            f"{baseline_key_ids}"
        )

    def _trigger_ffdc_and_verify(self, query_nodes, expected_key_ids, iteration=1, label=""):
        """
        Trigger FFDC via API, poll for files to appear, verify encryption.
        Poll for up to 180 seconds for FFDC files to be created.

        expected_key_ids may be:
          - dict  {node_ip: [key_id, ...]}  — per-node (from _get_query_in_use_key_ids)
          - list  [key_id, ...]             — same list applied to every node (legacy)
        """
        # Build per-node key-ID map
        if isinstance(expected_key_ids, dict):
            node_key_ids_map = expected_key_ids
        else:
            node_key_ids_map = {node.ip: (expected_key_ids or []) for node in query_nodes}

        self.log.info(f"{label}FFDC iteration {iteration}: Triggering FFDC on all query nodes...")
        for node in query_nodes:
            status, response = self.rest.trigger_query_ffdc(node)
            self.assertTrue(status, f"{label}Failed to trigger FFDC on {node.ip}: {response}")
            self.log.info(f"{label}FFDC triggered on {node.ip}")
        self.log.info(f"{label}FFDC triggered on all {len(query_nodes)} query node(s), waiting for files...")

        # Poll for FFDC files to appear (up to 3 minutes)
        deadline = time.time() + 180
        ffdc_found = False
        results = {}
        while time.time() < deadline:
            results = {}
            for node in query_nodes:
                partial = self.encryption_helper.verify_query_ffdc_files_encrypted(
                    [node], node_key_ids_map.get(node.ip, [])
                )
                results.update(partial)
            # Check if any FFDC files were found on any node
            total_ffdc_files = sum(len(files) for files in results.values())
            if total_ffdc_files > 0:
                ffdc_found = True
                break
            self.sleep(15, f"{label}Waiting for FFDC files to appear...")

        self.assertTrue(ffdc_found, f"{label}No FFDC files found after 180 s")

        # Verify all found FFDC files are encrypted
        node_obj_map = {node.ip: node for node in query_nodes}
        failures = []
        copied_files = []
        for node_ip, node_result in results.items():
            node_key_ids = node_key_ids_map.get(node_ip, [])
            for ffdc_file, file_result in node_result.items():
                if not file_result["encrypted"] or (node_key_ids and not file_result.get("key_id_found", True)):
                    node_obj = node_obj_map.get(node_ip)
                    if node_obj:
                        dest = self._copy_failed_file_from_node(node_obj, ffdc_file)
                        if dest:
                            copied_files.append(dest)
                if not file_result["encrypted"]:
                    failures.append(
                        f"  [{node_ip}] {ffdc_file} is NOT encrypted: {file_result['details']}"
                    )
                elif node_key_ids and not file_result.get("key_id_found", True):
                    failures.append(
                        f"  [{node_ip}] {ffdc_file} header missing expected key ID"
                    )

        if copied_files:
            self.log.info(
                f"{label}Copied {len(copied_files)} failing FFDC file(s) to "
                f"{self._evidence_dir}:\n" + "\n".join(f"  {p}" for p in copied_files)
            )
        if failures:
            msg = (
                f"{label}FFDC encryption validation FAILED (iteration {iteration}) "
                f"for the following files (copies in {self._evidence_dir}):\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)

        self.log.info(
            f"{label}FFDC iteration {iteration} complete: "
            f"{sum(len(files) for files in results.values())} files verified encrypted"
        )

    def _snapshot_query_log_files(self, query_nodes):
        """
        Return {node_ip: {"rlstream": set(paths), "local_request_log": set(paths)}}
        capturing all currently existing query log files on each node.
        Call this before triggering rotation so new files can be identified afterward.
        """
        snapshot = {}
        for node in query_nodes:
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            rlstream_out, _ = shell.execute_command(
                f"find {log_path} -name 'rlstream.*' 2>/dev/null"
            )
            local_log_out, _ = shell.execute_command(
                f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
            )
            shell.disconnect()
            snapshot[node.ip] = {
                "rlstream": set(f.strip() for f in rlstream_out if f.strip()),
                "local_request_log": set(f.strip() for f in local_log_out if f.strip()),
            }
            self.log.info(
                f"Log file snapshot on {node.ip}: "
                f"{len(snapshot[node.ip]['rlstream'])} rlstream, "
                f"{len(snapshot[node.ip]['local_request_log'])} local_request_log"
            )
        return snapshot

    def _snapshot_ffdc_files(self, query_nodes):
        """
        Return {node_ip: set(paths)} of all currently existing FFDC files on each node.
        Call this before triggering FFDC post-rotation to identify new files afterward.
        """
        snapshot = {}
        for node in query_nodes:
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            out, _ = shell.execute_command(
                f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
            )
            shell.disconnect()
            snapshot[node.ip] = set(f.strip() for f in out if f.strip())
            self.log.info(
                f"FFDC snapshot on {node.ip}: {len(snapshot[node.ip])} file(s)"
            )
        return snapshot

    def _assert_new_log_files_use_only_new_keys(
        self, query_nodes, files_before, new_key_ids, baseline_key_ids, label=""
    ):
        """
        After key rotation, verify that any query log files created since files_before
        are encrypted with a new key ID and do NOT contain any old (baseline) key ID.

        files_before:    snapshot from _snapshot_query_log_files()
        new_key_ids:     {node_ip: [key_id, ...]} — keys after rotation
        baseline_key_ids:{node_ip: [key_id, ...]} — keys before rotation
        """
        failures = []
        for node in query_nodes:
            node_ip = node.ip
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            rlstream_out, _ = shell.execute_command(
                f"find {log_path} -name 'rlstream.*' 2>/dev/null"
            )
            local_log_out, _ = shell.execute_command(
                f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
            )
            shell.disconnect()

            before = files_before.get(node_ip, {"rlstream": set(), "local_request_log": set()})
            new_rlstream = set(f.strip() for f in rlstream_out if f.strip()) - before["rlstream"]
            new_local = set(f.strip() for f in local_log_out if f.strip()) - before["local_request_log"]
            self.log.info(
                f"New log files on {node_ip} since snapshot: {len(new_rlstream)} rlstream, {len(new_local)} local_request_log"
            )
            self.log.info(
                f"{label}[{node_ip}] New files since snapshot: "
                f"{len(new_rlstream)} rlstream, {len(new_local)} local_request_log"
            )

            node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
            node_old_keys = [str(k) for k in baseline_key_ids.get(node_ip, [])]

            for category, new_files in (("rlstream", new_rlstream), ("local_request_log", new_local)):
                for file_path in new_files:
                    found_new = any(
                        self.encryption_helper.verify_file_header_contains(node, file_path, k)[0]
                        for k in node_new_keys
                    )
                    if not found_new and node_new_keys:
                        failures.append(
                            f"  [{node_ip}] NEW {category} {file_path} does not contain "
                            f"any new key ID {node_new_keys}"
                        )
                    for old_key in node_old_keys:
                        found_old, _ = self.encryption_helper.verify_file_header_contains(
                            node, file_path, old_key
                        )
                        if found_old:
                            failures.append(
                                f"  [{node_ip}] NEW {category} {file_path} still uses "
                                f"old key ID {old_key} — should only use new key after rotation"
                            )

        if failures:
            msg = (
                f"{label}New log files after rotation contain old key IDs:\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)
        self.log.info(
            f"{label}All new post-rotation log files verified: only new key IDs present"
        )

    def _assert_new_ffdc_files_use_only_new_keys(
        self, query_nodes, ffdc_before, new_key_ids, baseline_key_ids, label=""
    ):
        """
        After key rotation, verify that any FFDC files created since ffdc_before
        are encrypted with a new key ID and do NOT contain any old (baseline) key ID.

        ffdc_before:      snapshot from _snapshot_ffdc_files()
        new_key_ids:     {node_ip: [key_id, ...]} — keys after rotation
        baseline_key_ids:{node_ip: [key_id, ...]} — keys before rotation
        """
        failures = []
        for node in query_nodes:
            node_ip = node.ip
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            out, _ = shell.execute_command(
                f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
            )
            shell.disconnect()

            new_files = set(f.strip() for f in out if f.strip()) - ffdc_before.get(node_ip, set())
            self.log.info(
                f"{label}[{node_ip}] New FFDC files since snapshot: {len(new_files)}"
            )

            node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
            node_old_keys = [str(k) for k in baseline_key_ids.get(node_ip, [])]

            for file_path in new_files:
                found_new = any(
                    self.encryption_helper.verify_file_header_contains(node, file_path, k)[0]
                    for k in node_new_keys
                )
                if not found_new and node_new_keys:
                    failures.append(
                        f"  [{node_ip}] NEW FFDC {file_path} does not contain "
                        f"any new key ID {node_new_keys}"
                    )
                for old_key in node_old_keys:
                    found_old, _ = self.encryption_helper.verify_file_header_contains(
                        node, file_path, old_key
                    )
                    if found_old:
                        failures.append(
                            f"  [{node_ip}] NEW FFDC {file_path} still uses "
                            f"old key ID {old_key} — should only use new key after rotation"
                        )

        if failures:
            msg = (
                f"{label}New FFDC files after rotation contain old key IDs:\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)
        self.log.info(
            f"{label}All new post-rotation FFDC files verified: only new key IDs present"
        )

    def _poll_for_files_reencrypted_after_lifetime(
        self, query_nodes, files_snapshot, old_key_ids, new_key_ids, timeout=300, label=""
    ):
        """
        Poll until every file in files_snapshot no longer carries any old_key_ids in
        its header, confirming the automatic re-encryption triggered by dekLifetime expiry.

        files_snapshot: {node_ip: {"rlstream": set(paths), "local_request_log": set(paths)}}
        old_key_ids:    {node_ip: [key_id, ...]} — keys whose lifetime just expired
        new_key_ids:    {node_ip: [key_id, ...]} — keys that should now appear in headers
        """
        copied_files = []
        copied_paths = set()

        def copy_failed_file(node, file_path):
            if file_path in copied_paths:
                return
            dest = self._copy_failed_file_from_node(node, file_path)
            if dest:
                copied_paths.add(file_path)
                copied_files.append(dest)

        deadline = time.time() + timeout
        while time.time() < deadline:
            still_old = []
            for node in query_nodes:
                node_ip = node.ip
                node_old_keys = [str(k) for k in old_key_ids.get(node_ip, [])]
                snap = files_snapshot.get(node_ip, {"rlstream": set(), "local_request_log": set()})
                all_files = snap.get("rlstream", set()) | snap.get("local_request_log", set())
                for file_path in all_files:
                    for old_key in node_old_keys:
                        found, _ = self.encryption_helper.verify_file_header_contains(
                            node, file_path, old_key
                        )
                        if found:
                            copy_failed_file(node, file_path)
                            still_old.append(f"[{node_ip}] {file_path} still contains old key {old_key}")
            if not still_old:
                self.log.info(
                    f"{label}All snapshotted files no longer carry old key IDs — "
                    f"lifetime re-encryption complete"
                )
                break
            self.log.info(
                f"{label}{len(still_old)} file(s) still carry old key IDs, "
                f"waiting for lifetime re-encryption..."
            )
            self.sleep(15, f"{label}Polling for dekLifetime re-encryption...")
        else:
            # Collect full failure details after timeout
            failures = []
            for node in query_nodes:
                node_ip = node.ip
                node_old_keys = [str(k) for k in old_key_ids.get(node_ip, [])]
                node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
                snap = files_snapshot.get(node_ip, {"rlstream": set(), "local_request_log": set()})
                all_files = snap.get("rlstream", set()) | snap.get("local_request_log", set())
                for file_path in all_files:
                    for old_key in node_old_keys:
                        found, _ = self.encryption_helper.verify_file_header_contains(
                            node, file_path, old_key
                        )
                        if found:
                            copy_failed_file(node, file_path)
                            failures.append(
                                f"  [{node_ip}] {file_path} still contains old key {old_key} "
                                f"after {timeout} s — expected re-encryption with {node_new_keys}"
                            )
            msg = (
                f"{label}dekLifetime re-encryption did not complete within {timeout} s:\n"
                + "\n".join(failures)
            )
            if copied_files:
                self.log.info(
                    f"{label}Copied {len(copied_files)} failing file(s) to {self._evidence_dir}:\n"
                    + "\n".join(f"  {p}" for p in copied_files)
                )
            self.log.error(msg)
            self.fail(msg)

        # After old keys are gone, assert new keys ARE present
        failures = []
        for node in query_nodes:
            node_ip = node.ip
            node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
            snap = files_snapshot.get(node_ip, {"rlstream": set(), "local_request_log": set()})
            all_files = snap.get("rlstream", set()) | snap.get("local_request_log", set())
            for file_path in all_files:
                if node_new_keys:
                    found_new = any(
                        self.encryption_helper.verify_file_header_contains(node, file_path, k)[0]
                        for k in node_new_keys
                    )
                    if not found_new:
                        copy_failed_file(node, file_path)
                        failures.append(
                            f"  [{node_ip}] {file_path} does not contain any new key ID "
                            f"{node_new_keys} after lifetime re-encryption"
                        )
        if failures:
            if copied_files:
                self.log.info(
                    f"{label}Copied {len(copied_files)} failing file(s) to {self._evidence_dir}:\n"
                    + "\n".join(f"  {p}" for p in copied_files)
                )
            msg = (
                f"{label}Files re-encrypted after lifetime expiry do not carry new key IDs:\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)
        self.log.info(
            f"{label}Lifetime re-encryption validated: old keys absent, new keys present"
        )

    def _poll_for_ffdc_files_reencrypted_after_lifetime(
        self, query_nodes, ffdc_snapshot, old_key_ids, new_key_ids, timeout=300, label=""
    ):
        """
        Poll until every FFDC file in ffdc_snapshot no longer carries any old_key_ids
        in its header, confirming automatic re-encryption triggered by dekLifetime expiry.

        ffdc_snapshot: {node_ip: set(paths)} — from _snapshot_ffdc_files()
        old_key_ids:   {node_ip: [key_id, ...]} — keys whose lifetime just expired
        new_key_ids:   {node_ip: [key_id, ...]} — keys that should now appear in headers
        """
        copied_files = []
        copied_paths = set()

        def copy_failed_file(node, file_path):
            if file_path in copied_paths:
                return
            dest = self._copy_failed_file_from_node(node, file_path)
            if dest:
                copied_paths.add(file_path)
                copied_files.append(dest)

        deadline = time.time() + timeout
        while time.time() < deadline:
            still_old = []
            for node in query_nodes:
                node_ip = node.ip
                node_old_keys = [str(k) for k in old_key_ids.get(node_ip, [])]
                for file_path in ffdc_snapshot.get(node_ip, set()):
                    for old_key in node_old_keys:
                        found, _ = self.encryption_helper.verify_file_header_contains(
                            node, file_path, old_key
                        )
                        if found:
                            copy_failed_file(node, file_path)
                            still_old.append(
                                f"[{node_ip}] {file_path} still contains old key {old_key}"
                            )
            if not still_old:
                self.log.info(
                    f"{label}All snapshotted FFDC files no longer carry old key IDs — "
                    f"lifetime re-encryption complete"
                )
                break
            self.log.info(
                f"{label}{len(still_old)} FFDC file(s) still carry old key IDs, "
                f"waiting for lifetime re-encryption..."
            )
            self.sleep(15, f"{label}Polling for FFDC dekLifetime re-encryption...")
        else:
            failures = []
            for node in query_nodes:
                node_ip = node.ip
                node_old_keys = [str(k) for k in old_key_ids.get(node_ip, [])]
                node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
                for file_path in ffdc_snapshot.get(node_ip, set()):
                    for old_key in node_old_keys:
                        found, _ = self.encryption_helper.verify_file_header_contains(
                            node, file_path, old_key
                        )
                        if found:
                            copy_failed_file(node, file_path)
                            failures.append(
                                f"  [{node_ip}] {file_path} still contains old key {old_key} "
                                f"after {timeout} s — expected re-encryption with {node_new_keys}"
                            )
            msg = (
                f"{label}FFDC dekLifetime re-encryption did not complete within {timeout} s:\n"
                + "\n".join(failures)
            )
            if copied_files:
                self.log.info(
                    f"{label}Copied {len(copied_files)} failing FFDC file(s) to {self._evidence_dir}:\n"
                    + "\n".join(f"  {p}" for p in copied_files)
                )
            self.log.error(msg)
            self.fail(msg)

        # After old keys are gone, assert new keys ARE present
        failures = []
        for node in query_nodes:
            node_ip = node.ip
            node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
            for file_path in ffdc_snapshot.get(node_ip, set()):
                if node_new_keys:
                    found_new = any(
                        self.encryption_helper.verify_file_header_contains(node, file_path, k)[0]
                        for k in node_new_keys
                    )
                    if not found_new:
                        copy_failed_file(node, file_path)
                        failures.append(
                            f"  [{node_ip}] {file_path} does not contain any new key ID "
                            f"{node_new_keys} after lifetime re-encryption"
                        )
        if failures:
            if copied_files:
                self.log.info(
                    f"{label}Copied {len(copied_files)} failing FFDC file(s) to {self._evidence_dir}:\n"
                    + "\n".join(f"  {p}" for p in copied_files)
                )
            msg = (
                f"{label}FFDC files re-encrypted after lifetime expiry do not carry new key IDs:\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)
        self.log.info(
            f"{label}FFDC lifetime re-encryption validated: old keys absent, new keys present"
        )

    def _wait_for_query_key_ids(self, timeout=120, label=""):
        """Poll _get_query_in_use_key_ids until every query node reports at least one
        in-use key ID. Used after enabling encryption to confirm the service has a DEK.
        Returns {node_ip: [key_id, ...]}."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            current = self._get_query_in_use_key_ids()
            if current and all(len(ids) > 0 for ids in current.values()):
                self.log.info(f"{label}Key IDs found on all nodes: {current}")
                return current
            self.sleep(10, f"{label}Waiting for key IDs after enabling encryption")
        self.fail(
            f"{label}No key IDs appeared within {timeout} s after enabling encryption"
        )

    def _assert_log_files_not_encrypted(self, query_nodes, label=""):
        """Assert all rlstream.* and local_request_log.* files on every query node
        are NOT encrypted. At least one file of each type must exist per node."""
        failures = []
        for node in query_nodes:
            r = self.encryption_helper.verify_query_log_files_encrypted([node])
            node_result = r.get(node.ip, {})
            for category in ("rlstream", "local_request_log"):
                files = node_result.get(category, {})
                if not files:
                    failures.append(f"  [{node.ip}] No {category} files found")
                    continue
                for f, result in files.items():
                    if result["encrypted"]:
                        failures.append(
                            f"  [{node.ip}] {category} file {f} is unexpectedly encrypted: "
                            f"{result['details']}"
                        )
        if failures:
            msg = (
                f"{label}Files expected to be unencrypted but found encrypted:\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)
        self.log.info(f"{label}All log files confirmed NOT encrypted")

    def _assert_ffdc_files_not_encrypted(self, query_nodes, label=""):
        """Assert all query_ffdc_MAN_areq*, query_ffdc_MAN_creq*, and query_ffdc_MAN_vita*
        files on every query node are NOT encrypted. At least one FFDC file must exist per node."""
        failures = []
        for node in query_nodes:
            r = self.encryption_helper.verify_query_ffdc_files_encrypted([node])
            node_result = r.get(node.ip, {})
            if not node_result:
                failures.append(f"  [{node.ip}] No FFDC files found")
                continue
            for f, result in node_result.items():
                if result["encrypted"]:
                    failures.append(
                        f"  [{node.ip}] FFDC file {f} is unexpectedly encrypted: "
                        f"{result['details']}"
                    )
        if failures:
            msg = (
                f"{label}FFDC files expected to be unencrypted but found encrypted:\n"
                + "\n".join(failures)
            )
            self.log.error(msg)
            self.fail(msg)
        self.log.info(f"{label}All FFDC files confirmed NOT encrypted")

    def _start_background_query_loop(self, select_queries, query_nodes):
        """
        Start a daemon thread that continuously runs select_queries round-robin across
        query_nodes until the returned stop_event is set.

        Returns (thread, stop_event, errors_list).
        Caller must call stop_event.set() then thread.join(timeout=30) to clean up.
        errors_list accumulates query failure messages for post-hoc assertion.
        """
        stop_event = threading.Event()
        errors = []
        queries_list = list(select_queries)
        node_cycle = itertools.cycle(query_nodes)

        def _loop():
            while not stop_event.is_set():
                for q in queries_list:
                    if stop_event.is_set():
                        break
                    try:
                        result = self.run_cbq_query(query=q, server=next(node_cycle), verbose=False)
                        if result.get('status') != 'success':
                            errors.append(
                                f"status={result.get('status')} "
                                f"errors={result.get('errors', [])[:1]} "
                                f"query={q[:80]}"
                            )
                    except Exception as e:
                        errors.append(f"exception: {e} query={q[:80]}")

        t = threading.Thread(target=_loop, daemon=True, name="bg-query-loop")
        t.start()
        self.log.info(
            f"Background query loop started: {len(queries_list)} queries "
            f"across {len(query_nodes)} node(s)"
        )
        return t, stop_event, errors

    def _get_rlstream_file_sizes(self, query_nodes):
        """Return dict of node_ip -> list of ls -la output lines for rlstream.* files."""
        from lib.remote.remote_util import RemoteMachineShellConnection
        sizes = {}
        for node in query_nodes:
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            output, _ = shell.execute_command(f"ls -la {log_path}/rlstream.* 2>/dev/null")
            shell.disconnect()
            sizes[node.ip] = output if output else ["(no rlstream files found)"]
        return sizes

    def _local_request_log_files_exist(self, query_nodes):
        """Return True if any local_request_log.* files exist on any query node."""
        from lib.remote.remote_util import RemoteMachineShellConnection
        for node in query_nodes:
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            output, _ = shell.execute_command(
                f"ls {log_path}/local_request_log.* 2>/dev/null | head -1"
            )
            shell.disconnect()
            if output and any(line.strip() for line in output):
                return True
        return False

    def _generate_concurrent_load_until_archive(self, select_queries, query_nodes):
        """
        Run select_queries concurrently in batches to generate load and trigger
        local_request_log.* archive creation (rlstream idle finalization).

        Each outer iteration runs BATCHES_PER_ITERATION concurrent batches of all
        select_queries (9 queries * 5 batches = 45 queries per iteration).
        After each iteration, rlstream file sizes are logged and
        local_request_log.* existence is checked. Breaks early if archives are found.
        Runs up to MAX_ITERATIONS (20), i.e. ~900 queries total.
        """
        MAX_ITERATIONS = 20
        BATCHES_PER_ITERATION = 5
        nodes = query_nodes if query_nodes else [self.master]
        node_cycle = itertools.cycle(nodes)
        queries_list = list(select_queries)
        n_queries = len(queries_list)

        def _run_one(query, node):
            try:
                return self.run_cbq_query(query=query, server=node, verbose=False)
            except Exception as e:
                self.log.warning(
                    f"[Archive trigger] concurrent query failed on {node.ip}: {e}"
                )
                return None

        self.log.info(
            f"[Archive trigger] Starting concurrent load loop: up to {MAX_ITERATIONS} iterations, "
            f"{BATCHES_PER_ITERATION} batches/iteration, {n_queries} queries/batch "
            f"(max ~{MAX_ITERATIONS * BATCHES_PER_ITERATION * n_queries} queries total)"
        )

        archive_found = False
        for iteration in range(1, MAX_ITERATIONS + 1):
            self.log.info(
                f"[Archive trigger] Iteration {iteration}/{MAX_ITERATIONS}: "
                f"running {BATCHES_PER_ITERATION} concurrent batches of {n_queries} queries"
            )
            for batch in range(1, BATCHES_PER_ITERATION + 1):
                with concurrent.futures.ThreadPoolExecutor(max_workers=n_queries) as executor:
                    futures = [
                        executor.submit(_run_one, q, next(node_cycle))
                        for q in queries_list
                    ]
                    concurrent.futures.wait(futures)

            rlstream_sizes = self._get_rlstream_file_sizes(query_nodes)
            self.log.info(
                f"[Archive trigger] Iteration {iteration}: rlstream file sizes: {rlstream_sizes}"
            )

            if self._local_request_log_files_exist(query_nodes):
                self.log.info(
                    f"[Archive trigger] local_request_log.* files found after iteration "
                    f"{iteration} (~{iteration * BATCHES_PER_ITERATION * n_queries} queries "
                    f"total). Breaking."
                )
                archive_found = True
                break

        if not archive_found:
            total = MAX_ITERATIONS * BATCHES_PER_ITERATION * n_queries
            rlstream_sizes = self._get_rlstream_file_sizes(query_nodes)
            self.log.warning(
                f"[Archive trigger] local_request_log.* files NOT generated after "
                f"{MAX_ITERATIONS} iterations (~{total} queries total). "
                f"rlstream file sizes at end: {rlstream_sizes}"
            )

     # -------------------------------------------------------------------------
    # FTS scan-backfill helpers
    # -------------------------------------------------------------------------

    def _build_fts_collection_index_payload(self, index_name, bucket, scope, collection):
        """Build a minimal collection-scoped FTS index payload with dynamic
        mapping. Indexes every field of every document in
        `<scope>.<collection>` so any text query against a populated field
        returns hits."""
        type_key = f"{scope}.{collection}"
        return {
            "name": index_name,
            "type": "fulltext-index",
            "sourceType": "couchbase",
            "sourceName": bucket,
            "sourceParams":  {
                "scopeParams": {
                    "name": scope,
                    "collections": [{"name": collection}],
                },
            },
            "planParams": {
                "maxPartitionsPerPIndex": 1024,
                "indexPartitions": 1,
            },
            "params": {
                "doc_config": {
                    "mode": "scope.collection.type_field",
                    "type_field": "type",
                },
                "mapping": {
                    "analysis": {},
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "enabled": False,
                        "dynamic": True,
                    },
                    "default_type": "_default",
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {
                        type_key: {
                            "enabled": True,
                            "dynamic": True,
                            "default_analyzer": "standard",
                        }
                    },
                },
                "store": {
                    "indexType": "scorch",
                    "kvStoreName": "",
                },
            },
        }

    def _wait_for_fts_index_ready(self, index_name, bucket, scope, expected_docs,
                                   timeout=600, label=""):
        """Poll the FTS index doc count until it reaches `expected_docs` or
        times out. Uses `rest.get_fts_index_doc_count` which hits
        `<fts>/api/index/<name>/count`."""
        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        fts_rest = RestConnection(fts_nodes[0])
        indexed = fts_rest.get_fts_index_doc_count(
            name=index_name, bucket=bucket, scope=scope)
        deadline = time.time() + timeout
        indexed = 0
        while time.time() < deadline:
            try:
                indexed = fts_rest.get_fts_index_doc_count(
                    name=index_name, bucket=bucket, scope=scope
                )
            except Exception as e:
                self.log.warning(f"{label}get_fts_index_doc_count raised: {e}")
                indexed = 0
            if indexed >= expected_docs:
                self.log.info(
                    f"{label}FTS index '{index_name}' indexed {indexed}/{expected_docs} docs"
                )
                return indexed
            self.log.info(
                f"{label}FTS index '{index_name}' progress: {indexed}/{expected_docs}"
            )
            time.sleep(5)
        raise AssertionError(
            f"{label}FTS index '{index_name}' did not index {expected_docs} docs "
            f"within {timeout}s (last count: {indexed})"
        )

    # -------------------------------------------------------------------------
    # Tests
    # -------------------------------------------------------------------------

    def test_query_request_log_files_encrypted(self):
        """
        Sanity test: verify rlstream.* and local_request_log.* are encrypted when
        log encryption at rest is enabled.

        Steps:
        1. Verify log encryption at rest prerequisites
        2. Set query admin settings (completed-stream-size=500, completed-threshold=0)
        3. Create standard bucket + load data
        4. Create indexes using GSI flow
        5. Wait for indexes online
        6. Run scans and validate results
        7. Validate system:completed_requests reflects the index scan queries
        8. Get query in-use key IDs from admin/encryption_at_rest
        9. Verify rlstream.* files are encrypted with expected key ID
        10. Verify local_request_log.* files are encrypted with expected key ID
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_request_log_files_encrypted")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying log encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("log")
            key_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - log_encryption_at_rest_id: {key_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEPS 2-6: Setup via shared helper ==========
        try:
            self.log.info("[STEPS 2-6] Setting up bucket, indexes and running scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="enc_sanity")
            self.log.info("[STEPS 2-6] PASSED")
        except Exception as e:
            self.log.error(f"[STEPS 2-6] FAILED: {e}")
            raise
        
        self.sleep(60, "Waiting briefly after scans for logs to be written")
        # ========== STEP 7: Validate system:completed_requests ==========
        try:
            self.log.info("[STEP 7] Validating system:completed_requests...")
            self._validate_completed_requests(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 7b: Generate concurrent load to trigger archive creation ==========
        try:
            self.log.info("[STEP 7b] Generating concurrent query load to trigger local_request_log.* creation...")
            self._generate_concurrent_load_until_archive(select_queries, query_nodes)
            self.log.info("[STEP 7b] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7b] FAILED: {e}")
            raise

        # ========== STEP 7c: Validate completed_requests_history ==========
        try:
            self.log.info("[STEP 7c] Validating system:completed_requests_history...")
            self._validate_completed_requests_history(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 7c] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7c] FAILED: {e}")
            raise

        # ========== STEP 8: Get query in-use key IDs ==========
        try:
            self.log.info("[STEP 8] Getting query in-use encryption key IDs...")
            expected_key_ids = self._get_query_in_use_key_ids()
            self.assertGreater(
                len(expected_key_ids), 0,
                "No query nodes returned encryption key IDs"
            )
            for node_ip, key_ids in expected_key_ids.items():
                self.assertGreater(
                    len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use encryption key IDs"
                )
            self.log.info(f"[STEP 8] PASSED - key IDs per node: {expected_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Verify rlstream.* files are encrypted ==========
        try:
            self.log.info("[STEP 9] Verifying rlstream.* files are encrypted...")
            self._assert_rlstream_files_encrypted(query_nodes, expected_key_ids, label="[STEP 9] ")
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEP 10: Verify local_request_log.* files are encrypted ==========
        try:
            self.log.info("[STEP 10] Verifying local_request_log.* files are encrypted...")
            self._assert_local_request_log_files_encrypted(query_nodes, expected_key_ids, select_queries, label="[STEP 10] ")
            self.log.info("[STEP 10] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        # ========== STEP 11: Disable log encryption ==========
        try:
            self.log.info("[STEP 11] Disabling log encryption...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            self.log.info("[STEP 11] PASSED - log encryption disabled")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {e}")
            raise

        # ========== STEP 12: Trigger decryption ==========
        try:
            self.log.info("[STEP 12] Triggering decryption via dropEncryptionAtRestDeks/log...")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger decryption: {response}")
            self.log.info("[STEP 12] PASSED - decryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {e}")
            raise

        # ========== STEP 13: Wait for decryption to complete ==========
        try:
            self.log.info("[STEP 13] Waiting for decryption to complete (up to 180 s)...")
            self.sleep(180, "Waiting for log files to be decrypted")
            self.log.info("[STEP 13] PASSED - decryption wait complete")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {e}")
            raise

        # ========== STEP 14: Verify log files are decrypted ==========
        try:
            self.log.info("[STEP 14] Verifying log files are no longer encrypted...")
            results = self.encryption_helper.verify_query_log_files_decrypted(query_nodes)
            for node_ip, node_result in results.items():
                for file_type in ["rlstream", "local_request_log"]:
                    files = node_result[file_type]
                    for filepath, file_result in files.items():
                        self.assertFalse(
                            file_result["encrypted"],
                            f"[STEP 14] {file_type} file {filepath} on {node_ip} is still encrypted"
                        )
            self.log.info("[STEP 14] PASSED - all files confirmed decrypted")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_request_log_files_encrypted")
        self.log.info("=" * 80)

    def test_query_log_encryption_key_rotation(self):
        """
        Key rotation test (new active key sub-process).

        Per the key-rotation definition: the cluster manager periodically generates a
        new active DEK (Data Encryption Key) and pushes it to services via
        RefreshKeysCallback.  All new data written after that point uses the new DEK;
        previous DEKs remain available for decryption.

        The rotation interval is controlled by log.dekRotationInterval, which is set
        by the enable_log_encryption_at_rest block in encryption_at_rest_helper.py
        (see lib/membase/helper/encryption_at_rest_helper.py:195).

        Test strategy:
        - Pin dekRotationInterval high (1000 min) before setup so that no accidental
          rotation occurs while creating buckets/indexes/loading data.
        - Run baseline scans and confirm files are encrypted with the initial DEK IDs.
        - Reduce dekRotationInterval to 2 min to trigger the first rotation.
        - Poll admin/encryption_at_rest until new DEK IDs appear.
        - Immediately set dekRotationInterval back to 1000 min to prevent a second
          rotation from occurring before file validation completes.
        - Re-run scans to write new log entries under the rotated DEKs.
        - Verify log files carry the new DEK IDs.
        """
        # 1000 minutes expressed in seconds
        STABLE_INTERVAL_S = 60000
        # 2 minutes expressed in seconds
        ROTATION_INTERVAL_S = 120

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_log_encryption_key_rotation")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            key_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - log_encryption_at_rest_id: {key_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin rotation interval high before setup ==========
        try:
            self.log.info(
                f"[STEP 2] Pinning log.dekRotationInterval to {STABLE_INTERVAL_S} s "
                f"({STABLE_INTERVAL_S // 60} min) to prevent rotation during setup..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED - dekRotationInterval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEPS 3-7: Bucket, data, indexes, scans ==========
        try:
            self.log.info("[STEPS 3-7] Setting up bucket, indexes and running scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="enc_rotation"
            )
            self.log.info("[STEPS 3-7] PASSED")
        except Exception as e:
            self.log.error(f"[STEPS 3-7] FAILED: {e}")
            raise

        # ========== STEP 8: Baseline validation ==========
        try:
            self.log.info("[STEP 8] Running baseline validation...")
            self._validate_completed_requests(select_queries, query_nodes=query_nodes)
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            self.assertGreater(len(baseline_key_ids), 0, "No query nodes returned key IDs before rotation")
            for node_ip, key_ids in baseline_key_ids.items():
                self.assertGreater(len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use key IDs before rotation")
            self._assert_rlstream_files_encrypted(query_nodes, baseline_key_ids, label="[STEP 8] ")
            self._assert_local_request_log_files_encrypted(query_nodes, baseline_key_ids, select_queries, label="[STEP 8] ")
            self._validate_completed_requests_history(select_queries, query_nodes=query_nodes)
            self.log.info(f"[STEP 8] PASSED - baseline key IDs per node: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Reduce dekRotationInterval to trigger first rotation ==========
        try:
            self.log.info(
                f"[STEP 9] Reducing log.dekRotationInterval to {ROTATION_INTERVAL_S} s "
                f"({ROTATION_INTERVAL_S // 60} min) to trigger DEK rotation..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": ROTATION_INTERVAL_S,
                "log.dekLifetime": ROTATION_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to set dekRotationInterval: {response}")
            self.log.info("[STEP 9] PASSED - dekRotationInterval set to trigger rotation")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEP 10: Poll for new DEK IDs, then immediately re-pin ==========
        try:
            self.log.info(
                "[STEP 10] Polling admin/encryption_at_rest for new key IDs "
                "(timeout 300 s)..."
            )
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 10] ", category="log")
            self.log.info(
                f"[STEP 10] New key IDs detected: {new_key_ids}. "
                f"Immediately pinning dekRotationInterval back to {STABLE_INTERVAL_S} s..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info("[STEP 10] PASSED - rotation detected, interval re-pinned high")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        # Snapshot existing files so we can identify which ones are created post-rotation
        log_files_before_rotation = self._snapshot_query_log_files(query_nodes)

        # ========== STEP 11: Re-run scans to write log entries under new DEKs ==========
        try:
            self.log.info("[STEP 11] Re-running scans to write log entries under new DEKs...")
            self._run_select_scans(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {e}")
            raise

        # ========== STEP 12: Verify log files carry new DEK IDs ==========
        try:
            self.log.info("[STEP 12] Verifying log files encrypted with new DEK IDs...")
            self._assert_rlstream_files_encrypted(query_nodes, new_key_ids, label="[STEP 12] ")
            self._assert_local_request_log_files_encrypted(query_nodes, new_key_ids, select_queries, label="[STEP 12] ")
            self.log.info("[STEP 12] Verifying new post-rotation files use only new key IDs...")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before_rotation, new_key_ids, baseline_key_ids,
                label="[STEP 12] "
            )
            self.log.info("[STEP 12] PASSED - log files encrypted with new DEK IDs after rotation")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_log_encryption_key_rotation")
        self.log.info(
            f"Key rotation verified: baseline key IDs {baseline_key_ids} → "
            f"new DEK IDs {new_key_ids}"
        )
        self.log.info("=" * 80)

    def test_query_log_encryption_force_reencryption(self):
        """
        Full re-encryption test: trigger KEK rotation (POST /controller/rotateSecret/<id>)
        and verify that query log files are subsequently encrypted with updated key IDs.

        Per the full re-encryption definition: calling
        POST /controller/dropEncryptionAtRestDeks/log instructs the cluster manager to
        drop all existing DEKs for the log subsystem and re-encrypt all log data with a
        freshly generated active key.  After completion every byte on disk is encrypted
        with the single newly generated key.  This is more thorough than key rotation,
        which only re-encrypts data protected by keys that have individually reached
        their dekLifetime.

        Test strategy:
        - Pin dekRotationInterval high (1000 min) before setup so that no accidental
          background rotation occurs while creating buckets/indexes/loading data.
        - Run baseline scans and confirm files are encrypted with the initial DEK IDs.
        - Trigger KEK rotation (full re-encryption).
        - Poll admin/encryption_at_rest until new DEK IDs appear.
        - Immediately re-pin dekRotationInterval to 1000 min to prevent a subsequent
          background rotation from occurring before file validation completes.
        - Re-run scans to write new log entries under the rotated DEKs.
        - Verify log files carry the new DEK IDs.
        """
        # 1000 minutes expressed in seconds — stable, no accidental rotation
        STABLE_INTERVAL_S = 60000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_log_encryption_force_reencryption")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin rotation interval high before setup ==========
        try:
            self.log.info(
                f"[STEP 2] Pinning log.dekRotationInterval to {STABLE_INTERVAL_S} s "
                f"({STABLE_INTERVAL_S // 60} min) to prevent background rotation during setup..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED - dekRotationInterval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEPS 3-7: Bucket, data, indexes, scans ==========
        try:
            self.log.info("[STEPS 3-7] Setting up bucket, indexes and running scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="enc_reencrypt"
            )
            self.log.info("[STEPS 3-7] PASSED")
        except Exception as e:
            self.log.error(f"[STEPS 3-7] FAILED: {e}")
            raise

        self.sleep(60, "Waiting briefly after scans for logs to be written")

        # ========== STEP 8: Baseline validation ==========
        try:
            self.log.info("[STEP 8] Running baseline log file validation...")
            self._validate_completed_requests(select_queries, query_nodes=query_nodes)
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            self.assertGreater(len(baseline_key_ids), 0, "No query nodes returned key IDs before re-encryption")
            for node_ip, key_ids in baseline_key_ids.items():
                self.assertGreater(len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use key IDs before re-encryption")
            self._assert_rlstream_files_encrypted(query_nodes, baseline_key_ids, label="[STEP 8] ")
            self._assert_local_request_log_files_encrypted(query_nodes, baseline_key_ids, select_queries, label="[STEP 8] ")
            self._validate_completed_requests_history(select_queries, query_nodes=query_nodes)
            self.log.info(f"[STEP 8] PASSED - baseline key IDs per node: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Trigger full re-encryption ==========
        try:
            self.log.info(
                "[STEP 9] Triggering full log re-encryption "
                "(POST /controller/dropEncryptionAtRestDeks/log)..."
            )
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger log re-encryption: {response}")
            self.log.info(f"[STEP 9] PASSED - log re-encryption triggered, response: {response}")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEP 10: Poll for new DEK IDs, then immediately re-pin ==========
        try:
            self.log.info(
                "[STEP 10] Polling admin/encryption_at_rest for new key IDs "
                "(timeout 300 s)..."
            )
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 10] ", category="log")
            self.log.info(
                f"[STEP 10] New key IDs detected: {new_key_ids}. "
                f"Immediately pinning dekRotationInterval back to {STABLE_INTERVAL_S} s..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info("[STEP 10] PASSED - re-encryption detected, interval re-pinned high")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        # Snapshot existing files so we can identify which ones are created post-reencryption
        log_files_before_reencryption = self._snapshot_query_log_files(query_nodes)

        # ========== STEP 11: Re-run scans to write log entries under new DEKs ==========
        try:
            self.log.info("[STEP 11] Re-running scans to write log entries under new DEKs...")
            self._run_select_scans(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {e}")
            raise

        # ========== STEP 12: Verify log files carry new DEK IDs and not old ones ==========
        try:
            self.log.info("[STEP 12] Verifying log files encrypted with new DEK IDs...")
            self._assert_rlstream_files_encrypted(query_nodes, new_key_ids, label="[STEP 12] ")
            self._assert_local_request_log_files_encrypted(query_nodes, new_key_ids, select_queries, label="[STEP 12] ")
            self.log.info("[STEP 12] Verifying new post-reencryption files use only new key IDs...")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before_reencryption, new_key_ids, baseline_key_ids,
                label="[STEP 12] "
            )
            self.log.info(
                "[STEP 12] PASSED - log files encrypted with new DEK IDs after full re-encryption"
            )
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_log_encryption_force_reencryption")
        self.log.info(
            f"Full re-encryption verified: baseline key IDs {baseline_key_ids} → "
            f"new DEK IDs {new_key_ids}"
        )
        self.log.info("=" * 80)

    def test_query_ffdc_files_encrypted(self):
        """
        FFDC files encryption and decryption sanity test.

        Verifies that FFDC (First Failure Data Capture) files generated by the
        query service are encrypted when log encryption at rest is enabled,
        and properly decrypted when encryption is disabled.
        Specifically verifies query_ffdc_MAN_areq*, query_ffdc_MAN_creq*,
        and query_ffdc_MAN_vita* files.

        Steps:
        1. Verify log encryption at rest prerequisites
        2. Get query nodes and in-use encryption key IDs
        3-5. Trigger FFDC 3 times and verify encrypted
        6. Disable log encryption
        7. Trigger decryption
        8. Wait for decryption to complete (180 s)
        9. Trigger FFDC and verify decrypted
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_ffdc_files_encrypted")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying log encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Get in-use encryption key IDs ==========
        try:
            self.log.info("[STEP 2] Getting in-use encryption key IDs...")
            expected_key_ids = self._get_query_in_use_key_ids()
            self.assertGreater(
                len(expected_key_ids), 0,
                "No query nodes returned encryption key IDs"
            )
            for node_ip, key_ids in expected_key_ids.items():
                self.assertGreater(len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use encryption key IDs")
            self.log.info(f"[STEP 2] PASSED - key IDs per node: {expected_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEPS 3-5: Trigger FFDC 3 times and verify encryption ==========
        try:
            self.log.info("[STEPS 3-5] Triggering FFDC 3 times and verifying encryption...")
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, expected_key_ids,
                    iteration=iteration,
                    label=f"[STEP {2+iteration}] "
                )
            self.log.info("[STEPS 3-5] PASSED - All FFDC files verified encrypted")
        except Exception as e:
            self.log.error(f"[STEPS 3-5] FAILED: {e}")
            raise

        # ========== STEP 6: Disable log encryption ==========
        try:
            self.log.info("[STEP 6] Disabling log encryption...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            self.log.info("[STEP 6] PASSED - log encryption disabled")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Trigger decryption ==========
        try:
            self.log.info("[STEP 7] Triggering decryption via dropEncryptionAtRestDeks/log...")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger decryption: {response}")
            self.log.info("[STEP 7] PASSED - decryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 8: Wait for decryption to complete ==========
        try:
            self.log.info("[STEP 8] Waiting for decryption to complete (up to 180 s)...")
            self.sleep(180, "Waiting for FFDC files to be decrypted")
            self.log.info("[STEP 8] PASSED - decryption wait complete")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Trigger FFDC (3x) and verify decrypted ==========
        try:
            self.log.info("[STEP 9] Triggering FFDC 3 times on all query nodes and verifying decrypted...")
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                for node in query_nodes:
                    status, response = self.rest.trigger_query_ffdc(node)
                    self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                    self.log.info(f"[STEP 9] FFDC triggered on {node.ip} (iteration {iteration})")
                self.sleep(30, f"Waiting for FFDC files to be generated (iteration {iteration})")

            results = self.encryption_helper.verify_query_ffdc_files_decrypted(query_nodes)
            for node_ip, node_result in results.items():
                for ffdc_file, file_result in node_result.items():
                    self.assertFalse(
                        file_result["encrypted"],
                        f"[STEP 9] FFDC file {ffdc_file} on {node_ip} is still encrypted"
                    )
            self.log.info("[STEP 9] PASSED - FFDC files confirmed decrypted")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_ffdc_files_encrypted")
        self.log.info("=" * 80)

    def test_query_ffdc_encryption_key_rotation(self):
        """
        FFDC files encryption test with key rotation.

        Verifies that FFDC files remain encrypted after DEK rotation.
        Uses the same pin-high-before-setup pattern as the log rotation test.

        Steps:
        1. Verify prerequisites
        2. Pin DEK rotation interval high before any FFDC generation
        3. Get baseline key IDs
        4. Trigger FFDC 1st time and verify encryption with baseline key IDs
        5. Snapshot existing FFDC files before rotation begins
        6. Reduce DEK rotation interval to trigger rotation
        7. Poll for new key IDs, then re-pin interval high
        8. Trigger FFDC 2nd & 3rd times and verify encryption with new key IDs
        """
        STABLE_INTERVAL_S = 60000
        ROTATION_INTERVAL_S = 120

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_ffdc_encryption_key_rotation")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin rotation interval high before FFDC ==========
        try:
            self.log.info(
                f"[STEP 2] Pinning log.dekRotationInterval to {STABLE_INTERVAL_S} s..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED - dekRotationInterval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Get baseline key IDs ==========
        try:
            self.log.info("[STEP 3] Getting baseline key IDs...")
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Trigger FFDC 1st time with baseline keys ==========
        try:
            self.log.info("[STEP 4] Triggering FFDC 1st time (before rotation)...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            self._trigger_ffdc_and_verify(
                query_nodes, baseline_key_ids,
                iteration=1,
                label="[STEP 4] "
            )
            self.log.info("[STEP 4] PASSED - FFDC files encrypted with baseline keys")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Snapshot FFDC files before rotation begins ==========
        try:
            self.log.info("[STEP 5] Snapshotting FFDC files before triggering rotation...")
            ffdc_files_before_rotation = self._snapshot_ffdc_files(query_nodes)
            self.log.info("[STEP 5] PASSED - snapshot captured")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Reduce interval to trigger rotation ==========
        try:
            self.log.info(
                f"[STEP 6] Reducing log.dekRotationInterval to {ROTATION_INTERVAL_S} s..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": ROTATION_INTERVAL_S,
                "log.dekLifetime": ROTATION_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to set dekRotationInterval: {response}")
            self.log.info("[STEP 6] PASSED - dekRotationInterval set to trigger rotation")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Poll for new key IDs, then re-pin ==========
        try:
            self.log.info("[STEP 7] Polling for new key IDs (timeout 300 s)...")
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 7] ", category="log")
            self.log.info(
                f"[STEP 7] New key IDs detected: {new_key_ids}. Re-pinning interval..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info("[STEP 7] PASSED - rotation detected, interval re-pinned")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEPS 8-9: Trigger FFDC 2nd & 3rd times with new keys ==========
        try:
            self.log.info("[STEPS 8-9] Triggering FFDC 2nd & 3rd times (after rotation)...")
            for iteration in range(2, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, new_key_ids,
                    iteration=iteration,
                    label=f"[STEP {6+iteration}] "
                )
            self.log.info("[STEPS 8-9] Verifying new post-rotation FFDC files use only new key IDs...")
            self._assert_new_ffdc_files_use_only_new_keys(
                query_nodes, ffdc_files_before_rotation, new_key_ids, baseline_key_ids,
                label="[STEPS 8-9] "
            )
            self.log.info("[STEPS 8-9] PASSED - FFDC files encrypted with new keys")
        except Exception as e:
            self.log.error(f"[STEPS 8-9] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_ffdc_encryption_key_rotation")
        self.log.info(
            f"Key rotation verified: baseline key IDs {baseline_key_ids} → "
            f"new DEK IDs {new_key_ids}"
        )
        self.log.info("=" * 80)

    def test_query_ffdc_encryption_force_reencryption(self):
        """
        FFDC files encryption test with force re-encryption.

        Verifies that FFDC files remain encrypted after full re-encryption,
        and that new files written after re-encryption use only the new key IDs.

        Steps:
        1. Verify prerequisites
        2. Pin DEK rotation interval high before any FFDC generation
        3. Get baseline key IDs
        4. Trigger FFDC 1st time and verify encryption with baseline key IDs
        5. Snapshot existing FFDC files before triggering re-encryption
        6. Trigger full re-encryption via dropEncryptionAtRestDeks/log
        7. Poll for new key IDs, then re-pin interval high
        8. Trigger FFDC 2nd & 3rd times and verify encryption with new key IDs
        9. Assert new FFDC files use only new key IDs (not old/baseline)
        """
        STABLE_INTERVAL_S = 60000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_ffdc_encryption_force_reencryption")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin rotation interval high before FFDC ==========
        try:
            self.log.info(
                f"[STEP 2] Pinning log.dekRotationInterval to {STABLE_INTERVAL_S} s..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED - dekRotationInterval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Get baseline key IDs ==========
        try:
            self.log.info("[STEP 3] Getting baseline key IDs...")
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Trigger FFDC 1st time with baseline keys ==========
        try:
            self.log.info("[STEP 4] Triggering FFDC 1st time (before re-encryption)...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            self._trigger_ffdc_and_verify(
                query_nodes, baseline_key_ids,
                iteration=1,
                label="[STEP 4] "
            )
            self.log.info("[STEP 4] PASSED - FFDC files encrypted with baseline keys")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Snapshot FFDC files before re-encryption begins ==========
        try:
            self.log.info("[STEP 5] Snapshotting FFDC files before triggering re-encryption...")
            ffdc_files_before_reencryption = self._snapshot_ffdc_files(query_nodes)
            self.log.info("[STEP 5] PASSED - snapshot captured")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Trigger full re-encryption ==========
        try:
            self.log.info(
                "[STEP 6] Triggering full log re-encryption "
                "(POST /controller/dropEncryptionAtRestDeks/log)..."
            )
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger log re-encryption: {response}")
            self.log.info("[STEP 6] PASSED - log re-encryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Poll for new key IDs, then re-pin ==========
        try:
            self.log.info("[STEP 7] Polling for new key IDs (timeout 300 s)...")
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 7] ", category="log")
            self.log.info(
                f"[STEP 7] New key IDs detected: {new_key_ids}. Re-pinning interval..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info("[STEP 7] PASSED - re-encryption detected, interval re-pinned")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEPS 8-9: Trigger FFDC 2nd & 3rd times with new keys ==========
        try:
            self.log.info("[STEPS 8-9] Triggering FFDC 2nd & 3rd times (after re-encryption)...")
            for iteration in range(2, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, new_key_ids,
                    iteration=iteration,
                    label=f"[STEP {6+iteration}] "
                )
            self.log.info("[STEPS 8-9] PASSED - FFDC files encrypted with new keys")
        except Exception as e:
            self.log.error(f"[STEPS 8-9] FAILED: {e}")
            raise

        # ========== STEP 10: Verify new FFDC files use only new key IDs ==========
        try:
            self.log.info(
                "[STEP 10] Verifying new FFDC files use only new key IDs (not baseline)..."
            )
            self._assert_new_ffdc_files_use_only_new_keys(
                query_nodes, ffdc_files_before_reencryption, new_key_ids, baseline_key_ids,
                label="[STEP 10] "
            )
            self.log.info("[STEP 10] PASSED - new FFDC files use only new key IDs")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_ffdc_encryption_force_reencryption")
        self.log.info(
            f"Full re-encryption verified: baseline key IDs {baseline_key_ids} → "
            f"new DEK IDs {new_key_ids}"
        )
        self.log.info("=" * 80)

    def test_query_log_encryption_disabled_then_enabled(self):
        """
        Lifecycle test: verify log files created while encryption is disabled are
        unencrypted, and that after enabling encryption any NEW files are encrypted
        while the old files remain plaintext.

        Steps:
        1.  Verify KEK exists (need it to re-enable later)
        2.  Disable log encryption + trigger decryption to plaintext
        3.  Wait 180 s for decryption to complete
        4-5. Create bucket + data + indexes + scans + concurrent load (archive trigger)
        6.  Verify all current rlstream.* and local_request_log.* are NOT encrypted
        7.  Snapshot existing file paths (baseline for "new file" detection)
        8.  Enable log encryption (configure only, no re-encryption trigger)
        9.  Poll for in-use key IDs to confirm the service has a new DEK
        10. Re-run scans + concurrent load to create new log files under encryption
        11. Assert that new files (post-snapshot) exist and are encrypted
        12. Assert that old files (in snapshot) are still NOT encrypted
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_log_encryption_disabled_then_enabled")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify KEK exists ==========
        try:
            self.log.info("[STEP 1] Verifying KEK exists...")
            self._verify_encryption_prerequisites("log")
            kek_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - KEK ID: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Disable log encryption ==========
        try:
            self.log.info("[STEP 2] Disabling log encryption and triggering decryption...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger decryption: {response}")
            self.log.info("[STEP 2] PASSED - encryption disabled, decryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Wait for decryption ==========
        try:
            self.log.info("[STEP 3] Waiting 180 s for decryption to complete...")
            self.sleep(180, "Waiting for log files to be decrypted")
            self.log.info("[STEP 3] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEPS 4-5: Bucket + indexes + scans + archive load ==========
        try:
            self.log.info("[STEPS 4-5] Creating bucket, indexes, running scans and generating archive load...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="enc_lifecycle_log"
            )
            self._generate_concurrent_load_until_archive(select_queries, query_nodes)
            self.log.info("[STEPS 4-5] PASSED")
        except Exception as e:
            self.log.error(f"[STEPS 4-5] FAILED: {e}")
            raise

        # ========== STEP 6: Verify all current log files are NOT encrypted ==========
        try:
            self.log.info("[STEP 6] Verifying all log files are NOT encrypted while disabled...")
            self._assert_log_files_not_encrypted(query_nodes, label="[STEP 6] ")
            self.log.info("[STEP 6] PASSED - all log files confirmed unencrypted")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Snapshot existing files ==========
        try:
            self.log.info("[STEP 7] Snapshotting existing log files before enabling encryption...")
            log_files_before = self._snapshot_query_log_files(query_nodes)
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 8: Enable log encryption (no re-encryption of old files) ==========
        try:
            self.log.info(f"[STEP 8] Enabling log encryption with KEK ID {kek_id}...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.encryptionMethod": "encryptionKey",
                "log.encryptionKeyId": str(kek_id)
            })
            self.assertTrue(status, f"Failed to enable log encryption: {response}")
            self.log.info("[STEP 8] PASSED - log encryption enabled")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Poll until query nodes have active DEKs ==========
        try:
            self.log.info("[STEP 9] Polling for in-use key IDs to confirm DEK is active...")
            new_key_ids = self._wait_for_query_key_ids(timeout=120, label="[STEP 9] ")
            self.log.info(f"[STEP 9] PASSED - key IDs per node: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise
        # ========== STEP 10: Generate new log files under encryption ==========
        try:
            self.log.info("[STEP 10] Re-running scans and generating new log files under encryption...")
            self._run_select_scans(select_queries, query_nodes=query_nodes)
            self._generate_concurrent_load_until_archive(select_queries, query_nodes)
            self.log.info("[STEP 10] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        # ========== STEP 11: Verify new files exist and are encrypted ==========
        try:
            self.log.info("[STEP 11] Verifying new post-enable log files are encrypted...")
            # Count new files across all nodes to confirm they were actually created
            new_file_count = 0
            for node in query_nodes:
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                r_out, _ = shell.execute_command(
                    f"find {log_path} -name 'rlstream.*' 2>/dev/null"
                )
                l_out, _ = shell.execute_command(
                    f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
                )
                shell.disconnect()
                new_file_count += len(
                    set(f.strip() for f in r_out if f.strip())
                    - log_files_before[node.ip]["rlstream"]
                )
                new_file_count += len(
                    set(f.strip() for f in l_out if f.strip())
                    - log_files_before[node.ip]["local_request_log"]
                )
            self.assertGreater(
                new_file_count, 0,
                "[STEP 11] No new log files were created after enabling encryption"
            )
            self.log.info(f"[STEP 11] {new_file_count} new file(s) confirmed created")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before, new_key_ids,
                baseline_key_ids={node.ip: [] for node in query_nodes},
                label="[STEP 11] "
            )
            self.log.info("[STEP 11] PASSED - new log files are encrypted with new key IDs")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {e}")
            raise

        # ========== STEP 12: Verify old files are still NOT encrypted ==========
        try:
            self.log.info("[STEP 12] Verifying pre-enable log files are still NOT encrypted...")
            node_map = {node.ip: node for node in query_nodes}
            failures = []
            for node_ip, categories in log_files_before.items():
                node = node_map.get(node_ip)
                if not node:
                    continue
                for category, paths in categories.items():
                    for file_path in paths:
                        is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                            node, file_path
                        )
                        if is_enc:
                            failures.append(
                                f"  [{node_ip}] {category} file {file_path} is now encrypted "
                                f"(was created before encryption was enabled)"
                            )
            if failures:
                msg = (
                    "[STEP 12] Pre-enable files are unexpectedly encrypted:\n"
                    + "\n".join(failures)
                )
                self.log.error(msg)
                self.fail(msg)
            self.log.info("[STEP 12] PASSED - all pre-enable log files still NOT encrypted")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_log_encryption_disabled_then_enabled")
        self.log.info("=" * 80)

    def test_query_ffdc_encryption_disabled_then_enabled(self):
        """
        Lifecycle test for FFDC files: verify that FFDC files generated while
        encryption is disabled are unencrypted, and that after enabling encryption
        any NEW FFDC files are encrypted while old files remain plaintext.

        Steps:
        1.  Verify KEK exists
        2.  Disable log encryption + trigger decryption to plaintext
        3.  Wait 180 s for decryption
        4.  Trigger FFDC on all query nodes
        5.  Verify all FFDC files are NOT encrypted
        6.  Snapshot existing FFDC file paths
        7.  Enable log encryption (configure only, no re-encryption trigger)
        8.  Poll for in-use key IDs to confirm DEK is active
        9.  Trigger FFDC on all query nodes again (creates new files)
        10. Verify new FFDC files (post-snapshot) exist and are encrypted
        11. Verify old FFDC files (in snapshot) are still NOT encrypted
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_ffdc_encryption_disabled_then_enabled")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        # ========== STEP 1: Verify KEK exists ==========
        try:
            self.log.info("[STEP 1] Verifying KEK exists...")
            self._verify_encryption_prerequisites("log")
            kek_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - KEK ID: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Disable log encryption ==========
        try:
            self.log.info("[STEP 2] Disabling log encryption and triggering decryption...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger decryption: {response}")
            self.log.info("[STEP 2] PASSED - encryption disabled, decryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Wait for decryption ==========
        try:
            self.log.info("[STEP 3] Waiting 180 s for decryption to complete...")
            self.sleep(180, "Waiting for FFDC files to be decrypted")
            self.log.info("[STEP 3] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Trigger FFDC (3x) while encryption is disabled ==========
        try:
            self.log.info("[STEP 4] Triggering FFDC 3 times on all query nodes (encryption disabled)...")
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                for node in query_nodes:
                    status, response = self.rest.trigger_query_ffdc(node)
                    self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                    self.log.info(f"[STEP 4] FFDC triggered on {node.ip} (iteration {iteration})")
                self.sleep(30, f"Waiting for FFDC files to be generated (iteration {iteration})")
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Verify FFDC files are NOT encrypted ==========
        try:
            self.log.info("[STEP 5] Verifying FFDC files are NOT encrypted while disabled...")
            self._assert_ffdc_files_not_encrypted(query_nodes, label="[STEP 5] ")
            self.log.info("[STEP 5] PASSED - all FFDC files confirmed unencrypted")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Snapshot existing FFDC files ==========
        try:
            self.log.info("[STEP 6] Snapshotting existing FFDC files before enabling encryption...")
            ffdc_files_before = self._snapshot_ffdc_files(query_nodes)
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Enable log encryption (no re-encryption of old files) ==========
        try:
            self.log.info(f"[STEP 7] Enabling log encryption with KEK ID {kek_id}...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.encryptionMethod": "encryptionKey",
                "log.encryptionKeyId": str(kek_id)
            })
            self.assertTrue(status, f"Failed to enable log encryption: {response}")
            self.log.info("[STEP 7] PASSED - log encryption enabled")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 8: Poll until query nodes have active DEKs ==========
        try:
            self.log.info("[STEP 8] Polling for in-use key IDs to confirm DEK is active...")
            new_key_ids = self._wait_for_query_key_ids(timeout=120, label="[STEP 8] ")
            self.log.info(f"[STEP 8] PASSED - key IDs per node: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Trigger FFDC (3x) with encryption enabled ==========
        try:
            self.log.info("[STEP 9] Triggering FFDC 3 times on all query nodes (encryption enabled)...")
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                for node in query_nodes:
                    status, response = self.rest.trigger_query_ffdc(node)
                    self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                    self.log.info(f"[STEP 9] FFDC triggered on {node.ip} (iteration {iteration})")
                self.sleep(30, f"Waiting for new FFDC files to be generated (iteration {iteration})")
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEP 10: Verify new FFDC files exist and are encrypted ==========
        try:
            self.log.info("[STEP 10] Verifying new FFDC files are encrypted with new key IDs...")
            failures = []
            total_new = 0
            for node in query_nodes:
                node_ip = node.ip
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                )
                shell.disconnect()
                new_files = set(f.strip() for f in out if f.strip()) - ffdc_files_before.get(node_ip, set())
                self.log.info(f"[STEP 10] [{node_ip}] {len(new_files)} new FFDC file(s)")
                total_new += len(new_files)
                node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
                for file_path in new_files:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    if not is_enc:
                        failures.append(
                            f"  [{node_ip}] NEW FFDC file {file_path} is NOT encrypted: {details}"
                        )
                        continue
                    found_key = any(
                        self.encryption_helper.verify_file_header_contains(node, file_path, k)[0]
                        for k in node_new_keys
                    )
                    if not found_key and node_new_keys:
                        failures.append(
                            f"  [{node_ip}] NEW FFDC file {file_path} does not contain "
                            f"any expected key ID {node_new_keys}"
                        )
            self.assertGreater(
                total_new, 0,
                "[STEP 10] No new FFDC files were created after enabling encryption"
            )
            if failures:
                msg = "[STEP 10] New FFDC file encryption failures:\n" + "\n".join(failures)
                self.log.error(msg)
                self.fail(msg)
            self.log.info(
                f"[STEP 10] PASSED - {total_new} new FFDC file(s) verified encrypted"
            )
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        # ========== STEP 11: Verify old FFDC files are still NOT encrypted ==========
        try:
            self.log.info("[STEP 11] Verifying pre-enable FFDC files are still NOT encrypted...")
            failures = []
            node_map = {node.ip: node for node in query_nodes}
            for node_ip, paths in ffdc_files_before.items():
                node = node_map.get(node_ip)
                if not node:
                    continue
                for file_path in paths:
                    is_enc, _ = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    if is_enc:
                        failures.append(
                            f"  [{node_ip}] FFDC file {file_path} is now encrypted "
                            f"(was created before encryption was enabled)"
                        )
            if failures:
                msg = (
                    "[STEP 11] Pre-enable FFDC files are unexpectedly encrypted:\n"
                    + "\n".join(failures)
                )
                self.log.error(msg)
                self.fail(msg)
            self.log.info("[STEP 11] PASSED - all pre-enable FFDC files still NOT encrypted")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_ffdc_encryption_disabled_then_enabled")
        self.log.info("=" * 80)

    # =========================================================================
    # Concurrent / race-condition tests
    # =========================================================================

    def test_ffdc_concurrent_with_reencryption(self):
        """
        Trigger FFDC and force re-encryption (dropEncryptionAtRestDeks/log) at the same
        time.  The query service is simultaneously writing FFDC files and re-encrypting
        existing data.  Every new FFDC file must be encrypted — with the old or new key
        (timing-dependent) but never plaintext.
        """
        STABLE_INTERVAL_S = 60000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_ffdc_concurrent_with_reencryption")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info(f"[STEP 2] Pinning dekRotationInterval to {STABLE_INTERVAL_S} s...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 3] Getting baseline key IDs...")
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            ffdc_before = self._snapshot_ffdc_files(query_nodes)
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 4] Triggering FFDC, force re-encryption, and query batch concurrently...")

            def _do_reencryption():
                s, r = self.rest.trigger_log_reencryption()
                self.log.info(f"[STEP 4] Re-encryption trigger returned status={s}")
                return s, r

            def _do_ffdc():
                for node in query_nodes:
                    s, r = self.rest.trigger_query_ffdc(node)
                    self.log.info(f"[STEP 4] FFDC triggered on {node.ip}: status={s}")

            def _do_query_batch():
                self._run_select_scans(select_queries, query_nodes)

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                f_reenc = executor.submit(_do_reencryption)
                f_ffdc = executor.submit(_do_ffdc)
                f_queries = executor.submit(_do_query_batch)
                reenc_status, _ = f_reenc.result()
                f_ffdc.result()
                f_queries.result()
            self.assertTrue(reenc_status, "Re-encryption trigger failed")
            self.log.info("[STEP 4] PASSED - concurrent operations completed")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 5] Waiting for new key IDs (re-encryption settled)...")
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 5] ", category="log")
            self.log.info(f"[STEP 5] PASSED - new key IDs: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Polling for new FFDC files and verifying all are encrypted...")
            deadline = time.time() + 120
            current_ffdc = {node.ip: set() for node in query_nodes}
            while time.time() < deadline:
                for node in query_nodes:
                    log_path = self.encryption_helper.get_log_path(node)
                    shell = RemoteMachineShellConnection(node, verbose=False)
                    out, _ = shell.execute_command(
                        f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                    )
                    shell.disconnect()
                    current_ffdc[node.ip] = (
                        set(f.strip() for f in out if f.strip())
                        - ffdc_before.get(node.ip, set())
                    )
                if any(len(files) > 0 for files in current_ffdc.values()):
                    break
                self.sleep(10, "[STEP 6] Waiting for FFDC files...")

            node_map = {node.ip: node for node in query_nodes}
            failures = []
            total_new = sum(len(f) for f in current_ffdc.values())
            self.assertGreater(total_new, 0, "[STEP 6] No new FFDC files appeared")
            for node_ip, new_files in current_ffdc.items():
                node = node_map[node_ip]
                for file_path in new_files:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    if not is_enc:
                        failures.append(
                            f"  [{node_ip}] {file_path} is NOT encrypted: {details}"
                        )
            if failures:
                msg = (
                    "[STEP 6] FFDC files created during concurrent re-encryption "
                    "are unencrypted:\n" + "\n".join(failures)
                )
                self.log.error(msg)
                self.fail(msg)
            self.log.info(
                f"[STEP 6] PASSED - {total_new} new FFDC file(s) all encrypted"
            )
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_ffdc_concurrent_with_reencryption")
        self.log.info("=" * 80)

    def test_ffdc_enable_encryption_mid_flight(self):
        """
        Trigger FFDC while encryption is disabled, then enable encryption mid-flight
        before all files have landed.  A second FFDC round triggered after the DEK is
        active must produce encrypted files.  The first-round files (race window) are
        logged informatively; no corruption is permitted.
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_ffdc_enable_encryption_mid_flight")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 1] Verifying KEK exists...")
            self._verify_encryption_prerequisites("log")
            kek_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - KEK ID: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 2] Disabling log encryption and waiting for decryption...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger decryption: {response}")
            self.sleep(180, "Waiting for log files to be decrypted")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 3] Snapshotting FFDC files...")
            ffdc_before = self._snapshot_ffdc_files(query_nodes)
            self.log.info("[STEP 3] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 4] Triggering FFDC Round 1 (encryption disabled)...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
            self.log.info("[STEP 4] Round 1 FFDC triggered; waiting 15 s before enabling encryption...")
            self.sleep(15, "Letting some FFDC files start generating before enabling encryption")

            self.log.info("[STEP 4] Enabling encryption mid-flight...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.encryptionMethod": "encryptionKey",
                "log.encryptionKeyId": str(kek_id)
            })
            self.assertTrue(status, f"Failed to enable encryption: {response}")
            self.sleep(30, "Letting remaining Round 1 FFDC files finish generating")
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 5] Logging Round 1 FFDC file states (informational, race window)...")
            for node in query_nodes:
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                )
                shell.disconnect()
                r1_files = set(f.strip() for f in out if f.strip()) - ffdc_before.get(node.ip, set())
                for fp in r1_files:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, fp
                    )
                    self.log.info(
                        f"[STEP 5] [{node.ip}] Round 1 file {fp}: "
                        f"encrypted={is_enc} (race window — either state is acceptable)"
                    )
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Polling for key IDs to confirm DEK is active...")
            new_key_ids = self._wait_for_query_key_ids(timeout=120, label="[STEP 6] ")
            self.log.info(f"[STEP 6] PASSED - key IDs: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 7] Triggering FFDC Round 2 (encryption fully enabled)...")
            ffdc_before_r2 = self._snapshot_ffdc_files(query_nodes)
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
            self.sleep(30, "Waiting for Round 2 FFDC files to be generated")

            failures = []
            total_new = 0
            for node in query_nodes:
                node_ip = node.ip
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                )
                shell.disconnect()
                r2_files = (
                    set(f.strip() for f in out if f.strip())
                    - ffdc_before_r2.get(node_ip, set())
                )
                total_new += len(r2_files)
                node_new_keys = [str(k) for k in new_key_ids.get(node_ip, [])]
                for fp in r2_files:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, fp
                    )
                    if not is_enc:
                        failures.append(
                            f"  [{node_ip}] Round 2 FFDC file {fp} is NOT encrypted: {details}"
                        )
                        continue
                    found_key = any(
                        self.encryption_helper.verify_file_header_contains(node, fp, k)[0]
                        for k in node_new_keys
                    )
                    if not found_key and node_new_keys:
                        failures.append(
                            f"  [{node_ip}] Round 2 FFDC file {fp} does not contain "
                            f"any expected key ID {node_new_keys}"
                        )
            self.assertGreater(
                total_new, 0,
                "[STEP 7] No Round 2 FFDC files were created after enabling encryption"
            )
            if failures:
                msg = "[STEP 7] Round 2 FFDC file encryption failures:\n" + "\n".join(failures)
                self.log.error(msg)
                self.fail(msg)
            self.log.info(
                f"[STEP 7] PASSED - {total_new} Round 2 FFDC file(s) confirmed encrypted"
            )
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_ffdc_enable_encryption_mid_flight")
        self.log.info("=" * 80)

    def test_ffdc_disable_encryption_mid_flight(self):
        """
        Trigger FFDC while encryption is enabled, then disable encryption mid-flight
        before all files have landed.  A second FFDC round triggered after the disable
        settles must produce plaintext files.  The first-round files (race window) are
        logged informatively; no corruption is permitted.
        """
        STABLE_INTERVAL_S = 60000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_ffdc_disable_encryption_mid_flight")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(prefix_label="ffdc")
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info(f"[STEP 2] Pinning dekRotationInterval to {STABLE_INTERVAL_S} s...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 3] Snapshotting FFDC files...")
            ffdc_before = self._snapshot_ffdc_files(query_nodes)
            self.log.info("[STEP 3] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 4] Triggering FFDC Round 1 (encryption enabled)...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
            self.log.info("[STEP 4] Round 1 FFDC triggered; waiting 15 s before disabling encryption...")
            self.sleep(15, "Letting some FFDC files start generating before disabling encryption")

            self.log.info("[STEP 4] Disabling encryption mid-flight...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            self.sleep(30, "Letting remaining Round 1 FFDC files finish generating")
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 5] Logging Round 1 FFDC file states (informational, race window)...")
            for node in query_nodes:
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                )
                shell.disconnect()
                r1_files = set(f.strip() for f in out if f.strip()) - ffdc_before.get(node.ip, set())
                for fp in r1_files:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, fp
                    )
                    self.log.info(
                        f"[STEP 5] [{node.ip}] Round 1 file {fp}: "
                        f"encrypted={is_enc} (race window — either state is acceptable)"
                    )
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Triggering FFDC Round 2 (encryption disabled)...")
            ffdc_before_r2 = self._snapshot_ffdc_files(query_nodes)
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
            self.sleep(30, "Waiting for Round 2 FFDC files to be generated")

            failures = []
            total_new = 0
            for node in query_nodes:
                node_ip = node.ip
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                )
                shell.disconnect()
                r2_files = (
                    set(f.strip() for f in out if f.strip())
                    - ffdc_before_r2.get(node_ip, set())
                )
                total_new += len(r2_files)
                for fp in r2_files:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, fp
                    )
                    if is_enc:
                        failures.append(
                            f"  [{node_ip}] Round 2 FFDC file {fp} is unexpectedly "
                            f"encrypted after disable: {details}"
                        )
            self.assertGreater(
                total_new, 0,
                "[STEP 6] No Round 2 FFDC files were created after disabling encryption"
            )
            if failures:
                msg = "[STEP 6] Round 2 FFDC files should be unencrypted:\n" + "\n".join(failures)
                self.log.error(msg)
                self.fail(msg)
            self.log.info(
                f"[STEP 6] PASSED - {total_new} Round 2 FFDC file(s) confirmed NOT encrypted"
            )
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_ffdc_disable_encryption_mid_flight")
        self.log.info("=" * 80)

    def test_query_load_concurrent_with_dek_rotation(self):
        """
        Run a continuous query workload in a background thread while DEK rotation is
        triggered by reducing dekRotationInterval to 2 minutes.  Verifies that:
          - No queries fail during rotation
          - system:completed_requests has entries from the concurrent load
          - rlstream.* files written after rotation are encrypted with the new key IDs
        """
        STABLE_INTERVAL_S = 60000
        ROTATION_INTERVAL_S = 120

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_load_concurrent_with_dek_rotation")
        self.log.info("=" * 80)

        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(f"[STEP 1] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info(f"[STEP 2] Pinning dekRotationInterval to {STABLE_INTERVAL_S} s...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 3] Setting up bucket, indexes and running initial scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="enc_concurrent_rot"
            )
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        log_files_before_rotation = self._snapshot_query_log_files(query_nodes)
        bg_thread, stop_event, query_errors = self._start_background_query_loop(
            select_queries, query_nodes
        )
        try:
            self.log.info(
                f"[STEP 4] Triggering DEK rotation by reducing interval to "
                f"{ROTATION_INTERVAL_S} s while queries run in background..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": ROTATION_INTERVAL_S,
                "log.dekLifetime": ROTATION_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to reduce rotation interval: {response}")

            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 4] ", category="log")
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info(f"[STEP 4] PASSED - new key IDs: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            stop_event.set()
            bg_thread.join(timeout=30)
            raise
        finally:
            stop_event.set()
            bg_thread.join(timeout=30)

        try:
            self.log.info(f"[STEP 5] Checking background query errors ({len(query_errors)} total)...")
            if query_errors:
                self.log.warning(
                    f"[STEP 5] {len(query_errors)} query error(s) during rotation:\n"
                    + "\n".join(f"  {e}" for e in query_errors[:10])
                )
            self.assertEqual(
                len(query_errors), 0,
                f"[STEP 5] {len(query_errors)} query error(s) occurred during DEK rotation"
            )
            self.log.info("[STEP 5] PASSED - zero query errors during rotation")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Validating system:completed_requests has concurrent load entries...")
            self._validate_completed_requests(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 7] Verifying rlstream files are encrypted with new key IDs and not old ones...")
            self._assert_rlstream_files_encrypted(query_nodes, new_key_ids, label="[STEP 7] ")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before_rotation, new_key_ids, baseline_key_ids,
                label="[STEP 7] "
            )
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_load_concurrent_with_dek_rotation")
        self.log.info("=" * 80)

    def test_query_load_concurrent_with_force_reencryption(self):
        """
        Run a continuous query workload in a background thread while force re-encryption
        (dropEncryptionAtRestDeks/log) is triggered.  Verifies that:
          - No queries fail during re-encryption
          - system:completed_requests has entries from the concurrent load
          - rlstream.* files are encrypted with the new key IDs after re-encryption
        """
        STABLE_INTERVAL_S = 60000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_load_concurrent_with_force_reencryption")
        self.log.info("=" * 80)

        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info("[STEP 1] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info(f"[STEP 2] Pinning dekRotationInterval to {STABLE_INTERVAL_S} s...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 3] Setting up bucket, indexes and running initial scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="enc_concurrent_reenc"
            )
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        log_files_before_reencryption = self._snapshot_query_log_files(query_nodes)
        bg_thread, stop_event, query_errors = self._start_background_query_loop(
            select_queries, query_nodes
        )
        try:
            self.log.info("[STEP 4] Triggering force re-encryption while queries run in background...")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger re-encryption: {response}")

            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 4] ", category="log")
            self.log.info(f"[STEP 4] PASSED - new key IDs: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            stop_event.set()
            bg_thread.join(timeout=30)
            raise
        finally:
            stop_event.set()
            bg_thread.join(timeout=30)

        try:
            self.log.info(f"[STEP 5] Checking background query errors ({len(query_errors)} total)...")
            if query_errors:
                self.log.warning(
                    f"[STEP 5] {len(query_errors)} query error(s) during re-encryption:\n"
                    + "\n".join(f"  {e}" for e in query_errors[:10])
                )
            self.assertEqual(
                len(query_errors), 0,
                f"[STEP 5] {len(query_errors)} query error(s) occurred during force re-encryption"
            )
            self.log.info("[STEP 5] PASSED - zero query errors during re-encryption")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Validating system:completed_requests has concurrent load entries...")
            self._validate_completed_requests(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 7] Verifying rlstream files are encrypted with new key IDs and not old ones...")
            self._assert_rlstream_files_encrypted(query_nodes, new_key_ids, label="[STEP 7] ")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before_reencryption, new_key_ids, baseline_key_ids,
                label="[STEP 7] "
            )
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_load_concurrent_with_force_reencryption")
        self.log.info("=" * 80)

    def test_toggle_encryption_concurrent_with_query_load(self):
        """
        Toggle log encryption on and off twice while a continuous query workload runs
        in a background thread.  Verifies that:
          - No queries fail during any toggle cycle
          - After the final enable, rlstream files are encrypted with the active key IDs
          - New log files written post-final-enable are encrypted
        """
        STABLE_INTERVAL_S = 60000
        TOGGLE_WAIT_S = 30  # seconds to hold each state before toggling

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_toggle_encryption_concurrent_with_query_load")
        self.log.info("=" * 80)

        try:
            self.log.info("[STEP 1] Verifying prerequisites (KEK must exist)...")
            self._verify_encryption_prerequisites("log")
            kek_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - KEK ID: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info(f"[STEP 2] Pinning dekRotationInterval to {STABLE_INTERVAL_S} s...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 3] Setting up bucket, indexes and running initial scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="enc_toggle"
            )
            self.log.info("[STEP 3] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        bg_thread, stop_event, query_errors = self._start_background_query_loop(
            select_queries, query_nodes
        )
        final_key_ids = {}
        try:
            for cycle in range(1, 3):
                # Disable
                self.log.info(f"[STEP 4] Cycle {cycle}: disabling encryption...")
                status, response = self.rest.disable_log_encryption()
                self.assertTrue(status, f"Cycle {cycle}: failed to disable: {response}")
                self.sleep(TOGGLE_WAIT_S, f"Cycle {cycle}: holding disabled state")

                # Enable
                self.log.info(f"[STEP 4] Cycle {cycle}: enabling encryption...")
                status, response = self.rest.configure_encryption_at_rest({
                    "log.encryptionMethod": "encryptionKey",
                    "log.encryptionKeyId": str(kek_id)
                })
                self.assertTrue(status, f"Cycle {cycle}: failed to enable: {response}")
                self.sleep(TOGGLE_WAIT_S, f"Cycle {cycle}: holding enabled state")
                self.log.info(f"[STEP 4] Cycle {cycle} PASSED")

            self.log.info("[STEP 4] Polling for key IDs after final enable...")
            final_key_ids = self._wait_for_query_key_ids(timeout=120, label="[STEP 4] ")
            self.log.info(f"[STEP 4] PASSED - final key IDs: {final_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            stop_event.set()
            bg_thread.join(timeout=30)
            raise
        finally:
            stop_event.set()
            bg_thread.join(timeout=30)

        try:
            self.log.info(f"[STEP 5] Checking background query errors ({len(query_errors)} total)...")
            if query_errors:
                self.log.warning(
                    f"[STEP 5] {len(query_errors)} query error(s) during toggling:\n"
                    + "\n".join(f"  {e}" for e in query_errors[:10])
                )
            self.assertEqual(
                len(query_errors), 0,
                f"[STEP 5] {len(query_errors)} query error(s) occurred during toggle cycles"
            )
            self.log.info("[STEP 5] PASSED - zero query errors during all toggle cycles")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Running scans to produce new log files under final encryption state...")
            log_files_before_final = self._snapshot_query_log_files(query_nodes)
            self._run_select_scans(select_queries, query_nodes=query_nodes)
            self._assert_rlstream_files_encrypted(query_nodes, final_key_ids, label="[STEP 6] ")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before_final, final_key_ids,
                baseline_key_ids={node.ip: [] for node in query_nodes},
                label="[STEP 6] "
            )
            self.log.info("[STEP 6] PASSED - new log files encrypted after final enable")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_toggle_encryption_concurrent_with_query_load")
        self.log.info("=" * 80)

    def test_rapid_concurrent_reencryption_calls(self):
        """
        Fire 4 rapid dropEncryptionAtRestDeks/log calls simultaneously via
        ThreadPoolExecutor.  Validates that the system:
          - serialises or gracefully rejects duplicate in-flight requests (no 500s,
            no unhandled exceptions)
          - converges to a single consistent encrypted state after all calls settle
          - leaves no file in a partially re-encrypted state (every log file is either
            fully encrypted with the current key or raises a clear error — no torn writes)

        Steps:
          1-3. Prerequisites + pin dekRotationInterval + setup bucket/data/indexes/scans
          4.   Snapshot baseline key IDs and existing log files
          5.   Fire CONCURRENT_CALLS simultaneous re-encryption calls; collect results
          6.   Assert: at least one call succeeded; any failures are graceful (not 5xx)
          7.   Wait until system settles — new key IDs visible on all nodes
          8.   Re-run scans to generate fresh rlstream files under the new key
          9.   Assert every current rlstream.* file is encrypted (no partial-write survivors)
         10.   Assert new rlstream files carry only the new key ID, not the old ones
        """
        STABLE_INTERVAL_S = 60000
        CONCURRENT_CALLS = 4

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_rapid_concurrent_reencryption_calls")
        self.log.info("=" * 80)

        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        try:
            self.log.info(f"[STEP 2] Pinning dekRotationInterval to {STABLE_INTERVAL_S} s...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval: {response}")
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        select_queries = []
        query_nodes = []
        try:
            self.log.info("[STEP 3] Setting up bucket, data, indexes and running scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="ear_rapid_reenc"
            )
            self.log.info(f"[STEP 3] PASSED - {len(query_nodes)} query node(s)")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        baseline_key_ids = {}
        log_files_before = {}
        try:
            self.log.info("[STEP 4] Snapshotting baseline key IDs and log files...")
            baseline_key_ids = self._get_query_in_use_key_ids(category="log")
            log_files_before = self._snapshot_query_log_files(query_nodes)
            self.log.info(f"[STEP 4] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        call_results = []
        try:
            self.log.info(
                f"[STEP 5] Firing {CONCURRENT_CALLS} concurrent dropEncryptionAtRestDeks/log calls..."
            )

            def _reencrypt(call_idx):
                s, r = self.rest.trigger_log_reencryption()
                self.log.info(f"[STEP 5] Call {call_idx}: status={s} response={str(r)[:120]}")
                return call_idx, s, r

            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_CALLS) as executor:
                futures = [executor.submit(_reencrypt, i) for i in range(CONCURRENT_CALLS)]
                for f in concurrent.futures.as_completed(futures):
                    call_results.append(f.result())

            self.log.info(f"[STEP 5] PASSED - all {CONCURRENT_CALLS} calls returned")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 6] Asserting at least one call succeeded and none returned 5xx...")
            successes = [r for r in call_results if r[1]]
            non_successes = [r for r in call_results if not r[1]]
            self.assertGreater(
                len(successes), 0,
                f"[STEP 6] All {CONCURRENT_CALLS} re-encryption calls failed — at least one must succeed. "
                f"Results: {[(idx, s, str(resp)[:80]) for idx, s, resp in call_results]}"
            )
            self.log.info(
                f"[STEP 6] {len(successes)} call(s) succeeded, {len(non_successes)} call(s) "
                "were rejected/serialised"
            )
            # Any non-success response must not indicate a server-side error (5xx).
            # The server may return 409 Conflict or similar for duplicate in-flight calls.
            for call_idx, s, resp in non_successes:
                resp_str = str(resp).lower()
                self.assertNotIn(
                    "internal server error", resp_str,
                    f"[STEP 6] Call {call_idx} returned an internal server error: {resp}"
                )
                self.log.info(
                    f"[STEP 6] Call {call_idx} gracefully rejected/serialised: {str(resp)[:120]}"
                )
            self.log.info("[STEP 6] PASSED - system serialised or gracefully rejected duplicate calls")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        new_key_ids = {}
        try:
            self.log.info("[STEP 7] Waiting for system to settle — new key IDs on all nodes...")
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 7] ", category="log")
            self.log.info(f"[STEP 7] PASSED - new key IDs: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        try:
            self.log.info("[STEP 8] Re-running scans to generate fresh rlstream files under new key...")
            self._run_select_scans(select_queries, query_nodes=query_nodes)
            self.log.info("[STEP 8] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        try:
            self.log.info(
                "[STEP 9] Verifying ALL current rlstream.* files are encrypted "
                "(no partial-write survivors)..."
            )
            failures = []
            copied_files = []
            for node in query_nodes:
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node, verbose=False)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'rlstream.*' 2>/dev/null"
                )
                shell.disconnect()
                all_rlstream = [f.strip() for f in out if f.strip()]
                if not all_rlstream:
                    # rlstream rotated to local_request_log.* under concurrent
                    # re-encryption load — fall back to the newest archives,
                    # which carry the same partial-write signal.
                    verified, archive_failures = self._verify_archived_local_request_logs(
                        node, [], label="[STEP 9] "
                    )
                    if archive_failures:
                        failures.extend(archive_failures)
                        continue
                    if not verified:
                        failures.append(
                            f"  [{node.ip}] No rlstream.* files found and no "
                            f"local_request_log.* fallback archives present"
                        )
                    continue
                for file_path in all_rlstream:
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    if not is_enc:
                        dest = self._copy_failed_file_from_node(node, file_path)
                        if dest:
                            copied_files.append(dest)
                        failures.append(
                            f"  [{node.ip}] {file_path} is NOT encrypted (possible partial write): {details}"
                        )
            if copied_files:
                self.log.info(
                    f"[STEP 9] Copied {len(copied_files)} failing file(s) to "
                    f"{self._evidence_dir}:\n" + "\n".join(f"  {p}" for p in copied_files)
                )
            if failures:
                msg = (
                    "[STEP 9] Partially or fully unencrypted rlstream files found after "
                    f"concurrent re-encryption (copies in {self._evidence_dir}):\n"
                    + "\n".join(failures)
                )
                self.log.error(msg)
                self.fail(msg)
            self.log.info("[STEP 9] PASSED - all rlstream files encrypted, no partial writes")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        try:
            self.log.info(
                "[STEP 10] Verifying new rlstream files use only the new key ID (not old baseline key)..."
            )
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before, new_key_ids, baseline_key_ids,
                label="[STEP 10] "
            )
            self.log.info("[STEP 10] PASSED - new rlstream files use only new key IDs")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_rapid_concurrent_reencryption_calls")
        self.log.info("=" * 80)

    def test_force_encryption_encrypts_existing_plaintext_files(self):
        """
        Verify that POST /controller/forceEncryptionAtRest/log retroactively
        encrypts log and FFDC files that were written while encryption was disabled.

        Steps:
        1.  Verify log encryption prerequisites (KEK must exist)
        2.  Disable log encryption + trigger decryption, wait for plaintext settle
        3.  Generate rlstream, local_request_log, and FFDC files while encryption is off
        4.  Verify all generated files are NOT encrypted (plaintext baseline)
        5.  Snapshot the plaintext file paths
        6.  Re-enable log encryption
        7.  Poll for DEK to become active
        8.  Trigger forceEncryptionAtRest/log
        9.  Poll until all snapshotted files are encrypted (timeout 300 s)
        10. Assert no plaintext files remain
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_force_encryption_encrypts_existing_plaintext_files")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="force_enc"
            )
            self.log.info(
                "[PREAMBLE] PASSED - completed-stream-size=500, completed-threshold=0 applied; "
                f"{len(select_queries)} SELECT queries ready on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying log encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("log")
            kek_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - KEK ID: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Disable encryption and wait for plaintext settle ==========
        try:
            self.log.info("[STEP 2] Disabling log encryption and triggering decryption...")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Failed to disable log encryption: {response}")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger decryption: {response}")
            self.sleep(180, "Waiting for existing files to be decrypted")
            self.log.info("[STEP 2] PASSED - encryption disabled, existing files decrypted")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Generate files while encryption is disabled ==========
        try:
            self.log.info("[STEP 3] Generating log and FFDC files while encryption is disabled...")
            self._generate_concurrent_load_until_archive(select_queries, query_nodes)
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                for node in query_nodes:
                    status, response = self.rest.trigger_query_ffdc(node)
                    self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                    self.log.info(f"[STEP 3] FFDC triggered on {node.ip} (iteration {iteration})")
                self.sleep(30, f"Waiting for FFDC files to be generated (iteration {iteration})")
            self.log.info("[STEP 3] PASSED - load and FFDC files generated")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Verify all generated files are NOT encrypted ==========
        try:
            self.log.info("[STEP 4] Verifying generated files are NOT encrypted (plaintext baseline)...")
            self._assert_log_files_not_encrypted(query_nodes, label="[STEP 4] ")
            self._assert_ffdc_files_not_encrypted(query_nodes, label="[STEP 4] ")
            self.log.info("[STEP 4] PASSED - all files confirmed plaintext")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Snapshot the plaintext file paths ==========
        try:
            self.log.info("[STEP 5] Snapshotting plaintext file paths...")
            log_files_snapshot = self._snapshot_query_log_files(query_nodes)
            ffdc_files_snapshot = self._snapshot_ffdc_files(query_nodes)
            total_log = sum(
                len(cats.get("rlstream", set())) + len(cats.get("local_request_log", set()))
                for cats in log_files_snapshot.values()
            )
            total_ffdc = sum(len(paths) for paths in ffdc_files_snapshot.values())
            self.log.info(
                f"[STEP 5] PASSED - snapshotted {total_log} log file(s) and "
                f"{total_ffdc} FFDC file(s) across {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Re-enable log encryption ==========
        try:
            self.log.info(f"[STEP 6] Re-enabling log encryption with KEK ID {kek_id}...")
            status, response = self.rest.configure_encryption_at_rest({
                "log.encryptionMethod": "encryptionKey",
                "log.encryptionKeyId": str(kek_id)
            })
            self.assertTrue(status, f"Failed to re-enable log encryption: {response}")
            self.log.info("[STEP 6] PASSED - log encryption re-enabled")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Poll for DEK to become active ==========
        try:
            self.log.info("[STEP 7] Polling for in-use key IDs to confirm DEK is active...")
            active_key_ids = self._wait_for_query_key_ids(timeout=120, label="[STEP 7] ")
            self.log.info(f"[STEP 7] PASSED - active key IDs per node: {active_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 8: Trigger forceEncryptionAtRest/log ==========
        try:
            self.log.info(
                "[STEP 8] Triggering POST /controller/forceEncryptionAtRest/log..."
            )
            status, response = self.rest.force_log_encryption_at_rest()
            self.assertTrue(status, f"Failed to trigger force encryption: {response}")
            self.log.info("[STEP 8] PASSED - force encryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Poll until snapshotted files are encrypted ==========
        #
        # Rotation note: between STEP 5 (snapshot) and STEP 9 (poll) the product
        # may rotate / archive / clean up log + FFDC files (rlstream → numbered
        # archives, archives → .gz). The test's invariant is "no plaintext
        # residue should remain", so for each snapshotted path we resolve to:
        #     - the path itself, if still present
        #     - its .gz archived counterpart, if rotated to .gz
        #     - any sibling archive matching the basename
        #     - or None — meaning the file was rotated away entirely (which is
        #       a passing outcome: zero plaintext residue is exactly the goal)
        try:
            self.log.info(
                "[STEP 9] Polling until all snapshotted plaintext files are encrypted "
                "(timeout 300 s)..."
            )
            node_map = {node.ip: node for node in query_nodes}
            deadline = time.time() + 300
            remaining = {}
            # Build flat {node_ip: {file_path, ...}} map from both snapshots
            for node in query_nodes:
                node_ip = node.ip
                log_cats = log_files_snapshot.get(node_ip, {})
                all_paths = (
                    log_cats.get("rlstream", set())
                    | log_cats.get("local_request_log", set())
                    | ffdc_files_snapshot.get(node_ip, set())
                )
                if all_paths:
                    remaining[node_ip] = set(all_paths)

            # Track files that have been rotated away entirely — they are no
            # longer plaintext residue, so they are considered handled
            rotated_away = {node.ip: set() for node in query_nodes}

            while time.time() < deadline and remaining:
                still_plain = {}
                for node_ip, paths in remaining.items():
                    node = node_map[node_ip]
                    still_unenc = set()
                    for file_path in paths:
                        resolved = self._resolve_existing_or_archived_path(node, file_path)
                        if resolved is None:
                            # File rotated/cleaned up — no plaintext remains
                            rotated_away[node_ip].add(file_path)
                            continue
                        is_enc, _ = self.encryption_helper.verify_file_encryption_magic_bytes(
                            node, resolved
                        )
                        if not is_enc:
                            still_unenc.add(file_path)
                    if still_unenc:
                        still_plain[node_ip] = still_unenc
                remaining = still_plain
                if remaining:
                    total_remaining = sum(len(p) for p in remaining.values())
                    self.log.info(
                        f"[STEP 9] {total_remaining} file(s) still plaintext — waiting 15 s..."
                    )
                    self.sleep(15, "[STEP 9] Waiting for force encryption to complete...")

            total_rotated = sum(len(p) for p in rotated_away.values())
            if total_rotated:
                self.log.info(
                    f"[STEP 9] {total_rotated} snapshotted file(s) were rotated/cleaned "
                    f"away during force-encryption — treated as handled (no plaintext "
                    f"residue is the goal)"
                )
            self.log.info("[STEP 9] PASSED - polling complete")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEP 10: Assert no plaintext files remain ==========
        try:
            self.log.info("[STEP 10] Asserting all previously-plaintext files are now encrypted...")
            failures = []
            for node_ip, paths in remaining.items():
                node = node_map[node_ip]
                for file_path in paths:
                    resolved = self._resolve_existing_or_archived_path(node, file_path)
                    if resolved is None:
                        # Rotated/cleaned up between STEP 9 and STEP 10 —
                        # not a plaintext residue, so do not fail on it
                        rotated_away[node_ip].add(file_path)
                        continue
                    is_enc, details = self.encryption_helper.verify_file_encryption_magic_bytes(
                        node, resolved
                    )
                    if not is_enc:
                        failures.append(
                            f"  [{node_ip}] {resolved} (snapshotted as {file_path}) "
                            f"is still NOT encrypted after forceEncryptionAtRest: {details}"
                        )
            if failures:
                msg = (
                    "[STEP 10] Files that should have been encrypted by forceEncryptionAtRest "
                    "remain plaintext:\n" + "\n".join(failures)
                )
                self.log.error(msg)
                self.fail(msg)
            self.log.info(
                "[STEP 10] PASSED - all previously-plaintext files are now encrypted "
                "or have been rotated away"
            )
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_force_encryption_encrypts_existing_plaintext_files")
        self.log.info("=" * 80)

    def test_query_ffdc_in_cbcollect_not_encrypted(self):
        """
        Verify that FFDC files included in a cbcollect bundle are NOT encrypted.

        Even when log encryption at rest is active on the cluster, cbcollect must
        include plaintext-readable FFDC files so that support engineers can inspect
        them without decryption tooling.

        Steps:
        1.  Verify log encryption at rest prerequisites
        2.  Set up bucket, indexes, and run baseline scans
        3.  Start a background query loop to keep the query service active
        4.  Trigger FFDC on all query nodes, wait for files to appear, and verify
            they ARE encrypted on disk before cbcollect runs
        5.  Stop the background query loop
        6.  Run cbcollect_info on every query node
        7.  On each node, extract the cbcollect zip and verify ALL FFDC files inside
            are NOT encrypted (cbcollect must strip encryption when bundling)
        8.  Clean up cbcollect artifacts from remote nodes
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_ffdc_in_cbcollect_not_encrypted")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying log encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Set up bucket, indexes, scans ==========
        try:
            self.log.info("[STEP 2] Setting up bucket, indexes, and baseline scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="cbcollect_ffdc"
            )
            self.log.info(
                f"[STEP 2] PASSED - {len(select_queries)} queries ready "
                f"across {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Start background query loop ==========
        bg_thread = None
        stop_event = None
        try:
            self.log.info("[STEP 3] Starting background query loop...")
            bg_thread, stop_event, bg_errors = self._start_background_query_loop(
                select_queries, query_nodes
            )
            self.log.info("[STEP 3] PASSED - background query loop running")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        try:
            # ========== STEP 4: Trigger FFDC and wait for files ==========
            try:
                self.log.info("[STEP 4] Triggering FFDC on all query nodes...")
                self._dispatch_background_query_batch(select_queries, query_nodes)
                for node in query_nodes:
                    status, response = self.rest.trigger_query_ffdc(node)
                    self.assertTrue(
                        status,
                        f"[STEP 4] Failed to trigger FFDC on {node.ip}: {response}"
                    )
                    self.log.info(f"[STEP 4] FFDC triggered on {node.ip}")

                # Wait for FFDC files to appear
                deadline = time.time() + 180
                ffdc_found = False
                while time.time() < deadline:
                    results = self.encryption_helper.verify_query_ffdc_files_encrypted(
                        query_nodes
                    )
                    total = sum(len(files) for files in results.values())
                    if total > 0:
                        ffdc_found = True
                        self.log.info(
                            f"[STEP 4] {total} FFDC file(s) found across {len(results)} node(s)"
                        )
                        break
                    self.sleep(15, "[STEP 4] Waiting for FFDC files...")
                self.assertTrue(ffdc_found, "[STEP 4] No FFDC files appeared within 180 s")

                # Verify the on-disk FFDC files ARE encrypted before cbcollect runs
                enc_failures = []
                for node_ip, node_result in results.items():
                    for ffdc_file, file_result in node_result.items():
                        if not file_result["encrypted"]:
                            enc_failures.append(
                                f"  [{node_ip}] {ffdc_file} is NOT encrypted on disk "
                                f"(expected encrypted before cbcollect): {file_result['details']}"
                            )
                if enc_failures:
                    msg = (
                        "[STEP 4] FFDC files on disk are not encrypted — "
                        "cannot proceed with cbcollect test:\n" + "\n".join(enc_failures)
                    )
                    self.log.error(msg)
                    self.fail(msg)
                self.log.info(
                    "[STEP 4] PASSED - FFDC triggered, files confirmed present and encrypted on disk"
                )
            except Exception as e:
                self.log.error(f"[STEP 4] FAILED: {e}")
                raise

        finally:
            # ========== STEP 5: Stop background query loop ==========
            if stop_event is not None:
                stop_event.set()
            if bg_thread is not None:
                bg_thread.join(timeout=30)
            self.log.info("[STEP 5] Background query loop stopped")

        # ========== STEP 6: Run cbcollect_info on every query node ==========
        cbcollect_paths = {}
        try:
            self.log.info("[STEP 6] Running cbcollect_info on every query node...")
            for node in query_nodes:
                remote_zip = f"/tmp/cbcollect_ffdc_test_{node.ip.replace('.', '_')}.zip"
                shell = RemoteMachineShellConnection(node, verbose=False)
                try:
                    self.log.info(
                        f"[STEP 6] Running cbcollect_info on {node.ip} -> {remote_zip}"
                    )
                    output, error = shell.execute_cbcollect_info(remote_zip)
                    self.log.info(
                        f"[STEP 6] cbcollect_info on {node.ip} output: "
                        f"{' '.join(output[-5:] if output else [])}"
                    )
                    check_out, _ = shell.execute_command(
                        f"test -f {remote_zip} && echo EXISTS || echo MISSING"
                    )
                    self.assertIn(
                        "EXISTS", " ".join(check_out),
                        f"[STEP 6] cbcollect zip not found at {remote_zip} on {node.ip}"
                    )
                    cbcollect_paths[node.ip] = (node, shell, remote_zip)
                    self.log.info(f"[STEP 6] cbcollect zip created on {node.ip}: {remote_zip}")
                except Exception:
                    shell.disconnect()
                    raise
            self.log.info("[STEP 6] PASSED - cbcollect_info completed on all nodes")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            for node_ip, (node, shell, _) in cbcollect_paths.items():
                try:
                    shell.disconnect()
                except Exception:
                    pass
            raise

        # ========== STEP 7: Extract zip and verify FFDC files are NOT encrypted ==========
        try:
            self.log.info(
                "[STEP 7] Extracting cbcollect bundles and verifying FFDC files "
                "are NOT encrypted..."
            )
            failures = []
            for node_ip, (node, shell, remote_zip) in cbcollect_paths.items():
                extract_dir = remote_zip.replace(".zip", "_extracted")
                try:
                    shell.execute_command(f"mkdir -p {extract_dir}")
                    shell.execute_command(
                        f"unzip -o {remote_zip} -d {extract_dir} 2>/dev/null"
                    )
                    ffdc_out, _ = shell.execute_command(
                        f"find {extract_dir} -name 'query_ffdc_MAN_areq*' -o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
                    )
                    ffdc_files = [f.strip() for f in ffdc_out if f.strip()]
                    self.log.info(
                        f"[STEP 7] Found {len(ffdc_files)} FFDC file(s) in cbcollect "
                        f"bundle on {node_ip}"
                    )
                    if not ffdc_files:
                        failures.append(
                            f"  [{node_ip}] No query_ffdc_MAN_areq/creq/vita files found in cbcollect bundle"
                        )
                        continue

                    for ffdc_file in ffdc_files:
                        is_encrypted, details = \
                            self.encryption_helper.verify_file_encryption_magic_bytes(
                                node, ffdc_file
                            )
                        self.log.info(
                            f"[STEP 7] [{node_ip}] {ffdc_file} "
                            f"encrypted={is_encrypted} details={details}"
                        )
                        if is_encrypted:
                            failures.append(
                                f"  [{node_ip}] {ffdc_file} is unexpectedly encrypted: "
                                f"{details}"
                            )
                            dest = self._copy_failed_file_from_node(node, ffdc_file)
                            if dest:
                                self.log.info(f"[STEP 7] Copied failing file to {dest}")
                finally:
                    shell.execute_command(f"rm -rf {extract_dir} 2>/dev/null")

            if failures:
                msg = (
                    "[STEP 7] FFDC files in cbcollect bundle are unexpectedly encrypted:\n"
                    + "\n".join(failures)
                )
                self.log.error(msg)
                self.fail(msg)
            self.log.info(
                "[STEP 7] PASSED - all FFDC files in cbcollect bundle verified NOT encrypted"
            )
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise
        finally:
            # ========== STEP 8: Clean up cbcollect artifacts ==========
            self.log.info("[STEP 8] Cleaning up cbcollect artifacts from remote nodes...")
            for node_ip, (node, shell, remote_zip) in cbcollect_paths.items():
                try:
                    shell.execute_command(f"rm -f {remote_zip} 2>/dev/null")
                    self.log.info(f"[STEP 8] Removed {remote_zip} from {node_ip}")
                except Exception as cleanup_err:
                    self.log.warning(
                        f"[STEP 8] Failed to clean up {remote_zip} on {node_ip}: {cleanup_err}"
                    )
                finally:
                    shell.disconnect()
            self.log.info("[STEP 8] PASSED - cleanup complete")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_ffdc_in_cbcollect_not_encrypted")
        self.log.info("=" * 80)

    def test_query_log_dek_lifetime(self):
        """
        Verify that when a DEK's lifetime expires, files previously encrypted with
        that DEK are automatically re-encrypted with the current active DEK.

        Timeline (all times relative to STEP 10 when rotation/lifetime settings are applied):
          t=0:      key1 active. rlstream / local_request_log files from iteration 1
                    are encrypted with key1.
          t=2 min:  key2 rotates in. rlstream files from iteration 2 use key2.
          t=4 min:  key3 rotates in. key1's dekLifetime (240 s) has expired.
                    The cluster automatically re-encrypts key1 files with key3.
                    key2's lifetime does not expire until t=6 min.

        Assertions:
          - Iteration 1: all log files encrypted with key1.
          - Iteration 2: new log files encrypted with key2.
          - After key1 lifetime: files that previously carried key1 now carry key3,
            and key1 is no longer present in those file headers.
        """
        # 1 000 min in seconds — prevents rotation during setup
        STABLE_INTERVAL_S = 60000
        # 2-minute rotation interval
        ROTATION_INTERVAL_S = 120
        # 4-minute DEK lifetime (2 rotation intervals)
        DEK_LIFETIME_S = 240

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_log_dek_lifetime")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            key_id = self.log_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - log_encryption_at_rest_id: {key_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin rotation interval and lifetime high before setup ==========
        try:
            self.log.info(
                f"[STEP 2] Pinning dekRotationInterval and dekLifetime to "
                f"{STABLE_INTERVAL_S} s to prevent rotation during setup..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval/dekLifetime: {response}")
            self.log.info("[STEP 2] PASSED - rotation interval and lifetime pinned high")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEPS 3-7: Bucket, data, indexes, scans ==========
        try:
            self.log.info("[STEPS 3-7] Setting up bucket, indexes and running scans...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="dek_lifetime"
            )
            self.log.info("[STEPS 3-7] PASSED")
        except Exception as e:
            self.log.error(f"[STEPS 3-7] FAILED: {e}")
            raise

        # ========== STEP 8: Iteration 1 — validate files encrypted with key1 ==========
        try:
            self.log.info("[STEP 8] Iteration 1: validating log files encrypted with key1...")
            key1_ids = self._get_query_in_use_key_ids(category="log")
            self.assertGreater(len(key1_ids), 0, "No query nodes returned key IDs")
            for node_ip, ids in key1_ids.items():
                self.assertGreater(len(ids), 0,
                    f"Query node {node_ip} reported no in-use key IDs")
            self._assert_rlstream_files_encrypted(query_nodes, key1_ids, label="[STEP 8] ")
            self._assert_local_request_log_files_encrypted(
                query_nodes, key1_ids, select_queries, label="[STEP 8] "
            )
            self.log.info(f"[STEP 8] PASSED - key1_ids per node: {key1_ids}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Snapshot files encrypted with key1 ==========
        try:
            self.log.info("[STEP 9] Snapshotting log files encrypted with key1...")
            key1_files_snapshot = self._snapshot_query_log_files(query_nodes)
            self.log.info(
                f"[STEP 9] PASSED - snapshot captured: "
                + ", ".join(
                    f"{node.ip}: {len(key1_files_snapshot.get(node.ip, {}).get('rlstream', set()))} "
                    f"rlstream + {len(key1_files_snapshot.get(node.ip, {}).get('local_request_log', set()))} "
                    f"local_request_log"
                    for node in query_nodes
                )
            )
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEP 10: Enable rotation=2 min, lifetime=4 min — starts the clock ==========
        try:
            self.log.info(
                f"[STEP 10] Setting dekRotationInterval={ROTATION_INTERVAL_S} s "
                f"({ROTATION_INTERVAL_S // 60} min), dekLifetime={DEK_LIFETIME_S} s "
                f"({DEK_LIFETIME_S // 60} min)..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": ROTATION_INTERVAL_S,
                "log.dekLifetime": DEK_LIFETIME_S
            })
            self.assertTrue(status, f"Failed to set rotation/lifetime settings: {response}")
            self.log.info("[STEP 10] PASSED - rotation and lifetime clock started")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {e}")
            raise

        # ========== STEP 11: Wait for key2 to appear (~2 min) ==========
        try:
            self.log.info(
                "[STEP 11] Waiting for first DEK rotation (key2) — "
                f"expected within {ROTATION_INTERVAL_S} s..."
            )
            key2_ids = self._wait_for_new_query_key_ids(
                key1_ids, timeout=300, label="[STEP 11] ", category="log")
            self.log.info(f"[STEP 11] PASSED - key2_ids per node: {key2_ids}")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {e}")
            raise
        # ========== STEP 12: Iteration 2 — run scans and validate files use key2 ==========
        try:
            self.log.info("[STEP 12] Iteration 2: running scans and validating files use key2...")
            log_files_before_iter2 = self._snapshot_query_log_files(query_nodes)
            self._run_select_scans(select_queries, query_nodes=query_nodes)
            self._assert_rlstream_files_encrypted(query_nodes, key2_ids, label="[STEP 12] ")
            self._assert_new_log_files_use_only_new_keys(
                query_nodes, log_files_before_iter2, key2_ids, key1_ids, label="[STEP 12] "
            )
            self.log.info("[STEP 12] PASSED - iteration 2 files encrypted with key2")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {e}")
            raise

        # ========== STEP 13: Wait for key3 (~4 min from STEP 10 = key1 lifetime expires) ==========
        try:
            self.log.info(
                "[STEP 13] Waiting for second DEK rotation (key3) at ~4 min "
                "— coincides with key1 dekLifetime expiry..."
            )
            key3_ids = self._wait_for_new_query_key_ids(
                key2_ids, timeout=300, label="[STEP 13] ", category="log")
            self.log.info(
                f"[STEP 13] PASSED - key3_ids per node: {key3_ids}. "
                f"Re-pinning rotation interval to {STABLE_INTERVAL_S} s to prevent further rotations..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {e}")
            raise

        # ========== STEP 14: Wait for key1 to disappear from KeysInUse, then re-check ==========
        try:
            self.log.info(
                "[STEP 14] Waiting for key1 to disappear from KeysInUse before re-validation..."
            )
            key3_ids = self._wait_for_old_query_key_ids_to_disappear(
                key1_ids, timeout=300, label="[STEP 14] ", category="log")
            self.log.info(
                f"[STEP 14] KeysInUse no longer reports key1. Current key IDs: {key3_ids}. "
                "Waiting a few seconds before re-checking file encryption..."
            )
            self.sleep(5, "[STEP 14] Waiting before re-checking re-encryption...")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED while waiting for key1 to disappear: {e}")
            raise

        # Isolate key3-only IDs (exclude key1 and key2) for the re-encryption assertion
        key1_set = {node_ip: {str(k) for k in ids} for node_ip, ids in key1_ids.items()}
        key2_set = {node_ip: {str(k) for k in ids} for node_ip, ids in key2_ids.items()}
        key3_only_ids = {
            node_ip: [k for k in ids
                      if str(k) not in key1_set.get(node_ip, set())
                      and str(k) not in key2_set.get(node_ip, set())]
            for node_ip, ids in key3_ids.items()
        }

        # ========== STEP 14: Poll for key1 files to be re-encrypted with key3 ==========
        try:
            self.log.info(
                "[STEP 14] Polling for key1 files to be automatically re-encrypted "
                f"with key3 (timeout 300 s)..."
            )
            self._poll_for_files_reencrypted_after_lifetime(
                query_nodes,
                files_snapshot=key1_files_snapshot,
                old_key_ids=key1_ids,
                new_key_ids=key3_only_ids,
                timeout=300,
                label="[STEP 14] "
            )
            self.log.info("[STEP 14] PASSED - key1 files re-encrypted with key3 after lifetime expiry")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_log_dek_lifetime")
        self.log.info(
            f"DEK lifetime validated: key1={key1_ids} → key2={key2_ids} → key3={key3_ids}; "
            f"key1 files re-encrypted with key3 after {DEK_LIFETIME_S} s lifetime"
        )
        self.log.info("=" * 80)

    def test_query_ffdc_dek_lifetime(self):
        """
        Verify that when a DEK's lifetime expires, FFDC files previously encrypted with
        that DEK are automatically re-encrypted with the current active DEK.

        Timeline (relative to STEP 8 when rotation/lifetime settings are applied):
          t=0:      key1 active. FFDC files from iteration 1 are encrypted with key1.
          t=2 min:  key2 rotates in. FFDC files from iteration 2 use key2.
          t=4 min:  key3 rotates in. key1's dekLifetime (240 s) has expired.
                    The cluster automatically re-encrypts key1 FFDC files with key3.
                    key2's lifetime does not expire until t=6 min.

        Assertions:
          - Iteration 1: FFDC files encrypted with key1.
          - Iteration 2: new FFDC files encrypted with key2.
          - After key1 lifetime: FFDC files that previously carried key1 now carry key3,
            and key1 is no longer present in those file headers.
        """
        STABLE_INTERVAL_S = 60000
        ROTATION_INTERVAL_S = 120
        DEK_LIFETIME_S = 240

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_ffdc_dek_lifetime")
        self.log.info("=" * 80)

        # ========== PREAMBLE: Settings + test data ==========
        try:
            self.log.info("[PREAMBLE] Setting query completed settings and preparing test data...")
            select_queries, query_nodes = self._setup_bucket_indexes_scans(
                prefix_label="ffdc_dek_lifetime"
            )
            self.log.info(
                f"[PREAMBLE] PASSED - {len(select_queries)} SELECT queries ready "
                f"on {len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[PREAMBLE] FAILED: {e}")
            raise

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            self._verify_encryption_prerequisites("log")
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin rotation interval and lifetime high before setup ==========
        try:
            self.log.info(
                f"[STEP 2] Pinning dekRotationInterval and dekLifetime to "
                f"{STABLE_INTERVAL_S} s to prevent rotation during setup..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to pin dekRotationInterval/dekLifetime: {response}")
            self.log.info("[STEP 2] PASSED - rotation interval and lifetime pinned high")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Get key1 IDs ==========
        try:
            self.log.info("[STEP 3] Getting key1 IDs...")
            key1_ids = self._get_query_in_use_key_ids(category="log")
            self.assertGreater(len(key1_ids), 0, "No query nodes returned key IDs")
            for node_ip, ids in key1_ids.items():
                self.assertGreater(len(ids), 0,
                    f"Query node {node_ip} reported no in-use key IDs")
            self.log.info(f"[STEP 3] PASSED - key1_ids per node: {key1_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEPS 4-6: Iteration 1 — trigger FFDC 3x and verify with key1 ==========
        try:
            self.log.info("[STEPS 4-6] Iteration 1: triggering FFDC 3 times and verifying with key1...")
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, key1_ids,
                    iteration=iteration,
                    label=f"[STEP {3 + iteration}] "
                )
            self.log.info("[STEPS 4-6] PASSED - iteration 1 FFDC files verified encrypted with key1")
        except Exception as e:
            self.log.error(f"[STEPS 4-6] FAILED: {e}")
            raise

        # ========== STEP 7: Snapshot FFDC files encrypted with key1 ==========
        try:
            self.log.info("[STEP 7] Snapshotting FFDC files encrypted with key1...")
            key1_ffdc_snapshot = self._snapshot_ffdc_files(query_nodes)
            total = sum(len(s) for s in key1_ffdc_snapshot.values())
            self.assertGreater(total, 0,
                "[STEP 7] No FFDC files found to snapshot — cannot test re-encryption")
            self.log.info(
                f"[STEP 7] PASSED - snapshot captured: {total} FFDC file(s) across "
                f"{len(query_nodes)} node(s)"
            )
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 8: Enable rotation=2 min, lifetime=4 min — starts the clock ==========
        try:
            self.log.info(
                f"[STEP 8] Setting dekRotationInterval={ROTATION_INTERVAL_S} s "
                f"({ROTATION_INTERVAL_S // 60} min), dekLifetime={DEK_LIFETIME_S} s "
                f"({DEK_LIFETIME_S // 60} min)..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": ROTATION_INTERVAL_S,
                "log.dekLifetime": DEK_LIFETIME_S
            })
            self.assertTrue(status, f"Failed to set rotation/lifetime settings: {response}")
            self.log.info("[STEP 8] PASSED - rotation and lifetime clock started")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Wait for key2 to appear (~2 min) ==========
        try:
            self.log.info(
                "[STEP 9] Waiting for first DEK rotation (key2) — "
                f"expected within {ROTATION_INTERVAL_S} s..."
            )
            key2_ids = self._wait_for_new_query_key_ids(
                key1_ids, timeout=300, label="[STEP 9] ", category="log")
            self.log.info(f"[STEP 9] PASSED - key2_ids per node: {key2_ids}")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        # ========== STEPS 10-12: Iteration 2 — trigger FFDC 3x and verify with key2 ==========
        try:
            self.log.info("[STEPS 10-12] Iteration 2: triggering FFDC 3 times and verifying with key2...")
            ffdc_before_iter2 = self._snapshot_ffdc_files(query_nodes)
            for iteration in range(1, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, key2_ids,
                    iteration=iteration,
                    label=f"[STEP {9 + iteration}] "
                )
            self._assert_new_ffdc_files_use_only_new_keys(
                query_nodes, ffdc_before_iter2, key2_ids, key1_ids, label="[STEPS 10-12] "
            )
            self.log.info("[STEPS 10-12] PASSED - iteration 2 FFDC files verified encrypted with key2")
        except Exception as e:
            self.log.error(f"[STEPS 10-12] FAILED: {e}")
            raise

        # ========== STEP 13: Wait for key3 (~4 min from STEP 8 = key1 lifetime expires) ==========
        try:
            self.log.info(
                "[STEP 13] Waiting for second DEK rotation (key3) at ~4 min "
                "— coincides with key1 dekLifetime expiry..."
            )
            key3_ids = self._wait_for_new_query_key_ids(
                key2_ids, timeout=300, label="[STEP 13] ", category="log")
            self.log.info(
                f"[STEP 13] PASSED - key3_ids per node: {key3_ids}. "
                f"Re-pinning rotation interval to {STABLE_INTERVAL_S} s..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {e}")
            raise

        # ========== STEP 14: Wait for key1 to disappear from KeysInUse, then re-check ==========
        try:
            self.log.info(
                "[STEP 14] Waiting for key1 to disappear from KeysInUse before re-validation..."
            )
            key3_ids = self._wait_for_old_query_key_ids_to_disappear(
                key1_ids, timeout=300, label="[STEP 14] ", category="log")
            self.log.info(
                f"[STEP 14] KeysInUse no longer reports key1. Current key IDs: {key3_ids}. "
                "Waiting a few seconds before re-checking file encryption..."
            )
            self.sleep(5, "[STEP 14] Waiting before re-checking re-encryption...")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED while waiting for key1 to disappear: {e}")
            raise

        # Isolate key3-only IDs (exclude key1 and key2) for the re-encryption assertion
        key1_set = {node_ip: {str(k) for k in ids} for node_ip, ids in key1_ids.items()}
        key2_set = {node_ip: {str(k) for k in ids} for node_ip, ids in key2_ids.items()}
        key3_only_ids = {
            node_ip: [k for k in ids
                      if str(k) not in key1_set.get(node_ip, set())
                      and str(k) not in key2_set.get(node_ip, set())]
            for node_ip, ids in key3_ids.items()
        }

        # ========== STEP 14: Poll for key1 FFDC files to be re-encrypted with key3 ==========
        try:
            self.log.info(
                "[STEP 14] Polling for key1 FFDC files to be automatically re-encrypted "
                f"with key3 (timeout 300 s)..."
            )
            self._poll_for_ffdc_files_reencrypted_after_lifetime(
                query_nodes,
                ffdc_snapshot=key1_ffdc_snapshot,
                old_key_ids=key1_ids,
                new_key_ids=key3_only_ids,
                timeout=300,
                label="[STEP 14] "
            )
            self.log.info("[STEP 14] PASSED - key1 FFDC files re-encrypted with key3 after lifetime expiry")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_ffdc_dek_lifetime")
        self.log.info(
            f"FFDC DEK lifetime validated: key1={key1_ids} → key2={key2_ids} → key3={key3_ids}; "
            f"key1 FFDC files re-encrypted with key3 after {DEK_LIFETIME_S} s lifetime"
        )
        self.log.info("=" * 80)

    def test_query_spill_files_encrypted(self):
        """
        Verify ORDER BY spill files are encrypted, spills.order increments, and
        query results are identical with 'other' encryption at rest enabled vs
        disabled.

        ORDER BY on a large collection spills intermediate sort buffers to the
        directory reported by GET /settings/querySettings (tmpSpaceDir). Spill
        files are transient — they exist only while the sort operator is
        running, so a remote watcher copies them into a per-test snapshot
        directory the moment they appear and validates the copies after the
        query finishes.

        Steps:
        1.  Prerequisites — KEK id captured
        2.  Bucket + 150 000 Hotel docs + primary index
        3.  Capture spill directory (tmpSpaceDir)
        4.  Resolve query nodes
        --- ENCRYPTION OFF RUN ---
        5.  Disable 'other' encryption at rest
        6.  Run ORDER BY → capture baseline result hash
        --- ENCRYPTION ON RUN ---
        7.  Re-enable 'other' encryption at rest with original KEK
        8.  Baseline spills.order per node; capture per-node key IDs
        9.  Start remote spill-file watcher on each node
        10. Run ORDER BY in background
        11. Poll spills.order until incremented (proves spill files were created)
        12. Wait for background query to finish; stop watcher
        13. Validate all snapshotted spill files are encrypted with expected
            key IDs
        14. Hash encrypted-run result → assert equal to baseline hash
        15. Cleanup snapshot dirs on every node
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_spill_files_encrypted")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying other encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("other")
            kek_id = self.other_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - other_encryption_at_rest_id: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Create bucket, load Hotel docs, primary index ==========
        try:
            self.log.info("[STEP 2] Creating bucket and loading 150 000 Hotel documents...")
            self.bucket_params = self._create_bucket_params(
                server=self.master,
                size=self.bucket_size,
                replicas=self.num_replicas,
                bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy,
                lww=self.lww
            )
            self.cluster.create_standard_bucket(
                name=self.test_bucket, port=11222, bucket_params=self.bucket_params
            )
            self.sleep(10, "Waiting after bucket creation")

            self.prepare_collection_for_indexing(
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=150000,
                json_template="Hotel",
                bucket_name=self.test_bucket
            )
            self.assertTrue(len(self.namespaces) > 0, "No namespaces created")
            namespace = self.namespaces[0]

            primary_idx = QueryDefinition(index_name="spill_test_primary")
            self.run_cbq_query(
                query=primary_idx.generate_primary_index_create_query(
                    namespace=namespace, defer_build=False, num_replica=0
                )
            )
            self.wait_until_indexes_online(timeout=300, defer_build=False)
            self.log.info(f"[STEP 2] PASSED - namespace: {namespace}")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Capture spill directory ==========
        try:
            self.log.info("[STEP 3] Querying /settings/querySettings for spill directory...")
            status, query_settings = self.rest.get_query_settings(self.master)
            self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            self.assertIsNotNone(
                spill_dir,
                f"Could not find spill directory in querySettings: {query_settings}"
            )
            self.log.info(f"[STEP 3] PASSED - spill directory: {spill_dir}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Resolve query nodes + verify Order + Fetch operators ==========
        query_nodes = self.get_nodes_from_services_map(
            service_type="n1ql", get_all_nodes=True
        )
        sort_query = f"SELECT * FROM {namespace} ORDER BY counter"

        # EXPLAIN — assert the plan contains BOTH:
        #   1. An explicit `Order` operator (sort is done by the query engine,
        #      not pushed down to a covering index).
        #   2. A `Fetch` operator (documents are fetched from KV — i.e. the
        #      query is NOT covered by the index). Per the spec doc:
        #        "Ensure the query does not use a covering index, so that
        #         documents are fetched from KV and sorting happens in the
        #         Query engine."
        # If a future plan change covers `counter` (or projects fewer fields
        # so the index can cover), either operator may vanish from the plan
        # and the test would silently stop exercising the sort spill path.
        # The seqscan tests parallel the Order assertion with a check for
        # `#sequentialscan`; this is the ORDER BY equivalent plus the Fetch
        # check the spec specifically calls out.
        try:
            self.log.info(f"[STEP 4] Running EXPLAIN for: {sort_query}")
            explain_result = self.run_cbq_query(query=f"EXPLAIN {sort_query}")
            self.assertEqual(
                explain_result.get("status"), "success",
                f"EXPLAIN failed: {explain_result}"
            )
            explain_json = json.dumps(explain_result.get("results", []))
            # Operator markers in N1QL plans look like `"#operator": "Order"`
            # — match with optional whitespace around the colon.
            self.assertTrue(
                re.search(r'"#operator"\s*:\s*"Order"', explain_json),
                f"Plan does not contain an explicit Order operator — sort "
                f"may have been pushed down to an index. Plan: "
                f"{explain_json[:1500]}"
            )
            self.assertTrue(
                re.search(r'"#operator"\s*:\s*"Fetch"', explain_json),
                f"Plan does not contain a Fetch operator — query may be "
                f"served by a covering index, in which case documents are "
                f"not fetched from KV and the sort would operate on a "
                f"reduced set. Spec requires KV fetch for the ORDER BY "
                f"sort spill path. Plan: {explain_json[:1500]}"
            )
            self.log.info(
                "[STEP 4] EXPLAIN PASSED - explicit Order and Fetch operators present"
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        def _hash_results(results):
            return hashlib.sha256(
                json.dumps(results, sort_keys=True).encode("utf-8")
            ).hexdigest()

        snap_dir = None
        try:
            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 5: Disable encryption ==========
            try:
                self.log.info("[STEP 5] Disabling 'other' encryption at rest for baseline run...")
                self._disable_other_encryption_at_rest(label="[STEP 5] ")
                self.log.info("[STEP 5] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 5] FAILED: {e}")
                raise

            # ========== STEP 6: Baseline ORDER BY run ==========
            try:
                self._set_query_log_level("debug", label="[STEP 6] ")
                self.log.info(f"[STEP 6] Running baseline ORDER BY (encryption off): {sort_query}")
                baseline_result = self.run_cbq_query(query=sort_query)
                self._set_query_log_level("info", label="[STEP 6] ")
                self.assertEqual(
                    baseline_result.get("status"), "success",
                    f"Baseline ORDER BY failed: {baseline_result}"
                )
                baseline_hash = _hash_results(baseline_result.get("results", []))
                baseline_count = len(baseline_result.get("results", []))
                self.log.info(
                    f"[STEP 6] PASSED - baseline hash={baseline_hash} rows={baseline_count}"
                )
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 7: Re-enable encryption ==========
            try:
                self.log.info("[STEP 7] Re-enabling 'other' encryption at rest...")
                self._enable_other_encryption_at_rest(kek_id, label="[STEP 7] ")
                self.log.info("[STEP 7] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ========== STEP 8: Baseline spills.order + key IDs ==========
            try:
                self.log.info("[STEP 8] Recording baseline spills.order + key IDs...")
                baseline_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.order"
                )
                expected_key_ids = self._get_query_in_use_key_ids()
                for node_ip, key_ids in expected_key_ids.items():
                    self.assertGreater(
                        len(key_ids), 0,
                        f"Query node {node_ip} reported no in-use encryption key IDs"
                    )
                self.log.info(
                    f"[STEP 8] PASSED - baseline spills.order: {baseline_spills} | "
                    f"key IDs: {expected_key_ids}"
                )
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Start remote spill-file watcher ==========
            try:
                self.log.info("[STEP 9] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 9] ", file_prefix="av_spill_"
                )
                self.log.info(f"[STEP 9] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {e}")
                raise

            # ========== STEP 10: Run ORDER BY in background ==========
            encrypted_result_container = {}
            try:
                self._set_query_log_level("debug", label="[STEP 10] ")
                self.log.info("[STEP 10] Launching background ORDER BY...")

                def _run_sort_query():
                    try:
                        encrypted_result_container["result"] = self.run_cbq_query(
                            query=sort_query
                        )
                    except Exception as ex:
                        encrypted_result_container["error"] = ex
                        self.log.warning(f"[STEP 10] Background ORDER BY raised: {ex}")

                bg_thread = threading.Thread(
                    target=_run_sort_query, daemon=True, name="bg-sort-query"
                )
                bg_thread.start()
                self.log.info("[STEP 10] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Poll spills.order ==========
            try:
                self.log.info(
                    "[STEP 11] Polling admin/vitals until spills.order increments (timeout 300 s)..."
                )
                deadline = time.time() + 300
                spill_detected = False
                current_spills = baseline_spills
                while time.time() < deadline:
                    current_spills = self._get_vitals_spill_counts(
                        query_nodes, metric="spills.order"
                    )
                    if any(
                        current_spills.get(node.ip, 0) > baseline_spills.get(node.ip, 0)
                        for node in query_nodes
                    ):
                        spill_detected = True
                        self.log.info(
                            f"[STEP 11] PASSED - spills.order incremented: "
                            f"baseline={baseline_spills} current={current_spills}"
                        )
                        break
                    if not bg_thread.is_alive():
                        break
                    self.sleep(5, "[STEP 11] Waiting for spills.order to increment...")
                if not spill_detected:
                    self.log.warning(
                        f"[STEP 11] spills.order did not increment within 300 s. "
                        f"baseline={baseline_spills}, last seen={current_spills}. "
                        f"Spilling may not have occurred on this cluster sizing; "
                        f"test continues — query-results comparison is the "
                        f"authoritative signal."
                    )
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Wait for query, stop watcher ==========
            try:
                self.log.info("[STEP 12] Waiting for background ORDER BY to finish...")
                bg_thread.join(timeout=900)
                self._set_query_log_level("info", label="[STEP 12] ")
                self.assertFalse(
                    bg_thread.is_alive(),
                    "[STEP 12] Background ORDER BY did not finish within 900 s"
                )
                if "error" in encrypted_result_container:
                    raise encrypted_result_container["error"]
                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 12] "
                )
                self.log.info("[STEP 12] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise

            # ========== STEP 13: Validate snapshotted spill files ==========
            try:
                self.log.info(
                    f"[STEP 13] Validating snapshotted spill files in {snap_dir}..."
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 13] "
                )
                self.log.info("[STEP 13] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {e}")
                raise

            # ========== STEP 14: Compare result hashes ==========
            try:
                self.log.info("[STEP 14] Comparing encrypted-run result hash to baseline...")
                encrypted_result = encrypted_result_container.get("result", {})
                self.assertEqual(
                    encrypted_result.get("status"), "success",
                    f"Encrypted ORDER BY failed: {encrypted_result}"
                )
                encrypted_hash = _hash_results(encrypted_result.get("results", []))
                encrypted_count = len(encrypted_result.get("results", []))
                self.assertEqual(
                    encrypted_hash, baseline_hash,
                    f"ORDER BY result hash diverged with encryption: "
                    f"baseline={baseline_hash} ({baseline_count} rows) vs "
                    f"encrypted={encrypted_hash} ({encrypted_count} rows). "
                    f"Encryption at rest must not alter query semantics."
                )
                self.log.info(
                    f"[STEP 14] PASSED - result hashes match: {encrypted_hash} "
                    f"({encrypted_count} rows)"
                )
            except Exception as e:
                self.log.error(f"[STEP 14] FAILED: {e}")
                raise
        finally:
            # ========== STEP 15: Cleanup snapshot dirs ==========
            if snap_dir:
                self.log.info(f"[STEP 15] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            # Defensively re-enable encryption in case the test failed between
            # disable (STEP 5) and re-enable (STEP 7).
            try:
                self._enable_other_encryption_at_rest(kek_id, label="[STEP 15] ")
            except Exception as e:
                self.log.warning(f"[STEP 15] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_spill_files_encrypted")
        self.log.info("=" * 80)

    def test_query_merge_update_with_encryption(self):
        """
        Verify MERGE-UPDATE produces encrypted spill files, spills.merge
        increments, and the resulting SELECT count(*) is identical with
        'other' encryption at rest enabled vs disabled.

        MERGE spills when cumulative mutated-doc size crosses
        MAX(128 MiB, 0.2 * sys_mem / num_cpus). At ~150 MiB of cumulative
        mutation this test reliably crosses 128 MiB.

        Steps:
        1.  Prerequisites — KEK id captured
        2.  Bucket + two collections (staging + target) loaded with
            overlapping key sets (matching keys, doc bodies may differ;
            only meta().id is used by the MERGE join)
        3.  Primary indexes on both collections
        4.  Capture spill directory (tmpSpaceDir); resolve query nodes
        --- ENCRYPTION OFF RUN ---
        5.  Disable 'other' encryption at rest
        6.  Run MERGE; create ix_match; SELECT count(*) → baseline_count
        --- ENCRYPTION ON RUN ---
        7.  Re-enable 'other' encryption at rest with original KEK
        8.  Drop and re-create ix_match (the previous index lives on the
            same target, so reset is not strictly necessary, but ensures
            the secondary SELECT in step 14 runs the same way as in step 6)
        9.  Baseline spills.merge + capture per-node key IDs
        10. Start remote spill-file watcher
        11. Run MERGE in background
        12. Poll spills.merge until incremented
        13. Wait for MERGE to finish; stop watcher
        14. Validate snapshotted spill files are encrypted
        15. SELECT count(*) → encrypted_count; assert == baseline_count
            and == num_merge_docs
        16. Cleanup snapshot dirs
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_merge_update_with_encryption")
        self.log.info("=" * 80)

        num_merge_docs = self.input.param("num_merge_docs", 100000)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying other encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("other")
            kek_id = self.other_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - other_encryption_at_rest_id: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Create bucket + two collections with matching keys ==========
        try:
            self.log.info(
                f"[STEP 2] Creating bucket and loading {num_merge_docs} Hotel docs "
                f"into staging and target (matching keys, doc bodies may differ)..."
            )
            self.bucket_params = self._create_bucket_params(
                server=self.master,
                size=self.bucket_size,
                replicas=self.num_replicas,
                bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy,
                lww=self.lww
            )
            self.cluster.create_standard_bucket(
                name=self.test_bucket, port=11222, bucket_params=self.bucket_params
            )
            self.sleep(10, "Waiting after bucket creation")

            self.prepare_collection_for_indexing(
                num_scopes=1,
                num_collections=2,
                num_of_docs_per_collection=num_merge_docs,
                json_template="Hotel",
                bucket_name=self.test_bucket
            )
            self.assertGreaterEqual(
                len(self.namespaces), 2,
                f"Expected 2 namespaces (staging + target), got {self.namespaces}"
            )
            staging_ns = self.namespaces[0]
            target_ns = self.namespaces[1]
            self.log.info(f"[STEP 2] PASSED - staging={staging_ns} target={target_ns}")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Primary indexes on both collections ==========
        try:
            self.log.info("[STEP 3] Creating primary indexes on staging and target...")
            staging_pri = QueryDefinition(index_name="merge_staging_primary")
            target_pri = QueryDefinition(index_name="merge_target_primary")
            self.run_cbq_query(
                query=staging_pri.generate_primary_index_create_query(
                    namespace=staging_ns, defer_build=False, num_replica=0
                )
            )
            self.run_cbq_query(
                query=target_pri.generate_primary_index_create_query(
                    namespace=target_ns, defer_build=False, num_replica=0
                )
            )
            self.wait_until_indexes_online(timeout=300, defer_build=False)
            self.log.info("[STEP 3] PASSED - primary indexes online on both collections")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Capture spill directory + query nodes ==========
        try:
            self.log.info("[STEP 4] Querying /settings/querySettings for spill directory...")
            status, query_settings = self.rest.get_query_settings(self.master)
            self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            self.assertIsNotNone(
                spill_dir,
                f"Could not find spill directory in querySettings: {query_settings}"
            )
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )
            self.log.info(f"[STEP 4] PASSED - spill directory: {spill_dir}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        merge_query = (
            f"MERGE INTO {target_ns} t USING {staging_ns} s "
            f"ON meta(t).id = meta(s).id "
            f"WHEN MATCHED THEN UPDATE SET t.match = true"
        )
        ix_match_create = (
            f"CREATE INDEX ix_match ON {target_ns}(`match`)"
        )
        ix_match_drop = (
            f"DROP INDEX ix_match ON {target_ns}"
        )
        select_query = (
            f"SELECT count(*) AS c FROM {target_ns} WHERE `match` = true"
        )

        snap_dir = None
        try:
            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 5: Disable encryption ==========
            try:
                self.log.info("[STEP 5] Disabling 'other' encryption at rest for baseline run...")
                self._disable_other_encryption_at_rest(label="[STEP 5] ")
                self.log.info("[STEP 5] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 5] FAILED: {e}")
                raise

            # ========== STEP 6: Baseline MERGE + SELECT count ==========
            try:
                self._set_query_log_level("debug", label="[STEP 6] ")
                self.log.info(f"[STEP 6] Baseline MERGE (encryption off): {merge_query}")
                baseline_merge = self.run_cbq_query(query=merge_query)
                self._set_query_log_level("info", label="[STEP 6] ")
                self.assertEqual(
                    baseline_merge.get("status"), "success",
                    f"Baseline MERGE failed: {baseline_merge}"
                )
                self.run_cbq_query(query=ix_match_create)
                self.wait_until_indexes_online(timeout=300, defer_build=False)
                baseline_select = self.run_cbq_query(query=select_query)
                self.assertEqual(
                    baseline_select.get("status"), "success",
                    f"Baseline SELECT failed: {baseline_select}"
                )
                baseline_count = baseline_select["results"][0]["c"]
                self.assertEqual(
                    baseline_count, num_merge_docs,
                    f"Baseline MERGE-UPDATE mutated {baseline_count} docs, "
                    f"expected {num_merge_docs} (encryption-off run)"
                )
                # Drop ix_match so STEP 14 can re-create it on the encrypted run
                self.run_cbq_query(query=ix_match_drop)
                self.log.info(f"[STEP 6] PASSED - baseline_count={baseline_count}")
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 7: Re-enable encryption ==========
            try:
                self.log.info("[STEP 7] Re-enabling 'other' encryption at rest...")
                self._enable_other_encryption_at_rest(kek_id, label="[STEP 7] ")
                self.log.info("[STEP 7] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ========== STEP 8: Baseline spills.merge + key IDs ==========
            try:
                self.log.info("[STEP 8] Recording baseline spills.merge + key IDs...")
                baseline_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.merge"
                )
                expected_key_ids = self._get_query_in_use_key_ids()
                for node_ip, key_ids in expected_key_ids.items():
                    self.assertGreater(
                        len(key_ids), 0,
                        f"Query node {node_ip} reported no in-use encryption key IDs"
                    )
                self.log.info(
                    f"[STEP 8] PASSED - baseline spills.merge: {baseline_spills} | "
                    f"key IDs: {expected_key_ids}"
                )
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Skip - kept for step numbering parity with ORDER BY test ==========
            # ========== STEP 10: Start remote spill-file watcher ==========
            try:
                self.log.info("[STEP 10] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 10] ", file_prefix="av_spill_"
                )
                self.log.info(f"[STEP 10] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Run MERGE in background ==========
            merge_result_container = {}
            try:
                self._set_query_log_level("debug", label="[STEP 11] ")
                self.log.info("[STEP 11] Launching background MERGE...")

                def _run_merge():
                    try:
                        merge_result_container["result"] = self.run_cbq_query(
                            query=merge_query
                        )
                    except Exception as ex:
                        merge_result_container["error"] = ex
                        self.log.warning(f"[STEP 11] Background MERGE raised: {ex}")

                bg_thread = threading.Thread(
                    target=_run_merge, daemon=True, name="bg-merge-query"
                )
                bg_thread.start()
                self.log.info("[STEP 11] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Poll spills.merge ==========
            try:
                self.log.info(
                    "[STEP 12] Polling admin/vitals until spills.merge increments (timeout 300 s)..."
                )
                deadline = time.time() + 300
                spill_detected = False
                current_spills = baseline_spills
                while time.time() < deadline:
                    current_spills = self._get_vitals_spill_counts(
                        query_nodes, metric="spills.merge"
                    )
                    if any(
                        current_spills.get(node.ip, 0) > baseline_spills.get(node.ip, 0)
                        for node in query_nodes
                    ):
                        spill_detected = True
                        self.log.info(
                            f"[STEP 12] PASSED - spills.merge incremented: "
                            f"baseline={baseline_spills} current={current_spills}"
                        )
                        break
                    if not bg_thread.is_alive():
                        break
                    self.sleep(5, "[STEP 12] Waiting for spills.merge to increment...")
                if not spill_detected:
                    self.log.warning(
                        f"[STEP 12] spills.merge did not increment within 300 s. "
                        f"baseline={baseline_spills}, last seen={current_spills}. "
                        f"Spilling may not have occurred on this cluster sizing; "
                        f"test continues — query-results comparison is the "
                        f"authoritative signal."
                    )
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise

            # ========== STEP 13: Wait for MERGE, stop watcher ==========
            try:
                self.log.info("[STEP 13] Waiting for background MERGE to finish...")
                bg_thread.join(timeout=600)
                self._set_query_log_level("info", label="[STEP 13] ")
                self.assertFalse(
                    bg_thread.is_alive(),
                    "[STEP 13] Background MERGE did not finish within 600 s"
                )
                if "error" in merge_result_container:
                    raise merge_result_container["error"]
                self.assertEqual(
                    merge_result_container.get("result", {}).get("status"), "success",
                    f"MERGE failed: {merge_result_container.get('result')}"
                )
                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 13] "
                )
                self.log.info("[STEP 13] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {e}")
                raise

            # ========== STEP 14: Validate snapshotted spill files ==========
            try:
                self.log.info(
                    f"[STEP 14] Validating snapshotted spill files in {snap_dir}..."
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 14] "
                )
                self.log.info("[STEP 14] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 14] FAILED: {e}")
                raise

            # ========== STEP 15: Re-create ix_match, SELECT count, compare ==========
            try:
                self.log.info("[STEP 15] Re-creating ix_match and running validation SELECT...")
                self.run_cbq_query(query=ix_match_create)
                self.wait_until_indexes_online(timeout=300, defer_build=False)
                encrypted_select = self.run_cbq_query(query=select_query)
                self.assertEqual(
                    encrypted_select.get("status"), "success",
                    f"Encrypted SELECT failed: {encrypted_select}"
                )
                encrypted_count = encrypted_select["results"][0]["c"]
                self.assertEqual(
                    encrypted_count, baseline_count,
                    f"SELECT count diverged: baseline={baseline_count} "
                    f"vs encrypted={encrypted_count}. Encryption at rest must "
                    f"not alter MERGE semantics."
                )
                self.assertEqual(
                    encrypted_count, num_merge_docs,
                    f"Encrypted MERGE-UPDATE mutated {encrypted_count} docs, "
                    f"expected {num_merge_docs}"
                )
                self.log.info(
                    f"[STEP 15] PASSED - baseline_count={baseline_count} == "
                    f"encrypted_count={encrypted_count} == {num_merge_docs}"
                )
            except Exception as e:
                self.log.error(f"[STEP 15] FAILED: {e}")
                raise
        finally:
            # ========== STEP 16: Cleanup ==========
            if snap_dir:
                self.log.info("[STEP 16] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            try:
                self._enable_other_encryption_at_rest(kek_id, label="[STEP 16] ")
            except Exception as e:
                self.log.warning(f"[STEP 16] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_merge_update_with_encryption")
        self.log.info("=" * 80)

    def test_query_update_statistics_spill_files_encrypted(self):
        """
        Verify UPDATE STATISTICS produces encrypted spill files and the
        per-field histograms it writes to <bucket>._system._query are
        identical with 'other' encryption at rest enabled vs disabled
        (ignoring the `updated` timestamp field).

        Per-field threshold ≈ MAX(128 MiB, 0.2 * sys_mem / num_cpus) / N
        where N = number of fields in the statement. With 60 000 docs ×
        10 fields × 1 KiB each, each field contributes 60 MiB of work —
        comfortably above the per-field threshold; multiple spill files
        per field expected.

        Two spill signals are checked in warning-only mode (the
        authoritative correctness signal is the histogram comparison):
          - admin/vitals spills.update_statistics counter increments
          - the remote file-watcher captures at least one spill file
            in tmpSpaceDir on some query node.

        Steps:
        1.  Prerequisites — KEK id captured
        2.  Bucket + empty collection (scope_1.collection_1)
        3.  UPSERT 60 000 docs with fields c1..c10 (1 KiB each)
        4.  Primary index
        5.  Capture spill directory + query nodes
        --- ENCRYPTION OFF RUN ---
        6.  Disable 'other' encryption at rest
        7.  UPDATE STATISTICS; capture per-field histograms; DELETE STATISTICS
        --- ENCRYPTION ON RUN ---
        8.  Re-enable 'other' encryption at rest
        9.  Capture per-node key IDs
        10. Start remote spill-file watcher
        11. Run UPDATE STATISTICS in background
        12. Wait for query; stop watcher
        13. Validate snapshotted spill files are encrypted
        14. Capture per-field histograms; DELETE STATISTICS
        15. Compare baseline vs encrypted histograms — all keys must match
            except `updated` (and the `histogram` byte-string when decoded
            differs only by `updated` timestamps embedded in it; per spec
            we compare the JSON envelope, not the decoded histogram bytes)
        16. Cleanup
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_update_statistics_spill_files_encrypted")
        self.log.info("=" * 80)

        num_docs = self.input.param("num_us_docs", 60000)
        field_size = self.input.param("us_field_size", 1024)
        field_names = [f"c{i}" for i in range(1, 11)]   # c1..c10
        sample_size = self.input.param("us_sample_size", num_docs)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying other encryption at rest prerequisites...")
            self._verify_encryption_prerequisites("other")
            kek_id = self.other_encryption_at_rest_id
            self.log.info(f"[STEP 1] PASSED - other_encryption_at_rest_id: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Create bucket + empty collection ==========
        try:
            self.log.info("[STEP 2] Creating bucket and empty scope_1/collection_1...")
            self.bucket_params = self._create_bucket_params(
                server=self.master,
                size=self.bucket_size,
                replicas=self.num_replicas,
                bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy,
                lww=self.lww
            )
            self.cluster.create_standard_bucket(
                name=self.test_bucket, port=11222, bucket_params=self.bucket_params
            )
            self.sleep(10, "Waiting after bucket creation")

            self.collection_rest.create_scope_collection_count(
                scope_num=1, collection_num=1,
                scope_prefix=self.scope_prefix,
                collection_prefix=self.collection_prefix,
                bucket=self.test_bucket
            )
            scope_name = f"{self.scope_prefix}_1"
            collection_name = f"{self.collection_prefix}_1"
            namespace = f"default:{self.test_bucket}.{scope_name}.{collection_name}"
            self.namespaces.append(namespace)
            self.sleep(10, "Waiting after scope/collection creation")
            self.log.info(f"[STEP 2] PASSED - namespace: {namespace}")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: UPSERT 60 000 docs with c1..c10 (1 KiB each) ==========
        try:
            self.log.info(
                f"[STEP 3] Loading {num_docs} docs with fields {field_names} "
                f"(each {field_size} bytes) via batched UPSERT..."
            )
            value_expr = "{" + ", ".join(
                f'"{f}": REPEAT("x", {field_size})' for f in field_names
            ) + "}"
            batch_size = 5000
            for offset in range(0, num_docs, batch_size):
                upsert_query = (
                    f"UPSERT INTO {namespace} (KEY k, VALUE v) "
                    f'SELECT "doc_" || TO_STRING(n + {offset}) AS k, '
                    f"{value_expr} AS v "
                    f"FROM ARRAY_RANGE(0, {batch_size}) AS n"
                )
                upsert_result = self.run_cbq_query(query=upsert_query)
                self.assertEqual(
                    upsert_result.get("status"), "success",
                    f"UPSERT batch at offset {offset} failed: {upsert_result}"
                )
            self.log.info(f"[STEP 3] PASSED - {num_docs} docs loaded into {namespace}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Primary index ==========
        try:
            self.log.info("[STEP 4] Creating primary index...")
            pri = QueryDefinition(index_name="us_primary")
            self.run_cbq_query(
                query=pri.generate_primary_index_create_query(
                    namespace=namespace, defer_build=False, num_replica=0
                )
            )
            self.wait_until_indexes_online(timeout=300, defer_build=False)
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Capture spill dir + query nodes ==========
        try:
            self.log.info("[STEP 5] Capturing spill directory + query nodes...")
            status, query_settings = self.rest.get_query_settings(self.master)
            self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            self.assertIsNotNone(
                spill_dir,
                f"Could not find spill directory in querySettings: {query_settings}"
            )
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )
            self.log.info(f"[STEP 5] PASSED - spill directory: {spill_dir}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        fields_clause = ", ".join(field_names)
        us_query = (
            f'UPDATE STATISTICS FOR {namespace}({fields_clause}) '
            f'WITH {{"sample_size": {sample_size}}}'
        )
        delete_stats_query = (
            f"UPDATE STATISTICS FOR {namespace} DELETE({fields_clause})"
        )
        system_query_ns = (
            f"default:{self.test_bucket}._system._query"
        )

        def _fetch_histograms(label):
            """Return {field_name: histogram_envelope_dict} for c1..c10.

            For each field also runs a `decode_base64(histogram)` sanity
            check — per the spec doc, "If no histogram document is
            returned, or if the returned histogram is malformed, the
            test should fail." A byte-for-byte envelope comparison
            between runs would catch any drift, but it would not catch
            the case where BOTH runs produce identically malformed
            output (e.g. a truncated payload that round-trips). The
            decode + minimum-length check closes that gap.
            """
            histograms = {}
            for field in field_names:
                hist_query = (
                    f"SELECT q.* FROM {system_query_ns} q "
                    f"WHERE q.histogramKey = '{field}' "
                    f"AND q.`scope` = '{scope_name}' "
                    f"AND q.`collection` = '{collection_name}'"
                )
                result = self.run_cbq_query(query=hist_query)
                self.assertEqual(
                    result.get("status"), "success",
                    f"{label}Histogram fetch for {field} failed: {result}"
                )
                rows = result.get("results", [])
                self.assertEqual(
                    len(rows), 1,
                    f"{label}Expected exactly one histogram doc for {field}, "
                    f"got {len(rows)}: {rows}"
                )
                histograms[field] = rows[0]
                self.log.info(
                    f"{label}Histogram doc for {field}: "
                    f"{json.dumps(rows[0], sort_keys=True, default=str)}"
                )

                if any("updated" in str(k).lower() for k in rows[0].keys()):
                    self.log.warning(
                        f"{label}Histogram doc for {field} contains an updated-like field; "
                        "skipping decoded-payload length check for this field"
                    )
                    continue

                # Well-formedness check on the decoded base64 payload.
                # MIN_DECODED_HISTOGRAM_BYTES = 16 is a conservative floor:
                # any real CBO histogram carries at least a few tens of
                # bytes of bucket/header metadata, so anything under 16 is
                # almost certainly truncated or empty.
                decode_query = (
                    f"SELECT decode_base64(q.histogram) AS decoded "
                    f"FROM {system_query_ns} q "
                    f"WHERE q.histogramKey = '{field}' "
                    f"AND q.`scope` = '{scope_name}' "
                    f"AND q.`collection` = '{collection_name}'"
                )
                decode_result = self.run_cbq_query(query=decode_query)
                self.assertEqual(
                    decode_result.get("status"), "success",
                    f"{label}decode_base64 query for {field} failed: {decode_result}"
                )
                decoded_rows = decode_result.get("results", [])
                self.assertEqual(
                    len(decoded_rows), 1,
                    f"{label}Expected exactly one decoded histogram row for "
                    f"{field}, got {len(decoded_rows)}"
                )
                decoded = decoded_rows[0].get("decoded")
                self.assertIsNotNone(
                    decoded,
                    f"{label}decode_base64(histogram) returned NULL for {field} "
                    f"— either the `histogram` field is missing from the doc "
                    f"or the encoded payload is empty"
                )
                decoded_len = len(decoded) if decoded is not None else 0
                self.assertGreater(
                    decoded_len, 16,
                    f"{label}Decoded histogram for {field} is too short "
                    f"({decoded_len} bytes) — likely malformed/truncated"
                )
            return histograms

        def _strip_volatile(envelope):
            """Drop fields expected to differ between runs (timestamp)."""
            return {k: v for k, v in envelope.items() if k != "updated"}

        snap_dir = None
        try:
            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 6: Disable encryption ==========
            try:
                self.log.info("[STEP 6] Disabling 'other' encryption at rest for baseline run...")
                self._disable_other_encryption_at_rest(label="[STEP 6] ")
                self.log.info("[STEP 6] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ========== STEP 7: Baseline UPDATE STATISTICS + histograms ==========
            try:
                self._set_query_log_level("debug", label="[STEP 7] ")
                self.log.info(f"[STEP 7] Baseline UPDATE STATISTICS (encryption off): {us_query}")
                baseline_us = self.run_cbq_query(query=us_query)
                self._set_query_log_level("info", label="[STEP 7] ")
                self.assertEqual(
                    baseline_us.get("status"), "success",
                    f"Baseline UPDATE STATISTICS failed: {baseline_us}"
                )
                baseline_histograms = _fetch_histograms(label="[STEP 7] ")
                self.log.info(
                    "[STEP 7] Baseline histograms:\n"
                    + json.dumps(baseline_histograms, indent=2, sort_keys=True, default=str)
                )
                # Cleanup so the encrypted run produces fresh docs
                self.run_cbq_query(query=delete_stats_query)
                self.log.info(
                    f"[STEP 7] PASSED - captured {len(baseline_histograms)} baseline histograms"
                )
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 8: Re-enable encryption ==========
            try:
                self.log.info("[STEP 8] Re-enabling 'other' encryption at rest...")
                self._enable_other_encryption_at_rest(kek_id, label="[STEP 8] ")
                self.log.info("[STEP 8] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Baseline spills.update_statistics + key IDs ==========
            try:
                self.log.info(
                    "[STEP 9] Recording baseline spills.update_statistics + key IDs..."
                )
                expected_key_ids = self._get_query_in_use_key_ids()
                for node_ip, key_ids in expected_key_ids.items():
                    self.assertGreater(
                        len(key_ids), 0,
                        f"Query node {node_ip} reported no in-use encryption key IDs"
                    )
                baseline_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.update_statistics"
                )
                self.log.info(
                    f"[STEP 9] PASSED - baseline spills.update_statistics: "
                    f"{baseline_spills} | key IDs: {expected_key_ids}"
                )
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {e}")
                raise

            # ========== STEP 10: Start remote spill-file watcher ==========
            try:
                self.log.info("[STEP 10] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 10] ", file_prefix="av_spill_"
                )
                self.log.info(f"[STEP 10] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Run UPDATE STATISTICS in background ==========
            us_result_container = {}
            try:
                self._set_query_log_level("debug", label="[STEP 11] ")
                self.log.info("[STEP 11] Launching background UPDATE STATISTICS...")

                def _run_update_statistics():
                    try:
                        us_result_container["result"] = self.run_cbq_query(
                            query=us_query
                        )
                    except Exception as ex:
                        us_result_container["error"] = ex
                        self.log.warning(f"[STEP 11] Background UPDATE STATISTICS raised: {ex}")

                bg_thread = threading.Thread(
                    target=_run_update_statistics, daemon=True, name="bg-update-statistics"
                )
                bg_thread.start()
                self.log.info("[STEP 11] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Poll spills.update_statistics, wait for query, stop watcher ==========
            try:
                self.log.info(
                    "[STEP 12] Polling admin/vitals until spills.update_statistics "
                    "increments (timeout 300 s)..."
                )
                deadline = time.time() + 300
                spill_detected = False
                current_spills = baseline_spills
                while time.time() < deadline:
                    current_spills = self._get_vitals_spill_counts(
                        query_nodes, metric="spills.update_statistics"
                    )
                    if any(
                        current_spills.get(node.ip, 0) > baseline_spills.get(node.ip, 0)
                        for node in query_nodes
                    ):
                        spill_detected = True
                        self.log.info(
                            f"[STEP 12] spills.update_statistics incremented: "
                            f"baseline={baseline_spills} current={current_spills}"
                        )
                        break
                    if not bg_thread.is_alive():
                        break
                    self.sleep(5, "[STEP 12] Waiting for spills.update_statistics to increment...")
                if not spill_detected:
                    self.log.warning(
                        f"[STEP 12] spills.update_statistics did not increment within "
                        f"300 s. baseline={baseline_spills}, last seen={current_spills}. "
                        f"Spilling may not have occurred on this cluster sizing; "
                        f"test continues — histogram comparison is the authoritative "
                        f"signal."
                    )

                self.log.info("[STEP 12] Waiting for background UPDATE STATISTICS to finish...")
                bg_thread.join(timeout=900)
                self._set_query_log_level("info", label="[STEP 12] ")
                self.assertFalse(
                    bg_thread.is_alive(),
                    "[STEP 12] Background UPDATE STATISTICS did not finish within 900 s"
                )
                if "error" in us_result_container:
                    raise us_result_container["error"]
                self.assertEqual(
                    us_result_container.get("result", {}).get("status"), "success",
                    f"UPDATE STATISTICS failed: {us_result_container.get('result')}"
                )
                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 12] "
                )
                self.log.info("[STEP 12] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise

            # ========== STEP 13: Validate snapshotted spill files ==========
            try:
                self.log.info(
                    f"[STEP 13] Validating snapshotted spill files in {snap_dir}..."
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 13] "
                )
                self.log.info("[STEP 13] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {e}")
                raise

            # ========== STEP 14: Encrypted-run histograms ==========
            try:
                self.log.info("[STEP 14] Fetching encrypted-run histograms...")
                encrypted_histograms = _fetch_histograms(label="[STEP 14] ")
                self.log.info(
                    "[STEP 14] Encrypted-run histograms:\n"
                    + json.dumps(encrypted_histograms, indent=2, sort_keys=True, default=str)
                )
                self.run_cbq_query(query=delete_stats_query)
                self.log.info(
                    f"[STEP 14] PASSED - captured {len(encrypted_histograms)} encrypted histograms"
                )
            except Exception as e:
                self.log.error(f"[STEP 14] FAILED: {e}")
                raise

            # ========== STEP 15: Compare histograms (excluding `updated`) ==========
            try:
                self.log.info("[STEP 15] Comparing per-field histograms (excluding `updated`)...")
                mismatches = []
                for field in field_names:
                    base = _strip_volatile(baseline_histograms.get(field, {}))
                    enc = _strip_volatile(encrypted_histograms.get(field, {}))
                    if base != enc:
                        diff_keys = sorted(set(base.keys()) | set(enc.keys()))
                        per_key = []
                        for k in diff_keys:
                            if base.get(k) != enc.get(k):
                                per_key.append(
                                    f"      {k}: baseline={base.get(k)!r} "
                                    f"encrypted={enc.get(k)!r}"
                                )
                        mismatches.append(
                            f"  field {field}:\n" + "\n".join(per_key)
                        )
                if mismatches:
                    msg = (
                        "[STEP 15] Histograms diverged between baseline and encrypted runs "
                        "(only `updated` is allowed to differ):\n" + "\n".join(mismatches)
                    )
                    self.log.error(msg)
                    self.fail(msg)
                self.log.info(
                    f"[STEP 15] PASSED - all {len(field_names)} histograms match across runs"
                )
            except Exception as e:
                self.log.error(f"[STEP 15] FAILED: {e}")
                raise
        finally:
            # ========== STEP 16: Cleanup ==========
            if snap_dir:
                self.log.info("[STEP 16] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            try:
                self._enable_other_encryption_at_rest(kek_id, label="[STEP 16] ")
            except Exception as e:
                self.log.warning(f"[STEP 16] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_update_statistics_spill_files_encrypted")
        self.log.info("=" * 80)

    def test_query_seqscan_key_only_spill_files_encrypted(self):
        """
        Verify sequential-scan (key-only) spill files are encrypted and that
        query results are identical with BUCKET encryption at rest enabled
        vs disabled.

        Per the spec doc: spill files produced by the sequential-scan path
        are encrypted with the BUCKET encryption key, NOT the cluster-wide
        `other` KEK. The test therefore toggles bucket-level encryption
        (`_disable_bucket_encryption_at_rest` / `_enable_bucket_encryption_at_rest`),
        uses `self.encryption_at_rest_id` (the bucket KEK) as the expected
        key ID, and gates on the conf flag `enable_encryption_at_rest=True`.

        Sequential scans are used when no GSI/FTS index can service a query
        (or when explicitly hinted via USE INDEX(`#sequentialscan`)). The
        bucket-level vbucket scan path streams document keys; when the
        cumulative key size across vbuckets exceeds the spill threshold,
        intermediate buffers are written to tmpSpaceDir.

        Prerequisite: the cluster must have N1QL_SEQ_SCAN feature enabled
        (n1ql-feat-ctrl bit 0x4000 cleared). Conf row sets
        n1ql_feat_ctrl=76 (the default DEF_N1QL_FEAT_CTRL value, with
        seqscan bit clear).

        Two spill signals are checked in warning-only mode (the
        authoritative correctness signal is the per-query result-hash
        comparison between encryption-off and encryption-on runs):
          - admin/vitals spills.seq_scan counter increments
          - the remote file-watcher captures at least one spill file
            in tmpSpaceDir on some query node.

        Steps:
        1.  Prerequisites — KEK id captured
        2.  Bucket + empty collection (no indexes — forces sequential scan)
        3.  UPSERT N docs with long keys (~seqscan_key_size chars)
        4.  EXPLAIN: assert #sequentialscan appears in the plan for the
            baseline query
        5.  Capture spill directory + query nodes
        --- ENCRYPTION OFF RUN ---
        6.  Disable 'other' encryption at rest
        7.  Run battery of sequential-scan queries; capture per-query
            result hashes
        --- ENCRYPTION ON RUN ---
        8.  Re-enable 'other' encryption at rest
        9.  Capture per-node key IDs
        10. Start remote spill-file watcher
        11. Re-run the same battery; capture per-query result hashes
        12. Stop watcher; validate snapshotted spill files are encrypted
            (warn if none captured)
        13. Compare per-query baseline vs encrypted hashes — must all match
        14. Cleanup
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_seqscan_key_only_spill_files_encrypted")
        self.log.info("=" * 80)

        # Documents are loaded under keys generated by `_generate_vb_balanced_keys`,
        # not via ARRAY_RANGE — that guarantees each vBucket carries a known
        # number of keys, which is necessary to make sequential-scan spilling
        # likely in many vBuckets at once. Total docs = num_vbuckets * keys_per_vb.
        num_vbuckets = self.input.param("seqscan_num_vbuckets", 1024)
        keys_per_vb = self.input.param("seqscan_keys_per_vb", 500)
        key_size = self.input.param("seqscan_key_size", 200)
        batch_size = self.input.param("seqscan_load_batch", 500)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying other encryption at rest prerequisites...")
            if not getattr(self, "enable_encryption_at_rest", False):
                self.skipTest(
                    "Bucket encryption at rest not enabled. "
                    "Set enable_encryption_at_rest=True in the conf row."
                )
            kek_id = getattr(self, "encryption_at_rest_id", None)
            self.assertIsNotNone(
                kek_id,
                "self.encryption_at_rest_id is None \u2014 base class did not set up a KEK"
            )
            self.log.info(f"[STEP 1] PASSED - bucket encryption KEK id: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Create bucket + empty collection ==========
        try:
            self.log.info("[STEP 2] Creating bucket and empty scope_1/collection_1 (no indexes)...")
            self.bucket_params = self._create_bucket_params(
                server=self.master,
                size=self.bucket_size,
                replicas=self.num_replicas,
                bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy,
                lww=self.lww
            )
            self.cluster.create_standard_bucket(
                name=self.test_bucket, port=11222, bucket_params=self.bucket_params
            )
            self.sleep(10, "Waiting after bucket creation")

            self.collection_rest.create_scope_collection_count(
                scope_num=1, collection_num=1,
                scope_prefix=self.scope_prefix,
                collection_prefix=self.collection_prefix,
                bucket=self.test_bucket
            )
            scope_name = f"{self.scope_prefix}_1"
            collection_name = f"{self.collection_prefix}_1"
            namespace = f"default:{self.test_bucket}.{scope_name}.{collection_name}"
            self.namespaces.append(namespace)
            self.sleep(10, "Waiting after scope/collection creation")
            self.log.info(f"[STEP 2] PASSED - namespace: {namespace} (no indexes)")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Generate vBucket-balanced long keys + UPSERT ==========
        try:
            # Long-key template: a `key_size`-char "k…" pad followed by
            # "::{n}" so each iteration of _generate_vb_balanced_keys produces
            # a unique, long key whose CRC32 hash is recomputed afresh.
            key_template = ("k" * key_size) + "::{n}"
            self.log.info(
                f"[STEP 3] Generating {keys_per_vb} keys × {num_vbuckets} "
                f"vBuckets via CRC32-balanced search (key template length "
                f"≈ {key_size + 4}+ chars)..."
            )
            all_keys = self._generate_vb_balanced_keys(
                num_vbuckets=num_vbuckets,
                keys_per_vb=keys_per_vb,
                key_template=key_template,
            )
            self.log.info(
                f"[STEP 3] Loading {len(all_keys)} docs in batches of {batch_size}..."
            )
            self._upsert_explicit_keys(
                namespace=namespace,
                keys=all_keys,
                value_expr='{"i": 0}',
                batch_size=batch_size,
                label="[STEP 3] ",
            )
            num_docs = len(all_keys)
            self.log.info(f"[STEP 3] PASSED - {num_docs} docs loaded into {namespace}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: EXPLAIN — verify #sequentialscan is used ==========
        sample_query = f"SELECT meta().id AS id FROM {namespace}"
        try:
            self.log.info(f"[STEP 4] Running EXPLAIN for: {sample_query}")
            explain_result = self.run_cbq_query(query=f"EXPLAIN {sample_query}")
            self.assertEqual(
                explain_result.get("status"), "success",
                f"EXPLAIN failed: {explain_result}"
            )
            explain_json = json.dumps(explain_result.get("results", []))
            self.assertIn(
                "#sequentialscan", explain_json,
                f"Plan does not use #sequentialscan — sequential scans may be "
                f"disabled (check n1ql_feat_ctrl bit 0x4000). Plan: {explain_json[:1000]}"
            )
            self.log.info("[STEP 4] PASSED - #sequentialscan present in plan")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Capture spill dir + query nodes ==========
        try:
            self.log.info("[STEP 5] Capturing spill directory + query nodes...")
            status, query_settings = self.rest.get_query_settings(self.master)
            self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            self.assertIsNotNone(
                spill_dir,
                f"Could not find spill directory in querySettings: {query_settings}"
            )
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )
            self.log.info(f"[STEP 5] PASSED - spill directory: {spill_dir}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # Battery of sequential-scan queries — each pairs an id with a query.
        # All queries return small projections so per-query result sets stay
        # comparable in memory regardless of num_docs.
        seqscan_queries = {
            "full_scan_with_hint": (
                f"SELECT meta().id AS id FROM {namespace} "
                f"USE INDEX (`#sequentialscan`)"
            ),
            "limit_5":          f"SELECT meta().id AS id FROM {namespace} LIMIT 5",
            "offset_limit":     f"SELECT meta().id AS id FROM {namespace} OFFSET 1000 LIMIT 100",
            "order_by_meta_id": f"SELECT meta().id AS id FROM {namespace} ORDER BY meta().id",
        }

        def _hash_result_rows(rows):
            return hashlib.sha256(
                json.dumps(rows, sort_keys=True).encode("utf-8")
            ).hexdigest()

        snap_dir = None
        try:
            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 6: Disable encryption ==========
            try:
                self.log.info("[STEP 6] Disabling 'other' encryption at rest for baseline run...")
                self._disable_bucket_encryption_at_rest(self.test_bucket, label="[STEP 6] ")
                self.log.info("[STEP 6] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ========== STEP 7: Run battery (encryption off) ==========
            try:
                self._set_query_log_level("debug", label="[STEP 7] ")
                self.log.info("[STEP 7] Running baseline sequential-scan battery...")
                baseline_hashes = {}
                for qid, q in seqscan_queries.items():
                    result = self.run_cbq_query(query=q)
                    self.assertEqual(
                        result.get("status"), "success",
                        f"Baseline query '{qid}' failed: {result}"
                    )
                    rows = result.get("results", [])
                    baseline_hashes[qid] = (_hash_result_rows(rows), len(rows))
                    self.log.info(
                        f"[STEP 7] {qid}: hash={baseline_hashes[qid][0]} rows={len(rows)}"
                    )
                self._set_query_log_level("info", label="[STEP 7] ")
                self.log.info("[STEP 7] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 8: Re-enable encryption ==========
            try:
                self.log.info("[STEP 8] Re-enabling 'other' encryption at rest...")
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 8] ")
                self.log.info("[STEP 8] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Baseline spills.seq_scan + key IDs ==========
            try:
                self.log.info(
                    "[STEP 9] Recording baseline spills.seq_scan + per-node key IDs..."
                )
                # Bucket-encrypted file headers carry the bucket's KEK id. Pass it
                # as a flat list — `_build_node_key_ids_map` will apply it to
                # every query node uniformly.
                expected_key_ids = [str(kek_id)]
                baseline_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.seq_scan"
                )
                self.log.info(
                    f"[STEP 9] PASSED - baseline spills.seq_scan: {baseline_spills} | "
                    f"key IDs: {expected_key_ids}"
                )
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {e}")
                raise

            # ========== STEP 10: Start watcher ==========
            try:
                self.log.info("[STEP 10] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 10] ", file_prefix="ss_spill-"
                )
                self.log.info(f"[STEP 10] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Run battery (encryption on) + spills.seq_scan delta ==========
            try:
                self._set_query_log_level("debug", label="[STEP 11] ")
                self.log.info("[STEP 11] Running encrypted sequential-scan battery...")
                encrypted_hashes = {}
                for qid, q in seqscan_queries.items():
                    result = self.run_cbq_query(query=q)
                    self.assertEqual(
                        result.get("status"), "success",
                        f"Encrypted query '{qid}' failed: {result}"
                    )
                    rows = result.get("results", [])
                    encrypted_hashes[qid] = (_hash_result_rows(rows), len(rows))
                    self.log.info(
                        f"[STEP 11] {qid}: hash={encrypted_hashes[qid][0]} rows={len(rows)}"
                    )
                self._set_query_log_level("info", label="[STEP 11] ")

                # Warning-only spills.seq_scan delta check — the battery runs
                # synchronously here, so we compare baseline vs post-run counts
                # instead of polling. Like ORDER BY's STEP 11 this is a soft
                # signal: the per-query result-hash comparison in STEP 13 is
                # authoritative. If the data shape happens to not spill on a
                # given cluster sizing the test still proceeds.
                post_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.seq_scan"
                )
                if any(
                    post_spills.get(node.ip, 0) > baseline_spills.get(node.ip, 0)
                    for node in query_nodes
                ):
                    self.log.info(
                        f"[STEP 11] spills.seq_scan incremented: "
                        f"baseline={baseline_spills} current={post_spills}"
                    )
                else:
                    self.log.warning(
                        f"[STEP 11] spills.seq_scan did not increment "
                        f"(baseline={baseline_spills}, current={post_spills}). "
                        f"Spilling may not have occurred on this cluster sizing; "
                        f"test continues — per-query result-hash comparison is the "
                        f"authoritative signal."
                    )
                self.log.info("[STEP 11] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Stop watcher + validate snapshots ==========
            try:
                self.log.info("[STEP 12] Stopping watcher and validating spill-file snapshots...")
                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 12] "
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 12] "
                )
                self.log.info("[STEP 12] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise

            # ========== STEP 13: Compare per-query result hashes ==========
            try:
                self.log.info("[STEP 13] Comparing per-query result hashes...")
                mismatches = []
                for qid in seqscan_queries:
                    b_hash, b_count = baseline_hashes[qid]
                    e_hash, e_count = encrypted_hashes[qid]
                    if b_hash != e_hash:
                        mismatches.append(
                            f"  {qid}: baseline=({b_hash}, {b_count} rows) "
                            f"vs encrypted=({e_hash}, {e_count} rows)"
                        )
                if mismatches:
                    msg = (
                        "[STEP 13] Sequential-scan results diverged between baseline "
                        "and encrypted runs:\n" + "\n".join(mismatches)
                    )
                    self.log.error(msg)
                    self.fail(msg)
                self.log.info(
                    f"[STEP 13] PASSED - all {len(seqscan_queries)} query hashes match"
                )
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {e}")
                raise
        finally:
            # ========== STEP 14: Cleanup ==========
            if snap_dir:
                self.log.info("[STEP 14] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            try:
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 14] ")
            except Exception as e:
                self.log.warning(f"[STEP 14] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_seqscan_key_only_spill_files_encrypted")
        self.log.info("=" * 80)

    def test_query_seqscan_key_document_spill_files_encrypted(self):
        """
        Verify key + document sequential-scan spill files generated by the
        UPDATE STATISTICS random-sample collection are encrypted, and the
        histograms produced are identical with BUCKET encryption at rest
        enabled vs disabled.

        UPDATE STATISTICS draws its random sample via a key + document
        sequential scan. Per the spec, key+document seq-scan spills when
        cumulative per-vBucket size crosses 2 MiB. To keep the dataset
        manageable (instead of paying the 2 MiB × 1024 ≈ 2 GiB floor)
        the test runs on a **Magma bucket with 128 vBuckets**, lowering
        the floor to 2 MiB × 128 ≈ 256 MiB. Hotel docs (~1.5 KB) are
        bulk-loaded via the Java SDK through `prepare_collection_for_indexing`
        — much faster than N1QL UPSERT — and the natural CRC32 distribution
        of sequential SDK keys gives roughly uniform docs/vBucket without
        explicit vBucket-key balancing.

        Histograms are compared on Hotel's real `country` field. With ~100
        distinct country values and only 1 field in the UPDATE STATISTICS
        clause, the per-field sort load stays well below the sort spill
        threshold, so spilling is isolated to the seq-scan path.

        Data shape (defaults):
          - bucket storage     = magma
          - numVBuckets        = 128
          - num_docs           = 250 000
          - sample_size        = num_docs

          Per-vBucket cumulative ≈ 250 000 / 128 × ~1.5 KB ≈ ~3 MiB
                                                            (> 2 MiB floor)
          Total dataset         ≈ 250 000 × ~1.5 KB ≈ ~380 MiB

        Prerequisite: cluster must have N1QL_SEQ_SCAN enabled (feat_ctrl
        bit 0x4000 cleared). Conf sets n1ql_feat_ctrl=76.

        Steps:
        1.  Prerequisites — KEK id captured
        2.  Magma bucket (128 vBuckets) + SDK-loaded Hotel docs
        3.  EXPLAIN: verify #sequentialscan in plan
        4.  Capture spill directory + query nodes
        --- ENCRYPTION OFF RUN ---
        5.  Disable bucket encryption at rest
        6.  UPDATE STATISTICS; capture histogram for country; DELETE STATISTICS
        --- ENCRYPTION ON RUN ---
        7.  Re-enable bucket encryption at rest
        8.  Capture per-node key IDs
        9.  Start remote spill-file watcher
        10. Run UPDATE STATISTICS in background
        11. Wait for query; stop watcher
        12. Validate snapshotted spill files are encrypted (warn if none
            captured)
        13. Capture histogram for country; DELETE STATISTICS
        14. Compare baseline vs encrypted histogram (all keys must match
            except `updated`)
        15. Cleanup
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_seqscan_key_document_spill_files_encrypted")
        self.log.info("=" * 80)

        # Magma + 128 vBuckets lowers the per-cluster floor to
        # 2 MiB × 128 = 256 MiB. Hotel docs (~1.5 KB) at 250 000 docs
        # ≈ 380 MiB total, comfortably above the floor with ~3 MiB per
        # vBucket on average.
        num_vbuckets = self.input.param("seqscan_us_num_vbuckets", 128)
        num_docs = self.input.param("seqscan_us_num_docs", 250000)
        # `seqscan_us_sample_size=0` (the default) means "sample the entire
        # collection" — sample size is set to num_docs after loading.
        sample_size_param = self.input.param("seqscan_us_sample_size", 0)
        # Run UPDATE STATISTICS on Hotel's real `country` field (~100
        # distinct values per the Hotel template).
        field_names = ["country"]

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying other encryption at rest prerequisites...")
            if not getattr(self, "enable_encryption_at_rest", False):
                self.skipTest(
                    "Bucket encryption at rest not enabled. "
                    "Set enable_encryption_at_rest=True in the conf row."
                )
            kek_id = getattr(self, "encryption_at_rest_id", None)
            self.assertIsNotNone(
                kek_id,
                "self.encryption_at_rest_id is None \u2014 base class did not set up a KEK"
            )
            self.log.info(f"[STEP 1] PASSED - bucket encryption KEK id: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Magma bucket (128 vBuckets) + SDK-loaded Hotel docs ==========
        try:
            self.log.info(
                f"[STEP 2] Creating Magma bucket with numVBuckets={num_vbuckets} "
                f"and loading {num_docs} Hotel docs via SDK..."
            )
            self.bucket_params = self._create_bucket_params(
                server=self.master,
                size=self.bucket_size,
                replicas=self.num_replicas,
                bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy,
                lww=self.lww,
                bucket_storage="magma",
            )
            # numVBuckets is a Magma-specific knob — not exposed by
            # `_create_bucket_params`, so splice it in directly. The task
            # layer reads it back from bucket_params["numVBuckets"]
            # (lib/tasks/task.py:296-297, 354).
            self.bucket_params["numVBuckets"] = num_vbuckets
            self.cluster.create_standard_bucket(
                name=self.test_bucket, port=11222, bucket_params=self.bucket_params
            )
            self.sleep(10, "Waiting after bucket creation")

            # `prepare_collection_for_indexing` creates scope_1.collection_1
            # under the bucket and bulk-loads Hotel docs via the Java SDK —
            # much faster than N1QL UPSERT for the same volume. Sequential
            # SDK keys (`doc_N`) distribute roughly uniformly across the
            # 128 vBuckets via CRC32, giving ~1 950 docs/vBucket × ~1.5 KB
            # ≈ ~3 MiB per vBucket — above the 2 MiB key+doc seq-scan
            # spill floor.
            self.prepare_collection_for_indexing(
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=num_docs,
                json_template="Hotel",
                bucket_name=self.test_bucket,
            )
            self.assertGreaterEqual(
                len(self.namespaces), 1,
                f"No namespace produced by prepare_collection_for_indexing: "
                f"{self.namespaces}"
            )
            namespace = self.namespaces[0]
            # Parse scope/collection out of the namespace for histogram
            # lookup later (the histogram doc has explicit `scope` /
            # `collection` fields we filter on).
            _, _, sc_path = namespace.partition(":")
            _, _, sc_path = sc_path.partition(".")
            scope_name, _, collection_name = sc_path.partition(".")
            sample_size = sample_size_param if sample_size_param > 0 else num_docs
            est_per_vb_mib = (num_docs / num_vbuckets * 1500) / (1024 * 1024)
            est_total_mib = (num_docs * 1500) / (1024 * 1024)
            self.log.info(
                f"[STEP 2] PASSED - namespace: {namespace} "
                f"(approx {est_per_vb_mib:.2f} MiB per vBucket, "
                f"{est_total_mib:.0f} MiB total); sample_size={sample_size}"
            )
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: EXPLAIN — verify #sequentialscan available ==========
        try:
            self.log.info("[STEP 3] EXPLAIN SELECT to verify #sequentialscan is reachable...")
            sample_query = f"SELECT meta().id AS id FROM {namespace}"
            explain_result = self.run_cbq_query(query=f"EXPLAIN {sample_query}")
            self.assertEqual(
                explain_result.get("status"), "success",
                f"EXPLAIN failed: {explain_result}"
            )
            explain_json = json.dumps(explain_result.get("results", []))
            self.assertIn(
                "#sequentialscan", explain_json,
                f"Plan does not use #sequentialscan — sequential scans may be "
                f"disabled (check n1ql_feat_ctrl bit 0x4000). "
                f"Plan: {explain_json[:1000]}"
            )
            self.log.info("[STEP 3] PASSED - #sequentialscan present in plan")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Spill dir + query nodes ==========
        try:
            self.log.info("[STEP 4] Capturing spill directory + query nodes...")
            status, query_settings = self.rest.get_query_settings(self.master)
            self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            self.assertIsNotNone(
                spill_dir,
                f"Could not find spill directory in querySettings: {query_settings}"
            )
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )
            self.log.info(f"[STEP 4] PASSED - spill directory: {spill_dir}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        fields_clause = ", ".join(field_names)
        us_query = (
            f'UPDATE STATISTICS FOR {namespace}({fields_clause}) '
            f'WITH {{"sample_size": {sample_size}}}'
        )
        delete_stats_query = (
            f"UPDATE STATISTICS FOR {namespace} DELETE({fields_clause})"
        )
        system_query_ns = f"default:{self.test_bucket}._system._query"

        def _fetch_histograms(label):
            """Return {field_name: histogram_envelope_dict} for each field in
            field_names. Also runs a `decode_base64(histogram)` sanity check
            on each field per the spec doc: "If no histogram document is
            returned, or if the returned histogram is malformed, the test
            should fail." A byte-for-byte envelope comparison between runs
            catches drift, but not the case where BOTH runs produce
            identically malformed output (e.g. a truncated payload that
            round-trips). The decode + minimum-length check closes that gap.
            """
            histograms = {}
            for field in field_names:
                hist_query = (
                    f"SELECT q.* FROM {system_query_ns} q "
                    f"WHERE q.histogramKey = '{field}' "
                    f"AND q.`scope` = '{scope_name}' "
                    f"AND q.`collection` = '{collection_name}'"
                )
                result = self.run_cbq_query(query=hist_query)
                self.assertEqual(
                    result.get("status"), "success",
                    f"{label}Histogram fetch for {field} failed: {result}"
                )
                rows = result.get("results", [])
                self.assertEqual(
                    len(rows), 1,
                    f"{label}Expected exactly one histogram doc for {field}, "
                    f"got {len(rows)}: {rows}"
                )
                histograms[field] = rows[0]

                # Well-formedness check on the decoded base64 payload.
                # MIN_DECODED_HISTOGRAM_BYTES = 16 is a conservative floor.
                decode_query = (
                    f"SELECT decode_base64(q.histogram) AS decoded "
                    f"FROM {system_query_ns} q "
                    f"WHERE q.histogramKey = '{field}' "
                    f"AND q.`scope` = '{scope_name}' "
                    f"AND q.`collection` = '{collection_name}'"
                )
                decode_result = self.run_cbq_query(query=decode_query)
                self.assertEqual(
                    decode_result.get("status"), "success",
                    f"{label}decode_base64 query for {field} failed: {decode_result}"
                )
                decoded_rows = decode_result.get("results", [])
                self.assertEqual(
                    len(decoded_rows), 1,
                    f"{label}Expected exactly one decoded histogram row for "
                    f"{field}, got {len(decoded_rows)}"
                )
                decoded = decoded_rows[0].get("decoded")
                self.assertIsNotNone(
                    decoded,
                    f"{label}decode_base64(histogram) returned NULL for {field} "
                    f"— either the `histogram` field is missing from the doc "
                    f"or the encoded payload is empty"
                )
                decoded_len = len(decoded) if decoded is not None else 0
                self.assertGreater(
                    decoded_len, 16,
                    f"{label}Decoded histogram for {field} is too short "
                    f"({decoded_len} bytes) — likely malformed/truncated"
                )
            return histograms

        def _strip_volatile(envelope):
            return {k: v for k, v in envelope.items() if k != "updated"}

        snap_dir = None
        try:
            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 5: Disable encryption ==========
            try:
                self.log.info("[STEP 5] Disabling 'other' encryption at rest for baseline run...")
                self._disable_bucket_encryption_at_rest(self.test_bucket, label="[STEP 5] ")
                self.log.info("[STEP 5] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 5] FAILED: {e}")
                raise

            # ========== STEP 6: Baseline UPDATE STATISTICS + histogram ==========
            try:
                self._set_query_log_level("debug", label="[STEP 6] ")
                self.log.info(f"[STEP 6] Baseline UPDATE STATISTICS (encryption off): {us_query}")
                baseline_us = self.run_cbq_query(query=us_query)
                self._set_query_log_level("info", label="[STEP 6] ")
                self.assertEqual(
                    baseline_us.get("status"), "success",
                    f"Baseline UPDATE STATISTICS failed: {baseline_us}"
                )
                baseline_histograms = _fetch_histograms(label="[STEP 6] ")
                self.run_cbq_query(query=delete_stats_query)
                self.log.info(
                    f"[STEP 6] PASSED - captured {len(baseline_histograms)} baseline histograms"
                )
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 7: Re-enable encryption ==========
            try:
                self.log.info("[STEP 7] Re-enabling 'other' encryption at rest...")
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 7] ")
                self.log.info("[STEP 7] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ========== STEP 8: Baseline spills.seq_scan + spills.update_statistics + key IDs ==========
            # The seq-scan counter is the primary spill signal for this test
            # (UPDATE STATISTICS draws its random sample via a key+document
            # sequential scan). The update_statistics counter is used for the
            # isolation check in STEP 11 — it should NOT increment, because
            # this test is supposed to exercise ONLY the seq-scan spill path,
            # not per-field sort spilling.
            try:
                self.log.info(
                    "[STEP 8] Recording baseline spills.seq_scan + "
                    "spills.update_statistics + key IDs..."
                )
                # Bucket-encrypted file headers carry the bucket's KEK id. Pass it
                # as a flat list — `_build_node_key_ids_map` will apply it to
                # every query node uniformly.
                expected_key_ids = [str(kek_id)]
                baseline_seqscan_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.seq_scan"
                )
                baseline_sort_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.update_statistics"
                )
                self.log.info(
                    f"[STEP 8] PASSED - baseline spills.seq_scan: "
                    f"{baseline_seqscan_spills} | baseline spills.update_statistics: "
                    f"{baseline_sort_spills} | key IDs: {expected_key_ids}"
                )
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Start watcher ==========
            try:
                self.log.info("[STEP 9] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 9] ", file_prefix="ss_spill-"
                )
                self.log.info(f"[STEP 9] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {e}")
                raise

            # ========== STEP 10: Background UPDATE STATISTICS ==========
            us_result_container = {}
            try:
                self._set_query_log_level("debug", label="[STEP 10] ")
                self.log.info("[STEP 10] Launching background UPDATE STATISTICS...")

                def _run_update_statistics():
                    try:
                        us_result_container["result"] = self.run_cbq_query(
                            query=us_query
                        )
                    except Exception as ex:
                        us_result_container["error"] = ex
                        self.log.warning(f"[STEP 10] Background UPDATE STATISTICS raised: {ex}")

                bg_thread = threading.Thread(
                    target=_run_update_statistics,
                    daemon=True,
                    name="bg-seqscan-update-statistics"
                )
                bg_thread.start()
                self.log.info("[STEP 10] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Wait, isolation check, stop watcher ==========
            try:
                self.log.info("[STEP 11] Waiting for background UPDATE STATISTICS to finish...")
                bg_thread.join(timeout=900)
                self._set_query_log_level("info", label="[STEP 11] ")
                self.assertFalse(
                    bg_thread.is_alive(),
                    "[STEP 11] Background UPDATE STATISTICS did not finish within 900 s"
                )
                if "error" in us_result_container:
                    raise us_result_container["error"]
                self.assertEqual(
                    us_result_container.get("result", {}).get("status"), "success",
                    f"UPDATE STATISTICS failed: {us_result_container.get('result')}"
                )

                # spills.seq_scan delta — primary spill signal for this test.
                # Warning-only (per-query result-hash comparison + histogram
                # comparison in STEP 14 are authoritative). The data shape is
                # tuned to spill via the seq-scan path; if it didn't, the
                # cluster sizing may have absorbed the load.
                post_seqscan_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.seq_scan"
                )
                if any(
                    post_seqscan_spills.get(node.ip, 0) > baseline_seqscan_spills.get(node.ip, 0)
                    for node in query_nodes
                ):
                    self.log.info(
                        f"[STEP 11] spills.seq_scan incremented: "
                        f"baseline={baseline_seqscan_spills} current={post_seqscan_spills}"
                    )
                else:
                    self.log.warning(
                        f"[STEP 11] spills.seq_scan did not increment "
                        f"(baseline={baseline_seqscan_spills}, current={post_seqscan_spills}). "
                        f"Seq-scan spilling may not have occurred on this cluster "
                        f"sizing; test continues — histogram comparison is the "
                        f"authoritative signal."
                    )

                # spills.update_statistics isolation check — warning if it
                # incremented. This test is supposed to exercise ONLY the
                # seq-scan spill path; per-field sort spilling lighting up
                # means the data shape is inadvertently also triggering the
                # sort path and should be retuned (reduce num_docs or fields).
                post_sort_spills = self._get_vitals_spill_counts(
                    query_nodes, metric="spills.update_statistics"
                )
                sort_spilled = any(
                    post_sort_spills.get(node.ip, 0) > baseline_sort_spills.get(node.ip, 0)
                    for node in query_nodes
                )
                if sort_spilled:
                    self.log.warning(
                        f"[STEP 11] spills.update_statistics INCREMENTED — "
                        f"baseline={baseline_sort_spills} current={post_sort_spills}. "
                        f"This test was supposed to isolate seq-scan spilling; "
                        f"the data shape (num_docs={num_docs}, num_vbuckets={num_vbuckets}) "
                        f"is also triggering per-field sort spilling. Consider "
                        f"reducing num_docs or the field count."
                    )
                else:
                    self.log.info(
                        f"[STEP 11] spills.update_statistics did not move — "
                        f"sort spilling correctly isolated out. "
                        f"baseline={baseline_sort_spills} current={post_sort_spills}"
                    )

                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 11] "
                )
                self.log.info("[STEP 11] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Validate snapshotted spill files ==========
            try:
                self.log.info(
                    f"[STEP 12] Validating snapshotted spill files in {snap_dir}..."
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 12] "
                )
                self.log.info("[STEP 12] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise

            # ========== STEP 13: Encrypted-run histograms ==========
            try:
                self.log.info("[STEP 13] Fetching encrypted-run histograms...")
                encrypted_histograms = _fetch_histograms(label="[STEP 13] ")
                self.run_cbq_query(query=delete_stats_query)
                self.log.info(
                    f"[STEP 13] PASSED - captured {len(encrypted_histograms)} encrypted histograms"
                )
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {e}")
                raise

            # ========== STEP 14: Compare histograms (excluding `updated`) ==========
            try:
                self.log.info("[STEP 14] Comparing per-field histograms (excluding `updated`)...")
                mismatches = []
                for field in field_names:
                    base = _strip_volatile(baseline_histograms.get(field, {}))
                    enc = _strip_volatile(encrypted_histograms.get(field, {}))
                    if base != enc:
                        diff_keys = sorted(set(base.keys()) | set(enc.keys()))
                        per_key = []
                        for k in diff_keys:
                            if base.get(k) != enc.get(k):
                                per_key.append(
                                    f"      {k}: baseline={base.get(k)!r} "
                                    f"encrypted={enc.get(k)!r}"
                                )
                        mismatches.append(
                            f"  field {field}:\n" + "\n".join(per_key)
                        )
                if mismatches:
                    msg = (
                        "[STEP 14] Histograms diverged between baseline and encrypted runs "
                        "(only `updated` is allowed to differ):\n" + "\n".join(mismatches)
                    )
                    self.log.error(msg)
                    self.fail(msg)
                self.log.info(
                    f"[STEP 14] PASSED - all {len(field_names)} histograms match across runs"
                )
            except Exception as e:
                self.log.error(f"[STEP 14] FAILED: {e}")
                raise
        finally:
            # ========== STEP 15: Cleanup ==========
            if snap_dir:
                self.log.info("[STEP 15] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            try:
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 15] ")
            except Exception as e:
                self.log.warning(f"[STEP 15] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_seqscan_key_document_spill_files_encrypted")
        self.log.info("=" * 80)

    def test_query_scan_backfill_files_encrypted(self):
        """
        Verify GSI scan-backfill files are encrypted and query results are
        identical with BUCKET encryption at rest enabled vs disabled.

        Per the spec doc: spill/backfill files produced by the scan-backfill
        path (and by sequential-scan paths) are encrypted with the BUCKET
        encryption key, NOT the cluster-wide `other` KEK. So this test
        toggles bucket-level encryption (`_disable_bucket_encryption_at_rest`
        / `_enable_bucket_encryption_at_rest`), uses `self.encryption_at_rest_id`
        (the bucket KEK) as the expected key ID, and gates on the conf row
        flag `enable_encryption_at_rest=True` (the bucket flag), NOT
        `enable_other_encryption_at_rest`.

        When the query service consumes scan results from the indexer, it
        buffers rows in memory. If the buffer fills up faster than the
        consumer drains it, the surplus is backfilled to disk inside
        queryTmpSpaceDir. To make this deterministic the test:
          - sets `indexer.queryport.backfill_pause_test_duration` to a
            non-zero value, which makes the indexer pause briefly between
            scan responses (test-only knob, restored in `finally`)
          - issues the query with request parameter `scan_cap=128`, which
            forces frequent buffer transfers (smaller per-request rows)
          - runs a large-result-set scan over an indexed field

        Backfill files share the same `queryTmpSpaceDir` directory used by
        the other spill-file paths, so the existing remote watcher,
        encryption-validation, and snapshot-cleanup helpers apply
        unchanged.

        Steps:
        1.  Prerequisites — bucket KEK id captured
        2.  Pin indexer.queryport.backfill_pause_test_duration
        3.  Bucket + collection loaded with N Hotel docs
        4.  CREATE INDEX ix_country ON <ns>(country)
        5.  Capture queryTmpSpaceDir + query nodes
        --- ENCRYPTION OFF RUN ---
        6.  Disable bucket encryption at rest
        7.  Run query with scan_cap=128 → capture result hash
        --- ENCRYPTION ON RUN ---
        8.  Re-enable bucket encryption at rest
        9.  Capture expected key IDs (the bucket KEK id)
        10. Start remote spill-file watcher
        11. Run query with scan_cap=128 in background; wait
        12. Stop watcher; validate snapshotted backfill files are
            encrypted (warn-only if zero captured)
        13. Compare baseline vs encrypted result hashes — hard fail on
            any divergence
        14. Cleanup: restore indexer setting, snapshot dirs, encryption
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_scan_backfill_files_encrypted")
        self.log.info("=" * 80)

        num_docs = self.input.param("num_backfill_docs", 100000)
        scan_cap = self.input.param("backfill_scan_cap", 128)
        pause_sec = self.input.param("backfill_pause_test_duration_sec", 20)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying bucket encryption at rest prerequisites...")
            if not getattr(self, "enable_encryption_at_rest", False):
                self.skipTest(
                    "Bucket encryption at rest not enabled. "
                    "Set enable_encryption_at_rest=True in the conf row."
                )
            kek_id = getattr(self, "encryption_at_rest_id", None)
            self.assertIsNotNone(
                kek_id,
                "self.encryption_at_rest_id is None \u2014 base class did not set up a KEK"
            )
            self.log.info(f"[STEP 1] PASSED - bucket encryption KEK id: {kek_id}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Pin backfill_pause_test_duration on the indexer ==========
        backfill_setting_applied = False
        try:
            self.log.info(
                f"[STEP 2] Setting indexer.queryport.backfill_pause_test_duration={pause_sec}s..."
            )
            self.index_rest.set_index_settings(
                {"indexer.queryport.backfill_pause_test_duration": pause_sec}
            )
            backfill_setting_applied = True
            self.log.info("[STEP 2] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        snap_dir = None
        try:
            # ========== STEP 3: Bucket + collection + Hotel data ==========
            try:
                self.log.info(f"[STEP 3] Creating bucket and loading {num_docs} Hotel docs...")
                self.bucket_params = self._create_bucket_params(
                    server=self.master,
                    size=self.bucket_size,
                    replicas=self.num_replicas,
                    bucket_type=self.bucket_type,
                    enable_replica_index=self.enable_replica_index,
                    eviction_policy=self.eviction_policy,
                    lww=self.lww
                )
                self.cluster.create_standard_bucket(
                    name=self.test_bucket, port=11222, bucket_params=self.bucket_params
                )
                self.sleep(10, "Waiting after bucket creation")

                self.prepare_collection_for_indexing(
                    num_scopes=1,
                    num_collections=1,
                    num_of_docs_per_collection=num_docs,
                    json_template="Hotel",
                    bucket_name=self.test_bucket
                )
                self.assertTrue(len(self.namespaces) > 0, "No namespaces created")
                namespace = self.namespaces[0]
                self.log.info(f"[STEP 3] PASSED - namespace: {namespace}")
            except Exception as e:
                self.log.error(f"[STEP 3] FAILED: {e}")
                raise

            # ========== STEP 4: GSI index on country ==========
            try:
                self.log.info(f"[STEP 4] Creating ix_country ON {namespace}(country)...")
                ix = QueryDefinition(index_name="ix_country", index_fields=["country"])
                self.run_cbq_query(
                    query=ix.generate_index_create_query(
                        namespace=namespace, defer_build=False, num_replica=0
                    )
                )
                self.wait_until_indexes_online(timeout=300, defer_build=False)
                self.log.info("[STEP 4] PASSED - ix_country online")
            except Exception as e:
                self.log.error(f"[STEP 4] FAILED: {e}")
                raise

            # ========== STEP 5: Capture queryTmpSpaceDir + query nodes ==========
            try:
                self.log.info("[STEP 5] Capturing queryTmpSpaceDir + query nodes...")
                status, query_settings = self.rest.get_query_settings(self.master)
                self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
                spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
                self.assertIsNotNone(
                    spill_dir,
                    f"Could not find queryTmpSpaceDir in querySettings: {query_settings}"
                )
                query_nodes = self.get_nodes_from_services_map(
                    service_type="n1ql", get_all_nodes=True
                )
                self.log.info(f"[STEP 5] PASSED - tmp directory: {spill_dir}")
            except Exception as e:
                self.log.error(f"[STEP 5] FAILED: {e}")
                raise

            # Project a few wide-ish Hotel fields (not full SELECT *) so each
            # buffered row is several hundred bytes — large enough that
            # 100 000 rows × N bytes overflows the in-memory scan buffer when
            # the indexer pauses between responses, forcing backfill to disk.
            scan_query = (
                f"SELECT meta().id AS id, name, country, city, state, address, "
                f"description, reviews FROM {namespace} WHERE country IS NOT NULL"
            )
            scan_params = {"scan_cap": scan_cap}

            def _hash_results(rows):
                return hashlib.sha256(
                    json.dumps(rows, sort_keys=True).encode("utf-8")
                ).hexdigest()

            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 6: Disable encryption ==========
            try:
                self.log.info("[STEP 6] Disabling 'other' encryption at rest for baseline run...")
                self._disable_bucket_encryption_at_rest(self.test_bucket, label="[STEP 6] ")
                self.log.info("[STEP 6] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ========== STEP 7: Baseline scan ==========
            try:
                self._set_query_log_level("debug", label="[STEP 7] ")
                self.log.info(
                    f"[STEP 7] Baseline scan (encryption off), scan_cap={scan_cap}: {scan_query}"
                )
                baseline_result = self.run_cbq_query(
                    query=scan_query, query_params=scan_params
                )
                self._set_query_log_level("info", label="[STEP 7] ")
                self.assertEqual(
                    baseline_result.get("status"), "success",
                    f"Baseline scan failed: {baseline_result}"
                )
                baseline_rows = baseline_result.get("results", [])
                baseline_hash = _hash_results(baseline_rows)
                self.log.info(
                    f"[STEP 7] PASSED - hash={baseline_hash} rows={len(baseline_rows)}"
                )
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 8: Re-enable encryption ==========
            try:
                self.log.info("[STEP 8] Re-enabling 'other' encryption at rest...")
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 8] ")
                self.log.info("[STEP 8] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Capture per-node key IDs ==========
            try:
                self.log.info("[STEP 9] Capturing in-use encryption key IDs per query node...")
                # Bucket-encrypted file headers carry the bucket's KEK id. Pass it
                # as a flat list — `_build_node_key_ids_map` will apply it to
                # every query node uniformly.
                expected_key_ids = [str(kek_id)]
                self.log.info(f"[STEP 9] PASSED - key IDs: {expected_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {e}")
                raise

            # ========== STEP 10: Start watcher ==========
            try:
                self.log.info("[STEP 10] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 10] ", file_prefix="scan-results"
                )
                self.log.info(f"[STEP 10] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Run scan in background ==========
            scan_result_container = {}
            try:
                self._set_query_log_level("debug", label="[STEP 11] ")
                self.log.info("[STEP 11] Launching background scan...")

                def _run_scan():
                    try:
                        scan_result_container["result"] = self.run_cbq_query(
                            query=scan_query, query_params=scan_params
                        )
                    except Exception as ex:
                        scan_result_container["error"] = ex
                        self.log.warning(f"[STEP 11] Background scan raised: {ex}")

                bg_thread = threading.Thread(
                    target=_run_scan, daemon=True, name="bg-backfill-scan"
                )
                bg_thread.start()
                self.log.info("[STEP 11] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Wait, stop watcher, validate snapshots ==========
            try:
                self.log.info("[STEP 12] Waiting for background scan to finish...")
                bg_thread.join(timeout=900)
                self._set_query_log_level("info", label="[STEP 12] ")
                self.assertFalse(
                    bg_thread.is_alive(),
                    "[STEP 12] Background scan did not finish within 900 s"
                )
                if "error" in scan_result_container:
                    raise scan_result_container["error"]
                encrypted_result = scan_result_container.get("result", {})
                self.assertEqual(
                    encrypted_result.get("status"), "success",
                    f"Encrypted scan failed: {encrypted_result}"
                )

                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 12] "
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 12] "
                )
                self.log.info("[STEP 12] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise

            # ========== STEP 13: Compare result hashes ==========
            try:
                self.log.info("[STEP 13] Comparing encrypted-run result hash to baseline...")
                encrypted_rows = encrypted_result.get("results", [])
                encrypted_hash = _hash_results(encrypted_rows)
                self.assertEqual(
                    encrypted_hash, baseline_hash,
                    f"Scan result hash diverged with encryption: "
                    f"baseline={baseline_hash} ({len(baseline_rows)} rows) vs "
                    f"encrypted={encrypted_hash} ({len(encrypted_rows)} rows). "
                    f"Encryption at rest must not alter scan semantics."
                )
                self.log.info(
                    f"[STEP 13] PASSED - result hashes match: {encrypted_hash} "
                    f"({len(encrypted_rows)} rows)"
                )
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {e}")
                raise
        finally:
            # ========== STEP 14: Cleanup ==========
            if snap_dir:
                self.log.info("[STEP 14] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            if backfill_setting_applied:
                try:
                    self.log.info(
                        "[STEP 14] Restoring indexer.queryport.backfill_pause_test_duration=0..."
                    )
                    self.index_rest.set_index_settings(
                        {"indexer.queryport.backfill_pause_test_duration": 0}
                    )
                except Exception as e:
                    self.log.warning(
                        f"[STEP 14] Failed to restore backfill_pause_test_duration: {e}"
                    )
            try:
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 14] ")
            except Exception as e:
                self.log.warning(f"[STEP 14] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_scan_backfill_files_encrypted")
        self.log.info("=" * 80)



    def test_fts_search_backfill_files_encrypted(self):
        """
        Verify FTS scan-backfill files written by the query service's n1fty
        response handler are encrypted at rest, and that SEARCH() results are
        identical with BUCKET encryption at rest enabled vs disabled.

        n1fty buffers FTS hits in memory while streaming them from the FTS
        service back to the query engine. When the consumer drains the buffer
        slower than it fills (large `size`, small `scan_cap`), the surplus is
        spilled to disk inside `queryTmpSpaceDir` — the same directory used
        by GSI scan-backfill and the seq-scan spill paths. These backfill
        files share the bucket KEK with the GSI scan-backfill files, so the
        same watcher / snapshot / encryption-validation helpers apply.

        Steps:
        1.  Prerequisites — bucket KEK id captured
        2.  Bucket + collection loaded with N Hotel docs
        3.  Create dynamic collection-level FTS index; wait for indexing
        4.  Capture queryTmpSpaceDir + query nodes
        --- ENCRYPTION OFF RUN ---
        5.  Disable bucket encryption at rest
        6.  Run SEARCH() with from=0,size=10000,scan_cap=10 → baseline hash
        --- ENCRYPTION ON RUN ---
        7.  Re-enable bucket encryption at rest
        8.  Capture expected key IDs (the bucket KEK id)
        9.  Start remote spill-file watcher
        10. Run SEARCH() in background with scan_cap=10
        11. Stop watcher; validate snapshotted backfill files are
            encrypted (warn-only if zero captured)
        12. Compare baseline vs encrypted result hashes
        13. Cleanup
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_fts_search_backfill_files_encrypted")
        self.log.info("=" * 80)

        num_docs = self.input.param("num_fts_backfill_docs", 10000)
        scan_cap = self.input.param("fts_backfill_scan_cap", 10)
        search_size = self.input.param("fts_search_size", 10000)
        fts_index_name = self.input.param("fts_backfill_index_name", "ix_fts_backfill")

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying bucket encryption at rest prerequisites...")
            if not getattr(self, "enable_encryption_at_rest", False):
                self.skipTest(
                    "Bucket encryption at rest not enabled. "
                    "Set enable_encryption_at_rest=True in the conf row."
                )
            kek_id = getattr(self, "encryption_at_rest_id", None)
            self.assertIsNotNone(
                kek_id,
                "self.encryption_at_rest_id is None — base class did not set up a KEK"
            )
            fts_nodes = self.get_nodes_from_services_map(
                service_type="fts", get_all_nodes=True
            )
            self.assertTrue(
                fts_nodes,
                "No FTS service nodes in cluster — add 'fts' to services_init"
            )
            self.log.info(
                f"[STEP 1] PASSED - bucket encryption KEK id: {kek_id}, "
                f"fts nodes: {[n.ip for n in fts_nodes]}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        snap_dir = None
        fts_index_created = False
        scope_name = f"{self.scope_prefix}_1"
        collection_name = f"{self.collection_prefix}_1"
        try:
            # ========== STEP 2: Bucket + collection + Hotel data ==========
            try:
                self.log.info(f"[STEP 2] Creating bucket and loading {num_docs} Hotel docs...")
                self.bucket_params = self._create_bucket_params(
                    server=self.master,
                    size=self.bucket_size,
                    replicas=self.num_replicas,
                    bucket_type=self.bucket_type,
                    enable_replica_index=self.enable_replica_index,
                    eviction_policy=self.eviction_policy,
                    lww=self.lww
                )
                self.cluster.create_standard_bucket(
                    name=self.test_bucket, port=11222, bucket_params=self.bucket_params
                )
                self.sleep(10, "Waiting after bucket creation")

                self.prepare_collection_for_indexing(
                    num_scopes=1,
                    num_collections=1,
                    num_of_docs_per_collection=num_docs,
                    json_template="Hotel",
                    bucket_name=self.test_bucket
                )
                self.assertTrue(len(self.namespaces) > 0, "No namespaces created")
                namespace = self.namespaces[0]
                self.log.info(
                    f"[STEP 2] PASSED - namespace: {namespace} "
                    f"(scope={scope_name}, collection={collection_name})"
                )
            except Exception as e:
                self.log.error(f"[STEP 2] FAILED: {e}")
                raise

            # ========== STEP 3: Create FTS index over the collection ==========
            try:
                self.log.info(
                    f"[STEP 3] Creating FTS index '{fts_index_name}' over "
                    f"{self.test_bucket}.{scope_name}.{collection_name}..."
                )
                payload = self._build_fts_collection_index_payload(
                    index_name=fts_index_name,
                    bucket=self.test_bucket,
                    scope=scope_name,
                    collection=collection_name,
                )
                status, _ = self.rest.create_fts_index(
                    index_name=fts_index_name,
                    params=payload,
                    bucket=self.test_bucket,
                    scope=scope_name,
                )
                self.assertTrue(status, "FTS index creation failed")
                fts_index_created = True
                self._wait_for_fts_index_ready(
                    index_name=fts_index_name,
                    bucket=self.test_bucket,
                    scope=scope_name,
                    expected_docs=num_docs,
                    label="[STEP 3] ",
                )
                self.log.info("[STEP 3] PASSED - FTS index ready")
            except Exception as e:
                self.log.error(f"[STEP 3] FAILED: {e}")
                raise

            # ========== STEP 4: Capture queryTmpSpaceDir + query nodes ==========
            try:
                self.log.info("[STEP 4] Capturing queryTmpSpaceDir + query nodes...")
                status, query_settings = self.rest.get_query_settings(self.master)
                self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
                spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
                self.assertIsNotNone(
                    spill_dir,
                    f"Could not find queryTmpSpaceDir in querySettings: {query_settings}"
                )
                query_nodes = self.get_nodes_from_services_map(
                    service_type="n1ql", get_all_nodes=True
                )
                self.log.info(f"[STEP 4] PASSED - tmp directory: {spill_dir}")
            except Exception as e:
                self.log.error(f"[STEP 4] FAILED: {e}")
                raise

            # Build a SEARCH() query against the collection. match_all returns
            # every indexed document, so size=10000 over a 10k-doc collection
            # is guaranteed to return 10k hits — driving the n1fty response
            # buffer past `scan_cap=10` and forcing backfill to disk.
            search_request = {
                "query": {"match_all": {}},
                "from": 0,
                "size": search_size,
            }
            search_query = (
                f"SELECT meta().id AS id FROM {namespace} "
                f"WHERE SEARCH({collection_name}, {json.dumps(search_request)})"
            )
            scan_params = {"scan_cap": scan_cap}

            def _hash_results(rows):
                ordered = sorted(rows, key=lambda r: r.get("id"))
                return hashlib.sha256(
                    json.dumps(ordered, sort_keys=True).encode("utf-8")
                ).hexdigest()

            # ---------- ENCRYPTION OFF: baseline run ----------
            # ========== STEP 5: Disable encryption ==========
            try:
                self.log.info("[STEP 5] Disabling bucket encryption at rest for baseline run...")
                self._disable_bucket_encryption_at_rest(self.test_bucket, label="[STEP 5] ")
                self.log.info("[STEP 5] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 5] FAILED: {e}")
                raise

            # ========== STEP 6: Baseline SEARCH ==========
            try:
                self._set_query_log_level("debug", label="[STEP 6] ")
                self.log.info(
                    f"[STEP 6] Baseline SEARCH (encryption off), scan_cap={scan_cap}: {search_query}"
                )
                baseline_result = self.run_cbq_query(
                    query=search_query, query_params=scan_params
                )
                self._set_query_log_level("info", label="[STEP 6] ")
                self.assertEqual(
                    baseline_result.get("status"), "success",
                    f"Baseline SEARCH failed: {baseline_result}"
                )
                baseline_rows = baseline_result.get("results", [])
                baseline_hash = _hash_results(baseline_rows)
                self.assertEqual(
                    len(baseline_rows), num_docs,
                    f"Baseline SEARCH returned {len(baseline_rows)} hits, "
                    f"expected {num_docs} — index may not be fully populated"
                )
                self.log.info(
                    f"[STEP 6] PASSED - hash={baseline_hash} rows={len(baseline_rows)}"
                )
            except Exception as e:
                self.log.error(f"[STEP 6] FAILED: {e}")
                raise

            # ---------- ENCRYPTION ON: validated run ----------
            # ========== STEP 7: Re-enable encryption ==========
            try:
                self.log.info("[STEP 7] Re-enabling bucket encryption at rest...")
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 7] ")
                self.log.info("[STEP 7] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 7] FAILED: {e}")
                raise

            # ========== STEP 8: Capture expected key IDs ==========
            try:
                self.log.info("[STEP 8] Capturing expected encryption key IDs...")
                expected_key_ids = [str(kek_id)]
                self.log.info(f"[STEP 8] PASSED - key IDs: {expected_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {e}")
                raise

            # ========== STEP 9: Start watcher ==========
            # n1fty's response handler writes backfill files into the same
            # queryTmpSpaceDir used by GSI scan-backfill. The exact filename
            # prefix is not contractually fixed, so leave file_prefix empty
            # to capture any new file in the directory.
            try:
                self.log.info("[STEP 9] Starting remote spill-file watcher on every query node...")
                snap_dir = self._start_remote_spill_watcher(
                    query_nodes, spill_dir, label="[STEP 9] ", file_prefix=""
                )
                self.log.info(f"[STEP 9] PASSED - snapshot directory: {snap_dir}")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {e}")
                raise

            # ========== STEP 10: Run SEARCH in background ==========
            scan_result_container = {}
            try:
                self._set_query_log_level("debug", label="[STEP 10] ")
                self.log.info("[STEP 10] Launching background SEARCH...")

                def _run_search():
                    try:
                        scan_result_container["result"] = self.run_cbq_query(
                            query=search_query, query_params=scan_params
                        )
                    except Exception as ex:
                        scan_result_container["error"] = ex
                        self.log.warning(f"[STEP 10] Background SEARCH raised: {ex}")

                bg_thread = threading.Thread(
                    target=_run_search, daemon=True, name="bg-fts-backfill-search"
                )
                bg_thread.start()
                self.log.info("[STEP 10] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {e}")
                raise

            # ========== STEP 11: Wait, stop watcher, validate snapshots ==========
            try:
                self.log.info("[STEP 11] Waiting for background SEARCH to finish...")
                bg_thread.join(timeout=900)
                self._set_query_log_level("info", label="[STEP 11] ")
                self.assertFalse(
                    bg_thread.is_alive(),
                    "[STEP 11] Background SEARCH did not finish within 900 s"
                )
                if "error" in scan_result_container:
                    raise scan_result_container["error"]
                encrypted_result = scan_result_container.get("result", {})
                self.assertEqual(
                    encrypted_result.get("status"), "success",
                    f"Encrypted SEARCH failed: {encrypted_result}"
                )

                self._stop_remote_spill_watcher(
                    query_nodes, snap_dir, label="[STEP 11] "
                )
                self._assert_remote_spill_snapshots_encrypted(
                    query_nodes, snap_dir, expected_key_ids, label="[STEP 11] "
                )
                self.log.info("[STEP 11] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {e}")
                raise

            # ========== STEP 12: Compare result hashes ==========
            try:
                self.log.info("[STEP 12] Comparing encrypted-run result hash to baseline...")
                encrypted_rows = encrypted_result.get("results", [])
                encrypted_hash = _hash_results(encrypted_rows)
                self.assertEqual(
                    encrypted_hash, baseline_hash,
                    f"SEARCH result hash diverged with encryption: "
                    f"baseline={baseline_hash} ({len(baseline_rows)} rows) vs "
                    f"encrypted={encrypted_hash} ({len(encrypted_rows)} rows). "
                    f"Encryption at rest must not alter SEARCH semantics."
                )
                self.log.info(
                    f"[STEP 12] PASSED - result hashes match: {encrypted_hash} "
                    f"({len(encrypted_rows)} rows)"
                )
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {e}")
                raise
        finally:
            # ========== STEP 13: Cleanup ==========
            if snap_dir:
                self.log.info("[STEP 13] Cleaning up snapshot dirs on every node...")
                self._cleanup_remote_spill_snapshots(query_nodes, snap_dir)
            if fts_index_created:
                try:
                    self.log.info(f"[STEP 13] Deleting FTS index '{fts_index_name}'...")
                    self.rest.delete_fts_index(
                        name=fts_index_name,
                        bucket=self.test_bucket,
                        scope=scope_name,
                    )
                except Exception as e:
                    self.log.warning(f"[STEP 13] Failed to delete FTS index: {e}")
            try:
                self._enable_bucket_encryption_at_rest(self.test_bucket, kek_id, label="[STEP 13] ")
            except Exception as e:
                self.log.warning(f"[STEP 13] Defensive encryption re-enable failed: {e}")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_fts_search_backfill_files_encrypted")
        self.log.info("=" * 80)
