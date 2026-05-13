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

import concurrent.futures
import itertools
import json
import os
import random
import string
import threading
import time

from couchbase_helper.query_definitions import QueryDefinition
from pytests.gsi.base_gsi import BaseSecondaryIndexingTests
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
            shell = RemoteMachineShellConnection(node)
            shell.get_file(remote_dir, filename, local_dest)
            shell.disconnect()
            return local_dest
        except Exception as e:
            self.log.warning(f"Could not copy {remote_filepath} from {node.ip}: {e}")
            return None

    def _install_tools(self):
        """
        Ensure xxd is installed on every cluster node. Called once from suite_setUp.

        xxd ships in vim-common on Debian/Ubuntu and in vim-common (or vim-enhanced)
        on RHEL/CentOS/Fedora. The check is skipped for nodes where xxd is already
        present so repeated suite runs are fast.
        """
        for node in self.servers:
            shell = RemoteMachineShellConnection(node)
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

    def _verify_log_encryption_prerequisites(self):
        """
        Verify log encryption at rest is both requested (test param) and actually
        configured on the cluster (via /settings/security/encryptionAtRest).

        suite_setUp calls encryption_util.setup_encryption_at_rest() which creates
        the log KEK secret and calls configure_encryption_at_rest with
        log.encryptionMethod=encryptionKey and log.encryptionKeyId.
        This method confirms that setup completed successfully.
        """
        if not self.enable_log_encryption_at_rest:
            self.skipTest(
                "Log encryption at rest not enabled. "
                "Set enable_log_encryption_at_rest=True to run this test."
            )
        self.assertIsNotNone(
            self.log_encryption_at_rest_id,
            "log_encryption_at_rest_id is None — "
            "suite_setUp may not have configured log encryption"
        )

        # Verify the cluster actually has log encryption active
        status, config = self.rest.get_encryption_at_rest_config()
        self.assertTrue(status, f"Failed to retrieve encryption at rest config: {config}")
        if isinstance(config, (bytes, bytearray)):
            config = json.loads(config)

        log_config = config.get("log", {})
        self.assertEqual(
            log_config.get("encryptionMethod"), "encryptionKey",
            f"Log encryption method is not 'encryptionKey': {log_config}"
        )
        configured_key_id = log_config.get("encryptionKeyId")
        self.assertEqual(
            configured_key_id, self.log_encryption_at_rest_id,
            f"Cluster log encryptionKeyId ({configured_key_id}) does not match "
            f"log_encryption_at_rest_id ({self.log_encryption_at_rest_id})"
        )
        self.log.info(
            f"Log encryption verified on cluster: method=encryptionKey, "
            f"keyId={configured_key_id}"
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
                result = self.run_cbq_query(query=query, server=node)
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

    def _get_vitals_spill_counts(self, query_nodes):
        """Return {node_ip: spills.order} from admin/vitals on each query node."""
        counts = {}
        for node in query_nodes:
            status, vitals = self.rest.get_query_vitals(node)
            self.assertTrue(status, f"admin/vitals failed on {node.ip}: {vitals}")
            if isinstance(vitals, (bytes, bytearray)):
                vitals = json.loads(vitals)
            counts[node.ip] = vitals.get("spills.order", 0)
        return counts

    def _assert_spill_files_encrypted(self, query_nodes, spill_dir, expected_key_ids, label=""):
        """Find all files in spill_dir on each query node and verify they are encrypted."""
        node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)
        failures = []
        total_files = 0
        for node in query_nodes:
            shell = RemoteMachineShellConnection(node)
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

    def _get_query_in_use_key_ids(self):
        """
        Fetch the key IDs currently in use by the query service on every query node.

        Each node independently manages its own DEKs via its own
        GET /admin/encryption_at_rest endpoint. Returns a dict of
        {node_ip: [key_id, ...]} so callers can validate per-node.
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
            log_key_ids = [k for k in keys_in_use.get("log", []) if k]
            self.log.info(f"Query service in-use key IDs on {node.ip}: {log_key_ids}")
            node_key_ids[node.ip] = log_key_ids
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

    def _assert_rlstream_files_encrypted(self, query_nodes, expected_key_ids, label=""):
        """
        Assert that every query node has at least one rlstream.* file and that every
        such file carries the "Couchbase Encrypted" magic bytes and the expected key ID
        in its header.

        rlstream.* files are the active completed-request stream files written while
        the query service is running.  They should exist immediately after scans have
        been issued with completed-stream-size > 0.

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
                failures.append(f"  [{node_ip}] No rlstream.* files found")
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

    def _assert_local_request_log_files_encrypted(self, query_nodes, expected_key_ids, label=""):
        """
        Assert that every query node has at least one local_request_log.* file and that
        every such file carries the "Couchbase Encrypted" magic bytes and the expected
        key ID in its header.

        local_request_log.* files are the gzip-compressed archives created when the
        active rlstream file is finalised (idle timeout or size limit).  They may not
        exist immediately — this method polls up to 120 s for them to appear on every
        node before asserting.

        expected_key_ids: dict {node_ip: [key_id, ...]} or flat list — see
        _build_node_key_ids_map().
        """
        node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)

        deadline = time.time() + 120
        all_results = {}
        while time.time() < deadline:
            all_results = {}
            for node in query_nodes:
                r = self.encryption_helper.verify_query_log_files_encrypted(
                    [node], node_key_ids_map.get(node.ip, [])
                )
                all_results.update(r)
            if all(len(r["local_request_log"]) > 0 for r in all_results.values()):
                break
            self.sleep(5, "Waiting for local_request_log.* files to appear")

        node_obj_map = {node.ip: node for node in query_nodes}
        failures = []
        copied_files = []
        for node_ip, node_result in all_results.items():
            local_log_files = node_result["local_request_log"]
            if len(local_log_files) == 0:
                failures.append(
                    f"  [{node_ip}] No local_request_log.* files found after 120 s wait"
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

    def _wait_for_new_query_key_ids(self, baseline_key_ids, timeout=300, label=""):
        """
        Poll admin/encryption_at_rest until at least one query node reports a key ID
        that was not present in baseline_key_ids.

        baseline_key_ids: {node_ip: [key_id, ...]} — from _get_query_in_use_key_ids()

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
            current = self._get_query_in_use_key_ids()
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
            shell = RemoteMachineShellConnection(node)
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
            shell = RemoteMachineShellConnection(node)
            out, _ = shell.execute_command(
                f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
            shell = RemoteMachineShellConnection(node)
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
            shell = RemoteMachineShellConnection(node)
            out, _ = shell.execute_command(
                f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
        """Assert all query_ffdc_MAN_* files on every query node are NOT encrypted.
        At least one FFDC file must exist per node."""
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
                        result = self.run_cbq_query(query=q, server=next(node_cycle))
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
            shell = RemoteMachineShellConnection(node)
            output, _ = shell.execute_command(f"ls -la {log_path}/rlstream.* 2>/dev/null")
            shell.disconnect()
            sizes[node.ip] = output if output else ["(no rlstream files found)"]
        return sizes

    def _local_request_log_files_exist(self, query_nodes):
        """Return True if any local_request_log.* files exist on any query node."""
        from lib.remote.remote_util import RemoteMachineShellConnection
        for node in query_nodes:
            log_path = self.encryption_helper.get_log_path(node)
            shell = RemoteMachineShellConnection(node)
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
                return self.run_cbq_query(query=query, server=node)
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
            self._verify_log_encryption_prerequisites()
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
            self._assert_local_request_log_files_encrypted(query_nodes, expected_key_ids, label="[STEP 10] ")
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
            self._verify_log_encryption_prerequisites()
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
            self.sleep(120, "Waiting for entries to age into completed_requests_history")
            self._validate_completed_requests_history(select_queries, query_nodes=query_nodes)
            baseline_key_ids = self._get_query_in_use_key_ids()
            self.assertGreater(len(baseline_key_ids), 0, "No query nodes returned key IDs before rotation")
            for node_ip, key_ids in baseline_key_ids.items():
                self.assertGreater(len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use key IDs before rotation")
            self._assert_rlstream_files_encrypted(query_nodes, baseline_key_ids, label="[STEP 8] ")
            self._assert_local_request_log_files_encrypted(query_nodes, baseline_key_ids, label="[STEP 8] ")
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
                baseline_key_ids, timeout=300, label="[STEP 10] "
            )
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
            self._assert_local_request_log_files_encrypted(query_nodes, new_key_ids, label="[STEP 12] ")
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
            self._verify_log_encryption_prerequisites()
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
            self.sleep(120, "Waiting for entries to age into completed_requests_history")
            self._validate_completed_requests_history(select_queries, query_nodes=query_nodes)
            baseline_key_ids = self._get_query_in_use_key_ids()
            self.assertGreater(len(baseline_key_ids), 0, "No query nodes returned key IDs before re-encryption")
            for node_ip, key_ids in baseline_key_ids.items():
                self.assertGreater(len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use key IDs before re-encryption")
            self._assert_rlstream_files_encrypted(query_nodes, baseline_key_ids, label="[STEP 8] ")
            self._assert_local_request_log_files_encrypted(query_nodes, baseline_key_ids, label="[STEP 8] ")
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
                baseline_key_ids, timeout=300, label="[STEP 10] "
            )
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
            self._assert_local_request_log_files_encrypted(query_nodes, new_key_ids, label="[STEP 12] ")
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
            self._verify_log_encryption_prerequisites()
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

        # ========== STEP 9: Trigger FFDC and verify decrypted ==========
        try:
            self.log.info("[STEP 9] Triggering FFDC on all query nodes and verifying decrypted...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                self.log.info(f"[STEP 9] FFDC triggered on {node.ip}")
            self.sleep(30, "Waiting for FFDC files to be generated")

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
        3. Trigger FFDC 1st time and verify encryption with baseline key IDs
        4. Reduce DEK rotation interval to trigger rotation
        5. Poll for new key IDs
        6. Re-pin interval high
        7. Trigger FFDC 2nd & 3rd times and verify encryption with new key IDs
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
            self._verify_log_encryption_prerequisites()
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
            baseline_key_ids = self._get_query_in_use_key_ids()
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

        # ========== STEP 5: Reduce interval to trigger rotation ==========
        try:
            self.log.info(
                f"[STEP 5] Reducing log.dekRotationInterval to {ROTATION_INTERVAL_S} s..."
            )
            status, response = self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": ROTATION_INTERVAL_S,
                "log.dekLifetime": ROTATION_INTERVAL_S
            })
            self.assertTrue(status, f"Failed to set dekRotationInterval: {response}")
            self.log.info("[STEP 5] PASSED - dekRotationInterval set to trigger rotation")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Poll for new key IDs, then re-pin ==========
        try:
            self.log.info("[STEP 6] Polling for new key IDs (timeout 300 s)...")
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 6] "
            )
            self.log.info(
                f"[STEP 6] New key IDs detected: {new_key_ids}. Re-pinning interval..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info("[STEP 6] PASSED - rotation detected, interval re-pinned")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # Snapshot existing FFDC files so we can identify which ones are created post-rotation
        ffdc_files_before_rotation = self._snapshot_ffdc_files(query_nodes)

        # ========== STEPS 7-8: Trigger FFDC 2nd & 3rd times with new keys ==========
        try:
            self.log.info("[STEPS 7-8] Triggering FFDC 2nd & 3rd times (after rotation)...")
            for iteration in range(2, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, new_key_ids,
                    iteration=iteration,
                    label=f"[STEP {5+iteration}] "
                )
            self.log.info("[STEPS 7-8] Verifying new post-rotation FFDC files use only new key IDs...")
            self._assert_new_ffdc_files_use_only_new_keys(
                query_nodes, ffdc_files_before_rotation, new_key_ids, baseline_key_ids,
                label="[STEPS 7-8] "
            )
            self.log.info("[STEPS 7-8] PASSED - FFDC files encrypted with new keys")
        except Exception as e:
            self.log.error(f"[STEPS 7-8] FAILED: {e}")
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

        Verifies that FFDC files remain encrypted after full re-encryption.
        Uses the same pin-high-before-setup pattern as the log re-encryption test.

        Steps:
        1. Verify prerequisites
        2. Pin DEK rotation interval high before any FFDC generation
        3. Trigger FFDC 1st time and verify encryption with baseline key IDs
        4. Trigger full re-encryption via dropEncryptionAtRestDeks/log
        5. Poll for new key IDs
        6. Re-pin interval high
        7. Trigger FFDC 2nd & 3rd times and verify encryption with new key IDs
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
            self._verify_log_encryption_prerequisites()
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
            baseline_key_ids = self._get_query_in_use_key_ids()
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

        # ========== STEP 5: Trigger full re-encryption ==========
        try:
            self.log.info(
                "[STEP 5] Triggering full log re-encryption "
                "(POST /controller/dropEncryptionAtRestDeks/log)..."
            )
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger log re-encryption: {response}")
            self.log.info(f"[STEP 5] PASSED - log re-encryption triggered")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Poll for new key IDs, then re-pin ==========
        try:
            self.log.info("[STEP 6] Polling for new key IDs (timeout 300 s)...")
            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 6] "
            )
            self.log.info(
                f"[STEP 6] New key IDs detected: {new_key_ids}. Re-pinning interval..."
            )
            self.rest.configure_encryption_at_rest({
                "log.dekRotationInterval": STABLE_INTERVAL_S,
                "log.dekLifetime": STABLE_INTERVAL_S
            })
            self.log.info("[STEP 6] PASSED - re-encryption detected, interval re-pinned")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEPS 7-8: Trigger FFDC 2nd & 3rd times with new keys ==========
        try:
            self.log.info("[STEPS 7-8] Triggering FFDC 2nd & 3rd times (after re-encryption)...")
            for iteration in range(2, 4):
                self._dispatch_background_query_batch(select_queries, query_nodes)
                self._trigger_ffdc_and_verify(
                    query_nodes, new_key_ids,
                    iteration=iteration,
                    label=f"[STEP {5+iteration}] "
                )
            self.log.info("[STEPS 7-8] PASSED - FFDC files encrypted with new keys")
        except Exception as e:
            self.log.error(f"[STEPS 7-8] FAILED: {e}")
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
            self._verify_log_encryption_prerequisites()
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
                shell = RemoteMachineShellConnection(node)
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
            self._verify_log_encryption_prerequisites()
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

        # ========== STEP 4: Trigger FFDC while encryption is disabled ==========
        try:
            self.log.info("[STEP 4] Triggering FFDC on all query nodes (encryption disabled)...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                self.log.info(f"[STEP 4] FFDC triggered on {node.ip}")
            self.sleep(30, "Waiting for FFDC files to be generated")
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

        # ========== STEP 9: Trigger FFDC with encryption enabled ==========
        try:
            self.log.info("[STEP 9] Triggering FFDC on all query nodes (encryption enabled)...")
            self._dispatch_background_query_batch(select_queries, query_nodes)
            for node in query_nodes:
                status, response = self.rest.trigger_query_ffdc(node)
                self.assertTrue(status, f"Failed to trigger FFDC on {node.ip}: {response}")
                self.log.info(f"[STEP 9] FFDC triggered on {node.ip}")
            self.sleep(30, "Waiting for new FFDC files to be generated")
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
                shell = RemoteMachineShellConnection(node)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
            self._verify_log_encryption_prerequisites()
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
            baseline_key_ids = self._get_query_in_use_key_ids()
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
                baseline_key_ids, timeout=300, label="[STEP 5] "
            )
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
                    shell = RemoteMachineShellConnection(node)
                    out, _ = shell.execute_command(
                        f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
            self._verify_log_encryption_prerequisites()
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
            node_map = {node.ip: node for node in query_nodes}
            for node in query_nodes:
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
                shell = RemoteMachineShellConnection(node)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
            self._verify_log_encryption_prerequisites()
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
                shell = RemoteMachineShellConnection(node)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
                shell = RemoteMachineShellConnection(node)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'query_ffdc_MAN_*' 2>/dev/null"
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
            self._verify_log_encryption_prerequisites()
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
            baseline_key_ids = self._get_query_in_use_key_ids()
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

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
                baseline_key_ids, timeout=300, label="[STEP 4] "
            )
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
            self.log.info("[STEP 7] Verifying rlstream files are encrypted with new key IDs...")
            self._assert_rlstream_files_encrypted(query_nodes, new_key_ids, label="[STEP 7] ")
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
            self._verify_log_encryption_prerequisites()
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
            baseline_key_ids = self._get_query_in_use_key_ids()
            self.log.info(f"[STEP 3] PASSED - baseline key IDs: {baseline_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        bg_thread, stop_event, query_errors = self._start_background_query_loop(
            select_queries, query_nodes
        )
        try:
            self.log.info("[STEP 4] Triggering force re-encryption while queries run in background...")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"Failed to trigger re-encryption: {response}")

            new_key_ids = self._wait_for_new_query_key_ids(
                baseline_key_ids, timeout=300, label="[STEP 4] "
            )
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
            self.log.info("[STEP 7] Verifying rlstream files are encrypted with new key IDs...")
            self._assert_rlstream_files_encrypted(query_nodes, new_key_ids, label="[STEP 7] ")
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
            self._verify_log_encryption_prerequisites()
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
            self._verify_log_encryption_prerequisites()
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
            baseline_key_ids = self._get_query_in_use_key_ids()
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
                baseline_key_ids, timeout=300, label="[STEP 7] "
            )
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
            node_map = {node.ip: node for node in query_nodes}
            failures = []
            copied_files = []
            for node in query_nodes:
                log_path = self.encryption_helper.get_log_path(node)
                shell = RemoteMachineShellConnection(node)
                out, _ = shell.execute_command(
                    f"find {log_path} -name 'rlstream.*' 2>/dev/null"
                )
                shell.disconnect()
                all_rlstream = [f.strip() for f in out if f.strip()]
                if not all_rlstream:
                    failures.append(f"  [{node.ip}] No rlstream.* files found")
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

    def test_query_spill_files_encrypted(self):
        """
        Verify that query spill (temp) files are encrypted when log encryption at rest
        is enabled.

        Spill files are written to the directory reported by GET /settings/querySettings
        (tmpSpaceDir field) when the query engine cannot hold an intermediate sort result
        set in memory.  A SELECT * ... ORDER BY on a large collection reliably triggers
        spilling.

        Steps:
        1.  Verify log encryption at rest prerequisites
        2.  Create bucket, load 100 000 Hotel documents, create primary index
        3.  GET /settings/querySettings — capture spill directory (tmpSpaceDir)
        4.  GET admin/vitals — record baseline spills.order per query node
        5.  GET encryption key IDs per query node
        6.  Run SELECT * FROM <collection> ORDER BY counter in background
        7.  Poll admin/vitals until spills.order increments on any node (timeout 300 s)
        8.  While query is still running, validate spill files in tmpSpaceDir are encrypted
        9.  Wait for background query to finish
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_query_spill_files_encrypted")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying log encryption at rest prerequisites...")
            self._verify_log_encryption_prerequisites()
            self.log.info(
                f"[STEP 1] PASSED - log_encryption_at_rest_id: {self.log_encryption_at_rest_id}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {e}")
            raise

        # ========== STEP 2: Create bucket, load 100 000 Hotel docs, primary index ==========
        try:
            self.log.info("[STEP 2] Creating bucket and loading 100 000 Hotel documents...")
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
                num_of_docs_per_collection=100000,
                json_template="Hotel",
                bucket_name=self.test_bucket
            )
            self.assertTrue(len(self.namespaces) > 0, "No namespaces created")
            namespace = self.namespaces[0]
            self.log.info(f"[STEP 2] Data loaded into namespace: {namespace}")

            self.log.info("[STEP 2] Creating primary index...")
            primary_idx = QueryDefinition(index_name="spill_test_primary")
            primary_idx_query = primary_idx.generate_primary_index_create_query(
                namespace=namespace,
                defer_build=False,
                num_replica=0
            )
            self.run_cbq_query(query=primary_idx_query)
            self.wait_until_indexes_online(timeout=300, defer_build=False)
            self.log.info("[STEP 2] PASSED - bucket, data, and primary index ready")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {e}")
            raise

        # ========== STEP 3: Get spill directory from settings/querySettings ==========
        try:
            self.log.info("[STEP 3] Querying /settings/querySettings for spill directory...")
            status, query_settings = self.rest.get_query_settings(self.master)
            self.assertTrue(status, f"querySettings fetch failed: {query_settings}")
            spill_dir = query_settings.get("queryTmpSpaceDir") or query_settings.get("tmpSpaceDir")
            self.assertIsNotNone(
                spill_dir,
                f"[STEP 3] Could not find spill directory in querySettings: {query_settings}"
            )
            self.log.info(f"[STEP 3] PASSED - spill directory: {spill_dir}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {e}")
            raise

        # ========== STEP 4: Baseline spills.order per node ==========
        try:
            self.log.info("[STEP 4] Recording baseline spills.order per query node...")
            query_nodes = self.get_nodes_from_services_map(
                service_type="n1ql", get_all_nodes=True
            )
            baseline_spills = self._get_vitals_spill_counts(query_nodes)
            self.log.info(f"[STEP 4] PASSED - baseline spills.order: {baseline_spills}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {e}")
            raise

        # ========== STEP 5: Get expected encryption key IDs ==========
        try:
            self.log.info("[STEP 5] Getting in-use encryption key IDs per query node...")
            expected_key_ids = self._get_query_in_use_key_ids()
            self.assertGreater(len(expected_key_ids), 0, "No query nodes returned key IDs")
            for node_ip, key_ids in expected_key_ids.items():
                self.assertGreater(
                    len(key_ids), 0,
                    f"Query node {node_ip} reported no in-use encryption key IDs"
                )
            self.log.info(f"[STEP 5] PASSED - key IDs: {expected_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {e}")
            raise

        # ========== STEP 6: Run ORDER BY query in background to trigger spilling ==========
        try:
            self.log.info("[STEP 6] Launching background ORDER BY query to trigger spill files...")
            # Pick the first query node to route the heavy query through
            sort_node = query_nodes[0]
            sort_query = f"SELECT * FROM {namespace} ORDER BY counter"

            def _run_sort_query():
                try:
                    result = self.run_cbq_query(query=sort_query, server=sort_node)
                    self.log.info(
                        f"[STEP 6] Background ORDER BY query completed on {sort_node.ip}, "
                        f"status={result.get('status')}"
                    )
                except Exception as ex:
                    self.log.warning(f"[STEP 6] Background ORDER BY query raised: {ex}")

            bg_thread = threading.Thread(target=_run_sort_query, daemon=True, name="bg-sort-query")
            bg_thread.start()
            self.log.info(
                f"[STEP 6] PASSED - background sort query started on {sort_node.ip}: {sort_query}"
            )
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {e}")
            raise

        # ========== STEP 7: Poll vitals until spills.order increments ==========
        try:
            self.log.info(
                "[STEP 7] Polling admin/vitals until spills.order increments (timeout 300 s)..."
            )
            deadline = time.time() + 300
            spill_detected = False
            while time.time() < deadline:
                current_spills = self._get_vitals_spill_counts(query_nodes)
                self.log.info(
                    f"[STEP 7] Current spills.order: {current_spills} "
                    f"(baseline: {baseline_spills})"
                )
                if any(
                    current_spills.get(node.ip, 0) > baseline_spills.get(node.ip, 0)
                    for node in query_nodes
                ):
                    spill_detected = True
                    self.log.info(
                        f"[STEP 7] PASSED - spills.order incremented: "
                        f"baseline={baseline_spills} current={current_spills}"
                    )
                    break
                if not bg_thread.is_alive():
                    self.log.warning(
                        "[STEP 7] Background query finished before spill was detected"
                    )
                    break
                self.sleep(5, "[STEP 7] Waiting for spills.order to increment...")
            self.assertTrue(
                spill_detected,
                f"[STEP 7] spills.order did not increment within 300 s. "
                f"baseline={baseline_spills}, last seen={current_spills}"
            )
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {e}")
            raise

        # ========== STEP 8: Validate spill files are encrypted ==========
        try:
            self.log.info(
                f"[STEP 8] Validating spill files in {spill_dir} are encrypted "
                f"(query still running: {bg_thread.is_alive()})..."
            )
            self._assert_spill_files_encrypted(
                query_nodes, spill_dir, expected_key_ids, label="[STEP 8] "
            )
            self.log.info("[STEP 8] PASSED - all spill files verified encrypted")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {e}")
            raise

        # ========== STEP 9: Wait for background query to finish ==========
        try:
            self.log.info("[STEP 9] Waiting for background ORDER BY query to finish...")
            bg_thread.join(timeout=600)
            if bg_thread.is_alive():
                self.log.warning(
                    "[STEP 9] Background query did not finish within 600 s; test continues"
                )
            else:
                self.log.info("[STEP 9] PASSED - background query finished")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {e}")
            raise

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_query_spill_files_encrypted")
        self.log.info("=" * 80)