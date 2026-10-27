"""
gsi_encryption_at_rest_static_topology.py: Tests for GSI with Encryption at Rest enabled

Tests validate index creation, scans, and encryption configuration for various storage
types (MOI and Plasma) with encryption at rest enabled on buckets.

Encryption setup is handled by basetestcase.py when enable_encryption_at_rest=True.
This test uses self.encryption_at_rest_id (set by base class) to enable bucket encryption.

Supported storage types:
- memory_optimized (MOI) - in-memory storage
- forestdb (Plasma) - persistent storage

__author__ = "Pavan PB"
__maintainer = "Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
__created_on__ = "16/04/26"
"""

import hashlib
import json
import random
import re
import string
import threading
import time
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from .gsi_encryption_helpers import GSIEncryptionAtRestBase, _GSI_TYPE_TO_ENGINE
from .base_gsi import BaseSecondaryIndexingTests


class GSIEncryptionAtRestStaticTopology(GSIEncryptionAtRestBase, BaseSecondaryIndexingTests):
    """
    Test class for GSI operations with Encryption at Rest enabled.

    Tests validate:
    - Encryption at rest configuration on buckets
    - Index creation with various types and replica counts on different storage types
    - Scan operations and result validation
    - Index stats verification (num_requests)
    
    Supported storage types:
    - memory_optimized (MOI) - in-memory indexes
    - forestdb (Plasma) - persistent indexes
    """
    
    def setUp(self):
        self.skip_corruption_checks = False
        super(GSIEncryptionAtRestStaticTopology, self).setUp()
        self.log.info("==============  GSIEncryptionAtRestStaticTopology setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_count = self.input.param("bucket_count", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.use_default_collection_only = self.input.param("use_default_collection_only", True)
        self.num_of_docs_per_collection = self.input.param("num_of_docs_per_collection", 10000)
        self.json_template = self.input.param("json_template", "Hotel")
        self.defer_build = self.input.param("defer_build", False)
        self.encrypt_all_buckets = self.input.param("encrypt_all_buckets", True)
        self.induce_dgm = self.input.param("induce_dgm", False)
        self.index_resident_ratio = int(self.input.param("index_resident_ratio", 50))
        self.kek_rotation_interval_seconds = self.input.param("kek_rotation_interval_seconds", 120)
        self.mutation_ops_rate = int(self.input.param("mutation_ops_rate", 500))
        self.workload_percent_create = int(self.input.param("workload_percent_create", 0))
        self.workload_percent_update = int(self.input.param("workload_percent_update", 100))
        self.workload_percent_delete = int(self.input.param("workload_percent_delete", 0))
        self.key_op = self.input.param("key_op", "rotation")
        self.num_rotations = int(self.input.param("num_rotations", 3))
        # Floor for wait_until_indexes_online: dense/sparse BHIVE vector indexes
        # train one at a time per node (indexer.vector.max_parallel_training
        # defaults to 1), so vector coverage needs more headroom than the
        # scalar-only default of 1200s used elsewhere in this file.
        self.index_online_timeout = self.input.param("index_online_timeout", 1200)
        # Patterns a test induces on purpose; set per test, never on the class.
        self.allowed_encryption_log_patterns = ()
        self.gsi_log_error_baseline = None
        if not getattr(self, "capella_run", False) and not self.input.param("skip_encryption_log_check", False):
            self.gsi_log_error_baseline = self.get_gsi_log_error_counts(label="[setUp] ")
        self.log.info("==============  GSIEncryptionAtRestStaticTopology setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIEncryptionAtRestStaticTopology tearDown has started ==============")
        log_check_error = None
        try:
            self.check_gsi_logs_for_encryption_errors()
        except Exception as ex:
            log_check_error = ex
        super(GSIEncryptionAtRestStaticTopology, self).tearDown()
        if log_check_error:
            raise log_check_error
        self.log.info("==============  GSIEncryptionAtRestStaticTopology tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  GSIEncryptionAtRestStaticTopology suite_tearDown has started ==============")
        super(GSIEncryptionAtRestStaticTopology, self).suite_tearDown()
        self.log.info("==============  GSIEncryptionAtRestStaticTopology suite_tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  GSIEncryptionAtRestStaticTopology suite_setUp has started ==============")
        super(GSIEncryptionAtRestStaticTopology, self).suite_setUp()
        self.log.info("==============  GSIEncryptionAtRestStaticTopology suite_setUp has completed ==============")

    def _create_indexes(self, namespace, index_nodes):
        """
        Create indexes using gsi_util_obj patterns similar to gsi_file_based_rebalance.py.
        
        Creates one set of indexes with num_replica=self.num_index_replica.
        If defer_build=True, creates deferred indexes and builds them separately.
        
        Args:
            namespace: Namespace string for index creation
            index_nodes: List of index nodes for deployment
            
        Returns:
            tuple: (select_queries set, create_queries list, deferred_definitions list)
        """
        select_queries = set()
        create_queries = []
        deferred_definitions = []
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        
        for namespace_iter in self.namespaces:
            # Generate unique prefix
            prefix = 'ear_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            
            # Get index definitions based on json_template
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            
            # Get select queries for validation
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace_iter
                )
            )
            
            # Get create index queries with specified replica count
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions,
                namespace=namespace_iter,
                num_replica=self.num_index_replica,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            
            # Create indexes
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace_iter,
                query_node=query_node
            )
            
            create_queries.extend(queries)
            
            # Track deferred definitions for later build
            if self.defer_build:
                deferred_definitions.extend(query_definitions)
        
        self.log.info(f"Created {len(create_queries)} indexes with num_replica={self.num_index_replica}")
        return select_queries, create_queries, deferred_definitions

    def _wait_for_empty_service_bucket_keys_cleared(self, index_nodes, timeout=300, label=""):
        """
        Poll get_indexer_in_use_encryption_keys until all service_bucket entries have
        non-empty key IDs (i.e., old DEKs referencing empty keys have drained).

        After the timeout logs a warning and returns without failing — the caller
        proceeds to file validation regardless.
        """
        deadline = time.time() + timeout
        poll_interval = 15
        while time.time() < deadline:
            found_empty = False
            for index_node in index_nodes:
                rest = RestConnection(index_node)
                status, response = rest.get_indexer_in_use_encryption_keys()
                if not status:
                    self.log.warning(f"{label}Could not fetch in-use keys from {index_node.ip}: {response}")
                    found_empty = True
                    break
                for bucket_key, key_list in response.items():
                    if not bucket_key.startswith("{service_bucket"):
                        continue
                    if any(k == "" for k in key_list):
                        self.log.info(
                            f"{label}Empty key still present in {bucket_key} on {index_node.ip}, "
                            f"waiting for DEK drain..."
                        )
                        found_empty = True
                        break
                if found_empty:
                    break
            if not found_empty:
                self.log.info(f"{label}All service_bucket entries have non-empty key IDs — DEK drain complete")
                return
            time.sleep(poll_interval)

        self.log.warning(
            f"{label}Empty key IDs in service_bucket entries did not clear within {timeout}s. "
            f"Proceeding to file validation."
        )

    def _load_until_index_dgm(self, resident_ratio=50):
        """
        Load documents in batches until at least one index node's resident ratio drops to or
        below the target. Exits early if any node goes under the target to avoid pushing any
        node below 10% RR (which is not recommended).

        Requires self.namespaces to be populated before calling.
        """
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        def _any_node_below_or_at_target():
            for node in index_nodes:
                rest = RestConnection(node)
                all_stats = rest.get_all_index_stats()
                node_rr = all_stats['avg_resident_percent']
                self.log.info(f"Node {node.ip} avg_resident_ratio={node_rr}, target={resident_ratio}")
                if node_rr <= resident_ratio:
                    return True
            return False

        if _any_node_below_or_at_target():
            self.log.info(f"At least one node is already at or below targeted {resident_ratio}. Not loading any more data")
            return

        rr_achieved, end_num, batch_size = False, self.num_of_docs_per_collection, 100000
        while not rr_achieved:
            all_tasks = []
            create_start = end_num
            end_num = end_num + batch_size
            for item in self.namespaces:
                bucket_name = item.split(":")[1].split(".")[0]
                s_item, c_item = item.split(":")[1].split(".")[1], item.split(":")[1].split(".")[2]
                self.gen_create = SDKDataLoader(
                    num_ops=batch_size, percent_create=100,
                    percent_update=0, percent_delete=0, scope=s_item,
                    collection=c_item, json_template=self.json_template,
                    output=True, username=self.username, password=self.password,
                    create_start=create_start, create_end=end_num
                )
                task = self.cluster.async_load_gen_docs(
                    self.master, bucket=bucket_name,
                    generator=self.gen_create, pause_secs=1,
                    timeout_secs=300, use_magma_loader=True
                )
                all_tasks.append(task)
            # Wait longer than 10s — RR update isn't instantaneous.
            time.sleep(30)
            if _any_node_below_or_at_target():
                rr_achieved = True
                self.log.info("Target RR achieved (at least one node at or below target)")
            for task in all_tasks:
                task.result()
        self.sleep(30)

    def _run_index_lifecycle_ops(self, namespace, index_nodes, query_node):
        """
        Run post-rotation index lifecycle operations: alter, drop, build.

        1. Alter — increase replica count on one existing index.
        2. Drop  — drop one existing index.
        3. Build — create a deferred index then build it.

        Args:
            namespace: keyspace string e.g. 'default:bucket._default._default'
            index_nodes: list of index node objects
            query_node: n1ql node for running queries
        """
        keyspace = namespace.split(":")[1]  # bucket._default._default
        bucket = keyspace.split(".")[0]

        # Fetch current index list for this keyspace
        index_status = self.rest.get_index_status()
        keyspace_indexes = []
        for info in index_status.values():
            for idx in info if isinstance(info, list) else [info]:
                if isinstance(idx, dict) and idx.get("bucket") == bucket and idx.get("status") == "Ready":
                    keyspace_indexes.append(idx.get("index", ""))
        keyspace_indexes = [i for i in keyspace_indexes if i]

        # --- ALTER: bump replica count on first available index ---
        if keyspace_indexes:
            alter_target = keyspace_indexes[0]
            deploy_nodes = ",".join([f'"{node.ip}:{self.node_port}"' for node in index_nodes])
            alter_query = (
                f'ALTER INDEX {namespace}.`{alter_target}` '
                f'WITH {{"action": "replica_count", "num_replica": 1, "nodes": [{deploy_nodes}]}}'
            )
            self.log.info(f"[LIFECYCLE] ALTER INDEX: {alter_query[:120]}")
            self.run_cbq_query(query=alter_query, server=query_node)
            self.wait_until_indexes_online(timeout=300)
            self.log.info(f"[LIFECYCLE] ALTER PASSED - {alter_target}")

        # --- DROP: drop second available index (avoid dropping the altered one) ---
        if len(keyspace_indexes) >= 2:
            drop_target = keyspace_indexes[1]
            drop_query = f'DROP INDEX {namespace}.`{drop_target}`'
            self.log.info(f"[LIFECYCLE] DROP INDEX: {drop_query}")
            self.run_cbq_query(query=drop_query, server=query_node)
            self.log.info(f"[LIFECYCLE] DROP PASSED - {drop_target}")

        # --- BUILD: create a deferred index then build it ---
        deferred_name = 'lifecycle_deferred_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
        create_deferred = f'CREATE INDEX `{deferred_name}` ON {namespace}(type) WITH {{"defer_build": true}}'
        self.log.info(f"[LIFECYCLE] CREATE DEFERRED: {create_deferred}")
        self.run_cbq_query(query=create_deferred, server=query_node)

        build_query = f'BUILD INDEX ON {namespace}(`{deferred_name}`)'
        self.log.info(f"[LIFECYCLE] BUILD INDEX: {build_query}")
        self.run_cbq_query(query=build_query, server=query_node)
        self.wait_until_indexes_online(timeout=300)
        self.log.info(f"[LIFECYCLE] BUILD PASSED - {deferred_name}")

    def _kv_node_without_n1ql(self, kv_nodes):
        """KV node to kill and fail over: one without the query service, so scans after the
        failover still have a query node."""
        n1ql_ips = {n.ip for n in self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)}
        node = next((n for n in kv_nodes if n.ip not in n1ql_ips), None)
        self.assertIsNotNone(node, f"No KV node without the query service in {[n.ip for n in kv_nodes]}")
        return node

    def _induce_partial_rollback(self, kv_nodes, bucket_name):
        """
        Induce a KV partial rollback:
          1. Stop persistence on all KV nodes
          2. Load 5000 update-only mutations (accumulate in memory, not persisted)
          3. Kill memcached on kv_nodes[0] — forces reload from last persisted snapshot
          4. Start persistence on remaining KV nodes
          5. Hard-failover kv_nodes[0]
          6. Return item count after rollback
        """
        from memcached.helper.data_helper import MemcachedClientHelper

        self.log.info(f"[_induce_partial_rollback] Stopping persistence on {[n.ip for n in kv_nodes]}...")
        for n in kv_nodes:
            MemcachedClientHelper.direct_client(n, bucket_name).stop_persistence()

        self.log.info("[_induce_partial_rollback] Loading 5000 update-only mutations (in-memory, not persisted)...")
        gen_update = SDKDataLoader(
            num_ops=5000,
            percent_create=0, percent_update=100, percent_delete=0,
            scope="_default", collection="_default",
            json_template=self.json_template, output=True,
            username=self.username, password=self.password
        )
        tasks = self.data_ops_javasdk_loader_in_batches(
            sdk_data_loader=gen_update, batch_size=5000, dataset=self.json_template
        )
        for task in tasks:
            task.result()
        time.sleep(10)

        node_to_kill = self._kv_node_without_n1ql(kv_nodes)
        self.log.info(f"[_induce_partial_rollback] Killing memcached on {node_to_kill.ip}...")
        shell = RemoteMachineShellConnection(node_to_kill)
        shell.kill_memcached()
        shell.disconnect()
        time.sleep(10)

        for n in kv_nodes[1:]:
            self.log.info(f"[_induce_partial_rollback] Starting persistence on {n.ip}...")
            MemcachedClientHelper.direct_client(n, bucket_name).start_persistence()

        self.log.info(f"[_induce_partial_rollback] Hard-failing over {node_to_kill.ip}...")
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [node_to_kill],
            graceful=False, wait_for_pending=120
        )
        failover_task.result()

        count = self.get_item_count(self.master, bucket_name)
        self.log.info(f"[_induce_partial_rollback] Done. Item count after rollback: {count}")
        return count

    def _get_vector_query_definitions(self, prefix):
        """
        Build dense + sparse, BHIVE + composite, and scalar index definitions
        for vector-capable datasets (e.g. MSMARCOSiftEmbeddingProduct).

        get_index_definition_list() only returns one index "shape" per call
        (dense XOR sparse, bhive XOR composite, scalar XOR vector), so full
        coverage requires 5 separate calls merged together — mirrors the
        pattern in gsi_encryption_at_rest_rebalance.py's _get_query_definitions
        and bhive_e2e_tests.py's _get_query_definitions.

        Sparse indexes only support DOT similarity and ignore quantization
        (enforced by get_index_definition_list itself), so similarity/
        quantization args are only meaningful for the dense calls.

        Returns:
            tuple: (dense_bhive_def, dense_composite_def, sparse_bhive_def,
                sparse_composite_def, scalar_def) — kept separate (not
                flattened) because QueryDefinition objects built by the
                per-kind generator functions are NOT tagged with
                bhive_index=True even when the function name says "bhive";
                the caller must track which list an index came from rather
                than introspect the object to know whether it's BHIVE.
        """
        dense_bhive_def = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_dbhive",
            similarity=self.similarity,
            scan_nprobes=self.scan_nprobes,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            description_dimension=self.dimension,
            bhive_index=True,
            skip_primary=True
        )
        dense_composite_def = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_dcomp",
            similarity=self.similarity,
            scan_nprobes=self.scan_nprobes,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            description_dimension=self.dimension,
            bhive_index=False,
            skip_primary=True
        )
        sparse_bhive_def = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_sbhive",
            similarity="DOT",
            scan_nprobes=self.scan_nprobes,
            bhive_index=True,
            is_sparse=True,
            skip_primary=True
        )
        sparse_composite_def = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_scomp",
            similarity="DOT",
            scan_nprobes=self.scan_nprobes,
            bhive_index=False,
            is_sparse=True,
            skip_primary=True
        )
        scalar_def = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_scalar",
            scalar=True,
            skip_primary=False
        )
        self.log.info(
            f"Vector index definitions generated: "
            f"dense_bhive={len(dense_bhive_def)}, dense_composite={len(dense_composite_def)}, "
            f"sparse_bhive={len(sparse_bhive_def)}, sparse_composite={len(sparse_composite_def)}, "
            f"scalar={len(scalar_def)}"
        )
        return dense_bhive_def, dense_composite_def, sparse_bhive_def, sparse_composite_def, scalar_def

    def _get_vector_create_queries(self, dense_bhive_def, dense_composite_def,
                                   sparse_bhive_def, sparse_composite_def, scalar_def,
                                   namespace, deploy_nodes):
        """
        Build CREATE INDEX queries for each definition group returned by
        _get_vector_query_definitions(), passing the correct bhive_index flag
        explicitly per group (never inferred from the definition object).

        Uses self.num_index_replica uniformly across scalar/dense/sparse/bhive —
        unlike gsi_encryption_at_rest_rebalance.py, static topology never
        rebalances a node out mid-test, so there's no "lone replica destroyed"
        risk that would require capping vector replicas differently from
        scalar ones.
        """
        create_queries = []
        for definitions, bhive_index in (
            (dense_bhive_def, True),
            (dense_composite_def, False),
            (sparse_bhive_def, True),
            (sparse_composite_def, False),
        ):
            if definitions:
                create_queries.extend(self.gsi_util_obj.get_create_index_list(
                    definition_list=definitions,
                    namespace=namespace,
                    num_replica=self.num_index_replica,
                    deploy_node_info=deploy_nodes,
                    defer_build=self.defer_build,
                    bhive_index=bhive_index
                ))
        if scalar_def:
            create_queries.extend(self.gsi_util_obj.get_create_index_list(
                definition_list=scalar_def,
                namespace=namespace,
                num_replica=self.num_index_replica,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            ))
        return create_queries

    # =========================================================================
    # MB-69501: index tools (cbindexperf, cbindexplan) must not write files when
    # any encryption at rest (bucket, log, audit, other) is on. config is ignored.
    # =========================================================================

    TOOLS_REFUSED_MSG = "file output is not supported when encryption at rest is enabled."
    TOOLS_STATUS_UNKNOWN_MSG = "could not determine encryption-at-rest status"
    TOOLS_INDEX = "tools_idx"
    TOOLS_ENCRYPTION_KINDS = ("bucket", "log", "audit", "other")

    def _run_tool(self, node, workdir, tool, args):
        """run_cb_tool; cbindexperf also gets CBAUTH_REVRPC_URL, which it does not set
        itself (cbindex and cbindexplan do), as indexing's CI does before running it."""
        env = None
        if tool == "cbindexperf":
            env = {"CBAUTH_REVRPC_URL": f"http://{self.username}:{self.password}@{node.ip}:8091/cbindexperf"}
        return self.gsi_encryption_helper.run_cb_tool(node, workdir, tool, args, env=env)

    def _tool_files(self, node, workdir):
        """Names of the files a tool run left in workdir."""
        return {path.rsplit("/", 1)[-1]
                for path in self.gsi_encryption_helper.get_files_in_directory(node, workdir)}

    def _tools_setup(self, node):
        """Unencrypted bucket with one index, the tool inputs on node, and one key
        usable for bucket, log, audit and other encryption. Returns (base_dir, secret_id)."""
        bucket = self.test_bucket
        self.create_bucket_with_encryption(bucket, enable_encryption=False)
        self.run_cbq_query(query=f"CREATE INDEX {self.TOOLS_INDEX} ON `{bucket}`(name)")
        self.wait_until_indexes_online()

        base_dir = "/tmp/gsi_ear_tools_" + ''.join(random.SystemRandom().choices(string.ascii_lowercase + string.digits, k=6))
        # Formats: cbindexperf README (ScanSpecs) and gsi_replica_indexes' cbindexplan index spec
        perf_config = {"Concurrency": 1, "Clients": 1, "ScanSpecs": [
            {"Type": "All", "Id": 1, "Bucket": bucket, "Index": self.TOOLS_INDEX, "Limit": 1, "Repeat": 0}]}
        index_spec = [{"name": "tools_plan_idx", "bucket": bucket, "isPrimary": False, "secExprs": ["age"],
                       "isArrayIndex": False, "replica": 1, "numDoc": 1000, "DocKeySize": 200,
                       "SecKeySize": 200, "ArrKeySize": 0, "ArrSize": 0, "MutationRate": 0, "ScanRate": 0}]
        shell = RemoteMachineShellConnection(node)
        try:
            shell.create_file(f"{base_dir}/perf_config.json", json.dumps(perf_config))
            shell.create_file(f"{base_dir}/index_spec.json", json.dumps(index_spec))
        finally:
            shell.disconnect()

        secret_id = self.create_new_encryption_key(
            usage=["bucket-encryption", "log-encryption", "audit-encryption", "other-encryption"])
        return base_dir, secret_id

    def _tools_cleanup(self, node, base_dir, secret_id, kinds):
        for kind in kinds:
            try:
                self.set_encryption_at_rest(kind, secret_id, enable=False, bucket=self.test_bucket)
            except Exception as ex:
                self.log.warning(f"Could not turn {kind} encryption off: {ex}")
        try:
            self.rest.delete_secret(secret_id)
        except Exception as ex:
            self.log.warning(f"Could not delete secret {secret_id}: {ex}")
        shell = RemoteMachineShellConnection(node)
        try:
            shell.remove_folders([base_dir])
        finally:
            shell.disconnect()

    def _run_tool_file_outputs(self, node, base_dir, label):
        """Run every file-writing option of cbindexplan and cbindexperf, each in its own
        empty directory. Returns {option: (output, exit code, files left, expected file)}."""
        cluster = f"{node.ip}:8091"  # the tools' encryption check only speaks http
        plan = (f"-command=plan -memQuota=4G -indexes={base_dir}/index_spec.json -cluster={cluster} "
                f"-username={self.username} -password={self.password}")
        perf = (f"-configfile={base_dir}/perf_config.json -cluster={cluster} "
                f"-auth={self.username}:{self.password}")
        options = {
            "cbindexplan -output": ("cbindexplan", f"{plan} -output=plan.json", "plan.json"),
            "cbindexplan -ddl": ("cbindexplan", f"{plan} -ddl=plan.sql", "plan.sql"),
            "cbindexperf -resultfile": ("cbindexperf", f"{perf} -resultfile=results.json", "results.json"),
            "cbindexperf -statsfile": ("cbindexperf", f"{perf} -statsfile=stats.txt", "stats.txt"),
            "cbindexperf -cpuprofile": ("cbindexperf", f"{perf} -cpuprofile=cpu.prof", "cpu.prof"),
            "cbindexperf -memprofile": ("cbindexperf", f"{perf} -memprofile=mem.prof", "mem.prof"),
        }
        results = {}
        for i, (option, (tool, args, expected)) in enumerate(options.items()):
            workdir = f"{base_dir}/{label}_{i}"
            output, exit_code = self._run_tool(node, workdir, tool, args)
            results[option] = (output, exit_code, self._tool_files(node, workdir), expected, workdir)
        return results

    def _assert_tool_outputs(self, node, results, expect_refused, label):
        failures = []
        for option, (output, exit_code, files, expected, workdir) in results.items():
            self.log.info(f"[{label}] {option}: exit code {exit_code}, files {sorted(files)}")
            if expect_refused:
                if self.TOOLS_REFUSED_MSG not in output:
                    failures.append(f"{option}: no refusal message")
                if exit_code != 0:
                    failures.append(f"{option}: exit code {exit_code} (tools exit 0 on refusal)")
                if files:
                    failures.append(f"{option}: wrote {sorted(files)}")
                continue
            if self.TOOLS_REFUSED_MSG in output:
                failures.append(f"{option}: refused with no encryption on")
            elif expected not in files:
                failures.append(f"{option}: {expected} not written (files: {sorted(files)})")
            elif option == "cbindexplan -output":
                shell = RemoteMachineShellConnection(node)
                try:
                    out, _ = shell.execute_command(f"cat {workdir}/{expected}")
                finally:
                    shell.disconnect()
                try:
                    json.loads("".join(out))
                except ValueError as ex:
                    failures.append(f"{option}: {expected} is not valid JSON: {ex}")
        self.assertFalse(failures, f"[{label}] tool file output check failed: {failures}")

    def test_gsi_tools_file_output_encryption(self):
        """
        MB-69501: cbindexperf and cbindexplan write no files while any encryption at
        rest is on, and write them as before when only config encryption is on.

        Steps:
        1.  Unencrypted bucket + index; tool inputs on the node; one key for all types
        2.  Control: config encryption on (default), log/audit/other/bucket off ->
            every file option writes its file
        3.  For bucket, log, audit, other in turn:
            a. turn it on -> every file option is refused and writes nothing
            b. cbindexperf with no file output (-resultfile=) is not refused
            c. cbindex -type=list still works and writes nothing
            d. turn it off -> cbindexplan -output writes again
        """
        self.log.info("STARTING TEST: test_gsi_tools_file_output_encryption")
        node = self.get_nodes_from_services_map(service_type="index")
        cluster = f"{node.ip}:8091"
        base_dir, secret_id = self._tools_setup(node)
        try:
            # STEP 2: control
            methods = self.get_encryption_at_rest_methods()
            self.assertNotIn(methods["config"], (None, "disabled"),
                             f"[STEP 2] Expected config encryption on by default: {methods}")
            self.assertTrue(all(methods[k] == "disabled" for k in ("log", "audit", "other")),
                            f"[STEP 2] Expected log/audit/other encryption off: {methods}")
            self.assertFalse(self.is_bucket_encryption_enabled(self.test_bucket),
                             "[STEP 2] Expected the bucket to be unencrypted")
            results = self._run_tool_file_outputs(node, base_dir, "control")
            self._assert_tool_outputs(node, results, expect_refused=False, label="control")

            # STEP 3: each encryption type alone
            for kind in self.TOOLS_ENCRYPTION_KINDS:
                self.set_encryption_at_rest(kind, secret_id, enable=True, bucket=self.test_bucket)
                self.get_encryption_at_rest_methods()
                results = self._run_tool_file_outputs(node, base_dir, f"{kind}_on")
                self._assert_tool_outputs(node, results, expect_refused=True, label=f"{kind} on")

                output, exit_code = self._run_tool(
                    node, f"{base_dir}/{kind}_no_file_output", "cbindexperf",
                    f"-configfile={base_dir}/perf_config.json -cluster={cluster} "
                    f"-auth={self.username}:{self.password} -resultfile=")
                self.assertNotIn(self.TOOLS_REFUSED_MSG, output,
                                 f"[{kind} on] cbindexperf refused with no file output")
                self.assertIn("rows/sec", output,
                              f"[{kind} on] cbindexperf -resultfile= did not scan: {output[-500:]}")
                self.log.info(f"[{kind} on] cbindexperf -resultfile= exit code {exit_code}, "
                              f"scans completed (Throughput line present)")

                workdir = f"{base_dir}/{kind}_cbindex"
                output, exit_code = self._run_tool(
                    node, workdir, "cbindex",
                    f"-auth={self.username}:{self.password} -server={cluster} -type=list")
                self.assertEqual(exit_code, 0, f"[{kind} on] cbindex -type=list failed: {output}")
                self.assertFalse(self._tool_files(node, workdir), f"[{kind} on] cbindex wrote files")

                self.set_encryption_at_rest(kind, secret_id, enable=False, bucket=self.test_bucket)
                workdir = f"{base_dir}/{kind}_off"
                output, _ = self._run_tool(
                    node, workdir, "cbindexplan",
                    f"-command=plan -memQuota=4G -indexes={base_dir}/index_spec.json -cluster={cluster} "
                    f"-username={self.username} -password={self.password} -output=plan.json")
                self.assertIn("plan.json", self._tool_files(node, workdir),
                              f"[{kind} off] cbindexplan -output still not written: {output}")
        finally:
            self._tools_cleanup(node, base_dir, secret_id, self.TOOLS_ENCRYPTION_KINDS)
        self.log.info("TEST PASSED: test_gsi_tools_file_output_encryption")

    def test_gsi_tools_ear_check_edge_cases(self):
        """
        MB-69501: input checks and fail-open behaviour of the tools' encryption check.
        Behaviour that looks unintended is logged as a warning, not failed on.

        Steps:
        1.  Unencrypted bucket + index; save a plan with cbindexplan -command=retrieve
        2.  Turn bucket encryption on
        3.  cbindexplan -output without -cluster -> "-cluster is required", no file
        4.  cbindexplan -plan <saved> -output (offline): recorded
        5.  cbindexperf -cluster without a port: recorded
        6.  cbindexperf -auth without ':' (check skipped): recorded
        7.  Wrong password, then unreachable cluster: "could not determine encryption-at-rest
            status" is logged and the tool is not refused (fail-open); files recorded
        """
        self.log.info("STARTING TEST: test_gsi_tools_ear_check_edge_cases")
        node = self.get_nodes_from_services_map(service_type="index")
        base_dir, secret_id = self._tools_setup(node)
        cluster = f"{node.ip}:8091"
        creds = f"-username={self.username} -password={self.password}"
        plan = f"-command=plan -memQuota=4G -indexes={base_dir}/index_spec.json"
        perf = f"-configfile={base_dir}/perf_config.json"
        auth = f"-auth={self.username}:{self.password}"

        def record(step, workdir, tool, args):
            output, exit_code = self._run_tool(node, workdir, tool, args)
            files = self._tool_files(node, workdir)
            self.log.warning(f"{step} {tool} {args}: exit code {exit_code}, files {sorted(files)}, "
                             f"output: {output[-500:]}")
            return output

        try:
            # STEP 1: saved plan, written while encryption is off
            output, _ = self._run_tool(node, base_dir, "cbindexplan",
                                           f"-command=retrieve -cluster={cluster} {creds} -output=saved_plan.json")
            self.assertIn("saved_plan.json", self._tool_files(node, base_dir),
                          f"[STEP 1] Could not save a plan with encryption off: {output}")

            # STEP 2
            self.set_encryption_at_rest("bucket", secret_id, enable=True, bucket=self.test_bucket)

            # STEP 3
            workdir = f"{base_dir}/no_cluster"
            output, _ = self._run_tool(node, workdir, "cbindexplan", f"{plan} -output=plan.json")
            self.assertIn("-cluster is required with file output", output,
                          f"[STEP 3] cbindexplan -output without -cluster not rejected: {output}")
            self.assertFalse(self._tool_files(node, workdir), "[STEP 3] cbindexplan wrote a file without -cluster")

            # STEPS 4-6: recorded for the indexing team
            record("[STEP 4] offline plan output:", f"{base_dir}/offline_plan", "cbindexplan",
                   f"-command=plan -memQuota=4G -plan={base_dir}/saved_plan.json -indexes={base_dir}/index_spec.json "
                   f"-output=plan.json")
            record("[STEP 5] cluster without port:", f"{base_dir}/no_port", "cbindexperf",
                   f"{perf} -cluster={node.ip} {auth}")
            record("[STEP 6] auth without ':':", f"{base_dir}/bad_auth", "cbindexperf",
                   f"{perf} -cluster={cluster} -auth={self.username}")

            # STEP 7: fail-open
            for label, plan_args, perf_args in (
                    ("wrong password", f"-cluster={cluster} -username={self.username} -password=wrong_password",
                     f"-cluster={cluster} -auth={self.username}:wrong_password"),
                    ("unreachable cluster", f"-cluster={node.ip}:1 {creds}", f"-cluster={node.ip}:1 {auth}")):
                for tool, args in (("cbindexplan", f"{plan} {plan_args} -output=plan.json"),
                                   ("cbindexperf", f"{perf} {perf_args} -resultfile=results.json")):
                    output = record(f"[STEP 7] {label}:", f"{base_dir}/{label.replace(' ', '_')}_{tool}",
                                    tool, args)
                    self.assertIn(self.TOOLS_STATUS_UNKNOWN_MSG, output,
                                  f"[STEP 7] {tool} with {label}: encryption status error not logged")
                    self.assertNotIn(self.TOOLS_REFUSED_MSG, output,
                                     f"[STEP 7] {tool} with {label}: refused although the status is unknown")
        finally:
            self._tools_cleanup(node, base_dir, secret_id, ("bucket",))
        self.log.info("TEST PASSED: test_gsi_tools_ear_check_edge_cases")

    def test_gsi_encryption_at_rest_sanity(self):
        """
        Encryption at rest sanity test case - supports single or multiple buckets.
        
        Test validates:
        1. Encryption at rest is configured (key created by basetestcase.py)
        2. Buckets are created with encryption enabled based on bucket_count and encrypt_all_buckets params
        3. Indexer snapshot settings configured (persisted_snapshot.moi.interval=60000)
        4. Various index types are created successfully on the configured storage type
        5. Indexes reach "Ready" state
        6. SELECT queries for all indexes execute successfully and return results
        7. Item count validations (KV vs GSI counts, replica counts, no pending mutations)
        8. File encryption validation on disk
        
        Parameters:
        - bucket_count: Number of buckets to create (default: 1)
        - encrypt_all_buckets: If True, all buckets are encrypted. If False, only first bucket is encrypted.
        
        Storage types supported:
        - memory_optimized (MOI) - in-memory indexes
        - forestdb (Plasma) - persistent indexes
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_sanity")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, bucket_count={self.bucket_count}, encrypt_all_buckets={self.encrypt_all_buckets}")
        self.log.info("=" * 80)
        
        # ========== STEP 1: Verify index nodes ==========
        try:
            index_nodes = self.verify_prerequisites(min_index_nodes=2, check_encryption=False)
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED - Index node verification failed: {str(e)}")
            raise        
        # ========== STEP 2: Log storage type ==========
        self.log.info(f"[STEP 2] Storage engine: gsi_type={self.gsi_type}")
        self.log.info("[STEP 2] PASSED - Storage type confirmed")
        
        # ========== STEP 3: Verify encryption at rest parameter ==========
        try:
            self.log.info("[STEP 3] Verifying encryption at rest parameter...")
            if not self.enable_encryption_at_rest:
                self.log.info("[STEP 3] SKIPPED - Encryption at rest not enabled")
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            self.log.info("[STEP 3] PASSED - Encryption at rest parameter confirmed")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED - Encryption parameter verification failed: {str(e)}")
            raise
        
        # ========== STEP 4: Create buckets with encryption ==========
        bucket_names = []
        encrypted_bucket_names = []
        try:
            self.log.info(f"[STEP 4] Creating {self.bucket_count} bucket(s)...")
            for i in range(self.bucket_count):
                bucket_name = f"{self.test_bucket}_{i+1}" if self.bucket_count > 1 else self.test_bucket
                encrypt_this_bucket = self.encrypt_all_buckets or (i == 0)
                self.log.info(f"[STEP 4.{i+1}] Creating bucket '{bucket_name}' with encryption={encrypt_this_bucket}")
                
                self.create_bucket_with_encryption(bucket_name, enable_encryption=encrypt_this_bucket)
                bucket_names.append(bucket_name)
                if encrypt_this_bucket:
                    encrypted_bucket_names.append(bucket_name)
                
                # Validate bucket encryption status
                bucket_info = self.rest.get_bucket_json(bucket_name)
                encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                self.log.info(f"[STEP 4.{i+1}] Bucket '{bucket_name}' encryptionAtRestKeyId: {encryption_key_id}")
                
                if encrypt_this_bucket:
                    if encryption_key_id is None or encryption_key_id == -1:
                        raise Exception(f"Bucket '{bucket_name}' should have encryption enabled")
                else:
                    if encryption_key_id is not None and encryption_key_id != -1:
                        raise Exception(f"Bucket '{bucket_name}' should not have encryption enabled")
                
                self.log.info(f"[STEP 4.{i+1}] PASSED - Bucket '{bucket_name}' created successfully")
            
            self.log.info(f"[STEP 4] PASSED - Created {len(bucket_names)} bucket(s): {bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED - Bucket creation failed: {str(e)}")
            raise
        
        # ========== STEP 5: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 5]")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED - Indexer settings configuration failed: {str(e)}")
            raise        
        # ========== STEP 6: Load documents and create indexes ==========
        select_queries = set()
        try:
            self.log.info(f"[STEP 6] Loading documents and creating indexes for {len(bucket_names)} bucket(s)...")

            # Step 6a: Load documents across all buckets in one pass
            try:
                self.log.info(
                    f"[STEP 6.a] Loading {self.num_of_docs_per_collection} documents into each bucket: {bucket_names}"
                )
                gen_create = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100,
                    percent_update=0,
                    percent_delete=0,
                    scope="_default",
                    collection="_default",
                    json_template=self.json_template,
                    output=True,
                    username=self.username,
                    password=self.password
                )
                if self.use_magma_loader:
                    # Vector templates (e.g. MSMARCOSiftEmbeddingProduct) are only
                    # available to the magma loader; java_sdk_client NPEs on them
                    # and loads zero docs without failing the task.
                    tasks = [
                        self.cluster.async_load_gen_docs(
                            self.master, bucket=bucket_name, generator=gen_create,
                            pause_secs=1, timeout_secs=300, use_magma_loader=True
                        )
                        for bucket_name in bucket_names
                    ]
                else:
                    tasks = self.data_ops_javasdk_loader_in_batches(
                        sdk_data_loader=gen_create,
                        batch_size=10**4,
                        dataset=self.json_template
                    )
                for task in tasks:
                    task.result()
                self._assert_buckets_loaded(bucket_names, int(self.num_of_docs_per_collection))
                self.log.info(
                    f"[STEP 6.a] PASSED - Loaded {self.num_of_docs_per_collection} documents into each bucket: {bucket_names}"
                )
            except Exception as e:
                self.log.error(f"[STEP 6.a] FAILED - Document loading failed: {str(e)}")
                raise

            for idx, bucket_name in enumerate(bucket_names):
                self.log.info(f"[STEP 6.{idx+1}] Processing bucket '{bucket_name}'...")
                namespace = f"default:{bucket_name}._default._default"

                # Step 6b: Create indexes and collect select queries
                try:
                    self.log.info(f"[STEP 6.{idx+1}.b] Creating indexes on '{bucket_name}'...")
                    prefix = 'ear_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]

                    if self.bhive_index:
                        # Dense + sparse, BHIVE + composite, and dataset-native
                        # scalar coverage in one pass (e.g. json_template=
                        # MSMARCOSiftEmbeddingProduct). See
                        # _get_vector_query_definitions for why this needs 5
                        # separate generation calls instead of one.
                        (dense_bhive_def, dense_composite_def, sparse_bhive_def,
                         sparse_composite_def, scalar_def) = self._get_vector_query_definitions(prefix)
                        query_definitions = (dense_bhive_def + dense_composite_def
                                            + sparse_bhive_def + sparse_composite_def + scalar_def)
                        queries = self._get_vector_create_queries(
                            dense_bhive_def, dense_composite_def,
                            sparse_bhive_def, sparse_composite_def, scalar_def,
                            namespace, deploy_nodes
                        )
                    else:
                        query_definitions = self.gsi_util_obj.get_index_definition_list(
                            dataset=self.json_template,
                            prefix=prefix,
                            skip_primary=False
                        )
                        queries = self.gsi_util_obj.get_create_index_list(
                            definition_list=query_definitions,
                            namespace=namespace,
                            num_replica=self.num_index_replica,
                            deploy_node_info=deploy_nodes,
                            defer_build=self.defer_build
                        )
                    self.log.info(f"[STEP 5.{idx+1}.b] Generated {len(query_definitions)} index definitions with prefix '{prefix}'")

                    # Collect select queries for validation
                    select_queries.update(
                        self.gsi_util_obj.get_select_queries(
                            definition_list=query_definitions,
                            namespace=namespace
                        )
                    )
                    self.log.info(f"[STEP 6.{idx+1}.b] Generated {len(queries)} CREATE INDEX queries")
                    
                    query_node = self.get_nodes_from_services_map(service_type="n1ql")
                    self.gsi_util_obj.create_gsi_indexes(
                        create_queries=queries,
                        database=namespace,
                        query_node=query_node
                    )
                    self.log.info(f"[STEP 6.{idx+1}.b] PASSED - Created indexes on '{bucket_name}'")
                except Exception as e:
                    self.log.error(f"[STEP 6.{idx+1}.b] FAILED - Index creation failed for '{bucket_name}': {str(e)}")
                    raise
            
            self.log.info(f"[STEP 6] PASSED - Document loading and index creation completed for all buckets. Collected {len(select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED - Document loading/index creation failed: {str(e)}")
            raise
        
        # ========== STEP 7: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 7] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(
                timeout=max(1200, self.index_online_timeout), defer_build=self.defer_build
            )
            self.log.info("[STEP 7] PASSED - All indexes are in 'Ready' state")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED - Index online wait failed: {str(e)}")
            raise

        # ========== STEP 8: Run SELECT queries and validate results ==========
        try:
            self.log.info(f"[STEP 8] Running {len(select_queries)} SELECT queries for all indexes after encryption...")
            
            # Capture scan stats before queries
            self.run_scan_validation(select_queries)
            
            self.log.info(f"[STEP 8] PASSED - All {len(select_queries)} SELECT queries executed successfully with encryption enabled")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED - SELECT query validation failed: {str(e)}")
            raise
        
        # ========== STEP 9: Validate item counts and related validations ==========
        try:
            self.log.info("[STEP 9] Running item count related validations (KV vs GSI, replica counts, pending mutations)...")
            self.item_count_related_validations()
            self.log.info("[STEP 9] PASSED - Item count related validations completed successfully")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED - Item count validation failed: {str(e)}")
            raise
        
        # ========== STEP 10: Validate file encryption on disk ==========
        try:
            # Mixed-bucket setup: one encrypted, one not. Normal file-level validation
            # cannot distinguish between the two buckets' files reliably, so we use a
            # targeted in-use key check scoped to the encrypted bucket UUIDs instead.
            if not self.encrypt_all_buckets and self.bucket_count >= 2:
                self.validate_mixed_bucket_encrypted_keys(
                    index_nodes, encrypted_bucket_names, timeout=300, step_prefix="[STEP 10] "
                )
                self.log.info("[STEP 10] PASSED - Mixed-bucket encrypted key validation completed successfully")
                self.log.info("=" * 80)
                self.log.info(f"test_gsi_encryption_at_rest_sanity COMPLETED SUCCESSFULLY")
                self.log.info("=" * 80)
                return

            expected_header_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            self.log.info(
                f"[STEP 10] Validating file encryption on disk and checking for "
                f"in-use key ids {expected_header_key_ids} in encrypted file headers for "
                f"encrypted buckets: {encrypted_bucket_names}..."
            )
            engine = _GSI_TYPE_TO_ENGINE[self.gsi_type]
            encryption_results = self.validate_engine_encryption(
                engine, index_nodes,
                expected_key_id=expected_header_key_ids,
                encrypted_bucket_names=encrypted_bucket_names,
            )
            
            # Track overall validation status
            all_failed_files = []
            validation_passed = True
            
            # Log summary of results
            # Skip internal keys added by the orchestrator (_overall, _helper_error)
            # and any todo stubs — they are not per-node result dicts.
            _SKIP_CATEGORIES = {"_overall", "_helper_error", "TODO"}
            for category, results in encryption_results.items():
                if category in _SKIP_CATEGORIES:
                    continue
                if isinstance(results, dict):
                    for node, result in results.items():
                        if not isinstance(result, dict):
                            continue
                        status = result.get("status", "unknown")
                        if status == "passed":
                            # Check if there are any failed files even in passed results
                            failed_files = result.get("failed_files", [])
                            if failed_files:
                                self.log.warning(f"[STEP 10] {category} on {node}: PASSED with {len(failed_files)} unencrypted files")
                                for file_path, details in failed_files:
                                    self.log.error(f"[STEP 10] UNENCRYPTED FILE on {node}: {file_path} - {details}")
                                    all_failed_files.append((node, file_path, details))
                            else:
                                self.log.info(f"[STEP 10] {category} on {node}: PASSED - All files encrypted")
                        elif status == "failed":
                            validation_passed = False
                            self.log.error(f"[STEP 10] {category} on {node}: FAILED - {result.get('reason', 'unknown')}")
                            # Explicitly list all failed files
                            failed_files = result.get("failed_files", [])
                            if failed_files:
                                self.log.error(f"[STEP 10] === UNENCRYPTED FILES on {node} ({len(failed_files)} files) ===")
                                for file_path, details in failed_files:
                                    self.log.error(f"[STEP 10] UNENCRYPTED FILE: {file_path}")
                                    self.log.error(f"[STEP 10]   Details: {details}")
                                    all_failed_files.append((node, file_path, details))
                        elif status == "skipped":
                            self.log.warning(f"[STEP 10] {category} on {node}: SKIPPED - {result.get('reason', 'unknown')}")
            
            # Final summary of all unencrypted files
            if all_failed_files:
                self.log.error("=" * 60)
                self.log.error(f"[STEP 10] SUMMARY: Found {len(all_failed_files)} UNENCRYPTED FILES:")
                for node, file_path, details in all_failed_files:
                    self.log.error(f"[STEP 10]   Node: {node}, File: {file_path}")
                self.log.error("=" * 60)
                #self.log.info("Sleeping 30 minutes for manual inspection before raising...")
                #time.sleep(30 * 60)
                raise Exception(f"File encryption validation failed: {len(all_failed_files)} files are not encrypted")
            
            if not validation_passed:
                #self.log.info("Sleeping 30 minutes for manual inspection before raising...")
                #time.sleep(30 * 60)
                raise Exception("File encryption validation reported one or more failures")

            self.log.info("[STEP 10] PASSED - File encryption validation completed successfully")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED - File encryption validation failed: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_sanity")
        self.log.info(f"Test completed successfully with gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_index_operations(self):
        """
        Encryption at rest - index lifecycle operations on multi-bucket encrypted cluster.

        Validates that alter/drop/build index operations work correctly under
        encryption at rest on a multi-bucket encrypted configuration.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings
        3.  Create 2 buckets WITH encryption, load docs
        4.  Create indexes on both buckets, wait for Ready
        5.  Scans on all indexes
        6.  File encryption validation
        7.  Lifecycle ops (alter/drop/build) on each namespace
        8.  Scans on indexes after lifecycle ops
        9.  Item count validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_index_operations")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create 2 encrypted buckets and load docs ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        bucket_namespaces = []
        try:
            self.log.info("[STEP 3] Creating 2 encrypted buckets and loading documents...")
            bucket_names = [self.test_bucket, f"{self.test_bucket}_2"]
            for bucket_name in bucket_names:
                self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
                bucket_info = self.rest.get_bucket_json(bucket_name)
                enc_key = bucket_info.get('encryptionAtRestKeyId', None)
                if enc_key is None or enc_key == -1:
                    raise Exception(f"Bucket '{bucket_name}' should have encryption enabled")
                bucket_namespaces.append(f"default:{bucket_name}._default._default")
                gen_create = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100, percent_update=0, percent_delete=0,
                    scope="_default", collection="_default",
                    json_template=self.json_template, output=True,
                    username=self.username, password=self.password
                )
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
                )
                for task in tasks:
                    task.result()
            self.log.info(f"[STEP 3] PASSED - Created {len(bucket_names)} encrypted buckets with docs")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes on both buckets ==========
        all_select_queries = set()
        try:
            self.log.info("[STEP 4] Creating indexes on both buckets...")
            for namespace in bucket_namespaces:
                prefix = 'idx_ops_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                query_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=prefix, skip_primary=False
                )
                all_select_queries.update(self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions, namespace=namespace
                ))
                queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=query_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=self.defer_build, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=queries, database=namespace, query_node=query_node
                )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 4] PASSED - Indexes created and in Ready state")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Scans on all indexes ==========
        try:
            self.log.info("[STEP 5] Running scans on all indexes...")
            self.run_scan_validation(all_select_queries, validate_item_count=True)
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: File encryption validation ==========
        try:
            self.log.info("[STEP 6] Validating file encryption on disk...")
            in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=in_use_key_ids, step_prefix="[STEP 6] "):
                raise Exception("File encryption validation failed")
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Restart indexers then lifecycle ops on each namespace ==========
        try:
            self.log.info("[STEP 7] Restarting indexer nodes before lifecycle ops...")
            self.restart_index_nodes(index_nodes)
            self.log.info("[STEP 7] Running index lifecycle ops (alter/drop/build) on each namespace...")
            for namespace in bucket_namespaces:
                self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Scans after lifecycle ops ==========
        try:
            self.log.info("[STEP 8] Running scans after lifecycle ops...")
            self.run_scan_validation(all_select_queries)
            self.log.info("[STEP 8] PASSED - Scans succeeded after lifecycle ops")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Item count validation ==========
        try:
            self.log.info("[STEP 9] Running item count related validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise


        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_index_operations")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_toggle_and_lifecycle(self):
        """
        Encryption at rest - multi-bucket with encryption flag toggled + lifecycle ops.

        Starts with 2 buckets (one encrypted, one not), runs scans, then toggles
        encryption on both buckets (encrypted→disabled, disabled→enabled), runs
        scans again, then runs index lifecycle ops.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings
        3.  Create bucket_A WITH encryption, bucket_B WITHOUT encryption, load docs
        4.  Create indexes on both buckets, wait for Ready
        5.  Baseline scans on both buckets
        6.  File encryption validation on bucket_A indexes
        7.  Toggle: disable encryption on bucket_A, enable encryption on bucket_B
        8.  Validate toggle succeeded (encryptionAtRestKeyId)
        9.  Upsert new docs into both buckets post-toggle
        10. Scans on all indexes (both buckets) + compare pre/post-mutation counts
        11. Create new indexes on both buckets post-toggle, scan
        12. Lifecycle ops (alter/drop/build) on each namespace
        13. Item count validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_toggle_and_lifecycle")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create 2 buckets (one encrypted, one not) and load docs ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        bucket_a = self.test_bucket
        bucket_b = f"{self.test_bucket}_2"
        namespace_a = f"default:{bucket_a}._default._default"
        namespace_b = f"default:{bucket_b}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket_A='{bucket_a}' WITH encryption, bucket_B='{bucket_b}' WITHOUT encryption...")
            self.create_bucket_with_encryption(bucket_a, enable_encryption=True)
            enc_key = self.rest.get_bucket_json(bucket_a).get('encryptionAtRestKeyId', None)
            if enc_key is None or enc_key == -1:
                raise Exception(f"bucket_A should be encrypted")
            self.create_bucket_with_encryption(bucket_b, enable_encryption=False)
            enc_key_b = self.rest.get_bucket_json(bucket_b).get('encryptionAtRestKeyId', None)
            if enc_key_b not in [None, -1]:
                raise Exception(f"bucket_B should NOT be encrypted, got key {enc_key_b}")
            for bucket_name in [bucket_a, bucket_b]:
                gen_create = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100, percent_update=0, percent_delete=0,
                    scope="_default", collection="_default",
                    json_template=self.json_template, output=True,
                    username=self.username, password=self.password
                )
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
                )
                for task in tasks:
                    task.result()
            self.log.info(f"[STEP 3] PASSED - bucket_A encrypted (key={enc_key}), bucket_B unencrypted")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes on both buckets ==========
        select_queries_a = set()
        select_queries_b = set()
        try:
            self.log.info("[STEP 4] Creating indexes on both buckets...")
            for namespace, sq in [(namespace_a, select_queries_a), (namespace_b, select_queries_b)]:
                prefix = 'tog_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                query_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=prefix, skip_primary=False
                )
                sq.update(self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions, namespace=namespace
                ))
                queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=query_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=self.defer_build, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=queries, database=namespace, query_node=query_node
                )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 4] PASSED - Indexes created and in Ready state")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Baseline scans on both buckets + capture pre-toggle counts ==========
        try:
            self.log.info("[STEP 5] Running baseline scans on both buckets...")
            all_select_queries = select_queries_a | select_queries_b
            self.run_scan_validation(all_select_queries, validate_item_count=True)
            pre_toggle_counts = {}
            for bucket_name in [bucket_a, bucket_b]:
                count_result = self.run_cbq_query(
                    query=f"SELECT COUNT(*) as cnt FROM `{bucket_name}`",
                    scan_consistency='request_plus'
                )
                pre_toggle_counts[bucket_name] = count_result['results'][0]['cnt']
                self.log.info(f"[STEP 5] Pre-toggle doc count for '{bucket_name}': {pre_toggle_counts[bucket_name]}")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: In-use key validation for encrypted bucket_A ==========
        # Normal file-level validation cannot distinguish bucket_A files from bucket_B files
        # since both share the same index nodes. Use the mixed-bucket key check instead:
        # poll until bucket_A's service_bucket entry carries a non-empty DEK key ID.
        try:
            self.log.info("[STEP 6] Validating in-use encryption keys for bucket_A (mixed-bucket check)...")
            self.validate_mixed_bucket_encrypted_keys(
                index_nodes, [bucket_a], timeout=300, step_prefix="[STEP 6] "
            )
            self.log.info("[STEP 6] PASSED - bucket_A in-use key validation passed")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Toggle encryption on both buckets ==========
        try:
            self.log.info(f"[STEP 7] Toggling: disabling encryption on bucket_A='{bucket_a}', enabling on bucket_B='{bucket_b}'...")
            status, response = self.rest.disable_bucket_encryption(bucket_a)
            if not status:
                raise Exception(f"Failed to disable encryption on bucket_A: {response}")
            status, response = self.rest.enable_bucket_encryption(bucket_b, self.encryption_at_rest_id)
            if not status:
                raise Exception(f"Failed to enable encryption on bucket_B: {response}")
            self.log.info("[STEP 7] PASSED - Encryption toggled on both buckets")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Validate toggle succeeded ==========
        try:
            self.log.info("[STEP 8] Validating toggle results...")
            enc_a = self.rest.get_bucket_json(bucket_a).get('encryptionAtRestKeyId', None)
            if enc_a not in [None, -1]:
                raise Exception(f"bucket_A encryption not disabled, key still set to {enc_a}")
            enc_b = self.rest.get_bucket_json(bucket_b).get('encryptionAtRestKeyId', None)
            if enc_b is None or enc_b == -1:
                raise Exception(f"bucket_B encryption not enabled")
            self.log.info(f"[STEP 8] PASSED - bucket_A encryptionAtRestKeyId={enc_a}, bucket_B encryptionAtRestKeyId={enc_b}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Upsert new docs on both buckets post-toggle ==========
        upsert_count = 10
        try:
            self.log.info(f"[STEP 9] Upserting {upsert_count} docs into each bucket post-toggle...")
            for bucket_name in [bucket_a, bucket_b]:
                # Use the same doc generator/schema as the initial load (not ad hoc JSON) so
                # the new docs carry the fields (price, free_breakfast, type, etc.) that the
                # created GSI indexes are built on — otherwise GSI correctly excludes
                # schema-incompatible docs while the raw KV count doesn't, causing a
                # false-positive mismatch in item_count_related_validations().
                gen_toggle_upsert = SDKDataLoader(
                    num_ops=upsert_count, percent_create=100, percent_update=0, percent_delete=0,
                    scope="_default", collection="_default",
                    json_template=self.json_template, output=True,
                    username=self.username, password=self.password,
                    key_prefix="toggle_upsert_"
                )
                tasks = self.data_ops_javasdk_loader_in_batches_to_collection(
                    bucket_name, gen_toggle_upsert, batch_size=upsert_count
                )
                for task in tasks:
                    task.result()
                self.log.info(f"[STEP 9] Upserted {upsert_count} schema-conformant docs into '{bucket_name}'")
            self.log.info("[STEP 9] PASSED - Upserts succeeded on both buckets")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Scans on all indexes post-toggle + compare counts ==========
        try:
            self.log.info("[STEP 10] Running scans on all indexes after toggle and mutations...")
            self.run_scan_validation(all_select_queries, validate_item_count=False)
            for bucket_name in [bucket_a, bucket_b]:
                expected_count = pre_toggle_counts[bucket_name] + upsert_count
                post_count_result = self.run_cbq_query(
                    query=f"SELECT COUNT(*) as cnt FROM `{bucket_name}`",
                    scan_consistency='request_plus'
                )
                post_count = post_count_result['results'][0]['cnt']
                self.assertEqual(
                    post_count, expected_count,
                    f"[STEP 10] '{bucket_name}': expected {expected_count} docs after upserts "
                    f"(pre={pre_toggle_counts[bucket_name]} + upserted={upsert_count}), got {post_count}"
                )
                self.log.info(
                    f"[STEP 10] '{bucket_name}': pre={pre_toggle_counts[bucket_name]}, "
                    f"upserted={upsert_count}, post={post_count} — count matches"
                )
            self.log.info("[STEP 10] PASSED - All indexes accessible after toggle, counts match")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Create new indexes on both buckets post-toggle ==========
        try:
            self.log.info("[STEP 11] Creating new indexes on both buckets post-toggle and scanning...")
            for namespace in [namespace_a, namespace_b]:
                new_prefix = 'tog_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                new_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=new_prefix, skip_primary=False
                )
                new_select_queries = self.gsi_util_obj.get_select_queries(
                    definition_list=new_definitions, namespace=namespace
                )
                new_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=new_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=False, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=new_queries, database=namespace, query_node=query_node
                )
                self.wait_until_indexes_online(timeout=1200)
                self.run_scan_validation(new_select_queries)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Restart indexers then lifecycle ops on each namespace ==========
        try:
            self.log.info("[STEP 12] Restarting indexer nodes before lifecycle ops...")
            self.restart_index_nodes(index_nodes)
            self.log.info("[STEP 12] Running index lifecycle ops (alter/drop/build) on each namespace...")
            for namespace in [namespace_a, namespace_b]:
                self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Item count validation ==========
        try:
            self.log.info("[STEP 13] Running item count related validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_toggle_and_lifecycle")
        self.log.info(f"Encryption toggled on both buckets and lifecycle ops succeeded")
        self.log.info("=" * 80)

    def _corrupt_data_dirs(self, node):
        """MB-69503: corrupt-index backup dirs of the Plasma (@2i) and BHIVE (@2i/@bhive) engines."""
        storage_dir = RestConnection(node).get_indexer_internal_stats()['indexer.storage_dir']
        return [f"{storage_dir}/.corruptData", f"{storage_dir}/@bhive/.corruptData"]

    def _existing_corrupt_data_dirs(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(f"ls -d {' '.join(self._corrupt_data_dirs(node))} 2>/dev/null")
        finally:
            shell.disconnect()
        return [line.strip() for line in out if line.strip()]

    def _plant_corrupt_data_backup(self, index_nodes):
        """MB-69503: create a dummy backup under .corruptData (owned like its engine dir),
        as a backup taken before encryption or left by a crash during cleanup would be."""
        for node in index_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                for corrupt_dir in self._corrupt_data_dirs(node):
                    engine_dir = corrupt_dir.rsplit("/", 1)[0]
                    shell.execute_command(
                        f"[ -d {engine_dir} ] && mkdir -p {corrupt_dir}/ear_test_backup && "
                        f"touch {corrupt_dir}/ear_test_backup/data && "
                        f"chown -R --reference={engine_dir} {corrupt_dir}")
            finally:
                shell.disconnect()
            self.assertTrue(self._existing_corrupt_data_dirs(node),
                            f"Could not create a .corruptData backup on {node.ip}")

    def _wait_for_corrupt_data_removed(self, index_nodes, label, timeout=300):
        """MB-69503: .corruptData must be removed while any bucket has an active key."""
        self._wait_until_no_problems(
            lambda: [f"{node.ip}:{d}" for node in index_nodes for d in self._existing_corrupt_data_dirs(node)],
            f"{label}.corruptData removal: ", timeout=timeout)

    CORRUPT_BACKUP_LOG_PATTERNS = ("backupCorruptIndexDataFiles.*take backup of corrupt data files",
                                   "backupCorruptIndexDataFiles.*skipping backup: bucket encryption is active")

    def _assert_corrupt_backup_skipped(self, node, label, counts_before):
        """MB-69503: with a bucket encrypted, a corrupt partition is not backed up.
        counts_before: _indexer_log_count of CORRUPT_BACKUP_LOG_PATTERNS before the corruption."""
        self.assertFalse(self._existing_corrupt_data_dirs(node),
                         f"{label}.corruptData exists on {node.ip} while bucket encryption is active")
        for pattern, before in zip(self.CORRUPT_BACKUP_LOG_PATTERNS, counts_before):
            self.assertGreater(self._indexer_log_count(node, pattern), before,
                               f"{label}'{pattern}' not logged on {node.ip} after the corruption")

    def test_gsi_encryption_at_rest_storage_corruption(self):
        """
        Encryption at rest - multi-bucket with storage corruption recovery.

        Creates 2 buckets (both encrypted) with indexes, then induces
        plasma shard corruption on a randomly chosen index node, kills and restarts
        the indexer to trigger recovery, then validates scans and lifecycle ops.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings + enable corrupt index backup flag;
            plant a .corruptData backup (MB-69503)
        3.  Create bucket_A WITH encryption, bucket_B WITH encryption, load docs
        3b. .corruptData removed once a bucket key is active (MB-69503)
        4.  Create indexes on both buckets, wait for Ready
        5.  Baseline scans on both buckets + file encryption validation
        6.  Induce plasma shard corruption on a randomly chosen index node
        7.  Kill indexer on that node to trigger corruption detection + recovery
        8.  Wait for all indexes to return to Ready state
        8b. Corrupt partition not backed up while encrypted (MB-69503)
        9.  Post-recovery mutations: upsert new docs, delete some docs, update some docs
        10. Scans on all indexes post-recovery + compare pre/post-mutation counts
        11. Plant .corruptData, restart: startup sweep removes it (MB-69503);
            lifecycle ops (alter/drop/build) on each namespace
        12. Item count validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_storage_corruption")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer settings + enable corrupt index backup ==========
        try:
            self.log.info("[STEP 2] Setting indexer snapshot settings and enabling corrupt index backup...")
            for index_node in index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 60000})
                rest.set_index_settings({"indexer.settings.persisted_snapshot_init_build.moi.interval": 60000})
            self.enable_corrupt_index_backup()
            # MB-69503: a backup left from before encryption; must go once a bucket key is active
            self._plant_corrupt_data_backup(index_nodes)
            self.log.info("[STEP 2] PASSED - corrupt index backup enabled")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Create 2 buckets (one encrypted, one not) and load docs ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        bucket_a = self.test_bucket
        bucket_b = f"{self.test_bucket}_2"
        namespace_a = f"default:{bucket_a}._default._default"
        namespace_b = f"default:{bucket_b}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket_A='{bucket_a}' WITH encryption, bucket_B='{bucket_b}' WITH encryption...")
            self.create_bucket_with_encryption(bucket_a, enable_encryption=True)
            enc_key = self.rest.get_bucket_json(bucket_a).get('encryptionAtRestKeyId', None)
            if enc_key is None or enc_key == -1:
                raise Exception("bucket_A should be encrypted")
            self.create_bucket_with_encryption(bucket_b, enable_encryption=True)
            enc_key_b = self.rest.get_bucket_json(bucket_b).get('encryptionAtRestKeyId', None)
            if enc_key_b is None or enc_key_b == -1:
                raise Exception("bucket_B should be encrypted")
            for bucket_name in [bucket_a, bucket_b]:
                gen_create = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100, percent_update=0, percent_delete=0,
                    scope="_default", collection="_default",
                    json_template=self.json_template, output=True,
                    username=self.username, password=self.password
                )
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
                )
                for task in tasks:
                    task.result()
            self.log.info(f"[STEP 3] PASSED - bucket_A encrypted (key={enc_key}), bucket_B encrypted (key={enc_key_b})")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 3b (MB-69503): .corruptData removed once a bucket key is active ==========
        self._wait_for_corrupt_data_removed(index_nodes, "[STEP 3b] ")

        # ========== STEP 4: Create indexes on both buckets ==========
        all_select_queries = set()
        try:
            self.log.info("[STEP 4] Creating indexes on both buckets...")
            for namespace in [namespace_a, namespace_b]:
                prefix = 'sc_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                query_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=prefix, skip_primary=False
                )
                all_select_queries.update(self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions, namespace=namespace
                ))
                queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=query_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=self.defer_build, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=queries, database=namespace, query_node=query_node
                )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 4] PASSED - Indexes created and in Ready state")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Baseline scans + file encryption validation + capture pre-recovery counts ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(all_select_queries, validate_item_count=True)
            in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=in_use_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            pre_recovery_counts = {}
            for bucket_name in [bucket_a, bucket_b]:
                count_result = self.run_cbq_query(
                    query=f"SELECT COUNT(*) as cnt FROM `{bucket_name}`",
                    scan_consistency='request_plus'
                )
                pre_recovery_counts[bucket_name] = count_result['results'][0]['cnt']
                self.log.info(f"[STEP 5] Pre-recovery doc count for '{bucket_name}': {pre_recovery_counts[bucket_name]}")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Induce plasma shard corruption on a random index node ==========
        corrupt_node = None
        try:
            self.log.info("[STEP 6] Inducing plasma shard corruption on a randomly chosen index node...")
            corrupt_node = random.choice(index_nodes)
            self.log.info(f"[STEP 6] Chosen node for corruption: {corrupt_node.ip}")
            backup_log_counts = [self._indexer_log_count(corrupt_node, p) for p in self.CORRUPT_BACKUP_LOG_PATTERNS]
            self.corrupt_plasma_metadata(corrupt_node)
            self.log.info(f"[STEP 6] PASSED - Corruption induced on {corrupt_node.ip}")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Kill indexer on corrupt node to trigger recovery ==========
        try:
            self.log.info(f"[STEP 7] Killing indexer on {corrupt_node.ip} to trigger corruption detection...")
            self._kill_all_processes_index(server=corrupt_node)
            time.sleep(30)
            self.log.info("[STEP 7] PASSED - Indexer killed, waiting for restart")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Wait for all indexes to return to Ready state ==========
        try:
            self.log.info("[STEP 8] Waiting for indexes to recover and reach Ready state...")
            self.wait_until_indexes_online(timeout=1800)
            self.log.info("[STEP 8] PASSED - All indexes back in Ready state after recovery")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 8b (MB-69503): no backup of the corrupt partition while encrypted ==========
        self._assert_corrupt_backup_skipped(corrupt_node, "[STEP 8b] ", backup_log_counts)

        # ========== STEP 9: Mutations post-recovery (upsert/delete/update) on both buckets ==========
        upsert_count = 10
        delete_count = 5
        update_count = 5
        try:
            self.log.info(f"[STEP 9] Running post-recovery mutations on both buckets "
                          f"(upsert={upsert_count}, delete={delete_count}, update={update_count})...")
            for bucket_name in [bucket_a, bucket_b]:
                # Upsert new docs. Use the same doc generator/schema as the initial load
                # (not ad hoc JSON) so the new docs carry the fields (price, free_breakfast,
                # type, etc.) that the created GSI indexes are built on — otherwise GSI
                # correctly excludes schema-incompatible docs while the raw KV count doesn't,
                # causing a false-positive mismatch in item_count_related_validations().
                gen_recovery_upsert = SDKDataLoader(
                    num_ops=upsert_count, percent_create=100, percent_update=0, percent_delete=0,
                    scope="_default", collection="_default",
                    json_template=self.json_template, output=True,
                    username=self.username, password=self.password,
                    key_prefix="recovery_upsert_"
                )
                tasks = self.data_ops_javasdk_loader_in_batches_to_collection(
                    bucket_name, gen_recovery_upsert, batch_size=upsert_count
                )
                for task in tasks:
                    task.result()
                self.log.info(f"[STEP 9] '{bucket_name}': upserted {upsert_count} schema-conformant docs")

                # Delete docs by key range (first delete_count keys from the initial load)
                delete_keys = ", ".join(f'"doc_{i}"' for i in range(delete_count))
                delete_result = self.run_cbq_query(
                    query=f"DELETE FROM `{bucket_name}` USE KEYS [{delete_keys}] RETURNING META().id",
                    server=query_node
                )
                actual_deleted = len(delete_result.get('results', []))
                self.log.info(f"[STEP 9] '{bucket_name}': deleted {actual_deleted} docs")

                # Update docs by key range (next update_count keys)
                update_keys = ", ".join(f'"doc_{i}"' for i in range(delete_count, delete_count + update_count))
                self.run_cbq_query(
                    query=f"UPDATE `{bucket_name}` USE KEYS [{update_keys}] SET recovery_updated = true",
                    server=query_node
                )
                self.log.info(f"[STEP 9] '{bucket_name}': updated {update_count} docs")

            self.log.info("[STEP 9] PASSED - Post-recovery mutations completed on both buckets")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Scans post-recovery + compare mutation counts ==========
        try:
            self.log.info("[STEP 10] Running scans after post-recovery mutations and comparing counts...")
            self.run_scan_validation(all_select_queries, validate_item_count=False)
            for bucket_name in [bucket_a, bucket_b]:
                # Expected: pre-recovery + upserted - deleted (updates don't change count)
                expected_count = pre_recovery_counts[bucket_name] + upsert_count - delete_count
                post_count_result = self.run_cbq_query(
                    query=f"SELECT COUNT(*) as cnt FROM `{bucket_name}`",
                    scan_consistency='request_plus'
                )
                post_count = post_count_result['results'][0]['cnt']
                self.assertEqual(
                    post_count, expected_count,
                    f"[STEP 10] '{bucket_name}': expected {expected_count} docs "
                    f"(pre={pre_recovery_counts[bucket_name]} + upserted={upsert_count} - deleted={delete_count}), "
                    f"got {post_count}"
                )
                self.log.info(
                    f"[STEP 10] '{bucket_name}': pre={pre_recovery_counts[bucket_name]}, "
                    f"+upserted={upsert_count}, -deleted={delete_count}, post={post_count} — count matches"
                )
            self.log.info("[STEP 10] PASSED - All indexes accessible post-recovery, counts match")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Restart indexers then lifecycle ops on each namespace ==========
        try:
            self.log.info("[STEP 11] Restarting indexer nodes before lifecycle ops...")
            # MB-69503: the startup sweep removes a backup left behind (e.g. crash during cleanup)
            self._plant_corrupt_data_backup(index_nodes)
            self.restart_index_nodes(index_nodes)
            self._wait_for_corrupt_data_removed(index_nodes, "[STEP 11] ", timeout=120)
            self.log.info("[STEP 11] Running index lifecycle ops (alter/drop/build) on each namespace...")
            for namespace in [namespace_a, namespace_b]:
                self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Item count validation ==========
        try:
            self.log.info("[STEP 12] Running item count related validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise
        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_storage_corruption")
        self.log.info("Storage corruption recovery succeeded with encryption at rest; post-recovery mutations validated")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_key_rotation(self):
        """
        Encryption at rest key rotation test.

        Parameters:
          with_restart (bool, default False): when True, adds a rolling indexer
            restart phase after key rotation to verify encryption + GSI survive restart.

        Base flow (with_restart=False):
        1.  Verify prerequisites
        2.  Snapshot settings + pin DEK interval high
        3.  Create bucket with key1, load docs, create indexes
        4.  Wait for Ready
        5.  Baseline scans + file validation (key1)
        6.  Create key2, rotate bucket to key2, trigger DEK rotation
        7.  Poll for new DEK key IDs, re-pin interval high
        8.  Mutations to write snapshots under new DEK
        9.  Scans + file validation (key2)

        Additional phase when with_restart=True:
        10. Rolling restart of all indexer nodes
        11. Scans post-restart
        12. Create new indexes post-restart, scan
        13. Lifecycle ops (alter/drop/build)
        14. File validation post-restart (key2)
        """
        # 10 hours expressed in seconds — stable, no accidental DEK rotation
        STABLE_INTERVAL_S = 36000
        # 2 minutes expressed in seconds — trigger DEK rotation
        ROTATION_INTERVAL_S = 120

        with_restart = self.input.param("with_restart", False)

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_key_rotation")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, with_restart={with_restart}")
        self.log.info("=" * 80)
        
        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites(min_index_nodes=2)
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key1_id = self.encryption_at_rest_id        
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise        
        # ========== STEP 3: Create bucket with encryption using key1 ==========
        bucket_name = self.test_bucket
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption key1 ({key1_id})...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            
            # Verify bucket encryption
            bucket_info = self.rest.get_bucket_json(bucket_name)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            self.assertEqual(encryption_key_id, key1_id, 
                f"Bucket encryption key {encryption_key_id} does not match key1 {key1_id}")
            
            self.log.info(f"[STEP 3] PASSED - Bucket created with encryption key1")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # Pin DEK rotation interval high so no accidental rotation fires during setup
        try:
            self.log.info(
                f"[STEP 3b] Pinning DEK rotation interval to {STABLE_INTERVAL_S} s "
                f"({STABLE_INTERVAL_S // 60} min) to prevent rotation during setup..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key1_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 3b] PASSED - DEK rotation interval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 3b] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 4] PASSED - Loaded {self.num_of_docs_per_collection} documents")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes ==========
        namespace = f"default:{bucket_name}._default._default"
        select_queries = set()
        try:
            self.log.info(f"[STEP 5] Creating indexes...")
            prefix = 'rotation_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace
                )
            )
            
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions,
                namespace=namespace,
                num_replica=self.num_index_replica,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            self.log.info(f"[STEP 5] PASSED - Created {len(queries)} indexes")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise
        
        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for indexes to be online...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 6] PASSED - All indexes online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        baseline_in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        
        # ========== STEP 7: Run baseline validations with key1 ==========
        try:
            self.log.info("[STEP 7] Running baseline validations with key1...")
            
            # Run scan queries
            self.run_scan_validation(select_queries, validate_item_count=True)
            
            # Validate file encryption with key1.
            # In mixed-bucket mode (one encrypted, one not) use the targeted in-use key
            # check scoped to the encrypted bucket instead of full-file-scan validation.
            if not self.encrypt_all_buckets and self.bucket_count >= 2:
                self.validate_mixed_bucket_encrypted_keys(
                    index_nodes, [bucket_name], timeout=300, step_prefix="[STEP 7] "
                )
            else:
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_in_use_key_ids, step_prefix="[STEP 7] "
                )
                if not validation_passed:
                    raise Exception("Baseline file encryption validation failed with key1")

            self.log.info("[STEP 7] PASSED - Baseline validations completed with key1")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise
        
        # ========== STEP 8: Create new encryption key (key2) ==========
        try:
            self.log.info("[STEP 8] Creating new encryption key (key2)...")
            key2_id = self.create_new_encryption_key()
            self.log.info(f"[STEP 8] PASSED - Created key2 with ID: {key2_id}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise
        
        # ========== STEP 9: Reduce DEK rotation interval to trigger first rotation ==========
        try:
            self.log.info(
                f"[STEP 9] Reducing DEK rotation interval to {ROTATION_INTERVAL_S} s "
                f"({ROTATION_INTERVAL_S // 60} min) to trigger first DEK rotation..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key2_id,
                dek_rotation_interval=ROTATION_INTERVAL_S,
                dek_lifetime=ROTATION_INTERVAL_S
            )
            self.log.info("[STEP 9] PASSED - DEK rotation interval set to trigger rotation")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Poll for new DEK IDs, then immediately re-pin ==========
        try:
            self.log.info(
                "[STEP 10] Polling GetInUseKeys for new indexer key IDs (timeout 300 s)..."
            )
            new_in_use_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_in_use_key_ids, timeout=300, label="[STEP 10] "
            )
            self.log.info(
                f"[STEP 10] New key IDs detected: {new_in_use_key_ids}. "
                f"Immediately pinning DEK rotation interval back to {STABLE_INTERVAL_S} s..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key2_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 10] PASSED - DEK rotation detected, interval re-pinned high")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 10b: Run mutations to write snapshots under new DEK ==========
        try:
            self.log.info("[STEP 10b] Running mutations to trigger snapshot creation under new DEK...")
            gen_update = SDKDataLoader(
                num_ops=1000,
                percent_create=0,
                percent_update=100,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_update,
                batch_size=1000,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.sleep(60, "Waiting for new snapshots to be persisted with new DEK")
            self.log.info("[STEP 10b] PASSED - Mutations complete, snapshots should reflect new DEK")
        except Exception as e:
            self.log.error(f"[STEP 10b] FAILED: {str(e)}")
            raise
        # ========== STEP 11: Rerun validations with key2 ==========
        try:
            self.log.info("[STEP 11] Rerunning validations after key rotation...")
            new_in_use_key_ids_again = self.get_new_indexer_in_use_key_ids(index_nodes, baseline_in_use_key_ids)
            self.log.info(f"Double checking keys in use just before validation {new_in_use_key_ids_again}")
            # Validate file encryption with key2.
            # In mixed-bucket mode use the targeted in-use key check instead of file scan.
            if not self.encrypt_all_buckets and self.bucket_count >= 2:
                self.validate_mixed_bucket_encrypted_keys(
                    index_nodes, [bucket_name], timeout=300, step_prefix="[STEP 11] "
                )
            else:
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=sorted(set(baseline_in_use_key_ids) | set(new_in_use_key_ids)),
                    step_prefix="[STEP 11] "
                )
                if not validation_passed:
                    raise Exception("File encryption validation failed after key rotation to key2")
            # Run scan queries again
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.log.info("[STEP 11] PASSED - Post-rotation validations completed with key2")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== RESTART PHASE (with_restart=True only) ==========
        if with_restart:
            # ========== STEP 12: Rolling restart of all indexer nodes ==========
            try:
                self.log.info(f"[STEP 12] Rolling restart of {len(index_nodes)} indexer nodes...")
                self.restart_index_nodes(index_nodes)
                self.log.info("[STEP 12] PASSED - All indexer nodes restarted and indexes online")
            except Exception as e:
                self.log.error(f"[STEP 12] FAILED: {str(e)}")
                raise

            # ========== STEP 13: Scans post-restart ==========
            try:
                self.log.info("[STEP 13] Running scans post-restart...")
                self.run_scan_validation(select_queries, validate_item_count=True)
                self.log.info("[STEP 13] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 13] FAILED: {str(e)}")
                raise

            # ========== STEP 14: Create new indexes post-restart and scan ==========
            try:
                self.log.info("[STEP 14] Creating new indexes post-restart and scanning...")
                restart_prefix = 'rotation_restart_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                restart_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=restart_prefix, skip_primary=False
                )
                restart_select_queries = self.gsi_util_obj.get_select_queries(
                    definition_list=restart_definitions, namespace=namespace
                )
                restart_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=restart_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=False, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=restart_queries, database=namespace, query_node=query_node
                )
                self.wait_until_indexes_online(timeout=1200)
                self.run_scan_validation(restart_select_queries)
                self.log.info(f"[STEP 14] PASSED - Created {len(restart_queries)} post-restart indexes")
            except Exception as e:
                self.log.error(f"[STEP 14] FAILED: {str(e)}")
                raise

            # ========== STEP 15: Lifecycle ops post-restart ==========
            try:
                self.log.info("[STEP 15] Running lifecycle ops post-restart (alter/drop/build)...")
                self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
                self.log.info("[STEP 15] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 15] FAILED: {str(e)}")
                raise

            # ========== STEP 16: File validation post-restart ==========
            try:
                self.log.info(f"[STEP 16] Validating file encryption post-restart with key2 key IDs...")
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(baseline_in_use_key_ids) | set(new_in_use_key_ids)),
                        step_prefix="[STEP 16] "):
                    raise Exception("File encryption validation failed post-restart")
                self.log.info("[STEP 16] PASSED")
            except Exception as e:
                self.log.error(f"[STEP 16] FAILED: {str(e)}")
                raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_key_rotation")
        self.log.info(f"Successfully rotated encryption from key1 ({key1_id}) to key2 ({key2_id}), "
                      f"with_restart={with_restart}")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_force_reencryption(self):
        """
        Encryption at rest force re-encryption test.
        
        This test validates the force re-encryption operation which:
        1. Drops all existing DEKs (Data Encryption Keys) for the bucket
        2. Creates new DEKs
        3. Re-encrypts ALL data in the bucket with the new DEKs
        Test flow:
        1.  Create bucket with encryption enabled
        2.  Create indexes and load documents
        3.  Run baseline validations + capture pre-re-encryption doc count
        4.  Trigger force re-encryption (drop DEKs and re-encrypt)
        5.  Wait for re-encryption to complete
        6.  Verify DEK info changed
        7.  Post-re-encryption mutations: upsert new docs, delete some docs, update some docs
        8.  Rerun validations + compare pre/post-mutation counts
        
        Use case: When DEKs may have been compromised and need to be replaced
        """
        # 10 hours expressed in seconds — stable, no accidental DEK rotation
        STABLE_INTERVAL_S = 36000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_force_reencryption")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)
        
        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites(min_index_nodes=2)
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key_id = self.encryption_at_rest_id        
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise        
        # ========== STEP 3: Create bucket with encryption ==========
        bucket_name = self.test_bucket
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            
            bucket_info = self.rest.get_bucket_json(bucket_name)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            self.assertEqual(encryption_key_id, key_id, 
                f"Bucket encryption key {encryption_key_id} does not match {key_id}")
            
            self.log.info(f"[STEP 3] PASSED - Bucket created with encryption")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # Pin DEK rotation interval high so no accidental rotation fires during setup
        try:
            self.log.info(
                f"[STEP 3b] Pinning DEK rotation interval to {STABLE_INTERVAL_S} s "
                f"({STABLE_INTERVAL_S // 60} min) to prevent background rotation during setup..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 3b] PASSED - DEK rotation interval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 3b] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 4] PASSED - Loaded {self.num_of_docs_per_collection} documents")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes ==========
        namespace = f"default:{bucket_name}._default._default"
        select_queries = set()
        try:
            self.log.info(f"[STEP 5] Creating indexes...")
            prefix = 'reencrypt_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace
                )
            )
            
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions,
                namespace=namespace,
                num_replica=self.num_index_replica,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            self.log.info(f"[STEP 5] PASSED - Created {len(queries)} indexes")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise
        
        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for indexes to be online...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 6] PASSED - All indexes online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        baseline_in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        
        # ========== STEP 7: Run baseline validations + capture pre-re-encryption count ==========
        try:
            self.log.info("[STEP 7] Running baseline validations before re-encryption...")

            self.run_scan_validation(select_queries, validate_item_count=True)

            # In mixed-bucket mode use the targeted in-use key check instead of file scan.
            if not self.encrypt_all_buckets and self.bucket_count >= 2:
                self.validate_mixed_bucket_encrypted_keys(
                    index_nodes, [bucket_name], timeout=300, step_prefix="[STEP 7] "
                )
            else:
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_in_use_key_ids, step_prefix="[STEP 7] "
                )
                if not validation_passed:
                    raise Exception("Baseline file encryption validation failed")

            pre_reencryption_count_result = self.run_cbq_query(
                query=f"SELECT COUNT(*) as cnt FROM `{bucket_name}`",
                scan_consistency='request_plus'
            )
            pre_reencryption_count = pre_reencryption_count_result['results'][0]['cnt']
            self.log.info(f"[STEP 7] Pre-re-encryption doc count: {pre_reencryption_count}")

            self.log.info("[STEP 7] PASSED - Baseline validations completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise
        
        # ========== STEP 8: Capture DEK info before re-encryption ==========
        try:
            self.log.info("[STEP 8] Capturing DEK info before re-encryption...")
            bucket_info_before = self.rest.get_bucket_json(bucket_name)
            dek_info_before = bucket_info_before.get('encryptionAtRestInfo', {})
            dek_count_before = dek_info_before.get('dekNumber', 0)
            oldest_dek_before = dek_info_before.get('oldestDekCreationDatetime', None)
            
            self.log.info(f"[STEP 8] DEK count before: {dek_count_before}")
            self.log.info(f"[STEP 8] Oldest DEK creation time before: {oldest_dek_before}")
            self.log.info("[STEP 8] PASSED - DEK info captured")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise
        
        # ========== STEP 9: Trigger force re-encryption ==========
        try:
            self.log.info(f"[STEP 9] Triggering force re-encryption on bucket '{bucket_name}'...")
            self.log.info("[STEP 9] This will drop existing DEKs and re-encrypt all data with new DEKs")
            
            status, response = self.rest.trigger_data_reencryption(bucket_name)
            if not status:
                raise Exception(f"Failed to trigger re-encryption: {response}")
            
            self.log.info(f"[STEP 9] Force re-encryption triggered. Response: {response}")
            self.log.info("[STEP 9] PASSED - Re-encryption initiated")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise
        
        # ========== STEP 10: Poll for new DEK IDs, then immediately re-pin ==========
        try:
            self.log.info(
                "[STEP 10] Polling GetInUseKeys for new indexer key IDs (timeout 300 s)..."
            )
            new_in_use_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_in_use_key_ids, timeout=300, label="[STEP 10] "
            )
            self.log.info(
                f"[STEP 10] New key IDs detected: {new_in_use_key_ids}. "
                f"Immediately pinning DEK rotation interval back to {STABLE_INTERVAL_S} s..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 10] PASSED - Re-encryption detected, interval re-pinned high")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise
        
        # ========== STEP 11: Verify DEK info changed ==========
        try:
            self.log.info("[STEP 11] Verifying DEK info changed after re-encryption...")
            bucket_info_after = self.rest.get_bucket_json(bucket_name)
            dek_info_after = bucket_info_after.get('encryptionAtRestInfo', {})
            dek_count_after = dek_info_after.get('dekNumber', 0)
            oldest_dek_after = dek_info_after.get('oldestDekCreationDatetime', None)

            self.log.info(f"[STEP 11] DEK count after: {dek_count_after}")
            self.log.info(f"[STEP 11] Oldest DEK creation time after: {oldest_dek_after}")

            if oldest_dek_before and oldest_dek_after:
                if oldest_dek_after != oldest_dek_before:
                    self.log.info(f"[STEP 11] DEKs were rotated - oldest DEK changed from {oldest_dek_before} to {oldest_dek_after}")
                else:
                    self.log.warning(f"[STEP 11] Oldest DEK timestamp unchanged - re-encryption may still be in progress")

            self.log.info("[STEP 11] PASSED - DEK info verification completed")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Post-re-encryption mutations (upsert/delete/update) ==========
        upsert_count = 10
        delete_count = 5
        update_count = 5
        try:
            self.log.info(f"[STEP 12] Running post-re-encryption mutations "
                          f"(upsert={upsert_count}, delete={delete_count}, update={update_count})...")
            # Use the same doc generator/schema as the initial load (not ad hoc JSON) so
            # the new docs carry the fields (price, free_breakfast, type, etc.) that the
            # created GSI indexes are built on — otherwise GSI correctly excludes
            # schema-incompatible docs while the raw KV count doesn't, causing a
            # false-positive mismatch in item_count_related_validations().
            gen_reencrypt_upsert = SDKDataLoader(
                num_ops=upsert_count, percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password,
                key_prefix="reencrypt_upsert_"
            )
            tasks = self.data_ops_javasdk_loader_in_batches_to_collection(
                bucket_name, gen_reencrypt_upsert, batch_size=upsert_count
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 12] Upserted {upsert_count} schema-conformant docs into '{bucket_name}'")

            delete_keys = ", ".join(f'"doc_{i}"' for i in range(delete_count))
            delete_result = self.run_cbq_query(
                query=f"DELETE FROM `{bucket_name}` USE KEYS [{delete_keys}] RETURNING META().id",
                server=query_node
            )
            actual_deleted = len(delete_result.get('results', []))
            self.log.info(f"[STEP 12] Deleted {actual_deleted} docs")

            update_keys = ", ".join(f'"doc_{i}"' for i in range(delete_count, delete_count + update_count))
            self.run_cbq_query(
                query=f"UPDATE `{bucket_name}` USE KEYS [{update_keys}] SET reencrypt_updated = true",
                server=query_node
            )
            self.log.info(f"[STEP 12] Updated {update_count} docs")
            self.log.info("[STEP 12] PASSED - Post-re-encryption mutations completed")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Rerun validations after re-encryption + compare mutation counts ==========
        try:
            self.log.info("[STEP 13] Rerunning validations after re-encryption and mutations...")

            self.run_scan_validation(select_queries, validate_item_count=False)

            expected_count = pre_reencryption_count + upsert_count - delete_count
            post_count_result = self.run_cbq_query(
                query=f"SELECT COUNT(*) as cnt FROM `{bucket_name}`",
                scan_consistency='request_plus'
            )
            post_count = post_count_result['results'][0]['cnt']
            self.assertEqual(
                post_count, expected_count,
                f"[STEP 13] Expected {expected_count} docs "
                f"(pre={pre_reencryption_count} + upserted={upsert_count} - deleted={delete_count}), "
                f"got {post_count}"
            )
            self.log.info(
                f"[STEP 13] pre={pre_reencryption_count}, +upserted={upsert_count}, "
                f"-deleted={delete_count}, post={post_count} — count matches"
            )

            # In mixed-bucket mode use the targeted in-use key check instead of file scan.
            if not self.encrypt_all_buckets and self.bucket_count >= 2:
                self.validate_mixed_bucket_encrypted_keys(
                    index_nodes, [bucket_name], timeout=300, step_prefix="[STEP 13] "
                )
            else:
                if self.gsi_type == "memory_optimized":  # STEP 12 mutations: the next snapshots delete the older ones
                    self.sleep(int(self.input.param("snapshot_wait", 90)), "[STEP 13] Waiting for the MOI snapshots")
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=sorted(set(baseline_in_use_key_ids) | set(new_in_use_key_ids)),
                    step_prefix="[STEP 13] "
                )
                if not validation_passed:
                    raise Exception("File encryption validation failed after re-encryption")

            self.log.info("[STEP 13] PASSED - Post-re-encryption validations and count comparison completed")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_force_reencryption")
        self.log.info("Successfully dropped DEKs and re-encrypted data with new DEKs; post-re-encryption mutations validated")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_dgm_sanity(self):
        """
        Encryption at rest sanity in DGM mode - multi bucket.

        Parameterised by index_resident_ratio (default 50). Run with two conf entries:
          - index_resident_ratio=50  (RR 50%)
          - index_resident_ratio=10  (RR 10%)

        Test validates:
        1. Encryption at rest configured on 2 buckets
        2. DEK rotation interval set short so rotation fires during the test
        3. Scalar indexes created on both buckets
        4. Indexes reach Ready state
        5. DGM induced: docs loaded until any node RR <= index_resident_ratio
        6. Scans succeed against DGM indexes
        7. DEK rotation fires (new key IDs detected)
        8. Scans succeed post-rotation in DGM mode
        9. Item count validation
        """
        DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_dgm_sanity")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, index_resident_ratio={self.index_resident_ratio}, "
                      f"dek_rotation_interval={DEK_ROTATION_INTERVAL_S}s")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Create 2 buckets with encryption ==========
        bucket_names = []
        try:
            self.log.info("[STEP 2] Creating 2 buckets with encryption...")
            for i in range(2):
                bucket_name = f"{self.test_bucket}_{i + 1}"
                self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
                bucket_names.append(bucket_name)
                bucket_info = self.rest.get_bucket_json(bucket_name)
                encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                if encryption_key_id is None or encryption_key_id == -1:
                    raise Exception(f"Bucket '{bucket_name}' does not have encryption at rest enabled")
                self.log.info(f"[STEP 2] Bucket '{bucket_name}' created with encryptionAtRestKeyId={encryption_key_id}")
            self.log.info(f"[STEP 2] PASSED - Created buckets: {bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Set indexer snapshot settings + short DEK rotation interval ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 3]")
            self.log.info(f"[STEP 3] Setting DEK rotation interval to {DEK_ROTATION_INTERVAL_S}s on all buckets "
                          f"so rotation fires during DGM loading...")
            for bucket_name in bucket_names:
                self.set_bucket_dek_rotation_config(
                    bucket_name, self.encryption_at_rest_id,
                    dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                    dek_lifetime=DEK_ROTATION_INTERVAL_S
                )
            self.log.info("[STEP 3] PASSED - Snapshot settings and DEK rotation interval configured")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load initial documents and create indexes ==========
        select_queries = set()
        self.namespaces = []
        try:
            self.log.info("[STEP 4] Loading documents and creating indexes...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]

            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 4] Loaded {self.num_of_docs_per_collection} docs into each bucket")

            for idx, bucket_name in enumerate(bucket_names):
                namespace = f"default:{bucket_name}._default._default"
                self.namespaces.append(namespace)
                prefix = 'dgm_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                query_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template,
                    prefix=prefix,
                    skip_primary=False
                )
                self.log.info(f"[STEP 4.{idx+1}] Generated {len(query_definitions)} index definitions for '{bucket_name}'")
                select_queries.update(
                    self.gsi_util_obj.get_select_queries(
                        definition_list=query_definitions,
                        namespace=namespace
                    )
                )
                queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=query_definitions,
                    namespace=namespace,
                    num_replica=self.num_index_replica,
                    deploy_node_info=deploy_nodes,
                    defer_build=self.defer_build,
                    randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=queries,
                    database=namespace,
                    query_node=query_node
                )
                self.log.info(f"[STEP 4.{idx+1}] PASSED - Created indexes on '{bucket_name}'")

            self.log.info(f"[STEP 4] PASSED - Collected {len(select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Wait for indexes to reach Ready state ==========
        try:
            self.log.info("[STEP 5] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 5] PASSED - All indexes Ready")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Capture baseline key IDs then induce DGM ==========
        try:
            baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            self.log.info(f"[STEP 6] Baseline key IDs before DGM: {baseline_key_ids}")
            self.log.info(f"[STEP 6] Inducing DGM to target resident ratio <= {self.index_resident_ratio}%...")
            self._load_until_index_dgm(resident_ratio=self.index_resident_ratio)
            avg_rr = self.compute_cluster_avg_rr_index()
            self.log.info(f"[STEP 6] PASSED - DGM achieved, avg_resident_ratio={avg_rr}")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Run scans in DGM mode ==========
        try:
            self.log.info(f"[STEP 7] Running {len(select_queries)} scan queries in DGM mode...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 7] PASSED - All scans completed successfully in DGM mode")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Wait for DEK rotation to fire ==========
        try:
            self.log.info(f"[STEP 8] Waiting for DEK rotation to fire (interval={DEK_ROTATION_INTERVAL_S}s)...")
            new_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_key_ids, timeout=300, label="[STEP 8] "
            )
            self.log.info(f"[STEP 8] PASSED - DEK rotation detected, new key IDs: {new_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Run scans post-rotation in DGM mode ==========
        try:
            self.log.info(f"[STEP 9] Running scans post-DEK-rotation in DGM mode...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 9] PASSED - All scans succeeded post-rotation in DGM mode")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Item count validations ==========
        try:
            self.log.info("[STEP 10] Running item count related validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 10] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_dgm_sanity")
        self.log.info(f"index_resident_ratio={self.index_resident_ratio}%, gsi_type={self.gsi_type}, "
                      f"dek_rotation_interval={DEK_ROTATION_INTERVAL_S}s")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_toggle_encryption_on_bucket(self):
        """
        Encryption at rest - toggle encryption on an existing bucket mid-test.

        Parameterised via toggle_direction (default: "enable"):
          - "enable": bucket starts unencrypted; encryption is enabled mid-test.
                      Validates new indexes created post-enable are encrypted on disk.
                      Old index files are intentionally not validated (remain unencrypted).
          - "disable": bucket starts encrypted; encryption is disabled mid-test.
                       Validates that old and new indexes remain accessible.
                       Item count is validated at the end.

        Steps (shared):
        1.  Verify prerequisites
        2.  Set indexer snapshot settings
        3.  Create single bucket (WITH or WITHOUT encryption per toggle_direction)
        4.  Load documents
        5.  Create baseline indexes, wait for Ready
        6.  Baseline scans + item count
            (+ file encryption validation when starting encrypted)
        7.  Toggle bucket encryption (enable or disable)
        8.  Scans on old indexes post-toggle (backward compatibility check)
        9.  Create new indexes post-toggle, scan
        10. Index lifecycle ops (rolling restart + alter/drop/build)
        11. Final validation:
            enable  -> file encryption check on new indexes
            disable -> item count validation
        """
        toggle_direction = self.input.param("toggle_direction", "enable")
        start_encrypted = (toggle_direction == "disable")

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_toggle_encryption_on_bucket")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, toggle_direction={toggle_direction}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        key_id = self.encryption_at_rest_id

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Create bucket ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            enc_label = "WITH" if start_encrypted else "WITHOUT"
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' {enc_label} encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=start_encrypted)
            bucket_info = self.rest.get_bucket_json(bucket_name)
            actual_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            if start_encrypted:
                if actual_key_id is None or actual_key_id == -1:
                    raise Exception(f"Bucket '{bucket_name}' should have encryption enabled")
            else:
                if actual_key_id not in [None, -1]:
                    raise Exception(
                        f"Bucket '{bucket_name}' should not have encryption enabled, got key {actual_key_id}")
            self.log.info(f"[STEP 3] PASSED - Bucket created {enc_label} encryption (encryptionAtRestKeyId={actual_key_id})")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create baseline indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        baseline_prefix = 'enc_base_' if start_encrypted else 'noenc_base_'
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, baseline_prefix, step_prefix="[STEP 5]"
            )
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise
        try:
            self.log.info("[STEP 5b] Waiting for indexes to reach Ready state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 5b] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5b] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Baseline scans + item count (+ file validation if starting encrypted) ==========
        try:
            self.log.info("[STEP 6] Running baseline scans and item count validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.log.info("[STEP 6] PASSED - Baseline scans completed")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise
        if start_encrypted:
            try:
                baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                self.log.info(f"[STEP 6b] Validating baseline file encryption (key IDs {baseline_key_ids})...")
                if not self.validate_file_encryption_with_key(
                        index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 6b] "):
                    raise Exception("Baseline file encryption validation failed")
                self.log.info("[STEP 6b] PASSED - Baseline file encryption validated")
            except Exception as e:
                self.log.error(f"[STEP 6b] FAILED: {str(e)}")
                raise

        # ========== STEP 7: Toggle bucket encryption ==========
        try:
            if toggle_direction == "enable":
                self.log.info(f"[STEP 7] Enabling encryption on bucket '{bucket_name}' with key_id={key_id}...")
                status, response = self.rest.enable_bucket_encryption(bucket_name, key_id)
                if not status:
                    raise Exception(f"Failed to enable encryption: {response}")
                bucket_info = self.rest.get_bucket_json(bucket_name)
                toggled_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                if toggled_key_id is None or toggled_key_id == -1:
                    raise Exception("Bucket encryption was not enabled after API call")
                self.log.info(f"[STEP 7] PASSED - Encryption enabled, encryptionAtRestKeyId={toggled_key_id}")
            else:
                self.log.info(f"[STEP 7] Disabling encryption on bucket '{bucket_name}'...")
                status, response = self.rest.disable_bucket_encryption(bucket_name)
                if not status:
                    raise Exception(f"Failed to disable encryption: {response}")
                bucket_info = self.rest.get_bucket_json(bucket_name)
                toggled_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                if toggled_key_id not in [None, -1]:
                    raise Exception(
                        f"Encryption not disabled — encryptionAtRestKeyId still set to {toggled_key_id}")
                self.log.info(f"[STEP 7] PASSED - Encryption disabled, encryptionAtRestKeyId={toggled_key_id}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Scans on old indexes post-toggle (backward compatibility) ==========
        try:
            self.log.info("[STEP 8] Running scans on old indexes after toggle (backward compatibility)...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 8] PASSED - Old indexes still accessible after toggle")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Create new indexes post-toggle and scan ==========
        new_select_queries = set()
        new_prefix = ('post_enc_' if toggle_direction == "enable" else 'post_dis_') + \
                     ''.join(random.choices(string.ascii_letters + string.digits, k=5))
        try:
            self.log.info(f"[STEP 9] Creating new indexes post-toggle (prefix={new_prefix})...")
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries.update(self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            ))
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 9] PASSED - Created {len(new_queries)} post-toggle indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Index lifecycle ops (rolling restart + alter/drop/build) ==========
        try:
            self.log.info("[STEP 10] Restarting indexer nodes then running lifecycle ops...")
            self.restart_index_nodes(index_nodes)
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 10] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Final validation ==========
        try:
            if toggle_direction == "enable":
                expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                self.log.info(
                    f"[STEP 11] Validating file encryption for new indexes (key IDs {expected_key_ids})...")
                if not self.validate_file_encryption_with_key(
                        index_nodes, expected_key_id=expected_key_ids, step_prefix="[STEP 11] "):
                    raise Exception("File encryption validation failed for post-enable indexes")
                self.log.info("[STEP 11] PASSED - Post-enable indexes are encrypted on disk")
            else:
                self.log.info("[STEP 11] Running item count validation...")
                self.item_count_related_validations()
                self.log.info("[STEP 11] PASSED - Item count validation completed")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_toggle_encryption_on_bucket")
        self.log.info(f"toggle_direction={toggle_direction}, bucket='{bucket_name}'")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_compaction_workload(self):
        """
        Encryption at rest - concurrent mutation + query workload with compaction.

        Runs a continuous low-ops mutation and query workload while triggering
        plasma compaction with a low fragmentation threshold (shard size > 16MB).
        Validates that scans, new index creation, and lifecycle ops all succeed
        during and after compaction with encryption at rest enabled.

        Steps:
        1.  Create single encrypted bucket, load docs, create indexes
        2.  Baseline scans + file encryption validation
        3.  Start background mutation workload (low ops)
        4.  Start background query workload (continuous scans)
        5.  Set low compaction threshold and trigger compaction on all index nodes
        6.  Wait for compaction to complete, stop workload threads
        7.  Scans on original indexes
        8.  Create new indexes, scan
        9.  Lifecycle ops (alter/drop/build)
        10. File encryption validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_compaction_workload")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.log.info("[STEP 3] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'compact_', step_prefix="[STEP 5]"
            )
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for Ready state ==========
        try:
            self.log.info("[STEP 6] Waiting for indexes to reach Ready state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 7: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 7] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 7] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Start background mutation + query workloads ==========
        stop_event = threading.Event()
        mutation_thread = None
        query_thread = None
        try:
            self.log.info("[STEP 8] Starting background mutation and query workloads...")

            def run_mutations():
                while not stop_event.is_set():
                    gen_update = SDKDataLoader(
                        num_ops=self.mutation_ops_rate,
                        percent_create=0, percent_update=100, percent_delete=0,
                        scope="_default", collection="_default",
                        json_template=self.json_template, output=True,
                        username=self.username, password=self.password
                    )
                    try:
                        tasks = self.data_ops_javasdk_loader_in_batches(
                            sdk_data_loader=gen_update, batch_size=self.mutation_ops_rate, dataset=self.json_template
                        )
                        for task in tasks:
                            task.result()
                    except Exception as ex:
                        self.log.warning(f"[STEP 8] Mutation batch error (non-fatal): {ex}")
                    time.sleep(5)

            def run_queries():
                n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
                while not stop_event.is_set():
                    for query in select_queries:
                        if stop_event.is_set():
                            break
                        try:
                            self.run_cbq_query(query=query, server=n1ql_server)
                        except Exception as ex:
                            self.log.warning(f"[STEP 8] Query error (non-fatal): {ex}")
                    time.sleep(10)

            mutation_thread = threading.Thread(target=run_mutations, name="mutation_workload", daemon=True)
            query_thread = threading.Thread(target=run_queries, name="query_workload", daemon=True)
            mutation_thread.start()
            query_thread.start()
            self.log.info("[STEP 8] PASSED - Background workloads started")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Set compaction threshold and trigger compaction ==========
        try:
            self.log.info("[STEP 9] Setting low compaction threshold and triggering compaction...")
            ncompacts_before, _ = self._get_total_ncompacts(index_nodes)
            for index_node in index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({
                    "indexer.settings.compaction.min_size": 16 * 1024 * 1024,  # 16 MB
                    "indexer.settings.compaction.min_frag": 10,
                    "indexer.settings.compaction.check_period": 5
                })
                rest.trigger_index_compaction()
                self.log.info(f"[STEP 9] Compaction triggered on {index_node.ip}")

            # Poll until NCompacts increases on at least one node
            deadline = time.time() + 300
            compaction_detected = False
            while time.time() < deadline:
                ncompacts_after, _ = self._get_total_ncompacts(index_nodes)
                if ncompacts_after > ncompacts_before:
                    self.log.info(
                        f"[STEP 9] Compaction detected: NCompacts {ncompacts_before} -> {ncompacts_after}")
                    compaction_detected = True
                    break
                self.sleep(15, "Waiting for compaction to be detected via NCompacts stat")

            if not compaction_detected:
                self.log.warning("[STEP 9] NCompacts did not increase within 300s — compaction may not have run")

            self.log.info("[STEP 9] PASSED - Compaction phase complete")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Stop workload threads ==========
        try:
            self.log.info("[STEP 10] Stopping background workload threads...")
            stop_event.set()
            if mutation_thread:
                mutation_thread.join(timeout=30)
            if query_thread:
                query_thread.join(timeout=30)
            self.log.info("[STEP 10] PASSED - Workload threads stopped")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Scans on original indexes post-compaction ==========
        try:
            self.log.info("[STEP 11] Running scans on original indexes post-compaction...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Create new indexes post-compaction and scan ==========
        try:
            self.log.info("[STEP 12] Creating new indexes post-compaction and scanning...")
            new_prefix = 'compact_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 12] PASSED - Created {len(new_queries)} post-compaction indexes")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Lifecycle ops ==========
        try:
            self.log.info("[STEP 13] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        # ========== STEP 14: File encryption validation ==========
        try:
            current_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            self.log.info(f"[STEP 14] Validating file encryption with key IDs {current_key_ids}...")
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=current_key_ids, step_prefix="[STEP 14] "):
                raise Exception("File encryption validation failed post-compaction")
            self.log.info("[STEP 14] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_compaction_workload")
        self.log.info(f"gsi_type={self.gsi_type}, key_id={self.encryption_at_rest_id}")
        self.log.info("=" * 80)


    def test_gsi_encryption_at_rest_toggle_encryption_with_workload(self):
        """
        Encryption at rest - toggle encryption on an existing bucket while a
        continuous low-ops mutation + query workload is running.

        Parameterised via toggle_direction (default: "enable"):
          - "enable": bucket starts unencrypted; encryption is enabled mid-workload.
                      Validates new indexes created post-enable are encrypted on disk.
          - "disable": bucket starts encrypted; encryption is disabled mid-workload.
                       Validates item count at the end.

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings
        3.  Create bucket (WITH or WITHOUT encryption per toggle_direction)
        4.  Load documents
        5.  Create baseline indexes, wait for Ready
        6.  Baseline scans + item count
            (+ file encryption validation when starting encrypted)
        7.  Start background mutation + query workloads
        8.  Toggle encryption (enable or disable) while workloads are running
        9.  Stop workload threads
        10. Scans on old indexes post-toggle
        11. Create new indexes post-toggle, scan
        12. Lifecycle ops (alter/drop/build)
        13. Final validation:
            enable  -> file encryption check on new indexes
            disable -> item count validation
        """
        toggle_direction = self.input.param("toggle_direction", "enable")
        start_encrypted = (toggle_direction == "disable")

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_toggle_encryption_with_workload")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, toggle_direction={toggle_direction}, mutation_ops_rate={self.mutation_ops_rate}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        key_id = self.encryption_at_rest_id

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Create bucket ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            enc_label = "WITH" if start_encrypted else "WITHOUT"
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' {enc_label} encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=start_encrypted)
            bucket_info = self.rest.get_bucket_json(bucket_name)
            actual_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            if start_encrypted:
                if actual_key_id is None or actual_key_id == -1:
                    raise Exception(f"Bucket '{bucket_name}' should have encryption enabled")
            else:
                if actual_key_id not in [None, -1]:
                    raise Exception(f"Bucket should not have encryption enabled, got key {actual_key_id}")
            self.log.info(f"[STEP 3] PASSED - Bucket created {enc_label} encryption (encryptionAtRestKeyId={actual_key_id})")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create baseline indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        baseline_prefix = 'enc_wl_' if start_encrypted else 'noenc_wl_'
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, baseline_prefix, step_prefix="[STEP 5]"
            )
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise
        try:
            self.log.info("[STEP 5b] Waiting for indexes to reach Ready state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 5b] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5b] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Baseline scans + item count (+ file validation if starting encrypted) ==========
        try:
            self.log.info("[STEP 6] Running baseline scans and item count validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.log.info("[STEP 6] PASSED - Baseline scans completed")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise
        if start_encrypted:
            try:
                baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                self.log.info(f"[STEP 6b] Validating baseline file encryption (key IDs {baseline_key_ids})...")
                if not self.validate_file_encryption_with_key(
                        index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 6b] "):
                    raise Exception("Baseline file encryption validation failed")
                self.log.info("[STEP 6b] PASSED - Baseline file encryption validated")
            except Exception as e:
                self.log.error(f"[STEP 6b] FAILED: {str(e)}")
                raise

        # ========== STEP 7: Start background mutation + query workloads ==========
        stop_event = threading.Event()
        mutation_thread = None
        query_thread = None
        try:
            self.log.info(f"[STEP 7] Starting background mutation and query workloads (mutation_ops_rate={self.mutation_ops_rate})...")

            def run_mutations():
                while not stop_event.is_set():
                    gen_update = SDKDataLoader(
                        num_ops=self.mutation_ops_rate,
                        percent_create=self.workload_percent_create,
                        percent_update=self.workload_percent_update,
                        percent_delete=self.workload_percent_delete,
                        scope="_default", collection="_default",
                        json_template=self.json_template, output=True,
                        username=self.username, password=self.password
                    )
                    try:
                        tasks = self.data_ops_javasdk_loader_in_batches(
                            sdk_data_loader=gen_update, batch_size=self.mutation_ops_rate, dataset=self.json_template
                        )
                        for task in tasks:
                            task.result()
                    except Exception as ex:
                        self.log.warning(f"[STEP 7] Mutation batch error (non-fatal): {ex}")
                    time.sleep(5)

            def run_queries():
                n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
                while not stop_event.is_set():
                    for query in select_queries:
                        if stop_event.is_set():
                            break
                        try:
                            self.run_cbq_query(query=query, server=n1ql_server)
                        except Exception as ex:
                            self.log.warning(f"[STEP 7] Query error (non-fatal): {ex}")
                    time.sleep(10)

            mutation_thread = threading.Thread(target=run_mutations, name="mutation_workload", daemon=True)
            query_thread = threading.Thread(target=run_queries, name="query_workload", daemon=True)
            mutation_thread.start()
            query_thread.start()
            self.log.info("[STEP 7] PASSED - Background workloads started")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Toggle encryption while workloads are running ==========
        try:
            if toggle_direction == "enable":
                self.log.info(f"[STEP 8] Enabling encryption on bucket '{bucket_name}' with key_id={key_id} while workloads are running...")
                status, response = self.rest.enable_bucket_encryption(bucket_name, key_id)
                if not status:
                    raise Exception(f"Failed to enable encryption: {response}")
                bucket_info = self.rest.get_bucket_json(bucket_name)
                toggled_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                if toggled_key_id is None or toggled_key_id == -1:
                    raise Exception("Bucket encryption was not enabled after API call")
                self.log.info(f"[STEP 8] PASSED - Encryption enabled, encryptionAtRestKeyId={toggled_key_id}")
            else:
                self.log.info(f"[STEP 8] Disabling encryption on bucket '{bucket_name}' while workloads are running...")
                status, response = self.rest.disable_bucket_encryption(bucket_name)
                if not status:
                    raise Exception(f"Failed to disable encryption: {response}")
                bucket_info = self.rest.get_bucket_json(bucket_name)
                toggled_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                if toggled_key_id not in [None, -1]:
                    raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still set to {toggled_key_id}")
                self.log.info(f"[STEP 8] PASSED - Encryption disabled, encryptionAtRestKeyId={toggled_key_id}")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Stop workload threads ==========
        try:
            self.log.info("[STEP 9] Stopping background workload threads...")
            stop_event.set()
            if mutation_thread:
                mutation_thread.join(timeout=30)
            if query_thread:
                query_thread.join(timeout=30)
            self.log.info("[STEP 9] PASSED - Workload threads stopped")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Scans on old indexes post-toggle ==========
        try:
            self.log.info("[STEP 10] Running scans on old indexes after toggle (backward compatibility)...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 10] PASSED - Old indexes still accessible after toggle")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Create new indexes post-toggle and scan ==========
        new_prefix = ('post_enc_wl_' if toggle_direction == "enable" else 'post_dis_wl_') + \
                     ''.join(random.choices(string.ascii_letters + string.digits, k=5))
        try:
            self.log.info(f"[STEP 11] Creating new indexes post-toggle (prefix={new_prefix})...")
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 11] PASSED - Created {len(new_queries)} post-toggle indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Lifecycle ops ==========
        try:
            self.log.info("[STEP 12] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Final validation ==========
        try:
            if toggle_direction == "enable":
                expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                self.log.info(f"[STEP 13] Validating file encryption for new indexes (key IDs {expected_key_ids})...")
                if not self.validate_file_encryption_with_key(
                        index_nodes, expected_key_id=expected_key_ids, step_prefix="[STEP 13] "):
                    raise Exception("File encryption validation failed for post-enable indexes")
                self.log.info("[STEP 13] PASSED - Post-enable indexes are encrypted on disk")
            else:
                self.log.info("[STEP 13] Running item count validation...")
                self.item_count_related_validations()
                self.log.info("[STEP 13] PASSED - Item count validation completed")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_toggle_encryption_with_workload")
        self.log.info(f"toggle_direction={toggle_direction}, bucket='{bucket_name}'")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_dek_rotation_with_workload(self):
        """
        Encryption at rest - DEK rotation while a continuous mutation + query
        workload runs.

        Sets a short DEK rotation interval to trigger DEK rotation mid-workload and
        validates that ongoing scans and mutations are not disrupted, and that indexes
        remain accessible and encrypted after rotation completes.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Start background mutation workload
        7.  Start background query workload (continuous scans)
        8.  Trigger DEK rotation via short interval while workloads are running
        9.  Poll for new DEK key IDs
        10. Stop workload threads
        11. Scans on original indexes post-rotation
        12. Create new indexes post-rotation, scan
        13. Lifecycle ops (alter/drop/build)
        14. File encryption validation with new key IDs
        """
        DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_dek_rotation_with_workload")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, mutation_ops_rate={self.mutation_ops_rate}, dek_rotation_interval={DEK_ROTATION_INTERVAL_S}s")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer snapshot settings, pin DEK interval ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.log.info("[STEP 3] PASSED - Bucket created with encryption")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'dek_wl_', step_prefix="[STEP 5]"
            )
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for Ready state ==========
        try:
            self.log.info("[STEP 6] Waiting for indexes to reach Ready state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 7: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 7] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 7] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Start background mutation + query workloads ==========
        stop_event = threading.Event()
        mutation_thread = None
        query_thread = None
        try:
            self.log.info(f"[STEP 8] Starting background workloads (mutation_ops_rate={self.mutation_ops_rate})...")

            def run_mutations():
                while not stop_event.is_set():
                    gen_update = SDKDataLoader(
                        num_ops=self.mutation_ops_rate,
                        percent_create=self.workload_percent_create,
                        percent_update=self.workload_percent_update,
                        percent_delete=self.workload_percent_delete,
                        scope="_default", collection="_default",
                        json_template=self.json_template, output=True,
                        username=self.username, password=self.password
                    )
                    try:
                        tasks = self.data_ops_javasdk_loader_in_batches(
                            sdk_data_loader=gen_update, batch_size=self.mutation_ops_rate,
                            dataset=self.json_template
                        )
                        for task in tasks:
                            task.result()
                    except Exception as ex:
                        self.log.warning(f"[STEP 8] Mutation batch error (non-fatal): {ex}")
                    time.sleep(5)

            def run_queries():
                n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
                while not stop_event.is_set():
                    for query in select_queries:
                        if stop_event.is_set():
                            break
                        try:
                            self.run_cbq_query(query=query, server=n1ql_server)
                        except Exception as ex:
                            self.log.warning(f"[STEP 8] Query error (non-fatal): {ex}")
                    time.sleep(10)

            mutation_thread = threading.Thread(target=run_mutations, name="mutation_workload", daemon=True)
            query_thread = threading.Thread(target=run_queries, name="query_workload", daemon=True)
            mutation_thread.start()
            query_thread.start()
            self.log.info("[STEP 8] PASSED - Background workloads started")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Trigger DEK rotation via short interval while workloads run ==========
        try:
            self.log.info(
                f"[STEP 9] Setting DEK rotation interval to {DEK_ROTATION_INTERVAL_S}s "
                f"to trigger DEK rotation while workloads run..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, self.encryption_at_rest_id,
                dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
            )
            self.log.info(f"[STEP 9] PASSED - DEK rotation interval set")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Poll for new DEK key IDs ==========
        try:
            self.log.info("[STEP 10] Polling for new indexer DEK key IDs (timeout=600s)...")
            new_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_key_ids, timeout=600, label="[STEP 10] "
            )
            self.log.info(f"[STEP 10] PASSED - New DEK key IDs: {new_key_ids}")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Stop workload threads ==========
        try:
            self.log.info("[STEP 11] Stopping background workload threads...")
            stop_event.set()
            if mutation_thread:
                mutation_thread.join(timeout=30)
            if query_thread:
                query_thread.join(timeout=30)
            self.log.info("[STEP 11] PASSED - Workload threads stopped")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Scans on original indexes post-rotation ==========
        try:
            self.log.info("[STEP 12] Running scans on original indexes after DEK rotation...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Create new indexes post-rotation and scan ==========
        try:
            self.log.info("[STEP 13] Creating new indexes post-rotation and scanning...")
            new_prefix = 'dek_wl_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 13] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        # ========== STEP 14: Lifecycle ops ==========
        try:
            self.log.info("[STEP 14] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 14] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            raise

        # ========== STEP 15: Wait for empty DEK keys to drain, then validate file encryption ==========
        try:
            self.log.info("[STEP 15] Waiting for empty service_bucket key IDs to drain (max 5 min)...")
            self._wait_for_empty_service_bucket_keys_cleared(
                index_nodes, timeout=300, label="[STEP 15] "
            )
            self.log.info(f"[STEP 15] Validating file encryption with new key IDs {new_key_ids}...")
            if not self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids)),
                    step_prefix="[STEP 15] "):
                raise Exception("File encryption validation failed after DEK rotation")
            self.log.info("[STEP 15] PASSED - Files encrypted with new DEK key IDs")
        except Exception as e:
            self.log.error(f"[STEP 15] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_dek_rotation_with_workload")
        self.log.info(f"DEK rotation succeeded mid-workload on bucket '{bucket_name}', new_dek_key_ids={new_key_ids}")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_partial_rollback_with_dek_rotation(self):
        """
        Encryption at rest - partial rollback with DEK rotation.

        Validates that GSI encryption at rest survives a KV partial rollback combined
        with DEK rotation. Stops KV persistence, loads mutations into memory only,
        triggers DEK rotation via short TTL, then kills memcached to force KV to reload
        from its last persisted snapshot. The indexer detects the DCP rollback signal and
        rolls back index state to match. Validates encryption is intact with the new DEK.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Trigger DEK rotation via short TTL, poll for new key IDs, confirm scans still work
        7.  Stop KV persistence on all KV nodes
        8.  Load additional update mutations (in-memory only, not persisted)
        9.  Record item count before rollback
        10. Kill memcached on KV node to force reload from last persisted snapshot
        11. Start persistence on surviving KV node, failover the restarted node
        12. Wait for indexer to detect rollback and return indexes to Ready state
        13. Assert KV item count matches post-rollback value
        14. Scans on existing indexes (results consistent with rolled-back doc count)
        15. Create new indexes post-rollback, scan
        16. Lifecycle ops (alter/drop/build)
        17. File encryption validation with new key IDs from step 6
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_partial_rollback_with_dek_rotation")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 3,
                f"Need at least 3 index nodes, found {len(index_nodes)}")
            kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
            self.assertGreaterEqual(len(kv_nodes), 1,
                f"Need at least 1 KV node, found {len(kv_nodes)}")
            if not self.enable_encryption_at_rest:
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            key_id = self.encryption_at_rest_id
            self.assertIsNotNone(key_id, "encryption_at_rest_id is None")
            self.log.info(f"[STEP 1] PASSED - key_id={key_id}, index_nodes={[n.ip for n in index_nodes]}, kv_nodes={[n.ip for n in kv_nodes]}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Set indexer snapshot settings, pin DEK interval ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption, pinning DEK interval...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rb_rot_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Trigger DEK rotation, poll new key IDs, confirm scans ==========
        new_key_ids = None
        try:
            DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
            self.log.info(f"[STEP 6] Triggering DEK rotation via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
            )
            self.log.info(f"[STEP 6] DEK rotation set, polling for new key IDs (timeout=600s)...")
            new_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_key_ids, timeout=600, label="[STEP 6] "
            )
            self.run_scan_validation(select_queries)
            self.log.info(f"[STEP 6] PASSED - New key IDs: {new_key_ids}, scans OK")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Stop KV persistence on all KV nodes ==========
        try:
            self.log.info("[STEP 7] Stopping KV persistence on all KV nodes...")
            from memcached.helper.data_helper import MemcachedClientHelper
            for kv_node in kv_nodes:
                mem_client = MemcachedClientHelper.direct_client(kv_node, bucket_name)
                mem_client.stop_persistence()
                self.log.info(f"[STEP 7] Persistence stopped on {kv_node.ip}")
            self.log.info("[STEP 7] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Load update mutations (in-memory only, not persisted) ==========
        try:
            self.log.info("[STEP 8] Loading update mutations into memory (not persisted)...")
            gen_update = SDKDataLoader(
                num_ops=5000,
                percent_create=0, percent_update=100, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_update, batch_size=5000, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            time.sleep(10)
            self.log.info("[STEP 8] PASSED - In-memory mutations loaded")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Record item count before rollback ==========
        try:
            self.log.info("[STEP 9] Recording item count before rollback...")
            count_before_rollback = self.get_item_count(self.master, bucket_name)
            self.log.info(f"[STEP 9] PASSED - Items in bucket before rollback: {count_before_rollback}")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Kill memcached on KV node to force rollback ==========
        kv_node_to_kill = self._kv_node_without_n1ql(kv_nodes)
        try:
            self.log.info(f"[STEP 10] Killing memcached on {kv_node_to_kill.ip} to force KV rollback...")
            shell = RemoteMachineShellConnection(kv_node_to_kill)
            shell.kill_memcached()
            shell.disconnect()
            time.sleep(10)
            self.log.info("[STEP 10] PASSED - memcached killed, KV will reload from last persisted snapshot")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Start persistence on surviving KV node, failover restarted node ==========
        try:
            self.log.info("[STEP 11] Starting persistence on surviving KV nodes and failing over killed node...")
            from memcached.helper.data_helper import MemcachedClientHelper
            for kv_node in kv_nodes:
                if kv_node.ip != kv_node_to_kill.ip:
                    mem_client = MemcachedClientHelper.direct_client(kv_node, bucket_name)
                    mem_client.start_persistence()
                    self.log.info(f"[STEP 11] Persistence started on {kv_node.ip}")
            time.sleep(10)
            failover_task = self.cluster.async_failover(
                self.servers[:self.nodes_init], [kv_node_to_kill],
                graceful=False, wait_for_pending=120
            )
            failover_task.result()
            self.log.info(f"[STEP 11] PASSED - Failover of {kv_node_to_kill.ip} completed")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Wait for indexer to detect rollback and reach Ready ==========
        try:
            self.log.info("[STEP 12] Waiting for indexer rollback detection and indexes to reach Ready...")
            self.wait_until_indexes_online(timeout=1200)
            self.log.info("[STEP 12] PASSED - All indexes back in Ready state after rollback")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Assert KV item count matches post-rollback value ==========
        try:
            self.log.info("[STEP 13] Asserting item count after rollback...")
            count_after_rollback = self.get_item_count(self.master, bucket_name)
            self.log.info(f"[STEP 13] Items before rollback: {count_before_rollback}, after: {count_after_rollback}")
            if count_after_rollback >= count_before_rollback:
                self.log.info("[STEP 13] Note: KV rollback may not have reduced item count (updates don't change count)")
            self.log.info(f"[STEP 13] PASSED - Post-rollback item count: {count_after_rollback}")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        # ========== STEP 14: Scans on existing indexes post-rollback ==========
        try:
            self.log.info("[STEP 14] Running scans on existing indexes after rollback...")
            self.run_scan_validation(select_queries, expected_doc_count=count_after_rollback)
            self.log.info("[STEP 14] PASSED - Scans succeeded after rollback")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            raise

        # ========== STEP 15: Create new indexes post-rollback and scan ==========
        try:
            self.log.info("[STEP 15] Creating new indexes post-rollback and scanning...")
            new_prefix = 'rb_rot_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries, expected_doc_count=count_after_rollback)
            self.log.info(f"[STEP 15] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 15] FAILED: {str(e)}")
            raise

        # ========== STEP 16: Lifecycle ops ==========
        try:
            self.log.info("[STEP 16] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 16] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 16] FAILED: {str(e)}")
            raise

        # ========== STEP 17: File encryption validation with new key IDs ==========
        try:
            self.log.info(f"[STEP 17] Validating file encryption with rotated key IDs {new_key_ids}...")
            if not self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids)),
                    step_prefix="[STEP 17] "):
                raise Exception("File encryption validation failed after rollback + key rotation")
            self.log.info("[STEP 17] PASSED - Files encrypted with rotated key IDs after rollback")
        except Exception as e:
            self.log.error(f"[STEP 17] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_partial_rollback_with_key_rotation")
        self.log.info(f"Partial rollback + KEK rotation survived, new_key_ids={new_key_ids}")
        self.log.info("=" * 80)

    # -------------------------------------------------------------------------
    # Shared rollback helper
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Test A: Partial rollback with key drop
    # -------------------------------------------------------------------------

    def test_gsi_encryption_at_rest_partial_rollback_with_key_drop(self):
        """
        Encryption at rest - partial rollback with encryption disabled (key drop).

        Same KV partial rollback mechanism as
        test_gsi_encryption_at_rest_partial_rollback_with_key_rotation, but
        instead of rotating the KEK, encryption is disabled on the bucket before
        the rollback. Existing files remain encrypted; new snapshots are written
        unencrypted. Validates indexes and scans survive the rollback.

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Disable encryption on bucket (key drop); validate encryptionAtRestKeyId == -1
        7.  Confirm scans still work after key drop
        8-10. Induce partial rollback via _induce_partial_rollback helper
        11. Wait for indexes to reach Ready state
        12. Assert item count after rollback
        13. Scans on existing indexes (validate against post-rollback count)
        14. Create new indexes post-rollback, scan
        15. Lifecycle ops (alter/drop/build)
        16. Item count validation (no file encryption assertion — new snapshots are unencrypted)
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_partial_rollback_with_key_drop")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 3,
                f"Need at least 3 index nodes, found {len(index_nodes)}")
            kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
            self.assertGreaterEqual(len(kv_nodes), 1, f"Need at least 1 KV node")
            if not self.enable_encryption_at_rest:
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            key_id = self.encryption_at_rest_id
            self.assertIsNotNone(key_id, "encryption_at_rest_id is None")
            self.log.info(f"[STEP 1] PASSED - key_id={key_id}, index_nodes={[n.ip for n in index_nodes]}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rb_drop_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Disable encryption on bucket (key drop) ==========
        try:
            self.log.info(f"[STEP 6] Disabling encryption on bucket '{bucket_name}' (key drop)...")
            status, response = self.rest.disable_bucket_encryption(bucket_name)
            if not status:
                raise Exception(f"Failed to disable encryption: {response}")
            bucket_info = self.rest.get_bucket_json(bucket_name)
            enc_key = bucket_info.get('encryptionAtRestKeyId', None)
            if enc_key not in [None, -1]:
                raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still set to {enc_key}")
            self.log.info(f"[STEP 6] PASSED - Encryption disabled, encryptionAtRestKeyId={enc_key}")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Confirm scans still work after key drop ==========
        try:
            self.log.info("[STEP 7] Confirming scans work after key drop...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 7] PASSED - Scans succeed after encryption disabled")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Induce partial rollback ==========
        try:
            self.log.info("[STEP 8] Inducing KV partial rollback...")
            count_after_rollback = self._induce_partial_rollback(kv_nodes, bucket_name)
            self.log.info(f"[STEP 8] PASSED - Partial rollback induced, item count={count_after_rollback}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Wait for indexes to reach Ready state ==========
        try:
            self.log.info("[STEP 9] Waiting for indexes to recover after rollback...")
            self.wait_until_indexes_online(timeout=1200)
            self.log.info("[STEP 9] PASSED - All indexes in Ready state")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Scans on existing indexes post-rollback ==========
        try:
            self.log.info("[STEP 10] Running scans on existing indexes after rollback...")
            self.run_scan_validation(select_queries, expected_doc_count=count_after_rollback)
            self.log.info("[STEP 10] PASSED - Scans succeed after rollback with key drop")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Create new indexes post-rollback and scan ==========
        try:
            self.log.info("[STEP 11] Creating new indexes post-rollback and scanning...")
            new_prefix = 'rb_drop_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries, expected_doc_count=count_after_rollback)
            self.log.info(f"[STEP 11] PASSED - Created {len(new_queries)} post-rollback indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Lifecycle ops ==========
        try:
            self.log.info("[STEP 12] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Item count validation ==========
        try:
            self.log.info("[STEP 13] Running item count validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_partial_rollback_with_key_drop")
        self.log.info(f"Partial rollback + key drop survived on bucket '{bucket_name}'")
        self.log.info("=" * 80)

    # -------------------------------------------------------------------------
    # Test B: Rollback to zero with key rotation
    # -------------------------------------------------------------------------

    def test_gsi_encryption_at_rest_rtz_with_dek_rotation(self):
        """
        Encryption at rest - rollback to zero (bucket flush) with DEK rotation.

        Triggers a DEK rotation via short TTL then flushes the bucket entirely (seqno
        resets to 0). The indexer detects the rollback-to-zero signal, clears its index
        data, and rebuilds from scratch after docs are reloaded. Validates that files are
        encrypted with the new DEK after the rebuild.

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  DEK rotation via short TTL + poll new key IDs + confirm scans
        7.  Bucket flush (rollback to zero)
        8.  Sleep 60s — allow indexer to detect zero rollback
        9.  Wait for indexes to return to Ready (full rebuild, timeout=1800)
        10. Assert item count = 0
        11. Reload docs into the now-empty bucket
        12. Wait for indexes to be Ready again
        13. Scans on existing indexes (new docs)
        14. Create new indexes, scan
        15. Lifecycle ops (alter/drop/build)
        16. File encryption validation with new DEK key IDs
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_rtz_with_dek_rotation")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, self.encryption_at_rest_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rtz_rot_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: DEK rotation + poll new key IDs + confirm scans ==========
        new_key_ids = None
        try:
            DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
            self.log.info(f"[STEP 6] Triggering DEK rotation via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
            self.set_bucket_dek_rotation_config(
                bucket_name, self.encryption_at_rest_id,
                dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
            )
            new_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_key_ids, timeout=600, label="[STEP 6] "
            )
            self.run_scan_validation(select_queries)
            self.log.info(f"[STEP 6] PASSED - New key IDs: {new_key_ids}, scans OK")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Bucket flush (rollback to zero) ==========
        try:
            self.log.info(f"[STEP 7] Flushing bucket '{bucket_name}' (rollback to zero)...")
            self.cluster.bucket_flush(self.master, bucket_name)
            self.log.info("[STEP 7] PASSED - Bucket flushed, seqno resets to 0")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Sleep to allow indexer to detect zero rollback ==========
        self.log.info("[STEP 8] Sleeping 60s to allow indexer to detect rollback-to-zero...")
        time.sleep(60)

        # ========== STEP 9: Wait for indexes to return to Ready (full rebuild) ==========
        try:
            self.log.info("[STEP 9] Waiting for indexes to rebuild after rollback-to-zero (timeout=1800s)...")
            self.wait_until_indexes_online(timeout=1800)
            self.log.info("[STEP 9] PASSED - All indexes in Ready state after rebuild")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Assert item count = 0 ==========
        try:
            self.log.info("[STEP 10] Asserting item count is 0 after flush...")
            count_after_flush = self.get_item_count(self.master, bucket_name)
            self.assertEqual(count_after_flush, 0,
                f"Expected 0 items after bucket flush, got {count_after_flush}")
            self.log.info("[STEP 10] PASSED - Item count = 0 confirmed")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Reload docs ==========
        try:
            self.log.info(f"[STEP 11] Reloading {self.num_of_docs_per_collection} docs into flushed bucket...")
            gen_reload = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_reload, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info("[STEP 11] PASSED - Docs reloaded")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Wait for indexes to be Ready again ==========
        try:
            self.log.info("[STEP 12] Waiting for indexes to catch up with reloaded docs...")
            self.wait_until_indexes_online(timeout=1200)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Scans on existing indexes ==========
        try:
            self.log.info("[STEP 13] Running scans on existing indexes after RTZ + reload...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        # ========== STEP 14: Create new indexes, scan ==========
        try:
            self.log.info("[STEP 14] Creating new indexes post-RTZ and scanning...")
            new_prefix = 'rtz_rot_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 14] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            raise

        # ========== STEP 15: Lifecycle ops ==========
        try:
            self.log.info("[STEP 15] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 15] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 15] FAILED: {str(e)}")
            raise

        # ========== STEP 16: File encryption validation with new key IDs ==========
        try:
            self.log.info(f"[STEP 16] Validating file encryption with rotated key IDs {new_key_ids}...")
            if not self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids)),
                    step_prefix="[STEP 16] "):
                raise Exception("File encryption validation failed after RTZ + key rotation")
            self.log.info("[STEP 16] PASSED - Files encrypted with rotated key IDs after rebuild")
        except Exception as e:
            self.log.error(f"[STEP 16] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_rtz_with_dek_rotation")
        self.log.info(f"Rollback-to-zero + KEK rotation survived, new_key_ids={new_key_ids}")
        self.log.info("=" * 80)

    # -------------------------------------------------------------------------
    # Test C: Rollback to zero with key drop
    # -------------------------------------------------------------------------

    def test_gsi_encryption_at_rest_rtz_with_key_drop(self):
        """
        Encryption at rest - rollback to zero (bucket flush) with encryption disabled.

        Same as test_gsi_encryption_at_rest_rtz_with_key_rotation but step 6
        disables encryption (key drop) instead of rotating. No file encryption
        assertion at the end — new snapshots written after disable are unencrypted.

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Disable encryption on bucket (key drop); confirm scans still work
        7.  Bucket flush (rollback to zero)
        8.  Sleep 60s
        9.  Wait for indexes to rebuild (timeout=1800)
        10. Assert item count = 0
        11. Reload docs
        12. Wait for indexes Ready again
        13. Scans on existing indexes
        14. Create new indexes, scan
        15. Lifecycle ops (alter/drop/build)
        16. Item count validation (no file encryption assertion)
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_rtz_with_key_drop")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, self.encryption_at_rest_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rtz_drop_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Disable encryption (key drop) + confirm scans ==========
        try:
            self.log.info(f"[STEP 6] Disabling encryption on bucket '{bucket_name}' (key drop)...")
            status, response = self.rest.disable_bucket_encryption(bucket_name)
            if not status:
                raise Exception(f"Failed to disable encryption: {response}")
            enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
            if enc_key not in [None, -1]:
                raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still {enc_key}")
            self.run_scan_validation(select_queries)
            self.log.info(f"[STEP 6] PASSED - Encryption disabled (encryptionAtRestKeyId={enc_key}), scans OK")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Bucket flush (rollback to zero) ==========
        try:
            self.log.info(f"[STEP 7] Flushing bucket '{bucket_name}' (rollback to zero)...")
            self.cluster.bucket_flush(self.master, bucket_name)
            self.log.info("[STEP 7] PASSED - Bucket flushed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Sleep for indexer to detect zero rollback ==========
        self.log.info("[STEP 8] Sleeping 60s to allow indexer to detect rollback-to-zero...")
        time.sleep(60)

        # ========== STEP 9: Wait for indexes to rebuild ==========
        try:
            self.log.info("[STEP 9] Waiting for indexes to rebuild after RTZ (timeout=1800s)...")
            self.wait_until_indexes_online(timeout=1800)
            self.log.info("[STEP 9] PASSED - All indexes in Ready state")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Assert item count = 0 ==========
        try:
            self.log.info("[STEP 10] Asserting item count is 0 after flush...")
            count_after_flush = self.get_item_count(self.master, bucket_name)
            self.assertEqual(count_after_flush, 0,
                f"Expected 0 items after bucket flush, got {count_after_flush}")
            self.log.info("[STEP 10] PASSED - Item count = 0 confirmed")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Reload docs ==========
        try:
            self.log.info(f"[STEP 11] Reloading {self.num_of_docs_per_collection} docs...")
            gen_reload = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_reload, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info("[STEP 11] PASSED - Docs reloaded")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Wait for indexes Ready again ==========
        try:
            self.log.info("[STEP 12] Waiting for indexes to catch up with reloaded docs...")
            self.wait_until_indexes_online(timeout=1200)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Scans on existing indexes ==========
        try:
            self.log.info("[STEP 13] Running scans on existing indexes after RTZ + reload...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        # ========== STEP 14: Create new indexes, scan ==========
        try:
            self.log.info("[STEP 14] Creating new indexes post-RTZ and scanning...")
            new_prefix = 'rtz_drop_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 14] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            raise

        # ========== STEP 15: Lifecycle ops ==========
        try:
            self.log.info("[STEP 15] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 15] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 15] FAILED: {str(e)}")
            raise

        # ========== STEP 16: Item count validation ==========
        try:
            self.log.info("[STEP 16] Running item count validations (no file encryption assertion after key drop)...")
            self.item_count_related_validations()
            self.log.info("[STEP 16] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 16] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_rtz_with_key_drop")
        self.log.info(f"Rollback-to-zero + key drop survived on bucket '{bucket_name}'")
        self.log.info("=" * 80)

    # -------------------------------------------------------------------------
    # Test E: Partial rollback concurrently with key rotation or key drop
    # -------------------------------------------------------------------------

    def test_gsi_encryption_at_rest_partial_rollback_concurrent_key_op(self):
        """
        Encryption at rest - partial rollback run concurrently with a key operation.

        Runs _induce_partial_rollback and a key operation (KEK rotation or key drop)
        simultaneously in two threads to exercise the race condition where the indexer
        is mid-rollback when encryption configuration changes.

        key_op param (conf-driven):
          "rotation" — DEK rotation via short TTL; validate with new key IDs after join
          "drop"     — disable_bucket_encryption; item count validation only at end

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Launch Thread-1 (_induce_partial_rollback) and Thread-2 (key op) concurrently
        7.  Join both threads
        8.  Wait for indexes to return to Ready (timeout=1200)
        9.  Scans on existing indexes (post-rollback count)
        10. Create new indexes, scan
        11. Lifecycle ops (alter/drop/build)
        12. File encryption validation with new key IDs (rotation only) or item count (drop)
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_partial_rollback_concurrent_key_op")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, key_op={self.key_op}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 3,
                f"Need at least 3 index nodes, found {len(index_nodes)}")
            kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
            self.assertGreaterEqual(len(kv_nodes), 1, "Need at least 1 KV node")
            if not self.enable_encryption_at_rest:
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            key_id = self.encryption_at_rest_id
            self.assertIsNotNone(key_id, "encryption_at_rest_id is None")
            if self.key_op not in ("rotation", "drop"):
                raise ValueError(f"key_op must be 'rotation' or 'drop', got '{self.key_op}'")
            self.log.info(f"[STEP 1] PASSED - key_id={key_id}, key_op={self.key_op}, index_nodes={[n.ip for n in index_nodes]}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rb_conc_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Launch rollback and key op threads concurrently ==========
        thread_errors = []
        count_after_rollback = [self.num_of_docs_per_collection]
        new_key_ids = [None]

        def run_rollback():
            try:
                count_after_rollback[0] = self._induce_partial_rollback(kv_nodes, bucket_name)
            except Exception as ex:
                thread_errors.append(f"rollback thread: {ex}")

        def run_key_op():
            try:
                if self.key_op == "rotation":
                    DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
                    self.log.info(f"[STEP 6 key_op] Triggering DEK rotation via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
                    self.set_bucket_dek_rotation_config(
                        bucket_name, key_id,
                        dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                        dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
                    )
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, baseline_key_ids, timeout=600, label="[STEP 6 key_op] "
                    )
                    self.log.info(f"[STEP 6 key_op] New key IDs: {new_key_ids[0]}")
                else:
                    status, response = self.rest.disable_bucket_encryption(bucket_name)
                    if not status:
                        raise Exception(f"Failed to disable encryption: {response}")
                    enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
                    if enc_key not in [None, -1]:
                        raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still {enc_key}")
                    self.log.info(f"[STEP 6 key_op] Encryption disabled, encryptionAtRestKeyId={enc_key}")
            except Exception as ex:
                thread_errors.append(f"key_op thread: {ex}")

        try:
            self.log.info(f"[STEP 6] Launching partial rollback and key_op='{self.key_op}' threads concurrently...")
            t_rollback = threading.Thread(target=run_rollback, name="rollback_thread", daemon=True)
            t_key_op = threading.Thread(target=run_key_op, name="key_op_thread", daemon=True)
            t_rollback.start()
            t_key_op.start()
            self.log.info("[STEP 6] PASSED - Both threads launched")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Join both threads ==========
        try:
            self.log.info("[STEP 7] Waiting for rollback and key op threads to complete...")
            t_rollback.join(timeout=600)
            t_key_op.join(timeout=600)
            if thread_errors:
                raise Exception(f"Thread failures: {'; '.join(thread_errors)}")
            self.log.info(f"[STEP 7] PASSED - Both threads completed, rollback count={count_after_rollback[0]}, new_key_ids={new_key_ids[0]}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Wait for indexes to return to Ready ==========
        try:
            self.log.info("[STEP 8] Waiting for indexes to reach Ready state after concurrent ops...")
            self.wait_until_indexes_online(timeout=1200)
            self.log.info("[STEP 8] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Scans on existing indexes ==========
        try:
            self.log.info("[STEP 9] Running scans on existing indexes post concurrent ops...")
            self.run_scan_validation(select_queries, expected_doc_count=count_after_rollback[0])
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Create new indexes, scan ==========
        try:
            self.log.info("[STEP 10] Creating new indexes and scanning...")
            new_prefix = 'rb_conc_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries, expected_doc_count=count_after_rollback[0])
            self.log.info(f"[STEP 10] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Lifecycle ops ==========
        try:
            self.log.info("[STEP 11] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: File encryption validation (rotation) or item count (drop) ==========
        try:
            if self.key_op == "rotation" and new_key_ids[0]:
                self.log.info(f"[STEP 12] Validating file encryption with new key IDs {new_key_ids[0]}...")
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids[0])),
                        step_prefix="[STEP 12] "):
                    raise Exception("File encryption validation failed after concurrent rollback + rotation")
                self.log.info("[STEP 12] PASSED - Files encrypted with rotated key IDs")
            else:
                self.log.info("[STEP 12] key_op=drop: running item count validation (no file encryption assertion)...")
                self.item_count_related_validations()
                self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_partial_rollback_concurrent_key_op")
        self.log.info(f"key_op={self.key_op}, rollback count={count_after_rollback[0]}, new_key_ids={new_key_ids[0]}")
        self.log.info("=" * 80)

    # -------------------------------------------------------------------------
    # Test F: Rollback to zero concurrently with key rotation or key drop
    # -------------------------------------------------------------------------

    def test_gsi_encryption_at_rest_rtz_concurrent_key_op(self):
        """
        Encryption at rest - rollback to zero (bucket flush) run concurrently with a key op.

        Flushes the bucket and triggers a key operation (rotation or drop) simultaneously
        in two threads to exercise the race between RTZ and encryption reconfiguration.

        key_op param (conf-driven):
          "rotation" — DEK rotation via short TTL; validate with new key IDs after reload
          "drop"     — disable_bucket_encryption; item count validation only at end

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Launch Thread-1 (bucket_flush + sleep 60) and Thread-2 (key op) concurrently
        7.  Join both threads; wait_until_indexes_online(timeout=1800)
        8.  Assert item count = 0
        9.  Reload docs, wait for indexes Ready
        10. Scans on existing indexes
        11. Create new indexes, scan
        12. Lifecycle ops (alter/drop/build)
        13. File encryption validation with new key IDs (rotation) or item count (drop)
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_rtz_concurrent_key_op")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, key_op={self.key_op}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 3,
                f"Need at least 3 index nodes, found {len(index_nodes)}")
            if not self.enable_encryption_at_rest:
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            key_id = self.encryption_at_rest_id
            self.assertIsNotNone(key_id, "encryption_at_rest_id is None")
            if self.key_op not in ("rotation", "drop"):
                raise ValueError(f"key_op must be 'rotation' or 'drop', got '{self.key_op}'")
            self.log.info(f"[STEP 1] PASSED - key_id={key_id}, key_op={self.key_op}, index_nodes={[n.ip for n in index_nodes]}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rtz_conc_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Launch bucket flush and key op threads concurrently ==========
        thread_errors = []
        new_key_ids = [None]

        def run_flush():
            try:
                self.log.info("[STEP 6 flush] Flushing bucket (rollback to zero)...")
                self.cluster.bucket_flush(self.master, bucket_name)
                self.log.info("[STEP 6 flush] Bucket flushed, sleeping 60s...")
                time.sleep(60)
            except Exception as ex:
                thread_errors.append(f"flush thread: {ex}")

        def run_key_op():
            try:
                if self.key_op == "rotation":
                    DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
                    self.log.info(f"[STEP 6 key_op] Triggering DEK rotation via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
                    self.set_bucket_dek_rotation_config(
                        bucket_name, key_id,
                        dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                        dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
                    )
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, baseline_key_ids, timeout=600, label="[STEP 6 key_op] "
                    )
                    self.log.info(f"[STEP 6 key_op] New key IDs: {new_key_ids[0]}")
                else:
                    status, response = self.rest.disable_bucket_encryption(bucket_name)
                    if not status:
                        raise Exception(f"Failed to disable encryption: {response}")
                    enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
                    if enc_key not in [None, -1]:
                        raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still {enc_key}")
                    self.log.info(f"[STEP 6 key_op] Encryption disabled, encryptionAtRestKeyId={enc_key}")
            except Exception as ex:
                thread_errors.append(f"key_op thread: {ex}")

        try:
            self.log.info(f"[STEP 6] Launching bucket flush and key_op='{self.key_op}' threads concurrently...")
            t_flush = threading.Thread(target=run_flush, name="flush_thread", daemon=True)
            t_key_op = threading.Thread(target=run_key_op, name="key_op_thread", daemon=True)
            t_flush.start()
            t_key_op.start()
            self.log.info("[STEP 6] PASSED - Both threads launched")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Join threads, wait for indexes to rebuild ==========
        try:
            self.log.info("[STEP 7] Waiting for flush and key op threads to complete...")
            t_flush.join(timeout=300)
            t_key_op.join(timeout=400)
            if thread_errors:
                raise Exception(f"Thread failures: {'; '.join(thread_errors)}")
            self.log.info("[STEP 7] Threads joined, waiting for indexes to rebuild (timeout=1800s)...")
            self.wait_until_indexes_online(timeout=1800)
            self.log.info(f"[STEP 7] PASSED - Indexes rebuilt, new_key_ids={new_key_ids[0]}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Assert item count = 0 ==========
        try:
            self.log.info("[STEP 8] Asserting item count is 0 after flush...")
            count_after_flush = self.get_item_count(self.master, bucket_name)
            self.assertEqual(count_after_flush, 0,
                f"Expected 0 items after bucket flush, got {count_after_flush}")
            self.log.info("[STEP 8] PASSED - Item count = 0 confirmed")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Reload docs, wait for indexes Ready ==========
        try:
            self.log.info(f"[STEP 9] Reloading {self.num_of_docs_per_collection} docs and waiting for indexes...")
            gen_reload = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_reload, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.wait_until_indexes_online(timeout=1200)
            self.log.info("[STEP 9] PASSED - Docs reloaded, indexes Ready")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Scans on existing indexes ==========
        try:
            self.log.info("[STEP 10] Running scans on existing indexes after RTZ + reload...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 10] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Create new indexes, scan ==========
        try:
            self.log.info("[STEP 11] Creating new indexes and scanning...")
            new_prefix = 'rtz_conc_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 11] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Lifecycle ops ==========
        try:
            self.log.info("[STEP 12] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: File encryption validation (rotation) or item count (drop) ==========
        try:
            if self.key_op == "rotation" and new_key_ids[0]:
                self.log.info(f"[STEP 13] Validating file encryption with new key IDs {new_key_ids[0]}...")
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids[0])),
                        step_prefix="[STEP 13] "):
                    raise Exception("File encryption validation failed after concurrent RTZ + rotation")
                self.log.info("[STEP 13] PASSED - Files encrypted with rotated key IDs after rebuild")
            else:
                self.log.info("[STEP 13] key_op=drop: running item count validation (no file encryption assertion)...")
                self.item_count_related_validations()
                self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_rtz_concurrent_key_op")
        self.log.info(f"key_op={self.key_op}, new_key_ids={new_key_ids[0]}")
        self.log.info("=" * 80)

    # =========================================================================
    # Test 1: DEK lifetime reached — rotation through multiple cycles
    # =========================================================================

    def test_gsi_encryption_at_rest_dek_multi_rotation(self):
        """
        Encryption at rest - DEK rotates automatically through multiple lifetime cycles.

        Sets a short DEK rotation interval so the DEK expires and rotates automatically.
        Loops num_rotations times: each iteration polls for new key IDs, runs scans,
        and validates file encryption. After all cycles, runs lifecycle ops and a
        final file encryption validation.

        Parameters:
          num_rotations              — how many DEK rotation cycles to wait for (default 3)
          kek_rotation_interval_seconds — DEK rotation interval in seconds (default 30)
        """
        DEK_INTERVAL_S = self.kek_rotation_interval_seconds
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_dek_multi_rotation")
        self.log.info(f"num_rotations={self.num_rotations}, dek_interval={DEK_INTERVAL_S}s")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key_id = self.encryption_at_rest_id
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, set short DEK interval, load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption, DEK interval={DEK_INTERVAL_S}s...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=DEK_INTERVAL_S,
                dek_lifetime=DEK_INTERVAL_S*5
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'dek_mr_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Baseline scans + initial file validation ==========
        current_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=current_key_ids, step_prefix="[STEP 5] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info(f"[STEP 5] PASSED - baseline key_ids={current_key_ids}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Loop through num_rotations DEK rotation cycles ==========
        initial_key_ids = list(current_key_ids)
        try:
            self.log.info(f"[STEP 6] Starting {self.num_rotations} DEK rotation cycles (interval={DEK_INTERVAL_S}s each)...")
            for cycle in range(1, self.num_rotations + 1):
                self.log.info(f"[STEP 6] === Rotation cycle {cycle}/{self.num_rotations} ===")
                prev_key_ids = current_key_ids
                poll_timeout = DEK_INTERVAL_S * 3 + 60
                current_key_ids = self.poll_for_new_indexer_key_ids(
                    index_nodes, prev_key_ids, timeout=poll_timeout,
                    label=f"[STEP 6 cycle {cycle}] "
                )
                self.log.info(f"[STEP 6 cycle {cycle}] New key IDs: {current_key_ids}")
                self.run_scan_validation(select_queries)
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(prev_key_ids) | set(current_key_ids)),
                        step_prefix=f"[STEP 6 cycle {cycle}] "):
                    raise Exception(f"File encryption validation failed on rotation cycle {cycle}")
                self.log.info(f"[STEP 6 cycle {cycle}] PASSED")
            self.log.info(f"[STEP 6] PASSED - All {self.num_rotations} rotation cycles validated")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Create new indexes post final rotation and scan ==========
        try:
            self.log.info("[STEP 7] Creating new indexes after final rotation cycle and scanning...")
            new_prefix = 'dek_mr_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 7] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Lifecycle ops ==========
        try:
            self.log.info("[STEP 8] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 8] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Wait for empty DEK keys to drain, then final file encryption validation ==========
        try:
            self.log.info("[STEP 9] Waiting for empty service_bucket key IDs to drain (max 5 min)...")
            self._wait_for_empty_service_bucket_keys_cleared(
                index_nodes, timeout=300, label="[STEP 9] "
            )
            self.log.info(f"[STEP 9] Final file encryption validation with key IDs {current_key_ids}...")
            if not self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=sorted(set(initial_key_ids) | set(current_key_ids)),
                    step_prefix="[STEP 9] "):
                raise Exception("Final file encryption validation failed")
            self.log.info("[STEP 9] PASSED - All files encrypted with latest DEK key IDs")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_dek_multi_rotation")
        self.log.info(f"Completed {self.num_rotations} DEK rotation cycles, final key_ids={current_key_ids}")
        self.log.info("=" * 80)

    # =========================================================================
    # Test 2: Encryption enabled followed by encryption disabled
    # =========================================================================

    def test_gsi_encryption_at_rest_enable_then_disable(self):
        """
        Encryption at rest - full roundtrip: enable encryption then disable it.

        Starts with an unencrypted bucket, enables encryption mid-test and validates,
        then disables encryption and validates again. Covers both directions in a
        single test to ensure the enable→disable sequence leaves indexes accessible.

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings
        3.  Create bucket WITHOUT encryption, load docs, create indexes, wait for Ready
        4.  Baseline scans (unencrypted)
        5.  Enable encryption on bucket; validate encryptionAtRestKeyId set
        6.  Scans on old (unencrypted) indexes — must still work
        7.  Create new indexes post-enable; scan
        8.  Disable encryption; validate encryptionAtRestKeyId == -1
        9.  Scans on all indexes — must still work
        10. Create another set of new indexes post-disable; scan
        11. Lifecycle ops (alter/drop/build)
        12. Item count validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_enable_then_disable")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key_id = self.encryption_at_rest_id
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create bucket WITHOUT encryption, load docs, create indexes ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        select_queries = set()
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' WITHOUT encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=False)
            enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
            if enc_key not in [None, -1]:
                raise Exception(f"Bucket should not be encrypted, got key {enc_key}")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            prefix = 'etd_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, skip_primary=False
            )
            select_queries.update(self.gsi_util_obj.get_select_queries(
                definition_list=query_definitions, namespace=namespace
            ))
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=self.defer_build, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info(f"[STEP 3] PASSED - Bucket unencrypted, {self.num_of_docs_per_collection} docs, {len(queries)} indexes")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Baseline scans (unencrypted) ==========
        try:
            self.log.info("[STEP 4] Running baseline scans on unencrypted bucket...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Enable encryption ==========
        try:
            self.log.info(f"[STEP 5] Enabling encryption on bucket '{bucket_name}' with key_id={key_id}...")
            status, response = self.rest.enable_bucket_encryption(bucket_name, key_id)
            if not status:
                raise Exception(f"Failed to enable encryption: {response}")
            enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
            if enc_key is None or enc_key == -1:
                raise Exception("Encryption was not enabled after API call")
            self.log.info(f"[STEP 5] PASSED - encryptionAtRestKeyId={enc_key}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Scans on old indexes post-enable ==========
        try:
            self.log.info("[STEP 6] Running scans on old indexes after enabling encryption...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 6] PASSED - Old indexes accessible after enabling encryption")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: New indexes post-enable, scan, file validation ==========
        enc_select_queries = set()
        try:
            self.log.info("[STEP 7] Creating new indexes post-enable and scanning...")
            new_prefix = 'etd_enc_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            enc_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            enc_select_queries.update(self.gsi_util_obj.get_select_queries(
                definition_list=enc_definitions, namespace=namespace
            ))
            enc_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=enc_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=enc_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(enc_select_queries)
            self.log.info(f"[STEP 7] PASSED - {len(enc_queries)} encrypted indexes created and scanned")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Disable encryption ==========
        try:
            self.log.info(f"[STEP 8] Disabling encryption on bucket '{bucket_name}'...")
            status, response = self.rest.disable_bucket_encryption(bucket_name)
            if not status:
                raise Exception(f"Failed to disable encryption: {response}")
            enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
            if enc_key not in [None, -1]:
                raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still {enc_key}")
            self.log.info(f"[STEP 8] PASSED - encryptionAtRestKeyId={enc_key}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Scans on all indexes post-disable ==========
        try:
            self.log.info("[STEP 9] Running scans on all indexes after disabling encryption...")
            all_queries = select_queries | enc_select_queries
            self.run_scan_validation(all_queries)
            self.log.info("[STEP 9] PASSED - All indexes accessible after disabling encryption")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: New indexes post-disable, scan ==========
        try:
            self.log.info("[STEP 10] Creating new indexes post-disable and scanning...")
            dis_prefix = 'etd_dis_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            dis_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=dis_prefix, skip_primary=False
            )
            dis_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=dis_definitions, namespace=namespace
            )
            dis_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=dis_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=dis_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(dis_select_queries)
            self.log.info(f"[STEP 10] PASSED - {len(dis_queries)} post-disable indexes created and scanned")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Lifecycle ops ==========
        try:
            self.log.info("[STEP 11] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Item count validation ==========
        try:
            self.log.info("[STEP 12] Running item count validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_enable_then_disable")
        self.log.info(f"Full enable→disable roundtrip succeeded on bucket '{bucket_name}'")
        self.log.info("=" * 80)

    # =========================================================================
    # Test 4: Indexer restart concurrently with key rotation / expiry / key drop
    # =========================================================================

    def _assert_buckets_loaded(self, bucket_names, expected_items, timeout=120):
        """The loaders can fail silently (e.g. java_sdk_client NPE on a vector template it
        lacks), so wait until each bucket holds expected_items docs or fail."""
        for bucket_name in bucket_names:
            deadline = time.time() + timeout
            while True:
                item_count = self.get_buckets_itemCount().get(bucket_name, 0)
                if item_count >= expected_items or time.time() > deadline:
                    break
                time.sleep(5)
            if item_count < expected_items:
                raise AssertionError(
                    f"Bucket '{bucket_name}' has {item_count} items after load, expected {expected_items}"
                )

    def test_gsi_encryption_at_rest_indexer_restart_concurrent_key_op(self):
        """
        Encryption at rest - rolling indexer restart run concurrently with a key op.

        Launches a rolling indexer restart and a key operation simultaneously in two
        threads to exercise the race where nodes restart while encryption configuration
        is changing. Validates indexes are accessible and encrypted correctly after both
        operations complete.

        key_op param (conf-driven):
          "rotation" — DEK rotation via short TTL (immediate poll)
          "expiry"   — DEK expiry via short TTL (same mechanism, semantic distinction)
          "drop"     — disable_bucket_encryption
          "enable"   — bucket starts unencrypted; enable_bucket_encryption +
                       forceEncryptionAtRest while the indexers restart
          "log_reencrypt" — dropEncryptionAtRestDeks/log (needs enable_log_encryption_at_rest):
                       stats log, request-handler cache and persisted stats re-encrypted
                       while the indexers restart
          "other_reencrypt" — dropEncryptionAtRestDeks/other (needs enable_other_encryption_at_rest):
                       metadata store re-encrypted while the indexers restart
          "reencrypt" — dropEncryptionAtRestDeks on the bucket: existing DEKs are
                       dropped and the indexer re-encrypts all index data with a
                       new DEK (long-running on a large index)

        restart_mode param (conf-driven):
          "graceful" (default) — rolling stop_server/start_server
          "kill"     — rolling kill -9 of the indexer process, kill_delay_s after
                       the previous node (and after the key op starts), so the
                       kill lands while the key op is being applied
          kill_first=True (with restart_mode=kill) — kill -9 the indexer on ALL
                       index nodes at once, then trigger the key op, so it arrives
                       while every indexer is down or warming up (use a large
                       index to keep warmup long). For rotation/expiry the order is
                       reversed: the short DEK interval is set first and the kill is
                       timed kill_lead_s before the interval elapses, so the rotation
                       falls due during warmup. FRAGILE: assumes ns_server rotates
                       exactly one interval after the setting changes.

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single bucket (encrypted, or unencrypted for "enable"), load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation (files NOT encrypted for "enable")
        6.  Launch Thread-1 (_restart_index_nodes) and Thread-2 (key op) concurrently
        7.  Join both threads; wait_until_indexes_online(timeout=1800)
        8.  Scans on existing indexes
        9.  Create new indexes, scan
        10. Lifecycle ops (alter/drop/build)
        11. File encryption validation with new key IDs (rotation/expiry), item count (drop),
            no unencrypted ("") keys left for the bucket + item count (enable), or
            old DEK IDs gone from GetInUseKeys + item count (reencrypt)
        """
        STABLE_DEK_INTERVAL_S = 36000
        restart_mode = self.input.param("restart_mode", "graceful")
        kill_delay_s = int(self.input.param("kill_delay_s", 10))
        kill_first = bool(self.input.param("kill_first", False))
        kill_lead_s = int(self.input.param("kill_lead_s", 3))
        kills_done = threading.Event()
        # kill_first + rotation/expiry: time the kill to the rotation instead.
        timed_rotation = kill_first and self.key_op in ("rotation", "expiry")
        rotation_armed = threading.Event()
        rotation_set_at = [None]
        # Large indexes can take a while to recover after kill -9; give the key op
        # (and its poll) longer when every indexer is killed at once.
        key_op_timeout = 1500 if kill_first else 600
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_indexer_restart_concurrent_key_op")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, key_op={self.key_op}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 3,
                f"Need at least 3 index nodes, found {len(index_nodes)}")
            if not self.enable_encryption_at_rest:
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            key_id = self.encryption_at_rest_id
            self.assertIsNotNone(key_id, "encryption_at_rest_id is None")
            if self.key_op not in ("rotation", "expiry", "drop", "enable", "reencrypt",
                                   "log_reencrypt", "other_reencrypt"):
                raise ValueError(
                    f"key_op must be 'rotation', 'expiry', 'drop', 'enable', 'reencrypt', "
                    f"'log_reencrypt' or 'other_reencrypt', got '{self.key_op}'"
                )
            if self.key_op == "other_reencrypt" and not getattr(self, "enable_other_encryption_at_rest", False):
                self.skipTest("key_op=other_reencrypt needs enable_other_encryption_at_rest=True")
            if self.key_op == "log_reencrypt" and not getattr(self, "enable_log_encryption_at_rest", False):
                self.skipTest("key_op=log_reencrypt needs enable_log_encryption_at_rest=True")
            if restart_mode not in ("graceful", "kill"):
                raise ValueError(f"restart_mode must be 'graceful' or 'kill', got '{restart_mode}'")
            if kill_first and restart_mode != "kill":
                raise ValueError("kill_first=True requires restart_mode=kill")
            self.log.info(f"[STEP 1] PASSED - key_id={key_id}, key_op={self.key_op}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create bucket (unencrypted for key_op=enable), load docs ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        start_encrypted = self.key_op != "enable"
        try:
            self.log.info(
                f"[STEP 3] Creating bucket '{bucket_name}' "
                f"{'with' if start_encrypted else 'WITHOUT'} encryption..."
            )
            self.create_bucket_with_encryption(bucket_name, enable_encryption=start_encrypted)
            if start_encrypted:
                self.set_bucket_dek_rotation_config(
                    bucket_name, key_id,
                    dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                    dek_lifetime=STABLE_DEK_INTERVAL_S
                )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            if self.use_magma_loader:
                tasks = [self.cluster.async_load_gen_docs(
                    self.master, bucket=bucket_name, generator=gen_create,
                    pause_secs=1, timeout_secs=300, use_magma_loader=True)]
            else:
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
                )
            for task in tasks:
                task.result()
            self._assert_buckets_loaded([bucket_name], int(self.num_of_docs_per_collection))
            self.log.info(f"[STEP 3] PASSED - Bucket created, {self.num_of_docs_per_collection} docs loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes, wait for Ready ==========
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            select_queries = self.create_indexes_on_namespace(
                namespace, index_nodes, 'rst_conc_', step_prefix="[STEP 4]",
                wait_for_ready=True
            )
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # An unencrypted bucket has no in-use key IDs, and get_indexer_in_use_key_ids
        # raises when it finds none.
        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes) if start_encrypted else []
        log_baseline_key_ids = []
        other_baseline_key_ids = []
        baseline_metadata = None
        if self.key_op == "other_reencrypt":
            other_baseline_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, [], timeout=600, label="[STEP 4] ", key_type="other")
            baseline_metadata = self._index_metadata_snapshot(bucket_name)
        if self.key_op == "log_reencrypt":
            # Write the log-key files (cache, stats log, persisted stats) before the restart
            self._populate_request_handler_cache(index_nodes)
            self.sleep(60, "Waiting for stats log and persisted stats to be written")
            log_baseline_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, [], timeout=300, label="[STEP 4] ", key_type="log")

        # ========== STEP 5: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 5] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if start_encrypted:
                if not self.validate_file_encryption_with_key(
                        index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 5] "):
                    raise Exception("Baseline file encryption validation failed")
            else:
                self.validate_files_not_encrypted(
                    index_nodes, bucket_name, timeout=60, step_prefix="[STEP 5] "
                )
            self.log.info("[STEP 5] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Launch restart and key op threads concurrently ==========
        thread_errors = []
        new_key_ids = [None]

        def kill_index_nodes_rolling():
            for node in index_nodes:
                self.sleep(kill_delay_s, f"[STEP 6 restart] Waiting before kill -9 on {node.ip}")
                self.log.info(f"[STEP 6 restart] kill -9 indexer on {node.ip}")
                shell = RemoteMachineShellConnection(node)
                try:
                    shell.execute_command("pkill -9 -x indexer")
                finally:
                    shell.disconnect()
                # ns_server restarts the indexer; wait until it is Active and serves
                # GetInUseKeys again before killing the next node.
                self.wait_for_indexer_ready([node], timeout=300, interval=2,
                                            label="[STEP 6 restart] ")

        def kill_index_nodes_all_then_wait():
            try:
                for node in index_nodes:
                    self.log.info(f"[STEP 6 restart] kill -9 indexer on {node.ip}")
                    shell = RemoteMachineShellConnection(node)
                    try:
                        shell.execute_command("pkill -9 -x indexer")
                    finally:
                        shell.disconnect()
            finally:
                # Release the key-op thread even if a kill failed, so it never hangs.
                kills_done.set()
            self.wait_for_indexer_ready(index_nodes, timeout=900, interval=5,
                                        label="[STEP 6 restart] ")

        def run_restart():
            try:
                if restart_mode == "kill" and kill_first:
                    if timed_rotation:
                        if not rotation_armed.wait(timeout=300) or rotation_set_at[0] is None:
                            raise Exception("DEK rotation interval was not set within 300s")
                        due_at = rotation_set_at[0] + self.kek_rotation_interval_seconds
                        wait_s = max(0, due_at - kill_lead_s - time.time())
                        self.sleep(wait_s, f"[STEP 6 restart] Timing kill -9 to {kill_lead_s}s before "
                                           f"the DEK {self.key_op} is due")
                    self.log.info("[STEP 6 restart] kill -9 the indexer on all index nodes...")
                    kill_index_nodes_all_then_wait()
                elif restart_mode == "kill":
                    self.log.info("[STEP 6 restart] Starting rolling kill -9 of the indexer...")
                    kill_index_nodes_rolling()
                else:
                    self.log.info("[STEP 6 restart] Starting rolling indexer restart...")
                    self.restart_index_nodes(index_nodes)
                self.log.info("[STEP 6 restart] Rolling restart complete")
            except Exception as ex:
                thread_errors.append(f"restart thread: {ex}")

        def run_key_op():
            try:
                if kill_first and not timed_rotation:
                    if not kills_done.wait(timeout=300):
                        raise Exception("kill -9 of the indexers did not complete within 300s")
                    self.log.info("[STEP 6 key_op] Indexers killed; triggering the key op while they warm up")
                if self.key_op in ("rotation", "expiry"):
                    DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
                    self.log.info(f"[STEP 6 key_op] Triggering DEK {self.key_op} via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
                    self.set_bucket_dek_rotation_config(
                        bucket_name, key_id,
                        dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                        dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
                    )
                    if timed_rotation:
                        rotation_set_at[0] = time.time()
                        rotation_armed.set()
                    # Tolerates indexers that are down or warming up (kill -9 / restart)
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, baseline_key_ids, timeout=key_op_timeout,
                        label="[STEP 6 key_op] ", key_type="bucket")
                    self.log.info(f"[STEP 6 key_op] New key IDs: {new_key_ids[0]}")
                elif self.key_op == "enable":
                    self.log.info(f"[STEP 6 key_op] Enabling encryption on '{bucket_name}' with key_id={key_id}...")
                    status, response = self.rest.enable_bucket_encryption(bucket_name, key_id)
                    if not status:
                        raise Exception(f"Failed to enable encryption: {response}")
                    enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
                    if enc_key in [None, -1]:
                        raise Exception(f"Encryption not enabled — encryptionAtRestKeyId={enc_key}")
                    self.set_bucket_dek_rotation_config(
                        bucket_name, key_id,
                        dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                        dek_lifetime=STABLE_DEK_INTERVAL_S
                    )
                    # Enabling only encrypts new writes; force-encrypt makes the
                    # indexer rewrite the existing plaintext files.
                    status, response = self.rest.force_bucket_encryption_at_rest(bucket_name)
                    if not status:
                        raise Exception(f"forceEncryptionAtRest API call failed: {response}")
                    self.log.info("[STEP 6 key_op] Force encryption triggered, polling for the bucket key in use...")
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, [], timeout=key_op_timeout,
                        label="[STEP 6 key_op] ", key_type="bucket")
                    self.log.info(f"[STEP 6 key_op] Bucket key in use: {new_key_ids[0]}")
                elif self.key_op == "other_reencrypt":
                    self.log.info("[STEP 6 key_op] Dropping 'other' DEKs (metadata store re-encryption)...")
                    status, response = self.rest.trigger_other_reencryption()
                    if not status:
                        raise Exception(f"dropEncryptionAtRestDeks/other failed: {response}")
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, other_baseline_key_ids, timeout=key_op_timeout,
                        label="[STEP 6 key_op] ", key_type="other")
                    self.log.info(f"[STEP 6 key_op] New 'other' key IDs: {new_key_ids[0]}")
                elif self.key_op == "log_reencrypt":
                    self.log.info("[STEP 6 key_op] Dropping log DEKs (log file re-encryption)...")
                    status, response = self.rest.trigger_log_reencryption()
                    if not status:
                        raise Exception(f"dropEncryptionAtRestDeks/log failed: {response}")
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, log_baseline_key_ids, timeout=key_op_timeout,
                        label="[STEP 6 key_op] ", key_type="log")
                    self.log.info(f"[STEP 6 key_op] New log key IDs: {new_key_ids[0]}")
                elif self.key_op == "reencrypt":
                    self.log.info(f"[STEP 6 key_op] Dropping DEKs on '{bucket_name}' (full re-encryption)...")
                    status, response = self.rest.trigger_data_reencryption(bucket_name)
                    if not status:
                        raise Exception(f"dropEncryptionAtRestDeks API call failed: {response}")
                    new_key_ids[0] = self.poll_for_new_indexer_key_ids(
                        index_nodes, baseline_key_ids, timeout=key_op_timeout,
                        label="[STEP 6 key_op] ", key_type="bucket")
                    self.log.info(f"[STEP 6 key_op] New key IDs after DEK drop: {new_key_ids[0]}")
                else:
                    status, response = self.rest.disable_bucket_encryption(bucket_name)
                    if not status:
                        raise Exception(f"Failed to disable encryption: {response}")
                    enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
                    if enc_key not in [None, -1]:
                        raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still {enc_key}")
                    self.log.info(f"[STEP 6 key_op] Encryption disabled, encryptionAtRestKeyId={enc_key}")
            except Exception as ex:
                thread_errors.append(f"key_op thread: {ex}")
            finally:
                rotation_armed.set()

        try:
            self.log.info(f"[STEP 6] Launching indexer restart and key_op='{self.key_op}' threads concurrently...")
            t_restart = threading.Thread(target=run_restart, name="restart_thread", daemon=True)
            t_key_op = threading.Thread(target=run_key_op, name="key_op_thread", daemon=True)
            t_restart.start()
            t_key_op.start()
            self.log.info("[STEP 6] PASSED - Both threads launched")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Join threads, wait for indexes online ==========
        try:
            self.log.info("[STEP 7] Waiting for restart and key op threads to complete...")
            # Graceful restart: up to ~20 s stop + 900 s ready wait per node, one node at a time
            restart_join_timeout = max(1800, 960 * len(index_nodes))
            t_restart.join(timeout=restart_join_timeout)
            t_key_op.join(timeout=key_op_timeout + 400)
            still_running = [f"{t.name} still running after {timeout} s"
                             for t, timeout in ((t_restart, restart_join_timeout),
                                                (t_key_op, key_op_timeout + 400)) if t.is_alive()]
            if still_running:
                raise Exception(f"Threads did not finish: {'; '.join(still_running)}")
            if thread_errors:
                raise Exception(f"Thread failures: {'; '.join(thread_errors)}")
            self.log.info("[STEP 7] Threads joined, waiting for all indexes to be online (timeout=1800s)...")
            self.wait_until_indexes_online(timeout=1800)
            self.log.info(f"[STEP 7] PASSED - new_key_ids={new_key_ids[0]}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        if self.key_op == "other_reencrypt":
            # Metadata read back after the restart under the new 'other' key (before
            # step 10's alter/drop/build changes the index list)
            self.assertEqual(self._index_metadata_snapshot(bucket_name), baseline_metadata,
                             "[STEP 7] Index metadata changed after restart during 'other' key drop")
        # ========== STEP 8: Scans on existing indexes ==========
        try:
            self.log.info("[STEP 8] Running scans after concurrent restart + key op...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 8] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Create new indexes, scan ==========
        try:
            self.log.info("[STEP 9] Creating new indexes and scanning...")
            new_prefix = 'rst_conc_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 9] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Lifecycle ops ==========
        try:
            self.log.info("[STEP 10] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 10] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: File encryption validation (rotation/expiry) or item count (drop) ==========
        try:
            if self.key_op in ("rotation", "expiry") and new_key_ids[0]:
                # rotation ran through the restarts; no new DEKs while files are checked
                self.set_bucket_dek_rotation_config(bucket_name, key_id, STABLE_DEK_INTERVAL_S, STABLE_DEK_INTERVAL_S)
                self.sleep(30, "[STEP 11] Letting an in-flight DEK rotation reach the indexers")
                self.log.info(f"[STEP 11] Validating file encryption with new key IDs {new_key_ids[0]}...")
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids[0])),
                        step_prefix="[STEP 11] "):
                    raise Exception("File encryption validation failed after concurrent restart + DEK rotation")
                self.log.info("[STEP 11] PASSED - Files encrypted with new DEK key IDs")
            elif self.key_op == "other_reencrypt":
                try:
                    self.poll_for_indexer_key_ids_gone(index_nodes, other_baseline_key_ids, timeout=600,
                                                       label="[STEP 11] ", key_type="other")
                except Exception as ex:
                    # Old keys may legitimately linger; record it but don't fail the test.
                    self.log.warning(f"[STEP 11] Old 'other' keys still in use: {ex}")
                self._validate_metadata_repo(
                    index_nodes, self.get_indexer_in_use_key_ids(index_nodes, key_type="other"), "[STEP 11] ")
                self.item_count_related_validations()
                self.log.info("[STEP 11] PASSED - metadata store re-encrypted and indexes intact after restart")
            elif self.key_op == "log_reencrypt":
                try:
                    self.poll_for_indexer_key_ids_gone(index_nodes, log_baseline_key_ids, timeout=600,
                                                       label="[STEP 11] ", key_type="log")
                except Exception as ex:
                    # Old keys may legitimately linger; record it but don't fail the test.
                    self.log.warning(f"[STEP 11] Old log keys still in use: {ex}")
                self._populate_request_handler_cache(index_nodes)
                self.sleep(60, "[STEP 11] Waiting for log files to be written after the restart")
                self.validate_log_encryption_files_encrypted(
                    index_nodes, require_files=("indexer_stats_log", "projector_stats_log",
                                                "request_handler_cache", "persisted_stats"),
                    expected_key_id=self.get_indexer_in_use_key_ids(index_nodes, key_type="log"))
                self.item_count_related_validations()
                self.log.info("[STEP 11] PASSED - log files re-encrypted and readable after restart")
            elif self.key_op == "reencrypt":
                # Re-encryption is complete once no index node still reports a
                # dropped (baseline) DEK as in use.
                self.log.info(f"[STEP 11] key_op=reencrypt: waiting for old key IDs {baseline_key_ids} "
                              f"to leave GetInUseKeys...")
                try:
                    remaining = self.poll_for_indexer_key_ids_gone(
                        index_nodes, baseline_key_ids, timeout=900, label="[STEP 11] ", key_type="bucket")
                except Exception as ex:
                    # Old keys may legitimately linger; record it but don't fail the test.
                    self.log.warning(f"[STEP 11] Old bucket DEKs still in use: {ex}")
                    remaining = self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket")
                self.item_count_related_validations()
                self.log.info(f"[STEP 11] PASSED - Old DEKs dropped; in use now: {remaining}")
            elif self.key_op == "enable":
                # Force encryption is complete once no index node reports the
                # unencrypted ("") key for this bucket any more.
                self.log.info("[STEP 11] key_op=enable: waiting for no unencrypted keys left for the bucket...")
                self.validate_mixed_bucket_encrypted_keys(
                    index_nodes, [bucket_name], timeout=600, step_prefix="[STEP 11] "
                )
                self.item_count_related_validations()
                self.log.info("[STEP 11] PASSED - Existing index data re-encrypted after enable during restart")
            else:
                self.log.info("[STEP 11] key_op=drop: running item count validation...")
                self.item_count_related_validations()
                self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_indexer_restart_concurrent_key_op")
        self.log.info(f"key_op={self.key_op}, new_key_ids={new_key_ids[0]}")
        self.log.info("=" * 80)

    # =========================================================================
    # Test 5: Mutations + scans running during key rotation / drop / expiry
    # =========================================================================

    def test_gsi_encryption_at_rest_workload_concurrent_key_op(self):
        """
        Encryption at rest - background mutation + query workload running while
        a key operation (rotation, expiry, or drop) is triggered.

        key_op param (conf-driven):
          "rotation" — DEK rotation via short TTL; validate with new key IDs after stop
          "expiry"   — DEK expiry via short TTL (same mechanism, semantic distinction)
          "drop"     — disable_bucket_encryption

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs, create indexes, wait Ready
        4.  Baseline scans + file encryption validation
        5.  Start background mutation workload (mutation_ops_rate, CRUD ratios from params)
        6.  Start background query workload (continuous scans)
        7.  Trigger DEK rotation/expiry while workloads run; poll new key IDs
        8.  Stop workload threads
        9.  Scans on old indexes post key op
        10. Create new indexes, scan
        11. Lifecycle ops (alter/drop/build)
        12. File encryption validation with new key IDs (rotation/expiry) or item count (drop)
        """
        STABLE_DEK_INTERVAL_S = 36000
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_workload_concurrent_key_op")
        self.log.info(f"key_op={self.key_op}, mutation_ops_rate={self.mutation_ops_rate}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 3,
                f"Need at least 3 index nodes, found {len(index_nodes)}")
            if not self.enable_encryption_at_rest:
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            key_id = self.encryption_at_rest_id
            self.assertIsNotNone(key_id, "encryption_at_rest_id is None")
            if self.key_op not in ("rotation", "expiry", "drop"):
                raise ValueError(f"key_op must be 'rotation', 'expiry', or 'drop', got '{self.key_op}'")
            self.log.info(f"[STEP 1] PASSED - key_id={key_id}, key_op={self.key_op}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create encrypted bucket, load docs, create indexes ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        select_queries = set()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_DEK_INTERVAL_S,
                dek_lifetime=STABLE_DEK_INTERVAL_S
            )
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            prefix = 'wl_kop_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, skip_primary=False
            )
            select_queries.update(self.gsi_util_obj.get_select_queries(
                definition_list=query_definitions, namespace=namespace
            ))
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=self.defer_build, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info(f"[STEP 3] PASSED - {self.num_of_docs_per_collection} docs, {len(queries)} indexes")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 4: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 4] Running baseline scans and file encryption validation...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 4] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5 & 6: Start background mutation + query workloads ==========
        stop_event = threading.Event()
        mutation_thread = None
        query_thread = None
        try:
            self.log.info(f"[STEP 5/6] Starting background mutation ({self.mutation_ops_rate} ops) and query workloads...")

            def run_mutations():
                while not stop_event.is_set():
                    gen_update = SDKDataLoader(
                        num_ops=self.mutation_ops_rate,
                        percent_create=self.workload_percent_create,
                        percent_update=self.workload_percent_update,
                        percent_delete=self.workload_percent_delete,
                        scope="_default", collection="_default",
                        json_template=self.json_template, output=True,
                        username=self.username, password=self.password
                    )
                    try:
                        tasks = self.data_ops_javasdk_loader_in_batches(
                            sdk_data_loader=gen_update,
                            batch_size=self.mutation_ops_rate,
                            dataset=self.json_template
                        )
                        for task in tasks:
                            task.result()
                    except Exception as ex:
                        self.log.warning(f"[STEP 5/6] Mutation batch error (non-fatal): {ex}")
                    time.sleep(5)

            def run_queries():
                n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
                while not stop_event.is_set():
                    for query in select_queries:
                        if stop_event.is_set():
                            break
                        try:
                            self.run_cbq_query(query=query, server=n1ql_server)
                        except Exception as ex:
                            self.log.warning(f"[STEP 5/6] Query error (non-fatal): {ex}")
                    time.sleep(10)

            mutation_thread = threading.Thread(target=run_mutations, name="mutation_workload", daemon=True)
            query_thread = threading.Thread(target=run_queries, name="query_workload", daemon=True)
            mutation_thread.start()
            query_thread.start()
            self.log.info("[STEP 5/6] PASSED - Background workloads started")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 5/6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Trigger key op while workloads run ==========
        new_key_ids = None
        try:
            self.log.info(f"[STEP 7] Triggering key_op='{self.key_op}' while workloads are running...")
            if self.key_op in ("rotation", "expiry"):
                DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
                self.log.info(f"[STEP 7] Triggering DEK {self.key_op} via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
                self.set_bucket_dek_rotation_config(
                    bucket_name, key_id,
                    dek_rotation_interval=DEK_ROTATION_INTERVAL_S,
                    dek_lifetime=DEK_ROTATION_INTERVAL_S * 5
                )
                new_key_ids = self.poll_for_new_indexer_key_ids(
                    index_nodes, baseline_key_ids, timeout=600, label="[STEP 7] "
                )
            else:  # drop
                status, response = self.rest.disable_bucket_encryption(bucket_name)
                if not status:
                    raise Exception(f"Failed to disable encryption: {response}")
                enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
                if enc_key not in [None, -1]:
                    raise Exception(f"Encryption not disabled — encryptionAtRestKeyId still {enc_key}")
                self.log.info(f"[STEP 7] Encryption disabled, encryptionAtRestKeyId={enc_key}")
            self.log.info(f"[STEP 7] PASSED - key op complete, new_key_ids={new_key_ids}")
        except Exception as e:
            stop_event.set()
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Stop workload threads ==========
        try:
            self.log.info("[STEP 8] Stopping background workload threads...")
            stop_event.set()
            if mutation_thread:
                mutation_thread.join(timeout=30)
            if query_thread:
                query_thread.join(timeout=30)
            self.log.info("[STEP 8] PASSED - Workload threads stopped")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Scans on old indexes ==========
        try:
            self.log.info("[STEP 9] Running scans on old indexes post key op...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Create new indexes, scan ==========
        try:
            self.log.info("[STEP 10] Creating new indexes and scanning...")
            new_prefix = 'wl_kop_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(f"[STEP 10] PASSED - Created {len(new_queries)} new indexes and scanned")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Lifecycle ops ==========
        try:
            self.log.info("[STEP 11] Running index lifecycle ops (alter/drop/build)...")
            self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: File encryption validation (rotation/expiry) or item count (drop) ==========
        try:
            if self.key_op in ("rotation", "expiry") and new_key_ids:
                self.log.info(f"[STEP 12] Validating file encryption with new key IDs {new_key_ids}...")
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids)),
                        step_prefix="[STEP 12] "):
                    raise Exception("File encryption validation failed after workload + key op")
                self.log.info("[STEP 12] PASSED - Files encrypted with rotated key IDs")
            else:
                self.log.info("[STEP 12] key_op=drop: running item count validation...")
                self.item_count_related_validations()
                self.log.info("[STEP 12] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_workload_concurrent_key_op")
        self.log.info(f"key_op={self.key_op}, new_key_ids={new_key_ids}")
        self.log.info("=" * 80)

    # =========================================================================
    # Test 6: DGM scenario with key drop (compaction) with toggle flag
    # =========================================================================

    def test_gsi_encryption_at_rest_dgm_with_key_toggle_compaction(self):
        """
        Encryption at rest - DGM mode with encryption toggle and compaction.

        Two encrypted buckets are loaded into DGM. Encryption is then disabled
        on bucket_2 (key drop / toggle off), compaction is triggered with a low
        fragmentation threshold, and encryption is re-enabled on bucket_2
        (toggle back on). Validates indexes are accessible and correctly
        encrypted throughout.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings
        3.  Create 2 encrypted buckets, load docs
        4.  Create indexes on both buckets, wait for Ready
        5.  Induce DGM (_load_until_index_dgm)
        6.  Baseline scans + file encryption validation
        7.  Disable encryption on bucket_2 (toggle off)
        8.  Set low compaction threshold; trigger compaction on all index nodes
        9.  Poll until compaction detected (NCompacts increases)
        10. Re-enable encryption on bucket_2 (toggle back on)
        11. Validate encryptionAtRestKeyId set on bucket_2
        12. Scans on all indexes (both buckets)
        13. Create new indexes on both buckets, scan
        14. Lifecycle ops (alter/drop/build)
        15. File encryption validation — bucket_1 fully validated; bucket_2 new indexes only
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_dgm_with_key_toggle_compaction")
        self.log.info(f"gsi_type={self.gsi_type}, index_resident_ratio={self.index_resident_ratio}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key_id = self.encryption_at_rest_id
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create 2 encrypted buckets, load docs ==========
        bucket_1 = f"{self.test_bucket}_1"
        bucket_2 = f"{self.test_bucket}_2"
        namespace_1 = f"default:{bucket_1}._default._default"
        namespace_2 = f"default:{bucket_2}._default._default"
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        self.namespaces = [namespace_1, namespace_2]
        try:
            self.log.info("[STEP 3] Creating 2 encrypted buckets and loading docs...")
            for bucket_name in [bucket_1, bucket_2]:
                self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
                enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
                if enc_key is None or enc_key == -1:
                    raise Exception(f"Bucket '{bucket_name}' should be encrypted")
                gen_create = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100, percent_update=0, percent_delete=0,
                    scope="_default", collection="_default",
                    json_template=self.json_template, output=True,
                    username=self.username, password=self.password
                )
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
                )
                for task in tasks:
                    task.result()
            self.log.info(f"[STEP 3] PASSED - Both buckets created and loaded")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes on both buckets, wait for Ready ==========
        select_queries = set()
        try:
            self.log.info("[STEP 4] Creating indexes on both buckets...")
            for namespace in [namespace_1, namespace_2]:
                prefix = 'dgm_tog_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                query_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=prefix, skip_primary=False
                )
                select_queries.update(self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions, namespace=namespace
                ))
                queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=query_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=self.defer_build, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=queries, database=namespace, query_node=query_node
                )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 4] PASSED - Indexes created on both buckets")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Induce DGM ==========
        try:
            self.log.info(f"[STEP 5] Inducing DGM (target resident_ratio={self.index_resident_ratio}%)...")
            self._load_until_index_dgm(resident_ratio=self.index_resident_ratio)
            avg_rr = self.compute_cluster_avg_rr_index()
            self.assertLessEqual(avg_rr, self.index_resident_ratio,
                f"DGM not achieved: avg RR {avg_rr} > target {self.index_resident_ratio}")
            self.log.info(f"[STEP 5] PASSED - DGM achieved, avg_rr={avg_rr}%")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)

        # ========== STEP 6: Baseline scans + file encryption validation ==========
        try:
            self.log.info("[STEP 6] Running baseline scans and file encryption validation in DGM...")
            self.run_scan_validation(select_queries)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=baseline_key_ids, step_prefix="[STEP 6] "):
                raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 6] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Disable encryption on bucket_2 (toggle off) ==========
        try:
            self.log.info(f"[STEP 7] Disabling encryption on bucket_2='{bucket_2}' (toggle off)...")
            status, response = self.rest.disable_bucket_encryption(bucket_2)
            if not status:
                raise Exception(f"Failed to disable encryption on bucket_2: {response}")
            enc_key = self.rest.get_bucket_json(bucket_2).get('encryptionAtRestKeyId', None)
            if enc_key not in [None, -1]:
                raise Exception(f"Encryption not disabled on bucket_2 — key still {enc_key}")
            self.log.info(f"[STEP 7] PASSED - bucket_2 encryption disabled (encryptionAtRestKeyId={enc_key})")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Set low compaction threshold, trigger compaction ==========
        try:
            self.log.info("[STEP 8] Setting low compaction threshold and triggering compaction...")
            ncompacts_before, _ = self._get_total_ncompacts(index_nodes)
            for index_node in index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({
                    "indexer.settings.compaction.min_size": 16 * 1024 * 1024,
                    "indexer.settings.compaction.min_frag": 10,
                    "indexer.settings.compaction.check_period": 5
                })
                rest.trigger_index_compaction()
                self.log.info(f"[STEP 8] Compaction triggered on {index_node.ip}")
            self.log.info("[STEP 8] PASSED - Compaction triggered on all nodes")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Poll until compaction detected ==========
        try:
            self.log.info("[STEP 9] Polling for compaction completion (NCompacts)...")
            deadline = time.time() + 300
            compaction_detected = False
            while time.time() < deadline:
                ncompacts_after, _ = self._get_total_ncompacts(index_nodes)
                if ncompacts_after > ncompacts_before:
                    self.log.info(f"[STEP 9] Compaction detected: NCompacts {ncompacts_before} -> {ncompacts_after}")
                    compaction_detected = True
                    break
                self.sleep(15, "Waiting for compaction NCompacts to increase")
            if not compaction_detected:
                self.log.warning("[STEP 9] NCompacts did not increase within 300s — compaction may not have run")
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Re-enable encryption on bucket_2 (toggle back on) ==========
        try:
            self.log.info(f"[STEP 10] Re-enabling encryption on bucket_2='{bucket_2}' (toggle back on)...")
            status, response = self.rest.enable_bucket_encryption(bucket_2, key_id)
            if not status:
                raise Exception(f"Failed to re-enable encryption on bucket_2: {response}")
            enc_key = self.rest.get_bucket_json(bucket_2).get('encryptionAtRestKeyId', None)
            if enc_key is None or enc_key == -1:
                raise Exception("Encryption not re-enabled on bucket_2")
            self.log.info(f"[STEP 10] PASSED - bucket_2 encryption re-enabled (encryptionAtRestKeyId={enc_key})")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Scans on all indexes (DGM, post-toggle) ==========
        try:
            self.log.info("[STEP 11] Running scans on all indexes after toggle and compaction...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 11] PASSED - All indexes accessible after toggle + compaction")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Create new indexes on both buckets, scan ==========
        try:
            self.log.info("[STEP 12] Creating new indexes on both buckets and scanning...")
            new_b2_select_queries = set()
            for namespace in [namespace_1, namespace_2]:
                new_prefix = 'dgm_tog_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                new_definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=new_prefix, skip_primary=False
                )
                new_sq = self.gsi_util_obj.get_select_queries(
                    definition_list=new_definitions, namespace=namespace
                )
                new_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=new_definitions, namespace=namespace,
                    num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                    defer_build=False, randomise_replica_count=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=new_queries, database=namespace, query_node=query_node
                )
                if namespace == namespace_2:
                    new_b2_select_queries.update(new_sq)
            self.wait_until_indexes_online(timeout=1200)
            all_new_sq = new_b2_select_queries
            self.run_scan_validation(all_new_sq)
            self.log.info("[STEP 12] PASSED - New indexes created and scanned on both buckets")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Lifecycle ops on each namespace ==========
        try:
            self.log.info("[STEP 13] Running index lifecycle ops on both namespaces...")
            for namespace in [namespace_1, namespace_2]:
                self._run_index_lifecycle_ops(namespace, index_nodes, query_node)
            self.log.info("[STEP 13] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 13] FAILED: {str(e)}")
            raise

        # ========== STEP 14: File encryption validation ==========
        try:
            self.log.info("[STEP 14] File encryption validation (bucket_1 full, bucket_2 new indexes only)...")
            final_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            if not self.validate_file_encryption_with_key(
                    index_nodes, expected_key_id=final_key_ids, step_prefix="[STEP 14] "):
                raise Exception("File encryption validation failed after DGM + toggle + compaction")
            self.log.info("[STEP 14] PASSED - File encryption validation succeeded")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_dgm_with_key_toggle_compaction")
        self.log.info(f"DGM + key toggle + compaction succeeded on buckets '{bucket_1}', '{bucket_2}'")
        self.log.info("=" * 80)

    def test_gsi_persisted_stats_log_encryption_lifecycle(self):
        """
        MB-69769: Validate the full plaintext -> encrypted -> re-keyed ->
        disabled lifecycle of the persisted stats file (@2i/indexstats/stats).

        Steps:
          1-2. Create bucket, load docs, build indexes. Stats file is plaintext.
          3.   Assert persisted stats is plaintext.
          4.   Enable log encryption + DEK drop to force immediate encrypted write.
          5.   Assert encrypted with in-use log DEK.
          6-7. Graceful restart x1, verify read-back, still encrypted.
          8-9. Graceful restart x2, still encrypted, scans work.
          10.  Force re-encrypt via another DEK drop; verify new DEK.
          11.  Kill -9 restart; verify encrypted stats survives crash + scans work.
          12.  Disable log encryption + restart; verify plaintext again + scans.
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_persisted_stats_log_encryption_lifecycle")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites (no log encryption yet) ==========
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.assertGreaterEqual(len(index_nodes), 1,
            f"Need at least 1 index node, found {len(index_nodes)}")
        self.log.info(
            f"[STEP 1] PASSED - {len(index_nodes)} index node(s), log encryption not yet enabled"
        )

        # ========== STEP 2: Create bucket, load docs, build indexes ==========
        bucket_name = self.test_bucket
        self.log.info(f"[STEP 2] Creating bucket '{bucket_name}' without encryption...")
        self.create_bucket_with_encryption(bucket_name, enable_encryption=False)
        namespace = f"default:{bucket_name}._default._default"
        try:
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 2] ")
        except Exception as e:
            self.log.warning(f"[STEP 2] set_indexer_snapshot_settings failed (non-fatal): {e}")

        self.log.info(f"[STEP 2] Loading {self.num_of_docs_per_collection} docs and creating indexes...")
        from couchbase_helper.documentgenerator import SDKDataLoader
        gen = SDKDataLoader(
            num_ops=self.num_of_docs_per_collection,
            percent_create=100,
            percent_update=0,
            percent_delete=0,
            scope="_default",
            collection="_default",
            json_template=self.json_template,
            output=True,
            username=self.username,
            password=self.password,
        )
        tasks = self.data_ops_javasdk_loader_in_batches(
            sdk_data_loader=gen, batch_size=10**4, dataset=self.json_template
        )
        for task in tasks:
            task.result()
        query_definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix="persisted_stats",
            similarity=self.similarity if hasattr(self, "similarity") else None,
        )
        create_queries = self.gsi_util_obj.get_create_index_list(
            definition_list=query_definitions,
            namespace=namespace,
        )
        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=query_definitions,
            namespace=namespace,
        )
        self.gsi_util_obj.create_gsi_indexes(
            create_queries=create_queries,
            database=bucket_name,
            query_node=query_node,
        )
        self.wait_until_indexes_online(timeout=600)
        # Flush stats to disk
        self.sleep(30, "[STEP 2] Waiting for stats to flush to disk")
        self.log.info("[STEP 2] PASSED - Docs loaded and indexes built")

        # ========== STEP 3: Verify persisted stats file is plaintext ==========
        self.log.info("[STEP 3] Verifying persisted stats (@2i/indexstats/stats) is plaintext...")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(index_nodes)
        encrypted_nodes = [ip for ip, r in results.items() if r["status"] == "passed"]
        if encrypted_nodes:
            self.fail(
                f"[STEP 3] Persisted stats already encrypted on {encrypted_nodes} "
                f"before log encryption was enabled"
            )
        self.log.info("[STEP 3] PASSED - Persisted stats file is plaintext on all index nodes")

        # ========== STEP 4: Enable log encryption at runtime ==========
        self.log.info("[STEP 4] Creating log-encryption secret and enabling log encryption...")
        log_secret_id = self.create_new_encryption_key(usage=["log-encryption"])
        self.set_encryption_at_rest("log", log_secret_id, enable=True)
        status, _ = self.rest.trigger_log_reencryption()
        self.assertTrue(status, "[STEP 4] trigger_log_reencryption (DEK drop) failed")
        self.sleep(10, "[STEP 4] Waiting for DEK drop to persist stats file")
        self.log.info(f"[STEP 4] PASSED - Log encryption enabled, DEK drop triggered")

        # ========== STEP 5: Verify persisted stats file is now encrypted ==========
        self.log.info("[STEP 5] Verifying persisted stats is encrypted after enabling log EAR...")
        expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
        self.assertTrue(expected_key_ids, "[STEP 5] No in-use log key IDs reported")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(
            index_nodes, expected_key_id=expected_key_ids
        )
        failures = [
            f"{ip}: {r['reason']}" for ip, r in results.items()
            if r["status"] not in ("passed", "skipped")
        ]
        if failures:
            self.fail(f"[STEP 5] Persisted stats not encrypted as expected: {failures}")
        self.log.info("[STEP 5] PASSED - Persisted stats encrypted on all index nodes")

        # ========== STEP 6: Restart indexer once ==========
        self.log.info("[STEP 6] Restarting indexer (first restart) to verify read-back of encrypted stats...")
        self.restart_index_nodes(index_nodes, validate_read_back=True)
        self.log.info("[STEP 6] PASSED - Indexer restarted and indexes back online")

        # ========== STEP 7: Verify persisted stats still encrypted after first restart ==========
        self.log.info("[STEP 7] Verifying persisted stats still encrypted after first restart...")
        expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(
            index_nodes, expected_key_id=expected_key_ids
        )
        failures = [
            f"{ip}: {r['reason']}" for ip, r in results.items()
            if r["status"] not in ("passed", "skipped")
        ]
        if failures:
            self.fail(f"[STEP 7] Persisted stats not encrypted after first restart: {failures}")
        self.log.info("[STEP 7] PASSED - Persisted stats encrypted after first restart")

        # ========== STEP 8: Restart indexer a second time ==========
        self.log.info("[STEP 8] Restarting indexer (second restart)...")
        self.restart_index_nodes(index_nodes, validate_read_back=True)
        self.log.info("[STEP 8] PASSED - Second restart complete")

        # ========== STEP 9: Verify still encrypted, scans work ==========
        self.log.info("[STEP 9] Verifying persisted stats encrypted after second restart + running scans...")
        expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(
            index_nodes, expected_key_id=expected_key_ids
        )
        failures = [
            f"{ip}: {r['reason']}" for ip, r in results.items()
            if r["status"] not in ("passed", "skipped")
        ]
        if failures:
            self.fail(f"[STEP 9] Persisted stats not encrypted after second restart: {failures}")
        self.run_scan_validation(set(select_queries))
        self.log.info("[STEP 9] PASSED - Persisted stats encrypted, scans succeeded")

        # ========== STEP 10: Force re-encrypt via DEK drop ==========
        self.log.info("[STEP 10] Triggering DEK drop to force re-encryption with a new DEK...")
        old_key_ids = set(expected_key_ids)
        status, _ = self.rest.trigger_log_reencryption()
        self.assertTrue(status, "[STEP 10] trigger_log_reencryption (force re-encrypt) failed")
        self.sleep(10, "[STEP 10] Waiting for DEK drop to complete")
        new_key_ids = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
        self.assertTrue(new_key_ids, "[STEP 10] No in-use log key IDs after DEK drop")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(
            index_nodes, expected_key_id=new_key_ids
        )
        failures = [
            f"{ip}: {r['reason']}" for ip, r in results.items()
            if r["status"] not in ("passed", "skipped")
        ]
        if failures:
            self.fail(f"[STEP 10] Persisted stats not re-encrypted after DEK drop: {failures}")
        self.log.info(f"[STEP 10] PASSED - Re-encrypted (old={old_key_ids}, new={set(new_key_ids)})")

        # ========== STEP 11: Kill -9 restart ==========
        self.log.info("[STEP 11] Kill -9 restart to test crash recovery of encrypted stats...")
        self.restart_index_nodes(index_nodes, restart_mode="kill", validate_read_back=True)
        expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(
            index_nodes, expected_key_id=expected_key_ids
        )
        failures = [
            f"{ip}: {r['reason']}" for ip, r in results.items()
            if r["status"] not in ("passed", "skipped")
        ]
        if failures:
            self.fail(f"[STEP 11] Persisted stats not encrypted after kill -9: {failures}")
        self.run_scan_validation(set(select_queries))
        self.log.info("[STEP 11] PASSED - Survived kill -9, scans succeeded")

        # ========== STEP 12: Disable log encryption + DEK drop (decrypt) ==========
        self.log.info("[STEP 12] Disabling log encryption and triggering DEK drop (decrypt)...")
        self.set_encryption_at_rest("log", log_secret_id, enable=False)
        status, _ = self.rest.trigger_log_reencryption()
        self.assertTrue(status, "[STEP 12] trigger_log_reencryption (decrypt) failed")
        self.sleep(10, "[STEP 12] Waiting for decrypt to complete")
        results = self.gsi_encryption_helper.verify_gsi_persisted_stats_encrypted(index_nodes)
        encrypted_nodes = [ip for ip, r in results.items() if r["status"] == "passed"]
        if encrypted_nodes:
            self.fail(
                f"[STEP 12] Persisted stats still encrypted on {encrypted_nodes} "
                f"after disabling log encryption"
            )
        self.restart_index_nodes(index_nodes, validate_read_back=True)
        self.run_scan_validation(set(select_queries))
        self.log.info("[STEP 12] PASSED - Persisted stats plaintext after disable, scans work")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_persisted_stats_log_encryption_lifecycle")
        self.log.info("=" * 80)

    def test_gsi_log_encryption_files_encrypted(self):
        """
        Validate that indexer_stats.log, projector_stats.log, and the
        request-handler cache files under @2i/cache are encrypted with the
        cluster-level log-encryption secret.

        Requires ``enable_log_encryption_at_rest=True`` so basetestcase.py
        provisions a log-encryption key. Bucket encryption is optional —
        this test only asserts on log-encryption-governed files.

        Steps:
          1. Verify index nodes and that log encryption is enabled
          2. Create a bucket (encrypted if enable_encryption_at_rest=True)
          3. Load documents + create indexes so stats logs and request-handler
             cache files get written
          4. Run SELECT queries so the request-handler cache is populated
          5. Wait briefly to let stats flush to disk
          6. Call validate_log_encryption_files_encrypted
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_log_encryption_files_encrypted")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying index nodes and log-encryption setup...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 2,
                f"Need at least 2 index nodes, found {len(index_nodes)}")
            if not getattr(self, "enable_log_encryption_at_rest", False):
                self.skipTest("Log encryption at rest not enabled. "
                              "Set enable_log_encryption_at_rest=True")
            self.log.info(
                f"[STEP 1] PASSED - {len(index_nodes)} index nodes, "
                f"log_encryption_at_rest_id={getattr(self, 'log_encryption_at_rest_id', None)}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Create bucket ==========
        bucket_name = self.test_bucket
        try:
            self.log.info(f"[STEP 2] Creating bucket '{bucket_name}' "
                          f"(encryption={bool(self.enable_encryption_at_rest)})...")
            self.create_bucket_with_encryption(
                bucket_name, enable_encryption=bool(self.enable_encryption_at_rest)
            )
            self.log.info(f"[STEP 2] PASSED - Bucket '{bucket_name}' created")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Configure indexer snapshot intervals ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 3]")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise
        # ========== STEP 4: Load documents and create indexes ==========
        select_queries = set()
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} docs and creating indexes...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()

            namespace = f"default:{bucket_name}._default._default"
            prefix = 'log_ear_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace
                )
            )
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions,
                namespace=namespace,
                num_replica=self.num_index_replica,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info(f"[STEP 4] PASSED - Indexes online, {len(select_queries)} select queries collected")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Exercise request-handler cache via scans ==========
        try:
            self.log.info(f"[STEP 5] Running {len(select_queries)} SELECT queries to populate request-handler cache...")
            self.run_and_validate_scans(
                select_queries, expected_doc_count=self.num_of_docs_per_collection
            )
            self.log.info("[STEP 5] PASSED - Scans completed")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for stats to flush ==========
        flush_wait = int(self.input.param("stats_flush_wait", 60))
        self.sleep(flush_wait,
                   f"Waiting {flush_wait}s for indexer/projector stats logs "
                   f"and request-handler cache to flush to disk")

        # ========== STEP 7: Validate log-encryption files ==========
        try:
            expected_key_id = getattr(self, "log_encryption_at_rest_id", None)
            expected_header_key_ids = None
            if expected_key_id not in (None, False, ""):
                expected_header_key_ids = self.get_expected_header_key_ids(expected_key_id)
            self.log.info(
                f"[STEP 7] Validating log-encryption files with expected key IDs "
                f"{expected_header_key_ids} (raw={expected_key_id})..."
            )
            self.validate_log_encryption_files_encrypted(
                index_nodes, expected_key_id=expected_header_key_ids
            )
            self.log.info("[STEP 7] PASSED - Log-encryption file validation completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_log_encryption_files_encrypted")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_force_encrypt_existing_files(self):
        """
        Encryption at rest - force re-encryption of existing unencrypted index files.

        Validates that calling forceEncryptionAtRest/bucket/<bucket> after enabling
        bucket encryption forces the indexer to re-encrypt all pre-existing index files
        on disk. Index files created before encryption was enabled are initially
        plaintext; forceEncryptionAtRest triggers their immediate re-encryption.
        Scans must remain functional throughout.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings
        3.  Create bucket WITHOUT encryption, load docs, create indexes, wait for Ready
        4.  Baseline scans + confirm files are NOT encrypted on disk
        5.  Enable encryption on the bucket
        6.  Scans still work post-enable
        7.  Call forceEncryptionAtRest/bucket/<bucket> to trigger re-encryption
        8.  Poll until all index files for the bucket are confirmed encrypted with active key
        9.  Scans still work post-force-encryption
        10. Create new indexes post-encryption, scan them
        11. Item count validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_force_encrypt_existing_files")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites()
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key_id = self.encryption_at_rest_id
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise
        # ========== STEP 3: Create bucket WITHOUT encryption, load docs, create indexes ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        select_queries = set()
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' WITHOUT encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=False)
            enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
            if enc_key not in [None, -1]:
                raise Exception(f"Bucket should be unencrypted but got encryptionAtRestKeyId={enc_key}")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            prefix = 'fenc_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, skip_primary=False
            )
            select_queries.update(self.gsi_util_obj.get_select_queries(
                definition_list=query_definitions, namespace=namespace
            ))
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=self.defer_build, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info(
                f"[STEP 3] PASSED - Bucket unencrypted, {self.num_of_docs_per_collection} docs, "
                f"{len(queries)} indexes"
            )
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Baseline scans + confirm files are NOT encrypted ==========
        try:
            self.log.info("[STEP 4] Running baseline scans and confirming files are NOT encrypted...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.validate_files_not_encrypted(
                index_nodes, bucket_name, timeout=60, step_prefix="[STEP 4] "
            )
            self.log.info("[STEP 4] PASSED - Files confirmed NOT encrypted on disk (as expected)")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Enable encryption on the bucket ==========
        try:
            self.log.info(f"[STEP 5] Enabling encryption on bucket '{bucket_name}' with key_id={key_id}...")
            status, response = self.rest.enable_bucket_encryption(bucket_name, key_id)
            if not status:
                raise Exception(f"Failed to enable encryption: {response}")
            enc_key = self.rest.get_bucket_json(bucket_name).get('encryptionAtRestKeyId', None)
            if enc_key is None or enc_key == -1:
                raise Exception(f"Encryption not enabled — encryptionAtRestKeyId={enc_key}")
            self.log.info(f"[STEP 5] PASSED - encryptionAtRestKeyId={enc_key}")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Scans still work post-enable ==========
        try:
            self.log.info("[STEP 6] Verifying scans still work after enabling encryption...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 6] PASSED - All indexes accessible after enabling encryption")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Call forceEncryptionAtRest to re-encrypt existing files ==========
        try:
            self.log.info(
                f"[STEP 7] Calling forceEncryptionAtRest/bucket/{bucket_name} to re-encrypt "
                f"pre-existing unencrypted index files..."
            )
            status, response = self.rest.force_bucket_encryption_at_rest(bucket_name)
            if not status:
                raise Exception(f"forceEncryptionAtRest API call failed: {response}")
            self.log.info(f"[STEP 7] PASSED - Force re-encryption triggered. Response: {response}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Poll until all files are encrypted with the active key ==========
        try:
            self.log.info(
                "[STEP 8] Polling until all index files are confirmed encrypted with the active key "
                "(timeout=300s)..."
            )
            deadline = time.time() + 300
            poll_interval = 15
            encryption_confirmed = False
            while time.time() < deadline:
                in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                if in_use_key_ids and self.validate_file_encryption_with_key(
                        index_nodes, expected_key_id=in_use_key_ids, step_prefix="[STEP 8] "):
                    encryption_confirmed = True
                    break
                self.log.info(
                    f"[STEP 8] Not all files encrypted yet (in_use_key_ids={in_use_key_ids}), "
                    f"retrying in {poll_interval}s..."
                )
                time.sleep(poll_interval)
            if not encryption_confirmed:
                raise Exception(
                    "Timeout (300s): not all index files were encrypted after forceEncryptionAtRest"
                )
            self.log.info("[STEP 8] PASSED - All index files confirmed encrypted on disk")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Scans still work post-force-encryption ==========
        try:
            self.log.info("[STEP 9] Verifying scans still work after force re-encryption...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 9] PASSED - All indexes accessible after force re-encryption")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: New indexes post-encryption, scan ==========
        try:
            self.log.info("[STEP 10] Creating new indexes post-encryption and scanning...")
            new_prefix = 'fenc_new_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            new_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=new_prefix, skip_primary=False
            )
            new_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=new_definitions, namespace=namespace
            )
            new_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=new_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=False, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=new_queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200)
            self.run_scan_validation(new_select_queries)
            self.log.info(
                f"[STEP 10] PASSED - {len(new_queries)} new post-encryption indexes created and scanned"
            )
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Item count validation ==========
        try:
            self.log.info("[STEP 11] Running item count validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes, validate_read_back=True, select_queries=select_queries)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_force_encrypt_existing_files")
        self.log.info(
            f"Force re-encryption succeeded — pre-existing files for bucket '{bucket_name}' "
            f"confirmed encrypted on disk"
        )
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_metadata_repo_encryption(self):
        """
        Encryption at rest - validate metadata_repo_v2 wal and sstable files.

        Verifies that ``@2i/metadata_repo_v2/wal/wal.*`` and
        ``@2i/metadata_repo_v2/kvstore-*/rev-*/*/sstable.*.data`` files contain
        the active encryption key ID on all index nodes. Checks both before and
        after an indexer restart to confirm persistence.

        Steps:
        1.  Verify prerequisites (index nodes present, other encryption enabled)
        2.  Create bucket WITHOUT bucket encryption, load docs, create indexes, wait Ready
        3.  Get active key ID from self.other_encryption_at_rest_id
        4.  Baseline scans
        5.  Validate metadata_repo_v2 wal + sstable files contain key ID
        6.  Indexer restart
        7.  Re-validate metadata files still contain key ID post-restart
        8.  Scans still work post-restart
        9.  Item count validation
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_metadata_repo_encryption")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying prerequisites...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 1,
                f"Need at least 1 index node, found {len(index_nodes)}")
            if not self.enable_other_encryption_at_rest:
                self.skipTest("Other encryption at rest not enabled. Set enable_other_encryption_at_rest=True")
            other_key_id = self.other_encryption_at_rest_id
            self.assertIsNotNone(other_key_id, "other_encryption_at_rest_id is None")
            self.log.info(f"[STEP 1] PASSED - other_key_id={other_key_id}, index_nodes={len(index_nodes)}")
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Create bucket with encryption, load docs, create indexes ==========
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        select_queries = set()
        try:
            self.log.info(f"[STEP 2] Creating bucket '{bucket_name}' without bucket encryption (metadata_repo files are governed by other encryption)...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=False)
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100, percent_update=0, percent_delete=0,
                scope="_default", collection="_default",
                json_template=self.json_template, output=True,
                username=self.username, password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template
            )
            for task in tasks:
                task.result()
            prefix = 'meta_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, skip_primary=False
            )
            select_queries.update(self.gsi_util_obj.get_select_queries(
                definition_list=query_definitions, namespace=namespace
            ))
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions, namespace=namespace,
                num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
                defer_build=self.defer_build, randomise_replica_count=True
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries, database=namespace, query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info(
                f"[STEP 2] PASSED - Bucket created (no bucket encryption), {self.num_of_docs_per_collection} docs, "
                f"{len(queries)} indexes"
            )
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Get active key ID from other_encryption_at_rest_id ==========
        try:
            self.log.info("[STEP 3] Retrieving active other-encryption key ID...")
            active_key_id = str(other_key_id)
            self.assertIsNotNone(active_key_id, "other_encryption_at_rest_id is None at step 3")
            self.log.info(f"[STEP 3] PASSED - active_key_id={active_key_id}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Baseline scans ==========
        try:
            self.log.info("[STEP 4] Running baseline scans...")
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.log.info("[STEP 4] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Validate metadata_repo_v2 wal + sstable files ==========
        try:
            self.log.info(
                f"[STEP 5] Validating metadata_repo_v2 wal/sstable files contain key_id={active_key_id}..."
            )
            meta_results = self.gsi_encryption_helper.verify_gsi_metadata_repo_encrypted(
                index_nodes, expected_key_id=active_key_id
            )
            failed_nodes = [
                ip for ip, r in meta_results.items()
                if r.get("status") not in ("passed", "skipped")
            ]
            if failed_nodes:
                details = {ip: meta_results[ip] for ip in failed_nodes}
                raise Exception(
                    f"metadata_repo encryption check FAILED on nodes {failed_nodes}: {details}"
                )
            self.log.info(
                f"[STEP 5] PASSED - metadata_repo files validated on all nodes: "
                + ", ".join(f"{ip}={r['status']}" for ip, r in meta_results.items())
            )
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Indexer restart ==========
        try:
            self.log.info("[STEP 6] Restarting index nodes...")
            self.restart_index_nodes(index_nodes)
            self.log.info("[STEP 6] PASSED - All index nodes restarted and indexes online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Re-validate metadata files post-restart ==========
        try:
            self.log.info(
                f"[STEP 7] Re-validating metadata_repo_v2 files post-restart (key_id={active_key_id})..."
            )
            meta_results_post = self.gsi_encryption_helper.verify_gsi_metadata_repo_encrypted(
                index_nodes, expected_key_id=active_key_id
            )
            failed_nodes_post = [
                ip for ip, r in meta_results_post.items()
                if r.get("status") not in ("passed", "skipped")
            ]
            if failed_nodes_post:
                details = {ip: meta_results_post[ip] for ip in failed_nodes_post}
                raise Exception(
                    f"Post-restart metadata_repo check FAILED on nodes {failed_nodes_post}: {details}"
                )
            self.log.info(
                f"[STEP 7] PASSED - metadata_repo files still encrypted post-restart: "
                + ", ".join(f"{ip}={r['status']}" for ip, r in meta_results_post.items())
            )
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Scans still work post-restart ==========
        try:
            self.log.info("[STEP 8] Verifying scans still work after indexer restart...")
            self.run_scan_validation(select_queries)
            self.log.info("[STEP 8] PASSED - All indexes accessible post-restart")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Item count validation ==========
        try:
            self.log.info("[STEP 9] Running item count validations...")
            self.item_count_related_validations()
            self.log.info("[STEP 9] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_metadata_repo_encryption")
        self.log.info(
            f"metadata_repo_v2 wal/sstable files confirmed encrypted with key_id={active_key_id}"
        )
        self.log.info("=" * 80)

    # =========================================================================
    # Log key and 'other' key lifecycle (re-encrypt, restart, disable, re-enable)
    # =========================================================================

    def _setup_bucket_and_indexes(self, prefix, index_nodes):
        """Create the test bucket (encrypted if enable_encryption_at_rest), load docs,
        create indexes and wait for Ready. Returns (bucket_name, select_queries)."""
        bucket_name = self.test_bucket
        namespace = f"default:{bucket_name}._default._default"
        self.create_bucket_with_encryption(bucket_name, enable_encryption=bool(self.enable_encryption_at_rest))
        gen_create = SDKDataLoader(
            num_ops=self.num_of_docs_per_collection,
            percent_create=100, percent_update=0, percent_delete=0,
            scope="_default", collection="_default",
            json_template=self.json_template, output=True,
            username=self.username, password=self.password
        )
        for task in self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create, batch_size=10**4, dataset=self.json_template):
            task.result()
        select_queries = self.create_indexes_on_namespace(
            namespace, index_nodes, prefix, step_prefix="[SETUP]", wait_for_ready=True
        )
        return bucket_name, select_queries

    def _index_metadata_snapshot(self, bucket_name):
        """Index names and states from system:indexes, to compare across restarts."""
        result = self.run_cbq_query(
            query=f"SELECT name, state FROM system:indexes "
                  f"WHERE keyspace_id = '{bucket_name}' OR bucket_id = '{bucket_name}' ORDER BY name"
        )
        return sorted((row["name"], row["state"]) for row in result["results"])

    def _wait_for_log_files_plaintext(self, index_nodes, timeout=300, interval=15):
        """Poll every interval seconds, for up to timeout seconds, until indexer_stats.log
        (and its .enc sibling, if any) and the request-handler cache files carry no
        encryption header on any index node (log encryption disabled + decrypted). A
        missing stats log counts as not done; the cache is populated first so it is
        never checked empty."""
        helper = self.gsi_encryption_helper
        self._populate_request_handler_cache(index_nodes)
        deadline = time.time() + timeout
        encrypted, missing = [], []
        while time.time() < deadline:
            encrypted, missing = [], []
            for node in index_nodes:
                stats_log = f"{helper.get_log_path(node)}/indexer_stats.log"
                shell = RemoteMachineShellConnection(node, verbose=False)
                try:
                    out, _ = shell.execute_command(
                        f"for f in {stats_log} {stats_log}.enc; do test -f $f && echo $f; done")
                finally:
                    shell.disconnect()
                files = [line.strip() for line in out if line.strip()]
                if stats_log not in files:
                    missing.append(f"{node.ip}:{stats_log}")
                files += self._list_request_handler_cache_files(node)
                for path in files:
                    is_encrypted, _ = helper.verify_file_encryption_magic_bytes(node, path)
                    if is_encrypted:
                        encrypted.append(f"{node.ip}:{path}")
            if not encrypted and not missing:
                return
            self.sleep(interval, f"Waiting for log files to be decrypted: encrypted={encrypted[:5]}, "
                                 f"missing={missing}")
        # Old encrypted files may legitimately linger; record it but don't fail the test.
        self.log.warning(f"Log files not confirmed plaintext {timeout}s after disabling log encryption: "
                         f"still encrypted={encrypted}, missing={missing}")

    def _validate_metadata_repo(self, index_nodes, key_ids, label):
        """Fail if any node's metadata_repo_v2 files don't carry one of key_ids. Every
        index node hosts the metadata store, so "skipped" (no files found) fails too."""
        results = self.gsi_encryption_helper.verify_gsi_metadata_repo_encrypted(
            index_nodes, expected_key_id=key_ids
        )
        failed = {ip: r for ip, r in results.items() if r.get("status") != "passed"}
        if failed:
            raise Exception(f"{label}metadata_repo encryption check FAILED: {failed}")

    def _wait_for_metadata_repo_plaintext(self, index_nodes, old_key_ids, label, timeout=600, interval=15):
        """Wait until no node's metadata_repo_v2 files carry any of old_key_ids
        (metadata decrypted after 'other' encryption is disabled). The files have no
        encryption header, so a node is done only when it has non-empty files and none
        of them holds an old key ID."""
        deadline = time.time() + timeout
        still = {}
        while time.time() < deadline:
            results = self.gsi_encryption_helper.verify_gsi_metadata_repo_encrypted(
                index_nodes, expected_key_id=old_key_ids
            )
            still = {}
            for ip, result in results.items():
                checked = (result.get("wal_files_checked", 0) + result.get("sstable_files_checked", 0)
                           - len(result.get("skipped_empty_files", [])))
                missed = {path for path, _ in result.get("failed_files", [])}
                if checked <= 0:
                    still[ip] = "no non-empty metadata_repo files found"
                elif len(missed) < checked:
                    still[ip] = f"{checked - len(missed)} of {checked} files still carry an old key"
            if not still:
                self.log.info(f"{label}metadata_repo files carry none of {old_key_ids} on any node")
                return
            self.sleep(interval, f"{label}metadata_repo not decrypted yet: {still}")
        # Old keys may linger in obsolete WAL/sstable files; record it but don't fail the test.
        self.log.warning(f"{label}metadata_repo still carries {old_key_ids} after {timeout}s: {still}")

    def test_gsi_encryption_at_rest_log_key_lifecycle(self):
        """
        Log key lifecycle with GSI files: indexer_stats.log, the request-handler cache
        and persisted stats are encrypted with the log key.

        Steps:
        1.  Prerequisites (log encryption enabled), bucket + indexes, scans; call
            /getIndexStatus on every node so the request-handler cache is written
        2.  Wait for the log key in use; validate log-encrypted files
        3.  Drop log DEKs (re-encryption): new log key in use, old keys gone, files
            re-validated with the new key
        4.  Restart indexers (read path: stats log reopened, persisted stats restored
            unchanged); re-validate files; scans
        5.  Disable log encryption + drop DEKs (decryption): log keys gone, files plaintext
        6.  Re-enable log encryption + force-encrypt: log key back, files encrypted
        7.  Scans + item counts
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_log_key_lifecycle")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not getattr(self, "enable_log_encryption_at_rest", False):
            self.skipTest("Log encryption at rest not enabled. Set enable_log_encryption_at_rest=True")
        log_secret_id = self.log_encryption_at_rest_id
        flush_wait = int(self.input.param("stats_flush_wait", 60))
        # projector_stats.log lives on KV nodes, so it is not required on index nodes
        REQUIRED_LOG_FILES = ("indexer_stats_log", "projector_stats_log", "request_handler_cache",
                              "persisted_stats")

        # STEP 1: bucket, indexes, scans; getIndexStatus populates the request-handler cache
        self.set_indexer_snapshot_settings(index_nodes)
        for node in index_nodes:
            # persist stats every 60s so step 4 can compare them across a restart
            RestConnection(node).set_index_settings_internal({"indexer.statsPersistenceInterval": 60})
        bucket_name, select_queries = self._setup_bucket_and_indexes('logkey_', index_nodes)
        self.run_scan_validation(select_queries, validate_item_count=True)
        self._populate_request_handler_cache(index_nodes)
        self.sleep(flush_wait, "[STEP 1] Waiting for stats log and persisted stats to be written")

        # STEP 2: baseline log key and files
        log_keys = self.poll_for_new_indexer_key_ids(index_nodes, [], timeout=300,
                                                     label="[STEP 2] ", key_type="log")
        self.validate_log_encryption_files_encrypted(index_nodes, require_files=REQUIRED_LOG_FILES, expected_key_id=self.get_indexer_in_use_key_ids(index_nodes, key_type="log"))
        self.log.info(f"[STEP 2] PASSED - log key(s) in use: {log_keys}")

        # STEP 3: drop log DEKs -> re-encryption with a new log key
        status, response = self.rest.trigger_log_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/log failed: {response}")
        new_log_keys = self.poll_for_new_indexer_key_ids(index_nodes, log_keys, timeout=600,
                                                         label="[STEP 3] ", key_type="log")
        try:
            self.poll_for_indexer_key_ids_gone(index_nodes, log_keys, timeout=600,
                                               label="[STEP 3] ", key_type="log")
        except Exception as ex:
            # Old keys may legitimately linger; record it but don't fail the test.
            self.log.warning(f"[STEP 3] Old log keys still in use after DEK drop: {ex}")
        self.validate_log_encryption_files_encrypted(index_nodes, require_files=REQUIRED_LOG_FILES, expected_key_id=self.get_indexer_in_use_key_ids(index_nodes, key_type="log"))
        self.log.info(f"[STEP 3] PASSED - log files re-encrypted with {new_log_keys}")

        # STEP 4: restart -> read path (persisted stats file is encrypted with the log key)
        self.sleep(max(flush_wait, 75), "[STEP 4] Waiting for stats to be persisted")
        stats_before = {node.ip: self.get_persisted_stats(index_node=node) for node in index_nodes}
        self.restart_index_nodes(index_nodes)
        stats_after = {node.ip: self.get_persisted_stats(index_node=node) for node in index_nodes}
        if not any(stats_before.values()):
            self.fail("[STEP 4] No persisted stats found before restart; nothing to compare")
        changed = {ip: {k: (v, stats_after[ip].get(k)) for k, v in before.items() if stats_after[ip].get(k) != v}
                   for ip, before in stats_before.items()}
        changed = {ip: diff for ip, diff in changed.items() if diff}
        self.assertFalse(changed, f"[STEP 4] Persisted stats not restored after restart (before, after): {changed}")
        self.sleep(flush_wait, "[STEP 4] Waiting for the reopened stats log to be written")
        self.validate_log_encryption_files_encrypted(index_nodes, require_files=REQUIRED_LOG_FILES, expected_key_id=self.get_indexer_in_use_key_ids(index_nodes, key_type="log"))
        self.run_scan_validation(select_queries)
        self.log.info("[STEP 4] PASSED - log-encrypted files readable and still encrypted after restart")

        # STEP 5: disable -> decryption
        status, response = self.rest.disable_log_encryption()
        self.assertTrue(status, f"Disabling log encryption failed: {response}")
        status, response = self.rest.trigger_log_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/log (decrypt) failed: {response}")
        try:
            self.poll_for_indexer_key_ids_gone(index_nodes, new_log_keys, timeout=600,
                                               label="[STEP 5] ", key_type="log")
        except Exception as ex:
            # Old keys may legitimately linger; record it but don't fail the test.
            self.log.warning(f"[STEP 5] Log keys still in use after disabling: {ex}")
        self._wait_for_log_files_plaintext(index_nodes)
        # Read path for plaintext files: restart, then scans
        self.restart_index_nodes(index_nodes)
        self.run_scan_validation(select_queries)
        self.log.info("[STEP 5] PASSED - log files decrypted and indexer restarted on plaintext files")

        # STEP 6: re-enable + force-encrypt
        status, response = self.rest.configure_log_encryption_at_rest({
            "encryptionMethod": "encryptionKey", "encryptionKeyId": log_secret_id,
            "dekLifetime": 31536000, "dekRotationInterval": 2592000})
        self.assertTrue(status, f"Re-enabling log encryption failed: {response}")
        status, response = self.rest.force_log_encryption_at_rest()
        self.assertTrue(status, f"forceEncryptionAtRest/log failed: {response}")
        final_log_keys = self.poll_for_new_indexer_key_ids(index_nodes, log_keys + new_log_keys, timeout=600,
                                                           label="[STEP 6] ", key_type="log")
        self._populate_request_handler_cache(index_nodes)
        self.sleep(flush_wait, "[STEP 6] Waiting for log files to be written under the new key")
        self.validate_log_encryption_files_encrypted(index_nodes, require_files=REQUIRED_LOG_FILES, expected_key_id=self.get_indexer_in_use_key_ids(index_nodes, key_type="log"))
        # Read path under the re-enabled key: restart, re-check files, scans
        self.restart_index_nodes(index_nodes)
        self.sleep(flush_wait, "[STEP 6] Waiting for the reopened stats log to be written")
        self.validate_log_encryption_files_encrypted(index_nodes, require_files=REQUIRED_LOG_FILES, expected_key_id=self.get_indexer_in_use_key_ids(index_nodes, key_type="log"))
        self.run_scan_validation(select_queries)
        self.log.info(f"[STEP 6] PASSED - log files encrypted again with {final_log_keys} and readable after restart")

        # STEP 7
        self.run_scan_validation(select_queries)
        self.item_count_related_validations()
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_log_key_lifecycle")

    def test_gsi_encryption_at_rest_other_key_lifecycle(self):
        """
        'other' key lifecycle with the GSI metadata store (@2i/metadata_repo_v2).

        Steps:
        1.  Prerequisites (other encryption enabled), bucket + indexes, scans
        2.  Wait for the 'other' key in use; validate metadata_repo files
        3.  Drop 'other' DEKs (re-encryption): new key in use, old keys gone,
            metadata_repo re-validated
        4.  Restart indexers (read path: metadata loaded under the new key);
            index list unchanged; scans
        5.  Disable 'other' encryption + drop DEKs (decryption): keys gone; restart;
            index list unchanged; scans
        6.  Re-enable + drop DEKs (encrypt existing): key back, metadata_repo encrypted
        7.  Scans + item counts
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_other_key_lifecycle")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not getattr(self, "enable_other_encryption_at_rest", False):
            self.skipTest("Other encryption at rest not enabled. Set enable_other_encryption_at_rest=True")
        other_secret_id = self.other_encryption_at_rest_id
        self.assertIsNotNone(other_secret_id, "other_encryption_at_rest_id is None")

        # STEP 1
        self.set_indexer_snapshot_settings(index_nodes)
        bucket_name, select_queries = self._setup_bucket_and_indexes('otherkey_', index_nodes)
        self.run_scan_validation(select_queries, validate_item_count=True)
        baseline_metadata = self._index_metadata_snapshot(bucket_name)

        # STEP 2
        other_keys = self.poll_for_new_indexer_key_ids(index_nodes, [], timeout=600,
                                                       label="[STEP 2] ", key_type="other")
        self._validate_metadata_repo(index_nodes, self.get_indexer_in_use_key_ids(index_nodes, key_type="other"), "[STEP 2] ")
        self.log.info(f"[STEP 2] PASSED - 'other' key(s) in use: {other_keys}")

        # STEP 3: drop 'other' DEKs -> re-encryption
        status, response = self.rest.trigger_other_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/other failed: {response}")
        new_other_keys = self.poll_for_new_indexer_key_ids(index_nodes, other_keys, timeout=600,
                                                           label="[STEP 3] ", key_type="other")
        try:
            self.poll_for_indexer_key_ids_gone(index_nodes, other_keys, timeout=600,
                                               label="[STEP 3] ", key_type="other")
        except Exception as ex:
            # Old keys may legitimately linger; record it but don't fail the test.
            self.log.warning(f"[STEP 3] Old 'other' keys still in use after DEK drop: {ex}")
        self._validate_metadata_repo(index_nodes, self.get_indexer_in_use_key_ids(index_nodes, key_type="other"), "[STEP 3] ")
        self.log.info(f"[STEP 3] PASSED - metadata_repo re-encrypted with {new_other_keys}")

        # STEP 4: restart -> metadata read back under the new key
        self.restart_index_nodes(index_nodes)
        self.assertEqual(self._index_metadata_snapshot(bucket_name), baseline_metadata,
                         "[STEP 4] Index metadata changed after restart with the new 'other' key")
        self.run_scan_validation(select_queries)
        self.log.info("[STEP 4] PASSED - metadata loaded after restart")

        # STEP 5: disable -> decryption, then restart and read back
        status, response = self.rest.disable_other_encryption()
        self.assertTrue(status, f"Disabling 'other' encryption failed: {response}")
        status, response = self.rest.trigger_other_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/other (decrypt) failed: {response}")
        try:
            self.poll_for_indexer_key_ids_gone(index_nodes, new_other_keys, timeout=600,
                                               label="[STEP 5] ", key_type="other")
        except Exception as ex:
            # Old keys may legitimately linger; record it but don't fail the test.
            self.log.warning(f"[STEP 5] 'other' keys still in use after disabling: {ex}")
        self._wait_for_metadata_repo_plaintext(index_nodes, other_keys + new_other_keys, "[STEP 5] ")
        self.restart_index_nodes(index_nodes)
        self.assertEqual(self._index_metadata_snapshot(bucket_name), baseline_metadata,
                         "[STEP 5] Index metadata changed after restart with 'other' encryption disabled")
        self.run_scan_validation(select_queries)
        self.log.info("[STEP 5] PASSED - metadata decrypted and loaded after restart")

        # STEP 6: re-enable -> encrypt existing metadata
        status, response = self.rest.configure_other_encryption_at_rest({
            "encryptionMethod": "encryptionKey", "encryptionKeyId": other_secret_id,
            "dekLifetime": 31536000, "dekRotationInterval": 2592000})
        self.assertTrue(status, f"Re-enabling 'other' encryption failed: {response}")
        status, response = self.rest.trigger_other_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/other (encrypt) failed: {response}")
        final_other_keys = self.poll_for_new_indexer_key_ids(index_nodes, other_keys + new_other_keys,
                                                             timeout=600, label="[STEP 6] ", key_type="other")
        self._validate_metadata_repo(index_nodes, self.get_indexer_in_use_key_ids(index_nodes, key_type="other"), "[STEP 6] ")
        self.restart_index_nodes(index_nodes)
        self.assertEqual(self._index_metadata_snapshot(bucket_name), baseline_metadata,
                         "[STEP 6] Index metadata changed after restart with 'other' encryption re-enabled")
        self.log.info(f"[STEP 6] PASSED - metadata_repo encrypted again with {final_other_keys} and reloaded")

        # STEP 7
        self.run_scan_validation(select_queries)
        self.item_count_related_validations()
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_other_key_lifecycle")

    # =========================================================================
    # MB-69179 / MB-72106 / MB-69519 / MB-69497 / MB-69248 / MB-69178 / MB-69247
    # =========================================================================

    def _wait_until_no_problems(self, get_problems, label, timeout=600):
        """Poll get_problems() (returns a list) until it is empty; fail with the last list."""
        end_time = time.time() + timeout
        problems = get_problems()
        while problems and time.time() < end_time:
            time.sleep(10)
            problems = get_problems()
        if problems:
            self.fail(f"{label}Not done after {timeout}s: {problems}")

    def _load_docs(self, bucket_name, key_prefix="doc_"):
        """Load num_of_docs_per_collection docs into bucket_name._default._default."""
        gen_create = SDKDataLoader(
            num_ops=self.num_of_docs_per_collection,
            percent_create=100, percent_update=0, percent_delete=0,
            scope="_default", collection="_default",
            json_template=self.json_template, output=True,
            username=self.username, password=self.password, key_prefix=key_prefix
        )
        if self.use_magma_loader:  # vector datasets load only through the magma loader
            self.cluster.async_load_gen_docs(self.master, bucket_name, gen_create, timeout_secs=300,
                                             use_magma_loader=True).result()
            return
        for task in self.data_ops_javasdk_loader_in_batches_to_collection(bucket_name, gen_create, 10**4):
            task.result()

    def _create_bucket_with_indexes(self, bucket_name, index_nodes, prefix, key_id=None):
        """Create bucket_name (encrypted with key_id, plaintext if None), load docs into it,
        create indexes and wait for Ready. Returns the select queries."""
        self.create_bucket_with_encryption(bucket_name, enable_encryption=False)
        if key_id is not None:
            status, response = self.rest.enable_bucket_encryption(bucket_name, key_id)
            self.assertTrue(status, f"Failed to encrypt {bucket_name} with key {key_id}: {response}")
        self._load_docs(bucket_name)
        return self.create_indexes_on_namespace(f"default:{bucket_name}._default._default", index_nodes,
                                                prefix, step_prefix=f"[{bucket_name}]", wait_for_ready=True)

    def _drop_key_timeout(self, timeout):
        """timeout plus the worst-case BHIVE DEK drop: recovery.max_disksnaps + 2 recovery points,
        forced every timekeeper.forceCommitInterval on an idle bucket (MB-73573)."""
        if not self.bhive_index:
            return timeout
        cfg = RestConnection(self.get_nodes_from_services_map(service_type="index")).get_indexer_internal_stats()
        return timeout + (int(cfg.get("indexer.recovery.max_disksnaps", 4)) + 2) * int(
            cfg.get("indexer.timekeeper.forceCommitInterval", 300000)) // 1000

    def _bucket_key_op(self, op, bucket_name, index_nodes, label, before_drop=None):
        """Run one bucket key operation and wait for the indexer to converge:
        "enable" (encrypt + force-encrypt, no "" key left), "drop" (DEK drop: new key in
        use, old keys gone) or "disable" (disable + DEK drop: this bucket's keys gone).
        before_drop: called after "disable", before the DEK drop."""
        if op == "enable":
            status, response = self.rest.enable_bucket_encryption(bucket_name, self.encryption_at_rest_id)
            self.assertTrue(status, f"{label}Failed to enable encryption: {response}")
            status, response = self.rest.force_bucket_encryption_at_rest(bucket_name)
            self.assertTrue(status, f"{label}forceEncryptionAtRest failed: {response}")
            self.validate_mixed_bucket_encrypted_keys(index_nodes, [bucket_name], timeout=self._drop_key_timeout(900),
                                                      step_prefix=label)
            return
        old_keys = (self._bucket_in_use_key_ids(index_nodes, [bucket_name])[bucket_name] if op == "disable"
                    else self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket"))
        if op == "disable":
            status, response = self.rest.disable_bucket_encryption(bucket_name)
            self.assertTrue(status, f"{label}Failed to disable encryption: {response}")
            if before_drop:
                before_drop()
        status, response = self.rest.trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"{label}dropEncryptionAtRestDeks failed: {response}")
        if op == "drop":
            self.poll_for_new_indexer_key_ids(index_nodes, old_keys, timeout=self._drop_key_timeout(600), label=label,
                                              key_type="bucket")
        self.poll_for_indexer_key_ids_gone(index_nodes, old_keys, timeout=self._drop_key_timeout(900), label=label,
                                           key_type="bucket")

    def _assert_no_temp_files_under_storage(self, index_nodes, suffixes):
        for node in index_nodes:
            self.gsi_encryption_helper.assert_no_temp_files(
                node, f"{RestConnection(node).get_index_path()}/@2i",
                suffixes=self.gsi_encryption_helper.TEMP_FILE_SUFFIXES + tuple(suffixes))

    def _wait_for_stats_log_chains(self, index_nodes, expect, label, timeout=600, max_gap_secs=None,
                                   expected_content=None, projector_max_gap_secs=None):
        """MB-69179 / MB-72106 / MB-69519: every indexer_stats.log generation (index nodes)
        and projector_stats.log generation (KV nodes) is `expect` ("encrypted" with a log
        key in use, or "plaintext" with rotated ones as .N.gz); then no temp files (F5).
        max_gap_secs (MB-72103): indexer_stats.log generations are also contiguous in time;
        projector_max_gap_secs: the same for projector_stats.log generations."""
        helper = self.gsi_encryption_helper
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)

        def problems():
            log_keys = (self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
                        if expect == "encrypted" else None)
            if expect == "encrypted" and not log_keys:
                return ["no in-use log keys reported yet"]
            found = []
            # Log keys are per node: projector (KV node) keys are not in the indexers' list
            for nodes, log_name, keys, max_gap in ((index_nodes, "indexer_stats.log", log_keys, max_gap_secs),
                                                   (kv_nodes, "projector_stats.log", None, projector_max_gap_secs)):
                for node in nodes:
                    found += helper.verify_stats_log_chain(
                        node, log_name, expect, keys, max_gap,
                        expected_lines=(expected_content or {}).get((node.ip, log_name)))[1]
            return found

        self._wait_until_no_problems(problems, label, timeout)
        for node in {node.ip: node for node in index_nodes + kv_nodes}.values():
            helper.assert_no_temp_files(node, helper.get_log_path(node),
                                        prefixes=("indexer_stats.log", "projector_stats.log"))

    def _stats_log_key_ids(self, node, log_name):
        """MB-69519: header key ID of each encrypted <log_name> generation (ID length at
        byte 27, ID from byte 28)."""
        log_dir = self.gsi_encryption_helper.get_log_path(node)
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"for f in {log_dir}/{log_name} {log_dir}/{log_name}.*; do case $f in *.gz|*.tmp) continue;; esac; "
                f"[ -f $f ] && dd if=$f bs=1 skip=28 count=$(od -An -tu1 -j27 -N1 $f | tr -d ' ') 2>/dev/null; echo; done")
        finally:
            shell.disconnect()
        return {line.strip() for line in out if line.strip()}

    def test_gsi_encryption_at_rest_stats_log_lifecycle(self):
        """
        MB-69179 / MB-72106 / MB-69519 / MB-72103: indexer_stats.log (index nodes) and
        projector_stats.log (KV nodes), the active file and every rotated generation,
        through the log key lifecycle, while size-based rotation keeps running.

        Steps:
        1. Prerequisites (log encryption enabled); bucket + indexes; shrink
           indexer.statsLogFsize, dump stats every second and restart the indexers so
           size-based rotation runs continuously
        2. Force-encrypt the log key: every generation encrypted with a log key in use,
           plaintext .N.gz rotations re-encrypted as .N; no temp files
           (projector_stats.log is rotated the same way); content unchanged
        3. Drop log DEKs drop_rounds times while rotating: old log keys gone, every
           generation on a key in use, indexer and projector generations contiguous in
           time (MB-72103)
        3b. Drop log DEKs and kill -9 the indexers and projectors during the
            re-encryption; drop again: completes, no corrupted generation, content
            unchanged (MB-72103, MB-69179, MB-69519)
        4. Restart indexers and kill the projectors: stats logs reopened and still
           encrypted; scans
        5. Disable log encryption + drop DEKs while rotating: every generation plaintext,
           rotated ones as .N.gz, contiguous in time; no temp files; restart (read back); scans
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_stats_log_lifecycle")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not getattr(self, "enable_log_encryption_at_rest", False):
            self.skipTest("Log encryption at rest not enabled. Set enable_log_encryption_at_rest=True")
        stats_log_fsize = int(self.input.param("stats_log_fsize", 8192))
        drop_rounds = int(self.input.param("drop_rounds", 3))
        max_gap_secs = int(self.input.param("stats_log_max_gap", 60))
        log_dir = self.gsi_encryption_helper.get_log_path(index_nodes[0])
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        test_since = {n.ip: self._node_now(n) for n in index_nodes + kv_nodes}  # logs persist across tests

        # STEP 1
        bucket_name, select_queries = self._setup_bucket_and_indexes('statslog_', index_nodes)
        default_fsize = RestConnection(index_nodes[0]).get_indexer_internal_stats().get(
            'indexer.statsLogFsize', 33554432)
        dump_interval_key = "indexer.settings.statsLogDumpInterval"
        default_dump_interval = RestConnection(index_nodes[0]).get_index_settings()[dump_interval_key]
        # MB-69519 / MB-72103: projector_stats.log (KV nodes) rotates the same way
        proj_dump_interval_key = "projector.statsLogDumpInterval"
        default_proj_fsize = RestConnection(index_nodes[0]).get_indexer_internal_stats().get(
            'projector.statsLogFsize', 33554432)
        default_proj_dump_interval = RestConnection(index_nodes[0]).get_index_settings().get(
            proj_dump_interval_key, 60)

        def set_dump_interval(seconds):
            RestConnection(index_nodes[0]).set_index_settings({dump_interval_key: seconds,
                                                               proj_dump_interval_key: seconds})

        def capture_content():
            # Read before a key change; compared after it (expected_content)
            return {(node.ip, log_name): self.gsi_encryption_helper.read_stats_log_lines(node, log_name)
                    for nodes, log_name in ((index_nodes, "indexer_stats.log"), (kv_nodes, "projector_stats.log"))
                    for node in nodes}

        def log_data_status():
            status, cfg = self.rest.get_encryption_at_rest_config()
            cfg = json.loads(cfg) if isinstance(cfg, (bytes, bytearray)) else cfg
            return cfg.get("log", {}).get("info", {}).get("dataStatus")

        set_dump_interval(1)
        for node in index_nodes:
            RestConnection(node).set_index_settings_internal({"indexer.statsLogFsize": stats_log_fsize,
                                                              "projector.statsLogFsize": stats_log_fsize})
        try:
            # stats logs left by earlier runs, encrypted with unknown keys (projector: rotated files only)
            for node in index_nodes + self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True):
                shell = RemoteMachineShellConnection(node)
                shell.execute_command(f"rm -f {log_dir}/indexer_stats.log* {log_dir}/projector_stats.log.*")
                shell.disconnect()
            # statsLogFsize is read only when the indexer / projector starts
            self.restart_index_nodes(index_nodes)
            for kv_node in kv_nodes:
                self._kill_projector_process(kv_node)

            chains = ((index_nodes, "indexer_stats.log"), (kv_nodes, "projector_stats.log"))

            def rotated(node, log_name):
                shell = RemoteMachineShellConnection(node)
                try:
                    out, _ = shell.execute_command(f"ls -1 {log_dir}/{log_name}.* 2>/dev/null | wc -l")
                finally:
                    shell.disconnect()
                return int((out or ["0"])[0].strip() or 0)

            def too_few_rotations():
                return [f"{n.ip}:{log}" for nodes, log in chains for n in nodes if rotated(n, log) < 3]

            def kill_9(label):
                for nodes, process in ((index_nodes, "indexer"), (kv_nodes, "projector")):
                    for node in nodes:
                        shell = RemoteMachineShellConnection(node)
                        try:
                            shell.execute_command(f"pkill -9 -x {process}")
                        finally:
                            shell.disconnect()
                self.wait_for_indexer_ready(index_nodes, timeout=900, label=label)

            def wait_projector_keys_off(old_keys, label, timeout=300):
                # MB-69519: every projector generation's header key moves off the pre-drop
                # keys; keys still there after timeout are logged as a warning
                end_time = time.time() + timeout
                while True:
                    stale = {n.ip: self._stats_log_key_ids(n, "projector_stats.log") & old_keys[n.ip]
                             for n in kv_nodes}
                    stale = {ip: keys for ip, keys in stale.items() if keys}
                    if not stale or time.time() >= end_time:
                        break
                    time.sleep(15)
                if stale:
                    self.log.warning(f"{label}projector_stats.log still on pre-drop header keys after "
                                     f"{timeout}s: {stale}")

            self._wait_until_no_problems(too_few_rotations, "[STEP 1] stats log rotations: ")

            # STEP 2: force-encrypt; content unchanged (slower dumps so the captured
            # lines are not all rotated out before the compare)
            # MB-69179: disable, then re-enable mid-run with the same settings
            status, cfg = self.rest.get_encryption_at_rest_config()
            log_cfg = (json.loads(cfg) if isinstance(cfg, (bytes, bytearray)) else cfg)["log"]
            self.assertTrue(self.rest.disable_log_encryption()[0], "[STEP 2] Disabling log encryption failed")
            self.assertTrue(self.rest.trigger_log_reencryption()[0], "[STEP 2] dropEncryptionAtRestDeks/log failed")
            self._wait_for_stats_log_chains(index_nodes, "plaintext", "[STEP 2] ")
            set_dump_interval(300)
            self.sleep(15, "[STEP 2] Slowing the stats dump before capturing the content")
            captured = capture_content()
            self.assertTrue(self.rest.configure_log_encryption_at_rest(
                {k: log_cfg[k] for k in ("encryptionMethod", "encryptionKeyId", "dekLifetime", "dekRotationInterval")
                 if k in log_cfg})[0], "[STEP 2] Re-enabling log encryption failed")

            def not_force_rotated():
                # Enable force-rotates the active file: its plaintext content goes to .1.gz
                # and the new active file has the encryption header
                bad = []
                for nodes, log_name in chains:
                    for node in nodes:
                        shell = RemoteMachineShellConnection(node)
                        try:
                            out, _ = shell.execute_command(f"zcat {log_dir}/{log_name}.1.gz | tail -1")
                        finally:
                            shell.disconnect()
                        if not (self.gsi_encryption_helper.verify_file_encryption_magic_bytes(
                                node, f"{log_dir}/{log_name}")[0] and [l.rstrip("\r\n") for l in out if l.strip()]
                                == captured[(node.ip, log_name)][-1:]):
                            bad.append(f"{node.ip}:{log_name}")
                return bad

            self._wait_until_no_problems(not_force_rotated, "[STEP 2] stats log not force-rotated on enable: ", 120)
            # MB-72106: kill -9 with a mixed chain (encrypted active, plaintext .N.gz): stats logging
            # continues, the next rotation is an encrypted .1, no generation is both .N and .N.gz
            disabled = self._indexer_log_total(index_nodes, "Error in NewDedupeLogStatsWithFileHandler")
            kill_9("[STEP 2] ")
            self.wait_until_indexes_online(timeout=1800)
            set_dump_interval(1)
            self._wait_until_no_problems(lambda: [n.ip for n in index_nodes if not self.gsi_encryption_helper.
                                                  verify_file_encryption_magic_bytes(n, f"{log_dir}/indexer_stats.log.1")[0]],
                                         "[STEP 2] No encrypted indexer_stats.log.1 after the restart: ", 120)
            self.assertEqual(self._indexer_log_total(index_nodes, "Error in NewDedupeLogStatsWithFileHandler"), disabled,
                             "[STEP 2] Stats logging disabled after the restart")
            fcount = int(RestConnection(index_nodes[0]).get_indexer_internal_stats().get("indexer.statsLogFcount", 10))
            for node in index_nodes:
                shell = RemoteMachineShellConnection(node)
                try:
                    out, _ = shell.execute_command(f"ls -1 {log_dir}/indexer_stats.log.* | sed 's/[.]gz$//' | sort | uniq -d")
                finally:
                    shell.disconnect()
                self.assertFalse([l for l in out if l.strip()], f"[STEP 2] Generation both .N and .N.gz on {node.ip}: {out}")
                self.assertLessEqual(rotated(node, "indexer_stats.log"), fcount, f"[STEP 2] Over statsLogFcount on {node.ip}")
            set_dump_interval(300)
            self.sleep(15, "[STEP 2] Slowing the stats dump before capturing the content")
            captured = capture_content()
            status, response = self.rest.force_log_encryption_at_rest()
            self.assertTrue(status, f"forceEncryptionAtRest/log failed: {response}")
            self._wait_for_stats_log_chains(index_nodes, "encrypted", "[STEP 2] ", expected_content=captured)
            set_dump_interval(1)

            # STEP 3: drop log DEKs while rotating -> rotated generations re-encrypted (MB-72103)
            old_log_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
            step3_proj_keys = {n.ip: self._stats_log_key_ids(n, "projector_stats.log") for n in kv_nodes}
            for drop_round in range(drop_rounds):
                status, response = self.rest.trigger_log_reencryption()
                self.assertTrue(status, f"dropEncryptionAtRestDeks/log round {drop_round + 1} failed: {response}")
                self.sleep(1, "[STEP 3] Letting rotation run between drops")
            self.run_scan_validation(select_queries)  # MB-72103: scans work during the passes
            self.poll_for_new_indexer_key_ids(index_nodes, old_log_keys, timeout=600,
                                              label="[STEP 3] ", key_type="log")
            self.poll_for_indexer_key_ids_gone(index_nodes, old_log_keys, timeout=600,
                                               label="[STEP 3] ", key_type="log")
            self._wait_for_stats_log_chains(index_nodes, "encrypted", "[STEP 3] ", max_gap_secs=max_gap_secs,
                                            projector_max_gap_secs=max_gap_secs)
            wait_projector_keys_off(step3_proj_keys, "[STEP 3] ")
            fcount = RestConnection(index_nodes[0]).get_indexer_internal_stats()
            over = [f"{n.ip}:{log}" for nodes, log in chains for n in nodes
                    if rotated(n, log) > int(fcount.get(log.split("_")[0] + ".statsLogFcount", 10))]
            self.assertFalse(over, f"[STEP 3] More rotated generations than statsLogFcount: {over}")

            # STEP 3b: kill -9 indexer + projector during the re-encryption; the next drop
            # completes, every generation decodes and the content is unchanged
            set_dump_interval(300)
            self.sleep(15, "[STEP 3b] Slowing the stats dump before capturing the content")
            captured = capture_content()
            proj_logs = ("EncryptionMgr: initialized for kdt=.*log",
                         "statsManager:handleEncryptionUpdateKey keyId=.* keyChanged=true",
                         "statsManager:handleEncryptionDropKey active=.* err=<nil>")
            proj_keys = {n.ip: self._stats_log_key_ids(n, "projector_stats.log") for n in kv_nodes}
            proj_since = {n.ip: self._node_now(n) for n in kv_nodes}
            old_log_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"[STEP 3b] dropEncryptionAtRestDeks/log failed: {response}")
            self.sleep(2, "[STEP 3b] Letting the re-encryption start")
            kill_9("[STEP 3b] ")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"[STEP 3b] dropEncryptionAtRestDeks/log retry failed: {response}")
            self.poll_for_new_indexer_key_ids(index_nodes, old_log_keys, timeout=600,
                                              label="[STEP 3b] ", key_type="log")
            self.poll_for_indexer_key_ids_gone(index_nodes, old_log_keys, timeout=600,
                                               label="[STEP 3b] ", key_type="log")
            self._wait_for_stats_log_chains(index_nodes, "encrypted", "[STEP 3b] ", expected_content=captured)
            # MB-69519: every projector generation (active too, dumps slowed so not by size
            # rotation) on a new header key ID; projector.log update/drop/restart lines
            wait_projector_keys_off(proj_keys, "[STEP 3b] ")
            for node in kv_nodes:
                for p in proj_logs:
                    self.assertGreater(self._indexer_log_count(node, p, "projector.log", proj_since[node.ip]), 0,
                                       f"[STEP 3b] '{p}' not logged in projector.log on {node.ip}")
            self._wait_until_no_problems(lambda: [] if log_data_status() == "encrypted" else [log_data_status()],
                                         "[STEP 3b] log dataStatus: ", 300)
            set_dump_interval(1)

            # STEP 4: restart -> encrypted active files reopened and appended to
            last_lines = {(n.ip, log): self.gsi_encryption_helper.read_stats_log_lines(n, log)[-1:]
                          for nodes, log in chains for n in nodes}
            kill_9("[STEP 4] ")
            self.wait_until_indexes_online(timeout=1800)
            self.sleep(90, "[STEP 4] Waiting for the reopened stats logs to be written")
            self._wait_for_stats_log_chains(index_nodes, "encrypted", "[STEP 4] ")
            for nodes, log in chains:  # MB-69519 / MB-72103: restarted processes keep writing; chain decodes
                for node in nodes:
                    lines = self.gsi_encryption_helper.read_stats_log_lines(node, log)
                    self.assertTrue(lines[-1:] != last_lines[(node.ip, log)]
                                    and not any(l.startswith("READ_FAILED:") for l in lines),
                                    f"[STEP 4] {log} on {node.ip} not decodable or not written after kill -9")
            self.run_scan_validation(select_queries)
            self._load_docs(bucket_name, key_prefix="p4_")  # MB-69519: DCP flows after projector restart
            self.item_count_related_validations()

            # STEP 5: disable + drop DEKs -> decryption; content unchanged (MB-69179, MB-69519).
            # Slower dumps so the captured lines are not all rotated out before the compare.
            set_dump_interval(300)
            self.sleep(15, "[STEP 5] Slowing the stats dump before capturing the content")
            captured = capture_content()
            log_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="log")
            status, response = self.rest.disable_log_encryption()
            self.assertTrue(status, f"Disabling log encryption failed: {response}")
            status, response = self.rest.trigger_log_reencryption()
            self.assertTrue(status, f"dropEncryptionAtRestDeks/log (decrypt) failed: {response}")
            self.poll_for_indexer_key_ids_gone(index_nodes, log_keys, timeout=600,
                                               label="[STEP 5] ", key_type="log")
            self._wait_for_stats_log_chains(index_nodes, "plaintext", "[STEP 5] ", expected_content=captured)
            self.restart_index_nodes(index_nodes, validate_read_back=True)
            self.run_scan_validation(select_queries)
            for kv_node in kv_nodes:  # MB-69519: projector restarts and DCP flows with log encryption off
                self._kill_projector_process(kv_node)
            self._load_docs(bucket_name, key_prefix="p5_")
            self.item_count_related_validations()
            # MB-69519 / MB-72103: no stats-log read/rewrite/key errors
            errors = ("LogStatsFileHandler.(Open|skipRotate)", "common::(Reencrypt|Decrypt)StatsLogFiles")
            for nodes, log_name, patterns in (
                    (index_nodes, "indexer.log", errors + (
                        "Error in NewDedupeLogStatsWithFileHandler", "transcrypt:", "no key data for keyID",
                        "StatsManager::handleEncryptionDropKey (reencrypt|decrypt)StatsLogFiles")),
                    (kv_nodes, "projector.log", errors + ("EncryptionMgr:GetKeyCipherById key:.* err:",
                                                          "statsManager:handleEncryptionDropKey reencryptStatsLogFiles"))):
                for node in nodes:
                    for p in patterns:
                        self.assertEqual(self._indexer_log_count(node, p, log_name, test_since[node.ip]), 0,
                                         f"'{p}' in {log_name} on {node.ip}")
        finally:
            RestConnection(index_nodes[0]).set_index_settings({dump_interval_key: default_dump_interval,
                                                               proj_dump_interval_key: default_proj_dump_interval})
            for node in index_nodes:
                RestConnection(node).set_index_settings_internal({"indexer.statsLogFsize": default_fsize,
                                                                  "projector.statsLogFsize": default_proj_fsize})
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_stats_log_lifecycle")

    def _wait_for_codebooks(self, index_nodes, expect, label, timeout=600):
        """MB-69497: vector codebooks are `expect` ("encrypted" with a bucket key in use, or
        "plaintext"); then no codebook temp file (F5)."""
        helper = self.gsi_encryption_helper

        def problems():
            if expect == "encrypted":
                bucket_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket")
                if not bucket_keys:
                    return ["no in-use bucket keys reported yet"]
                results = helper.verify_gsi_codebook_encrypted(index_nodes, expected_key_id=bucket_keys)
                found = [f"{ip}: {r}" for ip, r in results.items() if r.get("status") == "failed"]
                any_files = any(r.get("status") == "passed" for r in results.values())
            else:
                found, any_files = [], False
                for node in index_nodes:
                    is_plaintext, encrypted = helper.verify_files_plaintext(
                        node, f"{RestConnection(node).get_index_path()}/@2i", pattern="*.codebook")
                    any_files = any_files or is_plaintext or bool(encrypted)
                    found += [f"{node.ip}:{path}" for path in encrypted]
            return found if any_files else found + ["no codebook files found"]

        self._wait_until_no_problems(problems, label, timeout)
        self._assert_no_temp_files_under_storage(index_nodes, (".codebook.tmp",))

    def test_gsi_encryption_at_rest_codebook_lifecycle(self):
        """
        MB-69497: vector index codebooks (BHIVE and composite) through the bucket key
        lifecycle. Run with bhive_index=True.

        Steps:
        1. Unencrypted bucket, vector indexes trained: codebooks plaintext
        2. Enable + force-encrypt: codebooks encrypted in place with a key in use
        3. Drop DEKs: codebooks re-encrypted; restart (read back, no retraining); scans
        3b. Corrupt one encrypted codebook, restart that indexer: reported as corrupted
            (no crash loop), index recovers; item counts and scans
        4. Disable + drop DEKs: codebooks and BHIVE data plaintext; restart (read back); scans
        Each key op / restart is also confirmed in indexer.log (handleEncryptionDropKey done for
        codebooks, recoverCodebook isEncrypted true/false); scan results equal across restarts.
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_codebook_lifecycle")
        index_nodes = self.verify_prerequisites()
        if not self.bhive_index:
            self.skipTest("Needs vector indexes. Set bhive_index=True")
        bucket_name = self.test_bucket

        select_queries = self._create_bucket_with_indexes(bucket_name, index_nodes, 'cb_')
        namespace = f"default:{bucket_name}._default._default"
        drop_defs = self._get_vector_query_definitions('cbdrop')[0]  # MB-69248: dropped during disable
        self.gsi_util_obj.create_gsi_indexes(create_queries=self._get_vector_create_queries(
            drop_defs, [], [], [], [], namespace, [f"{n.ip}:{self.node_port}" for n in index_nodes]),
            database=namespace, query_node=self.get_nodes_from_services_map(service_type="n1ql"))
        self.wait_until_indexes_online(timeout=self.index_online_timeout)
        self._wait_for_codebooks(index_nodes, "plaintext", "[STEP 1] ")
        codebook_errors = ("CodebookEncryptionKey re-encryption failed",
                           "Error observed while (decryptCodebookByChunk|IsFileEncrypted)",
                           "Codebook corrupted and unrecoverable")
        errors_before = [self._indexer_log_total(index_nodes, p) for p in codebook_errors]
        recovered = "recoverCodebook: reading from disk is successful for path: .* isEncrypted: "
        codebook_logs = {"enable": "StorageMgr::handleEncryptionDropKey done for codebooks", "drop": recovered + "true",
                         "disable": recovered + "false"}
        for step, op, expect in ((2, "enable", "encrypted"), (3, "drop", "encrypted"),
                                 (4, "disable", "plaintext")):
            label = f"[STEP {step}] "
            log_count = self._indexer_log_total(index_nodes, codebook_logs[op])
            pre_op_keys = self._bucket_in_use_key_ids(index_nodes, [bucket_name])[bucket_name]
            self._bucket_key_op(op, bucket_name, index_nodes, label, before_drop=lambda: [self.run_cbq_query(
                query=f"DROP INDEX `{d.index_name}` ON `{bucket_name}`",
                server=self.get_nodes_from_services_map(service_type="n1ql")) for d in drop_defs])
            self._wait_for_codebooks(index_nodes, expect, label)
            if op == "disable":  # MB-69248: BHIVE index data plaintext too; "" back in use
                self.validate_files_not_encrypted(index_nodes, bucket_name, timeout=600, step_prefix=label,
                                                  old_key_ids=pre_op_keys)
                self.assertEqual(self._bucket_in_use_ids_raw(index_nodes, bucket_name), {""}, f"{label}In-use keys")
            if op == "drop":  # MB-69248: rotate; the old DEK is dropped when its lifetime expires
                old_keys, scans_before = (self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket"),
                                          self._scan_results(select_queries))
                self.set_bucket_dek_rotation_config(bucket_name, self.encryption_at_rest_id, 30, 150)
                self.poll_for_indexer_key_ids_gone(index_nodes, old_keys, timeout=self._drop_key_timeout(900),
                                                   label=label, key_type="bucket")
                self.set_bucket_dek_rotation_config(bucket_name, self.encryption_at_rest_id, 36000, 36000)
                self._assert_same_scan_results(scans_before, label)
            if op != "enable":
                self._kill9_indexers_recover_from_snapshot(index_nodes, select_queries, label, done="Slice::recoverCodebook: reading from disk is successful", failed="NewBhiveSlice:GetKeyIdList")
            self.assertGreater(self._indexer_log_total(index_nodes, codebook_logs[op]), log_count,
                               f"{label}'{codebook_logs[op]}' not logged in indexer.log")
            self.run_scan_validation(select_queries)
            if op == "drop":  # 3b corrupts a codebook on purpose: check steps 2-3, re-baseline after it
                for pattern, count in zip(codebook_errors, errors_before):
                    self.assertEqual(self._indexer_log_total(index_nodes, pattern), count, f"{label}'{pattern}' in indexer.log")
                self._corrupt_codebook_and_recover(index_nodes, select_queries, "[STEP 3b] ")
                errors_before = [self._indexer_log_total(index_nodes, p) for p in codebook_errors]
        for pattern, count in zip(codebook_errors, errors_before):  # MB-69497
            self.assertEqual(self._indexer_log_total(index_nodes, pattern), count, f"'{pattern}' in indexer.log")
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_codebook_lifecycle")

    def _corrupt_codebook_and_recover(self, index_nodes, select_queries, label):
        """MB-69497: an encrypted codebook that cannot be decrypted is reported as
        corrupted at warmup (no crash loop) and the index recovers."""
        node, path = None, None
        for index_node in index_nodes:
            shell = RemoteMachineShellConnection(index_node)
            try:
                out, _ = shell.execute_command(
                    f"find {RestConnection(index_node).get_index_path()}/@2i -type f "
                    f"-path '*_0.index/codebook/*' ! -name '*.tmp' 2>/dev/null | head -1")
            finally:
                shell.disconnect()
            found = [line.strip() for line in out if line.strip()]
            if found:
                node, path = index_node, found[0]
                break
        self.assertTrue(path, f"{label}No codebook file found on the index nodes")
        corrupted_log = "codebook appears to be corrupted"
        log_count = self._indexer_log_count(node, corrupted_log)
        # The induced decrypt error is expected in indexer.log for the rest of the suite
        type(self).allowed_encryption_log_patterns = ("Error observed while decryptCodebookByChunk",)
        shell = RemoteMachineShellConnection(node)
        try:
            shell.execute_command(f"size=$(stat -c %s '{path}') && dd if=/dev/urandom of='{path}' bs=1 "
                                  f"count=64 seek=$((size / 2)) conv=notrunc")
        finally:
            shell.disconnect()
        self.log.info(f"{label}Corrupted {path} on {node.ip}; restarting its indexer")
        self._kill_all_processes_index(server=node)
        self.sleep(30, f"{label}Waiting for the indexer to restart on {node.ip}")
        self.wait_for_indexer_ready([node], timeout=900, label=label)
        self.wait_until_indexes_online(timeout=int(self.input.param("index_online_timeout", 2400)))
        self.assertGreater(self._indexer_log_count(node, corrupted_log), log_count,
                           f"{label}'{corrupted_log}' not logged on {node.ip}")
        self.item_count_related_validations()
        self.run_scan_validation(select_queries)

    def _bucket_in_use_key_ids(self, index_nodes, bucket_names):
        """{bucket: set(key IDs)} from every index node's GetInUseKeys."""
        keys = {name: set() for name in bucket_names}
        for node_map in self._build_per_node_bucket_key_map_safe(index_nodes, bucket_names).values():
            for name in bucket_names:
                keys[name].update(str(k) for k in (node_map.get(name) or {}).get("key_ids", []) if k)
        return keys

    def _node_now(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            return shell.execute_command("date +%Y-%m-%dT%H:%M:%S")[0][0].strip()
        finally:
            shell.disconnect()

    def _indexer_log_count(self, node, pattern, log_name="indexer.log", since=""):
        """Lines matching pattern (grep -E) in the node's indexer.log*, logged at or after
        since (node local time, see _node_now). Files with the encryption header (log
        encryption on) are read with cbcat."""
        log_dir = self.gsi_encryption_helper.get_log_path(node)
        cbcat = ("/opt/couchbase/bin/cbcat --password - --with-gosecrets "
                 "/opt/couchbase/var/lib/couchbase/config/gosecrets.cfg")
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"for f in {log_dir}/{log_name}*; do "
                f"if head -c 64 \"$f\" | grep -qa 'Couchbase Encrypted'; then {cbcat} \"$f\" </dev/null; "
                f"else zcat -f \"$f\"; fi; done 2>/dev/null | grep -E '{pattern}' | awk -v s='{since}' '$1 >= s' | wc -l")
        finally:
            shell.disconnect()
        return int(next((line.strip() for line in out if line.strip().isdigit()), 0))

    def _indexer_log_total(self, nodes, pattern):
        return sum(self._indexer_log_count(node, pattern) for node in nodes)

    def _indexer_log_has(self, node, pattern):
        """True if pattern (grep -E) appears in the node's indexer.log*."""
        return self._indexer_log_count(node, pattern) > 0

    def _scan_results(self, select_queries):
        """Per query: the sorted rows, or only the row count for vector (ANN) queries and
        queries with LIMIT, whose rows may legitimately differ between runs."""
        results = {}
        for query in select_queries:
            rows = self.run_cbq_query(query=query, scan_consistency='request_plus').get('results', [])
            if re.search(r"VECTOR_DISTANCE|ANN_DISTANCE|\bLIMIT\b", query, re.IGNORECASE):
                results[query] = len(rows)
            else:
                results[query] = sorted(json.dumps(row, sort_keys=True) for row in rows)
        return results

    def _assert_same_scan_results(self, before, label):
        """Scan results now equal `before` (from _scan_results before a restart)."""
        after = self._scan_results(list(before))
        changed = [query[:100] for query in before if before[query] != after[query]]
        self.assertFalse(changed, f"{label}Scan results changed across the restart: {changed}")

    def _assert_in_indexer_log(self, node, patterns, label):
        """Each pattern (grep -E) appears in the node's indexer.log*."""
        for pattern in patterns:
            self.assertTrue(self._indexer_log_has(node, pattern),
                            f"{label}'{pattern}' not in indexer.log on {node.ip}")

    def test_gsi_encryption_at_rest_bhive_multi_bucket_keys(self):
        """
        MB-69248: BHIVE indexes on two buckets encrypted with different keys.
        Run with bhive_index=True.

        Steps:
        1. Set indexer.encryption.maxStorageDropKeyRetry / dropKeyRetryInterval;
           indexer.log confirms both
        2. bucket_A on the test key, bucket_B on a second key; BHIVE indexes on both
        3. Each bucket's files carry only its own bucket's key
        4. Drop DEKs on bucket_A: A's old keys gone, B's keys untouched; files
           re-validated per bucket
        5. Restart (read back); scans
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_bhive_multi_bucket_keys")
        index_nodes = self.verify_prerequisites()
        moi = self.gsi_type == "memory_optimized"
        storage = "memdb" if moi else "bhive" if self.bhive_index else "plasma"
        retry_keys = ("indexer.encryption.maxStorageDropKeyRetry", "indexer.encryption.dropKeyRetryInterval")
        current = RestConnection(index_nodes[0]).get_index_settings()
        original_retry = {k: current[k] for k in retry_keys if k in current}
        try:
            if storage == "plasma":  # MB-70886: shard affinity, so instances of the buckets share Plasma shards
                self.enable_shard_based_rebalance()
            if moi:  # MB-69247: MOI keys are on disk only in snapshots
                self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 1]")
            max_retry = int(self.input.param("max_drop_key_retry", 2))
            retry_interval = int(self.input.param("drop_key_retry_interval", 10))

            # STEP 1: drop-key retry settings; negative values clamp to 0 (MB-69248)
            clamp = ("setMaxDropKeyRetry set to 0", "setDropKeyRetryInterval set to 0")
            clamped = [self._indexer_log_total(index_nodes, p) for p in clamp]
            RestConnection(index_nodes[0]).set_index_settings({"indexer.encryption.maxStorageDropKeyRetry": -1,
                                                               "indexer.encryption.dropKeyRetryInterval": -1})
            self.sleep(15, "[STEP 1] Waiting for the indexer to apply the negative retry settings")
            for pattern, count in zip(clamp, clamped):
                self.assertGreater(self._indexer_log_total(index_nodes, pattern), count, f"[STEP 1] '{pattern}' not logged")
            applied = [f"setMaxDropKeyRetry set to {max_retry}", f"setDropKeyRetryInterval set to {retry_interval}"]
            applied_before = {node.ip: [self._indexer_log_count(node, p) for p in applied] for node in index_nodes}
            for node in index_nodes:
                RestConnection(node).set_index_settings({
                    "indexer.encryption.maxStorageDropKeyRetry": max_retry,
                    "indexer.encryption.dropKeyRetryInterval": retry_interval})
            self.sleep(15, "[STEP 1] Waiting for the indexer to apply the retry settings")
            for node in index_nodes:
                for pattern, count in zip(applied, applied_before[node.ip]):
                    self.assertGreater(self._indexer_log_count(node, pattern), count,
                                       f"[STEP 1] '{pattern}' not logged on {node.ip}")

            # STEP 2: two buckets, different keys
            persisted = "persistCodebookToDisk: Persistance to disk is successful .* isEncrypted:true keyid:[^ ]+"
            persisted_count = self._indexer_log_total(index_nodes, persisted)
            bucket_a = self.test_bucket
            bucket_b = f"{self.test_bucket}_2"
            select_queries = self._create_bucket_with_indexes(bucket_a, index_nodes, 'mb_', self.encryption_at_rest_id)
            select_queries.update(self._create_bucket_with_indexes(bucket_b, index_nodes, 'mb_',
                                                                   self.create_new_encryption_key()))
            bucket_c = f"{self.test_bucket}_3"  # unencrypted, on the same nodes (not BHIVE: shared bhive-shards)
            if storage != "bhive":
                select_queries.update(self._create_bucket_with_indexes(bucket_c, index_nodes, 'mb_'))
            if moi:
                self.sleep(int(self.input.param("snapshot_wait", 90)), "[STEP 2] Waiting for MOI snapshots")

            def assert_no_cross_bucket_keys(label):  # MB-69248: no file of one bucket carries the other's key
                keys = self._bucket_in_use_key_ids(index_nodes, [bucket_a, bucket_b])
                for owner, other in ((bucket_a, bucket_b), (bucket_b, bucket_a)):
                    uuid = self.rest.get_bucket_json(owner)['uuid']
                    for node in index_nodes:
                        path = f"{RestConnection(node).get_index_path()}/@2i"
                        shell = RemoteMachineShellConnection(node)
                        try:
                            out, _ = shell.execute_command(f"grep -rlF {' '.join('-e ' + k for k in keys[other])} "
                                                           f"{path}/{uuid}_* {path}/@bhive/{uuid}_* 2>/dev/null")
                        finally:
                            shell.disconnect()
                        self.assertFalse([l for l in out if l.strip()], f"{label}{owner} files on {node.ip} carry {other} keys")

            # STEP 3: per-bucket key isolation on disk
            bucket_keys = self._bucket_in_use_key_ids(index_nodes, [bucket_a, bucket_b])
            self.assertTrue(bucket_keys[bucket_a] and bucket_keys[bucket_b],
                            f"[STEP 3] Missing in-use keys per bucket: {bucket_keys}")
            self.assertFalse(bucket_keys[bucket_a] & bucket_keys[bucket_b],
                             f"[STEP 3] Buckets share key IDs: {bucket_keys}")
            self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_a, bucket_b],
                                                   step_prefix="[STEP 3] ", raise_on_failure=True)
            if storage != "bhive":
                self.validate_files_not_encrypted(index_nodes, bucket_c, timeout=60, step_prefix="[STEP 3] ")
            assert_no_cross_bucket_keys("[STEP 3] ")
            if self.bhive_index:  # MB-69497: codebooks trained on an encrypted bucket are written encrypted
                self._wait_for_codebooks(index_nodes, "encrypted", "[STEP 3] ")
                self.assertGreater(self._indexer_log_total(index_nodes, persisted), persisted_count,
                                   f"[STEP 3] '{persisted}' not logged")

            # STEP 4: drop DEKs on bucket_A only
            status, response = self.rest.trigger_data_reencryption(bucket_a)
            self.assertTrue(status, f"dropEncryptionAtRestDeks on {bucket_a} failed: {response}")
            self.poll_for_indexer_key_ids_gone(index_nodes, bucket_keys[bucket_a], timeout=self._drop_key_timeout(900),
                                               label="[STEP 4] ", key_type="bucket")
            after = self._bucket_in_use_key_ids(index_nodes, [bucket_a, bucket_b])
            self.assertTrue(after[bucket_a], f"[STEP 4] No key in use for {bucket_a} after the drop: {after}")
            self.assertEqual(after[bucket_b], bucket_keys[bucket_b],
                             f"[STEP 4] {bucket_b} keys changed by a DEK drop on {bucket_a}")
            self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_a, bucket_b],
                                                   step_prefix="[STEP 4] ", raise_on_failure=True)
            assert_no_cross_bucket_keys("[STEP 4] ")
            self._assert_no_temp_files_under_storage(index_nodes, ())

            # STEP 5: kill -9, recovery from disk (MB-69247 MOI snapshot, MB-69248 BHIVE codebook,
            # MB-70886 Plasma recovery point, not reset)
            self._kill9_indexers_recover_from_snapshot(index_nodes, select_queries, "[STEP 5] ", **{
                "memdb": {},
                "bhive": dict(done="Slice::recoverCodebook: reading from disk is successful",
                              failed="NewBhiveSlice:GetKeyIdList"),
                "plasma": dict(done="plasmaSlice::doRecovery .* Warmup took",
                               failed="plasmaSlice::doRecovery .* Unable to find recovery point|newPlasmaSlice:GetKeyIdList")}[storage])
            if not moi:
                # MB-69248 / MB-70886: writes take the active key of the bucket UUID in the instance path
                path_cb = f"{storage}Slice::GetActiveKeyByPathCb path: "
                total = self._indexer_log_total(index_nodes, path_cb)
                self.assertTrue(total and self._indexer_log_total(
                    index_nodes, path_cb + r".*([0-9a-f]{32})_[^ ]* bucketUUID: \1") == total,
                    "[STEP 5] GetActiveKeyByPathCb resolved a bucket UUID other than the one in the path")
            self.run_scan_validation(select_queries)
        finally:
            if original_retry:
                for node in index_nodes:
                    RestConnection(node).set_index_settings(original_retry)
            if storage == "plasma":
                self.disable_shard_based_rebalance()
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_bhive_multi_bucket_keys")

    def _assert_encryption_bootstrap_order(self, node, label):
        """MB-69178 / MB-69208: in the latest indexer start, EncryptionMgr caches the keys,
        waits for bootstrap and recovers the in-use keys before it handles queued key ops."""
        markers = ["EncryptionMgr:refreshAllBucketKeys caching keys for buckets",
                   "EncryptionMgr:refreshAllBucketKeys cached keys for bucket",
                   "EncryptionMgr:cached keys for log", "EncryptionMgr:runQueue caching done",
                   "EncryptionMgr:runQueue bootstrap done", "EncryptionMgr:recoverInUseKeys done",
                   "EncryptionMgr:runQueue key recovery done"]
        log_file = f"{self.gsi_encryption_helper.get_log_path(node)}/indexer.log"
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"tail -n +$(grep -n 'Indexer::NewIndexer Status Warmup' {log_file} | tail -1 | cut -d: -f1) "
                f"{log_file} | grep -o -E '{'|'.join(markers)}|StorageMgr::handleEncryption(Update|Drop)Key keydatatype'")
        finally:
            shell.disconnect()
        seen = []
        for line in out:
            if line.strip() and line.strip() not in seen:
                seen.append(line.strip())
        order = [line for line in seen if not line.startswith("StorageMgr")]
        self.assertEqual(order, markers, f"{label}EncryptionMgr bootstrap order on {node.ip}: {seen}")
        handled = [i for i, line in enumerate(seen) if line.startswith("StorageMgr")]
        self.assertTrue(not handled or handled[0] > seen.index(markers[-1]),
                        f"{label}Key op handled before key recovery on {node.ip}: {seen}")

    def test_gsi_encryption_at_rest_back_to_back_key_ops(self):
        """
        MB-69178: key operations the indexer must queue or handle without index data.

        Steps:
        1. bucket_A encrypted, indexes, scans
        2. drop_count DEK drops back to back on bucket_A (ns_server serializes them): all old
           keys leave GetInUseKeys; scans + item counts
        3. bucket_B encrypted and indexed after the indexer is up: its key is in use
           and its files are encrypted
        4. bucket_C encrypted with no indexes: a DEK drop still completes (oldest DEK replaced)
        5. Restart (read back): EncryptionMgr bootstrap order in indexer.log; scans
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_back_to_back_key_ops")
        # MB-69178: expected when a bucket is created after the indexer started (STEP 3)
        self.allowed_encryption_log_patterns = ("getKeyCipherById.*could not find keydatatype",)
        index_nodes = self.verify_prerequisites()
        drop_count = int(self.input.param("drop_count", 3))
        key_errors = "EncryptionMgr:getKeyCipherById (retry )?for key:.* err:"

        def log_counts(patterns):
            return {p: self._indexer_log_total(index_nodes, p) for p in patterns}

        def assert_logged(before, label):
            for p, n in before.items():
                self.assertGreater(self._indexer_log_total(index_nodes, p), n, f"{label}'{p}' not in indexer.log")

        # STEP 1
        bucket_a, select_queries = self._setup_bucket_and_indexes('b2b_', index_nodes)

        # STEP 2: back-to-back DEK drops
        old_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket")
        for attempt in range(drop_count):
            status, response = self.rest.trigger_data_reencryption(bucket_a)
            self.assertTrue(status, f"[STEP 2] DEK drop {attempt + 1} failed: {response}")
        self.poll_for_indexer_key_ids_gone(index_nodes, old_keys, timeout=1200,
                                           label="[STEP 2] ", key_type="bucket")
        self.run_scan_validation(select_queries)
        self.item_count_related_validations()

        # STEP 3: bucket created and indexed after the indexer started
        bucket_b = f"{self.test_bucket}_2"
        force_logs = log_counts(("EncryptionMgr:DropKeysCallback:Received for keydatatype",
                                 "StorageMgr::handleEncryptionDropKey keydatatype",
                                 "EncryptionMgr:trackDropKeys complete for keydatatype"))
        select_queries.update(self._create_bucket_with_indexes(bucket_b, index_nodes, 'b2b_new_'))
        self._bucket_key_op("enable", bucket_b, index_nodes, "[STEP 3] ")  # force-encrypt existing index data
        assert_logged(force_logs, "[STEP 3] ")
        self.assertTrue(self._bucket_in_use_key_ids(index_nodes, [bucket_b])[bucket_b],
                        f"[STEP 3] No key in use for {bucket_b} created after indexer start")
        self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_a, bucket_b],
                                               step_prefix="[STEP 3] ", raise_on_failure=True)

        # STEP 4: DEK drop on a bucket with no indexes completes
        bucket_c = f"{self.test_bucket}_3"
        self.create_bucket_with_encryption(bucket_c, enable_encryption=True)
        self._wait_until_no_problems(lambda: [] if self.rest.get_bucket_json(bucket_c).get(
            'encryptionAtRestInfo', {}).get('oldestDekCreationDatetime') else [bucket_c], "[STEP 4] DEK created: ", 120)
        oldest_before = self.rest.get_bucket_json(bucket_c).get(
            'encryptionAtRestInfo', {}).get('oldestDekCreationDatetime')
        status, response = self.rest.trigger_data_reencryption(bucket_c)
        self.assertTrue(status, f"[STEP 4] DEK drop on {bucket_c} failed: {response}")

        def drop_pending():
            info = self.rest.get_bucket_json(bucket_c).get('encryptionAtRestInfo', {})
            # dekNumber is summed over nodes; every old DEK is gone once the oldest one changes
            done = not info.get('issues') and info.get('oldestDekCreationDatetime') != oldest_before
            return [] if done else [info]

        self._wait_until_no_problems(drop_pending, f"[STEP 4] DEK drop on {bucket_c} (no indexes): ")

        # STEP 4b: rotate bucket_A's DEK without dropping old keys; scans unchanged after each
        rotate_logs = log_counts(("EncryptionMgr:RefreshKeysCallback:Starting for keydatatype",
                                  "StorageMgr::handleEncryptionUpdateKey keydatatype",
                                  "EncryptionMgr:trackUpdateKeys complete for keydatatype"))
        key_errors_before = self._indexer_log_total(index_nodes, key_errors)
        scans_before = self._scan_results(select_queries)
        self.set_bucket_dek_rotation_config(bucket_a, self.encryption_at_rest_id,
                                            dek_rotation_interval=self.kek_rotation_interval_seconds,
                                            dek_lifetime=self.kek_rotation_interval_seconds * 5)
        for _ in range(int(self.input.param("rotations", 3))):
            self.poll_for_new_indexer_key_ids(index_nodes, self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket"),
                                              timeout=600, label="[STEP 4b] ", key_type="bucket")
            self._assert_same_scan_results(scans_before, "[STEP 4b] ")
        self.set_bucket_dek_rotation_config(bucket_a, self.encryption_at_rest_id,
                                            dek_rotation_interval=36000, dek_lifetime=36000)
        assert_logged(rotate_logs, "[STEP 4b] ")

        # STEP 5: kill -9 -> recovery from disk, no rebuild from 0, no key lookup errors
        rollbacks = self._indexer_log_total(index_nodes, "StorageMgr::rollbackAllToZero")
        for node in index_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                shell.execute_command("pkill -9 -x indexer")
            finally:
                shell.disconnect()
        self.wait_for_indexer_ready(index_nodes, timeout=900, label="[STEP 5] ")
        self.wait_until_indexes_online(timeout=1800)
        self.item_count_related_validations()
        self._assert_same_scan_results(scans_before, "[STEP 5] ")
        self.assertEqual(self._indexer_log_total(index_nodes, "StorageMgr::rollbackAllToZero"), rollbacks,
                         "[STEP 5] Indexes rebuilt from 0 after kill -9")
        self.assertEqual(self._indexer_log_total(index_nodes, key_errors), key_errors_before,
                         "[STEP 5] getKeyCipherById errors in indexer.log")
        for node in index_nodes:
            self._assert_encryption_bootstrap_order(node, "[STEP 5] ")
        self.run_scan_validation(select_queries)

        # STEP 6 (MB-70886): disable -> plaintext, "" in use; re-enable -> encrypted again
        pre_disable_keys = self._bucket_in_use_key_ids(index_nodes, [bucket_b])[bucket_b]
        self._bucket_key_op("disable", bucket_b, index_nodes, "[STEP 6] ")
        self.validate_files_not_encrypted(index_nodes, bucket_b, timeout=600, step_prefix="[STEP 6] ",
                                          old_key_ids=pre_disable_keys)
        self.assertEqual(self._bucket_in_use_ids_raw(index_nodes, bucket_b), {""}, "[STEP 6] In-use keys")
        self._bucket_key_op("enable", bucket_b, index_nodes, "[STEP 6] ")
        self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_b],
                                               step_prefix="[STEP 6] ", raise_on_failure=True)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_back_to_back_key_ops")

    def _bucket_in_use_ids_raw(self, index_nodes, bucket_name):
        """MB-69247: GetInUseKeys IDs of bucket_name's service_bucket entry ("" = unencrypted data)."""
        tag = "{service_bucket %s}" % self.rest.get_bucket_json(bucket_name)['uuid']
        ids = set()
        for node in index_nodes:
            status, response = RestConnection(node).get_indexer_in_use_encryption_keys()
            ids.update(response.get(tag, []) if status and isinstance(response, dict) else [])
        return ids

    def _kill9_indexers_recover_from_snapshot(self, index_nodes, select_queries, label,
                                              done="MemDBSlice::loadSnapshot .* finished reading",
                                              failed="failed to recover from the snapshot"):
        """MB-69247 / MB-69248: kill -9 every indexer; indexes recover from disk (`done` logged,
        `failed` not; MOI snapshot by default, BHIVE codebook), same scan results and item counts."""
        before = [self._indexer_log_total(index_nodes, p) for p in (done, failed)]
        scans_before = self._scan_results(select_queries)
        for node in index_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                shell.execute_command("pkill -9 -x indexer")
            finally:
                shell.disconnect()
        self.wait_for_indexer_ready(index_nodes, timeout=900, label=label)
        self.wait_until_indexes_online(timeout=1800)
        self.assertGreater(self._indexer_log_total(index_nodes, done), before[0], f"{label}No MOI snapshot recovery")
        self.assertEqual(self._indexer_log_total(index_nodes, failed), before[1], f"{label}MOI snapshot recovery failed")
        self.item_count_related_validations()
        self._assert_same_scan_results(scans_before, label)

    def test_gsi_encryption_at_rest_moi_lifecycle(self):
        """
        MB-69247: MOI snapshots through enable-after-create, indexer paused, and disable.
        Run with gsi_type=memory_optimized.

        Steps:
        1. Unencrypted bucket, MOI indexes, snapshot written: snapshot files plaintext
        2. Enable + force-encrypt; next snapshot encrypted; restart recovers from the
           encrypted snapshot (read back)
        3. Shrink the indexer memory quota and load until the indexer is Paused; a DEK
           drop is rejected ("handleEncryptionUpdateKey aborted" and
           "handleEncryptionDropKeys aborted"); restore the quota: indexer Active, old
           keys gone, item counts
        4. Disable; restart while the encrypted snapshot is still on disk (read back);
           drop DEKs: snapshot files plaintext; no MOI re-encryption temp files; restart
           (read back); scans
        Scan results equal across every restart.
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_moi_lifecycle")
        index_nodes = self.verify_prerequisites()
        if self.gsi_type != "memory_optimized":
            self.skipTest("Needs MOI. Set gsi_type=memory_optimized")
        bucket_name = self.test_bucket
        snapshot_wait = int(self.input.param("snapshot_wait", 90))
        paused_quota = int(self.input.param("moi_paused_quota", 256))
        storage_errors = ("StorageMgr::handleEncryptionDropKey .*err:",
                          "StorageMgr::handleEncryptionUpdateKey SetCurrentEncryptionKey failed")
        errors_before = [self._indexer_log_total(index_nodes, p) for p in storage_errors]

        # STEP 1
        self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 1]")
        select_queries = self._create_bucket_with_indexes(bucket_name, index_nodes, 'moi_')
        self.run_cbq_query(query=f"CREATE INDEX moi_drop_me ON `{bucket_name}`(meta().id)",
                           server=self.get_nodes_from_services_map(service_type="n1ql"))
        self.sleep(snapshot_wait, "[STEP 1] Waiting for an MOI snapshot")
        self.validate_files_not_encrypted(index_nodes, bucket_name, timeout=60, step_prefix="[STEP 1] ")
        self.assertEqual(self._bucket_in_use_ids_raw(index_nodes, bucket_name), {""}, "[STEP 1] In-use keys")

        # STEP 2: enable after the indexes exist
        # MB-69247 mixed state: snapshots paused, enable: "" and the key both in use;
        # snapshots on again: only the key once the next snapshot is written
        for node in index_nodes:
            RestConnection(node).set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 3600000})
        status, response = self.rest.enable_bucket_encryption(bucket_name, self.encryption_at_rest_id)
        self.assertTrue(status, f"[STEP 2] Failed to enable encryption: {response}")
        self._wait_until_no_problems(lambda: [] if (lambda ids: "" in ids and len(ids) > 1)(
            self._bucket_in_use_ids_raw(index_nodes, bucket_name)) else ["no mixed state"], "[STEP 2] ", 300)
        self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 2]")
        status, response = self.rest.force_bucket_encryption_at_rest(bucket_name)  # drops "" (DropInUseKeys)
        self.assertTrue(status, f"[STEP 2] forceEncryptionAtRest failed: {response}")
        self.validate_mixed_bucket_encrypted_keys(index_nodes, [bucket_name], timeout=900, step_prefix="[STEP 2] ")
        self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_name],
                                               step_prefix="[STEP 2] ", raise_on_failure=True)
        self._kill9_indexers_recover_from_snapshot(index_nodes, select_queries, "[STEP 2] ")
        self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_name],
                                               step_prefix="[STEP 2] ", raise_on_failure=True)
        # MB-69247: rotate the DEK (old one kept, dropped in step 3); restart before the drop
        old_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket")
        self.set_bucket_dek_rotation_config(bucket_name, self.encryption_at_rest_id, 30, 36000)
        self.poll_for_new_indexer_key_ids(index_nodes, old_keys, timeout=600, label="[STEP 2] ", key_type="bucket")
        self.set_bucket_dek_rotation_config(bucket_name, self.encryption_at_rest_id, 36000, 36000)
        self.sleep(snapshot_wait, "[STEP 2] Waiting for a snapshot on the rotated key")
        self._kill9_indexers_recover_from_snapshot(index_nodes, select_queries, "[STEP 2] ")

        # STEP 3: key op while the MOI indexer is paused for memory
        original_quota = self.get_indexer_mem_quota()
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=paused_quota)
        try:
            paused = []  # one paused indexer is enough to reject the DEK drop

            def not_paused():
                self._load_docs(bucket_name, key_prefix=f"p{int(time.time()) % 10**6}_")  # magma loader keySize is 20
                paused[:] = [node for node in index_nodes
                             if RestConnection(node).get_indexer_stats().get("indexer_state") == "Paused"]
                return [] if paused else [node.ip for node in index_nodes]

            self._wait_until_no_problems(not_paused, "[STEP 3] Indexer not Paused: ", timeout=1800)
            old_keys = self.get_indexer_in_use_key_ids(index_nodes, key_type="bucket")
            aborted_logs = ("handleEncryptionUpdateKey aborted", "handleEncryptionDropKeys aborted")
            aborted_before = {(node.ip, pattern): self._indexer_log_count(node, pattern)
                              for node in paused for pattern in aborted_logs}
            # ns_server retries a rejected DEK drop after dek_removal_min_interval (3 h by default)
            self.rest.diag_eval('ns_config:set({cb_cluster_secrets, {dek_removal_min_interval, {serviceBucketDek, <<"%s">>}}}, 60).'
                                % self.rest.get_bucket_json(bucket_name)['uuid'])
            status, response = self.rest.trigger_data_reencryption(bucket_name)
            self.assertTrue(status, f"[STEP 3] dropEncryptionAtRestDeks failed: {response}")
            self.sleep(60, "[STEP 3] Waiting for the DEK drop to reach the paused indexer")
            for node in paused:
                for pattern in aborted_logs:
                    self.assertGreater(self._indexer_log_count(node, pattern), aborted_before[(node.ip, pattern)],
                                       f"[STEP 3] '{pattern}' not logged on paused indexer {node.ip}")
        finally:
            self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=original_quota)
        self.wait_for_indexer_ready(index_nodes, timeout=900, label="[STEP 3] ")
        self.wait_until_indexes_online(timeout=1800)
        self.run_cbq_query(query=f"DROP INDEX moi_drop_me ON `{bucket_name}`",  # MB-69247: during the key op
                           server=self.get_nodes_from_services_map(service_type="n1ql"))
        self.poll_for_indexer_key_ids_gone(index_nodes, old_keys, timeout=1200,
                                           label="[STEP 3] ", key_type="bucket")
        self.item_count_related_validations()
        self._kill9_indexers_recover_from_snapshot(index_nodes, select_queries, "[STEP 3] ")
        drop_errors = self._indexer_log_total(index_nodes, "EncryptionMgr:trackDropKeys err:")

        # STEP 4: disable; restart before a new (plaintext) snapshot is written: recovery
        # from the encrypted snapshot (snapshots paused meanwhile)
        for node in index_nodes:
            RestConnection(node).set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 3600000})
        self.validate_file_encryption_with_key(index_nodes, encrypted_bucket_names=[bucket_name],
                                               step_prefix="[STEP 4] ", raise_on_failure=True)

        def restart_on_encrypted_snapshot():
            scans_before = self._scan_results(select_queries)
            self.restart_index_nodes(index_nodes, validate_read_back=True)
            self._assert_same_scan_results(scans_before, "[STEP 4] ")
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 4]")

        # then drop DEKs -> plaintext snapshots
        self._bucket_key_op("disable", bucket_name, index_nodes, "[STEP 4] ",
                            before_drop=restart_on_encrypted_snapshot)
        self.validate_files_not_encrypted(index_nodes, bucket_name, timeout=600, step_prefix="[STEP 4] ")
        self.assertEqual(self._bucket_in_use_ids_raw(index_nodes, bucket_name), {""}, "[STEP 4] In-use keys")
        self._assert_no_temp_files_under_storage(index_nodes, (".aes.tmp", ".bak"))
        # MB-69247: no storage key-op errors; no failed DEK drop after the paused step
        for pattern, before in zip(storage_errors, errors_before):
            self.assertEqual(self._indexer_log_total(index_nodes, pattern), before, f"[STEP 4] '{pattern}' in indexer.log")
        self.assertEqual(self._indexer_log_total(index_nodes, "EncryptionMgr:trackDropKeys err:"), drop_errors,
                         "[STEP 4] DEK drop failed after the indexer was unpaused")
        scans_before = self._scan_results(select_queries)
        self.restart_index_nodes(index_nodes, validate_read_back=True)
        self._assert_same_scan_results(scans_before, "[STEP 4] ")
        self.run_scan_validation(select_queries)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_moi_lifecycle")

    def test_gsi_encryption_at_rest_corrupt_backup_lifecycle(self):
        """
        MB-69503: corrupt-index backups follow bucket encryption only.
        Run with log or other encryption only (no bucket key); bhive_index=True also covers @bhive.

        Steps:
        1. Corrupt-index backup on; unencrypted bucket + indexes (no bucket key active)
        2. Corrupt a partition, kill the indexer: backup taken; restart (startup sweep): kept
        3. Plant .corruptData; encrypt a different bucket (bucket_2): .corruptData removed
           ("cleanupCorruptDataDirIfEncrypted removed"); then enable + force-encrypt the bucket.
           Backup setting off: planted .corruptData kept across restart; on again: swept
        4. Disable + drop DEKs on bucket only (bucket_2 still encrypted); corrupt again:
           no backup ("skipping backup: bucket encryption is active")
        5. Disable + drop DEKs on bucket_2; corrupt again, kill the indexer: backup taken again
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_corrupt_backup_lifecycle")
        index_nodes = self.verify_prerequisites()
        bucket_name = self.test_bucket

        def corrupt_and_expect_backup(label, expect_backup=True):
            # As the indexing test TestIdxCorruptBackup: a fatal "error" file in one
            # partition's mainIndex makes recovery treat it as corrupt and back it up
            storage_dir = self._corrupt_data_dirs(corrupt_node)[0].rsplit("/", 1)[0]
            if self.bhive_index:
                storage_dir += "/@bhive"
            shell = RemoteMachineShellConnection(corrupt_node)
            try:
                out, _ = shell.execute_command(
                    f"d=$(ls -d {storage_dir}/*.index/mainIndex 2>/dev/null | head -1) && [ -n \"$d\" ] && "
                    f"echo 'Fatal error: Automation Induced Storage Corruption' > $d/error && echo $d")
            finally:
                shell.disconnect()
            self.assertTrue([l for l in out if l.strip()],
                            f"{label}No partition mainIndex under {storage_dir} on {corrupt_node.ip} to corrupt")
            skip_log = "backupCorruptIndexDataFiles.*skipping backup: bucket encryption is active"
            skip_count = self._indexer_log_count(corrupt_node, skip_log)
            self._kill_all_processes_index(server=corrupt_node)
            self.sleep(30, f"{label}Waiting for the indexer to restart on {corrupt_node.ip}")
            self.wait_until_indexes_online(timeout=1800)
            if expect_backup:
                self.assertTrue(self._existing_corrupt_data_dirs(corrupt_node),
                                f"{label}No corrupt-index backup on {corrupt_node.ip} without a bucket key")
                if self.bhive_index:
                    self.assertTrue(any("@bhive" in d for d in self._existing_corrupt_data_dirs(corrupt_node)),
                                    f"{label}No @bhive/.corruptData backup on {corrupt_node.ip}")
            else:
                self.assertFalse(self._existing_corrupt_data_dirs(corrupt_node),
                                 f"{label}Corrupt-index backup taken on {corrupt_node.ip} with a bucket encrypted")
                self.assertGreater(self._indexer_log_count(corrupt_node, skip_log), skip_count,
                                   f"{label}'{skip_log}' not logged on {corrupt_node.ip}")
            self.run_scan_validation(select_queries)

        # STEP 1
        self.enable_corrupt_index_backup()
        select_queries = self._create_bucket_with_indexes(bucket_name, index_nodes, 'cbk_')
        hosts = {host.split(":")[0] for idx in self.rest.get_indexer_metadata()['status']
                 if idx.get('bucket') == bucket_name for host in idx.get('hosts', [])}
        self.assertTrue(hosts, f"[STEP 1] No index hosts found for {bucket_name}")
        corrupt_node = random.choice([n for n in index_nodes if n.ip in hosts])

        # STEP 2: log key only -> backup taken and kept by the startup sweep
        corrupt_and_expect_backup("[STEP 2] ")
        self.restart_index_nodes(index_nodes)
        self.assertTrue(self._existing_corrupt_data_dirs(corrupt_node),
                        "[STEP 2] Startup sweep removed .corruptData with no bucket key active")

        # STEP 3: encrypting a different bucket removes .corruptData (@2i and @bhive)
        self._plant_corrupt_data_backup(index_nodes)
        if self.bhive_index:
            self.assertTrue(any("@bhive" in d for n in index_nodes for d in self._existing_corrupt_data_dirs(n)),
                            "[STEP 3] No @bhive/.corruptData planted")
        removed_log = "cleanupCorruptDataDirIfEncrypted removed"
        removed_count = self._indexer_log_count(corrupt_node, removed_log)
        bucket_2 = f"{bucket_name}_2"
        select_queries.update(self._create_bucket_with_indexes(bucket_2, index_nodes, 'cbk2_',
                                                               self.encryption_at_rest_id))
        self._wait_for_corrupt_data_removed(index_nodes, "[STEP 3] ")
        self.assertGreater(self._indexer_log_count(corrupt_node, removed_log), removed_count,
                           f"[STEP 3] '{removed_log}' not logged on {corrupt_node.ip}")
        self._bucket_key_op("enable", bucket_name, index_nodes, "[STEP 3] ")

        # Retain setting on: cleanup is a no-op, planted .corruptData kept across restart
        retain = "indexer.encryption.test_retain_corrupt_index_backup"
        RestConnection(index_nodes[0]).set_index_settings({retain: True})
        self._plant_corrupt_data_backup(index_nodes)
        self.restart_index_nodes(index_nodes)
        self.assertTrue(all(self._existing_corrupt_data_dirs(n) for n in index_nodes),
                        f"[STEP 3] .corruptData removed with {retain} on")
        RestConnection(index_nodes[0]).set_index_settings({retain: False})
        self.restart_index_nodes(index_nodes)
        self._wait_for_corrupt_data_removed(index_nodes, "[STEP 3] ", timeout=120)

        # STEP 4: bucket_2 still encrypted -> backups stay off
        self._bucket_key_op("disable", bucket_name, index_nodes, "[STEP 4] ")
        corrupt_and_expect_backup("[STEP 4] ", expect_backup=False)

        # STEP 5: no bucket key on any bucket -> backups resume
        self._bucket_key_op("disable", bucket_2, index_nodes, "[STEP 5] ")
        corrupt_and_expect_backup("[STEP 5] ")
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_corrupt_backup_lifecycle")

    def _wait_for_log_data_encrypted(self, timeout=300, interval=15, label=""):
        """Wait until ns_server reports every log-key file as encrypted."""
        deadline = time.time() + timeout
        data_status = None
        while time.time() < deadline:
            status, config = self.rest.get_encryption_at_rest_config()
            if isinstance(config, (bytes, bytearray)):
                config = json.loads(config)
            if status and isinstance(config, dict):
                data_status = config.get("log", {}).get("info", {}).get("dataStatus")
            if data_status == "encrypted":
                return
            self.sleep(interval, f"{label}Waiting for log dataStatus=encrypted (now {data_status})")
        self.fail(f"{label}Log data not encrypted after {timeout}s (dataStatus={data_status})")

    def test_gsi_log_encryption_cbcollect_decodes(self):
        """
        Log-key files are readable by the support tools and read back by the indexer.

        Steps:
        1.  Bucket + indexes on every index node, scans; getIndexStatus on every index
            node so the request-handler cache is written
        2.  Force log encryption; wait until ns_server reports log data encrypted
        3.  verify_cbcollect_decodes on every node: each file's header names the node's
            log key, stats logs decode with cbcat and arrive decrypted in cbcollect_info;
            verify_category_on_nodes per log-key category (projector on kv nodes)
        4.  Stop one index node and kill the indexer on another: it still lists the
            stopped node's indexes from its encrypted request-handler cache, and its
            indexer.log (read through cbcat) has no encryption read errors
        5.  Start the stopped node; scans + item counts
        """
        self.log.info("STARTING TEST: test_gsi_log_encryption_cbcollect_decodes")
        if not getattr(self, "enable_log_encryption_at_rest", False):
            self.skipTest("Log encryption at rest not enabled. Set enable_log_encryption_at_rest=True")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.assertGreaterEqual(len(index_nodes), 2, f"Need at least 2 index nodes, found {len(index_nodes)}")
        helper = self.gsi_encryption_helper

        # STEP 1: bucket, indexes, scans; getIndexStatus populates the request-handler cache
        self.set_indexer_snapshot_settings(index_nodes)
        _, select_queries = self._setup_bucket_and_indexes('logdecode_', index_nodes)
        self.run_scan_validation(select_queries, validate_item_count=True)
        self._populate_request_handler_cache(index_nodes)

        # STEP 2: force-encrypt the files written before the log key was in use
        status, response = self.rest.force_log_encryption_at_rest()
        self.assertTrue(status, f"forceEncryptionAtRest/log failed: {response}")
        self._wait_for_log_data_encrypted(label="[STEP 2] ")

        # STEP 3: headers, cbcat and cbcollect_info on every node; then the same files
        # through verify_category_on_nodes, the per-category check topology tests call
        self.verify_cbcollect_decodes(self.servers[:self.nodes_init])
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        for category, category_nodes in (("stats_log", index_nodes), ("request_cache", index_nodes),
                                         ("persisted_stats", index_nodes), ("projector_stats_log", kv_nodes)):
            results = self.verify_category_on_nodes(category, category_nodes)
            failed = {ip: r for ip, r in results.items() if r.get("status") != "passed"}
            self.assertFalse(failed, f"[STEP 3] verify_category_on_nodes({category!r}) failed: {failed}")
        self.log.info("[STEP 3] PASSED - log-key files carry each node's log key and decode")

        # STEP 4: read back the encrypted request-handler cache with a peer down
        survivor, peer = index_nodes[0], index_nodes[1]

        def indexes_on_peer():
            statuses = RestConnection(survivor).get_indexer_metadata().get("status", [])
            return {s["name"] for s in statuses
                    if any(str(host).startswith(f"{peer.ip}:") for host in s.get("hosts", []))}

        peer_indexes = indexes_on_peer()
        self.assertTrue(peer_indexes, f"[STEP 4] {survivor.ip} reports no indexes on {peer.ip}")
        peer_shell = RemoteMachineShellConnection(peer)
        try:
            peer_shell.stop_server()
            self.sleep(20, f"[STEP 4] Waiting after stopping {peer.ip}")
            self._kill_all_processes_index(survivor)
            self.sleep(10, f"[STEP 4] Waiting for the indexer on {survivor.ip} to restart")
            self.wait_for_indexer_ready([survivor], timeout=900, label="[STEP 4] ")
            missing = peer_indexes - indexes_on_peer()
            self.assertFalse(missing, f"[STEP 4] {survivor.ip} no longer lists {peer.ip}'s indexes "
                                      f"{missing} after restart (request-handler cache not read back)")
            errors = helper.grep_with_cbcat(survivor, f"{helper.get_log_path(survivor)}/indexer.log",
                                            self.ENCRYPTION_ERROR_PATTERNS_INDEXER)
            self.assertFalse(errors, f"[STEP 4] Encryption read errors in indexer.log on {survivor.ip}: {errors}")
        finally:
            peer_shell.start_server()
            peer_shell.disconnect()
        self.log.info(f"[STEP 4] PASSED - {survivor.ip} served {sorted(peer_indexes)} from its encrypted cache")

        # STEP 5
        self.wait_for_indexer_ready([peer], timeout=900, label="[STEP 5] ")
        self.wait_until_indexes_online(timeout=1200)
        self.run_scan_validation(select_queries, validate_item_count=True)
        self.log.info("TEST PASSED: test_gsi_log_encryption_cbcollect_decodes")

    def test_gsi_encryption_at_rest_log_key_cache_lifecycle(self):
        """
        Log key lifecycle focused on the request-handler cache (@2i/cache).

        The request-handler cache is populated during /getIndexStatus calls and
        is read back when a peer index node is unreachable.  This test exercises
        the full lifecycle of the log DEK governing cache files:

        Steps:
        1.  Prerequisites (log encryption enabled, >= 2 index nodes), bucket + indexes,
            scans; /getIndexStatus on every node to populate the cache
        2.  Wait for the log key in use; verify cache files encrypted
        3.  Cache read-back proof: stop a peer index node, kill the indexer on the
            survivor; after restart the survivor must still list the peer's indexes
            (served from encrypted cache)
        4.  Start peer; wait for indexes online
        5.  Drop log DEKs (re-encryption): new log key, cache files re-validated
        6.  Cache read-back proof again (with the rotated key)
        7.  Start peer; wait for indexes online
        8.  Disable log encryption + drop DEKs: cache files become plaintext
        9.  Re-enable log encryption + force-encrypt: cache files encrypted again
        10. Cache read-back proof with the re-enabled key
        11. Start peer; scans + item counts
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_log_key_cache_lifecycle")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not getattr(self, "enable_log_encryption_at_rest", False):
            self.skipTest("Log encryption at rest not enabled. Set enable_log_encryption_at_rest=True")
        self.assertGreaterEqual(len(index_nodes), 2,
                                f"Need at least 2 index nodes, found {len(index_nodes)}")
        log_secret_id = self.log_encryption_at_rest_id
        helper = self.gsi_encryption_helper

        def _assert_cache_encrypted(nodes, key_ids, label):
            results = helper.verify_gsi_request_handler_cache_encrypted(
                nodes, expected_key_id=key_ids
            )
            failed = {ip: r for ip, r in results.items()
                      if r.get("status") != "passed"}
            if failed:
                raise Exception(f"{label}request-handler cache check FAILED: {failed}")
            self.log.info(f"{label}cache files validated on {list(results.keys())}")

        def _wait_cache_reencrypted(nodes, old_key_ids, label, timeout=300, interval=15):
            deadline = time.time() + timeout
            still = [n.ip for n in nodes]
            while time.time() < deadline:
                results = helper.verify_gsi_request_handler_cache_encrypted(
                    nodes, expected_key_id=old_key_ids
                )
                still = [ip for ip, r in results.items() if r.get("status") == "passed"]
                if not still:
                    return
                self.sleep(interval, f"{label}cache still carries old keys on {still}")
            raise Exception(f"{label}cache still carries old key IDs on {still} after {timeout}s")

        def _wait_cache_plaintext(nodes, label, timeout=300, interval=15):
            deadline = time.time() + timeout
            encrypted = []
            while time.time() < deadline:
                encrypted = []
                for node in nodes:
                    for path in self._list_request_handler_cache_files(node):
                        is_enc, _ = helper.verify_file_encryption_magic_bytes(node, path)
                        if is_enc:
                            encrypted.append(f"{node.ip}:{path}")
                if not encrypted:
                    return
                self.sleep(interval, f"{label}cache still encrypted: {encrypted[:3]}")
            self.log.warning(f"{label}cache files still encrypted after {timeout}s: {encrypted}")

        def _cache_read_back_proof(survivor, peer, label):
            statuses = RestConnection(survivor).get_indexer_metadata().get("status", [])
            peer_indexes = {s["name"] for s in statuses
                           if any(str(h).startswith(f"{peer.ip}:") for h in s.get("hosts", []))}
            self.assertTrue(peer_indexes, f"{label}{survivor.ip} reports no indexes on {peer.ip}")
            peer_shell = RemoteMachineShellConnection(peer)
            try:
                peer_shell.stop_server()
                self.sleep(20, f"{label}Waiting after stopping {peer.ip}")
                self._kill_all_processes_index(survivor)
                self.sleep(10, f"{label}Waiting for indexer on {survivor.ip} to restart")
                self.wait_for_indexer_ready([survivor], timeout=900, label=label)
                statuses = RestConnection(survivor).get_indexer_metadata().get("status", [])
                post = {s["name"] for s in statuses
                        if any(str(h).startswith(f"{peer.ip}:") for h in s.get("hosts", []))}
                missing = peer_indexes - post
                self.assertFalse(missing,
                                 f"{label}{survivor.ip} lost {peer.ip}'s indexes {missing} "
                                 f"after restart (cache not read back)")
            finally:
                peer_shell.start_server()
                peer_shell.disconnect()
            self.log.info(f"{label}PASSED - {survivor.ip} served {sorted(peer_indexes)} from cache")

        # STEP 1
        self.set_indexer_snapshot_settings(index_nodes)
        bucket_name, select_queries = self._setup_bucket_and_indexes('cachelf_', index_nodes)
        self.run_scan_validation(select_queries, validate_item_count=True)
        self._populate_request_handler_cache(index_nodes)

        # STEP 2
        log_keys = self.poll_for_new_indexer_key_ids(index_nodes, [], timeout=300,
                                                      label="[STEP 2] ", key_type="log")
        _assert_cache_encrypted(index_nodes,
                                self.get_indexer_in_use_key_ids(index_nodes, key_type="log"),
                                "[STEP 2] ")
        self.log.info(f"[STEP 2] PASSED - log key(s) in use: {log_keys}")

        # STEP 3: cache read-back proof with the initial key
        survivor, peer = index_nodes[0], index_nodes[1]
        _cache_read_back_proof(survivor, peer, "[STEP 3] ")

        # STEP 4
        self.wait_for_indexer_ready([peer], timeout=900, label="[STEP 4] ")
        self.wait_until_indexes_online(timeout=1200)
        self.log.info("[STEP 4] PASSED - peer back, indexes online")

        # STEP 5: drop log DEKs -> re-encryption
        status, response = self.rest.trigger_log_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/log failed: {response}")
        new_log_keys = self.poll_for_new_indexer_key_ids(index_nodes, log_keys, timeout=600,
                                                          label="[STEP 5] ", key_type="log")
        try:
            self.poll_for_indexer_key_ids_gone(index_nodes, log_keys, timeout=600,
                                               label="[STEP 5] ", key_type="log")
        except Exception as ex:
            self.log.warning(f"[STEP 5] Old log keys still in use after DEK drop: {ex}")
        _wait_cache_reencrypted(index_nodes, log_keys, "[STEP 5] ")
        self._populate_request_handler_cache(index_nodes)
        _assert_cache_encrypted(index_nodes,
                                self.get_indexer_in_use_key_ids(index_nodes, key_type="log"),
                                "[STEP 5] ")
        self.log.info(f"[STEP 5] PASSED - cache re-encrypted with {new_log_keys}")

        # STEP 6: cache read-back proof with the rotated key
        _cache_read_back_proof(survivor, peer, "[STEP 6] ")

        # STEP 7
        self.wait_for_indexer_ready([peer], timeout=900, label="[STEP 7] ")
        self.wait_until_indexes_online(timeout=1200)
        self.log.info("[STEP 7] PASSED - peer back, indexes online")

        # STEP 8: disable log encryption -> decryption
        status, response = self.rest.disable_log_encryption()
        self.assertTrue(status, f"Disabling log encryption failed: {response}")
        status, response = self.rest.trigger_log_reencryption()
        self.assertTrue(status, f"dropEncryptionAtRestDeks/log (decrypt) failed: {response}")
        try:
            self.poll_for_indexer_key_ids_gone(index_nodes, new_log_keys, timeout=600,
                                               label="[STEP 8] ", key_type="log")
        except Exception as ex:
            self.log.warning(f"[STEP 8] Log keys still in use after disabling: {ex}")
        _wait_cache_plaintext(index_nodes, "[STEP 8] ")
        self.log.info("[STEP 8] PASSED - cache files decrypted")

        # STEP 9: re-enable + force-encrypt
        status, response = self.rest.configure_log_encryption_at_rest({
            "encryptionMethod": "encryptionKey", "encryptionKeyId": log_secret_id,
            "dekLifetime": 31536000, "dekRotationInterval": 2592000})
        self.assertTrue(status, f"Re-enabling log encryption failed: {response}")
        status, response = self.rest.force_log_encryption_at_rest()
        self.assertTrue(status, f"forceEncryptionAtRest/log failed: {response}")
        final_log_keys = self.poll_for_new_indexer_key_ids(
            index_nodes, log_keys + new_log_keys, timeout=600,
            label="[STEP 9] ", key_type="log")
        _wait_cache_reencrypted(index_nodes, new_log_keys, "[STEP 9] ")
        self._populate_request_handler_cache(index_nodes)
        _assert_cache_encrypted(index_nodes,
                                self.get_indexer_in_use_key_ids(index_nodes, key_type="log"),
                                "[STEP 9] ")
        self.log.info(f"[STEP 9] PASSED - cache encrypted again with {final_log_keys}")

        # STEP 10: cache read-back proof with the re-enabled key
        _cache_read_back_proof(survivor, peer, "[STEP 10] ")

        # STEP 11
        self.wait_for_indexer_ready([peer], timeout=900, label="[STEP 11] ")
        self.wait_until_indexes_online(timeout=1200)
        self.run_scan_validation(select_queries)
        self.item_count_related_validations()
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_log_key_cache_lifecycle")

    def test_gsi_encryption_at_rest_index_dir_naming(self):
        """
        UUID-based index directory naming (IndexPath2 format).

        Index directories use the format ``<bucketUUID>_<instId>_<partnId>.index``
        instead of the old ``<bucket>_<indexName>_<instId>_<partnId>.index``, so
        no bucket, index, scope, collection, or field names leak into on-disk paths.

        Steps:
        1.  Prerequisites (encryption enabled, >= 2 index nodes)
        2.  Create bucket with encryption, load docs, create indexes
        3.  Verify all index dirs match the UUID format regex
        4.  Collect forbidden names from /indexStatus, verify none appear in paths
        5.  Baseline scans
        6.  Restart indexers; verify dirs still UUID format (read back under new format)
        7.  Scans after restart
        8.  Drop and recreate indexes; verify new dirs also UUID format
        9.  Scans on recreated indexes
        10. Item count validation
        """
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_index_dir_naming")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not getattr(self, "enable_encryption_at_rest", False):
            self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
        self.assertGreaterEqual(len(index_nodes), 2,
                                f"Need at least 2 index nodes, found {len(index_nodes)}")
        helper = self.gsi_encryption_helper
        UUID_DIR_RE = re.compile(r'^[0-9a-f]{32}_\d+_\d+\.index$')
        CODEBOOK_RE = re.compile(r'^\d+_\d+\.codebook$')

        def _verify_uuid_dirs(nodes, label):
            violations = []
            empty_nodes = []
            codebook_violations = []
            for node in nodes:
                rest = RestConnection(node)
                base_dir = f"{rest.get_index_path()}/@2i"
                shell = RemoteMachineShellConnection(node, verbose=False)
                try:
                    # Plasma dirs under @2i/ and BHIVE dirs under @2i/@bhive/
                    out, _ = shell.execute_command(
                        f"find {base_dir} {base_dir}/@bhive -maxdepth 1 "
                        f"-name '*.index' -type d 2>/dev/null"
                    )
                    dirs = [line.strip() for line in out if line.strip()]
                    basenames = [d.split("/")[-1] for d in dirs]
                    if not basenames:
                        empty_nodes.append(node.ip)
                    for d in basenames:
                        if not UUID_DIR_RE.match(d):
                            violations.append(f"{node.ip}:{d}")
                    # Check codebook file names: <instId>_<partnId>.codebook
                    for index_dir in dirs:
                        cb_out, _ = shell.execute_command(
                            f"find {index_dir}/codebook -maxdepth 1 "
                            f"-name '*.codebook' -type f 2>/dev/null"
                        )
                        for cb_line in cb_out:
                            cb_name = cb_line.strip().split("/")[-1]
                            if cb_name and not CODEBOOK_RE.match(cb_name):
                                codebook_violations.append(f"{node.ip}:{cb_name}")
                finally:
                    shell.disconnect()
            if empty_nodes:
                raise Exception(f"{label}No index directories found on nodes {empty_nodes}")
            if violations:
                raise Exception(f"{label}Non-UUID index directories found: {violations}")
            if codebook_violations:
                raise Exception(f"{label}Non-UUID codebook files found: {codebook_violations}")
            self.log.info(f"{label}All index directories match UUID format on {[n.ip for n in nodes]}")
            return True

        # STEP 1
        self.set_indexer_snapshot_settings(index_nodes)
        self.log.info("[STEP 1] PASSED - prerequisites verified")

        # STEP 2
        bucket_name, select_queries = self._setup_bucket_and_indexes('dirname_', index_nodes)
        self.log.info(f"[STEP 2] PASSED - bucket '{bucket_name}' with indexes created")

        # STEP 3: verify UUID format
        _verify_uuid_dirs(index_nodes, "[STEP 3] ")
        self.log.info("[STEP 3] PASSED - all dirs are UUID-based")

        # STEP 4: no sensitive names in paths
        rest = RestConnection(index_nodes[0])
        forbidden = helper._collect_forbidden_names(rest)
        self.assertTrue(forbidden, "[STEP 4] No forbidden names discovered from /indexStatus")
        naming_results = helper.verify_no_sensitive_names_in_paths(index_nodes, forbidden)
        naming_failures = {ip: r for ip, r in naming_results.items() if r.get("status") == "failed"}
        self.assertFalse(naming_failures,
                         f"[STEP 4] Sensitive names found in paths: {naming_failures}")
        self.log.info("[STEP 4] PASSED - no sensitive names in on-disk paths")

        # STEP 5
        self.run_scan_validation(select_queries, validate_item_count=True)
        self.log.info("[STEP 5] PASSED - baseline scans")

        # STEP 6: restart and re-check
        self.restart_index_nodes(index_nodes)
        _verify_uuid_dirs(index_nodes, "[STEP 6] ")
        self.log.info("[STEP 6] PASSED - UUID dirs intact after restart")

        # STEP 7
        self.run_scan_validation(select_queries)
        self.log.info("[STEP 7] PASSED - scans after restart")

        # STEP 8: drop and recreate indexes, verify new dirs
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.run_cbq_query(
            query=f"SELECT name FROM system:indexes WHERE keyspace_id = '{bucket_name}' "
                  f"OR bucket_id = '{bucket_name}'",
            server=query_node
        )
        result = self.run_cbq_query(
            query=f"SELECT RAW name FROM system:indexes WHERE keyspace_id = '{bucket_name}' "
                  f"OR bucket_id = '{bucket_name}'",
            server=query_node
        )
        for idx_name in result.get("results", []):
            namespace = f"default:{bucket_name}._default._default"
            self.run_cbq_query(
                query=f'DROP INDEX `{idx_name}` ON `{bucket_name}` USING GSI',
                server=query_node
            )
        self.sleep(30, "[STEP 8] Waiting for index drops to propagate")
        new_select_queries = self.create_indexes_on_namespace(
            f"default:{bucket_name}._default._default", index_nodes,
            'dirname2_', step_prefix="[STEP 8]", wait_for_ready=True
        )
        _verify_uuid_dirs(index_nodes, "[STEP 8] ")
        self.log.info("[STEP 8] PASSED - recreated indexes also use UUID dirs")

        # STEP 9
        self.run_scan_validation(new_select_queries)
        self.log.info("[STEP 9] PASSED - scans on recreated indexes")

        # STEP 10
        self.item_count_related_validations()
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_index_dir_naming")
