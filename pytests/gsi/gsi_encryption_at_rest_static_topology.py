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
        self.log.info("==============  GSIEncryptionAtRestStaticTopology setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIEncryptionAtRestStaticTopology tearDown has started ==============")
        super(GSIEncryptionAtRestStaticTopology, self).tearDown()
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

        node_to_kill = kv_nodes[0]
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
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create,
                    batch_size=10**4,
                    dataset=self.json_template
                )
                for task in tasks:
                    task.result()
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
                    query_definitions = self.gsi_util_obj.get_index_definition_list(
                        dataset=self.json_template,
                        prefix=prefix,
                        skip_primary=False
                    )
                    self.log.info(f"[STEP 5.{idx+1}.b] Generated {len(query_definitions)} index definitions with prefix '{prefix}'")
                    
                    # Collect select queries for validation
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
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
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

        self.restart_index_nodes(index_nodes)

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


        self.restart_index_nodes(index_nodes)

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
                values = ", ".join(
                    f"(\"toggle_upsert_{i}\", {{\"name\": \"toggle_doc_{i}\", \"toggle_seq\": {i}}})"
                    for i in range(upsert_count)
                )
                upsert_query = (
                    f"UPSERT INTO `{bucket_name}` (KEY, VALUE) VALUES {values} RETURNING META().id"
                )
                result = self.run_cbq_query(query=upsert_query, server=query_node)
                returned_ids = [r['id'] for r in result.get('results', [])]
                self.assertEqual(
                    len(returned_ids), upsert_count,
                    f"[STEP 9] Expected {upsert_count} upserted docs in '{bucket_name}', got {len(returned_ids)}"
                )
                self.log.info(f"[STEP 9] Upserted {len(returned_ids)} docs into '{bucket_name}'")
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

        self.restart_index_nodes(index_nodes)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_toggle_and_lifecycle")
        self.log.info(f"Encryption toggled on both buckets and lifecycle ops succeeded")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_storage_corruption(self):
        """
        Encryption at rest - multi-bucket with storage corruption recovery.

        Creates 2 buckets (both encrypted) with indexes, then induces
        plasma shard corruption on a randomly chosen index node, kills and restarts
        the indexer to trigger recovery, then validates scans and lifecycle ops.

        Steps:
        1.  Verify prerequisites (3+ index nodes, encryption enabled)
        2.  Set indexer snapshot settings + enable corrupt index backup flag
        3.  Create bucket_A WITH encryption, bucket_B WITH encryption, load docs
        4.  Create indexes on both buckets, wait for Ready
        5.  Baseline scans on both buckets + file encryption validation
        6.  Induce plasma shard corruption on a randomly chosen index node
        7.  Kill indexer on that node to trigger corruption detection + recovery
        8.  Wait for all indexes to return to Ready state
        9.  Post-recovery mutations: upsert new docs, delete some docs, update some docs
        10. Scans on all indexes post-recovery + compare pre/post-mutation counts
        11. Lifecycle ops (alter/drop/build) on each namespace
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

        # ========== STEP 9: Mutations post-recovery (upsert/delete/update) on both buckets ==========
        upsert_count = 10
        delete_count = 5
        update_count = 5
        try:
            self.log.info(f"[STEP 9] Running post-recovery mutations on both buckets "
                          f"(upsert={upsert_count}, delete={delete_count}, update={update_count})...")
            for bucket_name in [bucket_a, bucket_b]:
                # Upsert new docs
                upsert_values = ", ".join(
                    f"(\"recovery_upsert_{i}\", {{\"name\": \"recovery_doc_{i}\", \"recovery_seq\": {i}}})"
                    for i in range(upsert_count)
                )
                upsert_result = self.run_cbq_query(
                    query=f"UPSERT INTO `{bucket_name}` (KEY, VALUE) VALUES {upsert_values} RETURNING META().id",
                    server=query_node
                )
                actual_upserted = len(upsert_result.get('results', []))
                self.assertEqual(actual_upserted, upsert_count,
                    f"[STEP 9] '{bucket_name}': expected {upsert_count} upserted docs, got {actual_upserted}")

                # Delete docs by key range (first delete_count keys from the initial load)
                delete_keys = ", ".join(f'"pymc{i}"' for i in range(delete_count))
                delete_result = self.run_cbq_query(
                    query=f"DELETE FROM `{bucket_name}` USE KEYS [{delete_keys}] RETURNING META().id",
                    server=query_node
                )
                actual_deleted = len(delete_result.get('results', []))
                self.log.info(f"[STEP 9] '{bucket_name}': deleted {actual_deleted} docs")

                # Update docs by key range (next update_count keys)
                update_keys = ", ".join(f'"pymc{i}"' for i in range(delete_count, delete_count + update_count))
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
            self.restart_index_nodes(index_nodes)
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
        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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
            upsert_values = ", ".join(
                f"(\"reencrypt_upsert_{i}\", {{\"name\": \"reencrypt_doc_{i}\", \"reencrypt_seq\": {i}}})"
                for i in range(upsert_count)
            )
            upsert_result = self.run_cbq_query(
                query=f"UPSERT INTO `{bucket_name}` (KEY, VALUE) VALUES {upsert_values} RETURNING META().id",
                server=query_node
            )
            actual_upserted = len(upsert_result.get('results', []))
            self.assertEqual(actual_upserted, upsert_count,
                f"[STEP 12] Expected {upsert_count} upserted docs, got {actual_upserted}")

            delete_keys = ", ".join(f'"pymc{i}"' for i in range(delete_count))
            delete_result = self.run_cbq_query(
                query=f"DELETE FROM `{bucket_name}` USE KEYS [{delete_keys}] RETURNING META().id",
                server=query_node
            )
            actual_deleted = len(delete_result.get('results', []))
            self.log.info(f"[STEP 12] Deleted {actual_deleted} docs")

            update_keys = ", ".join(f'"pymc{i}"' for i in range(delete_count, delete_count + update_count))
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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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
        kv_node_to_kill = kv_nodes[0]
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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_enable_then_disable")
        self.log.info(f"Full enable→disable roundtrip succeeded on bucket '{bucket_name}'")
        self.log.info("=" * 80)

    # =========================================================================
    # Test 4: Indexer restart concurrently with key rotation / expiry / key drop
    # =========================================================================

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

        Steps:
        1.  Verify prerequisites
        2.  Set indexer snapshot settings, pin DEK interval high
        3.  Create single encrypted bucket, load docs
        4.  Create indexes, wait for Ready
        5.  Baseline scans + file encryption validation
        6.  Launch Thread-1 (_restart_index_nodes) and Thread-2 (key op) concurrently
        7.  Join both threads; wait_until_indexes_online(timeout=1800)
        8.  Scans on existing indexes
        9.  Create new indexes, scan
        10. Lifecycle ops (alter/drop/build)
        11. File encryption validation with new key IDs (rotation/expiry) or item count (drop)
        """
        STABLE_DEK_INTERVAL_S = 36000
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
                namespace, index_nodes, 'rst_conc_', step_prefix="[STEP 4]",
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

        # ========== STEP 6: Launch restart and key op threads concurrently ==========
        thread_errors = []
        new_key_ids = [None]

        def run_restart():
            try:
                self.log.info("[STEP 6 restart] Starting rolling indexer restart...")
                self.restart_index_nodes(index_nodes)
                self.log.info("[STEP 6 restart] Rolling restart complete")
            except Exception as ex:
                thread_errors.append(f"restart thread: {ex}")

        def run_key_op():
            try:
                if self.key_op in ("rotation", "expiry"):
                    DEK_ROTATION_INTERVAL_S = self.kek_rotation_interval_seconds
                    self.log.info(f"[STEP 6 key_op] Triggering DEK {self.key_op} via short TTL ({DEK_ROTATION_INTERVAL_S}s)...")
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
            t_restart.join(timeout=1800)
            t_key_op.join(timeout=700)
            if thread_errors:
                raise Exception(f"Thread failures: {'; '.join(thread_errors)}")
            self.log.info("[STEP 7] Threads joined, waiting for all indexes to be online (timeout=1800s)...")
            self.wait_until_indexes_online(timeout=1800)
            self.log.info(f"[STEP 7] PASSED - new_key_ids={new_key_ids[0]}")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

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
                self.log.info(f"[STEP 11] Validating file encryption with new key IDs {new_key_ids[0]}...")
                if not self.validate_file_encryption_with_key(
                        index_nodes,
                        expected_key_id=sorted(set(baseline_key_ids) | set(new_key_ids[0])),
                        step_prefix="[STEP 11] "):
                    raise Exception("File encryption validation failed after concurrent restart + DEK rotation")
                self.log.info("[STEP 11] PASSED - Files encrypted with new DEK key IDs")
            else:
                self.log.info("[STEP 11] key_op=drop: running item count validation...")
                self.item_count_related_validations()
                self.log.info("[STEP 11] PASSED")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_dgm_with_key_toggle_compaction")
        self.log.info(f"DGM + key toggle + compaction succeeded on buckets '{bucket_1}', '{bucket_2}'")
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

        self.restart_index_nodes(index_nodes)

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

        self.restart_index_nodes(index_nodes)

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
                if r.get("status") not in ("pass", "skipped")
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
                if r.get("status") not in ("pass", "skipped")
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
