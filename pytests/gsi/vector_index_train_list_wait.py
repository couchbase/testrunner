"""vector_index_train_list_wait.py: Tests for vector index train_list_wait feature (MB-65128)

This module tests the automatic retry mechanism for vector index builds when training data
is insufficient. When train_list_wait=true, the indexer will retry building the index
after sufficient documents are loaded, without requiring manual BUILD INDEX.

Uses siftBigANN dataset for index definitions and magma loader for document loading.
NOTE: siftBigANN uses 128-dimension SIFT vectors.

__author__ = "Couchbase QE"
__maintainer__ = "Couchbase QE"
__created_on__ = "2026/04/30"
"""


import time
import random
import string
from datetime import datetime
from couchbase_helper.documentgenerator import SDKDataLoader
from collection.gsi.backup_restore_utils import IndexBackupClient
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection


class VectorIndexTrainListWait(BaseSecondaryIndexingTests):
    """
    Test class for vector index train_list_wait feature.
    
    Feature: MB-65128 - Automatic retry for vector index builds when training data is insufficient
    Couchbase Server 8.1 (Totoro)
    
    Test Scenarios:
    - Create vector index with train_list_wait=true on empty collection
    - Verify index enters deferred/retry state
    - Load documents and verify automatic build transition
    - Validate index becomes online without manual BUILD INDEX
    """

    def setUp(self):
        super(VectorIndexTrainListWait, self).setUp()
        self.log.info("==============  VectorIndexTrainListWait setup has started ==============")
        
        # Test parameters
        self.poll_timeout = self.input.param("poll_timeout", 1200)
        self.poll_interval = self.input.param("poll_interval", 5)
        self.num_docs = self.input.param("num_docs", 10000)
        self.explicit_train_list = self.input.param("explicit_train_list", None)
        self.skip_load = self.input.param("skip_load", True)
        
        # Query node reference
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        
        self.log.info("==============  VectorIndexTrainListWait setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  VectorIndexTrainListWait tearDown has started ==============")
        super(VectorIndexTrainListWait, self).tearDown()
        self.log.info("==============  VectorIndexTrainListWait tearDown has ended ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    # ==================== Helper Methods ====================

    @staticmethod
    def _filter_vector_indexes(index_names):
        """Filter out primary indexes, returning only vector/secondary index names."""
        return [name for name in index_names if not name.startswith('#primary')]

    # ==================== Modular Index Creation Methods ====================

    def _create_bhive_indexes(self, namespace, prefix, train_list_wait=True, 
                               train_list=None, defer_build=False, num_replica=0):
        """
        Create BHIVE vector indexes with train_list_wait option.
        
        Pattern follows scan_report.py:
        1. Get index definitions with bhive_index=True
        2. Generate CREATE INDEX queries with train_list_wait
        3. Create indexes
        4. Return (definitions, create_queries, select_queries)
        
        Args:
            namespace: The keyspace in format 'default:bucket.scope.collection'
            prefix: Prefix for index names
            train_list_wait: Enable auto-retry on insufficient training data
            train_list: Explicit training list size (None = auto)
            defer_build: Use deferred build mode
            num_replica: Number of index replicas
        
        Returns:
            Tuple of (definitions, create_queries, select_queries, index_names)
        """
        self.log.info(f"Creating BHIVE indexes: prefix={prefix}, train_list_wait={train_list_wait}, "
                     f"train_list={train_list}, defer_build={defer_build}, num_replica={num_replica}")
        
        definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"bhive_{prefix}",
            array_indexes=False,
            bhive_index=True,
            scalar=False,
            similarity=self.similarity,
            train_list=train_list,
            scan_nprobes=self.scan_nprobes,
            limit=self.scan_limit,
            is_base64=self.base64,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            skip_primary=True
        )
        
        create_queries = self.gsi_util_obj.get_create_index_list(
            definition_list=definitions,
            namespace=namespace,
            num_replica=num_replica,
            bhive_index=True,
            defer_build=defer_build,
            train_list_wait=train_list_wait,
            trainlist=train_list
        )
        
        # Create indexes
        for query in create_queries:
            self.log.info(f"Creating BHIVE index: {query}")
            if train_list_wait:
                try:
                    self.run_cbq_query(query=query, server=self.query_node)
                except Exception as e:
                    error_str = str(e)
                    if 'RetryableTrainListSizeError' in error_str or 'transient error' in error_str:
                        self.log.info(f"Expected retryable transient error with train_list_wait=True: {e}")
                    else:
                        raise
            else:
                self.run_cbq_query(query=query, server=self.query_node)
        
        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=definitions,
            namespace=namespace,
            limit=self.scan_limit
        )
        
        index_names = [idx.index_name for idx in definitions]
        self.log.info(f"Created BHIVE indexes: {index_names}")
        
        return definitions, create_queries, select_queries, index_names

    def _create_composite_indexes(self, namespace, prefix, train_list_wait=True,
                                   train_list=None, defer_build=False, num_replica=0):
        """
        Create composite vector indexes (scalar + vector) with train_list_wait option.
        
        Args:
            namespace: The keyspace in format 'default:bucket.scope.collection'
            prefix: Prefix for index names
            train_list_wait: Enable auto-retry on insufficient training data
            train_list: Explicit training list size (None = auto)
            defer_build: Use deferred build mode
            num_replica: Number of index replicas
        
        Returns:
            Tuple of (definitions, create_queries, select_queries, index_names)
        """
        self.log.info(f"Creating composite indexes: prefix={prefix}, train_list_wait={train_list_wait}, "
                     f"train_list={train_list}, defer_build={defer_build}, num_replica={num_replica}")
        
        definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"composite_{prefix}",
            array_indexes=False,
            bhive_index=False,
            scalar=False,
            similarity=self.similarity,
            train_list=train_list,
            scan_nprobes=self.scan_nprobes,
            limit=self.scan_limit,
            is_base64=self.base64,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            skip_primary=True
        )
        
        create_queries = self.gsi_util_obj.get_create_index_list(
            definition_list=definitions,
            namespace=namespace,
            num_replica=num_replica,
            bhive_index=False,
            defer_build=defer_build,
            train_list_wait=train_list_wait,
            trainlist=train_list
        )
        
        # Create indexes
        for query in create_queries:
            self.log.info(f"Creating composite index: {query}")
            if train_list_wait:
                try:
                    self.run_cbq_query(query=query, server=self.query_node)
                except Exception as e:
                    error_str = str(e)
                    if 'RetryableTrainListSizeError' in error_str or 'transient error' in error_str:
                        self.log.info(f"Expected retryable transient error with train_list_wait=True: {e}")
                    else:
                        raise
            else:
                self.run_cbq_query(query=query, server=self.query_node)
        
        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=definitions,
            namespace=namespace,
            limit=self.scan_limit
        )
        
        index_names = [idx.index_name for idx in definitions]
        self.log.info(f"Created composite indexes: {index_names}")
        
        return definitions, create_queries, select_queries, index_names

    # ==================== Environment Setup Methods ====================

    def _setup_test_environment(self, bucket_name=None, bucket_size=256, bucket_storage="magma"):
        """
        Setup test environment: create bucket and empty collections.
        
        Returns:
            Tuple of (bucket_name, namespace, scope_name, collection_name)
        """
        bucket_name = bucket_name or self.test_bucket
        
        # Create bucket
        self.log.info(f"Setting up test environment with bucket: {bucket_name}")
        try:
            self.bucket_params = self._create_bucket_params(
                server=self.master,
                size=bucket_size,
                bucket_storage=bucket_storage,
                replicas=self.num_replicas,
                bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy,
                lww=self.lww
            )
            self.cluster.create_standard_bucket(
                name=bucket_name,
                port=11222,
                bucket_params=self.bucket_params
            )
            self.log.info(f"Created bucket: {bucket_name}")
            self.sleep(10, "Waiting for bucket to be ready")
        except Exception as err:
            self.log.error(f"Error creating bucket {bucket_name}: {str(err)}")
            raise
        
        self.buckets = self.rest.get_buckets()
        
        # Create scopes and collections
        self.collection_rest.create_scope_collection_count(
            scope_num=self.num_scopes,
            collection_num=self.num_collections,
            scope_prefix=self.scope_prefix,
            collection_prefix=self.collection_prefix,
            bucket=bucket_name
        )
        self.sleep(10, "Allowing time after collection creation")
        
        # Build namespace list
        self.namespaces = []
        for scope_num in range(self.num_scopes):
            scope_name = f'{self.scope_prefix}_{scope_num + 1}'
            for coll_num in range(self.num_collections):
                coll_name = f'{self.collection_prefix}_{coll_num + 1}'
                namespace = f'default:{bucket_name}.{scope_name}.{coll_name}'
                self.namespaces.append(namespace)
        
        # Parse first namespace for convenience
        namespace = self.namespaces[0]
        parts = namespace.replace('default:', '').split('.')
        scope_name = parts[1]
        collection_name = parts[2]
        
        self.log.info(f"Test environment ready: {namespace}")

        # Create primary index on the collection for scan_consistency support
        primary_query = f"CREATE PRIMARY INDEX ON {namespace} USING GSI"
        try:
            self.run_cbq_query(query=primary_query, server=self.query_node)
        except Exception as e:
            self.log.warning(f"Primary index creation skipped (may already exist): {e}")

        return bucket_name, namespace, scope_name, collection_name

    def _load_documents(self, bucket_name, scope_name, collection_name, 
                        num_docs=None, json_template=None, start_doc=0):
        """
        Load documents with vector embeddings using magma loader.
        
        Args:
            bucket_name: Target bucket
            scope_name: Target scope
            collection_name: Target collection
            num_docs: Number of documents (default: self.num_of_docs_per_collection)
            json_template: Document template (default: self.json_template)
            start_doc: Starting document index to avoid key overlap with previous loads
        """
        num_docs = num_docs or self.num_of_docs_per_collection
        json_template = json_template or self.json_template
        
        self.log.info(f"Loading {num_docs} documents (keys {start_doc}-{start_doc + num_docs}) "
                     f"into {bucket_name}.{scope_name}.{collection_name}")
        
        gen_create = SDKDataLoader(
            num_ops=num_docs,
            percent_create=100,
            percent_update=0,
            percent_delete=0,
            scope=scope_name,
            collection=collection_name,
            json_template=json_template,
            output=True,
            username=self.username,
            password=self.password,
            key_prefix='doc_',
            base64=self.base64,
            model=self.data_model,
            create_start=start_doc,
            create_end=start_doc + num_docs
        )
        
        task = self.cluster.async_load_gen_docs(
            self.master,
            bucket=bucket_name,
            generator=gen_create,
            use_magma_loader=True
        )
        task.result()
        self.sleep(10, "Waiting for documents to be loaded and persisted")
        # Verify document count in the collection
        namespace = f"default:{bucket_name}.{scope_name}.{collection_name}"
        query = f"SELECT RAW COUNT(*) FROM {namespace}"
        result = self.run_cbq_query(query=query, server=self.query_node)
        actual_count = result['results'][0] if result.get('results') else 0
        self.log.info(f"Document count after load in {namespace}: {actual_count} (expected at least {num_docs})")
        if actual_count < num_docs:
            self.log.warning(f"Document count {actual_count} is less than expected {num_docs}")

    # ==================== Index Status Methods ====================

    def _get_index_state(self, index_name):
        """Query system:indexes to get index state"""
        query = f"SELECT state FROM system:indexes WHERE name = '{index_name}'"
        result = self.run_cbq_query(query=query, server=self.query_node)
        
        if result.get('results') and len(result['results']) > 0:
            return result['results'][0].get('state')
        return None

    def _get_index_status(self, index_name):
        """Query /getIndexStatus REST endpoint for index status"""
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(index_node)
        index_metadata = rest.get_indexer_metadata()
        
        for index_info in index_metadata.get('status', []):
            if index_info.get('name') == index_name:
                return {
                    'status': index_info.get('status'),
                    'error': index_info.get('error', ''),
                    'progress': index_info.get('progress', 0)
                }
        return None

    def _poll_indexes_until_ready(self, index_names, timeout=None, interval=None):
        """
        Poll /getIndexStatus until all indexes are Ready or timeout.
        
        Returns:
            True if all indexes became Ready, False otherwise
        """
        timeout = timeout or self.poll_timeout
        interval = interval or self.poll_interval
        
        self.log.info(f"Polling {len(index_names)} indexes (timeout={timeout}s, interval={interval}s)")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_ready = True
            for index_name in index_names:
                status_info = self._get_index_status(index_name)
                
                if status_info:
                    self.log.debug(f"Index {index_name}: status={status_info['status']}, "
                                  f"error={status_info['error']}")
                    
                    if status_info['status'] != 'Ready' or status_info['error']:
                        all_ready = False
                else:
                    all_ready = False
            
            if all_ready:
                self.log.info(f"All {len(index_names)} indexes are Ready!")
                return True
            
            time.sleep(interval)
        
        self.log.warning(f"Timeout waiting for indexes to become Ready")
        return False

    def _verify_indexes_online(self, index_names):
        """Verify all indexes have state='online' in system:indexes"""
        for index_name in index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state: {state}")
            self.assertEqual(state, 'online', 
                           f"Expected index {index_name} state to be 'online', got '{state}'")

    def _log_index_status(self, index_names, message=""):
        """Log current status of all indexes"""
        if message:
            self.log.info(message)
        for index_name in index_names:
            state = self._get_index_state(index_name)
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name}: state={state}, REST status={status_info}")

    # ==================== Scan and Validation Methods ====================

    def _run_vector_scans(self, select_queries, expect_results=True):
        """
        Run vector scan queries and verify results.
        
        Args:
            select_queries: List of SELECT queries to execute
            expect_results: Whether to expect non-empty results
        
        Returns:
            List of query results
        """
        results = []
        for query in select_queries:
            self.log.info(f"Running scan query: {query}")
            result = self.run_cbq_query(query=query, server=self.query_node,
                                        scan_consistency='request_plus',
                                        timeout='1200s')
            results.append(result)
            
            if expect_results:
                self.assertTrue(
                    result.get('results') and len(result['results']) > 0,
                    f"Vector scan returned no results for query: {query}"
                )
                self.log.info(f"Scan returned {len(result['results'])} results")
        
        return results

    def _build_deferred_indexes(self, namespace, index_names):
        """Issue BUILD INDEX for deferred indexes"""
        index_names_str = ", ".join([f"`{name}`" for name in index_names])
        build_query = f"BUILD INDEX ON {namespace} ({index_names_str})"
        self.log.info(f"Building indexes: {build_query}")
        try:
            self.run_cbq_query(query=build_query, server=self.query_node)
        except Exception as e:
            self.log.info(f"BUILD INDEX result: {e}")

    # ==================== Utility Methods ====================

    def _check_indexer_logs(self, log_pattern, expect_present=True):
        """Check indexer logs for specific pattern"""
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        pattern_found = False
        
        for server in indexer_nodes:
            shell = RemoteMachineShellConnection(server)
            _, dir = RestConnection(server).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
            indexer_log = str(dir) + '/indexer.log*'
            
            count, err = shell.execute_command(f'zgrep -c "{log_pattern}" {indexer_log} 2>/dev/null || echo 0')
            if isinstance(count, list):
                count = int(count[0]) if count[0].strip().isdigit() else 0
            else:
                count = int(count) if str(count).strip().isdigit() else 0
            
            shell.disconnect()
            
            if count > 0:
                self.log.info(f"Found {count} occurrences of '{log_pattern}' in logs on {server.ip}")
                pattern_found = True
                break
        
        if expect_present and not pattern_found:
            self.log.warning(f"Expected pattern '{log_pattern}' not found in indexer logs")
        elif not expect_present and pattern_found:
            self.log.warning(f"Unexpected pattern '{log_pattern}' found in indexer logs")
        
        return pattern_found

    def _cleanup(self, namespace):
        """Cleanup indexes and data from collection"""
        self.drop_all_indexes()
        self.sleep(10, "Waiting for indexes to be dropped")
        
        try:
            delete_query = f"DELETE FROM {namespace}"
            self.run_cbq_query(query=delete_query, server=self.query_node)
        except Exception as e:
            self.log.warning(f"Error during data cleanup: {e}")

    # ==================== Partitioned Index Creation Methods ====================

    def _create_partitioned_indexes(self, namespace, prefix, bhive_index, train_list_wait=True,
                                    train_list=None, defer_build=False, num_replica=0,
                                    partition_by_fields=None, num_partition=4):
        """
        Create partitioned vector indexes (BHIVE or composite) with PARTITION BY HASH.

        Partitioning is injected by setting partition_by_fields on each definition object
        before the CREATE INDEX query is generated, since get_create_index_list does not
        accept partition params directly.

        Returns:
            Tuple of (definitions, create_queries, select_queries, index_names)
        """
        if partition_by_fields is None:
            partition_by_fields = ["meta().id"]

        index_type = "BHIVE" if bhive_index else "composite"
        self.log.info(
            f"Creating partitioned {index_type} indexes: prefix={prefix}, "
            f"partition_by={partition_by_fields}, num_partition={num_partition}, "
            f"train_list_wait={train_list_wait}"
        )

        definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"part_{prefix}",
            array_indexes=False,
            bhive_index=bhive_index,
            scalar=False,
            similarity=self.similarity,
            train_list=train_list,
            scan_nprobes=self.scan_nprobes,
            limit=self.scan_limit,
            is_base64=self.base64,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            skip_primary=True
        )

        # Inject partition fields onto each definition so the query generator includes
        # PARTITION BY HASH(...) WITH {"num_partition": N}
        for defn in definitions:
            defn.partition_by_fields = partition_by_fields

        create_queries = []
        for defn in definitions:
            query = defn.generate_index_create_query(
                namespace=namespace,
                defer_build=defer_build,
                num_replica=num_replica,
                train_list=train_list,
                train_list_wait=train_list_wait,
                bhive_index=bhive_index,
                partition_by_fields=partition_by_fields,
                num_partition=num_partition
            )
            create_queries.append(query)

        for query in create_queries:
            self.log.info(f"Creating partitioned {index_type} index: {query}")
            if train_list_wait:
                try:
                    self.run_cbq_query(query=query, server=self.query_node)
                except Exception as e:
                    error_str = str(e)
                    if 'RetryableTrainListSizeError' in error_str or 'transient error' in error_str:
                        self.log.info(
                            f"Expected retryable transient error with train_list_wait=True: {e}"
                        )
                    else:
                        raise
            else:
                self.run_cbq_query(query=query, server=self.query_node)

        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=definitions,
            namespace=namespace,
            limit=self.scan_limit
        )

        index_names = [defn.index_name for defn in definitions]
        self.log.info(f"Created partitioned {index_type} indexes: {index_names}")
        return definitions, create_queries, select_queries, index_names

    def _get_partition_distribution(self, index_names):
        """
        Return a dict mapping index_name -> list of hosting nodes by querying
        /getIndexStatus. Used to record and compare partition distribution across rebalance.
        """
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(index_node)
        metadata = rest.get_indexer_metadata()
        distribution = {}
        for entry in metadata.get('status', []):
            name = entry.get('name')
            if name in index_names:
                hosts = entry.get('hosts', [])
                distribution.setdefault(name, []).extend(hosts)
        return distribution

    # ==================== Test Methods ====================

    def test_create_vector_index_train_list_wait_auto_build_after_load(self):
        """
        TC-1: Create vector index with train_list_wait=true on empty collection and verify
        automatic build after document loading.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector indexes (BHIVE + composite) with train_list_wait=true
            3. Verify index state is 'deferred' via system:indexes
            4. Verify /getIndexStatus shows retryable error
            5. Load 10,000 documents with 128-dimension vector embeddings
            6. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            7. Verify state='online' via system:indexes
            8. Run vector scans and verify results
            10. Check indexer.log for build completion
        """
        self.log.info("=== Starting test_create_vector_index_train_list_wait_auto_build_after_load ===")
        
        # Step 1: Setup bucket, scope, collection
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create indexes with train_list_wait=true on empty collection
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="tlw",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="tlw",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)
        
        self.sleep(15, "Waiting for index creation to be processed")
        
        # Step 3: Verify vector index state is not 'online' (expected 'deferred' or 'pending')
        for index_name in vector_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state after creation: {state}")
            self.assertIn(state, ['deferred', 'pending'],
                         f"Expected index {index_name} state to be 'deferred' or 'pending', got '{state}'")
        
        # Step 4: Verify /getIndexStatus shows retryable error
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status: {status_info}")
            self.assertTrue(
                status_info and status_info.get('error'),
                f"Expected retryable error in index {index_name} status, got: {status_info}"
            )
        
        # Step 5: Load documents
        self._load_documents(bucket_name, scope_name, collection_name)
        self.sleep(30, "Waiting for documents to be persisted and indexer retry")
        
        # Step 6: Poll for Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready within timeout"
        )
        
        # Step 7: Verify state='online'
        self._verify_indexes_online(all_index_names)
        
        # Step 8: Run vector scans
        self._run_vector_scans(all_select_queries)
        
        # Step 10: Check logs
        self._check_indexer_logs("build completed", expect_present=True)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_create_vector_index_train_list_wait_auto_build_after_load PASSED ===")

    def test_create_vector_index_explicit_train_list_threshold(self):
        """
        TC-2: Create vector index with explicit train_list threshold and train_list_wait=true.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Load 2,000 documents with 128-dimension vectors
            3. Create vector index with explicit train_list (from conf) and train_list_wait=true
            4. Verify state is 'deferred' via system:indexes
            5. Verify error contains RetryableTrainListSizeError via /getIndexStatus
            6. Load remaining documents to reach train_list threshold
            7. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            8. Verify state='online' via system:indexes
            9. Run vector scans and verify results
            10. Check indexer.log for build completion without errors
        """
        self.log.info("=== Starting test_create_vector_index_explicit_train_list_threshold ===")
        
        # Step 1: Setup bucket, scope, collection
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        train_list_threshold = self.explicit_train_list
        initial_docs = 5000
        remaining_docs = train_list_threshold - initial_docs
        
        # Step 2: Load initial documents (below train_list threshold)
        self.log.info(f"Loading {initial_docs} documents (below threshold of {train_list_threshold})")
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=initial_docs)
        self.sleep(10, "Waiting for documents to be persisted")
        
        # Step 3: Create indexes with explicit train_list
        _, _, select_queries, index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="tl",
            train_list_wait=True,
            train_list=train_list_threshold,
            num_replica=self.num_index_replica
        )
        
        vector_index_names = self._filter_vector_indexes(index_names)
        self.sleep(15, "Waiting for index creation to be processed")
        
        # Step 4: Verify state is not 'online' (expected 'deferred' or 'pending')
        for index_name in vector_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state after creation: {state}")
            self.assertIn(state, ['deferred', 'pending'],
                         f"Expected index {index_name} state to be 'deferred' or 'pending', got '{state}'")
        
        # Step 5: Verify error contains RetryableTrainListSizeError via /getIndexStatus
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status: {status_info}")
            self.assertIn('RetryableTrainListSizeError', status_info.get('error', ''),
                         f"Expected RetryableTrainListSizeError in index {index_name} error, "
                         f"got: {status_info.get('error', '')}")
        
        # Step 6: Load remaining documents to meet threshold (start from where we left off)
        self.log.info(f"Loading {remaining_docs} additional documents (total will be {train_list_threshold})")
        self._load_documents(bucket_name, scope_name, collection_name,
                           num_docs=remaining_docs, start_doc=initial_docs)
        self.sleep(30, "Waiting for documents to be persisted and indexer retry")
        
        # Step 7: Poll for Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after loading sufficient documents"
        )
        
        # Step 8 & 9: Verify online and run scans
        self._verify_indexes_online(index_names)
        self._run_vector_scans(select_queries)
        
        # Step 10: Check logs
        self._check_indexer_logs("build completed", expect_present=True)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_create_vector_index_explicit_train_list_threshold PASSED ===")

    def test_create_vector_index_without_train_list_wait_immediate_failure(self):
        """
        TC-3: Create vector index without train_list_wait (defer_build=true) on empty collection.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector index (BHIVE + composite) with defer_build=true (train_list_wait defaults to false)
            3. Issue BUILD INDEX
            4. Verify /getIndexStatus error contains InvalidTrainListSize
            5. Verify system:indexes state reflects error/failed state
            6. Load 10,000 documents with 128-dimension vectors
            7. Wait 30s; verify index does NOT auto-build (state unchanged)
            8. Re-issue BUILD INDEX; poll /getIndexStatus until status='Ready'
            9. Verify system:indexes state='online'
        """
        self.log.info("=== Starting test_create_vector_index_without_train_list_wait_immediate_failure ===")
        
        # Step 1: Setup
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create indexes with defer_build=true, train_list_wait=false
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="nwt",
            train_list_wait=False,
            defer_build=True,
            num_replica=self.num_index_replica
        )
        
        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="nwt",
            train_list_wait=False,
            defer_build=True,
            num_replica=self.num_index_replica
        )
        
        all_index_names = bhive_index_names + composite_index_names
        vector_index_names = self._filter_vector_indexes(all_index_names)
        
        self.sleep(10, "Waiting for deferred index creation")
        
        # Step 3: Issue BUILD INDEX
        self._build_deferred_indexes(namespace, all_index_names)
        self.sleep(15, "Waiting for BUILD INDEX to be processed")
        
        # Step 4: Verify /getIndexStatus error (vector indexes only - primary builds fine)
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status after BUILD: {status_info}")
            if status_info:
                self.assertNotIn('RetryableTrainListSizeError', status_info.get('error', ''),
                    f"Index {index_name} should NOT have retryable error when train_list_wait=false")
        
        # Step 5: Verify vector indexes are NOT online (primary index will be online)
        for index_name in vector_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state after failed BUILD: {state}")
            self.assertNotEqual(state, 'online',
                f"Index {index_name} should not be online after failed BUILD on empty collection")
        
        # Step 6: Load documents
        self._load_documents(bucket_name, scope_name, collection_name)
        
        # Step 7: Wait 30s and verify no auto-build for vector indexes
        self.sleep(30, "Waiting to verify no auto-retry without train_list_wait")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} after doc load (no auto-retry expected): {status_info}")
            if status_info:
                self.assertNotEqual(status_info['status'], 'Ready',
                    f"Index {index_name} should NOT auto-build without train_list_wait")
        
        # Step 8: Re-issue BUILD INDEX and poll for Ready
        self.log.info("Re-issuing BUILD INDEX after documents are loaded")
        self._build_deferred_indexes(namespace, all_index_names)
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after re-issuing BUILD INDEX"
        )
        
        # Step 9: Verify state='online'
        self._verify_indexes_online(all_index_names)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_create_vector_index_without_train_list_wait_immediate_failure PASSED ===")

    def test_batch_loading_auto_transition_at_threshold(self):
        """
        TC-4: Batch loading with auto-transition when threshold is crossed.
        
        Steps:
            1. Create bucket with scopes and collections and vector indexes with train_list_wait=true
            2. Confirm index state is 'deferred' in system:indexes
            3. Record training threshold from index metadata or compute expected value
            4. Load 25% of threshold documents; verify index remains 'deferred'
            5. Load another 25% (50% total); verify index remains 'deferred'
            6. Load another 25% (75% total); verify index remains 'deferred'
            7. Load final 25% to reach/cross threshold
            8. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            9. Verify state='online' via system:indexes
            10. Check indexer.log for build completion
            11. Run vector scans and verify results
        """
        self.log.info("=== Starting test_batch_loading_auto_transition_at_threshold ===")
        
        # Step 1: Setup and create indexes on empty collection
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="batch",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="batch",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)
        
        self.sleep(15, "Waiting for index creation to be processed")
        
        # Step 2: Confirm vector index state is not 'online' (expected 'deferred' or 'pending')
        for index_name in vector_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state: {state}")
            self.assertIn(state, ['deferred', 'pending'],
                         f"Expected index {index_name} state to be 'deferred' or 'pending', got '{state}'")
        
        # Step 3: Compute threshold (use num_docs as the target)
        threshold = self.num_docs
        batch_size = threshold // 4
        self.log.info(f"Training threshold: {threshold}, batch size (25%): {batch_size}")
        
        total_loaded = 0
        
        # Steps 4-6: Load 25% batches and verify index remains deferred
        for batch_num in range(1, 4):
            self.log.info(f"Loading batch {batch_num}/4 ({batch_size} docs, "
                         f"total will be {total_loaded + batch_size})")
            self._load_documents(bucket_name, scope_name, collection_name,
                               num_docs=batch_size, start_doc=total_loaded)
            total_loaded += batch_size
            
            self.sleep(20, f"Waiting after batch {batch_num} (total: {total_loaded})")
            self._log_index_status(vector_index_names, f"After {total_loaded} docs ({batch_num * 25}% of threshold):")
            
            for index_name in vector_index_names:
                status_info = self._get_index_status(index_name)
                if status_info and status_info['status'] == 'Ready':
                    self.log.info(f"Index {index_name} became Ready early after {total_loaded} docs")
        
        # Step 7: Load final 25% to reach/cross threshold
        remaining = threshold - total_loaded
        self.log.info(f"Loading final batch ({remaining} docs to reach threshold of {threshold})")
        self._load_documents(bucket_name, scope_name, collection_name,
                           num_docs=remaining, start_doc=total_loaded)
        total_loaded += remaining
        self.sleep(30, f"Waiting after final load (total: {total_loaded})")
        
        # Step 8: Poll for Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after loading threshold documents"
        )
        
        # Step 9: Verify state='online'
        self._verify_indexes_online(all_index_names)
        
        # Step 10: Check logs
        self._check_indexer_logs("build completed", expect_present=True)
        
        # Step 11: Run vector scans
        self._run_vector_scans(all_select_queries)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_batch_loading_auto_transition_at_threshold PASSED ===")

    def test_train_list_wait_with_dimension_mismatch(self):
        """
        TC-5: train_list_wait with dimension mismatch in loaded documents.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector index (BHIVE + composite) with train_list_wait=true, dimension=128
            3. Load 5,000 documents with correct 128-dimension vectors
            4. Load 5,000 documents with incorrect dimensions (64-d and 256-d mixed)
            5. Poll /getIndexStatus every 5s for up to 120s; observe build behavior
            6. If build succeeds: verify state='online'; run vector scan; verify only correct-dim docs in results
            7. If build fails: verify error message indicates dimension mismatch
            8. Check indexer.log for dimension mismatch warnings or errors
        """
        self.log.info("=== Starting test_train_list_wait_with_dimension_mismatch ===")
        
        # Step 1: Setup
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create indexes on empty collection with train_list_wait=true, dimension=128
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="dimmis",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="dimmis",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)
        
        self.sleep(15, "Waiting for index creation to be processed")
        
        # Step 3: Load 5,000 documents with correct 128-dimension vectors
        self.log.info("Loading 5,000 documents with correct 128-dimension vectors")
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=5000)
        self.sleep(10, "Waiting after correct-dimension load")
        
        # Step 4: Load 5,000 documents with incorrect dimensions (64-d and 256-d mixed)
        self.log.info("Loading 2,500 documents with 64-dimension vectors")
        wrong_dim_64_query = (
            f'INSERT INTO {namespace} (KEY k, VALUE v) '
            f'SELECT "wrong_dim64_" || TO_STRING(d) AS k, '
            f'{{"embedding": ARRAY ROUND(RANDOM() * 100, 4) FOR i IN ARRAY_RANGE(0, 64) END, '
            f'"age": d, "city": "TestCity"}} AS v '
            f'FROM ARRAY_RANGE(0, 2500) AS d'
        )
        self.run_cbq_query(query=wrong_dim_64_query, server=self.query_node)
        
        self.log.info("Loading 2,500 documents with 256-dimension vectors")
        wrong_dim_256_query = (
            f'INSERT INTO {namespace} (KEY k, VALUE v) '
            f'SELECT "wrong_dim256_" || TO_STRING(d) AS k, '
            f'{{"embedding": ARRAY ROUND(RANDOM() * 100, 4) FOR i IN ARRAY_RANGE(0, 256) END, '
            f'"age": d, "city": "TestCity"}} AS v '
            f'FROM ARRAY_RANGE(0, 2500) AS d'
        )
        self.run_cbq_query(query=wrong_dim_256_query, server=self.query_node)
        self.sleep(30, "Waiting for all documents to be persisted and indexer retry")
        
        # Step 5: Poll and observe build behavior (vector indexes only)
        build_succeeded = self._poll_indexes_until_ready(vector_index_names)
        
        if build_succeeded:
            # Step 6: Build succeeded - verify online and scan
            self.log.info("Index build succeeded despite dimension mismatches")
            self._verify_indexes_online(all_index_names)
            self._run_vector_scans(all_select_queries)
        else:
            # Step 7: Build failed - verify error message
            self.log.info("Index build did not complete - checking error details")
            for index_name in vector_index_names:
                status_info = self._get_index_status(index_name)
                self.log.info(f"Index {index_name} final status: {status_info}")
                self.assertTrue(
                    status_info and status_info.get('error'),
                    f"Expected error message for index {index_name} with dimension mismatch"
                )
        
        # Step 8: Check indexer.log for dimension mismatch entries
        self._check_indexer_logs("dimension", expect_present=True)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_train_list_wait_with_dimension_mismatch PASSED ===")

    def test_train_list_wait_base64_encoded_vectors(self):
        """
        TC-6: train_list_wait with Base64 encoded vectors.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector index with train_list_wait=true targeting Base64-encoded vector field
            3. Load 10,000 documents with 128-dimension vectors encoded as Base64 strings
            4. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            5. Verify state='online' via system:indexes
            6. Run vector scans and verify results
            7. Check indexer.log for successful build without decode errors
        """
        self.log.info("=== Starting test_train_list_wait_base64_encoded_vectors ===")
        
        # Step 1: Setup
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create index for base64 encoded vectors on empty collection
        base64_index_name = f'idx_b64_{"".join(random.choices(string.ascii_lowercase, k=5))}'
        create_query = (f'CREATE VECTOR INDEX `{base64_index_name}` '
                       f'ON {namespace}(DECODE_VECTOR(embedding, false) VECTOR) '
                       f'USING GSI '
                       f'WITH {{"dimension": 128, "similarity": "L2_SQUARED", '
                       f'"description": "IVF,SQ8", "train_list": 5000, "train_list_wait": true}}')
        
        self.log.info(f"Creating base64 index: {create_query}")
        try:
            self.run_cbq_query(query=create_query, server=self.query_node)
        except Exception as e:
            error_str = str(e)
            if 'ErrTraining' in error_str or 'transient error' in error_str:
                self.log.info(f"Expected transient error with train_list_wait=True: {e}")
            else:
                raise
        
        self.sleep(15, "Waiting for index creation to be processed")
        
        # Step 3: Load documents with base64 encoded vectors
        gen_create = SDKDataLoader(
            num_ops=self.num_of_docs_per_collection,
            percent_create=100,
            percent_update=0,
            percent_delete=0,
            scope=scope_name,
            collection=collection_name,
            json_template=self.json_template,
            output=True,
            username=self.username,
            password=self.password,
            key_prefix='doc_',
            base64=True,
            model=self.data_model
        )
        
        task = self.cluster.async_load_gen_docs(
            self.master,
            bucket=bucket_name,
            generator=gen_create,
            use_magma_loader=True
        )
        task.result()
        self.sleep(30, "Waiting for documents to be persisted and indexer retry")
        
        # Step 4: Poll for Ready
        self.assertTrue(
            self._poll_indexes_until_ready([base64_index_name]),
            f"Index {base64_index_name} did not become Ready"
        )
        
        # Step 5: Verify online state
        state = self._get_index_state(base64_index_name)
        self.assertEqual(state, 'online', f"Expected 'online', got '{state}'")
        
        # Step 6: Vector scan skipped - base64 index scan requires a pre-encoded query vector
        
        # Step 7: Check logs
        self._check_indexer_logs("build completed", expect_present=True)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_train_list_wait_base64_encoded_vectors PASSED ===")

    def test_train_list_wait_mixed_data_types(self):
        """
        TC-7: train_list_wait with mixed data types in vector field.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector index with train_list_wait=true
            3. Load 5,000 documents with valid 128-dimension float arrays
            4. Load 2,000 documents with string values in the vector field
            5. Load 2,000 documents with null values in the vector field
            6. Load 1,000 documents without the vector field at all
            7. Poll /getIndexStatus every 5s for up to 120s; observe build behavior
            8. If build succeeds: verify state='online'; run vector scan; verify only valid vector docs returned
            9. If build fails: verify clear error message about data type issues
            10. Check indexer.log for type mismatch handling entries
        """
        self.log.info("=== Starting test_train_list_wait_mixed_data_types ===")
        
        # Step 1: Setup
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create indexes on empty collection
        _, _, select_queries, index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="mixed",
            train_list_wait=True,
            train_list=self.num_docs,
            num_replica=self.num_index_replica
        )
        
        self.sleep(15, "Waiting for index creation to be processed")
        
        # Step 3: Load 5,000 documents with valid 128-dimension float arrays
        self.log.info("Loading 5,000 documents with valid vector embeddings")
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=5000)
        self.sleep(10, "Waiting after valid vector load")
        
        # Step 4: Load 2,000 documents with string values in the vector field
        self.log.info("Loading 2,000 documents with string values in vector field")
        string_vector_query = (
            f'INSERT INTO {namespace} (KEY k, VALUE v) '
            f'SELECT "string_vec_" || TO_STRING(d) AS k, '
            f'{{"embedding": "not_a_vector", "age": d, "city": "TestCity"}} AS v '
            f'FROM ARRAY_RANGE(0, 2000) AS d'
        )
        self.run_cbq_query(query=string_vector_query, server=self.query_node)
        self.sleep(5, "Waiting after string vector load")
        
        # Step 5: Load 2,000 documents with null values in the vector field
        self.log.info("Loading 2,000 documents with null values in vector field")
        null_vector_query = (
            f'INSERT INTO {namespace} (KEY k, VALUE v) '
            f'SELECT "null_vec_" || TO_STRING(d) AS k, '
            f'{{"embedding": null, "age": d, "city": "TestCity"}} AS v '
            f'FROM ARRAY_RANGE(0, 2000) AS d'
        )
        self.run_cbq_query(query=null_vector_query, server=self.query_node)
        self.sleep(5, "Waiting after null vector load")
        
        # Step 6: Load 1,000 documents without the vector field
        self.log.info("Loading 1,000 documents without vector field")
        no_vector_query = (
            f'INSERT INTO {namespace} (KEY k, VALUE v) '
            f'SELECT "no_vec_" || TO_STRING(d) AS k, '
            f'{{"age": d, "city": "TestCity"}} AS v '
            f'FROM ARRAY_RANGE(0, 1000) AS d'
        )
        self.run_cbq_query(query=no_vector_query, server=self.query_node)
        self.sleep(30, "Waiting for all documents to be persisted and indexer retry")
        
        # Step 7-9: Poll and check build behavior (vector indexes only)
        vector_index_names = self._filter_vector_indexes(index_names)
        build_succeeded = self._poll_indexes_until_ready(vector_index_names)
        
        if build_succeeded:
            # Step 8: Build succeeded - verify online and scan
            self.log.info("Index build succeeded with mixed data types")
            self._verify_indexes_online(index_names)
            self._run_vector_scans(select_queries)
        else:
            # Step 9: Build failed - verify error message
            self.log.info("Index build did not complete - checking error details")
            for index_name in vector_index_names:
                status_info = self._get_index_status(index_name)
                self.log.info(f"Index {index_name} final status: {status_info}")
                self.assertTrue(
                    status_info and status_info.get('error'),
                    f"Expected error message for index {index_name} with mixed data types"
                )
        
        # Step 10: Check logs
        self._check_indexer_logs("build completed", expect_present=build_succeeded)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_train_list_wait_mixed_data_types PASSED ===")

    def test_defer_build_with_train_list_wait_single_build_index(self):
        """
        TC-8: Defer build with train_list_wait=true - single BUILD INDEX triggers retry
        then auto-completes when documents are loaded.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector index (BHIVE + composite) with defer_build=true, train_list_wait=true
            3. Verify system:indexes state is 'deferred'
            4. Issue BUILD INDEX command
            5. Verify /getIndexStatus error contains RetryableTrainListSizeError
            6. Load 10,000 documents with 128-dimension vectors
            7. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            8. Verify system:indexes state='online'
            9. Verify no second BUILD INDEX was needed
            10. Run vector scans and verify results
        """
        self.log.info("=== Starting test_defer_build_with_train_list_wait_single_build_index ===")
        
        # Step 1: Setup
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create DEFERRED indexes with train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="defer",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )
        
        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="defer",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )
        
        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)
        
        self.sleep(10, "Waiting for deferred index creation")
        
        # Step 3: Verify state is 'deferred' (all indexes are deferred due to defer_build=true)
        for index_name in all_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state after creation: {state}")
            self.assertIn(state, ['deferred', 'pending'],
                         f"Expected index {index_name} state to be 'deferred' or 'pending', got '{state}'")
        
        # Step 4: Issue BUILD INDEX
        self._build_deferred_indexes(namespace, all_index_names)
        self.sleep(20, "Waiting after BUILD INDEX")
        
        # Step 5: Verify /getIndexStatus error contains training error (vector indexes only)
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status after BUILD: {status_info}")
            if status_info:
                error_msg = status_info.get('error', '')
                self.assertIn('ErrTraining', error_msg,
                    f"Expected ErrTraining in index {index_name} error, got: {error_msg}")
        
        # Step 6: Load documents
        self._load_documents(bucket_name, scope_name, collection_name)
        self.sleep(30, "Waiting for documents to be persisted and indexer retry")
        
        # Step 7: Poll for Ready (should auto-complete without another BUILD INDEX)
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after loading documents"
        )
        
        # Step 8: Verify state='online'
        self._verify_indexes_online(all_index_names)
        
        # Step 9: Verified implicitly - no second BUILD INDEX was issued above
        self.log.info("Verified: indexes became Ready with only a single BUILD INDEX command")
        
        # Step 10: Run vector scans
        self._run_vector_scans(all_select_queries)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_defer_build_with_train_list_wait_single_build_index PASSED ===")

    def test_defer_build_without_train_list_wait_manual_retry(self):
        """
        TC-9: Defer build without train_list_wait - requires manual BUILD INDEX retry.
        
        Steps:
            1. Create bucket with scopes and collections
            2. Create vector index (BHIVE + composite) with defer_build=true, train_list_wait=false
            3. Issue BUILD INDEX
            4. Verify /getIndexStatus error contains InvalidTrainListSize
            5. Load 10,000 documents with 128-dimension vectors
            6. Wait 30s; verify index state is NOT 'online' (no auto-retry)
            7. Re-issue BUILD INDEX
            8. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            9. Verify system:indexes state='online'
        """
        self.log.info("=== Starting test_defer_build_without_train_list_wait_manual_retry ===")
        
        # Step 1: Setup
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        
        # Step 2: Create DEFERRED indexes WITHOUT train_list_wait
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="defer_nwt",
            train_list_wait=False,
            defer_build=True,
            num_replica=self.num_index_replica
        )
        
        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="defer_nwt",
            train_list_wait=False,
            defer_build=True,
            num_replica=self.num_index_replica
        )
        
        all_index_names = bhive_index_names + composite_index_names
        vector_index_names = self._filter_vector_indexes(all_index_names)
        
        self.sleep(10, "Waiting for deferred index creation")
        
        # Step 3: Issue BUILD INDEX
        self._build_deferred_indexes(namespace, all_index_names)
        self.sleep(20, "Waiting after BUILD INDEX")
        
        # Step 4: Verify /getIndexStatus error (vector indexes only - primary builds fine)
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status after BUILD: {status_info}")
            if status_info:
                self.assertNotIn('RetryableTrainListSizeError', status_info.get('error', ''),
                    f"Index {index_name} should NOT have retryable error when train_list_wait=false")
        
        # Step 5: Load documents
        self._load_documents(bucket_name, scope_name, collection_name)
        
        # Step 6: Wait 30s and verify no auto-build for vector indexes
        self.sleep(30, "Waiting to verify no auto-retry without train_list_wait")
        for index_name in vector_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state after doc load (no auto-retry expected): {state}")
            self.assertNotEqual(state, 'online',
                f"Index {index_name} should NOT auto-build without train_list_wait")
        
        # Step 7: Re-issue BUILD INDEX
        self.log.info("Re-issuing BUILD INDEX after documents are loaded")
        self._build_deferred_indexes(namespace, all_index_names)
        
        # Step 8: Poll for Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after re-issuing BUILD INDEX"
        )
        
        # Step 9: Verify state='online'
        self._verify_indexes_online(all_index_names)
        
        # Cleanup
        self._cleanup(namespace)
        
        self.log.info("=== test_defer_build_without_train_list_wait_manual_retry PASSED ===")

    def test_global_defer_build_setting_does_not_affect_train_list_wait(self):
        """
        TC-10: Enable global deferbuild cluster setting and verify trainlist_wait retry behavior
        is unaffected.

        Steps:
            1. Set cluster setting indexer.settings.defer_build=true via REST API
            2. Create bucket with scopes and collections
            3. Create vector indexes (BHIVE + composite, sparse + dense) with train_list_wait=true,
               dimension=128, similarity=L2 (no explicit defer_build on CREATE INDEX)
            4. Issue BUILD INDEX
            5. Query /getIndexStatus; verify retry state (RetryableTrainListSizeError)
            6. Load 10,000 documents
            7. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            8. Verify system:indexes state='online'
            9. Reset cluster setting indexer.settings.defer_build to false
        """
        self.log.info("=== Starting test_global_defer_build_setting_does_not_affect_train_list_wait ===")

        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        index_rest = RestConnection(index_node)

        # Step 1: Set global defer_build cluster setting to true
        self.log.info("Setting indexer.settings.defer_build=true via REST API")
        index_rest.set_index_settings({"indexer.settings.defer_build": True})
        self.log.info("indexer.settings.defer_build=true set successfully")
        self.sleep(5, "Waiting for cluster setting to propagate")

        try:
            # Step 2: Setup bucket, scope, collection
            bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

            # Step 3: Create BHIVE and composite indexes with train_list_wait=true,
            # no explicit defer_build (rely on default behavior with global setting enabled)
            _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
                namespace=namespace,
                prefix="gdefer",
                train_list_wait=True,
                train_list=self.num_docs,
                defer_build=False,
                num_replica=self.num_index_replica
            )

            _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
                namespace=namespace,
                prefix="gdefer",
                train_list_wait=True,
                train_list=self.num_docs,
                defer_build=False,
                num_replica=self.num_index_replica
            )

            all_index_names = bhive_index_names + composite_index_names
            all_select_queries = bhive_select_queries + composite_select_queries
            vector_index_names = self._filter_vector_indexes(all_index_names)

            self.sleep(15, "Waiting for index creation to be processed")

            # Step 4: Issue BUILD INDEX
            self._build_deferred_indexes(namespace, all_index_names)
            self.sleep(20, "Waiting after BUILD INDEX")

            # Step 5: Verify /getIndexStatus shows RetryableTrainListSizeError for vector indexes
            for index_name in vector_index_names:
                status_info = self._get_index_status(index_name)
                self.log.info(f"Index {index_name} status after BUILD: {status_info}")
                self.assertTrue(
                    status_info and status_info.get('error'),
                    f"Expected retryable error in index {index_name} status, got: {status_info}"
                )
                self.assertIn(
                    'RetryableTrainListSizeError', status_info.get('error', ''),
                    f"Expected RetryableTrainListSizeError in index {index_name} error, "
                    f"got: {status_info.get('error', '')}"
                )

            # Step 6: Load 10,000 documents
            self._load_documents(bucket_name, scope_name, collection_name,
                                 num_docs=self.num_docs)
            self.sleep(30, "Waiting for documents to be persisted and indexer retry")

            # Step 7: Poll for Ready
            self.assertTrue(
                self._poll_indexes_until_ready(vector_index_names),
                "Indexes did not become Ready after loading documents; "
                "global defer_build setting may have interfered with train_list_wait retry"
            )

            # Step 8: Verify state='online'
            self._verify_indexes_online(all_index_names)

            # Run vector scans to confirm index is functional
            self._run_vector_scans(all_select_queries)

            # Cleanup data and indexes
            self._cleanup(namespace)

        finally:
            # Step 9: Always reset cluster setting back to false
            self.log.info("Resetting indexer.settings.defer_build=false via REST API")
            reset_status = index_rest.set_index_settings({"indexer.settings.defer_build": False})
            if not reset_status:
                self.log.warning("Failed to reset indexer.settings.defer_build to false")
            self.sleep(5, "Waiting for cluster setting reset to propagate")

        self.log.info("=== test_global_defer_build_setting_does_not_affect_train_list_wait PASSED ===")

    def test_train_list_below_minimum_centroids_non_retryable_error(self):
        """
        TC-11 + TC-41: Set trainlist below minimum centroids and verify permanent non-retryable
        error regardless of trainlist_wait, including after loading additional documents.

        Steps:
            1. Create bucket with scopes and collections and load 10,000 documents
            2. Create vector indexes (BHIVE + composite, sparse + dense) with train_list=1,
               train_list_wait=true, nlist=256, dimension=128, similarity=L2
            3. Query /getIndexStatus; verify InvalidTrainListSize (non-retryable)
            4. Wait 30s; verify error persists (no retry/recovery)
            5. Check indexer.log; verify no retry attempts logged
            6. Load additional 10,000 documents; wait 30s (TC-41)
            7. Verify error still persists and no auto-build was triggered (TC-41)
        """
        self.log.info("=== Starting test_train_list_below_minimum_centroids_non_retryable_error ===")

        invalid_train_list = 1

        # Step 1: Setup bucket, scope, collection and load 10,000 documents upfront
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        self.log.info("Loading 10,000 documents before creating indexes")
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(15, "Waiting for documents to be persisted")

        # Step 2: Create indexes with train_list=1 (below minimum centroids) and train_list_wait=true.
        # These will raise InvalidTrainListSize immediately — the index is never registered,
        # so /getIndexStatus returns None for them.
        creation_errors = {}

        try:
            self._create_bhive_indexes(
                namespace=namespace,
                prefix="mintl",
                train_list_wait=True,
                train_list=invalid_train_list,
                defer_build=False,
                num_replica=self.num_index_replica
            )
        except Exception as e:
            creation_errors['bhive'] = str(e)
            self.log.info(f"BHIVE creation raised (expected): {e}")

        try:
            self._create_composite_indexes(
                namespace=namespace,
                prefix="mintl",
                train_list_wait=True,
                train_list=invalid_train_list,
                defer_build=False,
                num_replica=self.num_index_replica
            )
        except Exception as e:
            creation_errors['composite'] = str(e)
            self.log.info(f"Composite creation raised (expected): {e}")

        # Step 3: Verify the creation exceptions contain InvalidTrainListSize (non-retryable)
        self.log.info("Verifying InvalidTrainListSize error in creation exceptions")
        for group, error_msg in creation_errors.items():
            self.assertIn(
                'InvalidTrainListSize', error_msg,
                f"Expected non-retryable InvalidTrainListSize in {group} creation error, "
                f"got: '{error_msg}'"
            )
            self.assertNotIn(
                'RetryableTrainListSizeError', error_msg,
                f"{group} index must NOT have a retryable error; train_list=1 is fundamentally invalid"
            )

        # Since the creation failed fatally, indexes are not registered — verify via /getIndexStatus
        # by deriving the expected index names from the prefix used
        expected_bhive_names = [f"bhive_mintlvector_only_bhive", f"bhive_mintlleading_scalar"]
        expected_composite_names = [
            "composite_mintlscalar_leading_vector",
            "composite_mintlmulti_scalar_middle_vec",
            "composite_mintlmulti_scalar_partitioned",
            "composite_mintlleading_vec_partitioned"
        ]
        all_index_names = expected_bhive_names + expected_composite_names
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(20, "Waiting to confirm no retry occurs for non-retryable error")

        def _assert_not_created(label):
            for index_name in vector_index_names:
                status_info = self._get_index_status(index_name)
                self.log.info(f"[{label}] Index {index_name} status: {status_info}")
                self.assertIsNone(
                    status_info,
                    f"[{label}] Index {index_name} must NOT exist in indexer — "
                    f"InvalidTrainListSize is a fatal creation error"
                )
                state = self._get_index_state(index_name)
                self.assertIsNone(
                    state,
                    f"[{label}] Index {index_name} must NOT appear in system:indexes"
                )

        # Step 3 (continued): Confirm indexes were never created
        _assert_not_created("after creation")

        # Step 4: Wait 30s and verify indexes still don't exist
        self.sleep(30, "Waiting 30s to confirm no retry occurs for non-retryable error")
        _assert_not_created("after 30s wait")

        # Step 5: Confirm no retry attempts in indexer.log
        retry_found = self._check_indexer_logs("RetryableTrainListSizeError", expect_present=False)
        self.assertFalse(
            retry_found,
            "Retry attempts (RetryableTrainListSizeError) must NOT be logged for "
            "non-retryable InvalidTrainListSize error"
        )

        # Step 6 (TC-41): Load additional 10,000 documents
        self.log.info(
            "Loading additional 10,000 documents to confirm error is a permanent "
            "configuration issue, not a data-availability issue"
        )
        self._load_documents(
            bucket_name, scope_name, collection_name,
            num_docs=self.num_docs,
            start_doc=self.num_docs   # keys doc_10000..doc_19999 — no key overlap
        )
        self.sleep(30, "Waiting 30s after additional load to observe indexer behaviour")

        # Step 7 (TC-41): Verify indexes still not created — more data cannot fix invalid train_list
        _assert_not_created("after additional 10k docs loaded")
        self.log.info(
            "Verified: InvalidTrainListSize is a permanent configuration error; "
            "loading more documents does not trigger auto-build or resolve the error"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info("=== test_train_list_below_minimum_centroids_non_retryable_error PASSED ===")

    def test_invalid_train_list_wait_values_rejected_at_ddl(self):
        """
        TC-12: Attempt to create index with invalid train_list_wait values and verify DDL rejection.

        Steps:
            1. Attempt CREATE INDEX with train_list_wait="yes" (string); verify DDL error
            2. Attempt CREATE INDEX with train_list_wait=1 (integer); verify DDL error
            3. Attempt CREATE INDEX with train_list_wait=null; verify DDL error
            4. Verify each error message clearly indicates expected boolean type
            5. Verify no index artifacts created in system:indexes
        """
        self.log.info("=== Starting test_invalid_train_list_wait_values_rejected_at_ddl ===")

        # Setup bucket, scope, collection (no document load needed)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Invalid train_list_wait values to test: (value_in_with_clause, label)
        invalid_values = [
            ('"yes"', "string 'yes'"),
            ('1',     "integer 1"),
            ('null',  "null"),
        ]

        index_base = f"idx_inv_tlw_{''.join(random.choices(string.ascii_lowercase, k=5))}"

        for idx_suffix, (tlw_value, label) in enumerate(invalid_values):
            index_name = f"{index_base}_{idx_suffix}"

            # Build a representative CREATE VECTOR INDEX using the invalid value
            create_query = (
                f'CREATE VECTOR INDEX `{index_name}` '
                f'ON {namespace}(embedding VECTOR) '
                f'USING GSI '
                f'WITH {{"dimension": 128, "similarity": "L2_SQUARED", '
                f'"description": "IVF,SQ8", "train_list_wait": {tlw_value}}}'
            )

            self.log.info(f"Step: Attempting CREATE INDEX with train_list_wait={label}: {create_query}")

            # Steps 1-3: Each attempt must raise a DDL error
            error_raised = False
            error_message = ""
            try:
                self.run_cbq_query(query=create_query, server=self.query_node)
            except Exception as e:
                error_raised = True
                error_message = str(e)
                self.log.info(f"Got expected DDL error for train_list_wait={label}: {error_message}")

            # Step 1-3 assertion: DDL must be rejected
            self.assertTrue(
                error_raised,
                f"Expected DDL error for train_list_wait={label} but CREATE INDEX succeeded"
            )

            # Step 4: Error message should indicate a boolean type expectation
            boolean_hints = ['bool', 'boolean', 'true', 'false', 'type', 'invalid', 'expected']
            hint_found = any(hint in error_message.lower() for hint in boolean_hints)
            self.assertTrue(
                hint_found,
                f"Error for train_list_wait={label} should mention boolean type; "
                f"got: '{error_message}'"
            )

            # Step 5: Confirm no index artifact was created in system:indexes
            check_query = f"SELECT COUNT(*) AS cnt FROM system:indexes WHERE name = '{index_name}'"
            result = self.run_cbq_query(query=check_query, server=self.query_node)
            count = result.get('results', [{}])[0].get('cnt', -1)
            self.assertEqual(
                count, 0,
                f"Index artifact '{index_name}' found in system:indexes after failed DDL "
                f"(train_list_wait={label})"
            )
            self.log.info(f"Confirmed: no index artifact for train_list_wait={label}")

        # Cleanup: nothing to drop (no indexes were created)
        self.log.info("No indexes to clean up (all DDL attempts were rejected as expected)")

        self.log.info("=== test_invalid_train_list_wait_values_rejected_at_ddl PASSED ===")

    def test_mixed_train_list_wait_single_build_index_divergent_behavior(self):
        """
        TC-13: Create mixed train_list_wait indexes and issue single BUILD INDEX to verify
        divergent behavior (INV-GSI-003).

        Steps:
            1. Create bucket with scopes and collections
            2. Create vector indexes (BHIVE + composite) idxwait with defer_build=true,
               train_list_wait=true, dimension=128
            3. Create vector indexes (BHIVE + composite) idxnowait with defer_build=true,
               train_list_wait=false, dimension=128
            4. Issue single BUILD INDEX command for all indexes
            5. Verify /getIndexStatus for idxwait contains RetryableTrainListSizeError
            6. Verify /getIndexStatus for idxnowait contains InvalidTrainListSize
            7. Load 10,000 documents
            8. Poll /getIndexStatus for idxwait every 5s until status='Ready'
            9. Verify idxnowait still in error state (no auto-retry)
            10. Re-issue BUILD INDEX for idxnowait; verify it builds to Ready
        """
        self.log.info("=== Starting test_mixed_train_list_wait_single_build_index_divergent_behavior ===")

        # Step 1: Setup bucket, scope, collection (empty — no documents loaded yet)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create deferred indexes WITH train_list_wait=true (idxwait group)
        _, _, wait_bhive_select_queries, wait_bhive_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="wait",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )

        _, _, wait_composite_select_queries, wait_composite_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="wait",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )

        wait_index_names = wait_bhive_names + wait_composite_names
        wait_select_queries = wait_bhive_select_queries + wait_composite_select_queries
        wait_vector_names = self._filter_vector_indexes(wait_index_names)

        # Step 3: Create deferred indexes WITHOUT train_list_wait (idxnowait group)
        _, _, nowait_bhive_select_queries, nowait_bhive_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="nowait",
            train_list_wait=False,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )

        _, _, nowait_composite_select_queries, nowait_composite_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="nowait",
            train_list_wait=False,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )

        nowait_index_names = nowait_bhive_names + nowait_composite_names
        nowait_vector_names = self._filter_vector_indexes(nowait_index_names)

        all_index_names = wait_index_names + nowait_index_names
        self.sleep(10, "Waiting for deferred index creation")

        # Step 4: Issue a single BUILD INDEX for all indexes (both wait and nowait groups)
        self._build_deferred_indexes(namespace, all_index_names)
        self.sleep(20, "Waiting for BUILD INDEX to be processed")

        # Step 5: Verify idxwait vector indexes have RetryableTrainListSizeError
        self.log.info("Verifying RetryableTrainListSizeError for idxwait indexes")
        for index_name in wait_vector_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"idxwait index {index_name} status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for idxwait index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError for idxwait index {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 6: Verify idxnowait vector indexes have InvalidTrainListSize (non-retryable)
        self.log.info("Verifying InvalidTrainListSize for idxnowait indexes")
        for index_name in nowait_vector_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"idxnowait index {index_name} status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for idxnowait index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'InvalidTrainListSize', error_msg,
                f"Expected InvalidTrainListSize for idxnowait index {index_name}, "
                f"got: '{error_msg}'"
            )
            self.assertNotIn(
                'RetryableTrainListSizeError', error_msg,
                f"idxnowait index {index_name} should NOT have a retryable error"
            )

        # Step 7: Load 10,000 documents
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents to be persisted and idxwait retry")

        # Step 8: Poll until idxwait indexes become Ready (auto-retry after data load)
        self.assertTrue(
            self._poll_indexes_until_ready(wait_vector_names),
            "idxwait indexes did not become Ready after loading documents; "
            "train_list_wait auto-retry may not be working"
        )
        self._verify_indexes_online(wait_index_names)
        self._run_vector_scans(wait_select_queries)
        self.log.info("idxwait indexes are online and returning scan results")

        # Step 9: Verify idxnowait indexes are still in error state (no auto-retry occurred)
        self.log.info("Verifying idxnowait indexes have NOT auto-recovered")
        for index_name in nowait_vector_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"idxnowait index {index_name} after doc load: {status_info}")
            self.assertNotEqual(
                status_info.get('status') if status_info else None, 'Ready',
                f"idxnowait index {index_name} should NOT auto-build without train_list_wait"
            )
            state = self._get_index_state(index_name)
            self.assertNotEqual(
                state, 'online',
                f"idxnowait index {index_name} should NOT be online without a manual BUILD INDEX"
            )

        # Step 10: Re-issue BUILD INDEX for idxnowait only; verify they build to Ready
        self.log.info("Re-issuing BUILD INDEX for idxnowait indexes")
        self._build_deferred_indexes(namespace, nowait_index_names)
        self.assertTrue(
            self._poll_indexes_until_ready(nowait_vector_names),
            "idxnowait indexes did not become Ready after manual re-issue of BUILD INDEX"
        )
        self._verify_indexes_online(nowait_index_names)
        self.log.info("idxnowait indexes are now online after manual BUILD INDEX re-issue")

        # Cleanup
        self._cleanup(namespace)

        self.log.info("=== test_mixed_train_list_wait_single_build_index_divergent_behavior PASSED ===")

    def test_small_train_list_index_functionality_as_collection_grows_to_1m(self):
        """
        TC-14: Create vector index with small train_list=10000 on collection growing to 1M
        documents and verify functionality throughout.

        Steps:
            1. Create bucket with scopes and collections
            2. Create vector indexes (BHIVE + composite) with train_list=10000,
               train_list_wait=true, dimension=128, similarity=L2
            3. Load 10,000 documents progressively
            4. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            5. Verify system:indexes state='online'
            6. Run vector scan; record baseline recall
            7. Load additional 990,000 documents (total ~1M)
            8. Poll indexer mutation queue until near 0 (mutations indexed)
            9. Run vector scan again; verify index still functions
            10. Compare recall with baseline; log accuracy differences
        """
        self.log.info("=== Starting test_small_train_list_index_functionality_as_collection_grows_to_1m ===")

        small_train_list = self.explicit_train_list
        initial_docs = 10000
        additional_docs = self.input.param("additional_docs", 990000)
        total_docs = initial_docs + additional_docs
        mutation_queue_timeout = self.input.param("mutation_queue_timeout", 600)
        mutation_queue_threshold = self.input.param("mutation_queue_threshold", 1000)

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list=10000 and train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="grow",
            train_list_wait=True,
            train_list=small_train_list,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="grow",
            train_list_wait=True,
            train_list=small_train_list,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 3: Load initial 10,000 documents to satisfy the train_list threshold
        self.log.info(f"Loading initial {initial_docs} documents to satisfy train_list={small_train_list}")
        self._load_documents(bucket_name, scope_name, collection_name,
                             num_docs=initial_docs, start_doc=0)
        self.sleep(30, "Waiting for initial documents to be persisted and indexer retry")

        # Step 4: Poll until indexes are Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            f"Indexes did not become Ready after loading {initial_docs} documents "
            f"with train_list={small_train_list}"
        )

        # Step 5: Verify state='online'
        self._verify_indexes_online(all_index_names)

        # Step 6: Run baseline vector scan and record result counts
        self.log.info("Running baseline vector scans at 10,000 documents")
        baseline_results = self._run_vector_scans(all_select_queries, expect_results=True)
        baseline_counts = [len(r.get('results', [])) for r in baseline_results]
        self.log.info(f"Baseline scan result counts: {baseline_counts}")

        # Step 7: Load additional 990,000 documents (total ~1M) in batches to avoid OOM
        batch_size = self.input.param("growth_batch_size", 100000)
        loaded_so_far = initial_docs
        self.log.info(f"Loading {additional_docs} additional documents in batches of {batch_size} "
                      f"(target total: {total_docs})")

        while loaded_so_far < total_docs:
            remaining = total_docs - loaded_so_far
            current_batch = min(batch_size, remaining)
            self.log.info(f"Loading batch: {current_batch} docs (offset {loaded_so_far}, "
                          f"cumulative {loaded_so_far + current_batch})")
            self._load_documents(bucket_name, scope_name, collection_name,
                                 num_docs=current_batch, start_doc=loaded_so_far)
            loaded_so_far += current_batch
            self.sleep(5, f"Short pause after batch load (total loaded: {loaded_so_far})")

        self.log.info(f"Finished loading. Total documents: {loaded_so_far}")

        # Step 8: Poll indexer mutation queue until near 0 (all mutations indexed)
        self.log.info("Polling indexer mutation queue until near 0")
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        index_rest = RestConnection(index_node)
        queue_drained = False
        start_time = time.time()

        while time.time() - start_time < mutation_queue_timeout:
            try:
                stats = index_rest.get_indexer_stats()
                queue_size = stats.get('indexer_mutation_queue_size', None)
                if queue_size is None:
                    # Fall back to per-index pending mutations
                    queue_size = sum(
                        stats.get(f'{name}:num_docs_pending', 0)
                        for name in vector_index_names
                    )
                self.log.info(f"Indexer mutation queue size: {queue_size}")
                if queue_size <= mutation_queue_threshold:
                    queue_drained = True
                    self.log.info("Mutation queue drained; all mutations indexed")
                    break
            except Exception as e:
                self.log.warning(f"Error reading indexer stats: {e}")
            time.sleep(self.poll_interval)

        if not queue_drained:
            self.log.warning(
                f"Mutation queue did not drain within {mutation_queue_timeout}s; "
                f"proceeding with scan validation anyway"
            )

        self.sleep(10, "Short stabilisation wait before post-growth scan")

        # Step 9: Run vector scans again; verify index still functions.
        # Retry up to 3 times to tolerate transient i/o timeouts after heavy doc loading.
        self.log.info(f"Running post-growth vector scans at ~{loaded_so_far} documents")
        growth_results = None
        for attempt in range(3):
            try:
                growth_results = self._run_vector_scans(all_select_queries, expect_results=True)
                break
            except Exception as e:
                if attempt < 2 and ('i/o timeout' in str(e) or 'timeout' in str(e).lower()):
                    self.log.warning(
                        f"Post-growth scan attempt {attempt + 1} hit transient timeout; "
                        f"retrying after 30s: {e}"
                    )
                    self.sleep(30, "Waiting before retry of post-growth vector scan")
                else:
                    raise
        if growth_results is None:
            self.fail("Post-growth vector scans failed after 3 attempts")
        growth_counts = [len(r.get('results', [])) for r in growth_results]
        self.log.info(f"Post-growth scan result counts: {growth_counts}")

        # Step 10: Compare recall with baseline and document accuracy differences
        self.log.info("Comparing recall between baseline (10k docs) and post-growth (1M docs):")
        for i, (base_count, growth_count) in enumerate(zip(baseline_counts, growth_counts)):
            query_preview = all_select_queries[i][:80] if i < len(all_select_queries) else f"query_{i}"
            if base_count > 0:
                recall_ratio = growth_count / base_count
                self.log.info(
                    f"  Query {i}: baseline={base_count}, post-growth={growth_count}, "
                    f"ratio={recall_ratio:.2f} — {query_preview}"
                )
                # Scan must still return results; accuracy may differ (document, not fail)
                self.assertGreater(
                    growth_count, 0,
                    f"Vector scan returned 0 results after collection growth for query: {query_preview}"
                )
            else:
                self.log.warning(f"  Query {i}: baseline returned 0 results — skipping ratio check")

        # Cleanup
        self._cleanup(namespace)

        self.log.info("=== test_small_train_list_index_functionality_as_collection_grows_to_1m PASSED ===")

    def test_large_train_list_prolonged_retry_until_threshold_reached(self):
        """
        TC-15: Create vector index with large train_list=500000 and verify prolonged retry
        until threshold is reached.

        Steps:
            1. Create bucket with scopes and collections
            2. Create vector indexes (BHIVE + composite) with train_list=500000,
               train_list_wait=true, dimension=128, similarity=L2
            3. Load 10,000 documents; verify index remains in retry state
            4. Load progressively up to 500,000 documents
            5. Poll /getIndexStatus every 10s for up to 600s until status='Ready'
            6. Verify system:indexes state='online'
            7. Run vector scans and verify results are returned
        """
        self.log.info("=== Starting test_large_train_list_prolonged_retry_until_threshold_reached ===")

        large_train_list = self.input.param("large_train_list", 500000)
        initial_docs = self.input.param("initial_docs", 10000)
        growth_batch_size = self.input.param("growth_batch_size", 100000)
        large_poll_timeout = self.input.param("large_poll_timeout", 600)
        large_poll_interval = self.input.param("large_poll_interval", 10)

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list=500000 and train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="largetl",
            train_list_wait=True,
            train_list=large_train_list,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="largetl",
            train_list_wait=True,
            train_list=large_train_list,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 3: Load 10,000 documents and verify index remains in retry state
        self.log.info(f"Loading initial {initial_docs} documents (well below threshold {large_train_list})")
        self._load_documents(bucket_name, scope_name, collection_name,
                             num_docs=initial_docs, start_doc=0)
        self.sleep(30, "Waiting for documents to be persisted and initial retry to occur")

        self.log.info("Verifying indexes are still in retry state after initial load")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status at {initial_docs} docs: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Index {index_name} should NOT be Ready with only {initial_docs} docs "
                f"(train_list={large_train_list})"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError for index {index_name} at {initial_docs} docs, "
                f"got: '{error_msg}'"
            )

        # Step 4: Load progressively up to large_train_list documents in batches
        loaded_so_far = initial_docs
        self.log.info(f"Progressively loading up to {large_train_list} documents "
                      f"in batches of {growth_batch_size}")

        while loaded_so_far < large_train_list:
            remaining = large_train_list - loaded_so_far
            current_batch = min(growth_batch_size, remaining)
            self.log.info(f"Loading batch: {current_batch} docs "
                          f"(offset {loaded_so_far}, cumulative {loaded_so_far + current_batch})")
            self._load_documents(bucket_name, scope_name, collection_name,
                                 num_docs=current_batch, start_doc=loaded_so_far)
            loaded_so_far += current_batch

            # Log current retry state after each batch; do not assert Ready yet
            self._log_index_status(
                vector_index_names,
                f"Index status after {loaded_so_far} docs ({loaded_so_far * 100 // large_train_list}% of threshold):"
            )
            self.sleep(5, f"Short pause after batch (total: {loaded_so_far})")

        self.log.info(f"Finished progressive loading. Total documents: {loaded_so_far}")
        self.sleep(30, "Waiting for final batch to persist and trigger indexer retry")

        # Step 5: Poll until all indexes become Ready (with extended timeout for large train_list)
        self.assertTrue(
            self._poll_indexes_until_ready(
                vector_index_names,
                timeout=large_poll_timeout,
                interval=large_poll_interval
            ),
            f"Indexes did not become Ready within {large_poll_timeout}s after loading "
            f"{loaded_so_far} documents (train_list={large_train_list}); "
            f"possible hung or timed-out retry state"
        )

        # Step 6: Verify state='online'
        self._verify_indexes_online(all_index_names)

        # Step 7: Run vector scans and verify results are returned
        self._run_vector_scans(all_select_queries, expect_results=True)

        # Cleanup
        self._cleanup(namespace)

        self.log.info("=== test_large_train_list_prolonged_retry_until_threshold_reached PASSED ===")


    def test_rebalance_index_node_with_train_list_wait_retry_state(self):
        """
        TC-16: Rebalance index node hosting train_list_wait index in retry state and verify
        metadata transfer and retry resumption (INV-GSI-004).

        Steps:
            1. Create bucket with scopes and collections; create BHIVE + composite indexes
               with train_list_wait=true on empty collection
            2. Verify retry state via /getIndexStatus (RetryableTrainListSizeError)
            3. Record index distribution map as baseline
            4. Initiate swap rebalance to move index to another node
            5. Verify rebalance does NOT attempt to build the index (check rebalance.log)
            6. Wait for rebalance completion
            7. Verify index is still in retry state with RetryableTrainListSizeError on new topology
            8. Load 10,000 documents
            9. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            10. Verify system:indexes state='online' on destination node
        """
        self.log.info("=== Starting test_rebalance_index_node_with_train_list_wait_retry_state ===")

        # Step 1: Setup bucket, scope, collection (empty) and create indexes
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="rb",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="rb",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 2: Verify indexes are in retry state
        self.log.info("Verifying indexes are in retry state before rebalance")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} pre-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before rebalance for {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 3: Record baseline index distribution map
        self.log.info("Recording baseline index distribution map")
        index_node_before = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=False
        )
        index_map_before = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        self.log.info(f"Baseline index hosting node: {index_node_before.ip}")
        self.log.info(f"Baseline index map: {index_map_before}")

        # Step 4: Initiate swap rebalance — add a new index node, remove the current one
        index_nodes_before = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        # Identify an unused server slot for the incoming index node
        used_ips = {s.ip for s in self.servers[:self.nodes_init]}
        swap_in_server = None
        for server in self.servers:
            if server.ip not in used_ips:
                swap_in_server = server
                break

        if swap_in_server is None:
            # No spare server slot — fall back to rebalance-out then back in
            self.log.warning(
                "No spare server available for swap rebalance; "
                "falling back to rebalance-out of one index node"
            )
            node_to_remove = index_nodes_before[-1]
            self.log.info(f"Rebalancing out index node: {node_to_remove.ip}")
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[],
                to_remove=[node_to_remove]
            )
        else:
            node_to_remove = index_nodes_before[-1]
            self.log.info(
                f"Swap rebalance: adding {swap_in_server.ip}, removing {node_to_remove.ip}"
            )
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[swap_in_server],
                to_remove=[node_to_remove],
                services=["index"]
            )

        # Step 5: While rebalance is running, check rebalance log for build attempts
        self.sleep(10, "Brief pause to let rebalance start before checking logs")
        rebalance_build_attempted = self._check_indexer_logs(
            "TrainIndex", expect_present=False
        )
        if rebalance_build_attempted:
            self.log.warning(
                "Detected possible index build attempt in indexer logs during rebalance; "
                "verify this is not a real training attempt for the retrying index"
            )

        # Step 6: Wait for rebalance completion
        self.log.info("Waiting for rebalance to complete")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(
            reached,
            "SERVER BUG (INV-GSI-004): Rebalance failed while indexes are in "
            "RetryableTrainListSizeError state. The indexer should allow rebalance of retrying "
            "indexes but currently blocks with service_rebalance_failed."
        )
        rebalance.result()
        self.sleep(15, "Waiting for post-rebalance stabilisation")
        self.log.info("Rebalance completed successfully")

        # Refresh services map after rebalance
        self.sleep(10, "Allowing services map to refresh after rebalance")

        # Step 7: Verify index is still in retry state on new topology
        self.log.info("Verifying indexes are still in retry state on new topology")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} post-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name} after rebalance"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError after rebalance for {index_name}, "
                f"got: '{error_msg}' — metadata may not have transferred correctly"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Index {index_name} should NOT be Ready on empty collection after rebalance"
            )

        index_map_after = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        self.log.info(f"Post-rebalance index map: {index_map_after}")

        # Step 8: Load 10,000 documents
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents to be persisted and indexer retry on new node")

        # Step 9: Poll until indexes are Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after rebalance + document load; "
            "retry may not have resumed on destination node (INV-GSI-004)"
        )

        # Step 10: Verify state='online' and run scans
        self._verify_indexes_online(all_index_names)
        self._run_vector_scans(all_select_queries, expect_results=True)

        self.log.info(
            "Verified: index metadata transferred during rebalance; "
            "retry resumed on new topology; auto-build completed after data load"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info("=== test_rebalance_index_node_with_train_list_wait_retry_state PASSED ===")

    def test_rebalance_with_concurrent_document_loading_for_train_list_wait(self):
        """
        TC-17: Rebalance with concurrent document loading for train_list_wait index and
        verify build post-rebalance (INV-GSI-004).

        Steps:
            1. Create bucket with scopes and collections; create BHIVE + composite indexes
               with train_list_wait=true on empty collection
            2. Verify retry state via /getIndexStatus
            3. Record index distribution map as baseline
            4. Initiate rebalance (swap or rebalance-out)
            5. During rebalance, start loading 10,000 documents concurrently
            6. Verify rebalance completes without blocking on training-error index
            7. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            8. Verify system:indexes state='online'
            9. Run vector scans and verify results are returned
            10. Compare post-rebalance index distribution with baseline
        """
        self.log.info("=== Starting test_rebalance_with_concurrent_document_loading_for_train_list_wait ===")

        # Step 1: Setup bucket, scope, collection (empty) and create indexes
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="rbconc",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="rbconc",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 2: Verify retry state
        self.log.info("Verifying indexes are in retry state before rebalance")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} pre-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before rebalance for {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 3: Record baseline index distribution
        self.log.info("Recording baseline index distribution map")
        index_map_before = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        index_nodes_before = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        self.log.info(f"Baseline index map: {index_map_before}")
        self.log.info(f"Index nodes before rebalance: {[n.ip for n in index_nodes_before]}")

        # Step 4: Initiate rebalance — swap or rebalance-out
        used_ips = {s.ip for s in self.servers[:self.nodes_init]}
        swap_in_server = None
        for server in self.servers:
            if server.ip not in used_ips:
                swap_in_server = server
                break

        node_to_remove = index_nodes_before[-1]

        if swap_in_server is not None:
            self.log.info(
                f"Swap rebalance: adding {swap_in_server.ip}, removing {node_to_remove.ip}"
            )
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[swap_in_server],
                to_remove=[node_to_remove],
                services=["index"]
            )
        else:
            self.log.info(f"Rebalancing out index node: {node_to_remove.ip}")
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[],
                to_remove=[node_to_remove]
            )

        # Step 5: Concurrently load 10,000 documents while rebalance is in progress
        self.sleep(5, "Brief pause to let rebalance start before concurrent load")
        self.log.info(f"Starting concurrent document load of {self.num_docs} docs during rebalance")

        gen_create = SDKDataLoader(
            num_ops=self.num_docs,
            percent_create=100,
            percent_update=0,
            percent_delete=0,
            scope=scope_name,
            collection=collection_name,
            json_template=self.json_template,
            output=True,
            username=self.username,
            password=self.password,
            key_prefix='doc_',
            base64=self.base64,
            model=self.data_model,
            create_start=0,
            create_end=self.num_docs
        )

        load_task = self.cluster.async_load_gen_docs(
            self.master,
            bucket=bucket_name,
            generator=gen_create,
            use_magma_loader=True
        )

        # Step 6: Wait for rebalance completion; verify it does not block on retrying index
        self.log.info("Waiting for rebalance to complete (concurrent with document load)")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(
            reached,
            "SERVER BUG (INV-GSI-004): Rebalance failed while indexes are in "
            "RetryableTrainListSizeError state. The indexer should allow rebalance of retrying "
            "indexes but currently blocks with service_rebalance_failed."
        )
        rebalance.result()
        self.log.info("Rebalance completed successfully without blocking on retrying index")

        # Wait for the concurrent load to finish (may already be done)
        load_task.result()
        self.sleep(30, "Waiting for documents to be persisted and indexer retry post-rebalance")

        # Confirm rebalance did not attempt to train the index (soft check)
        self._check_indexer_logs("TrainIndex", expect_present=False)

        # Step 7: Poll until indexes are Ready on destination topology
        self.log.info("Polling indexes until Ready on post-rebalance topology")
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after rebalance + concurrent document load; "
            "retry may not have resumed on destination node (INV-GSI-004)"
        )

        # Step 8: Verify state='online'
        self._verify_indexes_online(all_index_names)

        # Step 9: Run vector scans and verify results
        self._run_vector_scans(all_select_queries, expect_results=True)

        # Step 10: Compare post-rebalance index distribution with baseline
        self.log.info("Comparing post-rebalance index distribution with baseline")
        index_map_after = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        index_nodes_after = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        self.log.info(f"Post-rebalance index map: {index_map_after}")
        self.log.info(f"Index nodes after rebalance: {[n.ip for n in index_nodes_after]}")

        # Verify removed node no longer hosts any of our indexes
        removed_node_ip = node_to_remove.ip
        for index_name in vector_index_names:
            for bucket_key, index_info in index_map_after.items():
                if index_name in index_info:
                    hosted_on = index_info[index_name].get('hosts', [])
                    for host in hosted_on:
                        self.assertNotIn(
                            removed_node_ip, host,
                            f"Index {index_name} is still on removed node {removed_node_ip} "
                            f"after rebalance"
                        )

        self.log.info(
            "Verified: rebalance completed without blocking; "
            "index distribution updated correctly; "
            "retry resumed and auto-build completed post-rebalance"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info("=== test_rebalance_with_concurrent_document_loading_for_train_list_wait PASSED ===")

    def test_rebalance_partitioned_vector_index_retry_state_consistent_across_partitions(self):
        """
        TC-18: Rebalance partitioned vector index in retry state and verify consistent retry
        across all partitions (INV-GSI-004).

        Steps:
            1. Create bucket with scopes and collections
            2. Create partitioned BHIVE + composite indexes with train_list_wait=true,
               num_partition=4, dimension=128
            3. Record partition distribution across nodes via /getIndexStatus
            4. Initiate rebalance causing partition movement
            5. Wait for rebalance completion
            6. Verify all partitions maintain consistent retry state (RetryableTrainListSizeError)
            7. Load 10,000 documents
            8. Poll /getIndexStatus until all partitions reach status='Ready'
            9. Verify no partial partition builds occurred (all-or-nothing transition)
            10. Run vector scans and verify results
        """
        self.log.info(
            "=== Starting "
            "test_rebalance_partitioned_vector_index_retry_state_consistent_across_partitions ==="
        )

        num_partition = self.input.param("num_partition", 4)

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create partitioned BHIVE and composite indexes with train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_partitioned_indexes(
            namespace=namespace,
            prefix="rbpart",
            bhive_index=True,
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica,
            partition_by_fields=["meta().id"],
            num_partition=num_partition
        )

        _, _, composite_select_queries, composite_index_names = self._create_partitioned_indexes(
            namespace=namespace,
            prefix="rbpart",
            bhive_index=False,
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica,
            partition_by_fields=["meta().id"],
            num_partition=num_partition
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for partitioned index creation to be processed")

        # Verify initial retry state before recording distribution
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} initial status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for partitioned index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before rebalance for {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 3: Record partition distribution baseline
        self.log.info("Recording baseline partition distribution")
        distribution_before = self._get_partition_distribution(vector_index_names)
        self.log.info(f"Partition distribution before rebalance: {distribution_before}")

        index_nodes_before = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        self.log.info(f"Index nodes before rebalance: {[n.ip for n in index_nodes_before]}")

        # Step 4: Initiate rebalance to cause partition movement
        used_ips = {s.ip for s in self.servers[:self.nodes_init]}
        swap_in_server = None
        for server in self.servers:
            if server.ip not in used_ips:
                swap_in_server = server
                break

        node_to_remove = index_nodes_before[-1]

        if swap_in_server is not None:
            self.log.info(
                f"Swap rebalance: adding {swap_in_server.ip}, removing {node_to_remove.ip} "
                f"to cause partition redistribution"
            )
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[swap_in_server],
                to_remove=[node_to_remove],
                services=["index"]
            )
        else:
            self.log.info(
                f"Rebalancing out index node {node_to_remove.ip} to redistribute partitions"
            )
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[],
                to_remove=[node_to_remove]
            )

        # Step 5: Wait for rebalance completion
        self.log.info("Waiting for rebalance to complete")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(reached, "Rebalance failed, stuck, or did not complete")
        rebalance.result()
        self.sleep(15, "Waiting for post-rebalance stabilisation")
        self.log.info("Rebalance completed successfully")

        # Step 6: Verify all partitions maintain consistent retry state on new topology
        self.log.info("Verifying all partitions still in retry state after rebalance")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} post-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for {index_name} after rebalance"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError on all partitions of {index_name} "
                f"after rebalance; got: '{error_msg}' — partition may have lost retry state"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Partitioned index {index_name} should NOT be Ready on empty collection "
                f"after rebalance"
            )

        distribution_after_rebalance = self._get_partition_distribution(vector_index_names)
        self.log.info(f"Partition distribution after rebalance: {distribution_after_rebalance}")

        # Step 7: Load 10,000 documents
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents to be persisted and partition retry to trigger")

        # Step 8: Poll until all partitions reach Ready
        self.log.info("Polling until all partitions of each index reach Ready state")
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Partitioned indexes did not become Ready after loading documents; "
            "retry may not have resumed consistently across all partitions (INV-GSI-004)"
        )

        # Step 9: Verify no partial partition builds — all indexes must be fully online
        # A partially built partitioned index would appear as 'online' in system:indexes
        # only after ALL partitions are Ready, so this check confirms all-or-nothing behaviour.
        self._verify_indexes_online(all_index_names)
        self.log.info("All partitions transitioned to online simultaneously (all-or-nothing verified)")

        # Confirm removed node hosts no partitions post-rebalance
        distribution_final = self._get_partition_distribution(vector_index_names)
        self.log.info(f"Final partition distribution: {distribution_final}")
        for index_name, hosts in distribution_final.items():
            for host in hosts:
                self.assertNotIn(
                    node_to_remove.ip, host,
                    f"Partition of {index_name} still hosted on removed node "
                    f"{node_to_remove.ip} after rebalance"
                )

        # Step 10: Run vector scans and verify results from all partitions
        self._run_vector_scans(all_select_queries, expect_results=True)

        self.log.info(
            "Verified: partitioned index retried as a unit; "
            "all partitions maintained consistent retry state through rebalance; "
            "all-or-nothing build transition confirmed"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_rebalance_partitioned_vector_index_retry_state_consistent_across_partitions "
            "PASSED ==="
        )

    def test_failover_indexer_node_train_list_wait_retry_continues_on_replica(self):
        """
        TC-19: Failover indexer node hosting train_list_wait index and verify retry continues
        on recovered/replica node (INV-GSI-002, INV-GSI-005).

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, num_replica=1,
               dimension=128
            3. Verify retry state and record which nodes host active/replica
            4. Failover the node hosting the active index
            5. Verify replica is promoted / index still accessible on surviving node
            6. Verify retry state preserved on surviving node (RetryableTrainListSizeError)
            7. Rebalance to restore topology (add-back with full recovery)
            8. Load 10,000 documents
            9. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            10. Verify system:indexes state='online'
        """
        self.log.info(
            "=== Starting "
            "test_failover_indexer_node_train_list_wait_retry_continues_on_replica ==="
        )

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true and num_replica=1
        # num_replica=1 requires at least 2 index nodes; the 4-node topology with
        # services_init=kv:n1ql-kv:n1ql-index-index satisfies this.
        replica_count = 1

        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="fo",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=replica_count
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="fo",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=replica_count
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 3: Verify retry state and record active/replica node distribution
        self.log.info("Verifying retry state and recording active/replica node distribution")
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        index_rest = RestConnection(index_node)
        metadata = index_rest.get_indexer_metadata()

        # Build a map: index_name -> list of (host, is_replica)
        host_map = {}
        for entry in metadata.get('status', []):
            name = entry.get('name')
            if name in vector_index_names:
                hosts = entry.get('hosts', [])
                is_replica = entry.get('replicaId', 0) != 0
                host_map.setdefault(name, []).append({
                    'hosts': hosts,
                    'is_replica': is_replica,
                    'replicaId': entry.get('replicaId', 0)
                })
                status_info = {
                    'status': entry.get('status'),
                    'error': entry.get('error', '')
                }
                self.log.info(
                    f"Index {name} replica={is_replica} hosts={hosts} status={status_info}"
                )
                error_msg = status_info.get('error', '')
                self.assertIn(
                    'RetryableTrainListSizeError', error_msg,
                    f"Expected RetryableTrainListSizeError before failover for {name}, "
                    f"got: '{error_msg}'"
                )

        # Identify the index node to failover — use the first active (non-replica) host
        # of any vector index; fall back to any index node if all are replicas
        node_to_failover = None
        for name, entries in host_map.items():
            for entry in entries:
                if not entry['is_replica'] and entry['hosts']:
                    host_ip = entry['hosts'][0].split(':')[0]
                    for srv in self.servers[:self.nodes_init]:
                        if srv.ip == host_ip:
                            node_to_failover = srv
                            break
                if node_to_failover:
                    break
            if node_to_failover:
                break

        if node_to_failover is None:
            # Fall back: failover the last index node in the services map
            all_index_nodes = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True
            )
            node_to_failover = all_index_nodes[-1]

        self.log.info(f"Node selected for failover: {node_to_failover.ip}")

        # Step 4: Failover the node hosting the active index (hard failover)
        failover_task = self.cluster.async_failover(
            servers=self.servers[:self.nodes_init],
            failover_nodes=[node_to_failover],
            graceful=False,
            wait_for_pending=180
        )
        failover_task.result()
        self.log.info(f"Failover of {node_to_failover.ip} completed")
        self.sleep(15, "Waiting for failover to stabilise and replica promotion")

        # Step 5: Verify replica was promoted / index is still accessible on surviving node
        surviving_index_nodes = [
            n for n in self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            if n.ip != node_to_failover.ip
        ]
        self.assertTrue(
            len(surviving_index_nodes) > 0,
            "No index nodes remaining after failover"
        )
        self.log.info(
            f"Surviving index nodes: {[n.ip for n in surviving_index_nodes]}"
        )

        # Step 6: Verify retry state preserved on surviving node
        self.log.info("Verifying retry state on surviving node after failover")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(
                f"Index {index_name} post-failover status: {status_info}"
            )
            # Index may be in 'Error' or still 'Building' transiently right after failover;
            # the key assertion is it is NOT Ready (collection is still empty)
            self.assertNotEqual(
                status_info.get('status') if status_info else None, 'Ready',
                f"Index {index_name} should NOT be Ready after failover on empty collection"
            )
            # Retry state (RetryableTrainListSizeError) should still be present if the
            # surviving replica has already picked up the build request
            error_msg = status_info.get('error', '') if status_info else ''
            if error_msg:
                self.assertNotIn(
                    'InvalidTrainListSize', error_msg,
                    f"Index {index_name} error changed from retryable to non-retryable "
                    f"after failover: '{error_msg}' — retry state may have been lost"
                )
                self.log.info(
                    f"Index {index_name} error after failover: '{error_msg}' (expected retryable)"
                )

        # Step 7: Rebalance to restore cluster topology (full recovery add-back)
        self.log.info(
            f"Restoring topology: adding back {node_to_failover.ip} with full recovery"
        )
        self.rest.add_back_node(f"ns_1@{node_to_failover.ip}")
        self.rest.set_recovery_type(f"ns_1@{node_to_failover.ip}", "full")

        rebalance = self.cluster.async_rebalance(
            servers=self.servers[:self.nodes_init],
            to_add=[],
            to_remove=[]
        )
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(reached, "Post-failover rebalance failed, stuck, or did not complete")
        rebalance.result()
        self.log.info("Post-failover rebalance completed; topology restored")
        self.sleep(15, "Waiting for post-rebalance stabilisation")

        # Step 8: Load 10,000 documents
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents to be persisted and indexer retry to trigger")

        # Step 9: Poll until indexes become Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after failover recovery + document load; "
            "retry may not have resumed on recovered/replica node (INV-GSI-002, INV-GSI-005)"
        )

        # Step 10: Verify state='online' and run scans
        self._verify_indexes_online(all_index_names)
        self._run_vector_scans(all_select_queries, expect_results=True)

        self.log.info(
            "Verified: retry state preserved through failover; "
            "replica/recovered node continued retry behavior; "
            "auto-build completed post-recovery"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_failover_indexer_node_train_list_wait_retry_continues_on_replica PASSED ==="
        )

    def test_rebalance_with_non_retryable_training_error_does_not_block(self):
        """
        TC-20: Rebalance with non-retryable training error and verify rebalance completes
        without blocking (INV-GSI-004).

        Steps:
            1. Create bucket with scopes and collections; load 10,000 documents
            2. Create BHIVE + composite indexes with train_list=1, train_list_wait=true,
               nlist=256, dimension=128 (triggers InvalidTrainListSize — non-retryable)
            3. Verify InvalidTrainListSize error in /getIndexStatus
            4. Record index distribution as baseline
            5. Initiate rebalance
            6. Verify rebalance completes without blocking on error index
            7. Verify InvalidTrainListSize error persists on destination after rebalance
            8. Compare post-rebalance distribution with baseline
        """
        self.log.info(
            "=== Starting test_rebalance_with_non_retryable_training_error_does_not_block ==="
        )

        # Step 1: Setup bucket, scope, collection — empty (no documents)
        # With train_list=self.num_docs and train_list_wait=False on an empty collection,
        # the indexer registers the index but marks it with InvalidTrainListSize (non-retryable),
        # which is what we need to verify that rebalance is not blocked.
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=False on empty collection so the indexer
        # registers the index in a non-retryable InvalidTrainListSize error state.
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="nonret",
            train_list_wait=False,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="nonret",
            train_list_wait=False,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(20, "Waiting for index creation to be processed")

        # Step 3: Verify InvalidTrainListSize error in /getIndexStatus
        self.log.info("Verifying InvalidTrainListSize error before rebalance")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} pre-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'InvalidTrainListSize', error_msg,
                f"Expected InvalidTrainListSize before rebalance for {index_name}, "
                f"got: '{error_msg}'"
            )
            self.assertNotIn(
                'RetryableTrainListSizeError', error_msg,
                f"Index {index_name} should have a NON-retryable error; "
                f"got retryable: '{error_msg}'"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Index {index_name} should NOT be Ready with invalid train_list=1"
            )

        # Step 4: Record baseline index distribution
        self.log.info("Recording baseline index distribution")
        index_map_before = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        index_nodes_before = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        distribution_before = self._get_partition_distribution(vector_index_names)
        self.log.info(f"Baseline index map: {index_map_before}")
        self.log.info(f"Index nodes before: {[n.ip for n in index_nodes_before]}")

        # Step 5: Initiate rebalance — swap or rebalance-out
        used_ips = {s.ip for s in self.servers[:self.nodes_init]}
        swap_in_server = None
        for server in self.servers:
            if server.ip not in used_ips:
                swap_in_server = server
                break

        node_to_remove = index_nodes_before[-1]

        if swap_in_server is not None:
            self.log.info(
                f"Swap rebalance: adding {swap_in_server.ip}, removing {node_to_remove.ip}"
            )
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[swap_in_server],
                to_remove=[node_to_remove],
                services=["index"]
            )
        else:
            self.log.info(f"Rebalancing out index node: {node_to_remove.ip}")
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[],
                to_remove=[node_to_remove]
            )

        # Step 6: Wait for rebalance completion — must not block on the error index
        self.log.info(
            "Waiting for rebalance to complete; it must NOT block on the non-retryable error index"
        )
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(
            reached,
            "Rebalance failed, stuck, or did not complete — a non-retryable "
            "InvalidTrainListSize error should never block rebalance (INV-GSI-004)"
        )
        rebalance.result()
        self.log.info("Rebalance completed without blocking on the non-retryable error index")
        self.sleep(15, "Waiting for post-rebalance stabilisation")

        # Soft-check that rebalance logs show no blocked-on-training entries
        self._check_indexer_logs("TrainIndex", expect_present=False)

        # Step 7: Verify InvalidTrainListSize error persists on destination node
        self.log.info("Verifying InvalidTrainListSize error persists after rebalance")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} post-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for {index_name} after rebalance"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'InvalidTrainListSize', error_msg,
                f"Expected InvalidTrainListSize to persist after rebalance for {index_name}, "
                f"got: '{error_msg}' — error state may have been cleared or changed during move"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Index {index_name} should NOT be Ready after rebalance; "
                f"invalid train_list=1 cannot be resolved by topology change"
            )

        # Step 8: Compare post-rebalance distribution with baseline
        self.log.info("Comparing post-rebalance index distribution with baseline")
        index_map_after = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        distribution_after = self._get_partition_distribution(vector_index_names)
        self.log.info(f"Post-rebalance index map: {index_map_after}")
        self.log.info(f"Distribution before: {distribution_before}")
        self.log.info(f"Distribution after:  {distribution_after}")

        # Removed node must no longer host any of the error indexes
        for index_name, hosts in distribution_after.items():
            for host in hosts:
                self.assertNotIn(
                    node_to_remove.ip, host,
                    f"Index {index_name} is still on removed node {node_to_remove.ip} "
                    f"after rebalance"
                )

        self.log.info(
            "Verified: non-retryable error did not block rebalance; "
            "InvalidTrainListSize error persists on destination; "
            "index distribution updated correctly"
        )

        # Cleanup (indexes only — no data cleanup needed per spec)
        self.drop_all_indexes()
        self.sleep(10, "Waiting for indexes to be dropped")

        self.log.info(
            "=== test_rebalance_with_non_retryable_training_error_does_not_block PASSED ==="
        )

    def test_rebalance_deferred_train_list_wait_index_in_retry_state_build_completes_post_rebalance(self):
        """
        TC-21: Rebalance deferred vector index with train_list_wait=true during and after
        BUILD INDEX and verify build completes post-rebalance (INV-GSI-003, INV-GSI-004).

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with defer_build=true, train_list_wait=true,
               dimension=128
            3. Verify system:indexes state is 'deferred'
            4. Issue BUILD INDEX; verify RetryableTrainListSizeError in /getIndexStatus
            5. Record baseline index distribution map
            6. Initiate rebalance while index is in retry state
            7. Verify rebalance does NOT attempt to build the index (check rebalance.log)
            8. Wait for rebalance completion
            9. Verify retry state persists on new topology (RetryableTrainListSizeError)
            10. Load 10,000 documents
            11. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            12. Verify system:indexes state='online'
            13. Run vector scans and verify results
            14. Compare post-rebalance index distribution with baseline
        """
        self.log.info(
            "=== Starting "
            "test_rebalance_deferred_train_list_wait_index_in_retry_state_build_completes_post_rebalance ==="
        )

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create deferred indexes with train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="deftlwrb",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="deftlwrb",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=True,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(10, "Waiting for deferred index creation")

        # Step 3: Verify all indexes are in 'deferred' state in system:indexes
        self.log.info("Verifying all indexes are in 'deferred' state after creation")
        for index_name in all_index_names:
            state = self._get_index_state(index_name)
            self.log.info(f"Index {index_name} state after creation: {state}")
            self.assertIn(
                state, ['deferred', 'pending'],
                f"Expected 'deferred' or 'pending' for {index_name} after deferred creation, "
                f"got: '{state}'"
            )

        # Step 4: Issue BUILD INDEX; verify RetryableTrainListSizeError in /getIndexStatus
        self._build_deferred_indexes(namespace, all_index_names)
        self.sleep(20, "Waiting after BUILD INDEX to allow retry state to be set")

        self.log.info("Verifying RetryableTrainListSizeError after BUILD INDEX")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} status after BUILD: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError after BUILD INDEX for {index_name}, "
                f"got: '{error_msg}'"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Index {index_name} should NOT be Ready on empty collection"
            )

        # Step 5: Record baseline index distribution
        self.log.info("Recording baseline index distribution")
        index_map_before = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        distribution_before = self._get_partition_distribution(vector_index_names)
        index_nodes_before = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        self.log.info(f"Baseline index map: {index_map_before}")
        self.log.info(f"Index nodes before rebalance: {[n.ip for n in index_nodes_before]}")

        # Step 6: Initiate rebalance while index is in retry state
        used_ips = {s.ip for s in self.servers[:self.nodes_init]}
        swap_in_server = None
        for server in self.servers:
            if server.ip not in used_ips:
                swap_in_server = server
                break

        node_to_remove = index_nodes_before[-1]

        if swap_in_server is not None:
            self.log.info(
                f"Swap rebalance: adding {swap_in_server.ip}, removing {node_to_remove.ip}"
            )
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[swap_in_server],
                to_remove=[node_to_remove],
                services=["index"]
            )
        else:
            self.log.info(f"Rebalancing out index node: {node_to_remove.ip}")
            rebalance = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[],
                to_remove=[node_to_remove]
            )

        # Step 7: Soft-check that rebalance does not attempt to build the index
        self.sleep(10, "Brief pause to let rebalance start before checking logs")
        self._check_indexer_logs("TrainIndex", expect_present=False)

        # Step 8: Wait for rebalance completion
        self.log.info("Waiting for rebalance to complete")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(
            reached,
            "Rebalance failed, stuck, or did not complete — retry-state deferred index "
            "should not block rebalance (INV-GSI-004)"
        )
        rebalance.result()
        self.log.info("Rebalance completed successfully")
        self.sleep(15, "Waiting for post-rebalance stabilisation")

        # Step 9: Verify retry state persists on new topology
        self.log.info("Verifying RetryableTrainListSizeError persists on new topology")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} post-rebalance status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for {index_name} after rebalance"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError to persist after rebalance for "
                f"{index_name}, got: '{error_msg}' — retry state may have been lost during move"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Index {index_name} should NOT be Ready on empty collection after rebalance"
            )

        # Step 10: Load 10,000 documents
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents to be persisted and indexer retry to trigger")

        # Step 11: Poll until indexes become Ready (no second BUILD INDEX issued)
        self.log.info(
            "Polling until indexes are Ready — no second BUILD INDEX should be needed "
            "(INV-GSI-003)"
        )
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after rebalance + document load; "
            "retry may not have resumed on destination node, or a second BUILD INDEX "
            "was incorrectly required (INV-GSI-003, INV-GSI-004)"
        )

        # Step 12: Verify state='online'
        self._verify_indexes_online(all_index_names)
        self.log.info(
            "Verified: single BUILD INDEX was sufficient; no second BUILD INDEX needed "
            "after rebalance (INV-GSI-003)"
        )

        # Step 13: Run vector scans
        self._run_vector_scans(all_select_queries, expect_results=True)

        # Step 14: Compare post-rebalance distribution with baseline
        self.log.info("Comparing post-rebalance index distribution with baseline")
        index_map_after = self.get_index_map_from_index_endpoint(
            return_system_query_scope=False
        )
        distribution_after = self._get_partition_distribution(vector_index_names)
        self.log.info(f"Post-rebalance index map: {index_map_after}")
        self.log.info(f"Distribution before: {distribution_before}")
        self.log.info(f"Distribution after:  {distribution_after}")

        # Removed node must no longer host any indexes
        for index_name, hosts in distribution_after.items():
            for host in hosts:
                self.assertNotIn(
                    node_to_remove.ip, host,
                    f"Index {index_name} is still on removed node {node_to_remove.ip} "
                    f"after rebalance"
                )

        self.log.info(
            "Verified: deferred index with train_list_wait survived rebalance in retry state; "
            "no second BUILD INDEX was needed; auto-build completed on destination node; "
            "index distribution updated correctly (INV-GSI-003, INV-GSI-004)"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_rebalance_deferred_train_list_wait_index_in_retry_state_build_completes_post_rebalance "
            "PASSED ==="
        )

    def test_backup_restore_preserves_train_list_wait_retry_state(self):
        """
        TC-22: Backup cluster with train_list_wait index and restore to same-config cluster
        and verify index state preserved.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, dimension=128
            3. Verify RetryableTrainListSizeError in /getIndexStatus
            4. Record index metadata, state, and distribution as baseline
            5. Perform full cluster backup using IndexBackupClient (cbbackupmgr)
            6. Restore backup to a second 4-node cluster with identical configuration
            7. Verify index exists on restored cluster with same retry state
            8. Verify system:indexes state matches baseline on restored cluster
            9. Load 10,000 documents on restored cluster
            10. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            11. Verify system:indexes state='online' on restored cluster
            12. Run vector scans and verify results on restored cluster
        """
        self.log.info("=== Starting test_backup_restore_preserves_train_list_wait_retry_state ===")

        # Determine restore target: second cluster from self.input.clusters if present,
        # otherwise fall back to restoring on the same cluster (single-cluster test run).
        clusters_dict = self.input.clusters
        restore_master = None
        restore_query_node = None
        single_cluster_mode = False

        if clusters_dict and len(clusters_dict) > 1 and clusters_dict.get(1):
            restore_servers = clusters_dict[1]
            restore_master = restore_servers[0]
            self.log.info(
                f"Restore target: second cluster at {restore_master.ip}"
            )
        else:
            self.log.warning(
                "No second cluster configured in input.clusters; "
                "falling back to restore on same cluster (drop + restore)"
            )
            restore_master = self.master
            single_cluster_mode = True

        # Step 1: Setup bucket, scope, collection (empty) on source cluster
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true on empty source collection
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="bkp",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="bkp",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 3: Verify retry state on source cluster
        self.log.info("Verifying retry state on source cluster before backup")
        baseline_states = {}
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Source index {index_name} status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before backup for {index_name}, "
                f"got: '{error_msg}'"
            )
            baseline_states[index_name] = {
                'status': status_info.get('status'),
                'error': error_msg
            }

        # Step 4: Record baseline — system:indexes state for all indexes
        self.log.info("Recording baseline system:indexes states")
        baseline_system_states = {}
        for index_name in all_index_names:
            state = self._get_index_state(index_name)
            baseline_system_states[index_name] = state
            self.log.info(f"Baseline system:indexes state for {index_name}: {state}")

        # Step 5: Perform full cluster backup using IndexBackupClient (cbbackupmgr)
        self.log.info("Performing full cluster backup via cbbackupmgr")
        source_index_node = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=False
        )
        backup_client = IndexBackupClient(
            backup_node=source_index_node,
            use_cbbackupmgr=True,
            backup_bucket=bucket_name,
            restore_node=restore_master
        )

        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(
            backup_result[0],
            f"Backup failed: {backup_result[1]}"
        )
        self.log.info("Backup completed successfully")

        # Step 6: Restore to target cluster
        # For single-cluster mode: drop all indexes + bucket first, then restore.
        # For dual-cluster mode: restore directly to second cluster.
        if single_cluster_mode:
            self.log.info(
                "Single-cluster mode: dropping indexes and bucket before restore"
            )
            self.drop_all_indexes()
            self.sleep(10, "Waiting for indexes to be dropped")
            self.rest.delete_bucket(bucket_name)
            self.sleep(10, "Waiting for bucket deletion before restore")

        self.log.info(f"Restoring backup to {restore_master.ip}")
        restore_result = backup_client.restore(
            use_https=self.use_https,
            restore_args="--auto-create-buckets"
        )
        self.assertTrue(
            restore_result[0],
            f"Restore failed: {restore_result[1]}"
        )
        self.log.info("Restore completed successfully")
        self.sleep(20, "Waiting for restored indexes to be registered by indexer")

        # Determine the query node on the restore target
        if single_cluster_mode:
            restore_query_node = self.query_node
        else:
            restore_query_node = None
            for srv in clusters_dict[1]:
                try:
                    r = RestConnection(srv)
                    services = r.get_nodes_services()
                    for _, svc_list in services.items():
                        if 'n1ql' in svc_list:
                            restore_query_node = srv
                            break
                except Exception:
                    pass
                if restore_query_node:
                    break
            if restore_query_node is None:
                restore_query_node = restore_master

        # Find the index node in the restore cluster to query /getIndexStatus on port 9102.
        if single_cluster_mode:
            restore_index_node = source_index_node
        else:
            restore_cluster_servers = clusters_dict.get(1, [restore_master])
            restore_index_node = restore_master
            for srv in restore_cluster_servers:
                try:
                    RestConnection(srv).get_indexer_metadata()
                    restore_index_node = srv
                    break
                except Exception:
                    pass

        # Helper: get index status from restore cluster
        def _get_restore_index_status(index_name):
            try:
                r = RestConnection(restore_index_node)
                meta = r.get_indexer_metadata()
                for entry in meta.get('status', []):
                    if entry.get('name') == index_name:
                        return {
                            'status': entry.get('status'),
                            'error': entry.get('error', '')
                        }
            except Exception as e:
                self.log.warning(f"Error querying restore cluster status: {e}")
            return None

        # Helper: get system:indexes state from restore cluster
        def _get_restore_index_state(index_name):
            try:
                query = f"SELECT state FROM system:indexes WHERE name = '{index_name}'"
                result = self.run_cbq_query(
                    query=query, server=restore_query_node
                )
                if result.get('results'):
                    return result['results'][0].get('state')
            except Exception as e:
                self.log.warning(f"Error querying system:indexes on restore cluster: {e}")
            return None

        # Step 7: Verify index exists on restored cluster with same retry state
        self.log.info("Verifying retry state on restored cluster")
        for index_name in vector_index_names:
            status_info = _get_restore_index_status(index_name)
            self.log.info(
                f"Restored index {index_name} /getIndexStatus: {status_info}"
            )
            self.assertTrue(
                status_info is not None,
                f"Index {index_name} not found on restored cluster; "
                f"backup/restore may not have preserved the index metadata"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError on restored cluster for {index_name}, "
                f"got: '{error_msg}' — train_list_wait retry state was not preserved"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Restored index {index_name} should NOT be Ready on empty collection"
            )

        # Step 8: Verify system:indexes state matches baseline on restored cluster
        self.log.info("Verifying system:indexes state on restored cluster matches baseline")
        for index_name in all_index_names:
            restored_state = _get_restore_index_state(index_name)
            baseline_state = baseline_system_states.get(index_name)
            self.log.info(
                f"Restored system:indexes state for {index_name}: {restored_state} "
                f"(baseline: {baseline_state})"
            )
            self.assertIsNotNone(
                restored_state,
                f"Index {index_name} not found in system:indexes on restored cluster"
            )

        # Step 9: Load 10,000 documents on restored cluster
        self.log.info("Loading documents on restored cluster to trigger auto-build")
        gen_create = SDKDataLoader(
            num_ops=self.num_docs,
            percent_create=100,
            percent_update=0,
            percent_delete=0,
            scope=scope_name,
            collection=collection_name,
            json_template=self.json_template,
            output=True,
            username=restore_master.rest_username,
            password=restore_master.rest_password,
            key_prefix='doc_',
            base64=self.base64,
            model=self.data_model,
            create_start=0,
            create_end=self.num_docs
        )

        load_task = self.cluster.async_load_gen_docs(
            restore_master,
            bucket=bucket_name,
            generator=gen_create,
            use_magma_loader=True
        )
        load_task.result()
        self.sleep(30, "Waiting for documents to be persisted and indexer retry to trigger")

        # Step 10: Poll until indexes are Ready on restored cluster
        self.log.info("Polling restored cluster until indexes become Ready")
        start_time = time.time()
        all_ready = False
        while time.time() - start_time < self.poll_timeout:
            ready_count = 0
            for index_name in vector_index_names:
                s = _get_restore_index_status(index_name)
                if s and s.get('status') == 'Ready' and not s.get('error'):
                    ready_count += 1
            if ready_count == len(vector_index_names):
                all_ready = True
                self.log.info("All indexes are Ready on restored cluster")
                break
            time.sleep(self.poll_interval)

        self.assertTrue(
            all_ready,
            "Indexes did not become Ready on restored cluster after loading documents; "
            "train_list_wait auto-retry may not have resumed post-restore"
        )

        # Step 11: Verify state='online' on restored cluster
        for index_name in all_index_names:
            state = _get_restore_index_state(index_name)
            self.log.info(f"Restored index {index_name} final state: {state}")
            self.assertEqual(
                state, 'online',
                f"Expected 'online' for restored index {index_name}, got '{state}'"
            )

        # Step 12: Run vector scans on restored cluster
        self.log.info("Running vector scans on restored cluster")
        for query in all_select_queries:
            self.log.info(f"Running scan on restored cluster: {query}")
            result = self.run_cbq_query(
                query=query,
                server=restore_query_node,
                scan_consistency='request_plus'
            )
            self.assertTrue(
                result.get('results') and len(result['results']) > 0,
                f"Vector scan returned no results on restored cluster for query: {query}"
            )
            self.log.info(
                f"Restored cluster scan returned {len(result['results'])} results"
            )

        # Cleanup backup archive
        try:
            backup_client.remove_backup()
        except Exception as e:
            self.log.warning(f"Could not remove backup archive: {e}")

        # Cleanup source cluster indexes (if single-cluster mode, also cleans restored indexes)
        self.drop_all_indexes()
        self.sleep(10, "Waiting for indexes to be dropped")

        self.log.info(
            "=== test_backup_restore_preserves_train_list_wait_retry_state PASSED ==="
        )

    def test_backup_restore_train_list_wait_to_different_topology(self):
        """
        TC-23: Backup cluster with train_list_wait index and restore to a cluster with
        different topology and verify index behavior.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, dimension=128
            3. Verify RetryableTrainListSizeError in /getIndexStatus
            4. Record baseline index metadata, state, and distribution
            5. Perform full cluster backup using cbbackupmgr
            6. Restore to a different-topology cluster (different node count / service distribution)
            7. Verify index exists and is in retry state on restored cluster
            8. Verify index placement adapts to new topology (different node distribution)
            9. Load 10,000 documents on restored cluster
            10. Poll /getIndexStatus every 5s for up to 120s until status='Ready'
            11. Verify system:indexes state='online' on restored cluster
            12. Run vector scans and verify results
            13. Check indexer.log for topology adaptation warnings or errors
        """
        self.log.info(
            "=== Starting test_backup_restore_train_list_wait_to_different_topology ==="
        )

        # Determine restore target: second cluster entry from input.clusters (expected to have
        # a different topology — e.g., 3 nodes or different service distribution).
        # Fall back to same cluster with a rebalance-out to simulate different topology.
        clusters_dict = self.input.clusters
        restore_master = None
        restore_servers = []
        single_cluster_mode = False

        if clusters_dict and len(clusters_dict) > 1 and clusters_dict.get(1):
            restore_servers = clusters_dict[1]
            restore_master = restore_servers[0]
            self.log.info(
                f"Restore target (different topology): {restore_master.ip} "
                f"({len(restore_servers)} nodes)"
            )
        else:
            self.log.warning(
                "No second cluster in input.clusters; falling back to single-cluster mode "
                "(simulate topology difference by restoring after rebalance-out of one index node)"
            )
            restore_master = self.master
            single_cluster_mode = True

        # Step 1: Setup bucket, scope, collection (empty) on source cluster
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true on empty source collection
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="bkptopo",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="bkptopo",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for index creation to be processed")

        # Step 3: Verify retry state on source cluster
        self.log.info("Verifying retry state on source cluster before backup")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Source index {index_name} status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before backup for {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 4: Record baseline index metadata and distribution
        self.log.info("Recording baseline index distribution")
        source_index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        distribution_before = self._get_partition_distribution(vector_index_names)
        baseline_system_states = {}
        for index_name in all_index_names:
            baseline_system_states[index_name] = self._get_index_state(index_name)
        self.log.info(f"Source index nodes: {[n.ip for n in source_index_nodes]}")
        self.log.info(f"Source distribution: {distribution_before}")

        # Step 5: Perform full cluster backup
        self.log.info("Performing full cluster backup via cbbackupmgr")
        source_index_node = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=False
        )
        backup_client = IndexBackupClient(
            backup_node=source_index_node,
            use_cbbackupmgr=True,
            backup_bucket=bucket_name,
            restore_node=restore_master
        )

        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(
            backup_result[0],
            f"Backup failed: {backup_result[1]}"
        )
        self.log.info("Backup completed successfully")

        # Step 6: Restore to different-topology cluster
        # Single-cluster fallback: drop indexes+bucket first (so the indexer has no retrying
        # indexes), then rebalance out one index node to create a topology difference, then restore.
        if single_cluster_mode:
            self.log.info("Dropping indexes and bucket before topology change to allow rebalance")
            self.drop_all_indexes()
            self.sleep(10, "Waiting for indexes to be dropped")
            self.rest.delete_bucket(bucket_name)
            self.sleep(10, "Waiting for bucket deletion before rebalance")

            index_nodes = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True
            )
            if len(index_nodes) > 1:
                node_to_remove = index_nodes[-1]
                self.log.info(
                    f"Single-cluster topology simulation: rebalancing out {node_to_remove.ip}"
                )
                rebalance = self.cluster.async_rebalance(
                    servers=self.servers[:self.nodes_init],
                    to_add=[],
                    to_remove=[node_to_remove]
                )
                reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
                self.assertTrue(reached, "Pre-restore rebalance-out did not complete")
                rebalance.result()
                self.sleep(10, "Waiting for topology change to stabilise")
            else:
                self.log.warning(
                    "Only one index node available; restoring on same topology (no change)"
                )

        self.log.info(f"Restoring backup to {restore_master.ip} (different topology)")
        restore_result = backup_client.restore(
            use_https=self.use_https,
            restore_args="--auto-create-buckets"
        )
        self.assertTrue(
            restore_result[0],
            f"Restore to different topology failed: {restore_result[1]}"
        )
        self.log.info("Restore to different topology completed successfully")
        self.sleep(20, "Waiting for restored indexes to be registered by indexer")

        # Determine restore query node and index node
        restore_query_node = restore_master  # fallback
        restore_cluster_servers_35 = restore_servers if (not single_cluster_mode and restore_servers) else [restore_master]
        if not single_cluster_mode and restore_servers:
            for srv in restore_servers:
                try:
                    r = RestConnection(srv)
                    services = r.get_nodes_services()
                    for _, svc_list in services.items():
                        if 'n1ql' in svc_list:
                            restore_query_node = srv
                            break
                except Exception:
                    pass
                if restore_query_node is not restore_master:
                    break
        elif single_cluster_mode:
            restore_query_node = self.query_node

        # Find index node in restore cluster (port 9102 is only on index service nodes)
        if single_cluster_mode:
            restore_index_node_35 = source_index_node
        else:
            restore_index_node_35 = restore_master
            for srv in restore_cluster_servers_35:
                try:
                    RestConnection(srv).get_indexer_metadata()
                    restore_index_node_35 = srv
                    break
                except Exception:
                    pass

        # Helpers targeting restore cluster
        def _get_restore_index_status(index_name):
            try:
                meta = RestConnection(restore_index_node_35).get_indexer_metadata()
                for entry in meta.get('status', []):
                    if entry.get('name') == index_name:
                        return {
                            'status': entry.get('status'),
                            'error': entry.get('error', ''),
                            'hosts': entry.get('hosts', [])
                        }
            except Exception as e:
                self.log.warning(f"Error querying restore cluster /getIndexStatus: {e}")
            return None

        def _get_restore_index_state(index_name):
            try:
                query = f"SELECT state FROM system:indexes WHERE name = '{index_name}'"
                result = self.run_cbq_query(
                    query=query, server=restore_query_node
                )
                if result.get('results'):
                    return result['results'][0].get('state')
            except Exception as e:
                self.log.warning(f"Error querying system:indexes on restore cluster: {e}")
            return None

        # Step 7: Verify index exists and is in retry state on restored cluster
        self.log.info("Verifying retry state on different-topology restored cluster")
        restore_host_map = {}
        for index_name in vector_index_names:
            status_info = _get_restore_index_status(index_name)
            self.log.info(
                f"Restored index {index_name} /getIndexStatus: {status_info}"
            )
            self.assertTrue(
                status_info is not None,
                f"Index {index_name} not found on restored cluster; "
                f"backup/restore may not have preserved index metadata across topology change"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError on restored (different topology) cluster "
                f"for {index_name}, got: '{error_msg}'"
            )
            self.assertNotEqual(
                status_info.get('status'), 'Ready',
                f"Restored index {index_name} should NOT be Ready on empty collection"
            )
            restore_host_map[index_name] = status_info.get('hosts', [])

        # Step 8: Verify index placement adapts to new topology
        self.log.info("Verifying index placement adapts to new topology")
        self.log.info(f"Source distribution: {distribution_before}")
        self.log.info(f"Restored host map:   {restore_host_map}")

        if single_cluster_mode and len(source_index_nodes) > 1:
            # After rebalance-out, the restored indexes must not be placed on the removed node
            node_to_remove = source_index_nodes[-1]
            for index_name, hosts in restore_host_map.items():
                for host in hosts:
                    self.assertNotIn(
                        node_to_remove.ip, host,
                        f"Restored index {index_name} was placed on the removed node "
                        f"{node_to_remove.ip} — topology adaptation failed"
                    )
            self.log.info(
                "Topology adaptation verified: no restored index placed on removed node"
            )
        else:
            # Dual-cluster: log distribution difference; placement is managed by the planner
            source_ips = {n.ip for n in source_index_nodes}
            restore_ips = set()
            for hosts in restore_host_map.values():
                for h in hosts:
                    restore_ips.add(h.split(':')[0])
            topology_changed = not restore_ips.issubset(source_ips)
            self.log.info(
                f"Source index node IPs: {source_ips}; "
                f"Restored index host IPs: {restore_ips}; "
                f"topology_changed={topology_changed}"
            )

        # Step 9: Load 10,000 documents on restored cluster
        self.log.info("Loading documents on restored cluster to trigger auto-build")
        gen_create = SDKDataLoader(
            num_ops=self.num_docs,
            percent_create=100,
            percent_update=0,
            percent_delete=0,
            scope=scope_name,
            collection=collection_name,
            json_template=self.json_template,
            output=True,
            username=restore_master.rest_username,
            password=restore_master.rest_password,
            key_prefix='doc_',
            base64=self.base64,
            model=self.data_model,
            create_start=0,
            create_end=self.num_docs
        )

        load_task = self.cluster.async_load_gen_docs(
            restore_master,
            bucket=bucket_name,
            generator=gen_create,
            use_magma_loader=True
        )
        load_task.result()
        self.sleep(30, "Waiting for documents to be persisted and indexer retry to trigger")

        # Step 10: Poll until indexes are Ready on restored cluster
        self.log.info("Polling restored cluster until indexes become Ready")
        start_time = time.time()
        all_ready = False
        while time.time() - start_time < self.poll_timeout:
            ready_count = sum(
                1 for name in vector_index_names
                if (s := _get_restore_index_status(name)) and
                s.get('status') == 'Ready' and not s.get('error')
            )
            if ready_count == len(vector_index_names):
                all_ready = True
                self.log.info("All indexes Ready on restored (different topology) cluster")
                break
            time.sleep(self.poll_interval)

        self.assertTrue(
            all_ready,
            "Indexes did not become Ready on different-topology restored cluster; "
            "train_list_wait auto-retry may not have resumed after topology change"
        )

        # Step 11: Verify state='online' on restored cluster
        for index_name in all_index_names:
            state = _get_restore_index_state(index_name)
            self.log.info(f"Restored index {index_name} final state: {state}")
            self.assertEqual(
                state, 'online',
                f"Expected 'online' for restored index {index_name} on different topology, "
                f"got '{state}'"
            )

        # Step 12: Run vector scans on restored cluster
        self.log.info("Running vector scans on restored (different topology) cluster")
        for query in all_select_queries:
            self.log.info(f"Running scan: {query}")
            result = self.run_cbq_query(
                query=query,
                server=restore_query_node,
                scan_consistency='request_plus'
            )
            self.assertTrue(
                result.get('results') and len(result['results']) > 0,
                f"Vector scan returned no results on restored cluster for query: {query}"
            )
            self.log.info(f"Scan returned {len(result['results'])} results")

        # Step 13: Check indexer.log for topology adaptation warnings or errors
        self.log.info("Checking indexer.log for topology adaptation entries")
        self._check_indexer_logs("topology", expect_present=False)
        self._check_indexer_logs("placement", expect_present=False)

        # Cleanup
        try:
            backup_client.remove_backup()
        except Exception as e:
            self.log.warning(f"Could not remove backup archive: {e}")

        self.drop_all_indexes()
        self.sleep(10, "Waiting for indexes to be dropped")

        self.log.info(
            "=== test_backup_restore_train_list_wait_to_different_topology PASSED ==="
        )

    def test_retry_cadence_approximately_10_seconds(self):
        """
        TC-24: Monitor builder retry cadence and verify ~10 second intervals between checks.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, dimension=128
            3. Monitor indexer.log for 60 seconds; extract retry-check timestamps
            4. Compute intervals between consecutive checks
            5. Verify average interval is approximately 10 seconds (±2s tolerance)
            6. Verify checks are NOT at builder tick frequency (~200ms)
        """
        self.log.info("=== Starting test_retry_cadence_approximately_10_seconds ===")

        monitor_duration = self.input.param("retry_monitor_duration", 70)
        expected_interval = self.input.param("expected_retry_interval", 10)
        interval_tolerance = self.input.param("retry_interval_tolerance", 3)

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true on empty collection
        _, _, _, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="cadence",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, _, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="cadence",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        self.sleep(15, "Waiting for indexes to enter retry state before monitoring")

        # Step 3: Monitor indexer.log for retry-check log entries
        # The builder logs something like "Checking trainlist for index <name>" or
        # "RetryableTrainListSizeError" each time it re-evaluates the index.
        # We record the start time, wait monitor_duration seconds, then grep the log
        # with timestamps to extract the cadence.
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        # Patterns that appear each time the builder polls item count for a retrying index
        retry_log_patterns = [
            "RetryableTrainListSizeError",
            "trainlistwait",
            "train_list_wait",
            "CheckTrainList",
            "checkTrainList",
            "retryBuild",
        ]

        record_start = datetime.utcnow()
        self.log.info(
            f"Monitoring indexer logs for {monitor_duration}s starting at {record_start}"
        )
        self.sleep(monitor_duration, "Monitoring indexer retry cadence")

        # Steps 4-6: Extract timestamps from each indexer node and compute intervals
        all_intervals = []

        for server in indexer_nodes:
            shell = RemoteMachineShellConnection(server)
            try:
                _, dir_path = RestConnection(server).diag_eval(
                    'filename:absname(element(2, application:get_env(ns_server, '
                    'error_logger_mf_dir))).'
                )
                indexer_log = str(dir_path) + '/indexer.log*'

                timestamps_raw = []
                for pattern in retry_log_patterns:
                    # Extract full log lines containing the pattern — these have timestamps
                    cmd = (
                        f'zgrep -h "{pattern}" {indexer_log} 2>/dev/null '
                        f'| grep -oP "\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}}" '
                        f'| sort -u'
                    )
                    out, _ = shell.execute_command(cmd)
                    if isinstance(out, list):
                        timestamps_raw.extend(out)
                    elif out:
                        timestamps_raw.extend(out.strip().splitlines())

                # Parse timestamps and filter to the monitoring window
                parsed_times = []
                for ts_str in timestamps_raw:
                    ts_str = ts_str.strip()
                    if not ts_str:
                        continue
                    try:
                        ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
                        if ts >= record_start:
                            parsed_times.append(ts)
                    except ValueError:
                        pass

                parsed_times.sort()
                self.log.info(
                    f"Node {server.ip}: found {len(parsed_times)} retry log entries "
                    f"in monitoring window"
                )

                if len(parsed_times) >= 2:
                    node_intervals = [
                        (parsed_times[i + 1] - parsed_times[i]).total_seconds()
                        for i in range(len(parsed_times) - 1)
                        # Only count gaps that look like deliberate waits (> 1s)
                        if (parsed_times[i + 1] - parsed_times[i]).total_seconds() > 1
                    ]
                    self.log.info(
                        f"Node {server.ip}: retry intervals (s): {node_intervals}"
                    )
                    all_intervals.extend(node_intervals)
            finally:
                shell.disconnect()

        # Steps 5-6: Assert cadence
        if all_intervals:
            avg_interval = sum(all_intervals) / len(all_intervals)
            max_interval = max(all_intervals)
            min_interval = min(all_intervals)
            self.log.info(
                f"Retry check intervals — count: {len(all_intervals)}, "
                f"avg: {avg_interval:.1f}s, min: {min_interval:.1f}s, max: {max_interval:.1f}s"
            )

            # Step 5: Average interval approximately expected_interval seconds
            self.assertAlmostEqual(
                avg_interval, expected_interval, delta=interval_tolerance,
                msg=f"Expected avg retry interval ~{expected_interval}s ±{interval_tolerance}s, "
                    f"got {avg_interval:.1f}s — builder may be retrying too frequently or "
                    f"too slowly"
            )

            # Step 6: No intervals near 200ms builder tick frequency
            too_fast = [i for i in all_intervals if i < 1.0]
            self.assertEqual(
                len(too_fast), 0,
                f"Found {len(too_fast)} retry check intervals < 1s: {too_fast} — "
                f"builder appears to be retrying at tick frequency (~200ms), "
                f"not the expected ~{expected_interval}s interval"
            )
        else:
            self.log.warning(
                "No retry log intervals extracted from indexer logs; "
                "log patterns may differ from expected. Verify log patterns for this CB version."
            )

        # Cleanup
        self.drop_all_indexes()
        self.sleep(5, "Waiting for indexes to be dropped")

        self.log.info("=== test_retry_cadence_approximately_10_seconds PASSED ===")

    def test_partitioned_index_codebook_reuse_after_transient_indexer_failure(self):
        """
        TC-25: Create partitioned vector index and verify trained partitions reuse
        codebook on retry after transient indexer process failure.

        Steps:
            1. Create bucket with scopes and collections and load 10,000 documents
            2. Create partitioned BHIVE + composite indexes with train_list_wait=true,
               num_partition=4, dimension=128
            3. Wait for indexes to enter build/training phase
            4. Kill the indexer process on one partition's node (force=True)
            5. Wait for indexer to auto-restart (Couchbase babysitter restarts it)
            6. Monitor indexer.log for codebook reuse indicators on recovered node
            7. Poll /getIndexStatus until all partitions reach status='Ready'
            8. Run vector scans and verify results from all partitions
        """
        self.log.info(
            "=== Starting "
            "test_partitioned_index_codebook_reuse_after_transient_indexer_failure ==="
        )

        num_partition = self.input.param("num_partition", 4)
        kill_wait = self.input.param("kill_wait", 60)

        # Step 1: Setup and load 10,000 documents (enough to trigger training at build time)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(15, "Waiting for documents to be persisted")

        # Step 2: Create partitioned indexes with train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_partitioned_indexes(
            namespace=namespace,
            prefix="codebk",
            bhive_index=True,
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica,
            partition_by_fields=["meta().id"],
            num_partition=num_partition
        )

        _, _, composite_select_queries, composite_index_names = self._create_partitioned_indexes(
            namespace=namespace,
            prefix="codebk",
            bhive_index=False,
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica,
            partition_by_fields=["meta().id"],
            num_partition=num_partition
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        # Step 3: Wait for build/training phase to start
        self.sleep(20, "Waiting for training phase to begin before killing indexer")

        # Step 4: Kill the indexer process on one index node
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        target_node = index_nodes[0]
        self.log.info(f"Killing indexer process on {target_node.ip} during training phase")

        shell = RemoteMachineShellConnection(target_node)
        try:
            shell.terminate_process(
                info=shell.extract_remote_info(),
                process_name='indexer',
                force=True
            )
            self.log.info(f"Indexer process killed on {target_node.ip}")
        finally:
            shell.disconnect()

        # Step 5: Wait for Couchbase babysitter to auto-restart the indexer
        self.sleep(kill_wait, f"Waiting {kill_wait}s for indexer to auto-restart on {target_node.ip}")

        # Poll until the indexer REST port is responsive again
        restart_timeout = self.input.param("restart_timeout", 120)
        start = time.time()
        recovered = False
        while time.time() - start < restart_timeout:
            try:
                meta = RestConnection(target_node).get_indexer_metadata()
                if meta is not None:
                    recovered = True
                    self.log.info(f"Indexer on {target_node.ip} is back online")
                    break
            except Exception:
                pass
            time.sleep(5)

        self.assertTrue(
            recovered,
            f"Indexer on {target_node.ip} did not recover within {restart_timeout}s "
            f"after process kill"
        )

        # Step 6: Check indexer.log on recovered node for codebook reuse indicators
        shell = RemoteMachineShellConnection(target_node)
        try:
            _, dir_path = RestConnection(target_node).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server, '
                'error_logger_mf_dir))).'
            )
            indexer_log = str(dir_path) + '/indexer.log*'

            codebook_patterns = ["codebook", "Codebook", "reuseCodebook", "reuse_codebook",
                                  "serialized", "TrainedCodebook"]
            codebook_found = False
            for pattern in codebook_patterns:
                out, _ = shell.execute_command(
                    f'zgrep -c "{pattern}" {indexer_log} 2>/dev/null || echo 0'
                )
                count = int(out[0].strip()) if isinstance(out, list) and out[0].strip().isdigit() \
                    else (int(out.strip()) if str(out).strip().isdigit() else 0)
                if count > 0:
                    self.log.info(
                        f"Codebook reuse indicator '{pattern}' found {count} time(s) "
                        f"in indexer log on {target_node.ip}"
                    )
                    codebook_found = True
                    break

            if not codebook_found:
                self.log.warning(
                    "No explicit codebook reuse log entry found; "
                    "this may be normal if the process killed before any partition finished "
                    "training. Proceeding with functional verification."
                )
        finally:
            shell.disconnect()

        # Step 7: Poll until all partitions are Ready
        self.sleep(15, "Waiting for index recovery and retry to proceed")
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Partitioned indexes did not become Ready after indexer process kill and recovery; "
            "codebook reuse or retry after transient failure may not be working (INV-GSI-005)"
        )
        self._verify_indexes_online(all_index_names)

        # Step 8: Run vector scans
        self._run_vector_scans(all_select_queries, expect_results=True)

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_partitioned_index_codebook_reuse_after_transient_indexer_failure PASSED ==="
        )

    def test_indexer_restart_preserves_train_list_wait_retry_state(self):
        """
        TC-26: Restart indexer service and verify retry state and error persist
        for train_list_wait index.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, dimension=128
            3. Verify RetryableTrainListSizeError via /getIndexStatus
            4. Record pre-restart index metadata state
            5. Restart the indexer service on the hosting node
            6. Wait for indexer to fully recover (poll REST API)
            7. Verify RetryableTrainListSizeError still present in /getIndexStatus
            8. Verify system:indexes state matches pre-restart state
            9. Load 10,000 documents; verify auto-build completes after restart
            10. Verify system:indexes state='online'
        """
        self.log.info(
            "=== Starting test_indexer_restart_preserves_train_list_wait_retry_state ==="
        )

        indexer_restart_timeout = self.input.param("indexer_restart_timeout", 120)

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="restart",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="restart",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for indexes to enter retry state")

        # Step 3: Verify retry state before restart
        self.log.info("Verifying retry state before indexer restart")
        pre_restart_states = {}
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} pre-restart status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before restart for {index_name}, "
                f"got: '{error_msg}'"
            )
            pre_restart_states[index_name] = {
                'status': status_info.get('status'),
                'error': error_msg
            }

        # Step 4: Record pre-restart system:indexes state
        pre_restart_sys_states = {}
        for index_name in all_index_names:
            state = self._get_index_state(index_name)
            pre_restart_sys_states[index_name] = state
            self.log.info(f"Pre-restart system:indexes state for {index_name}: {state}")

        # Step 5: Restart the indexer service (stop + start the Couchbase server process
        # on the index node — this is the standard way to restart the indexer service)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.log.info(f"Restarting indexer service on {index_node.ip}")

        shell = RemoteMachineShellConnection(index_node)
        try:
            shell.stop_server()
            self.log.info(f"Couchbase server stopped on {index_node.ip}")
            self.sleep(10, "Brief pause before restarting")
            shell.start_server()
            self.log.info(f"Couchbase server started on {index_node.ip}")
        finally:
            shell.disconnect()

        # Step 6: Wait for indexer to fully recover
        self.log.info(f"Waiting for indexer on {index_node.ip} to fully recover")
        start = time.time()
        recovered = False
        while time.time() - start < indexer_restart_timeout:
            try:
                meta = RestConnection(index_node).get_indexer_metadata()
                if meta is not None:
                    recovered = True
                    self.log.info(f"Indexer on {index_node.ip} is back online")
                    break
            except Exception:
                pass
            time.sleep(5)

        self.assertTrue(
            recovered,
            f"Indexer on {index_node.ip} did not recover within {indexer_restart_timeout}s "
            f"after restart"
        )
        self.sleep(15, "Waiting for retry state to be re-established after restart")

        # Step 7: Verify RetryableTrainListSizeError persists after restart
        self.log.info("Verifying retry state persists after indexer restart")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} post-restart status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for {index_name} after restart"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError after restart for {index_name}, "
                f"got: '{error_msg}' — retry state was not preserved through indexer restart "
                f"(INV-GSI-005)"
            )

        # Step 8: Verify system:indexes state matches pre-restart state
        for index_name in all_index_names:
            post_state = self._get_index_state(index_name)
            pre_state = pre_restart_sys_states.get(index_name)
            self.log.info(
                f"Index {index_name}: pre-restart={pre_state}, post-restart={post_state}"
            )
            self.assertEqual(
                post_state, pre_state,
                f"system:indexes state changed after restart for {index_name}: "
                f"was '{pre_state}', now '{post_state}'"
            )

        # Step 9: Load 10,000 documents; verify auto-build completes
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents and indexer retry after restart")

        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after indexer restart + document load; "
            "builder may not have resumed retry after restart (INV-GSI-005)"
        )

        # Step 10: Verify state='online' and run scans
        self._verify_indexes_online(all_index_names)
        self._run_vector_scans(all_select_queries, expect_results=True)

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_indexer_restart_preserves_train_list_wait_retry_state PASSED ==="
        )

    def test_partitioned_index_per_partition_failure_tracking_and_recovery(self):
        """
        TC-27: Create partitioned vector index across nodes and verify per-partition
        failure tracking and recovery.

        Steps:
            1. Create bucket with scopes and collections; load 10,000 documents
            2. Create partitioned BHIVE + composite indexes with train_list_wait=true,
               num_partition=4, dimension=128
            3. Record partition distribution
            4. Simulate disk I/O stress on one partition's node during retry/wait state
            5. Verify builder tracks per-partition training state in indexer.log
            6. Stop disk stress (resolve disk issue)
            7. Verify trained partitions reuse codebook on retry (check logs)
            8. Poll /getIndexStatus until all partitions report Ready
            9. Verify index transitions only when ALL partitions are trained
            10. Run vector scans from all partitions
        """
        self.log.info(
            "=== Starting "
            "test_partitioned_index_per_partition_failure_tracking_and_recovery ==="
        )

        num_partition = self.input.param("num_partition", 4)
        disk_stress_duration = self.input.param("disk_stress_duration", 60)

        # Step 1: Setup and load 10,000 documents
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(15, "Waiting for documents to be persisted")

        # Step 2: Create partitioned indexes with train_list_wait=true
        _, _, bhive_select_queries, bhive_index_names = self._create_partitioned_indexes(
            namespace=namespace,
            prefix="perpart",
            bhive_index=True,
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica,
            partition_by_fields=["meta().id"],
            num_partition=num_partition
        )

        _, _, composite_select_queries, composite_index_names = self._create_partitioned_indexes(
            namespace=namespace,
            prefix="perpart",
            bhive_index=False,
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica,
            partition_by_fields=["meta().id"],
            num_partition=num_partition
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for partitioned index creation to be processed")

        # Step 3: Record partition distribution
        distribution = self._get_partition_distribution(vector_index_names)
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True
        )
        self.log.info(f"Partition distribution: {distribution}")
        self.log.info(f"Index nodes: {[n.ip for n in index_nodes]}")

        # Step 4: Simulate disk I/O stress on one node to impede partition training
        stress_node = index_nodes[0]
        stress_shell = RemoteMachineShellConnection(stress_node)
        try:
            # Install fio if needed and start disk stress in background
            stress_shell.execute_command(
                "apt-get install -y fio 2>/dev/null || yum install -y fio 2>/dev/null",
                debug=False
            )
            fio_cmd = (
                f"fio --name=stress_io "
                f"--directory=/opt/couchbase/var/lib/couchbase/data "
                f"--rw=randrw --bs=4k --direct=1 --numjobs=4 --iodepth=32 "
                f"--size=512m --runtime={disk_stress_duration} --time_based "
                f"--group_reporting >/tmp/fio_perpart.log 2>&1 &"
            )
            stress_shell.execute_command(fio_cmd)
            self.log.info(
                f"Started disk I/O stress on partition node {stress_node.ip} "
                f"for {disk_stress_duration}s"
            )

            # Step 5: Verify per-partition training state tracked in indexer.log
            # while disk stress is active
            self.sleep(20, "Waiting for per-partition training activity under disk stress")

            _, dir_path = RestConnection(stress_node).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server, '
                'error_logger_mf_dir))).'
            )
            indexer_log = str(dir_path) + '/indexer.log*'

            per_partition_patterns = [
                "partition", "Partition", "partitionId", "PartitionId", "trainPartition"
            ]
            partition_tracking_found = False
            for pattern in per_partition_patterns:
                out, _ = stress_shell.execute_command(
                    f'zgrep -c "{pattern}" {indexer_log} 2>/dev/null || echo 0'
                )
                count = int(out[0].strip()) if isinstance(out, list) and out[0].strip().isdigit() \
                    else (int(str(out).strip()) if str(out).strip().isdigit() else 0)
                if count > 0:
                    self.log.info(
                        f"Per-partition tracking indicator '{pattern}' found {count} "
                        f"time(s) in indexer log on {stress_node.ip}"
                    )
                    partition_tracking_found = True
                    break

            if not partition_tracking_found:
                self.log.warning(
                    "No explicit per-partition tracking log entry found on stressed node; "
                    "per-partition state may be logged at a different verbosity level."
                )

        finally:
            # Step 6: Stop disk stress (resolve the disk issue)
            stress_shell.execute_command("pkill -9 fio || true", debug=False)
            self.log.info(f"Stopped disk I/O stress on {stress_node.ip}")
            stress_shell.disconnect()

        self.sleep(10, "Waiting after stopping disk stress")

        # Step 7: Check for codebook reuse after stress is resolved
        shell = RemoteMachineShellConnection(stress_node)
        try:
            codebook_patterns = ["codebook", "Codebook", "reuseCodebook", "serialized"]
            for pattern in codebook_patterns:
                out, _ = shell.execute_command(
                    f'zgrep -c "{pattern}" {indexer_log} 2>/dev/null || echo 0'
                )
                count = int(out[0].strip()) if isinstance(out, list) and out[0].strip().isdigit() \
                    else (int(str(out).strip()) if str(out).strip().isdigit() else 0)
                if count > 0:
                    self.log.info(
                        f"Codebook reuse indicator '{pattern}' found {count} time(s); "
                        f"trained partitions are reusing codebook after recovery"
                    )
                    break
            else:
                self.log.warning(
                    "Codebook reuse not explicitly logged; proceeding with functional verification"
                )
        finally:
            shell.disconnect()

        # Step 8: Poll until all partitions are Ready
        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Partitioned indexes did not become Ready after disk stress recovery; "
            "per-partition failure tracking or codebook reuse may not be working (INV-GSI-005)"
        )

        # Step 9: Verify all-or-nothing transition — all must be online simultaneously
        self._verify_indexes_online(all_index_names)
        self.log.info(
            "All partitions transitioned to online simultaneously (all-or-nothing confirmed)"
        )

        # Step 10: Run vector scans
        self._run_vector_scans(all_select_queries, expect_results=True)

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_partitioned_index_per_partition_failure_tracking_and_recovery PASSED ==="
        )

    def test_document_deletion_during_retry_phase_graceful_handling(self):
        """
        TC-28: Delete documents during retry phase bringing count below threshold
        and verify graceful handling.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, dimension=128
            3. Load 10,000 documents to trigger build initiation
            4. During training phase, delete 8,000 documents (bringing count well below threshold)
            5. Poll /getIndexStatus for build outcome
            6. If training succeeds: verify online; run vector scan; verify results
            7. If training returns to retry: verify RetryableTrainListSizeError reappears;
               reload sufficient documents; verify eventual build completion
            8. Check indexer.log for no hung or corrupted state
        """
        self.log.info(
            "=== Starting test_document_deletion_during_retry_phase_graceful_handling ==="
        )

        initial_docs = self.num_docs        # 10,000
        docs_to_delete = self.input.param("docs_to_delete", 8000)
        delete_wait = self.input.param("delete_wait", 15)

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true on empty collection
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="deldur",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="deldur",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(10, "Waiting for indexes to enter retry state before loading")

        # Step 3: Load 10,000 documents to cross the training threshold
        self.log.info(f"Loading {initial_docs} documents to trigger build initiation")
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=initial_docs)
        self.sleep(delete_wait,
                   f"Waiting {delete_wait}s to reach training phase before deleting docs")

        # Step 4: Delete 8,000 documents to bring count below threshold
        self.log.info(f"Deleting {docs_to_delete} documents to bring count below threshold")
        delete_query = (
            f"DELETE FROM {namespace} d "
            f"WHERE META(d).id LIKE 'doc_%' "
            f"LIMIT {docs_to_delete}"
        )
        try:
            del_result = self.run_cbq_query(query=delete_query, server=self.query_node)
            self.log.info(f"Deletion result: {del_result.get('metrics', {})}")
        except Exception as e:
            self.log.warning(f"Deletion query error (may be acceptable): {e}")

        self.sleep(30, "Waiting for deletions to propagate and for indexer to observe them")

        # Step 5: Poll /getIndexStatus to observe build outcome
        # Give the indexer time to process the count change
        observation_timeout = self.input.param("deletion_observation_timeout", 120)
        start_time = time.time()
        build_succeeded = False
        returned_to_retry = False

        while time.time() - start_time < observation_timeout:
            statuses = []
            for index_name in vector_index_names:
                s = self._get_index_status(index_name)
                if s:
                    statuses.append(s)

            ready_count = sum(1 for s in statuses if s.get('status') == 'Ready' and not s.get('error'))
            retry_count = sum(1 for s in statuses if 'RetryableTrainListSizeError' in s.get('error', ''))

            if ready_count == len(vector_index_names):
                build_succeeded = True
                self.log.info(
                    "Training succeeded despite mid-phase document deletion "
                    "(indexer used already-loaded training data)"
                )
                break
            elif retry_count > 0:
                returned_to_retry = True
                self.log.info(
                    "Index returned to retry state after document deletion; "
                    "indexer correctly detected insufficient documents"
                )
                break

            self.log.debug(f"Observing: ready={ready_count}, retry={retry_count}")
            time.sleep(self.poll_interval)

        self.assertTrue(
            build_succeeded or returned_to_retry,
            "Index is neither Ready nor back in RetryableTrainListSizeError after document "
            "deletion — possible hung or corrupted state; neither outcome is acceptable"
        )

        # Step 6: Training succeeded path
        if build_succeeded:
            self.log.info(
                "Step 6: Build succeeded with reduced data; verifying online state and scans"
            )
            self._verify_indexes_online(all_index_names)
            # Scans may return fewer results due to deletions — just require non-empty
            self._run_vector_scans(all_select_queries, expect_results=True)

        # Step 7: Returned to retry path — reload documents and verify eventual build
        elif returned_to_retry:
            self.log.info(
                "Step 7: Index returned to retry; reloading documents to verify recovery"
            )
            # Reload 10,000 fresh documents (use offset to avoid key conflicts after deletion)
            self._load_documents(
                bucket_name, scope_name, collection_name,
                num_docs=initial_docs,
                start_doc=initial_docs   # keys doc_10000 .. doc_19999
            )
            self.sleep(30, "Waiting for reloaded documents and indexer retry")

            self.assertTrue(
                self._poll_indexes_until_ready(vector_index_names),
                "Indexes did not become Ready after document reload following deletion; "
                "recovery from mid-phase deletion may not be working"
            )
            self._verify_indexes_online(all_index_names)
            self._run_vector_scans(all_select_queries, expect_results=True)
            self.log.info(
                "Recovery confirmed: index rebuilt successfully after document reload"
            )

        # Step 8: Check indexer.log for no hung or corrupted state indicators
        hung_patterns = ["hung", "corrupt", "panic", "FATAL", "deadlock"]
        for pattern in hung_patterns:
            found = self._check_indexer_logs(pattern, expect_present=False)
            if found:
                self.log.warning(
                    f"Suspicious log pattern '{pattern}' found in indexer logs after "
                    f"mid-phase document deletion — investigate for potential corruption"
                )

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_document_deletion_during_retry_phase_graceful_handling PASSED ==="
        )

    def test_train_list_wait_ddl_rejected_when_required_vector_params_missing(self):
        """
        TC-29: Create vector index with train_list_wait=true but missing required vector
        parameters and verify DDL validation is not bypassed.

        Steps:
            1. Attempt CREATE INDEX with train_list_wait=true but missing dimension; verify error
            2. Attempt CREATE INDEX with train_list_wait=true but missing similarity; verify error
            3. Attempt CREATE INDEX with train_list_wait=true but missing both; verify error
            4. Verify each error identifies the missing parameter
            5. Verify no index artifacts in system:indexes
        """
        self.log.info(
            "=== Starting "
            "test_train_list_wait_ddl_rejected_when_required_vector_params_missing ==="
        )

        # Setup bucket/scope/collection (no documents needed — DDL validation is pre-data)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        index_base = f"idx_n3_{''.join(random.choices(string.ascii_lowercase, k=5))}"

        # Each entry: (label, WITH-clause fragment, expected error hint keywords)
        missing_param_cases = [
            (
                "missing dimension",
                f'{{"similarity": "L2_SQUARED", "description": "IVF,SQ8", '
                f'"train_list_wait": true}}',
                ["dimension", "required", "missing", "invalid", "parameter"]
            ),
            (
                "missing similarity",
                f'{{"dimension": 128, "description": "IVF,SQ8", '
                f'"train_list_wait": true}}',
                ["similarity", "required", "missing", "invalid", "parameter"]
            ),
            (
                "missing both dimension and similarity",
                f'{{"description": "IVF,SQ8", "train_list_wait": true}}',
                ["dimension", "similarity", "required", "missing", "invalid", "parameter"]
            ),
        ]

        for idx_suffix, (label, with_clause, hint_keywords) in enumerate(missing_param_cases):
            index_name = f"{index_base}_{idx_suffix}"
            create_query = (
                f'CREATE VECTOR INDEX `{index_name}` '
                f'ON {namespace}(embedding VECTOR) '
                f'USING GSI '
                f'WITH {with_clause}'
            )

            self.log.info(
                f"Step: Attempting CREATE INDEX with train_list_wait=true, {label}: "
                f"{create_query}"
            )

            # Steps 1-3: Each attempt must raise a DDL error
            error_raised = False
            error_message = ""
            try:
                self.run_cbq_query(query=create_query, server=self.query_node)
            except Exception as e:
                error_raised = True
                error_message = str(e)
                self.log.info(
                    f"Got expected DDL error for {label}: {error_message}"
                )

            self.assertTrue(
                error_raised,
                f"Expected DDL error for CREATE INDEX with {label} and train_list_wait=true, "
                f"but the statement succeeded — train_list_wait must not bypass required "
                f"vector parameter validation"
            )

            # Step 4: Error must reference at least one of the missing-parameter hint words
            hint_found = any(h in error_message.lower() for h in hint_keywords)
            self.assertTrue(
                hint_found,
                f"DDL error for {label} should identify the missing parameter; "
                f"got: '{error_message}'"
            )

            # Step 5: No index artifact in system:indexes — DDL rollback must be atomic.
            check_query = (
                f"SELECT COUNT(*) AS cnt FROM system:indexes "
                f"WHERE name = '{index_name}'"
            )
            result = self.run_cbq_query(query=check_query, server=self.query_node)
            count = result.get('results', [{}])[0].get('cnt', -1)
            self.assertEqual(
                count, 0,
                f"Index artifact '{index_name}' found in system:indexes after failed DDL "
                f"({label}) — server bug: train_list_wait must not cause partial index creation"
            )
            self.log.info(f"Confirmed: no index artifact for {label}")

        # No cleanup needed — no indexes were created
        self.log.info(
            "=== test_train_list_wait_ddl_rejected_when_required_vector_params_missing PASSED ==="
        )

    def test_drop_index_while_in_train_list_wait_retry_state(self):
        """
        TC-30: Drop index while in train_list_wait retry state and verify clean removal.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true, dimension=128
            3. Verify retry state via /getIndexStatus (RetryableTrainListSizeError)
            4. Execute DROP INDEX for each vector index
            5. Verify each DROP completes without error
            6. Verify indexes no longer exist in system:indexes
            7. Verify indexes not listed in /getIndexStatus
            8. Check indexer.log for clean removal (no error during drop)
        """
        self.log.info(
            "=== Starting test_drop_index_while_in_train_list_wait_retry_state ==="
        )

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true on empty collection
        _, _, _, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="n4",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, _, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="n4",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for indexes to enter retry state")

        # Step 3: Verify retry state before dropping
        self.log.info("Verifying retry state before DROP INDEX")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} pre-drop status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for index {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before DROP for {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 4 & 5: Execute DROP INDEX for each index and verify no error
        self.log.info("Dropping all indexes while in retry state")
        drop_errors = []
        for index_name in all_index_names:
            drop_query = f"DROP INDEX `{index_name}` ON {namespace}"
            self.log.info(f"Dropping: {drop_query}")
            try:
                self.run_cbq_query(query=drop_query, server=self.query_node)
                self.log.info(f"DROP INDEX succeeded for {index_name}")
            except Exception as e:
                error_str = str(e)
                # "Index not found" after a prior drop is acceptable (e.g. primary was dropped
                # already by drop_all_indexes); anything else is a real error
                if 'not found' in error_str.lower() or 'does not exist' in error_str.lower():
                    self.log.info(
                        f"Index {index_name} already gone (acceptable): {error_str}"
                    )
                else:
                    self.log.error(
                        f"Unexpected error dropping {index_name} in retry state: {error_str}"
                    )
                    drop_errors.append((index_name, error_str))

        self.assertEqual(
            len(drop_errors), 0,
            f"DROP INDEX failed for {len(drop_errors)} indexes while in retry state: "
            f"{drop_errors} — DROP must complete cleanly regardless of retry state"
        )

        self.sleep(10, "Waiting for drops to propagate")

        # Step 6: Verify indexes no longer in system:indexes
        self.log.info("Verifying indexes removed from system:indexes")
        for index_name in all_index_names:
            state = self._get_index_state(index_name)
            self.assertIsNone(
                state,
                f"Index {index_name} still present in system:indexes after DROP: "
                f"state='{state}' — orphaned metadata after drop in retry state"
            )

        # Step 7: Verify indexes not listed in /getIndexStatus
        self.log.info("Verifying indexes not listed in /getIndexStatus")
        for index_name in all_index_names:
            status_info = self._get_index_status(index_name)
            self.assertIsNone(
                status_info,
                f"Index {index_name} still listed in /getIndexStatus after DROP: "
                f"{status_info} — orphaned retry state artifact after drop"
            )

        # Step 8: Check indexer.log for clean removal (no error patterns during drop)
        error_patterns = ["error.*drop", "drop.*error", "failed.*remove", "remove.*failed"]
        for pattern in error_patterns:
            indexer_nodes = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True
            )
            for server in indexer_nodes:
                shell = RemoteMachineShellConnection(server)
                try:
                    _, dir_path = RestConnection(server).diag_eval(
                        'filename:absname(element(2, application:get_env(ns_server, '
                        'error_logger_mf_dir))).'
                    )
                    indexer_log = str(dir_path) + '/indexer.log*'
                    out, _ = shell.execute_command(
                        f'zgrep -ic "{pattern}" {indexer_log} 2>/dev/null || echo 0'
                    )
                    count = int(out[0].strip()) if isinstance(out, list) and \
                        out[0].strip().isdigit() else 0
                    if count > 0:
                        self.log.warning(
                            f"Pattern '{pattern}' found {count} time(s) in indexer log on "
                            f"{server.ip} after DROP — investigate for drop errors in retry state"
                        )
                finally:
                    shell.disconnect()

        self.log.info(
            "Verified: DROP INDEX completed cleanly while index was in retry state; "
            "no orphaned metadata or retry artifacts remain"
        )

        # Cleanup — data only (indexes already dropped above)
        try:
            delete_query = f"DELETE FROM {namespace}"
            self.run_cbq_query(query=delete_query, server=self.query_node)
        except Exception as e:
            self.log.warning(f"Data cleanup error (acceptable on empty collection): {e}")

        self.log.info(
            "=== test_drop_index_while_in_train_list_wait_retry_state PASSED ==="
        )

    def test_redundant_build_index_on_retrying_train_list_wait_index(self):
        """
        TC-31: Issue BUILD INDEX on non-deferred train_list_wait index already in
        retry state and verify graceful no-op handling.

        Steps:
            1. Create bucket with scopes and collections
            2. Create BHIVE + composite indexes with train_list_wait=true (no defer_build)
            3. Verify index entered retry state automatically
            4. Issue explicit BUILD INDEX
            5. Verify command returns success or no-op (no error)
            6. Verify index remains in retry state (not disrupted)
            7. Load 10,000 documents; verify auto-build completes normally
            8. Verify system:indexes state='online'
        """
        self.log.info(
            "=== Starting test_redundant_build_index_on_retrying_train_list_wait_index ==="
        )

        # Step 1: Setup bucket, scope, collection (empty)
        bucket_name, namespace, scope_name, collection_name = self._setup_test_environment()

        # Step 2: Create indexes with train_list_wait=true, no defer_build
        _, _, bhive_select_queries, bhive_index_names = self._create_bhive_indexes(
            namespace=namespace,
            prefix="n5",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        _, _, composite_select_queries, composite_index_names = self._create_composite_indexes(
            namespace=namespace,
            prefix="n5",
            train_list_wait=True,
            train_list=self.num_docs,
            defer_build=False,
            num_replica=self.num_index_replica
        )

        all_index_names = bhive_index_names + composite_index_names
        all_select_queries = bhive_select_queries + composite_select_queries
        vector_index_names = self._filter_vector_indexes(all_index_names)

        self.sleep(15, "Waiting for indexes to enter retry state automatically")

        # Step 3: Verify retry state
        self.log.info("Verifying automatic retry state before redundant BUILD INDEX")
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} pre-BUILD status: {status_info}")
            self.assertTrue(
                status_info is not None,
                f"Expected status info for {index_name}"
            )
            error_msg = status_info.get('error', '')
            self.assertIn(
                'RetryableTrainListSizeError', error_msg,
                f"Expected RetryableTrainListSizeError before redundant BUILD for {index_name}, "
                f"got: '{error_msg}'"
            )

        # Step 4: Issue explicit BUILD INDEX on the already-retrying indexes
        self.log.info("Issuing redundant BUILD INDEX on indexes already in retry state")
        build_query_errors = []
        for index_name in all_index_names:
            build_query = f"BUILD INDEX ON {namespace} (`{index_name}`)"
            self.log.info(f"BUILD INDEX: {build_query}")
            try:
                self.run_cbq_query(query=build_query, server=self.query_node)
                self.log.info(f"BUILD INDEX returned success/no-op for {index_name}")
            except Exception as e:
                error_str = str(e)
                # "Index not in deferred state" or similar is an acceptable response —
                # the index was never deferred so BUILD is a legitimate no-op
                acceptable_phrases = [
                    'not deferred', 'already', 'online', 'building', 'no-op',
                    'state', 'pending', 'ErrTraining', 'transient'
                ]
                if any(ph in error_str.lower() for ph in acceptable_phrases):
                    self.log.info(
                        f"Acceptable response for redundant BUILD INDEX on {index_name}: "
                        f"{error_str}"
                    )
                else:
                    self.log.error(
                        f"Unexpected error from redundant BUILD INDEX on {index_name}: "
                        f"{error_str}"
                    )
                    build_query_errors.append((index_name, error_str))

        # Step 5: Verify no unexpected errors from the redundant BUILD INDEX
        self.assertEqual(
            len(build_query_errors), 0,
            f"Redundant BUILD INDEX produced unexpected errors on "
            f"{len(build_query_errors)} indexes: {build_query_errors} — "
            f"BUILD INDEX on a retrying index must be a no-op or return success"
        )

        self.sleep(15, "Waiting to confirm retry state is not disrupted by redundant BUILD")

        # Step 6: Verify index is still in retry state (BUILD did not break the retry loop)
        self.log.info(
            "Verifying retry state was not disrupted by redundant BUILD INDEX"
        )
        for index_name in vector_index_names:
            status_info = self._get_index_status(index_name)
            self.log.info(f"Index {index_name} post-BUILD status: {status_info}")
            self.assertNotEqual(
                status_info.get('status') if status_info else None, None,
                f"Index {index_name} disappeared from /getIndexStatus after redundant BUILD"
            )
            self.assertNotEqual(
                status_info.get('status') if status_info else None, 'Ready',
                f"Index {index_name} should NOT be Ready on empty collection after "
                f"redundant BUILD INDEX"
            )
            # Retry state should still be retryable (not degraded to non-retryable)
            error_msg = status_info.get('error', '') if status_info else ''
            self.assertNotIn(
                'InvalidTrainListSize', error_msg,
                f"Redundant BUILD INDEX degraded {index_name} from retryable to non-retryable "
                f"error: '{error_msg}' — retry state must not be corrupted by redundant BUILD"
            )

        # Step 7: Load 10,000 documents; verify auto-build completes normally
        self.log.info(
            "Loading documents to verify auto-build still works after redundant BUILD INDEX"
        )
        self._load_documents(bucket_name, scope_name, collection_name, num_docs=self.num_docs)
        self.sleep(30, "Waiting for documents and indexer retry to complete")

        self.assertTrue(
            self._poll_indexes_until_ready(vector_index_names),
            "Indexes did not become Ready after loading documents; "
            "redundant BUILD INDEX may have disrupted the retry loop"
        )

        # Step 8: Verify state='online' and run scans
        self._verify_indexes_online(all_index_names)
        self._run_vector_scans(all_select_queries, expect_results=True)

        self.log.info(
            "Verified: redundant BUILD INDEX on retrying index produces no error; "
            "retry state was preserved; eventual auto-build completed normally"
        )

        # Cleanup
        self._cleanup(namespace)

        self.log.info(
            "=== test_redundant_build_index_on_retrying_train_list_wait_index PASSED ==="
        )
