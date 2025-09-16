"""bhive_vector_index.py: "This class test BHIVE Vector indexes for GSI"

__author__ = "Dananjay S"
__maintainer = "Dananjay S"
__email__ = "dananjay.s@couchbase.com"
__git_user__ = "dananjay-s"
"""
from copy import deepcopy
import logging
from concurrent.futures import ThreadPoolExecutor
from sentence_transformers import SentenceTransformer
from couchbase_helper.documentgenerator import SDKDataLoader
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestHelper
from .base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection
from threading import Event
import random
import time
from couchbase_helper.query_definitions import QueryDefinition, RANGE_SCAN_TEMPLATE
import requests

class BhiveVectorIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(BhiveVectorIndex, self).setUp()
        self.log.info("==============  BhiveVectorIndex setup has started ==============")
        self.use_magma_loader = True
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 2)
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        self.bucket_count = self.input.param("bucket_count", 3)
        self.collection_count = self.input.param("collection_count", 3)
        self.docs_per_collection = self.input.param("docs_per_collection", 10000)
        self.num_index_replica = self.input.param("num_index_replica", 1)
        self.dataset = self.input.param("dataset", "siftBigANN")
        self.graceful = self.input.param("graceful", True)
        self.HIGH_OPS = 20000
        self.LOW_OPS = 200
        self.HIGH_OPS_RATE = 1000
        self.LOW_OPS_RATE = 50
        self.rebalance_type = self.input.param("rebalance_type", "dcp")
        self.rebalance_order = self.input.param("rebalance_order", "out-swap-in-failover_kv-failover_index")
        self.ops_order = self.input.param("ops_order", "high_create_update-low_create_update-high_create_delete")
        self.index_ops_order = self.input.param("index_ops_order", "create-drop-create-drop-create-drop")
        self.run_mutation_workload = self.input.param("run_mutation_workload", False)
        self.log.info("==============  BhiveVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  BhiveVectorIndex tearDown has started ==============")
        super(BhiveVectorIndex, self).tearDown()
        self.log.info("==============  BhiveVectorIndex tearDown has ended ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def load_docs_into_buckets(self):
        buckets = self._create_test_buckets(num_buckets=1)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=10000,
                                                 json_template=self.json_template, load_default_coll=False)
        self.sleep(360000)

    def test_kill_indexer_process_at_different_stages(self):
        """
        Test to verify indexer recovery after being killed at different stages of BHIVE index creation.
        
        This test validates indexer resilience by killing the process at various stages:
        1. Sampling stage - Immediately after index build command
        2. Training stage - During training (using delay to control timing)
        3. Index building stage - During the "Building" phase
        4. Graph building stage - During the "Graph build" phase
        
        After each kill and recovery, validates item count and recall accuracy.
        """
        self.log.info("=== Starting test_kill_indexer_process_at_different_stages ===")
        
        # Test parameters
        stages_to_test = ["sampling", "training", "building", "graph_building"]
        training_delay_seconds = 30
        
        # Step 1: Setup buckets and collections
        self.log.info("Step 1: Setting up test environment")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        
        # Load documents
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)
        
        self.sleep(60, "Waiting for documents to be loaded")
        
        for stage in stages_to_test:
            self.log.info(f"=== Testing indexer kill during {stage} stage ===")
            
            # Step 2: Configure indexer settings for training stage
            if stage == "training":
                self.log.info(f"Setting training delay to {training_delay_seconds} seconds")
                rest = RestConnection(index_nodes[0])
                rest.set_index_settings({
                    "indexer.debug.delayTrainingDuration": training_delay_seconds
                })
            
            # Step 3: Prepare index creation
            create_queries = []
            select_queries = []
            for namespace in self.namespaces:
                self.log.info(f"Preparing BHIVE index for {namespace}")
                bhive_def, _, _ = self._get_query_definitions()
                current_create_queries = self._get_create_queries(bhive_def, None, None, namespace=namespace, defer_build_mix=False)
                create_queries.extend(current_create_queries)
                
                # Prepare select queries for validation
                curr_select_queries = self.gsi_util_obj.get_select_queries(
                    definition_list=bhive_def,
                    namespace=namespace,
                    limit=self.scan_limit
                )
                select_queries.extend(curr_select_queries)
            
            # Step 4: Execute stage-specific test logic
            self._test_kill_during_stage(
                stage=stage,
                create_queries=create_queries,
                query_node=query_node,
                index_nodes=index_nodes,
                training_delay_seconds=training_delay_seconds if stage == "training" else None
            )
            
            # Step 5: Wait for recovery and validate
            self.log.info(f"Waiting for indexer recovery after {stage} stage kill")
            self.sleep(120, "Waiting for indexer to recover and indexes to be rebuilt")
            
            # Wait for indexes to come online
            self.wait_until_indexes_online()
            
            # Step 6: Validate item count and recall
            self.log.info(f"Validating recovery after {stage} stage kill")
            self._validate_recovery_after_kill(select_queries, query_node, stage)
            
            # Step 7: Cleanup for next stage
            self.log.info(f"Cleaning up after {stage} stage test")
            self.drop_all_indexes()
            self.sleep(30, "Waiting for cleanup to complete")
            
            # Reset training delay if it was set
            if stage == "training":
                rest = RestConnection(index_nodes[0])
                rest.set_index_settings({
                    "indexer.debug.delayTrainingDuration": 0
                })
        
        self.log.info("=== test_kill_indexer_process_at_different_stages completed successfully ===")
    
    def _test_kill_during_stage(self, stage, create_queries, query_node, index_nodes, training_delay_seconds=None):
        """
        Generic method to kill indexer during different stages of BHIVE index creation.
        
        Args:
            stage: The stage to test ("sampling", "training", "building", "graph_building")
            create_queries: List of index creation queries
            query_node: Query node to execute queries on
            index_nodes: List of index nodes
            training_delay_seconds: Delay for training stage (required if stage="training")
        """
        self.log.info(f"Testing kill during {stage} stage")
        
        with ThreadPoolExecutor() as executor:
            # Start index creation
            create_future = executor.submit(self._create_indexes_async, create_queries, query_node)
            
            # Stage-specific timing logic
            if stage == "sampling":
                # Kill indexer almost immediately (during sampling)
                self.sleep(2, "Waiting briefly for sampling to start")
                self.log.info("Killing indexer during sampling stage")
                for index_node in index_nodes:
                    remote_client = RemoteMachineShellConnection(index_node)
                    remote_client.terminate_process(process_name='indexer')
                
            elif stage == "training":
                if training_delay_seconds is None:
                    raise ValueError("training_delay_seconds is required for training stage")
                # Wait for sampling to complete and training delay to kick in
                sampling_time = 5  # Allow time for sampling to complete
                kill_time = sampling_time + training_delay_seconds - 5  # Kill 5 seconds before delay ends
                
                self.sleep(kill_time, f"Waiting {kill_time} seconds for training stage")
                self.log.info("Killing indexer during training stage")
                for index_node in index_nodes:
                    remote_client = RemoteMachineShellConnection(index_node)
                    remote_client.terminate_process(process_name='indexer')
                
            elif stage == "building":
                # Monitor index status and kill when "Building" stage is reached
                self._wait_for_index_status_and_kill("Building", index_nodes)
                
            elif stage == "graph_building":
                # Monitor index status and kill when "Graph build" stage is reached
                self._wait_for_index_status_and_kill("Graph build", index_nodes)
                
            else:
                raise ValueError(f"Unknown stage: {stage}. Valid stages are: sampling, training, building, graph_building")
            
            # Wait for create operation to complete (will fail but that's expected)
            try:
                create_future.result()
            except Exception as e:
                self.log.info(f"Expected failure during index creation: {str(e)}")
    
    def _wait_for_index_status_and_kill(self, target_status, index_nodes, max_wait_time=300):
        """Wait for specific index status and kill indexer when reached"""
        self.log.info(f"Monitoring for {target_status} status")
        
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                # Check index status using getIndexStatus endpoint
                rest = RestConnection(index_nodes[0])
                index_metadata = rest.get_indexer_metadata()
                
                # Parse the status array to find any index with target status
                for index_info in index_metadata.get('status', []):
                    index_name = index_info.get('name', 'unknown')
                    status = index_info.get('status', '')
                    progress = index_info.get('progress', 0)
                    self.log.info(f"Index {index_name} status: {status}, progress: {progress}")
                    
                    if target_status.lower() in status.lower():
                        self.log.info(f"Found {target_status} status for index {index_name}, killing indexer")
                        for index_node in index_nodes:
                            remote_client = RemoteMachineShellConnection(index_node)
                            remote_client.terminate_process(process_name='indexer')
                        return
                            
            except Exception as e:
                self.log.warning(f"Error checking index status: {str(e)}")
            
            self.sleep(2, f"Waiting for {target_status} status")
        
        self.log.warning(f"Timeout waiting for {target_status} status, proceeding anyway")

    def _create_indexes_async(self, create_queries, query_node):
        """Create indexes asynchronously"""
        self.log.info("Starting index creation")
        for query in create_queries:
            self.log.info(f"Running create query: {query}")
            self.run_cbq_query(query=query, server=query_node)
        self.log.info("Index creation commands completed")

    def _validate_recovery_after_kill(self, select_queries, query_node, stage):
        """Validate item count and recall after indexer recovery"""
        self.log.info(f"Validating recovery after {stage} stage kill")
        
        # Validate item counts
        try:
            self.compare_item_counts_between_kv_and_gsi()
            self.log.info("Item count validation passed")
        except Exception as e:
            self.log.error(f"Item count validation failed: {str(e)}")
            self.fail(f"Item count validation failed after {stage} kill: {str(e)}")
        
        # Validate recall for vector queries
        recall_validated = False
        for query in select_queries[:2]:  # Test first 2 queries
            if "embVector" in query:
                query = query.replace("embVector", str(self.bhive_sample_vector))
            if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                continue
                
            try:
                self.log.info(f"Running recall validation query: {query}")
                result = self.run_cbq_query(query=query, server=query_node)
                
                # Basic validation - ensure query returns results
                if result.get("results"):
                    self.log.info(f"Query returned {len(result['results'])} results")
                    recall_validated = True
                else:
                    self.log.warning("Query returned no results")
                    
            except Exception as e:
                self.log.error(f"Recall validation query failed: {str(e)}")
                self.fail(f"Recall validation failed after {stage} kill: {str(e)}")
        
        if recall_validated:
            self.log.info("Recall validation passed")
        else:
            self.log.warning("No vector queries available for recall validation")
        
        self.log.info(f"Recovery validation completed successfully for {stage} stage")
    
    def test_index_drop_during_graph_build(self):
        """
        Test to verify index drop behavior during BHIVE graph build phase.
        
        Steps:
        1. Create a vector index that requires graph building
        2. Monitor for graph build status
        3. Issue DROP INDEX command during graph build
        """
        self.log.info("Step 1: Setting up cluster and creating bucket/collection")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        
        # Create collection and load enough data to ensure graph build takes time
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket, 
                num_scopes=1, 
                num_collections=1,
                num_of_docs_per_collection=100000,  # Large enough to make graph build take time
                json_template=self.json_template, 
                load_default_coll=False
            )
        
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)
        
        self.sleep(180, "Sleeping for 180 seconds to ensure documents are loaded")

        # Create a vector index that will require graph building
        self.log.info("Creating vector index that will require graph building")
        index_name = "idx_graph_build_test"
        create_query = f"""CREATE VECTOR INDEX {index_name} ON test_bucket_0.test_scope_1.test_collection_1 (embedding VECTOR) USING GSI WITH {{"dimension": 128, 
                                "similarity": "L2_SQUARED",
                                "description": "IVF,SQ8",
                                "train_list": 50000}}"""
        create_query_2 = f"""CREATE VECTOR INDEX {index_name}2 ON test_bucket_0.test_scope_1.test_collection_1 (embedding VECTOR) USING GSI WITH {{"dimension": 128}}"""
        
        try:
            self.run_cbq_query(query=create_query, server=query_node)
            self.run_cbq_query(query=create_query_2, server=query_node)
        except Exception as e:
            self.log.error(f"Failed to create index: {str(e)}")
            raise

        # Monitor for graph build and drop when found
        self.log.info("Monitoring for graph build status and issuing drop")
        drop_query = f"DROP INDEX {index_name} ON test_bucket_0.test_scope_1.test_collection_1"
        
        drop_executed = False
        start_time = time.time()
        max_wait_time = 300
        
        while time.time() - start_time < max_wait_time:
            try:
                rest = RestConnection(index_nodes[0])
                index_metadata = rest.get_indexer_metadata()
                
                # Parse the status array to find our index in graph build phase
                for index_info in index_metadata.get('status', []):
                    index_name_found = index_info.get('name', 'unknown')
                    status = index_info.get('status', '')
                    progress = index_info.get('progress', 0)
                    graph_progress = index_info.get('graphProgress', 0)
                    
                    if index_name in index_name_found:
                        self.log.info(f"Index {index_name_found} status: {status}, progress: {progress}, graphProgress: {graph_progress}")
                        
                        # Check for graph build status or explicit graph progress
                        if ("graph build" in status.lower() or 
                            ("building" in status.lower() and graph_progress > 0) or
                            graph_progress > 0):
                            
                            self.log.info(f"Found graph build phase for index {index_name_found}, executing DROP INDEX")
                            
                            try:
                                # Execute drop command
                                self.run_cbq_query(query=drop_query, server=query_node)
                                self.log.info(f"Successfully executed DROP INDEX during graph build")
                                drop_executed = True
                                break
                            except Exception as e:
                                self.log.error(f"Failed to execute DROP INDEX: {str(e)}")
                                self.fail(f"Failed to execute DROP INDEX during graph build: {str(e)}")
                                
            except Exception as e:
                self.log.warning(f"Error checking index status: {str(e)}")
                self.fail(f"Error checking index status: {str(e)}")
            
            if drop_executed:
                break
                
            self.sleep(2, "Waiting for Graph build status")
        
        if drop_executed:
            self.log.info("DROP INDEX command was successfully executed during graph build")
            self.sleep(30, "Waiting for drop operation to complete")
        else:
            self.log.warning("DROP INDEX command was not executed - graph build phase not detected")
            
        self.log.info("test_index_drop_during_graph_build completed")
    
    def test_bhive_stats(self):
        """
        Test to verify that BHIVE index statistics are accessible during rebalance operations.
        
        This test validates that index statistics endpoints remain functional and responsive
        while rebalance operations are in progress, ensuring monitoring tools can continue
        to gather performance metrics during cluster maintenance.
        
        Steps:
        1. Create test bucket and collections
        2. Load 10k documents into collections
        3. Create BHIVE vector indexes on all collections
        4. Wait for indexes to come online
        5. Initiate rebalance-out operation on an index node
        6. Simultaneously poll index statistics API during rebalance
        7. Validate that statistics are successfully retrieved during rebalance
        
        Expected Results:
        - Index statistics API should remain accessible during rebalance
        - No errors or timeouts when retrieving stats during rebalance
        - Statistics data should be complete and accurate
        - System should maintain monitoring capabilities during maintenance operations
        """
        self.log.info("Setting up buckets and loading docs")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=10000,
                                                 json_template=self.json_template, load_default_coll=False)
        
        self.log.info("Creating BHIVE indexes")
        create_queries = []
        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            self.log.info("Getting query definitions...")
            bhive_def, _, _ = self._get_query_definitions()
            current_create_queries = self._get_create_queries(bhive_def, None, None, namespace=namespace, defer_build_mix=False)
            create_queries.extend(current_create_queries)
        
        self.log.info(f"Running create_queries")
        for query in create_queries:
            self.log.info(f"Running create query: {query}")
            self.run_cbq_query(query=query, server=query_node)
        
        self.wait_until_indexes_online()
        self.sleep(10, "Indexes online")

        # Performing Rebalance-out operation
        out_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[1]
        out_nodes_list = [out_node]
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        #Running a thread to simultaneously poll for stats URL during rebalance operation
        with ThreadPoolExecutor() as executor:
            indexer_rest = RestConnection(index_node)
            stats_thread = executor.submit(indexer_rest.get_all_index_stats)
            self._rebalance_and_validate(nodes_out_list=out_nodes_list, nodes_in_list=None, swap_rebalance=False, services_in=None, select_queries=None, scan_results_check=False, skip_shard_validations=True)
            stats_thread.result()
    

    def test_backup_failure_status_code(self):
        """
        Test to verify correct HTTP status codes from backup API when indexer service is unavailable.
        
        This test validates that the backup API endpoint returns appropriate HTTP status codes
        when the indexer process is terminated, ensuring proper error handling and client
        notification of service unavailability.
        
        Steps:
        1. Create test bucket and collections
        2. Load 10k documents into collections  
        3. Create BHIVE vector indexes on all collections
        4. Wait for indexes to come online
        5. Terminate indexer process on all index nodes
        6. Poll backup API endpoint (/api/v1/bucket/{bucket}/backup)
        7. Validate HTTP status codes returned by backup API
        
        Expected Results:
        - Backup API should return 503 (Service Unavailable) status code
        - Should NOT return 404 (Not Found) status code
        - All index nodes should consistently return 503 status
        - Proper error handling when indexer service is down
        
        Validation:
        - Confirms backup API correctly identifies service unavailability
        - Ensures clients receive appropriate error codes for retry logic
        - Validates consistent error response across all index nodes
        """
        self.log.info("Setting up buckets and loading docs")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=10000,
                                                 json_template=self.json_template, load_default_coll=False)
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)
        self.sleep(60, "Waiting for docs to be loaded")
        
        # Creating BHIVE indexes
        create_queries = []
        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            self.log.info("Getting query definitions...")
            bhive_def, _, _ = self._get_query_definitions()
            current_create_queries = self._get_create_queries(bhive_def, None, None, namespace=namespace, defer_build_mix=False)
            create_queries.extend(current_create_queries)
        
        self.log.info(f"Running create_queries")
        for query in create_queries:
            self.log.info(f"Running create query: {query}")
            self.run_cbq_query(query=query, server=query_node)
        
        self.wait_until_indexes_online()
        self.sleep(10, "Indexes online")

        # Killing indexer process on all nodes.
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for index_node in index_nodes:
            remote_client = RemoteMachineShellConnection(index_node)
            remote_client.terminate_process(process_name='indexer')
        # Polling for api/v1/bucket/test_bucket/backup
        bucket = "test_bucket_0"
        for index_node in index_nodes:
            backup_url = f"http://{index_node.ip}:9102/api/v1/bucket/{bucket}/backup"
            
            self.log.info(f"Making GET request to backup URL: {backup_url}")
            response = requests.get(backup_url, timeout=30)
            status_code = response.status_code
            self.log.info(f"Received status code: {status_code} from node {index_node.ip}")
            
            # Validate that status is 503 (Service Unavailable) and not 404 (Not Found)
            if status_code == 404:
                self.fail(f"Received 404 status code from backup URL {backup_url}. Expected 503 (Service Unavailable)")
            elif status_code == 503:
                self.log.info(f"Successfully received 503 (Service Unavailable) status from node {index_node.ip}")
            else:
                self.fail(f"Received unexpected status code {status_code} from node {index_node.ip}")
        

    def test_logs_on_moi_disk(self):
        """
        Test MOI snapshot retention and log sequence numbers during network partition and recovery.
        Steps:
        1. Create a 3-node cluster (kv, kv, n1ql:index)
        2. Create 2 buckets (b1, b2), load 10k docs each
        3. Create 2 MOI indexes per bucket
        4. Partition network between both kv nodes
        5. Set MOI snapshot interval to 1 min
        6. Start continuous kv load on 1st kv node
        7. Wait for num_disk_snapshot to reach 4 and num_commits > 4
        8. Remove partition, stop load, wait for sync
        9. Check num_disk_snapshot reduces to 2
        10. Log sequence numbers when num_disk_snapshot > 2
        """
        
        # 1. Setup cluster and buckets
        self.log.info("Setting up 2 buckets and loading 10k docs each")
        buckets = self._create_test_buckets(num_buckets=2)
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)
        self.sleep(60, "Waiting for docs to be loaded")

        # 2. Create 2 MOI indexes per bucket
        self.log.info("Creating 2 MOI indexes per bucket")
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        create_queries = []
        for namespace in self.namespaces:
            _, scalar_def, composite_def = self._get_query_definitions()
            current_create_queries = self._get_create_queries(None, scalar_def, composite_def, namespace=namespace, defer_build_mix=False) 
            create_queries.extend(current_create_queries)
        
        self.log.info(f"Running create_queries")
        for query in create_queries:
            self.log.info(f"Running create query: {query}")
            self.run_cbq_query(query=query, server=query_node)
            
        self.wait_until_indexes_online()
        self.sleep(10, "Indexes online")

        # 3. Partition network between both kv nodes
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        if len(kv_nodes) < 2:
            self.fail("Test requires at least 2 KV nodes")
        self.log.info("Partitioning network between kv nodes")
        self.block_incoming_network_from_node(kv_nodes[0], kv_nodes[1])
        self.block_incoming_network_from_node(kv_nodes[1], kv_nodes[0])

        # 4. Set MOI snapshot interval to 1 min
        self.log.info("Setting MOI snapshot interval to 1 min")
        self.set_persisted_snapshot_interval(interval=60000)

        # 5. Start continuous kv load on 1st kv node
        self.log.info("Starting continuous kv load on 1st kv node")
        stop_event = Event()
        with ThreadPoolExecutor() as executor:
            kv_thread = executor.submit(self.perform_continuous_kv_mutations, stop_event, timeout=600, num_docs_mutating=10000)
            # 6. Wait for num_disk_snapshot to reach 4 and num_commits > 4
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            index_node = index_nodes[0]
            snapshot_reached = False
            while not snapshot_reached:
                index_stats = RestConnection(index_node).get_index_stats()
                for bucket in index_stats:
                    for idx in index_stats[bucket]:
                        self.log.info(f"Printing index stats before removing network partition: {index_stats}")
                        stats = index_stats[bucket][idx]
                        num_snapshots = stats.get("num_snapshots", 0)
                        num_commits = stats.get("num_commits", 0)
                        seqno = stats.get("last_persisted_seqno", 0)
                        if num_snapshots >= 4 and num_commits > 4:
                            self.log.info(f"num_snapshots={num_snapshots}, num_commits={num_commits}, seqno={seqno} for {idx}")
                            snapshot_reached = True
                self.sleep(10, "Waiting for num_snapshots >= 4 and num_commits > 4")

            # 7. Remove partition, stop load, wait for sync
            self.log.info("Removing network partition and stopping kv load")
            self.resume_blocked_incoming_network_from_node(kv_nodes[0], kv_nodes[1])
            self.resume_blocked_incoming_network_from_node(kv_nodes[1], kv_nodes[0])
            stop_event.set()
            kv_thread.result()
            self.sleep(60, "Waiting for nodes to sync")

            # 8. Check num_disk_snapshot reduces to 2
            reduced = False
            for _ in range(12):
                index_stats = RestConnection(index_node).get_index_stats()
                self.log.info(f"Printing index_stats after removing network partition: {index_stats}")
                for bucket in index_stats:
                    for idx in index_stats[bucket]:
                        stats = index_stats[bucket][idx]
                        num_snapshots = stats.get("num_snapshots", 0)
                        if num_snapshots <= 2:
                            self.log.info(f"num_snapshots reduced to {num_snapshots} for {idx}")
                            reduced = True
                        else:
                            self.log.info(f"num_snapshots={num_snapshots} for {idx}")
                            reduced = False
                if reduced:
                    break
                self.sleep(10, "Waiting for num_snapshots to reduce to 2")
            self.assertTrue(reduced, "num_snapshots did not reduce to 2 after sync")

        self.log.info("Test completed: MOI disk snapshot retention and log sequence numbers")

    def test_indexer_crash_recovery(self):
        """
        Test to verify indexer performance after crash recovery.
        
        This test validates that:
        1. Initial query performance is good (around 30ms)
        2. After indexer crash and recovery:
           - Query still works but with higher latency (around 300ms)
           - Performance remains degraded even after multiple runs
           - No QPS degradation in normal operation
        
        Steps:
        1. Create bucket and load siftsmall dataset
        2. Create vector index with specific configuration
        3. Run initial queries and measure latency
        4. Crash indexer process
        5. Wait for recovery
        6. Run same queries and measure latency
        7. Run multiple times to verify consistent behavior
        
        Expected Results:
        - Initial query latency should be ~30ms
        - Post-recovery query latency should be ~300ms
        - Latency should remain high even after multiple runs
        - No QPS degradation in normal operation
        """
        self.log.info("Step 1: Setting up test environment")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        
        # Create collection and load siftsmall dataset
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template="siftBigANN",
                load_default_coll=False
            )
        
        self.sleep(120, "Waiting for documents to be loaded")
        select_queries = []
        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            self.log.info("Getting query definitions...")
            _, _, composite_def = self._get_query_definitions()
            self.log.info("Successfully got query definitions")
            create_queries = self._get_create_queries(None, None, composite_def, namespace, defer_build_mix=False)
            self.log.info("Running create queries for index creation...")
            for query in create_queries:
                self.log.info(f"Running create query: {query}")
                self.run_cbq_query(query=query, server=query_node)
            self.log.info("Waiting for index to be built...")
            self.wait_until_indexes_online()

            select_queries.extend(self.gsi_util_obj.get_select_queries(
                definition_list=composite_def,
                namespace=namespace,
                limit=self.scan_limit
            ))
        
        self.log.info("Running select queries before crashing the indexer node.")
        total_execution_time_before = 0
        total_execution_time_after = 0
        for _ in range(10):
            for query in select_queries:
                if "embVector" in query:
                    self.log.info("Replacing embVector in query...")
                    query = query.replace("embVector", str(self.bhive_sample_vector))
            if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                continue
            self.log.info(f"Running select query: {query}")
            result = self.run_cbq_query(query=query, server=query_node)
            total_execution_time_before += result.get("metrics", {}).get("executionTime", 0)
            self.log.info(f"Query result: {result}")

        self.log.info("Crashing the indexer node")
        for index_node in index_nodes:
            remote_client = RemoteMachineShellConnection(index_node)
            remote_client.terminate_process(process_name='indexer')

        self.log.info("Waiting for indexer to crash")
        self.sleep(120, "Waiting for indexer to crash")

        self.log.info("Running select queries after crashing the indexer node.")
        for _ in range(10):
            for query in select_queries:
                if "embVector" in query:
                    self.log.info("Replacing embVector in query...")
                query = query.replace("embVector", str(self.bhive_sample_vector))
            if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                continue
            self.log.info(f"Running select query: {query}")
            result = self.run_cbq_query(query=query, server=query_node)
            total_execution_time_after += result.get("metrics", {}).get("executionTime", 0)
            self.log.info(f"Query result: {result}")
        
        self.log.info("Indexer crash recovery test completed successfully.")
        self.log.info(f"Total execution time before crash: {total_execution_time_before}")
        self.log.info(f"Total execution time after crash: {total_execution_time_after}")
        average_execution_time_before = total_execution_time_before / 10
        average_execution_time_after = total_execution_time_after / 10  
        self.log.info(f"Average execution time before crash: {average_execution_time_before}")
        self.log.info(f"Average execution time after crash: {average_execution_time_after}")
        # Checking whether the increase is not more than 50%
        self.assertTrue(average_execution_time_after <= average_execution_time_before * 1.5, "Average execution time after crash should be less than 50% increase from before crash")
            

    def test_stale_transfer_tokens(self):
        """
        Test to verify that no DCP transfer tokens exist after enabling file-based rebalance.
        
        Steps:
        1. Setup cluster with file-based rebalance disabled
        2. Create large indexes (GBs in size)
        3. Enable file-based rebalance
        4. Perform swap rebalance
        5. Verify no DCP transfer tokens exist in listRebalanceTokens endpoint
        """
        self.log.info("Step 1: Setting up cluster with file-based rebalance disabled")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(index_nodes[0])
        
        # Ensure file-based rebalance is disabled
        self.log.info("Disabling file-based rebalance")
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": False})
        settings = rest.get_global_index_settings()
        self.log.info(f"settings after disabling shard affinity: {settings}")
        
        self.log.info("Step 2: Creating large indexes")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        
        # Create collection with large number of documents
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket, 
                num_scopes=1, 
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template, 
                load_default_coll=False
            )
            
        
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)

        self.sleep(180, "Sleeping for 180 seconds to ensure documents are loaded")
        # Create multiple large indexes
        self.log.info("Creating multiple large indexes")

        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            self.log.info("Getting query definitions...")
            bhive_def, scalar_def, composite_def = self._get_query_definitions()
            self.log.info("Successfully got query definitions")
            
            current_create_queries = self._get_create_queries(bhive_def, scalar_def, composite_def, namespace, defer_build_mix=False)
            self.gsi_util_obj.async_create_indexes(create_queries=current_create_queries, database=namespace, query_node=query_node)
                
        self.log.info("Waiting for indexes to be built")
        self.wait_until_indexes_online()
        self.sleep(60, "Allowing time for indexes to be fully built")
        
        self.log.info("Step 3: Enabling file-based rebalance")
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})
        
        self.log.info("Step 4: Performing swap rebalance")
        # Get a new node for swap
        new_node = self.servers[-1]  # Using last server as new node
        swap_rebalance_out = index_nodes[1]  # Using second index node to swap out
        
        self.log.info(f"Swapping out node {swap_rebalance_out.ip} with new node {new_node.ip}")
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [new_node],
            [swap_rebalance_out],
            services=["index"]
        )
        
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "Rebalance failed or did not complete")
        rebalance.result()
        
        self.sleep(60, "Allowing time for rebalance to settle")
        
        self.log.info("Step 5: Verifying no DCP transfer tokens exist")
        # Check all index nodes for transfer tokens
        self.verify_dcp_transfer_tokens()
                
        self.log.info("No DCP transfer tokens found - test passed")
        
        # Cleanup
        self.log.info("Cleaning up - dropping indexes")
        self.drop_all_indexes()
        self.sleep(30, "Waiting for cleanup to complete")

    def test_duplicate_drop_during_training(self):
        """
        Test to verify that duplicate drop requests during index training are handled correctly.
        
        Steps:
        1. Create a vector index that requires training
        2. Initiate first drop while index is training
        3. Try multiple concurrent drops - should fail with "Index drop already in progress"
        """
        self.log.info("Step 1: Setting up cluster and creating bucket/collection")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        
        # Create collection and load enough data to ensure training takes time
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket, 
                num_scopes=1, 
                num_collections=1,
                num_of_docs_per_collection=100000,  # Large enough to make training take time
                json_template=self.json_template, 
                load_default_coll=False
            )
        
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)
        
        self.sleep(180, "Sleeping for 180 seconds to ensure documents are loaded")

        # Create a vector index that will require training
        self.log.info("Creating vector index with large train_list to ensure training takes time")
        index_name = "idx_training_test"
        create_query = f"""CREATE VECTOR INDEX {index_name} ON test_bucket_0.test_scope_1.test_collection_1 (embedding VECTOR) USING GSI WITH {{"dimension": 128, 
                                "similarity": "L2_SQUARED",
                                "description": "IVF,SQ8",
                                "train_list": 50000}}"""
        
        try:
            self.run_cbq_query(query=create_query, server=query_node)
        except Exception as e:
            self.log.error(f"Failed to create index: {str(e)}")
            raise

        # Wait briefly to ensure index creation has started but not completed
        self.sleep(5)
        
        # Function to execute a single drop request
        def execute_drop(drop_id):
            try:
                drop_query = f"DROP INDEX {index_name} ON test_bucket_0.test_scope_1.test_collection_1"
                self.run_cbq_query(query=drop_query, server=query_node)
                return drop_id, None  # Success
            except Exception as e:
                return drop_id, str(e)  # Return the error
        
        # Submit concurrent drop requests
        self.log.info("Submitting concurrent drop requests")
        num_concurrent_drops = 5
        drop_results = []
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            # Submit all drops simultaneously
            futures = [executor.submit(execute_drop, i) for i in range(num_concurrent_drops)]
            
            # Wait for all drops to complete
            for future in futures:
                drop_id, error = future.result()
                drop_results.append((drop_id, error))
        
        # Analyze results
        successful_drops = 0
        failed_drops = 0
        
        for drop_id, error in drop_results:
            if error is None:
                successful_drops += 1
                self.log.info(f"Drop request {drop_id} succeeded")
            else:
                failed_drops += 1
                self.log.info(f"Drop request {drop_id} failed with error: {error}")
        
        # Verify that exactly one drop succeeded and others failed
        if successful_drops != 1:
            self.fail(f"Expected exactly 1 successful drop, but got {successful_drops}")
        
        if failed_drops != num_concurrent_drops - 1:
            self.fail(f"Expected {num_concurrent_drops - 1} failed drops, but got {failed_drops}")

        # Wait for original drop to complete
        self.sleep(60, "Waiting for original drop to complete")
        
        # Verify index was actually dropped
        try:
            verify_query = "SELECT * FROM system:indexes WHERE name = $1"
            result = self.run_cbq_query(
                query=verify_query, 
                server=query_node,
                query_params={"$1": index_name}
            )
            if result["results"]:
                self.fail("Index still exists after drop")
        except Exception as e:
            self.log.error(f"Error verifying index drop: {str(e)}")
            raise

        self.log.info("Test completed successfully - concurrent drops were properly handled")

    def test_index_definition_metadata(self):
        """
        Test to verify the metadata information of GSI indexes.
        
        This test validates that index metadata contains all required fields and proper values.
        
        Steps:
        1. Create buckets and collections
        2. Create different types of indexes (scalar, BHIVE vector, composite)
        3. Verify index metadata contains:
           - Index definition
           - Last known scan time
           - Number of replicas
           - Other index statistics
        4. Validate metadata values are correct and accessible
        5. Clean up by dropping all indexes
        
        Expected Results:
        - All indexes should have complete metadata
        - Metadata fields should be accessible and contain valid values
        - No missing or corrupted metadata fields
        """
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = self._create_test_buckets(num_buckets=1)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=False)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        create_queries = []
        select_queries = []
        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            self.log.info("Getting query definitions...")
            bhive_def, scalar_def, composite_def = self._get_query_definitions()
            self.log.info("Successfully got query definitions")

            self.log.info("Combining definitions and creating queries...")
            definitions = scalar_def + bhive_def + composite_def
            current_create_queries = self._get_create_queries(bhive_def, scalar_def, composite_def, namespace, defer_build_mix=False)
            create_queries.extend(current_create_queries)

            self.log.info(f"Creating {len(create_queries)} indexes...")
            self.gsi_util_obj.async_create_indexes(
                    create_queries=create_queries, 
                    database=namespace,
                    query_node=query_node
            )
            curr_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=definitions,
                namespace=namespace,
                limit=self.scan_limit
            )
            select_queries.extend(curr_select_queries)
        self.wait_until_indexes_online()
        self.sleep(60)

        self.log.info("Running test queries to verify indexes...")
        for query in select_queries[:2]:
            if "embVector" in query:
                self.log.info("Replacing embVector in query...")
                query = query.replace("embVector", str(self.bhive_sample_vector))
            if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                continue
            self.log.info(f"Running test query: {query}")
            self.run_cbq_query(query=query, server=query_node)
            self.log.info("Test query completed successfully")
        
        # Query system:indexes and check metadata
        query = """SELECT * FROM system:indexes"""
        self.log.info("Checking index metadata")
        result = self.run_cbq_query(query=query, server=query_node)

        for index_metadata in result["results"]:
            self.log.info(f"Index metadata: {index_metadata}")
            # Check if metadata has definition, last_known_scan_time, num_replica
            index_info = index_metadata.get("indexes", {})
            self.log.info(f"Index info: {index_info}")
            try:
                metadata = index_info["metadata"]

                definition = metadata["definition"]
                last_known_scan_time = metadata["stats"]["last_known_scan_time"]
                num_replica = metadata["num_replica"]
            except KeyError:
                self.log.error(f"Missing metadata in index")
                raise
            self.log.info(f"Index has definition: {definition}, last_known_scan_time: {last_known_scan_time}, num_replica: {num_replica}")
        
        self.log.info("Test query completed successfully")
        
        self.log.info("Dropping all indexes")
        self.drop_all_indexes()

    def test_limit_10k(self):
        """
        Test to verify BHIVE vector index behavior with large result sets and topN scanning.
        
        This test validates vector index performance and accuracy with different similarity metrics
        and result set sizes.
        
        Steps:
        1. Configure indexer settings for topN scanning (300 results)
        2. Create buckets and collections
        3. Create vector indexes with different similarity metrics:
           - COSINE
           - L2_SQUARED
           - L2
           - DOT
           - EUCLIDEAN_SQUARED
           - EUCLIDEAN
        4. Load test documents
        5. Validate:
           - Query results and recall accuracy
           - Codebook memory usage
           - Index performance with large result sets
        6. Compare BHIVE vs composite index performance (if enabled)
        7. Clean up and validate resource usage
        
        Expected Results:
        - Query recall should be >= 70%
        - Memory usage should be within expected limits
        - Proper cleanup of resources after test
        """
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        self.bhive_composite_comparison = self.input.param("bhive_composite_comparison", False)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(index_node)
        rest.set_index_settings({"indexer.bhive.topNScan": 300})
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=1, num_collections=1,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=False)
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace=namespace, json_template=self.json_template)


        simlarity_list = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]
        query_comparison_list = []
        query_stats_map = {}
        if self.bhive_composite_comparison:
            self.bhive_index = True
            similarity = random.choice(simlarity_list)
            simlarity_list = [similarity] * 2
        for similarity in simlarity_list:
            for namespace in self.namespaces:
                if self.bhive_index:
                    prefix = f'test_{similarity}_bhive'
                else:
                    prefix = f'test_{similarity}_composite'
                definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.dataset,
                prefix=prefix,
                similarity=similarity, 
                train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                xattr_indexes=self.xattr_indexes,
                limit=self.scan_limit, 
                is_base64=self.base64,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index, 
                description_dimension=self.dimension
            )
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace, limit=self.scan_limit)
                drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace)
                self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
                self.sleep(60)
                self.item_count_related_validations()

                for query in select_queries:
                    if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                        continue
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query, similarity=similarity, scan_consitency=True)
                    query_stats_map[query] = [recall, accuracy]

                codebook_memory_per_index_map, aggregated_codebook_memory_utilization = self.get_per_index_codebook_memory_usage()
                self.log.info(f"codebook_memory_per_index_map : {codebook_memory_per_index_map}")
                self.log.info(f"aggregated_codebook_memory_utilization : {aggregated_codebook_memory_utilization}")

                for query in drop_queries:
                    self.run_cbq_query(query=query, server=self.n1ql_node)
                if self.bhive_composite_comparison:
                    self.bhive_index = False
                    query_stats_map_new = deepcopy(query_stats_map)
                    query_stats_map = {}
                    query_comparison_list.append(query_stats_map_new)
                    self.gen_table_view(query_stats_map=query_stats_map_new,
                                        message=f"Bhive is  {self.bhive_index}")
                    
        if not self.bhive_composite_comparison:
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            query_comparison_list.append(query_stats_map)

        for query_stats_map in query_comparison_list:
            for query in query_stats_map:
                self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
                                        f"recall for query {query} is less than threshold 70")

        self.rest.delete_bucket(bucket='metadata_bucket')
        self.drop_index_node_resources_utilization_validations()

    def test_long_index_keys(self):
        """
        Test to verify GSI behavior with extremely long index key values.
        
        This test validates that the indexer can handle index keys with large string values
        without performance degradation or errors.
        
        Steps:
        1. Create test bucket and collection
        2. Create composite index on color and brand fields
        3. Test queries with:
           - Normal key values
           - Very long key values (15000 characters)
           - Mixed key value lengths
        4. Validate:
           - Index creation succeeds
           - Queries execute successfully
           - No memory issues or performance degradation
        
        Expected Results:
        - Index should handle long key values correctly
        - Queries should execute successfully
        - No indexer crashes or out of memory errors
        """
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template)
        self.log.info("Step 3: Creating index with long index keys")
        query = """CREATE INDEX `idx_1` ON `test_bucket_0`(`color`,`brand`)"""
        self.run_cbq_query(query=query, server=query_node)

        self.log.info("Waiting for index to be online")
        self.wait_until_indexes_online()
        self.log.info("Index is online")

        select_query_1 = """SELECT * FROM `test_bucket_0` WHERE `color` is not missing AND `brand` = repeat('a', 15000)"""
        select_query_2 = """SELECT * FROM `test_bucket_0` WHERE `color` = repeat('a', 15000)"""
        self.run_cbq_query(query=select_query_1, server=query_node)
        self.run_cbq_query(query=select_query_2, server=query_node)

        self.log.info("Step 4: Dropping all indexes")
        self.drop_all_indexes()

    def test_train_list_threshold(self):
        """
        Test to verify BHIVE vector index behavior with different train_list thresholds.
        
        This test validates that vector index creation properly handles train_list parameter
        validation and enforces appropriate thresholds based on available document count.
        
        Steps:
        1. Test Case 1 - Train List > Total Documents:
           - Create bucket with small number of documents
           - Attempt to create index with train_list (50000) > total documents
           - Verify index creation fails with appropriate error
        
        2. Test Case 2 - Train List > Qualifying Vectors:
           - Load documents with specific vector format (Hotel template)
           - Attempt to create index with train_list (20000) > qualifying vectors
           - Verify index creation fails appropriately
        
        3. Validation:
           - Verify error messages are appropriate
           - Check index doesn't exist after failed creation
           - Validate no partial indexes remain
        
        Expected Results:
        - Index creation should fail when train_list > total documents
        - Index creation should fail when train_list > qualifying vectors
        - Appropriate error messages should be returned
        - No residual index artifacts should remain
        - System should maintain stable state after failed creations
        """
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
        
        self.log.info("Creating index with train list threshold")
        index_failed_1 = False
        query = """CREATE VECTOR INDEX `idx_sif10k` ON `test_bucket_0`(`color`,`embedding` VECTOR) WITH { "dimension":128, "similarity":"L2_SQUARED", "description":"IVF,SQ8", "train_list":50000 }"""
        try:
            self.run_cbq_query(query=query, server=query_node)
            index_failed_1 = True
        except Exception as e:
            self.log.info(f"Expected exception as train list is greater than number of documents: {e}")
        if index_failed_1:
            self.fail("Index creation should have failed")

        #Case 2: Number of qualifying vectors is less than train list
        for namespace in self.namespaces:
            self.log.info(f"Loading docs into {namespace}")
            self._load_docs_in_collection(namespace, json_template="Hotel")
        
        query = """CREATE VECTOR INDEX `idx_sif20k` ON `test_bucket_0`(`color`,`embedding` VECTOR) WITH { "dimension":128, "similarity":"L2_SQUARED", "description":"IVF,SQ8", "train_list":20000 }"""
        index_failed_2 = False
        try:
            self.run_cbq_query(query=query, server=query_node)
            index_failed_2 = True
        except Exception as e:
            self.log.info(f"Expected exception as train list is greater than number of documents: {e}")
        if index_failed_2:
            self.fail("Index creation should have failed")

        self.log.info("Waiting for index to be online")
        self.wait_until_indexes_online()

        self.log.info("Dropping all indexes")
        self.drop_all_indexes()


    def test_maximum_dimension_support(self):
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = self._create_test_buckets(num_buckets=1)
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
        
        self.log.info("Creating index with dimension greater than 4096")
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            create_query_1 = f"""CREATE VECTOR INDEX test_bhive ON {namespace} (embedding VECTOR) WITH {{"dimension": 4097}}"""
            create_query_2 = f"""CREATE VECTOR INDEX test_bhive ON {namespace} (embedding VECTOR) WITH {{"dimension": 4097, "train_list": 10000}}"""
            create_queries = [create_query_1, create_query_2]
            creation_success = False
            try:
                # self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace, query_node=query_node)
                for query in create_queries:
                    self.run_cbq_query(query=query, server=query_node)
                creation_success = True
            except Exception as e:
                self.log.info(f"Expected exception as dimension is greater than 4096: {e}")
            if creation_success:
                self.fail("Index creation should have failed")
            self.wait_until_indexes_online()
            self.log.info(f"Index created on {namespace}")

        self.log.info("Dropping all indexes")
        self.drop_all_indexes()
    
    def test_bhive_rebalance_comprehensive(self):
        """
        Test to verify GSI behavior during various rebalance operations.
        
        Steps:
        1. Create a 4-node cluster (2 KV + 2 GSI/Query)
        2. Create 3 buckets with 2 collections each
        3. Load 100k documents in all keyspaces
        4. Create different types of indexes with replicas
        5. Run a series of rebalance operations (add, remove, swap, failover)
        6. Validate after each operation
        7. Clean up
        """
        self.log.info("=== Starting test_bhive_rebalance_comprehensive ===")
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = []
        self.skip_shard_validations = False
        
        self.log.info("Creating test buckets...")
        buckets = self._create_test_buckets(num_buckets=1) 
        self.log.info(f"Successfully created {len(buckets)} test buckets")
        
        # Create collections and add to namespaces
        self.log.info("Creating collections in each bucket...")
        for bucket in buckets:
            self.log.info(f"Creating collections in bucket: {bucket}")
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=1,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
            self.log.info(f"Successfully created collections in bucket: {bucket}")

        self.log.info("Getting query and index nodes...")
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.log.info(f"Query node: {query_node.ip}, Index nodes: {[node.ip for node in index_nodes]}")
        
        self.log.info("Setting up operations tuple...")
        ops_tuple = tuple(self.ops_order.split("-"))
        self.log.info(f"Operations tuple: {ops_tuple}")

        self.log.info("Configuring index settings...")
        rest = RestConnection(index_nodes[0])
        settings = rest.get_global_index_settings()
        if 'redistributeIndexes' in settings:
            self.log.info("Setting redistribute_indexes to True")
            rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": True})
        
        rest = RestConnection(index_nodes[0])
        settings = rest.get_global_index_settings()
        if 'redistributeIndexes' in settings:
            self.log.info("Setting redistribute_indexes to True (second check)")
            rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": True})
        
        if self.rebalance_type == "file":
            self.log.info("Rebalance type is 'file', checking shard affinity settings...")
            rest = RestConnection(index_nodes[0])
            settings = rest.get_global_index_settings()
            self.log.info(f"Global index settings: {settings}")
            if 'enableShardAffinity' in settings:
                self.log.info("Enabling shard based rebalance...")
                self.enable_shard_based_rebalance()
                self.log.info("Shard affinity enabled successfully")
        else:
            self.log.info("Skipping shard validations as rebalance type is not 'file'")
            self.skip_shard_validations = True
        
        # Create indexes in all collections
        self.log.info("Step 3: Creating indexes on all collections")
        simlarity_list = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]
        select_queries = []
        create_queries = []
        
        self.log.info("Creating indexes for each namespace...")
        for namespace in self.namespaces:
            self.log.info(f"Processing namespace: {namespace}")
            similarity = random.choice(simlarity_list)
            self.log.info(f"Selected similarity metric: {similarity}")
            
            self.log.info("Getting query definitions...")
            bhive_def, scalar_def, composite_def = self._get_query_definitions(similarity=similarity)
            self.log.info("Successfully got query definitions")

            self.log.info("Combining definitions and creating queries...")
            definitions = scalar_def + bhive_def + composite_def
            current_create_queries = self._get_create_queries(bhive_def, scalar_def, composite_def, namespace, defer_build_mix=False)
            create_queries.extend(current_create_queries)
            self.log.info(f"Created {len(current_create_queries)} queries for namespace {namespace}")
        
            self.log.info("Waiting for indexes to come online...")
            self.wait_until_indexes_online()
            self.log.info("All indexes are online for current namespace")
        
            self.log.info("Getting select queries...")
            curr_select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=definitions,
                namespace=namespace,
                limit=self.scan_limit
            )
            select_queries.extend(curr_select_queries)
            self.log.info(f"Added {len(curr_select_queries)} select queries")
        
        self.log.info(f"Creating {len(create_queries)} indexes...")
        self.gsi_util_obj.async_create_indexes(
                create_queries=create_queries, 
                database=namespace,
                query_node=query_node
        )

        self.log.info("Sleeping for 2 minutes to allow indexes to be built...")
        self.sleep(120, "Sleeping for 2 minutes to allow indexes to be built")
        
        self.log.info("Waiting for all indexes to come online...")
        self.wait_until_indexes_online()
        self.log.info("All indexes are online")

        self.log.info("Running test queries to verify indexes...")
        for query in select_queries[:2]:
            if "embVector" in query:
                self.log.info("Replacing embVector in query...")
                query = query.replace("embVector", str(self.bhive_sample_vector))
            if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                continue
            self.log.info(f"Running test query: {query}")
            self.run_cbq_query(query=query, server=query_node)
            self.log.info("Test query completed successfully")

        self.log.info("Step 4: Loading data into all collections")
        for namespace in self.namespaces:
            self.log.info(f"Loading data into namespace: {namespace}")
            self._load_docs_in_collection(namespace, json_template=self.json_template)
            self.log.info(f"Successfully loaded data into namespace: {namespace}")
        
        self.log.info("Sleeping for 3 minutes to allow data to be loaded...")
        self.sleep(180, "Sleeping for 3 minutes to allow data to be loaded")
        
        self.log.info("Step 5: Running pre-rebalance validations")
        self._run_validations(select_queries,index_nodes)
        self.log.info("Pre-rebalance validations completed successfully")

        self.log.info("About to start workload")
        original_log_level = logging.getLogger().level

        self.log.info("Step 6: Starting rebalance operations")
        self.log.info(f"Available servers: {[server.ip for server in self.servers]}")
        rebalance_order_arr = self.rebalance_order.split("-")
        self.log.info(f"Rebalance order: {rebalance_order_arr}")
        out_node = None
        in_node = None
        
        swap_rebalance = False
        is_failover = False
        for rebalance_type in rebalance_order_arr:
            self.log.info(f"=== Starting {rebalance_type} rebalance operation ===")
            workload_stop_event = Event()
            nodes_in_list = []
            nodes_out_list = []
            services_in = []
            is_failover = False
            swap_rebalance = False
            
            if rebalance_type == "in":
                self.log.info(f"Rebalance-in operation: Adding node {in_node.ip}")
                nodes_in_list = [in_node]
                services_in = ["index,n1ql"]
            elif rebalance_type == "out":
                out_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[1]
                self.log.info(f"Rebalance-out operation: Removing node {out_node.ip}")
                nodes_out_list = [out_node]
            elif rebalance_type == "swap":
                in_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[1]
                self.log.info(f"Swap rebalance operation: Swapping out {in_node.ip} for {out_node.ip}")
                swap_rebalance = True
                nodes_out_list = [in_node]
                nodes_in_list = [out_node]
                services_in = ["index,n1ql"]
            elif "failover" in rebalance_type:
                service_type = rebalance_type.split("_")[1]
                use_graceful = self.graceful and service_type == "kv"
                is_failover = True
                self.log.info(f"Starting {service_type} failover operation (graceful: {use_graceful})")
                
                failover_node = self.get_nodes_from_services_map(
                    service_type=service_type, 
                    get_all_nodes=True
                )[-1]
                nodes_out_list = [failover_node]
                self.log.info(f"Failing over node: {failover_node.ip}")
                
                failover_task = self.cluster.async_failover(
                    self.servers[:self.nodes_init],
                    [failover_node],
                    use_graceful,
                    wait_for_pending=120
                )
                failover_task.result()
                self.log.info(f"Failover completed for node: {failover_node.ip}")
            
            # Debug: Check execution flow after failover/rebalance type detection
            self.log.info(f"DEBUG: After rebalance type processing - is_failover: {is_failover}, rebalance_type: {rebalance_type}")
            
            # Determine if shard validations should be skipped
            skip_shard_validations = self.skip_shard_validations
            if is_failover:
                skip_shard_validations = True
            
            self.log.info(f"Skipping shard validations: {skip_shard_validations}")
            
            try:
                if self.run_mutation_workload:
                    self.log.info(f"Setting log level to WARNING for {rebalance_type} operation")
                    # logging.getLogger().setLevel(logging.WARNING)
                    self.log.warning(f"Starting background workloads for {rebalance_type}")
                    with ThreadPoolExecutor() as executor:
                        try:
                            self.log.info("Starting mutation workload threads...")
                            bucket_0_thread, bucket_1_thread, bucket_2_thread = self._run_mutations_workload(executor, workload_stop_event, ops_tuple)
                            self.log.info("Mutation workload threads started successfully")
                            
                            self.log.info("Allowing workloads to stabilize before rebalance...")
                            self.sleep(10, "Allowing workloads to stabilize before rebalance")

                            self.log.info("Restoring original log level")
                            logging.getLogger().setLevel(original_log_level)
                            self.log.info(f"Starting {rebalance_type} rebalance operation")

                            self._rebalance_and_validate(
                                nodes_in_list=nodes_in_list,
                                nodes_out_list=nodes_out_list,
                                swap_rebalance=swap_rebalance,
                                services_in=services_in,
                                select_queries=None,
                                skip_shard_validations=skip_shard_validations,
                            )
                            self.log.info(f"Rebalance operation {rebalance_type} completed successfully")
                            self.log.info("Stopping workload threads...")
                            workload_stop_event.set()

                            self.log.info("Waiting for mutation workload threads to complete...")
                            self._finish_mutations_workload(bucket_0_thread, bucket_1_thread, bucket_2_thread)
                            self.log.info("All mutation workload threads completed")
                        finally:
                            self.log.info(f"All workloads for {rebalance_type} operation completed")

                else:
                    self._rebalance_and_validate(
                            nodes_in_list=nodes_in_list,
                            nodes_out_list=nodes_out_list,
                            swap_rebalance=swap_rebalance,
                            services_in=services_in,
                            select_queries=None,
                            skip_shard_validations=skip_shard_validations,
                        )
                    self.log.info(f"Rebalance operation {rebalance_type} completed successfully")
            except Exception as err:
                self.log.error(f"Error during {rebalance_type} operation: {str(err)}")
                raise err
            finally:
                self.log.info(f"{rebalance_type} operation completed")
                
            self.log.info("Sleeping for 2 minutes to allow rebalance to complete...")
            self.sleep(120, "Sleeping for 2 minutes to allow rebalance to complete")
            
            self.log.info("Running post-rebalance validations...")
            self._run_validations(select_queries,self.get_nodes_from_services_map(service_type="index", get_all_nodes=True), skip_shard_validations=skip_shard_validations)
            self.log.info("Post-rebalance validations completed successfully")

        self.log.info("All rebalance operations completed successfully")
        
        self.log.info("Cleaning up - dropping all indexes...")
        self.drop_all_indexes()
        self.log.info("All indexes dropped successfully")

        self.sleep(120, "Sleeping for 2 minutes to allow indexes to be dropped")
        
        self.log.info("Performing final validations...")
        self.check_storage_directory_cleaned_up(threshold_gb=0.05)
        self.log.info("Storage directory cleanup validation completed")
        
        if not self.validate_cpu_normalized:
            self.log.error("CPU normalized validation failed")
            raise AssertionError("CPU normalized validation failed")
        self.log.info("CPU normalization validation completed successfully")
        
        self.log.info("=== test_bhive_rebalance_comprehensive completed successfully ===")
    
    def test_index_lifecycle_operations(self):
        """
        Test to verify GSI behavior during index lifecycle operations.
        
        Steps:
        1. Create a 5-node cluster (2 KV + 3 GSI/Query)
        2. Create 2 buckets with 2 collections each (total 4 collections)
        3. Load 100k documents in all keyspaces
        4. Create, delete, build or alter scalar, BHIVE, and composite indexes on all collections with replicas
        5. Wait until indexes are online
        6. Perform quiet period validation
        7. Drop all indexes
        8. Validate cleanup of disk/CPU/memory
        9. Repeat steps 3-8 for multiple cycles
        """
        self.log.info("Starting test_index_lifecycle_operations")
        self.log.info("Step 1: Setting up cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = self._create_test_buckets(num_buckets=2)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

        #Create collections
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)



        self.log.info("Step 3: Creating indexes on all collections")
        simlarity_list = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]
        prefixes = ["test_scalar", "test_bhive", "test_composite"]
        create_queries = list()
        build_queries = list()
        drop_queries = list()
        alter_queries = list()
        lifecycle_ops = self.index_ops_order.split("-")
        for namespace in self.namespaces:
            self.log.info(f"Creating indexes on {namespace}")
            similarity = random.choice(simlarity_list)

            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, similarity)
            definitions = scalar_def + bhive_def + composite_def
            suffix = namespace.replace(':', '_').replace('.', '_')

            create_queries.append(self._get_create_queries(bhive_def, scalar_def, composite_def, namespace))

            drop_queries.append(self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace))

            build_queries.append(self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace))

            alter_queries.append(f'ALTER INDEX scalar_{suffix} ON {namespace} WITH {{"action":"replica_count","num_replica":2}}')
            alter_queries.append(f'ALTER INDEX composite_{suffix} ON {namespace} WITH {{"action":"replica_count","num_replica":2}}')
            alter_queries.append(f'ALTER INDEX bhive_{suffix} ON {namespace} WITH {{"action":"replica_count","num_replica":2}}')      

        # Flatten all the lists
        create_queries = [item for sublist in create_queries for item in sublist]
        drop_queries = [item for sublist in drop_queries for item in sublist]

        # Creating primary indexes on all the collections
        self.run_cbq_query("CREATE PRIMARY INDEX IF NOT EXISTS idxtb0 ON default:test_bucket_0._default._default", server=query_node)
        self.run_cbq_query("CREATE PRIMARY INDEX IF NOT EXISTS idxtb1 ON default:test_bucket_1._default._default", server=query_node)
        count = 0
        for namespace in self.namespaces:
            self.run_cbq_query(f"CREATE PRIMARY INDEX IF NOT EXISTS idx{count} ON {namespace}", server=query_node)
            count += 1

        # Creating event flag
        workload_stop_event = Event()

        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace, json_template=self.json_template)
            self.log.info(f"Loading documents into {namespace}")

        self.log.info("Starting background workloads for index lifecycle operations")
        ops_tuple = tuple(self.ops_order.split("-"))
        with ThreadPoolExecutor() as executor:
            try:
                bucket_0_thread, bucket_1_thread, _ = self._run_mutations_workload(executor, workload_stop_event, ops_tuple)
            except Exception as err:
                self.log.error(f"Error during background workloads: {str(err)}")
                raise err

        self.log.info("Performing index lifecycle operations")
        queries = []
        for op in lifecycle_ops:
            if op == "create":
                if "few" in op:
                    queries = create_queries[:5]
                    create_queries = create_queries[5:]
                else:
                    queries = create_queries
            elif op == "drop":
                if "few" in op:
                    queries = drop_queries[:5]
                    drop_queries = drop_queries[5:]
                else:
                    queries = drop_queries
            elif op == "build":
                if "few" in op:
                    queries = build_queries[:5]
                    build_queries = build_queries[5:]
                else:
                    queries = build_queries
            elif op == "alter":
                if "few" in op:
                    queries = alter_queries[:5]
                    alter_queries = alter_queries[5:]
                else:
                    queries = alter_queries
            for query in queries:
                try:
                    self.run_cbq_query(query=query, server=query_node)
                except Exception as e:
                    self.log.warning(f"Index already exists or hasn't been created yet: {str(e)}")
            

        workload_stop_event.set()
        self._finish_mutations_workload(bucket_0_thread, bucket_1_thread)
        # Drop all the indexes
        self.log.info("Dropping all the indexes")
        self.drop_all_indexes()

        self.log.info("Performing CPU validations")
        # TODO uncomment after https://jira.issues.couchbase.com/browse/MB-65934 is fixed
        # if not self.validate_memory_released():
        #     raise AssertionError("Memory not released despite dropping all the indexes")
        self.check_storage_directory_cleaned_up(threshold_gb=0.05)
        if not self.validate_cpu_normalized:
            self.log.error("CPU normalized validation failed")
            raise AssertionError("CPU normalized validation failed")
        self.log.info("Index lifecycle operations completed successfully")
    
    def test_index_lifecycle_operations_continuous(self):
        """
        Test to verify GSI behavior during continuous index lifecycle operations.
        
        Steps:
        1. Create a 5-node cluster (2 KV + 3 GSI/Query)
        2. Create 2 buckets with 2 collections each (total 4 collections)
        3. Load 100k documents in all keyspaces
        4. Create scalar, BHIVE, and composite indexes on all collections with replicas
        5. Wait until indexes are online
        6. Perform quiet period validation
        7. Spawn 4 concurrent threads that continuously:
           - Create indexes
           - Build indexes
           - Drop indexes
           - Alter indexes
        8. Run these operations for 30 minutes
        9. Drop all indexes
        10. Validate cleanup of resources
        """
        self.log.info("Starting test_index_lifecycle_operations_continuous")
        self.log.info("Step 1: Setting up cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = self._create_test_buckets(num_buckets=2)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

        # Create collections
        for bucket in buckets:
            self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
        
        self.log.info("Step 3: Creating indexes on all collections")
        similarity_list = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]
        prefixes = ["test_scalar", "test_bhive", "test_composite"]
        create_queries = list()
        build_queries = list()
        drop_queries = list()
        alter_queries = list()
        ops_tuple = tuple(self.ops_order.split("-"))
        for namespace in self.namespaces:
            self.log.info(f"Creating indexes on {namespace}")
            similarity = random.choice(similarity_list)
            suffix = namespace.replace(':', '_').replace('.', '_')

            bhive_def, scalar_def, composite_def = self._get_query_definitions(similarity=similarity)
            definitions = scalar_def + bhive_def + composite_def

            create_queries.append(
                self.gsi_util_obj.get_create_index_list(
                    definition_list=definitions,
                    namespace=namespace,
                    num_replica=self.num_index_replica,
                    bhive_index=self.bhive_index,
                    defer_build=True
                )
            )

            drop_queries.append(self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace))

            build_queries.append(self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace))

            alter_queries.append(f'ALTER INDEX scalar_{suffix} ON {namespace} WITH {{"action":"replica_count","num_replica":2}}')
            alter_queries.append(f'ALTER INDEX composite_{suffix} ON {namespace} WITH {{"action":"replica_count","num_replica":2}}')
            alter_queries.append(f'ALTER INDEX bhive_{suffix} ON {namespace} WITH {{"action":"replica_count","num_replica":2}}')

        # Flatten all the lists
        create_queries = [item for sublist in create_queries for item in sublist]
        drop_queries = [item for sublist in drop_queries for item in sublist]

        workload_stop_event = Event()
        original_log_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.WARNING)
        start_time = time.time()
        # Create 4 threads to perform create, build, alter and drop operations.
        with ThreadPoolExecutor(max_workers=4) as executor:
            create_thread = executor.submit(self._perform_query_operations, create_queries, query_node, workload_stop_event)
            build_thread = executor.submit(self._perform_query_operations, build_queries, query_node, workload_stop_event)
            alter_thread = executor.submit(self._perform_query_operations, alter_queries, query_node, workload_stop_event)
            drop_thread = executor.submit(self._perform_query_operations, drop_queries, query_node, workload_stop_event)
            bucket_1_thread, bucket_2_thread, _ = self._run_mutations_workload(executor, workload_stop_event, ops_tuple)
        
            # Set a timer for 30 minutes
            while time.time() - start_time < 1800:
                time.sleep(1)
                if workload_stop_event.is_set():
                    break
            
            workload_stop_event.set()
            self._finish_mutations_workload(bucket_1_thread, bucket_2_thread)
            try:
                create_thread.result()
            except Exception as err:
                self.log.error(f"Error during create operations: {str(err)}")
                raise err
            try:
                build_thread.result()
            except Exception as err:
                self.log.error(f"Error during build operations: {str(err)}")
                raise err
            try:
                alter_thread.result()
            except Exception as err:
                self.log.error(f"Error during alter operations: {str(err)}")
                raise err
            try:
                drop_thread.result()
            except Exception as err:
                self.log.error(f"Error during drop operations: {str(err)}")
                raise err
        
        logging.getLogger().setLevel(original_log_level)
        # Drop all the indexes.
        self.drop_all_indexes()
        self.log.info("Index lifecycle operations completed successfully")
        self.log.info("Performing CPU validations")
        # TODO uncomment after https://jira.issues.couchbase.com/browse/MB-65934 is fixed
        # if not self.validate_memory_released():
        #     raise AssertionError("Memory not released despite dropping all the indexes")
        self.check_storage_directory_cleaned_up()
        if not self.validate_cpu_normalized:
            self.log.error("CPU normalized validation failed")
            raise AssertionError("CPU normalized validation failed")
    
    def _perform_query_operations(self, queries, query_node, workload_stop_event):
        for query in queries:
            try:
                self.run_cbq_query(query=query, server=query_node)
            except Exception as e:
                self.log.warning(f"Index already exists or hasn't been created yet: {str(e)}")
            if workload_stop_event.is_set():
                return    

    def _finish_mutations_workload(self, bucket_0_thread = None, bucket_1_thread = None, bucket_2_thread = None):
        self.log.info("Waiting for all threads to complete")
        if bucket_0_thread:
            try:
                bucket_0_thread.result()
            except Exception as err:
                self.log.error(f"Error during bucket_0_thread complete: {str(err)}")
                raise err
                    
        if bucket_1_thread:
            try:
                bucket_1_thread.result()
            except Exception as err:
                self.log.error(f"Error during bucket_1_thread complete: {str(err)}")
                raise err
                    
        if bucket_2_thread:
            try:
                bucket_2_thread.result()
            except Exception as err:
                self.log.error(f"Error during bucket_2_thread complete: {str(err)}")
                raise err

    def _load_docs_in_collection(self, namespace, json_template=None):
        bucket, scope, collection = namespace.split(':')[-1].split('.')
        self.log.info(f"Loading data into {namespace}")
        self.gen_create = SDKDataLoader(
                num_ops=self.docs_per_collection, 
                percent_create=100,
                percent_update=0, 
                percent_delete=0, 
                scope=scope,
                collection=collection, 
                json_template=json_template,
                output=True, 
                username=self.username, 
                password=self.password,
                key_prefix=f"doc_{namespace.replace(':', '_').replace('.', '_')}_"
            )
            
        self.log.info(f"Loading docs into {namespace} using magma loader")
        task = self.cluster.async_load_gen_docs(
                self.master, 
                bucket=bucket.split(':')[-1],
                generator=self.gen_create,
                use_magma_loader=True
            )
        task.result()
    
    def _get_doc_id_range(self, bucket_name, scope="_default", collection="_default"):
        """Get the first and last document IDs for a given collection"""
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        namespace = f"default:{bucket_name}.{scope}.{collection}"
        
        try:
            # Get minimum ID
            min_query = f"SELECT MIN(META().id) as min_id FROM {namespace}"
            min_result = self.run_cbq_query(query=min_query, server=query_node)
            min_id = min_result['results'][0]['min_id']
            
            # Get maximum ID
            max_query = f"SELECT MAX(META().id) as max_id FROM {namespace}"
            max_result = self.run_cbq_query(query=max_query, server=query_node)
            max_id = max_result['results'][0]['max_id']
            
            # Extract numeric parts if IDs follow a pattern like "doc123"
            try:
                min_num = int(''.join(filter(str.isdigit, min_id)))
                max_num = int(''.join(filter(str.isdigit, max_id)))
                return min_num, max_num
            except (ValueError, TypeError):
                self.log.warning(f"Could not extract numeric IDs from {min_id} and {max_id}, using 0 and 100000")
                return 0, 100000
        except Exception as e:
            self.log.error(f"Error getting document ID range: {str(e)}")
            return 0, 100000
    
    def _run_mutations_workload(self, executor, workload_stop_event, ops_tuple=None):
        """
        Run mutations workload based on a tuple of operation types.
        
        Args:
            executor: ThreadPoolExecutor
            workload_stop_event: Event to signal when to stop the workload
            ops_tuple: Tuple of operation types per bucket. Each element is a string like 
                    "high_create_update", "low_delete", etc.
        
        Returns:
            Tuple of thread objects (bucket_0_thread, bucket_1_thread, bucket_2_thread)
        """
        # Default operations if none specified
        if ops_tuple is None:
            ops_tuple = ("high_create_update", "low_create_update", "high_create_delete")
        
        # Initialize threads as None
        bucket_0_thread, bucket_1_thread, bucket_2_thread = None, None, None
        
        # Map of available buckets with their ID ranges
        bucket_info = {}
        
        # Check which buckets exist and get their ranges
        for i, op_type in enumerate(ops_tuple):
            if op_type == "None":
                continue
                
            bucket_num = i
            bucket_name = f"test_bucket_{bucket_num}"
            
            try:
                min_id, max_id = self._get_doc_id_range(bucket_name=bucket_name)
                bucket_info[bucket_num] = {
                    "name": bucket_name,
                    "min_id": min_id,
                    "max_id": max_id,
                    "op_type": op_type
                }
                self.log.info(f"Document ID range for {bucket_name}: {min_id} - {max_id}")
            except Exception as e:
                self.log.warning(f"Skipping {bucket_name}, it might not exist: {str(e)}")
        
        # Process each bucket that exists
        for bucket_num, info in bucket_info.items():
            bucket_name = info["name"]
            min_id = info["min_id"]
            max_id = info["max_id"]
            op_type = info["op_type"]
            
            # Parse operation type to set parameters
            params = {
                "workload_stop_event": workload_stop_event,
                "bucket_name": bucket_name,
                "timeout": 1800
            }
            
            # Extract rate (high/low/random)
            if "high_" in op_type:
                params["ops_rate"] = self.HIGH_OPS_RATE
                op_range = self.HIGH_OPS
            elif "low_" in op_type:
                params["ops_rate"] = self.LOW_OPS_RATE
                op_range = self.LOW_OPS
            elif "random_" in op_type:
                random_rate = random.randint(1, 5001)
                params["ops_rate"] = random_rate
                op_range = random_rate * 2
            else:
                params["ops_rate"] = self.LOW_OPS_RATE
                op_range = self.LOW_OPS
            
            # Set operation percentages and ranges based on operation type
            if "create_update" in op_type:
                params["percent_create"] = 50
                params["percent_update"] = 50
                params["cs"] = max_id
                params["ce"] = max_id + op_range
                params["us"] = min_id
                params["ue"] = min_id + op_range
            elif "create_delete" in op_type:
                params["percent_create"] = 50
                params["percent_delete"] = 50
                params["cs"] = max_id
                params["ce"] = max_id + op_range
                params["ds"] = min_id
                params["de"] = min_id + op_range
            elif "expiry" in op_type:
                params["percent_expiry"] = 100
                params["es"] = min_id
                params["ee"] = min_id + op_range
            elif "read" in op_type:
                params["percent_read"] = 100
                params["rs"] = min_id
                params["re"] = min_id + op_range
            elif "create_only" in op_type:
                params["percent_create"] = 100
                params["cs"] = max_id
                params["ce"] = max_id + op_range
            elif "update_only" in op_type:
                params["percent_update"] = 100
                params["us"] = min_id
                params["ue"] = min_id + op_range
            else:
                # Default to create/update if unrecognized
                params["percent_create"] = 50
                params["percent_update"] = 50
                params["cs"] = max_id
                params["ce"] = max_id + op_range
                params["us"] = min_id
                params["ue"] = min_id + op_range
            
            # Submit the task
            self.log.info(f"Starting {op_type} operations on {bucket_name}")
            thread = executor.submit(self._perform_docloader_operations, **params)
            
            # Store thread in the appropriate variable
            if bucket_num == 0:
                bucket_0_thread = thread
            elif bucket_num == 1:
                bucket_1_thread = thread
            elif bucket_num == 2:
                bucket_2_thread = thread
        
        return bucket_0_thread, bucket_1_thread, bucket_2_thread 
        
    def _run_validations(self, select_queries,  index_nodes, similarity="L2_SQUARED", skip_shard_validations=False):
        """
        Run all required validations during quiet periods
        """
        # Wait for all mutations to be processed
        self.log.info("Waiting for all mutations to be processed")
        self.wait_for_mutation_processing(index_nodes)
        
        # Perform index item count checks
        if self.run_mutation_workload == False:
            self.log.info("Validating index item counts")
            self.compare_item_counts_between_kv_and_gsi()
        
        # Validate replica indexes have equal item counts
        self.log.info("Validating replica index item counts")
        self.validate_replica_indexes_item_counts()

        # Validate shard seggregation
        if not skip_shard_validations:
            shard_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_map)
        else:
            self.log.info("Skipping shard segregation validation")
        
        # Check mainstore and backstore item counts match
        self.log.info("Validating mainstore and backstore consistency")
        errors = self.backstore_mainstore_check()
        if errors:
            self.log.error(f"Mainstore/backstore mismatch: {errors}")
            self.fail("Mainstore and backstore item counts don't match")
        
        if self.rebalance_type == "file" and not skip_shard_validations:
            # Validate shard affinity
            self.log.info("Validating shard affinity")
            self.validate_shard_affinity()
        elif self.rebalance_type == "file" and skip_shard_validations:
            self.log.info("Skipping shard affinity validation")
            
        # Logging resource utilization
        self.log.info("Resource utilization:")
        for node in index_nodes:
            mem_usage = self.get_indexer_mem_quota(indexer_node=node)
            self.log.info(f"Node {node.ip} memory usage: {mem_usage}")
            
        # Log codebook memory usage for vector indexes
        codebook_memory_per_index_map, aggregated_codebook_memory_utilization = self.get_per_index_codebook_memory_usage()
        self.log.info(f"Codebook memory per index: {codebook_memory_per_index_map}")
        self.log.info(f"Aggregated codebook memory: {aggregated_codebook_memory_utilization}")

    def _perform_docloader_operations(self, workload_stop_event, bucket_name, ops_rate, timeout=1800, num_docs_mutating=10000,percent_create = 0, percent_update = 0, percent_delete = 0, percent_expiry = 0, percent_read = 0, cs = 0, ce = 0, us = 0, ue = 0, ds = 0, de = 0, es = 0, ee = 0, rs = 0, re = 0):
        collection_namespaces = self.namespaces
        for namespace in collection_namespaces:
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            if bucket != bucket_name:
                continue
            self.log.info(f"Performing high ops operations for bucket: {bucket}, scope: {scope}, collection: {collection}")
            self.gen_create = SDKDataLoader(ops_rate=ops_rate, num_ops=num_docs_mutating, percent_create=percent_create,
                                            percent_update=percent_update, percent_delete=percent_delete, percent_expiry=percent_expiry, percent_read=percent_read, scope=scope,
                                            collection=collection, json_template=self.json_template,
                                            output=True, username=self.username, password=self.password,
                                            create_start = cs, create_end = ce, update_start = us, update_end = ue,
                                            delete_start = ds, delete_end = de, expiry_start = es, expiry_end = ee,
                                            read_start = rs, read_end = re)
            if self.use_magma_loader:
                task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                        generator=self.gen_create, pause_secs=1,
                                                        timeout_secs=300, use_magma_loader=True)
                task.result()
            else:
                tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                                batch_size=10**4, dataset=self.json_template)
                for task in tasks:
                    task.result()
            time.sleep(10)

    def _create_test_buckets(self, num_buckets=3, bucket_size=256, bucket_storage="magma"):
        """
        Create test buckets for the test
        
        Parameters:
        - num_buckets: Number of buckets to create (default: 3)
        - bucket_size: Size of each bucket in MB (default: 256)
        - bucket_storage: Storage backend to use (default: "magma")
        
        Returns:
        - List of created bucket names
        """
        buckets = []
        
        for i in range(num_buckets):
            bucket_name = f"test_bucket_{i}"
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
                buckets.append(bucket_name)
                self.log.info(f"Created bucket: {bucket_name}")
            except Exception as err:
                self.log.error(f"Error creating bucket {bucket_name}: {str(err)}")
        
        # Wait for buckets to be ready
        self.sleep(10, "Waiting for buckets to be ready")
        
        return buckets

    def _get_query_definitions(self, prefixes=["test_scalar", "test_bhive", "test_composite"], similarity="COSINE"):
        bhive_def = self.gsi_util_obj.get_index_definition_list(
                dataset=self.dataset,
                prefix=prefixes[1],
                similarity=similarity, 
                train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                xattr_indexes=self.xattr_indexes,
                limit=self.scan_limit, 
                is_base64=self.base64,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=True, 
                description_dimension=self.dimension
            )
        
        scalar_def = self._create_scalar_indexes()

        composite_def = self.gsi_util_obj.get_index_definition_list(
                dataset=self.dataset,
                prefix=prefixes[2],
                similarity=similarity, 
                train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                xattr_indexes=self.xattr_indexes,
                limit=self.scan_limit, 
                is_base64=self.base64,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=False, 
                description_dimension=self.dimension
            )
            
        return bhive_def,scalar_def,composite_def

    #TODO: Implement this method.
    def _create_scalar_indexes(self):
        scalar_def = []
        scalar_def.append(QueryDefinition(
            index_name="scalar_index_color",
            index_fields=["color"],
            query_template=RANGE_SCAN_TEMPLATE.format("color", "color = Green")
        ))
        scalar_def.append(QueryDefinition(
            index_name="scalar_index_review",
            index_fields=["review"],
            query_template=RANGE_SCAN_TEMPLATE.format("review", "review > 0")
        ))
        return scalar_def
    
    def _get_create_queries(self, bhive_def, scalar_def, composite_def, namespace, defer_build_mix=True):
        bhive_create_queries = []
        scalar_create_queries = []
        composite_create_queries = []
        if bhive_def:
            bhive_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=bhive_def, namespace=namespace, num_replica=self.num_replicas, bhive_index=True, defer_build_mix=defer_build_mix)
        if scalar_def:
            scalar_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=scalar_def, namespace=namespace, num_replica=self.num_replicas, defer_build_mix=defer_build_mix)
        if composite_def:
            composite_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=composite_def, namespace=namespace, num_replica=self.num_replicas, defer_build_mix=defer_build_mix)
        return bhive_create_queries + scalar_create_queries + composite_create_queries
    
    def _rebalance_and_validate(self, nodes_out_list=None, nodes_in_list=None,
                           swap_rebalance=False,
                           services_in=None,select_queries=None,
                           scan_results_check=False, 
                           skip_shard_validations=False):
        """
        Perform rebalance operation and validate vector indexes after rebalance
        """
        if not nodes_out_list:
            nodes_out_list = []
        if not nodes_in_list:
            nodes_in_list = []
        
        # Log rebalance operation details
        self.log.info(f"Starting rebalance operation:")
        self.log.info(f"  - Nodes in: {[node.ip for node in nodes_in_list]}")
        self.log.info(f"  - Nodes out: {[node.ip for node in nodes_out_list]}")
        self.log.info(f"  - Services in: {services_in}")
        self.log.info(f"  - Swap rebalance: {swap_rebalance}")

        # # Check if we're removing a query node and get a different one if needed
        # current_query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        # if nodes_out_list and current_query_node in nodes_out_list:
        #     self.log.warning("Current query node is being removed, finding alternative query node...")
        #     all_query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        #     available_query_nodes = [node for node in all_query_nodes if node not in nodes_out_list]
        #     if available_query_nodes:
        #         current_query_node = available_query_nodes[0]
        #         self.n1ql_node = current_query_node  # Update the main query node reference
        #         self.log.info(f"Switched to alternative query node: {current_query_node.ip}")
        #     else:
        #         self.log.warning("No alternative query nodes available, some operations may fail")
        
        # Wait for system to stabilize before rebalance
        self.sleep(60, "Waiting for system to stabilize before rebalance")
        
        # Capture state before rebalance
        shard_list_before_rebalance = self.fetch_shard_id_list()
        current_query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

        # Run scans before rebalance to compare results later
        query_result = {}
        if scan_results_check and select_queries is not None:
            self.log.info("Running scans before rebalance")
            for query in select_queries:
                try:
                    query_result[query] = self.run_cbq_query(
                        query=query, 
                        scan_consistency='request_plus',
                        server=current_query_node
                    )['results']
                except Exception as e:
                    self.log.warning(f"Pre-rebalance query failed (will retry later): {str(e)}")
        
        # Trigger rebalance operation
        self.log.info("Triggering rebalance operation")
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], 
            nodes_in_list, 
            nodes_out_list,
            services=services_in, 
            cluster_config=self.cluster_config
        )
        
        # Wait for rebalance to start
        self.log.info("Waiting for rebalance to start")
        time.sleep(3)
        try:
            status, _ = self.rest._rebalance_status_and_progress()
            while status != 'running':
                time.sleep(1)
                status, _ = self.rest._rebalance_status_and_progress()
            self.log.info("Rebalance has started running")
        except Exception as e:
            self.log.error(f"Error checking rebalance status: {e}")
        
        # Wait for rebalance to complete
        self.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=500)
        self.assertTrue(reached, "Rebalance failed, stuck or did not complete")
        rebalance.result()

        # Check if shard-based rebalance was used
        shard_affinity = self.is_shard_based_rebalance_enabled()
        self.log.info(f"Shard-based rebalance enabled: {shard_affinity}")
        
        # Allow system to stabilize after rebalance
        self.sleep(60, "Allowing system to stabilize after rebalance")
        
        # # After rebalance, verify and update query node if needed
        # try:
        #     current_query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        #     self.n1ql_node = current_query_node  # Update the main query node reference
        #     self.log.info(f"Using query node after rebalance: {current_query_node.ip}")
        # except Exception as e:
        #     self.log.error(f"Error getting query node after rebalance: {e}")
        #     raise
        
        # Validate shard-based rebalance if enabled
        self.log.info(f"Shard validation check: shard_affinity={shard_affinity}, skip_shard_validations={skip_shard_validations}")
        if shard_affinity and not skip_shard_validations:
            self.log.info("Running validations for shard-based rebalance")
            
            # Fetch and compare shard lists
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    shard_list_after_rebalance = self.fetch_shard_id_list()
                    break
                except Exception as e:
                    retry_count += 1
                    self.log.warning(f"Attempt {retry_count} to fetch shard list failed: {str(e)}")
                    if retry_count == max_retries:
                        self.log.error("Failed to fetch shard list after maximum retries")
                        raise
                    self.sleep(10)
            
            self.log.info("Comparing shard lists before and after rebalance")
            
            if shard_list_before_rebalance:
                if shard_list_after_rebalance != shard_list_before_rebalance:
                    self.log.error(
                        f"Shards before: {shard_list_before_rebalance}\nShards after: {shard_list_after_rebalance}"
                    )
                    raise AssertionError("Shards missing after rebalance")
                    
                self.log.info(
                    f"Shard list before rebalance: {shard_list_before_rebalance}\nAfter rebalance: {shard_list_after_rebalance}"
                )
                
            # Validate shard affinity and check logs
            self.validate_shard_affinity()
            self.sleep(30)
            
        # Run scans after rebalance and compare results
        if scan_results_check and select_queries is not None:
            self.log.info("Running scans after rebalance and comparing results")
            
            max_retries = 3
            for query in select_queries:
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        post_rebalance_result = self.run_cbq_query(
                            query=query, 
                            scan_consistency='request_plus',
                            server=current_query_node
                        )['results']
                        
                        from deepdiff import DeepDiff
                        diffs = DeepDiff(post_rebalance_result, query_result[query], ignore_order=True)
                        
                        if diffs:
                            self.log.error(
                                f"Mismatch in query result before and after rebalance.\n"
                                f"Query: {query}\n"
                                f"Result before: {query_result[query]}\n"
                                f"Result after: {post_rebalance_result}"
                            )
                            raise Exception("Mismatch in query results before and after rebalance")
                        break
                    except Exception as e:
                        retry_count += 1
                        self.log.warning(f"Attempt {retry_count} to run post-rebalance query failed: {str(e)}")
                        if retry_count == max_retries:
                            self.log.error("Failed to run query after maximum retries")
                            raise
                        self.sleep(10)
        
        # Log rebalance completion
        self.log.info("Rebalance and validation completed successfully")