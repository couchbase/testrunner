"""bhive_vector_index.py: "This class test BHIVE Vector indexes  for GSI"

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

    def test_failover_nodes(self):
        self.log.info("Step 1: Setting up cluster with file-based rebalance disabled")
        self.log.info("Step 2 : Creating bucket")
        buckets = self._create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket, 
                num_scopes=1, 
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template, 
                load_default_coll=False
            )
        
        # create_query = f"""CREATE VECTOR INDEX test_idx_1 ON `test_bucket_0` (embedding VECTOR) USING GSI WITH {{"dimension": 128, 
        #                         "similarity": "L2_SQUARED",
        #                         "description": "IVF,SQ8"}}"""
        # self.log.info(f"Creating index on {bucket}")
        # self.run_cbq_query(query=create_query, server=query_node)

        
        self.sleep(36000, "Sleeping for 30600 seconds to ensure documents are loaded")
            

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
        prefixes = ["test_scalar", "test_bhive", "test_composite"]
        similarity = "COSINE"

        for namespace in self.namespaces:
            self.log.info(f"Creating index on {namespace}")
            self.log.info("Getting query definitions...")
            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, namespace, similarity)
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
        create_query = f"""CREATE VECTOR INDEX {index_name} ON `test_bucket_0` (embedding VECTOR) USING GSI WITH {{"dimension": 128, 
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
                drop_query = f"DROP INDEX {index_name} ON `test_bucket_0`"
                self.run_cbq_query(query=drop_query, server=query_node)
                return drop_id, None  # Success
            except Exception as e:
                return drop_id, str(e)  # Return the error
        
        # Submit concurrent drop requests
        self.log.info("Submitting concurrent drop requests")
        num_concurrent_drops = 5
        drop_results = []
        
        with ThreadPoolExecutor(max_workers=num_concurrent_drops) as executor:
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
        prefixes = ["test_scalar", "test_bhive", "test_composite"]
        similarity = "COSINE"
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
            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, namespace, similarity)
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
            bhive_def = self.gsi_util_obj.get_index_definition_list(
                dataset=self.dataset,
                prefix="test_bhive",
                similarity="L2_SQUARED", 
                train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                xattr_indexes=self.xattr_indexes,
                limit=self.scan_limit, 
                is_base64=self.base64,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=True, 
                description_dimension=4097
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=bhive_def, namespace=namespace)
            creation_success = False
            try:
                self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace, query_node=query_node)
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
        prefixes = ["test_scalar", "test_bhive", "test_composite"]
        select_queries = []
        create_queries = []
        
        self.log.info("Creating indexes for each namespace...")
        for namespace in self.namespaces:
            self.log.info(f"Processing namespace: {namespace}")
            similarity = random.choice(simlarity_list)
            self.log.info(f"Selected similarity metric: {similarity}")
            
            self.log.info("Getting query definitions...")
            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, namespace, similarity)
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
        for rebalance_type in rebalance_order_arr:
            self.log.info(f"=== Starting {rebalance_type} rebalance operation ===")
            workload_stop_event = Event()
            nodes_in_list = []
            nodes_out_list = []
            services_in = []
            
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
                                skip_shard_validations=self.skip_shard_validations,
                            )
                            self.log.info(f"Rebalance operation {rebalance_type} completed successfully")
                            
                            swap_rebalance = False
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
                            skip_shard_validations=self.skip_shard_validations,
                        )
                    self.log.info(f"Rebalance operation {rebalance_type} completed successfully")
                
                swap_rebalance = False
            except Exception as err:
                self.log.error(f"Error during {rebalance_type} operation: {str(err)}")
                raise err
            finally:
                self.log.info(f"{rebalance_type} operation completed")
                
            self.log.info("Sleeping for 2 minutes to allow rebalance to complete...")
            self.sleep(120, "Sleeping for 2 minutes to allow rebalance to complete")
            
            self.log.info("Running post-rebalance validations...")
            self._run_validations(select_queries,self.get_nodes_from_services_map(service_type="index", get_all_nodes=True))
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

            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, namespace, similarity)
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

            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, namespace, similarity)
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
        
    def _run_validations(self, select_queries,  index_nodes, similarity="L2_SQUARED"):
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
        shard_map = self.get_shards_index_map()
        self.validate_shard_seggregation(shard_index_map=shard_map)
        
        # Check mainstore and backstore item counts match
        self.log.info("Validating mainstore and backstore consistency")
        errors = self.backstore_mainstore_check()
        if errors:
            self.log.error(f"Mainstore/backstore mismatch: {errors}")
            self.fail("Mainstore and backstore item counts don't match")
        
        if self.rebalance_type == "file":
            # Validate shard affinity
            self.log.info("Validating shard affinity")
            self.validate_shard_affinity()
            
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

    def _get_query_definitions(self, prefixes, namespace, similarity):
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
        bhive_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=bhive_def, namespace=namespace, num_replica=self.num_replicas, bhive_index=True, defer_build_mix=defer_build_mix)
        scalar_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=scalar_def, namespace=namespace, num_replica=self.num_replicas, defer_build_mix=defer_build_mix)
        # scalar_create_queries = []
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