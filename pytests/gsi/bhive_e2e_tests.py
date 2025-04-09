"""bhive_vector_index.py: "This class test BHIVE Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "13/03/24 02:03 pm"

"""
import datetime
import json
import logging

import requests
from concurrent.futures import ThreadPoolExecutor

from docutils.nodes import definition_list
from requests.auth import HTTPBasicAuth
from sentence_transformers import SentenceTransformer

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition, FULL_SCAN_ORDER_BY_TEMPLATE, \
     RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE, RANGE_SCAN_TEMPLATE, RANGE_SCAN_ORDER_BY_TEMPLATE
#from gsi.base_gsi import BaseSecondaryIndexingTests
from gsi.gsi_file_based_rebalance import FileBasedRebalance
from membase.api.on_prem_rest_client import RestHelper
from membase.api.rest_client import RestConnection
from scripts.multilevel_dict import MultilevelDict
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView
from memcached.helper.data_helper import MemcachedClientHelper
from threading import Thread
from threading import Event
import random
import time
import copy
import threading


class BhiveVectorIndex(FileBasedRebalance):
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
        self.docs_per_collection = self.input.param("docs_per_collection", 100000)
        self.num_index_replica = self.input.param("num_index_replica", 1)
        self.dataset = self.input.param("dataset", "siftBigANN")
        self.graceful = self.input.param("graceful", True)
        self.HIGH_OPS = 20000
        self.LOW_OPS = 200
        self.HIGH_OPS_RATE = 1000
        self.LOW_OPS_RATE = 50
        self.rebalance_type = self.input.param("rebalance_type", "dcp")
        self.rebalance_order = self.input.param("rebalance_order", "in-out-swap-failover_kv-failover_index")
        self.ops_order = self.input.param("ops_order", "high_create_update-low_create_update-high_create_delete")
        self.index_ops_order = self.input.param("index_ops_order", "create-drop-create-drop-create-drop")
        self.log.info("==============  BhiveVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  BhiveVectorIndex tearDown has started ==============")
        super(BhiveVectorIndex, self).tearDown()
        self.log.info("==============  BhiveVectorIndex tearDown has ended ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass
    
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
        self.log.info("Step 1: Setting up the cluster with required services")
        self.log.info("Step 2: Creating buckets and collections")
        buckets = []
        
        buckets = self._create_test_buckets(num_buckets=3) 
        
        # Create collections and add to namespaces
        for bucket in buckets:
          self.prepare_collection_for_indexing(bucket_name=bucket, num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)


            
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        ops_tuple = tuple(self.ops_order.split("-"))
        if self.rebalance_type == "file":
            rest = RestConnection(index_nodes[0])
            settings = rest.get_global_index_settings()
            self.log.info(f"Global index settings: {settings}")
            if 'enableShardAffinity' in settings:
                rest.set_index_settings({'enableShardAffinity': True})
                self.log.info("Shard affinity enabled")
        
        # Create indexes in all collections
        self.log.info("Step 3: Creating indexes on all collections")
        simlarity_list = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]
        prefixes = ["test_scalar", "test_bhive", "test_composite"]
        for namespace in self.namespaces:
            self.log.info(f"Creating indexes on {namespace}")
            similarity = random.choice(simlarity_list)

            
            bhive_def, scalar_def, composite_def = self._get_query_definitions(prefixes, namespace, similarity)

            definitions = scalar_def + bhive_def + composite_def
            create_queries = self._get_create_queries(bhive_def, scalar_def, composite_def, namespace)
        
            self.gsi_util_obj.async_create_indexes(
                create_queries=create_queries, 
                database=namespace,
                query_node=query_node
            )
        
            # Wait for all indexes to be online
            self.log.info("Waiting for all indexes to come online")
            self.wait_until_indexes_online()
            self.log.info("All indexes are online")
        
            select_queries = self.gsi_util_obj.get_select_queries(
                definition_list=definitions,
                namespace=namespace,
                limit=self.scan_limit
            )

            self.log.info(f"Select queries: {select_queries}")

            for query in select_queries:
                self.log.info(f"Running query: {query}")
                # self.run_cbq_query(query=create)
                if "embVector" in query:
                    query = query.replace("embVector", str(self.sample_vector))
                if "DISTINCT" in query or "ANN" not in query:
                    continue
                try:
                    query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query,
                                                                                    similarity=similarity,
                                                                                    scan_consitency=True)
                    self.log.info(f"Query: {query}, Recall: {recall}, Accuracy: {accuracy}")
                    #TODO: Fail the test if recall is less than 0.7
                    # if recall < 0.70:
                    #     raise Exception(f"Recall is less than 0.70 for query {query}")
                except Exception as err:
                    self.log.error(f"Error validating select query {query}: {str(err)}")
                    raise err
    
        self.log.info("Step 4: Loading data into all collections")
        # Load data into all collections
        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace)
        
        self.log.info("Step 5: Running pre-rebalance validations")
        self._run_validations(select_queries,index_nodes)

        self.log.info("About to start workload.")

        original_log_level = logging.getLogger().level

        # Step 6: Add a new node and rebalance
        self.log.info("Step 6: Starting rebalance operations")
        self.log.info(f"Servers: {self.servers}")
        new_node = self.servers[-1]
        swap_rebalance_in = index_nodes[1]
        swap_rebalance_out = new_node
        rebalance_order_arr = self.rebalance_order.split("-")
        for rebalance_type in rebalance_order_arr:
            workload_stop_event = Event()
            nodes_in_list = []
            nodes_out_list = []
            services_in = []
            if rebalance_type == "in":
                self.log.info("Starting rebalance-in operation")
                nodes_in_list = [new_node]
                services_in = ["index"]
            elif rebalance_type == "out":
                self.log.info("Starting rebalance-out operation")
                nodes_out_list = [self.get_index_nodes_from_services_map(service_type="index", get_all_nodes=True)[1]]
            elif rebalance_type == "swap":
                self.log.info("Starting swap rebalance operation")
                nodes_out_list = [swap_rebalance_in]
                nodes_in_list = [swap_rebalance_out]
                services_in = ["index"]
            elif "failover" in rebalance_type:
                service_type = rebalance_type.split("_")[1]  # Should be "kv" or "index"
                
                self.log.info(f"Starting failover-{service_type} operation")
                
                # Get the node to fail over based on service type
                failover_node = self.get_nodes_from_services_map(
                    service_type=service_type, 
                    get_all_nodes=True
                )[-1]
                
                # Execute the failover
                failover_task = self.cluster.async_failover(
                    self.servers[:self.nodes_init],
                    [failover_node],
                    self.graceful,
                    wait_for_pending=120
                )
                failover_task.result()
            
            try:
                logging.getLogger().setLevel(logging.WARNING)
                self.log.warning(f"Starting background workloads for {rebalance_type}")
                with ThreadPoolExecutor() as executor:
                    try:
                        bucket_0_thread, bucket_1_thread, bucket_2_thread = self._run_mutations_workload(executor, workload_stop_event, ops_tuple)
                        self.sleep(10, "Allowing workloads to stabilize before rebalance")

                        logging.getLogger().setLevel(original_log_level)
                        self.log.info(f"Starting {rebalance_type} operation")

                        self.rebalance_and_validate(
                            nodes_in_list=nodes_in_list,
                            nodes_out_list=nodes_out_list,
                            swap_rebalance=False,
                            services_in=services_in,
                            select_queries=None,
                        )
                        logging.getLogger().setLevel(logging.WARNING)
                        workload_stop_event.set()

                        self._finish_mutations_workload(bucket_0_thread, bucket_1_thread, bucket_2_thread)
                    finally:
                        self.log.info(f"All workloads for {rebalance_type} operation completed")
                    # Run validations after rebalance
            except Exception as err:
                self.log.error(f"Error during {rebalance_type} operation: {str(err)}")
                raise err
            finally:
                self.log.info(f"{rebalance_type} operation completed")
            self._run_validations(select_queries,self.get_nodes_from_services_map(service_type="index", get_all_nodes=True))
            logging.getLogger().setLevel(original_log_level)

        self.log.info("All rebalance operations completed")
        self.log.info("Dropping all indexes")
        self.drop_all_indexes()
        self.log.info("Performing CPU validations")
        # TODO uncomment after https://jira.issues.couchbase.com/browse/MB-65934 is fixed
        # if not self.validate_memory_released():
        #     raise AssertionError("Memory not released despite dropping all the indexes")
        self.check_storage_directory_cleaned_up()
        if not self.validate_cpu_normalized:
            self.log.error("CPU normalized validation failed")
            raise AssertionError("CPU normalized validation failed")
        self.log.info("Comprehensive rebalance test completed successfully")
    
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
        self.run_cbq_query("CREATE PRIMARY INDEX idxtb0 ON default:test_bucket_0._default._default", server=query_node)
        self.run_cbq_query("CREATE PRIMARY INDEX idxtb1 ON default:test_bucket_1._default._default", server=query_node)
        count = 0
        for namespace in self.namespaces:
            self.run_cbq_query(f"CREATE PRIMARY INDEX idx{count} ON {namespace}", server=query_node)
            count += 1

        # Creating event flag
        workload_stop_event = Event()

        for namespace in self.namespaces:
            self._load_docs_in_collection(namespace)
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
                self.run_cbq_query(query=query, server=query_node)
            

        workload_stop_event.set()
        self._finish_mutations_workload(bucket_0_thread, bucket_1_thread)
        # Drop all the indexes
        self.log.info("Dropping all the indexes")
        self.drop_all_indexes()

        self.log.info("Performing CPU validations")
        # TODO uncomment after https://jira.issues.couchbase.com/browse/MB-65934 is fixed
        # if not self.validate_memory_released():
        #     raise AssertionError("Memory not released despite dropping all the indexes")
        self.check_storage_directory_cleaned_up()
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
            self.run_cbq_query(query=query, server=query_node)
            if workload_stop_event.is_set():
                return    

    def _finish_mutations_workload(self, bucket_0_thread = None, bucket_1_thread = None, bucket_2_thread = None):
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

    def _load_docs_in_collection(self, namespace):
        bucket, scope, collection = namespace.split(':')[-1].split('.')
        self.log.info(f"Loading data into {namespace}")
        self.gen_create = SDKDataLoader(
                num_ops=self.docs_per_collection, 
                percent_create=100,
                percent_update=0, 
                percent_delete=0, 
                scope=scope,
                collection=collection, 
                json_template=self.json_template,
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
        self.log.info("Validating index item counts")
        self.compare_item_counts_between_kv_and_gsi()
        
        # Validate replica indexes have equal item counts
        self.log.info("Validating replica index item counts")
        self.validate_replica_indexes_item_counts()

        # Validate shard seggregation
        self.validate_shard_seggregation()
        
        # Check mainstore and backstore item counts match
        self.log.info("Validating mainstore and backstore consistency")
        errors = self.backstore_mainstore_check()
        if errors:
            self.log.error(f"Mainstore/backstore mismatch: {errors}")
            self.fail("Mainstore and backstore item counts don't match")
        
        # Check for recall and accuracy
        # TODO: Add a check for recall and accuracy using the groundtruths instead of the below function!
        self.log.info("Validating vector recall and accuracy")
        for query in select_queries:
            self.log.info(f"Validating query: {query}")
            _, recall, accuracy = self.validate_scans_for_recall_and_accuracy(
                select_query=query,
                similarity=similarity,
            )
            self.log.info(f"Query recall: {recall*100}%, accuracy: {accuracy*100}%")
            self.assertGreaterEqual(recall*100, 70, f"Recall for query {query} is less than threshold 70%")

        
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

    def _perform_docloader_operations(self, event, bucket_name, ops_rate, timeout=1800, num_docs_mutating=10000,percent_create = 0, percent_update = 0, percent_delete = 0, percent_expiry = 0, percent_read = 0, cs = 0, ce = 0, us = 0, ue = 0, ds = 0, de = 0, es = 0, ee = 0, rs = 0, re = 0):
        collection_namespaces = self.namespaces
        time_now = time.time()
        while not event.is_set() and time.time() - time_now < timeout:
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
                bhive_index=False, 
                description_dimension=self.dimension
            )
            
        return bhive_def,[scalar_def],[composite_def]

    #TODO: Implement this method.
    def _create_scalar_indexes(self):
        return []
    
    def _get_create_queries(self, bhive_def, scalar_def, composite_def, namespace):
        bhive_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=bhive_def, namespace=namespace, num_replica=self.num_replicas, bhive_index=True, defer_build_mix=True)
        scalar_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=scalar_def, namespace=namespace, num_replica=self.num_replicas, defer_build_mix=True)
        composite_create_queries = self.gsi_util_obj.get_create_index_list(definition_list=composite_def, namespace=namespace, num_replica=self.num_replicas, defer_build_mix=True)
        return bhive_create_queries + scalar_create_queries + composite_create_queries
