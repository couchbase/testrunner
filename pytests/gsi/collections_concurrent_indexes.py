"""collections_concurrent_indexes.py: Test Cases for Concurrent Index creations

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "24/07/20 1:10 pm" 

"""
import re

from concurrent.futures import ThreadPoolExecutor
from itertools import combinations, chain

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from .base_gsi import BaseSecondaryIndexingTests
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats


def powerset(iter_item):
    # todo: will move to some other place later
    """powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"""
    s = list(iter_item)
    return list(chain.from_iterable(combinations(s, r) for r in range(len(s) + 1)))[1:]


class ConcurrentIndexes(BaseSecondaryIndexingTests):
    def setUp(self):
        super(ConcurrentIndexes, self).setUp()
        self.log.info("==============  ConcurrentIndexes setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_concurrent_indexes = self.input.param("num_concurrent_indexes", 10)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.num_of_indexes = self.input.param('num_of_indexes', 1)
        self.bucket_params = self._create_bucket_params(server=self.master, size=100,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.cli_rest = CollectionsRest(self.master)
        self.stat = CollectionsStats(self.master)
        self.scope_prefix = 'test_scope'
        self.collection_prefix = 'test_collection'
        self.run_cbq_query = self.n1ql_helper.run_cbq_query
        index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                    'suffix', 'filler1', 'phone', 'zipcode'])
        self.index_field_list = [list(item) for item in index_field_set]
        self.err_msg1 = 'The index is scheduled for background creation'
        self.err_msg2 = 'Index creation will be retried in background'
        self.system_query = "select * from system:indexes"
        self.log.info("==============  ConcurrentIndexes setup has completed ==============")
    
    def tearDown(self):
        self.log.info("==============  ConcurrentIndexes tearDown has started ==============")
        super(ConcurrentIndexes, self).tearDown()
        self.log.info("==============  ConcurrentIndexes tearDown has completed ==============")
    
    def suite_tearDown(self):
        pass
    
    def suite_setUp(self):
        pass
    
    def test_create_concurrent_indexes(self):
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
        
        tasks = []
        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
                if count % 10 == 0:
                    self.sleep(5, "Running create index in batch of 10")
            
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    count_err_1, count_err_2 = 0, 0
                    if self.err_msg1 in str(err):
                        count_err_1 += 1
                    elif self.err_msg2 in str(err):
                        count_err_2 += 2
                    else:
                        self.fail(err)
            self.log.info(f"No of concurrent indexes issued: "
                          f"{len(self.err_msg1)}/{len(self.err_msg1) + len(self.err_msg2)}")
        
        result = self.wait_until_indexes_online(timeout=20 * self.num_of_indexes)
        if not result:
            self.log.warn("All indexes didn't build in given timeout. Increase timeout or check logs")
        self.sleep(10)
        index_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_info), len(index_gen_query_list), "No. of indexes is not matching to expected value")
        for index in index_info:
            self.assertEqual(index['status'], 'Ready', index['status'])
            self.assertEqual(index['completion'], 100, index['completion'])
            self.assertFalse(index['stale'], index['stale'])
        
        for index_gen in index_gen_list:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
    
    def test_create_concurrent_indexes_across_different_keyspace(self):
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, num_scopes=2)
        collection_namespace_1, collection_namespace_2 = self.namespaces
        
        self.default_bucket_name = 'default'
        self.cluster.create_standard_bucket(name=self.default_bucket_name, port=11222,
                                            bucket_params=self.bucket_params)
        default_bucket_namespaces = []
        for s_item in range(2):
            scope = f'test_scope_{s_item + 1}'
            collection = 'test_collection_1'
            self.cli_rest.create_scope_collection(bucket=self.default_bucket_name, scope=scope,
                                                  collection=collection)
            self.sleep(10, "Allowing time after collection creation")
            gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, json_template='Person')
            task = self.cluster.async_load_gen_docs(server=self.master, bucket=self.default_bucket_name,
                                                    generator=gen_create, timeout_secs=300)
            task.result()
            default_bucket_namespaces.append(f'default:{self.default_bucket_name}.{scope}.{collection}')
        
        collection_namespace_3, collection_namespace_4 = default_bucket_namespaces
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        for idx_num, index_fields in zip(range(self.num_of_indexes), self.index_field_list):
            index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace_1,
                                                          defer_build=self.defer_build)
            index_gen_query_list.append(query)
            query = index_gen.generate_index_create_query(namespace=collection_namespace_2,
                                                          defer_build=self.defer_build)
            index_gen_query_list.append(query)
            query = index_gen.generate_index_create_query(namespace=collection_namespace_3,
                                                          defer_build=self.defer_build)
            index_gen_query_list.append(query)
            query = index_gen.generate_index_create_query(namespace=collection_namespace_4,
                                                          defer_build=self.defer_build)
            index_gen_query_list.append(query)
        
        tasks = []
        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
                if count % 10 == 0:
                    self.sleep(2, "Running create index in batch of 10")
            
            count_err_1, count_err_2 = 0, 0
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if self.err_msg1 in str(err):
                        count_err_1 += 1
                    elif self.err_msg2 in str(err):
                        count_err_2 += 2
                    else:
                        self.fail(err)
            self.log.info(f"No of concurrent indexes issued: {count_err_1}/{count_err_1 + count_err_2}")
        
        result = self.wait_until_indexes_online(timeout=20 * self.num_of_indexes * 4)
        if not result:
            self.log.warn("All indexes didn't build in given timeout. Increase timeout or check logs")
        self.sleep(20)
        index_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_info), len(index_gen_query_list), "No. of indexes is not matching to expected value")
        for index in index_info:
            self.assertEqual(index['status'], 'Ready', index['status'])
            self.assertEqual(index['completion'], 100, index['completion'])
            self.assertFalse(index['stale'], index['stale'])
        
        for index_gen in index_gen_list:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace_1)
            self.run_cbq_query(query=query)
            query = index_gen.generate_index_drop_query(namespace=collection_namespace_2)
            self.run_cbq_query(query=query)
    
    def test_monitor_schedule_indexes(self):
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
        
        tasks = []
        scheduled_indexes = {}
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
                if count % 10 == 0:
                    self.sleep(5, "Running create index in batch of 10")
            
            count_err_1, count_err_2 = 0, 0
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if self.err_msg1 in str(err):
                        count_err_1 += 1
                        out = re.search(regex_pattern, str(err))
                        if out:
                            index_id = out.groups()[0]
                            scheduled_indexes[index_id] = False
                    elif self.err_msg2 in str(err):
                        count_err_2 += 2
                    else:
                        self.fail(err)
            self.log.info(f"No of concurrent indexes issued: {count_err_1}/{count_err_1 + count_err_2}")
        
        self.sleep(1)
        
        # Verifying schedule indexes is reported in system:indexes
        query = "select * from system:indexes"
        result = self.run_cbq_query(query=query)['results']
        index_state = {item['indexes']['name']: item['indexes']['state'] for item in result}
        self.assertTrue('scheduled for creation' in index_state.values(), index_state)
        
        # verifying schedule indexes is reported in REST call
        count = 0
        index_info = False
        while count < 100:
            status_list = []
            index_info = self.rest.get_indexer_metadata()['status']
            for index in index_info:
                index_status = index['status']
                index_name = index['indexName']
                if index_name in scheduled_indexes:
                    if index_status == 'Ready':
                        status_list.append(True)
                    elif index_status == 'Scheduled for Creation':
                        status_list.append(False)
                        scheduled_indexes[index_name] = True
                    else:
                        status_list.append(False)
                else:
                    if index_status == 'Ready':
                        status_list.append(True)
                    elif index_status == 'Scheduled for Creation':
                        self.fail(f"Index {index_status['indexName']} shouldn't be scheduled for background creation")
                    else:
                        status_list.append(False)
            self.sleep(5)
            count += 1
            if all(status_list):
                break
        else:
            self.log.error("Not all indexes are ready for use. Check logs")
            self.fail(index_info)
        
        self.assertTrue(any(status_list), "None of the indexes were in Schedule mode")
        for index_gen in index_gen_list:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        pass
    
    def test_drop_indexes_schedule_for_background_creation(self):
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
        
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        scheduled_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        drop_query = f'drop index {index_name} on {collection_namespace}'
                        scheduled_indexes.append(index_name)
                        try:
                            self.run_cbq_query(query=drop_query)
                        except Exception as drop_err:
                            self.log.info(drop_err)
                    elif self.err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        self.sleep(10)
        result = self.run_cbq_query(query=self.system_query)['results']
        self.assertEqual(len(result), len(index_gen_list) - len(scheduled_indexes),
                         "No. of indexes available are not matching expected no.s")
    
    def test_schedule_indexes_with_drop_bsc(self):
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
        
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        scheduled_indexes = []
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        scheduled_indexes.append(index_name)
                    elif self.err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
            self.delete_bucket_scope_collection(server=self.servers[0], delete_item=self.item_to_delete, bucket=bucket,
                                                scope=scope, collection=collection)
        self.sleep(10)
        result = self.run_cbq_query(query=self.system_query)['results']
        self.assertFalse(result)
    
    def test_run_scan_on_scheduled_indexes(self):
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields
        
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        where_field = index_fields_list[index_name][0]
                        try:
                            select_query = f'select {where_field} from {collection_namespace} ' \
                                           f'where {where_field} is not NULL'
                            self.run_cbq_query(query=select_query)
                        except Exception as err:
                            err_msg = 'No index available on keyspace test_collection_1 that matches your query'
                            self.assertTrue(err_msg in str(err), "Error msg not matching. Check logs")
                    elif self.err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.log.info("Waiting for all indexes to be online...")
        self.wait_until_indexes_online()
        self.sleep(10)
        result = self.run_cbq_query(query=self.system_query)['results']
        self.assertEqual(len(result), len(index_gen_list), "No. of indexes are not matching with expected value")
    
    def test_schedule_indexes_on_specific_node(self):
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        node_a, node_b = None, None
        for node in index_nodes:
            if node.ip != self.master.ip:
                if not node_a:
                    node_a = node.ip
                else:
                    node_b = node.ip
                    break
        
        if len(index_nodes) < 3:
            self.fail("Need at least 3 nodes")
        index_nodes_to_be_used = [node_a, node_b]
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            deploy_node_info = [f"{node}:8091" for node in index_nodes_to_be_used]
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=self.num_replicas,
                                                          deploy_node_info=deploy_node_info)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields
        
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        schedule_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        schedule_indexes.append(index_name)
                    elif self.err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.log.info("Waiting for all indexes to be online...")
        self.wait_until_indexes_online()
        
        index_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_info), len(index_gen_list) * (self.num_replicas + 1))
        node_a_indexes, node_b_indexes = [], []
        for index in index_info:
            index_host = index['hosts'][0].split(':')[0]
            self.assertNotEqual(self.master.ip, index_host,
                                "Index is deployed to node not specified in create command")
            if index_host == node_a:
                node_a_indexes.append(index['name'])
            else:
                node_b_indexes.append(index['name'])
        for index in node_a_indexes:
            if 'replica' in index:
                break
        else:
            self.fail("Indexes and replicas are not distruted")
        for index in node_b_indexes:
            if 'replica' in index:
                break
        else:
            self.fail("Indexes and replicas are not distruted")
        
        for index_name in index_fields_list:
            where_field = index_fields_list[index_name][0]
            select_query = f'select count({where_field}) from {collection_namespace} where {where_field} is not NULL'
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertTrue(result > 0)

    def test_build_of_deferred_schedule_indexes(self):
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields
    
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        schedule_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
        
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        schedule_indexes.append(index_name)
                    elif self.err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.log.info("Waiting for all indexes to be online...")
        self.wait_until_indexes_online(defer_build=True)
        
        build_query = f'BUILD INDEX ON {collection_namespace}({", ".join(index_fields_list.keys())}) USING GSI'
        self.run_cbq_query(query=build_query)
        self.wait_until_indexes_online()

        # if no query throws error, that mean all indexes are fine
        for index_name in index_fields_list:
            where_field = index_fields_list[index_name][0]
            select_query = f'select count({where_field}) from {collection_namespace} ' \
                           f'where {where_field} is not NULL'
            self.run_cbq_query(query=select_query)

    def test_retries_for_failed_schedule_indexes(self):
        # Will use this num to configure no. of retries
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 10)
        #todo: add configurable param for retries. Not available now.
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 3 nodes")
        node_a, node_b = index_nodes

        node_b_rest = RestConnection(node_b)
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields
    
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        schedule_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

            try:
                self.sleep(2)
                self.log.info("Blocking Indexer nodes communication to Data node")
                self.block_incoming_network_from_node(self.master, node_b)
                self.block_incoming_network_from_node(self.master, node_a)
                self.sleep(5)
                self.log.info("Resuming communication of one Indexer node to Data node")
                self.resume_blocked_incoming_network_from_node(self.master, node_a)
                self.sleep(6*num_retries_for_failed_index)
                # self.log.info("Resuming communication of another Indexer node to Data node after
                # retries are exhausted")
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif self.err_msg2 in str(err):
                            continue
                        else:
                            self.log.info(err)
            except Exception as err:
                self.resume_blocked_incoming_network_from_node(self.master, node_a)
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
                self.fail(err)
        self.wait_until_indexes_online()
        index_info = node_b_rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_info), len(index_gen_list), "No. of indexes not matching expected count")

    def test_drop_failed_scheduled_indexes(self):
        # Will use this num to configure no. of retries
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 10)
        # todo: add configurable param for retries. Not available now.
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 3 nodes")
        node_a, node_b = index_nodes
    
        node_b_rest = RestConnection(node_b)
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields
    
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        schedule_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
        
            try:
                self.sleep(5)
                self.log.info("Blocking Indexer nodes communication to Data node")
                self.block_incoming_network_from_node(self.master, node_b)
                self.sleep(5 * num_retries_for_failed_index + 1)
                failed_index_count = 0
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif self.err_msg2 in str(err):
                            continue
                        else:
                            failed_index_count += 1
                            self.log.info(err)
            except Exception as err:
                self.fail(err)
            finally:
                for index_name in index_fields_list:
                    drop_query = f'drop index {index_name} on {collection_namespace}'
                    self.run_cbq_query(query=drop_query)
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
        index_info = node_b_rest.get_indexer_metadata()['status']
        # self.assertEqual(len(index_info), len(index_gen_list), "No. of indexes not matching expected count")

    def test_index_creation_with_rebalanced_out_assigned_index_node(self):
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 3 nodes")
        node_a, node_b = None, None
        for node in index_nodes:
            if node.ip != self.master.ip:
                if node_a:
                    node_b = node
                    break
                node_a = node
            else:
                continue

        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        index_nodes_to_be_used = [node_a.ip, node_b.ip]
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            deploy_node_info = [f"{node}:8091" for node in index_nodes_to_be_used]
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          deploy_node_info=deploy_node_info)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields

        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        schedule_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
    
            try:
                self.sleep(2)
                self.log.info("Blocking Indexer nodes communication to Data node")
                self.log.info("Blocking node B so that schedule indexes move other node in cluster")
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_b])
                rebalance.result()
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif self.err_msg2 in str(err):
                            continue
                        else:
                            self.log.info(err)
            except Exception as err:
                self.log.info(err)
            finally:
                self.wait_until_indexes_online()
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
        index_info = self.rest.get_indexer_metadata()['status']
        pass

    def test_index_creation_with_failover_of_assigned_index_node(self):
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 3 nodes")
        node_a, node_b = None, None
        for node in index_nodes:
            if node.ip != self.master.ip:
                if node_a:
                    node_b = node
                    break
                node_a = node
            else:
                continue
    
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        index_nodes_to_be_used = [node_a.ip, node_b.ip]
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            deploy_node_info = [f"{node}:8091" for node in index_nodes_to_be_used]
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          deploy_node_info=deploy_node_info)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields
    
        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        schedule_indexes = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
        
            try:
                self.sleep(5)
                self.cluster.failover(servers=self.servers, failover_nodes=[node_b])
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif self.err_msg2 in str(err):
                            continue
                        else:
                            self.log.info(err)
            except Exception as err:
                self.log.info(err)
            finally:
                self.wait_until_indexes_online()
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
        index_info = self.rest.get_indexer_metadata()['status']
