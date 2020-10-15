"""collections_indexes_rebalance.py: Test Cases for gsi with rebalance

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "14/10/20 1:10 pm"
"""

import re

from concurrent.futures import ThreadPoolExecutor
from itertools import combinations, chain

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection

from .base_gsi import BaseSecondaryIndexingTests
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats


class CollectionIndexesRebalance(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionIndexesRebalance, self).setUp()
        self.log.info("==============  ConcurrentIndexes setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_concurrent_indexes = self.input.param("num_concurrent_indexes", 10)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.num_of_indexes = self.input.param('num_of_indexes', 1)
        self.services_in = self.input.param('services_in', None)
        self.server_out = self.input.param('server_out', None)
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
        self.err_msg1 = 'The index is scheduled for background creation'
        self.err_msg2 = 'Index creation will be retried in background'
        self.err_msg3 = 'will retry building in the background for reason: Build Already In Progress.'
        self.system_query = "select * from system:indexes"
        self.log.info("==============  ConcurrentIndexes setup has completed ==============")
    
    def tearDown(self):
        self.log.info("==============  ConcurrentIndexes tearDown has started ==============")
        super(CollectionIndexesRebalance, self).tearDown()
        self.log.info("==============  ConcurrentIndexes tearDown has completed ==============")
    
    def suite_tearDown(self):
        pass
    
    def suite_setUp(self):
        pass

    def test_rebalance_swap_with_indexer(self):
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1', 'phone']
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            index_gen_query_list.append(query)
    
        tasks = []
        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        self.log.info(f"{index_name} is scheduled for background")
                    elif self.err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), 10 * (self.num_replicas + 1))
        
        self.log.info("Swaping out Indexer node B with C and D")
        gen_create = SDKDataLoader(num_ops=10**4, percent_create=100, percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, json_template='Person', key_prefix="new_doc_")

        add_nodes = [self.servers[2]]
        remove_node = [self.servers[1]]
        tasks = []
        tasks.append(self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                  to_remove=remove_node, services=['index', 'index']))
        tasks.extend(self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=10 ** 4))
        for task in tasks:
            task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), 10 * (self.num_replicas + 1))

    def test_rebalance_indexer_nodes_with_multiple_BSC(self):
        num_of_docs = 10 ** 4
        self.rest.delete_all_buckets()
        bucket_1 = 'test_bucket_1'
        bucket_2 = 'test_bucket_2'
        self.cluster.create_standard_bucket(name=bucket_1, port=11222, bucket_params=self.bucket_params)
        self.cluster.create_standard_bucket(name=bucket_2, port=11222, bucket_params=self.bucket_params)
        collection_namespaces = []
        scope_prefix = 'test_scope_'
        collection_prefix = 'test_collection_'
    
        data_load_tasks = []
        for bucket in (bucket_1, bucket_2):
            for s_item in range(self.num_scopes):
                scope = f'{scope_prefix}_{s_item}'
                self.cli_rest.create_scope(bucket=bucket, scope=scope)
                for c_item in range(self.num_collections):
                    collection = f'{collection_prefix}_{c_item}'
                    self.cli_rest.create_collection(bucket=bucket, scope=scope, collection=collection)
                    self.sleep(10)
                    gen_create = SDKDataLoader(num_ops=num_of_docs, percent_create=100,
                                               percent_update=0, percent_delete=0, scope=scope,
                                               collection=collection, json_template='Person')
                    task =self.cluster.async_load_gen_docs(self.master, bucket, gen_create, timeout_secs=300)
                    data_load_tasks.append(task)
                    collection_namespaces.append(f'default:{bucket}.{scope}.{collection}')
        for task in data_load_tasks:
            task.result()

        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_build_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1', 'phone']
        for collection_namespace in collection_namespaces:
            for index_fields, idx_num in zip(index_field_list, range(10)):
                index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=[index_fields])
                index_gen_list.append(index_gen)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=1)
                build_query = index_gen.generate_build_query(namespace=collection_namespace)
                index_gen_query_list.append(query)
                index_build_query_list.append(build_query)
    
        tasks = []
        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
        
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        self.log.info(f"{index_name} is scheduled for background")
                    elif self.err_msg2 in str(err):
                        continue
                    elif self.err_msg3 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        index_meta_info = self.rest.get_indexer_metadata()['status']
        # self.assertEqual(len(index_meta_info), 10 * (self.num_replicas + 1) )

        tasks = []
        self.log.info("Swapping out Indexer node B with C and D")
        add_nodes = [self.servers[2]]
        remove_node = [self.servers[1]]
        try:
            tasks.append(self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                      to_remove=remove_node, services=['index', 'index']))
            for collection_namespace in collection_namespaces:
                _, keyspace = collection_namespace.split(':')
                bucket, scope, collection = keyspace.split('.')
                gen_create = SDKDataLoader(num_ops=10 ** 3, percent_create=100, percent_update=0, percent_delete=0,
                                           scope=scope, collection=collection, json_template='Person',
                                           key_prefix="new_doc_")
                tasks.extend(self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=10 ** 4))
            for task in tasks:
                task.result()
        except Exception as err:
            self.fail(err)
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        self.wait_until_indexes_online(defer_build=self.defer_build)
        if self.defer_build:
            for build_query in index_build_query_list:
                self.run_cbq_query(query=build_query)
        self.wait_until_indexes_online(timeout=1800)
        index_meta_info = self.rest.get_indexer_metadata()['status']
        pass
