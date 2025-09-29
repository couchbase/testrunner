"""collections_indexes_rebalance.py: Test Cases for gsi with rebalance

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "14/10/20 1:10 pm"
"""
import re
import time

from concurrent.futures import ThreadPoolExecutor
from itertools import combinations, chain

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection

from .base_gsi import BaseSecondaryIndexingTests, ConCurIndexOps
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from tasks.taskmanager import TaskManager


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
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
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
        self.err_msg4 = 'Create index or Alter replica cannot proceed due to another concurrent create index request'
        self.system_query = "select * from system:indexes"
        self.log.info("==============  ConcurrentIndexes setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  ConcurrentIndexes tearDown has started ==============")
        super(CollectionIndexesRebalance, self).tearDown()
        self.log.info("==============  ConcurrentIndexes tearDown has completed ==============")

    def test_multiple_type_indexes_with_rebalance(self):
        unique_index_type_per_collection = 8
        num_of_docs = self.num_of_docs_per_collection
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        self.run_tasks = True
        self.index_ops_obj = ConCurIndexOps()
        self.index_create_task_manager = TaskManager("index_create_task_manager")
        self.index_create_task_manager.start()
        self.n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)

        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, num_scopes=self.num_scopes,
                                             num_collections=self.num_collections, json_template='Employee')
        self.update_keyspace_list(bucket=self.test_bucket)
        index_create_tasks = self.create_indexes(num=3*3*unique_index_type_per_collection*2,
                                                 query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        result = self.wait_until_indexes_online()
        if not result:
            self.log.error("Indexes status got timed out. Check logs or increase timeout")

        before_rebalance_index_meta_info = self.rest.get_indexer_metadata()['status']
        for index in before_rebalance_index_meta_info:
            self.assertEqual(index['status'],'Ready')

        for index_to_scan in self.index_ops_obj.get_create_index_list():
            self.log.info(f'Processing index: {index_to_scan["name"]}')
            query_def = index_to_scan["query_def"]
            query = query_def.generate_query(bucket=query_def.keyspace)
            try:
                result = self.run_cbq_query(query=query)['results'][0]
                self.assertTrue(result)
            except Exception as err:
                self.fail(f'{query} failed with {err}')

        add_nodes = [self.servers[2]]
        remove_nodes = [self.servers[1]]

        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=add_nodes,
                                                      to_remove=remove_nodes,
                                                      services=['index', 'index'])
        self.sleep(5)
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        self.sleep(5)
        after_rebalance_index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(before_rebalance_index_meta_info), len(after_rebalance_index_meta_info))
        for index in after_rebalance_index_meta_info:
            self.assertEqual(index['status'],'Ready')

        for index_to_scan in self.index_ops_obj.get_create_index_list():
            self.log.info(f'Processing index: {index_to_scan["name"]}')
            query_def = index_to_scan["query_def"]
            query = query_def.generate_query(bucket=query_def.keyspace)
            try:
                result = self.run_cbq_query(query=query)['results'][0]
                self.assertTrue(result)
            except Exception as err:
                self.fail(f'{query} failed with {err}')

    def test_schedule_index_drop_during_rebalance(self):
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        self.rest.set_index_settings(schedule_index_disable)
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, num_scopes=self.num_scopes,
                                             num_collections=self.num_collections)
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        drop_index_queries = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1', 'phone']

        for index_fields, idx_num in zip(index_field_list, range(10)):
            for collection_namespace in self.namespaces:
                index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=[index_fields])
                index_gen_list.append(index_gen)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build)
                drop_query = index_gen.generate_index_drop_query(namespace=collection_namespace)
                index_gen_query_list.append(query)
                drop_index_queries.append(drop_query)

        tasks = []
        rebalance_flag = False
        cqueries_before_rebalance = index_gen_query_list[0:15]
        cqueries_during_rebalance = index_gen_query_list[15:]
        dqueries_during_rebalance = drop_index_queries[0:15]
        dqueries_after_rebalance = drop_index_queries[15:]
        with ThreadPoolExecutor() as executor:
            for query in cqueries_before_rebalance:
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
                        add_nodes = [self.servers[2]]
                        remove_nodes = [self.servers[1]]
                        if not rebalance_flag:
                            self.sleep(30)
                            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                                          to_add=add_nodes,
                                                                          to_remove=remove_nodes,
                                                                          services=['index', 'index'])
                            self.sleep(5)
                            # creating indexes during rebalance operation
                            for query, drop_query in zip(cqueries_during_rebalance, dqueries_during_rebalance):
                                task = executor.submit(self.run_cbq_query, query=query)
                                tasks.append(task)
                                task = executor.submit(self.run_cbq_query, query=drop_query)
                                tasks.append(task)

                            result = rebalance_task.result()
                            rebalance_status = RestHelper(self.rest).rebalance_reached()
                            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
                            rebalance_flag = True
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.log.info(err)
            for query in dqueries_after_rebalance:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    pass
            self.sleep(10)

        result = self.wait_until_indexes_online(timeout=60)
        if not result:
            self.log.error("Timed out while checking for index status. Check index logs")
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info(f"Index Metadata: {index_metadata}")
        system_indexes = self.run_cbq_query(query=self.system_query)['results']
        self.assertFalse(system_indexes)


    def test_schedule_index_create_during_rebalance(self):
        self.index_rest.set_index_settings({"queryport.client.waitForScheduledIndex": False})
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        self.rest.set_index_settings(schedule_index_disable)
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, num_scopes=3, num_collections=1)
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        drop_index_queries = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1', 'phone']

        for index_fields, idx_num in zip(index_field_list, range(10)):
            for collection_namespace in self.namespaces:
                index_gen = QueryDefinition(index_name=f'{idx_prefix}_{idx_num}', index_fields=[index_fields])
                index_gen_list.append(index_gen)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build)
                drop_query = index_gen.generate_index_drop_query(namespace=collection_namespace)
                index_gen_query_list.append(query)
                drop_index_queries.append(drop_query)

        tasks = []
        rebalance_flag = False
        with ThreadPoolExecutor() as executor:
            queries_before_rebalance = index_gen_query_list[0:10]
            queries_during_rebalance = index_gen_query_list[10:20]
            queries_after_rebalance = index_gen_query_list[20:]
            for query in queries_before_rebalance:
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
                        add_nodes = [self.servers[2]]
                        remove_nodes = [self.servers[1]]
                        if not rebalance_flag:
                            self.sleep(10)
                            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                                to_remove=remove_nodes, services=['index', 'index'])
                            self.sleep(5)
                            # creating indexes during rebalance operation
                            for query in queries_during_rebalance:
                                task = executor.submit(self.run_cbq_query, query=query)
                                tasks.append(task)
                            result = rebalance_task.result()
                            rebalance_status = RestHelper(self.rest).rebalance_reached()
                            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
                            rebalance_flag = True
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.log.info(err)
            schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
            self.rest.set_index_settings(schedule_index_enable)

            for query in queries_after_rebalance:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    self.log.info(err)
            self.sleep(10)
        self.sleep(300)
        result = self.wait_until_indexes_online()
        if not result:
            self.log.error("Timed out while checking for index status. Check index logs")
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.log.info(f"Index metadata after rebalance {index_meta_info}")
        for index in index_meta_info:
            self.assertTrue(self.servers[1].ip not in index['hosts'][0])
            self.assertEqual(index['status'], 'Ready',
                             f"Index {index['name']} for scope:{index['scope']} and "
                             f"collection:{index['collection']} status is not matching with expected value.")

    def test_concurrent_indexes_with_failedover_nodes(self):
        """
        https://issues.couchbase.com/browse/MB-43442
        :return:
        """
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 1)
        doc = {"indexer.scheduleCreateRetries": num_retries_for_failed_index}
        self.rest.set_index_settings(doc)
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
        failover_flag = False
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
                        self.sleep(5)
                        node_out = self.servers[2]
                        if not failover_flag:
                            failover_task = self.cluster.async_failover(
                                self.servers[:self.nodes_init],
                                [node_out],
                                self.graceful, wait_for_pending=180)

                            failover_task.result()
                            failover_flag = True
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        for index in index_meta_info:
            self.log.info(index)

    def test_rebalance_redistribution_with_rebalance_in(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err)  or self.err_msg4 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.sleep(30, "Waiting before checking for index status")
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.log.info(f"Index Metadata: {index_meta_info}")
        self.assertEqual(len(index_meta_info), 10 * (self.num_replicas + 1))
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])

        self.log.info("Swaping in Indexer node  C")

        add_nodes = [self.servers[2]]
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                            to_remove=[], services=['index', 'index'])
        task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        self.sleep(30, "Waiting before checking for index status")
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])
        self.assertTrue(self.servers[2].ip in index_hosts)


    def test_rebalance_in_of_nodes_with_failed_rebalance(self):
        """
        https://issues.couchbase.com/browse/MB-43664
        :return:
        """
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            index_gen_query_list.append(query)
            index_lists.append(index_name)

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
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_field_list) * (self.num_replicas + 1), f"Mismatch. Metadata {index_meta_info}")

        self.log.info('Starting Rebalance In process')
        add_nodes = [self.servers[2]]
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                            to_remove=[], services=['index', 'index'])
        while self.rest._rebalance_progress_status() != "running":
            self.sleep(5, "Allowing some time for rebalance to make progress")
        self.stop_server(self.servers[2])
        self.sleep(5)
        self.start_server(self.servers[2])
        try:
            task.result()
        except Exception as err:
            self.log.info(err)
        self.wait_until_indexes_online()
        self.sleep(10)
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_lists) * 2, len(index_meta_info),
                         "Some Index/es  is/are missing due to rebalance failover")

        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")
        timeout_duration = 3600  # 1 hour in seconds
        start_time = time.time()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        while True:
            for node in index_nodes:
                rest = RestConnection(node)
                status = rest.get_index_rebalance_token_cleanup_status()
                if status == 'done':
                    self.log.info("Rebalance cleanup status is done")
                    break
                elapsed_time = time.time() - start_time
                if elapsed_time >= timeout_duration:
                    self.log.error(f"Timeout reached ({timeout_duration} seconds) while waiting for rebalance cleanup status to be done. Last status: {status}")
                    break
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[], to_remove=[])
        task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])
        self.assertTrue(self.servers[2].ip in index_hosts)

    def test_rebalance_out_of_nodes_with_failed_rebalance(self):
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            index_gen_query_list.append(query)
            index_lists.append(index_name)

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
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        all_items_indexed_flag = False
        for i in range(20):
            index_meta_info = self.rest.get_indexer_metadata()['status']
            if len(index_meta_info) == len(index_field_list) * (self.num_replicas + 1):
                all_items_indexed_flag = True
                break
            self.sleep(10)
        if not all_items_indexed_flag:
            self.log.error(f"Index metadata info from getIndexStatus endpoint {index_meta_info}")
            self.fail("Failed to index all the items in the bucket.")
        self.log.info('Starting Rebalance out process')
        remove_nodes = [self.servers[2]]
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                            to_remove=remove_nodes)
        while self.rest._rebalance_progress_status() != "running":
            self.sleep(5, "Allowing some time for rebalance to make progress")
        self.stop_server(self.servers[2])
        self.sleep(5)
        self.start_server(self.servers[2])
        try:
            task.result()
        except Exception as err:
            self.log.info(err)
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_lists) * 2, len(index_meta_info),
                         "Some Index/es  is/are missing due to rebalance failover")
        self.sleep(30)
        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")

        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[], to_remove=remove_nodes)
        task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_field_list) * 2, "No. indexes are not matching the expected value")
        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])
        self.assertTrue(self.servers[2].ip not in index_hosts)

    def test_rebalance_swap_of_nodes_with_failed_rebalance(self):
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            index_gen_query_list.append(query)
            index_lists.append(index_name)

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
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.log.error(err)
        self.wait_until_indexes_online()
        all_items_indexed_flag = False
        for i in range(20):
            index_meta_info = self.rest.get_indexer_metadata()['status']
            if len(index_meta_info) == len(index_field_list) * (self.num_replicas + 1):
                all_items_indexed_flag = True
                break
            self.sleep(10)
        if not all_items_indexed_flag:
            self.log.error(f"Index metadata info from getIndexStatus endpoint {index_meta_info}")
            self.fail("Failed to index all the items in the bucket.")
        self.log.info('Starting Rebalance Swap process')
        add_nodes = [self.servers[2]]
        remove_nodes = [self.servers[1]]
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                            to_remove=remove_nodes, services=['index', 'index'])

        while self.rest._rebalance_progress_status() != "running":
            self.sleep(5, "Allowing some time for rebalance to make progress")
        self.stop_server(self.servers[2])
        self.sleep(5)
        self.start_server(self.servers[2])
        try:
            task.result()
        except Exception as err:
            self.log.info(err)
        self.wait_until_indexes_online()
        self.sleep(30)
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_lists) * 2, len(index_meta_info),
                         "Some Index/es  is/are missing due to rebalance failover")
        self.sleep(30)
        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")

        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[], to_remove=remove_nodes)
        task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_field_list) * 2, "No. indexes are not matching the expected value")
        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])
        self.assertTrue(self.servers[1].ip not in index_hosts)


    def test_rebalance_in_with_incomplete_rebalance(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            index_gen_query_list.append(query)
            index_lists.append(index_name)

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
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_field_list) * (self.num_replicas + 1))
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])

        self.log.info("Swaping in Indexer node  C")

        add_nodes = [self.servers[2]]
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                            to_remove=[], services=['index', 'index'])

        self.sleep(15, "Allowing sometime for rebalance to make progress")
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        result = task.result()
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_lists) * 2, len(index_meta_info),
                         "Some Index/es  is/are missing due to in-process rebalance cancel")
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])
        self.assertTrue(self.servers[2].ip in index_hosts)
        for index_field in index_field_list:
            query = f"select count(*) from {collection_namespace} where {index_field} is not null"
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, num_of_docs, "No. indexed docs are not matching after rebalance")

    def test_rebalance_out_node_with_schedule_indexes(self):
        self.index_rest.set_index_settings({"queryport.client.waitForScheduledIndex": False})
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        self.rest.set_index_settings(schedule_index_disable)
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=self.num_replicas)
            index_gen_query_list.append(query)
            index_lists.append(index_name)

        tasks = []
        rebalance_flag = False
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
                        if not rebalance_flag:
                            self.sleep(15)
                            remove_nodes = [self.servers[2]]
                            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                                          to_add=[], to_remove=remove_nodes)
                            result = rebalance_task.result()
                            self.assertTrue(result)
                            rebalance_status = RestHelper(self.rest).rebalance_reached()
                            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
                            schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
                            self.rest.set_index_settings(schedule_index_enable)
                            rebalance_flag = True
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online(timeout=120)
        index_meta_info = self.rest.get_indexer_metadata()['status']
        indexes_after_rebalance_out = set()
        for index in index_meta_info:
            indexes_after_rebalance_out.add(index['indexName'])
        self.assertEqual(len(index_field_list), len(indexes_after_rebalance_out))

        add_nodes = [self.servers[3]]
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=add_nodes, to_remove=[], services=['index'])
        result = rebalance_task.result()
        self.assertTrue(result)
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        result = self.wait_until_indexes_online()
        if not result:
            self.log.error("Timed out while checking for index status. Check index logs")
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_field_list) * (self.num_replicas + 1))
        for index in index_meta_info:
            self.assertEqual(index['status'], 'Ready')


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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        self.sleep(20)
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
        scope_prefix = 'test_scope'
        collection_prefix = 'test_collection'

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
                    task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create, timeout_secs=300)
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
            for index_fields, idx_num in zip(index_field_list, range(self.num_of_indexes)):
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
                    elif self.err_msg2 in str(err) or self.err_msg4 in str(err) or self.err_msg3 in str(err):
                        continue
                    else:
                        self.log.info(err)
        self.sleep(10, "Giving some time before checking index status")
        self.wait_until_indexes_online(defer_build=self.defer_build)

        tasks = []
        self.log.info("Swapping out Indexer node B with C and D")
        add_nodes = self.servers[2:4]
        remove_node = [self.servers[1]]
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
            try:
                task.result()
            except Exception as err:
                self.log.error(err)
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        self.sleep(30, "Giving some time before checking index status")
        self.wait_until_indexes_online(defer_build=self.defer_build)
        if self.defer_build:
            for build_query in index_build_query_list:
                try:
                    self.run_cbq_query(query=build_query)
                except Exception as err:
                    self.log.info(err)
        self.sleep(120, "Giving some time before checking index status")
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.log.info(f"Index Metadata: {index_meta_info}")
        self.assertEqual(len(index_meta_info), self.num_of_indexes * (self.num_replicas + 1) * self.num_scopes * self.num_collections * 2)
        for index in index_meta_info:
            self.assertEqual(index['status'], 'Ready', index['status'])
            self.assertEqual(index['completion'], 100, index['completion'])
            self.assertFalse(index['stale'], index['stale'])
