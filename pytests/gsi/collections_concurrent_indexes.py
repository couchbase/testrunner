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
        self.indexer_crash_trigger = self.input.param('indexer_crash_trigger', None)
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
        index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                    'suffix', 'filler1', 'phone', 'zipcode'])
        self.index_field_list = [list(item) for item in index_field_set]
        self.err_msg1 = 'The index is scheduled for background creation'
        self.err_msg2 = 'Index creation will be retried in background'
        self.err_msg3 = 'Create index or Alter replica cannot proceed due to another concurrent create index request'
        self.err_msg4 = 'will retry building in the background for reason: Build Already In Progress'
        self.system_query = "select * from system:indexes"
        self.schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        self.schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
        self.log.info("==============  ConcurrentIndexes setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  ConcurrentIndexes tearDown has started ==============")
        super(ConcurrentIndexes, self).tearDown()
        self.log.info("==============  ConcurrentIndexes tearDown has completed ==============")

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
            count_err_1, count_err_2 = 0, 0
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:

                    if self.err_msg1 in str(err):
                        count_err_1 += 1
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        count_err_2 += 1
                    else:
                        self.fail(err)
            self.log.info(f"No. of concurrent indexes issued: {count_err_1}")

        result = self.wait_until_indexes_online(timeout=180 * self.num_of_indexes, defer_build=self.defer_build)
        if not result:
            self.log.warn("All indexes didn't build in given timeout. Increase timeout or check logs")
        self.sleep(30)
        index_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_info), len(index_gen_query_list), "No. of indexes is not matching to expected value")
        for index in index_info:
            if self.defer_build:
                self.assertEqual(index['status'], 'Created', index['status'])
                self.assertEqual(index['completion'], 0, index['completion'])
            else:
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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        count_err_2 += 1
                    else:
                        self.fail(err)
            self.log.info(f"No. of concurrent indexes issued: {count_err_1}")

        result = self.wait_until_indexes_online(timeout=60 * self.num_of_indexes * 4)
        if not result:
            self.log.error("All indexes didn't build in given timeout. Increase timeout or check logs")
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
        self.rest.set_index_settings(self.schedule_index_disable)
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

            # Verifying schedule indexes are reported in system:indexes
            self.sleep(120)
            result = self.run_cbq_query(query=self.system_query)['results']
            index_state = {item['indexes']['name']: item['indexes']['state'] for item in result}
            self.assertTrue('scheduled for creation' in index_state.values(), index_state)
            self.rest.set_index_settings(self.schedule_index_enable)

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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        count_err_2 += 1
                    else:
                        self.fail(err)
            self.log.info(f"No. of concurrent indexes issued: {count_err_1}")

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

    def test_drop_indexes_schedule_for_background_creation(self):
        self.index_rest.set_index_settings({"queryport.client.waitForScheduledIndex": False})
        num_of_docs = 10 ** 5
        self.rest.set_index_settings(self.schedule_index_disable)
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
            self.sleep(30, "Waiting before dropping indexes")
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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.rest.set_index_settings(self.schedule_index_enable)
        self.wait_until_indexes_online()
        index_metadata = self.rest.get_indexer_metadata()['status']
        self.log.info(index_metadata)
        self.assertEqual(len(index_metadata), len(index_gen_list) - len(scheduled_indexes),
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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
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
                            err_msg = f'No index available on keyspace'
                            self.assertTrue(err_msg in str(err), "Error msg not matching. Check logs")
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
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
            if self.use_https:
                port = '18091'
            else:
                port = '8091'
            deploy_node_info = [f"{node}:{port}" for node in index_nodes_to_be_used]
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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.log.info("Waiting for all indexes to be online...")
        self.wait_until_indexes_online()
        self.sleep(20)
        for i in range(20):
            index_info = self.rest.get_indexer_metadata()['status']
            if len(index_info) == len(index_gen_list) * (self.num_replicas + 1):
                break
            self.sleep(10)
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
                    elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.log.info("Waiting for all indexes to be online...")
        self.wait_until_indexes_online(defer_build=True)
        self.sleep(5)
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
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 1)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        index_rest = RestConnection(index_node)
        doc = {"indexer.scheduleCreateRetries": num_retries_for_failed_index}
        index_rest.set_index_settings(doc)
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 3 nodes")
        node_a, node_b = index_nodes[:2]

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
                self.sleep(10)
                self.log.info("Blocking Indexer nodes communication to Data node")
                self.block_incoming_network_from_node(self.master, node_b)
                self.block_incoming_network_from_node(self.master, node_a)
                self.sleep(5)
                self.log.info("Resuming communication of one Indexer node to Data node")
                self.resume_blocked_incoming_network_from_node(self.master, node_a)
                self.sleep(6 * num_retries_for_failed_index)
                # self.log.info("Resuming communication of another Indexer node to Data node after
                # retries are exhausted")
                self.sleep(60)
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
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
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 0)
        num_of_docs = 10 ** 4
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 2 nodes")
        node_a, node_b = index_nodes

        node_b_rest = RestConnection(node_b)
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        doc = {"indexer.scheduleCreateRetries": num_retries_for_failed_index}
        self.rest.set_index_settings(doc)
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
                self.log.info("Resuming Indexer nodes communication to Data node")
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                            self.log.info(err)
                        else:
                            self.fail(err)
            except Exception as err:
                self.fail(err)
            self.sleep(20)
            index_info = node_b_rest.get_indexer_metadata()['status']
            system_index_results = self.run_cbq_query(query=self.system_query)['results']
            self.log.info(f"System Index results: {system_index_results}")
            for index in index_info:
                if index['status'] == 'Error':
                    index_name = index['name']
                    drop_query = f'drop index {index_name} on {collection_namespace}'
                    self.run_cbq_query(query=drop_query)
                    index_fields_list.pop(index_name)
            for system_index in system_index_results:
                if system_index['indexes']['name'] not in index_fields_list:
                    self.assertEqual(system_index['state'], "offline",
                                     "Index state not matching to expected value in system indexes")
            self.sleep(5)
            index_info = node_b_rest.get_indexer_metadata()['status']
            self.assertEqual(len(index_info), len(index_fields_list),
                             "No. of indexes not matching expected count after dropping failed indexes")

    def test_index_creation_with_rebalanced_out_assigned_index_node(self):
        """
        https://issues.couchbase.com/browse/MB-41897
        """
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 1)
        doc = {"indexer.scheduleCreateRetries": num_retries_for_failed_index}
        self.rest.set_index_settings(doc)
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
            if self.use_https:
                port = '18091'
            else:
                port = '8091'
            deploy_node_info = [f"{node}:{port}" for node in index_nodes_to_be_used]
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
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_b])
                rebalance.result()
                rebalance_err_msg = ' Fail to create index due to rebalancing, another concurrent request,' \
                                    ' network partition, or node failed. The operation may have succeed.' \
                                    '  If not, please retry the operation at later time.'
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif rebalance_err_msg in str(err):
                            self.log.info("Index creation failed due to rebalanced out of node")
                        else:
                            self.log.error(err)
            except Exception as err:
                self.log.error(err)
            finally:
                self.wait_until_indexes_online(defer_build=self.defer_build)
        index_info = self.rest.get_indexer_metadata()['status']
        for index in index_info:
            self.assertTrue(f'{node_b.ip}:{node_b.port}' not in index['hosts'])

    def test_schedule_indexes_during_rebalance(self):
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
                for task in tasks:
                    task.results()
            except Exception as err:
                if self.err_msg1 in str(err):
                    out = re.search(regex_pattern, str(err))
                    index_name = out.groups()[0]
                    schedule_indexes.append(index_name)
                else:
                    self.log.info(err)

        self.wait_until_indexes_online()
        self.sleep(20)
        new_index_query_list = []
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}_1'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            new_index_query_list.append(query)
            index_fields_list[index_name] = index_fields

        self.log.info("Initiating Rebalance out")
        tasks = []
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_b])
        tasks.append(task)
        self.sleep(5)
        err_msg = ' Create index or Alter replica cannot proceed due to rebalance in progres'
        indexes_during_rebalance = 0
        with ThreadPoolExecutor() as executor:
            for query in new_index_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            for task in tasks:
                try:
                    task.result()
                except Exception as err:
                    if err_msg in str(err):
                        indexes_during_rebalance += 1
                    else:
                        self.log.info(err)
        self.wait_until_indexes_online()
        self.assertTrue(indexes_during_rebalance > 0, "None of the indexes scheduled during rebalance")
        index_info = self.rest.get_indexer_metadata()['status']
        for index in index_info:
            self.assertEqual(index['status'], 'Ready')
            self.assertTrue(f'{node_b.ip}:{node_b.port}' not in index['hosts'])

    def test_insufficient_nodes_during_schedule_indexes(self):
        '''
        MB-51158
        '''
        num_of_docs = 10 ** 5
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) != 2:
            self.fail("Need 2 or more index nodes")
        node_a, node_b = index_nodes

        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 1)
        doc = {"indexer.scheduleCreateRetries": num_retries_for_failed_index}
        self.rest.set_index_settings(doc)
        self.rest.set_index_settings(self.schedule_index_disable)
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
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
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
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_b])
                rebalance.result()
                rebalance_err_msg = ' Fail to create index due to rebalancing, another concurrent request,' \
                                    ' network partition, or node failed. The operation may have succeed.' \
                                    '  If not, please retry the operation at later time.'
                for task in tasks:
                    try:
                        task.result()
                    except Exception as err:
                        if self.err_msg1 in str(err):
                            out = re.search(regex_pattern, str(err))
                            index_name = out.groups()[0]
                            schedule_indexes.append(index_name)
                        elif rebalance_err_msg in str(err):
                            self.log.info("Index creation failed due to rebalanced out of node")
                        else:
                            self.log.error(err)
            except Exception as err:
                self.log.error(err)
        self.rest.set_index_settings(self.schedule_index_enable)
        self.sleep(10*(num_retries_for_failed_index+1))
        index_info = self.rest.get_indexer_metadata()['status']
        failed_schedule_indexes = [index['name'] for index in index_info if index['status'] == 'Error']
        self.assertTrue(len(failed_schedule_indexes) == 0)

    def test_index_creation_with_failover_of_assigned_index_node(self):
        num_of_docs = 10 ** 5
        num_retries_for_failed_index = self.input.param("num_retries_for_failed_index", 1)
        doc = {"indexer.scheduleCreateRetries": num_retries_for_failed_index}
        self.rest.set_index_settings(doc)
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
            if self.use_https:
                port = '18091'
            else:
                port = '8091'
            deploy_node_info = [f"{node}:{port}" for node in index_nodes_to_be_used]
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
                        elif self.err_msg2 in str(err) or self.err_msg3 in str(err) or self.err_msg4 in str(err):
                            continue
                        else:
                            self.log.info(err)
            except Exception as err:
                self.log.info(err)
            finally:
                self.resume_blocked_incoming_network_from_node(self.master, node_b)
                self.wait_until_indexes_online()
        index_info = self.rest.get_indexer_metadata()['status']
        failed_schedule_indexes = [index['name'] for index in index_info if index['status'] == 'Error']
        self.log.info(f"Indexes that failed to get scheduled: {failed_schedule_indexes}")
        self.assertTrue(len(failed_schedule_indexes) == 0)
        for index in index_info:
            if index['name'] not in failed_schedule_indexes:
                self.assertEqual(index['status'], 'Ready')

    # Negative test
    def test_build_schedule_indexes_during_schedule_state(self):
        num_of_docs = 10 ** 4
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
            for count, query in enumerate(index_gen_query_list):
                task1 = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task1)
            self.sleep(10)
            build_err_validation = 'This index is scheduled for background creation.' \
                                   ' The operation is not allowed on this index at this time.' \
                                   ' Please wait until the index is created.'
            build_progress_err = 'retry building in the background for reason:' \
                                 ' Build Already In Progress.'
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if self.err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        build_query = f'BUILD INDEX ON {collection_namespace}({index_name}) USING GSI'
                        try:
                            self.run_cbq_query(query=build_query)
                        except Exception as build_err:
                            if build_err_validation in str(build_err):
                                self.log.info(f"Exception thrown for building a schedule index {index_name}")
                                self.log.info(build_err)
                                break
                            elif build_progress_err in str(build_err):
                                continue
                            else:
                                self.fail(build_err)
                    else:
                        self.log.info(err)
            else:
                self.fail("Expected error for building indexes during schedule state didn't occur.")

    def test_recovery_of_schedule_index_from_crashes(self):
        num_of_docs = 10 ** 4
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Expecting at least 2 index nodes")
        node_a, node_b = None, None
        for node in index_nodes:
            if not node_a:
                node_a = node
            elif not node_b:
                node_b = node
                break
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        build_query_list = []
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            build_query_list.append(build_query)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields

        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            self.sleep(5)
        if self.indexer_crash_trigger == 'kill_memcached':
            self.kill_memcached()
        elif self.indexer_crash_trigger == 'node_block':
            self.block_incoming_network_from_node(node1=node_a, node2=node_b)
            self.sleep(60 * 3, f"Blocking communication between {node_a.ip} and {node_b.ip}")
            self.resume_blocked_incoming_network_from_node(node1=node_a, node2=node_b)
        elif self.indexer_crash_trigger == 'server_kill':
            self.stop_server(node_b)
            self.sleep(60 * 3, f"Sleeping for some time before bringing index node {node_b} up")
            self.start_server(node_b)
        elif self.indexer_crash_trigger == 'projector_kill':
            kv_shell = RemoteMachineShellConnection(self.master)
            out = kv_shell.get_running_processes()
            kv_shell.terminate_process(process_name='projector', force=True)
            self.sleep(30, "Waiting for projector to start again")
            kv_shell.disconnect()
        else:
            self.fail("Incorrect value for indexer_crash_trigger")

        for task in tasks:
            try:
                task.result()
            except Exception as err:
                if self.err_msg1 in str(err):
                    out = re.search(regex_pattern, str(err))
                    index_name = out.groups()[0]
                    self.log.info(f"Index {index_name} is schedule for background creation")
                else:
                    self.log.info(err)
        self.sleep(10)
        self.wait_until_indexes_online(timeout=2 * 60 * self.num_of_indexes, defer_build=self.defer_build)
        if self.defer_build:
            for build_query in build_query_list:
                self.run_cbq_query(query=build_query)
        self.wait_until_indexes_online(timeout=900)
        index_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_info), self.num_of_indexes)
        self.log.info(f"Index info: {index_info}")
        for index in index_info:
            self.assertEqual(index['status'], 'Ready')
            self.assertEqual(index['completion'], 100)

    # Negative Test
    def test_build_schedule_indexes_before_create(self):
        num_of_docs = 10 ** 4
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
        build_query = f'BUILD INDEX ON {collection_namespace}({", ".join(index_fields_list.keys())}) USING GSI'

        tasks = []
        pattern = re.compile("code': 5000, 'msg': 'GSI index idx_.*? not found.")
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            task = executor.submit(self.run_cbq_query, query=build_query)
            tasks.append(task)

        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                match = re.search(pattern, str(err))
                if match:
                    self.log.info(f"Exception thrown for building a schedule indexes")
                    self.log.info(err)
                    break
                elif self.err_msg1 in str(err):
                    self.log.info("Background index creation observed")
                else:
                    self.log.info(err)
        else:
            self.fail("Expected error for building indexes during schedule state didn't occur.")

    # Negative Test
    def test_negative_case_on_schedule_indexes_with_alter_index(self):
        num_of_docs = 10 ** 4
        self.alter_call = self.input.param("alter_call", None)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Expecting at least 2 index nodes")
        node_a, node_b = None, None
        for node in index_nodes:
            if not node_a:
                node_a = node
            elif not node_b:
                node_b = node
                break
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        if self.alter_call == 'replica_count_change':
            alter_index_query = f'ALTER INDEX schedule_index on {collection_namespace}' \
                                f' WITH {{"action": "replica_count", "num_replica": 1}}'
        elif self.alter_call == 'move_node':
            if self.use_https:
                port = '18091'
            else:
                port = '8091'
            alter_index_query = f'ALTER INDEX schedule_index on {collection_namespace}' \
                                f' WITH {{"action": "move", "nodes": ["{node_b.ip}:{port}"]}}'
        else:
            self.fail("Unexpected value for alter_call")
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            if self.use_https:
                port = '18091'
            else:
                port = '8091'
            deploy_node_info = [f"{node_a.ip}:{port}"]
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          deploy_node_info=deploy_node_info)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields

        tasks = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            self.sleep(2)

        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                if self.err_msg1 in str(err):
                    out = re.search(regex_pattern, str(err))
                    index_name = out.groups()[0]
                    alter_index_query = alter_index_query.replace('schedule_index', index_name)
                    try:
                        self.run_cbq_query(query=alter_index_query)
                    except Exception as alter_err:
                        self.log.info("Expected error occurred for alter index query for schedule index")
                        self.log.info(alter_err)
                        break
                else:
                    self.log.info(err)
        else:
            self.fail("Expected error for building indexes during schedule state didn't occur.")

    # Negative Test
    def test_errored_schedule_index_query_with_duplicate_name(self):
        """
        https://issues.couchbase.com/browse/MB-42011
        """
        num_of_docs = 10 ** 2
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        index_gen_list = []
        index_gen_query_list = []
        index_fields_list = {}

        # Checking for duplicate named index
        index_name = 'idx'
        for index_fields, idx_num in zip(self.index_field_list, range(self.num_of_indexes)):
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_fields)
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            index_gen_query_list.append(query)
            index_fields_list[index_name] = index_fields

        tasks = []
        err_msg = f'The index idx already exists.'
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                self.log.info(err)
                self.assertTrue(err_msg in str(err))

    # Negative test
    def test_errored_schedule_index_query_with_non_existent_node_info(self):
        """
        https://issues.couchbase.com/browse/MB-42012
        """
        num_of_docs = 10 ** 2
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        # issuing concurrent create index  with invalid node info
        index_gen_query_list = []
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        deploy_node1 = [f'{self.master.ip}:{port}']
        deploy_node2 = [f'10.112.205.102:{port}']
        index_gen1 = QueryDefinition(index_name='idx_1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx_2', index_fields=['city'])
        index_gen3 = QueryDefinition(index_name='idx_3', index_fields=['country'])
        index_gen4 = QueryDefinition(index_name='idx_4', index_fields=['firstName'])

        query = index_gen1.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       deploy_node_info=deploy_node1)
        index_gen_query_list.append(query)
        query = index_gen2.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       deploy_node_info=deploy_node2)
        index_gen_query_list.append(query)
        query = index_gen3.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       deploy_node_info=deploy_node2)
        index_gen_query_list.append(query)
        query = index_gen4.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       deploy_node_info=deploy_node2)
        index_gen_query_list.append(query)

        tasks = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

        err_msg = 'The node may be failed or under' \
                  ' rebalance or network partitioned from query process.'
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                self.log.info(err)
                self.assertTrue(err_msg in str(err))

    # Negative test
    def test_errored_schedule_index_with_incorrect_num_replica(self):
        num_of_docs = 10 ** 2
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        # issuing concurrent create index  with invalid replica num
        index_gen_query_list = []
        index_gen1 = QueryDefinition(index_name='idx_1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx_2', index_fields=['city'])
        index_gen3 = QueryDefinition(index_name='idx_3', index_fields=['country'])
        index_gen4 = QueryDefinition(index_name='idx_4', index_fields=['firstName'])

        query = index_gen1.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        index_gen_query_list.append(query)
        query = index_gen2.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       num_replica=1)
        index_gen_query_list.append(query)
        query = index_gen3.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       num_replica=1)
        index_gen_query_list.append(query)
        query = index_gen4.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                       num_replica=1)
        index_gen_query_list.append(query)

        tasks = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

        err_msg = 'Fails to create index.  There are not enough indexer nodes to create index with replica ' \
                  'count of 1. Some indexer nodes may be marked as excluded'
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                self.log.info(err)
                self.assertTrue(err_msg in str(err))

    # Negative test
    def test_errored_schedule_index_with_invalid_namespace(self):
        num_of_docs = 10 ** 2
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        invalid_bucket_namespace = collection_namespace.replace('bucket', 'invalid_bucket')
        invalid_scope_namespace = collection_namespace.replace('scope', 'invalid_scope')
        invalid_collection_namespace = collection_namespace.replace('collection', 'invalid_collection')
        # issuing concurrent create index  with invalid namespace
        index_gen_query_list = []
        index_gen1 = QueryDefinition(index_name='idx_1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx_2', index_fields=['city'])
        index_gen3 = QueryDefinition(index_name='idx_3', index_fields=['country'])
        index_gen4 = QueryDefinition(index_name='idx_4', index_fields=['firstName'])

        query = index_gen1.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        index_gen_query_list.append(query)
        query = index_gen2.generate_index_create_query(namespace=invalid_collection_namespace,
                                                       defer_build=self.defer_build)
        index_gen_query_list.append(query)
        query = index_gen3.generate_index_create_query(namespace=invalid_bucket_namespace, defer_build=self.defer_build)
        index_gen_query_list.append(query)
        query = index_gen4.generate_index_create_query(namespace=invalid_scope_namespace, defer_build=self.defer_build)
        index_gen_query_list.append(query)

        tasks = []
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

        err_msg1 = 'Scope not found in CB datastore'
        err_msg2 = 'Keyspace not found in CB datastore:'
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                self.log.info(err)
                self.assertTrue(err_msg1 in str(err) or err_msg2 in str(err))

    # Negative test
    def test_errored_schedule_index_with_delete_bsc(self):
        self.index_rest.set_index_settings({"queryport.client.waitForScheduledIndex": False})
        num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
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
        with ThreadPoolExecutor() as executor:
            for query in index_gen_query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as err:
                if self.err_msg1 in str(err):
                    self.log.info(f"Deleting {self.item_to_delete} for schedule indexes")
                    self.delete_bucket_scope_collection(server=self.servers[0], delete_item=self.item_to_delete,
                                                        bucket=bucket, scope=scope)
                    break
                else:
                    self.fail(err)
        self.sleep(120)
        result = self.run_cbq_query(query=self.system_query)['results']
        self.assertFalse(result)
