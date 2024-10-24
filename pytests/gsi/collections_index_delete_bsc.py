"""collections_index_delete_bsc.py: Test index commands viability, indexer behaviour and index states with delete of
Bucket, Scope and Collection.
Test details available at - https://docs.google.com/spreadsheets/d/1Y7RKksVazSDG_qY9SqpnydXr5A2FSSHCMTEx-V0Ar08/edit?pli=1#gid=1763392107

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "28/07/20 11:14 am" 

"""
from concurrent.futures import ThreadPoolExecutor
import random

from .base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.capella_rest_client import RestConnection as RestConnectionCapella


class CollectionsIndexDeleteBSC(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexDeleteBSC, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        self.indexMemQuota = self.input.param("indexMemQuota", 256)
        if self.indexMemQuota > 256:
            self.log.info(f"Setting indexer memory quota to {self.indexMemQuota} MB...")
            self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=self.indexMemQuota)
            self.sleep(30)
        self.index_nodes = self.get_nodes_from_services_map(service_type="index")
        if self.enable_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.index_nodes])
            indexer_mem_quota = self.get_indexer_mem_quota()
            self.log.info("Current Indexer Memory Quota is {0}".format(indexer_mem_quota))
        if self.capella_run:
            buckets = self.rest.get_buckets()
            if buckets:
                for bucket in buckets:
                    RestConnectionCapella.delete_bucket(self, bucket=bucket.name)

        else:
            self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.password = self.input.membase_settings.rest_password
        self.buckets = self.rest.get_buckets()
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        pass

    def suite_tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(CollectionsIndexDeleteBSC, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def drop_indexes(self, index_list, collection_namespace):
        drop_index_tasks = []
        with ThreadPoolExecutor() as drop_task:
            for index in index_list:
                drop_query = f'Drop index {index} on {collection_namespace}'
                drop_index_tasks.append(drop_task.submit(self.run_cbq_query, query=drop_query))
        return drop_index_tasks

    def test_index_creation_with_keyspace_delete(self):
        """
        summary: This test validate process of index creation with delete/drop of Bucket/Scope/Collection
        """
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        try:
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.run_cbq_query, query=query, rest_timeout=30)
                self.sleep(5)
                task2 = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                        delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                        collection=collection)
                result = task2.result()
                self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
                result = task1.result()
                self.assertTrue(result, f"Index was successfully created with deletion of {self.item_to_delete}")
        except Exception as err:
            self.log.info(str(err))
            self.sleep(30)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)

    def test_index_during_scan_with_delete_bsc(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx1', index_fields=['age'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')

        if self.defer_build:
            query = f"Build index on {collection_namespace}(idx1)"
            self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        try:
            query1 = f'select age, city from {collection_namespace} where age > 10 and age < 60 and city like "C%"'
            query2 = f' select age, fistName from {collection_namespace} where age > 10 and age < 60 and' \
                     f' firstName like "P%"'
            query3 = f'select age, lastName from {collection_namespace} where age > 10 and age < 60 ' \
                     f'and lastName like "A%" '
            query4 = f'select age, country from {collection_namespace} where age > 10 and age < 60 ' \
                     f'and country like "S%"'
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.run_cbq_query, query=query1, rest_timeout=30)
                task2 = executor.submit(self.run_cbq_query, query=query2, rest_timeout=30)
                task3 = executor.submit(self.run_cbq_query, query=query3, rest_timeout=30)
                task4 = executor.submit(self.run_cbq_query, query=query4, rest_timeout=30)
                self.sleep(1)
                task5 = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                        delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                        collection=collection)
                result = task5.result()
                self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
                result = task1.result()['results']
                self.assertEqual(len(result), 0, f"Index scanned docs for deleted: {self.item_to_delete}")
                result = task2.result()['results']
                self.assertEqual(len(result), 0, f"Index scanned docs for deleted: {self.item_to_delete}")
                result = task3.result()['results']
                self.assertEqual(len(result), 0, f"Index scanned docs for deleted: {self.item_to_delete}")
                result = task4.result()['results']
                self.assertEqual(len(result), 0, f"Index scanned docs for deleted: {self.item_to_delete}")
        except Exception as err:
            self.log.info(str(err))
            self.sleep(5)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)

    def test_drop_index_with_delete_bsc(self):

        #num_of_docs_per_collection = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='#primary')
        query = index_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')

        if self.defer_build:
            query = f"Build index on {collection_namespace}(`#primary`)"
            self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        query = f'select * from {collection_namespace} limit 1'
        result = self.run_cbq_query(query=query)['results']
        index_fields = []
        for key in result[0][collection]:
            index_fields.append(key)
        try:
            index_list = []
            # Creating indexes on all fields in docs
            for index_suffix, index_field in enumerate(index_fields):
                index_name = f'idx_{index_suffix}'
                index_list.append(index_name)
                query = f'Create index {index_name} on {collection_namespace}({index_field}) USING GSI ' \
                        f'WITH {{\'defer_build\': {self.defer_build}}}'
                self.run_cbq_query(query)
            if self.defer_build:
                query = f'Build index on {collection_namespace}({",".join(index_list)})'
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online(timeout=600)

            # running a  select query
            query = f'select count(*) from {collection_namespace} where age >= 0'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection,
                             f"Result not matching. Expected: {self.num_of_docs_per_collection}, Actual: {result}")

            # deleting BSC while dropping indexes
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.drop_indexes, index_list=index_list,
                                        collection_namespace=collection_namespace)

                task2 = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                        delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                        collection=collection)
                result = task2.result()
                drop_index_tasks = task1.result()
                self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
                for drop_index_task in drop_index_tasks:
                    result = drop_index_task.result()
                    self.log.info(result)
        except Exception as err:
            self.log.info(str(err))
            self.sleep(5)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)

    def test_delete_bsc_while_index_updates_mutation(self):
        #num_of_docs = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        select_query = f'select count(age) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=select_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection)
        gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection * 10, percent_create=80,
                                   percent_update=10, percent_delete=10, scope=scope,
                                   collection=collection, start_seq_num=self.num_of_docs_per_collection + 1, username=self.username, password=self.password)
        try:
            # deleting BSC while indexes catching up with new mutations
            with ThreadPoolExecutor() as executor:
                executor.submit(self._load_all_buckets, self.master, gen_create)
                self.sleep(30)
                select_task = executor.submit(self.run_cbq_query, query=select_query)
                delete_bsc = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                             delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                             collection=collection)
                delete_result = delete_bsc.result()
                self.assertTrue(delete_result, f"Failed to delete: {self.item_to_delete}")
                count = select_task.result()['results'][0]['$1']
                self.assertTrue(count > self.num_of_docs_per_collection, "Delete bucket happened before mutation operation began")
            self.sleep(30)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)
        except Exception as err:
            if self.item_to_delete == 'scope' or self.item_to_delete == 'collection':
                self.log.info(str(err))
                # err_msg = "Unknown scope or collection in operation"
                # self.assertTrue(err_msg in str(err), "Error msg not matching")
            else:
                self.fail(str(err))
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)

    def test_delete_multiple_bsc(self):
        bucket_prefix = self.test_bucket
        buckets_list = []
        collection_namespaces_list = []
        self.rest.delete_bucket(bucket=self.test_bucket)
        self.sleep(10)
        for bucket_num in range(5):
            self.test_bucket = f'{bucket_prefix}_{bucket_num}'
            buckets_list.append(self.test_bucket)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222, bucket_params=self.bucket_params)
            # self.prepare_collection_for_indexing(num_of_docs_per_collection=10 ** 4)
            self.collection_rest.create_scope_collection_count(scope_num=1, collection_num=1,
                                                               scope_prefix=self.scope_prefix,
                                                               collection_prefix=self.collection_prefix,
                                                               bucket=self.test_bucket)
            scope, collection = f'{self.scope_prefix}_1', f'{self.collection_prefix}_1'
            gen_create = SDKDataLoader(num_ops=10 ** 3, percent_create=100,
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, json_template='Person', username=self.username, password=self.password)
            task = self.cluster.async_load_gen_docs(server=self.master, generator=gen_create, bucket=self.test_bucket,
                                                    scope=scope, collection=collection)
            task.result()
            collection_namespace = f'default:{self.test_bucket}.{scope}.{collection}'
            collection_namespaces_list.append(collection_namespace)
        index_list = []
        for count, collection_namespace in enumerate(collection_namespaces_list):
            index = f'idx_{count}'
            index_list.append(index)
            index_gen = QueryDefinition(index_name=index, index_fields=['age', 'city', 'country'])
            query = index_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                  defer_build=self.defer_build)
            self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # deleting multiple indexes across multiple bucket
        collection_namespace_to_be_deleted = random.sample(collection_namespaces_list, 3)
        with ThreadPoolExecutor() as executor:
            task_list = []
            for collection_namespace in collection_namespace_to_be_deleted:
                _, keyspace = collection_namespace.split(':')
                bucket, scope, collection = keyspace.split('.')
                index_list.remove(f'idx_{bucket.split("_")[-1]}')
                task = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                       delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                       collection=collection)
                task_list.append(task)
            for task in task_list:
                result = task.result()
                self.assertTrue(result)
        index_status = self.rest.get_index_status()
        for index in index_list:
            idx = index_status[f'test_bucket_{index.split("_")[-1]}']
            self.assertTrue(index in idx,
                            'Index of available bucket is missing')

    def test_delete_bsc_with_flush_running(self):
        #num_of_docs_per_collection = 10 ** 6
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection, batch_size=5*10**4)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_primary_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        try:
            # running a  select query
            query = f'select count(*) from {collection_namespace} where age >= 0'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection,
                             f"Result not matching. Expected: {self.num_of_docs_per_collection}, Actual: {result}")

            # deleting BSC while Flushing
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.cluster.async_bucket_flush, self.master, self.test_bucket)
                task2 = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                        delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                        collection=collection)
                result = task2.result()
                self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
                flush_result = task1.result()
                self.log.info(flush_result)
                self.sleep(30)
                index_status = self.rest.get_index_status()
                self.assertFalse(index_status)
        except Exception as err:
            self.fail(str(err))

    def test_delete_bsc_with_only_deferred_Index(self):
        #num_of_docs_per_collection = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        # deleting BSC with only deferred index
        try:
            result = self.delete_bucket_scope_collection(server=self.servers[0],
                                                         delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                                         collection=collection)

            self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
            self.sleep(30)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)
        except Exception as err:
            self.fail(str(err))

    def test_delete_recreate_collection_indexes(self):
        #num_of_docs_per_collection = 10 ** 4
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        primary_gen = QueryDefinition(index_name='#primary')
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
        index_gen3 = QueryDefinition(index_name='idx3', index_fields=['country'])

        query1 = index_gen1.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query1)
        query2 = index_gen2.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query2)
        query3 = index_gen3.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query3)
        query4 = primary_gen.generate_primary_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query4)
        self.wait_until_indexes_online()

        # deleting bsc and re-creating it
        result = self.delete_bucket_scope_collection(server=self.servers[0],
                                                     delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                                     collection=collection)
        self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
        if self.item_to_delete == 'bucket':
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        elif self.item_to_delete == 'scope':
            self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        elif self.item_to_delete == 'collection':
            self.collection_rest.create_collection(bucket=bucket, scope=scope, collection=collection)
            self.sleep(10)
            self._load_all_buckets(self.master, self.gen_create)
        self.run_cbq_query(query=query1)
        self.run_cbq_query(query=query2)
        self.run_cbq_query(query=query3)
        self.run_cbq_query(query=query4)
        self.wait_until_indexes_online()

        select_query = f'select count(*) from {collection_namespace}'
        result = self.run_cbq_query(query=select_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection)
        age_query = f'select count(age) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=age_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection)
        self.sleep(10)
        index_status = self.rest.get_index_status()

        if self.capella_run:
            self.assertEqual(sorted(list(index_status['test_bucket'].keys())),
                             sorted(['#primary', 'idx1', 'idx2', 'idx3', '#primary (replica 1)', 'idx1 (replica 1)', 'idx2 (replica 1)', 'idx3 (replica 1)']),
                             "Some Indexes are missing from index status")
        else:
            self.assertEqual(sorted(list(index_status['test_bucket'].keys())),
                             sorted(['#primary', 'idx1', 'idx2', 'idx3']),
                             "Some Indexes are missing from index status")





    def test_delete_multiple_collections_with_indexes(self):
        #num_of_docs_per_collection = 10 ** 2
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)
        for namespace in self.namespaces:
            index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
            index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
            query1 = index_gen1.generate_index_create_query(namespace=namespace, defer_build=False)
            self.run_cbq_query(query=query1)
            query2 = index_gen2.generate_index_create_query(namespace=namespace, defer_build=False)
            self.run_cbq_query(query=query2)
        self.wait_until_indexes_online()

        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(5)]
        for scope in scopes[:4]:
            result = self.delete_bucket_scope_collection(server=self.servers[0], delete_item=self.item_to_delete,
                                                         bucket=self.test_bucket, scope=scope)
            self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
        select_query1 = f'select count(age) from default:{self.test_bucket}.{scopes[-1]}.test_collection_1 ' \
                        f'where age >= 0'
        select_query2 = f'select count(city) from default:{self.test_bucket}.{scopes[-1]}.test_collection_2 ' \
                        f'where city like "A%" '
        self.sleep(10)
        result1 = self.run_cbq_query(query=select_query1)['results'][0]['$1']
        result2 = self.run_cbq_query(query=select_query2)['results'][0]['$1']
        self.assertTrue(result2 > 0)
        self.assertEqual(result1, self.num_of_docs_per_collection)
        index_status = self.rest.get_index_status()
        self.assertEqual(len(index_status['test_bucket']), 2 * (self.num_index_replica + 1))

    def test_delete_deleted_bsc(self):
        #num_of_docs_per_collection = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        query = f'select count(age) from {collection_namespace} where age > 0 and city like "A%"'
        result = self.run_cbq_query(query=query)['results'][0]['$1']
        self.assertTrue(result > 0)
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        try:
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                        delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                        collection=collection)
                # retrying delete of deleted BSC
                self.sleep(1)
                task2 = executor.submit(self.delete_bucket_scope_collection, server=self.servers[0],
                                        delete_item=self.item_to_delete, bucket=bucket, scope=scope,
                                        collection=collection, timeout=5)
                result = task1.result()
                self.assertTrue(result, f"Failed to Delete {self.item_to_delete}")
                result = task2.result()
                self.assertFalse(result, f"Got second success for delete operation")
                self.sleep(10)
                index_status = self.rest.get_index_status()
                self.assertFalse(index_status)
        except Exception as err:
            self.fail(str(err))
