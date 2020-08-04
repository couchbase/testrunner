"""collections_index_delete_bsc.py: Test index commands viability, indexer behaviour and index states with delete of
Bucket, Scope and Collection

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "28/07/20 11:14 am" 

"""
from concurrent.futures import ThreadPoolExecutor
from .base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition


class CollectionsIndexDeleteBSC(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexDeleteBSC, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        self.item_to_delete = self.input.param('item_to_delete', None)
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
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(CollectionsIndexDeleteBSC, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def drop_indexes(self, index_list, collection_namespace):
        drop_index_tasks = []
        with ThreadPoolExecutor() as drop_task:
            for index in index_list:
                drop_query = f'Drop index {index} on {collection_namespace}'
                drop_index_tasks.append(drop_task.submit(self.run_cbq_query, query=drop_query))
        return drop_index_tasks

    def delete_bucket_scope_collection(self, delete_item, server, bucket, scope='test_scope_1',
                                       collection='test_collection_1'):
        if not server:
            server = self.servers[0]
        if not bucket:
            bucket = self.test_bucket
        if not delete_item:
            delete_item = self.item_to_delete
        if delete_item == 'bucket':
            self.log.info(f"Deleting bucket: {bucket}")
            return self.cluster.bucket_delete(server=server, bucket=bucket)
        elif delete_item == 'scope':
            self.log.info(f"Deleting Scope: {scope}")
            return self.collection_rest.delete_scope(bucket=bucket, scope=scope)
        elif delete_item == 'collection':
            self.log.info(f"Deleting Collection: {collection}")
            return self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=collection)

    def test_index_creation_with_keyspace_delete(self):
        """
        summary: This test validate process of index creation with delete/drop of Bucket/Scope/Collection
        """
        self.prepare_collection_for_indexing(num_of_docs_per_collection=10 ** 6)
        collection_namespace = self.namespace[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        try:
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.run_cbq_query, query=query, timeout=30)
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
            self.sleep(5)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)

    def test_index_during_scan_with_delete_bsc(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=10 ** 6)
        collection_namespace = self.namespace[0]
        index_gen = QueryDefinition(index_name='idx1', index_fields=['age'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        # index_gen = QueryDefinition(index_name='idx2', index_fields=['city'])
        # query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        # self.run_cbq_query(query=query)
        # index_gen = QueryDefinition(index_name='idx3', index_fields=['firstName'])
        # query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        # self.run_cbq_query(query=query)
        # index_gen = QueryDefinition(index_name='idx4', index_fields=['lastName'])
        # query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        # self.run_cbq_query(query=query)
        # index_gen = QueryDefinition(index_name='idx5', index_fields=['country'])
        # query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        # self.run_cbq_query(query=query)
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
                task1 = executor.submit(self.run_cbq_query, query=query1, timeout=30)
                task2 = executor.submit(self.run_cbq_query, query=query2, timeout=30)
                task3 = executor.submit(self.run_cbq_query, query=query3, timeout=30)
                task4 = executor.submit(self.run_cbq_query, query=query4, timeout=30)
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
        num_of_docs_per_collection = 10 ** 6
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs_per_collection)
        collection_namespace = self.namespace[0]
        index_gen = QueryDefinition(index_name='`#primary`')
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
            self.assertEqual(result, num_of_docs_per_collection,
                             f"Result not matching. Expected: {num_of_docs_per_collection}, Actual: {result}")

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
                    # self.assertFalse()
                    self.log.info(result)
        except Exception as err:
            self.log.info(str(err))
            self.sleep(5)
            index_status = self.rest.get_index_status()
            self.assertFalse(index_status)
