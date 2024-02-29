"""collections_indexes_with_flush.py: These tests validate indexer behavior for collection during bucket flush

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "08/09/20 12:31 pm" 

"""
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.query_definitions import QueryDefinition

from .base_gsi import BaseSecondaryIndexingTests


class CollectionsIndexesWithFlush(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexesWithFlush, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
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
        super(CollectionsIndexesWithFlush, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def test_index_status_with_bucket_flush(self):
        num_of_docs_per_collection = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        select_query = f'select count(age) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=select_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs_per_collection, "Doc count not matching")

        # Checking indexer status after bucket flush
        try:
            num_rollback = self.rest.get_num_rollback_stat(bucket=self.test_bucket)
            self.log.info(f"No. of rollbacks: {num_rollback}")
            self.cluster.bucket_flush(server=self.master, bucket=self.test_bucket, timeout=600)
            self.sleep(120, "Giving some time to indexer to update indexes after flush")
            index_info = self.rest.get_indexer_metadata()['status']
            self.log.info(index_info)
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertEqual(result, 0, "Doc count not matching")
            rollback_after_flush = self.rest.get_num_rollback_stat(bucket=self.test_bucket)
            self.sleep(10)
            self.log.info(rollback_after_flush)
            self.assertEqual(rollback_after_flush, num_rollback + 2, "Flush didn't send rollback to Zero to Indexer")
            num_rollback = rollback_after_flush

            # Checking indexer status after with incremental build and then flush
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.data_ops_javasdk_loader_in_batches, sdk_data_loader=self.gen_create,
                                        batch_size=2 * 10 ** 4)
                self.sleep(15, "There is 10sec delay in doc loader")
                task2 = executor.submit(self.run_cbq_query, query=select_query)
                task3 = self.cluster.async_bucket_flush(server=self.master, bucket=self.test_bucket)
                result = task2.result()['results'][0]['$1']
                self.assertTrue(result > 0, "Indexer not indexed newly inserted docs")
                self.log.info(task3.result())
                tasks = task1.result()
                for task in tasks:
                    out = task.result()
                    self.log.info(out)

            self.sleep(15, "Giving some time to indexer to update indexes after flush")
            idx_stats = self.rest.get_all_index_stats()
            num_snapshot = idx_stats[f'{bucket}:{scope}:{collection}:idx:num_commits']
            rollback_after_flush = self.rest.get_num_rollback_stat(bucket=self.test_bucket)
            self.log.info(rollback_after_flush)
            # Rolling back to disk snapshot instead of zero
            if num_snapshot > 0:
                self.assertEqual(rollback_after_flush, num_rollback + 1,
                                 "Flush didn't send rollback to Zero to Indexer")
            else:
                self.assertEqual(rollback_after_flush, num_rollback + 2,
                                 "Flush didn't send rollback to Zero to Indexer")
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertEqual(result, 0, "Doc count not matching")

            # Flushing already Flushed bucket
            result = self.cluster.bucket_flush(server=self.master, bucket=self.test_bucket)
            self.log.info(result)
            self.sleep(15, "Giving some time to indexer to update indexes after flush")
            rollback_after_flush = self.rest.get_num_rollback_stat(bucket=self.test_bucket)
            self.log.info(rollback_after_flush)
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertEqual(result, 0, "Doc count not matching")

            drop_index_query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=drop_index_query)
        except Exception as err:
            self.fail(err)

    def test_index_status_with_multiple_collection_with_bucket_flush(self):
        num_of_docs_per_collection = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs_per_collection, num_collections=2)

        index_gen_list = []
        for collection_namespace in self.namespaces:
            index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False)
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()

            select_query = f'select count(age) from {collection_namespace} where age >= 0'
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertEqual(result, num_of_docs_per_collection, "Doc count not matching")

        # Checking indexer status after bucket flush
        try:
            num_rollback = self.rest.get_num_rollback_stat(bucket=self.test_bucket)
            self.log.info(f"num_rollback before flush:{num_rollback}")
            task = self.cluster.async_bucket_flush(server=self.master, bucket=self.test_bucket)
            result = task.result(timeout=200)
            self.log.info(result)
            self.sleep(15, "Giving some time to indexer to update indexes after flush")
            rollback = self.rest.get_num_rollback_stat(bucket=self.test_bucket)
            self.log.info(f"num_rollback after flush:{rollback}")
            # self.assertEqual(rollback, num_rollback+1)
            for collection_namespace in self.namespaces:
                select_query = f'select count(age) from {collection_namespace} where age >= 0'
                result = self.run_cbq_query(query=select_query)['results'][0]['$1']
                self.assertEqual(result, 0, "Doc count not matching")
        except Exception as err:
            self.fail(err)

    def test_index_status_with_flush_during_index_building(self):
        num_of_docs_per_collection = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)
        task = self.cluster.async_bucket_flush(server=self.master, bucket=self.test_bucket)
        result = task.result()
        self.log.info(result)
        self.sleep(15, "Giving some time to indexer to update indexes after flush")
        select_query = f'select count(age) from {collection_namespace} where age >= 0'
        try:
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertEqual(result, num_of_docs_per_collection, "Doc count not matching")
        except Exception as err:
            self.log.info(err)

        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create, batch_size=2 * 10 ** 4)
        for task in tasks:
            task.result()
        # building the index and flushing bucket in parallel
        build_query = index_gen.generate_build_query(namespace=collection_namespace)
        with ThreadPoolExecutor() as executor:
            task1 = executor.submit(self.run_cbq_query, query=build_query)
            task2 = executor.submit(self.cluster.async_bucket_flush, server=self.master, bucket=self.test_bucket)
            out = task2.result()
            self.log.info(out)
            out = task1.result()
            self.log.info(out)

        self.sleep(60, "Giving some time to indexer to update indexes after flush")
        self.wait_until_indexes_online()
        result = self.run_cbq_query(query=select_query)['results'][0]['$1']
        self.assertEqual(result, 0, "Doc count not matching")

    def test_index_status_with_node_disconnect_during_flush(self):
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 2)
        num_of_docs_per_collection = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        select_query = f'select count(age) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=select_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs_per_collection, "Doc count not matching")

        try:
            with ThreadPoolExecutor() as executor:
                task1 = executor.submit(self.cluster.async_bucket_flush, server=self.master, bucket=self.test_bucket)
                self.sleep(1)
                task2 = executor.submit(self.stop_server(data_nodes[1]))
                out2 = task2.result()
                self.log.info(out2)
                out1 = task1.result()
                self.log.info(out1)

            self.sleep(5, "Wait for few secs before bringing node back on")
            self.start_server(data_nodes[1])
            result = self.run_cbq_query(query=select_query)['results'][0]['$1']
            self.assertEqual(result, 0, "Doc count not matching")
        except Exception as err:
            self.log.info(err)
            self.start_server(data_nodes[1])
        self.sleep(10)
        result = self.run_cbq_query(query=select_query)['results'][0]['$1']
        self.log.info(f"Doc count in collection with flush failed due to node disconnect: {result}")
        self.assertTrue(result > 0, "Doc count not matching")
