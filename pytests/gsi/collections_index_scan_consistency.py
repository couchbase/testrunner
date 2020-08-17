"""collections_index_scan_consistency.py: Tests to check index scan consistency

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "07/08/20 10:51 am" 

"""

from concurrent.futures import ThreadPoolExecutor

import testconstants
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests


class CollectionsIndexScanConsistency(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexScanConsistency, self).setUp()
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
        super(CollectionsIndexScanConsistency, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def test_request_plus_index_consistency(self):
        """
        Summary: This test validate request_plus scan consistency with flooding high data load to cluster and
         at the same instance issuing select query to fetch new docs
        """
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        doc_body = {
            'age': 34,
            'country': 'test_country',
            'city': 'test_city',
            'filler1': ['ut','distinctio','sit','inventore','quo','quos','saepe','doloremque','sed','omnis'],
            'firstName': 'Mitch',
            'lastName': 'Funk',
            'streetAddress': '66877 Williamson Terrace',
            'suffix': 'V',
            'title': 'International Solutions Coordinator'
        }
        insert_query = f'INSERT INTO {collection_namespace} (KEY, VALUE) VALUES ("scan_doc_1", {doc_body})'
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        gen_create = SDKDataLoader(num_ops=2 * num_of_docs, percent_create=100,
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, start_seq_num=num_of_docs + 1)
        select_query = f'Select country, city from {collection_namespace} where meta().id = "scan_doc_1"'
        count_query = f'Select count(meta().id) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs)
        try:
            with ThreadPoolExecutor() as executor:
                executor.submit(self._load_all_buckets, self.master, gen_create)
                executor.submit(self.run_cbq_query, query=insert_query)
                self.sleep(5)
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='request_plus')
                count_task = executor.submit(self.run_cbq_query, query=count_query, scan_consistency='request_plus')

                result1 = select_task.result()['results'][0]
                result2 = count_task.result()['results'][0]['$1']

            self.assertEqual(result1, {'city': 'test_city', 'country': 'test_country'},
                             "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertTrue(result2 > num_of_docs + 1, "request plus scan is not able to wait for new inserted docs")
        except Exception as err:
            self.fail(str(err))

    def test_at_plus_index_consistency(self):
        num_of_docs = 1 ** 5
        if self.testrunner_client != testconstants.PYTHON_SDK:
            self.skipTest("Can't run test with memcached client. Use python3 sdk or Java sdk")
        from couchbase.cluster import Cluster, ClusterOptions
        from couchbase_core.cluster import PasswordAuthenticator
        cluster = Cluster(f'couchbase://{self.master.ip}', ClusterOptions(PasswordAuthenticator('username', 'password')))

        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]

        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        cb_bucket_obj = cluster.bucket(bucket)
        cb_col = cb_bucket_obj.collection(collection)
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        doc_body = {
            'age': 34,
            'country': 'test_country',
            'city': 'test_city',
            'filler1': ['ut','distinctio','sit','inventore','quo','quos','saepe','doloremque','sed','omnis'],
            'firstName': 'Mitch',
            'lastName': 'Funk',
            'streetAddress': '66877 Williamson Terrace',
            'suffix': 'V',
            'title': 'International Solutions Coordinator'
        }
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        gen_create = SDKDataLoader(num_ops=2 * num_of_docs, percent_create=100,
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, start_seq_num=num_of_docs + 1)
        select_query = f'Select country, city from {collection_namespace} where meta().id = "scan_doc_1"'
        count_query = f'Select count(meta().id) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs)
        try:
            with ThreadPoolExecutor() as executor:
                executor.submit(self._load_all_buckets, self.master, gen_create)
                insert_task = executor.submit(cb_col.insert, 'scan_doc_1', doc_body)
                scan_vector = insert_task.result()
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                count_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                             scan_vector=scan_vector)
                result1 = select_task.result()['results'][0]
                result2 = count_task.result()['results'][0]['$1']
            self.assertEqual(result1, {'city': 'test_city', 'country': 'test_country'},
                             "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertTrue(result2 > num_of_docs + 1, "request plus scan is not able to wait for new inserted docs")
        except Exception as err:
            self.fail(str(err))
