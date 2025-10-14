"""collections_index_basics.py: This test file contains tests for basic index operation in Collections context

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "26/05/20 2:13 pm"

"""
import random

from concurrent.futures import ThreadPoolExecutor

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection
from membase.api.capella_rest_client import RestConnection as RestConnectionCapella
class CollectionsIndexBasics(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexBasics, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        if self.capella_run:
            buckets = self.rest.get_buckets()
            if buckets:
                for bucket in buckets:
                    RestConnectionCapella.delete_bucket(self, bucket=bucket.name)

        else:
            self.rest.delete_all_buckets()
        self.password = self.input.membase_settings.rest_password
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        if not self.capella_run and 'community' not in self.cb_version:
            self._create_server_groups()
            self.cb_version = float(self.cb_version.split('-')[0][0:3])
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        pass

    def suite_tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(CollectionsIndexBasics, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def test_create_primary_index_for_collections(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        query_gen_1 = QueryDefinition(index_name='#primary')
        query_gen_2 = QueryDefinition(index_name='name_primary_idx')
        # preparing index
        try:
            query = query_gen_1.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen_1.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            result = self.wait_until_indexes_online()
            self.log.info(result)
            self.sleep(10, "Waiting before running SELECT Query")
            query = f'SELECT COUNT(*) from {collection_namespace}'
            count = self.run_query_with_retry(query=query, expected_result=self.num_of_docs_per_collection,
                                              is_count_query=True)
            self.assertEqual(count, self.num_of_docs_per_collection, "Docs count not matching")

            # Checking for named primary index
            query = query_gen_2.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen_2.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(10, "Waiting before running SELECT Query")
            query = f'SELECT COUNT(*) from {collection_namespace}'
            count = self.run_query_with_retry(query=query, expected_result=self.num_of_docs_per_collection,
                                              is_count_query=True)
            self.assertEqual(count, self.num_of_docs_per_collection, "Docs count not matching")
        except Exception as err:
            self.fail(str(err))
        finally:
            query_1 = query_gen_1.generate_index_drop_query(namespace=collection_namespace)
            query_2 = query_gen_2.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query_1)
            self.run_cbq_query(query=query_2)

    def test_gsi_for_collection(self):
        pre_load_idx_pri, pre_load_idx_gsi = self.prepare_collection_for_indexing(indexes_before_load=True)
        collection_namespace = self.namespaces[0]

        query_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        indx_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().expiration'])
        primary_gen = QueryDefinition(index_name='#primary')
        try:
            # Checking for secondary index creation on named collection
            query = query_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            # todo (why it's failing even though build is complete)
            self.sleep(120)

            query = indx_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = indx_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            # todo (why it's failing even though build is complete)
            self.sleep(120)

            query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = primary_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            # todo (why it's failing even though build is complete)
            self.sleep(120)

            query = f'SELECT age from {collection_namespace} where age > 65'
            result = self.run_cbq_query(query=query)['results']
            self.assertNotEqual(len(result), 0, f'Actual : {result}')

            # creating few docs to check the index behavior for insert with expiration
            doc_count_query = f'SELECT count(*) FROM {collection_namespace}'
            count = self.run_cbq_query(query=doc_count_query)['results'][0]['$1']

            value = {
                "city": "Test Dee",
                "country": "Test Verde",
                "firstName": "Test name",
                "lastName": "Test Funk",
                "streetAddress": "66877 Williamson Terrace",
                "suffix": "V",
                "title": "International Solutions Coordinator"
            }
            # exptime = 20
            exptime = 0
            for key_id in range(20):
                doc_id = f'new_doc_{key_id}'
                doc_body = value
                doc_body['age'] = random.randint(30, 70)
                insert_query = f"INSERT into {collection_namespace} (KEY, VALUE) VALUES('{doc_id}', {doc_body}," \
                               f" {{'expiration': {exptime}}}) "
                self.run_cbq_query(query=insert_query)

            count += 20
            doc_count = self.run_query_with_retry(query=doc_count_query, expected_result=count, is_count_query=True)
            self.assertEqual(doc_count, count,
                             f"Results are not matching. Actual: {result}, Expected: {count} ")

            # # Checking for TTL
            # self.sleep(20, 'Waiting for docs to get expired')
            # count -= 20
            # query = f'SELECT meta().id FROM {collection_namespace} WHERE meta().expiration IS NOT NULL'
            # result = self.run_cbq_query(query=query)['results']
            # self.assertEqual(len(result), count, f"Results are not matching. Actual: {result}, Expected: {count} ")

            # deleting docs
            query = f'SELECT meta().id FROM {collection_namespace}'
            doc_ids = self.run_cbq_query(query=query)['results']
            docs_to_delete = [doc_id['id'] for doc_id in random.choices(doc_ids, k=10)]
            doc_ids = ", ".join([f'"{item}"' for item in docs_to_delete])
            delete_query = f'DELETE FROM {collection_namespace} d WHERE meta(d).id in [{doc_ids}] RETURNING d'
            self.run_cbq_query(query=delete_query)
            count -= len(docs_to_delete)
            doc_count = self.run_query_with_retry(query=doc_count_query, expected_result=count, is_count_query=True)
            self.assertEqual(doc_count, count, f"Actual: {doc_count}, Expected: {count}")

            # updating docs
            query = f'UPDATE {collection_namespace} SET updated = true WHERE age > 65'
            self.run_cbq_query(query=query)
            query = f'SELECT meta().id FROM {collection_namespace} WHERE age > 65'
            queried_docs = self.run_cbq_query(query=query)['results']
            queried_docs = sorted([item['id'] for item in queried_docs])
            query = f'SELECT meta().id FROM {collection_namespace} WHERE updated = true'
            updated_docs_ids = self.run_cbq_query(query=query)['results']
            updated_docs_ids = sorted([item['id'] for item in updated_docs_ids])

            self.assertEqual(queried_docs, updated_docs_ids, f"Actual: {queried_docs}, Expected: {updated_docs_ids}")
            doc_count = self.run_query_with_retry(query=doc_count_query, expected_result=count, is_count_query=True)
            self.assertEqual(doc_count, count, f"Actual: {doc_count}, Expected: {count}")

            # upserting docs
            upsert_doc_list = ['upsert-1', 'upsert-2']
            query = f'UPSERT INTO {collection_namespace} (KEY, VALUE) VALUES ' \
                    f'("upsert-1", {{ "firstName": "Michael", "age": 72}}),' \
                    f'("upsert-2", {{"firstName": "George", "age": 75}})' \
                    f' RETURNING VALUE name'
            self.run_cbq_query(query=query)
            self.sleep(5, 'Giving some time to indexer to index newly inserted docs')
            query = f'SELECT meta().id FROM {collection_namespace} WHERE age > 70'
            upsert_doc_ids = self.run_cbq_query(query=query)['results']
            upsert_doc_ids = sorted([item['id'] for item in upsert_doc_ids])
            self.assertEqual(upsert_doc_ids, upsert_doc_list,
                             f"Actual: {upsert_doc_ids}, Expected: {upsert_doc_list}")
            count += len(upsert_doc_list)
            doc_count = self.run_query_with_retry(query=doc_count_query, expected_result=count, is_count_query=True)
            self.assertEqual(doc_count, count, f"Actual: {doc_count}, Expected: {count}")

            # checking if pre-load indexes indexed docs
            query = f'SELECT COUNT(*) FROM {collection_namespace} WHERE firstName is not null'
            result = self.run_query_with_retry(query=query, expected_result=count, is_count_query=True)
            self.assertEqual(result, count, f"Actual: {doc_count}, Expected: {count}")

            query = f'SELECT COUNT(*) FROM {collection_namespace}'
            result = self.run_query_with_retry(query=query, expected_result=count, is_count_query=True)
            self.assertEqual(result, count, f"Actual: {doc_count}, Expected: {count}")
        except Exception as err:
            self.fail(str(err))
        finally:
            query = query_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = indx_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = primary_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

            # Deleting pre-load-indexes
            query = pre_load_idx_pri.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = pre_load_idx_gsi.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

    def test_multiple_indexes_on_same_field(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        primary_gen = QueryDefinition(index_name='#primary')
        query_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        query_gen_copy = QueryDefinition(index_name='idx_copy', index_fields=['age'])
        # preparing index
        try:
            query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            if self.defer_build:
                query = primary_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online(defer_build=self.defer_build)
            query = f'SELECT COUNT(*) from {collection_namespace}'
            count = self.run_query_with_retry(query=query, expected_result=self.num_of_docs_per_collection,
                                              is_count_query=True)
            self.assertEqual(count, self.num_of_docs_per_collection, "Docs count not matching")

            query = query_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online(defer_build=self.defer_build)

            query = query_gen_copy.generate_index_create_query(namespace=collection_namespace,
                                                               defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen_copy.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=self.defer_build)

            # Running query against GSI index idx and idx_copy
            query = f'select count(*) from {collection_namespace} where age is not null;'
            result = self.run_query_with_retry(query=query, expected_result=count, is_count_query=True)
            self.assertEqual(result, count, f"Actual: {result}, Expected: {count}")

        except Exception as err:
            self.fail(str(err))
        finally:
            query_1 = primary_gen.generate_index_drop_query(namespace=collection_namespace)
            query_2 = query_gen.generate_index_drop_query(namespace=collection_namespace)
            query_3 = query_gen_copy.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query_1)
            self.run_cbq_query(query=query_2)
            self.run_cbq_query(query=query_3)

    def test_gsi_indexes_with_WITH_clause(self):
        if self.capella_run:
            self.skipTest("With clause cannot be used in provsioned tests")
        index_nodes = self.get_nodes_from_services_map(service_type="index",
                                                       get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 2 index nodes to run this test")
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]

        # index creation with num replica
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = index_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(10)
            # querying docs for the idx index
            query = f'SELECT COUNT(*) FROM {collection_namespace} WHERE age > 65'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0, f"Actual: {result}, Expected: 0")
        except Exception as err:
            self.fail(str(err))
        finally:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

        # index creation with node info
        try:
            replica_nodes = []
            for node in index_nodes:
                if self.use_https:
                    port = '18091'
                else:
                    port = '8091'
                replica_nodes.append(f'{node.ip}:{port}')
            query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                          deploy_node_info=replica_nodes, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = index_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(5)

            # querying docs for the idx index
            query = f'SELECT COUNT(*) FROM {collection_namespace} WHERE age > 65'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0, f"Actual: {result}, Expected: 0")
        except Exception as err:
            self.fail(str(err))
        finally:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

    def test_gsi_array_indexes(self):
        self.prepare_collection_for_indexing(json_template="Employee")
        collection_namespace = self.namespaces[0]
        primary_gen = QueryDefinition(index_name='#primary')
        self.sleep(30)
        doc_count = self.run_cbq_query(query=f'select count(*) from {collection_namespace}')['results'][0]['$1']
        arr_index = "arr_index"
        # preparing index
        try:
            query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            if self.defer_build:
                query = primary_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(10)

            # Creating Array index
            query = f"create index {arr_index} on {collection_namespace}(ALL ARRAY v.name for v in VMs END) "
            self.run_cbq_query(query=query)
            self.sleep(10)
            result = self.wait_until_indexes_online()
            if not result:
                self.wait_until_indexes_online(timeout=1200)
            # Run a query that uses array indexes
            query = f'select count(*) from {collection_namespace}  where any v in VMs satisfies v.name like "vm_%" END'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(result, doc_count, f"Expected: {doc_count}, Actual: {result}")
            # Dropping the indexer
            query = f"DROP INDEX {arr_index} ON {collection_namespace}"
            self.run_cbq_query(query=query)

            # Partial Indexes
            query = f"create index {arr_index} on {collection_namespace}(ALL ARRAY v.name for v in VMs END) " \
                    f"where join_mo > 8"
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            # Run a query that uses array indexes
            query = f'explain select count(*) from {collection_namespace}  where join_mo > 8 AND ' \
                    f'any v in VMs satisfies v.name like "vm_%" END'
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['scan']['index'], 'arr_index',
                             "Array index arr_index is not used.")
            query = f'explain select count(*) from {collection_namespace}  where join_mo > 7 AND ' \
                    f'any v in VMs satisfies v.name like "vm_%" END'
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], '#primary',
                             "Array index arr_index is used.")
            # Dropping the indexer
            query = f"DROP INDEX {arr_index} ON {collection_namespace}"
            self.run_cbq_query(query=query)

            # Checking for Simplified index
            query = f"create index {arr_index} on {collection_namespace}(ALL  VMs)"
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            # Run a query that uses array indexes
            query = f' select count(*) from {collection_namespace} where ' \
                    f'any v in VMs satisfies v.name like "vm_%" and v.memory like "%1%" END'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 1000)
            query = f'explain {query}'
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['scan']['index'], 'arr_index',
                             "Array index arr_index is not used.")
            # Dropping the indexer
            query = f"DROP INDEX {arr_index} ON {collection_namespace}"
            self.run_cbq_query(query=query)
        except Exception as err:
            # Dropping the indexer
            query = f"DROP INDEX {arr_index} ON {collection_namespace}"
            self.run_cbq_query(query=query)
            self.fail(str(err))
        finally:
            query = primary_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

    def test_index_partitioning(self):
        self.prepare_collection_for_indexing(json_template="Employee")
        collection_namespace = self.namespaces[0]
        arr_index = "arr_index"
        primary_gen = QueryDefinition(index_name='#primary')
        index_gen = QueryDefinition(index_name=arr_index, index_fields=['join_mo', 'join_day'],
                                    partition_by_fields=['meta().id'])
        try:
            query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            if self.defer_build:
                query = primary_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(60)

            # Creating Partitioned index
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = index_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(60)

            # Validating index partition
            index_metadata = self.rest.get_indexer_metadata()['status']
            self.log.info("Index metadata after index partition query :{}".format(index_metadata))
            for index in index_metadata:
                if index['name'] != arr_index:
                    continue
                self.assertTrue(index['partitioned'], f"{arr_index} is not a partitioned index")
                self.assertEqual(index['numPartition'], 8, "No. of partitions are not matching")

            # Run a query that uses partitioned indexes
            query = f'select count(*) from {collection_namespace}  where join_mo > 3 and join_day > 15'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)

            query = f'explain {query}'
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], 'arr_index',
                             f"index arr_index is not used. Index used is {result[0]['plan']['~children'][0]['index']}")
            # Dropping the indexer
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

            # Creating partial partitioned index
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          index_where_clause="test_rate > 5")
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = index_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online(defer_build=self.defer_build)
            self.sleep(60)

            # Validating index partition
            index_metadata = self.rest.get_indexer_metadata()['status']
            self.log.info("Index metadata after Creating partial partitioned index :{}".format(index_metadata))
            for index in index_metadata:
                if index['name'] != arr_index:
                    continue
                self.assertTrue(index['partitioned'], f"{arr_index} is not a partitioned index")
                self.assertEqual(index['numPartition'], 8, "No. of partitions are not matching")

            # Run a query that uses partitioned indexes
            query = f'select count(*) from {collection_namespace}  where join_mo > 3 and join_day > 15' \
                    f' and test_rate > 5'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)

            query = f'explain {query}'
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], 'arr_index',
                             f"index arr_index is not used. Index used is {result[0]['plan']['~children'][0]['index']}")
            # Dropping the indexer
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            self.fail(str(err))
        finally:
            query = primary_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

    def test_partial_indexes(self):
        self.prepare_collection_for_indexing(json_template="Employee")
        collection_namespace = self.namespaces[0]
        arr_index = "arr_index"
        primary_gen = QueryDefinition(index_name='#primary')
        index_gen = QueryDefinition(index_name=arr_index, index_fields=['join_mo', 'join_day'])
        try:
            query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            if self.defer_build:
                query = primary_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(10)

            # Creating Partial index
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          index_where_clause="join_yr > 2010")
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = index_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.sleep(10)

            query = f"select count(*) from {collection_namespace} where join_mo > 8 and join_day > 15 and" \
                    f" join_yr > 2010"
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)

            query = f'Explain {query}'
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], arr_index,
                             f'index {arr_index} is not used.')
            # Dropping the indexer
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

        except Exception as err:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            self.fail(str(err))
        finally:
            query = primary_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

    def test_same_name_indexes(self):
        # different scopes, different named collections
        num_combo = 2
        collection_prefix = 'test_collection'
        scope_prefix = 'test_scope'
        collection_namespaces = []
        arr_index = 'arr_index'
        index_gen = QueryDefinition(index_name=arr_index, index_fields=['age'])
        for item in range(num_combo):
            scope = f"{scope_prefix}_{item}"
            collection = f"{collection_prefix}_{item}"
            self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
            col_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
            collection_namespaces.append(col_namespace)
            self.gen_create = SDKDataLoader(num_ops=1000, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope, collection=collection, username=self.username, password=self.password)
            self._load_all_buckets(self.master, self.gen_create)
            query = index_gen.generate_index_create_query(namespace=col_namespace)
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()

        for col_namespace in collection_namespaces:
            _, keyspace = col_namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            try:
                # running a query that would use above index
                query = f"Select count(*) from {col_namespace} where age > 40"
                result = self.run_cbq_query(query=query)['results'][0]['$1']
                self.assertNotEqual(result, 0)

                query = f"Explain {query}"
                result = self.run_cbq_query(query=query)['results']
                self.assertEqual(result[0]['plan']['~children'][0]['index'], arr_index,
                                 f'index {arr_index} is not used.')
                query = index_gen.generate_index_drop_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.collection_rest.delete_scope_collection(bucket=bucket, scope=scope, collection=collection)
            except Exception as err:
                query = index_gen.generate_index_drop_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.fail(str(err))

        # different scopes, same named collection
        num_scope = 2
        collection = 'test_collection'
        collection_namespaces = []
        for item in range(num_scope):
            scope = f"scope_{item}"
            self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
            collection_namespaces.append(f"default:{self.test_bucket}.{scope}.{collection}")
            self.gen_create = SDKDataLoader(num_ops=1000, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope, collection=collection, username=self.username, password=self.password)
            self._load_all_buckets(self.master, self.gen_create)
        for col_namespace in collection_namespaces:
            _, keyspace = col_namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            try:
                query = index_gen.generate_index_create_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online()

                # running a query that would use above index
                query = f"Select count(*) from {col_namespace} where age > 40"
                result = self.run_cbq_query(query=query)['results'][0]['$1']
                self.assertNotEqual(result, 0)

                query = f"Explain {query}"
                result = self.run_cbq_query(query=query)['results']
                self.assertEqual(result[0]['plan']['~children'][0]['index'], arr_index,
                                 f'index {arr_index} is not used.')
                query = index_gen.generate_index_drop_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.collection_rest.delete_scope_collection(bucket=bucket, scope=scope, collection=collection)
            except Exception as err:
                query = index_gen.generate_index_drop_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.fail(str(err))

        # Same scope, different named collections
        num_collection = 2
        scope = 'test_scope'
        collection_namespaces = []
        self.collection_rest.create_scope(bucket=self.test_bucket, scope=scope)
        for item in range(num_collection):
            collection = f"collection_{item}"
            self.collection_rest.create_collection(bucket=self.test_bucket, scope=scope, collection=collection)
            collection_namespaces.append(f"default:{self.test_bucket}.{scope}.{collection}")
            self.gen_create = SDKDataLoader(num_ops=1000, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope, collection=collection, username=self.username, password=self.password)
            self._load_all_buckets(self.master, self.gen_create)
        for col_namespace in collection_namespaces:
            _, keyspace = col_namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            try:
                query = index_gen.generate_index_create_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online()

                # running a query that would use above index
                query = f"Select count(*) from {col_namespace} where age > 40"
                result = self.run_cbq_query(query=query)['results'][0]['$1']
                self.assertNotEqual(result, 0)

                query = f"Explain {query}"
                result = self.run_cbq_query(query=query)['results']
                self.assertEqual(result[0]['plan']['~children'][0]['index'], arr_index,
                                 f'index {arr_index} is not used.')
                query = index_gen.generate_index_drop_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=collection)
            except Exception as err:
                query = index_gen.generate_index_drop_query(namespace=col_namespace)
                self.run_cbq_query(query=query)
                self.fail(str(err))

    def test_build_indexes_at_different_stages(self):
        # todo: Incomplete test
        scope = 'test_scope'
        collection = 'test_collection'
        arr_index = 'arr_index'
        index_gen = QueryDefinition(index_name=arr_index, index_fields=['age'])
        primary_gen = QueryDefinition(index_name='#primary')
        self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
        collection_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
        self.gen_create = SDKDataLoader(num_ops=10000, percent_create=100,
                                        percent_update=0, percent_delete=0, scope=scope, collection=collection,username=self.username, password=self.password)
        self._load_all_buckets(self.master, self.gen_create)
        query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        query = f"select meta().id from {collection_namespace}"
        results = self.run_cbq_query(query=query)['results']
        doc_ids = [doc['id'] for doc in results]
        # building indexes during create operation

        # building indexes during upsert operation

        # building indexes during update operation

        # building indexes during delete operation

    def test_index_creation_with_increased_seq_num(self):
        """Create/Drop collection increment the seqno of a vbucket. It is important to test if indexer can handle those
         e.g. load docs in colA, create index idx1 on colA, create colB, build index idx1 on colA, drop colB,
         build index idx2 on colA etc. Try it with both empty collection and collection with mutations."""
        # Todo: MB-40288
        arr_index_1 = 'arr_index_1'
        arr_index_2 = 'arr_index_2'
        index_gen_1 = QueryDefinition(index_name=arr_index_1, index_fields=['age'])
        index_gen_2 = QueryDefinition(index_name=arr_index_2, index_fields=['city'])
        primary_gen = QueryDefinition(index_name='#primary')
        scope = 'test_scope'
        col_a = 'colA'
        col_b = 'colB'

        # creating colA and then creating index arr_index_1 on empty collection.
        # creating colB and then building index arr_index_1.
        # dropping colB and then creating index arr_index_2
        namespace = f"default:{self.test_bucket}.{scope}.{col_a}"
        try:
            self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope,
                                                         collection=col_a)
            self.sleep(5)
            query = primary_gen.generate_primary_index_create_query(namespace=namespace)
            self.run_cbq_query(query=query)
            query = index_gen_1.generate_index_create_query(namespace=namespace, defer_build=True)
            self.run_cbq_query(query=query)
            self.collection_rest.create_collection(bucket=self.test_bucket, scope=scope, collection=col_b)
            query = index_gen_1.generate_build_query(namespace=namespace)
            self.run_cbq_query(query=query)
            self.collection_rest.delete_collection(bucket=self.test_bucket, scope=scope, collection=col_b)
            self.wait_until_indexes_online(defer_build=True)
            self.sleep(5)
            query = index_gen_2.generate_index_create_query(namespace=namespace)
            self.run_cbq_query(query=query)
            query = f'select count(*) from {namespace}'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(result, 0)
            query = index_gen_1.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query=query)
            query = index_gen_2.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query=query)
            self.collection_rest.delete_scope_collection(bucket=self.test_bucket, scope=scope, collection=col_a)

            # Trying with loaded collection
            self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=col_a)
            self.gen_create = SDKDataLoader(num_ops=1000, percent_create=100, percent_update=0, percent_delete=0,
                                            scope=scope, collection=col_a,username=self.username, password=self.password)
            self._load_all_buckets(self.master, self.gen_create)
            self.collection_rest.create_collection(bucket=self.test_bucket, scope=scope, collection=col_b)
            query = index_gen_1.generate_index_create_query(namespace=namespace, defer_build=True)
            self.run_cbq_query(query=query)
            self.collection_rest.delete_collection(bucket=self.test_bucket, scope=scope, collection=col_b)
            query = index_gen_1.generate_build_query(namespace=namespace)
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online(defer_build=True)
            query = index_gen_2.generate_index_create_query(namespace=namespace)
            self.run_cbq_query(query=query)
            query = f'select count(*) from {namespace} where age > 0'
            result = self.run_query_with_retry(query=query, expected_result=1000, is_count_query=True)
            self.assertEqual(result, 1000)
            query = index_gen_1.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query=query)
            query = index_gen_2.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query=query)
            self.collection_rest.delete_scope_collection(bucket=self.test_bucket, scope=scope, collection=col_a)
        except Exception as err:
            self.log.error(str(err))
            query = index_gen_1.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query=query)
            query = index_gen_2.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query=query)
            self.fail()

    def test_indexes_with_deferred_build(self):
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        index_gen_1 = QueryDefinition(index_name='idx_1', index_fields=['age'])
        index_gen_2 = QueryDefinition(index_name='idx_2', index_fields=['city'])
        index_gen_3 = QueryDefinition(index_name='idx_3', index_fields=['city', 'age'])
        index_gen_4 = QueryDefinition(index_name='idx_4', index_fields=['firstName'])

        # preparing index and then building multiple indexes together
        try:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace,
                                                            deploy_node_info=self.deploy_node_info,
                                                            defer_build=True,
                                                            num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace,
                                                            deploy_node_info=self.deploy_node_info,
                                                            defer_build=True,
                                                            num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            query = index_gen_3.generate_index_create_query(namespace=collection_namespace,
                                                            deploy_node_info=self.deploy_node_info,
                                                            defer_build=True,
                                                            num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)
            query = index_gen_4.generate_index_create_query(namespace=collection_namespace,
                                                            deploy_node_info=self.deploy_node_info,
                                                            defer_build=True,
                                                            num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)

            query = f"BUILD INDEX ON {collection_namespace}(idx_1, idx_2, idx_3) USING GSI "
            self.run_cbq_query(query=query)
            self.sleep(5)

            # Building index while another index is in progress to check background queue
            try:
                query = f"BUILD INDEX ON {collection_namespace}(idx_4) USING GSI "
                self.run_cbq_query(query=query)
            except Exception as err:
                err_msg = 'Index idx_4 will retry building in the background for reason: Build Already In Progress'
                self.assertTrue(err_msg in str(err), str(err))
            result = self.wait_until_indexes_online()
            if not result:
                self.wait_until_indexes_online(timeout=1200)
            self.sleep(10)

            query = f"select count(age) from {collection_namespace} where age > 30"
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)
            query = f"Explain {query}"
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], 'idx_1', 'index idx_1 is not used.')

            query = f"select count(city) from {collection_namespace} where city like 'C%'"
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)
            query = f"Explain {query}"
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], 'idx_2', 'index idx_2 is not used.')

            query = f"select count(*) from {collection_namespace} where age > 30 and city like 'T%' "
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)
            query = f"Explain {query}"
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], 'idx_3', 'index idx_3 is not used.')

            query = f"select count(*) from {collection_namespace} where firstName like 'A%' "
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0)
            query = f"Explain {query}"
            result = self.run_cbq_query(query=query)['results']
            self.assertEqual(result[0]['plan']['~children'][0]['index'], 'idx_4', 'index idx_4 is not used.')
        except Exception as err:
            self.fail(str(err))
        finally:
            query = index_gen_1.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = index_gen_2.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = index_gen_3.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = index_gen_4.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

    def test_drop_indexes_scenarios_with_dgm(self):
        if self.gsi_type == 'memory_optimized':
            self.skipTest("DGM can be achieved only for plasma")
        dgm_server = self.get_nodes_from_services_map(service_type="index")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=10 ** 5)
        self.get_dgm_for_plasma(indexer_nodes=[dgm_server])
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'firstName', 'country'])
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                          deploy_node_info=self.deploy_node_info,
                                                          defer_build=False,
                                                          num_replica=self.num_index_replicas)

            self.run_cbq_query(query=query)

            def __run_query_in_loop(query_in_loop, num_of_runs=100):
                query_results = []
                with ThreadPoolExecutor() as task_executor:
                    for _ in range(num_of_runs):
                        task_out = task_executor.submit(self.run_cbq_query, query_in_loop)
                        query_results.append(task_out.result())
                return query_results

            # Checking for Secondary index
            query = f'select age, city from {collection_namespace} where age > 0 and age < 60 and' \
                    f' city like "%t%" and city not like "P%" and firstName like "%A%" and country like "S%"'
            drop_query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            try:
                with ThreadPoolExecutor() as executor:
                    task_1 = executor.submit(__run_query_in_loop, query)
                    self.sleep(1)
                    task_2 = executor.submit(self.run_cbq_query, drop_query)
                    self.log.info(task_2.result())
                    task_1.result()
            except Exception as err:
                err_msg = 'No index available on keyspace'
                self.assertTrue(err_msg in str(err), err)

        except Exception as err:
            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            self.fail(str(err))

    def test_drop_indexes_scenarios(self):
        scope = 'test_scope'
        collection = 'test_collection'
        self.prepare_collection_for_indexing()
        self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
        self.sleep(30)
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'])

        collection_namespace = f'default:{self.test_bucket}.{scope}.{collection}'
        collection_namespace_2 = self.namespaces[0]
        is_ee = self.rest.is_enterprise_edition()

        system_indexes_query = "Select * from system:indexes"

        # Dropping index on empty collection
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query)
            if self.defer_build:
                query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(query)
            self.wait_until_indexes_online(defer_build=self.defer_build)

            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

            result = self.run_cbq_query(system_indexes_query)['results']
            self.assertEqual(len(result), 0, "Drop Index didn't work as expected")
        except Exception as err:
            self.fail(f'Failed to Drop Index on empty collection: {str(err)}')

        # Dropping deferred index before creating it
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True)
            self.run_cbq_query(query)

            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

            result = self.run_cbq_query(system_indexes_query)['results']
            self.assertEqual(len(result), 0, "Drop Index didn't work as expected")
        except Exception as err:
            self.fail(f'Failed to Drop Deferred Index on before build: {str(err)}')

        # Dropping index with same name on different collections
        try:
            query_1 = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                            defer_build=self.defer_build)
            query_2 = index_gen.generate_index_create_query(namespace=collection_namespace_2,
                                                            defer_build=self.defer_build)
            self.run_cbq_query(query_1)
            self.run_cbq_query(query_2)

            drop_query_1 = index_gen.generate_index_drop_query(namespace=collection_namespace)
            drop_query_2 = index_gen.generate_index_drop_query(namespace=collection_namespace_2)
            self.run_cbq_query(drop_query_1)
            self.run_cbq_query(drop_query_2)

            result = self.run_cbq_query(system_indexes_query)['results']
            self.assertEqual(len(result), 0, "Drop Index didn't work as expected")
        except Exception as err:
            self.fail(f'Failed to Drop indexes with same name on different collections: {str(err)}')

        # Dropping replica index
        if not is_ee:
            self.log.info("Replica indexes are not supported in CE. Hence skipping drop of Replica Indexes")
            return
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace_2,
                                                          defer_build=self.defer_build, num_replica=1)
            self.run_cbq_query(query)
            if self.defer_build:
                query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(query)
            self.wait_until_indexes_online(defer_build=self.defer_build)

            query = f'ALTER INDEX idx on {collection_namespace_2} WITH {{"action":"drop_replica","replicaId": 1}}'
            self.run_cbq_query(query=query)

            result = self.run_cbq_query(system_indexes_query)['results']
            result = result[0]['indexes']
            self.assertNotEqual(len(result), 0, "Drop Index didn't work as expected")
            self.sleep(10)
            status = self.rest.get_index_status()
            self.assertEqual(len(status[self.test_bucket]), 1, f"Fail to drop replica of idx index: {status}")
        except Exception as err:
            self.fail(f'Failed to Drop Index on empty collection: {str(err)}')

    def test_create_negative_scenarios(self):
        # Checking for duplicate index names
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        index_gen_1 = QueryDefinition(index_name='idx', index_fields=['age'])
        index_gen_2 = QueryDefinition(index_name='idx', index_fields=['city'])
        try:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'The index idx already exists'
            self.assertTrue(err_msg in str(err), f"Index with duplicate named is create: {err}")

        # invalid name for GSI
        index_gen_1 = QueryDefinition(index_name='invalid*', index_fields=['age'])
        index_gen_2 = QueryDefinition(index_name='invalid%', index_fields=['age'])
        try:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Invalid index name'
            self.assertTrue(err_msg in str(err), f"Index with invalid name is created: {err}")
        try:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Invalid index name'
            self.assertTrue(err_msg in str(err), f"Secondary Index with duplicate named is created: {err}")

        # index on a non-existent bucket/scope/collection
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        invalid_bucket = collection_namespace.replace(self.test_bucket, 'invalid_bucket')
        invalid_scope = collection_namespace.replace('scope', 'invalid_scope')
        invalid_collection = collection_namespace.replace('collection', 'invalid_collection')
        try:
            query = index_gen.generate_index_create_query(namespace=invalid_bucket)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Keyspace not found in CB datastore: default:invalid_bucket - ' \
                      'cause: No bucket named invalid_bucket'
            self.assertTrue(err_msg in str(err), f"Index with non-existent bucket is created: {err}")
        try:
            query = index_gen.generate_index_create_query(namespace=invalid_scope)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Scope not found in CB datastore default:test_bucket.test_invalid_scope_1'
            self.assertTrue(err_msg in str(err), f"Index with non-existent scope created: {err}")
        try:
            query = index_gen.generate_index_create_query(namespace=invalid_collection)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Keyspace not found in CB datastore: default:test_bucket.test_scope_1.test_invalid_collection_1'
            self.assertTrue(err_msg in str(err), f"Index with non-existent collection is created: {err}")

        # index on dropped collection
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=collection)
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            # err_msg = 'Keyspace not found in CB datastore: default:test_bucket.test_scope_1.test_collection_1'
            # self.assertTrue(err_msg in str(err), f"Index with duplicate named is create: {err}")
            self.log.info("Couldn't create duplicate named index")
            self.log.info(err)

    def test_drop_negative_scenarios(self):
        # dropping an already dropped index
        self.prepare_collection_for_indexing(num_of_docs_per_collection=9 * 10 ** 4)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        system_indexes_query = "Select * from system:indexes"
        try:
            query = index_gen.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()

            query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)

            result = self.run_cbq_query(query=system_indexes_query)['results']
            self.assertEqual(len(result), 0, 'Indexes still available in system indexes after drop')
            # trying to drop already dropped index
            self.run_cbq_query(query)
        except Exception as err:
            err_msg = 'GSI index idx not found'
            self.assertTrue(err_msg in str(err), f"Dropping an already dropped index is not giving error: {err}")

        # dropping an non-existent index
        try:
            query = f'Drop index idx_idx on {collection_namespace}'
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'GSI index idx_idx not found'
            self.assertTrue(err_msg in str(err), f"Dropping an non-existent index is not giving error: {err}")

        # dropping an index during index creation
        # try:
        #     query = index_gen.generate_index_create_query(namespace=collection_namespace)
        #     drop_query = index_gen.generate_index_drop_query(namespace=collection_namespace)
        #     with ThreadPoolExecutor() as executor:
        #         executor.submit(self.run_cbq_query, query)
        #         self.sleep(1)
        #         executor.submit(self.run_cbq_query, drop_query)
        #         out = executor.result()
        # except Exception as err:
        #     err_msg = 'GSI index idx not found.'
        #     # self.assertTrue(err_msg in str(err), f"Dropping an non-existent index is not giving error: {err}")
        # result = self.run_cbq_query(system_indexes_query)['results']
        # self.assertEqual(len(result), 0, "Index didn't drop during index creation phase ")

    def test_build_scenarios(self):
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        index_gen_list = []
        num_index = 5
        for item in range(num_index):
            index_gen_list.append(QueryDefinition(index_name=f'idx_{item}', index_fields=['age']))
        indexes = [f"idx_{item}" for item in range(num_index)]
        indexes_to_build = random.sample(indexes, 2)
        try:
            for index_gen in index_gen_list:
                query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True)
                self.run_cbq_query(query=query)

            # Building some indexes twice
            build_query = f'Build index on {collection_namespace}({",".join(indexes_to_build)})'
            self.run_cbq_query(query=build_query)

            # Building all indexes with some already built
            build_query = f'Build index on {collection_namespace}({",".join(indexes)})'
            self.run_cbq_query(query=build_query)
            self.sleep(10)
            self.wait_until_indexes_online()
        except Exception as err:
            err_msg1 = "Build index fails. Some index will be retried building in the background"
            err_msg2 = "will retry building in the background for reason: Build Already In Progress"
            if err_msg1 in str(err) or err_msg2 in str(err):
                self.sleep(10)
                result = self.wait_until_indexes_online()
                if not result:
                    self.log.info("Index status check timed out. Retrying one more time")
                    result = self.wait_until_indexes_online()
                    if not result:
                        self.fail("Still not finished building. Check the logs")
                    else:
                        self.log.info("All indexes are built")
            else:
                self.fail(str(err))

    def test_build_multiple_index_concurrently_across_collections(self):
        num_docs = 1000
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=num_docs)
        index_gen_list = []
        for item, namespace in enumerate(self.namespaces):
            idx_name = f'idx_{item}'
            index_gen = QueryDefinition(index_name=idx_name, index_fields=['age', 'city'])
            query = index_gen.generate_index_create_query(namespace=namespace, defer_build=False)
            self.run_cbq_query(query=query)
            index_gen_list.append(index_gen)

        with ThreadPoolExecutor() as executor:
            for index_gen, namespace in zip(index_gen_list, self.namespaces):
                query = index_gen.generate_build_query(namespace=namespace)
                executor.submit(self.run_cbq_query, query=query)
        self.wait_until_indexes_online(timeout=600)

        for namespace in self.namespaces:
            query = f'select count(age) from {namespace} where age >= 0'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            if self.dgm_run:
                self.assertTrue(result > num_docs,
                                f"Doc count not matching expected value")
            else:
                self.assertEqual(num_docs, result, "Doc count not matching")

    # def test_large_value_in_metadata_store(self):
    #     '''
    #     keeping it as a placeholder
    #     Will cover it in volume or system test. after that will remove it from here.
    #     '''
    #     buckets_list = []
    #     collection_namespaces_list = []
    #     self.rest.delete_bucket(bucket=self.test_bucket)
    #     num_docs = 10
    #     task_list = []
    #
    #     namespaces_dict = {}
    #     with ThreadPoolExecutor() as executor:
    #         for item in range(10):
    #             bucket_name = f'bucket_{item}_'
    #             bucket_name = bucket_name + "".join(random.choices(string.ascii_letters + string.digits,
    #                                                                k=100 - len(bucket_name)))
    #             self.test_bucket = bucket_name
    #             buckets_list.append(self.test_bucket)
    #             executor.submit(self.cluster.create_standard_bucket, name=self.test_bucket, port=11222,
    #                             bucket_params=self.bucket_params)
    #             if item > 5:
    #                 self.sleep(10)
    #             namespaces_dict[bucket_name] = {}
    #
    #     with ThreadPoolExecutor() as executor:
    #         for bucket_name in namespaces_dict:
    #             scope_tasks = []
    #             for item in range(10):
    #                 scope = f"test_scope_{item}_"
    #                 scope = scope + "".join(random.choices(string.ascii_letters + string.digits, k=30 - len(scope)))
    #                 task = executor.submit(self.collection_cli.create_scope, bucket=bucket_name, scope=scope)
    #                 namespaces_dict[bucket_name][scope] = []
    #                 scope_tasks.append(task)
    #             for task in scope_tasks:
    #                 task.result()
    #     self.sleep(10)
    #     with ThreadPoolExecutor() as executor:
    #         for bucket_name in namespaces_dict:
    #             for scope in namespaces_dict[bucket_name]:
    #                 collection_tasks = []
    #                 for item in range(10):
    #                     collection = f'test_collection_{item}_'
    #                     collection = collection + "".join(random.choices(string.ascii_letters + string.digits,
    #                                                                      k=30 - len(collection)))
    #                     task = executor.submit(self.collection_cli.create_collection, bucket=bucket_name,
    #                                            scope=scope, collection=collection)
    #                     collection_namespace = f'default:{bucket_name}.{scope}.{collection}'
    #                     collection_namespaces_list.append(collection_namespace)
    #                     collection_tasks.append(task)
    #                     namespaces_dict[bucket_name][scope].append(collection)
    #                 for task in collection_tasks:
    #                     task.result()
    #                 self.log.info(f"Created collection no.: {len(collection_namespaces_list)}")
    #     self.sleep(10, "Giving some time after collections creation")
    #
    #     batch_size = 5
    #     try:
    #         for batch in range(0, len(collection_namespaces_list), batch_size):
    #             for namespace in collection_namespaces_list[batch: batch+batch_size]:
    #                 _, keyspace = namespace.split(':')
    #                 bucket, scope, collection = keyspace.split('.')
    #                 gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100,
    #                                            percent_update=0, percent_delete=0, scope=scope,
    #                                            collection=collection, json_template='Person',username=self.username, password=self.password)
    #                 task = self.cluster.async_load_gen_docs(server=self.master, generator=gen_create,
    #                                                         bucket=bucket,
    #                                                         scope=scope, collection=collection)
    #                 task_list.append(task)
    #             for task in task_list:
    #                 task.result()
    #             self.sleep(15, f"Giving some time after batch: {collection_namespaces_list[batch: batch+batch_size]}")
    #     except Exception as err:
    #         self.fail(f"Failed to load data to Collections. Exception occurred:{err}")
    #
    #     index_gen_list = []
    #     for item, namespace in enumerate(collection_namespaces_list):
    #         idx_name = f'idx_{item}'
    #         index_gen = QueryDefinition(index_name=idx_name, index_fields=['age', 'city'])
    #         query = index_gen.generate_index_create_query(namespace=namespace, defer_build=self.defer_build)
    #         self.run_cbq_query(query=query)
    #         index_gen_list.append(index_gen)
    #         self.wait_until_indexes_online()
    #
    #     for namespace in collection_namespaces_list:
    #         query = f'select count(age) from {namespace} where age >= 0'
    #         result = self.run_cbq_query(query=query)['results'][0]['$1']
    #         self.assertEqual(num_docs, result, "Doc count not matching")
    #
    #     index_info = self.rest.get_indexer_metadata()['status']
    #     self.assertEqual(len(index_info), 10 * 6)

    def test_mutations_for_persistent_snapshot(self):
        '''
        Create a memory optimized index. Make changes in the indexer settings. Perform some mutations and validate that
        the persistent snapshots are getting created.
        https://issues.couchbase.com/browse/MB-50034
        '''
        if self.gsi_type != 'memory_optimized':
            self.skipTest("Peristent snapshot test is meant for memory optimized indexes")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        query = index_gen1.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        snapshot_count_before = self.get_persistent_snapshot_count(index_nodes[0], 'idx1')
        query = f'select count(*) from {collection_namespace} where age is not missing'
        self.run_cbq_query(query=query)
        total_doc_count_before = self.run_cbq_query(query=query)['results'][0]['$1']
        for index_node in index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 10})
        self.sleep(10)
        scope, collection = self.namespaces[0].split(":")[1].split(".")[1], self.namespaces[0].split(":")[1].split(".")[
            2]
        self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection,
                                        percent_create=0,
                                        percent_update=50, percent_delete=50, scope=scope,
                                        collection=collection, json_template="Person", username=self.username, password=self.password)
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                        batch_size=10 ** 4)
        for task in tasks:
            task.result()
        self.sleep(20)
        total_doc_count_after = self.run_cbq_query(query=query)['results'][0]['$1']
        snapshot_count_after = self.get_persistent_snapshot_count(index_nodes[0], 'idx1')
        self.assertNotEqual(total_doc_count_before, total_doc_count_after,
                            f'Indexing not happening after editing index settings.'
                            f'Count of docs before:{total_doc_count_before}. '
                            f'Count of docs after {total_doc_count_after}')
        self.assertNotEqual(snapshot_count_before, snapshot_count_after, f'Indexing not happening after editing index settings'
                                                                         f':Snapshot count before {snapshot_count_before}. '
                                                                         f'Snapshot count after {snapshot_count_after}')
