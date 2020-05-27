"""collections_index_basics.py: This test file contains tests for basic index operation in Collections context

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "26/05/20 2:13 pm" 

"""
import random

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats


class CollectionsIndexBasics(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexBasics, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
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
        self.num_of_docs_per_collection = 1000
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(CollectionsIndexBasics, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def _prepare_collection_for_indexing(self, num_scopes=1, num_collections=1, num_of_docs_per_collection=1000,
                                         skip_defaults=True, indexes_before_load=False):
        self.namespace = []
        pre_load_idx_pri = None
        pre_load_idx_gsi = None
        self.cli_rest.create_scope_collection_count(scope_num=num_scopes, collection_num=num_collections,
                                                    scope_prefix=self.scope_prefix,
                                                    collection_prefix=self.collection_prefix,
                                                    bucket=self.test_bucket)
        self.scopes = self.cli_rest.get_bucket_scopes(bucket=self.test_bucket)
        self.collections = self.cli_rest.get_bucket_collections(bucket=self.test_bucket)
        self.sleep(5)
        if skip_defaults:
            self.scopes.remove('_default')
            self.collections.remove('_default')
        if num_of_docs_per_collection > 0:
            for s_item in self.scopes:
                for c_item in self.collections:
                    self.namespace.append(f'default:{self.test_bucket}.{s_item}.{c_item}')
                    if indexes_before_load:
                        pre_load_idx_pri = QueryDefinition(index_name='pre_load_idx_pri')
                        pre_load_idx_gsi = QueryDefinition(index_name='pre_load_idx_gsi', index_fields=['firstname'])
                        query = pre_load_idx_pri.generate_primary_index_create_query(namespace=self.namespace[0])
                        self.run_cbq_query(query=query)
                        query = pre_load_idx_gsi.generate_index_create_query(bucket=self.namespace[0])
                        self.run_cbq_query(query=query)
                    self.gen_create = SDKDataLoader(num_ops=num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=s_item,
                                                    collection=c_item)
                    self._load_all_buckets(self.master, self.gen_create)
                    # gens_load = self.generate_docs(self.docs_per_day)
                    # self.load(gens_load, flag=self.item_flag, verify_data=False, batch_size=self.batch_size,
                    # collection=f"{s_item}.{c_item}")

        return pre_load_idx_pri, pre_load_idx_gsi

    def test_create_primary_index_for_collections(self):
        self._prepare_collection_for_indexing()
        collection_namespace = self.namespace[0]
        query_gen_1 = QueryDefinition(index_name='`#primary`')
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
                self.wait_until_indexes_online(defer_build=True)
            query = f'SELECT COUNT(*) from {collection_namespace}'
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, self.num_of_docs_per_collection, "Docs count not matching")
            # stat = self.stat.get_collection_stats(bucket=self.buckets[0])

            # Checking for named primary index
            query = query_gen_2.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen_2.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=True)
            query = f'SELECT COUNT(*) from {collection_namespace}'
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, self.num_of_docs_per_collection, "Docs count not matching")
        except Exception as err:
            self.fail(str(err))
        finally:
            query_1 = query_gen_1.generate_index_drop_query(bucket=collection_namespace, use_gsi_for_primary=True)
            query_2 = query_gen_2.generate_index_drop_query(bucket=collection_namespace, use_gsi_for_primary=True)
            self.run_cbq_query(query=query_1)
            self.run_cbq_query(query=query_2)

    def test_gsi_for_collection(self):
        pre_load_idx_pri, pre_load_idx_gsi = self._prepare_collection_for_indexing(indexes_before_load=True)
        collection_namespace = self.namespace[0]

        query_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        indx_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().expiration'])
        primary_gen = QueryDefinition(index_name='`#primary`')
        try:
            # Checking for secondary index creation on named collection
            query = query_gen.generate_index_create_query(bucket=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=True)
                # todo (why it's failing even though build is complete)
                self.sleep(5)

            query = indx_gen.generate_index_create_query(bucket=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = indx_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=True)
                # todo (why it's failing even though build is complete)
                self.sleep(5)

            query = primary_gen.generate_primary_index_create_query(namespace=collection_namespace,
                                                                    deploy_node_info=self.deploy_node_info,
                                                                    defer_build=self.defer_build,
                                                                    num_replica=self.num_index_replicas)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = primary_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=True)
                # todo (why it's failing even though build is complete)
                self.sleep(5)

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
            result = self.run_cbq_query(query=delete_query)['results']
            count -= len(docs_to_delete)
            # self.assertEqual(result, docs_to_delete, f"Actual: {result}, Expected: {docs_to_delete}")
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
            result = self.run_cbq_query(query=query)['results']
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
            self.assertEqual(result, count,  f"Actual: {doc_count}, Expected: {count}")

            query = f'SELECT COUNT(*) FROM {collection_namespace}'
            result = self.run_query_with_retry(query=query, expected_result=count, is_count_query=True)
            self.assertEqual(result, count,  f"Actual: {doc_count}, Expected: {count}")
        except Exception as err:
            self.fail(str(err))
        finally:
            query = query_gen.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)
            query = indx_gen.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)
            query = primary_gen.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)

            # Deleting pre-load-indexes
            query = pre_load_idx_pri.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)
            query = pre_load_idx_gsi.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)

    def test_multiple_indexes_on_same_field(self):
        self._prepare_collection_for_indexing()
        collection_namespace = self.namespace[0]
        primary_gen = QueryDefinition(index_name='`#primary`')
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
                self.wait_until_indexes_online(defer_build=True)
            query = f'SELECT COUNT(*) from {collection_namespace}'
            count = self.run_query_with_retry(query=query,expected_result=self.num_of_docs_per_collection,
                                              is_count_query=True)
            count = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertEqual(count, self.num_of_docs_per_collection, "Docs count not matching")

            query = query_gen.generate_index_create_query(bucket=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=True)

            query = query_gen_copy.generate_index_create_query(bucket=collection_namespace,
                                                               defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                query = query_gen_copy.generate_build_query(collection_namespace)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=True)

            # Running query against GSI index idx and idx_copy
            query = f'select count(*) from {collection_namespace} where age is not null;'
            result = self.run_query_with_retry(query=query, expected_result=count, is_count_query=True)
            self.assertEqual(result, count, f"Actual: {result}, Expected: {count}")

            query = f'EXPLAIN {query}'
            result = self.run_cbq_query(query=query)
            # self.assertEqual(result)

        except Exception as err:
            self.fail(str(err))
        finally:
            query_1 = primary_gen.generate_index_drop_query(bucket=collection_namespace)
            query_2 = query_gen.generate_index_drop_query(bucket=collection_namespace)
            query_3 = query_gen_copy.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query_1)
            self.run_cbq_query(query=query_2)
            self.run_cbq_query(query=query_3)

    def test_gsi_indexes_with_WITH_clause(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index",
                                                       get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 2 index nodes to run this test")
        self._prepare_collection_for_indexing()
        collection_namespace = self.namespace[0]

        # index creation
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'])
        try:
            query = index_gen.generate_index_create_query(bucket=collection_namespace, num_replica=1, desc=False)
            self.run_cbq_query(query=query)

            # querying docs for the idx index
            query = f'SELECT COUNT(*) FROM {collection_namespace} WHERE age > 65'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0, f"Actual: {result}, Expected: 0")
        except Exception as err:
            self.fail(str(err))
        finally:
            query = index_gen.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)

        try:
            replica_nodes = []
            for node in index_nodes:
                replica_nodes.append(f'{node.ip}:8091')
            query = index_gen.generate_index_create_query(bucket=collection_namespace, deploy_node_info=replica_nodes,
                                                          desc=True)
            self.run_cbq_query(query=query)

            # querying docs for the idx index
            query = f'SELECT COUNT(*) FROM {collection_namespace} WHERE age > 65'
            result = self.run_cbq_query(query=query)['results'][0]['$1']
            self.assertNotEqual(result, 0, f"Actual: {result}, Expected: 0")
        except Exception as err:
            self.fail(str(err))
        finally:
            query = index_gen.generate_index_drop_query(bucket=collection_namespace)
            self.run_cbq_query(query=query)

    def test_gsi_array_indexes(self):
        pass
