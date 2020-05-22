"""n1ql_collection_use_keys.py: Test cases for USE KEYS clause for Collection namespaces

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "13/05/20 12:30 pm"

"""
from random import randint

from collection.collections_rest_client import CollectionsRest
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.exception import CBQError

from .tuq import QueryTests


class QueryCollectionsUseKeys(QueryTests):
    def setUp(self):
        super(QueryCollectionsUseKeys, self).setUp()
        self.log.info("==============  QueryCollectionsUseKeys setup has started ==============")
        self.skip_load = True
        self.collection_bucket_name = 'default_1'
        self.bucket_params = self._create_bucket_params(server=self.master, size=100,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.collection_bucket_name, port=11222,
                                            bucket_params=self.bucket_params)
        self.scope_name = "my_scope"
        self.my_collection_name = "my_collection"

        self.col_rest = CollectionsRest(self.master)
        self.buckets = self.rest.get_buckets()

        result = self.col_rest.create_scope_collection(bucket=self.collection_bucket_name, scope=self.scope_name,
                                                       collection=self.my_collection_name)
        self.assertTrue(result, "Failed to create Scope and Collection")
        self.sleep(20)
        self.num_items = 1000
        self.gen_create = SDKDataLoader(num_ops=self.num_items, percent_create=100, percent_update=0,
                                        percent_delete=0, scope=self.scope_name, collection=self.my_collection_name)
        self._load_all_buckets(self.master, self.gen_create)

        if not self.query_context:
            self.col_namespace = f"default:{self.collection_bucket_name}.{self.scope_name}.{self.my_collection_name}"
        else:
            if '.' in self.query_context:
                _, bucket_space = self.query_context.split(':')
                bucket, scope = bucket_space.split('.')
                if bucket != self.collection_bucket_name:
                    self.fail("Bucket name in query_context is not matching with bucket available")
                if scope != self.scope_name:
                    self.fail("Scope name in query_context is not matching with bucket available")
                self.col_namespace = self.my_collection_name
            else:
                self.fail("Invalid query_context value")
        self.query_params = {"query_context": self.query_context}
        self.log.info("==============  QueryCollectionsUseKeys setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  QueryCollectionsUseKeys tearDown has started ==============")
        super(QueryCollectionsUseKeys, self).tearDown()
        self.log.info("==============  QueryCollectionsUseKeys tearDown has ended ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryCollectionsUseKeys suite_setup has started ==============")
        super(QueryCollectionsUseKeys, self).suite_setUp()
        self.log.info("==============  QueryCollectionsUseKeys suite_setup has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryCollectionsUseKeys suite_tearDown has started ==============")
        super(QueryCollectionsUseKeys, self).tearDown()
        self.log.info("==============  QueryCollectionsUseKeys suite_tearDown has ended ==============")

    def test_select_with_use_keys(self):
        doc_id = f"doc_{randint(1, 10)}"
        self.query = f'SELECT * FROM {self.col_namespace} USE KEYS "{doc_id}"'
        result = self.run_cbq_query(query_params=self.query_params)
        num_item_in_docs = 9
        self.assertEqual(len(result['results'][0][self.my_collection_name]), num_item_in_docs,
                         "result is not matching with expected value")

        doc_ids = ", ".join([f'"doc_{item}"' for item in range(self.num_items+1)])
        self.query = f'SELECT * FROM {self.col_namespace} USE KEYS [{doc_ids}]'
        result = self.run_cbq_query(query_params=self.query_params)
        self.assertEqual(len(result['results']), self.num_items, "Num of expected result not matching with actual")

    def test_update_with_use_keys(self):
        doc_id = f"doc_{randint(1, 10)}"
        updated_value = "updated_by_use_keys"
        self.query = f'UPDATE {self.col_namespace} USE KEYS "{doc_id}" SET modifier = "{updated_value}"'
        self.run_cbq_query(query_params=self.query_params)
        self.query = f'SELECT modifier FROM {self.col_namespace} USE KEYS "{doc_id}"'
        result = self.run_cbq_query()['results'][0]
        self.assertEqual(result['modifier'], updated_value, "Failed to update using USE KEYS")

        low = randint(10, 50)
        high = randint(100, 150)
        doc_ids = ", ".join([f'"doc_{item}"' for item in range(low, high)])
        self.query = f'UPDATE {self.col_namespace} USE KEYS [{doc_ids}] SET modifier = "{updated_value}"'
        self.run_cbq_query(query_params=self.query_params)
        self.query = f'SELECT COUNT(*) FROM {self.col_namespace} USE KEYS [{doc_ids}] WHERE modifier="{updated_value}"'
        result = self.run_cbq_query(query_params=self.query_params)['results'][0]['$1']
        self.assertEqual(result, high - low, "Failed")

    def test_delete_with_use_keys(self):
        doc_id = f"doc_{randint(1, 10)}"
        self.query = f'DELETE FROM {self.col_namespace} d USE KEYS "{doc_id}" RETURNING d'
        self.run_cbq_query(query_params=self.query_params)
        self.query = f'SELECT * FROM {self.col_namespace} USE KEYS "{doc_id}"'
        result = self.run_cbq_query(query_params=self.query_params)['results']
        self.assertFalse(result, "Failed to Delete doc using USE KEYS")

        low = randint(10, 50)
        high = randint(100, 150)
        doc_ids = ", ".join([f'"doc_{item}"' for item in range(low, high)])
        self.query = f'DELETE FROM {self.col_namespace} d USE KEYS [{doc_ids}] RETURNING d'
        self.run_cbq_query(query_params=self.query_params)
        self.query = f'SELECT * FROM {self.col_namespace} USE KEYS [{doc_ids}]'
        result = self.run_cbq_query(query_params=self.query_params)['results']
        self.assertFalse(result, "Failed to Delete docs using USE KEYS")

    def test_merge_with_use_keys(self):
        doc_id = f"doc_{randint(1, 10)}"
        try:
            self.query = f'MERGE INTO {self.col_namespace} b1 USE KEYS("{doc_id}") USING {self.col_namespace} b2 on ' \
                         f'b1.name == b2.name when not matched then insert (KEY "test",VALUE {{"name": "new"}})'
            self.run_cbq_query(query_params=self.query_params)
        except CBQError as e:
            self.assertTrue("Keyspace reference cannot have USE KEYS hint in MERGE statement." in str(e))

        low = randint(10, 50)
        high = randint(100, 150)
        doc_ids = ", ".join([f'"doc_{item}"' for item in range(low, high)])
        try:
            self.query = f'MERGE INTO {self.col_namespace} b1 USE KEYS([{doc_ids}]) USING {self.col_namespace} b2 ' \
                         f'on b1.name == b2.name when not matched then insert (KEY "test",VALUE {{"name": "new"}})'
            result = self.run_cbq_query(query_params=self.query_params)
            self.assertTrue(result is not None, "Failed")
        except CBQError as e:
            self.assertTrue("Keyspace reference cannot have USE KEYS hint in MERGE statement." in str(e))

    # def test_upsert_with_use_keys(self):
    #     doc_id = f"doc_{randint(1, 10)}"
    #     self.query = f'SELECT * FROM {self.col_namespace} USE KEYS "{doc_id}"'
    #     result = self.run_cbq_query(query_params=self.query_params)
    #     num_item_in_docs = 9
    #     self.assertEqual(len(result['results'][0][self.my_collection_name]), num_item_in_docs,
    #                      "result is not matching with expected value")
    #
    #     doc_ids = ", ".join([f'"doc_{item}"' for item in range(self.num_items+1)])
    #     self.query = f'SELECT * FROM {self.col_namespace} USE KEYS [{doc_ids}]'
    #     result = self.run_cbq_query(query_params=self.query_params)
    #     self.assertEqual(len(result['results']), self.num_items, "Num of expected result not matching with actual")

    def test_negative_scneario_with_use_keys(self):
        if self.query_context:
            self.skipTest("Can't run these tests with query context")
        # Using USE KEYS with non-existing scope
        doc_id = f"doc_{randint(1, 10)}"
        non_existing_namespace = self.col_namespace.replace(self.scope_name, 'default')
        try:
            self.query = f'SELECT * FROM {non_existing_namespace} USE KEYS "{doc_id}"'
            self.run_cbq_query()
        except CBQError as err:
            msg = "Scope not found in CB datastore default:default_1.default"
            self.assertTrue(msg in err._message, "Exception msg not matching")

        # Using USE KEYS with non-existing collection
        non_existing_namespace = self.col_namespace.replace(self.my_collection_name, 'default')
        try:
            self.query = f'SELECT * FROM {non_existing_namespace} USE KEYS "{doc_id}"'
            self.run_cbq_query()
        except CBQError as err:
            msg = "Keyspace not found in CB datastore: default:default_1.my_scope.default"
            self.assertTrue(msg in err._message, "Exception msg not matching")

        # Using USE KEYS for non-existing doc with SELECT/UPDATE/DELETE

        self.query = f'SELECT * FROM {self.col_namespace} USE KEYS "non_existing_doc"'
        result = self.run_cbq_query()['results']
        self.assertFalse(result, f"Unexpected results found for SELECT: {result}")

        self.query = f'UPDATE {self.col_namespace} USE KEYS "non_existing_doc" SET modifier = "updated"'
        result = self.run_cbq_query()['results']
        self.assertFalse(result, f"Unexpected results found for UPDATE {result}")

        self.query = f'DELETE FROM {self.col_namespace} d USE KEYS "non_existing_doc" RETURNING d'
        result = self.run_cbq_query()['results']
        self.assertFalse(result, f"Unexpected results found for DELETE: {result}")
