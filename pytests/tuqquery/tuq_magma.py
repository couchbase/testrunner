import time
from deepdiff import DeepDiff
import uuid
from .tuq import QueryTests
from collection.collections_stats import CollectionsStats
from couchbase_helper.cluster import Cluster
from lib.couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection



class QueryMagmaTests(QueryTests):
    def setUp(self):
        super(QueryMagmaTests, self).setUp()
        self.log.info("==============  QueryMagmaTests setup has started ==============")
        self.bucket_name = self.input.param("bucket", self.default_bucket_name)
        self.active_resident_threshold = self.input.param("resident_ratio", 100)
        self.num_items = self.input.param("num_items", 10000)
        self.expiry = self.input.param("expiry", 0)
        self.conn = RestConnection(self.master)
        self.stat = CollectionsStats(self.master)
        self.cluster = Cluster()
        self.log.info("==============  QueryMagmaTests setup has completed ==============")

    def suite_setUp(self):
        super(QueryMagmaTests, self).suite_setUp()
        self.log.info("==============  QueryMagmaTests suite_setup has started ==============")
        self.log.info("==============  QueryMagmaTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryMagmaTests tearDown has started ==============")
        self.log.info("==============  QueryMagmaTests tearDown has completed ==============")
        super(QueryMagmaTests, self).tearDown()


    def suite_tearDown(self):
        self.log.info("==============  QueryMagmaTests suite_tearDown has started ==============")
        self.log.info("==============  QueryMagmaTests suite_tearDown has completed ==============")
        super(QueryMagmaTests, self).suite_tearDown()

    ##############################################################################################
    #
    #   DML
    ##############################################################################################

    def test_insert_with_select(self):
        self.load_data()
        num_docs = self.input.param('num_docs', 10)
        keys, values = self._insert_gen_keys(num_docs, prefix="select_i")
        prefix = 'insert%s' % str(uuid.uuid4())[:5]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            for i in range(num_docs):
                self.query = 'insert into %s (key "%s_%s", value {"name": name}) select name from %s use keys ["%s"]' % (
                query_bucket, prefix, str(i), query_bucket, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys [%s]' % (
            query_bucket, bucket.name, ','.join(['"%s_%s"' % (prefix, i) for i in range(num_docs)]))
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: {'name': doc['name']}} for doc in values[:num_docs]]
            # print("--> expected_result:{}".format(expected_result))
            # print("--> actual_result:{}".format(actual_result))
            expected_result = sorted(expected_result, key=(lambda x: x[bucket.name]['name']))
            actual_result = sorted(actual_result['results'], key=(lambda x: x[bucket.name]['name']))
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result, 'Item did not appear')
        self.conn.delete_all_buckets()


    def test_insert_returning_elements(self):
        self.load_data()
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            diffs = DeepDiff(actual_result['results'],  expected_item_values, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        self.conn.delete_all_buckets()

    def test_update_where(self):
        self.load_data()
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where' + str(uuid.uuid4())[:4])
        updated_value = 'new_name'
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day=1 returning name' % (query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')
        self.conn.delete_all_buckets()

    def test_delete_where_clause_json(self):
        self.load_data()
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["job_title"] == 'Engineer']
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s where job_title="Engineer"' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)
        self.conn.delete_all_buckets()

    def test_merge_update_match_set(self):
        self.load_data(num_extra_buckets=1)
        self.run_cbq_query("CREATE INDEX idx1 on default(name)")
        self.run_cbq_query("CREATE INDEX idx2 on bucket0(name)")
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into bucket0 (key , value) VALUES ("%s", %s)' % (key, value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        value = '{"name": "new1"}'
        self.query = 'INSERT into default (key , value) VALUES ("%s", %s)' % ("new1", value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        new_name = 'edit'
        self.query = 'MERGE INTO default b1 USING bucket0 b2 on b1.name = b2.name when matched then update set b1.name="%s"' % (new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT * FROM default where name = "{0}"'.format(new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')
        self.conn.delete_all_buckets()

    def test_merge_delete_match(self):
        self.load_data(num_extra_buckets=1)
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        self.query = 'MERGE INTO default USING bucket0 sd on key meta().id when matched then delete where meta(sd).id = "%s"' % (keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')
        self.conn.delete_all_buckets()

    ##############################################################################################
    #
    #   use keys
    ##############################################################################################

    def test_delete_use_keys(self):
        self.load_data()
        keys = "'doc_10000','doc_10100','doc_10200','doc_10300','doc_10400','doc_10500','doc_10600','doc_10700','doc_10800','doc_10900','doc_11000'"
        delete_result = self.run_cbq_query("DELETE FROM default USE KEYS [{0}]".format(keys))
        self.assertEqual(delete_result['status'], 'success', 'Query was not run successfully')
        ensure_deleted = self.run_cbq_query("SELECT * from default USE KEYS [{0}]".format(keys))
        self.assertEqual(ensure_deleted['metrics']['resultCount'], 0)
        self.conn.delete_all_buckets()

    def test_update_use_keys(self):
        self.load_data()
        keys = "'doc_10000','doc_10100','doc_10200','doc_10300','doc_10400','doc_10500','doc_10600','doc_10700','doc_10800','doc_10900','doc_11000'"
        delete_result = self.run_cbq_query("UPDATE default USE KEYS [{0}] SET new_field = 'updated'".format(keys))
        self.assertEqual(delete_result['status'], 'success', 'Query was not run successfully')
        ensure_deleted = self.run_cbq_query("SELECT new_field from default where new_field = 'updated'")
        self.assertEqual(ensure_deleted['metrics']['resultCount'], 11)
        self.conn.delete_all_buckets()


    def test_raw_limit(self):
        self.load_data()
        for query_bucket in self.query_buckets:
            self.query = "select raw firstName from %s order by firstName limit 5" % query_bucket
            actual_list = self.run_cbq_query()
            expected_results = ['Aaron', 'Aaron', 'Aaron', 'Aaron', 'Aaron']
            diffs = DeepDiff(actual_list['results'], expected_results, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        self.conn.delete_all_buckets()

    def load_data(self, num_extra_buckets=0):
        self.conn.delete_all_buckets()
        time.sleep(5)
        self.conn.create_bucket(bucket="default", ramQuotaMB=256, proxyPort=11220, storageBackend="magma")
        for i in range(0, num_extra_buckets):
            self.conn.create_bucket(bucket="bucket{0}".format(i), ramQuotaMB=256, proxyPort=11220, storageBackend="magma")
            self.run_cbq_query("CREATE PRIMARY INDEX on bucket{0}".format(i))
        self.run_cbq_query("CREATE PRIMARY INDEX on default")
        self.buckets = self.conn.get_buckets()
        self.query_buckets = self.buckets
        self.gen_create = SDKDataLoader(num_ops=self.num_items)
        for bucket in self.buckets:
            self.cluster.async_load_gen_docs_till_dgm(server=self.master,
                                                  active_resident_threshold=self.active_resident_threshold,
                                                  bucket=bucket,
                                                  scope=None, collection=None,
                                                  exp=self.expiry,
                                                  value_size=self.value_size, timeout_mins=5,
                                                  java_sdk_client=self.java_sdk_client)
        for bkt in self.buckets:
            print(self.stat.get_collection_stats(bkt))

