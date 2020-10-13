
from .tuq import QueryTests
import time

class QueryContextTests(QueryTests):

    def setUp(self):
        super(QueryContextTests, self).setUp()
        self.log.info("==============  QuerySanityTests setup has started ==============")
        self.special_scope = self.input.param('special_scope', '')
        self.special_collection = self.input.param('special_collection', '')
        if self.bucket_name != "default" and self.bucket_name != "bucket0" and self.bucket_name != "default:default.test.test1":
            self.rest.create_bucket(bucket=self.bucket_name, ramQuotaMB=100)
            time.sleep(10)
            self.query_bucket = self.bucket_name
        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryContextTests, self).suite_setUp()
        self.log.info("==============  QuerySanityTests suite_setup has started ==============")
        if self.load_collections:
            self.run_cbq_query(query='CREATE INDEX idx on default(name)')
            self.sleep(5)
            self.wait_for_all_indexes_online()
            self.collections_helper.create_scope(bucket_name="default", scope_name="test2")
            self.collections_helper.create_collection(bucket_name="default", scope_name="test2",
                                                      collection_name=self.collections[0])
            self.collections_helper.create_collection(bucket_name="default", scope_name="test2",
                                                      collection_name=self.collections[1])
            self.run_cbq_query(
                query="CREATE INDEX idx1 on default:default.test2.{0}(name)".format(self.collections[0]))
            self.run_cbq_query(
                query="CREATE INDEX idx2 on default:default.test2.{0}(name)".format(self.collections[1]))
            self.sleep(5)
            self.wait_for_all_indexes_online()
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[
                    1]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[1]) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[1]) + '(KEY, VALUE) VALUES ("key3", { "type" : "hotel", "name" : "new hotel" })'))
            time.sleep(20)
        self.log.info("==============  QuerySanityTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QuerySanityTests tearDown has started ==============")
        self.log.info("==============  QuerySanityTests tearDown has completed ==============")
        super(QueryContextTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QuerySanityTests suite_tearDown has started ==============")
        self.log.info("==============  QuerySanityTests suite_tearDown has completed ==============")
        super(QueryContextTests, self).suite_tearDown()

    def test_context_bucket_scope(self):
        results = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context='default.test')
        self.assertEqual(results['results'][0],{'test1': {'name': 'new hotel', 'type': 'hotel'}})

    def test_context_name_bucket_scope(self):
        results = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context='default:default.test')
        self.assertEqual(results['results'][0],{'test1': {'name': 'new hotel', 'type': 'hotel'}})

    def test_context_namespace(self):
        results = self.run_cbq_query(query='select * from default.test.test1 where name = "new hotel"', query_context='default:')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})

    def test_context_semicolon_bucket_scope(self):
        results = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context=':default.test')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})

    def test_default(self):
        results = self.run_cbq_query(query='select * from default:default where name = "employee-9"')
        self.assertEqual(results['metrics']['resultCount'], 72)

    def test_default_full_path(self):
        results = self.run_cbq_query(query='select * from default:default.test.test1 where name = "new hotel"')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})

    def test_context_join(self):
        results = self.run_cbq_query(query='select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"', query_context='default:default.test2')
        self.assertEqual(results['results'][0], {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}}, {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}})
        results2 = self.run_cbq_query(query='select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"', query_context='default:default.test')
        self.assertEqual(results2['results'][0], {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}})

    def test_ignore_context(self):
        results = self.run_cbq_query(query='select * from default:default.test.test1 where name = "old hotel"', query_context='default:default.test2')
        self.assertEqual(results['metrics']['resultCount'], 3)

    def test_collection_names(self):
        results = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context='default:default.test')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})
        results = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context='default:default.test2')
        self.assertEqual(results['results'], [])

    def test_scope_collection_identical(self):
        self.run_cbq_query(query="CREATE scope default:default.test3")
        time.sleep(10)
        self.run_cbq_query(
            query="CREATE COLLECTION default:default.test3.test3")
        time.sleep(10)
        self.run_cbq_query(
            query="CREATE PRIMARY INDEX ON default:default.test3.test3")
        time.sleep(5)
        self.wait_for_all_indexes_online()
        results = self.run_cbq_query(query='select * from test3 where name = "new hotel"', query_context='default:default.test3')
        self.assertEqual(results['results'], [])

    def test_invalid_context(self):
        try:
            results = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context='default:default.`test^`')
            self.fail()
        except Exception as e:
            self.assertTrue('Scope not found in CB datastore default:default.test^' in str(e))
        try:
            results2 = self.run_cbq_query(query='select * from test1 where name = "new hotel"', query_context='fakevalue')
            self.fail()
        except Exception as e:
            self.assertTrue('Keyspace not found in CB datastore: fakevalue:test1 - cause: No bucket named test1' in str(e) or 'Invalid path specified: path has wrong number of parts' in str(e))

    def test_special_characters(self):
        try:
            results = self.run_cbq_query(query='CREATE scope default:`{0}`.`{1}`'.format(self.bucket_name, self.special_scope))
            time.sleep(20)

            results = self.run_cbq_query(query='CREATE COLLECTION default:`{0}`.`{1}`.`{2}`'.format(self.bucket_name, self.special_scope, self.special_collection))
            time.sleep(20)

            self.run_cbq_query(query='CREATE PRIMARY INDEX ON default:`{0}`.`{1}`.`{2}`'.format(self.bucket_name, self.special_scope, self.special_collection), query_context='default:`{0}`.`{1}`'.format(self.bucket_name, self.special_scope))
            time.sleep(5)
            self.wait_for_all_indexes_online()
            self.run_cbq_query(
                query=('INSERT INTO default:`{0}`.`{1}`.`{2}`'.format(self.bucket_name,self.special_scope,self.special_collection) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
            time.sleep(10)
            results = self.run_cbq_query(query='select * from `{0}` where name = "old hotel"'.format(self.special_collection), query_context='default:`{0}`.`{1}`'.format(self.bucket_name, self.special_scope))
            self.assertEquals(results['results'][0], {"{0}".format(self.special_collection): {"name": "old hotel","type": "hotel"}})

        except Exception as e:
            self.log.error("Scope/Collection unable to be created with valid special characters: {0}".format(str(e)))
            self.fail()
