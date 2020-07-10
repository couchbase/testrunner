
from .tuq import QueryTests
import time

class QueryContextTests(QueryTests):

    def setUp(self):
        super(QueryContextTests, self).setUp()
        self.log.info("==============  QuerySanityTests setup has started ==============")
        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryContextTests, self).suite_setUp()
        self.log.info("==============  QuerySanityTests suite_setup has started ==============")
        self.run_cbq_query(query="CREATE scope default:default.test")
        time.sleep(20)
        self.run_cbq_query(query="CREATE COLLECTION default:default.test.test1")
        self.run_cbq_query(query="CREATE COLLECTION default:default.test.test2")
        time.sleep(20)
        self.run_cbq_query(query="CREATE INDEX idx1 on default:default.test.test1(name)")
        self.run_cbq_query(query="CREATE INDEX idx2 on default:default.test.test2(name)")
        self.run_cbq_query(query="CREATE INDEX idx3 on default:default.test.test1(nested)")
        self.run_cbq_query(query="CREATE INDEX idx4 on default:default.test.test1(ALL numbers)")

        time.sleep(10)

        self.run_cbq_query(query='INSERT INTO default:default.test.test1 (KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })')
        self.run_cbq_query(query='INSERT INTO default:default.test.test1 (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })')
        self.run_cbq_query(query='INSERT INTO default:default.test.test2 (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" })')
        self.run_cbq_query(query='INSERT INTO default:default.test.test1 (KEY, VALUE) VALUES ("key3", { "nested" : {"fields": "fake"}, "name" : "old hotel" })')
        self.run_cbq_query(query='INSERT INTO default:default.test.test1 (KEY, VALUE) VALUES ("key4", { "numbers": [1,2,3,4] , "name" : "old hotel" })')
        time.sleep(10)

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

