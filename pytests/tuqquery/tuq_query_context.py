
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

