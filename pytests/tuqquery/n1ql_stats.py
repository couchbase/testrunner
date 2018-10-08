import math
from membase.api.rest_client import RestConnection
from tuqquery.tuq import QueryTests


class StatsTests(QueryTests):
    def setUp(self):
        super(StatsTests, self).setUp()
        server = self.master
        if self.input.tuq_client and "client" in self.input.tuq_client:
            server = self.tuq_client
        self.rest = RestConnection(server)

    def suite_setUp(self):
        super(StatsTests, self).suite_setUp()

    def tearDown(self):
        super(StatsTests, self).tearDown()

    def suite_tearDown(self):
        super(StatsTests, self).suite_tearDown()

    def test_cmd_line(self):
        stats = self.rest.query_tool_stats()
        self.assertTrue(stats['cmdline'][0].find('cbq-engine') != -1, 'command line is incorrect')
        self.log.info('cmd line is checked')

    def test_requests_select(self):
        stats = self.rest.query_tool_stats()
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END LIKE 'win%'"
            self.run_cbq_query()
        new_stats = self.rest.query_tool_stats()
        self.assertTrue(new_stats['requests.count'] == stats['requests.count']+len(self.buckets), 'Request were not increased')
        self.assertTrue(new_stats['selects.count'] == stats['selects.count']+len(self.buckets), 'Selects count were not increased')
        self.log.info('select count is checked')

    def test_errors(self):
        stats = self.rest.query_tool_stats()
        for bucket in self.buckets:
            self.query = "SELECT ALL FROM %s" % (bucket.name)
            try:
                self.run_cbq_query()
            except:
                pass
        new_stats = self.rest.query_tool_stats()
        self.assertTrue(new_stats['requests.count'] == stats['requests.count']+len(self.buckets), 'Request were not increased')
        self.assertTrue(new_stats['errors.count'] == stats['errors.count']+len(self.buckets), 'Selects count were not increased')
        self.log.info('errors count is checked')

    def test_requests_insert(self):
        stats = self.rest.query_tool_stats()
        for bucket in self.buckets:
            self.query = 'INSERT into %s key "%s" VALUES %s' % (bucket.name, 'key', 'value')
            self.run_cbq_query()
        new_stats = self.rest.query_tool_stats()
        self.assertTrue(new_stats['requests.count'] == stats['requests.count']+len(self.buckets), 'Request were not increased')
        self.assertTrue(new_stats['inserts.count'] == stats['inserts.count']+len(self.buckets), 'Inserts count were not increased')
        self.log.info('insert count is checked')

    def test_requests_update(self):
        stats = self.rest.query_tool_stats()
        for bucket in self.buckets:
            self.query = 'INSERT into %s key "%s" VALUES %s' % (bucket.name, 'key', 'value')
            self.query = "update %s use keys ['%s'] set name='new'" % (bucket.name, 'key')
            self.run_cbq_query()
        new_stats = self.rest.query_tool_stats()
        self.assertTrue(new_stats['requests.count'] == stats['requests.count']+len(self.buckets), 'Request were not increased')
        self.assertTrue(new_stats['updates.count'] == stats['updates.count']+len(self.buckets), 'Updates count were not increased')
        self.log.info('update count is checked')

    def test_requests_delete(self):
        stats = self.rest.query_tool_stats()
        for bucket in self.buckets:
            self.query = 'INSERT into %s key "%s" VALUES %s' % (bucket.name, 'key', 'value')
            self.query = "delete from %s use keys ['%s']" % (bucket.name, 'key')
            self.run_cbq_query()
        new_stats = self.rest.query_tool_stats()
        self.assertTrue(new_stats['requests.count'] == stats['requests.count']+len(self.buckets), 'Request were not increased')
        self.assertTrue(new_stats['deletes.count'] == stats['deletes.count']+len(self.buckets), 'Deletes count were not increased')
        self.log.info('delete count is checked')

    def test_audit_requests_total(self):
        stats = self.rest.query_tool_stats()
        self.assertTrue(stats['audit_requests_total.count'] >=0, 'Audit requests total is unavailable')

    def test_audit_requests_filtered(self):
        stats = self.rest.query_tool_stats()
        self.assertTrue(stats['audit_requests_filtered.count'] >=0, 'Audit requests filtered is unavailable')

    def test_audit_actions(self):
        stats = self.rest.query_tool_stats()
        self.assertTrue(stats['audit_actions.count'] >=0, 'Audit actions is unavailable')

    def test_audit_actions_failed(self):
        stats = self.rest.query_tool_stats()
        self.assertTrue(stats['audit_actions_failed.count'] >=0, 'Audit actions failed is unavailable')

