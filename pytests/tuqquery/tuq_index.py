import math

from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class QueriesViewsTests(QueryTests):
    def setUp(self):
        super(QueriesViewsTests, self).setUp()

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesViewsTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            self.query = "CREATE INDEX my_index ON %s(name) " % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.query = "DROP INDEX %s.my_index" % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])

    def test_primary_create_delete_index(self):
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            self.query = "CREATE INDEX my_index ON %s(name) " % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.test_case()
            self.query = "DROP INDEX %s.my_index" % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.test_case()