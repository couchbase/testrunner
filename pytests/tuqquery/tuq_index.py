import math

from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class QueriesViewsTests(QueryTests):

    FIELDS_TO_INDEX = ['name', 'job_title', 'join_yr']
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']

    def setUp(self):
        super(QueriesViewsTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesViewsTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (
                                            view_name, bucket.name, self.FIELDS_TO_INDEX[ind-1])
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['resultset'], [])
                    created_indexes.append(view_name)
                    self._verify_view_is_present(view_name, bucket)
                    self.assertTrue(self._is_index_in_list(bucket, view_name), "Index is not in list")
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['resultset'], [])
                    self.assertFalse(self._is_index_in_list(bucket, view_name), "Index is in list")

    def test_primary_create_delete_index(self):
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % (bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])
            self.assertTrue(self._is_index_in_list(bucket, "#primary"), "Index is not in list")

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (view_name, bucket.name, self.FIELDS_TO_INDEX[ind-1])
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['resultset'], [])
                    created_indexes.append(view_name)
                    self.test_case()
            except Exception, ex:
                content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                  bucket="default", retry_time=1)
                self.log.info("Generated view has %s items" % len(content['rows']))
                raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name,view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['resultset'], [])
                self.test_case()

    def test_explain(self):
        for bucket in self.buckets:
            try:
                self.query = "CREATE PRIMARY INDEX ON %s " % (bucket.name)
                self.run_cbq_query()
            except CBQError as ex:
                if str(ex).find("Primary index already exists") == -1:
                    raise ex
            self.query = "EXPLAIN SELECT * FROM %s" % (bucket.name)
            res = self.run_cbq_query()
            self.assertTrue(res["resultset"][0]["input"]["type"] == "fetch",
                            "Type should be fetch, but is: %s" % res["resultset"])
            self.assertTrue(res["resultset"][0]["input"]["input"]["type"] == "scan",
                            "Type should be scan, but is: %s" % res["resultset"])
            self.assertTrue(res["resultset"][0]["input"]["input"]["index"] == "#primary",
                            "Type should be #alldocs, but is: %s" % res["resultset"])

    def test_explain_index_attr(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_attr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind-1])
                    self.run_cbq_query()
                    self.query = "EXPLAIN SELECT * FROM %s WHERE %s = 'abc'" % (bucket.name, self.FIELDS_TO_INDEX[ind-1])
                    res = self.run_cbq_query()
                    created_indexes.append(index_name)
                    self.assertTrue(res["resultset"][0]["input"]["type"] == "filter",
                                    "Type should be fetch, but is: %s" % res["resultset"])
                    self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["type"] == "scan",
                                    "Type should be scan, but is: %s" % res["resultset"])
                    self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()

    def test_explain_non_index_attr(self):
        for bucket in self.buckets:
            index_name = "my_non_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = "EXPLAIN SELECT * FROM %s WHERE email = 'abc'" % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["type"] == "filter",
                                "Type should be fetch, but is: %s" % res["resultset"])
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["type"] == "scan",
                                "Type should be scan, but is: %s" % res["resultset"])
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] != index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_index_aggr_gn(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind-1])
                    self.run_cbq_query()
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT COUNT(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind-1], bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % (bucket.name) +\
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) +\
                             'WHERE join_mo>7 and task_points > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_childs_objects_element(self):
        for bucket in self.buckets:
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) +\
                             'WHERE join_mo>7 and  task_points.task1 > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_childs_list_element(self):
        for bucket in self.buckets:
            index_name = "my_index_list_el"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' +\
                         ' FROM %s WHERE skills[0] = "abc"' % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_childs_list(self):
        for bucket in self.buckets:
            index_name = "my_index_list"
            try:
                self.query = "CREATE INDEX %s ON %s(skills) " % (index_name, bucket.name)
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' +\
                         ' FROM %s WHERE skill[0] = "skill2010"' % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["input"]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_several_complex_objects(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_index_complex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind-1])
                    self.run_cbq_query()
                    created_indexes.append(index_name)
                    self.query = 'EXPLAIN SELECT DISTINCT %s as complex FROM %s WHERE %s = "abc"' % (self.FIELDS_TO_INDEX[ind-1],
                                                                                                      bucket.name,
                                                                                                      self.FIELDS_TO_INDEX[ind-1])
                    res = self.run_cbq_query()
                    self.assertTrue(res["resultset"][0]["input"]["input"]["input"]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name,res["resultset"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()

    def _verify_view_is_present(self, view_name, bucket):
        ddoc, _ = RestConnection(self.master).get_ddoc(bucket.name, "ddl_%s" % view_name)
        self.assertTrue(view_name in ddoc["views"], "View %s wasn't created" % view_name)

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM :system.indexes"
        res = self.run_cbq_query(query)
        for item in res['resultset']:
            if item['bucket_id'] == bucket.name and item['name'] == index_name:
                return True
        return False