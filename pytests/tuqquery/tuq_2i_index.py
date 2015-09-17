import math

from tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class QueriesIndexTests(QueryTests):

    FIELDS_TO_INDEX = [('name', 'job_title'), ('name', 'join_yr'), ('VMs', 'name')]
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']

    def setUp(self):
        super(QueriesIndexTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')

    def suite_setUp(self):
        super(QueriesIndexTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesIndexTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesIndexTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING GSI" % (
                                            view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.assertTrue(self._is_index_in_list(bucket, view_name), "Index is not in list")
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, view_name), "Index is in list")

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s)  USING GSI" % (view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                    actual_result = self.run_cbq_query()
                    self.assertTrue(len(actual_result['results']), self.num_items)
            except Exception, ex:
                content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                  bucket="default", retry_time=1)
                self.log.info("Generated view has %s items" % len(content['rows']))
                raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                actual_result = self.run_cbq_query(query_params={'scan_consistency' : 'statement_plus'})
                self.assertTrue(len(actual_result['results']), self.num_items)

    def test_explain_query_count(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs, name)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s where VMs is not null union SELECT count(name) FROM %s where name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]['~children'][0]["children"][0]['~children'][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_group_by(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs, join_yr)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(*) FROM %s GROUP BY VMs, join_yr' % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta(%s).type, name)  USING GSI" % (index_name, bucket.name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).type = "json" and name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_explain_index_join(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, join_day)  USING GSI" % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT name, join_day FROM %s where name = 'employee-9'"% (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index()
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task USE KEYS ['key1']" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()


    def test_explain_index_subquery(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_day, name)  USING GSI" % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s use keys ['query-1'] where join_day>2 and name =='abc') as names from %s" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child2"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs[0].RAM, VMS[1].RAM)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points, join_mo)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE join_mo>7 and task_points > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects_element(self):
        for bucket in self.buckets:
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1, join_mo)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE join_mo>7 and  task_points.task1 > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM system:indexes"
        res = self.run_cbq_query(query)
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                self.log.error(item)
                continue
            if item['indexes']['keyspace_id'] == bucket.name and item['indexes']['name'] == index_name:
                return True
        return False