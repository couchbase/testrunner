import math
import time
import uuid
from tuq import QueryTests
from tuq import ExplainPlanHelper
from tuq_join import JoinTests
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
        self.log.info('-'*100)
        self.log.info('Temp fix for MB-16888')
        self.log.info('-'*100)

        self.shell.execute_command("killall -9 cbq-engine")
        self.shell.execute_command("killall -9 indexes")
        self.sleep(60, 'wait for indexer, cbq processes to come back up ..')
        self.log.info('-'*100)

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesViewsTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            created_indexes = []
            self.log.info('Temp fix for create index failures MB-16888')
            self.sleep(30, 'sleep before create indexes .. ')
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                                            view_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self._wait_for_index_online(bucket, view_name)
                    if self.index_type == 'VIEW':
                        self._verify_view_is_present(view_name, bucket)
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, view_name, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])

    def test_primary_create_delete_index(self):
        for bucket in self.buckets:
            self.query = "DROP PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])
            self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index_%s%s" % (bucket.name, ind)
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (view_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.test_case()
            except Exception, ex:
                if self.index_type == 'VIEW':
                    content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                      bucket="default", retry_time=1)
                    self.log.info("Generated view has %s items" % len(content['rows']))
                    raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, view_name, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.test_case()

    def test_create_same_name(self):
        for bucket in self.buckets:
            view_name = "tuq_index_%s%s" % (bucket.name, 'VMs')
            try:
                self.query = "CREATE INDEX %s ON %s(%s) USING VIEW" % (view_name, bucket.name, 'VMs')
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])
                self.query = "CREATE INDEX %s ON %s(%s) USING GSI" % (view_name, bucket.name, 'VMs')
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, view_name)
                self._verify_results(actual_result['results'], [])
            finally:
                try:
                    self.query = "DROP INDEX %s.%s USING VIEW" % (bucket.name, view_name)
                    self.run_cbq_query()
                    self.query = "DROP INDEX %s.%s USING GSI" % (bucket.name, view_name)
                    self.run_cbq_query()
                except:
                    pass

    def test_explain(self):
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT * FROM %s" % (bucket.name)
            res = self.run_cbq_query()
            self.log.info(res)
	    plan = ExplainPlanHelper(res)
            self.assertTrue(plan["~children"][0]["index"] == "#primary",
                            "Type should be #primary, but is: %s" % plan)

            self.query = "EXPLAIN SELECT * FROM %s LIMIT %s" %(bucket.name,10);
            res = self.run_cbq_query()
            self.assertTrue('limit' in str(res['results']),
                            "Limit is not pushed to primary scan")

    def test_explain_query_count(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s ' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_group_by(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s GROUP BY VMs' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_array(self):
        for bucket in self.buckets:
            index_name = "my_index_arr"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT ARRAY vm.memory FOR vm IN VMs END AS vm_memories FROM %s' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta(%s).type) USING %s" % (index_name, bucket.name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).type = "json"' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_index_with_fn(self):
        for bucket in self.buckets:
            index_name = "my_index_fn"
            try:
                self.query = "CREATE INDEX %s ON %s(round(test_rate)) USING %s" % (index_name, bucket.name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN select name, round(test_rate) as rate from %s WHERE round(test_rate) = 2' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()


    def test_explain_index_attr(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_attr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    self.query = "EXPLAIN SELECT * FROM %s WHERE %s = 'abc'" % (bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
                    created_indexes.append(index_name)
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_non_index_attr(self):
        for bucket in self.buckets:
            index_name = "my_non_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = "EXPLAIN SELECT * FROM %s WHERE email = 'abc'" % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] != index_name,
                                "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_index_count_gn(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT COUNT(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_aggr_gn(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT SUM(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_join(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_unnest(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(tasks_ids) USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT emp.name, task FROM %s emp UNNEST emp.tasks_ids task" % (bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_day) USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s use keys ['query-1'] where join_day>2) as names from %s" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE tasks_points > 0'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects_element(self):
        for bucket in self.buckets:
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE tasks_points.task1 > 0'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list_element(self):
        for bucket in self.buckets:
            index_name = "my_index_list_el"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                         ' FROM %s WHERE skills[0] = "abc"' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list(self):
        for bucket in self.buckets:
            index_name = "my_index_list"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) USING %s" % (index_name, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                         ' FROM %s WHERE skill[0] = "skill2010"' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_several_complex_objects(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_index_complex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'EXPLAIN SELECT DISTINCT %s as complex FROM %s WHERE %s = "abc"' % (self.FIELDS_TO_INDEX[ind - 1],
                                                                                                      bucket.name,
                                                                                                      self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_index_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
               self.query = "CREATE INDEX %s ON %s(" % (index_name, bucket.name) + \
               "meta(%s"%(bucket.name) + ").id) USING %s" % self.index_type
               # if self.gsi_type:
               #     self.query += " WITH {'index_type': 'memdb'}"
               self.run_cbq_query()
            except Exception, ex:
               self.assertTrue(str(ex).find("Error creating index") != -1,
                              "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")
    
    def test_index_dates(self):
        for bucket in self.buckets:
            index_name = "my_index_date"
            try:
                self.query = "CREATE INDEX %s ON %s(" % (index_name, bucket.name) + \
                "str_to_millis(tostr(join_yr) || '-0' || tostr(join_mo) || '-' || tostr(join_day))) "
                self.run_cbq_query()
            except Exception, ex:
                self.assertTrue(str(ex).find("Error creating index") != -1,
                                "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")

    def test_multiple_index_hints_explain_select(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo', 'join_day,join_mo']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % (ind_name))
                for ind in created_indexes:
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_index_hints_explain_aggr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['job_title', 'test_rate', 'job_title,test_rate']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % (ind_name))
                for ind in created_indexes:
                    self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s as employees USE INDEX(%s using %s)" % (bucket.name, ind, self.index_type) +\
                                 " WHERE job_title='Sales' GROUP BY join_mo " +\
                                 "HAVING SUM(employees.test_rate) > 0 and " +\
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
                    self.assertTrue(res["status"] == 'success')
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_index_hints_explain_same_attr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                fields = ['job_title', 'job_title,test_rate']
                for attr in fields:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (ind_name,
                                                                       bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % (ind_name))
                for ind in created_indexes:
                    self.query = "EXPLAIN SELECT join_mo, SUM(test_rate) as rate FROM %s  as employees USE INDEX(%s using %s)" % (bucket.name, ind, self.index_type) +\
                                 "WHERE job_title='Sales' GROUP BY join_mo " +\
                                 "HAVING SUM(employees.test_rate) > 0 and " +\
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_indexes_query_attr(self):
        index_name_prefix = 'auto_ind'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_day,join_mo']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s) " % (ind_name,
                                                                    bucket.name, attr)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append(ind_name)
                    self.query = 'SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (bucket.name)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name" : doc['name'], "join_mo" : doc['join_mo'], "join_day" : doc["join_day"]}
                                       for doc in full_list if doc['join_day'] > 2 and doc['join_mo'] > 3]
                    #import pdb;pdb.set_trace()
                    self.query = "select * from %s" % bucket.name
                    self.run_cbq_query()
                    self._verify_results(sorted(res['results']), sorted(expected_result))
                    #self.assertTrue(len(res['results'])==0)
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind_name,
                                    "Index should be %s, but is: %s" % (ind_name, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_indexes_query_non_ind_attr(self):
        index_name_prefix = 'auto_ind'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s%s' % (index_name_prefix, attr, str(uuid.uuid4())[:4])
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name,
                                                                bucket.name, attr)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name" : doc['name'], "join_yr" : doc['join_yr'], "join_day" : doc["join_day"]}
                                       for doc in full_list if doc['join_yr'] > 3]
                    #import pdb;pdb.set_trace()
                    self._verify_results(sorted(res['results']), sorted(expected_result))
                    #self.assertTrue(len(res['results'])==10)
                    self.query = 'EXPLAIN SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] != '%s_%s' % (index_name_prefix, attr),
                                    "Index should be %s_%s, but is: %s" % (index_name_prefix, attr, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_negative_indexes(self):
        queries_errors = {'create index gsi on default(name) using gsi': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_prepared_with_index_simple_where(self):
        index_name_prefix = 'auto_ind_prepared'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s' % (index_name_prefix, attr)
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name,
                                                                    bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append('%s_%s' % (index_name_prefix, attr))
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    self.prepared_common_body()
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_run_query(self):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_any')
        index_fields = self.input.param("index_field", '').split(';')
        index_name = "test"
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
            except:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
                raise
        try:
            self.query = "select * from system:indexes where name = %s" % (index_name)
            self.log.info(self.run_cbq_query())
            self.hint_index = indexes[0]
            fn = getattr(self, method_name)
            fn()
        finally:
            for bucket in self.buckets:
                for indx in indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_prepared_hints_letting(self):
        self.run_query_prepared("SELECT join_mo, sum_test from {0} USE INDEX ({1} using {2}) WHERE join_mo>7 group by join_mo letting sum_test = sum(tasks_points.task1)")

    def test_prepared_hints_array(self):
        self.run_query_prepared("SELECT job_title, array_append(array_agg(DISTINCT name), 'new_name') as names FROM {0} USE INDEX ({1} using {2}) GROUP BY job_title" )

    def test_prepared_hints_intersect(self):
        self.run_query_prepared("select name from {0} intersect all select name from {0} s USE INDEX ({1} using {2}) where s.join_day>5")

    def run_query_prepared(self, query):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        index_fields = self.input.param("index_field", '').split(';')
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
            except:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
                raise
        try:
            self.query = "select * from system:indexes where name = %s" % (index_name)
            self.log.info(self.run_cbq_query())
            for bucket in self.buckets:
                self.query = query.format(bucket.name, indexes[0], self.index_type)
                self.prepared_common_body()
        finally:
            for bucket in self.buckets:
                for indx in indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_intersect_scan(self):
        test_to_run = self.input.param("test_to_run", '')
        indexes = []
        try:
            indexes, query = self.run_intersect_scan_query(test_to_run)
            self.run_intersect_scan_explain_query(indexes, query)
        finally:
            if indexes:
                  for bucket in self.buckets:
                    for indx in indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_intersect_scan_meta(self):
        test_to_run = self.input.param("test_to_run", '')
        indexes = []
        try:
            indexes = []
            index_name_prefix = "inter_index_" + str(uuid.uuid4())[:4]
            index_fields = self.input.param("index_field", '').split(';')
            for bucket in self.buckets:
                for field in index_fields:
                    index_name = '%sid_meta' % (index_name_prefix)
                    query = "CREATE INDEX %s ON %s(meta(%s).id) USING %s" % (
                        index_name, bucket.name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                    index_name = '%stype_meta' % (index_name_prefix)
                    query = "CREATE INDEX %s ON %s(meta(%s).type) USING %s" % (
                        index_name, bucket.name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                self.test_comparition_meta()
            for bucket in self.buckets:
                self.query = "SELECT meta(%s).id, meta(%s).type FROM %s" % (bucket.name, bucket.name, bucket.name)
                self.run_cbq_query()
                query = 'EXPLAIN ' % (self.query % (bucket.name, bucket.name, bucket.name))
                res = self.run_cbq_query(query=query)
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == 'IntersectScan',
                                        "Index should be intersect scan and is %s" % (plan))
                actual_indexes = [scan['index'] for scan in plan["~children"][0]["~children"][0]['scans']]
                self.assertTrue(set(actual_indexes) == set(indexes),
                                "Indexes should be %s, but are: %s" % (indexes, actual_indexes))
        finally:
            for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def run_intersect_scan_query(self, query_method):
        indexes = []
        query = None
        index_name_prefix = "inter_index_" + str(uuid.uuid4())[:4]
        index_fields = self.input.param("index_field", '').split(';')
        try:
            for bucket in self.buckets:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                    index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                fn = getattr(self, query_method)
                query = fn()
        finally:
            return indexes, query

    def run_intersect_scan_explain_query(self, indexes_names, query_temp):
        for bucket in self.buckets:
            if (query_temp.find('%s') > 0):
                query_temp = query_temp % bucket.name
            query = 'EXPLAIN %s' % (query_temp)
            res = self.run_cbq_query(query=query)

	    plan = ExplainPlanHelper(res)
            print plan
            #import pdb;pdb.set_trace()
            self.log.info('-'*100)
            if (query.find("CREATE INDEX") < 0):
                result = plan["~children"][0]["~children"][0] if "~children" in plan["~children"][0] \
                        else plan["~children"][0]
                print result
                #import pdb;pdb.set_trace()
                if not(result['scans'][0]['#operator']=='DistinctScan'):
                    self.assertTrue(result["#operator"] == 'IntersectScan',
                                    "Index should be intersect scan and is %s" % (plan))
                    # actual_indexes = []
                    # for scan in result['scans']:
                    #     print scan
                    #     if (scan['#operator'] == 'IndexScan'):
                    #         actual_indexes.append([result['scans'][0]['index']])
                    #
                    #     elif (scan['#operator'] == 'DistinctScan'):
                    #         actual_indexes.append([result['scans'][0]['scan']['index']])
                    #     else:
                    #          actual_indexes.append(scan['index'])


                    actual_indexes = [scan['index'] if scan['#operator'] == 'IndexScan' else scan['scan']['index'] if scan['#operator'] == 'DistinctScan' else scan['index']
                            for scan in result['scans']]

                    print actual_indexes

                    actual_indexes = [x.encode('UTF8') for x in actual_indexes]

                    self.log.info('actual indexes "{0}"'.format(actual_indexes))
                    self.log.info('compared against "{0}"'.format(indexes_names))
                    self.assertTrue(set(actual_indexes) == set(indexes_names),"Indexes should be %s, but are: %s" % (indexes_names, actual_indexes))
            else:
                result = plan
                self.assertTrue(result['#operator'] == 'CreateIndex',
                                    "Operator is not create index and is %s" % (result))
            self.log.info('-'*100)

    def _delete_indexes(self, indexes):
        count = 0
        for bucket in self.buckets:
                query = "DROP INDEX %s.%s USING %s" % (bucket.name, indexes[count], self.index_type)
                count =count+1
                try:
                   self.run_cbq_query(query=query)
                except:
                   pass

    def _verify_view_is_present(self, view_name, bucket):
        if self.primary_indx_type.lower() == 'gsi':
            return
        ddoc, _ = RestConnection(self.master).get_ddoc(bucket.name, "ddl_%s" % view_name)
        self.assertTrue(view_name in ddoc["views"], "View %s wasn't created" % view_name)

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

class QueriesJoinViewsTests(JoinTests):


    def setUp(self):
        super(QueriesJoinViewsTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        self.index_type = self.input.param('index_type', 'VIEW')

    def suite_setUp(self):
        super(QueriesJoinViewsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesJoinViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesJoinViewsTests, self).suite_tearDown()

    def test_run_query(self):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_simple_join_keys')
        index_fields = self.input.param("index_field", '').split(';')
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                fn = getattr(self, method_name)
                fn()
            finally:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
