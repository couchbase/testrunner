import math
import time
import uuid
from tuq import QueryTests
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
                                            view_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self._wait_for_index_online(bucket, view_name)
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
                    self.query = "CREATE INDEX %s ON %s(%s) " % (view_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.test_case()
            except Exception, ex:
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

    def test_explain(self):
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT * FROM %s" % (bucket.name)
            res = self.run_cbq_query()
            self.log.info(res)
            self.assertTrue(res["results"][0]["~children"][0]["index"] == "#primary",
                            "Type should be #primary, but is: %s" % res["results"])

    def test_explain_query_count(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s ' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_group_by(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s GROUP BY VMs' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_array(self):
        for bucket in self.buckets:
            index_name = "my_index_arr"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT ARRAY vm.memory FOR vm IN VMs END AS vm_memories FROM %s' % (bucket.name)
                res = self.run_cbq_query()
                self.log.info(res)
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_query_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta(%s).type) " % (index_name, bucket.name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).type = "json"' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_index_with_fn(self):
        for bucket in self.buckets:
            index_name = "my_index_fn"
            try:
                self.query = "CREATE INDEX %s ON %s(round(test_rate)) " % (index_name, bucket.name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN select name, round(test_rate) as rate from %s WHERE round(test_rate) = 2' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()


    def test_explain_index_attr(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_attr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    self.query = "EXPLAIN SELECT * FROM %s WHERE %s = 'abc'" % (bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
                    created_indexes.append(index_name)
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_non_index_attr(self):
        for bucket in self.buckets:
            index_name = "my_non_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = "EXPLAIN SELECT * FROM %s WHERE email = 'abc'" % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] != index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_index_count_gn(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT COUNT(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
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
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT SUM(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
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
                    self.query = "CREATE INDEX %s ON %s(name) " % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
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
                    self.query = "CREATE INDEX %s ON %s(tasks_ids) " % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT emp.name, task FROM %s emp UNNEST emp.tasks_ids task" % (bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
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
                    self.query = "CREATE INDEX %s ON %s(join_day) " % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s use keys ['query-1'] where join_day>2) as names from %s" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE tasks_points > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects_element(self):
        for bucket in self.buckets:
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE tasks_points.task1 > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list_element(self):
        for bucket in self.buckets:
            index_name = "my_index_list_el"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                         ' FROM %s WHERE skills[0] = "abc"' % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list(self):
        for bucket in self.buckets:
            index_name = "my_index_list"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) " % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                         ' FROM %s WHERE skill[0] = "skill2010"' % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_explain_several_complex_objects(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "my_index_complex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name, bucket.name, self.FIELDS_TO_INDEX[ind - 1])
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'EXPLAIN SELECT DISTINCT %s as complex FROM %s WHERE %s = "abc"' % (self.FIELDS_TO_INDEX[ind - 1],
                                                                                                      bucket.name,
                                                                                                      self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_index_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
               self.query = "CREATE INDEX %s ON %s(" % (index_name, bucket.name) + \
               "meta(%s"%(bucket.name) + ").id)"
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
                "str_to_millis(tostr(join_yr) || '-0' || tostr(join_mo) || '-0' || tostr(join_day))) "
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
                    ind_name = attr.split('.')[0].split('[')[0].replace(',', '_')
                    self.query = "CREATE INDEX %s_%s ON %s(%s)  USING %s" % (index_name_prefix, ind_name,
                                                                    bucket.name, attr, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s_%s' % (index_name_prefix, ind_name))
                for ind in created_indexes:
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_multiple_index_hints_explain_aggr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['job_title', 'test_rate', 'job_title,test_rate']:
                    ind_name = attr.split('.')[0].split('[')[0].replace(',', '_')
                    self.query = "CREATE INDEX %s_%s ON %s(%s)  USING %s" % (index_name_prefix, ind_name,
                                                                    bucket.name, attr, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s_%s' % (index_name_prefix,ind_name))
                for ind in created_indexes:
                    self.query = "EXPLAIN SELECT join_mo, SUM(test_rate) as rate FROM %s as employees USE INDEX(%s using %s)" % (bucket.name, ind, self.index_type) +\
                                 " WHERE job_title='Sales' GROUP BY join_mo " +\
                                 "HAVING SUM(employees.test_rate) > 0 and " +\
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_multiple_index_hints_explain_same_attr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        for bucket in self.buckets:
            created_indexes = []
            try:
                fields = ['job_title', 'job_title,test_rate']
                for attr in fields:
                    ind_name = attr.split('.')[0].split('[')[0].replace(',', '_')
                    self.query = "CREATE INDEX %s_%s ON %s(%s) USING %s" % (index_name_prefix, ind_name,
                                                                       bucket.name, attr, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s_%s' % (index_name_prefix, ind_name))
                for ind in created_indexes:
                    self.query = "EXPLAIN SELECT join_mo, SUM(test_rate) as rate FROM %s  as employees USE INDEX(%s using %s)" % (bucket.name, ind, self.index_type) +\
                                 "WHERE job_title='Sales' GROUP BY join_mo " +\
                                 "HAVING SUM(employees.test_rate) > 0 and " +\
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_multiple_indexes_query_attr(self):
        index_name_prefix = 'auto_ind'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_day,join_mo']:
                    self.query = "CREATE INDEX %s_%s ON %s(%s) " % (index_name_prefix, attr,
                                                                    bucket.name, attr)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, '%s_%s' % (index_name_prefix, attr))
                    created_indexes.append('%s_%s' % (index_name_prefix, attr if attr.find(',') == -1 else attr.split(',')[1]))
                    self.query = 'SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (bucket.name)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name" : doc['name'], "join_mo" : doc['join_mo'], "join_day" : doc["join_day"]}
                                       for doc in full_list if doc['join_day'] > 2 and doc['join_mo'] > 3]
                    self._verify_results(sorted(res['results']), sorted(expected_result))
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == '%s_%s' % (index_name_prefix, attr),
                                    "Index should be %s_%s, but is: %s" % (index_name_prefix, attr, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_multiple_indexes_query_non_ind_attr(self):
        index_name_prefix = 'auto_ind'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s%s' % (index_name_prefix, attr, str(uuid.uuid4())[:4])
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name,
                                                                bucket.name, attr)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name" : doc['name'], "join_yr" : doc['join_yr'], "join_day" : doc["join_day"]}
                                       for doc in full_list if doc['join_yr'] > 3]
                    self._verify_results(sorted(res['results']), sorted(expected_result))
                    self.query = 'EXPLAIN SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] != '%s_%s' % (index_name_prefix, attr),
                                    "Index should be %s_%s, but is: %s" % (index_name_prefix, attr, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_negative_indexes(self):
        queries_errors = {'create index gsi on default(name) using gsi': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_prepared_with_index_simple_where(self):
        index_name_prefix = 'auto_ind_prepared'
        for bucket in self.buckets:
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    self.query = "CREATE INDEX %s_%s ON %s(%s) " % (index_name_prefix, attr,
                                                                    bucket.name, attr)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append('%s_%s' % (index_name_prefix, attr))
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % (bucket.name)
                    self.prepared_common_body()
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    self.run_cbq_query()

    def test_run_query(self):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_any')
        index_fields = self.input.param("index_field", '').split(';')
        for bucket in self.buckets:
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, bucket.name, ','.join(field.split(';')), self.index_type)
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
