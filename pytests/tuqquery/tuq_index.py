import uuid
import time

from .tuq_join import JoinTests
from .tuq_sanity import QuerySanityTests


class QueriesViewsTests(QuerySanityTests):
    FIELDS_TO_INDEX = ['name', 'job_title', 'join_yr']
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']

    def setUp(self):
        super(QueriesViewsTests, self).setUp()
        self.log.info("==============  QueriesViewsTests setup has started ==============")
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        # self.log.info('-' * 100)
        # self.log.info('Temp fix for MB-16888')
        # self.log.info('-' * 100)
        # self.shell.execute_command("killall -9 cbq-engine")
        # self.shell.execute_command("killall -9 indexes")
        # self.sleep(60, 'wait for indexer, cbq processes to come back up ..')
        # self.log.info('-' * 100)
        self.log.info("==============  QueriesViewsTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()
        self.log.info("==============  QueriesViewsTests suite_setup has started ==============")
        self.log.info("==============  QueriesViewsTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueriesViewsTests tearDown has started ==============")
        self.log.info("==============  QueriesViewsTests tearDown has completed ==============")
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueriesViewsTests suite_tearDown has started ==============")
        self.log.info("==============  QueriesViewsTests suite_tearDown has completed ==============")
        super(QueriesViewsTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            self.log.info('Temp fix for create index failures MB-16888')
            self.sleep(30, 'sleep before create indexes .. ')
            try:
                self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400) if self.DGM else None
                for ind in range(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (view_name, query_bucket,
                                                                         self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(view_name in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append(view_name)
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_view_is_present(view_name, bucket) if self.index_type == 'VIEW' else None
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (view_name, query_bucket, self.index_type)
                    actual_result = self.run_cbq_query()

    def test_primary_create_delete_index(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (query_bucket, self.primary_indx_type)
                self.run_cbq_query()
            except:
                pass
            self.query = "DROP PRIMARY INDEX ON %s USING %s" % (query_bucket, self.primary_indx_type)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])
            self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (query_bucket, self.primary_indx_type)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_create_delete_index_with_query(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    view_name = "tuq_index_%s%s" % (query_bucket, ind)
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (view_name, query_bucket,
                                                                         self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.run_test_case()
            except Exception as ex:
                if self.index_type == 'VIEW':
                    content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale": "ok"},
                                                      bucket="default", retry_time=1)
                    self.log.info("Generated view has %s items" % len(content['rows']))
                    raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (view_name, query_bucket, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.run_test_case()

    def test_create_same_name(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            view_name = "tuq_index_%s%s" % (bucket.name, 'VMs')
            try:
                self.query = "CREATE INDEX %s ON %s(%s) USING VIEW" % (view_name, query_bucket, 'VMs')
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])
                self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (view_name, query_bucket, 'VMs', self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, view_name)
                self._verify_results(actual_result['results'], [])
            finally:
                try:
                    self.query = "DROP INDEX %s ON %s USING VIEW" % (view_name, query_bucket)
                    self.run_cbq_query()
                    self.query = "DROP INDEX %s ON %s USING GSI" % (view_name, query_bucket)
                    self.run_cbq_query()
                except:
                    pass

    '''MB-22129: Test that the created index coveries the queries using LET and LETTING'''

    def test_explain_let_letting(self):
        idx = "idx_bc"
        self.query = 'CREATE INDEX %s ON ' % idx + self.query_buckets[0] + '( join_mo, join_yr) '
        self.run_cbq_query()

        # Test let
        # Number of expected hits for the select query
        result_count = 504
        self.query = "EXPLAIN SELECT d, e FROM " + self.query_buckets[0] + " LET d = join_mo, e = join_yr " \
                                                                           "WHERE d > 11 AND e > 2010"
        result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(result)
        self.query = "SELECT d, e FROM " + self.query_buckets[0] + " LET d = join_mo, e = join_yr " \
                                                                   "WHERE d > 11 AND e > 2010"
        result = self.run_cbq_query()
        self.assertTrue(plan['~children'][0]['index'] == idx
                        and 'join_mo' in plan['~children'][0]['covers'][0]
                        and 'join_yr' in plan['~children'][0]['covers'][1]
                        and result['metrics']['resultCount'] == result_count)

        # Test letting
        result_count = 2
        self.query = 'EXPLAIN SELECT d, e FROM ' + self.query_buckets[0] + \
                     ' LET d = join_mo WHERE d > 10 GROUP BY d LETTING e = SUM(join_yr) HAVING e > 20'
        result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(result)
        self.query = 'SELECT d, e FROM ' + self.query_buckets[0] + \
                     ' LET d = join_mo WHERE d > 10 GROUP BY d LETTING e = SUM(join_yr) HAVING e > 20'
        result = self.run_cbq_query()
        self.assertTrue(plan['~children'][0]['index'] == idx
                        and 'join_mo' in plan['~children'][0]['covers'][0]
                        and 'join_yr' in plan['~children'][0]['covers'][1]
                        and result['metrics']['resultCount'] == result_count)

        self.query = "DROP INDEX %s ON %s USING %s" % (idx, self.query_buckets[0], self.index_type)

    '''MB-22148: The span produced by an OR predicate should be variable in length'''

    def test_variable_length_sarging_or(self):
        idx = "idx_ab"
        result_count = 468
        self.query = 'CREATE INDEX %s ON ' % idx + self.query_buckets[0] + '( join_day, join_mo) '
        self.run_cbq_query()

        self.query = "EXPLAIN SELECT * FROM " + self.query_buckets[0] + \
                     " WHERE join_day = 5 OR ( join_day = 10 AND join_mo = 10 )"
        result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(result)
        self.query = "SELECT * FROM " + self.query_buckets[0] + \
                     " WHERE join_day = 5 OR ( join_day = 10 AND join_mo = 10 )"
        result = self.run_cbq_query()
        self.assertTrue(result['metrics']['resultCount'] == result_count)
        self.assertTrue(len(plan['~children'][0]['spans']) == 2)

        self.query = "DROP INDEX %s ON %s USING %s" % (idx, self.query_buckets[0], self.index_type)

    '''MB-22111: Unnest array covering indexes should not have DistinctScan unless a Distinct array
       is being used in the index'''

    def test_unnest_covering_array_index(self):
        idx = "by_VMs"
        self.query = 'CREATE INDEX %s ON ' % idx + self.query_buckets[0] + \
                     ' (ALL ARRAY r.`name` FOR r IN VMs END, email)'
        self.run_cbq_query()

        result_count = 3024
        self.query = 'explain SELECT t.email, r.`name` FROM ' + self.query_buckets[0] + \
                     ' t UNNEST t.VMs AS r WHERE r.`name` IN [ "vm_12", "vm_13" ]'
        result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(result)
        self.query = 'SELECT t.email, r.`name` FROM ' + self.query_buckets[0] + \
                     ' t UNNEST t.VMs AS r WHERE r.`name` IN [ "vm_12", "vm_13" ]'
        query_result = self.run_cbq_query()
        # plan.values()[1][0].values() is where DistinctScan would appear if it exists
        self.assertTrue("DistinctScan" not in list(plan.values())[1][0].values()
                        and query_result['metrics']['resultCount'] == result_count)

        result_count = 2016
        self.query = 'explain SELECT t.email, r.`name` FROM ' + self.query_buckets[0] + \
                     ' t UNNEST t.VMs AS r WHERE r.`name` = "vm_12"'
        result = self.run_cbq_query()
        plan2 = self.ExplainPlanHelper(result)
        self.query = 'SELECT t.email, r.`name` FROM ' + self.query_buckets[0] + ' t UNNEST t.VMs AS r ' \
                                                                                'WHERE r.`name` = "vm_12"'
        query_result2 = self.run_cbq_query()
        # plan.values()[1][0].values() is where DistinctScan would appear if it exists
        self.assertTrue("DistinctScan" not in list(plan2.values())[1][0].values()
                        and query_result2['metrics']['resultCount'] == result_count)
        self.query = "DROP INDEX %s ON %s USING %s" % (idx, self.query_buckets[0], self.index_type)
        self.run_cbq_query()

        idx2 = "by_VMs2"
        self.query = 'CREATE INDEX %s ON ' % idx2 + self.query_buckets[0] + \
                     ' (DISTINCT ARRAY r.`name` FOR r IN VMs END,VMs, email)'
        self.run_cbq_query()

        self.query = 'explain SELECT t.email, r.`name` FROM ' + self.query_buckets[0] + ' t UNNEST t.VMs AS r ' \
                                                                                        'WHERE r.`name` = "vm_12"'
        result = self.run_cbq_query()
        plan3 = self.ExplainPlanHelper(result)
        self.query = 'SELECT t.email, r.`name` FROM ' + self.query_buckets[0] + ' t UNNEST t.VMs AS r ' \
                                                                                'WHERE r.`name` = "vm_12"'
        query_result3 = self.run_cbq_query()
        # Since DistinctScan does exist we can just look for its specific key
        self.assertTrue("DistinctScan" in list(plan3.values())[1][0].values()
                        and plan3['~children'][0]['#operator'] == "DistinctScan"
                        and query_result3['metrics']['resultCount'] == result_count)
        self.query = "DROP INDEX %s ON %s USING %s" % (idx2, self.query_buckets[0], self.index_type)
        self.run_cbq_query()

    def test_explain(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "EXPLAIN SELECT * FROM %s" % query_bucket
            res = self.run_cbq_query()
            self.log.info(res)
            plan = self.ExplainPlanHelper(res)
            self.assertTrue(plan["~children"][0]["index"] == "#primary",
                            "Type should be #primary, but is: %s" % plan)

            self.query = "EXPLAIN SELECT * FROM %s LIMIT %s" % (query_bucket, 10)
            res = self.run_cbq_query()
            self.assertTrue('limit' in str(res['results']),
                            "Limit is not pushed to primary scan")

    def test_explain_query_count(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s ' % query_bucket
                res = self.run_cbq_query()
                self.log.info(res)
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_query_group_by(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s GROUP BY VMs' % query_bucket
                res = self.run_cbq_query()
                self.log.info(res)
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_query_array(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_arr"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT ARRAY vm.memory FOR vm IN VMs END AS vm_memories FROM %s' % query_bucket
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_query_meta(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta().type) USING %s" % (index_name, query_bucket,
                                                                              self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s d WHERE meta(d).type = "json"' % query_bucket
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_push_limit_intersect_unionscan(self):
        created_indexes = []
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                self.query = "create index ix1 on " + self.query_buckets[0] + "(join_day,VMs[0].os)"
                self.run_cbq_query()
                created_indexes.append("ix1")
                self.query = "create index ix2 on " + self.query_buckets[0] + "(VMs[0].os)"
                self.run_cbq_query()
                created_indexes.append("ix2")
                self.query = "create index ix3 on " + self.query_buckets[0] + "(VMs[0].memory) where VMs[0].memory > 10"
                self.run_cbq_query()
                created_indexes.append("ix3")
                self.query = "explain select * from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].os = 'ubuntu' LIMIT 10"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("limit" in plan['~children'][0]['~children'][0])

                self.query = "explain select * from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].memory > 10"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" not in str(plan))
                self.query = "explain select join_day from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].memory > 10"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("cover" not in str(plan))
                self.query = "select join_day from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].memory > 10 order by meta().id"
                expected_result = self.run_cbq_query()
                self.query = "create index ix4 on " + self.query_buckets[
                    0] + "(VMs[0].memory,join_day) where VMs[0].memory > 10"
                self.run_cbq_query()
                created_indexes.append("ix4")
                self._wait_for_index_online(bucket, "ix4")
                self.query = "explain select join_day from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].memory > 10"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("cover" in str(plan))
                self.query = "select join_day from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].memory > 10 order by meta().id"
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = "select join_day from " + self.query_buckets[
                    0] + " use index(`#primary`) where join_day > 10 AND VMs[0].memory > 10 order by meta().id"
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = "select * from " + self.query_buckets[
                    0] + " where join_day > 10 AND VMs[0].os = 'ubuntu' LIMIT 10"
                res = self.run_cbq_query()
                self.assertTrue(res['metrics']['resultCount'] == 10)
                self.query = "explain select * from " + self.query_buckets[
                    0] + " where join_day > 10 OR VMs[0].os = 'ubuntu'"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("cover" not in str(plan))
                self.query = "explain select join_day from " + self.query_buckets[
                    0] + " where join_day > 10 OR VMs[0].memory > 10"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("cover" not in str(plan))
                self.query = "explain select join_day from " + self.query_buckets[
                    0] + " where join_day > 10 OR VMs[0].os = 'ubuntu'"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("cover" not in str(plan))
                self.query = "select * from " + self.query_buckets[
                    0] + " where join_day > 10 OR VMs[0].os = 'ubuntu' LIMIT 10"
                res = self.run_cbq_query()
                self.assertTrue(res['metrics']['resultCount'] == 10)
                self.query = "explain select * from " + self.query_buckets[
                    0] + " where join_day > 10 and VMs[0].memory > 0 and VMs[0].os = 'ubuntu' LIMIT 10"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("limit" not in plan['~children'][0]['~children'][0])
                self.query = "select * from " + self.query_buckets[
                    0] + " where join_day > 10 and VMs[0].memory > 0 and VMs[0].os = 'ubuntu' LIMIT 10"
                res = self.run_cbq_query()
                self.assertTrue(res['metrics']['resultCount'] == 10)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (idx, self.query_buckets[0], self.index_type)
                    self.run_cbq_query()

    def test_meta_no_duplicate_results(self):
        if self.DGM:
            self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
        self.query = 'insert into ' + self.query_buckets[0] + ' values ("k01",{"name":"abc"})'
        self.run_cbq_query()
        self.query = 'select name,meta().id from ' + self.query_buckets[0] + ' where meta().id IN ["k01",""]'
        res = self.run_cbq_query()
        self.assertTrue(res['results'] == [{'id': 'k01', 'name': 'abc'}])
        self.query = 'delete from ' + self.query_buckets[0] + ' use keys ["k01"]'
        self.run_cbq_query()

    def test_unnest_when(self):
        created_indexes = []
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                idx1 = "unnest_idx"
                idx2 = "idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i.memory FOR i in %s  when i.memory > 10 END) " % (
                    idx1, query_bucket, "VMs")
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx1)
                self.assertTrue(idx1 in str(actual_result['results']),
                                f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx1)
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i.memory FOR i in %s END) " % (
                    idx2, query_bucket, "VMs")
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']),
                                f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx1), "Index is not in list")
                self.query = "EXPLAIN select d.name from %s d UNNEST VMs as x where any i in " \
                             "d.VMs satisfies i.memory > 9 END" % query_bucket
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)
                self.query = "EXPLAIN select d.name from %s d UNNEST VMs as x where any i in" \
                             " d.VMs satisfies i.memory > 10 END" % query_bucket
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx1 or result1 == idx2)
                self.assertTrue(result2 == idx1 or result2 == idx2)
                self.assertTrue(result1 != result2)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (idx, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_notin_notwithin(self):
        created_indexes = []
        try:
            idx = "ix"
            self.query = 'create index {0} on '.format(idx) + self.query_buckets[0] + '(join_day)'
            self.run_cbq_query()
            self.query = 'explain select 1 from ' + self.query_buckets[0] + ' where NOT (join_day IN [ 1])'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['index'] == idx)
            self.query = 'explain select 1 from ' + self.query_buckets[0] + ' where NOT (join_day WITHIN [ 1])'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['index'] == idx)
            self.query = 'explain select 1 from ' + self.query_buckets[0] + ' where (join_day IN NOT [ 1])'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['index'] == idx)
            self.query = 'explain select 1 from ' + self.query_buckets[0] + ' where (join_day WITHIN NOT [ 1])'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['index'] == idx)
            self.query = 'explain select 1 from ' + self.query_buckets[0] + ' where join_day NOT WITHIN [ 1]'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['index'] == idx)
            self.query = 'explain select 1 from ' + self.query_buckets[0] + ' where join_day NOT IN [ 1]'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['index'] == idx)
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (idx, self.query_buckets[0], self.index_type)
                self.run_cbq_query()

    def test_create_arrays_ranging_over_object(self):
        if self.DGM:
            self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
        self.query = 'select array j for i:j in {"a":1, "b":2} end'
        res = self.run_cbq_query()
        self.assertTrue(res['results'] == [{'$1': [1, 2]}])
        self.query = 'select array j for i:j in {"a":1, "b":2, "c":[2,3], "d": "%s", "e":2, "f": %s } end' % (
            "verbose", '{"a":1}')
        res = self.run_cbq_query()
        self.assertTrue(res['results'] == [{'$1': [1, 2, [2, 3], 'verbose', 2, {'a': 1}]}])

    def test_explain_index_with_fn(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_fn"
            try:
                self.query = "CREATE INDEX %s ON %s(round(test_rate)) USING %s" % (
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN select name, round(test_rate) as rate from %s ' \
                             'WHERE round(test_rate) = 2' % query_bucket
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_index_attr(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "my_attr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                        index_name, query_bucket, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    self.query = "EXPLAIN SELECT * FROM %s WHERE %s = 'abc'" % (
                        query_bucket, self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
                    created_indexes.append(index_name)
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_explain_non_index_attr(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_non_index"
            try:
                self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = "EXPLAIN SELECT * FROM %s WHERE email = 'abc'" % query_bucket
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] != index_name,
                                "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_index_count_gn(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                        index_name, query_bucket, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT COUNT(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_aggr_gn(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "my_aggr_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                        index_name, query_bucket, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT SUM(%s) FROM %s" % (self.FIELDS_TO_INDEX[ind - 1], query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_join(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee" \
                                 " JOIN %s as new_task" % (query_bucket, query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_unnest(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(tasks_ids) USING %s" % (
                        index_name, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT emp.name, task FROM %s emp UNNEST emp.tasks_ids task" % query_bucket
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan["~children"][0]["index"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_day) USING %s" % (
                        index_name, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s use keys ['query-1'] " \
                                 "where join_day>2) as names from %s" % (query_bucket, query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) USING %s" % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % query_bucket + \
                             'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points) USING %s" % (
                    index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % query_bucket + \
                             'WHERE tasks_points > 0'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_objects_element(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1) USING %s" % (
                    index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % query_bucket + \
                             'WHERE tasks_points.task1 > 0'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list_element(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_list_el"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) USING %s" % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                             ' FROM %s WHERE skills[0] = "abc"' % query_bucket
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_childs_list(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_list"
            try:
                self.query = "CREATE INDEX %s ON %s(skills[0]) USING %s" % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT DISTINCT skills[0] as skill' + \
                             ' FROM %s WHERE skill[0] = "skill2010"' % query_bucket
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_explain_several_complex_objects(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for ind in range(self.num_indexes):
                    index_name = "my_index_complex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                        index_name, query_bucket, self.FIELDS_TO_INDEX[ind - 1], self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'EXPLAIN SELECT DISTINCT %s as complex FROM %s WHERE %s = "abc"' % (
                        self.FIELDS_TO_INDEX[ind - 1],
                        query_bucket,
                        self.FIELDS_TO_INDEX[ind - 1])
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_index_meta(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(" % (index_name, query_bucket) + \
                             "meta().id) USING %s" % self.index_type
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
            except Exception as ex:
                self.assertTrue(str(ex).find("Error creating index") != -1,
                                "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")

    def test_index_dates(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            index_name = "my_index_date"
            try:
                self.query = "CREATE INDEX %s ON %s(" % (index_name, query_bucket) + \
                             "str_to_millis(tostr(join_yr) || '-0' || tostr(join_mo) || '-' || tostr(join_day))) "
                self.run_cbq_query()
            except Exception as ex:
                self.assertTrue(str(ex).find("Error creating index") != -1,
                                "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")

    def test_multiple_index_hints_explain_select(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo', 'join_day,join_mo']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                          query_bucket, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % ind_name)
                for ind in created_indexes:
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s  USE INDEX(%s using %s) ' \
                                 'WHERE join_day>2 AND join_mo>3' % (query_bucket, ind, self.index_type)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_index_hints_explain_aggr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for attr in ['job_title', 'test_rate', 'job_title,test_rate']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                          query_bucket, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % ind_name)
                for ind in created_indexes:
                    self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s as employees " \
                                 "USE INDEX(%s using %s)" % (query_bucket, ind, self.index_type) + \
                                 " WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING SUM(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
                    self.assertTrue(res["status"] == 'success')
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_index_hints_explain_same_attr(self):
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                fields = ['job_title', 'job_title,test_rate']
                for attr in fields:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (ind_name,
                                                                         query_bucket, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('%s' % ind_name)
                for ind in created_indexes:
                    self.query = "EXPLAIN SELECT join_mo, SUM(test_rate) as rate FROM %s  as employees " \
                                 "USE INDEX(%s using %s)" % (query_bucket, ind, self.index_type) + \
                                 "WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING SUM(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_indexes_query_attr(self):
        self.fail_if_no_buckets()
        index_name_prefix = 'auto_ind'
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for attr in ['join_day', 'join_day,join_mo']:
                    ind_name = '%s_%s' % (index_name_prefix, attr.split('.')[0].split('[')[0].replace(',', '_'))
                    self.query = "CREATE INDEX %s ON %s(%s) " % (ind_name,
                                                                 query_bucket, attr)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append(ind_name)
                    self.query = 'SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (
                        query_bucket)
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name": doc['name'], "join_mo": doc['join_mo'], "join_day": doc["join_day"]}
                                       for doc in full_list if doc['join_day'] > 2 and doc['join_mo'] > 3]
                    self.query = "select * from %s" % query_bucket
                    self.run_cbq_query()
                    self._verify_results(res['results'], expected_result)
                    self.query = 'EXPLAIN SELECT name, join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3' % (
                        query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == ind_name,
                                    "Index should be %s, but is: %s" % (ind_name, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_multiple_indexes_query_non_ind_attr(self):
        self.fail_if_no_buckets()
        index_name_prefix = 'auto_ind'
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s%s' % (index_name_prefix, attr, str(uuid.uuid4())[:4])
                    self.query = "CREATE INDEX %s ON %s(%s) " % (index_name,
                                                                 query_bucket, attr)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % query_bucket
                    res = self.run_cbq_query()
                    full_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [{"name": doc['name'], "join_yr": doc['join_yr'], "join_day": doc["join_day"]}
                                       for doc in full_list if doc['join_yr'] > 3]
                    self._verify_results(res['results'], expected_result)
                    # self.assertTrue(len(res['results'])==10)
                    self.query = 'EXPLAIN SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % query_bucket
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    print(plan)
                    print(plan['~children'][0])
                    self.assertTrue(plan["~children"][0]["index"] != '%s_%s' % (index_name_prefix, attr),
                                    "Index should be %s_%s, but is: %s" % (index_name_prefix, attr, plan))
            finally:
                for index_name in set(created_indexes):
                    try:
                        self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                        self.run_cbq_query()
                    except:
                        pass

    def test_negative_indexes(self):
        queries_errors = {'create index gsi on ' + self.query_buckets[0] + '(name) USING %s': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_prepared_with_index_simple_where(self):
        self.fail_if_no_buckets()
        index_name_prefix = 'auto_ind_prepared'
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                for attr in ['join_day', 'join_mo']:
                    index_name = '%s_%s' % (index_name_prefix, attr)
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name,
                                                                         query_bucket, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append('%s_%s' % (index_name_prefix, attr))
                    self.query = 'SELECT name, join_day, join_yr FROM %s WHERE join_yr>3' % query_bucket
                    self.prepared_common_body()
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
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
        if self.DGM:
            self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, query_bucket, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
            except:
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (query_bucket, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
                raise
        try:
            self.query = "select * from system:indexes where name = %s" % index_name
            self.log.info(self.run_cbq_query())
            self.hint_index = indexes[0]
            fn = getattr(self, method_name)
            fn()
        finally:
            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (query_bucket, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_prepared_hints_letting(self):
        self.run_query_prepared("SELECT join_mo, sum_test from {0} USE INDEX ({1} using {2}) WHERE join_mo>7 group by"
                                " join_mo letting sum_test = sum(tasks_points.task1)")

    def test_prepared_hints_array(self):
        self.run_query_prepared("SELECT job_title, array_append(array_agg(DISTINCT name), 'new_name') as names FROM {0}"
                                " USE INDEX ({1} using {2}) GROUP BY job_title")

    def test_prepared_hints_intersect(self):
        self.run_query_prepared(
            "select name from {0} intersect all select name from {0} s USE INDEX ({1} using {2}) where s.join_day>5")

    def run_query_prepared(self, query):
        indexes = []
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        index_fields = self.input.param("index_field", '').split(';')
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                        index_name, query_bucket, ','.join(field.split(';')), self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
            except:
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (query_bucket, indx, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
                raise
        try:
            self.query = "select * from system:indexes where name = %s" % index_name
            self.log.info(self.run_cbq_query())
            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                self.query = query.format(query_bucket, indexes[0], self.index_type)
                self.prepared_common_body()
        finally:
            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (query_bucket, indx, self.index_type)
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
                for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                    for indx in indexes:
                        self.query = "DROP INDEX %s ON %s USING %s" % (query_bucket, indx, self.index_type)
                        try:
                            self.run_cbq_query()
                        except:
                            pass

    def test_intersect_scan_meta(self):
        test_to_run = self.input.param("test_to_run", '')
        indexes = []
        self.fail_if_no_buckets()
        try:
            indexes = []
            index_name_prefix = "inter_index_" + str(uuid.uuid4())[:4]
            index_fields = self.input.param("index_field", '').split(';')

            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                for _ in index_fields:
                    index_name = '%sid_meta' % index_name_prefix
                    query = "CREATE INDEX %s ON %s(meta(%s).id) USING %s" % (
                        index_name, query_bucket, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                    index_name = '%stype_meta' % index_name_prefix
                    query = "CREATE INDEX %s ON %s(meta(%s).type) USING %s" % (
                        index_name, query_bucket, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                self.test_comparition_meta()

            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                self.query = "SELECT meta(d).id, meta(d).type FROM %s d" % query_bucket
                self.run_cbq_query()
                query = 'EXPLAIN %s' % self.query
                res = self.run_cbq_query(query=query)
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == 'IntersectScan',
                                "Index should be intersect scan and is %s" % plan)
                actual_indexes = [scan['index'] for scan in plan["~children"][0]["~children"][0]['scans']]
                self.assertTrue(set(actual_indexes) == set(indexes),
                                "Indexes should be %s, but are: %s" % (indexes, actual_indexes))
        finally:
            for indx in indexes:
                for query_bucket in self.query_buckets:
                    self.query = "DROP INDEX %s ON %s USING %s" % (indx, query_bucket, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_index_order_partial_sort_pruning(self):
        """
        Test that index order can prune sort size when the first sort term matches index order.
        Creates index: (c1, c2 desc, c3)
        Query: ORDER BY c1, c2 desc, c3 desc
        Should show partial_sort_term_count: 2 and reduced sortCount.
        """
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                # Create test data with multiple distinct c1 values
                self._create_test_data_for_sort_pruning(query_bucket)

                # Create index with mixed order: (c1, c2 desc, c3)
                index_name = "ix1"
                self.query = f"CREATE INDEX {index_name} ON {query_bucket}(c1, c2 DESC, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)

                # Test query with ORDER BY that matches index order for first term
                self.query = f"EXPLAIN SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)

                # Verify Order operator exists and has partial_sort_term_count
                order_operator = self._find_order_operator(plan)
                self.assertIsNotNone(order_operator, "Order operator not found in plan")
                self.assertEqual(order_operator.get('partial_sort_term_count'), 2, 
                                "Expected partial_sort_term_count to be 2")
                self.assertEqual(order_operator.get('limit'), "2")
                self.assertEqual(order_operator.get('offset'), "9")

                # Verify sort terms
                sort_terms = order_operator.get('sort_terms', [])
                self.assertEqual(len(sort_terms), 3, "Expected 3 sort terms")
                self.assertEqual(sort_terms[0]['expr'], f"_index_key ((`{query_bucket}`.`c1`))")
                self.assertEqual(sort_terms[1]['expr'], f"_index_key ((`{query_bucket}`.`c2`))")
                self.assertEqual(sort_terms[1]['desc'], '"desc"')
                self.assertEqual(sort_terms[2]['expr'], f"_index_key ((`{query_bucket}`.`c3`))")
                self.assertEqual(sort_terms[2]['desc'], '"desc"')

                # Run the actual query and verify sortCount metric
                self.query = f"SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res = self.run_cbq_query(query_params={'profile': 'phases'})

                # Verify sortCount is reduced (should be around 6 for 5 distinct c1 values)
                sort_count = res['profile'].get('phaseCounts', {}).get('sort', 0)
                self.log.info(f"Sort count: {sort_count}")
                self.assertLess(sort_count, 125, f"Expected reduced sortCount, got {sort_count}")
                self.assertGreater(sort_count, 0, f"Expected positive sortCount, got {sort_count}")

            finally:
                for index_name in created_indexes:
                    self.query = f"DROP INDEX {index_name} ON {query_bucket} USING {self.index_type}"
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_index_order_no_partial_sort_desc_first(self):
        """
        Test that when first sort term is DESC and doesn't match index order,
        no partial_sort_term_count is used.
        Creates index: (c1, c2 desc, c3)
        Query: ORDER BY c1 DESC, c2 DESC, c3 DESC
        Should NOT show partial_sort_term_count and full sortCount.
        """
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                # Create test data with multiple distinct c1 values
                self._create_test_data_for_sort_pruning(query_bucket)

                # Create index with mixed order: (c1, c2 desc, c3)
                index_name = "ix1"
                self.query = f"CREATE INDEX {index_name} ON {query_bucket}(c1, c2 DESC, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)

                # Test query with ORDER BY that has DESC first term (doesn't match index)
                self.query = f"EXPLAIN SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1 DESC, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)

                # Verify Order operator exists but NO partial_sort_term_count
                order_operator = self._find_order_operator(plan)
                self.assertIsNotNone(order_operator, "Order operator not found in plan")
                self.assertNotIn('partial_sort_term_count', order_operator, 
                                "Should not have partial_sort_term_count when first sort term is DESC")

                # Run the actual query and verify full sortCount
                self.query = f"SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1 DESC, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res = self.run_cbq_query(query_params={'profile': 'phases'})

                # Verify sortCount is full (should be around 125 for 5*5*5 documents)
                sort_count = res['profile'].get('phaseCounts', {}).get('sort', 0)
                self.log.info(f"Sort count: {sort_count}")
                self.assertGreaterEqual(sort_count, 100, f"Expected full sortCount, got {sort_count}")

            finally:
                for index_name in created_indexes:
                    self.query = f"DROP INDEX {index_name} ON {query_bucket} USING {self.index_type}"
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_index_order_partial_sort_with_equality_filter(self):
        """
        Test that equality filter on first index column re-enables partial sorting
        even when first sort term is DESC.
        Creates index: (c1, c2 desc, c3)
        Query: WHERE c1 = 1 ORDER BY c1 DESC, c2 DESC, c3 DESC
        Should show partial_sort_term_count: 2 due to equality filter.
        """
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                # Create test data with multiple distinct c1 values
                self._create_test_data_for_sort_pruning(query_bucket)

                # Create index with mixed order: (c1, c2 desc, c3)
                index_name = "ix1"
                self.query = f"CREATE INDEX {index_name} ON {query_bucket}(c1, c2 DESC, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)

                # Test query with equality filter and DESC first sort term
                self.query = f"EXPLAIN SELECT noncover FROM {query_bucket} WHERE c1 = 1 ORDER BY c1 DESC, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)

                # Verify Order operator exists and has partial_sort_term_count due to equality filter
                order_operator = self._find_order_operator(plan)
                self.assertIsNotNone(order_operator, "Order operator not found in plan")
                self.assertEqual(order_operator.get('partial_sort_term_count'), 1, 
                                "Expected partial_sort_term_count to be 1 due to equality filter")

                # Run the actual query and verify reduced sortCount
                self.query = f"SELECT noncover FROM {query_bucket} WHERE c1 = 1 ORDER BY c1 DESC, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res = self.run_cbq_query(query_params={'profile': 'phases'})

                # Verify sortCount is reduced (should be around 16 for 5*5 documents with c1=1)
                sort_count = res['profile'].get('phaseCounts', {}).get('sort', 0)
                self.log.info(f"Sort count: {sort_count}")
                self.assertLess(sort_count, 125, f"Expected reduced sortCount with equality filter, got {sort_count}")
                self.assertGreater(sort_count, 0, f"Expected positive sortCount, got {sort_count}")

            finally:
                for index_name in created_indexes:
                    self.query = f"DROP INDEX {index_name} ON {query_bucket} USING {self.index_type}"
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_index_order_partial_sort_complex_scenarios(self):
        """
        Test various complex scenarios for partial sort pruning:
        1. Index: (c1, c2, c3) with ORDER BY c1, c2, c3 (full match)
        2. Index: (c1, c2, c3) with ORDER BY c1, c2 DESC, c3 (partial match)
        3. Index: (c1 DESC, c2, c3) with ORDER BY c1 DESC, c2, c3 (full match)
        """
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                # Create test data
                self._create_test_data_for_sort_pruning(query_bucket)

                # Test Case 1: Full match - all ASC
                index_name1 = "ix1_asc"
                self.query = f"CREATE INDEX {index_name1} ON {query_bucket}(c1, c2, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name1)
                created_indexes.append(index_name1)

                self.query = f"EXPLAIN SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1, c2, c3 OFFSET 9 LIMIT 2"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.log.info(f"Plan: {plan}")

                order_operator = self._find_order_operator(plan)
                self.assertIsNone(order_operator, "Order operator should not be found in plan")

                # Test Case 2: Partial match - mixed order
                index_name2 = "ix2_mixed"
                self.query = f"CREATE INDEX {index_name2} ON {query_bucket}(c1, c2, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name2)
                created_indexes.append(index_name2)

                self.query = f"EXPLAIN SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1, c2 DESC, c3 OFFSET 9 LIMIT 2"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.log.info(f"Plan: {plan}")

                order_operator = self._find_order_operator(plan)
                self.assertIsNotNone(order_operator, "Order operator not found in plan")
                self.assertEqual(order_operator.get('partial_sort_term_count'), 1, 
                                "Expected partial_sort_term_count to be 1 for partial match")

                # Test Case 3: Full match - first DESC
                index_name3 = "ix3_desc"
                self.query = f"CREATE INDEX {index_name3} ON {query_bucket}(c1 DESC, c2, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name3)
                created_indexes.append(index_name3)

                self.query = f"EXPLAIN SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1 DESC, c2, c3 OFFSET 9 LIMIT 2"
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.log.info(f"Plan: {plan}")

                order_operator = self._find_order_operator(plan)
                self.assertIsNone(order_operator, "Order operator should not be found in plan")

            finally:
                for index_name in created_indexes:
                    self.query = f"DROP INDEX {index_name} ON {query_bucket} USING {self.index_type}"
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def test_index_order_partial_sort_metrics_validation(self):
        """
        Test that the sortCount metrics are correctly reported for partial sort scenarios.
        Validates that partial sorting reduces the actual number of items sorted.
        """
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            created_indexes = []
            try:
                # Create test data with known distribution
                self._create_test_data_for_sort_pruning(query_bucket)

                # Create index: (c1, c2 desc, c3)
                index_name = "ix1"
                self.query = f"CREATE INDEX {index_name} ON {query_bucket}(c1, c2 DESC, c3) USING {self.index_type}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)

                # Test 1: Partial sort enabled (should have lower sortCount)
                self.query = f"SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res_partial = self.run_cbq_query(query_params={'profile': 'phases'})
                sort_count_partial = res_partial['profile'].get('phaseCounts', {}).get('sort', 0)
                self.log.info(f"Sort count partial: {sort_count_partial}")

                # Test 2: No partial sort (should have higher sortCount)
                self.query = f"SELECT noncover FROM {query_bucket} WHERE c1 IS VALUED ORDER BY c1 DESC, c2 DESC, c3 DESC OFFSET 9 LIMIT 2"
                res_full = self.run_cbq_query(query_params={'profile': 'phases'})
                sort_count_full = res_full['profile'].get('phaseCounts', {}).get('sort', 0)
                self.log.info(f"Sort count full: {sort_count_full}")

                # Verify that partial sort has lower sortCount
                self.assertLess(sort_count_partial, sort_count_full, 
                               f"Partial sort should have lower sortCount. Partial: {sort_count_partial}, Full: {sort_count_full}")

                # Verify reasonable ranges
                self.assertGreater(sort_count_partial, 0, "Partial sortCount should be positive")
                self.assertLess(sort_count_partial, 50, "Partial sortCount should be significantly less than total documents")
                self.assertGreater(sort_count_full, 100, "Full sortCount should be close to total documents")

            finally:
                for index_name in created_indexes:
                    self.query = f"DROP INDEX {index_name} ON {query_bucket} USING {self.index_type}"
                    try:
                        self.run_cbq_query()
                    except:
                        pass

    def _create_test_data_for_sort_pruning(self, query_bucket):
        """
        Create test data with 5 distinct c1 values, each with 5 distinct c2 values, 
        each with 5 distinct c3 values (total 125 documents).
        """
        # Clear existing data first
        self.query = f"DELETE FROM {query_bucket}"
        self.run_cbq_query()

        # Insert test data with controlled distribution
        for c1 in range(1, 6):  # 5 distinct c1 values
            for c2 in range(1, 6):  # 5 distinct c2 values  
                for c3 in range(1, 6):  # 5 distinct c3 values
                    doc_id = f"doc_{c1}_{c2}_{c3}"
                    self.query = (
                        f"INSERT INTO {query_bucket} (KEY, VALUE) VALUES ('{doc_id}', "
                        f"{{'c1': {c1}, 'c2': {c2}, 'c3': {c3}, 'noncover': 'value_{c1}_{c2}_{c3}'}})"
                    )
                    self.run_cbq_query()
        # Verify data was inserted
        end_time = time.time() + 60
        while time.time() < end_time:
            self.query = f"SELECT COUNT(*) as cnt FROM {query_bucket}"
            res = self.run_cbq_query()
            result_count = res['results'][0]['cnt']
            if res['results'][0]['cnt'] == 125:
                return
            else:
                self.sleep(1, f'result count is not 125. res is {result_count} sleeping for 1 second')
        raise Exception('result count is not 125. last response is %s' % (res))

    def _find_order_operator(self, plan):
        """
        Recursively find the Order operator in the explain plan.
        """
        if isinstance(plan, dict):
            if plan.get('#operator') == 'Order':
                return plan

            # Check children
            for key, value in plan.items():
                if key == '~children' and isinstance(value, list):
                    for child in value:
                        result = self._find_order_operator(child)
                        if result:
                            return result
                elif isinstance(value, dict):
                    result = self._find_order_operator(value)
                    if result:
                        return result
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            result = self._find_order_operator(item)
                            if result:
                                return result
        return None

class QueriesJoinViewsTests(JoinTests):

    def setUp(self):
        super(QueriesJoinViewsTests, self).setUp()
        self.log.info("==============  QueriesJoinViewsTests setup has started ==============")
        self.num_indexes = self.input.param('num_indexes', 1)
        self.index_type = self.input.param('index_type', 'VIEW')
        self.log.info("==============  QueriesJoinViewsTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueriesJoinViewsTests, self).suite_setUp()
        self.log.info("==============  QueriesJoinViewsTests suite_setup has started ==============")
        self.log.info("==============  QueriesJoinViewsTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueriesJoinViewsTests tearDown has started ==============")
        self.log.info("==============  QueriesJoinViewsTests tearDown has completed ==============")
        super(QueriesJoinViewsTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueriesJoinViewsTests suite_tearDown has started ==============")
        self.log.info("==============  QueriesJoinViewsTests suite_tearDown has completed ==============")
        super(QueriesJoinViewsTests, self).suite_tearDown()

    def test_run_query(self):
        indexes = []
        index_name_prefix = f"my_index_{str(uuid.uuid4())[:4]}"
        method_name = self.input.param('to_run', 'test_simple_join_keys')
        index_fields = self.input.param("index_field", '').split(';')
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                for field in index_fields:
                    index_name = f"{index_name_prefix}{field.split('.')[0].split('[')[0]}"
                    self.query = f"CREATE INDEX {index_name} ON {query_bucket}({','.join(field.split(';'))}) USING {self.index_type}"
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    indexes.append(index_name)
                fn = getattr(self, method_name)
                fn()
            finally:
                for indx in indexes:
                    self.query = f"DROP INDEX {indx} ON {query_bucket} USING {self.index_type}"
                    try:
                        self.run_cbq_query()
                    except:
                        pass
