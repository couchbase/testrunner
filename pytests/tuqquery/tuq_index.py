import uuid

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
                    self.sleep(30, 'sleep before running query...')
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
                    self.log.info(f'plan is: {plan}')
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
        index_name_prefix = "my_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_simple_join_keys')
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
                fn = getattr(self, method_name)
                fn()
            finally:
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (indx, query_bucket, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass
