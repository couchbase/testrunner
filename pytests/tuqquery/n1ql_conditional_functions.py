from tuq import QueryTests


class QueryConditionalFunctionsTests(QueryTests):

    def setUp(self):
        super(QueryConditionalFunctionsTests, self).setUp()
        self.log.info("==============  QueryFunctionsTests setup has started ==============")
        self.primary_index_def = {'name': '#primary', 'bucket': 'default', 'fields': [],
                                  'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        self.log.info("==============  QueryFunctionsTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryConditionalFunctionsTests, self).suite_setUp()
        self.log.info("==============  QueryFunctionsTests suite_setup has started ==============")
        self.log.info("==============  QueryFunctionsTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryFunctionsTests tearDown has started ==============")
        self.log.info("==============  QueryFunctionsTests tearDown has completed ==============")
        super(QueryConditionalFunctionsTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryFunctionsTests suite_tearDown has started ==============")
        self.log.info("==============  QueryFunctionsTests suite_tearDown has completed ==============")
        super(QueryConditionalFunctionsTests, self).suite_tearDown()

    def verifier(self, compare_query, query_index):
        return lambda x: self.compare_queries(x['q_res'][query_index], compare_query)

    def plan_verifier(self, check_type, check_data, query_index):
        return lambda x: self.check_plan(x, check_type, check_data, query_index)

    def check_plan(self, explain_results, check_type, check_data, query_index):
        plan = self.ExplainPlanHelper(explain_results['q_res'][query_index])
        if check_type == "index":
            index = plan["~children"][0]['index']
            self.assertEqual(index, check_data)
        if check_type == "pushdown":
            self.assertTrue("index_group_aggs" in str(plan))

    def compare_queries(self, actual_results, compare_query):
        results = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(results), len(compare_docs))
        self.log.info('comparing results...')
        for i in range(0, len(results)):
            self.assertEqual(results[i], compare_docs[i])
        self.log.info('results match')

    def test_decode_basic(self):
        queries = dict()

        query_1 = "select decode(join_yr, 2010, 'first_result_term', 2011, 'second_result_term') from default"
        verify_1 = "select case when join_yr = 2010 then 'first_result_term' when join_yr = 2011 then 'second_result_term' end from default"

        query_2 = "select decode(join_yr, 2011, 'first_result_term', 2012, 'second_result_term', 'default_term') from default"
        verify_2 = "select case when join_yr = 2011 then 'first_result_term' when join_yr = 2012 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(join_yr, 2011, 'first_result_term', 2011, 'second_result_term', 'default_term') from default"
        verify_3 = "select case when join_yr = 2011 then 'first_result_term' when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(join_yr, 2011, 'first_result_term', 2014, 'second_result_term', 'default_term') from default"
        verify_4 = "select case when join_yr = 2011 then 'first_result_term' when join_yr = 2014 then 'second_result_term' else 'default_term' end from default"

        query_5 = "select decode(join_yr, 2014, 'first_result_term', 2012, 'second_result_term', 'default_term') from default"
        verify_5 = "select case when join_yr = 2014 then 'first_result_term' when join_yr = 2012 then 'second_result_term' else 'default_term' end from default"

        query_6 = "select decode(join_yr, 2013, 'first_result_term', 2014, 'second_result_term', 'default_term') from default"
        verify_6 = "select case when join_yr = 2013 then 'first_result_term' when join_yr = 2014 then 'second_result_term' else 'default_term' end from default"

        query_7 = "select decode(join_yr, 2010, join_mo, 2011, join_day, 'default_term') from default"
        verify_7 = "select case when join_yr = 2010 then join_mo when join_yr = 2011 then join_day else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}

        self.query_runner(queries)

    def test_decode_functions(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(join_yr-round(join_yr,-1), 0, 'first_result_term', 1, 'second_result_term') from default"
        verify_1 = "select case when join_yr-round(join_yr,-1) = 0 then 'first_result_term' when join_yr-round(join_yr,-1) = 1 then 'second_result_term' end from default"

        query_2 = "select decode(join_yr-round(join_yr,-1), 1, 'first_result_term', 2, 'second_result_term', 'default_term') from default"
        verify_2 = "select case when join_yr-round(join_yr,-1) = 1 then 'first_result_term' when join_yr-round(join_yr,-1) = 2 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(join_yr-round(join_yr,-1), 1, 'first_result_term', 1, 'second_result_term', 'default_term') from default"
        verify_3 = "select case when join_yr-round(join_yr,-1) = 1 then 'first_result_term' when join_yr-round(join_yr,-1) = 1 then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(join_yr-round(join_yr,-1), 1, 'first_result_term', 4, 'second_result_term', 'default_term') from default"
        verify_4 = "select case when join_yr-round(join_yr,-1) = 1 then 'first_result_term' when join_yr-round(join_yr,-1) = 4 then 'second_result_term' else 'default_term' end from default"

        query_5 = "select decode(join_yr-round(join_yr,-1), 4, 'first_result_term', 2, 'second_result_term', 'default_term') from default"
        verify_5 = "select case when join_yr-round(join_yr,-1) = 4 then 'first_result_term' when join_yr-round(join_yr,-1) = 2 then 'second_result_term' else 'default_term' end from default"

        query_6 = "select decode(join_yr-round(join_yr,-1), 3, 'first_result_term', 4, 'second_result_term', 'default_term') from default"
        verify_6 = "select case when join_yr-round(join_yr,-1) = 3 then 'first_result_term' when join_yr-round(join_yr,-1) = 4 then 'second_result_term' else 'default_term' end from default"

        query_7 = "select count(decode(join_yr, 2010, 'first_result_term', 2011, 'second_result_term','default_term')) from default"
        explain_7 = "explain " + query_7
        verify_7 = "select 2016"

        query_8 = "select count(decode(join_yr, 2010, 'first_result_term', 'default_term')) from default"
        explain_8 = "explain " + query_8
        verify_8 = "select 2016"

        query_9 = "select count(decode(join_yr, 2010, 'first_result_term')) from default"
        explain_9 = "explain " + query_9
        verify_9 = "select 1008"

        query_10 = "select decode(join_yr, 2010, ln(join_mo), 2011, ln(join_day), 'default_term') from default"
        verify_10 = "select case when join_yr = 2010 then ln(join_mo) when join_yr = 2011 then ln(join_day) else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_7, explain_7],
                        "asserts": [self.verifier(verify_7, 0), self.plan_verifier("index", "idx1", 1)]}
        queries["h"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_8, explain_8],
                        "asserts": [self.verifier(verify_8, 0), self.plan_verifier("index", "idx1", 1)]}
        queries["i"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_9, explain_9],
                        "asserts": [self.verifier(verify_9, 0), self.plan_verifier("index", "idx1", 1)]}
        queries["j"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_10], "asserts": [self.verifier(verify_10, 0)]}

        self.query_runner(queries)

    def test_decode_pushdown(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        num_docs_string = str(self.docs_per_day * 2016)

        query_1 = "select decode(count(join_yr), %s, 'first_result_term', 'default_term') from default" % num_docs_string
        explain_1 = "explain " + query_1
        verify_1 = "select 'first_result_term'"

        query_2 = "select decode(count(join_yr), %s, 'first_result_term', %s, 'second_result_term', 'default_term') from default" % (num_docs_string, num_docs_string)
        explain_2 = "explain " + query_1
        verify_2 = "select 'first_result_term'"

        query_3 = "select decode(count(join_yr), 0, 'first_result_term', %s, 'second_result_term', 'default_term') from default" % num_docs_string
        explain_3 = "explain " + query_1
        verify_3 = "select 'second_result_term'"

        query_4 = "select decode(count(join_yr), 0, 'first_result_term', 1, 'second_result_term', 'default_term') from default"
        explain_4 = "explain " + query_1
        verify_4 = "select 'default_term'"

        query_5 = "select decode(count(join_yr), 0, 'first_result_term', 1, 'second_result_term') from default"
        explain_5 = "explain " + query_1
        verify_5 = "select null"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1, explain_1],
                        "asserts": [self.verifier(verify_1, 0), self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2, explain_2],
                        "asserts": [self.verifier(verify_2, 0), self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3, explain_3],
                        "asserts": [self.verifier(verify_3, 0), self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_4, explain_4],
                        "asserts": [self.verifier(verify_4, 0), self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}
        queries["e"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_5, explain_5],
                        "asserts": [self.verifier(verify_5, 0), self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        self.query_runner(queries)

    def test_decode_subquery(self):
        queries = dict()

        query_1 = "select table.* from (select decode(join_yr, 2010, 'first_result_term', 2011, 'second_result_term', 'default_term') from default) as table"
        verify_1 = "select case when join_yr = 2010 then 'first_result_term' when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        query_2 = "select decode(dd.join_yr, 2010, 'first_result_term', 2011, 'second_result_term', 'default_term') from (select d.join_yr from default d) as dd"
        verify_2 = "select case when join_yr = 2010 then 'first_result_term' when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode((select raw join_yr from default d)[0], 2010, 'first_result_term', 2011, 'second_result_term', 'default_term')"
        verify_3 = "select 'second_result_term'"

        query_4 = "select decode(join_yr, 2010, (select join_yr, join_mo, join_day from default d limit 3), 2011, 'second_result_term', 'default_term') from default"
        verify_4 = "select case when join_yr = 2010 then (select join_yr, join_mo, join_day from default d limit 3) when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}

        self.query_runner(queries)

    def test_decode_missing_and_nulls(self):
        queries = dict()

        query_1 = "select decode(field_that_is_missing, missing, 'first_result_term', 'default_term') from default"
        verify_1 = "select case when field_that_is_missing is missing then 'first_result_term' else 'default_term' end from default"

        query_2 = "select decode(join_yr, missing, '', 'default_term') from default"
        verify_2 = "select case when join_yr is missing then '' else 'default_term' end from default"

        query_3 = "select decode(field_that_is_missing, not missing, 'first_result_term', 'default_term') from default"
        verify_3 = "select case when field_that_is_missing is not missing then 'first_result_term' else 'default_term' end from default"

        query_4 = "select decode(join_yr, null, '', 'default_term') from default"
        verify_4 = "select case when join_yr is null then '' else 'default_term' end from default"

        query_5 = "select decode(join_yr, not null, '', 'default_term') from default"
        verify_5 = "select case when join_yr is not null then '' else 'default_term' end from default"

        query_6 = "select decode(null, null, '', 'default_term') from default"
        verify_6 = "select case when null is null then '' else 'default_term' end from default"

        query_7 = "select decode(null, missing, '', 'default_term') from default"
        verify_7 = "select case when null is missing then '' else 'default_term' end from default"

        query_8 = "select decode(missing, null, '', 'default_term') from default"
        verify_8 = "select case when missing is null then '' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}
        queries["h"] = {"indexes": [self.primary_index_def], "queries": [query_8], "asserts": [self.verifier(verify_8, 0)]}
        self.query_runner(queries)

    def test_decode_nested(self):
        queries = dict()

        query_1 = "select decode(decode(join_yr, 2010, 'inner_2010'), 'inner_2010', 'outer_2010', 'default') from default"
        verify_1 = "select case when join_yr = 2010 then 'outer_2010' else 'default' end from default"

        query_2 = "select decode(decode(decode(join_yr, 2010, 'inner_2010'), 'inner_2010', 'outer_2010', 'default'), 'outer_2010', 'super_outer_2010', 'default', 'super_outer_default', 'double_default' ) from default"
        verify_2 = "select case when join_yr = 2010 then 'super_outer_2010' when join_yr = 2011 then 'super_outer_default' else 'double_default' end from default"

        query_3 = "select decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok','not ok')))))))))) from default"
        verify_3 = "select 'not ok' from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1],
                        "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2],
                        "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3],
                        "asserts": [self.verifier(verify_3, 0)]}

        self.query_runner(queries)

    def test_decode_indexes(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        index_2 = {'name': 'idx2', 'bucket': 'default', 'fields': [("join_yr", 0), ("join_mo", 1), ("join_day", 2)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(join_yr, 2010, 'first_result_term', 'default_term') from default"
        verify_1 = "select case when join_yr = 2010 then 'first_result_term' else 'default_term' end from default"
        explain_1 = "explain " + query_1

        query_2 = "select decode(join_yr, 2010, 'first_result_term', 'default_term') from default where join_yr is not null"
        verify_2 = "select case when join_yr = 2010 then 'first_result_term' else 'default_term' end from default where join_yr is not null"
        explain_2 = "explain " + query_2

        query_3 = "select decode(join_yr, 2010, join_mo, 'default_term') from default where join_yr is not null"
        verify_3 = "select case when join_yr = 2010 then join_mo else 'default_term' end from default where join_yr is not null"
        explain_3 = "explain " + query_3

        query_4 = "select decode(join_yr, 2010, join_day, 'default_term') from default where join_yr is not null"
        verify_4 = "select case when join_yr = 2010 then join_day else 'default_term' end from default where join_yr is not null"
        explain_4 = "explain " + query_4

        queries["a"] = {"indexes": [self.primary_index_def, index_1, index_2], "queries": [query_1, explain_1],
                        "asserts": [self.verifier(verify_1, 0), self.plan_verifier("index", "#primary", 1)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1, index_2], "queries": [query_2, explain_2],
                        "asserts": [self.verifier(verify_2, 0), self.plan_verifier("index", "idx1", 1)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1, index_2], "queries": [query_3, explain_3],
                        "asserts": [self.verifier(verify_3, 0), self.plan_verifier("index", "idx2", 1)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1, index_2], "queries": [query_4, explain_4],
                        "asserts": [self.verifier(verify_4, 0), self.plan_verifier("index", "idx2", 1)]}

        self.query_runner(queries)

    def test_decode_joins(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(join_yr, 2010, 'first_result_term', 2011, 'second_result_term', 'default') from (select join_yr from default order by meta().id limit 10) d inner join (select join_yr from default order by meta().id limit 10) dd on (d.join_yr == dd.join_yr)"
        verify_1 = "select case when join_yr = 2010 then 'first_result_term' when join_yr = 2011 then 'second_result_term' else 'default_term' end from (select join_yr from default order by meta().id limit 10) d inner join (select join_yr from default order by meta().id limit 10) dd on (d.join_yr == dd.join_yr)"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}

        self.query_runner(queries)

    def test_decode_datatypes(self):
        queries = dict()

        query_1 = "select decode(name, 'employee-9', 'first_result_term', 'employee-8', 'second_result_term', 'default_term') from default"
        verify_1 = "select case when name = 'employee-9' then 'first_result_term' when name = 'employee-8' then 'second_result_term' else 'default_term' end from default"

        query_2 = "select decode(skills, ['skill2010'], 'first_result_term', ['skill2010', 'skill2011'], 'second_result_term', 'default_term') from default"
        verify_2 = "select case when skills = ['skill2010'] then 'first_result_term' when skills = ['skill2010', 'skill2011'] then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(tasks_points, {'task1': 2, 'task2': 2}, 'first_result_term', {'task1': 1, 'task2': 1}, 'second_result_term','default_term') from default"
        verify_3 = "select case when tasks_points = {'task1': 2, 'task2': 2} then 'first_result_term' when tasks_points = {'task1': 1, 'task2': 1} then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(VMs, [{'RAM': 10,'memory': 10,'name': 'vm_10','os': 'ubuntu'},{'RAM': 10,'memory': 10,'name': 'vm_11','os': 'windows'}], 'first_result_term', 'default_term') from default"
        verify_4 = "select case when VMs = [{'RAM': 10,'memory': 10,'name': 'vm_10','os': 'ubuntu'},{'RAM': 10,'memory': 10,'name': 'vm_11','os': 'windows'}] then 'first_result_term' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1],
                        "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2],
                        "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3],
                        "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4],
                        "asserts": [self.verifier(verify_4, 0)]}

        self.query_runner(queries)

    def test_decode_meta(self):
        queries = dict()

        query_1 = "select decode(meta().expiration, 1, 'first_result_term', 0, 'second_result_term', 'default_term') from default"
        verify_1 = "select case when meta().expiration = 1 then 'first_result_term' when meta().expiration = 0 then 'second_result_term' else 'default_term' end from default"

        query_2 = "select decode(meta().flags, 1, 'first_result_term', 4042322160, 'second_result_term', 'default_term') from default"
        verify_2 = "select case when meta().flags = 1 then 'first_result_term' when meta().flags = 4042322160 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(meta().type, 'binary', 'first_result_term', 'json', 'second_result_term', 'default_term') from default"
        verify_3 = "select case when meta().type = 'binary' then 'first_result_term' when meta().type = 'json' then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(meta().id, 'query-testemployee10153.1877827-0', 'first_result_term', 'query-testemployee10194.855617-0', 'second_result_term', 'default_term') from default"
        verify_4 = "select case when meta().id = 'query-testemployee10153.1877827-0' then 'first_result_term' when meta().id = 'query-testemployee10194.855617-0' then 'second_result_term' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}

        self.query_runner(queries)

    def test_array_binary_search_basic(self):
        queries = dict()

        query_1 = "select array_binary_search([0,0,0,1,1,1,2,2,2], 0)"
        verify_1 = "select 0"

        query_2 = "select array_binary_search([0,0,0,1,1,1,2,2,2], 1)"
        verify_2 = "select 3"

        query_3 = "select array_binary_search([0,0,0,1,1,1,2,2,2], 2)"
        verify_3 = "select 6"

        query_4 = "select array_binary_search([0,0,1,1,1,2,2,2], 0)"
        verify_4 = "select 0"

        query_5 = "select array_binary_search([0,0,1,1,1,2,2,2], 1)"
        verify_5 = "select 2"

        query_6 = "select array_binary_search([0,0,1,1,1,2,2,2], 2)"
        verify_6 = "select 5"

        query_7 = "select array_binary_search([0,0,1,1,2,2,2], 0)"
        verify_7 = "select 0"

        query_8 = "select array_binary_search([0,0,1,1,2,2,2], 1)"
        verify_8 = "select 2"

        query_9 = "select array_binary_search([0,0,1,1,2,2,2], 2)"
        verify_9 = "select 4"

        query_10 = "select array_binary_search([0,0,1,1,2,2], 0)"
        verify_10 = "select 0"

        query_11 = "select array_binary_search([0,0,1,1,2,2], 1)"
        verify_11 = "select 2"

        query_12 = "select array_binary_search([0,0,1,1,2,2], 2)"
        verify_12 = "select 4"

        query_13 = "select array_binary_search([0,1,1,2,2], 0)"
        verify_13 = "select 0"

        query_14 = "select array_binary_search([0,1,1,2,2], 1)"
        verify_14 = "select 1"

        query_15 = "select array_binary_search([0,1,1,2,2], 2)"
        verify_15 = "select 3"

        query_16 = "select array_binary_search([0,1,2,2], 0)"
        verify_16 = "select 0"

        query_17 = "select array_binary_search([0,1,2,2], 1)"
        verify_17 = "select 1"

        query_18 = "select array_binary_search([0,1,2,2], 2)"
        verify_18 = "select 2"

        query_19 = "select array_binary_search([0,1,2], 0)"
        verify_19 = "select 0"

        query_20 = "select array_binary_search([0,1,2], 1)"
        verify_20 = "select 1"

        query_21 = "select array_binary_search([0,1,2], 2)"
        verify_21 = "select 2"

        query_22 = "select array_binary_search([0,0,1,1,2], 1)"
        verify_22 = "select 2"

        query_23 = "select array_binary_search([0,1,1,2,2], 1)"
        verify_23 = "select 1"

        query_24 = "select array_binary_search([0,1,1,2,2], -1)"
        verify_24 = "select -1"

        query_25 = "select array_binary_search([0,1,1,2,2], 3)"
        verify_25 = "select -1"

        query_26 = "select array_binary_search([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0], 0)"
        verify_26 = "select 0"

        query_27 = "select array_binary_search([], 0)"
        verify_27 = "select -1"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}
        queries["h"] = {"indexes": [self.primary_index_def], "queries": [query_8], "asserts": [self.verifier(verify_8, 0)]}
        queries["i"] = {"indexes": [self.primary_index_def], "queries": [query_9], "asserts": [self.verifier(verify_9, 0)]}
        queries["j"] = {"indexes": [self.primary_index_def], "queries": [query_10], "asserts": [self.verifier(verify_10, 0)]}
        queries["k"] = {"indexes": [self.primary_index_def], "queries": [query_11], "asserts": [self.verifier(verify_11, 0)]}
        queries["l"] = {"indexes": [self.primary_index_def], "queries": [query_12], "asserts": [self.verifier(verify_12, 0)]}
        queries["m"] = {"indexes": [self.primary_index_def], "queries": [query_13], "asserts": [self.verifier(verify_13, 0)]}
        queries["n"] = {"indexes": [self.primary_index_def], "queries": [query_14], "asserts": [self.verifier(verify_14, 0)]}
        queries["o"] = {"indexes": [self.primary_index_def], "queries": [query_15], "asserts": [self.verifier(verify_15, 0)]}
        queries["p"] = {"indexes": [self.primary_index_def], "queries": [query_16], "asserts": [self.verifier(verify_16, 0)]}
        queries["q"] = {"indexes": [self.primary_index_def], "queries": [query_17], "asserts": [self.verifier(verify_17, 0)]}
        queries["r"] = {"indexes": [self.primary_index_def], "queries": [query_18], "asserts": [self.verifier(verify_18, 0)]}
        queries["s"] = {"indexes": [self.primary_index_def], "queries": [query_19], "asserts": [self.verifier(verify_19, 0)]}
        queries["t"] = {"indexes": [self.primary_index_def], "queries": [query_20], "asserts": [self.verifier(verify_20, 0)]}
        queries["u"] = {"indexes": [self.primary_index_def], "queries": [query_21], "asserts": [self.verifier(verify_21, 0)]}
        queries["v"] = {"indexes": [self.primary_index_def], "queries": [query_22], "asserts": [self.verifier(verify_22, 0)]}
        queries["w"] = {"indexes": [self.primary_index_def], "queries": [query_23], "asserts": [self.verifier(verify_23, 0)]}
        queries["x"] = {"indexes": [self.primary_index_def], "queries": [query_24], "asserts": [self.verifier(verify_24, 0)]}
        queries["y"] = {"indexes": [self.primary_index_def], "queries": [query_25], "asserts": [self.verifier(verify_25, 0)]}
        queries["z"] = {"indexes": [self.primary_index_def], "queries": [query_26], "asserts": [self.verifier(verify_26, 0)]}
        queries["aa"] = {"indexes": [self.primary_index_def], "queries": [query_27], "asserts": [self.verifier(verify_27, 0)]}

        self.query_runner(queries)

    def test_array_binary_search_datatypes(self):
        queries = dict()

        query_1 = "select array_binary_search(['a','a','a','b','b','b','c','c','c'], 'a')"
        verify_1 = "select 0"

        query_2 = "select array_binary_search(['a','a','a','b','b','b','c','c','c'], 'b')"
        verify_2 = "select 3"

        query_3 = "select array_binary_search(['a','a','a','b','b','b','c','c','c'], 'c')"
        verify_3 = "select 6"

        query_4 = "select array_binary_search(['a','a','a','b','b','b','c','c','c'], '')"
        verify_4 = "select -1"

        query_5 = "select array_binary_search(['a','a','a','b','b','b','c','c','c'], 'd')"
        verify_5 = "select -1"

        query_6 = "select array_binary_search(['a','a','a','a','a','a','a','a','a'], 'a')"
        verify_6 = "select 0"

        query_7 = "select array_binary_search([], 'a')"
        verify_7 = "select -1"

        query_8 = "select array_binary_search([[], [0], [0,1], [1,0], [1,0,0]], [0,1])"
        verify_8 = "select 2"

        query_9 = "select array_binary_search([[], [0], [1], [1,0], [1,0,0]], [1])"
        verify_9 = "select 2"

        query_10 = "select array_binary_search([[], [0], [1], [1,0], [1,0,0]], [1,0])"
        verify_10 = "select 3"

        query_11 = "select array_binary_search([[], [0], [1], [1,0], [1,0,0]], [])"
        verify_11 = "select 0"

        query_12 = "select array_binary_search([[], [0], [1], [1,0], [1,0,0]], [1,0,0,0])"
        verify_12 = "select -1"

        query_13 = "select array_binary_search([[], [], [], [], [], [], []], [])"
        verify_13 = "select 0"

        query_14 = "select array_binary_search([], [])"
        verify_14 = "select -1"

        query_15 = "select array_binary_search([{},{'a':0},{'b':1},{'b':1},{'b':1},{'c':0},{'c':1}], {'b':1})"
        verify_15 = "select 2"

        query_16 = "select array_binary_search([{},{'a':0},{'a':1},{'b':1},{'b':1},{'c':0},{'c':1}], {'b':1})"
        verify_16 = "select 3"

        query_17 = "select array_binary_search([{},{'a':0},{'a':1},{'b':0},{'b':1},{'c':0},{'c':1}], {'b':1})"
        verify_17 = "select 4"

        query_18 = "select array_binary_search([{},{'a':0},{'a':1},{'b':0},{'b':1},{'c':0},{'c':1}], {})"
        verify_18 = "select 0"

        query_19 = "select array_binary_search([{},{'a':0},{'a':1},{'b':0},{'b':1},{'c':0},{'c':1}], {'c':1})"
        verify_19 = "select 6"

        query_20 = "select array_binary_search([{},{'a':0},{'a':1},{'b':0},{'b':1},{'c':0},{'c':1}], {'d':1})"
        verify_20 = "select -1"

        query_21 = "select array_binary_search([{},{},{},{},{},{},{}], {})"
        verify_21 = "select 0"

        query_22 = "select array_binary_search([], {})"
        verify_22 = "select -1"

        query_23 = "select array_binary_search([false, false, false, true, true, true], false)"
        verify_23 = "select 0"

        query_24 = "select array_binary_search([false, false, false, true, true, true], true)"
        verify_24 = "select 3"

        query_25 = "select array_binary_search([false, false, false, false, true, true], false)"
        verify_25 = "select 0"

        query_26 = "select array_binary_search([false, false, false, false], false)"
        verify_26 = "select 0"

        query_27 = "select array_binary_search([false, false, false, false], true)"
        verify_27 = "select -1"

        query_28 = "select array_binary_search([], true)"
        verify_28 = "select -1"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}
        queries["h"] = {"indexes": [self.primary_index_def], "queries": [query_8], "asserts": [self.verifier(verify_8, 0)]}
        queries["i"] = {"indexes": [self.primary_index_def], "queries": [query_9], "asserts": [self.verifier(verify_9, 0)]}
        queries["j"] = {"indexes": [self.primary_index_def], "queries": [query_10], "asserts": [self.verifier(verify_10, 0)]}
        queries["k"] = {"indexes": [self.primary_index_def], "queries": [query_11], "asserts": [self.verifier(verify_11, 0)]}
        queries["l"] = {"indexes": [self.primary_index_def], "queries": [query_12], "asserts": [self.verifier(verify_12, 0)]}
        queries["m"] = {"indexes": [self.primary_index_def], "queries": [query_13], "asserts": [self.verifier(verify_13, 0)]}
        queries["n"] = {"indexes": [self.primary_index_def], "queries": [query_14], "asserts": [self.verifier(verify_14, 0)]}
        queries["o"] = {"indexes": [self.primary_index_def], "queries": [query_15], "asserts": [self.verifier(verify_15, 0)]}
        queries["p"] = {"indexes": [self.primary_index_def], "queries": [query_16], "asserts": [self.verifier(verify_16, 0)]}
        queries["q"] = {"indexes": [self.primary_index_def], "queries": [query_17], "asserts": [self.verifier(verify_17, 0)]}
        queries["r"] = {"indexes": [self.primary_index_def], "queries": [query_18], "asserts": [self.verifier(verify_18, 0)]}
        queries["s"] = {"indexes": [self.primary_index_def], "queries": [query_19], "asserts": [self.verifier(verify_19, 0)]}
        queries["t"] = {"indexes": [self.primary_index_def], "queries": [query_20], "asserts": [self.verifier(verify_20, 0)]}
        queries["u"] = {"indexes": [self.primary_index_def], "queries": [query_21], "asserts": [self.verifier(verify_21, 0)]}
        queries["v"] = {"indexes": [self.primary_index_def], "queries": [query_22], "asserts": [self.verifier(verify_22, 0)]}
        queries["w"] = {"indexes": [self.primary_index_def], "queries": [query_23], "asserts": [self.verifier(verify_23, 0)]}
        queries["x"] = {"indexes": [self.primary_index_def], "queries": [query_24], "asserts": [self.verifier(verify_24, 0)]}
        queries["y"] = {"indexes": [self.primary_index_def], "queries": [query_25], "asserts": [self.verifier(verify_25, 0)]}
        queries["z"] = {"indexes": [self.primary_index_def], "queries": [query_26], "asserts": [self.verifier(verify_26, 0)]}
        queries["aa"] = {"indexes": [self.primary_index_def], "queries": [query_27], "asserts": [self.verifier(verify_27, 0)]}
        queries["bb"] = {"indexes": [self.primary_index_def], "queries": [query_28], "asserts": [self.verifier(verify_28, 0)]}

        self.query_runner(queries)

    def test_array_binary_search_indexes(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("skills", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        index_2 = {'name': 'idx2', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        index_3 = {'name': 'idx3', 'bucket': 'default', 'fields': [("join_yr", 0), ("join_day", 1), ("skills", 2)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select * from default where ARRAY_BINARY_SEARCH(skills, 'skill2010') = 0"
        verify_1 = "select * from default where skills[0] = 'skill2010'"
        explain_1 = "explain " + query_1

        query_2 = "select * from default where ARRAY_BINARY_SEARCH(skills, 'skill2011') = 1"
        verify_2 = "select * from default where skills[1] = 'skill2011'"
        explain_2 = "explain " + query_2

        query_3 = "select * from default where ARRAY_BINARY_SEARCH(skills, 'skill2012') = -1"
        verify_3 = "select * from default where 'skill2012' not in skills"
        explain_3 = "explain " + query_3

        query_4 = "select ARRAY_BINARY_SEARCH(skills, 'skill2012') from default where join_yr is not missing"
        verify_4 = "select -1 from default where join_yr is not missing"
        explain_4 = "explain " + query_4

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1, explain_1],
                         "asserts": [self.verifier(verify_1, 0), self.plan_verifier("index", "idx1", 1)]}

        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2, explain_2],
                        "asserts": [self.verifier(verify_2, 0), self.plan_verifier("index", "idx1", 1)]}

        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3, explain_3],
                        "asserts": [self.verifier(verify_3, 0), self.plan_verifier("index", "idx1", 1)]}

        queries["d"] = {"indexes": [self.primary_index_def, index_2, index_3], "queries": [query_4, explain_4],
                        "asserts": [self.verifier(verify_4, 0), self.plan_verifier("index", "idx3", 1)]}

        self.query_runner(queries)

    def test_array_binary_search_nested(self):
        queries = dict()

        query_1 = "select array_binary_search([array_binary_search(skills, 'noskills'), array_binary_search(skills,'skill2010'), array_binary_search(skills,'skill2011')], -1) from default"
        verify_1 = "select 0 from default"

        query_2 = "select array_binary_search([array_binary_search(skills, 'noskills'), array_binary_search(skills,'skill2010'), array_binary_search(skills,'skill2011')], 0) from default"
        verify_2 = "select 1 from default"

        query_3 = "select array_binary_search([array_binary_search(skills, 'noskills'), array_binary_search(skills,'skill2010'), array_binary_search(skills,'skill2011')], 1) from default"
        verify_3 = "select 2 from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1],
                        "asserts": [self.verifier(verify_1, 0)]}

        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2],
                        "asserts": [self.verifier(verify_2, 0)]}

        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3],
                        "asserts": [self.verifier(verify_3, 0)]}

        self.query_runner(queries)

    def test_array_binary_search_pushdown(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("skills", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select array_binary_search([0, COUNT(skills)], 2016) from default"
        explain_1 = "explain " + query_1
        verify_1 = "select 1"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1, explain_1],
                        "asserts": [self.verifier(verify_1, 0), self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        self.query_runner(queries)

    def test_array_binary_search_functions(self):
        queries = dict()

        query_1 = "select ln(array_binary_search([0, 1], 1))"
        verify_1 = "select 0"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1],
                        "asserts": [self.verifier(verify_1, 0)]}

        self.query_runner(queries)

    def test_array_binary_search_subquery(self):
        queries = dict()

        query_1 = "select table.* from (select array_binary_search(skills, 'skill2010') from default) as table"
        verify_1 = "select 0 from default"

        query_2 = "select array_binary_search((select distinct raw join_day from default order by join_day), 1) from default d"
        verify_2 = "select 0 from default"

        query_3 = "select array_binary_search((select distinct raw join_day from default order by join_day), (select raw 1)[0]) from default d"
        verify_3 = "select 0 from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}

        self.query_runner(queries)

    def test_array_binary_search_missing_and_nulls(self):
        queries = dict()

        query_1 = "select array_binary_search([missing, missing, missing], missing) from default"
        verify_1 = "select 0 from default"

        query_2 = "select array_binary_search([field_that_is_missing, field_that_is_missing, field_that_is_missing], missing) from default"
        verify_2 = "select 0 from default"

        query_3 = "select array_binary_search([field_that_is_missing,join_yr, join_day], missing) from default"
        verify_3 = "select 0 from default"

        query_4 = "select array_binary_search([missing, missing, missing], not missing) from default"
        verify_4 = "select 0 from default"

        query_5 = "select array_binary_search([null, null, null], null) from default"
        verify_5 = "select 0 from default"

        query_6 = "select array_binary_search([null, join_yr, join_day], null) from default"
        verify_6 = "select 0 from default"

        query_7 = "select array_binary_search([null, null, null], not null) from default"
        verify_7 = "select 0 from default"

        query_8 = "select array_binary_search([null, null, null], missing) from default"
        verify_8 = "select -1 from default"

        query_9 = "select array_binary_search([missing, missing, missing], null) from default"
        verify_9 = "select -1 from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}
        queries["h"] = {"indexes": [self.primary_index_def], "queries": [query_8], "asserts": [self.verifier(verify_8, 0)]}
        queries["i"] = {"indexes": [self.primary_index_def], "queries": [query_9], "asserts": [self.verifier(verify_9, 0)]}

        self.query_runner(queries)


    def test_decode_meta(self):
        queries = dict()

        query_1 = "select array_binary_search((select raw meta().expiration from default), 0)"
        verify_1 = "select 0"

        query_2 = "select array_binary_search((select raw meta().expiration from default), 1)"
        verify_2 = "select -1"

        query_3 = "select array_binary_search([meta().expiration-1, meta().expiration, meta().expiration+1], 0) from default"
        verify_3 = "select 1 from default"

        query_4 = "select array_binary_search((select raw meta().cas from default), 0)"
        verify_4 = "select -1"

        query_5 = "select array_binary_search([meta().cas-1, meta().cas, meta().cas+1], meta().cas) from default"
        verify_5 = "select 1 from default"

        query_6 = "select array_binary_search((select raw meta().type from default), 'json')"
        verify_6 = "select 0"

        query_7 = "select array_binary_search([meta().type, meta().type, meta().type], meta().type) from default"
        verify_7 = "select 0 from default"

        query_8 = "select array_binary_search((select raw meta().id from default order by meta().id), 'query-testemployee10153.1877827-0')"
        verify_8 = "select 0"

        query_9 = "select array_binary_search([meta().id, meta().id, meta().id], meta().id) from default"
        verify_9 = "select 0 from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}
        queries["h"] = {"indexes": [self.primary_index_def], "queries": [query_8], "asserts": [self.verifier(verify_8, 0)]}
        queries["i"] = {"indexes": [self.primary_index_def], "queries": [query_9], "asserts": [self.verifier(verify_9, 0)]}

        self.query_runner(queries)
