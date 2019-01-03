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

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

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

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}
        queries["e"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_5], "asserts": [self.verifier(verify_5, 0)]}
        queries["f"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_6], "asserts": [self.verifier(verify_6, 0)]}
        queries["g"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_7], "asserts": [self.verifier(verify_7, 0)]}

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

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select table.* from (select decode(join_yr, 2010, 'first_result_term', 2011, 'second_result_term', 'default_term') from default) as table"
        verify_1 = "select case when join_yr = 2010 then 'first_result_term' when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        query_2 = "select decode(dd.join_yr, 2010, 'first_result_term', 2011, 'second_result_term', 'default_term') from (select d.join_yr from default d) as dd"
        verify_2 = "select case when join_yr = 2010 then 'first_result_term' when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode((select raw join_yr from default d)[0], 2010, 'first_result_term', 2011, 'second_result_term', 'default_term')"
        verify_3 = "select 'second_result_term'"

        query_4 = "select decode(join_yr, 2010, (select join_yr, join_mo, join_day from default d limit 3), 2011, 'second_result_term', 'default_term') from default"
        verify_4 = "select case when join_yr = 2010 then (select join_yr, join_mo, join_day from default d limit 3) when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}

        self.query_runner(queries)

    def test_decode_missing_and_nulls(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(field_that_is_missing, missing, 'first_result_term', 'default_term') from default"
        verify_1 = "select case when field_that_is_missing is missing then 'first_result_term' else 'default_term' end from default"

        query_2 = "select decode(join_yr, missing, '', 'default_term') from default"
        verify_2 = "select case when join_yr is missing then '' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}

        self.query_runner(queries)

    def test_decode_nested(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(decode(join_yr, 2010, 'inner_2010'), 'inner_2010', 'outer_2010', 'default') from default"
        verify_1 = "select case when join_yr = 2010 then 'outer_2010' else 'default' end from default"

        query_2 = "select decode(decode(decode(join_yr, 2010, 'inner_2010'), 'inner_2010', 'outer_2010', 'default'), 'outer_2010', 'super_outer_2010', 'default', 'super_outer_default', 'double_default' ) from default"
        verify_2 = "select case when join_yr = 2010 then 'super_outer_2010' when join_yr = 2011 then 'super_outer_default' else 'double_default' end from default"

        query_3 = "select decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok', decode(join_yr, 0, 'ok','not ok')))))))))) from default"
        verify_3 = "select 'not ok' from default"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1],
                        "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2],
                        "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3],
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

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(name, 'employee-9', 'first_result_term', 'employee-8', 'second_result_term', 'default_term') from default"
        verify_1 = "select case when name = 'employee-9' then 'first_result_term' when name = 'employee-8' then 'second_result_term' else 'default_term' end from default"

        query_2 = "select decode(skills, ['skill2010'], 'first_result_term', ['skill2010', 'skill2011'], 'second_result_term', 'default_term') from default"
        verify_2 = "select case when skills = ['skill2010'] then 'first_result_term' when skills = ['skill2010', 'skill2011'] then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(tasks_points, {'task1': 2, 'task2': 2}, 'first_result_term', {'task1': 1, 'task2': 1}, 'second_result_term','default_term') from default"
        verify_3 = "select case when tasks_points = {'task1': 2, 'task2': 2} then 'first_result_term' when tasks_points = {'task1': 1, 'task2': 1} then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(VMs, [{'RAM': 10,'memory': 10,'name': 'vm_10','os': 'ubuntu'},{'RAM': 10,'memory': 10,'name': 'vm_11','os': 'windows'}], 'first_result_term', 'default_term') from default"
        verify_4 = "select case when VMs = [{'RAM': 10,'memory': 10,'name': 'vm_10','os': 'ubuntu'},{'RAM': 10,'memory': 10,'name': 'vm_11','os': 'windows'}] then 'first_result_term' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1],
                        "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2],
                        "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3],
                        "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_4],
                        "asserts": [self.verifier(verify_4, 0)]}

        self.query_runner(queries)

    def test_decode_meta(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "select decode(meta().expiration, 1, 'first_result_term', 0, 'second_result_term', 'default_term') from default"
        verify_1 = "select case when meta().expiration = 1 then 'first_result_term' when meta().expiration = 0 then 'second_result_term' else 'default_term' end from default"

        query_2 = "select decode(meta().flags, 1, 'first_result_term', 4042322160, 'second_result_term', 'default_term') from default"
        verify_2 = "select case when meta().flags = 1 then 'first_result_term' when meta().flags = 4042322160 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(meta().type, 'binary', 'first_result_term', 'json', 'second_result_term', 'default_term') from default"
        verify_3 = "select case when meta().type = 'binary' then 'first_result_term' when meta().type = 'json' then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(meta().id, 'query-testemployee10153.1877827-0', 'first_result_term', 'query-testemployee10194.855617-0', 'second_result_term', 'default_term') from default"
        verify_4 = "select case when meta().id = 'query-testemployee10153.1877827-0' then 'first_result_term' when meta().id = 'query-testemployee10194.855617-0' then 'second_result_term' else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1, 0)]}
        queries["b"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_2], "asserts": [self.verifier(verify_2, 0)]}
        queries["c"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_3], "asserts": [self.verifier(verify_3, 0)]}
        queries["d"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_4], "asserts": [self.verifier(verify_4, 0)]}

        self.query_runner(queries)

    #def test_decode_neg(self):
