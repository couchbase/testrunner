from deepdiff import DeepDiff

from .tuq import QueryTests


class QueryINDEXUNNESTTests(QueryTests):
    def setUp(self):
        super(QueryINDEXUNNESTTests, self).setUp()
        self.log.info("==============  QueryINDEXUNNESTTests setup has started ==============")
        self.query_bucket = self.get_query_buckets()[0]
        self.primary_indx_def = {'name': '#primary',
                                 'bucket': 'default',
                                 'fields': [],
                                 'state': 'online',
                                 'using': self.index_type.lower(),
                                 'is_primary': True}
        self.log.info("==============  QueryINDEXUNNESTTests setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryINDEXUNNESTTests tearDown has started ==============")
        self.log.info("==============  QueryINDEXUNNESTTests tearDown has completed ==============")
        super(QueryINDEXUNNESTTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryINDEXUNNESTTests suite_tearDown has started ==============")
        self.log.info("==============  QueryINDEXUNNESTTests suite_tearDown has completed ==============")
        super(QueryINDEXUNNESTTests, self).suite_tearDown()

    def verifier(self, compare_query, query_index):
        return lambda x: self.compare_queries(x['q_res'][query_index], compare_query)

    def plan_verifier(self, keyword, query_index):
        return lambda x: self.explain_check(x, keyword, query_index)

    def explain_check(self, results, keywords, query_index):
        explain_plan = self.ExplainPlanHelper(results['q_res'][query_index])
        self.assertTrue(keywords[0] in str(explain_plan['~children'][0]['spans'])
                        and keywords[1] in str(explain_plan['~children'][0]['spans']),
                        "The non-array index keys are not being used")

    def compare_queries(self, actual_results, compare_query):
        let_letting_docs = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(let_letting_docs), len(compare_docs))
        self.assertEqual(sorted(let_letting_docs), sorted(compare_docs))

    def test_simple_2(self):
        queries = dict()

        index_1 = {'name': 'idx',
                   'bucket': 'default',
                   'fields': (("ALL ARRAY v1 FOR v1 IN VMs END", 0), ("email", 1), ("department", 2)),
                   'state': 'online',
                   'using': self.index_type.lower(),
                   'is_primary': False}

        query_1 = 'SELECT v1 FROM ' + self.query_bucket + 'AS d UNNEST d.VMs AS v1 where d.email == ' \
                                                          '"9-mail@couchbase.com" and d.department == "support" and ' \
                                                          'v1.RAM == 10 '
        explain_1 = "Explain " + query_1
        verify_1 = 'SELECT v1 FROM ' + self.query_bucket + 'AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                                                           'd.email == "9-mail@couchbase.com" and d.department == ' \
                                                           '"Support" and v1.RAM == 10 '

        queries["a"] = {"indexes": [self.primary_indx_def, index_1], "queries": [explain_1, query_1],
                        "asserts": [self.plan_verifier(['9-mail@couchbase.com', 'support'], 0),
                                    self.verifier(verify_1, 1)]}

        self.query_runner(queries)

    def test_simple(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where d.email == ' \
                                                            '"9-mail@couchbase.com" and d.department == "support" ' \
                                                            'and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['spans'])
                            and 'support' in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where d.email == "9-mail@couchbase.com" and ' \
                                                                    'd.department == "Support" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_arbitrary_alias(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT nv1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS nv1 where d.email == ' \
                                                             '"9-mail@couchbase.com" and d.department == "support" ' \
                                                             'and nv1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('nv1' in str(plan['~children'][0]['covers'][0]) and
                            plan['~children'][0]['index'] == 'array_index',
                            "The non-array index keys are not being used")
            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT nv1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS ' \
                                                                     'nv1 where d.email == "9-mail@couchbase.com" and' \
                                                                     ' d.department == "Support" and nv1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_str_functions(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY v1 FOR v1 IN VMs END,lower(email)," \
                                                                     "upper(department)) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where lower(d.email) == ' \
                                                            '"9-mail@couchbase.com" and upper(d.department) == ' \
                                                            '"SUPPORT" and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where lower(d.email) == "9-mail@couchbase.com" ' \
                                                                    'and upper(d.department) == "SUPPORT" and v1.RAM ' \
                                                                    '== 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_int_functions(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY v1 FOR v1 IN VMs END,exp(join_mo)," \
                                                                     "upper(department)) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where exp(d.join_mo) == 81 and ' \
                                                            'upper(d.department) == "SUPPORT" and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('81' in str(plan['~children'][0]['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'exp(d.join_mo) == 81 and upper(d.department) == ' \
                                                                    '"SUPPORT" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    ##############################################################################################
    #
    #  Distinct
    ##############################################################################################

    def test_distinct_simple(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY v1 FOR v1 IN VMs END,email," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where d.email == ' \
                                                            '"9-mail@couchbase.com" and d.department == "support" ' \
                                                            'and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'support' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'd.email == "9-mail@couchbase.com" and ' \
                                                                    'd.department == "Support" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_distinct_arbitrary_simple(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY v1 FOR v1 IN VMs END,email," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT nv1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS nv1 where d.email == ' \
                                                             '"9-mail@couchbase.com" and d.department == "support" ' \
                                                             'and nv1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'support' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'd.email == "9-mail@couchbase.com" and ' \
                                                                    'd.department == "Support" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_distinct_str_functions(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY v1 FOR v1 IN VMs END,lower(" \
                                                                     "email),upper(department)) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where lower(d.email) == ' \
                                                            '"9-mail@couchbase.com" and upper(d.department) == ' \
                                                            '"SUPPORT" and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'lower(d.email) == "9-mail@couchbase.com" and ' \
                                                                    'upper(d.department) == "SUPPORT" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_distinct_int_functions(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY v1 FOR v1 IN VMs END,exp(" \
                                                                     "join_mo),upper(department)) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where exp(d.join_mo) == 81 and ' \
                                                            'upper(d.department) == "SUPPORT" and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('81' in str(plan['~children'][0]['scan']['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'exp(d.join_mo) == 81 and upper(d.department) == ' \
                                                                    '"SUPPORT" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    ##############################################################################################
    #
    #  Nested Unnest
    ##############################################################################################

    def test_arbitrary_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY (ALL ARRAY v2.region1 FOR v2 IN " \
                                                                     "v1.Marketing END)  FOR v1 IN tasks END,email," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS nv1 UNNEST nv1.Marketing as nv2 ' \
                                                           'where nv2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_distinct_arbitrary_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY (DISTINCT ARRAY v2.region1 FOR " \
                                                                     "v2 IN v1.Marketing END)  FOR v1 IN tasks END," \
                                                                     "email,department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS nv1 UNNEST nv1.Marketing as nv2 ' \
                                                           'where nv2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_arbitrary_distinct_all_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY (ALL ARRAY v2.region1 FOR v2 " \
                                                                     "IN v1.Marketing END)  FOR v1 IN tasks " \
                                                                     "END,email,department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS nv1 UNNEST nv1.Marketing as nv2 ' \
                                                           'where nv2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY (ALL ARRAY v2.region1 FOR v2 IN " \
                                                                     "v1.Marketing END)  FOR v1 IN tasks END,email," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 ' \
                                                           'where v2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_distinct_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY (DISTINCT ARRAY v2.region1 FOR " \
                                                                     "v2 IN v1.Marketing END)  FOR v1 IN tasks END," \
                                                                     "email,department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 ' \
                                                           'where v2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_distinct_all_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY (ALL ARRAY v2.region1 FOR v2 " \
                                                                     "IN v1.Marketing END)  FOR v1 IN tasks " \
                                                                     "END,email,department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 ' \
                                                           'where v2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    ##############################################################################################
    #
    #  Negative
    ##############################################################################################

    def test_neg_distinct_simple(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(DISTINCT ARRAY v1 FOR v1 IN VMs END,email," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where d.email == ' \
                                                            '"9-mail@couchbase.com" and d.department == "support" ' \
                                                            'and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'support' in str(plan['~children'][0]['scan']['spans'])
                            and '10' not in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'd.email == "9-mail@couchbase.com" and ' \
                                                                    'd.department == "Support" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_neg_simple(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(ALL ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d UNNEST d.VMs AS v1 where d.email == ' \
                                                            '"9-mail@couchbase.com" and d.department == "support" ' \
                                                            'and v1.RAM == 10 '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['spans'])
                            and 'support' in str(plan['~children'][0]['spans'])
                            and '10' not in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.VMs AS v1 ' \
                                                                    'where ' \
                                                                    'd.email == "9-mail@couchbase.com" and ' \
                                                                    'd.department == "Support" and v1.RAM == 10 '
            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_neg_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(email,ALL ARRAY (ALL ARRAY v2.region1 FOR v2 " \
                                                                     "IN v1.Marketing END)  FOR v1 IN tasks END," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2  ' \
                                                           'where v2.region1 == "South" and d.department ==  ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' not in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_arbitrary_neg_nested_unnest(self):
        index = "CREATE INDEX array_index ON " + self.query_bucket + "(email,ALL ARRAY (ALL ARRAY v2.region1 FOR v2 " \
                                                                     "IN v1.Marketing END)  FOR v1 IN tasks END," \
                                                                     "department) "
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM ' + self.query_bucket + ' AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as nv2 ' \
                                                           'where nv2.region1 == "South" and d.department == ' \
                                                           '"Developer" and d.email == "1-mail@couchbase.com" '
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' not in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM ' + self.query_bucket + ' AS d use index (`#primary`) UNNEST d.tasks AS v1 ' \
                                                                   'UNNEST v1.Marketing ' \
                                                                   'as v2 where v2.region1 == "South" and ' \
                                                                   'd.department == "Developer" and d.email == ' \
                                                                   '"1-mail@couchbase.com" '

            expected_results = self.run_cbq_query(query=primary_query)

            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX array_index ON " + self.query_bucket)

    def test_MB63414(self):
        self.run_cbq_query(f'upsert into {self.query_bucket} values("k01", {{"c1": 1, "a1": [{{"ac1":1}}, {{"ac1":1}}, {{"ac1":1}}, {{"ac1":1}}]}})')
        self.run_cbq_query(f'upsert into {self.query_bucket} values("k02", {{"c1": 1, "a1": [{{"ac1":0}}, {{"ac1":0}}, {{"ac1":1}}, {{"ac1":1}}]}})')
        self.run_cbq_query(f'upsert into {self.query_bucket} values("k03", {{"c1": 2, "a1": [{{"ac1":1}}, {{"ac1":1}}, {{"ac1":1}}, {{"ac1":1}}]}})')
        self.run_cbq_query(f'upsert into {self.query_bucket} values("k04", {{"c1": 2, "a1": [{{"ac1":0}}, {{"ac1":0}}, {{"ac1":1}}, {{"ac1":1}}]}})')

        # Create index
        self.run_cbq_query(f'create index ix20 on {self.query_bucket} (ALL ARRAY v.ac1 FOR v IN a1 END)')
        unnest_query_unnest = f'select * FROM {self.query_bucket} AS d UNNEST d.a1 AS v WHERE v.ac1 > 0'
        unnest_query_array = f'select * FROM {self.query_bucket} AS d WHERE ANY v IN d.a1 SATISFIES v.ac1 > 0 END'
        self.run_cbq_query(unnest_query_unnest, query_params={'memory_quota':1024})
        self.run_cbq_query(unnest_query_array, query_params={'memory_quota':1024})

        # create index
        self.run_cbq_query(f'create index ix21 on {self.query_bucket} (c1, ALL ARRAY v.ac1 FOR v IN a1 END)')
        unnest_query_any = f'select c1, COUNT(price) AS cnt FROM {self.query_bucket} AS d WHERE c1 > 0 GROUP BY c1'
        self.run_cbq_query(unnest_query_any, query_params={'memory_quota':1024})