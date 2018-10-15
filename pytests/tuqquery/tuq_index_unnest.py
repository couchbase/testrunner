from tuq import QueryTests

class QueryINDEXUNNESTTests(QueryTests):
    def setUp(self):
        super(QueryINDEXUNNESTTests, self).setUp()
        self.log.info("==============  QueryANSIJOINSTests setup has started ==============")
        self.log.info("==============  QueryANSIJOINSTests setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryANSIJOINSTests tearDown has started ==============")
        self.log.info("==============  QueryANSIJOINSTests tearDown has completed ==============")
        super(QueryINDEXUNNESTTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryANSIJOINSTests suite_tearDown has started ==============")
        self.log.info("==============  QueryANSIJOINSTests suite_tearDown has completed ==============")
        super(QueryINDEXUNNESTTests, self).suite_tearDown()

    def test_simple(self):
        index = "CREATE INDEX array_index ON default(ALL ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            #First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where d.email == "9-mail@couchbase.com" and d.department == "support" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['spans'])
                            and 'support' in str(plan['~children'][0]['spans']), "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                    'd.email == "9-mail@couchbase.com" and d.department == "Support" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query = "DROP INDEX default.array_index")

    def test_str_functions(self):
        index = "CREATE INDEX array_index ON default(ALL ARRAY v1 FOR v1 IN VMs END,lower(email),upper(department))"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where lower(d.email) == "9-mail@couchbase.com" and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                            'lower(d.email) == "9-mail@couchbase.com" and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_int_functions(self):
        index = "CREATE INDEX array_index ON default(ALL ARRAY v1 FOR v1 IN VMs END,exp(join_mo),upper(department))"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where exp(d.join_mo) == 81 and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('81' in str(plan['~children'][0]['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                            'exp(d.join_mo) == 81 and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_distinct_simple(self):
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            #First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where d.email == "9-mail@couchbase.com" and d.department == "support" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'support' in str(plan['~children'][0]['scan']['spans']), "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                    'd.email == "9-mail@couchbase.com" and d.department == "Support" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query = "DROP INDEX default.array_index")

    def test_distinct_str_functions(self):
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY v1 FOR v1 IN VMs END,lower(email),upper(department))"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where lower(d.email) == "9-mail@couchbase.com" and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                            'lower(d.email) == "9-mail@couchbase.com" and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_distinct_int_functions(self):
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY v1 FOR v1 IN VMs END,exp(join_mo),upper(department))"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where exp(d.join_mo) == 81 and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('81' in str(plan['~children'][0]['scan']['spans'])
                            and 'SUPPORT' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                            'exp(d.join_mo) == 81 and upper(d.department) == "SUPPORT" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_nested_unnest(self):
        index = "CREATE INDEX array_index ON default(ALL ARRAY (ALL ARRAY v2.region1 FOR v2 IN v1.Marketing END)  FOR v1 IN tasks END,email,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM default AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM default AS d use index (`#primary`) UNNEST d.tasks AS v1 UNNEST v1.Marketing ' \
                            'as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'

            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_distinct_nested_unnest(self):
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY (DISTINCT ARRAY v2.region1 FOR v2 IN v1.Marketing END)  FOR v1 IN tasks END,email,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM default AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM default AS d use index (`#primary`) UNNEST d.tasks AS v1 UNNEST v1.Marketing ' \
                            'as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'

            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_distinct_all_nested_unnest(self):
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY (ALL ARRAY v2.region1 FOR v2 IN v1.Marketing END)  FOR v1 IN tasks END,email,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM default AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM default AS d use index (`#primary`) UNNEST d.tasks AS v1 UNNEST v1.Marketing ' \
                            'as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'

            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_neg_nested_unnest(self):
        index = "CREATE INDEX array_index ON default(email,ALL ARRAY (ALL ARRAY v2.region1 FOR v2 IN v1.Marketing END)  FOR v1 IN tasks END,department)"
        self.run_cbq_query(query=index)
        try:
            # First check explain to make sure improvement is happening
            query = 'SELECT * FROM default AS d UNNEST d.tasks AS v1 UNNEST v1.Marketing as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('1-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'Developer' in str(plan['~children'][0]['scan']['spans'])
                            and 'South' not in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT * FROM default AS d use index (`#primary`) UNNEST d.tasks AS v1 UNNEST v1.Marketing ' \
                            'as v2 where v2.region1 == "South" and d.department == "Developer" and d.email == "1-mail@couchbase.com"'

            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")

    def test_neg_distinct_simple(self):
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            #First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where d.email == "9-mail@couchbase.com" and d.department == "support" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'support' in str(plan['~children'][0]['scan']['spans'])
                            and '10' not in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                    'd.email == "9-mail@couchbase.com" and d.department == "Support" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query = "DROP INDEX default.array_index")

    def test_neg_simple(self):
        index = "CREATE INDEX array_index ON default(ALL ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        try:
            #First check explain to make sure improvement is happening
            query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where d.email == "9-mail@couchbase.com" and d.department == "support" and v1.RAM == 10'
            explain_query = 'EXPLAIN ' + query
            explain_plan = self.run_cbq_query(explain_query)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['spans'])
                            and 'support' in str(plan['~children'][0]['spans'])
                            and '10' not in str(plan['~children'][0]['spans']),
                            "The non-array index keys are not being used")

            actual_results = self.run_cbq_query(query=query)

            # Ensure the results using the array_indexing improvements are the same as the primary index results
            primary_query = 'SELECT v1 FROM default AS d use index (`#primary`) UNNEST d.VMs AS v1 where ' \
                    'd.email == "9-mail@couchbase.com" and d.department == "Support" and v1.RAM == 10'
            expected_results = self.run_cbq_query(query=primary_query)

            self.assertEqual(sorted(actual_results), sorted(expected_results), "The results dont match up")
        finally:
            self.run_cbq_query(query="DROP INDEX default.array_index")



