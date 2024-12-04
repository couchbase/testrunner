from .tuq import QueryTests
from deepdiff import DeepDiff


class QuerySkipRangeScanTests(QueryTests):

    def setUp(self):
        super(QuerySkipRangeScanTests, self).setUp()
        self.log.info("==============  QuerySkipRangeScanTests setup has started ==============")
        self.primary_index_def = {'name': '#primary', 'bucket': self.default_bucket_name, 'fields': [],
                                  'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        self.query_bucket = self.get_query_buckets()[0]
        self.log.info("==============  QuerySkipRangeScanTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QuerySkipRangeScanTests, self).suite_setUp()
        self.log.info("==============  QuerySkipRangeScanTests suite_setup has started ==============")
        self.log.info("==============  QuerySkipRangeScanTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QuerySkipRangeScanTests tearDown has started ==============")
        self.log.info("==============  QuerySkipRangeScanTests tearDown has completed ==============")
        super(QuerySkipRangeScanTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QuerySkipRangeScanTests suite_tearDown has started ==============")
        self.log.info("==============  QuerySkipRangeScanTests suite_tearDown has completed ==============")
        super(QuerySkipRangeScanTests, self).suite_tearDown()

    def verifier(self, compare_query, query_index):
        return lambda x: self.compare_queries(x['q_res'][query_index], compare_query)

    def plan_verifier(self, check_type, check_data, query_index):
        return lambda x: self.check_plan(x, check_type, check_data, query_index)

    def check_plan(self, explain_results, check_type, check_data, query_index):
        plan = self.ExplainPlanHelper(explain_results['q_res'][query_index])
        if check_type == "spans":
            spans = plan["~children"][0]['spans'][0]['range']
            self.assertEqual(spans, check_data)
        if check_type == "deep_spans":
            spans = plan["~children"][0]["~children"][0]['spans'][0]['range']
            self.assertEqual(spans, check_data)
        if check_type == "index":
            index = plan["~children"][0]['index']
            self.assertEqual(index, check_data, f"Index got {index} but we expected {check_data}. Plan was {plan}")
        if check_type == "join_index":
            self.assertTrue(check_data in str(plan), f"Join index got {plan} but we expected {check_data}. Plan was {plan}")
        if check_type == "join_span":
            spans = plan["~children"][2]['~child']['~children'][0]['~child']['~children'][0]['spans'][0]['range']
            self.assertEqual(spans, check_data)
        if check_type == "deep_index":
            index = plan["~children"][0]["~children"][0]['index']
            self.assertEqual(index, check_data)
        if check_type == "pushdown":
            self.assertTrue("index_group_aggs" in str(plan))

    def compare_queries(self, actual_results, compare_query):
        results = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(results), len(compare_docs))
        diffs = DeepDiff(results, compare_docs, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_basic_skip_range_scan(self):
        queries = dict()

        query_1a = "select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3"
        verify_1 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and join_day > 3"
        index_1 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (("join_yr", 0), ("join_mo", 1), ("join_day", 2)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_1b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3"
        spans_1 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`', 'low': '3'}]

        query_2a = "select * from " + self.query_bucket + " where join_yr > 2010 and join_mo > 3"
        verify_2 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and join_mo > 3"
        index_2 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (("join_yr", 0), ("join_mo", 1), ("join_day", 2)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_2b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_mo > 3"
        spans_2 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`', 'low': '3'}]

        query_3a = "select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3"
        verify_3 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and join_day > 3"
        index_3 = {'name': 'idx1', 'bucket': self.default_bucket_name,
                   'fields': (("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3)), 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_3b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3"
        spans_3 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`', 'low': '3'}]

        query_4a = "select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3 and name > 'employee-9' and email > '9-mail@couchbase.com'"
        verify_4 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and join_day > 3 and name > 'employee-9' and email > '9-mail@couchbase.com'"
        index_4 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_4b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3 and name > 'employee-9' and email > '9-mail@couchbase.com'"
        spans_4 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`', 'low': '3'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`','low': '"employee-9"'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`email`','low': '"9-mail@couchbase.com"'}]

        query_5a = "select * from " + self.query_bucket + " where join_yr > 2010"
        verify_5 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010"
        index_5 = {'name': 'idx1', 'bucket': self.default_bucket_name,
                   'fields': (("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3)), 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_5b = "explain select * from " + self.query_bucket + " where join_yr > 2010"
        spans_5 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_6a = "select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3 and name > 'employee-9'"
        verify_6 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and join_day > 3 and name > 'employee-9'"
        index_6 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_6b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 3 and name > 'employee-9'"
        spans_6 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`', 'low': '3'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`', 'low': '"employee-9"'}]

        query_7a = "select * from " + self.query_bucket + " where join_yr > 2010 and email > '9-mail@couchbase.com'"
        verify_7 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and email > '9-mail@couchbase.com'"
        index_7 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_7b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and email > '9-mail@couchbase.com'"
        spans_7 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`'}, {'inclusion': 0, 'index_key': '`test_rate`'},
                   {'inclusion': 0, 'index_key': '`email`', 'low': '"9-mail@couchbase.com"'}]

        query_8a = "select * from " + self.query_bucket + " where join_yr > 2010 and job_title > 'Engineer'"
        verify_8 = "select * from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and job_title > 'Engineer'"
        index_8 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_8b = "explain select * from " + self.query_bucket + " where join_yr > 2010 and job_title > 'Engineer'"
        spans_8 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`'}, {'inclusion': 0, 'index_key': '`job_title`', 'low': '"Engineer"'}]

        query_9a = "select join_mo, join_day, email from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        verify_9 = "select join_mo, join_day, email from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and test_rate > 0"
        index_9 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_9b = "explain select join_mo, join_day, email from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        spans_9 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`'}, {'inclusion': 0, 'index_key': '`test_rate`', 'low': '0'}]

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1a, query_1b],
                        "asserts": [self.verifier(verify_1, 0), self.plan_verifier("spans", spans_1, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        queries["b"] = {"indexes": [self.primary_index_def, index_2], "queries": [query_2a, query_2b],
                        "asserts": [self.verifier(verify_2, 0), self.plan_verifier("spans", spans_2, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        queries["c"] = {"indexes": [self.primary_index_def, index_3], "queries": [query_3a, query_3b],
                        "asserts": [self.verifier(verify_3, 0), self.plan_verifier("spans", spans_3, 1),
                                    self.plan_verifier("index", "idx1", 1)]}
# Disable query to revisit post Neo
#        queries["d"] = {"indexes": [self.primary_index_def, index_4], "queries": [query_4a, query_4b],
#                        "asserts": [self.verifier(verify_4, 0), self.plan_verifier("spans", spans_4, 1),
#                                    self.plan_verifier("index", "idx1", 1)]}

        queries["e"] = {"indexes": [self.primary_index_def, index_5], "queries": [query_5a, query_5b],
                        "asserts": [self.verifier(verify_5, 0), self.plan_verifier("spans", spans_5, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        queries["f"] = {"indexes": [self.primary_index_def, index_6], "queries": [query_6a, query_6b],
                        "asserts": [self.verifier(verify_6, 0), self.plan_verifier("spans", spans_6, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        queries["g"] = {"indexes": [self.primary_index_def, index_7], "queries": [query_7a, query_7b],
                        "asserts": [self.verifier(verify_7, 0), self.plan_verifier("spans", spans_7, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        queries["h"] = {"indexes": [self.primary_index_def, index_8], "queries": [query_8a, query_8b],
                        "asserts": [self.verifier(verify_8, 0), self.plan_verifier("spans", spans_8, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        queries["i"] = {"indexes": [self.primary_index_def, index_9], "queries": [query_9a, query_9b],
                        "asserts": [self.verifier(verify_9, 0), self.plan_verifier("spans", spans_9, 1),
                                    self.plan_verifier("index", "idx1", 1)]}

        self.query_runner(queries)

    def test_skip_range_scan_pushdown(self):
        queries = dict()

        # without group by

        query_1a = "select COUNT(join_yr), SUM(join_day) from " + self.query_bucket + " where join_yr > 2010"
        verify_1 = "select COUNT(join_yr), SUM(join_day) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010"
        index_1 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (("join_yr", 0), ("join_mo", 1), ("join_day", 2)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_1b = "explain select COUNT(join_yr), SUM(join_day) from " + self.query_bucket + " where join_yr > 2010"
        spans_1 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_2a = "select COUNT(join_day) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        verify_2 = "select COUNT(join_day) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and test_rate > 0"
        index_2 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_2b = "explain select COUNT(join_day) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        spans_2 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`', 'low': 'null'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`'}, {'inclusion': 0, 'index_key': '`test_rate`', 'low': '0'}]

        query_3a = "select COUNT(join_day), SUM(join_day) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        verify_3 = "select COUNT(join_day), SUM(join_day) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and test_rate > 0"
        index_3 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_3b = "explain select COUNT(join_day), SUM(join_day) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        spans_3 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`', 'low': 'null'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`'}, {'inclusion': 0, 'index_key': '`test_rate`', 'low': '0'}]

        query_4a = "select COUNT(join_day), SUM(join_day+1) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        verify_4 = "select COUNT(join_day), SUM(join_day+1) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and test_rate > 0"
        index_4 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_4b = "explain select COUNT(join_day), SUM(join_day+1) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        spans_4 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}, {'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`'}, {'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`'}, {'inclusion': 0, 'index_key': '`test_rate`', 'low': '0'}]

        query_5a = "select COUNT(join_day), COUNT(name) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        verify_5 = "select COUNT(join_day), COUNT(name) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010 and test_rate > 0"
        index_5 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_5b = "explain select COUNT(join_day), COUNT(name) from " + self.query_bucket + " where join_yr > 2010 and test_rate > 0"
        spans_5 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'},{'inclusion': 0, 'index_key': '`join_mo`'},
                   {'inclusion': 0, 'index_key': '`join_day`'},{'inclusion': 0, 'index_key': '`job_title`'},
                   {'inclusion': 0, 'index_key': '`name`'},{'inclusion': 0, 'index_key': '`test_rate`', 'low': '0'}]

        # with group by

        query_6a = "select COUNT(join_day) from " + self.query_bucket + " where join_yr > 2010 group by name"
        verify_6 = "select COUNT(join_day) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010  group by name"
        index_6 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_6b = "explain select COUNT(join_day) from " + self.query_bucket + " where join_yr > 2010 group by name"
        spans_6 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_7a = "select COUNT(join_day), COUNT(email) from " + self.query_bucket + " where join_yr > 2010 group by name"
        verify_7 = "select COUNT(join_day), COUNT(email) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010  group by name"
        index_7 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_7b = "explain select COUNT(join_day), COUNT(email) from " + self.query_bucket + " where join_yr > 2010 group by name"
        spans_7 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_8a = "select name, COUNT(join_day), COUNT(email) from " + self.query_bucket + " where join_yr > 2010 group by name"
        verify_8 = "select name, COUNT(join_day), COUNT(email) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010  group by name"
        index_8 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_8b = "explain select name, COUNT(join_day), COUNT(email) from " + self.query_bucket + " where join_yr > 2010 group by name"
        spans_8 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_9a = "select name, COUNT(join_day), COUNT(email) from " + self.query_bucket + " where join_yr > 2010 group by name, test_rate"
        verify_9 = "select name, COUNT(join_day), COUNT(email) from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010  group by name, test_rate"
        index_9 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_9b = "explain select name, COUNT(join_day), COUNT(email) from " + self.query_bucket + " where join_yr > 2010 group by name, test_rate"
        spans_9 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_10a = "select name, COUNT(join_day), COUNT(email), test_rate from " + self.query_bucket + " where join_yr > 2010 group by name, test_rate"
        verify_10 = "select name, COUNT(join_day), COUNT(email), test_rate from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010  group by name, test_rate"
        index_10 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        query_10b = "explain select name, COUNT(join_day), COUNT(email), test_rate from " + self.query_bucket + " where join_yr > 2010 group by name, test_rate"
        spans_10 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        query_11a = "select name, COUNT(join_day), COUNT(email), test_rate from " + self.query_bucket + " where join_yr > 2010 group by name, test_rate having LENGTH(name) > 1 order by test_rate"
        verify_11 = "select name, COUNT(join_day), COUNT(email), test_rate from " + self.query_bucket + " USE INDEX (`#primary` USING GSI) where join_yr > 2010  group by name, test_rate having LENGTH(name) > 1 order by test_rate"
        index_11 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': (
        ("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4), ("test_rate", 5), ("email", 6)),
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        query_11b = "explain select name, COUNT(join_day), COUNT(email), test_rate from " + self.query_bucket + " where join_yr > 2010 group by name, test_rate having LENGTH(name) > 1 order by test_rate"
        spans_11 = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': '2010'}]

        queries["a"] = {"indexes": [self.primary_index_def, index_1], "queries": [query_1a, query_1b],
                        "asserts": [self.verifier(verify_1, 0), self.plan_verifier("spans", spans_1, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["b"] = {"indexes": [self.primary_index_def, index_2], "queries": [query_2a, query_2b],
                        "asserts": [self.verifier(verify_2, 0), self.plan_verifier("spans", spans_2, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["c"] = {"indexes": [self.primary_index_def, index_3], "queries": [query_3a, query_3b],
                        "asserts": [self.verifier(verify_3, 0), self.plan_verifier("spans", spans_3, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["d"] = {"indexes": [self.primary_index_def, index_4], "queries": [query_4a, query_4b],
                        "asserts": [self.verifier(verify_4, 0), self.plan_verifier("spans", spans_4, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["e"] = {"indexes": [self.primary_index_def, index_5], "queries": [query_5a, query_5b],
                        "asserts": [self.verifier(verify_5, 0), self.plan_verifier("spans", spans_5, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["f"] = {"indexes": [self.primary_index_def, index_6], "queries": [query_6a, query_6b],
                        "asserts": [self.verifier(verify_6, 0), self.plan_verifier("spans", spans_6, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["g"] = {"indexes": [self.primary_index_def, index_7], "queries": [query_7a, query_7b],
                        "asserts": [self.verifier(verify_7, 0), self.plan_verifier("spans", spans_7, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["h"] = {"indexes": [self.primary_index_def, index_8], "queries": [query_8a, query_8b],
                        "asserts": [self.verifier(verify_8, 0), self.plan_verifier("spans", spans_8, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["i"] = {"indexes": [self.primary_index_def, index_9], "queries": [query_9a, query_9b],
                        "asserts": [self.verifier(verify_9, 0), self.plan_verifier("spans", spans_9, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["j"] = {"indexes": [self.primary_index_def, index_10], "queries": [query_10a, query_10b],
                        "asserts": [self.verifier(verify_10, 0), self.plan_verifier("spans", spans_10, 1),
                                    self.plan_verifier("index", "idx1", 1), self.plan_verifier("pushdown", None, 1)]}

        queries["k"] = {"indexes": [self.primary_index_def, index_11], "queries": [query_11a, query_11b],
                        "asserts": [self.verifier(verify_11, 0), self.plan_verifier("deep_spans", spans_11, 1),
                                    self.plan_verifier("deep_index", "idx1", 1),
                                    self.plan_verifier("pushdown", None, 1)]}

        self.query_runner(queries)

    def test_skip_range_scan_joins(self):
        queries = dict()

        query_1 = "explain select * from " + self.query_bucket + " d1 INNER JOIN " + self.query_bucket + " d2 on (d1.join_yr == d2.join_yr)"

        index_1a = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_1b = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_yr", 1)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        spans_1a = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}, {'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}]
        spans_1b = [{'high': '(`d1`.`join_yr`)', 'low': '(`d1`.`join_yr`)', 'inclusion': 3}]

        queries["a"] = {"indexes": [self.primary_index_def, index_1a, index_1b], "queries": [query_1],
                        "asserts": [self.plan_verifier("index", "idx2", 0),
                                    self.plan_verifier("join_index", "idx2", 0),
                                    self.plan_verifier("spans", spans_1a, 0),
                                    self.plan_verifier("join_spans", spans_1b, 0)]}

        query_2 = "explain select d1.join_yr from " + self.query_bucket + " d1 INNER JOIN " + self.query_bucket + " d2 on (d1.join_yr == d2.join_yr)"

        index_2a = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_2b = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_mo", 1)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        spans_2a = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}]
        spans_2b = [{'high': '(`d1`.`join_yr`)', 'low': '(`d1`.`join_yr`)', 'inclusion': 3}]

        queries["b"] = {"indexes": [self.primary_index_def, index_2a, index_2b], "queries": [query_2],
                        "asserts": [self.plan_verifier("index", "idx1", 0),
                                    self.plan_verifier("join_index", "idx1", 0),
                                    self.plan_verifier("spans", spans_2a, 0),
                                    self.plan_verifier("join_spans", spans_2b, 0)]}

        query_3 = "explain select d1.join_yr from " + self.query_bucket + " d1 INNER JOIN " + self.query_bucket + " d2 on (d1.join_yr == d2.join_yr and d1.join_day == d2.join_day)"

        index_3a = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_3b = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_day", 1)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_3c = {'name': 'idx3', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_mo", 1), ("join_day", 2)],
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        spans_3a = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}]
        spans_3b = [{'high': '(`d1`.`join_yr`)', 'low': '(`d1`.`join_yr`)', 'inclusion': 3}]

        queries["c"] = {"indexes": [self.primary_index_def, index_3a, index_3b, index_3c], "queries": [query_3],
                        "asserts": [self.plan_verifier("index", "idx2", 0),
                                    self.plan_verifier("join_index", "idx2", 0),
                                    self.plan_verifier("spans", spans_3a, 0),
                                    self.plan_verifier("join_spans", spans_3b, 0)]}

        query_4 = "explain select * from " + self.query_bucket + " d1 INNER JOIN " + self.query_bucket + " d2 on (d1.join_yr == d2.join_yr and d1.join_day == d2.join_day)"

        index_4a = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_4b = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_day", 0), ("join_mo", 1), ("join_yr", 2)],
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        spans_4a = [{'inclusion': 0, 'index_key': '`join_day`', 'low': 'null'},{'inclusion': 0, 'index_key': '`join_mo`'},
                    {'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}]
        spans_4b = [{'high': '(`d1`.`join_yr`)', 'low': '(`d1`.`join_yr`)', 'inclusion': 3}]

        queries["d"] = {"indexes": [self.primary_index_def, index_4a, index_4b], "queries": [query_4],
                        "asserts": [self.plan_verifier("index", "#primary", 0),
                                    self.plan_verifier("join_index", "#primary", 0),
                                    self.plan_verifier("spans", spans_4a, 0),
                                    self.plan_verifier("join_spans", spans_4b, 0)]}

        query_5 = "explain select * from " + self.query_bucket + " d1 INNER JOIN " + self.query_bucket + " d2 on (d1.join_yr == d2.join_yr and d1.join_day == d2.join_day)"

        index_5a = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_5b = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_day", 0), ("join_mo", 1), ("join_yr", 2)],
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_5c = {'name': 'idx3', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_day", 1)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        spans_5a = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}, {'inclusion': 0, 'index_key': '`join_day`', 'low': 'null'}]
        spans_5b = [{'high': '(`d1`.`join_yr`)', 'low': '(`d1`.`join_yr`)', 'inclusion': 3}]

        queries["e"] = {"indexes": [self.primary_index_def, index_5a, index_5b, index_5c], "queries": [query_5],
                        "asserts": [self.plan_verifier("index", '#primary', 0),
                                    self.plan_verifier("join_index", "#primary", 0),
                                    self.plan_verifier("spans", spans_5a, 0),
                                    self.plan_verifier("join_spans", spans_5b, 0)]}

        query_6 = "explain select * from " + self.query_bucket + " d1 INNER JOIN " + self.query_bucket + " d2 on (d1.join_yr == d2.join_yr and d1.join_day == d2.join_day)"

        index_6a = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_6b = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_day", 0), ("join_mo", 1), ("join_yr", 2)],
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_6c = {'name': 'idx3', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_day", 1)], 'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        index_6d = {'name': 'idx4', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_day", 1), ("join_mo", 2)],
                    'state': 'online',
                    'using': self.index_type.lower(), 'is_primary': False}
        spans_6a = [{'inclusion': 0, 'index_key': '`join_yr`', 'low': 'null'}, {'inclusion': 0, 'index_key': '`join_day`', 'low': 'null'}]
        spans_6b = [{'high': '(`d1`.`join_yr`)', 'low': '(`d1`.`join_yr`)', 'inclusion': 3}]

        queries["f"] = {"indexes": [self.primary_index_def, index_6a, index_6b, index_6c, index_6d],
                        "queries": [query_6],
                        "asserts": [self.plan_verifier("index", "#primary", 0),
                                    self.plan_verifier("join_index", "#primary", 0),
                                    self.plan_verifier("spans", spans_6a, 0),
                                    self.plan_verifier("join_spans", spans_6b, 0)]}
        self.query_runner(queries)

    # this test will fail until MB-31203 is fixed
    def test_skip_range_scan_index_selection(self):
        queries = dict()

        index_1 = {'name': 'idx1', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_2 = {'name': 'idx2', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_mo", 1)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_3 = {'name': 'idx3', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_mo", 1), ("join_day", 2)],
                   'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_4 = {'name': 'idx4', 'bucket': self.default_bucket_name,
                   'fields': [("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_5 = {'name': 'idx5', 'bucket': self.default_bucket_name, 'fields': [("join_yr", 0), ("join_day", 1)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_6 = {'name': 'idx6', 'bucket': self.default_bucket_name,
                   'fields': [("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4),
                              ("test_rate", 5)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_7 = {'name': 'idx7', 'bucket': self.default_bucket_name,
                   'fields': [("join_yr", 0), ("join_mo", 1), ("join_day", 2), ("job_title", 3), ("name", 4),
                              ("test_rate", 5), ("email", 6)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        index_8 = {'name': 'idx8', 'bucket': self.default_bucket_name,
                   'fields': [("join_yr", 0), ("join_mo", 1), ("job_title", 2), ("join_day", 3)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        query_1 = "explain select * from " + self.query_bucket + " where join_yr > 2010"
        queries["a"] = {"indexes": [self.primary_index_def, index_1, index_2], "queries": [query_1],
                        "asserts": [self.plan_verifier("index", "idx1", 0)]}

        query_2 = "explain select * from " + self.query_bucket + " where join_mo > 2010"
        queries["b"] = {"indexes": [self.primary_index_def, index_1, index_2], "queries": [query_2],
                        "asserts": [self.plan_verifier("index", "#primary", 0)]}

        query_3 = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_mo > 1"
        queries["c"] = {"indexes": [self.primary_index_def, index_1, index_2, index_3], "queries": [query_3],
                        "asserts": [self.plan_verifier("index", "idx2", 0)]}

        query_4 = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 1"
        queries["d"] = {"indexes": [self.primary_index_def, index_3, index_4], "queries": [query_4],
                        "asserts": [self.plan_verifier("index", "idx3", 0)]}

        query_5 = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 1"
        queries["e"] = {"indexes": [self.primary_index_def, index_3, index_5], "queries": [query_5],
                        "asserts": [self.plan_verifier("index", "idx5", 0)]}

        query_6 = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 1"
        queries["f"] = {"indexes": [self.primary_index_def, index_4, index_5], "queries": [query_6],
                        "asserts": [self.plan_verifier("index", "idx5", 0)]}

        query_6 = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 1"
        queries["g"] = {"indexes": [self.primary_index_def, index_6, index_7], "queries": [query_6],
                        "asserts": [self.plan_verifier("index", "idx6", 0)]}

        query_7 = "explain select * from " + self.query_bucket + " where join_yr > 2010 and join_day > 1"
        queries["h"] = {"indexes": [self.primary_index_def, index_3, index_8], "queries": [query_7],
                        "asserts": [self.plan_verifier("index", "idx3", 0)]}

        self.query_runner(queries)
