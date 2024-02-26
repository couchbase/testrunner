from membase.api.exception import CBQError
from .tuq import QueryTests
from deepdiff import DeepDiff



class QueryANSIJOINSTests(QueryTests):
    def setUp(self):
        super(QueryANSIJOINSTests, self).setUp()
        self.log.info("==============  QueryANSIJOINSTests setup has started ==============")
        if not self.sample_bucket:
            self.sample_bucket = 'travel-sample'
        self.query_buckets = self.get_query_buckets(check_all_buckets=True, sample_buckets=[self.sample_bucket])
        self.default_query_bucket = self.query_buckets[0]
        self.query_bucket = self.query_buckets[1]
        self.standard_query_bucket = self.query_buckets[2]
        if self.load_sample:
            self.rest.load_sample(self.sample_bucket)
        self.log.info("==============  QueryANSIJOINSTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryANSIJOINSTests, self).suite_setUp()
        self.log.info("==============  QueryANSIJOINSTests suite_setup has started ==============")
        if not self.sample_bucket:
            self.sample_bucket = 'travel-sample'
        self.query_buckets = self.get_query_buckets(check_all_buckets=True, sample_buckets=[self.sample_bucket])
        self.default_query_bucket = self.query_buckets[0]
        self.query_bucket = self.query_buckets[1]
        self.standard_query_bucket = self.query_buckets[2]
        self.rest.load_sample(self.sample_bucket)
        self.load_nest_data()
        self.log.info("==============  QueryANSIJOINSTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryANSIJOINSTests tearDown has started ==============")
        self.log.info("==============  QueryANSIJOINSTests tearDown has completed ==============")
        super(QueryANSIJOINSTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryANSIJOINSTests suite_tearDown has started ==============")
        self.log.info("==============  QueryANSIJOINSTests suite_tearDown has completed ==============")
        super(QueryANSIJOINSTests, self).suite_tearDown()

    ''' Basic test of ANSI JOIN syntax
        -Create an index on travel-sample.id
        -Run a basic join query (expected resultCount is 1728)'''

    def test_basic_join(self):
        idx_list = []
        queries_to_run = []
        index = "CREATE INDEX idx1 on {0}(id)".format(self.query_bucket)
        idx_list.append((index, (self.query_bucket, "idx1")))

        query = "select * from {1} d1 INNER JOIN {0} t on (d1.join_day == t.id)".format(self.query_bucket,
                                                                                        self.default_query_bucket)
        queries_to_run.append((query, 1728))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a join where there are two comparisons in the ON clause that require two separate indexes
        -Create an index on default.name and default.join_day (these are the two fields in default that will be in the
         on clause
        -Run the join query, it should go through and have 1728 results '''

    def test_basic_join_non_covering(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
        idx2 = "CREATE INDEX join_day on {0}(join_day)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "name")))
        idx_list.append((idx2, (self.default_query_bucket, "join_day")))

        query = "select * from {0} t INNER JOIN {1} d1 on (d1.join_day == t.id OR" \
                " t.name == d1.name)".format(self.query_bucket, self.default_query_bucket)
        queries_to_run.append((query, 1728))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test the features of RIGHT OUTER JOIN, it will only work as the front of the join, the fields on the lhs bucket
        need to be covered by an index (MB-27763)
            -Create the needed index on the left hand side of the on expression
            -Run the ROJ query
            -This test is not yet complete, the bug is still open'''

    def test_ROJ(self):
        idxs = []
        try:
            index = "CREATE INDEX idx1 on {0}(name)".format(self.default_query_bucket)
            self.run_cbq_query(query=index)
            idxs.append((self.default_query_bucket, "idx1"))
            query = "SELECT * FROM {0} d RIGHT OUTER JOIN {1} s ON " \
                    "(d.name == s.name) LIMIT 10".format(self.default_query_bucket, self.standard_query_bucket)
            self.run_cbq_query(query=query)
        finally:
            for idx in idxs:
                drop_query = "DROP INDEX %s ON %s" % (idx[1], idx[0])
                self.run_cbq_query(query=drop_query)

    ''' Test the features of RIGHT OUTER JOIN, it will only work as the front of the join, the fields on the lhs bucket
        need to be covered by an index
            -Create the needed index on the left hand side of the on expression
            -Run the ROJ/IJ chain query'''

    def test_ROJ_chain(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX join_day on {0}(join_day)".format(self.default_query_bucket)
        idx2 = "CREATE INDEX join_mo on {0}(join_mo)".format(self.default_query_bucket)
        idx3 = "CREATE INDEX id on {0}(id)".format(self.query_bucket)

        idx_list.append((idx, (self.default_query_bucket, "join_day")))
        idx_list.append((idx2, (self.default_query_bucket, "join_mo")))
        idx_list.append((idx3, (self.query_bucket, "id")))

        query = "select * from {1} d RIGHT OUTER JOIN {0} t  ON (t.id == d.join_day) INNER JOIN " \
                "{1} d1 ON (t.id == d1.join_mo) limit 1000".format(self.query_bucket, self.default_query_bucket)
        queries_to_run.append((query, 1000))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' With subquery support both sides of a join can be subqueries'''

    def test_join_subquery(self):
        queries_to_run = []

        query_1 = "select * from (select * from {0} as d_main) d INNER JOIN (select * from {0} d1 where d1.name == " \
                  "'employee-9') d2 ON (d.d_main.name = d2.d1.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        # Ensure that the build side is d2
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_1, 186624))

        self.run_common_body(queries_to_run=queries_to_run)

    ''' With subquery support both sides of a join can be expressions'''

    def test_join_expression(self):
        queries_to_run = []

        query_1 = "select * from ([{'name' : 'employee-9'}, {'name': 'employee-10'}, {'name': 'employee-11'}, {'name': 'employee-12'}]) d INNER JOIN" \
                  " ([{'name' : 'employee-9'},{'name' : 'employee-10'}, {'name': 'employee-11'}, {'name': 'employee-12'}]) d2 ON (d.name = d2.name)"
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        # Ensure that the build side is d2
        self.assertTrue('d2' in plan['~children'][1]['build_aliases'])
        queries_to_run.append((query_1, 2))

        self.run_common_body(queries_to_run=queries_to_run)

    ''' With subquery support both sides of a join can be expressions/subquery'''

    def test_join_mixed(self):
        queries_to_run = []

        query_1 = "select * from (select * from {0} d1 where d1.name == 'employee-9') d INNER JOIN " \
                  "([{{'name' : 'employee-9'}},{{'name' : 'employee-10'}},{{'name' : 'employee-11'}},{{'name' : 'employee-12'}}]) d2 ON " \
                  "(d.d1.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_1, 432))

        query_2 = "select * from ([{{'name' : 'employee-9'}},{{'name' : 'employee-10'}}]) d2 INNER JOIN " \
                  "(select * from {0} d1 where d1.name == 'employee-9') d ON " \
                  "(d.d1.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_2
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d' in plan['~children'][1]['build_aliases'])
        queries_to_run.append((query_2, 432))

        self.run_common_body(queries_to_run=queries_to_run)

    ''' The right hand side of a join can be an expression, it needs to be a key space (a bucket)
            -Run a query where the right hand expression is a subquery
            -Run a query where the right hand expression is another join'''

    def test_join_right_hand_subquery(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "name")))

        query_1 = "select * from {0} d INNER JOIN (select * from {0} d1 where d1.name == 'ajay') d2 ON " \
                  "(d.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        queries_to_run.append((query_1, 0))

        query_2 = "select * from {0} d INNER JOIN (select * from {0} d1 inner join {0} d3 on " \
                  "d1.name == d3.name LIMIT 100) d2 ON (d.name = d3.name) LIMIT 100".format(self.default_query_bucket)

        queries_to_run.append((query_2, 100))

        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

        try:
            idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
            self.run_cbq_query(idx)
            explain_query_2 = "EXPLAIN " + query_2
            explain_plan = self.run_cbq_query(explain_query_2)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue("HashJoin" in str(plan))

        finally:
            self.run_cbq_query("drop index name on {0}".format(self.default_query_bucket))

    ''' The right hand side of a join can be an expression, it needs to be a key space (a bucket)
            -Run a query where the right hand expression is a json doc'''

    def test_join_right_hand_expression(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "name")))

        query_1 = "select * from {0} d INNER JOIN ({{'name' : 'employee-9'}}) d2 ON " \
                  "(d.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        queries_to_run.append((query_1, 432))

        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ##############################################################################################
    #
    #   Hints
    #
    ##############################################################################################

    ''' With subquery support both sides of a join can be subqueries w/hash hints'''

    def test_join_subquery_hints(self):
        queries_to_run = []

        query_1 = "select * from (select * from {0} d_main) d INNER JOIN (select * from {0} d1 where d1.name == " \
                  "'employee-9') d2 USE HASH(build) ON (d.d_main.name = d2.d1.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        # Ensure that the build side is d2
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_1, 186624))

        query_2 = "select * from (select * from {0} d_main) d INNER JOIN (select * from {0} d1 where d1.name == " \
                  "'employee-9') d2 USE HASH(probe) ON (d.d_main.name = d2.d1.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_2
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        # d2 is the probe side so d should be the build side
        self.assertTrue('d' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_2, 186624))

        self.run_common_body(queries_to_run=queries_to_run)

    ''' With subquery support both sides of a join can be expressions w/hash hints'''

    def test_join_expression_hints(self):
        queries_to_run = []

        query_1 = "select * from ([{'name' : 'employee-9'}, {'name': 'employee-10'}]) d INNER JOIN ([{'name' : " \
                  "'employee-9'},{'name' : 'employee-10'}]) d2 USE HASH(build) ON (d.name = d2.name)"
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        # Ensure that the build side is d2
        self.assertTrue('d2' in plan['~children'][1]['build_aliases'])
        queries_to_run.append((query_1, 2))

        query_2 = "select * from ([{'name' : 'employee-9'}, {'name': 'employee-10'}]) d INNER JOIN ([{'name' : " \
                  "'employee-9'},{'name' : 'employee-10'}]) d2 USE HASH(probe) ON (d.name = d2.name)"
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_2
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        # d2 is the probe side so d should be the build side
        self.assertTrue('d' in plan['~children'][1]['build_aliases'])
        queries_to_run.append((query_2, 2))

        self.run_common_body(queries_to_run=queries_to_run)

    ''' With subquery support both sides of a join can be expressions/subquery w/hash hints'''

    def test_join_mixed_hints(self):
        queries_to_run = []

        query_1 = "select * from (select * from {0} d1 where d1.name == 'employee-9') d INNER JOIN ([{{'name' : " \
                  "'employee-9'}},{{'name' : 'employee-10'}}]) d2 USE HASH(build) ON (d.d1.name = " \
                  "d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_1, 432))

        query_2 = "select * from ([{{'name' : 'employee-9'}},{{'name' : 'employee-10'}}]) d2 INNER JOIN (select * " \
                  "from {0} d1 where d1.name == 'employee-9') d USE HASH(probe) ON " \
                  "(d.d1.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_2
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_2, 432))

        self.run_common_body(queries_to_run=queries_to_run)

    ''' Run a udf with hash join hints that needs to reopen, should return correct results'''
    def test_MB59082(self):
            self.run_cbq_query(
                'UPSERT INTO default (KEY k, VALUE v) SELECT "k00"||TO_STR(d) AS k, {"c1":d, "c2":d, "c3":d} '
                'AS v FROM ARRAY_RANGE(1,10) AS d')
            self.run_cbq_query('CREATE INDEX ix1 ON default(c1,c2, c3)')
            self.run_cbq_query('CREATE OR REPLACE FUNCTION f10(a) '
                               '{( SELECT l, r FROM default AS l JOIN default AS r USE HASH (BUILD) '
                               'ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c1 > 0 AND r.c2 = a)}')
            actual_results = self.run_cbq_query('select raw f10(t.c1) from default as t where t.c1 > 0')
            expected_results = self.run_cbq_query('SELECT l, r FROM default AS l JOIN default AS r USE HASH (BUILD) '
                                                  'ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c1 > 0 AND r.c2 = r.c1 and r.c1 >0')
            i = 0
            for results in actual_results['results']:
                if results[0] != expected_results['results'][i]:
                    self.fail(
                        f"Results are incorrect please check expected:{expected_results}, actual:{actual_results}")
                i += 1

    ''' The right hand side of a join can be an expression, it needs to be a key space (a bucket)
            -Run a query where the right hand expression is a subquery
            -Run a query where the right hand expression is another join'''

    def test_join_right_hand_subquery_hints(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "name")))

        query_1 = "select * from {0} d INNER JOIN (select * from {0} d1 where d1.name == 'ajay') d2 USE HASH(build) " \
                  "ON (d.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])

        queries_to_run.append((query_1, 0))

        query_2 = "select * from {0} d INNER JOIN (select * from {0} d1 inner join {0} d3 on " \
                  "d1.name == d3.name LIMIT 100) d2 USE HASH(build) ON (d.name = d3.name) " \
                  "LIMIT 100".format(self.default_query_bucket)

        queries_to_run.append((query_2, 100))

        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

        try:
            idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
            self.run_cbq_query(idx)
            explain_query_2 = "EXPLAIN " + query_2
            explain_plan = self.run_cbq_query(explain_query_2)
            plan = self.ExplainPlanHelper(explain_plan)
            self.assertTrue("HashJoin" in str(plan))
            hashjoin_operator = next((op for op in plan['~children'][0]['~children'] if op["#operator"] == "HashJoin"), None)
            self.assertTrue('d2' in hashjoin_operator['build_aliases'], "d2 not in build_aliases: {0}".format(hashjoin_operator))

        finally:
            self.run_cbq_query("drop index name ON {0}".format(self.default_query_bucket))

    ''' The right hand side of a join can be an expression, it needs to be a key space (a bucket)
            -Run a query where the right hand expression is a json doc'''

    def test_join_right_hand_expression_hints(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX name on {0}(name)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "name")))

        query_1 = "select * from {0} d INNER JOIN ({{'name' : 'employee-9'}}) d2 USE HASH(build) ON " \
                  "(d.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_1
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d2' in plan['~children'][2]['build_aliases'])
        queries_to_run.append((query_1, 432))

        query_2 = "select * from {0} d INNER JOIN ({{'name' : 'employee-9'}}) d2 USE HASH(probe) ON " \
                  "(d.name = d2.name)".format(self.default_query_bucket)
        # With subquery support this join should be a hash join
        explain_query = "EXPLAIN " + query_2
        explain_plan = self.run_cbq_query(explain_query)
        plan = self.ExplainPlanHelper(explain_plan)
        self.assertTrue("HashJoin" in str(plan))
        self.assertTrue('d' in plan['~children'][1]['build_aliases'])
        queries_to_run.append((query_1, 432))

        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ##############################################################################################
    #
    #   Array Indexing
    #
    ##############################################################################################

    ''' Test a join that contains array-indexing
        -Create an index on join_day, join_mo, and the RAM field of the VM array
        -Create an index on travel-sample.id
        -Run a query that contains an array on the right hand side'''

    def test_basic_array_indexing_right_hand_side(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX array_index on {0}(join_day,join_mo,DISTINCT ARRAY v.RAM " \
              "for v in VMs END )".format(self.default_query_bucket)
        idx2 = "CREATE INDEX id on {0}(id)".format(self.query_bucket)

        idx_list.append((idx, (self.default_query_bucket, "array_index")))
        idx_list.append((idx2, (self.query_bucket, "id")))

        query = "select * from {0} t INNER JOIN {1} d ON (t.id == d.join_mo AND t.id == d.join_day " \
                "AND ANY var IN d.VMs SATISFIES var.RAM = t.id END) WHERE t.id = 10".format(self.query_bucket,
                                                                                            self.default_query_bucket)
        queries_to_run.append((query, 36))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a join that contains array-indexing comparing arrays contained in each keyspace
        -Create an index on the array in default
        -Create an index on the array in standard_bucket0
        -Run a query that compares the array in defaul to the array in standard_bucket0
        -This test is not yet complete, a bug is still open'''

    def test_array_indexing_full_arrays(self):
        idxs = []
        try:
            index = "CREATE INDEX arrays on {0}( VMs)".format(self.default_query_bucket)
            self.run_cbq_query(query=index)
            idxs.append((self.default_query_bucket, "arrays"))

            index = "CREATE INDEX arrays1 on standard_bucket0( VMs)"
            self.run_cbq_query(query=index)
            idxs.append(("standard_bucket0", "arrays1"))

            query = "select * from standard_bucket0 s INNER JOIN {0} d ON (d.VMs == s.VMs) " \
                    "LIMIT 10".format(self.default_query_bucket)
            query_results = self.run_cbq_query(query=query)
            # The result count must be updated when this is unblocked
            self.assertEqual(query_results['metrics']['resultCount'], 36)
        finally:
            for idx in idxs:
                drop_query = "DROP INDEX %s ON %s" % (idx[1], idx[0])
                self.run_cbq_query(query=drop_query)

    ''' Test a join that contains array-indexing
        -Create an index on join_day, join_mo, and the RAM field of the VM array, this join is 1-1 because the ANY pred
         doesn't involve any joins
        -Create an index on travel-sample.id
        -Run a query that contains an array on the right hand side'''

    def test_array_indexing_one_to_one_right(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX array_index on {0}(join_day,join_mo,DISTINCT ARRAY v.RAM " \
              "for v in VMs END )".format(self.default_query_bucket)
        idx2 = "CREATE INDEX id on {0}(id)".format(self.query_bucket)

        idx_list.append((idx, (self.default_query_bucket, "array_index")))
        idx_list.append((idx2, (self.query_bucket, "id")))

        query = "select * from {0} t INNER JOIN {1} d ON (t.id == d.join_mo AND t.id == d.join_day " \
                "AND ANY var IN d.VMs SATISFIES var.RAM = 10 END) WHERE t.id = 10".format(self.query_bucket,
                                                                                          self.default_query_bucket)
        queries_to_run.append((query, 36))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a join that contains array-indexing
        -Create an index on join_day, join_mo, and the RAM field of the VM array, this join is 1-1 because the ANY pred
         doesn't involve any joins
        -Create an index on travel-sample.id
        -Run a query that contains an array on the left hand side'''

    def test_array_indexing_one_to_one_left(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX array_index on {0}(join_mo,DISTINCT ARRAY v.RAM " \
              "for v in VMs END )".format(self.default_query_bucket)
        idx2 = "CREATE INDEX id on {0}(id)".format(self.query_bucket)

        idx_list.append((idx, (self.default_query_bucket, "array_index")))
        idx_list.append((idx2, (self.query_bucket, "id")))

        query = "select * from {1} d INNER JOIN {0} t ON (t.id == d.join_mo " \
                "AND ANY var IN d.VMs SATISFIES var.RAM = 10 END)".format(self.query_bucket, self.default_query_bucket)
        queries_to_run.append((query, 1008))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a join that contains array-indexing
        -Create an index on join_day, join_mo, and the RAM field of the VM array
        -Create an index on travel-sample.id
        -Run a query that contains an array on the left hand side, use UNNEST keyword to make up for issues with arrays 
         being on the lefthand expression of JOINs, unnest will have different results than IN because unnest will treat
         each element of the array as separate values, therefore if there are duplicate elements in an array the query 
         will get a match twice instead of once'''

    def test_array_indexing_left_hand_array_unnest(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX array_index on {0}(DISTINCT ARRAY v.RAM for v in VMs END )".format(
            self.default_query_bucket)
        idx2 = "CREATE INDEX id on {0}(id)".format(self.query_bucket)

        idx_list.append((idx, (self.default_query_bucket, "array_index")))
        idx_list.append((idx2, (self.query_bucket, "id")))

        query = "select * from {1} d UNNEST d.VMs as noarray  INNER JOIN {0} t ON " \
                "(noarray.RAM == t.id)".format(self.query_bucket, self.default_query_bucket)
        queries_to_run.append((query, 2016))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a join that contains array-indexing
        -Create an index on join_day, join_mo, and the RAM field of the VM array
        -Create an index on travel-sample.id
        -Run a query that contains an array on the left hand side, use IN keyword to make up for issues with arrays 
         being on the lefthand expression of JOINs'''

    def test_array_indexing_left_hand_array_IN(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX array_index on {0}(DISTINCT ARRAY v.RAM for v in VMs END )".format(
            self.default_query_bucket)
        idx2 = "CREATE INDEX id on {0}(id)".format(self.query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "array_index")))
        idx_list.append((idx2, (self.query_bucket, "id")))

        query = "select * from {1} d INNER JOIN {0} t ON (t.id IN " \
                "ARRAY_DISTINCT(ARRAY v.RAM for v in d.VMs END))".format(self.query_bucket, self.default_query_bucket)
        queries_to_run.append((query, 1008))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ##############################################################################################
    #
    #   NEST tests
    #
    ##############################################################################################
    ''' Test a basic ansi nest, there are four ways to do it
        1. The array is on the left and uses IN keyword
            - Create an index on default(lineitems_id)
            - Run an ansi nest query (expect 2 results using this method)'''

    def test_ansi_nest_basic_left_IN(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX lineitem_id on {0}(lineitem_id)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "lineitem_id")))

        query = "SELECT * FROM {0} d NEST {0} li ON (li.lineitem_id in d.lineitems)".format(self.default_query_bucket)
        queries_to_run.append((query, 2))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a basic ansi nest, there are four ways to do it
        2. The array is on the left and uses UNNEST keyword
            - Create an index on default(lineitem_id)
            - Run an ansi nest query(expect 5 results using this method)'''

    def test_ansi_nest_basic_left_UNNEST(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX lineitem_id on {0}(lineitem_id)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "lineitem_id")))

        query = "Select * from {0} d unnest d.lineitems as l nest {0} li on l = li.lineitem_id".format(
            self.default_query_bucket)
        queries_to_run.append((query, 5))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a basic ansi nest, there are four ways to do it
        3. The array is on the right and uses IN keyword
            - Create an index on default(lineitems)
            - Run an ansi nest query (expect 5 results using this method)'''

    def test_ansi_nest_basic_right_IN(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX lineitems on {0}(lineitems)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "lineitems")))

        query = "SELECT * FROM {0} li NEST {0} d ON (li.lineitem_id in d.lineitems)".format(self.default_query_bucket)
        queries_to_run.append((query, 5))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test a basic ansi nest, there are four ways to do it
        4. The array is on the right and uses IN keyword
            - Create an index on default( DISTINCT lineitems)
            - Run an ansi nest query (expect 5 results using this method)'''

    def test_ansi_nest_basic_right(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX lineitems on {0}(DISTINCT lineitems)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "lineitems")))

        query = "SELECT * FROM {0} li NEST {0} d ON (ANY item in d.lineitems satisfies item = " \
                "li.lineitem_id END)".format(self.default_query_bucket)
        queries_to_run.append((query, 5))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ''' Test that an ANSI JOIN will work with an ANSI NEST
        -Create the necessary indexes
        -Run a query that mixes ANSI JOIN and ANSI NEST'''

    def test_ansi_nest_with_ansi_join(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX lineitems on {0}(DISTINCT lineitems)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "lineitems")))
        idx2 = "CREATE INDEX lineitem_id on {0}(lineitem_id)".format(self.default_query_bucket)
        idx_list.append((idx2, (self.default_query_bucket, "lineitem_id")))

        query = "SELECT * FROM {0} li NEST {0} d ON (ANY item in d.lineitems satisfies item = " \
                "li.lineitem_id END) JOIN {0} d1 ON(d1.lineitem_id == li.lineitem_id)".format(self.default_query_bucket)
        queries_to_run.append((query, 5))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    '''Mix ansi join w/ansi nest and unnest
        -Create the necessary index
        -Run a query that mixes ansi join w/ansi nest and unnest'''

    def test_ansi_nest_ansi_join_unnest(self):
        idx_list = []
        queries_to_run = []
        idx = "CREATE INDEX lineitem_id on {0}(lineitem_id)".format(self.default_query_bucket)
        idx_list.append((idx, (self.default_query_bucket, "lineitem_id")))

        query = "select * from {0} d unnest d.lineitems as l nest {0} li on l = li.lineitem_id inner join " \
                "{0} d1 on l = d1.lineitem_id".format(self.default_query_bucket)
        queries_to_run.append((query, 5))
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    ##############################################################################################
    #
    #   Negative tests
    #
    ##############################################################################################
    ''' This test will try a join without the proper index for the right hand side of the ON expression, the expectation 
        is that it will not work
            -Run an inner join w/no indexes present besides the primary on default and standard
            -Run an outer join w/no indexes present besides the primary indexes on default and standard'''

    def test_join_no_index(self):
        try:
            query_1 = 'select * from {0} d INNER JOIN standard_bucket0 s ON ' \
                      '(d.name == s.name)'.format(self.default_query_bucket)
            self.run_cbq_query(query=query_1)
        except CBQError as error:
            self.assertTrue("No index available for ANSI join term s" in str(error),
                            "The error message is incorrect. It should have been %s" % error)
        try:
            query_1 = 'select * from {0} d JOIN standard_bucket0 s ON ' \
                      '(d.name == s.name)'.format(self.default_query_bucket)
            self.run_cbq_query(query=query_1)
        except CBQError as error:
            self.assertTrue("No index available for ANSI join term s" in str(error),
                            "The error message is incorrect. It should have been %s" % error)

    ''' Special test case of join w/o index because in a ROJ the left hand side of the ON must be covered by an index, 
        rather than the right hand side
            -Create an index on the right hand side of the expression
            -Run ROJ '''

    def test_ROJ_no_index(self):
        idxs = []
        try:
            standard_index = "CREATE INDEX standard_index on standard_bucket0(name)"
            self.run_cbq_query(query=standard_index)
            idxs.append(("standard_bucket0", "standard_index"))
            query_1 = "SELECT * FROM {0} d RIGHT OUTER JOIN standard_bucket0 s on" \
                      " d.name == s.name".format(self.default_query_bucket)
            self.run_cbq_query(query=query_1)
        except CBQError as error:
            self.assertTrue("No index available for ANSI join term d" in str(error),
                            "The error message is incorrect. It should have been %s" % error)
        finally:
            for idx in idxs:
                drop_query = "DROP INDEX %s ON %s" % (idx[1], idx[0])
                self.run_cbq_query(query=drop_query)

    ''' Right outer join is basically a reversed left outer join, therefore the lefthand side needs to be a keyspace
        this means that an ROJ can only be the first join in a chain of joins.
            -Run a query where an ROJ is not the first join'''

    def test_ROJ_out_of_order(self):
        try:
            query_1 = "select * from {0} d1 INNER JOIN {0} d2 on (d1.name == d2.name) INNER JOIN {0} d3 on " \
                      "(d2.name == d3.name) RIGHT OUTER JOIN {0} d4 on " \
                      "(d3.name == d4.name)".format(self.default_query_bucket)
            self.run_cbq_query(query=query_1)
        except CBQError as error:
            self.assertTrue("syntax error - line 1, column 118, near '...d2.name == d3.name) ', at: RIGHT" in str(error),
                            "The error message is incorrect. The error message given by the server is %s" % error)

    ''' Try to mix the old join syntax with the new ansi join syntax, the syntax mixing should not be allowed
            -Run a query that mixes the old join syntax with a new syntax'''

    def test_mixed_syntax(self):
        try:
            query_1 = "select * from {0} d1 INNER JOIN {0} d2 on keys (d1._id) INNER JOIN {0} d4 on " \
                      "(d2.name == d4.name) ".format(self.default_query_bucket)
            self.run_cbq_query(query=query_1)
        except CBQError as error:
            self.assertTrue("Cannot mix non ANSI JOIN on d2 with ANSI JOIN on d4." in str(error),
                            "The error message is incorrect. The error message given by the server is %s" % error)

    ''' Test a join that contains array-indexing
        -Create an index on join_day, join_mo, and the RAM field of the VM array
        -Create an index on travel-sample.id
        -Run a query that contains an array on the left hand side, this query should not work'''

    def test_array_indexing_left_hand_array_neg(self):
        idxs = []
        try:
            index = "CREATE INDEX array_index on {0}(DISTINCT ARRAY v.RAM " \
                    "for v in VMs END )".format(self.default_query_bucket)
            self.run_cbq_query(query=index)
            idxs.append((self.default_query_bucket, "array_index"))

            index = "CREATE INDEX id on {0}(id)".format(self.query_bucket)
            self.run_cbq_query(query=index)
            idxs.append((self.query_bucket, "id"))

            query = "select * from {1} d INNER JOIN {0} t ON (ANY var IN d.VMs SATISFIES var.RAM = " \
                    "t.id END)".format(self.query_bucket, self.default_query_bucket)
            self.run_cbq_query(query=query)
        except CBQError as error:
            self.assertTrue("No index available for ANSI join term t" in str(error),
                            "The error message is incorrect. It should have been %s" % error)
        finally:
            for idx in idxs:
                drop_query = "DROP INDEX %s ON %s" % (idx[1], idx[0])
                self.run_cbq_query(query=drop_query)

    '''ANSI JOIN cannot be mixed with normal NEST, must be mixed with ANSI NEST
        -Run a query that mixes ANSI JOIN w/normal NEST to verify correct error is thrown'''

    def test_mix_ansi_join_normal_nest_neg(self):
        try:
            query = "SELECT * FROM {0} d NEST {0} li ON KEYS d.lineitems JOIN {0} d1 ON (d1.lineitem_d " \
                    "== li.lineitem_id)".format(self.default_query_bucket)
            self.run_cbq_query(query=query)
        except CBQError as error:
            self.assertTrue("Cannot mix non ANSI NEST on li with ANSI JOIN on d1." in str(error),
                            "The error message is incorrect. It should have been %s" % error)

    ##############################################################################################
    #
    #   Helpers
    #
    ##############################################################################################
    '''Load the data necessary to run the nest tests'''

    def load_nest_data(self):
        insert = 'INSERT INTO %s ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": "order", "customer_id": ' \
                 '"24601", "total_price": 30.3, "lineitems": [ "11", "12", "13" ] }),("11", { "lineitem_id": "11", ' \
                 '"type": "lineitem", "item_id": 576, "quantity": 3, "item_price": 4.99, "base_price": 14.97, "tax": ' \
                 '0.75, "final_price": 15.72 }),("12",{ "lineitem_id": "12", "type": "lineitem", "item_id": 234, ' \
                 '"quantity": 1, "item_price": 12.95, "base_price": 12.95, "tax": 0.65, "final_price": 13.6 }),("13"' \
                 ',{ "lineitem_id": "13", "type": "lineitem", "item_id": 122, "quantity": 2, "item_price": 0.49, ' \
                 '"base_price": 0.98, "final_price": 0.98 }),("5",{ "order_id": "5", "type": "order", "customer_id": ' \
                 '"98732", "total_price": 428.04, "lineitems" : [ "51", "52" ] }),("51",{ "lineitem_id": "51", ' \
                 '"type": "lineitem", "item_id": 770, "quantity": 2, "item_price": 95.97, "base_price": 287.91, ' \
                 '"tax": 14.4, "final_price": 302.31 }),("52",{ "lineitem_id": "52", "type": "lineitem", "item_id": ' \
                 '712, "quantity": 1, "item_price": 125.73, "base_price": 125.73,' \
                 ' "final_price": 125.73 })' % self.default_query_bucket
        self.run_cbq_query(query=insert)

    '''The common code for all positive functional tests, specify which indexes to create and which queries to run
        then ensure the queries that ran match up with the expected results
        -index_list = a list that contains tuples that look like (index_definition,(bucket_name,index_name)) the 
        second nested tuple is used to drop the indexes at the end 
        -queries to run = a list that contains tuples that look like (query_definition, expected_results)'''

    def run_common_body(self, index_list=[], queries_to_run=[]):
        try:
            for idx in index_list:
                self.run_cbq_query(query=idx[0])

            for query in queries_to_run:
                query_results = self.run_cbq_query(query=query[0])
                self.assertEqual(query_results['metrics']['resultCount'], query[1])
        finally:
            for idx in index_list:
                drop_query = "DROP INDEX %s ON %s" % (idx[1][1], idx[1][0])
                self.run_cbq_query(query=drop_query)
