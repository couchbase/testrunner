from tuq import QueryTests


class QueryCTETests(QueryTests):

    def setUp(self):
        super(QueryCTETests, self).setUp()
        self.log.info("==============  QueryCTETests setup has started ==============")
        self.log.info("==============  QueryCTETests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryCTETests, self).suite_setUp()
        self.log.info("==============  QueryCTETests suite_setup has started ==============")
        self.log.info("==============  QueryCTETests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryCTETests tearDown has started ==============")
        self.log.info("==============  QueryCTETests tearDown has completed ==============")
        super(QueryCTETests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryCTETests suite_tearDown has started ==============")
        self.log.info("==============  QueryCTETests suite_tearDown has completed ==============")
        super(QueryCTETests, self).suite_tearDown()

    def verifier(self, compare_query):
        return lambda x: self.compare_queries(x['q_res'][0], compare_query)

    def plan_verifier(self, keyword):
        return lambda x: self.assertTrue(keyword in str(x['q_res'][0]))

    def compare_queries(self, actual_results, compare_query):
        let_letting_docs = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(let_letting_docs), len(compare_docs))
        self.assertEqual(let_letting_docs, compare_docs)

    def test_basic_cte(self):
        queries = dict()

        # constant expression
        query_1 = 'with a as (10), b as ("bob"), c as (3.3), d as (0), e as (""), f as (missing), g as (null) '
        query_1 += 'select a, b, c, d, e, f, g, join_yr as h, join_yr+a as i, join_yr+b as j, join_yr+c as k, join_yr+d as l, join_yr+e as m, join_yr+f as n, join_yr+g as o from default '
        query_1 += 'where a > 9 and b == "bob" and c < 4 and d == 0 and e is valued and f is missing and g is null '
        query_1 += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o'
        verify_1 = 'select 10 as a, "bob" as b, 3.3 as c, 0 as d, "" as e, missing as f, null as g, join_yr as h, join_yr+10 as i, join_yr+"bob" as j, join_yr+3.3 as k, join_yr+0 as l, join_yr+"" as m, join_yr+missing as n, join_yr+null as o from default '
        verify_1 += 'where 10 > 9 and "bob" == "bob" and 3.3 < 4 and 0 == 0 and "" is valued and missing is missing and null is null '
        verify_1 += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o'

        # subquery
        query_2 = 'with a as (select join_yr from default) select table1.join_yr from a as table1 order by table1.join_yr'
        verify_2 = 'select join_yr from default order by join_yr'

        query_3 = 'with a as (select join_yr from default) select table1.join_yr from a as table1 where table1.join_yr in a[*].join_yr order by table1.join_yr'
        verify_3 = 'select join_yr from default where join_yr order by join_yr'

        query_4 = 'with a as (select join_yr from default) select join_yr from default where join_yr in a[*].join_yr order by join_yr'
        verify_4 = 'select join_yr from default where join_yr order by join_yr'

        query_5 = 'with a as (select raw join_yr from default) select join_yr from default where join_yr in a order by join_yr'
        verify_5 = 'select join_yr from default where join_yr order by join_yr'

        query_6 = 'with a as (select join_yr from default) select table1.join_yr from a as table1 where table1.join_yr in a[*].join_yr order by table1.join_yr'
        verify_6 = 'select join_yr from default where join_yr order by join_yr'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["f"] = {"queries": [query_6], "asserts": [self.verifier(verify_6)]}

        self.query_runner(queries)

    def test_multiple_cte(self):
        queries = dict()

        # constant expression
        query_1 = 'with a as (2010), b as (2015) select join_yr from default where join_yr between a and b order by join_yr'
        verify_1 = 'select join_yr from default where join_yr between 2010 and 2015 order by join_yr'

        # subquery
        query_2 = 'with a as (select join_yr from default d1),b as (select join_mo from default d2) select join_day from default d0 where join_yr in a[*].join_yr and join_mo in b[*].join_mo order by join_day'
        verify_2 = 'select join_day from default d0 where join_yr in (select join_yr from default d1)[*].join_yr and join_mo in (select join_mo from default d2)[*].join_mo order by join_day'

        query_3 = 'with a as (select raw join_yr from default d1),b as (select raw join_mo from default d2) select join_day from default d0 where join_yr in a and join_mo in b order by join_day'
        verify_3 = 'select join_day from default d0 where join_yr in (select raw join_yr from default d1) and join_mo in (select raw join_mo from default d2) order by join_day'

        # mixed
        query_4 = 'with a as (select raw join_yr from default d1),b as (select raw join_mo from default d2), c as (2010), d as ("employee-9") select join_day from default d0 where join_yr in a and join_mo in b and join_yr >= c and name == d order by join_day'
        verify_4 = 'select join_day from default d0 where join_yr in (select raw join_yr from default d1) and join_mo in (select raw join_mo from default d2) and join_yr >= 2010 and name == "employee-9" order by join_day'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}

        self.query_runner(queries)

    def test_cte_alias(self):
        queries = dict()

        query_1 = 'with a as (2010), b as (select join_yr from default d1) select c as d from a as c order by d'
        verify_1 = 'select 2010 as d'

        query_2 = 'with a as (2010), b as (select join_yr from default d1) select c from a as c order by c'
        verify_2 = 'select 2010 as c'

        query_3 = 'with a as (2010), b as (select join_yr from default d1) select c.join_yr as d from b as c order by d'
        verify_3 = 'select join_yr as d from default order by d'

        query_4 = 'with a as (2010), b as (select join_yr from default d1) select c.join_yr from b as c order by c.join_yr'
        verify_4 = 'select join_yr from default order by join_yr'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}

        self.query_runner(queries)

    def test_system_table_cte(self):
        queries = dict()

        query_1 = 'with a as (2010) select a from system:indexes'
        verify_1 = 'select 2010 as a'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}

        self.query_runner(queries)

    def test_chained_cte(self):
        queries = dict()

        query_1 = 'with a as (2000), b as (a+10), c as (b+10) select join_yr from default where join_yr between a and c and join_yr != b order by join_yr'
        verify_1 = 'select join_yr from default where join_yr between 2000 and 2020 and join_yr != 2010 order by join_yr'

        query_2 = 'with a as (select raw join_yr from default d1), b as (select raw join_mo from default d2 where join_yr in a), c as (select raw join_day from default d3 where join_mo in b) select join_yr from default d0 where join_yr in a and join_mo in b and join_day in c order by join_yr'
        verify_2 = 'select join_yr from default d0 where join_yr in (select raw join_yr from default d1) and join_mo in (select raw join_mo from default d2 where join_yr in a) and join_day in (select raw join_day from default d3 where join_mo in b) order by join_yr'

        query_3 = 'with a as (2010), b as (a+10), c as (select raw join_yr from default d1 where join_yr > a), d as (select raw join_yr from default d2 where join_yr in c and join_yr < b) select join_yr from default d0 where join_yr in d order by join_yr'
        verify_3 = 'select join_yr from default d0 where join_yr in (select raw join_yr from default d2 where join_yr in (select raw join_yr from default d1 where join_yr > 2010) and join_yr < 2020) order by join_yr'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}

        self.query_runner(queries)

    def test_nested_cte(self):
        queries = dict()

        query_1 = 'with a as (with b as (2010) select raw c from b as c) select d from a as d'
        verify_1 = 'select 2010 as d'

        #query_2 = 'with a as (with b as (select raw join_yr from default d1) select raw join_mo from default d2 where join_yr in b) select join_yr from default d0 where join_mo in a'
        #verify_2 = ''

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}

        self.query_runner(queries)

    def test_chained_and_nested_cte(self):
        self.fail()

    def test_cte_joins(self):
        queries = dict()

        # hash join

        # no hint
        query_1 = 'with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from default AS table1 inner join with_table as table2 on (table1.join_yr == table2.d0.join_yr)'
        verify_1 = 'select * from default AS table1 inner join (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) as table2 on (table1.join_yr == table2.d0.join_yr)'

        query_2 = 'explain with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from default AS table1 inner join with_table as table2 on (table1.join_yr == table2.d0.join_yr)'

        # use hash(probe)
        query_3 = 'with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from default AS table1 inner join with_table as table2  use hash(probe) on (table1.join_yr == table2.d0.join_yr)'
        verify_3 = 'select * from default AS table1 inner join (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) as table2 use hash(probe) on (table1.join_yr == table2.d0.join_yr)'

        query_4 = 'explain with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from default AS table1 inner join with_table as table2 use hash(probe) on (table1.join_yr == table2.d0.join_yr)'

        # use hash(build)
        query_5 = 'with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from default AS table1 inner join with_table as table2  use hash(build) on (table1.join_yr == table2.d0.join_yr)'
        verify_5 = 'select * from default AS table1 inner join (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) as table2 use hash(build) on (table1.join_yr == table2.d0.join_yr)'

        query_6 = 'explain with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from default AS table1 inner join with_table as table2 use hash(build) on (table1.join_yr == table2.d0.join_yr)'

        # nested loop join

        query_7 = 'with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from with_table as table2 inner join default AS table1 on (table1.join_yr == table2.d0.join_yr)'
        verify_7 = 'select * from (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) as table2 inner join default AS table1 on (table1.join_yr == table2.d0.join_yr)'

        query_8 = 'explain with with_table as (select * from default d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from with_table as table2 inner join default AS table1 on (table1.join_yr == table2.d0.join_yr)'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.plan_verifier("HashJoin")]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.plan_verifier("HashJoin")]}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["f"] = {"queries": [query_6], "asserts": [self.plan_verifier("HashJoin")]}
        queries["g"] = {"queries": [query_7], "asserts": [self.verifier(verify_7)]}
        queries["h"] = {"queries": [query_8], "asserts": [self.plan_verifier("NestedLoopJoin")]}

        self.query_runner(queries)

    def test_cte_index_scan(self):
        queries = dict()

        query_1 = 'explain with a as (10) select a'

        query_2 = 'explain with a as (select join_yr from default) select a'

        query_3 = 'explain with a as (select join_yr from default) select a.join_yr'

        query_4 = 'explain with a as (select join_yr from default) select b.join_yr from a as b'

        query_5 = 'explain with a as (select raw join_yr from default) select join_yr from default where join_yr in a'

        query_6 = 'explain with a as (select raw join_yr from default) select join_yr from default where join_yr in a'
        index_6 = {'name': 'idx', 'bucket': 'default', 'fields': ["join_yr"], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        queries["a"] = {"queries": [query_1], "asserts": [self.plan_verifier("DummyScan")]}
        queries["b"] = {"queries": [query_2], "asserts": [self.plan_verifier("DummyScan")]}
        queries["c"] = {"queries": [query_3], "asserts": [self.plan_verifier("DummyScan")]}
        queries["d"] = {"queries": [query_4], "asserts": [self.plan_verifier("ExpressionScan")]}
        queries["e"] = {"queries": [query_5], "asserts": [self.plan_verifier("PrimaryScan3")]}
        queries["f"] = {"indexes": [index_6], "queries": [query_6], "asserts": [self.plan_verifier("IndexScan3")]}

        self.query_runner(queries)
