from .tuq import QueryTests


class QueryCTETests(QueryTests):

    def setUp(self):
        super(QueryCTETests, self).setUp()
        self.log.info("==============  QueryCTETests setup has started ==============")
        self.log.info("==============  QueryCTETests setup has completed ==============")
        self.log_config_info()
        self.query_bucket = self.get_query_buckets(check_all_buckets=True)[0]

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
        query_1 += 'select a, b, c, d, e, f, g, join_yr as h, join_yr+a as i, join_yr+b as j, join_yr+c as k, join_yr+d as l, join_yr+e as m, join_yr+f as n, join_yr+g as o from ' + self.query_bucket + ' '
        query_1 += 'where a > 9 and b == "bob" and c < 4 and d == 0 and e is valued and f is missing and g is null '
        query_1 += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o'
        verify_1 = 'select 10 as a, "bob" as b, 3.3 as c, 0 as d, "" as e, missing as f, null as g, join_yr as h, join_yr+10 as i, join_yr+"bob" as j, join_yr+3.3 as k, join_yr+0 as l, join_yr+"" as m, join_yr+missing as n, join_yr+null as o from ' + self.query_bucket + ' '
        verify_1 += 'where 10 > 9 and "bob" == "bob" and 3.3 < 4 and 0 == 0 and "" is valued and missing is missing and null is null '
        verify_1 += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o'

        # subquery
        query_2 = 'with a as (select join_yr from ' + self.query_bucket + ' ) select table1.join_yr from a as table1 order by table1.join_yr'
        verify_2 = 'select join_yr from ' + self.query_bucket + '  order by join_yr'

        query_3 = 'with a as (select join_yr from ' + self.query_bucket + ' ) select table1.join_yr from a as table1 where table1.join_yr in a[*].join_yr order by table1.join_yr'
        verify_3 = 'select join_yr from ' + self.query_bucket + '  where join_yr order by join_yr'

        query_4 = 'with a as (select join_yr from ' + self.query_bucket + ' ) select join_yr from ' + self.query_bucket + '  where join_yr in a[*].join_yr order by join_yr'
        verify_4 = 'select join_yr from ' + self.query_bucket + '  where join_yr order by join_yr'

        query_5 = 'with a as (select raw join_yr from ' + self.query_bucket + ' ) select join_yr from ' + self.query_bucket + '  where join_yr in a order by join_yr'
        verify_5 = 'select join_yr from ' + self.query_bucket + '  where join_yr order by join_yr'

        query_6 = 'with a as (select join_yr from ' + self.query_bucket + ' ) select table1.join_yr from a as table1 where table1.join_yr in a[*].join_yr order by table1.join_yr'
        verify_6 = 'select join_yr from ' + self.query_bucket + '  where join_yr order by join_yr'

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
        query_1 = 'with a as (2010), b as (2015) select join_yr from ' + self.query_bucket + '  where join_yr between a and b order by join_yr'
        verify_1 = 'select join_yr from ' + self.query_bucket + '  where join_yr between 2010 and 2015 order by join_yr'

        # subquery
        query_2 = 'with a as (select join_yr from ' + self.query_bucket + '  d1),b as (select join_mo from ' + self.query_bucket + '  d2) select join_day from ' + self.query_bucket + '  d0 where join_yr in a[*].join_yr and join_mo in b[*].join_mo order by join_day'
        verify_2 = 'select join_day from ' + self.query_bucket + '  d0 where join_yr in (select join_yr from ' + self.query_bucket + '  d1)[*].join_yr and join_mo in (select join_mo from ' + self.query_bucket + '  d2)[*].join_mo order by join_day'

        query_3 = 'with a as (select raw join_yr from ' + self.query_bucket + '  d1),b as (select raw join_mo from ' + self.query_bucket + '  d2) select join_day from ' + self.query_bucket + '  d0 where join_yr in a and join_mo in b order by join_day'
        verify_3 = 'select join_day from ' + self.query_bucket + '  d0 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d1) and join_mo in (select raw join_mo from ' + self.query_bucket + '  d2) order by join_day'

        # mixed
        query_4 = 'with a as (select raw join_yr from ' + self.query_bucket + '  d1),b as (select raw join_mo from ' + self.query_bucket + '  d2), c as (2010), d as ("employee-9") select join_day from ' + self.query_bucket + '  d0 where join_yr in a and join_mo in b and join_yr >= c and name == d order by join_day'
        verify_4 = 'select join_day from ' + self.query_bucket + '  d0 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d1) and join_mo in (select raw join_mo from ' + self.query_bucket + '  d2) and join_yr >= 2010 and name == "employee-9" order by join_day'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}

        self.query_runner(queries)

    def test_cte_alias(self):
        queries = dict()

        query_1 = 'with a as (2010), b as (select join_yr from ' + self.query_bucket + '  d1) select c as d from a as c order by d'
        verify_1 = 'select 2010 as d'

        query_2 = 'with a as (2010), b as (select join_yr from ' + self.query_bucket + '  d1) select c from a as c order by c'
        verify_2 = 'select 2010 as c'

        query_3 = 'with a as (2010), b as (select join_yr from ' + self.query_bucket + '  d1) select c.join_yr as d from b as c order by d'
        verify_3 = 'select join_yr as d from ' + self.query_bucket + '  order by d'

        query_4 = 'with a as (2010), b as (select join_yr from ' + self.query_bucket + '  d1) select c.join_yr from b as c order by c.join_yr'
        verify_4 = 'select join_yr from ' + self.query_bucket + '  order by join_yr'

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

        query_1 = 'with a as (2000), b as (a+10), c as (b+10) select join_yr from ' + self.query_bucket + '  where join_yr between a and c and join_yr != b order by join_yr'
        verify_1 = 'select join_yr from ' + self.query_bucket + '  where join_yr between 2000 and 2020 and join_yr != 2010 order by join_yr'

        query_2 = 'with usekeys as (select raw meta(def).id from ' + self.query_bucket + '  def), a as (select raw join_yr from ' + self.query_bucket + '  d1 use keys usekeys), b as (select raw join_mo from ' + self.query_bucket + '  d2 use keys usekeys where join_yr in a), c as (select raw join_day from ' + self.query_bucket + '  d3 use keys usekeys where join_mo in b) select join_yr from ' + self.query_bucket + '  d0 use keys usekeys where join_yr in a and join_mo in b and join_day in c order by join_yr'
        verify_2 = 'select join_yr from ' + self.query_bucket + '  d0 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d1) and join_mo in (select raw join_mo from ' + self.query_bucket + '  d2 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d1)) and join_day in (select raw join_day from ' + self.query_bucket + '  d3 where join_mo in (select raw join_mo from ' + self.query_bucket + '  d2 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d1))) order by join_yr'

        query_3 = 'with usekeys as (select raw meta(def).id from ' + self.query_bucket + '  def), a as (2010), b as (a+10), c as (select raw join_yr from ' + self.query_bucket + '  d1 use keys usekeys where join_yr > a), d as (select raw join_yr from ' + self.query_bucket + '  d2 use keys usekeys where join_yr in c and join_yr < b) select join_yr from ' + self.query_bucket + '  d0 use keys usekeys where join_yr in d order by join_yr'
        verify_3 = 'select join_yr from ' + self.query_bucket + '  d0 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d2 where join_yr in (select raw join_yr from ' + self.query_bucket + '  d1 where join_yr > 2010) and join_yr < 2020) order by join_yr'

        query_4 = 'with a as (select join_yr from ' + self.query_bucket + '  where join_yr > 2010), b as (select aa.join_yr from a as aa where aa.join_yr < 2012) select b.* from b'
        verify_4 = 'select join_yr from ' + self.query_bucket + '  where join_yr < 2012 and join_yr > 2010'

        # will fail until MB-32271 is fixed
        query_5 = 'with a as (select join_yr from ' + self.query_bucket + '  where join_yr > 2010), b as (select a1.join_yr from a a1 where a1.join_yr < 2012) select b.* from b'
        verify_5 = 'select join_yr from ' + self.query_bucket + '  where join_yr < 2012 and join_yr > 2010'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}

        self.query_runner(queries)

    def test_nested_cte(self):
        queries = dict()

        query_1 = 'with a as (with b as (2010) select raw c from b as c) select d from a as d'
        verify_1 = 'select 2010 as d'

        query_2 = 'with a as (with b as (select join_yr from ' + self.query_bucket + '  d1) select raw bb.join_yr from b as bb) select raw aa from a as aa order by aa'
        verify_2 = 'select raw join_yr from ' + self.query_bucket + '  d1 order by join_yr'

        # will fail until MB-31827 is fixed
        query_3 = 'with a as (with b as (select raw field1 from ' + self.query_bucket + '  d1 limit 1) select raw field2 from ' + self.query_bucket + '  d2 where field1 in b limit 1) select 1'
        verify_3 = 'select 1'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}

        self.query_runner(queries)

    def test_chained_and_nested_cte(self):
        queries = dict()

        query_1 = 'with a as (with b as (2010) select raw c from b as c) select d from a as d'
        verify_1 = 'select 2010 as d'

        query_2 = 'with a as (with b as (select join_yr from ' + self.query_bucket + '  d1) select raw bb.join_yr from b as bb) select raw aa from a as aa order by aa'
        verify_2 = 'select raw join_yr from ' + self.query_bucket + '  d1 order by join_yr'

        query_3 = 'with usekeys as (select raw meta(def).id from ' + self.query_bucket + '  def), a as (with aa as (select d0.join_yr from ' + self.query_bucket + '  d0) select raw aa.join_yr from aa), b as  (with bb as (select d1.join_yr from ' + self.query_bucket + '  d1) select raw bb.join_yr from bb where bb.join_yr in a), c as (select join_yr from ' + self.query_bucket + '  d2 use keys usekeys where join_yr in b) select c.join_yr from c order by c.join_yr'
        verify_3 = 'select join_yr from ' + self.query_bucket + '  order by join_yr'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}

        self.query_runner(queries)

    def test_cte_subquery(self):
        queries = dict()

        # subquery in select
        query_1 = 'select (with a as (select join_mo from ' + self.query_bucket + ' ) select a.join_mo from a where a.join_mo > 11) as b'
        verify_1 = 'select (select join_mo from ' + self.query_bucket + '  where join_mo > 11) as b'

        # subquery in from
        query_2 = 'select b.* from (with a as (select join_mo from ' + self.query_bucket + ' ) select a.join_mo from a where a.join_mo > 11) as b'
        verify_2 = 'select join_mo from ' + self.query_bucket + '  where join_mo > 11'

        # subquery in where
        query_3 = 'select join_mo from ' + self.query_bucket + '  where join_mo in (with a as (select join_mo from ' + self.query_bucket + '  d0) select raw a.join_mo from a where a.join_mo > 11)'
        verify_3 = 'select join_mo from ' + self.query_bucket + '  where join_mo > 11'

        # subquery in let
        query_4 = 'select raw a[0] as a from ' + self.query_bucket + '  let a=(with b as (select join_mo from ' + self.query_bucket + '  d0) select b.join_mo from b where b.join_mo = 12)'
        verify_4 = 'select 12 as join_mo from ' + self.query_bucket + ' '

        # subquery in letting
        query_5 = 'select count(join_mo) as count_join_mo, join_yr from ' + self.query_bucket + '  group by join_yr letting a=(with b as (select join_yr from ' + self.query_bucket + '  d0) select raw min(b.join_yr) from b) having join_yr>a[0]'
        verify_5 = 'select count(join_mo) as count_join_mo, join_yr from ' + self.query_bucket + '  where join_yr = 2011 group by join_yr'

        # subquery in having
        query_6 = 'select join_mo from ' + self.query_bucket + '  group by join_mo having join_mo in (with a as (select join_mo from ' + self.query_bucket + '  d0) select raw a.join_mo from a where a.join_mo = 5)'
        verify_6 = 'select join_mo from ' + self.query_bucket + '  where join_mo = 5 group by join_mo'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}
        queries["f"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["g"] = {"queries": [query_6], "asserts": [self.verifier(verify_6)]}

        self.query_runner(queries)

    def test_cte_joins(self):
        queries = dict()
        primary_index = {'name': '#primary', 'bucket': 'default', 'fields': [],
                                  'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}

        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}

        # hash join

        # no hint
        query_1 = 'with with_table as (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from ' + self.query_bucket + ' d1  inner join with_table on (d1.join_yr == with_table.d0.join_yr)'
        verify_1 = 'select * from ' + self.query_bucket + ' d1  inner join (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) as with_table on (d1.join_yr == with_table.d0.join_yr)'

        query_2 = 'explain with with_table as (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from ' + self.query_bucket + ' d1 inner join with_table on (d1.join_yr == with_table.d0.join_yr)'

        # use hash(probe)
        query_3 = 'with with_table as (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from ' + self.query_bucket + ' d1 inner join with_table use hash(probe) on (d1.join_yr == with_table.d0.join_yr)'
        verify_3 = 'select * from ' + self.query_bucket + ' d1 inner join (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) as with_table use hash(probe) on (d1.join_yr == with_table.d0.join_yr)'

        query_4 = 'explain with with_table as (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from ' + self.query_bucket + ' d1 inner join with_table use hash(probe) on (d1.join_yr == with_table.d0.join_yr)'

        # use hash(build)
        query_5 = 'with with_table as (select * from ' + self.query_bucket + '  d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from ' + self.query_bucket + ' d1 inner join with_table use hash(build) on (d1.join_yr == with_table.d0.join_yr)'
        verify_5 = 'select * from ' + self.query_bucket + ' d1 inner join (select * from ' + self.query_bucket + ' d0 where join_yr == 2010 order by meta(d0).id limit 1) as with_table use hash(build) on (d1.join_yr == with_table.d0.join_yr)'

        query_6 = 'explain with with_table as (select * from ' + self.query_bucket + ' d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from ' + self.query_bucket + ' d1 inner join with_table use hash(build) on (d1.join_yr == with_table.d0.join_yr)'

        # nested loop join

        query_7 = 'with with_table as (select * from ' + self.query_bucket + ' d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from with_table inner join ' + self.query_bucket + ' d1 on (d1.join_yr == with_table.d0.join_yr)'
        verify_7 = 'select * from (select * from ' + self.query_bucket + ' d0 where join_yr == 2010 order by meta(d0).id limit 1) as with_table inner join ' + self.query_bucket + ' d1 on (d1.join_yr == with_table.d0.join_yr)'

        query_8 = 'explain with with_table as (select * from ' + self.query_bucket + ' d0 where join_yr == 2010 order by meta(d0).id limit 1) select * from with_table inner join ' + self.query_bucket + ' d1 on (d1.join_yr == with_table.d0.join_yr)'

        queries["a"] = {"indexes": [primary_index, index_1], "queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"indexes": [primary_index, index_1], "queries": [query_2], "asserts": [self.plan_verifier("HashJoin")]}
        queries["c"] = {"indexes": [primary_index, index_1], "queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"indexes": [primary_index, index_1], "queries": [query_4], "asserts": [self.plan_verifier("HashJoin")]}
        queries["e"] = {"indexes": [primary_index, index_1], "queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["f"] = {"indexes": [primary_index, index_1], "queries": [query_6], "asserts": [self.plan_verifier("HashJoin")]}
        queries["g"] = {"indexes": [primary_index, index_1], "queries": [query_7], "asserts": [self.verifier(verify_7)]}
        queries["h"] = {"indexes": [primary_index, index_1], "queries": [query_8], "asserts": [self.plan_verifier("HashJoin")]}
        self.query_runner(queries)

    def test_cte_index_scan(self):
        queries = dict()

        query_1 = 'explain with a as (10) select a'

        query_2 = 'explain with a as (select join_yr from ' + self.query_bucket + ' ) select a'

        query_3 = 'explain with a as (select join_yr from ' + self.query_bucket + ' ) select a.join_yr'

        query_4 = 'explain with a as (select join_yr from ' + self.query_bucket + ' ) select b.join_yr from a as b'

        query_5 = 'explain with a as (select raw join_yr from ' + self.query_bucket + ' ) select join_yr from ' + self.query_bucket + '  where join_yr in a'

        query_6 = 'explain with a as (select raw join_yr from ' + self.query_bucket + ' ) select join_yr from ' + self.query_bucket + '  where join_yr in a'
        index_6 = {'name': 'idx', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        primary = {'name': '#primary', 'bucket': 'default', 'fields': frozenset(), 'state': 'online',
                   'using': 'gsi', 'where': '', 'is_primary': True}


        queries["a"] = {"queries": [query_1], "asserts": [self.plan_verifier("DummyScan")]}
        queries["b"] = {"queries": [query_2], "asserts": [self.plan_verifier("DummyScan")]}
        queries["c"] = {"queries": [query_3], "asserts": [self.plan_verifier("DummyScan")]}
        queries["d"] = {"queries": [query_4], "asserts": [self.plan_verifier("ExpressionScan")]}
        queries["e"] = {"queries": [query_5], "asserts": [self.plan_verifier("PrimaryScan3")]}
        queries["f"] = {"indexes": [index_6, primary], "queries": [query_6], "asserts": [self.plan_verifier("IndexScan3")]}

        self.query_runner(queries)

    def test_MB59170(self):
        cte_nested_query = 'with cte2 as ( with cte1 as ( select 1 from [] as t1 ) select 1 from ( select 1 from cte1 ) as t2 ) select 1 from cte2'
        result = self.run_cbq_query(cte_nested_query, server=self.server)
        self.assertEqual(result['results'], [])

    def test_MB54045(self):
        self.run_cbq_query(f'DELETE FROM {self.query_bucket} WHERE myid is not missing')
        self.run_cbq_query(f'INSERT INTO {self.query_bucket} VALUES("k01", {{"myid":1}})')
        self.run_cbq_query(f'CREATE INDEX myix1 IF NOT EXISTS ON {self.query_bucket} (myid)')

        cte_query = f'with cte1 as ( select * from ( select t1.*,t2.* from [{{"a":1}}] as t1 join [{{"b":1}}] as t2 on t1.a = t2.b) lhs join {self.query_bucket} AS c1 use hash(build) on lhs.a = c1.myid ) select raw 1 from  cte1'
        result = self.run_cbq_query(cte_query)
        expected_result = [1]
        self.assertEqual(result['results'], expected_result)