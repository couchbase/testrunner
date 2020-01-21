from .tuq import QueryTests


class QueryChainedLetTests(QueryTests):

    def setUp(self):
        super(QueryChainedLetTests, self).setUp()
        self.log.info("==============  QueryChainedLetTests setup has started ==============")
        self.log.info("==============  QueryChainedLetTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryChainedLetTests, self).suite_setUp()
        self.log.info("==============  QueryChainedLetTests suite_setup has started ==============")
        self.log.info("==============  QueryChainedLetTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryChainedLetTests tearDown has started ==============")
        self.log.info("==============  QueryChainedLetTests tearDown has completed ==============")
        super(QueryChainedLetTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryChainedLetTests suite_tearDown has started ==============")
        self.log.info("==============  QueryChainedLetTests suite_tearDown has completed ==============")
        super(QueryChainedLetTests, self).suite_tearDown()

    def verifier(self, compare_query):
        return lambda x: self.compare_queries(x['q_res'][0], compare_query)

    def compare_queries(self, actual_results, compare_query):
        let_letting_docs = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(let_letting_docs), len(compare_docs))
        self.assertEqual(let_letting_docs, compare_docs)

    # creates a let query for different scenarios and and equivalent query without let to compare results with
    def test_basic_chained_let(self):
        queries = dict()

        # constants
        query_1 = 'select a, b, c from default let a=1,b=2,c=3 order by a, b, c limit 10'
        verify_1 = 'select 1 as a, 2 as b, 3 as c from default order by a, b, c limit 10'

        query_2 = 'select a, b, c from default let a=cos(1),b=cos(2),c=cos(3) order by a, b, c limit 10'
        verify_2 = 'select cos(1) as a, cos(2) as b, cos(3) as c from default order by a, b, c limit 10'

        query_3 = 'select a, b, c from default let a=1,b=a+1,c=b+1 order by a, b, c limit 10'
        verify_3 = 'select 1 as a, 2 as b, 3 as c from default order by a, b, c limit 10'

        query_4 = 'select a, b, c from default let a=cos(1),b=cos(a+1),c=cos(b+1) order by a, b, c limit 10'
        verify_4 = 'select cos(1) as a, cos(cos(1)+1) as b, cos(cos(cos(1)+1)+1) as c from default order by a, b, c limit 10'

        # fields
        query_5 = 'select a, b, c from default let a=join_yr,b=join_day,c=join_mo order by a, b, c limit 10'
        verify_5 = 'select join_yr as a, join_day as b, join_mo as c from default order by a, b, c limit 10'

        query_6 = 'select a, b, c from default let a=cos(join_yr+1),b=cos(join_day+1),c=cos(join_mo+1) order by a, b, c limit 10'
        verify_6 = 'select cos(join_yr+1) as a, cos(join_day+1) as b, cos(join_mo+1) as c from default order by a, b, c limit 10'

        query_7 = 'select a, b, c from default let a=join_yr,b=a+join_day,c=b+join_mo order by a, b, c limit 10'
        verify_7 = 'select join_yr as a, join_yr+join_day as b, join_yr+join_day+join_mo as c from default order by a, b, c limit 10'

        query_8 = 'select a, b, c from default let a=cos(join_yr+1),b=cos(a+join_day+1),c=cos(b+join_mo+1) order by a, b, c limit 10'
        verify_8 = 'select cos(join_yr+1) as a, cos(cos(join_yr+1)+join_day+1) as b, cos(cos(cos(join_yr+1)+join_day+1)+join_mo+1) as c from default order by a, b, c limit 10'

        # subqueries
        query_9 = 'select a, b, c from default d0 let a=(select join_yr from default d2 order by join_yr limit 5), b=(select join_mo from default d3 order by join_mo limit 5), c=(select join_day from default d4 order by join_day limit 5) order by a, b, c limit 10'
        verify_9 = 'select (select join_yr from default d2 order by join_yr limit 5) as a, (select join_mo from default d3 order by join_mo limit 5) as b, (select join_day from default d4 order by join_day limit 5) as c from default d0 order by a, b, c limit 10'

        query_10 = 'select a, b from default d0 let usekeys=(select meta(d1).id, join_day from default d1 limit 10), a=(select join_day from default d2 limit 10), b=(select join_mo from default d3 use keys usekeys[*].id where join_day in a[*].join_day limit 10) order by a, b limit 10'
        verify_10 = 'select (select join_day from default d1 limit 10) as a, (select join_mo from default d2 where join_mo in (select raw join_mo from default d3 limit 10) limit 10) as b from default d7 order by a, b limit 10'

        query_11 = 'select a, b, c from default d0 let usekeys=(select raw meta(d1).id from default d1 order by meta(d1).id limit 10),a=1,b=join_mo+a,c=(select join_day from default d2 use keys usekeys where join_day != b order by join_day limit 10) order by a, b, c limit 10'
        verify_11 = 'select 1 as a, join_mo+1 as b, (select join_day from default d2 use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 10) where join_day != (join_mo+1) order by join_day limit 10) as c from default d0 order by a, b, c limit 10'

        # full query
        query_12 = 'select a, b, c from default let a=join_yr, b=join_mo, c=join_day where (a > 100 OR c < 100) and b != 200 group by a,b,c having (a>b and b>c) or a == b+c order by a, b, c limit 10'
        verify_12 = 'select join_yr as a, join_mo as b, join_day as c from default where (join_yr > 100 OR join_day < 100) and join_mo != 200 group by join_yr, join_mo, join_day having (join_yr>join_mo and join_mo>join_day) or join_yr == join_mo+join_day order by join_yr, join_mo, join_day limit 10'

        # mixed
        query_13 = 'select a, b, c from default d0 let usekeys=(select raw meta(d1).id from default d1 order by meta(d1).id limit 10),a=1,b=cos(join_day)+a,c=(select join_day from default d1 use keys usekeys where join_day > a + b order by join_day limit 10) where join_day in c order by a,b,c limit 10'
        verify_13 = 'select 1 as a, cos(join_day)+1 as b, (select join_day from default d1 use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 10) where join_day > 1 + cos(join_day)+1 order by join_day limit 10) as c from default d0 where join_day in (select join_day from default d1 use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 10) where join_day > 1 + cos(join_day)+1 order by join_day limit 10) order by a,b,c limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["c"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["f"] = {"queries": [query_6], "asserts": [self.verifier(verify_6)]}
        queries["g"] = {"queries": [query_7], "asserts": [self.verifier(verify_7)]}
        queries["h"] = {"queries": [query_8], "asserts": [self.verifier(verify_8)]}
        queries["i"] = {"queries": [query_9], "asserts": [self.verifier(verify_9)]}
        queries["j"] = {"queries": [query_10], "asserts": [self.verifier(verify_10)]}
        queries["k"] = {"queries": [query_11], "asserts": [self.verifier(verify_11)]}
        queries["l"] = {"queries": [query_12], "asserts": [self.verifier(verify_12)]}
        queries["m"] = {"queries": [query_13], "asserts": [self.verifier(verify_13)]}

        self.query_runner(queries)

    def test_basic_chained_letting(self):
        queries = dict()

        # constants
        query_1 = 'select a, b, c from default group by a, b, c letting a=1,b=2,c=3 order by a, b, c'
        verify_1 = 'select 1 as a, 2 as b, 3 as c from default group by a, b, c order by a, b, c'

        query_2 = 'select a, b, c from default group by a, b, c letting a=cos(1),b=cos(2),c=cos(3) order by a, b, c'
        verify_2 = 'select cos(1) as a, cos(2) as b, cos(3) as c from default group by a, b, c order by a, b, c'

        query_3 = 'select a, b, c from default group by a, b, c letting a=1,b=a+1,c=b+1 order by a, b, c'
        verify_3 = 'select 1 as a, 2 as b, 3 as c from default group by a, b, c order by a, b, c'

        query_4 = 'select a, b, c from default group by a, b, c letting a=cos(1),b=cos(a+1),c=cos(b+1) order by a, b, c'
        verify_4 = 'select cos(1) as a, cos(cos(1)+1) as b, cos(cos(cos(1)+1)+1) as c from default group by a, b, c order by a, b, c'

        # fields
        query_5 = 'select a, b, c from default group by join_yr, join_mo, join_day letting a=join_yr,b=join_mo,c=join_day order by a, b, c'
        verify_5 = 'select join_yr as a, join_mo as b, join_day as c from default group by join_yr, join_mo, join_day order by a, b, c'

        query_6 = 'select a, b, c from default group by join_yr, join_mo, join_day letting a=join_yr,b=a+join_mo,c=b+join_day order by a, b, c'
        verify_6 = 'select join_yr as a, join_yr+join_mo as b, join_yr+join_mo+join_day as c from default group by join_yr, join_mo, join_day order by a, b, c'

        query_7 = 'select a, b, c from default group by join_yr, join_mo, join_day letting a=cos(join_yr+1),b=cos(join_mo+1),c=cos(join_day+1) order by a, b, c'
        verify_7 = 'select cos(join_yr+1) as a, cos(join_mo+1) as b, cos(join_day+1) as c from default group by join_yr, join_mo, join_day order by a, b, c'

        query_8 = 'select a, b, c from default group by join_yr, join_mo, join_day letting a=cos(join_yr+1),b=cos(a+join_mo+1),c=cos(b+join_day+1) order by a, b, c'
        verify_8 = 'select cos(join_yr+1) as a, cos(cos(join_yr+1)+join_mo+1) as b, cos(cos(cos(join_yr+1)+join_mo+1)+join_day+1) as c from default group by join_yr, join_mo, join_day order by a, b, c'

        # subqueries
        query_9 = 'select a, b, c from default d0 group by join_yr, join_mo, join_day letting a=(select join_yr from default d1 order by join_yr limit 10),b=(select join_mo from default d2 order by join_mo limit 10),c=(select join_day from default d3 order by join_day limit 10) order by a, b, c limit 10'
        verify_9 = 'select (select join_yr from default d1 order by join_yr limit 10)  as a, (select join_mo from default d2 order by join_mo limit 10) as b, (select join_day from default d3 order by join_day limit 10) as c from default d0 group by join_yr, join_mo, join_day order by a, b, c limit 10'

        # full query
        query_10 = 'select a, b, c from default d0 where join_yr > 0 group by join_yr letting a=join_yr,b=SUM(join_mo),c=SUM(join_day),d=b+c having d > 0 and a > 0 order by a, b, c limit 10'
        verify_10 = 'select join_yr as a, SUM(join_mo) as b, SUM(join_day) as c from default d0 where join_yr > 0 group by join_yr having SUM(join_mo)+SUM(join_day) > 0  and  join_yr > 0 order by a, b, c limit 10'

        # mixed
        query_11 = 'select a, b, c from default d0 where join_yr > 0 group by join_yr letting aa=1,aaa=aa+1,aaaa=join_yr,a=aaaa+aaa,b=SUM(join_mo),c=SUM(join_day),d=b+c,e=(select raw join_yr from default d1 order by join_yr) having d > 0 and a > 0  and aaaa in e order by a, b, c limit 10'
        verify_11 = 'select join_yr+2 as a, SUM(join_mo) as b, SUM(join_day) as c from default d0 where join_yr > 0 group by join_yr having SUM(join_mo) + SUM(join_day) > 0 and join_yr + 2 > 0 and join_yr in (select raw join_yr from default d1 order by join_yr) order by a, b, c limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["f"] = {"queries": [query_6], "asserts": [self.verifier(verify_6)]}
        queries["g"] = {"queries": [query_7], "asserts": [self.verifier(verify_7)]}
        queries["h"] = {"queries": [query_8], "asserts": [self.verifier(verify_8)]}
        queries["i"] = {"queries": [query_9], "asserts": [self.verifier(verify_9)]}
        queries["j"] = {"queries": [query_10], "asserts": [self.verifier(verify_10)]}
        queries["k"] = {"queries": [query_11], "asserts": [self.verifier(verify_11)]}

        self.query_runner(queries)

    def test_chained_let_and_letting(self):
        queries = dict()

        # constants
        query_1 = 'select a, b, c, aa, bb, cc from default let a=1,b=2,c=3 group by a, b, c letting aa=a+1,bb=b+2,cc=c+3 order by a, b, c'
        verify_1 = 'select 1 as a, 2 as b, 3 as c, 2 as aa, 4 as bb, 6 as cc from default group by a, b, c order by a, b, c'

        # fields
        query_2 = 'select a, b, c, aa, bb, cc from default let a=join_yr,b=join_day,c=join_mo group by a, b, c letting aa=cos(a),bb=aa+cos(b),cc=bb+c having aa > 0 and bb > aa and cc > 0 order by a, b, c limit 10'
        verify_2 = 'select join_yr as a, join_day as b, join_mo as c, cos(join_yr) as aa, cos(join_yr)+cos(join_day) as bb, cos(join_yr)+cos(join_day)+join_mo as cc from default group by join_yr, join_day, join_mo having cos(join_yr) > 0 and cos(join_yr)+cos(join_day) > cos(join_yr) and cos(join_yr)+cos(join_day)+join_mo > 0 order by a, b, c limit 10'

        # subqueries
        query_3 = 'select a, b, c, aa, bb from default d0 let usekeys=(select raw meta(d1).id from default d1 order by meta(d1).id limit 20),a=(select raw join_day from default d2),b=(select raw join_mo from default d3 use keys usekeys where join_mo in a),c=join_yr group by a,b,c letting aa=(1 in a),bb=COUNT((select raw meta(d4).id from default d4 order by meta(d4).id limit 10)) having aa and bb != 0 order by a, b, c limit 10'
        verify_3 = 'select (select raw join_day from default d2) as a, (select raw join_mo from default d3 use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 20) where join_mo in (select raw join_day from default d6)) as b, join_yr as c, (1 in (select raw join_day from default d2)) as aa, COUNT((select raw meta(d4).id from default d4 order by meta(d4).id limit 10)) as bb from default d0 group by a,b,join_yr having (1 in (select raw join_day from default d2)) and COUNT((select raw meta(d5).id from default d5 order by meta(d5).id limit 10)) != 0 order by a, b, c limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}

        self.query_runner(queries)

    def test_chained_let_in_let_subquery(self):
        queries = dict()

        query_1 = 'select a, b, c from default d0 let usekeys=(select raw meta(d1).id from default d1 order by meta(d1).id limit 20), a=join_yr, b=a+1, c=(select e from default d4 use keys usekeys let d=b+1,e=d+1 order by join_day limit 5) order by a, b, c limit 10'
        verify_1 = 'select join_yr as a, join_yr+1 as b, (select d0.join_yr+3 as e from default d4 use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 20) order by join_day limit 5) as c from default d0 order by a, b, c limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}

        self.query_runner(queries)

    def test_chained_let_in_letting_subquery(self):
        queries = dict()

        query_1 = 'select a from default d0 group by join_yr letting a=(select bb from default d1 let aa=join_yr,bb=aa+1 order by join_yr limit 10) order by a limit 10'
        verify_1 = 'select (select join_yr+1 as bb from default d1 order by join_yr limit 10) as a from default d0 group by join_yr order by a limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}

        self.query_runner(queries)

    def test_chained_letting_in_let_subquery(self):
        queries = dict()

        query_1 = 'select a from default d0 let a=(select aaa from default d2 let aaa=join_yr,aaaa=aaa+1 group by aaa, aaaa letting bb=aaaa+1,cc=bb+1 having cc > 0 order by aaa limit 5) group by a order by a limit 10'
        verify_1 = 'select (select join_yr as aaa from default d2 group by join_yr, join_yr + 1 having join_yr+3 > 0 order by join_yr limit 5) as a from default d0 group by a order by a limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}

        self.query_runner(queries)

    def test_chained_letting_in_letting_subquery(self):
        queries = dict()

        query_1 = 'select a from default d0 group by join_yr letting a=(select bb from default d1 let aa=join_yr,bb=aa+1 group by bb letting cc=bb+1,dd=cc+1 having dd > 0 order by bb limit 10) order by a limit 10'
        verify_1 = 'select (select join_yr+1 as bb from default d1 group by join_yr + 1 having join_yr + 1 + 2 > 0 order by join_yr+1 limit 10) as a from default d0 group by join_yr order by a limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}

        self.query_runner(queries)

    def test_chained_let_and_letting_index_selection(self):
        self.fail()

    def test_chained_let_and_letting_with_joins(self):
        self.fail()

    def test_chained_let_and_letting_negative(self):
        self.fail()
