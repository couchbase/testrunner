from .tuq import QueryTests
from membase.api.exception import CBQError
import pdb

class GroupByAliasTests(QueryTests):

    def setUp(self):
        super(GroupByAliasTests, self).setUp()

    def tearDown(self):
        super(GroupByAliasTests, self).tearDown()

    def suite_setUp(self):
        super(GroupByAliasTests, self).suite_setUp()

    def suite_tearDown(self):
        super(GroupByAliasTests, self).suite_tearDown()

    def run_all(self):
        self.test_alias_group_by_function_call()
        self.test_alias_grooup_by_field_alias()
        self.test_field_alias_group_by_not_in_select()
        self.test_alias_mixed_field_aliases_usage()
        self.test_alias_mixed_usage_including_order_by()
        self.test_alias_mixed_usage_order_by_not_select()
        self.test_alias_mixed_usage_select_group_by_2()
        self.test_alias_select_function_argument()
        self.test_alias_select_function_not_using()
        self.test_alias_not_select_function_argument()
        self.test_alias_not_select_not_function()
        self.test_index_pushdown_1()
        self.test_index_pushdown_3()
        self.test_index_pushdown_4()
        self.test_index_pushdown_5()
        self.test_negative_1()

    def test_alias_group_by_function_call(self):
        alias_query = "SELECT count(*) as employee_count from default as o group by DATE_PART_STR(o.join_mo, 'month') as month"
        simple_query = "SELECT count(*) as employee_count from default group by DATE_PART_STR(join_mo, 'month') as month"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        self.assertEqual(alias_result['results'][0], simple_result['results'][0], "Alias in group by function call is failed.")

    def test_alias_grooup_by_field_alias(self):
        alias_query = "select job_title as jt from default where email like '%couchbase.com' group by job_title as jt order by jt asc"
        simple_query = "select job_title from default where email like '%couchbase.com' group by job_title order by job_title asc"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = []
        simple_results = []
        for res in alias_result['results']:
            alias_results.append(list(res.values())[0])
        for res in simple_result['results']:
            simple_results.append(list(res.values())[0])
        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_field_alias_group_by_not_in_select(self):
        alias_query = "select job_title from default where email like '%couchbase.com' group by job_title as jt order by jt asc"
        simple_query = "select job_title from default where email like '%couchbase.com' group by job_title order by job_title asc"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = []
        simple_results = []
        for res in alias_result['results']:
            alias_results.append(list(res.values())[0])
        for res in simple_result['results']:
            simple_results.append(list(res.values())[0])
        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_mixed_field_aliases_usage(self):
        alias_query = "select jd,jm,join_yr from default where email like '%couchbase.com' group by join_day as jd, join_mo as jm, join_yr order by jd,jm,join_yr asc"
        simple_query = "select join_day, join_mo, join_yr from default where email like '%couchbase.com' group by join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['jd'])
            alias_results[counter].append(res['jm'])
            alias_results[counter].append(res['join_yr'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['join_day'])
            simple_results[counter].append(res['join_mo'])
            simple_results[counter].append(res['join_yr'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_mixed_usage_including_order_by(self):
        alias_query = "select join_day,jm,join_yr from default where email like '%couchbase.com' group by join_day as jd, join_mo as jm, join_yr order by jd,jm,join_yr asc"
        simple_query = "select join_day, join_mo, join_yr from default where email like '%couchbase.com' group by join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['join_day'])
            alias_results[counter].append(res['jm'])
            alias_results[counter].append(res['join_yr'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['join_day'])
            simple_results[counter].append(res['join_mo'])
            simple_results[counter].append(res['join_yr'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_mixed_usage_order_by_not_select(self):
        alias_query = "select join_day,join_mo,join_yr from default where email like '%couchbase.com' group by join_day as jd, join_mo as jm, join_yr order by jd,jm,join_yr asc"
        simple_query = "select join_day, join_mo, join_yr from default where email like '%couchbase.com' group by join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['join_day'])
            alias_results[counter].append(res['join_mo'])
            alias_results[counter].append(res['join_yr'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['join_day'])
            simple_results[counter].append(res['join_mo'])
            simple_results[counter].append(res['join_yr'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_mixed_usage_select_group_by_2(self):
        alias_query = "select jd,join_mo,join_yr from default where email like '%couchbase.com' group by join_day as jd, join_mo as jm, join_yr order by jd,jm,join_yr asc"
        simple_query = "select join_day, join_mo, join_yr from default where email like '%couchbase.com' group by join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['jd'])
            alias_results[counter].append(res['join_mo'])
            alias_results[counter].append(res['join_yr'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['join_day'])
            simple_results[counter].append(res['join_mo'])
            simple_results[counter].append(res['join_yr'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_select_function_argument(self):
        alias_query = "SELECT s, ARRAY_CONTAINS(s, 'skill2010') AS array_contains_value FROM default group by skills s"
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM default group by skills s"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['array_contains_value'])
            alias_results[counter].append(res['s'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['array_contains_value'])
            simple_results[counter].append(res['skills'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_select_function_not_using(self):
        alias_query = "SELECT s, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM default group by skills s"
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM default group by skills s"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['array_contains_value'])
            alias_results[counter].append(res['s'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['array_contains_value'])
            simple_results[counter].append(res['skills'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_not_select_function_argument(self):
        alias_query = "SELECT skills, ARRAY_CONTAINS(s, 'skill2010') AS array_contains_value FROM default group by skills s"
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM default group by skills s"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['array_contains_value'])
            alias_results[counter].append(res['skills'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['array_contains_value'])
            simple_results[counter].append(res['skills'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_alias_not_select_not_function(self):
        alias_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM default group by skills s"
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM default group by skills s"
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        alias_results = {}
        simple_results = {}
        counter = 0
        for res in alias_result['results']:
            alias_results[counter] = []
            alias_results[counter].append(res['array_contains_value'])
            alias_results[counter].append(res['skills'])
            counter = counter + 1
        counter = 0
        for res in simple_result['results']:
            simple_results[counter] = []
            simple_results[counter].append(res['array_contains_value'])
            simple_results[counter].append(res['skills'])
            counter = counter + 1

        self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")

    def test_index_pushdown_1(self):
        self.create_indexes()
        alias_query = "explain SELECT count(*) as employee_count from default as o where join_mo is not null group by DATE_PART_STR(o.join_mo, 'month') as month"
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find("index_group_aggs") > 0, "Alias in group by function call is failed.")
        finally:
            self.drop_indexes()

    def test_index_pushdown_3(self):
        self.create_indexes()
        alias_query = "explain select job_title as jt from default where job_title is not null group by job_title as jt"
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find('index_group_aggs') > 0, "Alias in group by is failed.")
        finally:
            self.drop_indexes()

    def test_index_pushdown_4(self):
        self.create_indexes()
        alias_query = "explain select jd,jm,join_yr from default where join_day is not null and join_mo is not null and join_yr is not null group by join_day as jd, join_mo as jm, join_yr"
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find('index_group_aggs') > 0, "Alias in group by is failed.")
        finally:
            self.drop_indexes()

    def test_index_pushdown_5(self):
        self.create_indexes()
        alias_query = "explain select jd, join_mo, join_yr from default where join_day is not null and join_mo is not null and join_yr is not null group by join_day as jd, join_mo, join_yr"
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find('index_group_aggs') > 0, "Alias in group by is failed.")
        finally:
            self.drop_indexes()

    def test_negative_1(self):
        query = "select email as eml from default where email is not null group by eml as em"
        error_detected = False
        try:
            self.run_cbq_query(query)
        except CBQError as e:
            error_detected = True

        self.assertEqual(error_detected, True, query)


    def create_indexes(self):
        self.run_cbq_query('CREATE INDEX ix1 ON default(join_mo)')
        self.run_cbq_query('CREATE INDEX ix2 ON default(job_title)')
        self.run_cbq_query('CREATE INDEX ix3 ON default(join_day)')
        self.run_cbq_query('CREATE INDEX ix4 ON default(join_yr)')
        self.run_cbq_query('CREATE INDEX ix5 ON default(join_day, join_mo, join_yr)')

    def drop_indexes(self):
        self.run_cbq_query('DROP INDEX default.ix1')
        self.run_cbq_query('DROP INDEX default.ix2')
        self.run_cbq_query('DROP INDEX default.ix3')
        self.run_cbq_query('DROP INDEX default.ix4')
        self.run_cbq_query('DROP INDEX default.ix5')
