from membase.api.exception import CBQError
from .tuq import QueryTests


class GroupByAliasTests(QueryTests):

    def setUp(self):
        super(GroupByAliasTests, self).setUp()
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.query_bucket = self.query_buckets[0]

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
        alias_query = "SELECT count(*) as employee_count from {0} as o group by DATE_PART_STR(o.join_mo, 'month')" \
                      " as month".format(self.query_bucket)
        simple_query = "SELECT count(*) as employee_count from {0} group by DATE_PART_STR(join_mo, 'month') " \
                       "as month".format(self.query_bucket)
        alias_result = self.run_cbq_query(alias_query)
        simple_result = self.run_cbq_query(simple_query)
        self.assertEqual(alias_result['results'][0], simple_result['results'][0],
                         "Alias in group by function call is failed.")

    def test_alias_grooup_by_field_alias(self):
        alias_query = "select job_title as jt from {0} where email like '%couchbase.com' group by job_title as jt" \
                      " order by jt asc".format(self.query_bucket)
        simple_query = "select job_title from {0} where email like '%couchbase.com' group by job_title order by" \
                       " job_title asc".format(self.query_bucket)
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
        alias_query = "select job_title from {0} where email like '%couchbase.com' group by job_title as jt" \
                      " order by jt asc".format(self.query_bucket)
        simple_query = "select job_title from {0} where email like '%couchbase.com' group by job_title order" \
                       " by job_title asc".format(self.query_bucket)
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
        alias_query = "select jd,jm,join_yr from {0} where email like '%couchbase.com' group by join_day as jd, " \
                      "join_mo as jm, join_yr order by jd,jm,join_yr asc".format(self.query_bucket)
        simple_query = "select join_day, join_mo, join_yr from {0} where email like '%couchbase.com' group by " \
                       "join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc".format(self.query_bucket)
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
        alias_query = "select join_day,jm,join_yr from {0} where email like '%couchbase.com' group by join_day as jd," \
                      " join_mo as jm, join_yr order by jd,jm,join_yr asc".format(self.query_bucket)
        simple_query = "select join_day, join_mo, join_yr from {0} where email like '%couchbase.com' group by " \
                       "join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc".format(self.query_bucket)
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
        alias_query = "select join_day,join_mo,join_yr from {0} where email like '%couchbase.com' group by join_day " \
                      "as jd, join_mo as jm, join_yr order by jd,jm,join_yr asc".format(self.query_bucket)
        simple_query = "select join_day, join_mo, join_yr from {0} where email like '%couchbase.com' group by " \
                       "join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc".format(self.query_bucket)
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
        alias_query = "select jd,join_mo,join_yr from {0} where email like '%couchbase.com' group by join_day as jd," \
                      " join_mo as jm, join_yr order by jd,jm,join_yr asc".format(self.query_bucket)
        simple_query = "select join_day, join_mo, join_yr from {0} where email like '%couchbase.com' group by " \
                       "join_day, join_mo, join_yr order by join_day, join_mo, join_yr asc".format(self.query_bucket)
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
        alias_query = "SELECT s, ARRAY_CONTAINS(s, 'skill2010') AS array_contains_value FROM {0} group by " \
                      "skills s".format(self.query_bucket)
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM {0} " \
                       "group by skills s".format(self.query_bucket)
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
        alias_query = "SELECT s, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM {0} group" \
                      " by skills s".format(self.query_bucket)
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM {0} group" \
                       " by skills s".format(self.query_bucket)
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
        alias_query = "SELECT skills, ARRAY_CONTAINS(s, 'skill2010') AS array_contains_value FROM {0} group by " \
                      "skills s".format(self.query_bucket)
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM {0} group by" \
                       " skills s".format(self.query_bucket)
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
        alias_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM {0} " \
                      "group by skills s".format(self.query_bucket)
        simple_query = "SELECT skills, ARRAY_CONTAINS(skills, 'skill2010') AS array_contains_value FROM {0} group by " \
                       "skills s".format(self.query_bucket)
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
        alias_query = "explain SELECT count(*) as employee_count from {0} as o where join_mo is not null group by " \
                      "DATE_PART_STR(o.join_mo, 'month') as month".format(self.query_bucket)
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find("index_group_aggs") > 0,
                            "Alias in group by function call is failed.")
        finally:
            self.drop_indexes()

    def test_index_pushdown_3(self):
        self.create_indexes()
        alias_query = "explain select job_title as jt from {0} where job_title is not null group by " \
                      "job_title as jt".format(self.query_bucket)
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find('index_group_aggs') > 0, "Alias in group by is failed.")
        finally:
            self.drop_indexes()

    def test_index_pushdown_4(self):
        self.create_indexes()
        alias_query = "explain select jd,jm,join_yr from {0} where join_day is not null and join_mo is not null and" \
                      " join_yr is not null group by join_day as jd, join_mo as jm, join_yr".format(self.query_bucket)
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find('index_group_aggs') > 0, "Alias in group by is failed.")
        finally:
            self.drop_indexes()

    def test_index_pushdown_5(self):
        self.create_indexes()
        alias_query = "explain select jd, join_mo, join_yr from {0} where join_day is not null and join_mo is not " \
                      "null and join_yr is not null group by join_day as jd, join_mo, join_yr".format(self.query_bucket)
        alias_result = self.run_cbq_query(alias_query)
        try:
            self.assertTrue(str(alias_result).find('index_group_aggs') > 0, "Alias in group by is failed.")
        finally:
            self.drop_indexes()

    def test_negative_1(self):
        query = "select email as eml from {0} where email is not null group by eml as em".format(self.query_bucket)
        error_detected = False
        try:
            self.run_cbq_query(query)
        except CBQError:
            error_detected = True

        self.assertEqual(error_detected, True, query)

    def create_indexes(self):
        self.run_cbq_query('CREATE INDEX ix1 ON {0}(join_mo)'.format(self.query_bucket))
        self.run_cbq_query('CREATE INDEX ix2 ON {0}(job_title)'.format(self.query_bucket))
        self.run_cbq_query('CREATE INDEX ix3 ON {0}(join_day)'.format(self.query_bucket))
        self.run_cbq_query('CREATE INDEX ix4 ON {0}(join_yr)'.format(self.query_bucket))
        self.run_cbq_query('CREATE INDEX ix5 ON {0}(join_day, join_mo, join_yr)'.format(self.query_bucket))

    def drop_indexes(self):
        self.run_cbq_query('DROP INDEX ix1 on {0}'.format(self.query_bucket))
        self.run_cbq_query('DROP INDEX ix2 on {0}'.format(self.query_bucket))
        self.run_cbq_query('DROP INDEX ix3 on {0}'.format(self.query_bucket))
        self.run_cbq_query('DROP INDEX ix4 on {0}'.format(self.query_bucket))
        self.run_cbq_query('DROP INDEX ix5 on {0}'.format(self.query_bucket))
