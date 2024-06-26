from .tuq import QueryTests
from deepdiff import DeepDiff
import time

class QueryEarlyFilterTests(QueryTests):
    def setUp(self):
        super(QueryEarlyFilterTests, self).setUp()
        self.log.info("==============  QueryEarlyFilterTests setup has started ==============")
        self.log_config_info()
        self.query_buckets = self.get_query_buckets(sample_buckets=['travel-sample'], check_all_buckets=True)
        self.query_bucket = self.query_buckets[0]
        self.sample_bucket = self.query_buckets[1]
        self.sample_bucket = self.sample_bucket.replace('`travel-sample`', '\`travel-sample\`')
        self.partial_index = self.input.param("partial_index", False)
        self.log.info("==============  QueryEarlyFilterTests setup has completed ==============")


    def suite_setUp(self):
        super(QueryEarlyFilterTests, self).suite_setUp()
        self.log.info("==============  QueryEarlyFilterTests suite_setup has started ==============")
        self.rest.load_sample("travel-sample")
        self.log.info("==============  QueryCurlTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryEarlyFilterTests tearDown has started ==============")
        self.log.info("==============  QueryEarlyFilterTests tearDown has completed ==============")
        super(QueryEarlyFilterTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryEarlyFilterTests suite_tearDown has started ==============")
        self.log.info("==============  QueryEarlyFilterTests suite_tearDown has completed ==============")
        super(QueryEarlyFilterTests, self).suite_tearDown()

    def test_early_filter(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)
        try:
            select_query = "select name,join_yr from default where job_title like '%E%'"
            primary_query = "select name,join_yr from default use index(`#primary`) where job_title like '%E%'"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_multiple_fields(self):
        #If partial index is present, we expect a new field(index_condition) to appear in explain and for one field to not appear in index_keys
        if self.partial_index:
            index = "Create index idx1 on default(job_title,join_day,join_mo,name) where lower(name) = 'employee-9'"
        else:
            index = "Create index idx1 on default(job_title,join_day,join_mo,name)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default where job_title like '%E%' and lower(name) = 'employee-9'"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like '%E%' and lower(name) = 'employee-9'"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")

            if self.partial_index:
                self.assertTrue("index_conditions" in str(explain_plan),f"We expect early filter to take place here but it does not, please check plan {explain_plan}" )
                self.assertTrue("'_index_key ((`default`.`name`))'" not in str(explain_plan), f"The index has a condition that covers this field, it should not be present! please check explain plan {explain_plan}")
                self.assertTrue("'_index_condition (lower((`default`.`name`)))': 'employee-9'" in str(explain_plan), f"The partial index has a condition that covers the function! please check explain plan {explain_plan}")
            else:
                self.assertTrue("'_index_key ((`default`.`name`))'" in str(explain_plan), f"We expect this key to be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_multiple_predicates(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo,name)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default where job_title like '%E%' and name = 'employee-9'"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like '%E%' and name = 'employee-9'"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`name`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_leading_wildcard(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo,name)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default where job_title like '%r' and name = 'employee-9'"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like '%r' and name = 'employee-9'"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`name`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            select_query = "select name,join_yr from default where job_title like '%r' and upper(name) = 'EMPLOYEE-9'"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like '%r' and upper(name) = 'EMPLOYEE-9'"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`name`))'" in str(explain_plan),
                            f"This key should be apart of the early filter! please check explain plan {explain_plan}")
            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_like_between(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default where job_title like '%E%' and join_mo between 6 and 12"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like '%E%' and join_mo between 6 and 12"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_mo`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_like_nonequal(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default where job_title like '%E%' and join_mo >= 5 and join_yr < 2020"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like '%E%' and join_mo >= 5 and join_yr < 2020"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_mo`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_yr`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_subquery(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo,name)"
        self.run_cbq_query(index)

        try:
            select_query = "select d.name,d.join_yr from (select name,join_yr from default where job_title like '%E%' and lower(name) = 'employee-9') d"
            primary_query = "select d.name,d.join_yr from (select name,join_yr from default use index (`#primary`) where job_title like '%E%' and lower(name) = 'employee-9') d"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`name`))'" in str(explain_plan), f"We expect this key to be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_named_params(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)
        query_params = {'$job_title': "\"%E%\"", "$join_mo": 5, "$join_yr": 2020}
        try:
            select_query = "select name,join_yr from default where job_title like $job_title and join_mo >= $join_mo and join_yr < $join_yr"
            primary_query = "select name,join_yr from default use index (`#primary`) where job_title like $job_title and join_mo >= $join_mo and join_yr < $join_yr"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_params=query_params)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_mo`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_yr`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_params=query_params)
            expected_results = self.run_cbq_query(primary_query, query_params=query_params)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_cte(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)

        try:
            select_query = "WITH titles as(select raw job_title from default where job_title like '%E%') select name,join_yr from default where job_title in titles and join_mo > 5 and join_yr < 2020"
            primary_query = "WITH titles as(select raw job_title from default where job_title like '%E%') select name,join_yr from default use index (`#primary`) where job_title in titles and join_mo > 5 and join_yr < 2020"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_mo`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`join_yr`))'" not in str(explain_plan),
                            f"This key has an exact span and should not be apart of the early filter! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_let(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default let eng_title = 'engineer' where lower(job_title) = eng_title"
            primary_query = "select name,join_yr from default use index(`#primary`) let eng_title = 'engineer' where lower(job_title) = eng_title"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_filter_query_context(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE lower(city) = 'paris'"
            primary_query = "SELECT city, name, address FROM landmark use index (`def_inventory_airline_primary`) WHERE lower(city) = 'paris'"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("def_inventory_landmark_city" in str(explain_plan) or "ix_landmark_city_name" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")
        pass

    def test_early_order(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE city IS NOT MISSING ORDER BY name, city DESC OFFSET 100 LIMIT 5"
            primary_query = "SELECT city, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE city IS NOT MISSING ORDER BY name, city DESC OFFSET 100 LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # we need to make sure the order is being applied in the correct sequence idx -> order -> offset -> fetch
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][2]['#operator'] == 'Fetch', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_early_order_no_offset(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE city IS NOT MISSING ORDER BY name, city DESC LIMIT 5"
            primary_query = "SELECT city, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE city IS NOT MISSING ORDER BY name, city DESC LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # we need to make sure the order is being applied in the correct sequence idx -> order -> fetch
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][2]['#operator'] == 'Fetch', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_early_order_asc(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city ASC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE city IS NOT MISSING ORDER BY name, city ASC OFFSET 100 LIMIT 5"
            primary_query = "SELECT city, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE city IS NOT MISSING ORDER BY name, city ASC OFFSET 100 LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # we need to make sure the order is being applied in the correct sequence idx -> order -> offset -> fetch
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][2]['#operator'] == 'Fetch', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_early_order_alias(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city as aliased, name, address FROM landmark WHERE city IS NOT MISSING ORDER BY name, aliased DESC OFFSET 100 LIMIT 5"
            primary_query = "SELECT city as aliased, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE city IS NOT MISSING ORDER BY name, aliased DESC OFFSET 100 LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # we need to make sure the order is being applied in the correct sequence idx -> order -> offset -> fetch
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order',
                            f"Order is not being applied first, it should be. please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][2]['#operator'] == 'Fetch',
                            f"Order is not being applied first, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_early_order_function(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo,name)"
        self.run_cbq_query(index)

        try:
            select_query = "select name,join_yr from default where job_title like '%E%' and lower(name) = 'employee-9' ORDER BY join_mo,job_title LIMIT 5"
            primary_query = "select name,join_yr from default where job_title like '%E%' and lower(name) = 'employee-9' ORDER BY join_mo,job_title LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`name`))'" in str(explain_plan),
                                f"We expect this key to be apart of the early filter! please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_order_like_between_nonequal(self):
        index = "Create index idx1 on default(job_title,join_day,join_mo)"
        self.run_cbq_query(index)
        try:
            select_query = "select name,join_yr from default where job_title like '%E%' and join_mo >= 5 and join_day between 28 and 30 ORDER BY join_mo,job_title LIMIT 5"
            primary_query = "select name,join_yr from default use index(`#primary`) where job_title like '%E%' and join_mo >= 5 and join_day between 28 and 30 ORDER BY join_mo,job_title LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`job_title`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.idx1")

    def test_early_order_index_order(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE city IS NOT MISSING ORDER BY city DESC, name OFFSET 100 LIMIT 5"
            primary_query = "SELECT city, name, address FROM landmark use index(`def_inventory_airline_primary `) WHERE city IS NOT MISSING ORDER BY city DESC,name OFFSET 100 LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("Order" not in str(explain_plan),
                            f"Query does not need to use order operator, please check plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_early_order_no_limit(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE city like 'S%' ORDER BY name, city DESC OFFSET 100"
            primary_query = "SELECT city, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE city like 'S%' ORDER BY name, city DESC OFFSET 100"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("def_inventory_landmark_city" in str(explain_plan) or "ix_landmark_city_name" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" not in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" not in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" not in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Order', f"Order is not being applied after fetch, it should be. please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][2]['#operator'] == 'Offset', f"Order is not being applied after fetch, it should be. please check explain plan {explain_plan}")



            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_early_order_function_in_index(self):
        index = "CREATE INDEX ix_landmark_city_name_2 ON landmark(lower(city) DESC, name)"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT city, name, address FROM landmark WHERE lower(city) = 'paris' ORDER BY name, city DESC OFFSET 100 LIMIT 5"
            primary_query = "SELECT city, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE lower(city) = 'paris' ORDER BY name, city DESC OFFSET 100 LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name_2" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # we need to make sure the order is being applied in the correct sequence idx -> order -> offset -> fetch
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Order', f"Order is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name_2")

    def test_early_order_negative_join(self):
        select_query = "SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name,MIN(aport.tz) AS Airport__Time, MIN(lmark.name) AS Landmark_Name FROM `travel-sample`.inventory.airport aport LEFT JOIN `travel-sample`.inventory.landmark lmark ON aport.city = lmark.city AND lmark.country = 'United States' GROUP BY aport.airportname ORDER BY aport.airportname LIMIT 4"
        explain_query = "EXPLAIN " + select_query
        explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
        # this will not be able to put order at the beginning of the sequence
        self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Order', f"Order is not being applied after fetch, it should be. please check explain plan {explain_plan}")
        self.assertTrue(explain_plan['results'][0]['plan']['~children'][2]['#operator'] == 'Limit', f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")


    def test_early_order_negative_distinct(self):
        index = "CREATE INDEX ix_landmark_city_name ON landmark(city DESC, name);"
        self.run_cbq_query(index, query_context='default:`travel-sample`.inventory')

        try:
            select_query = "SELECT DISTINCT city, name, address FROM landmark WHERE city like 'S%' ORDER BY name, city DESC OFFSET 100 LIMIT 5"
            primary_query = "SELECT DISTINCT city, name, address FROM landmark use index(`def_inventory_airline_primary`) WHERE city like 'S%' ORDER BY name, city DESC OFFSET 100 LIMIT 5"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query, query_context='default:`travel-sample`.inventory')
            self.assertTrue("ix_landmark_city_name" in str(explain_plan) or "def_inventory_landmark_city" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" not in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" not in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" not in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Order', f"Order is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query, query_context='default:`travel-sample`.inventory')
            expected_results = self.run_cbq_query(primary_query, query_context='default:`travel-sample`.inventory')
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.ix_landmark_city_name")

    def test_non_covering_exact_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select f1,f3 from default where f1 < 10 and f2 = 2 order by f1 desc limit 2"
            primary_query = "select f1,f3 from default use index (`#primary`) where f1 < 10 and f2 = 2 order by f1 desc limit 2"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f3`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            select_query = "select f1,f3 from default where f1 < 10 and f2 = 2 order by f1 asc limit 3"
            primary_query = "select f1,f3 from default use index (`#primary`) where f1 < 10 and f2 = 2 order by f1 asc limit 3"
            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_non_covering_greater_than_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select f1,f3 from default where f1 < 10 and f2 > 2 order by f1 desc limit 1"
            primary_query = "select f1,f3 from default use index (`#primary`) where f1 < 10 and f2 > 2 order by f1 desc limit 1"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f3`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_non_covering_exact_early_filter_offset(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select f1,f3 from default where f1 < 10 and f2 = 2 order by f1 desc limit 1 offset 1"
            primary_query = "select f1,f3 from default use index (`#primary`) where f1 < 10 and f2 = 2 order by f1 desc limit 1 offset 1"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f3`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][2], f"Offset is not being applied after fetch, it should be. please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_covering_exact_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2,f3)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select * from default where f1 < 10 and f2 = 2 and f3 = 3 order by f1 desc limit 1"
            primary_query = "select * from default use index (`#primary`) where f1 < 10 and f2 = 2 and f3 = 3 order by f1 desc limit 1"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f3`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_non_covering_like_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2,`string`)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select f1,f3 from default where f1 < 10 and f2 = 2 and lower(`string`) like '%a%' order by f1 desc limit 1"
            primary_query = "select f1,f3 from default use index(`#primary`) where f1 < 10 and f2 = 2 and lower(`string`) like '%a%' order by f1 desc limit 1"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`string`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_covering_like_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2,`string`)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select * from default where f1 < 10 and f2 = 2 and lower(`string`) like '%a%' order by f1 desc limit 1"
            primary_query = "select * from default use index(`#primary`) where f1 < 10 and f2 = 2 and lower(`string`) like '%a%' order by f1 desc limit 1"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`string`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_non_covering_in_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3}),values("k4",{"f1":1,"f2":2,"f3":3,"string":"abcde"}),values("k5",{"f1":1,"f2":2,"f3":3,"string":"cdesf"}),values("k6",{"f1":0,"f2":2,"f3":3,"string":"abcd"}),values("k7",{"f1":1,"f2":2,"f3":3,"string":"defsr"})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2,`string`)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select f1,f3 from default where f1 < 10 and f2 = 2 and `string` in ['abcd','abcde','cdesf'] order by f1 desc limit 3"
            primary_query = "select f1,f3 from default use index(`#primary`) where f1 < 10 and f2 = 2 and `string` in ['abcd','abcde','cdesf'] order by f1 desc limit 3"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`string`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            select_query = "select * from default where f1 < 10 and f2 = 2 and lower(`string`) in ['abcd','abcde','cdesf'] order by f1,meta().id desc limit 2 offset 1"
            primary_query = "select * from default use index(`#primary`) where f1 < 10 and f2 = 2 and lower(`string`) in ['abcd','abcde','cdesf'] order by f1,meta().id desc limit 2 offset 1"

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")

    def test_covering_in_early_filter(self):
        upsert = 'upsert into default values("k0",{"f1":1,"f2":2,"f3":3}),values("k1",{"f1":2,"f2":2,"f3":0}),values("k2",{"f1":1,"f2":2,"f3":3,"string":"abcd"}),values("k3",{"f1":0,"f2":2,"f3":3}),values("k4",{"f1":1,"f2":2,"f3":3,"string":"abcde"}),values("k5",{"f1":1,"f2":2,"f3":3,"string":"cdesf"}),values("k6",{"f1":0,"f2":2,"f3":3,"string":"abcd"}),values("k7",{"f1":1,"f2":2,"f3":3,"string":"defsr"})'
        self.run_cbq_query(upsert)
        index = "create index ixto on default(f1,f2,`string`)"
        self.run_cbq_query(index)
        self.wait_for_all_indexes_online()

        try:
            select_query = "select * from default where f1 < 10 and f2 = 2 and `string` in ['abcd','abcde','cdesf'] order by f1 desc limit 3"
            primary_query = "select * from default use index(`#primary`) where f1 < 10 and f2 = 2 and `string` in ['abcd','abcde','cdesf'] order by f1 desc limit 3"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("ixto" in str(explain_plan) in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan),
                            f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f1`))'" in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`f2`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`default`.`string`))'" not in str(explain_plan),
                            f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # this will not be able to put order at the beginning of the sequence
            self.assertTrue("'#operator': 'Order'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Order should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue("'#operator': 'Filter'" in str(explain_plan['results'][0]['plan']['~children'][0]),
                            f"Filter should be applied before limit, please check explain plan {explain_plan}")
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][1]['#operator'] == 'Limit',
                            f"Limit is not being applied after fetch, it should be. please check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            select_query = "select * from default where f1 < 10 and f2 = 2 and lower(`string`) in ['abcd','abcde','cdesf'] order by f1 desc,meta().id limit 2 offset 1"
            primary_query = "select * from default use index(`#primary`) where f1 < 10 and f2 = 2 and lower(`string`) in ['abcd','abcde','cdesf'] order by f1 desc,meta().id limit 2 offset 1"

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default.ixto")