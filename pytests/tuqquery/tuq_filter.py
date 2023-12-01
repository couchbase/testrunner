from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff
from membase.api.exception import CBQError

class QueryFilterTests(QueryTests):

    def setUp(self):
        super(QueryFilterTests, self).setUp()
        self.log.info("==============  QueryFilterTests setup has started ==============")

        if self.load_sample:
            self.rest.load_sample("travel-sample")
            init_time = time.time()
            while True:
                next_time = time.time()
                query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + self.bucket_name + "`")
                self.log.info(f"{self.bucket_name}+ count: {query_response['results'][0]['$1']}")
                if query_response['results'][0]['$1'] == 31591:
                    break
                if next_time - init_time > 600:
                    break
                time.sleep(2)

            self.wait_for_all_indexes_online()
        
        self.log.info("==============  QueryFilterTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryFilterTests, self).suite_setUp()
        self.log.info("==============  QueryFilterTests suite_setup has started ==============")
        self.log.info("==============  QueryFilterTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryFilterTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryFilterTests tearDown has completed ==============")
        super(QueryFilterTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryFilterTests suite_tearDown has started ==============")
        self.log.info("==============  QueryFilterTests suite_tearDown has completed ==============")
        super(QueryFilterTests, self).suite_tearDown()

    def test_filter_count_star(self):
        filter_query = "SELECT COUNT(*) FILTER (WHERE city = 'Paris') as count_paris FROM `travel-sample` WHERE type = 'airport'"
        case_query = "SELECT COUNT(CASE WHEN city = 'Paris' THEN 1 ELSE NULL END) as count_paris FROM `travel-sample` WHERE type = 'airport'"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_agg_1(self):
        AGG_FUNCS = ['AVG', 'MAX', 'MIN', 'MEDIAN', 'MEAN']
        for agg_func in AGG_FUNCS:
            filter_query = f"SELECT geo.accuracy, \
                    {agg_func}(geo.lat) FILTER (WHERE country = 'United Kingdom') as UK_agg, \
                    {agg_func}(geo.lat) FILTER (WHERE country = 'France') as FR_agg \
                FROM `travel-sample` WHERE type ='landmark' GROUP BY geo.accuracy ORDER BY geo.accuracy"
            case_query = f"SELECT geo.accuracy, \
                    {agg_func}(CASE WHEN country = 'United Kingdom' THEN geo.lat ELSE NULL END) as UK_agg, \
                    {agg_func}(CASE WHEN country = 'France' THEN geo.lat ELSE NULL END) as FR_agg \
                FROM `travel-sample` WHERE type ='landmark' GROUP BY geo.accuracy ORDER BY geo.accuracy"
            filter_results = self.run_cbq_query(filter_query)
            case_results = self.run_cbq_query(case_query)
            self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_agg_2(self):
        AGG_FUNCS = ['SUM', 'COUNT', 'COUNTN']
        for agg_func in AGG_FUNCS:
            filter_query = f"SELECT {agg_func}(distance) FILTER (WHERE stops > 0) as agg_stops FROM `travel-sample` WHERE type = 'route'"
            case_query = f"SELECT {agg_func}(CASE WHEN stops > 0 THEN distance ELSE NULL END) as agg_stops FROM `travel-sample` WHERE type = 'route'"
            filter_results = self.run_cbq_query(filter_query)
            case_results = self.run_cbq_query(case_query)
            self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_stddev(self):
        STDDEV_FUNCS = ['STDDEV', 'STDDEV_POP', 'STDDEV_SAMP']
        for stddev_func in STDDEV_FUNCS:
            filter_query = f"SELECT country, \
                    {stddev_func}(reviews[0].ratings.Cleanliness) FILTER (WHERE free_parking) AS PARKING_stddev, \
                    {stddev_func}(reviews[0].ratings.Cleanliness) as TOTAL_stddev \
                FROM `travel-sample` WHERE type = 'hotel' GROUP BY country ORDER BY country"
            case_query = f"SELECT country, \
                    {stddev_func}(CASE WHEN free_parking THEN reviews[0].ratings.Cleanliness ELSE NULL END) AS PARKING_stddev, \
                    {stddev_func}(reviews[0].ratings.Cleanliness) as TOTAL_stddev \
                FROM `travel-sample` WHERE type = 'hotel' GROUP BY country ORDER BY country"
            filter_results = self.run_cbq_query(filter_query)
            case_results = self.run_cbq_query(case_query)
            self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_variance(self):
        VARIANCE_FUNCS = ['VARIANCE', 'VARIANCE_POP', 'VARIANCE_SAMP']
        for variance_func in VARIANCE_FUNCS:
            filter_query = f"SELECT country, \
                    {variance_func}(reviews[0].ratings.Cleanliness) FILTER (WHERE free_parking) AS PARKING_stddev, \
                    {variance_func}(reviews[0].ratings.Cleanliness) as TOTAL_stddev \
                FROM `travel-sample` WHERE type = 'hotel' GROUP BY country ORDER BY country"
            case_query = f"SELECT country, \
                    {variance_func}(CASE WHEN free_parking THEN reviews[0].ratings.Cleanliness ELSE NULL END) AS PARKING_stddev, \
                    {variance_func}(reviews[0].ratings.Cleanliness) as TOTAL_stddev \
                FROM `travel-sample` WHERE type = 'hotel' GROUP BY country ORDER BY country"
            filter_results = self.run_cbq_query(filter_query)
            case_results = self.run_cbq_query(case_query)
            self.assertEqual(filter_results['results'], case_results['results'])

    def test_neg_invalid(self):
        error_code = 3000
        error_message = "Invalid function maxx (near line 1, column 8)"
        filter_query = "select maxx(geo.lat) FILTER(WHERE country = 'France') from `travel-sample` where type = 'landmark'"
        try:
            self.run_cbq_query(filter_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_neg_nonagg(self):
        error_code = 3000
        error_message = "FILTER clause syntax is not valid for function array_max"
        filter_query = "select name, array_max(reviews[*].ratings[*].Location) FILTER (WHERE city = 'Paris') from `travel-sample` where type = 'hotel'"
        try:
            self.run_cbq_query(filter_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertTrue(error_message in error['msg'], f"We failed to find expected {error_message} in actual {error['msg']} ")
        
    def test_neg_subquery(self):
        error_code = 4000
        error_message = "Subqueries are not allowed in aggregate filter (near line 1, column 34)."
        filter_query = "SELECT max(geo.lat) FILTER(WHERE country = (select 'France' from system:dual)) FROM `travel-sample` WHERE type = 'landmark'"
        try:
            self.run_cbq_query(filter_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_neg_window(self):
        error_code = 4000
        error_message = "Window aggregates are not allowed inside Aggregates (near line 1, column 8)."
        filter_query = "SELECT max(geo.alt) FILTER(WHERE DENSE_RANK() OVER (PARTITION BY country ORDER BY geo.alt NULLS LAST) < 5) FROM `travel-sample` WHERE type = 'airport'"
        try:
            self.run_cbq_query(filter_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_filter_prepared(self):
        prepare_filter_query = "PREPARE filter_query as SELECT COUNT(*) FILTER (WHERE city = 'Paris') as count_paris FROM `travel-sample` WHERE type = 'airport'"
        case_query = "SELECT COUNT(CASE WHEN city = 'Paris' THEN 1 ELSE NULL END) as count_paris FROM `travel-sample` WHERE type = 'airport'"
        self.run_cbq_query(prepare_filter_query)
        filter_results = self.run_cbq_query(query="EXECUTE filter_query")
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_function(self):
        filter_query = "SELECT COUNT(*) FILTER (WHERE lower(city) = 'paris') as count_paris FROM `travel-sample` WHERE type = 'airport'"
        case_query = "SELECT COUNT(CASE WHEN city = 'Paris' THEN 1 ELSE NULL END) as count_paris FROM `travel-sample` WHERE type = 'airport'"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_parameter(self):
        filter_query = "SELECT COUNT(*) FILTER (WHERE city = $ville) as count_paris FROM `travel-sample` WHERE type = 'airport'"
        case_query = "SELECT COUNT(CASE WHEN city = 'Paris' THEN 1 ELSE NULL END) as count_paris FROM `travel-sample` WHERE type = 'airport'"
        filter_results = self.run_cbq_query(query=filter_query,query_params={'$ville': '"Paris"'})
        case_results = self.run_cbq_query(query=case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_count_one(self):
        filter_query = "SELECT COUNT(1) FILTER (WHERE type = 'airport') as count_airport FROM `travel-sample`"
        case_query = "SELECT COUNT(CASE WHEN type = 'airport' THEN 1 ELSE NULL END) as count_airport FROM `travel-sample`"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_multiple_conditions(self):
        filter_query = f"SELECT COUNT(distance) FILTER (WHERE (stops > 0 and distance > 1000) or (stops = 0 and distance < 1000)) as agg_stops FROM `travel-sample` WHERE type = 'route'"
        case_query = f"SELECT COUNT(CASE WHEN (stops > 0 and distance > 1000) or (stops = 0 and distance < 1000) THEN distance ELSE NULL END) as agg_stops FROM `travel-sample` WHERE type = 'route'"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_array_agg(self):
        filter_query = "SELECT \
            ARRAY_COUNT(ARRAY_AGG(reviews[0].ratings.Cleanliness) FILTER (WHERE country = 'France')) AS ReviewsFrance, \
            ARRAY_COUNT(ARRAY_AGG(reviews[0].ratings.Cleanliness) FILTER (WHERE country = 'United States')) AS ReviewsUS \
                FROM `travel-sample` WHERE type = 'hotel'"
        case_query = "SELECT \
            ARRAY_COUNT(ARRAY_AGG(CASE WHEN country = 'France' THEN reviews[0].ratings.Cleanliness ELSE NULL END)) AS ReviewsFrance, \
            ARRAY_COUNT(ARRAY_AGG(CASE WHEN country = 'United States' THEN reviews[0].ratings.Cleanliness ELSE NULL END)) AS ReviewsUS \
                FROM `travel-sample` WHERE type = 'hotel'"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_window(self):
        filter_query = "SELECT d.id, d.destinationairport, d.equipment, \
                MAX(distance) FILTER (WHERE equipment in ['788','744']) OVER (PARTITION BY d.destinationairport) \
                FROM `travel-sample` as d where type ='route' and distance > 13000 ORDER BY d.id"
        case_query = "SELECT d.id, d.destinationairport, d.equipment, \
                MAX(CASE WHEN equipment in ['788','744'] THEN distance ELSE NULL END) OVER (PARTITION BY d.destinationairport) \
                FROM `travel-sample` as d where type ='route' and distance > 13000 ORDER BY d.id"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def test_filter_correlated(self):
        filter_query = "WITH city_with_2_or_more_airport \
                AS (SELECT value city FROM `travel-sample` \
                    WHERE type = 'airport' AND country = 'France' \
                    GROUP BY city HAVING count(*) > 2) \
            SELECT count(*) filter (WHERE city in city_with_2_or_more_airport) FROM `travel-sample` WHERE type = 'hotel' AND country = 'France'"
        filter_results = self.run_cbq_query(filter_query)
        self.assertEqual(filter_results['results'][0]['$1'], 64)
        

