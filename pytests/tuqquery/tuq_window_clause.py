from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff
from membase.api.exception import CBQError

class QueryWindowClauseTests(QueryTests):

    def setUp(self):
        super(QueryWindowClauseTests, self).setUp()
        self.log.info("==============  QueryWindowClauseTests setup has started ==============")
        self.function_name = self.input.param("function_name", "CUME_DIST")

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
        
        self.log.info("==============  QueryWindowClauseTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryWindowClauseTests, self).suite_setUp()
        self.log.info("==============  QueryWindowClauseTests suite_setup has started ==============")
        self.log.info("==============  QueryWindowClauseTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryWindowClauseTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryWindowClauseTests tearDown has completed ==============")
        super(QueryWindowClauseTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryWindowClauseTests suite_tearDown has started ==============")
        self.log.info("==============  QueryWindowClauseTests suite_tearDown has completed ==============")
        super(QueryWindowClauseTests, self).suite_tearDown()

    def test_window_single(self):
        window_clause_query = "SELECT d.id, d.destinationairport, \
            AVG(d.distance) OVER ( window1 ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport) \
            ORDER BY 1 \
            LIMIT 5"
        window_query = "SELECT d.id, d.destinationairport, \
            AVG(d.distance) OVER ( PARTITION BY d.destinationairport ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            ORDER BY 1 \
            LIMIT 5"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_unused(self):
        window_query = "SELECT d.id, d.destinationairport \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport ORDER by d.distance) \
            LIMIT 5"
        self.run_cbq_query(window_query)

    def test_window_multiple(self):
        window_clause_query = "SELECT d.id, d.destinationairport, \
            ROW_NUMBER() OVER ( w1 ) AS `row`, \
            AVG(d.distance) OVER w1 AS `avg`, \
            AVG(d.distance) OVER ( PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST ) AS `average`, \
            LEAD(r.distance, 1, 'No next distance') OVER ( w3 ) AS `next-distance` \
            FROM `travel-sample` AS d \
            WHERE d.type='route' \
            WINDOW w1 AS (PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST), \
                w2 AS (PARTITION BY d.sourceairport ORDER BY d.distance), \
                w3 AS (PARTITION BY r.airline ORDER BY r.distance NULLS LAST) \
            ORDER BY 1 \
            LIMIT 7"
        window_query = "SELECT d.id, d.destinationairport, \
            ROW_NUMBER() OVER ( PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST ) AS `row`, \
            AVG(d.distance) OVER ( PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST ) AS `avg`, \
            AVG(d.distance) OVER ( PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST ) AS `average`, \
            LEAD(r.distance, 1, 'No next distance') OVER ( PARTITION BY r.airline ORDER BY r.distance NULLS LAST ) AS `next-distance` \
            FROM `travel-sample` AS d \
            WHERE d.type='route' \
            ORDER BY 1 \
            LIMIT 7"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_order(self):
        window_clause_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( window1 ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport ORDER by d.distance) \
            ORDER BY 1 \
            LIMIT 5"
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( PARTITION BY d.destinationairport ORDER by d.distance ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            ORDER BY 1 \
            LIMIT 5"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_name_order(self):
        window_clause_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( window1 ORDER by d.distance ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport) \
            ORDER BY 1 \
            LIMIT 5"
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( PARTITION BY d.destinationairport ORDER by d.distance ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            ORDER BY 1 \
            LIMIT 5"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])
    
    def test_window_name_frame(self):
        window_clause_query = "SELECT r.sourceairport, r.destinationairport, r.distance, \
                    NTH_VALUE(r.distance, 2) FROM FIRST OVER ( \
                        window1 \
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) AS `shortest_distance_but_1` \
                FROM `travel-sample` AS r \
                WHERE r.type='route' \
                WINDOW window1 AS ( PARTITION BY r.sourceairport ORDER BY r.distance) \
                ORDER BY 1 \
                LIMIT 7"
        window_query = "SELECT r.sourceairport, r.destinationairport, r.distance, \
                    NTH_VALUE(r.distance, 2) FROM FIRST OVER ( \
                        PARTITION BY r.sourceairport \
                        ORDER BY r.distance \
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) AS `shortest_distance_but_1` \
                FROM `travel-sample` AS r \
                WHERE r.type='route' \
                ORDER BY 1 \
                LIMIT 7"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_name_order_frame(self):
        window_clause_query = "SELECT r.sourceairport, r.destinationairport, r.distance, \
                    NTH_VALUE(r.distance, 2) FROM FIRST OVER ( \
                        window1 \
                        ORDER BY r.distance \
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) AS `shortest_distance_but_1` \
                FROM `travel-sample` AS r \
                WHERE r.type='route' \
                WINDOW window1 AS ( PARTITION BY r.sourceairport ) \
                ORDER BY 1 \
                LIMIT 7"
        window_query = "SELECT r.sourceairport, r.destinationairport, r.distance, \
                    NTH_VALUE(r.distance, 2) FROM FIRST OVER ( \
                        PARTITION BY r.sourceairport \
                        ORDER BY r.distance \
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) AS `shortest_distance_but_1` \
                FROM `travel-sample` AS r \
                WHERE r.type='route' \
                ORDER BY 1 \
                LIMIT 7"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_no_parenthesis(self):
        window_clause_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER window_name_with_no_parenthesis AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window_name_with_no_parenthesis AS (PARTITION BY d.destinationairport ORDER by d.distance) \
            ORDER BY 1 \
            LIMIT 5"
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( PARTITION BY d.destinationairport ORDER by d.distance ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            ORDER BY 1 \
            LIMIT 5"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_name_function(self):
        window_function = {
            'CUME_DIST': {
                'arg':'', 'select_list': 'd.id, d.destinationairport', 
                'wdef': 'PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS d WHERE d.type=\'route\''
            },
            'DENSE_RANK': {
                'arg':'', 'select_list': 'a.airportname, a.geo.alt', 
                'wdef': 'PARTITION BY a.country ORDER BY a.geo.alt NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS a WHERE a.type=\'airport\''
            },
            'FIRST_VALUE': {
                'arg':'r.distance', 'select_list': 'r.sourceairport, r.destinationairport, r.distance', 
                'wdef': 'PARTITION BY r.sourceairport ORDER BY r.distance', 'frame_clause': '',
                'from_where': '`travel-sample` AS r WHERE r.type=\'route\''
            },
            'LAG': {
                'arg':'r.distance, 1, "No previous distance"', 'select_list': 'r.airline, r.id, r.distance', 
                'wdef': 'PARTITION BY r.airline ORDER BY r.distance NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS r WHERE r.type=\'route\''
            },
            'LAST_VALUE': {
                'arg':'r.distance', 'select_list': 'r.sourceairport, r.destinationairport, r.distance', 
                'wdef': 'PARTITION BY r.sourceairport ORDER BY r.distance', 'frame_clause': 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING',
                'from_where': '`travel-sample` AS r WHERE r.type=\'route\''
            },
            'LEAD': {
                'arg': 'r.distance, 1, "No next distance"', 'select_list': 'r.airline, r.id, r.distance',
                'wdef': 'PARTITION BY r.airline ORDER BY r.distance NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS r WHERE r.type=\'route\''
            },
            'NTH_VALUE': {
                'arg': 'r.distance, 2', 'select_list': 'r.sourceairport, r.destinationairport, r.distance',
                'wdef': 'PARTITION BY r.sourceairport ORDER BY r.distance', 'frame_clause': 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING',
                'from_where': '`travel-sample` AS r WHERE r.type=\'route\''
            },
            'NTILE': {
                'arg': 3, 'select_list': 'r.airline, r.distance',
                'wdef': 'PARTITION BY r.airline ORDER BY r.distance', 'frame_clause': '',
                'from_where': '`travel-sample` AS r WHERE r.type=\'route\''
            },
            'PERCENT_RANK': {
                'arg':'', 'select_list': 'd.id, d.destinationairport', 
                'wdef': 'PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS d WHERE d.type=\'route\''
            },
            'RANK': {
                'arg':'', 'select_list': 'a.airportname, a.geo.alt', 
                'wdef': 'PARTITION BY a.country ORDER BY a.geo.alt NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS a WHERE a.type=\'airport\''
            },
            'RATIO_TO_REPORT': {
                'arg':'d.distance', 'select_list': 'd.id, d.destinationairport', 
                'wdef': 'PARTITION BY d.destinationairport', 'frame_clause': '',
                'from_where': '`travel-sample` AS d WHERE d.type=\'route\''
            },
            'ROW_NUMBER': {
                'arg':'', 'select_list': 'd.id, d.destinationairport', 
                'wdef': 'PARTITION BY d.destinationairport ORDER BY d.distance NULLS LAST', 'frame_clause': '',
                'from_where': '`travel-sample` AS d WHERE d.type=\'route\''
            }
        }
        function = self.function_name
        select_list = window_function[function]['select_list']
        arg = window_function[function]['arg']
        from_where = window_function[function]['from_where']
        window_definition = window_function[function]['wdef']
        frame_clause = window_function[function]['frame_clause']
        # run query with window function
        window_clause_query = f"SELECT {select_list}, {function}({arg}) OVER (window_name {frame_clause}) AS {function}_col FROM {from_where} WINDOW window_name AS ( {window_definition} ) LIMIT 7"
        window_query = f"SELECT {select_list}, {function}({arg}) OVER ( {window_definition} {frame_clause}) AS {function}_col FROM {from_where} LIMIT 7"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def test_window_prepared(self):
        prepare_window_query = "PREPARE window_query as SELECT d.id, d.destinationairport, CUME_DIST() OVER ( `Window-1` ) AS `rank` FROM `travel-sample` AS d WHERE d.type='route' WINDOW `Window-1` AS (PARTITION by d.destinationairport ORDER BY d.distance) limit 5"
        self.run_cbq_query(prepare_window_query)
        self.run_cbq_query(query="EXECUTE window_query")

    def test_neg_non_unique(self):
        error_code = 6500
        error_message = "Duplicate window clause alias window1."
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( window1 ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport ORDER by d.distance), \
                window1 AS (PARTITION BY d.destinationairport)"
        try:
            self.run_cbq_query(window_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)
    
    def test_neg_non_exists(self):
        error_code = 6500
        error_message = "Window window2 not in the scope of window clause"
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( window2 ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport ORDER by d.distance)"
        try:
            self.run_cbq_query(window_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)
    
    def test_neg_order(self):
        error_code = 6500
        error_message = "Window window1 shall not have a window ordering clause"
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( window1 ORDER BY d.distance NULLS LAST) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport ORDER by d.distance)"
        try:
            self.run_cbq_query(window_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_neg_frame(self):
        error_code = 6500
        error_message = "Window window1 shall not have a window framing clause"
        window_query = "SELECT r.sourceairport, r.destinationairport, r.distance, \
            LAST_VALUE(r.distance) OVER (window1) AS `longest_distance` \
            FROM `travel-sample` AS r \
            WHERE r.type='route' \
            WINDOW window1 AS (PARTITION BY r.sourceairport ORDER BY r.distance ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        try:
            self.run_cbq_query(window_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_neg_partition(self):
        error_code = 6500
        error_message = "Window window1 shall not have a window partitioning clause"
        window_query = "SELECT d.id, d.destinationairport, \
            CUME_DIST() OVER ( window1 PARTITION BY d.destinationairport) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport ORDER by d.distance)"
        try:
            self.run_cbq_query(window_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)
