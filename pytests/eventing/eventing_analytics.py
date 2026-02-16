import json
import logging

from pytests.eventing.eventing_constants import HANDLER_CODE_ANALYTICS
from pytests.eventing.eventing_base import EventingBaseTest
from lib.membase.api.rest_client import RestConnection

log = logging.getLogger()


class EventingAnalytics(EventingBaseTest):

    '''
    dict mapping of base handler codes to their corresponding analytics query. 
    Used to run the same analytics query directly via the CBAS REST API (without eventing), 
    to test the result against the eventing handler.
    '''

    ANALYTICS_QUERY_MAP = {
        'analytics_basic_select': "SELECT name, country FROM `travel-sample`.`inventory`.`airline` LIMIT 5",
        'analytics_count_distinct': "SELECT COUNT(DISTINCT country) AS distinct_country_count FROM `travel-sample`.`inventory`.`airline`",
        'analytics_filtered': "SELECT name, country FROM `travel-sample`.`inventory`.`airline` WHERE country = 'United States' LIMIT 10",
        'analytics_groupby': "SELECT country, COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline` GROUP BY country ORDER BY cnt DESC LIMIT 5",
        'analytics_joins': "SELECT r.airline, a.name AS airline_name, COUNT(*) AS route_count "
                           "FROM `travel-sample`.`inventory`.`route` r "
                           "JOIN `travel-sample`.`inventory`.`airline` a ON r.airline = a.iata "
                           "GROUP BY r.airline, a.name ORDER BY route_count DESC LIMIT 5",
        'analytics_subqueries': "SELECT name, country FROM `travel-sample`.`inventory`.`airline` "
                                "WHERE country IN (SELECT VALUE r.country FROM `travel-sample`.`inventory`.`airline` r "
                                "GROUP BY r.country HAVING COUNT(*) > 10)",
    }

    def setUp(self):
        super(EventingAnalytics, self).setUp()
        self.handler_code_param = self.input.param('handler_code', 'analytics_basic')
        handler_code = self.handler_code_param
        if handler_code == 'analytics_basic_select':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
        elif handler_code == 'analytics_count_distinct':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_COUNT_DISTINCT
        elif handler_code == 'analytics_filtered':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_FILTERED
        elif handler_code == 'analytics_groupby':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_GROUPBY
        elif handler_code == 'analytics_joins':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_JOINS
        elif handler_code == 'analytics_subqueries':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_SUBQUERIES
        elif handler_code == 'analytics_count_distinct_with_n1ql':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_COUNT_DISTINCT_WITH_N1QL
        elif handler_code == 'analytics_count_distinct_with_base64':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_COUNT_DISTINCT_WITH_BASE64
        elif handler_code == 'analytics_count_distinct_with_timer':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_COUNT_DISTINCT_WITH_TIMER
        elif handler_code == 'analytics_filtered_with_base64':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_FILTERED_WITH_BASE64
        elif handler_code == 'analytics_filtered_with_timer':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_FILTERED_WITH_TIMER
        elif handler_code == 'analytics_groupby_with_crc':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_GROUPBY_WITH_CRC
        elif handler_code == 'analytics_groupby_with_bucket_op':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_GROUPBY_WITH_BUCKET_OP
        elif handler_code == 'analytics_joins_with_n1ql':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_JOINS_WITH_N1QL
        elif handler_code == 'analytics_joins_with_curl':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_JOINS_WITH_CURL
        elif handler_code == 'analytics_subqueries_with_xattrs':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_SUBQUERIES_WITH_XATTRS
        elif handler_code == 'analytics_subqueries_with_crc':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_SUBQUERIES_WITH_CRC

        # Common setup for all analytics tests
        self.load_sample_buckets(self.server, "travel-sample")
        self.load_data_to_collection(1, "default.scope0.collection0")
        self._setup_analytics()

    def tearDown(self):
        try:
            cbas_node = self.get_nodes_from_services_map(service_type="cbas")
            if cbas_node:
                cbas_rest = RestConnection(cbas_node)
                cbas_rest.execute_statement_on_cbas("DISCONNECT LINK Local", None)
        except Exception:
            log.exception("Analytics teardown cleanup failed")
        super(EventingAnalytics, self).tearDown()

    def _setup_analytics(self):
        """
        Create analytics dataverse + collection on travel-sample airline
        data, connect the local link, and wait for data ingestion.
        """
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        cbas_rest.execute_statement_on_cbas("CREATE DATAVERSE `travel-sample`.`inventory`", None)
        cbas_rest.execute_statement_on_cbas("CREATE ANALYTICS COLLECTION `travel-sample`.`inventory`.`airline` ON `travel-sample`.`inventory`.`airline`", None)
        cbas_rest.execute_statement_on_cbas("CONNECT LINK Local", None)
        self.sleep(15, "Waiting for analytics to ingest travel-sample data")
        # Verify analytics returns data before deploying any handler
        result = cbas_rest.execute_statement_on_cbas("SELECT COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline`", None)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        parsed = json.loads(result)
        count = parsed["results"][0]["cnt"]
        log.info("Analytics airline collection has %s docs" % count)
        self.assertTrue(count > 0, "Analytics airline collection has 0 docs — ingestion failed")

    def _get_base_handler_code(self, handler_code_param):
        """
        Map coexistence handler codes back to their base analytics query type.
        e.g. 'analytics_count_distinct_with_n1ql' -> 'analytics_count_distinct'
        """
        for base in self.ANALYTICS_QUERY_MAP:
            if handler_code_param == base or handler_code_param.startswith(base + "_with_"):
                return base
        return None

    def _verify_analytics_result_matches_direct_query(self):
        """
        Run the same analytics query directly via the CBAS REST API (without eventing),
        then read the doc written by the eventing handler from collection1 via N1QL,
        and compare the results.
        """
        base_handler = self._get_base_handler_code(self.handler_code_param)
        if not base_handler:
            log.warning("No direct analytics query mapped for handler_code=%s, skipping verification" % self.handler_code_param)
            return

        analytics_query = self.ANALYTICS_QUERY_MAP[base_handler]
        data_field = 'data'

        # Run the analytics query directly via CBAS REST
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        direct_result = cbas_rest.execute_statement_on_cbas(analytics_query, None)
        if isinstance(direct_result, bytes):
            direct_result = direct_result.decode("utf-8")
        direct_parsed = json.loads(direct_result)
        direct_rows = direct_parsed["results"]
        log.info("Direct analytics query returned %s rows" % len(direct_rows))

        # Read the eventing-written doc from collection1 via N1QL
        try:
            self.n1ql_helper.run_cbq_query(
                query="CREATE PRIMARY INDEX IF NOT EXISTS ON default.scope0.collection1",
                server=self.n1ql_server)
        except Exception as e:
            log.info("Index creation note: %s" % str(e))

        n1ql_result = self.n1ql_helper.run_cbq_query(
            query="SELECT * FROM default.scope0.collection1",
            server=self.n1ql_server)
        self.assertTrue(len(n1ql_result["results"]) > 0, "No docs found in collection1 written by eventing handler")

        eventing_doc = n1ql_result["results"][0]["collection1"]
        eventing_data = eventing_doc.get(data_field, [])
        log.info("Eventing handler wrote %s rows in field '%s'" % (len(eventing_data), data_field))

        # For count_distinct, the result is a single object not a list of rows
        if base_handler == 'analytics_count_distinct':
            self.assertEqual(
                eventing_doc.get("distinct_country_count"),
                direct_rows[0].get("distinct_country_count"),
                "COUNT(DISTINCT) mismatch: eventing=%s, direct=%s" % (
                    eventing_doc.get("distinct_country_count"),
                    direct_rows[0].get("distinct_country_count")))
            log.info("Analytics result verification PASSED for %s" % base_handler)
            return

        # Sort both result sets for deterministic comparison
        def sort_key(row):
            return json.dumps(row, sort_keys=True)

        direct_sorted = sorted(direct_rows, key=sort_key)
        eventing_sorted = sorted(eventing_data, key=sort_key)

        self.assertEqual(
            len(eventing_sorted), len(direct_sorted),
            "Row count mismatch: eventing=%d, direct=%d" % (len(eventing_sorted), len(direct_sorted)))

        for i, (ev_row, direct_row) in enumerate(zip(eventing_sorted, direct_sorted)):
            self.assertEqual(
                ev_row, direct_row,
                "Row %d mismatch:\n  eventing: %s\n  direct:   %s" % (i, ev_row, direct_row))

        log.info("Analytics result verification PASSED for %s (%d rows match)" % (base_handler, len(direct_sorted)))

    def test_analytics_sanity(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.sleep(10)
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self._verify_analytics_result_matches_direct_query()
        self.undeploy_and_delete_function(body)


    def test_analytics_coexistence(self):
        coexistence_type = self.input.param('coexistence_type', '')
        expected_doc_count = 1
        sleep_time = 10
        sleep_msg = None

        if 'xattrs' in coexistence_type:
            expected_doc_count = 2
        if 'timer' in coexistence_type:
            sleep_time = 60
            sleep_msg = "Waiting for timer callback to fire and write results"

        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.sleep(sleep_time, sleep_msg)
        self.verify_doc_count_collections("default.scope0.collection1", expected_doc_count)
        self._verify_analytics_result_matches_direct_query()
        self.undeploy_and_delete_function(body)