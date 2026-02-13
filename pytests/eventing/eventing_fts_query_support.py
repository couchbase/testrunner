import json
import logging

from pytests.eventing.eventing_constants import HANDLER_CODE_FTS_QUERY_SUPPORT
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.fts.fts_callable import FTSCallable
from pytests.eventing.fts_query_definitions import ALL_QUERIES
from lib.membase.api.rest_client import RestConnection

log = logging.getLogger()

class EventingFTSQuerySupport(EventingBaseTest):
    def setUp(self):
        super(EventingFTSQuerySupport, self).setUp()
        self.fts_index_name = self.input.param("fts_index_name", "travel_sample_test")
        self.fts_doc_count = self.input.param("fts_doc_count", 31500)
        self.fts_namespace = self.input.param("fts_namespace", "travel-sample._default._default")
        self.fts_namespace_match_all = self.input.param("fts_namespace", "travel-sample.inventory.airline")
        self.fts_index_partitions = self.input.param("fts_index_partitions", 1)
        self.fts_index_replicas = self.input.param("fts_index_replicas", 0)
        self.fts_index = None
        self.fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
        self.fts_query = None
        self.fts_memory_quota = self.input.param("fts_memory_quota", 3000)
        log.info("quota for fts service will be %s MB" % self.fts_memory_quota)
        rest = RestConnection(self.master)
        rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=self.fts_memory_quota)
        handler_code = self.input.param('handler_code', 'match_query')
        if handler_code == 'match_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'match_phrase_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_PHRASE_QUERY
            self.fts_query = ALL_QUERIES['match_phrase_query']
        elif handler_code == 'regex_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_REGEX_QUERY
            self.fts_query = ALL_QUERIES['regex_query']
        elif handler_code == 'query_string_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_QUERY_STRING_QUERY
            self.fts_query = ALL_QUERIES['query_string_query']
        elif handler_code == 'numeric_range_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_NUMERIC_RANGE_QUERY
            self.fts_query = ALL_QUERIES['numeric_range_query']
        elif handler_code == 'conjuncts_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_CONJUNCTS_QUERY
            self.fts_query = ALL_QUERIES['conjuncts_query']
        elif handler_code == 'disjuncts_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_DISJUNCTS_QUERY
            self.fts_query = ALL_QUERIES['disjuncts_query']
        elif handler_code == 'boolean_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_BOOLEAN_QUERY
            self.fts_query = ALL_QUERIES['boolean_query']
        elif handler_code == 'wildcard_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_WILDCARD_QUERY
            self.fts_query = ALL_QUERIES['wildcard_query']
        elif handler_code == 'docids_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_DOC_IDS_QUERY
            self.fts_query = ALL_QUERIES['docids_query']
        elif handler_code == 'term_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_TERM_QUERY
            self.fts_query = ALL_QUERIES['term_query']
        elif handler_code == 'phrase_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_PHRASE_QUERY
            self.fts_query = ALL_QUERIES['phrase_query']
        elif handler_code == 'prefix_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_PREFIX_QUERY
            self.fts_query = ALL_QUERIES['prefix_query']
        elif handler_code == 'match_all_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_ALL_QUERY
            self.fts_query = ALL_QUERIES['match_all_query']
        elif handler_code == 'match_none_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_NONE_QUERY
            self.fts_query = ALL_QUERIES['match_none_query']
        elif handler_code == 'match_query_counter':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_COUNTER
            self.fts_query = ALL_QUERIES['match_query_simple']
        elif handler_code == 'match_query_crc64':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_CRC64
            self.fts_query = ALL_QUERIES['match_query_with_analyzer']
        elif handler_code == 'match_query_base64':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_BASE64
            self.fts_query = ALL_QUERIES['match_query_simple']
        elif handler_code == 'match_query_xattrs':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_XATTRS
            self.fts_query = ALL_QUERIES['match_query_simple']
        elif handler_code == 'match_query_n1ql':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_N1QL
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'match_query_with_bucket_cache_getop':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_BUCKET_CACHE
            self.fts_query = ALL_QUERIES['match_query_simple']
        elif handler_code == 'match_query_subdoc_op':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_SUBDOC_OP
            self.fts_query = ALL_QUERIES['match_query_hostel']
        elif handler_code == 'match_query_with_timer':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_TIMER
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'match_query_with_curl':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_CURL
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'match_query_constant_binding':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_CONSTANT_BINDING
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'match_query_analytics_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY_ANALYTICS
            self.fts_query = ALL_QUERIES['match_query_simple']

    def tearDown(self):
        if getattr(self, "fts_index", None):
            try:
                self.fts_callable.delete_fts_index(self.fts_index_name)
            except Exception:
                log.exception("Cleaning FTS index %s failed", self.fts_index_name)
            finally:
                self.fts_index = None
        super(EventingFTSQuerySupport, self).tearDown()

    def _namespace_parts(self):
        if self.handler_code == "match_all_query":
            fts_namespace = self.fts_namespace_match_all
        else:
            fts_namespace = self.fts_namespace
        parts = fts_namespace.split(".")
        bucket = parts[0]
        scope = parts[1] if len(parts) > 1 else "_default"
        collection = parts[2] if len(parts) > 2 else "_default"
        return bucket, scope, collection

    def create_fts_index(self, bucket_name):
        plan_params = {
            "indexPartitions": self.fts_index_partitions,
            "numReplicas": self.fts_index_replicas
        }
        return self.fts_callable.create_default_index(
            index_name=self.fts_index_name,
            bucket_name=bucket_name,
            plan_params=plan_params
        )

    def construct_fts_query(self, index_name, query):
        fts_query = {
            "indexName": index_name,
            "query": query,
            "size": 10,
            "from": 0,
            "explain": False,
            "fields": [],
            "ctl": {
                "consistency": {
                    "level": "",
                    "vectors": {}
                }
            }
        }
        return fts_query

    def test_eventing_fts_query_support_sanity(self):
        """
        Load Docs
        Create FTS Index
        Wait for Indexing
        Run FTS Query
        Deploy Eventing Handler
        Write Back Result to Destination Keyspace
        Compare Results
        Verify Doc Count
        """
        bucket, scope, _ = self._namespace_parts()
        self.load_sample_buckets(self.server,"travel-sample")
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.fts_index = self.create_fts_index(bucket)
        self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=self.fts_index)
        self.sleep(30, "Waiting for indexing to complete")
        self.run_fts_to_test_against_queries()
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.sleep(10)
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.undeploy_and_delete_function(body)

    def run_fts_to_test_against_queries(self):
        bucket, scope, _ = self._namespace_parts()
        fts_query = self.construct_fts_query(
            self.fts_index_name,
            self.fts_query['query']
        )
        hits, _, _, _ = self.fts_callable.run_fts_query(
            index_name=self.fts_index_name,
            query_dict=fts_query,
            bucket_name=bucket,
            scope_name=scope,
            node=self.servers[1]
        )
        print("hits:", hits)
        self.assertEqual(
            hits,
            self.fts_query['expected_hits'],
            "FTS query should return %s hits, got %s" % (
                self.fts_query['expected_hits'],
                hits
            )
        )

    def test_fts_with_subdoc_op(self):
        '''
        FTS match query + sub-document lookup
        '''
        bucket, scope, _ = self._namespace_parts()
        self.load_sample_buckets(self.server, "travel-sample")
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.fts_index = self.create_fts_index(bucket)
        self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=self.fts_index)
        self.sleep(30, "Waiting for indexing to complete")
        body = self.create_save_function_body(self.function_name, self.handler_code,
            src_binding=True)
        for b in body['depcfg']['buckets']:
            if b['alias'] == 'src_bucket':
                b['alias'] = 'src_col'
                break
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.sleep(10)
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.run_fts_to_test_against_queries()
        self.undeploy_and_delete_function(body)

    def test_fts_with_constant_binding(self):
        '''
        FTS match query using constant binding for search term
        '''
        bucket, scope, _ = self._namespace_parts()
        self.load_sample_buckets(self.server, "travel-sample")
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.fts_index = self.create_fts_index(bucket)
        self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=self.fts_index)
        self.sleep(30, "Waiting for indexing to complete")
        body = self.create_save_function_body(self.function_name,self.handler_code)
        body['depcfg']['constants'] = [{"value": "FTS_TERM", "literal": "\"location hostel\""}]
        self.rest.delete_single_function(body['appname'], self.function_scope)
        self.log.info("Deleted function to re-create with constant bindings")
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.log.info("Re-created function with constants: {}".format(
            body['depcfg']['constants']))
        server_def = json.loads(self.rest.get_function_details(
            body['appname'], self.function_scope))
        self.log.info("Server-side depcfg.constants = {}".format(
            server_def.get('depcfg', {}).get('constants', 'NOT FOUND')))
        self.deploy_function(body)
        self.sleep(30)
        exec_stats = self.rest.get_event_execution_stats(
            body['appname'], self.function_scope)
        self.log.info("Execution stats: {}".format(exec_stats))
        fail_stats = self.rest.get_event_failure_stats(
            body['appname'], self.function_scope)
        self.log.info("Failure stats: {}".format(fail_stats))
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.run_fts_to_test_against_queries()
        self.undeploy_and_delete_function(body)

    def test_fts_with_analytics(self):
        '''
        FTS match query + Analytics query
        '''
        bucket, scope, _ = self._namespace_parts()
        self.load_sample_buckets(self.server, "travel-sample")
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.fts_index = self.create_fts_index(bucket)
        self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=self.fts_index)
        self.sleep(30, "Waiting for indexing to complete")
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        cbas_rest.execute_statement_on_cbas(
            "CREATE DATAVERSE `travel-sample`.`inventory`", None)
        cbas_rest.execute_statement_on_cbas(
            "CREATE ANALYTICS COLLECTION `travel-sample`.`inventory`.`airline` "
            "ON `travel-sample`.`inventory`.`airline`", None)
        cbas_rest.execute_statement_on_cbas("CONNECT LINK Local", None)
        self.sleep(15, "Waiting for analytics to ingest travel-sample data")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.sleep(10)
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.run_fts_to_test_against_queries()
        self.undeploy_and_delete_function(body)

    def test_fts_coexistence(self):
        """
        test for counter, timer, crc64, base64, n1ql, curl, bcuket_cache and xattrs.
        """
        coexistence_type = self.input.param('coexistence_type', '')
        expected_doc_count = 1
        sleep_time = 10
        sleep_msg = None

        if any(key in coexistence_type for key in ('curl', 'xattrs', 'base64')):
            expected_doc_count = 2
        else:
            expected_doc_count = 1

        if 'timer' in coexistence_type:
            sleep_time = 60
            sleep_msg = "Waiting for timer callback to fire and write results"

        bucket, scope, _ = self._namespace_parts()
        self.load_sample_buckets(self.server, "travel-sample")
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.fts_index = self.create_fts_index(bucket)
        self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count,idx=self.fts_index)
        self.sleep(30, "Waiting for indexing to complete")
        self.run_fts_to_test_against_queries()
        if coexistence_type == 'bucket_cache':
            body = self.create_save_function_body(self.function_name, self.handler_code,
            src_binding=True)
        else:
            body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.sleep(sleep_time, sleep_msg)
        self.verify_doc_count_collections("default.scope0.collection1", expected_doc_count)