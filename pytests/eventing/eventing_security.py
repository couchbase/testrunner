import copy

from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_ONDEPLOY, HANDLER_CODE_FTS_QUERY_SUPPORT, HANDLER_CODE_ANALYTICS
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.security.ntonencryptionBase import ntonencryptionBase
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from pytests.fts.fts_callable import FTSCallable
from pytests.eventing.fts_query_definitions import ALL_QUERIES
import logging
import json

log = logging.getLogger()


class EventingSecurity(EventingBaseTest):
    def setUp(self):
        super(EventingSecurity, self).setUp()
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert_rebalance.js"
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'ondeploy_test':
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_TLS_N2N
        elif handler_code == 'fts_match_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY
            self.is_fts = True
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'analytics_basic_select':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
            self.is_analytics = True
        else:
            self.handler_code = "handler_code/ABO/insert_rebalance.js"

        # FTS setup
        if getattr(self, 'is_fts', False):
            self.fts_index_name = "travel_sample_test"
            self.fts_doc_count = 31500
            self.fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
            self.fts_memory_quota = 3000
            log.info("Setting FTS memory quota to %s MB" % self.fts_memory_quota)
            self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=self.fts_memory_quota)
        # Analytics setup
        if getattr(self, 'is_analytics', False):
            self.load_sample_buckets(self.server, "travel-sample")
            self._setup_analytics()

    def tearDown(self):
        if getattr(self, 'is_fts', False) and getattr(self, 'fts_index_name', None):
            try:
                self.fts_callable.delete_fts_index(self.fts_index_name)
                log.info("Deleted FTS index: %s" % self.fts_index_name)
            except Exception as e:
                log.warning("Failed to delete FTS index %s: %s" % (self.fts_index_name, str(e)))
        elif getattr(self, 'is_analytics', False):
            try:
                cbas_node = self.get_nodes_from_services_map(service_type="cbas")
                if cbas_node:
                    cbas_rest = RestConnection(cbas_node)
                    cbas_rest.execute_statement_on_cbas("DISCONNECT LINK Local", None)
            except Exception as e:
                log.exception("Analytics teardown cleanup failed: %s", str(e))
        super(EventingSecurity, self).tearDown()

    '''
    Test steps -
    1. Disable n2n encryption.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, enable n2n encryption, deploy/resume handler, delete docs from src bucket and verify mutations are processed or not.
    4. Pause/undeploy handler, change encryption level to all, deploy/resume handler, load docs into src bucket and verify mutations are processed or not.
    5. Pause/undeploy handler, disable n2n encryption, deploy/resume handler, delete docs from src bucket and verify mutations are processed or not.
    '''
    def test_eventing_with_n2n_encryption_enabled(self):
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.server, "travel-sample")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        ntonencryptionBase().disable_nton_cluster([self.master])
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after enabling n2n encryption")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel="all")
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after changing encryption level to all")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        # Run FTS validation if FTS handler is being used
        if getattr(self, 'is_fts', False):
            self.run_fts_validation()

        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self._verify_analytics_result_matches_direct_query()

        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().disable_nton_cluster([self.master])
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after disabling n2n encryption")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable enforce tls, n2n encryption.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, enable n2n encryption, deploy/resume handler, load docs into src bucket and verify mutations are processed or not.
    4. Pause/undeploy handler, enforce tls, deploy/resume handler, load docs into src bucket and verify mutations are processed or not.
    5. Verify that all processes bind to ssl ports only after enforcing tls.
    6. Pause/undeploy handler, disable enforce tls by changing encryption level to control, load docs into src bucket and verify mutations are processed or not.
    7. Pause/undeploy handler, disable n2n encryption, deploy/resume handler, delete docs from src bucket and verify mutations are processed or not.
    '''
    def test_eventing_with_enforce_tls_feature(self):
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.server, "travel-sample")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        ntonencryptionBase().disable_nton_cluster([self.master])
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after enabling n2n encryption")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after enforcing strict TLS")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        # Run FTS validation if FTS handler is being used
        if getattr(self, 'is_fts', False):
            self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self._verify_analytics_result_matches_direct_query()
        assert ClusterOperationHelper.check_if_services_obey_tls(servers=[self.master]), "Port binding after enforcing TLS incorrect"
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after changing encryption level")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().disable_nton_cluster([self.master])
        if getattr(self, 'is_analytics', False) or getattr(self, 'is_fts', False):
            self.sleep(30, "Waiting for services to stabilize after disabling n2n encryption")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable enforce tls.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Enforce tls, delete docs from src bucket and verify mutations are not processed.
    4. Pause/undeploy handler, resume/deploy handler and verify that mutations are processed.
    5. Verify that all processes bind to ssl ports only after enforcing tls.
    6. Pause/undeploy handler, disable enforce tls and n2n encryption.
    7. Resume/deploy handler, load docs into src bucket and verify mutations are processed or not.
    '''
    # MB-47707
    def test_verify_mutations_are_not_processed_after_enforce_tls_and_before_any_lifecycle_operation(self):
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        # Wait for eventing to catch up with all the delete mutations
        self.sleep(10)
        on_delete_success = self.get_stats_value(self.function_name, "execution_stats.on_delete_success")
        on_delete_failure = self.get_stats_value(self.function_name, "execution_stats.on_delete_failure")
        self.log.info("execution_stats.on_delete_success: {}".format(on_delete_success))
        self.log.info("execution_stats.on_delete_failure: {}".format(on_delete_failure))
        if on_delete_success == 0:
            self.fail("on_delete_success is 0, expected at least some on_delete mutations processed. on_delete_failure: {0}".format(on_delete_failure))
        assert ClusterOperationHelper.check_if_services_obey_tls(servers=[self.master]), "Port binding after enforcing TLS incorrect"
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        ntonencryptionBase().disable_nton_cluster([self.master])
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable enforce tls.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, resume/deploy handler and while handler is undergoing lifecycle operation enforce tls.
    4. Validate that handler isn't stuck in deploying state.
    5. Delete docs from src bucket and  verify mutations are processed or not.
    6. Verify that all processes bind to ssl ports only after enforcing tls.
    '''
    # MB-47946
    def test_enforcing_tls_during_handler_lifecycle_operation(self):
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        if self.pause_resume:
            self.resume_function(body, wait_for_resume=False)
        else:
            self.deploy_function(body, wait_for_bootstrap=False)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.wait_for_handler_state(body['appname'], "deployed")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations
        self.sleep(10)
        on_delete_success = self.get_stats_value(self.function_name, "execution_stats.on_delete_success")
        on_delete_failure = self.get_stats_value(self.function_name, "execution_stats.on_delete_failure")
        self.log.info("execution_stats.on_delete_success: {}".format(on_delete_success))
        self.log.info("execution_stats.on_delete_failure: {}".format(on_delete_failure))
        if on_delete_success == 0:
            self.fail("on_delete_success is 0, expected at least some on_delete mutations processed. on_delete_failure: {0}".format(on_delete_failure))
        assert ClusterOperationHelper.check_if_services_obey_tls(servers=[self.master]), "Port binding after enforcing TLS incorrect"
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable n2n encryption.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, enable n2n encryption with encryption level control and rebalance in eventing node.
    4. Resume/deploy handler, delete docs from src bucket and verify mutations are processed or not.
    5. Rebalance out eventing node, load docs into src bucket and verify mutations are processed or not.
    6. Repeat steps 3-5 for encryption level all and strict.
    '''
    def test_eventing_rebalance_with_n2n_encryption_and_enforce_tls(self):
        ntonencryptionBase().disable_nton_cluster([self.master])
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        for level in ["control", "all", "strict"]:
            if self.pause_resume:
                self.pause_function(body)
            else:
                self.undeploy_function(body)
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel=level)
            if self.x509enable:
                self.upload_x509_certs(self.servers[self.nodes_init])
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            if self.pause_resume:
                self.resume_function(body)
            else:
                self.deploy_function(body)
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
            self.verify_doc_count_collections("default.scope0.collection1", 0)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def construct_fts_query(self, index_name, query):
        """Helper method to construct FTS query for validation"""
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

    def run_fts_validation(self):
        """Run FTS query validation to ensure FTS is working correctly"""
        if not getattr(self, 'is_fts', False):
            return

        log.info("Running FTS validation...")
        bucket = "travel-sample"
        scope = "_default"

        fts_query = self.construct_fts_query(
            self.fts_index_name,
            self.fts_query['query']
        )

        hits, _, _, _ = self.fts_callable.run_fts_query(
            index_name=self.fts_index_name,
            query_dict=fts_query,
            bucket_name=bucket,
            scope_name=scope,
            node=self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)
        )

        log.info("FTS query returned %s hits, expected %s" % (hits, self.fts_query['expected_hits']))
        self.assertEqual(
            hits,
            self.fts_query['expected_hits'],
            "FTS query should return %s hits, got %s" % (
                self.fts_query['expected_hits'],
                hits
            )
        )
        log.info("FTS validation passed successfully")
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
        result = cbas_rest.execute_statement_on_cbas("SELECT COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline`", None)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        parsed = json.loads(result)
        count = parsed["results"][0]["cnt"]
        log.info("Analytics airline collection has %s docs" % count)
        self.assertTrue(count > 0, "Analytics airline collection has 0 docs — ingestion failed")

    def _verify_analytics_result_matches_direct_query(self):
        """
        Run the analytics basic select query directly via the CBAS REST API,
        then read the doc written by the eventing handler from collection1 via N1QL,
        and compare the results.
        """
        analytics_query = "SELECT name, country FROM `travel-sample`.`inventory`.`airline` LIMIT 5"
        data_field = 'data'

        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        direct_result = cbas_rest.execute_statement_on_cbas(analytics_query, None)
        if isinstance(direct_result, bytes):
            direct_result = direct_result.decode("utf-8")
        direct_parsed = json.loads(direct_result)
        direct_rows = direct_parsed["results"]
        log.info("Direct analytics query returned %s rows" % len(direct_rows))

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

        log.info("Analytics result verification PASSED (%d rows match)" % len(direct_sorted))
