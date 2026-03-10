import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL, HANDLER_CODE_FTS_QUERY_SUPPORT, HANDLER_CODE_ANALYTICS
from pytests.eventing.eventing_base import EventingBaseTest
from membase.helper.cluster_helper import ClusterOperationHelper
from pytests.fts.fts_callable import FTSCallable
from pytests.eventing.fts_query_definitions import ALL_QUERIES
import logging

log = logging.getLogger()


class EventingFailover(EventingBaseTest):
    def setUp(self):
        super(EventingFailover, self).setUp()
        self.buckets = self.rest.get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_RAND
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        elif handler_code == 'n1ql_op_without_timers':
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITHOUT_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION
        elif handler_code == 'bucket_op_curl_jenkins':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_JENKINS
        elif handler_code == 'fts_match_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY
            self.is_fts = True
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'analytics_basic_select':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
            self.is_analytics = True
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        force_disable_new_orchestration = self.input.param('force_disable_new_orchestration', False)
        if force_disable_new_orchestration:
            self.rest.diag_eval("ns_config:set(force_disable_new_orchestration, true).")
        if getattr(self, 'is_fts', False):
            self.fts_index_name = "travel_sample_test"
            self.fts_doc_count = 31500
            self.fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
            self.fts_memory_quota = 3000
            log.info("Setting FTS memory quota to %s MB" % self.fts_memory_quota)
            self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=self.fts_memory_quota)
        # analytics setup
        if getattr(self, 'is_analytics', False):
            self.load_sample_buckets(self.server, "travel-sample")
            self.load_data_to_collection(1, "default.scope0.collection0")
            self._setup_analytics()

    def tearDown(self):
        if getattr(self, 'is_fts', False) and getattr(self, 'fts_index_name', None):
            try:
                self.fts_callable.delete_fts_index(self.fts_index_name)
                log.info("Deleted FTS index: %s" % self.fts_index_name)
            except Exception as e:
                log.warning("Failed to delete FTS index %s: %s" % (self.fts_index_name, str(e)))
        try:
            if getattr(self, 'is_analytics', False):
                cbas_node = self.get_nodes_from_services_map(service_type="cbas")
                if cbas_node:
                    cbas_rest = RestConnection(cbas_node)
                    cbas_rest.execute_statement_on_cbas("DISCONNECT LINK Local", None)
        except Exception as e:
            log.exception("Analytics teardown cleanup failed: %s", str(e))
        super(EventingFailover, self).tearDown()

    def test_vb_shuffle_during_failover(self):
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.master, "travel-sample")
            self.sleep(60, "Waiting for travel-sample bucket to load and replicate")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if getattr(self, 'is_analytics', False):
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",
                                              wait_for_loading=False)
        elif self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        task.result()
        fail_over_task.result()
        # Wait for failover to complete
        self.sleep(10)
        # Run FTS validation if FTS handler is being used
        if getattr(self, 'is_fts', False):
            self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
            self._verify_analytics_result_matches_direct_query()
            self.undeploy_and_delete_function(body)
            return
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,expected_duplicate=True)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_vb_shuffle_during_failover_add_back(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        task.result()
        # Wait for failover to complete
        self.sleep(10)
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,expected_duplicate=True)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,expected_duplicate=True)
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + eventing_server[1].ip, "full")
        self.rest.add_back_node('ns_1@' + eventing_server[1].ip)
        self.undeploy_and_delete_function(body)

    def test_vb_shuffle_during_failover_rebalance(self):
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.master, "travel-sample")
            self.sleep(60, "Waiting for travel-sample bucket to load and replicate")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if getattr(self, 'is_analytics', False):
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",
                                              wait_for_loading=False)
        elif self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + eventing_server[1].ip, "full")
        self.rest.add_back_node('ns_1@' + eventing_server[1].ip)
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        task.result()
        # Run FTS validation if FTS handler is being used
        if getattr(self, 'is_fts', False):
            self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self.sleep(30, "Waiting for cluster to stabilize after recovery and rebalance")
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
            self._verify_analytics_result_matches_direct_query()
            self.undeploy_and_delete_function(body)
            return
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,expected_duplicate=True)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,expected_duplicate=True)
        self.undeploy_and_delete_function(body)


    def test_failover_and_lifecycle(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        body1 = self.create_save_function_body(self.function_name+"1", self.handler_code)
        try:
            self.deploy_function(body1)
        except Exception as e:
            self.log.info(str(e))
            if "ERR_REBALANCE_OR_FAILOVER_ONGOING" not in str(e):
                self.fail("Lifecycle operation succeed even when failover is running")
        fail_over_task.result()
        task.result()
        # Wait for failover to complete
        self.sleep(10)
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2,expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2,
                                                  expected_duplicate=True)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_lifecycle_and_failover(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        self.deploy_function(body)
        body1 = self.create_save_function_body(self.function_name + "1", self.handler_code)
        del body1['depcfg']['buckets'][0]
        body1['depcfg']['buckets'].append({"alias": "dst_bucket", "bucket_name": self.dst_bucket_name1, "access": "rw"})
        self.rest.create_function(body1['appname'], body1)
        self.deploy_function(body1,wait_for_bootstrap=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        task.result()
        self.wait_for_handler_state(body1['appname'],"deployed")
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2,expected_duplicate=True)
                self.verify_doc_count_collections("dst_bucket1._default._default",
                                                  self.docs_per_day * self.num_docs * 2, expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2,
                                                  expected_duplicate=True)
                self.verify_doc_count_collections("dst_bucket1._default._default",self.docs_per_day * self.num_docs * 2,
                                                  expected_duplicate=True)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
                self.verify_doc_count_collections("dst_bucket1._default._default",
                                                  self.docs_per_day * self.num_docs, expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
                self.verify_doc_count_collections("dst_bucket1._default._default",
                                                  self.docs_per_day * self.num_docs, expected_duplicate=True)
        self.undeploy_delete_all_functions()

    def test_multiple_eventing_failover_with_failover_running(self):
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.master, "travel-sample")
            self.sleep(60, "Waiting for travel-sample bucket to load and replicate")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if getattr(self, 'is_analytics', False):
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",
                                              wait_for_loading=False)
        elif self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task2 = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[2]],
                                                      graceful=False)
        fail_over_task.result()
        fail_over_task2.result()
        task.result()
        # Run FTS validation if FTS handler is being used
        if getattr(self, 'is_fts', False):
            self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self.sleep(10, "Waiting for cluster to stabilize after multiple failovers")
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
            self._verify_analytics_result_matches_direct_query()
            self.undeploy_and_delete_function(body)
            return
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2,expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2,
                                                  expected_duplicate=True)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
        while self.check_eventing_rebalance():
            pass
        self.undeploy_and_delete_function(body)

    def test_multiple_eventing_failover(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task2 = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[2]],
                                                      graceful=False)
        fail_over_task.result()
        fail_over_task2.result()
        task.result()
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2,expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2,
                                                  expected_duplicate=True)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,
                                                  expected_duplicate=True)
        self.undeploy_and_delete_function(body)


    def test_failover_with_multiple_handlers(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        body1 = self.create_save_function_body(self.function_name + "1", self.handler_code)
        del body1['depcfg']['buckets'][0]
        body1['depcfg']['buckets'].append({"alias": "dst_bucket", "bucket_name": self.dst_bucket_name1,"access": "rw"})
        self.rest.create_function(body1['appname'], body1)
        self.deploy_function(body1)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        task.result()
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , skip_stats_validation=True,
                                     expected_duplicate=True)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , bucket=self.dst_bucket_name1,
                                     skip_stats_validation=True,expected_duplicate=True)
        while self.check_eventing_rebalance():
            pass
        self.undeploy_delete_all_functions()


    def test_failover_rebalance_failed(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        self.create_n_handlers(5)
        self.deploy_n_handlers(5)
        self.wait_for_deployment_n_handlers(5)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        try:
            self.cluster.rebalance(self.servers[:self.nodes_init], [], [eventing_server[1]])
            self.fail("Rebalance operation succeed even when failover is running")
        except Exception as e:
            self.log.info(str(e))
            if "Rebalance failed. See logs for detailed reason. You can try again." not in str(e):
                self.fail("Error missmatch {}".format(e))
        task.result()
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
        self.undeploy_delete_all_functions()

    def test_failover_rebalance_out(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        task.result()
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,
                                              expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_failover_with_multiple_handlers_pause(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        body1 = self.create_save_function_body(self.function_name + "1", self.handler_code)
        del body1['depcfg']['buckets'][0]
        body1['depcfg']['buckets'].append({"alias": "dst_bucket", "bucket_name": self.dst_bucket_name1,"access": "rw"})
        self.rest.create_function(body1['appname'], body1)
        self.deploy_function(body1)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        self.pause_function(body1)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        task.result()
        self.resume_function(body1)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , skip_stats_validation=True,
                                     expected_duplicate=True)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , bucket=self.dst_bucket_name1,
                                     skip_stats_validation=True,expected_duplicate=True)
        self.undeploy_delete_all_functions()


    def test_vb_shuffle_disable(self):
        self.enable_disable_vb_distribution(enable=False)
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        task.result()
        fail_over_task.result()
        stats_dst = self.rest.get_bucket_stats(self.dst_bucket_name)
        self.log.info("documents in destination bucket {}".format(stats_dst["curr_items"]))
        if stats_dst["curr_items"] < self.docs_per_day * 2016:
            pass
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
        # Verify analytics returns data before deploying any handler
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

        log.info("Analytics result verification PASSED (%d rows match)" % len(direct_sorted))