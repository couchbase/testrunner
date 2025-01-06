import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest
from membase.helper.cluster_helper import ClusterOperationHelper
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
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        force_disable_new_orchestration = self.input.param('force_disable_new_orchestration', False)
        if force_disable_new_orchestration:
            self.rest.diag_eval("ns_config:set(force_disable_new_orchestration, true).")

    def tearDown(self):
        super(EventingFailover, self).tearDown()

    def test_vb_shuffle_during_failover(self):
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
        # Wait for failover to complete
        self.sleep(10)
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
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + eventing_server[1].ip, "full")
        self.rest.add_back_node('ns_1@' + eventing_server[1].ip)
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        task.result()
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
