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
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.replicas = self.input.param("replicas", 0)
            self.bucket_size = 100
            # This is needed as we have increased the context size to 93KB. If this is not increased the metadata
            # bucket goes into heavy DGM
            self.metadata_bucket_size = 200
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.replicas)
            bucket_params_meta = self._create_bucket_params(server=self.server, size=self.metadata_bucket_size,
                                                       replicas=self.replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params_meta)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
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
        ##index is required for delete operation through n1ql
        # self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        # self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
        #                               item_flag=self.item_flag, n1ql_port=self.n1ql_port,
        #                               full_docs_list=self.full_docs_list, log=self.log, input=self.input,
        #                               master=self.master, use_rest=True)
        # self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)


    def tearDown(self):
        try:
            self.print_go_routine_dump_from_all_eventing_nodes()
        except:
            # This is just a go routine dump API. Ignore the exceptions.
            pass
        try:
            self.print_eventing_stats_from_all_eventing_nodes()
        except:
            # This is just a stats API. Ignore the exceptions.
            pass
        super(EventingFailover, self).tearDown()

    def test_vb_shuffle_during_failover(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        self.wait_for_failover()
        task.result()
        fail_over_task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_vb_shuffle_during_failover_add_back(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        self.wait_for_failover()
        fail_over_task.result()
        task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + eventing_server.ip, "full")
        self.rest.add_back_node('ns_1@' + eventing_server.ip)
        self.undeploy_and_delete_function(body)

    def test_vb_shuffle_during_failover_rebalance(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + eventing_server.ip, "full")
        self.rest.add_back_node('ns_1@' + eventing_server.ip)
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        self.undeploy_and_delete_function(body)


    def test_failover_and_lifecycle(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        body = self.create_save_function_body(self.function_name+"1", self.handler_code)
        self.wait_for_failover()
        try:
            self.deploy_function(body)
        except Exception as e:
            self.log.info(str(e))
            if "ERR_REBALANCE_OR_FAILOVER_ONGOING" not in str(e):
                self.fail("Lifecycle operation succeed even when failover is running")
        fail_over_task.result()
        task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_lifecycle_and_failover(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        self.deploy_function(body,wait_for_bootstrap=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        self.wait_for_failover()
        fail_over_task.result()
        task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_multiple_eventing_failover_with_failover_running(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        self.wait_for_failover()
        fail_over_task2 = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[2]],
                                                      graceful=False)
        fail_over_task.result()
        fail_over_task2.result()
        task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_multiple_eventing_failover(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task2 = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[2]],
                                                      graceful=False)
        self.wait_for_failover()
        fail_over_task.result()
        fail_over_task2.result()
        task.result()
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,
                                         expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                         expected_duplicate=True)
        self.undeploy_and_delete_function(body)


    def test_failover_with_multiple_handlers(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, replicas=self.replicas)
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
        self.wait_for_failover()
        fail_over_task.result()
        task.result()
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , skip_stats_validation=True)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , bucket=self.dst_bucket_name1,
                                     skip_stats_validation=True)
        self.undeploy_delete_all_functions()


    def test_failover_rebalance_failed(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        self.wait_for_failover()
        try:
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            self.fail("Rebalance operation succeed even when failover is running")
        except Exception as e:
            self.log.info(str(e))
            if "Rebalance failed. See logs for detailed reason. You can try again." not in str(e):
                self.fail("Error missmatch {}".format(e))
        fail_over_task.result()
        task.result()
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                     expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_failover_rebalance_out(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        fail_over_task.result()
        self.wait_for_failover()
        while self.check_eventing_rebalance():
            pass
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        task.result()
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                     expected_duplicate=True)
        self.undeploy_and_delete_function(body)

    def test_failover_with_multiple_handlers_pause(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, replicas=self.replicas)
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
        self.wait_for_failover()
        fail_over_task.result()
        task.result()
        while self.check_eventing_rebalance():
            pass
        self.resume_function(body1)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , skip_stats_validation=True)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 , bucket=self.dst_bucket_name1,
                                     skip_stats_validation=True)
        self.undeploy_delete_all_functions()