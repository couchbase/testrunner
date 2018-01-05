import copy

from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingRebalance(EventingBaseTest):
    def setUp(self):
        super(EventingRebalance, self).setUp()
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            # index is required for delete operation through n1ql
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.n1ql_helper = N1QLHelper(shell=self.shell,
                                          max_verify=self.max_verify,
                                          buckets=self.buckets,
                                          item_flag=self.item_flag,
                                          n1ql_port=self.n1ql_port,
                                          full_docs_list=self.full_docs_list,
                                          log=self.log, input=self.input,
                                          master=self.master,
                                          use_rest=True
                                          )
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE

    def tearDown(self):
        super(EventingRebalance, self).tearDown()

    def test_eventing_rebalance_in_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_eventing_rebalance_out_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_eventing_swap_rebalance_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # swap rebalance an eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_kv_rebalance_in_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # rebalance in a kv node when eventing is processing mutations
        services_in = ["kv"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_kv_rebalance_out_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # rebalance out kv node when eventing is processing mutations
        nodes_out_kv = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_kv])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_kv_swap_rebalance_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # swap rebalance kv node when eventing is processing mutations
        services_in = ["kv"]
        nodes_out_kv = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [nodes_out_kv], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_rebalance_in_with_different_topologies(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        self.services_in = self.input.param("services_in")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=[self.services_in])
        task.result()
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_rebalance_out_with_different_topologies(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        self.server_out = self.input.param("server_out")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        nodes_out_list = self.servers[self.server_out]
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        task.result()
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_swap_rebalance_with_different_topologies(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        self.server_out = self.input.param("server_out")
        self.services_in = self.input.param("services_in")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        nodes_out_list = self.servers[self.server_out]
        # do a swap rebalance
        self.rest.add_node(self.master.rest_username, self.master.rest_password,
                           self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port,
                           services=[self.services_in])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_autofailover_with_eventing_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                         self.buckets[0].kvs[1], 'create')
        RestConnection(self.master).update_autofailover_settings(True, 30)
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        remote = RemoteMachineShellConnection(kv_node[1])
        remote.stop_server()
        self.sleep(40, "Wait for autofailover")
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], [eventing_node, kv_node[1]])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception, ex:
            self.fail("rebalance failed with  error : {0}".format(str(ex)))
        finally:
            remote.start_server()
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after failover/recovery/rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_kv_failover_and_recovery_rebalance_with_eventing_node(self):
        failover_type = self.input.param('failover_type', 'hard')
        recovery_type = self.input.param('recovery_type', 'full')
        gen_load_del = copy.deepcopy(self.gens_load)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # fail over the kv node
        if failover_type == "hard":
            fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=False)
        else:
            fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=True)
        fail_over_task.result()
        self.sleep(120)
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + kv_server.ip, recovery_type)
        self.rest.add_back_node('ns_1@' + kv_server.ip)
        task.result()
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        if rebalance:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after failover/recovery/rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_failover_and_recovery_and_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # fail over the kv node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server], graceful=False)
        fail_over_task.result()
        self.sleep(120)
        # do a recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + eventing_server.ip, "full")
        self.rest.add_back_node('ns_1@' + eventing_server.ip)
        task.result()
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        if rebalance:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after failover/recovery/rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_stop_start_eventing_rebalance(self):
        enable_failover = self.input.param('enable_failover', False)
        gen_load_del = copy.deepcopy(self.gens_load)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        if enable_failover:
            self.cluster.failover([self.master], failover_nodes=[nodes_out_ev])
        for i in xrange(5):
            # start eventing node rebalance
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
            if not RestHelper(self.rest).is_cluster_rebalanced():
                # stop the rebalance
                log.info("Stop the rebalance")
                stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, msg="unable to stop rebalance")
                # rebalance.result()
            else:
                log.info("Rebalance was completed when tried to stop rebalance on {0}%".format(str(30)))
        task.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_stop_start_kv_rebalance_which_has_eventing_nodes(self):
        enable_failover = self.input.param('enable_failover', False)
        gen_load_del = copy.deepcopy(self.gens_load)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        if enable_failover:
            self.cluster.failover([self.master], failover_nodes=[nodes_out_ev])
        for i in xrange(5):
            # start eventing node rebalance
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
            if not RestHelper(self.rest).is_cluster_rebalanced():
                # stop the rebalance
                log.info("Stop the rebalance")
                stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, msg="unable to stop rebalance")
                # rebalance.result()
            else:
                log.info("Rebalance was completed when tried to stop rebalance on {0}%".format(str(30)))
        task.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_rebalance_with_multiple_eventing_nodes(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        gen_load_create = copy.deepcopy(self.gens_load)
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        self.deploy_function(body)
        # load data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing", "eventing"]
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        task1 = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_del,
                                                 self.buckets[0].kvs[1], 'delete')
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        to_remove_nodes = nodes_out_list[0:2]
        # rebalance out all eventing nodes
        rebalance1 = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached1 = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached1, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        task1.result()
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        # do a swap rebalance
        all_eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # add the previously removed nodes as part of swap rebalance
        for node in to_remove_nodes:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port,
                               services=["eventing"])
        # load data
        task2 = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_create,
                                                 self.buckets[0].kvs[1], 'create')
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], all_eventing_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task2.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_rebalance_with_multiple_functions_deployed(self):
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        # Create one extra bucket for second function
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=500)
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        # deploy the first function
        body = self.create_save_function_body(self.function_name,
                                              HANDLER_CODE.BUCKET_OPS_ON_UPDATE,
                                              worker_count=3)
        self.deploy_function(body)
        # deploy the second function
        body1 = self.create_save_function_body(self.function_name + "_1",
                                               HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER_WITH_SECOND_BUCKET,
                                               worker_count=3)
        # this is required to deploy multiple functions at the same time
        del body1['depcfg']['buckets'][0]
        body1['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        self.deploy_function(body1)
        # do a swap rebalance
        self.rest.add_node(self.master.rest_username, self.master.rest_password,
                           self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port,
                           services=["eventing"])
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.dst_bucket_name = self.dst_bucket_name1
        self.verify_eventing_results(self.function_name + "_1", self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)
        self.undeploy_and_delete_function(body1)
