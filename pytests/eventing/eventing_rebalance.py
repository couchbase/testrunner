import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingRebalance(EventingBaseTest):
    def setUp(self):
        super(EventingRebalance, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.replicas = self.input.param("replicas", 0)
            self.bucket_size = 100
            # This is needed as we have increased the context size to 93KB. If this is not increased the metadata
            # bucket goes into heavy DGM
            self.metadata_bucket_size = 400
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
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMERS
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
        elif handler_code == 'n1ql_op_without_timers':
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
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITHOUT_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION
        elif handler_code == 'source_bucket_mutation_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS
        elif handler_code == 'source_bucket_mutation_delete':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE
        elif handler_code == 'source_bucket_mutation_timers_delete':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE
        elif handler_code == 'bucket_op_curl_get':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET
        elif handler_code == 'bucket_op_curl_post':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_POST
        elif handler_code == 'bucket_op_curl_put':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_PUT
        elif handler_code == 'bucket_op_curl_delete':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_DELETE
        elif handler_code == 'timer_op_curl_get':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_GET
        elif handler_code == 'timer_op_curl_post':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_POST
        elif handler_code == 'timer_op_curl_put':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_PUT
        elif handler_code == 'timer_op_curl_delete':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_DELETE
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        force_disable_new_orchestration = self.input.param('force_disable_new_orchestration', False)
        if force_disable_new_orchestration:
            self.rest.diag_eval("ns_config:set(force_disable_new_orchestration, true).")

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
        # try:
        #     self.cleanup_eventing()
        # except:
        #     # This is just a cleanup API. Ignore the exceptions.
        #     pass
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
        if self.pause_resume:
            self.pause_function(body, wait_for_pause=False)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code !=HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and self.handler_code
                !=HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
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
        if self.pause_resume:
            self.pause_function(body)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Fail over all eventing nodes and rebalance them out
        self.cluster.failover([self.master], failover_nodes=nodes_out_list, graceful=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
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
        if self.pause_resume:
            self.pause_function(body, wait_for_pause=False)
        # swap rebalance an eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
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
        if self.pause_resume:
            self.pause_function(body, wait_for_pause=False)
        # rebalance in a kv node when eventing is processing mutations
        services_in = ["kv"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                            self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
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
        if self.pause_resume:
            self.pause_function(body)
        # rebalance out kv node when eventing is processing mutations
        nodes_out_kv = self.servers[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_kv])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                            self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
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
        if self.pause_resume:
            self.pause_function(body, wait_for_pause=False)
        # swap rebalance kv node when eventing is processing mutations
        services_in = ["kv"]
        nodes_out_kv = self.servers[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [nodes_out_kv], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                            self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_rebalance_in_with_different_topologies(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        self.services_in = self.input.param("services_in")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=[self.services_in])
        task.result()
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                            self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_rebalance_out_with_different_topologies(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        self.server_out = self.input.param("server_out")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        nodes_out_list = self.servers[self.server_out]
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        task.result()
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                            self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
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
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        nodes_out_list = self.servers[self.server_out]
        # do a swap rebalance
        self.rest.add_node(self.master.rest_username, self.master.rest_password,
                           self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port,
                           services=[self.services_in])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                            self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_autofailover_with_eventing_rebalance(self):
        # enable auto-failover
        status = RestConnection(self.master).update_autofailover_settings(True, 10)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        gen_load_del = copy.deepcopy(self.gens_load)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                         self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        remote = RemoteMachineShellConnection(kv_node[1])
        remote.stop_server()
        self.sleep(40, "Wait for autofailover")
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], [eventing_node, kv_node[1]])
            reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            self.fail("rebalance failed with  error : {0}".format(str(ex)))
        finally:
            remote.start_server()
            self.sleep(120, "Wait for server to start")
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        if self.pause_resume:
            self.resume_function(body)
        try:
            self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True,
                                         timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a failover
            pass
        if self.pause_resume:
            self.pause_function(body)
        try:
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        except Exception as ex:
            log.info(str(ex))
            pass
        if self.pause_resume:
            self.resume_function(body)
        try:
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after failover/recovery/rebalance goes through successfully
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a delete as Onupdate would have extra mutations in destination
            pass
        self.undeploy_and_delete_function(body)

    def test_kv_failover_and_recovery_rebalance_with_eventing_node(self):
        failover_type = self.input.param('failover_type', 'hard')
        recovery_type = self.input.param('recovery_type', 'full')
        gen_load_del = copy.deepcopy(self.gens_load)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
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
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        #task.result()
        if rebalance:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # This is intenionally added
        self.sleep(60)
        if self.pause_resume:
            self.resume_function(body)
        try:
            stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True,
                                         timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a failover
            pass
        if self.pause_resume:
            self.pause_function(body)
        try:
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        except:
            pass
        if self.pause_resume:
            self.resume_function(body)
        try :
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after failover/recovery/rebalance goes through successfully
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a delete as Onupdate would have extra mutations in destination
            pass
        self.undeploy_and_delete_function(body)

    def test_eventing_failover_and_recovery_and_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
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
        # This is intenionally added
        self.sleep(60)
        if self.pause_resume:
            self.resume_function(body)
        try:
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after rebalance goes through successfully
            stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True,
                                         timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a failover
            pass
        if self.pause_resume:
            self.pause_function(body)
        try:
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        except:
            pass
        if self.pause_resume:
            self.resume_function(body)
        try:
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after failover/recovery/rebalance goes through successfully
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a delete as Onupdate would have extra mutations in destination
            pass
        self.undeploy_and_delete_function(body)

    def test_stop_start_eventing_rebalance(self):
        enable_failover = self.input.param('enable_failover', False)
        gen_load_del = copy.deepcopy(self.gens_load)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        if enable_failover:
            self.cluster.failover([self.master], failover_nodes=[nodes_out_ev])
        for i in range(5):
            # start eventing node rebalance
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
            # This sleep is intentional, if this is not present, rebalance_reached reports 100% (rebalance completed)
            # before rebalance could even start
            self.sleep(30)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
            if not RestHelper(self.rest).is_cluster_rebalanced():
                # stop the rebalance
                log.info("Stop the rebalance")
                stopped = RestConnection(self.master).stop_rebalance(wait_timeout=100)
                self.assertTrue(stopped, msg="unable to stop rebalance")
                # rebalance.result()
            else:
                log.info("Rebalance completed when tried to stop rebalance on {0}%".format(str(30)))
            if rebalance.state != "FINISHED":
                rebalance.result()
        if not enable_failover:
            task.result()
            if self.pause_resume:
                self.resume_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results after rebalance
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2,
                                             skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after rebalance goes through successfully
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.refresh_rest_server()
        self.undeploy_and_delete_function(body)

    def test_stop_start_kv_rebalance_which_has_eventing_nodes(self):
        enable_failover = self.input.param('enable_failover', False)
        gen_load_del = copy.deepcopy(self.gens_load)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_kv = self.servers[1]
        if enable_failover:
            self.cluster.failover([self.master], failover_nodes=[nodes_out_kv])
        for i in range(5):
            # start eventing node rebalance
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_kv])
            # This sleep is intentional, if this is not present, rebalance_reached reports 100% (rebalance completed)
            # before rebalance could even start
            self.sleep(30)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
            if not RestHelper(self.rest).is_cluster_rebalanced():
                # stop the rebalance
                log.info("Stop the rebalance")
                stopped = RestConnection(self.master).stop_rebalance(wait_timeout=100)
                self.assertTrue(stopped, msg="unable to stop rebalance")
                # rebalance.result()
            else:
                log.info("Rebalance was completed when tried to stop rebalance on {0}%".format(str(30)))
            if rebalance.state != "FINISHED":
                rebalance.result()
        if not enable_failover:
            task.result()
            if self.pause_resume:
                self.resume_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results after rebalance
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2,
                                             skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after rebalance goes through successfully
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,
                                             skip_stats_validation=True)
            else:
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
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance in a eventing nodes when eventing is processing mutations
        services_in = ["eventing", "eventing"]
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        task1 = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_del,
                                                 self.buckets[0].kvs[1], 'delete', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Remove 2 eventing nodes
        to_remove_nodes = nodes_out_list[0:2]
        # rebalance out 2 eventing nodes
        rebalance1 = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached1 = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached1, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        task1.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        if self.pause_resume:
            self.pause_function(body)
        # do a swap rebalance
        all_eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # add the previously removed nodes as part of swap rebalance
        for node in to_remove_nodes:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port,
                               services=["eventing"])
        # load data
        task2 = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_create,
                                                 self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], all_eventing_nodes)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task2.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 3, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    # Adding this test to validate MB-30394
    def test_eventing_rebalance_with_multiple_kv_nodes(self):
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
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance in a multiple 2 kv nodes when eventing is processing mutations
        services_in = ["kv", "kv"]
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        task1 = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_del,
                                                 self.buckets[0].kvs[1], 'delete', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        to_remove_nodes = to_add_nodes
        rebalance1 = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached1 = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached1, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        task1.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        all_kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        # add the previously removed nodes as part of swap rebalance
        for node in to_add_nodes:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port,
                               services=["kv"])
        # load data
        task2 = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_create,
                                                 self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], all_kv_nodes[1:3])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task2.result()
        self.master = all_kv_nodes[0]
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 3, skip_stats_validation=True)
        else:
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
        if self.pause_resume:
            self.pause_function(body)
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
        if self.pause_resume:
            self.pause_function(body1)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        if self.pause_resume:
            self.resume_function(body)
            self.resume_function(body1)
        # Wait for eventing to catch up with all the create mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.dst_bucket_name = self.dst_bucket_name1
        if self.is_sbm:
            self.verify_eventing_results(self.function_name+"_1", self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name+"_1", self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.undeploy_and_delete_function(body1)

    def test_memcache_crash_on_kv_and_eventing_node_during_eventing_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        kv_node = self.servers[1]
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        try:
            # load some data
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
            # rebalance in a eventing node when eventing is processing mutations
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            self.sleep(15)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            # kill memcached on kv and eventing when eventing rebalance is going on
            for node in [kv_node, eventing_node]:
                self.kill_memcached_service(node)
            self.sleep(15)
            reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            task.result()
        except Exception as ex:
            self.fail("Rebalance failed or hung after memcached crash")
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if not self.is_sbm:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_erl_crash_on_kv_and_eventing_node_during_eventing_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        kv_node = self.servers[1]
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        try:
            # load some data
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
            # rebalance in a eventing node when eventing is processing mutations
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            self.sleep(15)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            # kill erlang process on kv and eventing when eventing rebalance is going on
            for node in [kv_node, eventing_node]:
                self.kill_erlang_service(node)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            task.result()
        except Exception as ex:
            log.info("Rebalance failed as expected after erlang got killed: {0}".format(str(ex)))
        else:
            self.fail("Rebalance succeeded even after erl crash")
        # retry the failed rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], [])
        self.sleep(30)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "retry of the failed rebalance failed, stuck or did not complete")
        rebalance.result()
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if not self.is_sbm:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_reboot_of_kv_and_eventing_node_during_eventing_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        kv_node = self.servers[1]
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        try:
            # load some data
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
            # rebalance in a eventing node when eventing rebalance is going on
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            self.sleep(15)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            # reboot kv and eventing when eventing is processing mutations
            for node in [kv_node, eventing_node]:
                self.reboot_server(node)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            task.result()
        except Exception as ex:
            log.info("Rebalance failed as expected after reboot of kv and eventing: {0}".format(str(ex)))
        else:
            self.fail("Rebalance succeeded even after rebooting kv and eventing node")
        # retry the failed rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], [])
        self.sleep(30)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "retry of the failed rebalance failed, stuck or did not complete")
        rebalance.result()
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        try:
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a failover
            pass
        self.undeploy_and_delete_function(body)

    def test_killing_eventing_processes_during_eventing_rebalance(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        try:
            # load some data
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
            # rebalance in a eventing node when eventing is processing mutations
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            self.sleep(15)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            # kill eventing process when eventing rebalance is going on
            if len(eventing_nodes) < 2:
                self.fail("At least two eventing nodes are required")
            self.kill_consumer(eventing_nodes[0])
            self.kill_consumer(self.servers[self.nodes_init])
            self.kill_producer(eventing_nodes[1])
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            task.result()
        except Exception as ex:
            log.info("Rebalance failed as expected after eventing got killed: {0}".format(str(ex)))
        else:
            self.fail("Rebalance succeeded even after killing eventing processes")
        self.sleep(120)
        # retry the failed rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], [])
        self.sleep(30)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "retry of the failed rebalance failed, stuck or did not complete")
        rebalance.result()
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if not self.is_sbm:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_rebalance_out_all_eventing_nodes_and_rebalance_in_eventing_node_and_functions_should_be_restored(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # deploy a function
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # rebalance in a single eventing node
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_kv_eventing_failover_and_kv_eventing_rebalance_simultaneously(self):
        self.server_failed_over = self.input.param("server_failed_over")
        self.server_out = self.input.param("server_out")
        self.services_in = self.input.param("services_in")
        gen_load_del = copy.deepcopy(self.gens_load)
        server_failed_over = self.servers[self.server_failed_over]
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        try:
            # load some data
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        except:
            pass
        # failover a node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[server_failed_over],
                                                     graceful=False)
        fail_over_task.result()
        self.sleep(120)
        # do a swap rebalance
        server_out = self.servers[self.server_out]
        self.rest.add_node(self.master.rest_username, self.master.rest_password,
                           self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port,
                           services=[self.services_in])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [server_out])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        try:
            task.result()
        except:
            # data load might fail because of hard failover
            pass
        self.sleep(60)
        if self.pause_resume:
            self.resume_function(body)
        try:
            stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
            # Wait for eventing to catch up with all the update mutations and verify results after rebalance
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True,
                                         timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a failover
            pass
        try:
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        except:
            pass
        try:
            # Wait for eventing to catch up with all the delete mutations and verify results
            # This is required to ensure eventing works after rebalance goes through successfully
            if not self.is_sbm:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, timeout=240)
        except Exception as ex:
            log.info(str(ex))
            # data mismatch is expected in case of a delete as Onupdate would have extra mutations in destination
            pass
        self.undeploy_and_delete_function(body)

    def test_function_deploy_when_a_node_is_rebalanced_in(self):
        services_in = self.input.param("services_in", "eventing")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        try:
            # rebalance in a node when eventing is processing mutations
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [], services=[services_in])
            self.sleep(timeout=5)
            expected_progress = 35
            reached = RestHelper(self.rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(expected_progress))
            if not RestHelper(self.rest).is_cluster_rebalanced():
                self.deploy_function(body)
            else:
                self.fail("Rebalance completed before we could start deployment of function")
        except Exception as ex:
            log.info("{0}".format(str(ex)))
            if "Rebalance ongoing on some/all Eventing nodes, creating new apps or changing settings for existing " \
               "apps isn't allowed" not in str(ex):
                self.fail("Function deployment did not fail with expected error message : {0}".format(str(ex)))
        else:
            self.fail("Deployment of function succeeded during rebalance...")
        finally:
            reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        self.undeploy_and_delete_function(body)

    def test_function_rebalance_when_lifecycle_operation_is_going_on(self):
        services_in = self.input.param("services_in", "eventing")
        # worker_count_count is intentionally set to smaller value so that bootstrap takes more time completed
        # and we get enough time
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # deploy the function, don't wait for bootstrap to complete
        self.deploy_function(body, wait_for_bootstrap=False)
        try:
            # rebalance in a node when eventing is processing mutations
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [], services=[services_in])
            self.sleep(timeout=5)
            reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        except Exception as ex:
            log.info("{0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("Rebalance failed with wrong error message : {0}".format(str(ex)))
        else:
            self.fail("Rebalance succeeded when lifecycle operation is going on...")
        self.wait_for_handler_state(self.function_name, "deployed")
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # Retry failed rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init+1], [], [])
        self.sleep(60)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_kv_eventing_rebalance_with_multiple_functions_deployed(self):
        self.services_in = self.input.param("services_in", "eventing")
        self.server_out = self.input.param("server_out")
        self.num_functions = self.input.param("num_functions", 10)
        self.skip_validation = self.input.param("skip_validation", False)
        handler_code = self.input.param('handler_code', 'bucket_op_with_rand')
        if handler_code == 'bucket_op_with_doc_timer_rand':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_DOC_TIMER_RAND
        elif handler_code == 'bucket_op_with_cron_timer_rand':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_CRON_TIMER_RAND
        elif handler_code == 'bucket_op_with_rand':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_RAND
        else:
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_RAND
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
        body_array = []
        # deploy multiple functions
        for i in range(self.num_functions):
            body = self.create_save_function_body(self.function_name + str(i), self.handler_code)
            body_array.append(body)
            self.deploy_function(body)
            if self.pause_resume:
                self.pause_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        nodes_out_list = self.servers[self.server_out]
        # do a swap rebalance while multiple functions are deployed
        self.rest.add_node(self.master.rest_username, self.master.rest_password,
                           self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port,
                           services=[self.services_in])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task.result()
        if self.pause_resume:
            for body in body_array:
                self.resume_function(body)
        # This needs to be skipped in case of doc timers as multiple doc timers can't process same doc
        if not self.skip_validation:
            try:
                # Wait for eventing to catch up with all the update mutations and verify results after rebalance
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * self.num_functions,
                                             skip_stats_validation=True, timeout=1200)
            except Exception as ex:
                log.info(str(ex))
                stats_map = self.get_index_stats(perNode=False)
                item_count_dst_bucket = stats_map[self.dst_bucket_name]["#primary"]["items_count"]
                log.info("Destination item count : {0}\n\n".format(item_count_dst_bucket))
                # See MB-30764, Hence adding these queries
                n1ql_query1 = "select meta().id, * from metadata where \
                                meta().id not like 'eventing::%::vb::%' and \
                                meta().id not like 'eventing::%:rt:%' and \
                                meta().id not like 'eventing::%:sp'"
                result1 = self.n1ql_helper.run_cbq_query(query=n1ql_query1, server=self.n1ql_node)
                log.info("\n RESULTS for query {1} : count : {2} \n\n{0} \n\n".format(json.dumps(result1["results"],
                                                                                                sort_keys=True,
                                                                                                indent=4),
                                                                                      n1ql_query1,
                                                                                      len(result1["results"])))
                n1ql_query2 = "select meta().id, * from metadata where \
                                meta().id like 'eventing::%:sp'\
                                and sta != stp"
                result2 = self.n1ql_helper.run_cbq_query(query=n1ql_query2, server=self.n1ql_node)
                log.info("\n RESULTS for query {1} : count : {2} \n\n{0} \n\n".format(json.dumps(result2["results"],
                                                                                                sort_keys=True,
                                                                                                indent=4),
                                                                                      n1ql_query2,
                                                                                      len(result2["results"])))
                n1ql_query3 = "select meta().id, * from metadata where\
                                meta().id like 'eventing::%:rt:%'"
                result3 = self.n1ql_helper.run_cbq_query(query=n1ql_query3, server=self.n1ql_node)
                log.info("\n RESULTS for query {1} : count : {2} \n\n{0} \n\n".format(json.dumps(result3["results"],
                                                                                                sort_keys=True,
                                                                                                indent=4),
                                                                                      n1ql_query3,
                                                                                      len(result3["results"])))
                if item_count_dst_bucket != self.docs_per_day * 2016 * self.num_functions:
                    raise
        else:
            self.sleep(300)
            eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
            for eventing_node in eventing_nodes:
                rest_conn = RestConnection(eventing_node)
                out = rest_conn.get_all_eventing_stats()
                log.info("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                            indent=4)))
        try:
            self.refresh_rest_server()
            # delete all the functions
            for body in body_array:
                self.undeploy_and_delete_function(body)
        except:
            pass
        try:
            stats_map = self.get_index_stats(perNode=False)
            item_count_metadata = stats_map[self.metadata_bucket_name]["#primary"]["items_count"]
            log.info("No of items in metadata bucket after undeploy/delete of all the functions : {0}".
                     format(item_count_metadata))
            if item_count_metadata != 0:
                log.warn("metadata bucket still has some documents after undeploying the function : {0} docs are "
                         "remaining".format(item_count_metadata))
        except:
            pass

