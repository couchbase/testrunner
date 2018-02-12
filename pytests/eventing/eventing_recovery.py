import copy
import json

from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.memcached.helper.data_helper import MemcachedClientHelper
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingRecovery(EventingBaseTest):
    def setUp(self):
        super(EventingRecovery, self).setUp()
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
        super(EventingRecovery, self).tearDown()

    def test_killing_eventing_consumer_when_eventing_is_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_eventing_producer_when_eventing_is_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # kill eventing producer when eventing is processing mutations
        self.kill_producer(eventing_node)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # kill eventing producer when eventing is processing mutations
        self.kill_producer(eventing_node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_memcached_when_eventing_is_processing_mutations(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # kill memcached on kv and eventing when eventing is processing mutations
        for node in [kv_node, eventing_node]:
            self.kill_memcached_service(node)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # kill memcached on kv and eventing when eventing is processing mutations
        for node in [kv_node, eventing_node]:
            self.kill_memcached_service(node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_erlang_when_eventing_is_processing_mutations(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # kill erlang on kv and eventing when eventing is processing mutations
        for node in [kv_node, eventing_node]:
            self.kill_erlang_service(node)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # kill erlang on kv and eventing when eventing is processing mutations
        for node in [kv_node, eventing_node]:
            self.kill_erlang_service(node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_reboot_eventing_node_when_it_is_processing_mutations(self):
        gen_load_non_json_del = copy.deepcopy(self.gens_load)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # reboot eventing node when it is processing mutations
        self.reboot_server(eventing_node)
        task.result()
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json_del,
                                                self.buckets[0].kvs[1], 'delete')
        # reboot eventing node when it is processing mutations
        self.reboot_server(eventing_node)
        task.result()
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_network_partitioning_eventing_node_when_its_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        try:
            # partition the eventing node when its processing mutations
            for i in xrange(5):
                self.start_firewall_on_node(eventing_node)
            # load some data
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.sleep(180)
            self.stop_firewall_on_node(eventing_node)
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        except Exception:
            self.stop_firewall_on_node(eventing_node)
        finally:
            self.stop_firewall_on_node(eventing_node)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # This is intentionally added, it is sometimes seen that the stats are not populated for some time after
        # firewall stop/start
        self.sleep(60)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_reboot_n1ql_node_when_eventing_node_is_querying(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # reboot eventing node when it is processing mutations
        self.reboot_server(n1ql_node)
        task.result()
        stats = self.rest.get_all_eventing_stats()
        on_update_failure = stats[0]["execution_stats"]["on_update_failure"]
        n1ql_op_exception_count = stats[0]["failure_stats"]["n1ql_op_exception_count"]
        self.undeploy_and_delete_function(body)
        log.info("stats : {0}".format(stats))
        if on_update_failure != n1ql_op_exception_count or on_update_failure == 0 or n1ql_op_exception_count == 0:
            self.fail("No n1ql exceptions were found when n1ql node was rebooted while it was"
                      "processing queries from handler code or stats returned incorrect value")
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_erlang_on_n1ql_node_when_eventing_node_is_querying(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create')
        # reboot eventing node when it is processing mutations
        self.kill_erlang_service(n1ql_node)
        task.result()
        stats = self.rest.get_all_eventing_stats()
        on_update_failure = stats[0]["execution_stats"]["on_update_failure"]
        n1ql_op_exception_count = stats[0]["failure_stats"]["n1ql_op_exception_count"]
        self.undeploy_and_delete_function(body)
        log.info("stats : {0}".format(stats))
        if on_update_failure != n1ql_op_exception_count or on_update_failure == 0 or n1ql_op_exception_count == 0:
            self.fail("No n1ql exceptions were found when erlang process was killed on n1ql node while it was"
                      " processing queries from handler code or stats returned incorrect value")
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_memcached_on_n1ql_when_eventing_is_processing_mutations(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_UPDATE_DELETE)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # kill memcached on n1ql and eventing when eventing is processing mutations
        for node in [n1ql_node, eventing_node]:
            self.kill_memcached_service(node)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # kill memcached on n1ql and eventing when eventing is processing mutations
        for node in [n1ql_node, eventing_node]:
            self.kill_memcached_service(node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_network_partitioning_eventing_node_with_n1ql_when_its_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_UPDATE_DELETE)
        self.deploy_function(body)
        try:
            # partition the eventing node when its processing mutations
            for i in xrange(5):
                self.start_firewall_on_node(eventing_node)
            # load some data
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.sleep(180)
            self.stop_firewall_on_node(eventing_node)
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        except Exception:
            self.stop_firewall_on_node(eventing_node)
        finally:
            self.stop_firewall_on_node(eventing_node)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_n1ql_in_different_time_zone(self):
        try:
            eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
            self.change_time_zone(eventing_node, timezone="Asia/Kolkata")
            kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
            self.change_time_zone(kv_node, timezone="America/Los_Angeles")
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)
        finally:
            self.change_time_zone(eventing_node, timezone="UTC")
            self.change_time_zone(kv_node, timezone="UTC")

    def test_time_drift_between_kv_eventing(self):
        try:
            kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
            self.change_time_zone(kv_node, timezone="America/Los_Angeles")
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)
        finally:
            self.change_time_zone(kv_node, timezone="UTC")

    def test_partial_rollback(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        log.info("kv nodes:{0}".format(kv_node))
        for node in kv_node:
            mem_client = MemcachedClientHelper.direct_client(node, self.src_bucket_name)
            mem_client.stop_persistence()
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              worker_count=3)
        try:
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create')
        except Exception as e:
            log.info("error while loading data")
        self.deploy_function(body,wait_for_bootstrap=False)
        # Kill memcached on Node A
        self.log.info("Killing memcached on {0}".format(kv_node[1]))
        shell = RemoteMachineShellConnection(kv_node[1])
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on {0}".
                      format(kv_node[0]))
        mem_client = MemcachedClientHelper.direct_client(kv_node[0],
                                                         self.src_bucket_name)
        mem_client.start_persistence()
        # Wait for bootstrap to complete
        self.wait_for_bootstrap_to_complete(body['appname'])
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        log.info(stats_src)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)