import json
import logging
import random
import datetime
import os
from TestInput import TestInputSingleton
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.couchbase_helper.stats_tools import StatsCommon
from lib.membase.api.rest_client import RestConnection
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.basetestcase import BaseTestCase
from testconstants import INDEX_QUOTA, MIN_KV_QUOTA, EVENTING_QUOTA
from pytests.query_tests_helper import QueryHelperTests
from pytests.security.rbacmain import rbacmain

log = logging.getLogger()


class EventingBaseTest(QueryHelperTests, BaseTestCase):
    panic_count = 0

    def setUp(self):
        if self._testMethodDoc:
            log.info("\n\nStarting Test: %s \n%s" % (self._testMethodName, self._testMethodDoc))
        else:
            log.info("\n\nStarting Test: %s" % (self._testMethodName))
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(EventingBaseTest, self).setUp()
        self.master = self.servers[0]
        self.server = self.master
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.log.info(
            "Setting the min possible memory quota so that adding mode nodes to the cluster wouldn't be a problem.")
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=330)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
        # self.rest.set_service_memoryQuota(service='eventingMemoryQuota', memoryQuota=EVENTING_QUOTA)
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.log_level = self.input.param('log_level', 'TRACE')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.create_functions_buckets = self.input.param('create_functions_buckets', True)
        self.docs_per_day = self.input.param("doc-per-day", 1)
        random.seed(datetime.time)
        self.function_name = "Function_{0}_{1}".format(random.randint(1, 1000000000), self._testMethodName)

    def tearDown(self):
        # catch panics and print it in the test log
        self.check_eventing_logs_for_panic()
        super(EventingBaseTest, self).tearDown()

    def create_save_function_body(self, appname, appcode, description="Sample Description",
                                  checkpoint_interval=10000, cleanup_timers=False,
                                  dcp_stream_boundary="everything", deployment_status=True,
                                  rbacpass="password", rbacrole="admin", rbacuser="cbadminbucket",
                                  skip_timer_threshold=86400,
                                  sock_batch_size=1, tick_duration=5000, timer_processing_tick_interval=500,
                                  timer_worker_pool_size=3, worker_count=3, processing_status=True,
                                  cpp_worker_thread_count=1, multi_dst_bucket=False, execution_timeout=3):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        if multi_dst_bucket:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        body['depcfg']['metadata_bucket'] = self.metadata_bucket_name
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['settings'] = {}
        body['settings']['checkpoint_interval'] = checkpoint_interval
        body['settings']['cleanup_timers'] = cleanup_timers
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = deployment_status
        body['settings']['description'] = description
        body['settings']['log_level'] = self.log_level
        body['settings']['rbacpass'] = rbacpass
        body['settings']['rbacrole'] = rbacrole
        body['settings']['rbacuser'] = rbacuser
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        body['settings']['execution_timeout'] = execution_timeout
        return body

    def wait_for_bootstrap_to_complete(self, name):
        result = self.rest.get_deployed_eventing_apps()
        count = 0
        while name not in result and count < 20:
            self.sleep(30, message="Waiting for eventing node to come out of bootstrap state...")
            count += 1
            result = self.rest.get_deployed_eventing_apps()
        if count == 20:
            raise Exception(
                'Eventing took lot of time to come out of bootstrap state or did not successfully bootstrap')

    def verify_eventing_results(self, name, expected_dcp_mutations, doc_timer_events=False, on_delete=False,
                                skip_stats_validation=False):
        # This resets the rest server as the previously used rest server might be out of cluster due to rebalance
        num_nodes = self.refresh_rest_server()
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        if not skip_stats_validation:
            # we can't rely on DCP_MUTATION stats when doc timers events are set.
            # TODO : add this back when getEventProcessingStats works reliably for doc timer events as well
            if not doc_timer_events:
                count = 0
                if num_nodes <= 1:
                    stats = self.rest.get_event_processing_stats(name)
                else:
                    stats = self.rest.get_aggregate_event_processing_stats(name)
                if on_delete:
                    mutation_type = "DCP_DELETION"
                else:
                    mutation_type = "DCP_MUTATION"
                actual_dcp_mutations = stats[mutation_type]
                # This is required when binary data is involved where DCP_MUTATION will have process DCP_MUTATIONS
                # but ignore it
                # wait for eventing node to process dcp mutations
                log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
                while actual_dcp_mutations != expected_dcp_mutations and count < 20:
                    self.sleep(30, message="Waiting for eventing to process all dcp mutations...")
                    count += 1
                    if num_nodes <= 1:
                        stats = self.rest.get_event_processing_stats(name)
                    else:
                        stats = self.rest.get_aggregate_event_processing_stats(name)
                    actual_dcp_mutations = stats[mutation_type]
                    log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
                if count == 20:
                    raise Exception(
                        "Eventing has not processed all the {0}. Current : {1} Expected : {2}".format(mutation_type,
                                                                                                      actual_dcp_mutations,
                                                                                                      expected_dcp_mutations
                                                                                                      ))
        # wait for bucket operations to complete and verify it went through successfully
        count = 0
        stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        while stats_dst["curr_items"] != expected_dcp_mutations and count < 20:
            self.sleep(30, message="Waiting for handler code to complete all bucket operations...")
            count += 1
            stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        if stats_dst["curr_items"] != expected_dcp_mutations:
            # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
            for eventing_node in eventing_nodes:
                rest_conn = RestConnection(eventing_node)
                out = rest_conn.get_all_eventing_stats()
                full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
                log.info("Stats for Node {0} is {1} ".format(eventing_node.ip, out))
                log.debug("Full Stats for Node {0} is {1} ".format(eventing_node.ip, full_out))
            raise Exception(
                "Bucket operations from handler code took lot of time to complete or didn't go through. Current : {0} "
                "Expected : {1}".format(stats_dst["curr_items"], expected_dcp_mutations))
        # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
        # print all stats from all eventing nodes
        # These are the stats that will be used by ns_server and UI
        for eventing_node in eventing_nodes:
            rest_conn = RestConnection(eventing_node)
            out = rest_conn.get_all_eventing_stats()
            full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
            log.info("Stats for Node {0} is {1} ".format(eventing_node.ip, out))
            log.debug("Full Stats for Node {0} is {1} ".format(eventing_node.ip, full_out))

    def eventing_stats(self):
        self.sleep(30)
        content=self.rest.get_all_eventing_stats()
        js=json.loads(content)
        log.info("execution stats: {0}".format(js))
        # for j in js:
        #     print j["function_name"]
        #     print j["execution_stats"]["on_update_success"]
        #     print j["failure_stats"]["n1ql_op_exception_count"]

    def deploy_function(self, body, deployment_fail=False, wait_for_bootstrap=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        # save the function so that it appears in UI
        content = self.rest.save_function(body['appname'], body)
        # deploy the function
        log.info("Deploying the following handler code")
        log.info("\n{0}".format(body['appcode']))
        content1 = self.rest.deploy_function(body['appname'], body)
        log.info("deploy Application : {0}".format(content1))
        if deployment_fail:
            res = json.loads(content1)
            if not res["compile_success"]:
                return
            else:
                raise Exception("Deployment is expected to be failed but no message of failure")
        if wait_for_bootstrap:
            # wait for the function to come out of bootstrap state
            self.wait_for_bootstrap_to_complete(body['appname'])

    def undeploy_and_delete_function(self, body):
        self.undeploy_function(body)
        self.delete_function(body)

    def undeploy_function(self, body):
        body['settings']['deployment_status'] = False
        body['settings']['processing_status'] = False
        # save the function so that it disappears from UI
        content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Undeploy Application : {0}".format(content1))
        return content, content1

    def delete_function(self, body):
        # delete the function from the UI and backend
        content = self.rest.delete_function_from_temp_store(body['appname'])
        content1 = self.rest.delete_function(body['appname'])
        log.info("Delete Application : {0}".format(body['appname']))
        return content, content1

    def pause_function(self, body):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = False
        # save the function so that it is visible in UI
        content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Pause Application : {0}".format(content1))

    def resume_function(self, body):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        # save the function so that it is visible in UI
        content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Resume Application : {0}".format(content1))

    def refresh_rest_server(self):
        eventing_nodes_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        self.restServer = eventing_nodes_list[0]
        self.rest = RestConnection(self.restServer)
        return len(eventing_nodes_list)

    def check_if_eventing_consumers_are_cleaned_up(self):
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        array_of_counts = []
        command = "ps -ef | grep eventing-consumer | grep -v grep | wc -l"
        for eventing_node in eventing_nodes:
            shell = RemoteMachineShellConnection(eventing_node)
            count, error = shell.execute_non_sudo_command(command)
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            log.info("Node : {0} , eventing_consumer processes running : {1}".format(eventing_node.ip, count))
            array_of_counts.append(count)
        count_of_all_eventing_consumers = sum(array_of_counts)
        if count_of_all_eventing_consumers != 0:
            return False
        return True

    """
        Checks if a string 'panic' is present in eventing.log on server and returns the number of occurrences
    """

    def check_eventing_logs_for_panic(self):
        self.generate_map_nodes_out_dist()
        panic_str = "panic"
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        if not eventing_nodes:
            return None
        for eventing_node in eventing_nodes:
            shell = RemoteMachineShellConnection(eventing_node)
            _, dir_name = RestConnection(eventing_node).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
            eventing_log = str(dir_name) + '/eventing.log*'
            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                               format(panic_str, eventing_log))
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            if count > self.panic_count:
                log.info("===== PANIC OBSERVED IN EVENTING LOGS ON SERVER {0}=====".format(eventing_node.ip))
                panic_trace, _ = shell.execute_command("zgrep \"{0}\" {1}".
                                                       format(panic_str, eventing_log))
                log.info("\n {0}".format(panic_trace))
                self.panic_count = count
            shell.disconnect()

    def print_execution_and_failure_stats(self,name):
        out_event_execution = self.rest.get_event_execution_stats(name)
        log.info("Event execution stats : {0}".format(out_event_execution))
        out_event_failure = self.rest.get_event_failure_stats(name)
        log.info("Event failure stats : {0}".format(out_event_failure))

    """
        Push the bucket into DGM and return the number of items it took to push the bucket to DGM
    """
    def push_to_dgm(self, bucket, dgm_percent):
        doc_size = 1024
        curr_active = self.bucket_stat('vb_active_perc_mem_resident', bucket)
        total_items = self.bucket_stat('curr_items', bucket)
        batch_items = 20000
        # go into dgm
        while curr_active > dgm_percent:
            curr_items = self.bucket_stat('curr_items', bucket)
            gen_create = BlobGenerator('dgmkv', 'dgmkv-', doc_size, start=curr_items + 1, end=curr_items + 20000)
            total_items += batch_items
            try:
                self.cluster.load_gen_docs(self.master, bucket, gen_create, self.buckets[0].kvs[1],
                                           'create', exp=0, flag=0, batch_size=1000)
            except:
                pass
            curr_active = self.bucket_stat('vb_active_perc_mem_resident', bucket)
        log.info("bucket {0} in DGM, resident_ratio : {1}%".format(bucket, curr_active))
        total_items = self.bucket_stat('curr_items', bucket)
        return total_items

    def bucket_stat(self, key, bucket):
        stats = StatsCommon.get_stats([self.master], bucket, "", key)
        val = stats.values()[0]
        if val.isdigit():
            val = int(val)
        return val

    def bucket_compaction(self):
        for bucket in self.buckets:
            log.info("Compacting bucket : {0}".format(bucket.name))
            self.rest.compact_bucket(bucket=bucket.name)

    def verify_user_noroles(self,username):
        status, content, header=rbacmain(self.master)._retrieve_user_roles()
        res = json.loads(content)
        userExist=False
        for ele in res:
            log.debug("user {0}".format(ele["name"]))
            log.debug(ele["name"] == username)
            if ele["name"] == username:
                log.debug("user roles {0}".format(ele["roles"]))
                if not ele["roles"]:
                    log.info("user {0} has no roles".format(username))
                    userExist=True
                    break
        if not userExist:
            raise Exception("user {0} roles are not empty".format(username))

    def kill_consumer(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.kill_eventing_process(name="eventing-consumer")
        remote_client.disconnect()

    def kill_producer(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.kill_eventing_process(name="eventing-producer")
        remote_client.disconnect()

    def kill_memcached_service(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.kill_memcached()
        remote_client.disconnect()

    def kill_erlang_service(self, server):
        remote_client = RemoteMachineShellConnection(server)
        os_info = remote_client.extract_remote_info()
        remote_client.kill_erlang(os_info)
        remote_client.start_couchbase()
        remote_client.disconnect()
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 2)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([server], self, wait_if_warmup=True)

    def reboot_server(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.reboot_node()
        remote_client.disconnect()
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 5)
        # disable firewall on these nodes
        self.stop_firewall_on_node(server)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([server], self, wait_if_warmup=True)

    def undeploy_delete_all_functions(self):
        content=self.rest.get_deployed_eventing_apps()
        res = content.keys()
        log.info("all keys {}".format(res))
        for a in res:
            self.rest.undeploy_function(a)
        self.sleep(30)
        self.rest.delete_all_function()

    def change_time_zone(self,server,timezone="UTC"):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.execute_command("timedatectl set-timezone "+timezone)
        remote_client.disconnect()