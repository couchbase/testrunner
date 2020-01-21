import json
import logging
import random
import datetime
import os, sys
import socket

from TestInput import TestInputSingleton
from couchbase_helper.tuq_helper import N1QLHelper
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_generators import JsonGenerator
from lib.membase.api.rest_client import RestConnection
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.basetestcase import BaseTestCase
from testconstants import INDEX_QUOTA, MIN_KV_QUOTA, EVENTING_QUOTA
from pytests.query_tests_helper import QueryHelperTests

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
        self.rest.set_indexer_storage_mode()
        self.log.info(
            "Setting the min possible memory quota so that adding mode nodes to the cluster wouldn't be a problem.")
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=330)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
        # self.rest.set_service_memoryQuota(service='eventingMemoryQuota', memoryQuota=EVENTING_QUOTA)
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.create_functions_buckets = self.input.param('create_functions_buckets', True)
        self.docs_per_day = self.input.param("doc-per-day", 1)
        self.use_memory_manager = self.input.param('use_memory_manager', True)
        self.print_eventing_handler_code_in_logs = self.input.param('print_eventing_handler_code_in_logs', True)
        random.seed(datetime.time)
        function_name = "Function_{0}_{1}".format(random.randint(1, 1000000000), self._testMethodName)
        # See MB-28447, From now function name can only be max of 100 chars
        self.function_name = function_name[0:90]
        self.timer_storage_chan_size = self.input.param('timer_storage_chan_size', 10000)
        self.dcp_gen_chan_size = self.input.param('dcp_gen_chan_size', 10000)
        self.is_sbm=self.input.param('source_bucket_mutation', False)
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                      item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                      master=self.master, use_rest=True)
        self.pause_resume = self.input.param('pause_resume', False)
        self.pause_resume_number = self.input.param('pause_resume_number', 1)
        self.is_curl=self.input.param('curl', False)
        self.hostname = self.input.param('host', 'https://postman-echo.com/')
        self.curl_username = self.input.param('curl_user', None)
        self.curl_password = self.input.param('curl_password', None)
        self.auth_type = self.input.param('auth_type', 'no-auth')
        self.bearer_key=self.input.param('bearer_key', None)
        self.url = self.input.param('path', None)
        self.cookies = self.input.param('cookies', False)
        self.bearer_key = self.input.param('bearer_key', '')
        if self.hostname=='local':
            self.insall_dependencies()
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            self.hostname= "http://"+ip+":1080/"
            self.log.info("local ip address:{}".format(self.hostname))
            self.setup_curl()

    def tearDown(self):
        # catch panics and print it in the test log
        self.check_eventing_logs_for_panic()
        rest = RestConnection(self.master)
        buckets = rest.get_buckets()
        for bucket in buckets:
            stats = rest.get_bucket_stats(bucket)
            self.log.info("Bucket {} DGM is {}".format(bucket, stats["vb_active_resident_items_ratio"]))
        self.hostname = self.input.param('host', 'https://postman-echo.com/')
        if self.hostname == 'local':
            self.teardown_curl()
        super(EventingBaseTest, self).tearDown()

    def create_save_function_body(self, appname, appcode, description="Sample Description",
                                  checkpoint_interval=20000, cleanup_timers=False,
                                  dcp_stream_boundary="everything", deployment_status=True,
                                  # rbacpass="password", rbacrole="admin", rbacuser="cbadminbucket",
                                  skip_timer_threshold=86400,
                                  sock_batch_size=1, tick_duration=5000, timer_processing_tick_interval=500,
                                  timer_worker_pool_size=3, worker_count=3, processing_status=True,
                                  cpp_worker_thread_count=1, multi_dst_bucket=False, execution_timeout=60,
                                  data_chan_size=10000, worker_queue_cap=100000, deadline_timeout=62):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name,"access": "rw"})
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
        body['settings']['log_level'] = self.eventing_log_level
        # See MB-26756, reason for commenting out these lines
        # body['settings']['rbacpass'] = rbacpass
        # body['settings']['rbacrole'] = rbacrole
        # body['settings']['rbacuser'] = rbacuser
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        body['settings']['execution_timeout'] = execution_timeout
        body['settings']['data_chan_size'] = data_chan_size
        body['settings']['worker_queue_cap'] = worker_queue_cap
        # See MB-27967, the reason for adding this config
        body['settings']['use_memory_manager'] = self.use_memory_manager
        # since deadline_timeout has to always greater than execution_timeout
        if execution_timeout != 3:
            deadline_timeout = execution_timeout + 1
        body['settings']['deadline_timeout'] = deadline_timeout
        body['settings']['timer_storage_chan_size'] = self.timer_storage_chan_size
        body['settings']['dcp_gen_chan_size'] = self.dcp_gen_chan_size
        if self.is_sbm:
            del body['depcfg']['buckets'][0]
            body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,"access": "rw"})
        body['depcfg']['curl'] = []
        if self.is_curl:
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,
                                           "allow_cookies": self.cookies})
            if self.auth_type=="bearer":
                body['depcfg']['curl'][0]['bearer_key']=self.bearer_key
        return body

    def wait_for_bootstrap_to_complete(self, name, iterations=20):
        result = self.rest.get_deployed_eventing_apps()
        count = 0
        while name not in result and count < iterations:
            self.sleep(30, message="Waiting for eventing node to come out of bootstrap state...")
            count += 1
            result = self.rest.get_deployed_eventing_apps()
        if count == iterations:
            raise Exception(
                'Eventing took lot of time to come out of bootstrap state or did not successfully bootstrap')

    def wait_for_undeployment(self, name, iterations=20):
        self.sleep(30, message="Waiting for undeployment of function...")
        result = self.rest.get_running_eventing_apps()
        count = 0
        while name in result and count < iterations:
            self.sleep(30, message="Waiting for undeployment of function...")
            count += 1
            result = self.rest.get_running_eventing_apps()
        if count == iterations:
            raise Exception('Eventing took lot of time to undeploy')

    def verify_eventing_results(self, name, expected_dcp_mutations, doc_timer_events=False, on_delete=False,
                                skip_stats_validation=False, bucket=None, timeout=600):
        # This resets the rest server as the previously used rest server might be out of cluster due to rebalance
        num_nodes = self.refresh_rest_server()
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        if bucket is None:
            bucket=self.dst_bucket_name
        if self.is_sbm:
            bucket=self.src_bucket_name
        if not skip_stats_validation:
            # we can't rely on dcp_mutation stats when doc timers events are set.
            # TODO : add this back when getEventProcessingStats works reliably for doc timer events as well
            if not doc_timer_events:
                count = 0
                if num_nodes <= 1:
                    stats = self.rest.get_event_processing_stats(name)
                else:
                    stats = self.rest.get_aggregate_event_processing_stats(name)
                if on_delete:
                    mutation_type = "dcp_deletion"
                else:
                    mutation_type = "dcp_mutation"
                actual_dcp_mutations = stats[mutation_type]
                # This is required when binary data is involved where dcp_mutation will have process DCP_MUTATIONS
                # but ignore it
                # wait for eventing node to process dcp mutations
                log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
                while actual_dcp_mutations != expected_dcp_mutations and count < 20:
                    self.sleep(timeout//20, message="Waiting for eventing to process all dcp mutations...")
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
        stats_dst = self.rest.get_bucket_stats(bucket)
        while stats_dst["curr_items"] != expected_dcp_mutations and count < 20:
            message = "Waiting for handler code {2} to complete bucket operations... Current : {0} Expected : {1}".\
                      format(stats_dst["curr_items"], expected_dcp_mutations, name)
            self.sleep(timeout//20, message=message)
            curr_items=stats_dst["curr_items"]
            stats_dst = self.rest.get_bucket_stats(bucket)
            if curr_items == stats_dst["curr_items"]:
                count += 1
        if stats_dst["curr_items"] != expected_dcp_mutations:
            total_dcp_backlog = 0
            timers_in_past = 0
            lcb = {}
            # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
            for eventing_node in eventing_nodes:
                rest_conn = RestConnection(eventing_node)
                out = rest_conn.get_all_eventing_stats()
                total_dcp_backlog += out[0]["events_remaining"]["dcp_backlog"]
                if "TIMERS_IN_PAST" in out[0]["event_processing_stats"]:
                    timers_in_past += out[0]["event_processing_stats"]["TIMERS_IN_PAST"]
                total_lcb_exceptions= out[0]["lcb_exception_stats"]
                host=eventing_node.ip
                lcb[host]=total_lcb_exceptions
                full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
                log.info("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                          indent=4)))
                log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(full_out,
                                                                                                sort_keys=True,
                                                                                                indent=4)))
            raise Exception(
                "Bucket operations from handler code took lot of time to complete or didn't go through. Current : {0} "
                "Expected : {1}  dcp_backlog : {2}  TIMERS_IN_PAST : {3} lcb_exceptions : {4}".format(stats_dst["curr_items"],
                                                                                 expected_dcp_mutations,
                                                                                 total_dcp_backlog,
                                                                                 timers_in_past, lcb))
        log.info("Final docs count... Current : {0} Expected : {1}".
                 format(stats_dst["curr_items"], expected_dcp_mutations))
        # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
        # print all stats from all eventing nodes
        # These are the stats that will be used by ns_server and UI
        for eventing_node in eventing_nodes:
            rest_conn = RestConnection(eventing_node)
            out = rest_conn.get_all_eventing_stats()
            full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
            log.info("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                      indent=4)))
            log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(full_out, sort_keys=True,
                                                                                            indent=4)))

    def eventing_stats(self):
        self.sleep(30)
        content=self.rest.get_all_eventing_stats()
        js=json.loads(content)
        log.info("execution stats: {0}".format(js))
        # for j in js:
        #     print j["function_name"]
        #     print j["execution_stats"]["on_update_success"]
        #     print j["failure_stats"]["n1ql_op_exception_count"]

    def deploy_function(self, body, deployment_fail=False, wait_for_bootstrap=True,pause_resume=False,pause_resume_number=1):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if self.print_eventing_handler_code_in_logs:
            log.info("Deploying the following handler code : {0} with {1}".format(body['appname'], body['depcfg']))
            log.info("\n{0}".format(body['appcode']))
        content1 = self.rest.create_function(body['appname'], body)
        log.info("deploy Application : {0}".format(content1))
        if deployment_fail:
            res = json.loads(content1)
            if not res["compile_success"]:
                return
            else:
                raise Exception("Deployment is expected to be failed but no message of failure")
        if wait_for_bootstrap:
            # wait for the function to come out of bootstrap state
            self.wait_for_handler_state(body['appname'], "deployed")
        if pause_resume and pause_resume_number > 0:
            self.pause_resume_n(body, pause_resume_number)


    def undeploy_and_delete_function(self, body):
        self.undeploy_function(body)
        self.sleep(5)
        self.delete_function(body)

    def undeploy_function(self, body):
        self.refresh_rest_server()
        content = self.rest.undeploy_function(body['appname'])
        log.info("Undeploy Application : {0}".format(body['appname']))
        self.wait_for_handler_state(body['appname'], "undeployed")
        return content

    def delete_function(self, body):
        content1 = self.rest.delete_single_function(body['appname'])
        log.info("Delete Application : {0}".format(body['appname']))
        return content1

    def pause_function(self, body,wait_for_pause=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = False
        self.refresh_rest_server()
        # save the function so that it is visible in UI
        #content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Pause Application : {0}".format(body['appname']))
        if wait_for_pause:
            self.wait_for_handler_state(body['appname'], "paused")

    def resume_function(self, body,wait_for_resume=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if "dcp_stream_boundary" in body['settings']:
            body['settings'].pop('dcp_stream_boundary')
        log.info("Settings after deleting dcp_stream_boundary : {0}".format(body['settings']))
        self.refresh_rest_server()
        #body['settings']['dcp_stream_boundary'] = "from_prior"
        # save the function so that it is visible in UI
        #content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Resume Application : {0}".format(body['appname']))
        if wait_for_resume:
            self.wait_for_handler_state(body['appname'], "deployed")

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
            os_info = shell.extract_remote_info()
            if os_info.type.lower() == "windows":
                # This is a fixed path in all windows systems inside couchbase
                dir_name_crash = 'c://CrashDumps'
            else:
                dir_name_crash = str(dir_name) + '/../crash/'
            core_dump_count, err = shell.execute_command("ls {0}| wc -l".format(dir_name_crash))
            if isinstance(core_dump_count, list):
                core_dump_count = int(core_dump_count[0])
            else:
                core_dump_count = int(core_dump_count)
            if core_dump_count > 0:
                log.info("===== CORE DUMPS SEEN ON EVENTING NODES, SERVER {0} : {1} crashes seen =====".format(
                         eventing_node.ip, core_dump_count))
            shell.disconnect()

    def print_execution_and_failure_stats(self, name):
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
                                           'create', exp=0, flag=0, batch_size=1000, compression=self.sdk_compression)
            except:
                pass
            curr_active = self.bucket_stat('vb_active_perc_mem_resident', bucket)
        log.info("bucket {0} in DGM, resident_ratio : {1}%".format(bucket, curr_active))
        total_items = self.bucket_stat('curr_items', bucket)
        return total_items

    def bucket_stat(self, key, bucket):
        stats = StatsCommon.get_stats([self.master], bucket, "", key)
        val = list(stats.values())[0]
        if val.isdigit():
            val = int(val)
        return val

    def bucket_compaction(self):
        for bucket in self.buckets:
            log.info("Compacting bucket : {0}".format(bucket.name))
            self.rest.compact_bucket(bucket=bucket.name)

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
        log.info("os_info : {0}".format(os_info))
        if os_info.type.lower() == "windows":
            remote_client.kill_erlang(os="windows")
        else:
            remote_client.kill_erlang()
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
        res = list(content.keys())
        log.info("all keys {}".format(res))
        for a in res:
            self.rest.undeploy_function(a)
        for a in res:
            self.wait_for_handler_state(a, "undeployed")
        self.rest.delete_all_function()

    def change_time_zone(self,server,timezone="UTC"):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.execute_command("timedatectl set-timezone "+timezone)
        remote_client.disconnect()

    def cleanup_eventing(self):
        ev_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        ev_rest = RestConnection(ev_node)
        log.info("Running eventing cleanup api...")
        ev_rest.cleanup_eventing()

    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=(2016 * docs_per_day), start=start, value_size=document_size)

    def print_eventing_stats_from_all_eventing_nodes(self):
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        for eventing_node in eventing_nodes:
            rest_conn = RestConnection(eventing_node)
            out = rest_conn.get_all_eventing_stats()
            log.info("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                      indent=4)))

    def print_go_routine_dump_from_all_eventing_nodes(self):
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        for eventing_node in eventing_nodes:
            rest_conn = RestConnection(eventing_node)
            out = rest_conn.get_eventing_go_routine_dumps()
            log.info("Go routine dumps for Node {0} is \n{1} ======================================================"
                     "============================================================================================="
                     "\n\n".format(eventing_node.ip, out))

    def verify_source_bucket_mutation(self,doc_count,deletes=False,timeout=600,bucket=None):
        if bucket == None:
            bucket=self.src_bucket_name
        # query = "create primary index on {}".format(self.src_bucket_name)
        # self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        num_nodes = self.refresh_rest_server()
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        count=0
        result=0
        while count <= 20 and doc_count != result:
            self.sleep(timeout // 20, message="Waiting for eventing to process all dcp mutations...")
            if deletes:
                    query="select raw(count(*)) from {} where doc_deleted = 1".format(bucket)
            else:
                query="select raw(count(*)) from {} where updated_field = 1".format(bucket)
            result_set=self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
            result=result_set["results"][0]
            if deletes:
                self.log.info("deleted docs:{}  expected doc: {}".format(result, doc_count))
            else:
                self.log.info("updated docs:{}  expected doc: {}".format(result, doc_count))
            count=count+1

        if count > 20 and doc_count != result:
            total_dcp_backlog = 0
            timers_in_past = 0
            lcb = {}
            # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
            for eventing_node in eventing_nodes:
                rest_conn = RestConnection(eventing_node)
                out = rest_conn.get_all_eventing_stats()
                total_dcp_backlog += out[0]["events_remaining"]["dcp_backlog"]
                if "TIMERS_IN_PAST" in out[0]["event_processing_stats"]:
                    timers_in_past += out[0]["event_processing_stats"]["TIMERS_IN_PAST"]
                total_lcb_exceptions = out[0]["lcb_exception_stats"]
                host = eventing_node.ip
                lcb[host] = total_lcb_exceptions
                full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
                log.info(
                    "Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True, indent=4)))
                log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip,
                                                                     json.dumps(full_out, sort_keys=True, indent=4)))
            raise Exception("Eventing has not processed all the mutation in expected time, docs:{}  expected doc: {}".format(result, doc_count))

    def pause_resume_n(self, body, num):
        for i in range(num):
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)


    def wait_for_handler_state(self, name,status,iterations=20):
        self.sleep(20, message="Waiting for {} to {}...".format(name, status))
        result = self.rest.get_composite_eventing_status()
        count = 0
        composite_status = None
        while composite_status != status and count < iterations:
            self.sleep(20, "Waiting for {} to {}...".format(name, status))
            result = self.rest.get_composite_eventing_status()
            for i in range(len(result['apps'])):
                if result['apps'][i]['name'] == name:
                    composite_status = result['apps'][i]['composite_status']
            count+=1
        if count == iterations:
            raise Exception('Eventing took lot of time for handler {} to {}'.format(name, status))

    def setup_curl(self,):
        o=os.system('python scripts/curl_setup.py start')
        self.log.info("=== started docker container =======".format(o))
        self.sleep(10)
        o=os.system('python scripts/curl_setup.py setup')
        self.log.info("=== setup done =======")
        self.log.info(o)

    def teardown_curl(self):
        o = os.system('python scripts/curl_setup.py stop')
        self.log.info("=== stopping docker container =======")

    def insall_dependencies(self):
        try:
            import docker
        except ImportError as e:
            o = os.system("python scripts/install_docker.py docker")
            self.log.info("docker installation done: {}".format(o))
            self.sleep(30)
            try:
                import docker
            except ImportError as e:
                raise Exception("docker installation fails with {}".format(o))
