import queue
import copy
import threading
from random import randint
from couchbase_helper.tuq_helper import N1QLHelper
from pytests.eventing.eventing_helper import EventingHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from .newupgradebasetest import NewUpgradeBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from testconstants import FUTURE_BUILD_NUMBER
from pytests.fts.fts_callable import FTSCallable

class UpgradeTests(NewUpgradeBaseTest):

    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.queue = queue.Queue()
        self.run_partition_validation = self.input.param("run_partition_validation", False)
        self.isRebalanceComplete = False
        self.graceful = self.input.param("graceful", False)
        self.vector_upgrade = self.input.param("vector_upgrade",False)
        self.after_upgrade_nodes_in = self.input.param("after_upgrade_nodes_in", 1)
        self.after_upgrade_nodes_out = self.input.param("after_upgrade_nodes_out", 1)
        self.verify_vbucket_info = self.input.param("verify_vbucket_info", True)
        self.initialize_events = self.input.param("initialize_events", "").split(":")
        self.upgrade_services_in = self.input.param("upgrade_services_in", None)
        self.after_upgrade_services_in = \
            self.input.param("after_upgrade_services_in", None)
        self.after_upgrade_services_out_dist = \
            self.input.param("after_upgrade_services_out_dist", None)
        self.in_between_events = self.input.param("in_between_events", "").split(":")
        self.after_events = self.input.param("after_events", "").split(":")
        self.before_events = self.input.param("before_events", "").split(":")
        self.upgrade_type = self.input.param("upgrade_type", "online")
        self.sherlock_upgrade = self.input.param("sherlock", False)
        self.max_verify = self.input.param("max_verify", None)
        self.verify_after_events = self.input.param("verify_after_events", True)
        self.online_upgrade_type = self.input.param("online_upgrade_type", "swap")
        self.offline_upgrade_type = self.input.param("offline_upgrade_type", "offline_shutdown")
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.source_bucket_mutation_name = self.input.param('source_bucket_mutation_name', 'source_bucket_mutation')
        self.dst_bucket_curl_name = self.input.param('dst_bucket_curl_name', 'dst_bucket_curl')
        self.create_functions_buckets = self.input.param('create_functions_buckets', True)
        self.use_memory_manager = self.input.param('use_memory_manager', True)
        self.test_upgrade_with_xdcr = self.input.param('xdcr', False)
        self.final_events = []
        self.n1ql_helper = None
        self.total_buckets = 1
        self.fts_port = 8094
        self.in_servers_pool = self._convert_server_map(self.servers[:self.nodes_init])
        """ Init nodes to not upgrade yet """
        for key in list(self.in_servers_pool.keys()):
            self.in_servers_pool[key].upgraded = False
        self.out_servers_pool = self._convert_server_map(self.servers[self.nodes_init:])
        self.gen_initial_create = BlobGenerator('upgrade', 'upgrade',
                                                self.value_size,
                                                end=self.num_items)
        self.gen_create = BlobGenerator('upgrade', 'upgrade', self.value_size,
                                        start=self.num_items + 1,
                                        end=self.num_items * 1.5)
        self.gen_update = BlobGenerator('upgrade', 'upgrade', self.value_size,
                                        start=self.num_items // 2,
                                        end=self.num_items)
        self.gen_delete = BlobGenerator('upgrade', 'upgrade', self.value_size,
                                        start=self.num_items // 4,
                                        end=self.num_items // 2 - 1)
        self.after_gen_create = BlobGenerator('upgrade', 'upgrade',
                                              self.value_size,
                                              start=self.num_items * 1.6,
                                              end=self.num_items * 2)
        self.after_gen_update = BlobGenerator('upgrade', 'upgrade',
                                              self.value_size, start=1,
                                              end=self.num_items/4)
        self.after_gen_delete = BlobGenerator('upgrade', 'upgrade',
                                              self.value_size,
                                              start=self.num_items * .5,
                                              end=self.num_items * 0.75)
        initial_services_setting = self.input.param("initial-services-setting", None)
        if initial_services_setting is not None and initial_services_setting.count("kv") < 2:
            raise Exception("This test need at least 2 kv nodes to run")
        """ Install original cb server """
        self._install(self.servers[:self.nodes_init])
        if not self.init_nodes and initial_services_setting is not None:
            if "-" in initial_services_setting:
                self.multi_nodes_services = True
                initial_services = initial_services_setting.split("-")[0]
            else:
                initial_services = initial_services_setting
            self.initialize_nodes([self.servers[:self.nodes_init][0]],
                                  services=initial_services)
        RestConnection(self.master).set_indexer_storage_mode()
        self._log_start(self)
        if len(self.servers[:self.nodes_init]) > 1:
            if initial_services_setting is None:
                self.cluster.rebalance(self.servers[:1],
                                       self.servers[1:self.nodes_init],
                                       [],
                                       use_hostnames=self.use_hostnames)
            else:
                set_services = self.initial_services(initial_services_setting)
                for i in range(1, len(set_services)):
                    self.cluster.rebalance([self.servers[0]],
                                           [self.servers[i]],
                                           [],
                                           use_hostnames=self.use_hostnames,
                                           services=[set_services[i]])
                    self.sleep(10)
        else:
            self.cluster.rebalance([self.servers[0]], self.servers[1:], [],
                                   use_hostnames=self.use_hostnames)
        self.sleep(5)
        """ sometimes, when upgrade failed and node does not install couchbase
            server yet, we could not set quota at beginning of the test.  We
            have to wait to install new couchbase server to set it properly here """
        servers_available = copy.deepcopy(self.servers)
        if len(self.servers) > int(self.nodes_init):
            servers_available = servers_available[:self.nodes_init]
        self.quota = self._initialize_nodes(
            self.cluster, servers_available, self.disabled_consistent_view,
            self.rebalanceIndexWaitingDisabled,
            self.rebalanceIndexPausingDisabled, self.maxParallelIndexers,
            self.maxParallelReplicaIndexers, self.port)
        self.add_built_in_server_user(node=self.master)
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        if self.travel_sample_bucket:
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("""curl -g -v -u Administrator:password \
                         -X POST http://{0}:8091/sampleBuckets/install \
                         -d  '["travel-sample"]'""".format(self.master.ip))
            shell.disconnect()

            buckets = RestConnection(self.master).get_buckets()
            for bucket in buckets:
                if bucket.name != "travel-sample":
                    self.fail("travel-sample bucket did not create")
            self.sleep(60, "time needs to load data to travel- sample")
            if self.dgm_run:
                self.create_buckets()
        else:
            self.create_buckets()
        self.n1ql_server = None
        self.success_run = True
        self.failed_thread = None
        self.generate_map_nodes_out_dist_upgrade(self.after_upgrade_services_out_dist)
        if self.upgrade_services_in != "same":
            self.upgrade_services_in = self.get_services(list(self.in_servers_pool.values()),
                                          self.upgrade_services_in, start_node = 0)
        self.after_upgrade_services_in = self.get_services(list(self.out_servers_pool.values()),
                                           self.after_upgrade_services_in, start_node = 0)
        self.fts_obj = None
        self.index_name_prefix = None
        if self.test_upgrade_with_xdcr:
            from pytests.xdcr.xdcr_callable import XDCRCallable
            # Setup XDCR src and target clusters
            self.xdcr_handle = XDCRCallable(self.servers[:self.nodes_init])

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    """
       This test_upgrade is written to upgrade from 5.x.x to 6.5.x
       This test_upgrade function could run with many differnt test cases.  All you need is params.
       params:
         **** Must include when run test_upgrade in job config or in conf file ****
         upgrade_test=True             (this param must include to run this test_upgrade)
         skip_init_check_cbserver=true (this param will by pass check ns_server inside node)

         *** these params could change its value ***
         items=10000                   (can any number)
         initial_version=5.5.0-2958    (original cb version in cluster.
                                        Must be in format x.x.x-xxxx )
         released_upgrade_version=6.5.0-3265 (upgrade cluster to Mad-Hatter.
                                        Must be in format x.x.x-xxxx )
         nodes_init=3                  (number of node cluster will form)
         upgrade_type=offline          (if this param not pass, default value is online.
                                        If value is offline, default value of
                                        offline_upgrade_type is normal offline upgrade)
         offline_upgrade_type=offline_failover (this param is used with upgrade_type=offline
                                                if do offline failover, it needs to pass
                                                offline_upgrade_type=offline_failover)
         initialize_events=event_before_upgrade    (it must be separated with dash like
                                        kv_ops_initialize-create_fts_index_query_compare.
                                        Function called must be in underscore format)
         initial-services-setting=kv,index-kv,n1ql,fts-kv,eventing,index,n1ql
                                       Services for each node is separated with dash.
                                       Remember, no space around comma
                                       In example above, node 1 with services kv,index
                                                         node 2 with services kv,n1ql,fts
                                                         node 3 with services kv,eventing,index
         init_nodes=False  (default value is true and will get service from ini file, disable
                            initial-services-setting param above)
         upgrade_services_in=same    (if not pass this param, it will get services in ini file)
         after_events=rebalance_in-run_fts_query_and_compare   (event must separate with dash)
         after_upgrade_services_in=kv,fts    (this param will pass services to rebalance_in a
                                              node above.  If add 2 nodes in, it needs 2
                                              services separated by dash.  Otherwise, it will
                                              get service from ini file)

      Here is example of an offline failover upgrade test with fts
        -t upgrade.upgrade_tests.UpgradeTests.test_upgrade,items=5000,initial_version=5.5.0-2958,
           nodes_init=3,initialize_events=kv_ops_initialize-create_fts_index_query_compare,
           initial-services-setting=kv,index-kv,n1ql,fts-kv,eventing,index,n1ql,
           upgrade_services_in=same,after_events=rebalance_in-run_fts_query_and_compare,
           after_upgrade_services_in=kv,fts,disable_HTP=True,upgrade_test=True,init_nodes=False,
           skip_init_check_cbserver=true,released_upgrade_version=6.5.0-3265,dgm_run=true,
           doc-per-day=1,upgrade_type=offline,offline_upgrade_type=offline_failover
    """

    def load_items_during_rebalance(self,server):
        self.sleep(15)
        self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False)
        while(not self.isRebalanceComplete):
            if self.vector_upgrade:
                """delete 20% of docs"""
                status = self.fts_obj.delete_doc_by_key(server,10000001,10005001,0.2)

                if not status:
                    self.fail("CRUD operation failed during rebalance. OPS : DELETE")
                else:
                    self.log.info("CRUD operation successful during rebalance. OPS : DELETE")

                """upsert (update) the first 1000 docs out of 5000 docs present in the cluster"""
                self.fts_obj.load_data(1000)

                """upserting the vector data"""
                try:
                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password),xattr=True,base64Flag=True, end_index=10001001)

                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password), xattr=True, base64Flag=False, end_index=10001001)

                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password), xattr=False, base64Flag=False, end_index=10001001)

                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password), xattr=False, base64Flag=True, end_index=10001001)

                except Exception as e:
                    self.log.info(e)
            else:
                self.fts_obj.load_data(1000)
                status = self.fts_obj.delete_doc_by_key(server,10000001,10001000,0.2)

                if not status:
                    self.fail("CRUD operation failed during rebalance. OPS : DELETE")
                else:
                    self.log.info("CRUD operation successful during rebalance. OPS : DELETE")
        
        self.isRebalanceComplete = False


    def test_upgrade(self):
        self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False)
        self.event_threads = []
        self.after_event_threads = []
        try:
            self.log.info("\n*** Start init operations before upgrade begins ***")
            if self.initialize_events:
                initialize_events = self.run_event(self.initialize_events)
            self.finish_events(initialize_events)
            if not self.success_run and self.failed_thread is not None:
                raise Exception("*** Failed to {0} ***".format(self.failed_thread))
            else:
                if self.run_partition_validation:
                    try:
                        self.partition_validation(skip_check=False)
                    except Exception as ex:
                        self.fail(ex)
            self.cluster_stats(self.servers[:self.nodes_init])
            if self.before_events:
                self.event_threads += self.run_event(self.before_events)

            self.log.info("\n*** Start upgrade cluster ***")
            self.event_threads += self.upgrade_event()
            self.finish_events(self.event_threads)

            self.log.info("\nWill install upgrade version to any free nodes")
            out_nodes = self._get_free_nodes()
            if out_nodes:
                self.log.info("Here is free nodes {0}".format(out_nodes))
                self.initial_version = self.upgrade_versions[0]
                if self.upgrade_to_future_build:
                    tmp = self.upgrade_versions[0].split("-")
                    tmp[1] = str(int(tmp[1]) + FUTURE_BUILD_NUMBER)
                    self.initial_version = "-".join(tmp)
                self._install(out_nodes)
            else:
                self.log.info("No free nodes")
            self.generate_map_nodes_out_dist_upgrade(
                self.after_upgrade_services_out_dist)

            self.log.info("\n\n*** Start operations after upgrade is done ***")
            self.add_built_in_server_user()
            if self.after_events:
                self.after_event_threads = self.run_event(self.after_events)
            self.finish_events(self.after_event_threads)
            if not self.success_run and self.failed_thread is not None:
                raise Exception("*** Failed to {0} ***".format(self.failed_thread))
            else:
                if self.run_partition_validation:
                    try:
                        self.partition_validation(skip_check=False)
                    except Exception as ex:
                        self.fail(ex)
            """ Default set to always verify data """
            if self.after_events[0]:
                self.log.info("*** Start after events ***")
                for event in self.after_events[0].split("-"):
                    if "delete_buckets" in event:
                        self.log.info("After events has delete buckets event. "
                                      "No items verification needed")
                        self.verify_after_events = False
                        break
            if self.verify_after_events:
                self.log.info("*** Start data verification ***")
                self.cluster_stats(list(self.in_servers_pool.values()))
                self._verify_data_active_replica()
        except Exception as ex:
            self.log.info(ex)
            print("*** Stop all events to stop the test ***")
            self.stop_all_events(self.event_threads)
            self.stop_all_events(self.after_event_threads)
            raise
        finally:
            self.log.info("any events for which we need to cleanup")
            self.cleanup_events()

    def _record_vbuckets(self, master, servers):
        bucket_map = dict()
        for bucket in self.buckets:
            self.log.info("Record vbucket for the bucket {0}"
                          .format(bucket.name))
            bucket_map[bucket.name] = RestHelper(RestConnection(master))\
                ._get_vbuckets(servers, bucket_name=bucket.name)
        return bucket_map

    def _find_master(self):
        self.master = list(self.in_servers_pool.values())[0]

    def _verify_data_active_replica(self):
        """ set data_analysis True by default """
        self.data_analysis =  self.input.param("data_analysis", False)
        self.total_vbuckets =  self.initial_vbuckets
        if self.data_analysis:
            disk_replica_dataset, disk_active_dataset = \
                self.get_and_compare_active_replica_data_set_all(
                    list(self.in_servers_pool.values()),
                    self.buckets, path=None)
            self.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.get_nodes_in_cluster_after_upgrade(), self.buckets, path=None)
            """ check vbucket distribution analysis after rebalance """
            self.vb_distribution_analysis(
                servers=list(self.in_servers_pool.values()),
                buckets=self.buckets, std=1.0,
                total_vbuckets=self.total_vbuckets)

    def _verify_vbuckets(self, old_vbucket_map, new_vbucket_map):
        for bucket in self.buckets:
            self._verify_vbucket_nums_for_swap(old_vbucket_map[bucket.name],
                                               new_vbucket_map[bucket.name])

    def stop_all_events(self, thread_list):
        for t in thread_list:
            try:
                if t.isAlive():
                    t.stop()
            except Exception as ex:
                self.log.info(ex)

    def cleanup_events(self):
        thread_list = []
        for event in self.final_events:
            t = threading.Thread(target=self.find_function(event), args=())
            t.daemon = True
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()

    def run_event_in_sequence(self, events):
        q = self.queue
        self.log.info("run_event_in_sequence")
        for event in events.split("-"):
            t = threading.Thread(target=self.find_function(event), args=(q,))
            t.daemon = True
            t.start()
            t.join()
            self.success_run = True
            while not self.queue.empty():
                self.success_run &= self.queue.get()
            if not self.success_run:
                self.failed_thread = event
                break

    def run_event(self, events):
        thread_list = []
        for event in events:
            if "-" in event:
                t = threading.Thread(target=self.run_event_in_sequence, args=(event,))
                t.start()
                t.join()
            elif event != '':
                t = threading.Thread(target=self.find_function(event), args=())
                t.daemon = True
                t.start()
                thread_list.append(t)
        return thread_list

    def find_function(self, event):
        return getattr(self, event)

    def finish_events(self, thread_list):
        for t in thread_list:
            t.join()

    def upgrade_event(self):
        self.log.info("upgrade_event")
        self.start_upgrade_server = True
        thread_list = []
        if self.upgrade_type == "online":
            t = threading.Thread(target=self.online_upgrade, args=())
        elif self.upgrade_type == "offline":
            t = threading.Thread(target=self.offline_upgrade, args=())
        t.daemon = True
        t.start()
        thread_list.append(t)
        return thread_list

    def server_crash(self):
        try:
            self.log.info("server_crash")
            self.targetProcess = self.input.param("targetProcess", 'memcached')
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.terminate_process(process_name=self.targetProcess)
        except Exception as ex:
            self.log.info(ex)
            raise

    def server_stop(self):
        try:
            self.log.info("server_stop")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.stop_server()
            self.final_events.append("start_server")
        except Exception as ex:
            self.log.info(ex)
            raise

    def start_server(self):
        try:
            self.log.info("start_server")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.start_server()
        except Exception as ex:
            self.log.info(ex)
            raise

    def failover(self, queue=None):
        failover_node = False
        try:
            self.log.info("VVVVVV failover a node ")
            print("failover node   ", self.nodes_out_list)
            nodes = self.get_nodes_in_cluster_after_upgrade()
            failover_task = self.cluster.async_failover([self.master],
                        failover_nodes = self.nodes_out_list, graceful=self.graceful)
            failover_task.result()
            if self.graceful:
                """ Check if rebalance is still running """
                msg = "graceful failover failed for nodes"
                self.assertTrue(RestConnection(self.master).monitorRebalance(\
                                                     stop_if_loop=True), msg=msg)
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                       [], self.nodes_out_list)
                rebalance.result()
                failover_node = True
            else:
                msg = "Failed to failover a node"
                self.assertTrue(RestConnection(self.master).monitorRebalance(\
                                                     stop_if_loop=True), msg=msg)
                rebalance = self.cluster.async_rebalance(nodes, [],
                                                    self.nodes_out_list)
                rebalance.result()
                failover_node = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if failover_node and queue is not None:
            queue.put(True)

    def autofailover(self):
        try:
            self.log.info("autofailover")
            autofailover_timeout = 30
            status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
            self.assertTrue(status, 'failed to change autofailover_settings!')
            servr_out = self.nodes_out_list
            remote = RemoteMachineShellConnection(self.nodes_out_list[0])
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                       [], [self.nodes_out_list[0]])
            rebalance.result()
        except Exception as ex:
            self.log.info(ex)
            raise

    def network_partitioning(self):
        try:
            self.log.info("network_partitioning")
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
            self.final_events.append("undo_network_partitioning")
        except Exception as ex:
            self.log.info(ex)
            raise

    def undo_network_partitioning(self):
        try:
            self.log.info("remove_network_partitioning")
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)
        except Exception as ex:
            self.log.info(ex)
            raise

    def bucket_compaction(self):
        try:
            self.log.info("couchbase_bucket_compaction")
            compact_tasks = []
            for bucket in self.buckets:
                compact_tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
        except Exception as ex:
            self.log.info(ex)
            raise

    def warmup(self, queue=None):
        node_warmuped = False
        try:
            self.log.info("Start warmup operation")
            nodes = self.get_nodes_in_cluster_after_upgrade()
            for server in nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.start_server()
                remote.disconnect()
            ClusterOperationHelper.wait_for_ns_servers_or_assert(nodes, self)
            node_warmuped = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if node_warmuped and queue is not None:
            queue.put(True)

    def create_lww_bucket(self):
        self.time_synchronization='enabledWithOutDrift'
        bucket='default'
        print('time_sync {0}'.format(self.time_synchronization))

        helper = RestHelper(self.rest)
        if not helper.bucket_exists(bucket):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(
                self.servers)
            info = self.rest.get_nodes_self()
            self.rest.create_bucket(bucket=bucket,
                ramQuotaMB=512, timeSynchronization=self.time_synchronization)
            try:
                ready = BucketOperationHelper.wait_for_memcached(self.master,
                    bucket)
                self.assertTrue(ready, '', msg = '[ERROR] Expect bucket creation to not work.')
            finally:
                self.log.info("Success, created lww bucket")


    def bucket_flush(self, queue=None):
        bucket_flushed = False
        try:
            self.log.info("bucket_flush ops")
            self.rest =RestConnection(self.master)
            for bucket in self.buckets:
                self.rest.flush_bucket(bucket.name)
            bucket_flushed = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if bucket_flushed and queue is not None:
            queue.put(True)

    def delete_buckets(self, queue=None):
        bucket_deleted = False
        try:
            self.log.info("delete_buckets")
            self.rest = RestConnection(self.master)
            for bucket in self.buckets:
                self.log.info("delete bucket {0}".format(bucket.name))
                self.rest.delete_bucket(bucket.name)
            bucket_deleted = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if bucket_deleted and queue is not None:
            queue.put(True)

    def create_buckets(self, queue=None):
        bucket_created = False
        try:
            self.log.info("create_buckets")
            if self.dgm_run:
                self.bucket_size = 256
                self.default_bucket = False
                self.sasl_buckets = 1
                self.sasl_bucket_name = self.sasl_bucket_name + "_" \
                                            + str(self.total_buckets)
                sasl_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                           replicas=self.num_replicas, bucket_type=self.bucket_type)
                self.cluster.create_sasl_bucket(name=self.sasl_bucket_name, password=self.sasl_password, bucket_params=sasl_bucket_params)
            else:
                self.bucket_size = 512
                self.rest = RestConnection(self.master)
                self._bucket_creation()
            self.sleep(5, "sleep after create bucket")
            self.total_buckets +=1
            bucket_created = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if bucket_created and queue is not None:
            queue.put(True)

    def change_bucket_properties(self):
        try:
            self.rest = RestConnection(self.master)
            #Change Bucket Properties
            for bucket in self.buckets:
                self.rest.change_bucket_props(bucket, ramQuotaMB=None,\
                        replicaNumber=0,\
                    proxyPort=None, replicaIndex=None, flushEnabled=False)
        except Exception as ex:
            self.log.info(ex)
            raise

    def rebalance_in(self, queue=None):
        rebalance_in = False
        service_in = copy.deepcopy(self.after_upgrade_services_in)
        if service_in is None:
            service_in = ["kv"]
        free_nodes = self._convert_server_map(self._get_free_nodes())
        if not list(free_nodes.values()):
            raise Exception("No free node available to rebalance in")
        try:
            self.nodes_in_list =  list(self.out_servers_pool.values())[:self.nodes_in]
            if int(self.nodes_in) == 1:
                if len(list(free_nodes.keys())) > 1:
                    free_node_in = [list(free_nodes.values())[0]]
                    if len(self.after_upgrade_services_in) > 1:
                        service_in = [self.after_upgrade_services_in[0]]
                else:
                    free_node_in = list(free_nodes.values())
                self.log.info("<<<=== rebalance_in node {0} with services {1}"\
                                      .format(free_node_in, service_in[0]))
                rebalance = \
                       self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                                      free_node_in,
                                                         [], services = service_in)

            rebalance.result()
            self.in_servers_pool.update(free_nodes)
            rebalance_in = True
            if any("index" in services for services in service_in):
                self.log.info("Set storageMode to forestdb after add "
                         "index node {0} to cluster".format(list(free_nodes.keys())))
                RestConnection(list(free_nodes.values())[0]).set_indexer_storage_mode()
            if self.after_upgrade_services_in and \
                len(self.after_upgrade_services_in) > 1:
                self.log.info("remove service '{0}' from service list after "
                        "rebalance done ".format(self.after_upgrade_services_in[0]))
                self.after_upgrade_services_in.pop(0)
            self.sleep(10, "wait 10 seconds after rebalance")
            if free_node_in and free_node_in[0] not in self.servers:
                found_node = False
                for server in self.servers:
                    if free_node_in[0].ip in server.ip:
                        found_node = True
                        break
                if not found_node:
                    self.servers.append(free_node_in[0])
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if rebalance_in and queue is not None:
            queue.put(True)

    def rebalance_out(self, queue=None):
        rebalance_out = False
        try:
            self.log.info("=====>>>> rebalance_out node {0}"\
                              .format(self.nodes_out_list))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],\
                                                             [], self.nodes_out_list)
            rebalance.result()
            rebalance_out = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if rebalance_out and queue is not None:
            queue.put(True)

    def rebalance_in_out(self, queue=None):
        rebalance_in_out = False
        try:
            self.nodes_in_list =  list(self.out_servers_pool.values())[:self.nodes_in]
            self.log.info("<<<<<===== rebalance_in node {0}"\
                                                    .format(self.nodes_in_list))
            self.log.info("=====>>>>> rebalance_out node {0}"\
                                                   .format(self.nodes_out_list))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],\
                                            self.nodes_in_list, self.nodes_out_list,\
                                           services = self.after_upgrade_services_in)
            rebalance.result()
            rebalance_in_out = True
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if rebalance_in_out and queue is not None:
            queue.put(True)

    def incremental_backup(self):
        self.log.info("incremental_backup")

    def full_backup(self):
        self.log.info("full_backup")

    def cb_collect_info(self):
        try:
            self.log.info("cb_collect_info")
            log_file_name = "/tmp/sample.zip"
            output, error = self.shell.execute_cbcollect_info("%s" % (log_file_name))
        except Exception as ex:
            raise
        finally:
            self.log.info(ex)

    def create_index(self, queue=None):
        self.log.info("create_index")
        self.index_list = {}
        create_index = False
        self._initialize_n1ql_helper()
        try:
            self.n1ql_helper.create_primary_index(using_gsi = True,
                                               server = self.n1ql_server)
            #self.n1ql_helper.create_primary_index(using_gsi = False,
            #                                  server = self.n1ql_server)
            self.log.info("done create_index")
            create_index = True
        except Exception as e:
            self.log.info(e)
            if queue is not None:
                queue.put(False)
        if create_index and queue is not None:
            queue.put(True)

    def create_index_with_replica_and_query(self, queue=None):
        """ ,groups=simple,reset_services=True
        """
        self.log.info("Create index with replica and query")
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self._initialize_n1ql_helper()
        self.index_name_prefix = "random_index_" + str(randint(100000, 999999))
        create_index_query = "CREATE INDEX " + self.index_name_prefix + \
              " ON default(age) USING GSI WITH {{'num_replica': {0}}};"\
                                        .format(self.num_index_replicas)
        try:
            self.create_index()
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as e:
            self.log.info(e)
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([self.index_name_prefix],
                                                    index_map,
                                                    self.num_index_replicas)

    def verify_index_with_replica_and_query(self, queue=None):
        index_map = self.get_index_map()
        try:
            self.n1ql_helper.verify_replica_indexes([self.index_name_prefix],
                                                    index_map,
                                                    self.num_index_replicas)
        except Exception as e:
            self.log.info(e)
            if queue is not None:
                queue.put(False)

    def create_views(self, queue=None):
        self.log.info("*** create_views ***")
        """ default is 1 ddoc. Change number of ddoc by param ddocs_num=new_number
            default is 2 views. Change number of views by param
            view_per_ddoc=new_view_per_doc """
        try:
            self.create_ddocs_and_views(queue)
        except Exception as e:
            self.log.info(e)

    def query_views(self, queue=None):
        self.log.info("*** query_views ***")
        try:
           self.verify_all_queries(queue)
        except Exception as e:
            self.log.info(e)

    def drop_views(self):
        self.log.info("drop_views")

    def drop_index(self):
        self.log.info("drop_index")
        for bucket_name in list(self.index_list.keys()):
            query = "drop index {0} on {1} using gsi"\
                 .format(self.index_list[bucket_name], bucket_name)
            self.n1ql_helper.run_cbq_query(query, self.n1ql_server)

    def query_explain(self):
        self.log.info("query_explain")
        for bucket in self.buckets:
            query = "select count(*) from {0}".format(bucket.name)
            self.n1ql_helper.run_cbq_query(query, self.n1ql_server)
            query = "explain select count(*) from {0}".format(bucket.name)
            self.n1ql_helper.run_cbq_query(query, self.n1ql_server)
            query = "select count(*) from {0} where field_1 = 1".format(bucket.name)
            self.n1ql_helper.run_cbq_query(query, self.n1ql_server)
            query = "explain select count(*) from {0} where field_1 = 1".format(bucket.name)
            self.n1ql_helper.run_cbq_query(query, self.n1ql_server)

    def change_settings(self):
        try:
            status = True
            if "update_notifications" in self.input.test_params:
                status &= self.rest.update_notifications(str(self.input.param("update_notifications", 'true')).lower())
            if "autofailover_timeout" in self.input.test_params:
                status &= self.rest.update_autofailover_settings(True, self.input.param("autofailover_timeout", None))
            if "autofailover_alerts" in self.input.test_params:
                status &= self.rest.set_alerts_settings('couchbase@localhost', 'root@localhost', 'user', 'pwd')
            if "autocompaction" in self.input.test_params:
                tmp, _, _ = self.rest.set_auto_compaction(viewFragmntThresholdPercentage=
                                         self.input.param("autocompaction", 50))
                status &= tmp
            if not status:
                self.fail("some settings were not set correctly!")
        except Exception as ex:
            self.log.info(ex)
            raise

    def create_eventing_services(self, queue=None):
        """ Only work after cluster upgrade to 5.5.0 completely """
        try:
            rest = RestConnection(self.master)
            cb_version = rest.get_nodes_version()
            if 5.5 > float(cb_version[:3]):
                self.log.info("This eventing test is only for cb version 5.5 and later.")
                return

            bucket_params = self._create_bucket_params(server=self.master, size=128,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.source_bucket_mutation_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.dst_bucket_curl_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.expiry = 3

            self.restServer = self.get_nodes_from_services_map(service_type="eventing")
            """ must be self.rest to pass in deploy_function"""
            self.rest = RestConnection(self.restServer)
            self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)

            event = EventingHelper(servers=self.servers,master=self.master)
            event.deploy_bucket_op_function()
            event.verify_documents_in_destination_bucket('test_import_function_1', 1, 'dst_bucket')
            event.undeploy_bucket_op_function()
            event.deploy_curl_function()
            event.verify_documents_in_destination_bucket('bucket_op_curl', 1, 'dst_bucket_curl')
            event.undeploy_curl_function()
            event.deploy_sbm_function()
            event.verify_documents_in_destination_bucket('bucket_op_sbm', 1, 'source_bucket_mutation')
            event.undeploy_sbm_function()
        except Exception as e:
            self.log.info(e)

    def create_cbas_services(self, queue=None):
        """
           This test only need max 4 servers to run and only upgrade to vulcan and later
           Command to run:
            upgrade.upgrade_tests.UpgradeTests.test_upgrade,items=5000,initial_version=4.6.4-xxxx,
            nodes_init=3,initialize_events=kv_ops_initialize,upgrade_services_in='kv:index',
            after_events=rebalance_in-create_cbas_services,after_upgrade_services_in=cbas,
            dgm_run=true,upgrade_test=True,skip_init_check_cbserver=true,released_upgrade_version=5.5.0-xxx
        """
        try:
            self.validate_error = False
            rest = RestConnection(self.master)
            cb_version = rest.get_nodes_version()
            if 5.5 > float(cb_version[:3]):
                self.log.info("This analytic test is only for cb version 5.5 and later.")
                return
            self.log.info("Get cbas nodes in cluster")
            cbas_node = self.get_nodes_from_services_map(service_type="cbas")
            cbas_rest = RestConnection(self.servers[self.nodes_init])
            self.get_services_map()

            kv_nodes = copy.deepcopy(self.servers)
            kv_maps = [x.replace(":8091", "") for x in self.services_map["kv"]]
            self.log.info("Get kv node in cluster")
            for i, node in enumerate(kv_nodes):
                if node.ip not in kv_maps:
                    del kv_nodes[i]
            self.cbas_node = cbas_node
            items_travel_sample = 63182
            # Toy build or Greater than CC build
            if float(cb_version[:3]) == 0.0 or float(cb_version[:3]) >= 7.0:
                items_travel_sample = 63288
            self.load_sample_buckets(servers=kv_nodes, bucketName="travel-sample",
                                                  total_items=items_travel_sample,
                                                  rest=cbas_rest)
            # self.test_create_dataset_on_bucket()
        except Exception as e:
            self.log.info(e)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def online_upgrade(self):
        try:
            self.log.info("online_upgrade")
            self.initial_version = self.upgrade_versions[0]
            self.sleep(self.sleep_time,
                       "Pre-setup of old version is done. "
                       "Wait for online upgrade to {0} version"
                       .format(self.initial_version))
            self.product = 'couchbase-server'
            if self.online_upgrade_type == "swap":
                self.online_upgrade_swap_rebalance()
            else:
                self.online_upgrade_incremental()
        except Exception as ex:
            self.log.info(ex)
            raise
    
    def trigger_rebalance(self,servers, servers_in,servers_out,servicesNodeOut):
        try:
            if self.upgrade_services_in == "same":
                self.cluster.rebalance(list(servers.values()),
                                        list(servers_in.values()),
                                        list(servers_out.values()),
                                        services=[servicesNodeOut],
                                        sleep_before_rebalance=15)
            elif self.upgrade_services_in is not None \
                    and len(self.upgrade_services_in) > 0:
                tem_services = self.upgrade_services_in[
                                start_services_num:start_services_num
                                + len(list(servers_in.values()))]
                self.cluster.rebalance(list(servers.values()),
                                        list(servers_in.values()),
                                        list(servers_out.values()),
                                        services=tem_services,
                                        sleep_before_rebalance=15)
                start_services_num += len(list(servers_in.values()))
            else:
                self.cluster.rebalance(list(servers.values()),
                                        list(servers_in.values()),
                                        list(servers_out.values()),
                                        sleep_before_rebalance=15)
            
            self.isRebalanceComplete = True
        except BaseException as ex:
            self.fail(ex)


    def online_upgrade_swap_rebalance(self):
        self.log.info("online_upgrade_swap_rebalance")
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        servers = self._convert_server_map(self.servers[:self.nodes_init])
        out_servers = self._convert_server_map(self.servers[self.nodes_init:])
        self.swap_num_servers = min(self.swap_num_servers, len(out_servers))
        start_services_num = 0
        for i in range(self.nodes_init // self.swap_num_servers):
            servers_in = {}
            new_servers = copy.deepcopy(servers)
            servicesNodeOut = ""
            for key in list(out_servers.keys()):
                servers_in[key] = out_servers[key]
                out_servers[key].upgraded = True
                out_servers.pop(key)
                if len(servers_in) == self.swap_num_servers:
                    break
            servers_out = {}
            node_out = None
            new_servers.update(servers_in)
            for key in list(servers.keys()):
                if len(servers_out) == self.swap_num_servers:
                    break
                elif not servers[key].upgraded:
                    servers_out[key] = servers[key]
                    new_servers.pop(key)
            out_servers.update(servers_out)
            rest = RestConnection(list(servers.values())[0])
            self.log.info("****************************************".format(servers))
            self.log.info("cluster nodes = {0}".format(list(servers.values())))
            self.log.info("cluster service map = {0}".format(rest.get_nodes_services()))
            self.log.info("cluster version map = {0}".format(rest.get_nodes_version()))
            self.log.info("to include in cluster = {0}".format(list(servers_in.values())))
            self.log.info("to exclude from cluster = {0}".format(list(servers_out.values())))
            self.log.info("****************************************".format(servers))
            rest = RestConnection(list(servers_out.values())[0])
            servicesNodeOut = rest.get_nodes_services()
            servicesNodeOut = ",".join(servicesNodeOut[list(servers_out.keys())[0]] )
            self._install(list(servers_in.values()))
            self.sleep(10, "Wait for ns server is ready")
            old_vbucket_map = self._record_vbuckets(self.master, list(servers.values()))
            
            cluster_ips = []
            in_server_ips = []
            out_server_ips = []
            load_data_target_server = None

            for i in list(servers.values()):
                cluster_ips.append(i.ip)

            for i in list(servers_in.values()):
                in_server_ips.append(i.ip)
            
            for i in list(servers_out.values()):
                out_server_ips.append(i.ip)
            
            itr = 0
            for i in cluster_ips:
                if i not in in_server_ips and i not in out_server_ips:
                    load_data_target_server = servers[i + ':8091']
                    break
                itr+=1
            
            if not load_data_target_server:
                load_data_target_server = next(iter(servers_in.values()))

            try:
                thread1 = threading.Thread(target=self.load_items_during_rebalance, args=(load_data_target_server,))
                thread2 = threading.Thread(target=self.trigger_rebalance, args=(servers, servers_in,servers_out,servicesNodeOut,))
                
                thread1.start()
                thread2.start()

                thread1.join()
                thread2.join()
            except Exception as ex:
                self.log.info("Could not push data while rebalancing")
                self.log.info(ex)

            self.out_servers_pool = servers_out
            self.in_servers_pool = new_servers
            servers = new_servers
            self.servers = list(servers.values())
            self.master = self.servers[0]
            if self.verify_vbucket_info:
                new_vbucket_map = self._record_vbuckets(self.master, self.servers)
                self._verify_vbuckets(old_vbucket_map, new_vbucket_map)
            # in the middle of online upgrade events
            if self.in_between_events:
                self.event_threads = []
                self.event_threads += self.run_event(self.in_between_events)
                self.finish_events(self.event_threads)
                self.in_between_events = None

            if self.run_partition_validation:
                try:
                    self.partition_validation()
                except Exception as ex:
                    self.fail(ex)

    def online_upgrade_incremental(self):
        self.log.info("online_upgrade_incremental")
        try:
            for server in self.servers[1:]:
                self.cluster.rebalance(self.servers, [], [server])
                self.initial_version = self.upgrade_versions[0]
                self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version".\
                       format(self.initial_version))
                self.product = 'couchbase-server'
                self._install([server])
                self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
                self.cluster.rebalance(self.servers, [server], [])
                self.log.info("Rebalanced in upgraded nodes")
                self.sleep(self.sleep_time)

                try:
                    if self.in_between_events:
                        self.event_threads = []
                        self.event_threads += self.run_event(self.in_between_events)
                        self.finish_events(self.event_threads)
                        self.in_between_events = None
                except Exception as ex:
                    print(ex)

                if self.run_partition_validation:
                    try:
                        self.partition_validation()
                    except Exception as ex:
                        print(ex)

            self._new_master(self.servers[1])
            self.cluster.rebalance(self.servers, [], [self.servers[0]])
            self.log.info("Rebalanced out all old version nodes")
        except Exception as ex:
            self.log.info(ex)
            raise

    def offline_upgrade(self):
        if self.offline_upgrade_type == "offline_shutdown":
            self._offline_upgrade()
        elif self.offline_upgrade_type == "offline_failover":
            self.offline_fail_over_upgrade()

    def failover_add_back(self):
        try:
            rest = RestConnection(self.master)
            recoveryType = self.input.param("recoveryType", "full")
            servr_out = self.nodes_out_list
            failover_task =self.cluster.async_failover([self.master],
                    failover_nodes = servr_out, graceful=self.graceful)
            failover_task.result()
            nodes_all = rest.node_statuses()
            nodes = []
            if servr_out[0].ip == "127.0.0.1":
                for failover_node in servr_out:
                    nodes.extend([node for node in nodes_all
                        if (str(node.port) == failover_node.port)])
            else:
                for failover_node in servr_out:
                    nodes.extend([node for node in nodes_all
                        if node.ip == failover_node.ip])
            for node in nodes:
                self.log.info(node)
                rest.add_back_node(node.id)
                rest.set_recovery_type(otpNode=node.id, recoveryType=recoveryType)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                                             [], [])
            rebalance.result()
        except Exception as ex:
            raise

    def auto_retry_with_rebalance_in(self, queue=None):
        self.change_retry_rebalance_settings(True, 300, 1)
        rebalance_in = False
        service_in = copy.deepcopy(self.after_upgrade_services_in)
        if service_in is None:
            service_in = ["kv"]
        free_nodes = self._convert_server_map(self._get_free_nodes())
        free_node_in = []
        if not free_nodes.values():
            raise Exception("No free node available to rebalance in")
        try:
            self.nodes_in_list = self.out_servers_pool.values()[:self.nodes_in]
            if int(self.nodes_in) == 1:
                if len(free_nodes.keys()) > 1:
                    free_node_in = [free_nodes.values()[0]]
                    if len(self.after_upgrade_services_in) > 1:
                        service_in = [self.after_upgrade_services_in[0]]
                else:
                    free_node_in = free_nodes.values()
                self.log.info("<<<=== rebalance_in node {0} with services {1}" \
                              .format(free_node_in, service_in[0]))
                shell = RemoteMachineShellConnection(free_node_in[0])
                shell.stop_server()
                rebalance = \
                    self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 free_node_in,
                                                 [], services=service_in)

            rebalance.result()
            self.in_servers_pool.update(free_nodes)
            rebalance_in = True
            if any("index" in services for services in service_in):
                self.log.info("Set storageMode to forestdb after add "
                              "index node {0} to cluster".format(free_nodes.keys()))
                RestConnection(free_nodes.values()[0]).set_indexer_storage_mode()
            if self.after_upgrade_services_in and \
                    len(self.after_upgrade_services_in) > 1:
                self.log.info("remove service '{0}' from service list after "
                              "rebalance done ".format(self.after_upgrade_services_in[0]))
                self.after_upgrade_services_in.pop(0)
            self.sleep(10, "wait 10 seconds after rebalance")
            if free_node_in and free_node_in[0] not in self.servers:
                found_node = False
                for server in self.servers:
                    if free_node_in[0].ip in server.ip:
                        found_node = True
                        break
                if not found_node:
                    self.servers.append(free_node_in[0])
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            self.check_retry_rebalance_succeeded()
            if queue is not None:
                queue.put(False)
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            self.start_server(free_node_in[0])
        if rebalance_in and queue is not None:
            queue.put(True)


    def kv_ops_initialize(self, queue=None):
        try:
            self.log.info("kv_ops_initialize")
            self._load_all_buckets(self.master, self.gen_initial_create,
                                   "create", self.expire_time,
                                   flag=self.item_flag)
            self.log.info("done kv_ops_initialize")
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
            raise
        if queue is not None:
            queue.put(True)

    def kv_after_ops_create(self, queue=None):
        try:
            self.log.info("kv_after_ops_create")
            self._load_all_buckets(self.master, self.after_gen_create, "create",\
                                           self.expire_time, flag=self.item_flag)
            for bucket in self.buckets:
                self.log.info(" record vbucket for the bucket {0}"\
                                              .format(bucket.name))
                curr_items = \
                    RestConnection(self.master).get_active_key_count(bucket.name)
                self.log.info("{0} curr_items in bucket {1} "\
                              .format(curr_items, bucket.name))
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def kv_after_ops_update(self):
        try:
            self.log.info("kv_after_ops_update")
            self._load_all_buckets(self.master, self.after_gen_update, "update",
                                          self.expire_time, flag=self.item_flag)
        except Exception as ex:
            self.log.info(ex)
            raise

    def kv_after_ops_delete(self):
        try:
            self.log.info("kv_after_ops_delete")
            self._load_all_buckets(self.master, self.after_gen_delete,
                                   "delete", self.expire_time,
                                   flag=self.item_flag)
        except Exception as ex:
            self.log.info(ex)
            raise

    def doc_ops_initialize(self, queue=None):
        try:
            self.log.info("load doc to all buckets")
            self._load_doc_data_all_buckets(data_op="create", batch_size=1000,
                                            gen_load=None)
            self.log.info("done initialize load doc to all buckets")
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def kv_ops_create(self):
        try:
            self.log.info("kv_ops_create")
            self._load_all_buckets(self.master, self.gen_create, "create",
                                   self.expire_time, flag=self.item_flag)
        except Exception as ex:
            self.log.info(ex)
            raise

    def kv_ops_update(self):
        try:
            self.log.info("kv_ops_update")
            self._load_all_buckets(self.master, self.gen_update, "update",
                                   self.expire_time, flag=self.item_flag)
        except Exception as ex:
            self.log.info(ex)
            raise

    def kv_ops_delete(self):
        try:
            self.log.info("kv_ops_delete")
            self._load_all_buckets(self.master, self.gen_delete, "delete",
                                   self.expire_time, flag=self.item_flag)
        except Exception as ex:
            self.log.info(ex)
            raise

    def add_sub_doc(self):
        try:
            self.log.info("add sub doc")
            """add sub doc code here"""
        except Exception as ex:
            self.log.info(ex)
            raise

    def create_fts_index(self, queue=None):
        try:
            self.log.info("Checking if index already exists ...")
            name = "default"
            """ test on one bucket """
            for bucket in self.buckets:
                name = bucket.name
                break
            SOURCE_CB_PARAMS = {
                      "authUser": "default",
                      "authPassword": "",
                      "authSaslUser": "",
                      "authSaslPassword": "",
                      "clusterManagerBackoffFactor": 0,
                      "clusterManagerSleepInitMS": 0,
                      "clusterManagerSleepMaxMS": 20000,
                      "dataManagerBackoffFactor": 0,
                      "dataManagerSleepInitMS": 0,
                      "dataManagerSleepMaxMS": 20000,
                      "feedBufferSizeBytes": 0,
                      "feedBufferAckThreshold": 0
                       }
            self.index_type = 'fulltext-index'
            self.index_definition = {
                          "type": "fulltext-index",
                          "name": "",
                          "uuid": "",
                          "params": {},
                          "sourceType": "couchbase",
                          "sourceName": "",
                          "sourceUUID": "",
                          "sourceParams": SOURCE_CB_PARAMS,
                          "planParams": {}
                          }
            self.name = self.index_definition['name'] = \
                                self.index_definition['sourceName'] = name
            fts_node = self.get_nodes_from_services_map(
                "fts", servers=self.get_nodes_in_cluster_after_upgrade())
            if fts_node:
                rest = RestConnection(fts_node)
                status, _ = rest.get_fts_index_definition(self.name)
                if status != 400:
                    rest.delete_fts_index(self.name)
                self.log.info("Creating {0} {1} on {2}"
                              .format(self.index_type, self.name, rest.ip))
                rest.create_fts_index(self.name, self.index_definition)
            else:
                raise("No FTS node in cluster")
            self.ops_dist_map = self.calculate_data_change_distribution(
                create_per=self.create_ops_per, update_per=self.update_ops_per,
                delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                start=0, end=self.docs_per_day)
            self.log.info(self.ops_dist_map)
            self.dataset = "default"
            self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
            self.async_ops_all_buckets(self.docs_gen_map, batch_size=100)
        except Exception as ex:
            self.log.info(ex)

    def create_fts_index_query(self, queue=None):
        try:
            self.fts_obj = self.create_fts_index_query_compare()
            return self.fts_obj
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def xdcr_create_replication(self):
        try:
            self.xdcr_handle._create_replication()
        except Exception as ex:
            self.log.info(ex)

    def xdcr_set_replication_properties(self):
        try:
            param_str = self.__input.param(
                "%s@%s" %
                ("default", "C"), None)
            self.xdcr_handle._set_replication_properties(param_str)
        except Exception as ex:
            self.log.info(ex)

    def xdcr_get_replication_properties(self):
        try:
            self.xdcr_handle._get_replication_properties()
        except Exception as ex:
            self.log.info(ex)

    def create_n1ql_index_query(self, queue=None):
        try:
            self.create_n1ql_index_and_query()
            #return self.n1ql_obj
        except Exception as ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def cluster_stats(self, servers):
        self._wait_for_stats_all_buckets(servers)

    def _initialize_n1ql_helper(self):
        if self.n1ql_helper is None:
            self.n1ql_server = self.get_nodes_from_services_map(
                service_type="n1ql", servers=self.input.servers)
            self.n1ql_helper = N1QLHelper(
                version="sherlock", shell=None,
                use_rest=True, max_verify=self.max_verify,
                buckets=self.buckets, item_flag=None,
                n1ql_port=self.n1ql_server.n1ql_port, full_docs_list=[],
                log=self.log, input=self.input, master=self.master)

    def _get_free_nodes(self):
        self.log.info("Get free nodes in pool not in cluster yet")
        nodes = self.get_nodes_in_cluster_after_upgrade()
        free_nodes = copy.deepcopy(self.input.servers)
        for node in nodes:
            for server in free_nodes:
                if str(server.ip).strip() == str(node.ip).strip():
                    self.log.info("this node {0} is in cluster".format(server))
                    free_nodes.remove(server)
        if not free_nodes:
            self.log.info("No free node")
        else:
            self.log.info("Here is the list of free nodes {0}"
                          .format(free_nodes))
        return free_nodes

    def get_nodes_in_cluster_after_upgrade(self, master_node=None):
        if master_node is None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.input.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set


class UpgradeEventTests(UpgradeTests):
    pass
