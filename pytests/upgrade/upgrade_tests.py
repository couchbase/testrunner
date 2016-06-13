from newupgradebasetest import NewUpgradeBaseTest
import json
import os
import zipfile
import pprint
import Queue
import json
import logging
import copy
from membase.helper.cluster_helper import ClusterOperationHelper
import mc_bin_client
import threading
from fts.fts_base import FTSIndex
from memcached.helper.data_helper import  VBucketAwareMemcached
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.tuq_helper import N1QLHelper
from couchbase_helper.query_helper import QueryHelper
from TestInput import TestInputSingleton
from couchbase_helper.tuq_helper import N1QLHelper
from couchbase_helper.query_helper import QueryHelper
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator


class UpgradeTests(NewUpgradeBaseTest):

    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.queue = Queue.Queue()
        self.graceful =  self.input.param("graceful",False)
        self.after_upgrade_nodes_in =  self.input.param("after_upgrade_nodes_in",1)
        self.after_upgrade_nodes_out =  self.input.param("after_upgrade_nodes_out",1)
        self.verify_vbucket_info =  self.input.param("verify_vbucket_info",True)
        self.initialize_events = self.input.param("initialize_events","").split(":")
        self.upgrade_services_in = self.input.param("upgrade_services_in", None)
        self.after_upgrade_services_in = \
                              self.input.param("after_upgrade_services_in",None)
        self.after_upgrade_services_out_dist = \
                            self.input.param("after_upgrade_services_out_dist",None)
        self.in_between_events = self.input.param("in_between_events","").split(":")
        self.after_events = self.input.param("after_events","").split(":")
        self.before_events = self.input.param("before_events","").split(":")
        self.upgrade_type = self.input.param("upgrade_type","online")
        self.sherlock_upgrade = self.input.param("sherlock",False)
        self.max_verify = self.input.param("max_verify", None)
        self.verify_after_events = self.input.param("verify_after_events", True)
        self.online_upgrade_type = self.input.param("online_upgrade_type","swap")
        self.final_events = []
        self.n1ql_helper = None
        self.total_buckets = 1
        self.in_servers_pool = self._convert_server_map(self.servers[:self.nodes_init])
        """ Init nodes to not upgrade yet """
        for key in self.in_servers_pool.keys():
            self.in_servers_pool[key].upgraded = False
        self.out_servers_pool = self._convert_server_map(self.servers[self.nodes_init:])
        self.gen_initial_create = BlobGenerator('upgrade', 'upgrade',\
                                            self.value_size, end=self.num_items)
        self.gen_create = BlobGenerator('upgrade', 'upgrade', self.value_size,\
                            start=self.num_items + 1 , end=self.num_items * 1.5)
        self.gen_update = BlobGenerator('upgrade', 'upgrade', self.value_size,\
                                   start=self.num_items / 2, end=self.num_items)
        self.gen_delete = BlobGenerator('upgrade', 'upgrade', self.value_size,\
                           start=self.num_items / 4, end=self.num_items / 2 - 1)
        self.after_gen_create = BlobGenerator('upgrade', 'upgrade',\
             self.value_size, start=self.num_items * 1.6 , end=self.num_items * 2)
        self.after_gen_update = BlobGenerator('upgrade', 'upgrade',\
                                  self.value_size, start=1 , end=self.num_items/4)
        self.after_gen_delete = BlobGenerator('upgrade', 'upgrade',\
                                      self.value_size, start=self.num_items * .5,\
                                                         end=self.num_items* 0.75)
        self._install(self.servers[:self.nodes_init])
        self._log_start(self)
        self.cluster.rebalance([self.master], self.servers[1:self.nodes_init], [])
        """ sometimes, when upgrade failed and node does not install couchbase
            server yet, we could not set quota at beginning of the test.  We
            have to wait to install new couchbase server to set it properly here """
        self.quota = self._initialize_nodes(self.cluster, self.servers,\
                                         self.disabled_consistent_view,\
                                    self.rebalanceIndexWaitingDisabled,\
                                    self.rebalanceIndexPausingDisabled,\
                                              self.maxParallelIndexers,\
                                       self.maxParallelReplicaIndexers,\
                                                             self.port)
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        self.create_buckets()
        self.n1ql_server = None
        self.success_run = True
        self.failed_thread = None
        self.generate_map_nodes_out_dist_upgrade(self.after_upgrade_services_out_dist)
        self.upgrade_services_in = self.get_services(self.in_servers_pool.values(),
                                          self.upgrade_services_in, start_node = 0)
        self.after_upgrade_services_in = self.get_services(self.out_servers_pool.values(),
                                           self.after_upgrade_services_in, start_node = 0)

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    def test_upgrade(self):
        self.event_threads = []
        self.after_event_threads = []
        try:
            self.log.info("*** Start init operations before upgrade begins ***")
            if self.initialize_events:
                initialize_events = self.run_event(self.initialize_events)
            self.finish_events(initialize_events)
            if not self.success_run and self.failed_thread is not None:
                raise Exception("*** Failed to {0} ***".format(self.failed_thread))
            self.cluster_stats(self.servers[:self.nodes_init])
            if self.before_events:
                self.event_threads += self.run_event(self.before_events)
            self.log.info("*** Start upgrade cluster ***")
            self.event_threads += self.upgrade_event()
            if self.in_between_events:
                self.event_threads += self.run_event(self.in_between_events)
            self.finish_events(self.event_threads)
            if self.upgrade_type == "online":
                self.monitor_dcp_rebalance()
            self.log.info("Will install upgrade version to any free nodes")
            out_nodes = self._get_free_nodes()
            self.log.info("Here is free nodes {0}".format(out_nodes))
            """ only install nodes out when there is cluster operation """
            cluster_ops = ["rebalance_in", "rebalance_out", "rebalance_in_out"]
            for event in self.after_events[0].split("-"):
                if event in cluster_ops:
                    self.log.info("There are cluster ops after upgrade.  Need to "
                                  "install free nodes in upgrade version")
                    self._install(out_nodes)
                    break
            self.generate_map_nodes_out_dist_upgrade(\
                                      self.after_upgrade_services_out_dist)
            self.log.info("*** Start operations after upgrade is done ***")
            if self.after_events:
                self.after_event_threads = self.run_event(self.after_events)
            self.finish_events(self.after_event_threads)
            if not self.success_run and self.failed_thread is not None:
                raise Exception("*** Failed to {0} ***".format(self.failed_thread))
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
                self.cluster_stats(self.in_servers_pool.values())
                self._verify_data_active_replica()
        except Exception, ex:
            self.log.info(ex)
            print  "*** Stop all events to stop the test ***"
            self.stop_all_events(self.event_threads)
            self.stop_all_events(self.after_event_threads)
            raise
        finally:
            self.log.info("any events for which we need to cleanup")
            self.cleanup_events()

    def _record_vbuckets(self, master, servers):
        map ={}
        for bucket in self.buckets:
            self.log.info(" record vbucket for the bucket {0}".format(bucket.name))
            map[bucket.name] = RestHelper(RestConnection(master))\
                                   ._get_vbuckets(servers, bucket_name=bucket.name)
        return map

    def _find_master(self):
        self.master = self.in_servers_pool.values()[0]

    def _verify_data_active_replica(self):
        """ set data_analysis True by default """
        self.data_analysis =  self.input.param("data_analysis",False)
        self.total_vbuckets =  self.initial_vbuckets
        if self.data_analysis:
            disk_replica_dataset, disk_active_dataset = \
                        self.get_and_compare_active_replica_data_set_all(\
                                           self.in_servers_pool.values(),\
                                                            self.buckets,\
                                                                path=None)
            self.data_analysis_active_replica_all(disk_active_dataset,\
                                                 disk_replica_dataset,\
                                        self.in_servers_pool.values(),\
                                               self.buckets, path=None)
            """ check vbucket distribution analysis after rebalance """
            self.vb_distribution_analysis(servers = \
                      self.in_servers_pool.values(),\
                             buckets = self.buckets,\
                                         std = 1.0 ,\
                total_vbuckets = self.total_vbuckets)

    def _verify_vbuckets(self, old_vbucket_map, new_vbucket_map):
        for bucket in self.buckets:
            self._verify_vbucket_nums_for_swap(old_vbucket_map[bucket.name],\
                                                new_vbucket_map[bucket.name])

    def stop_all_events(self, thread_list):
        for t in thread_list:
            try:
                if t.isAlive():
                    t.stop()
            except Exception, ex:
                self.log.info(ex)

    def cleanup_events(self):
        thread_list = []
        for event in self.final_events:
            t = threading.Thread(target=self.find_function(event), args = ())
            t.daemon = True
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()

    def run_event_in_sequence(self, events):
        q = self.queue
        self.log.info("run_event_in_sequence")
        for event in events.split("-"):
            t = threading.Thread(target=self.find_function(event), args = (q,))
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
                t = threading.Thread(target=self.run_event_in_sequence, args = (event,))
                t.start()
                t.join()
            elif event != '':
                t = threading.Thread(target=self.find_function(event), args = ())
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
        thread_list = []
        if self.upgrade_type == "online":
            t = threading.Thread(target=self.online_upgrade, args = ())
        elif self.upgrade_type == "offline":
            t = threading.Thread(target=self.offline_upgrade, args = ())
        t.daemon = True
        t.start()
        thread_list.append(t)
        return thread_list

    def server_crash(self):
        try:
            self.log.info("server_crash")
            self.targetProcess= self.input.param("targetProcess",'memcached')
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.terminate_process(process_name=self.targetProcess)
        except Exception, ex:
            self.log.info(ex)
            raise

    def server_stop(self):
        try:
            self.log.info("server_stop")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.stop_server()
            self.final_events.append("start_server")
        except Exception, ex:
            self.log.info(ex)
            raise

    def start_server(self):
        try:
            self.log.info("start_server")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.start_server()
        except Exception, ex:
            self.log.info(ex)
            raise

    def failover(self, queue=None):
        failover_node = False
        try:
            self.log.info("VVVVVV failover a node ")
            print "failover node   ", self.nodes_out_list
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
        except Exception, ex:
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
        except Exception, ex:
            self.log.info(ex)
            raise

    def network_partitioning(self):
        try:
            self.log.info("network_partitioning")
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
            self.final_events.append("undo_network_partitioning")
        except Exception, ex:
            self.log.info(ex)
            raise

    def undo_network_partitioning(self):
        try:
            self.log.info("remove_network_partitioning")
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)
        except Exception, ex:
            self.log.info(ex)
            raise

    def bucket_compaction(self):
        try:
            self.log.info("couchbase_bucket_compaction")
            compact_tasks = []
            for bucket in self.buckets:
                compact_tasks.append(self.cluster.async_compact_bucket(self.master,bucket))
        except Exception, ex:
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
        except Exception, ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if node_warmuped and queue is not None:
            queue.put(True)

    def bucket_flush(self, queue=None):
        bucket_flushed = False
        try:
            self.log.info("bucket_flush ops")
            self.rest =RestConnection(self.master)
            for bucket in self.buckets:
                self.rest.flush_bucket(bucket.name)
            bucket_flushed = True
        except Exception, ex:
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
        except Exception, ex:
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
            self.rest = RestConnection(self.master)
            self._bucket_creation()
            self.total_buckets +=1
            bucket_created = True
        except Exception, ex:
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
                       authType=None, saslPassword=None, replicaNumber=0,\
                    proxyPort=None, replicaIndex=None, flushEnabled=False)
        except Exception, ex:
            self.log.info(ex)
            raise

    def rebalance_in(self, queue=None):
        rebalance_in = False
        service_in = copy.deepcopy(self.after_upgrade_services_in)
        if service_in is None:
            service_in = ["kv"]
        free_nodes = self._convert_server_map(self._get_free_nodes())
        if not free_nodes.values():
            raise Exception("No free node available to rebalance in")
        try:
            self.nodes_in_list =  self.out_servers_pool.values()[:self.nodes_in]
            if int(self.nodes_in) == 1:
                if len(free_nodes.keys()) > 1:
                    free_node_in = [free_nodes.values()[0]]
                    if len(self.after_upgrade_services_in) > 1:
                        service_in = [self.after_upgrade_services_in[0]]
                else:
                    free_node_in = free_nodes.values()
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
                         "index node {0} to cluster".format(free_nodes.keys()))
                RestConnection(free_nodes.values()[0]).set_indexer_storage_mode()
            if self.after_upgrade_services_in and \
                len(self.after_upgrade_services_in) > 1:
                self.log.info("remove service '{0}' from service list after "
                        "rebalance done ".format(self.after_upgrade_services_in[0]))
                self.after_upgrade_services_in.pop(0)
            self.sleep(10, "wait 10 seconds after rebalance")
        except Exception, ex:
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
        except Exception, ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if rebalance_out and queue is not None:
            queue.put(True)

    def rebalance_in_out(self, queue=None):
        rebalance_in_out = False
        try:
            self.nodes_in_list =  self.out_servers_pool.values()[:self.nodes_in]
            self.log.info("<<<<<===== rebalance_in node {0}"\
                                                    .format(self.nodes_in_list))
            self.log.info("=====>>>>> rebalance_out node {0}"\
                                                   .format(self.nodes_out_list))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],\
                                            self.nodes_in_list, self.nodes_out_list,\
                                           services = self.after_upgrade_services_in)
            rebalance.result()
            rebalance_in_out = True
        except Exception, ex:
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
        except Exception, ex:
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
        except Exception, e:
            self.log.info(e)
            if queue is not None:
                queue.put(False)
        if create_index and queue is not None:
            queue.put(True)

    def create_views(self, queue=None):
        self.log.info("*** create_views ***")
        """ default is 1 ddoc. Change number of ddoc by param ddocs_num=new_number
            default is 2 views. Change number of views by param
            view_per_ddoc=new_view_per_doc """
        try:
            self.create_ddocs_and_views(queue)
        except Exception, e:
            self.log.info(e)

    def query_views(self, queue=None):
        self.log.info("*** query_views ***")
        try:
           self.verify_all_queries(queue)
        except Exception, e:
            self.log.info(e)

    def drop_views(self):
        self.log.info("drop_views")

    def drop_index(self):
        self.log.info("drop_index")
        for bucket_name in self.index_list.keys():
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
        except Exception, ex:
            self.log.info(ex)
            raise

    def online_upgrade(self):
        try:
            self.log.info("online_upgrade")
            self.initial_version = self.upgrade_versions[0]
            self.sleep(self.sleep_time, "Pre-setup of old version is done. "
                                    "Wait for online upgrade to {0} version"\
                                               .format(self.initial_version))
            self.product = 'couchbase-server'
            if self.online_upgrade_type == "swap":
                self.online_upgrade_swap_rebalance()
            else:
                self.online_upgrade_incremental()
        except Exception, ex:
            self.log.info(ex)
            raise

    def online_upgrade_swap_rebalance(self):
        self.log.info("online_upgrade_swap_rebalance")
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        servers = self._convert_server_map(self.servers[:self.nodes_init])
        out_servers = self._convert_server_map(self.servers[self.nodes_init:])
        self.swap_num_servers = min(self.swap_num_servers, len(out_servers))
        start_services_num = 0
        for i in range(self.nodes_init / self.swap_num_servers):
            servers_in = {}
            new_servers = copy.deepcopy(servers)
            for key in out_servers.keys():
                servers_in[key] = out_servers[key]
                out_servers[key].upgraded = True
                out_servers.pop(key)
                if len(servers_in) == self.swap_num_servers:
                    break
            servers_out = {}
            new_servers.update(servers_in)
            for key in servers.keys():
                if len(servers_out) == self.swap_num_servers:
                    break
                elif not servers[key].upgraded:
                    servers_out[key] = servers[key]
                    new_servers.pop(key)
            out_servers.update(servers_out)
            self.log.info("current {0}".format(servers))
            self.log.info("will come inside {0}".format(servers_in))
            self.log.info("will go out {0}".format(servers_out))
            self._install(servers_in.values())
            old_vbucket_map = self._record_vbuckets(self.master, servers.values())
            if self.upgrade_services_in != None and len(self.upgrade_services_in) > 0:
                self.cluster.rebalance(servers.values(),
                                    servers_in.values(),
                      servers_out.values(), services = \
                   self.upgrade_services_in[start_services_num:start_services_num+\
                                                         len(servers_in.values())])
                start_services_num += len(servers_in.values())
            else:
                self.cluster.rebalance(servers.values(), servers_in.values(),\
                                                         servers_out.values())
            self.out_servers_pool = servers_out
            self.in_servers_pool = new_servers
            servers = new_servers
            self.servers = servers.values()
            self.master = self.servers[0]
            if self.verify_vbucket_info:
                new_vbucket_map = self._record_vbuckets(self.master, self.servers)
                self._verify_vbuckets(old_vbucket_map, new_vbucket_map)

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
            self._new_master(self.servers[1])
            self.cluster.rebalance(self.servers, [], [self.servers[0]])
            self.log.info("Rebalanced out all old version nodes")
        except Exception, ex:
            self.log.info(ex)
            raise

    def offline_upgrade(self):
        try:
            self.log.info("offline_upgrade")
            stoped_nodes = self.servers[:self.nodes_init]
            for upgrade_version in self.upgrade_versions:
                self.sleep(self.sleep_time, "Pre-setup of old version is done. "
                        " Wait for upgrade to {0} version".format(upgrade_version))
                for server in stoped_nodes:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    remote.disconnect()
                self.sleep(self.sleep_time)
                upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed!")
                self.dcp_rebalance_in_offline_upgrade_from_version2()
            """ set install cb version to upgrade version after done upgrade """
            self.initial_version = self.upgrade_versions[0]
        except Exception, ex:
            self.log.info(ex)
            raise

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
        except Exception, ex:
            raise

    def kv_ops_initialize(self, queue=None):
        try:
            self.log.info("kv_ops_initialize")
            self._load_all_buckets(self.master, self.gen_initial_create, "create",\
                                             self.expire_time, flag=self.item_flag)
            self.log.info("done kv_ops_initialize")
        except Exception, ex:
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
        except Exception, ex:
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
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_after_ops_delete(self):
        try:
            self.log.info("kv_after_ops_delete")
            self._load_all_buckets(self.master, self.after_gen_delete, "delete",
                                          self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def doc_ops_initialize(self, queue=None):
        try:
            self.log.info("load doc to all buckets")
            self._load_doc_data_all_buckets(data_op="create", batch_size=1000,
                                                                gen_load=None)
            self.log.info("done initialize load doc to all buckets")
        except Exception, ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def _convert_server_map(self, servers):
        map = {}
        for server in servers:
            key  = self._gen_server_key(server)
            map[key] = server
        return map

    def _gen_server_key(self, server):
        return "{0}:{1}".format(server.ip, server.port)

    def kv_ops_create(self):
        try:
            self.log.info("kv_ops_create")
            self._load_all_buckets(self.master, self.gen_create, "create",
                                    self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_ops_update(self):
        try:
            self.log.info("kv_ops_update")
            self._load_all_buckets(self.master, self.gen_update, "update",
                                    self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_ops_delete(self):
        try:
            self.log.info("kv_ops_delete")
            self._load_all_buckets(self.master, self.gen_delete, "delete",
                                    self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def add_sub_doc(self):
        try:
            self.log.info("add sub doc")
            """add sub doc code here"""
        except Exception, ex:
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
            fts_node = self.get_nodes_from_services_map("fts", \
                                servers=self.get_nodes_in_cluster_after_upgrade())
            if fts_node:
                rest = RestConnection(fts_node)
                status, _ = rest.get_fts_index_definition(self.name)
                if status != 400:
                    rest.delete_fts_index(self.name)
                self.log.info("Creating {0} {1} on {2}".format(self.index_type,
                                                           self.name, rest.ip))
                rest.create_fts_index(self.name, self.index_definition)
            else:
                raise("No FTS node in cluster")
            self.ops_dist_map = self.calculate_data_change_distribution(
                create_per=self.create_ops_per , update_per=self.update_ops_per ,
                delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                start=0, end=self.docs_per_day)
            self.log.info(self.ops_dist_map)
            self.dataset = "default"
            self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
            self.async_ops_all_buckets(self.docs_gen_map, batch_size=100)
        except Exception, ex:
            self.log.info(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def cluster_stats(self, servers):
        self._wait_for_stats_all_buckets(servers)

    def _initialize_n1ql_helper(self):
        if self.n1ql_helper == None:
            self.n1ql_server = self.get_nodes_from_services_map(service_type = \
                                              "n1ql",servers=self.input.servers)
            self.n1ql_helper = N1QLHelper(version = "sherlock", shell = None,
                use_rest = True, max_verify = self.max_verify,
                buckets = self.buckets, item_flag = None,
                n1ql_port = self.n1ql_server.n1ql_port, full_docs_list = [],
                log = self.log, input = self.input, master = self.master)

    def _get_free_nodes(self):
        self.log.info("Get free nodes in pool not in cluster yet")
        nodes = self.get_nodes_in_cluster_after_upgrade()
        free_nodes = copy.deepcopy(self.input.servers)
        found = False
        for node in nodes:
            for server in free_nodes:
                if str(server.ip).strip() == str(node.ip).strip():
                    self.log.info("this node {0} is in cluster".format(server))
                    free_nodes.remove(server)
                    found = True
        if not free_nodes:
            self.log.info("no free node")
            return free_nodes
        else:
            self.log.info("here is the list of free nodes {0}"\
                                       .format(free_nodes))
            return free_nodes

    def get_nodes_in_cluster_after_upgrade(self, master_node=None):
        rest = None
        if master_node == None:
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
