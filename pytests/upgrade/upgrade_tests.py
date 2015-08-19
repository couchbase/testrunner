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
from memcached.helper.data_helper import  VBucketAwareMemcached
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.tuq_helper import N1QLHelper
from couchbase_helper.query_helper import QueryHelper
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator


class UpgradeTests(NewUpgradeBaseTest):

    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.initialize_events = self.input.param("initialize_events","").split(":")
        self.in_between_events = self.input.param("in_between_events","").split(":")
        self.after_events = self.input.param("after_events","").split(":")
        self.before_events = self.input.param("before_events","").split(":")
        self.upgrade_type = self.input.param("upgrade_type","online")
        self.online_upgrade_type = self.input.param("online_upgrade_type","swap")
        self.final_events = []
        self.gen_initial_create = BlobGenerator('upgrade', 'upgrade', self.value_size, end=self.num_items)
        self.gen_create = BlobGenerator('upgrade', 'upgrade', self.value_size, start=self.num_items + 1 , end=self.num_items * 1.5)
        self.gen_update = BlobGenerator('upgrade', 'upgrade', self.value_size, start=self.num_items / 2, end=self.num_items)
        self.gen_delete = BlobGenerator('upgrade', 'upgrade', self.value_size, start=self.num_items / 4, end=self.num_items / 2 - 1)
        self.after_gen_create = BlobGenerator('upgrade', 'upgrade', self.value_size, start=self.num_items * 1.6 , end=self.num_items * 2)
        self.after_gen_update = BlobGenerator('upgrade', 'upgrade', self.value_size, start=1 , end=self.num_items/4)
        self.after_gen_delete = BlobGenerator('upgrade', 'upgrade', self.value_size, start=self.num_items * .5 , end=self.num_items* 0.75)

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    def test_upgrade(self):
        self.event_threads = []
        self.after_event_threads = []
        try:
            if self.initialize_events:
                initialize_events = self.run_event(self.initialize_events)
            self.finish_events(initialize_events)
            if self.before_events:
                self.event_threads += self.run_event(self.before_events)
            self.event_threads += self.upgrade_event()
            if self.in_between_events:
                self.event_threads += self.run_event(self.in_between_events)
            self.finish_events(self.event_threads)
            if self.after_events:
                self.after_event_threads = self.run_event(self.after_events)
            self.finish_events(self.after_event_threads)
        except Exception, ex:
            self.log.info(ex)
            self.stop_all_events(self.event_threads)
            self.stop_all_events(self.after_event_threads)
            raise
        finally:
            self.log.info("any events for which we need to cleanup")
            self.cleanup_events()

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

    def run_event(self, events):
        thread_list = []
        for event in events:
            if event != '':
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
        else:
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
            raise
        finally:
            self.log.info(ex)

    def server_stop(self):
        try:
            self.log.info("server_stop")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.stop_server()
            self.final_events.append("start_server")
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def server_start(self):
        try:
            self.log.info("server_start")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.start_server()
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def failover(self):
        try:
            self.log.info("failover")
            failover_task = self.cluster.async_failover([self.master],
                        failover_nodes = servr_out, graceful=self.graceful)
            failover_task.result()
            if self.graceful:
            # Check if rebalance is still running
                msg = "graceful failover failed for nodes"
                self.assertTrue(RestConnection(self.master).monitorRebalance(stop_if_loop=True), msg=msg)
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                       [], servr_out)
                rebalance.result()
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def autofailover(self):
        try:
            self.log.info("autofailover")
            autofailover_timeout = 30
            status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
            self.assertTrue(status, 'failed to change autofailover_settings!')
            self._run_initial_index_tasks()
            servr_out = self.nodes_out_list
            remote = RemoteMachineShellConnection(servr_out[0])
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                       [], [servr_out[0]])
            in_between_index_ops = self._run_in_between_tasks()
            rebalance.result()
            self.final_events.append("start_server")
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def network_partitioning(self):
        try:
            self.log.info("network_partitioning")
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
            self.final_events.append("undo_network_partitioning")
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def undo_network_partitioning(self):
        try:
            self.log.info("remove_network_partitioning")
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def bucket_compaction(self):
        try:
            self.log.info("couchbase_bucket_compaction")
            compact_tasks = []
            for bucket in self.buckets:
                compact_tasks.append(self.cluster.async_compact_bucket(self.master,bucket))
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def warmup(self):
        try:
            self.log.info("warmup")
            for server in self.nodes_out_list:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.start_server()
                remote.disconnect()
            ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def bucket_flush(self):
        try:
            self.log.info("bucket_flush")
            for bucket in self.buckets:
                self.rest.flush_bucket(bucket.name)
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def delete_bucket(self):
        try:
            self.log.info("delete_bucket")
            for bucket in self.buckets:
                self.rest.delete_bucket(bucket.name)
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def create_bucket(self):
        try:
            self.log.info("create_bucket")
            self._bucket_creation()
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def change_bucket_properties(self):
        try:
        #Change Bucket Properties
            for bucket in self.buckets:
                self.rest.change_bucket_props(bucket, ramQuotaMB=None, authType=None, saslPassword=None, replicaNumber=0,
                    proxyPort=None, replicaIndex=None, flushEnabled=False)
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def rebalance_in(self):
        try:
            self.log.info("rebalance_in")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],self.nodes_in_list, [], services = self.services_in)
            rebalance.result()
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def rebalance_out(self):
        try:
            self.log.info("rebalance_out")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],self.nodes_out_list)
            rebalance.result()
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

    def swap_rebalance(self):
        try:
            self.log.info("swap_rebalance")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                        self.nodes_in_list,
                                       self.nodes_out_list, services = self.services_in)
            rebalance.result()
        except Exception, ex:
            raise
        finally:
            self.log.info(ex)

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

    def create_index(self):
        self.log.info("create_index")

    def create_views(self):
        self.create_ddocs_and_views()

    def view_queries(self):
        self.verify_all_queries()

    def drop_views(self):
        self.log.info("drop_views")

    def drop_index(self):
        self.log.info("drop_index")

    def query_with_views(self):
        self.log.info("query_with_views")

    def query_explain(self):
        self.log.info("query_explain")

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
            #self._install(self.servers[:self.nodes_init])
            #self.initial_version = self.upgrade_versions[0]
            #self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version".\
            #               format(self.initial_version))
            self.product = 'couchbase-server'
            if self.online_upgrade_type == "swap":
                self.online_upgrade_swap_rebalance()
            else:
                self.online_upgrade_incremental()
        except Exception, ex:
            self.log.info(ex)
            raise

    def online_upgrade_swap_rebalance(self):
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        servers = self._convert_server_map(self.servers[:self.nodes_init])
        out_servers = self._convert_server_map(self.servers[self.nodes_init:])
        self.swap_num_servers = min(self.swap_num_servers, len(out_servers))
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
            self._install(servers_out.values())
            self.cluster.rebalance(servers.values(), servers_in.values(), servers_out.values())
            servers = new_servers

    def online_upgrade_incremental(self):
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
            self._install(self.servers[:self.nodes_init])
            num_nodes_reinstall = self.input.param('num_nodes_reinstall', 1)
            stoped_nodes = self.servers[self.nodes_init - (self.nodes_init - num_nodes_reinstall):self.nodes_init]
            nodes_reinstall = self.servers[:num_nodes_reinstall]
            for upgrade_version in self.upgrade_versions:
                self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version".\
                           format(upgrade_version))
                for server in stoped_nodes:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    remote.disconnect()
                self.sleep(self.sleep_time)
                upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
                self.force_reinstall(nodes_reinstall)
                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed!")
                self.dcp_rebalance_in_offline_upgrade_from_version2_to_version3()
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_ops_initialize(self):
        try:
            self.log.info("kv_ops_initialize")
            self._load_all_buckets(self.master, self.gen_initial_create, "intialize_create", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_after_ops_create(self):
        try:
            self.log.info("kv_after_ops_create")
            self._load_all_buckets(self.master, self.after_gen_create, "create", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_after_ops_update(self):
        try:
            self.log.info("kv_after_ops_update")
            self._load_all_buckets(self.master, self.after_gen_update, "update", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv__after_ops_delete(self):
        try:
            self.log.info("kv_after_ops_update")
            self._load_all_buckets(self.master, self.after_gen_delete, "delete", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

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
            self._load_all_buckets(self.master, self.gen_create, "create", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_ops_update(self):
        try:
            self.log.info("kv_ops_update")
            self._load_all_buckets(self.master, self.gen_update, "update", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def kv_ops_delete(self):
        try:
            self.log.info("kv_ops_delete")
            self._load_all_buckets(self.master, self.gen_delete, "delete", self.expire_time, flag=self.item_flag)
        except Exception, ex:
            self.log.info(ex)
            raise

    def cluster_stats(self):
        self._wait_for_stats_all_buckets(self.servers)