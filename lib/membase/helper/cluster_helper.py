from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.exception import ServerAlreadyJoinedException
from membase.helper.rebalance_helper import RebalanceHelper
from TestInput import TestInputSingleton
import memcacheConstants

import logger
import testconstants
import time
import queue
from threading import Thread
import traceback

from lib.Cb_constants.CBServer import CbServer


class ClusterOperationHelper(object):
    # the first ip is taken as the master ip

    # Returns True if cluster successfully finished then rebalance
    @staticmethod
    def add_and_rebalance(servers, wait_for_rebalance=True):
        log = logger.Logger.get_logger()
        master = servers[0]
        all_nodes_added = True
        rebalanced = True
        rest = RestConnection(master)
        if len(servers) > 1:
            for serverInfo in servers[1:]:
                log.info('adding node : {0}:{1} to the cluster'.format(
                        serverInfo.ip, serverInfo.port))
                if serverInfo.internal_ip:
                    rest = RestConnection(serverInfo)
                    rest.set_alternate_address(serverInfo.ip)
                otpNode = rest.add_node(master.rest_username, master.rest_password, serverInfo.cluster_ip, port=serverInfo.port)
                if otpNode:
                    log.info('added node : {0} to the cluster'.format(otpNode.id))
                else:
                    all_nodes_added = False
                    break
            if all_nodes_added:
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                if wait_for_rebalance:
                    rebalanced &= rest.monitorRebalance()
                else:
                    rebalanced = False
        return all_nodes_added and rebalanced

    @staticmethod
    def add_all_nodes_or_assert(master, all_servers, rest_settings, test_case):
        log = logger.Logger.get_logger()
        otpNodes = []
        all_nodes_added = True
        rest = RestConnection(master)
        for serverInfo in all_servers:
            if serverInfo.ip != master.ip:
                log.info('adding node : {0}:{1} to the cluster'.format(
                        serverInfo.cluster_ip, serverInfo.port))
                if serverInfo.internal_ip:
                    rest = RestConnection(serverInfo)
                    rest.set_alternate_address(serverInfo.ip)
                otpNode = rest.add_node(rest_settings.rest_username,
                                        rest_settings.rest_password,
                                        serverInfo.cluster_ip)
                if otpNode:
                    log.info('added node : {0} to the cluster'.format(otpNode.id))
                    otpNodes.append(otpNode)
                else:
                    all_nodes_added = False
        if not all_nodes_added:
            if test_case:
                test_case.assertTrue(all_nodes_added,
                                     msg="unable to add all the nodes to the cluster")
            else:
                log.error("unable to add all the nodes to the cluster")
        return otpNodes

    #wait_if_warmup=True is useful in tearDown method for (auto)failover tests
    @staticmethod
    def wait_for_ns_servers_or_assert(servers, testcase, wait_time=360,
                                      wait_if_warmup=False, debug=True):
        for server in servers:
            rest = RestConnection(server)
            log = logger.Logger.get_logger()
            if debug:
                log.info("waiting for ns_server @ {0}:{1}"
                         .format(server.ip, server.port))
            if RestHelper(rest).is_ns_server_running(wait_time):
                if debug:
                    log.info("ns_server @ {0}:{1} is running"
                             .format(server.ip, server.port))

            elif wait_if_warmup:
                # wait when warmup completed
                buckets = rest.get_buckets()
                for bucket in buckets:
                    testcase.assertTrue(ClusterOperationHelper._wait_warmup_completed(testcase,\
                                [server], bucket.name, wait_time), "warmup was not completed!")

            else:
                testcase.fail("ns_server {0} is not running in {1} sec"
                                         .format(server.ip, wait_time))

    # returns true if warmup is completed in wait_time sec
    # otherwise return false
    @staticmethod
    def _wait_warmup_completed(self, servers, bucket_name, wait_time=300):
        warmed_up = False
        log = logger.Logger.get_logger()
        for server in servers:
            mc = None
            start = time.time()
            # Try to get the stats for 5 minutes, else hit out.
            while time.time() - start < wait_time:
                # Get the wamrup time for each server
                try:
                    mc = MemcachedClientHelper.direct_client(server, bucket_name)
                    stats = mc.stats()
                    if stats is not None and 'ep_warmup_thread' in stats and stats['ep_warmup_thread'] == 'complete':
                        break
                    else:
                        log.info(" Did not get the stats from the server yet, trying again.....")
                        time.sleep(2)
                except Exception as e:
                    log.error(
                        "Could not get ep_warmup_time stats from server %s:%s, exception %s" %
                             (server.ip, server.port, e))
            else:
                self.fail(
                    "Fail! Unable to get the warmup-stats from server %s:%s after trying for %s seconds." % (
                        server.ip, server.port, wait_time))

            # Waiting for warm-up
            start = time.time()
            warmed_up = False
            while time.time() - start < wait_time and not warmed_up:
                if mc.stats()["ep_warmup_thread"] == "complete":
                    log.info("warmup completed, awesome!!! Warmed up. %s items " % (mc.stats()["curr_items_tot"]))
                    warmed_up = True
                    continue
                elif mc.stats()["ep_warmup_thread"] == "running":
                    log.info(
                                "still warming up .... curr_items_tot : %s" % (mc.stats()["curr_items_tot"]))
                else:
                    self.fail("Value of ep warmup thread does not exist, exiting from this server")
                time.sleep(5)
            mc.close()
        return warmed_up

    @staticmethod
    def verify_persistence(servers, test, keys_count=400000, timeout_in_seconds=300):
        log = logger.Logger.get_logger()
        master = servers[0]
        rest = RestConnection(master)
        log.info("Verifying Persistence")
        buckets = rest.get_buckets()
        for bucket in buckets:
        # Load some data
            l_threads = MemcachedClientHelper.create_threads([master], bucket.name,
                                                                     - 1, keys_count, {1024: 0.50, 512: 0.50}, 2, -1,
                                                                     True, True)
            [t.start() for t in l_threads]
            # Do persistence verification
            ready = ClusterOperationHelper.persistence_verification(servers, bucket.name, timeout_in_seconds)
            log.info("Persistence Verification returned ? {0}".format(ready))
            log.info("waiting for persistence threads to finish...")
            for t in l_threads:
                t.aborted = True
            for t in l_threads:
                t.join()
            log.info("persistence thread has finished...")
            test.assertTrue(ready, msg="Cannot verify persistence")

    @staticmethod
    def persistence_verification(servers, bucket, timeout_in_seconds=1260):
        log = logger.Logger.get_logger()
        verification_threads = []
        queue = queue.Queue()
        rest = RestConnection(servers[0])
        nodes = rest.get_nodes()
        nodes_ip = []
        for node in nodes:
            nodes_ip.append(node.ip)
        for i in range(len(servers)):
            if servers[i].ip in nodes_ip:
                log.info("Server {0}:{1} part of cluster".format(
                        servers[i].ip, servers[i].port))
                rest = RestConnection(servers[i])
                t = Thread(target=ClusterOperationHelper.persistence_verification_per_node,
                           name="verification-thread-{0}".format(servers[i]),
                           args=(rest, bucket, queue, timeout_in_seconds))
                verification_threads.append(t)
        for t in verification_threads:
            t.start()
        for t in verification_threads:
            t.join()
            log.info("thread {0} finished".format(t.name))
        while not queue.empty():
            item = queue.get()
            if item is False:
                return False
        return True

    @staticmethod
    def persistence_verification_per_node(rest, bucket, queue=None, timeout=1260):
        log = logger.Logger.get_logger()
        stat_key = 'ep_flusher_todo'
        start = time.time()
        stats = []
        # Collect stats data points
        while time.time() - start <= timeout:
            _new_stats = rest.get_bucket_stats(bucket)
            if _new_stats and 'ep_flusher_todo' in _new_stats:
                stats.append(_new_stats[stat_key])
                time.sleep(0.5)
            else:
                log.error("unable to obtain stats for bucket : {0}".format(bucket))
        value_90th = ClusterOperationHelper.percentile(stats, 90)
        average = float(sum(stats)) / len(stats)
        log.info("90th percentile value is {0} and average {1}".format(value_90th, average))
        if value_90th == 0 and average == 0:
            queue.put(False)
            return
        queue.put(True)

    @staticmethod
    def percentile(samples, percentile):
        element_idx = int(len(samples) * (percentile / 100.0))
        samples.sort()
        value = samples[element_idx]
        return value

    @staticmethod
    def start_cluster(servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            if shell.is_couchbase_installed():
                shell.start_couchbase()
            else:
                shell.start_membase()

    @staticmethod
    def stop_cluster(servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            if shell.is_couchbase_installed():
                shell.stop_couchbase()
            else:
                shell.stop_membase()

    @staticmethod
    def cleanup_cluster(servers, wait_for_rebalance=True, master = None):
        log = logger.Logger.get_logger()
        if master is None:
            master = servers[0]
        rest = RestConnection(master)
        helper = RestHelper(rest)
        helper.is_ns_server_running(timeout_in_seconds=testconstants.NS_SERVER_TIMEOUT)
        nodes = rest.node_statuses()
        master_id = rest.get_nodes_self().id
        for node in nodes:
            if int(node.port) in range(9091, 9991):
                rest.eject_node(node)
                nodes.remove(node)

        if len(nodes) > 1:
            log.info("rebalancing all nodes in order to remove nodes")
            rest.log_client_error("Starting rebalance from test, ejected nodes %s" % \
                                                             [node.id for node in nodes if node.id != master_id])
            removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                          ejectedNodes=[node.id for node in nodes if node.id != master_id],
                                          wait_for_rebalance=wait_for_rebalance)
            success_cleaned = []
            alt_addr = TestInputSingleton.input.param("alt_addr", False)
            for removed in [node for node in nodes if (node.id != master_id)]:
                removed.rest_password = servers[0].rest_password
                removed.rest_username = servers[0].rest_username
                try:
                    if alt_addr:
                        for server in servers:
                            shell = RemoteMachineShellConnection(server)
                            internal_IP = shell.get_ip_address()
                            internal_IP = [x for x in internal_IP if x != "127.0.0.1"]
                            shell.disconnect()
                            if internal_IP == removed.ip:
                                rest = RestConnection(server)
                                break
                    else:
                        rest = RestConnection(removed)
                except Exception as ex:
                    log.error("can't create rest connection after rebalance out for ejected nodes,\
                        will retry after 10 seconds according to MB-8430: {0} ".format(ex))
                    time.sleep(10)
                    rest = RestConnection(removed)
                start = time.time()
                while time.time() - start < 30:
                    if len(rest.get_pools_info()["pools"]) == 0:
                        success_cleaned.append(removed)
                        break
                    else:
                        time.sleep(0.1)
                if time.time() - start > 10:
                    log.error("'pools' on node {0}:{1} - {2}".format(
                           removed.ip, removed.port, rest.get_pools_info()["pools"]))
            for node in {node for node in nodes if (node.id != master_id)} - set(success_cleaned):
                log.error("node {0}:{1} was not cleaned after removing from cluster".format(
                           removed.ip, removed.port))
                try:
                    if alt_addr:
                        for server in servers:
                            shell = RemoteMachineShellConnection(server)
                            internal_IP = shell.get_ip_address()
                            internal_IP = [x for x in internal_IP if x != "127.0.0.1"]
                            shell.disconnect()
                            if internal_IP == removed.ip:
                                rest = RestConnection(server)
                                break
                    else:
                        rest = RestConnection(removed)
                    if not alt_addr:
                        rest.force_eject_node()
                except Exception as ex:
                    log.error("force_eject_node {0}:{1} failed: {2}".format(removed.ip, removed.port, ex))
            if len({node for node in nodes if (node.id != master_id)}\
                    - set(success_cleaned)) != 0:
                if not alt_addr:
                    raise Exception("not all ejected nodes were cleaned successfully")

            log.info("removed all the nodes from cluster associated with {0} ? {1}".format(servers[0], \
                    [(node.id, node.port) for node in nodes if (node.id != master_id)]))

    @staticmethod
    def flushctl_start(servers, username=None, password=None):
        for server in servers:
            c = MemcachedClient(server.ip, 11210)
            if username:
                c.sasl_auth_plain(username, password)
            c.start_persistence()

    @staticmethod
    def flushctl_stop(servers, username=None, password=None):
        for server in servers:
            c = MemcachedClient(server.ip, 11210)
            if username:
                c.sasl_auth_plain(username, password)
            c.stop_persistence()

    @staticmethod
    def flush_os_caches(servers):
        log = logger.Logger.get_logger()
        for server in servers:
            try:
                shell = RemoteMachineShellConnection(server)
                shell.flush_os_caches()
                log.info("Clearing os caches on {0}".format(server))
            except:
                pass

    @staticmethod
    def flushctl_set(master, key, val, bucket='default'):
        rest = RestConnection(master)
        servers = rest.get_nodes()
        if key == "exp_pager_stime":
            rest.change_bucket_props(bucket, expiryPagerSleepTime=val)
        else:
            for server in servers:
                if "kv" in server.services:
                    _server = {"ip": server.ip, "port": server.port,
                               "username": master.rest_username,
                               "password": master.rest_password}
                    ClusterOperationHelper.flushctl_set_per_node(_server, key, val, bucket)

    @staticmethod
    def flushctl_set_per_node(server, key, val, bucket='default'):
        log = logger.Logger.get_logger()
        rest = RestConnection(server)
        node = rest.get_nodes_self()
        mc = MemcachedClientHelper.direct_client(server, bucket)
        log.info("Setting flush param on server {0}, {1} to {2} on {3}".format(server, key, val, bucket))
        # Workaround for CBQE-249, ideally this should be node.version
        index_path = node.storage[0].get_index_path()
        if index_path == '':
            # Indicates non 2.0 build
            rv = mc.set_flush_param(key, str(val))
        else:
            type = ClusterOperationHelper._get_engine_param_type(key)

            if val == 'true' or val == 'false':
               rv = mc.set_param(key, val, type)
            else:
               rv = mc.set_param(key, str(val), type)

        log.info("Setting flush param on server {0}, {1} to {2}, result: {3}".format(server, key, val, rv))
        mc.close()

    @staticmethod
    def _get_engine_param_type(key):
        tap_params = ['tap_keepalive', 'tap_throttle_queue_cap', 'tap_throttle_threshold']
        checkpoint_params = ['chk_max_items', 'chk_period', 'inconsistent_slave_chk', 'keep_closed_chks',
                             'max_checkpoints']
        flush_params = ['bg_fetch_delay', 'couch_response_timeout', 'exp_pager_stime', 'flushall_enabled',
                        'klog_compactor_queue_cap', 'klog_max_log_size', 'klog_max_entry_ratio',
                        'queue_age_cap', 'max_size', 'max_txn_size', 'mem_high_wat', 'mem_low_wat',
                        'min_data_age', 'timing_log', 'alog_sleep_time', 'bfilter_enabled' ]
        if key in tap_params:
            return memcacheConstants.ENGINE_PARAM_TAP
        if key in checkpoint_params:
            return memcacheConstants.ENGINE_PARAM_CHECKPOINT
        if key in flush_params:
            return memcacheConstants.ENGINE_PARAM_FLUSH

    @staticmethod
    def set_expiry_pager_sleep_time(master, bucket, value=30):
        log = logger.Logger.get_logger()
        rest = RestConnection(master)
        servers = rest.get_nodes()
        for server in servers:
            # this is not bucket specific so no need to pass in the bucketname
            log.info("connecting to memcached {0}:{1}".format(server.ip, server.memcached))
            mc = MemcachedClientHelper.direct_client(server, bucket)
            log.info("Set exp_pager_stime flush param on server {0}:{1}".format(server.ip, server.port))
            try:
                mc.set_flush_param("exp_pager_stime", str(value))
                log.info("Set exp_pager_stime flush param on server {0}:{1}".format(server.ip, server.port))
            except Exception as ex:
                traceback.print_exc()
                log.error("Unable to set exp_pager_stime flush param on memcached {0}:{1}".format(server.ip, server.memcached))

    @staticmethod
    def get_mb_stats(servers, key):
        log = logger.Logger.get_logger()
        for server in servers:
            c = MemcachedClient(server.ip, 11210)
            log.info("Get flush param on server {0}, {1}".format(server, key))
            value = c.stats().get(key, None)
            log.info("Get flush param on server {0}, {1}".format(server, value))
            c.close()

    @staticmethod
    def change_erlang_threads_values(servers, sync_threads=True, num_threads='16:16'):
        """Change the the type of sync erlang threads and its value
           sync_threads=True means sync threads +S with default threads number equal 16:16
           sync_threads=False means async threads: +A 16, for instance

        Default: +S 16:16
        """
        log = logger.Logger.get_logger()
        for server in servers:
            sh = RemoteMachineShellConnection(server)
            product = "membase"
            if sh.is_couchbase_installed():
                product = "couchbase"

            sync_type = sync_threads and "S" or "A"
            command = r"sed -i 's/+[A,S] .*/+[0] [1] \\\/g' /opt/[2]/bin/[2]-server"\
                .format(sync_type, num_threads, product)
            # command = "sed -i 's/+[A,S] .*/+%s %s \\\/g' /opt/%s/bin/%s-server" % \
            #      (sync_type, num_threads, product, product)
            o, r = sh.execute_command(command)
            sh.log_command_output(o, r)
            msg = "modified erlang +%s to %s for server %s"
            log.info(msg % (sync_type, num_threads, server.ip))

    @staticmethod
    def set_erlang_schedulers(servers, value="16:16"):
        """
        Set num of erlang schedulers.
        Also erase async option (+A)
        """
        ClusterOperationHelper.stop_cluster(servers)

        log = logger.Logger.get_logger()
        for server in servers:
            sh = RemoteMachineShellConnection(server)
            product = "membase"
            if sh.is_couchbase_installed():
                product = "couchbase"
            command = r"sed -i 's/S\+ 128:128/S [0]/' /opt/[1]/bin/[1]-server".format(value, product)
            o, r = sh.execute_command(command)
            sh.log_command_output(o, r)
            log.info("modified erlang +A to %s for server %s"
                     % (value, server.ip))

        ClusterOperationHelper.start_cluster(servers)

    @staticmethod
    def change_erlang_gc(servers, value=None):
        """Change the frequency of erlang_gc process
           export ERL_FULLSWEEP_AFTER=0 (most aggressive)

        Default: None
        """
        log = logger.Logger.get_logger()
        if value is None:
            return
        for server in servers:
            sh = RemoteMachineShellConnection(server)
            product = "membase"
            if sh.is_couchbase_installed():
                product = "couchbase"
            command = "sed -i '/exec erl/i export ERL_FULLSWEEP_AFTER=%s' /opt/%s/bin/%s-server" % \
                      (value, product, product)
            o, r = sh.execute_command(command)
            sh.log_command_output(o, r)
            msg = "modified erlang gc to full_sweep_after %s on %s " % (value, server.ip)
            log.info(msg)

    @staticmethod
    def begin_rebalance_in(master, servers, timeout=5):
        RebalanceHelper.begin_rebalance_in(master, servers, timeout)

    @staticmethod
    def begin_rebalance_out(master, servers, timeout=5):
        RebalanceHelper.begin_rebalance_out(master, servers, timeout)

    @staticmethod
    def end_rebalance(master):
        RebalanceHelper.end_rebalance(master)

    @staticmethod
    # Returns the otpNode for Orchestrator
    def find_orchestrator(master):
        rest = RestConnection(master)
        status, content = ClusterOperationHelper.find_orchestrator_with_rest(rest)
        # Get rid of single quotes 'ns_1@10.1.3.74'
        content = content.replace("'", '')
        return status, content

    @staticmethod
    def find_orchestrator_with_rest(rest):
        command = "mb_master:master_node()."
        status, content = rest.diag_eval(command)
        return status, content

    @staticmethod
    def set_vbuckets(master, vbuckets):
        rest = RestConnection(master)
        command = "rpc:eval_everywhere(ns_config, set, [couchbase_num_vbuckets_default, {0}]).".format(vbuckets)
        status, content = rest.diag_eval(command)
        return status, content

    @staticmethod
    def check_if_services_obey_tls(servers, port_map=CbServer.ssl_port_map):
        """
        Parameters:
        servers - list of servers on which to check
        port_map (optional) - a dict with key as non-ssl port
            and its value as tls-port. If not given, it will take the port map from
            CbServer.ssl_port_map

        Returns False if
        a. the non-ssl port is open on any other address other than localhost
        b. the tls port is not open on all (*) addresses
        else True
        """
        log = logger.Logger.get_logger()
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            # service should listen on non-ssl port only on localhost/no-address
            for port in port_map.keys():
                addresses = shell.get_port_recvq(port)
                for address in addresses:
                    expected_address = "127.0.0.1:" + port
                    if address != expected_address:
                        log.error("On Server {0} Expected {1} Actual {2} !!!!!!!!!!!!!!!!".
                                  format(server.ip, expected_address, address))
                        shell.disconnect()
                        return False
            # service should listen on tls_port(if there is one) for all outside addresses
            for port in port_map.keys():
                ssl_port = CbServer.ssl_port_map.get(port)
                if ssl_port is None:
                    continue
                addresses = shell.get_port_recvq(ssl_port)
                for address in addresses:
                    expected_address = ["*:" + ssl_port,
                                        "0.0.0.0:" + ssl_port]
                    if address not in expected_address:
                        log.error("On Server {0} Expected {1} Actual {2} !!!!!!!!!!!!!!!!".
                                  format(server.ip, expected_address, address))
                        shell.disconnect()
                        return False
            shell.disconnect()
        return True
