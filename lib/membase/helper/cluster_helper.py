from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.exception import ServerAlreadyJoinedException

import logger
import testconstants
import time
import Queue
from threading import Thread
import traceback


class ClusterOperationHelper(object):
    #the first ip is taken as the master ip

    # Returns True if cluster successfully finished ther rebalance
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
                otpNode = rest.add_node(master.rest_username, master.rest_password, serverInfo.ip, port=serverInfo.port)
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

    # For a clearer API
    @staticmethod
    def remove_and_rebalance(servers, wait_for_rebalance=True):
        return ClusterOperationHelper.cleanup_cluster(
            servers, wait_for_rebalance)

    @staticmethod
    def add_all_nodes_or_assert(master, all_servers, rest_settings, test_case):
        log = logger.Logger.get_logger()
        otpNodes = []
        all_nodes_added = True
        rest = RestConnection(master)
        for serverInfo in all_servers:
            if serverInfo.ip != master.ip:
                log.info('adding node : {0}:{1} to the cluster'.format(
                        serverInfo.ip, serverInfo.port))
                otpNode = rest.add_node(rest_settings.rest_username,
                                        rest_settings.rest_password,
                                        serverInfo.ip)
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


    @staticmethod
    def wait_for_ns_servers_or_assert(servers, testcase):
        for server in servers:
            rest = RestConnection(server)
            log = logger.Logger.get_logger()
            log.info("waiting for ns_server @ {0}:{1}".format(server.ip, server.port))
            testcase.assertTrue(RestHelper(rest).is_ns_server_running(),
                                "ns_server is not running in {0}".format(server.ip))

    @staticmethod
    def verify_persistence(servers, test, keys_count=400000, timeout_in_seconds=300):
        log = logger.Logger.get_logger()
        master = servers[0]
        rest = RestConnection(master)
        log.info("Verifying Persistence")
        buckets = rest.get_buckets()
        for bucket in buckets:
        #Load some data
            l_threads = MemcachedClientHelper.create_threads([master], bucket.name,
                                                                     -1, keys_count, {1024: 0.50, 512: 0.50}, 2, -1,
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
        queue = Queue.Queue()
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
            if shell.is_membase_installed():
                shell.start_membase()
            else:
                shell.start_couchbase()

    @staticmethod
    def stop_cluster(servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            if shell.is_membase_installed():
                shell.stop_membase()
            else:
                shell.stop_couchbase()

    @staticmethod
    def cleanup_cluster(servers, wait_for_rebalance=True):
        log = logger.Logger.get_logger()
        rest = RestConnection(servers[0])
        helper = RestHelper(rest)
        helper.is_ns_server_running(timeout_in_seconds=testconstants.NS_SERVER_TIMEOUT)
        nodes = rest.node_statuses()
        master_id = rest.get_nodes_self().id
        if len(nodes) > 1:
            log.info("rebalancing all nodes in order to remove nodes")
            removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                          ejectedNodes=[node.id for node in nodes if node.id != master_id],
                                          wait_for_rebalance=wait_for_rebalance)
            log.info("removed all the nodes from cluster associated with {0} ? {1}".format(servers[0], removed))

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
    def flushctl_set(servers, key, val):
        log = logger.Logger.get_logger()
        for server in servers:
            c = MemcachedClient(server.ip, 11210)
            log.info("Setting flush param on server {0}, {1} to {2}".format(server, key, val))
            rv = c.set_flush_param(key, val)
            log.info("Setting flush param on server {0}, {1} to {2}, result: {3}".format(server, key, val, rv))
            c.close()


    @staticmethod
    def set_expiry_pager_sleep_time(master, bucket, value=30):
        log = logger.Logger.get_logger()
        rest = RestConnection(master)
        servers = rest.get_nodes()
        for server in servers:
            #this is not bucket specific so no need to pass in the bucketname
            log.info("connecting to memcached {0}:{1}".format(server.ip, server.memcached))
            mc = MemcachedClientHelper.direct_client(server, bucket)
            log.info("Set exp_pager_stime flush param on server {0}:{1}".format(server.ip, server.port))
            try:
                mc.set_flush_param("exp_pager_stime", str(value))
                log.info("Set exp_pager_stime flush param on server {0}:{1}".format(server.ip, server.port))
            except Exception as ex:
                traceback.print_exc()
                log.error("Unable to set exp_pager_stime flush param on server {0}:{1}".format(server.ip, server.port))

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
    def change_erlang_async(servers, original, modified):
        log = logger.Logger.get_logger()
        for server in servers:
            sh = RemoteMachineShellConnection(server)
            command = "sed -i 's/+A {0}/+A {1}/g' /opt/couchbase/bin/membase-server".format(original, modified)
            o, r = sh.execute_command(command)
            sh.log_command_output(o, r)
            msg = "modified erlang +A from {0} to {1} for server {2}"
            log.info(msg.format(original, modified, server.ip))

    @staticmethod
    def begin_rebalance_in(master, servers, timeout=5):
        log = logger.Logger.get_logger()
        rest = RestConnection(master)
        otpNode = None

        for server in servers:
            if server == master:
                continue
            log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            try:
                otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
                msg = "unable to add node {0}:{1} to the cluster"
                assert otpNode, msg.format(server.ip, server.port)
            except ServerAlreadyJoinedException:
                log.info("server {0} already joined".format(server))
        log.info("beginning rebalance in")
        try:
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        except:
            log.error("rebalance failed, trying again after {0} seconds".format(timeout))
            time.sleep(timeout)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

    @staticmethod
    def begin_rebalance_out(master, servers, timeout=5):
        log = logger.Logger.get_logger()
        rest = RestConnection(master)

        master_node = rest.get_nodes_self()

        allNodes = []
        ejectedNodes = []
        nodes = rest.node_statuses()
        for server in servers:
            server_node = RestConnection(server).get_nodes_self()
            if server_node == master_node:
                continue
            log.info("removing node {0}:{1} from cluster".format(server_node.ip, server_node.port))
            for node in nodes:
                if "{0}:{1}".format(node.ip, node.port) == "{0}:{1}".format(server_node.ip, server_node.port):
                    ejectedNodes.append(node.id)
        log.info("beginning rebalance out")
        try:
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
        except:
            log.error("rebalance failed, trying again after {0} seconds".format(timeout))
            time.sleep(timeout)
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

    @staticmethod
    def end_rebalance(master):
        log = logger.Logger.get_logger()
        rest = RestConnection(master)
        assert rest.monitorRebalance(), "rebalance operation failed after adding nodes"
        log.info("rebalance finished")
