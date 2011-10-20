from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection

import logger
import testconstants
import time
import Queue
from threading import Thread


class ClusterOperationHelper(object):
    #the first ip is taken as the master ip

    @staticmethod
    def add_and_rebalance(servers,rest_password):
        log = logger.Logger.get_logger()
        master = servers[0]
        all_nodes_added = True
        rebalanced = True
        rest = RestConnection(master)
        if len(servers) > 1:
            for serverInfo in servers[1:]:
                log.info('adding node : {0} to the cluster'.format(serverInfo.ip))
                otpNode = rest.add_node("Administrator", rest_password, serverInfo.ip, port=serverInfo.port)
                if otpNode:
                    log.info('added node : {0} to the cluster'.format(otpNode.id))
                else:
                    all_nodes_added = False
                    break
            if all_nodes_added:
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                rebalanced &= rest.monitorRebalance()
        return all_nodes_added and rebalanced

    @staticmethod
    def add_all_nodes_or_assert(master,all_servers,rest_settings,test_case):
        log = logger.Logger.get_logger()
        otpNodes = []
        all_nodes_added = True
        rest = RestConnection(master)
        for serverInfo in all_servers:
            if serverInfo.ip != master.ip:
                log.info('adding node : {0} to the cluster'.format(serverInfo.ip))
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
    def wait_for_ns_servers_or_assert(servers,testcase):
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
           thread = Thread(target=MemcachedClientHelper.load_bucket,
                           name="loading thread for bucket {0}".format(bucket.name),
                           args=([master], bucket.name, -1, keys_count, None, 3, -1, True, True))

           thread.start()
           # Do persistence verification
           ready = ClusterOperationHelper.persistence_verification(servers, bucket.name, timeout_in_seconds)
           log.info("Persistence Verification returned ? {0}".format(ready))
           log.info("waiting for persistence threads to finish...")
           thread.join()
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
                log.info("Server {0} part of cluster".format(servers[i].ip))
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
        start=time.time()
        stats = []
        # Collect stats data points
        while time.time() - start <= timeout:
            stats.append(rest.get_bucket_stats(bucket)[stat_key])
            time.sleep(2)
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
    def cleanup_cluster(servers):
        log = logger.Logger.get_logger()
        rest = RestConnection(servers[0])
        RestHelper(rest).is_ns_server_running(timeout_in_seconds=testconstants.NS_SERVER_TIMEOUT)
        nodes = rest.node_statuses()
        master_id = rest.get_nodes_self().id
        if len(nodes) > 1:
                log.info("rebalancing all nodes in order to remove nodes")
                helper = RestHelper(rest)
                removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                              ejectedNodes=[node.id for node in nodes if node.id != master_id])
                log.info("removed all the nodes from cluster associated with {0} ? {1}".format(servers[0], removed))

    @staticmethod
    def flushctl_start(servers, username=None, password=None):
        for server in servers:
            c = mc_bin_client.MemcachedClient(server.ip, 11210)
            if username:
                c.sasl_auth_plain(username, password)
            c.start_persistence()

    @staticmethod
    def flushctl_stop(servers, username=None, password=None):
        for server in servers:
            c = mc_bin_client.MemcachedClient(server.ip, 11210)
            if username:
                c.sasl_auth_plain(username, password)
            c.stop_persistence()
