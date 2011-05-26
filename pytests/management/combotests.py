from random import shuffle
from TestInput import TestInputSingleton
import logger
import time

import unittest
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection


class ComboBaseTests(unittest.TestCase):
    # start from 1..n
    # then from no failover x node and rebalance and
    # verify we did not lose items

    @staticmethod
    def common_setup(input, testcase):
        servers = input.servers
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_membase()
        log = logger.Logger.get_logger()
        log.info("10 seconds delay to wait for membase-server to start")
        time.sleep(10)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        try:
            MemcachedClientHelper.flush_bucket(servers[0], 'default', 11211)
        except Exception:
            pass
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

    @staticmethod
    def choose_nodes(master, nodes, howmany):
        selected = []
        for node in nodes:
            if not FailoverBaseTest.contains(node.ip, master.ip) and\
               not FailoverBaseTest.contains(node.ip, '127.0.0.1'):
                selected.append(node)
                if len(selected) == howmany:
                    break
        return selected

    @staticmethod
    def contains(string1, string2):
        if string1 and string2:
            return string1.find(string2) != -1
        return False


class ComboTests(unittest.TestCase):

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        ComboBaseTests.common_setup(self._input, self)

    def tearDown(self):
        ComboBaseTests.common_tearDown(self._servers, self)

    def test_failover_normal_1_replica_1_percent(self):
        self.common_test_body(1, 'normal', 1)

    def test_failover_normal_2_replica_1_percent(self):
        self.common_test_body(2, 'normal', 1)

    def test_failover_normal_3_replica_1_percent(self):
        self.common_test_body(3, 'normal', 1)

    def test_failover_normal_1_replica_10_percent(self):
        self.common_test_body(1, 'normal', 10)

    def test_failover_normal_2_replica_10_percent(self):
        self.common_test_body(2, 'normal', 10)

    def test_failover_normal_3_replica_10_percent(self):
        self.common_test_body(3, 'normal', 10)

    def test_failover_normal_1_replica_30_percent(self):
        self.common_test_body(1, 'normal', 30)

    def test_failover_normal_2_replica_30_percent(self):
        self.common_test_body(2, 'normal', 30)

    def test_failover_normal_3_replica_30_percent(self):
        self.common_test_body(3, 'normal', 30)


    def test_failover_stop_membase_1_replica_1_percent(self):
        self.common_test_body(1, 'stop_membase', 1)

    def test_failover_stop_membase_2_replica_1_percent(self):
        self.common_test_body(2, 'stop_membase', 1)

    def test_failover_stop_membase_3_replica_1_percent(self):
        self.common_test_body(3, 'stop_membase', 1)

    def test_failover_stop_membase_1_replica_10_percent(self):
        self.common_test_body(1, 'stop_membase', 10)

    def test_failover_stop_membase_2_replica_10_percent(self):
        self.common_test_body(2, 'stop_membase', 10)

    def test_failover_stop_membase_3_replica_10_percent(self):
        self.common_test_body(3, 'stop_membase', 10)

    def test_failover_stop_membase_1_replica_30_percent(self):
        self.common_test_body(1, 'stop_membase', 30)

    def test_failover_stop_membase_2_replica_30_percent(self):
        self.common_test_body(2, 'stop_membase', 30)

    def test_failover_stop_membase_3_replica_30_percent(self):
        self.common_test_body(3, 'stop_membase', 30)


    def common_test_body(self, replica, steps, load_ratio,timeout=10):
        log = logger.Logger.get_logger()
        start_time = time.time()
        log.info("replica : {0}".format(replica))
        log.info("failover_reason : {0}".format(failover_reason))
        log.info("load_ratio : {0}".format(load_ratio))
        master = self._servers[0]
        log.info('picking server : {0} as the master'.format(master))
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket='default',
                           ramQuotaMB=bucket_ram,
                           replicaNumber=replica,
                           proxyPort=11211)
        BucketOperationHelper.wait_till_memcached_is_ready_or_assert(servers=[master],
                                                                     bucket_port=11211,
                                                                     test=self)
        credentials = self._input.membase_settings
        log.info("inserting some items in the master before adding any nodes")
        if load_ratio >= 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        else:
            distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=load_ratio,
                                          value_size_distribution=distribution,
                                          number_of_threads=40)
        log.info('inserted {0} keys'.format(inserted_count))
        ClusterOperationHelper.add_all_nodes_or_assert(master, self._servers, credentials, self)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)

        while time.time()  > ( start_time + 60 * timeout) :

            #rebalance out step nodes
            self.rebalance_in(how_many=steps)
            self.rebalance_out(how_many=steps)



    def rebalance_out(self,how_many):
        msg = "choosing three nodes and rebalance them out from the cluster"
        self.log.info(msg)
        rest = RestConnection(self._servers[0])
        nodes = rest.node_statuses()
        nodeIps = [node.ip for node in nodes]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeEjected = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            for node in nodes:
                if server.ip == node.ip:
                    toBeEjected.append(node.id)
                    break
            if len(toBeEjected) == how_many:
                break
        self.log.info("selected {0} for rebalance out from the cluster".format(toBeEjected))
        otpNodes = [node.id for node in nodes]
        started = rest.rebalance(otpNodes,toBeEjected)
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
            self.log.info(msg.format(result))
            return result
        return False

    def rebalance_in(self,how_many):
        rest = RestConnection(self._servers[0])
        nodes = rest.node_statuses()
        #choose how_many nodes from self._servers which are not part of
        # nodes
        nodeIps = [node.ip for node in nodes]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeAdded = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            if server.ip in nodeIps:
                toBeAdded.append(server)
            if len(toBeAdded) == how_many:
                break

        for server in toBeAdded:
            rest.add_node('Administrator','password',server.ip)
            #check if its added ?
        otpNodes = [node.id for node in nodes]
        started = rest.rebalance(otpNodes,[])
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
            self.log.info(msg.format(result))
            return result
        return False
