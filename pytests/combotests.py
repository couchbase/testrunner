from random import shuffle
from TestInput import TestInputSingleton
import logger
import time
import threading

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
            if not ComboBaseTests.contains(node.ip, master.ip) and\
               not ComboBaseTests.contains(node.ip, '127.0.0.1'):
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


    def loop(self):
        duration = 240
        replica = 1
        step = 1
        if 'duration' in self._input.test_params:
            duration = int(self._input.test_params['duration'])
        if 'step' in self._input.test_params:
            step = int(self._input.test_params['step'])
        if 'replica' in self._input.test_params:
            replica = int(self._input.test_params['replica'])
        self.common_test_body(replica, step, 5, duration)

    def common_test_body(self, replica, steps, load_ratio,timeout=10):
        log = logger.Logger.get_logger()
        start_time = time.time()
        log.info("replica : {0}".format(replica))
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
        json_bucket = {'name':'default','port':11211,'password':''}
        BucketOperationHelper.wait_for_memcached(master, json_bucket)
        log.info("inserting some items in the master before adding any nodes")
        distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        threads = MemcachedClientHelper.create_threads(servers=[master],
                                                       ram_load_ratio=load_ratio,
                                                       value_size_distribution=distribution,
                                                       number_of_threads=20)
        for thread in threads:
            thread.start()
        while time.time() < ( start_time + 60 * timeout):
            #rebalance out step nodes
            self.rebalance_in(how_many=steps)
            self.rebalance_out(how_many=steps)
            [t.join() for t in threads]
            threads = MemcachedClientHelper.create_threads(servers=[master],
                                                           ram_load_ratio=10,
                                                           value_size_distribution=distribution,
                                                           number_of_threads=20)


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
            if not server.ip in nodeIps:
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
