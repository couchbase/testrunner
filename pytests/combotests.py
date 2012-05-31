from random import shuffle, Random
from TestInput import TestInputSingleton
import logger
import time

import unittest
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
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


    def test_loop(self):
        duration = 240
        replica = 1
        load_ratio = 5
        if 'duration' in self._input.test_params:
            duration = int(self._input.test_params['duration'])
        if 'load_ratio' in self._input.test_params:
            load_ratio = int(self._input.test_params['load_ratio'])
        if 'replica' in self._input.test_params:
            replica = int(self._input.test_params['replica'])
        self.common_test_body(replica, load_ratio, duration)

    def common_test_body(self, replica, load_ratio, timeout=10):
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
        json_bucket = {'name': 'default', 'port': 11211, 'password': ''}
        BucketOperationHelper.wait_for_memcached(master, json_bucket)
        log.info("inserting some items in the master before adding any nodes")
        distribution = {1024: 0.4, 2 * 1024: 0.5, 512: 0.1}
        threads = MemcachedClientHelper.create_threads(servers=[master],
                                                       value_size_distribution=distribution,
                                                       number_of_threads=len(self._servers),
                                                       number_of_items=400000000,
                                                       moxi=False,
                                                       write_only=True,
                                                       async_write=True)
        for thread in threads:
            thread.terminate_in_minutes = 24 * 60
            thread.start()
        while time.time() < ( start_time + 60 * timeout):
            #rebalance out step nodes
            #let's add some items ?
            nodes = rest.node_statuses()
            delta = len(self._servers) - len(nodes)
            if delta > 0:
                if delta > 1:
                    how_many_add = Random().randint(1, delta)
                else:
                    how_many_add = 1
                self.log.info("going to add {0} nodes".format(how_many_add))
                self.rebalance_in(how_many=how_many_add)
            else:
                self.log.info("all nodes already joined the clustr")
            time.sleep(240)
            RestHelper(rest).wait_for_replication(600)
            #dont rebalance out if there are not too many nodes
            if len(nodes) >= (3.0 / 4.0 * len(self._servers)):
                nodes = rest.node_statuses()
                how_many_out = Random().randint(1, len(nodes) - 1)
                self.log.info("going to remove {0} nodes".format(how_many_out))
                self.rebalance_out(how_many=how_many_out)

        for t in threads:
            t.aborted = True
            t.join()

    def rebalance_out(self, how_many):
        msg = "choosing three nodes and rebalance them out from the cluster"
        self.log.info(msg)
        rest = RestConnection(self._servers[0])
        nodes = rest.node_statuses()
        nodeIps = [node.ip for node in nodes]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeEjected = []
        toBeEjectedServers = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            for node in nodes:
                if server.ip == node.ip:
                    toBeEjected.append(node.id)
                    toBeEjectedServers.append(server)
                    break
            if len(toBeEjected) == how_many:
                break
        if len(toBeEjected) > 0:
            self.log.info("selected {0} for rebalance out from the cluster".format(toBeEjected))
            otpNodes = [node.id for node in nodes]
            started = rest.rebalance(otpNodes, toBeEjected)
            msg = "rebalance operation started ? {0}"
            self.log.info(msg.format(started))
            if started:
                result = rest.monitorRebalance()
                msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
                self.log.info(msg.format(result))
                for server in toBeEjectedServers:
                    shell = RemoteMachineShellConnection(server)
                    try:
                        shell.stop_membase()
                    except:
                        pass
                    try:
                        shell.start_membase()
                    except:
                        pass
                    shell.disconnect()
                    RestHelper(RestConnection(server)).is_ns_server_running()
                    #let's restart membase on those nodes
                return result
        return True

    def rebalance_in(self, how_many):
        return RebalanceHelper.rebalance_in(self._servers, how_many)
