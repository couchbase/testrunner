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


class FailoverBaseTest(unittest.TestCase):
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


class FailoverTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        FailoverBaseTest.common_setup(self._input, self)

    def tearDown(self):
        FailoverBaseTest.common_tearDown(self._servers, self)

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


    def common_test_body(self, replica, failover_reason, load_ratio):
        log = logger.Logger.get_logger()
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
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        if load_ratio == 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        elif load_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=load_ratio,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        log.info('inserted {0} keys'.format(inserted_count))
        ClusterOperationHelper.add_all_nodes_or_assert(master, self._servers, credentials, self)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)


        nodes = rest.node_statuses()
        #while len(node) > replica * 2
        while (len(nodes) - replica) >= replica:
            chosen = FailoverBaseTest.choose_nodes(master, nodes, replica)
            for node in chosen:
                #let's do op
                if failover_reason == 'stop_membase':
                    self.stop_membase(node)
                    log.info("10 seconds delay to wait for membase-server to shutdown")
                    time.sleep(10)
                rest.fail_over(node.id)
                log.info("failed over node : {0}".format(node.id))

            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[node.id for node in chosen])
            msg="rebalance failed while removing failover nodes {0}".format(chosen)
            self.assertTrue(rest.monitorRebalance(), msg=msg)

            final_replication_state = RestHelper(rest).wait_for_replication(120)
            msg = "replication state after waiting for up to 2 minutes : {0}"
            self.log.info(msg.format(final_replication_state))

            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
            RebalanceHelper.print_taps_from_all_nodes(rest, 'default')
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], inserted_count,
                              msg=msg.format(stats["curr_items"], inserted_count))
            nodes = rest.node_statuses()

    def stop_membase(self,node):
        log = logger.Logger.get_logger()
        for server in self._servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                shell.stop_membase()
                log.info("stopped membase server on {0}".format(server))
                break


