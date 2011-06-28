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
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_membase()
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            shell.disconnect()
        time.sleep(10)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_membase()
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            shell.disconnect()
            #also flush the firewall rules
        log = logger.Logger.get_logger()
        log.info("10 seconds delay to wait for membase-server to start")
        time.sleep(10)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        try:
            MemcachedClientHelper.flush_bucket(servers[0], 'default')
        except Exception:
            pass
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)

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

    def test_failover_firewall_1_replica_1_percent(self):
        self.common_test_body(1, 'firewall', 1)

    def test_failover_firewall_1_replica_10_percent(self):
        self.common_test_body(1, 'firewall', 10)


    def test_failover_firewall_2_replica_1_percent(self):
        self.common_test_body(2, 'firewall', 1)

    def test_failover_firewall_3_replica_1_percent(self):
        self.common_test_body(3, 'firewall', 1)

    def test_failover_firewall_3_replica_10_percent(self):
        self.common_test_body(3, 'firewall', 10)


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
                           proxyPort=info.moxi)
        ready = BucketOperationHelper.wait_for_memcached(master,"default")
        self.assertTrue(ready,"wait_for_memcached_failed")

        credentials = self._input.membase_settings

        log.info("inserting some items in the master before adding any nodes")
        distribution = {512: 0.4, 1 * 1024: 0.59, 5 * 1024: 0.01}
        if load_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}

        ClusterOperationHelper.add_all_nodes_or_assert(master, self._servers, credentials, self)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)


        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(servers=[master],
                                          ram_load_ratio=load_ratio,
                                          value_size_distribution=distribution,
                                          number_of_threads=20,
                                          moxi=False)
        log.info('inserted {0} keys'.format(inserted_count))
        nodes = rest.node_statuses()
        #while len(node) > replica * 2
        while (len(nodes) - replica) >= 1:
            final_replication_state = RestHelper(rest).wait_for_replication(900)
            msg = "replication state after waiting for up to 15 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            chosen = FailoverBaseTest.choose_nodes(master, nodes, replica)
            for node in chosen:
                #let's do op
                if failover_reason == 'stop_membase':
                    self.stop_membase(node)
                    log.info("10 seconds delay to wait for membase-server to shutdown")
                    #wait for 5 minutes until node is down
                    self.assertTrue(RestHelper(rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")
                elif failover_reason == "firewall":
                    self.enable_firewall(node)
                    self.assertTrue(RestHelper(rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")

                failed_over = rest.fail_over(node.id)
                if not failed_over:
                    self.log.info("unable to failover the node the first time. try again in  60 seconds..")
                    #try again in 60 seconds
                    time.sleep(75)
                    failed_over = rest.fail_over(node.id)
                self.assertTrue(failed_over, "unable to failover node after {0}".format(failover_reason))
                log.info("failed over node : {0}".format(node.id))
            #REMOVEME -
            log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[node.id for node in chosen])
            msg="rebalance failed while removing failover nodes {0}".format(chosen)
            self.assertTrue(rest.monitorRebalance(), msg=msg)


            nodes = rest.node_statuses()
            if len(nodes) / (1 + replica) >= 1:
                final_replication_state = RestHelper(rest).wait_for_replication(900)
                msg = "replication state after waiting for up to 15 minutes : {0}"
                self.log.info(msg.format(final_replication_state))
                self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                                              bucket='default',
                                                                              timeout_in_seconds=600),
                                msg="replication was completed but sum(curr_items) dont match the curr_items_total")

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
                shell.disconnect()
                log.info("stopped membase server on {0}".format(server))
                break

#iptables -A INPUT -p tcp -i eth0 --dport 1000:20000 -j REJECT

    def enable_firewall(self,node):
        log = logger.Logger.get_logger()
        for server in self._servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                o,r = shell.execute_command("iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j REJECT")
                shell.log_command_output(o, r)
                log.info("enabled firewall on {0}".format(server))
                o,r = shell.execute_command("iptables --list")
                shell.log_command_output(o, r)
                shell.disconnect()
                break
