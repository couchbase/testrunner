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


class AutoFailoverBaseTest(unittest.TestCase):
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
            if not AutoFailoverBaseTest.contains(node.ip, master.ip) and\
               not AutoFailoverBaseTest.contains(node.ip, '127.0.0.1'):
                selected.append(node)
                if len(selected) == howmany:
                    break
        return selected

    @staticmethod
    def contains(string1, string2):
        if string1 and string2:
            return string1.find(string2) != -1
        return False

    @staticmethod
    def wait_for_failover_or_assert(master, autofailover_count, age,  testcase):
        testcase.log.info("waiting for {0} seconds for autofailover".format((age + 30)))
        time.sleep(age + 30)

        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            testcase.log.info("{0} is in state {1} and {2}".format(node['hostname'],node['status'],node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        testcase.assertTrue(failover_count == autofailover_count, "{0} nodes failed over, expected {1}".format(failover_count, autofailover_count))

class AutoFailoverSettingsTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        AutoFailoverBaseTest.common_setup(self._input, self)

    def tearDown(self):
        AutoFailoverBaseTest.common_tearDown(self._servers, self)

    def test_enable(self):
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings("true", 30, 1)
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def test_disable(self):
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(False, 60, 1)
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertEquals(settings.enabled, False)

    def test_negative_timeout(self):
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(False, -60, 1)
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertTrue(settings.timeout > 0)

    def test_timeout_valid_mins(self):
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        mins = [30, 60, 90, 120, 180, 240]
        for min in mins:
            rest.update_autofailover_settings(True, 3600, 1)
            #read settings and verify
            settings = rest.get_autofailover_settings()
            self.assertEquals(settings.timeout, min)

    def test_negative_max_nodes(self):
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, 30, -1)
        #it should have
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertFalse(settings)

    def test_max_nodes_1(self):
        max_nodes = 1
        log = logger.Logger.get_logger()
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, 30, max_nodes)
        #it should have
        #read settings and verify
        rest.get_autofailover_settings()
        log.info("set max_nodes to {0}".format(max_nodes))


    def test_timeout_1_hour(self):
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, 3600, 1)
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertTrue(settings.timeout > 0)


    def _cluster_setup(self):
        bucket_name = "default"
        master = self._servers[0]
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        rest.reset_autofailover()
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket=bucket_name,
                           ramQuotaMB=bucket_ram,
                           proxyPort=info.moxi)
        ready = BucketOperationHelper.wait_for_memcached(master, bucket_name)
        self.assertTrue(ready, "wait_for_memcached failed")


class AutoFailoverTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        AutoFailoverBaseTest.common_setup(self._input, self)

    def tearDown(self):
        AutoFailoverBaseTest.common_tearDown(self._servers, self)


    def test_autofailover_firewall_1_replica_1_percent_60_age_1_node(self):
        self.common_test_body(1, 'firewall', 1, 60, 1)

    def test_autofailover_firewall_1_replica_150_percent_20_age_1_node(self):
        self.common_test_body(1, 'firewall', 150, 60, 1)

    def test_autofailover_stop_membase_2_replica_1_percent_20_age_1_node(self):
        self.common_test_body(2, 'stop_membase', 1, 60, 1)

    def test_autofailover_stop_membase_2_replica_1_percent_20_age_2_node(self):
        self.common_test_body(2, 'stop_membase', 1, 60, 2)

    def test_autofailover_stop_membase_2_replica_150_percent_20_age_1_node(self):
        self.common_test_body(2, 'stop_membase', 150, 60, 1)

    def test_autofailover_stop_membase_1_replica_1_percent_60_age_1_node(self):
        self.common_test_body(1, 'stop_membase', 1, 60, 1)

    def test_autofailover_stop_membase_2_replica_1_percent_0_age_1_node(self):
        self.common_test_body(2, 'stop_membase', 1, 0, 1)

    def test_autofailover_stop_membase_2_replica_150_percent_0_age_1_node(self):
        self.common_test_body(2, 'stop_membase', 150, 0, 1)

    def test_autofailover_stop_membase_3_replica_1_percent_20_age_3_node(self):
        self.common_test_body(3, 'stop_membase', 1, 60, 3)


    def common_test_body(self, replica, failover_reason, load_ratio, age, max_nodes):
        log = logger.Logger.get_logger()
        bucket_name = "default"
        log.info("replica : {0}".format(replica))
        log.info("failover_reason : {0}".format(failover_reason))
        log.info("load_ratio : {0}".format(load_ratio))
        log.info("age : {0}".format(age))
        log.info("max_nodes : {0}".format(max_nodes))
        master = self._servers[0]
        log.info('picking server : {0} as the master'.format(master))
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        rest.update_autofailover_settings(True, age, max_nodes)
        rest.reset_autofailover()
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket=bucket_name,
                           ramQuotaMB=bucket_ram,
                           replicaNumber=replica,
                           proxyPort=info.moxi)
        ready = BucketOperationHelper.wait_for_memcached(master, bucket_name)
        self.assertTrue(ready, "wait_for_memcached failed")

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
        MemcachedClientHelper.load_bucket(servers=self._servers,
                                          ram_load_ratio=load_ratio,
                                          value_size_distribution=distribution,
                                          number_of_threads=1)
        log.info('inserted {0} keys'.format(inserted_count))
        nodes = rest.node_statuses()
        # why are we in this while loop?
        while (len(nodes) - replica) >= 1:
            final_replication_state = RestHelper(rest).wait_for_replication(900)
            msg = "replication state after waiting for up to 15 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            chosen = AutoFailoverBaseTest.choose_nodes(master, nodes, replica)
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
            # list pre-autofailover stats
            stats = rest.get_bucket_stats()
            self.log.info("pre-autofail - curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
            AutoFailoverBaseTest.wait_for_failover_or_assert(master, replica, age, self)

            # manually fail over any unhealthy:active nodes left, max that we should need to manually failover is replica-max_nodes
            manual_failover_count = replica - max_nodes
            for node in chosen:
                self.log.info("checking {0}".format(node.ip))
                if node.status.lower() == "unhealthy" and node.clusterMembership == "active":
                    msg = "node {0} not failed over and we are over out manual failover limit of {1}"
                    self.assertTrue(manual_failover_count > 0, msg.format(node.ip, (replica - max_nodes)))
                    self.log.info("manual failover {0}".format(node.ip))
                    rest.fail_over(node.id)
                    manual_failover_count -= 1

            stats = rest.get_bucket_stats()
            self.log.info("post-autofail - curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
            self.assertTrue(stats["curr_items"] == inserted_count, "failover completed but curr_items ({0}) does not match inserted items ({1})".format(stats["curr_items"], inserted_count))

            log.info("10 seconds sleep after autofailover before invoking rebalance...")
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
                self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master,bucket_name,600),
                                msg="replication was completed but sum(curr_items) dont match the curr_items_total")

                start_time = time.time()
                stats = rest.get_bucket_stats()
                while time.time() < (start_time + 120) and stats["curr_items"] != inserted_count:
                    self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
                    time.sleep(5)
                    stats = rest.get_bucket_stats()
                RebalanceHelper.print_taps_from_all_nodes(rest, bucket_name)
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
