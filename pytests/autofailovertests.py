from TestInput import TestInputSingleton
import logger
import time

import unittest
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper


class AutoFailoverBaseTest(unittest.TestCase):
    # start from 1..n
    # then from no failover x node and rebalance and
    # verify we did not lose items

    # maximum time we allow ns_server to take to detect a failed node
    MAX_FAIL_DETECT_TIME = 5

    @staticmethod
    def common_setup(input, testcase):
        servers = input.servers
        RemoteUtilHelper.common_basic_setup(servers)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        RemoteUtilHelper.common_basic_setup(servers)
        log = logger.Logger.get_logger()
        log.info("10 seconds delay to wait for couchbase-server to start")
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
    def wait_for_failover_or_assert(master, autofailover_count, timeout, testcase):
        time_start = time.time()
        time_max_end = time_start + timeout + 60
        failover_count = 0

        while time.time() < time_max_end:
            failover_count = AutoFailoverBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        time_end = time.time()

        testcase.assertTrue(failover_count == autofailover_count, "{0} nodes failed over, expected {1} in {2} seconds".format(failover_count, autofailover_count, time_end - time_start))

    @staticmethod
    def wait_for_no_failover_or_assert(master, autofailover_count, timeout, testcase):
        log = logger.Logger.get_logger()

        time_start = time.time()
        time_max_end = time_start + timeout + 60
        failover_count = 0

        while time.time() < time_max_end:
            failover_count = AutoFailoverBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        time_end = time.time()

        testcase.assertFalse(failover_count == autofailover_count, "{0} nodes failed over, didn't expect {1} in {2} seconds".format(failover_count, autofailover_count, time_end - time_start))
        log.info("{0} nodes failed over, didn't expect {1} in {2} seconds".format(failover_count, autofailover_count, time_end - time_start))

    @staticmethod
    def get_failover_count(master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count


class AutoFailoverTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        AutoFailoverBaseTest.common_setup(self._input, self)

    def tearDown(self):
        AutoFailoverBaseTest.common_tearDown(self._servers, self)

    def test_enable(self):
        # AUTOFAIL_TEST_1
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, 30)
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def test_disable(self):
        # AUTOFAIL_TEST_2
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        rest.update_autofailover_settings(False, 60)
        #read settings and verify
        settings = rest.get_autofailover_settings()
        self.assertEquals(settings.enabled, False)

    def test_valid_timeouts(self):
        # AUTOFAIL_TEST_3
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        timeouts = [30, 31, 300, 3600, 300000]
        for timeout in timeouts:
            rest.update_autofailover_settings(True, timeout)
            #read settings and verify
            settings = rest.get_autofailover_settings()
            self.assertTrue(settings.timeout==timeout)

    def test_30s_timeout_firewall(self):
        # AUTOFAIL_TEST_4
        timeout = 30
        self._cluster_setup()
        master = self._servers[0]
        server_fail = self._servers[1]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)
        time_start = time.time()
        self._enable_firewall(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

    def test_60s_timeout_firewall(self):
        # AUTOFAIL_TEST_5
        timeout = 60
        self._cluster_setup()
        master = self._servers[0]
        server_fail = self._servers[1]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)
        time_start = time.time()
        self._enable_firewall(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

    def test_30s_timeout_stop(self):
        # AUTOFAIL_TEST_6
        timeout = 60
        self._cluster_setup()
        master = self._servers[0]
        server_fail = self._servers[1]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)
        time_start = time.time()
        self._stop_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

    def test_60s_timeout_stop(self):
        # AUTOFAIL_TEST_7
        timeout = 60
        self._cluster_setup()
        master = self._servers[0]
        server_fail = self._servers[1]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)
        time_start = time.time()
        self._stop_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

    def test_reset_count(self):
        # AUTOFAIL_TEST_8
        timeout = 30
        self._cluster_setup()
        master = self._servers[0]
        server_fail1 = self._servers[1]
        server_fail2 = self._servers[2]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)

        self.log.info("stopping the first server")
        time_start = time.time()
        self._stop_couchbase(server_fail1)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

        self.log.info("resetting the autofailover count")
        rest.reset_autofailover()

        self.log.info("stopping the second server")
        time_start = time.time()
        self._stop_couchbase(server_fail2)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 2, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

    def test_30s_timeout_pause(self):
        # AUTOFAIL_TEST_9
        timeout = 30
        self._cluster_setup()
        master = self._servers[0]
        server_fail = self._servers[1]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)
        time_start = time.time()
        self._pause_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

    def test_60s_timeout_pause(self):
        # AUTOFAIL_TEST_10
        timeout = 60
        self._cluster_setup()
        master = self._servers[0]
        server_fail = self._servers[1]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)
        time_start = time.time()
        self._pause_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))


    def test_invalid_timeouts(self):
        # AUTOFAIL_TESTN_1
        self._cluster_setup()
        master = self._servers[0]
        rest = RestConnection(master)
        timeouts = [-360, -60, 0, 15, 29]
        for timeout in timeouts:
            rest.update_autofailover_settings(True, timeout)
            #read settings and verify
            settings = rest.get_autofailover_settings()
            self.assertTrue(settings.timeout >= 30)

    def test_two_failed_nodes(self):
        # AUTOFAIL_TESTN_4
        timeout = 30
        self._cluster_setup()
        master = self._servers[0]
        server_fail1 = self._servers[1]
        server_fail2 = self._servers[2]
        rest = RestConnection(master)
        rest.update_autofailover_settings(True, timeout)

        self.log.info("stopping the first server")
        time_start = time.time()
        self._stop_couchbase(server_fail1)
        AutoFailoverBaseTest.wait_for_failover_or_assert(master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end-time_start, timeout)
        self.assertTrue(abs((time_end-time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end-time_start))

        self.log.info("stopping the second server")
        time_start = time.time()
        self._stop_couchbase(server_fail2)
        AutoFailoverBaseTest.wait_for_no_failover_or_assert(master, 2, timeout, self)

    def _stop_couchbase(self, server):
        log = logger.Logger.get_logger()
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    def _pause_couchbase(self, server):
        self._pause_beam(server)
        self._pause_memcached(server)

    def _pause_memcached(self, server):
        log = logger.Logger.get_logger()
        shell = RemoteMachineShellConnection(server)
        shell.pause_memcached()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    def _pause_beam(self, server):
        log = logger.Logger.get_logger()
        shell = RemoteMachineShellConnection(server)
        shell.pause_beam()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    def _enable_firewall(self, server):
        log = logger.Logger.get_logger()
        shell = RemoteMachineShellConnection(server)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j REJECT")
        shell.log_command_output(o, r)
        log.info("enabled firewall on {0}".format(server))
        o,r = shell.execute_command("/sbin/iptables --list")
        shell.log_command_output(o, r)
        shell.disconnect()

    def _cluster_setup(self):
        bucket_name = "default"
        master = self._servers[0]
        credentials = self._input.membase_settings
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        rest.reset_autofailover()
        ClusterOperationHelper.add_all_nodes_or_assert(master, self._servers, credentials, self)
        bucket_ram = info.memoryQuota * 2 / 3
        rest.create_bucket(bucket=bucket_name,
                           ramQuotaMB=bucket_ram,
                           proxyPort=info.moxi)
        ready = BucketOperationHelper.wait_for_memcached(master, bucket_name)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)
        self.assertTrue(ready, "wait_for_memcached failed")
