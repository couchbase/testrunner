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
from membase.helper.rebalance_helper import RebalanceHelper

class AutoFailoverBaseTest(unittest.TestCase):
    # start from 1..n
    # then from no failover x node and rebalance and
    # verify we did not lose items

    # maximum time we allow ns_server to take to detect a failed node
    MAX_FAIL_DETECT_TIME = 60

    @staticmethod
    def common_setup(input, testcase):
        log = logger.Logger.get_logger()
        log.info("==============  common_setup was started for test #{0} {1}=============="\
                      .format(testcase.case_number, testcase._testMethodName))
        servers = input.servers
        RemoteUtilHelper.common_basic_setup(servers)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        log.info("==============  common_setup was finished for test #{0} {1} =============="\
                      .format(testcase.case_number, testcase._testMethodName))

    @staticmethod
    def common_tearDown(servers, testcase):
        log = logger.Logger.get_logger()
        log.info("==============  common_tearDown was started for test #{0} {1} =============="\
                          .format(testcase.case_number, testcase._testMethodName))
        RemoteUtilHelper.common_basic_setup(servers)

        log.info("10 seconds delay to wait for couchbase-server to start")
        time.sleep(10)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase, \
                wait_time=AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME * 15, wait_if_warmup=True)
        try:
            rest = RestConnection(self._servers[0])
            buckets = rest.get_buckets()
            for bucket in buckets:
                MemcachedClientHelper.flush_bucket(servers[0], bucket.name)
        except Exception:
            pass
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        log.info("==============  common_tearDown was finished for test #{0} {1} =============="\
                          .format(testcase.case_number, testcase._testMethodName))

    @staticmethod
    def wait_for_failover_or_assert(master, autofailover_count, timeout, testcase):
        time_start = time.time()
        time_max_end = time_start + timeout + 120
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = AutoFailoverBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        if failover_count != autofailover_count:
            rest = RestConnection(master)
            testcase.log.info("Latest logs from UI:")
            for i in rest.get_logs(): testcase.log.error(i)
            testcase.log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
            testcase.fail("{0} nodes failed over, expected {1} in {2} seconds".
                            format(failover_count, autofailover_count, time.time() - time_start))
        else:
            testcase.log.info("{0} nodes failed over as expected".format(failover_count))

    @staticmethod
    def wait_for_no_failover_or_assert(master, autofailover_count, timeout, testcase):
        log = logger.Logger.get_logger()
        time_start = time.time()
        time_max_end = time_start + timeout + 120
        failover_count = 0

        while time.time() < time_max_end:
            failover_count = AutoFailoverBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        time_end = time.time()

        testcase.assertFalse(failover_count == autofailover_count, "{0} nodes failed over, didn't expect {1} in {2} seconds".
                             format(failover_count, autofailover_count, time.time() - time_start))
        log.info("{0} nodes failed over as expected in {1} seconds".format(failover_count, time_end - time_start))

    @staticmethod
    def get_failover_count(master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()
        log = logger.Logger.get_logger()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count


class AutoFailoverTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self.case_number = self._input.param("case_number", 0)
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        self.master = self._servers[0]
        self.rest = RestConnection(self.master)
        self.timeout = 60

        AutoFailoverBaseTest.common_setup(self._input, self)
        self._cluster_setup()

    def tearDown(self):
        AutoFailoverBaseTest.common_tearDown(self._servers, self)


    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def test_enable(self):
        # AUTOFAIL_TEST_1
        status = self.rest.update_autofailover_settings(True, self.timeout / 2)
        if not status:
            self.fail('failed to change autofailover_settings!')
        #read settings and verify
        settings = self.rest.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def test_disable(self):
        # AUTOFAIL_TEST_2
        status = self.rest.update_autofailover_settings(False, self.timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        #read settings and verify
        settings = self.rest.get_autofailover_settings()
        self.assertEquals(settings.enabled, False)

    def test_valid_timeouts(self):
        # AUTOFAIL_TEST_3
        timeouts = [30, 31, 300, 3600, 300000]
        for timeout in timeouts:
            status = self.rest.update_autofailover_settings(True, timeout)
            if not status:
                self.fail('failed to change autofailover_settings!')
            #read settings and verify
            settings = self.rest.get_autofailover_settings()
            self.assertTrue(settings.timeout == timeout)

    def test_30s_timeout_firewall(self):
        # AUTOFAIL_TEST_4
        timeout = self.timeout / 2
        server_fail = self._servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        time_start = time.time()
        RemoteUtilHelper.enable_firewall(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

    def test_60s_timeout_firewall(self):
        # AUTOFAIL_TEST_5
        timeout = self.timeout
        server_fail = self._servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        time_start = time.time()
        RemoteUtilHelper.enable_firewall(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

    def test_30s_timeout_stop(self):
        # AUTOFAIL_TEST_6
        timeout = self.timeout
        server_fail = self._servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        time_start = time.time()
        self._stop_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

    def test_60s_timeout_stop(self):
        # AUTOFAIL_TEST_7
        timeout = self.timeout
        server_fail = self._servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        time_start = time.time()
        self._stop_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

    def test_reset_count(self):
        # AUTOFAIL_TEST_8
        timeout = self.timeout / 2
        server_fail1 = self._servers[1]
        server_fail2 = self._servers[2]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        self.log.info("stopping the first server")
        time_start = time.time()
        self._stop_couchbase(server_fail1)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

        self.log.info("resetting the autofailover count")
        status = self.rest.reset_autofailover()
        if not status:
            self.fail('failed to reset autofailover count!')

        self.log.info("stopping the second server")
        time_start = time.time()
        self._stop_couchbase(server_fail2)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

        self.log.info("resetting the autofailover count")
        status = self.rest.reset_autofailover()
        if not status:
            self.fail('failed to reset autofailover count!')

    def test_30s_timeout_pause(self):
        # AUTOFAIL_TEST_9
        timeout = self.timeout / 2
        server_fail = self._servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        time_start = time.time()
        self._pause_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

    def test_60s_timeout_pause(self):
        # AUTOFAIL_TEST_10
        timeout = self.timeout
        server_fail = self._servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        time_start = time.time()
        self._pause_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))


    def test_invalid_timeouts(self):
        # AUTOFAIL_TESTN_1
        timeouts = [-360, -60, 0, 15, 29]
        for timeout in timeouts:
            status = self.rest.update_autofailover_settings(True, timeout)
            if status:
                self.fail('autofailover_settings have been changed incorrectly!')
            #read settings and verify
            settings = self.rest.get_autofailover_settings()
            self.assertTrue(settings.timeout >= 30)

    def test_two_failed_nodes(self):
        # AUTOFAIL_TESTN_4
        timeout = self.timeout / 2
        server_fail1 = self._servers[1]
        server_fail2 = self._servers[2]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings!')
        self.sleep(5)
        self.log.info("stopping the first server")
        time_start = time.time()
        self._stop_couchbase(server_fail1)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout, self)
        time_end = time.time()
        msg = "{0} != {1}".format(time_end - time_start, timeout)
        self.assertTrue(abs((time_end - time_start) - timeout) <= AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, msg)
        self.log.info("expected failover in {0} seconds, actual time {1} seconds".format(timeout, time_end - time_start))

        self.log.info("stopping the second server")
        time_start = time.time()
        self._stop_couchbase(server_fail2)
        AutoFailoverBaseTest.wait_for_no_failover_or_assert(self.master, 2, timeout, self)

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

    def load_data(self, master, bucket, keys_count):
        log = logger.Logger.get_logger()
        inserted_keys_cnt = 0
        while inserted_keys_cnt < keys_count:
            keys_cnt, rejected_keys_cnt = \
            MemcachedClientHelper.load_bucket(servers=[master],
                name=bucket,
                number_of_items=keys_count,
                number_of_threads=5,
                write_only=True)
            inserted_keys_cnt += keys_cnt
        log.info("wait until data is completely persisted on the disk")
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0)
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0)
        return inserted_keys_cnt

    def _cluster_setup(self):
        log = logger.Logger.get_logger()

        replicas = self._input.param("replicas", 1)
        keys_count = self._input.param("keys-count", 0)
        num_buckets = self._input.param("num-buckets", 1)

        bucket_name = "default"
        master = self._servers[0]
        credentials = self._input.membase_settings
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        rest.reset_autofailover()
        ClusterOperationHelper.add_all_nodes_or_assert(self.master, self._servers, credentials, self)
        bucket_ram = info.memoryQuota * 2 / 3

        if num_buckets == 1:
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=replicas,
                               proxyPort=info.moxi)
        else:
            created = BucketOperationHelper.create_multiple_buckets(self.master, replicas, howmany=num_buckets)
            self.assertTrue(created, "unable to create multiple buckets")

        buckets = rest.get_buckets()
        for bucket in buckets:
                ready = BucketOperationHelper.wait_for_memcached(self.master, bucket.name)
                self.assertTrue(ready, msg="wait_for_memcached failed")
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])


        for bucket in buckets:
            inserted_keys_cnt = self.load_data(self.master, bucket.name, keys_count)
            log.info('inserted {0} keys'.format(inserted_keys_cnt))

        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)
        self.assertTrue(ready, "wait_for_memcached failed")
