from TestInput import TestInputSingleton
import logger
import time
import unittest
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from security.rbac_base import RbacBase



log = logger.Logger.get_logger()

class AutoFailoverBaseTest(unittest.TestCase):
    # start from 1..n
    # then from no failover x node and rebalance and
    # verify we did not lose items

    # maximum time we allow ns_server to take to detect a failed node
    # timeout as workaround for MB-7863
    MAX_FAIL_DETECT_TIME = 120

    @staticmethod
    def common_setup(input, testcase):
        log.info("==============  common_setup was started for test #{0} {1}=============="\
                      .format(testcase.case_number, testcase._testMethodName))
        servers = input.servers
        RemoteUtilHelper.common_basic_setup(servers)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)

        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', servers[0])
        
        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(servers[0]), 'builtin')
        
        log.info("==============  common_setup was finished for test #{0} {1} =============="\
                      .format(testcase.case_number, testcase._testMethodName))

    @staticmethod
    def common_tearDown(servers, testcase):
        log.info("==============  common_tearDown was started for test #{0} {1} =============="\
                          .format(testcase.case_number, testcase._testMethodName))
        RemoteUtilHelper.common_basic_setup(servers)

        log.info("10 seconds delay to wait for couchbase-server to start")
        time.sleep(10)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase, \
                wait_time=AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME * 10, wait_if_warmup=True)
        try:
            rest = RestConnection(servers[0])
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
        time_max_end = time_start + timeout
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = AutoFailoverBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                testcase.log.info("{0} nodes failed over as expected".format(failover_count))
                testcase.log.info("expected failover in {0} seconds, actual time {1} seconds".format\
                              (timeout - AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, time.time() - time_start))
                return
            time.sleep(2)

        rest = RestConnection(master)
        rest.print_UI_logs()
        testcase.log.warning("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
        testcase.fail("{0} nodes failed over, expected {1} in {2} seconds".
                         format(failover_count, autofailover_count, time.time() - time_start))

    @staticmethod
    def wait_for_no_failover_or_assert(master, autofailover_count, timeout, testcase):
        time_start = time.time()
        time_max_end = time_start + timeout
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

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count


class AutoFailoverTests(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.case_number = self.input.param("case_number", 0)
        self.servers = self.input.servers
        self.log = logger.Logger().get_logger()
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.timeout = 60
        AutoFailoverBaseTest.common_setup(self.input, self)
        self._cluster_setup()

    def tearDown(self):
        AutoFailoverBaseTest.common_tearDown(self.servers, self)

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def test_enable(self):
        status = self.rest.update_autofailover_settings(True, self.timeout // 2)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        #read settings and verify
        settings = self.rest.get_autofailover_settings()
        self.assertEqual(settings.enabled, True)

    def test_disable(self):
        status = self.rest.update_autofailover_settings(False, self.timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        #read settings and verify
        settings = self.rest.get_autofailover_settings()
        self.assertEqual(settings.enabled, False)

    def test_valid_timeouts(self):
        timeouts = [30, 31, 300, 3600]
        for timeout in timeouts:
            status = self.rest.update_autofailover_settings(True, timeout)
            if not status:
                self.fail('failed to change autofailover_settings! See MB-7282')
            #read settings and verify
            settings = self.rest.get_autofailover_settings()
            self.assertTrue(settings.timeout == timeout)

    def test_30s_timeout_firewall(self):
        timeout = self.timeout // 2
        server_fail = self.servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        RemoteUtilHelper.enable_firewall(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def test_60s_timeout_firewall(self):
        timeout = self.timeout
        server_fail = self.servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        RemoteUtilHelper.enable_firewall(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def test_30s_timeout_stop(self):
        timeout = self.timeout
        server_fail = self.servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        self._stop_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def test_60s_timeout_stop(self):
        timeout = self.timeout
        server_fail = self.servers[1]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        self._stop_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def test_reset_count(self):
        timeout = self.timeout // 2
        server_fail1 = self.servers[1]
        server_fail2 = self.servers[2]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        self.log.info("stopping the first server")
        self._stop_couchbase(server_fail1)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

        self.log.info("resetting the autofailover count")
        if not self.rest.reset_autofailover():
            self.fail('failed to reset autofailover count!')

        self.log.info("stopping the second server")
        self._stop_couchbase(server_fail2)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

        self.log.info("resetting the autofailover count")
        if not self.rest.reset_autofailover():
            self.fail('failed to reset autofailover count!')

    def test_30s_timeout_pause(self):
        timeout = self.timeout // 2
        server_fail = self.servers[1]
        shell = RemoteMachineShellConnection(server_fail)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        if type.lower() == 'windows':
            self.log.info("test will be skipped because the signals SIGSTOP and SIGCONT do not exist for Windows")
            return
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        self._pause_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def test_60s_timeout_pause(self):
        timeout = self.timeout
        server_fail = self.servers[1]
        shell = RemoteMachineShellConnection(server_fail)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        if type.lower() == 'windows':
            self.log.info("test will be skipped because the signals SIGSTOP and SIGCONT do not exist for Windows")
            return
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        self._pause_couchbase(server_fail)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def test_invalid_timeouts(self):
        # The value of "timeout" must be a positive integer in a range from 5 to 3600
        timeouts = [-360, -60, 0, 300000]
        for timeout in timeouts:
            self.rest.update_autofailover_settings(True, timeout)
            # read settings and verify
            settings = self.rest.get_autofailover_settings()
            self.log.info("Value returned by autofailover settings : {0}".format(settings.timeout))
            self.assertNotEqual(settings.timeout, timeout)

    def test_two_failed_nodes(self):
        timeout = self.timeout // 2
        server_fail1 = self.servers[1]
        server_fail2 = self.servers[2]
        status = self.rest.update_autofailover_settings(True, timeout)
        if not status:
            self.fail('failed to change autofailover_settings! See MB-7282')
        self.sleep(5)
        self.log.info("stopping the first server")
        self._stop_couchbase(server_fail1)
        AutoFailoverBaseTest.wait_for_failover_or_assert(self.master, 1, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

        self.log.info("stopping the second server")
        self._stop_couchbase(server_fail2)
        AutoFailoverBaseTest.wait_for_no_failover_or_assert(self.master, 2, timeout + AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME, self)

    def _stop_couchbase(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    #the signals SIGSTOP and SIGCONT do not exist for Windows
    def _pause_couchbase(self, server):
        self._pause_beam(server)
        self._pause_memcached(server)

    #the signals SIGSTOP and SIGCONT do not exist for Windows
    def _pause_memcached(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.pause_memcached()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    #the signals SIGSTOP and SIGCONT do not exist for Windows
    def _pause_beam(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.pause_beam()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    def load_data(self, master, bucket, keys_count):
        inserted_keys_cnt = 0
        repeat_count = 0
        while inserted_keys_cnt < keys_count and repeat_count < 5:
            keys_cnt, rejected_keys_cnt = \
            MemcachedClientHelper.load_bucket(servers=[master],
                name=bucket,
                number_of_items=keys_count,
                number_of_threads=5,
                write_only=True)
            inserted_keys_cnt += keys_cnt
            if keys_cnt == 0:
                repeat_count += 1
            else:
                repeat_count = 0
        if repeat_count == 5:
            log.exception("impossible to load data")
        log.info("wait until data is completely persisted on the disk")
        RebalanceHelper.wait_for_persistence(master, bucket)
        return inserted_keys_cnt

    def _cluster_setup(self):
        replicas = self.input.param("replicas", 1)
        keys_count = self.input.param("keys-count", 0)
        num_buckets = self.input.param("num-buckets", 1)
        bucket_storage = self.input.param("bucket_storage", 'couchstore')

        bucket_name = "default"
        master = self.servers[0]
        credentials = self.input.membase_settings
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        rest.reset_autofailover()
        ClusterOperationHelper.add_and_rebalance(self.servers, True)

        if num_buckets == 1:
            bucket_ram = info.memoryQuota * 2 // 3
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=replicas,
                               storageBackend=bucket_storage)
        else:
            created = BucketOperationHelper.create_multiple_buckets(self.master, replicas, howmany=num_buckets,
                                                                    bucket_ram_ratio=(1.0 / 4.0),
                                                                    bucket_storage=bucket_storage)
            self.assertTrue(created, "unable to create multiple buckets")

        buckets = rest.get_buckets()
        for bucket in buckets:
                ready = BucketOperationHelper.wait_for_memcached(self.master, bucket.name)
                self.assertTrue(ready, msg="wait_for_memcached failed")

        for bucket in buckets:
            inserted_keys_cnt = self.load_data(self.master, bucket.name, keys_count)
            log.info('inserted {0} keys'.format(inserted_keys_cnt))
