import random
import time
import unittest

import logger
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper

from TestInput import TestInputSingleton
from security.rbac_base import RbacBase

log = logger.Logger.get_logger()


class AutoReprovisionBaseTest(unittest.TestCase):
    # start from 1..n
    # then from no failover x node and rebalance and
    # verify we did not lose items

    # maximum time we allow ns_server to take to detect a failed node
    # timeout as workaround for MB-7863
    MAX_FAIL_DETECT_TIME = 120

    @staticmethod
    def common_setup(input, testcase):
        log.info("==============  common_setup was started for test #{0} {1}==============" \
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
   
        log.info("==============  common_setup was finished for test #{0} {1} ==============" \
                 .format(testcase.case_number, testcase._testMethodName))

    @staticmethod
    def common_tearDown(servers, testcase):
        log.info("==============  common_tearDown was started for test #{0} {1} ==============" \
                 .format(testcase.case_number, testcase._testMethodName))
        RemoteUtilHelper.common_basic_setup(servers)

        log.info("10 seconds delay to wait for couchbase-server to start")
        time.sleep(10)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase, \
                                                             wait_time=AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME * 10,
                                                             wait_if_warmup=True)
        try:
            rest = RestConnection(servers[0])
            buckets = rest.get_buckets()
            for bucket in buckets:
                MemcachedClientHelper.flush_bucket(servers[0], bucket.name)
        except Exception:
            pass
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        log.info("==============  common_tearDown was finished for test #{0} {1} ==============" \
                 .format(testcase.case_number, testcase._testMethodName))

    @staticmethod
    def wait_for_failover_or_assert(master, autofailover_count, timeout, testcase):
        time_start = time.time()
        time_max_end = time_start + timeout
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = AutoReprovisionBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                testcase.log.info("{0} nodes failed over as expected".format(failover_count))
                testcase.log.info("expected failover in {0} seconds, actual time {1} seconds".format \
                                      (timeout - AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                       time.time() - time_start))
                return
            time.sleep(2)

        rest = RestConnection(master)
        rest.print_UI_logs()
        testcase.log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
        testcase.fail("{0} nodes failed over, expected {1} in {2} seconds".
                      format(failover_count, autofailover_count, time.time() - time_start))

    @staticmethod
    def wait_for_warmup_or_assert(master, warmup_count, timeout, testcase):
        time_start = time.time()
        time_max_end = time_start + timeout
        bucket_name = testcase.rest.get_buckets()[0].name
        while time.time() < time_max_end:
            num_nodes_with_warmup = 0
            for node in testcase.rest.get_bucket(bucket_name).nodes:
                if node.status == 'warmup':
                    num_nodes_with_warmup += 1
            if num_nodes_with_warmup == warmup_count:
                testcase.log.info("{0} nodes warmup as expected".format(num_nodes_with_warmup))
                testcase.log.info("expected warmup in {0} seconds, actual time {1} seconds".format \
                                      (timeout - AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                       time.time() - time_start))
                return
            time.sleep(2)

        rest = RestConnection(master)
        rest.print_UI_logs()
        testcase.log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
        testcase.fail("{0} nodes warmup, expected {1} in {2} seconds".
                      format(num_nodes_with_warmup, warmup_count, time.time() - time_start))

    @staticmethod
    def wait_for_no_failover_or_assert(master, autofailover_count, timeout, testcase):
        time_start = time.time()
        time_max_end = time_start + timeout
        failover_count = 0

        while time.time() < time_max_end:
            failover_count = AutoReprovisionBaseTest.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        time_end = time.time()

        testcase.assertFalse(failover_count == autofailover_count,
                             "{0} nodes failed over, didn't expect {1} in {2} seconds".
                             format(failover_count, autofailover_count, time.time() - time_start))
        log.info("{0} nodes failed over as expected in {1} seconds".format(failover_count, time_end - time_start))

    @staticmethod
    def get_failover_count(master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['status']))
            if node['status'] == "unhealthy":
                failover_count += 1

        return failover_count


class AutoReprovisionTests(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.case_number = self.input.param("case_number", 0)
        self.use_master = self.input.param("use_master", False)
        self.skip_services = self.input.param("skip_services", True)
        self.replicas = self.input.param("replicas", 1)
        self.servers = self.input.servers
        self.log = logger.Logger().get_logger()
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.timeout = 60
        self.loaded_items = dict()
        AutoReprovisionBaseTest.common_setup(self.input, self)
        self._cluster_setup()
        if self.use_master:
            self.server_fail = self.servers[0]
            self.master = self.servers[1]
        else:
            self.server_fail = self.servers[1]

    def tearDown(self):
        AutoReprovisionBaseTest.common_tearDown(self.servers, self)

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def test_default_values(self):
        # read settings and verify
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 1)
        self.assertEqual(settings.count, 0)

    def test_enable(self):
        status = self.rest.update_autoreprovision_settings(True, 2)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        # read settings and verify
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 0)

    def test_disable(self):
        status = self.rest.update_autoreprovision_settings(False, 2)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        # read settings and verify
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, False)
        self.assertEqual(settings.max_nodes, 1)
        self.assertEqual(settings.count, 0)

    def test_valid_max_nodes(self):
        max_nodes = [1, 2, 5, 10, 100]  # 0?
        for max_node in max_nodes:
            status = self.rest.update_autoreprovision_settings(True, max_node)
            if not status:
                self.fail('failed to change autoreprovision_settings!')
            # read settings and verify
            settings = self.rest.get_autoreprovision_settings()
            self.assertEqual(settings.max_nodes, max_node)

    def test_node_firewall_enabled(self):
        timeout = self.timeout // 2

        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        RemoteUtilHelper.enable_firewall(self.server_fail)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self.sleep(5)
        shell = RemoteMachineShellConnection(self.server_fail)
        shell.disable_firewall()
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())
        buckets = self.rest.get_buckets()
        for bucket in buckets:
            self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_node_stop(self):
        timeout = self.timeout // 2
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        self._stop_couchbase(self.server_fail)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self.sleep(30)
        self._start_couchbase(self.server_fail)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self.sleep(10)
        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is not balanced")
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())
        buckets = self.rest.get_buckets()
        for bucket in buckets:
            self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_node_cb_restart(self):
        timeout = self.timeout // 2
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        shell = RemoteMachineShellConnection(self.server_fail)
        shell.restart_couchbase()
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self.sleep(5)
        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is not balanced")
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())
        buckets = self.rest.get_buckets()
        for bucket in buckets:
            self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_node_reboot(self):
        wait_timeout = 120
        timeout = self.timeout // 2
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        shell = RemoteMachineShellConnection(self.server_fail)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        if shell.extract_remote_info().type.lower() == 'windows':
            time.sleep(wait_timeout * 5)
        else:
            time.sleep(wait_timeout)
        # disable firewall on the node
        shell = RemoteMachineShellConnection(self.server_fail)
        shell.disable_firewall()
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is balanced")
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())
        buckets = self.rest.get_buckets()
        for bucket in buckets:
            self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_firewall_node_when_autoreprovisioning(self):
        wait_timeout = 120
        before = self.input.param("before", True)
        timeout = self.timeout // 2
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        shell = RemoteMachineShellConnection(self.server_fail)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        if shell.extract_remote_info().type.lower() == 'windows':
            time.sleep(wait_timeout * 5)
        else:
            time.sleep(wait_timeout)
        # disable firewall on the node
        shell = RemoteMachineShellConnection(self.server_fail)
        shell.disable_firewall()
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is balanced")
        # self.sleep(5)
        if before:
            RemoteUtilHelper.enable_firewall(self.servers[2])
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        if not before:
            RemoteUtilHelper.enable_firewall(self.servers[2])
        # self.sleep(5)
        try:
            self.rest.monitorRebalance()
            self.fail("Rebalance failed expected")
        except RebalanceFailedException:
            self.log.info("Rebalance failed but it's expected")
        shell = RemoteMachineShellConnection(self.servers[2])
        shell.disable_firewall()
        self.sleep(5)
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())
        buckets = self.rest.get_buckets()
        for bucket in buckets:
            self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_node_memcached_failure(self):
        timeout = self.timeout // 2
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        self._pause_couchbase(self.server_fail)
        self.sleep(5)
        AutoReprovisionBaseTest.wait_for_warmup_or_assert(self.master, 1,
                                                          timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                          self)
        RemoteUtilHelper.common_basic_setup([self.server_fail])
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
        buckets = self.rest.get_buckets()
        for bucket in buckets:
            self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_two_failed_nodes(self):
        timeout = self.timeout // 2
        server_fail1 = self.servers[1]
        server_fail2 = self.servers[2]
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        self.log.info("stopping the first server")
        self._stop_couchbase(server_fail1)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)

        self.log.info("stopping the second server")
        self._stop_couchbase(server_fail2)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 2,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        helper = RestHelper(self.rest)
        self.assertFalse(helper.is_cluster_healthy(), "cluster status is healthy")
        self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
        self._start_couchbase(server_fail1)

        self._start_couchbase(server_fail1)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self._start_couchbase(server_fail2)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        self.sleep(20)
        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is balanced")
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())

    def test_reset_count(self):
        timeout = self.timeout // 2
        server_fail1 = self.servers[1]
        server_fail2 = self.servers[2]
        status = self.rest.update_autoreprovision_settings(True, 2)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        self.log.info("stopping the first server")
        self._stop_couchbase(server_fail1)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)

        self.log.info("resetting the autoreprovision count")
        if not self.rest.reset_autoreprovision():
            self.fail('failed to reset autoreprovision count!')

        self.log.info("stopping the second server")
        self._stop_couchbase(server_fail2)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 2,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)

        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 0)

        self._start_couchbase(server_fail2)
        self._start_couchbase(server_fail1)
        self.sleep(30)
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 2)
        self.log.info("resetting the autoreprovision count")
        if not self.rest.reset_autoreprovision():
            self.fail('failed to reset autoreprovision count!')
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 0)

        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is balanced")
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())

    def test_invalid_max_nodes(self):
        max_nodes = [-30, -2, 0.5, 'df', True]  # 3000000000 ?
        for max_node in max_nodes:
            status = self.rest.update_autoreprovision_settings(True, max_node)
            if status:
                self.fail('autoreprovision_settings have been changed incorrectly!')
            # read settings and verify
            settings = self.rest.get_autoreprovision_settings()
            self.assertEqual(settings.max_nodes, 1)

    def test_node_memcached_failure_in_series(self):
        timeout = self.timeout // 2
        status = self.rest.update_autoreprovision_settings(True, 1)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        data_lost = False
        for i in reversed(range(len(self.servers))):
            print(self.servers[i])
            operation = random.choice(['stop', 'memcached_failure', 'restart', 'failover', 'reboot'])
            shell = RemoteMachineShellConnection(self.servers[i])
            print("operation", operation)
            if i == 0:
                self.master = self.servers[1]
            if operation == 'stop':
                self._stop_couchbase(self.servers[i])
            elif operation == 'memcached_failure':
                self._pause_couchbase(self.servers[i])
            elif operation == 'restart':
                shell.restart_couchbase()
            elif operation == 'failover':
                RemoteUtilHelper.enable_firewall(self.servers[i])
            elif operation == 'reboot':
                if shell.extract_remote_info().type.lower() == 'windows':
                    o, r = shell.execute_command("shutdown -r -f -t 0")
                    self.sleep(200)
                elif shell.extract_remote_info().type.lower() == 'linux':
                    o, r = shell.execute_command("reboot")
                shell.log_command_output(o, r)
                self.sleep(60)
            self.sleep(40)
            if operation == 'memcached_failure':
                AutoReprovisionBaseTest.wait_for_warmup_or_assert(self.master, 1,
                                                                  timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                                  self)
            if operation != 'restart' and operation != 'memcached_failure' and operation != 'reboot':
                AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                                    timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                                    self)
            if operation != 'restart':
                RemoteUtilHelper.common_basic_setup([self.servers[i]])
            AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 0,
                                                                timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                                self)
            helper = RestHelper(RestConnection(self.master))
            self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
            self.sleep(40)
            if operation == 'memcached_failure' or operation == 'failover':
                self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
            else:
                if 'kv' in self.servers[i].services and self.replicas > 0:
                    self.assertFalse(helper.is_cluster_rebalanced(), "cluster is balanced")
                    self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
                    self.assertTrue(self.rest.monitorRebalance())
                else:
                    self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
            buckets = self.rest.get_buckets()
            if self.replicas == 0 and (operation == 'restart' or operation == 'reboot'):
                data_lost = True
            for bucket in buckets:
                if not data_lost:
                    self.verify_loaded_data(self.master, bucket.name, self.loaded_items[bucket.name])

    def test_ui_logs(self):
        timeout = self.timeout // 2
        server_fail1 = self.servers[1]
        server_fail2 = self.servers[2]
        status = self.rest.update_autoreprovision_settings(True, 2)
        if not status:
            self.fail('failed to change autoreprovision_settings!')
        self.sleep(5)
        logs = self.rest.get_logs(5)
        self.assertTrue('Enabled auto-reprovision config with max_nodes set to 2' in [l['text'] for l in logs])

        self.log.info("stopping the first server")
        self._stop_couchbase(server_fail1)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 1,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)

        self.log.info("resetting the autoreprovision count")
        if not self.rest.reset_autoreprovision():
            self.fail('failed to reset autoreprovision count!')
        logs = self.rest.get_logs(5)
        self.assertTrue('auto-reprovision count reset from 0' in [l['text'] for l in logs])

        self.log.info("stopping the second server")
        self._stop_couchbase(server_fail2)
        AutoReprovisionBaseTest.wait_for_failover_or_assert(self.master, 2,
                                                            timeout + AutoReprovisionBaseTest.MAX_FAIL_DETECT_TIME,
                                                            self)
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 0)
        self._start_couchbase(server_fail2)
        self._start_couchbase(server_fail1)
        self.sleep(30)
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 2)
        logs = self.rest.get_logs(5)
        self.assertTrue('auto-reprovision is disabled as maximum number of nodes (2) '
                        'that can be auto-reprovisioned has been reached.' in [l['text'] for l in logs])

        self.log.info("resetting the autoreprovision count")
        if not self.rest.reset_autoreprovision():
            self.fail('failed to reset autoreprovision count!')
        settings = self.rest.get_autoreprovision_settings()
        self.assertEqual(settings.enabled, True)
        self.assertEqual(settings.max_nodes, 2)
        self.assertEqual(settings.count, 0)
        logs = self.rest.get_logs(5)
        self.assertTrue('auto-reprovision count reset from 2' in [l['text'] for l in logs])

        helper = RestHelper(self.rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertFalse(helper.is_cluster_rebalanced(), "cluster is balanced")
        self.rest.rebalance(otpNodes=[node.id for node in self.rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(self.rest.monitorRebalance())
        logs = self.rest.get_logs(5)
        # https://issues.couchbase.com/browse/MB-24520
        self.assertFalse('Reset auto-failover count' in [l['text'] for l in logs])
        self.assertTrue('Rebalance completed successfully.' in [l['text'] for l in logs])

    def _stop_couchbase(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    def _start_couchbase(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.start_couchbase()
        shell.disconnect()
        log.info("srarted couchbase server on {0}".format(server))

    # the signals SIGSTOP and SIGCONT do not exist for Windows
    def _pause_couchbase(self, server):
        self._pause_beam(server)
        self._pause_memcached(server)

    # the signals SIGSTOP and SIGCONT do not exist for Windows
    def _pause_memcached(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.pause_memcached()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    # the signals SIGSTOP and SIGCONT do not exist for Windows
    def _pause_beam(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.pause_beam()
        shell.disconnect()
        log.info("stopped couchbase server on {0}".format(server))

    @staticmethod
    def load_bucket_and_return_the_keys(servers=None,
                                        name='default',
                                        ram_load_ratio=-1,
                                        number_of_items=-1,
                                        value_size_distribution=None,
                                        number_of_threads=1,
                                        override_vBucketId=-1,
                                        write_only=False,
                                        moxi=True,
                                        delete_ratio=0,
                                        expiry_ratio=0):
        inserted_keys = []
        rejected_keys = []
        log = logger.Logger.get_logger()
        threads = MemcachedClientHelper.create_threads(servers,
                                                       name,
                                                       ram_load_ratio,
                                                       number_of_items,
                                                       value_size_distribution,
                                                       number_of_threads,
                                                       override_vBucketId,
                                                       write_only=write_only,
                                                       moxi=moxi,
                                                       delete_ratio=delete_ratio,
                                                       expiry_ratio=expiry_ratio)

        # we can start them!
        for thread in threads:
            thread.start()
        log.info("waiting for all worker thread to finish their work...")
        [thread.join() for thread in threads]
        log.info("worker threads are done...")

        inserted_count = 0
        rejected_count = 0
        deleted_count = 0
        expired_count = 0
        for thread in threads:
            t_inserted, t_rejected = thread.keys_set()
            inserted_count += thread.inserted_keys_count()
            rejected_count += thread.rejected_keys_count()
            deleted_count += thread._delete_count
            expired_count += thread._expiry_count
            inserted_keys.extend(t_inserted)
            rejected_keys.extend(t_rejected)
        msg = "inserted keys count : {0} , rejected keys count : {1}"
        log.info(msg.format(inserted_count, rejected_count))
        msg = "deleted keys count : {0} , expired keys count : {1}"
        log.info(msg.format(deleted_count, expired_count))
        return inserted_keys, rejected_keys

    def verify_loaded_data(self, master, bucket, inserted_keys):
        keys_exist = BucketOperationHelper.keys_exist_or_assert_in_parallel(inserted_keys, master, bucket, self,
                                                                            concurrency=4)
        self.assertTrue(keys_exist, msg="unable to verify keys after restore")

    def _cluster_setup(self):
        keys_count = self.input.param("keys-count", 0)
        num_buckets = self.input.param("num-buckets", 1)
        bucketType = self.input.param("bucketType", "ephemeral")
        evictionPolicy = self.input.param("evictionPolicy", "noEviction")  # fullEviction

        # master = self.servers[0]
        # credentials = self.input.membase_settings
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        memory = min(info.mcdMemoryReserved, self.input.param("kv_memory", 1000))
        rest.init_cluster_memoryQuota(memoryQuota=memory)
        rest.reset_autoreprovision()
        self._add_and_rebalance(self.servers, True)

        if num_buckets == 1:
            bucket_name = "default"
            bucket_ram = info.memoryQuota * 2 // 3
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=self.replicas,
                               proxyPort=info.moxi,
                               bucketType=bucketType,
                               evictionPolicy=evictionPolicy)
        else:
            created = BucketOperationHelper.create_multiple_buckets(
                self.master, self.replicas, howmany=num_buckets,
                bucketType=bucketType, evictionPolicy=evictionPolicy)
            self.assertTrue(created, "unable to create multiple buckets")

        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(self.master, bucket.name)
            self.assertTrue(ready, msg="wait_for_memcached failed")

        for bucket in buckets:
            distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
            inserted_keys, rejected_keys = self.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                name=bucket.name,
                                                                                # ram_load_ratio=0.02,
                                                                                value_size_distribution=distribution,
                                                                                write_only=True,
                                                                                moxi=True,
                                                                                number_of_threads=2,
                                                                                number_of_items=keys_count)
            self.loaded_items[bucket.name] = inserted_keys

    def _add_and_rebalance(self, servers, wait_for_rebalance=True):
        log = logger.Logger.get_logger()
        master = servers[0]
        all_nodes_added = True
        rebalanced = True
        rest = RestConnection(master)
        if len(servers) > 1:
            for serverInfo in servers[1:]:
                log.info('adding {0} node : {1}:{2} to the cluster'.format(
                    serverInfo.services, serverInfo.ip, serverInfo.port))
                services = serverInfo.services.split()
                if self.skip_services:
                    services = None
                otpNode = rest.add_node(master.rest_username, master.rest_password, serverInfo.ip, port=serverInfo.port,
                                        services=services)
                if otpNode:
                    log.info('added node : {0} to the cluster'.format(otpNode.id))
                else:
                    all_nodes_added = False
                    break
            if all_nodes_added:
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                if wait_for_rebalance:
                    rebalanced &= rest.monitorRebalance()
                else:
                    rebalanced = False
        return all_nodes_added and rebalanced
