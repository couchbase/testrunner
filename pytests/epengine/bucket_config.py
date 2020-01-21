import time
import logger
import unittest
from TestInput import TestInput, TestInputSingleton

from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from membase.helper.bucket_helper import BucketOperationHelper
import json
from membase.helper.cluster_helper import ClusterOperationHelper
import logger
from couchbase_helper.cluster import Cluster

from remote.remote_util import RemoteMachineShellConnection
from membase.helper.rebalance_helper import RebalanceHelper
import re
import traceback

from basetestcase import BaseTestCase


class BucketConfig(BaseTestCase):

    def setUp(self):
        super(BucketConfig, self).setUp()
        self.testcase = '2'
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        #self.time_synchronization = self.input.param("time_sync", "enabledWithoutDrift")
        self.lww = self.input.param("lww", True)
        self.drift = self.input.param("drift", False)
        self.bucket='bucket-1'
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.cluster = Cluster()
        self.skip_rebalance = self.input.param("skip_rebalance", False)

        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        mem_quota = int(self.rest.get_nodes_self().mcdMemoryReserved *
                        node_ram_ratio)

        if not self.skip_rebalance:
            self.rest.init_cluster(self.master.rest_username,
                self.master.rest_password)
            self.rest.init_cluster_memoryQuota(self.master.rest_username,
                self.master.rest_password,
                memoryQuota=mem_quota)
            for server in self.servers:
                ClusterOperationHelper.cleanup_cluster([server])
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [self.master], self.testcase)
            try:
                rebalanced = ClusterOperationHelper.add_and_rebalance(
                    self.servers)

            except Exception as e:
                self.fail(e, 'cluster is not rebalanced')

        self._create_bucket(self.lww, self.drift)

    def tearDown(self):
        super(BucketConfig, self).tearDown()
        return
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            BucketOperationHelper.delete_all_buckets_or_assert(
                self.servers, self.testcase)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                self.servers, self.testcase)

    def test_modify_bucket_params(self):
        try:
            self.log.info("Modifying timeSynchronization value after bucket creation .....")
            self._modify_bucket()
        except Exception as e:
            traceback.print_exc()
            self.fail('[ERROR] Modify testcase failed .., {0}'.format(e))

    def test_restart(self):
        try:
            self.log.info("Restarting the servers ..")
            self._restart_server(self.servers[:])
            self.log.info("Verifying bucket settings after restart ..")
            self._check_config()
        except Exception as e:
            traceback.print_exc()
            self.fail("[ERROR] Check data after restart failed with exception {0}".format(e))

    def test_failover(self):
        num_nodes=1
        self.cluster.failover(self.servers, self.servers[1:num_nodes])
        try:
            self.log.info("Failing over 1 of the servers ..")
            self.cluster.rebalance(self.servers, [], self.servers[1:num_nodes])
            self.log.info("Verifying bucket settings after failover ..")
            self._check_config()
        except Exception as e:
            traceback.print_exc()
            self.fail('[ERROR]Failed to failover .. , {0}'.format(e))

    def test_rebalance_in(self):
        try:
            self.log.info("Rebalancing 1 of the servers ..")
            ClusterOperationHelper.add_and_rebalance(
                self.servers)
            self.log.info("Verifying bucket settings after rebalance ..")
            self._check_config()
        except Exception as e:
            self.fail('[ERROR]Rebalance failed .. , {0}'.format(e))

    def test_backup_same_cluster(self):
        self.shell = RemoteMachineShellConnection(self.master)
        self.buckets = RestConnection(self.master).get_buckets()
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = "/tmp/backup"
        self.command_options = self.input.param("command_options", '')
        try:
            shell = RemoteMachineShellConnection(self.master)
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

            time.sleep(5)
            shell.restore_backupFile(self.couchbase_login_info, self.backup_location, [bucket.name for bucket in self.buckets])

        finally:
            self._check_config()

    def test_backup_diff_bucket(self):
        self.shell = RemoteMachineShellConnection(self.master)
        self.buckets = RestConnection(self.master).get_buckets()
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = "/tmp/backup"
        self.command_options = self.input.param("command_options", '')
        try:
            shell = RemoteMachineShellConnection(self.master)
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

            time.sleep(5)
            self._create_bucket(lww=False, name="new_bucket")
            self.buckets = RestConnection(self.master).get_buckets()
            shell.restore_backupFile(self.couchbase_login_info, self.backup_location, ["new_bucket"])

        finally:
            self._check_config()

    ''' Helper functions for above testcases
    '''
    #create a bucket if it doesn't exist. The drift parameter is currently unused
    def _create_bucket(self, lww=True, drift=False, name=None):

        if lww:
            self.lww=lww

        if  name:
            self.bucket=name

        helper = RestHelper(self.rest)
        if not helper.bucket_exists(self.bucket):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(
                self.servers)
            info = self.rest.get_nodes_self()
            self.rest.create_bucket(bucket=self.bucket,
                ramQuotaMB=512, authType='sasl', lww=self.lww)
            try:
                ready = BucketOperationHelper.wait_for_memcached(self.master,
                    self.bucket)
            except Exception as e:
                self.fail('unable to create bucket')

    # KETAKI tochange this
    def _modify_bucket(self):
        helper = RestHelper(self.rest)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(
            self.servers)
        info = self.rest.get_nodes_self()

        status, content = self.rest.change_bucket_props(bucket=self.bucket,
            ramQuotaMB=512, authType='sasl', timeSynchronization='enabledWithOutDrift')
        if re.search('TimeSyncronization not allowed in update bucket', content):
            self.log.info('[PASS]Expected modify bucket to disallow Time Synchronization.')
        else:
            self.fail('[ERROR] Not expected to allow modify bucket for Time Synchronization')

    def _restart_server(self, servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_couchbase()
            time.sleep(10)
            shell.start_couchbase()
            shell.disconnect()
        time.sleep(30)

    # REBOOT
    def _reboot_server(self):
        try:
            for server in self.servers[:]:
                shell = RemoteMachineShellConnection(server)
                if shell.extract_remote_info().type.lower() == 'windows':
                    o, r = shell.execute_command("shutdown -r -f -t 0")
                    shell.log_command_output(o, r)
                    shell.disconnect()
                    self.log.info("Node {0} is being stopped".format(server.ip))
                elif shell.extract_remote_info().type.lower() == 'linux':
                    o, r = shell.execute_command("reboot")
                    shell.log_command_output(o, r)
                    shell.disconnect()
                    self.log.info("Node {0} is being stopped".format(server.ip))

                    time.sleep(120)
                    shell = RemoteMachineShellConnection(server)
                    command = "/sbin/iptables -F"
                    o, r = shell.execute_command(command)
                    shell.log_command_output(o, r)
                    shell.disconnect()
                    self.log.info("Node {0} backup".format(server.ip))
        finally:
            self.log.info("Warming-up servers ..")
            time.sleep(100)



    def _check_config(self):
        rc = self.rest.get_bucket_json(self.bucket)
        if 'conflictResolution' in rc:
            conflictResolution  = self.rest.get_bucket_json(self.bucket)['conflictResolutionType']
            self.assertTrue(conflictResolution == 'lww', 'Expected conflict resolution of lww but got {0}'.format(conflictResolution))


        """ drift is disabled in 4.6, commenting out for now as it may come back later
        if self.lww and not self.drift:
            time_sync = 'enabledWithoutDrift'
        elif self.lww and self.drift:
            time_sync = 'enabledWithDrift'
        elif not self.lww:
            time_sync = 'disabled'
        self.assertEqual(result,time_sync, msg='ERROR, Mismatch on expected time synchronization values, ' \
                                               'expected {0} but got {1}'.format(time_sync, result))
        self.log.info("Verified results")
        """


