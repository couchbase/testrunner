import time

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from tasks.task import AutoFailoverNodesFailureTask, \
    NodeMonitorsAnalyserTask, NodeDownTimerTask
from tasks.taskmanager import TaskManager
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from membase.api.exception import RebalanceFailedException, \
    ServerUnavailableException
from membase.api.rest_client import RestConnection, RestHelper


class AutoFailoverAbortsRebalance(AutoFailoverBaseTest, BaseTestCase):
    MAX_FAIL_DETECT_TIME = 120
    ORCHESTRATOR_TIMEOUT_BUFFER = 60

    def setUp(self):
        super(AutoFailoverAbortsRebalance, self).setUp()
        self.master = self.servers[0]
        self._get_params()
        self.rest = RestConnection(self.orchestrator)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        self.num_buckets = self.num_buckets - 1  # this is done as default is created by base class
        if self.num_buckets:
            BucketOperationHelper.create_multiple_buckets(self.master, self.num_replicas, node_ram_ratio * (2.0 / 3.0),
                                                          howmany=self.num_buckets, bucket_storage=self.bucket_storage)
        self.buckets = self.rest.get_buckets()
        for bucket in self.buckets:
            ready = BucketOperationHelper.wait_for_memcached(self.master, bucket.name)
            self.assertTrue(ready, "wait_for_memcached failed")
        self.initial_load_gen = BlobGenerator('auto-failover',
                                              'auto-failover-',
                                              self.value_size,
                                              end=self.num_items)
        self.update_load_gen = BlobGenerator('auto-failover',
                                             'auto-failover-',
                                             self.value_size,
                                             end=self.update_items)
        self.delete_load_gen = BlobGenerator('auto-failover',
                                             'auto-failover-',
                                             self.value_size,
                                             start=self.update_items,
                                             end=self.delete_items)
        self._load_all_buckets(self.servers[0], self.initial_load_gen,
                               "create", 0)
        self._async_load_all_buckets(self.orchestrator,
                                     self.update_load_gen, "update", 0)
        self._async_load_all_buckets(self.orchestrator,
                                     self.delete_load_gen, "delete", 0)

    def tearDown(self):
        super(AutoFailoverAbortsRebalance, self).tearDown()

    def test_failure_scenarios_during_rebalance_in_of_node_A(self):
        # enable auto failover
        self.enable_autofailover_and_validate()
        self.sleep(5)
        # Start rebalance in
        rebalance_task = self.cluster.async_rebalance(self.servers,
                                                      self.servers_to_add,
                                                      self.servers_to_remove)
        reached = RestHelper(self.rest).rebalance_reached(percentage=30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.log.info(
                    "Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(
                        str(ex)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        # Reset auto failover settings
        self.disable_autofailover_and_validate()

    def test_failure_scenarios_during_recovery_of_node_A(self):
        self.recovery_type = self.input.param("recovery_type", 'full')
        # enable auto failover
        self.enable_autofailover_and_validate()
        self.sleep(5)
        # do a graceful failover
        self.cluster.failover([self.master], failover_nodes=[self.servers[self.server_index_to_fail]], graceful=True)
        # wait for failover to complete
        self.wait_for_failover_or_assert(1, 500)
        # do a delta recovery
        self.rest.set_recovery_type(otpNode='ns_1@' + self.servers[self.server_index_to_fail].ip,
                                    recoveryType=self.recovery_type)
        # Start rebalance of recovered nodes
        rebalance_task = self.cluster.async_rebalance(self.servers, [], [])
        reached = RestHelper(self.rest).rebalance_reached(percentage=30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.log.info(
                    "Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(
                        str(ex)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        # Reset auto failover settings
        self.disable_autofailover_and_validate()

    def test_failure_scenarios_during_rebalance_out_of_node_A(self):
        # enable auto failover
        self.enable_autofailover_and_validate()
        self.sleep(5)
        # Start rebalance out
        rebalance_task = self.cluster.async_rebalance(self.servers,
                                                      [],
                                                      [self.servers[self.server_index_to_fail]])
        reached = RestHelper(self.rest).rebalance_reached(percentage=30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.log.info(
                    "Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(
                        str(ex)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        # Reset auto failover settings
        self.disable_autofailover_and_validate()

    def test_failure_scenarios_during_rebalance_out_of_failedover_node_A(self):
        # enable auto failover
        self.enable_autofailover_and_validate()
        # failover a node
        self.cluster.failover([self.master], failover_nodes=[self.servers[self.server_index_to_fail]], graceful=False)
        # wait for failover to complete
        self.wait_for_failover_or_assert(1, 500)
        # Start rebalance out
        rebalance_task = self.cluster.async_rebalance(self.servers,
                                                      [],
                                                      [self.servers[self.server_index_to_fail]])
        reached = RestHelper(self.rest).rebalance_reached(percentage=30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.fail("Rebalance failed when it was not expected to fail".format(str(ex)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.fail("Rebalance interrupted due to auto-failover of nodes - It was not expected")
                else:
                    self.log.info("Rebalance was not interrupted as expected")
            else:
                self.log.info("Rebalance completes successfully")
        # Reset auto failover settings
        self.disable_autofailover_and_validate()

    def test_failure_scenarios_during_rebalance_out_of_failedover_other_than_node_A(self):
        self.server_index_to_failover = self.input.param("server_index_to_failover", None)
        # enable auto failover
        self.enable_autofailover_and_validate()
        # failover a node
        self.cluster.failover([self.master], failover_nodes=[self.servers[self.server_index_to_failover]],
                              graceful=False)
        self.sleep(5)
        # Start rebalance out
        rebalance_task = self.cluster.async_rebalance(self.servers,
                                                      [],
                                                      [self.servers[self.server_index_to_failover]])
        reached = RestHelper(self.rest).rebalance_reached(percentage=30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.log.info(
                    "Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(
                        str(ex)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        # Reset auto failover settings
        self.disable_autofailover_and_validate()

    def test_failure_scenarios_during_recovery_of_node_other_than_node_A(self):
        self.server_index_to_failover = self.input.param("server_index_to_failover", None)
        self.recovery_type = self.input.param("recovery_type", 'full')
        # enable auto failover
        self.enable_autofailover_and_validate()
        self.sleep(5)
        # do a graceful failover
        self.cluster.failover([self.master], failover_nodes=[self.servers[self.server_index_to_failover]],
                              graceful=True)
        # wait for failover to complete
        self.wait_for_failover_or_assert(1, 500)
        # do a delta recovery
        self.rest.set_recovery_type(otpNode='ns_1@' + self.servers[self.server_index_to_failover].ip,
                                    recoveryType=self.recovery_type)
        # Start rebalance of recovered nodes
        rebalance_task = self.cluster.async_rebalance(self.servers, [], [])
        reached = RestHelper(self.rest).rebalance_reached(percentage=30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.log.info(
                    "Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(
                        str(ex)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        # Reset auto failover settings
        try:
            self.disable_autofailover_and_validate()
        except:
            pass
