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
        self.task_manager = TaskManager("Autofailover_thread")
        self.task_manager.start()
        self.node_failure_task_manager = TaskManager("Nodes_failure_detector_thread")
        self.node_failure_task_manager.start()
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        self.num_buckets = self.num_buckets - 1 # this is done as default is created by base class
        if self.num_buckets:
            BucketOperationHelper.create_multiple_buckets(self.master, self.num_replicas, node_ram_ratio * (2.0 / 3.0),
                                                          howmany=self.num_buckets)
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

    def test_failure_scenarios_during_rebalance_of_node_A(self):
        # enable auto failover and canAbortRebalance
        self.enable_autofailover_and_validate()
        self.sleep(5)
        # Start rebalance in
        rebalance_task = self.cluster.async_rebalance(self.servers,
                                                      self.servers_to_add,
                                                      self.servers_to_remove)
        self.sleep(5)
        try:
            # Do a fail over action - reboot, hang, kill. This is defined in the conf file
            self.failover_actions[self.failover_action](self)
            rebalance_task.result()
        except Exception as ex:
            self.log.info("Rebalance failed with : {0}".format(str(ex)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex):
                self.log.info("Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(str(ex)))
            if not RestHelper(self.rest).is_cluster_rebalanced():
                if any("Rebalance interrupted due to auto-failover of nodes ['ns_1@{0}'].". format(self.server_to_fail[0].ip) in d.values() for d in
                       self.rest.get_logs(100)):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        # Reset auto failover settings
        self.disable_autofailover_and_validate()