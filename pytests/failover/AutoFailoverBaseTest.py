import gc
import time

from basetestcase import BaseTestCase
from couchbase_cli import CouchbaseCLI
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from tasks.task import AutoFailoverNodesFailureTask, \
    NodeMonitorsAnalyserTask, NodeDownTimerTask
from tasks.taskmanager import TaskManager


class AutoFailoverBaseTest(BaseTestCase):
    MAX_FAIL_DETECT_TIME = 120
    ORCHESTRATOR_TIMEOUT_BUFFER = 60

    def setUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self._get_params()
        self.rest = RestConnection(self.orchestrator)
        self.task_manager = TaskManager("Autofailover_thread")
        self.task_manager.start()
        self.node_failure_task_manager = TaskManager(
            "Nodes_failure_detector_thread")
        self.node_failure_task_manager.start()
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
        if not self.skip_load and str(self.__class__).find('newupgradetests') != -1:
            self._load_all_buckets(self.servers[0], self.initial_load_gen,
                                   "create", 0)
            self._async_load_all_buckets(self.orchestrator,
                                         self.update_load_gen, "update", 0)
            self._async_load_all_buckets(self.orchestrator,
                                         self.delete_load_gen, "delete", 0)
        self.server_index_to_fail = self.input.param("server_index_to_fail", None)
        if self.server_index_to_fail is None:
            self.server_to_fail = self._servers_to_fail()
        else:
            if isinstance(self.server_index_to_fail, str):
                self.server_to_fail = [self.servers[int(node_item)] for node_item in self.server_index_to_fail.split(":")]
            else:
                self.server_to_fail = [self.servers[self.server_index_to_fail]]
        self.servers_to_add = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self.servers_to_remove = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]

    def bareSetUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self._get_params()
        self.rest = RestConnection(self.orchestrator)
        self.task_manager = TaskManager("Autofailover_thread")
        self.task_manager.start()
        self.node_failure_task_manager = TaskManager(
            "Nodes_failure_detector_thread")
        self.node_failure_task_manager.start()
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
        self.server_to_fail = self._servers_to_fail()
        self.servers_to_add = self.servers[self.nodes_init:self.nodes_init +
                                                           self.nodes_in]
        self.servers_to_remove = self.servers[self.nodes_init -
                                              self.nodes_out:self.nodes_init]

    def tearDown(self):
        self.log.info("============AutoFailoverBaseTest teardown============")
        self._get_params()
        self.task_manager = TaskManager("Autofailover_thread")
        self.task_manager.start()
        self.server_to_fail = self._servers_to_fail()
        self.start_couchbase_server()
        self.sleep(10)
        self.disable_firewall()
        self.rest = RestConnection(self.orchestrator)
        self.rest.reset_autofailover()
        self.disable_autofailover()
        if not self.input.param("get-cbcollect-info", False):
            self._cleanup_cluster()
        super(AutoFailoverBaseTest, self).tearDown()
        if hasattr(self, "node_monitor_task"):
            if self.node_monitor_task._exception:
                self.fail("{}".format(self.node_monitor_task._exception))
            self.node_monitor_task.stop = True
        self.task_manager.shutdown(force=True)

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1. Rebalance the nodes in the end.
        Nodes are divided into groups iteratively i.e. 1st node in Group 1, 2nd in Group 2, 3rd in Group 1 and so on, when
        zone=2.
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        serverinfo = self.orchestrator
        rest = RestConnection(serverinfo)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [serverinfo.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                if not rest.is_zone_exist(zones[i]):
                    rest.add_zone(zones[i])
                nodes_in_zone[zones[i]] = []
            # Divide the nodes between zones.
            nodes_in_cluster = [node.ip for node in self.get_nodes_in_cluster()]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.servers)):
                if self.servers[i].ip in nodes_in_cluster and self.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(self.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    {node  for node in rest.get_nodes_in_zone(zones[i])})
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        self.zones = nodes_in_zone
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses() if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes, ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))

    def enable_autofailover(self):
        """
        Enable the autofailover setting with the given timeout.
        :return: True If the setting was set with the timeout, else return
        False
        """
        status = self.rest.update_autofailover_settings(True, self.timeout,
                                                        maxCount=self.max_count)
        return status

    def disable_autofailover(self):
        """
        Disable the autofailover setting.
        :return: True If the setting was disabled, else return
        False
        """
        status = self.rest.update_autofailover_settings(False, 120)
        return status

    def enable_autofailover_and_validate(self):
        """
        Enable autofailover with given timeout and then validate if the
        settings.
        :return: Nothing
        """
        status = self.enable_autofailover()
        self.assertTrue(status, "Failed to enable autofailover_settings!")
        self.sleep(5)
        settings = self.rest.get_autofailover_settings()
        self.assertTrue(settings.enabled, "Failed to enable "
                                          "autofailover_settings!")
        self.assertEqual(self.timeout, settings.timeout,
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}".format(self.timeout,
                                                           settings.timeout))

    def disable_autofailover_and_validate(self):
        """
        Disable autofailover setting and then validate if the setting was
        disabled.
        :return: Nothing
        """
        status = self.disable_autofailover()
        self.assertTrue(status, "Failed to change autofailover_settings!")
        self.sleep(5)
        settings = self.rest.get_autofailover_settings()
        self.log.info(f"Autofailover settings are {settings}")
        self.assertFalse(settings.enabled, "Failed to disable "
                                           "autofailover_settings!")

    def start_node_monitors_task(self):
        """
        Start the node monitors task to analyze the node status monitors.
        :return: The NodeMonitorAnalyserTask.
        """
        node_monitor_task = NodeMonitorsAnalyserTask(self.orchestrator)
        self.task_manager.schedule(node_monitor_task, sleep_time=5)
        return node_monitor_task

    def async_enable_firewall(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "enable_firewall", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def enable_firewall(self):
        """
        Enable firewall on the nodes to fail in the tests.
        :return: Nothing
        """
        task = self.async_enable_firewall()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def async_disable_firewall(self):
        self.time_start = time.time()
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "disable_firewall",
                                            self.timeout,
                                            self.pause_between_failover_action,
                                            False,
                                            self.timeout_buffer, False)
        return task

    def disable_firewall(self):
        """
        Disable firewall on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_disable_firewall()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def async_restart_couchbase_server(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, node.port)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "restart_couchbase",
                                            self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def restart_couchbase_server(self):
        """
        Restart couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_restart_couchbase_server()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def async_stop_couhbase_server(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, node.port)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "stop_couchbase",
                                            self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def stop_couchbase_server(self):
        """
        Stop couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_stop_couhbase_server()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def async_start_couchbase_server(self):
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "start_couchbase", self.timeout,
                                            0, False, self.timeout_buffer,
                                            False)
        return task

    def start_couchbase_server(self):
        """
        Start the couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_start_couchbase_server()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def async_stop_restart_network(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "restart_network", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def stop_restart_network(self):
        """
        Stop and restart network for said timeout period on the nodes to
        fail in the tests
        :return: Nothing
        """
        task = self.async_stop_couhbase_server()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def async_restart_machine(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "restart_machine", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def restart_machine(self):
        """
        Restart the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_restart_machine()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:

            self.fail("Exception: {}".format(e))
        finally:
            for node in self.server_to_fail:
                for i in range(0, 6):
                    try:
                        shell = RemoteMachineShellConnection(node)
                        break
                    except:
                        self.log.info("Unable to connect to the host. "
                                      "Machine has not restarted")
                        self.sleep(60, "Sleep for couple of minutes and try "
                                       "again")

    def async_stop_memcached(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, 11211)
            node_down_timer_tasks.append(node_failure_timer_task)
        self.timeout_buffer += 3
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "stop_memcached", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def stop_memcached(self):
        """
        Stop the memcached on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_stop_memcached()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))
        finally:
            task = AutoFailoverNodesFailureTask(self.orchestrator,
                                                self.server_to_fail,
                                                "start_memcached",
                                                self.timeout, 0, False, 0,
                                                check_for_failover=False)
            self.task_manager.schedule(task)
            task.result()

    def async_block_indexer_port(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, 11211)
            node_down_timer_tasks.append(node_failure_timer_task)
        self.timeout_buffer += 3
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "block_indexer_port", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def block_indexer_port(self):
        """
        Stop the indexer on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_block_indexer_port()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))
        finally:
            task = AutoFailoverNodesFailureTask(self.orchestrator,
                                                self.server_to_fail,
                                                "resume_indexer_port",
                                                self.timeout, 0, False, 0,
                                                check_for_failover=False)
            self.task_manager.schedule(task)
            task.result()

    def async_stop_indexer(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, 11211)
            node_down_timer_tasks.append(node_failure_timer_task)
        self.timeout_buffer += 3
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "stop_indexer", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        return task

    def stop_indexer(self):
        """
        Stop the indexer on the nodes to fail in the tests
        :return: Nothing
        """
        task = self.async_stop_indexer()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))
        finally:
            task = AutoFailoverNodesFailureTask(self.orchestrator,
                                                self.server_to_fail,
                                                "start_indexer",
                                                self.timeout, 0, False, 0,
                                                check_for_failover=False)
            self.task_manager.schedule(task)
            task.result()

    def async_split_network(self):
        self.time_start = time.time()
        if self.server_to_fail.__len__() < 2:
            self.fail("Need atleast 2 servers to fail")
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "network_split", self.timeout,
                                            self.pause_between_failover_action,
                                            False,
                                            self.timeout_buffer)
        return task

    def split_network(self):
        """
        Split the network in the cluster. Stop network traffic from few
        nodes while allowing the traffic from rest of the cluster.
        :return: Nothing
        """
        task = self.async_split_network()
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))
        self.disable_firewall()

    def bring_back_failed_nodes_up(self):
        """
        Bring back the failed nodes.
        :return: Nothing
        """
        if self.failover_action == "firewall":
            self.disable_firewall()
        elif self.failover_action == "stop_server":
            self.start_couchbase_server()

    def _servers_to_fail(self):
        """
        Select the nodes to be failed in the tests.
        :return: Nothing
        """
        if self.failover_orchestrator:
            servers_to_fail = self.servers[0:self.num_node_failures]
        else:
            servers_to_fail = self.servers[1:self.num_node_failures + 1]
        return servers_to_fail

    def _get_params(self):
        """
        Initialize the test parameters.
        :return:  Nothing
        """
        self.timeout = self.input.param("timeout", 60)
        self.max_count = self.input.param("maxCount", 1)
        self.failover_action = self.input.param("failover_action",
                                                "stop_server")
        self.failover_orchestrator = self.input.param("failover_orchestrator",
                                                      False)
        self.multiple_node_failure = self.input.param("multiple_nodes_failure",
                                                      False)
        self.num_items = self.input.param("num_items", 1000000)
        self.update_items = self.input.param("update_items", 100000)
        self.delete_items = self.input.param("delete_items", 100000)
        self.add_back_node = self.input.param("add_back_node", True)
        self.recovery_strategy = self.input.param("recovery_strategy",
                                                  "delta")
        self.multi_node_failures = self.input.param("multi_node_failures",
                                                    False)
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.deny_autofailover = self.input.param("deny_autofailover", None)
        self.services = self.input.param("services", None)
        self.zone = self.input.param("zone", 1)
        self.concurrent_failover = self.input.param("concurrent_failover", False)
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        self.multi_services_node = self.input.param("multi_services_node",
                                                    False)
        self.pause_between_failover_action = self.input.param(
            "pause_between_failover_action", 0)
        self.remove_after_failover = self.input.param(
            "remove_after_failover", False)
        self.timeout_buffer = 180 if self.failover_orchestrator else 60
        failover_not_expected = (self.max_count == 1 and self.num_node_failures > 1 and
                                self.pause_between_failover_action <
                                self.timeout or self.num_replicas < 1)
        failover_not_expected = failover_not_expected or (1 < self.max_count < self.num_node_failures and
                                                          self.pause_between_failover_action < self.timeout or
                                                          self.num_replicas < self.max_count)
        self.log.info(f'num_node_faliures : {self.num_node_failures}')
        self.log.info(f'servers existing : {self.servers}')
        self.failover_expected = not failover_not_expected
        if self.deny_autofailover is False:
            self.failover_expected = True
        elif self.deny_autofailover is True:
            self.failover_expected = False
        if self.failover_action is "restart_server":
            self.num_items *= 100
        self.orchestrator = self.servers[0] if not \
            self.failover_orchestrator else self.servers[
            self.num_node_failures]

    def _cleanup_cluster(self):
        """
        Cleaup the cluster. Delete all the buckets in the nodes and remove
        the nodes from any cluster that has been formed.
        :return:
        """
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for node in self.servers:
            master = node
            try:
                ClusterOperationHelper.cleanup_cluster(self.servers,
                                                       master=master)
            except:
                continue

    failover_actions = {
        "firewall": enable_firewall,
        "stop_server": stop_couchbase_server,
        "restart_server": restart_couchbase_server,
        "restart_machine": restart_machine,
        "restart_network": stop_restart_network,
        "stop_indexer": stop_indexer,
        "stop_memcached": stop_memcached,
        "network_split": split_network,
        "block_indexer_port": block_indexer_port,
    }

    def _auto_failover_message_present_in_logs(self, ipaddress):
        return any("Rebalance interrupted due to auto-failover of nodes ['ns_1@{0}'].".format(ipaddress) in
                   d['text'] for d in self.rest.get_logs(20))

    def wait_for_failover_or_assert(self, expected_failover_count, timeout):
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        self.assertTrue(actual_failover_count == expected_failover_count, "{0} nodes failed over, expected : {1}".
                        format(actual_failover_count, expected_failover_count))
        self.log.info(
            "{0} nodes failed over as expected in {1} seconds".format(actual_failover_count, time_end - time_start))

    def get_failover_count(self):
        rest = RestConnection(self.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

class DiskAutoFailoverBasetest(AutoFailoverBaseTest):
    def setUp(self):
        super(DiskAutoFailoverBasetest, self).bareSetUp()
        self.log.info("=============Starting Diskautofailover base setup=============")
        self.original_data_path = self.rest.get_data_path()
        ClusterOperationHelper.cleanup_cluster(self.servers, True, self.master)
        self.targetMaster = True
        self.reset_cluster()
        self.disk_location = self.input.param("data_location", "/data")
        self.disk_location_size = self.input.param("data_location_size", 5120)
        self.data_location = "{0}/data".format(self.disk_location)
        self.disk_timeout = self.input.param("disk_timeout", 120)
        self.read_loadgen = self.input.param("read_loadgen", False)
        self.log.info("Cleanup the cluster and set the data location to the one specified by the test.")
        for server in self.servers:
            self._create_data_locations(server)
            if server == self.master:
                master_services = self.get_services(self.servers[:1],
                                                    self.services_init,
                                                    start_node=0)
            else:
                master_services = None
            if master_services:
                master_services = master_services[0].split(",")
            self._initialize_node_with_new_data_location(server, self.data_location,
                                                         master_services)
        self.services = self.get_services(self.servers[:self.nodes_init], None)
        self.cluster.rebalance(self.servers[:1],
                               self.servers[1:self.nodes_init],
                               [], services=self.services)
        self.add_built_in_server_user(node=self.master)
        if self.read_loadgen:
            self.bucket_size = 256
        # super(DiskAutoFailoverBasetest,self)._bucket_creation()
        self._load_all_buckets(self.servers[0], self.initial_load_gen,
                               "create", 0)
        self.failover_actions['disk_failure'] = self.fail_disk_via_disk_failure
        self.failover_actions['disk_full'] = self.fail_disk_via_disk_full
        self.loadgen_tasks = []
        self.log.info("=============Finished Diskautofailover base setup=============")

    def tearDown(self):
        self.log.info("=============Starting Diskautofailover teardown ==============")
        self.targetMaster = True
        if hasattr(self, "original_data_path"):
            for task in self.loadgen_tasks:
                try:
                    task.set_result(True)
                    task.result()
                except Exception:
                    pass
            self.task_manager.shutdown(force=True)
            self.bring_back_failed_nodes_up()
            self.reset_cluster()
            for server in self.servers:
                self._initialize_node_with_new_data_location(server, self.original_data_path)
            for object in gc.get_objects():
                try:
                    if object.name in ["Autofailover_thread", "Cluster_Thread", "Nodes_failure_detector_thread"]:
                        self.log.info(object.name)
                        object.shutdown(force=True)
                except:
                    pass
        self.log.info("=============Finished Diskautofailover teardown ==============")

    def enable_disk_autofailover(self):
        status = self.rest.update_autofailover_settings(True, self.timeout, enable_disk_failure=True,
                                                        disk_timeout=self.disk_timeout)
        return status

    def enable_disk_autofailover_and_validate(self):
        status = self.enable_disk_autofailover()
        self.assertTrue(status, "Failed to enable disk autofailover for the cluster")
        self.sleep(5)
        settings = self.rest.get_autofailover_settings()
        self.assertTrue(settings.enabled, "Failed to enable "
                                          "autofailover_settings!")
        self.assertEqual(self.timeout, settings.timeout,
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}".format(self.timeout,
                                                           settings.timeout))
        self.assertTrue(settings.failoverOnDataDiskIssuesEnabled, "Failed to enable disk autofailover for the cluster")
        self.assertEqual(int(self.disk_timeout), settings.failoverOnDataDiskIssuesTimeout,
                         "Incorrect timeout period for disk failover set. Expected Timeout: {0} "
                         "Actual timeout: {1}".format(self.disk_timeout, settings.failoverOnDataDiskIssuesTimeout))

    def disable_disk_autofailover(self, disable_autofailover=False):
        status = self.rest.update_autofailover_settings(not disable_autofailover, self.timeout, enable_disk_failure=False,
                                                        disk_timeout=self.disk_timeout)
        return status

    def disable_disk_autofailover_and_validate(self, disable_autofailover=False):
        status = self.disable_disk_autofailover(disable_autofailover)
        self.assertTrue(status, "Failed to update autofailover settings. Failed to disable disk failover settings")
        settings = self.rest.get_autofailover_settings()
        self.assertEqual(not disable_autofailover, settings.enabled, "Failed to update autofailover settings.")
        self.assertFalse(settings.failoverOnDataDiskIssuesEnabled, "Failed to disable disk autofailover for the "
                                                                   "cluster")

    def _create_data_locations(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.create_new_partition(self.disk_location, self.disk_location_size)
        shell.create_directory(self.data_location)
        shell.give_directory_permissions_to_couchbase(self.data_location)
        shell.disconnect()

    def _initialize_node_with_new_data_location(self, server, data_location, services=None):
        init_port = server.port or '8091'
        init_tasks = []
        cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
        output, error, status = cli.node_init(data_location, None, None)
        self.log.info(output)
        if error or "ERROR" in output:
            self.log.info(error)
            self.fail("Failed to set new data location. Check error message.")
        init_tasks.append(self.cluster.async_init_node(server, self.disabled_consistent_view,
                                                       self.rebalanceIndexWaitingDisabled,
                                                       self.rebalanceIndexPausingDisabled, self.maxParallelIndexers,
                                                       self.maxParallelReplicaIndexers, init_port,
                                                       self.quota_percent, services=services,
                                                       index_quota_percent=self.index_quota_percent,
                                                       gsi_type=self.gsi_type))
        for task in init_tasks:
            task.result()

    def fail_disk_via_disk_failure(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "disk_failure", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks,
                                            disk_timeout=self.disk_timeout,
                                            disk_location=self.disk_location,
                                            disk_size=self.disk_location_size)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def fail_disk_via_disk_full(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "disk_full", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks,
                                            disk_timeout=self.disk_timeout, disk_location=self.disk_location,
                                            disk_size=self.disk_location_size)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def bring_back_failed_nodes_up(self):
        if self.failover_action == "disk_failure":
            task = AutoFailoverNodesFailureTask(self.orchestrator,
                                                self.server_to_fail,
                                                "recover_disk_failure", self.timeout,
                                                self.pause_between_failover_action,
                                                expect_auto_failover=False,
                                                timeout_buffer=self.timeout_buffer,
                                                check_for_failover=False,
                                                disk_timeout=self.disk_timeout, disk_location=self.disk_location,
                                                disk_size=self.disk_location_size)
            self.task_manager.schedule(task)
            try:
                task.result()
            except Exception as e:
                self.fail("Exception: {}".format(e))
        elif self.failover_action == "disk_full":
            task = AutoFailoverNodesFailureTask(self.orchestrator,
                                                self.server_to_fail,
                                                "recover_disk_full_failure", self.timeout,
                                                self.pause_between_failover_action,
                                                expect_auto_failover=False,
                                                timeout_buffer=self.timeout_buffer,
                                                check_for_failover=False,
                                                disk_timeout=self.disk_timeout, disk_location=self.disk_location,
                                                disk_size=self.disk_location_size)
            self.task_manager.schedule(task)
            try:
                task.result()
            except Exception as e:
                self.fail("Exception: {}".format(e))
        else:
            super(DiskAutoFailoverBasetest, self).bring_back_failed_nodes_up()
