import time

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
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
        self._load_all_buckets(self.servers[0], self.initial_load_gen,
                               "create", 0)
        self._async_load_all_buckets(self.orchestrator,
                                     self.update_load_gen, "update", 0)
        self._async_load_all_buckets(self.orchestrator,
                                     self.delete_load_gen, "delete", 0)
        self.server_to_fail = self._servers_to_fail()
        self.servers_to_add = self.servers[self.nodes_init:self.nodes_init +
                                                           self.nodes_in]
        self.servers_to_remove = self.servers[self.nodes_init -
                                              self.nodes_out:self.nodes_init]
        # self.node_monitor_task = self.start_node_monitors_task()

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
        super(AutoFailoverBaseTest, self).tearDown()
        """ Delete the new zones created if zone > 1. """
        if self.input.param("zone", 1) > 1:
            rest = RestConnection(self.servers[0])
            for i in range(1, int(self.input.param("zone", 1))):
                a = "Group "
                if rest.is_zone_exist(a + str(i + 1)):
                    rest.delete_zone(a + str(i + 1))
        for node in self.servers:
            master = node
            try:
                ClusterOperationHelper.cleanup_cluster(self.servers,
                                                       master=master)
            except:
                continue
        master = self.orchestrator
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()
        if cluster_status and self.failover_orchestrator and \
                        self.remove_after_failover is not True:
            cluster_cleanup = False
            active_servers = []
            for node in cluster_status['nodes']:
                if node['clusterMembership'] == "inactiveFailed":
                    cluster_cleanup = True
                else:
                    node_ip = node['hostname']
                    server = [x for x in self.servers if "{}:{}".format(
                        x.ip, x.port) == node_ip][0]
                    active_servers.append(server)
            if cluster_cleanup:
                master = active_servers[0]
                ClusterOperationHelper.cleanup_cluster(active_servers,
                                                       master=master)
        if self.failover_orchestrator and self.remove_after_failover:
            master = self.servers[1]
            ClusterOperationHelper.cleanup_cluster(self.servers[1:],
                                                   master=master)
        if hasattr(self, "node_monitor_task"):
            if self.node_monitor_task._exception:
                self.fail("{}".format(self.node_monitor_task._exception))
            self.node_monitor_task.stop = True
        self.task_manager.shutdown(force=True)

    def enable_autofailover(self):
        """
        Enable the autofailover setting with the given timeout.
        :return: True If the setting was set with the timeout, else return
        False
        """
        status = self.rest.update_autofailover_settings(True,
                                                        self.timeout)
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
        settings = self.rest.get_autofailover_settings()
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

    def enable_firewall(self):
        """
        Enable firewall on the nodes to fail in the tests.
        :return: Nothing
        """
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
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))

    def disable_firewall(self):
        """
        Disable firewall on the nodes to fail in the tests
        :return: Nothing
        """
        self.time_start = time.time()
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "disable_firewall",
                                            self.timeout,
                                            self.pause_between_failover_action,
                                            False,
                                            self.timeout_buffer, False)
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))

    def restart_couchbase_server(self):
        """
        Restart couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
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
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))

    def stop_couchbase_server(self):
        """
        Stop couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
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
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def start_couchbase_server(self):
        """
        Start the couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "start_couchbase", self.timeout,
                                            0, False, self.timeout_buffer,
                                            False)
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))

    def stop_restart_network(self):
        """
        Stop and restart network for said timeout period on the nodes to
        fail in the tests
        :return: Nothing
        """

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
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))

    def restart_machine(self):
        """
        Restart the nodes to fail in the tests
        :return: Nothing
        """

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
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:

            self.fail("Exception: {}".format(e))
        finally:
            self.sleep(120, "Sleeping for 2 min for the machines to restart")
            for node in self.server_to_fail:
                for i in range(0, 2):
                    try:
                        shell = RemoteMachineShellConnection(node)
                        break
                    except:
                        self.log.info("Unable to connect to the host. "
                                      "Machine has not restarted")
                        self.sleep(60, "Sleep for another minute and try "
                                       "again")

    def stop_memcached(self):
        """
        Stop the memcached on the nodes to fail in the tests
        :return: Nothing
        """
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, 11211)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "stop_memcached", self.timeout,
                                            self.pause_between_failover_action,
                                            self.failover_expected,
                                            self.timeout_buffer,
                                            failure_timers=node_down_timer_tasks)
        for node_down_timer_task in node_down_timer_tasks:
            self.node_failure_task_manager.schedule(node_down_timer_task, 2)
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))
        finally:
            task = AutoFailoverNodesFailureTask(self.orchestrator,
                                                self.server_to_fail,
                                                "start_memcached",
                                                self.timeout, 0, False, 0,
                                                check_for_failover=False)
            self.task_manager.schedule(task)
            task.result()

    def split_network(self):
        """
        Split the network in the cluster. Stop network traffic from few
        nodes while allowing the traffic from rest of the cluster.
        :return: Nothing
        """
        self.time_start = time.time()
        if self.server_to_fail.__len__() < 2:
            self.fail("Need atleast 2 servers to fail")
        task = AutoFailoverNodesFailureTask(self.orchestrator,
                                            self.server_to_fail,
                                            "network_split", self.timeout,
                                            self.pause_between_failover_action,
                                            False,
                                            self.timeout_buffer)
        self.task_manager.schedule(task)
        try:
            task.result()
        except Exception, e:
            self.fail("Exception: {}".format(e))

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
        self.services = self.input.param("services", None)
        self.zone = self.input.param("zone", 1)
        self.multi_services_node = self.input.param("multi_services_node",
                                                    False)
        self.pause_between_failover_action = self.input.param(
            "pause_between_failover_action", 0)
        self.remove_after_failover = self.input.param(
            "remove_after_failover", False)
        self.timeout_buffer = 120 if self.failover_orchestrator else 3
        failover_not_expected = self.num_node_failures > 1 and \
                                self.pause_between_failover_action < \
                                self.timeout or self.num_replicas < 1
        self.failover_expected = not failover_not_expected
        if self.failover_action is "restart_server":
            self.num_items *= 100
        self.orchestrator = self.servers[0] if not \
            self.failover_orchestrator else self.servers[
            self.num_node_failures]

    failover_actions = {
        "firewall": enable_firewall,
        "stop_server": stop_couchbase_server,
        "restart_server": restart_couchbase_server,
        "restart_machine": restart_machine,
        "restart_network": stop_restart_network,
        "stop_memcached": stop_memcached,
        "network_split": split_network
    }
