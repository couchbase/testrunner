import time

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from tasks.task import AutoFailoverNodesFailureTask
from tasks.taskmanager import TaskManager


class AutoFailoverBaseTest(BaseTestCase):
    MAX_FAIL_DETECT_TIME = 120

    def setUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self.timeout = self.input.param("timeout", 60)
        self.failover_action = self.input.param("failover_action",
                                                "stop_server")
        self.failover_orchestrator = self.input.param("failover_orchestrator",
                                                      False)
        self.multiple_node_failure = self.input.param("multiple_nodes_failure",
                                                      False)
        self.update_items = self.input.param("update_items", 1000)
        self.delete_items = self.input.param("delete_items", 1000)
        self.add_back_node = self.input.param("add_back_node", True)
        self.buckets
        self.recovery_strategy = self.input.param("recovery_strategy",
                                                  "graceful")
        self.multi_node_failures = self.input.param("multi_node_failures",
                                                    False)
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.services = self.input.param("services", None)
        self.zone = self.input.param("zone", 1)
        self.multi_services_node = self.input.param("multi_services_node",
                                                    False)
        self.pause_between_failover_action = self.input.param(
            "pause_between_failover_action", 0)
        self.rest = RestConnection(self.master)
        self.task_manager = TaskManager("Autofailover_thread")
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
                                             end=self.delete_items)
        self._load_all_buckets(self.servers[0], self.initial_load_gen,
                               "create", self.expiry)
        self.server_to_fail = self._servers_to_fail()
        self.servers_to_add = self.servers[self.nodes_init:self.nodes_init +
                                                           self.nodes_in]
        self.servers_to_remove = self.servers[self.nodes_init -
                                              self.nodes_out:self.nodes_init]

    def tearDown(self):
        super(AutoFailoverBaseTest, self).tearDown()
        """ Delete the new zones created if zone > 1. """
        if self.input.param("zone", 1) > 1:
            rest = RestConnection(self.servers[0])
            for i in range(1, int(self.input.param("zone", 1))):
                a = "Group "
                if rest.is_zone_exist(a + str(i + 1)):
                    rest.delete_zone(a + str(i + 1))

    def post_failover_steps(self):
        self.log.info("Post Failover Steps")

    def enable_autofailover(self):
        status = self.rest.update_autofailover_settings(True,
                                                        self.timeout)
        return status

    def disable_autofailover(self):
        status = self.rest.update_autofailover_settings(False, 120)
        return status

    def enable_autofailover_and_validate(self):
        status = self.enable_autofailover()
        self.assertTrue(status, "Failed to enable autofailover_settings!")
        settings = self.rest.get_autofailover_settings()
        self.assertTrue(settings.enabled, "Failed to enable "
                                          "autofailover_settings!")
        self.assertEqual(self.timeout, settings.timeout,
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}".format(self.timeout,
                                                           settings.timeout))

    def disable_autofailover_and_validate(self):
        status = self.disable_autofailover()
        self.assertTrue(status, "Failed to change autofailover_settings!")
        settings = self.rest.get_autofailover_settings()
        self.assertFalse(settings.enabled, "Failed to disable "
                                           "autofailover_settings!")

    def wait_for_failover_or_assert(self, master, autofailover_count):
        time_start = time.time()
        time_max_end = time_start + self.timeout
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = self.get_failover_count(master)
            if failover_count == autofailover_count:
                self.log.info(
                    "{0} nodes failed over as expected".format(failover_count))
                self.log.info(
                    "expected failover in {0} seconds, actual time {1} "
                    "seconds".format
                    (self.timeout - AutoFailoverBaseTest.MAX_FAIL_DETECT_TIME,
                     time.time() - time_start))
                return
            time.sleep(2)
        if self.num_node_failures > 1:
            self.log.info("No nodes were failed over since number of node "
                          "failures was greater than 1.")
            return
        rest = RestConnection(master)
        rest.print_UI_logs()
        self.log.warn("pools/default from {0} : {1}".format
                      (master.ip, rest.cluster_status()))
        self.fail("{0} nodes failed over, expected {1} in {2} seconds".
                  format(failover_count, autofailover_count,
                         time.time() - time_start))

    def wait_for_no_failover_or_assert(self, master, autofailover_count):
        time_start = time.time()
        time_max_end = time_start + self.timeout
        failover_count = 0

        while time.time() < time_max_end:
            failover_count = self.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        time_end = time.time()

        self.assertFalse(failover_count == autofailover_count,
                         "{0} nodes failed over, didn't expect {1} in {"
                         "2} seconds".
                         format(failover_count, autofailover_count,
                                time.time() - time_start))
        self.log.info(
            "{0} nodes failed over as expected in {1} seconds".format(
                failover_count, time_end - time_start))

    def get_failover_count(self, master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            self.log.info("'clusterMembership' for node {0} is {1}".format(
                node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def enable_firewall(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "enable_firewall", self.timeout,
                                            self.pause_between_failover_action)
        self.task_manager.schedule(task)
        task.result()

    def disable_firewall(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "disable_firewall",
                                            self.timeout,
                                            self.pause_between_failover_action)
        self.task_manager.schedule(task)
        task.result()

    def restart_couchbase_server(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "restart_couchbase",
                                            self.timeout,
                                            self.pause_between_failover_action)
        self.task_manager.schedule(task)
        task.result()

    def stop_couchbase_server(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "stop_couchbase", self.timeout,
                                            self.pause_between_failover_action)
        self.task_manager.schedule(task)
        task.result()

    def start_couchbase_server(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "start_couchbase", self.timeout,
                                            self.pause_between_failover_action)
        self.task_manager.schedule(task)
        task.result()

    def stop_restart_network(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "restart_network", self.timeout,
                                            self.pause_between_failover_action)

        self.task_manager.schedule(task)
        task.result()

    def restart_machine(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "restart_machine", self.timeout,
                                            self.pause_between_failover_action)

        self.task_manager.schedule(task)
        task.result()

    def stop_memcached(self):
        task = AutoFailoverNodesFailureTask(self.server_to_fail,
                                            "stop_memcached", self.timeout,
                                            self.pause_between_failover_action)
        self.task_manager.schedule(task)
        task.result()

    def _servers_to_fail(self):
        if self.failover_orchestrator:
            servers_to_fail = self.servers[0:self.num_node_failures]
        else:
            servers_to_fail = self.servers[1:self.num_node_failures]
        return servers_to_fail

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1. Rebalance the
        nodes in the end.
        Nodes are divided into groups iteratively i.e. 1st node in Group 1,
        2nd in Group 2, 3rd in Group 1 and so on, when
        zone=2.
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        serverinfo = self.servers[0]
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
            nodes_in_cluster = [node.ip for node in
                                self.get_nodes_in_cluster()]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.servers)):
                if self.servers[i].ip in nodes_in_cluster and \
                                self.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(
                        self.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in
                                         rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses() if
                           node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes,
                                 ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Verify replicas of one node should not be in the same zone as
        # active vbuckets of the node.
        if self.zone > 1:
            self._verify_replica_distribution_in_zones(nodes_in_zone)

    def add_remove_servers_and_rebalance(self, to_add, to_remove):
        """
        Add and/or remove servers and rebalance.
        :param to_add: List of nodes to be added.
        :param to_remove: List of nodes to be removed.
        """
        serverinfo = self.servers[0]
        rest = RestConnection(serverinfo)
        for node in to_add:
            rest.add_node(user=serverinfo.rest_username,
                          password=serverinfo.rest_password,
                          remoteIp=node.ip)
        self.shuffle_nodes_between_zones_and_rebalance(to_remove)

    failover_actions = {
        "firewall": enable_firewall,
        "stop_server": disable_firewall,
        "restart_server": restart_couchbase_server,
        "restart_machine": restart_machine,
        "restart_network": stop_restart_network,
        "stop_memcached": stop_memcached
    }
