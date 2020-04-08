import copy
import json
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from .failoverbasetests import FailoverBaseTest

class NegativeFailoverTests(FailoverBaseTest):

    def setUp(self):
        super(NegativeFailoverTests, self).setUp()

    def tearDown(self):
        super(NegativeFailoverTests, self).tearDown()

    def graceful_failover_when_rebalance_running(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.rest.node_statuses()
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            status = self.rest.rebalance(otpNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in nodes[1:]])
            self.assertTrue(status, " Rebalance did not run ")
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertFalse(success_failed_over, " Failover did not fail as expected ")
        except Exception as ex:
            self.assertTrue(("Rebalance running" in str(ex)), "unexpected exception {0}".format(ex))

    def graceful_failover_when_graceful_failover_running(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertTrue(success_failed_over, " Failover failed ")
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertFalse(success_failed_over, " Failover did not fail as expected ")
        except Exception as ex:
            self.assertTrue(("Rebalance running" in str(ex)), "unexpected exception {0}".format(ex))

    def hard_failover_when_graceful_failover_running(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertTrue(success_failed_over, " Failover failed ")
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertFalse(success_failed_over, " Failover did not fail as expected ")
        except Exception as ex:
            self.assertTrue(("Rebalance running" in str(ex)), "unexpected exception {0}".format(ex))

    def hard_failover_nonexistant_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            success_failed_over = self.rest.fail_over("non-existant", graceful=False)
            self.assertFalse(success_failed_over, " Failover did not fail as expected ")
        except Exception as ex:
            self.assertTrue(("Unknown server given" in str(ex)), "unexpected exception {0}".format(ex))

    def graceful_failover_nonexistant_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            success_failed_over = self.rest.fail_over("non-existant", graceful=True)
            self.assertFalse(success_failed_over, " Failover did not fail as expected ")
        except Exception as ex:
            self.assertTrue(("Unknown server given" in str(ex)), "unexpected exception {0}".format(ex))

    def failover_failed_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertTrue(success_failed_over, " Failover did not happen as expected ")
            fail_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertFalse(fail_failed_over, " Failover did not fail as expected ")
        except Exception as ex:
            self.assertTrue(("Unknown server given" in str(ex)), "unexpected exception {0}".format(ex))

    def addback_non_existant_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode="non-existant", recoveryType="delta")
        except Exception as ex:
            self.assertTrue(("invalid node name or node" in str(ex)), "unexpected exception {0}".format(ex))

    def addback_an_unfailed_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
        except Exception as ex:
            self.assertTrue(("invalid node name or node" in str(ex)), "unexpected exception {0}".format(ex))

    def addback_with_incorrect_recovery_type(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="xxx")
        except Exception as ex:
            self.assertTrue(("recoveryType" in str(ex)), "unexpected exception {0}".format(ex))

    def failure_recovery_delta_node_with_failover_node(self):
        try:
            self.rest = RestConnection(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=2)
            # Mark Node(s) for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            success_failed_over = self.rest.fail_over(chosen[1].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            servers_out = self.add_remove_servers(self.servers, [], [], [chosen[1]])
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], servers_out)
            rebalance.result()
        except Exception as ex:
            self.assertTrue(("deltaRecoveryNotPossible" in str(ex)), "unexpected exception {0}".format(ex))

    def failure_recovery_delta_node_before_rebalance_in(self):
        try:
            self.rest = RestConnection(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [])
            rebalance.result()
        except Exception as ex:
            self.assertTrue(("deltaRecoveryNotPossible" in str(ex)), "unexpected exception {0}".format(ex))

    def failure_recovery_delta_node_after_add_node(self):
        try:
            self.rest = RestConnection(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            self.rest.add_node(self.master.rest_username, self.master.rest_password, self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            self.nodes = self.rest.node_statuses()
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[])
            self.assertFalse(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("deltaRecoveryNotPossible" in str(ex)), "unexpected exception {0}".format(ex))

    def failure_recovery_delta_node_after_eject_node(self):
        try:
            self.rest = RestConnection(self.master)
            eject_out_node = self.find_node_info(self.master, self.servers[self.nodes_init-1])
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            self.rest.eject_node(self.master.rest_username, self.master.rest_password, self.servers[self.nodes_init-1])
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            self.nodes = self.rest.node_statuses()
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[chosen[0].id, eject_out_node.id])
            self.assertFalse(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("deltaRecoveryNotPossible" in str(ex)), "unexpected exception {0}".format(ex))

    def failure_recovery_delta_node_before_rebalance_in_out(self):
        try:
            self.rest = RestConnection(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            ejectedNodes  = []
            self.rest.add_node(self.master.rest_username, self.master.rest_password,
                self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port)
            self.rest.add_node(self.master.rest_username, self.master.rest_password,
                self.servers[self.nodes_init+1].ip, self.servers[self.nodes_init+1].port)
            self.nodes = self.rest.node_statuses()
            for server in self.nodes:
                if server.ip == self.servers[self.nodes_init].ip:
                   ejectedNodes.append(server.id)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=ejectedNodes)
            self.assertFalse(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("deltaRecoveryNotPossible" in str(ex)), "unexpected exception {0}".format(ex))

    def graceful_failover_unhealthy_node_not_allowed(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            self.stop_server(self.servers[1])
            # Mark Node for failover
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertFalse(success_failed_over, " Graceful failover allowed for unhealthy node")
        finally:
            self.start_server(self.servers[1])

    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    self.log.info("Membase stopped")
                shell.disconnect()

    def start_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.start_couchbase()
                    self.log.info("Couchbase started")
                else:
                    shell.start_membase()
                    self.log.info("Membase started")
                shell.disconnect()
                break

