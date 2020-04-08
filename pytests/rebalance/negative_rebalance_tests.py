import time

from threading import Thread
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper

class NegativeRebalanceTests(RebalanceBaseTest):

    def setUp(self):
        super(NegativeRebalanceTests, self).setUp()

    def tearDown(self):
        super(NegativeRebalanceTests, self).tearDown()

    def pass_no_arguments(self):
        try:
            self.rest = RestConnection(self.master)
            status = self.rest.rebalance(otpNodes=[], ejectedNodes=[])
            self.assertFalse(status, " Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("empty_known_nodes" in str(ex)), "Rebalance did not fail as expected, got an unexpected exception {0}".format(ex))

    def add_no_nodes(self):
        self.rest = RestConnection(self.master)
        nodes = self.get_nodes(self.master)
        status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=[])
        self.assertTrue(status, " Rebalance did not fail as expected")

    def remove_all_nodes(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes)
            self.assertTrue(status, " Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("No active nodes" in str(ex)), "Rebalance did not fail as expected, got an unexpected exception {0}".format(ex))

    def pass_non_existant_nodes(self):
        try:
            self.rest = RestConnection(self.master)
            status = self.rest.rebalance(otpNodes=['non-existant'], ejectedNodes=['non-existant'])
            self.assertFalse(status, " Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("mismatch" in str(ex)), "Rebalance did not fail as expected, got an unexpected exception {0}".format(ex))

    def non_existant_recovery_bucket(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes[1:], deltaRecoveryBuckets =['non-existant'])
            self.assertFalse(status, " Rebalance did not fail as expected")
        except Exception as ex:
            self.assertTrue(("deltaRecoveryNotPossible" in str(ex)), "Rebalance did not fail as expected, got an unexpected exception {0}".format(ex))

    def not_ready_for_recovery(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            # Mark Node for failover
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.stop_server(self.servers[1])
            # Mark Node for full recovery
            if success_failed_over:
                self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
            status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes[1:])
            self.assertFalse(status, " Rebalance did not fail as expected ")
        finally:
            self.start_server(self.servers[1])

    def node_down_cannot_rebalance(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            self.stop_server(self.servers[1])
            status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes[1:])
            self.assertFalse(status, " Rebalance did not fail as expected ")
        finally:
            self.start_server(self.servers[1])

    def rebalance_running_cannot_rebalance(self):
        self.rest = RestConnection(self.master)
        nodes = self.get_nodes(self.master)
        status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes[1:])
        status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes[1:])
        self.assertFalse(status, "Rebalance did not fail as expected ")

    def rebalance_graceful_failover_running_cannot_rebalance(self):
        self.rest = RestConnection(self.master)
        nodes = self.get_nodes(self.master)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
        status = self.rest.rebalance(otpNodes=nodes, ejectedNodes=nodes[1:])
        self.assertFalse(status, " Rebalance did not fail as expected ")

    def get_nodes(self, server):
        self.rest = RestConnection(self.master)
        nodes = self.rest.node_statuses()
        nodes = [node.id for node in nodes]
        return nodes

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