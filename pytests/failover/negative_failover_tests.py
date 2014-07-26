import copy
import json
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from failoverbasetests import FailoverBaseTest

class NegativeFailoverTests(FailoverBaseTest):

    def setUp(self):
        super(NegativeFailoverTests, self).setUp(self)

    def tearDown(self):
        super(NegativeFailoverTests, self).tearDown(self)

    def graceful_failover_when_rebalance_running(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            status = self.rest.rebalance(otpNodes=nodes,ejectedNodes=nodes[1:])
            self.assertTrue(status," Rebalance did not run ")
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertFalse(success_failed_over," Failover did not fail as expected ")
        except Exception,ex:
            self.assertTrue(("Rebalance running" in str(ex)),"unexpected exception {0}".format(ex))

    def graceful_failover_when_graceful_failover_running(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertTrue(success_failed_over," Failover failed ")
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertFalse(success_failed_over," Failover did not fail as expected ")
        except Exception,ex:
            self.assertTrue(("Rebalance running" in str(ex)),"unexpected exception {0}".format(ex))

    def hard_failover_when_graceful_failover_running(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=True)
            self.assertTrue(success_failed_over," Failover failed ")
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertFalse(success_failed_over," Failover did not fail as expected ")
        except Exception,ex:
            self.assertTrue(("Rebalance running" in str(ex)),"unexpected exception {0}".format(ex))

    def hard_failover_nonexistant_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            success_failed_over = self.rest.fail_over("non-existant", graceful=False)
            self.assertFalse(success_failed_over," Failover did not fail as expected ")
        except Exception,ex:
            self.assertTrue(("Unknown server given" in str(ex)),"unexpected exception {0}".format(ex))

    def graceful_failover_nonexistant_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            success_failed_over = self.rest.fail_over("non-existant", graceful=True)
            self.assertFalse(success_failed_over," Failover did not fail as expected ")
        except Exception,ex:
            self.assertTrue(("Unknown server given" in str(ex)),"unexpected exception {0}".format(ex))

    def failover_failed_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertTrue(success_failed_over," Failover did not happen as expected ")
            fail_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertFalse(fail_failed_over," Failover did not fail as expected ")
        except Exception,ex:
            self.assertTrue(("Unknown server given" in str(ex)),"unexpected exception {0}".format(ex))

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
        except Exception, ex:
            self.assertTrue(("invalid node name or node" in str(ex)),"unexpected exception {0}".format(ex))

    def addback_an_unfailed_node(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="delta")
        except Exception, ex:
            self.assertTrue(("invalid node name or node" in str(ex)),"unexpected exception {0}".format(ex))

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
        except Exception, ex:
            self.assertTrue(("recoveryType" in str(ex)),"unexpected exception {0}".format(ex))

    def graceful_failover_unhealthy_node_not_allowed(self):
        try:
            self.rest = RestConnection(self.master)
            nodes = self.get_nodes(self.master)
            self.stop_server(self.servers[1])
            # Mark Node for failover
            chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
            success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
            self.assertFalse(success_failed_over," Graceful failover allowed for unhealthy node")
        finally:
            self.start_server(self.servers[1])

    def get_nodes(self,server):
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
