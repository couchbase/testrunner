from random import shuffle
import logger
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection

num_nodes_mismatch = "self.server has {0} nodes but cluster has {1} nodes"
not_enough_nodes = "there are not enough nodes to failover.cluster has{0} nodes and test wants to failover {1} nodes"
start_cluster = "rpc:call('{0}', ns_server_cluster_sup, start_cluster, [], infinity)."
stop_cluster = "rpc:call('{0}', ns_server_cluster_sup, stop_cluster, [], infinity)."

class FailoverHelper(object):
    def __init__(self, servers, test):
        self.log = logger.Logger.get_logger()
        self.servers = servers
        self.test = test
        #master is usually the first node ?

    # failover any node except self.servers[0]
    # assuming that replica = howmany
    def failover(self, howmany):
        #chekck if all nodes are part of the cluster
        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()
        if len(nodes) != len(self.servers):
            self.test.fail(num_nodes_mismatch.format(len(self.servers), len(nodes)))
        if len(nodes) - howmany < 2:
            self.test.fail(num_nodes_mismatch.format(len(nodes), howmany))
        master_node = rest.get_nodes_self()
        #when selecting make sure we dont pick the master node
        selection=[n for n in nodes if n.id!=master_node.id]

        shuffle(selection)
        failed = selection[0:howmany]
        for f in failed:
            self.log.info("will fail over node : {0}".format(f.id))

        if len(nodes) / (1 + howmany) >= 1:
            self.test.assertTrue(RestHelper(rest).wait_for_replication(900),
                            msg="replication did not finish after 15 minutes")
            for f in failed:
                self._stop_server(f)
                self.log.info("10 seconds delay to wait for membase-server to shutdown")
            #wait for 5 minutes until node is down

            for f in failed:
                if f.port == 8091:
                    self.test.assertTrue(RestHelper(rest).wait_for_node_status(f, "unhealthy", 300),
                                msg="node status is not unhealthy even after waiting for 5 minutes")
                self.test.assertTrue(rest.fail_over(f.id), msg="failover did not complete")
                self.log.info("failed over node : {0}".format(f.id))
        return failed


    # Start and add the failovered nodes back to the cluster and rebalance it
    def undo_failover(self, failover_nodes):
        self.log.info("Add nodes back to the cluster: {0}".format(failover_nodes))
        rest = RestConnection(self.servers[0])

        self._start_servers(failover_nodes)
        rest.rebalance(otpNodes=set([node.id for node in rest.node_statuses()] +
                                    [node.id for node in failover_nodes]),
                       ejectedNodes=[])
        rest.monitorRebalance()


    def _stop_server(self, node):
        master_rest = RestConnection(self.servers[0])
        for server in self.servers:
            rest = RestConnection(server)
            self.log.info("see if server {0}:{1} is running".format(server.ip, server.port))
            if not RestHelper(rest).is_ns_server_running(timeout_in_seconds=5):
                continue
            node_id = rest.get_nodes_self().id
            if node_id == node.id:
                # if its 8091 then do ssh otherwise use ns_servr
                if node.port == 8091:
                    shell = RemoteMachineShellConnection(server)
                    if shell.is_membase_installed():
                        shell.stop_membase()
                        self.log.info("Membase stopped")
                    else:
                        shell.stop_couchbase()
                        self.log.info("Couchbase stopped")
                    shell.disconnect()
                    break
                else:
                    self.log.info("running {0}".format(stop_cluster.format(node.id)))
                    master_rest.diag_eval(stop_cluster.format(node.id))


    def _start_servers(self, nodes):
        for node in nodes:
            for server in self.servers:
                if node.ip == server.ip and str(node.port) == server.port:
                    shell = RemoteMachineShellConnection(server)
                    shell.start_couchbase()