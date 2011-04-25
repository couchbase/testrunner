from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
import logger
import os

class ClusterOperationHelper(object):
    #the first ip is taken as the master ip
    @staticmethod
    def cleanup_cluster(servers):
        log = logger.Logger.get_logger()
        master = servers[0]
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        allNodes = []
        toBeEjectedNodes = []
        for node in nodes:
            allNodes.append(node.id)
            if node.id.find(master.ip) < 0 and node.id.find('127.0.0.1') < 0:
                toBeEjectedNodes.append(node.id)
                for server in servers:
                    if server.ip == node.ip:
                        remote = RemoteMachineShellConnection(server)
                        info = remote.extract_remote_info()
                        remote.terminate_process(info, 'memcached')
                        remote.terminate_process(info, 'moxi')
                        break
            #let's rebalance to remove all the nodes from the master
                #this is not the master , let's remove it
                #now load data into the main bucket
        if toBeEjectedNodes:
            log.info("rebalancing all nodes in order to remove nodes")
            helper = RestHelper(rest)
            removed = helper.remove_nodes(knownNodes=allNodes,ejectedNodes=toBeEjectedNodes)
            log.info("removed all the nodes from this cluster ? ".format(removed))