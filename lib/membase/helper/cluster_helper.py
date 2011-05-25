from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
import logger
import os

class ClusterOperationHelper(object):
    #the first ip is taken as the master ip

    @staticmethod
    def add_all_nodes_or_assert(master,all_servers,rest_settings,test_case):
        log = logger.Logger.get_logger()
        otpNodes = []
        all_nodes_added = True
        rest = RestConnection(master)
        for serverInfo in all_servers:
            if serverInfo.ip != master.ip:
                log.info('adding node : {0} to the cluster'.format(serverInfo.ip))
                otpNode = rest.add_node(rest_settings.rest_username,
                                        rest_settings.rest_password,
                                        serverInfo.ip)
                if otpNode:
                    log.info('added node : {0} to the cluster'.format(otpNode.id))
                    otpNodes.append(otpNode)
                else:
                    all_nodes_added = False
        if not all_nodes_added:
            if test_case:
                test_case.assertTrue(all_nodes_added,
                                     msg="unable to add all the nodes to the cluster")
            else:
                log.error("unable to add all the nodes to the cluster")
        return otpNodes

    @staticmethod
    def wait_for_ns_servers_or_assert(servers,testcase):
        for server in servers:
            rest = RestConnection(server)
            testcase.assertTrue(RestHelper(rest).is_ns_server_running(),
                            "ns_server is not running in {0}".format(server.ip))

    @staticmethod
    def cleanup_cluster(servers):
        log = logger.Logger.get_logger()
        for master in servers:
            rest = RestConnection(master)
            nodes = rest.node_statuses()
            allNodes = []
            toBeEjectedNodes = []
            for node in nodes:
                allNodes.append(node.id)
                if node.id.find(master.ip) < 0 and node.id.find('127.0.0.1') < 0:
                    toBeEjectedNodes.append(node.id)
                #let's rebalance to remove all the nodes from the master
                    #this is not the master , let's remove it
                    #now load data into the main bucket
            if toBeEjectedNodes:
                log.info("rebalancing all nodes in order to remove nodes")
                helper = RestHelper(rest)
                removed = helper.remove_nodes(knownNodes=allNodes,ejectedNodes=toBeEjectedNodes)
                log.info("removed all the nodes from this cluster ? ".format(removed))

    @staticmethod
    def rebalance_params_for_declustering(master,all_nodes):
        log = logger.Logger.get_logger()
        otpNodeIds = [node.id for node in all_nodes]
        knownNodes = []
        knownNodes.extend(otpNodeIds)
        if 'ns_1@' + master.ip in otpNodeIds:
            otpNodeIds.remove('ns_1@' + master.ip)
        if 'ns_1@127.0.0.1' in otpNodeIds:
            otpNodeIds.remove('ns_1@127.0.0.1')
        ejectedNodes = otpNodeIds
        log.info('ejectedNodes : {0} , knownNodes : {1}'.format(ejectedNodes,knownNodes))
        return knownNodes,ejectedNodes