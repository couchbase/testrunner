from membase.api.rest_client import RestConnection, RestHelper
import logger

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
            log = logger.Logger.get_logger()
            log.info("waiting for ns_server @ {0}:{1}".format(server.ip, server.port))
            testcase.assertTrue(RestHelper(rest).is_ns_server_running(),
                            "ns_server is not running in {0}".format(server.ip))

    @staticmethod
    def cleanup_cluster(servers):
        log = logger.Logger.get_logger()
        master = servers[0]
#        for master in servers:
        rest = RestConnection(master)
        RestHelper(rest).is_ns_server_running(timeout_in_seconds=120)
        nodes = rest.node_statuses()
        allNodes = []
        toBeEjectedNodes = []
        for node in nodes:
            allNodes.append(node.id)
            if "{0}:{1}".format(node.ip,node.port) != "{0}:{1}".format(master.ip,master.port):
                toBeEjectedNodes.append(node.id)
            #let's rebalance to remove all the nodes from the master
                #this is not the master , let's remove it
                #now load data into the main bucket
        if len(allNodes) > len(toBeEjectedNodes) and toBeEjectedNodes:
            log.info("rebalancing all nodes in order to remove nodes")
            helper = RestHelper(rest)
            removed = helper.remove_nodes(knownNodes=allNodes,ejectedNodes=toBeEjectedNodes)
            log.info("removed all the nodes from cluster associated with {0} ? {1}".format(master.ip, removed))