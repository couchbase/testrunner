import os
import random
import unittest
import socket
import zlib
import ctypes
import uuid
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection, RestHelper

class AddRebalanceNodesTest(unittest.TestCase):
    version = None
    ips = None
    keys = None

    def setUp(self):
        self.ips = self.extract_server_ips()
        #make sure the master node does not have any other node
        #loop through all nodes and remove those nodes left over
        #from previous test runs
        master = self.ips[0]
        rest = RestConnection(ip=self.ips[0],
                              username='Administrator',
                              password='password')
        nodes = rest.node_statuses()
        for node in nodes:
            if node.id.find(master) < 0:
                #this is not the master , let's remove it
                rest.eject_node(otpNode=node.id,
                                user='Administrator',
                                password='password')
                print 'node {0} is removed from the cluster'.format(node.id)
            #now load data into the main bucket
        self._load_data()

    def _load_data(self):
        master = self.ips[0]
        client = MemcachedClient(master, 11211)
        #populate key
        testuuid = uuid.uuid4()
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(100)]

        for key in self.keys:
            payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0',
                                            random.randint(100, 1024))
            flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
            try:
                (opaque, cas, data) = client.set(key, 0, flag, payload)
            except MemcachedError as error:
                print error
                client.close()
                self.fail("unable to push key : {0} to bucket : {1}".format(key, client.vbucketId))
        client.close()


    def _erase_data(self):
        master = self.ips[0]
        client = MemcachedClient(master, 11211)
        for key in self.keys:
            try:
                self.clients[ip].delete(key=key)
            except MemcachedError as error:
                print error
                client.close()
                self.fail('unable to delete key : {0} from memcached @ {1}'.format(key, ip))
        client.close()

    def test_add_rebalance_remove(self):
        #add nodes
        index = 0
        rest = None
        otpNodes = []
        master = ''
        for ip in self.ips:
            if index == 0:
                rest = RestConnection(ip=ip,
                                      username='Administrator',
                                      password='password')
                master = ip
                print 'master ip : ', master
            else:
                otpNode = rest.add_node(user='Administrator',
                                        password='password',
                                        remoteIp=ip)
                if otpNode:
                    print 'added node : {0} to the cluster'.format(otpNode.id)
                    otpNodes.append(otpNode)

            index += 1
            #rebalance
        #create knownNodes
        otpNodeIds = ['ns_1@' + master]
        for otpNode in otpNodes:
            otpNodeIds.append(otpNode.id)
        rebalanceStarted = rest.rebalance(otpNodeIds)
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master))
        print 'started rebalance operation on master node {0}'.format(master)
        rebalanceSucceeded = rest.monitorRebalance()
        self.assertTrue(rebalanceSucceeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        print 'rebalance operaton succeeded for nodes: {0}'.format(otpNodeIds)
        #now remove the nodes
        #make sure its rebalanced and node statuses are healthy
        helper = RestHelper(rest)
        self.assertTrue(helper.is_cluster_healthy, "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced, "cluster is not balanced")
        for otpNode in otpNodes:
            print 'otpNode : ', otpNode.id, otpNode.ip
            rest.eject_node(otpNode=otpNode.id,
                            user='Administrator',
                            password='password')
            print 'ejected node {0} from master {1}'.format(otpNode.id, master)

    def tearDown(self):
        self._erase_data()
        master = self.ips[0]
        rest = RestConnection(ip=self.ips[0],
                              username='Administrator',
                              password='password')
        nodes = rest.node_statuses()
        for node in nodes:
            if node.id.find(master) < 0:
                #this is not the master , let's remove it
                rest.eject_node(otpNode=node.id,
                                user='Administrator',
                                password='password')
                print 'node {0} is removed from the cluster'.format(node.id)


    def extract_server_ips(self):
        servers_string = os.getenv("SERVERS")
        servers = servers_string.split(" ")
        return [server[:server.index(':')] for server in servers]

    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]
