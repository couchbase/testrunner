import os
import unittest
import socket
import zlib
import ctypes
import uuid
from TestInput import TestInputSingleton
import logger
import crc32
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection

class AddRebalanceNodesTest(unittest.TestCase):
    version = None
    input = None
    servers = None
    keys = None
    clients = None
    bucket_name = None
    keys_not_pushed = None
    log = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        self.bucket_name = 'default'
        rest = RestConnection(self.servers[0])
        rest.init_cluster()
        rest.init_cluster_memoryQuota(username=self.input.membase_settings.rest_username,
                                      password=self.input.membase_settings.rest_password,
                                      memoryQuota=200)

        rest.create_bucket(bucket=self.bucket_name,
                           ramQuotaMB=256,
                           replicaNumber=1,
                           proxyPort=11211)
        remote = RemoteMachineShellConnection(self.servers[0])
        info = remote.extract_remote_info()
        remote.terminate_process(info, 'memcached')
        remote.terminate_process(info, 'moxi')
        BucketOperationHelper.wait_till_memcached_is_ready_or_assert(servers=[self.servers[0]],
                                                                     bucket_port=11211,
                                                                     test=self)

    #let's try to fill up ram up to y percentage
    def _load_data(self, fill_ram_percentage=10.0):
        if fill_ram_percentage <= 0.0:
            fill_ram_percentage = 5.0
        master = self.servers[0]
        client = MemcachedClient(master.ip, 11211)
        #populate key
        rest = RestConnection(master)
        testuuid = uuid.uuid4()
        info = rest.get_bucket(self.bucket_name)
        emptySpace = info.stats.ram - info.stats.memUsed
        self.log.info('emptySpace : {0} fill_ram_percentage : {1}'.format(emptySpace, fill_ram_percentage))
        fill_space = (emptySpace * fill_ram_percentage) / 100.0
        self.log.info("fill_space {0}".format(fill_space))
        # each packet can be 10 KB
        packetSize = int(10 * 1024)
        number_of_buckets = int(fill_space) / packetSize
        self.log.info('packetSize: {0}'.format(packetSize))
        self.log.info('memory usage before key insertion : {0}'.format(info.stats.memUsed))
        self.log.info('inserting {0} new keys to memcached @ {0}'.format(number_of_buckets, master.ip))
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(number_of_buckets)]
        self.keys_not_pushed = []
        for key in self.keys:
            vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
            client.vbucketId = vbucketId
            payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0', 10 * 1024)
            flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
            try:
                client.set(key, 0, flag, payload)
            except MemcachedError as error:
                self.log.error(error)
                self.log.error("unable to push key : {0} to bucket : {1}".format(key, client.vbucketId))
                self.keys_not_pushed.append(key)
        client.close()


    #let's parametrize number of nodes!!


    def test_rebalance_0_1_percent_ram(self):
        self.add_rebalance_remove(0.1)

    def test_rebalance_1_percent_ram(self):
        self.add_rebalance_remove(1.0)

    def test_rebalance_5_percent_ram(self):
        self.add_rebalance_remove(5.0)

    def test_rebalance_10_percent_ram(self):
        self.add_rebalance_remove(10.0)

    def test_rebalance_50_percent_ram(self):
        self.add_rebalance_remove(50.0)

    def test_rebalance_99_percent_ram(self):
        self.add_rebalance_remove(99.0)

    #add them all rebalance
    def add_rebalance_remove(self, load_percentage):
        self._load_data(load_percentage)
        #add nodes
        index = 0
        otpNodes = []
        master = self.servers[0]
        rest = RestConnection(master)
        all_nodes_added = True
        for serverInfo in self.servers:
            if index > 0:
                self.log.info('adding node : {0} to the cluster'.format(serverInfo.ip))
                otpNode = rest.add_node(user=self.input.membase_settings.rest_username,
                                        password=self.input.membase_settings.rest_password,
                                        remoteIp=serverInfo.ip)
                if otpNode:
                    self.log.info('added node : {0} to the cluster'.format(otpNode.id))
                    otpNodes.append(otpNode)
                else:
                    all_nodes_added = False
            index += 1
            #rebalance
        #create knownNodes
        #let's kill all memcached
        self.assertTrue(all_nodes_added,
                        msg="unable tgit logo add the nodes to the cluster")
        nodes = rest.node_statuses()
        otpNodeIds = []
        for node in nodes:
            otpNodeIds.append(node.id)
            #vbucketmapbefore
        #how about the main node ?
        before_info = rest.get_bucket(self.bucket_name)

        rebalanceStarted = rest.rebalance(otpNodeIds,[])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master.ip))
        self.log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        self.assertTrue(rebalanceSucceeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        self.log.info('rebalance operaton succeeded for nodes: {0}'.format(otpNodeIds))
        after_info = rest.get_bucket(self.bucket_name)
        RebalanceHelper.verify_maps(before_info.forward_map, after_info.vbuckets)
        #now remove the nodes
        #make sure its rebalanced and node statuses are healthy
        helper = RestHelper(rest)
        self.assertTrue(helper.is_cluster_healthy, "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced, "cluster is not balanced")
        self.assertTrue(helper.wait_for_replication(180),
                        msg="replication did not complete")

        allnodes = []
        allnodes.extend(otpNodeIds)
        if 'ns_1@' + master.ip in otpNodeIds:
            otpNodeIds.remove('ns_1@' + master.ip)
        if 'ns_1@127.0.0.1' + master.ip in otpNodeIds:
            otpNodeIds.remove('ns_1@127.0.0.1')
        helper.remove_nodes(knownNodes=allnodes,
                            ejectedNodes=otpNodeIds)

        #add them all rebalance

    #    def add_rebalance_remove_one_by_one(self, load_percentage):
    #        self._load_data(load_percentage)
    #        #add nodes
    #        index = 0
    #        otpNodes = []
    #        master = self.ips[0]
    #        rest = RestConnection(ip=master,
    #                              username='Administrator',
    #                              password='password')
    #        all_nodes_added = True
    #        for ip in self.ips:
    #            if index > 0:
    #                self.log.info('adding node : {0} to the cluster'.format(ip))
    #                otpNode = rest.add_node(user='Administrator',
    #                                        password='password',
    #                                        remoteIp=ip)
    #                if otpNode:
    #                    self.log.info('added node : {0} to the cluster'.format(otpNode.id))
    #                    otpNodes.append(otpNode)
    ##                    remote = RemoteMachineShellConnection(ip=ip,
    ##                                                          username='root',
    ##                                                          pkey_location=os.getenv("KEYFILE"))
    ##                    remote.execute_command('killall -9 memcached;killall -9 moxi;')
    ##                    self.log.info('killing memcached and moxi on {0} after adding the node.'.format(ip))
    ##                    time.sleep(5)
    #                else:
    #                    all_nodes_added = False
    #            index += 1
    #            #rebalance
    #        #create knownNodes
    #        #let's kill all memcached
    #        self.assertTrue(all_nodes_added,
    #                        msg="unable to add the nodes to the cluster")
    #        otpNodeIds = ['ns_1@' + master]
    #        for otpNode in otpNodes:
    #            otpNodeIds.append(otpNode.id)
    #            #vbucketmapbefore
    #        before_info = rest.get_bucket(self.bucket_name)
    #
    #        rebalanceStarted = rest.rebalance(otpNodeIds)
    #        self.assertTrue(rebalanceStarted,
    #                        "unable to start rebalance on master node {0}".format(master))
    #        self.log.info('started rebalance operation on master node {0}'.format(master))
    #        rebalanceSucceeded = rest.monitorRebalance()
    #        self.assertTrue(rebalanceSucceeded,
    #                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
    #        self.log.info('rebalance operaton succeeded for nodes: {0}'.format(otpNodeIds))
    #        after_info = rest.get_bucket(self.bucket_name)
    #        RebalanceHelper.verify_maps(before_info.forward_map, after_info.vbuckets)
    #        #now remove the nodes
    #        #make sure its rebalanced and node statuses are healthy
    #        helper = RestHelper(rest)
    #        self.assertTrue(helper.is_cluster_healthy, "cluster status is not healthy")
    #        self.assertTrue(helper.is_cluster_rebalanced, "cluster is not balanced")
    #        self.assertTrue(helper.wait_for_replication(180),
    #                        msg="replication did not complete")
    #
    #        allnodes = []
    #        allnodes.extend(otpNodeIds)
    #        otpNodeIds.remove('ns_1@' + master)
    #        helper.remove_nodes(knownNodes=allnodes,
    #                            ejectedNodes=otpNodeIds)


    #        self.log.info('ejected node {0} from master {1}'.format(otpNode.id, master))

    def tearDown(self):
        #let's just clean up the buckets in the master node
        BucketOperationHelper.delete_all_buckets_or_assert([self.servers[0]], self)


    def extract_server_ips(self):
        servers_string = os.getenv("SERVERS")
        servers = servers_string.split(" ")
        return [server[:server.index(':')] for server in servers]

    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]


        #test cases for adding the node and rebalancing one by one

#test case : keep injecting data while test is running