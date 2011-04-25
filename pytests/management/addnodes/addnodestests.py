import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.exception import ServerJoinException, MembaseHttpExceptionTypes, ServerAlreadyJoinedException
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
import time

class AddNodesTests(unittest.TestCase):


    input = None
    servers = None
    log = None
    membase = None

    # simple addnode tests without rebalancing
    # add node to itself
    # add an already added node
    # add node and remove them 10 times serially
    # add node and remove the node in parallel threads later...

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.membase = self.input.membase_settings

    #we dont necessarily care about the test case
    def common_setUp(self,with_buckets = False):
        #depending on buckets
        ClusterOperationHelper.cleanup_cluster(self.servers)
        if with_buckets:
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers,test_case=self)
            BucketOperationHelper.create_default_buckets(servers=self.servers,
                                                         number_of_replicas=1,
                                                         assert_on_test=self)


    def common_tearDown(self,with_buckets):
        if with_buckets:
            BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers,test_case=self)


    #add nodes one by one
    def _add_1_node_body(self):
        master = self.servers[0]
        master_rest = RestConnection(master)
        for i in range(1,len(self.servers)):
            ip = self.servers[i].ip
            self.log.info('adding node : {0} to the cluster'.format(ip))
            otpNode = master_rest.add_node(user=self.membase.rest_username,
                                           password=self.membase.rest_password,
                                           remoteIp=ip)
            if otpNode:
                self.log.info('added node : {0} to the cluster'.format(otpNode.id))
                #now lets eject it
                self.log.info("ejecting the node {0}".format(otpNode.id))
                ejected = master_rest.eject_node(user=self.membase.rest_username,
                                                 password=self.membase.rest_password,
                                                 otpNode=otpNode.id)
                self.assertTrue(ejected,
                                msg="unable to eject the node {0}".format(otpNode.id))
            else:
                self.fail(msg="unable to add node : {0} to the cluster".format(ip))
            time.sleep(5)

    def _add_all_node_body(self):
        self.common_setUp(False)
        master = self.servers[0]
        master_rest = RestConnection(master)
        added_otps = []
        for i in range(1,len(self.servers)):
            ip = self.servers[i].ip
            self.log.info('adding node : {0} to the cluster'.format(ip))
            otpNode = master_rest.add_node(user=self.membase.rest_username,
                                           password=self.membase.rest_password,
                                           remoteIp=ip)
            if otpNode:
                added_otps.append(otpNode)
                self.log.info('added node : {0} to the cluster'.format(otpNode.id))
            else:
                self.fail(msg="unable to add node : {0} to the cluster".format(ip))
            time.sleep(5)
        for otpNode in added_otps:
                #now lets eject it
                self.log.info("ejecting the node {0}".format(otpNode.id))
                ejected = master_rest.eject_node(user=self.membase.rest_username,
                                                 password=self.membase.rest_password,
                                                 otpNode=otpNode.id)
                self.assertTrue(ejected,
                                msg="unable to eject the node {0}".format(otpNode.id))

    def _add_node_itself_body(self):
        self.common_setUp(False)
        master = self.servers[0]
        master_rest = RestConnection(master)
        self.log.info('adding node : {0} to the cluster'.format(master))
        try:
            master_rest.add_node(user=self.membase.rest_username,
                                           password=self.membase.rest_password,
                                           remoteIp=master.ip)
            self.fail("server did not raise any exception while adding the node to itself")
        except ServerJoinException as ex:
            self.assertEquals(ex.type,MembaseHttpExceptionTypes.NODE_CANT_ADD_TO_ITSELF)

    def _add_node_already_added_body(self):
        self.common_setUp(False)
        master = self.servers[0]
        master_rest = RestConnection(master)
        for i in range(1,len(self.servers)):
            ip = self.servers[i].ip
            self.log.info('adding node : {0} to the cluster'.format(ip))
            otpNode = master_rest.add_node(user=self.membase.rest_username,
                                           password=self.membase.rest_password,
                                           remoteIp=ip)
            if otpNode:
                self.log.info('added node : {0} to the cluster'.format(otpNode.id))
                #try to add again
                try:
                    readd_otpNode = master_rest.add_node(user=self.membase.rest_username,
                                           password=self.membase.rest_password,
                                           remoteIp=ip)
                    if readd_otpNode:
                        self.fail("server did not raise any exception when calling add_node on an already added node")
                except ServerAlreadyJoinedException:
                    self.log.info("server raised ServerAlreadyJoinedException as expected")

                #now lets eject it
                self.log.info("ejecting the node {0}".format(otpNode.id))
                ejected = master_rest.eject_node(user=self.membase.rest_username,
                                                 password=self.membase.rest_password,
                                                 otpNode=otpNode.id)
                self.assertTrue(ejected,
                                msg="unable to eject the node {0}".format(otpNode.id))
            else:
                self.fail(msg="unable to add node : {0} to the cluster".format(ip))

    def test_add_all_node_no_buckets(self):
        self.common_setUp(False)
        self._add_all_node_body()

    def test_add_all_node_with_bucket(self):
        self.common_setUp(True)
        self._add_all_node_body()

    def test_add_node_itself_no_buckets(self):
        self.common_setUp(False)
        self._add_node_itself_body()

    def test_add_node_itself_with_bucket(self):
        self.common_setUp(True)
        self._add_node_itself_body()

    def test_add_node_already_added_no_buckets(self):
        self.common_setUp(False)
        self._add_node_already_added_body()

    def test_add_node_already_added_with_bucket(self):
        self.common_setUp(True)
        self._add_node_already_added_body()

    def test_add_1_node_no_buckets(self):
        self.common_setUp(False)
        self._add_1_node_body()

    def test_add_1_node_with_bucket(self):
        self.common_setUp(True)
        self._add_1_node_body()
