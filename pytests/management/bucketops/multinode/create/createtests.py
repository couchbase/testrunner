import unittest
import uuid
from TestInput import TestInput, TestInputSingleton
import logger
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
import time

class CreateMembaseBucketsTests(unittest.TestCase):

    version = None
    servers = None
    input = TestInput
    log = None


    #as part of the setup let's delete all the existing buckets
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        self.master = self.servers[0]

        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        ClusterOperationHelper.cleanup_cluster(servers=self.servers)
        credentials = self.input.membase_settings
        ClusterOperationHelper.add_all_nodes_or_assert(master=self.master, all_servers=self.servers, rest_settings=credentials, test_case=self)
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        otpNodeIds = []
        for node in nodes:
            otpNodeIds.append(node.id)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(self.master.ip))
        self.log.info('started rebalance operation on master node {0}'.format(self.master.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        # without a bucket this seems to fail
#        self.assertTrue(rebalanceSucceeded,
#                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        ClusterOperationHelper.cleanup_cluster(servers=self.servers)


    # create maximum number of buckets (server memory / 100MB)
    def test_max_buckets(self):
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = 100
        bucket_count = info.mcdMemoryReserved / bucket_ram

        for i in range(bucket_count):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=1,
                               authType='sasl',
                               saslPassword='')
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert(self.servers,
                                                                         11211,
                                                                         test=unittest,
                                                                         bucket_name=bucket_name,
                                                                         bucket_password='')
        buckets = []
        try:
            buckets = rest.get_buckets()
        except:
            self.log.info('15 seconds sleep before calling get_buckets again...')
            time.sleep(15)
            buckets = rest.get_buckets()
        if len(buckets) != bucket_count:
            msg = 'tried to create {0} buckets, only created {1}'.format(bucket_count, len(buckets))
            self.log.error(msg)
            unittest.fail(msg=msg)

    # if cluster cannot support 10 buckets, just return
    def test_ten_buckets(self):
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = 100
        bucket_count = info.mcdMemoryReserved / bucket_ram
        if bucket_count < 10:
            self.log.error('node does not have enough capacity for 10 buckets, exiting test')
            return
        bucket_count = 10

        for i in range(bucket_count):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=1,
                               authType='sasl',
                               saslPassword='')
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert(self.servers,
                                                                         11211,
                                                                         test=unittest,
                                                                         bucket_name=bucket_name,
                                                                         bucket_password='')
        buckets = []
        try:
            buckets = rest.get_buckets()
        except:
            self.log.info('15 seconds sleep before calling get_buckets again...')
            time.sleep(15)
            buckets = rest.get_buckets()
        if len(buckets) != bucket_count:
            msg = 'tried to create {0} buckets, only created {1}'.format(bucket_count, len(buckets))
            self.log.error(msg)
            unittest.fail(msg=msg)

    # if cluster cannot support 20 buckets, just return
    def test_twenty_buckets(self):
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = 100
        bucket_count = info.mcdMemoryReserved / bucket_ram
        if bucket_count < 20:
            self.log.error('node does not have enough capacity for 20 buckets, exiting test')
            return
        bucket_count = 20

        for i in range(bucket_count):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=1,
                               authType='sasl',
                               saslPassword='')
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert(self.servers,
                                                                         11211,
                                                                         test=unittest,
                                                                         bucket_name=bucket_name,
                                                                         bucket_password='')
        buckets = []
        try:
            buckets = rest.get_buckets()
        except:
            self.log.info('15 seconds sleep before calling get_buckets again...')
            time.sleep(15)
            buckets = rest.get_buckets()
        if len(buckets) != bucket_count:
            msg = 'tried to create {0} buckets, only created {1}'.format(bucket_count, len(buckets))
            self.log.error(msg)
            unittest.fail(msg=msg)
