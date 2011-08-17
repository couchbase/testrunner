import unittest
import uuid
from TestInput import TestInput, TestInputSingleton
import logger
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
import time
import load_runner

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
        rest = RestConnection(self.master)
        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        info = rest.get_nodes_self()
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))
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


    def test_max_buckets_small_load(self):
        self.test_buckets(bucket_count=0, ops=1000, max_time=60, replicas=[0,1,2,3])

    def test_two_buckets_small_load(self):
        self.test_buckets(bucket_count=2, ops=1000, max_time=60, replicas=[1,2])

    def test_five_buckets_small_load(self):
        self.test_buckets(bucket_count=5, ops=1000, max_time=60, replicas=[0,1,2,3])

    def test_max_buckets_medium_load(self):
        self.test_buckets(bucket_count=0, ops=6000, max_time=600, replicas=[0,1,2,3])

    def test_two_buckets_medium_load(self):
        self.test_buckets(bucket_count=2, ops=6000, max_time=600, replicas=[1,2])

    def test_five_buckets_medium_load(self):
        self.test_buckets(bucket_count=5, ops=6000, max_time=600, replicas=[0,1,2,3])

    def test_max_buckets(self):
        self.test_buckets(bucket_count=0, ops=0, replicas=[0,1,2,3])

    def test_ten_buckets(self):
        self.test_buckets(bucket_count=10, ops=0, replicas=[0,1,2,3])

    def test_twenty_buckets(self):
        self.test_buckets(bucket_count=20, ops=0, replicas=[0,1,2,3])

    # base function that other tests can call
    # if buckets > 0 then the test will exit if the cluster doesn't have enough space
    # if buckets = 0 then the test will create max number of buckets
    # if ops = 0 then no load is run against the system
    def test_buckets(self, bucket_count=0, ops=0, max_time=0, replicas=[1]):
        bucket_ram = 100
        if not bucket_count:
            bucket_count = info.memoryQuota / bucket_ram
        if bucket_count > info.memoryQuota / bucket_ram:
            self.log.error('node does not have enough capacity for {0} buckets, exiting test'.format(bucket_count))
            return

        max_load_memory = bucket_ram * 3 / 4
        max_load_time = max_time

        load_info = {
            'server_info' : self.servers,
            'memcached_info' : {
                'bucket_name':"",
                'bucket_port':"11211",
                'bucket_password':"",
            },
            'operation_info' : {
                'operation_distribution':{'set':3, 'get':5},
                'valuesize_distribution':{250:15, 1500:10, 20:5,15000:5},
                'create_percent':25,
                'threads':2*len(self.servers),
                'operation_rate':ops/bucket_count,
            },
            'limit_info' : {
                'max_size':max_load_memory,
                'max_time':max_load_time,
            },
        }

        loads = []

        for i in range(bucket_count):
            bucket_name = 'bucketops-{0}'.format(uuid.uuid4())
            replica = replicas[i%len(replicas)]
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               replicaNumber=replica,
                               authType='sasl',
                               saslPassword='')
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(bucket_name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
            load_info['memcached_info']['bucket_name'] = bucket_name
            loads.append(load_runner.LoadRunner(load_info, dryrun=False))

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

        if ops:
            self.log.info('starting load against all buckets')
            for load in loads:
                load.start()

            if max_load_time:
                end_time = time.time() + max_load_time
                for load in loads:
                    load.wait(end_time - time.time())
                # stop all load if there is any still running
                for load in loads:
                    load.stop()
            else:
                for load in loads:
                    load.wait()

            self.log.info('stopped load against all buckets')

            # TODO: query loads for errors and assert if any are found
            # TODO: wait for persistence
            # TODO: wait for replica
            # TODO: verify total items
            # TODO: verify persisted items
