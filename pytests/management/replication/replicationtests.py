import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import crc32
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

log = logger.Logger.get_logger()

class ReplicationTests(unittest.TestCase):

    servers = None
    keys = None
    clients = None
    bucket_name = None
    keys_updated = None
    log = None
    input = None


    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        #make sure the master node does not have any other node
        #loop through all nodes and remove those nodes left over
        #from previous test runs


    def _load_data(self,fill_ram_percentage = 1.0):
        if fill_ram_percentage <= 0.0:
            fill_ram_percentage = 5.0
        master = self.servers[0]
        #populate key
        rest = RestConnection(master)
        testuuid = uuid.uuid4()
        info = rest.get_bucket(self.bucket_name)
        emptySpace = info.stats.ram - info.stats.memUsed
        self.log.info('emptySpace : {0} fill_ram_percentage : {1}'.format(emptySpace, fill_ram_percentage))
        fill_space = (emptySpace * fill_ram_percentage)/100.0
        self.log.info("fill_space {0}".format(fill_space))
        # each packet can be 10 KB
        packetSize = int(10 *1024)
        number_of_buckets = int(fill_space) / packetSize
        self.log.info('packetSize: {0}'.format(packetSize))
        self.log.info('memory usage before key insertion : {0}'.format(info.stats.memUsed))
        self.log.info('inserting {0} new keys to memcached @ {0}'.format(number_of_buckets,master))
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(number_of_buckets)]
        client = MemcachedClient(master.ip, 11220)
        self.keys_not_pushed = []
        for key in self.keys:
            vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
            client.vbucketId = vbucketId
            try:
                payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0',10 * 1024)
                client.set(key, 0, 0, payload)
            except MemcachedError as error:
                self.log.error(error)
                self.log.error("unable to push key : {0} to bucket : {1}".format(key, client.vbucketId))
                self.keys_not_pushed.append(key)
        client.close()

    def test_failover_1_replica_0_1_percent(self):
        self._test_failover_body(fill_ram_percentage=0.1,number_of_replicas=1)

    def test_failover_1_replica_1_percent(self):
        self._test_failover_body(fill_ram_percentage=1,number_of_replicas=1)

    def test_failover_1_replica_10_percent(self):
        self._test_failover_body(fill_ram_percentage=10,number_of_replicas=1)

    def test_failover_1_replica_50_percent(self):
        self._test_failover_body(fill_ram_percentage=50,number_of_replicas=1)

    def test_failover_1_replica_99_percent(self):
        self._test_failover_body(fill_ram_percentage=99,number_of_replicas=1)



    def test_replication_1_replica_0_1_percent(self):
        self._test_body(fill_ram_percentage=0.1,number_of_replicas=1)

    def test_replication_1_replica_1_percent(self):
        self._test_body(fill_ram_percentage=1,number_of_replicas=1)

    def test_replication_1_replica_10_percent(self):
        self._test_body(fill_ram_percentage=10,number_of_replicas=1)

    def test_replication_1_replica_50_percent(self):
        self._test_body(fill_ram_percentage=50,number_of_replicas=1)

    def test_replication_1_replica_99_percent(self):
        self._test_body(fill_ram_percentage=99,number_of_replicas=1)


    def test_replication_2_replica_0_1_percent(self):
        self._test_body(fill_ram_percentage=0.1,number_of_replicas=2)

    def test_replication_2_replica_1_percent(self):
        self._test_body(fill_ram_percentage=1,number_of_replicas=2)

    def test_replication_2_replica_10_percent(self):
        self._test_body(fill_ram_percentage=10,number_of_replicas=2)

    def test_replication_2_replica_50_percent(self):
        self._test_body(fill_ram_percentage=50,number_of_replicas=2)

    def test_replication_2_replica_99_percent(self):
        self._test_body(fill_ram_percentage=99,number_of_replicas=2)


    def test_replication_3_replica_0_1_percent(self):
        self._test_body(fill_ram_percentage=0.1,number_of_replicas=3)

    def test_replication_3_replica_1_percent(self):
        self._test_body(fill_ram_percentage=1,number_of_replicas=3)

    def test_replication_3_replica_10_percent(self):
        self._test_body(fill_ram_percentage=10,number_of_replicas=3)

    def test_replication_3_replica_50_percent(self):
        self._test_body(fill_ram_percentage=50,number_of_replicas=3)

    def test_replication_3_replica_99_percent(self):
        self._test_body(fill_ram_percentage=99,number_of_replicas=3)

    def _check_vbuckets(self,number_of_replicas):
        #this method makes sure for each vbucket there is x number
        #of replicas
        #each vbucket should have x replicas
        rest = RestConnection(self.servers[0])
        buckets = rest.get_buckets()
        failed_verification = []
        for bucket in buckets:
            #get the vbuckets
            vbuckets = bucket.vbuckets
            index = 0
            for vbucket in vbuckets:
                if len(vbucket.replica) != number_of_replicas:
                    self.log.error("vbucket # {0} number of replicas : {1} vs expected : {2}".format(index,
                    len(vbucket.replica),number_of_replicas))
                    failed_verification.append(index)
                index += 1
        if not failed_verification:
            self.fail("unable to verify number of replicas for {0} vbuckets".format(len(failed_verification)))

    #visit each node and get the data to verify the replication

    #update keys
    def _update_keys(self,version):
        client = MemcachedClient(self.servers[0].ip, 11220)
        self.updated_keys = []
        for key in self.keys:
            vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
            client.vbucketId = vbucketId
            value = '{0}{1}'.format(key,version)
            try:
                client.set(key, 0, 0, value)
                self.updated_keys.append(key)
            except MemcachedError as error:
                self.log.error(error)
                self.log.error("unable to update key : {0} to bucket : {1}".format(key, client.vbucketId))
        client.close()


    #verify

    def _verify_minimum_requirement(self,number_of_replicas):
        # we should at least have
        # x = ips.length
        #-
        self.assertTrue(len(self.servers)/(1+number_of_replicas) >= 1,"there are not enough number of nodes available")

    def _create_bucket(self,number_of_replicas=1):

        self.bucket_name = 'ReplicationTest-{0}'.format(uuid.uuid4())
        ip_rest = RestConnection(self.servers[0])
        self.log.info('creating bucket : {0}'.format(self.bucket_name))
        ip_rest.create_bucket(bucket=self.bucket_name,
                           ramQuotaMB=256,
                           replicaNumber=number_of_replicas,
                           proxyPort=11220)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(self.bucket_name)
        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(self.bucket_name,
                                                                       ip_rest), msg=msg)
        remote = RemoteMachineShellConnection(self.servers[0])
        info = remote.extract_remote_info()
        remote.terminate_process(info, 'memcached')
        remote.terminate_process(info, 'moxi')

        BucketOperationHelper.wait_till_memcached_is_ready_or_assert([self.servers[0]],
                                                                     bucket_port=11220,
                                                                     test=self)

    def _cleanup_cluster(self):
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

    def _verify_data(self,version):
        #verify all the keys
        master = self.servers[0]
        client = MemcachedClient(master.ip, 11220)
        index = 0
        all_verified = True
        keys_failed = []
        for key in self.keys:
            if key in self.updated_keys:
                try:
                    index += 1
                    vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                    client.vbucketId = vbucketId
                    flag, keyx, value = client.get(key=key)
                    expected = '{0}{1}'.format(key,version)
                    self.assertEquals(value, expected,
                                      msg='values do not match . expected : {0} actual : {1}'.format(value,expected))
    #                self.log.info("verified key #{0} : {1} value : {2}".format(index,key,value))
                except MemcachedError as error:
                    self.log.error(error)
                    self.log.error("memcachedError : {0} - unable to get a pre-inserted key : {0}".format(error.status,key))
                    keys_failed.append(key)
                    all_verified = False
#            except :
#                self.log.error("unknown errors unable to get a pre-inserted key : {0}".format(key))
#                keys_failed.append(key)
#                all_verified = False

        client.close()
        self.assertTrue(all_verified,
                        'unable to verify #{0} keys'.format(len(keys_failed)))
    

    def add_nodes_and_rebalance(self):
        index = 0
        otpNodes = []
        master = self.servers[0]
        rest = RestConnection(master)
        for serverInfo in self.servers:
            if index > 0:
                self.log.info('adding node : {0} to the cluster'.format(serverInfo.ip))
                otpNode = rest.add_node(user=self.input.membase_settings.rest_username,
                                        password=self.input.membase_settings.rest_password,
                                        remoteIp=serverInfo.ip)
                if otpNode:
                    self.log.info('added node : {0} to the cluster'.format(otpNode.id))
                    otpNodes.append(otpNode)
#            remote = RemoteMachineShellConnection(ip=ip,
#                                                  username='root',
#                                                  pkey_location=os.getenv("KEYFILE"))
#            remote.execute_command('killall -9 memcached')
            index += 1
            #rebalance
        #let's kill all memcached
        otpNodeIds = ['ns_1@' + master.ip]
        for otpNode in otpNodes:
            otpNodeIds.append(otpNode.id)
        rebalanceStarted = rest.rebalance(otpNodeIds,[])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master.ip))
        self.log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        self.assertTrue(rebalanceSucceeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        self.log.info('rebalance operaton succeeded for nodes: {0}'.format(otpNodeIds))
        #now remove the nodes
        #make sure its rebalanced and node statuses are healthy
        helper = RestHelper(rest)
        self.assertTrue(helper.is_cluster_healthy, "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced, "cluster is not balanced")


    #setup part1 : cleanup the clsuter
    #part 2: load data
    #part 3 : add nodes and rebalance
    #part 4 : update keys

    def _test_body(self,fill_ram_percentage = 1,number_of_replicas=1):
        self._verify_minimum_requirement(number_of_replicas)
        self._cleanup_cluster()
        self.log.info('cluster is setup')
        self._create_bucket(number_of_replicas)
        self.log.info('created the bucket')
        self._load_data(fill_ram_percentage=0.1)
        self.add_nodes_and_rebalance()
        self.log.info('loading more data into the bucket')
        self._load_data(fill_ram_percentage=fill_ram_percentage)
        self.log.info('updating all keys by appending _20 to each value')
        self._update_keys('20')
        self.log.info('verifying keys now...._20')
        self._verify_data('20')
        rest = RestConnection(self.servers[0])
        self.assertTrue(RestHelper(rest).wait_for_replication(180),
                        msg="replication did not complete")
        self.log.info('updating all keys by appending _30 to each value')
        self._update_keys('30')
        self.log.info('verifying keys now...._20')
        self._verify_data('30')

    def _test_failover_body(self,fill_ram_percentage = 1,number_of_replicas=1):
        self._verify_minimum_requirement(number_of_replicas)
        self._cleanup_cluster()
        self.log.info('cluster is setup')
        self._create_bucket(number_of_replicas)
        self.log.info('created the bucket')
        # tiny amount of data in the bucket
        self._load_data(fill_ram_percentage=0.1)
        self.add_nodes_and_rebalance()
        self.log.info('loading more data into the bucket')
        self._load_data(fill_ram_percentage=fill_ram_percentage)
        self.log.info('updating all keys by appending _20 to each value')
        self._update_keys('20')
        self.log.info('verifying keys now...._20')
        self._verify_data('20')
        rest = RestConnection(self.servers[0])
        self.assertTrue(RestHelper(rest).wait_for_replication(180),
                        msg="replication did not complete")
        self.log.info('updating all keys by appending _30 to each value')
        self._update_keys('30')
        self.log.info('verifying keys now...._20')
        self._verify_data('30')
        self.assertTrue(RestHelper(rest).wait_for_replication(180),
                        msg="replication did not complete")
        #only remove one of the nodes
        second_node = self.servers[1]
        self.log.info('failing over node : {0} from the cluster'.format(second_node.ip))
        rest.fail_over('ns_1@{0}'.format(second_node.ip))
        nodes = rest.node_statuses()
        allNodes = [node.id for node in nodes]
        RestHelper(rest).remove_nodes(knownNodes=allNodes, ejectedNodes=[])
        self.log.info('verifying keys now...._30')
        self._verify_data('30')

    def tearDown(self):
        self._cleanup_cluster()

    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]