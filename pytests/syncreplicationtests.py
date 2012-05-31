import json
import random
from threading import Thread
import unittest
import uuid
from TestInput import TestInputSingleton
import logger
from mc_bin_client import MemcachedError
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper

import crc32
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

class SyncPersistenceOneNodeTest(unittest.TestCase):

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.servers = TestInputSingleton.input.servers
        self.params = TestInputSingleton.input.test_params
        master = self.servers[0]
        rest = RestConnection(self.servers[0])
        rest.init_cluster(master.rest_username,master.rest_password)
        rest.init_cluster_memoryQuota(master.rest_username, master.rest_password,memoryQuota=1000)
        ClusterOperationHelper.cleanup_cluster(self.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self._create_default_bucket()
        self.smartclient = MemcachedClientHelper.direct_client(master, "default")


    def _create_default_bucket(self):
        name = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
            info = rest.get_nodes_self()
            available_ram = info.mcdMemoryReserved * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram))
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))
        self.load_thread = None
        self.shutdown_load_data = False

    def test_10k_items(self):
        keys = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, 100)]
        value = MemcachedClientHelper.create_value("*", 1024)
        for k in keys:
            mc = self.smartclient
            vBucket = crc32.crc32_hash(k) & (mc.vbucket_count - 1)
            mc.set(k, 0, 0, value)
            mc.sync_persistence([{"key": k, "vbucket": vBucket}])

    def test_10k_items_during_load(self):
        keys = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, 100)]
        value = MemcachedClientHelper.create_value("*", 1024)
        prefix = str(uuid.uuid4())
        working_set_size = 10 * 1000
        self.load_thread = Thread(target=self._insert_data_till_stopped, args=("default", prefix, working_set_size))
        self.load_thread.start()
        for k in keys:
            mc = self.smartclient
            vBucket = crc32.crc32_hash(k) & (mc.vbucket_count - 1)
            mc.set(k, 0, 0, value)
            mc.sync_persistence([{"key": k, "vbucket": vBucket}])
        self.shutdown_load_data = True
        self.load_thread.join()

    def tearDown(self):
        self.shutdown_load_data = True
        if self.load_thread:
            self.load_thread.join()
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)


    def _insert_data_till_stopped(self, bucket, prefix, number_of_items):
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        inserted_index = []
        while not self.shutdown_load_data:
            for i in range(0, number_of_items):
                key = doc_name = "{0}-{1}".format(prefix, i)
                value = {"name": doc_name, "age": random.randint(0, 5000)}
                try:
                    moxi.set(key, 0, 0, json.dumps(value))
                    inserted_index.append(i)
                except Exception as ex:
                    self.log.error("unable to set item , error {0}".format(ex))
        self.log.info("inserted {0} json documents".format(inserted_index))





class SyncReplicationTest(unittest.TestCase):
    awareness = None

    def common_setup(self, replica):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        first = self._servers[0]
        self.log = logger.Logger().get_logger()
        self.log.info(self._input)
        rest = RestConnection(first)
        for server in self._servers:
            RestHelper(RestConnection(server)).is_ns_server_running()

        ClusterOperationHelper.cleanup_cluster(self._servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)
        ClusterOperationHelper.add_all_nodes_or_assert(self._servers[0], self._servers, self._input.membase_settings, self)
        nodes = rest.node_statuses()
        otpNodeIds = []
        for node in nodes:
            otpNodeIds.append(node.id)
        info = rest.get_nodes_self()
        bucket_ram = info.mcdMemoryReserved * 3 / 4
        rest.create_bucket(bucket="default",
                           ramQuotaMB=int(bucket_ram),
                           replicaNumber=replica,
                           proxyPort=rest.get_nodes_self().moxi)
        msg = "wait_for_memcached fails"
        ready = BucketOperationHelper.wait_for_memcached(first, "default"),
        self.assertTrue(ready, msg)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(first.ip))
        self.log.info('started rebalance operation on master node {0}'.format(first.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        # without a bucket this seems to fail
        self.assertTrue(rebalanceSucceeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        self.awareness = VBucketAwareMemcached(rest, "default")

    def tearDown(self):
        if self.awareness:
            self.awareness.done()
            ClusterOperationHelper.cleanup_cluster(self._servers)
            BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_one_replica(self):
        self.common_setup(1)
        keys = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, 100)]
        value = MemcachedClientHelper.create_value("*", 1024)
        for k in keys:
            vBucket = crc32.crc32_hash(k)
            mc = self.awareness.memcached(k)
            mc.set(k, 0, 0, value)
            mc.sync_replication([{"key": k, "vbucket": vBucket}], 1)
        for k in keys:
            mc = self.awareness.memcached(k)
            mc.get(k)


    def test_one_replica_one_node(self):
        pass

    def test_one_replica_multiple_nodes(self):
        pass

    def test_one_replica_bucket_replica_one(self):
        pass

    def test_two_replica(self):
        self._unsupported_replicas(2)

    def test_three_replica(self):
        self._unsupported_replicas(1)

    def _unsupported_replicas(self, replica):
        self.common_setup(1)
        keys = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, 100)]
        value = MemcachedClientHelper.create_value("*", 102400)
        for k in keys:
            vBucket = crc32.crc32_hash(k)
            mc = self.awareness.memcached(k)
            mc.set(k, 0, 0, value)
            mc.get(k)
            try:
                mc.sync_replication([{"key": k, "vbucket": vBucket}], replica)
                msg = "server did not raise an error when running sync_replication with {0} replicas"
                self.fail(msg.format(replica))
            except MemcachedError as error:
                self.log.info("error {0} {1} as expected".format(error.status, error.msg))

        for k in keys:
            mc = self.awareness.memcached(k)
            mc.get(k)

    def test_invalid_key(self):
        pass

    def test_not_your_vbucket(self):
        self.common_setup(1)
        keys = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, 100)]
        value = MemcachedClientHelper.create_value("*", 1024)
        for k in keys:
            vBucket = crc32.crc32_hash(k)
            mc = self.awareness.memcached(k)
            mc.set(k, 0, 0, value)
            not_your_vbucket_mc = self.awareness.not_my_vbucket_memcached(k)
            try:
                count = 0
                expected_error = 0
                while count < 100:
                    a, b, response = not_your_vbucket_mc.sync_replication([{"key": k, "vbucket": vBucket}], 1)
                    count += 1
                    self.log.info("response : {0}".format(response))
                    if response and response[0]["event"] != "invalid key":
                        expected_error += 1
                if expected_error is not 100:
                    self.fail(msg="server did not raise an error when running sync_replication with invalid vbucket")
            except MemcachedError as error:
                self.log.error(error)

    def test_some_invalid_keys(self):
        pass

    def stest_ome_not_your_vbucket(self):
        pass

    def test_some_large_values(self):
        pass

    def test_too_many_keys(self):
        pass

    def test_singlenode(self):
        pass

