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
from remote.remote_util import RemoteMachineShellConnection

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


    def one_replica(self):
        self.common_setup(1)
        keys = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, 100)]
        value = MemcachedClientHelper.create_value("*", 1024)
        for k in keys:
            vBucket = crc32.crc32_hash(k)
            mc = self.awareness.memcached(k)
            mc.set(k, 0, 0, value)
            mc.sync_replication(1, [{"key": k, "vbucket": vBucket}])
        for k in keys:
            mc = self.awareness.memcached(k)
            mc.get(k)


    def one_replica_one_node(self):
        pass

    def one_replica_multiple_nodes(self):
        pass

    def one_replica_bucket_replica_one(self):
        pass

    def two_replica(self):
        self._unsupported_replicas(2)

    def three_replica(self):
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
                mc.sync_replication(replica, [{"key": k, "vbucket": vBucket}])
                msg = "server did not raise an error when running sync_replication with {0} replicas"
                self.fail(msg.format(replica))
            except MemcachedError as error:
                self.log.info("error {0} {1} as expected".format(error.status, error.msg))

        for k in keys:
            mc = self.awareness.memcached(k)
            mc.get(k)

    def invalid_key(self):
        pass

    def not_your_vbucket(self):
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
                    a, b, response = not_your_vbucket_mc.sync_replication(1,
                        [{"key": k, "vbucket": vBucket}])
                    count += 1
                    self.log.info("response : {0}".format(response))
                    if response and response[0]["event"] != "invalid key":
                        expected_error += 1
                if expected_error is not 100:
                    self.fail(msg="server did not raise an error when running sync_replication with invalid vbucket")
            except MemcachedError as error:
                self.log.error(error)

    def some_invalid_keys(self):
        pass

    def some_not_your_vbucket(self):
        pass

    def some_large_values(self):
        pass

    def too_many_keys(self):
        pass

    def singlenode(self):
        pass

