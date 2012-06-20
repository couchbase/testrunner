import logger
import unittest
import copy

from couchbase.cluster import Cluster
from TestInput import TestInputSingleton
from memcached.helper.kvstore import KVStore
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper

class BaseTestCase(unittest.TestCase):

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.cluster = Cluster()
        self.servers = self.input.servers
        self.buckets = {}

        self.default_bucket = self.input.param("default_bucket", True)
        self.standard_buckets = self.input.param("standard_buckets", 0)
        self.sasl_buckets = self.input.param("sasl_buckets", 0)
        self.total_buckets = self.sasl_buckets + self.default_bucket + self.standard_buckets
        self.num_servers = self.input.param("servers", len(self.servers))
        self.num_replicas = self.input.param("replicas", 1)
        self.num_items = self.input.param("items", 1000)
        self.dgm_run = self.input.param("dgm_run", False)

        if not self.input.param("skip_cleanup", False):
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            for server in self.servers:
                ClusterOperationHelper.cleanup_cluster([server])
            ClusterOperationHelper.wait_for_ns_servers_or_assert([self.servers[0]], self)

        self.quota = self._initialize_nodes(self.cluster, self.servers)
        if self.dgm_run:
            self.quota = 256
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        if self.default_bucket:
            self.cluster.create_default_bucket(self.servers[0], self.bucket_size, self.num_replicas)
            self.buckets['default'] = {1 : KVStore()}
        self._create_sasl_buckets(self.servers[0], self.sasl_buckets)
        # TODO (Mike): Create Standard buckets

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        ClusterOperationHelper.cleanup_cluster(self.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self.buckets = {}
        self.cluster.shutdown()

    def _initialize_nodes(self, cluster, servers):
        quota = 0
        init_tasks = []
        for server in servers:
            init_tasks.append(cluster.async_init_node(server))
        for task in init_tasks:
            node_quota = task.result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        return quota

    def _get_bucket_size(self, quota, num_buckets, ratio=2.0/3.0):
        ip = self.servers[0]
        for server in self.servers:
            if server.ip == ip:
                return int(ratio / float(self.num_servers) / float(num_buckets) * float(quota))
        return int(ratio / float(num_buckets) * float(quota))

    def _create_sasl_buckets(self, server, num_buckets):
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'bucket' + str(i)
            bucket_tasks.append(self.cluster.async_create_sasl_bucket(server, name,
                                                                      'password',
                                                                      self.bucket_size,
                                                                      self.num_replicas))
            self.buckets[name] = {1 : KVStore()}
        for task in bucket_tasks:
            task.result()

    def _verify_stats_all_buckets(self, servers):
        stats_tasks = []
        for bucket, kv_stores in self.buckets.items():
            items = sum([len(kv_store) for kv_store in kv_stores.values()])
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                               'curr_items', '==', items))
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                               'vb_active_curr_items', '==', items))

            available_replicas = self.num_replicas
            if len(servers) == self.num_replicas:
                available_replicas = len(servers) - 1
            elif len(servers) <= self.num_replicas:
                available_replicas = len(servers) - 1

            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                   'vb_replica_curr_items', '==', items * available_replicas))
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                   'curr_items_tot', '==', items * (available_replicas + 1)))

        for task in stats_tasks:
            task.result(60)


    """Asynchronously applys load generation to all bucekts in the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_gen - The generator to use to generate load. (DocumentGenerator)
        op_type - "create", "read", "update", or "delete" (String)
        exp - The expiration for the items if updated or created (int)
        kv_store - The index of the bucket's kv_store to use. (int)

    Returns:
        A list of all of the tasks created.
    """
    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        tasks = []
        for bucket, kv_stores in self.buckets.items():
            gen = copy.deepcopy(kv_gen)
            tasks.append(self.cluster.async_load_gen_docs(server, bucket, gen,
                                                          kv_stores[kv_store],
                                                          op_type, exp))
        return tasks

    """Synchronously applys load generation to all bucekts in the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_gen - The generator to use to generate load. (DocumentGenerator)
        op_type - "create", "read", "update", or "delete" (String)
        exp - The expiration for the items if updated or created (int)
        kv_store - The index of the bucket's kv_store to use. (int)
    """
    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store)
        for task in tasks:
            task.result()

    """Waits for queues to drain on all servers and buckets in a cluster.

    A utility function that waits for all of the items loaded to be persisted
    and replicated.

    Args:
        servers - A list of all of the servers in the cluster. ([TestInputServer])
    """
    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', '==', 0))
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_flusher_todo', '==', 0))
        for task in tasks:
            task.result()

    """Verifies data on all of the nodes in a cluster.

    Verifies all of the data in a specific kv_store index for all buckets in
    the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_store - The kv store index to check. (int)
    """
    def _verify_all_buckets(self, server, kv_store=1):
        tasks = []
        for bucket, kv_stores in self.buckets.items():
            tasks.append(self.cluster.async_verify_data(server, bucket, kv_stores[kv_store]))
        for task in tasks:
            task.result()

