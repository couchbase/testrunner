import time
import copy

from basetestcase import BaseTestCase
from couchbase.stats_tools import StatsCommon
from couchbase.documentgenerator import BlobGenerator

class NewRebalanceTests(BaseTestCase):

    def setUp(self):
        super(NewRebalanceTests, self).setUp()
        self.value_size = self.input.param("value_size", 256)

    def tearDown(self):
        super(NewRebalanceTests, self).tearDown()

    """Rebalances nodes into a cluster while doing mutations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update all of the items in the cluster. Once the cluster has been
    rebalanced we recreate the deleted items, wait for the disk queues to drain, and
    then verify that there has been no data loss. Once all nodes have been rebalanced
    in the test is finished."""
    def incremental_rebalance_in_with_mutation(self):
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.servers[0], gen, "update", 0)
            rebalance.result()
            stats = []
            for j in range(self.num_servers)[:i]:
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_queue_size', '==', 0, stats)
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_flusher_todo', '==', 0, stats)
            self._wait_for_stats_all_buckets(stats)
            self._verify_all_buckets(self.servers[0])

    """Rebalances nodes into a cluster while doing mutations and deletions.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update half of the items in the cluster and delete the other half.
    Once the cluster has been rebalanced we recreate the deleted items, wait for the
    disk queues to drain, and then verify that there has been no data loss. Once all
    nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_mutation_and_deletion(self):
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        gen_1 = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.servers[0], gen_1, "update", 1)
            self._load_all_buckets(self.servers[0], gen_2, "delete", 0)
            rebalance.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            stats = []
            for j in range(self.num_servers)[:i]:
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_queue_size', '==', 0, stats)
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_flusher_todo', '==', 0, stats)
            self._wait_for_stats_all_buckets(stats)
            self._verify_all_buckets(self.servers[0])

    """Rebalances nodes into a cluster while doing mutations and expirations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update all items in the cluster. Half of the items updated are also
    given an expiration time of 5 seconds. Once the cluster has been rebalanced we
    recreate the expired items, wait for the disk queues to drain, and then verify
    that there has been no data loss. Once all nodes have been rebalanced in the test
    is finished."""
    def incremental_rebalance_in_with_mutation_and_expiration(self):
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        gen_1 = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.servers[0], gen_1, "update", 0)
            self._load_all_buckets(self.servers[0], gen_2, "update", 5)
            time.sleep(5)
            rebalance.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            stats = []
            for j in range(self.num_servers)[:i]:
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_queue_size', '==', 0, stats)
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_flusher_todo', '==', 0, stats)
            self._wait_for_stats_all_buckets(stats)
            self._verify_all_buckets(self.servers[0])

    """Rebalances nodes out of a cluster while doing mutations.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update all of the items in the
    cluster. Once the cluster has been rebalanced the test waits for the disk queues
    to drain and then verifies that there has been no data loss. Once all nodes have
    been rebalanced out of the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation(self):
        self.cluster.rebalance(self.servers, self.servers[1:], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            self._load_all_buckets(self.servers[0], gen, "update", 0)
            rebalance.result()
            stats = []
            for j in range(self.num_servers)[:i]:
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_queue_size', '==', 0, stats)
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_flusher_todo', '==', 0, stats)
            self._wait_for_stats_all_buckets(stats)
            self._verify_all_buckets(self.servers[0])

    """Rebalances nodes out of a cluster while doing mutations and deletions.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update half of the items in the
    cluster and delete the other half. Once the cluster has been rebalanced the test
    recreates all of the deleted items, waits for the disk queues to drain, and then
    verifies that there has been no data loss. Once all nodes have been rebalanced out
    of the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation_and_deletion(self):
        self.cluster.rebalance(self.servers, self.servers[1:], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        gen_1 = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            self._load_all_buckets(self.servers[0], gen_1, "update", 0)
            self._load_all_buckets(self.servers[0], gen_2, "delete", 0)
            rebalance.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            stats = []
            for j in range(self.num_servers)[:i]:
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_queue_size', '==', 0, stats)
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_flusher_todo', '==', 0, stats)
            self._wait_for_stats_all_buckets(stats)
            self._verify_all_buckets(self.servers[0])

    """Rebalances nodes out of a cluster while doing mutations and expirations.

    This test begins with all servers clustered together and loads a user defined number
    of items into the cluster. It then removes one node at a time from the cluster and
    rebalances. During the rebalance we update all of the items in the cluster and set
    half of the items to expire in 5 seconds. Once the cluster has been rebalanced the
    test recreates all of the expired items, waits for the disk queues to drain, and then
    verifies that there has been no data loss. Once all nodes have been rebalanced out of
    the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation_and_expiration(self):
        self.cluster.rebalance(self.servers, self.servers[1:], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        gen_1 = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            self._load_all_buckets(self.servers[0], gen_1, "update", 0)
            self._load_all_buckets(self.servers[0], gen_2, "update", 5)
            rebalance.result()
            time.sleep(5)
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            stats = []
            for j in range(self.num_servers)[:i]:
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_queue_size', '==', 0, stats)
                StatsCommon.build_stat_check(self.servers[j], '', 'ep_flusher_todo', '==', 0, stats)
            self._wait_for_stats_all_buckets(stats)
            self._verify_all_buckets(self.servers[0])

    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        load_tasks = []
        for name, kv_stores in self.buckets.items():
            gen = copy.deepcopy(kv_gen)
            load_tasks.append(self.cluster.async_load_gen_docs(server, name, gen,
                                                               kv_stores[kv_store],
                                                               op_type, exp))
        for task in load_tasks:
            task.result()

    def _wait_for_stats_all_buckets(self, stats):
        stats_tasks = []
        for bucket in self.buckets:
            stats_tasks.append(self.cluster.async_wait_for_stats(stats, bucket))
        for task in stats_tasks:
            task.result()

    def _verify_all_buckets(self, server, kv_store=1):
        verify_tasks = []
        for bucket, kv_stores in self.buckets.items():
            verify_tasks.append(self.cluster.async_verify_data(server, bucket,
                                                               kv_stores[kv_store]))
        for task in verify_tasks:
            task.result()
