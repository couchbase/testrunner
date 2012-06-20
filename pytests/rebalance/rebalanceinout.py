import time

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase.documentgenerator import BlobGenerator

class RebalanceInOutTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceInOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceInOutTests, self).tearDown()

    """Rebalances nodes out and in of the cluster while doing mutations.

    This test begins by loading a given number of items into the cluster. It then
    removes one node, rebalances that node out the cluster, and then rebalances it back
    in. During the rebalancing we update all of the items in the cluster. Once the
    node has been removed and added back we  wait for the disk queues to drain, and
    then verify that there has been no data loss. We then remove and add back two
    nodes at a time and so on until we have reached the point where we are adding
    back and removing at least half of the nodes."""
    def incremental_rebalance_in_out_with_mutation(self):
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in reversed(range(self.num_servers)[self.num_servers/2:]):
            tasks = self._async_load_all_buckets(self.servers[0], gen, "update", 0)

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(5)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result()
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:self.num_servers])


    """Rebalances nodes in and out of the cluster while doing mutations.

    This test begins by loading a initial number of nodes into the cluster.
    It then adds one node, rebalances that node into the cluster,
    and then rebalances it back out. During the rebalancing we update all  of
    the items in the cluster. Once the nodes have been removed and added back we
    wait for the disk queues to drain, and then verify that there has been no data loss.
    We then add and remove back two nodes at a time and so on until we have reached
    the point where we are adding back and removing at least half of the nodes."""
    def incremental_rebalance_out_in_with_mutation(self):
        init_num_nodes = self.input.param("init_num_nodes", 1)

        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:init_num_nodes], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in range(self.num_servers):
            tasks = self._async_load_all_buckets(self.servers[0], gen, "update", 0)

            self.cluster.rebalance(self.servers[:self.num_servers], self.servers[init_num_nodes:init_num_nodes + i + 1], [])
            time.sleep(5)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   [], self.servers[init_num_nodes:init_num_nodes + i + 1])
            for task in tasks:
                task.result()
            self._wait_for_stats_all_buckets(self.servers[:init_num_nodes])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:init_num_nodes])


    """Rebalances nodes into and out of the cluster while doing mutations and
    deletions.

    This test begins by loading a given number of items into the cluster. It then
    adds one node, rebalances that node into the cluster, and then rebalances it back
    out. During the rebalancing we update half of the items in the cluster and delete
    the other half. Once the node has been removed and added back we recreate the
    deleted items, wait for the disk queues to drain, and then verify that there has
    been no data loss. We then remove and add back two nodes at a time and so on
    until we have reached the point where we are adding back and removing at least
    half of the nodes."""
    def incremental_rebalance_in_out_with_mutation_and_deletion(self):
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        gen_1 = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in reversed(range(self.num_servers)[self.num_servers/2:]):
            tasks = self._async_load_all_buckets(self.servers[0], gen_1, "update", 0)
            tasks.extend(self._async_load_all_buckets(self.servers[0], gen_2, "delete", 0))

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(5)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:self.num_servers])

    """Rebalances nodes into and out of the cluster while doing mutations and
    expirations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node, rebalances that node into the cluster, and then rebalances it back
    out. During the rebalancing we update half of the items in the cluster and expire
    the other half. Once the node has been removed and added back we recreate the
    expired items, wait for the disk queues to drain, and then verify that there has
    been no data loss. We then remove and add back two nodes at a time and so on
    until we have reached the point where we are adding back and removing at least
    half of the nodes."""
    def incremental_rebalance_in_out_with_mutation_and_expiration(self):
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        gen_1 = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        self._load_all_buckets(self.servers[0], gen, "create", 0)

        for i in reversed(range(self.num_servers)[self.num_servers/2:]):
            tasks = self._async_load_all_buckets(self.servers[0], gen_1, "update", 0)
            tasks.extend(self._async_load_all_buckets(self.servers[0], gen_2, "update", 5))

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(5)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:self.num_servers])
