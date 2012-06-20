import time

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase.documentgenerator import BlobGenerator

class RebalanceInTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceInTests, self).setUp()
        #gen_load data is used for upload before each test(1000 items by default)
        self.gen_load = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        #gen_update is used for doing mutation for 1/2th of uploaded data
        self.gen_update = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
        #upload data before each test
        self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)

    def tearDown(self):
        super(RebalanceInTests, self).tearDown()

    """Rebalances nodes into a cluster while doing docs ops:create, delete, update.

    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform docs ops(add/remove/update/readd)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss.
    Once all nodes have been rebalanced in the test is finished."""
    def rebalance_in_with_ops(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items *3 / 2)
        servs_in=[self.servers[i+1] for i in range(self.nodes_in)]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])
        if(self.doc_ops is not None):
            # define which doc's ops will be performed during rebalancing
            # allows multiple of them but one by one
            if("update" in self.doc_ops):
                self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                self._load_all_buckets(self.servers[0], gen_create, "create", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.servers[0], gen_delete, "delete", 0)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in+1])
        self._verify_all_buckets(self.servers[0])
        self._verify_stats_all_buckets(self.servers[:self.nodes_in+1])



    """Rebalances nodes into a cluster while doing mutations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the rebalance we
    update(all of the items in the cluster)/delete( num_items/(num_servers -1) in each iteration)/
    create(a half of initial items in each iteration). Once the cluster has been
    rebalanced we wait for the disk queues to drain, and then verify that
     there has been no data loss. Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_ops(self):
        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            if self.doc_ops is not None:
            # define which doc's operation will be performed during rebalancing
            #only one type of ops can be passed
                if("update" in self.doc_ops):
                    # 1/2th of data will be updated in each iteration
                    self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
                elif("create" in self.doc_ops):
                    # 1/2th of initial data will be added in each iteration
                    gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items*(1 +i ) / 2.0 , end=self.num_items * (1 + i / 2.0))
                    self._load_all_buckets(self.servers[0], gen_create, "create", 0)
                elif("delete" in self.doc_ops):
                    # 1/(num_servers) of initial data will be removed after each iteration
                    # at the end we should get empty base( or couple items)
                    gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=int(self.num_items * (1 - i / (self.num_servers -1.0))) + 1, end=int(self.num_items *( 1- (i - 1) / (self.num_servers -1.0))))
                    self._load_all_buckets(self.servers[0], gen_delete, "delete", 0)
            rebalance.result()
            self._wait_for_stats_all_buckets(self.servers[:i+1])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i+1])

    """Rebalances nodes into a cluster while doing mutations and deletions.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update half of the items in the cluster and delete the other half.
    Once the cluster has been rebalanced we recreate the deleted items, wait for the
    disk queues to drain, and then verify that there has been no data loss. Once all
    nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_mutation_and_deletion(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)

        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
            self._load_all_buckets(self.servers[0], gen_delete, "delete", 0)
            rebalance.result()
            self._load_all_buckets(self.servers[0], gen_delete, "create", 0)
            self._wait_for_stats_all_buckets(self.servers[:i+1])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i+1])

    """Rebalances nodes into a cluster while doing mutations and expirations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update all items in the cluster. Half of the items updated are also
    given an expiration time of 5 seconds. Once the cluster has been rebalanced we
    recreate the expired items, wait for the disk queues to drain, and then verify
    that there has been no data loss. Once all nodes have been rebalanced in the test
    is finished."""
    def incremental_rebalance_in_with_mutation_and_expiration(self):
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
            self._load_all_buckets(self.servers[0], gen_2, "update", 5)
            time.sleep(5)
            rebalance.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            self._wait_for_stats_all_buckets(self.servers[:i+1])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i+1])
