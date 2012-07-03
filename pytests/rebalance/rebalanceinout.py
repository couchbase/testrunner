import time

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper

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

    """Start-stop rebalance in/out with adding/removing aditional after stopping rebalance.

    This test begins by loading a given number of items into the cluster. It then
    add  servs_in nodes and remove  servs_out nodes and start rebalance. Then rebalance
    is stopped when its progress reached 20%. After we add  extra_nodes_in and remove
    extra_nodes_out. Restart rebalance with new cluster configuration. Later rebalance
    will be stop/restart on progress 40/60/80%. After each iteration we wait for
    the disk queues to drain, and then verify that there has been no data loss.
    Once cluster was rebalanced the test is finished.
    The oder of add/remove nodes looks like:
    nodes_init|servs_in|extra_nodes_in|extra_nodes_out|servs_out"""
    def start_stop_rebalance_in_out(self):
        #initial number of items in the cluster
        nodes_init = self.input.param("nodes_init", 1)
        extra_nodes_in = self.input.param("extra_nodes_in", 0)
        extra_nodes_out = self.input.param("extra_nodes_out", 0)
        servs_init = self.servers[:nodes_init]
        servs_in = [self.servers[i + nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[nodes_init - i - 1] for i in range(self.nodes_out)]
        extra_servs_in = [self.servers[i + nodes_init + self.nodes_in] for i in range(extra_nodes_in)]
        extra_servs_out = [self.servers[nodes_init - i - 1 -self.nodes_out] for i in range(extra_nodes_out)]
        rest=RestConnection(self.servers[0])
        self.cluster.rebalance(self.servers[:1], servs_init[1:], [])
        self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        add_in_once = extra_servs_in
        #the latest iteration will be with i=5, for this case rebalance should be completed, that also is verified and tracked
        for i in range(1, 6):
            if i==1:
                rebalance = self.cluster.async_rebalance(servs_init[:nodes_init], servs_in, servs_out)
            else:
                self.cluster.async_rebalance(servs_init[:nodes_init] + servs_in, add_in_once, servs_out + extra_servs_out)
                add_in_once = []
            time.sleep(5)
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")

            if RestHelper(rest).is_cluster_rebalanced():
                self._wait_for_stats_all_buckets(set(servs_init + servs_in + extra_servs_in) - set(servs_out + extra_servs_out))
                self._verify_all_buckets(self.servers[0])
                self._verify_stats_all_buckets(set(servs_init + servs_in + extra_servs_in) - set(servs_out + extra_servs_out))
                self.log.info("rebalance was completed when tried to stop rebalance on {0}%".format(str(expected_progress)))
                break
            else:
                self._wait_for_stats_all_buckets(servs_init)
                self._verify_all_buckets(self.servers[0])

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
