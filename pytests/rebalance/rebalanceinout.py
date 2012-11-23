import time

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.bucket_helper import BucketOperationHelper

class RebalanceInOutTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceInOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceInOutTests, self).tearDown()

    """Rebalances nodes out and in of the cluster while doing mutations with max
    number of buckets in the cluster.

    This test begins by creating max number of buckets with bucket_size=100( by default):
    one default bucket, all other are sasl and standart buckets. Then we load
    a given number of items into the cluster. It then removes two nodes,
    rebalances that nodes out the cluster, and then rebalances them back
    in. During the rebalancing we update all of the items in the cluster. Once the
    node has been removed and added back we  wait for the disk queues to drain, and
    then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
    We then remove and add back two nodes at a time and so on until we have reached the point
    where we are adding back and removing at least half of the nodes."""
    def incremental_rebalance_in_out_with_max_buckets_number(self):
        self.bucket_size = self.input.param("bucket_size", 100)
        bucket_num = self.quota / self.bucket_size
        self.log.info('total %s buckets will be created with size %s MB' % (bucket_num, self.bucket_size))
        self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, (bucket_num - 1) / 2)

        self._create_standard_buckets(self.master, bucket_num - 1 - (bucket_num - 1) / 2)
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            self._async_load_all_buckets(self.master, gen, "update", 0)

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(5)
            self._async_load_all_buckets(self.master, gen, "update", 0)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            self.verify_cluster_stats(self.servers[:self.num_servers])

    """Rebalances nodes in/out at once while doing mutations with max
    number of buckets in the cluster.

    This test begins by creating max number of buckets with bucket_size=quota/maxBucketCount.
    one default bucket, all other are sasl and standart buckets. Then we load
    a given number of items into the cluster. It then removes servs_in nodes and adds
    servs_out, rebalances cluster. During the rebalancing we update all of the items in the cluster.
    Once the node has been rebalanced we  wait for the disk queues to drain, and
    then verify that there has been no data loss, sum(curr_items) match the curr_items_total."""
    def rebalance_in_out_at_once_with_max_buckets_number(self):
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        rest = RestConnection(self.master)
        self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)

        rest = RestConnection(self.master)
        bucket_num = rest.get_internalSettings("maxBucketCount")
        self.bucket_size = self.quota / bucket_num

        self.log.info('total %s buckets will be created with size %s MB' % (bucket_num, self.bucket_size))
        self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, (bucket_num - 1) / 2)
        self._create_standard_buckets(self.master, bucket_num - 1 - (bucket_num - 1) / 2)

        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self._wait_for_stats_all_buckets(servs_init)

        rebalance = self.cluster.async_rebalance(servs_init, servs_in, servs_out)
        self._async_load_all_buckets(self.master, gen, "update", 0)
        rebalance.result()
        self.verify_cluster_stats(result_nodes)

    """Rebalances nodes out and in of the cluster while doing mutations.

    This test begins by loading a given number of items into the cluster. It then
    removes one node, rebalances that node out the cluster, and then rebalances it back
    in. During the rebalancing we update all of the items in the cluster. Once the
    node has been removed and added back we  wait for the disk queues to drain, and
    then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
    We then remove and add back two nodes at a time and so on until we have reached the point
    where we are adding back and removing at least half of the nodes."""
    def incremental_rebalance_in_out_with_mutation(self):
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            tasks = self._async_load_all_buckets(self.master, gen, "update", 0)

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(5)
            for task in tasks:
                task.result(self.wait_timeout * 20)
            tasks = self._async_load_all_buckets(self.master, gen, "update", 0)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result(self.wait_timeout * 20)
            self.verify_cluster_stats(self.servers[:self.num_servers])

    """Start-stop rebalance in/out with adding/removing aditional after stopping rebalance.

    This test begins by loading a given number of items into the cluster. It then
    add  servs_in nodes and remove  servs_out nodes and start rebalance. Then rebalance
    is stopped when its progress reached 20%. After we add  extra_nodes_in and remove
    extra_nodes_out. Restart rebalance with new cluster configuration. Later rebalance
    will be stop/restart on progress 40/60/80%. After each iteration we wait for
    the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total. Once cluster was rebalanced the test is finished.
    The oder of add/remove nodes looks like:
    self.nodes_init|servs_in|extra_nodes_in|extra_nodes_out|servs_out"""
    def start_stop_rebalance_in_out(self):
        extra_nodes_in = self.input.param("extra_nodes_in", 0)
        extra_nodes_out = self.input.param("extra_nodes_out", 0)
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        extra_servs_in = [self.servers[i + self.nodes_init + self.nodes_in] for i in range(extra_nodes_in)]
        extra_servs_out = [self.servers[self.nodes_init - i - 1 - self.nodes_out] for i in range(extra_nodes_out)]
        rest = RestConnection(self.master)
        self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        add_in_once = extra_servs_in
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        #the latest iteration will be with i=5, for this case rebalance should be completed, that also is verified and tracked
        for i in range(1, 6):
            if i == 1:
                rebalance = self.cluster.async_rebalance(servs_init[:self.nodes_init], servs_in, servs_out)
            else:
                rebalance = self.cluster.async_rebalance(servs_init[:self.nodes_init] + servs_in, add_in_once, servs_out + extra_servs_out)
                add_in_once = []
                result_nodes = set(servs_init + servs_in + extra_servs_in) - set(servs_out + extra_servs_out)
            time.sleep(5)
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
            stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
            self.assertTrue(stopped, msg="unable to stop rebalance")
            rebalance.result()
            if RestHelper(rest).is_cluster_rebalanced():
                self.verify_cluster_stats(result_nodes)
                self.log.info("rebalance was completed when tried to stop rebalance on {0}%".format(str(expected_progress)))
                break
            else:
                self.log.info("rebalance is still required")
                self._wait_for_stats_all_buckets(servs_init)
                self._verify_all_buckets(self.master, max_verify=self.max_verify)

    """Rebalances nodes in and out of the cluster while doing mutations.

    This test begins by loading a initial number of nodes into the cluster.
    It then adds one node, rebalances that node into the cluster,
    and then rebalances it back out. During the rebalancing we update all  of
    the items in the cluster. Once the nodes have been removed and added back we
    wait for the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total.
    We then add and remove back two nodes at a time and so on until we have reached
    the point where we are adding back and removing at least half of the nodes."""
    def incremental_rebalance_out_in_with_mutation(self):
        init_num_nodes = self.input.param("init_num_nodes", 1)

        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:init_num_nodes], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        for i in range(self.num_servers):
            tasks = self._async_load_all_buckets(self.master, gen, "update", 0)

            self.cluster.rebalance(self.servers[:self.num_servers], self.servers[init_num_nodes:init_num_nodes + i + 1], [])
            time.sleep(10)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   [], self.servers[init_num_nodes:init_num_nodes + i + 1])
            for task in tasks:
                task.result()
            self.verify_cluster_stats(self.servers[:init_num_nodes])

    """Rebalances nodes into and out of the cluster while doing mutations and
    deletions.

    This test begins by loading a given number of items into the cluster. It then
    adds one node, rebalances that node into the cluster, and then rebalances it back
    out. During the rebalancing we update half of the items in the cluster and delete
    the other half. Once the node has been removed and added back we recreate the
    deleted items, wait for the disk queues to drain, and then verify that there has
    been no data loss, sum(curr_items) match the curr_items_total. We then remove and
    add back two nodes at a time and so on until we have reached the point
    where we are adding back and removing at least half of the nodes."""
    def incremental_rebalance_in_out_with_mutation_and_deletion(self):
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)

        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            tasks = self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            tasks.extend(self._async_load_all_buckets(self.master, gen_delete, "delete", 0))

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(10)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result()
            self._load_all_buckets(self.master, gen_delete, "create", 0)
            self.verify_cluster_stats(self.servers[:self.num_servers])

    """Rebalances nodes into and out of the cluster while doing mutations and
    expirations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node, rebalances that node into the cluster, and then rebalances it back
    out. During the rebalancing we update half of the items in the cluster and expire
    the other half. Once the node has been removed and added back we recreate the
    expired items, wait for the disk queues to drain, and then verify that there has
    been no data loss, sum(curr_items) match the curr_items_total.We then remove and
    add back two nodes at a time and so on until we have reached the point
    where we are adding back and removing at least half of the nodes."""
    def incremental_rebalance_in_out_with_mutation_and_expiration(self):
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        gen_expire = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)

        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            tasks = self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            tasks.extend(self._async_load_all_buckets(self.master, gen_expire, "update", 5))

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            time.sleep(5)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result()
            self._load_all_buckets(self.master, gen_expire, "create", 0)
            self.verify_cluster_stats(self.servers[:self.num_servers])

    """Rebalance in/out at once.

    This test begins by loading a given number of items into the cluster. It then
    add  servs_in nodes and remove  servs_out nodes and start rebalance.
    Once cluster was rebalanced the test is finished.
    Available parameters by default are:
    nodes_init=1, nodes_in=1, nodes_out=1"""
    def rebalance_in_out_at_once(self):
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        rest = RestConnection(self.master)
        self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.cluster.rebalance(servs_init[:self.nodes_init], servs_in, servs_out)
        self.verify_cluster_stats(result_nodes)
