import time

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase.document import View

class RebalanceInOutTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceInOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceInOutTests, self).tearDown()

    """Rebalances nodes out and in of the cluster while doing mutations with max
    number of buckets in the cluster.

    This test begins by creating max number of buckets with bucket_size=100( by default).
    no we have limitation in 10 buckets:
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
        bucket_num = min(10, self.quota / self.bucket_size)
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
            self.sleep(5)
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
            self.sleep(10)
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
            self.sleep(20)
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
            if not RestHelper(rest).is_cluster_rebalanced():
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
                self._verify_all_buckets(self.master, timeout=None, max_verify=self.max_verify, batch_size=1)

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
            self.sleep(10)
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
            self.sleep(10)
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
            self.log.info("iteration #{0}".format(i))
            tasks = self._async_load_all_buckets(self.master, self.gen_update, "update", 0, batch_size=50)
            tasks.extend(self._async_load_all_buckets(self.master, gen_expire, "update", 10, batch_size=50))

            self.cluster.rebalance(self.servers[:i], [], self.servers[i:self.num_servers])
            self.sleep(20)
            tasks.extend(self.cluster.async_rebalance(self.servers[:self.num_servers],
                                   self.servers[i:self.num_servers], []))
            try:
                for task in tasks:
                    task.result(min(self.wait_timeout * 30, 36000))
            except Exception, ex:
                for task in tasks:
                    task._Thread__stop()
                raise ex

            self._load_all_buckets(self.master, gen_expire, "create", 0)
            self.verify_cluster_stats(self.servers[:self.num_servers])

    """PERFORMANCE:Rebalance in/out at once.

    Then it creates cluster with self.nodes_init nodes. Further
    test loads a given number of items into the cluster. It then
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

    """PERFORMANCE:Rebalance in/out at once with stopped persistence.

    This test begins by loading a given number of items into the cluster with
    self.nodes_init nodes in it. Then we stop persistence on some nodes.
    Test starts  to update some data and load new data in the cluster.
    At that time we add  servs_in nodes and remove  servs_out nodes and start rebalance.
    After rebalance and data ops are completed we start verification phase:
    wait for the disk queues to drain, verify the number of items that were/or not persisted
    with expected values, verify that there has been no data loss,
    sum(curr_items) match the curr_items_total.Once All checks passed, test is finished.
    Available parameters by default are:
    nodes_init=1, nodes_in=1, nodes_out=1,num_nodes_with_stopped_persistence=1
    num_items_without_persistence=100000"""
    def rebalance_in_out_at_once_persistence_stopped(self):
        num_nodes_with_stopped_persistence = self.input.param("num_nodes_with_stopped_persistence", 1)
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        rest = RestConnection(self.master)
        self._wait_for_stats_all_buckets(servs_init)
        for server in servs_init[:min(num_nodes_with_stopped_persistence, self.nodes_init)]:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                shell.execute_cbepctl(bucket, "stop", "", "", "")
        self.sleep(5)
        self.num_items_without_persistence = self.input.param("num_items_without_persistence", 100000)
        gen_extra = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2\
                                      , end=self.num_items / 2 + self.num_items_without_persistence)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        tasks = self._async_load_all_buckets(self.master, gen_extra, "create", 0, batch_size=1000)
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        # wait timeout in 60 min because MB-7386 rebalance stuck
        self.cluster.rebalance(servs_init[:self.nodes_init], servs_in, servs_out, timeout=self.wait_timeout * 60)
        for task in tasks:
            task.result()

        self._wait_for_stats_all_buckets(servs_init[:self.nodes_init - self.nodes_out], \
                                         ep_queue_size=self.num_items_without_persistence * 0.9, ep_queue_size_cond='>')
        self._wait_for_stats_all_buckets(servs_in)
        self._verify_all_buckets(self.master, timeout=None)
        self._verify_stats_all_buckets(result_nodes)
        #verify that curr_items_tot corresponds to sum of curr_items from all nodes
        verified = True
        for bucket in self.buckets:
            verified &= RebalanceHelper.wait_till_total_numbers_match(self.master, bucket)
        self.assertTrue(verified, "Lost items!!! Replication was completed but sum(curr_items) don't match the curr_items_total")


    """PERFORMANCE:Test to measure time to index doc during rebalance

    Then it creates cluster with self.nodes_init nodes. Further
    test loads a given number of items into the cluster. We create num_ddocs ddocs
    and num_views views in each. Perform all queries and wait while view index is
    completed. Then we additionally load data_perc_add items that were loaded before,
    add servs_in nodes and remove  servs_out nodes and start rebalance.
    After we begin to measure time, perform all view query and wait until
    all the data will be obtained for each query.
    Once we got all data,cluster was rebalanced the test is finished.
    Available parameters by default are:
    nodes_init=1, nodes_in=1, nodes_out=1
    num_ddocs=1,num_views=1,data_perc_add=10"""
    def measure_time_index_during_rebalance(self):
        num_ddocs = self.input.param("num_ddocs", 1)
        num_views = self.input.param("num_views", 1)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        ddoc_names = ['ddoc' + str(i) for i in xrange(num_ddocs)]
        map_func = """function (doc, meta) {{\n  emit(meta.id, "emitted_value_{0}");\n}}"""
        views = [View("view" + str(i), map_func.format(i), None, is_dev_ddoc , False) for i in xrange(num_views)]
        #views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)

        prefix = ("", "dev_")[is_dev_ddoc]
        query = {}
        query["connectionTimeout"] = 60000;
        if not is_dev_ddoc:
            query["full_set"] = "true"
        tasks = []

        for bucket in self.buckets:
            for ddoc_name in ddoc_names:
                tasks += self.async_create_views(self.master, ddoc_name, views, bucket)
        for task in tasks:
            task.result(self.wait_timeout * 5)
        for view in views:
            # run queries to create indexes
            self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)
        now = time.time()
        self.sleep(5)
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        for i in xrange(num_ddocs * num_views * len(self.buckets)):
            #wait until all initial_build indexer processes are completed
            active_tasks = self.cluster.async_monitor_active_task(servs_init, "indexer", "True", wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
        self.log.info("PERF: indexing time for {0} ddocs with {1} views:{2}".
                      format(num_ddocs, num_views, time.time() - now))

        rest = RestConnection(self.master)
        #self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)

        data_perc_add = self.input.param("data_perc_add", 10)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * (100 + data_perc_add) / 100)
        load_tasks = self._async_load_all_buckets(self.master, gen_create, "create", 0)
        rebalance = self.cluster.async_rebalance(servs_init, servs_in, servs_out)

        expected_rows = self.num_items * (100 + data_perc_add) / 100 - 1
        start_time = time.time()

        tasks = {}
        for bucket in self.buckets:
            for ddoc_name in ddoc_names:
                for i in xrange(num_views):
                    tasks["{0}/_design/{1}/_view/{2}".format(bucket, ddoc_name, "view" + str(i))] = \
                        self.cluster.async_query_view(self.master, \
                                        prefix + ddoc_name, "view" + str(i), query, expected_rows, bucket)
        while len(tasks) > 0 and time.time() - start_time < self.wait_timeout * 30 :
            completed_tasks = []
            for task in  tasks:
                if tasks[task].done:
                    if tasks[task].result():
                        self.log.info("expected query result with view {0} was obtained in {1} seconds".\
                                 format(task, time.time() - start_time))
                        completed_tasks += [task]
            for completed_task in completed_tasks:
                del tasks[completed_task]

        if len(tasks) > 0:
            for task in  tasks:
                tasks[task].result(self.wait_timeout)

        load_tasks[0].result()
        rebalance.result()

        self.verify_cluster_stats(result_nodes)
