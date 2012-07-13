import time

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection

class RebalanceOutTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceOutTests, self).setUp()
        #rebalance all nodes into the cluster before each test
        self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [])

    def tearDown(self):
        super(RebalanceOutTests, self).tearDown()

    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update.

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. It then remove nodes_out from the cluster at a time
    and rebalances. During the rebalance we perform docs ops(add/remove/update/readd)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss.
    Once all nodes have been rebalanced the test is finished."""
    def rebalance_out_with_ops(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items *3 / 2)
        servs_out = [self.servers[self.num_servers -i -1] for i in range(self.nodes_out)]
        rebalance = self.cluster.async_rebalance(self.servers[:1], [], servs_out)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                self._load_all_buckets(self.servers[0], gen_create, "create", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.servers[0], gen_delete, "delete", 0)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])
        self._verify_all_buckets(self.servers[0])
        self._verify_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])


    """Rebalances nodes out of a cluster while doing docs' ops.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update(all of the items in the cluster)/
    delete( num_items/(num_servers -1) in each iteration)/
    create(a half of initial items in each iteration). Once the cluster has been rebalanced
    the test waits for the disk queues to drain and then verifies that there has been no data loss.
    Once all nodes have been rebalanced out of the cluster the test finishes."""
    def incremental_rebalance_out_with_ops(self):
        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
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
            self._wait_for_stats_all_buckets(self.servers[:i])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i])



    """Rebalances nodes out of a cluster during view queries.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It creates num_views as
    development/production view with default map view funcs(is_dev_ddoc = True by default).
    It then removes nodes_out nodes at a time and rebalances that node from the cluster.
    During the rebalancing we perform view queries for all views and verify the expected number
    of docs for them. Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss.
    Once successful view queries the test is finished."""
    def rebalance_out_with_queries(self):
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"
        tasks = []
        tasks = self.async_create_views(self.master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self.wait_timeout * 2)
        self.perform_verify_queries(num_views, prefix, ddoc_name, query)
        servs_out=self.servers[-self.nodes_out:]
        rebalance = self.cluster.async_rebalance([self.master], [], servs_out)
        time.sleep(self.wait_timeout / 5)
        #see that the result of view queries are the same as expected during the test
        self.perform_verify_queries(num_views, prefix, ddoc_name, query)
        #verify view queries results after rebalancing
        rebalance.result()
        self.perform_verify_queries(num_views, prefix, ddoc_name, query)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])
        self._verify_all_buckets(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])

    """Rebalances nodes out of a cluster during view queries incrementally.

    This test begins with all servers clustered together and  loading a given number of items
    into the cluster. It creates num_views as development/production view with
    default map view funcs(is_dev_ddoc = True by default).  It then adds one node at a time and
    rebalances that node into the cluster. During the rebalancing we perform view queries
    for all views and verify the expected number of docs for them.
    Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_out_with_queries(self):
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        #inrease timout for big data
        timeout = max(self.wait_timeout * 4, self.wait_timeout * self.num_items / 100000)
        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"
        tasks = self.async_create_views(self.master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self.wait_timeout)
        self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout)
        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            time.sleep(self.wait_timeout / 5)
            #see that the result of view queries are the same as expected during the test
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout)
            #verify view queries results after rebalancing
            rebalance.result()
            tasks = []
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout)
            self._wait_for_stats_all_buckets(self.servers[:i])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i])

    """Rebalances nodes into a cluster when one node is warming up.

    This test begins with loads a user defined number of items into the cluster
    and all servers clustered together. Next steps are: stop defined
    node(master_restart = False by default), wait 20 sec and start the stopped node.
    Without waiting for the node to start up completely, rebalance out servs_out servers.
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss."""
    def rebalance_out_with_warming_up(self):
        master_restart = self.input.param("master_restart", False)
        if master_restart:
            warmup_node = self.master
        else:
            warmup_node = self.servers[len(self.servers) - self.nodes_out -1]
        servs_out = self.servers[len(self.servers) - self.nodes_out:]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        time.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        rebalance = self.cluster.async_rebalance(self.servers, [], servs_out)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:len(self.servers) - self.nodes_out])
        self._verify_all_buckets(self.master)
        self._verify_stats_all_buckets(self.servers[:len(self.servers) - self.nodes_out])

    """Rebalances nodes out of cluster  during ddoc compaction.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It creates num_views as development/production
    view with default map view funcs(is_dev_ddoc = True by default).
    Then we disabled compaction for ddoc. While we don't reach expected fragmentation
    for ddoc we update docs and perform view queries. We rebalance in  nodes_in nodes
    and start compation when fragmentation was reached fragmentation_value.
    During the rebalancing we wait while compaction will be completed.
    After rebalancing and compaction we wait for the disk queues to drain,
    and then verify that there has been no data loss."""
    def rebalance_out_with_ddoc_compaction(self):
        num_views = self.input.param("num_views", 5)
        fragmentation_value = self.input.param("fragmentation_value", 80)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"
        tasks = []
        tasks = self.async_create_views(self.servers[0], ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self.wait_timeout * 2)
        self.disable_compaction()
        fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.servers[0],
                         prefix + ddoc_name, fragmentation_value, self.default_bucket_name, timeout=20)
        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":
            # update docs to create fragmentation
            self._load_all_buckets(self.master, self.gen_update, "update", 0)
            for view in views:
                # run queries to create indexes
                query = {"stale" : "false"}
                self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)
        fragmentation_monitor.result()

        compaction_task = self.cluster.async_compact_view(self.servers[0], prefix + ddoc_name, self.default_bucket_name)

        servs_out=self.servers[-self.nodes_out:]
        rebalance = self.cluster.async_rebalance([self.master], [], servs_out)
        result = compaction_task.result()
        self.assertTrue(result)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])
        self._verify_all_buckets(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])



    """Rebalances nodes out of a cluster while doing mutations and deletions.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update half of the items in the
    cluster and delete the other half. Once the cluster has been rebalanced the test
    recreates all of the deleted items, waits for the disk queues to drain, and then
    verifies that there has been no data loss. Once all nodes have been rebalanced out
    of the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation_and_deletion(self):
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
            self._load_all_buckets(self.servers[0], gen_2, "delete", 0)
            rebalance.result()
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            self._wait_for_stats_all_buckets(self.servers[:i])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i])

    """Rebalances nodes out of a cluster while doing mutations and expirations.

    This test begins with all servers clustered together and loads a user defined number
    of items into the cluster. It then removes one node at a time from the cluster and
    rebalances. During the rebalance we update all of the items in the cluster and set
    half of the items to expire in 5 seconds. Once the cluster has been rebalanced the
    test recreates all of the expired items, waits for the disk queues to drain, and then
    verifies that there has been no data loss. Once all nodes have been rebalanced out of
    the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation_and_expiration(self):
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)

        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
            self._load_all_buckets(self.servers[0], gen_2, "update", 5)
            rebalance.result()
            time.sleep(5)
            self._load_all_buckets(self.servers[0], gen_2, "create", 0)
            self._wait_for_stats_all_buckets(self.servers[:i])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i])