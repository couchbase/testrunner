import time

from threading import Thread
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection
from couchbase.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper

class RebalanceInTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceInTests, self).setUp()

    def tearDown(self):
        super(RebalanceInTests, self).tearDown()


    """Rebalances nodes into a cluster while doing docs ops:create, delete, update.

    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform docs ops(add/remove/update/readd)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    then verify that there has been no data loss and sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def rebalance_in_with_ops(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 / 2)
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        if self.output_time:
            start_time = time.time()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_in, [])
        if(self.doc_ops is not None):
            tasks = []
            # define which doc's ops will be performed during rebalancing
            # allows multiple of them but one by one
            if("update" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0)
            if("delete" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0)
            for task in tasks:
                task.result()
        rebalance.result()
        if self.output_time:
            delta_time = time.time() - start_time
            self.log.info("TIME FOR REBALANCE IS %s SECS (%s MINS)" % (delta_time, delta_time / 60))
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])

    def rebalance_in_with_ops_batch(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=(self.num_items / 2 - 1), end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 / 2)
        servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])
        if(self.doc_ops is not None):
            # define which doc's ops will be performed during rebalancing
            # allows multiple of them but one by one
            if("update" in self.doc_ops):
                self._load_all_buckets(self.servers[0], self.gen_update, "update", 0, 1, 4294967295, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("create" in self.doc_ops):
                self._load_all_buckets(self.servers[0], gen_create, "create", 0, 1, 4294967295, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.servers[0], gen_delete, "delete", 0, 1, 4294967295, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        self._verify_all_buckets(self.master, 1, 1000, None, only_store_hash=True, batch_size=5000)
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1])

    """Rebalances nodes into a cluster during getting random keys.

    This test begins by loading a given number of items into the node.
    Then it creates cluster with self.nodes_init nodes. Then we
    send requests to all nodes in the cluster to get random key values.
    Next step is add nodes_in nodes into cluster and rebalance it. During rebalancing
    we get random keys from all nodes and verify that are different every time.
    Once the cluster has been rebalanced we again get random keys from all new nodes
    in the cluster, than we wait for the disk queues to drain, and then
    verify that there has been no data loss, sum(curr_items) match the curr_items_total."""
    def rebalance_in_get_random_key(self):
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])
        self.sleep(5)
        rest_cons = [RestConnection(self.servers[i]) for i in xrange(self.nodes_init)]
        result = []
        num_iter = 0
        # get random keys for each node during rebalancing
        while rest_cons[0]._rebalance_progress_status() == 'running' and num_iter < 100:
            list_threads = []
            temp_result = []
            self.log.info("getting random keys for all nodes in cluster....")
            for rest in rest_cons:
                t = Thread(target=rest.get_random_key,
                       name="get_random_key",
                       args=(self.default_bucket_name,))
                list_threads.append(t)
                temp_result.append(rest.get_random_key(self.default_bucket_name))

                t.start()
            [t.join() for t in list_threads]

            if tuple(temp_result) == tuple(result):
                self.log.exception("random keys are not changed")
            else:
                result = temp_result
            num_iter += 1

        rebalance.result()
        # get random keys for new added nodes
        rest_cons = [RestConnection(self.servers[i]) for i in xrange(self.nodes_init + self.nodes_in)]
        list_threads = []
        for rest in rest_cons:
              t = Thread(target=rest.get_random_key,
                       name="get_random_key",
                       args=(self.default_bucket_name,))
              list_threads.append(t)
              t.start()
        [t.join() for t in list_threads]
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])

    """Rebalances nodes into a cluster while doing mutations.

    This test begins by loading a given number of items into the cluster. It then
    adds two nodes at a time and rebalances that node into the cluster. During the rebalance we
    update(all of the items in the cluster)/delete( num_items/(num_servers -1) in each iteration)/
    create(a half of initial items in each iteration). Once the cluster has been
    rebalanced we wait for the disk queues to drain, and then verify that
    there has been no data loss,  sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_ops(self):
        for i in range(1, self.num_servers, 2):
            rebalance = self.cluster.async_rebalance(self.servers[:i], self.servers[i:i + 2], [])
            if self.doc_ops is not None:
            # define which doc's operation will be performed during rebalancing
            #only one type of ops can be passed
                if("update" in self.doc_ops):
                    # 1/2th of data will be updated in each iteration
                    self._load_all_buckets(self.master, self.gen_update, "update", 0)
                elif("create" in self.doc_ops):
                    # 1/2th of initial data will be added in each iteration
                    gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items * (1 + i) / 2.0 , end=self.num_items * (1 + i / 2.0))
                    self._load_all_buckets(self.master, gen_create, "create", 0)
                elif("delete" in self.doc_ops):
                    # 1/(num_servers) of initial data will be removed after each iteration
                    # at the end we should get empty base( or couple items)
                    gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=int(self.num_items * (1 - i / (self.num_servers - 1.0))) + 1, end=int(self.num_items * (1 - (i - 1) / (self.num_servers - 1.0))))
                    self._load_all_buckets(self.master, gen_delete, "delete", 0)
            rebalance.result()
            self.verify_cluster_stats(self.servers[:i + 2])

    """Rebalances nodes into a cluster  during view queries.

    This test begins by loading a given number of items into the cluster.
    It creates num_views as development/production views with default
    map view funcs(is_dev_ddoc = True by default). It then adds nodes_in nodes
    at a time and rebalances that node into the cluster. During the rebalancing
    we perform view queries for all views and verify the expected number of docs for them.
    Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total.
    Once successful view queries the test is finished.

    added reproducer for MB-6683"""
    def rebalance_in_with_queries(self):


        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])

        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        reproducer = self.input.param("reproducer", False)
        num_tries = self.input.param("num_tries", 10)
        iterations_to_try = (1, num_tries)[reproducer]
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"

        views = []
        tasks = []
        for bucket in self.buckets:
            temp = self.make_default_views(self.default_view_name, num_views,
                                           is_dev_ddoc, different_map=reproducer)
            temp_tasks = self.async_create_views(self.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks

        timeout = max(self.wait_timeout * 4, len(self.buckets) * self.wait_timeout * self.num_items / 50000)

        for task in tasks:
            task.result(self.wait_timeout * 20)

        for bucket in self.buckets:
                for view in views:
                    # run queries to create indexes
                    self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)

        active_tasks = self.cluster.async_monitor_active_task(self.servers[:self.nodes_init], "indexer", "_design/" + prefix + ddoc_name, wait_task=False)
        for active_task in active_tasks:
            result = active_task.result()
            self.assertTrue(result)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"


        for bucket in self.buckets:
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)
        for i in xrange(iterations_to_try):
            servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
            rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
            self.sleep(self.wait_timeout / 5)

            #see that the result of view queries are the same as expected during the test
            for bucket in self.buckets:
                self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)

            rebalance.result()

            #verify view queries results after rebalancing
            for bucket in self.buckets:
                self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)

            self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])
            if reproducer:
                rebalance = self.cluster.async_rebalance(self.servers, [], servs_in)
                rebalance.result()
                self.sleep(self.wait_timeout)

    """Rebalances nodes into a cluster incremental during view queries.

    This test begins by loading a given number of items into the cluster. It creates num_views as
    development/production view with default map view funcs(is_dev_ddoc = True by default).
    It then adds two nodes at a time and rebalances that node into the cluster. During the rebalancing
    we perform view queries for all views and verify the expected number of docs for them.
    Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_queries(self):
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        #increase timeout for big data
        timeout = max(self.wait_timeout * 4, self.wait_timeout * self.num_items / 25000)
        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"
        tasks = []
        tasks = self.async_create_views(self.master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self.wait_timeout * 2)
        for view in views:
            # run queries to create indexes
            self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)

        active_tasks = self.cluster.async_monitor_active_task(self.master, "indexer", "_design/" + prefix + ddoc_name, wait_task=False)
        for active_task in active_tasks:
            result = active_task.result()
            self.assertTrue(result)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
        query["stale"] = "update_after"
        for i in range(1, self.num_servers, 2):
            rebalance = self.cluster.async_rebalance(self.servers[:i], self.servers[i:i + 2], [])
            self.sleep(self.wait_timeout / 5)
            #see that the result of view queries are the same as expected during the test
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
            #verify view queries results after rebalancing
            rebalance.result()
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
            self.verify_cluster_stats(self.servers[:i + 2])

    """Rebalances nodes into a cluster when one node is warming up.

    This test begins by loading a given number of items into the node.
    Then it creates cluster with self.nodes_init nodes. Next steps are:
    stop the latest node in servs_init list( if list size equals 1, master node/
    cluster will be stopped), wait 20 sec and start the stopped node. Without waiting for
    the node to start up completely, rebalance in servs_in servers. Expect that
    rebalance is failed. Wait for warmup complted and strart rebalance with the same
    configuration. Once the cluster has been rebalanced we wait for the disk queues
    to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""
    def rebalance_in_with_warming_up(self):
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_init = self.servers[:self.nodes_init]
        warmup_node = servs_init[-1]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        try:
            rebalance = self.cluster.async_rebalance(servs_init, servs_in, [])
            rebalance.result()
        except RebalanceFailedException:
            self.log.info("rebalance was failed as expected")
            self.assertTrue(ClusterOperationHelper._wait_warmup_completed(self, [warmup_node], \
                            self.default_bucket_name, wait_time=self.wait_timeout * 10))

            self.log.info("second attempt to rebalance")
            rebalance = self.cluster.async_rebalance(servs_init + servs_in, [], [])
            rebalance.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])

    """Rebalances nodes into a cluster during ddoc compaction.

    This test begins by loading a given number of items into the cluster.
    It creates num_views as development/production view with default
    map view funcs(is_dev_ddoc = True by default). Then we disabled compaction for
    ddoc. While we don't reach expected fragmentation for ddoc we update docs and perform
    view queries. We rebalance in  nodes_in nodes and start compation when fragmentation
    was reached fragmentation_value. During the rebalancing we wait
    while compaction will be completed. After rebalancing and compaction we wait for
    the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""
    def rebalance_in_with_ddoc_compaction(self):
        num_views = self.input.param("num_views", 5)
        fragmentation_value = self.input.param("fragmentation_value", 80)
        # now dev_ indexes are not auto-updated, doesn't work with dev view
        is_dev_ddoc = False
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
        self.disable_compaction()
        fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                         prefix + ddoc_name, fragmentation_value, self.default_bucket_name)
        end_time = time.time() + self.wait_timeout * 30
        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
            # update docs to create fragmentation
            self._load_all_buckets(self.master, self.gen_update, "update", 0)
            for view in views:
                # run queries to create indexes
                self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)
        if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
            self.fail("impossible to reach compaction value {0} after {1} sec".
                      format(fragmentation_value, (self.wait_timeout * 30)))

        fragmentation_monitor.result()

        for i in xrange(3):
            active_tasks = self.cluster.async_monitor_active_task(self.master, "indexer", "_design/" + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
            self.sleep(2)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=self.wait_timeout * 3, expected_rows=expected_rows)

        compaction_task = self.cluster.async_compact_view(self.master, prefix + ddoc_name, self.default_bucket_name, with_rebalance=True)
        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        result = compaction_task.result(self.wait_timeout * 10)
        self.assertTrue(result)
        rebalance.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])

    """Rebalances nodes into a cluster while doing mutations and deletions.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update half of the items in the cluster and delete the other half.
    Once the cluster has been rebalanced we recreate the deleted items, wait for the
    disk queues to drain, and then verify that there has been no data loss.
    sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_mutation_and_deletion(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)

        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.master, self.gen_update, "update", 0)
            self._load_all_buckets(self.master, gen_delete, "delete", 0)
            rebalance.result()
            self._load_all_buckets(self.master, gen_delete, "create", 0)
            self.verify_cluster_stats(self.servers[:i + 1])

    """Rebalances nodes into a cluster while doing mutations and expirations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update all items in the cluster. Half of the items updated are also
    given an expiration time of 5 seconds. Once the cluster has been rebalanced we
    recreate the expired items, wait for the disk queues to drain, and then verify
    that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_mutation_and_expiration(self):
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items / 2,
                              end=self.num_items)
        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(self.servers[:i], [self.servers[i]], [])
            self._load_all_buckets(self.master, self.gen_update, "update", 0)
            self._load_all_buckets(self.master, gen_2, "update", 5)
            self.sleep(5)
            rebalance.result()
            self._load_all_buckets(self.master, gen_2, "create", 0)
            self.verify_cluster_stats(self.servers[:i + 1])

    '''
    test rebalances nodes_in nodes ,
    changes bucket passwords and then rebalances nodes_in_second nodes
    '''
    def rebalance_in_with_bucket_password_change(self):
        if self.sasl_buckets == 0:
            self.fail("no sasl buckets are specified!")
        new_pass = self.input.param("new_pass", "new_pass")
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        nodes_in_second = self.input.param("nodes_in_second", 1)
        servs_in_second = self.servers[self.nodes_init + self.nodes_in:
                                       self.nodes_init + self.nodes_in + nodes_in_second]
        servs_init = self.servers[:self.nodes_init]
        servs_result = self.servers[:self.nodes_init + self.nodes_in]

        rebalance = self.cluster.async_rebalance(servs_init, servs_in, [])
        rebalance.result()
        rest = RestConnection(self.master)
        bucket_to_change = [bucket for bucket in self.buckets
                            if bucket.authType == 'sasl' and bucket.name != 'default'][0]
        rest.change_bucket_props(bucket_to_change, saslPassword=new_pass)
        rebalance = self.cluster.async_rebalance(servs_result, servs_in_second, [])
        rebalance.result()
