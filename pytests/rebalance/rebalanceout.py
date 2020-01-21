import time

from threading import Thread
import threading
from rebalance.rebalance_base import RebalanceBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import RebalanceFailedException
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.kvstore import KVStore

class RebalanceOutTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceOutTests, self).tearDown()

    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update.

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
    from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
    disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
    curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""
    def rebalance_out_after_ops(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 // 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks = []
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0)
            if("delete" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0)
            for task in tasks:
                task.result()
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        self._verify_stats_all_buckets(self.servers[:self.num_servers], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.num_servers], self.buckets)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.num_servers], self.buckets)
        record_data_set = self.get_data_set_all(self.servers[:self.num_servers], self.buckets)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        rebalance = self.cluster.async_rebalance(self.servers[:1], [], servs_out)
        rebalance.result()
        self._verify_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out], timeout=120)
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out], check_ep_items_remaining=True)
        new_failover_stats = self.compare_failovers_logs(prev_failover_stats, self.servers[:self.num_servers - self.nodes_out], self.buckets)
        new_vbucket_stats = self.compare_vbucket_seqnos(prev_vbucket_stats, self.servers[:self.num_servers - self.nodes_out], self.buckets, perNode=False)
        self.sleep(60)
        self.data_analysis_all(record_data_set, self.servers[:self.num_servers - self.nodes_out], self.buckets)
        self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers=nodes, buckets=self.buckets, std=1.0, total_vbuckets=self.total_vbuckets)

    """Rebalances nodes out with failover and full recovery add back of a node

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
    from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
    disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
    curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""
    def rebalance_out_with_failover_full_addback_recovery(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 // 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks = []
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0)
            if("delete" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0)
            for task in tasks:
                task.result()
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        self._verify_stats_all_buckets(self.servers[:self.num_servers], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.rest = RestConnection(self.master)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        self.sleep(20)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.num_servers], self.buckets)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.num_servers], self.buckets)
        record_data_set = self.get_data_set_all(self.servers[:self.num_servers], self.buckets)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="full")
        rebalance = self.cluster.async_rebalance(self.servers[:1], [], servs_out)
        rebalance.result()
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out], check_ep_items_remaining=True)
        self.compare_failovers_logs(prev_failover_stats, self.servers[:self.num_servers - self.nodes_out], self.buckets)
        self.sleep(30)
        self.data_analysis_all(record_data_set, self.servers[:self.num_servers - self.nodes_out], self.buckets)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers=nodes, buckets=self.buckets, std=1.0, total_vbuckets=self.total_vbuckets)

    """Rebalances nodes out with failover

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
    from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
    disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
    curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""
    def rebalance_out_with_failover(self):
        fail_over = self.input.param("fail_over", False)
        self.rest = RestConnection(self.master)
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 // 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks = []
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0)
            if("delete" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0)
            for task in tasks:
                task.result()
        ejectedNode = self.find_node_info(self.master, self.servers[self.nodes_init - 1])
        self._verify_stats_all_buckets(self.servers[:self.num_servers], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.sleep(20)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        record_data_set = self.get_data_set_all(self.servers[:self.nodes_init], self.buckets)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.master)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        new_server_list = self.add_remove_servers(self.servers, self.servers[:self.nodes_init], [self.servers[self.nodes_init - 1], chosen[0]], [])
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=fail_over)
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[chosen[0].id, ejectedNode.id])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance failed")
        self.verify_cluster_stats(new_server_list, check_ep_items_remaining=True)
        self.sleep(30)
        self.data_analysis_all(record_data_set, new_server_list, self.buckets)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers=nodes, buckets=self.buckets, std=1.0, total_vbuckets=self.total_vbuckets)

    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update.

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. It then remove nodes_out from the cluster at a time
    and rebalances. During the rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced the test is finished."""
    def rebalance_out_with_ops(self):
        tasks = list()
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size,
                                   start=self.num_items // 2,
                                   end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size,
                                   start=self.num_items + 1,
                                   end=self.num_items * 3 // 2)
        servs_out = [self.servers[self.num_servers - i - 1]
                     for i in range(self.nodes_out)]
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        if self.doc_ops is not None:
            if "update" in self.doc_ops:
                tasks += self._async_load_all_buckets(
                    self.master, self.gen_update, "update", 0)
            if "create" in self.doc_ops:
                tasks += self._async_load_all_buckets(
                    self.master, gen_create, "create", 0)
            if "delete" in self.doc_ops:
                tasks += self._async_load_all_buckets(
                    self.master, gen_delete, "delete", 0)
        rebalance = self.cluster.async_rebalance(
            self.servers[:1], [], servs_out,
            sleep_before_rebalance=self.sleep_before_rebalance)

        rebalance.result()
        for task in tasks:
            task.result()

        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])
        self.verify_unacked_bytes_all_buckets()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update along with compaction.

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. It then remove nodes_out from the cluster at a time
    and rebalances. During the rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced the test is finished."""
    def rebalance_out_with_compaction_and_ops(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 // 2)
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        tasks = [self.cluster.async_rebalance(self.servers[:1], [], servs_out)]
        for bucket in self.buckets:
            tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            if("create" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0)
            if("delete" in self.doc_ops):
                tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0)
        for task in tasks:
            task.result()
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes from a cluster during getting random keys.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. Then we send requests to all nodes in the cluster
    to get random key values. Next step is remove nodes_out from the cluster
    and rebalance it. During rebalancing we get random keys from all nodes and verify
    that are different every time. Once the cluster has been rebalanced
    we again get random keys from all new nodes in the cluster,
    than we wait for the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""
    def rebalance_out_get_random_key(self):
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        # get random keys for new added nodes
        rest_cons = [RestConnection(self.servers[i]) for i in range(self.num_servers)]
        list_threads = []
        for rest in rest_cons:
              t = Thread(target=rest.get_random_key,
                       name="get_random_key",
                       args=(self.default_bucket_name,))
              list_threads.append(t)
              t.start()
        [t.join() for t in list_threads]

        rest_cons = [RestConnection(self.servers[i]) for i in range(self.num_servers - self.nodes_out)]
        rebalance = self.cluster.async_rebalance(self.servers[:self.num_servers], [], servs_out)
        self.sleep(2)
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
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of a cluster while doing docs' ops.

    This test begins with all servers clustered together and loads a user
    defined number of items into the cluster. It then removes two nodes at a
    time from the cluster and rebalances. During the rebalance we update
    (all of the items in the cluster)/delete(num_items/(num_servers -1) in
    each iteration)/create(a half of initial items in each iteration).
    Once the cluster has been rebalanced the test waits for the disk queues
    to drain and then verifies that there has been no data loss,
    sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes.
    """
    def incremental_rebalance_out_with_ops(self):
        batch_size = 1000
        for i in reversed(list(range(1, self.num_servers, 2))):
            if i == 1:
                batch_size = 1
            tasks = list()
            if self.doc_ops is not None:
                # define which doc_ops will be performed during rebalancing
                # only one type of ops can be passed
                if "update" in self.doc_ops:
                    # 1/2th of data will be updated in each iteration
                    tasks += self._async_load_all_buckets(
                        self.master, self.gen_update, "update", 0,
                        batch_size=batch_size)
                elif "create" in self.doc_ops:
                    # 1/2th of initial data will be added in each iteration
                    gen_create = BlobGenerator(
                        'mike', 'mike-', self.value_size,
                        start=self.num_items * (self.num_servers - i) // 2.0,
                        end=self.num_items * (self.num_servers - i + 1) // 2.0)
                    tasks += self._async_load_all_buckets(
                        self.master, gen_create, "create", 0,
                        batch_size=batch_size)
                elif "delete" in self.doc_ops:
                    # 1/(num_servers) of initial data will be removed after each iteration
                    # at the end we should get empty base( or couple items)
                    gen_delete = BlobGenerator(
                        'mike', 'mike-', self.value_size,
                        start=int(self.num_items * (1 - i // (self.num_servers - 1.0))) + 1,
                        end=int(self.num_items * (1 - (i - 1) // (self.num_servers - 1.0))))
                    tasks += self._async_load_all_buckets(
                        self.master, gen_delete, "delete", 0,
                        batch_size=batch_size)
            rebalance = self.cluster.async_rebalance(
                self.servers[:i], [], self.servers[i:i + 2],
                sleep_before_rebalance=self.sleep_before_rebalance)
            try:
                rebalance.result()
                for task in tasks:
                    task.result()
            except Exception as ex:
                rebalance.cancel()
                for task in tasks:
                    task.cancel()
                raise ex

            self.verify_cluster_stats(self.servers[:i])
            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()

        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of a cluster during view queries.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It creates num_views as
    development/production view with default map view funcs(is_dev_ddoc = True by default).
    It then removes nodes_out nodes at a time and rebalances that node from the cluster.
    During the rebalancing we perform view queries for all views and verify the expected number
    of docs for them. Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss,sum(curr_items) match
    the curr_items_total. Once successful view queries the test is finished."""
    def rebalance_out_with_queries(self):
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"

        views = []
        tasks = []
        for bucket in self.buckets:
            temp = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
            temp_tasks = self.async_create_views(self.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 4, len(self.buckets) * self.wait_timeout * self.num_items // 50000)

        for task in tasks:
            task.result(self.wait_timeout * 20)

        for bucket in self.buckets:
                for view in views:
                    # run queries to create indexes
                    self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)

        active_tasks = self.cluster.async_monitor_active_task(self.servers, "indexer", "_design/" + prefix + ddoc_name, wait_task=False)
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

        servs_out = self.servers[-self.nodes_out:]
        rebalance = self.cluster.async_rebalance([self.master], [], servs_out)
        self.sleep(self.wait_timeout // 5)
        # see that the result of view queries are the same as expected during the test
        for bucket in self.buckets:
           self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)
        # verify view queries results after rebalancing
        rebalance.result()
        for bucket in self.buckets:
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of a cluster during view queries incrementally.

    This test begins with all servers clustered together and  loading a given number of items
    into the cluster. It creates num_views as development/production view with
    default map view funcs(is_dev_ddoc = True by default).  It then adds two nodes at a time and
    rebalances that node into the cluster. During the rebalancing we perform view queries
    for all views and verify the expected number of docs for them.
    Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match
    the curr_items_total. Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_out_with_queries(self):
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        # increase timeout for big data
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 5, self.wait_timeout * self.num_items // 25000)
        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"
        tasks = []
        tasks = self.async_create_views(self.master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self.wait_timeout * 2)
        for view in views:
            # run queries to create indexes
            self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query, timeout=self.wait_timeout * 2)

        for i in range(3):
            active_tasks = self.cluster.async_monitor_active_task(self.servers, "indexer", "_design/" + prefix + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
            self.sleep(2)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
        query["stale"] = "update_after"
        for i in reversed(list(range(1, self.num_servers, 2))):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], self.servers[i:i + 2])
            self.sleep(self.wait_timeout // 5)
            # see that the result of view queries are the same as expected during the test
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
            # verify view queries results after rebalancing
            rebalance.result()
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
            self.verify_cluster_stats(self.servers[:i])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes into a cluster when one node is warming up.

    This test begins with loads a user defined number of items into the cluster
    and all servers clustered together. Next steps are: stop defined
    node(master_restart = False by default), wait 20 sec and start the stopped node.
    Without waiting for the node to start up completely, rebalance out servs_out servers.
    Expect that rebalance is failed. Wait for warmup completed and start
    rebalance with the same configuration. Once the cluster has been rebalanced
    we wait for the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""
    def rebalance_out_with_warming_up(self):
        master_restart = self.input.param("master_restart", False)
        if master_restart:
            warmup_node = self.master
        else:
            warmup_node = self.servers[len(self.servers) - self.nodes_out - 1]
        servs_out = self.servers[len(self.servers) - self.nodes_out:]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        try:
            rebalance = self.cluster.async_rebalance(self.servers, [], servs_out)
            rebalance.result()
        except RebalanceFailedException:
            self.log.info("rebalance was failed as expected")
            self.assertTrue(ClusterOperationHelper._wait_warmup_completed(self, [warmup_node], \
                            self.default_bucket_name, wait_time=self.wait_timeout * 10))

            self.log.info("second attempt to rebalance")
            rebalance = self.cluster.async_rebalance(self.servers, [], servs_out)
            rebalance.result()
        self.verify_cluster_stats(self.servers[:len(self.servers) - self.nodes_out])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of cluster  during ddoc compaction.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It creates num_views as development/production
    view with default map view funcs(is_dev_ddoc = True by default).
    Then we disabled compaction for ddoc. While we don't reach expected fragmentation
    for ddoc we update docs and perform view queries. We rebalance in  nodes_in nodes
    and start compaction when fragmentation was reached fragmentation_value.
    During the rebalancing we wait while compaction will be completed.
    After rebalancing and compaction we wait for the disk queues to drain, and then
    verify that there has been no data loss, sum(curr_items) match the curr_items_total."""
    def rebalance_out_with_ddoc_compaction(self):
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
        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows

        tasks = []
        tasks = self.async_create_views(self.master, ddoc_name, views, self.default_bucket_name, with_query=False)
        for task in tasks:
            task.result(self.wait_timeout * 2)
        self.disable_compaction()
        fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                         prefix + ddoc_name, fragmentation_value, self.default_bucket_name)
        end_time = time.time() + self.wait_timeout * 30
        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
            # update docs to create fragmentation
            """
            it's better to use _load_all_buckets instead of _async_load_all_buckets
            it's workaround: unable to load data when disable_compaction
            now we have a lot of issues for views & compaction....
            """
            update_tasks = self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            for update_task in update_tasks:
                update_task.result(600)
            for view in views:
                # run queries to create indexes
                try:
                    self.log.info("query view {0}/{1}".format(prefix + ddoc_name, view.name))
                    self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)
                except SetViewInfoNotFound as e:
                    self.log.warn("exception on self.cluster.query_view")
                    fragmentation_monitor.cancel()
                    raise e

        if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
            self.fail("impossible to reach compaction value {0} after {1} sec".
                      format(fragmentation_value, (self.wait_timeout * 30)))

        fragmentation_monitor.result()

        for i in range(3):
            active_tasks = self.cluster.async_monitor_active_task(self.servers, "indexer", "_design/" + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
            self.sleep(2)

        query["stale"] = "false"

        self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=self.wait_timeout * 5, expected_rows=expected_rows)

        compaction_task = self.cluster.async_compact_view(self.master, prefix + ddoc_name, self.default_bucket_name, with_rebalance=True)

        servs_out = self.servers[-self.nodes_out:]
        rebalance = self.cluster.async_rebalance([self.master], [], servs_out)
        result = compaction_task.result(self.wait_timeout * 10)
        self.assertTrue(result)
        rebalance.result()
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of a cluster while doing mutations and deletions.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update half of the items in the
    cluster and delete the other half. Once the cluster has been rebalanced the test
    recreates all of the deleted items, waits for the disk queues to drain, and then
    verifies that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation_and_deletion(self):
        gen_2 = BlobGenerator('rebalance-del', 'rebalance-del-', self.value_size, start=self.num_items // 2 + 2000,
                              end=self.num_items)
        batch_size = 1000
        for i in reversed(list(range(self.num_servers))[1:]):
            # don't use batch for rebalance out 2-1 nodes
            for bucket in self.buckets:
                bucket.kvs[2] = KVStore()
            tasks = [self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])]
            tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0, kv_store=1, batch_size=batch_size, timeout_secs=60)
            tasks += self._async_load_all_buckets(self.master, gen_2, "delete", 0, kv_store=2, batch_size=batch_size, timeout_secs=60)
            for task in tasks:
                task.result()
            self.sleep(5)
            self._load_all_buckets(self.master, gen_2, "create", 0)
            self.verify_cluster_stats(self.servers[:i])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of a cluster while doing mutations and expirations.

    This test begins with all servers clustered together and loads a user defined number
    of items into the cluster. It then removes one node at a time from the cluster and
    rebalances. During the rebalance we update all of the items in the cluster and set
    half of the items to expire in 5 seconds. Once the cluster has been rebalanced the
    test recreates all of the expired items, waits for the disk queues to drain, and then
    verifies that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""
    def incremental_rebalance_out_with_mutation_and_expiration(self):
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2 + 2000,
                              end=self.num_items)
        batch_size = 1000
        for i in reversed(list(range(self.num_servers))[2:]):
            # don't use batch for rebalance out 2-1 nodes
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            self._load_all_buckets(self.master, self.gen_update, "update", 0, batch_size=batch_size, timeout_secs=60)
            self._load_all_buckets(self.master, gen_2, "update", 5, batch_size=batch_size, timeout_secs=60)
            rebalance.result()
            self.sleep(5)
            self._load_all_buckets(self.master, gen_2, "create", 0)
            self.verify_cluster_stats(self.servers[:i])
        self.verify_unacked_bytes_all_buckets()

    def rebalance_out_with_compaction_and_expiration_ops(self):
        self.total_loader_threads = self.input.param("total_loader_threads", 10)
        self.expiry_items = self.input.param("expiry_items", 100000)
        self.max_expiry = self.input.param("max_expiry", 30)
        thread_list = []
        self._expiry_pager(self.master, val=1000000)
        for bucket in self.buckets:
            RestConnection(self.master).set_auto_compaction(dbFragmentThreshold=100, bucket = bucket.name)
        num_items = self.expiry_items
        expiry_range = self.max_expiry
        for x in range(1, self.total_loader_threads):
            t = threading.Thread(target=self.run_mc_bin_client, args = (num_items, expiry_range))
            t.daemon = True
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()
        for x in range(1, self.total_loader_threads):
            t = threading.Thread(target=self.run_mc_bin_client, args = (num_items, expiry_range))
            t.daemon = True
            t.start()
            thread_list.append(t)
        self.sleep(20)
        tasks = []
        servs_out = self.servers[len(self.servers) - self.nodes_out:]
        tasks = [self.cluster.async_rebalance(self.servers[:1], [], servs_out)]
        t = threading.Thread(target=self._run_compaction)
        t.daemon = True
        t.start()
        thread_list.append(t)
        for task in tasks:
            task.result()
        for t in thread_list:
            t.join()
