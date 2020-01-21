import time, os

from threading import Thread
import threading
from basetestcase import BaseTestCase
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.helper.rebalance_helper import RebalanceHelper
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
    def rebalance_in_after_ops(self):
        gen_update = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)

        tasks = list()
        tasks += self._async_load_all_buckets(self.master, gen_update, "update", 0)
        for task in tasks:
            task.result()
        servs_in = [self.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        self._verify_stats_all_buckets(self.servers[:self.nodes_init], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self.sleep(20)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        disk_replica_dataset, disk_active_dataset = self.get_and_compare_active_replica_data_set_all(self.servers[:self.nodes_init], self.buckets, path=None)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        rebalance.result()
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + self.nodes_init], timeout=120)
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init], check_ep_items_remaining = True)
        new_failover_stats = self.compare_failovers_logs(prev_failover_stats, self.servers[:self.nodes_in + self.nodes_init], self.buckets)
        new_vbucket_stats = self.compare_vbucket_seqnos(prev_vbucket_stats, self.servers[:self.nodes_in + self.nodes_init], self.buckets)
        self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        self.sleep(30)
        self.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, self.servers[:self.nodes_in + self.nodes_init], self.buckets, path=None)
        self.verify_unacked_bytes_all_buckets()

        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(
            servers=nodes, buckets=self.buckets, std=1.0,
            total_vbuckets=self.total_vbuckets)

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

    """Rebalances nodes in with failover and full recovery add back of a node

    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform docs ops(add/remove/update/readd)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    then verify that there has been no data loss and sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def rebalance_in_with_failover_full_addback_recovery(self):
        gen_update = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        tasks = []
        tasks += self._async_load_all_buckets(self.master, gen_update, "update", 0)
        for task in tasks:
            task.result()
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        self._verify_stats_all_buckets(self.servers[:self.nodes_init], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self.sleep(20)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        disk_replica_dataset, disk_active_dataset = self.get_and_compare_active_replica_data_set_all(self.servers[:self.nodes_init], self.buckets, path=None)
        self.rest = RestConnection(self.master)
        self.nodes = self.get_nodes(self.master)

        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)

        # Perform doc-mutation after node failover
        tasks = self._async_load_all_buckets(
            self.master, gen_update, "update", 0)
        for task in tasks:
            task.result()

        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="full")

        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        rebalance.result()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

        self._verify_stats_all_buckets(self.servers[:self.nodes_in + self.nodes_init], timeout=120)
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init], check_ep_items_remaining = True)
        self.compare_failovers_logs(prev_failover_stats, self.servers[:self.nodes_in + self.nodes_init], self.buckets)
        self.sleep(30)
        self.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, self.servers[:self.nodes_in + self.nodes_init], self.buckets, path=None)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers = nodes, buckets = self.buckets, std = 1.0, total_vbuckets = self.total_vbuckets)

    """Rebalances  after we do add node and graceful failover

    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform docs ops(add/remove/update/readd)
    in the cluster( operate with a half of items that were loaded before).
    We then  add a node and do graceful failover followed by rebalance
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    then verify that there has been no data loss and sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def rebalance_in_with_failover(self):
        fail_over = self.input.param("fail_over", False)
        gen_update = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        tasks = []
        tasks += self._async_load_all_buckets(self.master, gen_update, "update", 0)
        for task in tasks:
            task.result()
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        self._verify_stats_all_buckets(self.servers[:self.nodes_init], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self.sleep(20)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        disk_replica_dataset, disk_active_dataset = self.get_and_compare_active_replica_data_set_all(self.servers[:self.nodes_init], self.buckets, path=None)
        self.rest = RestConnection(self.master)
        self.nodes = self.get_nodes(self.master)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        self.rest = RestConnection(self.master)
        self.rest.add_node(self.master.rest_username,
                           self.master.rest_password,
                           self.servers[self.nodes_init].ip,
                           self.servers[self.nodes_init].port)

        # Perform doc-mutation after add-node
        tasks = self._async_load_all_buckets(self.master, gen_update, "update", 0)
        for task in tasks:
            task.result()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

        # Mark Node for failover
        self.rest.fail_over(chosen[0].id, graceful=fail_over)

        # Perform doc-mutation after node failover
        tasks = self._async_load_all_buckets(self.master, gen_update, "update", 0)
        for task in tasks:
            task.result()

        if fail_over:
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Graceful Failover Failed")
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[chosen[0].id])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance Failed")

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

        # Verification
        new_server_list = self.add_remove_servers(self.servers, self.servers[:self.nodes_init], [chosen[0]], [self.servers[self.nodes_init]])
        self._verify_stats_all_buckets(new_server_list, timeout=120)
        self.verify_cluster_stats(new_server_list, check_ep_items_remaining = True)
        self.compare_failovers_logs(prev_failover_stats, new_server_list, self.buckets)
        self.sleep(30)
        self.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, new_server_list, self.buckets, path=None)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers = nodes, buckets = self.buckets, std = 1.0, total_vbuckets = self.total_vbuckets)

    """Rebalances nodes into a cluster while doing docs ops:create, delete, update.

    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform docs ops(add/remove/update/readd)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    then verify that there has been no data loss and sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def rebalance_in_with_ops(self):
        tasks = list()
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3//2)
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        if self.doc_ops is not None:
            # define which doc's ops will be performed during rebalancing
            # allows multiple of them but one by one
            if "update" in self.doc_ops:
                tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0)
            if "create" in self.doc_ops:
                tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0)
            if "delete" in self.doc_ops:
                tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0)

        rebalance_task = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)

        rebalance_task.result()
        for task in tasks:
            task.result()

        self.sleep(10, "Sleep before stat verification")
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])
        self.verify_unacked_bytes_all_buckets()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

    """Rebalances nodes into a cluster while doing docs ops:create, delete, update.

    This test begins by loading a given number of items into the cluster.
    We later run compaction on all buckets and do ops as well
    """
    def rebalance_in_with_compaction_and_ops(self):
        tasks = list()
        servs_in = [self.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]

        for bucket in self.buckets:
            tasks.append(self.cluster.async_compact_bucket(self.master,
                                                           bucket))
        if self.doc_ops is not None:
            if "update" in self.doc_ops:
                # 1/2th of data will be updated in each iteration
                tasks += self._async_load_all_buckets(
                    self.master, self.gen_update, "update", 0,
                    batch_size=20000, pause_secs=5, timeout_secs=180)
            elif "create" in self.doc_ops:
                # 1/2th of initial data will be added in each iteration
                gen_create = BlobGenerator(
                    'mike', 'mike-', self.value_size,
                    start=self.num_items * (1 + i) // 2.0,
                    end=self.num_items * (1 + i // 2.0))
                tasks += self._async_load_all_buckets(
                    self.master, gen_create, "create", 0,
                    batch_size=20000, pause_secs=5, timeout_secs=180)
            elif "delete" in self.doc_ops:
                 # 1/(num_servers) of initial data will be removed after each iteration
                # at the end we should get empty base( or couple items)
                gen_delete = BlobGenerator('mike', 'mike-', self.value_size,
                                           start=int(self.num_items * (1 - i // (self.num_servers - 1.0))) + 1,
                                           end=int(self.num_items * (1 - (i - 1) // (self.num_servers - 1.0))))
                tasks += self._async_load_all_buckets(
                    self.master, gen_delete, "delete", 0,
                    batch_size=20000, pause_secs=5, timeout_secs=180)

        rebalance_task = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)

        rebalance_task.result()
        for task in tasks:
            task.result()

        self.verify_cluster_stats(self.servers[:self.nodes_in+self.nodes_init])
        self.verify_unacked_bytes_all_buckets()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

    def rebalance_in_with_compaction_and_expiration_ops(self):
        self.total_loader_threads = self.input.param("total_loader_threads", 10)
        self.expiry_items = self.input.param("expiry_items", 100000)
        self.max_expiry = self.input.param("max_expiry", 30)
        self.rebalance_attempts = self.input.param("rebalance_attempts", 100)
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
            num_items = 1000000
            t = threading.Thread(target=self.run_mc_bin_client, args = (num_items, expiry_range))
            t.daemon = True
            t.start()
            thread_list.append(t)
        self.sleep(20)
        tasks = []
        for x in range(1, self.rebalance_attempts):
            servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], servs_in, [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            rebalance.result()
            self.servers  = self.servers[:self.nodes_init+len(servs_in)]
            servs_out = self.servers[len(self.servers) - self.nodes_out:]
            rebalance = self.cluster.async_rebalance(
                self.servers[:1], [], servs_out,
                sleep_before_rebalance=self.sleep_before_rebalance)
            rebalance.result()
        t = threading.Thread(target=self._run_compaction)
        t.daemon = True
        t.start()
        thread_list.append(t)
        for task in tasks:
            task.result()

    def rebalance_in_with_ops_batch(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=(self.num_items // 2 - 1), end=self.num_items)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 // 2)
        servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
        rebalance = self.cluster.async_rebalance(
            self.servers[:1], servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        if self.doc_ops is not None:
            # define which doc's ops will be performed during rebalancing
            # allows multiple of them but one by one
            if "update" in self.doc_ops:
                self._load_all_buckets(self.servers[0], self.gen_update, "update", 0, 1, 4294967295, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if "create" in self.doc_ops:
                self._load_all_buckets(self.servers[0], gen_create, "create", 0, 1, 4294967295, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if "delete" in self.doc_ops:
                self._load_all_buckets(self.servers[0], gen_delete, "delete", 0, 1, 4294967295, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        self._verify_all_buckets(self.master, 1, 1000, None, only_store_hash=True, batch_size=5000)
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes into a cluster during getting random keys.

    This test begins by loading a given number of items into the node.
    Then it creates cluster with self.nodes_init nodes. Then we
    send requests to all nodes in the cluster to get random key values.
    Next step is add nodes_in nodes into cluster and rebalance it.
    During rebalancing we get random keys from all nodes and verify
    that are different every time.
    Once the cluster has been rebalanced we again get random keys
    from all new nodes in the cluster, than we wait for the disk queues
    to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""
    def rebalance_in_get_random_key(self):
        servs_in = self.servers[self.nodes_init:self.nodes_init+self.nodes_in]
        rebalance = self.cluster.async_rebalance(
            self.servers[:1], servs_in, [])
        self.sleep(5)
        rest_cons = [RestConnection(self.servers[i])
                     for i in range(self.nodes_init)]
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
        rest_cons = [RestConnection(self.servers[i])
                     for i in range(self.nodes_init + self.nodes_in)]
        list_threads = []
        for rest in rest_cons:
            t = Thread(target=rest.get_random_key,
                       name="get_random_key",
                       args=(self.default_bucket_name,))
            list_threads.append(t)
            t.start()
        [t.join() for t in list_threads]
        self.verify_cluster_stats(self.servers[:self.nodes_in+self.nodes_init])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes into a cluster while doing mutations.

    This test begins by loading a given number of items into the cluster.
    It then adds two nodes at a time and rebalances that node into the cluster.
    During the rebalance we update(all of the items in the cluster)/
    delete(num_items/(num_servers -1) in each iteration)/
    create(a half of initial items in each iteration).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss,  sum(curr_items)
    match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_ops(self):
        for i in range(1, self.num_servers, 2):
            tasks = list()
            if self.doc_ops is not None:
                # define which doc's operation will be performed during
                # rebalancing only one type of ops can be passed
                if "update" in self.doc_ops:
                    # 1/2th of data will be updated in each iteration
                    tasks += self._async_load_all_buckets(self.master, self.gen_update, "update", 0, batch_size=20000, pause_secs=5, timeout_secs=180)
                elif "create" in self.doc_ops:
                    # 1/2th of initial data will be added in each iteration
                    gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items * (1 + i) // 2.0, end=self.num_items * (1 + i / 2.0))
                    tasks += self._async_load_all_buckets(self.master, gen_create, "create", 0, batch_size=20000, pause_secs=5, timeout_secs=180)
                elif "delete" in self.doc_ops:
                    # 1/(num_servers) of initial data will be removed after each iteration
                    # at the end we should get empty base( or couple items)
                    gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=int(self.num_items * (1 - i // (self.num_servers - 1.0))) + 1, end=int(self.num_items * (1 - (i - 1) // (self.num_servers - 1.0))))
                    tasks += self._async_load_all_buckets(self.master, gen_delete, "delete", 0, batch_size=20000, pause_secs=5, timeout_secs=180)

            rebalance_task = self.cluster.async_rebalance(
                self.servers[:i], self.servers[i:i + 2], [],
                sleep_before_rebalance=self.sleep_before_rebalance)

            rebalance_task.result()
            for task in tasks:
                task.result()
            self.verify_cluster_stats(self.servers[:i + 2])

            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()

        self.verify_unacked_bytes_all_buckets()

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

        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 4, len(self.buckets) * self.wait_timeout * self.num_items // 50000)

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
        for i in range(iterations_to_try):
            servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
            rebalance = self.cluster.async_rebalance(
                [self.master], servs_in, [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            self.sleep(self.wait_timeout // 5)

            # see that the result of view queries are the same as expected during the test
            for bucket in self.buckets:
                self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)

            rebalance.result()

            # verify view queries results after rebalancing
            for bucket in self.buckets:
                self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)

            self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])
            if reproducer:
                rebalance = self.cluster.async_rebalance(
                    self.servers, [], servs_in,
                    sleep_before_rebalance=self.sleep_before_rebalance)
                rebalance.result()
                self.sleep(self.wait_timeout)
        self.verify_unacked_bytes_all_buckets()

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
        num_views = self.input.param("num_views", 3)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        # increase timeout for big data
        timeout = max(self.wait_timeout * 4, self.wait_timeout * self.num_items // 25000)
        query = {}
        query["connectionTimeout"] = 60000
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
            rebalance = self.cluster.async_rebalance(
                self.servers[:i], self.servers[i:i + 2], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            self.sleep(self.wait_timeout // 5)
            # see that the result of view queries are the same as expected during the test
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
            # verify view queries results after rebalancing
            rebalance.result()
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
            self.verify_cluster_stats(self.servers[:i + 2])
        self.verify_unacked_bytes_all_buckets()

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
            rebalance = self.cluster.async_rebalance(
                servs_init, servs_in, [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            rebalance.result()
        except RebalanceFailedException:
            self.log.info("rebalance was failed as expected")
            self.assertTrue(ClusterOperationHelper._wait_warmup_completed(self, [warmup_node], \
                            self.default_bucket_name, wait_time=self.wait_timeout * 10))

            self.log.info("second attempt to rebalance")
            rebalance = self.cluster.async_rebalance(
                servs_init + servs_in, [], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            rebalance.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + self.nodes_init])
        self.verify_unacked_bytes_all_buckets()


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

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows

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

        for i in range(3):
            active_tasks = self.cluster.async_monitor_active_task(self.master, "indexer", "_design/" + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
            self.sleep(2)

        query["stale"] = "false"

        self.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=self.wait_timeout * 3, expected_rows=expected_rows)

        compaction_task = self.cluster.async_compact_view(self.master, prefix + ddoc_name, self.default_bucket_name, with_rebalance=True)
        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance(
            [self.master], servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        result = compaction_task.result(self.wait_timeout * 10)
        self.assertTrue(result)
        rebalance.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes into a cluster while doing mutations and deletions.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update half of the items in the cluster and delete the other half.
    Once the cluster has been rebalanced we recreate the deleted items, wait for the
    disk queues to drain, and then verify that there has been no data loss.
    sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_mutation_and_deletion(self):
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2,
                              end=self.num_items)

        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(
                self.servers[:i], [self.servers[i]], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            self._load_all_buckets(self.master, self.gen_update, "update", 0)
            self._load_all_buckets(self.master, gen_delete, "delete", 0)
            rebalance.result()
            self._load_all_buckets(self.master, gen_delete, "create", 0)
            self.verify_cluster_stats(self.servers[:i + 1])
        self.verify_unacked_bytes_all_buckets()

    """Rebalances nodes into a cluster while doing mutations and expirations.

    This test begins by loading a given number of items into the cluster. It then
    adds one node at a time and rebalances that node into the cluster. During the
    rebalance we update all items in the cluster. Half of the items updated are also
    given an expiration time of 5 seconds. Once the cluster has been rebalanced we
    recreate the expired items, wait for the disk queues to drain, and then verify
    that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced in the test is finished."""
    def incremental_rebalance_in_with_mutation_and_expiration(self):
        gen_2 = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2,
                              end=self.num_items)
        for i in range(self.num_servers)[1:]:
            rebalance = self.cluster.async_rebalance(
                self.servers[:i], [self.servers[i]], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            self._load_all_buckets(self.master, self.gen_update, "update", 0)
            self._load_all_buckets(self.master, gen_2, "update", 5)
            self.sleep(5)
            rebalance.result()
            self._load_all_buckets(self.master, gen_2, "create", 0)
            self.verify_cluster_stats(self.servers[:i + 1])
        self.verify_unacked_bytes_all_buckets()

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

        rebalance = self.cluster.async_rebalance(
            servs_init, servs_in, [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        rebalance.result()
        rest = RestConnection(self.master)
        bucket_to_change = [bucket for bucket in self.buckets
                            if bucket.authType == 'sasl' and bucket.name != 'default'][0]
        rest.change_bucket_props(bucket_to_change, saslPassword=new_pass)
        rebalance = self.cluster.async_rebalance(
            servs_result, servs_in_second, [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        rebalance.result()
        self.verify_unacked_bytes_all_buckets()

    '''
    test changes password of cluster during rebalance.
    http://www.couchbase.com/issues/browse/MB-6459
    '''
    def rebalance_in_with_cluster_password_change(self):
        new_password = self.input.param("new_password", "new_pass")
        servs_result = self.servers[:self.nodes_init + self.nodes_in]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            self.servers[self.nodes_init:self.nodes_init + self.nodes_in], [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        old_pass = self.master.rest_password
        self.sleep(10, "Wait for rebalance have some progress")
        self.change_password(new_password=new_password)
        try:
            rebalance.result()
            self.log.exception("rebalance should be failed when password is changing")
            self.verify_unacked_bytes_all_buckets()
        except Exception as ex:
            self.sleep(10, "wait for rebalance failed")
            rest = RestConnection(self.master)
            self.log.info("Latest logs from UI:")
            for i in rest.get_logs(): self.log.error(i)
            self.assertFalse(RestHelper(rest).is_cluster_rebalanced())
        finally:
            self.change_password(new_password=old_pass)

    '''
    test changes ram quota during rebalance.
    http://www.couchbase.com/issues/browse/CBQE-1649
    '''
    def test_rebalance_in_with_cluster_ramquota_change(self):
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            self.servers[self.nodes_init:self.nodes_init + self.nodes_in], [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        self.sleep(10, "Wait for rebalance have some progress")
        remote = RemoteMachineShellConnection(self.master)
        cli_command = "setting-cluster"
        options = "--cluster-ramsize=%s" % (3000)
        output, error = remote.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost",
                                                     user=self.master.rest_username, password=self.master.rest_password)
        self.assertTrue('\n'.join(output).find('SUCCESS') != -1, 'RAM wasn\'t chnged')
        rebalance.result()


class RebalanceWithPillowFight(BaseTestCase):

    def load(self, server, items, batch=1000):
        import subprocess
        from lib.testconstants import COUCHBASE_FROM_SPOCK
        rest = RestConnection(server)
        cmd = "cbc version"
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail("Exception running cbc-version: subprocess module returned non-zero response!")
        cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -M 50 -B 1000 --populate-only --json" \
            .format(server.ip, items, batch)
        if rest.get_nodes_version()[:5] in COUCHBASE_FROM_SPOCK:
            cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def check_dataloss(self, server, bucket):
        from couchbase.bucket import Bucket
        from couchbase.exceptions import NotFoundError
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name), username="cbadminbucket", password="password")
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = 10000
        errors = []
        while self.num_items > batch_end:
            batch_end = batch_start + batch_size
            keys = []
            for i in range(batch_start, batch_end, 1):
                keys.append(str(i).rjust(20, '0'))
            try:
                bkt.get_multi(keys)
                self.log.info("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys)-1]))
            except Exception as e:
                self.log.error(e)
                self.log.info("Now trying keys in the batch one at a time...")
                key = ''
                try:
                    for key in keys:
                        bkt.get(key)
                except NotFoundError:
                    vBucketId = VBucketAware._get_vBucket_id(key)
                    errors.append("Missing key: {0}, VBucketId: {1}".
                                  format(key, vBucketId))
            batch_start += batch_size
        return errors

    def test_dataloss_rebalance_in(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = Thread(target=self.load,
                               name="pillowfight_load",
                               args=(self.master, self.num_items))

        self.log.info('starting the load thread...')
        load_thread.start()
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            self.servers[self.nodes_init:self.nodes_init + self.nodes_in], [],
            sleep_before_rebalance=self.sleep_before_rebalance)
        rebalance.result()
        load_thread.join()
        errors = self.check_dataloss(self.master, bucket)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print(error)
        if self.num_items != rest.get_active_key_count(bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                          format(self.num_items, rest.get_active_key_count(bucket) ))

