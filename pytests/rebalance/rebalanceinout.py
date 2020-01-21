import time
import threading

from rebalance.rebalance_base import RebalanceBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.document import View


class RebalanceInOutTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceInOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceInOutTests, self).tearDown()

    def test_rebalance_in_out_after_mutation(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations.
        Use different nodes_in and nodes_out params to have uneven add and
        deletion. Use 'zone' param to have nodes divided into server groups
        by having zone > 1.

        This test begins by loading a given number of items into the cluster.
        It then removes one node, rebalances that node out the cluster, and
        then rebalances it back in. During the rebalancing we update all of
        the items in the cluster. Once the node has been removed and added
        back we  wait for the disk queues to drain, and then verify that there
        has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have
        reached the point where we are adding back and removing at least half
        of the nodes.
        """
        # Shuffle the nodes if zone > 1 is specified.
        if self.zone > 1:
            self.shuffle_nodes_between_zones_and_rebalance()
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        tasks = self._async_load_all_buckets(self.master, gen, "update", 0)
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        result_nodes = list(set(self.servers[:self.nodes_init] + servs_in) - set(servs_out))
        for task in tasks:
            task.result(self.wait_timeout * 20)

        # Validate seq_no snap_start/stop values
        self.check_snap_start_corruption()

        self._verify_stats_all_buckets(self.servers[:self.nodes_init], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self.sleep(20)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        disk_replica_dataset, disk_active_dataset = self.get_and_compare_active_replica_data_set_all(
            self.servers[:self.nodes_init], self.buckets, path=None)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.add_remove_servers_and_rebalance(servs_in, servs_out)
        self._verify_stats_all_buckets(result_nodes, timeout=120)
        self.verify_cluster_stats(result_nodes, check_ep_items_remaining=True)
        new_failover_stats = self.compare_failovers_logs(prev_failover_stats, result_nodes, self.buckets)
        new_vbucket_stats = self.compare_vbucket_seqnos(prev_vbucket_stats, result_nodes, self.buckets,
                                                        perNode=False)
        self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        self.sleep(30)
        self.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, result_nodes, self.buckets,
                                              path=None)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers=nodes, std=1.0, total_vbuckets=self.total_vbuckets)

        # Validate seq_no snap_start/stop values
        self.check_snap_start_corruption()

    def test_rebalance_in_out_with_failover_addback_recovery(self):
        """
        Rebalances nodes out and in with failover and full/delta recovery add back of a node
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        recovery_type = self.input.param("recoveryType", "full")
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        tasks = self._async_load_all_buckets(self.master, gen, "update", 0)
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        for task in tasks:
            task.result(self.wait_timeout * 20)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self.sleep(20)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        disk_replica_dataset, disk_active_dataset = self.get_and_compare_active_replica_data_set_all(
            self.servers[:self.nodes_init], self.buckets, path=None)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.master)
        self.nodes = self.get_nodes(self.master)
        result_nodes = list(set(self.servers[:self.nodes_init] + servs_in) - set(servs_out))
        for node in servs_in:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        # Mark Node for failover
        self.sleep(30)
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType=recovery_type)
        self.sleep(30)
        try:
            self.shuffle_nodes_between_zones_and_rebalance(servs_out)
        except Exception as e:
            if "deltaRecoveryNotPossible" not in e.__str__():
                self.fail("Rebalance did not fail. Rebalance has to fail since no delta recovery should be possible"
                          " while adding nodes too")

    def test_rebalance_in_out_with_failover(self):
        """
        Rebalances nodes out and in with failover
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        fail_over = self.input.param("fail_over", False)
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        tasks = self._async_load_all_buckets(self.master, gen, "update", 0)
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        for task in tasks:
            task.result(self.wait_timeout * 20)
        # Validate seq_no snap_start/stop values after initial doc_load
        self.check_snap_start_corruption()

        self._verify_stats_all_buckets(self.servers[:self.nodes_init], timeout=120)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self.sleep(20)
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
        prev_failover_stats = self.get_failovers_logs(self.servers[:self.nodes_init], self.buckets)
        disk_replica_dataset, disk_active_dataset = self.get_and_compare_active_replica_data_set_all(
            self.servers[:self.nodes_init], self.buckets, path=None)
        self.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.master)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=1)
        result_nodes = list(set(self.servers[:self.nodes_init] + servs_in) - set(servs_out))
        for node in servs_in:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port)

        # Load data after add-node
        self._load_all_buckets(self.master, gen, "update", 0)
        # Validate seq_no snap_start/stop values
        self.check_snap_start_corruption()

        # Mark Node for failover
        self.rest.fail_over(chosen[0].id, graceful=fail_over)

        # Load data after failover
        self._load_all_buckets(self.master, gen, "update", 0)
        # Validate seq_no snap_start/stop values
        self.check_snap_start_corruption()

        # No need to pass self.sleep_before_rebalance,
        # since prev ops are synchronous call
        self.shuffle_nodes_between_zones_and_rebalance(servs_out)
        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

        self.verify_cluster_stats(result_nodes, check_ep_items_remaining=True)
        self.compare_failovers_logs(prev_failover_stats, result_nodes, self.buckets)
        self.sleep(30)
        self.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, result_nodes, self.buckets,
                                              path=None)
        self.verify_unacked_bytes_all_buckets()
        nodes = self.get_nodes_in_cluster(self.master)
        self.vb_distribution_analysis(servers=nodes, std=1.0, total_vbuckets=self.total_vbuckets)

    def test_incremental_rebalance_in_out_with_max_buckets_number(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations with max
        number of buckets in the cluster.
        Use 'zone' param to have nodes divided into server groups by having zone > 1.

        This test begins by creating max number of buckets with bucket_size=10( by default).
        no we have limitation in 10 buckets:
        one default bucket, all other are sasl and standart buckets. Then we load
        a given number of items into the cluster. It then removes two nodes,
        rebalances that nodes out the cluster, and then rebalances them back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(self.servers[1:self.num_servers], [])
        self.bucket_size = self.input.param("bucket_size", 10)
        bucket_num = min(10, self.quota // self.bucket_size)
        self.log.info('total %s buckets will be created with size %s MB' % (bucket_num, self.bucket_size))
        bucket_params= self._create_bucket_params(server=self.master, size=self.bucket_size, replicas=self.num_replicas)
        self.cluster.create_default_bucket(bucket_params)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                   num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, (bucket_num - 1) // 2)
        self._create_standard_buckets(self.master, bucket_num - 1 - (bucket_num - 1) // 2)
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        # Validate seq_no snap_start/stop values after doc_loading
        self.check_snap_start_corruption()

        for i in reversed(list(range(self.num_servers))[self.num_servers // 2:]):
            tasks = self._async_load_all_buckets(self.master, gen, "update", 0, batch_size=50)
            self.cluster.rebalance(
                self.servers[:i], [], self.servers[i:self.num_servers],
                sleep_before_rebalance=self.sleep_before_rebalance)
            for task in tasks:
                task.result()
            self.sleep(5)
            # Validate seq_no snap_start/stop values
            self.check_snap_start_corruption()

            tasks = self._async_load_all_buckets(self.master, gen, "update", 0, batch_size=50)
            self.add_remove_servers_and_rebalance(
                self.servers[i:self.num_servers], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            for task in tasks:
                task.result()
            self.verify_cluster_stats(self.servers[:self.num_servers], timeout=2400)
            # Validate seq_no snap_start/stop values
            self.check_snap_start_corruption()
        self.verify_unacked_bytes_all_buckets()

    def test_rebalance_in_out_at_once_with_max_buckets_number(self):
        """
        Rebalances nodes in/out at once while doing mutations with max
        number of buckets in the cluster.

        This test begins by creating max number of buckets with bucket_size=quota/maxBucketCount.
        one default bucket, all other are sasl and standart buckets. Then we load
        a given number of items into the cluster. It then removes servs_in nodes and adds
        servs_out, rebalances cluster. During the rebalancing we update all of the items in the cluster.
        Once the node has been rebalanced we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        """
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
        self.bucket_size = self.quota // bucket_num

        self.log.info('total %s buckets will be created with size %s MB' % (bucket_num, self.bucket_size))
        bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                          replicas=self.num_replicas)
        self.cluster.create_default_bucket(bucket_params)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                   num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, (bucket_num - 1) // 2)
        self._create_standard_buckets(self.master, bucket_num - 1 - (bucket_num - 1) // 2)

        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self._wait_for_stats_all_buckets(servs_init)

        rebalance = self.cluster.async_rebalance(
            servs_init, servs_in, servs_out,
            sleep_before_rebalance=self.sleep_before_rebalance)

        tasks = self._async_load_all_buckets(self.master, gen, "update", 0)
        rebalance.result()
        for task in tasks:
            task.result()

        self.verify_cluster_stats(result_nodes)
        self.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_in_out_with_mutation(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations.
        Use 'zone' param to have nodes divided into server groups by
        having zone > 1.

        This test begins by loading a given number of items into the cluster.
        It then removes one node, rebalances that node out the cluster,
        and then rebalances it back in. During the rebalancing we update all
        of the items in the cluster. Once the node has been removed and
        added back we  wait for the disk queues to drain, and then verify that
        there is no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until
        we have reached the point where we are adding back and removing
        at least half of the nodes.
        """
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self.add_remove_servers_and_rebalance(self.servers[1:self.num_servers], [])
        self._load_all_buckets(self.master, gen, "create", 0)

        # Validate seq_no snap_start/stop values with all nodes-in
        self.check_snap_start_corruption()

        batch_size = 50
        for i in reversed(list(range(self.num_servers))[self.num_servers // 2:]):
            tasks = self._async_load_all_buckets(
                self.master, gen, "update", 0,
                batch_size=batch_size, timeout_secs=60)
            self.cluster.rebalance(
                self.servers[:i], [], self.servers[i:self.num_servers],
                sleep_before_rebalance=self.sleep_before_rebalance)

            self.sleep(10)
            for task in tasks:
                task.result(self.wait_timeout * 20)

            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()

            tasks = self._async_load_all_buckets(
                self.master, gen, "update", 0,
                batch_size=batch_size, timeout_secs=60)
            self.add_remove_servers_and_rebalance(
                self.servers[i:self.num_servers], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            for task in tasks:
                task.result(self.wait_timeout * 20)

            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()
            self.verify_cluster_stats(self.servers[:self.num_servers])
        self.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_in_out_with_mutation_and_compaction(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations and
        compaction. Use 'zone' param to have nodes divided into server groups
        by having zone > 1.

        This test begins by loading a given number of items into the cluster.
        It then removes one node, rebalances that node out the cluster, and
        then rebalances it back in. During the rebalancing we update all of the
        items in the cluster. Once the node has been removed and added back we
        wait for the disk queues to drain, and then verify that there has been
        no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until
        we have reached the point where we are adding back and removing
        at least half of the nodes.
        """
        gen = BlobGenerator('mike', 'mike-', self.value_size,
                            end=self.num_items)
        self.add_remove_servers_and_rebalance(
            self.servers[1:self.num_servers], [])
        self._load_all_buckets(self.master, gen, "create", 0)
        batch_size = 50
        for i in reversed(list(range(self.num_servers))[self.num_servers // 2:]):
            tasks = self._async_load_all_buckets(
                self.master, gen, "update", 0,
                batch_size=batch_size, timeout_secs=60)
            for bucket in self.buckets:
                tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
            self.cluster.rebalance(
                self.servers[:i], [], self.servers[i:self.num_servers],
                sleep_before_rebalance=self.sleep_before_rebalance)

            self.sleep(10)
            for task in tasks:
                task.result(self.wait_timeout * 20)

            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()

            tasks = self._async_load_all_buckets(self.master, gen, "update", 0, batch_size=batch_size, timeout_secs=60)
            self.add_remove_servers_and_rebalance(self.servers[i:self.num_servers], [])
            for task in tasks:
                task.result(self.wait_timeout * 20)

            # Validate seq_no snap_start/stop values
            self.check_snap_start_corruption()
            self.verify_cluster_stats(self.servers[:self.num_servers])

        self.verify_unacked_bytes_all_buckets()

    def test_rebalance_in_out_with_compaction_and_expiration_ops(self):
        self.total_loader_threads = self.input.param("total_loader_threads", 10)
        self.expiry_items = self.input.param("expiry_items", 100000)
        self.max_expiry = self.input.param("max_expiry", 30)
        thread_list = []
        self._expiry_pager(self.master, val=1000000)
        for bucket in self.buckets:
            RestConnection(self.master).set_auto_compaction(dbFragmentThreshold=100, bucket=bucket.name)
        num_items = self.expiry_items
        expiry_range = self.max_expiry
        for x in range(1, self.total_loader_threads):
            t = threading.Thread(target=self.run_mc_bin_client, args=(num_items, expiry_range))
            t.daemon = True
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()
        for x in range(1, self.total_loader_threads):
            t = threading.Thread(target=self.run_mc_bin_client, args=(num_items, expiry_range))
            t.daemon = True
            t.start()
            thread_list.append(t)
        self.sleep(20)
        servs_in = self.servers[self.nodes_init:self.nodes_init + 1]
        servs_out = self.servers[self.nodes_init - 1:self.nodes_init]
        result_nodes = list(set(self.servers[:self.nodes_init] + servs_in) - set(servs_out))
        tasks = [self.cluster.async_rebalance(result_nodes, servs_in, servs_out)]
        t = threading.Thread(target=self._run_compaction)
        t.daemon = True
        t.start()
        thread_list.append(t)
        for task in tasks:
            task.result()
        for t in thread_list:
            t.join()

    def test_incremental_rebalance_out_in_with_mutation(self):
        """
        Rebalances nodes in and out of the cluster while doing mutations.
        Use 'zone' param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a initial number of nodes into the cluster.
        It then adds one node, rebalances that node into the cluster,
        and then rebalances it back out. During the rebalancing we update all  of
        the items in the cluster. Once the nodes have been removed and added back we
        wait for the disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        We then add and remove back two nodes at a time and so on until we have reached
        the point where we are adding back and removing at least half of the nodes.
        """
        init_num_nodes = self.input.param("init_num_nodes", 1)
        self.add_remove_servers_and_rebalance(self.servers[1:init_num_nodes], [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        for i in range(self.num_servers):
            tasks = self._async_load_all_buckets(self.master, gen, "update", 0, batch_size=10, timeout_secs=60)
            self.add_remove_servers_and_rebalance(self.servers[init_num_nodes:init_num_nodes + i + 1], [])
            self.sleep(10)
            self.cluster.rebalance(self.servers[:self.num_servers],
                                   [], self.servers[init_num_nodes:init_num_nodes + i + 1])
            for task in tasks:
                task.result(self.wait_timeout * 30)
            self.verify_cluster_stats(self.servers[:init_num_nodes])
        self.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_in_out_with_mutation_and_deletion(self):
        """
        Rebalances nodes into and out of the cluster while doing mutations and
        deletions. Use 'zone' param to have nodes divided into server groups
        by having zone > 1.

        This test begins by loading a given number of items into the cluster.
        It then adds one node, rebalances that node into the cluster, and then
        rebalances it back out. During the rebalancing we update half of the
        items in the cluster and delete the other half. Once the node has been
        removed and added back we recreate the deleted items, wait for the
        disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total. We then remove and
        add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(
            self.servers[1:self.num_servers], [])
        gen_delete = BlobGenerator('mike', 'mike-', self.value_size,
                                   start=self.num_items // 2 + 2000,
                                   end=self.num_items)
        for i in reversed(list(range(self.num_servers))[self.num_servers // 2:]):
            tasks = self._async_load_all_buckets(
                self.master, self.gen_update, "update", 0,
                pause_secs=5, batch_size=1, timeout_secs=60)
            tasks.extend(self._async_load_all_buckets(
                self.master, gen_delete, "delete", 0,
                pause_secs=5, batch_size=1, timeout_secs=60))

            self.cluster.rebalance(
                self.servers[:i], [], self.servers[i:self.num_servers],
                sleep_before_rebalance=self.sleep_before_rebalance)
            self.sleep(60)
            self.add_remove_servers_and_rebalance(
                self.servers[i:self.num_servers], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            for task in tasks:
                task.result(self.wait_timeout * 30)

            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()

            self._load_all_buckets(self.master, gen_delete, "create", 0)
            self.verify_cluster_stats(self.servers[:self.num_servers])

            # Validate seq_no snap_start/stop values after doc_ops 'create'
            self.check_snap_start_corruption()

    def test_incremental_rebalance_in_out_with_mutation_and_expiration(self):
        """
        Rebalances nodes into and out of the cluster while doing mutations and
        expirations. Use 'zone' param to have nodes divided into server groups
        by having zone > 1.

        This test begins by loading a given number of items into the cluster.
        It then adds one node, rebalances that node into the cluster, and then
        rebalances it back out. During the rebalancing we update half of the
        items in the cluster and expire the other half. Once the node has been
        removed and added back we recreate the expired items, wait for the
        disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.We then remove and
        add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(self.servers[1:self.num_servers], [])
        gen_expire = BlobGenerator('mike', 'mike-', self.value_size,
                                   start=self.num_items // 2,
                                   end=self.num_items)
        for i in reversed(list(range(self.num_servers))[self.num_servers // 2:]):
            self.log.info("iteration #{0}".format(i))
            tasks = self._async_load_all_buckets(
                self.master, self.gen_update, "update", 0, batch_size=1)
            tasks.extend(self._async_load_all_buckets(
                self.master, gen_expire, "update", 10, batch_size=1))

            self.cluster.rebalance(
                self.servers[:i], [], self.servers[i:self.num_servers],
                sleep_before_rebalance=self.sleep_before_rebalance)
            self.sleep(20)

            # Validate seq_no snap_start/stop values after rebalance
            self.check_snap_start_corruption()

            rebalance = self.cluster.async_rebalance(
                self.servers[:self.num_servers],
                self.servers[i:self.num_servers], [],
                sleep_before_rebalance=self.sleep_before_rebalance)
            try:
                for task in tasks + [rebalance]:
                    task.result(min(self.wait_timeout * 30, 36000))
            except:
                for task in tasks + [rebalance]:
                    # cancel task won't fix the issue
                    # (load tasks are popped from queue and there is
                    # nothing to cancel, but still there is a thread running)
                    if hasattr(task, '_Thread__stop'):
                        task._Thread__stop()
                raise

            # Validate seq_no snap_start/stop values
            self.check_snap_start_corruption()

            self._load_all_buckets(self.master, gen_expire, "create", 0)
            self.verify_cluster_stats(self.servers[:self.num_servers])
        self.verify_unacked_bytes_all_buckets()

    def test_rebalance_in_out_at_once(self):
        """
        PERFORMANCE:Rebalance in/out at once.
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.


        Then it creates cluster with self.nodes_init nodes. Further
        test loads a given number of items into the cluster. It then
        add  servs_in nodes and remove  servs_out nodes and start rebalance.
        Once cluster was rebalanced the test is finished.
        Available parameters by default are:
        nodes_init=1, nodes_in=1, nodes_out=1
        """
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        rest = RestConnection(self.master)
        self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.add_remove_servers_and_rebalance(servs_in, servs_out)
        self.verify_cluster_stats(result_nodes)
        self.verify_unacked_bytes_all_buckets()

    def test_rebalance_in_out_at_once_persistence_stopped(self):
        """
        PERFORMANCE:Rebalance in/out at once with stopped persistence.

        This test begins by loading a given number of items into the cluster
        with self.nodes_init nodes in it. Then we stop persistence on some
        nodes. Test starts  to update some data and load new data in the
        cluster. At that time we add  servs_in nodes and remove servs_out nodes
        and start rebalance. After rebalance and data ops are completed we
        start verification phase: wait for the disk queues to drain,
        verify the number of items that were/or not persisted
        with expected values, verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.Once All checks passed,
        test is finished.
        Available parameters by default are:
        nodes_init=1, nodes_in=1,
        nodes_out=1, num_nodes_with_stopped_persistence=1
        num_items_without_persistence=100000
        """
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
        gen_extra = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items // 2,
                                  end=self.num_items // 2 + self.num_items_without_persistence)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        tasks = self._async_load_all_buckets(self.master, gen_extra, "create",
                                             0, batch_size=1000)
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        # wait timeout in 60 min because MB-7386 rebalance stuck
        self.cluster.rebalance(
            servs_init[:self.nodes_init], servs_in, servs_out,
            timeout=self.wait_timeout * 60,
            sleep_before_rebalance=self.sleep_before_rebalance)
        for task in tasks:
            task.result()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()

        self._wait_for_stats_all_buckets(servs_init[:self.nodes_init - self.nodes_out],
                                         ep_queue_size=self.num_items_without_persistence * 0.9, ep_queue_size_cond='>')
        self._wait_for_stats_all_buckets(servs_in)
        self._verify_all_buckets(self.master, timeout=None)
        self._verify_stats_all_buckets(result_nodes)
        # verify that curr_items_tot corresponds to sum of curr_items from all nodes
        verified = True
        for bucket in self.buckets:
            verified &= RebalanceHelper.wait_till_total_numbers_match(self.master, bucket)
        self.assertTrue(verified,
                        "Lost items!!! Replication was completed but sum(curr_items) don't match the curr_items_total")
        self.verify_unacked_bytes_all_buckets()

    def test_measure_time_index_during_rebalance(self):
        """
        PERFORMANCE:Test to measure time to index doc during rebalance

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
        num_ddocs=1,num_views=1,data_perc_add=10
        """
        num_ddocs = self.input.param("num_ddocs", 1)
        num_views = self.input.param("num_views", 1)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        ddoc_names = ['ddoc' + str(i) for i in range(num_ddocs)]
        map_func = """function (doc, meta) {{\n  emit(meta.id, "emitted_value_{0}");\n}}"""
        views = [View("view" + str(i), map_func.format(i), None, is_dev_ddoc, False) for i in range(num_views)]
        # views = self.make_default_views(self.default_view_name, num_views, is_dev_ddoc)

        prefix = ("", "dev_")[is_dev_ddoc]
        query = {"connectionTimeout": 60000}
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
        for i in range(num_ddocs * num_views * len(self.buckets)):
            # wait until all initial_build indexer processes are completed
            active_tasks = self.cluster.async_monitor_active_task(servs_init, "indexer", "True", wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
        self.log.info("PERF: indexing time for {0} ddocs with {1} views:{2}".
                      format(num_ddocs, num_views, time.time() - now))

        rest = RestConnection(self.master)
        # self._wait_for_stats_all_buckets(servs_init)
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)

        data_perc_add = self.input.param("data_perc_add", 10)
        gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * (100 + data_perc_add) // 100)
        load_tasks = self._async_load_all_buckets(self.master, gen_create, "create", 0)
        rebalance = self.cluster.async_rebalance(
            servs_init, servs_in, servs_out,
            sleep_before_rebalance=self.sleep_before_rebalance)

        expected_rows = self.num_items * (100 + data_perc_add) // 100 - 1
        start_time = time.time()

        tasks = dict()
        for bucket in self.buckets:
            for ddoc_name in ddoc_names:
                for i in range(num_views):
                    tasks["{0}/_design/{1}/_view/{2}".format(bucket, ddoc_name, "view" + str(i))] = \
                        self.cluster.async_query_view(self.master,
                                                      prefix + ddoc_name, "view" + str(i), query, expected_rows, bucket)
        while len(tasks) > 0 and time.time() - start_time < self.wait_timeout * 30:
            completed_tasks = []
            for task in tasks:
                if tasks[task].done():
                    if tasks[task].result(self.wait_timeout * 30 + start_time - time.time()):
                        self.log.info("expected query result with view {0} was obtained in {1} seconds".
                                      format(task, time.time() - start_time))
                        completed_tasks += [task]
            for completed_task in completed_tasks:
                del tasks[completed_task]

        if len(tasks) > 0:
            for task in tasks:
                tasks[task].result(self.wait_timeout)

        load_tasks[0].result()
        rebalance.result()

        # Validate seq_no snap_start/stop values after rebalance
        self.check_snap_start_corruption()
        self.verify_cluster_stats(result_nodes)
        self.verify_unacked_bytes_all_buckets()
