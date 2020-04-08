import copy

from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator
from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import NodeHelper
from .xdcrnewbasetests import Utility, BUCKET_NAME, OPS
from remote.remote_util import RemoteMachineShellConnection
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection


# Assumption that at least 2 nodes on every cluster
class bidirectional(XDCRNewBaseTest):
    def setUp(self):
        super(bidirectional, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()

    def tearDown(self):
        super(bidirectional, self).tearDown()

    def __perform_ops_joint_sets(self):
        # Merging the keys as keys are actually replicated.
        temp_expires = self._expires
        self._expires = 0   # Assigning it to 0, so that merge_buckets don't wait for expiration here.
        self.merge_all_buckets()

        tasks = []
        kv_gen_src = self.src_cluster.get_kv_gen()[OPS.CREATE]
        gen_update = BlobGenerator(kv_gen_src.name,
                                   kv_gen_src.seed,
                                   kv_gen_src.value_size,
                                   start=0,
                                   end=int(kv_gen_src.end * (float)(self._perc_upd) / 100))
        gen_delete = BlobGenerator(kv_gen_src.name,
                                   kv_gen_src.seed,
                                   kv_gen_src.value_size,
                                   start=int((kv_gen_src.end) * (float)(100 - self._perc_del) / 100),
                                   end=kv_gen_src.end)
        if "C1" in self._upd_clusters:
            tasks += self.src_cluster.async_load_all_buckets_from_generator(gen_update, OPS.UPDATE, self._expires)
        if "C2" in self._upd_clusters:
            tasks += self.dest_cluster.async_load_all_buckets_from_generator(gen_update, OPS.UPDATE, self._expires)
        if "C1" in self._del_clusters:
            tasks += self.src_cluster.async_load_all_buckets_from_generator(gen_delete, OPS.DELETE, 0)
        if "C2" in self._del_clusters:
            tasks += self.dest_cluster.async_load_all_buckets_from_generator(gen_delete, OPS.DELETE, 0)

        for task in tasks:
            task.result()

        self._expires = temp_expires
        if (self._wait_for_expiration and self._expires) and ("C1" in self._upd_clusters or "C2" in self._upd_clusters):
            self.sleep(self._expires)

        self.sleep(self._wait_timeout)

    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket."""
    def load_with_ops(self):
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()

    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket.
    Here running incremental load on both cluster1 and cluster2 as specified by the user/conf file"""

    def load_with_async_ops(self):
        self.setup_xdcr_and_load()
        self.async_perform_update_delete()
        self.verify_results()

    """Testing Bidirectional load( Loading at source/destination). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """
    def load_with_async_ops_and_joint_sets(self):
        self.setup_xdcr_and_load()
        self.async_perform_update_delete()
        self.verify_results()

    def load_with_async_ops_with_warmup(self):
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node())
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node())

        self.sleep(self._wait_timeout)
        NodeHelper.wait_warmup_completed(warmupnodes)
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout // 2)
        self.verify_results()

    def load_with_async_ops_with_warmup_master(self):
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node(master=True))
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node(master=True))

        self.sleep(self._wait_timeout)
        NodeHelper.wait_warmup_completed(warmupnodes)
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout // 2)
        self.verify_results()

    def load_with_async_ops_and_joint_sets_with_warmup(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            "Test case does not apply for Ephemeral buckets"
            return
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node())
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node())

        self.sleep(self._wait_timeout)
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_and_joint_sets_with_warmup_master(self):
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node(master=True))
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node(master=True))

        self.sleep(self._wait_timeout)
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_failover(self):
        self.setup_xdcr_and_load()

        if "C1" in self._failover:
            self.src_cluster.failover_and_rebalance_nodes()
        if "C2" in self._failover:
            self.dest_cluster.failover_and_rebalance_nodes()

        self.sleep(self._wait_timeout // 6)
        self.perform_update_delete()
        self.sleep(300)

        self.verify_results()

    def load_with_failover_then_add_back(self):

        self.setup_xdcr_and_load()

        if "C1" in self._failover:
            self.src_cluster.failover_and_rebalance_nodes(rebalance=False)
            self.src_cluster.add_back_node()
        if "C2" in self._failover:
            self.dest_cluster.failover_and_rebalance_nodes(rebalance=False)
            self.dest_cluster.add_back_node()

        self.perform_update_delete()

        self.verify_results()

    def load_with_failover_master(self):
        self.setup_xdcr_and_load()

        if "C1" in self._failover:
            self.src_cluster.failover_and_rebalance_master()
        if "C2" in self._failover:
            self.dest_cluster.failover_and_rebalance_master()

        self.sleep(self._wait_timeout // 6)
        self.perform_update_delete()

        self.verify_results()

    """Replication with compaction ddocs and view queries on both clusters.

    This test begins by loading a given number of items on both clusters.
    It creates _num_views as development/production view with default
    map view funcs(_is_dev_ddoc = True by default) on both clusters.
    Then we disabled compaction for ddoc on src cluster. While we don't reach
    expected fragmentation for ddoc on src cluster we update docs and perform
    view queries for all views. Then we start compaction when fragmentation
    was reached fragmentation_value. When compaction was completed we perform
    a full verification: wait for the disk queues to drain
    and then verify that there has been no data loss on both clusters."""
    def replication_with_ddoc_compaction(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return

        self.setup_xdcr()

        self.src_cluster.load_all_buckets(self._num_items)
        self.dest_cluster.load_all_buckets(self._num_items)

        num_views = self._input.param("num_views", 5)
        is_dev_ddoc = self._input.param("is_dev_ddoc", True)
        fragmentation_value = self._input.param("fragmentation_value", 80)
        for bucket in self.src_cluster.get_buckets():
            views = Utility.make_default_views(bucket.name, num_views, is_dev_ddoc)

        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {"full_set": "true", "stale": "false"}

        tasks = self.src_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
        tasks += self.dest_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
        for task in tasks:
            task.result(self._poll_timeout)

        self.src_cluster.disable_compaction()
        fragmentation_monitor = self.src_cluster.async_monitor_view_fragmentation(prefix + ddoc_name, fragmentation_value, BUCKET_NAME.DEFAULT)
        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":
            # update docs to create fragmentation
            self.src_cluster.update_delete_data(OPS.UPDATE)
            for view in views:
                # run queries to create indexes
                self.src_cluster.query_view(prefix + ddoc_name, view.name, query)
                self.dest_cluster.query_view(prefix + ddoc_name, view.name, query)
        fragmentation_monitor.result()

        compaction_task = self.src_cluster.async_compact_view(prefix + ddoc_name, 'default')

        self.assertTrue(compaction_task.result())

        self.verify_results()

    def replication_with_view_queries_and_ops(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return
        tasks = []
        try:
            self.setup_xdcr()

            self.src_cluster.load_all_buckets(self._num_items)
            self.dest_cluster.load_all_buckets(self._num_items)

            num_views = self._input.param("num_views", 5)
            is_dev_ddoc = self._input.param("is_dev_ddoc", True)
            for bucket in self.src_cluster.get_buckets():
                views = Utility.make_default_views(bucket.name, num_views, is_dev_ddoc)

            ddoc_name = "ddoc1"
            prefix = ("", "dev_")[is_dev_ddoc]

            query = {"full_set": "true", "stale": "false", "connection_timeout": 60000}

            tasks = self.src_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
            tasks += self.dest_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)

            for task in tasks:
                task.result(self._poll_timeout)

            tasks = []
            # Setting up doc-ops at source nodes
            if "C1" in self._upd_clusters:
                tasks.extend(self.src_cluster.async_update_delete(OPS.UPDATE, self._perc_upd, self._expires))
            if "C1" in self._del_clusters:
                tasks.extend(self.src_cluster.async_update_delete(OPS.DELETE, self._perc_del))
            if "C2" in self._upd_clusters:
                tasks.extend(self.dest_cluster.async_update_delete(OPS.UPDATE, self._perc_upd, self._expires))
            if "C2" in self._del_clusters:
                tasks.extend(self.dest_cluster.async_update_delete(OPS.DELETE, self._perc_del))

            self.sleep(5)
            while True:
                for view in views:
                    self.src_cluster.query_view(prefix + ddoc_name, view.name, query)
                    self.dest_cluster.query_view(prefix + ddoc_name, view.name, query)
                if {task.state for task in tasks} != {"FINISHED"}:
                    continue
                else:
                    if self._wait_for_expiration:
                        if "C1" in self._upd_clusters or "C2" in self._upd_clusters:
                            self.sleep(self._expires)
                    break

            self.merge_all_buckets()
            self.src_cluster.verify_items_count()
            self.dest_cluster.verify_items_count()

            tasks = []
            src_buckets = self.src_cluster.get_buckets()
            dest_buckets = self.dest_cluster.get_buckets()
            for view in views:
                tasks.append(self.src_cluster.async_query_view(prefix + ddoc_name, view.name, query, src_buckets[0].kvs[1].__len__()))
                tasks.append(self.src_cluster.async_query_view(prefix + ddoc_name, view.name, query, dest_buckets[0].kvs[1].__len__()))

            for task in tasks:
                task.result(self._poll_timeout)

            self.verify_results()
        finally:
            # For timeout error, all tasks to be cancelled
            # Before proceeding to next test
            for task in tasks:
                task.cancel()

    """Replication with disabled/enabled ddoc compaction on both clusters.

    This test begins by loading a given number of items on both clusters.
    Then we disabled or enabled compaction on both clusters( set via params).
    Then we mutate and delete data on clusters 3 times. After deletion we recreate
    deleted items. When data was changed 3 times we perform
    a full verification: wait for the disk queues to drain
    and then verify that there has been no data loss on both clusters."""
    def replication_with_disabled_ddoc_compaction(self):
        self.setup_xdcr()
        self.src_cluster.load_all_buckets(self._num_items)
        self.dest_cluster.load_all_buckets(self._num_items)

        if "C1" in self._disable_compaction:
            self.src_cluster.disable_compaction()
        if "C2" in self._disable_compaction:
            self.dest_cluster.disable_compaction()

        # perform doc's ops 3 times to increase rev number
        for _ in range(3):
            self.async_perform_update_delete()
            # wait till deletes have been sent to recreate
            self.sleep(60)
            # restore(re-creating) deleted items
            if 'C1' in self._del_clusters:
                c1_kv_gen = self.src_cluster.get_kv_gen()

                c1_gen_delete = copy.deepcopy(c1_kv_gen[OPS.DELETE])
                if self._expires:
                    # if expiration set, recreate those keys before
                    # trying to update
                    c1_gen_update = copy.deepcopy(c1_kv_gen[OPS.UPDATE])
                    self.src_cluster.load_all_buckets_from_generator(kv_gen=c1_gen_update)
                self.src_cluster.load_all_buckets_from_generator(kv_gen=c1_gen_delete)
            if 'C2' in self._del_clusters:
                c2_kv_gen = self.dest_cluster.get_kv_gen()
                c2_gen_delete = copy.deepcopy(c2_kv_gen[OPS.DELETE])
                if self._expires:
                    c2_gen_update = copy.deepcopy(c2_kv_gen[OPS.UPDATE])
                    self.dest_cluster.load_all_buckets_from_generator(kv_gen=c2_gen_update)
                self.dest_cluster.load_all_buckets_from_generator(kv_gen=c2_gen_delete)
            # wait till we recreate deleted keys before we can delete/update
            self.sleep(300)

        self.verify_results()

    def replication_while_rebooting_a_non_master_src_dest_node(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return
        self.setup_xdcr_and_load()
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout)

        reboot_node_dest = self.dest_cluster.reboot_one_node(self)
        NodeHelper.wait_node_restarted(reboot_node_dest, self, wait_time=self._wait_timeout * 4, wait_if_warmup=True)

        reboot_node_src = self.src_cluster.reboot_one_node(self)
        NodeHelper.wait_node_restarted(reboot_node_src, self, wait_time=self._wait_timeout * 4, wait_if_warmup=True)

        self.sleep(120)
        ClusterOperationHelper.wait_for_ns_servers_or_assert([reboot_node_dest], self, wait_if_warmup=True)
        ClusterOperationHelper.wait_for_ns_servers_or_assert([reboot_node_src], self, wait_if_warmup=True)
        self.verify_results()

    def test_disk_full(self):
        self.setup_xdcr_and_load()
        self.verify_results()

        self.sleep(self._wait_timeout)

        zip_file = "%s.zip" % (self._input.param("file_name", "collectInfo"))
        try:
            for node in [self.src_master, self.dest_master]:
                self.shell = RemoteMachineShellConnection(node)
                self.shell.execute_cbcollect_info(zip_file)
                if self.shell.extract_remote_info().type.lower() != "windows":
                    command = "unzip %s" % (zip_file)
                    output, error = self.shell.execute_command(command)
                    self.shell.log_command_output(output, error)
                    if len(error) > 0:
                        raise Exception("unable to unzip the files. Check unzip command output for help")
                    cmd = 'grep -R "Approaching full disk warning." cbcollect_info*/'
                    output, _ = self.shell.execute_command(cmd)
                else:
                    cmd = "curl -0 http://{1}:{2}@{0}:8091/diag 2>/dev/null | grep 'Approaching full disk warning.'".format(
                                                        self.src_master.ip,
                                                        self.src_master.rest_username,
                                                        self.src_master.rest_password)
                    output, _ = self.shell.execute_command(cmd)
                self.assertNotEqual(len(output), 0, "Full disk warning not generated as expected in %s" % node.ip)
                self.log.info("Full disk warning generated as expected in %s" % node.ip)

                self.shell.delete_files(zip_file)
                self.shell.delete_files("cbcollect_info*")
        except Exception as e:
            self.log.info(e)

    def test_rollback(self):
        bucket = self.src_cluster.get_buckets()[0]
        src_nodes = self.src_cluster.get_nodes()
        dest_nodes = self.dest_cluster.get_nodes()
        nodes = src_nodes + dest_nodes

        # Stop Persistence on Node A & Node B
        for node in nodes:
            mem_client = MemcachedClientHelper.direct_client(node, bucket)
            mem_client.stop_persistence()

        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()
        self.dest_cluster.pause_all_replications()

        gen = BlobGenerator("C1-", "C1-", self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)
        gen = BlobGenerator("C2-", "C2-", self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()
        self.dest_cluster.resume_all_replications()

        # Perform mutations on the bucket
        self.async_perform_update_delete()

        rest1 = RestConnection(self.src_cluster.get_master_node())
        rest2 = RestConnection(self.dest_cluster.get_master_node())

        # Fetch count of docs in src and dest cluster
        _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
        _count2 = rest2.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]

        self.log.info("Before rollback src cluster count = {0} dest cluster count = {1}".format(_count1, _count2))

        # Kill memcached on Node A so that Node B becomes master
        shell = RemoteMachineShellConnection(self.src_cluster.get_master_node())
        shell.kill_memcached()
        shell = RemoteMachineShellConnection(self.dest_cluster.get_master_node())
        shell.kill_memcached()

        # Start persistence on Node B
        mem_client = MemcachedClientHelper.direct_client(src_nodes[1], bucket)
        mem_client.start_persistence()
        mem_client = MemcachedClientHelper.direct_client(dest_nodes[1], bucket)
        mem_client.start_persistence()

        # Failover Node B
        failover_task = self.src_cluster.async_failover()
        failover_task.result()
        failover_task = self.dest_cluster.async_failover()
        failover_task.result()

        # Wait for Failover & rollback to complete
        self.sleep(60)

        # Fetch count of docs in src and dest cluster
        _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
        _count2 = rest2.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]

        self.log.info("After rollback src cluster count = {0} dest cluster count = {1}".format(_count1, _count2))

        self.assertTrue(self.src_cluster.wait_for_outbound_mutations(),
                        "Mutations in source cluster not replicated to target after rollback")
        self.assertTrue(self.dest_cluster.wait_for_outbound_mutations(),
                        "Mutations in target cluster not replicated to source after rollback")

        count = NodeHelper.check_goxdcr_log(
                        src_nodes[0],
                        "Received rollback from DCP stream",
                        goxdcr_log)
        self.assertGreater(count, 0, "rollback did not happen as expected")
        self.log.info("rollback happened as expected")

        count = NodeHelper.check_goxdcr_log(
                        dest_nodes[0],
                        "Received rollback from DCP stream",
                        goxdcr_log)
        self.assertGreater(count, 0, "rollback did not happen as expected")
        self.log.info("rollback happened as expected")

    def test_scramsha(self):
        """
        Creates a new bi-xdcr replication with scram-sha
        Make sure to pass use-scramsha=True
        from command line
        """
        self.setup_xdcr()
        self.sleep(60, "wait before checking logs")
        for node in [self.src_cluster.get_master_node()]+[self.dest_cluster.get_master_node()]:
            count = NodeHelper.check_goxdcr_log(node,
                        "HttpAuthMech=ScramSha for remote cluster reference remote_cluster", timeout=60)
            if count <= 0:
                self.fail("Node {0} does not use SCRAM-SHA authentication".format(node.ip))
            else:
                self.log.info("SCRAM-SHA auth successful on node {0}".format(node.ip))
        self.verify_results()

    def test_update_to_scramsha_auth(self):
        """
        Start with ordinary replication, then switch to use scram_sha_auth
        Search for success log stmtsS
        """
        old_count = NodeHelper.check_goxdcr_log(self.src_cluster.get_master_node(),
                                                "HttpAuthMech=ScramSha for remote cluster reference remote_cluster", timeout=60)
        self.setup_xdcr()
        # modify remote cluster ref to use scramsha
        for remote_cluster in self.src_cluster.get_remote_clusters()+self.dest_cluster.get_remote_clusters():
            remote_cluster.use_scram_sha_auth()
        self.sleep(60, "wait before checking the logs for using scram-sha")
        for node in [self.src_cluster.get_master_node()]+[self.dest_cluster.get_master_node()]:
            count = NodeHelper.check_goxdcr_log(node, "HttpAuthMech=ScramSha for remote cluster reference remote_cluster", timeout=60)
            if count <= old_count:
                self.fail("Node {0} does not use SCRAM-SHA authentication".format(node.ip))
            else:
                self.log.info("SCRAM-SHA auth successful on node {0}".format(node.ip))
        self.verify_results()