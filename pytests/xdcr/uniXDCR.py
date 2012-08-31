from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from random import randrange

import time

#Assumption that at least 2 nodes on every cluster
class unidirectional(XDCRReplicationBaseTest):
    def setUp(self):
        super(unidirectional, self).setUp()

    def tearDown(self):
        super(unidirectional, self).tearDown()

    def _async_modify_data(self):
        tasks = []
        """Setting up creates/updates/deletes at source nodes"""
        if self._doc_ops is not None:
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "create" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """

    def load_with_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        self._modify_src_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters. Create/Update/Delete are performed in parallel- doc-ops specified by the user. """

    def load_with_async_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self._wait_for_stats_all_buckets(self.src_nodes)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed after based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_ops_with_warmup(self):
        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnode = self.src_nodes
        elif self._warmup == "destination":
            warmupnode = self.dest_nodes
        elif self._warmup == "all":
            warmupnode = self.src_nodes
            dest_warm_flag = 1
        warmupnodes.append(warmupnode[randrange(1, len(warmupnode))])
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._modify_src_data()

        self._wait_for_stats_all_buckets(self.src_nodes)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_ops_with_warmup_master(self):
        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnodes.append(self.src_master)
        elif self._warmup == "destination":
            warmupnodes.append(self.dest_master)
        elif self._warmup == "all":
            warmupnodes.append(self.src_master)
            dest_warm_flag = 1
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_master)
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._modify_src_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_with_warmup(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnode = self.src_nodes
        elif self._warmup == "destination":
            warmupnode = self.dest_nodes
        elif self._warmup == "all":
            warmupnode = self.src_nodes
            dest_warm_flag = 1
        warmupnodes.append(warmupnode[randrange(1, len(warmupnode))])
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._async_modify_data()

        time.sleep(30)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self._wait_for_stats_all_buckets(self.src_nodes)
        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_with_warmup_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnodes.append(self.src_master)
        elif self._warmup == "destination":
            warmupnodes.append(self.dest_master)
        elif self._warmup == "all":
            warmupnodes.append(self.src_master)
            dest_warm_flag = 1
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_master)
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._async_modify_data()

        time.sleep(30)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self._wait_for_stats_all_buckets(self.src_nodes)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if "source" in self._failover:
            if len(self.src_nodes) > 1:
                i = len(self.src_nodes) - 1
                self._log.info(
                    " Failing over Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip, self.src_nodes[i].port))
                self._cluster_helper.failover(self.src_nodes, [self.src_nodes[i]])
                self._log.info(" Rebalance out Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip,
                    self.src_nodes[i].port))
                self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
                self.src_nodes.remove(self.src_nodes[i])
            else:
                self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                    len(self.src_nodes)))
        if "destination" in self._failover:
            if len(self.dest_nodes) > 1:
                i = len(self.dest_nodes) - 1
                self._log.info(" Failing over Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                    self.dest_nodes[i].port))
                self._cluster_helper.failover(self.dest_nodes, [self.dest_nodes[i]])
                self._log.info(" Rebalance out Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                    self.dest_nodes[i].port))
                self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
                self.dest_nodes.remove(self.dest_nodes[i])
            else:
                self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                    len(self.dest_nodes)))

        time.sleep(self._timeout / 6)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_failover_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if "source" in self._failover:
            self._log.info(" Failing over Source Master Node {0}:{1}".format(self.src_master.ip, self.src_master.port))
            self._cluster_helper.failover(self.src_nodes, [self.src_master])
            self._log.info(" Rebalance out Source Master Node {0}".format(self.src_master.ip))
            self._cluster_helper.rebalance(self.src_nodes, [], [self.src_master])
            self.src_nodes.remove(self.src_master)
            self.src_master = self.src_nodes[0]

        if "destination" in self._failover:
            self._log.info(" Failing over Destination Master Node {0}".format(self.dest_master.ip))
            self._cluster_helper.failover(self.dest_nodes, [self.dest_master])
            self._log.info(" Rebalance out Destination Master Node {0}".format(self.dest_master.ip))
            self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_master])
            self.dest_nodes.remove(self.dest_master)
            self.dest_master = self.dest_nodes[0]

        time.sleep(self._timeout / 6)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_async_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        tasks = []
        """Setting up failover while creates/updates/deletes at source nodes"""
        if "source" in self._failover:
            i = randrange(1, len(self.src_nodes))
            tasks.extend(self._async_failover(self.src_nodes, [self.src_nodes[i]]))
            self._log.info(" Failing over Source Node {0}".format(self.src_nodes[i].ip))
        if "destination" in self._failover:
            i = randrange(1, len(self.dest_nodes))
            tasks.extend(self._async_failover(self.dest_nodes, [self.dest_nodes[i]]))
            self._log.info(" Failing over Destination Node {0}".format(self.dest_nodes[i].ip))

        self._async_modify_data()

        time.sleep(15)
        for task in tasks:
            task.result()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        if "source" in self._failover:
            self._log.info(" Rebalance out Source Node {0}".format(self.src_nodes[i].ip))
            self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
            self.src_nodes.pop(i)

        if "destination" in self._failover:
            self._log.info(" Rebalance out Destination Node {0}".format(self.dest_nodes[i].ip))
            self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
            self.dest_nodes.pop(i)

            self.verify_results()
            #ToDO - Failover and ADD BACK NODE

    """Replication with compaction ddocs and view queries on both clusters.

        This test begins by loading a given number of items on the source cluster.
        It creates num_views as development/production view with default
        map view funcs(_is_dev_ddoc = True by default) on both clusters.
        Then we disabled compaction for ddoc on src cluster. While we don't reach
        expected fragmentation for ddoc on src cluster we update docs and perform
        view queries for all views. Then we start compaction when fragmentation
        was reached fragmentation_value. When compaction was completed we perform
        a full verification: wait for the disk queues to drain
        and then verify that there has been any data loss on all clusters."""
    def replication_with_ddoc_compaction(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        src_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set" : "true", "stale" : "false"}

        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)

        self.disable_compaction()
        fragmentation_monitor = self._cluster_helper.async_monitor_view_fragmentation(self.src_master,
            prefix + ddoc_name, self.fragmentation_value, "default")

        #generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":
            #update docs to create fragmentation
            self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
            for view in views:
                # run queries to create indexes
                self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
        fragmentation_monitor.result()

        compaction_task = self._cluster_helper.async_compact_view(self.src_master, prefix + ddoc_name, 'default')

        result = compaction_task.result()
        self.assertTrue(result)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()

    def replication_with_view_queries_and_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        src_buckets = self._get_cluster_buckets(self.src_master)
        dest_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)

        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set" : "true", "stale" : "false"}

        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)

        tasks = []
        #Setting up doc-ops at source nodes
        if self._doc_ops is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))

            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))

            time.sleep(5)

        while True:
            for view in views:
                self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                self._cluster_helper.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
            for task in tasks:
                if task.state != "FINISHED":
                    continue
            break

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        tasks = []
        for view in views:
            tasks.append(self._cluster_helper.async_query_view(self.src_master, prefix + ddoc_name, view.name, query, src_buckets[0].kvs[1].__len__()))
            tasks.append(self._cluster_helper.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query, dest_buckets[0].kvs[1].__len__()))

        for task in tasks:
            task.result(self._poll_timeout)

        self.verify_results()

    """Replication with disabled/enabled ddoc compaction on source cluster.

        This test begins by loading a given number of items on the source cluster.
        Then we disabled or enabled compaction on both clusters( set via params).
        Then we mutate and delete data on the source cluster 3 times.
        After deletion we recreate deleted items. When data was changed 3 times
        we perform a full verification: wait for the disk queues to drain
        and then verify that there has been no data loss on both all clusters."""
    def replication_with_disabled_ddoc_compaction(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self.disable_src_comp:
            self.disable_compaction(self.src_master)
        if self.disable_dest_comp:
            self.disable_compaction(self.dest_master)

        # perform doc's ops 3 times to increase rev number
        for i in range(3):
            self._async_modify_data()
            #restore deleted items
            if self._doc_ops is not None:
                if "delete" in self._doc_ops:
                    task = self._async_load_all_buckets(self.src_master, self.gen_delete, "create", 0)
                    time.sleep(5)
                    task.result()


        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()
