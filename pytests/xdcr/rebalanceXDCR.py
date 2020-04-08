from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import Utility, BUCKET_NAME, OPS
"""Testing Rebalance on Unidirectional and Bidirectional XDCR replication setup"""


# VERIFICATION CURRENTLY DOESN'T SUPPORT STAR TOPOLOGY
class Rebalance(XDCRNewBaseTest):
    def setUp(self):
        super(Rebalance, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.__rebalance = self._input.param("rebalance", "").split('-')
        self.__failover = self._input.param("failover", "").split('-')
        self.__num_rebalance = self._input.param("num_rebalance", 1)
        self.__num_failover = self._input.param("num_failover", 1)

    def tearDown(self):
        super(Rebalance, self).tearDown()

    def suite_setUp(self):
        self.log.info("*** Rebalance: suite_setUp() ***")

    def suite_tearDown(self):
        self.log.info("*** Rebalance: suite_tearDown() ***")

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Async Rebalance-In node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_in(self):
        try:
            self.setup_xdcr_and_load()

            # Rebalance-IN
            if "C1" in self.__rebalance:
                self.src_cluster.rebalance_in()
            if "C2" in self.__rebalance:
                self.dest_cluster.rebalance_in()

            self.perform_update_delete()

            self.sleep(150)
            self.verify_results()
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
     Async Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out(self):
        try:
            # MB-9497 to load data during rebalance-out/replication
            # FIXME for async_load
            self.setup_xdcr_and_load()

            # Rebalance-Out
            if "C1" in self.__rebalance:
                self.src_cluster.rebalance_out()
            if "C2" in self.__rebalance:
                self.dest_cluster.rebalance_out()

            self.perform_update_delete()

            self.sleep(150)
            self.verify_results()
        finally:
            pass

    def failover_and_rebalance_in_out(self):
        """
        MB-18887
        This test makes use of async_load,
        we start loading, then failover and rebalance-in-out required nodes
        on respective clusters and then after loading completes, perform deletes
        and then verify data.
        """
        try:
            tasks = self.setup_xdcr_async_load()

            if "C1" in self.__failover:
                self.src_cluster.failover(
                    num_nodes=self.__num_failover)
                tasks.append(self.src_cluster.async_rebalance_in_out(
                    num_add_nodes=self.__num_rebalance,
                    remove_nodes=[]))
            if "C2" in self.__failover:
                self.dest_cluster.failover(
                    num_nodes=self.__num_failover)
                tasks.append(self.dest_cluster.async_rebalance_in_out(
                    num_add_nodes=self.__num_rebalance,
                    remove_nodes=[]))
            for task in tasks:
                task.result()

            self.perform_update_delete()
            self.verify_results()
        finally:
            pass

    """Loading only at source cluster. Async Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out_master(self):
        try:
            self.setup_xdcr_and_load()

            # Rebalance-IN
            if "C1" in self.__rebalance:
                self.src_cluster.rebalance_out_master()
            if "C2" in self.__rebalance:
                self.dest_cluster.rebalance_out_master()

            self.perform_update_delete()

            self.sleep(150)
            self.verify_results()
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Swap Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance(self):
        try:
            self.setup_xdcr_and_load()

            # Swap-Rebalance
            for _ in range(self.__num_rebalance):
                if "C1" in self.__rebalance:
                    self.src_cluster.swap_rebalance()
                if "C2" in self.__rebalance:
                    self.dest_cluster.swap_rebalance()

            self.perform_update_delete()

            self.sleep(150)
            self.verify_results()
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Swap Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance_out_master(self):
        try:
            self.setup_xdcr_and_load()

            # Swap-Rebalance
            if "C1" in self.__rebalance:
                self.src_cluster.swap_rebalance_master()
            if "C2" in self.__rebalance:
                self.dest_cluster.swap_rebalance_master()

            self.perform_update_delete()

            self.sleep(150)
            self.verify_results()
        finally:
            pass

    """Replication with compaction ddocs and view queries on both clusters.Loading only
        at source cluster, swap rebalancing at source/destination as specified by the user.
    """

    def swap_rebalance_replication_with_ddoc_compaction(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return
        try:
            self.setup_xdcr_and_load()

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

            # Swap-Rebalance
            for _ in range(self.__num_rebalance):
                if "C1" in self.__rebalance:
                    tasks.append(self.src_cluster.async_swap_rebalance())
                if "C2" in self.__rebalance:
                    tasks.append(self.dest_cluster.async_swap_rebalance())

            self.sleep(self._wait_timeout // 2)
            for task in tasks:
                task.result(self._poll_timeout)

            self.src_cluster.disable_compaction()
            fragmentation_monitor = self.src_cluster.async_monitor_view_fragmentation(prefix + ddoc_name, fragmentation_value, BUCKET_NAME.DEFAULT)
            # generate load until fragmentation reached
            while fragmentation_monitor.state != "FINISHED":
                # update docs to create fragmentation
                self.src_cluster.update_delete_data(OPS.UPDATE, self._perc_upd, self._expires)
                for view in views:
                    # run queries to create indexes
                    self.src_cluster.query_view(prefix + ddoc_name, view.name, query)
                    self.dest_cluster.query_view(prefix + ddoc_name, view.name, query)
            fragmentation_monitor.result()

            compaction_task = self.src_cluster.async_compact_view(prefix + ddoc_name, 'default')

            self.assertTrue(compaction_task.result())

            self.verify_results()
        finally:
            pass

    def swap_rebalance_replication_with_view_queries_and_ops(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return
        tasks = []
        try:
            self.setup_xdcr_and_load()

            num_views = self._input.param("num_views", 5)
            is_dev_ddoc = self._input.param("is_dev_ddoc", True)
            for bucket in self.src_cluster.get_buckets():
                views = Utility.make_default_views(bucket.name, num_views, is_dev_ddoc)

            ddoc_name = "ddoc1"
            prefix = ("", "dev_")[is_dev_ddoc]

            query = {"full_set" : "true", "stale" : "false", "connection_timeout" : 60000}

            tasks = self.src_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
            tasks += self.dest_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)

            for task in tasks:
                task.result(self._poll_timeout)

            self.async_perform_update_delete()

            tasks=[]
            # Swap-Rebalance
            for _ in range(self.__num_rebalance):
                if "C1" in self.__rebalance:
                    tasks.append(self.src_cluster.async_swap_rebalance())
                if "C2" in self.__rebalance:
                    tasks.append(self.dest_cluster.async_swap_rebalance())

            for task in tasks:
                task.result()

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
            # Some query tasks not finished after timeout and keep on running,
            # it should be cancelled before proceeding to next test.
            [task.cancel() for task in tasks]
