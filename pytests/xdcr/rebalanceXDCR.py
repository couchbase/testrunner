from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from xdcrbasetests import XDCRReplicationBaseTest

"""Testing Rebalance on Unidirectional and Bidirectional XDCR replication setup"""

#VERIFICATION CURRENTLY DOESN'T SUPPORT STAR TOPOLOGY

class Rebalance(XDCRReplicationBaseTest):
    def setUp(self):
        super(Rebalance, self).setUp()
        self.verify_src = False
        if self._replication_direction_str in "bidirection":
            self.gen_create2 = BlobGenerator('LoadTwo', 'LoadTwo', self._value_size, end=self.num_items)
            self.gen_delete2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size,
                start=int((self.num_items) * (float)(100 - self._percent_delete) / 100), end=self.num_items)
            self.gen_update2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size, start=0,
                end=int(self.num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
        super(Rebalance, self).tearDown()

    def __update_delete(self):
        if self._replication_direction_str in "unidirection":
            self._async_modify_data()
        elif self._replication_direction_str in "bidirection":
            self._async_update_delete_data()
        self.sleep(self.wait_timeout / 2)

    def __merge_buckets(self):
        if self._replication_direction_str in "unidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        elif self._replication_direction_str in "bidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            self.verify_src = True

    def __sync_load_data(self):
        self.log.info("Loading data Synchronously")
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
        self.sleep(self.wait_timeout / 2)

    def __async_load_data(self):
        self.log.info("Loading data Asynchronously")
        load_tasks = self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0, batch_size=1000)
        if self._replication_direction_str in "bidirection":
            load_tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_create2, "create", 0, batch_size=1000))
        return load_tasks

    def __load_data(self):
        async_data_load = self._input.param("async_load", False)
        load_tasks = []
        if async_data_load:
            load_tasks = self.__async_load_data()
        else:
            self.__sync_load_data()
        return load_tasks

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Async Rebalance-In node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_in(self):
        try:
            self.__load_data()

            # Rebalance-IN
            tasks = self._async_rebalance_in()
            [task.result() for task in tasks]

            # Update-Deletes
            self.__update_delete()

            # Merge Items
            self.__merge_buckets()
            self.verify_results(verify_src=self.verify_src)
        finally:
            pass


    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
     Async Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out(self):
        try:
            #MB-9497 to load data during rebalance-out/replication
            tasks = self.__load_data()

            # rebalance-out
            tasks += self._async_rebalance_out()
            [task.result() for task in tasks]

            self.__update_delete()

            self.__merge_buckets()
            self.verify_results(verify_src=self.verify_src)
        finally:
            pass

    """Loading only at source cluster. Async Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out_master(self):
        try:
            self.__load_data()

            # rebalance-out Master
            tasks = self._async_rebalance_out(master=True)
            [task.result() for task in tasks]

            self.__update_delete()

            self.__merge_buckets()

            self.verify_results(verify_src=self.verify_src)
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Swap Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance(self):
        try:
            self.__load_data()
            # Swap-rebalance
            for _ in range(self._num_rebalance):
                tasks = self._async_swap_rebalance()
                [task.result() for task in tasks]

                self.__update_delete()

            self.__merge_buckets()
            self.verify_results(verify_src=self.verify_src)
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Swap Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance_out_master(self):
        try:
            self.__load_data()

            # Swap-rebalance-master
            tasks = self._async_swap_rebalance(master=True)
            [task.result() for task in tasks]

            self.__update_delete()
            self.__merge_buckets()
            self.verify_results(verify_src=self.verify_src)
        finally:
            pass

    """Replication with compaction ddocs and view queries on both clusters.Loading only
        at source cluster, swap rebalancing at source/destination as specified by the user.
    """

    def swap_rebalance_replication_with_ddoc_compaction(self):
        try:
            self.__load_data()

            src_buckets = self._get_cluster_buckets(self.src_master)
            for bucket in src_buckets:
                views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
            ddoc_name = "ddoc1"
            prefix = ("", "dev_")[self._is_dev_ddoc]

            query = {"full_set": "true", "stale": "false"}

            for _ in range(self._num_rebalance):
                tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
                tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
                self.sleep(self.wait_timeout / 2)

                # Swap-rebalance
                tasks += self._async_swap_rebalance()
                self.sleep(30)
                [task.result(self._poll_timeout) for task in tasks]

                self.disable_compaction()
                fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.src_master,
                    prefix + ddoc_name, self.fragmentation_value)
                # generate load until fragmentation reached
                while fragmentation_monitor.state != "FINISHED":
                    # update docs to create fragmentation
                    self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
                    for view in views:
                        # run queries to create indexes
                        self.cluster.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                        self.cluster.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
                fragmentation_monitor.result()

                compaction_task = self.cluster.async_compact_view(self.src_master, prefix + ddoc_name, 'default')

                result = compaction_task.result()
                self.assertTrue(result)

            self.__merge_buckets()
            self.verify_results(verify_src=self.verify_src)
        finally:
            pass

    def swap_rebalance_replication_with_view_queries_and_ops(self):
        tasks = []
        try:
            self.__load_data()

            src_buckets = self._get_cluster_buckets(self.src_master)
            dest_buckets = self._get_cluster_buckets(self.src_master)
            for bucket in src_buckets:
                views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
            ddoc_name = "ddoc1"
            prefix = ("", "dev_")[self._is_dev_ddoc]

            query = {"full_set": "true", "stale": "false"}

            for _ in range(self._num_rebalance):
                tasks = []
                tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
                tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
                [task.result(self._poll_timeout) for task in tasks]

                tasks = []
                #Setting up doc-ops at source nodes
                if self._doc_ops is not None:
                    # allows multiple of them but one by one on either of the clusters
                    if "update" in self._doc_ops:
                        tasks.extend(
                            self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
                    if "delete" in self._doc_ops:
                        tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))

                    self.sleep(10)

                if self._doc_ops_dest is not None:
                    if "update" in self._doc_ops_dest:
                        tasks.extend(
                            self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires))
                    if "delete" in self._doc_ops_dest:
                        tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))

                    self.sleep(10)

                # Swap-rebalance
                tasks += self._async_swap_rebalance()

                self.sleep(5)

                while True:
                    for view in views:
                        self.cluster.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                        self.cluster.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
                    for task in tasks:
                        if task.state != "FINISHED":
                            break
                    else:
                        break

                self.__merge_buckets()

                self._verify_item_count(self.src_master, self.src_nodes)
                self._verify_item_count(self.dest_master, self.dest_nodes)
                tasks = []
                for view in views:
                    tasks.append(
                        self.cluster.async_query_view(self.src_master, prefix + ddoc_name, view.name, query,
                            src_buckets[0].kvs[1].__len__()))
                    tasks.append(
                        self.cluster.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query,
                            dest_buckets[0].kvs[1].__len__()))

                [task.result(self._poll_timeout) for task in tasks]
            self.verify_results(verify_src=self.verify_src)
        finally:
            # Some query tasks not finished after timeout and keep on running,
            # it should be cancelled before proceeding to next test.
            [task.cancel() for task in tasks]
