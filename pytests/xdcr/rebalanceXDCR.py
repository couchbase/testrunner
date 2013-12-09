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
        if self._replication_direction_str in "bidirection":
            self.gen_create2 = BlobGenerator('LoadTwo', 'LoadTwo', self._value_size, end=self._num_items)
            self.gen_delete2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size,
                start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
            self.gen_update2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size, start=0,
                end=int(self._num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
#        super(Rebalance, self).tearDown()
        try:
            self.log.info("==============  XDCRbasetests stats for test #{0} {1} =============="\
                        .format(self.case_number, self._testMethodName))
            self._end_replication_flag = 1
            if hasattr(self, '_stats_thread1'): self._stats_thread1.join()
            if hasattr(self, '_stats_thread2'): self._stats_thread2.join()
            if hasattr(self, '_stats_thread3'): self._stats_thread3.join()
            if self._replication_direction_str in "bidirection":
                if hasattr(self, '_stats_thread4'): self._stats_thread4.join()
                if hasattr(self, '_stats_thread5'): self._stats_thread5.join()
                if hasattr(self, '_stats_thread6'): self._stats_thread6.join()
            if self._replication_direction_str in "bidirection":
                self.log.info("Type of run: BIDIRECTIONAL XDCR")
            else:
                self.log.info("Type of run: UNIDIRECTIONAL XDCR")
            self._print_stats(self.src_master)
            if self._replication_direction_str in "bidirection":
                self._print_stats(self.dest_master)
            self.log.info("============== = = = = = = = = END = = = = = = = = = = ==============")
            self.log.info("==============  rebalanceXDCR cleanup was started for test #{0} {1} =============="\
                    .format(self.case_number, self._testMethodName))
            for nodes in [self.src_nodes, self.dest_nodes]:
                for node in nodes:
                    BucketOperationHelper.delete_all_buckets_or_assert([node], self)
                    ClusterOperationHelper.cleanup_cluster([node], self)
                    ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self)
            self.log.info("==============  rebalanceXDCR cleanup was finished for test #{0} {1} =============="\
                    .format(self.case_number, self._testMethodName))
        finally:
            self.cluster.shutdown()
            self._log_finish(self)

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Async Rebalance-In node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_in(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

            self.sleep(self._timeout / 2)
            for num_rebalance in range(self._num_rebalance):
                add_node = self._floating_servers_set[num_rebalance - 1]
                tasks = []
                if "source" in self._rebalance:
                    tasks.extend(self._async_rebalance(self.src_nodes, [add_node], []))
                    self.log.info(
                        " Starting rebalance-in node {0} at Source cluster {1}".format(add_node.ip, self.src_master.ip))

                    self.src_nodes.extend([add_node])
                    self._floating_servers_set.remove(add_node)

                if "destination" in self._rebalance:
                    if "source" in self._rebalance:
                        add_node = self._floating_servers_set[num_rebalance - 1]
                    tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], []))
                    self.log.info(" Starting rebalance-in node{0} at Destination cluster {1}".format(add_node.ip,
                        self.dest_master.ip))
                    self.dest_nodes.extend([add_node])
                    self._floating_servers_set.remove(add_node)

                self.sleep(self._timeout / 2)
                for task in tasks:
                    task.result()

                if self._replication_direction_str in "unidirection":
                    self._async_modify_data()
                elif self._replication_direction_str in "bidirection":
                    self._async_update_delete_data()
                self.sleep(self._timeout / 2)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass


    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
     Async Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

            self.sleep(self._timeout / 2)
            for num_rebalance in range(self._num_rebalance):
                tasks = []
                if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                    remove_node = self.src_nodes[len(self.src_nodes) - 1]
                    tasks.extend(self._async_rebalance(self.src_nodes, [], [remove_node]))
                    self.log.info(" Starting rebalance-out node {0} at Source cluster {1}".format(remove_node.ip,
                        self.src_master.ip))
                    self.src_nodes.remove(remove_node)
                if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                    remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                    tasks.extend(self._async_rebalance(self.dest_nodes, [], [remove_node]))
                    self.log.info(" Starting rebalance-out node{0} at Destination cluster {1}".format(remove_node.ip,
                        self.dest_master.ip))
                    self.dest_nodes.remove(remove_node)

                self.sleep(self._timeout / 2)
                for task in tasks:
                    task.result()

                if self._replication_direction_str in "unidirection":
                    self._async_modify_data()
                elif self._replication_direction_str in "bidirection":
                    self._async_update_delete_data()
                self.sleep(self._timeout / 2)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass

    """Loading only at source cluster. Async Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out_master(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
            src_buckets = dest_buckets = []
            self.sleep(self._timeout / 2)
            tasks = []
            if "source" in self._rebalance:
                src_buckets = self._get_cluster_buckets(self.src_master)
                tasks.extend(self._async_rebalance(self.src_nodes, [], [self.src_master]))
                self.log.info(" Starting rebalance-out Master node {0} at Source cluster {1}".format(self.src_master.ip,
                    self.src_master.ip))
                self.src_nodes.remove(self.src_nodes[0])
                self.src_master = self.src_nodes[0]
            if "destination" in self._rebalance:
                dest_buckets = self._get_cluster_buckets(self.dest_master)
                tasks.extend(self._async_rebalance(self.dest_nodes, [], [self.dest_master]))
                self.log.info(
                    " Starting rebalance-out Master node {0} at Destination cluster {1}".format(self.dest_master.ip,
                        self.dest_master.ip))
                self.dest_nodes.remove(self.dest_nodes[0])
                self.dest_master = self.dest_nodes[0]

            self.sleep(self._timeout / 2)
            for task in tasks:
                task.result()

            master_id = RestConnection(self.src_master).get_nodes_self().id
            for bucket in src_buckets:
                bucket.master_id = master_id
            master_id = RestConnection(self.dest_master).get_nodes_self().id
            for bucket in dest_buckets:
                bucket.master_id = master_id

            if self._replication_direction_str in "unidirection":
                self._async_modify_data()
            elif self._replication_direction_str in "bidirection":
                self._async_update_delete_data()
            self.sleep(self._timeout / 2)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Swap Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

            self.sleep(self._timeout / 2)
            for num_rebalance in range(self._num_rebalance):
                tasks = []
                add_node = self._floating_servers_set[num_rebalance - 1]
                if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                    remove_node = self.src_nodes[len(self.src_nodes) - 1]
                    tasks.extend(self._async_rebalance(self.src_nodes, [add_node], [remove_node]))
                    self.log.info(
                        " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                            self.src_master.ip, add_node.ip, remove_node.ip))
                    self.src_nodes.remove(remove_node)
                    self.src_nodes.append(add_node)
                self.sleep(self._timeout / 2)
                if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                    if "source" in self._rebalance:
                        add_node = self._floating_servers_set[num_rebalance]
                    remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                    tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], [remove_node]))
                    self.log.info(
                        " Starting swap-rebalance at Destination cluster {0} add node {1} and remove node {2}".format(
                            self.dest_master.ip, add_node.ip, remove_node.ip))
                    self.dest_nodes.remove(remove_node)
                    self.dest_nodes.append(add_node)

                self.sleep(self._timeout / 2)
                for task in tasks:
                    task.result()

                if self._replication_direction_str in "unidirection":
                    self._async_modify_data()
                elif self._replication_direction_str in "bidirection":
                    self._async_update_delete_data()
                self.sleep(self._timeout / 2)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Swap Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance_out_master(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

            self.sleep(self._timeout / 2)
            for num_rebalance in range(self._num_rebalance):
                tasks = []
                src_buckets = dest_buckets = []
                add_node = self._floating_servers_set[num_rebalance - 1]
                if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                    src_buckets = self._get_cluster_buckets(self.src_master)
                    tasks.extend(self._async_rebalance(self.src_nodes, [add_node], [self.src_master]))
                    self.log.info(
                        " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                            self.src_master.ip, add_node.ip, self.src_master.ip))
                    self.src_nodes.remove(self.src_nodes[0])
                    self.src_nodes.insert(0, add_node)
                    self.src_master = add_node

                if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                    if "source" in self._rebalance:
                        add_node = self._floating_servers_set[num_rebalance]
                    dest_buckets = self._get_cluster_buckets(self.dest_master)
                    tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], [self.dest_master]))
                    self.log.info(
                        " Starting swap-rebalance at Destination cluster {0} add node {1} and remove node {2}".format(
                            self.dest_master.ip, add_node.ip, self.dest_master.ip))
                    self.dest_nodes.remove(self.dest_nodes[0])
                    self.dest_nodes.insert(0, add_node)
                    self.dest_master = add_node

                self.sleep(self._timeout / 2)
                for task in tasks:
                    task.result()

                master_id = RestConnection(self.src_master).get_nodes_self().id
                for bucket in src_buckets:
                    bucket.master_id = master_id
                master_id = RestConnection(self.dest_master).get_nodes_self().id
                for bucket in dest_buckets:
                    bucket.master_id = master_id

                if self._replication_direction_str in "unidirection":
                    self._async_modify_data()
                elif self._replication_direction_str in "bidirection":
                    self._async_update_delete_data()
                self.sleep(self._timeout / 2)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass

    """Replication with compaction ddocs and view queries on both clusters.Loading only
        at source cluster, swap rebalancing at source/destination as specified by the user.
    """

    def swap_rebalance_replication_with_ddoc_compaction(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

            self.sleep(self._timeout / 2)

            src_buckets = self._get_cluster_buckets(self.src_master)
            for bucket in src_buckets:
                views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
            ddoc_name = "ddoc1"
            prefix = ("", "dev_")[self._is_dev_ddoc]

            query = {"full_set": "true", "stale": "false"}

            for num_rebalance in range(self._num_rebalance):
                tasks = []
                tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
                tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
                self.sleep(self._timeout / 2)

                add_node = self._floating_servers_set[self._num_rebalance - 1]
                if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                    remove_node = self.src_nodes[len(self.src_nodes) - 1]
                    tasks += self._async_rebalance(self.src_nodes, [add_node], [remove_node])
                    self.log.info(
                        " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                            self.src_master.ip, add_node.ip, remove_node.ip))
                    self.src_nodes.remove(remove_node)
                if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                    remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                    tasks += self._async_rebalance(self.dest_nodes, [add_node], [remove_node])
                    self.log.info(
                        " Starting swap-rebalance at Destination cluster {0} add node {1} and remove node {2}".format(
                            self.dest_master.ip, add_node.ip, remove_node.ip))
                    self.dest_nodes.remove(remove_node)

                self.sleep(30)
                for task in tasks:
                    task.result(self._poll_timeout)
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

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass

    def swap_rebalance_replication_with_view_queries_and_ops(self):
        try:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

            if self._replication_direction_str in "bidirection":
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

            self.sleep(self._timeout / 2)

            src_buckets = self._get_cluster_buckets(self.src_master)
            dest_buckets = self._get_cluster_buckets(self.src_master)
            for bucket in src_buckets:
                views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
            for bucket in src_buckets:
                views_dest = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
            ddoc_name = "ddoc1"
            prefix = ("", "dev_")[self._is_dev_ddoc]

            query = {"full_set": "true", "stale": "false"}

            for num_rebalance in range(self._num_rebalance):
                tasks = []
                add_node = self._floating_servers_set[self._num_rebalance - 1]
                tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
                tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
                for task in tasks:
                    task.result(self._poll_timeout)

                tasks = []
                #Setting up doc-ops at source nodes
                if self._doc_ops is not None:
                    # allows multiple of them but one by one on either of the clusters
                    if "update" in self._doc_ops:
                        tasks.extend(
                            self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
                    if "delete" in self._doc_ops:
                        tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))

                    self.sleep(5)

                if self._doc_ops_dest is not None:
                    if "update" in self._doc_ops_dest:
                        tasks.extend(
                            self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires))
                    if "delete" in self._doc_ops_dest:
                        tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))

                    self.sleep(5)

                if self._rebalance is not None:
                    if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                        remove_node = self.src_nodes[len(self.src_nodes) - 1]
                        tasks += self._async_rebalance(self.src_nodes, [add_node], [remove_node])
                        self.log.info(
                            " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                                self.src_master.ip, add_node.ip, remove_node.ip))
                        self.src_nodes.remove(remove_node)
                        self.src_nodes.append(add_node)

                    if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                        remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                        tasks += self._async_rebalance(self.dest_nodes, [add_node], [remove_node])
                        self.log.info(
                            " Starting swap-rebalance at Destination cluster {0} add node {1} and remove node {2}".format(
                                self.dest_master.ip, add_node.ip, remove_node.ip))
                        self.dest_nodes.remove(remove_node)
                        self.dest_nodes.append(add_node)

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

                if self._replication_direction_str in "unidirection":
                    self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                elif self._replication_direction_str in "bidirection":
                    self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

                tasks = []
                src_nodes = self.get_servers_in_cluster(self.src_master)
                dest_nodes = self.get_servers_in_cluster(self.dest_master)
                self._verify_stats_all_buckets(src_nodes)
                self._verify_stats_all_buckets(dest_nodes)
                for view in views:
                    tasks.append(
                        self.cluster.async_query_view(self.src_master, prefix + ddoc_name, view.name, query,
                            src_buckets[0].kvs[1].__len__()))
                    tasks.append(
                        self.cluster.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query,
                            dest_buckets[0].kvs[1].__len__()))

                for task in tasks:
                    task.result(self._poll_timeout)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results(False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(True)
        finally:
            pass
