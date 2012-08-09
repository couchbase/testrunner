from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
import time

"""Testing Rebalance on Unidirectional and Bidirectional XDCR replication setup"""

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
        super(Rebalance, self).tearDown()

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

    def _async_update_delete_data(self):
        self._log.info("The tasks:-")
        tasks = []
        #Setting up doc-ops at source nodes
        if self._doc_ops is not None or self._doc_ops_dest is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "update" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", self._expires))
            if "delete" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))
            time.sleep(5)
            for task in tasks:
                task.result()


    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
    Async Rebalance-In node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_in(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        time.sleep(self._timeout / 6)
        for num_rebalance in range(self._num_rebalance):
            add_node = self._floating_servers_set[num_rebalance - 1]
            tasks = []
            if "source" in self._rebalance:
                tasks.extend(self._async_rebalance(self.src_nodes, [add_node], []))
                self._log.info(
                    " Starting rebalance-in node {0} at Source cluster {1}".format(add_node.ip, self.src_master.ip))

                self.src_nodes.extend([add_node])

            if "destination" in self._rebalance:
                tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], []))
                self._log.info(" Starting rebalance-in node{0} at Destination cluster {1}".format(add_node.ip,
                    self.dest_master.ip))
                self.dest_nodes.extend([add_node])
            time.sleep(self._timeout / 2)

            if self._replication_direction_str in "unidirection":
                self._async_modify_data()
            elif self._replication_direction_str in "bidirection":
                self._async_update_delete_data()

            time.sleep(self._timeout / 6)
            for task in tasks:
                task.result()

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results()

            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(verify_src=True)

    """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
     Async Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        time.sleep(self._timeout / 6)
        for num_rebalance in range(self._num_rebalance):
            tasks = []
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                remove_node = self.src_nodes[len(self.src_nodes) - 1]
                tasks.extend(self._async_rebalance(self.src_nodes, [], [remove_node]))
                self._log.info(" Starting rebalance-out node {0} at Source cluster {1}".format(remove_node.ip,
                    self.src_master.ip))
                self.src_nodes.remove(remove_node)
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                tasks.extend(self._async_rebalance(self.dest_nodes, [], [remove_node]))
                self._log.info(" Starting rebalance-out node{0} at Destination cluster {1}".format(remove_node.ip,
                    self.dest_master.ip))
                self.dest_nodes.remove(remove_node)

            time.sleep(self._timeout / 6)

            if self._replication_direction_str in "unidirection":
                self._async_modify_data()
            elif self._replication_direction_str in "bidirection":
                self._async_update_delete_data()

            time.sleep(self._timeout / 6)
            for task in tasks:
                task.result()

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results()
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(verify_src=True)

    """Loading only at source cluster. Async Rebalance-Out Master node at Source/Destination while
Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
        time.sleep(self._timeout / 6)
        tasks = []
        if "source" in self._rebalance:
            tasks.extend(self._async_rebalance(self.src_nodes, [], [self.src_master]))
            self._log.info(" Starting rebalance-out Master node {0} at Source cluster {1}".format(self.src_master.ip,
                self.src_master.ip))
            self.src_nodes.remove(self.src_master)
            self.src_master = self.src_nodes[0]
        if "destination" in self._rebalance:
            tasks.extend(self._async_rebalance(self.dest_nodes, [], [self.dest_master]))
            self._log.info(
                " Starting rebalance-out Master node {0} at Destination cluster {1}".format(self.dest_master.ip,
                    self.dest_master.ip))
            self.dest_nodes.remove(self.dest_master)
            self.dest_master = self.dest_nodes[0]

        time.sleep(self._timeout / 6)

        if self._replication_direction_str in "unidirection":
            self._async_modify_data()
        elif self._replication_direction_str in "bidirection":
            self._async_update_delete_data()

        time.sleep(self._timeout / 6)
        for task in tasks:
            task.result()

        if self._replication_direction_str in "unidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
            self.verify_results()
        elif self._replication_direction_str in "bidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            self.verify_results(verify_src=True)

        """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
        Swap Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        time.sleep(self._timeout / 6)
        for num_rebalance in range(self._num_rebalance):
            tasks = []
            add_node = self._floating_servers_set[num_rebalance - 1]
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                remove_node = self.src_nodes[len(self.src_nodes) - 1]
                tasks.extend(self._async_rebalance(self.src_nodes, [add_node], [remove_node]))
                self._log.info(
                    " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                        self.src_master.ip, add_node.ip, remove_node.ip))
                self.src_nodes.remove(remove_node)
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], [remove_node]))
                self._log.info(
                    " Starting rebalance-out node{0} at Destination cluster {1}".format(self.dest_master.ip,
                        add_node.ip, remove_node.ip))
                self.dest_nodes.remove(remove_node)

            time.sleep(self._timeout / 6)
            if self._replication_direction_str in "unidirection":
                self._async_modify_data()
            elif self._replication_direction_str in "bidirection":
                self._async_update_delete_data()

            time.sleep(self._timeout / 6)
            for task in tasks:
                task.result()

                if self._replication_direction_str in "unidirection":
                    self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                    self.verify_results()
                elif self._replication_direction_str in "bidirection":
                    self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                    self.verify_results(verify_src=True)

            """Load data only at source for unidirectional, and at both source/destination for bidirection replication.
             Swap Rebalance-Out Master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def swap_rebalance_out_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        time.sleep(self._timeout / 6)
        for num_rebalance in range(self._num_rebalance):
            tasks = []
            add_node = self._floating_servers_set[num_rebalance - 1]
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                remove_node = self.src_master
                tasks.extend(self._async_rebalance(self.src_nodes, [add_node], [remove_node]))
                self._log.info(
                    " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                        self.src_master.ip, add_node.ip, remove_node.ip))
                self.src_nodes.remove(self.src_master)
                self.src_master = self.src_nodes[0]
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                remove_node = self.dest_master
                tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], [remove_node]))
                self._log.info(
                    " Starting rebalance-out node{0} at Destination cluster {1}".format(self.dest_master.ip,
                        add_node.ip, remove_node.ip))
                self.dest_nodes.remove(self.dest_master)
                self.dest_master = self.dest_nodes[0]

            time.sleep(self._timeout / 6)
            if self._replication_direction_str in "unidirection":
                self._async_modify_data()
            elif self._replication_direction_str in "bidirection":
                self._async_update_delete_data()

            time.sleep(self._timeout / 6)
            for task in tasks:
                task.result()

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results()
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(verify_src=True)

    """Replication with compaction ddocs and view queries on both clusters.Loading only
        at source cluster, swap rebalancing at source/destination as specified by the user.
    """

    def swap_rebalance_replication_with_ddoc_compaction(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        time.sleep(self._timeout / 6)

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
            time.sleep(self._timeout / 2)

            add_node = self._floating_servers_set[self._num_rebalance - 1]
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                remove_node = self.src_nodes[len(self.src_nodes) - 1]
                tasks += self._async_rebalance(self.src_nodes, [add_node], [remove_node])
                self._log.info(
                    " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                        self.src_master.ip, add_node.ip, remove_node.ip))
                self.src_nodes.remove(remove_node)
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                tasks += self._async_rebalance(self.dest_nodes, [add_node], [remove_node])
                self._log.info(
                    " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                        self.dest_master.ip, add_node.ip, remove_node.ip))
                self.dest_nodes.remove(remove_node)

            time.sleep(30)
            for task in tasks:
                task.result(self._poll_timeout)
            self.disable_compaction()
            fragmentation_monitor = self._cluster_helper.async_monitor_view_fragmentation(self.src_master,
                prefix + ddoc_name, self.fragmentation_value)
            # generate load until fragmentation reached
            while fragmentation_monitor.state != "FINISHED":
                # update docs to create fragmentation
                self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
                for view in views:
                    # run queries to create indexes
                    self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                    self._cluster_helper.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
            fragmentation_monitor.result()

            compaction_task = self._cluster_helper.async_compact_view(self.src_master, prefix + ddoc_name, 'default')

            result = compaction_task.result()
            self.assertTrue(result)

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
                self.verify_results()
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
                self.verify_results(verify_src=True)

    def swap_rebalance_replication_with_view_queries_and_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        time.sleep(self._timeout / 6)

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
            if self._doc_ops is not None or self._doc_ops_dest is not None:
                # allows multiple of them but one by one on either of the clusters
                if "update" in self._doc_ops:
                    tasks.extend(
                        self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
                if "delete" in self._doc_ops:
                    tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
                if "update" in self._doc_ops_dest:
                    tasks.extend(
                        self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires))
                if "delete" in self._doc_ops_dest:
                    tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))
                if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                    remove_node = self.src_nodes[len(self.src_nodes) - 1]
                    tasks += self._async_rebalance(self.src_nodes, [add_node], [remove_node])
                    self._log.info(
                        " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                            self.src_master.ip, add_node.ip, remove_node.ip))
                    self.src_nodes.remove(remove_node)
                if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                    remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                    tasks += self._async_rebalance(self.dest_nodes, [add_node], [remove_node])
                    self._log.info(
                        " Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                            self.dest_master.ip, add_node.ip, remove_node.ip))
                    self.dest_nodes.remove(remove_node)

                time.sleep(5)
            while True:
                for view in views:
                    self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                    self._cluster_helper.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
                for task in tasks:
                    if task.state != "FINISHED":
                        continue;
                break;

            if self._replication_direction_str in "unidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
            elif self._replication_direction_str in "bidirection":
                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

            tasks = []
            for view in views:
                tasks.append(
                    self._cluster_helper.async_query_view(self.src_master, prefix + ddoc_name, view.name, query,
                        src_buckets[0].kvs[1].__len__()))
                tasks.append(
                    self._cluster_helper.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query,
                        dest_buckets[0].kvs[1].__len__()))

            for task in tasks:
                task.result(self._poll_timeout)

            if self._replication_direction_str in "unidirection":
                self.verify_results()
            elif self._replication_direction_str in "bidirection":
                self.verify_results(verify_src=True)
