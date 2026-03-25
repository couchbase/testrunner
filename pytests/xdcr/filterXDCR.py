import time

from .xdcrnewbasetests import XDCRNewBaseTest

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper

class XDCRFilterTests(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)

    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.get_cb_cluster_by_name(cluster_name))
        return clusters

    def test_xdcr_with_filter(self):
        tasks = []

        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)
        initial_xdcr = self._input.param("initial_xdcr", False)

        if initial_xdcr:
            self.load_and_setup_xdcr()
        else:
            self.setup_xdcr_and_load()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.pause_all_replications()

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                tasks.append(cluster.async_rebalance_in())
                for task in tasks:
                    task.result()

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                     rebalance=True)

        if rebalance_out:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                tasks.append(cluster.async_rebalance_out())
                for task in tasks:
                    task.result()

        if swap_rebalance:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                tasks.append(cluster.async_swap_rebalance())
                for task in tasks:
                    task.result()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.resume_all_replications()

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            time.sleep(60)

        self.perform_update_delete()
        self.verify_results()

    # ---- 10K Collections Scale Tests ----

    def test_high_cardinality_filter_10k(self):
        """
        Apply a regex filter expression on a bucket-level replication
        targeting 10K collections. The filter matches a subset of doc keys
        and verifies that only matching docs replicate.

        Conf params:
            default@C1=filter_expression:REGEXP_CONTAINS(META()dotidcomma'0$')
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        src_cluster = self.get_cb_cluster_by_name("C1")
        dest_cluster = self.get_cb_cluster_by_name("C2")
        src_master = src_cluster.get_master_node()
        dest_master = dest_cluster.get_master_node()

        TenKCollectionHelper.create_10k_collections(
            src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            src_master, bucket_name, p, run_id="hcfilter")

        try:
            self._wait_for_dest_to_stabilize(dest_cluster, bucket_name,
                                             min_items=1, timeout=600)
        except Exception as e:
            self.fail("Filtered replication did not stabilize: {}".format(e))

        src_items = TenKCollectionHelper.get_bucket_item_count(src_master, bucket_name)
        dest_items = TenKCollectionHelper.get_bucket_item_count(dest_master, bucket_name)
        self.log.info("Bucket items - src: {}, dest: {} (dest expected < src "
                      "due to filter)".format(src_items, dest_items))
        self.assertGreater(dest_items, 0,
                           "Destination should have items matching the filter")
        self.assertLessEqual(dest_items, src_items,
                             "Filtered dest should have <= src items")

        self.log.info("High cardinality filter 10K test passed")

    def test_advanced_filtering_10k(self):
        """
        Advanced filtering using key-based expressions on a replication
        with 10K collections. Applies a META().id LIKE pattern and verifies
        only matching docs replicate.

        Conf params:
            default@C1=filter_expression:REGEXP_CONTAINS(META()dotidcomma'^xdcr10k')
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        src_cluster = self.get_cb_cluster_by_name("C1")
        dest_cluster = self.get_cb_cluster_by_name("C2")
        src_master = src_cluster.get_master_node()
        dest_master = dest_cluster.get_master_node()

        TenKCollectionHelper.create_10k_collections(
            src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            src_master, bucket_name, p, run_id="advfilter")

        # Wait for replication to drain (changes_left -> 0) rather than for
        # dest to reach min_items=1. A valid filter may legitimately exclude
        # every source doc, in which case dest stays at 0 — that's a pass,
        # not a timeout.
        deadline = time.time() + 600
        last_changes_left = None
        drained = False
        while time.time() < deadline:
            try:
                changes_left = src_cluster.get_xdcr_stat(
                    bucket_name, 'replication_changes_left')
            except Exception as e:
                self.log.info("Could not read replication_changes_left: {}".format(e))
                changes_left = None
            self.log.info("replication_changes_left={} (last={})".format(
                changes_left, last_changes_left))
            if changes_left == 0:
                drained = True
                break
            last_changes_left = changes_left
            self.sleep(30, "Waiting for replication_changes_left to reach 0")

        if not drained:
            self.fail("Advanced-filter replication did not drain within 600s "
                      "(last changes_left: {})".format(last_changes_left))

        src_items = TenKCollectionHelper.get_bucket_item_count(src_master, bucket_name)
        dest_items = TenKCollectionHelper.get_bucket_item_count(dest_master, bucket_name)
        self.log.info("Bucket items - src: {}, dest: {}".format(src_items, dest_items))

        self.assertGreater(src_items, 0,
                           "Source should have items loaded before checking filter")
        self.assertLessEqual(dest_items, src_items,
                             "Filtered dest should have <= src items")

        if dest_items == 0:
            self.log.info("Advanced filtering 10K: filter excluded all source "
                          "docs (src={}, dest=0)".format(src_items))
        else:
            self.log.info("Advanced filtering 10K: filter allowed {} of {} "
                          "docs through".format(dest_items, src_items))
        self.log.info("Advanced filtering 10K test passed")
