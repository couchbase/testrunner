
import time

from .xdcrnewbasetests import XDCRNewBaseTest, NodeHelper, Utility
from .xdcrnewbasetests import OPS
from membase.api.rest_client import RestConnection

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper


class XDCRTopologyTest(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.assertTrue(
            self.get_num_cb_cluster() >= 3,
            "Atleast 3 Clusters needed to run Topology Tests"
        )

    """
    rebalance : test parameter e.g. "C1:C2:C4"
    """

    def __get_rebalance_clusters(self):
        rebalance_clusters = self._input.param("rebalance", None)
        if rebalance_clusters:
            rebalance_clusters = rebalance_clusters.split(":")
        return rebalance_clusters

    def load_with_ops(self):
        self.setup_xdcr_and_load()

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    def load_with_rebalance_out(self):
        num_rebalance = self._input.param("num_rebalance", 1)

        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.rebalance_out(num_rebalance)

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    def load_with_rebalance_out_master(self):
        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.rebalance_out_master()

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    def load_with_rebalance_in(self):
        num_rebalance = self._input.param("num_rebalance", 1)
        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.rebalance_in(num_rebalance)

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    def load_with_swap_rebalance(self):
        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.swap_rebalance()

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    def load_with_swap_rebalance_master(self):
        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.swap_rebalance_master()

        self.perform_update_delete()

        self.verify_results()

    def load_with_failover(self):
        num_rebalance = self._input.param("num_rebalance", 1)
        graceful = self._input.param("graceful", False)
        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.failover_and_rebalance_nodes(
                num_nodes=num_rebalance,
                graceful=graceful)

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    def load_with_failover_master(self):
        graceful = self._input.param("graceful", False)
        self.setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_by_name(rebalance_cluster)
            cb_cluster.failover_and_rebalance_master(graceful=graceful)

        self.perform_update_delete()
        self.merge_all_buckets()
        self.verify_results()

    # ---- 10K Collections Scale Tests ----

    def _create_10k_on_all_clusters(self, bucket_name, p):
        for cb_cluster in self.get_cb_clusters():
            TenKCollectionHelper.create_10k_collections(
                cb_cluster.get_master_node(), bucket_name,
                **{k: p[k] for k in
                ("num_scopes", "collections_per_scope", "scope_prefix",
                 "collection_prefix")})

    def test_fanout_1_to_N_10k_collections(self):
        """
        Fanout: one source (C1) replicating to all other clusters with
        10K collections. Uses star topology with unidirection.
        Requires chain_length >= 3, ctopology=star, rdirection=unidirection.
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        self._create_10k_on_all_clusters(bucket_name, p)

        self.setup_xdcr()

        src_cluster = self.get_cb_cluster_by_name("C1")
        TenKCollectionHelper.select_and_load(
            src_cluster.get_master_node(), bucket_name, p, run_id="fanout")

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 900))
        except Exception as e:
            self.fail("Fanout replication catch-up failed: {}".format(e))

        src_count = TenKCollectionHelper.get_bucket_item_count(
            src_cluster.get_master_node(), bucket_name)
        for cb_cluster in self.get_cb_clusters():
            if cb_cluster.get_name() == "C1":
                continue
            dest_count = TenKCollectionHelper.get_bucket_item_count(
                cb_cluster.get_master_node(), bucket_name)
            self.log.info("{}: {} items (src C1: {})".format(
                cb_cluster.get_name(), dest_count, src_count))
            self.assertEqual(src_count, dest_count,
                             "Item mismatch on {}: src={}, dest={}".format(
                                 cb_cluster.get_name(), src_count, dest_count))

        self.log.info("Fanout 1-to-N 10K collections test passed")

    def test_fanin_N_to_1_10k_collections(self):
        """
        Fanin: multiple sources (C2, C3, ...) replicating to C1 with
        10K collections. Manually sets up reverse-star topology since the
        base class star goes C1->others.
        Requires chain_length >= 3.
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        self._create_10k_on_all_clusters(bucket_name, p)

        dest_cluster = self.get_cb_cluster_by_name("C1")
        for cb_cluster in self.get_cb_clusters():
            if cb_cluster.get_name() == "C1":
                continue
            cb_cluster.add_remote_cluster(
                dest_cluster,
                Utility.get_rc_name(cb_cluster.get_name(), dest_cluster.get_name()),
                self._demand_encryption,
                self._replicator_role,
                self._multiple_ca,
                self._client_cert,
                self._client_key,
                self._systemeventlog
            )

        self.setup_all_replications()

        source_clusters = [c for c in self.get_cb_clusters()
                           if c.get_name() != "C1"]
        for cb_cluster in source_clusters:
            TenKCollectionHelper.select_and_load(
                cb_cluster.get_master_node(), bucket_name, p,
                run_id="fanin_{}".format(cb_cluster.get_name()))

        timeout = 900
        end_time = time.time() + timeout
        dest_count = 0
        total_src_items = 0
        while time.time() < end_time:
            dest_count = TenKCollectionHelper.get_bucket_item_count(
                dest_cluster.get_master_node(), bucket_name)
            total_src_items = sum(
                TenKCollectionHelper.get_bucket_item_count(
                    c.get_master_node(), bucket_name)
                for c in source_clusters)
            self.log.info("Fanin progress: dest={}, total_src={}".format(
                dest_count, total_src_items))
            if dest_count >= total_src_items and total_src_items > 0:
                break
            time.sleep(30)
        else:
            self.fail("Fanin replication did not converge in {}s: "
                      "dest={}, total_src={}".format(
                          timeout, dest_count, total_src_items))

        for cb_cluster in source_clusters:
            src_items = TenKCollectionHelper.get_bucket_item_count(
                cb_cluster.get_master_node(), bucket_name)
            self.log.info("{} source items: {}".format(cb_cluster.get_name(), src_items))

        self.log.info("C1 (destination) items: {}, total source items: {}".format(
            dest_count, total_src_items))
        self.assertGreaterEqual(dest_count, total_src_items,
                                "Destination should have at least as many items as "
                                "all sources combined: dest={}, sources={}".format(
                                    dest_count, total_src_items))

        self.log.info("Fanin N-to-1 10K collections test passed")

    def test_topology_chain_10k_collections(self):
        """
        Chain topology (C1 -> C2 -> C3) with 10K collections.
        Requires chain_length=3, ctopology=chain, rdirection=unidirection.
        Loads on C1 and verifies data flows through to C3.
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        self._create_10k_on_all_clusters(bucket_name, p)

        self.setup_xdcr()

        c1 = self.get_cb_cluster_by_name("C1")
        TenKCollectionHelper.select_and_load(
            c1.get_master_node(), bucket_name, p, run_id="chain")

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 900))
        except Exception as e:
            self.fail("Chain replication catch-up failed: {}".format(e))

        c1_count = TenKCollectionHelper.get_bucket_item_count(
            c1.get_master_node(), bucket_name)
        c3 = self.get_cb_cluster_by_name("C3")
        c3_count = TenKCollectionHelper.get_bucket_item_count(
            c3.get_master_node(), bucket_name)
        self.log.info("Chain: C1={} items, C3={} items".format(c1_count, c3_count))
        self.assertEqual(c1_count, c3_count,
                         "Chain end-to-end mismatch: C1={}, C3={}".format(
                             c1_count, c3_count))

        self.log.info("Chain topology 10K collections test passed")

    def test_topology_star_10k_collections(self):
        """
        Star topology (hub C1 <-> C2, C1 <-> C3) with 10K collections.
        Bidirectional between hub and all spokes.
        Requires chain_length=3, ctopology=star, rdirection=bidirection.
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        self._create_10k_on_all_clusters(bucket_name, p)

        self.setup_xdcr()

        c1 = self.get_cb_cluster_by_name("C1")
        TenKCollectionHelper.select_and_load(
            c1.get_master_node(), bucket_name, p, run_id="star_hub")

        for cb_cluster in self.get_cb_clusters():
            if cb_cluster.get_name() == "C1":
                continue
            TenKCollectionHelper.select_and_load(
                cb_cluster.get_master_node(), bucket_name, p,
                run_id="star_{}".format(cb_cluster.get_name()))

        # Bidirectional star: hub accumulates own + each spoke's own load;
        # each spoke accumulates own + hub's own load (no spoke<->spoke link).
        # Therefore hub_count > any single spoke_count (for N>=2 spokes), so
        # pairwise src==dest is impossible. Wait for each cluster's count to
        # stabilize, then check the correct invariant.
        for cb_cluster in self.get_cb_clusters():
            try:
                self._wait_for_dest_to_stabilize(cb_cluster, bucket_name,
                                                 min_items=1, timeout=900)
            except Exception as e:
                self.fail("Star: cluster {} did not stabilize: {}".format(
                    cb_cluster.get_name(), e))

        c1_count = TenKCollectionHelper.get_bucket_item_count(
            c1.get_master_node(), bucket_name)
        for cb_cluster in self.get_cb_clusters():
            if cb_cluster.get_name() == "C1":
                continue
            count = TenKCollectionHelper.get_bucket_item_count(
                cb_cluster.get_master_node(), bucket_name)
            self.log.info("{}: {} items (hub C1: {})".format(
                cb_cluster.get_name(), count, c1_count))
            self.assertGreater(count, 0,
                               "Spoke {} should have items".format(cb_cluster.get_name()))
            self.assertGreaterEqual(c1_count, count,
                                    "Star: hub should accumulate at least as many "
                                    "items as any spoke: hub={}, spoke {}={}".format(
                                        c1_count, cb_cluster.get_name(), count))

        self.log.info("Star topology 10K collections test passed")

    def tearDown(self):
        return XDCRNewBaseTest.tearDown(self)
