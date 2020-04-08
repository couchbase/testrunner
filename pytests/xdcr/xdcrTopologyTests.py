from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import OPS


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

    def tearDown(self):
        return XDCRNewBaseTest.tearDown(self)
