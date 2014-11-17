from xdcrnewbasetests import XDCRNewBaseTest
from xdcrnewbasetests import OPS


class XDCRTopologyTest(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.assert_(
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

    def __get_doc_ops_clusters(self):
        doc_ops_clusters = self._input.param("doc_ops_cluster", None)
        if doc_ops_clusters:
            doc_ops_clusters = doc_ops_clusters.split(":")
            return doc_ops_clusters
        return []

    def __get_doc_ops(self):
        doc_ops = self._input.param("doc_ops", None)
        if doc_ops:
            return doc_ops.split('-')
        return []

    def __perform_update_delete(self):
        percent_update = self._input.param("upd", 30)
        percent_delete = self._input.param("del", 30)
        expires = self._input.param("expires", 0)
        wait_for_expiration = self._input.param("wait_for_expiration", True)
        for doc_ops_cluster in self.__get_doc_ops_clusters():
            cb_cluster = self.get_cb_cluster_from_name(doc_ops_cluster)
            for doc_op in self.__get_doc_ops():
                if doc_op == OPS.UPDATE:
                    cb_cluster.update_delete_data(
                        doc_op,
                        perc=percent_update,
                        expiration=expires,
                        wait_for_expiration=wait_for_expiration
                    )
                elif doc_op == OPS.DELETE:
                    cb_cluster.update_delete_data(doc_op, perc=percent_delete)

    def __setup_xdcr_and_load(self):
        self.set_xdcr_topology()
        self.setup_all_replications()
        self.load_data_topology()

    def load_with_ops(self):
        self.__setup_xdcr_and_load()

        self.__perform_update_delete()

        self.verify_results()

    # TODO - add update/deletes after rebalance operation
    def load_with_rebalance_out(self):
        num_rebalance = self._input.param("num_rebalance", 1)

        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.rebalance_out(num_rebalance)

        self.__perform_update_delete()

        self.verify_results()

    def load_with_rebalance_out_master(self):
        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.rebalance_out_master()

        self.__perform_update_delete()

        self.verify_results()

    def load_with_rebalance_in(self):
        num_rebalance = self._input.param("num_rebalance", 1)
        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.rebalance_in(num_rebalance)

        self.__perform_update_delete()

        self.verify_results()

    def load_with_swap_rebalance(self):
        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.swap_rebalance()

        self.__perform_update_delete()

        self.verify_results()

    def load_with_swap_rebalance_master(self):
        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.swap_rebalance_master()

        self.__perform_update_delete()

        self.verify_results()

    def load_with_failover(self):
        num_rebalance = self._input.param("num_rebalance", 1)
        graceful = self._input.param("graceful", False)
        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.failover_nodes(
                num_nodes=num_rebalance,
                graceful=graceful)

        self.__perform_update_delete()

        self.verify_results()

    def load_with_failover_master(self):
        graceful = self._input.param("graceful", False)
        self.__setup_xdcr_and_load()

        for rebalance_cluster in self.__get_rebalance_clusters():
            cb_cluster = self.get_cb_cluster_from_name(rebalance_cluster)
            cb_cluster.failover_master(graceful=graceful)

        self.__perform_update_delete()

        self.verify_results()

    def tearDown(self):
        return XDCRNewBaseTest.tearDown(self)
