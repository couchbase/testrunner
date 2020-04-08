from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import Utility
from .xdcrnewbasetests import BUCKET_NAME
from hostname.hostnamemgmt_base import HostnameBaseTests


# Assumption that at least 2 nodes on every cluster
class HostnameXdcrTest(XDCRNewBaseTest, HostnameBaseTests):
    def setUp(self):
        super(HostnameXdcrTest, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_nodes = self.src_cluster.get_nodes()
        self.dest_nodes = self.dest_cluster.get_nodes()

    def tearDown(self):
        self._deinitialize_servers(self.src_nodes)
        self._deinitialize_servers(self.dest_nodes)
        super(HostnameXdcrTest, self).tearDown()

    def test_hostnames_xdcr(self):
        self.setup_xdcr_and_load()
        self.verify_referenced_by_names(self.src_nodes, self.src_cluster.get_host_names())
        self.verify_referenced_by_names(self.dest_nodes, self.dest_cluster.get_host_names())
        self.verify_results()

    def test_replication_with_view_queries(self):
        self.verify_referenced_by_names(self.src_nodes, self.src_cluster.get_host_names())
        self.verify_referenced_by_names(self.dest_nodes, self.dest_cluster.get_host_names())

        self.setup_xdcr_and_load()

        num_views = self._input.param("num_views", 5)
        is_dev_ddoc = self._input.param("is_dev_ddoc", True)
        src_buckets = self.src_cluster.get_buckets()
        dest_buckets = self.dest_cluster.get_buckets()
        for bucket in src_buckets:
            views = Utility.make_default_views(bucket.name, num_views, is_dev_ddoc)
        for bucket in dest_buckets:
            views = Utility.make_default_views(bucket.name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        query = {"full_set" : "true", "stale" : "false"}

        tasks = self.src_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
        tasks += self.dest_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
        for task in tasks:
            task.result(self._poll_timeout)

        self.merge_all_buckets()
        tasks = []
        for view in views:
            tasks.append(self.src_cluster.async_query_view(prefix + ddoc_name, view.name, query, src_buckets[0].kvs[1].__len__()))
            tasks.append(self.dest_cluster.async_query_view(prefix + ddoc_name, view.name, query, dest_buckets[0].kvs[1].__len__()))
        for task in tasks:
            task.result(self._poll_timeout)

        self.verify_results()
        self.verify_referenced_by_names(self.src_nodes, self.src_cluster.get_host_names())
        self.verify_referenced_by_names(self.dest_nodes, self.dest_cluster.get_host_names())