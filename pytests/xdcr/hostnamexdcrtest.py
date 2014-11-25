from xdcrbasetests import XDCRReplicationBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from hostname.hostnamemgmt_base import HostnameBaseTests


#Assumption that at least 2 nodes on every cluster
class HostnameXdcrTest(XDCRReplicationBaseTest, HostnameBaseTests):
    def setUp(self):
        super(HostnameXdcrTest, self).setUp()
        self.bidirection = self._input.param("bidirection", False)

    def tearDown(self):
        self._deinitialize_servers(self.src_nodes)
        self._deinitialize_servers(self.dest_nodes)
        super(HostnameXdcrTest, self).tearDown()

    def test_hostnames_xdcr(self):
        gen_load = BlobGenerator('hostname', 'hostname', 1024, end=self.num_items)
        self._load_all_buckets(self.src_master, gen_load, "create", 0)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=self.bidirection)
        self.verify_referenced_by_names(self.src_nodes, self.hostnames)
        self.verify_referenced_by_names(self.dest_nodes, self.hostnames)
        for i in xrange(len(self.src_nodes)):
            self.src_nodes[i].hostname = self.hostnames[self.src_nodes[i]]
        for i in xrange(len(self.dest_nodes)):
            self.dest_nodes[i].hostname = self.hostnames[self.dest_nodes[i]]
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes)

    def test_replication_with_view_queries(self):
        self.verify_referenced_by_names(self.src_nodes, self.hostnames)
        self.verify_referenced_by_names(self.dest_nodes, self.hostnames)
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)
        src_buckets = self._get_cluster_buckets(self.src_master)
        dest_buckets = self._get_cluster_buckets(self.dest_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        for bucket in dest_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]
        query = {"full_set" : "true", "stale" : "false"}

        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)
        tasks = []
        for view in views:
            tasks.append(self.cluster.async_query_view(self.src_master, prefix + ddoc_name, view.name, query, self.num_items))
            tasks.append(self.cluster.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query, self.num_items))
        for task in tasks:
            task.result(self._poll_timeout)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes)
        self.verify_referenced_by_names(self.src_nodes, self.hostnames)
        self.verify_referenced_by_names(self.dest_nodes, self.hostnames)