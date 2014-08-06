from xdcrbasetests import XDCRReplicationBaseTest
from couchbase.documentgenerator import BlobGenerator
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
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, False)