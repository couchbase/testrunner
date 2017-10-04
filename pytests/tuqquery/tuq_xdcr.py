import re
from tuqquery.tuq import QueryTests
from xdcr.upgradeXDCR import UpgradeTests
from xdcr.xdcrbasetests import XDCRReplicationBaseTest

class XDCRTests(QueryTests, XDCRReplicationBaseTest):
    def setUp(self):
        try:
            super(XDCRTests, self).setUp()
            self.method_name = self.input.param("test_to_run", "test_simple_check")
            self.with_reb = self.input.param("with_reb", None)
            self.bucket_topology = self.input.param("bucket_topology", "default:1><2").split(";")
            self.src_init = self.input.param('src_init', 1)
            self.dest_init = self.input.param('dest_init', 1)
            self.buckets_on_src = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if re.search('\S+:\S*1', bucket_repl)]
            self.buckets_on_dest = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if re.search('\S+:\S*2', bucket_repl)]
            self.repl_buckets_from_src = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if bucket_repl.find("1>") != -1 ]
            self.repl_buckets_from_dest = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if bucket_repl.find("<2") != -1 ]
            if self.repl_buckets_from_dest:
                self._replication_direction_str = "bidirection"
            else:
                self._replication_direction_str = "unidirection"
            self._override_clusters_structure()
        except:
            self.cluster.shutdown()

    def suite_setUp(self):
        super(XDCRTests, self).suite_setUp()

    def tearDown(self):
        try:
            XDCRReplicationBaseTest.tearDown(self)
        except:
                self.cluster.shutdown()
        if not self._testMethodName in ['suite_tearDown', 'suite_setUp']:
            return
        super(XDCRTests, self).tearDown()

    def suite_tearDown(self):
        super(XDCRTests, self).suite_tearDown()

    def test_xdcr_queries(self):
        XDCRReplicationBaseTest.setUp(self)
        self.load(self.gens_load)
        self._wait_for_replication_to_catchup()
        bucket = self._get_bucket('default', self.src_master)
        self.do_merge_bucket(self.src_master, self.dest_master, False, bucket)
        fn = getattr(self, self.method_name)
        fn()
        if self.with_reb == 'src':
            srv_in = self.servers[self.src_init + self.dest_init :
                                  self.src_init + self.dest_init + self.nodes_in]
            task = self.cluster.async_rebalance(self.servers[:self.src_init],
                               srv_in, [])
        elif self.with_reb == 'dest':
            srv_in = self.servers[self.src_init + self.dest_init :
                                  self.src_init + self.dest_init + self.nodes_in]
            task = self.cluster.async_rebalance(self.servers[self.src_init:
                                                             self.src_init + self.dest_init],
                               srv_in, [])
        else:
            self.sleep(self.wait_timeout, "Wait some time and try again")
        fn = getattr(self, self.method_name)
        fn()
        if self.with_reb:
            task.result()
