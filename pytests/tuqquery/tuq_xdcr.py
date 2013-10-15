import re
from tuqquery.tuq import QueryTests
from xdcr.upgradeXDCR import UpgradeTests
from xdcr.xdcrbasetests import XDCRReplicationBaseTest

class XDCRTests(QueryTests, XDCRReplicationBaseTest):
    def setUp(self):
        super(XDCRTests, self).setUp()
        self.method_name = self.input.param("test_to_run", "test_simple_check")
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

    def suite_setUp(self):
        super(XDCRTests, self).suite_setUp()

    def tearDown(self):
        if not self._testMethodName in ['suite_tearDown',
                                        'suite_setUp']:
            try:
                self._case_number = self.case_number
                XDCRReplicationBaseTest.tearDown(self)
                return
            finally:
                self.cluster.shutdown()
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
        self.sleep(self.wait_timeout, "Wait some time and try again")
        fn = getattr(self, self.method_name)
        fn()

    def _override_clusters_structure(self):
        UpgradeTests._override_clusters_structure(self)

    def _create_buckets(self, nodes):
        UpgradeTests._create_buckets(self, nodes)

    def _setup_topology_chain(self):
        UpgradeTests._setup_topology_chain(self)

    def _set_toplogy_star(self):
        UpgradeTests._set_toplogy_star(self)

    def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        UpgradeTests._join_clusters(self, src_cluster_name, src_master,
                                    dest_cluster_name, dest_master)

    def _replicate_clusters(self, src_master, dest_cluster_name, buckets):
        UpgradeTests._replicate_clusters(self, src_master, dest_cluster_name, buckets)

    def _get_bucket(self, bucket_name, server):
        return UpgradeTests._get_bucket(self, bucket_name, server)