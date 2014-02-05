from xdcrbasetests import XDCRReplicationBaseTest
from couchbase.documentgenerator import BlobGenerator
from couchbase.document import View
from observe.observetest import ObserveTests


#Assumption that at least 2 nodes on every cluster
class ObserveXdcrTest(XDCRReplicationBaseTest):
    def setUp(self):
        super(ObserveXdcrTest, self).setUp()
        self.bidirection = self._input.param("bidirection", False)

        #Variables defined for _run_observe() in observetest.
        self.observe_with = self._input.param("observe_with", "")
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.default_design_doc = "Doc1"
        map_func = 'function (doc) { emit(null, doc);}'
        self.default_view = View("default_view", map_func, None)
        self.mutate_by = self._input.param("mutate_by", "set")
        self.wait_timeout = self._timeout

    def tearDown(self):
        super(ObserveXdcrTest, self).tearDown()

    def observe_xdcr(self):
        gen_load = BlobGenerator('observe', 'observe', 1024, end=self.num_items)
        self._load_all_buckets(self.src_master, gen_load, "create", 0)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=self.bidirection)
        self.verify_results()
        self.master = self.src_master
        ObserveTests._run_observe(self)
        self.master = self.dest_master
        ObserveTests._run_observe(self)
