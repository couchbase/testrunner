import time
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from view.view_base import ViewBaseTest
from mc_bin_client import MemcachedError
from couchbase.documentgenerator import BlobGenerator

class ObserveTests(ViewBaseTest):
    def setUp(self):
        super(ObserveTests, self).setUp()
        self.pre_warmup_stats = {}
        self.timeout = 120
        self.nodes_in = int(self.input.param("nodes_in", 1))
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.default_design_doc = "Doc1"
        self.servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
        self.log.info("Observe Rebalance Started")
        rebalance = self.cluster.async_rebalance(self.servers[:1], self.servs_in, [])
        rebalance.result()

    def tearDown(self):
        super(ObserveTests, self).tearDown()

    def _load_doc_data_all_buckets(self, op_type='create', start=0, end=0):
        loaded = False
        count = 0
        gen_load = BlobGenerator('observe', 'observe', 1024, start=start, end=end)
        while not loaded and count < 60:
            try :
                self._load_all_buckets(self.servers[0], gen_load, op_type, 0)
                loaded = True
            except MemcachedError as error:
                if error.status == 134:
                    loaded = False
                    self.log.error("Memcached error 134, wait for 5 seconds and then try again")
                    count += 1
                    time.sleep(5)

    def _async_load_doc_data_all_buckets(self, op_type='create', start=0, end=0):
        gen_load = BlobGenerator('observe', 'observe', 1024, start=start, end=end)
        tasks = self._async_load_all_buckets(self.servers[0], gen_load, op_type, 0)
        return tasks

    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', '==', 0))
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_flusher_todo', '==', 0))
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_uncommitted_items', '==', 0))
        for task in tasks:
            task.result()

    """test_observe_basic_data_load_delete will test observer basic scenario
       i) Loading data and then run observe
       ii) deleting data then run observe
       iii) deleting data parallel with observe in parallel
       then verify the persistence by querying a view """
    def test_observe_basic_data_load_delete(self):
        tasks = []
        self._load_doc_data_all_buckets('create', 0, self.num_items)
        rest = RestConnection(self.servers[0])
        self.servers = rest.get_nodes()
        #Persist all the loaded data item
        self._wait_for_stats_all_buckets(self.servers)
        self.cluster.create_view(
                    self.master, self.default_design_doc, self.default_view,
                    self.default_bucket_name, self.wait_timeout * 2)
        client = MemcachedClientHelper.direct_client(self.servers[0], self.default_bucket_name)
        observe_with = self.input.param("observe_with", "")
        # check whether observe has to run with delete and delete parallel with observe or not
        if len (observe_with) > 0 :
            if observe_with == "Delete" :
                self.log.info("Deleting 0- %s number of items" % (self.num_items / 2))
                self._load_doc_data_all_buckets('delete', 0, self.num_items / 2)
                query_set = "true"
            elif observe_with == "Delete_Parallel":
                self.log.info("Deleting Parallel 0- %s number of items" % (self.num_items / 2))
                tasks = self._async_load_doc_data_all_buckets('delete', 0, self.num_items / 2)
                query_set = "false"

        keys = ["observe%s" % (i) for i in range(0, self.num_items)]
        for key in keys:
            opaque, rep_time, persist_time, persisted = client.observe(key)
            self.log.info("##########key:-%s################" % (key))
            self.log.info("Persisted:- %s" % (persisted))
            self.log.info("Persist_Time:- %s" % (rep_time))

        for task in tasks:
            task.result()
        #verify the persistence of data by querying view
        query = {"stale" : "false", "full_set" : query_set, "connection_timeout" : 60000}
        self.cluster.query_view(self.master, "dev_Doc1", self.default_view.name, query, 10000, self.default_bucket_name)
        self.log.info("Observe Validation:- view: %s in design doc dev_Doc1 and in bucket %s" % (self.default_view, self.default_bucket_name))
