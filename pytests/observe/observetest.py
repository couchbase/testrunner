import time
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from couchbase_helper.documentgenerator import BlobGenerator
from datetime import datetime, timedelta
from couchbase_helper.document import View
from membase.helper.rebalance_helper import RebalanceHelper
from membase.helper.cluster_helper import  ClusterOperationHelper

class ObserveTests(BaseTestCase):
    def setUp(self):
        super(ObserveTests, self).setUp()
        # self.pre_warmup_stats = {}
        self.node_servers = []
        self.timeout = 120
        self.nodes_in = int(self.input.param("nodes_in", 1))
        self.observe_with = self.input.param("observe_with", "")
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.default_design_doc = "Doc1"
        map_func = 'function (doc) { emit(null, doc);}'
        self.default_view = View("default_view", map_func, None)
        self.access_log = self.input.param("access_log", False)
        self.servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
        self.mutate_by = self.input.param("mutate_by", "set")
        self.nodes_init = self.input.param("nodes_init", 2)
        self.without_access_log = self.input.param("without_access_log", False)
        try:
            self.log.info("Observe Rebalance Started")
            self.cluster.rebalance(self.servers[:1], self.servs_in, [])
        except Exception as e:
            pass
            #self.tearDown()
            #self.fail(e)

    def tearDown(self):
        super(ObserveTests, self).tearDown()

    def _load_doc_data_all_buckets(self, op_type='create', start=0, end=0, expiry=0):
        loaded = False
        count = 0
        gen_load = BlobGenerator('observe', 'observe', 1024, start=start, end=end)
        while not loaded and count < 60:
            try :
                self._load_all_buckets(self.servers[0], gen_load, op_type, expiry)
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

    def block_for_replication(self, key, cas=0, num=1, timeout=0, persist=False):
        """
        observe a key until it has been replicated to @param num of servers

        @param persist : block until item has been persisted to disk
        """
        vbucketid = self.client._get_vBucket_id(key)
        repl_servers = self._get_server_str(vbucketid, repl=True)
        persisted = 0
        self.log.info("VbucketId:%s on replicated servers:%s" % (vbucketid, repl_servers))

        while len(repl_servers) >= num > 0:
            for server in repl_servers:
                node = self._get_node(server)
                self.log.info("Replicated Server:- %s" % (server))
                newclient = MemcachedClientHelper.direct_client(node, self.default_bucket_name)
                t_start = datetime.now()
                while persisted == 0:
                    opaque, rep_time, persist_time, persisted, cas = newclient.observe(key)
                t_end = datetime.now()
                self.log.info("######key:-%s and Server:- %s#########" % (key, server))
                self.log.info("Persisted:- %s" % (persisted))
                self.log.info("Time taken to persist:- %s" % (t_end - t_start))
                num = num + 1
                if num == 0:
                    break
        return True

    def _get_server_str(self, vbucketid, repl=True):
        """retrieve server string {ip:port} based on vbucketid"""
        memcacheds, vBucketMap, vBucketMapReplica = self.client.request_map(self.client.rest, self.client.bucket)
        if repl:
            server = vBucketMapReplica[vbucketid]
        else:
            server = vBucketMap[vbucketid]
        return server

    def _get_node(self, server_ip_port):
        server_ip = server_ip_port.split(":")
        print(server_ip[0])
        for server in self.servers:
            print(server.ip)
            if server.ip == server_ip[0]:
                return server
            else:
                continue
        return None

    def _create_multi_set_batch(self):
        key_val = {}
        keys = ["observe%s" % (i) for i in range(self.num_items)]
        for key in keys:
            key_val[key] = "multiset"
        return key_val

    @staticmethod
    def _run_observe(self):
        tasks = []
        query_set = "true"
        persisted = 0
        mutated = False
        count = 0
        for bucket in self.buckets:
            self.cluster.create_view(self.master, self.default_design_doc,
                                      self.default_view, bucket, self.wait_timeout * 2)
            client = VBucketAwareMemcached(RestConnection(self.master), bucket)
            self.max_time = timedelta(microseconds=0)
            if self.mutate_by == "multi_set":
                key_val = self._create_multi_set_batch()
                client.setMulti(0, 0, key_val)
            keys = ["observe%s" % (i) for i in range(self.num_items)]
            for key in keys:
                mutated = False
                while not mutated and count < 60:
                    try:
                        if self.mutate_by == "set":
                            # client.memcached(key).set(key, 0, 0, "set")
                            client.set(key, 0, 0, "setvalue")
                        elif self.mutate_by == "append":
                            client.memcached(key).append(key, "append")
                        elif self.mutate_by == "prepend" :
                            client.memcached(key).prepend(key, "prepend")
                        elif self.mutate_by == "incr":
                            client.memcached(key).incr(key, 1)
                        elif self.mutate_by == "decr":
                            client.memcached(key).decr(key)
                        mutated = True
                        t_start = datetime.now()
                    except MemcachedError as error:
                        if error.status == 134:
                            loaded = False
                            self.log.error("Memcached error 134, wait for 5 seconds and then try again")
                            count += 1
                            time.sleep(5)
                while persisted == 0:
                    opaque, rep_time, persist_time, persisted, cas = client.observe(key)
                t_end = datetime.now()
                #self.log.info("##########key:-%s################" % (key))
                #self.log.info("Persisted:- %s" % (persisted))
                #self.log.info("Persist_Time:- %s" % (rep_time))
                #self.log.info("Time2:- %s" % (t_end - t_start))
                if self.max_time <= (t_end - t_start):
                    self.max_time = (t_end - t_start)
                    self.log.info("Max Time taken for observe is :- %s" % self.max_time)
                    self.log.info("Cas Value:- %s" % (cas))
            query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 600000}
            self.cluster.query_view(self.master, "dev_Doc1", self.default_view.name, query, self.num_items, bucket, timeout=self.wait_timeout)
            self.log.info("Observe Validation:- view: %s in design doc dev_Doc1 and in bucket %s" % (self.default_view, bucket))
            # check whether observe has to run with delete and delete parallel with observe or not
            if len (self.observe_with) > 0 :
                if self.observe_with == "delete" :
                    self.log.info("Deleting 0- %s number of items" % (self.num_items // 2))
                    self._load_doc_data_all_buckets('delete', 0, self.num_items // 2)
                    query_set = "true"
                elif self.observe_with == "delete_parallel":
                    self.log.info("Deleting Parallel 0- %s number of items" % (self.num_items // 2))
                    tasks = self._async_load_doc_data_all_buckets('delete', 0, self.num_items // 2)
                    query_set = "false"
                for key in keys:
                    opaque, rep_time, persist_time, persisted, cas = client.memcached(key).observe(key)
                    self.log.info("##########key:-%s################" % (key))
                    self.log.info("Persisted:- %s" % (persisted))
                if self.observe_with == "delete_parallel":
                    for task in tasks:
                        task.result()

                query = {"stale" : "false", "full_set" : query_set, "connection_timeout" : 600000}
                self.cluster.query_view(self.master, "dev_Doc1", self.default_view.name, query, self.num_items // 2, bucket, timeout=self.wait_timeout)
                self.log.info("Observe Validation:- view: %s in design doc dev_Doc1 and in bucket %s" % (self.default_view, self.default_bucket_name))

        """test_observe_basic_data_load_delete will test observer basic scenario
       i) Loading data and then run observe
       ii) deleting data then run observe
       iii) deleting data parallel with observe in parallel
       then verify the persistence by querying a view """
    def test_observe_basic_data_load_delete(self):
        self._load_doc_data_all_buckets('create', 0, self.num_items)
        # Persist all the loaded data item
        for bucket in self.buckets:
            RebalanceHelper.wait_for_persistence(self.master, bucket)
        rebalance = self.input.param("rebalance", "no")
        if rebalance == "in":
            self.servs_in = [self.servers[len(self.servers) - 1]]
            rebalance = self.cluster.async_rebalance(self.servers[:1], self.servs_in, [])
            self._run_observe(self)
            rebalance.result()
        elif rebalance == "out":
            self.servs_out = [self.servers[self.nodes_init - 1]]
            rebalance = self.cluster.async_rebalance(self.servers[:1], [], self.servs_out)
            self._run_observe(self)
            rebalance.result()
        else:
            self._run_observe(self)


    def test_observe_with_replication(self):
        self._load_doc_data_all_buckets('create', 0, self.num_items)
        if self.observe_with == "delete" :
            self.log.info("Deleting 0- %s number of items" % (self.num_items // 2))
            self._load_doc_data_all_buckets('delete', 0, self.num_items // 2)
            query_set = "true"
        elif self.observe_with == "delete_parallel":
            self.log.info("Deleting Parallel 0- %s number of items" % (self.num_items // 2))
            tasks = self._async_load_doc_data_all_buckets('delete', 0, self.num_items // 2)
            query_set = "false"
        keys = ["observe%s" % (i) for i in range(self.num_items)]
        self.key_count = 0
        self.max_time = 0
        self.client = VBucketAwareMemcached(RestConnection(self.master), self.default_bucket_name)
        for key in keys:
            self.key_count = self.key_count + 1
            self.block_for_replication(key, 0, 1)
        if self.observe_with == "delete_parallel":
            for task in tasks:
                task.result()

    def test_observe_with_warmup(self):
        self._load_doc_data_all_buckets('create', 0, self.num_items)
        # Persist all the loaded data item
        self.log.info("Nodes in cluster: %s" % self.servers[:self.nodes_init])
        for bucket in self.buckets:
            self.log.info('\n\nwaiting for persistence')
            RebalanceHelper.wait_for_persistence(self.master, bucket)
            self.log.info('\n\n_stats_befor_warmup')
            self._stats_befor_warmup(bucket.name)
            self.log.info('\n\n_restart_memcache')
            self._restart_memcache(bucket.name)
            # for bucket in self.buckets:
            self.log.info('\n\n_wait_warmup_completed')
            ClusterOperationHelper._wait_warmup_completed(self, self.servers[:self.nodes_init], bucket.name)
            self._run_observe(self)
