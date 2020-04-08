import time
from membase.api.rest_client import RestConnection, Bucket
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from couchbase_helper.documentgenerator import BlobGenerator
from threading import Thread

class StatsCrashRepro(BaseTestCase):
    def setUp(self):
        super(StatsRepro, self).setUp()
        self.timeout = 120
        self.bucket_name = self.input.param("bucket", "default")
        self.bucket_size = self.input.param("bucket_size", 100)
        self.data_size = self.input.param("data_size", 2048)
        self.threads_to_run = self.input.param("threads_to_run", 5)
#        self.nodes_in = int(self.input.param("nodes_in", 1))
#        self.servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
#        rebalance = self.cluster.async_rebalance(self.servers[:1], self.servs_in, [])
#        rebalance.result()
        bucket_params=self._create_bucket_params(server=self.servers[0], size=self.bucket_size, replicas=self.num_replicas)
        self.cluster.create_default_bucket(bucket_params)

        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
             num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        rest = RestConnection(self.servers[0])
        self.nodes_server = rest.get_nodes()


    def tearDown(self):
        super(StatsRepro, self).tearDown()

    def _load_doc_data_all_buckets(self, op_type='create', start=0, expiry=0):
        loaded = False
        count = 0
        gen_load = BlobGenerator('warmup', 'warmup-', self.data_size, start=start, end=self.num_items)
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

    def _get_stats(self, stat_str='all'):

#        for server in self.nodes_server:
            server = self.servers[0]
            mc_conn = MemcachedClientHelper.direct_client(server, self.bucket_name, self.timeout)
            stat_result = mc_conn.stats(stat_str)
#            self.log.info("Getting stats {0} : {1}".format(stat_str, stat_result))
            self.log.info("Getting stats {0}".format(stat_str))
            mc_conn.close()


    def _run_get(self):
        server = self.servers[0]
        mc_conn = MemcachedClientHelper.direct_client(server, self.bucket_name, self.timeout)
        for i in range(self.num_items):
            key = "warmup{0}".format(i)
            mc_conn.get(key)


    def run_test(self):
        ep_threshold = self.input.param("ep_threshold", "ep_mem_low_wat")
        active_resident_threshold = int(self.input.param("active_resident_threshold", 10))

        mc = MemcachedClientHelper.direct_client(self.servers[0], self.bucket_name)
        stats = mc.stats()
        threshold = int(self.input.param('threshold', stats[ep_threshold]))
        threshold_reached = False
        self.num_items = self.input.param("items", 10000)
        self._load_doc_data_all_buckets('create')

        # load items till reached threshold or mem-ratio is less than resident ratio threshold
        while not threshold_reached :
            mem_used = int(mc.stats()["mem_used"])
            if mem_used < threshold or int(mc.stats()["vb_active_perc_mem_resident"]) >= active_resident_threshold:
                self.log.info("mem_used and vb_active_perc_mem_resident_ratio reached at %s/%s and %s " % (mem_used, threshold, mc.stats()["vb_active_perc_mem_resident"]))
                items = self.num_items
                self.num_items += self.input.param("items", 10000)
                self._load_doc_data_all_buckets('create', items)
            else:
                threshold_reached = True
                self.log.info("DGM state achieved!!!!")

        # wait for draining of data before restart and warm up
        for bucket in self.buckets:
            RebalanceHelper.wait_for_persistence(self.nodes_server[0], bucket, bucket_type=self.bucket_type)


        while True:

#            read_data_task = self.cluster.async_verify_data(self.master, self.buckets[0], self.buckets[0].kvs[1])

            read_data_task = Thread(target=self._run_get)
            read_data_task.start()
            #5 threads to run stats all and reset asynchronously
            start = time.time()
            while (time.time() - start) < 300:

                stats_all_thread = []
                stats_reset_thread = []

                for i in range(self.threads_to_run):
                    stat_str = ''
                    stats_all_thread.append(Thread(target=self._get_stats, args=[stat_str]))
                    stats_all_thread[i].start()
                    stat_str = 'reset'
                    stats_reset_thread.append(Thread(target=self._get_stats, args=[stat_str]))
                    stats_reset_thread[i].start()


                for i in range(self.threads_to_run):
                    stats_all_thread[i].join()
                    stats_reset_thread[i].join()

                del stats_all_thread
                del stats_reset_thread

#            read_data_task.result()
            read_data_task.join()