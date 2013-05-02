import time
from membase.api.rest_client import RestConnection, Bucket
from couchbase.documentgenerator import DocumentGenerator
from basetestcase import BaseTestCase
from memcached.helper.kvstore import KVStore
from mc_bin_client import MemcachedError
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.documentgenerator import BlobGenerator
from couchbase.stats_tools import StatsCommon
import string
import random


def key_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


class WarmUpTests(BaseTestCase):
    def setUp(self):
        super(WarmUpTests, self).setUp()
        self.pre_warmup_stats = {}
        self.timeout = 120
        self.access_log = self.input.param("access_log", False)
        self.servers_in = [self.servers[i + 1] for i in range(self.self.num_servers - 1)]
        self.cluster.rebalance(self.servers[:1], self.servers_in, [])
        self.active_resident_threshold = int(self.input.param("active_resident_threshold", 90))
        self.access_log_time = self.input.param("access_log_time", 2)

    def tearDown(self):
        super(WarmUpTests, self).tearDown()

    def _warmup_check(self):
        warmed_up = {}
        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()
            warmed_up[bucket.name] = {}
            for server in self.servers:
                warmed_up[bucket.name][server] = False

        for bucket in self.buckets:
            for server in self.servers:
                start = time.time()
                # Try to get the stats for 5 minutes, else hit out.
                if server == self.servers[0]:
                    wait_time = 300
                else:
                    wait_time = 60

                while time.time() - start < wait_time:
                # Get the wamrup time for each server
                    try:
                        warmup_time = int(stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_time')[server])
                        if warmup_time is not None:                            
                            self.log.info("ep_warmup_time is %s for %s in bucket %s" % (warmup_time, server.ip, bucket.name))
                            break
                        else:
                            self.log.info(" Did not get the stats from the server yet, trying again.....")
                            time.sleep(2)
                    except Exception as e:
                        self.log.error("Could not get warmup_time stats from server %s:%s, exception %s" % (server.ip, server.port, e))

                # Waiting for warm-up
                start = time.time()
                while time.time() - start < self.timeout and not warmed_up[bucket.name][server]:
                    if stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_thread')[server] == "complete":
                        self.log.info("warmup completed for %s in bucket %s" % (server.ip, bucket.name))
                        time.sleep(5)
                        if stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items_tot')[server] ==\
                           self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)]["curr_items_tot"]:
                            self._stats_report(server, bucket.name, stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup')[server])
                            warmed_up[bucket.name][server] = True

                    elif stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_thread')[server] == "running":
                        self.log.info("warming up is still running for %s in bucket %s....curr_items_tot : %s" %
                                      (server.ip, bucket.name, stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items_tot')[server]))

                    time.sleep(5)

        for bucket in self.buckets:
            for server in self.servers:
                if warmed_up[bucket.name][server] == True:
                    continue
                elif warmed_up[bucket.name][server] == False:
                    return False
        return True

    def _stats_report(self, server, bucket_name, after_warmup_stat):
        self.log.info("******** Stats before Warmup **********")
        for key, value in self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)].iteritems():
            self.log.info("%s on, %s:%s is %s for bucket %s" % (key, server.ip, server.port, value, bucket_name))

        self.log.info("******** Stats after Warmup **********")
        for key, value in after_warmup_stat.iteritems():
                self.log.info("%s on, %s:%s is %s" % (key, server.ip, server.port, value))

    def _wait_for_access_run(self, access_log_time, access_scanner_runs, server, bucket, bucket_stats):
        access_log_created = False
        new_scanner_run = int(bucket_stats.get_stats([server], bucket, '', 'ep_num_access_scanner_runs')[server])
        time.sleep(access_log_time * 60)
        self.log.info("new access scanner run is %s for %s in bucket %s" % (new_scanner_run, server.ip, bucket.name))
        count = 0
        while not access_log_created and count < 5:
            if new_scanner_run <= int(access_scanner_runs):
                count += 1
                time.sleep(5)
                new_scanner_run = int(bucket_stats.get_stats([server], bucket, '', 'ep_num_access_scanner_runs')[server])
            else:
                access_log_created = True

        return access_log_created

    def _load_dgm(self):
        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)

        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()
        
        for bucket in self.buckets:
            threshold_reached = False
            while not threshold_reached :
                for server in self.servers:
                    active_resident = stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'vb_active_perc_mem_resident')[server]
                    if int(active_resident) > self.active_resident_threshold:
                        self.log.info("resident ratio is %s greater than %s for %s in bucket %s. Continue loading to the cluster" %
                                      (active_resident, self.active_resident_threshold, server.ip, bucket.name))
                        random_key = key_generator()
                        generate_load = BlobGenerator(random_key, '%s-'%random_key, self.value_size, end=self.num_items)
                        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                    else:
                        threshold_reached = True
                        self.log.info("DGM state achieved for %s in bucket %s!" % (server.ip, bucket.name))
                        break

    def _update_access_log(self):
        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()

        for bucket in self.buckets:
            scanner_runs_all_servers = stats_all_buckets[bucket.name].get_stats(self.servers, bucket, '', 'ep_num_access_scanner_runs')
            for server in self.servers: 
                self.log.info("current access scanner run for %s in bucket %s is %s times" % (server.ip, bucket.name, scanner_runs_all_servers[server]))
                self.log.info("setting access scanner time %s minutes for %s in bucket %s" % (self.access_log_time, server.ip, bucket.name))
                ClusterOperationHelper.flushctl_set(server, "alog_sleep_time", self.access_log_time , bucket.fname)
                if not self._wait_for_access_run(self.access_log_time, scanner_runs_all_servers[server], server, bucket, stats_all_buckets[bucket.name]):
                    self.fail("Not able to create access log within %s minutes" % self.access_log_time)


    def warmup_with_access_log(self):

        self._load_dgm()

        # wait for draining of data before restart and warm up
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        for bucket in self.buckets:
            self._stats_befor_warmup(bucket.name)

        self._update_access_log()

        for bucket in self.buckets:
            self._restart_memcache(bucket.name)

        if self._warmup_check():
            generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, generate_load, "update", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        else:
            raise Exception("warmup test failed in some node")

        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_stats_all_buckets(self.servers[:self.num_servers])
