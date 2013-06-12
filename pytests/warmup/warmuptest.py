import time
from membase.api.rest_client import RestConnection, Bucket
from couchbase.documentgenerator import DocumentGenerator
from basetestcase import BaseTestCase
from memcached.helper.kvstore import KVStore
from mc_bin_client import MemcachedError
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.documentgenerator import BlobGenerator
from couchbase.stats_tools import StatsCommon
from remote.remote_util import RemoteMachineShellConnection
import string
import random
import testconstants

def key_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


class WarmUpTests(BaseTestCase):
    def setUp(self):
        super(WarmUpTests, self).setUp()
        self.pre_warmup_stats = {}
        self.timeout = 120
        self.without_access_log = self.input.param("without_access_log", False)
        self.servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
        self.cluster.rebalance(self.servers[:1], self.servers_in, [])
        self.active_resident_threshold = int(self.input.param("active_resident_threshold", 90))
        self.access_log_time = self.input.param("access_log_time", 2)
        self.expire_time = self.input.param("expire_time", 30)
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.load_gen_list = []

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
                warmup_complete = False

                while not warmup_complete:
                    try:
                        if stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_thread')[server] == "complete":
                            self.log.info("warmup completed for %s in bucket %s" % (server.ip, bucket.name))
                            warmup_complete = True
                        elif stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_thread')[server] == "running":
                            self.log.info("warming up is still running for %s in bucket %s....curr_items_tot : %s" %
                                          (server.ip, bucket.name, stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items_tot')[server]))

                        warmup_time = int(stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_time')[server])
                        if warmup_time is not None:
                            self.log.info("ep_warmup_time is %s for %s in bucket %s" % (warmup_time, server.ip, bucket.name))
                    except Exception as e:
                        self.log.error("Could not get warmup_time stats from server %s:%s, exception %s" % (server.ip, server.port, e))

                start = time.time()
                while time.time() - start < self.timeout and not warmed_up[bucket.name][server]:
                    if stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items_tot')[server] == \
                        self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)]["curr_items_tot"]:
                        if stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items')[server] == \
                           self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)]["curr_items"]:
                            if self._warmup_check_without_access_log():
                                warmed_up[bucket.name][server] = True
                                self._stats_report(server, bucket, stats_all_buckets[bucket.name])
                        else:
                            self.log.info("curr_items is %s not equal to %s" % (stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items')[server],
                                                                                    self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)]["curr_items"]))
                    else:
                        self.log.info("curr_items_tot is %s not equal to %s" % (stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items_tot')[server],
                                                                            self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)]["curr_items_tot"]))

                    time.sleep(10)

        for bucket in self.buckets:
            for server in self.servers:
                if warmed_up[bucket.name][server] == True:
                    continue
                elif warmed_up[bucket.name][server] == False:
                    return False
        return True


    def _warmup_check_without_access_log(self):
        if not self.without_access_log:
            return True

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
                while time.time() - start < self.timeout and not warmed_up[bucket.name][server]:
                    if stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_key_count')[server] >= \
                       stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_min_item_threshold')[server] or \
                       stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'mem_used')[server] >= \
                       stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_min_memory_threshold')[server] or \
                       stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'mem_used')[server] >= \
                       stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'ep_mem_low_wat')[server]:
                        warmed_up[bucket.name][server] = True
                    else:
                        self.log.info("curr_items is %s and ep_warmup_min_item_threshold is %s" %
                                      (stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'curr_items')[server],
                                       stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_min_item_threshold')[server]))
                        self.log.info("vb_active_perc_mem_resident is %s and ep_warmup_min_memory_threshold is %s" %
                                      (stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'vb_active_perc_mem_resident')[server],
                                       stats_all_buckets[bucket.name].get_stats([server], bucket, 'warmup', 'ep_warmup_min_memory_threshold')[server]))
                        self.log.info("mem_used is %s and ep_mem_low_wat is %s" %
                                      (stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'mem_used')[server],
                                       stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'ep_mem_low_wat')[server]))

                    time.sleep(10)

        for bucket in self.buckets:
            for server in self.servers:
                if warmed_up[bucket.name][server] == True:
                    continue
                elif warmed_up[bucket.name][server] == False:
                    return False
        return True


    def _stats_report(self, server, bucket, after_warmup_stats):
        self.log.info("******** Stats before Warmup **********")
        for key, value in self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)].iteritems():
            self.log.info("%s on %s:%s is %s for bucket %s" % (key, server.ip, server.port, value, bucket.name))

        self.log.info("******** Stats after Warmup **********")
        self.log.info("uptime on, %s:%s is %s for bucket %s" % (server.ip, server.port, after_warmup_stats.get_stats([server], bucket, '', 'uptime')[server], bucket.name))
        self.log.info("curr_items on, %s:%s is %s for bucket %s" % (server.ip, server.port, after_warmup_stats.get_stats([server], bucket, '', 'curr_items')[server], bucket.name))
        self.log.info("curr_items_tot, %s:%s is %s for bucket %s" % (server.ip, server.port, after_warmup_stats.get_stats([server], bucket, '', 'curr_items_tot')[server], bucket.name))
        for key in self.stats_monitor:
            self.log.info("%s on, %s:%s is %s for bucket %s" % (key, server.ip, server.port, after_warmup_stats.get_stats([server], bucket, '', key)[server], bucket.name))
        if self.without_access_log:
            for key in self.warmup_stats_monitor:
                self.log.info("%s on, %s:%s is %s for bucket %s" % (key, server.ip, server.port, after_warmup_stats.get_stats([server], bucket, 'warmup', key)[server], bucket.name))

    def _wait_for_access_run(self, access_log_time, access_scanner_runs, server, bucket, bucket_stats):
        access_log_created = False
        time.sleep(access_log_time * 60)
        new_scanner_run = int(bucket_stats.get_stats([server], bucket, '', 'ep_num_access_scanner_runs')[server])
        count = 0
        while not access_log_created and count < 5:
            self.log.info("new access scanner run is %s times for %s in bucket %s" % (new_scanner_run, server.ip, bucket.name))
            if int(new_scanner_run) <= int(access_scanner_runs):
                count += 1
                time.sleep(5)
                new_scanner_run = int(bucket_stats.get_stats([server], bucket, '', 'ep_num_access_scanner_runs')[server])
            else:
                access_log_created = True

        return access_log_created

    def _load_dgm(self):
        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        self.load_gen_list.append(generate_load)

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
                        generate_load = BlobGenerator(random_key, '%s-' % random_key, self.value_size, end=self.num_items)
                        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                        self.load_gen_list.append(generate_load)
                    else:
                        threshold_reached = True
                        self.log.info("DGM state achieved for %s in bucket %s!" % (server.ip, bucket.name))
                        break


        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                for gen in self.load_gen_list[:int(len(self.load_gen_list) * 0.5)]:
                    self._load_all_buckets(self.master, gen, "update", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                for gen in self.load_gen_list[int(len(self.load_gen_list) * 0.5):]:
                    self._load_all_buckets(self.master, gen, "delete", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("expire" in self.doc_ops):
                for gen in self.load_gen_list[:int(len(self.load_gen_list) * 0.8)]:
                    self._load_all_buckets(self.master, gen, "update", self.expire_time, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                time.sleep(self.expire_time * 2)

                for server in self.servers:
                    shell = RemoteMachineShellConnection(server)
                    for bucket in self.buckets:
                        shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
                    shell.disconnect()
                time.sleep(30)


    def _create_access_log(self):
        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()

        for bucket in self.buckets:
            for server in self.servers:
                scanner_runs = stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'ep_num_access_scanner_runs')[server]
                self.log.info("current access scanner run for %s in bucket %s is %s times" % (server.ip, bucket.name, scanner_runs))
                self.log.info("setting access scanner time %s minutes for %s in bucket %s" % (self.access_log_time, server.ip, bucket.name))
                ClusterOperationHelper.flushctl_set(server, "alog_sleep_time", self.access_log_time , bucket.name)
                if not self._wait_for_access_run(self.access_log_time, scanner_runs, server, bucket, stats_all_buckets[bucket.name]):
                    self.fail("Not able to create access log within %s minutes" % self.access_log_time)

    def _delete_access_log(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            info = shell.extract_remote_info()
            type = info.type.lower()
            path = testconstants.COUCHBASE_DATA_PATH
            if type == 'windows':
                path = testconstants.WIN_COUCHBASE_DATA_PATH

            for bucket in self.buckets:
                command = "cd %s%s & remove *access*" % (path, bucket.name)
                output, error = shell.execute_command(command.format(command))
                shell.log_command_output(output, error)

            shell.disconnect()


    def warmup_test(self):
        self._load_dgm()

        # wait for draining of data before restart and warmup
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

        for bucket in self.buckets:
            self._stats_befor_warmup(bucket.name)

        if self.without_access_log:
            self._delete_access_log()
        else:
            self._create_access_log()

        for bucket in self.buckets:
            self._restart_memcache(bucket.name)

        if self._warmup_check():
            generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
            if self.doc_ops is not None:
                if "delete" in self.doc_ops or "expire" in self.doc_ops:
                    self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                else:
                    self._load_all_buckets(self.master, generate_load, "update", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            else:
                self._load_all_buckets(self.master, generate_load, "update", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        else:
            raise Exception("Warmup check failed. Warmup test failed in some node")

        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_stats_all_buckets(self.servers[:self.num_servers])


    def test_restart_node_with_full_disk(self):
        def _get_disk_usage_percentage(remote_client):
            disk_info = remote_client.get_disk_info()
            percentage = disk_info[1] + disk_info[2];
            for item in percentage.split():
                if "%" in item:
                    self.log.info("disk usage {0}".format(item))
                    return item[:-1]

        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_command_raw("rm -rf full_disk*", use_channel=True)
        remote_client.log_command_output(output, error)
        percentage = _get_disk_usage_percentage(remote_client)
        try:
            while int(percentage) < 95:
                output, error = remote_client.execute_command("dd if=/dev/zero of=full_disk{0} bs=3G count=1".format(percentage + str(time.time())), use_channel=True)
                remote_client.log_command_output(output, error)
                percentage = _get_disk_usage_percentage(remote_client)
            processes1 = remote_client.get_running_processes()
            output, error = remote_client.execute_command("/etc/init.d/couchbase-server restart", use_channel=True)
            remote_client.log_command_output(output, error)
        finally:
            output, error = remote_client.execute_command_raw("rm -rf full_disk*", use_channel=True)
            remote_client.log_command_output(output, error)
            remote_client.disconnect()
