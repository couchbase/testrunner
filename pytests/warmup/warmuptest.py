import time
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.stats_tools import StatsCommon
from remote.remote_util import RemoteMachineShellConnection
import testconstants

class WarmUpTests(BaseTestCase):
    def setUp(self):
        super(WarmUpTests, self).setUp()
        self.pre_warmup_stats = {}
        self.timeout = 120
        self.without_access_log = self.input.param("without_access_log", False)
        self.access_log_time = self.input.param("access_log_time", 2)
        self.expire_time = self.input.param("expire_time", 30)
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        generate_load = BlobGenerator('nosqlini', 'nosqlini-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.servers[0], generate_load, "create", 0, batch_size=2000)
        # reinitialize active_resident_threshold to avoid further data loading as dgm
        self.active_resident_threshold = 100

    def tearDown(self):
        super(WarmUpTests, self).tearDown()

    def _warmup_check(self, timeout=1800):
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
                end_time = start + timeout
                warmup_complete = False

                while not warmup_complete and time.time() < end_time:
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
                        self.sleep(5, "waiting for warmup...")
                    except Exception as e:
                        self.log.error("Could not get warmup_time stats from server %s:%s, exception %s" % (server.ip, server.port, e))

                self.assertTrue(warmup_complete, "Warm up wasn't complete in %s sec" % timeout)

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

                    self.sleep(10)

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

                    self.sleep(5)

        for bucket in self.buckets:
            for server in self.servers:
                if warmed_up[bucket.name][server] == True:
                    continue
                elif warmed_up[bucket.name][server] == False:
                    return False
        return True


    def _stats_report(self, server, bucket, after_warmup_stats):
        self.log.info("******** Stats before Warmup **********")
        for key, value in self.pre_warmup_stats[bucket.name]["%s:%s" % (server.ip, server.port)].items():
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
        self.sleep(access_log_time * 60)
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

    def _additional_ops(self):
        generate_update = BlobGenerator('nosql', 'nosql-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, generate_update, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        generate_delete = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items, end=self.num_items * 2)
        self._load_all_buckets(self.master, generate_delete, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        generate_expire = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items * 2, end=self.num_items * 3)
        self._load_all_buckets(self.master, generate_expire, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, generate_update, "update", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, generate_delete, "delete", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("expire" in self.doc_ops):
                self._load_all_buckets(self.master, generate_expire, "update", self.expire_time, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                self.sleep(self.expire_time + 10)

                self.expire_pager(self.servers[:self.num_servers])
                self.sleep(30)


    def _create_access_log(self):
        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()

        for bucket in self.buckets:
            for server in self.servers:
                scanner_runs = stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'ep_num_access_scanner_runs')[server]
                self.log.info("current access scanner run for %s in bucket %s is %s times" % (server.ip, bucket.name, scanner_runs))
                self.log.info("setting access scanner time %s minutes for %s in bucket %s" % (self.access_log_time, server.ip, bucket.name))
                ClusterOperationHelper.flushctl_set(server, "alog_sleep_time", self.access_log_time, bucket.name)
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
                command = "cd %s%s & rm -rf *access*" % (path, bucket.name)
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)

            shell.disconnect()


    def warmup_test(self):
        self._additional_ops()

        # wait for draining of data before restart and warmup
        self.expire_pager(self.servers[:self.num_servers])
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
                for ops in self.doc_ops:
                    if "delete" in self.doc_ops or "expire" in self.doc_ops:
                        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=5000, pause_secs=5, timeout_secs=120)
                    if ops == 'expire':
                        self._load_all_buckets(self.master, generate_load, "update", self.expire_time, 1, 0, True, batch_size=5000, pause_secs=5, timeout_secs=120)
                        self.sleep(self.expire_time + 1, 'wait for expiration')
                    else:
                        self._load_all_buckets(self.master, generate_load, ops, 0, 1, 0, True, batch_size=5000, pause_secs=5, timeout_secs=120)
            else:
                self._load_all_buckets(self.master, generate_load, "update", 0, 1, 0, True, batch_size=5000, pause_secs=5, timeout_secs=120)
            self.expire_pager(self.servers[:self.num_servers])
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

    def test_warm_up_progress(self):
        self.expire_pager(self.servers[:self.num_servers])
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_stats_all_buckets(self.servers[:self.num_servers])
        for bucket in self.buckets:
            self._stats_befor_warmup(bucket.name)
            self._restart_memcache(bucket.name)
        end_time = time.time() + self.wait_timeout
        rest = RestConnection(self.master)
        old_stats = rest.get_warming_up_tasks()
        while old_stats and time.time() < end_time:
            new_stats = rest.get_warming_up_tasks()
            self._check_warm_up_progress_stats(old_stats, new_stats)
            old_stats = new_stats

    def _check_warm_up_progress_stats(self, old_stats, stats):
        self.log.info("new stat is %s" % stats)
        self.log.info("old stat is %s" % old_stats)
        for task in stats:
            task_candidates = [o_task for o_task in old_stats
                        if task["bucket"] == o_task["bucket"] and task["node"] == o_task["node"]]
            if len(task_candidates) == 0:
                self.log.info('no task candidates %s' % task)
                continue
            old_task = task_candidates[0]

            self.assertEqual(task["status"], 'running', "Status is not expected")
            #https://github.com/membase/ep-engine/blob/master/docs/stats.json#L69
            self.assertTrue(task["stats"]["ep_warmup_state"] in ["done", "estimating database item count",
                "initialize", "loading access log", "loading data", "loading k/v pairs", "loading keys", "loading mutation log"],
                            "State is not expected: %s" % task["stats"]["ep_warmup_state"])
            if task["stats"]["ep_warmup_state"] == "loading data":
                self.assertEqual(task["stats"]["ep_warmup_thread"], 'running', "ep_warmup_thread is not expected")
                self.assertEqual(task["stats"]["ep_warmup"], 'enabled', "ep_warmup is not expected")
                self.assertTrue(int(task["stats"]["ep_warmup_value_count"]) <= int(task["stats"]["ep_warmup_estimated_value_count"]),
                                "warmed up values are greater than estimated count")
                self.assertTrue(int(task["stats"]["ep_warmup_key_count"]) <= int(task["stats"]["ep_warmup_estimated_key_count"]),
                                "warmed up keys are greater than estimated count")
                if old_task["stats"]["ep_warmup_state"] == "loading data":
                    self.assertEqual(task["stats"]["ep_warmup_estimated_value_count"],
                                     old_task["stats"]["ep_warmup_estimated_value_count"],
                                     "ep_warmup_estimated_value_count is changed")
                    self.assertEqual(task["stats"]["ep_warmup_estimated_key_count"],
                                     old_task["stats"]["ep_warmup_estimated_key_count"],
                                     "ep_warmup_estimated_key_count is not expected")
                    self.assertTrue(int(task["stats"]["ep_warmup_key_count"]) <= int(old_task["stats"]["ep_warmup_key_count"]),
                                "warmed up keys are greater than earlier value count")
                    self.assertTrue(int(task["stats"]["ep_warmup_value_count"]) <= int(old_task["stats"]["ep_warmup_value_count"]),
                                "warmed up keys are greater than earlier value count")
