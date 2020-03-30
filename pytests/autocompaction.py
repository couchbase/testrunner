import unittest
import logger
import random
import time
import json
import datetime
from threading import Thread, Event
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import BlobGenerator
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from testconstants import MIN_COMPACTION_THRESHOLD
from testconstants import MAX_COMPACTION_THRESHOLD


class AutoCompactionTests(BaseTestCase):

    servers = None
    clients = None
    log = None
    input = None

    def setUp(self):
        super(AutoCompactionTests, self).setUp()
        self.autocompaction_value = self.input.param("autocompaction_value", 0)
        self.is_crashed = Event()
        self.during_ops = self.input.param("during_ops", None)
        self.gen_load = BlobGenerator('compact', 'compact-', self.value_size, start=0, end=self.num_items)
        self.gen_update = BlobGenerator('compact', 'compact-', self.value_size, start=0, end=(self.num_items // 2))

    @staticmethod
    def insert_key(serverInfo, bucket_name, count, size):
        rest = RestConnection(serverInfo)
        smart = VBucketAwareMemcached(rest, bucket_name)
        for i in range(count * 1000):
            key = "key_" + str(i)
            flag = random.randint(1, 999)
            value = {"value" : MemcachedClientHelper.create_value("*", size)}
            smart.memcached(key).set(key, 0, 0, json.dumps(value))

    def load(self, server, compaction_value, bucket_name, gen):
        self.log.info('in the load, wait time is {0}'.format(self.wait_timeout) )
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(server, compaction_value, bucket_name)
        end_time = time.time() + self.wait_timeout * 5
        # generate load until fragmentation reached
        while monitor_fragm.state != "FINISHED":
            if self.is_crashed.is_set():
                self.cluster.shutdown(force=True)
                return

            if end_time < time.time():
                self.err = "Fragmentation level is not reached in %s sec" % self.wait_timeout * 5
                return
            # update docs to create fragmentation
            try:
                self._load_all_buckets(server, gen, "update", 0)
            except Exception as ex:
                self.is_crashed.set()
                self.log.error("Load cannot be performed: %s" % str(ex))
        monitor_fragm.result()

    def test_database_fragmentation(self):


        self.log.info('start test_database_fragmentation')

        self.err = None
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        percent_threshold = self.autocompaction_value
        bucket_name = "default"
        MAX_RUN = 100
        item_size = 1024
        update_item_size = item_size * ((float(100 - percent_threshold)) // 100)
        serverInfo = self.servers[0]
        self.log.info(serverInfo)

        rest = RestConnection(serverInfo)
        remote_client = RemoteMachineShellConnection(serverInfo)
        output, rq_content, header = rest.set_auto_compaction("false", dbFragmentThresholdPercentage=percent_threshold, viewFragmntThresholdPercentage=None)

        if not output and (percent_threshold <= MIN_COMPACTION_THRESHOLD or percent_threshold >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output, "it should be  impossible to set compaction value = {0}%".format(percent_threshold))
            import json
            self.assertTrue("errors" in json.loads(rq_content), "Error is not present in response")
            self.assertTrue(str(json.loads(rq_content)["errors"]).find("Allowed range is 2 - 100") > -1, \
                            "Error 'Allowed range is 2 - 100' expected, but was '{0}'".format(str(json.loads(rq_content)["errors"])))
            self.log.info("Response contains error = '%(errors)s' as expected" % json.loads(rq_content))

        elif (output and percent_threshold >= MIN_COMPACTION_THRESHOLD
                     and percent_threshold <= MAX_RUN):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * (node_ram_ratio) // 2
            items = (int(available_ram * 1000) // 2) // item_size
            print("ITEMS =============%s" % items)

            rest.create_bucket(bucket=bucket_name, ramQuotaMB=int(available_ram), authType='sasl',
                               saslPassword='password', replicaNumber=1, proxyPort=11211)
            BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            BucketOperationHelper.wait_for_vbuckets_ready_state(serverInfo, bucket_name)

            self.log.info("******start to load {0}K keys with {1} bytes/key".format(items, item_size))
            #self.insert_key(serverInfo, bucket_name, items, item_size)
            generator = BlobGenerator('compact', 'compact-', int(item_size), start=0, end=(items * 1000))
            self._load_all_buckets(self.master, generator, "create", 0, 1, batch_size=1000)
            self.log.info("sleep 10 seconds before the next run")
            time.sleep(10)

            self.log.info("********start to update {0}K keys with smaller value {1} bytes/key".format(items,
                                                                             int(update_item_size)))
            generator_update = BlobGenerator('compact', 'compact-', int(update_item_size), start=0, end=(items * 1000))
            if self.during_ops:
                if self.during_ops == "change_port":
                    self.change_port(new_port=self.input.param("new_port", "9090"))
                    self.master.port = self.input.param("new_port", "9090")
                elif self.during_ops == "change_password":
                    old_pass = self.master.rest_password
                    self.change_password(new_password=self.input.param("new_password", "new_pass"))
                    self.master.rest_password = self.input.param("new_password", "new_pass")
                rest = RestConnection(self.master)
            insert_thread = Thread(target=self.load,
                                   name="insert",
                                   args=(self.master, self.autocompaction_value,
                                         self.default_bucket_name, generator_update))
            try:
                self.log.info('starting the load thread')
                insert_thread.start()

                compact_run = remote_client.wait_till_compaction_end(rest, bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 10))

                if not compact_run:
                    self.fail("auto compaction does not run")
                elif compact_run:
                    self.log.info("auto compaction run successfully")
            except Exception as ex:
                self.log.info("exception in auto compaction")
                if self.during_ops:
                     if self.during_ops == "change_password":
                         self.change_password(new_password=old_pass)
                     elif self.during_ops == "change_port":
                         self.change_port(new_port='8091',
                                          current_port=self.input.param("new_port", "9090"))
                if str(ex).find("enospc") != -1:
                    self.is_crashed.set()
                    self.log.error("Disk is out of space, unable to load more data")
                    insert_thread._Thread__stop()
                else:
                    insert_thread._Thread__stop()
                    raise ex
            else:
                insert_thread.join()
                if self.err is not None:
                    self.fail(self.err)
        else:
            self.log.error("Unknown error")
        if self.during_ops:
                     if self.during_ops == "change_password":
                         self.change_password(new_password=old_pass)
                     elif self.during_ops == "change_port":
                         self.change_port(new_port='8091',
                                          current_port=self.input.param("new_port", "9090"))


    def _viewFragmentationThreshold(self):
        for serverInfo in self.servers:
            self.log.info(serverInfo)
            rest = RestConnection(serverInfo)
            rest.set_auto_compaction(dbFragmentThresholdPercentage=80, viewFragmntThresholdPercentage=80)

    def rebalance_in_with_DB_compaction(self):
        self.disable_compaction()
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self._monitor_DB_fragmentation()
        servs_in = self.servers[self.nodes_init:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        self.sleep(5)
        compaction_task = self.cluster.async_compact_bucket(self.master, self.default_bucket_name)
        result = compaction_task.result(self.wait_timeout * 5)
        self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])

    def rebalance_in_with_auto_DB_compaction(self):
        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value)
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self._monitor_DB_fragmentation()
        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 5))
        rebalance.result()
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.master, 0, self.default_bucket_name)
        result = monitor_fragm.result()
        if compact_run:
            self.log.info("auto compaction run successfully")
        elif result:
            self.log.info("Compaction is already completed")
        else:
            self.fail("auto compaction does not run")
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])
        remote_client.disconnect()

    def rebalance_out_with_DB_compaction(self):
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        self.disable_compaction()
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self._monitor_DB_fragmentation()
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        rebalance = self.cluster.async_rebalance([self.master], [], servs_out)
        compaction_task = self.cluster.async_compact_bucket(self.master, self.default_bucket_name)
        result = compaction_task.result(self.wait_timeout * 5)
        self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance.result()
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])

    def rebalance_out_with_auto_DB_compaction(self):
        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value)
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self._monitor_DB_fragmentation()
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        rebalance = self.cluster.async_rebalance([self.master], [], servs_out)
        compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 5))
        rebalance.result()
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.master, 0, self.default_bucket_name)
        result = monitor_fragm.result()
        if compact_run:
            self.log.info("auto compaction run successfully")
        elif result:
            self.log.info("Compaction is already completed")
        else:
            self.fail("auto compaction does not run")
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])
        remote_client.disconnect()

    def rebalance_in_out_with_DB_compaction(self):
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                            "ERROR: Not enough nodes to do rebalance in and out")
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.disable_compaction()
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        rebalance = self.cluster.async_rebalance(servs_init, servs_in, servs_out)
        while rebalance.state != "FINISHED":
            self._monitor_DB_fragmentation()
            compaction_task = self.cluster.async_compact_bucket(self.master, self.default_bucket_name)
            result = compaction_task.result(self.wait_timeout * 5)
            self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance.result()
        self.verify_cluster_stats(result_nodes)

    def rebalance_in_out_with_auto_DB_compaction(self):
        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                            "ERROR: Not enough nodes to do rebalance in and out")
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value)
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        rebalance = self.cluster.async_rebalance(servs_init, servs_in, servs_out)
        while rebalance.state != "FINISHED":
            self._monitor_DB_fragmentation()
            compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                 timeout_in_seconds=(self.wait_timeout * 5))
        rebalance.result()
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.master, 0, self.default_bucket_name)
        result = monitor_fragm.result()
        if compact_run:
            self.log.info("auto compaction run successfully")
        elif result:
            self.log.info("Compaction is already completed")
        else:
            self.fail("auto compaction does not run")
        self.verify_cluster_stats(result_nodes)
        remote_client.disconnect()

    def test_database_time_compaction(self):
        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        currTime = datetime.datetime.now()
        fromTime = currTime + datetime.timedelta(hours=1)
        toTime = currTime + datetime.timedelta(hours=10)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value, allowedTimePeriodFromHour=fromTime.hour,
                                 allowedTimePeriodFromMin=fromTime.minute, allowedTimePeriodToHour=toTime.hour, allowedTimePeriodToMin=toTime.minute,
                                 allowedTimePeriodAbort="false")
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self._monitor_DB_fragmentation()
        for i in range(10):
            active_tasks = self.cluster.async_monitor_active_task(self.master, "bucket_compaction", "bucket", wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
                self.sleep(2)
        currTime = datetime.datetime.now()
        #Need to make it configurable
        newTime = currTime + datetime.timedelta(minutes=5)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value, allowedTimePeriodFromHour=currTime.hour,
                                 allowedTimePeriodFromMin=currTime.minute, allowedTimePeriodToHour=newTime.hour, allowedTimePeriodToMin=newTime.minute,
                                 allowedTimePeriodAbort="false")
        compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 5))
        if compact_run:
            self.log.info("auto compaction run successfully")
        else:
            self.fail("auto compaction does not run")
        remote_client.disconnect()

    def rebalance_in_with_DB_time_compaction(self):
        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        currTime = datetime.datetime.now()
        fromTime = currTime + datetime.timedelta(hours=1)
        toTime = currTime + datetime.timedelta(hours=24)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value, allowedTimePeriodFromHour=fromTime.hour,
                                 allowedTimePeriodFromMin=fromTime.minute, allowedTimePeriodToHour=toTime.hour, allowedTimePeriodToMin=toTime.minute,
                                 allowedTimePeriodAbort="false")
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self._monitor_DB_fragmentation()
        for i in range(10):
            active_tasks = self.cluster.async_monitor_active_task(self.master, "bucket_compaction", "bucket", wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
                self.sleep(2)
        currTime = datetime.datetime.now()
        #Need to make it configurable
        newTime = currTime + datetime.timedelta(minutes=5)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value, allowedTimePeriodFromHour=currTime.hour,
                                 allowedTimePeriodFromMin=currTime.minute, allowedTimePeriodToHour=newTime.hour, allowedTimePeriodToMin=newTime.minute,
                                 allowedTimePeriodAbort="false")
        servs_in = self.servers[self.nodes_init:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 5))
        rebalance.result()
        if compact_run:
            self.log.info("auto compaction run successfully")
        else:
            self.fail("auto compaction does not run")
        remote_client.disconnect()

    def test_database_size_compaction(self):
        rest = RestConnection(self.master)
        percent_threshold = self.autocompaction_value * 1048576
        self.set_auto_compaction(rest, dbFragmentThreshold=percent_threshold)
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        end_time = time.time() + self.wait_timeout * 5
        monitor_fragm = self.cluster.async_monitor_disk_size_fragmentation(self.master, percent_threshold, self.default_bucket_name)
        while monitor_fragm.state != "FINISHED":
            if end_time < time.time():
                self.fail("Fragmentation level is not reached in %s sec" % self.wait_timeout * 5)
            try:
                monitor_fragm = self.cluster.async_monitor_disk_size_fragmentation(self.master, percent_threshold, self.default_bucket_name)
                self._load_all_buckets(self.master, self.gen_update, "update", 0)
                active_tasks = self.cluster.async_monitor_active_task(self.master, "bucket_compaction", "bucket", wait_task=False)
                for active_task in active_tasks:
                    result = active_task.result()
                    self.assertTrue(result)
                    self.sleep(2)
            except Exception as ex:
                self.log.error("Load cannot be performed: %s" % str(ex))
                self.fail(ex)
        monitor_fragm.result()

    def test_start_stop_DB_compaction(self):
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        self.log.info('loading the buckets')
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self.log.info('disabling compaction')
        self.disable_compaction()
        self.log.info('monitor db fragmentation')
        self._monitor_DB_fragmentation()
        self.log.info('async compact the bucket')
        compaction_task = self.cluster.async_compact_bucket(self.master, self.default_bucket_name)
        self.log.info('cancel bucket compaction')
        self._cancel_bucket_compaction(rest, self.default_bucket_name)
        #compaction_task.result(self.wait_timeout)
        self.log.info('compact again')
        self.cluster.async_compact_bucket(self.master, self.default_bucket_name)
        self.log.info('waiting for compaction to end')
        compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name, timeout_in_seconds=self.wait_timeout)
 
        if compact_run:
            self.log.info("auto compaction run successfully")
        else:
            self.fail("auto compaction does not run")
        remote_client.disconnect()


    # Created for MB-14976 - we need more than 65536 file revisions to trigger this problem.

    def test_large_file_version(self):
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        remote_client.extract_remote_info()

        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        self.disable_compaction()
        self._monitor_DB_fragmentation()

        # rename here

        remote_client.stop_couchbase()
        time.sleep(5)
        remote_client.execute_command("cd /opt/couchbase/var/lib/couchbase/data/default;rename .1 .65535 *.1")
        remote_client.execute_command("cd /opt/couchbase/var/lib/couchbase/data/default;rename .2 .65535 *.2")
        remote_client.start_couchbase()

        for i in range(5):
            self.log.info("starting a compaction iteration")
            compaction_task = self.cluster.async_compact_bucket(self.master, self.default_bucket_name)

            compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name, timeout_in_seconds=self.wait_timeout)
            res = compaction_task.result(self.wait_timeout)


        if compact_run:
            self.log.info("auto compaction run successfully")
        else:
            self.fail("auto compaction does not run")

        remote_client.disconnect()

    def test_start_stop_auto_DB_compaction(self):
        threads = []
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value)
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        threads.append(Thread(target=self._monitor_DB_fragmentation, name="DB_Thread", args=()))
        threads.append(Thread(target=self._cancel_bucket_compaction, name="cancel_Thread", args=(rest, self.default_bucket_name,)))
        for thread in threads:
            thread.start()
            self.sleep(2)
        for thread in threads:
            thread.join()
        if self.is_crashed.is_set():
            self.fail("Error occurred during test run")


    def _cancel_bucket_compaction(self, rest, bucket):
        remote_client = RemoteMachineShellConnection(self.master)

        try:
            result = self.cluster.cancel_bucket_compaction(self.master, bucket)
            self.assertTrue(result)
            remote_client.wait_till_compaction_end(rest, self.default_bucket_name, self.wait_timeout)
            compaction_running = False
        except Exception as ex:
            self.is_crashed.set()
            self.log.error("Compaction cannot be cancelled: %s" % str(ex))
        remote_client.disconnect()
  

    def test_auto_compaction_with_multiple_buckets(self):
        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        for bucket in self.buckets:
            if bucket.name == "default":
                self.disable_compaction()
            else:
                self.set_auto_compaction(rest, dbFragmentThresholdPercentage=self.autocompaction_value, bucket=bucket.name)
        self._load_all_buckets(self.master, self.gen_load, "create", 0, 1)
        end_time = time.time() + self.wait_timeout * 30
        for bucket in self.buckets:
            monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.master, self.autocompaction_value, bucket.name)
            while monitor_fragm.state != "FINISHED":
                if end_time < time.time():
                    self.fail("Fragmentation level is not reached in %s sec" % self.wait_timeout * 30)
                try:
                    self._load_all_buckets(self.servers[0], self.gen_update, "update", 0)
                except Exception as ex:
                    self.log.error("Load cannot be performed: %s" % str(ex))
                    self.fail(ex)
            monitor_fragm.result()
            compact_run = remote_client.wait_till_compaction_end(rest, bucket.name,
                                                                     timeout_in_seconds=(self.wait_timeout * 5))
            if compact_run:
                self.log.info("auto compaction run successfully")
        remote_client.disconnect()

    def _monitor_DB_fragmentation(self):
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.master, self.autocompaction_value, self.default_bucket_name)
        end_time = time.time() + self.wait_timeout * 30
        while monitor_fragm.state != "FINISHED":
            if end_time < time.time():
                self.fail("Fragmentation level is not reached in %s sec" % self.wait_timeout * 30)
            try:
                self._load_all_buckets(self.master, self.gen_update, "update", 0)
            except Exception as ex:
                self.is_crashed.set()
                self.log.error("Load cannot be performed: %s" % str(ex))
                self.fail(ex)
        result = monitor_fragm.result()
        if not result:
            self.is_crashed.set()
        self.assertTrue(result, "Fragmentation level is not reached")
