import time, json
from threading import Thread, Event
import datetime
from tasks.future import TimeoutError
from basetestcase import BaseTestCase
from couchbase.documentgenerator import DocumentGenerator
from couchbase.documentgenerator import BlobGenerator
from couchbase.document import DesignDocument, View
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.rebalance_helper import RebalanceHelper

class CompactionViewTests(BaseTestCase):

    def setUp(self):
        super(CompactionViewTests, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.fragmentation_value = self.input.param("fragmentation_value", 80)
        self.ddocs_num = self.input.param("ddocs_num", 1)
        self.view_per_ddoc = self.input.param("view_per_ddoc", 2)
        self.use_dev_views = self.input.param("use_dev_views", False)
        self.default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.default_map_func, None)
        self.ddocs = []
        self.gen_load = BlobGenerator('test_view_compaction', 'test_view_compaction-',
                                 self.value_size, end=self.num_items)
        self.thread_crashed = Event()
        self.thread_stopped = Event()

    def tearDown(self):
        super(CompactionViewTests, self).tearDown()

    """Trigger Compaction When specified Fragmentation is reached"""
    def test_multiply_compaction(self):
        # disable auto compaction
        self.disable_compaction()
        cycles_num = self.input.param("cycles-num", 3)

        # create ddoc and add views
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()

        # load initial documents
        self._load_all_buckets(self.master, self.gen_load, "create", 0)

        for i in xrange(cycles_num):

            for ddoc in self.ddocs:
                # start fragmentation monitor
                fragmentation_monitor = \
                    self.cluster.async_monitor_view_fragmentation(self.master,
                                                                  ddoc.name,
                                                                  self.fragmentation_value)

                # generate load until fragmentation reached
                while fragmentation_monitor.state != "FINISHED":
                    # update docs to create fragmentation
                    self._load_all_buckets(self.master, self.gen_load, "update", 0)
                    for view in ddoc.views:
                        # run queries to create indexes
                        self.cluster.query_view(self.master, ddoc.name, view.name, {})
                fragmentation_monitor.result()

            for ddoc in self.ddocs:
                result = self.cluster.compact_view(self.master, ddoc.name)
                self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")

    def make_ddocs(self, ddocs_num, views_per_ddoc):
        ddoc_name = "compaction_ddoc"
        view_name = "compaction_view"
        for i in xrange(ddocs_num):
            views = self.make_default_views(view_name, views_per_ddoc, different_map=True)
            self.ddocs.append(DesignDocument(ddoc_name + str(i), views))

    def create_ddocs(self, ddocs=None, bucket=None):
        bucket_views = bucket or self.buckets[0]
        ddocs_to_create = ddocs or self.ddocs
        for ddoc in ddocs_to_create:
            if not ddoc.views:
                self.cluster.create_view(self.master, ddoc.name, [], bucket=bucket_views)
            for view in ddoc.views:
                self.cluster.create_view(self.master, ddoc.name, view, bucket=bucket_views)

    '''
    test changes ram quota during index.
    http://www.couchbase.com/issues/browse/CBQE-1649
    '''
    def test_compaction_with_cluster_ramquota_change(self):
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        gen_load = BlobGenerator('test_view_compaction',
                                 'test_view_compaction-',
                                 self.value_size,
                                 end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        for ddoc in self.ddocs:
            fragmentation_monitor = \
                self.cluster.async_monitor_view_fragmentation(self.master,
                                                              ddoc.name,
                                                              self.fragmentation_value)
            while fragmentation_monitor.state != "FINISHED":
                self._load_all_buckets(self.master, gen_load, "update", 0)
                for view in ddoc.views:
                    self.cluster.query_view(self.master, ddoc.name, view.name, {})
            fragmentation_monitor.result()

        compaction_tasks = []
        for ddoc in self.ddocs:
            compaction_tasks.append(self.cluster.async_compact_view(self.master, ddoc.name))
        
        remote = RemoteMachineShellConnection(self.master)
        cli_command = "cluster-init"
        options = "--cluster-init-ramsize=%s" % (self.quota + 10)
        output, error = remote.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost",
                                                     user=self.master.rest_username, password=self.master.rest_password)
        self.assertTrue('\n'.join(output).find('SUCCESS') != -1, 'ram wasn\'t changed')
        self.log.info('Quota was changed')
        for task in compaction_tasks:
            task.result()

    def test_views_fragmentation(self):
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        for ddoc in self.ddocs:
                fragmentation_monitor = \
                    self.cluster.async_monitor_db_fragmentation(self.master, self.fragmentation_value, self.default_bucket_name, True)
                while fragmentation_monitor.state != "FINISHED":
                    self._load_all_buckets(self.master, self.gen_load, "update", 0)
                    for view in ddoc.views:
                        self.cluster.query_view(self.master, ddoc.name, view.name, {})
                fragmentation_monitor.result()
                result = self.cluster.async.monitor_compact_view(self.master, ddoc.name, frag_value=self.fragmentation_value)
                self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")

    def rebalance_in_with_auto_ddoc_compaction(self):
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        query = {"connectionTimeout" : "60000", "full_set" : "true", "stale" : "false"}
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        end_time = time.time() + self.wait_timeout * 30
        for ddoc in self.ddocs:
            fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                                                                                  ddoc.name, self.fragmentation_value, self.default_bucket_name)
            while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
                self._load_all_buckets(self.master, self.gen_load, "update", 0)
                for view in ddoc.views:
                    self.cluster.query_view(self.master, ddoc.name, view.name, query)
            if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
                self.fail("impossible to reach compaction value {0} after {1} sec".
                          format(self.fragmentation_value, (self.wait_timeout * 30)))
            fragmentation_monitor.result()
        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        for ddoc in self.ddocs:
            compaction_task = self.cluster.async.monitor_compact_view(self.master, ddoc.name, with_rebalance=True, frag_value=self.fragmentation_value)
            self.sleep(5)
        rebalance.result()
        self.assertTrue(compaction_task, "Compaction didn't finished correctly. Please check diags")
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])

    def test_time_views_fragmentation(self):
        rest = RestConnection(self.master)
        currTime = datetime.datetime.now()
        newTime = currTime + datetime.timedelta(hours=24)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value, allowedTimePeriodFromHour=currTime.hour,
                                  allowedTimePeriodFromMin=currTime.minute, allowedTimePeriodToHour=newTime.hour, allowedTimePeriodToMin=newTime.minute)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        for ddoc in self.ddocs:
                fragmentation_monitor = \
                    self.cluster.async_monitor_db_fragmentation(self.master, self.fragmentation_value, self.default_bucket_name, True)
                while fragmentation_monitor.state != "FINISHED":
                    self._load_all_buckets(self.master, self.gen_load, "update", 0)
                    for view in ddoc.views:
                        self.cluster.query_view(self.master, ddoc.name, view.name, {})
                fragmentation_monitor.result()
        currTime = datetime.datetime.now()
        newTime = currTime + datetime.timedelta(minutes=5)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value, allowedTimePeriodFromHour=currTime.hour,
                                  allowedTimePeriodFromMin=currTime.minute, allowedTimePeriodToHour=newTime.hour, allowedTimePeriodToMin=newTime.minute)
        status, content = rest.set_view_info(self.default_bucket_name, ddoc.name)
        curr_no_of_compactions = content["stats"]["compactions"]
        self.log.info("Current number of compactions is {0)".format(curr_no_of_compactions))
        for ddoc in self.ddocs:
            result = self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=self.fragmentation_value)
            self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")

    def load_DB_fragmentation(self, percent_threshold, rest, remote_client):
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.servers[0], percent_threshold, self.default_bucket_name)
        end_time = time.time() + self.wait_timeout * 50
        while monitor_fragm.state != "FINISHED":
            if end_time < time.time():
                self.fail("Fragmentation level is not reached in %s sec" % self.wait_timeout * 50)
            try:
                self._load_all_buckets(self.master, self.gen_load, "update", 0)
            except Exception, ex:
                self.log.error("Load cannot be performed: %s" % str(ex))
        monitor_fragm.result()
        try:
            compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout))
            self.assertTrue(compact_run, "Compaction didn't finished correctly. Please check diags")
        except Exception, ex:
            self.thread_crashed.set()
            self.log.error("****ERROR***** \n At least one of threads is crashed: %s" % (ex))
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()

    def load_view_fragmentation(self, percent_threshold):
        query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 60000}
        for ddoc in self.ddocs:
                fragmentation_monitor = \
                    self.cluster.async_monitor_db_fragmentation(self.master, percent_threshold , self.default_bucket_name, True)
                while fragmentation_monitor.state != "FINISHED":
                    self._load_all_buckets(self.master, self.gen_load, "update", 0)
                    for view in ddoc.views:
                        # run queries to create indexes
                        self.cluster.query_view(self.master, ddoc.name, view.name, query)
                fragmentation_monitor.result()
                try:
                    result = self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=self.fragmentation_value)
                    self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
                except Exception, ex:
                    self.thread_crashed.set()
                    self.log.error("****ERROR***** \n At least one of threads is crashed: %s" % (ex))
                    raise ex
                finally:
                    if not self.thread_stopped.is_set():
                        self.thread_stopped.set()

    def test_parallel_DB_views_compaction(self):
        threads = []
        percent_threshold = self.fragmentation_value
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        self.set_auto_compaction(rest, parallelDBAndVC="true", viewFragmntThresholdPercentage=percent_threshold, dbFragmentThresholdPercentage=percent_threshold)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)
        rebalance = self.cluster.async_rebalance([self.master], self.servers[1:self.nodes_in + 1], [])
        rebalance.result()
        threads.append(Thread(target=self.load_view_fragmentation, name="view_Thread", args=(percent_threshold,)))
        threads.append(Thread(target=self.load_DB_fragmentation, name="DB_Thread", args=(percent_threshold, rest, remote_client)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if self.thread_crashed.is_set():
            self.fail("Error occurred during run")

    def test_parallel_enable_views_compaction(self):
        threads = []
        percent_threshold = self.fragmentation_value
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        self.set_auto_compaction(rest, parallelDBAndVC="true", viewFragmntThresholdPercentage=percent_threshold)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)
        rebalance = self.cluster.async_rebalance([self.master], self.servers[1:self.nodes_in + 1], [])
        rebalance.result()
        threads.append(Thread(target=self.load_view_fragmentation, name="view_Thread", args=(percent_threshold,)))
        threads.append(Thread(target=self.load_DB_fragmentation, name="DB_Thread", args=(percent_threshold, rest, remote_client)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if self.thread_crashed.is_set():
            self.fail("Error occurred during run")

    def test_parallel_enable_DB_compaction(self):
        threads = []
        percent_threshold = self.fragmentation_value
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        self.set_auto_compaction(rest, parallelDBAndVC="true", dbFragmentThresholdPercentage=percent_threshold)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)
        rebalance = self.cluster.async_rebalance([self.master], self.servers[1:self.nodes_in + 1], [])
        rebalance.result()
        threads.append(Thread(target=self.load_view_fragmentation, name="view_Thread", args=(percent_threshold,)))
        threads.append(Thread(target=self.load_DB_fragmentation, name="DB_Thread", args=(percent_threshold, rest, remote_client)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if self.thread_crashed.is_set():
            self.fail("Error occurred during run")

    def test_views_size_fragmentation(self):
        percent_threshold = self.fragmentation_value * 1048576
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, viewFragmntThreshold=percent_threshold)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        for ddoc in self.ddocs:
            comp_rev, fragmentation = self._get_compaction_details(rest, self.default_bucket_name, ddoc.name)
            self.log.info("Stats Compaction Rev and Fragmentation before Compaction is ({0}), ({1})".format(comp_rev, fragmentation))
            fragmentation_monitor = \
                monitor_fragm = self.cluster.async_monitor_disk_size_fragmentation(self.servers[0], percent_threshold, self.default_bucket_name, True)
            while fragmentation_monitor.state != "FINISHED":
                self._load_all_buckets(self.master, self.gen_load, "update", 0)
                for view in ddoc.views:
                    self.cluster.query_view(self.master, ddoc.name, view.name, {})
            fragmentation_monitor.result()
            time.sleep(10)
            new_comp_rev, fragmentation = self._get_compaction_details(rest, self.default_bucket_name, ddoc.name)
            self.log.info("Stats Compaction Rev and Fragmentation After Compaction is ({0}) ({1})".format(new_comp_rev, fragmentation))
            if new_comp_rev > comp_rev:
                self.log.info("Compaction triggered successfully")
            else:
                try:
                    result = self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=percent_threshold)
                    self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
                except Exception, ex:
                    self.fail(ex)

    def _get_compaction_details(self, rest, bucket, design_doc_name):
        total_fragmentation = 0
        status, content = rest.set_view_info(bucket, design_doc_name)
        curr_no_of_compactions = content["stats"]["compactions"]
        total_disk_size = content['disk_size']
        total_data_size = content['data_size']
        if total_disk_size > 0 and total_data_size > 0:
            total_fragmentation = \
                (total_disk_size - total_data_size) / float(total_disk_size) * 100
        return (curr_no_of_compactions, total_fragmentation)

