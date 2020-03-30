import time, json
from threading import Thread, Event
import datetime
from tasks.future import TimeoutError
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.document import DesignDocument, View
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.rebalance_helper import RebalanceHelper
from view.viewquerytests import StoppableThread

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

        for i in range(cycles_num):

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
        for i in range(ddocs_num):
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
        cli_command = "setting-cluster"
        options = "--cluster-ramsize=%s" % (self.quota + 10)
        output, error = remote.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost",
                                                     user=self.master.rest_username, password=self.master.rest_password)
        self.assertTrue('\n'.join(output).find('SUCCESS') != -1, 'ram wasn\'t changed')
        self.log.info('Quota was changed')
        for task in compaction_tasks:
            task.result()

    def test_views_compaction(self):
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
                compaction_task = self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=self.fragmentation_value)
                compaction_task.result()

    def rebalance_in_with_auto_ddoc_compaction(self):
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        servs_in = self.servers[self.nodes_init:self.nodes_in + 1]
        compaction_tasks = []
        self._monitor_view_fragmentation()
        rebalance_task = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_in, [])
        for ddoc in self.ddocs:
            compaction_tasks.append(self.cluster.async_monitor_compact_view(self.master, ddoc.name, with_rebalance=True, frag_value=self.fragmentation_value))
        for task in compaction_tasks:
            task.result()
        rebalance_task.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])

    def rebalance_in_with_ddoc_compaction(self):
        self.disable_compaction()
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        servs_in = self.servers[self.nodes_init:self.nodes_in + 1]
        self._monitor_view_fragmentation()
        rebalance_task = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_in, [])
        self.sleep(5)
        for ddoc in self.ddocs:
            result = self.cluster.compact_view(self.master, ddoc.name, with_rebalance=True)
            self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance_task.result()
        self.verify_cluster_stats(self.servers[:self.nodes_in + 1])

    def rebalance_out_with_auto_ddoc_compaction(self):
        rest = RestConnection(self.master)
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        compaction_tasks = []
        self._monitor_view_fragmentation()
        rebalance_task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], servs_out)
        for ddoc in self.ddocs:
            compaction_tasks.append(self.cluster.async_monitor_compact_view(self.master, ddoc.name, with_rebalance=True, frag_value=self.fragmentation_value))
        for task in compaction_tasks:
            task.result()
        rebalance_task.result()
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])

    def rebalance_out_with_ddoc_compaction(self):
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        self.disable_compaction()
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        servs_out = [self.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        self._monitor_view_fragmentation()
        rebalance_task = self.cluster.async_rebalance([self.master], [], servs_out)
        self.sleep(5)
        for ddoc in self.ddocs:
            result = self.cluster.compact_view(self.master, ddoc.name, with_rebalance=True)
            self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance_task.result()
        self.verify_cluster_stats(self.servers[:self.num_servers - self.nodes_out])

    def rebalance_in_out_with_auto_ddoc_compaction(self):
        rest = RestConnection(self.master)
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                            "ERROR: Not enough nodes to do rebalance in and out")
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        compaction_tasks = []
        self._monitor_view_fragmentation()
        rebalance_task = self.cluster.async_rebalance(servs_init, servs_in, servs_out)
        for ddoc in self.ddocs:
            compaction_tasks.append(self.cluster.async_monitor_compact_view(self.master, ddoc.name, with_rebalance=True, frag_value=self.fragmentation_value))
        for task in compaction_tasks:
            task.result()
        rebalance_task.result()
        self.verify_cluster_stats(result_nodes)

    def rebalance_in_out_with_ddoc_compaction(self):
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                            "ERROR: Not enough nodes to do rebalance in and out")
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.disable_compaction()
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        self._monitor_view_fragmentation()
        rebalance_task = self.cluster.async_rebalance(servs_init, servs_in, servs_out)
        self.sleep(5)
        for ddoc in self.ddocs:
            result = self.cluster.compact_view(self.master, ddoc.name, with_rebalance=True)
            self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance_task.result()
        self.verify_cluster_stats(result_nodes)

    def test_views_time_compaction(self):
        rest = RestConnection(self.master)
        currTime = datetime.datetime.now()
        fromTime = currTime + datetime.timedelta(hours=1)
        toTime = currTime + datetime.timedelta(hours=12)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value, allowedTimePeriodFromHour=fromTime.hour,
                                 allowedTimePeriodFromMin=fromTime.minute, allowedTimePeriodToHour=toTime.hour, allowedTimePeriodToMin=toTime.minute,
                                 allowedTimePeriodAbort="false")
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        self._monitor_view_fragmentation()
        currTime = datetime.datetime.now()
        #Need to make it configurable
        newTime = currTime + datetime.timedelta(minutes=5)
        self.set_auto_compaction(rest, viewFragmntThresholdPercentage=self.fragmentation_value, allowedTimePeriodFromHour=currTime.hour,
                                  allowedTimePeriodFromMin=currTime.minute, allowedTimePeriodToHour=newTime.hour, allowedTimePeriodToMin=newTime.minute,
                                  allowedTimePeriodAbort="false")
        for ddoc in self.ddocs:
            status, content = rest.set_view_info(self.default_bucket_name, ddoc.name)
            curr_no_of_compactions = content["stats"]["compactions"]
            self.log.info("Current number of compactions is {0}".format(curr_no_of_compactions))
        compaction_tasks = []
        for ddoc in self.ddocs:
            compaction_tasks.append(self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=self.fragmentation_value))
        for task in compaction_tasks:
            task.result()

    def load_DB_fragmentation(self):
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(self.master, self.fragmentation_value, self.default_bucket_name)
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        end_time = time.time() + self.wait_timeout * 10
        if end_time < time.time() and monitor_fragm.state != "FINISHED":
            self.fail("Fragmentation level is not reached in {0} sec".format(self.wait_timeout * 10))
        monitor_fragm.result()
        try:
            compact_run = remote_client.wait_till_compaction_end(rest, self.default_bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 5))
            self.assertTrue(compact_run, "Compaction didn't finished correctly. Please check diags")
        except Exception as ex:
            self.thread_crashed.set()
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()

    def load_view_fragmentation(self):
        query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 60000}
        end_time = time.time() + self.wait_timeout * 10
        for ddoc in self.ddocs:
            fragmentation_monitor = \
               self.cluster.async_monitor_db_fragmentation(self.master, self.fragmentation_value, self.default_bucket_name, True)
            while fragmentation_monitor.state != "FINISHED":
                for view in ddoc.views:
                    self.cluster.query_view(self.master, ddoc.name, view.name, query)
            if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
                self.fail("impossible to reach compaction value {0} after {1} sec".
                          format(self.fragmentation_value, (self.wait_timeout * 10)))
            fragmentation_monitor.result()
            try:
                compaction_task = self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=self.fragmentation_value)
                compaction_task.result(self.wait_timeout * 5)
            except Exception as ex:
                self.thread_crashed.set()
                raise ex
            finally:
                if not self.thread_stopped.is_set():
                    self.thread_stopped.set()

    def test_parallel_DB_views_compaction(self):
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, parallelDBAndVC="true", viewFragmntThresholdPercentage=self.fragmentation_value, dbFragmentThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)
        self._compaction_thread()
        if self.thread_crashed.is_set():
            self.fail("Error occurred during run")

    def test_parallel_enable_views_compaction(self):
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, parallelDBAndVC="true", viewFragmntThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)
        self._compaction_thread()
        if self.thread_crashed.is_set():
                self.log.info("DB Compaction is not started as expected")

    def test_parallel_enable_DB_compaction(self):
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, parallelDBAndVC="true", dbFragmentThresholdPercentage=self.fragmentation_value)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)
        self._compaction_thread()
        if self.thread_crashed.is_set():
                self.log.info("View Compaction is not started as expected")

    def test_views_size_compaction(self):
        percent_threshold = self.fragmentation_value * 1048576
        rest = RestConnection(self.master)
        self.set_auto_compaction(rest, viewFragmntThreshold=percent_threshold)
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        for ddoc in self.ddocs:
            comp_rev, fragmentation = self._get_compaction_details(rest, self.default_bucket_name, ddoc.name)
            self.log.info("Stats Compaction Rev and Fragmentation before Compaction is ({0}), ({1})".format(comp_rev, fragmentation))
            fragmentation_monitor = self.cluster.async_monitor_disk_size_fragmentation(self.servers[0], percent_threshold, self.default_bucket_name, True)
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
                    compaction_task = self.cluster.async_monitor_compact_view(self.master, ddoc.name, frag_value=percent_threshold)
                    compaction_task.result()
                except Exception as ex:
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

    def _monitor_view_fragmentation(self):
        query = {"connectionTimeout" : "60000", "full_set" : "true", "stale" : "false"}
        end_time = time.time() + self.wait_timeout * 30
        for ddoc in self.ddocs:
            fragmentation_monitor = self.cluster.async_monitor_db_fragmentation(self.master,
                                                                                self.fragmentation_value, self.default_bucket_name, True)
            while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
                self._load_all_buckets(self.master, self.gen_load, "update", 0)
                for view in ddoc.views:
                    self.cluster.query_view(self.master, ddoc.name, view.name, query)
            result = fragmentation_monitor.result()
            self.assertTrue(result, "impossible to reach compaction value {0} after {1} sec".
                          format(self.fragmentation_value, (self.wait_timeout * 30)))

    def _compaction_thread(self):
        threads = []
        threads.append(StoppableThread(target=self.load_view_fragmentation, name="view_Thread", args=()))
        threads.append(StoppableThread(target=self.load_DB_fragmentation, name="DB_Thread", args=()))
        for thread in threads:
            thread.start()
        while True:
            if not threads:
                break
            else:
                self._load_all_buckets(self.master, self.gen_load, "update", 0)
            self.thread_stopped.wait(60)
            threads = [d for d in threads if d.is_alive()]
            self.log.info("Current amount of threads %s" % len(threads))
            self.thread_stopped.clear()
