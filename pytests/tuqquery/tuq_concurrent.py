import threading
import uuid
from threading import Event
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from tuqquery.tuq import QueryTests
from view.viewquerytests import StoppableThread
from tuqquery.tuq_sanity import QuerySanityTests

class ConcurrentTests(QuerySanityTests, QueryTests):
    def setUp(self):
        super(ConcurrentTests, self).setUp()
        self.thread_crashed = Event()
        self.thread_stopped = Event()
        self.num_threads = self.input.param("num_threads", 4)
        self.test_to_run = self.input.param("test_to_run", "test_max")
        self.ops = self.input.param("ops", None)

    def suite_setUp(self):
        super(ConcurrentTests, self).suite_setUp()

    def tearDown(self):
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status() == 'running':
            self.log.warning("rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
        try:
            super(ConcurrentTests, self).tearDown()
        except:
            pass
        #ClusterOperationHelper.cleanup_cluster(self.servers)
        #self.sleep(10)

    def suite_tearDown(self):
        super(ConcurrentTests, self).suite_tearDown()

    def test_concurrent_queries(self):
        task_ops = None
        if self.ops == 'rebalance':
            task_ops = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                self.servers[self.nodes_init:self.nodes_init + self.nodes_in], [])
        elif self.ops == 'failover':
            self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            task_ops = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               [], servr_out)
        query_threads = []
        for n in range(self.num_threads):
            t = StoppableThread(target=self.query_thread,
                name="query-{0}".format(n),
                args=(self.test_to_run,))
            query_threads.append(t)
            t.start()

        while True:
            if not query_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                self.log.error("Will stop all threads!")
                for t in query_threads:
                    t.stop()
                    self.log.error("Thread %s stopped" % str(t))
                break
            else:
                query_threads = [d for d in query_threads if d.is_alive()]
                self.log.info("Current amount of threads %s" % len(query_threads))
                self.thread_stopped.clear()
        if self.thread_crashed.is_set():
            self.fail("Test failed, see logs above!")
        if task_ops:
            task_ops.result()

    def test_concurrent_queries_hints(self):
        task_ops = None
        if self.ops == 'rebalance':
            task_ops = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                self.servers[self.nodes_init:self.nodes_init + self.nodes_in], [])
        elif self.ops == 'failover':
            self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            task_ops = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               [], servr_out)
        index_name_prefix = 'hint' + str(uuid.uuid4())[:4]
        try:
            created_indexes = []
            fields = self.input.param("index_field", '').replace(':', ',')
            fields = fields.split(';')
            for attr in fields:
                for bucket in self.buckets:
                    ind_name = attr.split('.')[0].split('[')[0].replace(',', '_')
                    self.query = "CREATE INDEX %s_%s_%s ON %s(%s) USING %s" % (index_name_prefix,
                                                                               ind_name,
                                                                               fields.index(attr),
                                                                               bucket.name, attr, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, '%s_%s_%s' % (index_name_prefix, ind_name,
                                                        fields.index(attr)))
                    created_indexes.append('%s_%s_%s' % (index_name_prefix, ind_name,
                                                        fields.index(attr)))
            for ind in created_indexes:
                self.hint_index = ind
                query_threads = []
                for n in range(self.num_threads):
                    t = StoppableThread(target=self.query_thread,
                        name="query-{0}".format(n),
                        args=(self.test_to_run,))
                    query_threads.append(t)
                    t.start()

                while True:
                    if not query_threads:
                        break
                    self.thread_stopped.wait(60)
                    if self.thread_crashed.is_set():
                        self.log.error("Will stop all threads!")
                        for t in query_threads:
                            t.stop()
                            self.log.error("Thread %s stopped" % str(t))
                        break
                    else:
                        query_threads = [d for d in query_threads if d.is_alive()]
                        self.log.info("Current amount of threads %s" % len(query_threads))
                        self.thread_stopped.clear()
                if self.thread_crashed.is_set():
                    self.fail("Test failed, see logs above!")
                if task_ops:
                    task_ops.result()
        finally:
            for bucket in self.buckets:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                    try:
                        self.run_cbq_query()
                    except:
                        pass