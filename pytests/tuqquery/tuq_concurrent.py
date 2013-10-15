import threading
from threading import Event
from tuqquery.tuq import QueryTests
from viewquerytests import StoppableThread

class ConcurrentTests(QueryTests):
    def setUp(self):
        super(ConcurrentTests, self).setUp()
        self.thread_crashed = Event()
        self.thread_stopped = Event()
        self.num_threads = self.input.param("num_threads", 4)
        self.test_to_run = self.input.param("test_to_run", "test_simple_check")
        self.ops = self.input.param("ops", None)

    def suite_setUp(self):
        super(ConcurrentTests, self).suite_setUp()

    def tearDown(self):
        super(ConcurrentTests, self).tearDown()

    def suite_tearDown(self):
        super(ConcurrentTests, self).suite_tearDown()

    def test_concurrent_queries(self):
        task_ops = None
        if self.ops == 'rebalance':
            task_ops = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                self.servers[self.nodes_init:self.nodes_init + self.nodes_in], [])
        elif self.ops == 'failover':
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            task_ops = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               [], servr_out)
        query_threads = []
        for n in xrange(self.num_threads):
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

    def query_thread(self, method_name):
        try:
            fn = getattr(self, method_name)
            fn()
        except Exception as ex:
            self.log.error("***************ERROR*************\n" +\
                           "At least one of query threads is crashed: {0}".format(ex))
            self.thread_crashed.set()
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()