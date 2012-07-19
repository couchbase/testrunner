import functools
from multiprocessing import Process

from eperf import EVPerfClient

class PerfWrapper(object):

    """Class of decorators to run complicated tests (multiclient, rebalance,
    rampup, xdcr and etc.) based on general performance tests (from eperf and
    perf modules).
    """

    @staticmethod
    def multiply(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            """This wrapper allows to launch multiple tests on the same
            client (machine). Number of concurrent processes depends on phase
            and "total_clients" parameter in *.conf file.

            There is no need to specify "prefix" and "num_clients".

            Processes don't share memory. However they share stdout/stderr.
            """
            total_clients = self.parami('total_clients', 1)
            # Limit number of workers during load phase
            if self.parami('load_phase', 0):
                total_clients = min(4, total_clients)
            self.input.test_params['num_clients'] = total_clients

            if self.parami('index_phase', 0) or self.parami('hot_load_phase', 0):
                # Single-threaded tasks (hot load phase, index phase)
                self.input.test_params['prefix'] = 0
                return test(self, *args, **kargs)
            else:
                # Concurrent tasks (load_phase, access phase)
                executors = list()

                for prefix in range(total_clients):
                    self.input.test_params['prefix'] = prefix
                    executor = Process(target=test, args=(self, ))
                    executor.start()
                    executors.append(executor)

                for executor in executors:
                    executor.join()

                return executors[0]
        return wrapper

    @staticmethod
    def rampup(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            """This wrapper launches two groups of processes:
            -- constant background load (mainly memcached sets/gets)
            -- ramping up load (mainly view queries)

            Currenly these processes don't have synchronization points.
            """
            if self.parami('index_phase', 0) or self.parami('load_phase', 0):
                return PerfWrapper.multiply(test(self, *args, **kargs))

            total_bg_clients = self.parami('total_bg_clients', 1)
            total_fg_clients = self.parami('total_fg_clients', 1)
            total_clients = total_bg_clients + total_fg_clients
            self.input.test_params['num_clients'] = total_clients

            executors = list()

            # Background load (memcached)
            original_delay = self.parami('start_delay', 30)
            self.input.test_params['start_delay'] = 5
            for prefix in range(0, total_bg_clients):
                self.input.test_params['prefix'] = prefix
                executor = Process(target=test, args=(self, ))
                executor.start()
                executors.append(executor)

            # Foreground load (memcached)
            self.input.test_params['start_delay'] = original_delay
            self.input.test_params['bg_max_ops_per_sec'] = 1
            for prefix in range(total_bg_clients, total_clients):
                self.input.test_params['prefix'] = prefix + total_bg_clients
                executor = Process(target=test, args=(self, ))
                executor.start()
                executors.append(executor)

            for executor in executors:
                executor.join()

            return executors[-1]
        return wrapper


class MultiClientTests(EVPerfClient):

    """Load tests with consistent number of clients. Each client performs the
    same work.
    """

    @PerfWrapper.multiply
    def test_evperf2(self):
        super(MultiClientTests, self).test_evperf2()

    @PerfWrapper.multiply
    def test_vperf2(self):
        super(MultiClientTests, self).test_vperf2()


class RampUpTests(EVPerfClient):

    """Ramup-up load tests with increasing number of foreground workers.
    """

    @PerfWrapper.rampup
    def test_view_rampup_1(self):
        super(RampUpTests, self).test_vperf2()
