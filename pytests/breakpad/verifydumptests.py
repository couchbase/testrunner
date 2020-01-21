import os
import re
import time
import logger
import threading
from .breakpadbase import BreakpadBase
from membase.api.rest_client import RestConnection
from .logpoll import NSLogPoller

log = logger.Logger.get_logger()

class BreakpadVerifyDumpTests(BreakpadBase):

    def verify_dump_exists_after_crash(self):
        vbucket = 0
        index = 0
        nodeA = self.servers[index]

        # load some documents
        self.load_docs(nodeA, 1000)

        # start log poller
        logp = NSLogPoller(index)
        logp.start()

        # kill memcahced
        assert self.kill_memcached(0)

        # get dmp_path from log poller
        dmp_path = logp.getDumpQItem()

        # verify a coresponding dmp exists on filesystem
        assert dmp_path and os.access(dmp_path, os.R_OK)

    def verify_dump_exists_various_kills(self):
        vbucket = 0
        index = 0
        nodeA = self.servers[index]

        # load some documents
        self.load_docs(nodeA, 1000)

        # start log poller
        logp = NSLogPoller(index)
        logp.start()

        # kill node 0 with sig=6
        assert self.kill_memcached(0, sig=6)
        dmp_path = logp.getDumpQItem()
        assert dmp_path and os.access(dmp_path, os.R_OK)

        logp = NSLogPoller(1)
        logp.start()
        # SIGSEGV to node1
        assert self.kill_memcached(1, sig=11)
        dmp_path = logp.getDumpQItem()
        assert dmp_path and os.access(dmp_path, os.R_OK)

    def verify_dump_exists_after_n_crashes(self):

        n = 5
        vbucket = 0
        index = 0
        nodeA = self.servers[index]
        crashes = 0
        logp = NSLogPoller(index)
        logp.start()

        self.load_docs(nodeA, 100000)

        # watch for mcd restarted
        logp.setMcEventFlag(True)
        # kill mc n times
        for i in range(n):
            rc = self.kill_memcached(0)
            assert rc, "Did not kill mc"
            # get restart msg
	    msg = logp.getEventQItem()
	    assert msg
            crashes += 1

        logp.setMcEventFlag(False)
        while True:
            dmp_path = logp.getDumpQItem()
            if dmp_path is None:
                break
            crashes -= 1
        assert crashes == 0, "Did not report all crashes"

    def verify_multi_node_dumps_exist(self):

        n = 4
        vbucket = 0
        index = 0
        crashes = 0
        nodeA = self.servers[index]
        pollers = []
        self.cluster.rebalance(
            [self.master],
            self.servers[1:], [])


        self.load_docs(nodeA, 10000)

        node_range = list(range(len(self.servers)))

        # start log pollers
        for i in node_range:
            logp = NSLogPoller(i)
            logp.start()
            pollers.append(logp)

        # kill mc n times
        # crash mcd on all nodes
        for i in node_range:

            # kill
            p = pollers[i]
            p.setMcEventFlag(True)
            assert self.kill_memcached(i)

            # get restart msg
            msg = p.getEventQItem()
            assert msg

            # get dump item
            dmp_path = p.getDumpQItem()
            if dmp_path is None:
                try:
                    time.sleep(5)
                    dmp_path = p.getDumpQItem()
                except Exception as e:
                    raise Exception("Failed to get dump path: {0}".format(e))
            assert dmp_path and os.access(dmp_path, os.R_OK)

    def verify_crash_during_rebalance(self):

        n = 4
        vbucket = 0
        index = 0
        crashes = 0
        pollers = []
        nodeA = self.servers[index]
        self.load_docs(nodeA, 100000)

        reb_poller= NSLogPoller(index)
        reb_poller.start()
        reb_poller.setRebalanceEventFlag(True)

        # rebalance async
        task = self.cluster.async_rebalance(
            [self.master],
            self.servers[1:], [])

        # did start rebalance
        assert reb_poller.getEventQItem()

        node_range = list(range(len(self.servers)))
        # start log pollers
        for i in node_range:
            logp = NSLogPoller(i)
            logp.start()
            pollers.append(logp)

        # crash mcd on all nodes
        for i in node_range:
            assert self.kill_memcached(i)

        for p in pollers:
            dmp_path = p.getDumpQItem()
            if dmp_path is None:
                continue
            crashes += 1

        assert crashes == len(self.servers)

    def verify_mini_dmp_to_core(self):
        vbucket = 0
        index = 0
        nodeA = self.servers[index]

        # load some documents
        self.load_docs(nodeA, 1000)

        # start log poller
        logp = NSLogPoller(index)
        logp.start()

        # kill memcahced
        assert self.kill_memcached(0)

        # get dmp_path from log poller
        dmp_path = logp.getDumpQItem()

        # verify a coresponding dmp exists on filesystem
        assert os.access(dmp_path, os.R_OK)

        f_core = self.dmp_to_core(dmp_path)
        assert os.access(f_core, os.R_OK)
        assert self.verify_core(f_core)

    def kill_during_concurrent_loader(self):

        nodeA = self.servers[0]
        logp = NSLogPoller(0)
        logp.start()
        running = True
        threads = []

        # inline doc loader
        def loader():
            while running:
                try:
                    self.load_docs(nodeA, 1000)
                except:
                    pass

        # async doc loaders
        for  i in range(10):
            t = threading.Thread(target=loader)
            t.start()
            threads.append(t)

        assert self.kill_memcached(0)
        dmp_path = logp.getDumpQItem()

        assert os.access(dmp_path, os.R_OK)

        running = False
        [t.join() for t in threads]

    def kill_during_compaction(self):

        nodeA = self.servers[0]
        logp = NSLogPoller(0)
        logp.start()
        running = True
        threads = []

        def loader():
            while running:
                try:
                    self.load_docs(nodeA, 1000)
                except:
                    pass

        def compact():
            rest = RestConnection(nodeA)
            rest.compact_bucket()

        # async doc loaders
        for  i in range(10):
            t = threading.Thread(target=loader)
            t.start()
            threads.append(t)

        # watch for compaction start
        logp.setCompactEventFlag(True)

        # async compact
        t = threading.Thread(target=compact)
        t.start()

        # verify compaction event did happen
	assert logp.getEventQItem()
        logp.setCompactEventFlag(False)

        # kill mc
        assert self.kill_memcached(0)

        # stop data loading
        running = False
        [t.join() for t in threads]

        # get dmp_path from log poller
        dmp_path = logp.getDumpQItem()
        # verify a coresponding dmp exists on filesystem
        assert dmp_path and os.access(dmp_path, os.R_OK)
