# Executive performance dashboard tests.

import unittest
import uuid
import logger
import time
import json
import os
import threading

from TestInput import TestInputSingleton

from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.performance.stats import StatsCollector, CallbackStatsCollector

import testconstants
import mcsoda
import perf

class EPerfMaster(perf.PerfBase):
    specURL = "http://hub.internal.couchbase.org/confluence/pages/viewpage.action?pageId=1901816"

    def setUp(self):
        self.dgm = False
        self.is_master = True
        self.input = TestInputSingleton.input
        self.mem_quota = self.parami("mem_quota", 10000)
        self.level_callbacks = []
        self.latched_rebalance_done = False
        super(EPerfMaster, self).setUp()

    def tearDown(self):
        self.aggregate_all_stats()
        super(EPerfMaster, self).tearDown()

    def aggregate_all_stats(self):
        # One node, the 'master', should aggregate stats from client and server nodes
        # and push results to couchdb.
        # TODO: Karan.
        pass

    def min_value_size(self):
        # Returns an array of different value sizes so that
        # the average value size is 2k and the ratio of
        # sizes is 33% 1k, 33% 2k, 33% 3k, 1% 10k.
        mvs = []
        for i in range(33):
            mvs.append(1024)
            mvs.append(2048)
            mvs.append(3072)
        mvs.append(10240)
        return mvs

    def gated_start(self, clients):
        if not self.is_master:
            self.setUpBase1()

    def gated_finish(self, clients, notify):
        pass

    def load_phase(self, num_nodes, num_items):
        if self.is_master or self.parami("load_phase", 0) > 0:
            self.nodes(num_nodes)
            self.load(self.parami("items", num_items),
                      self.param('size', self.min_value_size()),
                      kind=self.param('kind', 'json'),
                      protocol=self.param('protocol',
                                          'membase-binary://' + \
                                              self.input.servers[0].ip + ":8091"),
                      use_direct=self.parami('use_direct', 1),
                      doc_cache=self.parami('doc_cache', 0),
                      prefix=self.param("prefix", ""))
            self.loop_prep()

    def access_phase(self, items,
                     ratio_sets     = 0,
                     ratio_misses   = 0,
                     ratio_creates  = 0,
                     ratio_deletes  = 0,
                     ratio_hot      = 0,
                     ratio_hot_gets = 0,
                     ratio_hot_sets = 0,
                     max_creates    = 0):
        if (not self.is_master) or self.parami("access_phase", 0) > 0:
            items = self.parami("items", items)
            num_clients = len(self.input.clients) or 1
            start_at = int(self.paramf("start_at", 1.0) * \
                           (self.parami("prefix", 0) * items /
                            num_clients))
            start_delay = self.parami("start_delay", 2 * 60) # 2 minute delay.
            if start_delay > 0:
                time.sleep(start_delay * self.parami("prefix", 0))
            max_creates = self.parami("max_creates", max_creates) / num_clients
            self.loop(num_ops        = 0,
                      num_items      = items,
                      max_items      = -1,
                      max_creates    = max_creates,
                      min_value_size = self.param('size', self.min_value_size()),
                      kind           = self.param('kind', 'json'),
                      protocol       = self.param('protocol',
                                                  'membase-binary://' + \
                                                      self.input.servers[0].ip + ":8091"),
                      clients        = self.parami('clients', 1),
                      ratio_sets     = ratio_sets,
                      ratio_misses   = ratio_misses,
                      ratio_creates  = ratio_creates,
                      ratio_deletes  = ratio_deletes,
                      ratio_hot      = ratio_hot,
                      ratio_hot_gets = ratio_hot_gets,
                      ratio_hot_sets = ratio_hot_sets,
                      test_name      = self.id(),
                      use_direct     = self.parami('use_direct', 1),
                      doc_cache      = self.parami('doc_cache', 0),
                      prefix         = self.param("prefix", ""),
                      collect_server_stats = self.is_leader,
                      start_at       = start_at,
                      report         = int(max_creates * 0.1),
                      exit_after_creates = 1)

    def latched_rebalance(self, cur):
        if not self.latched_rebalance_done:
            self.latched_rebalance_done = True
            self.delayed_rebalance(self.parami("num_nodes_after", 15), 0.01)

    # ---------------------------------------------

    def test_ept_read(self):
        self.spec("EPT-READ")
        items = self.parami("items", 30000000)
        self.load_phase(self.parami("num_nodes", 10), items)
        notify = self.gated_start(self.input.clients)
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.1),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.30),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.1428),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          max_creates    = self.parami("max_creates", 10800000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_write(self):
        self.spec("EPT-WRITE")
        items = self.parami("items", 45000000)
        self.load_phase(self.parami("num_nodes", 10), items)
        notify = self.gated_start(self.input.clients)
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0833),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          max_creates    = self.parami("max_creates", 20000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_mixed(self):
        self.spec("EPT-MIXED")
        items = self.parami("items", 45000000)
        self.load_phase(self.parami("num_nodes", 10), items)
        notify = self.gated_start(self.input.clients)
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.13),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          max_creates    = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_rebalance_low(self):
        self.spec("EPT-REBALANCE-LOW-FETCH")
        items = self.parami("items", 45000000)
        self.load_phase(self.parami("num_nodes", 10), items)
        notify = self.gated_start(self.input.clients)
        num_clients = len(self.input.clients) or 1
        rebalance_after = self.parami("rebalance_after", 2000000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0833),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          max_creates    = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_rebalance_med(self):
        self.spec("EPT-REBALANCE-MED-FETCH")
        items = self.parami("items", 45000000)
        self.load_phase(self.parami("num_nodes", 10), items)
        notify = self.gated_start(self.input.clients)
        num_clients = len(self.input.clients) or 1
        rebalance_after = self.parami("rebalance_after", 2000000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0833),
                          ratio_hot      = self.paramf('ratio_hot', 0.6),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          max_creates    = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)


class EPerfClient(EPerfMaster):

    def setUp(self):
        self.dgm = False
        self.is_master = False
        self.level_callbacks = []
        self.latched_rebalance_done = False
        self.setUpBase0()
        self.is_leader = self.parami("prefix", 0) == 0

        pass # Skip super's setUp().  The master should do the real work.

    def tearDown(self):
        if self.sc is not None:
            self.sc.stop()
            self.sc = None

        pass # Skip super's tearDown().  The master should do the real work.

    def mk_stats(self, verbosity):
        if self.parami("prefix", 0) == 0 and self.level_callbacks:
            sc = CallbackStatsCollector(verbosity)
            sc.level_callbacks = self.level_callbacks
        else:
            sc = super(EPerfMaster, self).mk_stats(verbosity)

        return sc

    def test_ept_read(self):
        super(EPerfClient, self).test_ept_read()

    def test_ept_write(self):
        super(EPerfClient, self).test_ept_write()

    def test_ept_mixed(self):
        super(EPerfClient, self).test_ept_mixed()

    def test_ept_rebalance_low(self):
        super(EPerfClient, self).test_ept_rebalance_low()

    def test_ept_rebalance_med(self):
        super(EPerfClient, self).test_ept_rebalance_med()
