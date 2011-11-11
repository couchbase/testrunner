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
from membase.performance.stats import StatsCollector

import testconstants
import mcsoda
import perf

class EPerfMaster(perf.PerfBase):
    specURL = "http://hub.internal.couchbase.org/confluence/pages/viewpage.action?pageId=1901816"

    def setUp(self):
        self.dgm = False
        self.is_master = True
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
        pass

    def gated_finish(self, clients, notify):
        pass

    def load_phase(self, num_nodes, num_items):
        if self.is_master:
            self.nodes(num_nodes)
            self.load(self.parami("items", num_items),
                      self.param('size', self.min_value_size()),
                      kind=self.param('kind', 'json'),
                      protocol=self.param('protocol', 'binary'),
                      use_direct=self.parami('use_direct', 1),
                      doc_cache=self.parami('doc_cache', 0),
                      prefix=self.param("prefix", ""))

    def access_phase(self, items,
                     ratio_sets     = 0,
                     ratio_misses   = 0,
                     ratio_creates  = 0,
                     ratio_deletes  = 0,
                     ratio_hot      = 0,
                     ratio_hot_gets = 0,
                     ratio_hot_sets = 0,
                     max_creates    = 0):
        self.loop_prep()
        self.loop(num_ops        = 0,
                  num_items      = self.parami("items", 30000000),
                  max_creates    = self.parami("max_creates", max_creates),
                  min_value_size = self.param('size', self.min_value_size()),
                  kind           = self.param('kind', 'json'),
                  protocol       = self.param('protocol', 'binary'),
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
                  collect_server_stats = self.parami("collect_server_stats",
                                                     self.is_master))

    # ---------------------------------------------

    def test_ept_read(self):
        self.spec("EPT-READ")
        self.load_phase(10, 30000000)
        notify = self.gated_start(self.input.clients)
        self.access_phase(30000000,
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
        self.load_phase(10, 45000000)
        notify = self.gated_start(self.input.clients)
        self.access_phase(45000000,
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
        self.load_phase(10, 45000000)
        notify = self.gated_start(self.input.clients)
        self.access_phase(45000000,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0833),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          max_creates    = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)

    def TODO_test_ept_rebalance_low(self):
        self.spec("EPT-REBALANCE-LOW-FETCH")

    def TODO_test_ept_rebalance_med(self):
        self.spec("EPT-REBALANCE_MED_FETCH")


class EPerfClient(EPerfMaster):

    def setUp(self):
        self.dgm = False
        self.is_master = False
        self.setUpBase0()

        pass # Skip super's setUp().  The master should do the real work.

    def tearDown(self):
        if self.sc is not None:
            self.sc.stop()
            self.sc = None

        pass # Skip super's tearDown().  The master should do the real work.

    def test_ept_read(self):
        super(EPerfClient, self).test_ept_read()

    def test_ept_write(self):
        super(EPerfClient, self).test_ept_write()

    def test_ept_mixed(self):
        super(EPerfClient, self).test_ept_mixed()

