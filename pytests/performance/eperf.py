# Executive performance dashboard tests.

import unittest
import uuid
import logger
import time
import json
import os
import threading
import gzip
import ast

from TestInput import TestInputSingleton

from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.performance.stats import StatsCollector, CallbackStatsCollector
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper

import testconstants
import perf

class EPerfMaster(perf.PerfBase):
    specURL = "http://hub.internal.couchbase.org/confluence/pages/viewpage.action?pageId=1901816"

    def setUp(self):
        self.dgm = False
        self.is_master = True
        self.input = TestInputSingleton.input
        self.mem_quota = self.parami("mem_quota", 7000) # for 10G system
        self.level_callbacks = []
        self.latched_rebalance_done = False
        self.is_multi_node = False
        self.is_leader = self.parami("prefix", 0) == 0
        self.superSetUp();

    def superSetUp(self):
        super(EPerfMaster, self).setUp()

    def tearDown(self):
        self.superTearDown()

    def superTearDown(self):
        super(EPerfMaster, self).tearDown()

    def get_all_stats(self):
        # One node, the 'master', should aggregate stats from client and server nodes
        # and push results to couchdb.
        # Assuming clients ssh_username/password are same as server
        # Save servers list as clients
        # Replace servers ip with clients ip
        clients = self.input.servers
        for i  in range(len(clients)):
            clients[i].ip = self.input.clients[i]
        remotepath = '/tmp'

        i = 0
        for client in clients:
            shell = RemoteMachineShellConnection(client)
            filename = '{0}.json'.format(i)
            destination = "{0}/{1}".format(os.getcwd(), filename)
            print "Getting client stats file {0} from {1}".format(filename, client)
            if not shell.get_file(remotepath, filename, destination):
                print "Unable to fetch the json file {0} on Client {1} @ {2}".format(remotepath+'/'+filename \
                                                                                     , i, client.ip)
                exit(1)
            i += 1

        self.aggregate_all_stats(len(clients))

    def aggregate_all_stats(self, len_clients):
        i = 0
        file = gzip.open("{0}.loop.json.gz".format(i), 'rb')
        final_json = file.read()
        file.close()
        final_json = json.loads(final_json)
        i += 1
        merge_keys = []
        for latency in final_json.keys():
             if latency.startswith('latency'):
                 merge_keys.append(str(latency))

        for i in range(i, len_clients):
             file  = gzip.open("{0}.loop.json.gz".format(i),'rb')
             dict = file.read()
             file.close()
             dict = json.loads(dict)
             for key, value in dict.items():
                 if key in merge_keys:
                     final_json[key].extend(value)

        file = gzip.open("{0}.json.gz".format('final'), 'wb')
        file.write("{0}".format(json.dumps(final_json)))
        file.close()

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

    # Gets the vbucket count
    def gated_start(self, clients):
        if not self.is_master:
            self.setUpBase1()

    def gated_finish(self, clients, notify):
        pass

    def load_phase(self, num_nodes, num_items):
        # Cluster nodes if master
        if self.is_master:
            self.nodes(num_nodes)

        if self.parami("load_phase", 1) > 0:
            print "Loading"
            num_clients = self.parami("num_clients", len(self.input.clients) or 1)
            start_at = int(self.paramf("start_at", 1.0) * \
                           (self.parami("prefix", 0) * num_items /
                            num_clients))
            items = self.parami("num_items", num_items) / num_clients
            self.is_multi_node = False
            self.load(items,
                      self.param('size', self.min_value_size()),
                      kind=self.param('kind', 'json'),
                      protocol=self.mk_protocol(self.input.servers[0].ip),
                      use_direct=self.parami('use_direct', 1),
                      doc_cache=self.parami('doc_cache', 0),
                      prefix="",
                      start_at=start_at,
                      is_eperf=True)
            self.loop_prep()

    def access_phase(self, items,
                     ratio_sets     = 0,
                     ratio_misses   = 0,
                     ratio_creates  = 0,
                     ratio_deletes  = 0,
                     ratio_hot      = 0,
                     ratio_hot_gets = 0,
                     ratio_hot_sets = 0,
                     ratio_expirations = 0,
                     max_creates    = 0,
                     hot_shift      = 10,
                     ratio_queries  = 0,
                     queries        = None,
                     proto_prefix   = "membase-binary"):
        if self.parami("access_phase", 1) > 0:
            print "Accessing"
            items = self.parami("items", items)
            num_clients = self.parami("num_clients", len(self.input.clients) or 1)
            start_at = int(self.paramf("start_at", 1.0) * \
                           (self.parami("prefix", 0) * items /
                            num_clients))
            start_delay = self.parami("start_delay", 2 * 60) # 2 minute delay.
            if start_delay > 0:
                time.sleep(start_delay * self.parami("prefix", 0))
            max_creates = self.parami("max_creates", max_creates) / num_clients
            self.is_multi_node = False
            self.loop(num_ops        = 0,
                      num_items      = items,
                      max_items      = items + max_creates + 1,
                      max_creates    = max_creates,
                      min_value_size = self.param('size', self.min_value_size()),
                      kind           = self.param('kind', 'json'),
                      protocol       = (self.mk_protocol(self.input.servers[0].ip,
                                                         proto_prefix)),
                      clients        = self.parami('clients', 1),
                      ratio_sets     = ratio_sets,
                      ratio_misses   = ratio_misses,
                      ratio_creates  = ratio_creates,
                      ratio_deletes  = ratio_deletes,
                      ratio_hot      = ratio_hot,
                      ratio_hot_gets = ratio_hot_gets,
                      ratio_hot_sets = ratio_hot_sets,
                      ratio_expirations = ratio_expirations,
                      expiration     = self.parami('expiration', 60 * 5), # 5 minutes.
                      test_name      = self.id(),
                      use_direct     = self.parami('use_direct', 1),
                      doc_cache      = self.parami('doc_cache', 0),
                      prefix         = "",
                      collect_server_stats = self.is_leader,
                      start_at       = start_at,
                      report         = int(max_creates * 0.1),
                      exit_after_creates = self.parami('exit_after_creates', 1),
                      hot_shift = self.parami('hot_shift', hot_shift),
                      is_eperf=True,
                      ratio_queries = ratio_queries,
                      queries = queries)

    # create view and index document documents
    def index_phase(self, view, bucket="default"):
        if self.parami("index_phase", 1) > 0:
            map_fn = "function (doc) {if(doc.key_num !== undefined){emit(doc.key_num, null);}}"
            function = create_view_function(self.rest, view, map_fn)
            self.rest.create_view(bucket, view, function)
            params={'stale'    : 'false',
                    'full_set' : 'true'}
            self.rest.view_results(bucket, view, params)

    def latched_rebalance(self, cur):
        if not self.latched_rebalance_done:
            self.latched_rebalance_done = True
            self.delayed_rebalance(self.parami("num_nodes_after", 15), 0.01)

    # ---------------------------------------------

    def test_ept_read_1(self):
        self.spec("EPT-READ.1")
        items = self.parami("items", 5000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 90:3:6:1.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.1),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.30),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.1428),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.005),
                          max_creates    = self.parami("max_creates", 2000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_all_in_memory(self):
        self.spec("EPT-ALL-IN-MEMORY-1")
        items = self.parami("items",1000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates    = self.parami("max_creates", 1000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_all_in_memory_scaled_down_read(self):
        self.spec("EPT-ALL-IN-MEMORY-SCALED-DOWN-READ.1")
        items = self.parami("items",200000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 2), items)
        # Read:Insert:Update:Delete Ratio = 90:3:6:1.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.1),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.30),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.01428),
                          ratio_hot      = self.paramf('ratio_hot', 0.05),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.005),
                          max_creates    = self.parami("max_creates", 200000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_write_1(self):
        self.spec("EPT-WRITE.1")
        items = self.parami("items", 7000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates    = self.parami("max_creates", 3000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_scaled_down_write_1(self):
        self.spec("EPT-SCALED-DOWN-WRITE.1")
        items = self.parami("items",3000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 2), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates    = self.parami("max_creates", 300000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_scaled_down_write_2(self, ratio_hot=0.05, ratio_hot_get_set=0.95,
                                     max_creates=300000):
        self.spec("EPT-SCALED-DOWN-WRITE.2-" +
                  str(ratio_hot) + "-" +
                  str(ratio_hot_get_set) + "-" +
                  str(max_creates))
        items = self.parami("items",3000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 2), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot      = self.paramf('ratio_hot', ratio_hot),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', ratio_hot_get_set),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', ratio_hot_get_set),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates    = self.parami("max_creates", max_creates))
        self.gated_finish(self.input.clients, notify)

    def test_ept_scaled_down_write_2_5_98(self):
        self.test_ept_scaled_down_write_2(ratio_hot=0.05, ratio_hot_get_set=0.98)
    def test_ept_scaled_down_write_2_5_99(self):
        self.test_ept_scaled_down_write_2(ratio_hot=0.05, ratio_hot_get_set=0.99)
    def test_ept_scaled_down_write_2_5_995(self):
        self.test_ept_scaled_down_write_2(ratio_hot=0.05, ratio_hot_get_set=0.995)

    def test_ept_scaled_down_write_2_20_98(self):
        self.test_ept_scaled_down_write_2(ratio_hot=0.20, ratio_hot_get_set=0.98)
    def test_ept_scaled_down_write_2_20_99(self):
        self.test_ept_scaled_down_write_2(ratio_hot=0.20, ratio_hot_get_set=0.99)
    def test_ept_scaled_down_write_2_20_995(self):
        self.test_ept_scaled_down_write_2(ratio_hot=0.20, ratio_hot_get_set=0.995)

    def test_ept_scaled_down_mixed(self, ratio_hot=0.2, ratio_hot_get_set=0.95):
        self.spec("EPT-SCALED-DOWN-MIXED-" + str(ratio_hot) + "-" + str(ratio_hot_get_set))
        items = self.parami("items", 3000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 2), items)
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.08),
                          ratio_deletes = self.paramf('ratio_deletes', 0.13),
                          ratio_hot = self.paramf('ratio_hot', ratio_hot),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', ratio_hot_get_set),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', ratio_hot_get_set),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 300000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_scaled_down_mixed_2(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.05, ratio_hot_get_set=0.95)

    def test_ept_scaled_down_mixed_2_5_98(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.05, ratio_hot_get_set=0.98)
    def test_ept_scaled_down_mixed_2_5_99(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.05, ratio_hot_get_set=0.99)
    def test_ept_scaled_down_mixed_2_5_995(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.05, ratio_hot_get_set=0.995)

    def test_ept_scaled_down_mixed_2_20_98(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.20, ratio_hot_get_set=0.98)
    def test_ept_scaled_down_mixed_2_20_99(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.20, ratio_hot_get_set=0.99)
    def test_ept_scaled_down_mixed_2_20_995(self):
        self.test_ept_scaled_down_mixed(ratio_hot=0.20, ratio_hot_get_set=0.995)

    def test_ept_scaled_down_write_no_dgm_1(self):
        self.spec("EPT-SCALED-DOWN-WRITE-NO-DGM.1")
        items = self.parami("items", 1000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 2), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.8),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates    = self.parami("max_creates", 300000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_mixed_1(self, items=7000000, ratio_hot=0.2, ratio_hot_gets_sets=0.95,
                         max_creates=5000000):
        self.spec("EPT-MIXED.1-" + str(ratio_hot) + "-" + str(ratio_hot_gets_sets) + "-" +
                  str(max_creates))
        items = self.parami("items", items)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.13),
                          ratio_hot      = self.paramf('ratio_hot', ratio_hot),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', ratio_hot_gets_sets),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', ratio_hot_gets_sets),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates    = self.parami("max_creates", max_creates))
        self.gated_finish(self.input.clients, notify)

    def test_ept_mixed_1_5_995(self):
        self.test_ept_mixed_1(ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_mixed_1_15M_5_995(self):
        self.test_ept_mixed_1(items=15000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_mixed_1_22M_5_995(self):
        self.test_ept_mixed_1(items=22000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_mixed_1_22M_5_995_15M(self):
        self.test_ept_mixed_1(items=22000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995,
                              max_creates=15000000)

    def test_ept_rebalance_low_1(self):
        self.spec("EPT-REBALANCE-LOW-FETCH.1")
        items = self.parami("items", 7000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        rebalance_after = self.parami("rebalance_after", 2000000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.13),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates    = self.parami("max_creates", 5000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_rebalance_scaled_down_1(self):
        self.spec("EPT-REBALANCE-SCALED-DOWN.1")
        items = self.parami("items", 700000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 2), items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        rebalance_after = self.parami("rebalance_after", 200000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.13),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates    = self.parami("max_creates", 50000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_rebalance_med_1(self):
        self.spec("EPT-REBALANCE-MED-FETCH.1")
        items = self.parami("items", 7000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        rebalance_after = self.parami("rebalance_after", 2000000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.13),
                          ratio_hot      = self.paramf('ratio_hot', 0.6),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.parami('ratio_expirations', 0.03),
                          max_creates    = self.parami("max_creates", 5000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_mixed_nocompact(self):
        self.spec("EPT-MIXED-NOCOMPACT")
        items = self.parami("items", 7000000)
        if self.is_master:
            self.rest.set_autoCompaction("false", 100, 100) # 100% fragmentation thresholds.
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets     = self.paramf('ratio_sets', 0.5),
                          ratio_misses   = self.paramf('ratio_misses', 0.05),
                          ratio_creates  = self.paramf('ratio_creates', 0.08),
                          ratio_deletes  = self.paramf('ratio_deletes', 0.13),
                          ratio_hot      = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates    = self.parami("max_creates", 5000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_read_original(self):
        self.spec("EPT-READ-original")
        items = self.parami("items", 30000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 90:3:6:1.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.1),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.30),
                          ratio_deletes = self.paramf('ratio_deletes', 0.1428),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.005),
                          max_creates = self.parami("max_creates", 10800000))
        self.gated_finish(self.input.clients, notify)

    # This is a small version of test_ept_read_original with fewer creates.
    def test_ept_read_original_1(self, items=15000000,
                                 ratio_hot=0.05, ratio_hot_gets_sets=0.95,
                                 max_creates=5400000):
        self.spec("EPT-READ-original_1-" +
                  str(ratio_hot) + "-" +
                  str(ratio_hot_gets_sets) + "-" +
                  str(max_creates))
        items = self.parami("items", items)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 90:3:6:1.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.1),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.30),
                          ratio_deletes = self.paramf('ratio_deletes', 0.1428),
                          ratio_hot = self.paramf('ratio_hot', ratio_hot),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', ratio_hot_gets_sets),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', ratio_hot_gets_sets),
                          ratio_expirations = self.paramf('ratio_expirations', 0.005),
                          max_creates = self.parami("max_creates", max_creates))
        self.gated_finish(self.input.clients, notify)

    def test_ept_read_original_1_15M_5_995(self):
        self.test_ept_read_original_1(items=15000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_read_original_1_22M_5_995(self):
        self.test_ept_read_original_1(items=22000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_read_original_1_22M_5_995_15M(self):
        self.test_ept_read_original_1(items=22000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995,
                                      max_creates=15000000)

    def test_ept_write_original(self):
        self.spec("EPT-WRITE-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.8),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates = self.parami("max_creates", 20000000))
        self.gated_finish(self.input.clients, notify)

    # This is a small version of test_ept_mixed_original with fewer creates.
    def test_ept_write_original_1(self):
        self.spec("EPT-WRITE-original_1")
        items = self.parami("items", 22500000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.8),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates = self.parami("max_creates", 10000000))
        self.gated_finish(self.input.clients, notify)

    # hot itmes are 1/4 of the original_1
    def test_ept_write_original_2(self, items=22500000,
                                  ratio_hot=0.05, ratio_hot_gets_sets=0.95,
                                  max_creates=10000000):
        self.spec("EPT-WRITE-original_2-" +
                  str(ratio_hot) + "-" +
                  str(ratio_hot_gets_sets) + "-" +
                  str(max_creates))
        items = self.parami("items", items)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 20:15:60:5.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.8),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.1875),
                          ratio_deletes = self.paramf('ratio_deletes', 0.0769),
                          ratio_hot = self.paramf('ratio_hot', ratio_hot),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', ratio_hot_gets_sets),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', ratio_hot_gets_sets),
                          ratio_expirations = self.paramf('ratio_expirations', 0.025),
                          max_creates = self.parami("max_creates", max_creates))
        self.gated_finish(self.input.clients, notify)

    def test_ept_write_original_2_15M_5_995(self):
        self.test_ept_write_original_2(items=15000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_write_original_2_22M_5_995(self):
        self.test_ept_write_original_2(items=22000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995)

    def test_ept_write_original_2_22M_5_995_30M(self):
        self.test_ept_write_original_2(items=22000000, ratio_hot=0.05, ratio_hot_gets_sets=0.995,
                                       max_creates=30000000)

    def test_query_all_docs_mixed_original(self):
        self.spec("QEURY-ALLDOCS-MIXED-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        view = self.param("view","/default/_all_docs")
        limit = self.param("limit","10")
        queries = "{0}?limit={1}".format(view,limit)
        # self.index_phase(view) # Skip indexing because this is _all_docs, not a secondary index.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.10),
                          ratio_deletes = self.paramf('ratio_deletes', 0.05),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.2),
                          queries = queries,
                          proto_prefix = "couchbase")

        self.gated_finish(self.input.clients, notify)

    # run eperf tests with various query params
    #
    # underlying cbsoda lib supports templating: i.e
    # -p  prefix=1,num_clients=1,batch=100,load_phase=0,index_phase=1,access_phase=1,items=5000,query_params="{'startkey' : '{int100}' }"
    def test_query_params_mixed_original(self):
        self.spec("QUERY-PARAMS-MIXED-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)

        view = self.param("view","test_query_params_mixed_original")
        limit = self.param("limit","10")
        query_params = ast.literal_eval(self.param("query_params", "{}"))
        queries = "/default/_design/{0}/_view/{0}?limit={1}{2}".format(view,limit,params_to_str(query_params))
        self.index_phase(view)
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.10),
                          ratio_deletes = self.paramf('ratio_deletes', 0.05),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.2),
                          queries = queries,
                          proto_prefix = "couchbase")
        self.gated_finish(self.input.clients, notify)

    def test_ept_mixed_original(self):
        self.spec("EPT-MIXED-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.08),
                          ratio_deletes = self.paramf('ratio_deletes', 0.13),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)

    # This is a small version of test_ept_mixed_original with fewer creates.
    def test_ept_mixed_original_1(self):
        self.spec("EPT-MIXED-original_1")
        items = self.parami("items", 22500000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.08),
                          ratio_deletes = self.paramf('ratio_deletes', 0.13),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 15000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_rebalance_low_original(self):
        self.spec("EPT-REBALANCE-LOW-FETCH-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        rebalance_after = self.parami("rebalance_after", 2000000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.08),
                          ratio_deletes = self.paramf('ratio_deletes', 0.13),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_rebalance_med_original(self):
        self.spec("EPT-REBALANCE-MED-FETCH-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        rebalance_after = self.parami("rebalance_after", 2000000)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                 getattr(self, "latched_rebalance"))]
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.08),
                          ratio_deletes = self.paramf('ratio_deletes', 0.13),
                          ratio_hot = self.paramf('ratio_hot', 0.6),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.parami('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000))
        self.gated_finish(self.input.clients, notify)

    def test_ept_mixed_nocompact_original(self):
        self.spec("EPT-MIXED-NOCOMPACT-original")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        if self.is_master:
            self.rest.set_autoCompaction("false", 100, 100) # 100% fragmentation thresholds.
        self.load_phase(self.parami("num_nodes", 10), items)
        # Read:Insert:Update:Delete Ratio = 50:4:40:6.
        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.5),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.08),
                          ratio_deletes = self.paramf('ratio_deletes', 0.13),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000))
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

    def test_ept_scaled_down_write(self):
        super(EPerfClient, self).test_ept_scaled_down_write_1()

    def test_ept_scaled_down_write_no_dgm(self):
        super(EPerfClient, self).test_ept_scaled_down_write_no_dgm_1()

    def test_ept_all_in_memory(self):
        super(EPerfClient, self).test_ept_all_in_memory()

def params_to_str(params):
    param_str = ""
    if params is not None:
        for k,v in params.items():
            param_str += "&{0}={1}".format(k,v)
    return param_str

def create_view_function(rest, view, function, bucket="default", reduce=''):

    #if this view already exist then get the rev and embed it here ?
    dict = {"language": "javascript",
            "_id": "_design/{0}".format(view)}
    try:
        view_response = rest.get_view(bucket, view)
        if view_response:
            dict["_rev"] = view_response["_rev"]
    except:
        pass
    if reduce:
        dict["views"] = {view: {"map": function, "reduce": reduce}}
    else:
        dict["views"] = {view: {"map": function}}
    return json.dumps(dict)

