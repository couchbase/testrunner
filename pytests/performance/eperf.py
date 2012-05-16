"""
Executive performance dashboard tests.
"""

# general imports
import logger
import time
import json
import os
import gzip
import ast
import copy
import mcsoda
import threading

# membase imports
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.performance.stats import StatsCollector, CallbackStatsCollector
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper

# testrunner imports
from TestInput import TestInputSingleton
from perf_defaults import PerfDefaults
import perf

class EPerfMaster(perf.PerfBase):
    specURL = "http://hub.internal.couchbase.org/confluence/pages/viewpage.action?pageId=1901816"

    def setUp(self):
        self.dgm = False
        self.is_master = True
        self.input = TestInputSingleton.input
        self.mem_quota = self.parami("mem_quota", PerfDefaults.mem_quota) # for 10G system
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

    def merge_dict(self, output, input):
        """
        Merge two dicts. Add values together if key exists in both dicts.
        """
        if output is None or not output:
            return input

        if input is None or not input:
            return output

        for key, value in input.iteritems():
            if key in output.keys():
                output[key] += value
            else:
                output[key] = value

        return output

    def aggregate_all_stats(self, len_clients, type="loop"):
        i = 0

        # read [client #].[type].json.gz file until find one
        file = None
        while i < len_clients:
            try:
                file = gzip.open("{0}.{1}.json.gz".format(i, type), 'rb')
                break
            except IOError as e:
                print "[stats aggregation error] cannot open file : {0}.{1}.json.gz".format(i, type)
                i += 1

        if file is None:
            return

        final_json = file.read()
        file.close()
        final_json = json.loads(final_json)
        i += 1
        merge_keys = []
        for latency in final_json.keys():
             if latency.startswith('latency'):
                 merge_keys.append(str(latency))

        for i in range(i, len_clients):
             try:
                file  = gzip.open("{0}.{1}.json.gz".format(i, type),'rb')
             except IOError as e:
                # cannot find stats produced by this client, check stats collection.
                # the results might be incomplete.
                print "[stats aggregation error] cannot open file : {0}.{1}.json.gz".format(i, type)
                continue

             dict = file.read()
             file.close()
             dict = json.loads(dict)
             for key, value in dict.items():
                 if key in merge_keys:
                     if key.endswith("histogram"):
                         self.merge_dict(final_json[key], value)
                         continue
                     final_json[key].extend(value)

        file = gzip.open("{0}.{1}.json.gz".format('final', type), 'wb')
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
        """
        @deprecate
        Remove when we delete test functions.
        No useful? we are calling setupBased1() on SetUp() in perf.py
        """
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
                      protocol=self.mk_protocol(host=self.input.servers[0].ip,
                                                port=self.input.servers[0].port),
                      use_direct=self.parami('use_direct', 1),
                      doc_cache=self.parami('doc_cache', 0),
                      prefix="",
                      start_at=start_at,
                      is_eperf=True)
            self.restartProxy()

    def access_phase_clients_start_at(self):
        self.access_phase_items = self.parami("items", PerfDefaults.items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        start_at = int(self.paramf("start_at", 1.0) * \
                           (self.parami("prefix", 0) * \
                                self.access_phase_items / num_clients))
        return num_clients, start_at

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
                     proto_prefix   = "membase-binary",
                     host           = None):
        if self.parami("access_phase", 1) > 0:
            print "Accessing"
            items = self.parami("items", items)
            num_clients, start_at = self.access_phase_clients_start_at()
            start_delay = self.parami("start_delay", 2 * 60) # 2 minute delay.
            if start_delay > 0:
                time.sleep(start_delay * self.parami("prefix", 0))
            max_creates = self.parami("max_creates", max_creates) / num_clients
            self.is_multi_node = False
            if host is None:
                host = self.input.servers[0].ip
            port = self.input.servers[0].port
            self.loop(num_ops        = 0,
                      num_items      = items,
                      max_items      = items + max_creates + 1,
                      max_creates    = max_creates,
                      min_value_size = self.param('size', self.min_value_size()),
                      kind           = self.param('kind', 'json'),
                      protocol       = self.mk_protocol(host, port, proto_prefix),
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
                      report         = self.parami('report', int(max_creates * 0.1)),
                      exit_after_creates = self.parami('exit_after_creates', 1),
                      hot_shift = self.parami('hot_shift', hot_shift),
                      is_eperf=True,
                      ratio_queries = ratio_queries,
                      queries = queries)

    # create design docs and index documents
    def index_phase(self, ddocs, bucket="default"):
        if self.parami("index_phase", 1) > 0:
            for ddoc_name, d in ddocs.items():
                d = copy.copy(d)
                d["language"] = "javascript"
                d["_id"] = "_design/" + ddoc_name
                d_json = json.dumps(d)
                api = "{0}couchBase/{1}/_design/{2}".format(self.rest.baseUrl,
                                                            bucket, ddoc_name)
                self.rest._http_request(api, 'PUT', d_json,
                                        headers=self.rest._create_capi_headers())
            for ddoc_name, d in ddocs.items():
                for view_name, x in d["views"].items():
                    self.rest.query_view(ddoc_name, view_name, bucket, { "limit": 10 })

    def latched_rebalance(self, cur):
        if not self.latched_rebalance_done:
            self.latched_rebalance_done = True
            self.delayed_rebalance(self.parami("num_nodes_after", PerfDefaults.num_nodes_after), 0.01)

    # ---------------------------------------------

    def test_eperf_read(self):
        """
        Eperf read test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_read")
        items = self.parami("items", PerfDefaults.items)
        self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes), items)
        if self.parami("access_phase", 1) == 1:
            self.access_phase(items,
                              ratio_sets     = self.paramf('ratio_sets', PerfDefaults.ratio_sets),
                              ratio_misses   = self.paramf('ratio_misses', PerfDefaults.ratio_misses),
                              ratio_creates  = self.paramf('ratio_creates', PerfDefaults.ratio_creates),
                              ratio_deletes  = self.paramf('ratio_deletes', PerfDefaults.ratio_deletes),
                              ratio_hot      = self.paramf('ratio_hot', PerfDefaults.ratio_hot),
                              ratio_hot_gets = self.paramf('ratio_hot_gets', PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets = self.paramf('ratio_hot_sets', PerfDefaults.ratio_hot_sets),
                              ratio_expirations = self.paramf('ratio_expirations', PerfDefaults.ratio_expirations),
                              max_creates    = self.parami("max_creates", PerfDefaults.max_creates))

    def test_eperf_write(self):
        """
        Eperf write test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_write")
        items = self.parami("items", PerfDefaults.items)
        self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes), items)
        if self.parami("access_phase", 1) == 1:
            self.access_phase(items,
                              ratio_sets     = self.paramf('ratio_sets', PerfDefaults.ratio_sets),
                              ratio_misses   = self.paramf('ratio_misses', PerfDefaults.ratio_misses),
                              ratio_creates  = self.paramf('ratio_creates', PerfDefaults.ratio_creates),
                              ratio_deletes  = self.paramf('ratio_deletes', PerfDefaults.ratio_deletes),
                              ratio_hot      = self.paramf('ratio_hot', PerfDefaults.ratio_hot),
                              ratio_hot_gets = self.paramf('ratio_hot_gets', PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets = self.paramf('ratio_hot_sets', PerfDefaults.ratio_hot_sets),
                              ratio_expirations = self.paramf('ratio_expirations', PerfDefaults.ratio_expirations),
                              max_creates    = self.parami("max_creates", PerfDefaults.max_creates))

    def test_eperf_mixed(self):
        """
        Eperf mixed test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_mixed")
        items = self.parami("items", PerfDefaults.items)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes), items)
        if self.parami("access_phase", 1) == 1:
            self.access_phase(items,
                              ratio_sets     = self.paramf('ratio_sets', PerfDefaults.ratio_sets),
                              ratio_misses   = self.paramf('ratio_misses', PerfDefaults.ratio_misses),
                              ratio_creates  = self.paramf('ratio_creates', PerfDefaults.ratio_creates),
                              ratio_deletes  = self.paramf('ratio_deletes', PerfDefaults.ratio_deletes),
                              ratio_hot      = self.paramf('ratio_hot', PerfDefaults.ratio_hot),
                              ratio_hot_gets = self.paramf('ratio_hot_gets', PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets = self.paramf('ratio_hot_sets', PerfDefaults.ratio_hot_sets),
                              ratio_expirations = self.paramf('ratio_expirations', PerfDefaults.ratio_expirations),
                              max_creates    = self.parami("max_creates", PerfDefaults.max_creates))

    def test_eperf_rebalance(self):
        """
        Eperf rebalance test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_rebalance")
        items = self.parami("items", PerfDefaults.items)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes), items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        rebalance_after = self.parami("rebalance_after", PerfDefaults.rebalance_after)
        self.level_callbacks = [('cur-creates', rebalance_after / num_clients,
                                getattr(self, "latched_rebalance"))]
        if self.parami("access_phase", 1) == 1:
            self.access_phase(items,
                              ratio_sets     = self.paramf('ratio_sets', PerfDefaults.ratio_sets),
                              ratio_misses   = self.paramf('ratio_misses', PerfDefaults.ratio_misses),
                              ratio_creates  = self.paramf('ratio_creates', PerfDefaults.ratio_creates),
                              ratio_deletes  = self.paramf('ratio_deletes', PerfDefaults.ratio_deletes),
                              ratio_hot      = self.paramf('ratio_hot', PerfDefaults.ratio_hot),
                              ratio_hot_gets = self.paramf('ratio_hot_gets', PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets = self.paramf('ratio_hot_sets', PerfDefaults.ratio_hot_sets),
                              ratio_expirations = self.paramf('ratio_expirations', PerfDefaults.ratio_expirations),
                              max_creates    = self.parami("max_creates", PerfDefaults.max_creates))

    # -- read, write and rebalance tests below this line need to be replaced by conf files ---

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

    def test_evperf_workload1(self):
        self.spec("evperf_workload1")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)

        # self.index_phase(...) # Skip indexing because we use _all_docs, not a secondary index.

        view = self.param("view", "/default/_all_docs")
        limit = self.parami("limit", 10)
        queries = view + "?limit=" + str(limit) + '&startkey="{key}"'

        # Hot-Keys : 20% of total keys
        #
        # Read    50% - 95% of GET's should hit a hot key
        # Insert  10% - N/A
        # Update  15% - 95% of mutates should mutate a hot key
        # Delete  5%  - 95% of the deletes should delete a hot key
        # Queries 20% - A mix of queries on the _all_docs index. Queries may or may not hit hot keys.

        self.bg_max_ops_per_sec = self.parami("bg_max_ops_per_sec", 100)
        self.fg_max_ops = self.parami("fg_max_ops", 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami("prefix", 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.2857),
                          queries = queries,
                          proto_prefix = "couchbase",
                          host = host)
        self.gated_finish(self.input.clients, notify)

    def test_evperf_workload2(self):
        self.spec("evperf_workload2")
        items = self.parami("items", 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        ddocs = {}
        ddocs["A"] = { "views": {} }
        ddocs["A"]["views"]["city"] = {}
        ddocs["A"]["views"]["city"]["map"] = """
function(doc) {
  if (doc.city != null) {
    emit(doc.city, null);
  }
}
"""
        ddocs["A"]["views"]["city2"] = {}
        ddocs["A"]["views"]["city2"]["map"] = """
function(doc) {
  if (doc.city != null) {
    emit(doc.city, ["Name:" + doc.name, "E-mail:" + doc.email]);
  }
}
"""
        ddocs["B"] = { "views": {} }
        ddocs["B"]["views"]["realm"] = {}
        ddocs["B"]["views"]["realm"]["map"] = """
function(doc) {
  if (doc.realm != null) {
    emit(doc.realm, null);
  }
}
"""
        ddocs["B"]["views"]["experts"] = {}
        ddocs["B"]["views"]["experts"]["map"] = """
function(doc) {
  if (doc.category == 2) {
    emit([doc.name, doc.coins], null);
  }
}
"""
        ddocs["C"] = { "views": {} }
        ddocs["C"]["views"]["experts"] = {}
        ddocs["C"]["views"]["experts"]["map"] = """
function(doc) {
  emit([doc.category, doc.coins], null);
}
"""
        ddocs["C"]["views"]["realm"] = {}
        ddocs["C"]["views"]["realm"]["map"] = """
function(doc) {
  emit([doc.realm, doc.coins], null)
}
"""
        ddocs["C"]["views"]["realm2"] = {}
        ddocs["C"]["views"]["realm2"]["map"] = """
function(doc) {
  emit([doc.realm, doc.coins], [doc._id,doc.name,doc.email]);
}
"""
        ddocs["C"]["views"]["category"] = {}
        ddocs["C"]["views"]["category"]["map"] = """
function(doc) {
  emit([doc.category, doc.realm, doc.coins], [doc._id,doc.name,doc.email]);
}
"""
        self.index_phase(ddocs)

        limit = self.parami("limit", 10)

        b = '/default/'

        q = {}
        q['all_docs']    = b + '_all_docs?limit=' + str(limit) + '&startkey="{key}"'
        q['city']        = b + '_design/A/_view/city?limit=' + str(limit) + '&startkey="{city}"'
        q['city2']       = b + '_design/A/_view/city2?limit=' + str(limit) + '&startkey="{city}"'
        q['realm']       = b + '_design/B/_view/realm?limit=30&startkey="{realm}"'
        q['experts']     = b + '_design/B/_view/experts?limit=30&startkey="{name}"'
        q['coins-beg']   = b + '_design/C/_view/experts?limit=30&startkey=[0,{int10}]&endkey=[0,{int100}]'
        q['coins-exp']   = b + '_design/C/_view/experts?limit=30&startkey=[2,{int10}]&endkey=[2,{int100}]'
        q['and0']        = b + '_design/C/_view/realm?limit=30&startkey=["{realm}",{coins}]'
        q['and1']        = b + '_design/C/_view/realm?limit=30&startkey=["{realm}",{coins}]'
        q['and2']        = b + '_design/C/_view/category?limit=30&startkey=[0,"{realm}",{coins}]'

        queries_by_kind = [
            [ # 5
                q['all_docs']
                ],
            [ # 45% / 5 = 9
                q['city'],
                q['city2'],
                q['realm'],
                q['experts']
                ],
            [ # 30% / 5 = 6
                q['coins-beg'],
                q['coins-exp']
                ],
            [ # 25% / 5 = 5
                q['and0'],
                q['and1'],
                q['and2'],
                ]
            ]

        remaining = [5, 9, 6, 5]

        queries = compute_queries(queries_by_kind, remaining,
                                  self.param("query_suffix", ""))
        queries = join_queries(queries)

        self.bg_max_ops_per_sec = self.parami("bg_max_ops_per_sec", 100)
        self.fg_max_ops = self.parami("fg_max_ops", 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami("prefix", 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.3571),
                          queries = queries,
                          proto_prefix = "couchbase",
                          host = host)
        self.gated_finish(self.input.clients, notify)

    def test_evperf_workload3(self):
        """Like workload 2 but has queries on view with reduce step"""

        self.spec('evperf_workload3')

        """Load phase"""
        items = self.parami('items', 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami('num_nodes', 10), items)

        """Index phase"""
        view_gen = ViewGen()
        ddocs = view_gen.generate_ddocs([2, 2, 4], add_reduce=True)
        self.index_phase(ddocs)

        """Access phase"""
        limit = self.parami('limit', 10)
        query_suffix = self.param('query_suffix', '')
        queries = view_gen.generate_queries(limit, query_suffix, ddocs,
                                            use_all_docs=True,
                                            use_reduce=True)

        self.bg_max_ops_per_sec = self.parami('bg_max_ops_per_sec', 100)
        self.fg_max_ops = self.parami('fg_max_ops', 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami('prefix', 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami('max_creates', 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.3571),
                          queries = queries,
                          proto_prefix = 'couchbase',
                          host = host)

        self.gated_finish(self.input.clients, notify)

    def test_vperf1(self):
        # Like test_workload2, but just one design doc, one view.
        self.spec("vperf1")
        items = self.parami("items", 1000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami("num_nodes", 10), items)
        ddocs = {}
        ddocs["A"] = { "views": {} }
        ddocs["A"]["views"]["city"] = {}
        ddocs["A"]["views"]["city"]["map"] = """
function(doc) {
  if (doc.city != null) {
    emit(doc.city, null);
  }
}
"""
        self.index_phase(ddocs)

        limit = self.parami("limit", 10)

        b = '/default/'

        q = {}
        q['city'] = b + '_design/A/_view/city?limit=' + str(limit) + '&startkey="{city}"'

        queries_by_kind = [ [ q['city'] ] ]

        remaining = [1]

        queries = compute_queries(queries_by_kind, remaining,
                                  self.param("query_suffix", ""))
        queries = join_queries(queries)

        self.bg_max_ops_per_sec = self.parami("bg_max_ops_per_sec", 100)
        self.fg_max_ops = self.parami("fg_max_ops", 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami("prefix", 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.3571),
                          queries = queries,
                          proto_prefix = "couchbase",
                          host = host)
        self.gated_finish(self.input.clients, notify)

    def test_vperf2(self):
        """1 design document, 8 views"""

        self.spec("vperf2")

        # Load phase
        items = self.parami('items', 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami('num_nodes', 10), items)

        # Index phase
        view_gen = ViewGen()
        ddocs = view_gen.generate_ddocs([8])
        self.index_phase(ddocs)

        # Access phase
        limit = self.parami('limit', 10)
        query_suffix = self.param("query_suffix", "")
        queries = view_gen.generate_queries(limit, query_suffix, ddocs, use_all_docs=False)

        self.bg_max_ops_per_sec = self.parami('bg_max_ops_per_sec', 100)
        self.fg_max_ops = self.parami('fg_max_ops', 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami('prefix', 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.3571),
                          queries = queries,
                          proto_prefix = "couchbase",
                          host = host)
        self.gated_finish(self.input.clients, notify)

    def test_vperf3(self):
        """8 design documents, 1 view"""

        self.spec("vperf3")

        # Load phase
        items = self.parami('items', 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami('num_nodes', 10), items)

        # Index phase
        view_gen = ViewGen()
        ddocs = view_gen.generate_ddocs([1, 1, 1, 1, 1, 1, 1, 1])
        self.index_phase(ddocs)

        # Access phase
        limit = self.parami('limit', 10)
        query_suffix = self.param("query_suffix", "")
        queries = view_gen.generate_queries(limit, query_suffix, ddocs, use_all_docs=False)

        self.bg_max_ops_per_sec = self.parami('bg_max_ops_per_sec', 100)
        self.fg_max_ops = self.parami('fg_max_ops', 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami('prefix', 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.3571),
                          queries = queries,
                          proto_prefix = "couchbase",
                          host = host)
        self.gated_finish(self.input.clients, notify)

    def test_vperf4(self):
        """Like workload 2 but without queries on primary index"""

        self.spec("vperf4")

        # Load phase
        items = self.parami('items', 45000000)
        notify = self.gated_start(self.input.clients)
        self.load_phase(self.parami('num_nodes', 10), items)

        # Index phase
        view_gen = ViewGen()
        ddocs = view_gen.generate_ddocs([2, 2, 4])
        self.index_phase(ddocs)

        # Access phase
        limit = self.parami('limit', 10)
        query_suffix = self.param("query_suffix", "")
        queries = view_gen.generate_queries(limit, query_suffix, ddocs,
                                            use_all_docs=False,
                                            use_reduce=False)

        self.bg_max_ops_per_sec = self.parami('bg_max_ops_per_sec', 100)
        self.fg_max_ops = self.parami('fg_max_ops', 1000000)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        host = self.input.servers[self.parami('prefix', 0) % len(self.input.servers)].ip

        self.access_phase(items,
                          ratio_sets = self.paramf('ratio_sets', 0.3),
                          ratio_misses = self.paramf('ratio_misses', 0.05),
                          ratio_creates = self.paramf('ratio_creates', 0.33),
                          ratio_deletes = self.paramf('ratio_deletes', 0.25),
                          ratio_hot = self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations = self.paramf('ratio_expirations', 0.03),
                          max_creates = self.parami("max_creates", 30000000),
                          ratio_queries = self.paramf('ratio_queries', 0.3571),
                          queries = queries,
                          proto_prefix = "couchbase",
                          host = host)
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
        self.bg_max_ops_per_sec = 0
        self.fg_max_ops = 0

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

    def test_wait_until_drained(self):
        self.wait_until_drained()


# The EVPerfClient subclass deploys another background thread to drive
# a concurrent amount of memcached traffic, during the "loop" or
# access phase.  Otherwise, with the basic EPerfClient, the http
# queries would gate the ops/second performance due to mcsoda's
# lockstep batching.
#
class EVPerfClient(EPerfClient):

    def mcsoda_run(self, cfg, cur, protocol, host_port, user, pswd,
                   stats_collector = None, stores = None, ctl = None,
                   heartbeat = 0, why = None, bucket="default"):
        self.bg_thread = None
        self.bg_thread_ctl = None

        num_clients, start_at = self.access_phase_clients_start_at()

        # If non-zero background ops/second, the background thread,
        # with cloned parameters, doesn't do any queries, only basic
        # key-value ops.
        if why == "loop" and self.parami("bg_max_ops_per_sec", self.bg_max_ops_per_sec):
            self.bg_thread_cfg = copy.deepcopy(cfg)
            self.bg_thread_cfg['max-ops-per-sec'] = self.parami("bg_max_ops_per_sec",
                                                                self.bg_max_ops_per_sec)
            self.bg_thread_cfg['ratio-queries'] = self.paramf("bg_ratio_queries", 0.0)
            self.bg_thread_cfg['queries'] = self.param("bg_queries", "")
            self.bg_thread_cur = copy.deepcopy(cur)
            self.bg_thread_ctl = { 'run_ok': True }

            from cbsoda import StoreCouchbase

            self.bg_stores = [StoreCouchbase()]

            self.bg_thread = threading.Thread(target=mcsoda.run,
                                              args=(self.bg_thread_cfg,
                                                    self.bg_thread_cur,
                                                    protocol, host_port, user, pswd,
                                                    stats_collector, self.bg_stores, self.bg_thread_ctl,
                                                    heartbeat, "loop-bg"))
            self.bg_thread.daemon = True
            self.bg_thread.start()

            # Also, the main mcsoda run should do no memcached/key-value requests.
            cfg['ratio-sets'] = self.paramf("fg_ratio_sets", 0.0)
            cfg['ratio-queries'] = self.paramf("fg_ratio_queries", 1.0)

        if why == "loop" and self.parami("fg_max_ops", self.fg_max_ops):
            cfg['max-ops'] = start_at + (self.parami("fg_max_ops", self.fg_max_ops) / num_clients)

        cfg['stats_ops'] = self.parami("mcsoda_fg_stats_ops", PerfDefaults.mcsoda_fg_stats_ops)
        rv_cur, start_time, end_time = \
            super(EVPerfClient, self).mcsoda_run(cfg, cur, protocol, host_port, user, pswd,
                                                 stats_collector=stats_collector,
                                                 stores=stores,
                                                 ctl=ctl,
                                                 heartbeat=heartbeat,
                                                 why="loop-fg")
        if self.bg_thread_ctl:
            self.bg_thread_ctl['run_ok'] = False

        return rv_cur, start_time, end_time


def params_to_str(params):
    param_str = ""
    if params is not None:
        for k,v in params.items():
            param_str += "&{0}={1}".format(k,v)
    return param_str

def compute_queries(queries_by_kind, remaining, suffix=""):
    i = 0
    queries = []

    while remaining.count(0) < len(remaining):
        kind = i % len(remaining)
        count = remaining[kind]
        if count > 0:
            remaining[kind] = count - 1
            k = queries_by_kind[kind]
            queries.append(k[count % len(k)] + suffix)
        i = i + 1

    return queries

def join_queries(queries):
    queries = ';'.join(queries)
    queries = queries.replace('[', '%5B')
    queries = queries.replace(']', '%5D')
    queries = queries.replace(',', '%2C')
    return queries

class ViewGen:
    def __init__(self):
        self.ddoc_names = ["A", "B", "C", "D", "E", "F", "G", "H"]

        self.view_names = [
            'city1',
            'city2',
            'realm1',
            'experts1',
            'experts2',
            'realm2',
            'realm3',
            'category'
        ]

    def generate_ddocs(self, pattern=None, add_reduce=False):
        """Generate dictionary with design documents and views.
        Pattern looks like:
            [1] -- 1 ddoc (1 view)
            [2, 2, 4] -- 3 ddocs (2 views, 2 views, 4 views)
            [8] -- 1 ddoc (8 views)
            [1, 1, 1, 1, 1, 1, 1, 1] -- 8 ddocs (1 view)
        If `add_reduce` argument is True, additional ddoc with single
        map-reduce view is added
        """

        ddocs = dict()

        map_functions = [
            """
            function(doc) {
                if (doc.city != null) {
                    emit(doc.city, null);
                }
            }
            """,
            """
            function(doc) {
                if (doc.city != null) {
                    emit(doc.city, ["Name:" + doc.name, "E-mail:" + doc.email]);
                }
            }
            """,
            """
            function(doc) {
                if (doc.realm != null) {
                    emit(doc.realm, null);
                }
            }
            """,
            """
            function(doc) {
                if (doc.category == 2) {
                    emit([doc.name, doc.coins], null);
                }
            }
            """,
            """
            function(doc) {
                emit([doc.category, doc.coins], null);
            }
            """,
            """
            function(doc) {
                emit([doc.realm, doc.coins], null)
            }
            """,
            """
            function(doc) {
                emit([doc.realm, doc.coins], [doc._id,doc.name,doc.email]);
            }
            """,
            """
            function(doc) {
                emit([doc.category, doc.realm, doc.coins], [doc._id,doc.name,doc.email]);
            }
            """,
            """
            function (doc) {
                if (doc.achievements.length > 0) {
                    emit(doc.category, doc.coins);
                }
            }
            """
        ]

        reduce_functions = [
            "_count",
            "_sum",
            "_stats"
        ]

        index_of_map = 0
        index_of_ddoc = 0

        for number_of_views in pattern:
            ddoc_name = self.ddoc_names[index_of_ddoc]
            ddocs[ddoc_name] = {'views': {}}
            for index_of_view in range(number_of_views):
                view_name = self.view_names[index_of_map]
                ddocs[ddoc_name]['views'][view_name] = {}
                ddocs[ddoc_name]['views'][view_name]['map'] = map_functions[index_of_map]
                index_of_map += 1
            index_of_ddoc += 1

        if add_reduce:
            ddocs['reduce'] = {
                'views': {
                    'reduce': {
                        'map': map_functions[-1],
                        'reduce': reduce_functions[-1]
                    }
                }
            }

        return ddocs

    def generate_queries(self, limit, query_suffix, ddocs,
                         use_all_docs=False, use_reduce=False):
        """Generate string from permuted queries.

        Optional arguments:
        use_all_docs -- add query on primary index
        use_reduce -- add query on view with reduce step
        """

        ddoc_names = list()
        for ddoc_name, ddoc in sorted(ddocs.items()):
            for view in ddoc["views"]:
                ddoc_names.append(ddoc_name)

        b = '/default/'

        q = {}
        q['city']        = b + '_design/' + ddoc_names[0] + '/_view/city1?limit=' + str(limit) + '&startkey="{city}"'
        q['city2']       = b + '_design/' + ddoc_names[1] + '/_view/city2?limit=' + str(limit) + '&startkey="{city}"'
        q['realm']       = b + '_design/' + ddoc_names[2] + '/_view/realm1?limit=30&startkey="{realm}"'
        q['experts']     = b + '_design/' + ddoc_names[3] + '/_view/experts1?limit=30&startkey="{name}"'
        q['coins-beg']   = b + '_design/' + ddoc_names[4] + '/_view/experts2?limit=30&startkey=[0,{int10}]&endkey=[0,{int100}]'
        q['coins-exp']   = b + '_design/' + ddoc_names[4] + '/_view/experts2?limit=30&startkey=[2,{int10}]&endkey=[2,{int100}]'
        q['and0']        = b + '_design/' + ddoc_names[5] + '/_view/realm2?limit=30&startkey=["{realm}",{coins}]'
        q['and1']        = b + '_design/' + ddoc_names[6] + '/_view/realm3?limit=30&startkey=["{realm}",{coins}]'
        q['and2']        = b + '_design/' + ddoc_names[7] + '/_view/category?limit=30&startkey=[0,"{realm}",{coins}]'

        queries_by_kind = [
            [ # 45% / 5 = 9
                q['city'],
                q['city2'],
                q['realm'],
                q['experts']
                ],
            [ # 30% / 5 = 6
                q['coins-beg'],
                q['coins-exp']
                ],
            [ # 25% / 5 = 5
                q['and0'],
                q['and1'],
                q['and2'],
                ]
            ]

        remaining = [9, 6, 5]

        if use_all_docs:
            q['all_docs'] = b + '_all_docs?limit=' + str(limit) + '&startkey="{key}"'
            queries_by_kind = [[q['all_docs']]] + queries_by_kind
            remaining = [5] + remaining

        if use_reduce:
            q['reduce'] = b + '_design/reduce/_view/reduce?limit=' + str(limit)
            queries_by_kind = queries_by_kind + [[q['reduce']]]
            remaining = remaining + [5]

        queries = self.compute_queries(queries_by_kind, remaining, query_suffix)
        queries = self.join_queries(queries)

        return queries

    def compute_queries(self, queries_by_kind, remaining, suffix=""):
        """Return a list of permuted queries"""
        i = 0
        queries = []

        while remaining.count(0) < len(remaining):
            kind = i % len(remaining)
            count = remaining[kind]
            if count > 0:
                remaining[kind] = count - 1
                k = queries_by_kind[kind]
                queries.append(k[count % len(k)] + suffix)
            i = i + 1

        return queries

    def join_queries(self, queries):
        """Join queries into string"""
        queries = ';'.join(queries)
        queries = queries.replace('[', '%5B')
        queries = queries.replace(']', '%5D')
        queries = queries.replace(',', '%2C')
        return queries
