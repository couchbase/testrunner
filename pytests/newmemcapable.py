import copy
from basetestcase import BaseTestCase
from couchbase.documentgenerator import DocumentGenerator
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

class GetrTests(BaseTestCase):

    NO_REBALANCE = 0
    DURING_REBALANCE = 1
    AFTER_REBALANCE = 2

    def setUp(self):
        super(GetrTests, self).setUp()
        descr = self.input.param("descr", "")
        if descr:
            self.log.info("Test:{0}".format(descr))
        self.skipload = self.input.param("skipload", False)
        self.data_ops = self.input.param("data_ops", 'create')
        self.expiration = self.input.param("expiration", 0)
        self.wait_expiration = self.input.param("wait_expiration", False)
        self.flags = self.input.param("flags", 0)
        self.warmup_nodes = self.input.param("warmup", 0)
        self.rebalance = self.input.param("rebalance", 0)
        self.error = self.input.param("error", None)
        self.replica_to_read = self.input.param("replica_to_read", 0)

    def tearDown(self):
        super(GetrTests, self).tearDown()

    def getr_test(self):
        gen_1 = DocumentGenerator('test_docs', '{{"age": {0}}}', xrange(5),
                                      start=0, end=self.num_items/2)
        gen_2 = DocumentGenerator('test_docs', '{{"age": {0}}}', xrange(5),
                                      start=self.num_items/2, end=self.num_items)
        self.log.info("LOAD PHASE")
        if not self.skipload:
            self.perform_docs_ops(self.master, [gen_1, gen_2], self.data_ops)

        self.log.info("CLUSTER OPS PHASE")
        if self.rebalance == GetrTests.AFTER_REBALANCE:
            self.cluster.rebalance(self.servers[:self.nodes_init],
                                   self.servers[self.nodes_init:], [])
        if self.rebalance == GetrTests.DURING_REBALANCE:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                            self.servers[self.nodes_init : self.nodes_init + self.nodes_in],
                            [])
        if self.warmup_nodes:
            self.perform_warm_up()
        if self.wait_expiration:
            self.sleep(self.expiration)
        try:
            self.log.info("READ REPLICA PHASE")
            self.verify_cluster_stats(self.servers[:self.nodes_init], only_store_hash=False,
                                      replica_to_read=self.replica_to_read)
        except Exception, ex:
            if self.error and str(ex).find(self.error) != -1:
                self.log.info("Expected error %s appeared as expected" % self.error)
            else:
                raise ex
        if self.rebalance == GetrTests.DURING_REBALANCE:
            rebalance.result()


    def perform_docs_ops(self, server, gens, op_type, kv_store=1, only_store_hash=False,
                         batch_size=1):
        for gen in gens:
            gen_ops = copy.deepcopy(gen)
            self._load_all_buckets(server, gen_ops, 'create', self.expiration, kv_store=kv_store,
                                  flag=self.flags, only_store_hash=only_store_hash, batch_size=batch_size)
        gen_ops = copy.deepcopy(gens[0])
        if self.data_ops == 'update':
            self._load_all_buckets(server, gen_ops, 'update', self.expiration, kv_store=kv_store,
                              flag=self.flags, only_store_hash=only_store_hash, batch_size=batch_size)
        if self.data_ops in ['delete', 'recreate']:
            self._load_all_buckets(server, gen_ops, 'delete', self.expiration, kv_store=kv_store,
                              flag=self.flags, only_store_hash=only_store_hash, batch_size=batch_size)
        if self.data_ops == 'recreate':
            self._load_all_buckets(server, gen_ops, 'create', self.expiration, kv_store=kv_store,
                              flag=self.flags, only_store_hash=only_store_hash, batch_size=batch_size)
        self.verify_cluster_stats(self.servers[:self.nodes_init], only_store_hash=only_store_hash)

    def perform_warm_up(self):
        warmup_nodes = self.servers[-self.warmup_nodes:]
        for warmup_node in warmup_nodes:
            shell = RemoteMachineShellConnection(warmup_node)
            shell.stop_couchbase()
            shell.disconnect()
        self.sleep(20)
        for warmup_node in warmup_nodes:
            shell = RemoteMachineShellConnection(warmup_node)
            shell.start_couchbase()
            shell.disconnect()
        ClusterOperationHelper.wait_for_ns_servers_or_assert(warmup_nodes, self)