import copy
import random
import unittest
import time
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper


import traceback

class GetrTests(BaseTestCase):

    DURING_REBALANCE = 1
    AFTER_REBALANCE = 2
    SWAP_REBALANCE = 3

    FAILOVER_NO_REBALANCE = 1
    FAILOVER_ADD_BACK = 2
    FAILOVER_REBALANCE = 3

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
        self.failover = self.input.param("failover", 0)
        self.failover_factor = self.input.param("failover-factor", 1)
        self.error = self.input.param("error", None)
        self.replica_to_read = self.input.param("replica_to_read", 0)
        self.value_size = self.input.param("value_size", 0)

    def tearDown(self):
        super(GetrTests, self).tearDown()

    def getr_test(self):
        if self.nodes_init > len(self.servers):
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.skipped=[('getr_test', "There is not enough VMs!!!")]
            return result

        gen_1 = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=0, end=self.num_items//2)
        gen_2 = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=self.num_items//2, end=self.num_items)
        if self.value_size:
            gen_1 = DocumentGenerator('test_docs', '{{"name": "{0}"}}',
                                      [self.value_size * 'a'],
                                      start=0, end=self.num_items//2)
            gen_2 = DocumentGenerator('test_docs', '{{"name": "{0}"}}',
                                      [self.value_size * 'a'],
                                      start=self.num_items//2, end=self.num_items)
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
        if self.rebalance == GetrTests.SWAP_REBALANCE:
            self.cluster.rebalance(self.servers[:self.nodes_init],
                                   self.servers[self.nodes_init :
                                                self.nodes_init + self.nodes_in],
                                   self.servers[self.nodes_init - self.nodes_in : self.nodes_init])
        if self.warmup_nodes:
            self.perform_warm_up()
        if self.failover:
            self.perform_failover()
        if self.wait_expiration:
            self.sleep(self.expiration)
        try:
            self.log.info("READ REPLICA PHASE")
            servrs = self.servers[:self.nodes_init]
            self.expire_pager(servrs)
            if self.failover in [GetrTests.FAILOVER_NO_REBALANCE, GetrTests.FAILOVER_REBALANCE]:
                servrs = self.servers[:self.nodes_init - self.failover_factor]
            if self.rebalance == GetrTests.AFTER_REBALANCE:
                servrs = self.servers
            if self.rebalance == GetrTests.SWAP_REBALANCE:
                servrs = self.servers[:self.nodes_init - self.nodes_in]
                servrs.extend(self.servers[self.nodes_init :
                                           self.nodes_init + self.nodes_in])

            self.log.info("Checking replica read")
            if self.failover == GetrTests.FAILOVER_NO_REBALANCE:
                self._verify_all_buckets(self.master, only_store_hash=False,
                                         replica_to_read=self.replica_to_read,
                                         batch_size=1)
            else:
                self.verify_cluster_stats(servrs, only_store_hash=False,
                                          replica_to_read=self.replica_to_read, batch_size=1,
                                          timeout=(self.wait_timeout * 10))
        except Exception as ex:
            if self.error and str(ex).find(self.error) != -1:
                self.log.info("Expected error %s appeared as expected" % self.error)
            else:
                print(traceback.format_exc())
                raise ex
        if self.rebalance == GetrTests.DURING_REBALANCE:
            rebalance.result()

    def getr_negative_test(self):
        gen_1 = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=0, end=self.num_items//2)
        gen_2 = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=self.num_items//2, end=self.num_items)
        self.log.info("LOAD PHASE")
        if not self.skipload:
            self.perform_docs_ops(self.master, [gen_1, gen_2], self.data_ops)

        if self.wait_expiration:
            self.sleep(self.expiration)

        self.log.info("READ REPLICA PHASE")
        self.log.info("Checking replica read")
        self.expire_pager([self.master])
        try:
            self._load_all_buckets(self.master, gen_1, 'read_replica', self.expiration, batch_size=1)
        except Exception as ex:
            if self.error and str(ex).find(self.error) != -1:
                self.log.info("Expected error %s appeared as expected" % self.error)
            else:
                raise ex
        else:
            if self.error:
                self.fail("Expected error %s didn't appear as expected" % self.error)

    def getr_negative_corrupted_keys_test(self):
        key = self.input.param("key", '')
        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=0, end=self.num_items)
        self.perform_docs_ops(self.master, [gen], 'create')
        self.log.info("Checking replica read")
        client = VBucketAwareMemcached(RestConnection(self.master), self.default_bucket_name)
        try:
            o, c, d = client.getr(key)
        except Exception as ex:
            if self.error and str(ex).find(self.error) != -1:
                self.log.info("Expected error %s appeared as expected" % self.error)
            else:
                raise ex
        else:
            if self.error:
                self.fail("Expected error %s didn't appear as expected" % self.error)

    def test_getr_bucket_ops(self):
        bucket_to_delete_same_read = self.input.param("bucket_to_delete_same_read", True)
        gen_1 = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=0, end=self.num_items)
        self.log.info("LOAD PHASE")
        self.perform_docs_ops(self.master, [gen_1], self.data_ops)

        self.log.info("Start bucket ops")
        bucket_read = self.buckets[0]
        bucket_delete = (self.buckets[1], self.buckets[0])[bucket_to_delete_same_read]
        try:
            self.log.info("READ REPLICA PHASE")
            self.log.info("Checking replica read")
            task_verify = self.cluster.async_verify_data(self.master, bucket_read,
                                                         bucket_read.kvs[1],
                                                         only_store_hash=False,
                                                         replica_to_read=self.replica_to_read,
                                                         compression=self.sdk_compression)
            task_delete_bucket = self.cluster.async_bucket_delete(self.master, bucket_delete.name)
            task_verify.result()
            task_delete_bucket.result()
        except Exception as ex:
            task_delete_bucket.result()
            if self.error and str(ex).find(self.error) != -1:
                self.log.info("Expected error %s appeared as expected" % self.error)
            else:
                raise ex
        else:
            if self.error:
                self.fail("Expected error %s didn't appear as expected" % self.error)

    def getr_rebalance_test(self):
        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                      start=0, end=self.num_items)
        self.perform_docs_ops(self.master, [gen], 'create')
        self.log.info("Checking replica read")
        client = VBucketAwareMemcached(RestConnection(self.master), self.default_bucket_name)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                            self.servers[self.nodes_init : self.nodes_init + self.nodes_in],
                            [])
        try:
            while gen.has_next():
                key, _ = next(gen)
                o, c, d = client.getr(key)
        finally:
            rebalance.result()

    def getr_negative_corrupted_vbucket_test(self):
        vbucket_state = self.input.param("vbucket_state", '')
        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                start=0, end=self.num_items)
        self.perform_docs_ops(self.master, [gen], 'create')
        self.log.info("Checking replica read")
        client = VBucketAwareMemcached(RestConnection(self.master), self.default_bucket_name)
        vbuckets_num = RestConnection(self.master).get_vbuckets(self.buckets[0])
        while gen.has_next():
            try:
                key, _ = next(gen)
                vBucketId = client._get_vBucket_id(key)
                mem = client.memcached_for_replica_vbucket(vBucketId)
                if vbucket_state:
                    mem.set_vbucket_state(vBucketId, vbucket_state)
                    msg = "Vbucket %s set to pending state" % vBucketId
                    mem_to_read = mem
                else:
                    wrong_vbucket = [v for v in client.vBucketMapReplica
                                   if mem.host != client.vBucketMapReplica[v][0].split(':')[0] or\
                                   str(mem.port) != client.vBucketMapReplica[v][0].split(':')[1]][0]
                    mem_to_read = client.memcached_for_replica_vbucket(wrong_vbucket)
                    msg = "Key: %s. Correct host is %s, test try to get from %s host. " %(
                                                        key, mem.host, mem_to_read.host)
                    msg += "Correct vbucket %s, wrong vbucket %s" % (vBucketId, wrong_vbucket)
                self.log.info(msg)
                client._send_op(mem_to_read.getr, key)
            except Exception as ex:
                if self.error and str(ex).find(self.error) != -1:
                    self.log.info("Expected error %s appeared as expected" % self.error)
                else:
                    raise ex
            else:
                if self.error:
                    self.fail("Expected error %s didn't appear as expected" % self.error)

    def getr_dgm_test(self):
        resident_ratio = self.input.param("resident_ratio", 50)
        gens = []
        delta_items = 200000
        self.num_items = 0
        mc = MemcachedClientHelper.direct_client(self.master, self.default_bucket_name)

        self.log.info("LOAD PHASE")
        end_time = time.time() + self.wait_timeout * 30
        while (int(mc.stats()["vb_active_perc_mem_resident"]) == 0 or\
               int(mc.stats()["vb_active_perc_mem_resident"]) > resident_ratio) and\
              time.time() < end_time:
            self.log.info("Resident ratio is %s" % mc.stats()["vb_active_perc_mem_resident"])
            gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(5),
                                    start=self.num_items, end=(self.num_items + delta_items))
            gens.append(copy.deepcopy(gen))
            self._load_all_buckets(self.master, gen, 'create', self.expiration, kv_store=1,
                                   flag=self.flags, only_store_hash=False, batch_size=1)
            self.num_items += delta_items
            self.log.info("Resident ratio is %s" % mc.stats()["vb_active_perc_mem_resident"])
        self.assertTrue(int(mc.stats()["vb_active_perc_mem_resident"]) < resident_ratio,
                        "Resident ratio is not reached")
        self.verify_cluster_stats(self.servers[:self.nodes_init], only_store_hash=False,
                                  batch_size=1)
        self.log.info("Currently loaded items: %s" % self.num_items)

        self.log.info("READ REPLICA PHASE")
        self.verify_cluster_stats(self.servers[:self.nodes_init], only_store_hash=False,
                                  replica_to_read=self.replica_to_read, batch_size=1)

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
        self.verify_cluster_stats(self.servers[:self.nodes_init], only_store_hash=only_store_hash,
                                  batch_size=batch_size, timeout=(self.wait_timeout * 10))

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

    def perform_failover(self):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        failover_servers = self.servers[:self.nodes_init][-self.failover_factor:]
        failover_nodes = []
        for server in failover_servers:
            for node in nodes:
                if node.ip == server.ip and str(node.port) == server.port:
                    failover_nodes.append(node)
        for node in failover_nodes:
            rest.fail_over(node.id)
            self.sleep(5)
        if self.failover == GetrTests.FAILOVER_REBALANCE:
            self.cluster.rebalance(self.servers[:self.nodes_init],
                               [], failover_servers)
        if self.failover == GetrTests.FAILOVER_ADD_BACK:
            for node in failover_nodes:
                rest.add_back_node(node.id)
            self.cluster.rebalance(self.servers[:self.nodes_init],
                                   [], [])
