from TestInput import TestInputSingleton
import logger
import time
import copy
import sys

import unittest
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from couchbase.cluster import Cluster
from couchbase.documentgenerator import BlobGenerator, DocumentGenerator

DEFAULT_KEY_COUNT = 1000
DEFAULT_REPLICA = 1


class FailoverBaseTest(unittest.TestCase):

    @staticmethod
    def setUp(self):
        log = logger.Logger.get_logger()
        self._input = TestInputSingleton.input
        self._keys_count = self._input.param("keys_count", DEFAULT_KEY_COUNT)
        self._num_replicas = self._input.param("replica", DEFAULT_REPLICA)
        self.bidirectional = self._input.param("bidirectional", False)
        self.case_number = self._input.param("case_number", 0)
        self._value_size = self._input.param("value_size", 256)
        self._servers = self._input.servers
        self.master = self._servers[0]
        self._failed_nodes = []
        num_buckets = 0
        self.buckets = []
        self.default_bucket = self._input.param("default_bucket", True)
        if self.default_bucket:
            self.default_bucket_name = "default"
            num_buckets += 1
        self._standard_buckets = self._input.param("standard_buckets", 0)
        self._sasl_buckets = self._input.param("sasl_buckets", 0)
        num_buckets += self._standard_buckets + self._sasl_buckets
        self.dgm_run = self._input.param("dgm_run", True)
        self.log = logger.Logger().get_logger()
        self._cluster_helper = Cluster()
        self.disabled_consistent_view = self._input.param("disabled_consistent_view", None)
        self._quota = self._initialize_nodes(self._cluster_helper, self._servers, self.disabled_consistent_view)
        if self.dgm_run:
            self.quota = 256
        self.bucket_size = int((2.0 / 3.0) / float(num_buckets) * float(self._quota))
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self._value_size, end=self._keys_count)
        self.add_back_flag = False
        self._cleanup_nodes = []
        log.info("==============  setup was started for test #{0} {1}=============="\
                      .format(self.case_number, self._testMethodName))
        RemoteUtilHelper.common_basic_setup(self._servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)
        for server in self._servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterHelper.wait_for_ns_servers_or_assert(self._servers, self)
        self._setup_cluster()
        self._create_buckets_()
        log.info("==============  setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))

    @staticmethod
    def tearDown(self):
        try:
            log = logger.Logger.get_logger()
            log.info("==============  tearDown was started for test #{0} {1} =============="\
                              .format(self.case_number, self._testMethodName))
            RemoteUtilHelper.common_basic_setup(self._servers)
            log.info("10 seconds delay to wait for membase-server to start")
            time.sleep(10)
            for server in self._cleanup_nodes:
                shell = RemoteMachineShellConnection(server)
                o, r = shell.execute_command("iptables -F")
                shell.log_command_output(o, r)
                o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j ACCEPT")
                shell.log_command_output(o, r)
                o, r = shell.execute_command("/sbin/iptables -A OUTPUT -p tcp -o eth0 --dport 1000:60000 -j ACCEPT")
                shell.log_command_output(o, r)
                shell.log_command_output(o, r)
                o, r = shell.execute_command("/etc/init.d/couchbase-server start")
                shell.log_command_output(o, r)
            BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)
            ClusterOperationHelper.cleanup_cluster(self._servers)
            ClusterHelper.wait_for_ns_servers_or_assert(self._servers, self)
            log.info("==============  tearDown was finished for test #{0} {1} =============="\
                              .format(self.case_number, self._testMethodName))
        finally:
            sys.exit(0)

    def _initialize_nodes(self, cluster, servers, disabled_consistent_view=None):
        quota = 0
        init_tasks = []
        for server in servers:
            init_tasks.append(cluster.async_init_node(server, disabled_consistent_view))
        for task in init_tasks:
            node_quota = task.result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        return quota

    def _setup_cluster(self):
        rest = RestConnection(self.master)
        credentials = self._input.membase_settings
        ClusterOperationHelper.add_all_nodes_or_assert(self.master, self._servers, credentials, self)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)

    def _create_buckets_(self):
        cluster = Cluster()
        if self.default_bucket:
            cluster.create_default_bucket(self.master, self.bucket_size, self._num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self._num_replicas, bucket_size=self.bucket_size))

        self._create_sasl_buckets(self.master, self._sasl_buckets)
        self._create_standard_buckets(self.master, self._standard_buckets)

    def _create_sasl_buckets(self, server, num_buckets):
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'bucket' + str(i)
            bucket_tasks.append(self._cluster_helper.async_create_sasl_bucket(server, name,
                                                                      'password',
                                                                      self.bucket_size,
                                                                      self._num_replicas))
            self.buckets.append(Bucket(name=name, authType="sasl", saslPassword='password',
                                       num_replicas=self._num_replicas, bucket_size=self.bucket_size));
        for task in bucket_tasks:
            task.result()

    def _create_standard_buckets(self, server, num_buckets):
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'standard_bucket' + str(i)
            bucket_tasks.append(self._cluster_helper.async_create_standard_bucket(server, name,
                                                                          11214 + i,
                                                                          self.bucket_size,
                                                                          self._num_replicas))

            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None, num_replicas=self._num_replicas,
                                       bucket_size=self.bucket_size, port=11214 + i));
        for task in bucket_tasks:
            task.result()

    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=30):
        tasks = []
        for bucket in self.buckets:
            gen = copy.deepcopy(kv_gen)
            tasks.append(self._cluster_helper.async_load_gen_docs(server, bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp, flag, only_store_hash, batch_size, pause_secs, timeout_secs))
        return tasks

    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=30):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store, flag, only_store_hash, batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()

    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                tasks.append(self._cluster_helper.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', '==', 0))
                tasks.append(self._cluster_helper.async_wait_for_stats([server], bucket, '',
                                   'ep_flusher_todo', '==', 0))
        for task in tasks:
            task.result()

    def _verify_all_buckets(self, server, kv_store=1, timeout=180, max_verify=None, only_store_hash=True, batch_size=1):
        tasks = []
        for bucket in self.buckets:
            tasks.append(self._cluster_helper.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify, only_store_hash, batch_size))
        for task in tasks:
            task.result(timeout)

    def _verify_stats_all_buckets(self, servers):
        stats_tasks = []
        for bucket in self.buckets:
            items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
                               'curr_items', '==', items))
            stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
                               'vb_active_curr_items', '==', items))

            available_replicas = self._num_replicas
            if len(servers) == self._num_replicas:
                available_replicas = len(servers) - 1
            elif len(servers) <= self._num_replicas:
                available_replicas = len(servers) - 1

            stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
                                   'vb_replica_curr_items', '==', items * available_replicas))
            stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
                                   'curr_items_tot', '==', items * (available_replicas + 1)))

        for task in stats_tasks:
            task.result(60)

class FailoverTests(FailoverBaseTest):
    def setUp(self):
        super(FailoverTests, self).setUp(self)

    def tearDown(self):
        super(FailoverTests, self).tearDown(self)

    def test_failover_firewall(self):
        self.common_test_body(self._keys_count, self._num_replicas, 'firewall')

    def test_failover_normal(self):
        self.common_test_body(self._keys_count, self._num_replicas, 'normal')

    def test_failover_stop_server(self):
        self.common_test_body(self._keys_count, self._num_replicas, 'stop_server')

    def test_failover_then_add_back(self):
        self.add_back_flag = True
        self.common_test_body(self._keys_count, self._num_replicas, 'normal')

    def common_test_body(self, keys_count, replica, failover_reason):
        log = logger.Logger.get_logger()
        log.info("keys_count : {0}".format(keys_count))
        log.info("replicas : {0}".format(replica))
        log.info("failover_reason : {0}".format(failover_reason))
        log.info('picking server : {0} as the master'.format(self.master))

        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self._servers)

        _servers_ = self._servers
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        
        final_replication_state = RestHelper(rest).wait_for_replication(900)
        msg = "replication state after waiting for up to 15 minutes : {0}"
        self.log.info(msg.format(final_replication_state))
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=replica)
        for node in chosen:
            #let's do op
            if failover_reason == 'stop_server':
                self.stop_server(node)
                log.info("10 seconds delay to wait for membase-server to shutdown")
                #wait for 5 minutes until node is down
                self.assertTrue(RestHelper(rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")
            elif failover_reason == "firewall":
                RemoteUtilHelper.enable_firewall(self._servers, node, bidirectional=self.bidirectional)
                self.assertTrue(RestHelper(rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")

            failed_over = rest.fail_over(node.id)
            if not failed_over:
                self.log.info("unable to failover the node the first time. try again in  60 seconds..")
                #try again in 75 seconds
                time.sleep(75)
                failed_over = rest.fail_over(node.id)
            self.assertTrue(failed_over, "unable to failover node after {0}".format(failover_reason))
            log.info("failed over node : {0}".format(node.id))
            self._failed_nodes.append(node)

        if self.add_back_flag:
            for node in self._failed_nodes:
                rest.add_back_node(node.id)
                time.sleep(5)
            log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[])
            msg = "rebalance failed while removing failover nodes {0}".format(chosen)
            self.assertTrue(rest.monitorRebalance(stop_if_loop=True), msg=msg)
        else:
            log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[node.id for node in chosen])
            msg = "rebalance failed while removing failover nodes {0}".format(chosen)
            self.assertTrue(rest.monitorRebalance(stop_if_loop=True), msg=msg)
            for failed in chosen:
                for server in _servers_:
                    if server.ip == failed.ip:
                         _servers_.remove(server)
                         self._cleanup_nodes.append(server)

        log.info("Begin VERIFICATION ...")
        self._verify_stats_all_buckets(_servers_)
        self._verify_all_buckets(self.master, batch_size=10000)


    def stop_server(self, node):
        log = logger.Logger.get_logger()
        for server in self._servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    log.info("Membase stopped")
                shell.disconnect()
                break
