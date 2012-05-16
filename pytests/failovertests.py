from TestInput import TestInputSingleton
import logger
import time

import unittest
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper

DEFAULT_LOAD_RATIO = 5
DEFAULT_REPLICA = 1


class FailoverBaseTest(unittest.TestCase):

    @staticmethod
    def common_setup(input, testcase):
        servers = input.servers
        RemoteUtilHelper.common_basic_setup(servers)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        for server in servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        RemoteUtilHelper.common_basic_setup(servers)
        log = logger.Logger.get_logger()
        log.info("10 seconds delay to wait for membase-server to start")
        time.sleep(10)

        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)

    @staticmethod
    def replication_verification(master, bucket, replica, inserted_count, test):
        rest = RestConnection(master)
        nodes = rest.node_statuses()

        if len(nodes) / (1 + replica) >= 1:
                    final_replication_state = RestHelper(rest).wait_for_replication(900)
                    msg = "replication state after waiting for up to 15 minutes : {0}"
                    test.log.info(msg.format(final_replication_state))
                    # in windows, we need to set timeout_in_seconds to 15+ minutes
                    test.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                                                  bucket=bucket,
                                                                                  timeout_in_seconds=1200),
                                    msg="replication was completed but sum(curr_items) dont match the curr_items_total")

                    start_time = time.time()
                    stats = rest.get_bucket_stats()
                    while time.time() < (start_time + 120) and stats["curr_items"] != inserted_count:
                        test.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
                        time.sleep(5)
                        stats = rest.get_bucket_stats()
                    RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
                    test.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], inserted_count))
                    stats = rest.get_bucket_stats()
                    msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
                    test.assertEquals(stats["curr_items"], inserted_count,
                                      msg=msg.format(stats["curr_items"], inserted_count))

    @staticmethod
    def load_data(master, bucket, keys_count=-1, load_ratio=-1):
        log = logger.Logger.get_logger()
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[master],
                                                              name=bucket,
                                                              ram_load_ratio=load_ratio,
                                                              number_of_items=keys_count,
                                                              number_of_threads=2,
                                                              write_only=True)
        log.info("wait until data is completely persisted on the disk")
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0)
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0)
        return inserted_keys

    @staticmethod
    def verify_data(master, inserted_keys, bucket, test):
        log = logger.Logger.get_logger()
        log.info("Verifying data")
        ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0)
        test.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0)
        test.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        BucketOperationHelper.keys_exist_or_assert_in_parallel(keys=inserted_keys, server=master, bucket_name=bucket,
                                                                   test=test, concurrency=4)

    @staticmethod
    def get_test_params(input):
        _keys_count = -1
        _replica = -1
        _load_ratio = -1

        try:
            _keys_count = int(input.test_params['keys_count'])
        except KeyError:
            try:
                _load_ratio = float(input.test_params['load_ratio'])
            except KeyError:
                _load_ratio = DEFAULT_LOAD_RATIO

        try:
            _replica = int(input.test_params['replica'])
        except  KeyError:
            _replica = 1
        return _keys_count, _replica, _load_ratio


class FailoverTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self.bidirectional  = self._input.param("bidirectional", False)
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        FailoverBaseTest.common_setup(self._input, self)

    def tearDown(self):
        FailoverBaseTest.common_tearDown(self._servers, self)

    def test_failover_firewall(self):
        keys_count, replica, load_ratio = FailoverBaseTest.get_test_params(self._input)
        self.common_test_body(keys_count, replica, load_ratio, 'firewall')

    def test_failover_normal(self):
        keys_count, replica, load_ratio = FailoverBaseTest.get_test_params(self._input)
        self.common_test_body(keys_count, replica, load_ratio, 'normal')

    def test_failover_stop_server(self):
        keys_count, replica, load_ratio = FailoverBaseTest.get_test_params(self._input)
        self.common_test_body(keys_count, replica, load_ratio, 'stop_server')

    def common_test_body(self, keys_count, replica, load_ratio, failover_reason):
        log = logger.Logger.get_logger()
        log.info("keys_count : {0}".format(keys_count))
        log.info("replica : {0}".format(replica))
        log.info("load_ratio : {0}".format(load_ratio))
        log.info("failover_reason : {0}".format(failover_reason))
        master = self._servers[0]
        log.info('picking server : {0} as the master'.format(master))
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.memoryQuota * 2 / 3
        bucket = 'default'
        rest.create_bucket(bucket=bucket,
                           ramQuotaMB=bucket_ram,
                           replicaNumber=replica,
                           proxyPort=info.moxi)
        ready = BucketOperationHelper.wait_for_memcached(master, bucket)
        self.assertTrue(ready, "wait_for_memcached_failed")
        credentials = self._input.membase_settings

        ClusterOperationHelper.add_all_nodes_or_assert(master, self._servers, credentials, self)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        msg = "rebalance failed after adding these nodes {0}".format(nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)

        inserted_keys = FailoverBaseTest.load_data(master, bucket, keys_count, load_ratio)
        inserted_count = len(inserted_keys)
        log.info('inserted {0} keys'.format(inserted_count))

        nodes = rest.node_statuses()
        while (len(nodes) - replica) > 1:
            final_replication_state = RestHelper(rest).wait_for_replication(900)
            msg = "replication state after waiting for up to 15 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            chosen = RebalanceHelper.pick_nodes(master, howmany=replica)
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
                    #try again in 60 seconds
                    time.sleep(75)
                    failed_over = rest.fail_over(node.id)
                self.assertTrue(failed_over, "unable to failover node after {0}".format(failover_reason))
                log.info("failed over node : {0}".format(node.id))
            #REMOVEME -
            log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[node.id for node in chosen])
            msg = "rebalance failed while removing failover nodes {0}".format(chosen)
            self.assertTrue(rest.monitorRebalance(), msg=msg)
            FailoverBaseTest.replication_verification(master, bucket, replica, inserted_count, self)

            nodes = rest.node_statuses()
        FailoverBaseTest.verify_data(master, inserted_keys, bucket, self)

    def stop_server(self, node):
        log = logger.Logger.get_logger()
        for server in self._servers:
            rest = RestConnection(server)
            if not RestHelper(rest).is_ns_server_running(timeout_in_seconds=5):
                continue
            server_ip = rest.get_nodes_self().ip
            if server_ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_membase_installed():
                    shell.stop_membase()
                    log.info("Membase stopped")
                else:
                    shell.stop_couchbase()
                    log.info("Couchbase stopped")
                shell.disconnect()
                break