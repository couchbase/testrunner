from random import shuffle
import time
import unittest
import uuid
import json
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, MutationThread, VBucketAwareMemcached, LoadWithMcsoda
from threading import Thread


NUM_REBALANCE = 2
DEFAULT_LOAD_RATIO = 5
DEFAULT_REPLICA = 1
EXPIRY_RATIO = 0.1
DELETE_RATIO = 0.1

class RebalanceBaseTest(unittest.TestCase):
    @staticmethod
    def common_setup(input, testcase, bucket_ram_ratio=(2.0 / 3.0), replica=0):
        log = logger.Logger.get_logger()
        servers = input.servers
        serverInfo = servers[0]
        rest = RestConnection(serverInfo)
        rest.stop_rebalance()
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)

        log.info('picking server : {0} as the master'.format(serverInfo))
        #if all nodes are on the same machine let's have the bucket_ram_ratio as bucket_ram_ratio * 1/len(servers)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))
        if "num_buckets" in TestInputSingleton.input.test_params:
            num_buckets = int(TestInputSingleton.input.test_params["num_buckets"])
        else:
            num_buckets = 1
        if "ascii" in TestInputSingleton.input.test_params\
        and TestInputSingleton.input.test_params["ascii"].lower() == "true":
            BucketOperationHelper.create_multiple_buckets(serverInfo, replica, node_ram_ratio * bucket_ram_ratio,
                                                          howmany=num_buckets, sasl=False)
        else:
            BucketOperationHelper.create_multiple_buckets(serverInfo, replica, node_ram_ratio * bucket_ram_ratio,
                                                          howmany=num_buckets, sasl=True)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket.name)
            testcase.assertTrue(ready, "wait_for_memcached failed")

    @staticmethod
    def common_tearDown(servers, testcase):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

    @staticmethod
    def replication_verification(master, bucket_data, replica, test, failed_over=False):
        asserts = []
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        nodes = rest.node_statuses()
        test.log.info("expect {0} / {1} replication ? {2}".format(len(nodes),
            (1.0 + replica), len(nodes) / (1.0 + replica)))
        if len(nodes) / (1.0 + replica) >= 1:
            final_replication_state = RestHelper(rest).wait_for_replication(300)
            msg = "replication state after waiting for up to 5 minutes : {0}"
            test.log.info(msg.format(final_replication_state))
            #run expiry_pager on all nodes before doing the replication verification
            for bucket in buckets:
                ClusterOperationHelper.set_expiry_pager_sleep_time(master, bucket.name)
                test.log.info("wait for expiry pager to run on all these nodes")
                time.sleep(30)
                ClusterOperationHelper.set_expiry_pager_sleep_time(master, bucket.name, 3600)
                ClusterOperationHelper.set_expiry_pager_sleep_time(master, bucket.name)
                # windows need more than 15 minutes to get number matched
                replica_match = RebalanceHelper.wait_till_total_numbers_match(bucket=bucket.name,
                                                                              master=master,
                                                                              timeout_in_seconds=1200)
                if not replica_match:
                    asserts.append("replication was completed but sum(curr_items) dont match the curr_items_total")
                if not failed_over:
                    stats = rest.get_bucket_stats(bucket=bucket.name)
                    RebalanceHelper.print_taps_from_all_nodes(rest, bucket.name)
                    msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
                    active_items_match = stats["curr_items"] == bucket_data[bucket.name]["items_inserted_count"]
                    if not active_items_match:
                    #                        asserts.append(
                        test.log.error(
                            msg.format(stats["curr_items"], bucket_data[bucket.name]["items_inserted_count"]))

        if len(asserts) > 0:
            for msg in asserts:
                test.log.error(msg)
            test.assertTrue(len(asserts) == 0, msg=asserts)

    @staticmethod
    def get_distribution(load_ratio):
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        if load_ratio == 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        elif load_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
        return distribution

    @staticmethod
    def rebalance_in(servers, how_many):
        return RebalanceHelper.rebalance_in(servers, how_many)

    @staticmethod
    def load_data_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data, test):
        buckets = rest.get_buckets()
        for bucket in buckets:
            inserted_count, rejected_count =\
            MemcachedClientHelper.load_bucket(name=bucket.name,
                                              servers=rebalanced_servers,
                                              ram_load_ratio=load_ratio,
                                              value_size_distribution=distribution,
                                              number_of_threads=1,
                                              write_only=True,
                                              moxi=True)
            test.log.info('inserted {0} keys'.format(inserted_count))
            bucket_data[bucket.name]["items_inserted_count"] += inserted_count

    @staticmethod
    def threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data, delete_ratio=0,
                            expiry_ratio=0):
        buckets = rest.get_buckets()
        for bucket in buckets:
            threads = MemcachedClientHelper.create_threads(servers=rebalanced_servers,
                                                           name=bucket.name,
                                                           ram_load_ratio=load_ratio,
                                                           value_size_distribution=distribution,
                                                           number_of_threads=4,
                                                           delete_ratio=delete_ratio,
                                                           expiry_ratio=expiry_ratio)
            [t.start() for t in threads]
            bucket_data[bucket.name]["threads"] = threads
        return bucket_data

    @staticmethod
    def bucket_data_init(rest):
        bucket_data = {}
        buckets = rest.get_buckets()
        for bucket in buckets:
            bucket_data[bucket.name] = {}
            bucket_data[bucket.name]['items_inserted_count'] = 0
            bucket_data[bucket.name]['inserted_keys'] = []
        return bucket_data


    @staticmethod
    def load_data(master, bucket, keys_count=-1, load_ratio=-1, delete_ratio=0, expiry_ratio=0, test=None):
        log = logger.Logger.get_logger()
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[master],
                                                              name=bucket,
                                                              ram_load_ratio=load_ratio,
                                                              number_of_items=keys_count,
                                                              number_of_threads=2,
                                                              write_only=True,
                                                              delete_ratio=delete_ratio,
                                                              expiry_ratio=expiry_ratio,
                                                              moxi=True)
        log.info("wait until data is completely persisted on the disk")
        ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0, timeout_in_seconds=120)
        test.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0, timeout_in_seconds=120)
        test.assertTrue(ready, "wait_for ep_flusher_todo == 0 failed")
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


class IncrementalRebalanceComboTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, keys_count=-1, load_ratio=-1, replica=1, rebalance_in=2, verify=True):
        master = self._servers[0]
        creds = self._input.membase_settings
        rest = RestConnection(master)
        rebalanced_servers = [master]
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        temp_bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        buckets = rest.get_buckets()
        inserted_keys = []
        for bucket in buckets:
            inserted_keys = RebalanceBaseTest.load_data(master, bucket.name, -1, load_ratio, test=self)
            temp_bucket_data[bucket.name]['inserted_keys'].extend(inserted_keys)
            temp_bucket_data[bucket.name]["items_inserted_count"] += len(inserted_keys)
            self.log.info("inserted {0} keys".format(len(inserted_keys)))

        nodes = rest.node_statuses()
        while len(nodes) < len(self._servers):
            # Start loading threads
            bucket_data = RebalanceBaseTest.threads_for_buckets(rest, load_ratio, None, rebalanced_servers,
                                                                bucket_data,
                                                                delete_ratio=DELETE_RATIO, expiry_ratio=EXPIRY_RATIO)
            # Start rebalance in thread
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            rebalance_thread = Thread(target=RebalanceHelper.rebalance_in,
                                      args=(self._servers, rebalance_in), name="rebalance_thread")
            #rebalance_thread.daemon = True
            rebalance_thread.start()

            # Start reader daemon
            for bucket in buckets:
                reader_thread = Thread(target=BucketOperationHelper.keys_exist_or_assert_in_parallel,
                                       args=(inserted_keys, master, bucket.name, self, 4), name="reader_thread")
                reader_thread.daemon = True
                reader_thread.start()

            # Wait for loading threads to finish
            for name in bucket_data:
                for thread in bucket_data[name]["threads"]:
                    thread.join()
                    bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()
            rebalance_thread.join()

            # Verification step
            nodes = rest.node_statuses()
            if verify:
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

                for bucket in buckets:
                    RebalanceBaseTest.verify_data(master, bucket_data[bucket.name]['inserted_keys'], bucket.name, self)

            # Wait for replicaion and verify persistence
            final_replication_state = RestHelper(rest).wait_for_replication(300)
            msg = "replication state after waiting for up to 5 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
        #            ClusterOperationHelper.verify_persistence(self._servers, self)

    def test_load(self):
        keys_count, replica, load_ratio = RebalanceBaseTest.get_test_params(self._input)
        RebalanceBaseTest.common_setup(self._input, self, replica=replica)
        self._common_test_body(keys_count, load_ratio, replica, verify=False)


#load data. add one node rebalance , rebalance out
class IncrementalRebalanceInTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, keys_count=-1,
                          load_ratio=-1, replica=1,
                          rebalance_in=2, verify=True,
                          delete_ratio=0, expiry_ratio=0):
        master = self._servers[0]
        rest = RestConnection(master)
        rebalanced_servers = [master]
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        nodes = rest.node_statuses()
        while len(nodes) < len(self._servers):
            buckets = rest.get_buckets()
            for bucket in buckets:
                inserted_keys = RebalanceBaseTest.load_data(master, bucket.name, keys_count, load_ratio, delete_ratio,
                                                            expiry_ratio, self)
                bucket_data[bucket.name]['inserted_keys'].extend(inserted_keys)
                bucket_data[bucket.name]["items_inserted_count"] += len(inserted_keys)

            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))

            rebalanced_in, which_servers = RebalanceBaseTest.rebalance_in(self._servers, rebalance_in)
            self.assertTrue(rebalanced_in, msg="unable to add and rebalance more nodes")
            rebalanced_servers.extend(which_servers)
            nodes = rest.node_statuses()
            if verify:
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

                for bucket in buckets:
                    RebalanceBaseTest.verify_data(master, bucket_data[bucket.name]['inserted_keys'], bucket.name, self)
            final_replication_state = RestHelper(rest).wait_for_replication(300)
            msg = "replication state after waiting for up to 5 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
        #            ClusterOperationHelper.verify_persistence(self._servers, self, 20000, 120)

    def test_load(self):
        log = logger.Logger().get_logger()
        keys_count, replica, load_ratio = RebalanceBaseTest.get_test_params(self._input)
        delete_ratio = DELETE_RATIO
        expiry_ratio = EXPIRY_RATIO
        if "delete-ratio" in self._input.test_params:
            delete_ratio = float(self._input.test_params.get('delete-ratio', DELETE_RATIO))
        if "expiry-ratio" in self._input.test_params:
            expiry_ratio = float(self._input.test_params.get('expiry-ratio', EXPIRY_RATIO))
        log.info("keys_count, replica, load_ratio: {0} {1} {2}".format(keys_count, replica, load_ratio))
        RebalanceBaseTest.common_setup(self._input, self, replica=replica)
        self._common_test_body(keys_count, load_ratio, replica, True, delete_ratio, expiry_ratio)


class IncrementalRebalanceInWithParallelLoad(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, keys_count=-1,
                          load_ratio=-1, replica=1,
                          rebalance_in=2, verify=True,
                          delete_ratio=0, expiry_ratio=0):
        master = self._servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        creds = self._input.membase_settings
        rebalanced_servers = [master]

        for server in self._servers[1:]:
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))
            distribution = RebalanceBaseTest.get_distribution(load_ratio)
            bucket_data = RebalanceBaseTest.threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers,
                                                                bucket_data,
                                                                delete_ratio=delete_ratio, expiry_ratio=expiry_ratio)

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)

            for name in bucket_data:
                for thread in bucket_data[name]["threads"]:
                    thread.join()
                    bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()

            if verify:
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

    def test_load(self):
        log = logger.Logger().get_logger()
        keys_count, replica, load_ratio = RebalanceBaseTest.get_test_params(self._input)
        delete_ratio = DELETE_RATIO
        expiry_ratio = EXPIRY_RATIO
        if "delete-ratio" in self._input.test_params:
            delete_ratio = float(self._input.test_params.get('delete-ratio', DELETE_RATIO))
        if "expiry-ratio" in self._input.test_params:
            expiry_ratio = float(self._input.test_params.get('expiry-ratio', EXPIRY_RATIO))
        log.info("Test inputs {0}".format(self._input.test_params))
        RebalanceBaseTest.common_setup(self._input, self, replica=replica)
        self._common_test_body(keys_count, load_ratio, replica, 1, True, delete_ratio, expiry_ratio)


class IncrementalRebalanceWithParallelLoad(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance, then remove each node
    def _common_test_body(self, keys_count, repeat, max_ops_per_second, min_item_size):
        master = self._servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        creds = self._input.membase_settings
        rebalanced_servers = [master]

        # start load, max_ops_per_second is the combined limit for all buckets
        buckets = rest.get_buckets()
        loaders = []
        self.log.info("max-ops-per-second per bucket: {0}".format(max_ops_per_second / len(buckets)))
        for bucket in buckets:
            loader = {}
            loader["mcsoda"] = LoadWithMcsoda(master, keys_count, prefix='', bucket=bucket.name,password=bucket.saslPassword, protocol='membase-binary')
#            loader["mcsoda"] = LoadWithMcsoda(master, keys_count, prefix='', bucket=bucket.name, password=bucket.saslPassword, protocol='memcached-binary', port=11211)
            loader["mcsoda"].cfg["max-ops"] = 0
            loader["mcsoda"].cfg["max-ops-per-sec"] = max_ops_per_second / len(buckets)
            loader["mcsoda"].cfg["exit-after-creates"] = 0
            loader["mcsoda"].cfg["min-value-size"] = min_item_size
            loader["mcsoda"].cfg["json"] = 0
            loader["mcsoda"].cfg["batch"] = 100
            loader["thread"] = Thread(target=loader["mcsoda"].load_data, name='mcloader_'+bucket.name)
            loader["thread"].daemon = True
            loaders.append(loader)

        for loader in loaders:
            loader["thread"].start()


        for iteration in range(repeat):
            for server in self._servers[1:]:
                self.log.info("iteration {0}: ".format(iteration))
                self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
                self.log.info("adding node {0} and rebalance afterwards".format(server.ip))

                rebalance_done = False
                rebalance_try = 0
                while not rebalance_done:
                    try:
                        ClusterOperationHelper.begin_rebalance_in(master, [server])
                        ClusterOperationHelper.end_rebalance(master)
                        rebalance_done = True
                    except AssertionError as e:
                        rebalance_try += 1
                        self.log.error(e)
                        time.sleep(5)
                        if rebalance_try > 5:
                            raise e

            for server in self._servers[1:]:
                self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
                self.log.info("removing node {0} and rebalance afterwards".format(server.ip))

                rebalance_done = False
                rebalance_try = 0
                while not rebalance_done:
                    try:
                        ClusterOperationHelper.begin_rebalance_out(master, [server])
                        ClusterOperationHelper.end_rebalance(master)
                        rebalance_done = True
                    except AssertionError as e:
                        rebalance_try += 1
                        self.log.error(e)
                        time.sleep(5)
                        if rebalance_try > 5:
                            raise e

        # stop load
        for loader in loaders:
            loader["mcsoda"].load_stop()

        for loader in loaders:
            loader["thread"].join()


    def test_load(self):
        log = logger.Logger().get_logger()
        repeat = self._input.param("repeat", 1)
        keys_count = self._input.param("keys_count", 1000)
        replica = self._input.param("replica", 1)
        max_ops_per_second = self._input.param("max_ops_per_second", 500)
        min_item_size = self._input.param("min_item_size", 128)

        log.info("repeat: {0}, keys_count: {1}, replica: {2}, max_ops_per_second: {3}, min_item_size: {4}".format(repeat, keys_count, replica, max_ops_per_second, min_item_size))

        RebalanceBaseTest.common_setup(self._input, self, replica=replica)
        self._common_test_body(keys_count, repeat, max_ops_per_second, min_item_size)


#this test case will add all the nodes and then start removing them one by one

class IncrementalRebalanceOut(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()


    def test_load(self):
        keys_count, replica, load_ratio = RebalanceBaseTest.get_test_params(self._input)
        log = logger.Logger().get_logger()
        log.info("keys_count, replica, load_ratio: {0} {1} {2}".format(keys_count, replica, load_ratio))
        RebalanceBaseTest.common_setup(self._input, self, replica=replica)
        self._common_test_body(keys_count, load_ratio, replica)


    def _common_test_body(self, keys_count=-1, load_ratio=-1, replica=1):
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(load_ratio)

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes {0}".format(
                            [node.id for node in rest.node_statuses()]))
        #remove nodes one by one and rebalance , add some item and
        #rebalance ?
        rebalanced_servers = []
        rebalanced_servers.extend(self._servers)

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceHelper.pick_node(master)

            for bucket in rest.get_buckets():
                RebalanceBaseTest.load_data(master, bucket.name,
                                            keys_count, load_ratio,
                                            delete_ratio=0, expiry_ratio=0,
                                            test=self)

            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))

            for node in nodes:
                for rebalanced_server in rebalanced_servers:
                    if rebalanced_server.ip.find(node.ip) != -1:
                        rebalanced_servers.remove(rebalanced_server)
                        break
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            nodes = rest.node_statuses()

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class StopRebalanceAfterFailoverTests(unittest.TestCase):
    # this test will create cluster of n nodes
    # stops rebalance
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def stop_rebalance_1_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(1.0, replica=1)

    def stop_rebalance_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(1.0, replica=2)

    def stop_rebalance_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(1.0, replica=3)

    def _common_test_body(self, ratio, replica):
        ram_ratio = (ratio / (len(self._servers)))
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(ram_ratio)

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes")

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceHelper.pick_node(master)
            RebalanceBaseTest.load_data_for_buckets(rest, ram_ratio, distribution, [master], bucket_data, self)
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            #let's start/step rebalance three times
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest.fail_over(toBeEjectedNode.id)
            self.log.info("failed over {0}".format(toBeEjectedNode.id))
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                           ejectedNodes=[toBeEjectedNode.id])
            expected_progress = 30
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
            time.sleep(20)
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            time.sleep(20)

            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            nodes = rest.node_statuses()

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class StopRebalanceTests(unittest.TestCase):
    # this test will create cluster of n nodes
    # stops rebalance
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def stop_rebalance_1_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(5.0, replica=1)

    def stop_rebalance_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(5.0, replica=2)

    def stop_rebalance_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(5.0, replica=3)

    def _common_test_body(self, ratio, replica):
        ram_ratio = (ratio / (len(self._servers)))
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(ram_ratio)

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes")

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceHelper.pick_node(master)
            RebalanceBaseTest.load_data_for_buckets(rest, ram_ratio, distribution, [master], bucket_data, self)
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            #let's start/step rebalance three times
            for i in range(0, NUM_REBALANCE):
                self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
                expected_progress = 30
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                time.sleep(20)
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            time.sleep(20)

            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            nodes = rest.node_statuses()

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class IncrementalRebalanceWithParallelReadTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, items, replica=1, moxi=False):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        for server in self._servers[1:]:
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            self.log.info("adding node {0}:{1} and rebalance afterwards".format(server.ip, server.port))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            distribution = RebalanceBaseTest.get_distribution(items)
            for name in bucket_data:
                inserted_keys, rejected_keys =\
                MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self._servers[0]],
                                                                      name=name,
                                                                      ram_load_ratio=-1,
                                                                      number_of_items=items,
                                                                      value_size_distribution=distribution,
                                                                      number_of_threads=1,
                                                                      write_only=True)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                self.assertTrue(rest.monitorRebalance(),
                                msg="rebalance operation failed after adding node {0}".format(server.ip))
                self.log.info("completed rebalancing in server {0}".format(server))
                IncrementalRebalanceWithParallelReadTests._reader_thread(self, inserted_keys, bucket_data, moxi=moxi)
                self.assertTrue(rest.monitorRebalance(),
                                msg="rebalance operation failed after adding node {0}".format(server.ip))
                break

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    @staticmethod
    def _reader_thread(self, inserted_keys, bucket_data, moxi=False):
        errors = []
        rest = RestConnection(self._servers[0])
        smartclient = None
        for name in bucket_data:
            for key in inserted_keys:
                if moxi:
                    moxi = MemcachedClientHelper.proxy_client(self._servers[0], name)
                else:
                    smartclient = VBucketAwareMemcached(rest, name)
                try:
                    if moxi:
                        moxi.get(key)
                    else:
                        smartclient.memcached(key).get(key)
                except Exception as ex:
                    errors.append({"error": ex, "key": key})
                    self.log.info(ex)
                    if not moxi:
                        smartclient.done()
                        smartclient = VBucketAwareMemcached(rest, name)

    def test_10k_moxi(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10 * 1000, moxi=True)

    def test_10k_memcached(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10 * 1000)


class FailoverRebalanceRepeatTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio, replica=1):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        rebalanced_servers = [master]
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(load_ratio)
        bucket_data = RebalanceBaseTest.threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers,
                                                            bucket_data,
                                                            delete_ratio=DELETE_RATIO, expiry_ratio=EXPIRY_RATIO)

        for name in bucket_data:
            for thread in bucket_data[name]["threads"]:
                bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()

        for server in self._servers[1:]:
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            #do this 2 times , start rebalance , failover the node , remove the node and rebalance
            for i in range(0, NUM_REBALANCE):
                distribution = RebalanceBaseTest.get_distribution(load_ratio)

                RebalanceBaseTest.load_data_for_buckets(rest, load_ratio, distribution, [master], bucket_data, self)
                self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
                otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
                msg = "unable to add node {0} to the cluster {1}"
                self.assertTrue(otpNode, msg.format(server.ip, master.ip))
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                self.assertTrue(rest.monitorRebalance(),
                                msg="rebalance operation failed after adding node {0}".format(server.ip))
                rebalanced_servers.append(server)
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, True)
                rest.fail_over(otpNode.id)
                self.log.info("failed over {0}".format(otpNode.id))
                time.sleep(10)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                               ejectedNodes=[otpNode.id])
                msg = "rebalance failed while removing failover nodes {0}".format(otpNode.id)
                self.assertTrue(rest.monitorRebalance(), msg=msg)
                #now verify the numbers again ?
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, True)
                #wait 6 minutes
                time.sleep(6 * 60)

            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            distribution = RebalanceBaseTest.get_distribution(load_ratio)
            RebalanceBaseTest.load_data_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data,
                                                    self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, True)

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load_replica_1(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(1)


class RebalanceTestsWithMutationLoadTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio, replica):
        self.log.info(load_ratio)
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        if load_ratio == 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        elif load_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
        rebalanced_servers = [master]
        #let's run this only for the first bucket
        bucket0 = rest.get_buckets()[0].name
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=rebalanced_servers,
                                                              ram_load_ratio=load_ratio,
                                                              number_of_items=-1,
                                                              name=bucket0,
                                                              value_size_distribution=distribution,
                                                              number_of_threads=20,
                                                              write_only=True)
        items_inserted_count += len(inserted_keys)

        #let's mutate all those keys
        for server in self._servers[1:]:
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

            mutation = "{0}".format(uuid.uuid4())
            thread = MutationThread(serverInfo=server, keys=inserted_keys, seed=mutation, op="set", name=bucket0)
            thread.start()

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)

            self.log.info("waiting for mutation thread....")
            thread.join()

            rejected = thread._rejected_keys
            client = MemcachedClientHelper.proxy_client(master, bucket0)
            self.log.info("printing tap stats before verifying mutations...")
            RebalanceHelper.print_taps_from_all_nodes(rest, bucket0)
            did_not_mutate_keys = []
            for key in inserted_keys:
                if key not in rejected:
                    try:
                        flag, keyx, value = client.get(key)
                        try:
                            json_object = json.loads(value)
                            print json_object["seed"]
                            print json
                            print mutation
                            if  json.loads(value)["seed"] != mutation:
                                did_not_mutate_keys.append({'key': key, 'value': value})
                        except:
                            did_not_mutate_keys.append({'key': key, 'value': value})
                    except Exception as ex:
                        self.log.info(ex)
                        #                    self.log.info(value)
            if len(did_not_mutate_keys) > 0:
                for item in did_not_mutate_keys:
                    self.log.error(
                        "mutation did not replicate for key : {0} value : {1}".format(item['key'], item['value']))
                    self.fail("{0} mutations were not replicated during rebalance..".format(len(did_not_mutate_keys)))

            nodes = rest.node_statuses()
            self.log.info("expect {0} / {1} replication ? {2}".format(len(nodes),
                (1.0 + replica), len(nodes) / (1.0 + replica)))

            if len(nodes) / (1.0 + replica) >= 1:
                final_replication_state = RestHelper(rest).wait_for_replication(600)
                msg = "replication state after waiting for up to 10 minutes : {0}"
                self.log.info(msg.format(final_replication_state))
                # windows need more than 15 minutes to get number matched
                self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                                              bucket=bucket0,
                                                                              timeout_in_seconds=1200),
                                msg="replication was completed but sum(curr_items) dont match the curr_items_total")

            start_time = time.time()
            stats = rest.get_bucket_stats(bucket=bucket0)
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats(bucket=bucket0)
                #loop over all keys and verify

            RebalanceHelper.print_taps_from_all_nodes(rest, bucket0)
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            stats = rest.get_bucket_stats(bucket=bucket0)
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(0.05, replica=1)

    def test_small_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(1.0, replica=2)

    def test_small_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(1.0, replica=3)

    def test_medium_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10.0, replica=1)

    def test_medium_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(10.0, replica=2)

    def test_medium_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(10.0, replica=3)

    def test_heavy_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(40.0, replica=1)

    def test_heavy_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(40.0, replica=2)

    def test_heavy_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(401.0, replica=3)


# fill bucket len(nodes)X and then rebalance nodes one by one
class IncrementalRebalanceInDgmTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, dgm_ratio, distribution, rebalance_in=1, replica=1):
        master = self._servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        rebalanced_servers = [master]

        nodes = rest.node_statuses()
        while len(nodes) <= len(self._servers):
            rebalanced_in, which_servers = RebalanceBaseTest.rebalance_in(self._servers, rebalance_in)
            self.assertTrue(rebalanced_in, msg="unable to add and rebalance more nodes")
            rebalanced_servers.extend(which_servers)
            RebalanceBaseTest.load_data_for_buckets(rest, dgm_ratio * 50, distribution, rebalanced_servers, bucket_data,
                                                    self)

            nodes = rest.node_statuses()
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_1_5x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
        self._common_test_body(1.5, distribution)

    def test_1_5x_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=2)
        distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
        self._common_test_body(1.5, distribution, replica=2)

    def test_1_5x_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=3)
        distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
        self._common_test_body(1.5, distribution, replica=3)

    def test_1_5x_cluster_half_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=2)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 2, replica=2)

    def test_1_5x_cluster_half_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=3)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 2, replica=3)

    def test_1_5x_cluster_half(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 2)

    def test_1_5x_cluster_one_third(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 3)

    def test_5x_cluster_half(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(5, distribution, len(self._servers) / 2)

    def test_5x_cluster_one_third(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(5, distribution, len(self._servers) / 3)

    def test_1_5x_large_values(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution)

    def test_2_x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(2, distribution)

    def test_3_x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(3, distribution)

    def test_5_x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(5, distribution)


class RebalanceSwapTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    def rebalance_in(self, how_many):
        return RebalanceHelper.rebalance_in(self._servers, how_many)

    #load data add one node , rebalance add another node rebalance
    #swap one node with another node
    #swap two nodes with two another nodes
    def _common_test_body(self, swap_count):
        msg = "minimum {0} nodes required for running rebalance swap of {1} nodes"
        self.assertTrue(len(self._servers) >= (2 * swap_count),
                        msg=msg.format((2 * swap_count), swap_count))
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings

        self.log.info("inserting some items in the master before adding any nodes")

        self.assertTrue(self.rebalance_in(swap_count - 1))

        MemcachedClientHelper.load_bucket(name="bucket-0",
                                          servers=[master],
                                          number_of_items=20000,
                                          number_of_threads=1,
                                          write_only=True,
                                          moxi=True)
        #now add nodes for being swapped
        #eject the current nodes and add new ones
        # add 2 * swap -1 nodes
        nodeIpPorts = ["{0}:{1}".format(node.ip, node.port) for node in rest.node_statuses()]
        self.log.info("current nodes : {0}".format(nodeIpPorts))
        toBeAdded = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            if not "{0}:{1}".format(server.ip, server.port) in nodeIpPorts:
                toBeAdded.append(server)
                self.log.info("adding {0}:{1} to toBeAdded".format(server.ip, server.port))
            if len(toBeAdded) == swap_count:
                break
        toBeEjected = [node.id for node in rest.node_statuses()]
        for server in toBeAdded:
            rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)

        currentNodes = [node.id for node in rest.node_statuses()]
        started = rest.rebalance(currentNodes, toBeEjected)
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        self.assertTrue(started, "rebalance operation did not start for swapping out the nodes")
        result = rest.monitorRebalance()
        msg = "successfully swapped out {0} nodes from the cluster ? {0}"
        self.assertTrue(result, msg.format(swap_count))
        #rest will have to change now?

    def test_swap_1(self):
        self._common_test_body(1)

    def test_swap_2(self):
        self._common_test_body(2)

    def test_swap_4(self):
        self._common_test_body(4)


class RebalanceInOutWithParallelLoad(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    def _common_test_body(self, keys_count=-1,
                          load_ratio=-1, replica=1,
                          rebalance_in=2, verify=True,
                          delete_ratio=0, expiry_ratio=0):
        master = self._servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        creds = self._input.membase_settings
        rebalanced_servers = [master]

        otpNode = None
        for server in self._servers[1:]:
            if otpNode:
                # rebalance out the previous node
                ejectedNodes = [otpNode.id]
            else:
                ejectedNodes = []
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            self.log.info("adding node {0}, removing node {1} and rebalance afterwards".format(server.ip, ejectedNodes))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster and remove node {1}"
            self.assertTrue(otpNode, msg.format(server.ip, ejectedNodes))
            distribution = RebalanceBaseTest.get_distribution(load_ratio)
            bucket_data = RebalanceBaseTest.threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers,
                                                                bucket_data,
                                                                delete_ratio=delete_ratio, expiry_ratio=expiry_ratio)

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=ejectedNodes)
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0} and removing node {1}".format(
                                server.ip, ejectedNodes))
            rebalanced_servers.append(server)

            for name in bucket_data:
                for thread in bucket_data[name]["threads"]:
                    thread.join()
                    bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()
            if verify:
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


    def test_load(self):
        log = logger.Logger().get_logger()
        keys_count, replica, load_ratio = RebalanceBaseTest.get_test_params(self._input)
        delete_ratio = DELETE_RATIO
        expiry_ratio = EXPIRY_RATIO
        if "delete-ratio" in self._input.test_params:
            delete_ratio = float(self._input.test_params.get('delete-ratio',DELETE_RATIO))
        if "expiry-ratio" in self._input.test_params:
            expiry_ratio = float(self._input.test_params.get('expiry-ratio',EXPIRY_RATIO))
        log.info("Test inputs {0}".format(self._input.test_params))
        RebalanceBaseTest.common_setup(self._input, self, replica=replica)
        self._common_test_body(keys_count, load_ratio, replica, 1, True, delete_ratio, expiry_ratio)
class RebalanceTaskHelper():
    @staticmethod
    def add_node_init_task(tm, server):
        _t = task.NodeInitializeTask(server)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t)

    @staticmethod
    def add_nodes_init_task(tm, servers):
        return [TaskHelper.add_node_init_task(tm, server) for server in servers ]

    @staticmethod
    def add_bucket_create_task(tm, server, bucket='default', replicas=1, port=11210, size=0,
                           password=None, monitor = True):
        _t = task.BucketCreateTask(server, bucket, replicas, port, size, password)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)

    @staticmethod
    def add_bucket_delete_task(tm, server, bucket='default', monitor = True):
        _t = task.BucketDeleteTask(server, bucket)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)


    @staticmethod
    def add_rebalance_task(tm, servers, to_add, to_remove, monitor = False):
        _t = task.RebalanceTask(servers, to_add, to_remove)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)

    @staticmethod
    def incremental_rebalance_in_tasks(tm, servers, to_add, delay = 0):
        return [ RebalanceTaskHelper.schedule_task_helper(tm, _t, True, delay) for _t in
                    [ task.RebalanceTask(servers, [new_server], []) for new_server in to_add ]]

    @staticmethod
    def incremental_rebalance_out_tasks(tm, servers, to_remove):
        return [ RebalanceTaskHelper.schedule_task_helper(tm, _t, True) for _t in
                    [ task.RebalanceTask(servers, [], [old_server]) for old_server in to_remove ]]


    @staticmethod
    def add_doc_gen_task(tm, rest, count, bucket = "default", kv_store = None, store_enabled = True,
                           kv_template = None, seed = None, sizes = None, expiration = None,
                           loop = False, monitor = False, doc_generators = None):
        doc_generators = doc_generators or DocumentGenerator.get_doc_generators(count, kv_template, seed, sizes)
        _t = task.LoadDocGeneratorTask(rest, doc_generators, bucket, kv_store, store_enabled, expiration, loop)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)

    @staticmethod
    def add_doc_del_task(tm, rest, keys, bucket = "default", info = None,
                         kv_store = None, store_enabled = True,
                         monitor = False, delay = 0):
        _t = task.DocumentDeleteTask(rest, keys, bucket, info, kv_store, store_enabled)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor, delay)

    @staticmethod
    def add_doc_exp_task(tm, rest, keys, bucket = "default", info = None,
                         kv_store = None, store_enabled = True,
                         monitor = False, delay = 0, expiration = 5):
        _t = task.DocumentExpireTask(rest, keys, bucket, info, kv_store,
                                     store_enabled, expiration = expiration)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor, delay)



    @staticmethod
    def add_doc_get_task(tm, rest, keys, bucket = "default", info = None,
                         loop = False, monitor = False, delay = 0):
        _t = task.DocumentAccessTask(rest, keys, bucket, info, loop)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor, delay)


    @staticmethod
    def add_kv_integrity_helper(tm, rest, kv_store, bucket = "default", monitor = True):
        _t = task.KVStoreIntegrityTask(rest, kv_store, bucket)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)


    @staticmethod
    def schedule_task_helper(tm, task, monitor = False, delay = 0):
        tm.schedule(task, delay)
        if monitor:
            return task.result()
        return task


