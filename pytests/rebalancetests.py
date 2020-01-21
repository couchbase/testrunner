import time
import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached, LoadWithMcsoda
from threading import Thread

from old_tasks import task, taskmanager
from memcached.helper.data_helper import DocumentGenerator
from memcached.helper.old_kvstore import ClientKeyValueStore


class RebalanceBaseTest(unittest.TestCase):
    @staticmethod
    def common_setup(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        master = self.servers[0]
        rest = RestConnection(master)

        # Cleanup previous state
        self.task_manager = None
        rest.stop_rebalance()
        RebalanceBaseTest.reset(self)

        # Initialize test params
        self.replica = self.input.param("replica", 1)

        # By default we use keys-count for LoadTask
        # Use keys-count=-1 to use load-ratio
        self.keys_count = self.input.param("keys-count", 30000)
        self.load_ratio = self.input.param("load-ratio", 6)
        self.expiry_ratio = self.input.param("expiry-ratio", 0.1)
        self.delete_ratio = self.input.param("delete-ratio", 0.1)
        self.access_ratio = self.input.param("access-ratio", 1 - self.expiry_ratio - self.delete_ratio)
        self.num_buckets = self.input.param("num-buckets", 1)
        self.num_rebalance = self.input.param("num-rebalance", 1)
        self.do_ascii = self.input.param("ascii", False)
        self.do_verify = self.input.param("do-verify", True)
        self.repeat = self.input.param("repeat", 1)
        self.max_ops_per_second = self.input.param("max_ops_per_second", 500)
        self.min_item_size = self.input.param("min_item_size", 128)
        self.do_stop = self.input.param("do-stop", False)
        self.skip_cleanup = self.input.param("skip-cleanup", False)

        self.checkResidentRatio = self.input.param("checkResidentRatio", False)
        self.activeRatio = self.input.param("activeRatio", 50)
        self.replicaRatio = self.input.param("replicaRatio", 50)
        self.case_number = self.input.param("case_number", 0)

        self.log.info('picking server : {0} as the master'.format(master))

        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
            password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))
        BucketOperationHelper.create_multiple_buckets(master, self.replica, node_ram_ratio * (2.0 / 3.0),
                howmany=self.num_buckets, sasl=not self.do_ascii)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(master, bucket.name)
            self.assertTrue(ready, "wait_for_memcached failed")

        # Initialize and start the taskManager
        self.task_manager = taskmanager.TaskManager()
        self.task_manager.start()

    @staticmethod
    def common_tearDown(self):
        self.log.info("==============  basetestcase cleanup was started for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
        if self.task_manager is not None:
            self.task_manager.cancel()
            self.task_manager.join()
        if not self.skip_cleanup:
            RebalanceBaseTest.reset(self)
        self.log.info("==============  basetestcase cleanup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))

    @staticmethod
    def reset(self):
        rest = RestConnection(self.servers[0])
        if rest._rebalance_progress_status() == 'running':
            self.log.warning("rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        self.log.info("Stopping load in Teardown")
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)

    @staticmethod
    def replication_verification(master, bucket_data, replica, test, failed_over=False):
        asserts = []
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        nodes = rest.node_statuses()
        test.log.info("expect {0} / {1} replication ? {2}".format(len(nodes),
            (1.0 + replica), len(nodes) / (1.0 + replica)))
        for bucket in buckets:
            ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", 30, bucket.name)
        if len(nodes) / (1.0 + replica) >= 1:
            test.assertTrue(RebalanceHelper.wait_for_replication(rest.get_nodes(), timeout=300),
                            msg="replication did not complete after 5 minutes")
            #run expiry_pager on all nodes before doing the replication verification
            for bucket in buckets:
                ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", 30, bucket.name)
                test.log.info("wait for expiry pager to run on all these nodes")
                time.sleep(30)
                ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", 3600, bucket.name)
                ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", 30, bucket.name)
                # windows need more than 15 minutes to get number matched
                replica_match = RebalanceHelper.wait_till_total_numbers_match(bucket=bucket.name,
                    master=master,
                    timeout_in_seconds=600)
                if not replica_match:
                    asserts.append("replication was completed but sum(curr_items) don't match the curr_items_total %s" %
                                   bucket.name)
                if not failed_over:
                    stats = rest.get_bucket_stats(bucket=bucket.name)
                    RebalanceHelper.print_taps_from_all_nodes(rest, bucket.name)
                    msg = "curr_items : {0} is not equal to actual # of keys inserted : {1} : bucket: {2}"

                    if bucket_data[bucket.name]['kv_store'] is None:
                        items_inserted = bucket_data[bucket.name]["items_inserted_count"]
                    else:
                        items_inserted = len(bucket_data[bucket.name]['kv_store'].valid_items())

                    active_items_match = stats["curr_items"] == items_inserted
                    if not active_items_match:
                        asserts.append(msg.format(stats["curr_items"], items_inserted, bucket.name))

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
            inserted_count, rejected_count = \
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
            bucket_data[bucket.name]['kv_store'] = None
            bucket_data[bucket.name]['tasks'] = {}
        return bucket_data

    @staticmethod
    def load_data(master, bucket, keys_count=-1, load_ratio=-1, delete_ratio=0, expiry_ratio=0, test=None):
        log = logger.Logger.get_logger()
        inserted_keys, rejected_keys = \
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
        ready = RebalanceHelper.wait_for_persistence(master, bucket)
        BucketOperationHelper.keys_exist_or_assert_in_parallel(keys=inserted_keys, server=master, bucket_name=bucket,
            test=test, concurrency=4)

    @staticmethod
    def tasks_for_buckets(rest, task_manager,
                          bucket_data,
                          new_doc_seed=None,
                          new_doc_count=-1,
                          DELETE_RATIO=0,
                          ACCESS_RATIO=0,
                          EXPIRY_RATIO=0,
                          ttl=5, data_perc=1):
        # TODO: assert no value greater than 1
        # TODO: assert sum of mutation ratios not greater than 1

        for bucket in list(bucket_data.keys()):
            get_task = None
            del_task = None
            exp_task = None
            load_task = None

            kv_store = bucket_data[bucket]['kv_store']
            current_keys = kv_store.valid_items()
            current_keys = current_keys[: int(len(current_keys) * data_perc)]

            get_docs_count = int(len(current_keys) * ACCESS_RATIO)
            del_docs_count = int(len(current_keys) * DELETE_RATIO)
            exp_docs_count = int(len(current_keys) * EXPIRY_RATIO)
            end_exp_index = del_docs_count + exp_docs_count

            keys_to_get = current_keys[:get_docs_count]
            keys_to_delete = current_keys[:del_docs_count]
            keys_to_expire = current_keys[del_docs_count:end_exp_index]

            # delete ratio of current keys
            if len(keys_to_delete) > 0:
                del_task = \
                RebalanceTaskHelper.add_doc_del_task(task_manager, rest,
                    keys_to_delete,
                    bucket=bucket,
                    kv_store=kv_store)
                # expire ratio of current keys
            if len(keys_to_expire) > 0:
                exp_task = \
                RebalanceTaskHelper.add_doc_exp_task(task_manager, rest,
                    keys_to_expire,
                    bucket=bucket,
                    kv_store=kv_store)

            # get ratio of current keys
            if len(keys_to_get) > 0:
                get_task = \
                RebalanceTaskHelper.add_doc_get_task(task_manager, rest,
                    keys_to_get,
                    bucket=bucket)
                # load more data
            if new_doc_count == -1:
                new_doc_count = len(current_keys)

            load_task = \
            RebalanceTaskHelper.add_doc_gen_task(task_manager, rest,
                new_doc_count,
                bucket=bucket,
                seed=new_doc_seed,
                kv_store=kv_store)

            # update the users bucket_kv_info object
            bucket_data[bucket]['tasks'] = {'get_task': get_task,
                                            'del_task': del_task,
                                            'exp_task': exp_task,
                                            'load_task': load_task}


    @staticmethod
    def finish_all_bucket_tasks(rest, bucket_data):
        buckets = rest.get_buckets()
        [RebalanceBaseTest.finish_bucket_task(bucket_data[b.name]) for b in buckets]

    @staticmethod
    def finish_bucket_task(bucket_name_info):
        log = logger.Logger().get_logger()
        for k, _t in list(bucket_name_info['tasks'].items()):
            if _t is not None:
                log.info("Waiting for {0} task".format(k))
                _t.result()

    @staticmethod
    def load_all_buckets_task(rest, task_manager, bucket_data, ram_load_ratio,
                              distribution=None, keys_count=-1, seed=None,
                              monitor=True):
        buckets = rest.get_buckets()
        tasks = None

        for bucket in buckets:
            kv_store = bucket_data[bucket.name].get('kv_store', None)
            if  kv_store is None:
                kv_store = ClientKeyValueStore()
                bucket_data[bucket.name]['kv_store'] = kv_store
            tasks = RebalanceBaseTest.load_bucket_task_helper(rest, task_manager,
                bucket.name, ram_load_ratio,
                kv_store=kv_store,
                keys_count=keys_count,
                seed=seed,
                monitor=monitor)
        return tasks

    @staticmethod
    def load_bucket_task_helper(rest, task_manager, bucket, ram_load_ratio,
                                kv_store=None, distribution=None,
                                keys_count=-1, seed=None, monitor=True):
        log = logger.Logger().get_logger()
        tasks = []

        if keys_count == -1:
            # create document generators based on value_size_distrobution
            doc_gen_configs = \
            DocumentGenerator.get_doc_generators_by_load_ratio(rest,
                bucket,
                ram_load_ratio,
                distribution)

            for config in doc_gen_configs:
                doc_gen = config['value']
                how_many = config['how_many']
                size = config['size']
                seed = config['seed']

                # start bucket loading task
                msg = "start task to send {0} items with value of size : {1}"
                log.info(msg.format(how_many, size))

                doc_gen_task = RebalanceTaskHelper.add_doc_gen_task(task_manager, rest,
                    how_many, bucket,
                    kv_store=kv_store,
                    doc_generators=[doc_gen],
                    monitor=monitor)
                tasks.append({'task': doc_gen_task, 'seed': seed})
        else:
            msg = "start task to send {0} items to bucket {1}"
            log.info(msg.format(keys_count, bucket))
            doc_gen_task = RebalanceTaskHelper.add_doc_gen_task(task_manager,
                rest,
                keys_count,
                bucket,
                kv_store,
                seed=seed,
                monitor=monitor)
            tasks.append({'task': doc_gen_task, 'seed': seed})

        return tasks

    @staticmethod
    def do_kv_verification(task_manager, rest, bucket_data):
        log = logger.Logger().get_logger()
        error_list = []

        for bucket in bucket_data:
            kv_store = bucket_data[bucket]['kv_store']
            if kv_store is not None:
                log.info("verifying kv store integrity")
                errors = \
                RebalanceTaskHelper.add_kv_integrity_helper(task_manager,
                    rest,
                    kv_store,
                    bucket)
                error_list.append(errors)

        log.info("verification errors: {0} ".format(error_list))
        return error_list

    @staticmethod
    def do_kv_and_replica_verification(master, task_manager, bucket_data, replica, self, failed_over=False,):
        rest = RestConnection(master)

        RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, failed_over=failed_over)

        #verify only test without failed over cases
        if not failed_over:
            # run data integrity
            error_list = RebalanceBaseTest.do_kv_verification(task_manager, rest, bucket_data)

            [self.assertEqual(0, len(list(errors.items()))) for errors in error_list]

    @staticmethod
    def check_resident_ratio(self, master):
        """Check the memory stats- resident ratio from all the servers; expected range is either specified by the
        user or default to 50% for both active and replica items.

        Args:
          self - self
          master: master node.

        Returns:
          None.

        Raises:
          Error/Fail: If the resident ratio is below expected value ( default is 50)
        """

        rest = RestConnection(master)

        nodes = rest.node_statuses()
        buckets = rest.get_buckets()
        rebalance_stats = {}

        for node in nodes:
            for bucket in buckets:
                _node = {"ip": node.ip, "port": node.port, "username": master.rest_username,
                         "password": master.rest_password}
                mc_conn = MemcachedClientHelper.direct_client(_node, bucket.name)
                stats = mc_conn.stats()
                self.log.info(
                    "Bucket {0} node{1}:{2} \n high watermark : {0} , low watermark : {1}".format(bucket.name, node.ip,
                        node.port,
                        stats["ep_mem_high_wat"], stats["ep_mem_low_wat"]))

                key = "{0}:{1}".format(node.ip, node.port)
                rebalance_stats[key] = {}
                rebalance_stats[key] = stats

                active_items_ratio = int(rebalance_stats[key]["vb_active_perc_mem_resident"])
                replica_items_ratio = int(rebalance_stats[key]["vb_replica_perc_mem_resident"])
                self.log.info("active resident ratio is {0}".format(
                    active_items_ratio))
                self.log.info("replica resident ratio is {0}".format(
                    replica_items_ratio))
                if active_items_ratio < self.activeRatio:
                    self.fail(
                        "Very poor active resident ratio {0} for node {1}:{2} ".format(active_items_ratio, node.ip,
                            node.port))
                if replica_items_ratio < self.replicaRatio:
                    self.fail(
                        "Very poor replica resident ratio {0}".format(replica_items_ratio, node.ip, node.port))
                mc_conn.close()

class IncrementalRebalanceInTests(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self):
        master = self.servers[0]
        rest = RestConnection(master)

        bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        self.log.info("INTIAL LOAD")
        RebalanceBaseTest.load_all_buckets_task(rest, self.task_manager,
            bucket_data, self.load_ratio,
            keys_count=self.keys_count)

        #if data are too big we will operate only with 10% of data
        #because parallel ops is too slow due to num_locks=1 is used in old kvs store
        data_perc = 1

        #self.keys_count = self.keys_count // 10
        for server in self.servers[1:]:
            if self.keys_count >= 100000:
                data_perc *= 0.1
                self.log.info("we will operate only with 10% of data size {0} items".format(self.keys_count))

            self.log.info("PARALLEL LOAD")
            RebalanceBaseTest.tasks_for_buckets(rest, self.task_manager, bucket_data,
                DELETE_RATIO=self.delete_ratio,
                ACCESS_RATIO=self.access_ratio, EXPIRY_RATIO=self.expiry_ratio, data_perc=data_perc)

            self.log.info("INCREMENTAL REBALANCE IN")
            # rebalance in a server
            RebalanceTaskHelper.add_rebalance_task(self.task_manager,
                [master],
                [server],
                [], do_stop=self.do_stop)

            # wait for loading tasks to finish
            RebalanceBaseTest.finish_all_bucket_tasks(rest, bucket_data)
            self.log.info("DONE LOAD AND REBALANCE")

            # verification step
            if self.do_verify:
                self.log.info("VERIFICATION")
                RebalanceBaseTest.do_kv_and_replica_verification(master,
                    self.task_manager,
                    bucket_data,
                    self.replica,
                    self)
            else:
                self.log.info("NO VERIFICATION")

    def test_load(self):
        self._common_test_body()


class IncrementalRebalanceWithMcsoda(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    #load data add one node , rebalance add another node rebalance, then remove each node
    def _common_test_body(self):
        master = self.servers[0]
        rest = RestConnection(master)

        # start load, max_ops_per_second is the combined limit for all buckets
        buckets = rest.get_buckets()
        loaders = []
        self.log.info("max-ops-per-second per bucket: {0}".format(self.max_ops_per_second // len(buckets)))
        for bucket in buckets:
            loader = {}
            loader["mcsoda"] = LoadWithMcsoda(master, self.keys_count, prefix='', bucket=bucket.name,
                password=bucket.saslPassword, protocol='membase-binary')
            loader["mcsoda"].cfg["max-ops"] = 0
            loader["mcsoda"].cfg["max-ops-per-sec"] = self.max_ops_per_second // len(buckets)
            loader["mcsoda"].cfg["exit-after-creates"] = 0
            loader["mcsoda"].cfg["min-value-size"] = self.min_item_size
            loader["mcsoda"].cfg["json"] = 0
            loader["mcsoda"].cfg["batch"] = 100
            loader["thread"] = Thread(target=loader["mcsoda"].load_data, name='mcloader_' + bucket.name)
            loader["thread"].daemon = True
            loaders.append(loader)

        for loader in loaders:
            loader["thread"].start()

        for iteration in range(self.repeat):
            for server in self.servers[1:]:
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

            for server in self.servers[1:]:
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
        self._common_test_body()


class IncrementalRebalanceOut(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    def _common_test_body(self):
        master = self.servers[0]

        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        cluster_size = self.input.param("cluster_size", len(self.servers))
        howMany = self.input.param("howMany", cluster_size - 1)

        if howMany >= cluster_size:
            self.fail(
                "Input error! howMany {0} rebalance-outs should be lesser than cluster_size {1}".format(howMany, \
                    cluster_size))


        # add all servers
        self.log.info("Rebalancing In with cluster size {0}".format(cluster_size))
        RebalanceTaskHelper.add_rebalance_task(self.task_manager,
            [master],
            self.servers[1:cluster_size],
            [])
        self.log.info("Initial Load with key-count {0}".format(self.keys_count))
        RebalanceBaseTest.load_all_buckets_task(rest, self.task_manager,
            bucket_data, ram_load_ratio=self.load_ratio,
            keys_count=self.keys_count)

        while howMany > 0:
            if len(rest.node_statuses()) < 2:
                break
            if self.checkResidentRatio:
                self.log.info("Getting the resident ratio stats before failover/rebalancing out the nodes")
                RebalanceBaseTest.check_resident_ratio(self, master)

            # Never pick master node - The modified function takes care of this one.
            rebalanceOutNode = RebalanceHelper.pick_node(master)

            self.log.info(
                "Incrementally rebalancing out node {0}:{1}".format(rebalanceOutNode.ip, rebalanceOutNode.port))
            # rebalance out a server
            RebalanceTaskHelper.add_rebalance_task(self.task_manager,
                [master],
                [],
                [rebalanceOutNode], do_stop=self.do_stop)
            # wait for loading tasks to finish
            RebalanceBaseTest.finish_all_bucket_tasks(rest, bucket_data)
            self.log.info("Completed Loading and Rebalacing out")

            if self.checkResidentRatio:
                self.log.info("Getting the resident ratio stats after rebalancing out the nodes")
                RebalanceBaseTest.check_resident_ratio(self, master)

            # verification step
            if self.do_verify:
                self.log.info("Verifying with KV store")
                RebalanceBaseTest.do_kv_and_replica_verification(master, self.task_manager,
                    bucket_data, self.replica, self)
            else:
                self.log.info("No Verification with KV store")
            howMany = howMany - 1

    def test_load(self):
        self._common_test_body()


class StopRebalanceAfterFailoverTests(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    def stop_rebalance(self):
        self._common_test_body()

    def _common_test_body(self):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        ClusterOperationHelper.add_all_nodes_or_assert(master, self.servers, creds, self)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding nodes")

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceHelper.pick_node(master)
            distribution = RebalanceBaseTest.get_distribution(self.load_ratio)
            RebalanceBaseTest.load_data_for_buckets(rest, self.load_ratio, distribution, [master], bucket_data, self)
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
            RebalanceBaseTest.replication_verification(master, bucket_data, self.replica, self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            time.sleep(20)

            RebalanceBaseTest.replication_verification(master, bucket_data, self.replica, self)
            nodes = rest.node_statuses()


class IncrementalRebalanceWithParallelReadTests(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    def _common_test_body(self, moxi=False):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        for server in self.servers[1:]:
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            self.log.info("adding node {0}:{1} and rebalance afterwards".format(server.ip, server.port))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            for name in bucket_data:
                inserted_keys, rejected_keys = \
                MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.servers[0]],
                    name=name,
                    ram_load_ratio=-1,
                    number_of_items=self.keys_count,
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
        self._common_test_body(moxi=True)

    def test_10k_memcached(self):
        self._common_test_body()


class FailoverRebalanceRepeatTests(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings
        rebalanced_servers = [master]
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        self.log.info("INTIAL LOAD")
        RebalanceBaseTest.load_all_buckets_task(rest, self.task_manager, bucket_data, self.load_ratio,
            keys_count=self.keys_count)

        for name in bucket_data:
            for thread in bucket_data[name]["threads"]:
                bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()

        for server in self.servers[1:]:
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            #do this 2 times , start rebalance , failover the node , remove the node and rebalance
            for i in range(0, self.num_rebalance):
                distribution = RebalanceBaseTest.get_distribution(self.load_ratio)
                RebalanceBaseTest.load_data_for_buckets(rest, self.load_ratio, distribution, [master], bucket_data,
                    self)
                self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
                otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
                msg = "unable to add node {0} to the cluster {1}"
                self.assertTrue(otpNode, msg.format(server.ip, master.ip))
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                self.assertTrue(rest.monitorRebalance(),
                    msg="rebalance operation failed after adding node {0}".format(server.ip))
                rebalanced_servers.append(server)
                RebalanceBaseTest.replication_verification(master, bucket_data, self.replica, self, True)
                rest.fail_over(otpNode.id)
                self.log.info("failed over {0}".format(otpNode.id))
                time.sleep(10)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                    ejectedNodes=[otpNode.id])
                msg = "rebalance failed while removing failover nodes {0}".format(otpNode.id)
                self.assertTrue(rest.monitorRebalance(), msg=msg)
                #now verify the numbers again ?
                RebalanceBaseTest.replication_verification(master, bucket_data, self.replica, self, True)
                #wait 6 minutes
                time.sleep(6 * 60)

            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            distribution = RebalanceBaseTest.get_distribution(self.load_ratio)
            RebalanceBaseTest.load_data_for_buckets(rest, self.load_ratio, distribution, rebalanced_servers, bucket_data, self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)
            RebalanceBaseTest.replication_verification(master, bucket_data, self.replica, self, True)


    def test_load(self):
        self._common_test_body()


class RebalanceInOutWithParallelLoad(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    def _common_test_body(self):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        self.log.info("INTIAL LOAD")
        RebalanceBaseTest.load_all_buckets_task(rest, self.task_manager, bucket_data, self.load_ratio,
            keys_count=self.keys_count)

        rebalance_out = False
        for server in self.servers[1:]:
            if rebalance_out:
                # Pick a node to rebalance out, other than master
                ejectedNodes = [RebalanceHelper.pick_node(master)]
            else:
                ejectedNodes = []
            current_nodes = RebalanceHelper.getOtpNodeIds(master)
            self.log.info("current nodes : {0}".format(current_nodes))
            self.log.info("adding node {0}, removing node {1} and rebalance afterwards".format(server.ip,
                [node.ip for node in ejectedNodes]))

            self.log.info("START PARALLEL LOAD")
            RebalanceBaseTest.tasks_for_buckets(rest, self.task_manager, bucket_data,
                DELETE_RATIO=self.delete_ratio,
                ACCESS_RATIO=self.access_ratio, EXPIRY_RATIO=self.expiry_ratio)

            self.log.info("INCREMENTAL REBALANCE IN/OUT")
            # rebalance in/out a server
            RebalanceTaskHelper.add_rebalance_task(self.task_manager,
                [master],
                [server],
                ejectedNodes, do_stop=self.do_stop)
            # wait for loading tasks to finish
            RebalanceBaseTest.finish_all_bucket_tasks(rest, bucket_data)

            # Make sure we have at least 3 nodes, for replica=2
            if len(current_nodes) > 2:
                rebalance_out = True

        if self.do_verify:
            self.log.info("VERIFICATION")
            RebalanceBaseTest.do_kv_and_replica_verification(master,
                self.task_manager,
                bucket_data,
                self.replica,
                self)
        else:
            self.log.info("NO VERIFICATION")

    def test_load(self):
        self._common_test_body()


class RebalanceOutWithFailover(unittest.TestCase):
    def setUp(self):
        RebalanceBaseTest.common_setup(self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self)

    def _common_test_body(self):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        # add all servers
        self.log.info("Initially rebalancing in the nodes")
        RebalanceTaskHelper.add_rebalance_task(self.task_manager,
            [master],
            self.servers[1:],
            [], monitor=True, do_stop=self.do_stop)

        self.log.info("Initial loading of data")
        RebalanceBaseTest.load_all_buckets_task(rest, self.task_manager,
            bucket_data, self.load_ratio,
            keys_count=self.keys_count)

        nodes = rest.node_statuses()

        for node in nodes[1:]:
            # Get the current cluster size, we will continnue fail-over till current_cluster_size= replica+1
            current_cluster_len = len(rest.node_statuses())
            if current_cluster_len < (self.replica + 1):
                self.log.info(
                    "Replica count {0} is greater than the current cluster-size{1}, stopping failover test.".format(
                        self.replica, current_cluster_len))

            else:
                # Never pick master node
                if node.ip != master.ip:
                    self.log.info("Starting Parallel Load ..")
                    RebalanceBaseTest.tasks_for_buckets(rest, self.task_manager, bucket_data,
                        DELETE_RATIO=self.delete_ratio,
                        ACCESS_RATIO=self.access_ratio, EXPIRY_RATIO=self.expiry_ratio)

                    # Pick a Node to failover
                    toBeEjectedNode = RebalanceHelper.pick_node(master)
                    self.log.info("Starting Failover and Rebalance Out  for node {0}:{1}".format(toBeEjectedNode.ip,
                        toBeEjectedNode.port))

                    # rebalance Out
                    RebalanceTaskHelper.add_failover_task(self.task_manager,
                        [master],
                        [toBeEjectedNode], True)

                    self.log.info(
                        "Completed Failover for node {0}:{1}".format(toBeEjectedNode.ip, toBeEjectedNode.port))
                    # rebalance Out
                    RebalanceTaskHelper.add_rebalance_task(self.task_manager,
                        [master],
                        [],
                        [toBeEjectedNode], do_stop=self.do_stop, monitor=True)

                    # wait for all tasks to finish
                    RebalanceBaseTest.finish_all_bucket_tasks(rest, bucket_data)
                    self.log.info("Completed Load, Failover and Rebalance Out. ")

                    # verification step
                    if self.do_verify:
                        self.log.info("Verifying with KV-store")
                        RebalanceBaseTest.do_kv_and_replica_verification(master, self.task_manager,
                            bucket_data, self.replica, self, failed_over=True)
                    else:
                        self.log.info("No verification with KV-store specified")
                        # at least 2 nodes required per loop to rebalance out and verify replication
            self.log.info("Completed Load and Rebalance-Out")

    def test_load(self):
        self._common_test_body()


class RebalanceTaskHelper():
    @staticmethod
    def add_rebalance_task(tm, servers, to_add, to_remove, monitor=True,
                           do_stop=False, progress=30):
        _t = task.RebalanceTask(servers, to_add, to_remove, do_stop=do_stop,
            progress=progress)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor=monitor)

    @staticmethod
    def add_failover_task(tm, servers, to_remove, monitor=False):
        _t = task.FailOverTask(servers, to_remove)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)

    @staticmethod
    def incremental_rebalance_in_tasks(tm, servers, to_add, delay=0):
        return [RebalanceTaskHelper.schedule_task_helper(tm, _t, True, delay) for _t in
        [task.RebalanceTask(servers, [new_server], []) for new_server in to_add]]

    @staticmethod
    def incremental_rebalance_out_tasks(tm, servers, to_remove):
        return [RebalanceTaskHelper.schedule_task_helper(tm, _t, True) for _t in
        [task.RebalanceTask(servers, [], [old_server]) for old_server in to_remove]]

    @staticmethod
    def add_doc_gen_task(tm, rest, count, bucket="default", kv_store=None, store_enabled=True,
                         kv_template=None, seed=None, sizes=None, expiration=None,
                         loop=False, monitor=False, doc_generators=None):
        doc_generators = doc_generators or DocumentGenerator.get_doc_generators(count, kv_template, seed, sizes)
        _t = task.LoadDocGeneratorTask(rest, doc_generators, bucket=bucket, kv_store=kv_store,
            store_enabled=store_enabled, expiration=expiration, loop=loop)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)

    @staticmethod
    def add_doc_del_task(tm, rest, keys, bucket="default", info=None,
                         kv_store=None, store_enabled=True,
                         monitor=False, delay=0):
        _t = task.DocumentDeleteTask(rest, keys, bucket, info, kv_store, store_enabled)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor, delay)

    @staticmethod
    def add_doc_exp_task(tm, rest, keys, bucket="default", info=None,
                         kv_store=None, store_enabled=True,
                         monitor=False, delay=0, expiration=5):
        _t = task.DocumentExpireTask(rest, keys, bucket, info, kv_store,
            store_enabled, expiration=expiration)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor, delay)

    @staticmethod
    def add_doc_get_task(tm, rest, keys, bucket="default", info=None,
                         loop=False, monitor=False, delay=0):
        _t = task.DocumentAccessTask(rest, keys, bucket=bucket, info=info, loop=loop)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor, delay)

    @staticmethod
    def add_kv_integrity_helper(tm, rest, kv_store, bucket="default", monitor=True):
        _t = task.KVStoreIntegrityTask(rest, kv_store, bucket)
        return RebalanceTaskHelper.schedule_task_helper(tm, _t, monitor)

    @staticmethod
    def schedule_task_helper(tm, task, monitor=False, delay=0):
        tm.schedule(task, delay)
        if monitor:
            return task.result()
        return task


