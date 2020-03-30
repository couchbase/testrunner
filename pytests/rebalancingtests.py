import hashlib
import time
import uuid
import unittest
import json
import sys
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from mc_bin_client import MemcachedError
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import VBucketAwareMemcached
from threading import Thread
from memcached.helper.data_helper import DocumentGenerator
from memcached.helper.old_kvstore import ClientKeyValueStore
import traceback


class RebalanceBaseTest(unittest.TestCase):
    @staticmethod
    def common_setup(input, testcase, bucket_ram_ratio=(2.8 / 3.0), replica=0):
        log = logger.Logger.get_logger()
        servers = input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
        serverInfo = servers[0]

        log.info('picking server : {0} as the master'.format(serverInfo))
        #if all nodes are on the same machine let's have the bucket_ram_ratio as bucket_ram_ratio * 1/len(servers)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))
        if "ascii" in TestInputSingleton.input.test_params\
        and TestInputSingleton.input.test_params["ascii"].lower() == "true":
            BucketOperationHelper.create_multiple_buckets(serverInfo, replica, node_ram_ratio * bucket_ram_ratio,
                                                          howmany=1, sasl=False)
        else:
            BucketOperationHelper.create_multiple_buckets(serverInfo, replica, node_ram_ratio * bucket_ram_ratio,
                                                          howmany=1, sasl=True)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket.name)
            testcase.assertTrue(ready, "wait_for_memcached failed")

    @staticmethod
    def common_tearDown(servers, testcase):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
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
            test.assertTrue(RebalanceHelper.wait_for_replication(rest.get_nodes(), timeout=300),
                            msg="replication did not complete after 5 minutes")
            #run expiry_pager on all nodes before doing the replication verification
            for bucket in buckets:
                ClusterOperationHelper.set_expiry_pager_sleep_time(master, bucket.name)
                test.log.info("wait for expiry pager to run on all these nodes")
                time.sleep(30)
                ClusterOperationHelper.set_expiry_pager_sleep_time(master, bucket.name, 3600)
                ClusterOperationHelper.set_expiry_pager_sleep_time(master, bucket.name)
                replica_match = RebalanceHelper.wait_till_total_numbers_match(bucket=bucket.name,
                                                                              master=master,
                                                                              timeout_in_seconds=300)
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
    def rebalance_in(servers, how_many, monitor=True):
        return RebalanceHelper.rebalance_in(servers, how_many, monitor)


#load data. add one node rebalance , rebalance out
class RebalanceTestsUnderLoad(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    def _add_items(self, seed, bucket, num_of_docs, kv_store):
        master = self._servers[0]
        rest = RestConnection(master)
        params = {"sizes": [128], "count": num_of_docs,
                  "seed": seed}
        load_set_ops = {"ops": "set", "bucket": bucket.name}
        load_set_ops.update(params)
        load_set_ops["bucket"] = bucket.name
        load_set_ops_task_def = RebalanceDataGenerator.create_loading_tasks(load_set_ops)
        thread = RebalanceDataGenerator.start_load(rest, bucket.name, load_set_ops_task_def, kv_store)
        thread.start()
        thread.join()


    def test_rebalance_out(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        log = logger.Logger().get_logger()
        master = self._servers[0]
        num_of_docs = TestInputSingleton.input.param("num_of_docs", 100000)
        replica = TestInputSingleton.input.param("replica", 100000)
        add_items_count = TestInputSingleton.input.param("num_of_creates", 30000)
        size = TestInputSingleton.input.param("item_size", 256)
        params = {"sizes": [size], "count": num_of_docs, "seed": str(uuid.uuid4())[:7]}
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        bucket_data = {}
        generators = {}
        for bucket in buckets:
            bucket_data[bucket.name] = {"kv_store": ClientKeyValueStore()}

        rebalanced_in, which_servers = RebalanceBaseTest.rebalance_in(self._servers, len(self.servers) - 1)
        self.assertTrue(rebalanced_in, msg="unable to add and rebalance more nodes")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes {0}".format(
                            [node.id for node in rest.node_statuses()]))
        while len(rest.node_statuses()) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceHelper.pick_node(master)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            for bucket in buckets:
                kv_store = bucket_data[bucket.name]["kv_store"]
                add_items_seed = str(uuid.uuid4())[:7]
                self._add_items(add_items_seed, bucket, add_items_count, kv_store)
                errors = RebalanceDataGenerator.do_verification(kv_store, rest, bucket.name)
                if errors:
                    log.error("verification returned {0} errors".format(len(errors)))
                load_set_ops = {"ops": "set", "bucket": bucket.name}
                load_set_ops.update(params)
                load_delete_ops = {"ops": "delete", "bucket": bucket.name,
                                   "sizes": [size], "count": add_items_count // 5, "seed": add_items_seed}
                thread = RebalanceDataGenerator.start_load(rest, bucket.name,
                    RebalanceDataGenerator.create_loading_tasks(load_set_ops), kv_store)
                generators["set"] = {"thread": thread}
                #restart three times
                generators["set"]["thread"].start()
                thread = RebalanceDataGenerator.start_load(rest, bucket.name,
                    RebalanceDataGenerator.create_loading_tasks(load_delete_ops), kv_store)
                generators["delete"] = {"thread": thread}
                generators["delete"]["thread"].start()
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))

            for bucket in buckets:
                kv_store = bucket_data[bucket.name]["kv_store"]
                errors = RebalanceDataGenerator.do_verification(kv_store, rest, bucket.name)
                if errors:
                    log.error("verification returned {0} errors".format(len(errors)))
            generators["set"]["thread"].join()
            generators["delete"]["thread"].join()
            for bucket in buckets:
                kv_store = bucket_data[bucket.name]["kv_store"]
                bucket_data[bucket.name]["items_inserted_count"] = len(kv_store.valid_items())
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

    def test_rebalance_in(self):
        log = logger.Logger().get_logger()
        master = self._servers[0]
        num_of_docs = TestInputSingleton.input.param("num_of_docs", 100000)
        replica = TestInputSingleton.input.param("replica", 100000)
        add_items_count = TestInputSingleton.input.param("num_of_creates", 30000)
        rebalance_in = TestInputSingleton.input.param("rebalance_in", 1)
        size = TestInputSingleton.input.param("item_size", 256)
        params = {"sizes": [size], "count": num_of_docs, "seed": str(uuid.uuid4())[:7]}
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        bucket_data = {}
        generators = {}
        for bucket in buckets:
            bucket_data[bucket.name] = {"kv_store": ClientKeyValueStore()}
        while len(rest.node_statuses()) < len(self._servers):
            for bucket in buckets:
                kv_store = bucket_data[bucket.name]["kv_store"]
                add_items_seed = str(uuid.uuid4())[:7]
                self._add_items(add_items_seed, bucket, add_items_count, kv_store)
                errors = RebalanceDataGenerator.do_verification(kv_store, rest, bucket.name)
                if errors:
                    log.error("verification returned {0} errors".format(len(errors)))
                load_set_ops = {"ops": "set", "bucket": bucket.name}
                load_set_ops.update(params)
                load_delete_ops = {"ops": "delete", "bucket": bucket.name,
                                   "sizes": [size], "count": add_items_count // 5, "seed": add_items_seed}
                thread = RebalanceDataGenerator.start_load(rest, bucket.name,
                    RebalanceDataGenerator.create_loading_tasks(load_set_ops), kv_store)
                generators["set"] = {"thread": thread}
                #restart three times
                generators["set"]["thread"].start()
                thread = RebalanceDataGenerator.start_load(rest, bucket.name,
                    RebalanceDataGenerator.create_loading_tasks(load_delete_ops), kv_store)
                generators["delete"] = {"thread": thread}
                generators["delete"]["thread"].start()
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            rebalanced_in, which_servers = RebalanceBaseTest.rebalance_in(self._servers, rebalance_in)
            self.assertTrue(rebalanced_in, msg="unable to add and rebalance more nodes")
            for bucket in buckets:
                kv_store = bucket_data[bucket.name]["kv_store"]
                errors = RebalanceDataGenerator.do_verification(kv_store, rest, bucket.name)
                if errors:
                    log.error("verification returned {0} errors".format(len(errors)))
            generators["set"]["thread"].join()
            generators["delete"]["thread"].join()
            for bucket in buckets:
                kv_store = bucket_data[bucket.name]["kv_store"]
                bucket_data[bucket.name]["items_inserted_count"] = len(kv_store.valid_items())
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)


class RebalanceDataGenerator(object):
    @staticmethod
    def job_info():
        return {"title": "Software Engineer",
                "desc": "We are looking to add a smart, energetic, and fast learning System Administrator and heliport manager, who will develop, manage, and support Couchbase growing infrastructure including building and maintaining virtual cloud for QE and development, automate deployments using Chef, manage the network, security, build infrastructure, and more; last but not least, help maintain our growing fleet of helicopters, who crashes more often than not due to inexperienced pilots.As an independent thinker, you may be free to replace some of the technologies we currently use with ones you feel are better.  This might occasionally lead to arguments."
        }


    @staticmethod
    #expect "count","sizes"
    def create_loading_tasks(params):
        log = logger.Logger.get_logger()
        task = {"ops": params["ops"], "seed": params["seed"], "bucket": params["bucket"], "docs": [], "sizes": params["sizes"]}
        log.info("params : {0}".format(params))
        for size in params["sizes"]:
            info = RebalanceDataGenerator.job_info()
            if "kv_template" not in params:
                kv_template = {"name": "employee-${prefix}-${seed}", "sequence": "${seed}",
                               "join_yr": 2007, "join_mo": 10, "join_day": 20,
                               "email": "${prefix}@couchbase.com",
                               "job_title": "Software Engineer-${padding}",
                               "desc": info["desc"].encode("utf-8", "ignore")}
            else:
                kv_template = params["kv_template"]

            options = {"size": size, "seed": params["seed"]}
            if "padding" in params:
                options["padding"] = params["padding"]
            docs = DocumentGenerator.make_docs(params["count"] // len(params["sizes"]), kv_template, options)
            task["docs"].append(docs)
        return task

    @staticmethod
    def start_load(rest, bucket, task, kvstore):
        thread = Thread(target=RebalanceDataGenerator.run_load,
                        name="run_load-{0}".format(str(uuid.uuid4())[0:7]),
                        args=(rest, bucket, task, kvstore))
        return thread



    @staticmethod
    def decode_ops(task):
        do_with_expiration = do_sets = do_gets = do_deletes = False
        if task["ops"] == "set":
            do_sets = True
        elif task["ops"] == "get":
            do_gets = True
        elif task["ops"] == "delete":
            do_deletes = True
        elif task["ops"] == "expire":
            do_with_expiration = True
        return do_sets, do_gets, do_deletes, do_with_expiration

    @staticmethod
    def do_verification(kv_store, rest, bucket):
        keys = list(kv_store.keys())
        smart = VBucketAwareMemcached(rest, bucket)
        validation_failures = {}
        for k in keys:
            expected = kv_store.read(k)
            if expected:
                if expected["status"] == "deleted":
                    # it should not exist there ?
                    try:
                        smart.memcached(k).get(k)
                        validation_failures[k] = ["deleted key"]
                    except MemcachedError:
                        pass
                elif expected["status"] == "expired":
                    try:
                        smart.memcached(k).get(k)
                        validation_failures[k] = ["expired key"]
                    except MemcachedError:
                        pass
                else:
                    try:
                       x, y, value = smart.memcached(k).get(k)
                       actualmd5 = hashlib.md5(value).digest()
                       if actualmd5 != expected["value"]:
                          validation_failures[k] = ["value mismatch"]
                    except  MemcachedError:
                       validation_failures[k] = ["key not found"]
        return validation_failures

    @staticmethod
    def run_load(rest, bucket, task, kv_store):
        smart = VBucketAwareMemcached(rest, bucket)
        docs_iterators = task["docs"]
        do_sets, do_gets, do_deletes, do_with_expiration = RebalanceDataGenerator.decode_ops(task)
        doc_ids = []
        expiration = 0
        if do_with_expiration:
            expiration = task["expiration"]

        for docs_iterator in docs_iterators:
            for value in docs_iterator:
                _value = value.encode("ascii", "ignore")
                _json = json.loads(_value, encoding="utf-8")
                _id = _json["meta"]["id"].encode("ascii", "ignore")
                _value = json.dumps(_json["json"]).encode("ascii", "ignore")
                #                    _value = json.dumps(_json)
                try:
                    RebalanceDataGenerator.do_mc(rest, smart, _id, _value,
                                                 kv_store, doc_ids, do_sets, do_gets, do_deletes,
                                                 do_with_expiration, expiration)
                except:
                    traceback.print_exc(file=sys.stdout)
                    #post the results into the queue
        return

    @staticmethod
    def do_mc(rest, smart, key, value, kv_store, doc_ids,
              do_sets, do_gets, do_deletes,
              do_with_expiration, expiration):
        retry_count = 0
        ok = False
        value = value.replace(" ", "")
        while retry_count < 5 and not ok:
            try:
                if do_sets:
                    smart.memcached(key).set(key, 0, 0, value)
                    kv_store.write(key, hashlib.md5(value).digest())
                if do_gets:
                    smart.memcached(key).get(key)
                if do_deletes:
                    smart.memcached(key).delete(key)
                    kv_store.delete(key)
                if do_with_expiration:
                    smart.memcached(key).set(key, 0, expiration, value)
                    kv_store.write(key, hashlib.md5(value).digest(), expiration)
                doc_ids.append(key)
                ok = True
            except MemcachedError as error:
                if error.status == 7:
                    smart.reset_vbuckets(rest, key)
                #                    traceback.print_exc(file=sys.stdout)
                retry_count += 1
