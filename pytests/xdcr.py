from TestInput import TestInputSingleton
import logger
import time
import unittest
import string
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from rebalancingtests import RebalanceDataGenerator
from memcached.helper.old_kvstore import ClientKeyValueStore
from memcached.helper.data_helper import VBucketAwareMemcached

class XDCRBaseTest(unittest.TestCase):
    @staticmethod
    def cluster_rebalance_in(testcase, servers, monitor=True):
        servers_a = testcase._input.clusters.get(0)
        servers_b = testcase._input.clusters.get(1)

        RebalanceHelper.rebalance_in(servers_a, len(servers_a) - 1, monitor=True)
        RebalanceHelper.rebalance_in(servers_b, len(servers_b) - 1, monitor=True)

    @staticmethod
    def common_setup(input, testcase):
        # Resource file has 'cluster' tag
        for key, servers in input.clusters.items():
            XDCRBaseTest.common_tearDown(servers, testcase)
            XDCRBaseTest.cluster_initialization(servers)
            XDCRBaseTest.create_buckets(servers, testcase, howmany=1)

        #temp fix to rebalance-in the nodes for each cluster
        if testcase._initialRebalance:
            XDCRBaseTest.cluster_rebalance_in(testcase, servers, monitor=True)

    @staticmethod
    def common_tearDown(servers, testcase):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        for server in servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)

    @staticmethod
    def choose_nodes(master, nodes, howmany):
        selected = []
        for node in nodes:
            if not XDCRBaseTest.contains(node.ip, master.ip) and\
               not XDCRBaseTest.contains(node.ip, '127.0.0.1'):
                selected.append(node)
                if len(selected) == howmany:
                    break
        return selected

    @staticmethod
    def contains(string1, string2):
        if string1 and string2:
            return string1.find(string2) != -1
        return False

    @staticmethod
    def cluster_initialization(servers):
        log = logger.Logger().get_logger()
        master = servers[0]
        log.info('picking server : {0} as the master'.format(master))
        # if all nodes are on the same machine let's have the bucket_ram_ratio
        # as bucket_ram_ratio * 1/len(servers)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota =
            int(info.mcdMemoryReserved * node_ram_ratio))

    @staticmethod
    def create_buckets(servers, testcase, howmany=1, replica=1,
                       bucket_ram_ratio=(2.0 / 3.0)):
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        master = servers[0]
        BucketOperationHelper.create_multiple_buckets(
            master, replica,node_ram_ratio * bucket_ram_ratio, howmany=howmany, sasl=False)
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(master,
                                                             bucket.name)
            testcase.assertTrue(ready, "wait_for_memcached failed")

    @staticmethod
    def poll_for_condition_rec(condition, sleep, num_iters):
        if num_iters == 0:
            return False
        else:
            if condition():
                return True
            else:
                time.sleep(sleep)
                return XDCRBaseTest.poll_for_condition_rec(condition, sleep,
                                                           (num_iters - 1))

    @staticmethod
    def poll_for_condition(condition, sleep, timeout):
        num_iters = timeout/sleep
        return XDCRBaseTest.poll_for_condition_rec(condition, sleep, num_iters)

    @staticmethod
    def verify_replicated_data(rest_conn, bucket, kvstore, sleep, timeout):
        # FIXME: Use ep-engine stats for verification
        def verify():
            errors = RebalanceDataGenerator.do_verification(kvstore, rest_conn,
                                                            bucket)
            if errors:
                return False
            else:
                return True

        return XDCRBaseTest.poll_for_condition(verify, sleep, timeout)

    @staticmethod
    def verify_replicated_revs(rest_conn_a, rest_conn_b, bucket, sleep, timeout):
        def verify():
            all_docs_resp_a = rest_conn_a.all_docs(bucket)
            all_docs_resp_b = rest_conn_b.all_docs(bucket)
            if all_docs_resp_a[u'total_rows'] != all_docs_resp_b[u'total_rows']:
                return False
            all_docs_a = all_docs_resp_a[u'rows']
            all_docs_b = all_docs_resp_b[u'rows']
            for i in range(all_docs_resp_a[u'total_rows']):
                doc_a = all_docs_a[i][u'id'], all_docs_a[i][u'value'][u'rev']
                doc_b = all_docs_b[i][u'id'], all_docs_b[i][u'value'][u'rev']
                if doc_a != doc_b:
                    return False
            return True

        return XDCRBaseTest.poll_for_condition(verify, sleep, timeout)

    @staticmethod
    def get_rev_info(rest_conn, bucket, keys):
        vbmc = VBucketAwareMemcached(rest_conn,bucket)
        ris = []
        for k in keys:
            mc = vbmc.memcached(k)
            ri = mc.getRev(k)
            ris.append(ri)
        return ris

    @staticmethod
    def verify_del_items(rest_conn_a, rest_conn_b, bucket, keys, sleep, timeout):
        def verify():
            rev_a = XDCRBaseTest.get_rev_info(rest_conn_a, bucket, keys)
            rev_b = XDCRBaseTest.get_rev_info(rest_conn_b, bucket, keys)
            return rev_a == rev_b
        return XDCRBaseTest.poll_for_condition(verify, sleep, timeout)


class XDCRTests(unittest.TestCase):

    def setUp(self):
        self.log = logger.Logger().get_logger()
        self._input = TestInputSingleton.input
        self._clusters = self._input.clusters
        self._buckets = ["bucket-0"]
        self._initialRebalance = TestInputSingleton.input.param("initRebalance", True)
        self._num_items = TestInputSingleton.input.param("num_items", 100000)
        self._failover_factor = TestInputSingleton.input.param("failover-factor", 1)
        self._fail_orchestrator_a = TestInputSingleton.input.param("fail-orchestrator-a", False)
        self._fail_orchestrator_b = TestInputSingleton.input.param("fail-orchestrator-b", False)
        self._poll_sleep = TestInputSingleton.input.param("poll_sleep", 5)
        self._poll_timeout = TestInputSingleton.input.param("poll_timeout", 300)
        self._params = {"sizes" : [128], "count" : self._num_items,
                        "padding":"cluster_a", "seed" : "cluster_a",
                        "bucket" : self._buckets[0]}
        self._params["kv_template"] = {"name": "employee-${prefix}-${seed}",
                                       "sequence": "${seed}","join_yr": 2007,
                                       "join_mo": 10, "join_day": 20,
                                       "email": "${prefix}@couchbase.com",
                                       "job_title": "Software Engineer-${padding}"}
        self._state = []
        if not self._clusters:
            self.log.info("No Cluster tags defined in resource file")
            exit(1)
        if len(self._clusters.items()) == 2:
            XDCRBaseTest.common_setup(self._input, self)
        else:
            self.log.info("Two clusters needed")
            exit(1)
        self.log.info("Setup")

    def tearDown(self):
        self.log.info("Teardown")
        for (rest_conn, cluster_ref, rep_database, rep_id) in self._state:
            rest_conn.stop_replication(rep_database, rep_id)
            rest_conn.remove_remote_cluster(cluster_ref)
        for id, servers in self._clusters.items():
            XDCRBaseTest.common_tearDown(servers, self)


    # --------------------------------------------------------------------------#
    # ----                 BASIC XDCR FUNCTIONAL TEST CASES                 --- #
    # --------------------------------------------------------------------------#
    def test_continuous_unidirectional_sets(self):
        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        # Start replication
        replication_type = "continuous"
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Verify replication
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
                                                            self._buckets[0],
                                                            kvstore,
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Verification of replicated data failed")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
                                                            rest_conn_b,
                                                            self._buckets[0],
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Verification of replicated revisions failed")

    def test_continuous_unidirectional_sets_deletes(self):
        cluster_ref_a= "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        # Start replication
        replication_type = "continuous"
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Do some deletes
        self._params["ops"] = "delete"
        self._params["count"] = self._num_items/5
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Verify replication
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
                                                            self._buckets[0],
                                                            kvstore,
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Verification of replicated data failed")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
                                                            rest_conn_b,
                                                            self._buckets[0],
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Verification of replicated revisions failed")

    def test_continuous_unidirectional_recreates(self):
        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        # Disable compaction, we should remove these
        # once the bug in comapctor get fixed
        #rest_conn_a.set_auto_compaction("false", 100, 100)
        #rest_conn_b.set_auto_compaction("false", 100, 100)

        # Start load
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Start replication
        replication_type = "continuous"
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Verify replicated data
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
                                                            self._buckets[0],
                                                            kvstore,
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Replication verification failed")

        # Delete all keys
        self._params["ops"] = "delete"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Verify replicated data
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
                                                            self._buckets[0],
                                                            kvstore,
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Replication verification failed")

        # Recreate the keys with different values
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["padding"] = "recreated"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Verify replicated data
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
                                                            self._buckets[0],
                                                            kvstore,
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Replication verification failed")

    def test_continuous_unidirectional_deletes_1(self):
        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        # Load some data on cluster a. Do it a few times so that the seqnos are
        # bumped up and then delete it.
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        load_thread_list = []
        for i in [1, 2, 3]:
            task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
            load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                            self._buckets[0],
                                                            task_def, kvstore)
            load_thread_list.append(load_thread)

        for lt in load_thread_list:
            lt.start()
        for lt in load_thread_list:
            lt.join()
        time.sleep(10)

        self._params["ops"] = "delete"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Load the same data on cluster b but only once. This will cause the
        # seqno values to be lower than those on cluster a allowing the latter
        # to win during conflict resolution later. Then delete this data, too.
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_b,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()
        time.sleep(10)

        self._params["ops"] = "delete"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_b,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Start replication to replicate the deletes from cluster a (having
        # higher seqnos) to cluster b.
        replication_type = "continuous"
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Verify replicated data
        self.assertTrue(XDCRBaseTest.verify_del_items(rest_conn_a,
                                                      rest_conn_b,
                                                      self._buckets[0],
                                                      kvstore.keys(),
                                                      self._poll_sleep,
                                                      self._poll_timeout),
                        "Changes feed verification failed")

    def test_continuous_unidirectional_deletes_2(self):
        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        # Load some data on cluster a. Do it a few times so that the seqnos are
        # bumped up and then delete it.
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        load_thread_list = []
        for i in [1, 2, 3]:
            task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
            load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                            self._buckets[0],
                                                            task_def, kvstore)
            load_thread_list.append(load_thread)

        for lt in load_thread_list:
            lt.start()
        for lt in load_thread_list:
            lt.join()
        time.sleep(10)

        self._params["ops"] = "delete"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore)
        load_thread.start()
        load_thread.join()

        # Start replication to replicate the deletes from cluster a
        # to cluster b where the keys never existed.
        replication_type = "continuous"
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        time.sleep(15)

        # Verify replicated data#
        self.assertTrue(XDCRBaseTest.verify_del_items(rest_conn_a,
                                                      rest_conn_b,
                                                      self._buckets[0],
                                                      kvstore.keys(),
                                                      self._poll_sleep,
                                                      self._poll_timeout),
                        "Changes feed verification failed")

    def test_continuous_bidirectional_sets(self):
        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        load_thread_list = []

        # Load cluster a with keys that will be exclusively owned by it
        kvstore_a0 = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["seed"] = "cluster-a"
        self._params["count"] = self._num_items/4
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore_a0)
        load_thread_list.append(load_thread)

        # Load cluster a with keys that it will share with cluster b and which
        # will win during conflict resolution
        kvstore_a1 = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["seed"] = "cluster-a-wins"
        self._params["padding"] = "cluster-a-wins"
        self._params["count"] = self._num_items/4

        # Mutating these keys several times will increase their seqnos and allow
        # them to win during conflict resolution
        for i in [1, 2, 3]:
            task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
            load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                            self._buckets[0],
                                                            task_def,
                                                            kvstore_a1)
            load_thread_list.append(load_thread)

        # Load cluster a with keys that it will share with cluster b but which
        # will lose during conflict resolution
        kvstore_a2 = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["seed"] = "cluster-b-wins"
        self._params["padding"] = "cluster-a-loses"
        self._params["count"] = self._num_items/4
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
                                                        self._buckets[0],
                                                        task_def, kvstore_a2)
        load_thread_list.append(load_thread)

        # Load cluster b with keys that will be exclusively owned by it
        kvstore_b0 = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["seed"] = "cluster-b"
        self._params["count"] = self._num_items/4
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_b,
                                                        self._buckets[0],
                                                        task_def, kvstore_b0)
        load_thread_list.append(load_thread)

        # Load cluster b with keys that it will share with cluster a and which
        # will win during conflict resolution
        kvstore_b1 = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["seed"] = "cluster-b-wins"
        self._params["padding"] = "cluster-b-wins"
        self._params["count"] = self._num_items/4

        # Mutating these keys several times will increase their seqnos and allow
        # them to win during conflict resolution
        for i in [1, 2, 3]:
            task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
            load_thread = RebalanceDataGenerator.start_load(rest_conn_b,
                                                            self._buckets[0],
                                                            task_def,
                                                            kvstore_b1)
            load_thread_list.append(load_thread)

        # Load cluster b with keys that it will share with cluster a but which
        # will lose during conflict resolution
        kvstore_b2 = ClientKeyValueStore()
        self._params["ops"] = "set"
        self._params["seed"] = "cluster-a-wins"
        self._params["padding"] = "cluster-b-loses"
        self._params["count"] = self._num_items/4
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_b,
                                                        self._buckets[0],
                                                        task_def, kvstore_b2)
        load_thread_list.append(load_thread)

        # Setup bidirectional replication between cluster a and cluster b
        replication_type = "continuous"
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        rest_conn_b.add_remote_cluster(master_a.ip, master_a.port,
                                       master_a.rest_username,
                                       master_a.rest_password, cluster_ref_a)
        (rep_database_a, rep_id_a) = rest_conn_a.start_replication(
                                        replication_type, self._buckets[0],
                                        cluster_ref_b)
        (rep_database_b, rep_id_b) = rest_conn_b.start_replication(
                                        replication_type, self._buckets[0],
                                        cluster_ref_a)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database_a, rep_id_a))
        self._state.append((rest_conn_b, cluster_ref_a, rep_database_b, rep_id_b))

        # Start all loads concurrently and wait for them to end
        for lt in load_thread_list:
            lt.start()
        for lt in load_thread_list:
            lt.join()
        self.log.info("All loading threads finished")

        # Verify replicated data
        for rest_conn in [rest_conn_a, rest_conn_b]:
            for kvstore in [kvstore_a0, kvstore_a1, kvstore_b0, kvstore_b1]:
                self.assertTrue(
                    XDCRBaseTest.verify_replicated_data(rest_conn,
                                                        self._buckets[0],
                                                        kvstore,
                                                        self._poll_sleep,
                                                        self._poll_timeout),
                    "Verification of replicated data failed")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
                                                            rest_conn_b,
                                                            self._buckets[0],
                                                            self._poll_sleep,
                                                            self._poll_timeout),
                        "Verification of replicated revisions failed")


    # --------------------------------------------------------------------------#
    # ----          TEST CASES WITH REBALANCE (SOURCE)                      --- #
    # --------------------------------------------------------------------------#
    def test_rebalance_in_source_sets(self):
        # This test starts with a 1-1 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger rebalace-in on source
        # cluster a, to create a 2-1 replication. After all loading finish,
        # verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("No initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()

        # Trigger rebalance
        self.log.info("DURING replication, start rebalancing...")
        servers_a = self._input.clusters.get(0)
        self.log.info("REBALANCING IN Cluster A ...")
        RebalanceHelper.rebalance_in(servers_a, len(servers_a)-1, monitor=False)
        self.assertTrue(rest_conn_a.monitorRebalance(),
            msg="rebalance operation on cluster {0}".format(servers_a))

        self.log.info("ALL rebalancing done...")

        # Wait for loading to finish
        load_thread.join()
        self.log.info("All deleting threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")

    def test_rebalance_out_source_sets(self):
        # This test starts with a 2-2 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger rebalace-out on source
        # cluster a by evicting node 1 from cluster a, and create a 1-2 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()

        # sleep a while to allow more data loaded
        time.sleep(5)

        # Trigger rebalance
        self.log.info("DURING replication, start rebalancing...")
        self.log.info("REBALANCING OUT Cluster A ...")
        nodes_a = rest_conn_a.node_statuses()
        while len(nodes_a) > 1:
            toBeEjectedNode = RebalanceHelper.pick_node(master_a)
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master_a)))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest_conn_a.rebalance(otpNodes=[node.id for node in rest_conn_a.node_statuses()], \
                ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest_conn_a.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeEjectedNode.id))
            nodes_a = rest_conn_a.node_statuses()

        self.log.info("ALL rebalancing done...")

        # Wait for loading to finish
        load_thread.join()
        self.log.info("All deleting threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")



    # --------------------------------------------------------------------------#
    # ----           TEST CASES WITH FAILOVER (SOURCE)                      --- #
    # --------------------------------------------------------------------------#
    def test_failover_source_sets(self):
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        # This test starts with a 2-2 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger failover of one node
        # on source cluster , resulting a  1-2 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()
        # sleep a while to allow more data loaded
        time.sleep(5)

        self.log.info("current nodes on source cluster: {0}".format(RebalanceHelper.getOtpNodeIds(master_a)))

        # Trigger failover, we fail over one node each time until there is only one node remaining
        self.log.info("DURING replication, start failover...")
        self.log.info("FAILOVER nodes on Cluster A ...")
        nodes_a = rest_conn_a.node_statuses()
        while len(nodes_a) > 1:
            toBeFailedOverNode = RebalanceHelper.pick_node(master_a)
            self.log.info("failover node {0}".format(toBeFailedOverNode.id))
            rest_conn_a.fail_over(toBeFailedOverNode)
            self.log.info("rebalance after failover")
            rest_conn_a.rebalance(otpNodes=[node.id for node in rest_conn_a.node_statuses()], \
                ejectedNodes=[toBeFailedOverNode.id])
            self.assertTrue(rest_conn_a.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeFailedOverNode.id))
            nodes_a = rest_conn_a.node_statuses()

        self.log.info("ALL failed over done...")

        # Wait for loading threads to finish
        for lt in load_thread_list:
            lt.join()
        self.log.info("All loading threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")



    # --------------------------------------------------------------------------#
    # ----           TEST CASES WITH REBALANCE (DESTINATION)                --- #
    # --------------------------------------------------------------------------#
    def test_rebalance_in_dest_sets(self):
        # This test starts with a 1-1 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger rebalace-in on the
        # destination cluster b, to create a 1-2 replication. After all loading
        # finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("No initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()

        # Trigger rebalance
        self.log.info("DURING replication, start rebalancing...")
        servers_b = self._input.clusters.get(1)
        self.log.info("REBALANCING IN Cluster B ...")
        RebalanceHelper.rebalance_in(servers_b, len(servers_b)-1, monitor=False)
        self.assertTrue(rest_conn_b.monitorRebalance(),
            msg="rebalance operation on cluster {0}".format(servers_b))

        self.log.info("ALL rebalancing done...")

        # Wait for loading to finish
        load_thread.join()
        self.log.info("All deleting threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")

    def test_rebalance_out_dest_sets(self):
        # This test starts with a 2-2 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger rebalace-out on the
        # destination cluster by evicting node 1, and create a 2-1 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()

        # sleep a while to allow more data loaded
        time.sleep(5)

        # Trigger rebalance
        self.log.info("DURING replication, start rebalancing...")
        self.log.info("REBALANCING OUT Cluster B ...")
        nodes_b = rest_conn_b.node_statuses()

        # kick out one NON-master node in cluster
        while len(nodes_b) > 1:
            victim_b = self._input.clusters.get(1)[0]
            toBeEjectedNode = RebalanceHelper.pick_node(victim_b)
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(victim_b)))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest_conn_b.rebalance(otpNodes=[node.id for node in rest_conn_b.node_statuses()], \
                ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest_conn_b.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeEjectedNode.id))
            nodes_b = rest_conn_b.node_statuses()

        self.log.info("ALL rebalancing done...")

        # Wait for loading to finish
        load_thread.join()
        self.log.info("All deleting threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")


    # --------------------------------------------------------------------------#
    # ----           TEST CASES WITH FAILOVER (DESTINATION)                 --- #
    # --------------------------------------------------------------------------#
    def test_failover_dest_sets(self):
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        # This test starts with a 2-2 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger failover of one node
        # on source cluster , resulting a  1-2 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()
        # sleep a while to allow more data loaded
        time.sleep(5)

        self.log.info("current nodes on source cluster: {0}".format(RebalanceHelper.getOtpNodeIds(master_a)))

        # Trigger failover, we fail over one node each time until there is only one node remaining
        self.log.info("DURING replication, start failover...")
        self.log.info("FAILOVER non-master nodes on Cluster B ...")
        nodes = rest_conn_b.node_statuses()
        while len(nodes) > 1:
            victim = self._input.clusters.get(1)[0]
            toBeFailedOverNode = RebalanceHelper.pick_node(victim)
            self.log.info("failover node {0}".format(toBeFailedOverNode.id))
            rest_conn_b.fail_over(toBeFailedOverNode)
            self.log.info("rebalance after failover")
            rest_conn_b.rebalance(otpNodes=[node.id for node in rest_conn_b.node_statuses()], \
                ejectedNodes=[toBeFailedOverNode.id])
            self.assertTrue(rest_conn_b.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeFailedOverNode.id))
            nodes = rest_conn_b.node_statuses()

        self.log.info("ALL failed over done...")

        # Wait for loading threads to finish
        for lt in load_thread_list:
            lt.join()
        self.log.info("All loading threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")


    # -----------------------------------------------------------#
    # ---- TEST CASES WITH REBALANCE (SOURCE AND DESTINATION) -- #
    # -----------------------------------------------------------#
    def test_rebalance_in_source_dest_sets(self):
        # This test starts with a 1-1 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger rebalace-in on both
        # source and destination clusters, to create a 2-2 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("No initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()

        # Trigger rebalance
        self.log.info("DURING replication, start rebalancing in on both clusters...")
        servers_a = self._input.clusters.get(0)
        self.log.info("REBALANCING IN Cluster A ...")
        RebalanceHelper.rebalance_in(servers_a, len(servers_a)-1, monitor=False)
        self.assertTrue(rest_conn_a.monitorRebalance(),
            msg="rebalance operation on cluster {0}".format(servers_a))
        self.log.info("REBALANCING IN Cluster A done ...")

        servers_b = self._input.clusters.get(1)
        self.log.info("REBALANCING IN Cluster B ...")
        RebalanceHelper.rebalance_in(servers_b, len(servers_b)-1, monitor=False)
        self.assertTrue(rest_conn_b.monitorRebalance(),
            msg="rebalance operation on cluster {0}".format(servers_b))

        self.log.info("ALL rebalancing done...")

        # Wait for loading to finish
        load_thread.join()
        self.log.info("All deleting threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")

    def test_rebalance_out_source_dest_sets(self):
        # This test starts with a 2-2 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger rebalace-out on the
        # both source and destination clusters, and create a 1-1 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()

        # sleep a while to allow more data loaded
        time.sleep(5)

        # Trigger rebalance
        self.log.info("DURING replication, start rebalancing out on both clusters...")
        self.log.info("REBALANCING OUT Cluster A ...")
        nodes_a = rest_conn_a.node_statuses()
        while len(nodes_a) > 1:
            toBeEjectedNode = RebalanceHelper.pick_node(master_a)
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master_a)))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest_conn_a.rebalance(otpNodes=[node.id for node in rest_conn_a.node_statuses()], \
                ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest_conn_a.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeEjectedNode.id))
            nodes_a = rest_conn_a.node_statuses()

        self.log.info("rebalance-out on cluster A is done")
        self.log.info("REBALANCING OUT Cluster B ...")
        nodes_b = rest_conn_b.node_statuses()
        # kick out NON-master node in dest cluster
        while len(nodes_b) > 1:
            victim_b = self._input.clusters.get(1)[0]
            toBeEjectedNode = RebalanceHelper.pick_node(victim_b)
            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(victim_b)))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest_conn_b.rebalance(otpNodes=[node.id for node in rest_conn_b.node_statuses()], \
                ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest_conn_b.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeEjectedNode.id))
            nodes_b = rest_conn_b.node_statuses()

        self.log.info("ALL rebalancing done...")

        # Wait for loading to finish
        load_thread.join()
        self.log.info("All deleting threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")

    # ----------------------------------------------------------------#
    # ----   TEST CASES WITH FAILOVER (SOURCE AND DESTINATION)    --- #
    # ----------------------------------------------------------------#
    def test_failover_source_dest_sets(self):
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        # This test starts with a 2-2 unidirectional replication from cluster a
        # to cluster b; during the replication, we trigger failover for both
        # clusters, resulting a  1-1 replication.
        # After all loading finish, verify data and rev on both clusters.
        replication_type = "continuous"
        self.log.info("Force initial rebalance.")

        cluster_ref_a = "cluster_ref_a"
        master_a = self._input.clusters.get(0)[0]
        rest_conn_a = RestConnection(master_a)

        cluster_ref_b = "cluster_ref_b"
        master_b = self._input.clusters.get(1)[0]
        rest_conn_b = RestConnection(master_b)

        self.log.info("START XDC replication...")

        # Start replication
        rest_conn_a.add_remote_cluster(master_b.ip, master_b.port,
                                       master_b.rest_username,
                                       master_b.rest_password, cluster_ref_b)
        (rep_database, rep_id) = rest_conn_a.start_replication(replication_type,
                                                               self._buckets[0],
                                                               cluster_ref_b)
        self._state.append((rest_conn_a, cluster_ref_b, rep_database, rep_id))

        # Start load
        self.log.info("START loading data...")
        load_thread_list = []
        kvstore = ClientKeyValueStore()
        self._params["ops"] = "set"
        task_def = RebalanceDataGenerator.create_loading_tasks(self._params)
        load_thread = RebalanceDataGenerator.start_load(rest_conn_a,
            self._buckets[0],
            task_def, kvstore)
        load_thread.start()
        # sleep a while to allow more data loaded
        time.sleep(5)

        self.log.info("current nodes on source cluster: {0}".format(RebalanceHelper.getOtpNodeIds(master_a)))

        # Trigger failover, we fail over one node each time until there is only one node remaining
        self.log.info("DURING replication, start failover on both clusters...")

        self.log.info("FAILOVER nodes on Cluster A ...")
        nodes_a = rest_conn_a.node_statuses()
        while len(nodes_a) > 1:
            toBeFailedOverNode = RebalanceHelper.pick_node(master_a)
            self.log.info("failover node {0}".format(toBeFailedOverNode.id))
            rest_conn_a.fail_over(toBeFailedOverNode)
            self.log.info("rebalance after failover")
            rest_conn_a.rebalance(otpNodes=[node.id for node in rest_conn_a.node_statuses()], \
                ejectedNodes=[toBeFailedOverNode.id])
            self.assertTrue(rest_conn_a.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeFailedOverNode.id))
            nodes_a = rest_conn_a.node_statuses()

        self.log.info("FAILOVER of cluster A is done ...")

        self.log.info("FAILOVER non-master nodes on Cluster B ...")
        nodes = rest_conn_b.node_statuses()
        while len(nodes) > 1:
            victim = self._input.clusters.get(1)[0]
            toBeFailedOverNode = RebalanceHelper.pick_node(victim)
            self.log.info("failover node {0}".format(toBeFailedOverNode.id))
            rest_conn_b.fail_over(toBeFailedOverNode)
            self.log.info("rebalance after failover")
            rest_conn_b.rebalance(otpNodes=[node.id for node in rest_conn_b.node_statuses()], \
                ejectedNodes=[toBeFailedOverNode.id])
            self.assertTrue(rest_conn_b.monitorRebalance(),
                msg="rebalance operation failed after removing node {0}".format(toBeFailedOverNode.id))
            nodes = rest_conn_b.node_statuses()

        self.log.info("ALL failed over done...")

        # Wait for loading threads to finish
        for lt in load_thread_list:
            lt.join()
        self.log.info("All loading threads finished")

        # sleep a while, start verification too early will slow down the replication
        sleep_time = 60
        self.log.info("sleep for {0} seconds before verification...".format(sleep_time))
        time.sleep(sleep_time)

        # Verify replication
        self.log.info("START data verification at cluster A...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_a,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START data verification at cluster B...")
        self.assertTrue(XDCRBaseTest.verify_replicated_data(rest_conn_b,
            self._buckets[0],
            kvstore,
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated data failed")

        self.log.info("START revision verification on both clusters...")
        self.assertTrue(XDCRBaseTest.verify_replicated_revs(rest_conn_a,
            rest_conn_b,
            self._buckets[0],
            self._poll_sleep,
            self._poll_timeout),
            "Verification of replicated revisions failed")

