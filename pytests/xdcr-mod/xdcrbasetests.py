import sys
import time
import unittest
import logger
import copy

from membase.api.rest_client import RestConnection
from couchbase.cluster import Cluster
from TestInput import TestInputSingleton
from memcached.helper.kvstore import KVStore
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import VBucketAwareMemcached

from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from basetestcase import BaseTestCase
#===============================================================================
# class: XDCRConstants
# Constants used in XDCR application testing. It includes input parameters as well
# as internal constants used in the code.
# All string literals used in the XDCR code shall be defined in this class.
#===============================================================================

class XDCRConstants:
    INPUT_PARAM_REPLICATION_DIRECTION = "rdirection"
    INPUT_PARAM_CLUSTER_TOPOLOGY = "ctopology"

    #CLUSTER_TOPOLOGY_TYPE_LINE = "line"
    CLUSTER_TOPOLOGY_TYPE_CHAIN = "chain"
    CLUSTER_TOPOLOGY_TYPE_STAR = "star"

    REPLICATION_TYPE_CONTINUOUS = "continuous"
    REPLICATION_DIRECTION_UNIDIRECTION = "unidirection"
    REPLICATION_DIRECTION_BIDIRECTION = "bidirection"

#===============================================================================
# class: XDCRBaseTest
# This class is the base class (super class) for all the test classes written
# for XDCR. It configures the cluster as defined in the <cluster-file>.ini
# passed as an command line argument "-i" while running the test using testrunner.
# The child classes can override the following methods if required.
# - setup_extended
# - teardown_extended
# Note:
# It assumes that the each node is physically up and running.
# In ini file , the cluster name should start with "cluster" (prefix). 
#===============================================================================

class XDCRBaseTest(unittest.TestCase):
    def setUp(self):
        try:
            self._log = logger.Logger.get_logger()
            self._log.info("SetUp process started ...")
            self._input = TestInputSingleton.input
            self._cluster_helper = Cluster()

            self._init_parameters()

            if not self._input.param("skip_cleanup", False):
                self._cleanup_previous_setup()

            self._init_clusters()
            self.setup_extended()
            self._log.info("SetUp process completed...")
        except:
            self._log.error("Error while setting up clusters: %s", sys.exc_info()[0])
            self._cleanup_broken_setup()
            raise

    def tearDown(self):
        self.teardown_extended()
        self._do_cleanup()
        self._cluster_helper.shutdown()

    def _cleanup_previous_setup(self):
        self.teardown_extended()
        self._do_cleanup()

    def test_setUp(self):
        pass

    def _init_parameters(self):
        self._log.info("Initializing input parameters started...")
        self._clusters_dic = self._input.clusters # clusters is declared as dic in TestInput which is unordered.
        self._clusters_keys_olst = range(
            len(self._clusters_dic)) #clusters are populated in the dic in testrunner such that ordinal is the key.
        #orderedDic cannot be used in order to maintain the compability with python 2.6
        self._cluster_names_dic = {} # populated in _init_clusters() method
        self._cluster_counter_temp_int = 0 #TODO: fix the testrunner code to pass cluster name in params.
        self._buckets = {}
        self._default_bucket = self._input.param("default_bucket", True)
        if self._default_bucket:
            self.default_bucket_name = "default"
        self._num_replicas = self._input.param("replicas", 1)
        self._num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 128)
        self._dgm_run_bool = self._input.param("dgm_run", False)
        self._mem_quota_int = 0 # will be set in subsequent methods
        self.init_parameters_extended()
        self._doc_ops = self._input.param("doc-ops", None)
        if self._doc_ops is not None:
            self._doc_ops = self._doc_ops.split("-")
        self._doc_ops_dest = self._input.param("doc-ops-dest", None)
        # semi-colon separator is not accepted for some reason here
        if self._doc_ops_dest is not None:
            self._doc_ops_dest = self._doc_ops_dest.split("-")

        self._expires = self._input.param("expires", 0)
        self._log.info("Initializing input parameters completed...")
        self._timeout = self._input.param("timeout", 60)

    def _init_clusters(self):
        for key in self._clusters_keys_olst:
            self._set_cluster_names(key)
            self._setup_cluster(self._clusters_dic[key])

    # This method shall be overridden in case there are parameters that need to be initialized.
    def init_parameters_extended(self):
        pass

    # This method shall be overridden in case there are custom steps involved during setup.
    def setup_extended(self):
        pass

    # This method shall be overridden in case there are custom steps involved during teardown.
    def teardown_extended(self):
        pass

    def _do_cleanup(self):
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            BucketOperationHelper.delete_all_buckets_or_assert(nodes, self)
            ClusterOperationHelper.cleanup_cluster(nodes)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(nodes, self)

    def _cleanup_broken_setup(self):
        try:
            self.tearDown()
        except:
            self._log.info("Error while cleaning broken setup.")

    def _set_cluster_names(self, key):
        self._cluster_counter_temp_int += 1
        self._cluster_names_dic[key] = "cluster-{0}".format(self._cluster_counter_temp_int)

    def _setup_cluster(self, nodes):
        self._init_nodes(nodes)
        self._create_buckets(nodes)
        self._config_cluster(nodes)

    def _init_nodes(self, nodes):
        _tasks = []
        for node in nodes:
            _tasks.append(self._cluster_helper.async_init_node(node))
        for task in _tasks:
            mem_quota_node = task.result()
            if mem_quota_node < self._mem_quota_int or self._mem_quota_int == 0:
                self._mem_quota_int = mem_quota_node

    def _create_buckets(self, nodes):
        if self._dgm_run_bool:
            self._mem_quota_int = 256
        master_node = nodes[0]
        bucket_size = self._get_bucket_size(master_node, nodes, self._mem_quota_int, self._default_bucket)

        if self._default_bucket:
            self._cluster_helper.create_default_bucket(master_node, bucket_size, self._num_replicas)
            self._buckets[self.default_bucket_name] = {1: KVStore()}

    def _config_cluster(self, nodes):
        task = self._cluster_helper.async_rebalance(nodes, nodes[1:], [])
        task.result()

    def _get_bucket_size(self, master_node, nodes, mem_quota, num_buckets, ratio=2.0 / 3.0):
        for node in nodes:
            if node.ip == master_node.ip:
                return int(ratio / float(len(nodes)) / float(num_buckets) * float(mem_quota))
        return int(ratio / float(num_buckets) * float(mem_quota))


#===============================================================================
# class: XDCRReplicationBaseTest
# This class links the different clusters defined in ".ini" file and starts the
# replication. It supports different topologies for cluster which can be defined
# by input parameter "ctopology". The various values supported are as follows
# - chain
# - star (Not Implemented yet)
# - tree (Not Implemented yet)
# The direction of replication can be defined using parameter "rdirection".
# It supports following values
# - unidirection
# - bidirection
# Note:
# The first node is considered as master node in the respective cluster section 
# of .ini file.
# Cluster/Node ordinals are determined by the sequence of entry of node in .ini file.
#===============================================================================

class XDCRReplicationBaseTest(XDCRBaseTest):
    def setup_extended(self):
        self._setup_topology()


    def teardown_extended(self):
        for (rest_conn, cluster_ref, rep_database, rep_id) in self._cluster_state_arr:
            rest_conn.stop_replication(rep_database, rep_id)
            rest_conn.remove_remote_cluster(cluster_ref)

    def init_parameters_extended(self):
        self._cluster_state_arr = []
        self._cluster_topology_str = self._input.param(XDCRConstants.INPUT_PARAM_CLUSTER_TOPOLOGY,
            XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN)
        self._replication_direction_str = self._input.param(XDCRConstants.INPUT_PARAM_REPLICATION_DIRECTION,
            XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION)

    def _setup_topology(self):
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            self._setup_topology_chain()
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            self._set_toplogy_star()
        else:
            raise "Not supported cluster topology : %s", self._cluster_topology_str


    def _setup_topology_chain(self):
        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)
        dest_key_index = 1
        for src_key in ord_keys:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            dest_nodes = self._clusters_dic[dest_key]
            src_nodes = self._clusters_dic[src_key]
            src_master = src_nodes[0]
            dest_master = dest_nodes[0]
            src_cluster_name = self._cluster_names_dic[src_key]
            dest_cluster_name = self._cluster_names_dic[dest_key]
            self._join_clusters(src_cluster_name, src_master, dest_cluster_name, dest_master)
            dest_key_index += 1

    def _set_toplogy_star(self):
        src_master_identified = False
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            if not src_master_identified:
                src_master = nodes[0]
                src_cluster_name = self._cluster_names_dic[key]
                src_master_identified = True
                continue
            dest_cluster_name = self._cluster_names_dic[key]
            dest_master = nodes[0]
            self._join_clusters(src_cluster_name, src_master, dest_cluster_name, dest_master)

    def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        self._link_clusters(src_master, dest_cluster_name, dest_master)
        self._replicate_clusters(src_master, dest_cluster_name)
        if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
            self._link_clusters(dest_master, src_cluster_name, src_master)
            self._replicate_clusters(dest_master, src_cluster_name)

    def _link_clusters(self, src_master, dest_cluster_name, dest_master):
        rest_conn_src = RestConnection(src_master)
        rest_conn_src.add_remote_cluster(dest_master.ip, dest_master.port,
            dest_master.rest_username,
            dest_master.rest_password, dest_cluster_name)

    def _replicate_clusters(self, src_master, dest_cluster_name):
        rest_conn_src = RestConnection(src_master)
        (rep_database, rep_id) = rest_conn_src.start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
            "default",
            dest_cluster_name)
        self._cluster_state_arr.append((rest_conn_src, dest_cluster_name, rep_database, rep_id))


    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        tasks = []
        for bucket, kv_stores in self._buckets.items():
            gen = copy.deepcopy(kv_gen)
            tasks.append(self._cluster_helper.async_load_gen_docs(server, bucket, gen,
                kv_stores[kv_store],
                op_type, exp))
        return tasks

    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store)
        for task in tasks:
            task.result()

    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self._buckets:
                tasks.append(self._cluster_helper.async_wait_for_stats([server], bucket, '',
                    'ep_queue_size', '==', 0))
                tasks.append(self._cluster_helper.async_wait_for_stats([server], bucket, '',
                    'ep_flusher_todo', '==', 0))
                #TODO - add ack_log_size and idle stats check from TAP
        for task in tasks:
            task.result(self._timeout)

    def _verify_all_buckets(self, server, kv_store=1):
        for bucket, kv_stores in self._buckets.items():
            tasks = []
            num_try = 0 # wait exponentially upto 5 times on stats
            result = False
            while num_try < 5 and result is False:
                num_try += 1
                tasks.append(self._cluster_helper.async_verify_data(server, bucket, kv_stores[kv_store]))
                tasks.append(self._cluster_helper.async_verify_data(server, bucket, kv_stores[kv_store]))
                try:
                    for task in tasks:
                        result = task.result(self._timeout * num_try)
                except Exception as e:
                    self._log.info("exception {0}".format(e))
                    for task in tasks:
                        task.cancel()
                    raise Exception(
                        "Unable to get expected results for verify_all_buckets {0} sec and {1}th attempt".format(
                            self._timeout * num_try, num_try))


    def _verify_stats_all_buckets(self, servers):
        stats_tasks = []
        for bucket, kv_stores in self._buckets.items():
            items = sum([len(kv_store) for kv_store in kv_stores.values()])
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
            task.result(self._timeout)

    def _verify_revIds_deletes(self, src_server, dest_server, kv_store=1):
        tasks = []
        for bucket, kv_stores in self._buckets.items():
            tasks.append(self._cluster_helper.async_verify_revid(src_server, dest_server, bucket, kv_stores[kv_store]))
        for task in tasks:
            task.result()

    def _expiry_pager(self, master):
        for bucket in self._buckets:
            ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", 30, bucket)
            self._log.info("wait for expiry pager to run on all these nodes")
            time.sleep(30)