import sys
import time
import datetime
import unittest
import logger
import logging
import copy
import string
import random
from threading import Thread

from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.cluster import Cluster
from couchbase_helper.document import View
from TestInput import TestInputSingleton
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from couchbase_helper.stats_tools import StatsCommon
from scripts.collect_server_info import cbcollectRunner
from tasks.future import TimeoutError
from security.rbac_base import RbacBase

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.exception import ServerUnavailableException, XDCRException
from testconstants import STANDARD_BUCKET_PORT

#===============================================================================
# class: XDCRConstants
# Constants used in XDCR application testing. It includes input parameters as well
# as internal constants used in the code.
# All string literals used in the XDCR code shall be defined in this class.
#===============================================================================

class XDCRConstants:
    INPUT_PARAM_REPLICATION_DIRECTION = "rdirection"
    INPUT_PARAM_CLUSTER_TOPOLOGY = "ctopology"
    INPUT_PARAM_SEED_DATA = "sdata"
    INPUT_PARAM_SEED_DATA_MODE = "sdata_mode"

    INPUT_PARAM_SEED_DATA_OPERATION = "sdata_op"
    INPUT_PARAM_POLL_INTERVAL = "poll_interval"  # in seconds
    INPUT_PARAM_POLL_TIMEOUT = "poll_timeout"  # in seconds

    # CLUSTER_TOPOLOGY_TYPE_LINE = "line"
    CLUSTER_TOPOLOGY_TYPE_CHAIN = "chain"
    CLUSTER_TOPOLOGY_TYPE_STAR = "star"
    CLUSTER_TOPOLOGY_TYPE_RING = "ring"

    REPLICATION_TYPE_CONTINUOUS = "continuous"
    REPLICATION_DIRECTION_UNIDIRECTION = "unidirection"
    REPLICATION_DIRECTION_BIDIRECTION = "bidirection"

    SEED_DATA_MODE_SYNC = "sync"


#===============================================================================
# class: XDCRBaseTest
# This class is the base class (super class) for all the test classes written
# for XDCR. It configures the cluster as defined in the <cluster-file>.ini
# passed as an command line argument "-i" while running the test using testrunner.
# The child classes can override the following methods if required.
# - init_parameters_extended
# - setup_extendedself._input.param(XDCRConstants.INPUT_PARAM_REPLICATION_DIRECTION,
# - teardown_extended
# The reason for providing extended functions is so that inheriting classes can take advantage of
# clean_broken _setup in case an exception comes during test setUp.
# Note:B
# It assumes that the each node is physically up and running.
# In ini file , the cluster name should start with "cluster" (prefix).
#===============================================================================

class XDCRBaseTest(unittest.TestCase):
    def setUp(self):
        try:
            self.log = logger.Logger.get_logger()
            self._input = TestInputSingleton.input
            if self._input.param("log_level", None):
                self.log.setLevel(level=0)
                for hd in self.log.handlers:
                    if str(hd.__class__).find('FileHandler') != -1:
                        hd.setLevel(level=logging.DEBUG)
                    else:
                        hd.setLevel(level=getattr(logging, self._input.param("log_level", None)))
            self._init_parameters()
            if not hasattr(self, 'cluster'):
                self.cluster = Cluster()
            # REPLICATION RATE STATS
            self._local_replication_rate = {}
            self._xdc_replication_ops = {}
            self._xdc_replication_rate = {}
            self._start_replication_time = {}

            self.log.info("==============  XDCRbasetests setup is started for test #{0} {1}=============="\
                .format(self.case_number, self._testMethodName))
            if not self._input.param("skip_cleanup", False) and str(self.__class__).find('upgradeXDCR') == -1:
                self._cleanup_previous_setup()

            self._init_clusters(self._disabled_consistent_view)
            # XDCR global settings
            # Set this by default to 1 for all tests
            self.set_xdcr_param('xdcrFailureRestartInterval', 1)
            if self._checkpoint_interval:
                self.set_xdcr_param('xdcrCheckpointInterval', self._checkpoint_interval)
            self._optimistic_xdcr_threshold = self._input.param("optimistic_xdcr_threshold", 256)
            if self.src_master.ip != self.dest_master.ip:  # Only if it's not a cluster_run
                if self._optimistic_xdcr_threshold != 256:
                    self.set_xdcr_param('xdcrOptimisticReplicationThreshold', self._optimistic_xdcr_threshold)

            self.setup_extended()
            self.log.info("==============  XDCRbasetests setup is finished for test #{0} {1} =============="\
                .format(self.case_number, self._testMethodName))
            # # THREADS FOR STATS KEEPING
            self.__stats_threads = []
            if str(self.__class__).find('upgradeXDCR') == -1  and \
               str(self.__class__).find('tuq_xdcr') == -1 and self.print_stats:
                self.__stats_threads.append(Thread(target=self._replication_stat_keeper, args=["replication_data_replicated", self.src_master]))
                self.__stats_threads.append(Thread(target=self._replication_stat_keeper, args=["xdc_ops", self.dest_master]))
                self.__stats_threads.append(Thread(target=self._replication_stat_keeper, args=["data_replicated", self.src_master]))
                if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
                    self.__stats_threads.append(Thread(target=self._replication_stat_keeper, args=["replication_data_replicated", self.dest_master]))
                    self.__stats_threads.append(Thread(target=self._replication_stat_keeper, args=["xdc_ops", self.src_master]))
                    self.__stats_threads.append(Thread(target=self._replication_stat_keeper, args=["data_replicated", self.dest_master]))

                [st_thread.setDaemon(True) for st_thread in self.__stats_threads]
                [st_thread.start() for st_thread in self.__stats_threads]
                self._log_start(self)
        except Exception as e:
            self.log.error(str(e))
            self.log.error("Error while setting up clusters: %s", sys.exc_info())
            self._cleanup_broken_setup()
            raise

    def tearDown(self):
        try:
            test_failed = (hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures or self._resultForDoCleanups.errors)) \
                    or (hasattr(self, '_exc_info') and self._exc_info()[1] is not None)
            self._end_replication_flag = 1
            if str(self.__class__).find('tuq_xdcr') != -1:
                self._do_cleanup()
                return

            # Grab cbcollect before we cleanup
            if test_failed and TestInputSingleton.input.param("get-cbcollect-info", False):
                self.__get_cbcollect_info()

            cluster_run = len({server.ip for server in self._servers}) == 1
            if test_failed and not cluster_run and self.collect_data_files:
                self.__collect_data_files()

            if test_failed and (str(self.__class__).find('upgradeXDCR') != -1 or TestInputSingleton.input.param("stop-on-failure", False)):
                    self.log.warn("CLEANUP WAS SKIPPED")
                    return
            if self.print_stats:
                self.log.info("==============  XDCRbasetests stats for test #{0} {1} =============="\
                              .format(self.case_number, self._testMethodName))
                [st_thread.join() for st_thread in self.__stats_threads]
                self.log.info("Type of run: %s XDCR" % XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION.upper())
                self._print_stats(self.src_master)
                if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
                    self._print_stats(self.dest_master)

                self.log.info("============== = = = = = = = = END = = = = = = = = = = ==============")
            self.log.info("==============  XDCRbasetests cleanup is started for test #{0} {1} =============="\
                .format(self.case_number, self._testMethodName))
            self.teardown_extended()
            self._do_cleanup()
            self.log.info("==============  XDCRbasetests cleanup is finished for test #{0} {1} =============="\
                .format(self.case_number, self._testMethodName))
        finally:
            self.cluster.shutdown(force=True)
            self._log_finish(self)

    def __get_cbcollect_info(self):
        self.cluster.shutdown(force=True)
        self._log_finish(self)
        path = self._input.param("logs_folder", "/tmp")
        for server in self.src_nodes + self.dest_nodes:
            print("grabbing cbcollect from {0}".format(server.ip))
            path = path or "."
            try:
                cbcollectRunner(server, path).run()
                TestInputSingleton.input.test_params["get-cbcollect-info"] = False
            except:
                print("IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}".format(server.ip))

    def __collect_data_files(self):
        self.cluster.shutdown(force=True)
        self._log_finish(self)
        logs_folder = self._input.param("logs_folder", "/tmp")
        from scripts import collect_data_files
        nodes = self.src_nodes + self.dest_nodes
        for server in nodes:
            collect_data_files.cbdatacollectRunner(server, logs_folder).run()

    def _print_stats(self, node):
        if node == self.src_master:
            node1 = self.dest_master
        else:
            node1 = self.src_master
        self.log.info("STATS with source at {0} and destination at {1}".format(node.ip, node1.ip))
        for the_bucket in self._get_cluster_buckets(node):
            self.log.info("Bucket: {0}".format(the_bucket.name))
            if node in self._local_replication_rate:
                if the_bucket.name in self._local_replication_rate[node]:
                    if self._local_replication_rate[node][the_bucket.name] is not None:
                        _sum_ = 0
                        for _i in self._local_replication_rate[node][the_bucket.name]:
                            _sum_ += _i
                        self.log.info("\tAverage local replica creation rate for bucket '{0}': {1} KB per second"\
                            .format(the_bucket.name, (float)(_sum_) / (len(self._local_replication_rate[node][the_bucket.name]) * 1000)))
            if node1 in self._xdc_replication_ops:
                if the_bucket.name in self._xdc_replication_ops[node1]:
                    if self._xdc_replication_ops[node1][the_bucket.name] is not None:
                        _sum_ = 0
                        for _i in self._xdc_replication_ops[node1][the_bucket.name]:
                            _sum_ += _i
                        self._xdc_replication_ops[node1][the_bucket.name].sort()
                        _mid = len(self._xdc_replication_ops[node1][the_bucket.name]) // 2
                        if len(self._xdc_replication_ops[node1][the_bucket.name]) % 2 == 0:
                            _median_value_ = (float)(self._xdc_replication_ops[node1][the_bucket.name][_mid] +
                                                     self._xdc_replication_ops[node1][the_bucket.name][_mid - 1]) // 2
                        else:
                            _median_value_ = self._xdc_replication_ops[node1][the_bucket.name][_mid]
                        self.log.info("\tMedian XDC replication ops for bucket '{0}': {1} K ops per second"\
                            .format(the_bucket.name, (float)(_median_value_) / 1000))
                        self.log.info("\tMean XDC replication ops for bucket '{0}': {1} K ops per second"\
                            .format(the_bucket.name, (float)(_sum_) / (len(self._xdc_replication_ops[node1][the_bucket.name]) * 1000)))
            if node in self._xdc_replication_rate:
                if the_bucket.name in self._xdc_replication_rate[node]:
                    if self._xdc_replication_rate[node][the_bucket.name] is not None:
                        _sum_ = 0
                        for _i in self._xdc_replication_rate[node][the_bucket.name]:
                            _sum_ += _i
                        self.log.info("\tAverage XDCR data replication rate for bucket '{0}': {1} KB per second"\
                            .format(the_bucket.name, (float)(_sum_) / (len(self._xdc_replication_rate[node][the_bucket.name]) * 1000)))

    def _cleanup_previous_setup(self):
        self.teardown_extended()
        self._do_cleanup()

    def _init_parameters(self):
        self.log.info("Initializing input parameters started...")
        self._clusters_dic = self._input.clusters  # clusters is declared as dic in TestInput which is unordered.
        self._clusters_keys_olst = list(range(
            len(self._clusters_dic)))  # clusters are populated in the dic in testrunner such that ordinal is the key.
        # orderedDic cannot be used in order to maintain the compatibility with python 2.6
        self._cluster_counter_temp_int = 0
        self._cluster_names_dic = self._get_cluster_names()
        self._servers = self._input.servers
        self._disabled_consistent_view = self._input.param("disabled_consistent_view", None)
        self._floating_servers_set = self._get_floating_servers()  # These are the servers defined in .ini file but not linked to any cluster.
        self._cluster_counter_temp_int = 0  # TODO: fix the testrunner code to pass cluster name in params.
        self.buckets = []
        # max items number to verify in ValidateDataTask, None - verify all
        self.max_verify = self._input.param("max_verify", None)
        self._checkpoint_interval = self._input.param("checkpoint_interval", 1800)
        self._default_bucket = self._input.param("default_bucket", True)
        self._end_replication_flag = self._input.param("end_replication_flag", 0)

        """
        ENTER: sasl_buckets=[no.] or standard_buckets=[no.]
        """
        self._standard_buckets = self._input.param("standard_buckets", 0)
        self._sasl_buckets = self._input.param("sasl_buckets", 0)
        self._extra_buckets = self._input.param("extra_buckets", 0)  # number of buckets to add inside test body

        if self._default_bucket:
            self.default_bucket_name = "default"

        self._num_replicas = self._input.param("replicas", 1)
        self._mem_quota_int = 0  # will be set in subsequent methods
        self.eviction_policy = self._input.param("eviction_policy", 'valueOnly')  # or 'fullEviction'
        self.mixed_priority = self._input.param("mixed_priority", None)
        # if mixed priority is set by user, set high priority for sasl and standard buckets
        if self.mixed_priority:
            self.bucket_priority = 'high'
        else:
            self.bucket_priority = None
        self.num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 256)
        self._dgm_run = self._input.param("dgm_run", False)
        self.active_resident_threshold = int(self._input.param("active_resident_threshold", 0))

        self.rep_type = self._input.param("replication_type", "xmem")

        self._poll_interval = self._input.param(XDCRConstants.INPUT_PARAM_POLL_INTERVAL, 5)
        self._poll_timeout = self._input.param(XDCRConstants.INPUT_PARAM_POLL_TIMEOUT, 120)

        self.init_parameters_extended()

        self._doc_ops = self._input.param("doc-ops", "None")
        if self._doc_ops is not None:
            self._doc_ops = self._doc_ops.split("-")
        self._doc_ops_dest = self._input.param("doc-ops-dest", "None")
        # semi-colon separator is not accepted for some reason here
        if self._doc_ops_dest is not None:
            self._doc_ops_dest = self._doc_ops_dest.split("-")

        self.case_number = self._input.param("case_number", 0)
        self._expires = self._input.param("expires", 0)
        self.wait_timeout = self._input.param("timeout", 60)
        self._percent_update = self._input.param("upd", 30)
        self._percent_delete = self._input.param("del", 30)
        self._warmup = self._input.param("warm", None)
        self._failover = self._input.param("failover", None)
        self._graceful = self._input.param("graceful", False)
        self._demand_encryption = self._input.param("demand_encryption", 0)
        self._rebalance = self._input.param("rebalance", None)
        self._use_hostanames = self._input.param("use_hostnames", False)
        self.print_stats = self._input.param("print_stats", False)
        self._wait_for_expiration = self._input.param("wait_for_expiration", False)
        self.sdk_compression = self._input.param("sdk_compression", True)
        self.collect_data_files = False
        if self._warmup is not None:
            self._warmup = self._warmup.split("-")
        if self._failover is not None:
            self._failover = self._failover.split("-")
        if self._rebalance is not None:
            self._rebalance = self._rebalance.split("-")
            self._num_rebalance = self._input.param("num_rebalance", 1)


        """
        CREATE's a set of items,
        UPDATE's UPD% of the items starting from 0,
        DELETE's DEL% of the items starting from the end (count(items)).
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self.num_items)
        self.gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size,
            start=int((self.num_items) * (float)(100 - self._percent_delete) / 100), end=self.num_items)
        self.gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=0,
            end=int(self.num_items * (float)(self._percent_update) / 100))
        self.gen_append = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self.num_items)

        self.ord_keys = self._clusters_keys_olst
        self.ord_keys_len = len(self.ord_keys)


        self.src_nodes = copy.copy(self._clusters_dic[0])
        self.dest_nodes = copy.copy(self._clusters_dic[1])


        if int(self.src_nodes[0].port) in range(9091, 9991):
            temp = self.dest_nodes
            self.dest_nodes = self.src_nodes
            self.src_nodes = temp

        self.src_master = self.src_nodes[0]
        self.dest_master = self.dest_nodes[0]

        self._defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self._default_view_name = "default_view"
        self._default_view = View(self._default_view_name, self._defaul_map_func, None)
        self._num_views = self._input.param("num_views", 5)
        self._is_dev_ddoc = self._input.param("is_dev_ddoc", True)

        self.fragmentation_value = self._input.param("fragmentation_value", 80)
        self.disable_src_comp = self._input.param("disable_src_comp", True)
        self.disable_dest_comp = self._input.param("disable_dest_comp", True)

        self.log.info("Initializing input parameters completed.")

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.src_master[0]).log_client_error(msg)
            RestConnection(self.dest_master[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.src_master[0]).log_client_error(msg)
            RestConnection(self.dest_master[0]).log_client_error(msg)
        except:
            pass

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def _replication_stat_keeper(self, arg, node):
        while not self._end_replication_flag == 1:
            if node in self._clusters_dic[0]:
                consider_node = self.src_master
            elif node in self._clusters_dic[1]:
                consider_node = self.dest_master
            if arg == "replication_data_replicated":
                rest = RestConnection(consider_node)
                for the_bucket in self._get_cluster_buckets(consider_node):
                    stats = rest.fetch_bucket_stats(bucket=the_bucket.name)["op"]["samples"]
                    _x_ = stats[arg][-1]
                    if _x_ > 0:
                        _y_ = datetime.datetime.now()
                        if node not in self._local_replication_rate:
                            self._local_replication_rate[node] = {}
                        if the_bucket.name not in self._local_replication_rate[node]:
                            self._local_replication_rate[node][the_bucket.name] = []
                        self._local_replication_rate[node][the_bucket.name].append((float)(_x_) / (_y_ - self._start_replication_time[the_bucket.name]).seconds)
            elif arg == "xdc_ops":
                rest = RestConnection(consider_node)
                for the_bucket in self._get_cluster_buckets(consider_node):
                    stats = rest.fetch_bucket_stats(bucket=the_bucket.name)["op"]["samples"]
                    _x_ = stats[arg][-1]
                    if _x_ > 0:
                        if node not in self._xdc_replication_ops:
                            self._xdc_replication_ops[node] = {}
                        if the_bucket.name not in self._xdc_replication_ops[node]:
                            self._xdc_replication_ops[node][the_bucket.name] = []
                        self._xdc_replication_ops[node][the_bucket.name].append((float)(_x_))
            elif arg == "data_replicated":
                rest1 = RestConnection(consider_node)
                if consider_node == self.src_master:
                    rest2 = RestConnection(self.dest_master)
                else:
                    rest2 = RestConnection(self.src_master)
                dest_uuid = rest2.get_pools_info()["uuid"]
                for the_bucket in self._get_cluster_buckets(consider_node):
                    argument = "{0}/{1}/{2}/{3}/{4}".format("replications", dest_uuid, the_bucket.name, the_bucket.name, arg)
                    stats = rest1.fetch_bucket_stats(bucket=the_bucket.name)["op"]["samples"]
                    _x_ = stats[argument][-1]
                    if _x_ > 0:
                        _y_ = datetime.datetime.now()
                        if node not in self._xdc_replication_rate:
                            self._xdc_replication_rate[node] = {}
                        if the_bucket.name not in self._xdc_replication_rate[node]:
                            self._xdc_replication_rate[node][the_bucket.name] = []
                        self._xdc_replication_rate[node][the_bucket.name].append((float)(_x_) / (_y_ - self._start_replication_time[the_bucket.name]).seconds)

    def _get_floating_servers(self):
        cluster_nodes = []
        floating_servers = copy.copy(self._servers)

        for key, node in list(self._clusters_dic.items()):
            cluster_nodes.extend(node)

        for c_node in cluster_nodes:
            for node in floating_servers:
                if node.ip in str(c_node) and node.port in str(c_node):
                    floating_servers.remove(node)
        return floating_servers

    def _init_clusters(self, disabled_consistent_view=None):
        for key in self._clusters_keys_olst:
            for node in self._clusters_dic[key]:
                rest = RestConnection(node)
                rest.init_node()
            self._setup_cluster(self._clusters_dic[key], disabled_consistent_view)

    # This method shall be overridden in case there are parameters that need to be initialized.
    def init_parameters_extended(self):
        pass

    # This method shall be overridden in case there are custom steps involved during setup.
    def setup_extended(self):
        pass

    # This method shall be overridden in case there are custom steps involved during teardown.
    def teardown_extended(self):
        pass

    def __stop_rebalance(self, nodes):
        rest = RestConnection(nodes[0])
        if rest._rebalance_progress_status() == 'running':
            self.log.warning("rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")

    def _do_cleanup(self):
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            self.log.info("cleanup cluster{0}: {1}".format(key + 1, nodes))
            self.__stop_rebalance(nodes)
            for node in nodes:
                BucketOperationHelper.delete_all_buckets_or_assert([node], self)
                if self._input.param("forceEject", False) and node not in [self.src_nodes[0], self.dest_nodes[0] ]:
                            try:
                                rest = RestConnection(node)
                                rest.force_eject_node()
                            except BaseException as e:
                                self.log.error(e)
                else:
                    ClusterOperationHelper.cleanup_cluster([node], self)
                ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self)

    def _cleanup_broken_setup(self):
        try:
            self.tearDown()
        except:
            self.log.info("Error while cleaning broken setup.")

    def _get_cluster_names(self):
        cs_names = {}
        for key in self._clusters_keys_olst:
            cs_names[key] = "cluster{0}".format(self._cluster_counter_temp_int)
            self._cluster_counter_temp_int += 1
        return cs_names

    def _setup_cluster(self, nodes, disabled_consistent_view=None):
        self._init_nodes(nodes, disabled_consistent_view)
        self.cluster.async_rebalance(nodes, nodes[1:], [], use_hostnames=self._use_hostanames).result()
        if str(self.__class__).find('upgradeXDCR') != -1:
            self._create_buckets(self, nodes)
        else:
            self._create_buckets(nodes)

    def _init_nodes(self, nodes, disabled_consistent_view=None):
        _tasks = []
        for node in nodes:
            self._add_built_in_server_user(node=node)
            _tasks.append(self.cluster.async_init_node(node, disabled_consistent_view))
        for task in _tasks:
            mem_quota_node = task.result()
            if mem_quota_node < self._mem_quota_int or self._mem_quota_int == 0:
                self._mem_quota_int = mem_quota_node
        if self._use_hostanames:
            if not hasattr(self, 'hostnames'):
                self.hostnames = {}
            self.hostnames.update(self._rename_nodes(nodes))

    def _rename_nodes(self, servers):
        hostnames = {}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                hostname = shell.get_full_hostname()
                rest = RestConnection(server)
                renamed, content = rest.rename_node(hostname, username=server.rest_username, password=server.rest_password)
                self.assertTrue(renamed, "Server %s is not renamed!Hostname %s. Error %s" % (
                                        server, hostname, content))
                hostnames[server] = hostname
            finally:
                shell.disconnect()
        for i in range(len(servers)):
            if servers[i] in hostnames:
                if server and server.ip != servers[i].ip:
                    continue
                servers[i].hostname = hostnames[servers[i]]
        return hostnames

    def _add_built_in_server_user(self, testuser=None, rolelist=None, node=None):
        """
           From spock, couchbase server is built with some users that handles
           some specific task such as:
               cbadminbucket
           Default added user is cbadminbucket with admin role
        """
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                      'roles': 'admin'}]
        if node is None:
            node = self.master
        self.log.info("**** add built-in '%s' user to node %s ****" % (testuser[0]["name"],
                                                                                  node.ip))
        RbacBase().create_user_source(testuser, 'builtin', node)
        self.log.info("**** add '%s' role to '%s' user ****" % (rolelist[0]["roles"],
                                                                testuser[0]["name"]))
        RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')
        
    def _create_bucket_params(self, server, replicas=1, size=0, port=11211, password=None,
                             bucket_type='membase', enable_replica_index=1, eviction_policy='valueOnly',
                             bucket_priority=None, flush_enabled=1, lww=False):
        """Create a set of bucket_parameters to be sent to all of the bucket_creation methods
        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            bucket_name - The name of the bucket to be created. (String)
            port - The port to create this bucket on. (String)
            password - The password for this bucket. (String)
            size - The size of the bucket to be created. (int)
            enable_replica_index - can be 0 or 1, 1 enables indexing of replica bucket data (int)
            replicas - The number of replicas for this bucket. (int)
            eviction_policy - The eviction policy for the bucket, can be valueOnly or fullEviction. (String)
            bucket_priority - The priority of the bucket:either none, low, or high. (String)
            bucket_type - The type of bucket. (String)
            flushEnabled - Enable or Disable the flush functionality of the bucket. (int)
            lww = determine the conflict resolution type of the bucket. (Boolean)

        Returns:
            bucket_params - A dictionary containing the parameters needed to create a bucket."""

        bucket_params = {}
        bucket_params['server'] = server
        bucket_params['replicas'] = replicas
        bucket_params['size'] = size
        bucket_params['port'] = port
        bucket_params['password'] = password
        bucket_params['bucket_type'] = bucket_type
        bucket_params['enable_replica_index'] = enable_replica_index
        bucket_params['eviction_policy'] = eviction_policy
        bucket_params['bucket_priority'] = bucket_priority
        bucket_params['flush_enabled'] = flush_enabled
        bucket_params['lww'] = lww
        return bucket_params

    def _create_sasl_buckets(self, server, num_buckets, server_id, bucket_size):
        bucket_tasks = []
        sasl_params = self._create_bucket_params(server=server, password='password', size=bucket_size,
                                                        replicas=self._num_replicas,
                                                        eviction_policy=self.eviction_policy,
                                                        bucket_priority=self.bucket_priority)
        for i in range(num_buckets):
            name = "sasl_bucket_" + str(i + 1)
            bucket_tasks.append(self.cluster.async_create_sasl_bucket(name=name, password='password', bucket_params=sasl_params))

            self.buckets.append(Bucket(name=name, authType="sasl", saslPassword="password",
                                   num_replicas=self._num_replicas, bucket_size=bucket_size,
                                   master_id=server_id, eviction_policy=self.eviction_policy))

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

    def _create_standard_buckets(self, server, num_buckets, server_id, bucket_size):
        bucket_tasks = []
        standard_params = self._create_bucket_params(server=server, size=bucket_size, replicas=self._num_replicas,
                                                    eviction_policy=self.eviction_policy,
                                                    bucket_priority=self.bucket_priority)
        for i in range(num_buckets):
            name = "standard_bucket_" + str(i + 1)
            bucket_tasks.append(self.cluster.async_create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT+i,
                                                                          bucket_params=standard_params))

            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None,
                                    num_replicas=self._num_replicas, bucket_size=bucket_size,
                                    port=STANDARD_BUCKET_PORT + i, master_id=server_id, eviction_policy=self.eviction_policy))

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

    def _create_buckets(self, nodes):
        if self._dgm_run:
            self._mem_quota_int = 256
        master_node = nodes[0]
        total_buckets = self._sasl_buckets + self._default_bucket + self._standard_buckets + self._extra_buckets
        #to enable tests to skip creating buckets in setup and create them later
        if not total_buckets:
            return
        bucket_size = self._get_bucket_size(self._mem_quota_int, total_buckets)
        rest = RestConnection(master_node)
        master_id = rest.get_nodes_self().id
        if len({server.ip for server in self._servers}) != 1:  # if not cluster run use ip addresses instead of lh
            master_id = master_id.replace("127.0.0.1", master_node.ip).replace("localhost", master_node.ip)

        self._create_sasl_buckets(master_node, self._sasl_buckets, master_id, bucket_size)
        self._create_standard_buckets(master_node, self._standard_buckets, master_id, bucket_size)
        if self._default_bucket:
            bucket_params = self._create_bucket_params(server=master_node, size=bucket_size,
                                                              replicas=self._num_replicas,
                                                              eviction_policy=self.eviction_policy)
            self.cluster.create_default_bucket(bucket_params)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self._num_replicas, bucket_size=bucket_size, master_id=master_id,
                                       eviction_policy=self.eviction_policy))

    def _get_bucket_size(self, mem_quota, num_buckets):
        #min size is 100MB now
        return max(100, int(float(mem_quota) / float(num_buckets)))

    def _get_cluster_buckets(self, master_server):
        rest = RestConnection(master_server)
        master_id = rest.get_nodes_self().id

        if master_id.find('es') != 0:

            # verify if node_ids were changed for cluster_run
            for bucket in self.buckets:
                if ("127.0.0.1" in bucket.master_id and "127.0.0.1" not in master_id) or \
                   ("localhost" in bucket.master_id and "localhost" not in master_id):
                    new_ip = master_id[master_id.index("@") + 1:]
                    bucket.master_id = bucket.master_id.replace("127.0.0.1", new_ip).\
                    replace("localhost", new_ip)

        if len({server.ip for server in self._servers}) != 1:  # if not cluster run use ip addresses instead of lh
            master_id = master_id.replace("127.0.0.1", master_server.ip).replace("localhost", master_server.ip)
        buckets = [bucket for bucket in self.buckets if bucket.master_id == master_id]
        if not buckets:
            self.log.warn("No bucket(s) found on the server %s" % master_server)
        return buckets

    def do_a_warm_up(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        self.sleep(5)
        shell.start_couchbase()
        shell.disconnect()

    def adding_back_a_node(self, master, server):
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        for node in nodes:
            if server.ip == node.ip and int(server.port) == int(node.port):
                rest.add_back_node(node.id)

    def _get_active_replica_count_from_cluster(self, master):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            keys_loaded = sum([len(kv_store) for kv_store in list(bucket.kvs.values())])
            self.log.info("Keys loaded into bucket {0}:{1}".format(bucket.name,
                                                                   keys_loaded))
            self.log.info("Stat: vb_active_curr_items = {0}".
                          format(MemcachedClientHelper.direct_client(master,
                                 bucket.name).stats()['vb_active_curr_items']))
            self.log.info("Stat: vb_replica_curr_items = {0}".
                          format(MemcachedClientHelper.direct_client(master,
                                 bucket.name).stats()['vb_replica_curr_items']))

    def _async_failover(self, nodes, failover_nodes):
        self.log.info("Printing pre-failover stats")
        self._get_active_replica_count_from_cluster(nodes[0])
        tasks = []
        tasks.append(self.cluster.async_failover(nodes, failover_nodes, graceful=self._graceful))
        return tasks

    def _async_rebalance(self, nodes, to_add_node, to_remove_node):
        tasks = []
        tasks.append(self.cluster.async_rebalance(nodes, to_add_node, to_remove_node))
        return tasks

    # Change the master_id of buckets to new master node
    def __change_masterid_buckets(self, new_master, buckets):
        new_master_id = RestConnection(new_master).get_nodes_self().id
        for bucket in buckets:
            bucket.master_id = new_master_id

    # Initiates a rebalance-out asynchronously on both clusters
    def _async_rebalance_out(self, master=False):
        tasks = []
        if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
            src_buckets = self._get_cluster_buckets(self.src_master)
            tasks += self.__async_rebalance_out_cluster(self.src_nodes, self.src_master, master=master)
            if master:
                self.src_master = self.src_nodes[0]
                self.__change_masterid_buckets(self.src_master, src_buckets)

        if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
            dest_buckets = self._get_cluster_buckets(self.dest_master)
            tasks += self.__async_rebalance_out_cluster(self.dest_nodes, self.dest_master, master=master, cluster_type="source")
            if master:
                self.dest_master = self.dest_nodes[0]
                self.__change_masterid_buckets(self.dest_master, dest_buckets)
        return tasks

    def __async_rebalance_out_cluster(self, cluster_nodes, master_node, master=False, cluster_type="source"):
        remove_nodes = []
        if master:
            remove_nodes = [master_node]
        else:
            remove_nodes = cluster_nodes[len(cluster_nodes) - self._num_rebalance:]
        if self._failover and cluster_type in self._failover:
            tasks = self._async_failover(cluster_nodes, remove_nodes)
            for task in tasks:
                task.result()
        tasks = self._async_rebalance(cluster_nodes, [], remove_nodes)
        remove_node_ips = [remove_node.ip for remove_node in remove_nodes]
        self.log.info(" Starting rebalance-out nodes:{0} at {1} cluster {2}".
                          format(remove_node_ips, cluster_type, master_node.ip))
        list(map(cluster_nodes.remove, remove_nodes))
        return tasks

    # Initiates a rebalance-in asynchronously on both clusters
    def _async_rebalance_in(self):
        tasks = []
        if "source" in self._rebalance:
            tasks += self.__async_rebalance_in_cluster(self.src_nodes, self.src_master)

        if "destination" in self._rebalance:
            tasks += self.__async_rebalance_in_cluster(self.dest_nodes, self.dest_master, cluster_type="destination")
        return tasks

    def __async_rebalance_in_cluster(self, cluster_nodes, master_node, cluster_type="source"):
        add_nodes = self._floating_servers_set[0:self._num_rebalance]
        list(map(self._floating_servers_set.remove, add_nodes))
        tasks = self._async_rebalance(cluster_nodes, add_nodes, [])
        add_nodes_ips = [node.ip for node in add_nodes]
        self.log.info(" Starting rebalance-in nodes:{0} at {1} cluster {2}".
                          format(add_nodes_ips, cluster_type, master_node.ip))
        list(map(cluster_nodes.append, add_nodes))
        return tasks

    # Initiates a swap-rebalance asynchronously on both clusters
    def _async_swap_rebalance(self, master=False):
        tasks = []
        if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
            src_buckets = self._get_cluster_buckets(self.src_master)
            tasks += self.__async_swap_rebalance_cluster(self.src_nodes, self.src_master, master=master)
            if master:
                self.src_master = self.src_nodes[0]
                self.__change_masterid_buckets(self.src_master, src_buckets)

        if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
            dest_buckets = self._get_cluster_buckets(self.dest_master)
            tasks += self.__async_swap_rebalance_cluster(self.dest_nodes, self.dest_master, master=master, cluster_type="destination")
            if master:
                self.dest_master = self.dest_nodes[0]
                self.__change_masterid_buckets(self.dest_master, dest_buckets)
        return tasks

    def __async_swap_rebalance_cluster(self, cluster_nodes, master_node, master=False, cluster_type="source"):
        add_node = self._floating_servers_set.pop()
        if master:
            remove_node = master_node
        else:
            remove_node = cluster_nodes[len(cluster_nodes) - 1]
        tasks = self._async_rebalance(cluster_nodes, [add_node], [remove_node])
        self.log.info(" Starting swap-rebalance [remove_node:{0}] -> [add_node:{1}] at {2} cluster {3}"
                              .format(remove_node.ip, add_node.ip, cluster_type, master_node.ip))
        cluster_nodes.remove(remove_node)
        cluster_nodes.append(add_node)
        return tasks

    def get_servers_in_cluster(self, member):
        nodes = [node for node in RestConnection(member).get_nodes()]
        servers = []
        cluster_run = len({server.ip for server in self._servers}) == 1
        for server in self._servers:
            for node in nodes:
                if ((server.hostname if server.hostname else server.ip) == str(node.ip) or cluster_run)\
                 and server.port == str(node.port):
                    servers.append(server)
        return servers

    def wait_warmup_completed(self, warmupnodes, bucket_names=["default"]):
        if isinstance(bucket_names, str):
            bucket_names = [bucket_names]
        for server in warmupnodes:
            for bucket in bucket_names:
                mc = MemcachedClientHelper.direct_client(server, bucket)
                start = time.time()
                while time.time() - start < 150:
                    if mc.stats()["ep_warmup_thread"] == "complete":
                        self.log.info("Warmed up: %s items " % (mc.stats()["curr_items_tot"]))
                        self.sleep(10)
                        break
                    elif mc.stats()["ep_warmup_thread"] == "running":
                        self.log.info(
                            "Still warming up .. curr_items_tot : %s" % (mc.stats()["curr_items_tot"]))
                        continue
                    else:
                        self.log.info("Value of ep_warmup_thread does not exist, exiting from this server")
                        break
                if mc.stats()["ep_warmup_thread"] == "running":
                    self.log.info("ERROR: ep_warmup_thread's status not complete")
                mc.close

    def wait_node_restarted(self, server, wait_time=120, wait_if_warmup=False, check_service=False):
        now = time.time()
        if check_service:
            self.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time // 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                                            [server], self, wait_time=wait_time - num * 10, wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                self.sleep(10)

    def wait_service_started(self, server, wait_time=120):
        shell = RemoteMachineShellConnection(server)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, error = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                self.log.info("Couchbase service is running")
                return
            self.sleep(10, "couchbase service is not running")
        self.fail("Couchbase service is not running after {0} seconds".format(wait_time))

    def _enable_firewall(self, server):
        is_bidirectional = self._replication_direction_str == "bidirection"
        RemoteUtilHelper.enable_firewall(server, bidirectional=is_bidirectional, xdcr=True)

    def _disable_firewall(self, server):
        shell = RemoteMachineShellConnection(server)
        o, r = shell.execute_command("iptables -F")
        shell.log_command_output(o, r)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j ACCEPT")
        shell.log_command_output(o, r)
        if self._replication_direction_str == "bidirection":
            o, r = shell.execute_command("/sbin/iptables -A OUTPUT -p tcp -o eth0 --dport 1000:65535 -j ACCEPT")
            shell.log_command_output(o, r)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
        shell.log_command_output(o, r)
        self.log.info("enabled firewall on {0}".format(server))
        o, r = shell.execute_command("/sbin/iptables --list")
        shell.log_command_output(o, r)
        shell.disconnect()

    """ Reboot node, wait till nodes are warmed up """
    def reboot_node(self, node):
        self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 5)
        # disable firewall on these nodes
        self._disable_firewall(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self, wait_if_warmup=True)

    def disable_compaction(self, server=None, bucket="default"):
        server = server or self.src_master
        new_config = {"viewFragmntThresholdPercentage" : None,
                      "dbFragmentThresholdPercentage" :  None,
                      "dbFragmentThreshold" : None,
                      "viewFragmntThreshold" : None}
        self.cluster.modify_fragmentation_config(server, new_config, bucket)

    def make_default_views(self, prefix, count, is_dev_ddoc=False,):
        ref_view = self._default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        return [View(ref_view.name + str(i), ref_view.map_func, None, is_dev_ddoc) for i in range(count)]

    def async_create_views(self, server, design_doc_name, views, bucket="default"):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.cluster.async_create_view(server, design_doc_name, view, bucket)
                tasks.append(t_)
        else:
            t_ = self.cluster.async_create_view(server, design_doc_name, None, bucket)
            tasks.append(t_)
        return tasks

    def _get_num_items_ratio(self, op_type):
        if op_type in ["update", "delete"]:
            return self.num_items // 3
        else:
            return self.num_items

    def _load_gen_data(self, cname, node):
        for op_type in self._seed_data_ops_lst:
            num_items_ratio = self._get_num_items_ratio(op_type)
            load_gen = BlobGenerator(cname, cname, self._value_size, end=num_items_ratio)
            self.log.info("Starting Load operation '{0}' for items (ratio) '{1}' on node '{2}'....".format(op_type,
                num_items_ratio, cname))
            if self._seed_data_mode_str == XDCRConstants.SEED_DATA_MODE_SYNC:
                self._load_all_buckets(node, load_gen, op_type, 0)
                self.log.info("Completed Load of {0}".format(op_type))
            else:
                self._async_load_all_buckets(node, load_gen, op_type, 0)
                self.log.info("Started async Load of {0}".format(op_type))

    def _async_load_bucket(self, bucket, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30):
        gen = copy.deepcopy(kv_gen)
        task = self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                bucket.kvs[kv_store], op_type,
                                                exp, flag, only_store_hash,
                                                batch_size, pause_secs, timeout_secs,
                                                compression=self.sdk_compression)
        return task

    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=30):
        tasks = []
        buckets = self._get_cluster_buckets(server)
        for bucket in buckets:
            gen = copy.deepcopy(kv_gen)
            tasks.append(self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp, flag, only_store_hash,
                                                          batch_size, pause_secs, timeout_secs,
                                                          compression=self.sdk_compression))
        return tasks

    def _load_bucket(self, bucket, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30):
        task = self._async_load_bucket(bucket, server, kv_gen, op_type, exp, kv_store, flag, only_store_hash, batch_size, pause_secs, timeout_secs)
        task.result()

    def key_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store, flag, only_store_hash, batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()

        if self.active_resident_threshold:
            stats_all_buckets = {}
            for bucket in self._get_cluster_buckets(server):
                stats_all_buckets[bucket.name] = StatsCommon()

            for bucket in self._get_cluster_buckets(server):
                threshold_reached = False
                while not threshold_reached:
                    active_resident = stats_all_buckets[bucket.name].get_stats([server], bucket, '', 'vb_active_perc_mem_resident')[server]
                    if int(active_resident) > self.active_resident_threshold:
                        self.log.info("resident ratio is %s greater than %s for %s in bucket %s. Continue loading to the cluster" %
                                      (active_resident, self.active_resident_threshold, server.ip, bucket.name))
                        random_key = self.key_generator()
                        generate_load = BlobGenerator(random_key, '%s-' % random_key, self._value_size, end=batch_size * 50)
                        self._load_bucket(bucket, server, generate_load, "create", exp=0, kv_store=1, flag=0, only_store_hash=True, batch_size=batch_size, pause_secs=5, timeout_secs=60)
                    else:
                        threshold_reached = True
                        self.log.info("DGM state achieved for %s in bucket %s!" % (server.ip, bucket.name))
                        break

    def _modify_src_data(self):
        """Setting up creates/updates/deletes at source nodes"""

        if self._doc_ops is not None:
            if "create" in self._doc_ops:
                self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
            if "update" in self._doc_ops:
                self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
            if "delete" in self._doc_ops:
                self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
            self._wait_flusher_empty(self.src_master, self.src_nodes)
            if self._wait_for_expiration and self._expires:
                self.sleep(self._expires, "Waiting for expiration of updated items")

    def _async_modify_data(self):
        tasks = []
        """Setting up creates/updates/deletes at source nodes"""
        if self._doc_ops is not None:
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "create" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()
        if self._wait_for_expiration and self._expires:
            self.sleep(self._expires, "Waiting for expiration of updated items")

    def _async_update_delete_data(self):
        self.log.info("The tasks:-")
        tasks = []
        # Setting up doc-ops at source nodes and doc-ops-dest at destination nodes
        if self._doc_ops is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
            self.sleep(self.wait_timeout // 6)
        if self._doc_ops_dest is not None:
            if "update" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires))
            if "delete" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))
            self.sleep(self.wait_timeout // 6)
        for task in tasks:
            task.result()
        if self._wait_for_expiration and self._expires:
            self.sleep(self._expires, "Waiting for expiration of updated items")


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
# The first node is consideredn as master node in the respective cluster section
# of .ini file.
# Cluster/Node ordinals are determined by the sequence of entry of node in .ini file.
#===============================================================================
class XDCRReplicationBaseTest(XDCRBaseTest):
    def setup_extended(self):
        self._setup_topology()
        self._load_data()

    def teardown_extended(self):
        for _, clusters in list(self._clusters_dic.items()):
            rest = RestConnection(clusters[0])
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()
            rest.remove_all_recoveries()
#            #TODO should be added 'stop replication' when API to stop will be implemented
#        for (rest_conn, cluster_ref, rep_database, rep_id) in self._cluster_state_arr:
#            rest_conn.stop_replication(rep_database, rep_id)
            # rest_conn.remove_remote_cluster(cluster_ref)

    def init_parameters_extended(self):
        self._cluster_state_arr = []
        self._cluster_topology_str = self._input.param(XDCRConstants.INPUT_PARAM_CLUSTER_TOPOLOGY,
            XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN)
        self._replication_direction_str = self._input.param(XDCRConstants.INPUT_PARAM_REPLICATION_DIRECTION,
            XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION)
        self._seed_data = self._input.param(XDCRConstants.INPUT_PARAM_SEED_DATA, False)

        self._seed_data_ops_lst = self._input.param(XDCRConstants.INPUT_PARAM_SEED_DATA_OPERATION,
            "create").split('|')
        self._seed_data_mode_str = self._input.param(XDCRConstants.INPUT_PARAM_SEED_DATA_MODE,
            XDCRConstants.SEED_DATA_MODE_SYNC)

    def _setup_topology(self):
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            if str(self.__class__).find('upgradeXDCR') != -1:
                self._setup_topology_chain(self)
            else:
                self._setup_topology_chain()
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            if str(self.__class__).find('upgradeXDCR') != -1:
                self._set_topology_star(self)
            else:
                self._set_topology_star()
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_RING:
            self._set_topology_ring()
        else:
            raise Exception("Not supported cluster topology : %s", self._cluster_topology_str)

    def _load_data(self):
        if not self._seed_data:
            return
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            self._load_data_chain()
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            self._load_data_star()
        else:
            raise Exception("Seed data not supported cluster topology : %s", self._cluster_topology_str)

    def _setup_topology_chain(self):
        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)
        dest_key_index = 1
        for src_key in ord_keys:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            src_cluster_name = self._cluster_names_dic[src_key]
            dest_cluster_name = self._cluster_names_dic[dest_key]
            self._join_clusters(src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            dest_key_index += 1

    def _set_topology_star(self):
        src_master_identified = False
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            if not src_master_identified:
                src_cluster_name = self._cluster_names_dic[key]
                self.src_master = nodes[0]
                src_master_identified = True
                continue
            dest_cluster_name = self._cluster_names_dic[key]
            self.dest_master = nodes[0]
            self._join_clusters(src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)

    def _set_topology_ring(self):
        src_master_identified = False
        src_cluster_name = ""
        dest_cluster_name = ""
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            if not src_master_identified:
                src_cluster_name = self._cluster_names_dic[key]
                self.src_master = nodes[0]
                src_master_identified = True
                continue
            dest_cluster_name = self._cluster_names_dic[key]
            self.dest_master = nodes[0]
            self._join_clusters(self._cluster_names_dic[key - 1], self._clusters_dic[key - 1][0],
                                dest_cluster_name, self.dest_master)
        self._join_clusters(dest_cluster_name, self.dest_master, src_cluster_name, self.src_master)
        self.sleep(30)

    def _load_data_chain(self):
        for key in self._clusters_keys_olst:
            cluster_node = self._clusters_dic[key][0]
            cluster_name = self._cluster_names_dic[key]
            self.log.info("Starting Load # items {0} node {1} , cluster {2}....".format(self.num_items, cluster_node,
                cluster_name))
            self._load_gen_data(cluster_name, cluster_node)
            if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_UNIDIRECTION:
                break
            self.log.info("Completed Load")

    def set_xdcr_param(self, param, value):
        self.log.info("Setting {0} to {1} ..".format(param, value))
        src_rest = RestConnection(self.src_master)
        replications = src_rest.get_replications()
        for repl in replications:
            src_bucket = repl.get_src_bucket()
            dst_bucket = repl.get_dest_bucket()
            src_rest.set_xdcr_param(src_bucket.name, dst_bucket.name, param, value)

        if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
            dst_rest = RestConnection(self.dest_master)
            replications = dst_rest.get_replications()
            for repl in replications:
                src_bucket = repl.get_src_bucket()
                dst_bucket = repl.get_dest_bucket()
                dst_rest.set_xdcr_param(src_bucket.name, dst_bucket.name, param, value)

    def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        self._link_clusters(src_master, dest_cluster_name, dest_master)
        if int(self.dest_master.port) in range(9091, 9991):
            self.rep_type = 'capi'
        self._replicate_clusters(src_master, dest_cluster_name)
        if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
            self._link_clusters(dest_master, src_cluster_name, src_master)
            self._replicate_clusters(dest_master, src_cluster_name)

    # Set up cluster reference
    def _link_clusters(self, src_master, remote_cluster_name, dest_master):
        rest_conn_src = RestConnection(src_master)
        certificate = ""
        if self._demand_encryption:
            rest_conn_dest = RestConnection(dest_master)
            certificate = rest_conn_dest.get_cluster_ceritificate()
        if int(dest_master.port) in range(9091, 9991):
            rest_conn_src.add_remote_cluster(dest_master.ip, dest_master.port,
                dest_master.es_username,
                dest_master.es_password, remote_cluster_name,
                demandEncryption=self._demand_encryption, certificate=certificate)
        else:
            rest_conn_src.add_remote_cluster(dest_master.ip, dest_master.port,
                dest_master.rest_username,
                dest_master.rest_password, remote_cluster_name,
                demandEncryption=self._demand_encryption, certificate=certificate)

    def _modify_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master, require_encryption=None):
        rest_conn_src = RestConnection(src_master)
        certificate = ""
        if require_encryption:
            rest_conn_dest = RestConnection(dest_master)
            certificate = rest_conn_dest.get_cluster_ceritificate()
        rest_conn_src.modify_remote_cluster(dest_master.ip, dest_master.port,
            dest_master.rest_username,
            dest_master.rest_password, dest_cluster_name,
            demandEncryption=require_encryption, certificate=certificate)

    # Switch to encrypted xdcr
    def _switch_to_encryption(self, cluster):
        if "source" in cluster:
            src_remote_clusters = RestConnection(self.src_master).get_remote_clusters()
            for remote_cluster in src_remote_clusters:
                self._modify_clusters(None, self.src_master, remote_cluster['name'], self.dest_master,
                                      require_encryption=1)
        if "destination" in cluster:
            dest_remote_clusters = RestConnection(self.dest_master).get_remote_clusters()
            for remote_cluster in dest_remote_clusters:
                self._modify_clusters(None, self.dest_master, remote_cluster['name'], self.src_master,
                                      require_encryption=1)

    # Set up replication
    def _replicate_clusters(self, src_master, dest_cluster_name):
        rest_conn_src = RestConnection(src_master)
        for bucket in self._get_cluster_buckets(src_master):
            rep_id = rest_conn_src.start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
                bucket.name, dest_cluster_name, self.rep_type)
            self._start_replication_time[bucket.name] = datetime.datetime.now()
            self._cluster_state_arr.append((rest_conn_src, dest_cluster_name, rep_id))
            self.sleep(5)


    """merge 2 different kv strores from different clsusters/buckets
       assume that all elements in the second kvs are more relevant.

    Returns:
            merged kvs, that we expect to get on both clusters
    """
    def __merge_keys(self, kv_store_first, kv_store_second, kvs_num=1):
        valid_keys_first, deleted_keys_first = kv_store_first[kvs_num].key_set()
        valid_keys_second, deleted_keys_second = kv_store_second[kvs_num].key_set()

        for key in valid_keys_second:
            # replace/add the values for each key in first kvs
            if key not in valid_keys_first:
                partition1 = kv_store_first[kvs_num].acquire_partition(key)
                partition2 = kv_store_second[kvs_num].acquire_partition(key)
                key_add = partition2.get_key(key)
                partition1.set(key, key_add["value"], key_add["expires"], key_add["flag"])
                kv_store_first[kvs_num].release_partition(key)
                kv_store_second[kvs_num].release_partition(key)

        for key in deleted_keys_second:
            # add deleted keys to first kvs if the where deleted only in second kvs
            if key not in deleted_keys_first:
                partition1 = kv_store_first[kvs_num].acquire_partition(key)
                partition1.delete(key)
                kv_store_first[kvs_num].release_partition(key)

    def __do_merge_buckets(self, src_master, dest_master, bidirection):
        src_buckets = self._get_cluster_buckets(src_master)
        dest_buckets = self._get_cluster_buckets(dest_master)
        for src_bucket in src_buckets:
            for dest_bucket in dest_buckets:
                if src_bucket.name == dest_bucket.name:
                    if bidirection:
                        self.__merge_keys(src_bucket.kvs, dest_bucket.kvs, kvs_num=1)
                    dest_bucket.kvs[1] = src_bucket.kvs[1]

    def merge_buckets(self, src_master, dest_master, bidirection=True):
        self.log.info("merge buckets {0}->{1}, bidirection:{2}".format(src_master.ip, dest_master.ip, bidirection))
        # Wait for expiration if not already done
        if self._expires and not self._wait_for_expiration:
            self.sleep(self._expires, "Waiting for expiration of updated items")
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            self.__do_merge_buckets(src_master, dest_master, bidirection)
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            for i in range(1, len(self._clusters_dic)):
                dest_cluster = self._clusters_dic[i]
                self.__do_merge_buckets(src_master, dest_cluster[0], bidirection)

    def do_merge_bucket(self, src_master, dest_master, bidirection, bucket):
        # Wait for expiration if not already done
        if self._expires and not self._wait_for_expiration:
            self.sleep(self._expires, "Waiting for expiration of updated items")
        src_buckets = self._get_cluster_buckets(src_master)
        dest_buckets = self._get_cluster_buckets(dest_master)
        for src_bucket in src_buckets:
            for dest_bucket in dest_buckets:
                if src_bucket.name == dest_bucket.name and bucket.name == src_bucket.name:
                    if bidirection:
                        self.__merge_keys(src_bucket.kvs, dest_bucket.kvs, kvs_num=1)
                    dest_bucket.kvs[1] = src_bucket.kvs[1]

    def _wait_for_replication_to_catchup(self, timeout=1200):
        self._expiry_pager(self.src_master)
        self._expiry_pager(self.dest_master)
        self.sleep(15)

        rest1 = RestConnection(self.src_master)
        rest2 = RestConnection(self.dest_master)
        # 20 minutes by default
        end_time = time.time() + timeout

        for bucket in self.buckets:
            _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
            _count2 = rest2.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
            while _count1 != _count2 and (time.time() - end_time) < 0:
                self.sleep(60, "Expected: {0} items, found: {1}. Waiting for replication to catch up ..".format(_count1, _count2))
                _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
                _count2 = rest2.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
            if _count1 != _count2:
                self.fail("not all items replicated in {0} sec for {1} bucket. on source cluster:{2}, on dest:{3}".\
                          format(timeout, bucket.name, _count1, _count2))
            self.log.info("Replication caught up for bucket {0}: {1}".format(bucket.name, _count1))

    """ Gets the required xdcr stat value for the given bucket from a server """
    def get_xdcr_stat(self, server, bucket_name, param):
        return int(RestConnection(server).fetch_bucket_stats(bucket_name)
                   ['op']['samples'][param][-1])

    def _verify_revIds(self, src_server, dest_server, kv_store=1, timeout=900):
        error_count = 0
        tasks = []
        for src_bucket in self._get_cluster_buckets(src_server):
            for dest_bucket in  self._get_cluster_buckets(dest_server):
                if src_bucket.name == dest_bucket.name:
                    self.log.info("Verifying RevIds for {0} -> {1}, bucket: {2}"
                          .format(src_server.ip, dest_server.ip, src_bucket))
                    task_info = self.cluster.async_verify_revid(
                                                        src_server,
                                                        dest_server,
                                                        src_bucket,
                                                        src_bucket.kvs[kv_store],
                                                        dest_bucket.kvs[kv_store],
                                                        compression=self.sdk_compression)
                    tasks.append(task_info)
        for task in tasks:
            task.result(timeout)
            error_count += task.err_count
            if task.err_count:
                for ip, values in task.keys_not_found.items():
                    if values:
                        self.log.error("%s keys not found on %s:%s" % (len(values), ip, values))
        return error_count

    def _expiry_pager(self, master, val=10):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", val, bucket)
            self.log.info("wait for expiry pager to run on all these nodes")

    def _wait_flusher_empty(self, master, servers, timeout=120):
        tasks = []
        buckets = self._get_cluster_buckets(master)
        self.assertTrue(buckets, "No buckets recieved from the server {0} for verification".format(master.ip))
        for server in servers:
            for bucket in buckets:
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '', 'ep_queue_size', '==', 0))
        for task in tasks:
            task.result(timeout)

    def _verify_data_all_buckets(self, server, kv_store=1, timeout=None, max_verify=None, only_store_hash=True, batch_size=1000):
        tasks = []
        buckets = self._get_cluster_buckets(server)
        self.assertTrue(buckets, "No buckets recieved from the server {0} for verification".format(server.ip))
        for bucket in buckets:
            tasks.append(self.cluster.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify,
                                                        only_store_hash, batch_size, timeout_sec=60,
                                                        compression=self.sdk_compression))
        for task in tasks:
            task.result(timeout)

    def _verify_item_count(self, master, servers, timeout=120):
        stats_tasks = []
        buckets = self._get_cluster_buckets(master)
        self.assertTrue(buckets, "No buckets received from the server {0} for verification".format(master.ip))
        for bucket in buckets:
            items = sum([len(kv_store) for kv_store in list(bucket.kvs.values())])
            for stat in ['curr_items', 'vb_active_curr_items']:
                stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                    stat, '==', items))
            if self._num_replicas >= 1 and len(servers) > 1:
                stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                    'vb_replica_curr_items', '==', items * self._num_replicas))
        try:
            for task in stats_tasks:
                task.result(timeout)
            return items
        except TimeoutError:
            self.log.error('ERROR: Timed-out waiting for item count to match.'
                          ' Will proceed to do metadata checks.')


    # CBQE-1695 Wait for replication_changes_left (outbound mutations) to be 0.
    def __wait_for_outbound_mutations_zero(self, master_node, timeout=180):
        self.log.info("Waiting for Outbound mutation to be zero on cluster node: %s" % master_node.ip)
        buckets = self._get_cluster_buckets(master_node)
        curr_time = time.time()
        end_time = curr_time + timeout
        rest = RestConnection(master_node)
        while curr_time < end_time:
            found = 0
            for bucket in buckets:
                mutations = int(rest.get_xdc_queue_size(bucket.name))
                self.log.info("Current outbound mutations on cluster node: %s for bucket %s is %s" % (master_node.ip, bucket.name, mutations))
                if mutations == 0:
                    found = found + 1
            if found == len(buckets):
                break
            self.sleep(10)
            end_time = end_time - 10
        else:
            # MB-9707: Updating this code from fail to warning to avoid test to abort, as per this
            # bug, this particular stat i.e. replication_changes_left is buggy.
            self.log.error("Timeout occurs while waiting for mutations to be replicated")
            return False
        return True

    """Verify the stats at the destination cluster
    1. Data Validity check - using kvstore-node key-value check
    2. Item count check on source versus destination
    3. For deleted and updated items, check the CAS/SeqNo/Expiry/Flags for same key on source/destination
    * Make sure to call expiry_pager function to flush out temp items(deleted/expired items)"""
    def verify_xdcr_stats(self, src_nodes, dest_nodes, timeout=200):
        self.collect_data_files = True
        self._expiry_pager(self.src_nodes[0], val=10)
        self._expiry_pager(self.dest_nodes[0], val=10)
        self.sleep(10)

        if self._failover is not None or self._rebalance is not None:
            timeout *= 3

        # Wait for ep_queue_size on source to become 0
        self.log.info("Verify xdcr replication stats at Source Cluster : {0}".format(self.src_master.ip))
        self._wait_flusher_empty(self.src_master, src_nodes)

        # Wait for ep_queue_size on dest to become 0
        self.log.info("Verify xdcr replication stats at Destination Cluster : {0}".format(self.dest_master.ip))
        self._wait_flusher_empty(self.dest_master, dest_nodes)

        mutations_replicated = True
        data_verified = False
        try:
            # Source validations
            mutations_replicated &= self.__wait_for_outbound_mutations_zero(self.dest_master)
            self._verify_item_count(self.src_master, src_nodes, timeout=timeout)
            self._verify_data_all_buckets(self.src_master, max_verify=self.max_verify)
            # Dest validations
            mutations_replicated &= self.__wait_for_outbound_mutations_zero(self.src_master)
            self._verify_item_count(self.dest_master, dest_nodes, timeout=timeout)
            self._verify_data_all_buckets(self.dest_master, max_verify=self.max_verify)
            data_verified = True
        finally:
            errors_caught = 0
            bidirection = self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION
            if self._doc_ops is not None and ("update" in self._doc_ops or "delete" in self._doc_ops):
                errors_caught += self._verify_revIds(self.src_master, self.dest_master)
            if bidirection and self._doc_ops_dest is not None and ("update" in self._doc_ops_dest or "delete" in self._doc_ops_dest):
                errors_caught += self._verify_revIds(self.dest_master, self.src_master)
            if errors_caught > 0:
                self.fail("Mismatches on Meta Information on xdcr-replicated items!")
            if data_verified:
                self.collect_data_files = False

        if not mutations_replicated:
            if str(self.__class__).find('cbrecovery'):
                self.log.error("Test should be failed because Outbound mutations has not become zero(MB-9707), check the test logs above. \
                    but we skip the failure for current test")
            else:
                self.fail("Test is failed as Outbound mutations has not become zero, check the test logs above.")

    def verify_results(self):
        dest_key_index = 1
        if len(self.ord_keys) == 2:
            src_nodes = self.get_servers_in_cluster(self.src_master)
            dest_nodes = self.get_servers_in_cluster(self.dest_master)
            self.verify_xdcr_stats(src_nodes, dest_nodes)
        else:
            # Checking replication at destination clusters when more then 2 clusters defined
            for cluster_num in self.ord_keys[1:]:
                if dest_key_index == self.ord_keys_len:
                    break
                self.dest_nodes = self._clusters_dic[cluster_num]
                self.verify_xdcr_stats(self.src_nodes, self.dest_nodes)
