import logger
import unittest
import copy
import datetime
import time
import string
import random
import logging
import json
import subprocess
import mc_bin_client
import traceback
import re


from memcached.helper.data_helper import VBucketAwareMemcached
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.cluster import Cluster
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.stats_tools import StatsCommon
from TestInput import TestInputSingleton, TestInputServer
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.exception import ServerUnavailableException
from couchbase_helper.data_analysis_helper import *
from testconstants import STANDARD_BUCKET_PORT
from testconstants import MIN_COMPACTION_THRESHOLD
from testconstants import MAX_COMPACTION_THRESHOLD
from testconstants import LINUX_DIST_CONFIG
from membase.helper.cluster_helper import ClusterOperationHelper
from security.rbac_base import RbacBase
from collection.collections_rest_client import Collections_Rest

from couchbase_cli import CouchbaseCLI
import testconstants

from scripts.collect_server_info import cbcollectRunner


class BaseTestCase(unittest.TestCase):
    def suite_setUp(self):
        self.log.info("==============  BaseTestCase suite_setup has started ==============")
        self.log.info("==============  BaseTestCase suite_setup has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  BaseTestCase suite_teardown has started ==============")
        self.log.info("==============  BaseTestCase suite_teardown has completed ==============")


    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.primary_index_created = False
        self.use_sdk_client = self.input.param("use_sdk_client", False)
        self.analytics = self.input.param("analytics", False)
        if self.input.param("log_level", None):
            self.log.setLevel(level=0)
            for hd in self.log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    hd.setLevel(level=getattr(logging, self.input.param("log_level", None)))
        self.servers = self.input.servers
        if str(self.__class__).find('moxitests') != -1:
            self.moxi_server = self.input.moxis[0]
            self.servers = [server for server in self.servers
                            if server.ip != self.moxi_server.ip]
        self.buckets = []
        self.bucket_base_params = {}
        self.bucket_base_params['membase'] = {}
        self.master = self.servers[0]
        self.upgrade_test = self.input.param("upgrade_test", False)
        self.indexManager = self.servers[0]
        if not hasattr(self, 'cluster'):
            self.cluster = Cluster()
        self.pre_warmup_stats = {}
        self.cleanup = False
        self.nonroot = False
        shell = RemoteMachineShellConnection(self.master)
        self.os_info = shell.extract_remote_info().type.lower()
        if self.os_info != 'windows':
            if self.master.ssh_username != "root":
                self.nonroot = True
        shell.disconnect()
        """ some tests need to bypass checking cb server at set up
            to run installation """
        self.skip_init_check_cbserver = \
            self.input.param("skip_init_check_cbserver", False)
        self.data_collector = DataCollector()
        self.data_analyzer = DataAnalyzer()
        self.result_analyzer = DataAnalysisResultAnalyzer()
        self.set_testrunner_client()
        self.change_bucket_properties=False
        self.cbas_node = self.input.cbas
        self.cbas_servers = []
        self.kv_servers = []
        self.otpNodes = []
        self.collection_name={}
        for server in self.servers:
            if "cbas" in server.services:
                self.cbas_servers.append(server)
            if "kv" in server.services:
                self.kv_servers.append(server)
        if not self.cbas_node and len(self.cbas_servers)>=1:
            self.cbas_node = self.cbas_servers[0]

        try:
            self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
            self.vbuckets = self.input.param("vbuckets", 1024)
            self.collection=self.input.param("collection", False)
            self.scope_num=self.input.param("scope_num", 4)
            self.collection_num=self.input.param("collection_num", [2] * self.scope_num )
            self.upr = self.input.param("upr", None)
            self.index_quota_percent = self.input.param("index_quota_percent", None)
            self.targetIndexManager = self.input.param("targetIndexManager", False)
            self.targetMaster = self.input.param("targetMaster", False)
            self.reset_services = self.input.param("reset_services", False)
            self.auth_mech = self.input.param("auth_mech", "PLAIN")
            self.wait_timeout = self.input.param("wait_timeout", 60)
            # number of case that is performed from testrunner( increment each time)
            self.case_number = self.input.param("case_number", 0)
            self.default_bucket = self.input.param("default_bucket", True)
            self.parallelism = self.input.param("parallelism", False)
            if self.default_bucket:
                self.default_bucket_name = self.input.param("default_bucket_name", "default")
            self.skip_standard_buckets = self.input.param("skip_standard_buckets", False)
            self.standard_buckets = self.input.param("standard_buckets", 0)
            # list of list [[2,1,1],[3,1,2,1],[4,1,1,2,3]]
            self.standard_buckets_scope=self.input.param("bucket_scope", [2]*self.standard_buckets)
            if self.skip_standard_buckets:
                self.standard_buckets = 0
                BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
            self.sasl_buckets = self.input.param("sasl_buckets", 0)
            self.num_buckets = self.input.param("num_buckets", 0)

            self.verify_unacked_bytes = self.input.param("verify_unacked_bytes", False)
            self.memcached_buckets = self.input.param("memcached_buckets", 0)
            self.enable_flow_control = self.input.param("enable_flow_control", False)
            self.total_buckets = self.sasl_buckets + self.default_bucket + self.standard_buckets + self.memcached_buckets
            self.num_servers = self.input.param("servers", len(self.servers))
            # initial number of items in the cluster
            self.nodes_init = self.input.param("nodes_init", 1)
            self.nodes_in = self.input.param("nodes_in", 1)
            self.nodes_out = self.input.param("nodes_out", 1)
            self.services_init = self.input.param("services_init", None)
            self.services_in = self.input.param("services_in", None)
            self.forceEject = self.input.param("forceEject", False)
            self.force_kill_memcached = TestInputSingleton.input.param('force_kill_memcached', False)
            self.num_items = self.input.param("items", 1000)
            self.value_size = self.input.param("value_size", 512)
            self.dgm_run = self.input.param("dgm_run", False)
            self.active_resident_threshold = float(self.input.param("active_resident_threshold", 100))
            # max items number to verify in ValidateDataTask, None - verify all
            self.max_verify = self.input.param("max_verify", None)
            # we don't change consistent_view on server by default
            self.disabled_consistent_view = self.input.param("disabled_consistent_view", None)
            self.rebalanceIndexWaitingDisabled = self.input.param("rebalanceIndexWaitingDisabled", None)
            self.rebalanceIndexPausingDisabled = self.input.param("rebalanceIndexPausingDisabled", None)
            self.maxParallelIndexers = self.input.param("maxParallelIndexers", None)
            self.maxParallelReplicaIndexers = self.input.param("maxParallelReplicaIndexers", None)
            self.quota_percent = self.input.param("quota_percent", None)
            self.port = None
            self.log_message = self.input.param("log_message", None)
            self.log_info = self.input.param("log_info", None)
            self.log_location = self.input.param("log_location", None)
            self.stat_info = self.input.param("stat_info", None)
            self.port_info = self.input.param("port_info", None)
            if not hasattr(self, 'skip_buckets_handle'):
                self.skip_buckets_handle = self.input.param("skip_buckets_handle", False)
            self.nodes_out_dist = self.input.param("nodes_out_dist", None)
            self.absolute_path = self.input.param("absolute_path", True)
            self.test_timeout = self.input.param("test_timeout", 3600)  # kill hang test and jump to next one.
            self.enable_bloom_filter = self.input.param("enable_bloom_filter", False)
            self.enable_time_sync = self.input.param("enable_time_sync", False)
            self.gsi_type = self.input.param("gsi_type", 'plasma')
            # bucket parameters go here,
            self.bucket_size = self.input.param("bucket_size", None)
            self.bucket_type = self.input.param("bucket_type", 'membase')
            self.num_replicas = self.input.param("replicas", 1)
            self.enable_replica_index = self.input.param("index_replicas", 1)
            self.skip_bucket_setup = self.input.param("skip_bucket_setup", False)
            self.eviction_policy = self.input.param("eviction_policy", 'valueOnly')  # or 'fullEviction'
                           # for ephemeral bucket is can be noEviction or nruEviction
            if self.bucket_type == 'ephemeral' and self.eviction_policy == 'valueOnly':
                # use the ephemeral bucket default
                self.eviction_policy = 'noEviction'

                                          # for ephemeral buckets it
            self.sasl_password=self.input.param("sasl_password", 'password')
            self.lww = self.input.param("lww",
                                        False)  # only applies to LWW but is here because the bucket is created here
            self.maxttl = self.input.param("maxttl", None)
            self.compression_mode = self.input.param("compression_mode", 'passive')
            self.sdk_compression = self.input.param("sdk_compression", True)
            self.sasl_bucket_name = "bucket"
            self.sasl_bucket_priority = self.input.param("sasl_bucket_priority", None)
            self.standard_bucket_priority = self.input.param("standard_bucket_priority", None)
            # end of bucket parameters spot (this is ongoing)
            self.disable_diag_eval_on_non_local_host = self.input.param("disable_diag_eval_non_local", False)

            if self.collection:
                cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password)
                cli.enable_dp()

            if self.skip_setup_cleanup:
                self.buckets = RestConnection(self.master).get_buckets()
                return
            if not self.skip_init_check_cbserver:
                self.cb_version = None
                if RestHelper(RestConnection(self.master)).is_ns_server_running():
                    """ since every new couchbase version, there will be new features
                        that test code will not work on previous release.  So we need
                        to get couchbase version to filter out those tests. """
                    self.cb_version = RestConnection(self.master).get_nodes_version()
                else:
                    self.log.info("couchbase server does not run yet")
                self.protocol = self.get_protocol_type()
            self.services_map = None
            if self.sasl_bucket_priority is not None:
                self.sasl_bucket_priority = self.sasl_bucket_priority.split(":")
            if self.standard_bucket_priority is not None:
                self.standard_bucket_priority = self.standard_bucket_priority.split(":")

            self.log.info("==============  basetestcase setup was started for test #{0} {1}==============" \
                          .format(self.case_number, self._testMethodName))
            if not self.skip_buckets_handle and not self.skip_init_check_cbserver:
                self._cluster_cleanup()


            shared_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                       replicas=self.num_replicas,
                                                       enable_replica_index=self.enable_replica_index,
                                                       eviction_policy=self.eviction_policy, bucket_priority=None,
                                                       lww=self.lww, maxttl=self.maxttl,
                                                       compression_mode=self.compression_mode)

            membase_params = copy.deepcopy(shared_params)
            membase_params['bucket_type'] = 'membase'
            self.bucket_base_params['membase']['non_ephemeral'] = membase_params

            membase_ephemeral_params = copy.deepcopy(shared_params)
            membase_ephemeral_params['bucket_type'] = 'ephemeral'
            self.bucket_base_params['membase']['ephemeral'] = membase_ephemeral_params

            memcached_params = copy.deepcopy(shared_params)
            memcached_params['bucket_type'] = 'memcached'
            self.bucket_base_params['memcached'] = memcached_params

            # avoid any cluster operations in setup for new upgrade
            #  & upgradeXDCR tests
            if str(self.__class__).find('newupgradetests') != -1 or \
                            str(self.__class__).find('upgradeXDCR') != -1 or \
                            str(self.__class__).find('Upgrade_EpTests') != -1 or \
                            (str(self.__class__).find('UpgradeTests') != -1 and self.skip_init_check_cbserver) or \
                            hasattr(self, 'skip_buckets_handle') and \
                            self.skip_buckets_handle:
                self.log.info("any cluster operation in setup will be skipped")
                self.primary_index_created = True
                self.log.info("==============  basetestcase setup was finished for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
                return
            # avoid clean up if the previous test has been tear down
            if self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    self.log.warn("teardDown for previous test failed. will retry..")
                    self.case_number -= 1000
                self.cleanup = True
                if not self.skip_init_check_cbserver:
                    self.tearDown()
                self.cluster = Cluster()
            if not self.skip_init_check_cbserver:
                self.log.info("initializing cluster")
                self.reset_cluster()
                master_services = self.get_services(self.servers[:1],
                                                    self.services_init,
                                                    start_node=0)
                if master_services is not None:
                    master_services = master_services[0].split(",")

                self.quota = self._initialize_nodes(
                    self.cluster,
                    self.servers,
                    self.disabled_consistent_view,
                    self.rebalanceIndexWaitingDisabled,
                    self.rebalanceIndexPausingDisabled,
                    self.maxParallelIndexers,
                    self.maxParallelReplicaIndexers,
                    self.port,
                    self.quota_percent,
                    services=master_services)

                self.change_env_variables()
                self.change_checkpoint_params()

                # Add built-in user
                if not self.skip_init_check_cbserver:
                    self.add_built_in_server_user(node=self.master)
                self.log.info("done initializing cluster")
            else:
                self.quota = ""
            if self.input.param("log_info", None):
                self.change_log_info()
            if self.input.param("log_location", None):
                self.change_log_location()
            if self.input.param("stat_info", None):
                self.change_stat_info()
            if self.input.param("port_info", None):
                self.change_port_info()
            if self.input.param("port", None):
                self.port = str(self.input.param("port", None))
            try:
                if (str(self.__class__).find('rebalanceout.RebalanceOutTests') != -1) or \
                        (str(self.__class__).find('memorysanitytests.MemorySanity') != -1) or \
                                str(self.__class__).find('negativetests.NegativeTests') != -1 or \
                                str(self.__class__).find('warmuptest.WarmUpTests') != -1 or \
                                str(self.__class__).find('failover.failovertests.FailoverTests') != -1 or \
                                str(self.__class__).find('observe.observeseqnotests.ObserveSeqNoTests') != -1 or \
                                str(self.__class__).find('epengine.lwwepengine.LWW_EP_Engine') != -1:

                    self.services = self.get_services(self.servers, self.services_init)
                    # rebalance all nodes into the cluster before each test
                    if self.num_servers > 1:
                        self.cluster.rebalance(
                            self.servers[:self.num_servers],
                            self.servers[1:self.num_servers],
                            [],
                            services=self.services)
                elif self.nodes_init > 1 and not self.skip_init_check_cbserver:
                    self.services = self.get_services(self.servers[:self.nodes_init], self.services_init)
                    self.cluster.rebalance(self.servers[:1],
                                           self.servers[1:self.nodes_init],
                                           [],
                                           services=self.services)
                elif str(self.__class__).find('ViewQueryTests') != -1 and \
                        not self.input.param("skip_rebalance", False):
                    self.services = self.get_services(self.servers, self.services_init)
                    if self.num_servers > 1:
                        self.cluster.rebalance(self.servers, self.servers[1:],
                                           [], services=self.services)
                self.setDebugLevel(service_type="index")
                if not self.disable_diag_eval_on_non_local_host:
                    self.enable_diag_eval_on_non_local_hosts()
            except BaseException as e:
                # increase case_number to retry tearDown in setup for the next test
                self.case_number += 1000
                traceback.print_exc()
                self.fail(e)

            if self.dgm_run:
                self.quota = 256
            if self.total_buckets > 10:
                self.log.info("================== changing max buckets from 10 to {0} =================" \
                              .format(self.total_buckets))
                self.change_max_buckets(self, self.total_buckets)
            if self.total_buckets > 0 and not self.skip_init_check_cbserver:
                """ from sherlock, we have index service that could take some
                    RAM quota from total RAM quota for couchbase server.  We need
                    to get the correct RAM quota available to create bucket(s)
                    after all services were set """
                node_info = RestConnection(self.master).get_nodes_self()
                if node_info.memoryQuota and int(node_info.memoryQuota) > 0 :
                    ram_available = node_info.memoryQuota
                else:
                    ram_available = self.quota
                if self.bucket_size is None:
                    if self.dgm_run:
                        """ if dgm is set,
                            we need to set bucket size to dgm setting """
                        self.bucket_size = self.quota
                    else:
                        self.bucket_size = self._get_bucket_size(ram_available,\
                                                            self.total_buckets)

            self.bucket_base_params['membase']['non_ephemeral']['size'] = self.bucket_size
            self.bucket_base_params['membase']['ephemeral']['size'] = self.bucket_size
            self.bucket_base_params['memcached']['size'] = self.bucket_size

            if str(self.__class__).find('upgrade_tests') == -1 and \
                    str(self.__class__).find('newupgradetests') == -1 and \
                    not self.skip_bucket_setup:
                self._bucket_creation()
            self.log.info("==============  basetestcase setup was finished for test #{0} {1} =============="
                          .format(self.case_number, self._testMethodName))

            if not self.skip_init_check_cbserver:
                status, content, header = self._log_start(self)
                if not status:
                    self.sleep(10)
            self.print_cluster_stats()
        except Exception as e:
            traceback.print_exc()
            self.cluster.shutdown(force=True)
            self.fail(e)

    def print_cluster_stats(self):
        cluster_stats = RestConnection(self.master).get_cluster_stats()
        self.log.info("------- Cluster statistics -------")
        for cluster_node, node_stats in list(cluster_stats.items()):
            self.log.info("{0} => {1}".format(cluster_node, node_stats))
        self.log.info("--- End of cluster statistics ---")

    def get_cbcollect_info(self, server):
        """Collect cbcollectinfo logs for all the servers in the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        print("grabbing cbcollect from {0}".format(server.ip))
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
            TestInputSingleton.input.test_params[
                "get-cbcollect-info"] = False
        except Exception as e:
            self.log.error("IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}"
                           .format(server.ip, e))

    def tearDown(self):
        self.print_cluster_stats()

        if self.skip_setup_cleanup:
            return
        try:
            if hasattr(self, 'skip_buckets_handle') and self.skip_buckets_handle:
                return
            test_failed = (hasattr(self, '_resultForDoCleanups') and
                           len(self._resultForDoCleanups.failures or
                               self._resultForDoCleanups.errors)) or \
                          (hasattr(self, '_exc_info') and
                           self._exc_info()[1] is not None)

            if test_failed and TestInputSingleton.input.param("stop-on-failure", False) \
                    or self.input.param("skip_cleanup", False):
                self.log.warn("CLEANUP WAS SKIPPED")
            else:

                if test_failed:
                    # collect logs here instead of in test runner because we have not shut things down
                    if TestInputSingleton.param("get-cbcollect-info", False):
                        for server in self.servers:
                            self.log.info("Collecting logs @ {0}".format(server.ip))
                            self.get_cbcollect_info(server)
                        # collected logs so turn it off so it is not done later
                        TestInputSingleton.input.test_params["get-cbcollect-info"] = False

                    if TestInputSingleton.input.param('get_trace', None):
                        for server in self.servers:
                            try:
                                shell = RemoteMachineShellConnection(server)
                                output, _ = shell.execute_command("ps -aef|grep %s" %
                                                                  TestInputSingleton.input.param('get_trace', None))
                                output = shell.execute_command("pstack %s" % output[0].split()[1].strip())
                                print(output[0])
                            except:
                                pass
                    if self.input.param('BUGS', False):
                        self.log.warn("Test failed. Possible reason is: {0}".format(self.input.param('BUGS', False)))





                self.log.info("==============  basetestcase cleanup was started for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
                rest = RestConnection(self.master)
                alerts = rest.get_alerts()
                if self.force_kill_memcached:
                    self.kill_memcached()
                if self.forceEject:
                    self.force_eject_nodes()
                if alerts is not None and len(alerts) != 0:
                    self.log.warn("Alerts were found: {0}".format(alerts))
                if rest._rebalance_progress_status() == 'running':
                    self.kill_memcached()
                    self.log.warning("rebalancing is still running, test should be verified")
                    stopped = rest.stop_rebalance()
                    self.assertTrue(stopped, msg="unable to stop rebalance")
                BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
                ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)
                ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
                self.log.info("==============  basetestcase cleanup was finished for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
        except BaseException:
            # kill memcached
            self.kill_memcached()
            # increase case_number to retry tearDown in setup for the next test
            self.case_number += 1000
        finally:
            if not self.input.param("skip_cleanup", False):
                self.reset_cluster()
            # stop all existing task manager threads
            if self.cleanup:
                self.cleanup = False
            else:
                self.reset_env_variables()
            self.cluster.shutdown(force=True)
            self._log_finish(self)

    def get_index_map(self):
        return RestConnection(self.master).get_index_status()

    @staticmethod
    def change_max_buckets(self, total_buckets):
        command = "curl -X POST -u {0}:{1} -d maxBucketCount={2} http://{3}:{4}/internalSettings".format \
            (self.servers[0].rest_username,
             self.servers[0].rest_password,
             total_buckets,
             self.servers[0].ip,
             self.servers[0].port)
        shell = RemoteMachineShellConnection(self.servers[0])
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            return RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def sleep(self, timeout=15, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def _cluster_cleanup(self):
        rest = RestConnection(self.master)
        alerts = rest.get_alerts()
        if rest._rebalance_progress_status() == 'running':
            self.kill_memcached()
            self.log.warning("rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)
        # all required checks are implemented in ClusterOperationHelper.cleanup_cluster
        #self.sleep(10)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)


    def _initialize_nodes(self, cluster, servers, disabled_consistent_view=None, rebalanceIndexWaitingDisabled=None,
                          rebalanceIndexPausingDisabled=None, maxParallelIndexers=None, maxParallelReplicaIndexers=None,
                          port=None, quota_percent=None, services=None):
        quota = 0
        init_tasks = []
        count = 0
        for server in servers:
            init_port = port or server.port or '8091'
            if not self.upgrade_test:
                assigned_services = services
                if self.master != server:
                    assigned_services = None
            else:
                if services is not None:
                   assigned_services = []
                   if len(servers) == len(services):
                       if len(assigned_services) == 1 :
                           assigned_services[0] = services[count]
                       else:
                           assigned_services.append(services[count])
                       count += 1
                   elif len(servers) > len(services):
                       self.log.info("service will set to first element")
                       assigned_services.append(services[0])
                else:
                   assigned_services = None
            init_tasks.append(cluster.async_init_node(server, disabled_consistent_view, rebalanceIndexWaitingDisabled,
                                                      rebalanceIndexPausingDisabled, maxParallelIndexers,
                                                      maxParallelReplicaIndexers, init_port,
                                                      quota_percent, services=assigned_services,
                                                      index_quota_percent=self.index_quota_percent,
                                                      gsi_type=self.gsi_type))
        for task in init_tasks:
            node_quota = task.result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and not len({server.ip for server in self.servers}) == 1:
            self.log.warn("RAM quota was defined less than 100 MB:")
            for server in servers:
                remote_client = RemoteMachineShellConnection(server)
                ram = remote_client.extract_remote_info().ram
                self.log.info("{0}: {1} MB".format(server.ip, ram))
                remote_client.disconnect()
        return quota

    def _create_bucket_params(self, server, bucket_name='default', replicas=1, size=0, port=11211, password=None,
                              bucket_type='membase', enable_replica_index=1, eviction_policy='valueOnly',
                              bucket_priority=None, flush_enabled=1, lww=False, maxttl=None,
                              compression_mode='passive'):
        """Create a set of bucket_parameters to be sent to all of the bucket_creation methods
        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            port - The port to create this bucket on. (String)
            password - The password for this bucket. (String)
            size - The size of the bucket to be created. (int)
            enable_replica_index - can be 0 or 1, 1 enables indexing of replica bucket data (int)
            replicas - The number of replicas for this bucket. (int)
            eviction_policy - The eviction policy for the bucket (String). Can be
                ephemeral bucket: noEviction or nruEviction
                non-ephemeral bucket: valueOnly or fullEviction.
            bucket_priority - The priority of the bucket:either none, low, or high. (String)
            bucket_type - The type of bucket. (String)
            flushEnabled - Enable or Disable the flush functionality of the bucket. (int)
            lww = determine the conflict resolution type of the bucket. (Boolean)

        Returns:
            bucket_params - A dictionary containing the parameters needed to create a bucket."""

        bucket_params = dict()
        bucket_params['server'] = server
        bucket_params['bucket_name'] = bucket_name
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
        bucket_params['maxTTL'] = maxttl
        bucket_params['compressionMode'] = compression_mode
        return bucket_params

    def _bucket_creation(self):
        if self.default_bucket:
            default_params=self._create_bucket_params(
                server=self.master, bucket_name=self.default_bucket_name, size=self.bucket_size,
                replicas=self.num_replicas, bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy, lww=self.lww,
                maxttl=self.maxttl, compression_mode=self.compression_mode)
            self.cluster.create_default_bucket(default_params)
            self.buckets.append(Bucket(name="default",
                                       authType="sasl",
                                       saslPassword="",
                                       num_replicas=self.num_replicas,
                                       bucket_size=self.bucket_size,
                                       eviction_policy=self.eviction_policy,
                                       lww=self.lww,
                                       type=self.bucket_type))
            if self.enable_time_sync:
                self._set_time_sync_on_buckets(['default'])

            self.collection_name["default"]=[]

            if self.collection:
                self.create_scope_collection(scope_num=self.scope_num, collection_num=self.collection_num)



        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)
        self._create_memcached_buckets(self.master, self.memcached_buckets)


    def _get_bucket_size(self, mem_quota, num_buckets):
        # min size is 100MB now
        return max(100, int(float(mem_quota) / float(num_buckets)))

    def _set_time_sync_on_buckets(self, buckets):

        # get the credentials beforehand
        memcache_credentials = {}
        for s in self.servers:
            memcache_admin, memcache_admin_password = RestConnection(s).get_admin_credentials()
            memcache_credentials[s.ip] = {'id':memcache_admin, 'password':memcache_admin_password}

            # this is a failed optimization, in theory sasl could be done here but it didn't work
            #client = MemcachedClient(s.ip, 11210)
            #client.sasl_auth_plain(memcache_credentials[s.ip]['id'], memcache_credentials[s.ip]['password'])

        for b in buckets:
            client1 = VBucketAwareMemcached( RestConnection(self.master), b)

            for j in range(self.vbuckets):
                #print 'doing vbucket', j
                #try:
                    active_vbucket = client1.memcached_for_vbucket ( j )
                    #print memcache_credentials[active_vbucket.host]['id'], memcache_credentials[active_vbucket.host]['password']
                    active_vbucket.sasl_auth_plain(memcache_credentials[active_vbucket.host]['id'],
                                          memcache_credentials[active_vbucket.host]['password'])
                    active_vbucket.bucket_select(b)
                    result = active_vbucket.set_time_sync_state(j, 1)
                #except:
                    #print 'got a failure'

    def _create_sasl_buckets(self, server, num_buckets, server_id=None,\
                             bucket_size=None, password='password'):
        if not num_buckets:
            return
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self.bucket_size
        bucket_tasks = []
        if self.sasl_password != "password":
            password = self.sasl_password

        bucket_params = copy.deepcopy(self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        bucket_params['bucket_type'] = self.bucket_type

        for i in range(num_buckets):
            name = self.sasl_bucket_name + str(i)
            bucket_priority = None
            if self.sasl_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(self.sasl_bucket_priority[i])
            bucket_params['bucket_priority'] = bucket_priority

            bucket_tasks.append(self.cluster.async_create_sasl_bucket(
                name=name, password=password, bucket_params=bucket_params))
            self.buckets.append(Bucket(name=name, authType="sasl",
                                       saslPassword=password,
                                       num_replicas=self.num_replicas,
                                       bucket_size=self.bucket_size,
                                       master_id=server_id,
                                       eviction_policy=self.eviction_policy,
                                       lww=self.lww))
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)
        if self.enable_time_sync:
            self._set_time_sync_on_buckets(['bucket' + str(i) \
                                             for i in range(num_buckets)])
        for i in range(num_buckets):
            name = self.sasl_bucket_name + str(i)
            self.collection_name[name]=[]

    def _create_standard_buckets(self, server, num_buckets, server_id=None, bucket_size=None):
        if not num_buckets:
            return
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self.bucket_size
        bucket_tasks = []

        bucket_params = copy.deepcopy(self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        bucket_params['bucket_type'] = self.bucket_type
        cluster_compatibility = RestConnection(server).check_cluster_compatibility("5.0")
        if cluster_compatibility is None:
            pre_spock = True
        else:
            pre_spock = not cluster_compatibility
        for i in range(num_buckets):
            name = 'standard_bucket' + str(i)
            port = STANDARD_BUCKET_PORT + i + 1
            if pre_spock:
                bucket_params['proxyPort'] = port
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(self.standard_bucket_priority[i])

            bucket_params['bucket_priority'] = bucket_priority
            bucket_tasks.append(self.cluster.async_create_standard_bucket(name=name, port=port,
                                                                          bucket_params=bucket_params))
            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None,
                                       num_replicas=self.num_replicas,
                                       bucket_size=self.bucket_size,
                                       port=port, master_id=server_id,
                                       eviction_policy=self.eviction_policy, lww=self.lww))

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

        if self.enable_time_sync:
            self._set_time_sync_on_buckets(['standard_bucket' + str(i) for i in range(num_buckets)])

        for i in range(num_buckets):
            name = 'standard_bucket' + str(i)
            self.collection_name[name]=[]
            if self.collection:
                if self.standard_buckets_scope[i] is not list:
                    scope_num=self.standard_buckets_scope[i]
                    collection_num = [2] * self.standard_buckets_scope[i]
                else:
                    scope_num=self.standard_buckets_scope[i].remove
                    collection_num = self.standard_buckets_scope[i]
                self.create_scope_collection(scope_num=scope_num, collection_num=collection_num, bucket=name)

    def _create_buckets(self, server, bucket_list, server_id=None, bucket_size=None):
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self._get_bucket_size(self.quota, len(bucket_list))
        bucket_tasks = []
        if self.parallelism:
            i = random.randint(1, 10000)
        else:
            i = 0

        standard_params = self._create_bucket_params(server=server, size=bucket_size,
                                                     replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                     enable_replica_index=self.enable_replica_index,
                                                     eviction_policy=self.eviction_policy, lww=self.lww)

        for bucket_name in bucket_list:
            self.log.info(" Creating bucket {0}".format(bucket_name))
            i += 1
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(self.standard_bucket_priority[i])

            standard_params['bucket_priority']=bucket_priority
            bucket_tasks.append(self.cluster.async_create_standard_bucket(name=bucket_name, port=STANDARD_BUCKET_PORT+i,
                                                                          bucket_params=standard_params))
            self.buckets.append(Bucket(name=bucket_name, authType=None, saslPassword=None,
                                       num_replicas=self.num_replicas,
                                       bucket_size=bucket_size,
                                       port=STANDARD_BUCKET_PORT + i, master_id=server_id,
                                       eviction_policy=self.eviction_policy, type=self.bucket_type));
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

        if self.enable_time_sync:
            self._set_time_sync_on_bucket( bucket_list )

        for bucket_name in bucket_list:
            self.collection_name[bucket_name]=[]

    def _create_memcached_buckets(self, server, num_buckets, server_id=None, bucket_size=None):
        if not num_buckets:
            return
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self.bucket_size
        bucket_tasks = []

        bucket_params = copy.deepcopy(self.bucket_base_params['memcached'])
        bucket_params['size'] = bucket_size
        cluster_compatibility = RestConnection(server).check_cluster_compatibility("5.0")
        if cluster_compatibility is None:
            pre_spock = True
        else:
            pre_spock = not cluster_compatibility
        for i in range(num_buckets):

            name = 'memcached_bucket' + str(i)
            port = STANDARD_BUCKET_PORT + self.standard_buckets + 2 + i

            if pre_spock:
                bucket_params['proxyPort'] = port

            bucket_tasks.append(self.cluster.async_create_memcached_bucket(name=name, port=port,
                                                                           bucket_params=bucket_params))
            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None,
                                       num_replicas=self.num_replicas,
                                       bucket_size=bucket_size, port=port,
                                       master_id=server_id, type='memcached'));
        for task in bucket_tasks:
            task.result()

    def _all_buckets_delete(self, server):
        delete_tasks = []
        for bucket in self.buckets:
            delete_tasks.append(self.cluster.async_bucket_delete(server, bucket.name))

        for task in delete_tasks:
            task.result()
        self.buckets = []

    def _all_buckets_flush(self):
        flush_tasks = []
        for bucket in self.buckets:
            flush_tasks.append(self.cluster.async_bucket_flush(self.master, bucket.name))

        for task in flush_tasks:
            task.result()

    def _verify_stats_all_buckets(self, servers, master=None, timeout=60, collection=None):
        stats_tasks = []
        if not master:
            master = self.master
        servers = self.get_kv_nodes(servers, master)

        for bucket in self.buckets:

            items = sum([len(kv_store) for kv_store in list(bucket.kvs.values())])
            if bucket.type == 'memcached':
                items_actual = 0
                for server in servers:
                    client = MemcachedClientHelper.direct_client(server, bucket)
                    items_actual += int(client.stats()["curr_items"])

                self.assertEqual(items, items_actual, "Items are not correct")
                continue
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                                                 'curr_items', '==', items))
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                                                 'vb_active_curr_items', '==', items))

            available_replicas = self.num_replicas
            if len(servers) == self.num_replicas:
                available_replicas = len(servers) - 1
            elif len(servers) <= self.num_replicas:
                available_replicas = len(servers) - 1
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                                                 'vb_replica_curr_items', '==',
                                                                 items * available_replicas))
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                                                 'curr_items_tot', '==',
                                                                 items * (available_replicas + 1)))
        try:
            for task in stats_tasks:
                task.result(timeout)
        except Exception as e:
            self.log.info("{0}".format(e))
            for task in stats_tasks:
                task.cancel()
            self.log.error("unable to get expected stats for any node! Print taps for all nodes:")
            rest = RestConnection(self.master)
            for bucket in self.buckets:
                RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
            raise Exception("unable to get expected stats during {0} sec".format(timeout))

    """Asynchronously applys load generation to all bucekts in the cluster.
 bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp
    Args:
        server - A server in the cluster. (TestInputServer)
        kv_gen - The generator to use to generate load. (DocumentGenerator)
        op_type - "create", "read", "update", or "delete" (String)
        exp - The expiration for the items if updated or created (int)
        kv_store - The index of the bucket's kv_store to use. (int)

    Returns:
        A list of all of the tasks created.
    """

    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0,
                                only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=30,
                                proxy_client=None, collection=None):
        tasks = []

        for bucket in self.buckets:
            gen = copy.deepcopy(kv_gen)
            try :
                len(self.collection_name[bucket.name])
            except KeyError:
                self.collection_name[bucket.name] =[]
            if not collection and (len(self.collection_name[bucket.name]) > 0):
                for collections in self.collection_name[bucket.name]:
                    gen = copy.deepcopy(kv_gen)
                    if bucket.type != 'memcached':
                        tasks.append(self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                                      bucket.kvs[kv_store],
                                                                      op_type, exp, flag, only_store_hash,
                                                                      batch_size, pause_secs, timeout_secs,
                                                                      proxy_client, compression=self.sdk_compression, collection=collections))
                    else:
                        self._load_memcached_bucket(server, gen, bucket.name, collections)

            else:
                if bucket.type != 'memcached':
                        tasks.append(self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                                      bucket.kvs[kv_store],
                                                                      op_type, exp, flag, only_store_hash,
                                                                      batch_size, pause_secs, timeout_secs,
                                                                      proxy_client, compression=self.sdk_compression, collection=collection))
                else:
                    self._load_memcached_bucket(server, gen, bucket.name, collection)

        return tasks

    """Synchronously applys load generation to all bucekts in the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_gen - The generator to use to generate load. (DocumentGenerator)
        op_type - "create", "read", "update", or "delete" (String)
        exp - The expiration for the items if updated or created (int)
        kv_store - The index of the bucket's kv_store to use. (int)
    """

    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, proxy_client=None, collection=None):

        if self.enable_bloom_filter:
            for bucket in self.buckets:
                ClusterOperationHelper.flushctl_set(self.master,
                                                    "bfilter_enabled", 'true', bucket)

        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store, flag,
                                             only_store_hash, batch_size, pause_secs,
                                             timeout_secs, proxy_client, collection)

        for task in tasks:
            task.result()

        """
           Load bucket to DGM if params active_resident_threshold is passed
        """
        if self.active_resident_threshold < 100.0:
            stats_all_buckets = {}
            for bucket in self.buckets:
                stats_all_buckets[bucket.name] = StatsCommon()

            rest = RestConnection(self.master)
            for bucket in self.buckets:
                threshold_reached = False
                while not threshold_reached:
                    active_resident = \
                        rest.get_bucket_stats(bucket)['vb_active_resident_items_ratio']
                    if active_resident >= self.active_resident_threshold:
                        self.log.info(
                            "resident ratio is %s > %s for %s in bucket %s.\n"\
                            " Continue loading to the cluster" %
                                               (active_resident,
                                                self.active_resident_threshold,
                                                self.master.ip,
                                                bucket.name))
                        random_key = self.key_generator()
                        generate_load = BlobGenerator(random_key,
                                                      '%s-' % random_key,
                                                      self.value_size,
                                                      end=batch_size * 50)
                        self._load_bucket(bucket, self.master, generate_load,
                                          "create", exp=0, kv_store=1, flag=0,
                                          only_store_hash=True,
                                          batch_size=batch_size,
                                          pause_secs=5, timeout_secs=60, collection=collection)
                    else:
                        threshold_reached = True
                        self.log.info("\n DGM state achieved at %s %% for %s in bucket %s!"\
                                                                     % (active_resident,
                                                                        self.master.ip,
                                                                        bucket.name))
                        break

    def _async_load_bucket(self, bucket, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True,
                           batch_size=1000, pause_secs=1, timeout_secs=30, collection=None):
        gen = copy.deepcopy(kv_gen)
        task = self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                bucket.kvs[kv_store], op_type,
                                                exp, flag, only_store_hash,
                                                batch_size, pause_secs, timeout_secs,
                                                compression=self.sdk_compression, collection=collection)
        return task

    def _load_bucket(self, bucket, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True,
                     batch_size=1000, pause_secs=1, timeout_secs=30, collection=None):
        task = self._async_load_bucket(bucket, server, kv_gen, op_type, exp, kv_store, flag, only_store_hash,
                                       batch_size, pause_secs, timeout_secs, collection=collection)
        task.result()

    # Load all the buckets until there is no more memory
    # Assumption - all buckets are ephemeral
    # Work in progress

    def _load_all_ephemeral_buckets_until_no_more_memory(self, server, kv_gen, op_type, exp, increment, kv_store=1, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30,
                          proxy_client=None, percentage=0.90):



        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()

        for bucket in self.buckets:
            memory_is_full = False
            while not memory_is_full:
                memory_used = \
                    stats_all_buckets[bucket.name].get_stats([self.master], bucket, '',
                                                             'mem_used')[ server]
                # memory is considered full if mem_used is at say 90% of the available memory
                if int(memory_used) < percentage * self.bucket_size * 1000000:
                    self.log.info(
                        "Still have memory. %s used is less than %s MB quota for %s in bucket %s. Continue loading to the cluster" %
                        (memory_used, self.bucket_size, self.master.ip, bucket.name))

                    self._load_bucket(bucket, self.master, kv_gen, "create", exp=0, kv_store=1, flag=0,
                    only_store_hash=True, batch_size=batch_size, pause_secs=5, timeout_secs=60)
                    kv_gen.start = kv_gen.start + increment
                    kv_gen.end = kv_gen.end + increment
                    kv_gen = BlobGenerator('key-root', 'param2', self.value_size, start=kv_gen.start, end=kv_gen.end)
                else:
                    memory_is_full = True
                    self.log.info("Memory is full, %s bytes in use for %s and bucket %s!" %
                                  (memory_used, self.master.ip, bucket.name))

    def key_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    """Waits for queues to drain on all servers and buckets in a cluster.

    A utility function that waits for all of the items loaded to be persisted
    and replicated.

    Args:
        servers - A list of all of the servers in the cluster. ([TestInputServer])
        ep_queue_size - expected ep_queue_size (int)
        ep_queue_size_cond - condition for comparing (str)
        check_ep_dcp_items_remaining - to check if replication is complete
        timeout - Waiting the end of the thread. (str)
    """

    def _wait_for_stats_all_buckets(self, servers, ep_queue_size=0, \
                                    ep_queue_size_cond='==',
                                    check_ep_items_remaining=False, timeout=360, collection=None):
        tasks = []
        servers = self.get_kv_nodes(servers)
        for server in servers:
            for bucket in self.buckets:
                if bucket.type == 'memcached':
                    continue
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                                               'ep_queue_size', ep_queue_size_cond, ep_queue_size))
                if check_ep_items_remaining:
                    ep_items_remaining = 'ep_{0}_items_remaining' \
                        .format(self.protocol)
                    tasks.append(self.cluster.async_wait_for_stats([server],
                                                                   bucket, self.protocol,
                                                                   ep_items_remaining, "==", 0))
        for task in tasks:
            task.result(timeout)

    """Waits for max_unacked_bytes = 0 on all servers and buckets in a cluster.

     A utility function that waits upr flow with unacked_bytes = 0
    """

    def verify_unacked_bytes_all_buckets(self, filter_list=[], sleep_time=5, master_node=None):
        if self.verify_unacked_bytes:
            self.sleep(sleep_time)
            if master_node == None:
                servers = self.get_nodes_in_cluster()
            else:
                servers = self.get_nodes_in_cluster(master_node)
            servers = self.get_kv_nodes(servers)
            map = self.data_collector.collect_compare_dcp_stats(self.buckets, servers, filter_list=filter_list)
            for bucket in list(map.keys()):
                self.assertTrue(map[bucket], " the bucket {0} has unacked bytes != 0".format(bucket))

    """Verifies data on all of the nodes in a cluster.

    Verifies all of the data in a specific kv_store index for all buckets in
    the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_store - The kv store index to check. (int)
        timeout - Waiting the end of the thread. (str)
    """

    def _verify_all_buckets(self, server, kv_store=1, timeout=180, max_verify=None, only_store_hash=True,
                            batch_size=1000,
                            replica_to_read=None, collection_name=None):
        tasks = []

        if len(self.buckets) > 1:
            batch_size = 1
        for bucket in self.buckets:
            try :
                len(self.collection_name[bucket.name])
            except KeyError:
                self.collection_name[bucket.name] =[]
            if bucket.type == 'memcached':
                continue
            #self.collection_bucket=self.collection_name[bucket]
            if not collection_name and (len(self.collection_name[bucket.name]) > 0):
                for collections in self.collection_name[bucket.name]:
                    tasks.append(self.cluster.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify,
                                                        only_store_hash, batch_size, replica_to_read,
                                                        compression=self.sdk_compression, collection=collections))
            else:
                tasks.append(self.cluster.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify,
                                                        only_store_hash, batch_size, replica_to_read,
                                                        compression=self.sdk_compression, collection=collection_name))
        for task in tasks:
            task.result(timeout)

    def disable_compaction(self, server=None, bucket="default"):

        server = server or self.servers[0]
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.cluster.modify_fragmentation_config(server, new_config, bucket)

    def async_create_views(self, server, design_doc_name, views, bucket="default", with_query=True,
                           check_replication=False):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.cluster.async_create_view(server, design_doc_name, view, bucket, with_query,
                                                    check_replication=check_replication)
                tasks.append(t_)
        else:
            t_ = self.cluster.async_create_view(server, design_doc_name, None, bucket, with_query,
                                                check_replication=check_replication)
            tasks.append(t_)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket="default", timeout=None, check_replication=False):
        if len(views):
            for view in views:
                self.cluster.create_view(server, design_doc_name, view, bucket, timeout,
                                         check_replication=check_replication)
        else:
            self.cluster.create_view(server, design_doc_name, None, bucket, timeout,
                                     check_replication=check_replication)

    def make_default_views(self, prefix, count, is_dev_ddoc=False, different_map=False):
        ref_view = self.default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        if different_map:
            views = []
            for i in range(count):
                views.append(View(ref_view.name + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}' % str(i),
                                  None, is_dev_ddoc))
            return views
        else:
            return [View(ref_view.name + str(i), ref_view.map_func, None, is_dev_ddoc) for i in range(count)]

    def _load_doc_data_all_buckets(self, data_op="create", batch_size=1000, gen_load=None):
        # initialize the template for document generator
        age = list(range(5))
        first = ['james', 'sharon']
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        if gen_load is None:
            gen_load = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)

        self.log.info("%s %s documents..." % (data_op, self.num_items))
        self._load_all_buckets(self.master, gen_load, data_op, 0, batch_size=batch_size)
        return gen_load

    def verify_cluster_stats(self, servers=None, master=None, max_verify=None,
                             timeout=None, check_items=True, only_store_hash=True,
                             replica_to_read=None, batch_size=1000, check_bucket_stats=True,
                             check_ep_items_remaining=False, verify_total_items=True, collection=None):
        servers = self.get_kv_nodes(servers)
        if servers is None:
            servers = self.servers
        if master is None:
            master = self.master
        if max_verify is None:
            max_verify = self.max_verify

        self._wait_for_stats_all_buckets(servers, timeout=(timeout or 120),
                                         check_ep_items_remaining=check_ep_items_remaining)
        if check_items:
            try:
                self._verify_all_buckets(master, timeout=timeout, max_verify=max_verify,
                                         only_store_hash=only_store_hash, replica_to_read=replica_to_read,
                                         batch_size=batch_size, collection_name=collection)
            except ValueError as e:
                """ get/verify stats if 'ValueError: Not able to get values
                                             for following keys' was gotten """
                self._verify_stats_all_buckets(servers, timeout=(timeout or 120), collection=collection)
                raise e
            if check_bucket_stats:
                self._verify_stats_all_buckets(servers, timeout=(timeout or 120), collection=collection)
            # verify that curr_items_tot corresponds to sum of curr_items from all nodes
            if verify_total_items:
                verified = True
                for bucket in self.buckets:
                    verified &= RebalanceHelper.wait_till_total_numbers_match(master, bucket,
                                                                              timeout_in_seconds=(timeout or 500))
                self.assertTrue(verified, "Lost items!!! Replication was completed but "
                                          "          sum(curr_items) don't match the curr_items_total")
        else:
            self.log.warn("verification of items was omitted")

    def _stats_befor_warmup(self, bucket_name):
        self.pre_warmup_stats[bucket_name] = {}
        self.stats_monitor = self.input.param("stats_monitor", "")
        self.warmup_stats_monitor = self.input.param("warmup_stats_monitor", "")
        if self.stats_monitor is not '':
            self.stats_monitor = self.stats_monitor.split(";")
        if self.warmup_stats_monitor is not '':
            self.warmup_stats_monitor = self.warmup_stats_monitor.split(";")
        for server in self.servers:
            mc_conn = MemcachedClientHelper.direct_client(server, bucket_name, self.timeout)
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)] = {}
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"] = mc_conn.stats("")[
                "uptime"]
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["curr_items_tot"] = \
                mc_conn.stats("")["curr_items_tot"]
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["curr_items"] = mc_conn.stats("")[
                "curr_items"]
            for stat_to_monitor in self.stats_monitor:
                self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)][stat_to_monitor] = \
                    mc_conn.stats('')[stat_to_monitor]
            if self.without_access_log:
                for stat_to_monitor in self.warmup_stats_monitor:
                    self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)][stat_to_monitor] = \
                        mc_conn.stats('warmup')[stat_to_monitor]
            mc_conn.close()

    def _kill_nodes(self, nodes, servers, bucket_name):
        self.reboot = self.input.param("reboot", False)
        if not self.reboot:
            for node, server in zip(nodes, servers):
                _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                         "password": self.servers[0].rest_password}
                node_rest = RestConnection(_node)
                _mc = MemcachedClientHelper.direct_client(_node, bucket_name)
                self.log.info("restarted the node %s:%s" % (node.ip, node.port))
                pid = None
                try:
                    pid = _mc.stats()["pid"]
                    break
                except (EOFError, KeyError) as e:
                    if pid is None:
                        # sometimes pid is not returned by mc.stats()
                        shell = RemoteMachineShellConnection(server)
                        pid = shell.get_memcache_pid()
                        shell.disconnect()
                        if pid is None:
                            self.fail("impossible to get a PID")
                command = "os:cmd(\"kill -9 {0} \")".format(pid)
                self.log.info(command)
                killed = node_rest.diag_eval(command)
                self.log.info("killed ??  {0} ".format(killed))
                _mc.close()
        else:
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                command = "reboot"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()
                time.sleep(self.wait_timeout * 8)
                shell = RemoteMachineShellConnection(server)
                command = "/sbin/iptables -F"
                output, error = shell.execute_command(command)
                command = "iptables -F"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()

    def _restart_memcache(self, bucket_name):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        is_partial = self.input.param("is_partial", "True")
        _nodes = []
        if len(self.servers) > 1:
            skip = 2
        else:
            skip = 1
        if is_partial:
            _nodes = nodes[:len(nodes):skip]
        else:
            _nodes = nodes

        _servers = []
        for server in self.servers:
            for _node in _nodes:
                if server.ip == _node.ip:
                    _servers.append(server)

        self._kill_nodes(_nodes, _servers, bucket_name)

        start = time.time()
        memcached_restarted = False

        for server in _servers:
            while time.time() - start < (self.wait_timeout * 2):
                mc = None
                try:
                    mc = MemcachedClientHelper.direct_client(server, bucket_name)
                    stats = mc.stats()
                    new_uptime = int(stats["uptime"])
                    self.log.info("New warmup uptime %s:%s" % (
                        new_uptime, self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]))
                    if new_uptime < self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break;
                except Exception:
                    self.log.error("unable to connect to %s:%s for bucket %s" % (server.ip, server.port, bucket_name))
                    if mc:
                        mc.close()
                    time.sleep(5)
            if not memcached_restarted:
                self.fail("memcached did not start %s:%s for bucket %s" % (server.ip, server.port, bucket_name))

    def perform_verify_queries(self, num_views, prefix, ddoc_name, query, wait_time=120,
                               bucket="default", expected_rows=None, retry_time=2, server=None):
        tasks = []
        if server is None:
            server = self.master
        if expected_rows is None:
            expected_rows = self.num_items
        for i in range(num_views):
            tasks.append(self.cluster.async_query_view(server, prefix + ddoc_name,
                                                       self.default_view_name + str(i), query,
                                                       expected_rows, bucket, retry_time))
        try:
            for task in tasks:
                task.result(wait_time)
        except Exception as e:
            print(e);
            for task in tasks:
                task.cancel()
            raise Exception("unable to get expected results for view queries during {0} sec".format(wait_time))

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
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, error = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                self.log.info("Couchbase service is running")
                shell.disconnect()
                return
            else:
                self.log.warn("couchbase service is not running. {0}".format(output))
                self.sleep(10)
        shell.disconnect()
        self.fail("Couchbase service is not running after {0} seconds".format(wait_time))

    def _load_memcached_bucket(self, server, gen_load, bucket_name, collection=None):
        num_tries = 0
        while num_tries < 6:
            try:
                num_tries += 1
                client = MemcachedClientHelper.direct_client(server, bucket_name)
                break
            except Exception as ex:
                if num_tries < 5:
                    self.log.info("unable to create memcached client due to {0}. Try again".format(ex))
                else:
                    self.log.error("unable to create memcached client due to {0}.".format(ex))
        while gen_load.has_next():
            key, value = next(gen_load)
            for v in range(1024):
                try:
                    client.set(key, 0, 0, value, v, collection=collection)
                    break
                except:
                    pass
        client.close()

    def get_nodes_in_cluster(self, master_node=None):
        rest = None
        if master_node == None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    def change_checkpoint_params(self):
        self.chk_max_items = self.input.param("chk_max_items", None)
        self.chk_period = self.input.param("chk_period", None)
        if self.chk_max_items or self.chk_period:
            for server in self.servers:
                rest = RestConnection(server)
                if self.chk_max_items:
                    rest.set_chk_max_items(self.chk_max_items)
                if self.chk_period:
                    rest.set_chk_period(self.chk_period)

    def change_password(self, new_password="new_password"):
        nodes = RestConnection(self.master).node_statuses()


        cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password  )
        output, err, result = cli.setting_cluster(data_ramsize=False, index_ramsize=False, fts_ramsize=False, cluster_name=None,
                         cluster_username=None, cluster_password=new_password, cluster_port=False)

        self.log.info(output)
        # MB-10136 & MB-9991
        if not result:
            raise Exception("Password didn't change!")
        self.log.info("new password '%s' on nodes: %s" % (new_password, [node.ip for node in nodes]))
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.rest_password = new_password
                    break

    def change_port(self, new_port="9090", current_port='8091'):
        nodes = RestConnection(self.master).node_statuses()
        remote_client = RemoteMachineShellConnection(self.master)
        options = "--cluster-port=%s" % new_port
        cli_command = "cluster-edit"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                            cluster_host="localhost:%s" % current_port,
                                                            user=self.master.rest_username,
                                                            password=self.master.rest_password)
        self.log.info(output)
        # MB-10136 & MB-9991
        if error:
            raise Exception("Port didn't change! %s" % error)
        self.port = new_port
        self.log.info("new port '%s' on nodes: %s" % (new_port, [node.ip for node in nodes]))
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.port = new_port
                    break

    def change_env_variables(self):
        if self.vbuckets != 1024 or self.upr != None:
            for server in self.servers:
                dict = {}
                if self.vbuckets:
                    dict["COUCHBASE_NUM_VBUCKETS"] = self.vbuckets
                if self.upr != None:
                    if self.upr:
                        dict["COUCHBASE_REPL_TYPE"] = "upr"
                    else:
                        dict["COUCHBASE_REPL_TYPE"] = "tap"
                if len(dict) >= 1:
                    remote_client = RemoteMachineShellConnection(server)
                    remote_client.change_env_variables(dict)
                remote_client.disconnect()
            self.sleep(60, "waiting for server to restart")
            self.log.info("========= CHANGED ENVIRONMENT SETTING ===========")

    def reset_env_variables(self):
        if self.vbuckets != 1024 or self.upr != None:
            for server in self.servers:
                if self.upr or self.vbuckets:
                    remote_client = RemoteMachineShellConnection(server)
                    remote_client.reset_env_variables()
                    remote_client.disconnect()
            self.log.info("========= RESET ENVIRONMENT SETTING TO ORIGINAL ===========")

    def change_log_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_log_level(self.log_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED LOG LEVELS ===========")

    def change_log_location(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.configure_log_location(self.log_location)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED LOG LOCATION ===========")

    def change_stat_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_stat_periodicity(self.stat_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED STAT PERIODICITY ===========")

    def change_port_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_port_static(self.port_info)
            server.port = self.port_info
            self.log.info("New REST port %s" % server.port)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED ALL PORTS ===========")

    def force_eject_nodes(self):
        for server in self.servers:
            if server != self.servers[0]:
                try:
                    rest = RestConnection(server)
                    rest.force_eject_node()
                except BaseException as e:
                    self.log.error(e)

    def set_upr_flow_control(self, flow=True, servers=[]):
        servers = self.get_kv_nodes(servers)
        for bucket in self.buckets:
            for server in servers:
                rest = RestConnection(server)
                rest.set_enable_flow_control(bucket=bucket.name, flow=flow)
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.start_couchbase()
            remote_client.disconnect()

    def kill_memcached(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.kill_memcached()
            remote_client.disconnect()

    def get_vbucket_seqnos(self, servers, buckets, skip_consistency=False, per_node=True):
        """
            Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(buckets, servers, collect_vbucket=False,
                                                                      collect_vbucket_seqno=True,
                                                                      collect_vbucket_details=False, perNode=per_node)
        if not skip_consistency:
            new_vbucket_stats = self.compare_per_node_for_vbucket_consistency(new_vbucket_stats)
        return new_vbucket_stats

    def get_vbucket_seqnos_per_Node_Only(self, servers, buckets):
        """
            Method to get vbucket information from a cluster using cbstats
        """
        servers = self.get_kv_nodes(servers)
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(buckets, servers, collect_vbucket=False,
                                                                      collect_vbucket_seqno=True,
                                                                      collect_vbucket_details=False, perNode=True)
        self.compare_per_node_for_vbucket_consistency(new_vbucket_stats)
        return new_vbucket_stats

    def compare_vbucket_seqnos(self, prev_vbucket_stats, servers, buckets, perNode=False):
        """
            Method to compare vbucket information to a previously stored value
        """
        compare = "=="
        if self.withMutationOps:
            compare = "<="
        comp_map = {}
        comp_map["uuid"] = {'type': "string", 'operation': "=="}
        comp_map["abs_high_seqno"] = {'type': "long", 'operation': compare}
        comp_map["purge_seqno"] = {'type': "string", 'operation': compare}

        new_vbucket_stats = {}
        self.log.info(" Begin Verification for vbucket sequence numbers comparison ")
        if perNode:
            new_vbucket_stats = self.get_vbucket_seqnos_per_Node_Only(servers, buckets)
        else:
            new_vbucket_stats = self.get_vbucket_seqnos(servers, buckets)
        isNotSame = True
        result = ""
        summary = ""
        if not perNode:
            compare_vbucket_seqnos_result = self.data_analyzer.compare_stats_dataset(prev_vbucket_stats,
                                                                                     new_vbucket_stats, "vbucket_id",
                                                                                     comparisonMap=comp_map)
            isNotSame, summary, result = self.result_analyzer.analyze_all_result(compare_vbucket_seqnos_result,
                                                                                 addedItems=False, deletedItems=False,
                                                                                 updatedItems=False)
        else:
            compare_vbucket_seqnos_result = self.data_analyzer.compare_per_node_stats_dataset(prev_vbucket_stats,
                                                                                              new_vbucket_stats,
                                                                                              "vbucket_id",
                                                                                              comparisonMap=comp_map)
            isNotSame, summary, result = self.result_analyzer.analyze_per_node_result(compare_vbucket_seqnos_result,
                                                                                      addedItems=False,
                                                                                      deletedItems=False,
                                                                                      updatedItems=False)
        self.assertTrue(isNotSame, summary)
        self.log.info(" End Verification for vbucket sequence numbers comparison ")
        return new_vbucket_stats

    def compare_per_node_for_vbucket_consistency(self, map1, check_abs_high_seqno=False, check_purge_seqno=False):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = {}
        logic = True
        for bucket in list(map1.keys()):
            map = {}
            nodeMap = {}
            output = ""
            for node in list(map1[bucket].keys()):
                for vbucket in list(map1[bucket][node].keys()):
                    uuid = map1[bucket][node][vbucket]['uuid']
                    abs_high_seqno = map1[bucket][node][vbucket]['abs_high_seqno']
                    purge_seqno = map1[bucket][node][vbucket]['purge_seqno']
                    if vbucket in list(map.keys()):
                        if map[vbucket]['uuid'] != uuid:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original in node {2}. UUID {3}, Change in node {4}. UUID {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['uuid'], node, uuid)
                        if check_abs_high_seqno and int(map[vbucket]['abs_high_seqno']) != int(abs_high_seqno):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original in node {2}. UUID {3}, Change in node {4}. UUID {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['abs_high_seqno'], node, abs_high_seqno)
                        if check_purge_seqno and int(map[vbucket]['purge_seqno']) != int(purge_seqno):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original in node {2}. UUID {3}, Change in node {4}. UUID {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['abs_high_seqno'], node, abs_high_seqno)
                    else:
                        map[vbucket] = {}
                        map[vbucket]['uuid'] = uuid
                        map[vbucket]['abs_high_seqno'] = abs_high_seqno
                        map[vbucket]['purge_seqno'] = purge_seqno
                        nodeMap[vbucket] = node
            bucketMap[bucket] = map
        self.assertTrue(logic, output)
        return bucketMap

    def print_results_per_node(self, map):
        """ Method to print map results - Used only for debugging purpose """
        output = ""
        for bucket in list(map.keys()):
            print("----- Bucket {0} -----".format(bucket))
            for node in list(map[bucket].keys()):
                print("-------------Node {0}------------".format(node))
                for vbucket in list(map[bucket][node].keys()):
                    print("   for vbucket {0}".format(vbucket))
                    for key in list(map[bucket][node][vbucket].keys()):
                        print("            :: for key {0} = {1}".format(key, map[bucket][node][vbucket][key]))

    def get_meta_data_set_all(self, dest_server, kv_store=1):
        """ Method to get all meta data set for buckets and from the servers """
        data_map = {}
        for bucket in self.buckets:
            self.log.info(" Collect data for bucket {0}".format(bucket.name))
            task = self.cluster.async_get_meta_data(dest_server, bucket, bucket.kvs[kv_store],
                                                    compression=self.sdk_compression)
            task.result()
            data_map[bucket.name] = task.get_meta_data_store()
        return data_map

    def get_data_set_all(self, servers, buckets, path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        servers = self.get_kv_nodes(servers)
        info, dataset = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False, mode=mode)
        return dataset

    def get_data_set_with_data_distribution_all(self, servers, buckets, path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        servers = self.get_kv_nodes(servers)
        info, dataset = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False, mode=mode)
        distribution = self.data_analyzer.analyze_data_distribution(dataset)
        return dataset, distribution

    def get_vb_distribution_active_replica(self, servers=[], buckets=[]):
        """ Method to distribution analysis for active and replica vbuckets """
        servers = self.get_kv_nodes(servers)
        active, replica = self.data_collector.collect_vbucket_num_stats(servers, buckets)
        active_result, replica_result = self.data_analyzer.compare_analyze_active_replica_vb_nums(active, replica)
        return active_result, replica_result

    def get_and_compare_active_replica_data_set_all(self, servers, buckets, path=None, mode="disk"):
        """
           Method to get all data set for buckets and from the servers
           1)  Get active and replica data in the cluster
           2)  Compare active and replica data in the cluster
           3)  Return active and replica data
        """
        servers = self.get_kv_nodes(servers)
        info, disk_replica_dataset = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False,
                                                                      getReplica=True, mode=mode)
        info, disk_active_dataset = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False,
                                                                     getReplica=False, mode=mode)
        self.log.info(" Begin Verification for Active Vs Replica ")
        comparison_result = self.data_analyzer.compare_all_dataset(info, disk_replica_dataset, disk_active_dataset)
        logic, summary, output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems=False,
                                                                         addedItems=False, updatedItems=False)
        self.assertTrue(logic, summary)
        self.log.info(" End Verification for Active Vs Replica ")
        return disk_replica_dataset, disk_active_dataset

    def data_active_and_replica_analysis(self, server, max_verify=None, only_store_hash=True, kv_store=1):
        for bucket in self.buckets:
            task = self.cluster.async_verify_active_replica_data(server, bucket, bucket.kvs[kv_store], max_verify,
                                                                 self.sdk_compression)
            task.result()

    def data_meta_data_analysis(self, dest_server, meta_data_store, kv_store=1):
        for bucket in self.buckets:
            task = self.cluster.async_verify_meta_data(dest_server, bucket, bucket.kvs[kv_store],
                                                       meta_data_store[bucket.name])
            task.result()

    def _verify_replica_distribution_in_zones(self, nodes):
        """
        Verify the replica distribution in nodes in different zones.
        Validate that no replicas of a node are in the same zone.
        :param nodes: Map of the nodes in different zones. Each key contains the zone name and the ip of nodes in that zone.
        """
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            cbstat_command = "{0:s}cbstats".format(testconstants.LINUX_COUCHBASE_BIN_PATH)
        elif info == 'windows':
            cbstat_command = "{0:s}cbstats".format(testconstants.WIN_COUCHBASE_BIN_PATH)
        elif info == 'mac':
            cbstat_command = "{0:s}cbstats".format(testconstants.MAC_COUCHBASE_BIN_PATH)
        else:
            raise Exception("OS not supported.")
        saslpassword = ''
        versions = RestConnection(self.master).get_nodes_versions()
        for group in nodes:
            for node in nodes[group]:
                if versions[0][:5] in testconstants.COUCHBASE_VERSION_2:
                    command = "tap"
                    if not info == 'windows':
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep :vb_filter: |  awk '{print $1}' \
                            | xargs | sed 's/eq_tapq:replication_ns_1@//g'  | sed 's/:vb_filter://g' \
                            " % (cbstat_command, node, command, "default", saslpassword)
                    else:
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep.exe :vb_filter: | gawk.exe '{print $1}' \
                               | sed.exe 's/eq_tapq:replication_ns_1@//g'  | sed.exe 's/:vb_filter://g' \
                               " % (cbstat_command, node, command, "default", saslpassword)
                    output, error = shell.execute_command(commands)
                elif versions[0][:5] in testconstants.COUCHBASE_VERSION_3 or \
                                versions[0][:5] in testconstants.COUCHBASE_FROM_VERSION_4:
                    command = "dcp"
                    if not info == 'windows':
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep :replication:ns_1@%s |  grep vb_uuid | \
                                    awk '{print $1}' | sed 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                    sed 's/:.*//g' | sort -u | xargs \
                                   " % (cbstat_command, node, command, "default", saslpassword, node, node)
                        output, error = shell.execute_command(commands)
                    else:
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep.exe :replication:ns_1@%s |  grep vb_uuid | \
                                    gawk.exe '{print $1}' | sed.exe 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                    sed.exe 's/:.*//g' \
                                   " % (cbstat_command, node, command, "default", saslpassword, node, node)
                        output, error = shell.execute_command(commands)
                        output = sorted(set(output))
                shell.log_command_output(output, error)
                output = output[0].split(" ")
                if node not in output:
                    self.log.info("{0}".format(nodes))
                    self.log.info("replicas of node {0} are in nodes {1}".format(node, output))
                    self.log.info("replicas of node {0} are not in its zone {1}".format(node, group))
                else:
                    raise Exception("replica of node {0} are on its own zone {1}".format(node, group))
        shell.disconnect()

    def vb_distribution_analysis(self, servers=[], buckets=[], total_vbuckets=0, std=1.0, type="rebalance",
                                 graceful=True):
        """
            Method to check vbucket distribution analysis after rebalance
        """
        self.log.info(" Begin Verification for vb_distribution_analysis")
        servers = self.get_kv_nodes(servers)
        if self.std_vbucket_dist != None:
            std = self.std_vbucket_dist
        if self.vbuckets != None and self.vbuckets != self.total_vbuckets:
            self.total_vbuckets = self.vbuckets
        active, replica = self.get_vb_distribution_active_replica(servers=servers, buckets=buckets)
        for bucket in list(active.keys()):
            self.log.info(" Begin Verification for Bucket {0}".format(bucket))
            active_result = active[bucket]
            replica_result = replica[bucket]
            if graceful or type == "rebalance":
                self.assertTrue(active_result["total"] == total_vbuckets,
                                "total vbuckets do not match for active data set (= criteria), actual {0} expectecd {1}".format(
                                    active_result["total"], total_vbuckets))
            else:
                self.assertTrue(active_result["total"] <= total_vbuckets,
                                "total vbuckets do not match for active data set  (<= criteria), actual {0} expectecd {1}".format(
                                    active_result["total"], total_vbuckets))
            if type == "rebalance":
                rest = RestConnection(self.master)
                nodes = rest.node_statuses()
                if (len(nodes) - self.num_replicas) >= 1:
                    self.assertTrue(replica_result["total"] == self.num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (= criteria), actual {0} expected {1}".format(
                                        replica_result["total"], self.num_replicas ** total_vbuckets))
                else:
                    self.assertTrue(replica_result["total"] < self.num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}".format(
                                        replica_result["total"], self.num_replicas ** total_vbuckets))
            else:
                self.assertTrue(replica_result["total"] <= self.num_replicas * total_vbuckets,
                                "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}".format(
                                    replica_result["total"], self.num_replicas ** total_vbuckets))
            self.assertTrue(active_result["std"] >= 0.0 and active_result["std"] <= std,
                            "std test failed for active vbuckets")
            self.assertTrue(replica_result["std"] >= 0.0 and replica_result["std"] <= std,
                            "std test failed for replica vbuckets")
        self.log.info(" End Verification for vb_distribution_analysis")

    def data_analysis_active_replica_all(self, prev_data_set_active, prev_data_set_replica, servers, buckets, path=None,
                                         mode="disk"):
        """
            Method to do data analysis using cb transfer
            This works at cluster level
            1) Get Active and Replica data_path
            2) Compare Previous Active and Replica data
            3) Compare Current Active and Replica data
        """
        self.log.info(" Begin Verification for data comparison ")
        info, curr_data_set_replica = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False,
                                                                       getReplica=True, mode=mode)
        info, curr_data_set_active = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False,
                                                                      getReplica=False, mode=mode)
        self.log.info(" Comparing :: Prev vs Current :: Active and Replica ")
        comparison_result_replica = self.data_analyzer.compare_all_dataset(info, prev_data_set_replica,
                                                                           curr_data_set_replica)
        comparison_result_active = self.data_analyzer.compare_all_dataset(info, prev_data_set_active,
                                                                          curr_data_set_active)
        logic_replica, summary_replica, output_replica = self.result_analyzer.analyze_all_result(
            comparison_result_replica, deletedItems=False, addedItems=False, updatedItems=False)
        logic_active, summary_active, output_active = self.result_analyzer.analyze_all_result(comparison_result_active,
                                                                                              deletedItems=False,
                                                                                              addedItems=False,
                                                                                              updatedItems=False)
        self.assertTrue(logic_replica, output_replica)
        self.assertTrue(logic_active, output_active)
        self.log.info(" Comparing :: Current :: Active and Replica ")
        comparison_result = self.data_analyzer.compare_all_dataset(info, curr_data_set_active, curr_data_set_replica)
        logic, summary, output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems=False,
                                                                         addedItems=False, updatedItems=False)
        self.log.info(" End Verification for data comparison ")

    def data_analysis_all(self, prev_data_set, servers, buckets, path=None, mode="disk", deletedItems=False,
                          addedItems=False, updatedItems=False):
        """
            Method to do data analysis using cb transfer
            This works at cluster level
        """
        self.log.info(" Begin Verification for data comparison ")
        servers = self.get_kv_nodes(servers)
        info, curr_data_set = self.data_collector.collect_data(servers, buckets, data_path=path, perNode=False,
                                                               mode=mode)
        comparison_result = self.data_analyzer.compare_all_dataset(info, prev_data_set, curr_data_set)
        logic, summary, output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems=deletedItems,
                                                                         addedItems=addedItems,
                                                                         updatedItems=updatedItems)
        self.assertTrue(logic, summary)
        self.log.info(" End Verification for data comparison ")

    def compare_per_node_for_failovers_consistency(self, map1):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = {}
        for bucket in list(map1.keys()):
            map = {}
            nodeMap = {}
            logic = True
            output = ""
            for node in list(map1[bucket].keys()):
                for vbucket in list(map1[bucket][node].keys()):
                    id = map1[bucket][node][vbucket]['id']
                    seq = map1[bucket][node][vbucket]['seq']
                    num_entries = map1[bucket][node][vbucket]['num_entries']
                    if vbucket in list(map.keys()):
                        if map[vbucket]['id'] != id:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. UUID {3}, Change node {4}. UUID {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['id'], node, id)
                        if int(map[vbucket]['seq']) == int(seq):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. seq {3}, Change node {4}. seq {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['seq'], node, seq)
                        if int(map[vbucket]['num_entries']) == int(num_entries):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. num_entries {3}, Change node {4}. num_entries {5}".format(
                                bucket, vbucket, nodeMap[vbucket], map[vbucket]['num_entries'], node, num_entries)
                    else:
                        map[vbucket] = {}
                        map[vbucket]['id'] = id
                        map[vbucket]['seq'] = seq
                        map[vbucket]['num_entries'] = num_entries
                        nodeMap[vbucket] = node
            bucketMap[bucket] = map
        self.assertTrue(logic, output)
        return bucketMap

    def get_failovers_logs(self, servers, buckets):
        """
            Method to get failovers logs from a cluster using cbstats
        """
        servers = self.get_kv_nodes(servers)
        new_failovers_stats = self.data_collector.collect_failovers_stats(buckets, servers, perNode=True)
        new_failovers_stats = self.compare_per_node_for_failovers_consistency(new_failovers_stats)
        return new_failovers_stats

    def compare_failovers_logs(self, prev_failovers_stats, servers, buckets, perNode=False, comp_map=None):
        """
            Method to compare failovers log information to a previously stored value
        """
        comp_map = {}
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': "<="}
        comp_map["num_entries"] = {'type': "string", 'operation': "<="}

        self.log.info(" Begin Verification for failovers logs comparison ")
        servers = self.get_kv_nodes(servers)
        new_failovers_stats = self.get_failovers_logs(servers, buckets)
        compare_failovers_result = self.data_analyzer.compare_stats_dataset(prev_failovers_stats, new_failovers_stats,
                                                                            "vbucket_id", comp_map)
        isNotSame, summary, result = self.result_analyzer.analyze_all_result(compare_failovers_result, addedItems=False,
                                                                             deletedItems=False, updatedItems=False)
        self.assertTrue(isNotSame, summary)
        self.log.info(" End Verification for failovers logs comparison ")
        return new_failovers_stats

    def compare_per_node_for_failovers_consistency(self, map1, vbucketMap):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = {}
        logic = True
        for bucket in list(map1.keys()):
            map = {}
            tempMap = {}
            output = ""
            for node in list(map1[bucket].keys()):
                for vbucket in list(map1[bucket][node].keys()):
                    id = map1[bucket][node][vbucket]['id']
                    seq = map1[bucket][node][vbucket]['seq']
                    num_entries = map1[bucket][node][vbucket]['num_entries']
                    state = vbucketMap[bucket][node][vbucket]['state']
                    if vbucket in list(map.keys()):
                        if map[vbucket]['id'] != id:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: UUID {4}, Change node {5} {6} UUID {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['id'], node, state, id)
                        if int(map[vbucket]['seq']) != int(seq):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: seq {4}, Change node {5} {6}  :: seq {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['seq'], node, state, seq)
                        if int(map[vbucket]['num_entries']) != int(num_entries):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: num_entries {4}, Change node {5} {6} :: num_entries {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['num_entries'], node, state, num_entries)
                    else:
                        map[vbucket] = {}
                        tempMap[vbucket] = {}
                        map[vbucket]['id'] = id
                        map[vbucket]['seq'] = seq
                        map[vbucket]['num_entries'] = num_entries
                        tempMap[vbucket]['node'] = node
                        tempMap[vbucket]['state'] = state
            bucketMap[bucket] = map
        self.assertTrue(logic, output)
        return bucketMap

    def compare_vbucketseq_failoverlogs(self, vbucketseq={}, failoverlog={}):
        """
            Method to compare failoverlog and vbucket-seq for uuid and  seq no
        """
        isTrue = True
        output = ""
        for bucket in list(vbucketseq.keys()):
            for vbucket in list(vbucketseq[bucket].keys()):
                seq = vbucketseq[bucket][vbucket]['abs_high_seqno']
                uuid = vbucketseq[bucket][vbucket]['uuid']
                fseq = failoverlog[bucket][vbucket]['seq']
                fuuid = failoverlog[bucket][vbucket]['id']
                if seq < fseq:
                    output += "\n Error Condition in bucket {0} vbucket {1}:: seq : vbucket-seq {2} != failoverlog-seq {3}".format(
                        bucket, vbucket, seq, fseq)
                    isTrue = False
                if uuid != fuuid:
                    output += "\n Error Condition in bucket {0} vbucket {1}:: uuid : vbucket-seq {2} != failoverlog-seq {3}".format(
                        bucket, vbucket, uuid, fuuid)
                    isTrue = False
        self.assertTrue(isTrue, output)

    def get_failovers_logs(self, servers, buckets):
        """
            Method to get failovers logs from a cluster using cbstats
        """
        vbucketMap = self.data_collector.collect_vbucket_stats(buckets, servers, collect_vbucket=True,
                                                               collect_vbucket_seqno=False,
                                                               collect_vbucket_details=False, perNode=True)
        new_failovers_stats = self.data_collector.collect_failovers_stats(buckets, servers, perNode=True)
        new_failovers_stats = self.compare_per_node_for_failovers_consistency(new_failovers_stats, vbucketMap)
        return new_failovers_stats

    def compare_failovers_logs(self, prev_failovers_stats, servers, buckets, perNode=False):
        """
            Method to compare failovers log information to a previously stored value
        """
        comp_map = {}
        compare = "=="
        if self.withMutationOps:
            compare = "<="
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': compare}
        comp_map["num_entries"] = {'type': "string", 'operation': compare}
        self.log.info(" Begin Verification for failovers logs comparison ")
        new_failovers_stats = self.get_failovers_logs(servers, buckets)
        compare_failovers_result = self.data_analyzer.compare_stats_dataset(prev_failovers_stats, new_failovers_stats,
                                                                            "vbucket_id", comp_map)
        isNotSame, summary, result = self.result_analyzer.analyze_all_result(compare_failovers_result, addedItems=False,
                                                                             deletedItems=False, updatedItems=False)
        self.assertTrue(isNotSame, summary)
        self.log.info(" End Verification for Failovers logs comparison ")
        return new_failovers_stats

    def get_bucket_priority(self, priority):
        if priority == None:
            return None
        if priority.lower() == 'low':
            return None
        else:
            return priority

    def expire_pager(self, servers, val=10):
        for bucket in self.buckets:
            for server in servers:
                ClusterOperationHelper.flushctl_set(server, "exp_pager_stime", val, bucket)
        self.sleep(val, "wait for expiry pager to run on all these nodes")

    def set_auto_compaction(self, rest, parallelDBAndVC="false", dbFragmentThreshold=None, viewFragmntThreshold=None,
                            dbFragmentThresholdPercentage=None,
                            viewFragmntThresholdPercentage=None, allowedTimePeriodFromHour=None,
                            allowedTimePeriodFromMin=None, allowedTimePeriodToHour=None,
                            allowedTimePeriodToMin=None, allowedTimePeriodAbort=None, bucket=None):
        output, rq_content, header = rest.set_auto_compaction(parallelDBAndVC, dbFragmentThreshold,
                                                              viewFragmntThreshold, dbFragmentThresholdPercentage,
                                                              viewFragmntThresholdPercentage, allowedTimePeriodFromHour,
                                                              allowedTimePeriodFromMin, allowedTimePeriodToHour,
                                                              allowedTimePeriodToMin, allowedTimePeriodAbort, bucket)

        if not output and (dbFragmentThresholdPercentage, dbFragmentThreshold, viewFragmntThresholdPercentage,
                           viewFragmntThreshold <= MIN_COMPACTION_THRESHOLD
                           or dbFragmentThresholdPercentage,
                           viewFragmntThresholdPercentage >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output, "it should be  impossible to set compaction value = {0}%".format(
                viewFragmntThresholdPercentage))
            self.assertTrue("errors" in json.loads(rq_content), "Error is not present in response")
            self.assertTrue(str(json.loads(rq_content)["errors"]).find("Allowed range is 2 - 100") > -1, \
                            "Error 'Allowed range is 2 - 100' expected, but was '{0}'".format(
                                str(json.loads(rq_content)["errors"])))
            self.log.info("Response contains error = '%(errors)s' as expected" % json.loads(rq_content))

    def add_remove_servers(self, servers=[], list=[], remove_list=[], add_list=[]):
        """ Add or Remove servers from server list """
        initial_list = copy.deepcopy(list)
        for add_server in add_list:
            for server in self.servers:
                if add_server != None and server.ip == add_server.ip:
                    initial_list.append(add_server)
        for remove_server in remove_list:
            for server in initial_list:
                if remove_server != None and server.ip == remove_server.ip:
                    initial_list.remove(server)
        return initial_list

    def add_built_in_server_user(self, testuser=None, rolelist=None, node=None):
        """
           From spock, couchbase server is built with some users that handles
           some specific task such as:
               cbadminbucket
           Default added user is cbadminbucket with admin role
        """
        if node is None:
            node = self.master
        rest = RestConnection(node)
        cluster_compatibility = rest.check_cluster_compatibility("5.0")
        if cluster_compatibility is None:
            pre_spock = True
        else:
            pre_spock = not cluster_compatibility
        if pre_spock:
            self.log.info("At least one of the nodes in the cluster is "
                          "pre 5.0 version. Hence not creating rbac user "
                          "for the cluster. RBAC is a 5.0 feature.")
            return
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                      'roles': 'admin'}]

        self.log.info("**** add built-in '%s' user to node %s ****" % (testuser[0]["name"],
                                                                       node.ip))
        RbacBase().create_user_source(testuser, 'builtin', node)
        self.log.info("**** add '%s' role to '%s' user ****" % (rolelist[0]["roles"],
                                                                testuser[0]["name"]))
        status = RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')

        return status

    def get_nodes(self, server):
        """ Get Nodes from list of server """
        rest = RestConnection(self.master)
        nodes = rest.get_nodes()
        return nodes

    def find_node_info(self, master, node):
        """ Get Nodes from list of server """
        target_node = None
        rest = RestConnection(master)
        nodes = rest.get_nodes()
        for server in nodes:
            if server.ip == node.ip:
                target_node = server
        return target_node

    def get_item_count(self, server, bucket):
        client = MemcachedClientHelper.direct_client(server, bucket)
        return int(client.stats()["curr_items"])

    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    self.log.info("Membase stopped")
                shell.disconnect()
                break

    def start_server(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.start_couchbase()
                    self.log.info("Couchbase started")
                else:
                    shell.start_membase()
                    self.log.info("Membase started")
                shell.disconnect()
                break

    def reset_cluster(self):
        if self.targetMaster or self.reset_services:
            try:
                for node in self.servers:
                    shell = RemoteMachineShellConnection(node)
                    # Start node
                    rest = RestConnection(node)
                    data_path = rest.get_data_path()
                    # Stop node
                    self.stop_server(node)
                    # Delete Path
                    shell.cleanup_data_config(data_path)
                    self.start_server(node)

                    # If Ipv6 update dist_cfg file post server restart to change distribution to IPv6
                    if '.com' in node.ip or ':' in node.ip:
                        self.log.info("Updating dist_cfg for IPv6 Machines")
                        shell.update_dist_type()
                self.sleep(10)
            except Exception as ex:
                self.log.info(ex)

    def kill_server_memcached(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.kill_memcached()
                remote_client.disconnect()

    def start_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                RemoteUtilHelper.enable_firewall(server)

    def stop_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.disable_firewall()
                remote_client.disconnect()

    def get_victim_nodes(self, nodes, master=None, chosen=None, victim_type="master", victim_count=1):
        victim_nodes = [master]
        if victim_type == "graceful_failover_node":
            victim_nodes = [chosen]
        elif victim_type == "other":
            victim_nodes = self.add_remove_servers(nodes, nodes, [chosen, self.master], [])
            victim_nodes = victim_nodes[:victim_count]
        return victim_nodes

    def get_services_map(self, reset=True, master=None):
        if not reset:
            return
        else:
            self.services_map = {}
        if not master:
            master = self.master
        rest = RestConnection(master)
        map = rest.get_nodes_services()
        for key, val in map.items():
            for service in val:
                if service not in list(self.services_map.keys()):
                    self.services_map[service] = []
                self.services_map[service].append(key)

    def get_nodes_services(self):
        list_nodes_services = {}
        rest = RestConnection(self.master)
        map = rest.get_nodes_services()
        for k, v in map.items():
            if "8091" in k:
                k = k.replace(":8091", "")
            if len(v) == 1:
                v = v[0]
            elif len(v) > 1:
                v = ",".join(v)
            list_nodes_services[k] = v
        """ return {"IP": "kv", "IP": "index,kv"} """
        return list_nodes_services

    def get_buckets_itemCount(self):
        server = self.get_nodes_from_services_map(service_type="kv")
        return RestConnection(server).get_buckets_itemCount()

    def get_index_stats(self, perNode=False):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_map = None
        for server in servers:
            key = "{0}:{1}".format(server.ip, server.port)
            if perNode:
                if index_map == None:
                    index_map = {}
                index_map[key] = RestConnection(server).get_index_stats(index_map=None)
            else:
                index_map = RestConnection(server).get_index_stats(index_map=index_map)
        return index_map

    def get_nodes_from_services_map(self, service_type="n1ql", get_all_nodes=False,
                                    servers=None, master=None):
        if not servers:
            servers = self.servers
        if not master:
            master = self.master
        self.get_services_map(master=master)
        if (service_type not in self.services_map):
            self.log.info("cannot find service node {0} in cluster " \
                          .format(service_type))
        else:
            list = []
            for server_info in self.services_map[service_type]:
                tokens = server_info.rsplit(":", 1)
                ip = tokens[0]
                port = int(tokens[1])
                for server in servers:
                    """ In tests use hostname, if IP in ini file use IP, we need
                        to convert it to hostname to compare it with hostname
                        in cluster """
                    if "couchbase.com" in ip and "couchbase.com" not in server.ip:
                        shell = RemoteMachineShellConnection(server)
                        hostname = shell.get_full_hostname()
                        self.log.info("convert IP: {0} to hostname: {1}" \
                                      .format(server.ip, hostname))
                        server.ip = hostname
                        shell.disconnect()
                    elif ip.endswith(".svc"):
                        from kubernetes import client as kubeClient, config as kubeConfig
                        currNamespace = ip.split('.')[2]
                        kubeConfig.load_incluster_config()
                        v1 = kubeClient.CoreV1Api()
                        nodeList = v1.list_pod_for_all_namespaces(watch=False)

                        for node in nodeList.items:
                            if node.metadata.namespace == currNamespace and \
                               node.status.pod_ip == server.ip:
                                ip = node.status.pod_ip
                                break
                    elif "couchbase.com" in server.ip and "couchbase.com" not in ip:
                        node = TestInputServer()
                        node.ip = ip
                        """ match node.ip to server in ini file to get correct credential """
                        for server in servers:
                            shell = RemoteMachineShellConnection(server)
                            ips = shell.get_ip_address()
                            if node.ip in ips:
                                node.ssh_username = server.ssh_username
                                node.ssh_password = server.ssh_password
                                break

                        shell = RemoteMachineShellConnection(node)
                        hostname = shell.get_full_hostname()
                        self.log.info("convert IP: {0} to hostname: {1}" \
                                      .format(ip, hostname))
                        ip = hostname
                        shell.disconnect()
                    if (port != 8091 and port == int(server.port)) or \
                            (port == 8091 and server.ip.lower() == ip.lower()):
                        list.append(server)
            self.log.info("list of {0} nodes in cluster: {1}".format(service_type, list))
            if get_all_nodes:
                return list
            else:
                try:
                    return list[0]
                except IndexError as e:
                    self.log.info(self.services_map)
                    raise e

    def get_services(self, tgt_nodes, tgt_services, start_node=1):
        services = []
        if tgt_services == None:
            for node in tgt_nodes[start_node:]:
                if node.services != None and node.services != '':
                    services.append(node.services)
            if len(services) > 0:
                return services
            else:
                return None
        if tgt_services != None and "-" in tgt_services:
            services = tgt_services.replace(":", ",").split("-")[start_node:]
        elif tgt_services != None:
            for node in range(start_node, len(tgt_nodes)):
                services.append(tgt_services.replace(":", ","))
        return services

    def generate_map_nodes_out_dist(self):
        self.index_nodes_out = []
        self.nodes_out_list = []
        self.get_services_map(reset=True)
        if self.nodes_out_dist == None:
            if len(self.servers) > 1:
                self.nodes_out_list.append(self.servers[1])
            return
        for service_fail_map in self.nodes_out_dist.split("-"):
            tokens = service_fail_map.rsplit(":", 1)
            count = 0
            service_type = tokens[0]
            service_type_count = int(tokens[1])
            compare_string_master = "{0}:{1}".format(self.master.ip, self.master.port)
            compare_string_index_manager = "{0}:{1}".format(self.indexManager.ip, self.master.port)
            if service_type in list(self.services_map.keys()):
                for node_info in self.services_map[service_type]:
                    for server in self.servers:
                        compare_string_server = "{0}:{1}".format(server.ip, server.port)
                        addNode = False
                        if (self.targetMaster and (not self.targetIndexManager)) \
                                and (
                                                compare_string_server == node_info and compare_string_master == compare_string_server):
                            addNode = True
                            self.master = self.servers[1]
                        elif ((not self.targetMaster) and (not self.targetIndexManager)) \
                                and (
                                                    compare_string_server == node_info and compare_string_master != compare_string_server \
                                                and compare_string_index_manager != compare_string_server):
                            addNode = True
                        elif ((not self.targetMaster) and self.targetIndexManager) \
                                and (
                                                    compare_string_server == node_info and compare_string_master != compare_string_server
                                        and compare_string_index_manager == compare_string_server):
                            addNode = True
                        if addNode and (server not in self.nodes_out_list) and count < service_type_count:
                            count += 1
                            if service_type == "index":
                                if server not in self.index_nodes_out:
                                    self.index_nodes_out.append(server)
                            self.nodes_out_list.append(server)

    def generate_services_map(self, nodes, services=None):
        map = {}
        index = 0
        if services == None:
            return map
        for node in nodes:
            map[node.ip] = node.services
            index += 1
        return map

    def find_nodes_in_list(self):
        self.nodes_in_list = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self.services_in = self.get_services(self.nodes_in_list, self.services_in, start_node=0)

    def filter_nodes(self, filter_nodes):
        src_nodes = []
        for node in filter_nodes:
            check = False
            for src_node in self.servers:
                if node.ip == src_node.ip:
                    src_nodes.append(src_node)
        return src_nodes

    def load(self, generators_load, buckets=None, exp=0, flag=0,
             kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
             timeout_secs=30, op_type='create', start_items=0, verify_data=True, collection=None):
        if not buckets:
            buckets = self.buckets
        gens_load = {}
        for bucket in buckets:
            tmp_gen = []
            for generator_load in generators_load:
                tmp_gen.append(copy.deepcopy(generator_load))
            gens_load[bucket] = copy.deepcopy(tmp_gen)
        tasks = []
        items = 0
        for bucket in buckets:
            for gen_load in gens_load[bucket]:
                try:
                    gen_load.end = int(gen_load.end)
                except ValueError:
                    gen_load.end = 0
                    pass
                try:
                    gen_load.start = int(gen_load.start)
                except ValueError:
                    gen_load.start = 0
                    pass

                items += (gen_load.end - gen_load.start)
        for bucket in buckets:
            self.log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
            tasks.append(self.cluster.async_load_gen_docs(self.master, bucket.name,
                                                          gens_load[bucket],
                                                          bucket.kvs[kv_store], op_type, exp, flag,
                                                          only_store_hash, batch_size, pause_secs,
                                                          timeout_secs, compression=self.sdk_compression, collection=collection))
        try: 
          for task in tasks:
            task.result()
          self.num_items = items + start_items
        except Exception as e:
          traceback.print_exc()

        if verify_data:
            self.verify_cluster_stats(self.servers[:self.nodes_init])
        self.log.info("LOAD IS FINISHED")

    def async_load(self, generators_load, exp=0, flag=0,
                   kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
                   timeout_secs=30, op_type='create', start_items=0, collection=None):
        gens_load = {}
        for bucket in self.buckets:
            tmp_gen = []
            for generator_load in generators_load:
                tmp_gen.append(copy.deepcopy(generator_load))
            gens_load[bucket] = copy.deepcopy(tmp_gen)
        tasks = []
        items = 0
        for gen_load in gens_load[self.buckets[0]]:
            items += (gen_load.end - gen_load.start)

        for bucket in self.buckets:
            self.log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
            tasks.append(self.cluster.async_load_gen_docs(self.master, bucket.name,
                                                          gens_load[bucket],
                                                          bucket.kvs[kv_store], op_type, exp, flag,
                                                          only_store_hash, batch_size, pause_secs,
                                                          timeout_secs, compression=self.sdk_compression, collection=collection))
        self.num_items = items + start_items
        return tasks

    def generate_full_docs_list(self, gens_load=[], keys=[], update=False):
        all_docs_list = []
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                key, val = next(doc_gen)
                try:
                    val = json.loads(val)
                    if isinstance(val, dict) and 'mutated' not in list(val.keys()):
                        if update:
                            val['mutated'] = 1
                        else:
                            val['mutated'] = 0
                    else:
                        val['mutated'] += val['mutated']
                except TypeError:
                    pass
                if keys:
                    if not (key in keys):
                        continue
                all_docs_list.append(val)
        return all_docs_list

    def calculate_data_change_distribution(self, create_per=0, update_per=0,
                                           delete_per=0, expiry_per=0, start=0, end=0):
        count = end - start
        change_dist_map = {}
        create_count = int(count * create_per)
        start_pointer = start
        end_pointer = start
        if update_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * update_per)
            change_dist_map["update"] = {"start": start_pointer, "end": end_pointer}
        if expiry_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * expiry_per)
            change_dist_map["expiry"] = {"start": start_pointer, "end": end_pointer}
        if delete_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * delete_per)
            change_dist_map["delete"] = {"start": start_pointer, "end": end_pointer}
        if (1 - (update_per + delete_per + expiry_per)) != 0:
            start_pointer = end_pointer
            end_pointer = end
            change_dist_map["remaining"] = {"start": start_pointer, "end": end_pointer}
        if create_per != 0:
            change_dist_map["create"] = {"start": end, "end": create_count + end + 1}
        return change_dist_map

    def get_protocol_type(self):
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "3" > version:
                return "tap"
        return "dcp"

    def set_testrunner_client(self):
        self.testrunner_client = self.input.param("testrunner_client", None)
        if self.testrunner_client != None:
            os.environ[testconstants.TESTRUNNER_CLIENT] = self.testrunner_client

    def sync_ops_all_buckets(self, docs_gen_map={}, batch_size=10, verify_data=True):
        for key in list(docs_gen_map.keys()):
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    verify_data = False
                    self.expiry = 3
                self.load(docs_gen_map[key], op_type=op_type, exp=self.expiry, verify_data=verify_data,
                          batch_size=batch_size)
        if "expiry" in list(docs_gen_map.keys()):
            self._expiry_pager(self.master)

    def async_ops_all_buckets(self, docs_gen_map={}, batch_size=10):
        tasks = []
        if "expiry" in list(docs_gen_map.keys()):
            self._expiry_pager(self.master)
        for key in list(docs_gen_map.keys()):
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    self.expiry = 3
                tasks += self.async_load(docs_gen_map[key], op_type=op_type, exp=self.expiry, batch_size=batch_size)
        return tasks

    def _expiry_pager(self, master, val=10):
        for bucket in self.buckets:
            ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", val, bucket)

    def _version_compatability(self, compatible_version):
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if compatible_version <= version:
                return True
        return False

    def _run_compaction(self, number_of_times=100):
        try:
            for x in range(1, number_of_times):
                for bucket in self.buckets:
                    RestConnection(self.master).compact_bucket(bucket.name)
        except Exception as ex:
            self.log.info(ex)

    def get_kv_nodes(self, servers=None, master=None):
        if not master:
            master = self.master
        rest = RestConnection(master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "3.5" > version:
                return servers
        if servers is None:
            servers = self.servers
        kv_servers = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True, servers=servers, master=master)
        new_servers = []
        for server in servers:
            for kv_server in kv_servers:
                if kv_server.ip == server.ip and kv_server.port == server.port and (server not in new_servers):
                    new_servers.append(server)
        return new_servers

    def setDebugLevel(self, index_servers=None, service_type="kv"):
        index_debug_level = self.input.param("index_debug_level", None)
        if index_debug_level == None:
            return
        if index_servers == None:
            index_servers = self.get_nodes_from_services_map(service_type=service_type, get_all_nodes=True)
        json = {
            "indexer.settings.log_level": "debug",
            "projector.settings.log_level": "debug",
        }
        for server in index_servers:
            RestConnection(server).set_index_settings(json)

    def _load_data_in_buckets_using_mc_bin_client(self, bucket, data_set, max_expiry_range=None):
        client = VBucketAwareMemcached(RestConnection(self.master), bucket)
        try:
            for key in list(data_set.keys()):
                expiry = 0
                if max_expiry_range != None:
                    expiry = random.randint(1, max_expiry_range)
                o, c, d = client.set(key, expiry, 0, json.dumps(data_set[key]))
        except Exception as ex:
            print('WARN=======================')
            print(ex)

    def run_mc_bin_client(self, number_of_times=500000, max_expiry_range=30):
        data_map = {}
        for i in range(number_of_times):
            name = "key_" + str(i) + str((random.randint(1, 10000))) + str((random.randint(1, 10000)))
            data_map[name] = {"name": "none_the_less"}
        for bucket in self.buckets:
            try:
                self._load_data_in_buckets_using_mc_bin_client(bucket, data_map, max_expiry_range)
            except Exception as ex:
                self.log.info(ex)

    '''
    Returns ip address of the requesting machine
    '''

    def getLocalIPAddress(self):
        status, ipAddress = subprocess.getstatusoutput(
            "ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        return ipAddress


    def enable_diag_eval_on_non_local_hosts(self):
        """
        Enable diag/eval to be run on non-local hosts.
        :return:
        """
        remote = RemoteMachineShellConnection(self.master)
        output, error = remote.enable_diag_eval_on_non_local_hosts()
        if output is not None:
            if "ok" not in str(output):
                self.log.error("Error in enabling diag/eval on non-local hosts on {}: Error: {}".format(self.master.ip, error))
                raise Exception("Error in enabling diag/eval on non-local hosts on {}: Error: {}".format(self.master.ip, error))
            else:
                self.log.info(
                    "Enabled diag/eval for non-local hosts from {}".format(
                        self.master.ip))
        else:
            self.log.info("Running in compatibility mode, not enabled diag/eval for non-local hosts")


    # get the dot version e.g. x.y
    def _get_version(self):
        rest = RestConnection(self.master)
        version = rest.get_nodes_self().version
        return float(version[:3])

    def create_scope(self, bucket="default", scope="scope0"):
        Collections_Rest(self.master).create_scope(bucket, scope)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0"):
        Collections_Rest(self.master).create_collection(bucket, scope, collection)

    def delete_collection(self, bucket="default", scope='_default', collection='_default'):
        Collections_Rest(self.master).delete_collection(bucket, scope, collection)

    def delete_scope(self, scope, bucket="default"):  # scope should be passed as default scope can not be deleted
        Collections_Rest(self.master).delete_scope(scope, bucket)

    def create_scope_collection(self, scope_num, collection_num, bucket="default"):
        Collections_Rest(self.master).create_scope_collection(self.collection_name, scope_num, collection_num, bucket)

    def _record_vbuckets(self, master, servers):
        map = dict()
        for bucket in self.buckets:
            self.log.info("Record vbucket for the bucket {0}"
                          .format(bucket.name))
            map[bucket.name] = RestHelper(RestConnection(master))\
                ._get_vbuckets(servers, bucket_name=bucket.name)
        #self.log.info("Map: {0}".format(map))
        return map

    def _validate_seq_no_stats(self, vbucket_stats):
        failure_dict = dict()
        corruption = False
        buckets = list(vbucket_stats.keys())
        # self.log.info("buckets : {0}".format(buckets))
        for bucket in buckets:
            failure_dict[bucket] = dict()
            nodes = list(vbucket_stats[bucket].keys())
            # self.log.info("{0} : {1}".format(bucket, nodes))
            for node in nodes:
                failure_dict[bucket][node] = dict()
                vb_list = list(vbucket_stats[bucket][node].keys())
                for vb in vb_list:
                    last_persisted_seqno = int(vbucket_stats[bucket][node][vb]["last_persisted_seqno"])
                    last_persisted_snap_start = int(vbucket_stats[bucket][node][vb]["last_persisted_snap_start"])
                    last_persisted_snap_end = int(vbucket_stats[bucket][node][vb]["last_persisted_snap_end"])
                    if last_persisted_snap_start > last_persisted_seqno or \
                            last_persisted_snap_start > last_persisted_snap_end:
                        failure_dict[bucket][node][vb] = {}
                        failure_dict[bucket][node][vb]["last_persisted_seqno"] = last_persisted_seqno
                        failure_dict[bucket][node][vb]["last_persisted_snap_start"] = last_persisted_snap_start
                        failure_dict[bucket][node][vb]["last_persisted_snap_end"] = last_persisted_snap_end
                        corruption = True
        return failure_dict, corruption

    def check_snap_start_corruption(self, servers_to_check=None):
        if servers_to_check is None:
            rest = RestConnection(self.master)
            nodes = rest.get_nodes()
            servers_to_check = []
            for node in nodes:
                for server in self.servers:
                    if node.ip == server.ip and str(node.port) == str(server.port):
                        servers_to_check.append(server)
        self.log.info("Servers to check bucket-seqno: {0}"
                      .format(servers_to_check))
        self._record_vbuckets(self.master, servers_to_check)
        vbucket_stats = self.get_vbucket_seqnos(
            servers_to_check, self.buckets, skip_consistency=True)
        failure_dict, corruption = self._validate_seq_no_stats(vbucket_stats)
        if corruption:
            self.fail("snap_start and snap_end corruption found !!! . {0}"
                      .format(failure_dict))

    def set_flusher_batch_split_trigger(self, flusher_batch_split_trigger=3, buckets=None):
        self.log.info("Changing the bucket properties by changing flusher_batch_split_trigger to {0}".
                      format(flusher_batch_split_trigger))

        rest = RestConnection(self.master)
        for bucket in buckets:
            rest.change_flusher_batch_split_trigger(
                flusher_batch_split_trigger=flusher_batch_split_trigger,
                bucket=bucket.name)

        # Restart Memcached in all cluster nodes to reflect the settings
        for server in self.get_kv_nodes(master=self.master):
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # Add warmup check instead of a blind sleep.
        # TODO: See _warmup_check in WarmUpTests class
        self.sleep(30)

        for server in self.get_kv_nodes(master=self.master):
            for bucket in buckets:
                mc = MemcachedClient(server.ip, 11210)
                mc.sasl_auth_plain(self.master.rest_username,
                                   self.master.rest_password)
                mc.bucket_select(bucket.name)
                stats = mc.stats()
                self.assertEqual(int(stats['ep_flusher_batch_split_trigger']),
                                  flusher_batch_split_trigger)
