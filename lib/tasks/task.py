import os
import time
import logger
import random
import socket
import string
import copy
import json
import re
import math
import crc32
import traceback
import testconstants
from httplib import IncompleteRead
from threading import Thread
from memcacheConstants import ERR_NOT_FOUND,NotFoundError
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import KVStoreAwareSmartClient, MemcachedClientHelper
from memcached.helper.kvstore import KVStore
from couchbase_helper.document import DesignDocument, View
from mc_bin_client import MemcachedError, MemcachedClient
from tasks.future import Future
from couchbase_helper.stats_tools import StatsCommon
from membase.api.exception import N1QLQueryException, DropIndexException, CreateIndexException, DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException, \
                                    GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, FailoverFailedException, \
                                    ServerUnavailableException, BucketFlushFailed, CBRecoveryFailedException, BucketCompactionException, AutoFailoverException
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.documentgenerator import BatchedDocumentGenerator
from TestInput import TestInputServer, TestInputSingleton
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, COUCHBASE_FROM_4DOT6,\
                          THROUGHPUT_CONCURRENCY, ALLOW_HTP, CBAS_QUOTA, COUCHBASE_FROM_VERSION_4,\
                          CLUSTER_QUOTA_RATIO
from multiprocessing import Process, Manager, Semaphore
import memcacheConstants
from membase.api.exception import CBQError

from lib.cb_tools.cbstats import Cbstats

try:
    CHECK_FLAG = False
    if (testconstants.TESTRUNNER_CLIENT in os.environ.keys()) and os.environ[testconstants.TESTRUNNER_CLIENT] == testconstants.PYTHON_SDK:
        from sdk_client import SDKSmartClient as VBucketAwareMemcached
        from sdk_client import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient
    else:
        CHECK_FLAG = True
        from memcached.helper.data_helper import VBucketAwareMemcached,KVStoreAwareSmartClient
except Exception as e:
    CHECK_FLAG = True
    from memcached.helper.data_helper import VBucketAwareMemcached,KVStoreAwareSmartClient

# TODO: Setup stacktracer
# TODO: Needs "easy_install pygments"
# import stacktracer
# stacktracer.trace_start("trace.html",interval=30,auto=True) # Set auto flag to always update file!


CONCURRENCY_LOCK = Semaphore(THROUGHPUT_CONCURRENCY)
PENDING = 'PENDING'
EXECUTING = 'EXECUTING'
CHECKING = 'CHECKING'
FINISHED = 'FINISHED'

class Task(Future):
    def __init__(self, name):
        Future.__init__(self)
        self.log = logger.Logger.get_logger()
        self.state = PENDING
        self.name = name
        self.cancelled = False
        self.retries = 0
        self.res = None

    def step(self, task_manager):
        if not self.done():
            if self.state == PENDING:
                self.state = EXECUTING
                task_manager.schedule(self)
            elif self.state == EXECUTING:
                self.execute(task_manager)
            elif self.state == CHECKING:
                self.check(task_manager)
            elif self.state != FINISHED:
                raise Exception("Bad State in {0}: {1}".format(self.name, self.state))

    def execute(self, task_manager):
        raise NotImplementedError

    def check(self, task_manager):
        raise NotImplementedError

    def set_unexpected_exception(self, e, suffix = ""):
        self.log.error("Unexpected exception [{0}] caught".format(e) + suffix)
        self.log.error(''.join(traceback.format_stack()))
        self.set_exception(e)

class NodeInitializeTask(Task):
    def __init__(self, server, disabled_consistent_view=None,
                 rebalanceIndexWaitingDisabled=None,
                 rebalanceIndexPausingDisabled=None,
                 maxParallelIndexers=None,
                 maxParallelReplicaIndexers=None,
                 port=None, quota_percent=None,
                 index_quota_percent=None,
                 services = None, gsi_type='forestdb'):
        Task.__init__(self, "node_init_task")
        self.server = server
        self.port = port or server.port
        self.quota = 0
        self.index_quota = 0
        self.index_quota_percent = index_quota_percent
        self.quota_percent = quota_percent
        self.disable_consistent_view = disabled_consistent_view
        self.rebalanceIndexWaitingDisabled = rebalanceIndexWaitingDisabled
        self.rebalanceIndexPausingDisabled = rebalanceIndexPausingDisabled
        self.maxParallelIndexers = maxParallelIndexers
        self.maxParallelReplicaIndexers = maxParallelReplicaIndexers
        self.services = services
        self.gsi_type = gsi_type

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
                self.state = FINISHED
                self.set_exception(error)
                return
        self.log.info("server: %s, nodes/self ", self.server)
        info = Future.wait_until(lambda: rest.get_nodes_self(),
                                 lambda x: x.memoryTotal > 0 or x.storageTotalRam > 0,
                                 timeout_secs=60, interval_time=0.1,
                                 exponential_backoff=False)
        self.log.info(" %s", info.__dict__)

        username = self.server.rest_username
        password = self.server.rest_password

        if int(info.port) in range(9091,9991):
            self.state = FINISHED
            self.set_result(True)
            return

        self.quota = int(info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO)
        if self.index_quota_percent:
            self.index_quota = int((info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO) * \
                                      self.index_quota_percent / 100)
            rest.set_service_memoryQuota(service='indexMemoryQuota', username=username,\
                                         password=password, memoryQuota=self.index_quota)
        if self.quota_percent:
           self.quota = int(info.mcdMemoryReserved * self.quota_percent / 100)

        """ Adjust KV RAM to correct value when there is INDEX
            and FTS services added to node from Watson  """
        index_quota = INDEX_QUOTA
        cluster_setting = rest.cluster_status()
        fts_quota = FTS_QUOTA
        if cluster_setting:
            if cluster_setting["ftsMemoryQuota"] and \
                int(cluster_setting["ftsMemoryQuota"]) >= 256:
                fts_quota = int(cluster_setting["ftsMemoryQuota"])
        kv_quota = int(info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO)
        if self.index_quota_percent:
                index_quota = self.index_quota
        if not self.quota_percent:
            set_services = copy.deepcopy(self.services)
            if set_services is None:
                set_services = ["kv"]
#             info = rest.get_nodes_self()
#             cb_version = info.version[:5]
#             if cb_version in COUCHBASE_FROM_VERSION_4:
            if "index" in set_services:
                self.log.info("quota for index service will be %s MB" % (index_quota))
                kv_quota -= index_quota
                self.log.info("set index quota to node %s " % self.server.ip)
                rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=index_quota)
            if "fts" in set_services:
                self.log.info("quota for fts service will be %s MB" % (fts_quota))
                kv_quota -= fts_quota
                self.log.info("set both index and fts quota at node %s "% self.server.ip)
                rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=fts_quota)
            if "cbas" in set_services:
                self.log.info("quota for cbas service will be %s MB" % (CBAS_QUOTA))
                kv_quota -= CBAS_QUOTA
                rest.set_service_memoryQuota(service = "cbasMemoryQuota", memoryQuota=CBAS_QUOTA)
            if kv_quota < MIN_KV_QUOTA:
                    raise Exception("KV RAM needs to be more than %s MB"
                            " at node  %s"  % (MIN_KV_QUOTA, self.server.ip))
            if kv_quota < int(self.quota):
                self.quota = kv_quota

        rest.init_cluster_memoryQuota(username, password, self.quota)

        if self.services:
            status = rest.init_node_services(username= username, password = password,\
                                          port = self.port, hostname= self.server.ip,\
                                                              services= self.services)
            if not status:
                self.state = FINISHED
                self.set_exception(Exception('unable to set services for server %s'\
                                                               % (self.server.ip)))
                return
        if self.disable_consistent_view is not None:
            rest.set_reb_cons_view(self.disable_consistent_view)
        if self.rebalanceIndexWaitingDisabled is not None:
            rest.set_reb_index_waiting(self.rebalanceIndexWaitingDisabled)
        if self.rebalanceIndexPausingDisabled is not None:
            rest.set_rebalance_index_pausing(self.rebalanceIndexPausingDisabled)
        if self.maxParallelIndexers is not None:
            rest.set_max_parallel_indexers(self.maxParallelIndexers)
        if self.maxParallelReplicaIndexers is not None:
            rest.set_max_parallel_replica_indexers(self.maxParallelReplicaIndexers)

        rest.init_cluster(username, password, self.port)
        remote_shell = RemoteMachineShellConnection(self.server)
        remote_shell.enable_diag_eval_on_non_local_hosts()
        if rest.is_cluster_compat_mode_greater_than(4.0):
            if self.gsi_type == "plasma":
                if not rest.is_cluster_compat_mode_greater_than(5.0):
                    rest.set_indexer_storage_mode(username, password, "forestdb")
                else:
                    rest.set_indexer_storage_mode(username, password, self.gsi_type)
            else:
                rest.set_indexer_storage_mode(username, password, self.gsi_type)
        self.server.port = self.port
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
                self.state = FINISHED
                self.set_exception(error)
                return
        info = rest.get_nodes_self()

        if info is None:
            self.state = FINISHED
            self.set_exception(Exception('unable to get information on a server %s, it is available?' % (self.server.ip)))
            return
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(self.quota)


class BucketCreateTask(Task):
    def __init__(self, bucket_params):
        Task.__init__(self, "bucket_create_task")
        self.server = bucket_params['server']
        self.bucket = bucket_params['bucket_name']
        self.alt_addr = TestInputSingleton.input.param("alt_addr", False)
        self.replicas = bucket_params['replicas']
        self.port = bucket_params['port']
        self.size = bucket_params['size']
        self.password = bucket_params['password']
        self.bucket_type = bucket_params['bucket_type']
        self.enable_replica_index = bucket_params['enable_replica_index']
        self.eviction_policy = bucket_params['eviction_policy']
        self.lww = bucket_params['lww']
        if 'maxTTL' in bucket_params:
            self.maxttl = bucket_params['maxTTL']
        else:
            self.maxttl = 0
        if 'compressionMode' in bucket_params:
            self.compressionMode = bucket_params['compressionMode']
        else:
            self.compressionMode = 'passive'
        self.flush_enabled = bucket_params['flush_enabled']
        if bucket_params['bucket_priority'] is None or bucket_params['bucket_priority'].lower() is 'low':
            self.bucket_priority = 3
        else:
            self.bucket_priority = 8

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        info = rest.get_nodes_self()

        if self.size <= 0:
            self.size = info.memoryQuota * 2 / 3

        authType = 'none' if self.password is None else 'sasl'

        if int(info.port) in xrange(9091, 9991):
            try:
                self.port = info.port
                rest.create_bucket(bucket=self.bucket)
                self.state = CHECKING
                task_manager.schedule(self)
            except Exception as e:
                self.state = FINISHED
                self.set_exception(e)
            return

        version = rest.get_nodes_self().version
        try:
            if float(version[:2]) >= 3.0 and self.bucket_priority is not None:
                rest.create_bucket(bucket=self.bucket,
                                   ramQuotaMB=self.size,
                                   replicaNumber=self.replicas,
                                   proxyPort=self.port,
                                   authType=authType,
                                   saslPassword=self.password,
                                   bucketType=self.bucket_type,
                                   replica_index=self.enable_replica_index,
                                   flushEnabled=self.flush_enabled,
                                   evictionPolicy=self.eviction_policy,
                                   threadsNumber=self.bucket_priority,
                                   lww=self.lww,
                                   maxTTL=self.maxttl,
                                   compressionMode=self.compressionMode)
            else:
                rest.create_bucket(bucket=self.bucket,
                                   ramQuotaMB=self.size,
                                   replicaNumber=self.replicas,
                                   proxyPort=self.port,
                                   authType=authType,
                                   saslPassword=self.password,
                                   bucketType=self.bucket_type,
                                   replica_index=self.enable_replica_index,
                                   flushEnabled=self.flush_enabled,
                                   evictionPolicy=self.eviction_policy,
                                   lww=self.lww,
                                   maxTTL=self.maxttl,
                                   compressionMode=self.compressionMode)
            self.state = CHECKING
            task_manager.schedule(self)

        except BucketCreationException as e:
            self.state = FINISHED
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            if self.bucket_type == 'memcached' or int(self.port) in xrange(9091, 9991):
                self.set_result(True)
                self.state = FINISHED
                return
            if BucketOperationHelper.wait_for_memcached(self.server, self.bucket, self.alt_addr):
                self.log.info("bucket '{0}' was created with per node RAM quota: {1}".format(self.bucket, self.size))
                self.set_result(True)
                self.state = FINISHED
                return
            else:
                self.log.warn("vbucket map not ready after try {0}".format(self.retries))
                if self.retries >= 5:
                    self.set_result(False)
                    self.state = FINISHED
                    return
        except Exception as e:
            self.log.error("Unexpected error: %s" % str(e))
            self.log.warn("vbucket map not ready after try {0}".format(self.retries))
            if self.retries >= 5:
                self.state = FINISHED
                self.set_exception(e)
        self.retries = self.retries + 1
        task_manager.schedule(self)


class BucketDeleteTask(Task):
    def __init__(self, server, bucket="default"):
        Task.__init__(self, "bucket_delete_task")
        self.server = server
        self.bucket = bucket

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
            if rest.delete_bucket(self.bucket):
                self.state = CHECKING
                task_manager.schedule(self)
            else:
                self.log.info(StatsCommon.get_stats([self.server], self.bucket, "timings"))
                self.state = FINISHED
                self.set_result(False)
        # catch and set all unexpected exceptions

        except Exception as e:
            self.state = FINISHED
            self.log.info(StatsCommon.get_stats([self.server], self.bucket, "timings"))
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            rest = RestConnection(self.server)
            if BucketOperationHelper.wait_for_bucket_deletion(self.bucket, rest, 200):
                self.set_result(True)
            else:
                self.set_result(False)
            self.state = FINISHED
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info(StatsCommon.get_stats([self.server], self.bucket, "timings"))
            self.set_unexpected_exception(e)

class RebalanceTask(Task):
    def __init__(self, servers, to_add=[], to_remove=[],
                 do_stop=False, progress=30,
                 use_hostnames=False, services=None,
                 sleep_before_rebalance=None):
        Task.__init__(self, "rebalance_task")
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove
        self.start_time = None
        self.services = services
        self.monitor_vbuckets_shuffling = False
        self.sleep_before_rebalance = sleep_before_rebalance

        try:
            self.rest = RestConnection(self.servers[0])
        except ServerUnavailableException, e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)
        self.retry_get_progress = 0
        self.use_hostnames = use_hostnames
        self.previous_progress = 0
        self.old_vbuckets = {}

    def execute(self, task_manager):
        try:
            if len(self.to_add) and len(self.to_add) == len(self.to_remove):
                node_version_check = self.rest.check_node_versions()
                non_swap_servers = set(self.servers) - set(self.to_remove) - set(self.to_add)
                self.old_vbuckets = RestHelper(self.rest)._get_vbuckets(non_swap_servers, None)
                if self.old_vbuckets:
                    self.monitor_vbuckets_shuffling = True
                if self.monitor_vbuckets_shuffling and node_version_check and self.services:
                    for service_group in self.services:
                        if "kv" not in service_group:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling and node_version_check:
                    services_map = self.rest.get_nodes_services()
                    for remove_node in self.to_remove:
                         key = "{0}:{1}".format(remove_node.ip,remove_node.port)
                         services = services_map[key]
                         if "kv" not in services:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling:
                    self.log.info("This is swap rebalance and we will monitor vbuckets shuffling")
            self.add_nodes(task_manager)
            if self.sleep_before_rebalance:
                self.log.info("Sleep {0}secs before rebalance_start"
                              .format(self.sleep_before_rebalance))
                time.sleep(self.sleep_before_rebalance)
            self.start_rebalance(task_manager)
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            traceback.print_exc()
            self.set_exception(e)

    def add_nodes(self, task_manager):
        master = self.servers[0]
        services_for_node = None
        node_index = 0
        for node in self.to_add:
            self.log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            if self.services is not None:
                services_for_node = [self.services[node_index]]
                node_index += 1
            if self.use_hostnames:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.hostname, node.port, services = services_for_node)
            else:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.ip, node.port, services = services_for_node)

    def start_rebalance(self, task_manager):
        nodes = self.rest.node_statuses()

        # Determine whether its a cluster_run/not
        cluster_run = True

        firstIp = self.servers[0].ip
        if len(self.servers) == 1 and self.servers[0].port == '8091':
            cluster_run = False
        else:
            for node in self.servers:
                if node.ip != firstIp:
                    cluster_run = False
                    break
        ejectedNodes = []

        for server in self.to_remove:
            for node in nodes:
                if cluster_run:
                    if int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                else:
                    if self.use_hostnames:
                        if server.hostname == node.ip and int(server.port) == int(node.port):
                            ejectedNodes.append(node.id)
                    elif server.ip == node.ip and int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
        if self.rest.is_cluster_mixed():
            # workaround MB-8094
            self.log.warn("cluster is mixed. sleep for 15 seconds before rebalance")
            time.sleep(15)

        self.rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
        self.start_time = time.time()

    def check(self, task_manager):
        status = None
        progress = -100
        try:
            if self.monitor_vbuckets_shuffling:
                self.log.info("This is swap rebalance and we will monitor vbuckets shuffling")
                non_swap_servers = set(self.servers) - set(self.to_remove) - set(self.to_add)
                new_vbuckets = RestHelper(self.rest)._get_vbuckets(non_swap_servers, None)
                for vb_type in ["active_vb", "replica_vb"]:
                    for srv in non_swap_servers:
                        if not(len(self.old_vbuckets[srv][vb_type]) + 1 >= len(new_vbuckets[srv][vb_type]) and\
                           len(self.old_vbuckets[srv][vb_type]) - 1 <= len(new_vbuckets[srv][vb_type])):
                            msg = "Vbuckets were suffled! Expected %s for %s" % (vb_type, srv.ip) + \
                                " are %s. And now are %s" % (
                                len(self.old_vbuckets[srv][vb_type]),
                                len(new_vbuckets[srv][vb_type]))
                            self.log.error(msg)
                            self.log.error("Old vbuckets: %s, new vbuckets %s" % (self.old_vbuckets, new_vbuckets))
                            raise Exception(msg)
            (status, progress) = self.rest._rebalance_status_and_progress()
            self.log.info("Rebalance - status: {}, progress: {:.02f}%".format(status, progress))
            # if ServerUnavailableException
            if progress == -100:
                self.retry_get_progress += 1
            if self.previous_progress != progress:
                self.previous_progress = progress
                self.retry_get_progress = 0
            else:
                self.retry_get_progress += 1
        except RebalanceFailedException as ex:
            self.state = FINISHED
            self.set_exception(ex)
            self.retry_get_progress += 1
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e, " in {0} sec".format(time.time() - self.start_time))
        retry_get_process_num = 300
        if self.rest.is_cluster_mixed():
            """ for mix cluster, rebalance takes longer """
            self.log.info("rebalance in mix cluster")
            retry_get_process_num = 40
        # we need to wait for status to be 'none' (i.e. rebalance actually finished and
        # not just 'running' and at 100%) before we declare ourselves done
        if progress != -1 and status != 'none':
            if self.retry_get_progress < retry_get_process_num:
                task_manager.schedule(self, 10)
            else:
                self.state = FINISHED
                #self.set_result(False)
                self.rest.print_UI_logs()
                self.set_exception(RebalanceFailedException(\
                                "seems like rebalance hangs. please check logs!"))
        else:
            success_cleaned = []
            for removed in self.to_remove:
                try:
                    rest = RestConnection(removed)
                except ServerUnavailableException, e:
                    self.log.error(e)
                    continue
                start = time.time()
                while time.time() - start < 30:
                    try:
                        if 'pools' in rest.get_pools_info() and \
                                      (len(rest.get_pools_info()["pools"]) == 0):
                            success_cleaned.append(removed)
                            break
                        else:
                            time.sleep(0.1)
                    except (ServerUnavailableException, IncompleteRead), e:
                        self.log.error(e)
            result = True
            for node in set(self.to_remove) - set(success_cleaned):
                self.log.error("node {0}:{1} was not cleaned after removing from cluster"\
                                                              .format(node.ip, node.port))
                result = False

            self.log.info("rebalancing was completed with progress: {0}% in {1} sec".
                          format(progress, time.time() - self.start_time))
            self.state = FINISHED
            self.set_result(result)

class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, servers, bucket, param, stat, comparison, value, collection=None):
        Task.__init__(self, "stats_wait_task")
        self.servers = servers
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        self.param = param
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.conns = {}

    def execute(self, task_manager):
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        stat_result = 0
        for server in self.servers:
            try:
                shell = RemoteMachineShellConnection(server)
                cbstat = Cbstats(shell)
                stats = cbstat.all_stats(self.bucket, stat_name=self.param)
                if not stats.has_key(self.stat):
                    self.state = FINISHED
                    self.set_exception(Exception("Stat {0} not found".format(self.stat)))
                    shell.disconnect()
                    return
                if stats[self.stat].isdigit():
                    stat_result += long(stats[self.stat])
                else:
                    stat_result = stats[self.stat]
                shell.disconnect()
            except EOFError as ex:
                self.state = FINISHED
                self.set_exception(ex)
                shell.disconnect()
                return
        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.warn("Not Ready: %s %s %s %s expected on %s, %s bucket" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers(), self.bucket))
            task_manager.schedule(self, 5)
            return
        self.log.info("Saw %s %s %s %s expected on %s,%s bucket" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers(), self.bucket))

        for server, conn in self.conns.items():
            conn.close()
        self.state = FINISHED
        self.set_result(True)

    def _stringify_servers(self):
        return ''.join([`server.ip + ":" + str(server.port)` for server in self.servers])

    def _get_connection(self, server, admin_user='cbadminbucket',admin_pass='password'):
        if not self.conns.has_key(server):
            for i in xrange(3):
                try:
                    self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket, admin_user=admin_user,
                                                                             admin_pass=admin_pass)
                    return self.conns[server]
                except (EOFError, socket.error):
                    self.log.error("failed to create direct client, retry in 1 sec")
                    time.sleep(1)
            self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket, admin_user=admin_user,
                                                                     admin_pass=admin_pass)
        return self.conns[server]

    def _compare(self, cmp_type, a, b):
        if isinstance(b, (int, long)) and a.isdigit():
            a = long(a)
        elif isinstance(b, (int, long)) and not a.isdigit():
                return False
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or\
            (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or\
            (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or\
            (cmp_type == StatsWaitTask.LESS_THAN and a < b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False


class XdcrStatsWaitTask(StatsWaitTask):
    def __init__(self, servers, bucket, param, stat, comparison, value):
        StatsWaitTask.__init__(self, servers, bucket, param, stat, comparison, value)

    def check(self, task_manager):
        stat_result = 0
        for server in self.servers:
            try:
                rest = RestConnection(server)
                stat = 'replications/' + rest.get_replication_for_buckets(self.bucket, self.bucket)['id'] + '/' + self.stat
                # just get the required value, don't fetch the big big structure of stats
                stats_value = rest.fetch_bucket_xdcr_stats(self.bucket)['op']['samples'][stat][-1]
                stat_result += long(stats_value)
            except (EOFError, Exception)  as ex:
                self.state = FINISHED
                self.set_exception(ex)
                return
        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.warn("Not Ready: %s %s %s %s expected on %s, %s bucket" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers(), self.bucket))
            task_manager.schedule(self, 5)
            return
        self.log.info("Saw %s %s %s %s expected on %s,%s bucket" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers(), self.bucket))

        for server, conn in self.conns.items():
            conn.close()
        self.state = FINISHED
        self.set_result(True)

class GenericLoadingTask(Thread, Task):
    def __init__(self, server, bucket, kv_store, batch_size=1, pause_secs=1, timeout_secs=60, compression=True, collection=None):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task")

        self.kv_store=kv_store
        self.batch_size = batch_size
        self.pause = pause_secs
        self.timeout = timeout_secs
        self.server = server
        self.bucket = bucket
        self.collection=collection
        if CHECK_FLAG:
            self.client = VBucketAwareMemcached(RestConnection(server), bucket)
        else:
            self.client = VBucketAwareMemcached(RestConnection(server), bucket, compression=compression)
        self.process_concurrency = THROUGHPUT_CONCURRENCY
        # task queue's for synchronization
        process_manager = Manager()
        self.wait_queue = process_manager.Queue()
        self.shared_kvstore_queue = process_manager.Queue()

    def execute(self, task_manager):
        self.start()
        self.state = EXECUTING

    def check(self, task_manager):
        pass

    def run(self):
        while self.has_next() and not self.done():
            self.next()
        self.state = FINISHED
        self.set_result(True)

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def _unlocked_create(self, partition, key, value, is_base64_value=False):
        try:
            value_json = json.loads(value)
            if isinstance(value_json, dict):
                value_json['mutated'] = 0
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(range(len(value)))
            if not is_base64_value:
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
        except TypeError:
            value = json.dumps(value)

        try:
            self.client.set(key, self.exp, self.flag, value, collection=self.collection)

            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))
            partition.set(key, value, self.exp, self.flag)
        except Exception as error:
            self.state = FINISHED
            self.set_exception(error)


    def _unlocked_read(self, partition, key):
        try:
            o, c, d = self.client.get(key, collection=self.collection)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

    def _unlocked_replica_read(self, partition, key):
        try:
            o, c, d = self.client.getr(key, collection=self.collection)
        except Exception as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_update(self, partition, key):
        value = None
        try:
            o, c, value = self.client.get(key, collection=self.collection)
            if value is None:
                return

            value_json = json.loads(value)
            value_json['mutated'] += 1
            value = json.dumps(value_json)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                # there is no such item, we do not know what value to set
                return
            else:
                self.state = FINISHED
                self.log.error("%s, key: %s update operation." % (error, key))
                self.set_exception(error)
                return
        except ValueError:
            if value is None:
                return
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

        try:
            self.client.set(key, self.exp, self.flag, value, collection=self.collection)
            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))
            partition.set(key, value, self.exp, self.flag)
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_delete(self, partition, key):
        try:
            self.client.delete(key, collection=self.collection)
            partition.delete(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.log.error("%s, key: %s delete operation." % (error, key))
                self.set_exception(error)
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_append(self, partition, key, value):
        try:
            o, c, old_value = self.client.get(key, collection=self.collection)
            if value is None:
                return
            value_json = json.loads(value)
            old_value_json = json.loads(old_value)
            old_value_json.update(value_json)
            old_value = json.dumps(old_value_json)
            value = json.dumps(value_json)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                # there is no such item, we do not know what value to set
                return
            else:
                self.state = FINISHED
                self.set_exception(error)
                return
        except ValueError:
            o, c, old_value = self.client.get(key, collection=self.collection)
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            old_value += value
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

        try:
            self.client.append(key, value, collection=self.collection)
            if self.only_store_hash:
                old_value = str(crc32.crc32_hash(old_value))
            partition.set(key, old_value)
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)


    # start of batch methods
    def _create_batch_client(self, key_val, shared_client = None):
        """
        standalone method for creating key/values in batch (sans kvstore)

        arguments:
            key_val -- array of key/value dicts to load size = self.batch_size
            shared_client -- optional client to use for data loading
        """
        try:
            self._process_values_for_create(key_val)
            client = shared_client or self.client


            client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False, collection=self.collection)
        except (MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError, RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)

    def _create_batch(self, partition_keys_dic, key_val):
            self._create_batch_client(key_val)
            self._populate_kvstore(partition_keys_dic, key_val)

    def _update_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_update(partition_keys_dic, key_val)
            self.client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False, collection=self.collection)
            self._populate_kvstore(partition_keys_dic, key_val)
        except (MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError, RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)


    def _delete_batch(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            for key in keys:
                try:
                    self.client.delete(key, collection=self.collection)
                    partition.delete(key)
                except MemcachedError as error:
                    if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                        pass
                    else:
                        self.state = FINISHED
                        self.set_exception(error)
                        return
                except (ServerUnavailableException, socket.error, EOFError, AttributeError) as error:
                    self.state = FINISHED
                    self.set_exception(error)


    def _read_batch(self, partition_keys_dic, key_val):
        try:
            self.client.getMulti(key_val.keys(), self.pause, self.timeout, collection=self.collection)
            # print "the key is {} from collection {}".format(c, collection)
        except MemcachedError as error:
                self.state = FINISHED
                self.set_exception(error)

    def _process_values_for_create(self, key_val):
        for key, value in key_val.items():
            try:
                value_json = json.loads(value)
                value_json['mutated'] = 0
                value = json.dumps(value_json)
            except ValueError:
                index = random.choice(range(len(value)))
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            except TypeError:
                 value = json.dumps(value)
            finally:
                key_val[key] = value

    def _process_values_for_update(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            for key in keys:
                value = partition.get_valid(key)
                if value is None:
                    del key_val[key]
                    continue
                try:
                    value = key_val[key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                    value_json = json.loads(value)
                    value_json['mutated'] += 1
                    value = json.dumps(value_json)
                except ValueError:
                    index = random.choice(range(len(value)))
                    value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
                finally:
                    key_val[key] = value


    def _populate_kvstore(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            self._populate_kvstore_partition(partition, keys, key_val)

    def _release_locks_on_kvstore(self):
        for part in self._partitions_keyvals_dic.keys:
            self.kv_store.release_lock(part)

    def _populate_kvstore_partition(self, partition, keys, key_val):
        for key in keys:
            if self.only_store_hash:
                key_val[key] = str(crc32.crc32_hash(key_val[key]))
            partition.set(key, key_val[key], self.exp, self.flag)


class LoadDocumentsTask(GenericLoadingTask):

    def __init__(self, server, bucket, generator, kv_store, op_type, exp, flag=0,
                 only_store_hash=True, proxy_client=None, batch_size=1, pause_secs=1, timeout_secs=30,
                 compression=True,collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, batch_size=batch_size,pause_secs=pause_secs,
                                    timeout_secs=timeout_secs, compression=compression, collection=collection)

        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.flag = flag
        self.only_store_hash = only_store_hash
        self.collection=collection


        if proxy_client:
            self.log.info("Changing client to proxy %s:%s..." % (proxy_client.host,
                                                              proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator = None):
        if self.batch_size == 1:
            key, value = self.generator.next()
            partition = self.kv_store.acquire_partition(key,self.bucket, self.collection)
            if self.op_type == 'create':
                is_base64_value = (self.generator.__class__.__name__ == 'Base64Generator')
                self._unlocked_create(partition, key, value, is_base64_value=is_base64_value)
            elif self.op_type == 'read':
                self._unlocked_read(partition, key)
            elif self.op_type == 'read_replica':
                self._unlocked_replica_read(partition, key)
            elif self.op_type == 'update':
                self._unlocked_update(partition, key)
            elif self.op_type == 'delete':
                self._unlocked_delete(partition, key)
            elif self.op_type == 'append':
                self._unlocked_append(partition, key, value)
            else:
                self.state = FINISHED
                self.set_exception(Exception("Bad operation type: %s" % self.op_type))
            self.kv_store.release_partition(key,self.bucket, self.collection)

        else:
            doc_gen = override_generator or self.generator
            key_value = doc_gen.next_batch()

            partition_keys_dic = self.kv_store.acquire_partitions(key_value.keys(), self.bucket, self.collection)
            if self.op_type == 'create':
                self._create_batch(partition_keys_dic, key_value)
            elif self.op_type == 'update':
                self._update_batch(partition_keys_dic, key_value)
            elif self.op_type == 'delete':
                self._delete_batch(partition_keys_dic, key_value)
            elif self.op_type == 'read':
                self._read_batch(partition_keys_dic, key_value)
            else:
                self.state = FINISHED
                self.set_exception(Exception("Bad operation type: %s" % self.op_type))
            self.kv_store.release_partitions(partition_keys_dic.keys())



class LoadDocumentsGeneratorsTask(LoadDocumentsTask):
    def __init__(self, server, bucket, generators, kv_store, op_type, exp, flag=0, only_store_hash=True,
                 batch_size=1,pause_secs=1, timeout_secs=60, compression=True,collection=None):
        LoadDocumentsTask.__init__(self, server, bucket, generators[0], kv_store, op_type, exp, flag=flag,
                    only_store_hash=only_store_hash, batch_size=batch_size, pause_secs=pause_secs,
                                   timeout_secs=timeout_secs, compression=compression,collection=collection)

        if batch_size == 1:
            self.generators = generators
        else:
            self.generators = []
            for i in generators:
                self.generators.append(BatchedDocumentGenerator(i, batch_size))

        # only run high throughput for batch-create workloads
        # also check number of input generators isn't greater than
        # process_concurrency as too many generators become inefficient
        self.is_high_throughput_mode = False
        if ALLOW_HTP and not TestInputSingleton.input.param("disable_HTP", False):
            self.is_high_throughput_mode = self.op_type == "create" and \
                self.batch_size > 1 and \
                len(self.generators) < self.process_concurrency

        self.input_generators = generators
        self.bucket = bucket
        self.op_types = None
        self.buckets = None
        if isinstance(op_type, list):
            self.op_types = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        self.compression = compression
        self.collection=collection


    def run(self):
        if self.op_types:
            if len(self.op_types) != len(self.generators):
                self.state = FINISHED
                self.set_exception(Exception("not all generators have op_type!"))
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.state = FINISHED
                self.set_exception(Exception("not all generators have bucket specified!"))

        # check if running in high throughput mode or normal
        if self.is_high_throughput_mode:
            self.run_high_throughput_mode()
        else:
            self.run_normal_throughput_mode()

        self.state = FINISHED
        self.set_result(True)

    def run_normal_throughput_mode(self):
        iterator = 0
        for generator in self.generators:
            self.generator = generator
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            while self.has_next() and not self.done():
                self.next()
            iterator += 1

    def run_high_throughput_mode(self):

        # high throughput mode requires partitioning the doc generators
        self.generators = []
        for gen in self.input_generators:
            gen_start = int(gen.start)
            gen_end = max(int(gen.end), 1)
            gen_range = max(int(gen.end/self.process_concurrency), 1)
            for pos in range(gen_start, gen_end, gen_range):
                partition_gen = copy.deepcopy(gen)
                partition_gen.start = pos
                partition_gen.itr = pos
                partition_gen.end = pos+gen_range
                if partition_gen.end > gen.end:
                    partition_gen.end = gen.end
                batch_gen = BatchedDocumentGenerator(
                        partition_gen,
                        self.batch_size)
                self.generators.append(batch_gen)


        iterator = 0
        all_processes = []
        for generator in self.generators:

            # only start processing when there resources available
            CONCURRENCY_LOCK.acquire()

            generator_process = Process(
                target=self.run_generator,
                args=(generator, iterator))
            generator_process.start()
            iterator += 1
            all_processes.append(generator_process)

            # add child process to wait queue
            self.wait_queue.put(iterator)

        # wait for all child processes to finish
        self.wait_queue.join()

        # merge kvstore partitions
        while self.shared_kvstore_queue.empty() is False:

            # get partitions created by child process
            rv =  self.shared_kvstore_queue.get()
            if rv["err"] is not None:
                print(rv)
                raise Exception(rv["err"])

            # merge child partitions with parent
            generator_partitions = rv["partitions"]
            self.kv_store.merge_partitions(generator_partitions)

            # terminate child process
            iterator-=1
            all_processes[iterator].terminate()

    def run_generator(self, generator, iterator):

        tmp_kv_store = KVStore()
        rv = {"err": None, "partitions": None}

        try:

            if CHECK_FLAG:
                client = VBucketAwareMemcached(
                        RestConnection(self.server),
                        self.bucket)
            else:
                client = VBucketAwareMemcached(
                    RestConnection(self.server),
                    self.bucket, compression=self.compression)
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]

            while generator.has_next() and not self.done():

                # generate
                key_value = generator.next_batch()
                # create
                self._create_batch_client(key_value, client)

                # cache
                self.cache_items(tmp_kv_store, key_value)

        except Exception as ex:
            rv["err"] = ex
        else:
            rv["partitions"] = tmp_kv_store.get_partitions()
        finally:
            # share the kvstore from this generator
            self.shared_kvstore_queue.put(rv)
            self.wait_queue.task_done()
            # release concurrency lock
            CONCURRENCY_LOCK.release()


    def cache_items(self, store, key_value):
        """
            unpacks keys,values and adds them to provided store
        """
        for key, value in key_value.iteritems():

            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))

            partition = store.partition(key,self.collection,self.bucket)
            partition["partition"].set(
            key,
            value,
            self.exp,
            self.flag)


class ESLoadGeneratorTask(Task):
    """
        Class to load/update/delete documents into/from Elastic Search
    """

    def __init__(self, es_instance, index_name, generator, op_type="create",collection=None):
        Task.__init__(self, "ES_loader_task")
        self.es_instance = es_instance
        self.index_name = index_name
        self.generator = generator
        self.iterator = 0
        self.collection=collection
        self.log.info("Starting to load data into Elastic Search ...")

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(True)

    def execute(self, task_manager):
        for key, doc in self.generator:
            doc = json.loads(doc)
            self.es_instance.load_data(self.index_name,
                                       json.dumps(doc, encoding='utf-8'),
                                       doc['type'],
                                       key,self.collection)
            self.iterator += 1
            if math.fmod(self.iterator, 500) == 0.0:
                self.log.info("{0} documents loaded into ES".
                              format(self.iterator))
        self.state = FINISHED
        self.set_result(True)

class ESBulkLoadGeneratorTask(Task):
    """
        Class to load/update/delete documents into/from Elastic Search
    """

    def __init__(self, es_instance, index_name, generator, op_type="create",
                 batch=1000,collection=None):
        Task.__init__(self, "ES_loader_task")
        self.es_instance = es_instance
        self.index_name = index_name
        self.generator = generator
        self.iterator = 0
        self.op_type = op_type
        self.batch_size = batch
        self.collection=collection
        self.log.info("Starting operation '%s' on Elastic Search ..." % op_type)

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(True)

    def execute(self, task_manager):
        es_filename = "/tmp/es_bulk.txt"
        es_bulk_docs = []
        loaded = 0
        batched = 0
        for key, doc in self.generator:
            doc = json.loads(doc)
            es_doc = {
                self.op_type: {
                    "_index": self.index_name,
                    "_type": doc['type'],
                    "_id": key,
                }
            }
            es_bulk_docs.append(json.dumps(es_doc))
            if self.op_type == "create":
                es_bulk_docs.append(json.dumps(doc))
            elif self.op_type == "update":
                doc['mutated'] += 1
                es_bulk_docs.append(json.dumps({"doc": doc}))
            batched += 1
            if batched == self.batch_size or not self.generator.has_next():
                es_file = open(es_filename, "wb")
                for line in es_bulk_docs:
                    es_file.write("%s\n" %line)
                es_file.close()
                self.es_instance.load_bulk_data(es_filename)
                loaded += batched
                self.log.info("{0} documents bulk loaded into ES".format(loaded))
                self.es_instance.update_index(self.index_name)
                batched = 0
        indexed = self.es_instance.get_index_count(self.index_name)
        self.log.info("ES index count for '{0}': {1}".
                              format(self.index_name, indexed))
        self.state = FINISHED
        self.set_result(True)


class ESRunQueryCompare(Task):
    def __init__(self, fts_index, es_instance, query_index, es_index_name=None, n1ql_executor=None):
        Task.__init__(self, "Query_runner_task")
        self.fts_index = fts_index
        self.fts_query = fts_index.fts_queries[query_index]
        self.es = es_instance
        if self.es:
            self.es_query = es_instance.es_queries[query_index]
        self.max_verify = None
        self.show_results = False
        self.query_index = query_index
        self.passed = True
        self.es_index_name = es_index_name or "es_index"
        self.n1ql_executor = n1ql_executor
        self.score = TestInputSingleton.input.param("score",'')

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(self.result)

    def execute(self, task_manager):
        self.es_compare = True
        should_verify_n1ql = True
        try:
            self.log.info("---------------------------------------"
                          "-------------- Query # %s -------------"
                          "---------------------------------------"
                          % str(self.query_index+1))
            try:
                fts_hits, fts_doc_ids, fts_time, fts_status = \
                    self.run_fts_query(self.fts_query, self.score)
                self.log.info("Status: %s" % fts_status)
                if fts_hits < 0:
                    self.passed = False
                elif 'errors' in fts_status.keys() and fts_status['errors']:
                        if fts_status['successful'] == 0 and \
                                (list(set(fts_status['errors'].values())) ==
                                    [u'context deadline exceeded'] or
                                "TooManyClauses" in str(list(set(fts_status['errors'].values())))):
                            # too many clauses in the query for fts to process
                            self.log.info("FTS chose not to run this big query"
                                          "...skipping ES validation")
                            self.passed = True
                            self.es_compare = False
                        elif 0 < fts_status['successful'] < \
                                self.fts_index.num_pindexes:
                            # partial results
                            self.log.info("FTS returned partial results..."
                                          "skipping ES validation")
                            self.passed = True
                            self.es_compare = False
                self.log.info("FTS hits for query: %s is %s (took %sms)" % \
                        (json.dumps(self.fts_query, ensure_ascii=False),
                        fts_hits,
                        float(fts_time)/1000000))
            except ServerUnavailableException:
                self.log.error("ERROR: FTS Query timed out (client timeout=70s)!")
                self.passed = False
            es_hits = 0
            if self.es and self.es_query:
                es_hits, es_doc_ids, es_time = self.run_es_query(self.es_query)
                self.log.info("ES hits for query: %s on %s is %s (took %sms)" % \
                              (json.dumps(self.es_query,  ensure_ascii=False),
                               self.es_index_name,
                               es_hits,
                               es_time))
                if self.passed and self.es_compare:
                    if int(es_hits) != int(fts_hits):
                        msg = "FAIL: FTS hits: %s, while ES hits: %s"\
                              % (fts_hits, es_hits)
                        self.log.error(msg)
                    es_but_not_fts = list(set(es_doc_ids) - set(fts_doc_ids))
                    fts_but_not_es = list(set(fts_doc_ids) - set(es_doc_ids))
                    if not (es_but_not_fts or fts_but_not_es):
                        self.log.info("SUCCESS: Docs returned by FTS = docs"
                                      " returned by ES, doc_ids verified")
                    else:
                        if fts_but_not_es:
                            msg = "FAIL: Following %s doc(s) were not returned" \
                                  " by ES,but FTS, printing 50: %s" \
                                  % (len(fts_but_not_es), fts_but_not_es[:50])
                        else:
                            msg = "FAIL: Following %s docs were not returned" \
                                  " by FTS, but ES, printing 50: %s" \
                                  % (len(es_but_not_fts), es_but_not_fts[:50])
                        self.log.error(msg)
                        self.passed = False

            if fts_hits <= 0 and es_hits==0:
                should_verify_n1ql = False

            if self.n1ql_executor and should_verify_n1ql:
                if self.fts_index.dataset == 'all':
                    query_type = 'emp'
                    if int(TestInputSingleton.input.param("doc_maps", 1)) > 1:
                        query_type = 'wiki'
                    wiki_fields = ["revision.text", "title"]
                    if any(field in str(json.dumps(self.fts_query)) for field in wiki_fields):
                        query_type = 'wiki'
                else:
                    query_type = self.fts_index.dataset
                geo_strings = ['"field": "geo"']
                if any(geo_str in str(json.dumps(self.fts_query)) for geo_str in geo_strings):
                    query_type = 'earthquake'

                n1ql_query = "select meta().id from default where type='" + str(query_type) + "' and search(default, " + str(
                    json.dumps(self.fts_query)) + ")"
                self.log.info("Running N1QL query: "+str(n1ql_query))
                n1ql_result = self.n1ql_executor.run_n1ql_query(query=n1ql_query)
                if n1ql_result['status'] == 'success':
                    n1ql_hits = n1ql_result['metrics']['resultCount']
                    n1ql_doc_ids = []
                    for res in n1ql_result['results']:
                        n1ql_doc_ids.append(res['id'])
                    n1ql_time = n1ql_result['metrics']['elapsedTime']

                    self.log.info("N1QL hits for query: %s is %s (took %s)" % \
                                  (json.dumps(n1ql_query,  ensure_ascii=False),
                                   n1ql_hits,
                                   n1ql_time))
                    if self.passed:
                        if int(n1ql_hits) != int(fts_hits):
                            msg = "FAIL: FTS hits: %s, while N1QL hits: %s"\
                                  % (fts_hits, n1ql_hits)
                            self.log.error(msg)
                        n1ql_but_not_fts = list(set(n1ql_doc_ids) - set(fts_doc_ids))
                        fts_but_not_n1ql = list(set(fts_doc_ids) - set(n1ql_doc_ids))
                        if not (n1ql_but_not_fts or fts_but_not_n1ql):
                            self.log.info("SUCCESS: Docs returned by FTS = docs"
                                          " returned by N1QL, doc_ids verified")
                        else:
                            if fts_but_not_n1ql:
                                msg = "FAIL: Following %s doc(s) were not returned" \
                                          " by N1QL,but FTS, printing 50: %s" \
                                        % (len(fts_but_not_n1ql), fts_but_not_n1ql[:50])
                            else:
                                msg = "FAIL: Following %s docs were not returned" \
                                          " by FTS, but N1QL, printing 50: %s" \
                                        % (len(n1ql_but_not_fts), n1ql_but_not_fts[:50])
                            self.log.error(msg)
                            self.passed = False
                else:
                    self.passed = False
                    self.log.info("N1QL query execution is failed.")
                    self.log.error(n1ql_result["errors"][0]['msg'])
            self.state = CHECKING
            task_manager.schedule(self)

            if not should_verify_n1ql and self.n1ql_executor:
                self.log.info("Skipping N1QL result validation since FTS results are - "+str(fts_hits)+" and es results are - "+str(es_hits)+".")

        except Exception as e:
            self.log.error(e)
            self.set_exception(e)
            self.state = FINISHED

    def run_fts_query(self, query, score=''):
        return self.fts_index.execute_query(query, score=score)

    def run_es_query(self, query):
        return self.es.search(index_name=self.es_index_name, query=query)


# This will be obsolete with the implementation of batch operations in LoadDocumentsTaks
class BatchedLoadDocumentsTask(GenericLoadingTask):
    def __init__(self, server, bucket, generator, kv_store, op_type, exp, flag=0, only_store_hash=True,
                 batch_size=100, pause_secs=1, timeout_secs=60, compression=True,collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression, collection=collection)
        self.batch_generator = BatchedDocumentGenerator(generator, batch_size)
        self.op_type = op_type
        self.exp = exp
        self.flag = flag
        self.only_store_hash = only_store_hash
        self.batch_size = batch_size
        self.pause = pause_secs
        self.timeout = timeout_secs
        self.bucket = bucket
        self.server = server
        self.collection=collection

    def has_next(self):
        has = self.batch_generator.has_next()
        if math.fmod(self.batch_generator._doc_gen.itr, 50000) == 0.0 or not has:
            self.log.info("Batch {0} documents queued #: {1} with exp:{2} @ {3}, bucket {4}".\
                          format(self.op_type,
                                 (self.batch_generator._doc_gen.itr - self.batch_generator._doc_gen.start),
                                 self.exp,
                                 self.server.ip,
                                 self.bucket))
        return has

    def next(self):
        key_value = self.batch_generator.next_batch()
        partition_keys_dic = self.kv_store.acquire_partitions(key_value.keys(),self.bucket, self.collection)
        if self.op_type == 'create':
            self._create_batch(partition_keys_dic, key_value)
        elif self.op_type == 'update':
            self._update_batch(partition_keys_dic, key_value)
        elif self.op_type == 'delete':
            self._delete_batch(partition_keys_dic, key_value)
        elif self.op_type == 'read':
            self._read_batch(partition_keys_dic, key_value)
        else:
            self.state = FINISHED
            self.set_exception(Exception("Bad operation type: %s" % self.op_type))
        self.kv_store.release_partitions(partition_keys_dic.keys(), self.collection)

    def _create_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_create(key_val)
            self.client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False, collection=self.collection)
            self._populate_kvstore(partition_keys_dic, key_val)
        except (MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError, RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)


    def _update_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_update(partition_keys_dic, key_val)
            self.client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False, collection=self.collection)
            self._populate_kvstore(partition_keys_dic, key_val)
        except (MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError, RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)


    def _delete_batch(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            for key in keys:
                try:
                    self.client.delete(key, collection=self.collection)
                    partition.delete(key)
                except MemcachedError as error:
                    if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                        pass
                    else:
                        self.state = FINISHED
                        self.set_exception(error)
                        return
                except (ServerUnavailableException, socket.error, EOFError, AttributeError) as error:
                    self.state = FINISHED
                    self.set_exception(error)


    def _read_batch(self, partition_keys_dic, key_val):
        try:
            self.client.getMulti(key_val.keys(), self.pause, self.timeout, collection=self.collection)
        except MemcachedError as error:
                self.state = FINISHED
                self.set_exception(error)

    def _process_values_for_create(self, key_val):
        for key, value in key_val.items():
            try:
                value_json = json.loads(value)
                value_json['mutated'] = 0
                value = json.dumps(value_json)
            except ValueError:
                index = random.choice(range(len(value)))
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            finally:
                key_val[key] = value

    def _process_values_for_update(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            for key in keys:
                value = partition.get_valid(key)
                if value is None:
                    del key_val[key]
                    continue
                try:
                    value = key_val[key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                    value_json = json.loads(value)
                    value_json['mutated'] += 1
                    value = json.dumps(value_json)
                except ValueError:
                    index = random.choice(range(len(value)))
                    value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
                finally:
                    key_val[key] = value


    def _populate_kvstore(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            self._populate_kvstore_partition(partition, keys, key_val)

    def _release_locks_on_kvstore(self):
        for part in self._partitions_keyvals_dic.keys:
            self.kv_store.release_lock(part)

    def _populate_kvstore_partition(self, partition, keys, key_val):
        for key in keys:
            if self.only_store_hash:
                key_val[key] = str(crc32.crc32_hash(key_val[key]))
            partition.set(key, key_val[key], self.exp, self.flag)



class WorkloadTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, num_ops, create, read, update, delete, exp, compression=True, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression, collection=collection)
        self.itr = 0
        self.num_ops = num_ops
        self.create = create
        self.read = create + read
        self.update = create + read + update
        self.delete = create + read + update + delete
        self.exp = exp
        self.collection=collection
        self.bucket=bucket

    def has_next(self):
        if self.num_ops == 0 or self.itr < self.num_ops:
            return True
        return False

    def next(self):
        self.itr += 1
        rand = random.randint(1, self.delete)
        if rand > 0 and rand <= self.create:
            self._create_random_key()
        elif rand > self.create and rand <= self.read:
            self._get_random_key()
        elif rand > self.read and rand <= self.update:
            self._update_random_key()
        elif rand > self.update and rand <= self.delete:
            self._delete_random_key()

    def _get_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partitions(part_num)
            return

        self._unlocked_read(partition, key)
        self.kv_store.release_partitions(part_num)

    def _create_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition(False)
        if partition is None:
            return

        key = partition.get_random_deleted_key()
        if key is None:
            self.kv_store.release_partitions(part_num)
            return

        value = partition.get_deleted(key)
        if value is None:
            self.kv_store.release_partitions(part_num)
            return

        self._unlocked_create(partition, key, value)
        self.kv_store.release_partitions(part_num)

    def _update_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partitions(part_num)
            return

        self._unlocked_update(partition, key)
        self.kv_store.release_partitions(part_num)

    def _delete_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partitions(part_num)
            return

        self._unlocked_delete(partition, key)
        self.kv_store.release_partitions(part_num)

class ValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, only_store_hash=True, replica_to_read=None,
                 compression=True,collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression,collection=collection)
        self.collection=collection
        self.bucket=bucket
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        self.only_store_hash = only_store_hash
        self.replica_to_read = replica_to_read
        self.bucket=bucket
        self.server=server
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        self.log.info("%s items will be verified on %s bucket on collection %s" % (self.max_verify, bucket, self.collection))
        self.start_time = time.time()

    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and\
            self.itr < self.max_verify:
            if not self.itr % 50000:
                self.log.info("{0} items were verified".format(self.itr))
            return True
        self.log.info("{0} items were verified in {1} sec.the average number of ops\
            - {2} per second ".format(self.itr, time.time() - self.start_time,
                self.itr / (time.time() - self.start_time)).rstrip())
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr],self.bucket, self.collection)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys],self.bucket, self.collection)
        self.itr += 1

    def _check_valid_key(self, key, bucket="default", collection=None):
        partition = self.kv_store.acquire_partition(key,bucket, collection=collection)

        value = partition.get_valid(key)
        flag = partition.get_flag(key)
        if value is None or flag is None:
            self.kv_store.release_partition(key,bucket, collection=collection)
            return

        try:
            if self.replica_to_read is None:

                o, c, d = self.client.get(key, collection=collection)
            else:
                o, c, d = self.client.getr(key, replica_index=self.replica_to_read, collection=collection)
            if self.only_store_hash:
                if crc32.crc32_hash(d) != int(value):
                    self.state = FINISHED
                    self.set_exception(Exception('Key: %s, Bad hash result: %d != %d for key %s' % (key, crc32.crc32_hash(d), int(value), key)))
            else:
                value = json.dumps(value)
                if d != json.loads(value):
                    print "the collection is {} for which the value is failing".format(collection)
                    self.state = FINISHED
                    self.set_exception(Exception('Key: %s, Bad result: %s != %s for key %s' % (key, json.dumps(d), value, key)))
            if CHECK_FLAG and o != flag:
                self.state = FINISHED
                self.set_exception(Exception('Key: %s, Bad result for flag value: %s != the value we set: %s' % (key, o, flag)))

        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        except Exception as error:
            self.log.error("Unexpected error: %s" % str(error))
            self.state = FINISHED
            self.set_exception(error)
        self.kv_store.release_partition(key,bucket, collection=collection)

    def _check_deleted_key(self, key,bucket="deafult", collection=None):
        partition = self.kv_store.acquire_partition(key,bucket, collection=collection)

        try:
            self.client.delete(key, collection=collection)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        except Exception as error:
            if error.rc != NotFoundError:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key,bucket, collection=collection)

class ValidateDataWithActiveAndReplicaTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, compression=True, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression, collection=collection)
        self.colllection=collection
        self.bucket=bucket
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        self.log.info("%s items will be verified on %s bucket" % (self.max_verify, bucket))
        self.start_time = time.time()



    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and\
            self.itr < self.max_verify:
            if not self.itr % 50000:
                self.log.info("{0} items were verified".format(self.itr))
            return True
        self.log.info("{0} items were verified in {1} sec.the average number of ops\
            - {2} per second ".format(self.itr, time.time() - self.start_time,
                self.itr / (time.time() - self.start_time)).rstrip())
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr],self.bucket,self.collection)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys],self.bucket,self.collection)
        self.itr += 1

    def _check_valid_key(self, key,bucket, collection=None):
        partition = self.kv_store.acquire_partition(key,bucket, collection=collection)
        try:
            o, c, d = self.client.get(key, collection=collection)
            o_r, c_r, d_r = self.client.getr(key, replica_index=0, collection=collection)
            if o != o_r:
                self.state = FINISHED
                self.set_exception(Exception('ACTIVE AND REPLICA FLAG CHECK FAILED :: Key: %s, Bad result for CAS value: REPLICA FLAG %s != ACTIVE FLAG %s' % (key, o_r, o)))
            if c != c_r:
                self.state = FINISHED
                self.set_exception(Exception('ACTIVE AND REPLICA CAS CHECK FAILED :: Key: %s, Bad result for CAS value: REPLICA CAS %s != ACTIVE CAS %s' % (key, c_r, c)))
            if d != d_r:
                self.state = FINISHED
                self.set_exception(Exception('ACTIVE AND REPLICA VALUE CHECK FAILED :: Key: %s, Bad result for Value value: REPLICA VALUE %s != ACTIVE VALUE %s' % (key, d_r, d)))

        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        except Exception as error:
            self.log.error("Unexpected error: %s" % str(error))
            self.state = FINISHED
            self.set_exception(error)

    def _check_deleted_key(self, key,bucket, collection=None):
        partition = self.kv_store.acquire_partition(key,bucket, collection=collection)
        try:
            self.client.delete(key, collection=collection)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('ACTIVE CHECK :: Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        except Exception as error:
            if error.rc != NotFoundError:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key,bucket, collection=collection)

class BatchedValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, only_store_hash=True, batch_size=100,
                 timeout_sec=5, compression=True, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression, collection=collection)
        self.collection=collection
        self.bucket=bucket
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        self.timeout_sec = timeout_sec
        self.only_store_hash = only_store_hash
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        self.log.info("%s items will be verified on %s bucket" % (self.max_verify, bucket))
        self.batch_size = batch_size
        self.start_time = time.time()


    def has_next(self):
        has = False
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and self.itr < self.max_verify:
            has = True
        if math.fmod(self.itr, 10000) == 0.0:
                self.log.info("{0} items were verified".format(self.itr))
        if not has:
            self.log.info("{0} items were verified in {1} sec.the average number of ops\
                - {2} per second".format(self.itr, time.time() - self.start_time,
                self.itr / (time.time() - self.start_time)).rstrip())
        return has

    def next(self):
        if self.itr < self.num_valid_keys:
            keys_batch = self.valid_keys[self.itr:self.itr + self.batch_size]
            self.itr += len(keys_batch)
            self._check_valid_keys(keys_batch,self.bucket,self.collection)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys],self.bucket,self.collection)
            self.itr += 1

    def _check_valid_keys(self, keys,bucket, collection=None):
        partition_keys_dic = self.kv_store.acquire_partitions(keys, bucket, collection=collection)
        try:
            key_vals = self.client.getMulti(keys, parallel=True, timeout_sec=self.timeout_sec, collection=collection)
        except ValueError, error:
            self.log.error("Read failed via memcached client. Error: %s"%str(error))
            self.state = FINISHED
            self.kv_store.release_partitions(partition_keys_dic.keys())
            self.set_exception(error)
            return
        except BaseException, error:
        # handle all other exception, for instance concurrent.futures._base.TimeoutError
            self.log.error("Read failed via memcached client. Error: %s"%str(error))
            self.state = FINISHED
            self.kv_store.release_partitions(partition_keys_dic.keys())
            self.set_exception(error)
            return
        for partition, keys in partition_keys_dic.items():
            self._check_validity(partition, keys, key_vals)
        self.kv_store.release_partitions(partition_keys_dic.keys())

    def _check_validity(self, partition, keys, key_vals):

        for key in keys:
            value = partition.get_valid(key)
            flag = partition.get_flag(key)
            if value is None:
                continue
            try:
                o, c, d = key_vals[key]

                if self.only_store_hash:
                    if crc32.crc32_hash(d) != int(value):
                        self.state = FINISHED
                        self.set_exception(Exception('Key: %s Bad hash result: %d != %d' % (key, crc32.crc32_hash(d), int(value))))
                else:
                    #value = json.dumps(value)
                    if json.loads(d) != json.loads(value):
                        self.state = FINISHED
                        self.set_exception(Exception('Key: %s Bad result: %s != %s' % (key, json.dumps(d), value)))
                if CHECK_FLAG and o != flag:
                    self.state = FINISHED
                    self.set_exception(Exception('Key: %s Bad result for flag value: %s != the value we set: %s' % (key, o, flag)))
            except KeyError as error:
                self.state = FINISHED
                self.set_exception(error)

    def _check_deleted_key(self, key,bucket, collection=None):
        partition = self.kv_store.acquire_partition(key,bucket, collection=collection)
        try:
            self.client.delete(key, collection=collection)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.kv_store.release_partition(key,bucket, collection=collection)
                self.set_exception(error)
        except Exception as error:
            if error.rc != NotFoundError:
                self.state = FINISHED
                self.kv_store.release_partition(key,bucket, collection=collection)
                self.set_exception(error)
        self.kv_store.release_partition(key,bucket, collection=collection)


class VerifyRevIdTask(GenericLoadingTask):
    def __init__(self, src_server, dest_server, bucket, src_kv_store, dest_kv_store, max_err_count=200000,
                 max_verify=None, compression=True, collection=None):
        GenericLoadingTask.__init__(self, src_server, bucket, src_kv_store, compression=compression, collection=collection)
        from memcached.helper.data_helper import VBucketAwareMemcached as SmartClient
        self.collection=collection
        self.client_src = SmartClient(RestConnection(src_server), bucket)
        self.client_dest = SmartClient(RestConnection(dest_server), bucket)
        self.src_valid_keys, self.src_deleted_keys = src_kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.dest_valid_keys, self.dest_del_keys = dest_kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.num_valid_keys = len(self.src_valid_keys)
        self.num_deleted_keys = len(self.src_deleted_keys)
        self.keys_not_found = {self.client.rest.ip: [], self.client_dest.rest.ip: []}
        if max_verify:
            self.max_verify = max_verify
        else:
            self.max_verify = self.num_valid_keys + self.num_deleted_keys
        self.itr = 0
        self.not_matching_filter_keys = 0
        self.err_count = 0
        self.max_err_count = max_err_count
        self.src_server = src_server
        self.bucket = bucket
        self.log.info("RevID verification: in progress for %s ..." % bucket.name)


    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and \
                        self.err_count < self.max_err_count and \
                        self.itr < self.max_verify:
            return True
        self.log.info("RevId Verification : {0} existing items have been verified"
                      .format(self.itr if self.itr < self.num_valid_keys else self.num_valid_keys))
        self.log.info("RevId Verification : {0} deleted items have been verified"
                      .format(self.itr - self.num_valid_keys if self.itr > self.num_valid_keys else 0))
        self.log.info("RevId Verification : {0} keys were apparently filtered "
                      "out and not found in target bucket"
                      .format(self.not_matching_filter_keys))

        # if there are missing keys, we would have printed them by now
        # check if excess keys are present on server, if yes, set an exception
        # TODO : print excess keys
        server = RestConnection(self.src_server)
        server_count = server.fetch_bucket_stats(bucket=self.bucket.name)["op"]["samples"]["curr_items"][-1]
        if server_count > self.num_valid_keys:
            self.set_exception(Exception("ERROR: {0} keys present on bucket {1} "
                                        "on {2} while kvstore expects only {3}"
                                        .format(server_count, self.bucket.name,
                                         self.src_server.ip, self.num_valid_keys)))
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_key_revId(self.src_valid_keys[self.itr], collection=self.collection)
        elif self.itr < (self.num_valid_keys + self.num_deleted_keys):
            # verify deleted/expired keys
            self._check_key_revId(self.src_deleted_keys[self.itr - self.num_valid_keys],
                                  ignore_meta_data=['expiration','cas'], collection=self.collection)
        self.itr += 1

        # show progress of verification for every 50k items
        if math.fmod(self.itr, 50000) == 0.0:
            self.log.info("{0} items have been verified".format(self.itr))


    def __get_meta_data(self, client, key, collection=None):
        try:
            mc = client.memcached(key)
            meta_data = eval("{'deleted': %s, 'flags': %s, 'expiration': %s, 'seqno': %s, 'cas': %s}" % (mc.getMeta(key, collection=collection)))
            return meta_data
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                # if a filter was specified, the key will not be found in
                # target kv store if key did not match filter expression
                if key not in self.src_deleted_keys and key in (self.dest_valid_keys+self.dest_del_keys):
                    self.err_count += 1
                    self.keys_not_found[client.rest.ip].append(("key: %s" % key, "vbucket: %s" % client._get_vBucket_id(key, collection=collection)))
                else:
                    self.not_matching_filter_keys +=1
            else:
                self.state = FINISHED
                self.set_exception(error)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _check_key_revId(self, key, ignore_meta_data=[], collection=None):
        src_meta_data = self.__get_meta_data(self.client_src, key, collection=collection)
        dest_meta_data = self.__get_meta_data(self.client_dest, key, collection=collection)
        if not src_meta_data or not dest_meta_data:
            return
        prev_error_count = self.err_count
        err_msg = []
        # seqno number should never be zero
        if src_meta_data['seqno'] == 0:
            self.err_count += 1
            err_msg.append(
                "seqno on Source should not be 0, Error Count:{0}".format(self.err_count))

        if dest_meta_data['seqno'] == 0:
            self.err_count += 1
            err_msg.append(
                "seqno on Destination should not be 0, Error Count:{0}".format(self.err_count))

        # verify all metadata
        for meta_key in src_meta_data.keys():
            check = True
            if meta_key == 'flags' and not CHECK_FLAG:
                check = False
            if check and src_meta_data[meta_key] != dest_meta_data[meta_key] and meta_key not in ignore_meta_data:
                self.err_count += 1
                err_msg.append("{0} mismatch: Source {0}:{1}, Destination {0}:{2}, Error Count:{3}"
                    .format(meta_key, src_meta_data[meta_key],
                        dest_meta_data[meta_key], self.err_count))

        if self.err_count - prev_error_count > 0 and self.err_count < 200:
            self.log.error("===== Verifying rev_ids failed for key: {0}, bucket:{1} =====".format(key, self.bucket))
            [self.log.error(err) for err in err_msg]
            self.log.error("Source meta data: %s" % src_meta_data)
            self.log.error("Dest meta data: %s" % dest_meta_data)
            self.state = FINISHED

class VerifyMetaDataTask(GenericLoadingTask):
    def __init__(self, dest_server, bucket, kv_store, meta_data_store, max_err_count=100, compression=True, collection=None):
        GenericLoadingTask.__init__(self, dest_server, bucket, kv_store, compression=compression, collection=collection)
        from memcached.helper.data_helper import VBucketAwareMemcached as SmartClient
        self.collections=collection
        self.client = SmartClient(RestConnection(dest_server), bucket)
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.keys_not_found = {self.client.rest.ip: [], self.client.rest.ip: []}
        self.itr = 0
        self.err_count = 0
        self.max_err_count = max_err_count
        self.meta_data_store = meta_data_store


    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and self.err_count < self.max_err_count:
            return True
        self.log.info("Meta Data Verification : {0} existing items have been verified"
                      .format(self.itr if self.itr < self.num_valid_keys else self.num_valid_keys))
        self.log.info("Meta Data Verification : {0} deleted items have been verified"
                      .format(self.itr - self.num_valid_keys if self.itr > self.num_valid_keys else 0))
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_key_meta_data(self.valid_keys[self.itr], self.collections)
        elif self.itr < (self.num_valid_keys + self.num_deleted_keys):
            # verify deleted/expired keys
            self._check_key_meta_data(self.deleted_keys[self.itr - self.num_valid_keys],
                                  ignore_meta_data=['expiration'], collection=self.collections)
        self.itr += 1

        # show progress of verification for every 50k items
        if math.fmod(self.itr, 50000) == 0.0:
            self.log.info("{0} items have been verified".format(self.itr))

    def __get_meta_data(self, client, key, collection=None):
        try:
            mc = client.memcached(key)
            meta_data = eval("{'deleted': %s, 'flags': %s, 'expiration': %s, 'seqno': %s, 'cas': %s}" % (mc.getMeta(key, collection=collection)))
            return meta_data
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                if key not in self.deleted_keys:
                    self.err_count += 1
                    self.keys_not_found[client.rest.ip].append(("key: %s" % key, "vbucket: %s" % client._get_vBucket_id(key)))
            else:
                self.state = FINISHED
                self.set_exception(error)

    def _check_key_meta_data(self, key, ignore_meta_data=[], collection=None):
        src_meta_data = self.meta_data_store[key]
        dest_meta_data = self.__get_meta_data(self.client, key, collection=collection)
        if not src_meta_data or not dest_meta_data:
            return
        prev_error_count = self.err_count
        err_msg = []
        # seqno number should never be zero
        if dest_meta_data['seqno'] == 0:
            self.err_count += 1
            err_msg.append(
                "seqno on Destination should not be 0, Error Count:{0}".format(self.err_count))

        # verify all metadata
        for meta_key in src_meta_data.keys():
            if src_meta_data[meta_key] != dest_meta_data[meta_key] and meta_key not in ignore_meta_data:
                self.err_count += 1
                err_msg.append("{0} mismatch: Source {0}:{1}, Destination {0}:{2}, Error Count:{3}"
                    .format(meta_key, src_meta_data[meta_key],
                        dest_meta_data[meta_key], self.err_count))

        if self.err_count - prev_error_count > 0:
            self.log.error("===== Verifying meta data failed for key: {0} =====".format(key))
            [self.log.error(err) for err in err_msg]
            self.log.error("Source meta data: %s" % src_meta_data)
            self.log.error("Dest meta data: %s" % dest_meta_data)
            self.state = FINISHED

class GetMetaDataTask(GenericLoadingTask):
    def __init__(self, dest_server, bucket, kv_store, compression=True, collection=None):
        GenericLoadingTask.__init__(self, dest_server, bucket, kv_store, compression=compression, collection=collection)
        from memcached.helper.data_helper import VBucketAwareMemcached as SmartClient
        self.collection=collection
        self.client = SmartClient(RestConnection(dest_server), bucket)
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket,collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.keys_not_found = {self.client.rest.ip: [], self.client.rest.ip: []}
        self.itr = 0
        self.err_count = 0
        self.max_err_count = 100
        self.meta_data_store = {}


    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and self.err_count < self.max_err_count:
            return True
        self.log.info("Get Meta Data : {0} existing items have been gathered"
                      .format(self.itr if self.itr < self.num_valid_keys else self.num_valid_keys))
        self.log.info("Get Meta Data : {0} deleted items have been gathered"
                      .format(self.itr - self.num_valid_keys if self.itr > self.num_valid_keys else 0))
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self.meta_data_store[self.valid_keys[self.itr]] = self.__get_meta_data(self.client,self.valid_keys[self.itr], self.collection)
        elif self.itr < (self.num_valid_keys + self.num_deleted_keys):
            self.meta_data_store[self.deleted_keys[self.itr - self.num_valid_keys]] = self.__get_meta_data(self.client,self.deleted_keys[self.itr - self.num_valid_keys], collection=self.collection)
        self.itr += 1

    def __get_meta_data(self, client, key, collection=None):
        try:
            mc = client.memcached(key)
            meta_data = eval("{'deleted': %s, 'flags': %s, 'expiration': %s, 'seqno': %s, 'cas': %s}" % (mc.getMeta(key, collection=collection)))
            return meta_data
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                if key not in self.deleted_keys:
                    self.err_count += 1
                    self.keys_not_found[client.rest.ip].append(("key: %s" % key, "vbucket: %s" % client._get_vBucket_id(key)))
            else:
                self.state = FINISHED
                self.set_exception(error)

    def get_meta_data_store(self):
        return self.meta_data_store

class ViewCreateTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default", with_query=True,
                 check_replication=False, ddoc_options=None):
        Task.__init__(self, "create_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ""
        if self.view:
            prefix = ("", "dev_")[self.view.dev_view]
        if design_doc_name.find('/') != -1:
            design_doc_name = design_doc_name.replace('/', '%2f')
        self.design_doc_name = prefix + design_doc_name
        self.ddoc_rev_no = 0
        self.with_query = with_query
        self.check_replication = check_replication
        self.ddoc_options = ddoc_options
        self.rest = RestConnection(self.server)

    def execute(self, task_manager):

        try:
            # appending view to existing design doc
            content, meta = self.rest.get_ddoc(self.bucket, self.design_doc_name)
            ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
            # if view is to be updated
            if self.view:
                if self.view.is_spatial:
                    ddoc.add_spatial_view(self.view)
                else:
                    ddoc.add_view(self.view)
            self.ddoc_rev_no = self._parse_revision(meta['rev'])
        except ReadDocumentException:
            # creating first view in design doc
            if self.view:
                if self.view.is_spatial:
                    ddoc = DesignDocument(self.design_doc_name, [], spatial_views=[self.view])
                else:
                    ddoc = DesignDocument(self.design_doc_name, [self.view])
            # create an empty design doc
            else:
                ddoc = DesignDocument(self.design_doc_name, [])
            if self.ddoc_options:
                ddoc.options = self.ddoc_options
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

        try:
            self.rest.create_design_document(self.bucket, ddoc)
            self.state = CHECKING
            task_manager.schedule(self)

        except DesignDocCreationException as e:
            self.state = FINISHED
            self.set_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            # only query if the DDoc has a view
            if self.view:
                if self.with_query:
                    query = {"stale" : "ok"}
                    if self.view.is_spatial:
                        content = \
                            self.rest.query_view(self.design_doc_name, self.view.name,
                                                 self.bucket, query, type="spatial")
                    else:
                        content = \
                            self.rest.query_view(self.design_doc_name, self.view.name,
                                                 self.bucket, query)
                else:
                     _, json_parsed, _ = self.rest._get_design_doc(self.bucket, self.design_doc_name)
                     if self.view.is_spatial:
                         if self.view.name not in json_parsed["spatial"].keys():
                             self.set_exception(
                                Exception("design doc {O} doesn't contain spatial view {1}".format(
                                self.design_doc_name, self.view.name)))
                     else:
                         if self.view.name not in json_parsed["views"].keys():
                             self.set_exception(Exception("design doc {O} doesn't contain view {1}".format(
                                self.design_doc_name, self.view.name)))
                self.log.info("view : {0} was created successfully in ddoc: {1}".format(self.view.name, self.design_doc_name))
            else:
                # if we have reached here, it means design doc was successfully updated
                self.log.info("Design Document : {0} was updated successfully".format(self.design_doc_name))

            self.state = FINISHED
            if self._check_ddoc_revision():
                self.set_result(self.ddoc_rev_no)
            else:
                self.set_exception(Exception("failed to update design document"))

            if self.check_replication:
                self._check_ddoc_replication_on_nodes()

        except QueryViewException as e:
            if e.message.find('not_found') or e.message.find('view_undefined') > -1:
                task_manager.schedule(self, 2)
            else:
                self.state = FINISHED
                self.set_unexpected_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _check_ddoc_revision(self):
        valid = False
        try:
            content, meta = self.rest.get_ddoc(self.bucket, self.design_doc_name)
            new_rev_id = self._parse_revision(meta['rev'])
            if new_rev_id != self.ddoc_rev_no:
                self.ddoc_rev_no = new_rev_id
                valid = True
        except ReadDocumentException:
            pass

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

        return valid

    def _parse_revision(self, rev_string):
        return int(rev_string.split('-')[0])

    def _check_ddoc_replication_on_nodes(self):

        nodes = self.rest.node_statuses()
        retry_count = 3

        # nothing to check if there is only 1 node
        if len(nodes) <= 1:
            return

        for node in nodes:
            server_info = {"ip" : node.ip,
                       "port" : node.port,
                       "username" : self.rest.username,
                       "password" : self.rest.password}

            for count in xrange(retry_count):
                try:
                    rest_node = RestConnection(server_info)
                    content, meta = rest_node.get_ddoc(self.bucket, self.design_doc_name)
                    new_rev_id = self._parse_revision(meta['rev'])
                    if new_rev_id == self.ddoc_rev_no:
                        break
                    else:
                        self.log.info("Design Doc {0} version is not updated on node {1}:{2}. Retrying.".format(self.design_doc_name, node.ip, node.port))
                        time.sleep(2)
                except ReadDocumentException as e:
                    if(count < retry_count):
                        self.log.info("Design Doc {0} not yet available on node {1}:{2}. Retrying.".format(self.design_doc_name, node.ip, node.port))
                        time.sleep(2)
                    else:
                        self.log.error("Design Doc {0} failed to replicate on node {1}:{2}".format(self.design_doc_name, node.ip, node.port))
                        self.set_exception(e)
                        self.state = FINISHED
                        break
                except Exception as e:
                    if(count < retry_count):
                        self.log.info("Unexpected Exception Caught. Retrying.")
                        time.sleep(2)
                    else:
                        self.set_unexpected_exception(e)
                        self.state = FINISHED
                        break
            else:
                self.set_exception(Exception("Design Doc {0} version mismatch on node {1}:{2}".format(self.design_doc_name, node.ip, node.port)))


class ViewDeleteTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default"):
        Task.__init__(self, "delete_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ""
        if self.view:
            prefix = ("", "dev_")[self.view.dev_view]
        self.design_doc_name = prefix + design_doc_name

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
            if self.view:
                # remove view from existing design doc
                content, header = rest.get_ddoc(self.bucket, self.design_doc_name)
                ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
                if self.view.is_spatial:
                    status = ddoc.delete_spatial(self.view)
                else:
                    status = ddoc.delete_view(self.view)
                if not status:
                    self.state = FINISHED
                    self.set_exception(Exception('View does not exist! %s' % (self.view.name)))

                # update design doc
                rest.create_design_document(self.bucket, ddoc)
                self.state = CHECKING
                task_manager.schedule(self, 2)
            else:
                # delete the design doc
                rest.delete_view(self.bucket, self.design_doc_name)
                self.log.info("Design Doc : {0} was successfully deleted".format(self.design_doc_name))
                self.state = FINISHED
                self.set_result(True)

        except (ValueError, ReadDocumentException, DesignDocCreationException) as e:
            self.state = FINISHED
            self.set_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            rest = RestConnection(self.server)
            # make sure view was deleted
            query = {"stale" : "ok"}
            content = \
                rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
            self.state = FINISHED
            self.set_result(False)
        except QueryViewException as e:
            self.log.info("view : {0} was successfully deleted in ddoc: {1}".format(self.view.name, self.design_doc_name))
            self.state = FINISHED
            self.set_result(True)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class ViewQueryTask(Task):
    def __init__(self, server, design_doc_name, view_name,
                 query, expected_rows=None,
                 bucket="default", retry_time=2):
        Task.__init__(self, "query_view_task")
        self.server = server
        self.bucket = bucket
        self.view_name = view_name
        self.design_doc_name = design_doc_name
        self.query = query
        self.expected_rows = expected_rows
        self.retry_time = retry_time
        self.timeout = 900

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
            # make sure view can be queried
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query, self.timeout)

            if self.expected_rows is None:
                # no verification
                self.state = FINISHED
                self.set_result(content)
            else:
                self.state = CHECKING
                task_manager.schedule(self)

        except QueryViewException as e:
            # initial query failed, try again
            task_manager.schedule(self, self.retry_time)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            rest = RestConnection(self.server)
            # query and verify expected num of rows returned
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query, self.timeout)

            self.log.info("Server: %s, Design Doc: %s, View: %s, (%d rows) expected, (%d rows) returned" % \
                          (self.server.ip, self.design_doc_name, self.view_name, self.expected_rows, len(content['rows'])))

            raised_error = content.get(u'error', '') or ''.join([str(item) for item in content.get(u'errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)

            if len(content['rows']) == self.expected_rows:
                self.log.info("expected number of rows: '{0}' was found for view query".format(self.
                            expected_rows))
                self.state = FINISHED
                self.set_result(True)
            else:
                if len(content['rows']) > self.expected_rows:
                    raise QueryViewException(self.view_name, "Server: {0}, Design Doc: {1}, actual returned rows: '{2}' are greater than expected {3}"
                                             .format(self.server.ip, self.design_doc_name, len(content['rows']), self.expected_rows,))
                if "stale" in self.query:
                    if self.query["stale"].lower() == "false":
                        self.state = FINISHED
                        self.set_result(False)

                # retry until expected results or task times out
                task_manager.schedule(self, self.retry_time)
        except QueryViewException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class N1QLQueryTask(Task):
    def __init__(self,
                 server, bucket,
                 query, n1ql_helper = None,
                 expected_result=None,
                 verify_results = True,
                 is_explain_query = False,
                 index_name = None,
                 retry_time=2,
                 scan_consistency = None,
                 scan_vector = None):
        Task.__init__(self, "query_n1ql_task")
        self.server = server
        self.bucket = bucket
        self.query = query
        self.expected_result = expected_result
        self.n1ql_helper = n1ql_helper
        self.timeout = 900
        self.verify_results = verify_results
        self.is_explain_query = is_explain_query
        self.index_name = index_name
        self.retry_time = 2
        self.scan_consistency = scan_consistency
        self.scan_vector = scan_vector

    def execute(self, task_manager):
        try:
            # Query and get results
            self.log.info(" <<<<< START Executing Query {0} >>>>>>".format(self.query))
            if not self.is_explain_query:
                self.msg, self.isSuccess = self.n1ql_helper.run_query_and_verify_result(
                    query = self.query, server = self.server, expected_result = self.expected_result,
                    scan_consistency = self.scan_consistency, scan_vector = self.scan_vector,
                    verify_results = self.verify_results)
            else:
                self.actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.server,
                 scan_consistency = self.scan_consistency, scan_vector = self.scan_vector)
                self.log.info(self.actual_result)
            self.log.info(" <<<<< Done Executing Query {0} >>>>>>".format(self.query))
            self.state = CHECKING
            task_manager.schedule(self)
        except N1QLQueryException as e:
            self.state = FINISHED
            # initial query failed, try again
            task_manager.schedule(self, self.retry_time)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
           # Verify correctness of result set
           if self.verify_results:
            if not self.is_explain_query:
                if not self.isSuccess:
                    self.log.info(" Query {0} results leads to INCORRECT RESULT ".format(self.query))
                    raise N1QLQueryException(self.msg)
            else:
                check = self.n1ql_helper.verify_index_with_explain(self.actual_result, self.index_name)
                if not check:
                    actual_result = self.n1ql_helper.run_cbq_query(query = "select * from system:indexes", server = self.server)
                    self.log.info(actual_result)
                    raise Exception(" INDEX usage in Query {0} :: NOT FOUND {1} :: as observed in result {2}".format(
                        self.query, self.index_name, self.actual_result))
           self.log.info(" <<<<< Done VERIFYING Query {0} >>>>>>".format(self.query))
           self.set_result(True)
           self.state = FINISHED
        except N1QLQueryException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class CreateIndexTask(Task):
    def __init__(self,
                 server, bucket, index_name,
                 query, n1ql_helper = None,
                 retry_time=2, defer_build = False,
                 timeout = 240):
        Task.__init__(self, "create_index_task")
        self.server = server
        self.bucket = bucket
        self.defer_build = defer_build
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.timeout = timeout

    def execute(self, task_manager):
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.server)
            self.state = CHECKING
            task_manager.schedule(self)
        except CreateIndexException as e:
            # initial query failed, try again
            self.state = FINISHED
            task_manager.schedule(self, self.retry_time)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.error(e)
            self.set_exception(e)

    def check(self, task_manager):
        try:
           # Verify correctness of result set
            check = True
            if not self.defer_build:
                check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name, server = self.server, timeout = self.timeout)
            if not check:
                raise CreateIndexException("Index {0} not created as expected ".format(self.index_name))
            self.set_result(True)
            self.state = FINISHED
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.log.error(e)
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.error(e)
            self.set_exception(e)

class BuildIndexTask(Task):
    def __init__(self,
                 server, bucket,
                 query, n1ql_helper = None,
                 retry_time=2):
        Task.__init__(self, "build_index_task")
        self.server = server
        self.bucket = bucket
        self.query = query
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2

    def execute(self, task_manager):
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.server)
            self.state = CHECKING
            task_manager.schedule(self)
        except CreateIndexException as e:
            # initial query failed, try again
            self.state = FINISHED
            task_manager.schedule(self, self.retry_time)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
           # Verify correctness of result set
            self.set_result(True)
            self.state = FINISHED
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class MonitorIndexTask(Task):
    def __init__(self,
                 server, bucket, index_name,
                 n1ql_helper = None,
                 retry_time=2,
                 timeout = 240):
        Task.__init__(self, "build_index_task")
        self.server = server
        self.bucket = bucket
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.timeout = timeout

    def execute(self, task_manager):
        try:
            check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name,
             server = self.server, timeout = self.timeout)
            if not check:
                self.state = FINISHED
                raise CreateIndexException("Index {0} not created as expected ".format(self.index_name))
            self.state = CHECKING
            task_manager.schedule(self)
        except CreateIndexException as e:
            # initial query failed, try again
            self.state = FINISHED
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            self.set_result(True)
            self.state = FINISHED
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class DropIndexTask(Task):
    def __init__(self,
                 server, bucket, index_name,
                 query, n1ql_helper = None,
                 retry_time=2):
        Task.__init__(self, "drop_index_task")
        self.server = server
        self.bucket = bucket
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.timeout = 900
        self.retry_time = 2

    def execute(self, task_manager):
        try:
            # Query and get results
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server = self.server)
            if not check:
                raise DropIndexException("index {0} does not exist will not drop".format(self.index_name))
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.server)
            self.state = CHECKING
            task_manager.schedule(self)
        except N1QLQueryException as e:
            # initial query failed, try again
            self.state = FINISHED
            task_manager.schedule(self, self.retry_time)
        # catch and set all unexpected exceptions
        except DropIndexException as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
        # Verify correctness of result set
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server = self.server)
            if check:
                raise Exception("Index {0} not dropped as expected ".format(self.index_name))
            self.set_result(True)
            self.state = FINISHED
        except DropIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class MonitorViewQueryResultsTask(Task):
    def __init__(self, servers, design_doc_name, view,
                 query, expected_docs=None, bucket="default",
                 retries=100, error=None, verify_rows=False,
                 server_to_query=0):
        Task.__init__(self, "query_view_task")
        self.servers = servers
        self.bucket = bucket
        self.view_name = view.name
        self.view = view
        self.design_doc_name = design_doc_name
        self.query = query
        self.retries = retries
        self.current_retry = 0
        self.timeout = 900
        self.error = error
        self.expected_docs = expected_docs
        self.verify_rows = verify_rows
        self.rest = RestConnection(self.servers[server_to_query])
        self.results = None
        self.connection_timeout = 60000
        self.query["connection_timeout"] = self.connection_timeout
        if self.design_doc_name.find("dev_") == 0:
            self.query["full_set"] = "true"

    def execute(self, task_manager):

        try:
            self.current_retry += 1
            self.results = self.rest.query_view(
                self.design_doc_name, self.view_name, self.bucket, self.query,
                self.timeout)
            raised_error = self.results.get(u'error', '') or ''.join([str(item) for item in self.results.get(u'errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)
            else:
                self.log.info("view %s, query %s: expected- %s, actual -%s" % (
                                        self.design_doc_name, self.query,
                                        len(self.expected_docs),
                                        len(self.results.get(u'rows', []))))
                self.state = CHECKING
                task_manager.schedule(self)
        except QueryViewException, ex:
            self.log.error("During query run (ddoc=%s, query=%s, server=%s) error is: %s" % (
                                self.design_doc_name, self.query, self.servers[0].ip, str(ex)))
            if self.error and str(ex).find(self.error) != -1:
                self.state = FINISHED
                self.set_result({"passed" : True,
                                 "errors" : str(ex)})
            elif self.current_retry == self.retries:
                self.state = FINISHED
                self.set_result({"passed" : False,
                                 "errors" : str(ex)})
            elif str(ex).find('view_undefined') != -1 or \
                str(ex).find('not_found') != -1 or \
                str(ex).find('unable to reach') != -1 or \
                str(ex).find('socket error') != -1 or \
                str(ex).find('econnrefused') != -1 or \
                str(ex).find("doesn't exist") != -1 or \
                str(ex).find('missing') != -1 or \
                str(ex).find("Undefined set view") != -1:
                self.log.error(
                       "view_results not ready yet ddoc=%s , try again in 10 seconds..." %
                       self.design_doc_name)
                task_manager.schedule(self, 10)
            elif str(ex).find('timeout') != -1:
                self.connection_timeout = self.connection_timeout * 2
                self.log.error("view_results not ready yet ddoc=%s ," % self.design_doc_name + \
                               " try again in 10 seconds... and double timeout")
                task_manager.schedule(self, 10)
            else:
                self.state = FINISHED
                res = {"passed" : False,
                       "errors" : str(ex)}
                if self.results and self.results.get(u'rows', []):
                    res['results'] = self.results
                self.set_result(res)
        except Exception, ex:
            if self.current_retry == self.retries:
                self.state = CHECKING
                self.log.error("view %s, query %s: verifying results" % (
                                        self.design_doc_name, self.query))
                task_manager.schedule(self)
            else:
                self.log.error(
                       "view_results not ready yet ddoc=%s , try again in 10 seconds..." %
                       self.design_doc_name)
                task_manager.schedule(self, 10)

    def check(self, task_manager):
        try:
            if self.view.red_func and (('reduce' in self.query and\
                        self.query['reduce'] == "true") or (not 'reduce' in self.query)):
                if len(self.expected_docs) != len(self.results.get(u'rows', [])):
                    if self.current_retry == self.retries:
                        self.state = FINISHED
                        msg = "ddoc=%s, query=%s, server=%s" % (
                            self.design_doc_name, self.query, self.servers[0].ip)
                        msg += "Number of groups expected:%s, actual:%s" % (
                             len(self.expected_docs), len(self.results.get(u'rows', [])))
                        self.set_result({"passed" : False,
                                         "errors" : msg})
                    else:
                        RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                        self.state = EXECUTING
                        task_manager.schedule(self, 10)
                else:
                    for row in self.expected_docs:
                        key_expected = row['key']

                        if not (key_expected in [key['key'] for key in self.results.get(u'rows', [])]):
                            if self.current_retry == self.retries:
                                self.state = FINISHED
                                msg = "ddoc=%s, query=%s, server=%s" % (
                                    self.design_doc_name, self.query, self.servers[0].ip)
                                msg += "Key expected but not present :%s" % (key_expected)
                                self.set_result({"passed" : False,
                                                 "errors" : msg})
                            else:
                                RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                                self.state = EXECUTING
                                task_manager.schedule(self, 10)
                        else:
                            for res in self.results.get(u'rows', []):
                                if key_expected == res['key']:
                                    value = res['value']
                                    break
                            msg = "ddoc=%s, query=%s, server=%s\n" % (
                                    self.design_doc_name, self.query, self.servers[0].ip)
                            msg += "Key %s: expected value %s, actual: %s" % (
                                                                key_expected, row['value'], value)
                            self.log.info(msg)
                            if row['value'] == value:
                                self.state = FINISHED
                                self.log.info(msg)
                                self.set_result({"passed" : True,
                                                 "errors" : []})
                            else:
                                if self.current_retry == self.retries:
                                    self.state = FINISHED
                                    self.log.error(msg)
                                    self.set_result({"passed" : True,
                                                     "errors" : msg})
                                else:
                                    RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                                    self.state = EXECUTING
                                    task_manager.schedule(self, 10)
                return
            if len(self.expected_docs) > len(self.results.get(u'rows', [])):
                if self.current_retry == self.retries:
                    self.state = FINISHED
                    self.set_result({"passed" : False,
                                     "errors" : [],
                                     "results" : self.results})
                else:
                    RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                    if self.current_retry == 70:
                        self.query["stale"] = 'false'
                    self.log.info("View result is still not expected (ddoc=%s, query=%s, server=%s). retry in 10 sec" % (
                                    self.design_doc_name, self.query, self.servers[0].ip))
                    self.state = EXECUTING
                    task_manager.schedule(self, 10)
            elif len(self.expected_docs) < len(self.results.get(u'rows', [])):
                self.state = FINISHED
                self.set_result({"passed" : False,
                                 "errors" : [],
                                 "results" : self.results})
            elif len(self.expected_docs) == len(self.results.get(u'rows', [])):
                if self.verify_rows:
                    expected_ids = [row['id'] for row in self.expected_docs]
                    rows_ids = [str(row['id']) for row in self.results[u'rows']]
                    if expected_ids == rows_ids:
                        self.state = FINISHED
                        self.set_result({"passed" : True,
                                         "errors" : []})
                    else:
                        if self.current_retry == self.retries:
                            self.state = FINISHED
                            self.set_result({"passed" : False,
                                             "errors" : [],
                                             "results" : self.results})
                        else:
                            self.state = EXECUTING
                            task_manager.schedule(self, 10)
                else:
                    self.state = FINISHED
                    self.set_result({"passed" : True,
                                     "errors" : []})
        # catch and set all unexpected exceptions
        except Exception, e:
            self.state = FINISHED
            self.log.error("Exception caught %s" % str(e))
            self.set_exception(e)
            self.set_result({"passed" : False,
                             "errors" : str(e)})


class ModifyFragmentationConfigTask(Task):

    """
        Given a config dictionary attempt to configure fragmentation settings.
        This task will override the default settings that are provided for
        a given <bucket>.
    """

    def __init__(self, server, config=None, bucket="default"):
        Task.__init__(self, "modify_frag_config_task")

        self.server = server
        self.config = {"parallelDBAndVC" : "false",
                       "dbFragmentThreshold" : None,
                       "viewFragmntThreshold" : None,
                       "dbFragmentThresholdPercentage" : 100,
                       "viewFragmntThresholdPercentage" : 100,
                       "allowedTimePeriodFromHour" : None,
                       "allowedTimePeriodFromMin" : None,
                       "allowedTimePeriodToHour" : None,
                       "allowedTimePeriodToMin" : None,
                       "allowedTimePeriodAbort" : None,
                       "autoCompactionDefined" : "true"}
        self.bucket = bucket

        for key in config:
            self.config[key] = config[key]

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
            rest.set_auto_compaction(parallelDBAndVC=self.config["parallelDBAndVC"],
                                     dbFragmentThreshold=self.config["dbFragmentThreshold"],
                                     viewFragmntThreshold=self.config["viewFragmntThreshold"],
                                     dbFragmentThresholdPercentage=self.config["dbFragmentThresholdPercentage"],
                                     viewFragmntThresholdPercentage=self.config["viewFragmntThresholdPercentage"],
                                     allowedTimePeriodFromHour=self.config["allowedTimePeriodFromHour"],
                                     allowedTimePeriodFromMin=self.config["allowedTimePeriodFromMin"],
                                     allowedTimePeriodToHour=self.config["allowedTimePeriodToHour"],
                                     allowedTimePeriodToMin=self.config["allowedTimePeriodToMin"],
                                     allowedTimePeriodAbort=self.config["allowedTimePeriodAbort"],
                                     bucket=self.bucket)

            self.state = CHECKING
            task_manager.schedule(self, 10)

        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)


    def check(self, task_manager):
        try:
            rest = RestConnection(self.server)
            # verify server accepted settings
            content = rest.get_bucket_json(self.bucket)
            if content["autoCompactionSettings"] == False:
                self.set_exception(Exception("Failed to set auto compaction settings"))
            else:
                # retrieved compaction settings
                self.set_result(True)
            self.state = FINISHED

        except GetBucketInfoFailed as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)


class MonitorActiveTask(Task):

    """
        Attempt to monitor active task that  is available in _active_tasks API.
        It allows to monitor indexer, bucket compaction.

        Execute function looks at _active_tasks API and tries to identifies task for monitoring
        and its pid by: task type('indexer' , 'bucket_compaction', 'view_compaction' )
        and target value (for example "_design/ddoc" for indexing, bucket "default" for bucket compaction or
        "_design/dev_view" for view compaction).
        wait_task=True means that task should be found in the first attempt otherwise,
        we can assume that the task has been completed( reached 100%).

        Check function monitors task by pid that was identified in execute func
        and matches new progress result with the previous.
        task is failed if:
            progress is not changed  during num_iterations iteration
            new progress was gotten less then previous
        task is passed and completed if:
            progress reached wait_progress value
            task was not found by pid(believe that it's over)
    """

    def __init__(self, server, type, target_value, wait_progress=100, num_iterations=100, wait_task=True):
        Task.__init__(self, "monitor_active_task")

        self.server = server
        self.type = type  # indexer or bucket_compaction
        self.target_key = ""

        if self.type == 'indexer':
            pass # no special actions
        elif self.type == "bucket_compaction":
            self.target_key = "original_target"
        elif self.type == "view_compaction":
            self.target_key = "designDocument"
        else:
            raise Exception("type %s is not defined!" % self.type)
        self.target_value = target_value
        self.wait_progress = wait_progress
        self.num_iterations = num_iterations
        self.wait_task = wait_task

        self.rest = RestConnection(self.server)
        self.current_progress = None
        self.current_iter = 0
        self.task = None


    def execute(self, task_manager):
        tasks = self.rest.active_tasks()
        for task in tasks:
            if task["type"] == self.type and ((
                        self.target_key == "designDocument" and task[self.target_key] == self.target_value) or (
                        self.target_key == "original_target" and task[self.target_key]["type"] == self.target_value) or (
                        self.type == 'indexer') ):
                self.current_progress = task["progress"]
                self.task = task
                self.log.info("monitoring active task was found:" + str(task))
                self.log.info("progress %s:%s - %s %%" % (self.type, self.target_value, task["progress"]))
                if self.current_progress >= self.wait_progress:
                    self.log.info("expected progress was gotten: %s" % self.current_progress)
                    self.state = FINISHED
                    self.set_result(True)

                else:
                    self.state = CHECKING
                    task_manager.schedule(self, 5)
                return
        if self.wait_task:
            # task is not performed
            self.state = FINISHED
            self.log.error("expected active task %s:%s was not found" % (self.type, self.target_value))
            self.set_result(False)
        else:
            # task was completed
            self.state = FINISHED
            self.log.info("task for monitoring %s:%s completed" % (self.type, self.target_value))
            self.set_result(True)


    def check(self, task_manager):
        tasks = self.rest.active_tasks()
        for task in tasks:
            # if task still exists
            if task == self.task:
                self.log.info("progress %s:%s - %s %%" % (self.type, self.target_value, task["progress"]))
                # reached expected progress
                if task["progress"] >= self.wait_progress:
                        self.state = FINISHED
                        self.log.error("progress was reached %s" % self.wait_progress)
                        self.set_result(True)
                # progress value was changed
                if task["progress"] > self.current_progress:
                    self.current_progress = task["progress"]
                    self.currebt_iter = 0
                    task_manager.schedule(self, 2)
                # progress value was not changed
                elif task["progress"] == self.current_progress:
                    if self.current_iter < self.num_iterations:
                        time.sleep(2)
                        self.current_iter += 1
                        task_manager.schedule(self, 2)
                    # num iteration with the same progress = num_iterations
                    else:
                        self.state = FINISHED
                        self.log.error("progress for active task was not changed during %s sec" % 2 * self.num_iterations)
                        self.set_result(False)
                else:
                    self.state = FINISHED
                    self.log.error("progress for task %s:%s changed direction!" % (self.type, self.target_value))
                    self.set_result(False)

        # task was completed
        self.state = FINISHED
        self.log.info("task %s:%s was completed" % (self.type, self.target_value))
        self.set_result(True)

class MonitorViewFragmentationTask(Task):

    """
        Attempt to monitor fragmentation that is occurring for a given design_doc.
        execute stage is just for preliminary sanity checking of values and environment.

        Check function looks at index file accross all nodes and attempts to calculate
        total fragmentation occurring by the views within the design_doc.

        Note: If autocompaction is enabled and user attempts to monitor for fragmentation
        value higher than level at which auto_compaction kicks in a warning is sent and
        it is best user to use lower value as this can lead to infinite monitoring.
    """

    def __init__(self, server, design_doc_name, fragmentation_value=10, bucket="default"):

        Task.__init__(self, "monitor_frag_task")
        self.server = server
        self.bucket = bucket
        self.fragmentation_value = fragmentation_value
        self.design_doc_name = design_doc_name


    def execute(self, task_manager):

        # sanity check of fragmentation value
        if  self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = \
                "Invalid value for fragmentation %d" % self.fragmentation_value
            self.state = FINISHED
            self.set_exception(Exception(err_msg))

        # warning if autocompaction is less than <fragmentation_value>
        try:
            auto_compact_percentage = self._get_current_auto_compaction_percentage()
            if auto_compact_percentage != "undefined" and auto_compact_percentage < self.fragmentation_value:
                self.log.warn("Auto compaction is set to %s. Therefore fragmentation_value %s may not be reached" % (auto_compact_percentage, self.fragmentation_value))

            self.state = CHECKING
            task_manager.schedule(self, 5)
        except GetBucketInfoFailed as e:
            self.state = FINISHED
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)


    def _get_current_auto_compaction_percentage(self):
        """ check at bucket level and cluster level for compaction percentage """

        auto_compact_percentage = None
        rest = RestConnection(self.server)

        content = rest.get_bucket_json(self.bucket)
        if content["autoCompactionSettings"] == False:
            # try to read cluster level compaction settings
            content = rest.cluster_status()

        auto_compact_percentage = \
                content["autoCompactionSettings"]["viewFragmentationThreshold"]["percentage"]

        return auto_compact_percentage

    def check(self, task_manager):

        rest = RestConnection(self.server)
        new_frag_value = MonitorViewFragmentationTask.\
            calc_ddoc_fragmentation(rest, self.design_doc_name, bucket=self.bucket)

        self.log.info("%s: current amount of fragmentation = %d" % (self.design_doc_name,
                                                                    new_frag_value))
        if new_frag_value > self.fragmentation_value:
            self.state = FINISHED
            self.set_result(True)
        else:
            # try again
            task_manager.schedule(self, 2)

    @staticmethod
    def aggregate_ddoc_info(rest, design_doc_name, bucket="default", with_rebalance=False):

        nodes = rest.node_statuses()
        info = []
        for node in nodes:
            server_info = {"ip" : node.ip,
                           "port" : node.port,
                           "username" : rest.username,
                           "password" : rest.password}
            rest = RestConnection(server_info)
            status = False
            try:
                status, content = rest.set_view_info(bucket, design_doc_name)
            except Exception as e:
                print(str(e))
                if "Error occured reading set_view _info" in str(e) and with_rebalance:
                    print("node {0} {1} is not ready yet?: {2}".format(
                                    node.id, node.port, e.message))
                else:
                    raise e
            if status:
                info.append(content)
        return info

    @staticmethod
    def calc_ddoc_fragmentation(rest, design_doc_name, bucket="default", with_rebalance=False):

        total_disk_size = 0
        total_data_size = 0
        total_fragmentation = 0

        nodes_ddoc_info = \
            MonitorViewFragmentationTask.aggregate_ddoc_info(rest,
                                                         design_doc_name,
                                                         bucket, with_rebalance)
        total_disk_size = sum([content['disk_size'] for content in nodes_ddoc_info])
        total_data_size = sum([content['data_size'] for content in nodes_ddoc_info])

        if total_disk_size > 0 and total_data_size > 0:
            total_fragmentation = \
                (total_disk_size - total_data_size) / float(total_disk_size) * 100

        return total_fragmentation

class ViewCompactionTask(Task):

    """
        Executes view compaction for a given design doc. This is technicially view compaction
        as represented by the api and also because the fragmentation is generated by the
        keys emitted by map/reduce functions within views.  Task will check that compaction
        history for design doc is incremented and if any work was really done.
    """

    def __init__(self, server, design_doc_name, bucket="default", with_rebalance=False):

        Task.__init__(self, "view_compaction_task")
        self.server = server
        self.bucket = bucket
        self.design_doc_name = design_doc_name
        self.ddoc_id = "_design%2f" + design_doc_name
        self.compaction_revision = 0
        self.precompacted_fragmentation = 0
        self.with_rebalance = with_rebalance
        self.rest = RestConnection(self.server)

    def execute(self, task_manager):
        try:
            self.compaction_revision, self.precompacted_fragmentation = \
                self._get_compaction_details()
            self.log.info("{0}: stats compaction before triggering it: ({1},{2})".
                          format(self.design_doc_name,
                                 self.compaction_revision, self.precompacted_fragmentation))
            if self.precompacted_fragmentation == 0:
                self.log.info("%s: There is nothing to compact, fragmentation is 0" %
                              self.design_doc_name)
                self.set_result(False)
                self.state = FINISHED
                return
            self.rest.ddoc_compaction(self.ddoc_id, self.bucket)
            self.state = CHECKING
            task_manager.schedule(self, 2)
        except (CompactViewFailed, SetViewInfoNotFound) as ex:
            self.state = FINISHED
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    # verify compaction history incremented and some defraging occurred
    def check(self, task_manager):

        try:
            _compaction_running = self._is_compacting()
            new_compaction_revision, fragmentation = self._get_compaction_details()
            self.log.info("{0}: stats compaction:revision and fragmentation: ({1},{2})".
                          format(self.design_doc_name,
                                 new_compaction_revision, fragmentation))

            if new_compaction_revision == self.compaction_revision and _compaction_running :
                # compaction ran successfully but compaction was not changed
                # perhaps we are still compacting
                self.log.info("design doc {0} is compacting".format(self.design_doc_name))
                task_manager.schedule(self, 3)
            elif new_compaction_revision > self.compaction_revision or\
                 self.precompacted_fragmentation > fragmentation:
                self.log.info("{1}: compactor was run, compaction revision was changed on {0}".format(new_compaction_revision,
                                                                                                      self.design_doc_name))
                frag_val_diff = fragmentation - self.precompacted_fragmentation
                self.log.info("%s: fragmentation went from %d to %d" % \
                              (self.design_doc_name,
                               self.precompacted_fragmentation, fragmentation))

                if frag_val_diff > 0:

                    # compaction ran successfully but datasize still same
                    # perhaps we are still compacting
                    if self._is_compacting():
                        task_manager.schedule(self, 2)
                    self.log.info("compaction was completed, but fragmentation value {0} is more than before compaction {1}".
                                  format(fragmentation, self.precompacted_fragmentation))
                    # probably we already compacted, but no work needed to be done
                    self.set_result(self.with_rebalance)
                else:
                    self.set_result(True)
                self.state = FINISHED
            else:
                # Sometimes the compacting is not started immediately
                for i in xrange(17):
                    time.sleep(3)
                    if self._is_compacting():
                        task_manager.schedule(self, 2)
                        return
                    else:
                        new_compaction_revision, fragmentation = self._get_compaction_details()
                        self.log.info("{2}: stats compaction: ({0},{1})".
                          format(new_compaction_revision, fragmentation,
                                 self.design_doc_name))
                        # case of rebalance when with concurrent updates it's possible that
                        # compaction value has not changed significantly
                        if new_compaction_revision > self.compaction_revision and self.with_rebalance:
                            self.log.warn("the compaction revision was increased,\
                             but the actual fragmentation value has not changed significantly")
                            self.set_result(True)
                            self.state = FINISHED
                            return
                        else:
                            continue
                # print details in case of failure
                self.log.info("design doc {0} is compacting:{1}".format(self.design_doc_name, self._is_compacting()))
                new_compaction_revision, fragmentation = self._get_compaction_details()
                self.log.error("stats compaction still: ({0},{1})".
                          format(new_compaction_revision, fragmentation))
                status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
                stats = content["stats"]
                self.log.warn("general compaction stats:{0}".format(stats))
                self.set_exception(Exception("Check system logs, looks like compaction failed to start"))

        except (SetViewInfoNotFound) as ex:
            self.state = FINISHED
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _get_compaction_details(self):
        status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
        curr_no_of_compactions = content["stats"]["compactions"]
        curr_ddoc_fragemtation = \
            MonitorViewFragmentationTask.calc_ddoc_fragmentation(self.rest, self.design_doc_name, self.bucket, self.with_rebalance)
        return (curr_no_of_compactions, curr_ddoc_fragemtation)

    def _is_compacting(self):
        status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
        return content["compact_running"] == True

'''task class for failover. This task will only failover nodes but doesn't
 rebalance as there is already a task to do that'''
class FailoverTask(Task):
    def __init__(self, servers, to_failover=[], wait_for_pending=0, graceful=False, use_hostnames=False):
        Task.__init__(self, "failover_task")
        self.servers = servers
        self.to_failover = to_failover
        self.graceful = graceful
        self.wait_for_pending = wait_for_pending
        self.use_hostnames = use_hostnames

    def execute(self, task_manager):
        try:
            self._failover_nodes(task_manager)
            self.log.info("{0} seconds sleep after failover, for nodes to go pending....".format(self.wait_for_pending))
            time.sleep(self.wait_for_pending)
            self.state = FINISHED
            self.set_result(True)

        except FailoverFailedException as e:
            self.state = FINISHED
            self.set_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _failover_nodes(self, task_manager):
        rest = RestConnection(self.servers[0])
        # call REST fail_over for the nodes to be failed over
        for server in self.to_failover:
            for node in rest.node_statuses():
                if (server.hostname if self.use_hostnames else server.ip) == node.ip and int(server.port) == int(node.port):
                    self.log.info("Failing over {0}:{1} with graceful={2}".format(node.ip, node.port, self.graceful))
                    rest.fail_over(node.id, self.graceful)

class GenerateExpectedViewResultsTask(Task):

    """
        Task to produce the set of keys that are expected to be returned
        by querying the provided <view>.  Results can be later passed to
        ViewQueryVerificationTask and compared with actual results from
        server.

        Currently only views with map functions that emit a single string
        or integer as keys are accepted.

        Also NOTE, this task is to be used with doc_generators that
        produce json like documentgenerator.DocumentGenerator
    """
    def __init__(self, doc_generators, view, query):
        Task.__init__(self, "generate_view_query_results_task")
        self.doc_generators = doc_generators
        self.view = view
        self.query = query
        self.emitted_rows = []
        self.is_reduced = self.view.red_func is not None and (('reduce' in query and query['reduce'] == "true") or\
                         (not 'reduce' in query))
        self.custom_red_fn = self.is_reduced and not self.view.red_func in ['_count', '_sum', '_stats']
        self.type_filter = None


    def execute(self, task_manager):
        try:
            self.generate_emitted_rows()
            self.filter_emitted_rows()
            self.log.info("Finished generating expected query results")
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception, ex:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(self.emitted_rows)


    def generate_emitted_rows(self):
        emit_key = re.sub(r',.*', '', re.sub(r'.*emit\([ +]?doc\.', '', self.view.map_func))
        emit_value = None
        if re.match(r'.*emit\([ +]?\[doc\.*', self.view.map_func):
            emit_key = re.sub(r'],.*', '', re.sub(r'.*emit\([ +]?\[doc\.', '', self.view.map_func))
            emit_key = emit_key.split(", doc.")
        if re.match(r'.*new RegExp\("\^.*', self.view.map_func):
            filter_what = re.sub(r'.*new RegExp\(.*\)*doc\.', '',
                                 re.sub(r'\.match\(.*', '', self.view.map_func))
            self.type_filter = {"filter_what" : filter_what,
                               "filter_expr" : re.sub(r'[ +]?"\);.*', '',
                                 re.sub(r'.*.new RegExp\("\^', '', self.view.map_func))}
        if self.is_reduced and self.view.red_func != "_count":
            emit_value = re.sub(r'\);.*', '', re.sub(r'.*emit\([ +]?\[*],[ +]?doc\.', '', self.view.map_func))
            if self.view.map_func.count("[") <= 1:
                emit_value = re.sub(r'\);.*', '', re.sub(r'.*emit\([ +]?.*,[ +]?doc\.', '', self.view.map_func))
        for doc_gen in self.doc_generators:

            query_doc_gen = copy.deepcopy(doc_gen)
            while query_doc_gen.has_next():

                _id, val = query_doc_gen.next()
                val = json.loads(val)

                if isinstance(emit_key, list):
                    val_emit_key = []
                    for ek in emit_key:
                        val_emit_key.append(val[ek])
                else:
                    val_emit_key = val[emit_key]
                if self.type_filter:
                    filter_expr = r'\A{0}.*'.format(self.type_filter["filter_expr"])
                    if re.match(filter_expr, val[self.type_filter["filter_what"]]) is None:
                        continue
                if isinstance(val_emit_key, unicode):
                    val_emit_key = val_emit_key.encode('utf-8')
                if not self.is_reduced or self.view.red_func == "_count" or self.custom_red_fn:
                    self.emitted_rows.append({'id' : _id, 'key' : val_emit_key})
                else:
                    val_emit_value = val[emit_value]
                    self.emitted_rows.append({'value' : val_emit_value, 'key' : val_emit_key, 'id' : _id, })

    def filter_emitted_rows(self):

        query = self.query

        # parse query flags
        descending_set = 'descending' in query and query['descending'] == "true"
        startkey_set, endkey_set = 'startkey' in query, 'endkey' in query
        startkey_docid_set, endkey_docid_set = 'startkey_docid' in query, 'endkey_docid' in query
        inclusive_end_false = 'inclusive_end' in query and query['inclusive_end'] == "false"
        key_set = 'key' in query


        # sort expected results to match view results
        expected_rows = sorted(self.emitted_rows,
                               cmp=GenerateExpectedViewResultsTask.cmp_result_rows,
                               reverse=descending_set)

        # filter rows according to query flags
        if startkey_set:
            start_key = query['startkey']
            if isinstance(start_key, str) and start_key.find('"') == 0:
                start_key = start_key[1:-1]
            if isinstance(start_key, str) and start_key.find('[') == 0:
                start_key = start_key[1:-1].split(',')
                start_key = map(lambda x:int(x) if x != 'null' else None, start_key)
        else:
            start_key = expected_rows[0]['key']
            if isinstance(start_key, str) and start_key.find('"') == 0:
                start_key = start_key[1:-1]
        if endkey_set:
            end_key = query['endkey']
            if isinstance(end_key, str) and end_key.find('"') == 0:
                end_key = end_key[1:-1]
            if isinstance(end_key, str) and end_key.find('[') == 0:
                end_key = end_key[1:-1].split(',')
                end_key = map(lambda x:int(x) if x != 'null' else None, end_key)
        else:
            end_key = expected_rows[-1]['key']
            if isinstance(end_key, str) and end_key.find('"') == 0:
                end_key = end_key[1:-1]

        if descending_set:
            start_key, end_key = end_key, start_key

        if startkey_set or endkey_set:
            if isinstance(start_key, str):
                start_key = start_key.strip("\"")
            if isinstance(end_key, str):
                end_key = end_key.strip("\"")
            expected_rows = [row for row in expected_rows if row['key'] >= start_key and row['key'] <= end_key]

        if key_set:
            key_ = query['key']
            if isinstance(key_, str) and key_.find('[') == 0:
                key_ = key_[1:-1].split(',')
                key_ = map(lambda x:int(x) if x != 'null' else None, key_)
            start_key, end_key = key_, key_
            expected_rows = [row for row in expected_rows if row['key'] == key_]


        if descending_set:
            startkey_docid_set, endkey_docid_set = endkey_docid_set, startkey_docid_set

        if startkey_docid_set:
            if not startkey_set:
                self.log.warn("Ignoring startkey_docid filter when startkey is not set")
            else:
                do_filter = False
                if descending_set:
                    if endkey_docid_set:
                        startkey_docid = query['endkey_docid']
                        do_filter = True
                else:
                    startkey_docid = query['startkey_docid']
                    do_filter = True

                if do_filter:
                    expected_rows = \
                        [row for row in expected_rows if row['id'] >= startkey_docid or row['key'] > start_key]

        if endkey_docid_set:
            if not endkey_set:
                self.log.warn("Ignoring endkey_docid filter when endkey is not set")
            else:
                do_filter = False
                if descending_set:
                    if endkey_docid_set:
                        endkey_docid = query['startkey_docid']
                        do_filter = True
                else:
                    endkey_docid = query['endkey_docid']
                    do_filter = True

                if do_filter:
                    expected_rows = \
                        [row for row in expected_rows if row['id'] <= endkey_docid or row['key'] < end_key]


        if inclusive_end_false:
            if endkey_set and endkey_docid_set:
                # remove all keys that match endkey
                expected_rows = [row for row in expected_rows if row['id'] < query['endkey_docid']  or row['key'] < end_key]
            elif endkey_set:
                expected_rows = [row for row in expected_rows if row['key'] != end_key]

        if self.is_reduced:
            groups = {}
            gr_level = None
            if not 'group' in query and\
               not 'group_level' in query:
               if len(expected_rows) == 0:
                   expected_rows = []
                   self.emitted_rows = expected_rows
                   return
               if self.view.red_func == '_count':
                   groups[None] = len(expected_rows)
               elif self.view.red_func == '_sum':
                   groups[None] = 0
                   groups[None] = math.fsum([row['value']
                                               for row in expected_rows])
               elif self.view.red_func == '_stats':
                   groups[None] = {}
                   values = [row['value'] for row in expected_rows]
                   groups[None]['count'] = len(expected_rows)
                   groups[None]['sum'] = math.fsum(values)
                   groups[None]['max'] = max(values)
                   groups[None]['min'] = min(values)
                   groups[None]['sumsqr'] = math.fsum(map(lambda x: x * x, values))
               elif self.custom_red_fn:
                   custom_action = re.sub(r'.*return[ +]', '', re.sub(r'.*return[ +]', '', self.view.red_func))
                   if custom_action.find('String') != -1:
                       groups[None] = str(len(expected_rows))
                   elif custom_action.find('-') != -1:
                       groups[None] = -len(expected_rows)
            elif 'group' in query and query['group'] == 'true':
                if not 'group_level' in query:
                    gr_level = len(expected_rows) - 1
            elif 'group_level' in query:
                gr_level = int(query['group_level'])
            if gr_level is not None:
                for row in expected_rows:
                    key = str(row['key'][:gr_level])
                    if not key in groups:
                        if self.view.red_func == '_count':
                            groups[key] = 1
                        elif self.view.red_func == '_sum':
                            groups[key] = row['value']
                        elif self.view.red_func == '_stats':
                            groups[key] = {}
                            groups[key]['count'] = 1
                            groups[key]['sum'] = row['value']
                            groups[key]['max'] = row['value']
                            groups[key]['min'] = row['value']
                            groups[key]['sumsqr'] = row['value'] ** 2
                    else:
                        if self.view.red_func == '_count':
                           groups[key] += 1
                        elif self.view.red_func == '_sum':
                            groups[key] += row['value']
                        elif self.view.red_func == '_stats':
                            groups[key]['count'] += 1
                            groups[key]['sum'] += row['value']
                            groups[key]['max'] = max(row['value'], groups[key]['max'])
                            groups[key]['min'] = min(row['value'], groups[key]['min'])
                            groups[key]['sumsqr'] += row['value'] ** 2
            expected_rows = []
            for group, value in groups.iteritems():
                if isinstance(group, str) and group.find("[") == 0:
                    group = group[1:-1].split(",")
                    group = [int(k) for k in group]
                expected_rows.append({"key" : group, "value" : value})
            expected_rows = sorted(expected_rows,
                               cmp=GenerateExpectedViewResultsTask.cmp_result_rows,
                               reverse=descending_set)
        if 'skip' in query:
            expected_rows = expected_rows[(int(query['skip'])):]
        if 'limit' in query:
            expected_rows = expected_rows[:(int(query['limit']))]

        self.emitted_rows = expected_rows

    @staticmethod
    def cmp_result_rows(x, y):
        rc = cmp(x['key'], y['key'])
        if rc == 0:
            # sort by id is tie breaker
            rc = cmp(x['id'], y['id'])
        return rc

class ViewQueryVerificationTask(Task):

    """
        * query with stale=false
        * check for duplicates
        * check for missing docs
            * check memcached
            * check couch
    """
    def __init__(self, design_doc_name, view_name, query, expected_rows, server=None,
                num_verified_docs=20, bucket="default", query_timeout=120, results=None):

        Task.__init__(self, "view_query_verification_task")
        self.server = server
        self.design_doc_name = design_doc_name
        self.view_name = view_name
        self.query = query
        self.expected_rows = expected_rows
        self.num_verified_docs = num_verified_docs
        self.bucket = bucket
        self.query_timeout = query_timeout
        self.results = results

        try:
            for key in config:
                self.config[key] = config[key]
        except:
            pass

    def execute(self, task_manager):
        if not self.results:
            rest = RestConnection(self.server)

            try:
                # query for full view results
                self.query["stale"] = "false"
                self.query["reduce"] = "false"
                self.query["include_docs"] = "true"
                self.results = rest.query_view(self.design_doc_name, self.view_name,
                                               self.bucket, self.query, timeout=self.query_timeout)
            except QueryViewException as e:
                self.set_exception(e)
                self.state = FINISHED


            msg = "Checking view query results: (%d keys expected) vs (%d keys returned)" % \
                (len(self.expected_rows), len(self.results['rows']))
            self.log.info(msg)

        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        err_infos = []
        rc_status = {"passed" : False,
                     "errors" : err_infos}  # array of dicts with keys 'msg' and 'details'
        try:
            # create verification id lists
            expected_ids = [row['id'] for row in self.expected_rows]
            couch_ids = [str(row['id']) for row in self.results['rows']]

            # check results
            self.check_for_duplicate_ids(expected_ids, couch_ids, err_infos)
            self.check_for_missing_ids(expected_ids, couch_ids, err_infos)
            self.check_for_extra_ids(expected_ids, couch_ids, err_infos)
            self.check_for_value_corruption(err_infos)

            # check for errors
            if len(rc_status["errors"]) == 0:
               rc_status["passed"] = True

            self.state = FINISHED
            self.set_result(rc_status)
        except Exception, ex:
            self.state = FINISHED
            try:
                max_example_result = max(100, len(self.results['rows'] - 1))
                self.log.info("FIRST %s RESULTS for view %s : %s" % (max_example_result , self.view_name,
                                                                     self.results['rows'][max_example_result]))
            except Exception, inner_ex:
                 self.log.error(inner_ex)
            self.set_result({"passed" : False,
                             "errors" : "ERROR: %s" % ex})

    def check_for_duplicate_ids(self, expected_ids, couch_ids, err_infos):

        extra_id_set = set(couch_ids) - set(expected_ids)

        seen = set()
        for id in couch_ids:
            if id in seen and id not in extra_id_set:
                extra_id_set.add(id)
            else:
                seen.add(id)

        if len(extra_id_set) > 0:
            # extra/duplicate id verification
            dupe_rows = [row for row in self.results['rows'] if row['id'] in extra_id_set]
            err = { "msg" : "duplicate rows found in query results",
                    "details" : dupe_rows }
            err_infos.append(err)

    def check_for_missing_ids(self, expected_ids, couch_ids, err_infos):

        missing_id_set = set(expected_ids) - set(couch_ids)

        if len(missing_id_set) > 0:

            missing_id_errors = self.debug_missing_items(missing_id_set)

            if len(missing_id_errors) > 0:
                err = { "msg" : "missing ids from memcached",
                        "details" : missing_id_errors}
                err_infos.append(err)

    def check_for_extra_ids(self, expected_ids, couch_ids, err_infos):

        extra_id_set = set(couch_ids) - set(expected_ids)
        if len(extra_id_set) > 0:
                err = { "msg" : "extra ids from memcached",
                        "details" : extra_id_set}
                err_infos.append(err)

    def check_for_value_corruption(self, err_infos):

        if self.num_verified_docs > 0:

            doc_integrity_errors = self.include_doc_integrity()

            if len(doc_integrity_errors) > 0:
                err = { "msg" : "missmatch in document values",
                        "details" : doc_integrity_errors }
                err_infos.append(err)


    def debug_missing_items(self, missing_id_set):
        rest = RestConnection(self.server)
        client = KVStoreAwareSmartClient(rest, self.bucket)
        missing_id_errors = []

        # debug missing documents
        for doc_id in list(missing_id_set)[:self.num_verified_docs]:

            # attempt to retrieve doc from memcached
            mc_item = client.mc_get_full(doc_id)
            if mc_item == None:
                missing_id_errors.append("document %s missing from memcached" % (doc_id))

            # attempt to retrieve doc from disk
            else:

                num_vbuckets = len(rest.get_vbuckets(self.bucket))
                doc_meta = client.get_doc_metadata(num_vbuckets, doc_id)

                if(doc_meta != None):
                    if (doc_meta['key_valid'] != 'valid'):
                        msg = "Error expected in results for key with invalid state %s" % doc_meta
                        missing_id_errors.append(msg)

                else:
                    msg = "query doc_id: %s doesn't exist in bucket: %s" % \
                        (doc_id, self.bucket)
                    missing_id_errors.append(msg)

            if(len(missing_id_errors) == 0):
                msg = "view engine failed to index doc [%s] in query: %s" % (doc_id, self.query)
                missing_id_errors.append(msg)

            return missing_id_errors

    def include_doc_integrity(self):
        rest = RestConnection(self.server)
        client = KVStoreAwareSmartClient(rest, self.bucket)
        doc_integrity_errors = []

        if 'doc' not in self.results['rows'][0]:
            return doc_integrity_errors
        exp_verify_set = [row['doc'] for row in\
            self.results['rows'][:self.num_verified_docs]]

        for view_doc in exp_verify_set:
            doc_id = str(view_doc['_id'])
            mc_item = client.mc_get_full(doc_id)

            if mc_item is not None:
                mc_doc = json.loads(mc_item["value"])

                # compare doc content
                for key in mc_doc.keys():
                    if(mc_doc[key] != view_doc[key]):
                        err_msg = \
                            "error verifying document id %s: retrieved value %s expected %s \n" % \
                                (doc_id, mc_doc[key], view_doc[key])
                        doc_integrity_errors.append(err_msg)
            else:
                doc_integrity_errors.append("doc_id %s could not be retrieved for verification \n" % doc_id)

        return doc_integrity_errors

class BucketFlushTask(Task):
    def __init__(self, server, bucket="default"):
        Task.__init__(self, "bucket_flush_task")
        self.server = server
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
            if rest.flush_bucket(self.bucket):
                self.state = CHECKING
                task_manager.schedule(self)
            else:
                self.state = FINISHED
                self.set_result(False)

        except BucketFlushFailed as e:
            self.state = FINISHED
            self.set_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            # check if after flush the vbuckets are ready
            if BucketOperationHelper.wait_for_vbuckets_ready_state(self.server, self.bucket):
                self.set_result(True)
            else:
                self.log.error("Unable to reach bucket {0} on server {1} after flush".format(self.bucket, self.server))
                self.set_result(False)
            self.state = FINISHED
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class MonitorDBFragmentationTask(Task):

    """
        Attempt to monitor fragmentation that is occurring for a given bucket.

        Note: If autocompaction is enabled and user attempts to monitor for fragmentation
        value higher than level at which auto_compaction kicks in a warning is sent and
        it is best user to use lower value as this can lead to infinite monitoring.
    """

    def __init__(self, server, fragmentation_value=10, bucket="default", get_view_frag=False):

        Task.__init__(self, "monitor_frag_db_task")
        self.server = server
        self.bucket = bucket
        self.fragmentation_value = fragmentation_value
        self.get_view_frag = get_view_frag

    def execute(self, task_manager):

        # sanity check of fragmentation value
        if  self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = \
                "Invalid value for fragmentation %d" % self.fragmentation_value
            self.state = FINISHED
            self.set_exception(Exception(err_msg))

        self.state = CHECKING
        task_manager.schedule(self, 5)

    def check(self, task_manager):
        try:
            rest = RestConnection(self.server)
            stats = rest.fetch_bucket_stats(bucket=self.bucket)
            if self.get_view_frag:
                new_frag_value = stats["op"]["samples"]["couch_views_fragmentation"][-1]
                self.log.info("Current amount of views fragmentation = %d" % new_frag_value)
            else:
                new_frag_value = stats["op"]["samples"]["couch_docs_fragmentation"][-1]
                self.log.info("current amount of docs fragmentation = %d" % new_frag_value)
            if new_frag_value >= self.fragmentation_value:
                self.state = FINISHED
                self.set_result(True)
            else:
                # try again
                task_manager.schedule(self, 2)
        except Exception, ex:
            self.state = FINISHED
            self.set_result(False)
            self.set_exception(ex)


class CBRecoveryTask(Task):
    def __init__(self, src_server, dest_server, bucket_src='', bucket_dest='', username='', password='',
                 username_dest='', password_dest='', verbose=False, wait_completed=True):
        Task.__init__(self, "cbrecovery_task")
        self.src_server = src_server
        self.dest_server = dest_server
        self.bucket_src = bucket_src
        self.bucket_dest = bucket_dest
        if isinstance(bucket_src, Bucket):
            self.bucket_src = bucket_src.name
        if isinstance(bucket_dest, Bucket):
            self.bucket_dest = bucket_dest.name
        self.username = username
        self.password = password
        self.username_dest = username_dest
        self.password_dest = password_dest
        self.verbose = verbose
        self.wait_completed = wait_completed

        try:
            self.shell = RemoteMachineShellConnection(src_server)
            self.info = self.shell.extract_remote_info()
            self.rest = RestConnection(dest_server)
        except Exception, e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)
        self.progress = {}
        self.started = False
        self.retries = 0

    def execute(self, task_manager):
        try:
            if self.info.type.lower() == "linux":
                command = "/opt/couchbase/bin/cbrecovery "
            elif self.info.type.lower() == "windows":
                command = "C:/Program\ Files/Couchbase/Server/bin/cbrecovery.exe "

            src_url = "http://{0}:{1}".format(self.src_server.ip, self.src_server.port)
            dest_url = "http://{0}:{1}".format(self.dest_server.ip, self.dest_server.port)
            command += "{0} {1} ".format(src_url, dest_url)

            if self.bucket_src:
                command += "-b {0} ".format(self.bucket_src)
            if self.bucket_dest:
                command += "-B {0} ".format(self.bucket_dest)
            if self.username:
                command += "-u {0} ".format(self.username)
            if self.password:
                command += "-p {0} ".format(self.password)
            if self.username_dest:
                command += "-U {0} ".format(self.username_dest)
            if self.password_dest:
                command += "-P {0} ".format(self.password_dest)
            if self.verbose:
                command += " -v "
            transport = self.shell._ssh_client.get_transport()
            transport.set_keepalive(1)
            self.chan = transport.open_session()
            self.chan.settimeout(10 * 60.0)
            self.chan.exec_command(command)
            self.log.info("command was executed: '{0}'".format(command))
            self.state = CHECKING
            task_manager.schedule(self, 20)
        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)

    #it was done to keep connection alive
    def checkChannel(self):
        try:
            if self.chan.exit_status_ready():
                if self.chan.recv_ready():
                    output = self.chan.recv(1048576)
            if self.chan.recv_stderr_ready():
                error = self.chan.recv_stderr(1048576)
        except socket.timeout:
            print("SSH channel timeout exceeded.")
        except Exception:
            traceback.print_exc()

    def check(self, task_manager):
        self.checkChannel()
        self.recovery_task = self.rest.get_recovery_task()
        if self.recovery_task is not None:
            if not self.started:
                self.started = True
                if not self.wait_completed:
                    progress = self.rest.get_recovery_progress(self.recovery_task["recoveryStatusURI"])
                    self.log.info("cbrecovery strarted with progress: {0}".format(progress))
                    self.log.info("will not wait for the end of the cbrecovery")
                    self.state = FINISHED
                    self.set_result(True)
            progress = self.rest.get_recovery_progress(self.recovery_task["recoveryStatusURI"])
            if progress == self.progress:
                self.log.warn("cbrecovery progress was not changed")
                if self.retries > 20:
                    self.shell.disconnect()
                    self.rest.print_UI_logs()
                    self.state = FINISHED
                    self.log.warn("ns_server_tasks: {0}".format(self.rest.ns_server_tasks()))
                    self.log.warn("cbrecovery progress: {0}".format(self.rest.get_recovery_progress(self.recovery_task["recoveryStatusURI"])))
                    self.set_exception(CBRecoveryFailedException("cbrecovery hangs"))
                    return
                self.retries += 1
                task_manager.schedule(self, 20)
            else:
                self.progress = progress
                self.log.info("cbrecovery progress: {0}".format(self.progress))
                self.retries = 0
                task_manager.schedule(self, 20)
        else:
            if self.started:
                self.shell.disconnect()
                self.log.info("cbrecovery completed succesfully")
                self.state = FINISHED
                self.set_result(True)
            if self.retries > 5:
                self.shell.disconnect()
                self.rest.print_UI_logs()
                self.state = FINISHED
                self.log.warn("ns_server_tasks: {0}".format(self.rest.ns_server_tasks()))
                self.set_exception(CBRecoveryFailedException("cbrecovery was not started"))
                return
            else:
                self.retries += 1
                task_manager.schedule(self, 20)


class CompactBucketTask(Task):

    def __init__(self, server, bucket="default"):
        Task.__init__(self, "bucket_compaction_task")
        self.server = server
        self.bucket = bucket
        self.rest = RestConnection(server)
        self.retries = 20
        self.statuses = {}

        # get the current count of compactions

        nodes = self.rest.get_nodes()
        self.compaction_count = {}

        for node in nodes:
            self.compaction_count[node.ip] = 0

    def execute(self, task_manager):

        try:
            status = self.rest.compact_bucket(self.bucket)
            self.state = CHECKING

        except BucketCompactionException as e:
            self.log.error("Bucket compaction failed for unknown reason")
            self.set_exception(e)
            self.state = FINISHED
            self.set_result(False)

        task_manager.schedule(self)

    def check(self, task_manager):
        # check bucket compaction status across all nodes
        nodes = self.rest.get_nodes()
        current_compaction_count = {}

        for node in nodes:
            current_compaction_count[node.ip] = 0
            s = TestInputServer()
            s.ip = node.ip
            s.ssh_username = self.server.ssh_username
            s.ssh_password = self.server.ssh_password
            shell = RemoteMachineShellConnection(s)
            res = shell.execute_cbstats("", "raw", keyname="kvtimings", vbid="")


            for i in res[0]:
                # check for lines that look like
                #    rw_0:compact_131072,262144:        8
                if 'compact' in i:
                    current_compaction_count[node.ip] += int(i.split(':')[2])


        if cmp(current_compaction_count, self.compaction_count) == 1:
            # compaction count has increased
            self.set_result(True)
            self.state = FINISHED

        else:
            if self.retries > 0:
                # retry
                self.retries = self.retries - 1
                task_manager.schedule(self, 10)
            else:
                # never detected a compaction task running
                self.set_result(False)
                self.state = FINISHED

    def _get_disk_size(self):
        stats = self.rest.fetch_bucket_stats(bucket=self.bucket)
        total_disk_size = stats["op"]["samples"]["couch_total_disk_size"][-1]
        self.log.info("Disk size is = %d" % total_disk_size)
        return total_disk_size

class MonitorViewCompactionTask(ViewCompactionTask):

    def __init__(self, server, design_doc_name, bucket="default", with_rebalance=False, frag_value=0):
        ViewCompactionTask.__init__(self, server, design_doc_name, bucket, with_rebalance)
        self.ddoc_id = "_design%2f" + design_doc_name
        self.compaction_revision = 0
        self.precompacted_fragmentation = 0
        self.fragmentation_value = frag_value
        self.rest = RestConnection(self.server)

    def execute(self, task_manager):
        try:
            self.compaction_revision, self.precompacted_fragmentation = self._get_compaction_details()
            self.log.info("{0}: stats compaction before triggering it: ({1},{2})".
                          format(self.design_doc_name, self.compaction_revision, self.precompacted_fragmentation))
            self.disk_size = self._get_disk_size()
            self.log.info("Disk Size Before Compaction {0}".format(self.disk_size))
            if self.precompacted_fragmentation == 0:
                self.log.warn("%s: There is nothing to compact, fragmentation is 0" % self.design_doc_name)
                self.set_result(False)
                self.state = FINISHED
            elif self.precompacted_fragmentation < self.fragmentation_value:
                self.log.info("{0}: Compaction is already done and there is nothing to compact, current fragmentation is lesser {1} {2}".
                              format(self.design_doc_name, self.precompacted_fragmentation, self.fragmentation_value))
                self.compaction_revision, self.precompacted_fragmentation = self._get_compaction_details()
                self.log.info("{0}: stats compaction before triggering it: ({1},{2})".
                              format(self.design_doc_name, self.compaction_revision, self.precompacted_fragmentation))
                self.set_result(True)
                self.state = FINISHED
                return
            self.state = CHECKING
            task_manager.schedule(self, 2)
        except (CompactViewFailed, SetViewInfoNotFound) as ex:
            self.state = FINISHED
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    # verify compaction history incremented and some defraging occurred
    def check(self, task_manager):
        try:
            _compaction_running = self._is_compacting()
            new_compaction_revision, fragmentation = self._get_compaction_details()
            self.log.info("{0}: stats compaction:revision and fragmentation: ({1},{2})".
                          format(self.design_doc_name, new_compaction_revision, fragmentation))
            curr_disk_size = self._get_disk_size()
            self.log.info("Current Disk Size {0}".format(curr_disk_size))
            if new_compaction_revision == self.compaction_revision and _compaction_running:
                # compaction ran successfully but compaction was not changed, perhaps we are still compacting
                self.log.info("design doc {0} is compacting".format(self.design_doc_name))
                task_manager.schedule(self, 3)
            elif self.precompacted_fragmentation > fragmentation:
                self.log.info("%s: Pre Compacted fragmentation is more, before Compaction %d and after Compaction %d" % \
                              (self.design_doc_name, self.precompacted_fragmentation, fragmentation))
                frag_val_diff = fragmentation - self.precompacted_fragmentation
                if new_compaction_revision == self.compaction_revision or new_compaction_revision > self.compaction_revision:
                    self.log.info("{1}: compactor was run, compaction revision was changed on {0}".
                                  format(new_compaction_revision, self.design_doc_name))
                    self.log.info("%s: fragmentation went from %d to %d" % (self.design_doc_name, self.precompacted_fragmentation, fragmentation))
                if frag_val_diff > 0:
                    if self._is_compacting():
                        task_manager.schedule(self, 5)
                    self.log.info("compaction was completed, but fragmentation value {0} is more than before compaction {1}".
                                  format(fragmentation, self.precompacted_fragmentation))
                    self.log.info("Load is still in progress, Need to be checked")
                    self.set_result(self.with_rebalance)
                else:
                    self.set_result(True)
                self.state = FINISHED
            else:
                for i in xrange(10):
                    time.sleep(3)
                    if self._is_compacting():
                        task_manager.schedule(self, 2)
                        return
                    else:
                        new_compaction_revision, fragmentation = self._get_compaction_details()
                        self.log.info("{2}: stats compaction: ({0},{1})".format(new_compaction_revision, fragmentation,
                                                                                self.design_doc_name))
                        curr_disk_size = self._get_disk_size()
                        self.log.info("Disk Size went from {0} {1}".format(self.disk_size, curr_disk_size))
                        if new_compaction_revision > self.compaction_revision and self.precompacted_fragmentation > fragmentation:
                            self.log.warn("the compaction revision was increase and fragmentation value went from {0} {1}".
                                          format(self.precompacted_fragmentation, fragmentation))
                            self.set_result(True)
                            self.state = FINISHED
                            return
                        elif new_compaction_revision > self.compaction_revision and self.with_rebalance:
                            self.log.warn("the compaction revision was increased, but the actual fragmentation value has not changed significantly")
                            self.set_result(True)
                            self.state = FINISHED
                            return
                        else:
                            continue
                self.log.info("design doc {0} is compacting:{1}".format(self.design_doc_name, self._is_compacting()))
                new_compaction_revision, fragmentation = self._get_compaction_details()
                self.log.error("stats compaction still: ({0},{1})".
                               format(new_compaction_revision, fragmentation))
                status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
                stats = content["stats"]
                self.log.warn("general compaction stats:{0}".format(stats))
                self.state = FINISHED
                self.set_result(False)
                self.set_exception(Exception("Check system logs, looks like compaction failed to start"))
        except (SetViewInfoNotFound) as ex:
            self.state = FINISHED
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _get_disk_size(self):
        nodes_ddoc_info = MonitorViewFragmentationTask.aggregate_ddoc_info(self.rest, self.design_doc_name,
                                                                           self.bucket, self.with_rebalance)
        disk_size = sum([content['disk_size'] for content in nodes_ddoc_info])
        return disk_size

class MonitorDiskSizeFragmentationTask(Task):
    def __init__(self, server, fragmentation_value=10, bucket="default", get_view_frag=False):
        Task.__init__(self, "monitor_frag_db_task")
        self.server = server
        self.bucket = bucket
        self.fragmentation_value = fragmentation_value
        self.get_view_frag = get_view_frag
        self.rest = RestConnection(self.server)
        self.curr_disk_size = 0

    def execute(self, task_manager):
        if  self.fragmentation_value < 0:
            err_msg = \
                "Invalid value for fragmentation %d" % self.fragmentation_value
            self.state = FINISHED
            self.set_exception(Exception(err_msg))
        self.state = CHECKING
        task_manager.schedule(self, 5)

    def check(self, task_manager):
        try:
            rest = RestConnection(self.server)
            stats = rest.fetch_bucket_stats(bucket=self.bucket)
            if self.get_view_frag:
                new_disk_size = stats["op"]["samples"]["couch_views_actual_disk_size"][-1]
            else:
                new_disk_size = stats["op"]["samples"]["couch_total_disk_size"][-1]
            if self.curr_disk_size > new_disk_size:
                self.state = FINISHED
                self.set_result(True)
            else:
                # try again
                task_manager.schedule(self, 5)
            self.log.info("New and Current Disk size is {0} {1}".format(new_disk_size, self.curr_disk_size))
            self.curr_disk_size = new_disk_size
        except Exception, ex:
            self.state = FINISHED
            self.set_result(False)
            self.set_exception(ex)

class CancelBucketCompactionTask(Task):

    def __init__(self, server, bucket="default"):
        Task.__init__(self, "cancel_bucket_compaction_task")
        self.server = server
        self.bucket = bucket
        self.retries = 20
        self.statuses = {}
        try:
            self.rest = RestConnection(server)
        except ServerUnavailableException, e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)

    def execute(self, task_manager):
        try:
            status = self.rest.cancel_bucket_compaction(self.bucket)
            self.state = CHECKING
        except BucketCompactionException as e:
            self.log.error("Cancel Bucket compaction failed for unknown reason")
            self.set_exception(e)
            self.state = FINISHED
            self.set_result(False)
        task_manager.schedule(self)

    def check(self, task_manager):
        # check cancel bucket compaction status across all nodes
        nodes = self.rest.get_nodes()
        for node in nodes:
            last_status = self.statuses.get(node.id)
            try:
                rest = RestConnection(node)
            except ServerUnavailableException, e:
                self.log.error(e)
                self.state = FINISHED
                self.set_exception(e)
            running, progress = rest.check_compaction_status(self.bucket)
            if progress is None and last_status is False:
                # finished if previously detected running but not == 100%
                self.statuses[node.id] = True
            if running:
                self.log.info("Progress is {0}".format(progress))
                self.statuses[node.id] = (progress == 100)
        done = all(self.statuses.values())
        if done:
            self.log.info("Bucket Compaction Cancelled successfully")
            # task was completed sucessfully
            self.set_result(True)
            self.state = FINISHED
        else:
            if self.retries > 0:
                self.retries = self.retries - 1
                task_manager.schedule(self, 10)
            else:
                # never detected a compaction task running
                self.log.error("Bucket Compaction Cancellation not started")
                self.set_result(False)
                self.state = FINISHED

class EnterpriseBackupTask(Task):

    def __init__(self, backupset, objstore_provider, resume=False, purge=False, no_progress_bar=False,
                 cli_command_location='', cb_version=None, num_shards=''):
        Task.__init__(self, "enterprise_backup_task")
        self.backupset = backupset
        self.objstore_provider = objstore_provider
        self.resume = resume
        self.purge = purge
        self.no_progress_bar = no_progress_bar
        self.cli_command_location = cli_command_location
        self.cb_version = cb_version
        self.cluster_flag = "--host"
        self.num_shards=num_shards
        """ from couchbase version 4.6.x, --host flag is not supported """
        if self.cb_version is None:
            raise Exception("Need to pass Couchbase version to run correctly bk/rt ")
        elif self.cb_version[:5] in COUCHBASE_FROM_4DOT6:
            self.cluster_flag = "--cluster"
        self.output = []
        self.error = []
        try:
            self.remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        except Exception as e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)

    def execute(self, task_manager):
        try:
            args = "backup --archive {}{} ".format(self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else '', self.backupset.directory)
            args += "--repo {} ".format(self.backupset.name)
            args += "{} http://{}:{} ".format(self.cluster_flag, self.backupset.cluster_host.ip, self.backupset.cluster_host.port)
            args += "--username {} ".format(self.backupset.cluster_host.rest_username)
            args += "--password {} ".format(self.backupset.cluster_host.rest_password)
            args += "{} ".format(self.num_shards)
            args += "{} ".format('--obj-staging-dir ' + self.backupset.objstore_staging_directory if self.objstore_provider else '')
            args += "{} ".format('--obj-endpoint ' + self.backupset.objstore_endpoint if self.objstore_provider and self.backupset.objstore_endpoint else '')
            args += "{} ".format('--obj-region ' + self.backupset.objstore_region if self.objstore_provider and self.backupset.objstore_region else '')
            args += "{} ".format('--obj-access-key-id ' + self.backupset.objstore_access_key_id if self.objstore_provider and self.backupset.objstore_access_key_id else '')
            args += "{}".format('--obj-secret-access-key ' + self.backupset.objstore_secret_access_key if self.objstore_provider and self.backupset.objstore_secret_access_key else '')
            args += "{}".format(' --s3-force-path-style' if self.objstore_provider and self.objstore_provider.schema_prefix() == 's3://' else '')

            if self.resume:
                args += " --resume"
            if self.purge:
                args += " --purge"
            if self.no_progress_bar:
                args += " --no-progress-bar"
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception, e:
            self.log.error("Backup cluster failed for unknown reason")
            self.set_exception(e)
            self.state = FINISHED
            self.set_result(False)
        task_manager.schedule(self)

    def check(self, task_manager):
        if self.output:
            self.state = FINISHED
            self.set_result(self.output)
            self.remote_client.log_command_output(self.output, self.error)
        elif self.error:
            self.state = FINISHED
            self.set_result(self.error)
            self.remote_client.log_command_output(self.output, self.error)
        else:
            task_manager.schedule(self, 10)

class EnterpriseRestoreTask(Task):

    def __init__(self, backupset, objstore_provider, no_progress_bar=False, cli_command_location='', cb_version=None):
        Task.__init__(self, "enterprise_backup_task")
        self.backupset = backupset
        self.objstore_provider = objstore_provider
        self.no_progress_bar = no_progress_bar
        self.cli_command_location = cli_command_location
        self.cb_version = cb_version
        self.cluster_flag = "--host"
        """ from couchbase version 4.6.x, --host flag is not supported """
        if self.cb_version is None:
            raise Exception("Need to pass Couchbase version to run correctly bk/rt ")
        elif self.cb_version[:5] in COUCHBASE_FROM_4DOT6:
            self.cluster_flag = "--cluster"
        self.output = []
        self.error = []
        self.backups = backups
        self.start = start
        self.end = end
        try:
            self.remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        except Exception as e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)

    def execute(self, task_manager):
        try:
            try:
                backup_start = self.backups[int(self.start) - 1]
            except IndexError:
                backup_start = "{0}{1}".format(self.backups[-1], self.start)
            try:
                backup_end = self.backups[int(self.end) - 1]
            except IndexError:
                backup_end = "{0}{1}".format(self.backups[-1], self.end)

            args = "restore --archive {}{} ".format(self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else '', self.backupset.directory)
            args += "--repo {} ".format(self.backupset.name)
            args += "{} http://{}:{} ".format(self.cluster_flag, self.backupset.restore_cluster_host.ip, self.backupset.restore_cluster_host.port)
            args += "--username {} ".format(self.backupset.restore_cluster_host.rest_username)
            args += "--password {} ".format(self.backupset.restore_cluster_host.rest_password)
            args += "--start {} ".format(backup_start)
            args += "--end {} ".format(backup_end)
            args += "{} ".format('--obj-staging-dir ' + self.backupset.objstore_staging_directory if self.objstore_provider else '')
            args += "{} ".format('--obj-endpoint ' + self.backupset.objstore_endpoint if self.objstore_provider and self.backupset.objstore_endpoint else '')
            args += "{} ".format('--obj-region ' + self.backupset.objstore_region if self.objstore_provider and self.backupset.objstore_region else '')
            args += "{} ".format('--obj-access-key-id ' + self.backupset.objstore_access_key_id if self.objstore_provider and self.backupset.objstore_access_key_id else '')
            args += "{}".format('--obj-secret-access-key ' + self.backupset.objstore_secret_access_key if self.objstore_provider and self.backupset.objstore_secret_access_key else '')
            args += "{}".format(' --s3-force-path-style' if self.objstore_provider and self.objstore_provider.schema_prefix() == 's3://' else '')

            if self.no_progress_bar:
                args += " --no-progress-bar"
            if self.force_updates:
                args += " --force-updates"
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception, e:
            self.log.error("Restore failed for unknown reason")
            self.set_exception(e)
            self.state = FINISHED
            self.set_result(False)
        task_manager.schedule(self)

    def check(self, task_manager):
        if self.output:
            self.state = FINISHED
            self.set_result(self.output)
            self.remote_client.log_command_output(self.output, self.error)
        elif self.error:
            self.state = FINISHED
            self.set_result(self.error)
            self.remote_client.log_command_output(self.output, self.error)
        else:
            task_manager.schedule(self, 10)

class EnterpriseMergeTask(Task):

    def __init__(self, backup_host, backups=[], start=0, end=0, directory='', name='',
                 cli_command_location=''):
        Task.__init__(self, "enterprise_backup_task")
        self.backup_host = backup_host
        self.directory = directory
        self.name = name
        self.cli_command_location = cli_command_location
        self.output = []
        self.error = []
        self.backups = backups
        self.start = start
        self.end = end
        try:
            self.remote_client = RemoteMachineShellConnection(self.backup_host)
        except Exception, e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)

    def execute(self, task_manager):
        try:
            try:
                backup_start = self.backups[int(self.start) - 1]
            except IndexError:
                backup_start = "{0}{1}".format(self.backups[-1], self.start)
            try:
                backup_end = self.backups[int(self.end) - 1]
            except IndexError:
                backup_end = "{0}{1}".format(self.backups[-1], self.end)
            args = "merge --archive {0} --repo {1} --start {2} --end {3}".format(self.directory, self.name,
                                                                             backup_start, backup_end)
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception, e:
            self.log.error("Merge failed for unknown reason")
            self.set_exception(e)
            self.state = FINISHED
            self.set_result(False)
        task_manager.schedule(self)

    def check(self, task_manager):
        if self.output:
            self.state = FINISHED
            self.set_result(self.output)
            self.remote_client.log_command_output(self.output, self.error)
        elif self.error:
            self.state = FINISHED
            self.set_result(self.error)
            self.remote_client.log_command_output(self.output, self.error)
        else:
            task_manager.schedule(self, 10)

class EnterpriseCompactTask(Task):

    def __init__(self, backup_host, backup_to_compact, backups=[], directory='', name='',
                 cli_command_location=''):
        Task.__init__(self, "enterprise_backup_task")
        self.backup_host = backup_host
        self.backup_to_compact = backup_to_compact
        self.directory = directory
        self.name = name
        self.cli_command_location = cli_command_location
        self.output = []
        self.error = []
        self.backups = backups
        try:
            self.remote_client = RemoteMachineShellConnection(self.backup_host)
        except Exception, e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)

    def execute(self, task_manager):
        try:
            args = "compact --archive {0} --repo {1} --backup {2}".format(self.directory, self.name,
                                                                      self.backups[self.backup_to_compact])
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception, e:
            self.log.error("Compact failed for unknown reason")
            self.set_exception(e)
            self.state = FINISHED
            self.set_result(False)
        task_manager.schedule(self)

    def check(self, task_manager):
        if self.output:
            self.state = FINISHED
            self.set_result(self.output)
            self.remote_client.log_command_output(self.output, self.error)
        elif self.error:
            self.state = FINISHED
            self.set_result(self.error)
            self.remote_client.log_command_output(self.output, self.error)
        else:
            task_manager.schedule(self, 10)

class CBASQueryExecuteTask(Task):
    def __init__(self, server, cbas_endpoint, statement, mode=None, pretty=True):
        Task.__init__(self, "cbas_query_execute_task")
        self.server = server
        self.cbas_endpoint = cbas_endpoint
        self.statement = statement
        self.mode = mode
        self.pretty = pretty
        self.response = {}
        self.passed = True

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
            self.response = json.loads(rest.execute_statement_on_cbas(self.statement,
                                           self.mode, self.pretty, 70))
            if self.response:
                self.state = CHECKING
                task_manager.schedule(self)
            else:
                self.log.info("Some error")
                self.state = FINISHED
                self.passed = False
                self.set_result(False)
        # catch and set all unexpected exceptions

        except Exception as e:
            self.state = FINISHED
            self.passed = False
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
            if "errors" in self.response:
                errors = self.response["errors"]
            else:
                errors = None

            if "results" in self.response:
                results = self.response["results"]
            else:
                results = None

            if "handle" in self.response:
                handle = self.response["handle"]
            else:
                handle = None

            if self.mode != "async":
                if self.response["status"] == "success":
                    self.set_result(True)
                    self.passed = True
                else:
                    self.log.info(errors)
                    self.passed = False
                    self.set_result(False)
            else:
                if self.response["status"] == "started":
                    self.set_result(True)
                    self.passed = True
                else:
                    self.log.info(errors)
                    self.passed = False
                    self.set_result(False)
            self.state = FINISHED
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)


class AutoFailoverNodesFailureTask(Task):
    def __init__(self, master, servers_to_fail, failure_type, timeout,
                 pause=0, expect_auto_failover=True, timeout_buffer=3,
                 check_for_failover=True, failure_timers=None,
                 disk_timeout=0, disk_location=None, disk_size=200):
        Task.__init__(self, "AutoFailoverNodesFailureTask")
        self.master = master
        self.servers_to_fail = servers_to_fail
        self.num_servers_to_fail = self.servers_to_fail.__len__()
        self.itr = 0
        self.failure_type = failure_type
        self.timeout = timeout
        self.pause = pause
        self.expect_auto_failover = expect_auto_failover
        self.check_for_autofailover = check_for_failover
        self.start_time = 0
        self.timeout_buffer = timeout_buffer
        self.current_failure_node = self.servers_to_fail[0]
        self.max_time_to_wait_for_failover = self.timeout + \
                                             self.timeout_buffer + 60
        self.disk_timeout = disk_timeout
        self.disk_location = disk_location
        self.disk_size = disk_size
        if failure_timers is None:
            failure_timers = []
        self.failure_timers = failure_timers
        self.taskmanager = None
        self.rebalance_in_progress = False

    def execute(self, task_manager):
        self.taskmanager = task_manager
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status() == "running":
            self.rebalance_in_progress = True
        while self.has_next() and not self.done():
            self.next()
            if self.pause > 0 and self.pause > self.timeout:
                self.check(task_manager)
        if self.pause == 0 or 0 < self.pause < self.timeout:
            self.check(task_manager)
        self.state = FINISHED
        self.set_result(True)

    def check(self, task_manager):
        if not self.check_for_autofailover:
            self.state = EXECUTING
            return
        rest = RestConnection(self.master)
        max_timeout = self.timeout + self.timeout_buffer + self.disk_timeout
        if self.start_time == 0:
            message = "Did not inject failure in the system."
            rest.print_UI_logs(10)
            self.log.error(message)
            self.state = FINISHED
            self.set_result(False)
            self.set_exception(AutoFailoverException(message))
        if self.rebalance_in_progress:
            status, stop_time = self._check_if_rebalance_in_progress(120)
            if not status:
                if stop_time == -1:
                    message = "Rebalance already completed before failover " \
                              "of node"
                    self.log.error(message)
                    self.state = FINISHED
                    self.set_result(False)
                    self.set_exception(AutoFailoverException(message))
                elif stop_time == -2:
                    message = "Rebalance failed but no failed autofailover " \
                              "message was printed in logs"
                    self.log.warning(message)
                else:
                    message = "Rebalance not failed even after 2 minutes " \
                              "after node failure."
                    self.log.error(message)
                    rest.print_UI_logs(10)
                    self.state = FINISHED
                    self.set_result(False)
                    self.set_exception(AutoFailoverException(message))
            else:
                self.start_time = stop_time
        autofailover_initiated, time_taken = \
            self._wait_for_autofailover_initiation(
                self.max_time_to_wait_for_failover)
        if self.expect_auto_failover:
            if autofailover_initiated:
                if time_taken < max_timeout + 1:
                    self.log.info("Autofailover of node {0} successfully "
                                  "initiated in {1} sec".format(
                        self.current_failure_node.ip, time_taken))
                    rest.print_UI_logs(10)
                    self.state = EXECUTING
                else:
                    message = "Autofailover of node {0} was initiated after " \
                              "the timeout period. Expected  timeout: {1} " \
                              "Actual time taken: {2}".format(
                        self.current_failure_node.ip, self.timeout, time_taken)
                    self.log.error(message)
                    rest.print_UI_logs(10)
                    self.state = FINISHED
                    self.set_result(False)
                    self.set_exception(AutoFailoverException(message))
            else:
                message = "Autofailover of node {0} was not initiated after " \
                          "the expected timeout period of {1}".format(
                    self.current_failure_node.ip, self.timeout)
                rest.print_UI_logs(10)
                self.log.error(message)
                self.state = FINISHED
                self.set_result(False)
                self.set_exception(AutoFailoverException(message))
        else:
            if autofailover_initiated:
                message = "Node {0} was autofailed over but no autofailover " \
                          "of the node was expected".format(
                    self.current_failure_node.ip)
                rest.print_UI_logs(10)
                self.log.error(message)
                self.state = FINISHED
                self.set_result(False)
                self.set_exception(AutoFailoverException(message))
            else:
                self.log.info("Node not autofailed over as expected")
                rest.print_UI_logs(10)
                self.state = EXECUTING

    def has_next(self):
        return self.itr < self.num_servers_to_fail

    def next(self):
        if self.pause != 0:
            time.sleep(self.pause)
            if self.pause > self.timeout and self.itr != 0:
                rest = RestConnection(self.master)
                status = rest.reset_autofailover()
                self._rebalance()
                if not status:
                    self.state = FINISHED
                    self.set_result(False)
                    self.set_exception(Exception("Reset of autofailover "
                                                 "count failed"))
        self.current_failure_node = self.servers_to_fail[self.itr]
        self.log.info("before failure time: {}".format(time.ctime(time.time())))
        if self.failure_type == "enable_firewall":
            self._enable_firewall(self.current_failure_node)
        elif self.failure_type == "disable_firewall":
            self._disable_firewall(self.current_failure_node)
        elif self.failure_type == "restart_couchbase":
            self._restart_couchbase_server(self.current_failure_node)
        elif self.failure_type == "stop_couchbase":
            self._stop_couchbase_server(self.current_failure_node)
        elif self.failure_type == "start_couchbase":
            self._start_couchbase_server(self.current_failure_node)
        elif self.failure_type == "restart_network":
            self._stop_restart_network(self.current_failure_node,
                                       self.timeout + self.timeout_buffer + 30)
        elif self.failure_type == "restart_machine":
            self._restart_machine(self.current_failure_node)
        elif self.failure_type == "stop_memcached":
            self._stop_memcached(self.current_failure_node)
        elif self.failure_type == "start_memcached":
            self._start_memcached(self.current_failure_node)
        elif self.failure_type == "network_split":
            self._block_incoming_network_from_node(self.servers_to_fail[0],
                                                   self.servers_to_fail[
                                                       self.itr + 1])
            self.itr += 1
        elif self.failure_type == "disk_failure":
            self._fail_disk(self.current_failure_node)
        elif self.failure_type == "disk_full":
            self._disk_full_failure(self.current_failure_node)
        elif self.failure_type == "recover_disk_failure":
            self._recover_disk(self.current_failure_node)
        elif self.failure_type == "recover_disk_full_failure":
            self._recover_disk_full_failure(self.current_failure_node)
        self.log.info("Start time = {}".format(time.ctime(self.start_time)))
        self.itr += 1

    def _enable_firewall(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        RemoteUtilHelper.enable_firewall(node)
        self.log.info("Enabled firewall on {}".format(node))
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _disable_firewall(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_firewall()

    def _restart_couchbase_server(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        shell.restart_couchbase()
        shell.disconnect()
        self.log.info("Restarted the couchbase server on {}".format(node))
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _stop_couchbase_server(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        shell.disconnect()
        self.log.info("Stopped the couchbase server on {}".format(node))
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _start_couchbase_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.start_couchbase()
        shell.disconnect()
        self.log.info("Started the couchbase server on {}".format(node))

    def _stop_restart_network(self, node, stop_time):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        shell.stop_network(stop_time)
        shell.disconnect()
        self.log.info("Stopped the network for {0} sec and restarted the "
                      "network on {1}".format(stop_time, node))
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _restart_machine(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        command = "/sbin/reboot"
        shell.execute_command(command=command)
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _stop_memcached(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        o, r = shell.stop_memcached()
        self.log.info("Killed memcached. {0} {1}".format(o, r))
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _start_memcached(self, node):
        shell = RemoteMachineShellConnection(node)
        o, r = shell.start_memcached()
        self.log.info("Started back memcached. {0} {1}".format(o, r))
        shell.disconnect()

    def _block_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.log.info("Adding {0} into iptables rules on {1}".format(
            node1.ip, node2.ip))
        command = "iptables -A INPUT -s {0} -j DROP".format(node2.ip)
        shell.execute_command(command)
        self.start_time = time.time()

    def _fail_disk(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.unmount_partition(self.disk_location)
        success = True
        if output:
            for line in output:
                if self.disk_location in line:
                    success = False
        if success:
            self.log.info("Unmounted disk at location : {0} on {1}".format(self.disk_location, node.ip))
            self.start_time = time.time()
        else:
            self.log.info("Could not fail the disk at {0} on {1}".format(self.disk_location, node.ip))
            self.state = FINISHED
            self.set_exception(Exception("Could not fail the disk at {0} on {1}".format(self.disk_location, node.ip)))
            self.set_result(False)

    def _recover_disk(self, node):
        shell = RemoteMachineShellConnection(node)
        o,r = shell.mount_partition(self.disk_location)
        for line in o:
            if self.disk_location in line:
                self.log.info("Mounted disk at location : {0} on {1}".format(self.disk_location, node.ip))
                return
        self.set_exception(Exception("Could not mount disk at location {0} on {1}".format(self.disk_location, node.ip)))
        raise Exception()

    def _disk_full_failure(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.fill_disk_space(self.disk_location, self.disk_size)
        success = False
        if output:
            for line in output:
                if self.disk_location in line:
                    if "0 100% {0}".format(self.disk_location) in line:
                        success = True
        if success:
            self.log.info("Filled up disk Space at {0} on {1}".format(self.disk_location, node.ip))
            self.start_time = time.time()
        else:
            self.log.info("Could not fill the disk at {0} on {1}".format(self.disk_location, node.ip))
            self.state = FINISHED
            self.set_exception(Exception("Could not fill the disk at {0} on {1}".format(self.disk_location, node.ip)))

    def _recover_disk_full_failure(self, node):
        shell =  RemoteMachineShellConnection(node)
        delete_file = "{0}/disk-quota.ext3".format(self.disk_location)
        output, error = shell.execute_command("rm -f {0}".format(delete_file))
        self.log.info(output)
        if error:
            self.log.info(error)

    def _check_for_autofailover_initiation(self, failed_over_node):
        rest = RestConnection(self.master)
        ui_logs = rest.get_logs(10)
        ui_logs_text = [t["text"] for t in ui_logs]
        ui_logs_time = [t["serverTime"] for t in ui_logs]
        expected_log = "Starting failing over ['ns_1@{}']".format(
            failed_over_node.ip)
        if expected_log in ui_logs_text:
            failed_over_time = ui_logs_time[ui_logs_text.index(expected_log)]
            return True, failed_over_time
        return False, None

    def _wait_for_autofailover_initiation(self, timeout):
        autofailover_initated = False
        while time.time() < timeout + self.start_time:
            autofailover_initated, failed_over_time = \
                self._check_for_autofailover_initiation(
                    self.current_failure_node)
            if autofailover_initated:
                end_time = self._get_mktime_from_server_time(failed_over_time)
                time_taken = end_time - self.start_time
                return autofailover_initated, time_taken
        return autofailover_initated, -1

    def _get_mktime_from_server_time(self, server_time):
        time_format = "%Y-%m-%dT%H:%M:%S"
        server_time = server_time.split('.')[0]
        mk_time = time.mktime(time.strptime(server_time, time_format))
        return mk_time

    def _rebalance(self):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes])
        rebalance_progress = rest.monitorRebalance()
        if not rebalance_progress:
            self.set_result(False)
            self.state = FINISHED
            self.set_exception(Exception("Failed to rebalance after failover"))

    def _check_if_rebalance_in_progress(self, timeout):
        rest = RestConnection(self.master)
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                rebalance_status, progress = \
                    rest._rebalance_status_and_progress()
                if rebalance_status == "running":
                    continue
                elif rebalance_status is None and progress == 100:
                    return False, -1
            except RebalanceFailedException:
                ui_logs = rest.get_logs(10)
                ui_logs_text = [t["text"] for t in ui_logs]
                ui_logs_time = [t["serverTime"] for t in ui_logs]
                rebalace_failure_log = "Rebalance exited with reason"
                for ui_log in ui_logs_text:
                    if rebalace_failure_log in ui_log:
                        rebalance_failure_time = ui_logs_time[
                            ui_logs_text.index(ui_log)]
                        failover_log = "Could not automatically fail over " \
                                       "node ('ns_1@{}'). Rebalance is " \
                                       "running.".format(
                            self.current_failure_node.ip)
                        if failover_log in ui_logs_text:
                            return True, self._get_mktime_from_server_time(
                                rebalance_failure_time)
                        else:
                            return False, -2
        return False, -3


class NodeDownTimerTask(Task):
    def __init__(self, node, port=None, timeout=300):
        Task.__init__(self, "NodeDownTimerTask")
        self.log.info("Initializing NodeDownTimerTask")
        self.node = node
        self.port = port
        self.timeout = timeout
        self.start_time = 0

    def execute(self, task_manager):
        self.log.info("Starting execution of NodeDownTimerTask")
        end_task = time.time() + self.timeout
        while not self.done() and time.time() < end_task:
            if not self.port:
                try:
                    self.start_time = time.time()
                    response = os.system("ping -c 1 {} > /dev/null".format(
                        self.node))
                    if response != 0:
                        self.log.info("Injected failure in {}. Caught "
                                      "due to ping".format(self.node))
                        self.state = FINISHED
                        self.set_result(True)
                        break
                except Exception as e:
                    self.log.warning("Unexpected exception caught {"
                                     "}".format(e))
                    self.state = FINISHED
                    self.set_result(True)
                    break
                try:
                    self.start_time = time.time()
                    socket.socket().connect(("{}".format(self.node), 8091))
                    socket.socket().close()
                    socket.socket().connect(("{}".format(self.node), 11210))
                    socket.socket().close()
                except socket.error:
                    self.log.info("Injected failure in {}. Caught due "
                                  "to ports".format(self.node))
                    self.state = FINISHED
                    self.set_result(True)
                    break
            else:
                try:
                    self.start_time = time.time()
                    socket.socket().connect(("{}".format(self.node),
                                             int(self.port)))
                    socket.socket().close()
                    socket.socket().connect(("{}".format(self.node), 11210))
                    socket.socket().close()
                except socket.error:
                    self.log.info("Injected failure in {}".format(self.node))
                    self.state = FINISHED
                    self.set_result(True)
                    break
        if time.time() >= end_task:
            self.state = FINISHED
            self.set_result(False)
            self.log.info("Could not inject failure in {}".format(self.node))

    def check(self, task_manager):
        pass


class NodeMonitorsAnalyserTask(Task):

    def __init__(self, node, stop=False):
        Task.__init__(self, "NodeMonitorAnalyzerTask")
        self.command = "dict:to_list(node_status_analyzer:get_nodes())"
        self.master = node
        self.rest = RestConnection(self.master)
        self.stop = stop

    def execute(self, task_manager):
        while not self.done() and not self.stop:
            self.status, self.content = self.rest.diag_eval(self.command,
                                                            print_log=False)
            self.state = CHECKING

    def check(self, task_manager):
        if self.status and self.content:
            self.log.info("NodeStatus: {}".format(self.content))
            if not self.master.ip in self.content:
                self.set_result(False)
                self.state = FINISHED
                self.set_exception(Exception("Node status monitors does not "
                                             "contain the node status"))
                return
            time.sleep(1)
            self.state = EXECUTING
        else:
            raise Exception("Monitors not working correctly")
