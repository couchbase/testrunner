import copy
import json
import math
import os
import random
import re
import socket
import string
import time
import traceback
import sys
import subprocess
from functools import cmp_to_key
from http.client import IncompleteRead
from multiprocessing import Process, Manager, Semaphore
from threading import Thread
from pathlib import Path
import shutil
from typing import Dict

import crc32
import logger
import testconstants
from cb_tools.cbstats import Cbstats
from remote.remote_util import RemoteMachineShellConnection
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from couchbase_helper.document import DesignDocument
from couchbase_helper.documentgenerator import BatchedDocumentGenerator
from couchbase_helper.stats_tools import StatsCommon
from deepdiff import DeepDiff
from mc_bin_client import MemcachedError
from membase.api.exception import BucketCreationException
from membase.api.exception import N1QLQueryException, DropIndexException, CreateIndexException, \
    DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException, \
    GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, FailoverFailedException, \
    ServerUnavailableException, BucketFlushFailed, CBRecoveryFailedException, BucketCompactionException, \
    AutoFailoverException,NodesFailureException, ServerAlreadyJoinedException
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from memcacheConstants import ERR_NOT_FOUND, NotFoundError
from memcached.helper.data_helper import MemcachedClientHelper
from memcached.helper.kvstore import KVStore
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from tasks.future import Future
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, \
    THROUGHPUT_CONCURRENCY, ALLOW_HTP, CBAS_QUOTA, CLUSTER_QUOTA_RATIO

from TestInput import TestInputServer, TestInputSingleton
from lib.Cb_constants.CBServer import CbServer

try:
    CHECK_FLAG = False
    if (TestInputSingleton.input.param("testrunner_client", None) == testconstants.PYTHON_SDK) or \
        ((testconstants.TESTRUNNER_CLIENT in list(os.environ.keys())) and os.environ[testconstants.TESTRUNNER_CLIENT] == testconstants.PYTHON_SDK):
        try:
            from sdk_client import SDKSmartClient as VBucketAwareMemcached
            from sdk_client import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient
        except:
            from sdk_client3 import SDKSmartClient as VBucketAwareMemcached
            from sdk_client3 import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient
        if (TestInputSingleton.input.param("enable_sdk_logging", False)):
            import logging
            import couchbase

            logging.basicConfig(stream=sys.stderr, level=logging.INFO)
            couchbase.enable_logging()
    else:
        CHECK_FLAG = True
        from memcached.helper.data_helper import VBucketAwareMemcached, KVStoreAwareSmartClient
except Exception as e:
    CHECK_FLAG = False
    try:
        from sdk_client import SDKSmartClient as VBucketAwareMemcached
        from sdk_client import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient
    except:
        from sdk_client3 import SDKSmartClient as VBucketAwareMemcached
        from sdk_client3 import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient

# TODO: Setup stacktracer
# TODO: Needs "easy_install pygments"
# import stacktracer
# stacktracer.trace_start("trace.html",interval=30,auto=True) # Set auto flag to always update file!

from lib.capella.utils import CapellaAPI, ServerlessDatabase, ServerlessDataPlane
import lib.capella.utils as capella_utils

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

    def set_unexpected_exception(self, e, suffix=""):
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
                 services=None, gsi_type='forestdb'):
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
        except Exception as error:
            self.state = FINISHED
            print("debuging hanging issue task 127" + str(error))
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

        if int(info.port) in range(9091, 9991):
            self.state = FINISHED
            self.set_result(True)
            return

        self.quota = int(info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO)
        if self.index_quota_percent:
            self.index_quota = int((info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO) * \
                                   self.index_quota_percent // 100)
            rest.set_service_memoryQuota(service='indexMemoryQuota', username=username, \
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
            if "index" in set_services:
                self.log.info("quota for index service will be %s MB" % (index_quota))
                kv_quota -= index_quota
                self.log.info("set index quota to node %s " % self.server.ip)
                rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=index_quota)
            if "fts" in set_services:
                self.log.info("quota for fts service will be %s MB" % (fts_quota))
                kv_quota -= fts_quota
                self.log.info("set both index and fts quota at node %s " % self.server.ip)
                rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=fts_quota)
            if "cbas" in set_services:
                self.log.info("quota for cbas service will be %s MB" % (CBAS_QUOTA))
                kv_quota -= CBAS_QUOTA
                rest.set_service_memoryQuota(service="cbasMemoryQuota", memoryQuota=CBAS_QUOTA)
            if kv_quota < MIN_KV_QUOTA:
                raise Exception("KV RAM needs to be more than %s MB"
                                " at node  %s" % (MIN_KV_QUOTA, self.server.ip))
            if kv_quota < int(self.quota):
                self.quota = kv_quota

        rest.init_cluster_memoryQuota(username, password, self.quota)

        if self.services:
            status = rest.init_node_services(username=username, password=password, \
                                             port=self.port, hostname=self.server.ip, \
                                             services=self.services)
            if not status:
                self.state = FINISHED
                self.set_exception(Exception('unable to set services for server %s' \
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

        if self.server.internal_ip:
            rest.set_alternate_address(self.server.ip)

        rest.init_cluster(username, password, self.port)
        remote_shell = RemoteMachineShellConnection(self.server)
        remote_shell.enable_diag_eval_on_non_local_hosts()
        remote_shell.disconnect()
        if rest.is_cluster_compat_mode_greater_than(4.0):
            if self.gsi_type == "plasma":
                if (not rest.is_cluster_compat_mode_greater_than(5.0)) or (not rest.is_enterprise_edition()):
                    rest.set_indexer_storage_mode(username, password, "forestdb")
                else:
                    rest.set_indexer_storage_mode(username, password, self.gsi_type)
            else:
                rest.set_indexer_storage_mode(username, password, self.gsi_type)
        self.server.port = self.port
        try:
            rest = RestConnection(self.server)
        except Exception as error:
            self.state = FINISHED
            print("debuging hanging issue task 230" + str(error))
            self.set_exception(error)
            return
        info = rest.get_nodes_self()

        if info is None:
            self.state = FINISHED
            self.set_exception(
                Exception('unable to get information on a server %s, it is available?' % (self.server.ip)))
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
        self.storageBackend = bucket_params['bucket_storage']

        if 'maxTTL' in bucket_params:
            self.maxttl = bucket_params['maxTTL']
        else:
            self.maxttl = 0
        if 'compressionMode' in bucket_params:
            self.compressionMode = bucket_params['compressionMode']
        else:
            self.compressionMode = 'passive'
        self.flush_enabled = bucket_params['flush_enabled']
        if bucket_params['bucket_priority'] is None or bucket_params['bucket_priority'].lower() == 'low':
            self.bucket_priority = 3
        else:
            self.bucket_priority = 8

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
        except Exception as error:
            self.state = FINISHED
            print("debuging hanging issue task 279" + str(error))
            self.set_exception(error)
            return
        info = rest.get_nodes_self()

        if self.size is None or int(self.size) <= 0:
            self.size = info.memoryQuota * 2 // 3

        if int(info.port) in range(9091, 9991):
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
            if (float(version[:2]) >= 3.0 or float(version[:2]) == 0.0) \
                    and self.bucket_priority is not None:
                rest.create_bucket(bucket=self.bucket,
                                   ramQuotaMB=self.size,
                                   replicaNumber=self.replicas,
                                   proxyPort=self.port,
                                   bucketType=self.bucket_type,
                                   replica_index=self.enable_replica_index,
                                   flushEnabled=self.flush_enabled,
                                   evictionPolicy=self.eviction_policy,
                                   threadsNumber=self.bucket_priority,
                                   lww=self.lww,
                                   maxTTL=self.maxttl,
                                   compressionMode=self.compressionMode,
                                   storageBackend=self.storageBackend)
            else:
                rest.create_bucket(bucket=self.bucket,
                                   ramQuotaMB=self.size,
                                   replicaNumber=self.replicas,
                                   proxyPort=self.port,
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
            if int(self.port) in range(9091, 9991):
                self.set_result(True)
                self.state = FINISHED
                return
            if BucketOperationHelper.wait_for_memcached(self.server, self.bucket):
                self.log.info("bucket '{0}' was created with per node RAM quota: {1}".format(self.bucket, self.size))
                self.set_result(True)
                self.state = FINISHED
                return
            else:
                self.log.warning("vbucket map not ready after try {0}".format(self.retries))
                if self.retries >= 5:
                    self.set_result(False)
                    self.state = FINISHED
                    return
        except Exception as e:
            self.log.error("Unexpected error: %s" % str(e))
            self.log.warning("vbucket map not ready after try {0}".format(self.retries))
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


class CollectionCreateTask(Task):
    def __init__(self, server, bucket, scope, collection, params=None):
        Task.__init__(self, "collection_create_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope
        self.collection_name = collection
        self.collection_params = params

    def execute(self, task_manager):
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            rest.create_collection(bucket=self.bucket_name, scope=self.scope_name,
                                                           collection=self.collection_name,
                                                           params=self.collection_params)
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class ConcurrentIndexCreateTask(Task):
    def __init__(self, server, bucket, scope, collection, query_definitions=None, IndexTrackingObject=None,
                 n1ql_helper=None, num_indexes=1, defer_build="", itr=0, expected_failure=[],
                 query_def_group="plasma_test"):
        Task.__init__(self, "collection_create_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope
        self.collection_name = collection
        self.query_definitions = query_definitions
        self.test_fail = False
        self.index_tracking_obj = IndexTrackingObject
        self.n1ql_helper=n1ql_helper
        self.num_indexes = num_indexes
        self.defer_build = defer_build
        self.itr = itr
        self.expected_failure = expected_failure
        self.query_def_group = query_def_group

    def execute(self, task_manager):
        try:
            RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            itr = self.itr
            while itr < (self.num_indexes + self.itr) and not self.index_tracking_obj.get_stop_create_index():
                for query_def in self.query_definitions:
                    if itr >= (self.num_indexes + self.itr):
                        break
                    if self.query_def_group in query_def.groups:
                        query_def_copy = copy.deepcopy(query_def)
                        index_name = query_def_copy.get_index_name()
                        index_name = index_name + str(itr)
                        query_def_copy.update_index_name(index_name)
                        if self.defer_build == "":
                            defer_build = random.choice([True, False])
                        else:
                            defer_build = self.defer_build
                        index_meta = {"name": query_def_copy.get_index_name(), "query_def": query_def_copy,
                                      "defer_build": defer_build}
                        if "primary" in query_def.groups:
                            query = query_def_copy.generate_primary_index_create_query(defer_build=defer_build)
                        else:
                            query = query_def_copy.generate_index_create_query(use_gsi_for_secondary=True, gsi_type="plasma",
                                                                          defer_build=defer_build)
                        try:
                            # create index
                            self.n1ql_helper.run_cbq_query(query=query, server=self.server)
                            self.index_tracking_obj.all_indexes_metadata(index_meta=index_meta, operation="create",
                                                                          defer_build=defer_build)
                        except Exception as err:
                            if not any(error in str(err) for error in self.expected_failure) \
                                    and "Build Already In Progress" not in str(err) \
                                    and "Timeout 1ms exceeded" not in str(err):
                                error_map = {"query": query, "error": str(err)}
                                self.index_tracking_obj.update_errors(error_map)
                            elif not any(error in str(err) for error in self.expected_failure):
                                self.index_tracking_obj.all_indexes_metadata(index_meta=index_meta, operation="create",
                                                                              defer_build=defer_build)

                        itr += 1
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)
            task_manager.schedule(self)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)

class CollectionDeleteTask(Task):
    def __init__(self, server, bucket, scope, collection):
        Task.__init__(self, "collection_delete_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope
        self.collection_name = collection

    def execute(self, task_manager):
        try:
            RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            CollectionsRest(self.server).delete_collection(bucket=self.bucket_name, scope=self.scope_name,
                                                           collection=self.collection_name)
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class ScopeCollectionCreateTask(Task):
    def __init__(self, server, bucket, scope, collection, params=None):
        Task.__init__(self, "collection_create_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope
        self.collection_name = collection
        self.collection_params = params

    def execute(self, task_manager):
        try:
            RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            CollectionsRest(self.server).create_scope_collection(bucket=self.bucket_name, scope=self.scope_name,
                                                                 collection=self.collection_name,
                                                                 params=self.collection_params)
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class ScopeCollectionDeleteTask(Task):
    def __init__(self, server, bucket, scope, collection):
        Task.__init__(self, "collection_delete_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope
        self.collection_name = collection

    def execute(self, task_manager):
        try:
            RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            CollectionsRest(self.server).delete_scope_collection(bucket=self.bucket_name, scope=self.scope_name,
                                                                 collection=self.collection_name)
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class ScopeCreateTask(Task):
    def __init__(self, server, bucket, scope, params=None):
        Task.__init__(self, "scope_create_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope
        self.scope_params = params

    def execute(self, task_manager):
        try:
            RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            CollectionsRest(self.server).create_scope(bucket=self.bucket_name, scope=self.scope_name,
                                                      params=self.scope_params)
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class ScopeDeleteTask(Task):
    def __init__(self, server, bucket, scope):
        Task.__init__(self, "scope_delete_task")
        self.server = server
        self.bucket_name = bucket
        self.scope_name = scope

    def execute(self, task_manager):
        try:
            RestConnection(self.server)
        except ServerUnavailableException as error:
            self.state = FINISHED
            self.set_exception(error)
            return
        try:
            CollectionsRest(self.server).delete_scope(bucket=self.bucket_name, scope=self.scope_name)
            self.state = CHECKING
            task_manager.schedule(self)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class CapellaRebalanceTask(Task):
    def __init__(self, to_add, to_remove, services, cluster_config):
        Task.__init__(self, "CapellaRebalanceTask")

        provider, _, compute, disk_type, disk_iops, disk_size, _ = capella_utils.spec_options_from_input(TestInputSingleton.input)

        # get current servers list
        # remove to_remove
        # add to_add

        self.cluster_id = CbServer.capella_cluster_id
        self.capella_api = CapellaAPI(CbServer.capella_credentials)

        servers = self.capella_api.get_nodes_formatted(self.cluster_id)

        final_servers = []
        remove_ips = [server.ip for server in to_remove]

        for server in servers:
            if server.ip not in remove_ips:
                final_servers.append(server)

        for i, server in enumerate(to_add):
            if services:
                server.services = services[i] or ["kv"]
            final_servers.append(server)

        services_count = capella_utils.get_service_counts(final_servers)
        scale_params = capella_utils.create_specs(provider, services_count, compute, disk_type, disk_iops, disk_size)
        self.scale_params = {"specs": scale_params}
        self.cluster_config = cluster_config

    def execute(self, task_manager):
        try:
            self.capella_api.update_specs(self.cluster_id, self.scale_params)
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)

    def check(self, task_manager):
        complete = self.capella_api.wait_for_cluster_step(self.cluster_id, "Scaling")
        if complete:
            servers = self.capella_api.get_nodes_formatted(self.cluster_id, CbServer.rest_username, CbServer.rest_password)
            self.cluster_config.update_servers(servers)
            self.state = FINISHED
            self.set_result(True)
        else:
            task_manager.schedule(self, 10)

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
        if services is not None and not services:
            services = ["kv"]
        self.services = services
        self.monitor_vbuckets_shuffling = False
        self.sleep_before_rebalance = sleep_before_rebalance

        try:
            self.rest = RestConnection(self.servers[0])
        except ServerUnavailableException as e:
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
                non_swap_servers = (node for node in self.servers if node not in self.to_add and node not in self.to_remove)
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
                        key = "{0}:{1}".format(remove_node.ip, remove_node.port)
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
                remote_ip = node.hostname
            else:
                remote_ip = node.cluster_ip
            try:
                self.rest.add_node(master.rest_username, master.rest_password,
                                remote_ip, node.port, services=services_for_node)
            except ServerAlreadyJoinedException:
                pass

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
            self.log.warning("cluster is mixed. sleep for 15 seconds before rebalance")
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
                        if not (len(self.old_vbuckets[srv][vb_type]) + 1 >= len(new_vbuckets[srv][vb_type]) and \
                                len(self.old_vbuckets[srv][vb_type]) - 1 <= len(new_vbuckets[srv][vb_type])):
                            msg = "Vbuckets were suffled! Expected %s for %s" % (vb_type, srv.ip) + \
                                  " are %s. And now are %s" % (
                                      len(self.old_vbuckets[srv][vb_type]),
                                      len(new_vbuckets[srv][vb_type]))
                            self.log.error(msg)
                            self.log.error("Old vbuckets: %s, new vbuckets %s" % (self.old_vbuckets, new_vbuckets))
                            raise Exception(msg)
            time.sleep(10)
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
        if self.rest.is_cluster_mixed(timeout=300): # See MB-40670
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
                # self.set_result(False)
                self.rest.print_UI_logs()
                self.set_exception(RebalanceFailedException( \
                    "seems like rebalance hangs. please check logs!"))
        else:
            success_cleaned = []
            for removed in self.to_remove:
                try:
                    rest = RestConnection(removed)
                except ServerUnavailableException as e:
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
                    except (ServerUnavailableException, IncompleteRead) as e:
                        self.log.error(e)
            result = True
            for node in set(self.to_remove) - set(success_cleaned):
                self.log.error("node {0}:{1} was not cleaned after removing from cluster" \
                               .format(node.ip, node.port))
                result = False

            self.log.info("rebalancing was completed with progress: {0}% in {1} sec".
                          format(progress, time.time() - self.start_time))

            for added in self.to_add:
                if added.internal_ip:
                    self.log.info("Adding alternate address {} after rebalance in using internal ip {}".format(added.ip, added.internal_ip))
                    rest = RestConnection(added)
                    rest.set_alternate_address(added.ip)


            self.state = FINISHED
            self.set_result(result)


class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, servers, bucket, param, stat, comparison, value, scope=None, collection=None):
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
        self.scope = scope
        self.collection = collection
        self.collection_stats = CollectionsStats(self.servers[0])

    def execute(self, task_manager):
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        stat_result = 0
        for server in self.servers:
            try:
                client = self._get_connection(server)
                stats = client.stats(self.param)
                if self.stat not in stats:
                    self.state = FINISHED
                    self.set_exception(Exception("Stat {0} not found".format(self.stat)))
                    return
                if stats[self.stat].isdigit():
                    stat_result += int(stats[self.stat])
                else:
                    stat_result = stats[self.stat]
            except EOFError as ex:
                self.state = FINISHED
                self.set_exception(ex)
                return
            # if self.stat != 'ep_queue_size':
            #     cbo_doc_items = self.collection_stats.get_collection_item_count_cumulative(self.bucket,
            #                                                                                CbServer.system_scope,
            #                                                                                CbServer.query_collection)
            #     stat_result = stat_result - cbo_doc_items

            if self.stat in ['curr_items', 'vb_active_curr_items', 'vb_replica_curr_items', 'curr_items_tot']:
                cbo_doc_items = self.collection_stats.get_collection_item_count_cumulative(self.bucket,
                                                                                           CbServer.system_scope,
                                                                                           CbServer.query_collection)
                stat_result = stat_result - cbo_doc_items
        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.warning("Not Ready: %s %s %s %s expected on %s, %s bucket" % (self.stat, stat_result,
                                                                                   self.comparison, self.value,
                                                                                   self._stringify_servers(),
                                                                                   self.bucket))
            task_manager.schedule(self, 5)
            return
        self.log.info("Saw %s %s %s %s expected on %s,%s bucket" % (self.stat, stat_result,
                                                                    self.comparison, self.value,
                                                                    self._stringify_servers(), self.bucket))

        for server, conn in list(self.conns.items()):
            conn.close()
        self.state = FINISHED
        self.set_result(True)

    def _stringify_servers(self):
        return ''.join([repr(server.ip + ":" + str(server.port)) for server in self.servers])

    def _get_connection(self, server):
        if server not in self.conns:
            for i in range(3):
                try:
                    self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket)
                    return self.conns[server]
                except (EOFError, socket.error):
                    self.log.error("failed to create direct client, retry in 1 sec")
                    time.sleep(1)
            self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket)
        return self.conns[server]

    def _compare(self, cmp_type, a, b):
        if isinstance(b, int) and a.isdigit():
            a = int(a)
        elif isinstance(b, int) and not a.isdigit():
            return False
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or \
                (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or \
                (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or \
                (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or \
                (cmp_type == StatsWaitTask.LESS_THAN and a < b) or \
                (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False


class XdcrStatsWaitTask(StatsWaitTask):
    def __init__(self, servers, bucket, param, stat, comparison, value, scope=None, collection=None):
        StatsWaitTask.__init__(self, servers, bucket, param, stat, comparison, value, scope, collection)

    def check(self, task_manager):
        stat_result = 0
        for server in self.servers:
            try:
                rest = RestConnection(server)
                stat = 'replications/' + rest.get_replication_for_buckets(self.bucket, self.bucket)[
                    'id'] + '/' + self.stat
                # just get the required value, don't fetch the big big structure of stats
                stats_value = rest.fetch_bucket_xdcr_stats(self.bucket)['op']['samples'][stat][-1]
                stat_result += int(stats_value)
            except (EOFError, Exception) as ex:
                self.state = FINISHED
                self.set_exception(ex)
                return
        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.warning("Not Ready: %s %s %s %s expected on %s, %s bucket" % (self.stat, stat_result,
                                                                                   self.comparison, self.value,
                                                                                   self._stringify_servers(),
                                                                                   self.bucket))
            task_manager.schedule(self, 5)
            return
        self.log.info("Saw %s %s %s %s expected on %s,%s bucket" % (self.stat, stat_result,
                                                                    self.comparison, self.value,
                                                                    self._stringify_servers(), self.bucket))

        for server, conn in list(self.conns.items()):
            conn.close()
        self.state = FINISHED
        self.set_result(True)


class GenericLoadingTask(Thread, Task):
    def __init__(self, server, bucket, kv_store, batch_size=1, pause_secs=1, timeout_secs=60, compression=True,
                 scope=None, collection=None):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task")
        self.kv_store = kv_store
        self.batch_size = batch_size
        self.pause = pause_secs
        self.timeout = timeout_secs
        self.server = server
        self.bucket = bucket
        self.collection = collection
        self.scope = scope
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
            next(self)
        self.state = FINISHED
        self.set_result(True)

    def has_next(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

    def _unlocked_create(self, partition, key, value, is_base64_value=False):
        try:
            value_json = json.loads(value)
            if isinstance(value_json, dict):
                value_json['mutated'] = 0
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(list(range(len(value))))
            if not is_base64_value:
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
        except TypeError:
            value = json.dumps(value)

        try:
            self.client.set(key, self.exp, self.flag, value,  scope=self.scope, collection=self.collection)

            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))
            partition.set(key, value, self.exp, self.flag)
        except Exception as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_read(self, partition, key):
        try:
            o, c, d = self.client.get(key,  scope=self.scope, collection=self.collection)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

    def _unlocked_replica_read(self, partition, key):
        try:
            o, c, d = self.client.getr(key, scope=self.scope, collection=self.collection)
        except Exception as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_update(self, partition, key):
        value = None
        try:
            o, c, value = self.client.get(key,  scope=self.scope, collection=self.collection)
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
        except (ValueError, json.JSONDecodeError) as e:
            if value is None:
                return
            index = random.choice(list(range(len(value))))
            value = str(value[0:index]) + str(random.choice(string.ascii_uppercase).encode()) + str(value[index + 1:])
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

        try:
            self.client.set(key, self.exp, self.flag, value, scope=self.scope, collection=self.collection)
            if self.only_store_hash:
                if value != None:
                    value = str(crc32.crc32_hash(value))
            partition.set(key, value, self.exp, self.flag)
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_delete(self, partition, key):
        try:
            self.client.delete(key, scope=self.scope, collection=self.collection)
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
            o, c, old_value = self.client.get(key, scope=self.scope, collection=self.collection)
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
            o, c, old_value = self.client.get(key, scope=self.scope, collection=self.collection)
            index = random.choice(list(range(len(value))))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            old_value += value
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

        try:
            self.client.append(key, value, scope=self.scope, collection=self.collection)
            if self.only_store_hash:
                old_value = str(crc32.crc32_hash(old_value))
            partition.set(key, old_value)
        except BaseException as error:
            self.state = FINISHED
            self.set_exception(error)

    # start of batch methods
    def _create_batch_client(self, key_val, shared_client=None):
        """
        standalone method for creating key/values in batch (sans kvstore)

        arguments:
            key_val -- array of key/value dicts to load size = self.batch_size
            shared_client -- optional client to use for data loading
        """
        try:
            self._process_values_for_create(key_val)
            client = shared_client or self.client

            client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False,
                            scope=self.scope, collection=self.collection)
        except (
                MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError,
                RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)

    def _create_batch(self, partition_keys_dic, key_val):
        self._create_batch_client(key_val)
        self._populate_kvstore(partition_keys_dic, key_val)

    def _update_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_update(partition_keys_dic, key_val)
            self.client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False,
                                 scope=self.scope, collection=self.collection)
            self._populate_kvstore(partition_keys_dic, key_val)
        except (
                MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError,
                RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)

    def _delete_batch(self, partition_keys_dic, key_val):
        for partition, keys in list(partition_keys_dic.items()):
            for key in keys:
                try:
                    self.client.delete(key, scope=self.scope, collection=self.collection)
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
            self.client.getMulti(list(key_val.keys()), self.pause, self.timeout, scope=self.scope,
                                 collection=self.collection)
            # print "the key is {} from collection {}".format(c, collection)
        except MemcachedError as error:
            self.state = FINISHED
            self.set_exception(error)

    def _process_values_for_create(self, key_val):
        for key, value in list(key_val.items()):
            try:
                value_json = json.loads(value)
                value_json['mutated'] = 0
                value = json.dumps(value_json)
            except ValueError:
                index = random.choice(list(range(len(value))))
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            except TypeError:
                value = json.dumps(value)
            finally:
                key_val[key] = value

    def _process_values_for_update(self, partition_keys_dic, key_val):
        for partition, keys in list(partition_keys_dic.items()):
            for key in keys:
                value = partition.get_valid(key)
                if value is None:
                    del key_val[key]
                    continue
                try:
                    value = key_val[
                        key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                    value_json = json.loads(value)
                    value_json['mutated'] += 1
                    value = json.dumps(value_json)
                except ValueError:
                    index = random.choice(list(range(len(value))))
                    value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
                finally:
                    key_val[key] = value

    def _populate_kvstore(self, partition_keys_dic, key_val):
        for partition, keys in list(partition_keys_dic.items()):
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
                 compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, batch_size=batch_size, pause_secs=pause_secs,
                                    timeout_secs=timeout_secs, compression=compression, scope=scope,
                                    collection=collection)

        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.flag = flag
        self.only_store_hash = only_store_hash
        self.scope = scope
        self.collection = collection

        if proxy_client:
            self.log.info("Changing client to proxy %s:%s..." % (proxy_client.host,
                                                                 proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):
        if self.batch_size == 1:
            key, value = next(self.generator)
            partition = self.kv_store.acquire_partition(key, self.bucket, self.scope, self.collection)
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
            self.kv_store.release_partition(key, self.bucket, self.scope, self.collection)

        else:
            doc_gen = override_generator or self.generator
            key_value = doc_gen.next_batch()

            partition_keys_dic = self.kv_store.acquire_partitions(list(key_value.keys()), self.bucket,
                                                                  self.scope, self.collection)
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
            self.kv_store.release_partitions(list(partition_keys_dic.keys()))


class LoadDocumentsGeneratorsTask(LoadDocumentsTask):
    def __init__(self, server, bucket, generators, kv_store, op_type, exp, flag=0, only_store_hash=True,
                 batch_size=1, pause_secs=1, timeout_secs=60, compression=True, scope=None, collection=None):
        LoadDocumentsTask.__init__(self, server, bucket, generators[0], kv_store, op_type, exp, flag=flag,
                                   only_store_hash=only_store_hash, batch_size=batch_size, pause_secs=pause_secs,
                                   timeout_secs=timeout_secs, compression=compression, scope=scope,
                                   collection=collection)

        if batch_size == 1:
            self.generators = generators
        else:
            self.generators = []
            for i in generators:
                if i.isGenerator():
                    self.generators.append(BatchedDocumentGenerator(i, batch_size))
                else:
                    self.generators.append(i)

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
        self.scope = scope
        self.collection = collection

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
            gen_range = max(int(gen.end / self.process_concurrency), 1)
            for pos in range(gen_start, gen_end, gen_range):
                try:
                    partition_gen = copy.deepcopy(gen)
                    partition_gen.start = pos
                    partition_gen.itr = pos
                    partition_gen.end = pos + gen_range
                    if partition_gen.end > gen.end:
                        partition_gen.end = gen.end
                    batch_gen = BatchedDocumentGenerator(
                        partition_gen,
                        self.batch_size)
                    self.generators.append(batch_gen)
                except Exception as e:
                    traceback.print_exc()

        iterator = 0
        all_processes = []
        for generator in self.generators:
            # only start processing when there resources available
            CONCURRENCY_LOCK.acquire()

            # add child process to wait queue
            self.wait_queue.put(iterator + 1)

            generator_process = Process(
                target=self.run_generator,
                args=(generator, iterator))
            generator_process.start()
            iterator += 1
            all_processes.append(generator_process)

        # wait for all child processes to finish
        self.wait_queue.join()

        # merge kvstore partitions
        while self.shared_kvstore_queue.empty() is False:

            # get partitions created by child process
            rv = self.shared_kvstore_queue.get()
            if rv["err"] is not None:
                self.state = FINISHED
                self.set_exception(rv["err"])
                return

            # merge child partitions with parent
            generator_partitions = rv["partitions"]
            self.kv_store.merge_partitions(generator_partitions)

            # terminate child process
            iterator -= 1
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
            try:
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
            except Exception as e:
                traceback.print_exc()

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
        for key, value in key_value.items():

            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))

            partition = store.partition(key, self.scope, self.collection, self.bucket)
            partition["partition"].set(
                key,
                value,
                self.exp,
                self.flag)


class ESLoadGeneratorTask(Task):
    """
        Class to load/update/delete documents into/from Elastic Search
    """

    def __init__(self, es_instance, index_name, generator, op_type="create", scope=None, collection=None):
        Task.__init__(self, "ES_loader_task")
        self.es_instance = es_instance
        self.index_name = index_name
        self.generator = generator
        self.iterator = 0
        self.scope = scope
        self.collection = collection
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
                                       key, self.scope, self.collection)
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
                 batch=1000, dataset=None,scope=None, collection=None):
        Task.__init__(self, "ES_loader_task")
        self.es_instance = es_instance
        self.index_name = index_name
        self.generator = generator
        self.iterator = 0
        self.op_type = op_type
        self.batch_size = batch
        self.scope = scope
        self.collection = collection
        self.log.info("Starting operation '%s' on Elastic Search ..." % op_type)
        self.dataset=dataset

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
                    "_id": key,
                }
            }
            if self.dataset == "geojson":
                es_doc[self.op_type]["_type"] = "_doc"
            else:
                es_doc[self.op_type]["_type"] = doc['type']

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
                    es_file.write("{}\n".format(line).encode())
                es_file.close()
                self.es_instance.load_bulk_data(es_filename,self.index_name)
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
    def __init__(self, fts_index, es_instance, query_index, es_index_name=None, n1ql_executor=None,
                 use_collections=False,dataset=None,reduce_query_logging=False,variable_node = None):
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
        self.llm_model = TestInputSingleton.input.param("llm_model", "all-MiniLM-L6-v2")
        self.use_collections = use_collections
        self.dataset = dataset
        self.reduce_query_logging = reduce_query_logging
        self.variable_node = variable_node

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(self.result)

    def is_geoshape_query(self):
        res = False
        try:
            res = 'geo_shape' in self.es_query['query']['bool']['filter'].keys()
        except Exception as e:
            self.log.info(str(e))
        return res

    def log_query_to_file(self,differing_hits):
        query_index = str(self.query_index)
        query_log_path = "query_" + query_index
        Path(query_log_path).mkdir(parents=True,exist_ok=True)
        print("Logging in path ",query_log_path)
        es_query_file = os.path.join(query_log_path,"es_query_" + query_index + ".json")
        with open(es_query_file,'w') as fp:
            json.dump(self.es_query,fp)

        fts_query_file = os.path.join(query_log_path,"fts_query_" + query_index + ".json")
        with open(fts_query_file,'w') as fp:
            json.dump(self.fts_query,fp)

        if len(differing_hits.items()) > 0:
            differing_hits_file = os.path.join(query_log_path,"differing_hits.json")
            with open(differing_hits_file,'w') as fp:
                json.dump(differing_hits,fp)

        shutil.make_archive(query_log_path,'zip',query_log_path)

    def execute(self, task_manager):
        self.es_compare = True
        should_verify_n1ql = True
        try:
            if not self.reduce_query_logging:
                self.log.info("---------------------------------------"
                              "-------------- Query # %s -------------"
                              "---------------------------------------"
                              % str(self.query_index + 1))
            if "vector" in str(self.fts_query):
                from sentence_transformers import SentenceTransformer
                encoder = SentenceTransformer(self.llm_model)
                vector_query = self.fts_query["vector"]
                self.log.info(f"Searching for --> {vector_query}")
                k = self.fts_query["k"]
                search_vector = encoder.encode(vector_query)
                self.fts_query["vector"] = search_vector.tolist()
            try:
                fts_hits, fts_doc_ids, fts_time, fts_status = \
                    self.run_fts_query(self.fts_query, self.score)
                if "vector" in str(self.fts_query):
                    self.log.info(fts_doc_ids)
                if not self.reduce_query_logging:
                    self.log.info("Status: %s" % fts_status)
                elif fts_status == 'fail':
                    if str(fts_doc_ids).find("limiting/throttling: the request has been rejected according to regulator") == -1:
                        self.log.error(f"Query failed abruptly, reason : {fts_doc_ids}")

                if fts_status == 'fail':
                    error = fts_doc_ids
                    if "err: TooManyClauses over field" in str(error):
                        self.log.info("FTS chose not to run this big query"
                                      "...skipping ES validation")
                        self.passed = True
                        self.es_compare = False
                        should_verify_n1ql = False
                elif fts_hits < 0:
                    self.passed = False
                elif 'errors' in list(fts_status.keys()) and fts_status['errors']:
                    if fts_status['successful'] == 0 and \
                            (list(set(fts_status['errors'].values())) ==
                             ['context deadline exceeded'] or
                             "TooManyClauses" in str(list(set(fts_status['errors'].values())))):
                        # too many clauses in the query for fts to process
                        self.log.info("FTS chose not to run this big query"
                                      "...skipping ES validation")
                        self.passed = True
                        self.es_compare = False
                        should_verify_n1ql = False
                    elif 0 < fts_status['successful'] < \
                            self.fts_index.num_pindexes:
                        # partial results
                        self.log.info("FTS returned partial results..."
                                      "skipping ES validation")
                        self.passed = True
                        self.es_compare = False
                if not self.reduce_query_logging:
                    self.log.info("FTS hits for query: %s is %s (took %sms)" % \
                                  (json.dumps(self.fts_query, ensure_ascii=False),
                                   fts_hits,
                                   float(fts_time) / 1000000))
                else:
                    if fts_hits < 0:
                        self.log.info("FTS hits for query %s : %s is %s (took %sms)" % \
                                      (str(self.query_index + 1),
                                       json.dumps(self.fts_query, ensure_ascii=False),
                                       fts_hits,
                                       float(fts_time) / 1000000))

            except ServerUnavailableException:
                self.log.error("ERROR: FTS Query timed out (client timeout=70s)!")
                self.passed = False
            es_hits = 0
            if self.es and self.es_query and "vector" not in self.fts_query:
                es_hits, es_doc_ids, es_time = self.run_es_query(self.es_query,dataset=self.dataset)
                self.log.info("ES hits for query: %s on %s is %s (took %sms)" % \
                              (json.dumps(self.es_query, ensure_ascii=False),
                               self.es_index_name,
                               es_hits,
                               es_time))
                if self.passed and self.es_compare:
                    if type(es_hits) == dict:
                        es_hits = es_hits['value']
                        # added since newer versions of ES return results in this format:
                        # {'value': 0, 'relation': 'eq'}
                    if int(es_hits) != int(fts_hits):
                        msg = "FAIL: FTS hits: %s, while ES hits: %s" \
                              % (fts_hits, es_hits)
                        self.log.error(msg)
                    es_but_not_fts = list(set(es_doc_ids) - set(fts_doc_ids))
                    fts_but_not_es = list(set(fts_doc_ids) - set(es_doc_ids))
                    if not (es_but_not_fts or fts_but_not_es):
                        self.log.info("SUCCESS: Docs returned by FTS = docs"
                                      " returned by ES, doc_ids verified")
                    else:
                        differing_results = dict()
                        if fts_but_not_es:
                            msg = "FAIL: Following %s doc(s) were not returned" \
                                  " by ES,but FTS, printing 50: %s" \
                                  % (len(fts_but_not_es), fts_but_not_es[:50])
                            if self.is_geoshape_query():
                                differing_results['fts_but_not_es'] = fts_but_not_es
                        else:
                            msg = "FAIL: Following %s docs were not returned" \
                                  " by FTS, but ES, printing 50: %s" \
                                  % (len(es_but_not_fts), es_but_not_fts[:50])
                            if self.is_geoshape_query():
                                differing_results['es_but_not_fts'] = es_but_not_fts
                        if self.is_geoshape_query():
                            self.log_query_to_file(differing_results)
                        self.log.error(msg)
                        self.passed = False

            if fts_hits <= 0 and es_hits == 0:
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
                if self.use_collections:
                    kv_container = "default:default.scope1.collection1"
                else:
                    kv_container = "default"

                search_query = self.fts_query
                if "vector" in self.fts_query:
                    search_query = {}
                    search_query["query"] = {"match_none": {}}
                    search_query["explain"] = True
                    search_query["knn"] = [self.fts_query]
                n1ql_queries = [f"select meta().id from {kv_container} where type='" + str(
                    query_type) + "' and search(default, " + str(
                    json.dumps(search_query, ensure_ascii=False)) + ")", f"select meta().id from {kv_container} where type='" + str(
                    query_type) + "' and search(default, " + str(
                    json.dumps(search_query, ensure_ascii=False)) + ",{\"index\": \"" + self.fts_index.name + "\"})", f"select meta().id,* from {kv_container} where type='" + str(
                    query_type) + "' and search(default, " + str(
                    json.dumps(search_query, ensure_ascii=False)) + ",{\"index\": \"" + self.fts_index.name + "\"})"]
                for n1ql_query in n1ql_queries:
                    if ("disjuncts" not in n1ql_query and "-" not in n1ql_query) or "\"index\"" in n1ql_query:
                        self.log.info("Running N1QL query: " + str(n1ql_query))
                        n1ql_result = self.n1ql_executor.run_n1ql_query(query=n1ql_query)
                        if n1ql_result['status'] == 'success':
                            n1ql_hits = n1ql_result['metrics']['resultCount']
                            n1ql_doc_ids = []
                            for res in n1ql_result['results']:
                                n1ql_doc_ids.append(res['id'])
                            n1ql_time = n1ql_result['metrics']['elapsedTime']

                            self.log.info("N1QL hits for query: %s is %s (took %s)" % \
                                          (json.dumps(n1ql_query, ensure_ascii=False),
                                           n1ql_hits,
                                           n1ql_time))
                            if self.passed:
                                if int(n1ql_hits) != int(fts_hits):
                                    msg = "FAIL: FTS hits: %s, while N1QL hits: %s" \
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
            if "vector" in str(self.fts_query):
                import faiss
                import numpy as np
                _vector = np.array([search_vector])
                faiss.normalize_L2(_vector)
                distances, ann = self.fts_index.faiss_index.search(_vector, k=5*k)
                print("Results from faiss index --> ", distances, ann)
                faiss_doc_ids = [f"emp{10000000+res+1}" for res in ann[0]]
                print("Docids from faiss index --> ", faiss_doc_ids)
                if int(len(faiss_doc_ids)/5) != int(fts_hits):
                    msg = "FAIL: FTS hits: %s, while FAISS hits: %s" \
                          % (fts_hits, faiss_doc_ids)
                    self.log.error(msg)


                faiss_hits_ids = list(set(faiss_doc_ids))
                fts_hits_ids = list(set(fts_doc_ids))

                if all(elem in faiss_hits_ids for elem in fts_hits_ids):
                    self.log.info("SUCCESS: Docs returned by FTS = docs returned by FAISS, doc_ids verified")
                else:
                    missing_ids = [elem for elem in fts_hits_ids if elem not in faiss_hits_ids]
                    self.log.error(f"FAIL: Docs returned by FTS are not present in FAISS. Missing ids : {missing_ids}")
                    self.passed = False





            self.state = CHECKING
            task_manager.schedule(self)

            if not should_verify_n1ql and self.n1ql_executor:
                self.log.info("Skipping N1QL result validation since FTS results are - " + str(
                    fts_hits) + " and es results are - " + str(es_hits) + ".")

        except Exception as e:
            self.log.error(e)
            self.set_exception(e)
            self.state = FINISHED
    def run_fts_query(self, query, score=''):
        return self.fts_index.execute_query(query, score=score,variable_node=self.variable_node)

    def run_es_query(self, query,dataset=None):
        return self.es.search(index_name=self.es_index_name, query=query,dataset=dataset)


# This will be obsolete with the implementation of batch operations in LoadDocumentsTaks
class BatchedLoadDocumentsTask(GenericLoadingTask):
    def __init__(self, server, bucket, generator, kv_store, op_type, exp, flag=0, only_store_hash=True,
                 batch_size=100, pause_secs=1, timeout_secs=60, compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression, scope=scope,
                                    collection=collection)
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
        self.scope=scope
        self.collection = collection

    def has_next(self):
        has = self.batch_generator.has_next()
        if math.fmod(self.batch_generator._doc_gen.itr, 50000) == 0.0 or not has:
            self.log.info("Batch {0} documents queued #: {1} with exp:{2} @ {3}, bucket {4}". \
                          format(self.op_type,
                                 (self.batch_generator._doc_gen.itr - self.batch_generator._doc_gen.start),
                                 self.exp,
                                 self.server.ip,
                                 self.bucket))
        return has

    def __next__(self):
        key_value = self.batch_generator.next_batch()
        partition_keys_dic = self.kv_store.acquire_partitions(list(key_value.keys()), self.bucket, self.scope,
                                                              self.collection)
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
        self.kv_store.release_partitions(list(partition_keys_dic.keys()), self.scope, self.collection)

    def _create_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_create(key_val)
            self.client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False,
                                 scope=self.scope, collection=self.collection)
            self._populate_kvstore(partition_keys_dic, key_val)
        except (
                MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError,
                RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)

    def _update_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_update(partition_keys_dic, key_val)
            self.client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False,
                                 scope=self.scope, collection=self.collection)
            self._populate_kvstore(partition_keys_dic, key_val)
        except (
                MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError,
                RuntimeError) as error:
            self.state = FINISHED
            self.set_exception(error)

    def _delete_batch(self, partition_keys_dic, key_val):
        for partition, keys in list(partition_keys_dic.items()):
            for key in keys:
                try:
                    self.client.delete(key, scope=self.scope, collection=self.collection)
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
            self.client.getMulti(list(key_val.keys()), self.pause, self.timeout, scope=self.scope,
                                 collection=self.collection)
        except MemcachedError as error:
            self.state = FINISHED
            self.set_exception(error)

    def _process_values_for_create(self, key_val):
        for key, value in list(key_val.items()):
            try:
                value_json = json.loads(value)
                value_json['mutated'] = 0
                value = json.dumps(value_json)
            except ValueError:
                index = random.choice(list(range(len(value))))
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            finally:
                key_val[key] = value

    def _process_values_for_update(self, partition_keys_dic, key_val):
        for partition, keys in list(partition_keys_dic.items()):
            for key in keys:
                value = partition.get_valid(key)
                if value is None:
                    del key_val[key]
                    continue
                try:
                    value = key_val[
                        key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                    value_json = json.loads(value)
                    value_json['mutated'] += 1
                    value = json.dumps(value_json)
                except ValueError:
                    index = random.choice(list(range(len(value))))
                    value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
                finally:
                    key_val[key] = value

    def _populate_kvstore(self, partition_keys_dic, key_val):
        for partition, keys in list(partition_keys_dic.items()):
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
    def __init__(self, server, bucket, kv_store, num_ops, create, read, update, delete, exp, compression=True,
                 scope=None, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression,
                                    scope=scope, collection=collection)
        self.itr = 0
        self.num_ops = num_ops
        self.create = create
        self.read = create + read
        self.update = create + read + update
        self.delete = create + read + update + delete
        self.exp = exp
        self.scope = scope
        self.collection = collection
        self.bucket = bucket

    def has_next(self):
        if self.num_ops == 0 or self.itr < self.num_ops:
            return True
        return False

    def __next__(self):
        self.itr += 1
        rand = random.randint(1, self.delete)
        if 0 < rand <= self.create:
            self._create_random_key()
        elif self.create < rand <= self.read:
            self._get_random_key()
        elif self.read < rand <= self.update:
            self._update_random_key()
        elif self.update < rand <= self.delete:
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
                 compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression,
                                    scope=scope, collection=collection)
        self.collection = collection
        self.scope = scope
        self.bucket = bucket
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket, scope=self.scope,
                                                              collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        self.only_store_hash = only_store_hash
        self.replica_to_read = replica_to_read
        self.bucket = bucket
        self.server = server
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        self.log.info(
            "%s items will be verified on %s bucket on scope %s on collection %s" % (self.max_verify, bucket,
                                                                                     self.scope, self.collection))
        self.start_time = time.time()

    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and \
                self.itr < self.max_verify:
            if not self.itr % 50000:
                self.log.info("{0} items were verified".format(self.itr))
            return True
        self.log.info("{0} items were verified in {1} sec.the average number of ops\
            - {2} per second ".format(self.itr, time.time() - self.start_time,
                                      self.itr // (time.time() - self.start_time)).rstrip())
        return False

    def __next__(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr], self.bucket, scope=self.scope, collection=self.collection)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys], self.bucket,
                                    scope=self.scope, collection=self.collection)
        self.itr += 1

    def _check_valid_key(self, key, bucket="default", scope=None, collection=None):
        partition = self.kv_store.acquire_partition(key, bucket, scope=scope,  collection=collection)

        value = partition.get_valid(key)
        flag = partition.get_flag(key)
        if value is None or flag is None:
            self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)
            return

        try:
            if self.replica_to_read is None:

                o, c, d = self.client.get(key, scope=scope, collection=collection)
            else:
                o, c, d = self.client.getr(key, replica_index=self.replica_to_read, scope=scope, collection=collection)
            try:
                d = d.decode()
            except AttributeError:
                pass
            if self.only_store_hash:
                if crc32.crc32_hash(d) != int(value):
                    self.state = FINISHED
                    self.set_exception(Exception(
                        'Key: %s, Bad hash result: %d != %d for key %s' % (key, crc32.crc32_hash(d), int(value), key)))
            else:
                value = json.dumps(value)
                if d != json.loads(value):
                    self.log.info(f"the scope {scope} collection is {collection} for which the value is failing")
                    self.state = FINISHED
                    self.set_exception(
                        Exception('Key: %s, Bad result: %s != %s for key %s' % (key, json.dumps(d), value, key)))
            if CHECK_FLAG and o != flag:
                self.state = FINISHED
                self.set_exception(
                    Exception('Key: %s, Bad result for flag value: %s != the value we set: %s' % (key, o, flag)))

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
        self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)

    def _check_deleted_key(self, key, bucket="default", scope=None, collection=None):
        partition = self.kv_store.acquire_partition(key, bucket, scope=scope, collection=collection)

        try:
            self.client.delete(key, scope=scope, collection=collection)
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
        self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)


class ValidateDataWithActiveAndReplicaTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression,
                                    scope=scope, collection=collection)
        self.collection = collection
        self.scope = scope
        self.bucket = bucket
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket, scope=self.scope,
                                                              collection=self.collection)
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        self.log.info("%s items will be verified on %s bucket" % (self.max_verify, bucket))
        self.start_time = time.time()

    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and \
                self.itr < self.max_verify:
            if not self.itr % 50000:
                self.log.info("{0} items were verified".format(self.itr))
            return True
        self.log.info("{0} items were verified in {1} sec.the average number of ops\
            - {2} per second ".format(self.itr, time.time() - self.start_time,
                                      self.itr // (time.time() - self.start_time)).rstrip())
        return False

    def __next__(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr], self.bucket, self.scope, self.collection)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys], self.bucket,
                                    self.scope, self.collection)
        self.itr += 1

    def _check_valid_key(self, key, bucket, scope=None, collection=None):
        partition = self.kv_store.acquire_partition(key, bucket, scope=scope, collection=collection)
        try:
            o, c, d = self.client.get(key, scope=scope, collection=collection)
            o_r, c_r, d_r = self.client.getr(key, replica_index=0, scope=scope, collection=collection)
            if o != o_r:
                self.state = FINISHED
                self.set_exception(Exception(
                    'ACTIVE AND REPLICA FLAG CHECK FAILED :: Key: %s, Bad result for CAS value: REPLICA FLAG %s != ACTIVE FLAG %s' % (
                        key, o_r, o)))
            if c != c_r:
                self.state = FINISHED
                self.set_exception(Exception(
                    'ACTIVE AND REPLICA CAS CHECK FAILED :: Key: %s, Bad result for CAS value: REPLICA CAS %s != ACTIVE CAS %s' % (
                        key, c_r, c)))
            if d != d_r:
                self.state = FINISHED
                self.set_exception(Exception(
                    'ACTIVE AND REPLICA VALUE CHECK FAILED :: Key: %s, Bad result for Value value: REPLICA VALUE %s != ACTIVE VALUE %s' % (
                        key, d_r, d)))

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

    def _check_deleted_key(self, key, bucket, scope=None, collection=None):
        partition = self.kv_store.acquire_partition(key, bucket, scope=scope, collection=collection)
        try:
            self.client.delete(key, scope=scope, collection=collection)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('ACTIVE CHECK :: Not Deletes: %s' % key))
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
        self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)


class BatchedValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, only_store_hash=True, batch_size=100,
                 timeout_sec=30, compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, server, bucket, kv_store, compression=compression, scope=scope,
                                    collection=collection)
        self.collection = collection
        self.scope = scope
        self.bucket = bucket
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket, scope=self.scope,
                                                              collection=self.collection)
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
                                         self.itr // (time.time() - self.start_time)).rstrip())
        return has

    def __next__(self):
        if self.itr < self.num_valid_keys:
            keys_batch = self.valid_keys[self.itr:self.itr + self.batch_size]
            self.itr += len(keys_batch)
            self._check_valid_keys(keys_batch, self.bucket, self.scope, self.collection)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys], self.bucket, self.scope,
                                    self.collection)
            self.itr += 1

    def _check_valid_keys(self, keys, bucket, scope=None, collection=None):
        partition_keys_dic = self.kv_store.acquire_partitions(keys, bucket, scope=scope, collection=collection)
        try:
            key_vals = self.client.getMulti(keys, parallel=True, timeout_sec=self.timeout_sec, scope=scope,
                                            collection=collection)
        except ValueError as error:
            self.log.error("Read failed via memcached client. Error: %s" % str(error))
            self.state = FINISHED
            self.kv_store.release_partitions(list(partition_keys_dic.keys()))
            self.set_exception(error)
            return
        except BaseException as error:
            # handle all other exception, for instance concurrent.futures._base.TimeoutError
            self.log.error("Read failed via memcached client. Error: %s" % str(error))
            self.state = FINISHED
            self.kv_store.release_partitions(list(partition_keys_dic.keys()))
            self.set_exception(error)
            return
        for partition, keys in list(partition_keys_dic.items()):
            self._check_validity(partition, keys, key_vals)
        self.kv_store.release_partitions(list(partition_keys_dic.keys()))

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
                        self.set_exception(
                            Exception('Key: %s Bad hash result: %d != %d' % (key, crc32.crc32_hash(d), int(value))))
                else:
                    # value = json.dumps(value)
                    if json.loads(d) != json.loads(value):
                        self.state = FINISHED
                        self.set_exception(Exception('Key: %s Bad result: %s != %s' % (key, json.dumps(d), value)))
                if CHECK_FLAG and o != flag:
                    self.state = FINISHED
                    self.set_exception(
                        Exception('Key: %s Bad result for flag value: %s != the value we set: %s' % (key, o, flag)))
            except KeyError as error:
                self.state = FINISHED
                self.set_exception(error)

    def _check_deleted_key(self, key, bucket, scope=None, collection=None):
        partition = self.kv_store.acquire_partition(key, bucket, scope=scope, collection=collection)
        try:
            self.client.delete(key, scope=scope, collection=collection)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)
                self.set_exception(error)
        except Exception as error:
            if error.rc != NotFoundError:
                self.state = FINISHED
                self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)
                self.set_exception(error)
        self.kv_store.release_partition(key, bucket, scope=scope, collection=collection)


class VerifyRevIdTask(GenericLoadingTask):
    def __init__(self, src_server, dest_server, bucket, src_kv_store, dest_kv_store, max_err_count=200000,
                 max_verify=None, compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, src_server, bucket, src_kv_store, compression=compression,
                                    scope=scope, collection=collection)
        from memcached.helper.data_helper import VBucketAwareMemcached as SmartClient
        self.collection = collection
        self.scope = scope
        self.client_src = SmartClient(RestConnection(src_server), bucket)
        self.client_dest = SmartClient(RestConnection(dest_server), bucket)
        self.src_valid_keys, self.src_deleted_keys = src_kv_store.key_set(bucket=self.bucket, scope=self.scope,
                                                                          collection=self.collection)
        self.dest_valid_keys, self.dest_del_keys = dest_kv_store.key_set(bucket=self.bucket, scope=self.scope,
                                                                         collection=self.collection)
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
        self.log.info(f"RevID verification: in progress for {self.bucket.name} in scope:{scope}"
                      f" in collection: {collection}")

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

    def __next__(self):
        if self.itr < self.num_valid_keys:
            self._check_key_revId(self.src_valid_keys[self.itr], collection=self.collection)
        elif self.itr < (self.num_valid_keys + self.num_deleted_keys):
            # verify deleted/expired keys
            self._check_key_revId(self.src_deleted_keys[self.itr - self.num_valid_keys],
                                  ignore_meta_data=['expiration', 'cas'], collection=self.collection)
        self.itr += 1

        # show progress of verification for every 50k items
        if math.fmod(self.itr, 50000) == 0.0:
            self.log.info("{0} items have been verified".format(self.itr))

    def __get_meta_data(self, client, key, scope=None, collection=None):
        try:
            mc = client.memcached(key)
            meta_data = eval("{'deleted': %s, 'flags': %s, 'expiration': %s, 'seqno': %s, 'cas': %s}" % (
                mc.getMeta(key, scope=scope, collection=collection)))
            return meta_data
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                # if a filter was specified, the key will not be found in
                # target kv store if key did not match filter expression
                if key not in self.src_deleted_keys and key in (self.dest_valid_keys + self.dest_del_keys):
                    self.err_count += 1
                    self.keys_not_found[client.rest.ip].append(
                        ("key: %s" % key, "vbucket: %s" % client._get_vBucket_id(key, scope=scope,
                                                                                 collection=collection)))
                else:
                    self.not_matching_filter_keys += 1
            else:
                self.state = FINISHED
                self.set_exception(error)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _check_key_revId(self, key, ignore_meta_data=None, scope=None, collection=None):
        if ignore_meta_data is None:
            ignore_meta_data = []
        src_meta_data = self.__get_meta_data(self.client_src, key, scope=scope, collection=collection)
        dest_meta_data = self.__get_meta_data(self.client_dest, key, scope=scope, collection=collection)
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
        for meta_key in list(src_meta_data.keys()):
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

class VerifyCollectionDocCountTask(Task):
    def __init__(self, src, dest, bucket, mapping):
        Task.__init__(self, "verify_collection_doc_count_task")
        self.src = src
        self.dest = dest
        self.bucket = bucket
        self.mapping = mapping
        self.src_conn = CollectionsStats(src.get_master_node())
        self.dest_conn = CollectionsStats(dest.get_master_node())

        self.src_stats = self.src_conn.get_collection_stats(self.bucket)
        self.dest_stats = self.dest_conn.get_collection_stats(self.bucket)

    def execute(self, task_manager):
        try:
            time.sleep(60)
            for map_exp in self.mapping.items():
                self.log.info("Map expression: {0}".format(str(map_exp)))
                if ':' in map_exp[0]:
                    src_scope = map_exp[0].split(':')[0]
                    src_collection = map_exp[0].split(':')[1]
                    src_count = self.src_conn.get_collection_item_count(self.bucket,
                                                                        src_scope, src_collection,
                                                                        self.src.get_nodes(),
                                                                        self.src_stats)
                else:
                    if '.' in map_exp[0]:
                        src_scope = map_exp[0].split('.')[0]
                        src_collection = map_exp[0].split('.')[1]
                    else:
                        src_scope = map_exp[0]
                        src_collection = "all"
                    src_count = self.src_conn.get_scope_item_count(self.bucket, src_scope,
                                                                   self.src.get_nodes(), self.src_stats)

                if map_exp[1]:
                    if map_exp[1].lower() == "null":
                        self.log.info("{} mapped to null, skipping doc count verification"
                                  .format())
                    dest_collection_specified = False
                    if ':' in map_exp[1]:
                        dest_collection_specified = True
                        dest_scope = map_exp[1].split(':')[0]
                        dest_collection = map_exp[1].split(':')[1]
                    elif "colon" in map_exp[1]:
                        dest_collection_specified = True
                        dest_scope = map_exp[1].split("colon")[0]
                        dest_collection = map_exp[1].split("colon")[1]
                    if dest_collection_specified:
                        dest_count = self.dest_conn.get_collection_item_count(self.bucket,
                                                                              dest_scope, dest_collection,
                                                                              self.dest.get_nodes(),
                                                                              self.dest_stats)
                    else:
                        if '.' in map_exp[1]:
                            dest_scope = map_exp[1].split('.')[0]
                            dest_collection = map_exp[1].split('.')[1]
                        else:
                            dest_scope = map_exp[1]
                            dest_collection = "all"
                        dest_count = self.dest_conn.get_scope_item_count(self.bucket, dest_scope,
                                                                         self.dest.get_nodes(), self.dest_stats)
                else:
                    dest_scope = None
                    dest_count = -1

                self.log.info('-' * 100)
                if src_count == dest_count:
                    self.log.info("Item count on src:{} {} = {} on dest:{} for "
                                  "bucket {} \nsrc : scope {}-> collection {},"
                                  "dest: scope {}-> collection {}"
                                  .format(self.src.get_master_node().ip, src_count,
                                          dest_count, self.dest.get_master_node().ip,
                                          self.bucket, src_scope, src_collection,
                                          dest_scope, dest_collection))
                elif dest_scope == None:
                    dest_scope = src_scope
                    dest_collection = src_collection
                    dest_count = self.dest_conn.get_scope_item_count(self.bucket, dest_scope,
                                                                        self.dest.get_nodes(), self.dest_stats)
                    if dest_count == 0:
                        self.log.info("Item count on src:{} {} != {} on dest:{} for "
                                    "bucket {} \nsrc : scope {}-> collection {},"
                                    "dest: scope {}-> collection {}, due to "
                                    "scope denial (dest_scope=null) as expected"
                                    .format(self.src.get_master_node().ip, src_count,
                                            dest_count, self.dest.get_master_node().ip,
                                            self.bucket, src_scope, src_collection,
                                            dest_scope, dest_collection))
                elif dest_scope == "nonexistent" or dest_collection == "nonexistent":
                    if dest_count == 0:
                        self.log.info("Item count on src:{} {} != {} on dest:{} for "
                                    "bucket {} \nsrc : scope {}-> collection {},"
                                    "dest: scope {}-> collection {}, due to "
                                    "nonexistent scope/collection as expected"
                                    .format(self.src.get_master_node().ip, src_count,
                                            dest_count, self.dest.get_master_node().ip,
                                            self.bucket, src_scope, src_collection,
                                            dest_scope, dest_collection))
                else:
                    self.set_exception(Exception("ERROR: Item count on src:{} {} != {} on dest:{} for "
                                  "bucket {} \nsrc : scope {}-> collection {},"
                                  "dest: scope {}-> collection {}"
                                  .format(self.src.get_master_node().ip, src_count,
                                          dest_count, self.dest.get_master_node().ip,
                                          self.bucket, src_scope, src_collection,
                                          dest_scope, dest_collection)))
                self.log.info('-' * 100)
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

        self.check(task_manager)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class VerifyMetaDataTask(GenericLoadingTask):
    def __init__(self, dest_server, bucket, kv_store, meta_data_store, max_err_count=100, compression=True,
                 scope=None, collection=None):
        GenericLoadingTask.__init__(self, dest_server, bucket, kv_store, compression=compression, scope=scope,
                                    collection=collection)
        from memcached.helper.data_helper import VBucketAwareMemcached as SmartClient
        self.collections = collection
        self.scope = scope
        self.client = SmartClient(RestConnection(dest_server), bucket)
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket,
                                                              scope=self.scope, collection=self.collection)
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

    def __next__(self):
        if self.itr < self.num_valid_keys:
            self._check_key_meta_data(self.valid_keys[self.itr], self.collections)
        elif self.itr < (self.num_valid_keys + self.num_deleted_keys):
            # verify deleted/expired keys
            self._check_key_meta_data(self.deleted_keys[self.itr - self.num_valid_keys],
                                      ignore_meta_data=['expiration'], scope=self.scope, collection=self.collections)
        self.itr += 1

        # show progress of verification for every 50k items
        if math.fmod(self.itr, 50000) == 0.0:
            self.log.info("{0} items have been verified".format(self.itr))

    def __get_meta_data(self, client, key, scope=None, collection=None):
        try:
            mc = client.memcached(key)
            meta_data = eval("{'deleted': %s, 'flags': %s, 'expiration': %s, 'seqno': %s, 'cas': %s}" % (
                mc.getMeta(key, scope=scope, collection=collection)))
            return meta_data
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                if key not in self.deleted_keys:
                    self.err_count += 1
                    self.keys_not_found[client.rest.ip].append(
                        ("key: %s" % key, "vbucket: %s" % client._get_vBucket_id(key)))
            else:
                self.state = FINISHED
                self.set_exception(error)

    def _check_key_meta_data(self, key, ignore_meta_data=[], scope=None, collection=None):
        src_meta_data = self.meta_data_store[key]
        dest_meta_data = self.__get_meta_data(self.client, key, scope=scope, collection=collection)
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
        for meta_key in list(src_meta_data.keys()):
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
    def __init__(self, dest_server, bucket, kv_store, compression=True, scope=None, collection=None):
        GenericLoadingTask.__init__(self, dest_server, bucket, kv_store, compression=compression,
                                    scope=scope, collection=collection)
        from memcached.helper.data_helper import VBucketAwareMemcached as SmartClient
        self.collection = collection
        self.scope = scope
        self.client = SmartClient(RestConnection(dest_server), bucket)
        self.valid_keys, self.deleted_keys = kv_store.key_set(bucket=self.bucket, scope=self.scope,
                                                              collection=self.collection)
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

    def __next__(self):
        if self.itr < self.num_valid_keys:
            self.meta_data_store[self.valid_keys[self.itr]] = self.__get_meta_data(self.client,
                                                                                   self.valid_keys[self.itr],
                                                                                   self.scope, self.collection)
        elif self.itr < (self.num_valid_keys + self.num_deleted_keys):
            self.meta_data_store[self.deleted_keys[self.itr - self.num_valid_keys]] = self.__get_meta_data(self.client,
                                                                                                           self.deleted_keys[
                                                                                                               self.itr - self.num_valid_keys],
                                                                                                           scope=self.scope,
                                                                                                           collection=self.collection)
        self.itr += 1

    def __get_meta_data(self, client, key, scope=None, collection=None):
        try:
            mc = client.memcached(key)
            meta_data = eval("{'deleted': %s, 'flags': %s, 'expiration': %s, 'seqno': %s, 'cas': %s}" % (
                mc.getMeta(key, scope=scope, collection=collection)))
            return meta_data
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                if key not in self.deleted_keys:
                    self.err_count += 1
                    self.keys_not_found[client.rest.ip].append(
                        ("key: %s" % key, "vbucket: %s" % client._get_vBucket_id(key)))
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
                    query = {"stale": "ok"}
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
                        if self.view.name not in list(json_parsed["spatial"].keys()):
                            self.set_exception(
                                Exception("design doc {O} doesn't contain spatial view {1}".format(
                                    self.design_doc_name, self.view.name)))
                    else:
                        if self.view.name not in list(json_parsed["views"].keys()):
                            self.set_exception(Exception("design doc {O} doesn't contain view {1}".format(
                                self.design_doc_name, self.view.name)))
                self.log.info(
                    "view : {0} was created successfully in ddoc: {1}".format(self.view.name, self.design_doc_name))
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
            if str(e).find('not_found') or str(e).find('view_undefined') > -1:
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
            server_info = {"ip": node.ip,
                           "port": node.port,
                           "username": self.rest.username,
                           "password": self.rest.password}

            for count in range(retry_count):
                try:
                    rest_node = RestConnection(server_info)
                    content, meta = rest_node.get_ddoc(self.bucket, self.design_doc_name)
                    new_rev_id = self._parse_revision(meta['rev'])
                    if new_rev_id == self.ddoc_rev_no:
                        break
                    else:
                        self.log.info("Design Doc {0} version is not updated on node {1}:{2}. Retrying.".format(
                            self.design_doc_name, node.ip, node.port))
                        time.sleep(2)
                except ReadDocumentException as e:
                    if (count < retry_count):
                        self.log.info(
                            "Design Doc {0} not yet available on node {1}:{2}. Retrying.".format(self.design_doc_name,
                                                                                                 node.ip, node.port))
                        time.sleep(2)
                    else:
                        self.log.error(
                            "Design Doc {0} failed to replicate on node {1}:{2}".format(self.design_doc_name, node.ip,
                                                                                        node.port))
                        self.set_exception(e)
                        self.state = FINISHED
                        break
                except Exception as e:
                    if (count < retry_count):
                        self.log.info("Unexpected Exception Caught. Retrying.")
                        time.sleep(2)
                    else:
                        self.set_unexpected_exception(e)
                        self.state = FINISHED
                        break
            else:
                self.set_exception(Exception(
                    "Design Doc {0} version mismatch on node {1}:{2}".format(self.design_doc_name, node.ip, node.port)))


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
            query = {"stale": "ok"}
            content = \
                rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
            self.state = FINISHED
            self.set_result(False)
        except QueryViewException as e:
            self.log.info(
                "view : {0} was successfully deleted in ddoc: {1}".format(self.view.name, self.design_doc_name))
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
                          (self.server.ip, self.design_doc_name, self.view_name, self.expected_rows,
                           len(content['rows'])))

            raised_error = content.get('error', '') or ''.join([str(item) for item in content.get('errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)

            if len(content['rows']) == self.expected_rows:
                self.log.info("expected number of rows: '{0}' was found for view query".format(self.
                                                                                               expected_rows))
                self.state = FINISHED
                self.set_result(True)
            else:
                if len(content['rows']) > self.expected_rows:
                    raise QueryViewException(self.view_name,
                                             "Server: {0}, Design Doc: {1}, actual returned rows: '{2}' are greater than expected {3}"
                                             .format(self.server.ip, self.design_doc_name, len(content['rows']),
                                                     self.expected_rows, ))
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
                 query, n1ql_helper=None,
                 expected_result=None,
                 verify_results=True,
                 is_explain_query=False,
                 index_name=None,
                 retry_time=2,
                 scan_consistency=None,
                 scan_vector=None):
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
                    query=self.query, server=self.server, expected_result=self.expected_result,
                    scan_consistency=self.scan_consistency, scan_vector=self.scan_vector,
                    verify_results=self.verify_results)
            else:
                self.actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.server,
                                                                    scan_consistency=self.scan_consistency,
                                                                    scan_vector=self.scan_vector)
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
                        actual_result = self.n1ql_helper.run_cbq_query(query="select * from system:indexes",
                                                                       server=self.server)
                        self.log.info(actual_result)
                        raise Exception(
                            " INDEX usage in Query {0} :: NOT FOUND {1} :: as observed in result {2}".format(
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
                 query, n1ql_helper=None,
                 retry_time=2, defer_build=False,
                 timeout=240):
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
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
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
            #if not "will retry building in the background for reason" in e:
            #    self.log.error(e)
            #    self.set_exception(e)

    def check(self, task_manager):
        try:
            # Verify correctness of result set
            check = True
            if not self.defer_build:
                check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name, server=self.server,
                                                                     timeout=self.timeout)
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
                 query, n1ql_helper=None,
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
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
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
                 n1ql_helper=None,
                 retry_time=2,
                 timeout=240):
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
                                                                 server=self.server, timeout=self.timeout)
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
                 query, n1ql_helper=None,
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
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server=self.server)
            if not check:
                raise DropIndexException("index {0} does not exist will not drop".format(self.index_name))
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
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
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server=self.server)
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
            raised_error = self.results.get('error', '') or ''.join(
                [str(item) for item in self.results.get('errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)
            else:
                self.log.info("view %s, query %s: expected- %s, actual -%s" % (
                    self.design_doc_name, self.query,
                    len(self.expected_docs),
                    len(self.results.get('rows', []))))
                self.state = CHECKING
                task_manager.schedule(self)
        except QueryViewException as ex:
            self.log.error("During query run (ddoc=%s, query=%s, server=%s) error is: %s" % (
                self.design_doc_name, self.query, self.servers[0].ip, str(ex)))
            if self.error and str(ex).find(self.error) != -1:
                self.state = FINISHED
                self.set_result({"passed": True,
                                 "errors": str(ex)})
            elif self.current_retry == self.retries:
                self.state = FINISHED
                self.set_result({"passed": False,
                                 "errors": str(ex)})
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
                res = {"passed": False,
                       "errors": str(ex)}
                if self.results and self.results.get('rows', []):
                    res['results'] = self.results
                self.set_result(res)
        except Exception as ex:
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
            if self.view.red_func and (('reduce' in self.query and \
                                        self.query['reduce'] == "true") or (not 'reduce' in self.query)):
                if len(self.expected_docs) != len(self.results.get('rows', [])):
                    if self.current_retry == self.retries:
                        self.state = FINISHED
                        msg = "ddoc=%s, query=%s, server=%s" % (
                            self.design_doc_name, self.query, self.servers[0].ip)
                        msg += "Number of groups expected:%s, actual:%s" % (
                            len(self.expected_docs), len(self.results.get('rows', [])))
                        self.set_result({"passed": False,
                                         "errors": msg})
                    else:
                        RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                        self.state = EXECUTING
                        task_manager.schedule(self, 10)
                else:
                    for row in self.expected_docs:
                        key_expected = row['key']

                        if not (key_expected in [key['key'] for key in self.results.get('rows', [])]):
                            if self.current_retry == self.retries:
                                self.state = FINISHED
                                msg = "ddoc=%s, query=%s, server=%s" % (
                                    self.design_doc_name, self.query, self.servers[0].ip)
                                msg += "Key expected but not present :%s" % (key_expected)
                                self.set_result({"passed": False,
                                                 "errors": msg})
                            else:
                                RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                                self.state = EXECUTING
                                task_manager.schedule(self, 10)
                        else:
                            for res in self.results.get('rows', []):
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
                                self.set_result({"passed": True,
                                                 "errors": []})
                            else:
                                if self.current_retry == self.retries:
                                    self.state = FINISHED
                                    self.log.error(msg)
                                    self.set_result({"passed": True,
                                                     "errors": msg})
                                else:
                                    RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                                    self.state = EXECUTING
                                    task_manager.schedule(self, 10)
                return
            if len(self.expected_docs) > len(self.results.get('rows', [])):
                if self.current_retry == self.retries:
                    self.state = FINISHED
                    self.set_result({"passed": False,
                                     "errors": [],
                                     "results": self.results})
                else:
                    RestHelper(self.rest)._wait_for_indexer_ddoc(self.servers, self.design_doc_name)
                    if self.current_retry == 70:
                        self.query["stale"] = 'false'
                    self.log.info(
                        "View result is still not expected (ddoc=%s, query=%s, server=%s). retry in 10 sec" % (
                            self.design_doc_name, self.query, self.servers[0].ip))
                    self.state = EXECUTING
                    task_manager.schedule(self, 10)
            elif len(self.expected_docs) < len(self.results.get('rows', [])):
                self.state = FINISHED
                self.set_result({"passed": False,
                                 "errors": [],
                                 "results": self.results})
            elif len(self.expected_docs) == len(self.results.get('rows', [])):
                if self.verify_rows:
                    expected_ids = [row['id'] for row in self.expected_docs]
                    rows_ids = [str(row['id']) for row in self.results['rows']]
                    if expected_ids == rows_ids:
                        self.state = FINISHED
                        self.set_result({"passed": True,
                                         "errors": []})
                    else:
                        if self.current_retry == self.retries:
                            self.state = FINISHED
                            self.set_result({"passed": False,
                                             "errors": [],
                                             "results": self.results})
                        else:
                            self.state = EXECUTING
                            task_manager.schedule(self, 10)
                else:
                    self.state = FINISHED
                    self.set_result({"passed": True,
                                     "errors": []})
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.error("Exception caught %s" % str(e))
            self.set_exception(e)
            self.set_result({"passed": False,
                             "errors": str(e)})


class ModifyFragmentationConfigTask(Task):
    """
        Given a config dictionary attempt to configure fragmentation settings.
        This task will override the default settings that are provided for
        a given <bucket>.
    """

    def __init__(self, server, config=None, bucket="default"):
        Task.__init__(self, "modify_frag_config_task")

        self.server = server
        self.config = {"parallelDBAndVC": "false",
                       "dbFragmentThreshold": None,
                       "viewFragmntThreshold": None,
                       "dbFragmentThresholdPercentage": 100,
                       "viewFragmntThresholdPercentage": 100,
                       "allowedTimePeriodFromHour": None,
                       "allowedTimePeriodFromMin": None,
                       "allowedTimePeriodToHour": None,
                       "allowedTimePeriodToMin": None,
                       "allowedTimePeriodAbort": None,
                       "autoCompactionDefined": "true"}
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
            pass  # no special actions
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
                                                      self.target_key == "designDocument" and task[
                                                  self.target_key] == self.target_value) or (
                                                      self.target_key == "original_target" and task[self.target_key][
                                                  "type"] == self.target_value) or (
                                                      self.type == 'indexer')):
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
                        self.log.error(
                            "progress for active task was not changed during %s sec" % 2 * self.num_iterations)
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
        if self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = \
                "Invalid value for fragmentation %d" % self.fragmentation_value
            self.state = FINISHED
            self.set_exception(Exception(err_msg))

        # warning if autocompaction is less than <fragmentation_value>
        try:
            auto_compact_percentage = self._get_current_auto_compaction_percentage()
            if auto_compact_percentage != "undefined" and auto_compact_percentage < self.fragmentation_value:
                self.log.warning("Auto compaction is set to %s. Therefore fragmentation_value %s may not be reached" % (
                    auto_compact_percentage, self.fragmentation_value))

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
        new_frag_value = MonitorViewFragmentationTask. \
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
            server_info = {"ip": node.ip,
                           "port": node.port,
                           "username": rest.username,
                           "password": rest.password}
            rest = RestConnection(server_info)
            status = False
            try:
                status, content = rest.set_view_info(bucket, design_doc_name)
            except Exception as e:
                print((str(e)))
                if "Error occured reading set_view _info" in str(e) and with_rebalance:
                    print(("node {0} {1} is not ready yet?: {2}".format(
                        node.id, node.port, str(e))))
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

            if new_compaction_revision == self.compaction_revision and _compaction_running:
                # compaction ran successfully but compaction was not changed
                # perhaps we are still compacting
                self.log.info("design doc {0} is compacting".format(self.design_doc_name))
                task_manager.schedule(self, 3)
            elif new_compaction_revision > self.compaction_revision or \
                    self.precompacted_fragmentation > fragmentation:
                self.log.info(
                    "{1}: compactor was run, compaction revision was changed on {0}".format(new_compaction_revision,
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
                    self.log.info(
                        "compaction was completed, but fragmentation value {0} is more than before compaction {1}".
                            format(fragmentation, self.precompacted_fragmentation))
                    # probably we already compacted, but no work needed to be done
                    self.set_result(self.with_rebalance)
                else:
                    self.set_result(True)
                self.state = FINISHED
            else:
                # Sometimes the compacting is not started immediately
                for i in range(17):
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
                            self.log.warning("the compaction revision was increased,\
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
                self.log.warning("general compaction stats:{0}".format(stats))
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
            MonitorViewFragmentationTask.calc_ddoc_fragmentation(self.rest, self.design_doc_name, self.bucket,
                                                                 self.with_rebalance)
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
                if (server.hostname if self.use_hostnames else server.ip) == node.ip and int(server.port) == int(
                        node.port):
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
        self.is_reduced = self.view.red_func is not None and (('reduce' in query and query['reduce'] == "true") or \
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
        except Exception as ex:
            self.state = FINISHED
            self.set_unexpected_exception(ex)
            traceback.print_exc()

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
            self.type_filter = {"filter_what": filter_what,
                                "filter_expr": re.sub(r'[ +]?"\);.*', '',
                                                      re.sub(r'.*.new RegExp\("\^', '', self.view.map_func))}
        if self.is_reduced and self.view.red_func != "_count":
            emit_value = re.sub(r'\);.*', '', re.sub(r'.*emit\([ +]?\[*],[ +]?doc\.', '', self.view.map_func))
            if self.view.map_func.count("[") <= 1:
                emit_value = re.sub(r'\);.*', '', re.sub(r'.*emit\([ +]?.*,[ +]?doc\.', '', self.view.map_func))
        for doc_gen in self.doc_generators:

            query_doc_gen = copy.deepcopy(doc_gen)
            while query_doc_gen.has_next():

                _id, val = next(query_doc_gen)
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
                if isinstance(val_emit_key, str):
                    val_emit_key = val_emit_key.encode('utf-8')
                if not self.is_reduced or self.view.red_func == "_count" or self.custom_red_fn:
                    self.emitted_rows.append({'id': _id, 'key': val_emit_key})
                else:
                    val_emit_value = val[emit_value]
                    self.emitted_rows.append({'value': val_emit_value, 'key': val_emit_key, 'id': _id, })

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
                               key=cmp_to_key(lambda a, b: GenerateExpectedViewResultsTask.cmp_result_rows(a, b)),
                               reverse=descending_set)

        # filter rows according to query flags
        if startkey_set:
            start_key = query['startkey']
            if isinstance(start_key, str) and start_key.find('"') == 0:
                start_key = start_key[1:-1]
            if isinstance(start_key, str) and start_key.find('[') == 0:
                start_key = start_key[1:-1].split(',')
                start_key = [int(x) if x != 'null' else 0 for x in start_key]
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
                end_key = [int(x) if x != 'null' else None for x in end_key]
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
                key_ = [int(x) if x != 'null' else None for x in key_]
            start_key, end_key = key_, key_
            expected_rows = [row for row in expected_rows if row['key'] == key_]

        if descending_set:
            startkey_docid_set, endkey_docid_set = endkey_docid_set, startkey_docid_set

        if startkey_docid_set:
            if not startkey_set:
                self.log.warning("Ignoring startkey_docid filter when startkey is not set")
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
                self.log.warning("Ignoring endkey_docid filter when endkey is not set")
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
                expected_rows = [row for row in expected_rows if
                                 row['id'] < query['endkey_docid'] or row['key'] < end_key]
            elif endkey_set:
                expected_rows = [row for row in expected_rows if row['key'] != end_key]

        if self.is_reduced:
            groups = {}
            gr_level = None
            if not 'group' in query and \
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
                    groups[None]['sumsqr'] = math.fsum([x * x for x in values])
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
            for group, value in groups.items():
                if isinstance(group, str) and group.find("[") == 0:
                    group = group[1:-1].split(",")
                    group = [int(k) for k in group]
                expected_rows.append({"key": group, "value": value})
            expected_rows = sorted(expected_rows,
                                   key=cmp_to_key(lambda a, b: GenerateExpectedViewResultsTask.cmp_result_rows(a, b)),
                                   reverse=descending_set)
        if 'skip' in query:
            expected_rows = expected_rows[(int(query['skip'])):]
        if 'limit' in query:
            expected_rows = expected_rows[:(int(query['limit']))]

        self.emitted_rows = expected_rows

    @staticmethod
    def cmp_result_rows(x, y):
        rc = len(DeepDiff(x['key'], y['key'], ignore_order=True))
        if rc == 0:
            # sort by id is tie breaker
            rc = len(DeepDiff(x['id'], y['id'], ignore_order=True))
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
                 num_verified_docs=20, bucket="default", query_timeout=120, results=None,
                 config=None):

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
        rc_status = {"passed": False,
                     "errors": err_infos}  # array of dicts with keys 'msg' and 'details'
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
        except Exception as ex:
            self.state = FINISHED
            try:
                max_example_result = max(100, len(self.results['rows'] - 1))
                self.log.info("FIRST %s RESULTS for view %s : %s" % (max_example_result, self.view_name,
                                                                     self.results['rows'][max_example_result]))
            except Exception as inner_ex:
                self.log.error(inner_ex)
            self.set_result({"passed": False,
                             "errors": "ERROR: %s" % ex})

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
            err = {"msg": "duplicate rows found in query results",
                   "details": dupe_rows}
            err_infos.append(err)

    def check_for_missing_ids(self, expected_ids, couch_ids, err_infos):

        missing_id_set = set(expected_ids) - set(couch_ids)

        if len(missing_id_set) > 0:

            missing_id_errors = self.debug_missing_items(missing_id_set)

            if len(missing_id_errors) > 0:
                err = {"msg": "missing ids from memcached",
                       "details": missing_id_errors}
                err_infos.append(err)

    def check_for_extra_ids(self, expected_ids, couch_ids, err_infos):

        extra_id_set = set(couch_ids) - set(expected_ids)
        if len(extra_id_set) > 0:
            err = {"msg": "extra ids from memcached",
                   "details": extra_id_set}
            err_infos.append(err)

    def check_for_value_corruption(self, err_infos):

        if self.num_verified_docs > 0:

            doc_integrity_errors = self.include_doc_integrity()

            if len(doc_integrity_errors) > 0:
                err = {"msg": "missmatch in document values",
                       "details": doc_integrity_errors}
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

                if (doc_meta != None):
                    if (doc_meta['key_valid'] != 'valid'):
                        msg = "Error expected in results for key with invalid state %s" % doc_meta
                        missing_id_errors.append(msg)

                else:
                    msg = "query doc_id: %s doesn't exist in bucket: %s" % \
                          (doc_id, self.bucket)
                    missing_id_errors.append(msg)

            if (len(missing_id_errors) == 0):
                msg = "view engine failed to index doc [%s] in query: %s" % (doc_id, self.query)
                missing_id_errors.append(msg)

            return missing_id_errors

    def include_doc_integrity(self):
        rest = RestConnection(self.server)
        client = KVStoreAwareSmartClient(rest, self.bucket)
        doc_integrity_errors = []

        if 'doc' not in self.results['rows'][0]:
            return doc_integrity_errors
        exp_verify_set = [row['doc'] for row in \
                          self.results['rows'][:self.num_verified_docs]]

        for view_doc in exp_verify_set:
            doc_id = str(view_doc['_id'])
            mc_item = client.mc_get_full(doc_id)

            if mc_item is not None:
                mc_doc = json.loads(mc_item["value"])

                # compare doc content
                for key in list(mc_doc.keys()):
                    if (mc_doc[key] != view_doc[key]):
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
        if self.fragmentation_value < 0 or self.fragmentation_value > 100:
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
        except Exception as ex:
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
        except Exception as e:
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

    # it was done to keep connection alive
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
                self.log.warning("cbrecovery progress was not changed")
                if self.retries > 20:
                    self.shell.disconnect()
                    self.rest.print_UI_logs()
                    self.state = FINISHED
                    self.log.warning("ns_server_tasks: {0}".format(self.rest.ns_server_tasks()))
                    self.log.warning("cbrecovery progress: {0}".format(
                        self.rest.get_recovery_progress(self.recovery_task["recoveryStatusURI"])))
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
                self.log.warning("ns_server_tasks: {0}".format(self.rest.ns_server_tasks()))
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

        if len(DeepDiff(current_compaction_count, self.compaction_count)) == 1:
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
                self.log.warning("%s: There is nothing to compact, fragmentation is 0" % self.design_doc_name)
                self.set_result(False)
                self.state = FINISHED
            elif self.precompacted_fragmentation < self.fragmentation_value:
                self.log.info(
                    "{0}: Compaction is already done and there is nothing to compact, current fragmentation is lesser {1} {2}".
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
                    self.log.info("%s: fragmentation went from %d to %d" % (
                        self.design_doc_name, self.precompacted_fragmentation, fragmentation))
                if frag_val_diff > 0:
                    if self._is_compacting():
                        task_manager.schedule(self, 5)
                    self.log.info(
                        "compaction was completed, but fragmentation value {0} is more than before compaction {1}".
                            format(fragmentation, self.precompacted_fragmentation))
                    self.log.info("Load is still in progress, Need to be checked")
                    self.set_result(self.with_rebalance)
                else:
                    self.set_result(True)
                self.state = FINISHED
            else:
                for i in range(10):
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
                            self.log.warning(
                                "the compaction revision was increase and fragmentation value went from {0} {1}".
                                    format(self.precompacted_fragmentation, fragmentation))
                            self.set_result(True)
                            self.state = FINISHED
                            return
                        elif new_compaction_revision > self.compaction_revision and self.with_rebalance:
                            self.log.warning(
                                "the compaction revision was increased, but the actual fragmentation value has not changed significantly")
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
                self.log.warning("general compaction stats:{0}".format(stats))
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
        if self.fragmentation_value < 0:
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
        except Exception as ex:
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
        except ServerUnavailableException as e:
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
            except ServerUnavailableException as e:
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
        self.cluster_flag = "--cluster"
        self.num_shards = num_shards
        if self.cb_version is None:
            raise Exception("Need to pass Couchbase version to run correctly bk/rt ")
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
            args = (
                f"backup --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
                f" --repo {self.backupset.name}"
                f" {self.cluster_flag} http://{self.backupset.cluster_host.ip}:{self.backupset.cluster_host.port}"
                f" --username {self.backupset.cluster_host.rest_username}"
                f" --password {self.backupset.cluster_host.rest_password}"
                f" {self.num_shards}"
                f"{' --obj-staging-dir ' + self.backupset.objstore_staging_directory if self.objstore_provider else ''}"
                f"{' --obj-endpoint ' + self.backupset.objstore_endpoint if self.objstore_provider and self.backupset.objstore_endpoint else ''}"
                f"{' --obj-region ' + self.backupset.objstore_region if self.objstore_provider and self.backupset.objstore_region else ''}"
                f"{' --obj-access-key-id ' + self.backupset.objstore_access_key_id if self.objstore_provider and self.backupset.objstore_access_key_id else ''}"
                f"{' --obj-secret-access-key ' + self.backupset.objstore_secret_access_key if self.objstore_provider and self.backupset.objstore_secret_access_key else ''}"
                f"{' --s3-force-path-style' if self.objstore_provider and self.objstore_provider.schema_prefix() == 's3://' else ''}"
            )
            if self.resume:
                args += " --resume"
            if self.purge:
                args += " --purge"
            if self.no_progress_bar:
                args += " --no-progress-bar"
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception as e:
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

    def __init__(self, backupset, objstore_provider, no_progress_bar=False, cli_command_location='', cb_version=None, start="start", end="end", backups=[], force_updates=False, no_resume=False):
        Task.__init__(self, "enterprise_backup_task")
        self.backupset = backupset
        self.objstore_provider = objstore_provider
        self.no_progress_bar = no_progress_bar
        self.cli_command_location = cli_command_location
        self.cb_version = cb_version
        self.cluster_flag = "--cluster"
        """ from couchbase version 4.6.x, --host flag is not supported """
        if self.cb_version is None:
            raise Exception("Need to pass Couchbase version to run correctly bk/rt ")
        self.output = []
        self.error = []
        self.backups = backups
        self.start = start
        self.end = end
        self.force_updates = force_updates
        self.no_resume = no_resume
        try:
            self.remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        except Exception as e:
            self.log.error(e)
            self.state = FINISHED
            self.set_exception(e)

    def execute(self, task_manager):
        try:
            if isinstance(self.start, int) and isinstance(self.end, int):
                try:
                    backup_start = self.backups[int(self.start) - 1]
                except IndexError:
                    backup_start = "{0}{1}".format(self.backups[-1], self.start)
                try:
                    backup_end = self.backups[int(self.end) - 1]
                except IndexError:
                    backup_end = "{0}{1}".format(self.backups[-1], self.end)
            else:
                backup_start = self.start
                backup_end = self.end

            args = (
                f"restore --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
                f" --repo {self.backupset.name}"
                f" {self.cluster_flag} http://{self.backupset.restore_cluster_host.ip}:{self.backupset.restore_cluster_host.port}"
                f" --username {self.backupset.restore_cluster_host.rest_username} " 
                   f" --password {self.backupset.restore_cluster_host.rest_password}"
                f" --start {backup_start}"
                f" --end {backup_end}" 
                f"{' --obj-staging-dir ' + self.backupset.objstore_staging_directory if self.objstore_provider else ''}"
                       f"{' --obj-endpoint ' + self.backupset.objstore_endpoint if self.objstore_provider and self.backupset.objstore_endpoint else ''}"
                f"{' --obj-region ' + self.backupset.objstore_region if self.objstore_provider and self.backupset.objstore_region else ''}"
                       f"{' --obj-access-key-id ' + self.backupset.objstore_access_key_id if self.objstore_provider and self.backupset.objstore_access_key_id else ''}"
                       f"{' --obj-secret-access-key ' + self.backupset.objstore_secret_access_key if self.objstore_provider and self.backupset.objstore_secret_access_key else ''}"
                        f"{' --s3-force-path-style' if self.objstore_provider and self.objstore_provider.schema_prefix() == 's3://' else ''}"
                f"{' --resume' if self.backupset.resume and not self.no_resume else ''}"
            )
            if self.no_progress_bar:
                args += " --no-progress-bar"
            if self.force_updates:
                args += " --force-updates"
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception as e:
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
            args = "merge --archive {0} --repo {1} --start {2} --end {3}".format(self.directory, self.name,
                                                                                 backup_start, backup_end)
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
            self.output, self.error = self.remote_client.execute_command(command)
            self.state = CHECKING
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
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


class NodesFailureTask(Task):
    def __init__(self, master, servers_to_fail, failure_type, timeout,
                 pause=0, timeout_buffer=3, disk_timeout=0, disk_location=None, disk_size=5000, failure_timeout=60):
        Task.__init__(self, "NodesFailureTask")
        self.master = master
        self.servers_to_fail = servers_to_fail
        self.num_servers_to_fail = self.servers_to_fail.__len__()
        self.itr = 0
        self.failure_type = failure_type
        self.timeout = timeout
        self.failure_timeout = failure_timeout
        self.pause = pause
        self.start_time = 0
        self.timeout_buffer = timeout_buffer
        self.current_failure_node = self.servers_to_fail[0]
        self.max_time_to_wait_for_failover = self.timeout + \
                                             self.timeout_buffer + 60
        self.disk_timeout = disk_timeout
        self.disk_location = disk_location
        self.disk_size = disk_size
        self.taskmanager = None
        self.rebalance_in_progress = False

    def execute(self, task_manager):
        self.taskmanager = task_manager
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status() == "running":
            self.rebalance_in_progress = True
        while self.has_next() and not self.done():
            next(self)
            if self.pause > 0 and self.pause > self.timeout:
                self.check(task_manager)
        if self.pause == 0 or 0 < self.pause < self.timeout:
            self.check(task_manager)
        self.state = FINISHED
        self.set_result(True)

    def check(self, task_manager):
        rest = RestConnection(self.master)
        max_timeout = self.timeout + self.timeout_buffer + self.disk_timeout
        if self.start_time == 0:
            message = "Did not inject failure in the system."
            rest.print_UI_logs(10)
            self.log.error(message)
            self.state = FINISHED
            self.set_result(False)
            self.set_exception(NodesFailureException(message))

    def has_next(self):
        return self.itr < self.num_servers_to_fail

    def __next__(self):
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
        self.start_time = time.time()
        self.log.info("before failure time: {}".format(time.ctime(time.time())))
        if self.failure_type == "limit_file_limits_desc":
            self._enable_disable_limit_file_limits_desc(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_limit_file_limits_desc":
            self.enable_file_limit_desc(self.current_failure_node)
        elif self.failure_type == "disable_limit_file_limits_desc":
            self.disable_file_limit_desc(self.current_failure_node)
        elif self.failure_type == "limit_file_limits":
            self._enable_disable_limit_file_limits(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_limit_file_limits":
            self.enable_file_limit(self.current_failure_node)
        elif self.failure_type == "disable_limit_file_limits":
            self.disable_file_limit(self.current_failure_node)
        elif self.failure_type == "extra_files_in_log_dir":
            self._extra_files_in_log_dir(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_extra_files_in_log_dir":
            self.add_extra_files_in_log_dir(self.current_failure_node)
        elif self.failure_type == "disable_extra_files_in_log_dir":
            self.remove_extra_files_in_log_dir(self.current_failure_node)
        elif self.failure_type == "empty_files_in_log_dir":
            self._empty_file_in_log_dir(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_empty_files_in_log_dir":
            self.add_empty_file_in_log_dir(self.current_failure_node)
        elif self.failure_type == "disable_empty_files_in_log_dir":
            self.remove_dummy_file_in_log_dir(self.current_failure_node)
        elif self.failure_type == "dummy_file_in_log_dir":
            self._dummy_file_in_log_dir(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_dummy_file_in_log_dir":
            self.add_dummy_file_in_log_dir(self.current_failure_node)
        elif self.failure_type == "disable_dummy_file_in_log_dir":
            self.remove_dummy_file_in_log_dir(self.current_failure_node)
        elif self.failure_type == "limit_file_size_limit":
            self._enable_disable_limit_file_size_limit(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_limit_file_size_limit":
            self.enable_file_size_limit(self.current_failure_node)
        elif self.failure_type == "disable_limit_file_size_limit":
            self.disable_file_size_limit(self.current_failure_node)
        elif self.failure_type == "disk_readonly":
            self._enable_disable_disk_readonly(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_disk_readonly":
            self._enable_disk_readonly(self.current_failure_node)
        elif self.failure_type == "disable_disk_readonly":
            self._disable_disk_readonly(self.current_failure_node)
        elif self.failure_type == "stress_ram":
            self._enable_stress_ram(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "stress_cpu":
            self._enable_stress_cpu(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "network_delay":
            self._enable_disable_network_delay(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_network_delay":
            self.enable_network_delay(self.current_failure_node)
        elif self.failure_type == "disable_network_delay":
            self.delete_network_rule(self.current_failure_node)
        elif self.failure_type == "net_packet_loss":
            self._enable_disable_packet_loss(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_net_packet_loss":
            self.enable_packet_loss(self.current_failure_node)
        elif self.failure_type == "disable_net_packet_loss":
            self.delete_network_rule(self.current_failure_node)
        elif self.failure_type == "enable_firewall":
            self._enable_disable_firewall(self.current_failure_node, self.failure_timeout)
        if self.failure_type == "induce_enable_firewall":
            self._enable_firewall(self.current_failure_node)
        elif self.failure_type == "disable_firewall":
            self._disable_firewall(self.current_failure_node)
        elif self.failure_type == "restart_couchbase":
            self._restart_couchbase_server(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "stop_couchbase":
            self._stop_couchbase_server(self.current_failure_node)
        elif self.failure_type == "start_couchbase":
            self._start_couchbase_server(self.current_failure_node)
        elif self.failure_type == "restart_network":
            self._stop_restart_network(self.current_failure_node,
                                       self.failure_timeout)
        elif self.failure_type == "restart_machine":
            self._restart_machine(self.current_failure_node)
        elif self.failure_type == "stop_memcached":
            self._stop_memcached(self.current_failure_node)
        elif self.failure_type == "start_memcached":
            self._start_memcached(self.current_failure_node)
        elif self.failure_type == "kill_goxdcr":
            self._kill_goxdcr(self.current_failure_node)
        elif self.failure_type == "network_split":
            self._block_incoming_network_from_node(self.servers_to_fail[0],
                                                   self.servers_to_fail[
                                                       self.itr + 1])
            self.itr += 1
        elif self.failure_type == "disk_failure":
            self._fail_recover_disk_failure(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "induce_disk_failure":
            self._fail_disk(self.current_failure_node)
        elif self.failure_type == "disk_full":
            self._disk_full_recover_failure(self.current_failure_node, self.failure_timeout)
        elif self.failure_type == "shard_json_corruption":
            self.shard_json_corruption(self.current_failure_node)
        elif self.failure_type == "induce_disk_full":
            self._disk_full_failure(self.current_failure_node)
        elif self.failure_type == "recover_disk_failure":
            self._recover_disk(self.current_failure_node)
        elif self.failure_type == "recover_disk_full_failure":
            self._recover_disk_full_failure(self.current_failure_node)
        self.log.info("Start time = {}".format(time.ctime(self.start_time)))
        self.itr += 1

    def _enable_disable_firewall(self, node, recover_time):
        self._enable_firewall(node)
        time.sleep(recover_time)
        self._disable_firewall(node)

    def _enable_disable_packet_loss(self, node, recover_time):
        self.enable_packet_loss(node)
        time.sleep(recover_time)
        self.delete_network_rule(node)

    def _enable_disable_limit_file_limits(self, node, recover_time):
        self.enable_file_limit(node)
        time.sleep(recover_time)
        self.disable_file_limit(node)

    def _enable_disable_limit_file_size_limit(self, node, recover_time):
        self.enable_file_size_limit(node)
        time.sleep(recover_time)
        self.disable_file_size_limit(node)

    def enable_file_size_limit(self, node):
        shell = RemoteMachineShellConnection(node)
        self.log.info("Updating file size limit to 10MB on {}".format(node))
        shell.enable_file_size_limit()
        shell.disconnect()
        self.log.info("Enabled file size limit on {}".format(node))

    def disable_file_size_limit(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_file_size_limit()
        shell.disconnect()
        self.log.info("disabled file size limit on {}".format(node))

    def enable_file_limit(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.enable_file_limit()
        shell.disconnect()
        self.log.info("Enabled file limit on {}".format(node))

    def disable_file_limit(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_file_limit()
        shell.disconnect()
        self.log.info("disabled file limit on {}".format(node))

    def _enable_disable_limit_file_limits_desc(self, node, recover_time):
        self.enable_file_limit_desc(node)
        time.sleep(recover_time)
        self.disable_file_limit_desc(node)

    def enable_file_limit_desc(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.enable_file_limit_desc()
        shell.disconnect()
        self.log.info("Enabled file limit _desc on {}".format(node))

    def disable_file_limit_desc(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_file_limit_desc()
        shell.disconnect()
        self.log.info("disabled file limit _desc on {}".format(node))

    def _enable_disable_network_delay(self, node, recover_time):
        self.enable_network_delay(node)
        time.sleep(recover_time)
        self.delete_network_rule(node)

    def enable_network_delay(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.enable_network_delay()
        shell.disconnect()
        self.log.info("Enabled network delay on {}".format(node))

    def delete_network_rule(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.delete_network_rule()
        shell.disconnect()
        self.log.info("Disabled packet loss on {}".format(node))

    def enable_packet_loss(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.enable_packet_loss()
        shell.disconnect()
        self.log.info("Enabled packet loss on {}".format(node))

    def _enable_firewall(self, node):
        RemoteUtilHelper.enable_firewall(node)
        self.log.info("Enabled firewall on {}".format(node))

    def _disable_firewall(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_firewall()

    def _restart_couchbase_server(self, node, failure_timeout):
        shell = RemoteMachineShellConnection(node)
        shell.restart_couchbase()
        shell.disconnect()
        self.log.info("Restarted the couchbase server on {}".format(node))
        time.sleep(failure_timeout)

    def _stop_couchbase_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        shell.disconnect()
        self.log.info("Stopped the couchbase server on {}".format(node))

    def _start_couchbase_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.start_couchbase()
        shell.disconnect()
        self.log.info("Started the couchbase server on {}".format(node))

    def _enable_stress_cpu(self, node, stop_time):
        shell = RemoteMachineShellConnection(node)
        shell.cpu_stress(stop_time)
        shell.disconnect()
        self.log.info("cpu stressed for {0} sec on node {1}".format(stop_time, node))

    def _enable_stress_ram(self, node, stop_time):
        shell = RemoteMachineShellConnection(node)
        shell.ram_stress(stop_time)
        shell.disconnect()
        self.log.info("ram stressed for {0} sec on node {1}".format(stop_time, node))

    def _enable_disk_readonly(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.enable_disk_readonly(self.disk_location)
        shell.disconnect()
        self.log.info("Dir {} made readonly on node {}".format(self.disk_location, node))

    def _disable_disk_readonly(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_disk_readonly(self.disk_location)
        shell.disconnect()
        self.log.info("Dir {} made read/write on node {}".format(self.disk_location, node))

    def _stop_restart_network(self, node, stop_time):
        shell = RemoteMachineShellConnection(node)
        shell.stop_network(stop_time)
        shell.disconnect()
        self.log.info("Stopped the network for {0} sec and restarted the "
                      "network on {1}".format(stop_time, node))

    def _restart_machine(self, node):
        shell = RemoteMachineShellConnection(node)
        command = "/sbin/reboot"
        shell.execute_command(command=command)

    def _stop_memcached(self, node):
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        o, r = shell.stop_memcached()
        self.log.info("Killed memcached. {0} {1}".format(o, r))

    def _start_memcached(self, node):
        shell = RemoteMachineShellConnection(node)
        o, r = shell.start_memcached()
        self.log.info("Started back memcached. {0} {1}".format(o, r))
        shell.disconnect()

    def _kill_goxdcr(self, node):
        shell = RemoteMachineShellConnection(node)
        o, r = shell.kill_goxdcr()
        self.log.info("Killed goxdcr. {0} {1}".format(o, r))

    def _block_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.log.info("Adding {0} into iptables rules on {1}".format(
            node1.ip, node2.ip))
        command = "iptables -A INPUT -s {0} -j DROP".format(node2.ip)
        shell.execute_command(command)
        self.start_time = time.time()

    def _fail_recover_disk_failure(self, node, recover_time):
        self._fail_disk(node)
        time.sleep(recover_time)
        self._recover_disk(node)

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
        o, r = shell.mount_partition_ext4(self.disk_location)
        for line in o:
            if self.disk_location in line:
                self.log.info("Mounted disk at location : {0} on {1}".format(self.disk_location, node.ip))
                return
        self.set_exception(Exception("Could not mount disk at location {0} on {1}".format(self.disk_location, node.ip)))
        raise Exception()

    def _disk_full_recover_failure(self, node, recover_time):
        self._disk_full_failure(node)
        time.sleep(recover_time)
        self._recover_disk_full_failure(node)

    def _extra_files_in_log_dir(self, node, recover_time):
        self.add_extra_files_in_log_dir(node)
        time.sleep(recover_time)
        self.remove_extra_files_in_log_dir(node)

    def add_extra_files_in_log_dir(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.add_extra_files_index_log_dir(self.disk_location)
        if error:
            self.log.info(error)

    def remove_extra_files_in_log_dir(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.remove_extra_files_index_log_dir(self.disk_location)
        if error:
            self.log.info(error)

    def _dummy_file_in_log_dir(self, node, recover_time):
        self.add_dummy_file_in_log_dir(node)
        time.sleep(recover_time)
        self.remove_dummy_file_in_log_dir(node)

    def add_dummy_file_in_log_dir(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.add_dummy_file_index_log_dir(self.disk_location)
        if error:
            self.log.info(error)

    def shard_json_corruption(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.shard_json_corruption(self.disk_location)
        if error:
            self.log.info(error)

    def remove_dummy_file_in_log_dir(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.remove_dummy_file_index_log_dir(self.disk_location)
        if error:
            self.log.info(error)

    def _empty_file_in_log_dir(self, node, recover_time):
        self.add_empty_file_in_log_dir(node)
        time.sleep(recover_time)
        self.remove_dummy_file_in_log_dir(node)

    def add_empty_file_in_log_dir(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.add_empty_file_index_log_dir(self.disk_location)
        if error:
            self.log.info(error)

    def _enable_disable_disk_readonly(self, node, recover_time):
        self._enable_disk_readonly(node)
        time.sleep(recover_time)
        self._disable_disk_readonly(node)

    def _disk_full_failure(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.fill_disk_space(self.disk_location)
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
            #self.state = FINISHED
            #self.set_exception(Exception("Could not fill the disk at {0} on {1}".format(self.disk_location, node.ip)))

    def _recover_disk_full_failure(self, node):
        shell = RemoteMachineShellConnection(node)
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
                                             self.timeout_buffer + 200
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
            next(self)
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

    def __next__(self):
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
        elif self.failure_type == "stop_indexer":
            self._stop_indexer(self.current_failure_node)
        elif self.failure_type == "start_indexer":
            self._start_indexer(self.current_failure_node)
        elif self.failure_type == "block_indexer_port":
            self._block_indexer_port(self.current_failure_node)
        elif self.failure_type == "resume_indexer_port":
            self._resume_indexer_port(self.current_failure_node)
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

    def _block_indexer_port(self, node):
        shell = RemoteMachineShellConnection(node)
        self.log.info(f"Blocking port 9103 and 9105 on {node}")
        shell.execute_command("iptables -A INPUT -p tcp --destination-port 9103 -j DROP")
        shell.execute_command("iptables -A OUTPUT -p tcp --destination-port 9103 -j DROP")
        shell.execute_command("iptables -A INPUT -p tcp --destination-port 9105 -j DROP")
        shell.execute_command("iptables -A OUTPUT -p tcp --destination-port 9105 -j DROP")

    def _resume_indexer_port(self, node):
        shell = RemoteMachineShellConnection(node)
        self.log.info(f"Resuming port 9103 and 9105 on {node}")
        shell.execute_command("iptables -D INPUT -p tcp --destination-port 9103 -j DROP")
        shell.execute_command("iptables -D OUTPUT -p tcp --destination-port 9103 -j DROP")
        shell.execute_command("iptables -D INPUT -p tcp --destination-port 9105 -j DROP")
        shell.execute_command("iptables -D OUTPUT -p tcp --destination-port 9105 -j DROP")

    def _stop_indexer(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        o, r = shell.stop_indexer()
        self.log.info("Killed indexer. {0} {1}".format(o, r))
        node_failure_timer.result()
        self.start_time = node_failure_timer.start_time

    def _start_indexer(self, node):
        shell = RemoteMachineShellConnection(node)
        o, r = shell.start_indexer()
        self.log.info("Started back indexer. {0} {1}".format(o, r))
        shell.disconnect()

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
        o, r = shell.mount_partition(self.disk_location)
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
        shell = RemoteMachineShellConnection(node)
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

# Runs Magma docloader on slave
class MagmaDocLoader(Task):
    def __init__(self, server, bucket, sdk_docloader):
        Task.__init__(self, "MagmaDocLoader")
        self.server = server
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        else:
            self.bucket = bucket
        self.sdk_docloader = sdk_docloader

    def execute(self, task_manager):

        # added following to avoid breaking existing code
        if self.sdk_docloader.op_type == "create" and self.sdk_docloader.create_end == 0:
            self.sdk_docloader.create_end = self.sdk_docloader.end + 1

        command = f"java -jar magma_loader/DocLoader/target/magmadocloader/magmadocloader.jar -n {self.server.ip} " \
                  f"-user {self.sdk_docloader.username} -pwd {self.sdk_docloader.password} -b {self.bucket} " \
                  f"-p 11207 -create_s {self.sdk_docloader.create_start} -create_e {self.sdk_docloader.create_end} " \
                  f"-update_s {self.sdk_docloader.update_start} -update_e {self.sdk_docloader.update_end} " \
                  f"-delete_s {self.sdk_docloader.delete_start} -delete_e {self.sdk_docloader.delete_end} " \
                  f"-cr {self.sdk_docloader.percent_create} -up {self.sdk_docloader.percent_update} " \
                  f"-dl {self.sdk_docloader.percent_delete} -rd 0 -mutate {self.sdk_docloader.mutate} " \
                  f" -docSize {self.sdk_docloader.doc_size} -keyPrefix {self.sdk_docloader.key_prefix} " \
                  f"-scope {self.sdk_docloader.scope} -collection {self.sdk_docloader.collection} " \
                  f"-valueType {self.sdk_docloader.json_template} -dim {self.sdk_docloader.dim} " \
                  f"-ops {self.sdk_docloader.ops_rate}  -workers {self.sdk_docloader.workers} -model {self.sdk_docloader.model} -base64 {self.sdk_docloader.base64} -maxTTL 1800"

        self.log.info(command)
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            out = proc.communicate(timeout=self.sdk_docloader.timeout)
            if self.sdk_docloader.get_sdk_logs:
                self.sdk_docloader.set_sdk_results(out)
                self.log.info(out[0].decode("utf-8"))
            if proc.returncode != 0:
                raise Exception("Exception in magma loader to {}:{}\n{}".format(self.server.ip, self.bucket, out))
        except Exception as e:
            proc.terminate()
            self.state = FINISHED
            self.set_exception(e)
        proc.terminate()
        self.state = FINISHED
        self.set_result(True)
        self.check(task_manager)
        time.sleep(30)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)

# Runs java sdk client directly on slave
class SDKLoadDocumentsTask(Task):
    def __init__(self, server, bucket, sdk_docloader):
        Task.__init__(self, "SDKLoadDocumentsTask")
        self.server = server
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        else:
            self.bucket = bucket
        self.sdk_docloader = sdk_docloader

    def execute_for_collection(self, collection, start_seq_num_shift=0):
        import subprocess
        command = f"java -jar java_sdk_client/collections/target/javaclient/javaclient.jar " \
                  f"-i {self.server.ip} -u '{self.sdk_docloader.username}' -p '{self.sdk_docloader.password}' -b {self.bucket} " \
                  f"-s {self.sdk_docloader.scope} -c {collection} " \
                  f"-n {self.sdk_docloader.num_ops} -pc {self.sdk_docloader.percent_create} -pu {self.sdk_docloader.percent_update} " \
                  f"-pd {self.sdk_docloader.percent_delete} -l {self.sdk_docloader.load_pattern} " \
                  f"-dsn {self.sdk_docloader.start_seq_num + start_seq_num_shift} -dpx {self.sdk_docloader.key_prefix} -dt {self.sdk_docloader.json_template} " \
                  f"-de {self.sdk_docloader.doc_expiry} -ds {self.sdk_docloader.doc_size} -ac {self.sdk_docloader.all_collections} " \
                  f"-st {self.sdk_docloader.start+start_seq_num_shift} -en {self.sdk_docloader.end+start_seq_num_shift} -o {self.sdk_docloader.output} -sd {self.sdk_docloader.shuffle_docs} --secure {self.sdk_docloader.secure} -cpl {self.sdk_docloader.capella}"
        if self.sdk_docloader.es_compare:
            command = command + " -es true -es_host " + str(self.sdk_docloader.es_host) + " -es_port " + str(
                self.sdk_docloader.es_port) + \
                      " -es_login " + str(self.sdk_docloader.es_login) + " -es_password " + str(
                self.sdk_docloader.es_password)
        if self.sdk_docloader.op_type == "update":
            arr_fields_to_update = self.sdk_docloader.fields_to_update if self.sdk_docloader.fields_to_update else ""
            if len(arr_fields_to_update) > 0:
                command = command + " -fu "
                command = command + ",".join(arr_fields_to_update)
        self.log.info(command)
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            out = proc.communicate(timeout=self.sdk_docloader.timeout)
            if self.sdk_docloader.get_sdk_logs:
                self.sdk_docloader.set_sdk_results(out)
                self.log.info(out[0].decode("utf-8"))
            if proc.returncode != 0:
                raise Exception("Exception in java sdk client to {}:{}\n{}".format(self.server.ip, self.bucket, out))
        except Exception as e:
            proc.terminate()
            self.state = FINISHED
            self.set_exception(e)
        proc.terminate()
        self.state = FINISHED
        self.set_result(True)

    def execute(self, task_manager):
        if type(self.sdk_docloader.collection) is list:
            start_seq_num_shift = 0
            for c in self.sdk_docloader.collection:
                self.execute_for_collection(c, start_seq_num_shift)
                start_seq_num_shift = start_seq_num_shift + self.sdk_docloader.upd_del_shift
        else:
            self.execute_for_collection(self.sdk_docloader.collection)
        self.check(task_manager)
        #TODO additional sleep to let ES finish with docs indexing, should be replaced with something more intelligent.
        time.sleep(30)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)

# Runs java sdk client on a docker container on slave
class DockerSDKLoadDocumentsTask(Task):
    def __init__(self, server, bucket, sdk_docloader, pause_secs, timeout_secs):
        Task.__init__(self, "SDKLoadDocumentsTask")
        self.server = server
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        else:
            self.bucket = bucket
        self.params = sdk_docloader
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs

        from lib.collection.collections_dataloader import JavaSDKClient
        self.javasdkclient = JavaSDKClient(self.server, self.bucket, self.params)

    def execute(self, task_manager):
        try:
            self.javasdkclient.do_ops()
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            self.set_exception(Exception("Exception while loading data to {}:{}"
                                         .format(self.server.ip, self.bucket)))
        finally:
            self.javasdkclient.cleanup()

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)

#TODO:
# input params to include,exclude keywords,
# populate dictionary {node:matches} in setUp()
# call LogScanTask in basetestcase tearDown() and diff dict
# add sensitive data patterns
# pretty print matches
class LogScanTask(Task):
    def __init__(self, server, file_prefix):
        Task.__init__(self, "log_scan_task")
        self.server = server
        self.log_scan_file_name = f'{self.server.ip}_{file_prefix}'
        from lib.log_scanner import LogScanner
        exclude_keywords = TestInputSingleton.input.param("exclude_keywords", None)
        skip_security_scan = TestInputSingleton.input.param("skip_security_scan", False)
        self.log_scanner = LogScanner(server=self.server, exclude_keywords=exclude_keywords,
                                      skip_security_scan=skip_security_scan)

    def execute(self, task_manager):
        try:
            # Scan logs corresponding to node services
            matches = self.log_scanner.scan()
            target = open(self.log_scan_file_name, 'w+')
            target.write(str(matches))
            target.close()
            self.state = CHECKING
            task_manager.schedule(self)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        self.set_result(True)
        self.state = FINISHED
        task_manager.schedule(self)


class CreateServerlessDatabaseTask(Task):
    def __init__(self, api: CapellaAPI, config, databases: Dict[str, ServerlessDatabase], dataplanes: Dict[str, ServerlessDataPlane],
                 create_bypass_user=False):
        Task.__init__(self, "CreateServerlessDatabaseTask")
        self.api = api
        self.config = config
        self.databases = databases
        self.create_bypass_user = create_bypass_user
        self.dataplanes = dataplanes

    def execute(self, task_manager):
        try:
            self.log.info(
                "creating serverless database {}".format(self.config))
            self.database_id = self.api.create_serverless_database(self.config)
            self.databases[self.database_id] = ServerlessDatabase(self.database_id)
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)

    def check(self, task_manager):
        try:
            end_time = time.time() + 300
            database_healthy = False
            while time.time() < end_time and not database_healthy:
                complete = self.api.wait_for_database_step(self.database_id)
                if complete:
                    database_healthy = True
                    self.log.info("serverless database created {}".format(
                        {"database_id": self.database_id}))
                    self.log.info("allowing ip for serverless database {}".format(
                        {"database_id": self.database_id}))
                    self.api.allow_my_ip(self.database_id)
                    info = self.api.get_database_info(self.database_id)
                    self.log.info(f"Debug info for the serverless DB: {self.database_id}. Info: {info}")
                    self.log.info("generating API key for serverless database {}".format(
                        {"database_id": self.database_id}))
                    creds = self.api.generate_api_keys(self.database_id)
                    rest_api_info = None
                    if self.create_bypass_user:
                        self.log.info("Obtaining access to the dataplane nodes")
                        dataplane_id = self.api.get_resident_dataplane_id(database_id=self.database_id)
                        if dataplane_id not in self.dataplanes:
                            self.dataplanes[dataplane_id] = ServerlessDataPlane(dataplane_id)
                            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=dataplane_id)
                            self.dataplanes[dataplane_id].populate(rest_api_info)
                        else:
                            self.log.info("Tenant database resides on a DP that already has a bypass user. Reusing it")
                            rest_api_info = {"couchbaseCreds": {"username": self.dataplanes[dataplane_id].admin_username,
                                                                "password": self.dataplanes[dataplane_id].admin_password},
                                             "srv": self.dataplanes[dataplane_id].rest_srv}
                        self.log.info(f"Bypass username: {rest_api_info['couchbaseCreds']['username']}. "
                                      f"Password: {rest_api_info['couchbaseCreds']['password']}. SRV {rest_api_info['srv']} "
                                      f"for dataplane {dataplane_id}")
                    self.databases[self.database_id].populate(info, creds, rest_api_info)
                    self.state = FINISHED
                    self.set_result(self.database_id)
            if not database_healthy:
                raise Exception(f"Database {self.database_id} not healthy despite waiting 5 mins")
        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)
