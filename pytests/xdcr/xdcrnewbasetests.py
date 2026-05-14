import copy
import json
import logging
import os
import random
import re
import time
import unittest
import uuid
import base64
import urllib.parse
import yaml
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Tuple

import logger
from collection.collections_rest_client import CollectionsRest
from couchbase_cli import CouchbaseCLI
from couchbase_helper.cluster import Cluster
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator, SDKDataLoader
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection, Bucket
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from security.auditmain import audit
from security.ntonencryptionBase import ntonencryptionBase
from security.rbac_base import RbacBase
from security.x509_multiple_CA_util import x509main
from testconstants import STANDARD_BUCKET_PORT, WIN_COUCHBASE_LOGS_PATH

from TestInput import TestInputSingleton
from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.exception import XDCRException
from lib.SystemEventLogLib.Events import EventHelper
from lib.SystemEventLogLib.xdcr_events import XDCRServiceEvents
from lib import global_vars
from scripts import collect_data_files
from scripts.collect_server_info import cbcollectRunner
from scripts.java_sdk_setup import JavaSdkSetup

@dataclass
class LoadResult:
    success_pairs: List[Tuple[str, str]] = field(default_factory=list)
    failed_pairs: List[Tuple[str, str]] = field(default_factory=list)
    errors: dict = field(default_factory=dict)
    total_docs_loaded: int = 0
    elapsed_seconds: float = 0.0

    @property
    def total_attempted(self):
        return len(self.success_pairs) + len(self.failed_pairs)

    @property
    def success_rate(self):
        if self.total_attempted == 0:
            return 0.0
        return len(self.success_pairs) / self.total_attempted


class RenameNodeException(XDCRException):

    """Exception thrown when converting ip to hostname failed
    """

    def __init__(self, msg=''):
        XDCRException.__init__(self, msg)


class RebalanceNotStopException(XDCRException):

    """Exception thrown when stopping rebalance failed
    """

    def __init__(self, msg=''):
        XDCRException.__init__(self, msg)


def raise_if(cond, ex):
    """Raise Exception if condition is True
    """
    if cond:
        raise ex


class CONNECTIVITY_STATUS:
    RC_OK = "RC_OK"
    RC_DEGRADED = "RC_DEGRADED"
    RC_ERROR = "RC_ERROR"
    RC_AUTH_ERR = "RC_AUTH_ERR"


class TOPOLOGY:
    CHAIN = "chain"
    STAR = "star"
    RING = "ring"
    HYBRID = "hybrid"


class REPLICATION_DIRECTION:
    UNIDIRECTION = "unidirection"
    BIDIRECTION = "bidirection"


class REPLICATION_TYPE:
    CONTINUOUS = "continuous"


class REPLICATION_PROTOCOL:
    CAPI = "capi"
    XMEM = "xmem"


class INPUT:
    REPLICATION_DIRECTION = "rdirection"
    CLUSTER_TOPOLOGY = "ctopology"
    SEED_DATA = "sdata"
    SEED_DATA_MODE = "sdata_mode"
    SEED_DATA_OPERATION = "sdata_op"
    POLL_INTERVAL = "poll_interval"  # in seconds
    POLL_TIMEOUT = "poll_timeout"  # in seconds
    SEED_DATA_MODE_SYNC = "sync"


class OPS:
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    APPEND = "append"


class EVICTION_POLICY:
    VALUE_ONLY = "valueOnly"
    FULL_EVICTION = "fullEviction"
    NO_EVICTION = "noEviction"
    NRU_EVICTION = "nruEviction"
    CB = [VALUE_ONLY, FULL_EVICTION]
    EPH = [NO_EVICTION, NRU_EVICTION]

class BUCKET_PRIORITY:
    HIGH = "high"


class BUCKET_NAME:
    DEFAULT = "default"


class OS:
    WINDOWS = "windows"
    LINUX = "linux"
    OSX = "osx"


class COMMAND:
    SHUTDOWN = "shutdown"
    REBOOT = "reboot"


class STATE:
    RUNNING = "running"


class REPL_PARAM:
    FAILURE_RESTART = "failureRestartInterval"
    CHECKPOINT_INTERVAL = "checkpointInterval"
    OPTIMISTIC_THRESHOLD = "optimisticReplicationThreshold"
    FILTER_EXP = "filterExpression"
    FILTER_SKIP_RESTREAM = "filterSkipRestream"
    FILTER_BINARY = "filterBinary"
    SOURCE_NOZZLES = "sourceNozzlePerNode"
    TARGET_NOZZLES = "targetNozzlePerNode"
    BATCH_COUNT = "workerBatchSize"
    BATCH_SIZE = "docBatchSizeKb"
    LOG_LEVEL = "logLevel"
    MAX_REPLICATION_LAG = "maxExpectedReplicationLag"
    TIMEOUT_PERC = "timeoutPercentageCap"
    PAUSE_REQUESTED = "pauseRequested"
    PRIORITY = "priority"
    DESIRED_LATENCY = "desiredLatency"
    COMPRESSION_TYPE = "compressionType"
    EXPLICIT_MAPPING = "collectionsExplicitMapping"
    MAPPING_RULES = "colMappingRules"
    MIGRATION_MODE = "collectionsMigrationMode"
    MIRRORING_MODE = "collectionsMirroringMode"
    CONFLICT_LOGGING = "conflictLogging"
    DCP_FLOW_CONTROL_THROTTLE = "dcpFlowControlThrottle"
    COMPONENT_EVENTS_CHAN_LENGTH = "componentEventsChanLength"


class TEST_XDCR_PARAM:
    FAILURE_RESTART = "failure_restart_interval"
    CHECKPOINT_INTERVAL = "checkpoint_interval"
    OPTIMISTIC_THRESHOLD = "optimistic_threshold"
    FILTER_EXP = "filter_expression"
    FILTER_SKIP_RESTREAM = "filter_skip_restream"
    FILTER_BINARY = "filter_binary"
    SOURCE_NOZZLES = "source_nozzles"
    TARGET_NOZZLES = "target_nozzles"
    BATCH_COUNT = "batch_count"
    BATCH_SIZE = "batch_size"
    LOG_LEVEL = "log_level"
    MAX_REPLICATION_LAG = "max_replication_lag"
    TIMEOUT_PERC = "timeout_percentage"
    PRIORITY = "priority"
    DESIRED_LATENCY = "desired_latency"
    COMPRESSION_TYPE = "compression_type"
    EXPLICIT_MAPPING = "explicit_mapping"
    MAPPING_RULES = "colmapping_rules"
    MIGRATION_MODE = "migration_mode"
    MIRRORING_MODE = "mirroring_mode"
    CONFLICT_LOGGING = "conflict_logging"
    @staticmethod
    def get_test_to_create_repl_param_map():
        return {
            TEST_XDCR_PARAM.FAILURE_RESTART: REPL_PARAM.FAILURE_RESTART,
            TEST_XDCR_PARAM.CHECKPOINT_INTERVAL: REPL_PARAM.CHECKPOINT_INTERVAL,
            TEST_XDCR_PARAM.OPTIMISTIC_THRESHOLD: REPL_PARAM.OPTIMISTIC_THRESHOLD,
            TEST_XDCR_PARAM.FILTER_EXP: REPL_PARAM.FILTER_EXP,
            TEST_XDCR_PARAM.FILTER_SKIP_RESTREAM: REPL_PARAM.FILTER_SKIP_RESTREAM,
            TEST_XDCR_PARAM.FILTER_BINARY: REPL_PARAM.FILTER_BINARY,
            TEST_XDCR_PARAM.SOURCE_NOZZLES: REPL_PARAM.SOURCE_NOZZLES,
            TEST_XDCR_PARAM.TARGET_NOZZLES: REPL_PARAM.TARGET_NOZZLES,
            TEST_XDCR_PARAM.BATCH_COUNT: REPL_PARAM.BATCH_COUNT,
            TEST_XDCR_PARAM.BATCH_SIZE: REPL_PARAM.BATCH_SIZE,
            TEST_XDCR_PARAM.MAX_REPLICATION_LAG: REPL_PARAM.MAX_REPLICATION_LAG,
            TEST_XDCR_PARAM.TIMEOUT_PERC: REPL_PARAM.TIMEOUT_PERC,
            TEST_XDCR_PARAM.LOG_LEVEL: REPL_PARAM.LOG_LEVEL,
            TEST_XDCR_PARAM.PRIORITY: REPL_PARAM.PRIORITY,
            TEST_XDCR_PARAM.DESIRED_LATENCY: REPL_PARAM.DESIRED_LATENCY,
            TEST_XDCR_PARAM.COMPRESSION_TYPE: REPL_PARAM.COMPRESSION_TYPE,
            TEST_XDCR_PARAM.EXPLICIT_MAPPING: REPL_PARAM.EXPLICIT_MAPPING,
            TEST_XDCR_PARAM.MAPPING_RULES: REPL_PARAM.MAPPING_RULES,
            TEST_XDCR_PARAM.MIGRATION_MODE: REPL_PARAM.MIGRATION_MODE,
            TEST_XDCR_PARAM.MIRRORING_MODE: REPL_PARAM.MIRRORING_MODE,
            TEST_XDCR_PARAM.CONFLICT_LOGGING: REPL_PARAM.CONFLICT_LOGGING
        }


class XDCR_PARAM:
    # Per-replication params (input)
    XDCR_FAILURE_RESTART = "xdcrFailureRestartInterval"
    XDCR_CHECKPOINT_INTERVAL = "xdcrCheckpointInterval"
    XDCR_OPTIMISTIC_THRESHOLD = "xdcrOptimisticReplicationThreshold"
    XDCR_FILTER_EXP = "xdcrFilterExpression"
    XDCR_FILTER_SKIP_RESTREAM = "xdcrfilterSkipRestream"
    XDCR_SOURCE_NOZZLES = "xdcrSourceNozzlePerNode"
    XDCR_TARGET_NOZZLES = "xdcrTargetNozzlePerNode"
    XDCR_BATCH_COUNT = "xdcrWorkerBatchSize"
    XDCR_BATCH_SIZE = "xdcrDocBatchSizeKb"
    XDCR_LOG_LEVEL = "xdcrLogLevel"
    XDCR_MAX_REPLICATION_LAG = "xdcrMaxExpectedReplicationLag"
    XDCR_TIMEOUT_PERC = "xdcrTimeoutPercentageCap"
    XDCR_PRIORITY = "xdcrPriority"
    XDCR_DESIRED_LATENCY = "xdcrDesiredLatency"
    XDCR_COMPRESSION_TYPE = "xdcrCompressionType"
    XDCR_CONFLICT_LOGGING = "xdcrConflictLogging"


class CHECK_AUDIT_EVENT:
    CHECK = False

# Event Definition:
# https://github.com/couchbase/goxdcr/blob/master/etc/audit_descriptor.json

class GO_XDCR_AUDIT_EVENT_ID:
    CREATE_CLUSTER = 16384
    MOD_CLUSTER = 16385
    RM_CLUSTER = 16386
    CREATE_REPL = 16387
    PAUSE_REPL = 16388
    RESUME_REPL = 16389
    CAN_REPL = 16390
    DEFAULT_SETT = 16391
    IND_SETT = 16392

class BUCKET_COLLECTIONS_DENSITY:
    def __init__(self, density):
        self.density_map = {
            "low": {"num_scopes": 1, "num_collections_per_scope": 1, "num_docs_per_collection": 1000},
            "medium": {"num_scopes": 5, "num_collections_per_scope": 10, "num_docs_per_collection": 1000},
            "high": {"num_scopes": 10, "num_collections_per_scope": 20, "num_docs_per_collection": 1000}
        }
        self.density = density
        self.num_scopes = 0
        self.num_collections = 0
        self.num_docs = 0
        if self.density == "random":
            self.num_scopes = random.randint(1, 99)
            max_collections = int(1000 / self.num_scopes) - 1
            self.num_collections = random.randint(1, max_collections)
            while self.num_scopes*self.num_collections > 1000:
                self.num_scopes //= 10
                self.num_collections //= 10
            self.num_docs = random.randint(100, 10000)
        else:
            self.num_scopes = self.density_map[self.density]["num_scopes"]
            self.num_collections = self.density_map[self.density]["num_collections_per_scope"]
            self.num_docs = self.density_map[self.density]["num_docs_per_collection"]

class NodeHelper:
    _log = logger.Logger.get_logger()

    @staticmethod
    def disable_firewall(
            server, rep_direction=REPLICATION_DIRECTION.UNIDIRECTION):
        """Disable firewall to put restriction to replicate items in XDCR.
        @param server: server object to disable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()

        if shell.info.type.lower() == "windows":
            output, error = shell.execute_command('netsh advfirewall set publicprofile state off')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall set privateprofile state off')
            shell.log_command_output(output, error)
            # for details see RemoteUtilHelper.enable_firewall for windows
            output, error = shell.execute_command('netsh advfirewall firewall delete rule name="block erl.exe in"')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall firewall delete rule name="block erl.exe out"')
            shell.log_command_output(output, error)
        else:
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            o, r = shell.execute_command(
                "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j ACCEPT")
            shell.log_command_output(o, r)
            if rep_direction == REPLICATION_DIRECTION.BIDIRECTION:
                o, r = shell.execute_command(
                    "/sbin/iptables -A OUTPUT -p tcp -o eth0 --dport 1000:65535 -j ACCEPT")
                shell.log_command_output(o, r)
            o, r = shell.execute_command(
                "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
            shell.log_command_output(o, r)
            # self.log.info("enabled firewall on {0}".format(server))
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def reboot_server(server, test_case, wait_timeout=60):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.reboot_node()
        # wait for restart and warmup on all node
        ClusterOperationHelper.wait_for_ns_servers_or_assert([server], test_case, wait_if_warmup=True)
        # disable firewall on these nodes
        remote_client.disable_firewall()
        remote_client.disconnect()

    @staticmethod
    def enable_firewall(
            server, rep_direction=REPLICATION_DIRECTION.UNIDIRECTION):
        """Enable firewall
        @param server: server object to enable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        is_bidirectional = rep_direction == REPLICATION_DIRECTION.BIDIRECTION
        RemoteUtilHelper.enable_firewall(
            server,
            bidirectional=is_bidirectional,
            xdcr=True)

    @staticmethod
    def do_a_warm_up(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.restart_couchbase()
        shell.disconnect()

    @staticmethod
    def wait_service_started(server, wait_time=120):
        """Function will wait for Couchbase service to be in
        running phase.
        """
        shell = RemoteMachineShellConnection(server)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, _ = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                # self.log.info("Couchbase service is running")
                return
            time.sleep(10)
        raise Exception(
            "Couchbase service is not running after {0} seconds".format(
                wait_time))

    @staticmethod
    def wait_warmup_completed(warmupnodes, bucket_names=["default"]):
        if isinstance(bucket_names, str):
            bucket_names = [bucket_names]
        start = time.time()
        for server in warmupnodes:
            for bucket in bucket_names:
                while time.time() - start < 150:
                    try:
                        mc = MemcachedClientHelper.direct_client(server, bucket)
                        if mc.stats()["ep_warmup_thread"] == "complete":
                            NodeHelper._log.info(
                                "Warmed up: %s items on %s on %s" %
                                (mc.stats("warmup")["ep_warmup_key_count"], bucket, server))
                            time.sleep(10)
                            break
                        elif mc.stats()["ep_warmup_thread"] == "running":
                            NodeHelper._log.info(
                                "Still warming up .. ep_warmup_key_count : %s" % (mc.stats("warmup")["ep_warmup_key_count"]))
                            continue
                        else:
                            NodeHelper._log.info(
                                "Value of ep_warmup_thread does not exist, exiting from this server")
                            break
                    except Exception as e:
                        NodeHelper._log.info(e)
                        time.sleep(10)
                if mc.stats()["ep_warmup_thread"] == "running":
                    NodeHelper._log.info(
                            "ERROR: ep_warmup_thread's status not complete")
                mc.close()


    @staticmethod
    def wait_node_restarted(
            server, test_case, wait_time=120, wait_if_warmup=False,
            check_service=False):
        """Wait server to be re-started
        """
        now = time.time()
        if check_service:
            NodeHelper.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time // 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [server], test_case, wait_time=wait_time - num * 10,
                    wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                time.sleep(10)

    @staticmethod
    def kill_erlang(server):
        """Kill erlang process running on server.
        """
        NodeHelper._log.info("Killing erlang on server: {0}".format(server))
        shell = RemoteMachineShellConnection(server)
        os_info = shell.extract_remote_info()
        shell.kill_erlang(os_info)
        shell.disconnect()

    @staticmethod
    def kill_memcached(server):
        """Kill memcached process running on server.
        """
        shell = RemoteMachineShellConnection(server)
        shell.kill_erlang()
        shell.disconnect()

    @staticmethod
    def kill_goxdcr(server):
        """Kill goxdcr process running on server.
        """
        shell = RemoteMachineShellConnection(server)
        shell.kill_goxdcr()
        shell.disconnect()

    @staticmethod
    def get_goxdcr_log_dir(node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval('filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir).replace('"', '')

    @staticmethod
    def check_goxdcr_log(server, search_str, goxdcr_log=None, print_matches=None, log_name=None, timeout=0):
        """ Checks if a string 'str' is present in 'log_name' on 'server'
            and returns the number of occurances
            @param goxdcr_log: goxdcr log location on the server
            @timeout: search every 10 seconds until timeout
        """
        if not log_name:
            log_name = "goxdcr.log"
        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info().type.lower()
        if info == "windows":
            goxdcr_log = WIN_COUCHBASE_LOGS_PATH + log_name + '*'
            cmd = "grep "
        else:
            if not goxdcr_log:
                goxdcr_log = NodeHelper.get_goxdcr_log_dir(server) \
                             + '/' + log_name + '*'
            cmd = "zgrep "
        cmd += "\"{0}\" {1}".format(search_str, goxdcr_log)
        iter = 0
        count = 0
        matches = []
        # Search 5 times with a break of timeout sec
        while iter < 5:
            matches, err = shell.execute_command(cmd)
            count = len(matches)
            if count > 0 or timeout == 0:
                break
            else:
                NodeHelper._log.info("Waiting {0}s for {1} to appear in {2} ..".format(timeout, search_str, log_name))
                time.sleep(timeout)
            iter += 1
        shell.disconnect()
        NodeHelper._log.info(count)
        if print_matches:
            NodeHelper._log.info(matches)
        return matches, count

    @staticmethod
    def rename_nodes(servers):
        """Rename server name from ip to their hostname
        @param servers: list of server objects.
        @return: dictionary whose key is server and value is hostname
        """
        hostnames = {}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                if "ec2" in str(server.ip):
                    hostname = shell.get_aws_public_hostname()
                elif "azure" in str(server.ip):
                    hostname = str(server.ip)
                else:
                    hostname = shell.get_full_hostname()
                rest = RestConnection(server)
                renamed, content = rest.rename_node(
                    hostname, username=server.rest_username,
                    password=server.rest_password)
                raise_if(
                    not renamed,
                    RenameNodeException(
                        "Server %s is not renamed! Hostname %s. Error %s" % (
                            server, hostname, content)
                    )
                )
                hostnames[server] = hostname
                server.hostname = hostname
            finally:
                shell.disconnect()
        return hostnames

    # Returns version like "x.x.x" after removing build number
    @staticmethod
    def get_cb_version(node):
        rest = RestConnection(node)
        version = rest.get_nodes_self().version
        return version[:version.rfind('-')]

    @staticmethod
    def set_wall_clock_time(node, date_str):
        shell = RemoteMachineShellConnection(node)
        # os_info = shell.extract_remote_info()
        # if os_info == OS.LINUX:
        # date command works on linux and windows cygwin as well.
        shell.execute_command(
            "sudo date -s '%s'" %
            time.ctime(
                date_str.tx_time))
        # elif os_info == OS.WINDOWS:
        #    raise "NEED To SETUP DATE COMMAND FOR WINDOWS"

    @staticmethod
    def get_cbcollect_info(servers):
        """Collect cbcollectinfo logs for all the servers in the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        path = path or "."
        runner = cbcollectRunner(servers, path)
        runner.run()
        if len(runner.succ) > 0:
            TestInputSingleton.input.test_params[
                "get-cbcollect-info"] = False
        for (server, e) in runner.fail:
            NodeHelper._log.error(
                "IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(
                    server.ip,
                    e))
    @staticmethod
    def collect_data_files(server):
        """Collect bucket data files for all the servers in the cluster.
        Data files are collected only if data is not verified on the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        collect_data_files.cbdatacollectRunner(server, path).run()

    @staticmethod
    def collect_logs(servers, cluster_run=False):
        """Grab cbcollect before we cleanup
        """
        NodeHelper.get_cbcollect_info(servers)
        if not cluster_run:
            for server in servers:
                NodeHelper.collect_data_files(server)

    @staticmethod
    def setup_x509_certificates(cluster, encryption_type=None):
        """Generate and upload x509 certificates for a cluster.
        
        Generates CA-signed certificates using x509main utility and uploads
        them to all nodes in the cluster. Uses unencrypted PKCS8 keys by default.
        
        @param cluster: CouchbaseCluster object.
        @param encryption_type: Key encryption type (None for unencrypted,
                                'aes256' for encrypted). Defaults to None.
        """
        import logger
        log = logger.Logger.get_logger()
        from lib.Cb_constants.CBServer import CbServer
        
        master = cluster.get_master_node()
        CbServer.x509 = x509main(host=master, encryption_type=encryption_type)

        # Clean up any old certs
        for server in cluster.get_nodes():
            CbServer.x509.delete_inbox_folder_on_server(server=server)

        # Generate new CA-signed certificates
        CbServer.x509.generate_multiple_x509_certs(servers=cluster.get_nodes())

        # Upload root CAs to all nodes
        for server in cluster.get_nodes():
            CbServer.x509.upload_root_certs(server)

        # Upload node certificates
        CbServer.x509.upload_node_certs(servers=cluster.get_nodes())

        log.info("x509 certificates configured on cluster {0}".format(
            cluster.get_name()))

    @staticmethod
    def teardown_x509_certificates(clusters):
        """Remove x509 certificates from all nodes in given clusters.
        Retries once on failure to handle transient SSH/REST errors that
        could leave stale certs on VMs.
        
        @param clusters: List of CouchbaseCluster objects.
        """
        import logger
        log = logger.Logger.get_logger()
        from lib.Cb_constants.CBServer import CbServer
        
        if not CbServer.x509:
            return

        for attempt in range(2):
            try:
                for cluster in clusters:
                    CbServer.x509.teardown_certs(servers=cluster.get_nodes())
                return
            except Exception as e:
                if attempt == 0:
                    log.warning("Certificate teardown failed (attempt 1), "
                                "retrying in 5s: {0}".format(e))
                    time.sleep(5)
                else:
                    log.error("Certificate teardown failed after 2 attempts: "
                              "{0}".format(e))

    @staticmethod
    def get_ca_paths_for_node(node_ip):
        """Get intermediate CA paths for a given node.
        
        Looks up the CA that signed the node's certificate and returns
        paths to the intermediate CA key and certificate.
        
        @param node_ip: IP address of the node.
        @return: Tuple of (int_ca_key_path, int_ca_pem_path, int_ca_name).
        @raises Exception: If no certificate is found for the node.
        """
        from lib.Cb_constants.CBServer import CbServer
        
        x509_obj = CbServer.x509
        if not x509_obj:
            raise Exception("x509main not initialized. Call setup_x509_certificates first.")
        
        node_ca_info = x509_obj.node_ca_map.get(node_ip)
        if not node_ca_info:
            raise Exception(
                "No node certificate found for {0}".format(node_ip))

        int_ca_name = node_ca_info["signed_by"]
        root_ca_name = int_ca_name.split("_")[1]
        root_ca_dir = x509_obj.CACERTFILEPATH + root_ca_name + "/"
        int_ca_dir = root_ca_dir + int_ca_name + "/"
        int_ca_key = int_ca_dir + "int.key"
        int_ca_pem = int_ca_dir + "int.pem"
        
        return int_ca_key, int_ca_pem, int_ca_name

    @staticmethod
    def create_cert_with_san(cert_dir, cert_name, san_ips, int_ca_key, int_ca_pem,
                            cn="service.couchbase.svc", dns_names=None):
        """Generate a certificate with multiple IP/DNS SANs signed by an intermediate CA.
        
        Useful for creating certificates for load balancers, gateways, or services
        that need to be valid for multiple IPs or DNS names.
        
        @param cert_dir: Local directory to store cert files.
        @param cert_name: Base name for cert files (e.g., 'cng', 'haproxy').
        @param san_ips: List of IP addresses for subjectAltName (e.g., ['192.168.1.1', '192.168.1.2']).
        @param int_ca_key: Path to intermediate CA private key.
        @param int_ca_pem: Path to intermediate CA certificate.
        @param cn: Common Name for the certificate (default: 'service.couchbase.svc').
        @param dns_names: Optional list of DNS names for subjectAltName (e.g., ['*.example.com']).
        @return: Tuple of (key_path, cert_path, chain_path).
        """
        import os
        import subprocess
        
        os.makedirs(cert_dir, exist_ok=True)

        key_path = os.path.join(cert_dir, "{0}.key".format(cert_name))
        csr_path = os.path.join(cert_dir, "{0}.csr".format(cert_name))
        cert_path = os.path.join(cert_dir, "{0}.pem".format(cert_name))
        chain_path = os.path.join(cert_dir, "{0}_chain.pem".format(cert_name))
        ext_path = os.path.join(cert_dir, "{0}.ext".format(cert_name))

        # Build SAN string with IPs and optional DNS names
        san_entries = ["IP:{0}".format(ip) for ip in san_ips]
        if dns_names:
            san_entries.extend(["DNS:{0}".format(dns) for dns in dns_names])
        san_str = ",".join(san_entries)
        
        # Write extensions file with SANs
        with open(ext_path, "w") as f:
            f.write("basicConstraints=CA:FALSE\n")
            f.write("subjectAltName = {0}\n".format(san_str))

        # Generate private key
        subprocess.check_call([
            "openssl", "genrsa", "-out", key_path, "2048"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Generate CSR
        subprocess.check_call([
            "openssl", "req", "-new", "-key", key_path,
            "-out", csr_path,
            "-subj", "/C=UA/O=MyCompany/OU=TestRunner/CN={0}".format(cn)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Sign with intermediate CA
        subprocess.check_call([
            "openssl", "x509", "-req", "-in", csr_path,
            "-CA", int_ca_pem, "-CAkey", int_ca_key,
            "-CAcreateserial", "-out", cert_path,
            "-days", "365", "-sha256",
            "-extfile", ext_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Create chain: cert + intermediate CA
        with open(chain_path, "w") as chain:
            for pem_file in [cert_path, int_ca_pem]:
                with open(pem_file, "r") as f:
                    chain.write(f.read())

        return key_path, cert_path, chain_path

    GOXDCR_DEFAULT_SCAN_PATTERNS = [
        "panic", "fatal error", "nil pointer", "ERRO",
    ]

    @staticmethod
    def scan_goxdcr_log(cluster, label, patterns=None, rc_name=None,
                        repl_ids=None, tail_lines=10, extra_patterns=None):
        """Scan goxdcr.log on every node of a cluster for one or more patterns.

        @param patterns: list of regex/literal patterns. Defaults to
                         GOXDCR_DEFAULT_SCAN_PATTERNS (panic / nil pointer / ERRO).
        @param rc_name: when supplied, also greps for the remote-cluster name.
        @param repl_ids: replication IDs added to the search list so hits are
                         attributable to a specific pipeline.
        @param tail_lines: how many trailing matches to log per pattern hit.
        @return: dict {pattern: [(node_ip, count)]}.
        """
        patterns = list(patterns or NodeHelper.GOXDCR_DEFAULT_SCAN_PATTERNS)
        if extra_patterns:
            patterns.extend(extra_patterns)
        if rc_name:
            patterns.append(rc_name)
        if repl_ids:
            for rid in repl_ids:
                if rid:
                    patterns.append(rid)

        results = {}
        for node in cluster.get_nodes():
            try:
                goxdcr_log = NodeHelper.get_goxdcr_log_dir(node) + "/goxdcr.log*"
            except Exception as e:
                NodeHelper._log.warning(
                    "[{0}] goxdcr log dir lookup failed on {1}: {2}".format(
                        label, node.ip, e))
                continue
            for pattern in patterns:
                try:
                    matches, count = NodeHelper.check_goxdcr_log(
                        node, pattern, goxdcr_log, print_matches=False)
                except Exception as e:
                    NodeHelper._log.warning(
                        "[{0}] goxdcr scan on {1} for '{2}' failed: {3}".format(
                            label, node.ip, pattern, e))
                    continue
                results.setdefault(pattern, []).append((node.ip, count))
                if count:
                    tail = matches[-tail_lines:] if isinstance(matches, list) \
                        else [matches]
                    NodeHelper._log.info(
                        "[{0}] goxdcr.log {1} pattern='{2}' count={3}".format(
                            label, node.ip, pattern, count))
                    for line in tail:
                        NodeHelper._log.info("[{0}]   {1}".format(label, str(line).rstrip()))
        return results

    @staticmethod
    def start_pillowfight_burst_loaders(src_master, src_buckets, items_per_worker,
                                        docsize, rate_limit, workers_per_bucket):
        """Launch parallel cbc-pillowfight workers across every bucket.

        @return: list of (shell, pid, key_prefix) tuples; pass to
                 wait_for_pillowfight_completion or kill manually for cleanup.
        """
        loaders = []
        for bucket in src_buckets:
            for worker_id in range(workers_per_bucket):
                shell = RemoteMachineShellConnection(src_master)
                key_prefix = "burst-{0}-{1}".format(bucket.name, worker_id)
                cmd = (
                    "nohup /opt/couchbase/bin/cbc-pillowfight "
                    "-u {user} -P {pw} -U couchbase://localhost/{bucket} "
                    "-I {items} -m {sz} -M {sz} -B 1000 --rate-limit={rl} "
                    "--populate-only --collection _default._default "
                    "--key-prefix {kp}- "
                    ">/tmp/{kp}.log 2>&1 & echo $!"
                ).format(
                    user=src_master.rest_username, pw=src_master.rest_password,
                    bucket=bucket.name, items=items_per_worker, sz=docsize,
                    rl=rate_limit, kp=key_prefix)
                out, _ = shell.execute_command(cmd)
                pid = (out[0].strip() if out else "")
                NodeHelper._log.info(
                    "Launched burst worker {0} on bucket '{1}', pid={2}".format(
                        worker_id, bucket.name, pid))
                loaders.append((shell, pid, key_prefix))
        return loaders

    @staticmethod
    def wait_for_pillowfight_completion(loaders, timeout):
        """Wait for cbc-pillowfight workers to exit; force-kill any still
        running after the timeout. Always disconnects shells."""
        end_time = time.time() + timeout
        for shell, pid, label in loaders:
            try:                
                if not pid or not str(pid).isdigit():
                    NodeHelper._log.warning(
                        "Burst worker '{0}' has empty/non-numeric pid={1!r}; "
                        "skipping wait+kill".format(label, pid))
                    continue
                while time.time() < end_time:
                    out, _ = shell.execute_command(
                        "kill -0 {0} 2>/dev/null && echo RUNNING || echo DONE".format(pid))
                    if out and "DONE" in out[0]:
                        break
                    time.sleep(5)
                else:
                    NodeHelper._log.warning(
                        "Burst worker {0} (pid={1}) did not exit; killing".format(
                            label, pid))
                    shell.execute_command("kill -9 {0} 2>/dev/null || true".format(pid))
            finally:
                shell.disconnect()

    @staticmethod
    def setup_x509_certificates_multi_cluster(clusters, encryption_type=None):
        """Generate one trust chain across multiple clusters and upload to all
        nodes. Superset of setup_x509_certificates: cross-cluster trusted-CA
        propagation + master load step that XDCR replication needs.

        @param clusters: list of CouchbaseCluster.
        @param encryption_type: x509main encryption_type (None for unencrypted).
        """
        all_nodes = []
        for cluster in clusters:
            all_nodes.extend(cluster.get_nodes())

        master = clusters[0].get_master_node()
        CbServer.x509 = x509main(host=master, encryption_type=encryption_type)

        for node in all_nodes:
            CbServer.x509.delete_inbox_folder_on_server(server=node)

        CbServer.x509.generate_multiple_x509_certs(servers=all_nodes)

        for node in all_nodes:
            CbServer.x509.copy_trusted_CAs(
                root_ca_names=CbServer.x509.root_ca_names, server=node)

        for cluster in clusters:
            CbServer.x509.load_trusted_CAs(server=cluster.get_master_node())

        CbServer.x509.upload_node_certs(servers=all_nodes)
        NodeHelper._log.info(
            "x509 certificates configured on {0} clusters ({1} nodes)".format(
                len(clusters), len(all_nodes)))

    @staticmethod
    def teardown_x509_certificates_with_retry(clusters, max_attempts=4,
                                              log_fd_fn=None):
        """EMFILE-aware x509 teardown.

        Tests that open many paramiko transports (per-node TLS install, gateway
        setup, kill/restart cycles) can hit RLIMIT_NOFILE during teardown; the
        shared 2-attempt retry isn't enough — bailing halfway leaves stale
        trusted CAs that fail the next run's add_node TLS handshake. Force gc
        between attempts and retry on EMFILE specifically.
        """
        import errno
        import gc as _gc
        if not CbServer.x509:
            return
        last_err = None
        for attempt in range(1, max_attempts + 1):
            _gc.collect()
            if log_fd_fn:
                log_fd_fn("pre-cert-teardown attempt {0}".format(attempt))
            try:
                for cluster in clusters:
                    CbServer.x509.teardown_certs(servers=cluster.get_nodes())
                return
            except OSError as e:
                last_err = e
                if e.errno == errno.EMFILE and attempt < max_attempts:
                    NodeHelper._log.warning(
                        "Certificate teardown hit EMFILE on attempt {0}/{1};"
                        " forcing gc and retrying in {2}s".format(
                            attempt, max_attempts, attempt * 5))
                    _gc.collect()
                    time.sleep(attempt * 5)
                    continue
                raise
            except Exception as e:
                last_err = e
                if attempt < max_attempts:
                    NodeHelper._log.warning(
                        "Certificate teardown failed on attempt {0}/{1}: {2};"
                        " retrying in {3}s".format(
                            attempt, max_attempts, e, attempt * 5))
                    _gc.collect()
                    time.sleep(attempt * 5)
                    continue
                raise
        if last_err is not None:
            NodeHelper._log.error(
                "Certificate teardown failed after {0} attempts: {1}".format(
                    max_attempts, last_err))

    @staticmethod
    def provision_certs_for_floating_servers(num_nodes, floating_pool):
        """Pre-provision certs on the next num_nodes floating servers that
        async_rebalance_in / async_swap_rebalance will consume.

        CouchbaseCluster rebalance pops from the tail of the pool. Cluster-
        wide cert setup only covers already-joined nodes, so joiners would
        arrive with no node cert and no trusted CAs — add_node fails the
        TLS handshake with "unknown CA". Idempotent.
        """
        if CbServer.x509 is None:
            raise Exception(
                "x509 not initialised; setup_x509_certificates_multi_cluster "
                "must run before pre-provisioning floating-server certs")
        if len(floating_pool) < num_nodes:
            raise Exception(
                "Not enough floating servers ({0}) to provision for a "
                "{1}-node rebalance-in".format(len(floating_pool), num_nodes))
        if num_nodes <= 0:
            return []

        pending = list(floating_pool[-num_nodes:])
        NodeHelper._log.info(
            "Pre-provisioning certs for floating servers: {0}".format(
                [n.ip for n in pending]))

        if not CbServer.x509.node_ca_map:
            raise Exception(
                "CbServer.x509.node_ca_map is empty — no existing "
                "intermediate CA to sign floating-server certs with")
        existing = next(iter(CbServer.x509.node_ca_map.values()))
        int_ca_name = existing["signed_by"]
        root_ca_name = int_ca_name.split("_")[1]

        for node in pending:
            try:
                CbServer.x509.delete_inbox_folder_on_server(server=node)
            except Exception as e:
                NodeHelper._log.warning(
                    "delete_inbox_folder on {0} failed: {1}".format(node.ip, e))
            CbServer.x509.generate_node_certificate(
                root_ca_name, int_ca_name, node.ip)
            CbServer.x509.copy_trusted_CAs(
                root_ca_names=CbServer.x509.root_ca_names, server=node)
            try:
                CbServer.x509.load_trusted_CAs(server=node)
            except Exception as e:
                NodeHelper._log.warning(
                    "load_trusted_CAs on {0} failed (node may not be "
                    "initialised yet, continuing): {1}".format(node.ip, e))

        CbServer.x509.upload_node_certs(servers=pending)
        NodeHelper._log.info(
            "Pre-provisioned certs for {0} floating server(s): {1}".format(
                len(pending), [n.ip for n in pending]))
        return pending

    @staticmethod
    def log_fd_count(label, log=None):
        """Open-FD probe for EMFILE diagnosis. Linux /proc/self/fd; macOS /dev/fd."""
        import os as _os
        log = log or NodeHelper._log
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        except Exception:
            soft = hard = None
        count = None
        try:
            if _os.path.isdir("/proc/self/fd"):
                count = len(_os.listdir("/proc/self/fd"))
            elif _os.path.isdir("/dev/fd"):
                count = len(_os.listdir("/dev/fd"))
        except Exception as e:
            log.warning("[{0}] fd count probe failed: {1}".format(label, e))
        log.info("[{0}] open fds={1}, rlimit_nofile soft={2} hard={3}".format(
            label, count, soft, hard))

    @staticmethod
    def raise_fd_soft_limit(log=None):
        """Raise RLIMIT_NOFILE soft limit to hard. macOS default of 256 is
        too low for tests that open many paramiko transports."""
        log = log or NodeHelper._log
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft < hard:
                target = hard if hard != resource.RLIM_INFINITY else 65536
                resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
                log.info("RLIMIT_NOFILE raised: soft {0} -> {1} (hard={2})".format(
                    soft, target, hard))
        except Exception as e:
            log.warning("Could not raise RLIMIT_NOFILE: {0}".format(e))


class ValidateAuditEvent:

    @staticmethod
    def validate_audit_event(event_id, master_node, expected_results):
        if CHECK_AUDIT_EVENT.CHECK:
            audit_obj = audit(event_id, master_node)
            field_verified, value_verified = audit_obj.validateEvents(
                expected_results)
            raise_if(
                not field_verified,
                XDCRException("One of the fields is not matching"))
            raise_if(
                not value_verified,
                XDCRException("Values for one of the fields is not matching"))


class FloatingServers:

    """Keep Track of free servers, For Rebalance-in
    or swap-rebalance operations.
    """
    _serverlist = []


class XDCRRemoteClusterRef:

    """Class keep the information related to Remote Cluster References.
    """

    def __init__(self, src_cluster, dest_cluster, name, encryption=False, replicator_target_role=False,
                 multiple_ca=False, client_certificate=None, client_key=None, systemeventlog=None):
        """
        @param src_cluster: source couchbase cluster object.
        @param dest_cluster: destination couchbase cluster object:
        @param name: remote cluster reference name.
        @param encryption: True to enable SSL encryption for replication else
                        False
        """
        self.__src_cluster = src_cluster
        self.__dest_cluster = dest_cluster
        self.__name = name
        self.__encryption = encryption
        self.__multiple_ca = multiple_ca
        self.__client_certificate = client_certificate
        self.__client_key = client_key
        self.__systemeventlog = systemeventlog
        self.__rest_info = {}
        self.__replicator_target_role = replicator_target_role
        self.__use_scramsha = TestInputSingleton.input.param("use_scramsha", False)
        self.__pre_check = TestInputSingleton.input.param("pre_check", False)
        self.__taskId = ""

        # List of XDCReplication objects
        self.__replications = []

    def __str__(self):
        return "{0} -> {1}, Name: {2}".format(
            self.__src_cluster.get_name(), self.__dest_cluster.get_name(),
            self.__name)

    def get_cb_clusters(self):
        return self.__cb_clusters

    def get_src_cluster(self):
        return self.__src_cluster

    def get_dest_cluster(self):
        return self.__dest_cluster

    def get_name(self):
        return self.__name

    def get_replications(self):
        return self.__replications

    def get_rest_info(self):
        return self.__rest_info

    def get_systemeventlog(self):
        return self.__systemeventlog

    def get_replication_for_bucket(self, bucket):
        for replication in self.__replications:
            if replication.get_src_bucket().name == bucket.name:
                return replication
        return None

    def __get_event_expected_results(self):
        expected_results = {
                "real_userid:source": "ns_server",
                "real_userid:user": self.__src_cluster.get_master_node().rest_username,
                "cluster_name": self.__name,
                "cluster_hostname": "%s:%s" % (self.__dest_cluster.get_master_node().ip, self.__dest_cluster.get_master_node().port),
                "is_encrypted": self.__encryption,
                "encryption_type": ""}

        return expected_results

    def __validate_create_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.CREATE_CLUSTER,
            self.__src_cluster.get_master_node(),
            self.__get_event_expected_results())
        self.__systemeventlog.add_event(
            XDCRServiceEvents.create_remote_cluster_ref(self.__src_cluster.get_master_node().ip,
                                                        self.__dest_cluster.get_master_node().ip
                                                        + ':' + self.__dest_cluster.get_master_node().port,
                                                        self.__rest_info['uuid'],
                                                        "plain",
                                                        self.__name
                                                        ))
    def _extract_certs(self, raw_content):
        certs = ""
        for ca_dict in raw_content:
            certs += ca_dict["pem"]
        return certs

    def connection_pre_check(self, rest_conn_src, dest_master, certificate):
        log = logger.Logger.get_logger()
        log.info("Pre check flow started")
        res = rest_conn_src.start_connection_pre_check(
            dest_master.cluster_ip, dest_master.port,
            self.dest_user,
            self.dest_pass, self.__name,
            demandEncryption=self.__encryption,
            certificate=certificate,
            clientCertificate=self.__client_certificate,
            clientKey=self.__client_key)
        log.info(str(res))
        self.__taskId = res["taskId"]
        log.info("connection pre check started successfully with taskId {0}".format(self.__taskId))
        res = {}
        for i in range(10):
            log.info("iteration {0} of polling for connection pre check status".format(i + 1))
            res = rest_conn_src.connection_pre_check_status(
            self.dest_user, self.dest_pass, self.__taskId)
            if res["done"] == True:
                log.info("connection pre check completed")
                status = str(res["result"])
                log.info(status)
                break
            time.sleep(30)
        if res["done"] == False:
            log.error("connection pre check status failed after 10 attempts")

    def add(self):
        """create cluster reference- add remote cluster
        """
        rest_conn_src = RestConnection(self.__src_cluster.get_master_node())
        certificate = None
        dest_master = self.__dest_cluster.get_master_node()
        if self.__encryption:
            rest_conn_dest = RestConnection(dest_master)
            if self.__multiple_ca:
                raw_content = rest_conn_dest.get_trusted_CAs()
                certificate = self._extract_certs(raw_content)
            else:
                certificate = rest_conn_dest.get_cluster_ceritificate()
        if self.__replicator_target_role:
            self.dest_user = "replicator_user"
            self.dest_pass = "password"
        else:
            self.dest_user = dest_master.rest_username
            self.dest_pass = dest_master.rest_password

        logger.Logger.get_logger().info(self.__pre_check)
        if self.__pre_check:
            self.connection_pre_check(rest_conn_src, dest_master, certificate)

        if not self.__use_scramsha:
            self.__rest_info = rest_conn_src.add_remote_cluster(
                dest_master.cluster_ip, dest_master.port,
                self.dest_user,
                self.dest_pass, self.__name,
                demandEncryption=self.__encryption,
                certificate=certificate,
                clientCertificate=self.__client_certificate,
                clientKey=self.__client_key
            )
        else:
            print("Using scram-sha authentication")
            self.__rest_info = rest_conn_src.add_remote_cluster(
                dest_master.cluster_ip, dest_master.port,
                self.dest_user,
                self.dest_pass, self.__name,
                demandEncryption=self.__encryption,
                encryptionType="half"
            )

        self.__validate_create_event()

    def __validate_modify_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.MOD_CLUSTER,
            self.__src_cluster.get_master_node(),
            self.__get_event_expected_results())
        self.__systemeventlog.add_event(
            XDCRServiceEvents.update_remote_cluster_ref(self.__src_cluster.get_master_node(),
                                                        self.__dest_cluster.get_master_node().ip,
                                                        self.__rest_info['uuid'],
                                                        self.__encryption,
                                                        self.__name
                                                        ))

    def use_scram_sha_auth(self):
        self.__use_scramsha = True
        self.__encryption = True
        self.modify()

    def modify(self, encryption=True):
        dest_master = self.__dest_cluster.get_master_node()
        rest_conn_src = RestConnection(self.__src_cluster.get_master_node())
        if encryption:
            rest_conn_dest = RestConnection(dest_master)
            if not self.__use_scramsha:
                if self.__multiple_ca:
                    raw_content = rest_conn_dest.get_trusted_CAs()
                    certificate = self._extract_certs(raw_content)
                else:
                    certificate = rest_conn_dest.get_cluster_ceritificate()

                self.__rest_info = rest_conn_src.modify_remote_cluster(
                    dest_master.ip, dest_master.port,
                    self.dest_user,
                    self.dest_pass, self.__name,
                    demandEncryption=encryption,
                    certificate=certificate,
                    clientCertificate=self.__client_certificate,
                    clientKey=self.__client_key
                )
            else:
                print("Using scram-sha authentication")
                self.__rest_info = rest_conn_src.modify_remote_cluster(
                    dest_master.ip, dest_master.port,
                    self.dest_user,
                    self.dest_pass, self.__name,
                    demandEncryption=encryption,
                    encryptionType="half")
        self.__encryption = encryption
        self.__validate_modify_event()

    def __validate_remove_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.RM_CLUSTER,
            self.__src_cluster.get_master_node(),
            self.__get_event_expected_results())
        self.__systemeventlog.add_event(
        XDCRServiceEvents.delete_remote_cluster_ref(self.__src_cluster.get_master_node(),
                                                    self.__dest_cluster.get_master_node().ip,
                                                    self.__rest_info['uuid'],
                                                    self.__encryption,
                                                    self.__name
                                                    ))

    def remove(self):
        RestConnection(
            self.__src_cluster.get_master_node()).remove_remote_cluster(
            self.__name)
        self.__validate_remove_event()

    def create_replication(
            self, fromBucket,
            rep_type=REPLICATION_PROTOCOL.XMEM,
            toBucket=None):
        """Create replication objects, but replication will not get
        started here.
        """
        self.__replications.append(
            XDCReplication(
                self,
                fromBucket,
                rep_type,
                toBucket))


    def clear_all_replications(self):
        self.__replications = []

    def start_all_replications(self):
        """Start all created replication
        """
        [repl.start() for repl in self.__replications]

    def pause_all_replications(self, verify=False):
        """Pause all created replication
        """
        [repl.pause(verify=verify) for repl in self.__replications]

    def pause_all_replications_by_id(self, verify=False):
        [repl.pause(repl_id=repl.get_repl_id(), verify=verify) for repl in self.__replications]

    def resume_all_replications(self, verify=False):
        """Resume all created replication
        """
        [repl.resume(verify=verify) for repl in self.__replications]

    def resume_all_replications_by_id(self, verify=False):
        [repl.resume(repl_id=repl.get_repl_id(), verify=verify) for repl in self.__replications]

    def stop_all_replications(self):
        rest = RestConnection(self.__src_cluster.get_master_node())
        rest_all_repls = rest.get_replications()
        for repl in self.__replications:
            for rest_all_repl in rest_all_repls:
                if repl.get_repl_id() == rest_all_repl['id']:
                    repl.cancel(rest, rest_all_repl)
        self.clear_all_replications()


class XDCReplication:
    def __init__(self, remote_cluster_ref, from_bucket, rep_type, to_bucket):
        """
        @param remote_cluster_ref: XDCRRemoteClusterRef object
        @param from_bucket: Source bucket (Bucket object)
        @param rep_type: replication protocol REPLICATION_PROTOCOL.CAPI/XMEM
        @param to_bucket: Destination bucket (Bucket object)
        """
        self.__input = TestInputSingleton.input
        self.__remote_cluster_ref = remote_cluster_ref
        self.__from_bucket = from_bucket
        self.__to_bucket = to_bucket or from_bucket
        self.__src_cluster = self.__remote_cluster_ref.get_src_cluster()
        self.__dest_cluster = self.__remote_cluster_ref.get_dest_cluster()
        self.__src_cluster_name = self.__src_cluster.get_name()
        self.__rep_type = rep_type
        self.__test_xdcr_params = {}
        self.__updated_params = {}

        self.__parse_test_xdcr_params()
        self.log = logger.Logger.get_logger()

        # Response from REST API
        self.__rep_id = None

    def __str__(self):
        return "Replication {0}:{1} -> {2}:{3}".format(
            self.__src_cluster.get_name(),
            self.__from_bucket.name, self.__dest_cluster.get_name(),
            self.__to_bucket.name)

    def __get_random_filter(self, filter_type):
        from scripts.edgyjson.main import JSONDoc
        obj = JSONDoc(template="query.json", filter=True, load=False)
        filter_type = filter_type.split("-")[1]
        if filter_type == "random":
            filter_type = random.choice(obj.filters_json_objs_dict.keys())
        num_exps = random.randint(0, 5)
        ex = ""
        while num_exps:
            nesting_level = random.randint(0, 5)
            ex_no_brackets = random.choice(obj.filters_json_objs_dict[filter_type])
            ex_with_brackets = '( ' + ex_no_brackets + ' )'
            # Generate nested exps: for eg ((exp1 AND exp2) OR exp3)
            for _ in range(nesting_level):
                ex += '( ' + random.choice([ex_no_brackets, ex_with_brackets]) \
                        + ' )' + random.choice([" AND ", " OR "])
            num_exps -= 1
        # Enclose keys having '_' in quotes (eg:'string_long')
        if '_' in ex:
            ex = ("'%s_%s'") % ex
        ex += random.choice(obj.filters_json_objs_dict[filter_type])
        return ex

    # get per replication params specified as from_bucket@cluster_name=<setting>:<value>
    # eg. default@C1=filter_expression:loadOne,failure_restart_interval:20
    def __parse_test_xdcr_params(self):
        param_str = self.__input.param(
            "%s@%s" %
            (self.__from_bucket, self.__src_cluster_name), None)
        if param_str:
            argument_split = re.split('[:,]', param_str)
            self.__test_xdcr_params.update(
                dict(list(zip(argument_split[::2], argument_split[1::2])))
            )

            # Skip conflict_logging parameter to avoid issues with replication creation
            if TEST_XDCR_PARAM.CONFLICT_LOGGING in self.__test_xdcr_params:
                print(f"Skipping conflict_logging parameter in XDCReplication.__parse_test_xdcr_params")
                del self.__test_xdcr_params[TEST_XDCR_PARAM.CONFLICT_LOGGING]

        if 'filter_expression' in self.__test_xdcr_params:
            ex = self.__test_xdcr_params['filter_expression']
            if len(ex) > 0:
                if ex.startswith("random"):
                    self.__test_xdcr_params['filter_expression'] = self.__get_random_filter(ex)
                masked_input = {"comma": ',', "star": '*', "dot": '.', "equals": '=', "{": '', "}": '', "colon": ':'}
                for _ in masked_input:
                    self.__test_xdcr_params['filter_expression'] = self.__test_xdcr_params['filter_expression'].replace(
                        _, masked_input[_])
        if "colmapping_rules" in self.__test_xdcr_params:
            masked_input = {"comma": ',', "colon": ':', "dot": '.'}
            for _ in masked_input:
                self.__test_xdcr_params['colmapping_rules'] = self.__test_xdcr_params[
                    'colmapping_rules'].replace(
                    _, masked_input[_])

    def __convert_test_to_xdcr_params(self):
        xdcr_params = {}
        xdcr_param_map = TEST_XDCR_PARAM.get_test_to_create_repl_param_map()
        for test_param, value in self.__test_xdcr_params.items():
            xdcr_params[xdcr_param_map[test_param]] = value
        return xdcr_params

    def get_filter_exp(self):
        if TEST_XDCR_PARAM.FILTER_EXP in self.__test_xdcr_params:
            return self.__test_xdcr_params[TEST_XDCR_PARAM.FILTER_EXP]
        return None

    def get_src_bucket(self):
        return self.__from_bucket

    def get_dest_bucket(self):
        return self.__to_bucket

    def get_src_cluster(self):
        return self.__src_cluster

    def get_dest_cluster(self):
        return self.__dest_cluster

    def get_repl_id(self):
        return self.__rep_id

    def get_remote_cluster_ref(self):
        return self.__remote_cluster_ref

    def __get_event_expected_results(self):
        expected_results = {
                "real_userid:source": "ns_server",
                "real_userid:user": self.__src_cluster.get_master_node().rest_username,
                "local_cluster_name": "%s:%s" % (self.__src_cluster.get_master_node().ip, self.__src_cluster.get_master_node().port),
                "source_bucket_name": self.__from_bucket.name,
                "remote_cluster_name": self.__remote_cluster_ref.get_name(),
                "target_bucket_name": self.__to_bucket.name
            }
        # optional audit param
        if self.get_filter_exp():
            expected_results["filter_expression"] = self.get_filter_exp()
        return expected_results

    def __validate_update_repl_event(self):
        expected_results = {
            "settings": {
                "continuous": 'true',
                "target": self.__to_bucket.name,
                "source": self.__from_bucket.name,
                "type": "xdc-%s" % self.__rep_type
            },
            "id": self.__rep_id,
            "real_userid:source": "ns_server",
            "real_userid:user": self.__src_cluster.get_master_node().rest_username,
        }
        expected_results["settings"].update(self.__updated_params)

    def __validate_set_param_event(self):
        expected_results = self.__get_event_expected_results()
        expected_results["updated_settings"] = self.__updated_params
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.IND_SETT,
            self.get_src_cluster().get_master_node(), expected_results)
        self.get_remote_cluster_ref().get_systemeventlog().add_event(
            XDCRServiceEvents.update_replication(self.get_src_cluster().get_master_node(),
                                                 self.get_src_bucket().name,
                                                 self.get_dest_bucket().name,
                                                 self.get_repl_id(),
                                                 self.get_remote_cluster_ref().get_name(),
                                                 self.get_filter_exp()
                                                 ))

    def get_xdcr_setting(self, param):
        """Get a replication setting value
        """
        src_master = self.__src_cluster.get_master_node()
        return RestConnection(src_master).get_xdcr_param(
                    self.__from_bucket.name,
                    self.__to_bucket.name,
                    param)

    def set_xdcr_param(self, param, value, verify_event=True):
        """Set a replication setting to a value
        """
        src_master = self.__src_cluster.get_master_node()
        RestConnection(src_master).set_xdcr_param(
            self.__from_bucket.name,
            self.__to_bucket.name,
            param,
            value)
        self.log.info("Updated {0}={1} on bucket'{2}' on {3}".format(param, value, self.__from_bucket.name,
                                                                     self.__src_cluster.get_master_node().ip))
        self.__updated_params[param] = value
        if verify_event:
            self.__validate_set_param_event()

    def __validate_start_audit_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.CREATE_REPL,
                self.get_src_cluster().get_master_node(),
                self.__get_event_expected_results())
        self.get_remote_cluster_ref().get_systemeventlog().add_event(
            XDCRServiceEvents.create_replication(self.get_src_cluster().get_master_node().ip,
                                                 self.get_src_bucket().name,
                                                 self.get_dest_bucket().name,
                                                 self.get_repl_id(),
                                                 self.get_remote_cluster_ref().get_name(),
                                                 self.get_filter_exp()
                                                 ))

    def start(self):
        """Start replication"""
        src_master = self.__src_cluster.get_master_node()
        rest_conn_src = RestConnection(src_master)
        self.__rep_id = rest_conn_src.start_replication(
            REPLICATION_TYPE.CONTINUOUS,
            self.__from_bucket,
            self.__remote_cluster_ref.get_name(),
            rep_type=self.__rep_type,
            toBucket=self.__to_bucket,
            xdcr_params=self.__convert_test_to_xdcr_params())
        self.__validate_start_audit_event()
        #if within this 10s for pipeline updater if we try to create another replication, it doesn't work until the previous pipeline is updated.
        # but better to have this 10s sleep between replications.
        time.sleep(10)

    def __verify_pause(self):
        """Verify if replication is paused"""
        src_master = self.__src_cluster.get_master_node()
        # Is bucket replication paused?
        if not RestConnection(src_master).is_replication_paused(
                self.__from_bucket.name,
                self.__to_bucket.name):
            raise XDCRException(
                "XDCR is not paused for SrcBucket: {0}, Target Bucket: {1}".
                format(self.__from_bucket.name,
                       self.__to_bucket.name))

    def __validate_pause_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.PAUSE_REPL,
            self.get_src_cluster().get_master_node(),
            self.__get_event_expected_results())
        self.get_remote_cluster_ref().get_systemeventlog().add_event(
            XDCRServiceEvents.pause_replication(self.get_src_cluster().get_master_node().ip))

    def pause(self, repl_id=None, verify=False):
        """Pause replication"""
        src_master = self.__src_cluster.get_master_node()
        if repl_id:
            if not RestConnection(src_master).is_replication_paused_by_id(repl_id):
                RestConnection(src_master).pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')
        else:
            if not RestConnection(src_master).is_replication_paused(
                    self.__from_bucket.name, self.__to_bucket.name):
                self.set_xdcr_param(
                    REPL_PARAM.PAUSE_REQUESTED,
                    'true',
                    verify_event=False)

        self.__validate_pause_event()

        if verify:
            self.__verify_pause()

    def _is_cluster_replicating(self):
        count = 0
        src_master = self.__src_cluster.get_master_node()
        while count < 3:
            outbound_mutations = self.__src_cluster.get_xdcr_stat(
                self.__from_bucket.name,
                'replication_changes_left')
            if outbound_mutations == 0:
                self.log.info(
                    "Outbound mutations on {0} is {1}".format(
                        src_master.ip,
                        outbound_mutations))
                count += 1
                continue
            else:
                self.log.info(
                    "Outbound mutations on {0} is {1}".format(
                        src_master.ip,
                        outbound_mutations))
                self.log.info("Node {0} is replicating".format(src_master.ip))
                break
        else:
            self.log.info(
                "Outbound mutations on {0} is {1}".format(
                    src_master.ip,
                    outbound_mutations))
            self.log.info(
                "Cluster with node {0} is not replicating".format(
                    src_master.ip))
            return False
        return True

    def __verify_resume(self):
        """Verify if replication is resumed"""
        src_master = self.__src_cluster.get_master_node()
        # Is bucket replication paused?
        if RestConnection(src_master).is_replication_paused(self.__from_bucket.name,
                                                            self.__to_bucket.name):
            raise XDCRException(
                "Replication is not resumed for SrcBucket: {0}, \
                Target Bucket: {1}".format(self.__from_bucket, self.__to_bucket))

        if not self._is_cluster_replicating():
            self.log.info("XDCR completed on {0}".format(src_master.ip))

    def __validate_resume_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.RESUME_REPL,
            self.get_src_cluster().get_master_node(),
            self.__get_event_expected_results())
        self.get_remote_cluster_ref().get_systemeventlog().add_event(
            XDCRServiceEvents.resume_replication(self.get_src_cluster().get_master_node().ip))

    def resume(self, repl_id=None, verify=False):
        """Resume replication if paused"""
        src_master = self.__src_cluster.get_master_node()
        if repl_id:
            if RestConnection(src_master).is_replication_paused_by_id(repl_id):
                RestConnection(src_master).pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')
        else:
            if RestConnection(src_master).is_replication_paused(
                    self.__from_bucket.name, self.__to_bucket.name):
                self.set_xdcr_param(
                REPL_PARAM.PAUSE_REQUESTED,
                'false',
                verify_event=False)

        self.__validate_resume_event()

        if verify:
            self.__verify_resume()

    def __validate_cancel_event(self):
        ValidateAuditEvent.validate_audit_event(
            GO_XDCR_AUDIT_EVENT_ID.CAN_REPL,
            self.get_src_cluster().get_master_node(),
            self.__get_event_expected_results())
        self.get_remote_cluster_ref().get_systemeventlog().add_event(
            XDCRServiceEvents.delete_replication(self.get_src_cluster().get_master_node().ip))


    def cancel(self, rest, rest_all_repl):
        rest.stop_replication(rest_all_repl["cancelURI"])
        self.__validate_cancel_event()


class CouchbaseCluster:

    def __init__(self, name, nodes, log, use_hostname=False, sdk_compression=True,
                 use_java_sdk=False, scope_num=0, collection_num=0, use_https=False):
        """
        @param name: Couchbase cluster name. e.g C1, C2 to distinguish in logs.
        @param nodes: list of server objects (read from ini file).
        @param log: logger object to print logs.
        @param use_hostname: True if use node's hostname rather ip to access
                        node else False.
        """
        self.__name = name
        self.__nodes = nodes
        self.__log = log
        self.__mem_quota = 0
        self.__use_hostname = use_hostname
        self.__master_node = nodes[0]
        self.__design_docs = []
        self.__buckets = []
        self.__hostnames = {}
        self.__fail_over_nodes = []
        self.__data_verified = True
        self.__meta_data_verified = True
        self.__remote_clusters = []
        self.__clusterop = Cluster()
        self.__kv_gen = {}
        self.sdk_compression = sdk_compression
        self.use_java_sdk = use_java_sdk
        self.gen = None
        self.scope_num = scope_num
        self.collection_num = collection_num
        self.use_https = use_https

    def __str__(self):
        return "Couchbase Cluster: %s, Master Ip: %s" % (
            self.__name, self.__master_node.ip)

    def __stop_rebalance(self):
        rest = RestConnection(self.__master_node)
        if rest._rebalance_progress_status() == 'running':
            self.__log.warning(
                "rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            raise_if(
                not stopped,
                RebalanceNotStopException("unable to stop rebalance"))

    def __init_nodes(self, disabled_consistent_view=None):
        """Initialize all nodes. Rename node to hostname
        if needed by test.
        """
        tasks = []
        for node in self.__nodes:
            tasks.append(
                self.__clusterop.async_init_node(
                    node,
                    disabled_consistent_view=disabled_consistent_view))
        for task in tasks:
            mem_quota = task.result()
            if mem_quota < self.__mem_quota or self.__mem_quota == 0:
                self.__mem_quota = mem_quota
        if self.__use_hostname:
            self.__hostnames.update(NodeHelper.rename_nodes(self.__nodes))

    def get_host_names(self):
        return self.__hostnames

    def get_master_node(self):
        return self.__master_node

    def get_mem_quota(self):
        return self.__mem_quota

    def get_remote_clusters(self):
        return self.__remote_clusters

    def clear_all_remote_clusters(self):
        self.__remote_clusters = []

    def get_nodes(self):
        return self.__nodes

    def get_name(self):
        return self.__name

    def get_cluster(self):
        return self.__clusterop

    def get_kv_gen(self):
        raise_if(
            self.__kv_gen is None,
            XDCRException(
                "KV store is empty on couchbase cluster: %s" %
                self))
        return self.__kv_gen

    def get_remote_cluster_ref_by_name(self, cluster_name):
        for remote_cluster_ref in self.__remote_clusters:
            if remote_cluster_ref.get_name() == cluster_name:
                return remote_cluster_ref
        self.__log.info("Remote cluster reference by name %s does not exist on %s"
                        % (cluster_name, self.__name))
        return None

    def init_cluster(self, disabled_consistent_view=None):
        """Initialize cluster.
        1. Initialize all nodes.
        2. Add all nodes to the cluster.
        3. Enable xdcr trace logs to easy debug for xdcr items mismatch issues.
        """
        master = RestConnection(self.__master_node)
        self.enable_diag_eval_on_non_local_hosts(self.__master_node)
        self.__init_nodes(disabled_consistent_view=disabled_consistent_view)
        self.__clusterop.async_rebalance(
            self.__nodes,
            self.__nodes[1:],
            [],
            use_hostnames=self.__use_hostname).result()

        if CHECK_AUDIT_EVENT.CHECK:
            if master.is_enterprise_edition():
                # enable audit by default in all goxdcr tests
                audit_obj = audit(host=self.__master_node)
                status = audit_obj.getAuditStatus()
                self.__log.info("Audit status on {0} is {1}".
                            format(self.__name, status))
                if not status:
                    self.__log.info("Enabling audit ...")
                    audit_obj.setAuditEnable('true')

    def enable_diag_eval_on_non_local_hosts(self, master):
        """
        Enable diag/eval to be run on non-local hosts.
        :param master: Node information of the master node of the cluster
        :return: Nothing
        """
        remote = RemoteMachineShellConnection(master)
        output, error = remote.enable_diag_eval_on_non_local_hosts()
        if output is not None:
            if "ok" not in output:
                self.__log.error("Error in enabling diag/eval on non-local hosts on {}".format(master.ip))
                raise Exception("Error in enabling diag/eval on non-local hosts on {}".format(master.ip))
            else:
                self.__log.info(
                    "Enabled diag/eval for non-local hosts from {}".format(
                        master.ip))
        else:
            self.__log.info("Running in compatibility mode, not enabled diag/eval for non-local hosts")

    def _create_bucket_params(self, server, replicas=1, size=256, port=11211,
                              password=None,
                              bucket_type=None, enable_replica_index=1, eviction_policy='fullEviction',
                              bucket_priority=None, flush_enabled=1, lww=False, maxttl=None,
                              bucket_storage='magma', num_vbuckets=None):
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
        bucket_type = TestInputSingleton.input.param("bucket_type", "membase")
        bucket_params['bucket_type'] = bucket_type
        bucket_params['enable_replica_index'] = enable_replica_index
        if bucket_type == "ephemeral":
            if eviction_policy in EVICTION_POLICY.EPH:
                bucket_params['eviction_policy'] = eviction_policy
            else:
                bucket_params['eviction_policy'] = EVICTION_POLICY.NO_EVICTION
            # NRU eviction for src bkt implemented in 6.0.2
            # AllowSourceNRUCreation internal setting needs to be enabled for 6.0.2 to 6.5.0
            # It is enabled by default for 6.5.0 and up
            rest = RestConnection(server)
            if rest.check_node_versions("6.0"):
                self.set_internal_setting("AllowSourceNRUCreation", "true")
                bucket_params['eviction_policy'] = EVICTION_POLICY.NRU_EVICTION
            elif rest.check_node_versions("6.5"):
                bucket_params['eviction_policy'] = EVICTION_POLICY.NRU_EVICTION
            if eviction_policy == EVICTION_POLICY.NRU_EVICTION :
                if "6.0.2-" in NodeHelper.get_cb_version(server):
                    self.set_internal_setting("AllowSourceNRUCreation", "true")
        else:
            if eviction_policy in EVICTION_POLICY.CB:
                bucket_params['eviction_policy'] = eviction_policy
            else:
                bucket_params['eviction_policy'] = EVICTION_POLICY.VALUE_ONLY

            if num_vbuckets is not None and bucket_storage == "magma":
                bucket_params["numVBuckets"] = num_vbuckets

        bucket_params['bucket_storage'] = bucket_storage
        bucket_params['bucket_priority'] = bucket_priority
        bucket_params['flush_enabled'] = flush_enabled
        bucket_params['lww'] = lww
        bucket_params['maxTTL'] = maxttl
        return bucket_params

    def set_global_checkpt_interval(self, value):
        self.set_xdcr_param("checkpointInterval", value)

    def set_conflict_logging_settings(self, value):
        self.set_xdcr_param("conflictLogging", value)

    def get_conflict_logging_settings(self):
        return self.get_xdcr_param("conflictLogging")

    def get_internal_setting(self, param):
        import json
        output, _ = RemoteMachineShellConnection(self.__master_node).execute_command_raw(
            "curl -X GET http://Administrator:password@localhost:9998/xdcr/internalSettings", debug=True)
        json_parsed = json.loads(output[0])
        return json_parsed[param]

    def set_internal_setting(self, param, value):
        RemoteMachineShellConnection(self.__master_node).execute_command_raw(
            "curl http://Administrator:password@localhost:9998/xdcr/internalSettings -X POST -d " +
            param + '=' + str(value))

    def toggle_security_setting(self, servers, setting, value):
        n2nhelper = ntonencryptionBase()
        if setting == "tls":
            if value:
                n2nhelper.setup_nton_cluster(servers, clusterEncryptionLevel=value)
            else:
                n2nhelper.disable_nton_cluster(servers)
        if setting == "n2n":
            if value:
                n2nhelper.ntonencryption_cli(servers, value)
            else:
                is_enabled = n2nhelper.ntonencryption_cli(servers, "get")
                if is_enabled:
                    n2nhelper.ntonencryption_cli(servers, "disable")
        if setting == "autofailover":
            if value:
                n2nhelper.enable_autofailover(servers)
            else:
                is_enabled = n2nhelper.check_autofailover_enabled(servers)
                if is_enabled:
                    n2nhelper.disable_autofailover(servers)

    def __remove_all_remote_clusters(self):
        rest_remote_clusters = RestConnection(
            self.__master_node).get_remote_clusters()
        for remote_cluster_ref in self.__remote_clusters:
            for rest_remote_cluster in rest_remote_clusters:
                if remote_cluster_ref.get_name() == rest_remote_cluster[
                        'name']:
                    if not rest_remote_cluster.get('deleted', False):
                        remote_cluster_ref.remove()
        self.__remote_clusters = []

    def __remove_all_replications(self):
        for remote_cluster_ref in self.__remote_clusters:
            remote_cluster_ref.stop_all_replications()

    def cleanup_cluster(
            self,
            test_case,
            from_rest=False,
            cluster_shutdown=True):
        """Cleanup cluster.
        1. Remove all remote cluster references.
        2. Remove all replications.
        3. Remove all buckets.
        @param test_case: Test case object.
        @param test_failed: True if test failed else False.
        @param cluster_run: True if test execution is single node cluster run else False.
        @param cluster_shutdown: True if Task (task.py) Scheduler needs to shutdown else False
        """
        try:
            self.__log.info("removing xdcr/nodes settings")
            rest = RestConnection(self.__master_node)
            if from_rest:
                rest.remove_all_replications()
                rest.remove_all_remote_clusters()
            else:
                self.__remove_all_replications()
                self.__remove_all_remote_clusters()
            rest.remove_all_recoveries()
            self.__stop_rebalance()
            self.__log.info("cleanup {0}".format(self.__nodes))
            for node in self.__nodes:
                BucketOperationHelper.delete_all_buckets_or_assert(
                    [node],
                    test_case)
                force_eject = TestInputSingleton.input.param(
                    "forceEject",
                    False)
                if force_eject and node != self.__master_node:
                    try:
                        rest = RestConnection(node)
                        rest.force_eject_node()
                    except BaseException as e:
                        self.__log.error(e)
                else:
                    ClusterOperationHelper.cleanup_cluster([node])
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [node],
                    test_case)
        except Exception as e:
            self.__log.warning(e)
        finally:
            if cluster_shutdown:
                self.__clusterop.shutdown(force=True)
            try:
                if CbServer.multiple_ca:
                    CbServer.use_client_certs = False
                    CbServer.cacert_verify = False
                    x509main(host=self.__master_node).teardown_certs(servers=TestInputSingleton.input.servers)
            except Exception as e:
                self.__log.info(e)

    def create_sasl_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH, lww=False,
            maxttl=None, bucket_storage='couchstore'):
        """Create sasl buckets.
        @param bucket_size: size of the bucket.
        @param num_buckets: number of buckets to create.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        @param lww: conflict_resolution_type.
        """
        tasks = []
        for i in range(num_buckets):
            name = "sasl_bucket_" + str(i + 1)
            sasl_params = self._create_bucket_params(server=self.__master_node, password='password',
                                                     size=bucket_size, replicas=num_replicas,
                                                     eviction_policy=eviction_policy,
                                                     bucket_priority=bucket_priority,
                                                     lww=lww, maxttl=maxttl,
                                                     bucket_storage=bucket_storage)
            tasks.append(self.__clusterop.async_create_sasl_bucket(name=name, password='password',
                                                      bucket_params=sasl_params))
            self.__buckets.append(
                Bucket(
                    name=name,
                    num_replicas=num_replicas, bucket_size=bucket_size,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority,
                    lww=lww,
                    maxttl=maxttl,
                    bucket_storage=bucket_storage
                ))
            if self.scope_num or self.collection_num:
                tasks.append(CollectionsRest(self.__master_node).async_create_scope_collection(
                    self.scope_num, self.collection_num, name))

    def create_standard_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH, lww=False, maxttl=None,
            bucket_storage='couchstore', num_vbuckets=None):
        """Create standard buckets.
        @param bucket_size: size of the bucket.
        @param num_buckets: number of buckets to create.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        @param lww: conflict_resolution_type.
        """
        tasks = []
        for i in range(num_buckets):
            name = "standard_bucket_" + str(i + 1)
            standard_params = self._create_bucket_params(
                server=self.__master_node,
                size=bucket_size,
                replicas=num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority,
                lww=lww,
                maxttl=maxttl,
                bucket_storage=bucket_storage,
                num_vbuckets=num_vbuckets)
            tasks.append(self.__clusterop.async_create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT + i,
                                                          bucket_params=standard_params))
            self.__buckets.append(
                Bucket(
                    name=name,
                    num_replicas=num_replicas,
                    bucket_size=bucket_size,
                    port=STANDARD_BUCKET_PORT + i,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority,
                    lww=lww,
                    maxttl=maxttl,
                    bucket_storage=bucket_storage,
                    numVBuckets=num_vbuckets,
                ))
            if self.scope_num or self.collection_num:
                tasks.append(CollectionsRest(self.__master_node).async_create_scope_collection(
                    self.scope_num, self.collection_num, name))
        [task for task in tasks]

    def create_default_bucket(
            self, bucket_size, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH, lww=False,
            maxttl=None, bucket_storage='couchstore',
            num_vbuckets=None):
        """Create default bucket.
        @param bucket_size: size of the bucket.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        @param lww: conflict_resolution_type.
        """
        bucket_params = self._create_bucket_params(
            server=self.__master_node,
            size=bucket_size,
            replicas=num_replicas,
            eviction_policy=eviction_policy,
            bucket_priority=bucket_priority,
            lww=lww,
            maxttl=maxttl,
            bucket_storage=bucket_storage,
            num_vbuckets=num_vbuckets)

        tasks = [self.__clusterop.create_default_bucket(bucket_params)]
        self.__buckets.append(
            Bucket(
                name=BUCKET_NAME.DEFAULT,
                num_replicas=num_replicas,
                bucket_size=bucket_size,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority,
                lww=lww,
                maxttl=maxttl,
                bucket_storage=bucket_storage,
                numVBuckets=num_vbuckets
            ))
        if self.scope_num or self.collection_num:
            tasks.append(CollectionsRest(self.__master_node).async_create_scope_collection(
                self.scope_num, self.collection_num, BUCKET_NAME.DEFAULT))

    def get_buckets(self):
        return self.__buckets

    def add_bucket(self, bucket='',
                   ramQuotaMB=256,
                   replicaNumber=1,
                   proxyPort=11211,
                   bucketType='membase',
                   evictionPolicy='valueOnly'):
        self.__buckets.append(Bucket(bucket_size=ramQuotaMB, name=bucket,
                                     num_replicas=replicaNumber,
                                     port=proxyPort, type=bucketType, eviction_policy=evictionPolicy))

    def get_bucket_by_name(self, bucket_name):
        """Return the bucket with given name
        @param bucket_name: bucket name.
        @return: bucket object
        """
        for bucket in self.__buckets:
            if bucket.name == bucket_name:
                return bucket

        raise Exception(
            "Bucket with name: %s not found on the cluster" %
            bucket_name)

    def delete_bucket(self, bucket_name):
        """Delete bucket with given name
        @param bucket_name: bucket name (string) to delete
        """
        bucket_to_remove = self.get_bucket_by_name(bucket_name)
        self.__clusterop.bucket_delete(
            self.__master_node,
            bucket_to_remove.name)
        self.__buckets.remove(bucket_to_remove)

    def delete_all_buckets(self):
        for bucket_to_remove in self.__buckets:
            self.__clusterop.bucket_delete(
                self.__master_node,
                bucket_to_remove.name)
            self.__buckets.remove(bucket_to_remove)

    def flush_buckets(self, buckets=[]):
        buckets = buckets or self.__buckets
        tasks = []
        for bucket in buckets:
            tasks.append(self.__clusterop.async_bucket_flush(
                self.__master_node,
                bucket))
        [task.result() for task in tasks]

    def async_load_bucket(self, bucket, num_items, value_size=512, exp=0,
                          kv_store=1, flag=0, only_store_hash=True,
                          batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on given bucket. Function don't wait for
        load data to finish, return immediately.
        @param bucket: bucket where to load data.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @return: task object
        """
        if not self.gen:
            seed = "%s-key-" % self.__name
            self.__kv_gen[
                OPS.CREATE] = BlobGenerator(
                seed,
                seed,
                value_size,
                end=num_items)
            self.gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
        task = self.__clusterop.async_load_gen_docs(self.__master_node, bucket.name, self.gen,
                                                    bucket.kvs[kv_store],OPS.CREATE, exp,
                                                    flag, only_store_hash, batch_size, pause_secs,
                                                    timeout_secs, compression=self.sdk_compression)
        return task

    def load_bucket(self, bucket, num_items, value_size=512, exp=0,
                    kv_store=1, flag=0, only_store_hash=True,
                    batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data synchronously on given bucket. Function wait for
        load data to finish.
        @param bucket: bucket where to load data.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        task = self.async_load_bucket(bucket, num_items, value_size, exp,
                                      kv_store, flag, only_store_hash,
                                      batch_size, pause_secs, timeout_secs)
        task.result()

    def async_load_all_buckets(self, num_items, value_size=512, exp=0,
                               kv_store=1, flag=0, only_store_hash=True,
                               batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on all buckets of the cluster.
        Function don't wait for load data to finish, return immidiately.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @return: task objects list
        """
        seed = "%s-key-" % self.__name
        if self.use_java_sdk:
            self.gen = SDKDataLoader(num_ops=num_items, percent_create=100, percent_update=0,
                                    percent_delete=0, doc_expiry=exp, all_collections=True,
                                    key_prefix=seed)
        else:
            self.gen = self.__kv_gen[
                OPS.CREATE] = BlobGenerator(
                seed,
                seed,
                value_size,
                start=0,
                end=num_items)
        tasks = []
        for bucket in self.__buckets:
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, self.gen, bucket.kvs[kv_store],
                    OPS.CREATE, exp, flag, only_store_hash, batch_size,
                    pause_secs, timeout_secs, compression=self.sdk_compression)
            )
        return tasks

    def load_all_buckets(self, num_items, value_size=512, exp=0,
                         kv_store=1, flag=0, only_store_hash=True,
                         batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data synchronously on all buckets. Function wait for
        load data to finish.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        tasks = self.async_load_all_buckets(
            num_items, value_size, exp, kv_store, flag, only_store_hash,
            batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()
        if self.use_java_sdk and self.gen.get_sdk_logs:
            self.log.info(self.gen.get_sdk_results())

    def load_all_buckets_from_generator(self, kv_gen, ops=OPS.CREATE, exp=0,
                                        kv_store=1, flag=0, only_store_hash=True,
                                        batch_size=1000, pause_secs=1, timeout_secs=30,
                                        all_collections=True):
        """Load data synchronously on all buckets. Function wait for
        load data to finish.
        @param gen: BlobGenerator() object
        @param ops: OPS.CREATE/UPDATE/DELETE/APPEND.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @param all_collections: when False, java-sdk loader stays on
                _default._default; CB 8.x auto-exposes a `_system` scope
                whose collections are never XDCR-replicated, so setting
                this False is required for any test that compares
                src/dest curr_items across clusters.
        """
        if not self.use_java_sdk:
            # TODO append generator values if op_type is already present
            if ops not in self.__kv_gen:
                self.__kv_gen[ops] = kv_gen
            self.gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
        else:
            self.gen = SDKDataLoader(num_ops=kv_gen.end, percent_create=100, percent_update=0,
                                     percent_delete=0, doc_expiry=exp,
                                     all_collections=all_collections,
                                     key_prefix=kv_gen.name)

        tasks = []
        for bucket in self.__buckets:
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, self.gen,
                    bucket.kvs[kv_store], ops, exp, flag,
                    only_store_hash, batch_size, pause_secs, timeout_secs,
                    compression=self.sdk_compression)
            )
        for task in tasks:
            task.result()

    def async_load_all_buckets_from_generator(self, kv_gen, ops=OPS.CREATE, exp=0,
                                              kv_store=1, flag=0, only_store_hash=True,
                                              batch_size=1000, pause_secs=1, timeout_secs=30,
                                              all_collections=True):
        """Load data asynchronously on all buckets. Function wait for
        load data to finish.
        @param gen: BlobGenerator() object
        @param ops: OPS.CREATE/UPDATE/DELETE/APPEND.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @param all_collections: see load_all_buckets_from_generator.
        """
        if not self.use_java_sdk:
            # TODO append generator values if op_type is already present
            if ops not in self.__kv_gen:
                self.__kv_gen[ops] = kv_gen
            self.gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
        else:
            self.gen = SDKDataLoader(num_ops=kv_gen.end, percent_create=100, percent_update=0,
                                     percent_delete=0, doc_expiry=exp,
                                     all_collections=all_collections,
                                     key_prefix=kv_gen.name)

        tasks = []
        for bucket in self.__buckets:
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, self.gen,
                    bucket.kvs[kv_store], ops, exp, flag,
                    only_store_hash, batch_size, pause_secs, timeout_secs, compression=self.sdk_compression)
            )
        return tasks

    def load_all_buckets_till_dgm(self, active_resident_threshold, value_size, expiry):
        tasks = []
        for bucket in self.__buckets:
            tasks.append(self.__clusterop.async_load_gen_docs_till_dgm(server=self.__master_node, bucket=bucket,
                                                                       active_resident_threshold=active_resident_threshold,
                                                                       value_size=value_size,
                                                                       exp=expiry,
                                                                       java_sdk_client=self.use_java_sdk))
        return tasks

    def async_update_delete(
            self, op_type, perc=30, expiration=0, kv_store=1, num_items=1000):
        """Perform update/delete operation on all buckets. Function don't wait
        operation to finish.
        @param op_type: OPS.CREATE/OPS.UPDATE/OPS.DELETE
        @param perc: percentage of data to be deleted or created
        @param expiration: time for expire items
        @return: task object list
        """
        tasks = []
        if not self.use_java_sdk:
            raise_if(
                OPS.CREATE not in self.__kv_gen,
                XDCRException(
                    "Data is not loaded in cluster.Load data before update/delete")
            )
            for bucket in self.__buckets:
                if op_type == OPS.UPDATE:
                    if isinstance(self.__kv_gen[OPS.CREATE], BlobGenerator):
                        self.__kv_gen[OPS.UPDATE] = BlobGenerator(
                            self.__kv_gen[OPS.CREATE].name,
                            self.__kv_gen[OPS.CREATE].seed,
                            self.__kv_gen[OPS.CREATE].value_size,
                            start=0,
                            end=int(self.__kv_gen[OPS.CREATE].end * (float)(perc) / 100))
                    elif isinstance(self.__kv_gen[OPS.CREATE], DocumentGenerator):
                        self.__kv_gen[OPS.UPDATE] = DocumentGenerator(
                            self.__kv_gen[OPS.CREATE].name,
                            self.__kv_gen[OPS.CREATE].template,
                            self.__kv_gen[OPS.CREATE].args,
                            start=0,
                            end=int(self.__kv_gen[OPS.CREATE].end * (float)(perc) / 100))
                    gen = copy.deepcopy(self.__kv_gen[OPS.UPDATE])
                elif op_type == OPS.DELETE:
                    if isinstance(self.__kv_gen[OPS.CREATE], BlobGenerator):
                        self.__kv_gen[OPS.DELETE] = BlobGenerator(
                            self.__kv_gen[OPS.CREATE].name,
                            self.__kv_gen[OPS.CREATE].seed,
                            self.__kv_gen[OPS.CREATE].value_size,
                            start=int((self.__kv_gen[OPS.CREATE].end) * (float)(
                                100 - perc) // 100),
                            end=self.__kv_gen[OPS.CREATE].end)
                    elif isinstance(self.__kv_gen[OPS.CREATE], DocumentGenerator):
                        self.__kv_gen[OPS.DELETE] = DocumentGenerator(
                            self.__kv_gen[OPS.CREATE].name,
                            self.__kv_gen[OPS.CREATE].template,
                            self.__kv_gen[OPS.CREATE].args,
                            start=0,
                            end=int(self.__kv_gen[OPS.CREATE].end * (float)(perc) / 100))
                    gen = copy.deepcopy(self.__kv_gen[OPS.DELETE])
                else:
                    raise XDCRException("Unknown op_type passed: %s" % op_type)

            self.__log.info("At bucket '{0}' @ {1}: operation: {2}, key range {3} - {4}".
                       format(bucket.name, self.__name, op_type, gen.start, gen.end))
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node,
                    bucket.name,
                    gen,
                    bucket.kvs[kv_store],
                    op_type,
                    expiration,
                    batch_size=1000,
                    compression=self.sdk_compression)
            )
        else:
            for bucket in self.__buckets:
                percent_update = 0
                percent_delete = 0
                if op_type == OPS.UPDATE:
                    percent_update = perc
                elif op_type == OPS.DELETE:
                    percent_delete = perc
                gen = SDKDataLoader(num_ops=num_items, percent_create=0, percent_update=percent_update,
                                    percent_delete=percent_delete, doc_expiry=expiration, all_collections=True,
                                    key_prefix="%s-key-" % self.__name)
                tasks.append(
                    self.__clusterop.async_load_gen_docs(
                        self.__master_node,
                        bucket.name,
                        gen)
                )
        return tasks

    def update_delete_data(
            self, op_type, perc=30, expiration=0, wait_for_expiration=True):
        """Perform update/delete operation on all buckets. Function wait
        operation to finish.
        @param op_type: OPS.CREATE/OPS.UPDATE/OPS.DELETE
        @param perc: percentage of data to be deleted or created
        @param expiration: time for expire items
        @param wait_for_expiration: True if wait for expire of items after
        update else False
        """
        tasks = self.async_update_delete(op_type, perc, expiration)

        [task.result() for task in tasks]

        if wait_for_expiration and expiration:
            self.__log.info("Waiting for expiration of updated items")
            time.sleep(expiration)

    def run_expiry_pager(self, val=10):
        """Run expiry pager process and set interval to 10 seconds
        and wait for 10 seconds.
        @param val: time in seconds.
        """
        for bucket in self.__buckets:
            ClusterOperationHelper.flushctl_set(
                self.__master_node,
                "exp_pager_stime",
                val,
                bucket)
            self.__log.info("wait for expiry pager to run on all these nodes")
        time.sleep(val)

    def async_create_views(
            self, design_doc_name, views, bucket=BUCKET_NAME.DEFAULT):
        """Create given views on Cluster.
        @param design_doc_name: name of design doc.
        @param views: views objects.
        @param bucket: bucket name.
        @return: task list for CreateViewTask
        """
        tasks = []
        if len(views):
            for view in views:
                task = self.__clusterop.async_create_view(
                    self.__master_node,
                    design_doc_name,
                    view,
                    bucket)
                tasks.append(task)
        else:
            task = self.__clusterop.async_create_view(
                self.__master_node,
                design_doc_name,
                None,
                bucket)
            tasks.append(task)
        return tasks

    def async_compact_view(
            self, design_doc_name, bucket=BUCKET_NAME.DEFAULT,
            with_rebalance=False):
        """Create given views on Cluster.
        @param design_doc_name: name of design doc.
        @param bucket: bucket name.
        @param with_rebalance: True if compaction is called during
        rebalance or False.
        @return: task object
        """
        task = self.__clusterop.async_compact_view(
            self.__master_node,
            design_doc_name,
            bucket,
            with_rebalance)
        return task

    def disable_compaction(self, bucket=BUCKET_NAME.DEFAULT):
        """Disable view compaction
        @param bucket: bucket name.
        """
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.__clusterop.modify_fragmentation_config(
            self.__master_node,
            new_config,
            bucket)

    def async_monitor_view_fragmentation(
            self,
            design_doc_name,
            fragmentation_value,
            bucket=BUCKET_NAME.DEFAULT):
        """Monitor view fragmantation during compation.
        @param design_doc_name: name of design doc.
        @param fragmentation_value: fragmentation threshold to monitor.
        @param bucket: bucket name.
        """
        task = self.__clusterop.async_monitor_view_fragmentation(
            self.__master_node,
            design_doc_name,
            fragmentation_value,
            bucket)
        return task

    def async_query_view(
            self, design_doc_name, view_name, query,
            expected_rows=None, bucket="default", retry_time=2):
        """Perform View Query for given view asynchronously.
        @param design_doc_name: design_doc name.
        @param view_name: view name
        @param query: query expression
        @param expected_rows: number of rows expected returned in query.
        @param bucket: bucket name.
        @param retry_time: retry to perform view query
        @return: task object of ViewQueryTask class
        """
        task = self.__clusterop.async_query_view(
            self.__master_node,
            design_doc_name,
            view_name,
            query,
            expected_rows,
            bucket=bucket,
            retry_time=retry_time)
        return task

    def query_view(
            self, design_doc_name, view_name, query,
            expected_rows=None, bucket="default", retry_time=2, timeout=None):
        """Perform View Query for given view synchronously.
        @param design_doc_name: design_doc name.
        @param view_name: view name
        @param query: query expression
        @param expected_rows: number of rows expected returned in query.
        @param bucket: bucket name.
        @param retry_time: retry to perform view query
        @param timeout: None if wait for query result until returned
        else pass timeout value.
        """

        task = self.__clusterop.async_query_view(
            self.__master_node,
            design_doc_name,
            view_name,
            query,
            expected_rows,
            bucket=bucket, retry_time=retry_time)
        task.result(timeout)

    def __async_rebalance_out(self, master=False, num_nodes=1):
        """Rebalance-out nodes from Cluster
        @param master: True if rebalance-out master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        """
        raise_if(
            len(self.__nodes) <= num_nodes,
            XDCRException(
                "Cluster needs:{0} nodes for rebalance-out, current: {1}".
                format((num_nodes + 1), len(self.__nodes)))
        )
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = self.__nodes[-num_nodes:]
        self.__log.info(
            "Starting rebalance-out nodes:{0} at {1} cluster {2}".format(
                to_remove_node, self.__name, self.__master_node.ip))
        task = self.__clusterop.async_rebalance(
            self.__nodes,
            [],
            to_remove_node)

        [self.__nodes.remove(node) for node in to_remove_node]

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def async_rebalance_in_out(self, remove_nodes=None, num_add_nodes=1, master=False):
        """Rebalance-in-out nodes from Cluster
        @param remove_nodes: a list of nodes to be rebalanced-out
        @param master: True if rebalance-out master node only.
        @param num_nodes: number of nodes to add back to cluster.
        """
        raise_if(len(FloatingServers._serverlist) < num_add_nodes,
            XDCRException(
                "Cluster needs {0} nodes for rebalance-in, current: {1}".
                format((num_add_nodes),
                       len(FloatingServers._serverlist)))
        )

        add_nodes = []
        for _ in range(num_add_nodes):
            add_nodes.append(FloatingServers._serverlist.pop())

        self.__log.info(
            "Starting rebalance-out: {0}, rebalance-in: {1} at {2} cluster {3}".
            format(
                remove_nodes,
                add_nodes,
                self.__name,
                self.__master_node.ip))
        task = self.__clusterop.async_rebalance(
            self.__nodes,
            add_nodes,
            remove_nodes)

        if not remove_nodes:
            remove_nodes = self.__fail_over_nodes

        for node in remove_nodes:
            for server in self.__nodes:
                if node.ip == server.ip:
                    self.__nodes.remove(server)
        self.__nodes.extend(add_nodes)

        if master:
            self.__master_node = self.__nodes[0]
        return task

    def async_rebalance_out_master(self):
        return self.__async_rebalance_out(master=True)

    def async_rebalance_out(self, num_nodes=1):
        return self.__async_rebalance_out(num_nodes=num_nodes)

    def rebalance_out_master(self):
        task = self.__async_rebalance_out(master=True)
        task.result()

    def rebalance_out(self, num_nodes=1):
        task = self.__async_rebalance_out(num_nodes=num_nodes)
        task.result()

    def async_rebalance_in(self, num_nodes=1):
        """Rebalance-in nodes into Cluster asynchronously
        @param num_nodes: number of nodes to rebalance-in to cluster.
        """
        raise_if(
            len(FloatingServers._serverlist) < num_nodes,
            XDCRException(
                "Number of free nodes: {0} is not preset to add {1} nodes.".
                format(len(FloatingServers._serverlist), num_nodes))
        )
        to_add_node = []
        for _ in range(num_nodes):
            to_add_node.append(FloatingServers._serverlist.pop())
        self.__log.info(
            "Starting rebalance-in nodes:{0} at {1} cluster {2}".format(
                to_add_node, self.__name, self.__master_node.ip))
        task = self.__clusterop.async_rebalance(self.__nodes, to_add_node, [])
        self.__nodes.extend(to_add_node)
        return task

    def rebalance_in(self, num_nodes=1):
        """Rebalance-in nodes
        @param num_nodes: number of nodes to add to cluster.
        """
        task = self.async_rebalance_in(num_nodes)
        task.result()

    def __async_swap_rebalance(self, master=False):
        """Swap-rebalance nodes on Cluster
        @param master: True if swap-rebalance master node else False.
        """
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = [self.__nodes[-1]]

        to_add_node = [FloatingServers._serverlist.pop()]

        self.__log.info(
            "Starting swap-rebalance [remove_node:{0}] -> [add_node:{1}] at {2} cluster {3}"
            .format(to_remove_node[0].ip, to_add_node[0].ip, self.__name,
                    self.__master_node.ip))
        task = self.__clusterop.async_rebalance(
            self.__nodes,
            to_add_node,
            to_remove_node)

        [self.__nodes.remove(node) for node in to_remove_node]
        self.__nodes.extend(to_add_node)

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def async_swap_rebalance_master(self):
        return self.__async_swap_rebalance(master=True)

    def async_swap_rebalance(self):
        return self.__async_swap_rebalance()

    def swap_rebalance_master(self):
        """Swap rebalance master node.
        """
        task = self.__async_swap_rebalance(master=True)
        task.result()

    def swap_rebalance(self):
        """Swap rebalance non-master node
        """
        task = self.__async_swap_rebalance()
        task.result()

    def __async_failover(self, master=False, num_nodes=1, graceful=False):
        """Failover nodes from Cluster
        @param master: True if failover master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        @param graceful: True if graceful failover else False.
        """
        raise_if(
            len(self.__nodes) <= 1,
            XDCRException(
                "More than 1 node required in cluster to perform failover")
        )
        if master:
            self.__fail_over_nodes = [self.__master_node]
        else:
            self.__fail_over_nodes = self.__nodes[-num_nodes:]

        self.__log.info(
            "Starting failover for nodes:{0} at {1} cluster {2}".format(
                self.__fail_over_nodes, self.__name, self.__master_node.ip))
        task = self.__clusterop.async_failover(
            self.__nodes,
            self.__fail_over_nodes,
            graceful)

        return task

    def async_failover(self, num_nodes=1, graceful=False):
        return self.__async_failover(num_nodes=num_nodes, graceful=graceful)

    def failover(self, num_nodes=1, graceful=False):
        self.__async_failover(num_nodes=num_nodes, graceful=graceful).result()

    def failover_and_rebalance_master(self, graceful=False, rebalance=True, master=True):
        """Failover master node
        @param graceful: True if graceful failover else False
        @param rebalance: True if do rebalance operation after failover.
        """
        task = self.__async_failover(master, graceful=graceful)
        task.result()
        if graceful:
            # wait for replica update
            time.sleep(60)
            # use rebalance stats to monitor failover
            RestConnection(self.__master_node).monitorRebalance()
        if rebalance:
            self.rebalance_failover_nodes()
        self.__master_node = self.__nodes[0]

    def failover_and_rebalance_nodes(self, num_nodes=1, graceful=False,
                                     rebalance=True):
        """ Failover non-master nodes
        @param num_nodes: number of nodes to failover.
        @param graceful: True if graceful failover else False
        @param rebalance: True if do rebalance operation after failover.
        """
        task = self.__async_failover(
            master=False,
            num_nodes=num_nodes,
            graceful=graceful)
        task.result()
        if graceful:
            time.sleep(60)
            # use rebalance stats to monitor failover
            RestConnection(self.__master_node).monitorRebalance()
        if rebalance:
            self.rebalance_failover_nodes()

    def rebalance_failover_nodes(self):
        self.__clusterop.rebalance(self.__nodes, [], self.__fail_over_nodes)
        [self.__nodes.remove(node) for node in self.__fail_over_nodes]
        self.__fail_over_nodes = []

    def add_back_node(self, recovery_type=None):
        """add-back failed-over node to the cluster.
            @param recovery_type: delta/full
        """
        raise_if(
            len(self.__fail_over_nodes) < 1,
            XDCRException("No failover nodes available to add_back")
        )
        rest = RestConnection(self.__master_node)
        server_nodes = rest.node_statuses()
        for failover_node in self.__fail_over_nodes:
            for server_node in server_nodes:
                if server_node.ip == failover_node.ip:
                    rest.add_back_node(server_node.id)
                    if recovery_type:
                        rest.set_recovery_type(
                            otpNode=server_node.id,
                            recoveryType=recovery_type)
        for node in self.__fail_over_nodes:
            if node not in self.__nodes:
                self.__nodes.append(node)
        self.__clusterop.rebalance(self.__nodes, [], [])
        self.__fail_over_nodes = []

    def warmup_node(self, master=False):
        """Warmup node on cluster
        @param master: True if warmup master-node else False.
        """
        from random import randrange

        if master:
            warmup_node = self.__master_node

        else:
            warmup_node = self.__nodes[
                randrange(
                    1, len(
                        self.__nodes))]
        NodeHelper.do_a_warm_up(warmup_node)
        return warmup_node

    def reboot_one_node(self, test_case, master=False):
        from random import randrange

        if master:
            reboot_node = self.__master_node

        else:
            reboot_node = self.__nodes[
                randrange(
                    1, len(
                        self.__nodes))]
        NodeHelper.reboot_server(reboot_node, test_case)
        return reboot_node

    def restart_couchbase_on_all_nodes(self, bucket_names=["default"]):
        for node in self.__nodes:
            NodeHelper.do_a_warm_up(node)

        NodeHelper.wait_warmup_completed(self.__nodes, bucket_names)

    def get_xdcr_param(self, param):
        values = []
        for remote_ref in self.get_remote_clusters():
            for repl in remote_ref.get_replications():
                src_bucket = repl.get_src_bucket()
                dst_bucket = repl.get_dest_bucket()
                value = RestConnection(self.__master_node).get_xdcr_param(src_bucket.name, dst_bucket.name, param)
                print("{} is {} for {}".format(param, value, remote_ref))
                values.append(value)
        return values

    def set_xdcr_param(self, param, value):
        """Set Replication parameter on couchbase server:
        @param param: XDCR parameter name.
        @param value: Value of parameter.
        """
        for remote_ref in self.get_remote_clusters():
            for repl in remote_ref.get_replications():
                src_bucket = repl.get_src_bucket()
                dst_bucket = repl.get_dest_bucket()
                RestConnection(self.__master_node).set_xdcr_param(src_bucket.name, dst_bucket.name, param, value)

                expected_results = {
                    "real_userid:source": "ns_server",
                    "real_userid:user": self.__master_node.rest_username,
                    "local_cluster_name": "%s:%s" % (self.__master_node.ip, self.__master_node.port),
                    "updated_settings:" + param: value,
                    "source_bucket_name": repl.get_src_bucket().name,
                    "remote_cluster_name": "remote_cluster_C1-C2",
                    "target_bucket_name": repl.get_dest_bucket().name
                }

                # In case of ns_server xdcr, no events generate for it.
                ValidateAuditEvent.validate_audit_event(
                    GO_XDCR_AUDIT_EVENT_ID.IND_SETT,
                    self.get_master_node(),
                    expected_results)

    def get_xdcr_stat(self, bucket_name, stat):
        """ Return given XDCR stat for given bucket.
        @param bucket_name: name of bucket.
        @param stat: stat name
        @return: value of stat
        """
        return int(RestConnection(self.__master_node).fetch_bucket_xdcr_stats(
            bucket_name)['op']['samples'][stat][-1])

    def wait_for_xdcr_stat(self, bucket, stat, comparison, value):
        """Wait for given stat for a bucket to given condition.
        @param bucket: bucket name
        @param stat: stat name
        @param comparison: comparison operatior e.g. "==", "<"
        @param value: value to compare.
        """
        task = self.__clusterop.async_wait_for_xdcr_stat(
            self.__nodes,
            bucket,
            '',
            stat,
            comparison,
            value)
        task.result()

    def add_remote_cluster(self, dest_cluster, name, encryption=False, replicator_target_role=False,
                           multiple_ca=False, client_cert=None, client_key=None, systemeventlog=None):
        """Create remote cluster reference or add remote cluster for xdcr.
        @param dest_cluster: Destination cb cluster object.
        @param name: name of remote cluster reference
        @param encryption: True if encryption for xdcr else False
        """
        remote_cluster = XDCRRemoteClusterRef(
            self,
            dest_cluster,
            name,
            encryption,
            replicator_target_role,
            multiple_ca,
            client_cert,
            client_key,
            systemeventlog
        )
        remote_cluster.add()
        self.__remote_clusters.append(remote_cluster)

    # add params to what to modify
    def modify_remote_cluster(self, remote_cluster_name, require_encryption):
        """Modify Remote Cluster Reference Settings for given name.
        @param remote_cluster_name: name of the remote cluster to change.
        @param require_encryption: Value of encryption if need to change True/False.
        """
        for remote_cluster in self.__remote_clusters:
            if remote_cluster_name == remote_cluster.get_name():
                remote_cluster. modify(require_encryption)
                break
        else:
            raise XDCRException(
                "No such remote cluster found with name: {0}".format(
                    remote_cluster_name))

    def wait_for_flusher_empty(self, timeout=300):
        """Wait for disk queue to completely flush.
        """
        tasks = []
        for node in self.__nodes:
            for bucket in self.__buckets:
                tasks.append(
                    self.__clusterop.async_wait_for_stats(
                        [node],
                        bucket,
                        '',
                        'ep_queue_size',
                        '==',
                        0))
        for task in tasks:
            task.result(timeout)

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    # Sleep for interval seconds between polls, while waiting for event to complete
    def wait_interval(self, timeout=1, message=""):
        self.sleep(timeout, message)

    def verify_items_count(self, timeout=900):
        """Wait for actual bucket items count reach to the count on bucket kv_store.
        """
        active_key_count_passed = True
        replica_key_count_passed = True
        curr_time = time.time()
        end_time = curr_time + timeout

        # Check active, curr key count
        rest = RestConnection(self.__master_node)
        buckets = copy.copy(self.get_buckets())

        for bucket in buckets:
            items = sum([len(kv_store) for kv_store in list(bucket.kvs.values())])
            while True:
                try:
                    active_keys = int(rest.get_active_key_count(bucket.name))
                except Exception as e:
                    self.__log.error(e)
                    bucket_info = rest.get_bucket_json(bucket.name)
                    nodes = bucket_info["nodes"]
                    active_keys = 0
                    for node in nodes:
                        active_keys += node["interestingStats"]["curr_items"]
                if active_keys != items:
                        self.__log.warning("Not Ready: vb_active_curr_items %s == "
                                "%s expected on %s, %s bucket"
                                 % (active_keys, items, self.__name, bucket.name))
                        time.sleep(10)
                        if time.time() > end_time:
                            self.__log.error(
                            "ERROR: Timed-out waiting for active item count to match")
                            active_key_count_passed = False
                            break
                        continue
                else:
                    self.__log.info("Saw: vb_active_curr_items %s == "
                            "%s expected on %s, %s bucket"
                            % (active_keys, items, self.__name, bucket.name))
                    break
        # check replica count
        curr_time = time.time()
        end_time = curr_time + timeout
        buckets = copy.copy(self.get_buckets())

        for bucket in buckets:
            if len(self.__nodes) > 1:
                items = sum([len(kv_store) for kv_store in list(bucket.kvs.values())])
                items = items * bucket.numReplicas
            else:
                items = 0
            while True:
                try:
                    replica_keys = int(rest.get_replica_key_count(bucket.name))
                except Exception as e:
                    self.__log.error(e)
                    bucket_info = rest.get_bucket_json(bucket.name)
                    nodes = bucket_info["nodes"]
                    replica_keys = 0
                    for node in nodes:
                        replica_keys += node["interestingStats"]["vb_replica_curr_items"]
                if replica_keys != items:
                    self.__log.warning("Not Ready: vb_replica_curr_items %s == "
                            "%s expected on %s, %s bucket"
                             % (replica_keys, items, self.__name, bucket.name))
                    time.sleep(10)
                    if time.time() > end_time:
                        self.__log.error(
                        "ERROR: Timed-out waiting for replica item count to match")
                        replica_key_count_passed = False
                        self.run_cbvdiff()
                        break
                    continue
                else:
                    self.__log.info("Saw: vb_replica_curr_items %s == "
                            "%s expected on %s, %s bucket"
                            % (replica_keys, items, self.__name, bucket.name))
                    break
        return active_key_count_passed, replica_key_count_passed

    def run_cbvdiff(self):
        """ Run cbvdiff, a tool that compares active and replica vbucket keys
        Eg. ./cbvdiff -b standardbucket  172.23.105.44:11210,172.23.105.45:11210
             VBucket 232: active count 59476 != 59477 replica count
        """
        node_str = ""
        for node in self.__nodes:
            if node_str:
                node_str += ','
            node_str += node.ip + ':11210'
        ssh_conn = RemoteMachineShellConnection(self.__master_node)
        for bucket in self.__buckets:
            self.__log.info(
                "Executing cbvdiff for bucket {0}".format(
                    bucket.name))
            ssh_conn.execute_cbvdiff(bucket, node_str)
        ssh_conn.disconnect()

    def verify_data(self, kv_store=1, timeout=None,
                    max_verify=None, only_store_hash=True, batch_size=1000, skip=[]):
        """Verify data of all the buckets. Function read data from cb server and
        compare it with bucket's kv_store.
        @param kv_store: Index of kv_store where item values are stored on
        bucket.
        @param timeout: None if wait indefinitely else give timeout value.
        @param max_verify: number of items to verify. None if verify all items
        on bucket.
        @param only_store_hash: True if verify hash of items else False.
        @param batch_size: batch size to read items from server.
        """
        self.__data_verified = False
        tasks = []
        for bucket in self.__buckets:
            if bucket.name not in skip:
                tasks.append(
                        self.__clusterop.async_verify_data(
                                self.__master_node,
                                bucket,
                                bucket.kvs[kv_store],
                                max_verify,
                                only_store_hash,
                                batch_size,
                                timeout_sec=60,
                                compression=self.sdk_compression))
        for task in tasks:
            task.result(timeout)

        self.__data_verified = True

    def verify_meta_data(self, kv_store=1, timeout=None, skip=[]):
        """Verify if metadata for bucket matches on src and dest clusters
        @param kv_store: Index of kv_store where item values are stored on
        bucket.
        @param timeout: None if wait indefinitely else give timeout value.
        """
        self.__meta_data_verified = False
        tasks = []
        for bucket in self.__buckets:
            if bucket.name not in skip:
                data_map = {}
                gather_task = self.__clusterop.async_get_meta_data(self.__master_node, bucket, bucket.kvs[kv_store],
                                                        compression=self.sdk_compression)
                gather_task.result()
                data_map[bucket.name] = gather_task.get_meta_data_store()
                tasks.append(
                    self.__clusterop.async_verify_meta_data(
                        self.__master_node,
                        bucket,
                        bucket.kvs[kv_store],
                        data_map[bucket.name]
                        ))
        for task in tasks:
            task.result(timeout)
        self.__meta_data_verified = True

    def wait_for_dcp_queue_drain(self, timeout=600):
        """Wait for ep_dcp_xdcr_items_remaining to reach 0.
        @return: True if reached 0 else False.
        """
        self.__log.info(
            "Waiting for dcp queue to drain on cluster node: %s" %
            self.__master_node.ip)
        curr_time = time.time()
        end_time = curr_time + timeout
        rest = RestConnection(self.__master_node)
        buckets = copy.copy(self.get_buckets())
        for bucket in buckets:
            try:
                mutations = int(rest.get_dcp_queue_size(bucket.name))
                self.__log.info(
                    "Current dcp queue size on %s for %s is %s" %
                    (self.__name, bucket.name, mutations))
                if mutations == 0:
                    buckets.remove(bucket)
                else:
                    time.sleep(5)
                    end_time = end_time - 5
            except KeyError:
                self.__log.info(
                    "No XDCR DCP stats on %s for %s, treating as drained" %
                    (self.__name, bucket.name))
                buckets.remove(bucket)
            except Exception as e:
                self.__log.error(e)
            if curr_time > end_time:
                self.__log.error(
                "Timeout occurs while waiting for dcp queue to drain")
                return False
        return True

    def wait_for_outbound_mutations(self, timeout=600):
        """Wait for Outbound mutations to reach 0.
        @return: True if mutations reached to 0 else False.
        """
        curr_time = time.time()
        end_time = curr_time + timeout
        rest = RestConnection(self.__master_node)
        while curr_time < end_time:
            found = 0
            for bucket in self.__buckets:
                mutations = 0
                try:
                    mutations = int(rest.get_xdc_queue_size(bucket.name))
                except KeyError:
                    self.__log.info(
                        "No outbound XDCR replication stats on %s for %s, treating as 0" %
                        (self.__name, bucket.name))
                self.__log.info(
                    "Current Outbound mutations on cluster node: %s for bucket %s is %s" %
                    (self.__name, bucket.name, mutations))
                if mutations == 0:
                    found = found + 1
            if found == len(self.__buckets):
                break
            time.sleep(5)
            end_time = end_time - 5
        else:
            # MB-9707: Updating this code from fail to warning to avoid test
            # to abort, as per this
            # bug, this particular stat i.e. replication_changes_left is buggy.
            self.__log.error(
                "Timeout occurred while waiting for mutations to be replicated")
            return False
        return True

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    # Sleep for interval seconds between polls, while waiting for event to complete
    def wait_interval(self, timeout=1, message=""):
        self.sleep(timeout, message)

    def pause_all_replications(self, verify=False):
        for remote_cluster_ref in self.__remote_clusters:
            remote_cluster_ref.pause_all_replications(verify)

    def pause_all_replications_by_id(self, verify=False):
        for remote_cluster_ref in self.__remote_clusters:
            remote_cluster_ref.pause_all_replications_by_id(verify)

    def resume_all_replications(self, verify=False):
        for remote_cluster_ref in self.__remote_clusters:
            remote_cluster_ref.resume_all_replications(verify)

    def resume_all_replications_by_id(self, verify=False):
        for remote_cluster_ref in self.__remote_clusters:
            remote_cluster_ref.resume_all_replications_by_id(verify)

    def enable_time_sync(self, enable):
        """
        @param enable: True if time_sync needs to enabled else False
        """
        # TODO call rest api
        pass

    def set_wall_clock_time(self, date_str):
        for node in self.__nodes:
            NodeHelper.set_wall_clock_time(node, date_str)


class Utility:

    @staticmethod
    def make_default_views(prefix, num_views, is_dev_ddoc=False):
        """Create default views for testing.
        @param prefix: prefix for view name
        @param num_views: number of views to create
        @param is_dev_ddoc: True if Development View else False
        """
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        default_view_name = (prefix, "default_view")[prefix is None]
        return [View(default_view_name + str(i), default_map_func,
                     None, is_dev_ddoc) for i in range(num_views)]

    @staticmethod
    def get_rc_name(src_cluster_name, dest_cluster_name):
        return "remote_cluster_" + src_cluster_name + "-" + dest_cluster_name


class XDCRNewBaseTest(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self._input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()
        self.__init_logger()
        self.__cb_clusters = []
        self.__cluster_op = Cluster()
        self.__init_parameters()
        self.filter_exp = {}
        self.log.info(
            "==== XDCRNewbasetests setup is started for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

        self.__setup_for_test()

        self.log.info(
            "==== XDCRNewbasetests setup is finished for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

    def __is_test_failed(self):
        return (hasattr(self, '_resultForDoCleanups')
                and len(self._resultForDoCleanups.failures
                        or self._resultForDoCleanups.errors)) \
            or (hasattr(self, '_exc_info')
                and self._exc_info()[1] is not None)

    def __is_cleanup_needed(self):
        return self.__is_test_failed() and (str(self.__class__).find(
            'upgradeXDCR') != -1 or self._input.param("stop-on-failure", False)
        )

    def __is_cluster_run(self):
        return len({server.ip for server in self._input.servers}) == 1

    def tearDown(self):
        if self.validate_systemeventlog:
            for cluster in self.__cb_clusters:
                for remote_cluster_ref in cluster.get_remote_clusters():
                    src_cluster = remote_cluster_ref.get_src_cluster()
                    systemeventlog_failures = self._systemeventlog.validate(src_cluster.get_master_node())
                    if len(systemeventlog_failures):
                        raise XDCRException(
                        "System event log validation failed on cluster {0}\n{1}".format(src_cluster.get_master_node(),
                                                                                        systemeventlog_failures))

        """Clusters cleanup"""
        if self._input.param("negative_test", False):
            if hasattr(self, '_resultForDoCleanups') \
                and len(self._resultForDoCleanups.failures
                        or self._resultForDoCleanups.errors):
                self._resultForDoCleanups.failures = []
                self._resultForDoCleanups.errors = []
                self.log.info("This is marked as a negative test and contains "
                              "errors as expected, hence not failing it")
            else:
                raise XDCRException("Negative test passed!")

        # collect logs before tearing down clusters
        if self._input.param("get-cbcollect-info", False) and \
                self.__is_test_failed():
            NodeHelper.collect_logs(self._input.servers, self.__is_cluster_run())

        #for i in range(1, len(self.__cb_clusters) + 1):
        #    try:
                # Remove rbac users in teardown
                # role_del = ['cbadminbucket']
                # RbacBase().remove_user_role(role_del, RestConnection(self.get_cb_cluster_by_name('C' + str(i)).get_master_node()))
                # if self._replicator_role:
                #     role_del = ['replicator_user']
                #     RbacBase().remove_user_role(role_del,
                #                                 RestConnection(self.get_cb_cluster_by_name('C' + str(i)).get_master_node()))
        #    except Exception as e:
        #        self.log.warning(e)
        try:
            if self.__is_cleanup_needed() or self._input.param("skip_cleanup", False):
                self.log.warning("CLEANUP WAS SKIPPED")
                return
            self.log.info(
                "====  XDCRNewbasetests cleanup is started for test #{0} {1} ===="
                .format(self.__case_number, self._testMethodName))
            for cb_cluster in self.__cb_clusters:
                cb_cluster.cleanup_cluster(self, from_rest=True)
            self.log.info(
                "====  XDCRNewbasetests cleanup is finished for test #{0} {1} ==="
                .format(self.__case_number, self._testMethodName))
        except Exception as e:
            self.log.warning(e)
        finally:
            self.__cluster_op.shutdown(force=True)
            unittest.TestCase.tearDown(self)

    def __init_logger(self):
        if self._input.param("log_level", None):
            self.log.setLevel(level=0)
            for hd in self.log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    log_level = self._input.param("log_level", None)
                    if log_level:
                        log_level = str(log_level).upper()
                        hd.setLevel(level=getattr(logging, log_level))

    def add_built_in_server_user(self, testuser=None, rolelist=None, node=None):
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'roles': 'admin'}]

        self.log.info(f"Add built-in '{testuser[0]['name']}' user to node {node.ip}")
        RbacBase().create_user_source(testuser, 'builtin', node)
        # Some times in upgraded envs, user creation is taking some time. Added a small sleep to mitigate failures in subsequent steps.
        self.sleep(5)
        self.log.info("**** add '%s' role to '%s' user ****" % (rolelist[0]["roles"],
                                                                testuser[0]["name"]))
        status = RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')

    def add_user(self, id="cbadminbucket", name="cbadminbucket", password="password", roles="admin"):
        for i in range(1, len(self.__cb_clusters) + 1):
            self.log.info("Adding user: {} with roles: {}".format(name, roles))
            # Add built-in user
            testuser = [{'id': id, 'name': name, 'password': password}]
            self.log
            RbacBase().create_user_source(testuser, 'builtin',
                                          self.get_cb_cluster_by_name('C' + str(i)).get_master_node())
            # Assign role to user
            role_list = [{'id': id, 'name': name, 'roles': roles}]
            RbacBase().add_user_role(role_list,
                                     RestConnection(self.get_cb_cluster_by_name('C' + str(i)).get_master_node()),
                                     'builtin')

    def __setup_for_test(self):
        use_hostnames = self._input.param("use_hostnames", False)
        sdk_compression = self._input.param("sdk_compression", True)
        if self._use_java_sdk:
            JavaSdkSetup()
        if self._multiple_ca:
            CbServer.multiple_ca = True
            self._use_https = True
        if self._client_cert:
            CbServer.multiple_ca = True
            CbServer.use_client_certs = True
            CbServer.cacert_verify = True
        if self._use_https:
            CbServer.use_https = True
            self._demand_encryption = True
        global_vars.system_event_logs = EventHelper()
        self._systemeventlog = global_vars.system_event_logs
        self.validate_systemeventlog = \
            self._input.param("validate_systemeventlog", False)
        collection_density = self._input.param("collection_density", "low")
        if self._dgm_run:
            collection_density = "medium"
        scope_num = BUCKET_COLLECTIONS_DENSITY(collection_density).num_scopes
        collection_num = BUCKET_COLLECTIONS_DENSITY(collection_density).num_collections
        if scope_num or collection_num:
            self._num_items = BUCKET_COLLECTIONS_DENSITY(collection_density).num_docs
        counter = 1
        for _, nodes in self._input.clusters.items():
            cluster_nodes = copy.deepcopy(nodes)
            if len(self.__cb_clusters) == int(self.__chain_length):
                break
            self.__cb_clusters.append(
                CouchbaseCluster(
                    "C%s" % counter, cluster_nodes,
                    self.log, use_hostnames, sdk_compression=sdk_compression,
                    use_java_sdk=self._use_java_sdk, scope_num=scope_num,
                    collection_num=collection_num))
            counter += 1

        self.__cleanup_previous()
        self.__init_clusters()

        self.builtin_user_id = self._input.param("builtin_user_id", "cbadminbucket")
        self.builtin_user_name = self._input.param("builtin_user_name", "cbadminbucket")
        self.builtin_user_password = self._input.param("builtin_user_password", "password")
        self.builtin_user_roles = self._input.param("builtin_user_roles", "admin")
        self.add_user(self.builtin_user_id, self.builtin_user_name,
                      self.builtin_user_password, self.builtin_user_roles)

        self.enable_dp = self._input.param("enable_dp", False)
        if self.enable_dp:
            for node in self._input.servers:
                print("Enabling DP for %s" % node)
                cli = CouchbaseCLI(node)
                cli.enable_dp()

        self.__set_free_servers()
        if str(self.__class__).find('upgradeXDCR') == -1 and str(self.__class__).find('lww') == -1:
            self.__create_buckets()

        if self._replicator_role:
            for i in range(1, len(self.__cb_clusters) + 1):
                testuser_replicator = [{'id': 'replicator_user', 'name': 'replicator_user', 'password': 'password'}]
                RbacBase().create_user_source(testuser_replicator, 'builtin',
                                              self.get_cb_cluster_by_name('C' + str(i)).get_master_node())

                if self._replicator_role and self._replicator_all_buckets:
                    role = 'replication_target[*]'
                else:
                    role = 'replication_target[default]'
                role_list_replicator = [
                        {'id': 'replicator_user', 'name': 'replicator_user', 'roles': role}]
                RbacBase().add_user_role(role_list_replicator,
                                             RestConnection(self.get_cb_cluster_by_name('C' + str(i)).get_master_node()),
                                             'builtin')
        if self.validate_systemeventlog:
            self._systemeventlog.set_test_start_time()

    def __init_parameters(self):
        self.__case_number = self._input.param("case_number", 0)
        self.__topology = self._input.param("ctopology", TOPOLOGY.CHAIN)
        # complex topology tests (> 2 clusters must specify chain_length >2)
        self.__chain_length = self._input.param("chain_length", 2)
        self.__rep_type = self._input.param("replication_type", REPLICATION_PROTOCOL.XMEM)
        self.__num_sasl_buckets = self._input.param("sasl_buckets", 0)
        self.__num_stand_buckets = self._input.param("standard_buckets", 0)

        self.__eviction_policy = self._input.param("eviction_policy", 'fullEviction')
        self.__bucket_storage = self._input.param("bucket_storage", 'magma')
        self.__mixed_priority = self._input.param("mixed_priority", None)

        self.__lww = self._input.param("lww", 0)
        self.__fail_on_errors = self._input.param("fail_on_errors", True)
        # simply append to this list, any error from log we want to fail test on
        self.__report_error_list = []
        if self.__fail_on_errors:
            self.__report_error_list = ["panic:",
                                        "non-recoverable error from xmem client. response status=KEY_ENOENT"]

        # for format {ip1: {"panic": 2, "KEY_ENOENT":3}}
        self.__error_count_dict = {}
        if len(self.__report_error_list) > 0:
            self.__initialize_error_count_dict()

        self._repl_restart_count_dict = {}
        self.__initialize_repl_restart_count_dict()

        # Public init parameters - Used in other tests too.
        # Move above private to this section if needed in future, but
        # Ensure to change other tests too.
        self._demand_encryption = self._input.param(
            "demand_encryption",
            False)
        self._num_replicas = self._input.param("replicas", 1)
        self._create_default_bucket = self._input.param("default_bucket", True)
        self._rdirection = self._input.param("rdirection",
                            REPLICATION_DIRECTION.UNIDIRECTION)
        self._num_items = self._input.param("items", 100000)
        self._value_size = self._input.param("value_size", 512)
        self._poll_timeout = self._input.param("poll_timeout", 600)
        self._perc_upd = self._input.param("upd", 30)
        self._perc_del = self._input.param("del", 30)
        self._upd_clusters = self._input.param("update", [])
        if self._upd_clusters:
            self._upd_clusters = self._upd_clusters.split("-")
        self._del_clusters = self._input.param("delete", [])
        if self._del_clusters:
            self._del_clusters = self._del_clusters.split('-')
        self._expires = self._input.param("expires", 0)
        self._wait_for_expiration = self._input.param("wait_for_expiration", True)
        self._warmup = self._input.param("warm", "").split('-')
        self._rebalance = self._input.param("rebalance", "").split('-')
        self._failover = self._input.param("failover", "").split('-')
        self._wait_timeout = self._input.param("timeout", 300)
        self._disable_compaction = self._input.param("disable_compaction", "").split('-')
        self._item_count_timeout = self._input.param("item_count_timeout", 900)
        self._checkpoint_interval = self._input.param("checkpoint_interval", 60)
        self._optimistic_threshold = self._input.param("optimistic_threshold", 256)
        self._compression_type = self._input.param("compression_type", "")
        self._dgm_run = self._input.param("dgm_run", False)
        self._active_resident_threshold = \
            self._input.param("active_resident_threshold", 100)
        CHECK_AUDIT_EVENT.CHECK = self._input.param("verify_audit", 0)
        self._max_verify = self._input.param("max_verify", 100000)
        self._sdk_compression = self._input.param("sdk_compression", True)
        self._evict_with_compactor = self._input.param("evict_with_compactor", False)
        self._replicator_role = self._input.param("replicator_role", False)
        self._replicator_all_buckets = self._input.param("replicator_all_buckets", False)
        self._use_java_sdk = self._input.param("java_sdk_client", False)
        self._multiple_ca = self._input.param("multiple_ca", None)
        self._client_cert = self._input.param("client_cert", None)
        self._client_key = self._input.param("client_key", None)
        self._use_https = self._input.param("use_https", False)
        self._version_pruning_window_hrs = self._input.param("version_pruning_window_hrs", None)
        self._enable_cross_cluster_versioning = self._input.param("enable_cross_cluster_versioning", None)
        self.variable_vbuckets_test = self._input.param("variable_vbucket_test", False)

    def __initialize_error_count_dict(self):
        """
            initializes self.__error_count_dict with ip, error and err count
            like {ip1: {"panic": 2, "KEY_ENOENT":3}}
        """
        if not self.__is_cluster_run():
            goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'
        for node in self._input.servers:
            if self.__is_cluster_run():
                goxdcr_log = NodeHelper.get_goxdcr_log_dir(node)\
                     + '/goxdcr.log*'
            self.__error_count_dict[node.ip] = {}
            for error in self.__report_error_list:
                _, self.__error_count_dict[node.ip][error] = \
                    NodeHelper.check_goxdcr_log(node, error, goxdcr_log)
        self.log.info(self.__error_count_dict)

    def __initialize_repl_restart_count_dict(self):
        """
            initializes self.__error_count_dict with ip, repl restart count
            like {{ip1: 3}, {ip2: 4}}
        """
        if not self.__is_cluster_run():
            goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'
        for node in self._input.servers:
            if self.__is_cluster_run():
                goxdcr_log = NodeHelper.get_goxdcr_log_dir(node)\
                     + '/goxdcr.log*'
            _, self._repl_restart_count_dict[node.ip] = \
                NodeHelper.check_goxdcr_log(node,
                                            "Try to fix Pipeline",
                                            goxdcr_log)
        self.log.info(self._repl_restart_count_dict)

    def __cleanup_previous(self):
        for cluster in self.__cb_clusters:
            cluster.cleanup_cluster(
                self,
                from_rest=True,
                cluster_shutdown=False)

    def __init_clusters(self):
        self.log.info("Initializing all clusters...")
        disabled_consistent_view = self._input.param(
            "disabled_consistent_view",
            None)
        for cluster in self.__cb_clusters:
            cluster.init_cluster(disabled_consistent_view)

    def __set_free_servers(self):
        total_servers = self._input.servers
        cluster_nodes = []
        for _, nodes in self._input.clusters.items():
            cluster_nodes.extend(nodes)
        for server in total_servers:
            for cluster_node in cluster_nodes:
                if server.ip == cluster_node.ip and\
                                server.port == cluster_node.port:
                    break
                else:
                    continue
            else:
                FloatingServers._serverlist.append(server)

    def get_random_bucket_vbs(self):
        vbucket_options = ["magma_128vbs", "magma_1024vbs", "couchstore"]
        random.shuffle(vbucket_options)
        return vbucket_options

    def get_cluster_op(self):
        return self.__cluster_op

    def get_cb_cluster_by_name(self, name):
        """Return couchbase cluster object for given name.
        @return: CouchbaseCluster object
        """
        for cb_cluster in self.__cb_clusters:
            if cb_cluster.get_name() == name:
                return cb_cluster
        raise XDCRException("Couchbase Cluster with name: %s not exist" % name)

    def get_num_cb_cluster(self):
        """Return number of couchbase clusters for tests.
        """
        return len(self.__cb_clusters)

    def get_cb_clusters(self):
        return self.__cb_clusters

    def get_lww(self):
        return self.__lww

    def get_report_error_list(self):
        return self.__report_error_list

    def _toggle_ce_backdoor_on_node(self, server, enable):
        """Toggle the CE-restrictions backdoor on a single node.

        Sets `disable_ce_restrictions` in ns_config via diag/eval, then
        kills goxdcr so babysitter respawns it. The flag is cluster-wide
        (ns_config replicates it to every node) -- toggling on any node
        affects the whole cluster.

        Relies on `allow_nonlocal_eval=true` being set cluster-wide,
        which CouchbaseCluster.init_cluster() does as its first step.
        """
        flag = "true" if enable else "false"
        code = "ns_config:set(disable_ce_restrictions, {0}).".format(flag)
        status, content = RestConnection(server).diag_eval(code)
        self.assertTrue(
            status, "diag/eval failed on %s setting %s: %s"
            % (server.ip, code, content))
        shell = RemoteMachineShellConnection(server)
        try:
            shell.kill_goxdcr()
        finally:
            shell.disconnect()

    def __calculate_bucket_size(self, cluster_quota, num_buckets):
        if 'quota_percent' in self._input.test_params:
            quota_percent = int(self._input.test_params['quota_percent'])
        else:
            quota_percent = None
        bucket_size = 0
        if quota_percent is not None and num_buckets > 0:
            bucket_size = int(float(cluster_quota - 500) * float(quota_percent/100.0) /float(num_buckets))
        elif num_buckets > 0:
            bucket_size = int((float(cluster_quota) - 500)/float(num_buckets))
        # Setting upper limit of 3GB for bucket size
        if bucket_size > 3072:
            bucket_size = 3072
        return bucket_size

    def __create_buckets(self):
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        if self.__mixed_priority:
            bucket_priority = 'high'
        else:
            bucket_priority = None
        num_buckets = self.__num_sasl_buckets + \
            self.__num_stand_buckets + int(self._create_default_bucket)

        maxttl = self._input.param("maxttl", None)
        bucket_vb_options = self.get_random_bucket_vbs()

        for cluster_index, cb_cluster in enumerate(self.__cb_clusters):
            num_vbuckets = None
            if self.variable_vbuckets_test:
                vb_conf = bucket_vb_options[cluster_index % 3]
                if vb_conf == "magma_128vbs":
                    self.__bucket_storage = "magma"
                    num_vbuckets = 128
                elif vb_conf == "magma_1024vbs":
                    self.__bucket_storage = "magma"
                    num_vbuckets = 1024
                else:
                    self.__bucket_storage = "couchstore"

            total_quota = cb_cluster.get_mem_quota()
            bucket_size = self.__calculate_bucket_size(
                total_quota,
                num_buckets)

            if self._create_default_bucket:
                cb_cluster.create_default_bucket(
                    bucket_size,
                    self._num_replicas,
                    eviction_policy=self.__eviction_policy,
                    bucket_priority=bucket_priority,
                    lww=self.__lww,
                    maxttl=maxttl,
                    bucket_storage=self.__bucket_storage,
                    num_vbuckets=num_vbuckets)

            cb_cluster.create_sasl_buckets(
                bucket_size, num_buckets=self.__num_sasl_buckets,
                num_replicas=self._num_replicas,
                eviction_policy=self.__eviction_policy,
                bucket_priority=bucket_priority, lww=self.__lww,
                maxttl=maxttl,
                bucket_storage=self.__bucket_storage)

            cb_cluster.create_standard_buckets(
                bucket_size, num_buckets=self.__num_stand_buckets,
                num_replicas=self._num_replicas,
                eviction_policy=self.__eviction_policy,
                bucket_priority=bucket_priority, lww=self.__lww,
                maxttl=maxttl,
                bucket_storage=self.__bucket_storage)

    def create_buckets_on_cluster(self, cluster_name):
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        if self.__mixed_priority:
            bucket_priority = 'high'
        else:
            bucket_priority = None
        num_buckets = self.__num_sasl_buckets + \
            self.__num_stand_buckets + int(self._create_default_bucket)

        vb_conf = self.get_random_bucket_vbs()[0]
        num_vbuckets = None
        if self.variable_vbuckets_test:
            if vb_conf == "magma_128vbs":
                self.__bucket_storage = "magma"
                num_vbuckets = 128
            elif vb_conf == "magma_1024vbs":
                self.__bucket_storage = "magma"
                num_vbuckets = 1024
            else:
                self.__bucket_storage = "couchstore"
        cb_cluster = self.get_cb_cluster_by_name(cluster_name)
        total_quota = cb_cluster.get_mem_quota()
        bucket_size = self.__calculate_bucket_size(
            total_quota,
            num_buckets)

        if self._create_default_bucket:
            cb_cluster.create_default_bucket(
                bucket_size,
                self._num_replicas,
                eviction_policy=self.__eviction_policy,
                bucket_priority=bucket_priority,
                num_vbuckets=num_vbuckets)

        cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

        cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority,
            num_vbuckets=num_vbuckets)

    def __set_topology_chain(self):
        """Will Setup Remote Cluster Chain Topology i.e. A -> B -> C
        """
        for i, cb_cluster in enumerate(self.__cb_clusters):
            if i >= len(self.__cb_clusters) - 1:
                break
            cb_cluster.add_remote_cluster(
                self.__cb_clusters[i + 1],
                Utility.get_rc_name(
                    cb_cluster.get_name(),
                    self.__cb_clusters[i + 1].get_name()),
                self._demand_encryption,
                self._replicator_role,
                self._multiple_ca,
                self._client_cert,
                self._client_key,
                self._systemeventlog
            )
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                self.__cb_clusters[i + 1].add_remote_cluster(
                    cb_cluster,
                    Utility.get_rc_name(
                        self.__cb_clusters[i + 1].get_name(),
                        cb_cluster.get_name()),
                    self._demand_encryption,
                    self._replicator_role,
                    self._multiple_ca,
                    self._client_cert,
                    self._client_key,
                    self._systemeventlog
                )

    def __set_topology_star(self):
        """Will Setup Remote Cluster Star Topology i.e. A-> B, A-> C, A-> D
        """
        hub = self.__cb_clusters[0]
        for cb_cluster in self.__cb_clusters[1:]:
            hub.add_remote_cluster(
                cb_cluster,
                Utility.get_rc_name(hub.get_name(), cb_cluster.get_name()),
                self._demand_encryption,
                self._replicator_role,
                self._multiple_ca,
                self._client_cert,
                self._client_key,
                self._systemeventlog
            )
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                cb_cluster.add_remote_cluster(
                    hub,
                    Utility.get_rc_name(cb_cluster.get_name(), hub.get_name()),
                    self._demand_encryption,
                    self._replicator_role,
                    self._multiple_ca,
                    self._client_cert,
                    self._client_key,
                    self._systemeventlog
                )

    def __set_topology_ring(self):
        """
        Will Setup Remote Cluster Ring Topology i.e. A -> B -> C -> A
        """
        self.__set_topology_chain()
        self.__cb_clusters[-1].add_remote_cluster(
            self.__cb_clusters[0],
            Utility.get_rc_name(
                self.__cb_clusters[-1].get_name(),
                self.__cb_clusters[0].get_name()),
            self._demand_encryption,
            self._replicator_role,
            self._multiple_ca,
            self._client_cert,
            self._client_key,
            self._systemeventlog
        )
        if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
            self.__cb_clusters[0].add_remote_cluster(
                self.__cb_clusters[-1],
                Utility.get_rc_name(
                    self.__cb_clusters[0].get_name(),
                    self.__cb_clusters[-1].get_name()),
                self._demand_encryption,
                self._replicator_role,
                self._multiple_ca,
                self._client_cert,
                self._client_key,
                self._systemeventlog
            )

    def set_xdcr_topology(self):
        """Setup xdcr topology as per ctopology test parameter.
        """
        if self.__topology == TOPOLOGY.CHAIN:
            self.__set_topology_chain()
        elif self.__topology == TOPOLOGY.STAR:
            self.__set_topology_star()
        elif self.__topology == TOPOLOGY.RING:
            self.__set_topology_ring()
        elif self._input.param(TOPOLOGY.HYBRID, 0):
            self.set_hybrid_topology()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    def __parse_topology_param(self):
        tokens = re.split(r'(>|<>|<|\s)', self.__topology)
        return tokens

    def set_hybrid_topology(self):
        """Set user defined topology
        Hybrid Topology Notations:
        '> or <' for Unidirection replication between clusters
        '<>' for Bi-direction replication between clusters
        Test Input:  ctopology="C1>C2<>C3>C4<>C1"
        """
        tokens = self.__parse_topology_param()
        counter = 0
        while counter < len(tokens) - 1:
            src_cluster = self.get_cb_cluster_by_name(tokens[counter])
            dest_cluster = self.get_cb_cluster_by_name(tokens[counter + 2])
            if ">" in tokens[counter + 1]:
                src_cluster.add_remote_cluster(
                    dest_cluster,
                    Utility.get_rc_name(
                        src_cluster.get_name(),
                        dest_cluster.get_name()),
                    self._demand_encryption,
                    self._replicator_role,
                    self._multiple_ca,
                    self._client_cert,
                    self._client_key,
                    self._systemeventlog
                )
            if "<" in tokens[counter + 1]:
                dest_cluster.add_remote_cluster(
                    src_cluster,
                    Utility.get_rc_name(
                        dest_cluster.get_name(), src_cluster.get_name()),
                    self._demand_encryption,
                    self._replicator_role,
                    self._multiple_ca,
                    self._client_cert,
                    self._client_key,
                    self._systemeventlog
                )
            counter += 2

    def __async_load_chain(self):
        for i, cluster in enumerate(self.__cb_clusters):
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                if i > len(self.__cb_clusters) - 1:
                    break
            else:
                if i >= len(self.__cb_clusters) - 1:
                    break
            if not self._dgm_run:
                return cluster.async_load_all_buckets(self._num_items,
                                                      self._value_size,
                                                      self._expires)
            else:
                cluster.load_all_buckets_till_dgm(self._active_resident_threshold,
                                              self._value_size,
                                              self._expires)

    def __load_chain(self):
        for i, cluster in enumerate(self.__cb_clusters):
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                if i > len(self.__cb_clusters) - 1:
                    break
            else:
                if i >= len(self.__cb_clusters) - 1:
                    break
            if not self._dgm_run:
                cluster.load_all_buckets(self._num_items, self._value_size, self._expires)
            else:
                cluster.load_all_buckets_till_dgm(self._active_resident_threshold,
                                              self._value_size,
                                              self._expires)

    def __load_star(self):
        hub = self.__cb_clusters[0]
        if self._dgm_run:
            hub.load_all_buckets_till_dgm(self._active_resident_threshold,
                                          self._value_size,
                                          self._expires)
        else:
            hub.load_all_buckets(self._num_items, self._value_size, self._expires)

    def __async_load_star(self):
        hub = self.__cb_clusters[0]
        if self._dgm_run:
            hub.load_all_buckets_till_dgm(self._active_resident_threshold,
                                          self._value_size,
                                          self._expires)
        else:
            return hub.async_load_all_buckets(self._num_items, self._value_size, self._expires)

    def __load_ring(self):
        self.__load_chain()

    def __async_load_ring(self):
        self.__async_load_chain()

    def load_data_topology(self):
        """load data as per ctopology test parameter
        """
        if self.__topology == TOPOLOGY.CHAIN:
            self.__load_chain()
        elif self.__topology == TOPOLOGY.STAR:
            self.__load_star()
        elif self.__topology == TOPOLOGY.RING:
            self.__load_ring()
        elif self._input.param(TOPOLOGY.HYBRID, 0):
            self.__load_star()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    def async_load_data_topology(self):
        """load data as per ctopology test parameter
        """
        if self.__topology == TOPOLOGY.CHAIN:
            return self.__async_load_chain()
        elif self.__topology == TOPOLOGY.STAR:
            return self.__async_load_star()
        elif self.__topology == TOPOLOGY.RING:
            return self.__async_load_ring()
        elif self._input.param(TOPOLOGY.HYBRID, 0):
            return self.__async_load_star()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    def perform_update_delete(self):
        # UPDATES
        for doc_ops_cluster in self._upd_clusters:
            cb_cluster = self.get_cb_cluster_by_name(doc_ops_cluster)
            self.log.info("Updating keys @ {0}".format(cb_cluster.get_name()))
            cb_cluster.update_delete_data(
                OPS.UPDATE,
                perc=self._perc_upd,
                expiration=self._expires,
                wait_for_expiration=self._wait_for_expiration)

        # DELETES
        for doc_ops_cluster in self._del_clusters:
            cb_cluster = self.get_cb_cluster_by_name(doc_ops_cluster)
            self.log.info("Deleting keys @ {0}".format(cb_cluster.get_name()))
            cb_cluster.update_delete_data(OPS.DELETE, perc=self._perc_del)

    def async_perform_update_delete(self):
        tasks = []
        # UPDATES
        for doc_ops_cluster in self._upd_clusters:
            cb_cluster = self.get_cb_cluster_by_name(doc_ops_cluster)
            self.log.info("Updating keys @ {0}".format(cb_cluster.get_name()))
            tasks.extend(cb_cluster.async_update_delete(
                OPS.UPDATE,
                perc=self._perc_upd,
                expiration=self._expires,
                num_items=self._num_items))

        [task.result() for task in tasks]
        if tasks:
            self.log.info("Batched updates loaded to cluster(s)")

        tasks = []
        # DELETES
        for doc_ops_cluster in self._del_clusters:
            cb_cluster = self.get_cb_cluster_by_name(doc_ops_cluster)
            self.log.info("Deleting keys @ {0}".format(cb_cluster.get_name()))
            tasks.extend(
                cb_cluster.async_update_delete(
                    OPS.DELETE,
                    perc=self._perc_del,
                    expiration=self._expires,
                    num_items=self._num_items))

        [task.result() for task in tasks]
        if tasks:
            self.log.info("Batched deletes sent to cluster(s)")

        if self._wait_for_expiration and self._expires:
            self.wait_interval(
                self._expires,
                "Waiting for expiration of updated items")

    def setup_all_replications(self):
        """Setup replication between buckets on remote clusters
        based on the xdcr topology created.
        """
        for cb_cluster in self.__cb_clusters:
            for remote_cluster in cb_cluster.get_remote_clusters():
                for src_bucket in remote_cluster.get_src_cluster().get_buckets():
                    remote_cluster.create_replication(
                        src_bucket,
                        rep_type=self.__rep_type,
                        toBucket=remote_cluster.get_dest_cluster().get_bucket_by_name(
                            src_bucket.name))
                remote_cluster.start_all_replications()

    def _resetup_replication_for_recreate_buckets(self, cluster_name):
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                if remote_cluster_ref.get_src_cluster().get_name(
                ) != cluster_name and remote_cluster_ref.get_dest_cluster().get_name() != cluster_name:
                    continue
                remote_cluster_ref.clear_all_replications()
                for src_bucket in remote_cluster_ref.get_src_cluster().get_buckets():
                    remote_cluster_ref.create_replication(
                        src_bucket,
                        rep_type=self.__rep_type,
                        toBucket=remote_cluster_ref.get_dest_cluster().get_bucket_by_name(
                            src_bucket.name))

    def setup_xdcr(self):
        if self._enable_cross_cluster_versioning != None or self._version_pruning_window_hrs != None:
            for cb_cluster in self.__cb_clusters:
                rest1 = RestConnection(cb_cluster.get_master_node())
                for bucket in cb_cluster.get_buckets():
                    self.log.info("Mobile settings: Enable cross cluster versioning: {0}, Version pruning window hours: {1}".format(self._enable_cross_cluster_versioning, self._version_pruning_window_hrs))
                    if self._enable_cross_cluster_versioning != None:
                        test = rest1.change_bucket_props(bucket, enableCrossClusterVersioning=str(self._enable_cross_cluster_versioning).lower())
                        self.log.info("{0}".format(test))
                    if self._version_pruning_window_hrs != None and self._version_pruning_window_hrs >= 24:
                        test = rest1.change_bucket_props(bucket, versionPruningWindowHrs=self._version_pruning_window_hrs)
                        self.log.info("{0}".format(test))
        self.set_xdcr_topology()
        # Adding for MB-41318
        time.sleep(10)
        self.setup_all_replications()

        self._apply_throttle_params()

        if self._checkpoint_interval != 1800:
            for cluster in self.__cb_clusters:
                cluster.set_global_checkpt_interval(self._checkpoint_interval)

    def _apply_throttle_params(self):
        """MB-68340: apply DCP flow control throttle / component events chan
        length globally if specified in conf. No-op if neither is set."""
        throttle_params = {}
        dcp_throttle = self._input.param(REPL_PARAM.DCP_FLOW_CONTROL_THROTTLE, None)
        events_chan = self._input.param(REPL_PARAM.COMPONENT_EVENTS_CHAN_LENGTH, None)
        if dcp_throttle is not None:
            throttle_params[REPL_PARAM.DCP_FLOW_CONTROL_THROTTLE] = dcp_throttle
        if events_chan is not None:
            throttle_params[REPL_PARAM.COMPONENT_EVENTS_CHAN_LENGTH] = events_chan
        if not throttle_params:
            return
        for cluster in self.get_cb_clusters():
            rest = RestConnection(cluster.get_master_node())
            self.log.info("Applying throttle params on {}: {}".format(
                cluster.get_name(), throttle_params))
            rest.set_global_xdcr_params(throttle_params)

    def setup_xdcr_and_load(self):
        self.setup_xdcr()
        self.load_data_topology()

    def setup_xdcr_async_load(self):
        self.setup_xdcr()
        return self.async_load_data_topology()

    def load_and_setup_xdcr(self):
        """Initial xdcr
        first load then create xdcr
        """
        self.load_data_topology()
        self.setup_xdcr()

    def set_xdcr_global_param(self, param, value):
        """Set a global XDCR parameter and optionally verify.

        Args:
            param: REPL_PARAM constant
            value: Value to set
            verify: Whether to read back and verify the value
        """
        for cb_cluster in self.__cb_clusters:
            master_node = cb_cluster.get_master_node()
            rest = RestConnection(master_node)

            try:
                self.log.info("Setting global {} = {} on cluster {}".format(
                    param, value, cb_cluster.get_name()))
                rest.set_global_xdcr_param(param, value)

            except Exception as e:
                error_msg = "Failed to set global {} on {}: {}".format(
                    param, cb_cluster.get_name(), str(e))
                self.log.error(error_msg)
                self.fail(error_msg)

    def set_xdcr_per_replication_param(self, replication, param, value, verify=True):
        """Set a per-replication XDCR parameter and optionally verify.

        Args:
            replication: XDCReplication object
            param: REPL_PARAM constant
            value: Value to set
            verify: Whether to read back and verify the value
        """
        src_bucket = replication.get_src_bucket()
        dest_bucket = replication.get_dest_bucket()

        self.log.info("Setting per-replication {} = {} for {} -> {}".format(
            param, value, src_bucket.name, dest_bucket.name))
        replication.set_xdcr_param(param, value, verify_event=False)

        if verify:
            readback_value = replication.get_xdcr_setting(param)
            if readback_value != value:
                self.fail("Per-replication setting verification failed for {} -> {}: {} expected={}, actual={}".format(
                    src_bucket.name, dest_bucket.name, param, value, readback_value))
            self.log.info("Verified per-replication {} = {} for {} -> {}".format(
                param, value, src_bucket.name, dest_bucket.name))

    def update_xdcr_param_all_replications(self, param, value, scope="both", verify=True):
        """Update an XDCR parameter for all running replications.

        Args:
            param: REPL_PARAM constant
            value: Value to set
            scope: Where to apply - "global", "per_repl", or "both" (default)
            verify: Whether to verify the setting after applying
        """
        self.log.info("Updating XDCR parameter {} = {} (scope={})".format(param, value, scope))

        # Get all replications
        all_replications = []
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                all_replications.extend(remote_cluster_ref.get_replications())

        # Apply based on scope
        if scope in ("global", "both"):
            self.set_xdcr_global_param(param, value)

        if scope in ("per_repl", "both"):
            for repl in all_replications:
                self.set_xdcr_per_replication_param(repl, param, value, verify=verify)

        self.log.info("Successfully updated XDCR parameter {} = {}".format(param, value))

    def verify_rev_ids(self, xdcr_replications, kv_store=1, timeout=1200, skip=[]):
        """Verify RevId (sequence number, cas, flags value) for each item on
        every source and destination bucket.
        @param xdcr_replications: list of XDCRReplication objects.
        @param kv_store: Index of bucket kv_store to compare.
        """
        error_count = 0
        tasks = []
        for repl in xdcr_replications:
            if repl.get_src_bucket().name not in skip and repl.get_dest_bucket().name not in skip:
                self.log.info("Verifying RevIds for {0} -> {1}, bucket {2}".format(
                        repl.get_src_cluster(),
                        repl.get_dest_cluster(),
                        repl.get_src_bucket().name))
                task_info = self.__cluster_op.async_verify_revid(
                        repl.get_src_cluster().get_master_node(),
                        repl.get_dest_cluster().get_master_node(),
                        repl.get_src_bucket(),
                        repl.get_src_bucket().kvs[kv_store],
                        repl.get_dest_bucket().kvs[kv_store],
                        max_verify=self._max_verify,
                        compression=self._sdk_compression)
                tasks.append(task_info)
        for task in tasks:
            if self._dgm_run:
                task.result()
            else:
                task.result(timeout)
            error_count += task.err_count
            if task.err_count:
                for ip, values in task.keys_not_found.items():
                    if values:
                        self.log.error("%s keys not found on %s, "
                                       "printing first 100 keys: %s" % (len(values),
                                                                        ip, values[:100]))
        return error_count

    def __merge_keys(
            self, kv_src_bucket, kv_dest_bucket, kvs_num=1, filter_exp=None):
        """ Will merge kv_src_bucket keys that match the filter_expression
            if any into kv_dest_bucket.
        """
        valid_keys_src, deleted_keys_src = kv_src_bucket[
            kvs_num].key_set()
        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()

        self.log.info("src_kvstore has %s valid and %s deleted keys"
                      % (len(valid_keys_src), len(deleted_keys_src)))
        self.log.info("dest kvstore has %s valid and %s deleted keys"
                      % (len(valid_keys_dest), len(deleted_keys_dest)))

        if filter_exp:
            # If key based adv filter
            if "META().id" in filter_exp:
                filter_exp = filter_exp.split('\'')[1]

            filtered_src_keys = [key for key in valid_keys_src if re.search(str(filter_exp), key) is not None]
            valid_keys_src = filtered_src_keys
            self.log.info(
                "{0} keys matched the filter expression {1}".format(
                    len(valid_keys_src),
                    filter_exp))

        for key in valid_keys_src:
            # replace/add the values for each key in src kvs
            if key not in deleted_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
                # In case of lww, if source's key timestamp is lower than
                # destination than no need to set.
                if self.__lww and partition1.get_timestamp(
                        key) < partition2.get_timestamp(key):
                    continue
                key_add = partition1.get_key(key)
                partition2.set(
                    key,
                    key_add["value"],
                    key_add["expires"],
                    key_add["flag"])
                kv_src_bucket[kvs_num].release_partition(key)
                kv_dest_bucket[kvs_num].release_partition(key)

        for key in deleted_keys_src:
            if key not in deleted_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
                # In case of lww, if source's key timestamp is lower than
                # destination than no need to delete.
                if self.__lww and partition1.get_timestamp(
                        key) < partition2.get_timestamp(key):
                    continue
                partition2.delete(key)
                kv_src_bucket[kvs_num].release_partition(key)
                kv_dest_bucket[kvs_num].release_partition(key)

        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()
        self.log.info("After merging: destination bucket's kv_store now has {0}"
                      " valid keys and {1} deleted keys".
                      format(len(valid_keys_dest), len(deleted_keys_dest)))

    def __merge_all_buckets(self):
        """Merge bucket data between source and destination bucket
        for data verification. This method should be called after replication started.
        """
        # TODO need to be tested for Hybrid Topology
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                for repl in remote_cluster_ref.get_replications():
                    self.log.info("Merging keys for replication {0}".format(repl))
                    self.__merge_keys(
                        repl.get_src_bucket().kvs,
                        repl.get_dest_bucket().kvs,
                        kvs_num=1,
                        filter_exp=repl.get_filter_exp())

    # Interface for other tests.
    def merge_all_buckets(self):
        self.__merge_all_buckets()

    def print_panic_stacktrace(self, node):
        """ Prints panic stacktrace from goxdcr.log*
        """
        shell = RemoteMachineShellConnection(node)
        result, err = shell.execute_command("zgrep -A 40 'panic:' {0}/goxdcr.log*".
                            format(NodeHelper.get_goxdcr_log_dir(node)))
        for line in result:
            self.log.info(line)
        shell.disconnect()

    def check_errors_in_goxdcr_logs(self):
        """
        checks if new errors from self.__report_error_list
        were found on any of the goxdcr.logs
        """
        error_found_logger = []
        if not self.__is_cluster_run():
            goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'
        for node in self._input.servers:
            if self.__is_cluster_run():
                goxdcr_log = NodeHelper.get_goxdcr_log_dir(node)\
                     + '/goxdcr.log*'
            for error in self.__report_error_list:
                _, new_error_count = NodeHelper.check_goxdcr_log(node,
                                                              error,
                                                              goxdcr_log)
                self.log.info("Initial {0} count on {1} :{2}, now :{3}".
                            format(error,
                                node.ip,
                                self.__error_count_dict[node.ip][error],
                                new_error_count))
                if (new_error_count  > self.__error_count_dict[node.ip][error]):
                    error_found_logger.append("{0} found on {1}".format(error,
                                                                    node.ip))
                    if "panic" in error:
                        self.print_panic_stacktrace(node)
        if error_found_logger:
            self.log.error(error_found_logger)
        return error_found_logger

    def check_replication_restarted(self):
        """
            Checks if replication restarted
        """
        repl_restart_fail = self._input.param("fail_repl_restart", False)
        restarted = False
        if not self.__is_cluster_run():
            goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'
        for node in self._input.servers:
            if self.__is_cluster_run():
                goxdcr_log = NodeHelper.get_goxdcr_log_dir(node)\
                     + '/goxdcr.log*'
            reasons, new_repl_res_count = NodeHelper.check_goxdcr_log(node,
                                                          "Try to fix Pipeline",
                                                          goxdcr_log=goxdcr_log,
                                                          print_matches=True)
            self.log.info("Initial replication restart count on {0} :{1}, now :{2}".
                        format(node.ip,
                            self._repl_restart_count_dict[node.ip],
                            new_repl_res_count))
            if (new_repl_res_count  > self._repl_restart_count_dict[node.ip]):
                new_count = new_repl_res_count - \
                           self._repl_restart_count_dict[node.ip]
                restarted = True
                self.log.info("Number of new replication restarts this run: %s"
                    % new_count)
                for reason in reasons[-new_count:]:
                    self.log.info(reason)
        if repl_restart_fail and restarted:
            self.fail("Replication restarted on one of the nodes, scroll above"
                      "for reason")

    def get_collection_info(self, bucket, master):
        print("Getting collection info for bucket: ", bucket)
        print("Master: ", master)
        shell = RemoteMachineShellConnection(master)
        nameOutput, error = shell.execute_cbstats(bucket, "collections",
                                                   cbadmin_user="Administrator",
                                                   options=" | grep ':name'")
        itemsOutput, error = shell.execute_cbstats(bucket, "collections",
                                                   cbadmin_user="Administrator",
                                                   options=" | grep ':items'")
        shell.disconnect()
        # collection_names = []
        collection_info = {}
        if nameOutput and itemsOutput:
            for x, y in zip(nameOutput, itemsOutput):
                collection_info[x.split(":name:")[1].strip()] = int(y.split(":items:")[1].strip())
                # collection_names.append(x.split(":name:")[1].strip())
        return collection_info, error

    def get_incoming_replications(self, master):
        incoming_repl_uri = "xdcr/sourceClusters"
        status, content, _ = master._http_request(api = master.baseUrl + incoming_repl_uri, method="GET", timeout=60)
        if status:
            return json.loads(content)
        return None

    def get_outgoing_replications(self, master):
        outgoing_repl_uri = "pools/default/remoteClusters"
        auth_user, auth_pass = master.username, master.password
        auth = "Basic " + base64.b64encode(f"{auth_user}:{auth_pass}".encode('utf-8')).decode('utf-8')
        status, content, _ = master._http_request(api = master.baseUrl + outgoing_repl_uri, method="GET", timeout=60)
        if status:
            return json.loads(content)
        return None

    def load_docs_with_pillowfight(self, server, items, bucket, batch=1000, docsize=100, rate_limit=100000, scope="_default", collection="_default", command_timeout=10):
        server_shell = RemoteMachineShellConnection(server)
        cmd = f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password -U couchbase://localhost/"\
            f"{bucket} -I {items} -m {docsize} -M {docsize} -B {batch} --rate-limit={rate_limit} --populate-only --collection {scope}.{collection}"
        self.log.info("Executing '{0}'...".format(cmd))
        output, error  = server_shell.execute_command(cmd, timeout=command_timeout, use_channel=True)
        if output:
            self.log.info(f"Output: {output}")
        if error:
            self.fail(f"Failed to load docs in cluster in {bucket}.{scope}.{collection}")
        server_shell.disconnect()
        self.log.info(f"Data loaded into {bucket}.{scope}.{collection} successfully")

    @staticmethod
    def select_collections(server, bucket_name, mode="sample",
                           sample_size=200, explicit_pairs=None, seed=None):
        """
        Select (scope, collection) pairs from a bucket manifest.

        Args:
            server: Server object
            bucket_name: Bucket name
            mode: "sample" | "all" | "explicit"
            sample_size: Number of collections to sample (mode="sample")
            explicit_pairs: List of (scope, collection) tuples (mode="explicit")
            seed: Random seed for reproducible sampling

        Returns:
            list of (scope_name, collection_name) tuples
        """
        if mode == "explicit":
            if not explicit_pairs:
                raise ValueError("explicit_pairs required when mode='explicit'")
            return list(explicit_pairs)

        manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            server, bucket_name
        )

        all_pairs = []
        for scope in manifest.get("scopes", []):
            scope_name = scope["name"]
            if scope_name.startswith("_"):
                continue
            for col in scope.get("collections", []):
                all_pairs.append((scope_name, col["name"]))

        if mode == "all":
            return all_pairs

        if seed is not None:
            rng = random.Random(seed)
        else:
            rng = random.Random()

        if len(all_pairs) <= sample_size:
            return all_pairs
        return rng.sample(all_pairs, sample_size)

    @staticmethod
    def load_collections_with_sdk(server, bucket, pairs=None,
                                  docs_per_collection=5,
                                  doc_size=100,
                                  docid_prefix="xdcr10k",
                                  run_id=None,
                                  json_template="Person",
                                  all_collections=False,
                                  parallel_loaders=8,
                                  timeout=900):
        """
        SDK-based parallel collection loader. Replaces load_with_pillowfight_parallel.

        Spawns the Java SDK doc-loader jar (java_sdk_client/collections/...) directly
        as subprocesses. The shared TaskManager scheduler is bypassed because
        SDKLoadDocumentsTask runs serially (single-threaded queue + 30s ES-indexing
        sleep per task) — for thousands of collections that adds hours of wall time.

        Modes:
          all_collections=True  -> ONE Java invocation with -ac True (the jar
                                   iterates all non-system collections internally).
                                   `pairs` is ignored; LoadResult.success_pairs is
                                   populated by enumerating the bucket manifest.
          all_collections=False -> One Java subprocess per (scope, collection)
                                   in `pairs`, run concurrently in a
                                   ThreadPoolExecutor of size `parallel_loaders`.

        Args:
            server: Server object on the source cluster.
            bucket: Bucket name (string).
            pairs: List of (scope, collection) tuples. Required unless
                   all_collections=True.
            docs_per_collection: Number of docs to create per collection.
            doc_size: Document size in bytes.
            docid_prefix: Prefix for doc-key uniqueness.
            run_id: Unique run identifier (auto-generated if None).
            json_template: Java SDK json template (e.g. "Person").
            all_collections: If True, take the bulk fast-path (single Java process).
            parallel_loaders: Per-pair mode only — concurrent Java subprocesses.
                              Cap reflects JVM startup memory; default 8.
            timeout: Per-subprocess timeout in seconds.

        Returns:
            LoadResult with success/failed pairs and stats.
        """
        log = logger.Logger.get_logger()
        result = LoadResult()
        if run_id is None:
            run_id = uuid.uuid4().hex[:8]
        start_time = time.time()

        def _build_cmd(scope, collection, key_prefix, ac=False):
            return (
                "java -jar java_sdk_client/collections/target/javaclient/javaclient.jar "
                "-i {ip} -u '{u}' -p '{pw}' -b {bucket} "
                "-s {scope} -c {col} "
                "-n {n} -pc 100 -pu 0 -pd 0 -l uniform -dsn 0 -dpx {kp} -dt {tpl} "
                "-de 0 -ds {ds} -ac {ac} -st 0 -en {en} -o False -sd False "
                "--secure False -cpl False"
            ).format(
                ip=server.ip, u=server.rest_username, pw=server.rest_password,
                bucket=bucket, scope=scope, col=collection,
                n=docs_per_collection, kp=key_prefix, tpl=json_template,
                ds=doc_size, ac=ac, en=max(0, docs_per_collection - 1),
            )

        def _run_one(scope, collection, key_prefix, ac=False):
            cmd = _build_cmd(scope, collection, key_prefix, ac=ac)
            try:
                proc = subprocess.run(
                    cmd, shell=True, capture_output=True, timeout=timeout)
                if proc.returncode != 0:
                    return False, "exit={} stderr={}".format(
                        proc.returncode,
                        proc.stderr.decode("utf-8", "replace")[:200])
                return True, None
            except Exception as e:
                return False, str(e)

        if all_collections:
            log.info("SDK bulk load: all collections in {}, {} docs each, "
                     "run_id={}".format(bucket, docs_per_collection, run_id))
            ok, err = _run_one(
                "_default", "_default",
                "{}_{}_".format(docid_prefix, run_id), ac=True)
            manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                server, bucket)
            for scope in manifest.get("scopes", []):
                if scope["name"].startswith("_"):
                    continue
                for col in scope.get("collections", []):
                    if col["name"].startswith("_"):
                        continue
                    if ok:
                        result.success_pairs.append((scope["name"], col["name"]))
                        result.total_docs_loaded += docs_per_collection
                    else:
                        result.failed_pairs.append((scope["name"], col["name"]))
                        result.errors[(scope["name"], col["name"])] = err
            if not ok:
                log.error("Bulk all_collections load failed: {}".format(err))
        else:
            if not pairs:
                log.warning("load_collections_with_sdk called with no pairs "
                            "and all_collections=False; nothing to do")
                return result
            log.info("SDK per-pair load: {} collections, {} docs each, "
                     "parallel_loaders={}, run_id={}".format(
                         len(pairs), docs_per_collection, parallel_loaders, run_id))
            with ThreadPoolExecutor(max_workers=parallel_loaders) as executor:
                futures = {}
                for scope_name, col_name in pairs:
                    kp = "{}_{}_{}_{}_".format(
                        docid_prefix, run_id, scope_name, col_name)
                    fut = executor.submit(_run_one, scope_name, col_name, kp, False)
                    futures[fut] = (scope_name, col_name)
                for fut in as_completed(futures):
                    scope_name, col_name = futures[fut]
                    try:
                        ok, err = fut.result()
                    except Exception as e:
                        ok, err = False, str(e)
                    if ok:
                        result.success_pairs.append((scope_name, col_name))
                        result.total_docs_loaded += docs_per_collection
                    else:
                        log.error("SDK load failed for {}.{}: {}".format(
                            scope_name, col_name, err))
                        result.failed_pairs.append((scope_name, col_name))
                        result.errors[(scope_name, col_name)] = err

        result.elapsed_seconds = time.time() - start_time
        log.info("SDK load done: {}/{} pairs ok, {} docs in {:.1f}s".format(
            len(result.success_pairs), result.total_attempted,
            result.total_docs_loaded, result.elapsed_seconds))
        if result.failed_pairs:
            log.warning("Failed pairs (first 10): {}".format(
                result.failed_pairs[:10]))
        return result

    def insert_docs_with_xattr(self, server, bucket_name, num_docs, num_xattrs, xattr_key_values={}):

        """Uses docker image to insert xattrs """
        """	Define command-line flags
            clusterIP := flag.String("ip", "192.168.65.3", "Couchbase cluster IP address")
            username := flag.String("username", "Administrator", "Couchbase username")
            password := flag.String("password", "password", "Couchbase password")
            bucketName := flag.String("bucket", "default", "Bucket name")
            numDocs := flag.Int("num-docs", 50, "Number of documents to insert")
            numXattrs := flag.Int("num-xattrs", 1, "Number of xattrs to set for each document")
            xattrKVStr := flag.String("xattrs", "", "Comma-separated key-value pairs for xattrs (e.g., key1=val1,key2=val2)")
        """
        import subprocess
        # Pull the xattr_modify docker image
        pull_cmd = ["docker", "pull", "couchbaseqe/xattr_modify"]
        try:
            subprocess.run(pull_cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            self.fail(f"Failed to pull docker image: {e.stderr}")
        # Run the container with specified parameters
        run_cmd = [
            "docker", "run", "couchbaseqe/xattr_modify",
            "go", "run", "main.go",
            "-ip", server.ip,
            "-username", "Administrator",
            "-password", "password",
            "-bucket", bucket_name,
            "-num-docs", str(num_docs),
            "-num-xattrs", str(num_xattrs),
        ]
        # Build xattr key-value string
        if xattr_key_values:
            xattr_str = ""
            xattr_str = ",".join([f"{k}={v}" for k,v in xattr_key_values.items()])
            run_cmd.extend(["-xattrs", xattr_str])
        try:
            result = subprocess.run(run_cmd, check=True, capture_output=True, text=True)
            self.log.info(result.stdout)
        except subprocess.CalledProcessError as e:
            self.fail(f"Failed to run xattr_modify container: {e.stderr}")
        self.log.info("Successfully added xattrs to documents")

    def _if_docs_count_match_on_servers(self) -> bool:
        src_cluster = self.__cb_clusters[0]
        src_master = src_cluster.get_master_node()
        src_rest = RestConnection(src_master)
        filterexp_map = {}
        docs_match_map = {}
        replications = src_rest.get_replications()
        for repl in replications:
            bucket = repl['source']
            if repl['filterExpression']:
                exp_in_brackets = '( ' + str(repl['filterExpression']) + ' )'
                if bucket in filterexp_map.keys():
                    filterexp_map[bucket].add(exp_in_brackets)
                else:
                    filterexp_map[bucket] = {exp_in_brackets}
        for remote_cluster in src_cluster.get_remote_clusters():
            dest_cluster = remote_cluster.get_dest_cluster()
            dest_master = dest_cluster.get_master_node()
            for bucket in src_cluster.get_buckets():
                exp = set()
                src_count = 0
                dest_count = -1  # Different values so they don't accidently satisfy equality
                if bucket.name in filterexp_map.keys():  # check if filter exists for the bucket
                    exp = filterexp_map[bucket.name]
                    if len(exp) > 1:
                        exp = " AND ".join(exp)
                    else:
                        exp = next(iter(exp))
                try:
                    if len(exp) != 0:
                        res = RestConnection(src_master).query_tool("SELECT COUNT(*) FROM "
                                                     + bucket.name +
                                                     " WHERE " + exp, timeout=30)
                    else:
                        res = RestConnection(src_master).query_tool("SELECT COUNT(*) FROM "
                                                     + bucket.name, timeout=30)
                    src_count = res["results"][0]['$1']
                except Exception as e:
                    print("Exception while querying number of docs in source: ", str(e))
                try:
                    if len(exp) != 0:
                        res = RestConnection(dest_master).query_tool("SELECT COUNT(*) FROM "
                                                     + bucket.name +
                                                     " WHERE " + exp, timeout=30)
                    else:
                        res = RestConnection(dest_master).query_tool("SELECT COUNT(*) FROM "
                                                     + bucket.name, timeout=30)
                    dest_count = res["results"][0]['$1']
                except Exception as e:
                    print("Exception while querying number of docs in target: ", str(e))
                if src_count == dest_count:
                    print(f"DOCS COUNT MATCHED SRC:{src_count}, TARGET:{dest_count}")
                    docs_match_map[bucket.name] = True
                else:
                    print(f"DOCS COUNT DID NOT MATCH SRC:{src_count}, TARGET:{dest_count}")
                    docs_match_map[bucket.name] = False
        return all(docs_match_map.values())

    def _wait_for_xdcr_pipelines_ready(self, timeout=180, poll_interval=5):
        """
        Poll until every replication on every source cluster reports
        status=='running' AND xdc_queue_size is queryable for every bucket.

        Used after disruptive ops (failover, goxdcr restart, node remove)
        to replace fixed sleeps. Does NOT wait for outbound queue to drain
        (that is _wait_for_replication_to_catchup's job).

        Returns True if pipelines became ready before timeout, False otherwise.
        Logs a warning on timeout but does NOT raise — caller decides.
        """
        end_time = time.time() + timeout
        start = time.time()
        while time.time() < end_time:
            all_ready = True
            for cb_cluster in self.__cb_clusters:
                src_master = cb_cluster.get_master_node()
                try:
                    rest = RestConnection(src_master)
                    replications = rest.get_replications()
                except Exception as e:
                    self.log.info("Pipeline-ready poll: REST not responsive on %s yet: %s"
                                  % (src_master.ip, e))
                    all_ready = False
                    break

                if not replications:
                    continue

                for repl in replications:
                    status = repl.get("status") if isinstance(repl, dict) else getattr(repl, "status", None)
                    if status != "running":
                        self.log.info("Pipeline-ready poll: replication on %s status=%s"
                                      % (src_master.ip, status))
                        all_ready = False
                        break
                if not all_ready:
                    break

                for bucket in cb_cluster.get_buckets():
                    try:
                        rest.get_xdc_queue_size(bucket.name)
                    except Exception as e:
                        self.log.info("Pipeline-ready poll: xdc_queue_size not yet exposed for "
                                      "%s on %s: %s" % (bucket.name, src_master.ip, e))
                        all_ready = False
                        break
                if not all_ready:
                    break

            if all_ready:
                self.log.info("XDCR pipelines reported ready after %.1fs"
                              % (time.time() - start))
                return True
            time.sleep(poll_interval)

        self.log.warning("XDCR pipelines not confirmed ready within %ds; "
                         "proceeding anyway" % timeout)
        return False

    @staticmethod
    def _wait_for_collection_manifest_sync(src_server, dest_server, bucket_name,
                                           timeout=180, poll_interval=5):
        """
        Poll until destination bucket's manifest contains every (scope, collection)
        pair from source (excluding system scopes whose names start with '_').

        Used before _wait_for_replication_to_catchup in tests where a fresh
        manifest may not have synced yet — replaces fixed pre-replication sleeps.

        Returns True on sync, False on timeout. Caller decides whether to fail.
        """
        log = logger.Logger.get_logger()
        end_time = time.time() + timeout
        last_missing = -1
        missing = set()
        src_pairs = set()
        while time.time() < end_time:
            try:
                src_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                    src_server, bucket_name)
                dest_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                    dest_server, bucket_name)
            except Exception as e:
                log.info("Manifest-sync poll: fetch failed (%s); retrying" % e)
                time.sleep(poll_interval)
                continue

            src_pairs = set()
            for s in src_manifest.get("scopes", []):
                if s["name"].startswith("_"):
                    continue
                for c in s.get("collections", []):
                    src_pairs.add((s["name"], c["name"]))

            dest_pairs = set()
            for s in dest_manifest.get("scopes", []):
                if s["name"].startswith("_"):
                    continue
                for c in s.get("collections", []):
                    dest_pairs.add((s["name"], c["name"]))

            missing = src_pairs - dest_pairs
            if not missing:
                log.info("Manifest sync confirmed: %d collections present on dest"
                         % len(src_pairs))
                return True
            if len(missing) != last_missing:
                log.info("Manifest sync poll: %d collections still missing on dest "
                         "(of %d on source)" % (len(missing), len(src_pairs)))
                last_missing = len(missing)
            time.sleep(poll_interval)

        log.warning("Manifest sync did not converge within %ds; "
                    "%d collections still missing on dest" % (timeout, len(missing)))
        return False

    def _wait_for_replication_to_catchup(self, timeout=1200, fetch_bucket_stats_by="minute", exclude_paths=[]):

        _count1 = _count2 = 0
        for cb_cluster in self.__cb_clusters:
            cb_cluster.run_expiry_pager()

        # 5 minutes by default
        end_time = time.time() + timeout

        for cb_cluster in self.__cb_clusters:
            rest1 = RestConnection(cb_cluster.get_master_node())
            for remote_cluster in cb_cluster.get_remote_clusters():
                rest2 = RestConnection(remote_cluster.get_dest_cluster().get_master_node())
                for bucket in cb_cluster.get_buckets():
                    while time.time() < end_time :
                        try:
                            _count1 = rest1.fetch_bucket_stats(bucket=bucket.name, zoom=fetch_bucket_stats_by)["op"]["samples"]["curr_items"][-1]
                            _count2 = rest2.fetch_bucket_stats(bucket=bucket.name, zoom=fetch_bucket_stats_by)["op"]["samples"]["curr_items"][-1]
                        except Exception as e:
                            self.log.warning(e)
                            self.log.info("Trying other method to fetch bucket current items")
                            bucket_info1 = rest1.get_bucket_json(bucket.name)
                            nodes = bucket_info1["nodes"]
                            _count1 = 0
                            for node in nodes:
                                _count1 += node["interestingStats"]["curr_items"]

                            bucket_info2 = rest2.get_bucket_json(bucket.name)
                            nodes = bucket_info2["nodes"]
                            _count2 = 0
                            for node in nodes:
                                _count2 += node["interestingStats"]["curr_items"]

                        self.wait_interval(60, "Bucket: {0}, count in one cluster : {1} items, another : {2}. "
                                       "Waiting for replication to catch up ..".
                                   format(bucket.name, _count1, _count2))
                        if _count1 == _count2:
                            self.log.info("Replication caught up for bucket {0}: {1}".format(bucket.name, _count1))
                            break
                    else:
                        # SDKLoader loads docs in all collections including _system._query
                        # and _system._mobile, which are NOT replicated. Subtract them from
                        # both sides so we compare replicated-item totals. Nodes missing
                        # these collections contribute zero.
                        for node in cb_cluster.get_nodes():
                            collections_info = self.get_collection_info(bucket, node)[0]
                            self.log.info(collections_info)
                            _count1 -= collections_info.get("_query", 0)
                            _count1 -= collections_info.get("_mobile", 0)
                        for node in remote_cluster.get_dest_cluster().get_nodes():
                            dest_collections_info = self.get_collection_info(bucket, node)[0]
                            self.log.info(dest_collections_info)
                            _count2 -= dest_collections_info.get("_query", 0)
                            _count2 -= dest_collections_info.get("_mobile", 0)

                        self.log.info(f"Count1: {_count1}, Count2: {_count2}")

                        self.log.info(f"Excluding paths: {exclude_paths}")

                        for path in exclude_paths:
                            self.log.info(f"Excluding {path} from count")
                            if bucket.name in path.split(".")[1]:
                                self.log.info(f"Bucket {bucket.name} in path {path}")
                                docs_to_exclude_src = 0
                                docs_to_exclude_dest = 0
                                cluster_nodes = cb_cluster.get_nodes() if path.split(".")[0] == "C1" else remote_cluster.get_dest_cluster().get_nodes()
                                for node in cluster_nodes:
                                    self.log.info(f"Node {node.ip}")
                                    collections_info = self.get_collection_info(bucket, node)[0]
                                    self.log.info(f"Collections info for bucket {node.ip}: {collections_info}")
                                    if path.split(".")[-1] in collections_info:
                                        print("Path: ", path.split(".")[-1])
                                        print(path)
                                        if path.split(".")[0] == "C1":
                                            docs_to_exclude_src += collections_info[path.split(".")[-1]]
                                            self.log.info(f"Docs to exclude src: {docs_to_exclude_src}")

                                        if path.split(".")[0] == "C2":
                                            docs_to_exclude_dest += collections_info[path.split(".")[-1]]
                                            self.log.info(f"Docs to exclude dest: {docs_to_exclude_dest}")
                                    else:
                                        self.log.info(f"Collection {path.split('.')[-1]} not found in collections info")

                                _count1 -= docs_to_exclude_src
                                self.log.info(f"Count1 after excluding {path}: {_count1}")
                                self.log.info(f"Docs to exclude src: {docs_to_exclude_src}")
                                _count2 -= docs_to_exclude_dest
                                self.log.info(f"Count2 after excluding {path}: {_count2}")
                                self.log.info(f"Docs to exclude dest: {docs_to_exclude_dest}")
                        if _count1 == _count2:
                            self.log.info("Replication caught up for bucket {0}: {1}".format(bucket.name, _count1))
                            break
                        for node in cb_cluster.get_nodes():
                            self.log.info(f"Node {node.ip}")
                            collections_info = self.get_collection_info(bucket, node)[0]
                            self.log.info(f"Collections info for bucket {node.ip}: {collections_info}")
                        self.fail("Not all items replicated in {0} sec for {1} "
                                "bucket. on source cluster:{2}, on dest:{3}".\
                            format(timeout, bucket.name, _count1, _count2))

    def _wait_for_dest_to_stabilize(self, dest_cluster, bucket_name,
                                    min_items=1, stable_window_s=60,
                                    poll_interval_s=30, timeout=900):
        """Poll dest cluster's bucket item count until it has reached
        `min_items` and has not changed for `stable_window_s` seconds.

        Use this for tests where pairwise src == dest is not the right
        invariant (filtered, explicitly-mapped, cross-version, etc.) —
        the caller runs its scenario-specific assertion after this returns.

        Returns the final item count. Raises on timeout.
        """
        end_time = time.time() + timeout
        last_count = -1
        last_change_time = time.time()
        while time.time() < end_time:
            rest = RestConnection(dest_cluster.get_master_node())
            try:
                count = rest.fetch_bucket_stats(
                    bucket=bucket_name, zoom="minute"
                )["op"]["samples"]["curr_items"][-1]
            except Exception:
                bucket_info = rest.get_bucket_json(bucket_name)
                count = sum(node["interestingStats"]["curr_items"]
                            for node in bucket_info["nodes"])
            self.log.info("Dest stabilization poll: bucket={} count={} last={}".format(
                bucket_name, count, last_count))
            if count != last_count:
                last_count = count
                last_change_time = time.time()
            elif count >= min_items and (time.time() - last_change_time) >= stable_window_s:
                self.log.info("Dest bucket {} stable at {} items for {}s".format(
                    bucket_name, count, stable_window_s))
                return count
            time.sleep(poll_interval_s)
        raise Exception("Dest bucket {} did not stabilize within {}s "
                        "(last count: {})".format(bucket_name, timeout, last_count))

    def _wait_for_es_replication_to_catchup(self, timeout=300):
        from membase.api.esrest_client import EsRestConnection

        _count1 = _count2 = 0
        for cb_cluster in self.__cb_clusters:
            cb_cluster.run_expiry_pager()

        # 5 minutes by default
        end_time = time.time() + timeout

        for cb_cluster in self.__cb_clusters:
            rest1 = RestConnection(cb_cluster.get_master_node())
            for remote_cluster in cb_cluster.get_remote_clusters():
                rest2 = EsRestConnection(remote_cluster.get_dest_cluster().get_master_node())
                for bucket in cb_cluster.get_buckets():
                    while time.time() < end_time :
                        _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
                        _count2 = rest2.fetch_bucket_stats(bucket_name=bucket.name).itemCount
                        if _count1 == _count2:
                            self.log.info("Replication caught up for bucket {0}: {1}".format(bucket.name, _count1))
                            break
                        self.wait_interval(60, "Bucket: {0}, count in one cluster : {1} items, another : {2}. "
                                       "Waiting for replication to catch up ..".
                                   format(bucket.name, _count1, _count2))
                    else:
                        self.fail("Not all items replicated in {0} sec for {1} "
                                "bucket. on source cluster:{2}, on dest:{3}".\
                            format(timeout, bucket.name, _count1, _count2))

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    # Sleep for interval seconds between polls, while waiting for event to complete
    def wait_interval(self, timeout=1, message=""):
        self.sleep(timeout, message)

    def __execute_query(self, server, query):
        try:
            res = RestConnection(server).query_tool(query)
            if "COUNT" in query:
                return (int(res["results"][0]['$1']))
            else:
                return 0
        except Exception as e:
            self.fail(
                "Errors encountered while executing query {0} on {1} : {2}".format(query, server, str(e)))

    def _create_index(self, server, bucket, scope=None, collection=None):
        if scope != None and collection != None:
            collection_index = bucket + "_" + scope + "_" + collection + "_index"
            query_check_index_exists = "SELECT COUNT(*) FROM system:indexes " \
                                       "WHERE name='" + collection_index + "'"
            if not self.__execute_query(server, query_check_index_exists):
                self.__execute_query(server, "CREATE PRIMARY INDEX " + collection_index
                                     + " ON " + bucket + '.' + scope + '.' + collection)
        else:
            query_check_index_exists = "SELECT COUNT(*) FROM system:indexes " \
                                   "WHERE name='" + bucket + "_index'"
            if not self.__execute_query(server, query_check_index_exists):
                self.__execute_query(server, "CREATE PRIMARY INDEX " + bucket + "_index "
                                 + "ON " + bucket)

    def _get_doc_count(self, server, bucket, scope=None, collection=None):
        exp = self.filter_exp.get(bucket, set())
        if len(exp) == 0 and scope != None and collection != None:
            return self.__execute_query(server, "SELECT COUNT(*) FROM "
                                        + "default:" + bucket + "."
                                        + scope + "." + collection)
        elif len(exp) == 0 and scope == None and collection == None:
            return self.__execute_query(server, "SELECT COUNT(*) FROM "
                                        + bucket)
        elif len(exp) > 1:
            exp = " AND ".join(exp)
        else:
            exp = next(iter(exp))
        if "DATE" in exp:
            exp = exp.replace("DATE", '')
        if scope != None and collection != None:
            doc_count = self.__execute_query(server, "SELECT COUNT(*) FROM "
                                             + "default:" + bucket + "."
                                             + scope + "." + collection +
                                             " WHERE " + exp)
        else:
            doc_count = self.__execute_query(server, "SELECT COUNT(*) FROM "
                                             + bucket +
                                             " WHERE " + exp)
        if not doc_count:
            return 0
        return doc_count

    def verify_filtered_items(self, src_master, dest_master, replications, skip_index=False):
        # Assuming src and dest bucket of replication have the same name
        for repl in replications:
            # Assuming src and dest bucket of the replication have the same name
            bucket = repl['source']
            # filter_exp = {default:([filter_exp1, filter_exp2])}
            # and query will be "select count from default where filter_exp1 AND filter_exp2"
            if repl['filterExpression']:
                exp_in_brackets = '( ' + str(repl['filterExpression']) + ' )'
                if bucket in self.filter_exp.keys():
                    self.filter_exp[bucket].add(exp_in_brackets)
                else:
                    self.filter_exp[bucket] = {exp_in_brackets}
            if self._use_java_sdk:
                for scope in CollectionsRest(src_master).get_bucket_scopes(bucket):
                    for collection in CollectionsRest(src_master).get_scope_collections(bucket, scope):
                        if not skip_index:
                            self._create_index(src_master, bucket, scope, collection)
                            self._create_index(dest_master, bucket, scope, collection)
                        src_count = self._get_doc_count(src_master, bucket, scope, collection)
                        dest_count = self._get_doc_count(dest_master, bucket, scope, collection)
                        if src_count != dest_count:
                            self.fail("Doc count {0} on {1}:{2}->{3}->{4} does not match "
                                      "doc count {5} on {6}:{2}->{3}->{4} after applying filter {7}"
                                      .format(src_count, src_master.ip, bucket, scope, collection,
                                              dest_count, dest_master.ip, self.filter_exp))
                        self.log.info("Doc count {0} on {1}:{2}->{3}->{4} matches "
                                      "doc count {5} on {6}:{2}->{3}->{4} after applying filter {7}"
                                      .format(src_count, src_master.ip, bucket, scope, collection,
                                              dest_count, dest_master.ip, self.filter_exp))
            else:
                if not skip_index:
                    self._create_index(src_master, bucket)
                    self._create_index(dest_master, bucket)
                src_count = self._get_doc_count(src_master, bucket=bucket)
                dest_count = self._get_doc_count(dest_master, bucket=bucket)
                if src_count != dest_count:
                    self.fail("Doc count {0} on {1} does not match "
                              "doc count {2} on {3} "
                              "after applying filter {4}"
                              .format(src_count, src_master.ip,
                                      dest_count, dest_master.ip,
                                      self.filter_exp))
                self.log.info("Doc count {0} on {1} matches "
                              "doc count {2} on {3} "
                              "after applying filter {4}"
                              .format(src_count, src_master.ip,
                                      dest_count, dest_master.ip,
                                      self.filter_exp))

    def _check_lists_match(self, list1, list2):
        list_diff = [i for i in list1 + list2 if i not in list1 or i not in list2]
        if not list_diff:
            return True
        return False

    def verify_collection_doc_count(self, replications, timeout=1200):
        tasks = []
        mapping = {}
        for repl in replications:
            src_bucket = repl.get_src_bucket()
            filter_exp = repl.get_filter_exp()
            if repl.get_xdcr_setting('collectionsExplicitMapping'):
                mapping = repl.get_xdcr_setting('colMappingRules')
            else:
                src_rest = CollectionsRest(repl.get_src_cluster().get_master_node())
                src_scopes = src_rest.get_bucket_scopes(src_bucket)
                for scope in src_scopes:
                    src_collections = src_rest.get_scope_collections(src_bucket, scope)
                    for collection in src_collections:
                        map_exp = '"' + scope + ':' + collection + '"'
                        mapping[map_exp] = map_exp
            task_info = self.__cluster_op.async_verify_collection_doc_count(
                repl.get_src_cluster(),
                repl.get_dest_cluster(),
                src_bucket, mapping,
                filter_exp=filter_exp)
            tasks.append(task_info)
        for task in tasks:
            task.result(timeout)

    def verify_results(self, skip_verify_data=[], skip_verify_revid=[]):
        """Verify data between each couchbase and remote clusters.
        Run below steps for each source and destination cluster..
            1. Run expiry pager.
            2. Wait for disk queue size to 0 on each nodes.
            3. Wait for Outbound mutations to 0.
            4. Wait for Items counts equal to kv_store size of buckets.
            5. Verify items value on each bucket.
            6. Verify Revision id of each item.
        """
        skip_key_validation = self._input.param("skip_key_validation", False)
        skip_meta_validation = self._input.param("skip_meta_validation", True)
        skip_collection_key_validation = True
        if self._use_java_sdk:
            skip_collection_key_validation = self._input.param("skip_collection_key_validation", False)
            #TODO : add key validation against kv store for collections
            skip_key_validation = True
        src_dcp_queue_drained = False
        dest_dcp_queue_drained = False
        src_active_passed = False
        src_replica_passed = False
        dest_active_passed = False
        dest_replica_passed = False
        src_cluster = None
        dest_cluster = None
        self.__merge_all_buckets()
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                try:
                    src_cluster = remote_cluster_ref.get_src_cluster()
                    dest_cluster = remote_cluster_ref.get_dest_cluster()

                    if self._evict_with_compactor:
                        for b in src_cluster.get_buckets():
                           # only need to do compaction on the source cluster, evictions are propagated to the remote
                           # cluster
                           src_cluster.get_cluster().compact_bucket(src_cluster.get_master_node(), b)
                    else:
                        src_cluster.run_expiry_pager()
                        dest_cluster.run_expiry_pager()

                    src_cluster.wait_for_flusher_empty()
                    dest_cluster.wait_for_flusher_empty()

                    src_dcp_queue_drained = src_cluster.wait_for_dcp_queue_drain()
                    dest_dcp_queue_drained = dest_cluster.wait_for_dcp_queue_drain()

                    src_cluster.wait_for_outbound_mutations()
                    dest_cluster.wait_for_outbound_mutations()
                except Exception as e:
                    # just log any exception thrown, do not fail test
                    self.log.error(e)
                if not skip_key_validation:
                    try:
                        if len(src_cluster.get_nodes()) > 1:
                            src_active_passed, src_replica_passed =\
                                src_cluster.verify_items_count(timeout=self._item_count_timeout)
                        else:
                            self.log.info("Skipped active replica count check as source cluster has 1 node only")
                            src_active_passed = True
                            src_replica_passed = True
                        if len(dest_cluster.get_nodes()) > 1:
                            dest_active_passed, dest_replica_passed = \
                                dest_cluster.verify_items_count(timeout=self._item_count_timeout)
                        else:
                            self.log.info("Skipped active replica count check as dest cluster has 1 node only")
                            dest_active_passed = True
                            dest_replica_passed = True

                        src_cluster.verify_data(max_verify=self._max_verify, skip=skip_verify_data)
                        dest_cluster.verify_data(max_verify=self._max_verify, skip=skip_verify_data)
                    except Exception as e:
                        self.log.error(e)
                    finally:
                        rev_err_count = self.verify_rev_ids(remote_cluster_ref.get_replications(), skip=skip_verify_revid)
                        # we're done with the test, now report specific errors
                        # following errors are important only if there is a rev id mismatch
                        # if revids matched then these errors can be ignored
                        if (not(src_active_passed and dest_active_passed)) and \
                            (not(src_dcp_queue_drained and dest_dcp_queue_drained)) and rev_err_count:
                            self.fail("Incomplete replication: Keys stuck in dcp queue")
                        if not (src_active_passed and dest_active_passed) and rev_err_count:
                            self.fail("Incomplete replication: Active key count is incorrect")
                        if not (src_replica_passed and dest_replica_passed) and rev_err_count:
                            self.fail("Incomplete intra-cluster replication: "
                                      "replica count did not match active count")
                        if rev_err_count > 0:
                            self.fail("RevID verification failed for remote-cluster: {0}".
                                format(remote_cluster_ref))

                if not skip_meta_validation:
                    src_cluster.verify_meta_data()
                    dest_cluster.verify_meta_data()

                if not skip_collection_key_validation:
                    self.verify_collection_doc_count(remote_cluster_ref.get_replications())

        self.check_replication_restarted()
        # treat errors in self.__report_error_list as failures
        if len(self.__report_error_list) > 0:
            error_logger = self.check_errors_in_goxdcr_logs()
            if error_logger:
                self.fail("Errors found in logs : {0}".format(error_logger))

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
                self.wait_interval(10, "Waiting for couchbase service to be running. {0}".format(output))
        shell.disconnect()
        self.fail("Couchbase service is not running after {0} seconds".format(wait_time))

    def init_xdcr_differ(self, src_cluster, src_master, src_master_shell,
                         bin_path="/opt/couchbase/bin/xdcrDiffer",
                         yaml_conf_path="/tmp/xdcr_differ_params.yaml",
                         output_dir="/tmp/xdcr_differ_outputs"):
        self._xdcr_differ_src_cluster = src_cluster
        self._xdcr_differ_src_master = src_master
        self._xdcr_differ_src_master_shell = src_master_shell
        self._xdcr_differ_bin_path = bin_path
        self._xdcr_differ_yaml_conf_path = yaml_conf_path
        self._xdcr_differ_output_dir = output_dir

    def _setup_xdcr_differ_config(self, bucket_name):
        remote_cluster_name = None
        for remote_cluster in self._xdcr_differ_src_cluster.get_remote_clusters():
            remote_cluster_name = remote_cluster.get_name()
            break

        if not remote_cluster_name:
            remote_cluster_name = "remote_cluster_C1-C2"

        xdcr_differ_params = {
            "sourceUrl": f"{self._xdcr_differ_src_master.ip}:8091",
            "sourceUsername": self._xdcr_differ_src_master.rest_username,
            "sourcePassword": self._xdcr_differ_src_master.rest_password,
            "sourceBucketName": bucket_name,
            "remoteClusterName": remote_cluster_name,
            "targetUrl": "",
            "targetUsername": "",
            "targetPassword": "",
            "targetBucketName": bucket_name,
            "outputFileDir": self._xdcr_differ_output_dir,
            "sourceFileDir": f"{self._xdcr_differ_output_dir}/source",
            "targetFileDir": f"{self._xdcr_differ_output_dir}/target",
            "checkpointFileDir": f"{self._xdcr_differ_output_dir}/checkpoint",
            "fileDifferDir": f"{self._xdcr_differ_output_dir}/fileDiff",
            "mutationDifferDir": f"{self._xdcr_differ_output_dir}/mutationDiff",
            "oldCheckpointFileName": "",
            "newCheckpointFileName": "checkpoint.json",
            "checkpointInterval": 600,
            "completeByDuration": 0,
            "completeBySeqno": True,
            "compareType": "body",
            "runDataGeneration": True,
            "runFileDiffer": True,
            "runMutationDiffer": True,
            "enforceTLS": False,
            "clearBeforeRun": "true",
            "numberOfSourceDcpClients": 1,
            "numberOfWorkersPerSourceDcpClient": 64,
            "numberOfTargetDcpClients": 1,
            "numberOfWorkersPerTargetDcpClient": 64,
            "numberOfWorkersForFileDiffer": 30,
            "numberOfWorkersForMutationDiffer": 30,
            "numberOfBins": 5,
            "numberOfFileDesc": 500,
            "mutationDifferBatchSize": 100,
            "mutationDifferTimeout": 30,
            "debugMode": False,
            "setupTimeout": 10,
            "excludeDCPExpiryEvents": False
        }

        with open(self._xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(xdcr_differ_params, f)
        self._xdcr_differ_src_master_shell.copy_file_local_to_remote(
            self._xdcr_differ_yaml_conf_path, self._xdcr_differ_yaml_conf_path
        )
        self.log.info(f"xdcrDiffer config written to {self._xdcr_differ_yaml_conf_path}")

    def _cleanup_xdcr_differ_output(self):
        cmd = f"rm -rf {self._xdcr_differ_output_dir}"
        self._xdcr_differ_src_master_shell.execute_command(cmd)

    def _run_xdcr_differ(self, bucket_name, timeout=600):
        self._setup_xdcr_differ_config(bucket_name)
        self._cleanup_xdcr_differ_output()

        cbauth_url = (
            f"http://{self._xdcr_differ_src_master.rest_username}:"
            f"{self._xdcr_differ_src_master.rest_password}@{self._xdcr_differ_src_master.ip}:8091"
        )

        cmd = (
            f"export CBAUTH_REVRPC_URL=\"{cbauth_url}\" && "
            f"{self._xdcr_differ_bin_path} -yamlConfigFilePath {self._xdcr_differ_yaml_conf_path}"
        )

        self.log.info(f"Running xdcrDiffer for bucket {bucket_name}...")
        output, err = self._xdcr_differ_src_master_shell.execute_command(cmd, timeout=timeout)

        if err:
            self.log.warning(f"xdcrDiffer stderr: {err}")

        combined_output = '\n'.join(output) if output else ''
        self.log.info(f"xdcrDiffer output: {combined_output}")

        mismatch_count = self._parse_xdcr_differ_results()
        success = mismatch_count == 0

        return success, mismatch_count, combined_output

    def _parse_xdcr_differ_results(self):
        mutation_dir = f"{self._xdcr_differ_output_dir}/mutationDiff"

        cmd = f"find {mutation_dir} -name '*.json' -type f 2>/dev/null"
        files, _ = self._xdcr_differ_src_master_shell.execute_command(cmd)

        total_mismatches = 0
        for file_path in files:
            file_path = file_path.strip()
            if not file_path:
                continue

            cat_cmd = f"cat {file_path}"
            content, _ = self._xdcr_differ_src_master_shell.execute_command(cat_cmd)
            if content:
                try:
                    data = json.loads('\n'.join(content))
                    if isinstance(data, list):
                        total_mismatches += len(data)
                    elif isinstance(data, dict):
                        total_mismatches += len(data.get('mismatches', []))
                except json.JSONDecodeError:
                    self.log.warning(f"Could not parse differ result: {file_path}")

        self.log.info(f"xdcrDiffer found {total_mismatches} mismatched documents")
        return total_mismatches

    def _verify_no_data_loss_with_differ(self, bucket_name):
        self.log.info(f"Running xdcrDiffer to verify no data loss for bucket {bucket_name}")
        success, mismatch_count, output = self._run_xdcr_differ(bucket_name)

        if not success:
            self.fail(f"Data loss detected! xdcrDiffer found {mismatch_count} mismatched documents.")

        self.log.info(f"xdcrDiffer verification passed: No data loss detected for bucket {bucket_name}")

    def _enable_eccv_on_cluster(self, cluster):
        rest = RestConnection(cluster.get_master_node())
        for bucket in cluster.get_buckets():
            try:
                bucket_info = rest.get_bucket_json(bucket.name)
                if bucket_info.get('enableCrossClusterVersioning', False):
                    self.log.info(f"ECCV already enabled for bucket {bucket.name} on cluster {cluster.get_name()}")
                    continue
                self.log.info(f"Enabling ECCV for bucket {bucket.name} on cluster {cluster.get_name()}")
                result = rest.change_bucket_props(bucket, enableCrossClusterVersioning="true")
                self.log.info(f"ECCV enable result: {result}")
            except Exception as e:
                if "already enabled" in str(e).lower():
                    self.log.info(f"ECCV already enabled for bucket {bucket.name}")
                else:
                    raise

    # ------------------------------------------------------------------ #
    #  Reusable XDCR utility methods                                       #
    # ------------------------------------------------------------------ #

    def get_connectivity_status(self, rest, rc_name=None):
        """Get connectivityStatus from remote cluster references.

        @param rest: RestConnection to the cluster.
        @param rc_name: If given, return status for this ref only.
        @return: Single status string if rc_name given, else dict {name: status}.
        """
        remote_clusters = rest.get_remote_clusters()
        statuses = {}
        for rc in remote_clusters:
            statuses[rc["name"]] = rc.get("connectivityStatus", "UNKNOWN")
        if rc_name:
            return statuses.get(rc_name, "UNKNOWN")
        return statuses

    def wait_for_connectivity_status(self, rest, expected_status,
                                     rc_name=None, timeout=120, interval=5):
        """Poll until a remote cluster ref reaches the expected connectivity status.

        @return: True if reached, False on timeout.
        """
        self.log.info("Waiting for connectivity status '{0}'".format(expected_status))
        end_time = time.time() + timeout
        statuses = {}
        while time.time() < end_time:
            statuses = self.get_connectivity_status(rest)
            for name, status in statuses.items():
                if rc_name and name != rc_name:
                    continue
                if status == expected_status:
                    self.log.info("Remote cluster '{0}' reached status: {1}".format(
                        name, status))
                    return True
            time.sleep(interval)
        self.log.warning("Timeout waiting for status '{0}'. Current: {1}".format(
            expected_status, statuses))
        return False

    def wait_for_error_connectivity_status(self, rest, rc_name=None,
                                           timeout=120, interval=5):
        """Wait for any error connectivity status (RC_DEGRADED or RC_ERROR).

        @return: The error status string, or None on timeout.
        """
        self.log.info("Waiting for error connectivity status")
        end_time = time.time() + timeout
        error_statuses = [CONNECTIVITY_STATUS.RC_DEGRADED,
                          CONNECTIVITY_STATUS.RC_ERROR,
                          CONNECTIVITY_STATUS.RC_AUTH_ERR]
        statuses = {}
        while time.time() < end_time:
            statuses = self.get_connectivity_status(rest)
            for name, status in statuses.items():
                if rc_name and name != rc_name:
                    continue
                if status in error_statuses:
                    self.log.info("Remote cluster '{0}' reached error status: {1}".format(
                        name, status))
                    return status
            time.sleep(interval)
        self.log.warning("Timeout waiting for error status. Current: {0}".format(statuses))
        return None

    def block_nftables_traffic(self, cluster, target_ip):
        """Block outbound traffic from all nodes in a cluster to the target IP
        using nftables. Verifies the rule was applied."""
        for node in cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                cmd = ("nft add table inet filter 2>/dev/null; "
                       "nft 'add chain inet filter output "
                       "{{ type filter hook output priority 0 ; policy accept ; }}' "
                       "2>/dev/null; "
                       "nft add rule inet filter output ip daddr {0} drop".format(
                           target_ip))
                shell.execute_command(cmd, use_channel=True, timeout=10)
                verify_out, _ = shell.execute_command(
                    "nft list ruleset | grep '{0}' || echo NFT_RULE_MISSING".format(
                        target_ip))
                verify_str = " ".join(verify_out) if verify_out else ""
                if "NFT_RULE_MISSING" in verify_str:
                    raise Exception(
                        "nftables rule to block {0} was not applied on {1}".format(
                            target_ip, node.ip))
                self.log.info("Blocked traffic from {0} to {1}".format(
                    node.ip, target_ip))
            finally:
                shell.disconnect()

    def unblock_nftables(self, cluster):
        """Flush all nftables rules on all nodes of a cluster."""
        for node in cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                shell.execute_command(
                    "nft flush ruleset 2>/dev/null; true",
                    use_channel=True, timeout=10)
                self.log.info("Flushed nftables on {0}".format(node.ip))
            except Exception as e:
                self.log.warning("Failed to flush nftables on {0}: {1}".format(
                    node.ip, e))
            finally:
                shell.disconnect()

    def kill_goxdcr_on_cluster(self, cluster, wait_to_recover=True):
        """Kill goxdcr process on all nodes in a cluster."""
        for node in cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                shell.execute_command("pkill -9 goxdcr || true", use_channel=True)
                self.log.info("Killed goxdcr on {0}".format(node.ip))
            except Exception as e:
                self.log.error("Error killing goxdcr on {0}: {1}".format(node.ip, e))
            finally:
                shell.disconnect()
        if wait_to_recover:
            self.sleep(15, "Waiting for goxdcr to restart")

    def _goxdcr_admin_auth_header(self, server):
        """Build a base64 Authorization header value for the goxdcr debug
        endpoint. The endpoint is localhost-only on port 9998, so we must
        run curl on the remote node — but credentials and form data are
        interpolated into a shell command. base64 + shlex.quote keeps that
        interpolation injection-safe even when the password / param /
        value contains shell metacharacters (`&`, `'`, `$`, space, `!`).
        """
        import base64
        creds = "{0}:{1}".format(
            server.rest_username, server.rest_password).encode("utf-8")
        return base64.b64encode(creds).decode("ascii")

    def set_goxdcr_internal_setting(self, server, param, value):
        """Set an internal XDCR setting via the goxdcr debug endpoint."""
        import shlex
        shell = RemoteMachineShellConnection(server)
        try:            
            data_arg = shlex.quote(urllib.parse.urlencode({param: value}))
            auth = self._goxdcr_admin_auth_header(server)
            cmd = ("curl -s -X POST "
                   "-H 'Authorization: Basic {auth}' "
                   "http://localhost:9998/xdcr/internalSettings "
                   "-d {data}").format(auth=auth, data=data_arg)
            shell.execute_command(cmd, timeout=5, use_channel=True)
        finally:
            shell.disconnect()

    def get_goxdcr_internal_setting(self, server, param):
        """Get an internal XDCR setting from the goxdcr debug endpoint.

        @return: The setting value, or None on failure.
        """
        shell = RemoteMachineShellConnection(server)
        try:
            auth = self._goxdcr_admin_auth_header(server)
            cmd = ("curl -s -H 'Authorization: Basic {auth}' "
                   "http://localhost:9998/xdcr/internalSettings").format(
                       auth=auth)
            output, _ = shell.execute_command(cmd, timeout=5)
            if output:
                settings = json.loads(" ".join(output))
                return settings.get(param)
        except Exception as e:
            self.log.warning("Failed to get internal XDCR setting '{0}': {1}".format(
                param, e))
        finally:
            shell.disconnect()
        return None

    def set_replication_setting_by_id(self, rest, repl_id, param, value):
        """Set a per-replication setting via POST /settings/replications/<id>.

        @param rest: RestConnection to the source cluster.
        @param repl_id: Full replication spec ID (e.g. "uuid/bucket/bucket").
        @param param: Setting name.
        @param value: Setting value.
        """
        repl_id_encoded = repl_id.replace("/", "%2F")
        api = rest.baseUrl + "settings/replications/" + repl_id_encoded
        params = urllib.parse.urlencode({param: value})
        status, content, _ = rest._http_request(api, "POST", params)
        if not status:
            raise Exception(
                "Failed to set {0}={1} on replication {2}: {3}".format(
                    param, value, repl_id, content))
        self.log.info("Set {0}={1} on replication {2}".format(
            param, value, repl_id))

    def wait_for_replications_to_clear(self, rest, timeout=120, interval=5):
        """Poll until no XDCR replications exist on the cluster.

        @return: True if replications cleared, False on timeout.
        """
        self.log.info("Waiting for all replications to be garbage collected")
        end_time = time.time() + timeout
        while time.time() < end_time:
            replications = rest.get_replications()
            if not replications:
                self.log.info("All replications have been garbage collected")
                return True
            self.log.info("Still {0} replication(s) active, waiting...".format(
                len(replications)))
            time.sleep(interval)
        self.log.warning("Timeout: replications still exist after {0}s".format(timeout))
        return False

    def wait_for_replications_paused(self, rest, timeout=90, interval=5):
        """Poll until every active replication acknowledges pauseRequested or
        reports a paused status. Used as a bounded substitute for a blind
        post-pause sleep — a teardown DELETE during goxdcr's drain is what
        triggers the historical 500 from cancelXDCR.

        Returns True once paused, False on timeout, or True immediately if
        no replications exist.
        """
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                repls = rest.get_replications()
            except Exception as e:
                self.log.warning(
                    "get_replications during pause-wait failed: {0}".format(e))
                time.sleep(interval)
                continue
            if not repls:
                return True
            if all(
                str(r.get("pauseRequested", "")).lower() == "true"
                or r.get("status") == "paused"
                for r in repls
            ):
                return True
            time.sleep(interval)
        self.log.warning(
            "Timeout: replications did not reach paused after {0}s".format(timeout))
        return False

    def get_cluster_certificates(self, cluster):
        """Extract all trusted CA certificate PEMs from a cluster.

        @return: Concatenated PEM string of all trusted CAs.
        """
        rest_conn = RestConnection(cluster.get_master_node())
        raw_content = rest_conn.get_trusted_CAs()
        certificate = ""
        for ca_dict in raw_content:
            certificate += ca_dict["pem"]
        return certificate

    def get_successful_checkpoint_count(self, rest):
        """Get the total number of successful checkpoints across all replications."""
        replications = rest.get_replications()
        total = 0
        for repl in replications:
            try:                
                api = rest.baseUrl + "pools/default/buckets/@xdcr-{0}/stats".format(
                    urllib.parse.quote(repl["source"], safe=""))
                status, content, _ = rest._http_request(api, "GET")
                if status:
                    stats = json.loads(content)
                    ckpt_key = "replications/{0}/num_checkpoints".format(repl["id"])
                    if ckpt_key in stats.get("op", {}).get("samples", {}):
                        samples = stats["op"]["samples"][ckpt_key]
                        if samples:
                            total += samples[-1]
            except Exception as e:
                self.log.warning("Error fetching checkpoint stats: {0}".format(e))
        return total

    def wait_for_new_checkpoint(self, rest, baseline, poll_interval=10,
                                timeout=180):
        """Poll get_successful_checkpoint_count until it exceeds a baseline.

        Returns the most recent count (may equal baseline on timeout).
        """
        end = time.time() + timeout
        current = baseline
        while time.time() < end:
            current = self.get_successful_checkpoint_count(rest)
            if current > baseline:
                return current
            self.sleep(poll_interval,
                       "waiting for checkpoint > {0}".format(baseline))
        return current

    def log_per_replication_checkpoints(self, rest, label):
        """Dump num_checkpoints per replication via the xdcr stats endpoint.

        get_successful_checkpoint_count sums across all replications; if the
        total is flat we can't tell whether every replication is stuck or
        just one. This helper prints the raw sample for each replication.
        """
        try:
            replications = rest.get_replications()
        except Exception as e:
            self.log.warning("[{0}] get_replications failed: {1}".format(label, e))
            return
        for repl in replications:
            try:                
                api = rest.baseUrl + "pools/default/buckets/@xdcr-{0}/stats".format(
                    urllib.parse.quote(repl.get("source", "?"), safe=""))
                status, content, _ = rest._http_request(api, "GET")
                if not status:
                    self.log.warning("[{0}] stats fetch failed for {1}: status={2}".format(
                        label, repl.get("id"), status))
                    continue
                stats = json.loads(content)
                ckpt_key = "replications/{0}/num_checkpoints".format(repl.get("id"))
                samples = stats.get("op", {}).get("samples", {}).get(ckpt_key, [])
                last = samples[-1] if samples else None
                self.log.info("[{0}] repl {1} source={2} status={3} "
                              "num_checkpoints last_sample={4} samples_len={5}".format(
                                  label, repl.get("id"), repl.get("source"),
                                  repl.get("status"), last, len(samples)))
            except Exception as e:
                self.log.warning("[{0}] per-repl ckpt fetch failed for {1}: {2}".format(
                    label, repl.get("id"), e))

    def log_replications_state(self, rest, label):
        """Dump every active replication spec visible via ns_server tasks."""
        try:
            replications = rest.get_replications()
        except Exception as e:
            self.log.warning("[{0}] get_replications failed on {1}: {2}".format(
                label, rest.ip, e))
            return []
        self.log.info("[{0}] Active replications on {1}: count={2}".format(
            label, rest.ip, len(replications)))
        for i, r in enumerate(replications):
            summary = {k: r.get(k) for k in (
                "id", "source", "target", "status", "pauseRequested",
                "cancelURI", "settingsURI", "errors")}
            self.log.info("[{0}]   replication[{1}]={2}".format(label, i, summary))
        return replications

    def log_remote_clusters_state(self, rest, label):
        """Dump every remote cluster ref and its connectivity status."""
        try:
            refs = rest.get_remote_clusters()
        except Exception as e:
            self.log.warning("[{0}] get_remote_clusters failed on {1}: {2}".format(
                label, rest.ip, e))
            return []
        self.log.info("[{0}] Remote cluster refs on {1}: count={2}".format(
            label, rest.ip, len(refs)))
        for ref in refs:
            summary = {k: ref.get(k) for k in (
                "name", "uuid", "hostname", "encryptionType",
                "connectivityStatus", "connectivityErrors",
                "secureType", "deleted")}
            self.log.info("[{0}]   remote_cluster={1}".format(label, summary))
        return refs

    def log_xdcr_state(self, rest, cluster, label):
        """Combined replications + remote-refs snapshot under one label."""
        self.log.info("==== [{0}] XDCR state snapshot on cluster={1} ====".format(
            label, cluster.get_name() if cluster else "?"))
        self.log_remote_clusters_state(rest, label)
        self.log_replications_state(rest, label)

    def active_remote_refs(self, rest):
        """Active (non-deleted) remote cluster refs."""
        return [r for r in rest.get_remote_clusters() if not r.get('deleted')]

    def bucket_item_count(self, cluster, bucket_name):
        """Current bucket item count via REST bucket stats with a per-node
        fallback that some bucket configurations require."""
        rest = RestConnection(cluster.get_master_node())
        try:
            samples = rest.fetch_bucket_stats(
                bucket=bucket_name, zoom="minute")["op"]["samples"]
            return int(samples["curr_items"][-1])
        except Exception as e:
            self.log.warning("fetch_bucket_stats failed for {0}/{1}: {2}".format(
                cluster.get_name(), bucket_name, e))
            info = rest.get_bucket_json(bucket_name)
            return sum(n["interestingStats"]["curr_items"] for n in info["nodes"])

    def get_total_changes_left(self, src_cluster):
        """Sum replication_changes_left across every bucket on a source cluster."""
        total = 0
        for bucket in src_cluster.get_buckets():
            try:
                total += src_cluster.get_xdcr_stat(
                    bucket.name, 'replication_changes_left')
            except Exception as e:
                self.log.warning("changes_left fetch failed for {0}: {1}".format(
                    bucket.name, e))
        return total

    def wait_for_backlog_to_drain(self, src_cluster, timeout, poll_interval=60):
        """Poll replication_changes_left until 0 or timeout.

        Logs drain rate / ETA on every poll so a slow-but-progressing drain
        is obvious in the log and a stalled drain is distinguishable from
        a zero one. Returns the final backlog — caller asserts.
        """
        end_time = time.time() + timeout
        prev_backlog = None
        prev_ts = None
        last_backlog = self.get_total_changes_left(src_cluster)
        self.log.info("Waiting for backlog to drain: start={0}, timeout={1}s, "
                      "poll={2}s".format(last_backlog, timeout, poll_interval))
        while time.time() < end_time:
            if last_backlog <= 0:
                self.log.info("Backlog fully drained.")
                return last_backlog
            now = time.time()
            if prev_backlog is not None and prev_ts is not None:
                dt = max(now - prev_ts, 1.0)
                drained = max(prev_backlog - last_backlog, 0)
                rate_per_min = drained / dt * 60.0
                eta = (last_backlog / (drained / dt)) if drained > 0 else None
                eta_str = "{0:.0f}s".format(eta) if eta is not None else "stalled"
                self.log.info("Backlog drain: changes_left={0}, "
                              "rate={1:.0f} docs/min, eta={2}".format(
                                  last_backlog, rate_per_min, eta_str))
            else:
                self.log.info("Backlog drain: changes_left={0}".format(last_backlog))
            prev_backlog = last_backlog
            prev_ts = now
            self.sleep(poll_interval,
                       "Polling changes_left (current={0})".format(last_backlog))
            last_backlog = self.get_total_changes_left(src_cluster)
        self.log.warning("Backlog drain timed out after {0}s: final={1}".format(
            timeout, last_backlog))
        return last_backlog

    def change_standard_remote_ref_credentials(self, src_cluster, dest_cluster,
                                               rc_name, username, password):
        """Rotate the auth credentials on a non-CNG (couchbase://) remote ref."""
        rest = RestConnection(src_cluster.get_master_node())
        dest_master = dest_cluster.get_master_node()
        certificate = self.get_cluster_certificates(dest_cluster)
        rest.modify_remote_cluster(
            dest_master.cluster_ip, str(dest_master.port),
            username, password, rc_name,
            demandEncryption=1, certificate=certificate, encryptionType="full")
        self.log.info("Updated credentials for ref '{0}' (username={1})".format(
            rc_name, username))

    @staticmethod
    def find_matching_bucket(src_bucket, dest_buckets):
        """Find a destination bucket matching the source bucket name."""
        for db in dest_buckets:
            if db.name == src_bucket.name:
                return db
        return dest_buckets[0] if dest_buckets else src_bucket

    def create_scope_and_collection(self, rest, bucket_name,
                                    scope_name, collection_name):
        """Create a scope and a collection under it."""
        rest.create_scope(bucket_name, scope_name)
        time.sleep(2)
        rest.create_collection(bucket_name, scope_name, collection_name)
        self.log.info("Created {0}.{1}.{2}".format(
            bucket_name, scope_name, collection_name))

    def delete_scope_with_collections(self, rest, bucket_name, scope_name):
        """Delete a scope (which also drops all its collections).

        @return: True if deletion succeeded.
        """
        status = rest.delete_scope(bucket_name, scope_name)
        if status:
            self.log.info("Deleted scope {0}.{1}".format(bucket_name, scope_name))
        else:
            self.log.warning("Failed to delete scope {0}.{1}".format(
                bucket_name, scope_name))
        return status

    def verify_scope_exists(self, rest, bucket_name, scope_name):
        """Check if a scope exists in the bucket manifest."""
        manifest = rest.get_bucket_manifest(bucket_name)
        for scope in manifest.get("scopes", []):
            if scope["name"] == scope_name:
                return True
        return False

    def verify_collection_exists(self, rest, bucket_name, scope_name,
                                 collection_name):
        """Check if a collection exists under a scope."""
        manifest = rest.get_bucket_manifest(bucket_name)
        for scope in manifest.get("scopes", []):
            if scope["name"] == scope_name:
                for coll in scope.get("collections", []):
                    if coll["name"] == collection_name:
                        return True
        return False

    # ------------------------------------------------------------------ #
    #  RBAC user management helpers (cluster-scoped API)                   #
    # ------------------------------------------------------------------ #
    # These operate on a CouchbaseCluster object (not a single node) and
    # delegate to the cluster master. The node-scoped variants in
    # stagedCredentialsXDCR.py predate this API and are kept for that
    # suite's existing call sites; new code should use the cluster API here.

    def _create_xdcr_user(self, cluster, username, password,
                          roles="replication_target[*]"):
        """Create a local XDCR user on the cluster master with given roles."""
        node = cluster.get_master_node()
        testuser = [{'id': username, 'name': username, 'password': password}]
        RbacBase().create_user_source(testuser, 'builtin', node)
        self.sleep(2, "User creation propagation for '{0}'".format(username))
        role_list = [{'id': username, 'name': username,
                      'roles': roles, 'password': password}]
        RbacBase().add_user_role(role_list, RestConnection(node), 'builtin')
        self.log.info("Created user '{0}' with roles='{1}' on {2}".format(
            username, roles, node.ip))

    def _set_user_roles(self, cluster, username, roles, password=None):
        """Reassign roles for an existing user on the cluster master."""
        node = cluster.get_master_node()
        pw = password or node.rest_password
        role_list = [{'id': username, 'name': username,
                      'roles': roles, 'password': pw}]
        RbacBase().add_user_role(role_list, RestConnection(node), 'builtin')
        self.log.info("Set roles='{0}' for user '{1}' on {2}".format(
            roles, username, node.ip))

    def _strip_xdcr_roles(self, cluster, username, password=None):
        """Downgrade user to ro_admin to remove XDCR permissions."""
        self._set_user_roles(cluster, username, "ro_admin", password=password)
        self.log.info("Stripped XDCR roles from user '{0}'".format(username))

    def _delete_user(self, cluster, username):
        """Delete a user from the cluster; logs warning on failure."""
        try:
            RbacBase().remove_user_role(
                [username], RestConnection(cluster.get_master_node()))
            self.log.info("Deleted user '{0}'".format(username))
        except Exception as e:
            self.log.warning("Error deleting user '{0}': {1}".format(
                username, e))

    def _change_admin_password(self, cluster, new_password):
        """Change the Administrator password; updates node.rest_password.

        Body must be URL-encoded — a `&`, `=`, `+`, `%`, or space in the
        password would otherwise corrupt the x-www-form-urlencoded field
        (silent rotation to a truncated / garbled value, or rotation
        succeeding under a wrong key entirely).
        """
        node = cluster.get_master_node()
        rest = RestConnection(node)
        api = rest.baseUrl + "controller/changePassword"
        params = urllib.parse.urlencode({"password": new_password})
        status, content, _ = rest._http_request(api, 'POST', params)
        if not status:
            self.fail("Failed to change Administrator password on {0}: {1}".format(
                node.ip, content))
        node.rest_password = new_password
        self.log.info("Changed Administrator password on {0}".format(node.ip))

    # ==================================================================
    # Generic helpers promoted from forwardLocalOnlyXDCR. Reusable by any
    # XDCR suite. Helpers that log use `self._log_tag()` so subclasses
    # can prefix log lines with a per-suite tag.
    # ==================================================================
    def _log_tag(self):
        """Per-suite log tag for greppable CI output. Subclasses override
        to return e.g. `[FLO-pv-not-local]`. Default empty so base log
        lines are unchanged."""
        return ""

    # ------------------------------------------------------------------
    # Set-then-verify REST helpers. Every XDCR setting change SHOULD
    # route through one of these so a silently-rejected setting fails
    # the test fast instead of being shadowed by a downstream assertion.
    # ------------------------------------------------------------------
    def _coerce_xdcr_value(self, value):
        """Lowercase bools for goxdcr's expected string form. Pass-through
        for everything else. Single coercion site avoids drift between
        callers that pre-coerce and callers that don't."""
        if isinstance(value, bool):
            return str(value).lower()
        return value

    def set_xdcr_param_verified(self, cluster, src_bucket, dest_bucket,
                                 param, value, settle=0, assert_match=True):
        """POST `param=value` to replication settings, sleep `settle`s,
        GET and assert. Returns the read-back value (str)."""
        tag = self._log_tag()
        rest = RestConnection(cluster.get_master_node())
        coerced = self._coerce_xdcr_value(value)
        self.log.info(
            "{0} SET xdcr_param: cluster={1} {2}->{3} {4}={5}".format(
                tag, cluster.get_name(), src_bucket, dest_bucket,
                param, value))
        rest.set_xdcr_param(src_bucket, dest_bucket, param, coerced)
        if settle:
            time.sleep(settle)
        try:
            actual = rest.get_xdcr_param(src_bucket, dest_bucket, param)
        except Exception as e:
            self.fail(
                "{0} VERIFY FAILED: could not read back {1} on {2}->{3}: "
                "{4}".format(tag, param, src_bucket, dest_bucket, e))
        self.log.info(
            "{0} VERIFY xdcr_param: {1}->{2} {3}={4} (expected {5})".format(
                tag, src_bucket, dest_bucket, param, actual, value))
        if assert_match:
            self.assertEqual(
                str(actual).lower(), str(value).lower(),
                "{0} {1} did not persist on {2}->{3}; got {4!r}".format(
                    tag, param, src_bucket, dest_bucket, actual))
        return actual

    def set_xdcr_params_verified(self, cluster, src_bucket, dest_bucket,
                                  param_value_map, settle=0,
                                  assert_match=True):
        """Multi-key variant. POSTs all params in one REST call, sleeps
        `settle`, reads each key back and asserts."""
        tag = self._log_tag()
        rest = RestConnection(cluster.get_master_node())
        self.log.info(
            "{0} SET xdcr_params: cluster={1} {2}->{3} {4}".format(
                tag, cluster.get_name(), src_bucket, dest_bucket,
                param_value_map))
        rest.set_xdcr_params(src_bucket, dest_bucket, param_value_map)
        if settle:
            time.sleep(settle)
        readback = {}
        for param, expected in param_value_map.items():
            try:
                actual = rest.get_xdcr_param(
                    src_bucket, dest_bucket, param)
            except Exception as e:
                self.fail(
                    "{0} VERIFY FAILED: could not read back {1} on "
                    "{2}->{3}: {4}".format(
                        tag, param, src_bucket, dest_bucket, e))
            readback[param] = actual
            self.log.info(
                "{0} VERIFY xdcr_params: {1}->{2} {3}={4} (expected "
                "{5})".format(
                    tag, src_bucket, dest_bucket, param, actual,
                    expected))
            if assert_match:
                self.assertEqual(
                    str(actual).lower(), str(expected).lower(),
                    "{0} {1} did not persist on {2}->{3}; got "
                    "{4!r}".format(
                        tag, param, src_bucket, dest_bucket, actual))
        return readback

    def set_global_xdcr_param_verified(self, cluster, param, value,
                                        settle=0, assert_match=True):
        """POST global /settings/replications, sleep, GET, assert."""
        tag = self._log_tag()
        rest = RestConnection(cluster.get_master_node())
        coerced = self._coerce_xdcr_value(value)
        self.log.info(
            "{0} SET global xdcr_param: cluster={1} {2}={3}".format(
                tag, cluster.get_name(), param, value))
        rest.set_global_xdcr_param(param, coerced)
        if settle:
            time.sleep(settle)
        actual = self.get_global_xdcr_param(cluster, param)
        self.log.info(
            "{0} VERIFY global xdcr_param: cluster={1} {2}={3} "
            "(expected {4})".format(
                tag, cluster.get_name(), param, actual, value))
        if assert_match:
            self.assertEqual(
                str(actual).lower(), str(value).lower(),
                "{0} global {1} did not persist on {2}; got {3!r}".format(
                    tag, param, cluster.get_name(), actual))
        return actual

    def set_bucket_prop_verified(self, cluster, bucket, prop, value,
                                  settle=0, assert_match=True):
        """change_bucket_props one prop, sleep, GET via get_bucket_json,
        assert. Returns the read-back value."""
        tag = self._log_tag()
        rest = RestConnection(cluster.get_master_node())
        coerced = self._coerce_xdcr_value(value)
        self.log.info(
            "{0} SET bucket_prop: cluster={1} bucket={2} {3}={4}".format(
                tag, cluster.get_name(), bucket.name, prop, value))
        rest.change_bucket_props(bucket, **{prop: coerced})
        if settle:
            time.sleep(settle)
        try:
            info = rest.get_bucket_json(bucket.name)
            actual = info.get(prop)
        except Exception as e:
            self.fail(
                "{0} VERIFY FAILED: could not read back {1} on {2}/{3}: "
                "{4}".format(
                    tag, prop, cluster.get_name(), bucket.name, e))
        self.log.info(
            "{0} VERIFY bucket_prop: {1}/{2} {3}={4} (expected {5})".format(
                tag, cluster.get_name(), bucket.name, prop, actual, value))
        if assert_match:
            self.assertEqual(
                str(actual).lower(), str(value).lower(),
                "{0} {1} did not persist on {2}/{3}; got {4!r}".format(
                    tag, prop, cluster.get_name(), bucket.name, actual))
        return actual

    def get_global_xdcr_param(self, cluster, param):
        """GET /settings/replications, return param value or None."""
        rest = RestConnection(cluster.get_master_node())
        api = rest.baseUrl[:-1] + "/settings/replications"
        status, content, _ = rest._http_request(api)
        if not status:
            self.log.warning(
                "Failed to GET /settings/replications on {0}".format(
                    cluster.get_name()))
            return None
        try:
            obj = json.loads(content)
            return obj.get(param)
        except Exception as e:
            self.log.warning(
                "Could not parse global xdcr settings: {0}".format(e))
            return None

    # ------------------------------------------------------------------
    # Prometheus scrape helpers. Distinct from "metric absent from
    # response" (legitimate zero) -- raises on infra failure so callers
    # do not silently coerce a node-down event into a zero reading.
    # ------------------------------------------------------------------
    def scrape_prometheus_metric(self, server, metric_name,
                                   sum_across_labels=True,
                                   endpoints=("_prometheusMetrics",)):
        """Scrape `metric_name` from the node's Prometheus endpoint(s).
        Returns the summed float (default) or a list of (endpoint, line)
        tuples if `sum_across_labels=False`.
        Raises `MetricsScrapeError` if every endpoint is unreachable."""
        rest = RestConnection(server)
        total = 0.0
        lines = []
        any_ok = False
        last_err = None
        for endpoint in endpoints:
            try:
                status, content = rest.get_rest_endpoint_data(
                    endpoint, ip=server.ip, port=server.port)
            except Exception as e:
                last_err = e
                continue
            if not status:
                last_err = "non-OK status from {0}".format(endpoint)
                continue
            any_ok = True
            text = (content.decode()
                    if isinstance(content, bytes) else str(content))
            for line in text.splitlines():
                stripped = line.lstrip()
                if stripped.startswith("#") or not stripped:
                    continue
                # Exact-match the metric token. `startswith` would
                # also match histogram/summary auto-series
                # (`<name>_sum`, `<name>_count`, `<name>_bucket`,
                # `<name>_created`) AND any unrelated metric whose
                # name extends `metric_name` as a prefix -- both
                # inflate the sum and break strict counter
                # assertions. Token is everything before `{` or
                # whitespace, whichever comes first.
                name = stripped.split("{", 1)[0].split(None, 1)[0]
                if name != metric_name:
                    continue
                if sum_across_labels:
                    try:
                        total += float(line.rsplit(None, 1)[-1])
                    except ValueError:
                        self.log.warning(
                            "{0} Could not parse metric line: {1}"
                            .format(self._log_tag(), line))
                else:
                    lines.append((endpoint, line))
        if not any_ok:
            raise MetricsScrapeError(
                "Prometheus scrape failed on {0}: {1}".format(
                    server.ip, last_err))
        return total if sum_across_labels else lines

    def scrape_prometheus_lines(self, server, name_substr,
                                 endpoints=("_prometheusMetrics",
                                             "_prometheusMetricsHigh")):
        """Return every line whose name contains `name_substr`
        (case-insensitive) across the given endpoints. Used to detect
        whether a metric is exposed under a different name or label
        set than expected. Best-effort; does not raise on individual
        endpoint failure."""
        rest = RestConnection(server)
        matches = []
        for endpoint in endpoints:
            try:
                status, content = rest.get_rest_endpoint_data(
                    endpoint, ip=server.ip, port=server.port)
            except Exception as e:
                self.log.warning(
                    "Endpoint {0} scrape on {1} raised: {2}".format(
                        endpoint, server.ip, e))
                continue
            if not status:
                continue
            text = (content.decode()
                    if isinstance(content, bytes) else str(content))
            for line in text.splitlines():
                if line.startswith("#") or not line.strip():
                    continue
                if name_substr.lower() in line.lower():
                    matches.append((endpoint, line))
        return matches

    # ------------------------------------------------------------------
    # SDK helpers. _get_sdk_client is a hook subclasses may override to
    # return a pooled / cached client. Default opens a fresh SDKClient
    # which the caller MUST close.
    # ------------------------------------------------------------------
    def _get_sdk_client(self, cluster, bucket_name):
        """Default factory: open a fresh SDKClient. Subclasses may
        override to return a cached / shared client (e.g. FLO suite's
        `_borrow_sdk_client`)."""
        try:
            from sdk_client3 import SDKClient
        except ModuleNotFoundError:
            from lib.sdk_client3 import SDKClient
        master = cluster.get_master_node()
        return SDKClient(
            bucket=bucket_name,
            hosts=[master.ip],
            username=master.rest_username,
            password=master.rest_password)

    def sdk_get_raw(self, cluster, bucket, key, scope=None, collection=None):
        """Direct SDK get that BYPASSES sdk_client3.get()'s broken retry
        loop (sdk_client3.py:678 recurses infinitely on
        CouchbaseException). Returns the GetResult on success, None on
        DocumentNotFound, raises on any other SDK error.

        Use this anywhere a bare `client.get(key)` would be vulnerable
        to the recursion bug.

        The default `_get_sdk_client` factory opens a fresh SDKClient
        per call. The try/finally close() prevents fd leakage in
        callers that loop (e.g., `wait_for_doc_present`,
        `capture_doc_cas`) -- without it, suites that don't override
        `_get_sdk_client` exhaust file descriptors and hit EMFILE.
        Suites that override the factory to return a cached client
        (e.g. FLO's `_SharedSDKClient`) implement `close()` as a no-op
        so the cached connection survives across calls."""
        bucket_name = bucket.name if hasattr(bucket, "name") else bucket
        client = self._get_sdk_client(cluster, bucket_name)
        try:
            if scope or collection:
                client.collection_connect(scope, collection)
                coll = client.collection
            else:
                coll = client.default_collection
            try:
                return coll.get(key)
            except Exception as e:
                msg = str(e)
                if ("DocumentNotFoundException" in msg
                        or "document_not_found" in msg
                        or "KEY_ENOENT" in msg):
                    return None
                raise
        finally:
            try:
                client.close()
            except Exception as e:
                self.log.warning(
                    "sdk_get_raw client.close() raised "
                    "(non-fatal): {0}".format(e))

    def wait_for_doc_present(self, cluster, bucket, key, timeout=300,
                              poll_interval=5, scope=None, collection=None):
        """Poll `cluster/bucket` for `key` until it exists or `timeout`
        elapses. Returns the GetResult or None on timeout/error.
        Routes through `sdk_get_raw` to bypass sdk_client3.get()'s
        recursive retry path."""
        bucket_name = bucket.name if hasattr(bucket, "name") else bucket
        tag = self._log_tag()
        deadline = time.time() + timeout
        attempts = 0
        while time.time() < deadline:
            attempts += 1
            try:
                res = self.sdk_get_raw(
                    cluster, bucket, key, scope=scope,
                    collection=collection)
            except Exception as e:
                self.log.warning(
                    "{0} unexpected SDK error on {1}/{2}/{3} "
                    "(attempt {4}): {5}".format(
                        tag, cluster.get_name(),
                        bucket_name, key, attempts, e))
                return None
            if res is not None:
                self.log.info(
                    "{0} doc {1} found on {2}/{3} (attempt {4})".format(
                        tag, key, cluster.get_name(),
                        bucket_name, attempts))
                return res
            self.log.info(
                "{0} doc {1} not yet on {2}/{3} (attempt {4}); "
                "polling in {5}s".format(
                    tag, key, cluster.get_name(),
                    bucket_name, attempts, poll_interval))
            time.sleep(poll_interval)
        self.log.error(
            "{0} doc {1} NOT FOUND on {2}/{3} after {4}s "
            "({5} attempts)".format(
                tag, key, cluster.get_name(),
                bucket_name, timeout, attempts))
        return None

    # ------------------------------------------------------------------
    # CAS capture + diff. Sentinel distinguishes a doc that doesn't
    # exist (None) from a CAS read that failed for infra reasons -- so
    # cas_pre[k] != cas_post[k] comparisons don't reclassify a network
    # blip as a product CAS change.
    # ------------------------------------------------------------------
    CAS_READ_FAILED = "__cas_read_failed__"
    CAS_DIFF_MAX_FAIL_RATIO = 0.05

    def capture_doc_cas(self, cluster, bucket, key_prefix, count,
                         scope=None, collection=None):
        """Read CAS for keys `<prefix>-0..count-1`. Returns
        {key: cas-or-sentinel}. Values:
          * `int`               - live CAS
          * `None`              - doc does not exist (legitimate DNF)
          * `CAS_READ_FAILED`   - infra failure
        Diff comparisons MUST skip keys with the sentinel on either
        side -- otherwise a transient network blip reads as a CAS
        change."""
        tag = self._log_tag()
        bucket_name = bucket.name if hasattr(bucket, "name") else bucket
        cas_map = {}
        for i in range(count):
            key = "{0}-{1}".format(key_prefix, i)
            try:
                res = self.sdk_get_raw(
                    cluster, bucket, key,
                    scope=scope, collection=collection)
            except Exception as e:
                self.log.error(
                    "{0} CAS read INFRA FAILURE for {1} on {2}/{3}"
                    "{4}: {5}".format(
                        tag, key, cluster.get_name(), bucket_name,
                        " ({0}.{1})".format(scope, collection)
                        if scope else "", e))
                cas_map[key] = self.CAS_READ_FAILED
                continue
            if res is None:
                cas_map[key] = None
                continue
            cas = getattr(res, "cas", None)
            if cas is None and isinstance(res, dict):
                cas = res.get("cas")
            cas_map[key] = cas
        return cas_map

    def count_cas_changes(self, cas_pre, cas_post, log_label="",
                           max_fail_ratio=None):
        """Diff two CAS snapshots safely. Returns
        (changed, failed, failed_above_threshold)."""
        tag = self._log_tag()
        threshold = (max_fail_ratio if max_fail_ratio is not None
                     else self.CAS_DIFF_MAX_FAIL_RATIO)
        changed = 0
        failed = 0
        total = 0
        for k in cas_pre:
            total += 1
            v_pre = cas_pre[k]
            v_post = cas_post.get(k)
            if (v_pre == self.CAS_READ_FAILED
                    or v_post == self.CAS_READ_FAILED):
                failed += 1
                continue
            if v_pre != v_post:
                changed += 1
        ratio = (failed / total) if total > 0 else 0.0
        failed_above_threshold = ratio > threshold
        if failed:
            self.log.warning(
                "{0} {1}CAS-diff INFRA: {2}/{3} keys had read "
                "failures (ratio={4:.3f}; threshold={5:.3f})".format(
                    tag, log_label, failed, total, ratio, threshold))
        if failed_above_threshold:
            self.log.error(
                "{0} {1}CAS-diff failure ratio EXCEEDS threshold; "
                "`changed` count is unreliable".format(tag, log_label))
        return changed, failed, failed_above_threshold

    # ------------------------------------------------------------------
    # Topology assertion helpers. Run BEFORE feature assertions so
    # topology drift (e.g. ctopology=chain silently honored as bidir)
    # fails with explicit attribution rather than green-on-broken.
    # ------------------------------------------------------------------
    def assert_no_direct_replication(self, src_cluster, forbidden_cluster):
        """Assert `src_cluster` has no remote-cluster ref pointing
        directly at `forbidden_cluster`."""
        forbidden_ip = forbidden_cluster.get_master_node().ip
        rest = RestConnection(src_cluster.get_master_node())
        for remote in rest.get_remote_clusters():
            if remote.get("deleted"):
                continue
            host = remote.get("hostname", "")
            if forbidden_ip in host:
                self.fail(
                    "Topology assertion failed: cluster {0} has a direct "
                    "remote-cluster ref to {1} ({2}); should not contain "
                    "a shortcut".format(
                        src_cluster.get_name(),
                        forbidden_cluster.get_name(), host))
        self.log.info(
            "Topology asserted: no direct {0}<->{1} remote-cluster ref"
            .format(src_cluster.get_name(),
                     forbidden_cluster.get_name()))

    def assert_direct_replication(self, src_cluster, peer_cluster):
        """Assert `src_cluster` has a non-deleted remote-cluster ref
        pointing at `peer_cluster`. Used in star/ring/chain topology
        self-tests."""
        peer_ip = peer_cluster.get_master_node().ip
        rest = RestConnection(src_cluster.get_master_node())
        for remote in rest.get_remote_clusters():
            if remote.get("deleted"):
                continue
            if peer_ip in remote.get("hostname", ""):
                self.log.info(
                    "Topology asserted: {0}->{1} direct ref present"
                    .format(src_cluster.get_name(),
                             peer_cluster.get_name()))
                return
        self.fail(
            "Topology assertion failed: cluster {0} has no remote ref to "
            "{1} ({2})".format(
                src_cluster.get_name(), peer_cluster.get_name(), peer_ip))

    def assert_ring_topology(self, expected_n_clusters,
                              fail_on_conf_mismatch=False):
        """Verify a daisy-chain ring (A->B->C->A) topology with
        exactly N clusters and unidirectional forward edges.

        Strict positive proof (not just conf-string sniffing):
          * Cluster count matches.
          * Forward edges exist: C1->C2, C2->C3, ..., Cn->C1.
          * No "shortcut" edges (would defeat data-loss callouts).
        Skips on cluster-count mismatch unless `fail_on_conf_mismatch`
        is True; FAILS if conf claims ring + unidir but actual remote-
        cluster refs don't match (i.e. base class silently ignored
        the params)."""
        clusters = self.get_cb_clusters()
        if len(clusters) < expected_n_clusters:
            self.skipTest(
                "{0} requires >={1} clusters; got {2}".format(
                    self._log_tag(), expected_n_clusters, len(clusters)))
        topology = self._input.param("ctopology", "chain")
        if topology != "ring":
            if fail_on_conf_mismatch:
                self.fail("ctopology must be ring; got {0!r}".format(
                    topology))
            self.skipTest("requires ctopology=ring; got {0!r}".format(
                topology))
        rdirection = self._input.param("rdirection", "unidirection")
        if rdirection != "unidirection":
            if fail_on_conf_mismatch:
                self.fail(
                    "rdirection must be unidirection; got {0!r}"
                    .format(rdirection))
            self.skipTest(
                "requires rdirection=unidirection; got {0!r}"
                .format(rdirection))
        for i in range(expected_n_clusters):
            self.assert_direct_replication(
                clusters[i],
                clusters[(i + 1) % expected_n_clusters])
        for i in range(expected_n_clusters):
            src = clusters[i]
            forward = clusters[(i + 1) % expected_n_clusters]
            for other in clusters:
                if other is src or other is forward:
                    continue
                self.assert_no_direct_replication(src, other)

    def capture_checkpoint_seqnos(self, cluster):
        """Read the most recent vb checkpoint seqno per replication.
        Returns dict {repl_id: max_seqno} where max_seqno is the
        max across vbuckets (or 0 on read failure)."""
        tag = self._log_tag()
        rest = RestConnection(cluster.get_master_node())
        seqnos = {}
        for repl in rest.get_replications():
            repl_id = repl.get("id")
            if not repl_id:
                continue
            try:
                ckpt = rest.get_recent_xdcr_vb_ckpt(repl_id)
            except Exception as e:
                self.log.warning(
                    "{0} could not read checkpoint for {1}: {2}".format(
                        tag, repl_id, e))
                seqnos[repl_id] = 0
                continue
            if isinstance(ckpt, dict):
                seqnos[repl_id] = int(ckpt.get("seqno", 0))
            else:
                seqnos[repl_id] = 0
        return seqnos

    # ------------------------------------------------------------------
    # Version detection (mixed-mode aware)
    # ------------------------------------------------------------------
    def node_version(self, server):
        """Return the node's server version string. Raises on read
        failure: an unreachable node must not be silently coerced to
        None, which would make a partial cluster look uniform."""
        return RestConnection(server).get_nodes_self().version

    def cluster_versions(self, cluster):
        """Map node-ip -> version for every node. Raises if any node
        is unreachable."""
        return {node.ip: self.node_version(node)
                for node in cluster.get_nodes()}

    def is_mixed_mode(self, cluster):
        """True if the cluster has >=2 distinct node versions. Raises
        if version detection fails on any node."""
        versions = set(self.cluster_versions(cluster).values())
        versions.discard(None)
        return len(versions) > 1

    # ------------------------------------------------------------------
    # Pipeline-stats helpers. goxdcr stores `replication["target"]` as
    # `<remote_cluster_uuid>/<src_bucket>/<dst_bucket>` -- to filter
    # replications by destination cluster, callers MUST resolve the
    # peer's UUID first (the human cluster name is NOT in the target
    # path). Previous bug: peer_name-not-in-target silently dropped
    # every replication and zeros looked like product failure.
    # ------------------------------------------------------------------
    PIPELINE_STATS = (
        "docs_received_from_dcp",
        "docs_processed",
        "docs_filtered",
        "docs_written",
    )

    def remote_cluster_uuid_for_peer(self, cluster, peer_cluster):
        """Resolve `peer_cluster`'s remote-cluster UUID as recorded on
        `cluster`. Returns None when no matching ref exists."""
        tag = self._log_tag()
        peer_ip = peer_cluster.get_master_node().ip
        rest = RestConnection(cluster.get_master_node())
        refs = rest.get_remote_clusters()
        for ref in refs:
            if ref.get("deleted"):
                continue
            host = ref.get("hostname", "")
            if peer_ip in host:
                uuid = ref.get("uuid")
                if uuid:
                    self.log.info(
                        "{0} remote-cluster UUID for peer {1} ({2}) "
                        "on {3}: {4}".format(
                            tag, peer_cluster.get_name(), peer_ip,
                            cluster.get_name(), uuid))
                    return uuid
        self.log.warning(
            "{0} no remote-cluster ref for peer {1} ({2}) on {3}; "
            "refs={4}".format(
                tag, peer_cluster.get_name(), peer_ip,
                cluster.get_name(),
                [(r.get("name"), r.get("hostname"), r.get("uuid"))
                 for r in refs]))
        return None

    def replications_to_peer(self, cluster, peer_cluster,
                              src_bucket_filter=None):
        """Yield (replication_dict, src_bucket) for every active
        replication on `cluster` whose target is `peer_cluster`,
        optionally restricted to a single source bucket. If UUID
        resolution fails, yields ALL replications and logs a warning
        -- over-counting is preferable to silently dropping every
        replication when the peer's UUID can't be resolved."""
        rest = RestConnection(cluster.get_master_node())
        replications = rest.get_replications()
        peer_uuid = self.remote_cluster_uuid_for_peer(
            cluster, peer_cluster)
        for repl in replications:
            if peer_uuid is not None:
                target = repl.get("target", "") or ""
                rid = repl.get("id", "") or ""
                if peer_uuid not in target and peer_uuid not in rid:
                    continue
            src_bucket = repl.get("source", "default")
            if src_bucket_filter and src_bucket != src_bucket_filter:
                continue
            yield repl, src_bucket

    def read_repl_stat(self, cluster, repl_id, src_bucket, stat):
        """Fetch a per-replication stat from
        `pools/default/buckets/@xdcr-<bucket>/stats`. Path:
        `replications/<repl_id>/<stat>`. Raises on read failure."""
        full = "replications/{0}/{1}".format(repl_id, stat)
        return cluster.get_xdcr_stat(src_bucket, full)

    def get_docs_processed_to_peer(self, cluster, peer_cluster,
                                     src_bucket_filter=None):
        """Sum pipeline signals across replications on `cluster` whose
        destination is `peer_cluster`. Returns dict with the four
        PIPELINE_STATS keys plus `_failed_reads` (list of per-stat
        error messages, so callers can distinguish "stat not
        exported" from "stat is zero")."""
        tag = self._log_tag()
        stats = {s: 0 for s in self.PIPELINE_STATS}
        stats["_failed_reads"] = []
        any_repl = False
        for repl, src_bucket in self.replications_to_peer(
                cluster, peer_cluster,
                src_bucket_filter=src_bucket_filter):
            any_repl = True
            repl_id = repl.get("id", "")
            for stat in self.PIPELINE_STATS:
                try:
                    stats[stat] += int(self.read_repl_stat(
                        cluster, repl_id, src_bucket, stat))
                except Exception as e:
                    stats["_failed_reads"].append(
                        "{0}/{1}/{2}: {3}".format(
                            repl_id, src_bucket, stat, e))
        if not any_repl:
            self.log.warning(
                "{0} no replications matched on {1} for peer {2}; "
                "returning zeros".format(
                    tag, cluster.get_name(),
                    peer_cluster.get_name()))
        if stats["_failed_reads"]:
            self.log.warning(
                "{0} pipeline stat read failures on {1}->{2}: {3}"
                .format(tag, cluster.get_name(),
                         peer_cluster.get_name(),
                         stats["_failed_reads"]))
        self.log.info(
            "{0} pipeline stats on {1}->{2}: {3}".format(
                tag, cluster.get_name(),
                peer_cluster.get_name(), stats))
        return stats

    def get_docs_written_to_peer(self, cluster, peer_cluster):
        """Sum `docs_written` across replications on `cluster` whose
        destination is `peer_cluster`. Delegates to
        `get_docs_processed_to_peer` so peer-UUID resolution + read
        failure logging stay in one place."""
        return self.get_docs_processed_to_peer(
            cluster, peer_cluster).get("docs_written", 0)

    def wait_for_pipeline_delta(self, cluster, peer_cluster, baseline,
                                 min_delta, timeout=300,
                                 poll_interval=10):
        """Poll `get_docs_processed_to_peer` until any of the four
        PIPELINE_STATS advances by at least `min_delta` over
        `baseline`, or `timeout` elapses. Stats samples refresh on
        a periodic flush -- read 100ms after a write usually sees
        stale values, so a poll is required."""
        tag = self._log_tag()
        deadline = time.time() + timeout
        last = baseline
        while time.time() < deadline:
            last = self.get_docs_processed_to_peer(cluster, peer_cluster)
            for stat in self.PIPELINE_STATS:
                if last.get(stat, 0) - baseline.get(stat, 0) >= min_delta:
                    self.log.info(
                        "{0} pipeline delta observed: "
                        "{1} pre={2} post={3} >= {4}".format(
                            tag, stat, baseline.get(stat, 0),
                            last.get(stat, 0), min_delta))
                    return last
            time.sleep(poll_interval)
        self.log.warning(
            "{0} pipeline delta NOT observed within {1}s; final={2}"
            .format(tag, timeout, last))
        return last


class MetricsScrapeError(RuntimeError):
    """Raised when a Prometheus scrape endpoint cannot be read.
    Distinct from "metric absent from response" (legitimate zero)."""


