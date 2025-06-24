import copy
import json
import logging
import random
import re
import time
import unittest

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


class TEST_XDCR_PARAM:
    FAILURE_RESTART = "failure_restart_interval"
    CHECKPOINT_INTERVAL = "checkpoint_interval"
    OPTIMISTIC_THRESHOLD = "optimistic_threshold"
    FILTER_EXP = "filter_expression"
    FILTER_SKIP_RESTREAM = "filter_skip_restream"
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

    @staticmethod
    def get_test_to_create_repl_param_map():
        return {
            TEST_XDCR_PARAM.FAILURE_RESTART: REPL_PARAM.FAILURE_RESTART,
            TEST_XDCR_PARAM.CHECKPOINT_INTERVAL: REPL_PARAM.CHECKPOINT_INTERVAL,
            TEST_XDCR_PARAM.OPTIMISTIC_THRESHOLD: REPL_PARAM.OPTIMISTIC_THRESHOLD,
            TEST_XDCR_PARAM.FILTER_EXP: REPL_PARAM.FILTER_EXP,
            TEST_XDCR_PARAM.FILTER_SKIP_RESTREAM: REPL_PARAM.FILTER_SKIP_RESTREAM,
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
            TEST_XDCR_PARAM.MIRRORING_MODE: REPL_PARAM.MIRRORING_MODE
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
        remote_client.disconnect()
        # wait for restart and warmup on all node
        ClusterOperationHelper.wait_for_ns_servers_or_assert([server], test_case, wait_if_warmup=True)
        # disable firewall on these nodes
        NodeHelper.disable_firewall(server)

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
                    disabled_consistent_view))
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
        self.__init_nodes(disabled_consistent_view)
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

    def _create_bucket_params(self, server, replicas=1, size=0, port=11211, password=None,
                             bucket_type=None, enable_replica_index=1, eviction_policy='valueOnly',
                             bucket_priority=None, flush_enabled=1, lww=False, maxttl=None,
                             bucket_storage='couchstore'):
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
        bucket_params['bucket_storage'] = bucket_storage
        bucket_params['bucket_priority'] = bucket_priority
        bucket_params['flush_enabled'] = flush_enabled
        bucket_params['lww'] = lww
        bucket_params['maxTTL'] = maxttl
        return bucket_params

    def set_global_checkpt_interval(self, value):
        self.set_xdcr_param("checkpointInterval", value)

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
        [task for task in tasks]

    def create_standard_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH, lww=False, maxttl=None,
            bucket_storage='couchstore'):
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
                bucket_storage=bucket_storage)
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
                    bucket_storage=bucket_storage
                ))
            if self.scope_num or self.collection_num:
                tasks.append(CollectionsRest(self.__master_node).async_create_scope_collection(
                    self.scope_num, self.collection_num, name))
        [task for task in tasks]

    def create_default_bucket(
            self, bucket_size, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH, lww=False,
            maxttl=None, bucket_storage='couchstore'):
        """Create default bucket.
        @param bucket_size: size of the bucket.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        @param lww: conflict_resolution_type.
        """
        bucket_params=self._create_bucket_params(
            server=self.__master_node,
            size=bucket_size,
            replicas=num_replicas,
            eviction_policy=eviction_policy,
            bucket_priority=bucket_priority,
            lww=lww,
            maxttl=maxttl,
            bucket_storage=bucket_storage)

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
                bucket_storage=bucket_storage
            ))
        if self.scope_num or self.collection_num:
            tasks.append(CollectionsRest(self.__master_node).async_create_scope_collection(
                self.scope_num, self.collection_num, BUCKET_NAME.DEFAULT))
        [task for task in tasks]

    def get_buckets(self):
        return self.__buckets

    def add_bucket(self, bucket='',
                   ramQuotaMB=1,
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
        if self.use_java_sdk:
            self.gen = SDKDataLoader(num_ops=num_items, percent_create=100, percent_update=0,
                                    percent_delete=0, doc_expiry=exp, all_collections=True)
        else:
            seed = "%s-key-" % self.__name
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
                                        batch_size=1000, pause_secs=1, timeout_secs=30):
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
        """
        if not self.use_java_sdk:
            # TODO append generator values if op_type is already present
            if ops not in self.__kv_gen:
                self.__kv_gen[ops] = kv_gen
            self.gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
        else:
            self.gen = SDKDataLoader(num_ops=kv_gen.end, percent_create=100, percent_update=0,
                                     percent_delete=0, doc_expiry=exp, all_collections=True)

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
                                              batch_size=1000, pause_secs=1, timeout_secs=30):
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
        """
        if not self.use_java_sdk:
            # TODO append generator values if op_type is already present
            if ops not in self.__kv_gen:
                self.__kv_gen[ops] = kv_gen
            self.gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
        else:
            self.gen = SDKDataLoader(num_ops=kv_gen.end, percent_create=100, percent_update=0,
                                     percent_delete=0, doc_expiry=exp, all_collections=True)

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
                                    percent_delete=percent_delete, doc_expiry=expiration, all_collections=True)
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

    def wait_for_flusher_empty(self, timeout=60):
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

    def wait_for_dcp_queue_drain(self, timeout=180):
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
            except Exception as e:
                self.__log.error(e)
            if curr_time > end_time:
                self.__log.error(
                "Timeout occurs while waiting for dcp queue to drain")
                return False
        return True

    def wait_for_outbound_mutations(self, timeout=180):
        """Wait for Outbound mutations to reach 0.
        @return: True if mutations reached to 0 else False.
        """
        curr_time = time.time()
        end_time = curr_time + timeout
        rest = RestConnection(self.__master_node)
        while curr_time < end_time:
            found = 0
            for bucket in self.__buckets:
                try:
                    mutations = int(rest.get_xdc_queue_size(bucket.name))
                except KeyError:
                    self.__log.warning("Stat \"replication_changes_left\" not found")
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
                    hd.setLevel(
                        level=getattr(
                            logging,
                            self._input.param(
                                "log_level",
                                None)))

    def add_built_in_server_user(self, testuser=None, rolelist=None, node=None):
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'roles': 'admin'}]

        self.log.info("**** add built-in '%s' user to node %s ****" % (testuser[0]["name"],
                                                                       node.ip))
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
            if self.__bucket_storage == "magma":
                for node in cluster_nodes:
                    print("Enabling DP for %s" % node)
                    cli = CouchbaseCLI(node)
                    cli.enable_dp()
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

        self.__eviction_policy = self._input.param("eviction_policy", 'valueOnly')
        self.__bucket_storage = self._input.param("bucket_storage", 'couchstore')
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
        self._num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 512)
        self._poll_timeout = self._input.param("poll_timeout", 120)
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
        self._wait_timeout = self._input.param("timeout", 60)
        self._disable_compaction = self._input.param("disable_compaction", "").split('-')
        self._item_count_timeout = self._input.param("item_count_timeout", 300)
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

    def __calculate_bucket_size(self, cluster_quota, num_buckets):
        if 'quota_percent' in self._input.test_params:
            quota_percent = int(self._input.test_params['quota_percent'])
        else:
            quota_percent = None
        bucket_size = 0
        if quota_percent is not None and num_buckets > 0:
            bucket_size = int( float(cluster_quota - 500) * float(quota_percent/100.0 ) /float(num_buckets))
        elif num_buckets > 0:
            bucket_size = int((float(cluster_quota) - 500)/float(num_buckets))
        # Setting upper limit of 1GB for bucket size
        if bucket_size > 1024:
            bucket_size = 1024
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

        for cb_cluster in self.__cb_clusters:
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
                    bucket_storage=self.__bucket_storage)

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
                bucket_priority=bucket_priority)

        cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

        cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

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
        self.set_xdcr_topology()
        # Adding for MB-41318
        time.sleep(10)
        self.setup_all_replications()
        if self._checkpoint_interval != 1800:
            for cluster in self.__cb_clusters:
                cluster.set_global_checkpt_interval(self._checkpoint_interval)

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

    def _wait_for_replication_to_catchup(self, timeout=300, fetch_bucket_stats_by="minute"):

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
        exp = self.filter_exp[bucket]
        if len(exp) > 1:
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
                src_bucket, mapping)
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
