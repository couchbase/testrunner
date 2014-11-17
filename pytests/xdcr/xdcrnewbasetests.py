import unittest
import time
import copy
import logger
import logging

from couchbase.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import ServerUnavailableException
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from testconstants import STANDARD_BUCKET_PORT
from couchbase.document import View
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.stats_tools import StatsCommon
from membase.helper.bucket_helper import BucketOperationHelper
from TestInput import TestInputSingleton
from scripts.collect_server_info import cbcollectRunner
from scripts import collect_data_files

from couchbase.documentgenerator import BlobGenerator
from lib.membase.api.exception import XDCRException


class RenameNodeException(XDCRException):

    def __init__(self, msg=''):
        XDCRException.__init__(self, msg)


class RebalanceNotStopException(XDCRException):

    def __init__(self, msg=''):
        XDCRException.__init__(self, msg)

"""Raise Exception if condition is True
"""


def raise_if(cond, ex):
    if cond:
        raise ex


class TOPOLOGY:
    CHAIN = "chain"
    STAR = "star"
    RING = "ring"


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


class EVICTION_POLICY:
    VALUE_ONLY = "valueOnly"


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


class XDCR_PARAM:
    XDCR_FAILURE_RESTART_INTERVAL = "xdcrFailureRestartInterval"
    XDCR_CHECKPOINT_INTERVAL = "xdcrCheckpointInterval"
    XDCR_OPTIMISTIC_REPL_THRESHOLD = "xdcrOptimisticReplicationThreshold"


class NodeHelper:

    @staticmethod
    def disable_firewall(
            server, rep_direction=REPLICATION_DIRECTION.UNIDIRECTION):
        shell = RemoteMachineShellConnection(server)
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
    def reboot_node(node, test_case, wait_timeout=60):
        # self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            o, r = shell.execute_command(
                "{0} -r -f -t 0".format(COMMAND.SHUTDOWN))
        elif shell.extract_remote_info().type.lower() == OS.LINUX:
            o, r = shell.execute_command(COMMAND.REBOOT)
        shell.log_command_output(o, r)
        # wait for restart and warmup on all node
        time.sleep(wait_timeout * 5)
        # disable firewall on these nodes
        NodeHelper.disable_firewall(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [node],
            test_case,
            wait_if_warmup=True)
        RestConnection(node).enable_xdcr_trace_logging()

    @staticmethod
    def enable_firewall(
            server, rep_direction=REPLICATION_DIRECTION.UNIDIRECTION):
        is_bidirectional = rep_direction == REPLICATION_DIRECTION.BIDIRECTION
        RemoteUtilHelper.enable_firewall(
            server,
            bidirectional=is_bidirectional,
            xdcr=True)

    @staticmethod
    def do_a_warm_up(node):
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        time.sleep(5)
        shell.start_couchbase()
        shell.disconnect()
        RestConnection(node).enable_xdcr_trace_logging()

    @staticmethod
    def wait_service_started(server, wait_time=120):
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
                RestConnection(server).enable_xdcr_trace_logging()
                return
            time.sleep(10)
        raise Exception(
            "Couchbase service is not running after {0} seconds".format(
                wait_time))

    @staticmethod
    def wait_node_restarted(
            server, test_case, wait_time=120, wait_if_warmup=False, check_service=False):
        now = time.time()
        if check_service:
            NodeHelper.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time / 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    # FIXME 'self' is the test_case object. Need to check how
                    # can we pass it.
                    [server], test_case, wait_time=wait_time - num * 10,
                    wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                time.sleep(10)

    @staticmethod
    def kill_erlang(node):
        shell = RemoteMachineShellConnection(node)
        os_info = shell.extract_remote_info()
        shell.kill_erlang(os_info)
        shell.disconnect()

    @staticmethod
    def kill_memcached(node):
        shell = RemoteMachineShellConnection(node)
        shell.kill_erlang()
        shell.disconnect()

    @staticmethod
    def rename_nodes(servers):
        hostnames = {}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
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

# Keep Track of free servers
# For Rebalance-in or swap-rebalance operations.


class FloatingServers:
    _serverlist = []


class XDCRRemoteClusterRef:

    def __init__(self, src_cluster, dest_cluster, name, encryption=False):
        self.__src_cluster = src_cluster
        self.__dest_cluster = dest_cluster
        self.__name = name
        self.__encryption = encryption

        # List of XDCRepication objects
        self.__replications = []

    def get_src_cluster(self):
        return self.__src_cluster

    def get_dest_cluster(self):
        return self.__dest_cluster

    def get_name(self):
        return self.__name

    def get_replications(self):
        return self.__replications

    def add(self):
        rest_conn_src = RestConnection(self.__src_cluster.get_master_node())
        certificate = ""
        dest_master = self.__dest_cluster.get_master_node()
        if self.__encryption:
            rest_conn_dest = RestConnection(dest_master)
            certificate = rest_conn_dest.get_cluster_ceritificate()
        rest_conn_src.add_remote_cluster(
            dest_master.ip, dest_master.port,
            dest_master.rest_username,
            dest_master.rest_password, self.__name,
            demandEncryption=self.__encryption,
            certificate=certificate)

    def set_encryption(self, encryption=True):
        dest_master = self.__dest_cluster.get_dest_cluster(
        ).get_master_node()
        rest_conn_src = RestConnection(self.__src_cluster.get_master_node())
        certificate = ""
        if encryption:
            rest_conn_dest = RestConnection(dest_master)
            certificate = rest_conn_dest.get_cluster_ceritificate()
            rest_conn_src.modify_remote_cluster(
                dest_master.ip, dest_master.port,
                dest_master.rest_username,
                dest_master.rest_password, self.__name,
                demandEncryption=encryption,
                certificate=certificate)
        self.__encryption = encryption

    def create_replication(
            self, fromBucket,
            rep_type=REPLICATION_PROTOCOL.XMEM,
            toBucket=None
    ):
        self.__replications.append(
            XDCReplication(
                self,
                fromBucket,
                rep_type,
                toBucket))

    def start_all_replications(self):
        [repl.start() for repl in self.__replications]

    def pause_all_replications(self):
        [repl.pause() for repl in self.__replications]

    def resume_all_replications(self):
        [repl.resume() for repl in self.__replications]


class XDCReplication:

    def __init__(self, remote_cluster, from_bucket, rep_type, to_bucket):
        self.__remote_cluster_ref = remote_cluster
        self.__from_bucket = from_bucket
        self.__to_bucket = to_bucket or from_bucket
        self.__rep_type = rep_type
        self.__src_cluster = self.__remote_cluster_ref.get_src_cluster()
        self.__dest_cluster = self.__remote_cluster_ref.get_dest_cluster()
        self.log = logger.Logger.get_logger()

        # Response from REST API
        self.__rep_id = None

    def get_src_bucket(self):
        return self.__from_bucket

    def get_dest_bucket(self):
        return self.__to_bucket

    def get_src_cluster(self):
        return self.__src_cluster

    def get_dest_cluster(self):
        return self.__dest_cluster

    # Although Replication start as soon we configure it.
    # But here we can start after creating object of XDCReplication.
    # self.__repl_id will be assigned here.
    def start(self):
        src_master = self.__src_cluster.get_master_node()
        rest_conn_src = RestConnection(src_master)
        (_, self.__rep_id) = rest_conn_src.start_replication(
            REPLICATION_TYPE.CONTINUOUS,
            self.__from_bucket,
            self.__remote_cluster_ref.get_name(),
            rep_type=self.__rep_type,
            toBucket=self.__to_bucket)

    def __verify_pause(self):
        src_master = self.__src_cluster.get_master_node()
        # Is bucket replication paused?
        if not RestConnection(src_master).is_replication_paused(
                self.__from_bucket,
                self.__to_bucket):
            raise XDCRException(
                "XDCR is not paused for SrcBucket: {0}, Target Bucket: {1}".
                format(self.__from_bucket,
                       self.__to_bucket))

    def pause(self, verify=False):
        src_master = self.__src_cluster.get_master_node()
        if not RestConnection(src_master).is_replication_paused(
                self.__from_bucket, self.__to_bucket):
            RestConnection(src_master).set_xdcr_param(
                self.__from_bucket,
                self.__to_bucket,
                'pauseRequested',
                'true')

        if verify:
            self.__verify_pause()

    def __is_cluster_replicating(self):
        count = 0
        src_master = self.__src_cluster.get_master_node()
        while count < 3:
            outbound_mutations = self.__src_cluster.get_xdcr_stat(
                src_master,
                self.__from_bucket,
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
        src_master = self.__src_cluster.get_master_node()
        # Is bucket replication paused?
        if RestConnection(src_master).is_replication_paused(self.__from_bucket,
                                                            self.__to_bucket):
            raise XDCRException(
                "Replication is not resumed for SrcBucket: {0}, \
                Target Bucket: {1}".format(self.__from_bucket, self.__to_bucket))

        if not self.__is_cluster_replicating():
            self.log.info("XDCR completed on {0}".format(src_master.ip))

    def resume(self, verify=False):
        src_master = self.__src_cluster.get_master_node()
        if RestConnection(src_master).is_replication_paused(
                self.__from_bucket, self.__to_bucket):
            RestConnection(src_master).set_xdcr_param(self.__from_bucket,
                                                      self.__to_bucket,
                                                      'pauseRequested',
                                                      'false')

        if verify:
            self.__verify_resume()
#
# """Design document for Views"""
#
#
# class DesignDocument:
#
#     def __init__(self, name, bucket):
#         self.__name = name
#         self.__bucket = bucket
#         self.__views = []
#
#     def get_views(self):
#         return self.__views
#
#     def add_view(self, view):
#         self.__views.append(view)


class CouchbaseCluster:

    def __init__(self, name, nodes, log, use_host_name=False):
        self.__name = name
        self.__nodes = nodes
        self.__log = log
        self.__mem_quota = 0
        self.__use_hostname = use_host_name
        self.__master_node = nodes[0]
        self.__design_docs = []
        self.__buckets = []
        self.__hostnames = {}
        self.__fail_over_nodes = []
        self.__data_verified = True
        # List of XDCRRemoteClusterRef Objects.
        self.__remote_clusters = []
        self.__clusterop = Cluster()
        self.__kv_gen = None

    def __str__(self):
        return self.__master_node.ip

    def __sleep(self, timeout=1, message=""):
        self.__log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def __stop_rebalance(self):
        rest = RestConnection(self.__master_node)
        if rest._rebalance_progress_status() == 'running':
            self.__log.warning(
                "rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            raise_if(
                not stopped,
                RebalanceNotStopException("unable to stop rebalance"))

    # Init/Cleanup Cluster
    def __init_nodes(self, disabled_consistent_view=None):
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

    def get_name(self):
        return self.__name

    def init_cluster(self, disabled_consistent_view=None):
        self.__init_nodes(disabled_consistent_view)
        self.__clusterop.async_rebalance(
            self.__nodes,
            self.__nodes[1:],
            [],
            use_hostnames=self.__use_hostname).result()
        for node in self.__nodes:
            RestConnection(node).enable_xdcr_trace_logging()

    def __get_cbcollect_info(self):
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        for server in self.__nodes:
            print "grabbing cbcollect from {0}".format(server.ip)
            path = path or "."
            try:
                cbcollectRunner(server, path).run()
                TestInputSingleton.input.test_params[
                    "get-cbcollect-info"] = False
            except:
                print "IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}".format(server.ip)

    def __collect_data_files(self):
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        for server in self.__nodes:
            collect_data_files.cbdatacollectRunner(server, path).run()

    def __collect_logs(self, cluster_run):
            # Grab cbcollect before we cleanup
        if TestInputSingleton.input.param("get-cbcollect-info", False):
            self.__get_cbcollect_info()

        if not cluster_run and not self.__data_verified:
            self.__collect_data_files()

    def cleanup_cluster(
            self,
            test_case,
            test_failed=False,
            cluster_run=False,
            cluster_shutdown=True):
        try:
            if test_failed:
                self.__collect_logs(cluster_run)
            self.__log.info("removing xdcr/nodes settings")
            rest = RestConnection(self.__master_node)
            rest.remove_all_remote_clusters()
            rest.remove_all_replications()
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
        finally:
            if cluster_shutdown:
                self.__clusterop.shutdown(force=True)

    # Buckets Operations
    def create_sasl_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH):
        # FIXME eviction_policy and bucket_priority needs to be configurable
        bucket_tasks = []
        for i in range(num_buckets):
            name = "sasl_bucket_" + str(i + 1)
            bucket_tasks.append(self.__clusterop.async_create_sasl_bucket(
                self.__master_node,
                name,
                'password',
                bucket_size,
                num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority))
            self.__buckets.append(
                Bucket(
                    name=name, authType="sasl", saslPassword="password",
                    num_replicas=num_replicas, bucket_size=bucket_size,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority
                ))

        for task in bucket_tasks:
            task.result()

    def create_standard_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH):
        bucket_tasks = []
        for i in range(num_buckets):
            name = "standard_bucket_" + str(i + 1)
            bucket_tasks.append(self.__clusterop.async_create_standard_bucket(
                self.__master_node,
                name,
                STANDARD_BUCKET_PORT + i,
                bucket_size,
                num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority))
            self.__buckets.append(
                Bucket(
                    name=name,
                    authType=None,
                    saslPassword=None,
                    num_replicas=num_replicas,
                    bucket_size=bucket_size,
                    port=STANDARD_BUCKET_PORT + i,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority
                ))

        for task in bucket_tasks:
            task.result()

    def create_default_bucket(
            self, bucket_size, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH
    ):
        self.__clusterop.create_default_bucket(
            self.__master_node,
            bucket_size,
            num_replicas,
            eviction_policy=eviction_policy,
            bucket_priority=bucket_priority
        )
        self.__buckets.append(
            Bucket(
                name=BUCKET_NAME.DEFAULT,
                authType="sasl",
                saslPassword="",
                num_replicas=num_replicas,
                bucket_size=bucket_size,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority
            ))

    def get_buckets(self):
        return self.__buckets

    def get_bucket(self, bucket_name):
        for bucket in self.__buckets:
            if bucket.name == bucket_name:
                return bucket

        raise Exception(
            "Bucket with name: %s no found on the cluster" %
            bucket_name)

    def delete_bucket(self, bucket_name):
        bucket_removed = self.get_bucket(bucket_name)
        self.__clusterop.bucket_delete(self.__master_node, bucket_removed.name)
        self.__buckets.remove(bucket_removed)

    # Load buckets
    def async_load_bucket(
            self,
            bucket,
            num_items,
            value_size=256,
            exp=0,
            kv_store=1,
            flag=0,
            only_store_hash=True,
            batch_size=1000,
            pause_secs=1,
            timeout_secs=30):

        self.__kv_gen = BlobGenerator(
            self.__name,
            self.__name,
            value_size,
            end=num_items)

        gen = copy.deepcopy(self.__kv_gen)
        task = self.__clusterop.async_load_gen_docs(
            self.__master_node,
            bucket.name,
            gen,
            bucket.kvs[kv_store],
            OPS.CREATE,
            exp,
            flag,
            only_store_hash,
            batch_size,
            pause_secs,
            timeout_secs)
        return task

    def load_bucket(
            self,
            bucket,
            num_items,
            value_size=256,
            exp=0,
            kv_store=1,
            flag=0,
            only_store_hash=True,
            batch_size=1000,
            pause_secs=1,
            timeout_secs=30):
        task = self.async_load_bucket(
            bucket,
            num_items,
            value_size,
            exp,
            kv_store,
            flag,
            only_store_hash,
            batch_size,
            pause_secs,
            timeout_secs)
        task.result()

    def async_load_all_buckets(
            self,
            num_items,
            value_size=256,
            exp=0,
            kv_store=1,
            flag=0,
            only_store_hash=True,
            batch_size=1000,
            pause_secs=1,
            timeout_secs=30):

        self.__kv_gen = BlobGenerator(
            self.__name,
            self.__name,
            value_size,
            end=num_items)
        tasks = []
        for bucket in self.__buckets:
            gen = copy.deepcopy(self.__kv_gen)
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node,
                    bucket.name, gen,
                    bucket.kvs[kv_store],
                    OPS.CREATE,
                    exp,
                    flag,
                    only_store_hash,
                    batch_size,
                    pause_secs,
                    timeout_secs)
            )
        return tasks

    def load_all_buckets(
            self,
            num_items,
            value_size=256,
            exp=0,
            kv_store=1,
            flag=0,
            only_store_hash=True,
            batch_size=1000,
            pause_secs=1,
            timeout_secs=30):
        tasks = self.async_load_all_buckets(
            num_items,
            value_size,
            exp,
            kv_store,
            flag,
            only_store_hash,
            batch_size,
            pause_secs,
            timeout_secs)
        for task in tasks:
            task.result()

    def load_all_buckets_till_dgm(
            self,
            active_resident_threshold,
            value_size=256,
            exp=0,
            kv_store=1,
            flag=0,
            only_store_hash=True,
            batch_size=1000,
            pause_secs=5,
            timeout_secs=60):
        # Load data till active_resident_threshold reached
        random_key = 0
        for bucket in self.__buckets:
            current_active_resident = StatsCommon.get_stats(
                [self.__master_node],
                bucket,
                '',
                'vb_active_perc_mem_resident')[self.__master_node]
            while int(current_active_resident) > active_resident_threshold:
                self.__log.info(
                    "resident ratio is %s greater than %s for %s in bucket %s.\
                    Continue loading to the cluster" % (
                        current_active_resident,
                        active_resident_threshold,
                        self.__master_node.ip, bucket.name))

                kv_gen = BlobGenerator(
                    "loadDgm-%s-" % random_key,
                    "loadDgm-%s-" % random_key,
                    value_size,
                    end=batch_size * 10)

                self.load_bucket(
                    bucket,
                    kv_gen,
                    OPS.CREATE,
                    exp=exp,
                    kv_store=kv_store,
                    flag=flag,
                    only_store_hash=only_store_hash,
                    batch_size=batch_size,
                    pause_secs=pause_secs,
                    timeout_secs=timeout_secs)

                random_key += 1
    # @param kv_gen = gen_create/gen_update/gen_delete
    # @param opt_type = OPS.CREATE/OPS.UPDATE/OPS.DELETE
    # @param expiration time for expire for update

    def async_update_delete(
            self, op_type, perc=30, expiration=0, kv_store=1):
        """Setting up creates/updates/deletes at source nodes"""
        raise_if(
            not self.__kv_gen,
            XDCRException(
                "Data is not loaded in cluster.Load data before update/delete")
        )
        if op_type == OPS.UPDATE:
            gen = BlobGenerator(
                self.__kv_gen.name,
                self.__kv_gen.seed,
                self.__kv_gen.value_size,
                start=0,
                end=int(self.__kv_gen.end * (float)(perc) / 100))
        elif op_type == OPS.DELETE:
            gen = BlobGenerator(
                self.__kv_gen.name,
                self.__kv_gen.seed,
                self.__kv_gen.value_size,
                start=int((self.__kv_gen.end) * (float)(
                    100 - perc) / 100),
                end=self.__kv_gen.end)
        else:
            raise XDCRException("Unknown op_type passed: %s" % op_type)
        tasks = []
        for bucket in self.__buckets:
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node,
                    bucket.name,
                    gen,
                    bucket.kvs[kv_store],
                    op_type,
                    expiration)
            )
        return tasks

    # @param kv_gen = gen_create/gen_update/gen_delete
    # @param ops = create/update/delete
    # @param expiration time for expire for update
    # @param wait_for_expiration = True if wait for items to expire
    def update_delete_data(
            self, op_type, perc=30, expiration=0, wait_for_expiration=True):

        tasks = self.async_update_delete(op_type, perc, expiration)

        [task.result() for task in tasks]

        if wait_for_expiration and expiration:
            self.__sleep(expiration, "Waiting for expiration of updated items")

    def run_expiry_pager(self, val=10):
        for bucket in self.__buckets:
            ClusterOperationHelper.flushctl_set(
                self.__master_node,
                "exp_pager_stime",
                val,
                bucket)
            self.__log.info("wait for expiry pager to run on all these nodes")

    # Views Operations
    def async_create_views(
            self, design_doc_name, views, bucket=BUCKET_NAME.DEFAULT):
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
        task = self.__clusterop.async_compact_view(
            self.__master_node,
            design_doc_name,
            bucket,
            with_rebalance)
        return task

    def disable_compaction(self, bucket=BUCKET_NAME.DEFAULT):
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.__clusterop.modify_fragmentation_config(
            self.__master_node,
            new_config,
            bucket)

    def async_monitor_view_fragmentation(self,
                                         design_doc_name,
                                         fragmentation_value,
                                         bucket=BUCKET_NAME.DEFAULT):
        task = self.__clusterop.async_monitor_view_fragmentation(
            self.__master_node,
            design_doc_name,
            fragmentation_value,
            bucket)
        return task

    def async_query_view(
            self, design_doc_name, view_name, query,
            expected_rows=None, bucket="default", retry_time=2):
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
        task = self.__clusterop.async_query_view(
            self.__master_node,
            design_doc_name,
            view_name,
            query,
            expected_rows,
            bucket=bucket, retry_time=retry_time)
        task.result(timeout)

    # Rebalance-Out
    def __async_rebalance_out(self, master=False, num_nodes=1):
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

    # Rebalance-in
    def async_rebalance_in(self, num_nodes=1):
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
        task = self.async_rebalance_in(num_nodes)
        task.result()

    # Swap-Rebalance
    def __async_swap_rebalance(self, master=False):
        if master:
            to_remove_node = [self.__master_node]
        else:
            # TODO add assert if number of free servers are less than required
            # TODO add assert in case of only one node server
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
        task = self.__async_swap_rebalance(master=True)
        task.result()

    def swap_rebalance(self):
        task = self.__async_swap_rebalance()
        task.result()

    # Failover nodes
    def __async_failover(self, master=False, num_nodes=1, graceful=False):
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

        [self.__nodes.remove(node) for node in self.__fail_over_nodes]

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def async_failover_master(self, graceful=False):
        return self.__async_failover(master=True, graceful=graceful)

    def async_failover(self, num_nodes=1, graceful=False):
        return self.__async_failover(num_nodes=num_nodes, graceful=graceful)

    def failover_master(self, graceful=False, rebalance=True):
        task = self.__async_failover(master=True, graceful=graceful)
        task.result()
        if rebalance:
            self.__clusterop.rebalance(
                self.__nodes,
                [],
                self.__fail_over_nodes)
            self.__fail_over_nodes = []

    def failover_nodes(self, num_nodes=1, graceful=False, rebalance=True):
        task = self.__async_failover(
            master=False,
            num_nodes=num_nodes,
            graceful=graceful)
        task.result()
        if rebalance:
            self.__clusterop.rebalance(
                self.__nodes,
                [],
                self.__fail_over_nodes)
            self.__fail_over_nodes = []

    # Add-back
    # @param recovery_type=delta/full
    def add_back_node(self, recovery_type=None):
        raise_if(
            len(self.__fail_over_nodes) < 1,
            XDCRException("No failover nodes available to add_back")
        )
        for failover_node in self.__fail_over_nodes:
            rest = RestConnection(self.__master_node)
            rest.add_back_node(failover_node.id)
            if recovery_type:
                rest.set_recovery_type(
                    otpNode=failover_node.id,
                    recoveryType=recovery_type)
        [self.__nodes.append(node) for node in self.__fail_over_nodes]
        # FIXME: Passing all buckets as delta recovery nodes
        rest.rebalance(
            otpNodes=[
                node.id for node in self.__nodes],
            ejectedNodes=[],
            deltaRecoveryBuckets=self.__buckets)
        self.__fail_over_nodes = []

    # Warm-up
    def warmp_node(self, master=False):
        if master:
            NodeHelper.do_a_warm_up(self.__master_node)
        else:
            NodeHelper.do_a_warm_up(self.__nodes[-1])

    # XDCR Operations
    def set_xdcr_param(self, param, value):
        RestConnection(self.__master_node).set_internalSetting(param, value)

    def get_xdcr_stat(self, bucket_name, param):
        return int(RestConnection(self.__master_node).fetch_bucket_stats(
            bucket_name)
            ['op']['samples'][param][-1])

    def wait_for_xdcr_stat(self, bucket, stat, comparison, value):
        task = self.__clusterop.async_wait_for_xdcr_stat(
            self.__nodes,
            bucket,
            '',
            stat,
            comparison,
            value)
        task.result()

    def add_remote_cluster(self, dest_cluster, name, encryption=False):
        remote_cluster = XDCRRemoteClusterRef(
            self,
            dest_cluster,
            name,
            encryption
        )
        remote_cluster.add()
        self.__remote_clusters.append(remote_cluster)

    # add params to what to modify
    def modify_remote_cluster(self, remote_cluster_name, require_encryption):
        for remote_cluster in self.__remote_clusters:
            if remote_cluster_name == remote_cluster.get_name():
                remote_cluster.set_encryption(require_encryption)
                break
        else:
            raise XDCRException(
                "No such remote cluster found with name: {0}".format(
                    remote_cluster_name))

    def wait_for_flusher_empty(self, timeout=60):
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

    def verify_items_count(self, timeout=60):
        stats_tasks = []
        for bucket in self.__buckets:
            items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            for stat in ['curr_items', 'vb_active_curr_items']:
                stats_tasks.append(self.__clusterop.async_wait_for_stats(
                    self.__nodes, bucket, '',
                    stat, '==', items))
            if bucket.numReplicas >= 1 and len(self.__nodes) > 1:
                stats_tasks.append(self.__clusterop.async_wait_for_stats(
                    self.__nodes, bucket, '',
                    'vb_replica_curr_items', '==', items * bucket.numReplicas))
        for task in stats_tasks:
            task.result(timeout)

    def verify_data(self, kv_store=1, timeout=None,
                    max_verify=None, only_store_hash=True, batch_size=1000):
        self.__data_verified = False
        tasks = []
        for bucket in self.__buckets:
            tasks.append(
                self.__clusterop.async_verify_data(
                    self.__master_node,
                    bucket,
                    bucket.kvs[kv_store],
                    max_verify,
                    only_store_hash,
                    batch_size,
                    timeout_sec=60))
        for task in tasks:
            task.result(timeout)

        self.__data_verified = True

    def wait_for_outbound_mutations(self, timeout=180):
        self.__log.info(
            "Waiting for Outbound mutation to be zero on cluster node: %s" %
            self.__master_node.ip)
        curr_time = time.time()
        end_time = curr_time + timeout
        rest = RestConnection(self.__master_node)
        while curr_time < end_time:
            found = 0
            for bucket in self.__buckets:
                try:
                    mutations = int(rest.get_xdc_queue_size(bucket.name))
                except KeyError:
                    # Sometimes replication_changes_left are not found in the stat.
                    # So setting up -1 if not found sometimes.
                    mutations = -1
                self.__log.info(
                    "Current Outbound mutations on cluster node: %s for bucket %s is %s" %
                    (self.__master_node.ip, bucket.name, mutations))
                if mutations == 0:
                    found = found + 1
            if found == len(self.__buckets):
                break
            time.sleep(10)
            end_time = end_time - 10
        else:
            # MB-9707: Updating this code from fail to warning to avoid test
            # to abort, as per this
            # bug, this particular stat i.e. replication_changes_left is buggy.
            self.__log.error(
                "Timeout occurs while waiting for mutations to be replicated")
            return False
        return True


class Utility:

    @staticmethod
    def make_default_views(prefix, num_views, is_dev_ddoc=False):
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        default_view_name = (prefix, "default_view")[prefix is None]
        return [View(default_view_name + str(i), default_map_func,
                     None, is_dev_ddoc) for i in xrange(num_views)]

    @staticmethod
    def get_rc_name(src_cluster_name, dest_cluster_name):
        return "remote_cluster_" + src_cluster_name + "-" + dest_cluster_name


class XDCRNewBaseTest(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.__cb_clusters = []
        self.log = logger.Logger.get_logger()
        self._input = TestInputSingleton.input
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

        if not hasattr(self, 'cluster'):
            self.cluster = Cluster()
        self.__init_parameters()
        self.log.info(
            "==== XDCRNewbasetests setup is started for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

        use_hostanames = self._input.param("use_hostnames", False)
        counter = 1
        for _, nodes in self._input.clusters.iteritems():
            if self.__chain_length and len(
                    self.__cb_clusters) > self.__chain_length:
                break
            self.__cb_clusters.append(
                CouchbaseCluster(
                    "C%s" % counter, nodes,
                    self.log, use_hostanames))
            counter += 1

        self.__cleanup_previous()
        self.__init_clusters()
        self.__create_buckets()
        self.__set_free_servers()
        self.log.info(
            "==== XDCRNewbasetests setup is finished for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

    def tearDown(self):
        # Collect Logs files
        # Shutdown cluster
        # Cluster cleanup
        try:
            test_failed = (hasattr(self, '_resultForDoCleanups') and
                           len(self._resultForDoCleanups.failures or self._resultForDoCleanups.errors)) \
                or (hasattr(
                    self, '_exc_info') and self._exc_info()[1] is not None)

            if test_failed and (str(self.__class__).find(
                    'upgradeXDCR') != -1 or TestInputSingleton.input.param("stop-on-failure", False)):
                self.log.warn("CLEANUP WAS SKIPPED")
                return
            self.log.info(
                "====  XDCRNewbasetests cleanup is started for test #{0} {1} ===="
                .format(self.__case_number, self._testMethodName))
            cluster_run = len(
                set([server.ip for server in self._input.servers])) == 1
            for cb_cluster in self.__cb_clusters:
                cb_cluster.cleanup_cluster(
                    self,
                    test_failed=test_failed,
                    cluster_run=cluster_run)
            self.log.info(
                "====  XDCRNewbasetests cleanup is finished for test #{0} {1} ==="
                .format(self.__case_number, self._testMethodName))
        finally:
            self.cluster.shutdown(force=True)
            unittest.TestCase.tearDown(self)

    def __init_parameters(self):
        self.__case_number = self._input.param("case_number", 0)
        self.__num_items = self._input.param("items", 1000)
        self.__value_size = self._input.param("value_size", 256)
        self.__percent_update = self._input.param("upd", 30)
        self.__percent_delete = self._input.param("del", 30)
        self.__topology = self._input.param("ctopology", TOPOLOGY.CHAIN)
        self.__chain_length = self._input.param("chain_length", None)
        self.__rdirection = self._input.param(
            "rdirection",
            REPLICATION_DIRECTION.UNIDIRECTION)
        self.__demand_encryption = self._input.param(
            "demand_encryption",
            False)
        self.__rep_type = self._input.param(
            "replication_type",
            REPLICATION_PROTOCOL.CAPI)

    def __cleanup_previous(self):
        for cluster in self.__cb_clusters:
            cluster.cleanup_cluster(self, cluster_shutdown=False)

    def __init_clusters(self):
        self.log.info("Initializing all clusters...")
        disabled_consistent_view = self._input.param(
            "disabled_consistent_view",
            None)
        for cluster in self.__cb_clusters:
            cluster.init_cluster(disabled_consistent_view)

    def get_cb_cluster_from_name(self, name):
        for cb_cluster in self.__cb_clusters:
            if cb_cluster.get_name() == name:
                return cb_cluster
        raise XDCRException("Couchbase Cluster with name: %s not exist" % name)

    def get_num_cb_cluster(self):
        return len(self.__cb_clusters)

    def __create_buckets(self):
        num_sasl_buckets = self._input.param("sasl_buckets", 0)
        num_stand_buckets = self._input.param("standard_buckets", 0)
        create_default_bucket = self._input.param("default_bucket", True)
        num_replicas = self._input.param("replicas", 1)
        dgm_run = self._input.param("dgm_run", 1)
        eviction_policy = self._input.param(
            "eviction_policy",
            'valueOnly')  # or 'fullEviction'
        mixed_priority = self._input.param("mixed_priority", None)
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        if mixed_priority:
            bucket_priority = 'high'
        else:
            bucket_priority = None
        num_buckets = num_sasl_buckets + \
            num_stand_buckets + int(create_default_bucket)

        for cb_cluster in self.__cb_clusters:
            total_quota = cb_cluster.get_mem_quota()
            if dgm_run:
                total_quota = 256
            bucket_size = int(float(total_quota) / float(num_buckets))

            if create_default_bucket:
                cb_cluster.create_default_bucket(
                    bucket_size,
                    num_replicas,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority)

            cb_cluster.create_sasl_buckets(
                bucket_size, num_buckets=num_sasl_buckets,
                num_replicas=num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority)

            cb_cluster.create_standard_buckets(
                bucket_size, num_buckets=num_stand_buckets,
                num_replicas=num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority)

    def __set_free_servers(self):
        total_servers = self._input.servers
        cluster_nodes = []
        for _, nodes in self._input.clusters.iteritems():
            cluster_nodes.extend(nodes)

        FloatingServers._serverlist = [
            server for server in total_servers if server not in cluster_nodes]

    """Will Setup Remote Cluster Chain Topology i.e. A -> B -> C
    """

    def __set_topology_chain(self):
        for i, cb_cluster in enumerate(self.__cb_clusters):
            if i >= len(self.__cb_clusters) - 1:
                break
            cb_cluster.add_remote_cluster(
                self.__cb_clusters[i + 1],
                Utility.get_rc_name(
                    cb_cluster.get_name(),
                    self.__cb_clusters[i + 1].get_name()),
                self.__demand_encryption
            )
            if self.__rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                self.__cb_clusters[i + 1].add_remote_cluster(
                    cb_cluster,
                    Utility.get_rc_name(
                        self.__cb_clusters[i + 1].get_name(),
                        cb_cluster.get_name()),
                    self.__demand_encryption
                )

    """Will Setup Remote Cluster Star Topology i.e.
              B
              |
              A
            /   \
           D     C
    """

    def __set_topology_star(self):
        hub = self.__cb_clusters[0]
        for cb_cluster in self.__cb_clusters[1:]:
            hub.add_remote_cluster(
                cb_cluster,
                Utility.get_rc_name(hub.get_name(), cb_cluster.get_name()),
                self.__demand_encryption
            )
            if self.__rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                cb_cluster.add_remote_cluster(
                    hub,
                    Utility.get_rc_name(cb_cluster.get_name(), hub.get_name()),
                    self.__demand_encryption
                )

    """
    Will Setup Remote Cluster Ring Topology i.e. A -> B -> C -> A
    """

    def __set_topology_ring(self):
        self.__set_topology_chain()
        self.__cb_clusters[-1].add_remote_cluster(
            self.__cb_clusters[0],
            Utility.get_rc_name(
                self.__cb_clusters[-1].get_name(),
                self.__cb_clusters[0].get_name()),
            self.__demand_encryption
        )
        if self.__rdirection == REPLICATION_DIRECTION.BIDIRECTION:
            self.__cb_clusters[0].add_remote_cluster(
                self.__cb_clusters[-1],
                Utility.get_rc_name(
                    self.__cb_clusters[0].get_name(),
                    self.__cb_clusters[-1].get_name()),
                self.__demand_encryption
            )

    def set_xdcr_topology(self):
        if self.__topology == TOPOLOGY.CHAIN:
            self.__set_topology_chain()
        elif self.__topology == TOPOLOGY.STAR:
            self.__set_topology_star()
        elif self.__topology == TOPOLOGY.RING:
            self.__set_topology_ring()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    """Hybrid Topology
    Notations:
        '> or <' for Unidirection between clusters
        '<>' for Bi-direction between clusters
    User Input:  topology="C1>C2<>C3>C4<>C1"
    """

    def __parse_topology_param(self):
        import re
        tokens = re.split(r'(>|<>|<|\s)', self.__topology)
        return tokens

    def set_hybrid_topology(self):
        tokens = self.__parse_topology_param()
        counter = 0
        while counter < len(tokens) - 1:
            src_cluster = self.get_cb_cluster_from_name(tokens[counter])
            dest_cluster = self.get_cb_cluster_from_name(tokens[counter + 2])
            if ">" in tokens[counter + 1]:
                src_cluster.add_remote_cluster(
                    dest_cluster,
                    Utility.get_rc_name(
                        src_cluster.get_name(),
                        dest_cluster.get_name()),
                    self.__demand_encryption
                )
            if "<" in tokens[counter + 1]:
                dest_cluster.add_remote_cluster(
                    src_cluster,
                    Utility.get_rc_name(
                        dest_cluster.get_name(), src_cluster.get_name()),
                    self.__demand_encryption
                )
            counter += 2

    def __load_chain(self):
        for i, cluster in enumerate(self.__cb_clusters):
            if i >= len(self.__cb_clusters) - 1:
                break
            cluster.load_all_buckets(self.__num_items, self.__value_size)

    def __load_star(self):
        hub = self.__cb_clusters[0]
        hub.load_all_buckets(self.__num_items, self.__value_size)

    def __load_ring(self):
        self.__load_chain()
        cluster = self.__cb_clusters[-1]
        cluster.load_all_buckets(self.__num_items, self.__value_size)

    """load data as per topology
    """

    def load_data_topology(self):
        if self.__topology == TOPOLOGY.CHAIN:
            self.__load_chain()
        elif self.__topology == TOPOLOGY.STAR:
            self.__load_star()
        elif self.__topology == TOPOLOGY.RING:
            self.__load_ring()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    def setup_all_replications(self):
        for cb_cluster in self.__cb_clusters:
            for remote_cluster in cb_cluster.get_remote_clusters():
                for src_bucket in remote_cluster.get_src_cluster().get_buckets():
                    remote_cluster.create_replication(
                        src_bucket,
                        rep_type=self.__rep_type,
                        toBucket=remote_cluster.get_dest_cluster().get_bucket(
                            src_bucket.name))
                remote_cluster.start_all_replications()

    def verify_rev_ids(self, xdcr_replications, kv_store=1):
        error_count = 0
        tasks = []
        for repl in xdcr_replications:
            self.log.info("Verifying RevIds for {0} -> {1}".format(
                repl.get_src_cluster(),
                repl.get_dest_cluster()))
            task_info = self.cluster.async_verify_revid(
                repl.get_src_cluster().get_master_node(),
                repl.get_dest_cluster().get_master_node(),
                repl.get_src_bucket(),
                repl.get_src_bucket().kvs[kv_store])
            tasks.append(task_info)
        for task in tasks:
            task.result()
            error_count += task.err_count
            if task.err_count:
                for ip, values in task.keys_not_found.iteritems():
                    if values:
                        self.log.error("%s keys not found on %s:%s" %
                                       (len(values), ip, values))
        return error_count

    def __merge_keys(self, kv_src_bucket, kv_dest_bucket, kvs_num=1):
        valid_keys_src, deleted_keys_src = kv_src_bucket[
            kvs_num].key_set()
        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()

        for key in valid_keys_src:
            # replace/add the values for each key in src kvs
            if key not in valid_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
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
                kv_dest_bucket[kvs_num].acquire_partition(key).delete(key)
                kv_dest_bucket[kvs_num].release_partition(key)

    def __merge_all_buckets(self):
        # In case of ring topology first merging keys from last and first
        # cluster e.g. A -> B -> C -> A then merging C-> A then A-> B and B-> C
        # and C->A (to merge keys from B ->C).
        # FIXME need to be tested for Hybrid Topology
        if self.__topology == TOPOLOGY.RING and len(
                self.__cb_clusters) > 2:
            for remote_cluster_ref in self.__cb_clusters[-1].get_remote_clusters():
                for repl in remote_cluster_ref.get_replications():
                    self.__merge_keys(
                        repl.get_src_bucket().kvs,
                        repl.get_dest_bucket().kvs,
                        kvs_num=1
                    )

        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                for repl in remote_cluster_ref.get_replications():
                    self.__merge_keys(
                        repl.get_src_bucket().kvs,
                        repl.get_dest_bucket().kvs,
                        kvs_num=1
                    )

    def verify_results(self):
        self.__merge_all_buckets()
        for cb_cluster in self.__cb_clusters:
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                try:
                    src_cluster = remote_cluster_ref.get_src_cluster()
                    dest_cluster = remote_cluster_ref.get_dest_cluster()
                    src_cluster.run_expiry_pager()
                    dest_cluster.run_expiry_pager()

                    src_cluster.wait_for_flusher_empty()
                    dest_cluster.wait_for_flusher_empty()

                    src_cluster.wait_for_outbound_mutations()
                    dest_cluster.wait_for_outbound_mutations()

                    src_cluster.verify_items_count()
                    dest_cluster.verify_items_count()

                    src_cluster.verify_data()
                    dest_cluster.verify_data()
                finally:
                    self.verify_rev_ids(remote_cluster_ref.get_replications())
