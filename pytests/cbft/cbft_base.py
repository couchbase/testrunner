import unittest
import time
import copy
import logger
import logging

from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import ServerUnavailableException
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from testconstants import STANDARD_BUCKET_PORT
from couchbase_helper.document import View
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.stats_tools import StatsCommon
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from TestInput import TestInputSingleton
from scripts.collect_server_info import cbcollectRunner
from scripts import collect_data_files

from couchbase_helper.documentgenerator import JsonDocGenerator
from lib.membase.api.exception import CBFTException
from security.auditmain import audit


class RenameNodeException(CBFTException):

    """Exception thrown when converting ip to hostname failed
    """

    def __init__(self, msg=''):
        CBFTException.__init__(self, msg)


class RebalanceNotStopException(CBFTException):

    """Exception thrown when stopping rebalance failed
    """

    def __init__(self, msg=''):
        CBFTException.__init__(self, msg)


def raise_if(cond, ex):
    """Raise Exception if condition is True
    """
    if cond:
        raise ex


class OPS:
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    APPEND = "append"


class EVICTION_POLICY:
    VALUE_ONLY = "valueOnly"
    FULL_EVICTION = "fullEviction"


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


class CHECK_AUDIT_EVENT:
    CHECK = False

# Event Definition:
# https://github.com/couchbase/goxdcr/blob/master/etc/audit_descriptor.json

class NodeHelper:
    _log = logger.Logger.get_logger()

    @staticmethod
    def disable_firewall(server):
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
            o, r = shell.execute_command(
                "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
            shell.log_command_output(o, r)
            # self.log.info("enabled firewall on {0}".format(server))
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def reboot_server(server, test_case, wait_timeout=60):
        """Reboot a server and wait for couchbase server to run.
        @param server: server object, which needs to be rebooted.
        @param test_case: test case object, since it has assert() function
                        which is used by wait_for_ns_servers_or_assert
                        to throw assertion.
        @param wait_timeout: timeout to whole reboot operation.
        """
        # self.log.info("Rebooting server '{0}'....".format(server.ip))
        shell = RemoteMachineShellConnection(server)
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            o, r = shell.execute_command(
                "{0} -r -f -t 0".format(COMMAND.SHUTDOWN))
        elif shell.extract_remote_info().type.lower() == OS.LINUX:
            o, r = shell.execute_command(COMMAND.REBOOT)
        shell.log_command_output(o, r)
        # wait for restart and warmup on all server
        time.sleep(wait_timeout * 5)
        # disable firewall on these nodes
        NodeHelper.disable_firewall(server)
        # wait till server is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [server],
            test_case,
            wait_if_warmup=True)

    @staticmethod
    def enable_firewall(server):
        """Enable firewall
        @param server: server object to enable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        RemoteUtilHelper.enable_firewall(
            server)

    @staticmethod
    def do_a_warm_up(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        time.sleep(5)
        shell.start_couchbase()
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
        while num < wait_time / 10:
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
    def get_log_dir(node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval('filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)

    @staticmethod
    def check_cbft_log(server, str):
        """ Checks if a string 'str' is present in goxdcr.log on server
            and returns the number of occurances
        """
        shell = RemoteMachineShellConnection(server)
        cbft_log = NodeHelper.get_log_dir(server) + '/cbft.log*'
        count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                        format(str, cbft_log))
        if isinstance(count, list):
            count = int(count[0])
        else:
            count = int(count)
        NodeHelper._log.info(count)
        shell.disconnect()
        return count

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
    def get_cbcollect_info(server):
        """Collect cbcollectinfo logs for all the servers in the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        print "grabbing cbcollect from {0}".format(server.ip)
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
            TestInputSingleton.input.test_params[
                "get-cbcollect-info"] = False
        except Exception as e:
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
    def collect_logs(server, cluster_run=False):
        """Grab cbcollect before we cleanup
        """
        NodeHelper.get_cbcollect_info(server)
        if not cluster_run:
            NodeHelper.collect_data_files(server)


class FloatingServers:

    """Keep Track of free servers, For Rebalance-in
    or swap-rebalance operations.
    """
    _serverlist = []


class CouchbaseCluster:

    def __init__(self, name, nodes, log, use_hostname=False):
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
        self.__remote_clusters = []
        self.__clusterop = Cluster()
        self.__kv_gen = {}

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

    def __init_nodes(self):
        """Initialize all nodes. Rename node to hostname
        if needed by test.
        """
        tasks = []
        for node in self.__nodes:
            tasks.append(
                self.__clusterop.async_init_node(
                    node))
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

    def get_nodes(self):
        return self.__nodes

    def get_name(self):
        return self.__name

    def get_cluster(self):
        return self.__clusterop

    def get_kv_gen(self):
        raise_if(
            self.__kv_gen is None,
            CBFTException(
                "KV store is empty on couchbase cluster: %s" %
                self))
        return self.__kv_gen

    def init_cluster(self):
        """Initialize cluster.
        1. Initialize all nodes.
        2. Add all nodes to the cluster..
        """
        self.__init_nodes()
        self.__clusterop.async_rebalance(
            self.__nodes,
            self.__nodes[1:],
            [],
            use_hostnames=self.__use_hostname).result()

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
            self.__log.info("removing nodes from cluster ...")
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

    def create_sasl_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH):
        """Create sasl buckets.
        @param bucket_size: size of the bucket.
        @param num_buckets: number of buckets to create.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
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
        """Create standard buckets.
        @param bucket_size: size of the bucket.
        @param num_buckets: number of buckets to create.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
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
        """Create default bucket.
        @param bucket_size: size of the bucket.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
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

    def async_load_bucket(self, bucket, num_items, exp=0,
                          kv_store=1, flag=0, only_store_hash=True,
                          batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on given bucket. Function don't wait for
        load data to finish, return immidiately.
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
        seed = "%s-key-" % self.__name
        self.__kv_gen[OPS.CREATE] = JsonDocGenerator(seed,
                                                     encoding="utf-8",
                                                     start=0,
                                                     end=num_items)

        gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
        task = self.__clusterop.async_load_gen_docs(
            self.__master_node, bucket.name, gen, bucket.kvs[kv_store],
            OPS.CREATE, exp, flag, only_store_hash, batch_size, pause_secs,
            timeout_secs)
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

    def async_load_all_buckets(self, num_items, exp=0,
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
        prefix = "%s-" % self.__name
        self.__kv_gen[OPS.CREATE] = JsonDocGenerator(prefix,
                                                     encoding="utf-8",
                                                     start=0,
                                                     end=num_items)
        tasks = []
        for bucket in self.__buckets:
            gen = copy.deepcopy(self.__kv_gen[OPS.CREATE])
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, gen, bucket.kvs[kv_store],
                    OPS.CREATE, exp, flag, only_store_hash, batch_size,
                    pause_secs, timeout_secs)
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
            num_items, exp, kv_store, flag, only_store_hash,
            batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()



    def load_all_buckets_till_dgm(self, active_resident_threshold, items=0,
                                  value_size=512, exp=0, kv_store=1, flag=0,
                                  only_store_hash=True, batch_size=1000,
                                  pause_secs=1, timeout_secs=30):
        """Load data synchronously on all buckets till dgm (Data greater than memory)
        for given active_resident_threshold
        @param active_resident_threshold: Dgm threshold.
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        items = int(items)
        self.__log.info("First loading \"items\" {0} number keys to handle "
                      "update/deletes in dgm cases".format(items))
        self.load_all_buckets(items)

        self.__log.info("Now loading extra keys to reach dgm limit")
        seed = "%s-key-" % self.__name
        end = 0
        for bucket in self.__buckets:
            current_active_resident = StatsCommon.get_stats(
                [self.__master_node],
                bucket,
                '',
                'vb_active_perc_mem_resident')[self.__master_node]
            start = items
            end = start + batch_size * 10
            while int(current_active_resident) > active_resident_threshold:
                self.__log.info("loading %s keys ..." % (end-start))

                kv_gen = JsonDocGenerator(seed,
                                          encoding="utf-8",
                                          start=start,
                                          end=end)

                tasks = []
                tasks.append(self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, kv_gen, bucket.kvs[kv_store],
                    OPS.CREATE, exp, flag, only_store_hash, batch_size,
                    pause_secs, timeout_secs))

                for task in tasks:
                    task.result()
                start = end
                end = start + batch_size * 10
                current_active_resident = StatsCommon.get_stats(
                    [self.__master_node],
                    bucket,
                    '',
                    'vb_active_perc_mem_resident')[self.__master_node]
                self.__log.info(
                    "Current resident ratio: %s, desired: %s bucket %s" % (
                        current_active_resident,
                        active_resident_threshold,
                        bucket.name))
            self.__log.info("Loaded a total of %s keys into bucket %s"
                            % (end,bucket.name))
        self.__kv_gen[OPS.CREATE] = JsonDocGenerator(seed,
                                                    encoding="utf-8",
                                                    start=0,
                                                    end=end)

    def async_update_delete(
            self, op_type, perc=30, expiration=0, kv_store=1):
        """Perform update/delete operation on all buckets. Function don't wait
        operation to finish.
        @param op_type: OPS.CREATE/OPS.UPDATE/OPS.DELETE
        @param perc: percentage of data to be deleted or created
        @param expiration: time for expire items
        @return: task object list
        """
        raise_if(
            OPS.CREATE not in self.__kv_gen,
            CBFTException(
                "Data is not loaded in cluster.Load data before update/delete")
        )
        tasks = []
        for bucket in self.__buckets:
            if op_type == OPS.UPDATE:
                self.__kv_gen[OPS.UPDATE] = JsonDocGenerator(
                                                self.__kv_gen[OPS.CREATE].name,
                                                encoding="utf-8",
                                                start=0,
                                                end=int(self.__kv_gen[OPS.CREATE].end
                                                        * (float)(perc) / 100))
                gen = copy.deepcopy(self.__kv_gen[OPS.UPDATE])
            elif op_type == OPS.DELETE:
                self.__kv_gen[OPS.DELETE] = JsonDocGenerator(
                                                self.__kv_gen[OPS.CREATE].name,
                                                encoding="utf-8",
                                                start=int((self.__kv_gen[OPS.CREATE].end)
                                                          * (float)(100 - perc) / 100),
                                                end=self.__kv_gen[OPS.CREATE].end)
                gen = copy.deepcopy(self.__kv_gen[OPS.DELETE])
            else:
                raise CBFTException("Unknown op_type passed: %s" % op_type)

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
                    batch_size=1000)
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
            CBFTException(
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

    def async_rebalance_in(self, num_nodes=1):
        """Rebalance-in nodes into Cluster asynchronously
        @param num_nodes: number of nodes to rebalance-in to cluster.
        """
        raise_if(
            len(FloatingServers._serverlist) < num_nodes,
            CBFTException(
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
            CBFTException(
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

    def failover_and_rebalance_master(self, graceful=False, rebalance=True):
        """Failover master node
        @param graceful: True if graceful failover else False
        @param rebalance: True if do rebalance operation after failover.
        """
        task = self.__async_failover(master=True, graceful=graceful)
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
            CBFTException("No failover nodes available to add_back")
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

    def restart_couchbase_on_all_nodes(self):
        for node in self.__nodes:
            NodeHelper.do_a_warm_up(node)

        NodeHelper.wait_warmup_completed(self.__nodes)


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

    def verify_items_count(self, timeout=300):
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
            items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            while True:
                try:
                    active_keys = int(rest.get_active_key_count(bucket.name))
                    if active_keys != items:
                        self.__log.warn("Not Ready: vb_active_curr_items %s == "
                                "%s expected on %s, %s bucket"
                                 % (active_keys, items, self.__name, bucket.name))
                        time.sleep(5)
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
                except Exception as e:
                    self.__log.error(e)

        # check replica count
        curr_time = time.time()
        end_time = curr_time + timeout
        buckets = copy.copy(self.get_buckets())

        for bucket in buckets:
            if len(self.__nodes) > 1:
                items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
                items = items * bucket.numReplicas
            else:
                items = 0
            while True:
                try:
                    replica_keys = int(rest.get_replica_key_count(bucket.name))
                    if replica_keys != items:
                        self.__log.warn("Not Ready: vb_replica_curr_items %s == "
                                "%s expected on %s, %s bucket"
                                 % (replica_keys, items ,self.__name, bucket.name))
                        time.sleep(3)
                        if time.time() > end_time:
                            self.__log.error(
                            "ERROR: Timed-out waiting for replica item count to match")
                            replica_key_count_passed = False
                            break
                        continue
                    else:
                        self.__log.info("Saw: vb_replica_curr_items %s == "
                                "%s expected on %s, %s bucket"
                                % (replica_keys, items, self.__name, bucket.name))
                        break
                except Exception as e:
                    self.__log.error(e)
        return active_key_count_passed, replica_key_count_passed

    def verify_data(self, kv_store=1, timeout=None,
                    max_verify=None, only_store_hash=True, batch_size=1000):
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
                     None, is_dev_ddoc) for i in xrange(num_views)]

    @staticmethod
    def get_rc_name(src_cluster_name, dest_cluster_name):
        return "remote_cluster_" + src_cluster_name + "-" + dest_cluster_name


class CBFTBaseTest(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self._input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()
        self.__init_logger()
        self.__cluster_op = Cluster()
        self.__init_parameters()
        self.log.info(
            "==== CBFTbasetests setup is started for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

        self.__setup_for_test()

        self.log.info(
            "==== CBFTbasetests setup is finished for test #{0} {1} ===="
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
        return len(set([server.ip for server in self._input.servers])) == 1

    def tearDown(self):
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
                raise CBFTException("Negative test passed!")

        # collect logs before tearing down clusters
        if self._input.param("get-cbcollect-info", False) and \
                self.__is_test_failed():
            for server in self._input.servers:
                self.log.info("Collecting logs @ {0}".format(server.ip))
                NodeHelper.collect_logs(server, self.__is_cluster_run())

        try:
            if self.__is_cleanup_needed():
                self.log.warn("CLEANUP WAS SKIPPED")
                return
            self.log.info(
                "====  CBFTbasetests cleanup is started for test #{0} {1} ===="
                .format(self.__case_number, self._testMethodName))
            self._cb_cluster.cleanup_cluster(self)
            self.log.info(
                "====  CBFTbasetests cleanup is finished for test #{0} {1} ==="
                .format(self.__case_number, self._testMethodName))
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

    def __setup_for_test(self):
        use_hostanames = self._input.param("use_hostnames", False)
        print self._input.clusters
        nodes =  self._input.clusters[0]
        cluster_nodes = copy.deepcopy(nodes)
        print cluster_nodes
        self._cb_cluster = CouchbaseCluster("C1",
                                             cluster_nodes,
                                             self.log,
                                             use_hostanames)
        self._master = self._cb_cluster.get_master_node()
        self.__cleanup_previous()
        self.__init_clusters()
        self.__set_free_servers()
        self.__create_buckets()

    def __init_parameters(self):
        self.__case_number = self._input.param("case_number", 0)
        self.__num_sasl_buckets = self._input.param("sasl_buckets", 0)
        self.__num_stand_buckets = self._input.param("standard_buckets", 0)
        self.__eviction_policy = self._input.param("eviction_policy",'valueOnly')
        self.__mixed_priority = self._input.param("mixed_priority", None)

        self.__fail_on_errors = self._input.param("fail_on_errors", False)
        # simply append to this list, any error from log we want to fail test on
        self.__report_error_list = []
        if self.__fail_on_errors:
            self.__report_error_list = [""]

        # for format {ip1: {"panic": 2}}
        self.__error_count_dict = {}
        if len(self.__report_error_list) > 0:
            self.__initialize_error_count_dict()

        # Public init parameters - Used in other tests too.
        # Move above private to this section if needed in future, but
        # Ensure to change other tests too.

        self._num_replicas = self._input.param("replicas", 1)
        self._create_default_bucket = self._input.param("default_bucket",True)
        self._num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 512)
        self._poll_timeout = self._input.param("poll_timeout", 120)
        self._update = self._input.param("update", "False")
        self._delete = self._input.param("delete", "False")
        self._perc_upd = self._input.param("upd", 30)
        self._perc_del = self._input.param("del", 30)
        self._expires = self._input.param("expires", 0)
        self._wait_for_expiration = self._input.param(
            "wait_for_expiration",
            True)
        self._warmup = self._input.param("warm", "")
        self._rebalance = self._input.param("rebalance", "")
        self._failover = self._input.param("failover", "")
        self._wait_timeout = self._input.param("timeout", 60)
        self._disable_compaction = self._input.param("disable_compaction","")
        self._item_count_timeout = self._input.param("item_count_timeout", 300)
        self._dgm_run = self._input.param("dgm_run", False)
        self._active_resident_threshold = \
            self._input.param("active_resident_threshold", 100)
        CHECK_AUDIT_EVENT.CHECK = self._input.param("verify_audit", 0)
        self._max_verify = self._input.param("max_verify", 100000)

    def __initialize_error_count_dict(self):
        """
            initializes self.__error_count_dict with ip, error and err count
            like {ip1: {"panic": 2, "KEY_ENOENT":3}}
        """
        for node in self._input.servers:
            self.__error_count_dict[node.ip] = {}
            for error in self.__report_error_list:
                self.__error_count_dict[node.ip][error] = NodeHelper.check_cbft_log(node, error)
        self.log.info(self.__error_count_dict)

    def __cleanup_previous(self):
        self._cb_cluster.cleanup_cluster(self, cluster_shutdown=False)

    def __init_clusters(self):
        self.log.info("Initializing cluster 1...")
        self._cb_cluster.init_cluster()

    def __set_free_servers(self):
        total_servers = self._input.servers
        cluster_nodes = []
        for _, nodes in self._input.clusters.iteritems():
            cluster_nodes.extend(nodes)

        FloatingServers._serverlist = [
            server for server in total_servers if server not in cluster_nodes]

    def __calculate_bucket_size(self, cluster_quota, num_buckets):
        dgm_run = self._input.param("dgm_run", 0)
        if dgm_run:
            # buckets cannot be created if size<100MB
            bucket_size = 256
        else:
            bucket_size = int((float(cluster_quota) - 500)/float(num_buckets))
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

        total_quota = self._cb_cluster.get_mem_quota()
        bucket_size = self.__calculate_bucket_size(
                total_quota,
                num_buckets)

        if self._create_default_bucket:
            self._cb_cluster.create_default_bucket(
                    bucket_size,
                    self._num_replicas,
                    eviction_policy=self.__eviction_policy,
                    bucket_priority=bucket_priority)

        self._cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

        self._cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

    def create_buckets_on_cluster(self, cluster_name):
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        if self.__mixed_priority:
            bucket_priority = 'high'
        else:
            bucket_priority = None
        num_buckets = self.__num_sasl_buckets + \
            self.__num_stand_buckets + int(self._create_default_bucket)

        total_quota = self._cb_cluster.get_mem_quota()
        bucket_size = self.__calculate_bucket_size(
            total_quota,
            num_buckets)

        if self._create_default_bucket:
            self._cb_cluster.create_default_bucket(
                bucket_size,
                self._num_replicas,
                eviction_policy=self.__eviction_policy,
                bucket_priority=bucket_priority)

        self._cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

        self._cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)


    def load_cluster(self):
        if not self._dgm_run:
            self._cb_cluster.load_all_buckets(self._num_items, self._value_size)
        else:
            self._cb_cluster.load_all_buckets_till_dgm(
                active_resident_threshold=self._active_resident_threshold,
                items=self._num_items)

    def perform_update_delete(self):
        # UPDATES
        if self._update:
            self.log.info("Updating keys @ {0}".format(self._cb_cluster.get_name()))
            self._cb_cluster.update_delete_data(
                OPS.UPDATE,
                perc=self._perc_upd,
                expiration=self._expires,
                wait_for_expiration=self._wait_for_expiration)

        # DELETES
        if self._delete:
            self.log.info("Deleting keys @ {0}".format(self._cb_cluster.get_name()))
            self._cb_cluster.update_delete_data(OPS.DELETE, perc=self._perc_del)

    def async_perform_update_delete(self):
        tasks = []
        # UPDATES
        if self._update:
            self.log.info("Updating keys @ {0}".format(self._cb_cluster.get_name()))
            tasks.extend(self._cb_cluster.async_update_delete(
                OPS.UPDATE,
                perc=self._perc_upd,
                expiration=self._expires))

        [task.result() for task in tasks]
        if tasks:
            self.log.info("Batched updates loaded to cluster(s)")

        tasks = []
        # DELETES
        if self._delete:
            self.log.info("Deleting keys @ {0}".format(self._cb_cluster.get_name()))
            tasks.extend(
                self._cb_cluster.async_update_delete(
                    OPS.DELETE,
                    perc=self._perc_del))

        [task.result() for task in tasks]
        if tasks:
            self.log.info("Batched deletes sent to cluster(s)")

        if self._wait_for_expiration and self._expires:
            self.sleep(
                self._expires,
                "Waiting for expiration of updated items")


    def print_panic_stacktrace(self, node):
        """ Prints panic stacktrace from goxdcr.log*
        """
        shell = RemoteMachineShellConnection(node)
        result, err = shell.execute_command("zgrep -A 40 'panic:' {0}/cbft.log*".
                            format(NodeHelper.get_log_dir(node)))
        for line in result:
            self.log.info(line)
        shell.disconnect()

    def check_errors_in_cbft_logs(self):
        """
        checks if new errors from self.__report_error_list
        were found on any of the goxdcr.logs
        """
        error_found_logger = []
        for node in self._input.servers:
            for error in self.__report_error_list:
                new_error_count = NodeHelper.check_cbft_log(node, error)
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



    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)
