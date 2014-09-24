# FIXME: Remove this comment later
# Lets keep every data member private.
# If needed urgently, create getter function to return the data members

import unittest
import time
import copy
import string
import random

from couchbase.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import ServerUnavailableException, XDCRException
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from testconstants import STANDARD_BUCKET_PORT
from couchbase.document import View
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.stats_tools import StatsCommon
from membase.helper.bucket_helper import BucketOperationHelper

from couchbase.documentgenerator import BlobGenerator


class TOPOLOGY:
    CHAIN = "chain"
    START = "star"
    RING = "ring"


class REPPLICATION_DIRECTION:
    UNIDIRECTION = "unidirection"
    BIDIRECTION = "bidirection"
    REPLICATION_TYPE = "CONTINOUS"


class XDCR_REPLICATION_TYPE:
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


class NodeHelper:

    @staticmethod
    def reboot_node(node, wait_timeout=60):
        #self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == OS.LINUX:
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        # wait for restart and warmup on all node
        time.sleep(wait_timeout * 5)
        # disable firewall on these nodes
        NodeHelper.disable_firewall(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [node],
            self,
            wait_if_warmup=True)
        RestConnection(node).enable_xdcr_trace_logging()

    @staticmethod
    def disable_firewall(
            server, rep_direction=REPPLICATION_DIRECTION.UNIDIRECTION):
        shell = RemoteMachineShellConnection(server)
        o, r = shell.execute_command("iptables -F")
        shell.log_command_output(o, r)
        o, r = shell.execute_command(
            "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j ACCEPT")
        shell.log_command_output(o, r)
        if rep_direction == REPPLICATION_DIRECTION.BIDIRECTION:
            o, r = shell.execute_command(
                "/sbin/iptables -A OUTPUT -p tcp -o eth0 --dport 1000:65535 -j ACCEPT")
            shell.log_command_output(o, r)
        o, r = shell.execute_command(
            "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
        shell.log_command_output(o, r)
        #self.log.info("enabled firewall on {0}".format(server))
        o, r = shell.execute_command("/sbin/iptables --list")
        shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def enable_firewall(
            server, rep_direction=REPPLICATION_DIRECTION.UNIDIRECTION):
        is_bidirectional = rep_direction == REPPLICATION_DIRECTION.BIDIRECTION
        RemoteUtilHelper.enable_firewall(
            server,
            bidirectional=is_bidirectional,
            xdcr=True)

    @staticmethod
    def kill_erlang(server):
        pass

    @staticmethod
    def kill_memcached(server):
        pass

    @staticmethod
    def do_a_warm_up(node):
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        time.sleep(5)
        shell.start_couchbase()
        shell.disconnect()
        RestConnection(node).enable_xdcr_trace_logging()

    @staticmethod
    def wait_node_restarted(
            server, wait_time=120, wait_if_warmup=False, check_service=False):
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
                    [server], self, wait_time=wait_time - num * 10,
                    wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                time.sleep(10)

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
            "Couchbase service is not running after {0} seconds".format(wait_time))

# Keep Track of free servers
# For Rebalance-in or swap-rebalance operations.


class FloatingServers:
    _serverlist = []


class CouchbaseCluster:

    def __init__(self, name, nodes, mem_quota, dgm_run, default_bucket=True,
                 num_stand_buckets=0, num_sasl_buckets=0, replicas=1,
                 is_cluster_run=False):
        self.__name = name
        self.__nodes = nodes
        self.__master_node = nodes[0]
        self.__buckets = None
        self.__views = None
        self.__mem_quota = mem_quota
        if dgm_run:
            self.__mem_quota = 256
        self.__num_stand_buckets = num_stand_buckets
        self.__num_sasl_buckets = num_sasl_buckets
        self.__create_default_bucket = default_bucket
        self.__replicas = replicas
        self.__fail_over_nodes = []
        self.__is_cluster_run = is_cluster_run
        # List of XDCRRemoteCluster Objects.
        self.__remote_clusters = []
        self.__clusterop = Cluster()

    def __sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def __stop_rebalance(self):
        rest = RestConnection(self.__master_node)
        if rest._rebalance_progress_status() == 'running':
            self.log.warning(
                "rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")

    # Init/Cleanup Cluster
    def __init_nodes(self, disabled_consistent_view=None):
        _tasks = []
        for node in self.__nodes:
            _tasks.append(
                self.cluster.async_init_node(
                    node,
                    disabled_consistent_view))
        for task in _tasks:
            mem_quota = task.result()
            if mem_quota < self.__mem_quota or self.__mem_quota == 0:
                self.__mem_quota = mem_quota
        if self._use_hostanames:
            if not hasattr(self, 'hostnames'):
                self.hostnames = {}
            self.hostnames.update(self._rename_nodes(self.__nodes))

    def init_cluster(self, disabled_consistent_view=None):
        self.__init_nodes(disabled_consistent_view)
        self.cluster.async_rebalance(
            self.__nodes,
            self.__nodes[1:],
            [],
            use_hostnames=self._use_hostanames).result()
        self.__create_buckets()
        for node in self.__nodes:
            RestConnection(node).enable_xdcr_trace_logging()

    def cleanup_cluster(self, force_eject=False):
        self.log.info(
            "cleanup cluster{0}: {1}".format(
                self.__name,
                self.__nodes))
        self.__stop_rebalance()
        for node in self.__nodes:
            BucketOperationHelper.delete_all_buckets_or_assert([node], self)
            if force_eject and node != self.__master_node:
                try:
                    rest = RestConnection(node)
                    rest.force_eject_node()
                except BaseException as e:
                    self.log.error(e)
            else:
                ClusterOperationHelper.cleanup_cluster([node], self)
            ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self)

    # Buckets Operations
    def __create_sasl_buckets(
            self, server_id, bucket_size,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_prority=BUCKET_PRIORITY.HIGH):
        # FIXME eviction_policy and bucket_priority needs to be configurable
        bucket_tasks = []
        for i in range(self.__num_sasl_buckets):
            name = "sasl_bucket_" + str(i + 1)
            bucket_tasks.append(self.cluster.async_create_sasl_bucket(
                self.__master_node,
                name,
                'password',
                bucket_size,
                self.__replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_prority))
            self.__buckets.append(Bucket(
                name=name, authType="sasl", saslPassword="password",
                num_replicas=self.__replicas, bucket_size=bucket_size,
                master_id=server_id, eviction_policy=self.eviction_policy))

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

    def __create_standard_buckets(
            self, server_id, bucket_size,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_prority=BUCKET_PRIORITY.HIGH):
        bucket_tasks = []
        for i in range(self.__num_stand_buckets):
            name = "standard_bucket_" + str(i + 1)
            bucket_tasks.append(self.cluster.async_create_standard_bucket(
                self.__master_node,
                name,
                STANDARD_BUCKET_PORT + i,
                bucket_size,
                self.__replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_prority))
            self.__buckets.append(
                Bucket(
                    name=name, authType=None, saslPassword=None,
                    num_replicas=self.__replicas, bucket_size=bucket_size,
                    port=STANDARD_BUCKET_PORT + i, master_id=server_id,
                    eviction_policy=eviction_policy))

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

    def __create_default_bucket(
            self, master_id, bucket_size, eviction_policy=EVICTION_POLICY.VALUE_ONLY):
        self.cluster.create_default_bucket(
            self.__master_node,
            bucket_size,
            self.__replicas,
            eviction_policy=self.eviction_policy)
        self.__buckets.append(
            Bucket(name=BUCKET_NAME.DEFAULT, authType="sasl", saslPassword="",
                   num_replicas=self.__replicas, bucket_size=bucket_size,
                   master_id=master_id, eviction_policy=eviction_policy))

    def __create_buckets(self):

        master_id = RestConnection(self.__master_node).get_nodes_self().id
        # if not cluster run use ip addresses instead of lh
        if not self.__is_cluster_run:
            master_id = master_id.replace(
                "127.0.0.1",
                self.__master_node.ip).replace(
                "localhost",
                self.__master_node.ip)

        num_buckets = self.__num_sasl_buckets + \
            self.__num_stand_buckets + \
            (1 if self.__create_default_bucket else 0)
        bucket_size = int(float(self.__mem_quota) / float(num_buckets))

        if self.__create_default_bucket:
            self.__create_default_bucket(master_id, bucket_size)

        self.__create_sasl_buckets(master_id, bucket_size)

        self.__create_standard_buckets(master_id, bucket_size)

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
        self.cluster.bucket_delete(self.__master_node, bucket_removed.name)
        self.__buckets.remove(bucket_removed)

    # Load buckets
    def _async_load_all_buckets(self, kv_gen, op_type, exp, kv_store=1,
                                flag=0, only_store_hash=True, batch_size=1,
                                pause_secs=1, timeout_secs=30):
        tasks = []
        for bucket in self.__buckets:
            gen = copy.deepcopy(kv_gen)
            tasks.append(self.cluster.async_load_gen_docs(
                self.__master_node, bucket.name, gen,
                bucket.kvs[kv_store],
                op_type, exp, flag, only_store_hash,
                batch_size, pause_secs, timeout_secs))
        return tasks

    def _load_bucket(self, bucket, kv_gen, op_type, exp, kv_store=1,
                     flag=0, only_store_hash=True, batch_size=1000,
                     pause_secs=1, timeout_secs=30):
        task = self._async_load_bucket(
            bucket,
            self.__master_node,
            kv_gen,
            op_type,
            exp,
            kv_store,
            flag,
            only_store_hash,
            batch_size,
            pause_secs,
            timeout_secs)
        task.result()

    def key_generator(
            self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def _load_all_buckets(self, kv_gen, op_type, exp, kv_store=1, flag=0,
                          only_store_hash=True, batch_size=1000,
                          pause_secs=1, timeout_secs=30):
        tasks = self._async_load_all_buckets(
            kv_gen,
            op_type,
            exp,
            kv_store,
            flag,
            only_store_hash,
            batch_size,
            pause_secs,
            timeout_secs)
        for task in tasks:
            task.result()

        if self.active_resident_threshold:
            stats_all_buckets = {}
            for bucket in self._get_cluster_buckets(self.__master_node):
                stats_all_buckets[bucket.name] = StatsCommon()

            for bucket in self._get_cluster_buckets(self.__master_node):
                threshold_reached = False
                while not threshold_reached:
                    active_resident = stats_all_buckets[
                        bucket.name].get_stats(
                        [self.__master_node],
                        bucket,
                        '',
                        'vb_active_perc_mem_resident')[self.__master_node]
                    if int(active_resident) > self.active_resident_threshold:
                        self.log.info("resident ratio is %s greater than %s for %s in bucket %s. Continue loading to the cluster" %
                                      (active_resident,
                                       self.active_resident_threshold,
                                       self.__master_node.ip, bucket.name))
                        random_key = self.key_generator()
                        generate_load = BlobGenerator(
                            random_key,
                            '%s-' %
                            random_key,
                            self._value_size,
                            end=batch_size *
                            50)
                        self._load_bucket(
                            bucket,
                            self.__master_node,
                            generate_load,
                            OPS.CREATE,
                            exp=0,
                            kv_store=1,
                            flag=0,
                            only_store_hash=True,
                            batch_size=batch_size,
                            pause_secs=5,
                            timeout_secs=60)
                    else:
                        threshold_reached = True
                        self.log.info(
                            "DGM state achieved for %s in bucket %s!" %
                            (self.__master_node.ip, bucket.name))
                        break

    # @param kv_gen = gen_create/gen_update/gen_delete
    # @param ops = create/update/delete
    # @param expiration time for expire for update
    # @param wait_for_expiration = True if wait for items to expire
    def update_delete_data(
            self, kv_gen, ops, expiration=0, wait_for_expiration=True):

        tasks = self.async_update_delete(kv_gen, ops, expiration)

        [task.result() for task in tasks]

        if wait_for_expiration and expiration:
            self.__sleep(expiration, "Waiting for expiration of updated items")

    # @param kv_gen = gen_create/gen_update/gen_delete
    # @param ops = create/update/delete
    # @param expiration time for expire for update
    def async_update_delete(
            self, kv_gen, ops, expiration=0):
        """Setting up creates/updates/deletes at source nodes"""
        tasks = self._async_load_all_buckets(
            self.__master_node,
            kv_gen,
            ops,
            expiration)
        return tasks

    def run_expiry_pager(self, val=10):
        for bucket in self.__buckets:
            ClusterOperationHelper.flushctl_set(
                self.__master_node,
                "exp_pager_stime",
                val,
                bucket)
            self.log.info("wait for expiry pager to run on all these nodes")

    # Views Operations
    def make_default_views(self, prefix, count, is_dev_ddoc=False,):
        ref_view = self._default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        return [View(ref_view.name + str(i), ref_view.map_func,
                     None, is_dev_ddoc) for i in xrange(count)]

    def async_create_views(
            self, design_doc_name, views, bucket=BUCKET_NAME.DEFAULT):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.cluster.async_create_view(
                    self.__master_node,
                    design_doc_name,
                    view,
                    bucket)
                tasks.append(t_)
        else:
            t_ = self.cluster.async_create_view(
                self.__master_node,
                design_doc_name,
                None,
                bucket)
            tasks.append(t_)
        return tasks

    def create_views(self, defaultview=True):
        pass

    def async_query_views(self):
        pass

    def query_views(self):
        pass

    def disable_compaction(self, bucket=BUCKET_NAME.DEFAULT):
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.cluster.modify_fragmentation_config(
            self.__master_node,
            new_config,
            bucket)

    # Rebalance/Failover Operations
    def async_rebalance_out(self, master=False, num_nodes=1):
        # TODO add assert in case of only one node server
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = self.__nodes[-num_nodes:]
        task = self.cluster.async_rebalance(self.__nodes, [], to_remove_node)

        [self.__nodes.remove(node) for node in to_remove_node]

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def rebalance_out(self, master=False, num_nodes=1):
        task = self.async_rebalance_out(master, num_nodes)
        task.result()

    def async_rebalance_in(self, num_nodes=1):
        to_add_node = []
        for _ in range(num_nodes):
            to_add_node.appen(FloatingServers._serverlist.pop())
        task = self.cluster.async_rebalance(self.__nodes, to_add_node, [])
        self.__nodes.extend(to_add_node)
        return task

    def rebalance_in(self, num_nodes=1):
        task = self.async_rebalance_in(num_nodes)
        task.result()

    def async_swap_rebalance(self, master=False, num_nodes=1):
        if master:
            to_remove_node = [self.__master_node]
        else:
            # TODO add assert if number of free servers are less than required
            # TODO add assert in case of only one node server
            to_remove_node = self.__nodes[-num_nodes:]

        to_add_node = []
        for _ in range(num_nodes):
            to_add_node.append(FloatingServers._serverlist.pop())
        task = self.cluster.async_rebalance(
            self.__nodes,
            to_add_node,
            to_remove_node)

        [self.__nodes.remove(node) for node in to_remove_node]
        self.__nodes.extend(to_add_node)

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def swap_rebalance(self, master=False, num_nodes):
        task = self.async_swap_rebalance(master, num_nodes)
        task.result()

    def async_failover(self, master=False, num_nodes=1, graceful=False):
        # TODO add assert in case of only one node server
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = self.__nodes[-num_nodes:]
        task = self.cluster.async_failover(
            self.__nodes,
            to_remove_node,
            graceful)

        for node in to_remove_node:
            self.__nodes.remove(node)

        self.__fail_over_nodes = to_remove_node

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def failover(
            self, master=False, num_nodes=1, graceful=False, rebalance=True):
        task = self.async_failover(master, num_nodes, graceful)
        if rebalance:
            self.cluster.rebalance(self.__nodes, [], self.__fail_over_nodes)

            # self.__fail_over_nodes = []: So that no add_back is possible for
            # these nodes now
            self.__fail_over_nodes = []
        task.result()

    def add_back_node(self):
        # Add assert if no nodes to add_back i.e. self.__fail_over_nodes == 0
        for failover_node in self.__fail_over_nodes:
            rest = RestConnection(self.__master_node)
            rest.add_back_node(failover_node.id)
        self.__fail_over_nodes = []

    def warmp_node(self, master=False):
        if master:
            NodeHelper.do_a_warm_up(self.__master_node)
        else:
            NodeHelper.do_a_warm_up(self.__nodes[-1])

    # XDCR Operations
    def set_xdcr_param(self):
        pass

    def join_cluster(self, dest_cluster, name, topology, encryption=False):
        pass

    # add params to what to modify
    def modify_cluster(self):
        pass

    def verify_results(self):
        # Verify data between this cluster and other remote clusters
        pass


class XDCRRemoteCluster:

    def __init__(self, dest_cluster, name, topology):
        self.__dest_cluster = dest_cluster
        self.__name = name
        self.__topology = topology

        # List of XDCRepication objects
        self.__replications = []

    def create_replication(self, from_bucket=None, to_bucket=None):
        pass

    def start_all_replication(self):
        pass

    def pause_all_replication(self):
        pass

    def resume_all_replication(self):
        pass


class XDCReplication:

    def __init__(self, remote_cluster_name, from_bucket, to_bucket):
        self.__remote_cluster_name = remote_cluster_name
        self.__from_bucket = from_bucket
        self.__to_bucket = to_bucket

        # Response from REST API
        self.__rep_id = None

    # Although Replication start as soon we configure it.
    # But here we can start after creating object of XDCReplication.
    # self.__repl_id will be assigned here.
    def start(self):
        pass

    def pause(self):
        pass

    def resume(self):
        pass


class XDCRNewBaseTest(unittest.TestCase):

    def __init__(self):
        self.__couchbase_custers = []

    def setUp(self):
        unittest.TestCase.setUp(self)
        pass

    def tearDown(self):
        # Collect Logs files
        # Shutdown cluster
        # Cluster cleanup
        unittest.TestCase.tearDown(self)

    def set_topology_chain(self):
        pass

    def set_topology_star(self):
        pass

    def set_topology_ring(self):
        pass
