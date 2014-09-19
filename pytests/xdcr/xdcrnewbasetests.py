import unittest

from couchbase.cluster import Cluster


class XDCRConstants:
    INPUT_PARAM_REPLICATION_DIRECTION = "rdirection"
    INPUT_PARAM_CLUSTER_TOPOLOGY = "ctopology"
    INPUT_PARAM_SEED_DATA = "sdata"
    INPUT_PARAM_SEED_DATA_MODE = "sdata_mode"

    INPUT_PARAM_SEED_DATA_OPERATION = "sdata_op"
    INPUT_PARAM_POLL_INTERVAL = "poll_interval"  # in seconds
    INPUT_PARAM_POLL_TIMEOUT = "poll_timeout"  # in seconds

    # CLUSTER_TOPOLOGY_TYPE_LINE = "line"
    TOPOLOGY_CHAIN = "chain"
    TOPOLOGY_STAR = "star"
    TOPOLOGY_RING = "ring"

    REPLICATION_TYPE_CONTINUOUS = "continuous"
    REPLICATION_DIRECTION_UNIDIRECTION = "unidirection"
    REPLICATION_DIRECTION_BIDIRECTION = "bidirection"

    SEED_DATA_MODE_SYNC = "sync"


class NodeHelper:
    @staticmethod
    def reboot_node(server):
        pass

    @staticmethod
    def disable_firewall(server):
        pass

    @staticmethod
    def enable_firewall(server):
        pass

    @staticmethod
    def kill_erlang(server):
        pass

    @staticmethod
    def kill_memcached(server):
        pass


# Keep Track of free servers
# For Rebalance-in or swap-rebalance operations.
class FloatingServers:
    _serverlist = []


class CouchbaseCluster:
    def __init__(self, nodes):
        self.__nodes = nodes
        self.__buckets = None
        self.__views = None
        # List of XDCRRemoteCluster Objects.
        self.__remote_clusters = []
        self.__clusterop = Cluster()

    def get_master_node(self):
        # OR read ochrestor node from the cluster itself.
        return self.__nodes[0]

    # Whole Cluster Operations
    def init_cluster(self):
        pass

    def cleanup_cluster(self):
        pass

    # Buckets Operations
    def create_buckets(self):
        pass

    def delete_buckets(self):
        pass

    def load_all_buckets(self):
        pass

    def load_bucket(self, bucket=None):
        pass

    def async_update_delete(self):
        pass

    def run_expiry_pager(self):
        pass

    # Views Operations
    def create_views(self, defaultview=True):
        pass

    def async_query_views(self):
        pass

    def query_views(self):
        pass

    def disable_compaction(self):
        pass

    # Rebalance/Failover Operations
    def async_rebalance_out(self, master=False, num_nodes=1):
        pass

    def rebalance_out(self, master=False, num_nodes=1):
        task = self.async_rebalance_out(master, num_nodes)
        task.result()

    def async_rebalance_in(self, num_nodes=1):
        pass

    def rebalance_in(self, num_nodes=1):
        task = self.async_rebalance_in(num_nodes)
        task.result()

    def async_swap_rebalance(self, master=False, num_nodes=1):
        pass

    def swap_rebalance(self, master=False, num_nodes):
        task = self.async_swap_rebalance(master, num_nodes)
        task.result()

    def async_failover(self, master=False, rebalance=True):
        pass

    def add_back_node(self):
        pass

    def warmp_node(self, master=False):
        pass

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
