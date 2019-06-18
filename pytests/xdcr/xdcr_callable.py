import logger

from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection
from xdcrnewbasetests import CouchbaseCluster

class REPL_PARAM:
    FAILURE_RESTART = "failureRestartInterval"
    CHECKPOINT_INTERVAL = "checkpointInterval"
    OPTIMISTIC_THRESHOLD = "optimisticReplicationThreshold"
    FILTER_EXP = "filterExpression"
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


class TEST_XDCR_PARAM:
    FAILURE_RESTART = "failure_restart_interval"
    CHECKPOINT_INTERVAL = "checkpoint_interval"
    OPTIMISTIC_THRESHOLD = "optimistic_threshold"
    FILTER_EXP = "filter_expression"
    SOURCE_NOZZLES = "source_nozzles"
    TARGET_NOZZLES = "target_nozzles"
    BATCH_COUNT = "batch_count"
    BATCH_SIZE = "batch_size"
    LOG_LEVEL = "log_level"
    MAX_REPLICATION_LAG = "max_replication_lag"
    TIMEOUT_PERC = "timeout_percentage"
    PRIORITY = "priority"
    DESIRED_LATENCY = "desired_latency"


class XDCRCallable:
    def __init__(self, nodes, num_clusters=2):
        self.log = logger.Logger.get_logger()
        self.cluster_list = []
        self.__clusterop = Cluster()
        self.setup_xdcr(nodes, num_clusters)

    def setup_xdcr(self, nodes, num_clusters):
        self._setup_xdcr_topology(nodes, num_clusters)

    def __assign_nodes_to_clusters(self, nodes, num_nodes_per_cluster):
        for _ in range(0, len(nodes), num_nodes_per_cluster):
            yield nodes[_:_ + num_nodes_per_cluster]

    def _setup_xdcr_topology(self, nodes, num_clusters):
        num_nodes_per_cluster = len(nodes) // num_clusters
        count = 1
        for cluster_nodes in list(self.__assign_nodes_to_clusters(nodes, num_nodes_per_cluster)):
            cluster = CouchbaseCluster(name="C" + str(count), nodes=cluster_nodes, log=self.log)
            self.cleanup_cluster(cluster)
            #cluster.cleanup_cluster(test_case="xdcr upgrade")
            self.__init_cluster(cluster)
            self.log.info("Cluster {0}:{1} created".format(cluster.get_name(), cluster.get_nodes()))
            self.cluster_list.append(cluster)
            count += 1

        # TODO: implementing chain topology for now, need to extend to other xdcr topologies
        # C1->C2, C2->C3..
        for count, cluster in enumerate(self.cluster_list):
            if count < len(self.cluster_list) - 1:
                cluster.add_remote_cluster(self.cluster_list[count + 1],
                                           'C' + str(count) + "-to-" + 'C' + str(count + 1))

    def ___init_nodes(self, cluster, disabled_consistent_view=None):
        """Initialize all nodes.
        """
        tasks = []
        for node in cluster.get_nodes():
            tasks.append(
                self.__clusterop.async_init_node(
                    node))
        for task in tasks:
            task.result(60)

    def __init_cluster(self, cluster):
        """Initialize cluster.
        1. Initialize all nodes.
        2. Add all nodes to the cluster.
        """
        self.___init_nodes(cluster)
        self.__clusterop.async_rebalance(
            cluster.get_nodes(),
            cluster.get_nodes()[1:],
            []).result()

    def cleanup_cluster(self, cluster):
        """Cleanup cluster.
        1. Remove all remote cluster references.
        2. Remove all replications.
        3. Remove all buckets.
        """
        self.log.info("removing xdcr/nodes settings")
        rest = RestConnection(cluster.get_master_node())
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest.remove_all_recoveries()
        cluster.cleanup_cluster("upgradeXDCR")

    def _create_replication(self):
        for cluster in self.cluster_list:
            self.log.info("Creating replication from {0}->{1}".format(cluster.get_name, cluster.get_remote_clusters()))

    def _set_replication_properties(self, param_str):
        pass

    def _get_replication_properties(self, replid):
        pass

    def __del__(self):
        for cluster in self.cluster_list:
            self.cleanup_cluster(cluster)
