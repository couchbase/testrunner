import re
import time

from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from xdcrnewbasetests import XDCRNewBaseTest


class XDCRPrioritization(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.rdirection = self._input.param("rdirection", "unidirection")
        self.backfill = self._input.param("backfill", None)
        if self.backfill:
            self.load_and_setup_xdcr()
        else:
            self.setup_xdcr_and_load()

    def __verify_dcp_priority(self, server, expected_priority):
        shell = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        for bucket in rest.get_buckets():
            output, error = shell.execute_cbstats(bucket, "dcp", print_results=False)
            for stat in output:
                if re.search("eq_dcpq:xdcr:dcp_.*" + bucket.name + ".*==:priority:", stat):
                    actual_priority = stat.split("==:priority:")[1].lstrip()
                    if actual_priority not in expected_priority[bucket.name]:
                        self.fail("For replication bucket {0} -> bucket {0} "
                                  "on source cluster {1}, expected dcp priority: {2} does not match actual priority: {3}". \
                                  format(bucket.name, server.ip, expected_priority[bucket.name], actual_priority))
                    else:
                        self.log.info("For replication bucket {0} -> bucket {0} "
                                      "on source cluster {1}, expected dcp priority: {2} matches actual priority: {3}". \
                                      format(bucket.name, server.ip, expected_priority[bucket.name], actual_priority))

    def _verify_dcp_priority(self, server):
        expected_priority = {}
        rest = RestConnection(server)
        buckets = rest.get_buckets()
        for bucket in buckets:
            if self.backfill:
                 goxdcr_priority = rest.get_xdcr_param(bucket.name, bucket.name, "priority").lower()
                 if goxdcr_priority == "low" or goxdcr_priority == "high":
                    expected_priority[bucket.name] = goxdcr_priority
                 elif goxdcr_priority == "medium":
                    expected_priority[bucket.name] = ["medium", "high"]
            else:
                expected_priority[bucket.name] = "medium"
        self.__verify_dcp_priority(server, expected_priority)

    def _verify_goxdcr_priority(self, cluster):
        rest = RestConnection(cluster.get_master_node())
        for bucket in rest.get_buckets():
            param_str = self._input.param(
                "%s@%s" %
                (bucket.name, cluster.get_name()), None)
            if param_str:
                expected_priority = (param_str.split('priority:')[1]).split(',')[0]
                actual_priority = rest.get_xdcr_param(bucket.name, bucket.name, "priority")
                if expected_priority != actual_priority:
                    self.fail("For replication bucket {0} -> bucket {0} "
                              "on source cluster {1}, expected goxdcr priority: {2} does not match actual goxdcr priority: {3}". \
                              format(bucket.name, cluster.get_master_node().ip, expected_priority, actual_priority))
                else:
                    self.log.info("For replication bucket {0} -> bucket {0} "
                                  "on source cluster {1}, expected goxdcr priority: {2} matches actual goxdcr priority: {3}". \
                                  format(bucket.name, cluster.get_master_node().ip, expected_priority, actual_priority))

    def verify_results(self):
        self._verify_goxdcr_priority(self.src_cluster)
        self._verify_dcp_priority(self.src_master)
        if self.rdirection == "bidirection":
            self._verify_goxdcr_priority(self.dest_cluster)
            self._verify_dcp_priority(self.dest_master)

    def _verify_tunable(self, cluster, input_param, repl_param):
        rest = RestConnection(cluster.get_master_node())
        buckets = rest.get_buckets()
        for bucket in buckets:
            param_str = self._input.param(
                "%s@%s" %
                (bucket.name, cluster.get_name()), None)
            if param_str:
                expected =(param_str.split(input_param + ':')[1]).split(',')[0]
            actual = rest.get_xdcr_param(bucket.name, bucket.name, repl_param)
            if expected != actual:
                    self.fail("For replication bucket {0} -> bucket {0} "
                              "on source cluster {1}, expected {2}: {3} does not match actual {2}: {4}". \
                              format(bucket.name, server.ip, input_param, expected, actual))
            self.log.info("For replication bucket {0} -> bucket {0} "
                          "on source cluster {1}, expected {2}: {3} matches actual {2}: {4}". \
                          format(bucket.name, server.ip, input_param, expected, actual))

    def test_desired_latency(self):
        self._verify_tunable(self.src_cluster, "desired_latency", "desiredLatency")
        if self.rdirection == "bidirection":
            self._verify_tunable(self.dest_cluster, "desired_latency", "desiredLatency")

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.get_cb_cluster_by_name(cluster_name))
        return clusters

    def test_priority(self):
        tasks = []
        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.pause_all_replications()

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                tasks.append(cluster.async_rebalance_in())
                for task in tasks:
                    task.result()

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                     rebalance=True)

        if rebalance_out:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                tasks.append(cluster.async_rebalance_out())
                for task in tasks:
                    task.result()

        if swap_rebalance:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                tasks.append(cluster.async_swap_rebalance())
                for task in tasks:
                    task.result()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.resume_all_replications()

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            time.sleep(60)

        self.sleep(30)
        self.perform_update_delete()

        self.verify_results()
