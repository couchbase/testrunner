import re
import time

from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from .xdcrnewbasetests import XDCRNewBaseTest


class XDCRPrioritization(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.rdirection = self._input.param("rdirection", "unidirection")
        self.initial = self._input.param("initial", False)
        if self.initial:
            self.load_and_setup_xdcr()
        else:
            self.setup_xdcr_and_load()

    def print_status(self, bucket, server, param, actual, expected, match):
        if match:
            self.log.info("For replication {0}->{0} "
                          "on source {1}, actual {2}:{3} matches expected {2}:{4}". \
                          format(bucket, server, param, actual, expected))
        else:
            self.fail("For replication {0}->{0} "
                      "on source {1}, actual {2}:{3} does NOT match expected {2}:{4}". \
                      format(bucket, server, param, actual, expected))

    def __verify_dcp_priority(self, server, expected_priority):
        shell = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        match = True
        for bucket in rest.get_buckets():
            repl = rest.get_replication_for_buckets(bucket.name, bucket.name)
            # Taking 10 samples of DCP priority ~5 seconds apart.
            # cbstats takes ~4 secs + 2 seconds sleep
            for sample in range(10):
                output, error = shell.execute_cbstats(bucket, "dcp", print_results=False)
                for stat in output:
                    if re.search("eq_dcpq:xdcr:dcp_" + repl['id'] + ".*==:priority:", stat):
                        actual_priority = stat.split("==:priority:")[1].lstrip()
                        if actual_priority not in expected_priority[bucket.name]:
                            match = False
                        self.log.info("Sample {0}:".format(sample + 1))
                        self.print_status(bucket.name, server.ip, "dcp priority",
                                          actual_priority,
                                          expected_priority[bucket.name],
                                          match=match)
                        time.sleep(2)


    def _verify_dcp_priority(self, server):
        expected_priority = {}
        rest = RestConnection(server)
        buckets = rest.get_buckets()
        for bucket in buckets:
            if self.initial:
                goxdcr_priority = str(rest.get_xdcr_param(bucket.name, bucket.name, "priority")).lower()
                # All replications start with 'medium' dcp priority
                expected_priority[bucket.name] = ["medium"]
                if goxdcr_priority == "low" or goxdcr_priority == "high":
                    expected_priority[bucket.name].append(goxdcr_priority)
                elif goxdcr_priority == "medium":
                    expected_priority[bucket.name].append("high")
            else:
                expected_priority[bucket.name] = "medium"
        self.__verify_dcp_priority(server, expected_priority)

    def _verify_goxdcr_priority(self, cluster):
        rest = RestConnection(cluster.get_master_node())
        match = True
        for bucket in rest.get_buckets():
            param_str = self._input.param(
                "%s@%s" %
                (bucket.name, cluster.get_name()), None)
            if param_str:
                expected_priority = (param_str.split('priority:')[1]).split(',')[0]
                actual_priority = str(rest.get_xdcr_param(bucket.name, bucket.name, "priority"))
                if expected_priority != actual_priority:
                    match = False
                self.print_status(bucket.name, cluster.get_master_node().ip, "goxdcr priority",
                                   actual_priority, expected_priority, match=match)

    def verify_results(self):
        self._verify_goxdcr_priority(self.src_cluster)
        self._verify_dcp_priority(self.src_master)
        if self.rdirection == "bidirection":
            self._verify_goxdcr_priority(self.dest_cluster)
            self._verify_dcp_priority(self.dest_master)

    def _verify_tunable(self, cluster, input_param, repl_param):
        rest = RestConnection(cluster.get_master_node())
        buckets = rest.get_buckets()
        match = True
        for bucket in buckets:
            param_str = self._input.param(
                "%s@%s" %
                (bucket.name, cluster.get_name()), None)
            if param_str:
                if input_param in param_str:
                    expected = (param_str.split(input_param + ':')[1]).split(',')[0]
                    actual = str(rest.get_xdcr_param(bucket.name, bucket.name, repl_param))
                    if expected != actual:
                        match = False
                    self.print_status(bucket.name, cluster.get_master_node().ip, input_param, actual, expected,
                                       match=match)

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

    def wait_for_op_to_complete(self, duration):
        time.sleep(duration)

    def test_priority(self):
        tasks = []
        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)
        gomaxprocs = self._input.param("gomaxprocs", None)

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
            self.wait_for_op_to_complete(10)
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.resume_all_replications()

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            self.wait_for_op_to_complete(60)

        if gomaxprocs:
            rest = RestConnection(self.src_master)
            rest.set_global_xdcr_param("goMaxProcs", gomaxprocs)
            if self.rdirection == "bidirection":
                rest = RestConnection(self.dest_master)
                rest.set_global_xdcr_param("goMaxProcs", gomaxprocs)

        self._verify_tunable(self.src_cluster, "desired_latency", "desiredLatency")
        if self.rdirection == "bidirection":
            self._verify_tunable(self.dest_cluster, "desired_latency", "desiredLatency")

        self.async_perform_update_delete()
        self.verify_results()
