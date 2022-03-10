from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import NodeHelper
import random
import time


class XDCRP2PTests(XDCRNewBaseTest):

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            cluster = self.get_cb_cluster_by_name(cluster_name)
            cluster.set_xdcr_param("preReplicateVBMasterCheck", "true")
            time.sleep(5)
            clusters.append(cluster)
        return clusters

    def verify_p2p_discovery(self, new_node, node):
        _, count = NodeHelper.check_goxdcr_log(
            node,
            "Discovered peers: map\\[.*" + new_node.ip + ":", timeout=15)
        if count < 1:
            self.fail("peer {0} not discovered on {1}".format(new_node.ip, node.ip))
        self.log.info("P2P discovery as expected on {0}".format(node.ip))

    def test_xdcr_p2p(self):
        internal_setting = self._input.param("internal_setting", None)
        replication_setting = self._input.param("replication_setting", None)
        setting_name = self._input.param("setting_name", None)
        default_value = self._input.param("default_value", None)
        set_value = self._input.param("set_value", None)

        disable_autofailover = self._input.param("disable_autofailover", False)
        enable_n2n = self._input.param("enable_n2n", False)
        enforce_tls = self._input.param("enforce_tls", None)
        tls_level = self._input.param("tls_level", "control")
        enable_autofailover = self._input.param("enable_autofailover", False)
        disable_n2n = self._input.param("disable_n2n", None)
        disable_tls = self._input.param("disable_tls", None)

        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_in_out = self._input.param("rebalance_in_out", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        node_failure = self._input.param("node_failure", None)
        node_failure_type = self._input.param("node_failure_type", None)
        apply_settings_before_setup = self._input.param("apply_settings_before_setup", random.choice([True, False]))
        initial_xdcr = self._input.param("initial_xdcr", random.choice([True, False]))
        reset_clusters = []


        if not apply_settings_before_setup:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()

        if internal_setting:
            for cluster in self.get_cluster_objects_for_input(internal_setting):
                if set_value:
                    cluster.set_internal_setting(setting_name, set_value)
                    time.sleep(5)
                    current_value = cluster.get_internal_setting(setting_name)
                    self.assertEqual(current_value, set_value,
                                     setting_name + " value not as expected")
                if default_value:
                    current_value = cluster.get_internal_setting(setting_name)
                    self.assertEqual(current_value, default_value,
                                     setting_name + " value not as expected")
        if enforce_tls:
            for cluster in self.get_cluster_objects_for_input(enforce_tls):
                cluster.toggle_security_setting([cluster.get_master_node()], "tls", tls_level)
                reset_clusters.append(cluster)

        # Revert to default (control) tls level
        if disable_tls:
            for cluster in self.get_cluster_objects_for_input(disable_tls):
                cluster.toggle_security_setting([cluster.get_master_node()], "tls")

        if enable_n2n:
            for cluster in self.get_cluster_objects_for_input(enable_n2n):
                cluster.toggle_security_setting([cluster.get_master_node()], "n2n", "enable")
                reset_clusters.append(cluster)

        if disable_n2n:
            for cluster in self.get_cluster_objects_for_input(disable_n2n):
                cluster.toggle_security_setting([cluster.get_master_node()], "n2n")

        if enable_autofailover:
            for cluster in self.get_cluster_objects_for_input(enable_autofailover):
                cluster.toggle_security_setting([cluster.get_master_node()], "autofailover", "enable")

        if disable_autofailover:
            for cluster in self.get_cluster_objects_for_input(disable_autofailover):
                cluster.toggle_security_setting([cluster.get_master_node()], "autofailover")
                reset_clusters.append(cluster)

        if apply_settings_before_setup:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()

        if replication_setting:
            for cluster in self.get_cluster_objects_for_input(replication_setting):
                if set_value:
                    cluster.set_xdcr_param(setting_name, set_value)
                if default_value:
                    current_values = cluster.get_xdcr_param(setting_name)
                    for current_value in current_values:
                        if current_value != default_value:
                            self.fail("{} Current value {}!=Expected value {}".format(setting_name,
                                                                                      current_value, default_value))

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                nodes = cluster.get_nodes()
                cluster.rebalance_in()
                for node in nodes[:-1]:
                    self.verify_p2p_discovery(nodes[-1], node)

        if rebalance_in_out:
            for cluster in self.get_cluster_objects_for_input(rebalance_in_out):
                nodes = cluster.get_nodes()
                cluster.rebalance_out(1)
                cluster.rebalance_in(1)
                for node in nodes[:-1]:
                    self.verify_p2p_discovery(nodes[-1], node)

        if rebalance_out:
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                cluster.rebalance_out()

        if swap_rebalance:
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                cluster.swap_rebalance()

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                     rebalance=True)

        if node_failure:
            for cluster in self.get_cluster_objects_for_input(node_failure):
                nodes = cluster.get_nodes()
                chosen_node = nodes[-1]
                if node_failure_type == "enable_firewall":
                    NodeHelper.enable_firewall(chosen_node)
                    time.sleep(60)
                    NodeHelper.disable_firewall(chosen_node)
                if node_failure_type == "kill_goxdcr":
                    NodeHelper.kill_goxdcr(chosen_node)
                if node_failure_type == "restart_machine":
                    NodeHelper.reboot_server(chosen_node)
                if node_failure_type == "restart_couchbase":
                    NodeHelper.do_a_warm_up(chosen_node)
                time.sleep(150)
                for node in nodes[:-1]:
                    self.verify_p2p_discovery(nodes[-1], node)

        self.perform_update_delete()
        self.verify_results()

        if len(reset_clusters):
            try:
                for cluster in reset_clusters:
                    # Restore defaults - disable tls, enable autofailover, disable n2n
                    cluster.toggle_security_setting([cluster.get_master_node()], "tls")
                    cluster.toggle_security_setting([cluster.get_master_node()], "autofailover", "enable")
                    cluster.toggle_security_setting([cluster.get_master_node()], "n2n")
            except:
                pass
