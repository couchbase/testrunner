from .xdcrnewbasetests import XDCRNewBaseTest
import time
import random

class XDCRSecurityTests(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)

    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

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

    def test_xdcr_with_security(self):
        #Settings
        settings_values_map = {"autofailover":["enable", None],
                    "n2n":["enable", "disable"],
                    "tls":["all", "control", "strict"]
                    }

        apply_settings_before_setup = self._input.param("apply_settings_before_setup", False)
        disable_autofailover = self._input.param("disable_autofailover", False)
        enable_n2n = self._input.param("enable_n2n", False)
        enforce_tls = self._input.param("enforce_tls", None)
        tls_level = self._input.param("tls_level", "control")
        enable_autofailover = self._input.param("enable_autofailover", False)
        disable_n2n = self._input.param("disable_n2n", None)
        disable_tls = self._input.param("disable_tls", None)

        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)
        initial_xdcr = self._input.param("initial_xdcr", random.choice([True, False]))
        random_setting = self._input.param("random_setting", False)

        if not apply_settings_before_setup:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()

        if enforce_tls:
            for cluster in self.get_cluster_objects_for_input(enforce_tls):
                self.toggle_security_setting([cluster.get_master_node()], "tls", tls_level)

        #Revert to default (control) tls level
        if disable_tls:
            for cluster in self.get_cluster_objects_for_input(disable_tls):
                self.toggle_security_setting([cluster.get_master_node()], "tls")

        if enable_n2n:
            for cluster in self.get_cluster_objects_for_input(enable_n2n):
                self.toggle_security_setting([cluster.get_master_node()], "n2n", "enable")

        if disable_n2n:
            for cluster in self.get_cluster_objects_for_input(disable_n2n):
                self.toggle_security_setting([cluster.get_master_node()], "n2n")

        if enable_autofailover:
            for cluster in self.get_cluster_objects_for_input(enable_autofailover):
                self.toggle_security_setting([cluster.get_master_node()], "autofailover", "enable")

        if disable_autofailover:
            for cluster in self.get_cluster_objects_for_input(disable_autofailover):
                self.toggle_security_setting([cluster.get_master_node()], "autofailover")

        if random_setting:
            for cluster in self.get_cluster_objects_for_input(random_setting):
                setting = random.choice(list(settings_values_map.keys()))
                value = random.choice(settings_values_map.get(setting))
                self.toggle_security_setting([cluster.get_master_node()], setting, value)

        if apply_settings_before_setup:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.pause_all_replications()
                    time.sleep(60)

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                cluster.async_rebalance_in()

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                     rebalance=True)

        if rebalance_out:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                cluster.async_rebalance_out()

        if swap_rebalance:
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                cluster.async_swap_rebalance()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.resume_all_replications()

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            time.sleep(60)

        self.perform_update_delete()
        self.verify_results()

        try:
            # Restore defaults - Enable autofailover, disable n2n
            self.toggle_security_setting([cluster.get_master_node()], "autofailover", "enable")
            self.toggle_security_setting([cluster.get_master_node()], "n2n")
        except:
            pass