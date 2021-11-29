import json
import random
import time

from security.ntonencryptionBase import ntonencryptionBase

from lib.Cb_constants.CBServer import CbServer
from pytests.security.x509_multiple_CA_util import x509main
from .xdcrnewbasetests import XDCRNewBaseTest
from lib.remote.remote_util import RemoteMachineShellConnection


class XDCRSecurityTests(XDCRNewBaseTest):

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
        self.settings_values_map = {"autofailover":["enable", None],
                    "n2n":["enable", "disable"],
                    "tls":["all", "control", "strict"]
                    }

        self.apply_settings_before_setup = self._input.param("apply_settings_before_setup", False)
        self.disable_autofailover = self._input.param("disable_autofailover", False)
        self.enable_n2n = self._input.param("enable_n2n", False)
        self.enforce_tls = self._input.param("enforce_tls", None)
        self.tls_level = self._input.param("tls_level", "control")
        self.enable_autofailover = self._input.param("enable_autofailover", False)
        self.disable_n2n = self._input.param("disable_n2n", None)
        self.disable_tls = self._input.param("disable_tls", None)

        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)
        initial_xdcr = self._input.param("initial_xdcr", random.choice([True, False]))
        random_setting = self._input.param("random_setting", False)
        multiple_ca = self._input.param("multiple_ca", None)
        all_node_upload = self._input.param("all_node_upload", False)
        rotate_certs = self._input.param("rotate_certs", None)
        delete_certs = self._input.param("delete_certs", None)
        restart_pkey_nodes = self._input.param("restart_pkey_nodes", None)

        if not self.apply_settings_before_setup:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()

        if self.enforce_tls:
            for cluster in self.get_cluster_objects_for_input(self.enforce_tls):
                if self.tls_level == "rotate":
                    for level in self.settings_values_map["tls"]:
                        cluster.toggle_security_setting([cluster.get_master_node()], "tls", level)
                        time.sleep(5)
                else:
                    cluster.toggle_security_setting([cluster.get_master_node()], "tls", self.tls_level)

        #Revert to default (control) tls level
        if self.disable_tls:
            for cluster in self.get_cluster_objects_for_input(self.disable_tls):
                cluster.toggle_security_setting([cluster.get_master_node()], "tls")

        if self.enable_n2n:
            for cluster in self.get_cluster_objects_for_input(self.enable_n2n):
                cluster.toggle_security_setting([cluster.get_master_node()], "n2n", "enable")

        if self.disable_n2n:
            for cluster in self.get_cluster_objects_for_input(self.disable_n2n):
                cluster.toggle_security_setting([cluster.get_master_node()], "n2n")

        if self.enable_autofailover:
            for cluster in self.get_cluster_objects_for_input(self.enable_autofailover):
                cluster.toggle_security_setting([cluster.get_master_node()], "autofailover", "enable")

        if self.disable_autofailover:
            for cluster in self.get_cluster_objects_for_input(self.disable_autofailover):
                cluster.toggle_security_setting([cluster.get_master_node()], "autofailover")

        if random_setting:
            for cluster in self.get_cluster_objects_for_input(random_setting):
                setting = random.choice(list(self.settings_values_map.keys()))
                value = random.choice(self.settings_values_map.get(setting))
                cluster.toggle_security_setting([cluster.get_master_node()], setting, value)

        if multiple_ca:
            CbServer.multiple_ca = True
            self.passphrase_type = "script"
            self.encryption_type = "des3"
            self.use_client_certs = True
            self.cacert_verify = True
            CbServer.use_https = True
            CbServer.use_client_certs = self.use_client_certs
            CbServer.cacert_verify = self.cacert_verify
            for cluster in self.get_cluster_objects_for_input(multiple_ca):
                master = cluster.get_master_node()
                ntonencryptionBase().disable_nton_cluster([master])
                CbServer.x509 = x509main(host=master)
                for server in cluster.get_nodes():
                    CbServer.x509.delete_inbox_folder_on_server(server=server)
                CbServer.x509.generate_multiple_x509_certs(servers=cluster.get_nodes())
                if all_node_upload:
                    for node_num in range(len(cluster.get_nodes())):
                        CbServer.x509.upload_root_certs(server=cluster.get_nodes()[node_num],
                                            root_ca_names=[CbServer.x509.root_ca_names[node_num]])
                else:
                    for server in cluster.get_nodes():
                        CbServer.x509.upload_root_certs(server)
                CbServer.x509.upload_node_certs(servers=cluster.get_nodes())
                CbServer.x509.upload_client_cert_settings(server=master)
                ntonencryptionBase().setup_nton_cluster([master], clusterEncryptionLevel="strict")
            if rotate_certs:
                for cluster in self.get_cluster_objects_for_input(rotate_certs):
                    CbServer.x509.rotate_certs(cluster.get_nodes())
            if delete_certs:
                for cluster in self.get_cluster_objects_for_input(delete_certs):
                    CbServer.x509.delete_trusted_CAs(cluster.get_nodes())
            if restart_pkey_nodes:
                for cluster in self.get_cluster_objects_for_input(restart_pkey_nodes):
                    for node in cluster.get_nodes()[1:]:
                        shell = RemoteMachineShellConnection(node)
                        shell.restart_couchbase()
                        shell.disconnect()
                        time.sleep(10)
                        cluster.failover_and_rebalance_nodes()
                        cluster.add_back_node("delta")

        if self.apply_settings_before_setup:
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


def tearDown(self):
        try:
            # Restore defaults - Enable autofailover
            for cluster in self.get_cluster_objects_for_input(self.disable_autofailover):
                cluster.toggle_security_setting([cluster.get_master_node()], "autofailover", "enable")
            # Disable n2n
            for cluster in self.get_cluster_objects_for_input(self.enable_n2n):
                cluster.toggle_security_setting([cluster.get_master_node()], "n2n")
            # Disable multiple_ca
            for cluster in self.get_cluster_objects_for_input(self.multiple_ca):
                CbServer.x509.teardown_certs(servers=cluster.get_nodes())
        except:
            pass