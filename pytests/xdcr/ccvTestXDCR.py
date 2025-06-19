
import os
import subprocess
import time

from TestInput import TestInputSingleton
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.remote.remote_util import RemoteMachineShellConnection
from .xdcrnewbasetests import XDCRNewBaseTest

class CCVTestXDCR(XDCRNewBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.mobileimport_path = self.input.param("mobileimport_path", "/tmp/")
        self.couchbase_url = self.input.param("base_version_package_url", "https://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/neo/7604/couchbase-server-enterprise_7.2.5-7604-linux_amd64.deb")
        self.couchbase_upgrade_url = self.input.param("upgrade_version_package_url", "https://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/morpheus/3264/couchbase-server-enterprise_8.0.0-3264-linux_amd64.deb")
        self.use_mobile = self.input.param("use_mobile", False)
        self._install_base_version()
        print(f"Restored couchbase version to {self.couchbase_url.split('/')[-1]}")
        time.sleep(20)
        super().setUp()
        self.cluster_a = self.get_cb_cluster_by_name("C1")
        self.cluster_b = self.get_cb_cluster_by_name("C2")
        self.cluster_c = self.get_cb_cluster_by_name("C3")
        self.cluster_d = self.get_cb_cluster_by_name("C4")

    def _upgrade(self, server):
        remote_conn = RemoteMachineShellConnection(server)
        cmd = f"wget -P /tmp/ {self.couchbase_upgrade_url}"
        o, err = remote_conn.execute_command(cmd, use_channel=True, timeout=300)
        if err:
            self.fail("Error Upgrading to server")
        else:
            self.log.info(f"Downloaded package")
        cmd = f"""systemctl stop couchbase-server ; sleep 5 ; cd / ; dpkg -i /tmp/{self.couchbase_upgrade_url.split('/')[-1]}"""
        o, err = remote_conn.execute_command(cmd, use_channel=True, timeout=300)
        if err:
            self.fail("Error Upgrading to server")
        else:
            self.log.info(f"Upgraded {server.ip}")
        remote_conn.disconnect()

    def _install_base_version(self):
        servers = self.servers
        for server in servers:
            remote_conn = RemoteMachineShellConnection(server)
            cmd = f"wget -P /tmp/ {self.couchbase_url}"
            o, err = remote_conn.execute_command(cmd, use_channel=True, timeout=300)
            if err:
                self.fail("Error Upgrading to server")
            cmd = f"""systemctl stop couchbase-server ; dpkg -r couchbase-server ; sleep 5"""
            o, err = remote_conn.execute_command(cmd, use_channel=True, timeout=300)
            if err:
                self.fail("Error stopping and cleaning couchbase")
            cmd = f""" rm -rf /opt/couchbase ; cd / ; dpkg -i /tmp/{self.couchbase_url.split('/')[-1]}"""
            o, err = remote_conn.execute_command(cmd, use_channel=True, timeout=300)
            if err:
                self.fail("Error installing couchbase")
            remote_conn.disconnect()
    def toggle_cross_cluster_version(self, server):
        try:
            cmd = f"curl -v -X POST localhost:8091/pools/default/buckets/default -d enableCrossClusterVersioning=true -u {server.rest_username}:{server.rest_password}"
            shell_conn = RemoteMachineShellConnection(server)
            shell_conn.execute_command(cmd, use_channel=True, timeout=3)
        except Exception as e:
            self.fail(f"Failed to enable cross cluster versioning on {server.hostname}")
    def create_mixed_mode(self):
        servers_to_upgrade = self.cluster_b.get_nodes()
        servers_to_upgrade.extend(self.cluster_c.get_nodes())
        for node in servers_to_upgrade:
            try:
                self._upgrade(node)
            except Exception as e:
                self.fail("Exception happened: ", str(e))
        self.log.info("Created mixed mode")
        self.sleep(60, "sleeping after upgrade")
        self.toggle_cross_cluster_version(self.cluster_b.get_master_node())
        self.toggle_cross_cluster_version(self.cluster_c.get_master_node())

    def setup_mobileimport_sim(self):
        try:
            cmd = f"cd {os.path.dirname(self.mobileimport_path)} && git clone https://github.com/couchbaselabs/MobileImportSim.git"
            process = subprocess.Popen(cmd, shell=True)
            process.wait()
            if process.returncode != 0:
                raise Exception("Failed to clone MobileImportSim repository")
            self.log.info("Successfully cloned MobileImportSim repository")
        except Exception as e:
            self.fail(f"Failed to setup MobileImportSim: {str(e)}")
        try:
            build_cmds = [
                f"cd {self.mobileimport_path}MobileImportSim && make deps && make clean && make deps && make"
            ]
            for cmd in build_cmds:
                process = subprocess.Popen(cmd, shell=True)
                process.wait()
                if process.returncode != 0:
                    self.log.info(f"Build command failed: {cmd}")
            self.log.info("Successfully built MobileImportSim")
        except Exception as e:
            self.fail(f"Failed to build MobileImportSim: {str(e)}")
    def cleanup_mobileimport_sim(self):
        try:
            cmd = f"rm -rf {self.mobileimport_path}MobileImportSim"
            process = subprocess.Popen(cmd, shell=True)
            process.wait()
            if process.returncode != 0:
                raise Exception("Failed to remove MobileImportSim directory")
            self.log.info("Successfully removed MobileImportSim directory")
        except Exception as e:
            self.fail(f"Failed to cleanup MobileImportSim: {str(e)}")
    def enable_mobileimport_sim(self):
        try:
            ips = []
            for cluster in [self.cluster_b, self.cluster_c]:
                ips.append(cluster.get_master_node().ip)
            processes = []
            for ip in ips:
                cmd = f"{self.mobileimport_path}MobileImportSim/mobileImportSim -username Administrator -password password -bucketname default -hostAddr {ip}:8091"
                # Run locally instead of on remote node
                process = subprocess.Popen(cmd, shell=True)
                processes.append(process)
        except Exception as e:
            self.fail(f"Failed to run mobileImportSim: {str(e)}")
    def test_replication_with_mixed_mode(self):
        self.setup_xdcr()
        self.create_mixed_mode()
        self.toggle_cross_cluster_version(self.cluster_b.get_master_node())
        self.toggle_cross_cluster_version(self.cluster_c.get_master_node())
        if self.use_mobile:
            self.setup_mobileimport_sim()
            self.enable_mobileimport_sim()
        gen = BlobGenerator("doc-", "doc-", value_size=300, start=0, end=1000)
        self.cluster_b.load_all_buckets_from_generator(gen)
        gen = BlobGenerator("doc-", "doc-", value_size=310, start=0, end=1000)
        self.cluster_c.load_all_buckets_from_generator(gen)
        self.sleep(30, "sleeping to wait for docs to be populated")
        self._wait_for_replication_to_catchup(timeout=200)
        errors = self.check_errors_in_goxdcr_logs()
        self.log.info("ERRORS: ", errors)
        if self.use_mobile:
            self.cleanup_mobileimport_sim()
