
import os
import subprocess
import time

from TestInput import TestInputSingleton
from testconstants import CB_VERSION_NAME
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.remote.remote_util import RemoteMachineShellConnection
from .xdcrnewbasetests import XDCRNewBaseTest

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper

class CCVTestXDCR(XDCRNewBaseTest):
    # Tests that start with every node on base_version_package_url. The others
    # (e.g. the 10K collections test) run on the build the job installed.
    BASE_VERSION_TESTS = ("test_replication_with_mixed_mode",)

    def setUp(self):
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.mobileimport_path = self.input.param("mobileimport_path", "/tmp/")
        # A conf value can keep its quotes (upgrade_version_package_url="https://..."),
        # and a quote left on the URL ends up in the dpkg path built from it.
        self.couchbase_url = self.input.param("base_version_package_url", "https://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/neo/7604/couchbase-server-enterprise_7.2.5-7604-linux_amd64.deb").strip('"\'')
        self.couchbase_upgrade_url = self.input.param("upgrade_version_package_url", "https://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/morpheus/3264/couchbase-server-enterprise_8.0.0-3264-linux_amd64.deb").strip('"\'')
        self.use_mobile = self.input.param("use_mobile", False)
        restore_url = None
        try:
            if self._testMethodName in self.BASE_VERSION_TESTS:
                installed_url = self._installed_package_url()
                # Downgrading every node is only safe if the build can be fetched back.
                remote_conn = RemoteMachineShellConnection(self.servers[0])
                try:
                    self._run_checked(remote_conn, f"wget -q --spider {installed_url}",
                                      f"Reaching the restore build {installed_url}")
                finally:
                    remote_conn.disconnect()
                restore_url = installed_url
                self._install_version(self.couchbase_url)
                print(f"Restored couchbase version to {self.couchbase_url.split('/')[-1]}")
                time.sleep(20)
            super().setUp()
            self.cluster_a = self.get_cb_cluster_by_name("C1")
            self.cluster_b = self.get_cb_cluster_by_name("C2")
        except BaseException:
            # tearDown does not run after a failed setUp
            if restore_url:
                try:
                    self._install_version(restore_url)
                except BaseException as restore_err:
                    print(f"Could not reinstall {restore_url.split('/')[-1]}: {restore_err}")
            raise
        # Armed only now: BaseTestCase.setUp can call tearDown() as a
        # pre-cleanup, and that must not undo the install above.
        self._restore_url = restore_url
        try:
            self.cluster_c = self.get_cb_cluster_by_name("C3")
        except Exception:
            self.cluster_c = None
        try:
            self.cluster_d = self.get_cb_cluster_by_name("C4")
        except Exception:
            self.cluster_d = None

    def tearDown(self):
        try:
            super().tearDown()
        finally:
            restore_url = getattr(self, "_restore_url", None)
            if restore_url:
                self._restore_url = None
                self.log.info(f"Reinstalling {restore_url.split('/')[-1]} on every node")
                self._install_version(restore_url)
                time.sleep(20)

    def _installed_package_url(self):
        """URL of the package the job installed, read from the first node, so
        that tearDown can put it back after a test downgrades every node."""
        remote_conn = RemoteMachineShellConnection(self.servers[0])
        try:
            full_version, release, build = remote_conn.get_cbversion("linux")
            arch_out, _ = remote_conn.execute_command("dpkg --print-architecture")
        finally:
            remote_conn.disconnect()
        arch = arch_out[0].strip() if arch_out and arch_out[0].strip() else "amd64"
        codename = CB_VERSION_NAME.get(".".join(release.split(".")[:2]))
        if not full_version or not codename:
            self.fail(f"Cannot tell which build is installed on {self.servers[0].ip} "
                      f"(VERSION.txt gave '{full_version}'), so it could not be restored after "
                      f"this test; not downgrading the nodes")
        return (f"https://latestbuilds.service.couchbase.com/builds/latestbuilds/couchbase-server/"
                f"{codename}/{build}/couchbase-server-enterprise_{release}-{build}-linux_{arch}.deb")

    def _run_checked(self, remote_conn, cmd, what):
        # execute_command(use_channel=True) reports no failure on its own, and its
        # get_exit_code can read -1 because that path closes the channel before
        # the exit status arrives, so the status is echoed in-band.
        o, err = remote_conn.execute_command(f"{cmd} ; echo EXIT_CODE:$?", use_channel=True,
                                             timeout=300)
        if not o or not o[-1].strip().endswith("EXIT_CODE:0"):
            self.fail(f"{what} failed on {remote_conn.ip}: {o} {err}")

    def _upgrade(self, server):
        package = self.couchbase_upgrade_url.split('/')[-1]
        remote_conn = RemoteMachineShellConnection(server)
        try:
            self._run_checked(remote_conn, f"wget -q -O /tmp/{package} {self.couchbase_upgrade_url}",
                              f"Downloading {package}")
            self.log.info(f"Downloaded package")
            self._run_checked(remote_conn, f"systemctl stop couchbase-server ; sleep 5 ; cd / ; "
                                           f"dpkg -i /tmp/{package}",
                              f"Upgrading to {package}")
            self.log.info(f"Upgraded {server.ip}")
        finally:
            remote_conn.disconnect()

    def _install_version(self, package_url):
        """Uninstall Couchbase Server from every node and install package_url."""
        package = package_url.split('/')[-1]
        for server in self.servers:
            remote_conn = RemoteMachineShellConnection(server)
            try:
                self._run_checked(remote_conn, f"wget -q -O /tmp/{package} {package_url}",
                                  f"Downloading {package}")
                cmd = f"""systemctl stop couchbase-server ; dpkg -r couchbase-server ; sleep 5"""
                o, err = remote_conn.execute_command(cmd, use_channel=True, timeout=300)
                if err:
                    self.fail("Error stopping and cleaning couchbase")
                self._run_checked(remote_conn, f"rm -rf /opt/couchbase ; cd / ; dpkg -i /tmp/{package}",
                                  f"Installing {package}")
            finally:
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
                self.fail("Exception happened: {}".format(e))
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
        self.log.info("ERRORS: {}".format(errors))
        if self.use_mobile:
            self.cleanup_mobileimport_sim()

    # ---- 10K Collections Scale Tests ----
    def test_cr_revision_id_10k_collections(self):
        """
        Default RevisionID-based conflict resolution with 10K collections.
        Loads docs on both clusters concurrently using same docid prefix to
        create conflicts, verifies CR resolves correctly at scale.
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        c1_master = self.cluster_a.get_master_node()
        c2_master = self.cluster_b.get_master_node()

        TenKCollectionHelper.create_10k_collections(
            c1_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            c2_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()
        self.sleep(30)

        TenKCollectionHelper.select_and_load(
            c1_master, bucket_name, p, run_id="revid_c1")
        TenKCollectionHelper.select_and_load(
            c2_master, bucket_name, p, run_id="revid_c2")

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 900))
        except Exception as e:
            self.fail("RevisionID CR 10K catch-up failed: {}".format(e))

        c1_count = TenKCollectionHelper.get_bucket_item_count(c1_master, bucket_name)
        c2_count = TenKCollectionHelper.get_bucket_item_count(c2_master, bucket_name)
        self.log.info("RevisionID CR: C1={}, C2={}".format(c1_count, c2_count))
        self.assertEqual(c1_count, c2_count,
                         "RevisionID CR mismatch: C1={}, C2={}".format(c1_count, c2_count))
        self.log.info("RevisionID CR 10K test passed")
