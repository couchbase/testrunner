import re
import time
import testconstants
from builds.build_query import BuildQuery
from newupgradebasetest import NewUpgradeBaseTest

class SingleNodeUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(SingleNodeUpgradeTests, self).setUp()

    def tearDown(self):
        super(SingleNodeUpgradeTests, self).tearDown()

    def _get_build(self, master, version, is_amazon=False):
        #log = logger.Logger.get_logger()
        #remote = RemoteMachineShellConnection(master)
        info = self.remote.extract_remote_info()
        #remote.disconnect()
        builds, changes = BuildQuery().get_all_builds()
        self.log.info("finding build {0} for machine {1}".format(version, master))
        result = re.search('r', version)

        if result is None:
            appropriate_build = BuildQuery().\
                find_membase_release_build(self.product, info.deliverable_type,
                    info.architecture_type, version.strip(), is_amazon=is_amazon)
        else:
            appropriate_build = BuildQuery().\
                find_membase_build(builds, self.product, info.deliverable_type,
                    info.architecture_type, version.strip(), is_amazon=is_amazon)

        return appropriate_build

    def _upgrade(self, upgrade_version):
        appropriate_build = self._get_build(self.servers[0], upgrade_version)
        self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(upgrade_version))
        self.remote.download_build(appropriate_build)
        self.remote.membase_upgrade(appropriate_build, save_upgrade_config=False)
        self.rest_helper.is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
        self.rest.init_cluster_port(self.rest_settings.rest_username, self.rest_settings.rest_password)
        time.sleep(self.sleep_time)

    def _install(self, initial_version='1.8.1-942-rel'):
        self.remote.couchbase_uninstall()
        builds, changes = BuildQuery().get_all_builds()
        # check to see if we are installing from latestbuilds or releases
        if re.search('r', initial_version):
            builds, changes = BuildQuery().get_all_builds()
            older_build = BuildQuery().find_membase_build(builds, deliverable_type=self.info.deliverable_type,
                                                          os_architecture=self.info.architecture_type,
                                                          build_version=initial_version,
                                                          product=self.product, is_amazon=False)
        else:
            older_build = BuildQuery().find_membase_release_build(deliverable_type=self.info.deliverable_type,
                                                                  os_architecture=self.info.architecture_type,
                                                                  build_version=initial_version,
                                                                  product=self.product, is_amazon=False)
        self.remote.download_build(older_build)
        #Installation ?
        self.remote.membase_install(older_build)
        ns_running = self.rest_helper.is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
        self.assertTrue(ns_running, "NS_Server is not running")
        self.rest.init_cluster_port(self.rest_settings.rest_username, self.rest_settings.rest_password)

    def test_upgrade(self):
        # singlenode paramaterized install
        initial_version = self.input.param('initial_version', '1.8.1-942-rel')
        if initial_version.startswith("1.6") or initial_version.startswith("1.7"):
            self.product = 'membase-server-enterprise'
        else:
            self.product = 'couchbase-server-enterprise'
        self._install(initial_version=initial_version)
        self.operations()
        upgrade_versions = self.input.param('upgrade_version', '1.8.1-942-rel')
        upgrade_versions = upgrade_versions.split(";")
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        time.sleep(self.sleep_time)
        for upgrade_version in upgrade_versions:
            self._upgrade(upgrade_version)
            self.verfication()
        self.remote.disconnect()
