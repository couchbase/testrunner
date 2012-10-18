import time
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection

class SingleNodeUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(SingleNodeUpgradeTests, self).setUp()

    def tearDown(self):
        super(SingleNodeUpgradeTests, self).tearDown()

    def test_upgrade(self):
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'
        self._install([self.master])
        self.operations()
        upgrade_versions = self.input.param('upgrade_version', '2.0.0-1856-rel')
        upgrade_versions = upgrade_versions.split(";")
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        time.sleep(self.sleep_time)
        for upgrade_version in upgrade_versions:
            remote = RemoteMachineShellConnection(self.master)
            self._upgrade(upgrade_version, self.master, remote)
            time.sleep(self.expire_time+5)
            for bucket in self.buckets:
                remote.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
            time.sleep(30)
            remote.disconnect()
            self.verfication()

class MultiNodesUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(MultiNodesUpgradeTests, self).setUp()

    def tearDown(self):
        super(MultiNodesUpgradeTests, self).tearDown()

    def test_upgrade(self):
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'
        self._install(self.servers)
        self.operations(multi_nodes=True)
        upgrade_versions = self.input.param('upgrade_version', '2.0.0-1863-rel')
        upgrade_versions = upgrade_versions.split(";")
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        time.sleep(self.sleep_time)
        for upgrade_version in upgrade_versions:
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                self._upgrade(upgrade_version, server, remote)
                remote.disconnect()
            time.sleep(self.expire_time+5)
            self.verfication(multi_nodes=True)