import threading
from tuq import QueryTests
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection

class QueriesUpgradeTests(QueryTests, NewUpgradeBaseTest):

    def setUp(self):
        super(QueriesUpgradeTests, self).setUp()
        self.rest = None
        self._kill_all_processes_cbq()

    def suite_setUp(self):
        super(QueriesUpgradeTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesUpgradeTests, self).tearDown()
        if self._testMethodName == 'suite_tearDown' and str(self.__call__).find('setUp') == -1:
            for th in threading.enumerate():
                th._Thread__stop() if th != threading.current_thread() else None


    def suite_tearDown(self):
        super(QueriesUpgradeTests, self).suite_tearDown()

    def test_mixed_cluster(self):
        self.assertTrue(len(self.servers) > 1, 'Test needs more than 1 server')
        method_name = self.input.param('to_run', 'test_all_negative')
        self._install(self.servers[:2])
        self.bucket_size = 100
        self._bucket_creation()
        self.load(self.gens_load, flag=self.item_flag)
        upgrade_threads = self._async_update(self.upgrade_versions[0], [self.servers[1]], None, True)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.cluster.rebalance(self.servers[:1], self.servers[1:2], [])
        self.shell = RemoteMachineShellConnection(self.servers[1])
        self._kill_all_processes_cbq()
        self._start_command_line_query(self.servers[1])
        self.shell.execute_command("ps -aef| grep cbq-engine")
        self.master = self.servers[1]
        getattr(self, method_name)()

    def test_upgrade(self):
        method_name = self.input.param('to_run', 'test_any')
        self._install(self.servers[:2])
        self.bucket_size = 100
        self._bucket_creation()
        self.load(self.gens_load, flag=self.item_flag)
        self.cluster.rebalance(self.servers[:1], self.servers[1:2], [])
        upgrade_threads = self._async_update(self.upgrade_versions[0], self.servers[:2])
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self._kill_all_processes_cbq()
        self._start_command_line_query(self.master)
        self.create_primary_index_for_3_0_and_greater()
        getattr(self, method_name)()
