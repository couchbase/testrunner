import math

from tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError
from newupgradebasetest import NewUpgradeBaseTest

class QueriesUpgradeTests(QueryTests, NewUpgradeBaseTest):

    def setUp(self):
        super(QueriesUpgradeTests, self).setUp()
        self.rest = None
        if hasattr(self, 'shell'):
           o = self.shell.execute_command("ps -aef| grep cbq-engine")
           if len(o):
               for cbq_engine in o[0]:
                   if cbq_engine.find('grep') == -1:
                       pid = [item for item in cbq_engine.split(' ') if item][1]
                       self.shell.execute_command("kill -9 %s" % pid)

    def suite_setUp(self):
        super(QueriesUpgradeTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesUpgradeTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesUpgradeTests, self).suite_tearDown()

    def test_mixed_cluster(self):
        self.assertTrue(len(self.servers) > 1, 'Test needs more than 1 server')
        self._install(self.servers[:2])
        upgrade_threads = self._async_update(self.upgrade_versions[0], [self.servers[0]])
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        out = self._start_command_line_query(self.master)
        self.assertTrue(out.find('ERROR') != -1, 'N1ql started')

    def test_upgrade(self):
        method_name = self.input.param('to_run', 'test_any')
        self._install(self.servers[:2])
        self._start_command_line_query(self.master)
        self.create_primary_index_for_3_0_and_greater()
        getattr(self, method_name)()
        upgrade_threads = self._async_update(self.upgrade_versions[0], [self.servers[:2]])
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        o = self.shell.execute_command("ps -aef| grep cbq-engine")
        if len(o):
            for cbq_engine in o[0]:
                if cbq_engine.find('grep') == -1:
                    pid = [item for item in cbq_engine.split(' ') if item][1]
                    self.shell.execute_command("kill -9 %s" % pid)
        self._start_command_line_query(self.master)
        self.create_primary_index_for_3_0_and_greater()
        getattr(self, method_name)()
        
