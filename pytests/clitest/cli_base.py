from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection

class CliBaseTest(BaseTestCase):
    def setUp(self):
        self.times_teardown_called = 1
        super(CliBaseTest, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
        self.command_options = self.input.param("command_options", None)
        if self.command_options is not None:
            self.command_options = self.command_options.split(";")
        servers_in = [self.servers[i+1] for i in range(self.num_servers-1)]
        self.cluster.rebalance(self.servers[:1], servers_in, [])

    def tearDown(self):
        super(CliBaseTest, self).tearDown()
        if not self.input.param("skip_cleanup", True):
            if times_tear_down_called > 1 :
                self.shell.disconnect()
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called >1:
                self.shell.disconnect()
        self.times_teardown_called +=1
