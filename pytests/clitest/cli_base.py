from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH
from testconstants import MAC_COUCHBASE_BIN_PATH
import random
import zlib

class CliBaseTest(BaseTestCase):
    vbucketId = 0
    def setUp(self):
        self.times_teardown_called = 1
        super(CliBaseTest, self).setUp()
        self.r = random.Random()
        self.vbucket_count = 1024
        self.shell = RemoteMachineShellConnection(self.master)
        self.force_failover = self.input.param("force_failover", False)
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        self.excluded_commands = self.input.param("excluded_commands", None)
        self.os = 'linux'
        self.full_v = None
        self.short_v = None
        self.build_number = None
        self.cli_command_path = LINUX_COUCHBASE_BIN_PATH
        if type == 'windows':
            self.os = 'windows'
            self.cli_command_path = WIN_COUCHBASE_BIN_PATH
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'
            self.cli_command_path = MAC_COUCHBASE_BIN_PATH
        self.full_v, self.short_v, self.build_number = self.shell.get_cbversion(type)
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
        self.cli_command = self.input.param("cli_command", None)
        self.command_options = self.input.param("command_options", None)
        if self.command_options is not None:
            self.command_options = self.command_options.split(";")
        if str(self.__class__).find('couchbase_clitest.CouchbaseCliTest') == -1:
            servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
            self.cluster.rebalance(self.servers[:1], servers_in, [])

    def tearDown(self):
        if not self.input.param("skip_cleanup", True):
            if self.times_teardown_called > 1 :
                self.shell.disconnect()
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called > 1:
                self.shell.disconnect()
        self.times_teardown_called += 1
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)
        super(CliBaseTest, self).tearDown()

    """ in sherlock, there is an extra value called runCmd in the 1st element """
    def del_runCmd_value(self, output):
        if "runCmd" in output[0]:
            output = output[1:]
        return output
