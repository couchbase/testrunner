from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
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
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        self.os = 'linux'
        if type == 'windows':
            self.os = 'windows'
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
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
        super(CliBaseTest, self).tearDown()

    def _set_vbucket(self, key, vbucket= -1):
        if vbucket < 0:
            self.vbucketId = (((zlib.crc32(key)) >> 16) & 0x7fff) & (self.vbucket_count - 1)
        else:
            self.vbucketId = vbucket
