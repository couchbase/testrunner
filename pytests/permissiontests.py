import logger

import os

from membase.api.rest_client import RestConnection, RestHelper
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import LINUX_CB_PATH

class PermissionTests(BaseTestCase):

    def setUp(self):
        super(PermissionTests, self).setUp()

    def tearDown(self):
        super(PermissionTests, self).tearDown()

    def test_permissions(self):
        shell = RemoteMachineShellConnection(self.master)
        info = shell.extract_remote_info()
        if info.type.lower() == 'windows':
            self.log.info('Test is designed for linux only')
            return
        shell.execute_command('chmod 000 %s' % LINUX_CB_PATH)
        self.sleep(10, 'wait for couchbase stopping')
        shell.execute_command('chmod 755 %s' % LINUX_CB_PATH)
        self.sleep(10, 'wait for couchbase start')
        try:
            rest = RestConnection(self.master)
            self.assertTrue(RestHelper(rest).is_ns_server_running(timeout_in_seconds=60),
                                            'NS server is not up')
        except Exception as ex:
            self.log.error('Couchbase is not running')
            shell.execute_command('reboot')
            self.sleep(60, 'wait for reboot of VM')
            rest = RestConnection(self.master)
            self.assertTrue(RestHelper(rest).is_ns_server_running(timeout_in_seconds=60),
                            'NS server is not up')
            raise ex
        finally:
            shell.disconnect()
