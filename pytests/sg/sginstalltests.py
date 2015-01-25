

from basetestcase import BaseTestCase

from TestInput import TestInputSingleton
from sg.sg_base import GatewayBaseTest
from remote.remote_util import RemoteMachineShellConnection

help = ['This script creates an init service to run a sync_gateway instance.',
        'If you want to install more than one service instance',
        'create additional services with different names.',
        '', 'sync_gateway_service_install.sh', '    -h --help',
        '    --runas=<The user account to run sync_gateway as; default (sync_gateway)>',
        '    --runbase=<The directory to run sync_gateway from; defaut (/home/sync_gateway)>',
        '    --sgpath=<The path to the sync_gateway executable; default (/opt/couchbase-sync-gateway/bin/sync_gateway)>',
        '    --cfgpath=<The path to the sync_gateway JSON config file; default (/home/sync_gateway/sync_gateway.json)>',
        '    --logsdir=<The path to the log file direcotry; default (/home/sync_gateway/logs)>', '']


class SGInstallerTest(GatewayBaseTest):
    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(SGInstallerTest, self).setUp()

    def tearDown(self):
        super(SGInstallerTest, self).tearDown()


    def basicInstall(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.install(shell)
            self.kill_processes_gateway(shell)
            self.uninstall_gateway(shell)
            shell.disconnect()


    def testSGServiceInstallHelp(self):
        shell = RemoteMachineShellConnection(self.master)
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.install_gateway(shell)
        output, error = self.run_sync_gateway_service_install(shell, "-h")
        self.assertEqual(error, [])
        self.assertEqual(output, help)

        output, error = self.run_sync_gateway_service_install(shell)
        self.assertEqual(error[0], "The sync_gateway runtime user account does not exist \"sync_gateway\".")
        self.assertEqual(output, [])

        output, error = self.run_sync_gateway_service_install(shell, "bla-bla-bla")
        temp_help = ["ERROR: unknown parameter \"bla-bla-bla\""]
        temp_help.extend(help)

        self.assertEqual(error, [])
        self.assertEqual(output, temp_help)
        shell.disconnect()


    def testSGServiceInstallNoUser(self):
        shell = RemoteMachineShellConnection(self.master)
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.install_gateway(shell)
        output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
        self.assertEqual(error, [self.expected_error])
        self.assertEqual(output, [])
        shell.disconnect()