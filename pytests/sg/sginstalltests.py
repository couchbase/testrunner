from sg.sg_base import GatewayBaseTest
from remote.remote_util import RemoteMachineShellConnection
import time

help_string = ['This script creates an init service to run a sync_gateway instance.',
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
        super(SGInstallerTest, self).setUp()
        self.input.test_params["default_bucket"] = False

    def tearDown(self):
        super(SGInstallerTest, self).tearDown()

    def installBasic(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.install(shell)
            pid = self.is_sync_gateway_process_running(shell)
            self.assertNotEqual(pid, 0)
            exist = shell.file_exists('{0}/tmp/'.format(self.folder_prefix), 'gateway.log')
            self.assertTrue(exist)
            shell.disconnect()

    def serviceInstallNoSyncGatewayUser(self):
        try:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                self.assertTrue(self.service_clean(shell))
                self.assertTrue(self.install_gateway(shell))
                self.assertTrue(self.remove_user(shell, "sync_gateway"))
                output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
                self.assertEqual(error, [self.expected_error])
                self.assertEqual(output, [])
                self.assertFalse(self.is_sync_gateway_service_running(shell))
                self.assertFalse(self.is_sync_gateway_process_running(shell))
                self.assertFalse(shell.file_exists(self.logsdir, 'sync_gateway_error.log'))
                self.assertFalse(shell.file_exists(self.datadir, 'data'))
                self.assertFalse(shell.file_exists(self.configdir, self.configfile))
        finally:
            self.add_user(shell, "sync_gateway")

    def serviceInstallBasic(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            shell.execute_command_raw(
                'rm -rf {0}/* {1}/* {2}/sync_gateway.json {3}/tmp/test*; mkdir {3}/tmp/test {3}/tmp/test2'.
                    format(self.logsdir, self.datadir, self.configdir, self.folder_prefix))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.check_normal_error_output(shell, output, error)
            self.assertTrue(self.is_sync_gateway_service_running(shell))
            self.assertTrue(self.is_sync_gateway_process_running(shell))
            if not "--runbase" in self.extra_param:
                # hardcoded for services LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
                self.datadir = '/home/sync_gateway'
            if not "--logsdir" in self.extra_param:
                self.logsdir = '/home/sync_gateway/logs'
            if not "--cfgpath" in self.extra_param:
                self.configdir = '/home/sync_gateway'
            self.assertTrue(shell.file_exists(self.logsdir, 'sync_gateway_error.log'))
            self.assertTrue(shell.file_exists(self.datadir, 'data'))
            self.assertTrue(shell.file_exists(self.configdir, self.configfile))

    def serviceInstallSGPath(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            shell.execute_command_raw('mv /opt/couchbase-sync-gateway/bin /opt/couchbase-sync-gateway/bin2 ')
            output, error = self.run_sync_gateway_service_install(shell,
                                                                  '--sgpath=/opt/couchbase-sync-gateway/bin2/sync_gateway')
            self.check_normal_error_output(shell, output, error)
            self.assertTrue(self.is_sync_gateway_service_running(shell))
            self.assertTrue(self.is_sync_gateway_process_running(shell))
            self.assertTrue(shell.file_exists('/home/sync_gateway/logs', 'sync_gateway_error.log'))
            self.assertTrue(shell.file_exists('/home/sync_gateway', 'data'))
            self.assertTrue(shell.file_exists('/home/sync_gateway', self.configfile))

    def serviceInstallMultipleTimes(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.check_normal_error_output(shell, output, error)
            self.assertTrue(self.is_sync_gateway_service_running(shell))
            for i in range(3):
                output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
                self.assertTrue(self.check_job_already_running(shell, output, error))
                self.assertTrue(self.is_sync_gateway_service_running(shell))
                self.assertTrue(self.is_sync_gateway_process_running(shell))

    def serviceInstallThenStartService(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.check_normal_error_output(shell, output, error)
            self.assertTrue(self.is_sync_gateway_service_running(shell))
            self.assertTrue(self.is_sync_gateway_process_running(shell))
            for i in range(3):
                output, error = self.start_gateway_service(shell)
                self.assertTrue(self.check_job_already_running(shell, output, error))
                self.assertTrue(self.is_sync_gateway_service_running(shell))
                self.assertTrue(self.is_sync_gateway_process_running(shell))

    def serviceInstallStopStartServiceMultipleTimes(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.check_normal_error_output(shell, output, error)
            self.assertTrue(self.is_sync_gateway_service_running(shell))
            self.assertTrue(self.is_sync_gateway_process_running(shell))
            for i in range(2):
                output, error = self.stop_gateway_service(shell)
                self.assertFalse(self.is_sync_gateway_process_running(shell))
                self.assertFalse(self.is_sync_gateway_service_running(shell))
                output, error = self.start_gateway_service(shell)
                self.assertTrue(self.is_sync_gateway_service_running(shell))
                self.assertTrue(self.is_sync_gateway_process_running(shell))

    def serviceInstallHelp(self):
        shell = RemoteMachineShellConnection(self.master)
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.assertTrue(self.install_gateway(shell))
        output, error = self.run_sync_gateway_service_install(shell, "-h")
        self.assertEqual(error, [])
        self.assertEqual(output, help_string)
        output, error = self.run_sync_gateway_service_install(shell, "--help")
        self.assertEqual(error, [])
        self.assertEqual(output, help_string)
        shell.disconnect()

    def serviceInstallNegative(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.assertEqual(error, [self.expected_error])
            self.assertEqual(output, [])
            self.assertFalse(self.is_sync_gateway_service_running(shell))
            self.assertFalse(self.is_sync_gateway_process_running(shell))
            self.assertFalse(shell.file_exists(self.logsdir, 'sync_gateway_error.log'))
            self.assertFalse(shell.file_exists(self.datadir, 'data'))
            self.assertFalse(shell.file_exists(self.configdir, self.configfile))

    def serviceInstallNegativeCfgPath(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.assertTrue(error[0].startswith(self.expected_error))
            time.sleep(3)
            # self.assertEqual(output, [])
            # self.assertFalse(shell.file_exists("/tmp/sync_gateway", 'logs'))
            # self.assertFalse(shell.file_exists("/tmp/sync_gateway", 'data'))
            self.assertFalse(shell.file_exists("/tmp/sync_gateway", 'sync_gateway.json'))
            self.assertFalse(self.is_sync_gateway_service_running(shell))
            self.assertFalse(self.is_sync_gateway_process_running(shell))

    def serviceInstallLogsDirNotExist(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.assertTrue(self.service_clean(shell))
            self.assertTrue(self.install_gateway(shell))
            output, error = self.run_sync_gateway_service_install(shell, self.extra_param)
            self.check_normal_error_output(shell, output, error)
            self.assertTrue(self.is_sync_gateway_service_running(shell))
            self.assertTrue(self.is_sync_gateway_process_running(shell))
            self.assertTrue(shell.file_exists(self.logsdir, 'sync_gateway_error.log'))
            self.assertTrue(shell.file_exists(self.datadir, 'data'))
            self.assertTrue(shell.file_exists(self.configdir, self.configfile))

    def serviceInstallBadParameters(self):
        shell = RemoteMachineShellConnection(self.master)
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.assertTrue(self.install_gateway(shell))
        output, error = self.run_sync_gateway_service_install(shell, "-runbase /tmp/test")
        temp_help = ["ERROR: unknown parameter \"-runbase\""]
        temp_help.extend(help_string)
        self.assertEqual(error, [])
        self.assertEqual(output, temp_help)

        output, error = self.run_sync_gateway_service_install(shell, "-r/tmp/test")
        temp_help = ["ERROR: unknown parameter \"-r/tmp/test\""]
        temp_help.extend(help_string)
        self.assertEqual(error, [])
        self.assertEqual(output, temp_help)

        output, error = self.run_sync_gateway_service_install(shell, "-r /tmp/test")
        temp_help = ["ERROR: unknown parameter \"-r\""]
        temp_help.extend(help_string)
        self.assertEqual(error, [])
        self.assertEqual(output, temp_help)

        output, error = self.run_sync_gateway_service_install(shell, "-runbase==/tmp/test")
        temp_help = ["ERROR: unknown parameter \"-runbase\""]
        temp_help.extend(help_string)
        self.assertEqual(error, [])
        self.assertEqual(output, temp_help)

        output, error = self.run_sync_gateway_service_install(shell, "runbase=/tmp/test")
        temp_help = ["ERROR: unknown parameter \"runbase\""]
        temp_help.extend(help_string)
        self.assertEqual(error, [])
        self.assertEqual(output, temp_help)
        shell.disconnect()
