from sg.sg_config_base import GatewayConfigBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.bucket_helper import BucketOperationHelper
import shutil
import time
from couchbase_helper.cluster import Cluster

help_string = ['Usage of /opt/couchbase-sync-gateway/bin/sync_gateway:',
               '  -adminInterface="127.0.0.1:4985": Address to bind admin interface to',
               '  -bucket="sync_gateway": Name of bucket',
               '  -configServer="": URL of server that can return database configs',
               '  -dbname="": Name of Couchbase Server database (defaults to name of bucket)',
               '  -deploymentID="": Customer/project identifier for stats reporting',
               '  -interface=":4984": Address to bind to',
               '  -log="": Log keywords, comma separated',
               '  -logFilePath="": Path to log file',
               '  -personaOrigin="": Base URL that clients use to connect to the server',
               '  -pool="default": Name of pool',
               '  -pretty=false: Pretty-print JSON responses',
               '  -profileInterface="": Address to bind profile interface to',
               '  -url="walrus:": Address of Couchbase server',
               '  -verbose=false: Log more info about requests']


class SGConfigTests(GatewayConfigBaseTest):
    def setUp(self):
        super(SGConfigTests, self).setUp()
        for server in self.servers:
            if self.case_number == 1:
                with open('pytests/sg/resources/gateway_config_walrus_template.json', 'r') as file:
                    filedata = file.read()
                    filedata = filedata.replace('LOCAL_IP', server.ip)
                with open('pytests/sg/resources/gateway_config_walrus.json', 'w') as file:
                    file.write(filedata)
                shell = RemoteMachineShellConnection(server)
                shell.execute_command("rm -rf {0}/tmp/*".format(self.folder_prefix))
                shell.copy_files_local_to_remote('pytests/sg/resources', '{0}/tmp'.format(self.folder_prefix))
                # will install sg only the first time
                self.install(shell)
                pid = self.is_sync_gateway_process_running(shell)
                self.assertNotEqual(pid, 0)
                exist = shell.file_exists('{0}/tmp/'.format(self.folder_prefix), 'gateway.log')
                self.assertTrue(exist)
                shell.disconnect()
        if self.case_number == 1:
            shutil.copy2('pytests/sg/resources/gateway_config_backup.json', 'pytests/sg/resources/gateway_config.json')
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            self.cluster = Cluster()
            self.cluster.create_default_bucket(self.master, 150)
            task = self.cluster.async_create_sasl_bucket(self.master, 'test_%E-.5', 'password', 150, 1)
            task.result()
            task = self.cluster.async_create_standard_bucket(self.master, 'db', 11219, 150, 1)
            task.result()

    def tearDown(self):
        super(SGConfigTests, self).tearDown()
        if self.case_number == 1:
            self.cluster.shutdown(force=True)

    def configHelp(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command_raw('/opt/couchbase-sync-gateway/bin/sync_gateway -help')
            for index, str in enumerate(help_string):
                if index != help_string[index]:
                    self.log.info('configHelp found unmatched help text. error({0}), help({1})'.format(error[index],
                                                                                                       help_string[
                                                                                                           index]))
                self.assertEqual(error[index], help_string[index])
            shell.disconnect()

    def configCreateUser(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.config = 'gateway_config_walrus.json'
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.create_user(shell))
            if not self.expected_stdout:
                self.assertTrue(self.get_user(shell))
                self.delete_user(shell)
            shell.disconnect()

    def configGuestUser(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.config = 'gateway_config_walrus.json'
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.get_user(shell))
            self.assertFalse(self.delete_user(shell))
            shell.disconnect()

    def configCreateRole(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.config = 'gateway_config_walrus.json'
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.create_role(shell, self.role_name, self.admin_channels))
            if not self.expected_stdout:
                self.assertTrue(self.get_role(shell))
                self.delete_role(shell)
            shell.disconnect()

    def configUserRolesChannels(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.config = 'gateway_config_walrus.json'
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.parse_input_create_roles(shell))
            self.assertTrue(self.create_user(shell))
            if not self.expected_stdout:
                self.assertTrue(self.get_user(shell))
                self.delete_user(shell)
            shell.disconnect()

    def configUserRolesNotExist(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.config = 'gateway_config_walrus.json'
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.create_user(shell))
            if not self.expected_stdout:
                self.assertTrue(self.get_user(shell))
                self.delete_user(shell)
            shell.disconnect()

    def configInspectDocChannel(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.config = 'gateway_config_walrus.json'
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.parse_input_create_roles(shell))
            if self.doc_channels:
                success, revision = self.create_doc(shell)
                self.assertTrue(success)
                self.assertTrue(self.get_all_docs(shell))
                self.assertTrue(self.delete_doc(shell, revision))
            shell.disconnect()

    def configCBS(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shutil.copy2('pytests/sg/resources/gateway_config_backup.json', 'pytests/sg/resources/gateway_config.json')
            self.assertTrue(self.start_sync_gateway_template(shell, self.template))
            if not self.expected_error:
                time.sleep(5)
                success, revision = self.create_doc(shell)
                self.assertTrue(success)
                self.assertTrue(self.delete_doc(shell, revision))
            self.assertTrue(self.check_message_in_gatewaylog(shell, self.expected_log))
            shell.disconnect()

    def configStartSgw(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shutil.copy2('pytests/sg/resources/gateway_config_backup.json', 'pytests/sg/resources/gateway_config.json')
            shell.copy_files_local_to_remote('pytests/sg/resources', '/tmp')
            self.assertTrue(self.start_sync_gateway(shell))
            self.assertTrue(self.check_message_in_gatewaylog(shell, self.expected_log))
            if not self.expected_error:
                if self.admin_port:
                    self.assertTrue(self.get_users(shell))
                if self.sync_port:
                    success, revision = self.create_doc(shell)
                    self.assertTrue(success)
                    self.assertTrue(self.delete_doc(shell, revision))
            shell.disconnect()
