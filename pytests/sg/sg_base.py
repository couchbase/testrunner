import unittest
import logging
import logger
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper, RemoteMachineHelper
import requests
from requests.exceptions import ConnectionError


class GatewayBaseTest(unittest.TestCase):

    BUILDS = {
        'http://packages.couchbase.com.s3.amazonaws.com/builds/mobile/sync_gateway': (
            '1.0.4/{0}/couchbase-sync-gateway-community_{0}_{1}.{2}',
        ),
    }

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.version = self.input.param("version", "1.0.4-34")
        self.extra_param = self.input.param("extra_param", "").replace("$", "=")  # '=' is a delimiter in conf file
        self.expected_error = self.input.param("expected_error", "")
        self.servers = self.input.servers
        self.master = self.servers[0]

    def find_package(self, shell):
        for filename, url in self.get_expected_locations(shell):
            try:
                self.log.info('Trying to get "{0}"'.format(url))
                status_code = requests.head(url).status_code
            except ConnectionError:
                pass
            else:
                if status_code == 200:
                    self.log.info('Found "{0}"'.format(url))
                    return filename, url
        self.log.interrupt('Target build not found')

    def get_expected_locations(self, shell):
        self.info = shell.extract_remote_info()
        type = self.info.distribution_type.lower()
        if type == 'centos':
            file_ext = "rpm"
        elif type == 'ubuntu':
            file_ext = "deb"
        for location, patterns in self.BUILDS.items():
            for pattern in patterns:
                url = '{0}/{1}'.format(location, pattern.format(self.version,self.info.architecture_type,file_ext))
                filename = url.split('/')[-1]
                yield filename, url

    def install(self, shell):
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.install_gateway(shell)
        self.start_sync_gateways(shell)

    def kill_processes_gateway(self, shell):
        self.log.info('=== Killing Sync Gateway')
        shell.execute_command('killall -9 sync_gateway')

    def uninstall_gateway(self, shell):
        self.info = shell.extract_remote_info()
        if self.info.type.lower() == "linux":
            type = self.info.distribution_type.lower()
            if ( type == "centos"):
                cmd = 'yes | yum remove couchbase-sync-gateway'
            else:
                cmd = 'dpkg -r couchbase-sync-gateway'
        elif self.info.type.lower() == 'windows':
            self.log.info('verify_sync_gateway_service is not supported on windows')
            return False
        elif self.info.distribution_type.lower() == 'mac':
            self.log.info('verify_sync_gateway_service is not supported on mac')
            return False

        self.log.info('=== Un-installing Sync Gateway package on {0} - cmd: {1}'.format(self.master, cmd))
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        # do we need any additional cleanup?

    def install_gateway(self, shell):
        filename, url = self.find_package(shell)

        self.info = shell.extract_remote_info()
        if self.info.type.lower() == "linux":
            type = self.info.distribution_type.lower()
            if ( type == "centos"):
                cmd = 'yes | rpm -i /tmp/{0}'.format(filename)
            else:
                cmd = 'yes | dpkg -i /tmp/{0}'.format(filename)
        elif self.info.type.lower() == 'windows':
            self.log.info('verify_sync_gateway_service is not supported on windows')
            return False
        elif self.info.distribution_type.lower() == 'mac':
            self.log.info('verify_sync_gateway_service is not supported on mac')
            return False

        self.log.info('=== Installing Sync Gateway package {0} on {1} - cmd: {2}'.
                      format(filename, self.master, cmd))
        output, error = shell.execute_command_raw('cd /tmp; wget -q -O {0} {1};ls -lh'.format(filename, url))
        shell.log_command_output(output, error)
        output, error = shell.execute_command_raw(cmd)
        shell.log_command_output(output, error)
        exist = shell.file_exists('/opt/couchbase-sync-gateway/bin', 'sync_gateway')
        if exist:
            return True
        else:
            self.log.error('Installed file not found - /opt/couchbase-sync-gateway/service/sync_gateway_service_install.sh')
            return False

    def start_sync_gateways(self, shell):
        self.log.info('=== Starting Sync Gateway instances')
        shell.copy_files_local_to_remote('pytests/sg/resources', '/root')
        output, error = shell.execute_command_raw('nohup /opt/couchbase-sync-gateway/bin/sync_gateway'
                                                  ' /root/gateway_config.json >/root/gateway.log 2>&1 &')
        shell.log_command_output(output, error)

    def is_sync_gateway_service_running(self, shell):
        self.info = shell.extract_remote_info()
        if self.info.type.lower() == "linux":
            type = self.info.distribution_type.lower()
            if (self.info.distribution_version == "CentOS 7"):
                cmd = 'systemctl | grep sync_gateway'
            else:
                cmd = 'initctl list | grep sync_gateway'
        elif self.info.type.lower() == 'windows':
            self.log.info('verify_sync_gateway_service is not supported on windows')
            return False
        elif self.info.distribution_type.lower() == 'mac':
            self.log.info('verify_sync_gateway_service is not supported on mac')
            return False

        self.log.info('=== Check whether Sync Gateway service is running on {0} - cmd: {1}'.format(self.master, cmd))
        output, error = shell.execute_command_raw(cmd)
        shell.log_command_output(output, error)
        if output and output[0].startswith('sync_gateway start/running, process '):
            self.log.info('Sync Gateway service is running')
            return True
        else:
            self.log.info('Sync Gateway service is NOT running')
            return False

    def is_sync_gateway_process_running(self, shell):
        self.log.info('=== Check whether Sync Gateway is running on {0}'.format(self.master))
        obj = RemoteMachineHelper(shell).is_process_running("sync_gateway")
        if obj and obj.pid:
            self.log.info('Sync Gateway is running with pid of {0}'.format(obj.pid))
            return True
        else:
            self.log.info('Sync Gateway is NOT running')
            return False

    def service_clean(self, shell):
        self.log.info('=== Cleaning Sync Gateway service and remove files')
        self.uninstall_gateway(shell)
        output, error = shell.execute_command_raw('rm -rf /root/*.log /home/sync_gateway/* /opt/couchbase-sync-gateway*')
        # return true only if the service is not running
        if self.is_sync_gateway_service_running(shell):
            self.log.info('Fail to clean Sync Gateway service.   The service is still running')
            return False
        else:
            return True

    def service_install(self, shell):
        self.install_gateway(shell)
        self.run_sync_gateway_service_install(shell)

    def run_sync_gateway_service_install(self, shell, options=""):
        output, error = shell.execute_command_raw('cd /opt/couchbase-sync-gateway/service; . ./sync_gateway_service_install.sh ' + options)
        shell.log_command_output(output, error)
        return output, error

