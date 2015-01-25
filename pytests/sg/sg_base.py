import unittest
import logging
import logger
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
import requests
from requests.exceptions import ConnectionError


class GatewayBaseTest(unittest.TestCase):

    BUILDS = {
        'http://packages.couchbase.com/builds/mobile/sync_gateway': (
            '0.0.0/{0}/couchbase-sync-gateway_{0}_x86_64-community.rpm',
            '1.0.0/{0}/couchbase-sync-gateway_{0}_x86_64.rpm',
        ),
        'http://packages.couchbase.com.s3.amazonaws.com/builds/mobile/sync_gateway': (
            '0.0.0/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.0/{0}/couchbase-sync-gateway-enterprise_{0}_x86_64.rpm',
            '1.0.1/{0}/couchbase-sync-gateway-enterprise_{0}_x86_64.rpm',
            '1.0.2/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.3/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.4/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.1.0/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
        ),
        'http://cbfs-ext.hq.couchbase.com/builds': (
            'couchbase-sync-gateway_{}_x86_64-community.rpm',
        ),
        'http://latestbuilds.hq.couchbase.com/couchbase-sync-gateway': (
            '0.0.0/416/couchbase-sync-gateway-enterprise_0.0.0-326_x86_64.rpm',
        ),
    }

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.version = self.input.param("version", "0.0.0-422")
        self.extra_param = self.input.param("extra_param", "").replace("$", "=")  # '=' is a delimiter in conf file
        self.expected_error = self.input.param("expected_error", "")
        self.servers = self.input.servers
        self.master = self.servers[0]


    def find_package(self):
        for filename, url in self.get_expected_locations():
            try:
                status_code = requests.head(url).status_code
            except ConnectionError:
                pass
            else:
                if status_code == 200:
                    self.log.info('Found "{}"'.format(url))
                    return filename, url
        self.log.interrupt('Target build not found')

    def get_expected_locations(self):
        for location, patterns in self.BUILDS.items():
            for pattern in patterns:
                url = '{}/{}'.format(location, pattern.format(self.version))
                filename = url.split('/')[-1]
                yield filename, url

    def kill_processes_gateway(self, shell):
        self.log.info('Killing Sync Gateway')
        shell.execute_command('killall -9 sync_gateway sgw_test_info.sh sar')  # ???? check command


    def uninstall_gateway(self, shell):
        self.log.info('Uninstalling Sync Gateway package')
        output, error = shell.execute_command('yes | yum remove couchbase-sync-gateway')
        shell.log_command_output(output, error)
        # do we need any additional cleanup?

    def install_gateway(self, shell):
        filename, url = self.find_package()
        self.log.info('Installing Sync Gateway package - {}'.format(filename))
        output, error = shell.execute_command_raw('cd /tmp;wget -q -O {0} {1};cd /tmp;ls -lh'.format(filename, url))
        shell.log_command_output(output, error)
        output, error = shell.execute_command_raw('yes | rpm -i /tmp/{}'.format(filename))
        shell.log_command_output(output, error)


    def start_sync_gateways(self, shell):
          self.log.info('Starting Sync Gateway instances')
          output, error = shell.execute_command_raw('nohup /opt/couchbase-sync-gateway/bin/sync_gateway'
                                                   '/root/gateway_config.json >/root/gateway.log 2>&1 &')
          shell.log_command_output(output, error)


    def install(self, shell):
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.install_gateway(shell)
        self.start_sync_gateways(shell)

    def run_sync_gateway_service_install(self, shell, options=""):
        output, error = shell.execute_command_raw('/opt/couchbase-sync-gateway/service/sync_gateway_service_install.sh ' + options)
        shell.log_command_output(output, error)
        return output, error

