import logger
import unittest
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
import re
import time

try:
    import requests
    from requests.exceptions import ConnectionError
except ImportError as e:
    print('please install required modules:', e)
    raise


class GatewayBaseTest(unittest.TestCase):
    BUILDS = {
        'http://latestbuilds.hq.couchbase.com/couchbase-sync-gateway': (
            'release/1.1.0/{0}/couchbase-sync-gateway-community_{0}_{1}.{2}',
            'release/1.1.1/{0}/couchbase-sync-gateway-community_{0}_{1}.{2}',
            '1.1.0/{0}/couchbase-sync-gateway-community_{0}_{1}.{2}',
            '0.0.0/{0}/couchbase-sync-gateway-community_{0}_{1}.{2}',
            '0.0.1/{0}/couchbase-sync-gateway-community_{0}_{1}.{2}'
        ),
    }

    def setUp(self):
        super(GatewayBaseTest, self).setUp()
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.case_number = self.input.param("case_number", 0)
        self.version = self.input.param("version_sg", "0.0.0-358")
        self.extra_param = self.input.param("extra_param", "")
        if isinstance(self.extra_param, str):
            self.extra_param = self.extra_param.replace("$", "=")  # '=' is a delimiter in conf file
        self.logsdir = self.input.param("logsdir", "/tmp/sync_gateway/logs")
        self.datadir = self.input.param("datadir", "/tmp/sync_gateway")
        self.configdir = self.input.param("configdir", "/tmp/sync_gateway")
        self.configfile = self.input.param("configfile", "sync_gateway.json")
        self.expected_error = self.input.param("expected_error", "")
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.folder_prefix = ""
        self.installed_folder = '/opt/couchbase-sync-gateway/bin'
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        if type.lower() == 'windows':
            self.folder_prefix = "/cygdrive/c"
            self.installed_folder = '/cygdrive/c/Program\ Files\ \(x86\)/Couchbase'
            self.logsdir = self.folder_prefix + self.configdir
            self.datadir = self.folder_prefix + self.configdir
            self.configdir = self.folder_prefix + self.configdir

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
        self.log.fatal('Target build not found')

    def get_expected_locations(self, shell):
        self.info = shell.extract_remote_info()
        distribution_type = self.info.distribution_type.lower()
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos':
                file_ext = 'rpm'
            elif distribution_type == 'ubuntu':
                file_ext = 'deb'
            elif distribution_type == 'mac':
                file_ext = 'tar.gz'
        elif self.info.type.lower() == 'windows':
            file_ext = 'exe'

        for location, patterns in list(self.BUILDS.items()):
            for pattern in patterns:
                url = '{0}/{1}'.format(location, pattern.format(self.version, self.info.architecture_type, file_ext))
                filename = url.split('/')[-1]
                yield filename, url

    def install(self, shell):
        self.kill_processes_gateway(shell)
        self.uninstall_gateway(shell)
        self.install_gateway(shell)
        self.start_sync_gateways(shell)

    def kill_processes_gateway(self, shell):
        self.log.info('=== Killing Sync Gateway')
        shell.terminate_process(process_name='sync_gateway')
        shell.execute_command('killall sync_gateway')
        shell.execute_command('pkill sync_gateway')  # centos 7

    def uninstall_gateway(self, shell):
        self.info = shell.extract_remote_info()
        type = self.info.type.lower()
        distribution_type = self.info.distribution_type.lower()
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos':
                cmd = 'yes | yum remove couchbase-sync-gateway'
            elif distribution_type == 'ubuntu':
                cmd = 'dpkg -r couchbase-sync-gateway'
            elif distribution_type == 'mac':
                cmd = 'sudo launchctl unload /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist'
            else:
                self.log.info('uninstall_gateway is not supported on {0}, {1}'.format(type, distribution_type))
            self.log.info('=== Un-installing Sync Gateway package on {0} - cmd: {1}'.format(self.master, cmd))
            output, error = shell.execute_command(cmd)
            shell.log_command_output(output, error)
        elif self.info.type.lower() == 'windows':
            cmd = "wmic product where name='Couchbase Sync Gateway' uninstall"
            self.log.info('=== Un-installing Sync Gateway package on {0} - cmd: {1}'.format(self.master, cmd))
            output, error = shell.execute_batch_command(cmd)
            shell.log_command_output(output, error)

    def install_gateway(self, shell):
        filename, url = self.find_package(shell)
        type = shell.extract_remote_info().type.lower()
        distribution_type = self.info.distribution_type.lower()
        if type == 'linux':
            wget_str = 'wget'
            if distribution_type == 'centos':
                cmd = 'yes | rpm -i /tmp/{0}'.format(filename)
            elif distribution_type == 'ubuntu':
                cmd = 'yes | dpkg -i /tmp/{0}'.format(filename)
            elif distribution_type == 'mac':
                filename_tar = re.sub('\.gz$', '', filename)
                cmd = 'cd /tmp; gunzip {0}; tar xopf {1}; cp -r couchbase-sync-gateway /opt' \
                    .format(filename, filename_tar, filename_tar)
                wget_str = '/usr/local/bin/wget'
            else:
                self.log.info('install_gateway is not supported on {0}, {1}'.format(type, distribution_type))
            output, error = shell.execute_command_raw(
                'cd /tmp; {0} -q -O {1} {2};ls -lh'.format(wget_str, filename, url))

        elif type == 'windows':
            cmd = "cd /cygdrive/c/tmp;cmd /c 'couchbase-sync-gateway.exe /S /v/qn'"  # couchbase-sync-gateway.exe /v"/l*v C:\install.log /qb
            output, error = shell.execute_command(
                "cd /cygdrive/c/tmp;cmd /c 'c:\\automation\\wget.exe --no-check-certificate -q"
                " {0} -O {1}.exe';ls -lh;".format(url, "couchbase-sync-gateway"))
        shell.log_command_output(output, error)
        output, error = shell.execute_command_raw(cmd)
        shell.log_command_output(output, error)
        if type == 'windows':
            exist = shell.file_exists('/cygdrive/c/Program Files (x86)/Couchbase', 'sync_gateway.exe')
        else:
            exist = shell.file_exists(self.installed_folder.replace("\\", ""), 'sync_gateway')
        if exist:
            return True
        else:
            self.log.error('Installed file not found - {0}/sync_gateway'.format(self.installed_folder))
            return False

    def start_sync_gateways(self, shell):
        type = shell.extract_remote_info().type.lower()
        self.log.info('=== Starting Sync Gateway instances')
        shell.copy_files_local_to_remote('pytests/sg/resources', self.folder_prefix + '/tmp')
        if type == 'windows':
            output, error = shell.execute_command_raw('{0}/sync_gateway.exe'
                                                      ' c:/tmp/gateway_config.json > {1}/tmp/gateway.log 2>&1 &'.
                                                      format(self.installed_folder, self.folder_prefix))
        else:
            output, error = shell.execute_command_raw('nohup {0}/sync_gateway'
                                                      ' /tmp/gateway_config.json >/tmp/gateway.log 2>&1 &'.
                                                      format(self.installed_folder))
        shell.log_command_output(output, error)

    def start_simpleServe(self, shell):
        self.log.info('=== Starting SimpleServe instances')
        shell.copy_file_local_to_remote("pytests/sg/simpleServe.go",
                                        "{0}/tmp/simpleServe.go".format(self.folder_prefix))
        type = shell.extract_remote_info().type.lower()
        if type == 'windows':
            shell.terminate_process(process_name='simpleServe.exe')
            output, error = shell.execute_command_raw('c:/Go/bin/go.exe run c:/tmp/simpleServe.go 8081'
                                                      ' >{0}/tmp/simpleServe.txt 2>&1 &'.format(self.folder_prefix))
        else:
            shell.terminate_process(process_name='simpleServe')
            shell.execute_command("kill $(ps aux | grep '8081' | awk '{print $2}')")
            output, error = shell.execute_command_raw('go run {0}/tmp/simpleServe.go 8081'
                                                      '  >{0}/tmp/simpleServe.txt 2>&1 &'.format(self.folder_prefix))
        shell.log_command_output(output, error)

    def add_user(self, shell, user_name):
        self.log.info('=== Add user {0}'.format(user_name))
        self.info = shell.extract_remote_info()
        distribution_type = self.info.distribution_type.lower()
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos' or distribution_type == 'ubuntu':
                if not self.check_user(shell, user_name):
                    output, error = shell.execute_command_raw('useradd -m {0}'.format(user_name))
                    shell.log_command_output(output, error)
                return self.check_user(shell, user_name)
            else:
                self.log.info('add_user is not supported on Mac/Wind')
                return False
        else:
            type = self.info.type.lower()
            self.log.info('add_user is not supported on {0}, {1}'.format(type, distribution_type))
            return False

    def remove_user(self, shell, user_name):
        self.log.info('=== Remove user {0}'.format(user_name))
        self.info = shell.extract_remote_info()
        type = self.info.type.lower()
        distribution_type = self.info.distribution_type.lower()
        if type == 'linux':
            if distribution_type == 'centos' or distribution_type == 'ubuntu':
                if self.check_user(shell, user_name):
                    output, error = shell.execute_command_raw('userdel {0}'.format(user_name))
                    shell.log_command_output(output, error)
                return not self.check_user(shell, user_name)
            else:
                self.log.info('remove_user is only supported on centos and ubuntu')
                return False
        else:
            self.log.info('remove_user is not supported on {0}, {1}'.format(type, distribution_type))
            return False

    def check_user(self, shell, user_name):
        self.log.info('=== Check user {0} exists'.format(user_name))
        self.info = shell.extract_remote_info()
        distribution_type = self.info.distribution_type.lower()
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos' or distribution_type == 'ubuntu':
                output, error = shell.execute_command_raw('cat /etc/passwd | grep {0} '.format(user_name))
                shell.log_command_output(output, error)
                if output and output[0].startswith('{0}:'.format(user_name)):
                    self.log.info('User {0} exists'.format(user_name))
                    return True
                else:
                    self.log.info('User {0} does not exists'.format(user_name))
                    return False
            else:
                self.log.info('check_user is only supported on centos and ubuntu')
                return False
        else:
            type = self.info.type.lower()
            self.log.info('check_user is not supported on {0}, {1}', type, distribution_type)
            return False

    def is_sync_gateway_service_running(self, shell):
        self.info = shell.extract_remote_info()
        type = self.info.type.lower()
        distribution_type = self.info.distribution_type.lower()
        distribution_version = self.info.distribution_version
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos' and distribution_version == 'CentOS 7':
                cmd = 'systemctl | grep sync_gateway'
                service_str = 'sync_gateway.service                                                       ' \
                              '               loaded active running'
            elif distribution_type == 'ubuntu' or \
                    (distribution_type == 'centos' and distribution_version != 'CentOS 7'):
                cmd = 'initctl list | grep sync_gateway'
                service_str = 'sync_gateway start/running, process '
            elif distribution_type == 'mac':
                cmd = 'launchctl  list | grep sync_gateway'
                service_str = ''
            else:
                self.log.info('is_sync_gateway_service_running is not supported on {0}, {1}'
                              .format(type, distribution_type))
                return False
        else:
            self.log.info(
                'is_sync_gateway_service_running is not supported on {0}, {1}'.format(type, distribution_type))
            return False

        self.log.info('=== Check whether Sync Gateway service is running on {0} - cmd: {1}'.format(self.master, cmd))
        output, error = shell.execute_command_raw(cmd)
        shell.log_command_output(output, error)
        if output and output[0].startswith(service_str):
            self.log.info('Sync Gateway service is running')
            return True
        else:
            self.log.info('Sync Gateway service is NOT running')
            return False

    def start_gateway_service(self, shell):
        self.info = shell.extract_remote_info()
        type = self.info.type.lower()
        distribution_type = self.info.distribution_type.lower()
        distribution_version = self.info.distribution_version
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos':
                if distribution_version == 'CentOS 7':
                    cmd = 'systemctl start sync_gateway'
                else:
                    cmd = 'start sync_gateway'
            elif distribution_type == 'ubuntu':
                cmd = 'service sync_gateway start'
            elif distribution_type == 'mac':
                cmd = 'launchctl start com.couchbase.mobile.sync_gateway'
            else:
                self.log.info('start_gateway_service is not supported on {0}, {1}'.format(type, distribution_type))
                return False
        else:
            self.log.info('start_gateway_service is not supported on {0}, {1}'.format(type, distribution_type))
            return False

        self.log.info('=== Starting gateway service')
        output, error = shell.execute_command_raw(cmd)
        return output, error

    def stop_gateway_service(self, shell):
        self.info = shell.extract_remote_info()
        distribution_type = self.info.distribution_type.lower()
        distribution_version = self.info.distribution_version
        if self.info.type.lower() == 'linux':
            if distribution_type == 'centos':
                if (distribution_version == 'CentOS 7'):
                    cmd = 'systemctl stop sync_gateway'
                else:
                    cmd = 'stop sync_gateway'
            elif distribution_type == 'mac':
                cmd = 'launchctl stop com.couchbase.mobile.sync_gateway'
            else:
                cmd = 'service sync_gateway stop'

        self.log.info('=== Stopping gateway service')
        output, error = shell.execute_command_raw(cmd)
        return output, error

    def is_sync_gateway_process_running(self, shell):
        self.log.info('=== Check whether Sync Gateway process is running on {0}'.format(self.master))
        obj = RemoteMachineHelper(shell).is_process_running('sync_gateway')
        if obj and obj.pid:
            self.log.info('Sync Gateway is running with pid of {0}'.format(obj.pid))
            return True
        else:
            self.log.info('Sync Gateway is NOT running')
            return False

    def service_clean(self, shell):
        self.log.info('=== Cleaning Sync Gateway service and remove files')
        self.uninstall_gateway(shell)
        shell.execute_command_raw(
            'rm -rf /tmp/*.log /tmp/*.json /tmp/logs /tmp/data /tmp/sync_gateway/* '
            '/dirNotExist /opt/couchbase-sync-gateway* /tmp/couchbase-sync-gateway '
            '/tmp/couchbase-sync-gateway*tar')
        # return true only if the service is not running
        if self.is_sync_gateway_service_running(shell):
            self.log.error('Fail to clean Sync Gateway service.   The service is still running')
            return False
        else:
            return True

    def service_install(self, shell):
        self.install_gateway(shell)
        self.run_sync_gateway_service_install(shell)

    def run_sync_gateway_service_install(self, shell, options=''):
        self.info = shell.extract_remote_info()
        if self.servers[0].ssh_username == 'root':
            cmd = 'cd /opt/couchbase-sync-gateway/service; . ./sync_gateway_service_install.sh '
        else:
            cmd = 'cd /opt/couchbase-sync-gateway/service;  echo {0} | sudo . ./sync_gateway_service_install.sh ' \
                .format(self.servers[0].ssh_password)
        output, error = shell.execute_command(cmd + options)
        shell.log_command_output(output, error)
        return output, error

    def check_normal_error_output(self, shell, output, error):
        self.info = shell.extract_remote_info()
        type = self.info.type.lower()
        distribution_type = self.info.distribution_type.lower()
        distribution_version = self.info.distribution_version.lower()
        if type == 'linux':
            if distribution_type == 'centos' and distribution_version == 'centos 7':
                if error[0].startswith('ln -s ') and 'sync_gateway.service' in error[0]:
                    if output == []:
                        return True
                    else:
                        self.log.error('check_normal_error_output: On CentOS 7, expect output to be null, but got {0}'
                                       .format(output[0]))
                        return False
                else:
                    self.log.error('check_normal_error_output: On CentOS 7, expect error: '
                                   'ln -s \'/usr/lib/systemd/system/sync_gateway.service\'..., but got {0}'
                                   .format(error[0]))
                    return False
            elif distribution_type == 'ubuntu' or \
                    (distribution_type == 'centos' and distribution_version != 'centos 7'):
                if error == []:
                    if 'sync_gateway start/running' in output[0]:
                        return True
                    else:
                        self.log.error('check_normal_error_output: On linux, expect output to be: '
                                       '\'sync_gateway start/running\', but got {0}'.format(output[0]))
                        return False
                else:
                    self.log.error('check_normal_error_output: On linux, expect error to be null, but got {0}'
                                   .format(error[0]))
                    return False
            elif distribution_type == 'mac':
                if error == []:
                    expect_output = 'launchctl load /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist'
                    if output[0] == expect_output:
                        return True
                    else:
                        self.log.error('Expect output to be "{0}", but got {1}'.format(expect_output, output[0]))
                        return False
                else:
                    self.log.error('Expect error to be null, but got {0}'.format(error[0]))
                    return False
            else:
                self.log.info('check_normal_error_output is not supported on {0}, {1}'.format(type, distribution_type))
                return False
        else:
            self.log.info('check_normal_error_output is not supported on {0}, {1}'.format(type, distribution_type))
            return False

    def check_job_already_running(self, shell, output, error):
        self.info = shell.extract_remote_info()
        type = self.info.type.lower()
        distribution_type = self.info.distribution_type.lower()
        distribution_version = self.info.distribution_version.lower()
        if type == 'linux':
            if (distribution_type == 'centos') and (distribution_version == 'centos 7'):
                if error == []:
                    if output == []:
                        return True
                    else:
                        self.log.error('check_job_already_running: On CentOS 7, expect output to be null, but got {0}'
                                       .format(output[0]))
                        return False
                else:
                    self.log.error('check_job_already_running: On CentOS 7, expect error to be null, but got {0}'
                                   .format(error[0]))
                    return False
            elif distribution_type == 'ubuntu' or distribution_type == 'mac' or \
                    (distribution_type == 'centos' and distribution_version != 'centos 7'):
                if 'is already running' in error[0]:
                    if output == []:
                        return True
                    else:
                        self.log.error('check_job_already_running: On linux, expect output to be null, but got {0}'
                                       .format(output[0]))
                        return False
                else:
                    self.log.error(
                        'check_job_already_running: On linux, expect error to contain: \'is already running\' '
                        ' but get {0}'.format(error[0]))
                    return False
            else:
                self.log.info('check_job_already_running is not supported on {0}, {1}'.format(type, distribution_type))
                return False
        else:
            self.log.info('check_job_already_running is not supported on {0}, {1}'.format(type, distribution_type))
            return False

    def check_status_in_gateway_log(self, shell):
        logs = shell.read_remote_file('{0}/tmp/'.format(self.folder_prefix), 'gateway.log')[-5:]  # last 5 lines
        self.log.info(logs)
        status = re.search(".* got status (\w+)", logs[4])
        if not status:
            self.log.info('check_status_in_gateway_log failed, sync_gateway log has: {0}'.format(logs[4]))
            return ''
        else:
            return status.group(1)

    def check_message_in_gatewaylog(self, shell, expected_str):
        if not expected_str:
            return True
        for i in range(3):
            output, error = shell.execute_command_raw(
                'grep \'{0}\' {1}/tmp/gateway.log'.format(expected_str, self.folder_prefix))
            shell.log_command_output(output, error)
            if not output or not output[0]:
                if i < 2:
                    time.sleep(1)
                    continue
                else:
                    self.log.info('check_message_in_gatewaylog did not find expected error - {0}'.format(expected_str))
                    output, error = shell.execute_command_raw('cat {0}/tmp/gateway.log'.format(self.folder_prefix))
                    shell.log_command_output(output, error)
                    return False
            else:
                return True
