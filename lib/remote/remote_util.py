import os
import sys
import uuid
import paramiko
import time
import logging
from datetime import datetime
import logger
from builds.build_query import BuildQuery
import testconstants
from testconstants import WIN_REGISTER_ID
from testconstants import MEMBASE_VERSIONS
from testconstants import COUCHBASE_VERSIONS
from testconstants import MISSING_UBUNTU_LIB

from membase.api.rest_client import RestConnection, RestHelper


log = logger.Logger.get_logger()
logging.getLogger("paramiko").setLevel(logging.WARNING)


class RemoteMachineInfo(object):
    def __init__(self):
        self.type = ''
        self.ip = ''
        self.distribution_type = ''
        self.architecture_type = ''
        self.distribution_version = ''
        self.deliverable_type = ''
        self.ram = ''
        self.cpu = ''
        self.disk = ''
        self.hostname = ''


class RemoteMachineProcess(object):
    def __init__(self):
        self.pid = ''
        self.name = ''


class RemoteMachineHelper(object):
    remote_shell = None

    def __init__(self, remote_shell):
        self.remote_shell = remote_shell

    def monitor_process(self, process_name,
                        duration_in_seconds=120):
        # monitor this process and return if it crashes
        end_time = time.time() + float(duration_in_seconds)
        last_reported_pid = None
        while time.time() < end_time:
            # get the process list
            process = self.is_process_running(process_name)
            if process:
                if not last_reported_pid:
                    last_reported_pid = process.pid
                elif not last_reported_pid == process.pid:
                    message = 'process {0} was restarted. old pid : {1} new pid : {2}'
                    log.info(message.format(process_name, last_reported_pid, process.pid))
                    return False
                    # check if its equal
            else:
                # we should have an option to wait for the process
                # to start during the timeout
                # process might have crashed
                log.info("{0}:process {1} is not running or it might have crashed!".format(self.remote_shell.ip, process_name))
                return False
            time.sleep(1)
        #            log.info('process {0} is running'.format(process_name))
        return True

    def is_process_running(self, process_name):
        if getattr(self.remote_shell, "info", None) is None:
            self.remote_shell.info = self.remote_shell.extract_remote_info()

        if self.remote_shell.info.type.lower() == 'windows':
             output, error = self.remote_shell.execute_command('tasklist| grep {0}'.format(process_name), debug=False)
             if error or output == [""] or output == []:
                 return None
             words = output[0].split(" ")
             words = filter(lambda x: x != "", words)
             process = RemoteMachineProcess()
             process.pid = words[1]
             process.name = words[0]
             log.info("process is running on {0}: {1}".format(self.remote_shell.ip, words))
             return process
        else:
            processes = self.remote_shell.get_running_processes()
            for process in processes:
                if process.name == process_name:
                    return process
            return None


class RemoteMachineShellConnection:
    _ssh_client = None

    def __init__(self, username='root',
                 pkey_location='',
                 ip=''):
        self.username = username
        self.use_sudo = True
        if self.username == 'root':
           self.use_sudo = False
        # let's create a connection
        self._ssh_client = paramiko.SSHClient()
        self.ip = ip
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        log.info('connecting to {0} with username : {1} pem key : {2}'.format(ip, username, pkey_location))
        try:
            self._ssh_client.connect(hostname=ip, username=username, key_filename=pkey_location)
        except paramiko.AuthenticationException:
            log.info("Authentication failed for {0}".format(self.ip))
            exit(1)
        except paramiko.BadHostKeyException:
            log.info("Invalid Host key for {0}".format(self.ip))
            exit(1)
        except Exception:
            log.info("Can't establish SSH session with {0}".format(self.ip))
            exit(1)

    def __init__(self, serverInfo):
        # let's create a connection
        self.username = serverInfo.ssh_username
        self.use_sudo = True
        if self.username == 'root':
           self.use_sudo = False
        self._ssh_client = paramiko.SSHClient()
        self.ip = serverInfo.ip
        self.port = serverInfo.port
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        msg = 'connecting to {0} with username : {1} password : {2} ssh_key: {3}'
        log.info(msg.format(serverInfo.ip, serverInfo.ssh_username, serverInfo.ssh_password, serverInfo.ssh_key))
        # added attempts for connection because of PID check failed. RNG must be re-initialized after fork() error
        # That's a paramiko bug
        max_attempts_connect = 2
        attempt = 0
        while True:
            try:
                if serverInfo.ssh_key == '':
                    self._ssh_client.connect(hostname=serverInfo.ip,
                                             username=serverInfo.ssh_username,
                                             password=serverInfo.ssh_password)
                else:
                    self._ssh_client.connect(hostname=serverInfo.ip,
                                             username=serverInfo.ssh_username,
                                             key_filename=serverInfo.ssh_key)
                break
            except paramiko.AuthenticationException:
                log.error("Authentication failed")
                exit(1)
            except paramiko.BadHostKeyException:
                log.error("Invalid Host key")
                exit(1)
            except Exception as e:
                if str(e).find('PID check failed. RNG must be re-initialized') != -1 and\
                        attempt != max_attempts_connect:
                    log.error("Can't establish SSH session: {0}. Will try again in 1 sec"
                              .format(e))
                    attempt += 1
                    time.sleep(1)
                else:
                    log.error("Can't establish SSH session: {0}".format(e))
                    exit(1)
        log.info("Connected to {0}".format(serverInfo.ip))

    def sleep(self, timeout=1, message=""):
        log.info("{0}:sleep for {1} secs. {2} ...".format(self.ip, timeout, message))
        time.sleep(timeout)

    def get_running_processes(self):
        # if its linux ,then parse each line
        # 26989 ?        00:00:51 pdflush
        # ps -Ao pid,comm
        processes = []
        output, error = self.execute_command('ps -Ao pid,comm', debug=False)
        if output:
            for line in output:
                # split to words
                words = line.strip().split(' ')
                if len(words) >= 2:
                    process = RemoteMachineProcess()
                    process.pid = words[0]
                    process.name = words[1]
                    processes.append(process)
        return processes

    def get_mem_usage_by_process(self, process_name):
        """Now only linux"""
        output, error = self.execute_command('ps -e -o %mem,cmd|grep {0}'.format(process_name),
                                             debug=False)
        if output:
            for line in output:
                if not 'grep' in line.strip().split(' '):
                    return float(line.strip().split(' ')[0])

    def stop_membase(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()

        if self.info.type.lower() == 'windows':
            log.info("STOP SERVER")
            o, r = self.execute_command("net stop membaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            self.sleep(10, "Wait to stop service completely")
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/membase-server stop")
            self.log_command_output(o, r)

    def start_membase(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net start membaseserver")
            self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/membase-server start")
            self.log_command_output(o, r)

    def start_server(self, os="unix"):
        if os == "windows":
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        elif os == "unix":
            if self.is_couchbase_installed():
                o, r = self.execute_command("/etc/init.d/couchbase-server start")
            else:
                o, r = self.execute_command("/etc/init.d/membase-server start")
            self.log_command_output(o, r)
        elif os == "mac":
            o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
            self.log_command_output(o, r)
        else:
            self.log.error("don't know operating system or product version")

    def stop_server(self, os="unix"):
        if os == "windows":
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
        elif os == "unix":
            if self.is_couchbase_installed():
                o, r = self.execute_command("/etc/init.d/couchbase-server stop", use_channel=True)
            else:
                o, r = self.execute_command("/etc/init.d/membase-server stop")
            self.log_command_output(o, r)
        elif os == "mac":
            o, r = self.execute_command("ps aux | grep Couchbase | awk '{print $2}' | xargs kill -9")
            self.log_command_output(o, r)
            o, r = self.execute_command("killall -9 epmd")
            self.log_command_output(o, r)
        else:
            self.log.error("don't know operating system or product version")

    def stop_schedule_tasks(self):
        log.info("STOP ALL SCHEDULE TASKS: installme, removeme and upgrademe")
        output, error = self.execute_command("cmd /c schtasks /end /tn installme")
        self.log_command_output(output, error)
        output, error = self.execute_command("cmd /c schtasks /end /tn removeme")
        self.log_command_output(output, error)
        output, error = self.execute_command("cmd /c schtasks /end /tn upgrademe")
        self.log_command_output(output, error)

    def kill_erlang(self, os="unix"):
        if os == "windows":
            o, r = self.execute_command("/taskkill /F /T /IM erl.exe*")
            self.log_command_output(o, r)
            self.disconnect()
        else:
            o, r = self.execute_command("killall -9 beam.smp")
            self.log_command_output(o, r)
            self.disconnect()

    def change_log_level(self, new_log_level):
        log.info("CHANGE LOG LEVEL TO %s".format(new_log_level))
        # ADD NON_ROOT user config_details
        output, error = self.execute_command("sed -i '/loglevel_default, /c \\{loglevel_default, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_ns_server, /c \\{loglevel_ns_server, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_stats, /c \\{loglevel_stats, %s\}'. %s"
                                             % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_rebalance, /c \\{loglevel_rebalance, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_cluster, /c \\{loglevel_cluster, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_views, /c \\{loglevel_views, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_error_logger, /c \\{loglevel_error_logger, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_mapreduce_errors, /c \\{loglevel_mapreduce_errors, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_user, /c \\{loglevel_user, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_xdcr, /c \\{loglevel_xdcr, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/loglevel_menelaus, /c \\{loglevel_menelaus, %s\}'. %s"
                                            % (new_log_level, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def configure_log_location(self, new_log_location):
        mv_logs = testconstants.LINUX_LOG_PATH + '/' + new_log_location
        print " MV LOGS %s" % mv_logs
        error_log_tag = "error_logger_mf_dir"
        # ADD NON_ROOT user config_details
        log.info("CHANGE LOG LOCATION TO %s".format(mv_logs))
        output, error = self.execute_command("rm -rf %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("mkdir %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("chown -R couchbase %s" % mv_logs)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/%s, /c \\{%s, \"%s\"\}.' %s"
                                        % (error_log_tag, error_log_tag, mv_logs, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def change_stat_periodicity(self, ticks):
        # ADD NON_ROOT user config_details
        log.info("CHANGE STAT PERIODICITY TO every %s seconds" % ticks)
        output, error = self.execute_command("sed -i '$ a\{grab_stats_every_n_ticks, %s}.'  %s"
        % (ticks, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)

    def change_port_static(self, new_port):
        # ADD NON_ROOT user config_details
        log.info("=========CHANGE PORTS for REST: %s, MCCOUCH: %s,MEMCACHED: %s, MOXI: %s, CAPI: %s==============="
                % (new_port, new_port + 1, new_port + 2, new_port + 3, new_port + 4))
        output, error = self.execute_command("sed -i '/{rest_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{rest_port, %s}.' %s"
                                             % (new_port, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{mccouch_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{mccouch_port, %s}.' %s"
                                             % (new_port + 1, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{memcached_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{memcached_port, %s}.' %s"
                                             % (new_port + 2, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/{moxi_port/d' %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '$ a\{moxi_port, %s}.' %s"
                                             % (new_port + 3, testconstants.LINUX_STATIC_CONFIG))
        self.log_command_output(output, error)
        output, error = self.execute_command("sed -i '/port = /c\port = %s' %s"
                                             % (new_port + 4, testconstants.LINUX_CAPI_INI))
        self.log_command_output(output, error)
        output, error = self.execute_command("rm %s" % testconstants.LINUX_CONFIG_FILE)
        self.log_command_output(output, error)
        output, error = self.execute_command("cat %s" % testconstants.LINUX_STATIC_CONFIG)
        self.log_command_output(output, error)

    def is_couchbase_installed(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            if self.file_exists(testconstants.WIN_CB_PATH, testconstants.VERSION_FILE):
                #print running process on windows
                RemoteMachineHelper(self).is_process_running('memcached')
                RemoteMachineHelper(self).is_process_running('erl')
                return True
        elif self.info.distribution_type.lower() == 'mac':
            output, error = self.execute_command('ls %s%s' % (testconstants.MAC_CB_PATH, testconstants.VERSION_FILE))
            self.log_command_output(output, error)
            for line in output:
                if line.find('No such file or directory') == -1:
                    return True
        elif self.info.type.lower() == "linux":
            if self.file_exists(testconstants.LINUX_CB_PATH, testconstants.VERSION_FILE):
                return True
        return False

    def is_moxi_installed(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.log.error('Not implemented')
        elif self.info.distribution_type.lower() == 'mac':
            self.log.error('Not implemented')
        elif self.info.type.lower() == "linux":
            if self.file_exists(testconstants.LINUX_MOXI_PATH, 'moxi'):
                return True
        return False

    # /opt/moxi/bin/moxi -Z port_listen=11211 -u root -t 4 -O /var/log/moxi/moxi.log
    def start_moxi(self, ip, bucket, port, user=None, threads=4, log_file="/var/log/moxi.log"):
        if self.is_couchbase_installed():
            prod = "couchbase"
        else:
            prod = "membase"
        cli_path = "/opt/" + prod + "/bin/moxi"
        args = ""
        args += "http://{0}:8091/pools/default/bucketsStreaming/{1} ".format(ip, bucket)
        args += "-Z port_listen={0} -u {1} -t {2} -O {3} -d".format(port,
                                                                    user or prod,
                                                                    threads, log_file)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("{0} {1}".format(cli_path, args))
            self.log_command_output(o, r)
        else:
            raise Exception("running standalone moxi is not supported for windows")

    def stop_moxi(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("killall -9 moxi")
            self.log_command_output(o, r)
        else:
            raise Exception("stopping standalone moxi is not supported on windows")

    def download_build(self, build):
        return self.download_binary(build.url, build.deliverable_type, build.name, latest_url=build.url_latest_build)

    def disable_firewall(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == "windows":
            output, error = self.execute_command('netsh advfirewall set publicprofile state off')
            self.log_command_output(output, error)
            output, error = self.execute_command('netsh advfirewall set privateprofile state off')
            self.log_command_output(output, error)
            # for details see RemoteUtilHelper.enable_firewall for windows
            output, error = self.execute_command('netsh advfirewall firewall delete rule name="block erl.exe in"')
            self.log_command_output(output, error)
            output, error = self.execute_command('netsh advfirewall firewall delete rule name="block erl.exe out"')
            self.log_command_output(output, error)
        else:
            output, error = self.execute_command('/sbin/iptables -F')
            self.log_command_output(output, error)
            output, error = self.execute_command('/sbin/iptables -t nat -F')
            self.log_command_output(output, error)

    def download_binary(self, url, deliverable_type, filename, latest_url=None, skip_md5_check=True):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        self.disable_firewall()
        if self.info.type.lower() == 'windows':
            self.execute_command('taskkill /F /T /IM msiexec32.exe')
            self.execute_command('taskkill /F /T /IM msiexec.exe')
            self.execute_command('taskkill /F /T IM setup.exe')
            self.execute_command('taskkill /F /T /IM ISBEW64.*')
            self.execute_command('taskkill /F /T /IM firefox.*')
            self.execute_command('taskkill /F /T /IM iexplore.*')
            self.execute_command('taskkill /F /T /IM WerFault.*')
            self.execute_command('taskkill /F /T /IM memcached.exe')
            output, error = self.execute_command("rm -rf /cygdrive/c/automation/setup.exe")
            self.log_command_output(output, error)
            output, error = self.execute_command(
                 "cd /cygdrive/c/tmp;cmd /c 'c:\\automation\\wget.exe --no-check-certificate -q {0} -O setup.exe';ls -l;".format(url))
            self.log_command_output(output, error)
            return self.file_exists('/cygdrive/c/tmp/', 'setup.exe')
        elif self.info.distribution_type.lower() == 'mac':
            output, error = self.execute_command('cd ~/Downloads ; rm -rf couchbase-server* ; rm -rf Couchbase\ Server.app ; curl -O {0}'.format(url))
            self.log_command_output(output, error)
            output, error = self.execute_command('ls ~/Downloads/%s' % filename)
            self.log_command_output(output, error)
            for line in output:
                if line.find('No such file or directory') == -1:
                    return True
            return False
        else:
        # try to push this build into
        # depending on the os
        # build.product has the full name
        # first remove the previous file if it exist ?
        # fix this :
            output, error = self.execute_command_raw('cd /tmp ; D=$(mktemp -d cb_XXXX) ; mv {0} $D ; mv core.* $D ; rm -f * ; mv $D/* . ; rmdir $D'.format(filename))
            self.log_command_output(output, error)
            if skip_md5_check:
                output, error = self.execute_command_raw('cd /tmp;wget -q -O {0} {1};cd /tmp'.format(filename, url))
                self.log_command_output(output, error)
            else:
                log.info('get md5 sum for local and remote')
                output, error = self.execute_command_raw('cd /tmp ; rm -f *.md5 *.md5l ; wget -q -O {1}.md5 {0}.md5 ; md5sum {1} > {1}.md5l'.format(url, filename))
                self.log_command_output(output, error)
                if str(error).find('No such file or directory') != -1 and latest_url != '':
                    url = latest_url
                    output, error = self.execute_command_raw('cd /tmp ; rm -f *.md5 *.md5l ; wget -q -O {1}.md5 {0}.md5 ; md5sum {1} > {1}.md5l'.format(url, filename))
                log.info('comparing md5 sum and downloading if needed')
                output, error = self.execute_command_raw('cd /tmp;diff {0}.md5 {0}.md5l || wget -q -O {0} {1};rm -f *.md5 *.md5l'.format(filename, url))
                self.log_command_output(output, error)
            # check if the file exists there now ?
            return self.file_exists('/tmp', filename)
            # for linux environment we can just
            # figure out what version , check if /tmp/ has the
            # binary and then return True if binary is installed


    def get_file(self, remotepath, filename, todir):
        if self.file_exists(remotepath, filename):
            sftp = self._ssh_client.open_sftp()
            try:
                filenames = sftp.listdir(remotepath)
                for name in filenames:
                    if name == filename:
                        sftp.get('{0}/{1}'.format(remotepath, filename), todir)
                        sftp.close()
                        return True
                sftp.close()
                return False
            except IOError:
                return False

    def remove_directory(self, remote_path):
        sftp = self._ssh_client.open_sftp()
        try:
            log.info("removing {0} directory...".format(remote_path))
            sftp.rmdir(remote_path)
        except IOError:
            return False
        finally:
            sftp.close()
        return True

    def list_files(self, remote_path):
        sftp = self._ssh_client.open_sftp()
        files = []
        try:
            file_names = sftp.listdir(remote_path)
            for name in file_names:
                files.append({'path': remote_path, 'file': name})
            sftp.close()
        except IOError:
            return []
        return files

    # check if this file exists in the remote
    # machine or not
    def file_starts_with(self, remotepath, pattern):
        sftp = self._ssh_client.open_sftp()
        files_matched = []
        try:
            file_names = sftp.listdir(remotepath)
            for name in file_names:
                if name.startswith(pattern):
                    files_matched.append("{0}/{1}".format(remotepath, name))
        except IOError:
            # ignore this error
            pass
        sftp.close()
        if len(files_matched) > 0:
            log.info("found these files : {0}".format(files_matched))
        return files_matched

    def file_exists(self, remotepath, filename):
        sftp = self._ssh_client.open_sftp()
        try:
            filenames = sftp.listdir_attr(remotepath)
            for name in filenames:
                if name.filename == filename and int(name.st_size) > 0:
                    sftp.close()
                    return True
            sftp.close()
            return False
        except IOError:
            return False

    def download_binary_in_win(self, url, version):
        self.execute_command('taskkill /F /T /IM msiexec32.exe')
        self.execute_command('taskkill /F /T /IM msiexec.exe')
        self.execute_command('taskkill /F /T IM setup.exe')
        self.execute_command('taskkill /F /T /IM ISBEW64.*')
        self.execute_command('taskkill /F /T /IM iexplore.*')
        self.execute_command('taskkill /F /T /IM WerFault.*')
        self.execute_command('taskkill /F /T /IM Firefox.*')
        self.disable_firewall()
        version = version.replace("-rel", "")
        exist = self.file_exists('/cygdrive/c/tmp/', '{0}.exe'.format(version))
        if not exist:
            output, error = self.execute_command(
                 "cd /cygdrive/c/tmp;cmd /c 'c:\\automation\\wget.exe --no-check-certificate -q \
                                                     {0} -O {1}.exe';ls -l;".format(url, version))
            self.log_command_output(output, error)
        else:
            log.info('File {0}.exe exist in tmp directory'.format(version))
        return self.file_exists('/cygdrive/c/tmp/', '{0}.exe'.format(version))

    def copy_file_local_to_remote(self, src_path, des_path):
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.put(src_path, des_path)
        except IOError:
            log.error('Can not copy file')
        finally:
            sftp.close()

    def copy_file_remote_to_local(self, rem_path, des_path):
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.get(rem_path, des_path)
        except IOError:
            log.error('Can not copy file')
        finally:
            sftp.close()


    # copy multi files from local to remote server
    def copy_files_local_to_remote(self, src_path, des_path):
        files = os.listdir(src_path)
        log.info("copy file from {0} to {1}".format(src_path, des_path))
        # self.execute_batch_command("cp -r  {0}/* {1}".format(src_path, des_path))
        for file in files:
            if file.find("wget") != 1:
                a = ""
            full_src_path = os.path.join(src_path, file)
            full_des_path = os.path.join(des_path, file)
            self.copy_file_local_to_remote(full_src_path, full_des_path)


    # create a remote file from input string
    def create_file(self, remote_path, file_data):
        output, error = self.execute_command("echo '{0}' > {1}".format(file_data, remote_path))

    def find_file(self, remote_path, file):
        sftp = self._ssh_client.open_sftp()
        try:
            files = sftp.listdir(remote_path)
            for name in files:
                if name == file:
                    found_it = os.path.join(remote_path, name)
                    log.info("File {0} was found".format(found_it))
                    return found_it
            else:
                log.error('File(s) name in {0}'.format(remote_path))
                for name in files:
                    log.info(name)
                log.error('Can not find {0}'.format(file))
        except IOError:
            pass
        sftp.close()

    def find_build_version(self, path_to_version, version_file, product):
        sftp = self._ssh_client.open_sftp()
        ex_type = "exe"
        version_file = testconstants.VERSION_FILE
        if self.file_exists(testconstants.WIN_CB_PATH, testconstants.VERSION_FILE):
            path_to_version = testconstants.WIN_CB_PATH
        else:
            path_to_version = testconstants.WIN_MB_PATH
        try:
            log.info(path_to_version)
            f = sftp.open(os.path.join(path_to_version, version_file), 'r+')
            tmp_str = f.read().strip()
            full_version = tmp_str.replace("-rel", "")
            if full_version == "1.6.5.4-win64":
                full_version = "1.6.5.4"
            build_name = short_version = full_version
            print build_name, short_version, full_version
            return build_name, short_version, full_version
        except IOError:
            log.error('Can not read version file')
        sftp.close()

    def find_windows_info(self):
        found = self.find_file("/cygdrive/c/tmp", "windows_info.txt")
        if isinstance(found, str):
            sftp = self._ssh_client.open_sftp()
            try:
                f = sftp.open(found)
                log.info("get windows information")
                info = {}
                for line in f:
                    (key, value) = line.split('=')
                    key = key.strip(' \t\n\r')
                    value = value.strip(' \t\n\r')
                    info[key] = value
                return info
            except IOError:
                log.error("can not find windows info file")
            sftp.close()
        else:
            return self.create_windows_info()

    def create_windows_info(self):
            systeminfo = self.get_windows_system_info()
            info = {}
            if "OS Name" in systeminfo:
                info["os"] = systeminfo["OS Name"].find("indows") and "windows" or "NONE"
                info["os_name"] = systeminfo["OS Name"].find("2008 R2") and "2k8" or "NONE"
            if "System Type" in systeminfo:
                info["os_arch"] = systeminfo["System Type"].find("64") and "x86_64" or "NONE"
            info.update(systeminfo)
            self.execute_batch_command("rm -rf  /cygdrive/c/tmp/windows_info.txt")
            self.execute_batch_command("touch  /cygdrive/c/tmp/windows_info.txt")
            sftp = self._ssh_client.open_sftp()
            try:
                f = sftp.open('/cygdrive/c/tmp/windows_info.txt', 'w')
                content = ''
                for key in sorted(info.keys()):
                    content += '{0} = {1}\n'.format(key, info[key])
                f.write(content)
                log.info("/cygdrive/c/tmp/windows_info.txt was created with content: {0}".format(content))
            except IOError:
                log.error('Can not write windows_info.txt file')
            finally:
                sftp.close()
            return info

    # Need to add new windows register ID in testconstant file when
    # new couchbase server version comes out.
    def create_windows_capture_file(self, task, product, version):
        src_path = "resources/windows/automation"
        des_path = "/cygdrive/c/automation"

        # remove dot in version (like 2.0.0 ==> 200)
        reg_version = version[0:5:2]
        reg_id = WIN_REGISTER_ID[reg_version]

        if task == "install":
            template_file = "cb-install.wct"
            file = "install.iss"
        elif task == "uninstall":
            template_file = "cb-uninstall.wct"
            file = "uninstall.iss"

        # create in/uninstall file from windows capture template (wct) file
        full_src_path_template = os.path.join(src_path, template_file)
        full_src_path = os.path.join(src_path, file)
        full_des_path = os.path.join(des_path, file)

        f1 = open(full_src_path_template, 'r')
        f2 = open(full_src_path, 'w')
        # replace ####### with reg ID to install/uninstall
        if "2.2.0-837" in version:
            reg_id = "2B630EB8-BBC7-6FE4-C9B8-D8843EB1EFFA"
        log.info("register ID: {0}".format(reg_id))
        for line in f1:
            line = line.replace("#######", reg_id)
            if product == "mb" and task == "install":
                line = line.replace("Couchbase", "Membase")
            f2.write(line)
        f1.close()
        f2.close()

        self.copy_file_local_to_remote(full_src_path, full_des_path)
        # remove capture file from source after copy to destination
        # os.remove(full_src_path)

    def get_windows_system_info(self):
        try:
            info = {}
            o = self.execute_batch_command('systeminfo')
            for line in o:
                line_list = line.split(':')
                if len(line_list) > 2:
                    if line_list[0] == 'Virtual Memory':
                        key = "".join(line_list[0:2])
                        value = " ".join(line_list[2:])
                    else:
                        key = line_list[0]
                        value = " ".join(line_list[1:])
                elif len(line_list) == 2:
                    (key, value) = line_list
                else:
                    continue
                key = key.strip(' \t\n\r')
                if key.find("[") != -1:
                    info[key_prev] += '|' + key + value.strip(' |')
                else:
                    value = value.strip(' |')
                    info[key] = value
                    key_prev = key
            return info
        except Exception as ex:
            log.error("error {0} appeared during getting  windows info".format(ex))

    # this function used to modify bat file to run task schedule in windows
    def modify_bat_file(self, remote_path, file_name, name, version, task):
        found = self.find_file(remote_path, file_name)
        sftp = self._ssh_client.open_sftp()

        product_version = ""
        if version[:5] in MEMBASE_VERSIONS:
            product_version = version[:5]
            name = "mb"
        elif version[:5] in COUCHBASE_VERSIONS:
            product_version = version[:5]
            name = "cb"
        else:
            log.error('Windows automation does not support {0} version yet'.format(version))
            sys.exit()

        try:
            f = sftp.open(found, 'w')
            name = name.strip()
            version = version.strip()
            if task == "upgrade":
                content = 'c:\\tmp\setup.exe /s -f1c:\\automation\{0}_{1}_{2}.iss'.format(name,
                                                                         product_version, task)
            else:
                content = 'c:\\tmp\{0}.exe /s -f1c:\\automation\{1}.iss'.format(version, task)
            log.info("create {0} task with content:{1}".format(task, content))
            f.write(content)
            log.info('Successful write to {0}'.format(found))
        except IOError:
            log.error('Can not write build name file to bat file {0}'.format(found))
        sftp.close()

    def create_directory(self, remote_path):
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.stat(remote_path)
        except IOError, e:
            if e[0] == 2:
                log.info("Directory at {0} DOES NOT exist. We will create on here".format(remote_path))
                sftp.mkdir(remote_path)
                sftp.close()
                return False
            raise
        else:
            log.error("Directory at {0} DOES exist. Fx returns True".format(remote_path))
            return True

    # this function will remove the automation directory in windows
    def create_multiple_dir(self, dir_paths):
        sftp = self._ssh_client.open_sftp()
        try:
            for dir_path in dir_paths:
                if dir_path != '/cygdrive/c/tmp':
                    output = self.remove_directory('/cygdrive/c/automation')
                    if output:
                        log.info("{0} directory is removed.".format(dir_path))
                    else:
                        log.error("Can not delete {0} directory or directory {0} does not exist.".format(dir_path))
                self.create_directory(dir_path)
            sftp.close()
        except IOError:
            pass

    def membase_upgrade(self, build, save_upgrade_config=False, forcefully=False):
        # upgrade couchbase server
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        log.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))
        command = ''
        if self.info.type.lower() == 'windows':
                self.membase_upgrade_win(self.info.architecture_type, self.info.windows_name, build.name)
                log.info('********* continue upgrade process **********')

        elif self.info.deliverable_type == 'rpm':
            # run rpm -i to install
            if save_upgrade_config:
                self.membase_uninstall(save_upgrade_config=save_upgrade_config)
                install_command = 'rpm -i /tmp/{0}'.format(build.name)
                command = 'INSTALL_UPGRADE_CONFIG_DIR=/opt/membase/var/lib/membase/config {0}'.format(install_command)
            else:
                command = 'rpm -U /tmp/{0}'.format(build.name)
                if forcefully:
                    command = 'rpm -U --force /tmp/{0}'.format(build.name)
        elif self.info.deliverable_type == 'deb':
            if save_upgrade_config:
                self.membase_uninstall(save_upgrade_config=save_upgrade_config)
                install_command = 'dpkg -i /tmp/{0}'.format(build.name)
                command = 'INSTALL_UPGRADE_CONFIG_DIR=/opt/membase/var/lib/membase/config {0}'.format(install_command)
            else:
                command = 'dpkg -i /tmp/{0}'.format(build.name)
                if forcefully:
                    command = 'dpkg -i --force /tmp/{0}'.format(build.name)
        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def membase_upgrade_win(self, architecture, windows_name, version):
        task = "upgrade"
        bat_file = "upgrade.bat"
        version_file = "VERSION.txt"
        deleted = False
        self.modify_bat_file('/cygdrive/c/automation', bat_file, 'cb', version, task)
        self.stop_schedule_tasks()
        output, error = self.execute_command("cat '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'")
        log.info("version to upgrade: {0}".format(output))
        self.info('before running task schedule upgrademe')
        if '1.8.0' in str(output):
            # run installer in second time as workaround for upgrade 1.8.0 only:
            # Installer needs to update registry value in order to upgrade from the previous version.
            # Please run installer again to continue."""
            output, error = self.execute_command("cmd /c schtasks /run /tn upgrademe")
            self.log_command_output(output, error)
            self.sleep(200, "because upgrade version is {0}".format(output))
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN upgrademe /V")
            self.log_command_output(output, error)
            self.stop_schedule_tasks()
        # run task schedule to upgrade Membase server
        output, error = self.execute_command("cmd /c schtasks /run /tn upgrademe")
        self.log_command_output(output, error)
        deleted = self.wait_till_file_deleted(testconstants.WIN_CB_PATH, version_file, timeout_in_seconds=600)
        if not deleted:
            log.error("Uninstall was failed at node {0}".format(self.ip))
            sys.exit()
        self.wait_till_file_added(testconstants.WIN_CB_PATH, version_file, timeout_in_seconds=600)
        log.info("installed version:")
        output, error = self.execute_command("cat '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'")
        time.sleep(60, "wait for server to start up completely")
        ct = time.time()
        while time.time() - ct < 10800:
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN upgrademe /V| findstr Status ")
            if "Ready" in str(output):
                log.info("upgrademe task complteted")
                break
            elif "Could not start":
                log.exception("Ugrade failed!!!")
                break
            else:
                log.info("upgrademe task still running:{0}".format(output))
                self.sleep(30)
        output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN upgrademe /V")
        self.log_command_output(output, error)

    def install_server(self, build, startserver=True, path='/tmp', vbuckets=None,
                       swappiness=10, force=False, openssl='', upr=False, xdcr_upr=False):
        server_type = None
        success = True
        track_words = ("warning", "error", "fail")
        if build.name.lower().find("membase") != -1:
            server_type = 'membase'
            abbr_product = "mb"
        elif build.name.lower().find("couchbase") != -1:
            server_type = 'couchbase'
            abbr_product = "cb"
        else:
            raise Exception("its not a membase or couchbase?")
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        if self.info.type.lower() == 'windows':
            win_processes = ["msiexec32.exe", "msiexec32.exe", "setup.exe", "ISBEW64.*",
                             "firefox.*", "WerFault.*", "iexplore.*"]
            self.terminate_processes(self.info, win_processes)
            # to prevent getting full disk let's delete some large files
            self.remove_win_backup_dir()
            self.remove_win_collect_tmp()
            output, error = self.execute_command("cmd /c schtasks /run /tn installme")
            success &= self.log_command_output(output, error, track_words)
            self.wait_till_file_added("/cygdrive/c/Program Files/{0}/Server/".format(server_type.title()), 'VERSION.txt',
                                          timeout_in_seconds=600)
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN installme /V")
            self.log_command_output(output, error)
            # output, error = self.execute_command("cmd rm /cygdrive/c/tmp/{0}*.exe".format(build_name))
            # self.log_command_output(output, error)
        elif self.info.deliverable_type in ["rpm", "deb"]:
            if startserver and vbuckets == None:
                environment = ""
            else:
                environment = "INSTALL_DONT_START_SERVER=1 "
            log.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))

            # set default swappiness to 10 unless specify in params in all unix environment
            output, error = self.execute_command('/sbin/sysctl vm.swappiness={0}'.format(swappiness))
            success &= self.log_command_output(output, error, track_words)

            if self.info.deliverable_type == 'rpm':
                self.check_openssl_version(self.info.deliverable_type, openssl)
                self.check_pkgconfig(self.info.deliverable_type, openssl)
                if force:
                    output, error = self.execute_command('{0}rpm -Uvh --force /tmp/{1}'.format(environment, build.name))
                else:
                    output, error = self.execute_command('{0}rpm -i /tmp/{1}'.format(environment, build.name))
            elif self.info.deliverable_type == 'deb':
                self.check_openssl_version(self.info.deliverable_type, openssl)
                self.install_missing_lib()
                if force:
                    output, error = self.execute_command('{0}dpkg --force-all -i /tmp/{1}'.format(environment, build.name))
                else:
                    output, error = self.execute_command('{0}dpkg -i /tmp/{1}'.format(environment, build.name))
            success &= self.log_command_output(output, error, track_words)
            self.create_directory(path)
            output, error = self.execute_command('/opt/{0}/bin/{1}enable_core_dumps.sh  {2}'.
                                    format(server_type, abbr_product, path))
            success &= self.log_command_output(output, error, track_words)

            if vbuckets:
                self.execute_command("sed -i 's/ulimit -c unlimited/ulimit -c unlimited\\n    export {0}_NUM_VBUCKETS={1}/' /opt/{2}/etc/{2}_init.d".
                                                    format(server_type.upper(), vbuckets, server_type))
                success &= self.log_command_output(output, error, track_words)
            if upr:
                output, error = \
                    self.execute_command("sed -i 's/END INIT INFO/END INIT INFO\\nexport COUCHBASE_REPL_TYPE=upr/'\
                    /opt/{0}/etc/{0}_init.d".format(server_type))
                success &= self.log_command_output(output, error, track_words)
            if xdcr_upr:
                output, error = \
                    self.execute_command("sed -i 's/END INIT INFO/END INIT INFO\\nexport XDCR_USE_NEW_PATH=upr/'\
                    /opt/{0}/etc/{0}_init.d".format(server_type))
                success &= self.log_command_output(output, error, track_words)

            # skip output: [WARNING] couchbase-server is already started
            track_words = ("error", "fail")
            if startserver:
                output, error = self.execute_command('/etc/init.d/{0}-server start'.format(server_type))

                if (build.product_version.startswith("1.") or build.product_version.startswith("2.0.0"))\
                        and build.deliverable_type == "deb":
                    # skip error '* Failed to start couchbase-server' for 1.* & 2.0.0 builds(MB-7288)
                    # fix in 2.0.1 branch Change-Id: I850ad9424e295bbbb79ede701495b018b5dfbd51
                    log.warn("Error '* Failed to start couchbase-server' for 1.* builds will be skipped")
                    self.log_command_output(output, error, track_words)
                else:
                    success &= self.log_command_output(output, error, track_words)
        elif self.info.deliverable_type in ["zip"]:
            o, r = self.execute_command("ps aux | grep Archive | awk '{print $2}' | xargs kill -9")
            self.log_command_output(o, r)
            self.sleep(60)
            output, error = self.execute_command("cd ~/Downloads ; open couchbase-server*.zip")
            self.log_command_output(output, error)
            self.sleep(60)
            output, error = self.execute_command("mv ~/Downloads/couchbase-server*/Couchbase\ Server.app /Applications/")
            self.log_command_output(output, error)
            output, error = self.execute_command("open /Applications/Couchbase\ Server.app")
            self.log_command_output(output, error)

        output, error = self.execute_command("rm -f *-diag.zip")
        self.log_command_output(output, error, track_words)
        return success

    def install_server_win(self, build, version, startserver=True):
        remote_path = None
        success = True
        track_words = ("warning", "error", "fail")
        if build.name.lower().find("membase") != -1:
            remote_path = testconstants.WIN_MB_PATH
            abbr_product = "mb"
        elif build.name.lower().find("couchbase") != -1:
            remote_path = testconstants.WIN_CB_PATH
            abbr_product = "cb"

        if remote_path is None:
            raise Exception("its not a membase or couchbase?")
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        if self.info.type.lower() == 'windows':
            task = "install"
            bat_file = "install.bat"
            dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
            # build = self.build_url(params)
            self.create_multiple_dir(dir_paths)
            self.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
            self.create_windows_capture_file(task, abbr_product, version)
            self.modify_bat_file('/cygdrive/c/automation', bat_file, abbr_product, version, task)
            self.stop_schedule_tasks()
            self.remove_win_backup_dir()
            self.remove_win_collect_tmp()
            log.info('sleep for 5 seconds before running task schedule install me')
            time.sleep(5)
            # run task schedule to install Membase server
            output, error = self.execute_command("cmd /c schtasks /run /tn installme")
            success &= self.log_command_output(output, error, track_words)
            self.wait_till_file_added(remote_path, "VERSION.txt", timeout_in_seconds=600)
            self.sleep(30, "wait for server to start up completely")
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN installme /V")
            self.log_command_output(output, error)
            output, error = self.execute_command("rm -f *-diag.zip")
            self.log_command_output(output, error, track_words)
            return success


    def install_moxi(self, build):
        success = True
        track_words = ("warning", "error", "fail")
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(self.info.deliverable_type))
        if self.info.type.lower() == 'windows':
            self.log.error('Not implemented')
        elif self.info.deliverable_type in ["rpm"]:
            output, error = self.execute_command('rpm -i /tmp/{0}'.format(build.name))
            if error and ' '.join(error).find("ERROR") != -1:
                success = False
        elif self.info.deliverable_type == 'deb':
            output, error = self.execute_command('dpkg -i /tmp/{0}'.format(build.name))
            if error and ' '.join(error).find("ERROR") != -1:
                success = False
        success &= self.log_command_output(output, '', track_words)
        return success

    def wait_till_file_deleted(self, remotepath, filename, timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        deleted = False
        while time.time() < end_time and not deleted:
            # get the process list
            exists = self.file_exists(remotepath, filename)
            if exists:
                log.error('at {2} file still exists : {0}{1}'.format(remotepath, filename, self.ip))
                time.sleep(5)
            else:
                log.info('at {2} FILE DOES NOT EXIST ANYMORE : {0}{1}'.format(remotepath, filename, self.ip))
                deleted = True
        return deleted

    def wait_till_file_added(self, remotepath, filename, timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        added = False
        while time.time() < end_time and not added:
            # get the process list
            exists = self.file_exists(remotepath, filename)
            if not exists:
                log.error('at {2} file does not exist : {0}{1}'.format(remotepath, filename, self.ip))
                time.sleep(5)
            else:
                log.info('at {2} FILE EXISTS : {0}{1}'.format(remotepath, filename, self.ip))
                added = True
        return added

    def wait_till_compaction_end(self, rest, bucket, timeout_in_seconds=60):
        end_time = time.time() + float(timeout_in_seconds)
        compaction_started = False
        while time.time() < end_time:
            status, progress = rest.check_compaction_status(bucket)
            if status:
                log.info("compaction progress is %s" % progress)
                time.sleep(1)
                compaction_started = True
            elif compaction_started:
                return True
            else:
                log.info("auto compaction is not started yet.")
                time.sleep(1)
        log.error("auto compaction is not started in {0} sec.".format(str(timeout_in_seconds)))
        return False

    def terminate_processes(self, info, list):
        for process in list:
            type = info.distribution_type.lower()
            if type == "windows":
                self.execute_command("taskkill /F /T /IM {0}".format(process))
            elif type in ["ubuntu", "centos", "red hat"]:
                self.terminate_process(info, process)

    def remove_folders(self, list):
        for folder in list:
            output, error = self.execute_command("rm -rf {0}".format(folder))
            self.log_command_output(output, error)


    def couchbase_uninstall(self):
        linux_folders = ["/var/opt/membase", "/opt/membase", "/etc/opt/membase",
                         "/var/membase/data/*", "/opt/membase/var/lib/membase/*",
                         "/opt/couchbase"]
        terminate_process_list = ["beam.smp", "memcached", "moxi", "vbucketmigrator",
                                  "couchdb", "epmd", "memsup", "cpu_sup"]
        version_file = "VERSION.txt"
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info(self.info.distribution_type)
        type = self.info.distribution_type.lower()
        if type == 'windows':
            product = "cb"
            query = BuildQuery()
            builds, changes = query.get_all_builds()
            os_type = "exe"
            task = "uninstall"
            bat_file = "uninstall.bat"
            product_name = "couchbase-server-enterprise"
            version_path = "/cygdrive/c/Program Files/Couchbase/Server/"
            deleted = False

            exist = self.file_exists(version_path, version_file)
            log.info("Is VERSION file existed on {0}? {1}".format(self.ip, exist))
            if exist:
                log.info("VERSION file exists.  Start to uninstall {0} on {1} server".format(product, self.ip))
                build_name, short_version, full_version = self.find_build_version(version_path, version_file, product)
                log.info('Build name: {0}'.format(build_name))
                build_name = build_name.rstrip() + ".exe"
                log.info('Check if {0} is in tmp directory on {1} server'.format(build_name, self.ip))
                exist = self.file_exists("/cygdrive/c/tmp/", build_name)
                if not exist:  # if not exist in tmp dir, start to download that version build
                    build = query.find_build(builds, product_name, os_type, self.info.architecture_type, full_version)
                    downloaded = self.download_binary_in_win(build.url, short_version)
                    if downloaded:
                        log.info('Successful download {0}.exe on {1} server'.format(short_version, self.ip))
                    else:
                        log.error('Download {0}.exe failed'.format(short_version))
                dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
                self.create_multiple_dir(dir_paths)
                self.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
                self.stop_couchbase()
                # modify bat file to run uninstall schedule task
                self.create_windows_capture_file(task, product, full_version)
                self.modify_bat_file('/cygdrive/c/automation', bat_file, product, short_version, task)
                self.stop_schedule_tasks()
                log.info('sleep for 5 seconds before running task schedule uninstall on {0}'.format(self.ip))
                time.sleep(5)
                # run schedule task uninstall couchbase server
                output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
                self.log_command_output(output, error)
                deleted = self.wait_till_file_deleted(version_path, version_file, timeout_in_seconds=600)
                if not deleted:
                    log.error("Uninstall was failed at node {0}".format(self.ip))
                    sys.exit()
                self.sleep(30, "next step is to install")
                output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN removeme /V")
                self.log_command_output(output, error)
                # delete binary after uninstall
                output, error = self.execute_command("rm /cygdrive/c/tmp/{0}".format(build_name))
                self.log_command_output(output, error)
            else:
                log.info("No couchbase server on {0} server. Free to install".format(self.ip))
        elif type in ["ubuntu", "centos", "red hat"]:
            # uninstallation command is different
            if type == "ubuntu":
                uninstall_cmd = "dpkg -r {0};dpkg --purge {1};".format("couchbase-server", "couchbase-server")
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type in ["centos", "red hat"]:
                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server")
                log.info('running rpm -e to remove couchbase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            self.terminate_processes(self.info, terminate_process_list)
            self.remove_folders(linux_folders)
        elif self.info.distribution_type.lower() == 'mac':
            self.stop_server(os='mac')
            self.terminate_processes(self.info, terminate_process_list)
            output, error = self.execute_command("rm -rf /Applications/Couchbase\ Server.app")
            self.log_command_output(output, error)
            output, error = self.execute_command("rm -rf ~/Library/Application\ Support/Couchbase")
            self.log_command_output(output, error)

    def couchbase_win_uninstall(self, product, version, os_name, query):
        builds, changes = query.get_all_builds(version=version)
        version_file = 'VERSION.txt'
        bat_file = "uninstall.bat"
        task = "uninstall"
        deleted = False

        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        ex_type = self.info.deliverable_type
        if self.info.architecture_type == "x86_64":
            os_type = "64"
        elif self.info.architecture_type == "x86":
            os_type = "32"
        if product == "cse":
            name = "couchbase-server-enterprise"
            version_path = "/cygdrive/c/Program Files/Couchbase/Server/"

        exist = self.file_exists(version_path, version_file)
        if exist:
            # call uninstall function to install couchbase server
            # Need to detect csse or cse when uninstall.
            log.info("Start uninstall cb server on this server")
            build_name, rm_version = self.find_build_version(version_path, version_file)
            log.info('build needed to do auto uninstall {0}'.format(build_name))
            # find installed build in tmp directory to match with currently installed version
            build_name = build_name.rstrip() + ".exe"
            log.info('Check if {0} is in tmp directory'.format(build_name))
            exist = self.file_exists("/cygdrive/c/tmp/", build_name)
            if not exist:  # if not exist in tmp dir, start to download that version build
                build = query.find_build(builds, name, ex_type, self.info.architecture_type, rm_version)
                downloaded = self.download_binary_in_win(build.url, rm_version)
                if downloaded:
                    log.info('Successful download {0}.exe'.format(rm_version))
                else:
                    log.error('Download {0}.exe failed'.format(rm_version))
            # copy required files to automation directory
            dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
            self.create_multiple_dir(dir_paths)
            self.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
            # modify bat file to run uninstall schedule task
            self.modify_bat_file('/cygdrive/c/automation', bat_file, product, rm_version, task)
            self.sleep(5, "before running task schedule uninstall")
            # run schedule task uninstall couchbase server
            output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
            self.log_command_output(output, error)
            deleted = self.wait_till_file_deleted(version_path, version_file, timeout_in_seconds=600)
            if not deleted:
                log.error("Uninstall was failed at node {0}".format(self.ip))
                sys.exit()
            self.sleep(15, "next step is to install")
            output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN removeme /V")
            self.log_command_output(output, error)
            output, error = self.execute_command("rm /cygdrive/c/tmp/{0}".format(build_name))
            self.log_command_output(output, error)
        else:
            log.info('No couchbase server on this server')

    def membase_uninstall(self, save_upgrade_config=False):
        linux_folders = ["/var/opt/membase", "/opt/membase", "/etc/opt/membase",
                         "/var/membase/data/*", "/opt/membase/var/lib/membase/*"]
        terminate_process_list = ["beam", "memcached", "moxi", "vbucketmigrator",
                                  "couchdb", "epmd"]
        version_file = "VERSION.txt"
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info(self.info.distribution_type)
        type = self.info.distribution_type.lower()
        if type == 'windows':
            product = "mb"
            query = BuildQuery()
            builds, changes = query.get_all_builds()
            os_type = "exe"
            task = "uninstall"
            bat_file = "uninstall.bat"
            deleted = False
            product_name = "membase-server-enterprise"
            version_path = "/cygdrive/c/Program Files/Membase/Server/"

            exist = self.file_exists(version_path, version_file)
            log.info("Is VERSION file existed? {0}".format(exist))
            if exist:
                log.info("VERSION file exists.  Start to uninstall")
                build_name, short_version, full_version = self.find_build_version(version_path, version_file, product)
                if "1.8.0" in full_version or "1.8.1" in full_version:
                    product_name = "couchbase-server-enterprise"
                    product = "cb"
                log.info('Build name: {0}'.format(build_name))
                build_name = build_name.rstrip() + ".exe"
                log.info('Check if {0} is in tmp directory'.format(build_name))
                exist = self.file_exists("/cygdrive/c/tmp/", build_name)
                if not exist:  # if not exist in tmp dir, start to download that version build
                    build = query.find_build(builds, product_name, os_type, self.info.architecture_type, full_version)
                    downloaded = self.download_binary_in_win(build.url, short_version)
                    if downloaded:
                        log.info('Successful download {0}_{1}.exe'.format(product, short_version))
                    else:
                        log.error('Download {0}_{1}.exe failed'.format(product, short_version))
                dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
                self.create_multiple_dir(dir_paths)
                self.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
                # modify bat file to run uninstall schedule task
                self.create_windows_capture_file(task, product, full_version)
                self.modify_bat_file('/cygdrive/c/automation', bat_file, product, short_version, task)
                self.stop_schedule_tasks()
                self.sleep(5, "before running task schedule uninstall")
                # run schedule task uninstall Couchbase server
                output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
                self.log_command_output(output, error)
                deleted = self.wait_till_file_deleted(version_path, version_file, timeout_in_seconds=600)
                if not deleted:
                    log.error("Uninstall was failed at node {0}".format(self.ip))
                    sys.exit()
                self.sleep(30, "nex step is to install")
                output, error = self.execute_command("cmd /c schtasks /Query /FO LIST /TN removeme /V")
                self.log_command_output(output, error)
            else:
                log.info("No membase server on this server.  Free to install")
        elif type in ["ubuntu", "centos", "red hat"]:
            # uninstallation command is different
            if type == "ubuntu":
                uninstall_cmd = 'dpkg -r {0};dpkg --purge {1};'.format('membase-server', 'membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type in ["centos", "red hat"]:
                uninstall_cmd = 'rpm -e {0}'.format('membase-server')
                log.info('running rpm -e to remove membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            self.terminate_processes(self.info, terminate_process_list)
            if not save_upgrade_config:
                self.remove_folders(linux_folders)

    def moxi_uninstall(self):
        terminate_process_list = ["moxi"]
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        log.info(self.info.distribution_type)
        type = self.info.distribution_type.lower()
        if type == 'windows':
            self.log.error("Not implemented")
        elif type == "ubuntu":
            uninstall_cmd = "dpkg -r {0};dpkg --purge {1};".format("moxi-server", "moxi-server")
            output, error = self.execute_command(uninstall_cmd)
            self.log_command_output(output, error)
        elif type in ["centos", "red hat"]:
            uninstall_cmd = 'rpm -e {0}'.format("moxi-server")
            log.info('running rpm -e to remove couchbase-server')
            output, error = self.execute_command(uninstall_cmd)
            self.log_command_output(output, error)
        self.terminate_processes(self.info, terminate_process_list)

    def log_command_output(self, output, error, track_words=()):
        # success means that there are no track_words in the output
        # and there are no errors at all, if track_words is not empty
        # if track_words=(), the result is not important, and we return True
        success = True
        for line in error:
            log.error(line)
            if track_words:
                success = False
        for line in output:
            log.info(line)
            if any(s.lower() in line.lower() for s in track_words):
                success = False
                log.error('something wrong happened on {0}!!! output:{1}, error:{2}, track_words:{3}'
                          .format(self.ip, output, error, track_words))
        return success

    def execute_commands_inside(self, main_command, subcommands=[], min_output_size=0,
                                end_msg='', timeout=250):
        log.info("running command on {0}: {1}".format(self.ip, main_command))

        stdin, stdout, stderro = self._ssh_client.exec_command(main_command)
        time.sleep(1)
        for cmd in subcommands:
              log.info("running command {0} inside {1} ({2})".format(
                                                        main_command, cmd, self.ip))
              stdin.channel.send("{0}\n".format(cmd))
              end_time = time.time() + float(timeout)
              while True:
                  if time.time() > end_time:
                      raise Exception("no output in {3} sec running command \
                                       {0} inside {1} ({2})".format(main_command,
                                                                    cmd, self.ip,
                                                                    timeout))
                  output = stdout.channel.recv(1024)
                  if output.strip().endswith(end_msg) and len(output) >= min_output_size:
                          break
                  time.sleep(2)
              log.info("{0}:'{1}' -> '{2}' output\n: {3}".format(self.ip, main_command, cmd, output))
        stdin.close()
        stdout.close()
        stderro.close()
        return output

    def execute_command(self, command, info=None, debug=True, use_channel=False):
        if info is None and (getattr(self, "info", None) is None):
            self.info = self.extract_remote_info()
        elif getattr(self, "info", None) is None and info is not None :
            self.info = info

        if self.info.type.lower() == 'windows':
            self.use_sudo = False

        if self.use_sudo:
            command = "sudo " + command

        return self.execute_command_raw(command, debug=debug, use_channel=use_channel)

    def execute_command_raw(self, command, debug=True, use_channel=False):
        if debug:
            log.info("running command.raw on {0}: {1}".format(self.ip, command))
        output = []
        error = []
        temp = ''
        if self.use_sudo or use_channel:
            channel = self._ssh_client.get_transport().open_session()
            channel.get_pty()
            channel.settimeout(600)
            stdin = channel.makefile('wb')
            stdout = channel.makefile('rb')
            stderro = channel.makefile_stderr('rb')
            channel.exec_command(command)
            data = channel.recv(1024)
            while data:
                temp += data
                data = channel.recv(1024)
            channel.close()
        else:
            stdin, stdout, stderro = self._ssh_client.exec_command(command)
        stdin.close()

        for line in stdout.read().splitlines():
            output.append(line)
        for line in stderro.read().splitlines():
            error.append(line)
        if temp:
            line = temp.splitlines()
            output.extend(line)
        if debug:
            log.info('command executed successfully')
        stdout.close()
        stderro.close()
        return output, error

    def execute_non_sudo_command(self, command, info=None, debug=True, use_channel=False):

        info = info or getattr(self, "info", None)
        if info is None:
            info = self.extract_remote_info()
            self.info = info

        return self.execute_command_raw(command, debug=debug, use_channel=use_channel)

    def terminate_process(self, info=None, process_name=''):
        if not hasattr(self, 'info') or self.info is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM {0}*".format(process_name))
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("killall -9 {0}".format(process_name))
            self.log_command_output(o, r)

    def disconnect(self):
        self._ssh_client.close()

    def extract_remote_info(self):
        # use ssh to extract remote machine info
        # use sftp to if certain types exists or not
        mac_check_cmd = "sw_vers | grep ProductVersion | awk '{ print $2 }'"
        stdin, stdout, stderro = self._ssh_client.exec_command(mac_check_cmd)
        stdin.close()
        ver, err = stdout.read(), stderro.read()
        if not err:
            os_distro = "Mac"
            os_version = ver
            is_linux_distro = True
            is_mac = True
            self.use_sudo = False
        else:
            is_mac = False
            sftp = self._ssh_client.open_sftp()
            filenames = sftp.listdir('/etc/')
            os_distro = ""
            os_version = ""
            is_linux_distro = False
            for name in filenames:
                if name == 'issue':
                    # it's a linux_distro . let's downlaod this file
                    # format Ubuntu 10.04 LTS \n \l
                    filename = 'etc-issue-{0}'.format(uuid.uuid4())
                    sftp.get(localpath=filename, remotepath='/etc/issue')
                    file = open(filename)
                    etc_issue = ''
                    # let's only read the first line
                    for line in file.xreadlines():
                        etc_issue = line
                        break
                        # strip all extra characters
                    etc_issue = etc_issue.rstrip('\n').rstrip('\\l').rstrip('\\n')
                    if etc_issue.lower().find('ubuntu') != -1:
                        os_distro = 'Ubuntu'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('debian') != -1:
                        os_distro = 'Ubuntu'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('mint') != -1:
                        os_distro = 'Ubuntu'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('amazon linux ami') != -1:
                        os_distro = 'CentOS'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('centos') != -1:
                        os_distro = 'CentOS'
                        os_version = etc_issue
                        is_linux_distro = True
                    elif etc_issue.lower().find('red hat') != -1:
                        os_distro = 'Red Hat'
                        os_version = etc_issue
                        is_linux_distro = True
                    file.close()
                    # now remove this file
                    os.remove(filename)
                    break
        if not is_linux_distro:
            arch = ''
            os_version = 'unknown windows'
            win_info = self.find_windows_info()
            info = RemoteMachineInfo()
            info.type = win_info['os']
            info.windows_name = win_info['os_name']
            info.distribution_type = win_info['os']
            info.architecture_type = win_info['os_arch']
            info.ip = self.ip
            info.distribution_version = win_info['os']
            info.deliverable_type = 'exe'
            info.cpu = self.get_cpu_info(win_info)
            info.disk = self.get_disk_info(win_info)
            info.ram = self.get_ram_info(win_info)
            info.hostname = self.get_hostname(win_info)
            return info
        else:
            # now run uname -m to get the architechtre type
            stdin, stdout, stderro = self._ssh_client.exec_command('uname -m')
            stdin.close()
            os_arch = ''
            text = stdout.read().splitlines()
            for line in text:
                os_arch += line
                # at this point we should know if its a linux or windows ditro
            ext = {'Ubuntu': 'deb', 'CentOS': 'rpm',
                   'Red Hat': 'rpm', "Mac": "zip"}.get(os_distro, '')
            arch = {'i686': 'x86', 'i386': 'x86'}.get(os_arch, os_arch)
            info = RemoteMachineInfo()
            info.type = "Linux"
            info.distribution_type = os_distro
            info.architecture_type = arch
            info.ip = self.ip
            info.distribution_version = os_version
            info.deliverable_type = ext
            info.cpu = self.get_cpu_info(mac=is_mac)
            info.disk = self.get_disk_info(mac=is_mac)
            info.ram = self.get_ram_info(mac=is_mac)
            info.hostname = self.get_hostname()
            info.domain = self.get_domain()
            return info

    def get_extended_windows_info(self):
        info = {}
        win_info = self.extend_windows_info()
        info['ram'] = self.get_ram_info(win_info)
        info['disk'] = self.get_disk_info(win_info)
        info['cpu'] = self.get_cpu_info(win_info)
        info['hostname'] = self.get_hostname()
        return info

    def get_hostname(self, win_info=None):
        if win_info:
            if 'Host Name' not in win_info:
                win_info = self.create_windows_info()
            o = win_info['Host Name']
        o, r = self.execute_command_raw('hostname')
        if o:
            return o

    def get_domain(self):
        ret = self.execute_command_raw('hostname -d')
        return ret

    def get_cpu_info(self, win_info=None, mac=False):
        if win_info:
            if 'Processor(s)' not in win_info:
                win_info = self.create_windows_info()
            o = win_info['Processor(s)']
        elif mac:
            o, r = self.execute_command_raw('/sbin/sysctl -n machdep.cpu.brand_string')
        else:
            o, r = self.execute_command_raw('sudo cat /proc/cpuinfo')
        if o:
            return o

    def get_ram_info(self, win_info=None, mac=False):
        if win_info:
            if 'Virtual Memory Max Size' not in win_info:
                win_info = self.create_windows_info()
            o = "Virtual Memory Max Size =" + win_info['Virtual Memory Max Size'] + '\n'
            o += "Virtual Memory Available =" + win_info['Virtual Memory Available'] + '\n'
            o += "Virtual Memory In Use =" + win_info['Virtual Memory In Use']
        elif mac:
            o, r = self.execute_command_raw('/sbin/sysctl -n hw.memsize')
        else:
            o, r = self.execute_command_raw('sudo cat /proc/meminfo')
        if o:
            return o

    def get_disk_info(self, win_info=None, mac=False):
        if win_info:
            if 'Total Physical Memory' not in win_info:
                win_info = self.create_windows_info()
            o = "Total Physical Memory =" + win_info['Total Physical Memory'] + '\n'
            o += "Available Physical Memory =" + win_info['Available Physical Memory']
        elif mac:
            o, r = self.execute_command_raw('df -h')
        else:
            o, r = self.execute_command_raw('df -Th')
        if o:
            return o

    def stop_couchbase(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            self.sleep(10, "Wait to stop service completely")
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/couchbase-server stop", self.info, use_channel=True)
            self.log_command_output(o, r)
        if self.info.distribution_type.lower() == "mac":
            o, r = self.execute_command("ps aux | grep Couchbase | awk '{print $2}' | xargs kill -9")
            self.log_command_output(o, r)
            o, r = self.execute_command("killall -9 epmd")
            self.log_command_output(o, r)

    def start_couchbase(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == 'windows':
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/couchbase-server start")
            self.log_command_output(o, r)
        if self.info.distribution_type.lower() == "mac":
            o, r = self.execute_command("open /Applications/Couchbase\ Server.app")
            self.log_command_output(o, r)

    def pause_memcached(self):
        o, r = self.execute_command("killall -SIGSTOP memcached")
        self.log_command_output(o, r)

    def unpause_memcached(self):
        o, r = self.execute_command("killall -SIGCONT memcached")
        self.log_command_output(o, r)

    def pause_beam(self):
        o, r = self.execute_command("killall -SIGSTOP beam")
        self.log_command_output(o, r)

    def unpause_beam(self):
        o, r = self.execute_command("killall -SIGCONT beam")
        self.log_command_output(o, r)


    # TODO: Windows
    def flush_os_caches(self):
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("sync")
            self.log_command_output(o, r)
            o, r = self.execute_command("/sbin/sysctl vm.drop_caches=3")
            self.log_command_output(o, r)

    def get_data_file_size(self, path=None):

        output, error = self.execute_command('du -b {0}'.format(path))
        if error:
            return 0
        else:
            for line in output:
                size = line.strip().split('\t')
                if size[0].isdigit():
                    print size[0]
                    return size[0]
                else:
                    return 0

    def get_process_statistics(self, process_name=None, process_pid=None):
        '''
        Gets process statistics for windows nodes
        WMI is required to be intalled on the node
        stats_windows_helper should be located on the node
        '''
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        remote_command = "cd ~; /cygdrive/c/Python27/python stats_windows_helper.py"
        if process_name:
            remote_command.append(" " + process_name)
        elif process_pid:
            remote_command.append(" " + process_pid)

        if self.info.type.lower() == "windows":
            o, r = self.execute_command(remote_command, info)
            if r:
                log.error("Command didn't run successfully. Error: {0}".format(r))
            return o;
        else:
            log.error("Function is implemented only for Windows OS")
            return None

    def get_process_statistics_parameter(self, parameter, process_name=None, process_pid=None):
       if not parameter:
           log.error("parameter cannot be None")

       parameters_list = self.get_process_statistics(process_name, process_pid)

       if not parameters_list:
           log.error("no statistics found")
           return None
       parameters_dic = dict(item.split(' = ') for item in parameters_list)

       if parameter in parameters_dic:
           return parameters_dic[parameter]
       else:
           log.error("parameter '{0}' is not found".format(parameter))
           return None

    def set_environment_variable(self, name, value):
        """Request an interactive shell session, export custom variable and
        restart Couchbase server.

        Shell session is necessary because basic SSH client is stateless.
        """

        shell = self._ssh_client.invoke_shell()
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == "windows":
            shell.send('net stop CouchbaseServer\n')
            shell.send('set {0}={1}\n'.format(name, value))
            shell.send('net start CouchbaseServer\n')
        elif self.info.type.lower() == "linux":
            shell.send('export {0}={1}\n'.format(name, value))
            shell.send('/etc/init.d/couchbase-server restart\n')
        shell.close()

    def change_env_variables(self, dict):
        prefix="\\n    "
        shell = self._ssh_client.invoke_shell()
        init_file="couchbase_init.d"
        file_path="/opt/couchbase/etc/"
        environmentVariables=""
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        if self.info.type.lower() == "windows":
            init_file="service_start.bat"
            file_path="/cygdrive/c/Program Files/Couchbase/Server/bin/"
            prefix="\\n"
        backupfile=file_path+init_file+".bak"
        sourceFile=file_path+init_file
        o, r = self.execute_command("cp "+sourceFile+" "+backupfile)
        self.log_command_output(o, r)
        command="sed -i 's/{0}/{0}".format("ulimit -l unlimited")
        for key in dict.keys():
            o, r = self.execute_command("sed -i 's/{1}.*//' {0}".format(sourceFile,key))
            self.log_command_output(o, r)

        if self.info.type.lower() == "windows":
            command="sed -i 's/{0}/{0}".format("set NS_ERTS=%NS_ROOT%\erts-5.8.5.cb1\bin")

        for key in dict.keys():
            if self.info.type.lower() == "windows":
                environmentVariables+=prefix+'set {0}={1}'.format(key, dict[key])
            else:
                environmentVariables+=prefix+'export {0}={1}'.format(key, dict[key])

        command+=environmentVariables+"/'"+" "+sourceFile
        o, r = self.execute_command(command)
        self.log_command_output(o, r)

        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/couchbase-server restart")
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        shell.close()

    def reset_env_variables(self):
        shell = self._ssh_client.invoke_shell()
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        init_file="couchbase_init.d"
        file_path="/opt/couchbase/etc/"
        if self.info.type.lower() == "windows":
            init_file="service_start.bat"
            file_path="/cygdrive/c/Program Files/Couchbase/Server/bin"
        backupfile=file_path+init_file+".bak"
        sourceFile=file_path+init_file
        o, r = self.execute_command("mv "+backupfile+" "+sourceFile)
        self.log_command_output(o, r)
        if self.info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/couchbase-server restart")
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        shell.close()

    def set_node_name(self, name):
        """Edit couchbase-server shell script in place and set custom node name.
        This is necessary for cloud installations where nodes have both
        private and public addresses.

        It only works on Unix-like OS.

        Reference: http://bit.ly/couchbase-bestpractice-cloud-ip
        """

        # Stop server
        self.stop_couchbase()

        # Edit _start function
        cmd = r"sed -i 's/\(.*\-run ns_bootstrap.*\)/\1\n\t-name ns_1@{0} \\/' \
                /opt/couchbase/bin/couchbase-server".format(name)
        self.execute_command(cmd)

        # Cleanup
        for cmd in ('rm -fr /opt/couchbase/var/lib/couchbase/data/*',
                    'rm -fr /opt/couchbase/var/lib/couchbase/mnesia/*',
                    'rm -f /opt/couchbase/var/lib/couchbase/config/config.dat'):
            self.execute_command(cmd)

        # Start server
        self.start_couchbase()

    def execute_cluster_backup(self, login_info="Administrator:password", backup_location="/tmp/backup",
                               command_options='', cluster_ip="", cluster_port="8091"):
        backup_command = "%scbbackup" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        backup_file_location = backup_location
        # TODO: define WIN_COUCHBASE_BIN_PATH and implement a new function under RestConnectionHelper to use nodes/self info to get os info
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            backup_command = "%scbbackup.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH_RAW)
            backup_file_location = "C:%s" % (backup_location)
            output, error = self.execute_command("taskkill /F /T /IM cbbackup.exe")
            self.log_command_output(output, error)
        if self.info.distribution_type.lower() == 'mac':
            backup_command = "%scbbackup" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        self.delete_files(backup_file_location)
        self.create_directory(backup_file_location)

        command_options_string = ""
        if command_options is not '':
            command_options_string = ' '.join(command_options)
        cluster_ip = cluster_ip or self.ip
        cluster_port = cluster_port or self.port

        command = "%s %s%s@%s:%s %s %s" % (backup_command, "http://", login_info,
                                           cluster_ip, cluster_port, backup_file_location, command_options_string)
        if type == 'windows':
            command = "cmd /c START \"\" \"%s\" \"%s%s@%s:%s\" \"%s\" %s" % (backup_command, "http://", login_info,
                                               cluster_ip, cluster_port, backup_file_location, command_options_string)

        output, error = self.execute_command(command)
        self.log_command_output(output, error)

    def restore_backupFile(self, login_info, backup_location, buckets):
        restore_command = "%scbrestore" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        backup_file_location = backup_location
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            restore_command = "%scbrestore.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH_RAW)
            backup_file_location = "C:%s" % (backup_location)
        if self.info.distribution_type.lower() == 'mac':
            restore_command = "%scbrestore" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        for bucket in buckets:
            command = "%s %s %s%s@%s:%s %s %s" % (restore_command, backup_file_location, "http://",
                                                  login_info, self.ip, self.port, "-b", bucket)
            if type == 'windows':
                command = "cmd /c \"%s\" \"%s\" \"%s%s@%s:%s\" %s %s" % (restore_command, backup_file_location, "http://",
                                                      login_info, self.ip, self.port, "-b", bucket)
            output, error = self.execute_command(command)
            self.log_command_output(output, error)

    def delete_files(self, file_location):
        command = "%s%s" % ("rm -rf ", file_location)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)

    def execute_cbtransfer(self, source, destination, command_options=''):
        transfer_command = "%scbtransfer" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            transfer_command = "%scbtransfer.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH_RAW)
        if self.info.distribution_type.lower() == 'mac':
            transfer_command = "%scbtransfer" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        command = "%s %s %s %s" % (transfer_command, source, destination, command_options)
        if type == 'windows':
            command = "cmd /c \"%s\" \"%s\" \"%s\" %s" % (transfer_command, source, destination, command_options)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)

    def execute_cbdocloader(self, username, password, bucket, memory_quota, file):
        cbdocloader_command = "%stools/cbdocloader" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        command = "%s -u %s -p %s -n %s:%s -b %s -s %s %ssamples/%s.zip" % (cbdocloader_command,
                                                                            username, password, self.ip,
                                                                            self.port, bucket, memory_quota,
                                                                            testconstants.LINUX_CB_PATH, file)
        if self.info.distribution_type.lower() == 'mac':
            cbdocloader_command = "%stools/cbdocloader" % (testconstants.MAC_COUCHBASE_BIN_PATH)
            command = "%s -u %s -p %s -n %s:%s -b %s -s %s %ssamples/%s.zip" % (cbdocloader_command,
                                                                            username, password, self.ip,
                                                                            self.port, bucket, memory_quota,
                                                                            testconstants.MAC_CB_PATH, file)

        if type == 'windows':
            cbdocloader_command = "%stools/cbdocloader.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH)
            WIN_COUCHBASE_SAMPLES_PATH = "C:/Program\ Files/Couchbase/Server/samples/"
            command = "%s -u %s -p %s -n %s:%s -b %s -s %s %s%s.zip" % (cbdocloader_command,
                                                                        username, password, self.ip,
                                                                        self.port, bucket, memory_quota,
                                                                        WIN_COUCHBASE_SAMPLES_PATH, file)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_cbcollect_info(self, file):
        cbcollect_command = "%scbcollect_info" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            cbcollect_command = "%scbcollect_info.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbcollect_command = "%scbcollect_info" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        command = "%s %s" % (cbcollect_command, file)
        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def execute_cbepctl(self, bucket, persistence, param_type, param, value):
        cbepctl_command = "%scbepctl" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            cbepctl_command = "%scbepctl.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbepctl_command = "%scbepctl" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        if bucket.saslPassword == None:
            bucket.saslPassword = ''
        if persistence != "":
            command = "%s %s:11210 -b %s -p \"%s\" %s" % (cbepctl_command, self.ip,
                                                          bucket.name, bucket.saslPassword,
                                                          persistence)
        else:
            command = "%s %s:11210 -b %s -p \"%s\" %s %s %s" % (cbepctl_command, self.ip,
                                                                bucket.name, bucket.saslPassword,
                                                                param_type, param, value)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_cbstats(self, bucket, command, keyname="", vbid=0):
        cbstat_command = "%scbstats" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            cbstat_command = "%scbstats.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cbstat_command = "%scbstats" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        if command != "key" and command != "raw":
            if bucket.saslPassword == None:
                bucket.saslPassword = ''
            command = "%s %s:11210 %s -b %s -p \"%s\" " % (cbstat_command, self.ip, command,
                                                                bucket.name, bucket.saslPassword)
        else:
            command = "%s %s:11210 %s %s %s " % (cbstat_command, self.ip, command,
                                                                keyname, vbid)
        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_couchbase_cli(self, cli_command, cluster_host='localhost', options='', cluster_port=None, user='Administrator', password='password'):
        cb_client = "%scouchbase-cli" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()
        if type == 'windows':
            cb_client = "%scouchbase-cli.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH)
        if self.info.distribution_type.lower() == 'mac':
            cb_client = "%scouchbase-cli" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        cluster_param = (" --cluster={0}".format(cluster_host), "")[cluster_host is None]
        if cluster_param is not None:
            cluster_param += (":{0}".format(cluster_port), "")[cluster_port is None]

        user_param = (" -u {0}".format(user), "")[user is None]
        passwd_param = (" -p {0}".format(password), "")[password is None]
        # now we can run command in format where all parameters are optional
        # {PATH}/couchbase-cli [COMMAND] [CLUSTER:[PORT]] [USER] [PASWORD] [OPTIONS]
        command = cb_client + " " + cli_command + cluster_param + user_param + passwd_param + " " + options

        output, error = self.execute_command(command, use_channel=True)
        self.log_command_output(output, error)
        return output, error

    def execute_cbworkloadgen(self, username, password, num_items, ratio, bucket, item_size, command_options):
        cbworkloadgen_command = "%scbworkloadgen" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        if getattr(self, "info", None) is None:
            self.info = self.extract_remote_info()
        type = self.info.type.lower()

        if self.info.distribution_type.lower() == 'mac':
            cbworkloadgen_command = "%scbworkloadgen" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        if type == 'windows':
            cbworkloadgen_command = "%scbworkloadgen.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH)

        command = "%s -n %s:%s -r %s -i %s -b %s -s %s %s -u %s -p %s" % (cbworkloadgen_command, self.ip, self.port,
                                                                          ratio, num_items, bucket, item_size,
                                                                          command_options, username, password)

        output, error = self.execute_command(command)
        self.log_command_output(output, error)
        return output, error

    def execute_cbhealthchecker(self, username, password, command_options=None, path_to_store=''):
        command_options_string = ""
        if command_options is not None:
            command_options_string = ' '.join(command_options)

        cbhealthchecker_command = "%scbhealthchecker" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        info = self.extract_remote_info()
        type = info.type.lower()
        if info.distribution_type.lower() == 'mac':
            cbhealthchecker_command = "%scbhealthchecker" % (testconstants.MAC_COUCHBASE_BIN_PATH)

        if type == 'windows':
            cbhealthchecker_command = "%scbhealthchecker" % (testconstants.WIN_COUCHBASE_BIN_PATH)

        if path_to_store:
            self.execute_command('rm -rf %s; mkdir %s;cd %s' % (path_to_store,
                                                path_to_store, path_to_store))

        command = "%s -u %s -p %s -c %s:%s %s" % (cbhealthchecker_command,
                                                username, password, self.ip,
                                                self.port, command_options_string)

        if path_to_store:
            command = "cd %s; %s -u %s -p %s -c %s:%s %s" % (path_to_store,
                                                cbhealthchecker_command,
                                                username, password, self.ip,
                                                self.port, command_options_string)

        output, error = self.execute_command_raw(command)
        self.log_command_output(output, error)
        return output, error

    def execute_batch_command(self, command):
        remote_command = "echo \"{0}\" > /tmp/cmd.bat; /tmp/cmd.bat".format(command)
        o, r = self.execute_command_raw(remote_command)
        if r:
            log.error("Command didn't run successfully. Error: {0}".format(r))
        return o;

    def remove_win_backup_dir(self):
        win_paths = [testconstants.WIN_CB_PATH, testconstants.WIN_MB_PATH]
        for each_path in win_paths:
            backup_files = []
            files = self.list_files(each_path)
            for f in files:
                if f["file"].startswith("backup-"):
                    backup_files.append(f["file"])
             # keep the last one
            if len(backup_files) > 2:
                log.info("start remove previous backup directory")
                for f in backup_files[:-1]:
                    self.execute_command("rm -rf '{0}{1}'".format(each_path, f))

    def remove_win_collect_tmp(self):
        win_tmp_path = testconstants.WIN_TMP_PATH
        log.info("start remove tmp files from directory %s" % win_tmp_path)
        self.execute_command("rm -rf '%stmp*'" % win_tmp_path)

    # ps_name_or_id means process name or ID will be suspended
    def windows_process_utils(self, ps_name_or_id, cmd_file_name, option=""):
        success = False
        files_path = "cygdrive/c/utils/suspend/"
        # check to see if suspend files exist in server
        file_existed = self.file_exists(files_path, cmd_file_name)
        if file_existed:
            command = "{0}{1} {2} {3}".format(files_path, cmd_file_name, option, ps_name_or_id)
            o, r = self.execute_command(command)
            if not r:
                success = True
                self.log_command_output(o, r)
                self.sleep(30, "Wait for windows to execute completely")
            else:
                log.error("Command didn't run successfully. Error: {0}".format(r))
        else:
            o, r = self.execute_command("netsh advfirewall firewall add rule name=\"block erl.exe in\" dir=in action=block program=\"%ProgramFiles%\Couchbase\Server\\bin\erl.exe\"")
            if not r:
                success = True
                self.log_command_output(o, r)
            o, r = self.execute_command("netsh advfirewall firewall add rule name=\"block erl.exe out\" dir=out action=block program=\"%ProgramFiles%\Couchbase\Server\\bin\erl.exe\"")
            if not r:
                success = True
                self.log_command_output(o, r)
        return success

    def check_openssl_version(self, deliverable_type, openssl):
        if self.info.deliverable_type == "deb":
            ubuntu_version = ["12.04", "13.04"]
            o, r = self.execute_command("lsb_release -r")
            self.log_command_output(o, r)
            if o[0] != "":
                o = o[0].split(":")
                if o[1].strip() in ubuntu_version and "1" in openssl:
                    o, r = self.execute_command("dpkg --get-selections | grep libssl")
                    self.log_command_output(o, r)
                    for s in o:
                        if "libssl0.9.8" in s:
                            o, r = self.execute_command("apt-get --purge remove -y {0}".format(s[:11]))
                            self.log_command_output(o, r)
                            o, r = self.execute_command("dpkg --get-selections | grep libssl")
                            log.info("package {0} should not appear below".format(s[:11]))
                            self.log_command_output(o, r)
                elif openssl == "":
                    o, r = self.execute_command("dpkg --get-selections | grep libssl")
                    self.log_command_output(o, r)
                    if not o:
                        o, r = self.execute_command("apt-get install -y libssl0.9.8")
                        self.log_command_output(o, r)
                        o, r = self.execute_command("dpkg --get-selections | grep libssl")
                        log.info("package {0} should not appear below".format(s[:11]))
                        self.log_command_output(o, r)
                    elif o:
                        for s in o:
                            if "libssl0.9.8" not in s:
                                o, r = self.execute_command("apt-get install -y libssl0.9.8")
                                self.log_command_output(o, r)
                                o, r = self.execute_command("dpkg --get-selections | grep libssl")
                                log.info("package {0} should not appear below".format(s[:11]))
                                self.log_command_output(o, r)
        if self.info.deliverable_type == "rpm":
            centos_version = ["6.4"]
            o, r = self.execute_command("cat /etc/redhat-release")
            self.log_command_output(o, r)
            if o[0] != "":
                o = o[0].split(" ")
                if o[2] in centos_version and "1" in openssl:
                    o, r = self.execute_command("rpm -qa | grep openssl")
                    self.log_command_output(o, r)
                    for s in o:
                        if "openssl098e" in s:
                            o, r = self.execute_command("yum remove -y {0}".format(s))
                            self.log_command_output(o, r)
                            o, r = self.execute_command("rpm -qa | grep openssl")
                            log.info("package {0} should not appear below".format(s))
                            self.log_command_output(o, r)
                        elif "openssl-1.0.0" not in s:
                            o, r = self.execute_command("yum install -y openssl")
                            self.log_command_output(o, r)
                    o, r = self.execute_command("rpm -qa | grep openssl")
                    log.info("openssl-1.0.0 should appear below".format(s))
                    self.log_command_output(o, r)
                elif openssl == "":
                    o, r = self.execute_command("rpm -qa | grep openssl")
                    self.log_command_output(o, r)
                    if not o:
                        o, r = self.execute_command("yum install -y openssl098e")
                        self.log_command_output(o, r)
                        o, r = self.execute_command("rpm -qa | grep openssl")
                        log.info("package openssl098e should appear below")
                        self.log_command_output(o, r)
                    elif o:
                        for s in o:
                            if "openssl098e" not in s:
                                o, r = self.execute_command("yum install -y openssl098e")
                                self.log_command_output(o, r)
                                o, r = self.execute_command("rpm -qa | grep openssl")
                                log.info("package openssl098e should appear below")
                                self.log_command_output(o, r)

    def check_pkgconfig(self, deliverable_type, openssl):
        if self.info.deliverable_type == "rpm":
            centos_version = ["6.4"]
            o, r = self.execute_command("cat /etc/redhat-release")
            self.log_command_output(o, r)
            if o[0] != "":
                o = o[0].split(" ")
                if o[2] in centos_version and "1" in openssl:
                    o, r = self.execute_command("rpm -qa | grep pkgconfig")
                    self.log_command_output(o, r)
                    if not o:
                        o, r = self.execute_command("yum install -y pkgconfig")
                        self.log_command_output(o, r)
                        o, r = self.execute_command("rpm -qa | grep pkgconfig")
                        log.info("package pkgconfig should appear below")
                        self.log_command_output(o, r)
                    elif o:
                        for s in o:
                            if "pkgconfig" not in s:
                                o, r = self.execute_command("yum install -y pkgconfig")
                                self.log_command_output(o, r)
                                o, r = self.execute_command("rpm -qa | grep pkgconfig")
                                log.info("package pkgconfig should appear below")
                                self.log_command_output(o, r)
                elif openssl == "":
                    log.info("no need to install pkgconfig on couchbase binary using openssl 0.9.8")

    def install_missing_lib(self):
        if self.info.deliverable_type == "deb":
            for lib_name in MISSING_UBUNTU_LIB:
                if lib_name != "":
                    log.info("prepare install library {0}".format(lib_name))
                    o, r = self.execute_command("apt-get install -y {0}".format(lib_name))
                    self.log_command_output(o, r)
                    o, r = self.execute_command("dpkg --get-selections | grep {0}".format(lib_name))
                    self.log_command_output(o, r)
                    log.info("lib {0} should appear around this line".format(lib_name))


class RemoteUtilHelper(object):

    @staticmethod
    def enable_firewall(server, bidirectional=False, xdcr=False):
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()
        if shell.info.type.lower() == "windows":
            o, r = shell.execute_command('netsh advfirewall set publicprofile state on')
            shell.log_command_output(o, r)
            o, r = shell.execute_command('netsh advfirewall set privateprofile state on')
            shell.log_command_output(o, r)
            log.info("enabled firewall on {0}".format(server))
            suspend_erlang = shell.windows_process_utils("pssuspend.exe", "erl.exe", option="")
            if suspend_erlang:
                log.info("erlang process is suspended")
            else:
                log.error("erlang process failed to suspend")
        else:
            # Reject incoming connections on port 1000->65535
            o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j REJECT")
            shell.log_command_output(o, r)

            # Reject outgoing connections on port 1000->65535
            if bidirectional:
                o, r = shell.execute_command("/sbin/iptables -A OUTPUT -p tcp -o eth0 --sport 1000:65535 -j REJECT")
                shell.log_command_output(o, r)

            if xdcr:
                o, r = shell.execute_command("/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
                shell.log_command_output(o, r)

            log.info("enabled firewall on {0}".format(server))
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def common_basic_setup(servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_couchbase()
            shell.disable_firewall()
            shell.unpause_memcached()
            shell.unpause_beam()
            shell.disconnect()
        time.sleep(10)

    @staticmethod
    def use_hostname_for_server_settings(server):
        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info()
        version = RestConnection(server).get_nodes_self().version
        hostname = info.hostname[0]
        time.sleep(5)
        if version.startswith("1.8.") or version.startswith("2.") or version.startswith("3."):
            shell.stop_couchbase()
            if info.type.lower() == "windows":
                o = shell.execute_batch_command('ipconfig')
                suffix_dns_row = [row for row in o if row.find(" Connection-specific DNS Suffix") != -1]
                suffix_dns = suffix_dns_row[0].split(':')[1].strip()
                hostname = '%s.%s' % (hostname, suffix_dns)

                cmd = "'C:/Program Files/Couchbase/Server/bin/service_unregister.bat'"
                shell.execute_command_raw(cmd)
                cmd = 'cat "C:\\Program Files\\Couchbase\\Server\\bin\\service_register.bat"'
                old_reg = shell.execute_command_raw(cmd)
                new_start = '\n'.join(old_reg[0]).replace("ns_1@%IP_ADDR%", "ns_1@%s" % hostname)
                cmd = "echo '%s' > 'C:\\Program Files\\Couchbase\\Server\\bin\\service_register.bat'" % new_start
                shell.execute_command_raw(cmd)
                cmd = "'C:/Program Files/Couchbase/Server/bin/service_register.bat'"
                shell.execute_command_raw(cmd)
                cmd = 'rm -rf  "C:/Program Files/Couchbase/Server/var/lib/couchbase/mnesia/*"'
                shell.execute_command_raw(cmd)
            else:
                o = shell.execute_command_raw('nslookup %s' % hostname)
                suffix_dns_row = [row for row in o[0] if row.find("Name:") != -1]
                hostname = suffix_dns_row[0].split(':')[1].strip()
                cmd = "cat  /opt/couchbase/bin/couchbase-server"
                old_start = shell.execute_command_raw(cmd)
                cmd = r"sed -i 's/\(.*\-run ns_bootstrap.*\)/\1\n\t-name ns_1@{0} \\/' \
                        /opt/couchbase/bin/couchbase-server".format(hostname)
                o, r = shell.execute_command(cmd)
                shell.log_command_output(o, r)
                time.sleep(2)
                cmd = 'rm -rf /opt/couchbase/var/lib/couchbase/data/*'
                shell.execute_command(cmd)
                cmd = 'rm -rf /opt/couchbase/var/lib/couchbase/mnesia/*'
                shell.execute_command(cmd)
                cmd = 'rm -rf /opt/couchbase/var/lib/couchbase/config/config.dat'
                shell.execute_command(cmd)
                cmd = 'echo "%s" > /opt/couchbase/var/lib/couchbase/ip' % hostname
                shell.execute_command(cmd)
        else:
            o = shell.execute_command_raw('nslookup %s' % hostname)
            suffix_dns_row = [row for row in o[0] if row.find("Name:") != -1]
            hostname = suffix_dns_row[0].split(':')[1].strip()
            RestConnection(server).rename_node(hostname)
            shell.stop_couchbase()
        shell.start_couchbase()
        shell.disconnect()
        return hostname

    @staticmethod
    def is_text_present_in_logs(server, text_for_search, logs_to_check='debug'):
        shell = RemoteMachineShellConnection(server)
        info = shell.extract_remote_info()
        if info.type.lower() != "windows":
            path_to_log = testconstants.LINUX_COUCHBASE_LOGS_PATH
        else:
            path_to_log = testconstants.WIN_COUCHBASE_LOGS_PATH
        log_files = []
        files = shell.list_files(path_to_log)
        for f in files:
            if f["file"].startswith(logs_to_check):
                log_files.append(f["file"])
        is_txt_found = False
        for f in log_files:
            o, r = shell.execute_command("cat {0}/{1} | grep '{2}'".format(path_to_log,
                                                                          f,
                                                                          text_for_search))
            if ' '.join(o).find(text_for_search) != -1:
                is_txt_found = True
                break
        return is_txt_found
