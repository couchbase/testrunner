import os
import uuid
import paramiko
import logger
import time
from builds.build_query import BuildQuery
import testconstants

log = logger.Logger.get_logger()


class RemoteMachineInfo(object):
    def __init__(self):
        self.type = ''
        self.ip = ''
        self.distribution_type = ''
        self.architecture_type = ''
        self.distribution_version = ''
        self.deliverable_type = ''


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
        #monitor this process and return if it crashes
        end_time = time.time() + float(duration_in_seconds)
        last_reported_pid = None
        while time.time() < end_time:
            #get the process list
            process = self.is_process_running(process_name)
            if process:
                if not last_reported_pid:
                    last_reported_pid = process.pid
                elif not last_reported_pid == process.pid:
                    message = 'process {0} was restarted. old pid : {1} new pid : {2}'
                    log.info(message.format(process_name, last_reported_pid, process.pid))
                    return False
                    #check if its equal
            else:
                # we should have an option to wait for the process
                # to start during the timeout
                #process might have crashed
                log.info("process {0} is not running or it might have crashed!".format(process_name))
                return False
            time.sleep(1)
        #            log.info('process {0} is running'.format(process_name))
        return True

    def is_process_running(self, process_name):
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
        #let's create a connection
        self._ssh_client = paramiko.SSHClient()
        self.ip = ip
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        log.info('connecting to {0} with username : {1} pem key : {2}'.format(ip, username, pkey_location))
        self._ssh_client.connect(hostname=ip,
                                 username=username,
                                 key_filename=pkey_location)

    def __init__(self, serverInfo):
        #let's create a connection
        self._ssh_client = paramiko.SSHClient()
        self.ip = serverInfo.ip
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if serverInfo.ssh_key == '':
            #connect with username/password
            msg = 'connecting to {0} with username : {1} password : {2}'
            log.info(msg.format(serverInfo.ip, serverInfo.ssh_username, serverInfo.ssh_password))
            self._ssh_client.connect(hostname=serverInfo.ip,
                                     username=serverInfo.ssh_username,
                                     password=serverInfo.ssh_password)
        else:
            msg = 'connecting to {0} with username : {1} pem key : {2}'
            log.info(msg.format(serverInfo.ip, serverInfo.ssh_username, serverInfo.ssh_key))
            self._ssh_client.connect(hostname=serverInfo.ip,
                                     username=serverInfo.ssh_username,
                                     key_filename=serverInfo.ssh_key)

    def get_running_processes(self):
        #if its linux ,then parse each line
        #26989 ?        00:00:51 pdflush
        #ps -Ao pid,comm
        processes = []
        output, error = self.execute_command('ps -Ao pid,comm', debug=False)
        if output:
            for line in output:
                #split to words
                words = line.split(' ')
                if len(words) >= 2:
                    process = RemoteMachineProcess()
                    process.pid = words[0]
                    process.name = words[1]
                    processes.append(process)
        return processes

    def stop_membase(self):
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            o, r = self.execute_command("net stop membaseserver")
            self.log_command_output(o, r)
        if info.type.lower() == "linux":
            #check if /opt/membase exists or /opt/couchbase or /opt/couchbase-single
            o, r = self.execute_command("/etc/init.d/membase-server stop")
            self.log_command_output(o, r)

    def start_membase(self):
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            o, r = self.execute_command("net start membaseserver")
            self.log_command_output(o, r)
        if info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/membase-server start")
            self.log_command_output(o, r)

    def is_membase_installed(self):
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            if self.file_exists(testconstants.WIN_CB_PATH, testconstants.VERSION_FILE):
                return False
        elif info.type.lower() == "linux":
            if self.file_exists(testconstants.LINUX_CB_PATH, testconstants.VERSION_FILE):
                return False
        return True

    def download_build(self, build):
        return self.download_binary(build.url, build.deliverable_type, build.name)

    def download_binary(self, url, deliverable_type, filename):
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            self.execute_command('taskkill /F /T /IM msiexec32.exe')
            self.execute_command('taskkill /F /T /IM msiexec.exe')
            self.execute_command('taskkill /F /T IM setup.exe')
            self.execute_command('taskkill /F /T /IM ISBEW64.*')
            self.execute_command('taskkill /F /T /IM firefox.*')
            self.execute_command('taskkill /F /T /IM iexplore.*')
            self.execute_command('taskkill /F /T /IM WerFault.*')
            output, error = self.execute_command("rm -rf /cygdrive/c/automation/setup.exe")
            self.log_command_output(output, error)
            output, error = self.execute_command(
                "cd /cygdrive/c/automation;cmd /c 'c:\\automation\\GnuWin32\\bin\\wget.exe -q {0} -O setup.exe';ls;".format(
                    url))
            self.log_command_output(output, error)
            return self.file_exists('/cygdrive/c/automation/', 'setup.exe')
        else:
        #try to push this build into
        #depending on the os
        #build.product has the full name
        #first remove the previous file if it exist ?
        #fix this :
            output, error = self.execute_command('cd /tmp ; D=$(mktemp -d cb_XXXX) ; mv {0} $D ; mv core.* $D ; rm -f * ; mv $D/* . ; rmdir $D'.format(filename))
            self.log_command_output(output, error)
            log.info('get md5 sum for local and remote')
            output, error = self.execute_command('cd /tmp ; rm -f *.md5 *.md5l ; wget -q {0}.md5 ; md5sum {1} > {1}.md5l'.format(url, filename))
            self.log_command_output(output, error)
            log.info('comparing md5 sum and downloading if needed')
            output, error = self.execute_command('cd /tmp;diff {0}.md5 {0}.md5l || wget -q -O {0} {1};rm -f *.md5 *.md5l'.format(filename, url))
            self.log_command_output(output, error)
            #check if the file exists there now ?
            return self.file_exists('/tmp', filename)
            #for linux environment we can just
            #figure out what version , check if /tmp/ has the
            #binary and then return True if binary is installed

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
            path_of_files_in_dir = remote_path + '/*'
            log.info("removing {0} directory...".format(remote_path))
            files = self.list_files(remote_path)
            for f in files:
                sftp.remove("{0}/{1}".format(f["path"], f["file"]))
                log.info("{0}/{1} is deleted".format(f["path"], f["file"]))
            sftp.rmdir(remote_path)
            sftp.close()
        except IOError:
            return False
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

    #check if this file exists in the remote
    #machine or not
    def file_starts_with(self, remotepath, pattern):
        sftp = self._ssh_client.open_sftp()
        files_matched = []
        try:
            file_names = sftp.listdir(remotepath)
            for name in file_names:
                if name.startswith(pattern):
                    files_matched.append("{0}/{1}".format(remotepath, name))
        except IOError:
            #ignore this error
            pass
        sftp.close()
        if len(files_matched) > 0:
            log.info("found these files : {0}".format(files_matched))
        return files_matched

    def file_exists(self, remotepath, filename):
        sftp = self._ssh_client.open_sftp()
        try:
            filenames = sftp.listdir(remotepath)
            for name in filenames:
                if name == filename:
                    sftp.close()
                    return True
            sftp.close()
            return False
        except IOError:
            return False

    def download_binary_in_win(self, url, name, version):
        info = self.extract_remote_info()
        self.execute_command('taskkill /F /T /IM msiexec32.exe')
        self.execute_command('taskkill /F /T /IM msiexec.exe')
        self.execute_command('taskkill /F /T IM setup.exe')
        self.execute_command('taskkill /F /T /IM ISBEW64.*')
        self.execute_command('taskkill /F /T /IM firefox.*')
        self.execute_command('taskkill /F /T /IM iexplore.*')
        self.execute_command('taskkill /F /T /IM WerFault.*')
        output, error = self.execute_command(
             "cd /cygdrive/c/tmp;cmd /c 'c:\\automation\\wget.exe -q {0} -O {1}_{2}.exe';ls;".format(
                url, name, version))
        self.log_command_output(output, error)
        return self.file_exists('/cygdrive/c/tmp/', '{0}_{1}.exe'.format(name, version))

    def copy_file_local_to_remote(self, src_path, des_path):
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.put(src_path, des_path)
            sftp.close()
        except IOError:
            log.error('Can not copy file')

    # copy multi files from local to remote server
    def copy_files_local_to_remote(self, src_path, des_path):
        files = os.listdir(src_path)
        for file in files:
            full_src_path = os.path.join(src_path, file)
            full_des_path = os.path.join(des_path, file)
            self.copy_file_local_to_remote(full_src_path, full_des_path)
            log.info('file "{0}" is copied to {1}'.format(file, des_path))

    # create a remote file from input string
    def create_file(self, remote_path, file_data):
        output, error = self.execute_command("echo '{0}' > {1}".format(file_data, remote_path))

    def find_file(self, remote_path, file):
        sftp = self._ssh_client.open_sftp()
        try:
            #found = False
            files = sftp.listdir(remote_path)
            log.info('File(s) name in {0}'.format(remote_path))
            for name in files:
                log.info(name)
                if name == file:
                    found_it = os.path.join(remote_path, name)
                    #found = True
                    return found_it
                    sftp.close()
                    break
            else:
                log.error('Can not find {0}'.format(file))
        except IOError:
            pass

    def find_build_version(self, path_to_version, version_file):
        sftp = self._ssh_client.open_sftp()
        ex_type = "exe"
        try:
            if path_to_version == "/cygdrive/c/Program Files/Couchbase/Server/":
                product_name = 'cb'
            elif path_to_version == "/cygdrive/c/Program Files (x86)/Couchbase/Server/":
                product_name = 'css'
            log.info(path_to_version)
            f = sftp.open(os.path.join(path_to_version, version_file), 'r+')
            full_version = f.read().rstrip()
            tmp = full_version.split("-")
            build_name = "{0}_{1}-{2}".format(product_name, tmp[0], tmp[1])
            short_version = "{0}-{1}".format(tmp[0], tmp[1])
            return build_name, short_version, full_version
        except IOError:
            log.error('Can not read version file')
        sftp.close()

    # this function used to modify bat file to run task schedule in windows
    def modify_bat_file(self, remote_path, file_name, name, os_type, os_version, version, task):
        found = self.find_file(remote_path, file_name)
        sftp = self._ssh_client.open_sftp()
        try:
            f = sftp.open(found, 'w')
            log.info('c:\\tmp\{0}_{1}.exe /s -f1c:\\automation\{2}_{3}_{4}_{5}.iss'.format(name, version, name,
                                                                                       os_type, os_version, task))
            name = name.rstrip()
            version = version.rstrip()
            f.write('c:\\tmp\{0}_{1}.exe /s -f1c:\\automation\{2}_{3}_{4}_{5}.iss'.format(name, version, name,
                                                                                       os_type, os_version, task))
            log.info('Successful write to {0}'.format(found))
            sftp.close()
        except IOError:
            log.error('Can not write build name file to bat file {0}'.format(found))

    def create_directory(self, remote_path):
        sftp = self._ssh_client.open_sftp()
        try:
            sftp.stat(remote_path)
        except IOError, e:
            if e[0] == 2:
                log.info("Directory at  {0} DOES NOT exist.  We will create on here".format(remote_path))
                sftp.mkdir(remote_path)
                sftp.close()
                return False
            raise
        else:
            log.error("Directory at  {0} DOES exist.  Fx returns True".format(remote_path))
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

    def membase_upgrade(self, build):
        #install membase server ?
        #run the right command
        info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(info.deliverable_type))
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            log.error('automation does not support windows upgrade yet!')
        elif info.deliverable_type == 'rpm':
            #run rpm -i to install
            log.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))
            output, error = self.execute_command('rpm -U /tmp/{0}'.format(build.name))
            self.log_command_output(output, error)
        elif info.deliverable_type == 'deb':
            output, error = self.execute_command('dpkg -i /tmp/{0}'.format(build.name))
            self.log_command_output(output, error)

    def couchbase_single_install(self, build):
        is_couchbase_single = False
        if build.name.lower().find("couchbase-single") != -1:
            is_couchbase_single = True
        if not is_couchbase_single:
            raise Exception("its not a couchbase-single ?")
        info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(info.deliverable_type))
        info = self.extract_remote_info()
        type = info.type.lower()
        if type == 'windows':
            win_processes = ["msiexec32.exe", "msiexec.exe", "setup.exe", "ISBEW64.*",
                             "WerFault.*"]
            self.terminate_processes(info, win_processes)
            install_init_command = "echo '[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-DlgOrder]\r\n\
Dlg0={95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdWelcome-0\r\n\
Count=4\r\n\
Dlg1={95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdAskDestPath-0\r\n\
Dlg2={95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdStartCopy2-0\r\n\
Dlg3={95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdFinish-0\r\n\
[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdWelcome-0]\r\n\
Result=1\r\n\
[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdAskDestPath-0]\r\n\
szDir=C:\\Program Files (x86)\\Couchbase\\Server\\\r\n\
Result=1\r\n\
[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdStartCopy2-0]\r\n\
Result=1\r\n\
[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdFinish-0]\r\n\
Result=1\r\n\
bOpt1=0\r\n\
bOpt2=0' > /cygdrive/c/automation/css_win2k8_64_install.iss"
            output, error = self.execute_command(install_init_command)
            self.log_command_output(output, error)
            install_command = "echo 'c:\\automation\\setup.exe /s -f1c:\\automation\\css_win2k8_64_install.iss' > /cygdrive/c/automation/install.bat"
            output, error = self.execute_command(install_command)
            self.log_command_output(output, error)
            output, error = self.execute_command("cmd /c schtasks /run /tn installme")
            self.log_command_output(output, error)
            self.wait_till_file_added("/cygdrive/c/Program Files (x86)/Couchbase/Server/", 'VERSION.txt',
                                          timeout_in_seconds=600)
        elif info.deliverable_type in ["rpm", "deb"]:
            if info.deliverable_type == 'rpm':
                log.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))
                output, error = self.execute_command('rpm -i /tmp/{0}'.format(build.name))
                self.log_command_output(output, error)
            elif info.deliverable_type == 'deb':
                output, error = self.execute_command('dpkg -i /tmp/{0}'.format(build.name))
                self.log_command_output(output, error)

    def membase_install(self, build, startserver=True):
        is_membase = False
        is_couchbase = False
        if build.name.lower().find("membase") != -1:
            is_membase = True
        elif build.name.lower().find("couchbase") != -1:
            is_couchbase = True
        if not is_membase and not is_couchbase:
            raise Exception("its not a membase or couchbase ?")
        info = self.extract_remote_info()
        log.info('deliverable_type : {0}'.format(info.deliverable_type))
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            win_processes = ["msiexec32.exe", "msiexec32.exe", "setup.exe", "ISBEW64.*",
                             "firefox.*", "WerFault.*", "iexplore.*"]
            self.terminate_processes(info, win_processes)
            output, error = self.execute_command("cmd /c schtasks /run /tn installme")
            self.log_command_output(output, error)
            if is_membase:
                self.wait_till_file_added("/cygdrive/c/Program Files/Membase/Server/", 'VERSION.txt',
                                          timeout_in_seconds=600)
            else:
                self.wait_till_file_added("/cygdrive/c/Program Files/Couchbase/Server/", 'VERSION.txt',
                                          timeout_in_seconds=600)
        elif info.deliverable_type in ["rpm", "deb"]:
            if startserver:
                environment = ""
            else:
                environment = "INSTALL_DONT_START_SERVER=1 "
            if info.deliverable_type == 'rpm':
                log.info('/tmp/{0} or /tmp/{1}'.format(build.name, build.product))
                output, error = self.execute_command('{0}rpm -i /tmp/{1}'.format(environment, build.name))
                self.log_command_output(output, error)
            elif info.deliverable_type == 'deb':
                output, error = self.execute_command('{0}dpkg -i /tmp/{1}'.format(environment, build.name))
                self.log_command_output(output, error)
            if is_membase:
                output, error = self.execute_command('/opt/membase/bin/mbenable_core_dumps.sh  /tmp')
                self.log_command_output(output, error)
            else:
                output, error = self.execute_command('/opt/couchbase/bin/mbenable_core_dumps.sh  /tmp')
                self.log_command_output(output, error)

    def wait_till_file_deleted(self, remotepath, filename, timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        deleted = False
        while time.time() < end_time and not deleted:
            #get the process list
            exists = self.file_exists(remotepath, filename)
            if exists:
                log.error('file still exists : {0}/{1}'.format(remotepath, filename))
                time.sleep(1)
            else:
                log.info('file does not exist anymore : {0}/{1}'.format(remotepath, filename))
                deleted = True
        return deleted

    def wait_till_file_added(self, remotepath, filename, timeout_in_seconds=180):
        end_time = time.time() + float(timeout_in_seconds)
        added = False
        while time.time() < end_time and not added:
            #get the process list
            exists = self.file_exists(remotepath, filename)
            if not exists:
                log.error('file does not exist : {0}/{1}'.format(remotepath, filename))
                time.sleep(1)
            else:
                log.info('file exists : {0}/{1}'.format(remotepath, filename))
                added = True
        return added

    def wait_till_compaction_end(self, rest, bucket, timeout_in_seconds=60):
        end_time = time.time() + float(timeout_in_seconds)
        ended = False
        while time.time() < end_time and not ended:
            status, vBucket = rest.check_compaction_status(bucket)
            if status:
                log.info("compacting vBucket {0}".format(vBucket))
                time.sleep(1)
                compacting, vBucket = rest.check_compaction_status(bucket)
                while compacting:
                    log.info("compacting vBucket {0}".format(vBucket))
                    time.sleep(1)
                    compacting, vBucket = rest.check_compaction_status(bucket)
                    if compacting and vBucket == 0:
                        ended = True
            else:
                log.error("auto compaction does not start yet.")
                time.sleep(1)
        return ended

    def terminate_processes(self,info,list):
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

    def couchbase_single_uninstall(self):
        linux_folders = ["/opt/couchbase"]
        terminate_process_list = ["beam", "couchdb"]

        info = self.extract_remote_info()
        log.info(info.distribution_type)
        type = info.distribution_type.lower()
        if type == 'windows':
            if info.architecture_type == "x86_64":
                exists = self.file_exists("/cygdrive/c/Program Files (x86)/Couchbase/Server/", 'VERSION.txt')
            elif info.architecture_type == "x86":
                exists = self.file_exists("/cygdrive/c/Program Files/Couchbase/Server/", 'VERSION.txt')
            log.info("exists ? {0}".format(exists))
            if exists:
                uninstall_init_command = "echo '[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-DlgOrder]\r\n\
Dlg0={95137A8D-0896-4BB8-85B3-9D7723BA6811}-MessageBox-0\r\n\
Count=2\r\n\
Dlg1={95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdFinish-0\r\n\
[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-MessageBox-0]\r\n\
Result=6\r\n\
[{95137A8D-0896-4BB8-85B3-9D7723BA6811}-SdFinish-0]\r\n\
Result=1\r\n\
bOpt1=0\r\n\
bOpt2=0' > /cygdrive/c/automation/css_win2k8_64_uninstall.iss"
                output, error = self.execute_command(uninstall_init_command)
                self.log_command_output(output, error)
                uninstall_command = "echo 'c:\\automation\\setup.exe /s -f1c:\\automation\\css_win2k8_64_uninstall.iss' > /cygdrive/c/automation/uninstall.bat"
                self.log_command_output(output, error)
                output, error = self.execute_command(uninstall_command)
                self.log_command_output(output, error)
                win_processes = ["msiexec32.exe", "msiexec.exe", "setup.exe", "ISBEW64.*",
                                 "WerFault.*"]
                self.terminate_processes(info, win_processes)
                self.remove_folders(["/cygdrive/c/Program Files (x86)/Couchbase/Server/"])
                output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
                self.log_command_output(output, error)
                self.wait_till_file_deleted("/cygdrive/c/Program Files (x86)/Couchbase/Server/", 'VERSION.txt',
                                            timeout_in_seconds=120)
                log.info("Wait for 30 seconds before doing something else")
                time.sleep(30)
            else:
                log.info("No couchbase single server on this server.  Free to install")
        elif type in ["ubuntu", "centos", "red hat"]:
            #uninstallation command is different
            if type == "ubuntu":
                uninstall_cmd = "dpkg -r {0};dpkg --purge {1};".format("couchbase-server", "couchbase-server")
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type == "centos":
                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server", "couchbase-server")
                log.info('running rpm -e to remove couchbase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type == "red hat":
                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server", "couchbase-server")
                log.info('running rpm -e to remove couchbase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            self.terminate_processes(info, terminate_process_list)
            self.remove_folders(linux_folders)

    def couchbase_uninstall(self, product):
        linux_folders = ["/var/opt/membase", "/opt/membase", "/etc/opt/membase",
                         "/var/membase/data/*", "/opt/membase/var/lib/membase/*",
                         "/opt/couchbase"]
        terminate_process_list = ["beam.smp", "memcached", "moxi", "vbucketmigrator",
                                  "couchdb", "epmd", "memsup", "cpu_sup"]
        version_file = "VERSION.txt"
        info = self.extract_remote_info()
        log.info(info.distribution_type)
        type = info.distribution_type.lower()
        if type == 'windows':
            query = BuildQuery()
            builds, changes = query.get_all_builds()
            os_type = "exe"
            task = "uninstall"
            bat_file = "uninstall.bat"

            if product == "cb":
                product_name = "couchbase-server-enterprise"
                version_path = "/cygdrive/c/Program Files/Couchbase/Server/"

            exist = self.file_exists(version_path, version_file)
            log.info("Is VERSION file existed? {0}".format(exist))
            if exist:
                log.info("VERSION file exists.  Start to uninstall {0} server".format(product))
                build_name, short_version, full_version = self.find_build_version(version_path, version_file)
                log.info('Build name: {0}'.format(build_name))
                build_name = build_name.rstrip() + ".exe"
                log.info('Check if {0} is in tmp directory'.format(build_name))
                exist = self.file_exists("/cygdrive/c/tmp/", build_name)
                if not exist:   # if not exist in tmp dir, start to download that verion build
                    build = query.find_build(builds, product_name, os_type, info.architecture_type, full_version)
                    downloaded = self.download_binary_in_win(build.url, product, short_version)
                    if downloaded:
                        log.info('Successful download {0}_{1}.exe'.format(product, short_version))
                    else:
                        log.error('Download {0}_{1}.exe failed'.format(product, short_version))
                dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
                self.create_multiple_dir(dir_paths)
                self.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
                # modify bat file to run uninstall schedule task
                self.modify_bat_file('/cygdrive/c/automation', bat_file,
                                       product, info.architecture_type, info.windows_name, short_version, task)
                log.info('sleep for 5 seconds before running task schedule uninstall')
                time.sleep(5)
                # run schedule task uninstall couchbase server
                output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
                self.log_command_output(output, error)
                self.wait_till_file_deleted(version_path, version_file, timeout_in_seconds=600)
                log.info('sleep 30 seconds before running the next job ...')
                time.sleep(30)
            else:
                log.info('No {0} on this server'.format(product_name))
        elif type in ["ubuntu", "centos", "red hat"]:
            #uninstallation command is different
            if type == "ubuntu":
                uninstall_cmd = "dpkg -r {0};dpkg --purge {1};".format("couchbase-server", "couchbase-server")
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type == "centos":
                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server", "couchbase-server")
                log.info('running rpm -e to remove couchbase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type == "red hat":
                uninstall_cmd = 'rpm -e {0}'.format("couchbase-server", "couchbase-server")
                log.info('running rpm -e to remove couchbase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            self.terminate_processes(info, terminate_process_list)
            self.remove_folders(linux_folders)

    def couchbase_win_uninstall(self, product, version, os_name, query):
        builds, changes = query.get_all_builds()
        version_file = 'VERSION.txt'
        bat_file = "uninstall.bat"
        task = "uninstall"

        info = self.extract_remote_info()
        ex_type = info.deliverable_type
        if info.architecture_type == "x86_64":
            os_type = "64"
        elif info.architecture_type == "x86":
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
            if not exist:   # if not exist in tmp dir, start to download that verion build
                build = query.find_build(builds, name, ex_type, info.architecture_type, rm_version)
                downloaded = self.download_binary_in_win(build.url, product, rm_version)
                if downloaded:
                    log.info('Successful download {0}_{1}.exe'.format(product, rm_version))
                else:
                    log.error('Download {0}_{1}.exe failed'.format(product, rm_version))
            # copy required files to automation directory
            dir_paths = ['/cygdrive/c/automation', '/cygdrive/c/tmp']
            self.create_multiple_dir(dir_paths)
            self.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
            # modify bat file to run uninstall schedule task
            self.modify_bat_file('/cygdrive/c/automation', bat_file,
                                       product, os_type, os_name, rm_version, task)
            log.info('sleep for 5 seconds before running task schedule uninstall')
            time.sleep(5)
            # run schedule task uninstall couchbase server
            output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
            self.log_command_output(output, error)
            self.wait_till_file_deleted(version_path, version_file, timeout_in_seconds=600)
            log.info('sleep 15 seconds before running the next job ...')
            time.sleep(15)
        else:
            log.info('No couchbase server on this server')

    def membase_uninstall(self):
        linux_folders = ["/var/opt/membase", "/opt/membase", "/etc/opt/membase",
                         "/var/membase/data/*", "/opt/membase/var/lib/membase/*"]
        terminate_process_list = ["beam", "memcached", "moxi", "vbucketmigrator",
                                  "couchdb", "epmd"]
        info = self.extract_remote_info()
        log.info(info.distribution_type)
        type = info.distribution_type.lower()
        if type == 'windows':
            exists = self.file_exists("/cygdrive/c/Program Files/Membase/Server/", 'VERSION.txt')
            log.info("exists ? {0}".format(exists))
            if exists:
                log.info("start to uninstall membase server")
                install_command = "echo 'c:\\automation\\setup.exe /s -f1c:\\automation\\win2k8_64_install.iss' > \
                                   /cygdrive/c/automation/install.bat"
                output, error = self.execute_command(install_command)
                uninstall_command = "echo 'c:\\automation\\setup.exe /s -f1c:\\automation\\win2k8_64_uninstall.iss' > \
                                    /cygdrive/c/automation/uninstall.bat"
                self.log_command_output(output, error)
                output, error = self.execute_command(uninstall_command)
                self.log_command_output(output, error)
                win_processes = ["msiexec32.exe", "msiexec32.exe", "setup.exe", "ISBEW64.*",
                                 "firefox.*", "WerFault.*", "iexplore.*"]
                self.terminate_processes(info, win_processes)
                self.remove_folders([" /cygdrive/c/Program Files/Membase/Server/"])
                output, error = self.execute_command("cmd /c schtasks /run /tn removeme")
                self.log_command_output(output, error)
                self.wait_till_file_deleted("/cygdrive/c/Program Files/Membase/Server/", 'VERSION.txt',
                                            timeout_in_seconds=120)
                time.sleep(60)
            else:
                log.info("No membase server on this server.  Free to install")
        elif type in ["ubuntu", "centos", "red hat"]:
            #uninstallation command is different
            if type == "ubuntu":
                uninstall_cmd = 'dpkg -r {0};dpkg --purge {1};'.format('membase-server', 'membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type == "centos":
                uninstall_cmd = 'rpm -e {0}'.format('membase-server')
                log.info('running rpm -e to remove membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            elif type == "red hat":
                uninstall_cmd = 'rpm -e {0}'.format('membase-server')
                log.info('running rpm -e to remove membase-server')
                output, error = self.execute_command(uninstall_cmd)
                self.log_command_output(output, error)
            self.terminate_processes(info, terminate_process_list)
            self.remove_folders(linux_folders)

    def log_command_output(self, output, error):
        for line in error:
            log.error(line)
        for line in output:
            log.info(line)

    def execute_command(self, command, debug=True):
        if debug:
            log.info("running command  {0}".format(command))
        stdin, stdout, stderro = self._ssh_client.exec_command(command)
        stdin.close()
        output = []
        error = []
        for line in stdout.read().splitlines():
            output.append(line)
        for line in stderro.read().splitlines():
            error.append(line)
        if debug:
            log.info('command executed successfully')
        stdout.close()
        stderro.close()
        return output, error

    def terminate_process(self, info=None, process_name=''):
        if info is None:
            info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            o, r = self.execute_command("taskkill /F /T /IM {0}*".format(process_name))
            self.log_command_output(o, r)
        else:
            o, r = self.execute_command("killall -9 {0}".format(process_name))
            self.log_command_output(o, r)

    def disconnect(self):
        self._ssh_client.close()

    def extract_remote_info(self):
        #use ssh to extract remote machine info
        #use sftp to if certain types exists or not
        sftp = self._ssh_client.open_sftp()
        filenames = sftp.listdir('/etc/')
        os_distro = ""
        os_version = ""
        is_linux_distro = False
        for name in filenames:
            if name == 'issue':
                #it's a linux_distro . let's downlaod this file
                #format Ubuntu 10.04 LTS \n \l
                filename = 'etc-issue-{0}'.format(uuid.uuid4())
                sftp.get(localpath=filename, remotepath='/etc/issue')
                file = open(filename)
                etc_issue = ''
                #let's only read the first line
                for line in file.xreadlines():
                    etc_issue = line
                    break
                    #strip all extra characters
                etc_issue = etc_issue.rstrip('\n').rstrip('\\l').rstrip('\\n')
                if etc_issue.lower().find('ubuntu') != -1:
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
            #let's run 'systeminfo grep 'System Type:'
            system_type_response, error = self.execute_command("systeminfo | grep 'System Type:'")

            if system_type_response and system_type_response[0].find('x64') != -1:
                arch = 'x86_64'
            else:
                arch = 'x86'
            os_name_response, error = self.execute_command("systeminfo | grep 'OS Name: '")
            os_name_type = os_name_response[0].split(" ")
            for name in os_name_type:
                if name == "2008":
                    windows_name = "2k8"
                    break
                elif name == "7":
                    windows_name = "7"
                    break
            if os_name_response:
                log.info(os_name_response)
                first_item = os_name_response[0]
                if os_name_response[0].find('Microsoft') != -1:
                    os_version = first_item[first_item.index('Microsoft'):]
                #let's run 'systeminfo grep 'OS Name: '
            info = RemoteMachineInfo()
            info.type = "Windows"
            info.windows_name = windows_name
            info.distribution_type = "Windows"
            info.architecture_type = arch
            info.ip = self.ip
            info.distribution_version = os_version
            info.deliverable_type = 'exe'
            return info
        else:
            #now run uname -m to get the architechtre type
            stdin, stdout, stderro = self._ssh_client.exec_command('uname -m')
            stdin.close()
            os_arch = ''
            for line in stdout.read().splitlines():
                os_arch += line
                # at this point we should know if its a linux or windows ditro
            ext = {'Ubuntu': 'deb', 'CentOS': 'rpm', 'Red Hat': 'rpm'}.get(os_distro, '')
            arch = {'i686': 'x86', 'i386': 'x86'}.get(os_arch, os_arch)
            info = RemoteMachineInfo()
            info.type = "Linux"
            info.distribution_type = os_distro
            info.architecture_type = arch
            info.ip = self.ip
            info.distribution_version = os_version
            info.deliverable_type = ext
            return info

    def stop_couchbase(self):
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            o, r = self.execute_command("net stop couchbaseserver")
            self.log_command_output(o, r)
        if info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/couchbase-server stop")
            self.log_command_output(o, r)

    def start_couchbase(self):
        info = self.extract_remote_info()
        if info.type.lower() == 'windows':
            o, r = self.execute_command("net start couchbaseserver")
            self.log_command_output(o, r)
        if info.type.lower() == "linux":
            o, r = self.execute_command("/etc/init.d/couchbase-server start")
            self.log_command_output(o, r)
