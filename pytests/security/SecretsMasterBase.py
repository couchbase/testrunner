import json
import time
from threading import Thread, Event
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import ReadDocumentException
from membase.api.exception import DesignDocCreationException
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
import time
import subprocess
import logger
log = logger.Logger.get_logger()
import urllib.request, urllib.parse, urllib.error
import socket
import testconstants
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH_RAW
from testconstants import MAC_COUCHBASE_BIN_PATH
import random

class SecretsMasterBase():

    char_ascii_range_start = 97
    char_ascii_range_stop = 122
    int_ascii_range_start = 48
    int_ascii_range_stop = 57
    ext_ascii_range_start = 128
    ext_ascii_range_stop = 255
    WININSTALLPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/"
    LININSTALLPATH = "/opt/couchbase/var/lib/couchbase/"
    MACINSTALLPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/"

    def __init__(self,
                 host=None,
                 method='REST'):

        self.ipAddress = self.getLocalIPAddress()

    def _get_install_path(self,host):
        shell = RemoteMachineShellConnection(host)
        os_type = shell.extract_remote_info().distribution_type
        log.info ("OS type is {0}".format(os_type))
        if os_type == 'windows':
            install_path = SecretsMasterBase.WININSTALLPATH
        elif os_type == 'Mac':
            install_path = SecretsMasterBase.MACINSTALLPATH
        else:
            install_path = SecretsMasterBase.LININSTALLPATH

        return install_path

    def getLocalIPAddress(self):
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]
        '''
        status, ipAddress = subprocess.getstatusoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = subprocess.getstatusoutput("ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | awk '{print $2}'")
        return ipAddress

    def set_password(self,host,new_password):
        rest = RestConnection(host)
        status = self.execute_cli(host,new_password)
        log.info ("Status of set password command is - {0}".format(status))
    
    #Execute cli to set master password, and provide input
    def execute_cli(self,host,new_password=None):
        cmd = "/opt/couchbase/bin/couchbase-cli setting-master-password --new-password -c localhost -u Administrator -p password"
        shell = RemoteMachineShellConnection(host)
        chan = shell._ssh_client.invoke_shell()
        buff = ''
        while not buff.endswith('# '):
            resp = chan.recv(9999).decode('utf-8')
            buff += resp

        # ssh and wait for commmand prompt
        chan.send(cmd + "\n")
        time.sleep(5)
        resp1 = chan.recv(9999)
        chan.send(new_password + "\n")
        time.sleep(5)
        resp2= chan.recv(9999)
        chan.send(new_password + "\n")
        time.sleep(5)
        resp3= chan.recv(9999).decode('utf-8')
        if (resp3.find('SUCCESS: New master password set') > 0):
            return True
        else:
            return False

    #Execute cli to set master password and rotate-data-key
    def execute_cli_rotate_key(self,host,new_password=None):
        cmd = "/opt/couchbase/bin/couchbase-cli setting-master-password -c localhost:8091 -u Administrator -p password --rotate-data-key"
        shell = RemoteMachineShellConnection(host)
        chan = shell._ssh_client.invoke_shell()
        buff = ''
        while not buff.endswith('# '):
            resp = chan.recv(9999).decode('utf-8')
            buff += resp

        # ssh and wait for commmand prompt
        chan.send(cmd + "\n")
        time.sleep(5)
        resp = chan.recv(9999).decode('utf-8')

        if (resp.find('SUCCESS: Data key rotated') > 0):
            return True
        else:
            return False

    def get_log_dir(self,node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)


    def check_log_files(self,host,file_name,search_string):
        log_path = self.get_log_dir(host)
        shell = RemoteMachineShellConnection(host)
        debug_log = str(log_path) + '/' + file_name + "*"
        count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                           format(search_string, debug_log))
        if isinstance(count, list):
            count = int(count[0])
        else:
            count = int(count)
        shell.disconnect()
        if count > 0:
            log.info("===== PASSWORD {0} OBSERVED IN FILE {1} ON SERVER {2}=====".format(search_string,file_name,host))
            return True
        else:
            return False

    def check_config_files(self,host,file_path,file_name,search_string):
        final_file_name = file_path + file_name
        shell = RemoteMachineShellConnection(host)
        count, err = shell.execute_command("grep \"{0}\" {1} | wc -l".
                                           format(search_string, final_file_name))
        if isinstance(count, list):
            count = int(count[0])
        else:
            count = int(count)
        shell.disconnect()
        if count > 0:
            log.info(
                "===== PASSWORD {0} OBSERVED IN FILE {1} ON SERVER {2}=====".format(search_string, file_name, host))
            return False
        else:
            log.info(" PASSWORD WORD not found in config.dat file")
            return True

    def change_config_to_orginal(self,host,password):
        shell = RemoteMachineShellConnection(host)
        shell.execute_command("sed -i 's/export CB_MASTER_PASSWORD=*//' /opt/couchbase/bin/couchbase-server")
        #shell.execute_command("export CB_MASTER_PASSWORD="+ password + "; service couchbase-server restart")
        shell.execute_command("service couchbase-server restart")
        

    def restart_server_with_env(self,host,password):
        shell = RemoteMachineShellConnection(host)
        shell.execute_command("sed -i 's/export PATH/export PATH\\nexport CB_MASTER_PASSWORD='" + password + "'/' /opt/couchbase/bin/couchbase-server")
        #shell.execute_command("export CB_MASTER_PASSWORD="+ password + "; service couchbase-server restart")
        shell.execute_command("service couchbase-server restart")


    def correct_password_on_prompt(self,host,password,cmd):
        result = True
        shell = RemoteMachineShellConnection(host)
        chan = shell._ssh_client.invoke_shell()
        buff = ''
        while not buff.endswith('# '):
            resp = chan.recv(9999).decode('utf-8')
            buff += resp

        # ssh and wait for commmand prompt
        chan.send(cmd + "\n")

        buff = ""
        time.sleep(5)
        resp = chan.recv(9999).decode('utf-8')
        if (resp.find('Enter master password:') > 0):
            chan.send(password + "\n")

        time.sleep(5)
        resp = chan.recv(9999).decode('utf-8')
        log.info ("Response for password prompt - {0}".format(resp))
        if (resp.find('Password accepted. Node started booting.') > 0):
            log.info("node has started")
        else:
            result = False

        shell.disconnect()
        return result

    def incorrect_password(self,host,retries_number=3, incorrect_pass='abcd',input_correct_pass=False,\
                           correct_pass=False, cmd=None):
        result = True
        shell = RemoteMachineShellConnection(host)
        chan = shell._ssh_client.invoke_shell()
        buff = ''
        i=1
        while not buff.endswith('# '):
            resp = chan.recv(9999).decode('utf-8')
            buff += resp
        # ssh and wait for commmand prompt
        chan.send(cmd + "\n")
        time.sleep(5)
        resp = chan.recv(9999).decode('utf-8')
        if (resp.find('Enter master password:') > 0):
            while (i <= retries_number):
                log.info ("number of retries - {0}".format(i))
                if (input_correct_pass) and retries_number == i:
                    pass_string = correct_pass
                else:
                    pass_string = incorrect_pass
                chan.send(pass_string + "\n")
                time.sleep(5)
                resp = chan.recv(9999).decode('utf-8')
                log.info ("response from shell is  - {0}".format(resp))
                if (input_correct_pass) and retries_number == i:
                    if (resp.find("SUCCESS: Password accepted. Node started booting.") > 0):
                        log.info ("node has started after providing right password")
                        break
                    else:
                        log.info ("Password not accepted")
                        result = False
                else:
                    if resp.find("Incorrect password. Node shuts down.") and i == 3:
                        log.info ("node has finally shutdown after 3 retries")
                        break
                    else:
                        if (resp.find("Incorrect password") > 0):
                            log.info ("node in next retry, retry number - {0}".format(i))
                        else:
                            result = False
                i = i+1
        shell.disconnect()
        return result

    def start_server_prompt_diff_window(self,host):
        shell = RemoteMachineShellConnection(host)
        shell.execute_command("export CB_WAIT_FOR_MASTER_PASSWORD=true; service couchbase-server restart")
        shell.disconnect()


    def set_password_on_prompt(self,host,correct_master_password, retry=False, incorrect_master_password = 'incorrect', \
                               continue_with_correct=False):
        shell = RemoteMachineShellConnection(host)
        chan = shell.invoke_shell()
        buff = ''
        while not buff.endswith('# '):
            resp = chan.recv(9999).decode('utf-8')
            buff += resp
            # print(resp)

        # Ssh and wait for the password prompt.
        chan.send(cmd + '\n')

        buff = ''

        while not buff.endswith('Please enter master password:'):
            resp = chan.recv(9999).decode('utf-8')
            buff += resp

        # Send the password and wait for a prompt.

        time.sleep(10)
        if retry:
            chan.send(incorrect_master_password + '\n')
            buff = ''
            resp = chan.recv(9999).decode('utf-8')
            if (resp.find("Incorrect password") > 0):
                response = chan.send(incorrect_master_password + '\n')
                time.sleep(10)
                resp = chan.recv(9999).decode('utf-8')
                if (resp.find("Incorrect password") > 0):
                    log.info ("Incorrect Password from program for the second time")
                response = chan.send(incorrect_master_password + '\n')
                time.sleep(10)
                resp = chan.recv(9999)
                time.sleep(10)
                if (resp.find("Incorrect password. Node shuts down.") > 0):
                    log.info ("Finally the node shutdown after 3 retries")
        else:
            chan.send(correct_master_password + '\n')
            buff = ''
            resp = chan.recv(9999)
            if (resp.find('Password accepted. Node started booting.') > 0):
                log.info ("Node is started now")


        if (correct_master_password):
            chan.send(incorrect_master_password + '\n')
            time.sleep(10)
            resp = chan.recv(9999)
            if (resp.find("Incorrect password") > 0):
                log.info ("Incorrect password from the program for the first time")
            chan.send(correct_master_password + '\n')
            time.sleep(10)
            resp = chan.recv(9999)
            if (resp.find("Password accepted. Node started booting. ") > 0):
                log.info ("Node has started now")


    def generate_cb_collect(self,host,file_name,search_string):
        logs_folder = "/tmp/collectsecrets/"
        path = logs_folder + file_name
        shell = RemoteMachineShellConnection(host)
        shell.remove_folders([logs_folder])
        shell.create_directory(logs_folder)
        shell.execute_cbcollect_info(path)
        time.sleep(10)
        count, err = shell.execute_command("unzip {0} -d {1}".format(path,logs_folder))

        count, err = shell.execute_command("grep -r {0} {1} | wc -l".
                                           format(search_string, logs_folder))
        if (count[0] > '0'):
            return False
        else:
            return True



    def test_resetPass(self,host):
        shell = RemoteMachineShellConnection(host)
        info = shell.extract_remote_info()
        if info.type.lower() == "windows":
            command = "%scbreset_password.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH_RAW)
        else:
            command = "%scbreset_password" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        output, error = shell.execute_command(command)
        log.info ("Output of the command is  - {0}".format(command))


    def generate_password_simple(self, type, password_length, no_passwords):
        if type == 'char':
            start_range = self.char_ascii_range_start
            end_range = self.char_ascii_range_stop
        elif type == 'int':
            start_range = self.int_ascii_range_start
            end_range = self.int_ascii_range_stop
        elif type == "ext":
            start_range = self.ext_ascii_range_start
            end_range = self.ext_ascii_range_stop

        temp_char = ''
        pass_list = []
        for j in range(0, no_passwords):
            for i in range(0, password_length):
                temp_random_no = random.randint(start_range, end_range)
                temp_char += chr(temp_random_no)
            pass_list.append(temp_char)
            temp_char = ''
        return pass_list


    def generate_password_dual(self, type, password_length, no_passwords):
        if type == "char+int":
            first_start_range = self.char_ascii_range_start
            first_end_range = self.char_ascii_range_stop
            sec_start_range = self.int_ascii_range_start
            sec_stop_range = self.int_ascii_range_stop
        if type == "char+extended":
            first_start_range = self.char_ascii_range_start
            first_end_range = self.char_ascii_range_stop
            sec_start_range = self.ext_ascii_range_start
            sec_stop_range = self.ext_ascii_range_stop
        if type == "int+extended":
            first_start_range = self.int_ascii_range_start
            first_end_range = self.int_ascii_range_stop
            sec_start_range = self.ext_ascii_range_start
            sec_stop_range = self.ext_ascii_range_stop
        if type == "extended+char":
            first_start_range = self.ext_ascii_range_start
            first_end_range = self.ext_ascii_range_stop
            sec_start_range = self.char_ascii_range_start
            sec_stop_range = self.char_ascii_range_stop

        temp_char = ''
        pass_list = []
        for j in range(0, no_passwords):
            for i in range(0, password_length):
                if i % 2 == 0:
                    temp_random_no = random.randint(first_start_range, first_end_range)
                else:
                    temp_random_no = random.randint(sec_start_range, sec_stop_range)
                temp_char += chr(temp_random_no)
            pass_list.append(temp_char)
            temp_char = ''
        return pass_list


    def setup_pass_node(self,host,password="",startup_type='env'):
        shell = RemoteMachineShellConnection(host)
        self.set_password(host, password)
        if startup_type == 'env':
            self.restart_server_with_env(host, password)
        elif startup_type == 'prompt':
            shell.stop_server()
            self.start_server_prompt_diff_window(host)
            time.sleep(10)
            cmd = "/opt/couchbase/bin/couchbase-cli master-password --send-password"
            self.incorrect_password(host, cmd=cmd, retries_number=1, input_correct_pass=True, correct_pass=password)
        elif startup_type == 'simple':
            shell.stop_server()
            shell.start_server()
        shell.disconnect()
        time.sleep(30)


    def read_ns_config(self,host):
        shell = RemoteMachineShellConnection(host)
        output, error = shell.execute_command("/opt/couchbase/bin/cbdump-config /opt/couchbase/var/lib/couchbase/config/config.dat")
        self.rotate_data_key(host)
        change_output, change_error = shell.execute_command(
            "/opt/couchbase/bin/cbdump-config /opt/couchbase/var/lib/couchbase/config/config.dat")
        if (output[9] != change_output[9]):
            return True
        else:
            return False
