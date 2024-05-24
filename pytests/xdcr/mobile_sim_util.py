from lib.remote.remote_util import RemoteMachineShellConnection

class ServerInfo:
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 ssh_key=''):
        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key

class MobileSim:
    MobileSim_URL = "https://github.com/couchbaselabs/MobileImportSim.git"
    INSTALL_DIR = "/tmp"
    MobileSim_PATH = INSTALL_DIR + "/MobileImportSim"
    MobileSim_EXEC = MobileSim_PATH + "/mobileImportSim"

    def __init__(self, slave_ip='127.0.0.1'):
        self.slave_host = ServerInfo(slave_ip, 22, 'root', 'couchbase')
        self.install_mobile_sim()

    def install_mobile_sim(self):
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command("rm -rf " + MobileSim.MobileSim_PATH)
        output, error = shell.execute_command("cd {0};"
                                              "git clone {1}"
                                              .format(MobileSim.INSTALL_DIR,
                                                      MobileSim.MobileSim_URL))
        shell.log_command_output(output, error)
        output, error = shell.execute_command("cd {0};"
                                              "make clean;"
                                              "make deps;"
                                              "make"
                                              .format(MobileSim.MobileSim_PATH))
        shell.log_command_output(output, error)
        shell.disconnect()

    def uninstall_mobile_sim(self):
        shell = RemoteMachineShellConnection(self.slave_host)
        output, error = shell.execute_command("rm -rf " + MobileSim.MobileSim_PATH)
        shell.log_command_output(output, error)
        shell.disconnect()

    def run_simulator(self, hostAddr, username, password, bucketname,
                      bucketBufferCapacity=None, bucketOpTimeout=None,
                      dcpHandlerChanSize=None, debugMode=None):

        shell = RemoteMachineShellConnection(self.slave_host)
        execute_cmd = MobileSim.MobileSim_EXEC
        execute_cmd += " -hostAddr " + hostAddr + \
                       " -username " + username + \
                       " -password " + password + \
                       " -bucketname " + bucketname
        if bucketBufferCapacity:
            execute_cmd += " -bucketBufferCapacity " + bucketBufferCapacity
        if bucketOpTimeout:
            execute_cmd += " -bucketOpTimeout " + bucketOpTimeout
        if dcpHandlerChanSize:
            execute_cmd += " -dcpHandlerChanSize " + dcpHandlerChanSize
        if debugMode:
            execute_cmd += " -debugMode " + debugMode
        execute_cmd += " > /dev/null 2>&1 &"
        output, error = shell.execute_command(execute_cmd)
        shell.log_command_output(output, error)
        #get pid
        execute_cmd = "ps -ef | grep mobileImportSim | grep {} | grep {} | grep -v grep | awk '{{print $2}}'". \
                      format(hostAddr, bucketname)
        output, error = shell.execute_command(execute_cmd)
        pid = output[0]
        shell.disconnect()
        return pid

    def kill_simulator(self, pid):

        shell = RemoteMachineShellConnection(self.slave_host)
        execute_cmd = "kill -9 {}".format(pid)
        output, error = shell.execute_command(execute_cmd)
        shell.log_command_output(output, error)
        shell.disconnect()






