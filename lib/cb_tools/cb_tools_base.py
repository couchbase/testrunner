from TestInput import TestInputSingleton
from lib.Cb_constants.CBServer import CbServer
from lib.testconstants import \
    LINUX_COUCHBASE_BIN_PATH, LINUX_NONROOT_CB_BIN_PATH, \
    WIN_COUCHBASE_BIN_PATH, MAC_COUCHBASE_BIN_PATH
import os


class CbCmdBase:
    def __init__(self, shell_conn, binary_name):
        self.shellConn = shell_conn
        self.port = shell_conn.port
        self.mc_port = CbServer.memcached_port
        if int(shell_conn.port) in range(9000,
                                         9000 + 10):
            multiplier = 2 * (int(shell_conn.port) - 9000)
            self.mc_port = CbServer.cr_memcached_port + multiplier
        self.username = TestInputSingleton.input.servers[0].rest_username
        self.password = TestInputSingleton.input.servers[0].rest_password
        self.binaryName = binary_name
        self.cbstatCmd = "%s%s" % (LINUX_COUCHBASE_BIN_PATH, self.binaryName)
        if int(shell_conn.port) in range(9000,
                                         9000 + 10):
            # Cluster run case
            # self.cbstatCmd = os.path.join(TestInputSingleton.input.servers[0].cli_path,
            #                               "build", "kv_engine", self.binaryName)
            #TODo take this input from ini file for cluster_run
            self.cbstatCmd = "../install/bin/" + \
                             self.binaryName
        elif self.shellConn.extract_remote_info().type.lower() == 'windows':
            # Windows case
            self.cbstatCmd = "%s%s.exe" % (WIN_COUCHBASE_BIN_PATH,
                                           self.binaryName)
        elif self.shellConn.extract_remote_info().type.lower() == 'mac':
            # MacOS case
            self.cbstatCmd = "%s%s" % (MAC_COUCHBASE_BIN_PATH,
                                       self.binaryName)
        elif self.shellConn.username != "root":
            # Linux non-root case
            self.cbstatCmd = "%s%s" % (LINUX_NONROOT_CB_BIN_PATH,
                                       self.binaryName)

    def _execute_cmd(self, cmd):
        """
        Executed the given command in the target shell
        Arguments:
        :cmd - Command to execute
        Returns:
        :output - Output for the command execution
        :error  - Buffer containing warnings/errors from the execution
        """
        return self.shellConn.execute_command(cmd)
