from lib.remote.remote_util import RemoteMachineShellConnection


class ServerInfo():

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


class TestSSL:
    TestSSL_URL = "https://github.com/drwetter/testssl.sh.git"
    TEST_SSL_FILE_PATH = "/tmp/testssl.sh"
    TEST_SSL_FILENAME = "/tmp/testssl.sh/testssl.sh"
    SLAVE_HOST = ServerInfo('127.0.0.1', 22, 'root', 'couchbase')

    def __init__(self, path="/tmp"):
        self.path = path
        self.slave_host = TestSSL.SLAVE_HOST
        self.install_testssl()

    def install_testssl(self):
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command("rm -rf " + TestSSL.TEST_SSL_FILE_PATH)
        output, error = shell.execute_command("cd {0};"
                                              "git clone {1}"
                                              .format(self.path, TestSSL.TestSSL_URL))
        shell.log_command_output(output, error)
        shell.disconnect()

    def uninstall_testssl(self):
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command("rm -rf " + TestSSL.TEST_SSL_FILE_PATH)
        shell.disconnect()