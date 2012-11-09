import sys
sys.path.append('lib')

import paramiko
from lib.remote.scp import SCPClient, SCPException

import TestInput


def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


def main():
    try:
        input = TestInput.TestInputParser.get_test_input(sys.argv)
    except AttributeError:
        print 'ERROR: no servers specified. Please use the -i parameter.'
    else:
        for server in input.servers:
            ssh = createSSHClient(server.ip, 22, server.ssh_username,
                                  server.ssh_password)
            scp = SCPClient(ssh.get_transport())
            try:
                scp.get('/tmp/*.atop')
            except SCPException:
                print 'There is no atop stats on {0}'.format(server.ip)
            else:
                print 'Collected atop raw data from {0}'.format(server.ip)

if __name__ == "__main__":
    main()
