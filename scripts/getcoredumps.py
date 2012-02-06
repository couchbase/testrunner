import sys
import os


sys.path.append('.')
sys.path.append('lib')
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputParser

if __name__ == "__main__":
    input = TestInputParser.get_test_input(sys.argv)
    remote = RemoteMachineShellConnection(input.servers[0])
    server_type = 'couchbase'
    if remote.is_membase_installed():
        server_type = 'membase'
    for serverInfo in input.servers:
        try:
            remote = RemoteMachineShellConnection(serverInfo)
            info = remote.extract_remote_info()
            if info.type.lower() != 'windows':
                core_files = []
                print "looking for erl_crash files under /opt/{0}/var/lib/{0}/".format(server_type)
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "erl_crash"))
                print "looking for core* files under /opt/{0}/var/lib/{0}/".format(server_type)
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "core"))
                print "looking for core* files under /tmp/"
                core_files.extend(remote.file_starts_with("/tmp/", "core"))
                i = 0
                for core_file in core_files:
                    if core_file.find('erl_crash.dump') != -1:
                        #let's just copy that file back
                        erl_crash_file_name = "erlang-{0}-{1}.log".format(serverInfo.ip, i)
                        erl_crash_path = "/opt/{0}/var/lib/{0}/{1}".format(server_type, erl_crash_file_name)
                        remote.execute_command('cp {0} {1}'.format(core_file, erl_crash_path))
                        destination = "{0}/{1}".format(os.getcwd(), erl_crash_file_name)
                        if remote.get_file(remotepath="/opt/{0}/var/lib/{0}/".format(server_type),
                                           filename=erl_crash_file_name,
                                           todir=destination):
                            print 'downloaded core file : {0}'.format(destination)
                            i += 1
                    else:
                        command = "/opt/{0}/bin/analyze_core".format(server_type)
                        core_file_name = "core-{0}-{1}.log".format(serverInfo.ip, i)
                        core_log_output = "/{0}/{1}".format("tmp", core_file_name)
                        output, error = remote.execute_command('{0} {1} -f {2}'.format(command, core_file, core_log_output))
                        print output
                        destination = "{0}/{1}".format(os.getcwd(), core_file_name)
                        if remote.get_file(remotepath="/tmp", filename=core_file_name, todir=destination):
                            print 'downloaded core file : {0}'.format(destination)
                            i += 1
                if i > 0:
                    remote.execute_command('mkdir -p /tmp/backup;mv -f /tmp/core* /tmp/backup/;')
                    remote.execute_command('mv -f /opt/{0}/var/lib/{0}/erl_crash.dump /tmp/backup'.format(server_type))
                    remote.disconnect()
                if remote:
                    remote.disconnect()
        except Exception as ex:
            print ex