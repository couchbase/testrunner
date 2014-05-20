import sys
import os
import time


sys.path.append('.')
sys.path.append('lib')
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputParser

if __name__ == "__main__":
    input = TestInputParser.get_test_input(sys.argv)
    remote = RemoteMachineShellConnection(input.servers[0])
    server_type = 'membase'
    if remote.is_couchbase_installed():
        server_type = 'couchbase'
    stamp = time.strftime("%d_%m_%Y_%H_%M")
    for serverInfo in input.servers:
        try:
            remote = RemoteMachineShellConnection(serverInfo)
            info = remote.extract_remote_info()
            if info.type.lower() != 'windows':
                core_files = []
                print "looking for crashes on {0} ... ".format(info.ip)
                print "erl_crash files under /opt/{0}/var/lib/{0}/".format(server_type)
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "erl_crash"))
                print "core* files under /opt/{0}/var/lib/{0}/".format(server_type)
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "core"))
                print "core* files under /tmp/"
                core_files.extend(remote.file_starts_with("/tmp/", "core"))
                if core_files:
                    print "found crashes on {0}: {1}".format(info.ip, core_files)
                else:
                    print "crashes not found on {0}".format(info.ip)
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
                        command = "/opt/{0}/bin/tools/cbanalyze-core".format(server_type)
                        core_file_name = "core-{0}-{1}.log".format(serverInfo.ip, i)
                        core_log_output = "/tmp/{0}".format(core_file_name)
                        output, error = remote.execute_command('{0} {1} -f {2}'.format(command, core_file, core_log_output))
                        print output
                        destination = "{0}/{1}".format(os.getcwd(), core_file_name)
                        if remote.get_file(remotepath="/tmp", filename=core_file_name, todir=destination):
                            print 'downloaded core file : {0}'.format(destination)
                            i += 1
                if i > 0:
                    command = "mkdir -p /tmp/backup_crash/{0};mv -f /tmp/core* /tmp/backup_crash/{0}; mv -f /opt/{0}/var/lib/{1}/erl_crash.dump* /tmp/backup_crash/{0}".\
                        format(stamp, server_type)
                    print "put all crashes on {0} in backup folder: /tmp/backup_crash/{1}".format(serverInfo.ip, stamp)
                    remote.execute_command(command)
                    output, error = remote.execute_command("ls -la /tmp/backup_crash/{0}".format(stamp))
                    for o in output:
                        print o
                    remote.disconnect()
                if remote:
                    remote.disconnect()
        except Exception as ex:
            print ex