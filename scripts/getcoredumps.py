import sys
import os
import base64


sys.path.append('.')
sys.path.append('lib')
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputParser

if __name__ == "__main__":
    input = TestInputParser.get_test_input(sys.argv)
    for serverInfo in input.servers:
        try:
            remote = RemoteMachineShellConnection(serverInfo)
            info = remote.extract_remote_info()
            if info.type.lower() != 'windows':
                core_files = []
                core_files.extend(remote.file_starts_with("/opt/membase/var/lib/membase/", "erl_crash"))
                core_files.extend(remote.file_starts_with("/opt/membase/var/lib/membase/", "core"))
                core_files.extend(remote.file_starts_with("/tmp/", "core"))
                i = 0
                for core_file in core_files:
                    if core_file.startswith('erl'):
                        #let's just copy that file back
                        erl_crash_file_name = "erlang-{0}-{1}.log".format(serverInfo.ip, i)
                        destination = "{0}/{1}".format(os.getcwd(), erl_crash_file_name)
                        if remote.get_file(remotepath="/opt/membase/var/lib/membase/",
                                           filename=erl_crash_file_name,
                                           todir=destination):
                            print 'downloaded core file : {0}'.format(destination)
                        i += 1
                    else:
                        command = "/opt/membase/bin/analyze_core"
                        core_file_name = "core-{0}-{1}.log".format(serverInfo.ip, i)
                        core_log_output = "/{0}/{1}".format("tmp", core_file_name)
                        output, error = remote.execute_command('{0} {1} -f {2}'.format(command, core_file, core_log_output))
                        print output
                        destination = "{0}/{1}".format(os.getcwd(), core_file_name)
                        if remote.get_file(remotepath="/tmp", filename=core_file_name, todir=destination):
                            print 'downloaded core file : {0}'.format(destination)
                        i += 1
                remote.execute_command('mkdir -p /tmp/backup;mv -f /tmp/core* /tmp/backup/')
        except Exception as ex:
            print ex
            #ignoring all exceptions