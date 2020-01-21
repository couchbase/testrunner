#!/usr/bin/env python
import getopt
import sys
import os
import time
from threading import Thread

sys.path.append('.')
sys.path.append('lib')
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputParser


def usage(error=None):
    print("""\
Syntax: getcoredumps.py [options]

Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 path=<file_path> The destination path you want to put your zipped diag file

Example:
 getcoredumps.py -i cluster.ini -p path=/tmp/nosql
""")
    sys.exit(error)


class Getcoredumps(object):
    def __init__(self, server, path):
        self.server = server
        self.path = path

    def run(self):
        remote = RemoteMachineShellConnection(self.server)
        server_type = 'membase'
        if remote.is_couchbase_installed():
            server_type = 'couchbase'
        stamp = time.strftime("%d_%m_%Y_%H_%M")
        try:
            info = remote.extract_remote_info()
            if info.type.lower() != 'windows':
                core_files = []
                print("looking for crashes on {0} ... ".format(info.ip))
                print("erl_crash files under /opt/{0}/var/lib/{0}/".format(server_type))
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "erl_crash"))
                print("core* files under /opt/{0}/var/lib/{0}/".format(server_type))
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "core"))
                print("core* files under /tmp/")
                core_files.extend(remote.file_starts_with("/tmp/", "core"))
                print("breakpad *dmp files under /opt/{0}/var/lib/{0}/".format(server_type))
                core_files.extend(remote.file_ends_with("/opt/{0}/var/lib/{0}/".format(server_type), ".dmp"))
                if core_files:
                    print("found crashes on {0}: {1}".format(info.ip, core_files))
                else:
                    print("crashes not found on {0}".format(info.ip))
                i = 0
                for core_file in core_files:
                    if core_file.find('erl_crash.dump') != -1:
                        #let's just copy that file back
                        erl_crash_file_name = "erlang-{0}-{1}.log".format(self.server.ip, i)
                        remote_path, file_name = os.path.dirname(core_file), os.path.basename(core_file)
                        if remote.get_file(remote_path, file_name, os.path.join(self.path, erl_crash_file_name)):
                            print('downloaded core file : {0}'.format(core_file))
                            i += 1
                    elif core_file.find('.dmp') != -1:
                        breakpad_crash_file_name = "breakpad-{0}-{1}.dmp".format(self.server.ip, i)
                        remote_path, file_name = os.path.dirname(core_file), os.path.basename(core_file)
                        if remote.get_file(remote_path, file_name, os.path.join(self.path, breakpad_crash_file_name)):
                            print('downloaded breakpad .dmp file : {0}'.format(core_file))
                            i += 1
                    else:
                        command = "/opt/{0}/bin/tools/cbanalyze-core".format(server_type)
                        core_file_name = "core-{0}-{1}.log".format(self.server.ip, i)
                        core_log_output = "/tmp/{0}".format(core_file_name)
                        output, _ = remote.execute_command('{0} {1} -f {2}'.format(command, core_file, core_log_output))
                        print(output)
                        remote_path, file_name = os.path.dirname(core_log_output), os.path.basename(core_log_output)
                        if remote.get_file(remote_path, file_name, os.path.join(self.path, core_file_name)):
                            print('downloaded core backtrace : {0}'.format(core_log_output))
                            i += 1
                if i > 0:
                    command = "mkdir -p /tmp/backup_crash/{0};" \
                              "mv -f /tmp/core* /tmp/backup_crash/{0};" \
                              "mv -f /opt/{1}/var/lib/{1}/erl_crash.dump* /tmp/backup_crash/{0}; " \
                              "mv -f /opt/{1}/var/lib/{1}/*.dmp /tmp/backup_crash/{0};" \
                              "mv -f /opt/{1}/var/lib/{1}/crash/*.dmp /tmp/backup_crash/{0};".\
                        format(stamp, server_type)
                    print("put all crashes on {0} in backup folder: /tmp/backup_crash/{1}".format(self.server.ip, stamp))
                    remote.execute_command(command)
                    output, error = remote.execute_command("ls -la /tmp/backup_crash/{0}".format(stamp))
                    for o in output:
                        print(o)
                    remote.disconnect()
                    return True
                if remote:
                    remote.disconnect()
                return False
        except Exception as ex:
            print(ex)
            return False


class Clearcoredumps(object):
    def __init__(self, server, path):
        self.server = server
        self.path = path

    def run(self):
        remote = RemoteMachineShellConnection(self.server)
        server_type = 'membase'
        if remote.is_couchbase_installed():
            server_type = 'couchbase'
        stamp = time.strftime("%d_%m_%Y_%H_%M")
        try:
            info = remote.extract_remote_info()
            if info.type.lower() != 'windows':
                core_files = []
                print("looking for Erlang/Memcached crashes on {0} ... ".format(info.ip))
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "erl_crash"))
                core_files.extend(remote.file_starts_with("/opt/{0}/var/lib/{0}/".format(server_type), "core"))
                core_files.extend(remote.file_starts_with("/tmp/", "core"))
                core_files.extend(remote.file_ends_with("/opt/{0}/var/lib/{0}/crash".format(server_type), ".dmp"))
                if core_files:
                    print("found dumps on {0}: {1}".format(info.ip, core_files))
                    command = "mkdir -p /tmp/backup_crash/{0};" \
                              "mv -f /tmp/core* /tmp/backup_crash/{0};" \
                              "mv -f /opt/{1}/var/lib/{1}/erl_crash.dump* /tmp/backup_crash/{0}; " \
                              "mv -f /opt/{1}/var/lib/{1}/crash/*.dmp /tmp/backup_crash/{0};".\
                        format(stamp, server_type)
                    print("Moved all dumps on {0} to backup folder: /tmp/backup_crash/{1}".format(self.server.ip, stamp))
                    remote.execute_command(command)
                    output, error = remote.execute_command("ls -la /tmp/backup_crash/{0}".format(stamp))
                    for o in output:
                        print(o)
                    for core_file in core_files:
                        remote_path, file_name = os.path.dirname(core_file), os.path.basename(core_file)
                        if remote.delete_file(remote_path, file_name):
                            print('deleted core file : {0}'.format(core_file))
                    remote.disconnect()
                else:
                    print("dump files not found on {0}".format(info.ip))
                    if remote:
                        remote.disconnect()
        except Exception as ex:
            print(ex)

def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p', [])
        for o, a in opts:
            if o == "-h":
                usage()

        input = TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    file_path = input.param("path", ".")
    remotes = (Getcoredumps(server, file_path) for server in input.servers)
    remote_threads = [Thread(target=remote.run) for remote in remotes]

    for remote_thread in remote_threads:
        remote_thread.daemon = True
        remote_thread.start()
        run_time = 0
        while remote_thread.isAlive() and run_time < 1200:
            time.sleep(15)
            run_time += 15
            print("Waiting for another 15 seconds (time-out after 20 min)")
        if run_time == 1200:
            print("collect core dumps hung on this node. Jumping to next node")
        print("collect core dumps info done")

    for remote_thread in remote_threads:
        remote_thread.join(120)
        if remote_thread.isAlive():
            raise Exception("collect core dumps hung on remote node")

if __name__ == "__main__":
    main()