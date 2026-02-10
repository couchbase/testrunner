#!/usr/bin/env python
import getopt
import sys
import os
import time
from threading import Thread
from datetime import datetime
import subprocess
import platform

sys.path = [".", "lib"] + sys.path
from testconstants import WIN_COUCHBASE_BIN_PATH_RAW
import TestInput


def usage(error=None):
    print("""\
Syntax: collect_server_info.py [options]

Options
 -l  Lower case of L. For local run on windows only.  No need ini file.
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 path=<file_path> The destination path you want to put your zipped diag file

Example:
 collect_server_info.py -i cluster.ini -p path=/tmp/nosql
""")
    sys.exit(error)

def time_stamp():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    hour = now.timetuple().tm_hour
    min = now.timetuple().tm_min
    date_time = "%s%02d%02d-%02d%02d" % (year, month, day, hour, min)
    return date_time


class couch_dbinfo_Runner(object):
    def __init__(self, server, path, local=False):
        self.server = server
        self.path = path
        self.local = local

    def run(self):
        file_name = "%s-%s-couch-dbinfo.txt" % (self.server.ip.replace('[', '').replace(']', '').replace(':', '.'),
                                                time_stamp())
        if not self.local:
            from lib.remote.remote_util import RemoteMachineShellConnection
            remote_client = RemoteMachineShellConnection(self.server)
            print("Collecting dbinfo from %s\n" % self.server.ip)
            output, error = remote_client.execute_couch_dbinfo(file_name)
            print("\n".join(output))
            print("\n".join(error))

            user_path = "/home/"
            if remote_client.info.distribution_type.lower() == 'mac':
                user_path = "/Users/"
            else:
                if self.server.ssh_username == "root":
                    user_path = "/"

            remote_path = "%s%s" % (user_path, self.server.ssh_username)
            status = remote_client.file_exists(remote_path, file_name)
            if not status:
                raise Exception("%s doesn't exists on server" % file_name)
            status = remote_client.get_file(remote_path, file_name,
                                        "%s/%s" % (self.path, file_name))
            if status:
                print("Downloading dbinfo logs from %s" % self.server.ip)
            else:
                raise Exception("Fail to download db logs from %s"
                                                     % self.server.ip)
            remote_client.execute_command("rm -f %s" % os.path.join(remote_path, file_name))
            remote_client.disconnect()


class cbcollectRunner(object):
    def __init__(self, server, path, local=False):
        self.server = server
        self.path = path
        self.local = local
        self.succ = []
        self.fail = []

    def _run(self, server):
        try:
            file_name = "%s-%s-diag.zip" % (server.ip.replace('[', '').replace(']', '').replace(':', '.'),
                                        time_stamp())
            if not self.local:
                from lib.remote.remote_util import RemoteMachineShellConnection
                remote_client = RemoteMachineShellConnection(server)
                print("Collecting logs from %s\n" % server.ip)
                output, error = remote_client.execute_cbcollect_info(file_name)
                print("\n".join(error))

                user_path = "/home/"
                if remote_client.info.distribution_type.lower() == 'mac':
                    user_path = "/Users/"
                else:
                    if server.ssh_username == "root":
                        user_path = "/"

                remote_path = "%s%s" % (user_path, server.ssh_username)
                status = remote_client.file_exists(remote_path, file_name)
                if not status:
                    raise Exception("%s doesn't exists on server" % file_name)
                status = remote_client.get_file(remote_path, file_name,
                                            "%s/%s" % (self.path, file_name))
                if status:
                    print("Downloading zipped logs from %s" % server.ip)
                else:
                    raise Exception("Fail to download zipped logs from %s"
                                                        % server.ip)
                remote_client.execute_command("rm -f %s" % os.path.join(remote_path, file_name))
                remote_client.disconnect()
        except Exception as e:
            self.fail.append((server, e))
        else:
            self.succ.append(server)


    def run(self):
        if isinstance(self.server, list):
            cbcollect_threads = []
            timeout = time.time() + (60 * 60)
            for server in self.server:
                cbcollect_thread = Thread(target=self._run, args=(server,), daemon=True)
                cbcollect_thread.start()
                cbcollect_threads.append(cbcollect_thread)
            for [i, cbcollect_thread] in enumerate(cbcollect_threads):
                thread_timeout = timeout - time.time()
                if thread_timeout < 0:
                    thread_timeout = 0
                cbcollect_thread.join(timeout=thread_timeout)
                if cbcollect_thread.is_alive():
                    self.fail.append((self.server[i], Exception("timeout")))
        else:
            self._run(self.server)

class memInfoRunner(object):
    def __init__(self, server, local=False):
        self.server = server
        self.local = local
        self.succ = {}
        self.fail = []

    def _run(self, server):
        mem = None
        try:
            if not self.local:
                from lib.remote.remote_util import RemoteMachineShellConnection
                remote_client = RemoteMachineShellConnection(server)
                print("Collecting memory info from %s\n" % server.ip)
                remote_cmd = "sh -c 'case \"$OSTYPE\" in darwin*) sysctl hw.memsize|grep -Eo [0-9]+ ;; *) grep MemTotal /proc/meminfo|grep -Eo [0-9]+ ;; esac'"
                output, error = remote_client.execute_command(remote_cmd)
                print("\n".join(error))
                remote_client.disconnect()
                mem = int("".join(output))
        except Exception as e:
            self.fail.append((server.ip, e))
        else:
            if mem:
                self.succ[server.ip] = mem
            else:
                self.fail.append((server.ip, Exception("mem parse failed")))

    def run(self):
        if isinstance(self.server, list):
            meminfo_threads = []
            for server in self.server:
                meminfo_thread = Thread(target=self._run, args=(server,))
                meminfo_thread.start()
                meminfo_threads.append(meminfo_thread)
            for meminfo_thread in meminfo_threads:
                meminfo_thread.join()
        else:
            self._run(self.server)

def main():
    local = False
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hli:p', [])
        for o, a in opts:
            if o == "-h":
                usage()
            elif o == "-l":
                if platform.system() == "Windows":
                    print("*** windows os ***")
                    local = True
                else:
                    print("This option '-l' only works for local windows.")
                    sys.exit()
        if not local:
            input = TestInput.TestInputParser.get_test_input(sys.argv)
            if not input.servers:
                usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    if not local:
        file_path = input.param("path", ".")
        runner = cbcollectRunner(input.servers, file_path)
        runner.run()
        if len(runner.fail) > 0:
            for (server, e) in runner.fail:
                print("NOT POSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(server.ip, str(e)))
            exit(1)
    else:
        file_name = "%s-%s-diag.zip" % ("local", time_stamp())
        cbcollect_command = WIN_COUCHBASE_BIN_PATH_RAW + "cbcollect_info.exe"
        result = subprocess.check_call([cbcollect_command, file_name])
        if result == 0:
            print("Log file name is \n %s" % file_name)
        else:
            print("Failed to collect log")

if __name__ == "__main__":
    main()
