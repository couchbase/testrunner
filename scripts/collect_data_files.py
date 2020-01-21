#!/usr/bin/env python
import getopt
import sys
import os
import time
from threading import Thread
from datetime import datetime

sys.path.extend(('.', 'lib'))
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.membase.api.rest_client import RestConnection
import TestInput


def usage(error=None):
    print("""\
Syntax: collect_data_files.py [options]

Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 path=<file_path> The destination path you want to put your zipped diag file

Example:
 collect_data_files.py -i cluster.ini -p path=/tmp/nosql
""")
    sys.exit(error)


class cbdatacollectRunner(object):
    def __init__(self, server, path):
        self.server = server
        self.path = path

    def __get_data_path(self, os_type):
        data_path = None
        node_info = RestConnection(self.server).get_nodes_self()
        for storage in node_info.storage:
            if storage.type == 'hdd':
                data_path = storage.path
                break
        if os_type == 'windows':
            # Windows server will return the path as c:/Program Files/Couchbase/Server/var/lib/couchbase/data
            data_path = "/cygdrive/c{0}".format(data_path[data_path.find("/"):])
        return data_path

    def run(self):
        remote_client = RemoteMachineShellConnection(self.server)
        now = datetime.now()
        day = now.day
        month = now.month
        year = now.year
        hour = now.timetuple().tm_hour
        minute = now.timetuple().tm_min
        file_name = "%s-%s%s%s-%s%s-couch.tar.gz" % (self.server.ip,
                                                     month, day, year, hour,
                                                     minute)
        print("Collecting data files from %s\n" % self.server.ip)

        remote_client.extract_remote_info()
        data_path = self.__get_data_path(os_type=remote_client.info.type.lower())
        output, error = remote_client.execute_command("tar -zcvf {0} '{1}' >/dev/null 2>&1".
                                                      format(file_name, data_path))
        print("\n".join(output))
        print("\n".join(error))

        user_path = "/home/"
        if self.server.ssh_username == "root":
            user_path = "/"
        remote_path = "%s%s" % (user_path, self.server.ssh_username)
        status = remote_client.file_exists(remote_path, file_name)
        if not status:
            raise Exception("%s doesn't exists on server" % file_name)
        status = remote_client.get_file(remote_path, file_name,
                                        "%s/%s" % (self.path, file_name))
        if not status:
            raise Exception("Fail to download zipped logs from %s"
                            % self.server.ip)
        remote_client.execute_command("rm -f %s" % os.path.join(remote_path, file_name))
        remote_client.disconnect()


def main():
    try:
        (opts, _) = getopt.getopt(sys.argv[1:], 'hi:p', [])
        for o, _ in opts:
            if o == "-h":
                usage()

        _input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not _input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    file_path = _input.param("path", ".")
    remotes = (cbdatacollectRunner(server, file_path) for server in _input.servers)
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
            print("collect_data_files hung on this node. Jumping to next node")
        print("collect data files done")

    for remote_thread in remote_threads:
        remote_thread.join(120)
        if remote_thread.isAlive():
            raise Exception("collect_data_files hung on remote node")

if __name__ == "__main__":
    main()
