import getopt
import sys
import time
from threading import Thread
from datetime import datetime

sys.path.append(".")
sys.path.append("lib")
from remote.remote_util import RemoteMachineShellConnection
import testconstants
import TestInput
import logger

def usage(error=None):
    print """\
Syntax: collect_server_info.py [options]

Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 path=<file_path> The destination path you want to put your zipped diag file

Example:
 collect_server_info.py -i cluster.ini -p path=/tmp/nosql
"""
    sys.exit(error)

class cbcollectRunner(object):
    def __init__(self, server, path):
        self.server = server
        self.path = path

    def run(self):
        remote_client = RemoteMachineShellConnection(self.server)
        now = datetime.now()
        day = now.day
        month = now.month
        year = now.year
        file_name = "%s-%s%s%s-diag.zip" % (self.server.ip, month, day, year)
        print "Collecting logs from %s\n" % (self.server.ip)
        output, error = remote_client.execute_cbcollect_info(file_name)
        print "\n".join(output)
        print "\n".join(error)

        user_path = "/home/"
        if self.server.ssh_username == "root":
            user_path = "/"
        if not remote_client.file_exists("%s%s" % (user_path, self.server.ssh_username), file_name):
            raise Exception("%s doesn't exists on server" % (file_name))
        if remote_client.get_file("%s%s" % (user_path, self.server.ssh_username), file_name, "%s/%s" % (self.path, file_name)):
            print "Downloading zipped logs from %s" % (self.server.ip)
        else:
            raise Exception("Fail to download zipped logs from %s" % (self.server.ip))
        remote_client.disconnect()

if __name__ == "__main__":
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p', [])
        for o, a in opts:
            if o == "-h":
                usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError, error:
        usage("ERROR: " + str(error))

    file_path=input.param("path", ".")

    remotes = []
    for server in input.servers:
        remotes.append(cbcollectRunner(server, file_path))

    remote_threads = []
    for remote in remotes:
        remote_threads.append(Thread(target=remote.run))

    for remote_thread in remote_threads:
        remote_thread.start()
        stop = False
        start_time = 15
        while not stop:
            if remote_thread.isAlive():
                print "collecting info.  Wait for 15 seconds"
                time.sleep(15)
                start_time += 15
                if start_time >= 1200:
                    print "cbcollect_info hang on this node.  Jump to next node"
                    stop = True
            else:
                print "collect info done"
                stop = True

    for remote_thread in remote_threads:
        remote_thread.join(1800)
        if remote_thread.isAlive():
            raise Exception("cbcollect_info hangs on remote node")

