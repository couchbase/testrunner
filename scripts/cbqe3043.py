import getopt
import sys
import logging
import logging.config

sys.path = [".", "lib"] + sys.path
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.membase.api.rest_client import RestConnection
import TestInput

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()

def usage(error=None):
    print("Please provide INI file")
    sys.exit(error)

def main():
    try:
        (opts, _) = getopt.getopt(sys.argv[1:], 'hi:', [])
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

    for server in _input.servers:
        shell = RemoteMachineShellConnection(server)
        command = "mount | grep '/cbqe3043'"
        output, error = shell.execute_command(command)
        if len(error) > 0:
            raise Exception("Unable to determine if a partition of 20MB already exists on " + server.ip)
        if len(output) > 0:
            print("/cbqe3043 partition is already mounted on " + server.ip)
        if len(output) == 0:
            print("/cbqe3043 not mounted on " + server.ip)
            print("creating /cbqe3043 on " + server.ip)
            command = "rm -rf /cbqe3043; mkdir -p /cbqe3043"
            output, _ = shell.execute_command(command)
            if len(output) > 0:
                raise Exception("Unable to create directory /cbqe3043 on " + server.ip)
            print("creating /usr/disk-img on " + server.ip)
            command = "rm -rf /usr/disk-img; mkdir -p /usr/disk-img"
            output, _ = shell.execute_command(command)
            if len(output) > 0:
                raise Exception("Unable to create directory /usr/disk-img on " + server.ip)
            print("creating disk file /usr/disk-img/disk-cbqe3043.ext3 on " + server.ip)
            command = "dd if=/dev/zero of=/usr/disk-img/disk-cbqe3043.ext3 count=40960"
            output, _ = shell.execute_command(command)
            if len(output) > 0:
                raise Exception("Unable to create disk file /usr/disk-img/disk-cbqe3043.ext3 on " + server.ip)
            print("formatting disk file into an ext3 filesystem on " + server.ip)
            command = "/sbin/mkfs -t ext3 -q /usr/disk-img/disk-cbqe3043.ext3 -F"
            output, _ = shell.execute_command(command)
            if len(output) > 0:
                raise Exception("Unable to format disk file into an ext3 filesystem on " + server.ip)
            print("mounting /cbqe3043 on" + server.ip)
            command = "mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-cbqe3043.ext3 /cbqe3043"
            output, _ = shell.execute_command(command)
            if len(output) > 0:
                raise Exception("Unable to mount /cbqe3043 on " + server.ip)
            print("giving 777 permissions for /cbqe3043 on " + server.ip)
            command = "chmod 777 /cbqe3043"
            output, _ = shell.execute_command(command)
            if len(output) > 0:
                raise Exception("Unable to give permissions for /cbqe3043 on " + server.ip)

        print("setting data path to /cbqe3043 on " + server.ip)
        rest_conn = RestConnection(server)
        rest_conn.set_data_path(data_path="/cbqe3043")

if __name__ == "__main__":
    main()