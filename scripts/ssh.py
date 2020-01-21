#!/usr/bin/python

import getopt
import sys
from threading import Thread
from datetime import datetime
import uuid
sys.path = [".", "lib"] + sys.path
from remote.remote_util import RemoteMachineShellConnection
import TestInput

def usage(error=None):
    print("""\
Syntax: ssh.py [options] [command]

Options:
 -p <key=val,...> Comma-separated key=value info.
 -i <file>        Path to .ini file containing cluster information.

Available keys:
 script=<file>           Local script to run
 parallel=true           Run the command in parallel on all machines

Examples:
 ssh.py -i /tmp/ubuntu.ini -p script=/tmp/set_date.sh
 ssh.py -i /tmp/ubuntu.ini -p parallel=false ls -l /tmp/core*
""")
    sys.exit(error)


class CommandRunner(object):
    def __init__(self, server, command):
        self.server = server
        self.command = command

    def run(self):
        remote_client = RemoteMachineShellConnection(self.server)
        output, error = remote_client.execute_command(self.command)
        print(self.server.ip)
        print("\n".join(output))
        print("\n".join(error))

class ScriptRunner(object):
    def __init__(self, server, script):
        self.server = server
        with open(script) as  f:
            self.script_content = f.read()
        self.script_name = "/tmp/" + str(uuid.uuid4())

    def run(self):
        remote_client = RemoteMachineShellConnection(self.server)
        remote_client.create_file(self.script_name, self.script_content)
        output, error = remote_client.execute_command("chmod 777 {0} ; {0} ; rm -f {0}".format(self.script_name))
        print(self.server.ip)
        print("\n".join(output))
        print("\n".join(error))

class RemoteJob(object):
    def sequential_remote(self, input):
        remotes = []
        params = input.test_params
        for server in input.servers:
            if "script" in params:
                remotes.append(ScriptRunner(server, params["script"]))
            if "command" in params:
                remotes.append(CommandRunner(server, params["command"]))

        for remote in remotes:
            try:
                remote.run()
            except Exception as ex:
                print("unable to complete the job: {0}".format(ex))

    def parallel_remote(self, input):
        remotes = []
        params = input.test_params
        for server in input.servers:
            if "script" in params:
                remotes.append(ScriptRunner(server, params["script"]))
            if "command" in params:
                remotes.append(CommandRunner(server, params["command"]))

        remote_threads = []
        for remote in remotes:
            remote_threads.append(Thread(target=remote.run))

        for remote_thread in remote_threads:
            remote_thread.start()

        for remote_thread in remote_threads:
            remote_thread.join()

def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p:', [])
        for o, a in opts:
            if o == "-h":
                usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    command_offset = 3
    if "-p" in sys.argv[:4]:
        command_offset += 2

    command = " ".join(sys.argv[command_offset:])

    if command:
        input.test_params["command"] = command

    if input.param("parallel", True):
        # workaround for a python2.6 bug of using strptime with threads
        datetime.strptime("30 Nov 00", "%d %b %y")
        RemoteJob().parallel_remote(input)
    else:
        RemoteJob().sequential_remote(input)


if __name__ == "__main__":
    main()
