#!/usr/bin/env python
import getopt
import sys

sys.path.extend(('.', 'lib'))
from lib.remote.remote_util import RemoteMachineShellConnection
import TestInput
import logger
import logging.config
import os


def usage(error=None):
    print("""\
Syntax: measure_sched_delays.py [options]

Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 path=<file_path> The destination path to folder for deploying and running measure-sched-delays.tar.gz script
 task=<start/stop/fetch_logs> sart/stop measure-sched-delays script or fetch its logs ('start' by default)

Example:
 measure_sched_delays.py -i cluster.ini -p path=/tmp/measure-sched-delays,task=start
""")
    sys.exit(error)


class SchedDelays():
    def __init__(self, servers, path="/tmp/measure-sched-delays"):
        self.servers = servers
        self.path = path
        create_log_file("run_measure_sched_delays.logging.conf", "run_measure_sched_delays.log", "INFO")
        logging.config.fileConfig("run_measure_sched_delays.logging.conf")
        self.log = logger.Logger.get_logger()

    def start_measure_sched_delays(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            exists = shell.file_exists(self.path, 'measure-sched-delays')
            if not exists:
                shell.copy_file_local_to_remote("resources/linux/measure-sched-delays.tar.gz", "{0}.tar.gz".format(self.path))
                output, error = shell.execute_command_raw("cd /tmp/; tar -xvzf measure-sched-delays.tar.gz")
                shell.log_command_output(output, error)
                output, error = shell.execute_command_raw("cd {0}; ./configure; make".format(self.path))
                shell.log_command_output(output, error)
            else:
                self.log.info("measure-sched-delays already deployed on {0}:{1}".format(server.ip, self.path))
            self.stop_measure_sched_delay()
            output, error = shell.execute_command_raw("rm -rf {0}/sched-delay*".format(self.path))
            shell.log_command_output(output, error)
            self.launch_measure_sched_delay(shell, file="sched-delay-{0}".format(server.ip))
            shell.disconnect()

    def launch_measure_sched_delay(self, shell, file="sched-delay"):
        cmd = "cd {0}; nohup ./collect-sched-delays.rb {1} > /dev/null 2>&1 &".format(self.path, file)
        output, error = shell.execute_command_raw(cmd)
        shell.log_command_output(output, error)

    def stop_measure_sched_delay(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "killall -9 -r .*measure-sched-delays"
            output, error = shell.execute_command(cmd)
            shell.log_command_output(output, error)
            shell.disconnect()
            self.log.info("measure-sched-delays was stopped on {0}".format(server.ip))

    def fetch_logs(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            files = shell.list_files(self.path + "/")
            files = [file for file in files if file["file"].startswith("sched-delay")]
            for file in files:
                shell.copy_file_remote_to_local(file["path"] + file["file"], os.getcwd() + "/" + file["file"])
            self.log.info("copied {0} from {1}".format([file["file"] for file in files], server.ip))
            shell.disconnect()

def create_log_file(log_config_file_name, log_file_name, level):
    tmpl_log_file = open("logging.conf.sample")
    log_file = open(log_config_file_name, "w")
    log_file.truncate()
    for line in tmpl_log_file:
        newline = line.replace("@@LEVEL@@", level)
        newline = newline.replace("@@FILENAME@@", log_file_name)
        log_file.write(newline)
    log_file.close()
    tmpl_log_file.close()


def main():
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
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    tool_path = input.param("path", "/tmp/measure-sched-delays")
    task = input.param("task", "start")

    delays = SchedDelays(input.servers, tool_path)

    try:
        if task == "start":
            delays.start_measure_sched_delays()
        elif task == "stop":
            delays.stop_measure_sched_delay()
        elif task == "fetch_logs":
            delays.fetch_logs()
        else:
            usage("ERROR: task was not specified correctly")
    except BaseException as e:
        print(e)

if __name__ == "__main__":
    main()
