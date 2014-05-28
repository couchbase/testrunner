#!/usr/bin/env python
import getopt
import sys

sys.path.extend(('.', 'lib'))
from lib.remote.remote_util import RemoteMachineShellConnection
import TestInput
import logger
import logging.config
import os



"""Usage: memcachetest [-h host[:port]] [-t #threads] [-T] [-i #items] [-c #iterations]
            [-v] [-V] [-f dir] [-s seed] [-W size] [-C vbucketconfig]
    -h The hostname:port where the memcached server is running
       (use mulitple -h args for multiple servers)
    -t The number of threads to use
    -T The number of seconds for which test is to be carried out
       (used with -l loop and repeat)
    -i The number of items to operate with
    -c The number of iteratons each thread should do
    -l Loop and repeat the test, but print out information for each run
    -m The minimum object size to use during testing
    -M The maximum object size to use during testing
    -F Use fixed message size, specified by -M
    -V Verify the retrieved data
    -v Verbose output
    -L Use the specified memcached client library
    -W connection pool size
    -r Use random item values; default: 1 (true)
    -s Use the specified seed to initialize the random generator
    -S Skip the populate of the data
    -P The probability for a set operation
       (default: 33 meaning set 33% of the time)
    -K specify a prefix that is added to all of the keys
    -C Read vbucket data from host:port specified"""

class MemcachetestRunner():
    def __init__(self, server, path="/tmp/", memcached_ip="localhost", memcached_port="11211", num_items=100000, extra_params=""):
        self.server = server
        self.shell = RemoteMachineShellConnection(self.server)
        self.path = path
        self.memcached_ip = memcached_ip
        self.memcached_port = memcached_port
        self.num_items = num_items
        self.extra_params = extra_params
        self.log = logger.Logger.get_logger()

    def start_memcachetest(self):
        # check that memcachetest already installed
        exists = self.shell.file_exists('/usr/local/bin/', 'memcachetest')
        if not exists:
            # try to get from git and install
            output, error = self.shell.execute_command_raw("cd {0}; git clone git://github.com/membase/memcachetest.git".format(self.path))
            self.shell.log_command_output(output, error)
            if "git: command not found" in output[0]:
                self.fail("Git should be installed on hosts!")
            output, error = self.shell.execute_command_raw("cd {0}/memcachetest; ./config/autorun.sh && ./configure && make install".format(self.path))
            self.shell.log_command_output(output, error)
        else:
            self.log.info("memcachetest already set on {0}:/usr/local/bin/memcachetest".format(self.server.ip, self.path))
        self.stop_memcachetest()
        return self.launch_memcachetest()

    def launch_memcachetest(self):
        exists = self.shell.file_exists('/usr/local/bin/', 'memcachetest')
        if not exists:
            command = "{0}/memcachetest/memcachetest -h {1}:{2} -i {3} {4}".format(self.path, self.memcached_ip, self.memcached_port, self.num_items, self.extra_params)
        else:
            command = "/usr/local/bin/memcachetest -h {0}:{1} -i {2} {3}".format(self.memcached_ip, self.memcached_port, self.num_items, self.extra_params)
        output, error = self.shell.execute_command_raw(command)
        return self.shell.log_command_output(output, error, track_words=("downstream timeout",))

    def stop_memcachetest(self):
        cmd = "killall memcachetest"
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.log.info("memcachetest was stopped on {0}".format(self.server.ip))
