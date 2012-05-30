#!/usr/bin/python

# Collect and dump stats for release criteria tests

import os
import sys
import time
import signal
import subprocess
from threading import Timer
from optparse import OptionParser, OptionGroup

sys.path.append(".")
sys.path.append("lib")

from TestInput import TestInputParser, TestInputSingleton


class CBStatsCollector:

    _abort = False
    _timer = None
    _PORT = 11210
    _FREQ = 30
    _CB_EXEC = "/opt/couchbase/bin/cbstats"

    def collect_cb_stats(self, servers, cb_exc=_CB_EXEC, port=_PORT, frequency=_FREQ):
        """
        Collect cbstats from servers.

        Run periodically every [@param: frequency] seconds.

        Log files are available with timestamps in the current working directory:

        cb_stats_raw_memory.log:    raw data for different memory measurement
        cb_stats_timings.log:       timing stats
        cb_stats_disp_logs.log:     dispatcher log stats

        @param  servers     server list read from .ini file
        @param  cb_exc      path to cbstats executable
        @param  port        port number to cbstats against
        @param  frequency   frequncy to collect stats, in seconds
        """
        if CBStatsCollector._abort:
            return

        cur_time = (time.strftime('%X %x %Z'))
        print "Collecting cbstats: ({0})".format(cur_time)
        for server in servers:
            # TODO: replace system calls
            print "server: {0}".format(server.ip)

            # cbstats ip:port raw memory
            subprocess.call("echo \"\nServer: {0} ({1}):\" >> {2}"
                .format(server.ip, cur_time, "./cb_stats_raw_memory.log"), shell=True)
            subprocess.call("{0} {1}:{2} {3} >> {4}"
                .format(cb_exc, server.ip, port, "raw memory", "./cb_stats_raw_memory.log"), shell=True)

            # cbstats ip:port timings
            subprocess.call("echo \"\nServer: {0} ({1}):\" >> {2}"
                .format(server.ip, cur_time, "./cb_stats_timings.log"), shell=True)
            subprocess.call("{0} {1}:{2} {3} >> {4}"
                .format(cb_exc, server.ip, port, "timings", "./cb_stats_timings.log"), shell=True)

            # cbstats ip:port dispacher logs
            subprocess.call("echo \"\nServer: {0} ({1}):\" >> {2}"
                .format(server.ip, cur_time, "./cb_stats_disp_logs.log"), shell=True)
            subprocess.call("{0} {1}:{2} {3} >> {4}"
                .format(cb_exc, server.ip, port, "dispatcher logs", "./cb_stats_disp_logs.log"), shell=True)

        CBStatsCollector._timer = Timer(frequency, self.collect_cb_stats,
            args=(servers, cb_exc, port, frequency))
        CBStatsCollector._timer.start()

    def stop(self):
        """
        Stop the periodic stats collection
        """
        CBStatsCollector._abort = True
        if CBStatsCollector._timer:
            CBStatsCollector._timer.cancel()
        print "Stop collecting cbstats: ({0})".format(time.strftime('%X %x %Z'))

def parse_args(argv):

    parser = OptionParser()

    tgroup = OptionGroup(parser, "TestCase/Runlist Options")
    tgroup.add_option("-i", "--ini",
        dest="ini", help="Path to .ini file containing server information,e.g -i tmp/local.ini")
    tgroup.add_option("-c", "--cb",
        dest="cbstats", help="Path to cbstats executable, default: /opt/couchbase/bin/cbstats")
    tgroup.add_option("-f", "--freq",
        dest="freq", help="Frequency to collect stats (in seconds), default: 30s")
    parser.add_option_group(tgroup)
    options, args = parser.parse_args()

    if not options.ini:
        parser.error("please specify an .ini file (-i)")
        parser.print_help()

    cb_exc = options.cbstats or CBStatsCollector._CB_EXEC
    freq = options.freq or CBStatsCollector._FREQ

    return  cb_exc, int(freq)

def signal_handler(signum, frame):
    """
    Handles registered signal and exit.
    """
    print "[release_stats_collector] : received signal = {0}, aborting".format(signum)
    global statsCollector
    statsCollector.stop()
    signal.alarm(30)
    sys.exit(0)

if __name__ == "__main__":

    _cb_exc, _freq = parse_args(sys.argv)

    # remove freq from argv to be backward compatible
    if "-f" in sys.argv:
        sys.argv.remove("-f")
    elif "--freq" in sys.argv:
        sys.argv.remove("--freq")

    TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)

    if not TestInputSingleton.input.servers:
        print "[release_stats_collector error] empty server list, please check .ini file"
        sys.exit(-2)

    if not os.path.exists(_cb_exc):
        print "[release_stats_collector error] cannot find cbstats executable in \"{0}\""\
            .format(_cb_exc)
        sys.exit(-3)

    print "[Servers]"
    for server in TestInputSingleton.input.servers:
        print server.ip
    print
    print "Dumping stats for release criteria tests:"
    print "{0:20s}: {1}".format("start", time.strftime('%X %x %Z'))
    print "{0:20s}: {1}".format("cb_exec", _cb_exc)
    print "{0:20s}: {1} seconds".format("frequency", _freq)
    print

    # register interrupt handler
    signal.signal(signal.SIGINT, signal_handler)

    statsCollector = CBStatsCollector()
    statsCollector.collect_cb_stats(
        TestInputSingleton.input.servers, _cb_exc, 11210, _freq)    #TODO remove hardcoded port

