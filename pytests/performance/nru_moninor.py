import threading
import time

from lib.remote.remote_util import RemoteMachineShellConnection

from pytests.performance.perf_defaults import PerfDefaults


class NRUMonitor(threading.Thread):

    CMD_NUM_RUNS = "/opt/couchbase/bin/cbstats localhost:11210 "\
                   "all | grep ep_num_access_scanner_runs"

    CMD_NUM_ITEMS = "/opt/couchbase/bin/cbstats localhost:11210 "\
                    "all | grep ep_access_scanner_num_items"

    CMD_RUNTIME = "/opt/couchbase/bin/cbstats localhost:11210 "\
                  "all | grep ep_access_scanner_last_runtime"

    def __init__(self, freq, reb_delay, eperf):
        self.freq = freq
        self.reb_delay = reb_delay
        self.eperf = eperf
        self.shell = None
        super(NRUMonitor, self).__init__()

    def run(self):
        print("[NRUMonitor] started running")

        # TODO: evaluate all servers, smarter polling freq
        server = self.eperf.input.servers[0]
        self.shell = RemoteMachineShellConnection(server)

        nru_num = self.nru_num = self.get_nru_num()
        if self.nru_num < 0:
            return

        while nru_num <= self.nru_num:
            print("[NRUMonitor] nru_num = %d, sleep for %d seconds"\
                  % (nru_num, self.freq))
            time.sleep(self.freq)
            nru_num = self.get_nru_num()
            if nru_num < 0:
                break

        gmt_now = time.strftime(PerfDefaults.strftime, time.gmtime())
        speed, num_items, run_time = self.get_nru_speed()

        print("[NRUMonitor] access scanner finished at: %s, speed: %s, "\
              "num_items: %s, run_time: %s"\
              % (gmt_now, speed, num_items, run_time))

        self.eperf.clear_hot_keys()

        print("[NRUMonitor] scheduled rebalance after %d seconds"\
              % self.reb_delay)

        self.shell.disconnect()
        self.eperf.latched_rebalance(delay=self.reb_delay, sync=True)

        gmt_now = time.strftime(PerfDefaults.strftime, time.gmtime())
        print("[NRUMonitor] rebalance finished: %s" % gmt_now)

        print("[NRUMonitor] stopped running")

    def get_nru_num(self):
        """Retrieve how many times nru access scanner has been run"""
        return self._get_shell_int(NRUMonitor.CMD_NUM_RUNS)

    def get_nru_speed(self):
        """Retrieve runtime and num_items for the last access scanner run
        Calculate access running speed

        @return (speed, num_items, run_time)
        """
        num_items = self._get_shell_int(NRUMonitor.CMD_NUM_ITEMS)

        if num_items <= 0:
            return -1, -1, -1

        run_time = self._get_shell_int(NRUMonitor.CMD_RUNTIME)

        if run_time <= 0:
            return -1, num_items, -1

        speed = num_items // run_time

        return speed, num_items, run_time

    def _get_shell_int(self, cmd):
        """Fire a shell command and return output as integer"""
        if not cmd:
            print("<_get_shell_int> invalid cmd")
            return -1

        output, error = self.shell.execute_command(cmd)

        if error:
            print("<_get_shell_int> unable to execute cmd '%s' from %s: %s"\
                  % (cmd, self.shell.ip, error))
            return -1

        if not output:
            print("<_get_shell_int> unable to execute cmd '%s' from %s: "\
                  "empty output" % (cmd, self.shell.ip))
            return -1

        try:
            num = int(output[0].split(":")[1])
        except (AttributeError, IndexError, ValueError) as e:
            print("<_get_shell_int> unable to execute cmd '%s' from %s:"\
                  "output - %s, error - %s" % (cmd, self.shell.ip, output, e))
            return -1

        if num < 0:
            print("<_get_shell_int> invalid number: %d" % num)
            return -1

        return num
