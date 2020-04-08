import os
import re
import time
import subprocess
import select
import queue
import threading
from . import constants

class NSLogPoller(threading.Thread):

    def __init__(self, index, path = constants.NS_PATH):
        super(NSLogPoller, self).__init__()
        self.index = index
        self.filename = "%s/cluster_run_n_%i.log" % (path, index)
        self.dump_q = queue.Queue()
        self.event_q = queue.Queue()
        self.mc_flag = False
        self.compact_flag = False
        self.ns_started_flag = False
        self.rebalance_flag = False

    def setNSStartedEventFlag(self, flag):
        self.ns_started_flag = flag

    def setMcEventFlag(self, flag):
        self.mc_flag = flag

    def setCompactEventFlag(self, flag):
        self.compact_flag = flag

    def setRebalanceEventFlag(self, flag):
        self.rebalance_flag = flag

    def getQ(self, q, timeout):
        msg = None
        try:
            msg = q.get(timeout = timeout)
        except queue.Empty:
            pass
        return msg

    def getEventQItem(self, timeout = 30):
        return self.getQ(self.event_q, timeout)

    def getDumpQItem(self, timeout = 30):
        return self.getQ(self.dump_q, timeout)

    def run(self):
        f = open(self.filename, 'r', 1)
        existing_dumps = {}
        ns_running = True

        while ns_running:
            # poll for data to read
            read, write, ex = select.select([f], [], [])

            # till EOF
            msg = f.readline()
            while msg:
                if constants.NS_EXITED_STR in msg: # ns exited
                  print(msg)
                  ns_running = False
                  break
                m = re.search(' /.*/n_'+str(self.index)+'/.*dmp', msg)
                if m:
                    dmp_path = m.group(0).lstrip()
                    if dmp_path not in existing_dumps:
                        existing_dumps[dmp_path] = True
                        print(dmp_path)
                        self.dump_q.put(dmp_path)

                if self.mc_flag and constants.MC_STARTED_STR in msg:
                    print(msg)
                    self.event_q.put(msg)
                if self.compact_flag and constants.COMPACT_STARTED_STR in msg:
                    print(msg)
                    self.event_q.put(msg)
                if self.ns_started_flag and constants.NS_STARTED_STR in msg:
                    print(msg)
                    self.event_q.put(msg)
                if self.rebalance_flag and constants.REBALANCE_STARTED_STR in msg:
                    print(msg)
                    self.event_q.put(msg)

                # next msg
                msg = f.readline()

