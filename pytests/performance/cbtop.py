import os
import signal
from multiprocessing import Process
from functools import wraps

from pytests.performance.perf_defaults import PerfDefaults

try:
    from libcbtop.main import main as cbtop_run
except ImportError:
    print("unable to import cbtop: see http://pypi.python.org/pypi/cbtop")
    cbtop_run = None


class EventType:
    START = "start"
    STOP = "stop"
    ABORT = "abort"


def cbtop(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not cbtop_run or not self.parami("prefix", 1) == 0:
            return func(self, *args, **kwargs)

        try:
            ip = self.input.servers[0].ip
        except:
            return func(self, *args, **kwargs)

        dbhost = self.param("cbtop_dbhost", PerfDefaults.cbtop_dbhost)
        dbevent = self.param("cbtop_dbevent", PerfDefaults.cbtop_dbevent)

        if not self.init_seriesly(dbhost, dbevent):
            return func(self, *args, **kwargs)

        event = self.seriesly_event()
        event["name"] = func.__name__
        event["type"] = EventType.START
        self.seriesly[dbevent].append(event)

        kws = {"itv": self.parami("cbtop_itv", PerfDefaults.cbtop_itv),
               "dbhost": dbhost,
               "dbslow": self.param("cbtop_dbslow", PerfDefaults.cbtop_dbslow),
               "dbfast": self.param("cbtop_dbfast", PerfDefaults.cbtop_dbfast)}

        proc = Process(target=cbtop_run, args=(ip, ), kwargs=kws)
        proc.start()
        os.setpgid(proc.pid, proc.pid)

        try:
            ret = func(self, *args, **kwargs)
        except:
            event["type"] = EventType.ABORT
            self.seriesly[dbevent].append(event)
            raise
        finally:
            os.killpg(proc.pid, signal.SIGKILL)   # TODO - gracefully shutdown

        event["type"] = EventType.STOP
        self.seriesly[dbevent].append(event)

        return ret

    return wrapper
