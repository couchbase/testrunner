#!/usr/bin/env python
import re
import sys
import math
import time
import socket
import string
import struct
import random
import threading
import multiprocessing
import queue
import logging
import logging.config
from collections import deque
from hashlib import md5
import json
import inspect

sys.path.extend(('.', 'lib'))

from lib import crc32
from lib import mc_bin_client
from lib.membase.api.rest_client import RestConnection
from lib.membase.api.exception import QueryViewException, \
    ServerUnavailableException
from lib.memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE, \
    ERR_NOT_MY_VBUCKET, ERR_ENOMEM, ERR_EBUSY, ERR_ETMPFAIL, REQ_PKT_FMT, \
    RES_PKT_FMT, MIN_RECV_PACKET, SET_PKT_FMT, CMD_GET, CMD_SET, CMD_DELETE, \
    CMD_ADD, CMD_REPLACE, CMD_PREPEND, CMD_APPEND  # "ARPA"
from lib.perf_engines.libobserve.obs_mcsoda import McsodaObserver
from lib.perf_engines.libobserve.obs import Observable
from lib.perf_engines.libobserve.obs_helper import UnblockingJoinableQueue

logging.config.fileConfig("mcsoda.logging.conf")
log = logging.getLogger()

LARGE_PRIME = 9576890767
OPAQUE_MAX = 4294967295
INT_TYPE = type(123)
FLOAT_TYPE = type(0.1)
DICT_TYPE = type({})
RETRIES = 5


class Stack(object):
    """
    Not a traditional stack:

    If stack is full, append() removes an item from the bottom

    If rotate flag is on,
    pop() rotates the queue rather than removes an item from the top
    """
    def __init__(self, size, rotate=False):
        self.size = size
        self.rotate = rotate
        self.deq = deque()

    def __repr__(self):
        return "Stack(size=%r, rotate=%r, deq=%r" \
            % (self.size, self.rotate, self.deq)

    def pop(self):
        if self.size <= 0:
            log.error("unable to pop item from Stack: invalid size %s"
                      % self.size)
            return None
        try:
            if self.rotate:
                ret = self.deq[-1]
                self.deq.rotate(1)
                return ret
            else:
                return self.deq.pop()
        except IndexError:
            return None

    def append(self, val):
        if self.size <= 0:
            log.error("unable to append item to Stack: invalid size %s"
                      % self.size)
            return
        while len(self.deq) >= self.size:
            self.deq.popleft()
        self.deq.append(val)

    def clear(self):
        num_cleared = len(self.deq)
        self.deq.clear()
        log.info("cleared %d items from hot stack" % num_cleared)


def dict_to_s(d, level="", res=None, suffix=", ", ljust=None):
    res = res or []
    return ''.join(dict_to_s_inner(d, level, res, suffix, ljust))


def dict_to_s_inner(d, level, res, suffix, ljust):
    dtype = DICT_TYPE
    scalars = []
    complex = []

    for key in list(d.keys()):
        if isinstance(d[key], dtype):
            complex.append(key)
        else:
            scalars.append(key)
    scalars.sort()
    complex.sort()

    # Special case for histogram output.
    histo_max = 0
    histo_sum = 0
    if scalars and not complex and \
            isinstance(scalars[0], FLOAT_TYPE) and isinstance(d[scalars[0]], INT_TYPE):
        for key in scalars:
            v = d[key]
            histo_max = max(v, histo_max)
            histo_sum = histo_sum + v

    histo_cur = 0  # Running total for histogram output.
    for key in scalars:
        if isinstance(key, FLOAT_TYPE):
            k = re.sub("0*$", "", "%.7f" % key)
        else:
            k = str(key)
        if ljust:
            k = string.ljust(k, ljust)
        x = d[key]
        if histo_max:
            histo_cur = histo_cur + x
        v = str(x)
        if histo_max:
            v = string.rjust(v, 8) + " " + \
                string.rjust("{0:.1%}".format(histo_cur / float(histo_sum)), 8) + " " + \
                ("*" * int(math.ceil(50.0 * d[key] / histo_max)))

        res.append(level + k + ": " + v + suffix)

    # Recurse for nested, dictionary values.
    if complex:
        res.append("\n")
    for key in complex:
        res.append(level + str(key) + ":\n")
        dict_to_s_inner(d[key], level + "  ", res, "\n", 9)

    return res

# The histo dict is returned by add_timing_sample().
# The percentiles must be sorted, ascending, like [0.90, 0.99].


def histo_percentile(histo, percentiles):
    v_sum = 0
    bins = sorted(list(histo.keys()))
    for bin in bins:
        v_sum += histo[bin]
    v_sum = float(v_sum)
    v_cur = 0  # Running total.
    rv = []
    for bin in bins:
        if not percentiles:
            return rv
        v_cur += histo[bin]
        while percentiles and (v_cur // v_sum) >= percentiles[0]:
            rv.append((percentiles[0], bin))
            percentiles.pop(0)
    return rv

# --------------------------------------------------------

MIN_VALUE_SIZE = [10]


def obs_cb(store):
    """
    callback for observe thread.
    """
    if not store:
        log.error("obs_cb is broken")
        return

    log.info("obs_cb: clear obs_key_cas %s" % store.obs_key_cas)
    store.obs_key_cas.clear()


def woq_worker(req_queue, stats_queue, ctl, cfg, store):
    """
    measure latencies of standard write/observe/query patterns
    """
    bucket = "default"
    ddoc = "A"
    view = "city1"  # TODO pass from eperf
    query_params = {"limit": 10,
                    "stale": "false"}

    log.info("woq_worker started")
    woq_observer = McsodaObserver(ctl, cfg, store, None)

    while True:

        key, cas = req_queue.get(block=True)
        start_time = time.time()  # latency includes observe and query time

        # observe
        if not woq_observer.block_for_persistence(key, cas):
            # put an invalid object to indicate error
            stats_queue.put([key, cas, 0, 0, 0, 0], block=True)
            req_queue.task_done()
            continue

        obs_latency = time.time() - start_time
        if cfg.get("woq-verbose", 0):
            log.info("woq_worker obs latency: %s, key = %s, cas = %s "
                     % (obs_latency, key, cas))

        query_start = time.time()

        try:
            result = store.rest.query_view(ddoc, view, bucket, query_params)
        except QueryViewException as e:
            log.error("woq_worker QueryViewException: %s" % e)
            stats_queue.put([key, cas, 0, 0, 0, 0], block=True)
            req_queue.task_done()
            continue

        query_latency = time.time() - query_start
        if cfg.get("woq-verbose", 0):
            log.info("woq_worker query latency: %s, key = %s, cas = %s "
                     % (query_latency, key, cas))
            log.info("woq_worker query result: %s" % result)

        latency = time.time() - start_time
        stats_queue.put([key, cas, start_time, obs_latency, query_latency, latency],
                        block=True)
        req_queue.task_done()
    log.info("woq_worker stopped working")


def cor_worker(stats_queue, ctl, cfg, cur, store):
    """
    Sequentially measure latencies of create/observe_replications patterns

    Create brand new items instead of reusing the foreground mcsoda load
    """
    OP_WIN = 1     # ensure foreground load to dominate the traffic
    backoff = 0
    key_num = OPAQUE_MAX - cur.get('cur-gets', 0)
    store.cfg["cor"] = 1
    store.cfg["batch"] = 1
    persist = (int(cfg.get('cor-persist', 0)) == 1)

    if isinstance(store, StoreMembaseBinary):
        store.awareness.reset()
    else:
        log.error("cannot start cor_worker: invalid store %s" % store)
        return

    log.info("cor_worker started")

    observer = McsodaObserver(ctl, cfg, store, None)

    while True:

        if backoff:
            log.info("cor_worker sleep for %s seconds" % backoff)
            time.sleep(backoff)
            backoff = 0

        start_time = time.time()

        key_num -= 1
        key_str = prepare_key(key_num, cfg.get('prefix', ''))

        data = store.gen_doc(
            key_num, key_str,
            choose_entry(cfg.get('min-value-size', MIN_VALUE_SIZE), key_num)
        )

        grp = store.inflight_start()
        store.cmd_append("set", key_num, key_str, data, 0, grp)
        msg = store.inflight_complete(grp)

        store.inflight_send(msg)
        store.inflight_recv(1, grp, expectBuffer=False)

        cas = store.cor_key_cas[key_num]
        store.cor_key_cas.clear()

        status = \
            observer.block_for_replication(key_str, cas, persist=persist)
        latency = time.time() - start_time

        if status:
            stats_queue.put([key_str, cas, start_time, latency], block=True)
        else:
            # put an invalid object to indicate error
            stats_queue.put([key_str, cas, 0, 0], block=True)

        if latency < OP_WIN:
            backoff = OP_WIN - latency

    log.info("cor_worker stopped")


def run_worker(ctl, cfg, cur, store, prefix, heartbeat=0, why=""):
    i = 0
    t_last_flush = time.time()
    t_last_cycle = time.time()
    o_last_flush = store.num_ops(cur)
    t_last = time.time()
    o_last = store.num_ops(cur)
    xfer_sent_last = 0
    xfer_recv_last = 0
    store.why = why
    store.stats_ops = cfg.get("stats_ops", 10000)

    report = cfg.get('report', 0)
    hot_shift = cfg.get('hot-shift', 0)
    max_ops_per_sec = float(cfg.get('max-ops-per-sec', 0))

    if cfg.get('max-ops-per-sec', 0) > 0 and not 'batch' in cur:
        cur['batch'] = 10

    log.debug("%s starts cfg: %s" % (why, cfg))
    log.debug("%s starts cur: %s" % (why, cur))
    log.debug("%s starts store: %s" % (why, store))
    log.debug("%s starts prefix: %s" % (why, prefix))
    log.debug("%s starts running." % why)

    heartbeat_last = t_last

    if cfg.get('woq-pattern', 0):
        woq_req_queue = UnblockingJoinableQueue(1)    # pattern: write/observe/query
        woq_stats_queue = multiprocessing.Queue(1)
        woq_process = multiprocessing.Process(target=woq_worker,
                                              args=(woq_req_queue, woq_stats_queue,
                                                    ctl, cfg, store))
        woq_process.daemon = True
        woq_process.start()

    if cfg.get('observe', 0):
        observer = McsodaObserver(ctl, cfg, store, obs_cb)
        observer.start()

    if cfg.get('cor-pattern', 0):
        cor_stats_queue = multiprocessing.Queue()
        cor_process = multiprocessing.Process(
            target=cor_worker, args=(cor_stats_queue, ctl, cfg, cur, store))
        cor_process.daemon = True
        cor_process.start()

    while ctl.get('run_ok', True):
        num_ops = cur.get('cur-gets', 0) + cur.get('cur-sets', 0)

        if cfg.get('max-ops', 0) and num_ops >= cfg.get('max-ops', 0):
            log.info("exiting because of max ops")
            break
        if cfg.get('exit-after-creates', 0) and cfg.get('max-creates', 0) and \
                cur.get('cur-creates', 0) >= cfg.get('max-creates', 0):
            log.info("exiting because of max creates")
            break
        if cfg.get('exit-after-gets', 0) and cfg.get('max-gets', 0) and \
                cur.get('cur-gets', 0) >= cfg.get('max-gets', 0):
            log.info("exiting because of max gets")
            break
        if ctl.get('shutdown_event') is not None and \
                ctl['shutdown_event'].is_set():
            log.info("exiting because of shutdown event")
            break

        heartbeat_duration = time.time() - heartbeat_last
        if heartbeat != 0 and heartbeat_duration > heartbeat:
            heartbeat_last += heartbeat_duration
            if cfg.get('max-ops', 0):
                progress = 100.0 * num_ops // cfg['max-ops']
                log.info("%s num ops = %s out of %s (%.2f %%)",
                         why, num_ops, cfg['max-ops'], progress)
            else:
                log.info("%s num ops = %s", why, num_ops)

        command = next_cmd(cfg, cur, store)
        flushed = store.command(command)

        if flushed and cfg.get('woq-pattern', 0):

            # record stats
            if not woq_stats_queue.empty():
                try:
                    key, cas, start_time, obs_latency, query_latency, latency \
                        = woq_stats_queue.get(block=False)
                    if not start_time and not latency:
                        store.woq_key_cas.clear()   # error
                    else:
                        store.add_timing_sample("woq-obs", obs_latency)
                        store.add_timing_sample("woq-query", query_latency)
                        store.add_timing_sample("woq", latency)
                        store.save_stats(start_time)
                        store.woq_key_cas.clear()   # simply clear all, no key/cas sanity check
                        log.info("woq_stats: key: %s, cas: %s, "
                                 "obs_latency: %f, query_latency: %f, latency: %f"
                                 % (key, cas, obs_latency, query_latency, latency))
                except queue.Empty:
                    pass

            # produce request
            if woq_req_queue.all_finished():
                for key_num, cas in store.woq_key_cas.items():
                    key = prepare_key(key_num, cfg.get('prefix', ''))
                    try:
                        woq_req_queue.put([key, cas], block=False)
                    except queue.Full:
                        break

        if flushed and cfg.get('observe', 0):
            if store.obs_key_cas and not observer.num_observables():
                observables = []
                for key_num, cas in store.obs_key_cas.items():
                    obs = Observable(key=prepare_key(key_num, cfg.get('prefix', '')),
                                     cas=cas,
                                     persist_count=cfg.get('obs-persist-count', 1),
                                     repl_count=cfg.get('obs-repl-count', 1))
                    observables.append(obs)
                observer.load_observables(observables)

        if flushed and cfg.get('cor-pattern', 0):
            # record stats
            if not cor_stats_queue.empty():
                try:
                    key, cas, start_time, latency = \
                        cor_stats_queue.get(block=False)
                    if latency:
                        store.add_timing_sample("cor", latency)
                        store.save_stats(start_time)
                        log.info("cor_stats: key: %s, cas: %s, latency: %f"
                                 % (key, cas, latency))
                except queue.Empty:
                    pass

        i += 1

        if report > 0 and i % report == 0:
            t_curr = time.time()
            o_curr = store.num_ops(cur)
            xfer_sent_curr = store.xfer_sent
            xfer_recv_curr = store.xfer_recv

            t_delta = t_curr - t_last
            o_delta = o_curr - o_last
            xfer_sent_delta = xfer_sent_curr - xfer_sent_last
            xfer_recv_delta = xfer_recv_curr - xfer_recv_last

            try:
                ops_per_sec = o_delta // t_delta
                xfer_sent_per_sec = xfer_sent_delta // t_delta
                xfer_recv_per_sec = xfer_recv_delta // t_delta
            except ZeroDivisionError:
                ops_per_sec = o_delta
                xfer_sent_per_sec = xfer_sent_delta
                xfer_recv_per_sec = xfer_recv_delta


            log.debug(prefix + dict_to_s(cur))
            log.info("%s ops: %s secs: %s ops/sec: %s tx-bytes/sec: %s rx-bytes/sec: %s" %
                     (prefix, o_delta, t_delta, int(ops_per_sec),
                      int(xfer_sent_per_sec) or "unknown",
                      int(xfer_recv_per_sec) or "unknown"))

            t_last = t_curr
            o_last = o_curr
            xfer_sent_last = xfer_sent_curr
            xfer_recv_last = xfer_recv_curr

        if flushed:
            # Code below is responsible for speed limitation.
            # Stream looks like ^_^_^_^_^_^_^
            #
            # delta1 = flush time + previous sleep time (^_)
            # delta2 = flush time (^)
            #
            # TODO: dynamic correction factor.
            # We have to measure actual average throughput - let's say - every
            # minute. Thus we can adjust request rate. For now it's empiric,
            # because we always oversleep.
            CORRECTION_FACTOR = 0.975

            delta1 = time.time() - t_last_cycle
            delta2 = time.time() - t_last_flush
            t_last_cycle += delta1

            ops_done = float(store.num_ops(cur) - o_last_flush)
            o_last_flush += ops_done

            if max_ops_per_sec:
                # Taking into account global throughtput
                if cfg.get('active_fg_workers') is not None:
                    concurrent_workers = cfg.get('active_fg_workers').value
                else:
                    concurrent_workers = 1
                local_max_ops_per_sec = max_ops_per_sec // concurrent_workers
                # Actual throughput
                ops_per_sec = ops_done // delta2
                # Sleep if too fast. It must be too fast.
                if ops_per_sec > local_max_ops_per_sec:
                    sleep_time = CORRECTION_FACTOR * ops_done // local_max_ops_per_sec - delta2
                    time.sleep(max(sleep_time, 0))

            if hot_shift > 0:
                cur['cur-base'] = cur.get('cur-base', 0) + (hot_shift * delta1)

            t_last_flush = time.time()

    store.flush()


def next_cmd(cfg, cur, store):
    do_delete = False
    itm_val = None
    num_ops = cur.get('cur-ops', 0)

    do_set = cfg.get('ratio-sets', 0) > float(cur.get('cur-sets', 0)) / positive(num_ops)
    if do_set:
        itm_gen = True

        cmd = 'set'
        cur_sets = cur.get('cur-sets', 0) + 1
        cur['cur-sets'] = cur_sets
        cur['cur-ops'] = cur.get('cur-ops', 0) + 1

        do_set_create = (
            (cfg.get('max-items', 0) <= 0 or
             cfg.get('max-items', 0) > cur.get('cur-items', 0)) and
            cfg.get('max-creates', 0) > cur.get('cur-creates', 0) and
            cfg.get('ratio-creates', 0) >= float(cur.get('cur-creates', 0)) / positive(cur.get('cur-sets', 0))
        )
        if do_set_create:
            # Create...
            key_num = cur.get('cur-items', 0)

            cur['cur-items'] = cur.get('cur-items', 0) + 1
            cur['cur-creates'] = cur.get('cur-creates', 0) + 1
        else:
            # Update...
            num_updates = cur['cur-sets'] - cur.get('cur-creates', 0)

            do_delete = cfg.get('ratio-deletes', 0) > \
                float(cur.get('cur-deletes', 0)) / positive(num_updates)
            if do_delete:
                itm_gen = False
                cmd = 'delete'
                cur['cur-deletes'] = cur.get('cur-deletes', 0) + 1
            else:
                num_mutates = num_updates - cur.get('cur-deletes', 0)

                do_arpa = cfg.get('ratio-arpas', 0) > \
                    float(cur.get('cur-arpas', 0)) / positive(num_mutates)
                if do_arpa:
                    cmd = 'arpa'
                    cur['cur-arpas'] = cur.get('cur-arpas', 0) + 1

            key_num = choose_key_num(num_updates,
                                     cfg.get('ratio-hot', 0),
                                     cfg.get('ratio-hot-sets', 0),
                                     cur.get('cur-sets', 0),
                                     cur.get('cur-base', 0),
                                     cfg.get('random', 0),
                                     cur)

        if not do_delete and cfg.get('hot-stack', 0):
            stack = cur.get('hot-stack', None)
            if not stack:
                rotate = (cfg.get('hot-stack-rotate', 0) == 1)
                stack = Stack(cfg.get('hot-stack-size', 10), rotate)
                cur['hot-stack'] = stack
            stack.append(key_num)

        expiration = 0
        if cmd[0] == 's' and cfg.get('ratio-expirations', 0.0) * 100 > cur_sets % 100:
            expiration = cfg.get('expiration', 0)

        key_str = prepare_key(key_num, cfg.get('prefix', ''))
        if itm_gen:
            itm_val = store.gen_doc(key_num, key_str,
                                    choose_entry(cfg.get('min-value-size', MIN_VALUE_SIZE),
                                                 num_ops))

        return cmd, key_num, key_str, itm_val, expiration
    else:
        cmd = 'get'
        cur['cur-gets'] = cur.get('cur-gets', 0) + 1
        cur['cur-ops'] = cur.get('cur-ops', 0) + 1

        do_query = cfg.get('ratio-queries', 0) > \
            float(cur.get('cur-queries', 0)) / cur.get('cur-gets', 0)
        if do_query:
            cmd = 'query'
            cur['cur-queries'] = cur.get('cur-queries', 0) + 1

        do_get_hit = (cfg.get('ratio-misses', 0) * 100) <= (cur.get('cur-gets', 0) % 100)
        if do_get_hit:
            key_num = None
            do_hot = (cfg.get('ratio-hot-gets', 0) * 100)\
                > (cur.get('cur-gets', 0) % 100)
            stack = cur.get('hot-stack', None)

            if do_hot and stack:
                key_num = stack.pop()

            if cfg.get('exit-after-gets', 0):
                key_num = cur['cur-gets']

            if not key_num:
                key_num = choose_key_num(cur.get('cur-items', 0),
                                         cfg.get('ratio-hot', 0),
                                         cfg.get('ratio-hot-gets', 0),
                                         cur.get('cur-gets', 0),
                                         cur.get('cur-base', 0),
                                         cfg.get('random', 0),
                                         cur)
            key_str = prepare_key(key_num, cfg.get('prefix', ''))

            return cmd, key_num, key_str, itm_val, 0
        else:
            cur['cur-misses'] = cur.get('cur-misses', 0) + 1
            return cmd, -1, prepare_key(-1, cfg.get('prefix', '')), None, 0


def choose_key_num(num_items, ratio_hot, ratio_hot_choice,
                   num_ops, base, random_key, cur):
    """
    Choose a random or deterministic number in order to generate the MD5 hash.

    The deterministic algorithm always favors new items.
    i.e:
        If many items have been created (num_creates > num_hot_items), \
        hot items are chosen from the newest guys.
    """
    num_creates = cur.get('cur-creates', 0)
    if num_items < 0 or ratio_hot < 0 or ratio_hot > 1:
        log.error("num_items: {0}, num_creates:{1}, ratio_hot: {2}"
                  .format(num_items, num_creates, ratio_hot))
        return 1

    # get a random or deterministic key
    if random_key == 1:
        x = int(random.random() * num_items)
    else:
        pos = cur.get('pos', 0)
        pos = (pos + LARGE_PRIME) % positive(num_items)
        cur['pos'] = pos
        x = pos

    hit_hot_range = (ratio_hot_choice * 100) > (num_ops % 100)
    num_hot_items = positive(math.floor(ratio_hot * num_items))
    num_cold_items = positive(num_items - num_hot_items)
    num_init_items = positive(num_items - num_creates)
    base %= num_init_items

    # calculate offset and apply it to the base
    if hit_hot_range:
        offset = x % num_hot_items
        if offset > num_creates:                          # choose from the left hot set
            retval = (base + offset - num_creates) % num_init_items
        else:
            retval = num_items - offset                   # choose from the right hot set
    else:
        offset = x % num_cold_items
        if num_creates > num_hot_items:
            retval = offset
        elif base > num_cold_items:                         # no split-up on the cold set
            retval = (base + num_hot_items - num_creates + offset) % num_init_items
        elif offset < base:                                 # choose from the left cold set
            retval = offset
        else:
            retval = offset + num_hot_items - num_creates   # choose from the right cold set

    return int(retval) % num_items


def positive(x):
    if x > 0:
        return x
    return 1


def prepare_key(key_num, prefix=None):
    key_hash = md5(str(key_num)).hexdigest()[0:16]
    if prefix and len(prefix) > 0:
        return prefix + "-" + key_hash
    return key_hash


def choose_entry(arr, n):
    return arr[n % len(arr)]


class Store(object):

    def __init__(self):
        self.errors = dict()

    def connect(self, target, user, pswd, cfg, cur, bucket="default", backups=None):
        self.target = target
        self.cfg = cfg
        self.cur = cur
        self.xfer_sent = 0
        self.xfer_recv = 0

    def err_msg(self, error):
        """Generate error message with class.method names as prefix"""
        cname = self.__class__.__name__
        fname = inspect.stack()[2][3]  # err_msg <- save_error <- caller
        return "[{0}.{1}] {2}".format(cname, fname, error)

    def save_error(self, error):
        """Update dictionary with errors"""
        err_msg = self.err_msg(error)
        self.errors[err_msg] = self.errors.get(err_msg, 0) + 1

    def show_some_keys(self):
        log.debug("first 5 keys...")
        for i in range(5):
            log.debug(("echo get %s | nc %s %s" %
                      (self.cmd_line_get(i, prepare_key(i, self.cfg.get('prefix', ''))),
                       self.target.rsplit(':', 1)[0],
                       self.target.rsplit(':', 1)[1])))

    def stats_collector(self, sc):
        self.sc = sc

    def command(self, c):
        cmd, key_num, key_str, data, expiration = c
        if cmd[0] == 'g' or cmd[0] == 'q':
            log.debug(cmd + ' ' + key_str + '\r')
            return False
        if cmd[0] == 'd':
            log.debug('delete ' + key_str + '\r')
            return False

        c = 'set'
        if cmd[0] == 'a':
            c = self.arpa[self.cur.get('cur-sets', 0) % len(self.arpa)]

        log.debug("%s %s 0 %s %s\r\n%s\r" % (c, key_str, expiration,
                                             len(data), data))
        return False

    def flush(self):
        pass

    def num_ops(self, cur):
        return cur.get('cur-gets', 0) + cur.get('cur-sets', 0)

    def gen_doc(self, key_num, key_str, min_value_size, json=None, cache=None):
        if json is None:
            json = self.cfg.get('json', 1) > 0
        if cache is None:
            cache = self.cfg.get('doc-cache', 0)

        return gen_doc_string(key_num, key_str, min_value_size,
                              self.cfg['suffix'][min_value_size],
                              json, cache=cache)

    def cmd_line_get(self, key_num, key_str):
        return key_str

    def readbytes(self, skt, nbytes, buf):
        while len(buf) < nbytes:
            data = None
            try:
                data = skt.recv(max(nbytes - len(buf), 4096))
            except Exception as error:
                self.save_error(error)
                log.error(error)
            if not data:
                self.save_error("no data")
                log.error("no data")
                return None, ""
            buf += data
        return buf[:nbytes], buf[nbytes:]

    def add_timing_sample(self, cmd, delta, prefix="latency-"):
        base = prefix + cmd
        for suffix in self.cfg.get("timing-suffixes", ["", "-recent"]):
            key = base + suffix
            histo = self.cur.get(key, None)
            if histo is None:
                histo = {}
                self.cur[key] = histo
            try:
                bucket = round(self.histo_bucket(delta), 6)
                histo[bucket] = histo.get(bucket, 0) + 1
            except TypeError as error:
                self.save_error(error)
                log.error(error)

    def histo_bucket(self, samp):
        hp = self.cfg.get("histo-precision", 2)
        if samp > 0:
            p = 10 ** (math.floor(math.log10(samp)) - (hp - 1))
            r = round(samp // p)
            return r * p

    def drange(self, start, stop, step):
        r = start
        while r < stop:
            yield round(float(r), 6)
            r += float(step)


class StoreMemcachedBinary(Store):

    def connect(self, target, user, pswd, cfg, cur, bucket="default", backups=None):
        self.cfg = cfg
        self.cur = cur
        self.target = target
        self.host_port = (target + ":11211").rsplit(':', 1)[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.host_port = self.host_port[0].rsplit(':')[0:3]
        self.connect_host_port(self.host_port[0], self.host_port[1], user, pswd, bucket=bucket)
        self.inflight_reinit()
        self.queue = []
        self.cmds = 0
        self.ops = 0
        self.previous_ops = 0
        self.buf = ''
        self.arpa = [(CMD_ADD,     True),
                     (CMD_REPLACE, True),
                     (CMD_APPEND,  False),
                     (CMD_PREPEND, False)]
        self.xfer_sent = 0
        self.xfer_recv = 0
        self.obs_key_cas = {}  # {key_num: cas} pair
        self.woq_key_cas = {}  # {key_num: cas} pair
        self.cor_key_cas = {}  # {key_num: cas} pair
        self.retries = 0
        self.backups = backups
        self.bucket = bucket
        self.user = user
        self.pswd = pswd

    def reconnect(self):
        if self.backups:
            self.target = self.backups[0]
            self.backups.pop(0)

        log.info("StoreMemcachedBinary: reconnect to %s" % self.target)
        self.host_port = (self.target + ":11211").rsplit(':', 0)[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.connect_host_port(self.host_port[0], self.host_port[1],
                               self.user, self.pswd, bucket=self.bucket)

    def connect_host_port(self, host, port, user, pswd, bucket="default"):
        self.conn = mc_bin_client.MemcachedClient(host, port)
        if user and bucket != "default":
            self.conn.sasl_auth_plain(user, pswd)

    def inflight_reinit(self, inflight=0):
        self.inflight = inflight
        self.inflight_num_gets = 0
        self.inflight_num_sets = 0
        self.inflight_num_deletes = 0
        self.inflight_num_arpas = 0
        self.inflight_num_queries = 0
        self.inflight_start_time = 0
        self.inflight_end_time = 0
        self.inflight_grp = None

    def inflight_start(self):
        return []

    def inflight_complete(self, inflight_arr):
        return ''.join(inflight_arr)

    def inflight_send(self, inflight_msg):
        try:
            self.conn.s.sendall(inflight_msg)
        except socket.error as error:
            self.retries += 1
            self.save_error(error)
            log.error("%s, retries = %s", error, self.retries)
            if self.retries == RETRIES:
                e = ServerUnavailableException(self.host_port)
                self.reconnect()
                self.retries = 0
                raise e
            time.sleep(0.2)
            return 0
        return len(inflight_msg)

    def inflight_recv(self, inflight, inflight_arr, expectBuffer=None):
        received = 0
        for i in range(inflight):
            try:
                cmd, keylen, extralen, errcode, datalen, opaque, val, buf = \
                    self.recvMsg()
            except Exception as error:
                self.retries += 1
                self.save_error(error)
                log.error("%s, retries = %s", error, self.retries)
                if self.retries == RETRIES:
                    e = ServerUnavailableException(self.host_port)
                    self.reconnect()
                    self.retries = 0
                    raise e
                time.sleep(0.2)
                return received
            received += datalen + MIN_RECV_PACKET
        return received

    def inflight_append_buffer(self, grp, vbucketId, opcode, opaque):
        return grp

    def command(self, c):
        self.queue.append(c)
        if len(self.queue) <= self.flush_level():
            return False

        try:
            self.flush()
            return True
        except ServerUnavailableException as error:
            self.save_error(error)
            log.error(error)
            self.queue = list()
            self.inflight_reinit()
            return False

    def flush_level(self):
        return self.cur.get('batch') or self.cfg.get('batch', 100)

    def get_vbucketId(self, key):
        vbuckets = self.cfg.get("vbuckets", 0)
        if vbuckets > 0:
            return crc32.crc32_hash(key) & (vbuckets - 1)
        return 0

    def header(self, op, key, val, opaque=0, extra='', cas=0,
               dtype=0,
               fmt=REQ_PKT_FMT,
               magic=REQ_MAGIC_BYTE):
        vbucketId = self.get_vbucketId(key)
        return struct.pack(fmt, magic, op,
                           len(key), len(extra), dtype, vbucketId,
                           len(key) + len(extra) + len(val), opaque, cas), vbucketId

    def create_seed(self):
        """Return a seed (hashable tuple or int value) based on current stats.
        This seed ensures reproducible randomness for the same test
        configurations.

        """

        if self.why == 'loop-fg':
            return self.cur.get('cur-queries', 0)
        else:
            return (self.cur.get('cur-gets', 0),
                    self.cur.get('cur-sets', 0),
                    self.cur.get('cur-deletes', 0),
                    self.cur.get('cur-creates', 0),
                    self.cur.get('cur-arpas', 0))

    def flush(self):
        next_inflight = 0
        next_inflight_num_gets = 0
        next_inflight_num_sets = 0
        next_inflight_num_deletes = 0
        next_inflight_num_arpas = 0
        next_inflight_num_queries = 0

        next_grp = self.inflight_start()

        # Permutation of requests
        random.seed(self.create_seed())
        random.shuffle(self.queue)

        # Start a 1, not 0, due to the single latency measurement request.
        for i in range(1, len(self.queue)):
            cmd, key_num, key_str, data, expiration = self.queue[i]
            delta_gets, delta_sets, delta_deletes, delta_arpas, delta_queries = \
                self.cmd_append(cmd, key_num, key_str, data, expiration, next_grp)
            next_inflight += 1
            next_inflight_num_gets += delta_gets
            next_inflight_num_sets += delta_sets
            next_inflight_num_deletes += delta_deletes
            next_inflight_num_arpas += delta_arpas
            next_inflight_num_queries += delta_queries

        next_msg = self.inflight_complete(next_grp)

        latency_cmd = None
        latency_start = 0
        latency_end = 0

        if self.inflight > 0:
            # Receive replies from the previous batch of inflight requests.
            self.xfer_recv += self.inflight_recv(self.inflight, self.inflight_grp)
            self.inflight_end_time = time.time()
            self.ops += self.inflight
            if self.sc:
                self.sc.ops_stats({'tot-gets':    self.inflight_num_gets,
                                   'tot-sets':    self.inflight_num_sets,
                                   'tot-deletes': self.inflight_num_deletes,
                                   'tot-arpas':   self.inflight_num_arpas,
                                   'tot-queries': self.inflight_num_queries,
                                   'start-time':  self.inflight_start_time,
                                   'end-time':    self.inflight_end_time})

        if len(self.queue) > 0:
            # Use the first request in the batch to measure single
            # request latency.
            grp = self.inflight_start()
            latency_cmd, key_num, key_str, data, expiration = self.queue[0]
            self.cmd_append(latency_cmd, key_num, key_str, data, expiration, grp)
            msg = self.inflight_complete(grp)

            latency_start = time.time()
            self.xfer_sent += self.inflight_send(msg)
            self.xfer_recv += self.inflight_recv(1, grp, expectBuffer=False)
            latency_end = time.time()

            self.ops += 1

        self.queue = []

        self.inflight_reinit()
        if next_inflight > 0:
            self.inflight = next_inflight
            self.inflight_num_gets = next_inflight_num_gets
            self.inflight_num_sets = next_inflight_num_sets
            self.inflight_num_deletes = next_inflight_num_deletes
            self.inflight_num_arpas = next_inflight_num_arpas
            self.inflight_num_queries = next_inflight_num_queries
            self.inflight_start_time = time.time()
            self.inflight_grp = next_grp
            self.xfer_sent += self.inflight_send(next_msg)

        if latency_cmd:
            delta = latency_end - latency_start
            self.add_timing_sample(latency_cmd, delta)

        if self.sc:
            if self.ops - self.previous_ops > self.stats_ops:
                self.previous_ops = self.ops
                self.save_stats()
                log.debug("%s save_stats : %s" % (self.why, latency_cmd))

    def save_stats(self, cur_time=0):
        for key in self.cur:
            if key.startswith('latency-'):
                histo = self.cur.get(key, None)
                if histo:
                    self.sc.latency_stats(key, histo, cur_time)
                    if key.endswith('-recent'):
                        self.cur[key] = {}
        self.sc.sample(self.cur)

    def cmd_append(self, cmd, key_num, key_str, data, expiration, grp):
        self.cmds += 1
        if cmd[0] == 'g' or cmd[0] == 'q':
            hdr, vbucketId = self.header(CMD_GET, key_str, '', opaque=self.cmds)
            m = self.inflight_append_buffer(grp, vbucketId, CMD_GET, self.cmds)
            m.append(hdr)
            m.append(key_str)
            return 1, 0, 0, 0, 0
        elif cmd[0] == 'd':
            hdr, vbucketId = self.header(CMD_DELETE, key_str, '', opaque=self.cmds)
            m = self.inflight_append_buffer(grp, vbucketId, CMD_DELETE, self.cmds)
            m.append(hdr)
            m.append(key_str)
            return 0, 0, 1, 0, 0

        rv = (0, 1, 0, 0, 0)
        curr_cmd = CMD_SET
        curr_extra = struct.pack(SET_PKT_FMT, 0, expiration)

        if cmd[0] == 'a':
            rv = (0, 0, 0, 1, 0)
            curr_cmd, have_extra = self.arpa[self.cur.get('cur-sets', 0) % len(self.arpa)]
            if not have_extra:
                curr_extra = ''

        hdr, vbucketId = self.header(curr_cmd, key_str, data,
                                     extra=curr_extra, opaque=key_num)
        m = self.inflight_append_buffer(grp, vbucketId, curr_cmd, self.cmds)
        m.append(hdr)
        if curr_extra:
            m.append(curr_extra)
        m.append(key_str)
        m.append(data)
        return rv

    def num_ops(self, cur):
        return self.ops

    def recvMsg(self):
        sock = self.conn.s
        buf = self.buf
        pkt, buf = self.readbytes(sock, MIN_RECV_PACKET, buf)
        magic, cmd, keylen, extralen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(RES_PKT_FMT, pkt)
        if magic != RES_MAGIC_BYTE:
            raise Exception("Unexpected recvMsg magic: " + str(magic))
        val, buf = self.readbytes(sock, datalen, buf)
        self.buf = buf
        if not self.obs_key_cas and cmd == CMD_SET:
            self.obs_key_cas[opaque] = cas  # opaque is the key_num
        if not self.woq_key_cas and cmd == CMD_SET:
            self.woq_key_cas[opaque] = cas
        if "cor" in self.cfg and cmd == CMD_SET:
            self.cor_key_cas[opaque] = cas
        return cmd, keylen, extralen, errcode, datalen, opaque, val, buf


class StoreMembaseBinary(StoreMemcachedBinary):

    def connect_host_port(self, host, port, user, pswd, bucket="default"):
        """
        Connect to the server host using REST API.
        Username and password should be rest_username and rest_password, \
        generally they are different from ssh identities.
        """
        from lib.memcached.helper.data_helper import VBucketAwareMemcached

        info = {"ip": host, "port": port,
                'username': user or self.cfg.get("rest_username", "Administrator"),
                'password': pswd or self.cfg.get("rest_password", "password")}

        self.rest = RestConnection(info)
        self.awareness = VBucketAwareMemcached(self.rest, bucket, info)
        self.backoff = 0
        self.xfer_sent = 0
        self.xfer_recv = 0

    def flush_level(self):
        f = StoreMemcachedBinary.flush_level(self)
        return f * len(self.awareness.memcacheds)

    def inflight_start(self):
        return {
            's_bufs': {},  # Key is server str, value is [] of buffer.
            's_cmds': {}   # Key is server str, value is int (number of cmds).
        }

    def inflight_complete(self, inflight_grp):
        rv = []  # Array of tuples (server, buffer).
        s_bufs = inflight_grp['s_bufs']
        for server in list(s_bufs.keys()):
            buffers = s_bufs[server]
            rv.append((server, ''.join(buffers)))
        return rv

    def inflight_send(self, inflight_msg):
        """
        If timeout value is 0,
        blocks until everything been sent out \
        or the connection breaks.
        """
        timeout_sec = self.cfg.get("socket-timeout", 0)

        sent_total = 0
        for server, buf in inflight_msg:

            length = len(buf)
            if length == 0:
                continue

            sent_tuple = 0   # byte sent out per tuple in inflight_msg
            while sent_tuple < length:
                try:
                    conn = self.awareness.memcacheds[server]
                    if timeout_sec > 0:
                        conn.s.settimeout(timeout_sec)
                    sent = conn.s.send(buf)
                    if sent == 0:
                        self.save_error("socket.send returned 0")
                        log.error("socket.send returned 0")
                        break
                    sent_tuple += sent
                except socket.timeout:
                    self.save_error("socket timeout")
                    log.error("socket timeout")
                    break
                except Exception as error:
                    self.save_error(error)
                    log.error(error)
                    break

            sent_total += sent_tuple
        return sent_total

    def inflight_recv(self, inflight, inflight_grp, expectBuffer=None):
        received = 0
        s_cmds = inflight_grp['s_cmds']
        reset_my_awareness = False
        backoff = False

        for server in list(s_cmds.keys()):
            try:
                conn = self.awareness.memcacheds[server]
                try:
                    recvBuf = conn.recvBuf
                except AttributeError:
                    recvBuf = ''
                if expectBuffer == False and recvBuf != '':
                    raise Exception("Was expecting empty buffer, but have (" +
                                    str(len(recvBuf)) + "): " + recvBuf)
                cmds = s_cmds[server]
                for i in range(cmds):
                    try:
                        rcmd, keylen, extralen, errcode, datalen, ropaque, val, recvBuf = \
                            self.recvMsgSockBuf(conn.s, recvBuf)
                        received += datalen + MIN_RECV_PACKET
                        if errcode == ERR_NOT_MY_VBUCKET:
                            reset_my_awareness = True
                        elif errcode == ERR_ENOMEM or \
                                errcode == ERR_EBUSY or \
                                errcode == ERR_ETMPFAIL:
                            backoff = True
                            self.save_error("errorcode = %s" % errcode)
                            if errcode == ERR_ENOMEM:
                                log.error("errorcode = ENOMEM")
                            # Don't log backoffs due to ETMPFAIL/EBUSY
                    except Exception as error:
                        self.save_error(error)
                        log.error(error)
                        reset_my_awareness = True
                        backoff = True
                conn.recvBuf = recvBuf
            except Exception as error:
                self.save_error(error)
                log.error(error)
                reset_my_awareness = True
                backoff = True

        if backoff:
            self.backoff = max(self.backoff, 0.1) * \
                self.cfg.get('backoff-factor', 2.0)
            if self.backoff > 0:
                self.cur['cur-backoffs'] = self.cur.get('cur-backoffs', 0) + 1
                log.info("inflight recv backoff = %s" % self.backoff)
                time.sleep(self.backoff)
        else:
            self.backoff = 0

        if reset_my_awareness:
            try:
                self.awareness.reset()
            except Exception as error:
                self.save_error("awareness.reset: {0}".format(error))
                log.error("awareness.reset: %s", error)

        return received

    def recvMsgSockBuf(self, sock, buf):
        pkt, buf = self.readbytes(sock, MIN_RECV_PACKET, buf)
        magic, cmd, keylen, extralen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(RES_PKT_FMT, pkt)
        if magic != RES_MAGIC_BYTE:
            raise Exception("Unexpected recvMsg magic: " + str(magic))
        if not self.obs_key_cas and cmd == CMD_SET:
            self.obs_key_cas[opaque] = cas  # opaque is the key_num
        if not self.woq_key_cas and cmd == CMD_SET:
            self.woq_key_cas[opaque] = cas  # opaque is the key_num
        if "cor" in self.cfg and cmd == CMD_SET:
            self.cor_key_cas[opaque] = cas  # opaque is the key_num
        val, buf = self.readbytes(sock, datalen, buf)
        return cmd, keylen, extralen, errcode, datalen, opaque, val, buf

    def inflight_append_buffer(self, grp, vbucketId, opcode, opaque):
        s_bufs = grp['s_bufs']
        s_cmds = grp['s_cmds']
        s = self.awareness.vBucketMap[vbucketId]
        m = s_bufs.get(s, None)
        if m is None:
            m = []
            s_bufs[s] = m
            s_cmds[s] = 0
        s_cmds[s] += 1
        return m


class StoreMemcachedAscii(Store):

    def connect(self, target, user, pswd, cfg, cur, bucket="default", backups=None):
        self.cfg = cfg
        self.cur = cur
        self.target = target
        self.host_port = (target + ":11211").rsplit(':', 1)[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.skt.connect(tuple(self.host_port))
        self.queue = []
        self.ops = 0
        self.previous_ops = 0
        self.buf = ''
        self.arpa = ['add', 'replace', 'append', 'prepend']
        self.xfer_sent = 0
        self.xfer_recv = 0

    def command(self, c):
        self.queue.append(c)
        if len(self.queue) > (self.cur.get('batch') or
                              self.cfg.get('batch', 100)):
            self.flush()
            return True
        return False

    def command_send(self, cmd, key_num, key_str, data, expiration):
        if cmd[0] == 'g' or cmd[0] == 'q':
            return cmd + ' ' + key_str + '\r\n'
        if cmd[0] == 'd':
            return 'delete ' + key_str + '\r\n'

        c = 'set'
        if cmd[0] == 'a':
            c = self.arpa[self.cur.get('cur-sets', 0) % len(self.arpa)]
        return "%s %s 0 %s %s\r\n%s\r\n" % (c, key_str, expiration,
                                            len(data), data)

    def command_recv(self, cmd, key_num, key_str, data, expiration):
        buf = self.buf
        if cmd[0] == 'g' or cmd[0] == 'q':
            # GET...
            line, buf = self.readline(self.skt, buf)
            while line and line != 'END':
                # line == "VALUE k flags len"
                rvalue, rkey, rflags, rlen = line.split()
                data, buf = self.readbytes(self.skt, int(rlen) + 2, buf)
                line, buf = self.readline(self.skt, buf)
        elif cmd[0] == 'd':
            # DELETE...
            line, buf = self.readline(self.skt, buf)  # line == "DELETED"
        else:
            # SET...
            line, buf = self.readline(self.skt, buf)  # line == "STORED"
        self.buf = buf

    def flush(self):
        m = []
        for c in self.queue:
            cmd, key_num, key_str, data, expiration = c
            m.append(self.command_send(cmd, key_num, key_str, data, expiration))

        self.skt.send(''.join(m))

        for c in self.queue:
            cmd, key_num, key_str, data, expiration = c
            self.command_recv(cmd, key_num, key_str, data, expiration)

        self.ops += len(self.queue)
        self.queue = []

    def num_ops(self, cur):
        return self.ops

    def readline(self, skt, buf):
        while True:
            index = buf.find('\r\n')
            if index >= 0:
                break
            data = skt.recv(4096)
            if not data:
                return '', ''
            buf += data
        return buf[:index], buf[index + 2:]

# --------------------------------------------------------

# A key is a 16 char hex string.


def key_to_name(key_str):
    return "%s %s" % (key_str[-16:-12], key_str[-4:-1])


def key_to_email(key_str):
    return "%s@%s.com" % (key_str[-16:-12], key_str[-13:-11])


def key_to_city(key_str):
    return key_str[-12:-9]


def key_to_country(key_str):
    return key_str[-9:-7]


def key_to_realm(key_str):
    return key_str[-7:-5]


def key_to_coins(key_str):
    sub_key = key_str[-16:]
    return max(0.0, int(sub_key[0:4], 16) / 100.0)


def key_to_category(key_str):
    return int(key_str[-12], 16) % 3


def key_to_achievements(key_str):
    next = 300
    achievements = []
    sub_key = key_str[-16:]
    for i in range(len(sub_key)):
        next = (next + int(sub_key[i], 16) * i) % 500
        if next < 256:
            achievements.append(next)
    return achievements

doc_cache = {}


def gen_doc_string(key_num, key_str, min_value_size, suffix, json,
                   cache=None, key_name="key", suffix_ex="", whitespace=True):
    global doc_cache

    c = "{"
    if not json:
        c = "*"

    d = None
    if cache:
        d = doc_cache.get(key_num, None)

    if d is None:
        d = """"%s":"%s",
 "key_num":%s,
 "name":"%s",
 "email":"%s",
 "city":"%s",
 "country":"%s",
 "realm":"%s",
 "coins":%s,
 "category":%s,
 "achievements":%s,""" % (key_name, key_str,
                          key_num,
                          key_to_name(key_str),
                          key_to_email(key_str),
                          key_to_city(key_str),
                          key_to_country(key_str),
                          key_to_realm(key_str),
                          key_to_coins(key_str),
                          key_to_category(key_str),
                          key_to_achievements(key_str))
        if not whitespace:
            d = d.replace("\n ", "")
        if cache:
            doc_cache[key_num] = d

    return "%s%s%s%s" % (c, d, suffix_ex, suffix)

# --------------------------------------------------------

PROTOCOL_STORE = {'memcached-ascii': StoreMemcachedAscii,
                  'memcached-binary': StoreMemcachedBinary,
                  'membase-binary': StoreMembaseBinary,
                  'none-binary': Store,
                  'none': Store}


def final_report(cur, store, total_time):
    """Report final stats"""
    log.info(dict_to_s(cur))
    if cur.get('cur-queries', 0):
        total_cmds = cur.get('cur-queries', 0)
    else:
        total_cmds = cur.get('cur-gets', 0) + cur.get('cur-sets', 0)
    log.info("ops/sec: %s" % (total_cmds / float(total_time)))
    if store.errors:
        log.warn("errors:\n%s", json.dumps(store.errors, indent=4))


def run(cfg, cur, protocol, host_port, user, pswd, stats_collector=None,
        stores=None, ctl=None, heartbeat=0, why="", bucket="default", backups=None, collection=None):
    if isinstance(cfg['min-value-size'], str):
        cfg['min-value-size'] = string.split(cfg['min-value-size'], ",")
    if not isinstance(cfg['min-value-size'], list):
        cfg['min-value-size'] = [cfg['min-value-size']]

    cfg['body'] = {}
    cfg['suffix'] = {}

    for i in range(len(cfg['min-value-size'])):
        mvs = int(cfg['min-value-size'][i])
        cfg['min-value-size'][i] = mvs
        cfg['body'][mvs] = 'x'
        while len(cfg['body'][mvs]) < mvs:
            cfg['body'][mvs] = cfg['body'][mvs] + \
                md5(str(len(cfg['body'][mvs]))).hexdigest()
        cfg['suffix'][mvs] = "\"body\":\"" + cfg['body'][mvs] + "\"}"

    ctl = ctl or {'run_ok': True}

    threads = []

    for i in range(cfg.get('threads', 1)):
        store = None
        if stores and i < len(stores):
            store = stores[i]

        if store is None:
            store = PROTOCOL_STORE[protocol]()

        log.debug("store: %s - %s" % (i, store.__class__))

        store.connect(host_port, user, pswd, cfg, cur, bucket=bucket, backups=backups)
        store.stats_collector(stats_collector)

        threads.append(threading.Thread(target=run_worker,
                                        args=(ctl, cfg, cur, store,
                                              "thread-" + str(i) + ": ")))

    store.show_some_keys()

    if cfg.get("doc-cache", 0) > 0 and cfg.get("doc-gen", 0) > 0:
        min_value_size = cfg['min-value-size'][0]
        json = cfg.get('json', 1) > 0
        cache = cfg.get('doc-cache', 0)
        log.debug("doc-gen...")
        gen_start = time.time()
        for key_num in range(cfg.get("max-items", 0)):
            key_str = prepare_key(key_num, cfg.get('prefix', ''))
            store.gen_doc(key_num, key_str, min_value_size, json, cache)
        gen_end = time.time()
        log.debug("doc-gen...done (elapsed: %s, docs/sec: %s)" %
                 (gen_end - gen_start,
                  float(key_num) / (gen_end - gen_start)))

    def stop_after(secs):
        time.sleep(secs)
        log.info("exiting because of stop_after time")
        ctl['run_ok'] = False

    if cfg.get('time', 0) > 0:
        t = threading.Thread(target=stop_after, args=(cfg.get('time', 0),))
        t.daemon = True
        t.start()

    t_start = time.time()

    try:
        if len(threads) <= 1:
            run_worker(ctl, cfg, cur, store, "", heartbeat, why)
        else:
            for thread in threads:
                thread.daemon = True
                thread.start()

            while threads:
                threads[0].join(1)
                threads = [t for t in threads if t.isAlive()]
    except KeyboardInterrupt:
        log.warn("exiting because of KeyboardInterrupt")
        ctl['run_ok'] = False

    t_end = time.time()

    final_report(cur, store, total_time=t_end - t_start)

    threads = [t for t in threads if t.isAlive()]
    heartbeat = 0
    while threads:
        threads[0].join(1)
        heartbeat += 1
        if heartbeat >= 60:
            heartbeat = 0
            log.info("mcsoda is running with %s threads" % len(threads))
        threads = [t for t in threads if t.isAlive()]

    ctl['run_ok'] = False
    if ctl.get('shutdown_event') is not None:
        ctl['shutdown_event'].set()

    log.info("%s stopped running." % why)
    return cur, t_start, t_end

# --------------------------------------------------------


def main(argv, cfg_defaults=None, cur_defaults=None, protocol=None, stores=None,
         extra_examples=None):
    cfg_defaults = cfg_defaults or {
        "prefix":             ("",    "Prefix for every item key."),
        "max-ops":            (0,     "Max # of ops before exiting. 0 means keep going."),
        "max-items":          (-1,    "Max # of items; default 100000."),
        "max-creates":        (-1,    "Max # of creates; defaults to max-items."),
        "min-value-size":     ("10",  "Min value size (bytes) for SET's; comma-separated."),
        "ratio-sets":         (0.1,   "Fraction of requests that should be SET's."),
        "ratio-creates":      (0.1,   "Fraction of SET's that should create new items."),
        "ratio-misses":       (0.05,  "Fraction of GET's that should miss."),
        "ratio-hot":          (0.2,   "Fraction of items to have as a hot item subset."),
        "ratio-hot-sets":     (0.95,  "Fraction of SET's that hit the hot item subset."),
        "ratio-hot-gets":     (0.95,  "Fraction of GET's that hit the hot item subset."),
        "ratio-deletes":      (0.0,   "Fraction of SET updates that shold be DELETE's."),
        "ratio-arpas":        (0.0,   "Fraction of SET non-DELETE'S to be 'a-r-p-a' cmds."),
        "ratio-expirations":  (0.0,   "Fraction of SET's that use the provided expiration."),
        "ratio-queries":      (0.0,   "Fraction of GET hits that should be queries."),
        "expiration":         (0,     "Expiration time parameter for SET's"),
        "exit-after-creates": (0,     "Exit after max-creates is reached."),
        "threads":            (1,     "Number of client worker threads to use."),
        "batch":              (100,   "Batch/pipeline up this # of commands per server."),
        "json":               (1,     "Use JSON documents. 0 to generate binary documents."),
        "time":               (0,     "Stop after this many seconds if > 0."),
        "max-ops-per-sec":    (0,     "When >0, max ops/second target performance."),
        "report":             (40000, "Emit performance output after this many requests."),
        "histo-precision":    (1,     "Precision of histogram bins."),
        "vbuckets":           (0,     "When >0, vbucket hash in memcached-binary protocol."),
        "doc-cache":          (1,     "When 1, cache docs; faster, but uses O(N) memory."),
        "doc-gen":            (1,     "When 1 and doc-cache, pre-generate docs at start."),
        "backoff-factor":     (2.0,   "Exponential backoff factor on ETMPFAIL errors."),
        "hot-shift":          (0,     "# of keys/sec that hot item subset should shift."),
        "random":             (0,     "When 1, use random keys for gets and updates."),
        "queries":            ("",    "Query templates; semicolon-separated."),
        "socket-timeout":     (0,     "Used for socket.settimeout(), in seconds.")}

    cur_defaults = cur_defaults or {
        "cur-items":    (0, "Number of items known to already exist."),
        "cur-sets":     (0, "Number of sets already done."),
        "cur-creates":  (0, "Number of sets that were creates."),
        "cur-gets":     (0, "Number of gets already done."),
        "cur-deletes":  (0, "Number of deletes already done."),
        "cur-arpas":    (0, "# of add/replace/prepend/append's (a-r-p-a) cmds."),
        "cur-queries":  (0, "Number of gets that were view/index queries."),
        "cur-base":     (0, "Base of numeric key range. 0 by default.")}

    if len(argv) < 2 or "-h" in argv or "--help" in argv:
        print(("usage: %s [memcached[-binary|-ascii]://][user[:pswd]@]host[:port] [key=val]*\n" %
              (argv[0])))
        print("  default protocol = memcached-binary://")
        print("  default port     = 11211\n")
        examples = ["examples:",
                    "  %s membase://127.0.0.1:8091 max-items=1000000 json=1",
                    "  %s memcached://127.0.0.1:11210 vbuckets=1024",
                    "  %s memcached://127.0.0.1:11211",
                    "  %s memcached-ascii://127.0.0.1:11211",
                    "  %s memcached-binary://127.0.0.1:11211",
                    "  %s 127.0.0.1:11211",
                    "  %s 127.0.0.1",
                    "  %s my-test-bucket@127.0.0.1",
                    "  %s my-test-bucket:MyPassword@127.0.0.1",
                    "  %s none://"]
        if extra_examples:
            examples = examples + extra_examples
        for s in examples:
            if s.find("%s") > 0:
                print((s % (argv[0])))
            else:
                print(s)
        print("")
        print("optional key=val's and their defaults:")
        for d in [cfg_defaults, cur_defaults]:
            for k in sorted(d.keys()):
                print(("  %s = %s %s" %
                      (string.ljust(k, 18), string.ljust(str(d[k][0]), 5), d[k][1])))
        print("")
        print("  TIP: min-value-size can be comma-separated values: min-value-size=10,256,1024")
        print("")
        sys.exit(-1)

    cfg = {}
    cur = {}
    err = {}

    for (o, d) in [(cfg, cfg_defaults), (cur, cur_defaults)]:  # Parse key=val pairs.
        for (dk, dv) in d.items():
            o[dk] = dv[0]
        for kv in argv[2:]:
            s = (kv + '=').split('=')[0:-1]
            k = s[0]
            v = '='.join(s[1:])
            if k and v and k in o:
                if not isinstance(o[k], str):
                    try:
                        v = ({'y': '1', 'n': '0'}).get(v, v)
                        for parse in [float, int]:
                            if str(parse(v)) == v:
                                v = parse(v)
                    except:
                        err[kv] = err.get(kv, 0) + 1
                o[k] = v
            else:
                err[kv] = err.get(kv, 0) + 1

    for kv in err:
        if err[kv] > 1:
            log.error("problem parsing key=val option: " + kv)
    for kv in err:
        if err[kv] > 1:
            sys.exit(-1)

    if cfg.get('max-items', 0) < 0 and cfg.get('max-creates', 0) < 0:
        cfg['max-items'] = 100000
    if cfg.get('max-items', 0) < 0:
        cfg['max-items'] = cfg.get('max-creates', 0)
    if cfg.get('max-creates', 0) < 0:
        cfg['max-creates'] = cfg.get('max-items', 0)

    for o in [cfg, cur]:
        for k in sorted(o.keys()):
            log.info("    %s = %s" % (string.ljust(k, 20), o[k]))

    protocol = protocol or '-'.join(((["memcached"] +
                                    argv[1].split("://"))[-2] + "-binary").split('-')[0:2])
    host_port = ('@' + argv[1].split("://")[-1]).split('@')[-1] + ":11211"
    user, pswd = (('@' + argv[1].split("://")[-1]).split('@')[-2] + ":").split(':')[0:2]

    cfg["timing-suffixes"] = [""]

    run(cfg, cur, protocol, host_port, user, pswd, stores=stores)

if __name__ == "__main__":
    main(sys.argv)
