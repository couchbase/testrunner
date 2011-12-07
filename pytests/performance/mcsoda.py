#!/usr/bin/env python

import re
import os
import sys
import math
import time
import heapq
import socket
import string
import struct
import threading

sys.path.append("lib")
sys.path.append(".")

try:
   import logger
   log = logger.new_logger("mcsoda")
except:
   class P:
      def error(self, m): print(m)
      def info(self, m):  print(m)
   log = P()

try:
   from hashlib import md5
except ImportError:
   from md5 import md5

import crc32
import mc_bin_client
import memcacheConstants

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import ERR_NOT_MY_VBUCKET, ERR_ENOMEM, ERR_EBUSY, ERR_ETMPFAIL
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, CMD_GET, CMD_SET, CMD_DELETE
from memcacheConstants import CMD_ADD, CMD_REPLACE, CMD_PREPEND, CMD_APPEND # "ARPA"

# --------------------------------------------------------

INT_TYPE = type(123)
FLOAT_TYPE = type(0.1)
DICT_TYPE = type({})

def dict_to_s(d, level="", res=None, suffix=", ", ljust=None):
   res = res or []
   return ''.join(dict_to_s_inner(d, level, res, suffix, ljust))

def dict_to_s_inner(d, level, res, suffix, ljust):
   dtype = DICT_TYPE
   scalars = []
   complex = []

   for key in d.keys():
      if type(d[key]) == dtype:
         complex.append(key)
      else:
         scalars.append(key)
   scalars.sort()
   complex.sort()

   # Special case for histogram output.
   histo_max = 0
   histo_sum = 0
   if scalars and not complex and \
      type(scalars[0]) == FLOAT_TYPE and type(d[scalars[0]]) == INT_TYPE:
      for key in scalars:
         v = d[key]
         histo_max = max(v, histo_max)
         histo_sum = histo_sum + v

   histo_cur = 0 # Running total for histogram output.
   for key in scalars:
      if type(key) == FLOAT_TYPE:
         k = re.sub("0*$", "", "%.7f" % (key))
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
      res.append(level   + str(key) + ":\n")
      dict_to_s_inner(d[key], level + "  ", res, "\n", 9)

   return res

# The histo dict is returned by add_timing_sample().
# The percentiles must be sorted, ascending, like [0.90, 0.99].
def histo_percentile(histo, percentiles):
   v_sum = 0
   bins = histo.keys()
   bins.sort()
   for bin in bins:
      v_sum += histo[bin]
   v_sum = float(v_sum)
   v_cur = 0 # Running total.
   rv = []
   for bin in bins:
      if not percentiles:
         return rv
      v_cur += histo[bin]
      while percentiles and (v_cur / v_sum) >= percentiles[0]:
         rv.append((percentiles[0], bin))
         percentiles.pop(0)
   return rv

# --------------------------------------------------------

MIN_VALUE_SIZE = [10]

def run_worker(ctl, cfg, cur, store, prefix):
    i = 0
    t_last_flush = time.time()
    o_last_flush = store.num_ops(cur)
    t_last = time.time()
    o_last = store.num_ops(cur)
    ops_per_sec_prev = []

    report = cfg.get('report', 0)
    hot_shift = cfg.get('hot-shift', 0)
    max_ops_per_sec = cfg.get('max-ops-per-sec', 0)

    if cfg.get('max-ops-per-sec', 0) > 0 and not 'batch' in cur:
       cur['batch'] = 10

    while ctl.get('run_ok', True):
        num_ops = cur.get('cur-gets', 0) + cur.get('cur-sets', 0)

        if cfg.get('max-ops', 0) > 0 and cfg.get('max-ops', 0) <= num_ops:
            break
        if cfg.get('exit-after-creates', 0) > 0 and \
           cfg.get('max-creates', 0) > 0 and \
           cfg.get('max-creates', 0) <= cur.get('cur-creates', 0):
            break

        flushed = store.command(next_cmd(cfg, cur, store))
        i += 1

        if report > 0 and i % report == 0:
            t_curr = time.time()
            o_curr = store.num_ops(cur)

            t_delta = t_curr - t_last
            o_delta = o_curr - o_last

            ops_per_sec = o_delta / t_delta
            log.info(prefix + dict_to_s(cur))
            log.info("%s    ops: %s secs: %s ops/sec: %s" %
                     (prefix,
                      string.ljust(str(o_delta), 10),
                      string.ljust(str(t_delta), 15),
                      ops_per_sec))
            t_last = t_curr
            o_last = o_curr

            ops_per_sec_prev.append(ops_per_sec)
            while len(ops_per_sec_prev) > 10:
               ops_per_sec_prev.pop(0)

            max_ops_per_sec = cfg.get('max-ops-per-sec', 0)
            if max_ops_per_sec > 0 and len(ops_per_sec_prev) >= 2:
               # TODO: Do something clever here to prevent going over
               # the max-ops-per-sec.
               pass

        if flushed:
           t_curr_flush = time.time()
           o_curr_flush = store.num_ops(cur)

           d = t_curr_flush - t_last_flush

           if hot_shift > 0:
              cur['cur-base'] = cur.get('cur-base', 0) + (hot_shift * d)

           if max_ops_per_sec > 0:
              ops = o_curr_flush - o_last_flush
              ops_per_sec = float(ops) / d
              if ops_per_sec > max_ops_per_sec:
                 s = (float(ops) / float(max_ops_per_sec)) - d
                 time.sleep(s)

           t_last_flush = t_curr_flush
           o_last_flush = o_curr_flush

    store.flush()

def next_cmd(cfg, cur, store):
    itm_val = None
    num_ops = cur.get('cur-gets', 0) + cur.get('cur-sets', 0)

    do_set = cfg.get('ratio-sets', 0) > float(cur.get('cur-sets', 0)) / positive(num_ops)
    if do_set:
        itm_gen = True

        cmd = 'set'
        cur_sets = cur.get('cur-sets', 0) + 1
        cur['cur-sets'] = cur_sets

        do_set_create = ((cfg.get('max-items', 0) <= 0 or
                          cfg.get('max-items', 0) > cur.get('cur-items', 0)) and
                         cfg.get('max-creates', 0) > cur.get('cur-creates', 0) and
                         cfg.get('ratio-creates', 0) > \
                           float(cur.get('cur-creates', 0)) / positive(cur.get('cur-sets', 0)))
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

            key_num = choose_key_num(cur.get('cur-items', 0),
                                     cfg.get('ratio-hot', 0),
                                     cfg.get('ratio-hot-sets', 0),
                                     cur.get('cur-sets', 0),
                                     cur.get('cur-base', 0))

        expiration = 0
        if cmd[0] == 's' and cfg.get('ratio-expirations', 0.0) * 100 > cur_sets % 100:
           expiration = cfg.get('expiration', 0)

        key_str = prepare_key(key_num, cfg.get('prefix', ''))
        if itm_gen:
           itm_val = store.gen_doc(key_num, key_str,
                                   choose_entry(cfg.get('min-value-size', MIN_VALUE_SIZE),
                                                num_ops))

        return (cmd, key_num, key_str, itm_val, expiration)
    else:
        cmd = 'get'
        cur['cur-gets'] = cur.get('cur-gets', 0) + 1

        do_get_hit = (cfg.get('ratio-misses', 0) * 100) <= (cur.get('cur-gets', 0) % 100)
        if do_get_hit:
            key_num = choose_key_num(cur.get('cur-items', 0),
                                     cfg.get('ratio-hot', 0),
                                     cfg.get('ratio-hot-gets', 0),
                                     cur.get('cur-gets', 0),
                                     cur.get('cur-base', 0))
            key_str = prepare_key(key_num, cfg.get('prefix', ''))

            return (cmd, key_num, key_str, itm_val, 0)
        else:
            cur['cur-misses'] = cur.get('cur-misses', 0) + 1
            return (cmd, -1, prepare_key(-1, cfg.get('prefix', '')), None, 0)

def choose_key_num(num_items, ratio_hot, ratio_hot_choice, num_ops, base):
    hit_hot_range = (ratio_hot_choice * 100) > (num_ops % 100)
    if hit_hot_range:
        range = math.floor(ratio_hot * num_items)
    else:
        base  = base + math.floor(ratio_hot * num_items)
        range = math.floor((1.0 - ratio_hot) * num_items)

    return int(base + (num_ops % positive(range)))

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

# --------------------------------------------------------

class Store:

    def connect(self, target, user, pswd, cfg, cur):
        self.cfg = cfg
        self.cur = cur

    def stats_collector(self, sc):
        self.sc = sc

    def command(self, c):
        log.info("%s %s %s %s %s" % c)
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
            data = skt.recv(max(nbytes - len(buf), 4096))
            if not data:
                return None, ''
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
          bucket = round(self.histo_bucket(delta), 6)
          histo[bucket] = histo.get(bucket, 0) + 1

    def histo_bucket(self, samp):
       hp = self.cfg.get("histo-precision", 2)
       if samp > 0:
          p = 10 ** (math.floor(math.log10(samp)) - (hp - 1))
          r = round(samp / p)
          return r * p

    def drange(self, start, stop, step):
        r = start
        while r < stop:
            yield round(float(r), 6)
            r += float(step)


class StoreMemcachedBinary(Store):

    def connect(self, target, user, pswd, cfg, cur):
        self.cfg = cfg
        self.cur = cur
        self.target = target
        self.host_port = (target + ":11211").split(':')[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.connect_host_port(self.host_port[0], self.host_port[1], user, pswd)
        self.inflight_reinit()
        self.queue = []
        self.cmds = 0
        self.ops = 0
        self.previous_ops = 0
        self.buf = ''
        self.arpa = [ (CMD_ADD,     True),
                      (CMD_REPLACE, True),
                      (CMD_APPEND,  False),
                      (CMD_PREPEND, False) ]

    def connect_host_port(self, host, port, user, pswd):
        self.conn = mc_bin_client.MemcachedClient(host, port)
        if user:
           self.conn.sasl_auth_plain(user, pswd)

    def inflight_reinit(self, inflight=0):
        self.inflight = inflight
        self.inflight_num_gets = 0
        self.inflight_num_sets = 0
        self.inflight_num_deletes = 0
        self.inflight_num_arpas = 0
        self.inflight_start_time = 0
        self.inflight_end_time = 0
        self.inflight_grp = None

    def inflight_start(self):
        return []

    def inflight_complete(self, inflight_arr):
        return ''.join(inflight_arr)

    def inflight_send(self, inflight_msg):
        self.conn.s.send(inflight_msg)

    def inflight_recv(self, inflight, inflight_arr, expectBuffer=None):
       for i in range(inflight):
          self.recvMsg()

    def inflight_append_buffer(self, grp, vbucketId, opcode, opaque):
        return grp

    def command(self, c):
        self.queue.append(c)
        if len(self.queue) > self.flush_level():
            self.flush()
            return True
        return False

    def flush_level(self):
        return self.cur.get('batch') or \
               self.cfg.get('batch', 100)

    def header(self, op, key, val, opaque=0, extra='', cas=0,
               dtype=0, vbucketId=0,
               fmt=REQ_PKT_FMT,
               magic=REQ_MAGIC_BYTE):
        vbuckets = self.cfg.get("vbuckets", 0)
        if vbuckets > 0:
           vbucketId = crc32.crc32_hash(key) & (vbuckets - 1)
        return struct.pack(fmt, magic, op,
                           len(key), len(extra), dtype, vbucketId,
                           len(key) + len(extra) + len(val), opaque, cas), vbucketId

    def flush(self):
        next_inflight = 0
        next_inflight_num_gets = 0
        next_inflight_num_sets = 0
        next_inflight_num_deletes = 0
        next_inflight_num_arpas = 0

        next_grp = self.inflight_start()

        i = 1 # Start a 1, not 0, due to the single latency measurement request.
        n = len(self.queue)
        while i < n:
            cmd, key_num, key_str, data, expiration = self.queue[i]
            delta_gets, delta_sets, delta_deletes, delta_arpas = \
                self.cmd_append(cmd, key_num, key_str, data, expiration, next_grp)
            next_inflight += 1
            next_inflight_num_gets += delta_gets
            next_inflight_num_sets += delta_sets
            next_inflight_num_deletes += delta_deletes
            next_inflight_num_arpas += delta_arpas
            i = i + 1

        next_msg = self.inflight_complete(next_grp)

        latency_cmd = None
        latency_start = 0
        latency_end = 0

        delta_gets = 0
        delta_sets = 0
        delta_deletes = 0
        delta_arpas = 0

        if self.inflight > 0:
           # Receive replies from the previous batch of infight requests.
           self.inflight_recv(self.inflight, self.inflight_grp)
           self.inflight_end_time = time.time()
           self.ops += self.inflight
           if self.sc:
              self.sc.ops_stats({ 'tot-gets':    self.inflight_num_gets,
                                  'tot-sets':    self.inflight_num_sets,
                                  'tot-deletes': self.inflight_num_deletes,
                                  'tot-arpas':   self.inflight_num_arpas,
                                  'start-time':  self.inflight_start_time,
                                  'end-time':    self.inflight_end_time })

        if len(self.queue) > 0:
           # Use the first request in the batch to measure single
           # request latency.
           grp = self.inflight_start()
           latency_cmd, key_num, key_str, data, expiration = self.queue[0]
           delta_gets, delta_sets, delta_deletes, delta_arpas = \
                self.cmd_append(latency_cmd,
                                key_num, key_str, data, expiration, grp)
           msg = self.inflight_complete(grp)

           latency_start = time.time()
           self.inflight_send(msg)
           self.inflight_recv(1, grp, expectBuffer=False)
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
           self.inflight_start_time = time.time()
           self.inflight_grp = next_grp
           self.inflight_send(next_msg)

        if latency_cmd:
            self.add_timing_sample(latency_cmd, latency_end - latency_start)

        if self.sc:
            if self.ops - self.previous_ops > 10000:
                self.previous_ops = self.ops
                self.save_stats()

    def save_stats(self):
        for key in self.cur:
           if key.startswith('latency-'):
              histo = self.cur.get(key, None)
              if histo:
                 self.sc.latency_stats(key, histo)
                 if key.endswith('-recent'):
                    self.cur[key] = {}
        self.sc.sample(self.cur)

    def cmd_append(self, cmd, key_num, key_str, data, expiration, grp):
       self.cmds += 1
       if cmd[0] == 'g':
          hdr, vbucketId = self.header(CMD_GET, key_str, '', opaque=self.cmds)
          m = self.inflight_append_buffer(grp, vbucketId, CMD_GET, self.cmds)
          m.append(hdr)
          m.append(key_str)
          return 1, 0, 0, 0
       elif cmd[0] == 'd':
          hdr, vbucketId = self.header(CMD_DELETE, key_str, '', opaque=self.cmds)
          m = self.inflight_append_buffer(grp, vbucketId, CMD_DELETE, self.cmds)
          m.append(hdr)
          m.append(key_str)
          return 0, 0, 1, 0

       rv = (0, 1, 0, 0)
       curr_cmd = CMD_SET
       curr_extra = struct.pack(SET_PKT_FMT, 0, expiration)

       if cmd[0] == 'a':
          rv = (0, 0, 0, 1)
          curr_cmd, have_extra = self.arpa[self.cur.get('cur-sets', 0) % len(self.arpa)]
          if not have_extra:
             curr_extra = ''

       hdr, vbucketId = self.header(curr_cmd, key_str, data,
                                    extra=curr_extra, opaque=self.cmds)
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
        return cmd, keylen, extralen, errcode, datalen, opaque, val, buf


class StoreMembaseBinary(StoreMemcachedBinary):

    def connect_host_port(self, host, port, user, pswd):
        from membase.api.rest_client import RestConnection
        from memcached.helper.data_helper import VBucketAwareMemcached
        info = { "ip": host, "port": port,
                 'username': user or 'Administrator',
                 'password': pswd or 'password' }
        rest = RestConnection(info)
        self.awareness = VBucketAwareMemcached(rest, user or 'default', info)
        self.backoff = 0

    def flush_level(self):
        f = StoreMemcachedBinary.flush_level(self)
        return f * len(self.awareness.memcacheds)

    def inflight_start(self):
        return { 's_bufs': {}, # Key is server str, value is [] of buffer.
                 's_cmds': {}  # Key is server str, value is int (number of cmds).
               }

    def inflight_complete(self, inflight_grp):
        rv = [] # Array of tuples (server, buffer).
        s_bufs = inflight_grp['s_bufs']
        for server in s_bufs.keys():
           buffers = s_bufs[server]
           rv.append((server, ''.join(buffers)))
        return rv

    def inflight_send(self, inflight_msg):
        for server, buf in inflight_msg:
           conn = self.awareness.memcacheds[server]
           conn.s.send(buf)

    def inflight_recv(self, inflight, inflight_grp, expectBuffer=None):
        s_cmds = inflight_grp['s_cmds']
        reset_my_awareness = False
        backoff = False
        for server in s_cmds.keys():
           conn = self.awareness.memcacheds[server]
           try:
              recvBuf = conn.recvBuf
           except:
              recvBuf = ''
           if expectBuffer == False and recvBuf != '':
              raise Exception("Was expecting empty buffer, but have (" + \
                                 str(len(recvBuf)) + "): " + recvBuf)
           cmds = s_cmds[server]
           for i in range(cmds):
              rcmd, keylen, extralen, errcode, datalen, ropaque, val, recvBuf = \
                  self.recvMsgSockBuf(conn.s, recvBuf)
              if errcode == ERR_NOT_MY_VBUCKET:
                 reset_my_awareness = True
              elif errcode == ERR_ENOMEM or \
                   errcode == ERR_EBUSY or \
                   errcode == ERR_ETMPFAIL:
                 backoff = True
           conn.recvBuf = recvBuf
        if backoff:
           self.backoff = max(self.backoff, 0.1) * \
                          self.cfg.get('backoff-factor', 2.0)
           if self.backoff > 0:
              self.cur['cur-backoffs'] = self.cur.get('cur-backoffs', 0) + 1
              time.sleep(self.backoff)
        else:
           self.backoff = 0
        if reset_my_awareness:
           self.awareness.reset()

    def recvMsgSockBuf(self, sock, buf):
        pkt, buf = self.readbytes(sock, MIN_RECV_PACKET, buf)
        magic, cmd, keylen, extralen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(RES_PKT_FMT, pkt)
        if magic != RES_MAGIC_BYTE:
           raise Exception("Unexpected recvMsg magic: " + str(magic))
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

    def connect(self, target, user, pswd, cfg, cur):
        self.cfg = cfg
        self.cur = cur
        self.target = target
        self.host_port = (target + ":11211").split(':')[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.skt.connect(tuple(self.host_port))
        self.queue = []
        self.ops = 0
        self.previous_ops = 0
        self.buf = ''
        self.arpa = [ 'add', 'replace', 'append', 'prepend' ]

    def command(self, c):
        self.queue.append(c)
        if len(self.queue) > (self.cur.get('batch') or \
                              self.cfg.get('batch', 100)):
            self.flush()
            return True
        return False

    def command_send(self, cmd, key_num, key_str, data, expiration):
        if cmd[0] == 'g':
            return 'get ' + key_str + '\r\n'
        if cmd[0] == 'd':
            return 'delete ' + key_str + '\r\n'

        c = 'set'
        if cmd[0] == 'a':
           c = self.arpa[self.cur.get('cur-sets', 0) % len(self.arpa)]
        return "%s %s 0 %s %s\r\n%s\r\n" % (c, key_str, expiration,
                                            len(data), data)

    def command_recv(self, cmd, key_num, key_str, data, expiration):
        buf = self.buf
        if cmd[0] == 'g':
            # GET...
            line, buf = self.readline(self.skt, buf)
            while line and line != 'END':
                # line == "VALUE k flags len"
                rvalue, rkey, rflags, rlen = line.split()
                data, buf = self.readbytes(self.skt, int(rlen) + 2, buf)
                line, buf = self.readline(self.skt, buf)
        elif cmd[0] == 'd':
            # DELETE...
            line, buf = self.readline(self.skt, buf) # line == "DELETED"
        else:
            # SET...
            line, buf = self.readline(self.skt, buf) # line == "STORED"
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
        return buf[:index], buf[index+2:]

# --------------------------------------------------------

# A key is a 16 char hex string.
def key_to_name(key_num, key_str):
   return "%s %s" % (key_str[-16:-12], key_str[-4:-1])

def key_to_email(key_num, key_str):
   return "%s@%s.com" % (key_str[-16:-12], key_str[-13:-11])

def key_to_city(key_num, key_str):
   return key_str[-12:-9]

def key_to_country(key_num, key_str):
   return key_str[-9:-7]

def key_to_realm(key_num, key_str):
   return key_str[-7:-5]

def key_to_coins(key_num, key_str):
   sub_key = key_str[-16:]
   return max(0.0, int(sub_key[0:4], 16) / 100.0)

def key_to_achievements(key_num, key_str):
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
                   cache=None, key_name="key"):
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
 "achievements":%s,""" % (key_name, key_str,
                          key_num,
                          key_to_name(key_num, key_str),
                          key_to_email(key_num, key_str),
                          key_to_city(key_num, key_str),
                          key_to_country(key_num, key_str),
                          key_to_realm(key_num, key_str),
                          key_to_coins(key_num, key_str),
                          key_to_achievements(key_num, key_str))
       if cache:
          doc_cache[key_num] = d

    return "%s%s%s" % (c, d, suffix)

# --------------------------------------------------------

PROTOCOL_STORE = { 'memcached-ascii': StoreMemcachedAscii,
                   'memcached-binary': StoreMemcachedBinary,
                   'membase-binary': StoreMembaseBinary}

def run(cfg, cur, protocol, host_port, user, pswd,
        stats_collector = None, stores = None, ctl = None):
   if type(cfg['min-value-size']) == type(""):
       cfg['min-value-size'] = string.split(cfg['min-value-size'], ",")
   if type(cfg['min-value-size']) != type([]):
       cfg['min-value-size'] = [ cfg['min-value-size'] ]

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

   ctl = ctl or { 'run_ok': True }

   threads = []

   for i in range(cfg.get('threads', 1)):
      store = None
      if stores and i < len(stores):
          store = stores[i]

      if store is None:
         store = PROTOCOL_STORE[protocol]()

      log.info("store: %s - %s" % (i, store.__class__))

      store.connect(host_port, user, pswd, cfg, cur)
      store.stats_collector(stats_collector)

      threads.append(threading.Thread(target=run_worker,
                                      args=(ctl, cfg, cur, store,
                                            "thread-" + str(i) + ": ")))

   log.info("first 5 keys...")
   for i in range(5):
      print("echo get %s | nc %s %s" %
            (store.cmd_line_get(i, prepare_key(i, cfg.get('prefix', ''))),
             host_port.split(':')[0],
             host_port.split(':')[1]))

   if cfg.get("doc-cache", 0) > 0 and cfg.get("doc-gen", 0) > 0:
      min_value_size = cfg['min-value-size'][0]
      json = cfg.get('json', 1) > 0
      cache = cfg.get('doc-cache', 0)
      log.info("doc-gen...")
      gen_start = time.time()
      for key_num in range(cfg.get("max-items", 0)):
         key_str = prepare_key(key_num, cfg.get('prefix', ''))
         store.gen_doc(key_num, key_str, min_value_size, json, cache)
      gen_end = time.time()
      log.info("doc-gen...done (elapsed: %s, docs/sec: %s)" % \
                  (gen_end - gen_start,
                   float(key_num) / (gen_end - gen_start)))

   def stop_after(secs):
      time.sleep(secs)
      ctl['run_ok'] = False

   if cfg.get('time', 0) > 0:
      t = threading.Thread(target=stop_after, args=(cfg.get('time', 0),))
      t.daemon = True
      t.start()

   t_start = time.time()

   try:
      if len(threads) <= 1:
         run_worker(ctl, cfg, cur, store, "")
      else:
         for thread in threads:
            thread.daemon = True
            thread.start()

         while len(threads) > 0:
            threads[0].join(1)
            threads = [t for t in threads if t.isAlive()]
   except KeyboardInterrupt:
      ctl['run_ok'] = False

   t_end = time.time()

   log.info("")
   log.info(dict_to_s(cur))
   log.info("    ops/sec: %s" %
            ((cur.get('cur-gets', 0) + cur.get('cur-sets', 0)) / (t_end - t_start)))

   threads = [t for t in threads if t.isAlive()]
   while len(threads) > 0:
      threads[0].join(1)
      threads = [t for t in threads if t.isAlive()]

   return cur, t_start, t_end

# --------------------------------------------------------

def main(argv, cfg_defaults=None, cur_defaults=None, protocol=None, stores=None):
  cfg_defaults = cfg_defaults or {
     "prefix":             ("",    "Prefix for every item key."),
     "max-ops":            (0,     "Max number of ops before exiting. 0 means keep going."),
     "max-items":          (-1,    "Max number of items; default 100000."),
     "max-creates":        (-1,    "Max number of creates; defaults to max-items."),
     "min-value-size":     ("10",  "Minimal value size (bytes) during SET's; comma-separated."),
     "ratio-sets":         (0.1,   "Fraction of requests that should be SET's."),
     "ratio-creates":      (0.1,   "Fraction of SET's that should create new items."),
     "ratio-misses":       (0.05,  "Fraction of GET's that should miss."),
     "ratio-hot":          (0.2,   "Fraction of items to have as a hot item subset."),
     "ratio-hot-sets":     (0.95,  "Fraction of SET's that hit the hot item subset."),
     "ratio-hot-gets":     (0.95,  "Fraction of GET's that hit the hot item subset."),
     "ratio-deletes":      (0.0,   "Fraction of SET updates that should be DELETE's instead."),
     "ratio-arpas":        (0.0,   "Fraction of SET non-DELETE'S to be 'a-r-p-a' cmds."),
     "ratio-expirations":  (0.0,   "Fraction of SET's that use the provided expiration."),
     "expiration":         (0,     "Expiration time parameter for SET's"),
     "exit-after-creates": (0,     "Exit after max-creates is reached."),
     "threads":            (1,     "Number of client worker threads to use."),
     "batch":              (100,   "Batch / pipeline up this number of commands per server."),
     "json":               (1,     "Use JSON documents. 0 to generate binary documents."),
     "time":               (0,     "Stop after this many seconds if > 0."),
     "max-ops-per-sec":    (0,     "When > 0, max ops/second target performance."),
     "report":             (40000, "Emit performance output after this many requests."),
     "histo-precision":    (1,     "Precision of histogram bins."),
     "vbuckets":           (0,     "When > 0, vbucket hash during memcached-binary protocol."),
     "doc-cache":          (1,     "When 1, cache generated docs; faster but uses memory."),
     "doc-gen":            (1,     "When 1 and doc-cache, pre-generate docs before main loop."),
     "backoff-factor":     (2.0,   "Exponential backoff factor on ETMPFAIL errors."),
     "hot-shift":          (0,     "Number of keys/sec that hot item subset should shift.")
     }

  cur_defaults = cur_defaults or {
     "cur-items":    (0, "Number of items known to already exist."),
     "cur-sets":     (0, "Number of sets already done."),
     "cur-creates":  (0, "Number of sets that were creates."),
     "cur-gets":     (0, "Number of gets already done."),
     "cur-deletes":  (0, "Number of deletes already done."),
     "cur-arpas":    (0, "Number of add/replace/prepend/append's (a-r-p-a) commands."),
     "cur-base":     (0, "Base of numeric key range. 0 by default.")
     }

  if len(argv) < 2 or "-h" in argv or "--help" in argv:
     print("usage: %s [memcached[-binary|-ascii]://][user[:pswd]@]host[:port] [key=val]*\n" %
           (argv[0]))
     print("  default protocol = memcached-binary://")
     print("  default port     = 11211\n")
     for s in ["examples: %s memcached-binary://127.0.0.1:11211 max-items=1000000 json=1",
               "          %s memcached://127.0.0.1:11211",
               "          %s 127.0.0.1:11211",
               "          %s 127.0.0.1",
               "          %s my-test-bucket@127.0.0.1",
               "          %s my-test-bucket:MyPassword@127.0.0.1"]:
        print(s % (argv[0]))
     print("")
     print("optional key=val's and their defaults:")
     for d in [cfg_defaults, cur_defaults]:
        for k in sorted(d.iterkeys()):
           print("  %s = %s %s" %
                 (string.ljust(k, 20), string.ljust(str(d[k][0]), 5), d[k][1]))
     print("")
     print("  TIP: min-value-size can be comma-separated values: min-value-size=10,256,1024")
     print("")
     sys.exit(-1)

  cfg = {}
  cur = {}
  err = {}

  for (o, d) in [(cfg, cfg_defaults), (cur, cur_defaults)]: # Parse key=val pairs.
     for (dk, dv) in d.iteritems():
        o[dk] = dv[0]
     for kv in argv[2:]:
        k, v = (kv + '=').split('=')[0:2]
        if k and v and k in o:
           if type(o[k]) != type(""):
              try:
                 v = ({ 'y':'1', 'n':'0' }).get(v, v)
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
     for k in sorted(o.iterkeys()):
        log.info("    %s = %s" % (string.ljust(k, 20), o[k]))

  protocol = protocol or '-'.join(((["memcached"] + \
                                    argv[1].split("://"))[-2] + "-binary").split('-')[0:2])
  host_port = ('@' + argv[1].split("://")[-1]).split('@')[-1] + ":11211"
  user, pswd = (('@' + argv[1].split("://")[-1]).split('@')[-2] + ":").split(':')[0:2]

  cfg["timing-suffixes"] = [""]

  run(cfg, cur, protocol, host_port, user, pswd, stores=stores)


if __name__ == "__main__":
  main(sys.argv)

