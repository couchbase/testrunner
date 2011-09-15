#!/usr/bin/env python

import os
import sys
import math
import time
import socket
import string
import struct
import threading

sys.path.append("lib")

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

import mc_bin_client
import memcacheConstants

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, CMD_GET, CMD_SET

cfg_defaults = {
  "prefix":             ("",   "Prefix for every item key."),
  "max-ops":            (0,    "Max number of ops before exiting. 0 means keep going."),
  "max-items":          (-1,   "Max number of items; default 100000."),
  "max-creates":        (-1,   "Max number of creates; defaults to max-items."),
  "min-value-size":     (10,   "Minimal value size in bytes during SET's."),
  "ratio-sets":         (0.1,  "Fraction of requests that should be SET's."),
  "ratio-creates":      (0.1,  "Fraction of SET's that should create new items."),
  "ratio-misses":       (0.05, "Fraction of GET's that should miss."),
  "ratio-hot":          (0.2,  "Fraction of items to have as a hot item subset."),
  "ratio-hot-sets":     (0.95, "Fraction of SET's that hit the hot item subset."),
  "ratio-hot-gets":     (0.95, "Fraction of GET's that hit the hot item subset."),
  "exit-after-creates": (0,    "Exit after max-creates is reached."),
  "threads":            (1,    "Number of client worker threads to use."),
  "batch":              (100,  "Batch / pipeline up this number of commands."),
  "json":               (1,    "Use JSON documents. 0 to generate binary documents.")
}

cur_defaults = {
  "cur-items":   (0, "Number of items known to already exist."),
  "cur-sets":    (0, "Number of sets already done."),
  "cur-creates": (0, "Number of sets that were creates."),
  "cur-gets":    (0, "Number of gets already done.")
}

if len(sys.argv) < 2 or "-h" in sys.argv or "--help" in sys.argv:
    print("usage: %s [memcached[-binary|-ascii]://][user[:pswd]@]host[:port] [key=val]*\n" %
          (sys.argv[0]))
    print("  default protocol = memcached-binary://")
    print("  default port     = 11211\n")
    for s in ["examples: %s memcached-binary://127.0.0.1:11211 max-items=1000000 json=1",
              "          %s memcached://127.0.0.1:11211",
              "          %s 127.0.0.1:11211",
              "          %s 127.0.0.1",
              "          %s my-test-bucket@127.0.0.1",
              "          %s my-test-bucket:MyPassword@127.0.0.1"]:
       print(s % (sys.argv[0]))
    print("")
    print("optional key=val's and their defaults:")
    for d in [cfg_defaults, cur_defaults]:
        for k in sorted(d.iterkeys()):
            print("    %s = %s %s" %
                  (string.ljust(k, 20), string.ljust(str(d[k][0]), 4), d[k][1]))
    sys.exit(-1)

cfg = {}
cur = {}
err = {}

for (o, d) in [(cfg, cfg_defaults), (cur, cur_defaults)]: # Parse key=val pairs.
   for (dk, dv) in d.iteritems():
      o[dk] = dv[0]
   for kv in sys.argv[2:]:
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

if cfg['max-items'] < 0 and cfg['max-creates'] < 0:
    cfg['max-items'] = 100000
if cfg['max-items'] < 0:
    cfg['max-items'] = cfg['max-creates']
if cfg['max-creates'] < 0:
    cfg['max-creates'] = cfg['max-items']

for o in [cfg, cur]:
    for k in sorted(o.iterkeys()):
        log.info("    %s = %s" % (string.ljust(k, 20), o[k]))

# --------------------------------------------------------

body = 'x'
while len(body) < cfg['min-value-size']:
    body = body + md5(str(len(body))).hexdigest()
suffix = "\"body\":\"" + body + "\"}"

def gen_doc_obj(key_num, key_str, min_value_size):
    return { "_id": key_str,
             "key_num": key_num,
             "mid": key_str[-8:-1],
             "last": key_str[-1:],
             "body": body
           }

def gen_doc_string(key_num, key_str, min_value_size, key_name="key", json=True):
    c = "{"
    if not json:
        c = "*"
    s = """%s"%s":"%s", "key_num":%s, "mid":"%s", "last":"%s", %s"""
    return s % (c, key_name, key_str, key_num,
                key_str[-8:-1], key_str[-1:], suffix)

# --------------------------------------------------------

run_ok = True

def run(cfg, cur, store, prefix=""):
    i = 0
    t_last = time.time()
    o_last = store.num_ops(cur)

    while run_ok:
        num_ops = cur['cur-gets'] + cur['cur-sets']

        if cfg['max-ops'] > 0 and cfg['max-ops'] <= num_ops:
            break
        if cfg['exit-after-creates'] > 0 and \
           cfg['max-creates'] > 0 and \
           cfg['max-creates'] <= cur['cur-creates']:
            break

        store.command(next_cmd(cfg, cur, store))
        i += 1

        if i % 100000 == 0:
            t_curr = time.time()
            o_curr = store.num_ops(cur)
            log.info(prefix + str(cur))
            log.info("%s    ops: %s secs: %s ops/sec: %s" %
                  (prefix,
                   string.ljust(str(o_curr - o_last), 10),
                   string.ljust(str(t_curr - t_last), 15),
                   (o_curr - o_last) / (t_curr - t_last)))
            t_last = t_curr
            o_last = o_curr

    store.flush()

def next_cmd(cfg, cur, store):
    # for i in range(100):
    #     print(long("0x" + md5(str(i)).hexdigest(), 16) & 0xFFFFFFFF)
    #
    num_ops = cur['cur-gets'] + cur['cur-sets']

    do_set = cfg['ratio-sets'] > float(cur['cur-sets']) / positive(num_ops)
    if do_set:
        cmd = 'set'
        cur['cur-sets'] += 1

        do_set_create = (cfg['max-items'] > cur['cur-items'] and
                         cfg['max-creates'] > cur['cur-creates'] and
                         cfg['ratio-creates'] > \
                           float(cur['cur-creates']) / positive(cur['cur-sets']))
        if do_set_create:
            # Create...
            key_num = cur['cur-items']

            cur['cur-items'] += 1
            cur['cur-creates'] += 1
        else:
            # Update...
            key_num = choose_key_num(cur['cur-items'],
                                     cfg['ratio-hot'],
                                     cfg['ratio-hot-sets'],
                                     cur['cur-sets'])

        key_str = prepare_key(key_num, cfg['prefix'])
        itm_val = store.gen_doc(key_num, key_str, cfg['min-value-size'])

        return (cmd, key_num, key_str, itm_val)
    else:
        cmd = 'get'
        cur['cur-gets'] += 1

        do_get_hit = (cfg['ratio-misses'] * 100) < (cur['cur-gets'] % 100)
        if do_get_hit:
            key_num = choose_key_num(cur['cur-items'],
                                     cfg['ratio-hot'],
                                     cfg['ratio-hot-gets'],
                                     cur['cur-gets'])
            key_str = prepare_key(key_num, cfg['prefix'])
            itm_val = store.gen_doc(key_num, key_str, cfg['min-value-size'])

            return (cmd, key_num, key_str, itm_val)
        else:
            return (cmd, -1, prepare_key(-1, cfg['prefix']), None)

def choose_key_num(num_items, ratio_hot, ratio_hot_choice, num_ops):
    hit_hot_range = (ratio_hot_choice * 100) > (num_ops % 100)
    if hit_hot_range:
        base  = 0
        range = math.floor(ratio_hot * num_items)
    else:
        base  = math.floor(ratio_hot * num_items)
        range = math.floor((1.0 - ratio_hot) * num_items)

    return base + (num_ops % positive(range))

def prepare_key(key_num, prefix):
    key_hash = md5(str(key_num)).hexdigest()[0:16]
    if prefix and len(prefix) > 0:
        return prefix + "-" + key_hash
    return key_hash

def positive(x):
    if x > 0:
        return x
    return 1

# --------------------------------------------------------

class Store:

    def connect(self, target, user, pswd, cfg):
        pass

    def command(self, c):
        log.info("%s %s %s %s" % c)

    def flush(self):
        pass

    def num_ops(self, cur):
        return cur['cur-gets'] + cur['cur-sets']

    def gen_doc(self, key_num, key_str, min_value_size):
        return gen_doc_string(key_num, key_str, min_value_size)

    def cmd_line_get(self, key_num, key_str):
        return key_str

    def readbytes(self, skt, nbytes, buf):
        while len(buf) < nbytes:
            data = skt.recv(max(nbytes - len(buf), 4096))
            if not data:
                return None, ''
            buf += data
        return buf[:nbytes], buf[nbytes:]


class StoreMemcachedBinary(Store):

    def connect(self, target, user, pswd, cfg):
        self.cfg = cfg
        self.target = target
        self.host_port = (target + ":11211").split(':')[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.conn = mc_bin_client.MemcachedClient(self.host_port[0],
                                                  self.host_port[1])
        if user:
           self.conn.sasl_auth_plain(user, pswd)
        self.queue = []
        self.ops = 0
        self.buf = ''

    def command(self, c):
        self.queue.append(c)
        if len(self.queue) > self.cfg['batch']:
            self.flush()

    def header(self, op, key, val, opaque=0, extra='', cas=0,
               dtype=0, vbucketId=0,
               fmt=REQ_PKT_FMT,
               magic=REQ_MAGIC_BYTE):
        return struct.pack(fmt, magic, op,
                           len(key), len(extra), dtype, vbucketId,
                           len(key) + len(extra) + len(val), opaque, cas)

    def flush(self):
        extra = struct.pack(SET_PKT_FMT, 0, 0)

        m = []
        for c in self.queue:
            cmd, key_num, key_str, data = c
            if cmd[0] == 'g':
                m.append(self.header(CMD_GET, key_str, ''))
                m.append(key_str)
            else:
                m.append(self.header(CMD_SET, key_str, data, extra=extra))
                m.append(extra)
                m.append(key_str)
                m.append(data)
        self.conn.s.send(''.join(m))

        for c in self.queue:
            self.recvMsg()

        self.ops += len(self.queue)
        self.queue = []

    def num_ops(self, cur):
        return self.ops

    def recvMsg(self):
        buf = self.buf
        pkt, buf = self.readbytes(self.conn.s, MIN_RECV_PACKET, buf)
        magic, cmd, keylen, extralen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(RES_PKT_FMT, pkt)
        val, buf = self.readbytes(self.conn.s, datalen, buf)
        self.buf = buf


class StoreMemcachedAscii(Store):

    def connect(self, target, user, pswd, cfg):
        self.cfg = cfg
        self.target = target
        self.host_port = (target + ":11211").split(':')[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.skt.connect(tuple(self.host_port))
        self.queue = []
        self.ops = 0
        self.buf = ''

    def command(self, c):
        self.queue.append(c)
        if len(self.queue) > self.cfg['batch']:
            self.flush()

    def command_send(self, cmd, key_num, key_str, data):
        if cmd[0] == 'g':
            return 'get ' + key_str + '\r\n'
        return "set %s 0 0 %s\r\n%s\r\n" % (key_str, len(data), data)

    def command_recv(self, cmd, key_num, key_str, data):
        buf = self.buf
        if cmd[0] == 'g':
            # GET...
            line, buf = self.readline(self.skt, buf)
            while line and line != 'END':
                # line == "VALUE k flags len"
                rvalue, rkey, rflags, rlen = line.split()
                data, buf = self.readbytes(self.skt, int(rlen) + 2, buf)
                line, buf = self.readline(self.skt, buf)
        else:
            # SET...
            line, buf = self.readline(self.skt, buf) # line == "STORED"
        self.buf = buf

    def flush(self):
        m = []
        for c in self.queue:
            cmd, key_num, key_str, data = c
            m.append(self.command_send(cmd, key_num, key_str, data))
        self.skt.send(''.join(m))

        for c in self.queue:
            cmd, key_num, key_str, data = c
            self.command_recv(cmd, key_num, key_str, data)

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

protocol = (["memcached"] + sys.argv[1].split("://"))[-2] + "-binary"
host_port = ('@' + sys.argv[1].split("://")[-1]).split('@')[-1] + ":11211"
user, pswd = (('@' + sys.argv[1].split("://")[-1]).split('@')[-2] + ":").split(':')[0:2]

threads = []

for i in range(cfg['threads']):
    store = Store()
    if protocol.split('-')[0].find('memcache') >= 0:
        if protocol.split('-')[1] == 'ascii':
            store = StoreMemcachedAscii()
        else:
            store = StoreMemcachedBinary()

    store.connect(host_port, user, pswd, cfg)

    threads.append(threading.Thread(target=run, args=(cfg, cur, store,
                                                      "thread-" + str(i) + ": ")))

log.info("first 5 keys...")
for i in range(5):
    print("echo get %s | nc %s %s" %
          (store.cmd_line_get(i, prepare_key(i, cfg['prefix'])),
           host_port.split(':')[0],
           host_port.split(':')[1]))

t_start = time.time()

try:
    if len(threads) <= 1:
        run(cfg, cur, store)
    else:
       for thread in threads:
           thread.daemon = True
           thread.start()

       while len(threads) > 0:
           threads[0].join(1)
           threads = [t for t in threads if t.isAlive()]
except KeyboardInterrupt:
   run_ok = False

t_end = time.time()

log.info("\n" + str(cur))
log.info("    ops/sec: %s" %
      ((cur['cur-gets'] + cur['cur-sets']) / (t_end - t_start)))

