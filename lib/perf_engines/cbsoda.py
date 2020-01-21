#!/usr/bin/env python

import re
import sys
import socket
import threading
from . import mcsoda
import errno

sys.path.append("lib")
sys.path.append(".")

try:
   import logging
   logging.config.fileConfig("mcsoda.logging.conf")
   log = logging.getLogger()
except:
   class P:
      def error(self, m): print(m)
      def info(self, m):  print(m)
   log = P()


class Reader(threading.Thread):
    def __init__(self, src, reader_go, reader_done, cur, store):

        self.src = src
        self.reader_go = reader_go
        self.reader_done = reader_done
        self.cur = cur
        self.inflight = 0
        self.received = 0
        self.store= store
        threading.Thread.__init__(self)

    def run(self):
        self.reader_go.wait()
        self.reader_go.clear()
        while True:
            try:
                data = self.src.recv(16384)
                if not data:
                    self.store.save_error("no data")
                    self.reader_done.set()
                    self.reader_go = None
                    self.src = None
                    return

                self.received += len(data)

                found = len(re.findall("HTTP", data))
                found += int("P/1.1" in data[:7])

                self.inflight -= found
            except Exception as error:
                self.store.save_error(error)
                log.error(error)
                self.inflight = 0

            if self.inflight == 0:
                self.reader_done.set()
                self.reader_go.wait()
                self.reader_go.clear()


class StoreCouchbase(mcsoda.StoreMembaseBinary):

    def connect_host_port(self, host, port, user, pswd, bucket="default"):
        if not hasattr(self, 'identity'):
            self.identity = (port, user, pswd, bucket)

        super(StoreCouchbase, self).connect_host_port(host, *self.identity)
        self.memcacheds = self.awareness.memcacheds

        self.capi_skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.capi_skt.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # We really need timeout. Otherwise reader hangs up from time to time
        self.capi_skt.settimeout(60)
        if port == 8091:
            self.capi_host_port = (host, 8092)
        else:
            self.capi_host_port = (host, 9500)
        self.capi_skt.connect(self.capi_host_port)
        self.init_reader(self.capi_skt)

    def disconnect(self):
        """Close socket connection"""
        self.capi_skt.shutdown(2)
        self.capi_skt.close()

    def restart_connection(self):
        """Determine new hostname, drop existing connection, establish new
        socket connection."""
        servers = [host_port.split(':')[0] for host_port in self.memcacheds]
        host = servers[self.cfg.get('node_prefix', 0) % len(servers)]

        self.disconnect()
        self.connect_host_port(host, *self.identity)

    def init_reader(self, skt):
        self.reader_go = threading.Event()
        self.reader_done = threading.Event()
        self.reader = Reader(skt, self.reader_go, self.reader_done, self.cur,
                             self)
        self.reader.daemon = True
        self.reader.start()

    def inflight_start(self):
        inflight_grp = super(StoreCouchbase, self).inflight_start()
        inflight_grp['queries'] = [] # Array of queries.
        return inflight_grp

    def inflight_complete(self, inflight_grp):
        arr = super(StoreCouchbase, self).inflight_complete(inflight_grp)
        # Returns tuple of...
        #   (array of tuples (memcached_server, buffer) for memcached sends,
        #    buffer for capi send).
        return arr, ''.join(inflight_grp['queries']), len(inflight_grp['queries'])

    def inflight_send(self, t):
        # Before sending new batch, check number of active nodes. Restart HTTP
        # connection if this number changes.
        if len(self.memcacheds) != len(self.awareness.memcacheds):
            self.memcacheds = self.awareness.memcacheds
            self.restart_connection()

        for_mc, buf_capi, num_capi = t

        sent_mc = super(StoreCouchbase, self).inflight_send(for_mc)

        sent_capi = len(buf_capi)
        if sent_capi > 0:
            try:
                self.reader.inflight += num_capi
                self.capi_skt.send(buf_capi)
            except socket.error as error:
                self.save_error(error)
                log.error(error)
                if isinstance(error.args, tuple):
                    if error[0] == errno.EPIPE:
                        # Remote-end closed the socket - TODO: why does this happen?
                        self.capi_skt.close()
                        self.capi_skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        self.capi_skt.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        self.capi_skt.connect(self.capi_host_port)
                        self.init_reader(self.capi_skt) # TODO: we lose some stats from the old reader?

                        # Resend data
                        self.reader.inflight = num_capi
                        self.capi_skt.send(buf_capi)

        return sent_mc + sent_capi

    def inflight_recv(self, inflight, inflight_grp, expectBuffer=None):
        r = self.reader.received

        num_capi = len(inflight_grp['queries'])
        if num_capi > 0:
            self.reader_go.set()

        received_mc = super(StoreCouchbase, self).inflight_recv(inflight,
                                                                inflight_grp,
                                                                expectBuffer=expectBuffer)

        if num_capi > 0:
            self.reader_done.wait()
            self.reader_done.clear()

        received_capi = self.reader.received - r

        return received_mc + received_capi

    def cmd_append(self, cmd, key_num, key_str, data, expiration, grp):
        if cmd[0] == 'q':
            self.cmds += 1
            m = grp['queries']

            prefix = "GET "
            suffix = " HTTP/1.1\r\n\r\n"

            # A sample queries array might be...
            #
            #   ["/default/_design/my_ddoc/_views/by_email?startkey={email}",
            #    "/default/_design/my_ddoc/_views/by_country?startkey={country}&limit=10"]
            #
            queries = self.cfg.get("queries", "")
            queries = queries.split(';')
            if len(queries) == 0 or len(queries[0]) == 0:
                queries = [ "/default/{key}" ]

            query = queries[self.cur.get("cur-queries", 0) % len(queries)]

            message = query.format(key_str,
                                   key=key_str,
                                   name=mcsoda.key_to_name(key_str).replace(" ", "+"),
                                   email=mcsoda.key_to_email(key_str),
                                   city=mcsoda.key_to_city(key_str),
                                   country=mcsoda.key_to_country(key_str),
                                   realm=mcsoda.key_to_realm(key_str),
                                   coins=mcsoda.key_to_coins(key_str),
                                   category=mcsoda.key_to_category(key_str),
                                   cmds=self.cmds,
                                   int10=self.cmds % 10,
                                   int100=self.cmds % 100,
                                   int1000=self.cmds % 1000)

            m.append(prefix + message + suffix)

            return 0, 0, 0, 0, 1

        return super(StoreCouchbase, self).cmd_append(cmd, key_num, key_str,
                                                      data, expiration, grp)


if __name__ == "__main__":
    extra_examples=["          %s couchbase://127.0.0.1:8092 ratio-queries=0.2",
                    "          %s couchbase://127.0.0.1:8092 ratio-queries=0.2 \\",
                    "               queries=/default/_design/DDOC/_view/by_email?startkey={email}",
                    "",
                    "Available keys for queries templates:",
                    "    key, name, email, city, country, realm, coins, category,",
                    "    cmds (the number of commands sent so far),",
                    "    int10, int100, int1000 (various sized integers)"
                    ]

    if len(sys.argv) >= 2 and \
       (sys.argv[1].find("couchbase") == 0 or \
        sys.argv[1].find("cb") == 0):
        mcsoda.main(sys.argv,
                    protocol="couchbase",
                    stores=[StoreCouchbase()],
                    extra_examples=extra_examples)
    else:
        mcsoda.main(sys.argv,
                    extra_examples=extra_examples)
