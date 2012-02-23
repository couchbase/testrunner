#!/usr/bin/env python

import re
import sys
import copy
import math
import time
import socket
import threading
import mcsoda

from membase.api.rest_client import RestConnection

class Reader(threading.Thread):
    def __init__(self, src, reader_go, reader_done):
        self.src = src
        self.reader_go = reader_go
        self.reader_done = reader_done
        self.inflight = 0
        self.received = 0
        threading.Thread.__init__(self)

    def run(self):
        self.reader_go.wait()
        self.reader_go.clear()
        while True:
            data = self.src.recv(4096)
            if not data:
                break

            self.received += len(data)

            found = len(re.findall("HTTP/1.1 ", data))

            self.inflight -= found
            if self.inflight == 0:
                self.reader_done.set()
                self.reader_go.wait()
                self.reader_go.clear()


class StoreCouchbase(mcsoda.StoreMembaseBinary):

    def connect_host_port(self, host, port, user, pswd):
        mcsoda.StoreMembaseBinary.connect_host_port(self, host, port, user, pswd)

        self.capi_host_port = (host, 8092)
        self.capi_skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.capi_skt.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.capi_skt.connect(self.capi_host_port)

        self.reader_go = threading.Event()
        self.reader_done = threading.Event()
        self.reader = Reader(self.capi_skt, self.reader_go, self.reader_done)
        self.reader.daemon = True
        self.reader.start()

    def inflight_start(self):
        inflight_grp = mcsoda.StoreMembaseBinary.inflight_start(self)
        inflight_grp['queries'] = [] # Array of queries.
        return inflight_grp

    def inflight_complete(self, inflight_grp):
        arr = mcsoda.StoreMembaseBinary.inflight_complete(self, inflight_grp)
        # Returns tuple of...
        #   (array of tuples (memcached_server, buffer) for memcached sends,
        #    buffer for capi send).
        return arr, ''.join(inflight_grp['queries']), len(inflight_grp['queries'])

    def inflight_send(self, t):
        for_mc, buf_capi, num_capi = t

        sent_mc = mcsoda.StoreMembaseBinary.inflight_send(self, for_mc)

        self.reader.inflight += num_capi

        sent_capi = len(buf_capi)
        if sent_capi > 0:
            self.capi_skt.send(buf_capi)

        return sent_mc + sent_capi

    def inflight_recv(self, inflight, inflight_grp, expectBuffer=None):
        r = self.reader.received

        num_capi = len(inflight_grp['queries'])
        if num_capi > 0:
            self.reader_go.set()

        received_mc = mcsoda.StoreMembaseBinary.inflight_recv(self,
                                                              inflight, inflight_grp,
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

            # TODO: Do different kinds of view accesses here.
            m.append("GET /default/" + key_str + " HTTP/1.1\r\n\r\n")

            return 0, 0, 0, 0, 1

        return mcsoda.StoreMembaseBinary.cmd_append(self, cmd, key_num, key_str,
                                                    data, expiration, grp)


if __name__ == "__main__":
    extra_examples=["          %s couchbase://127.0.0.1:8091 ratio-queries=0.2"]

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
