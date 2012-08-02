#!/usr/bin/env python

import logging
import socket
from Queue import Queue, Empty as QEmpty, Full as QFull

from lib.perf_engines.sys_helper import SocketHelper
from carbon_msg import CarbonMessage

class CarbonFeeder:

    CARBON_PORT = 2003
    TIMEOUT = 30
    CACHE_SIZE = 10

    def __init__(self, host, port=CARBON_PORT,
                timeout=TIMEOUT, cache_size=CACHE_SIZE):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.cache_size = cache_size
        self.cache = Queue(maxsize=cache_size)
        self._connect()

    def _connect(self):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.skt.connect((self.host, self.port))
        except Exception, e:
            logging.error("could not connect to [%s:%s] : %s",
                self.host, self.host, e)
            self.skt = None
            return False

        return True

    def _flush(self):
        """
        feed all cached messages to carbon server
        """
        if not self.skt and not self._connect():
            return False

        buf = ''
        while not self.cache.empty():
            try:
                c_msg = self.cache.get(block=False)
            except QEmpty:
                continue
            buf += c_msg.pack()

        SocketHelper.send_bytes_ex(self.skt, buf, self.timeout)

    def flush(self):
        self._flush()

    def feed(self, key, value, timestamp=0, caching=True):
        """
        feed a single message to carbon server
        @param: caching - True: add to cache, False: send immediately
        """
        if not self.skt and not self._connect():
            return False

        c_msg = CarbonMessage(key, value, timestamp)
        if caching:
            if self.cache.full():
                self._flush()
            try:
                self.cache.put(c_msg, block=False)
            except QFull:
                SocketHelper.send_bytes_ex(self.skt, c_msg.pack(), self.timeout)
        else:
            SocketHelper.send_bytes_ex(self.skt, c_msg.pack(), self.timeout)