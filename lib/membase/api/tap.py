#!/usr/bin/env python
"""
tap protocol client.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""
import builtins as exceptions

import sys
import socket
import string
import random
import struct
import asyncore
import uuid
import mc_bin_server

from memcacheConstants import REQ_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT

import memcacheConstants

class TapConnection(object):
    def __init__(self, server, port, callback, clientId=None, opts={}):
        self.wbuf = self._createTapCall(clientId, opts)
        print(self.wbuf)
        self.callback = callback
        self.identifier = (server.ip, port)
        print('tap connection : {0} {1}'.format(server.ip, port))
        self.s = socket.socket()
        self.s.connect_ex((server.ip, port))
        sent = self.s.send(self.wbuf)
        self.wbuf = self.wbuf[sent:]
        self.rbuf = ""


    def __hasEnoughBytes(self):
        rv = False
        if len(self.rbuf) >= memcacheConstants.MIN_RECV_PACKET:
            magic, cmd, keylen, extralen, datatype, vb, remaining, opaque, cas =\
            struct.unpack(REQ_PKT_FMT, self.rbuf[:memcacheConstants.MIN_RECV_PACKET])
            rv = len(self.rbuf) - memcacheConstants.MIN_RECV_PACKET >= remaining
        return rv

    def receive(self):
        BUFFER_SIZE = 4096
        self.s.settimeout(10)
        try:
            self.rbuf += self.s.recv(BUFFER_SIZE)
        except:
            return
        while self.__hasEnoughBytes():
            magic, cmd, keylen, extralen, datatype, vb, remaining, opaque, cas =\
            struct.unpack(REQ_PKT_FMT, self.rbuf[:memcacheConstants.MIN_RECV_PACKET])
            assert magic == REQ_MAGIC_BYTE
            assert keylen <= remaining, "Keylen is too big: %d > %d"\
            % (keylen, remaining)
            assert extralen == memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0),\
            "Extralen is too large for cmd 0x%x: %d" % (cmd, extralen)
            # Grab the data section of this request
            data = self.rbuf[memcacheConstants.MIN_RECV_PACKET:memcacheConstants.MIN_RECV_PACKET + remaining]
            assert len(data) == remaining
            # Remove this request from the read buffer
            self.rbuf = self.rbuf[memcacheConstants.MIN_RECV_PACKET + remaining:]
            # Process the command
            cmdVal = self.processCommand(cmd, keylen, vb, extralen, cas, data)
            # Queue the response to the client if applicable.
            if cmdVal:
                try:
                    status, cas, response = cmdVal
                except ValueError:
                    print("Got", cmdVal)
                    raise
                dtype = 0
                extralen = memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0)
                self.wbuf += struct.pack(memcacheConstants.RES_PKT_FMT,
                                         memcacheConstants.RES_MAGIC_BYTE, cmd, keylen,
                                         extralen, dtype, status,
                                         len(response), opaque, cas) + response


    def _createTapCall(self, key=None, opts={}):
        # Client identifier
        if not key:
            key = "".join(random.sample(string.letters, 16))
        dtype = 0
        opaque = 0
        cas = 0

        extraHeader, val = self._encodeOpts(opts)
        print("opts: ", extraHeader, val)

        msg = struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
                          memcacheConstants.CMD_TAP_CONNECT,
                          len(key), len(extraHeader), dtype, 0,
                          len(key) + len(extraHeader) + len(val),
                          opaque, cas)
        return msg + extraHeader + key + val

    def _encodeOpts(self, opts):
        header = 0
        val = []
        for op in sorted(opts.keys()):
            header |= op
            if op in memcacheConstants.TAP_FLAG_TYPES:
                val.append(struct.pack(memcacheConstants.TAP_FLAG_TYPES[op],
                                       opts[op]))
            elif op == memcacheConstants.TAP_FLAG_LIST_VBUCKETS:
                val.append(self._encodeVBucketList(opts[op]))
            else:
                val.append(opts[op])
        return struct.pack(">I", header), ''.join(val)

    def _encodeVBucketList(self, vbl):
        l = list(vbl) # in case it's a generator
        vals = [struct.pack("!H", len(l))]
        for v in vbl:
            vals.append(struct.pack("!H", v))
        return ''.join(vals)

    def processCommand(self, cmd, klen, vb, extralen, cas, data):
        extra = data[0:extralen]
        key = data[extralen:(extralen + klen)]
        val = data[(extralen + klen):]
        self.callback(self.identifier, cmd, extra, key, vb, val, cas)

    def handle_connect(self):
        print("connected...")


    def handle_close(self):
        print("handle_close")
        self.close()


class TapClient(object):
    def __init__(self, servers, callback, opts={}):
        for t in servers:
            tc = TapConnection(t.host, t.port, callback, t.id, opts)


def buildGoodSet(goodChars=string.printable, badChar='?'):
    """Build a translation table that turns all characters not in goodChars
    to badChar"""
    allChars = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff'
    badchars = str.maketrans(allChars, allChars, goodChars)
    badchars1=str.translate(allChars,badchars)
    rv = str.maketrans(badchars1, badChar * len(badchars1))
    return rv


class TapDescriptor(object):
    port = 11210
    id = None

    def __init__(self, server):
        self.host = server.ip
        self.port = 11210
        self.id = str(uuid.uuid4())

# Build a translation table that includes only characters
transt = buildGoodSet()

def abbrev(v, maxlen=30):
    if len(v) > maxlen:
        return v[:maxlen] + "..."
    else:
        return v


def keyprint(v):
    return string.translate(abbrev(v), transt)

#def mainLoop(serverList, cb, opts={}):
#    """Run the given callback for each tap message from any of the
#    upstream servers.
#
#    loops until all connections drop
#    """
#
#    connections = (TapDescriptor(a) for a in serverList)
#    TapClient(connections, cb, opts=opts)
#    asyncore.loop()

#if __name__ == '__main__':
#    def cb(identifier, cmd, extra, key, vb, val, cas):
#        print "%s: ``%s'' (vb:%d) -> ``%s'' (%d bytes from %s)" % (
#            memcacheConstants.COMMAND_NAMES[cmd],
#            key, vb, keyprint(val), len(val), identifier)
#
#    # This is an example opts parameter to do future-only tap:
#    opts = {memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff}
#    # If you omit it, or supply a past time_t value for backfill, it
#    # will get all data.
#    opts = {}
#
#    mainLoop(sys.argv[1:], cb, opts)
