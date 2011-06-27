#!/usr/bin/env python
"""
tap protocol client.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""
import exceptions

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
        print self.wbuf
        self.callback = callback
        self.identifier = (server.ip, port)
        print 'tap connection : {0} {1}'.format(server.ip, port)
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
                    print "Got", cmdVal
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
        print "opts: ", extraHeader, val

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
        print "connected..."


    def handle_close(self):
        print "handle_close"
        self.close()


class TapClient(object):
    def __init__(self, servers, callback, opts={}):
        for t in servers:
            tc = TapConnection(t.host, t.port, callback, t.id, opts)


def buildGoodSet(goodChars=string.printable, badChar='?'):
    """Build a translation table that turns all characters not in goodChars
    to badChar"""
    allChars = string.maketrans("", "")
    badchars = string.translate(allChars, allChars, goodChars)
    rv = string.maketrans(badchars, badChar * len(badchars))
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
