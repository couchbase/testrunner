#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import array
import builtins as exceptions
import hmac
import json
import random
import re
import select
import socket
import struct
import sys
import time
import zlib

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE, ALT_REQ_MAGIC_BYTE, ALT_RES_MAGIC_BYTE, ALT_RES_PKT_FMT
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET, REQ_PKT_SD_EXTRAS, SUBDOC_FLAGS_MKDIR_P
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT, INCRDECR_RES_WITH_UUID_AND_SEQNO_FMT, META_CMD_FMT
from memcacheConstants import TOUCH_PKT_FMT, GAT_PKT_FMT, GETL_PKT_FMT, REQ_PKT_SD_EXTRAS_EXPIRY
from memcacheConstants import COMPACT_DB_PKT_FMT
import memcacheConstants
import logger
def decodeCollectionID(key):
    # A leb128 varint encodes the CID
    data = array.array('B', key)
    cid = data[0] & 0x7f
    end = 1
    if (data[0] & 0x80) == 0x80:
        shift =7
        for end in range(1, len(data)):
            cid |= ((data[end] & 0x7f) << shift)
            if (data[end] & 0x80) == 0:
                break
            shift = shift + 7

        end = end + 1
        if end == len(data):
            #  We should of stopped for a stop byte, not the end of the buffer
            raise exceptions.ValueError("encoded key did not contain a stop byte")
    return cid, key[end:]

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg=None):
        error_msg = error_to_str(status)
        supermsg = 'Memcached error #' + repr(status) + ' ' + repr(error_msg)
        msg = str(msg)
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status = status
        self.msg = msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)

class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211, timeout=30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._createConn()
        self.r = random.Random()
        self.vbucket_count = 1024
        self.feature_flag = set()
        self.features = set()
        self.error_map = None
        self.error_map_version = 1
        self.collection_map = {}
        self.log = logger.Logger.get_logger()
        self.collections_supported = False

    def _createConn(self):
        try:
            # IPv4
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return self.s.connect_ex((self.host, self.port))
        except:
            # IPv6
            self.host = self.host.replace('[', '').replace(']', '')
            self.s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return self.s.connect_ex((self.host, self.port, 0, 0))

    def reconnect(self):
        self.s.close()
        return self._createConn()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()


    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0, collection=None, extended_meta_data='',extraHeaderLength=None):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId, collection=collection, extended_meta_data=extended_meta_data, extraHeaderLength=extraHeaderLength)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=REQ_PKT_FMT, magic=REQ_MAGIC_BYTE, collection=None, extended_meta_data='', extraHeaderLength=None):
        if collection:
            key = self._encodeCollectionId(key, collection)
        # a little bit unfortunate but the delWithMeta command expects the extra data length to be the
        # overall packet length (28 in that case) so we need a facility to support that
        if extraHeaderLength is None:
            extraHeaderLength = len(extraHeader)

        msg = struct.pack(fmt, magic,
            cmd, len(key), extraHeaderLength, dtype, vbucketId,
                len(key) + len(extraHeader) + len(val) + len(extended_meta_data), opaque, cas)
        #self.log.info("--->unpack msg:magic,cmd,keylen,extralen/valelen/extlen,opaque,cas={}".format(struct.unpack(fmt,msg)))
        _, w, _ = select.select([], [self.s], [], self.timeout)
        if w:
            #self.log.info("--->_send:{},{},{},{},{}".format(type(msg),type(extraHeader),type(key),type(val),type(extended_meta_data)))
            #self.log.info("--->_send:{},{},{},{},{}".format(msg,extraHeader,key,val,extended_meta_data))
            try:
              key = key.encode()
            except AttributeError:
              pass

            try:
              extraHeader = extraHeader.encode()
            except AttributeError:
              pass

            try:
              val = val.encode()
            except AttributeError:
              pass

            try:
              extended_meta_data = extended_meta_data.encode()
            except AttributeError:
              pass

            #self.log.info("--->sending..{}".format(msg+extraHeader+key+val+extended_meta_data))
            self.s.send(msg+extraHeader+key+val+extended_meta_data)
        else:
            raise exceptions.EOFError("Timeout waiting for socket send. from {0}".format(self.host))




    def _recvMsg(self):
        response = b""
        while len(response) < MIN_RECV_PACKET:
            r, _, _ = select.select([self.s], [], [], self.timeout)
            if r:
                data = self.s.recv(MIN_RECV_PACKET - len(response))
                if data == '':
                    raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
                response += data
            else:
                raise exceptions.EOFError("Timeout waiting for socket recv. from {0}".format(self.host))

        #self.log.info("-->_recvMsg response={}".format(response))
        #self.log.info("-->_recvMsg response={},{},MIN_RECV_PACKET={}".format(str(response),len(response),MIN_RECV_PACKET))
        assert len(response) == MIN_RECV_PACKET

        # Peek at the magic so we can support alternative-framing
        magic = struct.unpack(b">B", response[0:1])[0]
        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE, ALT_RES_MAGIC_BYTE, ALT_REQ_MAGIC_BYTE)), "Got magic: 0x%x" % magic

        cmd = 0
        frameextralen = 0
        keylen = 0
        extralen = 0
        dtype = 0
        errcode = 0
        remaining = 0
        opaque = 0
        cas = 0
        if magic == ALT_RES_MAGIC_BYTE or magic == ALT_REQ_MAGIC_BYTE:
            magic, cmd, frameextralen, keylen, extralen, dtype, errcode, remaining, opaque, cas = \
                struct.unpack(ALT_RES_PKT_FMT, response)
        else:
            magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas = \
                struct.unpack(RES_PKT_FMT, response[0:MIN_RECV_PACKET])

        rv = b""
        while remaining > 0:
            r, _, _ = select.select([self.s], [], [], self.timeout)
            if r:
                data = self.s.recv(remaining)
                if data == '':
                    raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
                rv += data
                remaining -= len(data)
            else:
                raise exceptions.EOFError("Timeout waiting for socket recv. from {0}".format(self.host))

        return cmd, errcode, opaque, cas, keylen, extralen, dtype, rv, frameextralen

    def _handleKeyedResponse(self, myopaque):
        cmd, errcode, opaque, cas, keylen, extralen, dtype, rv, frameextralen = self._recvMsg()
        assert myopaque is None or opaque == myopaque, \
            "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode != 0:
            if self.error_map is None:
                msg = rv
            else:
                err = self.error_map['errors'].get(errcode, rv)
                msg = "{name} : {desc} : {rv}".format(rv=rv, **err)

            raise MemcachedError(errcode,  msg)
        return cmd, opaque, cas, keylen, extralen, rv, frameextralen

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data, frameextralen = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0, collection=None,extended_meta_data='',extraHeaderLength=None):
        """Send a command and await its response."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas, collection, extended_meta_data=extended_meta_data,
                      extraHeaderLength=extraHeaderLength)
        return self._handleSingleResponse(opaque)

    def _doSdCmd(self, cmd, key, path, val=None, expiry=0, opaque=0, cas=0, create=False, collection=None):
        createFlag = 0
        if opaque == 0:
            opaque = self.r.randint(0, 2**32)
        if create == True:
            createFlag = SUBDOC_FLAGS_MKDIR_P
        if expiry == 0:
            extraHeader = struct.pack(REQ_PKT_SD_EXTRAS, len(path), createFlag)
        else:
            extraHeader = struct.pack(REQ_PKT_SD_EXTRAS_EXPIRY, len(path), createFlag, expiry)
        body = path
        if val != None:
            body += str(val)
        self._sendCmd(cmd, key, body, opaque, extraHeader, cas, collection)
        return self._handleSingleResponse(opaque)

    def _doMultiSdCmd(self, cmd, key, cmdDict, opaque=0, collection=None):
        if opaque == 0:
            opaque = self.r.randint(0, 2**32)
        body = ''
        extraHeader = ''
        mcmd = None
        for k, v  in cmdDict.items():
            if k == "store":
                mcmd = memcacheConstants.CMD_SUBDOC_DICT_ADD
            elif k == "counter":
                mcmd = memcacheConstants.CMD_SUBDOC_COUNTER
            elif k == "add_unique":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_ADD_UNIQUE
            elif k == "push_first":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_FIRST
            elif k == "push_last":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_LAST
            elif k == "array_insert":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_INSERT
            elif k == "insert":
                mcmd = memcacheConstants.CMD_SUBDOC_DICT_ADD
            if v['create_parents'] == True:
                flags = 1
            else:
                flags = 0
            path = v['path']
            valuelen = 0
            if isinstance(v["value"], str):
                value = '"' + v['value'] + '"'
                valuelen = len(value)
            elif isinstance(v["value"], int):
                value = str(v['value'])
                valuelen = len(str(value))
            op_spec = struct.pack(memcacheConstants.REQ_PKT_SD_MULTI_MUTATE, mcmd, flags, len(path), valuelen)
            op_spec += path + value
            body += op_spec
        self._sendMsg(cmd, key, body, opaque, extraHeader, cas=0, collection=collection)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val, collection):
        collection = self.collection_name(collection)
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas, collection)

    def _cat(self, cmd, key, cas, val, collection):
        return self._doCmd(cmd, key, val, '', cas, collection)

    def append(self, key, value, cas=0, vbucket= -1, collection=None):
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value, collection)

    def prepend(self, key, value, cas=0, vbucket= -1, collection=None):
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value, collection)

    def __incrdecr(self, cmd, key, amt, init, exp, collection):
        collection = self.collection_name(collection)
        something, cas, val = self._doCmd(cmd, key, '',
            struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp), collection=collection)
        if len(val) == 8:
           return struct.unpack(INCRDECR_RES_FMT, val)[0], cas
        elif len(val) == 24:
           # new format <vbucket uuid> <seqno> <incr/decr value>
           # for compatibility, putting the uuid and seqno at the end
           res = struct.unpack(INCRDECR_RES_WITH_UUID_AND_SEQNO_FMT, val)
           return res[2], cas, res[0], res[1]


    def incr(self, key, amt=1, init=0, exp=0, vbucket= -1, collection=None):
        collection = self.collection_name(collection)
        """Increment or create the named counter."""
        self._set_vbucket(key, vbucket, collection=collection)
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp, collection)

    def decr(self, key, amt=1, init=0, exp=0, vbucket= -1, collection=None):
        """Decrement or create the named counter."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp, collection)

    def set(self, key, exp, flags, val, vbucket= -1, collection=None):
        """Set a value in the memcached server."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val, collection)

    def pack_the_extended_meta_data(self, adjusted_time, conflict_resolution_mode):
        return struct.pack(memcacheConstants.EXTENDED_META_DATA_FMT,
                    memcacheConstants.EXTENDED_META_DATA_VERSION,
                    memcacheConstants.META_DATA_ID_ADJUSTED_TIME,
                    memcacheConstants.META_DATA_ID_ADJUSTED_TIME_SIZE, adjusted_time,
                    memcacheConstants.META_DATA_ID_CONFLICT_RESOLUTION_MODE,
                    memcacheConstants.META_DATA_ID_CONFLICT_RESOLUTION_MODE_SIZE, conflict_resolution_mode)


    # doMeta - copied from the mc bin client on github
    def _doMetaCmd(self, cmd, key, value, cas, exp, flags, seqno, remote_cas, options, meta_len, collection):
        #extra = struct.pack('>IIQQI', flags, exp, seqno, remote_cas, 0)
        exp = 0
        extra = struct.pack('>IIQQIH', flags, exp, seqno, remote_cas, options, meta_len)

        collection = self.collection_name(collection)
        return self._doCmd(cmd, key, value, extra, cas, collection)

    def setWithMeta(self, key, value, exp, flags, seqno, remote_cas, meta_len, options=2, collection=None):
        """Set a value and its meta data in the memcached server."""
        collection = self.collection_name(collection)
        return self._doMetaCmd(memcacheConstants.CMD_SET_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas, options, meta_len, collection)

    def setWithMetaInvalid(self, key, value, exp, flags, seqno, remote_cas, options=2, collection=None):
        """Set a value with meta that can be an invalid number memcached server."""
        exp = 0
        collection = self.collection_name(collection)
        # 'i' allows for signed integer as remote_cas value
        extra = struct.pack('>IIQiI', flags, exp, seqno, remote_cas, options)
        cmd = memcacheConstants.CMD_SET_WITH_META
        return self._doCmd(cmd, key, value, extra, 0, collection)

    # set with meta using the LWW conflict resolution CAS
    def setWithMetaLWW(self, key, value, exp, flags,cas, collection=None):
        """Set a value and its meta data in the memcached server.
        The format is described here https://github.com/couchbase/ep-engine/blob/master/docs/protocol/set_with_meta.md,
        the first CAS will be 0 because that is the traditional CAS, and the CAS in the "extras" will be populated.
        The sequence number will be 0 because as to the best of my understanding it is not used with LWW.

        """
        #
        SET_META_EXTRA_FMT = '>IIQQH'    # flags (4), expiration (4), seqno (8), CAS (8), metalen (2)
        META_LEN = 0
        SEQNO = 0
        collection = self.collection_name(collection)
        self._set_vbucket(key, -1)

        return self._doCmd(memcacheConstants.CMD_SET_WITH_META, key, value,
                struct.pack(memcacheConstants.META_EXTRA_FMT, flags, exp,  SEQNO, cas, META_LEN), collection=collection)


    # set with meta using the LWW conflict resolution CAS
    def delWithMetaLWW(self, key, exp, flags,cas, collection=None):
        """Set a value and its meta data in the memcached server.
        The format is described here https://github.com/couchbase/ep-engine/blob/master/docs/protocol/del_with_meta.md,
        the first CAS will be 0 because that is the traditional CAS, and the CAS in the "extras" will be populated.
        The sequence number will be 0 because as to the best of my understanding it is not used with LWW.

        """

        META_LEN = 0
        SEQNO = 0

        collection = self.collection_name(collection)
        self._set_vbucket(key, -1, collection=collection)

        return self._doCmd(memcacheConstants.CMD_DEL_WITH_META, key, '',
                struct.pack('>IIQQI', flags, exp,  SEQNO, cas, memcacheConstants.FORCE_ACCEPT_WITH_META_OPS), collection=collection)
                #struct.pack(memcacheConstants.META_EXTRA_FMT, flags, exp,  SEQNO, cas, META_LEN))



    # hope to remove this and migrate existing calls to the aboce
    def set_with_meta(self, key, exp, flags, seqno, cas, val, vbucket= -1, add_extended_meta_data=False,
                      adjusted_time=0, conflict_resolution_mode=0, collection=None):
        """Set a value in the memcached server."""
        self._set_vbucket(key, vbucket, collection=collection)

        collection = self.collection_name(collection)
        return self._doCmd(memcacheConstants.CMD_SET_WITH_META,
            key,
            val,
            struct.pack(memcacheConstants.SKIP_META_CMD_FMT,
                flags,
                exp,
                seqno,
                cas,
                memcacheConstants.CR
            ), collection=collection)


        # Extended meta data was a 4.0 and 4.5 era construct, and not supported in 4.6 Not sure if will ever be needed
        # but leaving the code visible in case it is

        """
        if add_extended_meta_data:
            extended_meta_data =  self.pack_the_extended_meta_data( adjusted_time, conflict_resolution_mode)
            return self._doCmd(memcacheConstants.CMD_SET_WITH_META, key, val,
                   struct.pack(memcacheConstants.EXTENDED_META_CMD_FMT, flags, exp, seqno, cas, len(extended_meta_data)),
                   extended_meta_data=extended_meta_data)
        else:
            return self._doCmd(memcacheConstants.CMD_SET_WITH_META, key, val,
                   struct.pack(META_CMD_FMT, flags, exp, seqno, cas) )
        """



    def del_with_meta(self, key, exp, flags, seqno, cas, vbucket= -1,
                      options=0,
                      add_extended_meta_data=False,
                      adjusted_time=0, conflict_resolution_mode=0, collection=None):
        """Set a value in the memcached server."""
        self._set_vbucket(key, vbucket, collection=collection)
        collection = self.collection_name(collection)
        resp = self._doCmd(memcacheConstants.CMD_DEL_WITH_META, key, '',

                       struct.pack(memcacheConstants.EXTENDED_META_CMD_FMT, flags,
                                   exp, seqno, cas, options, 0), collection=collection )
        return resp



    def hello(self, feature_flag): #, key, exp, flags, val, vbucket= -1):
        resp = self._doCmd(memcacheConstants.CMD_HELLO, '', struct.pack(">H", feature_flag))
        result = struct.unpack('>H', resp[2])
        if result[0] != feature_flag:
            self.log.info("collections not supported")
        else:
            self.collections_supported = True
        supported = resp[2]
        for i in range(0, len(supported), struct.calcsize(">H")):
            self.features.update(
                struct.unpack_from(">H", supported, i))

        if self.is_xerror_supported():
            self.error_map = self.get_error_map()

    def send_set(self, key, exp, flags, val, vbucket= -1, collection=None):
        """Set a value in the memcached server without handling the response"""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_SET, key, val, opaque, struct.pack(SET_PKT_FMT, flags, exp), 0, collection=collection)

    def add(self, key, exp, flags, val, vbucket= -1, collection=None):
        """Add a value in the memcached server iff it doesn't already exist."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val, collection)

    def replace(self, key, exp, flags, val, vbucket= -1, collection=None):
        """Replace a value in the memcached server iff it already exists."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val, collection)

    def observe(self, key, vbucket= -1, collection=None):
        """Observe a key for persistence and replication."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        value = struct.pack('>HH', self.vbucketId, len(key)) + key
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_OBSERVE, '', value, collection=collection)
        rep_time = (cas & 0xFFFFFFFF)
        persist_time = (cas >> 32) & 0xFFFFFFFF
        persisted = struct.unpack('>B', data[4 + len(key)])[0]
        return opaque, rep_time, persist_time, persisted, cas


    def observe_seqno(self, key, vbucket_uuid, vbucket= -1, collection=None):
        """Observe a key for persistence and replication."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)

        value = struct.pack('>Q', vbucket_uuid)
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_OBSERVE_SEQNO, '', value, collection=collection)
        format_type = struct.unpack('>B', data[0])[0]
        vbucket_id = struct.unpack('>H', data[1:3])[0]
        r_vbucket_uuid = struct.unpack('>Q', data[3:11])[0]
        last_persisted_seq_no = struct.unpack('>Q', data[11:19])[0]
        current_seqno = struct.unpack('>Q', data[19:27])[0]

        if format_type == 1:
            old_vbucket_uuid = struct.unpack('>Q', data[27:35])[0]
            last_seqno_received = struct.unpack('>Q', data[35:43])[0]
        else:
            old_vbucket_uuid = None
            last_seqno_received = None

        return opaque, format_type, vbucket_id, r_vbucket_uuid, last_persisted_seq_no, current_seqno, old_vbucket_uuid, last_seqno_received


    def __parseGet(self, data, klen=0):
        flags = struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        if klen == 0:
            return flags, data[1], data[-1][4 + klen:]
        else:
            return flags, data[1], "{" + data[-1].split('{')[-1] # take only the value and value starts with "{"

    def get(self, key, vbucket= -1, collection=None):
        """Get the value for a given key within the memcached server."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        parts = self._doCmd(memcacheConstants.CMD_GET, key, '', collection=collection)

        return self.__parseGet(parts)

    def send_get(self, key, vbucket= -1, collection=None):
        """ sends a get message without parsing the response """
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_GET, key, '', opaque, collection)

    def getl(self, key, exp=15, vbucket= -1, collection=None):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket, collection=collection)
        parts = self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '',
            struct.pack(memcacheConstants.GETL_PKT_FMT, exp), collection=collection)
        return self.__parseGet(parts)

    def getr(self, key, vbucket= -1, collection=None):
        """Get the value for a given key within the memcached server from a replica vbucket."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        parts = self._doCmd(memcacheConstants.CMD_GET_REPLICA, key, '', collection=collection)
        return self.__parseGet(parts, len(key))


    def getMeta(self, key, request_extended_meta_data=False, collection=None):
        """Get the metadata for a given key within the memcached server."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, collection=collection)
        if request_extended_meta_data:
            extras = struct.pack('>B', 1)
        else:
            extras = ''
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_GET_META, key, '', extras, collection=collection)
        deleted = struct.unpack('>I', data[0:4])[0]
        flags = struct.unpack('>I', data[4:8])[0]
        exp = struct.unpack('>I', data[8:12])[0]
        seqno = struct.unpack('>Q', data[12:20])[0]

        if request_extended_meta_data:
            conflict_res = struct.unpack('>B', data[20:21])[0]
            return (deleted, flags, exp, seqno, cas, conflict_res)
        else:
            return (deleted, flags, exp, seqno, cas)


    def get_adjusted_time(self, vbucket, collection=None):
        """Get the value for a given key within the memcached server."""
        collection = self.collection_name(collection)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_GET_ADJUSTED_TIME, '', '', collection=collection)


    def set_time_drift_counter_state(self, vbucket, drift, state, collection=None):
        """Get the value for a given key within the memcached server."""
        collection = self.collection_name(collection)
        self.vbucketId = vbucket
        extras = struct.pack(memcacheConstants.SET_DRIFT_COUNTER_STATE_REQ_FMT, drift, state)
        return self._doCmd(memcacheConstants.CMD_SET_DRIFT_COUNTER_STATE, '', '', extras, collection=collection)



    def set_time_sync_state(self, vbucket, state, collection=None):
        """Get the value for a given key within the memcached server."""
        collection = self.collection_name(collection)
        self.vbucketId = vbucket
        extras = struct.pack(memcacheConstants.SET_DRIFT_COUNTER_STATE_REQ_FMT, 0, state)
        return self._doCmd(memcacheConstants.CMD_SET_DRIFT_COUNTER_STATE, '', '', extras)



    def cas(self, key, exp, flags, oldVal, val, vbucket= -1, collection=None):
        """CAS in a new value for the given key and comparison value."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val, collection)

    def touch(self, key, exp, vbucket= -1, collection=None):
        """Touch a key in the memcached server."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doCmd(memcacheConstants.CMD_TOUCH, key, '',
            struct.pack(memcacheConstants.TOUCH_PKT_FMT, exp), collection=collection)

    def gat(self, key, exp, vbucket= -1, collection=None):
        """Get the value for a given key and touch it within the memcached server."""
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        parts = self._doCmd(memcacheConstants.CMD_GAT, key, '',
            struct.pack(memcacheConstants.GAT_PKT_FMT, exp), collection=collection)
        return self.__parseGet(parts)

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(memcacheConstants.CMD_SASL_LIST_MECHS,
                               b'', b'')[2].split(b' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(memcacheConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        #self.log.info("-->sasl_auth_start: foruser={},user={},password={},join={}".format(foruser,user,password,'\0'.join([foruser, user, password])))
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user, password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        challenge = None
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError as e:
            if e.status != memcacheConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg.split(' ')[0]

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(memcacheConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def stop_persistence(self):
        return self._doCmd(memcacheConstants.CMD_STOP_PERSISTENCE, '', '')

    def start_persistence(self):
        return self._doCmd(memcacheConstants.CMD_START_PERSISTENCE, '', '')

    def set_flush_param(self, key, val):
        print("setting flush param:", key, val)
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val)

    def set_param(self, key, val, type):
        print("setting param:", key, val)
        type = struct.pack(memcacheConstants.GET_RES_FMT, type)
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val, type)

    def start_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_START_ONLINEUPDATE, '', '')

    def complete_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_COMPLETE_ONLINEUPDATE, '', '')

    def revert_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_REVERT_ONLINEUPDATE, '', '')

    def set_tap_param(self, key, val):
        print("setting tap param:", key, val)
        return self._doCmd(memcacheConstants.CMD_SET_TAP_PARAM, key, val)

    def set_vbucket_state(self, vbucket, stateName):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(memcacheConstants.VB_SET_PKT_FMT,
                            memcacheConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(memcacheConstants.CMD_SET_VBUCKET_STATE, '', '', state)

    def get_vbucket_state(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_GET_VBUCKET_STATE, '', '')


    def delete_vbucket(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_DELETE_VBUCKET, '', '')

    def evict_key(self, key, vbucket= -1, collection=None):
        collection = self.collection_name(collection)
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doCmd(memcacheConstants.CMD_EVICT_KEY, key, '', collection=collection)

    def getMulti(self, keys, vbucket= -1, collection=None):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        collection = self.collection_name(collection)

        opaqued = dict(enumerate(keys))
        terminal = len(opaqued) + 10
        # Send all of the keys in quiet
        vbs = set()
        for k, v in opaqued.items():
            self._set_vbucket(v, vbucket, collection=collection)
            vbs.add(self.vbucketId)
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k, collection=collection)

        for vb in vbs:
            self.vbucketId = vb
            self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        rv = {}
        for vb in vbs:
            self.vbucketId = vb
            done = False
            while not done:
                opaque, cas, data = self._handleSingleResponse(None)
                if opaque != terminal:
                    rv[opaqued[opaque]] = self.__parseGet((opaque, cas, data))
                else:
                    done = True

        return rv

    def setMulti(self, exp, flags, items, vbucket= -1, collection=None):
        """Multi-set (using setq).

        Give me (key, value) pairs."""

        # If this is a dict, convert it to a pair generator
        collection = self.collection_name(collection)

        if hasattr(items, 'items'):
            items = iter(items.items())

        opaqued = dict(enumerate(items))
        terminal = len(opaqued) + 10
        extra = struct.pack(SET_PKT_FMT, flags, exp)

        # Send all of the keys in quiet
        vbs = set()
        for opaque, kv in opaqued.items():
            self._set_vbucket(kv[0], vbucket, collection=collection)
            vbs.add(self.vbucketId)
            self._sendCmd(memcacheConstants.CMD_SETQ, kv[0], kv[1], opaque, extra, collection=collection)

        for vb in vbs:
            self.vbucketId = vb
            self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        failed = []
        for vb in vbs:
            self.vbucketId = vb
            done = False
            while not done:
                try:
                    opaque, cas, data = self._handleSingleResponse(None)
                    done = opaque == terminal
                except MemcachedError as e:
                    failed.append(e)

        return failed

    def collection_name(self, collection):
        if self.collections_supported:
            if collection == None:
                collection='_default._default'
        return collection

    def stats(self, sub=''):
        """Get stats."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data, frameextralen = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen].decode()] = data[klen:].decode()
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0, vbucket= -1, collection=None):
        """Delete the value for a given key within the memcached server."""
        collection = self.collection_name(collection)
        collection = self.collection_name(collection)

        self._set_vbucket(key, vbucket, collection=collection)
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas, collection=collection)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
            struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name):
        return self._doCmd(memcacheConstants.CMD_SELECT_BUCKET, name, '')

    def sync_persistence(self, keyspecs):
        payload = self._build_sync_payload(0x8, keyspecs)

        print("sending sync for persistence command for the following keyspecs:", keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_mutation(self, keyspecs):
        payload = self._build_sync_payload(0x4, keyspecs)

        print("sending sync for mutation command for the following keyspecs:", keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload((numReplicas & 0x0f) << 4, keyspecs)

        print("sending sync for replication command for the following keyspecs:", keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication_or_persistence(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0x8, keyspecs)

        print("sending sync for replication or persistence command for the " \
            "following keyspecs:", keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication_and_persistence(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0xA, keyspecs)

        print("sending sync for replication and persistence command for the " \
            "following keyspecs:", keyspecs)
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def _build_sync_payload(self, flags, keyspecs):
        payload = struct.pack(">I", flags)
        payload += struct.pack(">H", len(keyspecs))

        for spec in keyspecs:
            if not isinstance(spec, dict):
                raise TypeError("each keyspec must be a dict")
            if 'vbucket' not in spec:
                raise TypeError("missing vbucket property in keyspec")
            if 'key' not in spec:
                raise TypeError("missing key property in keyspec")

            payload += struct.pack(">Q", spec.get('cas', 0))
            payload += struct.pack(">H", spec['vbucket'])
            payload += struct.pack(">H", len(spec['key']))
            payload += spec['key']

        return payload

    def _parse_sync_response(self, data):
        keyspecs = []
        nkeys = struct.unpack(">H", data[0 : struct.calcsize("H")])[0]
        offset = struct.calcsize("H")

        for i in range(nkeys):
            spec = {}
            width = struct.calcsize("QHHB")
            (spec['cas'], spec['vbucket'], keylen, eventid) = \
                struct.unpack(">QHHB", data[offset : offset + width])
            offset += width
            spec['key'] = data[offset : offset + keylen]
            offset += keylen

            if eventid == memcacheConstants.CMD_SYNC_EVENT_PERSISTED:
                spec['event'] = 'persisted'
            elif eventid == memcacheConstants.CMD_SYNC_EVENT_MODIFED:
                spec['event'] = 'modified'
            elif eventid == memcacheConstants.CMD_SYNC_EVENT_DELETED:
                spec['event'] = 'deleted'
            elif eventid == memcacheConstants.CMD_SYNC_EVENT_REPLICATED:
                spec['event'] = 'replicated'
            elif eventid == memcacheConstants.CMD_SYNC_INVALID_KEY:
                spec['event'] = 'invalid key'
            elif spec['event'] == memcacheConstants.CMD_SYNC_INVALID_CAS:
                spec['event'] = 'invalid cas'
            else:
                spec['event'] = eventid

            keyspecs.append(spec)

        return keyspecs

    def restore_file(self, filename):
        """Initiate restore of a given file."""
        return self._doCmd(memcacheConstants.CMD_RESTORE_FILE, filename, '', '', 0)

    def restore_complete(self):
        """Notify the server that we're done restoring."""
        return self._doCmd(memcacheConstants.CMD_RESTORE_COMPLETE, '', '', '', 0)

    def deregister_tap_client(self, tap_name):
        """Deregister the TAP client with a given name."""
        return self._doCmd(memcacheConstants.CMD_DEREGISTER_TAP_CLIENT, tap_name, '', '', 0)

    def reset_replication_chain(self):
        """Reset the replication chain."""
        return self._doCmd(memcacheConstants.CMD_RESET_REPLICATION_CHAIN, '', '', '', 0)

    def _set_vbucket(self, key, vbucket= -1, collection=None):
        if vbucket < 0:
            self.vbucketId = (((zlib.crc32(key.encode())) >> 16) & 0x7fff) & (self.vbucket_count - 1)
        else:
            self.vbucketId = vbucket

    def get_config(self):
        """Get the config within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_GET_CLUSTER_CONFIG, '', '')

    def set_config(self, blob_conf):
        """Set the config within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_SET_CLUSTER_CONFIG, blob_conf, '')

    def sd_function(fn):
        def new_func(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as ex:
                raise
        return new_func

    @sd_function
    def get_sd(self, key, path, cas=0, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_GET, key, path, cas=cas, collection=collection)

    @sd_function
    def exists_sd(self, key, path, opaque=0, cas=0, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_EXISTS, key, path, opaque=opaque, cas=cas, collection=collection)

    @sd_function
    def dict_add_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DICT_ADD, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def dict_upsert_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DICT_UPSERT, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def delete_sd(self, key, path, opaque=0, cas=0, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DELETE, key, path, opaque=opaque, cas=cas, collection=collection)

    @sd_function
    def replace_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_REPLACE, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def array_push_last_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_LAST, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def array_push_first_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_FIRST, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def array_add_unique_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_ADD_UNIQUE, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def array_add_insert_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_INSERT, key, path, value, expiry, opaque, cas, create, collection=collection)

    @sd_function
    def counter_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_COUNTER, key, path, value, expiry, opaque, cas, create, collection=collection)


    '''
    usage:
    cmdDict["add_unique"] = {"create_parents" : False, "path": array, "value": 0}
    res  = mc.multi_mutation_sd(key, cmdDict)
    '''
    @sd_function
    def multi_mutation_sd(self, key, cmdDict, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doMultiSdCmd(memcacheConstants.CMD_SUBDOC_MULTI_MUTATION, key, cmdDict, opaque, collection=collection)
    @sd_function
    def multi_lookup_sd(self, key, path, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, collection=None):
        self._set_vbucket(key, vbucket, collection=collection)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_MULTI_LOOKUP, key, path, expiry, opaque, cas, create, collection=collection)

    def list_buckets(self):
        """Get the name of all buckets."""
        opaque, cas, data = self._doCmd(
            memcacheConstants.CMD_LIST_BUCKETS, '', '', '', 0)
        return data.strip().split(' ')

    def get_error_map(self):
        _, _, errmap = self._doCmd(memcacheConstants.CMD_GET_ERROR_MAP, '',
                    struct.pack("!H", self.error_map_version))

        errmap = json.loads(errmap)

        d = {}

        for k, v in errmap['errors'].items():
            d[int(k, 16)] = v

        errmap['errors'] = d
        return errmap

    def set_collections(self, manifest):
        rv = self._doCmd(memcacheConstants.CMD_COLLECTIONS_SET_MANIFEST,
                         '',
                         manifest)

        # Load up the collection map from the JSON
        self._update_collection_map(manifest)

    def get_collections(self, update_map=False):
        if self.collections_supported:
            rv = self._doCmd(memcacheConstants.CMD_COLLECTIONS_GET_MANIFEST,
                             '',
                             '')
            if update_map:
                self._update_collection_map(rv[2])
            return rv

    def enable_xerror(self):
        self.feature_flag.add(memcacheConstants.FEATURE_XERROR)

    def enable_collections(self):
        self.feature_flag.add(memcacheConstants.FEATURE_COLLECTIONS)


    def is_xerror_supported(self):
        return memcacheConstants.FEATURE_XERROR in self.features

    def is_collections_supported(self):
        return memcacheConstants.FEATURE_COLLECTIONS in self.features

    # Collections on the wire uses a varint encoding for the collection-ID
    # A simple unsigned_leb128 encoded is used:
    #    https://en.wikipedia.org/wiki/LEB128
    # @return a string with the binary encoding
    def _encodeCollectionId(self, key, collection):
        if not self.is_collections_supported():
                raise exceptions.RuntimeError("Collections are not enabled")

        if isinstance(collection, str):
            # expect scope.collection for name API
            try:
                collection = self.collection_map[collection]
            except KeyError as e:
                self.log.info("Error: cannot map collection \"{}\" to an ID".format(collection))
                self.log.info("name API expects \"scope.collection\" as the key")
                raise e

        output = array.array('B', [0])
        while collection > 0:
            byte = collection & 0xFF
            collection >>= 7
            # collection has more bits?
            if collection > 0:
                # Set the 'continue' bit of this byte
                byte |= 0x80
                output[-1] = byte
                output.append(0)
            else:
                output[-1] = byte
        return output.tostring() + key

    def _update_collection_map(self, manifest):
        self.collection_map = {}
        parsed = json.loads(manifest)
        for scope in parsed['scopes']:
            try:
                for collection in scope['collections']:
                    key = scope['name'] + "." + collection['name']
                    self.collection_map[key] = int(collection['uid'], 16)
            except KeyError:
                pass

def error_to_str(errno):
    if errno == 0x01:
        return "Not found"
    elif errno == 0x02:
        return "Exists"
    elif errno == 0x03:
        return "Too big"
    elif errno == 0x04:
        return "Invalid"
    elif errno == 0x05:
        return "Not stored"
    elif errno == 0x06:
        return "Bad Delta"
    elif errno == 0x07:
        return "Not my vbucket"
    elif errno == 0x08:
        return "No bucket"
    elif errno == 0x1f:
        return "Auth stale"
    elif errno == 0x20:
        return "Auth error"
    elif errno == 0x21:
        return "Auth continue"
    elif errno == 0x22:
        return "Outside legal range"
    elif errno == 0x23:
        return "Rollback"
    elif errno == 0x24:
        return "No access"
    elif errno == 0x25:
        return "Not initialized"
    elif errno == 0x81:
        return "Unknown Command"
    elif errno == 0x82:
        return "No Memory"
    elif errno == 0x83:
        return "Not Supported"
    elif errno == 0x84:
        return "Internal error"
    elif errno == 0x85:
        return "Busy"
    elif errno == 0x86:
        return "Temporary failure"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_ENOENT:
        return "Path not exists"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_MISMATCH:
        return "Path mismatch"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_EEXISTS:
        return "Path exists already"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_EINVAL:
        return "Invalid path"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_E2BIG:
        return "Path too big"
    elif errno == memcacheConstants.ERR_SUBDOC_VALUE_CANTINSERT:
        return "Cant insert"
    elif errno == memcacheConstants.ERR_SUBDOC_DOC_NOTJSON:
        return "Not json"
    elif errno == memcacheConstants.ERR_SUBDOC_NUM_ERANGE:
        return "Num out of range"
    elif errno == memcacheConstants.ERR_SUBDOC_DELTA_ERANGE:
        return "Delta out of range"
    elif errno == memcacheConstants.ERR_SUBDOC_DOC_ETOODEEP:
        return "Doc too deep"
    elif errno == memcacheConstants.ERR_SUBDOC_VALUE_TOODEEP:
        return "Value too deep"
    elif errno == memcacheConstants.ERR_SUBDOC_INVALID_CMD_COMBO:
        return "Invalid combinations of commands"
    elif errno == memcacheConstants.ERR_SUBDOC_MULTI_PATH_FAILURE:
        return "Specified key was successfully found, but one or more path operations failed"

