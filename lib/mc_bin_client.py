#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import hmac
import socket
import select
import random
import struct
import exceptions
import zlib

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
from memcacheConstants import TOUCH_PKT_FMT, GAT_PKT_FMT, GETL_PKT_FMT
import memcacheConstants

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        error_msg = error_to_str(status)
        supermsg='Memcached error #' + `status` + ' ' + `error_msg`
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status=status
        self.msg=msg

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
        self.r=random.Random()
        self.vbucket_count = 1024

    def _createConn(self):
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return self.s.connect_ex((self.host, self.port))

    def reconnect(self):
        self.s.close()
        return self._createConn()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=REQ_PKT_FMT, magic=REQ_MAGIC_BYTE):
        msg=struct.pack(fmt, magic,
            cmd, len(key), len(extraHeader), dtype, vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        _, w, _ = select.select([], [self.s], [], self.timeout)
        if w:
            self.s.send(msg + extraHeader + key + val)
        else:
            raise exceptions.EOFError("Timeout waiting for socket send. from {0}".format(self.host))

    def _recvMsg(self):
        response = ""
        while len(response) < MIN_RECV_PACKET:
            r, _, _ = select.select([self.s], [], [], self.timeout)
            if r:
                data = self.s.recv(MIN_RECV_PACKET - len(response))
                if data == '':
                    raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
                response += data
            else:
                raise exceptions.EOFError("Timeout waiting for socket recv. from {0}".format(self.host))
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas=\
            struct.unpack(RES_PKT_FMT, response)

        rv = ""
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

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), "Got magic: %d" % magic
        return cmd, errcode, opaque, cas, keylen, extralen, rv

    def _handleKeyedResponse(self, myopaque):
        cmd, errcode, opaque, cas, keylen, extralen, rv = self._recvMsg()
        assert myopaque is None or opaque == myopaque, \
            "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode:
            rv += " for vbucket :{0} to mc {1}:{2}".format(self.vbucketId, self.host, self.port)
            raise MemcachedError(errcode,  rv)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0):
        """Send a command and await its response."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val):
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas)

    def _cat(self, cmd, key, cas, val):
        return self._doCmd(cmd, key, val, '', cas)

    def append(self, key, value, cas=0, vbucket=-1):
        self._set_vbucket(key, vbucket)
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key, value, cas=0, vbucket=-1):
        self._set_vbucket(key, vbucket)
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val=self._doCmd(cmd, key, '',
            struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp))
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0, vbucket=-1):
        """Increment or create the named counter."""
        self._set_vbucket(key, vbucket)
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0, vbucket=-1):
        """Decrement or create the named counter."""
        self._set_vbucket(key, vbucket)
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp)

    def set(self, key, exp, flags, val, vbucket=-1):
        """Set a value in the memcached server."""
        self._set_vbucket(key, vbucket)
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val)

    def send_set(self, key, exp, flags, val, vbucket=-1):
        """Set a value in the memcached server without handling the response"""
        self._set_vbucket(key, vbucket)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_SET, key, val, opaque, struct.pack(SET_PKT_FMT, flags, exp), 0)

    def add(self, key, exp, flags, val, vbucket=-1):
        """Add a value in the memcached server iff it doesn't already exist."""
        self._set_vbucket(key, vbucket)
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val)

    def replace(self, key, exp, flags, val, vbucket=-1):
        """Replace a value in the memcached server iff it already exists."""
        self._set_vbucket(key, vbucket)
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val)

    def __parseGet(self, data, klen=0):
        flags=struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts=self._doCmd(memcacheConstants.CMD_GET, key, '')

        return self.__parseGet(parts)

    def send_get(self, key, vbucket=-1):
        """ sends a get message without parsing the response """
        self._set_vbucket(key, vbucket)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_GET, key, '', opaque)

    def getl(self, key, exp=15, vbucket=-1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts=self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '',
            struct.pack(memcacheConstants.GETL_PKT_FMT, exp))
        return self.__parseGet(parts)

    def getr(self, key, vbucket=-1):
        """Get the value for a given key within the memcached server from a replica vbucket."""
        self._set_vbucket(key, vbucket)
        parts=self._doCmd(memcacheConstants.CMD_GET_REPLICA, key, '')
        return self.__parseGet(parts, len(key))

    def __parseMeta(self, data):
        flags = struct.unpack('I', data[-1][0:4])[0]
        meta_type = struct.unpack('B', data[-1][4])[0]
        length = struct.unpack('B', data[-1][5])[0]
        meta = data[-1][6:6 + length]
        return (meta_type, flags, meta)

    def getMeta(self, key, vbucket=-1):
        """Get the metadata for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts=self._doCmd(memcacheConstants.CMD_GET_META, key, '')
        return self.__parseMeta(parts)

    def getRev(self, key, vbucket=-1):
        """Get the revision for a given key within the memcached server."""
        (meta_type, flags, meta_data) = self.getMeta(key, vbucket=-1)
        if meta_type != memcacheConstants.META_REVID:
            raise ValueError("Invalid meta type %x" % meta_type)
        try:
            seqno = struct.unpack('>Q', meta_data[:8])[0]
            cas = struct.unpack('>Q', meta_data[8:16])[0]
            exp_flags = struct.unpack('>I', meta_data[16:20])[0]
            flags_other = False
            flags_other = struct.unpack('>I', meta_data[20:])[0]
        except Exception as ex:
            print(
                "Error unpacking rev: ({0}) Rev parts we have:: seqno: {1} cas: {2} exp: {3} flags_other: {4}".format(ex, seqno, cas, exp_flags, flags_other))
        return (seqno, cas, exp_flags, flags_other)

    def cas(self, key, exp, flags, oldVal, val, vbucket=-1):
        """CAS in a new value for the given key and comparison value."""
        self._set_vbucket(key, vbucket)
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val)

    def touch(self, key, exp, vbucket=-1):
        """Touch a key in the memcached server."""
        self._set_vbucket(key, vbucket)
        return self._doCmd(memcacheConstants.CMD_TOUCH, key, '',
            struct.pack(memcacheConstants.TOUCH_PKT_FMT, exp))

    def gat(self, key, exp, vbucket=-1):
        """Get the value for a given key and touch it within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts=self._doCmd(memcacheConstants.CMD_GAT, key, '',
            struct.pack(memcacheConstants.GAT_PKT_FMT, exp))
        return self.__parseGet(parts)

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(memcacheConstants.CMD_SASL_LIST_MECHS,
                               '', '')[2].split(' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(memcacheConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user, password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        challenge = None
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError, e:
            if e.status != memcacheConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(memcacheConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def stop_persistence(self):
        return self._doCmd(memcacheConstants.CMD_STOP_PERSISTENCE, '', '')

    def start_persistence(self):
        return self._doCmd(memcacheConstants.CMD_START_PERSISTENCE, '', '')

    def set_flush_param(self, key, val):
        print "setting flush param:", key, val
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val)

    def set_param(self, key, val, type):
        print "setting param:", key, val
        type = struct.pack(memcacheConstants.GET_RES_FMT, type)
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val, type)

    def start_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_START_ONLINEUPDATE, '', '')

    def complete_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_COMPLETE_ONLINEUPDATE, '', '')

    def revert_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_REVERT_ONLINEUPDATE, '', '')

    def set_tap_param(self, key, val):
        print "setting tap param:", key, val
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
        return self._doCmd(memcacheConstants.CMD_GET_VBUCKET_STATE,
                           str(vbucket), '')

    def delete_vbucket(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_DELETE_VBUCKET, '', '')

    def evict_key(self, key, vbucket=-1):
        self._set_vbucket(key, vbucket)
        return self._doCmd(memcacheConstants.CMD_EVICT_KEY, key, '')

    def getMulti(self, keys, vbucket=-1):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued=dict(enumerate(keys))
        terminal=len(opaqued)+10
        # Send all of the keys in quiet
        vbs = set()
        for k,v in opaqued.iteritems():
            self._set_vbucket(v, vbucket)
            vbs.add(self.vbucketId)
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k)

        for vb in vbs:
            self.vbucketId = vb
            self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        rv={}
        for vb in vbs:
            self.vbucketId = vb
            done=False
            while not done:
                opaque, cas, data=self._handleSingleResponse(None)
                if opaque != terminal:
                    rv[opaqued[opaque]]=self.__parseGet((opaque, cas, data))
                else:
                    done=True

        return rv

    def setMulti(self, exp, flags, items, vbucket=-1):
        """Multi-set (using setq).

        Give me (key, value) pairs."""

        # If this is a dict, convert it to a pair generator
        if hasattr(items, 'iteritems'):
            items = items.iteritems()

        opaqued=dict(enumerate(items))
        terminal=len(opaqued)+10
        extra=struct.pack(SET_PKT_FMT, flags, exp)

        # Send all of the keys in quiet
        vbs = set()
        for opaque,kv in opaqued.iteritems():
            self._set_vbucket(kv[0], vbucket)
            vbs.add(self.vbucketId)
            self._sendCmd(memcacheConstants.CMD_SETQ, kv[0], kv[1], opaque, extra)

        for vb in vbs:
            self.vbucketId = vb
            self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        failed = []
        for vb in vbs:
            self.vbucketId = vb
            done=False
            while not done:
                try:
                    opaque, cas, data = self._handleSingleResponse(None)
                    done = opaque == terminal
                except MemcachedError, e:
                    failed.append(e)

        return failed

    def stats(self, sub=''):
        """Get stats."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(memcacheConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0, vbucket=-1):
        """Delete the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
            struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name):
        return self._doCmd(memcacheConstants.CMD_SELECT_BUCKET, name, '')

    def sync_persistence(self, keyspecs):
        payload = self._build_sync_payload(0x8, keyspecs)

        print "sending sync for persistence command for the following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_mutation(self, keyspecs):
        payload = self._build_sync_payload(0x4, keyspecs)

        print "sending sync for mutation command for the following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload((numReplicas & 0x0f) << 4, keyspecs)

        print "sending sync for replication command for the following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication_or_persistence(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0x8, keyspecs)

        print "sending sync for replication or persistence command for the " \
            "following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication_and_persistence(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0xA, keyspecs)

        print "sending sync for replication and persistence command for the " \
            "following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def _build_sync_payload(self, flags, keyspecs):
        payload = struct.pack(">I", flags)
        payload += struct.pack(">H", len(keyspecs))

        for spec in keyspecs:
            if not isinstance(spec, dict):
                raise TypeError("each keyspec must be a dict")
            if not spec.has_key('vbucket'):
                raise TypeError("missing vbucket property in keyspec")
            if not spec.has_key('key'):
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

        for i in xrange(nkeys):
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

    def _set_vbucket(self, key, vbucket=-1):
        if vbucket < 0:
            self.vbucketId = (((zlib.crc32(key)) >> 16) & 0x7fff) & (self.vbucket_count - 1)
        else:
            self.vbucketId = vbucket

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
    elif errno == 0x20:
        return "Auth error"
    elif errno == 0x21:
        return "Auth continue"
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
