#!/usr/bin/env python

import struct
from .obs_def import ObserveKeyState, ObservePktFmt, OBS_OPCODE
from memcacheConstants import RES_MAGIC_BYTE

class ObserveResponseKey:

    def __init__(self, key, vbucket=0x0000,
                 key_state=ObserveKeyState.OBS_IMPOSSIBLE,
                 cas=0x0000000000000000):
        self.key = key
        self.vbucket = vbucket
        self.key_state = key_state
        self.cas = cas
        self.key_len = len(key)

    def __repr__(self):
        return "<%s> vbucket: %d, key_len: %d, key: %s,"\
               " key_state: %x, cas = %x \n" %\
               (self.__class__.__name__, self.vbucket, self.key_len,
                self.key, self.key_state, self.cas)

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return 13 + self.key_len

class ObserveResponse:

    def __init__(self):
        self.magic = RES_MAGIC_BYTE
        self.opcode = OBS_OPCODE
        self.key_len = 0x0000
        self.extra_len = 0x00
        self.data_type = 0x00
        self.status = 0x0000
        self.body_len = 0x00000000
        self.opaque = 0x00000000
        self.persist_stat = 0x00000000
        self.repl_stat = 0x00000000
        self.keys = []       # [:ObserveResponseKey]

    def __repr__(self):
        repr = "<%s> magic: %x, opcode: %x, key_len: %d, extra_len: %x, data_type: %x,"\
               " status: %d, body_len: %d, opaque: %s, persist_stats: %f, repl_stat: %f\n" %\
               (self.__class__.__name__, self.magic, self.opcode, self.key_len, self.extra_len,
                self.data_type, self.status, self.body_len, self.opaque, self.persist_stat, self.repl_stat)
        for key in self.keys:
            repr += str(key)
        return repr

    def __str__(self):
        return self.__repr__()

    def _check_hdr(self):
        """@throws AssertionError"""
        assert self.magic == RES_MAGIC_BYTE
        assert self.opcode == OBS_OPCODE

    def unpack_hdr(self, hdr):
        self.magic, self.opcode, self.key_len, self.extra_len,\
        self.data_type, self.status, self.body_len, self.opaque,\
        self.persist_stat, self.repl_stat = struct.unpack(ObservePktFmt.OBS_RES_HDR_FMT, hdr)

        try:
            self._check_hdr()
        except AssertionError as e:
            print("<%s> AsseritionError: %s" % (self.__class__.__name__, e))
            return False

        if self.status:
            # TODO: observe opcode 0x96 collides with memcached CMD_SYNC
            #       for backward compatibility, we are supposed to receive
            #       ERR_UNKNOWN_CMD = 0x81 if we send OBSERVE to a 1.8 server
            #       http://www.couchbase.com/issues/browse/MB-5805
            print("<%s> server error: %d " % (self.__class__.__name__, self.status))
            return False

        return True

    def unpack_body(self, body):
        # TODO: chunk read, & simplify
        offset = 0
        while offset < self.body_len:
            vbucket, key_len = struct.unpack(ObservePktFmt.OBS_RES_BODY_FMT, body[offset:offset+4])
            offset += 4
            fmt = "!" + str(key_len) + "sBQ"
            length = struct.calcsize(fmt)
            key, key_state, cas = struct.unpack(fmt, body[offset:offset+length])
            obs_key = ObserveResponseKey(key, vbucket=vbucket, key_state=key_state, cas=cas)
            self.keys.append(obs_key)
            offset += length