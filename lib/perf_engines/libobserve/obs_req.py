#!/usr/bin/env python

import struct

from .obs_def import ObservePktFmt, OBS_OPCODE
from memcacheConstants import REQ_MAGIC_BYTE

class ObserveRequestKey:
    vbucket = 0x0000
    key_len = 0x0000
    key = ""

    def __init__(self, key, vbucket):
        self.key = key
        self.vbucket = vbucket
        self.key_len = len(key)

    def __repr__(self):
        return "<%s> vbucket: %d, key_len: %d, key: %s\n" %\
               (self.__class__.__name__, self.vbucket, self.key_len, self.key)

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return 4 + self.key_len

    def pack(self):
        pkt = []
        body = struct.pack(ObservePktFmt.OBS_REQ_BODY_FMT,
                           self.vbucket,
                           self.key_len)
        pkt.append(body)
        pkt.append(self.key)
        return ''.join(pkt)

class ObserveRequest:
    magic = REQ_MAGIC_BYTE
    opcode = OBS_OPCODE
    key_len = 0x0000
    extra_len = 0x00
    data_type = 0x00
    vbucket = 0x0000
    body_len = 0x00000000
    opaque = 0x00000000
    cas = 0x0000000000000000
    keys = []       # [:ObserveRequestKey]

    def __init__(self, keys, opaque=0x00000000, cas=0x0000000000000000):
        self.keys = keys
        self.opaque = opaque
        self.cas = cas

    def __repr__(self):
        repr = "<%s> magic: %x, opcode: %x, key_len: %d, extra_len: %x,"\
               " data_type: %x, vbucket: %d, body_len: %d, opaque: %s, cas: %x\n" %\
               (self.__class__.__name__, self.magic, self.opcode, self.key_len, self.extra_len,
                self.data_type, self.vbucket, self.body_len, self.opaque, self.cas)
        for key in self.keys:
            repr += str(key)
        return repr

    def __str__(self):
        return self.__repr__()

    def pack(self):
        pkt = []
        self.body_len = sum(len(s) for s in self.keys)

        header = struct.pack(ObservePktFmt.OBS_REQ_HDR_FMT, self.magic,
            self.opcode, self.key_len, self.extra_len,
            self.data_type, self.vbucket, self.body_len,
            self.opaque, self.cas)
        pkt.append(header)

        for key in self.keys:
            pkt.append(key.pack())

        return ''.join(pkt)
