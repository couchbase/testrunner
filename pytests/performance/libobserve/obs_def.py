#!/usr/bin/env python

from sys import path
from struct import calcsize
from crc32 import crc32_hash

path.append("lib")

from memcacheConstants import REQ_PKT_FMT

OBS_OPCODE = 0x96

class ObserveKeyState:
    OBS_NOT_FOUND = 0x80
    OBS_FOUND = 0x00
    OBS_PERSISITED = 0x01
    OBS_IMPOSSIBLE = 0x81

class ObservePktFmt:
    OBS_REQ_HDR_FMT = REQ_PKT_FMT
    OBS_REQ_BODY_FMT = "!HH"
    OBS_RES_HDR_FMT = "!BBHBBHIIII"
    OBS_RES_BODY_FMT = "!HH"
    OBS_RES_HDR_LEN = calcsize(OBS_RES_HDR_FMT)
    OBS_RES_BODY_LEN = calcsize(OBS_RES_BODY_FMT)

class VbucketHelper:
    #TODO move out
    @staticmethod
    def get_vbucket_id(key, num_vbuckets):
        vbucketId = 0
        if num_vbuckets > 0:
            vbucketId = crc32_hash(key) & (num_vbuckets - 1)
        return vbucketId