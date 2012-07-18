#!/usr/bin/env python

from sys import path
from struct import calcsize

path.append("lib")

from memcacheConstants import REQ_PKT_FMT

OBS_OPCODE = 0x92

class ObserveKeyState:
    OBS_NOT_FOUND = 0x80
    OBS_FOUND = 0x00
    OBS_PERSISITED = 0x01
    OBS_IMPOSSIBLE = 0x81

class ObserveStatus:
    OBS_SUCCESS = 0x00
    OBS_MODIFIED = 0x01
    OBS_TIMEDOUT = 0x02
    OBS_UNKNOWN = 0xff
    OBS_ERROR = 0xee

class ObservePktFmt:
    OBS_REQ_HDR_FMT = REQ_PKT_FMT
    OBS_REQ_BODY_FMT = "!HH"
    OBS_RES_HDR_FMT = "!BBHBBHIIII"
    OBS_RES_BODY_FMT = "!HH"
    OBS_RES_HDR_LEN = calcsize(OBS_RES_HDR_FMT)
    OBS_RES_BODY_LEN = calcsize(OBS_RES_BODY_FMT)