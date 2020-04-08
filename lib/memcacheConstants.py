#!/usr/bin/env python
"""

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import struct

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_SETQ = 0x11
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7
CMD_FLUSH = 8
CMD_GETQ = 9
CMD_NOOP = 10
CMD_VERSION = 11
CMD_STAT = 0x10
CMD_APPEND = 0x0e
CMD_PREPEND = 0x0f
CMD_STAT = 0x10
CMD_SETQ = 0x11
CMD_DELETEQ = 0x14
CMD_VERBOSE = 0x1b
CMD_TOUCH = 0x1c
CMD_GAT = 0x1d
CMD_HELLO = 0x1f
CMD_GET_REPLICA = 0x83
CMD_OBSERVE_SEQNO = 0x91
CMD_OBSERVE = 0x92


# UPR command opcodes
CMD_OPEN             = 0x50
CMD_ADD_STREAM       = 0x51
CMD_CLOSE_STREAM     = 0x52
CMD_STREAM_REQ       = 0x53
CMD_GET_FAILOVER_LOG = 0x54
CMD_STREAM_END       = 0x55
CMD_SNAPSHOT_MARKER  = 0x56
CMD_MUTATION         = 0x57
CMD_DELETION         = 0x58
CMD_EXPIRATION       = 0x59
CMD_FLUSH            = 0x5a
CMD_SET_VB_STATE     = 0x5b
CMD_UPR_NOOP         = 0x5c
CMD_UPR_ACK          = 0x5d
CMD_FLOW_CONTROL     = 0x5e
CMD_SYSTEM_EVENT     = 0x5f

# DCP opcode dictionary
DCP_Opcode_Dictionary = {CMD_OPEN: 'CMD_OPEN',
                         CMD_ADD_STREAM: 'CMD_ADD_STREAM',
                         CMD_CLOSE_STREAM: 'CMD_CLOSE_STREAM',
                         CMD_STREAM_REQ: 'CMD_STREAM_REQ',
                         CMD_GET_FAILOVER_LOG: 'CMD_GET_FAILOVER_LOG',
                         CMD_STREAM_END: 'CMD_STREAM_END',
                         CMD_SNAPSHOT_MARKER: 'CMD_SNAPSHOT_MARKER',
                         CMD_MUTATION: 'CMD_MUTATION',
                         CMD_DELETION: 'CMD_DELETION',
                         CMD_EXPIRATION: 'CMD_EXPIRATION',
                         CMD_FLUSH: 'CMD_FLUSH',
                         CMD_SET_VB_STATE: 'CMD_SET_VB_STATE',
                         CMD_UPR_ACK: 'CMD_ACK',
                         CMD_FLOW_CONTROL: 'CMD_FLOW_CONTROL',
                         CMD_UPR_NOOP: 'CMD_DCP_NOOP',
                         CMD_SYSTEM_EVENT: 'CMD_SYSTEM_EVENT'}

# SASL stuff
CMD_SASL_LIST_MECHS = 0x20
CMD_SASL_AUTH = 0x21
CMD_SASL_STEP = 0x22

# Bucket extension
CMD_CREATE_BUCKET = 0x85
CMD_DELETE_BUCKET = 0x86
CMD_LIST_BUCKETS = 0x87
CMD_EXPAND_BUCKET = 0x88
CMD_SELECT_BUCKET = 0x89

CMD_STOP_PERSISTENCE = 0x80
CMD_START_PERSISTENCE = 0x81
CMD_SET_FLUSH_PARAM = 0x82
CMD_SET_PARAM = 0x82

CMD_SET_TAP_PARAM = 0x92
CMD_EVICT_KEY = 0x93

CMD_RESTORE_FILE = 0x98
CMD_RESTORE_ABORT = 0x99
CMD_RESTORE_COMPLETE = 0x9a

#Online update
CMD_START_ONLINEUPDATE = 0x9b
CMD_COMPLETE_ONLINEUPDATE = 0x9c
CMD_REVERT_ONLINEUPDATE = 0x9d

# TAP client registration
CMD_DEREGISTER_TAP_CLIENT = 0x9e

# Reset replication chain
CMD_RESET_REPLICATION_CHAIN = 0x9f

CMD_GET_META = 0xa0
CMD_GETQ_META = 0xa1

CMD_SET_WITH_META = 0xa2
CMD_SETQ_WITH_META = 0xa3

CMD_ADD_WITH_META = 0xa4
CMD_ADDQ_WITH_META = 0xa5

CMD_DELETE_WITH_META = 0xa8
CMD_DELETEQ_WITH_META = 0xa9



CMD_SET_DRIFT_COUNTER_STATE = 0xc1
CMD_GET_ADJUSTED_TIME = 0xc2

META_ADJUSTED_TIME = 0x1
META_CONFLICT_RESOLUTION_MODE = 0x2

# Replication
CMD_TAP_CONNECT = 0x40
CMD_TAP_MUTATION = 0x41
CMD_TAP_DELETE = 0x42
CMD_TAP_FLUSH = 0x43
CMD_TAP_OPAQUE = 0x44
CMD_TAP_VBUCKET_SET = 0x45
CMD_TAP_CHECKPOINT_START = 0x46
CMD_TAP_CHECKPOINT_END = 0x47

# vbucket stuff
CMD_SET_VBUCKET_STATE = 0x3d
CMD_GET_VBUCKET_STATE = 0x3e
CMD_DELETE_VBUCKET = 0x3f

CMD_GET_LOCKED = 0x94
CMD_COMPACT_DB = 0xb3
CMD_GET_RANDOM_KEY = 0xb6

# Collections
CMD_COLLECTIONS_SET_MANIFEST = 0xb9
CMD_COLLECTIONS_GET_MANIFEST = 0xba
CMD_COLLECTIONS_GET_ID = 0xbb
CMD_COLLECTIONS_GET_SCOPE_ID = 0xbc

CMD_GET_ERROR_MAP = 0xfe

CMD_SYNC = 0x96

# event IDs for the SYNC command responses
CMD_SYNC_EVENT_PERSISTED = 1
CMD_SYNC_EVENT_MODIFED = 2
CMD_SYNC_EVENT_DELETED = 3
CMD_SYNC_EVENT_REPLICATED = 4
CMD_SYNC_INVALID_KEY = 5
CMD_SYNC_INVALID_CAS = 6

VB_STATE_ACTIVE = 1
VB_STATE_REPLICA = 2
VB_STATE_PENDING = 3
VB_STATE_DEAD = 4
VB_STATE_NAMES = {'active': VB_STATE_ACTIVE,
                'replica': VB_STATE_REPLICA,
                'pending': VB_STATE_PENDING,
                'dead': VB_STATE_DEAD}

# Parameter types of CMD_SET_PARAM command.
ENGINE_PARAM_FLUSH = 1
ENGINE_PARAM_TAP = 2
ENGINE_PARAM_REPLICATION= 2
ENGINE_PARAM_CHECKPOINT = 3
ENGINE_PARAM_DCP        = 4
ENGINE_PARAM_VBUCKET    = 5

COMMAND_NAMES = dict(((globals()[k], k) for k in globals() if k.startswith("CMD_")))

# TAP_OPAQUE types
TAP_OPAQUE_ENABLE_AUTO_NACK = 0
TAP_OPAQUE_INITIAL_VBUCKET_STREAM = 1
TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC = 2
TAP_OPAQUE_OPEN_CHECKPOINT = 3

# TAP connect flags
TAP_FLAG_BACKFILL = 0x01
TAP_FLAG_DUMP = 0x02
TAP_FLAG_LIST_VBUCKETS = 0x04
TAP_FLAG_TAKEOVER_VBUCKETS = 0x08
TAP_FLAG_SUPPORT_ACK = 0x10
TAP_FLAG_REQUEST_KEYS_ONLY = 0x20
TAP_FLAG_CHECKPOINT = 0x40
TAP_FLAG_REGISTERED_CLIENT = 0x80

TAP_FLAG_TYPES = {TAP_FLAG_BACKFILL: ">Q",
                  TAP_FLAG_REGISTERED_CLIENT: ">B"}

# TAP per-message flags
TAP_FLAG_ACK = 0x01
TAP_FLAG_NO_VALUE = 0x02 # The value for the key is not included in the packet

# UPR per-message flags
FLAG_OPEN_CONSUMER = 0x00
FLAG_OPEN_PRODUCER = 0x01
FLAG_OPEN_NOTIFIER = 0x02
FLAG_OPEN_INCLUDE_XATTRS = 0x4
FLAG_OPEN_INCLUDE_DELETE_TIMES = 0x20

#CCCP
CMD_SET_CLUSTER_CONFIG = 0xb4
CMD_GET_CLUSTER_CONFIG = 0xb5

#subdoc commands
CMD_SUBDOC_GET = 0xc5
CMD_SUBDOC_EXISTS = 0xc6

#Dictionary commands
CMD_SUBDOC_DICT_ADD = 0xc7
CMD_SUBDOC_DICT_UPSERT = 0xc8

#Generic modification commands
CMD_SUBDOC_DELETE = 0xc9
CMD_SUBDOC_REPLACE = 0xca

#Array commands
CMD_SUBDOC_ARRAY_PUSH_LAST = 0xcb
CMD_SUBDOC_ARRAY_PUSH_FIRST = 0xcc
CMD_SUBDOC_ARRAY_INSERT = 0xcd
CMD_SUBDOC_ARRAY_ADD_UNIQUE = 0xce

# Arithmetic commands
CMD_SUBDOC_COUNTER = 0xcf

# Multi commands
CMD_SUBDOC_MULTI_LOOKUP = 0xd0
CMD_SUBDOC_MULTI_MUTATION = 0xd1

SUBDOC_FLAGS_MKDIR_P = 0x01


# Flags, expiration
SET_PKT_FMT = ">II"
META_CMD_FMT = '>IIQQ'  # flags (4 bytes), expiration (4), seqno (8), CAS (8), metalen (2)

META_CMD_FMT = '>IIQQ'
EXTENDED_META_CMD_FMT = '>IIQQIH'
SKIP_META_CMD_FMT = '>IIQQI'

CR = 0x01

SET_DRIFT_COUNTER_STATE_REQ_FMT = '>qB'
GET_ADJUSTED_TIME_RES_FMT = '>Q'

# version - 1 byte [id, length, field]+ id is 1 byte, length is 2 bytes, field length is given by length
# for now we only support field ids adjusted time and conflict resolution mode and I am going to assume this:
# version=1, id_1= 1 (adjusted time), adjusted time length = 8 and then 8 bytes
#  and tthen id_2 = 2 (conflict resolution mode), length = 1 or 2? and then t conflict resolution mode
EXTENDED_META_DATA_FMT = '>BBHQBHB'
EXTENDED_META_DATA_VERSION = 0x1
META_DATA_ID_ADJUSTED_TIME = 0x1
META_DATA_ID_ADJUSTED_TIME_SIZE = 8
META_DATA_ID_CONFLICT_RESOLUTION_MODE = 0x2
META_DATA_ID_CONFLICT_RESOLUTION_MODE_SIZE = 1

# flags
GET_RES_FMT = ">I"

# How long until the deletion takes effect.
DEL_PKT_FMT = ""

## TAP stuff
# eng-specific length, flags, ttl, [res, res, res]; item flags, exp
TAP_MUTATION_PKT_FMT = ">HHbxxxII"
TAP_GENERAL_PKT_FMT = ">HHbxxx"

# amount, initial value, expiration
INCRDECR_PKT_FMT = ">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL = 0xffffffff
INCRDECR_RES_FMT = ">Q"
INCRDECR_RES_WITH_UUID_AND_SEQNO_FMT = ">QQQ"

# Time bomb
FLUSH_PKT_FMT = ">I"


# Meta LWW extras
META_EXTRA_FMT = '>IIQQH'

# Touch commands
# expiration
TOUCH_PKT_FMT = ">I"
GAT_PKT_FMT = ">I"
GETL_PKT_FMT = ">I"

# set param command
SET_PARAM_FMT=">I"
# 2 bit integer.  :/
VB_SET_PKT_FMT = ">I"

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = 0x80
MEMCACHED_REQUEST_MAGIC = '\x80'
RES_MAGIC_BYTE = 0x81

# ALternative encoding (frame info present)
ALT_REQ_MAGIC_BYTE = 0x08
ALT_RES_MAGIC_BYTE = 0x18

COMPACT_DB_PKT_FMT=">QQBxxxxxxx"

# magic, opcode, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
REQ_PKT_FMT = ">BBHBBHIIQ"
DATA_TYPE = '\x00'
VBUCKET = '\x00\x00'


# subdoc extras format - path len
REQ_PKT_SD_EXTRAS= ">HB"

# subdoc extras format - path len, expiration
REQ_PKT_SD_EXTRAS_EXPIRY= ">HBI"

# magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
RES_PKT_FMT = ">BBHBBHIIQ"

# magic, opcode, frameextra, keylen, extralen, datatype, status, bodylen, opaque, cas
ALT_RES_PKT_FMT = ">BBBBBBHIIQ"

# magic, opcode, frame_extra_len, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
ALT_REQ_PKT_FMT=">BBBBBBHIIQ"

#opcode, flags, pathlen, vallen
REQ_PKT_SD_MULTI_MUTATE = ">BBHI"


# min recv packet size
MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
# The header sizes don't deviate
assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

EXTRA_HDR_FMTS = {
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT,
    CMD_INCR: INCRDECR_PKT_FMT,
    CMD_DECR: INCRDECR_PKT_FMT,
    CMD_DELETE: DEL_PKT_FMT,
    CMD_FLUSH: FLUSH_PKT_FMT,
    CMD_TAP_MUTATION: TAP_MUTATION_PKT_FMT,
    CMD_TAP_DELETE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_FLUSH: TAP_GENERAL_PKT_FMT,
    CMD_TAP_OPAQUE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_VBUCKET_SET: TAP_GENERAL_PKT_FMT,
    CMD_SET_VBUCKET_STATE: VB_SET_PKT_FMT,
    CMD_COMPACT_DB: COMPACT_DB_PKT_FMT
}

EXTRA_HDR_SIZES = dict(
    [(k, struct.calcsize(v)) for (k, v) in list(EXTRA_HDR_FMTS.items())])

# Kept for backwards compatibility with existing mc_bin_client users.

ERR_UNKNOWN_CMD = 0x81
ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
ERR_AUTH = 0x20
NotFoundError = 0xD
ERR_SUCCESS = 0x00
ERR_KEY_ENOENT = 0x01
ERR_2BIG = 0x03
ERR_EINVAL = 0x04
ERR_NOT_STORED = 0x05
ERR_BAD_DELTA = 0x06
ERR_NOT_MY_VBUCKET = 0x07
ERR_AUTH_CONTINUE = 0x21
ERR_ERANGE = 0x22
ERR_ENOMEM = 0x82
ERR_NOT_SUPPORTED = 0x83
ERR_EINTERNAL = 0x84
ERR_EBUSY = 0x85
ERR_ETMPFAIL = 0x86

META_REVID = 0x01

ERR_SUBDOC_PATH_ENOENT = 0xc0
ERR_SUBDOC_PATH_MISMATCH = 0xc1
ERR_SUBDOC_PATH_EINVAL = 0xc2
ERR_SUBDOC_PATH_E2BIG = 0xc3
ERR_SUBDOC_DOC_ETOODEEP = 0xc4
ERR_SUBDOC_VALUE_CANTINSERT = 0xc5
ERR_SUBDOC_DOC_NOTJSON = 0xc6
ERR_SUBDOC_NUM_ERANGE = 0xc7
ERR_SUBDOC_DELTA_ERANGE = 0xc8
ERR_SUBDOC_PATH_EEXISTS = 0xc9
ERR_SUBDOC_VALUE_TOODEEP = 0xca
ERR_SUBDOC_INVALID_CMD_COMBO = 0xcb
ERR_SUBDOC_MULTI_PATH_FAILURE = 0xcc

# hello feature parameters - taken from protocol_binary.h
PROTOCOL_BINARY_FEATURE_DATATYPE = 0x01,
PROTOCOL_BINARY_FEATURE_TLS = 0x2,
PROTOCOL_BINARY_FEATURE_TCPNODELAY = 0x03,
PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO = 0x04
FEATURE_TCPDELAY = 0x05
FEATURE_XATTR = 0x06
FEATURE_XERROR = 0x07
FEATURE_SELECT_BUCKET = 0x08
FEATURE_COLLECTIONS = 0x09

# Enableable features
FEATURE_DATATYPE = 0x01
FEATURE_TLS = 0x2
FEATURE_TCPNODELAY = 0x03
FEATURE_MUTATION_SEQNO = 0x04


# LWW related - these are documented here https://github.com/couchbase/ep-engine/blob/master/docs/protocol/set_with_meta.md
SKIP_CONFLICT_RESOLUTION_FLAG = 0x1
FORCE_ACCEPT_WITH_META_OPS = 0x2
REGENERATE_CAS = 0x4

# Datatypes
DATATYPE_XATTR = 0x4
DURABILITY_LEVEL_MAJORITY = 0x1