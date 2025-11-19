##!/usr/bin/env python

"""

memcached tasks

"""


from app.celery import celery
from celery import Task
import random
import string
import sys
import copy
import time
import zlib
import importlib
import datetime

sys.path=["../lib"] + sys.path
from mc_bin_client import MemcachedClient, MemcachedError
from app.rest_client_tasks import create_rest
import couchbase

from cache import CacheHelper
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

import json
import eventlet
from random import randint

import testcfg as cfg

# import sdk
try:
    # workaround required to remove relative paths
    while True:
        del sys.path[sys.path.index('../lib')]
except ValueError:
    pass

importlib.reload(couchbase)
from couchbase.bucket import Bucket

class PersistedCB(Task):
    clientMap = {}
    _hosts = None
    #_stale_count = 0

    def couchbaseClient(self, bucket = "default", password = ""):
        #if bucket not in self.clientMap or self._stale_count >= 100:
        if bucket not in self.clientMap:
            addr = self._hosts[random.randint(0, len(self._hosts) - 1)].split(':')
            host = addr[0]
            port = 8091
            if len(addr) > 1:
                port = addr[1]

            logger.error("DEPRECIATED SDK CONNECT METHOD sdk: %s" % host)
            self._conn = Bucket("%s:%s/%s" % (host, port, bucket), password = password)
            self.clientMap[bucket] = self._conn
            self._stale_count = 0
        #self._stale_count = self._stale_count + 1
        return self.clientMap[bucket]

    def set_hosts(self, hosts):
        if self._hosts is not None:
            if (len(hosts) != len(self._hosts)):
                # host list miss match
                self._conn = None
            else:
                for _h in hosts:
                   if _h not in self._hosts:
                       # new host list
                       self._conn = None
                       break

        self._hosts = hosts

    def close(self):
        del self._conn
        self._conn = None


@celery.task(base = PersistedCB)
def mset(keys, template, bucket = "default", isupdate = False, password = "", hosts = None):
    mset.set_hosts(hosts)
    cb = mset.couchbaseClient(bucket, password)

    # decode magic strings in template before sending to sdk
    # python pass-by-reference updates attribute in function
    rawTemplate = copy.deepcopy(template)
    decodeMajgicStrings(rawTemplate)
    msg = {}
    for key in keys:
       msg[key] = rawTemplate

    try:
        n = datetime.datetime.now()
        #dispatch
        #eventlet.spawn_n(cb.set_multi, msg)
        p = datetime.datetime.now()
        #logger.error((p-n).microseconds)
    except Exception:
        mset._conn = None
 
    return keys, rawTemplate


@celery.task(base = PersistedCB)
def mget(keys, bucket = "default", password = "", hosts = None):
    mget.set_hosts(hosts)
    cb = mget.couchbaseClient(bucket, password)

    try:
        cb.get_multi(keys)
    except Exception:
        mget._conn = None

    #TODO: return failures


@celery.task(base = PersistedCB)
def mdelete(keys, bucket = "default", password = "", hosts = None):
    mdelete.set_hosts(hosts)
    cb = mdelete.couchbaseClient(bucket, password)
    try:
        cb.delete_multi(keys)
    except Exception:
        mdelete._conn = None

    #TODO: return failures


@celery.task
def set(key, value, bucket = "default", password = ""):

    cb = Bucket(host=cfg.COUCHBASE_IP + "/" + self.bucket)
    cb.set({key : value})

@celery.task
def get(key, bucket = "default", password = ""):

    cb = Bucket(host=cfg.COUCHBASE_IP + "/" + self.bucket)
    rc = cb.get(key)
    return rc

@celery.task
def delete(key, bucket = "default", password = ""):

    cb = Bucket(host=cfg.COUCHBASE_IP + "/" + self.bucket)
    rc = cb.delete(key)

"""
" raw mc op_latency task.
"
" op: (set,get,delete).
" args: operation arguments. i.e set => ('key', 0, 0, 'val')
"
" returns amount of time required to complete operation
"""
@celery.task
def mc_op_latency(op, key, val, ip, port = 8091, bucket = "default", password = ""):



    latency = 0
    args = None

    # define op args
    try:


        # get mc for vbucket
        mc = getDirectMC(key, ip, port, bucket, password)
        if mc is None: return

        # select op and create args
        if op == 'set':
            func = mc.set
            args = (key, 0, 0, val)
        if op == 'get':
            func = mc.get
            args = (key,)
        if op == 'delete':
            func = mc.delete
            args = (key,)

        # timed wrapper
        start = time.time()
        rc = func(*args)  # exec
        end = time.time()
        latency = end - start

    except MemcachedError as ex:
        msg = "error connecting to host %s:%s for gathering latency"\
               %  (ip, port)
        logger.error(ex)

    return latency

def getDirectMC(key, ip, port = 8091, bucket = "default", password = ""):

    real_mc_client = None

    # get initial mc client
    client = MemcachedClient(ip, int(port))

    # get vbucket map
    rest = create_rest(ip, port)
    vbuckets = rest.get_vbuckets(bucket)
    vbId = (((zlib.crc32(key.encode())) >> 16) & 0x7fff) & (len(vbuckets) - 1)

    # find vbucket responsible to this key and mapping host
    if vbuckets is not None:

        vbucket = [vbucket for vbucket in vbuckets if vbucket.id == vbId]
        if len(vbucket) == 1:
            mc_ip, mc_port = vbucket[0].master.split(":")
            real_mc_client = MemcachedClient(mc_ip, int(mc_port))
            real_mc_client.sasl_auth_plain(bucket, password)
            real_mc_client.vbucket_count = len(vbuckets)

    return real_mc_client

def decodeMajgicStrings(template):

    kv = template["kv"]

    for key, value in kv.items():
        if isinstance(value, str) and value.startswith("@"):
            #If string starts with "@", assume that it is a function call to self written generator
            value = value[1:]
            _c1_=0
            _c2_=0
            value_array = value.split(".")
            _name_module = value_array[0]
            _name_class = value_array[1]
            j = len(value_array[2])
            for i in range(0, len(value_array[2])): #0: module name, 1: class name, 2: function name + argument if any
                if value_array[2][i] == "(":
                    j = i
                    break
            _name_method = value_array[2][:j]
            if j == len(value_array[2]):
                _name_arg = " "
            else:
                if value_array[2][j+1] == ")":
                    _name_arg = " "
                else:
                    _name_arg = value_array[2][j+1:len(value_array[2])-1]
            the_module = __import__(_name_module)
            the_class = getattr(the_module, _name_class)()
            if _name_arg == " ":
                _a_ = getattr(the_class, _name_method)()
            else:
                _a_ = getattr(the_class, _name_method)(_name_arg)
        else:
            if isinstance(value, list):
                _a_ = value
                for i in range(0, len(_a_)):
                    _a_[i] = _int_float_str_gen(_a_[i])
            elif isinstance(value, dict):
                _a_ = value
                for _k, _v in _a_.items():
                    _a_[_k] = _int_float_str_gen(_v)
            elif value.startswith("$lis"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0, len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _a_ = []
                for i in range(0, _n_):
                    _a_.append(random.randint(0, 100))
            elif value.startswith("$int"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0, len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _x_ = pow(10, _n_)
                _a_ = random.randint(0, 1000000) % _x_
            elif value.startswith("$flo"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0, len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _x_ = pow(10, _n_)
                _a_ = random.random() % _x_
            elif value.startswith("$boo"):
                _n_ = random.randint(0, 10000)
                if _n_%2 == 0:
                    _a_ = True
                else:
                    _a_ = False
            elif value.startswith("$dic"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0, len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _a_ = {}
                for i in range(0, _n_):
                    _j_ = "a{0}".format(i)
                    _a_[_j_] = random.randint(0, 1000)
            elif "$str" in value:
                _x_ = value.find("$str")
                _val = value[_x_+4:]
                _n_ = 0
                j = 0
                if _val=="":
                    _n_ = 5
                else:
                    for i in range(0, len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                    if _val[0].isdigit():
                        value = value.replace(str(_n_), '')
                _out_ = _random_string(_n_)
                _a_ = value.replace("$str".format(_n_), _out_)
            else:
                _a_ = value

        kv.update({key : _a_})

    if template["size"] is not None:
        size_idx = random.randint(0, len(template["size"]) - 1)
        size = int(template['size'][size_idx])

        kv_size = sys.getsizeof(kv)/8
        if  kv_size < size:
            padding = _random_string(size - kv_size)
            kv.update({"padding" : padding})



def _random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def _int_float_str_gen(_str):
    if isinstance(_str, str):
        _str = str(_str)
    if isinstance(_str, str) and _str.startswith("$int"):
        _val = _str[4:]
        _n_ = 0
        if _val=="":
            _n_ = 5
        else:
            for k in range(0, len(_val)):
                if _val[k].isdigit():
                    _n_ = _n_*10 + int(_val[k])
                else:
                    break
        _x_ = pow(10, _n_)
        _temp_ = _str.replace("$int{0}".format(_n_), str(random.randint(0, 1000000) % _x_))
        return int(_temp_)
    elif isinstance(_str, str) and _str.startswith("$flo"):
        _val = _str[4:]
        _n_ = 0
        if _val=="":
            _n_ = 5
        else:
            for k in range(0, len(_val)):
                if _val[k].isdigit():
                    _n_ = _n_*10 + int(_val[k])
                else:
                    break
        _x_ = pow(10, _n_)
        _temp_ = _str.replace("$flo{0}".format(_n_), str((random.random()*1000000) % _x_))
        return float(_temp_)
    elif isinstance(_str, str) and _str.startswith("$str"):
        _val = _str[4:]
        _n_ = 0
        j = 0
        if _val=="":
            _n_ = 5
        else:
            for k in range(0, len(_val)):
                if _val[k].isdigit():
                    _n_ = _n_*10 + int(_val[k])
                else:
                    break
        return _random_string(_n_)
    else:
        return _str

