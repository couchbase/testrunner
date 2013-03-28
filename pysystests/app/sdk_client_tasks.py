##!/usr/bin/env python

"""

memcached tasks

"""

from __future__ import absolute_import
from app.celery import celery
import random
import string
import sys
import copy
import time
import zlib

sys.path=["../lib"] + sys.path
from mc_bin_client import MemcachedClient, MemcachedError
from app.rest_client_tasks import create_rest

from cache import CacheHelper
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

import json
import eventlet
from random import randint

import testcfg as cfg

###
SDK_IP = '127.0.0.1'
SDK_PORT = 50008
SDK_PORT2 = 50009
###



@celery.task
def mset(keys, template, bucket = "default", isupdate = False, password = "", hosts = None):

    # decode magic strings in template before sending to sdk
    # python pass-by-reference updates attribute in function
    rawTemplate = copy.deepcopy(template)
    decodeMajgicStrings(rawTemplate)

    message = {"command" : "mset",
               "args" : keys,
               "template" : rawTemplate,
               "bucket" : bucket,
               "password" : password,
               "hosts"  : hosts}
    rc = _send_msg(message)

    return keys, rawTemplate

@celery.task
def mget(keys, bucket = "default", password = "", hosts = None):
    message = {"command" : "mget",
               "bucket" : bucket,
               "password" : password,
               "args" : keys,
               "hosts" : hosts}
    return  _send_msg(message)

@celery.task
def set(key, value, bucket = "default", password = ""):
    message = {"command" : "set",
               "bucket" : bucket,
               "password" : password,
               "args" : [key, 0, 0, value]}
    return  _send_msg(message)

@celery.task
def get(key, bucket = "default", password = ""):

    message = {"command" : "get",
               "bucket" : bucket,
               "password" : password,
               "args" : [key]}
    return  _send_msg(message)


@celery.task
def delete(key, bucket = "default", password = ""):
    message = {"command" : "delete",
               "bucket" : bucket,
               "password" : password,
               "args" : [key]}
    return  _send_msg(message)

@celery.task
def mdelete(keys, bucket = "default", password = "", hosts = None):
    message = {"command" : "mdelete",
               "bucket" : bucket,
               "password" : password,
               "args" : keys,
               "hosts" : hosts}
    return  _send_msg(message)

def _send_msg(message, response=False):

    hostConfig =  {"cb_ip" : cfg.COUCHBASE_IP,
                   "cb_port" : cfg.COUCHBASE_PORT}


    if 'hosts' in message and message['hosts'] is not None:
        if len(message['hosts']) > 0:
                hosts = message['hosts']
                host = hosts[random.randint(0,len(hosts) - 1)]

                if host is not None:
                    hostConfig["cb_ip"], hostConfig["cb_port"] = host.split(":")
                else:
                    # cluster status with no hosts means cluster down
                    return

    message.update(hostConfig)

    rc = None
    blocking = response

    try:
        port = randint(50008, 50011)
        sdk_client = eventlet.connect((SDK_IP, port))
        sdk_client.setblocking(blocking)
        sdk_client.settimeout(5)
        sdk_client.sendall(json.dumps(message))
        if response:
            time.sleep(1)
            sdk_client.sendall('\0')
            rc = sdk_client.recv(1024)

    except Exception as ex:
        logger.error(ex)
        logger.error("message suppressed: %s" % message["command"])

    return rc

"""
" sdk op_latency task.
"
" op: (set,get,delete).
" args: operation arguments. i.e set => ('key', 0, 0, 'val')
"
" returns amount of time required to complete operation via sdks
"""
@celery.task
def sdk_op_latency(op, args, bucket = "default", password = ""):

    message = {"command" : "latency",
               "bucket" : bucket,
               "password" : password,
               "op" : op,
               "args" : args}

    latency = _send_msg(message, response = True)

    return latency

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
        logger.error(msg)

    return latency

def getDirectMC(key, ip, port = 8091, bucket = "default", password = ""):

    real_mc_client = None

    # get initial mc client
    client = MemcachedClient(ip, int(port))
    vbId = (((zlib.crc32(key)) >> 16) & 0x7fff) & (client.vbucket_count - 1)

    # get vbucket map
    rest = create_rest(ip, port)
    vbuckets = rest.get_vbuckets(bucket)

    # find vbucket responsible to this key and mapping host
    if vbuckets is not None:

        vbucket = [vbucket for vbucket in vbuckets if vbucket.id == vbId]

        if len(vbucket) == 1:
            mc_ip, mc_port = vbucket[0].master.split(":")
            real_mc_client = MemcachedClient(mc_ip, int(mc_port))
            real_mc_client.sasl_auth_plain(bucket, password)

    return real_mc_client

def decodeMajgicStrings(template):

    kv = template["kv"]

    for key, value in kv.iteritems():
        if type(value)==str and value.startswith("@"):
            #If string starts with "@", assume that it is a function call to self written generator
            value = value[1:]
            _c1_=0
            _c2_=0
            value_array = value.split(".")
            _name_module = value_array[0]
            _name_class = value_array[1]
            j = len(value_array[2])
            for i in range(0,len(value_array[2])): #0: module name, 1: class name, 2: function name + argument if any
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
            if type(value)==list:
                _a_ = value
                for i in range(0,len(_a_)):
                    _a_[i] = _int_float_str_gen(_a_[i])
            elif type(value)==dict:
                _a_ = value
                for _k,_v in _a_.iteritems():
                    _a_[_k] = _int_float_str_gen(_v)
            elif value.startswith("$lis"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0,len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _a_ = []
                for i in range(0,_n_):
                    _a_.append(random.randint(0,100))
            elif value.startswith("$int"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0,len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _x_ = pow(10, _n_)
                _a_ = random.randint(0,1000000) % _x_
            elif value.startswith("$flo"):
                _val = value[4:]
                _n_ = 0
                if _val=="":
                   _n_ = 5
                else:
                    for i in range(0,len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _x_ = pow(10, _n_)
                _a_ = random.random() % _x_
            elif value.startswith("$boo"):
                _n_ = random.randint(0,10000)
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
                    for i in range(0,len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                _a_ = {}
                for i in range(0,_n_):
                    _j_ = "a{0}".format(i)
                    _a_[_j_] = random.randint(0,1000)
            elif "$str" in value:
                _x_ = value.find("$str")
                _val = value[_x_+4:]
                _n_ = 0
                j = 0
                if _val=="":
                    _n_ = 5
                else:
                    for i in range(0,len(_val)):
                        if _val[i].isdigit():
                            _n_ = _n_*10 + int(_val[i])
                        else:
                            break
                    if _val[0].isdigit():
                        value = value.replace(str(_n_),'')
                _out_ = _random_string(_n_)
                _a_ = value.replace("$str".format(_n_),_out_)
            else:
                _a_ = value

        kv.update({key : _a_})

    if template["size"] is not None:
        size_idx = random.randint(0,len(template["size"]) - 1)
        size = int(template['size'][size_idx])

        kv_size = sys.getsizeof(kv)/8
        if  kv_size < size:
            padding = _random_string(size - kv_size)
            kv.update({"padding" : padding})



def _random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def _int_float_str_gen(_str):
    if type(_str)==unicode:
        _str = str(_str)
    if type(_str)==str and _str.startswith("$int"):
        _val = _str[4:]
        _n_ = 0
        if _val=="":
            _n_ = 5
        else:
            for k in range(0,len(_val)):
                if _val[k].isdigit():
                    _n_ = _n_*10 + int(_val[k])
                else:
                    break
        _x_ = pow(10, _n_)
        _temp_ = _str.replace("$int{0}".format(_n_),str(random.randint(0,1000000) % _x_))
        return int(_temp_)
    elif type(_str)==str and _str.startswith("$flo"):
        _val = _str[4:]
        _n_ = 0
        if _val=="":
            _n_ = 5
        else:
            for k in range(0,len(_val)):
                if _val[k].isdigit():
                    _n_ = _n_*10 + int(_val[k])
                else:
                    break
        _x_ = pow(10, _n_)
        _temp_ = _str.replace("$flo{0}".format(_n_),str((random.random()*1000000) % _x_))
        return float(_temp_)
    elif type(_str)==str and _str.startswith("$str"):
        _val = _str[4:]
        _n_ = 0
        j = 0
        if _val=="":
            _n_ = 5
        else:
            for k in range(0,len(_val)):
                if _val[k].isdigit():
                    _n_ = _n_*10 + int(_val[k])
                else:
                    break
        return _random_string(_n_)
    else:
        return _str

