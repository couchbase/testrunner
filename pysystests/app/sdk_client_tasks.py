##!/usr/bin/env python

"""

memcached tasks 

"""

from __future__ import absolute_import
from app.celery import celery

import json
import eventlet

import testcfg as cfg

###
SDK_IP = '127.0.0.1'
SDK_PORT = 50008
SDK_PORT2 = 50009
###


@celery.task
def mset(keys, template, bucket = "default", isupdate = False):
    message = {"command" : "mset",
               "args" : keys,
               "template" : template,
               "bucket" : bucket}
    rc = _send_msg(message)
    #TODO: return failed keys
    return keys

@celery.task
def mget(keys, bucket = "default"):
    message = {"command" : "mget",
               "bucket" : bucket,
               "args" : keys}
    return  _send_msg(message)

@celery.task
def set(key, value, bucket = "default"):
    message = {"command" : "set",
               "bucket" : bucket,
               "args" : [key, 0, 0, value]}
    return  _send_msg(message)

@celery.task
def get(key, bucket = "default"):

    message = {"command" : "get",
               "bucket" : bucket,
               "args" : [key]}
    return  _send_msg(message)


@celery.task
def delete(key, bucket = "default"):
    message = {"command" : "delete",
               "bucket" : bucket,
               "args" : [key]}
    return  _send_msg(message)

@celery.task

def mdelete(keys, bucket = "default"):
    message = {"command" : "mdelete",
               "bucket" : bucket,
               "args" : keys}
    return  _send_msg(message)

def _send_msg(message):
    message.update({"cb_ip" : cfg.COUCHBASE_IP,
                    "cb_port" : cfg.COUCHBASE_PORT})

    sdk_client = eventlet.connect((SDK_IP, SDK_PORT))
    sdk_client.sendall(json.dumps(message))

