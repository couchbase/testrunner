#!/usr/bin/env python2.6

import time
import json

import load_runner

class ServerInfo(object):
    def __init__(self, ip):
        self.ip = ip


def default(o):
    if isinstance(o, ServerInfo):
        return o.__dict__
    else:
        return json.JSONEncoder.default(o)


load_info = {
    'server_info': [
        ServerInfo("127.0.0.1"),
#        ServerInfo("10.80.122.208"),
#        ServerInfo("10.77.21.230"),
#        ServerInfo("10.83.25.146"),
    ],
    'memcached_info': {
        'bucket_name': "",
        'bucket_port': "11211",
        'bucket_password': "",
    },
    'operation_info': {
        'operation_distribution': {'set':3, 'get':5},
        'valuesize_distribution': {250:30, 1500:5, 20:5},
        'create_percent': 25,
        'threads': 8,
    },
    'limit_info': {
        'max_items': 0,
        'operation_count': 0,
        'time': 10,
        'max_size': 0,
    },
}

load = load_runner.LoadRunner(load_info, dryrun=False)

load.start()
time.sleep(60)
load.stop()
load.wait()
