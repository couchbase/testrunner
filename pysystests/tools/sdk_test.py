import json
import eventlet
from random import randint
import sys
import time
import random
import string
sys.path = sys.path + ['.', '../']
import app.sdk_client_tasks as sdk

pool = eventlet.GreenPool(10000)

template={'name': 'default', 'flags': 0, 'kv': {'city': '$str5', 'num': '$int', 'b': '$boo', 'flo': '$flo', 'map': {'sample': '$str3', 'complex': '$flo1', 'val': '$int2'}, 'di': '$dic', 'li': '$lis', 'list': ['$int1', '$str1', '$flo1'], 'email': 'couchbase.com', 'st': '$str'}, 'ttl': 0, 'cc_queues': None, 'size': '512'}


def _do_mset(keys):
    sdk.mset(keys, template)

def _random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def generate_load(batch_size = 1000, num_key_sets = 5, loops = 1):

    for i in range(loops):
        key_sets = [ [_random_string(4)+str(i) for i in range(batch_size)] for j in range(num_key_sets) ]
        for res in pool.imap(_do_mset, key_sets):
            pass
        time.sleep(1)

if __name__ == '__main__':
    generate_load()
