import json
import eventlet
from random import randint
import sys
import time
import random
import string
sys.path = sys.path + ['.','../']
import app.sdk_client_tasks as sdk

pool = eventlet.GreenPool(10000)

template={'name': 'default', 'flags': 0, 'kv': {u'city': u'$str5', u'num': u'$int', u'b': u'$boo', u'flo': u'$flo', u'map': {u'sample': u'$str3', u'complex': u'$flo1', u'val': u'$int2'}, u'di': u'$dic', u'li': u'$lis', u'list': [u'$int1', u'$str1', u'$flo1'], u'email': u'couchbase.com', u'st': u'$str'}, 'ttl': 0, 'cc_queues': None, 'size': '512'}


def _do_mset(keys):
    sdk.mset(keys, template)

def _random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def generate_load(batch_size = 1000, num_key_sets = 5, loops = 1):

    for i in xrange(loops):
        key_sets = [ [_random_string(4)+str(i) for i in xrange(batch_size)] for j in xrange(num_key_sets) ]
        for res in pool.imap(_do_mset, key_sets):
            pass
        time.sleep(1)

if __name__ == '__main__':
    generate_load()
