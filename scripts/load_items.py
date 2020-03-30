#!/usr/bin/env python
"""Load data into memcached and an on disk key value store."""

import time
import sys
import getopt
import pickle
import uuid
import random
import re
import mc_bin_client
import exceptions
import socket

from memcached.helper.data_helper import VBucketAwareMemcached
from membase.api.rest_client import RestConnection


def usage(err=None):
    err_code = 0
    if err:
        err_code = 1
        print("Error:", err)
        print()
    print("./load_items.py -m <master> -p <prefix> -o <# of sets>:<# of mutations>:<# of deletes> -f <file>")
    print("")
    print(" master             the master node rest interface")
    print(" prefix             prefix to use for key names")
    print(" operations         number of sets, mutations and deletes to do")
    print(" file               file to write out the kvstore to")
    print("")
    print("./load_items -m Administrator:password@10.1.2.99:8091 -p \"1-\" -o 1000:100:10 -f kvstore")
    sys.exit(err_code)

class KVStore(object):
    """Simple key value store that handles sets and deletes, has the ability to read and write from a file"""

    def __init__(self, filename=None):
        if filename:
            with open(filename) as f:
                self.data = pickle.load(f)
        else:
            self.data = {}

        self.index = 0

    def save(self, filename):
        """Write out the current kvstore to a pickle file"""
        with open(filename, 'w') as f:
            pickle.dump(self.data, f)

    def set(self, key, exp, flags, val):
        """Memcached set"""
        if exp and exp <= 2592000:
            exp += int(time.time())
        self.data[key] = (exp, flags, val, True)

    def delete(self, key):
        """Delete an item, but don't remove it from the kvstore completely"""
        if key in self.data:
            self.data[key] = (0, 0, "", False)

    def __iter__(self):
        return self.data.__iter__()

    def iteritems(self):
        return iter(self.data.items())


class Config(object):
    def __init__(self, argv):
        # defaults
        self.master = None
        self.prefix = ""
        self.sets = 0
        self.mutations = 0
        self.deletes = 0
        self.filename = None
        self.bucket = 'default'

        try:
            (opts, args) = getopt.getopt(argv, 'hm:p:o:f:b:', ['help', 'master=', 'prefix=', 'operations=', 'file=', 'bucket='])
        except IndexError:
            usage()
        except getopt.GetoptError as err:
            usage(err)

        for o, a in opts:
            if o == "-h" or o == "--help":
                usage()
            if o == "-m" or o == "--master":
                master_list = re.split('[:@]', a)
                self.master = {}
                self.master["username"] = master_list[0]
                self.master["password"] = master_list[1]
                self.master["ip"] = master_list[2]
                self.master["port"] = master_list[3]
            if o == "-p" or o == "--prefix":
                self.prefix = a
            if o == "-o" or o == "--operations":
                operations = a.split(":")
                self.sets = int(operations[0])
                self.mutations = int(operations[1])
                self.deletes = int(operations[2])
            if o == "-f" or o == "--file":
                self.filename = a
            if o == "-b" or o == "--bucket":
                self.bucket = a

        if not self.master:
            usage("missing master")
        if not self.filename:
            usage("missing file")


def set_aware(awareness, rest, key, exp, flags, val):
    timeout = 60 + time.time()
    passed = False
    while time.time() < timeout and not passed:
        try:
            awareness.memcached(key).set(key, exp, flags, value)
            passed = True
        except mc_bin_client.MemcachedError as e:
            if e.status == 7:
                awareness.reset_vbuckets(rest, key)
            else:
                raise e
        except exceptions.EOFError:
            awareness.reset(rest)
        except socket.error:
            awareness.reset(rest)
    if not passed:
        raise Exception("failed set after 60 seconds")

def delete_aware(awareness, rest, key):
    timeout = 60 + time.time()
    passed = False
    while time.time() < timeout and not passed:
        try:
            awareness.memcached(key).delete(key)
            passed = True
        except mc_bin_client.MemcachedError as e:
            if e.status == 7:
                awareness.reset_vbuckets(rest, key)
            else:
                raise e
        except exceptions.EOFError:
            awareness.reset(rest)
        except socket.error:
            awareness.reset(rest)
    if not passed:
        raise Exception("failed delete after 60 seconds")


if __name__ == "__main__":

    config = Config(sys.argv[1:])

    kv = KVStore()

    rest = RestConnection(config.master)
    awareness = VBucketAwareMemcached(rest, config.bucket)

    for i in range(config.sets):
        key = config.prefix + str(i)
        value = str(uuid.uuid4())
        kv.set(key, 0, 0, value)
        set_aware(awareness, rest, key, 0, 0, value)

    for i in range(config.mutations):
        key = config.prefix + str(random.randint(0, config.sets))
        value = str(uuid.uuid4())
        kv.set(key, 0, 0, value)
        set_aware(awareness, rest, key, 0, 0, value)

    for i in range(config.deletes):
        key = config.prefix + str(i)
        kv.delete(key)
        delete_aware(awareness, rest, key)

    awareness.done()
    kv.save(config.filename)
