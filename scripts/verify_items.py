#!/usr/bin/env python
"""Verify items exist, or were deleted from couchbase using an on disk kvstore"""

import time
import sys
import getopt
import pickle
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
    print("./verify_.py -m <master> -f <file>")
    print("")
    print(" master             the master node rest interface")
    print(" file               file to write out the kvstore to")
    print("")
    print("./verify_items -m Administrator:password@10.1.2.99:8091 -f kvstore")
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
        self.filename = None
        self.bucket = 'default'

        try:
            (opts, args) = getopt.getopt(argv, 'hm:f:b:', ['help', 'master=', 'file=', 'bucket='])
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
            if o == "-f" or o == "--file":
                self.filename = a
            if o == "-b" or o == "--bucket":
                self.bucket = a

        if not self.master:
            usage("missing master")
        if not self.filename:
            usage("missing file")


def get_aware(awareness, rest, key):
    timeout = 60 + time.time()
    passed = False
    while time.time() < timeout and not passed:
        try:
            val = awareness.memcached(key).get(key)
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
        raise Exception("failed get after 60 seconds")
    return val


if __name__ == "__main__":

    config = Config(sys.argv[1:])

    kv = KVStore(config.filename)

    rest = RestConnection(config.master)
    awareness = VBucketAwareMemcached(rest, config.bucket)

    undeleted = 0
    missing = 0
    badval = 0

    for key, val_expected in kv.items():
        if val_expected[3]:
            try:
                val = get_aware(awareness, rest, key)
                if val[2] != val_expected[2]:
                    badval += 1
            except mc_bin_client.MemcachedError as e:
                if e.status == 1:
                    missing += 1
                else:
                    raise e
        else:
            try:
                val = get_aware(awareness, rest, key)
                undeleted += 1
            except mc_bin_client.MemcachedError as e:
                if e.status != 1:
                    raise e

    awareness.done()

    print("undeleted:", undeleted)
    print("missing:", missing)
    print("badval:", badval)
