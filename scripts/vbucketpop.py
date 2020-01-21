#!/usr/bin/env python
"""Load data into memcached and an on disk key value store."""

import sys
import getopt
import mc_bin_client
import pickle

def usage(err=None):
    err_code = 0
    if err:
        err_code = 1
        print("Error:", err)
        print()
    print("./vbucketpop.py -i <server> -p <port> -b <bucket> -P <password> -f <file>")
    print("")
    print(" server             Server IP")
    print(" port               moxi port")
    print(" bucket             bucket (default)")
    print(" password           saslbucket password ('')")
    print(" file               kvstore filename")
    print("")
    print("Default bucket")
    print("./vbucketpop.py -i 127.0.0.1 -p 11211 -b default -f kvstore")
    print("Bucket with no password")
    print("./vbucketpop.py -i 127.0.0.1 -p 11211 -b temp -P '' -f kvstore")
    print("Sasl bucket with password")
    print("./vbucketpop.py -i 127.0.0.1 -p 11211 -b temp -P temp -f kvstore")
    sys.exit(err_code)

class Config(object):
    def __init__(self, argv):
        # defaults
        self.server = '127.0.0.1'
        self.port = 11211
        self.bucket = 'default'
        self.password = ''
        self.filename = ''

        try:
            (opts, args) = getopt.getopt(argv, 'hi:p:b:P:f:', ['help', 'server=', 'port=',
                                                              'bucket=', 'password=', 'filename='])
        except IndexError:
            usage()
        except getopt.GetoptError as err:
            usage(err)

        for o, a in opts:
            if o == "-h" or o == "--help":
                usage()
            if o == "-i" or o == "--server":
                self.server = a
            if o == "-p" or o == "--port":
                self.port = int(a)
            if o == "-b" or o == "--bucket":
                self.bucket = a
            if o == "-P" or o == "--password":
                self.password = a
            if o == "-f" or o == "--file":
                self.filename = a

        if not self.filename:
            usage("Missing kvstore file")

if __name__ == "__main__":

    config = Config(sys.argv[1:])
    store = {}

    # Read the kvstore
    try:
        f = open(config.filename, 'r')
    except IOError:
        print("Error opening kvstore file %s " % config.filename)
    else:
        store = pickle.load(f)
        f.close()

    # Create client connection
    client = mc_bin_client.MemcachedClient(config.server, config.port)
    if config.bucket != 'default':
            client.sasl_auth_plain(config.bucket,
                                   config.password)

    for vbucketid, keys in list(store.items()):
        print("Loading %s vbucketId with %s keys" % (vbucketid, keys))
        for key in keys:
            try:
                client.add(key, 0, 0, key)
            except mc_bin_client.MemcachedError as e:
                print(e)
