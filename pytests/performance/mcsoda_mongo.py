#!/usr/bin/env python

import sys
import math
import time
import mcsoda
import pymongo # Use: pip install pymongo==2.0

from pymongo import Connection

PERCENTILES = [0.90, 0.99]

mongoDocCache = {}

class StoreMongo(mcsoda.Store):

    def connect(self, target, user, pswd, cfg, cur):
        self.cfg = cfg
        self.cur = cur
        self.target = target
        self.host_port = (target + ":27017").split(':')[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.conn = Connection(self.host_port[0],
                               self.host_port[1])
        self.db = self.conn['default']
        self.coll = self.db['default']

    def gen_doc(self, key_num, key_str, min_value_size, json=None, cache=None):
        global mongoDocCache

        if json is None:
           json = self.cfg.get('json', 1) > 0
        if cache is None:
           cache = self.cfg.get('doc-cache', 0)

        d = None
        if cache:
            d = mongoDocCache.get(key_num, None)
        if d is None:
            d = { "_id": key_str,
                  "key_num": key_num,
                  "name": mcsoda.key_to_name(key_num, key_str),
                  "email": mcsoda.key_to_email(key_num, key_str),
                  "city": mcsoda.key_to_city(key_num, key_str),
                  "country": mcsoda.key_to_country(key_num, key_str),
                  "realm": mcsoda.key_to_realm(key_num, key_str),
                  "coins": max(0.0, int(key_str[0:4], 16) / 100.0),
                  "achievements": mcsoda.key_to_achievements(key_num, key_str) }
        if cache:
            mongoDocCache[key_num] = d

        return d

    def command(self, c):
        cmd, key_num, key_str, data = c
        cmd_start = time.time()
        if cmd[0] == 'g':
            self.coll.find_one(key_str)
        elif cmd[0] == 's':
            self.coll.save(data)
        elif cmd[0] == 'd':
            self.coll.remove(key_str)
        else:
            raise Exception("StoreMongo saw an unsupported cmd: " + cmd)
        cmd_end = time.time()

        histo = self.add_timing_sample(cmd, cmd_end - cmd_start)
        if self.sc:
            p = self.histo_percentile(histo, PERCENTILES)
            self.sc.latency_stats(cmd, p)


if __name__ == "__main__":
    if sys.argv[1].find("mongo") != 0:
        raise Exception("usage: mcsoda_mongo mongo://HOST:27017 ...")

    argv = (' '.join(sys.argv) + ' doc-gen=0').split(' ')

    mcsoda.main(argv, protocol="mongo", stores=[StoreMongo()])
