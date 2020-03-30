#!/usr/bin/env python

import sys
import copy
import time
from . import mcsoda

from pymongo import Connection # Use: pip install pymongo==2.0

mongoDocCache = {}

class StoreMongo(mcsoda.Store):

    def connect(self, target, user, pswd, cfg, cur, bucket=None):
        self.cfg = cfg
        self.cur = cur
        self.target = target
        self.host_port = (target + ":27017").split(':')[0:2]
        self.host_port[1] = int(self.host_port[1])
        self.conn = Connection(self.host_port[0],
                               self.host_port[1])
        self.db = self.conn['default']
        self.coll = self.db['default']
        self.xfer_sent = 0
        self.xfer_recv = 0

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
                  "name": mcsoda.key_to_name(key_str),
                  "email": mcsoda.key_to_email(key_str),
                  "city": mcsoda.key_to_city(key_str),
                  "country": mcsoda.key_to_country(key_str),
                  "realm": mcsoda.key_to_realm(key_str),
                  "coins": mcsoda.key_to_coins(key_str),
                  "category": mcsoda.key_to_category(key_str),
                  "achievements": mcsoda.key_to_achievements(key_str) }
        if cache:
            mongoDocCache[key_num] = d

        d = copy.deepcopy(d)
        d['body'] = self.cfg['body'][min_value_size]
        return d

    def command(self, c):
        cmd, key_num, key_str, data, expiration = c
        cmd_start = time.time()
        if cmd[0] == 'g' or cmd[0] == 'q':
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
            p = self.histo_percentile(histo, [0.90, 0.95, 0.99])
            self.sc.latency_stats(cmd, p)


if __name__ == "__main__":
    if sys.argv[1].find("mongo") != 0:
        raise Exception("usage: mcsoda_mongo mongo://HOST:27017 ...")

    argv = (' '.join(sys.argv) + ' doc-gen=0').split(' ')

    mcsoda.main(argv, protocol="mongo", stores=[StoreMongo()])
