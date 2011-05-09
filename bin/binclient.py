#!/usr/bin/env python

import mc_bin_client
import sys

if len(sys.argv) == 1:
    print "[sasl username:password] [cas casId] [expiry expiryTime] [vbucket vbucketId] <server:port> <op> <key> [value]"
    sys.exit(1)


if sys.argv[1] == "sasl":
    user_password = sys.argv[2].split(":")
    del(sys.argv[1])
    del(sys.argv[1])
else:
    user_password = ["",""]

if sys.argv[1] == "cas":
    cas = int(sys.argv[2])
    del(sys.argv[1])
    del(sys.argv[1])
else:
    cas = 0

if sys.argv[1] == "expiry":
    exp = int(sys.argv[2])
    del(sys.argv[1])
    del(sys.argv[1])
else:
    exp = 0

if sys.argv[1] == "vbucket":
    vbucketId = int(sys.argv[2])
    del(sys.argv[1])
    del(sys.argv[1])
else:
    vbucketId = 0

server_port = sys.argv[1].split(":")
server = server_port[0]
if len(server_port) > 1:
    port = int(server_port[1])
else:
    port = 11211

op = sys.argv[2]
if op.find('-') != -1:
    subops = op.split('-',1)[1:]
    op = op.split('-',1)[0]

if len(sys.argv) > 3:
    key = sys.argv[3]
else:
    key = ""

if len(sys.argv) > 4:
    value = sys.argv[4]
else:
    value = ""

user = user_password[0]
if len(user_password) > 1:
    password = user_password[1]
else:
    password = ""

mc = mc_bin_client.MemcachedClient(server, port)
mc.vbucketId = vbucketId
if len(user) + len(password) > 0:
    mc.sasl_auth_plain(user,password)

if op == 'exists':
    try:
        lc = mc.get(key)
        print "EXISTS", key, vbucketId
    except:
        print "MISSING", key, vbucketId

elif op == 'missing':
    sps = list(server_port.split(":") for server_port in server_port_arr)
    mcs = list(mc_bin_client.MemcachedClient(sp[0], int(sp[1])) for sp in sps)

    while True:
        reset = False
        found = 0
        excep = []

        for mc in mcs:
            mc.vbucketId = vbucketId
            try:
                mc.get(key)
                found = found + 1
            except Exception as ex:
                excep.append([mc.host, mc.port, ex])
                reset = True

        if found < 1:
            print "MISSING", key, "vbucket", vbucketId, found, excep

        if found > 1:
            print "OVER FOUND", key, "vbucket", vbucketId, found, excep

        if reset:
            for mc in mcs:
                mc.close()
            mcs = list(mc_bin_client.MemcachedClient(sp[0], int(sp[1])) for sp in sps)

elif op == 'get':
    lc = mc.get(key)
    print(lc[2])

elif op == 'gets':
    lc = mc.get(key)
    print(lc)

elif op == 'gat':
    lc = mc.gat(key, exp)
    print(lc[2])

elif op == 'gats':
    lc = mc.gat(key, exp)
    print(lc)

elif op == 'touch':
    mc.touch(key, exp)

elif op == 'set':
    mc.set(key, exp, 0, value)

elif op == 'cas':
    mc.cas(key, exp, 0, cas, value)

elif op == 'incr':
    lc = mc.incr(key, int(value))
    print(lc[0])

elif op == 'decr':
    lc = mc.decr(key, int(value))
    print(lc[0])

elif op == 'flush':
    mc.flush()

elif op == 'append':
    mc.append(key, value)

elif op == 'prepend':
    mc.prepend(key, value)

elif op == 'add':
    mc.add(key, 0, 0, value)

elif op == 'replace':
    mc.replace(key, 0, 0, value)

elif op == 'delete':
    mc.delete(key, cas)

elif op == 'sync':
    specs = [{'key':key, 'vbucket':vbucketId, 'cas':cas}]
    if '-'.join(subops) == 'mutation':
        print mc.sync_mutation(specs)
    elif '-'.join(subops) == 'persistence':
        print mc.sync_persistence(specs)
    elif '-'.join(subops) == 'replication':
        print mc.sync_replication(int(value), specs)
    elif '-'.join(subops) == 'replication-and-persistence' or '-'.join(subops) == 'persistence-and-replication':
        print mc.sync_replication_and_persistence(int(value), specs)
    elif '-'.join(subops) == 'replication-or-persistence' or '-'.join(subops) == 'persistence-or-replication':
        print mc.sync_replication_or_persistence(int(value), specs)
    else:
        print 'unknown command: ' + op + '-' + '-'.join(subops)

elif op == 'stats':
    stats = mc.stats()
    longest = max((len(x) + 2) for x in stats.keys())
    for stat, val in sorted(stats.items()):
        s = stat + ":"
        print "%s%s" % (s.ljust(longest), val)

else:
    print 'unknown command: ' + op
    sys.exit(1)
