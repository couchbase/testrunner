#!/usr/bin/env python

import mc_bin_client
import sys

if len(sys.argv) == 1:
	print "[sasl username:password] [vbucket vbucketId] <server:port> <op> <key> [value]"
	sys.exit(1)


if sys.argv[1] == "sasl":
	user_password = sys.argv[2].split(":")
	del(sys.argv[1])
	del(sys.argv[1])
else:
	user_password = ["",""]

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

if op == 'get':
	lc = mc.get(key)
	print(lc[2])

elif op == 'set':
	mc.set(key, 0, 0, value)

elif op == 'incr':
	lc = mc.incr(key, int(value))
	print(lc[0])

elif op == 'decr':
	lc = mc.decr(key, value)
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
	mc.delete(key)

elif op == 'stats':
	stats = mc.stats()
	longest = max((len(x) + 2) for x in stats.keys())
	for stat, val in sorted(stats.items()):
		s = stat + ":"
		print "%s%s" % (s.ljust(longest), val)

else:
	sys.exit(1)
