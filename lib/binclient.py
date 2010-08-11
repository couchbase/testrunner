#!/usr/bin/env python

import mc_bin_client
import sys

port = 11211

server = sys.argv[1];

mc = mc_bin_client.MemcachedClient(server, port)

if sys.argv[2] == 'get':
	lc = mc.get(sys.argv[3])
	print(lc[2])

elif sys.argv[2] == 'set':
	mc.set(sys.argv[3], 0, 0, sys.argv[4])

elif sys.argv[2] == 'incr':
	lc = mc.incr(sys.argv[3], int(sys.argv[4]))
	print(lc[0])

elif sys.argv[2] == 'decr':
	lc = mc.decr(sys.argv[3], sys.argv[4])
	print(lc[0])

elif sys.argv[2] == 'flush':
	mc.flush()

elif sys.argv[2] == 'append':
	mc.append(sys.argv[3], sys.argv[4])

elif sys.argv[2] == 'prepend':
	mc.prepend(sys.argv[3], sys.argv[4])

elif sys.argv[2] == 'add':
	mc.add(sys.argv[3], 0, 0, sys.argv[4])

elif sys.argv[2] == 'replace':
	mc.replace(sys.argv[3], 0, 0, sys.argv[4])

else:
	sys.exit(1)
