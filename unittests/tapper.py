from multiprocessing.queues import Queue
import sys
from threading import Thread
import uuid
import time
from mc_bin_client import MemcachedClient

import memcacheConstants
from memcached.helper.data_helper import MemcachedClientHelper
from resourceparser import ServerInfo
from membase.api.tap import TapConnection


class TapListener(Thread):
    def __init__(self, queue, server):
        Thread.__init__(self)
        self.queue = queue
        self.server = server
        self.stats = []
        self.aborted = False

    def run(self):
        self.tap()

    def callback(self, identifier, cmd, extra, key, vb, val, cas):

    #        if key == 'farshid':
    #        if cmd != 65 and cmd != 68:
    #            print cmd
        command_names = memcacheConstants.COMMAND_NAMES[cmd]
        if command_names != "CMD_TAP_MUTATION":
            print("%s: ``%s'' (vb:%d) -> (%d bytes from %s)" % (
            memcacheConstants.COMMAND_NAMES[cmd],
            key, vb, len(val), identifier))
            print(extra, cas)


    def tap(self):
        print("starting tap process")
        t = TapConnection(self.server, 11210, callback=self.callback, clientId=str(uuid.uuid4()),
#        opts={})
                          opts={memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff})
        while True and not self.aborted:
            t.receive()

sys.path.append("lib")
sys.path.append("pytests")

def tap(server, queue):
    listen = TapListener(queue, server)
    listen.tap()


queue = Queue(maxsize=10000)

server = ServerInfo()
server.ip = "10.17.12.20"

bucket = {'name': 'default', 'port': 11220, 'password': ''}
#vam = VBucketAwareMemcached(RestConnection(server), bucket)
#print vam.memcacheds
#print vam.vBucketMap
payload = MemcachedClientHelper.create_value('*', 10240)
keys = ["key_%d" % (i) for i in range(4000)]
#keys = ["key_%s_%d" % (str(uuid.uuid4()), i) for i in range(4)]
total_size = 0
#mc = MemcachedClientHelper.create_memcached_client("172.16.75.128","default",11210,"default")
mc = MemcachedClient("10.17.12.20", 11210)
#for key in keys:
#    vam.memcached(key).set(key, 1, 0, payload)
#    total_size += len(key) + len(payload) + 200
#time.sleep(10)
#for i in range(0,1023):
#    mc.set_vbucket_state(i, 'active')
new_thread = TapListener(queue, server)
new_thread.start()


i = 0
while i < 4000:
    for key in keys:
    #    vam.memcached(key).get(key)
        mc.set(key, 10, 0, payload, vbucket=0)
#    for key in keys:
    #    vam.memcached(key).get(key)
#        mc.set(key, 1, 0, payload, vbucket=0)
        try:
            a, b, c = mc.get(key, vbucket=0)
#            print c
        except:
            pass
    i += 1
#    print i



#for key in keys:
#    vam.memcached(key).get(key)
#    mc.set(key, 1, 0, payload, vbucket=0)
#    mc.get(key, vbucket=0)

#for key in keys:
#    vam.memcached(key).get(key)
#    mc.delete(key,vbucket=0)

time.sleep(10)

#    vam.memcached(key).delete(key)
#vam.done()
new_thread.aborted = True
time.sleep(30)
new_thread.join()
print("total_size", total_size)
#reader = Process(target=tap, args=(server, queue))
#reader.start()
#time.sleep(10)
#keys = []
#keys_count = 0
#was_empty = 0
#while was_empty < 50:
#    try:
#        key = queue.get(False, 5)
#
#        keys_count += 1
#        print key
#        keys.append(key)
#    except Empty:
#        print "exception thrown"
#        print "how many keys ? {0}".format(keys_count)
#        was_empty += 1
#
#reader.terminate()
#





