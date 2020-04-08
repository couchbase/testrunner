import sys
import uuid

from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from resourceparser import ServerInfo

sys.path.append("lib")
sys.path.append("pytests")

server = ServerInfo()
server.ip = "127.0.0.1"
server.rest_username = 'Administrator'
server.rest_password = 'password'
server.port = 9000
rest = RestConnection(server)
nodes = rest.node_statuses()

vm = VBucketAwareMemcached(rest, {"name":"bucket-0","password":""})
key = str(uuid.uuid4())
vm.memcached(key).set(key, 0, 0, "hi")
vm.memcached(key).get(key)

RebalanceHelper.print_taps_from_all_nodes(rest, bucket="bucket-0", password="")
RebalanceHelper.verify_items_count(server, "bucket-0")
RebalanceHelper.verify_items_count(server, "bucket-1")
RebalanceHelper.verify_items_count(server, "bucket-2")
RebalanceHelper.wait_till_total_numbers_match(server, "bucket-0", 120, "")


cm = MemcachedClientHelper.proxy_client(server, "bucket-0", "")
key = str(uuid.uuid4())
cm.set(key, 0, 0, "hi")
cm.get(key)



cm1 = MemcachedClientHelper.direct_client(server, "default", "")
key = str(uuid.uuid4())
cm1.set(key, 0, 0, "hi")
cm1.get(key)


c1 = MemcachedClientHelper.proxy_client(server, "hello", "")
key = str(uuid.uuid4())
c1.set(key, 0, 0, "hi")
c1.get(key)

c2 = MemcachedClientHelper.direct_client(server, "hello", "")
key = str(uuid.uuid4())
c2.set(key, 0, 0, "hi")
c2.get(key)

d1 = MemcachedClientHelper.direct_client(server, "hello1", "password")
key = str(uuid.uuid4())
d1.set(key, 0, 0, "hi")
d1.get(key)





#bucket = {'name': 'default', 'port': 11220, 'password': 'replica-1-ram-10-a4b4b36e-697b-4749-83a3-1f2cc404f549'}
#vam = VBucketAwareMemcached(rest, bucket)
#print vam.memcacheds
#print vam.vBucketMap
#keys = ["key_%s_%d" % (str(uuid.uuid4()), i) for i in range(100)]
#for key in keys:
#    vam.memcached(key).send_set(key, 0, 0, key)
#time.sleep(10)
#for key in keys:
#    vam.memcached(key).get(key)
#
#vam.done()