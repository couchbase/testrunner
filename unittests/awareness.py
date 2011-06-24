import sys
import uuid

from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached
from resourceparser import ServerInfo

sys.path.append("lib")
sys.path.append("pytests")

server = ServerInfo()
server.ip = "10.1.6.129"
server.rest_username = 'Administrator'
server.rest_password = 'password'
rest = RestConnection(server)
bucket = {'name': 'default', 'port': 11210, 'password': ''}
vam = VBucketAwareMemcached(rest, bucket)
print vam.memcacheds
print vam.vBucketMap
keys = ["key_%s_%d" % (str(uuid.uuid4()), i) for i in range(100)]
for key in keys:
    print vam.memcached(key).set(key, 0, 0, key)
    vam.memcached(key).get(key)

vam.done()