#this test will check the memory_left in the bucket and insert as many keys to fill up the memory
#it loops through all the servers in the list and fill the default bucket in each server
#cleanup will also delete these keys from the buckets
import os
import unittest
import mc_bin_client
import socket
import zlib
import ctypes
import uuid
from membase.api.rest_client import RestConnection
import logger
from mc_bin_client import MemcachedError

log = logger.Logger.get_logger()

class FillTheBucketTest(unittest.TestCase):
    keys = None
    clients = None
    ips = None

    def setUp(self):
        self.ips = self.extract_server_ips()
        self.clients = self.create_mc_bin_clients_for_ips(self.ips)
        testuuid = uuid.uuid4()
        # for 1 GB RAM - 1,000,000,000 - for a 100 byte packet = 10 million
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(100 * 1000)]

    # test case to set 1000 keys
    def test_fill_bucket(self):
        for ip in self.ips:

            log.info('rest connection : {0}'.format(ip))
            rest = RestConnection(ip=ip,
                                  username='Administrator',
                                  password='password')
            info = rest.get_bucket('default')
            emptySpace = info.stats.ram - info.stats.memUsed
            packetSize = emptySpace / ( 100 * 1000)
            packetSize = int(packetSize)
            log.info('packetSize: {0}'.format(packetSize))
            log.info('memory usage before key insertion : {0}'.format(info.stats.memUsed))
            log.info('inserting 10 * 1000 * 1000 new keys to memcached @ {0}'.format(ip))
            #TODO: vbucketId is not always 0 . FIX THIS
            self.clients[ip].vbucketId = 0
            self.values = {}
            count = 0
            for key in self.keys:
                payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0', packetSize)
                self.values[key] = payload
                flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
                try:
                    (opaque, cas, data) = self.clients[ip].set(key, 0, flag, payload)
                    if not count % 10000:
                        log.info('pushed in  the key # {0} to memcached @ {1}'.format(count,ip))
                    count += 1
                except MemcachedError as error:
                    print error
                    self.fail("unable to push key{0} to bucket{1}".format(key, self.clients[ip].vbucketId))
            #now verify each key
            for key in self.keys:
                flag, keyx, value = self.clients[ip].get(key)
                self.assertEquals(value,self.values[key],
                    msg = 'value retrieved different from value inserted for key {0}'.format(key))
            info = rest.get_bucket('default')
            log.info('memory usage after key insertion : {0}'.format(info.stats.memUsed))

    def tearDown(self):
        #let's clean up the memcached
        for ip in self.ips:
            for key in self.keys:
                try:
                    self.clients[ip].delete(key=key)
                except mc_bin_client.MemcachedError:
                    self.fail('unable to delete key : {0} from memcached @ {1}'.format(key, ip))

            self.clients[ip].close()


    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

    def create_mc_bin_clients_for_ips(self, ips):
        clients = {}
        for ip in ips:
            clients[ip] = mc_bin_client.MemcachedClient(ip, 11211)
        print clients
        return clients

    #move this to a common class
    def extract_server_ips(self):
        servers_string = os.getenv("SERVERS")
        print "SERVERS" , servers_string
        servers = servers_string.split(" ")
        return [server[:server.index(':')] for server in servers]
