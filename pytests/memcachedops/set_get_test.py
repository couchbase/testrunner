import os
import random
import unittest
import mc_bin_client
import socket
import zlib
import ctypes
import uuid

class SimpleSetGetTest(unittest.TestCase):
    keys = None
    clients = None
    ips = None

    def setUp(self):
        self.ips = self.extract_server_ips()
        self.clients = self.create_mc_bin_clients_for_ips(self.ips)
        #populate key
        testuuid = uuid.uuid4()
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(100)]

    # test case to set 100 keys
    def test_set(self):
        for ip in self.ips:
            print 'push 1000 new keys to memcached @ {0}'.format(ip)
            self.clients[ip].vbucketId = 0
            for key in self.keys:
                payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0', random.randint(100, 1024))
                flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
                try:
                    (opaque, cas, data) = self.clients[ip].set(key, 0, flag, payload)
                except:
                    self.fail("unable to push key{0} to bucket{1}".format(key, self.client.vbucketId))

    # test case to set 1000 keys and verify that those keys are stored
    def test_set_and_get(self):
        for ip in self.ips:
            print 'push 1000 new keys to memcached @ {0} and validate them after insertion'.format(ip)
            self.clients[ip].vbucketId = 0
            for key in self.keys:
                payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0',
                                                random.randint(100, 1024))
                flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
                try:
                    (opaque, cas, data) = self.clients[ip].set(key, 0, flag, payload)
                except:
                    self.fail("unable to push key : {0} to bucket : {1}".format(key, self.client.vbucketId))

            for key in self.keys:
                try:
                    flag, keyx, value = self.clients[ip].get(key=key)
                    hflag = socket.ntohl(flag)
                    #what are the flag ??
                #                if hflag == ctypes.c_uint32(zlib.adler32(value)).value:
                except mc_bin_client.MemcachedError:
                    self.fail("unable to get a pre-inserted key : {0}".format(key))

    def tearDown(self):
        #let's clean up the memcached
        for ip in self.ips:
            for key in self.keys:
                try:
                    self.clients[ip].delete(key=key)
                except mc_bin_client.MemcachedError:
                    print 'unable to delete key : {0} from memcached @ {1}'.format(key, ip)

            self.clients[ip].close()


    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

    def create_mc_bin_clients_for_ips(self, ips):
        clients = {}
        for ip in ips:
            clients[ip] = mc_bin_client.MemcachedClient(ip, 11211)
        return clients

    #move this to a common class
    def extract_server_ips(self):
        servers_string = os.getenv("SERVERS")
        servers = servers_string.split(" ")
        return [server[:server.index(':')] for server in servers]
