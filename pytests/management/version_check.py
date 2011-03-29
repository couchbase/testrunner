import unittest
import os
from membase.api.rest_client import RestConnection

class VerifyVersionTest(unittest.TestCase):
    version = None
    ips = None

    def setUp(self):
        self.version = os.getenv("VERSION")
        self.ips = self.extract_server_ips()

    # read each server's version number and compare it to self.version
    def test_verify_version(self):
        for ip in self.ips:
            rest = RestConnection(ip=ip,
                                  username='Administrator',
                                  password='password')
            version = rest.get_pools()
            if version.implementationVersion != self.version:
                self.fail("version mismatch for server @ {0}")
                self.assertEqual(first=self.version,
                                 second=version.implementationVersion,
                                 msg="version mismatch for server @ {0}".format(ip))


    def extract_server_ips(self):
        servers_string = os.getenv("SERVERS")
        servers = servers_string.split(" ")
        return [server[:server.index(':')] for server in servers]