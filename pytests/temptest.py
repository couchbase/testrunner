from TestInput import TestInputSingleton
import logger

import unittest
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper


class TempTest(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def test_create(self):
        master = self._servers[0]
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username,
                          password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket='default',
                           ramQuotaMB=bucket_ram,
                           replicaNumber=1,
                           proxyPort=11211)

    def test_flush(self):
        try:
            MemcachedClientHelper.flush_bucket(self._servers[0], 'default', 11211)
        except Exception:
            pass