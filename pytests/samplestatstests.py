import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
from performance.stats import StatsCollector


class SampleStatsTests(unittest.TestCase):

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.servers = TestInputSingleton.input.servers

    def test1(self):
        sc = StatsCollector(False)
         #nodes, bucket, pnames, frequency):
        _id = sc.start([self.servers[0]], "default", ["memcached", "beam.smp"], str(uuid.uuid4()), 1)
        time.sleep(10)
        sc.stop(_id)
        self.log.info("stopped the stat collection")
        sc.export(_id, "hi")