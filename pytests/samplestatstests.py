import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
from membase.performance.stats import StatsCollector


class SampleStatsTests(unittest.TestCase):

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.servers = TestInputSingleton.input.servers

    def test1(self):
        sc = StatsCollector(False)
         #nodes, bucket, pnames, frequency):
        sc.start([self.servers[0]], "default", ["memcached", "beam.smp"], str(uuid.uuid4()), 10)
        time.sleep(600)
        sc.stop()
        self.log.info("stopped the stat collection")
        sc.export("hi")
