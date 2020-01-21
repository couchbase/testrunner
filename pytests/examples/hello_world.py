import unittest
import TestInput
import logger


class HelloWorldTest(unittest.TestCase):

    log = None
    input = TestInput.TestInput

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInput.TestInputSingleton.input
        self.log.info('test setUp invoked')
        self.log.info('getting server ips')
        servers = self.input.servers
        for server in servers:
            self.log.info("server ip : {0}".format(server.ip))

    def test_hello_1(self):
        self.log.info('running hello_1')
        i = 1
        self.assertEqual(i, 1)

    def test_hello_2(self):
        self.log.info('running hello_2')
        i = 2
        self.assertEqual(i, 2)

    def tearDown(self):
        self.log.info('tearDown invoked')