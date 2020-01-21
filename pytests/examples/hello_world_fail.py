import unittest
import TestInput
import logger

class HelloWorldFailTest(unittest.TestCase):

    input = None
    log = None
    
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInput.TestInputSingleton.input
        self.log.info('test setUp invoked')
        self.log.info('getting server ips')
        servers = self.input.servers
        for server in servers:
            self.log.info("server ip : {0}".format(server.ip))

    def test_hello_fail_1(self):
        self.log.info('running test_hello_fail_1')
        i = 1
        self.assertEqual(i, 2)

    def test_hello_fail_2(self):
        self.log.info('running test_hello_fail_2')
        i = 2
        self.assertEqual(i, 3)

    def tearDown(self):
        self.log.info('tearDown invoked')