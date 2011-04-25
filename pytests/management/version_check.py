import unittest
import TestInput
import logger
from membase.api.rest_client import RestConnection

log = logger.Logger.get_logger()

class VerifyVersionTest(unittest.TestCase):
    servers = None
    log = None
    input = TestInput.TestInput

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInput.TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers


    # read each server's version number and compare it to self.version
    def test_verify_version(self):
        expected_version = self.input.test_params['version']
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.log_client_error('test_verify_version test-method running')
            version = rest.get_pools()
            self.log.info('expected version : {0}'.format(expected_version))
            self.log.info('actual version : {0}'.format(version.implementationVersion))
            self.assertEqual(first=expected_version,
                             second=version.implementationVersion,
                             msg='version mismatch for server @ %s' % serverInfo.ip)