import json
from threading import Thread
from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from remote.remote_util import RemoteMachineShellConnection
from pprint import pprint
from testconstants import CLI_COMMANDS
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH
from testconstants import MAC_COUCHBASE_BIN_PATH
from security.auditmain import audit
import socket
import commands

import urllib2
import stat
import os


"""
   Do the standard port check - heartbleed etc

   This test requires only a one node cluster.

   It it largely based on https://testssl.sh.

"""

class portscan(BaseTestCase):
    def setUp(self):
        super(portscan, self).setUp()




    def tearDown(self):
        super(portscan, self).tearDown()




    def get_the_testssl_script(self, testssl_file_name):
            # get the testssl script
        attempts = 0
        #print 'getting test ssl', testssl_file_name

        while attempts < 1:
            try:
                response = urllib2.urlopen("http://testssl.sh/testssl.sh", timeout = 5)
                content = response.read()
                f = open(testssl_file_name, 'w' )
                f.write( content )
                f.close()
                break
            except urllib2.URLError as e:
                attempts += 1
                print 'have an exception getting testssl', e


        st = os.stat(testssl_file_name)
        os.chmod(testssl_file_name, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


    def checkPortSecurity(self):

        TEST_SSL_FILENAME = '/tmp/testssl.sh'


        self.get_the_testssl_script(TEST_SSL_FILENAME)



        ports_to_check = [11207,11214, 18091, 18092]
        all_passed = True
        check_count = 0
        # Note this analysis is very coupled to the testssl.sh output format. If it changes this code may break.
        # I did ask the testssl person if he would do a machine readable output - EC - 12/14/2015


        for i in ports_to_check:
            self.log.info('Testing port {0}'.format(i))
            cmd  = TEST_SSL_FILENAME + ' --color 0 {0}:{1}'.format( self.master.ip, i)
            #print 'cmd is', cmd
            res = os.popen(cmd).read().split('\n')
            for r in res:
                # check vulnerabilities
                if 'Heartbleed (CVE-2014-0160)' in r:
                    check_count = check_count + 1
                    if 'VULNERABLE (NOT ok)' in r:
                        self.log.error('Heartbleed vulnerability on port {0}'.format(i))
                        all_passed = False


                elif 'POODLE, SSL (CVE-2014-3566)' in r:
                    check_count = check_count + 1
                    if 'VULNERABLE (NOT ok)' in r:
                        self.log.error('Poodle vulnerability on port {0}'.format(i))
                        all_passed = False



                elif 'LOGJAM (CVE-2015-4000)' in r:
                    check_count = check_count + 1
                    if 'VULNERABLE (NOT ok)' in r:
                        self.log.error('Poodle vulnerability on port {0}'.format(i))
                        all_passed = False



                # check protocols
                elif 'SSLv2' in r or 'SSLv3' in r:
                    check_count = check_count + 1
                    if  'offered (NOT ok)'  in r:
                        self.log.error('SSLvx is offered on port {0}'.format(i))
                        all_passed = False

                elif 'RC4 (CVE-2013-2566, CVE-2015-2808)' in r:
                    check_count = check_count + 1
                    if 'VULNERABLE (NOT ok)' in r:
                        self.log.error('RC4 is offered on port {0}'.format(i))
                        all_passed = False

                elif 'Medium grade encryption' in r:
                    check_count = check_count + 1
                    if 'not offered (OK)' not in r:
                        self.log.error('Medium grade encryption is offered on port {0}'.format(i))
                        all_passed = False




        self.assertTrue( all_passed, msg='Check logs for failures')


        # make sure all the tests were seen
        self.assertTrue( check_count==6, msg='Not all checks present')
