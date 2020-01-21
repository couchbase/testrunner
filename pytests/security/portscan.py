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
import subprocess

import urllib.request, urllib.error, urllib.parse
import stat
import os
import traceback
import time
from subprocess import Popen, PIPE

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
                response = urllib.request.urlopen("http://testssl.sh/testssl.sh", timeout = 5)
                content = response.read()
                f = open(testssl_file_name, 'w' )
                f.write( content )
                f.close()
                break
            except urllib.error.URLError as e:
                attempts += 1
                print('have an exception getting testssl', e)


        st = os.stat(testssl_file_name)
        os.chmod(testssl_file_name, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)



    TEST_SSL_FILENAME = '/tmp/testssl.sh'
    ports_to_check = [11207, 18091, 18092, 18093, 18094, 18095, 18096]



    """ the testssl.sh output looks like:
 TLS 1      not offered
 TLS 1.1    not offered
 TLS 1.2    offered (OK)
    """

    def check_all_servers( self, rest):

        for s in self.servers:
            for i in self.ports_to_check:
                self.log.info('{0} Testing port {1}'.format(s, i))
                cmd  = self.TEST_SSL_FILENAME + ' -p --warnings off --color 0 {0}:{1}'.format( s.ip, i)
                self.log.info('The command is {0}'.format( cmd ) )
                res = os.popen(cmd).read().split('\n')
                res1 = ''.join( res )

                self.assertFalse( 'error' in res1.lower(), msg=res )
                self.assertTrue( 'tls' in res1.lower(), msg=res )
                for r in res:
                    if 'TLS 1.1' in r:
                        self.assertTrue( 'not offered' in r,
                                        msg='TLS 1.1 is incorrect enabled on port {0}'.format(i))
                    elif 'TLS 1 ' in r:
                        self.assertTrue( 'not offered' in r,
                                        msg='TLS 1 is incorrect enabled on port {0}'.format(i))



    def checkTLS1_1_blocking(self):
        self.get_the_testssl_script(self.TEST_SSL_FILENAME)
        command = "ns_config:set(ssl_minimum_protocol, 'tlsv1.2')"
        self.log.info("posting: %s" % command)
        rest = RestConnection(self.master)
        res = rest.diag_eval(command)


        # do the initial check
        self.check_all_servers( rest )


        # restart the server
        try:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.stop_couchbase()
                time.sleep(10) # Avoid using sleep like this on further calls
                shell.start_couchbase()
                shell.disconnect()
        except Exception as e:
            self.log.error(traceback.format_exc())

        # and check again
        time.sleep(30)
        self.check_all_servers( rest )

    def checkPortSecurity(self):



        self.get_the_testssl_script(self.TEST_SSL_FILENAME)



        all_passed = True
        # Note this analysis is very coupled to the testssl.sh output format. If it changes this code may break.
        # I did ask the testssl person if he would do a machine readable output - EC - 12/14/2015


        for i in self.ports_to_check:
            check_count = 0
            self.log.info('Testing port {0}'.format(i))
            cmd  = self.TEST_SSL_FILENAME + ' --warnings off --color 0 {0}:{1}'.format( self.master.ip, i)
            print('cmd is', cmd)
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
                        self.log.error('LogJam vulnerability on port {0}'.format(i))
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


            self.assertTrue( all_passed, msg='Port {0} failed. Check logs for failures'.format(i))
            self.log.info('Testing port {0}'.format(i))

            # make sure all the tests were seen
            if str(i) in ('18095', '18096'):
                self.assertTrue( check_count==8, msg='Port {0}. Not all checks present - saw {1} checks'.format(i, check_count))
            else:
                self.assertTrue( check_count==9, msg='Port {0}. Not all checks present - saw {1} checks'.format(i, check_count))
