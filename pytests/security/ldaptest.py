import json
import time
from threading import Thread, Event
from basetestcase import BaseTestCase
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import ReadDocumentException
from membase.api.exception import DesignDocCreationException
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from collectFiles import cbfiledownload
import logger
from membase.api.rest_client import ldapUser

log = logger.Logger.get_logger()


class LDAPTests(BaseTestCase):
    def setup(self):
        try:
            super(LDAPTests, self).setUp()
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        super(LDAPTests, self).tearDown()

    '''
    Using REST API check and verify LDAPAuth Enabled.
    UI call the same REST API.
    '''
    def checkAuthEnabled (self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        rest.ldapRestOperation(True, None)
        content = rest.ldapRestOperationGET()
        self.assertEqual(True, content['enabled'])

    '''
    Using REST API, enable LDAPAuth and then disable
    '''
    def checkAuthDisabled (self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        rest.ldapRestOperation(True, None)
        rest.ldapRestOperation(0, None)
        content = rest.ldapRestOperationGET()
        self.assertEqual(False, content['enabled'])

    '''
    Using REST API, enable LDAPAuth and add Admin User.
    Verify using REST API, that user is added to LDAPAuth as Admin
    '''
    def addUserAdmin(self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        userlist = []
        userlist.append(ldapUser('rsharma','Admin'))
        userlist.append(ldapUser('bjones','ROAdmin'))
        rest.ldapRestOperation(True, userlist)
        content = rest.ldapRestOperationGET()
        self.assertEqual([u'rsharma'], content['admins'])

    '''
    Using REST API, enable LDAPAuth and add ROAdmin User.
    Verify using REST API, that user is added to LDAPAuth as ROAdmin
    '''
    def addUserROAdmin(self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        userlist = []
        userlist.append(ldapUser('rsharma','Admin'))
        userlist.append(ldapUser('bjones','ROAdmin'))
        rest.ldapRestOperation(True, userlist)
        content = rest.ldapRestOperationGET()
        self.assertEqual([u'bjones'], content['roAdmins'])

    '''
    Using REST API, enable LDAPAuth and add Admin User.
    Verify using REST API, that user is added to LDAPAuth as Admin and is able to login
    '''
    def addUserAuthAdminEnabledLogin(self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        userlist = []
        userlist.append(ldapUser('rsharma','Admin'))
        userlist.append(ldapUser('bjones','ROAdmin'))
        rest.ldapRestOperation(True, userlist)
        status = rest.validateLogin(userlist[0].userName,"p@ssword")
        self.assertEqual(status['status'], '200')
        log.info ("Value of content is {0}".format(status))

    '''
    Using REST API, enable LDAPAuth and add ROAdmin User.
    Verify using REST API, that user is added to LDAPAuth as ROAdmin and is able to login
    '''
    def addUserAuthROAdminEnabledLogin(self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        userlist = []
        userlist.append(ldapUser('rsharma','Admin'))
        userlist.append(ldapUser('bjones','ROAdmin'))
        rest.ldapRestOperation(True, userlist)
        status = rest.validateLogin(userlist[1].userName,"p@ssword")
        self.assertEqual(status['status'], '200')
        log.info ("Value of content is {0}".format(status))

    '''
    Using REST API, disable LDAPAuth.
    Verify using REST API, that user is not able to login to CB
    '''
    def addUserAuthAdminDisabledLogin(self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        userlist = []
        userlist.append(ldapUser('rsharma','Admin'))
        userlist.append(ldapUser('bjones','ROAdmin'))
        rest.ldapRestOperation(0, userlist)
        status = rest.validateLogin(userlist[0].userName,"p@ssword")
        self.assertEqual(status['status'], '400')
        log.info ("Value of content is {0}".format(status))

    '''
    Using REST API, disable LDAPAuth.
    Verify using REST API, that user is not able to login to CB
    '''
    def addUserAuthROAdminDisabledLogin(self):
        rest = RestConnection(self.master)
        rest.ldapRestOperation("clear", None)
        userlist = []
        userlist.append(ldapUser('rsharma','Admin'))
        userlist.append(ldapUser('bjones','ROAdmin'))
        rest.ldapRestOperation(0, userlist)
        status = rest.validateLogin(userlist[1].userName,"p@ssword")
        self.assertEqual(status['status'], '400')
        log.info ("Value of content is {0}".format(status))

