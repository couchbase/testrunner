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

class ldaptest(BaseTestCase):

    def setUp(self):
        super(ldaptest, self).setUp()
        #Capture details of LDAP Server
        self.ldapHost = self.input.param('ldapHost', '172.23.107.63')
        self.ldapPort = self.input.param('ldapPort', '389')
        self.ldapAdmin = self.input.param('ldapAdmin', 'cn=Manager,dc=couchbase,dc=com')
        self.ldapAdminPass = self.input.param('ldapAdminPass', 'p@ssword')
        self.ldapDN = self.input.param('ldapDN', 'ou=Users,dc=couchbase,dc=com')
        self.ldapObjectClass = self.input.param('ldapObjectClass', 'inetOrgPerson')
        self.authRole = self.input.param("authRole", True)

        self.authState = self.input.param("authState", True)
        if (self.authState):
            self.authState = 'true'
        else:
            self.authState = 0

        #Input list of users as parameters, convert them to list and then delete and create them in LDAP
        self.fullAdmin = self.returnUserList(self.input.param("fullAdmin", ''))
        self.ROAdmin = self.returnUserList(self.input.param("ROAdmin", ''))

        self._removeLdapUserRemote(self.fullAdmin)
        self._removeLdapUserRemote(self.ROAdmin)

        self._createLDAPUser(self.fullAdmin)
        self._createLDAPUser(self.ROAdmin)

    def tearDown(self):
        super(ldaptest, self).tearDown()

    '''
    Create user in LDAP. Input parameter is a list of user that need creation
    '''
    def _createLDAPUser(self, ldapUserList):
        for user in ldapUserList:
            if (user[0] != ''):
                userCreateCmmd = 'dn: cn=' + user[0] + "," + self.ldapDN + "\n" \
                    "cn:" + user[0] + "\n" \
                    "sn: " + user[0] + "\n" \
                    'objectClass: ' + self.ldapObjectClass + "\n" \
                    "userPassword :" + user[1] + "\n" \
                    "uid: " + user[0] + "\n"
                fileName = user[0] + 'name.ldif'
                shell = RemoteMachineShellConnection(self.master)
                try:
                    shell.write_remote_file("/tmp", fileName, userCreateCmmd)
                    command = "ldapadd -h " + self.ldapHost + " -p " + self.ldapPort + " -f /tmp/" + fileName + " -D " + self.ldapAdmin + " -w " + self.ldapAdminPass
                    o, r = shell.execute_command(command)
                    shell.log_command_output(o, r)
                    command = "rm -rf /tmp/*.ldif"
                    o, r = shell.execute_command(command)
                    shell.log_command_output(o, r)
                finally:
                    shell.disconnect()

    '''
    Delete user in LDAP. Input parameter is a list of users that need deletion
    '''
    def _removeLdapUserRemote(self, ldapUserList):
        for user in ldapUserList:
            if (user[0] != ''):
                userDeleteCmd = 'ldapdelete cn=' + user[0] + "," + self.ldapDN
                shell = RemoteMachineShellConnection(self.master)
                try:
                    command = userDeleteCmd + " -D " + self.ldapAdmin + " -w " + self.ldapAdminPass
                    o, r = shell.execute_command(command)
                    shell.log_command_output(o, r)
                finally:
                    shell.disconnect()

    ''' Converts the input of list of users to a python list'''
    def returnUserList(self, Admin):
        Admin = (items.split(':') for items in Admin.split("?"))
        return list(Admin)

    '''Returns list of Admins and ROAdmins from the response. Also strip up unicode identifier'''
    def _parseRestResponse(self, response):
        admin_roles = {'roAdmins': [], 'admins': []}
        for key, value in response.items():
            if key in admin_roles.keys():
                for tempValue in value:
                    admin_roles[key].append(tempValue.encode('utf-8'))
        return admin_roles['roAdmins'], admin_roles['admins']

    ''' Compare list of users passed to API command versus users returned by API after successful command '''
    def validateReponse(self, reponseList, userList):
        userList = [userItem[0] for userItem in userList]
        for responseItem in reponseList:
            if responseItem not in userList:
                return False
        return True

    '''Test case to add admins to the system and validate response and login'''
    def test_addMultipleAdmin(self):
        #Create a REST connection
        rest = RestConnection(self.master)

        #Depending on the role specified as command line, pass parameters to the API
        if (self.authRole == "fullAdmin"):
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and then parse the JSON object and then check for multiple users
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())

        #Validate the response and try to login as user added to ldapauth (Multiple verifications here)
        if (self.authRole in ['fullAdmin', 'Both']):
            self.assertTrue(self.validateReponse(Admins, self.fullAdmin), 'Response mismatch with expected fullAdmin - expected {0} -- actual {1}'.format(self.fullAdmin, Admins))
            for user in self.fullAdmin:
                status = rest.validateLogin(user[0], user[1], True)
                self.assertTrue(status, 'User added as Admin to LDAP Auth is not able to login - user {0}'.format(user))
        elif (self.authRole in ['roAdmin', 'Both']):
            self.assertTrue(self.validateReponse(roAdmins, self.ROAdmin), 'Response mismatch with expected ROAdmin - expected {0} -- actual {1}'.format(self.ROAdmin, roAdmins))
            for user in self.ROAdmin:
                status = rest.validateLogin(user[0], user[1], True)
                self.assertTrue(status, 'User added as ROAdmin to LDAP Auth is not able to login- user {0}'.format(user))

    ''' Test to Add users and then Remove users, validate by response and login to system'''
    def test_addRemoveMultipleAdmin(self):

        #Get parameters that need to be removed from the list
        removeUserAdmin = self.returnUserList(self.input.param("removeUserAdmin", ''))
        removeUserROAdmin = self.returnUserList(self.input.param("removeUserROAdmin", ''))

        #Create a REST connection and reset LDAPSetting
        rest = RestConnection(self.master)

        # As per the role, add users first using API. Execute another command to remove users
        # and execute API.
        if (self.authRole == "fullAdmin"):
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
            for user in removeUserAdmin:
                self.fullAdmin.remove((user))
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
            for user in removeUserROAdmin:
                self.ROAdmin.remove((user))
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)
            for user in removeUserAdmin:
                self.fullAdmin.remove((user))
            for user in removeUserROAdmin:
                self.ROAdmin.remove((user))
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and validate with expected user
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
        self.assertTrue(self.validateReponse(Admins, self.fullAdmin), 'Response mismatch with expected fullAdmin - expected {0} -- actual {1}'.format(self.fullAdmin, Admins))
        self.assertTrue(self.validateReponse(roAdmins, self.ROAdmin), 'Response mismatch with expected roAdmin - expected {0} -- actual {1}'.format(self.ROAdmin, roAdmins))

        if (self.authRole in ['fullAdmin', 'Both']):
            for user in removeUserAdmin:
                status = rest.validateLogin(user[0], user[1], False)
                self.assertTrue(status, 'User added as Admin has errors while login - user -- {0}'.format(user))
        elif (self.authRole in ['roAdmin', 'Both']):
            for user in removeUserROAdmin:
                status = rest.validateLogin(user[0], user[1], False)
                self.assertTrue(status, 'User added as RoAdmin has errors while login- user -- {0}'.format(user))

    ''' Test to Add more users to current list, validate by response and login to system'''
    def test_addMoreMultipleAdmin(self):

        #Get parameters that need to be removed from the list
        addUserAdmin = self.returnUserList(self.input.param("addUserAdmin", ''))
        addUserROAdmin = self.returnUserList(self.input.param("addUserROAdmin", ''))

        #Create a REST connection and reset LDAPSetting
        rest = RestConnection(self.master)

        # As per the role, add users first using API. Execute another command to remove users
        # and execute API.
        if (self.authRole == "fullAdmin"):
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
            for user in addUserAdmin:
                self.fullAdmin.append((user))
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
            for user in addUserROAdmin:
                self.ROAdmin.append((user))
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)
            for user in addUserAdmin:
                self.fullAdmin.append((user))
            for user in addUserROAdmin:
                self.ROAdmin.append((user))
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and validate with expected user
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
        self.assertTrue(self.validateReponse(Admins, self.fullAdmin), 'Response mismatch with expected fullAdmin - expected {0} -- actual {1}'.format(self.fullAdmin, Admins))
        self.assertTrue(self.validateReponse(roAdmins, self.ROAdmin), 'Response mismatch with expected roAdmin - expected {0} -- actual {1}'.format(self.ROAdmin, roAdmins))

        if (self.authRole in ['fullAdmin', 'Both']):
            for user in self.fullAdmin:
                status = rest.validateLogin(user[0], user[1], True)
                self.assertTrue(status, 'User added as Admin has errors while login - user -- {0}'.format(user))
        elif (self.authRole in ['roAdmin', 'Both']):
            for user in self.ROAdmin:
                status = rest.validateLogin(user[0], user[1], True)
                self.assertTrue(status, 'User added as RoAdmin has errors while login- user -- {0}'.format(user))

    ''' Pending to be executed this is more for disabled login'''
    def test_checkLoginDisabled(self):
        #Create a REST connection and reset LDAPSetting
        rest = RestConnection(self.master)

        if (self.authRole == "fullAdmin"):
            self.log.info ("Into authrole of fulladmin")
            self._createLDAPUser(self.fullAdmin)
            rest.ldapUserRestOperation(self.authState, self.fullAdmin)
            rest.ldapUserRestOperation('false')
        elif (self.authRole == 'roAdmin'):
            self._createLDAPUser(self.ROAdmin)
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
            rest.ldapUserRestOperation('false')
        elif (self.authRole == 'Both'):
            self._createLDAPUser(self.fullAdmin)
            self._createLDAPUser(self.ROAdmin)
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)
            rest.ldapUserRestOperation('false')

        #Get the response and then parse the JSON object and then check for multiple users
        content = rest.ldapRestOperationGetResponse()
        roAdmins, Admins = self._parseRestResponse(content)

        if (self.authRole in ['fullAdmin', 'Both']):
            self.assertTrue(self.validateReponse(Admins, self.fullAdmin))
            for user in self.fullAdmin:
                status = rest.validateLogin(user[0], user[1], True)
                self.assertTrue(status, 'User added as ROAdmin to LDAP Auth is not able to login')
        elif (self.authRole in ['roAdmin', 'Both']):
            self.assertTrue(self.validateReponse(roAdmins, self.ROAdmin))
            for user in self.ROAdmin:
                status = rest.validateLogin(user[0], user[1], True)
                self.assertTrue(status, 'User added as ROAdmin to LDAP Auth is not able to login')

    ''' Test case for checking wildcard * for roAdmin or Full Admin'''
    def test_checkWildCard(self):
        #Create a REST connection
        rest = RestConnection(self.master)

        #Depending on the role specified as command line, pass parameters to the API
        if (self.authRole == "fullAdmin"):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, "*")
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, "*", self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, "*", "*")


        #Get the response and then parse the JSON object and then check for multiple users
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())

        if (self.authRole == 'fullAdmin'):
            self.assertTrue(self.validateReponse(Admins, self.fullAdmin), 'Response mismatch with expected fullAdmin - {0}'.format(self.validateReponse(Admins, self.fullAdmin)))
            self.assertEqual("*", roAdmins[0], 'Validation for ROAdmins has failed: expected {0} - acutal {1}'.format("*", roAdmins))
        elif (self.authRole == 'roAdmin'):
            self.assertTrue(self.validateReponse(roAdmins, self.ROAdmin), 'Response mismatch with expected fullAdmin - {0}'.format(self.validateReponse(Admins, self.ROAdmin)))
            self.assertEqual("*", Admins[0], 'Validation for ROAdmins has failed: expected {0} - acutal {1}'.format("*", Admins))

        for user in self.fullAdmin:
            status = rest.validateLogin(user[0], user[1], True)
            self.assertTrue(status, 'User added as Admin has errors while login - user -- {0}'.format(user))
        for user in self.ROAdmin:
            status = rest.validateLogin(user[0], user[1], True)
            self.assertTrue(status, 'User added as RoAdmin has errors while login - user -- {0}'.format(user))


    ''' Test to check Validity for a particular user for authentication and role'''
    def test_validateCredentials(self):
        source = self.input.param("source", 'saslauthd')

        #Create a REST connection
        rest = RestConnection(self.master)

        #Depending on the role specified as command line, pass parameters to the API
        if (self.authRole == "fullAdmin"):
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)

        #Validate the response with authRole set and authRole returned
        self.log.info ("Validating response with authRole and source of user")
        if (self.authRole == 'fullAdmin'):
            status, content = rest.executeValidateCredentials(self.fullAdmin[0][0], self.fullAdmin[0][1])
        elif (self.authRole == 'roAdmin'):
            status, content = rest.executeValidateCredentials(self.roAdmin[0][0], self.roAdmin[0][1])
        self.assertEqual(content['role'], self.authRole, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(self.authRole, content['role']))
        self.assertEqual(content['source'], source, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(source, content['source']))

