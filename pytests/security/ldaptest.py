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
from random import randint
from clitest.cli_base import CliBaseTest


class ServerInfo():
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 ssh_key=''):

        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key


class ldaptest(BaseTestCase):

    def setUp(self):
        super(ldaptest, self).setUp()
        #Capture details of LDAP Server
        #LDAP host, where LDAP Server is running
        self.ldapHost = self.input.param('ldapHost', '172.23.120.175')
        #LDAP Port, port of LDAP Server
        self.ldapPort = self.input.param('ldapPort', '389')
        #LDAP Admin to connect to
        self.ldapAdmin = self.input.param('ldapAdmin', 'cn=admin,dc=couchbase,dc=com')
        #LDAP Admin password
        self.ldapAdminPass = self.input.param('ldapAdminPass', 'p@ssw0rd')
        #LDAP DN for Users
        self.ldapDN = self.input.param('ldapDN', 'ou=Users,dc=couchbase,dc=com')
        #LDAP Object Class for Users
        self.ldapObjectClass = self.input.param('ldapObjectClass', 'inetOrgPerson')
        #Role that user needs to be added to - Full Admin, RO Admin or Both
        self.authRole = self.input.param("authRole", True)
        #LDAP Auth State, whether enabled or disabled.
        self.authState = self.input.param("authState", True)

        #Input list of users as parameters, convert them to list and then delete and create them in LDAP
        #Get list of Full Admins and Read Only Admins
        self.fullAdmin = self.returnUserList(self.input.param("fullAdmin", ''))
        self.ROAdmin = self.returnUserList(self.input.param("ROAdmin", ''))

        self._removeLdapUserRemote(self.fullAdmin)
        self._removeLdapUserRemote(self.ROAdmin)

        self._createLDAPUser(self.fullAdmin)
        self._createLDAPUser(self.ROAdmin)

        rest = RestConnection(self.master)
        rest.disableSaslauthdAuth()

        self.ldap_server = ServerInfo(self.ldapHost, self.ldapPort, 'root', 'couchbase')

    def tearDown(self):
        super(ldaptest, self).tearDown()

    '''
    Create users in LDAP. Input parameter is a list of user that need creation
    Parameters
        ldapUserList - List of user that need to be created in LDAP. List should
        be of ['username','password']
    '''
    def _createLDAPUser(self, ldapUserList):
        for user in ldapUserList:
            if (user[0] != ''):
                #Create user command
                userCreateCmmd = 'dn: cn=' + user[0] + "," + self.ldapDN + "\n" \
                    "cn:" + user[0] + "\n" \
                    "sn: " + user[0] + "\n" \
                    'objectClass: ' + self.ldapObjectClass + "\n" \
                    "userPassword :" + user[1] + "\n" \
                    "uid: " + user[0] + "\n"
                fileName = 'name.ldif'
                #Execute ldapadd command to add users to the system
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
    Delete user in LDAP.
    Parameters:
        ldapUserList - List of user that need to be created in LDAP. List should
        be of ['username','password']
    '''
    def _removeLdapUserRemote(self, ldapUserList):
        for user in ldapUserList:
            if (user[0] != ''):
                userDeleteCmd = 'ldapdelete -h ' + self.ldapHost + " -p " + self.ldapPort + ' cn=' + user[0] + "," + self.ldapDN
                shell = RemoteMachineShellConnection(self.master)
                try:
                    command = userDeleteCmd + " -D " + self.ldapAdmin + " -w " + self.ldapAdminPass
                    o, r = shell.execute_command(command)
                    shell.log_command_output(o, r)
                finally:
                    shell.disconnect()

    '''
    Change password for a particular user
    Parameters
        user - username
        password - new password that need to be changed
    Returns

    '''
    def _changeLdapPassRemote(self, user, password):
        shell = RemoteMachineShellConnection(self.master)
        try:
            command = "ldappasswd -h " + self.ldapHost + " -s " + password + " cn=" + user + "," + self.ldapDN
            command = command + " -D " + self.ldapAdmin + " -w " + self.ldapAdminPass
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
        finally:
            shell.disconnect()

    '''
    Converts the input of list of users to a python list
    Multiple users in the list have to user1:password1?user2:password2
    Parameters
        Admin - String from input of format above

    Returns
        List that contains user and password - ['username','password']
    '''
    def returnUserList(self, Admin):
        Admin = (items.split(':') for items in Admin.split("?"))
        return list(Admin)

    '''
    Returns list of Admins and ROAdmins from the response. Also strip up unicode identifier
    Parameters
        response - Response from LDAPAuth Get Command
        {"enabled":true,"admins":["bjones03","bjones02"],"roAdmins":["bjones01","bjones"]}

    Returns
        2 list - roAdmins -  list of RO Admin['bjones03','bjones02']
        admins - list of FullAdmins
    '''
    def _parseRestResponse(self, response):
        admin_roles = {'roAdmins': [], 'admins': []}
        for key, value in list(response.items()):
            if key in list(admin_roles.keys()):
                for tempValue in value:
                    admin_roles[key].append(tempValue.encode('utf-8'))
        return admin_roles['roAdmins'], admin_roles['admins']

    '''
    Compare list of users passed as input with users returned by API after successful command
    Parameters
        responseList - List of user from response
        userList - List of user passed to the function

    Returns
        True/False - Based on comparison of input and API
    '''
    def validateReponse(self, reponseList, userList):
        userList = [userItem[0] for userItem in userList]
        for responseItem in reponseList:
            if responseItem not in userList:
                return False
        return True

    '''
    _setupLDAPAuth - Function for setting up LDAP Auth.
    Parameters:
        rest - RestConnection to the machines
        authRole - What kind of users you want to add, fullAdmin, ROAdmin or Both
        authState - LDAP Enabled or disabled
        fullAdmin - List of users that need to be added as fullAdmin
        ROAdmin - List of users that need to added as Read Only Admin
        default - Default user role/priviledge if user is not explicitly part of fullAdmin or ROAdmin

    Returns:

    '''
    def _setupLDAPAuth(self, rest, authRole, authState, fullAdmin, ROAdmin, default=None):
        #Depending on the role specified as command line, pass parameters to the API
        if (authRole == "fullAdmin"):
            rest.ldapUserRestOperation(authState, adminUser=fullAdmin, exclude=default)
        elif (authRole == 'roAdmin'):
            rest.ldapUserRestOperation(authState, ROadminUser=ROAdmin, exclude=default)
        elif (authRole == 'Both'):
            rest.ldapUserRestOperation(authState, fullAdmin, ROAdmin, exclude=default)

    '''
    _validateCred -
    Function for testing the TEST/Check Credentails/Role functionality. Test value of checkRole passed as
    argument to the function.

    Parameters:
        rest - RestConnection to CB
        authRole - What is the authRole that need to be checked
        fullAdmin - list of users that are added as fulLAdmin
        ROAdmin - list of users that are added as Read only Admin
        otherUsers - list of users that are not explicitly added as FullAdmin or ROAdmin
        checkRole - Role expected from REST command
        source - Souce of the user - builtin or ldapuser
        default - Value of default role
        valSource - Validate Source
        valRole - Validate Role

    Returns:
        None
    '''
    def _validateCred(self, rest, authRole, fullAdmin, ROAdmin, otherUsers, checkRole, source, default, valSource=False, valRole=False):
        self.log.info ("Validating response with authRole and source of user")
        if (authRole in ['fullAdmin', 'Both']):
            if ((checkRole == 'Both' or self.authRole == 'Both') and (checkRole != 'none')):
                checkRole = 'fullAdmin'
            for user in fullAdmin:
                status, content = rest.executeValidateCredentials(user[0], user[1])
                if (valRole):
                    self.assertEqual(content['role'], checkRole, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(checkRole, content['role']))
                if (valSource):
                    self.assertEqual(content['source'], source, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(source, content['source']))
        if (authRole in ['roAdmin', 'Both']):
            if ((checkRole == 'Both' or self.authRole == 'Both') and (checkRole != 'none')):
                checkRole = 'roAdmin'
            for user in ROAdmin:
                status, content = rest.executeValidateCredentials(user[0], user[1])
                if (valRole):
                    self.assertEqual(content['role'], checkRole, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(checkRole, content['role']))
                if (valSource):
                    self.assertEqual(content['source'], source, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(source, content['source']))
        if (otherUsers):
            for user in otherUsers:
                status, content = rest.executeValidateCredentials(user[0], user[1])
                self.log.info ("Value of content is {0}".format(content))
                self.assertEqual(content['role'], default, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(default, content['role']))
                self.assertEqual(content['source'], source, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(source, content['source']))

    '''
    _funcValidateResLogin - Validate REST response and login
    Parameters
        rest - Rest connection to CB
        authRole - Role for the user list
        authState - LDAP Auth State to be checked
        fullAdmin - list of users as Full Admin
        ROAdmin - list of users for ROADmin
        Admins - Result from REST command for saslauthd
        roAdmins - Result from REST command for saslauthd

    Returns:
    '''
    def _funcValidateResLogin(self, rest, authRole, authState, fullAdmin, ROAdmin, Admins, roAdmins, login=True):
        if (authRole in ['fullAdmin', 'Both']):
            self.assertTrue(self.validateReponse(Admins, fullAdmin), 'Response mismatch with expected fullAdmin - expected {0} -- actual {1}'.format(fullAdmin, Admins))
            for user in fullAdmin:
                self.log.info ("value of user is {0} - {1}".format(user[0], user[1]))
                status = rest.validateLogin(user[0], user[1], login)
                self.assertTrue(status, 'User added as Admin to LDAP Auth is not able to login - user {0}'.format(user))
        if (authRole in ['roAdmin', 'Both']):
            self.assertTrue(self.validateReponse(roAdmins, ROAdmin), 'Response mismatch with expected ROAdmin - expected {0} -- actual {1}'.format(ROAdmin, roAdmins))
            for user in ROAdmin:
                self.log.info ("value of user is {0} - {1}".format(user[0], user[1]))
                status = rest.validateLogin(user[0], user[1], login)
                self.assertTrue(status, 'User added as ROAdmin to LDAP Auth is not able to login- user {0}'.format(user))


    '''
    _valAdminLogin - check if Admin or any other user can login
    Parameters:
        rest - Rest Connection pointer
        AdminUser - username
        AdminPass - password
    '''
    def _valAdminLogin(self, rest, AdminUser, AdminPass, login):
        status = rest.validateLogin(AdminUser, AdminPass, login)


    '''Test case to add admins to the system and validate response and login'''
    def test_addMultipleAdmin(self):

        #Create a REST connection
        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and then parse the JSON object to convert it to list of users
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())

        #Validate the response and try to login as user added to ldapauth (Multiple verifications here)
        self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins, self.authState)


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
            self.fullAdmin = [item for item in self.fullAdmin if item not in removeUserAdmin]
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
            self.ROAdmin = [item for item in self.ROAdmin if item not in removeUserROAdmin]
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)
            self.fullAdmin = [item for item in self.fullAdmin if item not in removeUserAdmin]
            self.ROAdmin = [item for item in self.ROAdmin if item not in removeUserROAdmin]
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and validate with expected users
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
        self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

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
            self.fullAdmin = self.fullAdmin + addUserAdmin
            rest.ldapUserRestOperation(self.authState, adminUser=self.fullAdmin)
        elif (self.authRole == 'roAdmin'):
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
            self.ROAdmin = self.ROAdmin + addUserROAdmin
            rest.ldapUserRestOperation(self.authState, ROadminUser=self.ROAdmin)
        elif (self.authRole == 'Both'):
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)
            self.fullAdmin = self.fullAdmin + addUserAdmin
            self.ROAdmin = self.ROAdmin + addUserROAdmin
            rest.ldapUserRestOperation(self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and validate with expected user
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
        self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)


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
            status = rest.validateLogin(user[0], user[1], False)
            self.assertTrue(status, 'User added as Admin has errors while login - user -- {0}'.format(user))
        for user in self.ROAdmin:
            status = rest.validateLogin(user[0], user[1], False)
            self.assertTrue(status, 'User added as RoAdmin has errors while login - user -- {0}'.format(user))


    ''' Test list of users along with default roles for users that are not explicitly part of the
    fullAdmin or RO Admin role
    '''
    def test_validateDefaultRole(self):
        #Take in source, currently saslauthd is used
        source = self.input.param("source", 'saslauthd')
        #Default role or privilege
        default = self.input.param("default", None)
        #List of users that are in LDAP but not added explicit to ROAdmin or Full Admin roles
        otherUsers = self.input.param("otherUsers", '')
        if (otherUsers):
            otherUsers = self.returnUserList(otherUsers)
            self._removeLdapUserRemote(otherUsers)
            self._createLDAPUser(otherUsers)

        #Create a REST connection and setup LDAP Auth
        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, default)

        # This is for special case for users that are not part of explicit FullAdmin or ROAdmin roles
        # and will depend on Default privilege/role. In case of none, the role is none and user is builtin.
        # Basically no check if default privilege is None.
        if (default is None):
            self.log.info ("Into default is none")
            default = 'none'
            source = 'builtin'

        #Validate the response with authRole set and authRole returned
        self._validateCred(rest, self.authRole, self.fullAdmin, self.ROAdmin, otherUsers, self.authRole, source, default, valRole=True)


    ''' Test to check Test Functionality on LDAP page. '''
    def test_validateTest(self):
        #Take in source, currently saslauthd is used
        source = self.input.param("source", 'saslauthd')
        #Expected role to be compared with out of the command
        checkRole = self.input.param('checkRole', 'none')
        #Flag to check for incorrect password
        incorrectPass = self.input.param('incorrectPass', False)
        #Default role or privilege
        default = self.input.param('default', None)
        #List of users that are in LDAP but not added explicit to ROAdmin or Full Admin roles
        otherUsers = self.input.param("otherUsers", '')
        if (otherUsers):
            otherUsers = self.returnUserList(otherUsers)
            self._removeLdapUserRemote(otherUsers)
            self._createLDAPUser(otherUsers)

        #Get the rest connection and setup LDAPAuth
        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, default)

        #Taking care of incorrect password, behavior will be like one.
        #source=builtin and checkRole=none and default=none. Also change passwords for everyone
        if ((incorrectPass)):
            self.log.info ("into default is None")
            source = 'builtin'
            checkRole = 'none'
            default = 'none'
            i = 0
            for i in range(len(self.fullAdmin)):
                self.fullAdmin[i][1] = 'password1'
            for i in range(len(self.ROAdmin)):
                self.ROAdmin[i][1] = 'password1'
            if (otherUsers):
                for i in range(len(otherUsers)):
                    otherUsers[i][1] = 'password1'

        #Settings for special case where default = none
        if (default == 'none'):
            source = 'builtin'
            default = 'none'

        #Validate the response with authRole set and authRole returned
        self._validateCred(rest, self.authRole, self.fullAdmin, self.ROAdmin, otherUsers, checkRole, source, default, True, True)


    '''
    Cluster add nodes from master verify on other node
    '''
    def test_checkOnClusterAddNode(self):

        addRemUserAdmin = self.returnUserList(self.input.param("addRemUserAdmin", ''))
        addRemUserROAdmin = self.returnUserList(self.input.param("addRemUserROAdmin", ''))

        ldapOps = self.input.param("ldapOps")

        #Create a list of Rest connections for servers in Cluster
        rest = []
        for i in range(0, len(self.servers)):
            node1 = self.servers[i]
            restNode1 = RestConnection(node1)
            rest.append(restNode1)

        #Adding users from master Node
        self._setupLDAPAuth(rest[0], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        #LDAP Operations for
        if (ldapOps == 'addUser'):
            self.fullAdmin = self.fullAdmin + addRemUserAdmin
            self.ROAdmin = self.ROAdmin + addRemUserROAdmin
            #Appending users from Second Node
            self._setupLDAPAuth(rest[1], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)
        elif(ldapOps == 'removeUser'):
            self.fullAdmin = [item for item in self.fullAdmin if item not in addRemUserAdmin]
            self.ROAdmin = [item for item in self.ROAdmin if item not in addRemUserROAdmin]
            #Removing users from Second Node
            self._setupLDAPAuth(rest[1], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        for i in range (len(rest)):
            roAdmins, Admins = self._parseRestResponse(rest[i].ldapRestOperationGetResponse())
            self._funcValidateResLogin(rest[i], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

    ''' Add remove Nodes'''
    def test_checkOnRemoveFailoverNodes(self):
        clusterOps = self.input.param("clusterOps")
        servs_inout = self.servers[1:self.nodes_in + 1]

        rest = []
        for i in range(0, len(self.servers)):
            node1 = self.servers[i]
            restNode1 = RestConnection(node1)
            rest.append(restNode1)

        #Adding users from  Node 1
        self._setupLDAPAuth(rest[1], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        if (clusterOps == 'remove'):
            self.cluster.rebalance(self.servers, [], servs_inout)
        elif(clusterOps == 'failover'):
            self.cluster.failover(self.servers, servs_inout)
            self.cluster.rebalance(self.servers, [], [])
            self.sleep(30, "30 seconds sleep after failover before invoking rebalance...")
        elif(clusterOps == 'failoverGrace'):
            self.cluster.failover(self.servers, servs_inout, True)
            self.cluster.rebalance(self.servers, [], [])
            self.sleep(30, "30 seconds sleep after failover before invoking rebalance...")

        roAdmins, Admins = self._parseRestResponse(rest[0].ldapRestOperationGetResponse())
        self._funcValidateResLogin(rest[0], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

        self.cluster.rebalance(self.servers, servs_inout, [])

        for i in range (len(rest)):
            roAdmins, Admins = self._parseRestResponse(rest[i].ldapRestOperationGetResponse())
            self._funcValidateResLogin(rest[i], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)



    def test_ClusterEndToEnd(self):
        servs_inout = self.servers[1:self.nodes_in + 1]
        addRemUserAdmin = self.returnUserList(self.input.param("addRemUserAdmin", ''))
        addRemUserROAdmin = self.returnUserList(self.input.param("addRemUserROAdmin", ''))
        ldapOps = self.input.param("ldapOps")

        servs_inout = self.servers[1:self.nodes_in]

        rest = []
        for i in range(0, len(self.servers)):
            node1 = self.servers[i]
            restNode1 = RestConnection(node1)
            rest.append(restNode1)

        # Add users via master node
        self._setupLDAPAuth(rest[0], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        #Verify that you can login from both the nodes
        for i in range (len(rest)):
            roAdmins, Admins = self._parseRestResponse(rest[i].ldapRestOperationGetResponse())
            self._funcValidateResLogin(rest[i], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

        #Remove the master node from the cluster & verify login from different node
        self.cluster.rebalance(self.servers, [], servs_inout)
        roAdmins, Admins = self._parseRestResponse(rest[1].ldapRestOperationGetResponse())
        self._funcValidateResLogin(rest[1], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

        #Add users to the different node now
        self.fullAdmin = self.fullAdmin + addRemUserAdmin
        self.ROAdmin = self.ROAdmin + addRemUserROAdmin

        #Add users from the pending node
        self._setupLDAPAuth(rest[1], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        #Bring back removed node & checking by login to recently added node
        self.cluster.rebalance(self.servers, servs_inout, [])
        roAdmins, Admins = self._parseRestResponse(rest[0].ldapRestOperationGetResponse())
        self._funcValidateResLogin(rest[0], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

        #Remove few users
        self.fullAdmin = [item for item in self.fullAdmin if item not in addRemUserAdmin]
        self.ROAdmin = [item for item in self.ROAdmin if item not in addRemUserROAdmin]

        #Remove few users and remove the node where users were added
        self._setupLDAPAuth(rest[1], self.authRole, self.authState, self.fullAdmin, self.ROAdmin)
        self.cluster.rebalance(self.servers, [], servs_inout)

        #Login to master and Check
        roAdmins, Admins = self._parseRestResponse(rest[0].ldapRestOperationGetResponse())
        self._funcValidateResLogin(rest[0], self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)


    def test_checkInvalidISASLPW(self):
        ldapAdministrator = self.input.param("ldapAdministrator", False)
        shell = RemoteMachineShellConnection(self.master)
        try:
            rest = RestConnection(self.master)
            self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)
            roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
            self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)
            self._valAdminLogin(rest, 'Administrator', 'password', True)
            command = "mv /opt/couchbase/var/lib/couchbase/isasl.pw /tmp"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
            self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)
            if (ldapAdministrator):
                self._valAdminLogin(rest, 'Administrator', 'password', True)
            else:
                self._valAdminLogin(rest, 'Administrator', 'password', False)
        finally:
            command = "mv /tmp/isasl.pw /opt/couchbase/var/lib/couchbase"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            shell.disconnect()

    def test_checkPasswordChange(self):

        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        for i, j in zip(list(range(len(self.fullAdmin))), list(range(len(self.ROAdmin)))):
            self.fullAdmin[i][1] = 'password1'
            self._changeLdapPassRemote(self.fullAdmin[i][0], 'password1')
            self.ROAdmin[j][1] = 'password1'
            self._changeLdapPassRemote(self.ROAdmin[j][0], 'password1')

        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
        self.log.info("value of roadmin and admin returned is {0} - {1}".format(roAdmins, Admins))
        self.log.info("value of roadmin and admin returned is {0} - {1}".format(self.ROAdmin, self.fullAdmin))
        self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins)

    def test_ldapDeleteUser(self):

        #Get parameters that need to be removed from the list
        removeUserAdmin = self.returnUserList(self.input.param("removeUserAdmin", ''))
        removeUserROAdmin = self.returnUserList(self.input.param("removeUserROAdmin", ''))

        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        self._removeLdapUserRemote(removeUserAdmin)
        self._removeLdapUserRemote(removeUserROAdmin)

        for userAdmin, userROAdmin in zip(removeUserAdmin, removeUserROAdmin):
            status = rest.validateLogin(userAdmin[0], userAdmin[1], False)
            self.assertTrue(status)
            status = rest.validateLogin(userROAdmin[0], userROAdmin[1], False)
            self.assertTrue(status)


    def test_checkHigherPermission(self):
        #Take in source, currently saslauthd is used
        source = self.input.param("source", 'saslauthd')
        #Expected role to be compared with out of the command
        checkRole = self.input.param('checkRole', 'none')

        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        status, content = rest.executeValidateCredentials(self.ROAdmin[0][0], self.ROAdmin[0][1])
        self.assertEqual(content['role'], checkRole, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(checkRole, content['role']))
        self.assertEqual(content['source'], source, 'Difference in expected and actual: expected - {0}, actual -{1}'.format(source, content['source']))

    def test_checkInitialState(self):
        rest = RestConnection(self.master)
        content = rest.ldapRestOperationGetResponse()
        self.assertEqual(content['enabled'], False)


    '''Test case to add admins to the system and validate response and login'''
    def test_addNegativeTC(self):

        loginState = self.input.param("loginState")
        #Create a REST connection
        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

        #Get the response and then parse the JSON object to convert it to list of users
        roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())

        #Validate the response and try to login as user added to ldapauth (Multiple verifications here)
        self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins, loginState)

    def test_stopLDAPServer(self):
        loginState = self.input.param("loginState")
        shell = RemoteMachineShellConnection(self.ldap_server)
        try:

            rest = RestConnection(self.master)
            self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

            #Get the response and then parse the JSON object to convert it to list of users
            roAdmins, Admins = self._parseRestResponse(rest.ldapRestOperationGetResponse())

            command = "service slapd stop"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)

            #Validate the response and try to login as user added to ldapauth (Multiple verifications here)
            self._funcValidateResLogin(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin, Admins, roAdmins, loginState)
        finally:
            command = "service slapd start"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)


class ldapCLITest(CliBaseTest):

    def setUp(self):
        super(ldapCLITest, self).setUp()
        self.enableStatus = self.input.param("enableStatus", None)
        self.admin = self.returnUserList(self.input.param("admin", None))
        self.roAdmin = self.returnUserList(self.input.param("roAdmin", None))
        self.errorMsg = self.input.param("errorMsg", None)
        self.default = self.input.param("default", None)
        self.ldapUser = self.input.param('ldapUser', 'Administrator')
        self.ldapPass = self.input.param('ldapPass', 'password')
        self.source = self.input.param('source', None)
        if (self.source == 'saslauthd'):
            rest = RestConnection(self.master)
            rest.ldapUserRestOperation(True, [[self.ldapUser]], exclude=None)

    def tearDown(self):
        super(ldapCLITest, self).tearDown()

    def _parseRestResponse(self, response):
        admin_roles = {'roAdmins': [], 'admins': []}
        for key, value in list(response.items()):
            if key in list(admin_roles.keys()):
                for tempValue in value:
                    admin_roles[key].append(tempValue.encode('utf-8'))
        return admin_roles['roAdmins'], admin_roles['admins']

    def returnUserList(self, Admin):
        if Admin is not None:
            Admin = Admin.split("?")
            return list(Admin)

    def returnBool(self, boolString):
        if boolString in ('True', True, 'true'):
            return 1
        else:
            return 0


    def test_enableDisableLdap(self):
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        origState = rest.ldapRestOperationGetResponse()['enabled']
        cli_command = 'setting-ldap'
        options = "--ldap-enable=0"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        tempStatus = rest.ldapRestOperationGetResponse()['enabled']
        self.assertFalse(tempStatus, "Issues with setting LDAP enable/disable")
        cli_command = 'setting-ldap'
        options = "--ldap-enable=1"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
        tempStatus = rest.ldapRestOperationGetResponse()['enabled']
        self.assertTrue(tempStatus, "Issues with setting LDAP enable/disable")



    def test_setLDAPParam(self):
        cli_command = "setting-ldap"
        options = " --ldap-enable={0}".format(self.enableStatus)
        options += " --ldap-roadmins={0}".format(",".join(self.roAdmin))
        options += " --ldap-admins={0}".format(",".join(self.admin))
        options += " --ldap-default={0}".format(self.default)
        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        tempFlag = self.validateSettings(self.enableStatus, self.admin, self.roAdmin, self.default)
        self.assertTrue(tempFlag)


    def validateSettings(self, status, admin, roAdmin, default):
        rest = RestConnection(self.master)
        temproAdmins, tempAdmins = self._parseRestResponse(rest.ldapRestOperationGetResponse())
        print(temproAdmins)
        print(tempAdmins)
        tempStatus = rest.ldapRestOperationGetResponse()['enabled']
        print(rest.ldapRestOperationGetResponse())
        flag = True

        if (status is not self.returnBool(tempStatus)):
            self.log.info ("Mismatch with status - Expected - {0} -- Actual - {1}".format(status, tempStatus))
            flag = False

        if (default == 'admins'):
            tempAdmins = "".join(tempAdmins)
            if (tempAdmins == 'asterisk'):
                print("Admins is Default")
            else:
                print("Admin is not Default")
                flag = False
        else:
            if (tempAdmins != admin):
                self.log.info ("Mismatch with Admin - Expected - {0} -- Actual - {1}".format(admin, tempAdmins))
                flag = False

        if (default == 'roadmins'):
            temproAdmins = "".join(temproAdmins)
            if (temproAdmins == 'asterisk'):
                print("Admins is Default")
            else:
                print("Admin is not Default")
                flag = False
        else:
            if (temproAdmins != roAdmin):
                print(roAdmin)
                self.log.info ("Mismatch with ROAdmin - Expected - {0} -- Actual - {1}".format(roAdmin, temproAdmins))
                flag = False

        return flag
