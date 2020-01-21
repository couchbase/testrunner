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
import subprocess
from security.auditmain import audit
from clitest.cli_base import CliBaseTest
import socket
import urllib.request, urllib.parse, urllib.error


class auditcheckconfig(BaseTestCase):
    AUDITCONFIGRELOAD = 4096
    AUDITSHUTDOWN = 4097

    def setUp(self):
        super(auditcheckconfig, self).setUp()
        self.ipAddress = self.getLocalIPAddress()
        self.eventID = self.input.param('id', None)

    def tearDown(self):
        super(auditcheckconfig, self).tearDown()

    '''
    Returns ip address of the requesting machine
    '''
    def getLocalIPAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("couchbase.com", 0))
        return s.getsockname()[0]
        '''
        status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        return ipAddress
        '''

    def getTimeStampForFile(self, audit):
        return (audit.getTimeStampFirstEvent()).replace(":", "-")

    def getHostName(self, host):
        shell = RemoteMachineShellConnection(host)
        try:
            return (shell.execute_command("hostname")[0][0])
        except:
            shell.disconnect()

    def createRemoteFolder(self, host, newPath):
        shell = RemoteMachineShellConnection(host)
        try:
            shell.create_directory(newPath)
            command = 'chown couchbase:couchbase ' + newPath
            shell.execute_command(command)
        finally:
            shell.disconnect()

    def changePathWindows(self, path):
        shell = RemoteMachineShellConnection(self.master)
        os_type = shell.extract_remote_info().distribution_type
        self.log.info ("OS type is {0}".format(os_type))
        if os_type == 'windows':
            path = path.replace("/", "\\")
        return path


    def createBucketAudit(self, host, bucketName):
        rest = RestConnection(host)
        #Create an Event for Bucket Creation
        expectedResults = {'name':bucketName, 'ram_quota':104857600, 'num_replicas':1,
                                   'replica_index':False, 'eviction_policy':'value_only', 'type':'membase', \
                                    'auth_type':'sasl', "autocompaction":'false', "purge_interval":"undefined", \
                                    "flush_enabled":1, "num_threads":3, \
                                    "ip":self.ipAddress, "port":57457, 'sessionid':'' }
        rest.create_bucket(expectedResults['name'], expectedResults['ram_quota'] // 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                    '11211', 'membase', 0, expectedResults['num_threads'], expectedResults['flush_enabled'], 'valueOnly')
        return expectedResults

    def returnBoolVal(self, boolVal):
        if boolVal == 1:
            return 'true'
        else:
            return 'false'
    '''
    checkConfig - Wrapper around audit class
    Parameters:
        eventID - eventID of the event
        host - host for creating and reading event
        expectedResult - dictionary of fields and value for event
    '''
    def checkConfig(self, eventID, host, expectedResults):
        Audit = audit(eventID=eventID, host=host)
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    '''
    Test cases to validate default state of audit for first install
    '''
    def getDefaultState(self):
        auditIns = audit(host=self.master)

        #Validate that status of enabled is false
        tempStatus = auditIns.getAuditStatus()
        if (tempStatus == 'true'):
            tempStatus = True
        else:
            tempStatus = False
        self.assertFalse(tempStatus, "Audit is not disabled by default")

    '''
    Check enabled disable and reload of config
    '''
    def test_AuditEvent(self):
        auditIns = audit(host=self.master)
        ops = self.input.param("ops", None)
        source = 'internal'
        user = 'couchbase'
        rest = RestConnection(self.master)
        #status = rest.setAuditSettings(enabled='true')
        auditIns.setAuditEnable('true')
        if (ops in ['enable', 'disable']):
            if ops == 'disable':
                #status = rest.setAuditSettings(enabled='false')
                auditIns.setAuditEnable('false')
            else:
                #status = rest.setAuditSettings(enabled='true')
                auditIns.setAuditEnable('true')

        if ops == 'disable':
            shell = RemoteMachineShellConnection(self.master)
            try:
                result = shell.file_exists(auditIns.getAuditLogPath(), auditIns.AUDITLOGFILENAME)
            finally:
                shell.disconnect()
            self.assertTrue(result, 'Issue with file getting create in new directory')
        else:
            auditIns = audit(host=self.master)
            expectedResults = {"auditd_enabled":auditIns.getAuditStatus(),
                               "descriptors_path":self.changePathWindows(auditIns.getAuditConfigElement('descriptors_path')),
                               "log_path":self.changePathWindows((auditIns.getAuditLogPath())[:-1]), "source":"internal",
                               "user":"couchbase", "rotate_interval":86400, "version":2, 'hostname':self.getHostName(self.master),
                               "uuid":"64333612"}
            self.checkConfig(self.AUDITCONFIGRELOAD, self.master, expectedResults)

    #Test error on setting of Invalid Log file path
    def test_invalidLogPath(self):
        auditIns = audit(host=self.master)
        newPath = auditIns.getAuditLogPath() + 'test'
        rest = RestConnection(self.master)
        status, content = rest.setAuditSettings(logPath=newPath)
        self.assertFalse(status, "Audit is able to set invalid path")
        self.assertEqual(content['errors']['logPath'], 'The value must be a valid directory', 'No error or error changed')

    #Test error on setting of Invalid log file in cluster
    def test_invalidLogPathCluster(self):
        auditIns = audit(host=self.master)
        newPath = auditIns.getAuditLogPath() + 'test'
        rest = RestConnection(self.master)
        status, content = rest.setAuditSettings(logPath=newPath)
        self.assertFalse(status, "Audit is able to set invalid path")
        self.assertEqual(content['errors']['logPath'], 'The value must be a valid directory', 'No error or error changed')

    #Test changing of log file path
    def test_changeLogPath(self):
        nodes_init = self.input.param("nodes_init", 0)
        auditMaster = audit(host=self.servers[0])
        auditSecNode = audit(host=self.servers[1])
        #Capture original Audit Log Path
        originalPath = auditMaster.getAuditLogPath()

        #Create folders on CB server machines and change permission
        try:
            newPath = auditMaster.getAuditLogPath() + "folder"

            for server in self.servers[:nodes_init]:
                shell = RemoteMachineShellConnection(server)
                try:
                    shell.create_directory(newPath)
                    command = 'chown couchbase:couchbase ' + newPath
                    shell.execute_command(command)
                finally:
                    shell.disconnect()

            source = 'ns_server'
            user = self.master.rest_username
            auditMaster.setAuditLogPath(newPath)

            #Create an event of Updating autofailover settings
            for server in self.servers[:nodes_init]:
                rest = RestConnection(server)
                expectedResults = {'max_nodes':1, "timeout":120, 'source':source, "user":user, 'ip':self.ipAddress, 'port':12345}
                rest.update_autofailover_settings(True, expectedResults['timeout'])

                self.sleep(120, 'Waiting for new audit file to get created')
                #check for audit.log remotely
                shell = RemoteMachineShellConnection(server)
                try:
                    result = shell.file_exists(newPath, auditMaster.AUDITLOGFILENAME)
                finally:
                    shell.disconnect()

                if (result is False):
                    self.assertTrue(result, 'Issue with file getting create in new directory')

        finally:
            auditMaster.setAuditLogPath(originalPath)

    #Check file rollover for different Server operations
    def test_cbServerOps(self):
        ops = self.input.param("ops", None)
        auditIns = audit(host=self.master)

        #Capture timestamp from first event for filename
        firstEventTime = self.getTimeStampForFile(auditIns)

        shell = RemoteMachineShellConnection(self.master)

        #Kill memcached to check for file roll over and new audit.log
        if (ops == "kill"):
            result = shell.kill_memcached()
            self.sleep(10)

        #Stop CB Server to check for file roll over and new audit.log
        if (ops == 'shutdown'):
            try:
                result = shell.stop_couchbase()
                self.sleep(120, 'Waiting for server to shutdown')
            finally:
                result = shell.start_couchbase()

        #Check for audit.log and for roll over file
        self.sleep(120, 'Waiting for server to start after shutdown')
        rest = RestConnection(self.master)
        #Create an Event for Bucket Creation
        #expectedResults = self.createBucketAudit(self.master, "TestBucketKillShutdown")
        status, content = rest.validateLogin("Administrator", "password", True, getContent=True)
        self.sleep(30)
        result = shell.file_exists(auditIns.pathLogFile, audit.AUDITLOGFILENAME)
        self.assertTrue(result, "Audit.log is not created when memcached server is killed or stopped")
        hostname = shell.execute_command("hostname")

        archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
        self.log.info ("Archive File expected is {0}".format(auditIns.pathLogFile + archiveFile))
        result = shell.file_exists(auditIns.pathLogFile, archiveFile)
        self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed or stopped")

        #archiveFile = auditIns.currentLogFile + "/" + archiveFile

        if (ops == 'shutdown'):
            expectedResult = {"source":"internal", "user":"couchbase", "id":4097, "name":"shutting down audit daemon", "description":"The audit daemon is being shutdown"}
            data = auditIns.returnEvent(4097, archiveFile)
            flag = True
            for items in data:
                if (items == 'timestamp'):
                    tempFlag = auditIns.validateTimeStamp(data['timestamp'])
                    if (tempFlag is False):
                        flag = False
                else:
                    if (isinstance(data[items], dict)):
                        for seclevel in data[items]:
                            tempValue = expectedResult[seclevel]
                            if data[items][seclevel] == tempValue:
                                self.log.info ('Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(tempValue, data[items][seclevel], seclevel))
                            else:
                                self.log.info ('Mis-Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(tempValue, data[items][seclevel], seclevel))
                                flag = False
                    else:
                        if (data[items] == expectedResult[items]):
                            self.log.info ('Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items.encode('utf-8')], data[items.encode('utf-8')], items))
                        else:
                            self.log.info ('Mis - Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items.encode('utf-8')], data[items.encode('utf-8')], items))
                            flag = False
            self.assertTrue(flag, "Shutdown event is not printed")

        expectedResults = {"auditd_enabled":auditIns.getAuditConfigElement('auditd_enabled'),
                           "descriptors_path":self.changePathWindows(auditIns.getAuditConfigElement('descriptors_path')),
                           "log_path":self.changePathWindows(auditIns.getAuditLogPath().strip()[:-2]),
                           'source':'internal', 'user':'couchbase',
                           "rotate_interval":auditIns.getAuditConfigElement('rotate_interval'),
                           "version":1, 'hostname':self.getHostName(self.master)}
        self.checkConfig(self.AUDITCONFIGRELOAD, self.master, expectedResults)


    #Disable audit event and check if event is printed in audit.log
    def test_eventDisabled(self):
        disableEvent = self.input.param("disableEvent", None)
        Audit = audit(host=self.master)
        temp = Audit.getAuditConfigElement('all')
        temp['disabled'] = [disableEvent]
        Audit.writeFile(lines=temp)
        rest = RestConnection(self.master)
        rest.update_autofailover_settings(True, 120)
        auditIns = audit(eventID=disableEvent, host=self.master)
        status = auditIns.checkLastEvent()
        self.assertFalse(status, "Event still getting printed after getting disabled")

    '''Test roll over of audit.log as per rotate interval'''
    def test_rotateInterval(self):
        intervalSec = self.input.param("intervalSec", None)
        auditIns = audit(host=self.master)
        rest = RestConnection(self.master)
        originalInt = auditIns.getAuditRotateInterval()
        try:
            firstEventTime = self.getTimeStampForFile(auditIns)
            self.log.info ("first time evetn is {0}".format(firstEventTime))
            auditIns.setAuditRotateInterval(intervalSec)
            self.sleep(intervalSec + 20, 'Sleep for log roll over to happen')
            status, content = rest.validateLogin(self.master.rest_username, self.master.rest_password, True, getContent=True)
            self.sleep(120)
            shell = RemoteMachineShellConnection(self.master)
            try:
                hostname = shell.execute_command("hostname")
                archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
                self.log.info ("Archive File Name is {0}".format(archiveFile))
                result = shell.file_exists(auditIns.pathLogFile, archiveFile)
                self.assertTrue(result, "Archive Audit.log is not created on time interval")
                self.log.info ("Validation of archive File created is True, Audit archive File is created {0}".format(archiveFile))
                result = shell.file_exists(auditIns.pathLogFile, auditIns.AUDITLOGFILENAME)
                self.assertTrue(result, "Audit.log is not created when memcached server is killed")
            finally:
                shell.disconnect()
        finally:
            auditIns.setAuditRotateInterval(originalInt)


    ''' Test roll over of audit.log as per rotate interval in a cluster'''
    def test_rotateIntervalCluster(self):
        intervalSec = self.input.param("intervalSec", None)
        nodes_init = self.input.param("nodes_init", 2)
        auditIns = audit(host=self.master)
	auditIns.setAuditEnable('true')
        originalInt = auditIns.getAuditRotateInterval()
        auditIns.setAuditRotateInterval(intervalSec)
        firstEventTime = []

        try:
            for i in range(len(self.servers[:nodes_init])):
                auditTemp = audit(host=self.servers[i])
                firstEventTime.append(self.getTimeStampForFile(auditTemp))

            self.sleep(intervalSec + 20, 'Sleep for log roll over to happen')

            for i in range(len(self.servers[:nodes_init])):
                shell = RemoteMachineShellConnection(self.servers[i])
                rest = RestConnection(self.servers[i])
                status, content = rest.validateLogin(self.master.rest_username, self.master.rest_password, True, getContent=True)
                self.sleep(120, "sleeping for log file creation")
                try:
                    hostname = shell.execute_command("hostname")
                    self.log.info ("print firstEventTime {0}".format(firstEventTime[i]))
                    archiveFile = hostname[0][0] + '-' + firstEventTime[i] + "-audit.log"
                    self.log.info ("Archive File Name is {0}".format(archiveFile))
                    result = shell.file_exists(auditIns.pathLogFile, archiveFile)
                    self.assertTrue(result, "Archive Audit.log is not created on time interval")
                    self.log.info ("Validation of archive File created is True, Audit archive File is created {0}".format(archiveFile))
                    result = shell.file_exists(auditIns.pathLogFile, auditIns.AUDITLOGFILENAME)
                    self.assertTrue(result, "Audit.log is not created as per the roll over time specified")
                finally:
                    shell.disconnect()
        finally:
            auditIns.setAuditRotateInterval(originalInt)

    ''' Test enabling/disabling in a cluster'''
    def test_enableStatusCluster(self):
        nodes_init = self.input.param("nodes_init", 2)
        auditIns = audit(host=self.master)
        origState = auditIns.getAuditStatus()
        auditIns.setAuditEnable('true')

        try:
            for i in range(len(self.servers[:nodes_init])):
                auditTemp = audit(host=self.servers[i])
                tempStatus = auditTemp.getAuditStatus()
                self.log.info ("value of current status is {0} on ip -{1}".format(tempStatus, self.servers[i].ip))
                self.assertTrue(tempStatus, "Audit is not enabled across the cluster")

            auditTemp = audit(host=self.servers[1])
            auditTemp.setAuditEnable('false')

            for i in range(len(self.servers[:nodes_init])):
                auditTemp = audit(host=self.servers[i])
                tempStatus = auditTemp.getAuditStatus()
                self.log.info ("value of current status is {0} on ip -{1}".format(tempStatus, self.servers[i].ip))
                self.assertFalse(tempStatus, "Audit is not enabled across the cluster")

        finally:
            auditIns.setAuditEnable(self.returnBoolVal(origState))


    '''Boundary Condition for rotate interval'''
    def test_rotateIntervalShort(self):
        intervalSec = self.input.param("intervalSec", None)
        auditIns = audit(host=self.master)
        auditIns.setAuditRotateInterval(intervalSec)
        originalInt = auditIns.getAuditRotateInterval()
        status, content = auditIns.setAuditRotateInterval(intervalSec)
        self.assertFalse(status, "Audit log interval setting is <900 or > 604800")
        self.assertEqual(content['errors']['rotateInterval'], 'The value must be in range from 15 minutes to 7 days')

    #Add test case where folder update does not exist in 2nd node - MB-13442
    def test_folderMisMatchCluster(self):
        auditIns = audit(host=self.master)
        orginalPath = auditIns.getAuditLogPath()
        newPath = originalPath + 'testFolderMisMatch'
        shell = RemoteMachineShellConnection(self.servers[0])
        try:
            shell.create_directory(newPath)
            command = 'chown couchbase:couchbase ' + newPath
            shell.execute_command(command)
        finally:
            shell.disconnect()

        auditIns.setsetAuditLogPath(newPath)

        for server in self.servers:
            rest = RestConnection(sever)
            #Create an Event for Bucket Creation
            expectedResults = {'name':'TestBucket ' + server.ip, 'ram_quota':536870912, 'num_replicas':1,
                                       'replica_index':False, 'eviction_policy':'value_only', 'type':'membase', \
                                       'auth_type':'sasl', "autocompaction":'false', "purge_interval":"undefined", \
                                        "flush_enabled":False, "num_threads":3, "source":source, \
                                       "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':'' }
            rest.create_bucket(expectedResults['name'], expectedResults['ram_quota'] // 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                       '11211', 'membase', 0, expectedResults['num_threads'], expectedResults['flush_enabled'], 'valueOnly')

            #Check on Events
            try:
                self.checkConfig(self.eventID, self.servers[0], expectedResults)
            except:
                self.log.info ("Issue reading the file at Node {0}".format(server.ip))


    #Add test case for MB-13511
    def test_fileRotate20MB(self):
        auditIns = audit(host=self.master)
        firstEventTime = self.getTimeStampForFile(auditIns)
        tempEventCounter = 0
        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)
        filePath = auditIns.pathLogFile + auditIns.AUDITLOGFILENAME
        number = int (shell.get_data_file_size(filePath))
        hostname = shell.execute_command("hostname")
        archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
        result = shell.file_exists(auditIns.pathLogFile, archiveFile)
        tempTime = 0
        starttime = time.time()
        while ((number < 21089520) and (tempTime < 36000) and (result == False)):
            for i in range(1, 10):
                status, content = rest.validateLogin("Administrator", "password", True, getContent=True)
                tempEventCounter += 1
                number = int (shell.get_data_file_size(filePath))
                currTime = time.time()
                tempTime = int (currTime - starttime)
                result = shell.file_exists(auditIns.pathLogFile, archiveFile)
        self.sleep(30)
        result = shell.file_exists(auditIns.pathLogFile, archiveFile)
        shell.disconnect()
        self.log.info ("--------Total Event Created ---- {0}".format(tempEventCounter))
        self.assertTrue(result, "Archive Audit.log is not created on reaching 20MB threshhold")

    def test_clusterEndToEnd(self):
        rest = []
        for i in range(0, len(self.servers)):
            node1 = self.servers[i]
            restNode1 = RestConnection(node1)
            rest.append(restNode1)

        auditNodeFirst = audit(host=self.servers[0])
        auditNodeSec = audit (host=self.servers[1])
        origLogPath = auditNodeFirst.getAuditLogPath()

        try:
            # Create Events on both the nodes
            for server in self.servers:
                rest = RestConnection(sever)
                #Create an Event for Bucket Creation
                expectedResults = self.createBucketAudit(server, "TestBucket" + server.ip)
                self.checkConfig(self.eventID, server, expectedResults)

            #Remove one node from the cluser
            self.cluster.rebalance(self.server, [], self.servers[1:self.nodes_out + 1])

            #Change path on first cluster + Create Bucket Event
            newPath = auditNodeFirst.getAuditLogPath() + "changeClusterLogPath"
            self.createRemoteFolder(self.server[0], newPath)
            auditNodeFirst.setAuditLogPath(newPath)
            expectedResults = self.createBucketAudit(self.server[0], "TestBucketFirstNode")
            self.checkConfig(self.eventID, self.servers[0], expectedResults)

            #Add one node to the cluster
            self.createRemoteFolder(self.server[1], newPath)

            self.cluster.rebalance(self.server, self.servers[1:self.nodes_out + 1], [])

            expectedResults = self.createBucketAudit(self.server[1], "TestBucketSecondNode")
            self.checkConfig(self.eventID, self.servers[1], expectedResults)

            #Change path on first cluster + Create Bucket Event
            auditNodeFirst.setAuditLogPath(origLogPath)
            expectedResults = self.createBucketAudit(self.server[0], "TestBucketFirstNode")
            self.checkConfig(self.eventID, self.servers[0], expectedResults)
        except:
            auditNodeFirst.setAuditLogPath(origLogPath)


class auditCLITest(CliBaseTest):

    def setUp(self):
        super(auditCLITest, self).setUp()
        self.enableStatus = self.input.param("enableStatus", None)
        self.logPath = self.input.param("logPath", None)
        self.rotateInt = self.input.param("rotateInt", None)
        self.errorMsg = self.input.param("errorMsg", None)
        self.ldapUser = self.input.param('ldapUser', 'Administrator')
        self.ldapPass = self.input.param('ldapPass', 'password')
        self.source = self.input.param('source', None)
        self.shell = RemoteMachineShellConnection(self.master)
        info = self.shell.extract_remote_info()
        self.os_type = info.type.lower()
        if self.os_type == 'windows' and self.source == 'saslauthd':
            raise Exception(" Ldap Tests cannot run on windows");
        elif self.source == 'saslauthd':
                rest = RestConnection(self.master)
                self.setupLDAPSettings(rest)
                #rest.ldapUserRestOperation(True, [[self.ldapUser]], exclude=None)
                self.set_user_role(rest, self.ldapUser)

    def tearDown(self):
        super(auditCLITest, self).tearDown()

    def set_user_role(self,rest,username,user_role='admin'):
        payload = "name=" + username + "&roles=" + user_role
        content = rest.set_user_roles(user_id=username, payload=payload)

    def setupLDAPSettings (self, rest):
        api = rest.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({"enabled":'true',"admins":[],"roAdmins":[]})
        status, content, header = rest._http_request(api, 'POST', params)
        return status, content, header

    def returnBool(self, boolString):
        if boolString in ('True', True, 'true'):
            return 1
        else:
            return 0

    def returnBoolVal(self, boolVal):
        if boolVal == 1:
            return 'true'
        else:
            return 'false'



    def test_enableDisableAudit(self):
        auditIns = audit(host=self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        tempEnable = auditIns.getAuditStatus()
        try:
            cli_command = 'setting-audit'
            options = "--audit-enable=0"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
            tempEnable = auditIns.getAuditStatus()
            self.assertFalse(tempEnable, "Issues enable/disable via CLI")
            if self.os_type == 'windows':
                log_path = audit.WINLOGFILEPATH
            else:
                log_path = audit.LINLOGFILEPATH
            options = "--audit-enable=1 --audit-log-path=" + log_path
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
            tempEnable = auditIns.getAuditStatus()
            self.assertTrue(tempEnable, "Issues enable/disable via CLI")
        finally:
            auditIns.setAuditEnable(self.returnBoolVal(tempEnable))

    def test_setAuditParam(self):
        auditIns = audit(host=self.master)
        tempEnable = auditIns.getAuditStatus()
        tempLogPath = auditIns.getAuditLogPath()
        tempRotateInt = auditIns.getAuditRotateInterval()
        try:
            remote_client = RemoteMachineShellConnection(self.master)
            cli_command = "setting-audit"
            options = " --audit-enable={0}".format(self.enableStatus)
            options += " --audit-log-rotate-interval={0}".format(self.rotateInt)
            options += " --audit-log-path={0}".format(self.logPath)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
            temp_rotate_int = self.rotateInt*60
            tempFlag = self.validateSettings(self.enableStatus, self.logPath, temp_rotate_int)
            self.assertTrue(tempFlag)
        finally:
            auditIns.setAuditEnable(self.returnBoolVal(tempEnable))
            auditIns.setAuditLogPath(tempLogPath)
            auditIns.setAuditRotateInterval(tempRotateInt)


    def validateSettings(self, status, log_path, rotate_interval):
        auditIns = audit(host=self.master)
        tempLogPath = (auditIns.getAuditLogPath())[:-1]
        tempStatus = auditIns.getAuditStatus()
        tempRotateInt = auditIns.getAuditRotateInterval()
        flag = True

        if (status != self.returnBool(tempStatus)):
            self.log.info ("Mismatch with status - Expected - {0} -- Actual - {1}".format(status, tempStatus))
            flag = False

        if (log_path != tempLogPath):
            self.log.info ("Mismatch with log path - Expected - {0} -- Actual - {1}".format(log_path, tempLogPath))
            flag = False

        if (rotate_interval != tempRotateInt):
            self.log.info ("Mismatch with rotate interval - Expected - {0} -- Actual - {1}".format(rotate_interval, tempRotateInt))
            flag = False
        return flag
