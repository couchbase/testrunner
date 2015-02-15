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
import commands
from security.auditmain import audit


class auditcheckconfig(BaseTestCase):
    AUDITCONFIGRELOAD = 4096
    AUDITCONFIGEDISABLED = 4098
    AUDITCONFIGENABLED = 4097
    AUDITSHUTDOWN = 4099

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
        status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        return ipAddress

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

    def createBucketAudit(self, host, bucketName):
        rest = RestConnection(host)
        #Create an Event for Bucket Creation
        expectedResults = {'name':bucketName, 'ram_quota':536870912, 'num_replicas':1,
                                   'replica_index':False, 'eviction_policy':'value_only', 'type':'membase', \
                                    'auth_type':'sasl', "autocompaction":'false', "purge_interval":"undefined", \
                                    "flush_enabled":False, "num_threads":3, "source":source, \
                                    "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':'' }
        rest.create_bucket(expectedResults['name'], expectedResults['ram_quota'] / 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                    '11211', 'membase', 0, expectedResults['num_threads'], expectedResults['flush_enabled'], 'valueOnly')
        return expectedResults

    '''
    checkConfig - Wrapper around audit class
    Parameters:
        eventID - eventID of the event 
        host - host for creating and reading event
        expectedResult - dictionary of fields and value for event
    '''
    def checkConfig(self, eventID, host, expectedResults):
        Audit = audit(eventID=eventID, host=self.master)
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

        #Validate that initial configuration is loaded with audit false
        expectedResults = {"archive_path":auditIns.getArchivePath(), "auditd_enabled":auditIns.getAuditStatus(),
                           "descriptors_path":auditIns.getAuditConfigElement('descriptors_path'),
                           "log_path":auditIns.getAuditLogPath(), "source":"internal",
                           "user":"couchbase", "rotate_interval":86400, "version":1, 'hostname':self.getHostName(self.master)}
        self.checkConfig(self.AUDITCONFIGRELOAD, self.master, expectedResults)

        #Validate event for event disabled for default
        expectedResults = {"source":"internal", "user":"couchbase"}
        self.checkConfig(self.AUDITCONFIGEDISABLED, self.master, expectedResults)

    '''
    Check enabled disable and reload of config
    '''
    def test_AuditEvent(self):
        ops = self.input.param("ops", None)
        source = 'internal'
        user = 'couchbase'
        rest = RestConnection(self.master)
        status = rest.setAuditSettings(enabled='true')
        if (ops in ['enable', 'disable']):
            if ops == 'disable':
                status = rest.setAuditSettings(enabled='false')
            else:
                status = rest.setAuditSettings(enabled='true')
        expectedResults = {'source':source, 'user':user}

        self.checkConfig(self.eventID, self.master, expectedResults)

        auditIns = audit(host=self.master)
        expectedResults = {"archive_path":auditIns.getArchivePath(), "auditd_enabled":auditIns.getAuditStatus(),
                           "descriptors_path":auditIns.getAuditConfigElement('descriptors_path'),
                           "log_path":auditIns.getAuditLogPath(), "source":"internal",
                           "user":"couchbase", "rotate_interval":86400, "version":1, 'hostname':self.getHostName(self.master)}
        self.checkConfig(self.AUDITCONFIGRELOAD, self.master, expectedResults)

    #Test error on setting of Invalid Log file path
    def test_invalidLogPath(self):
        auditIns = audit(host=self.master)
        newPath = auditIns.getAuditLogPath() + 'test'
        rest = RestConnection(self.master)
        status, content = rest.setAuditSettings(logPath=newPath)
        self.assertFalse(status, "Audit is able to set invalid path")
        self.assertEqual(content['errors']['log_path'], 'The value of log_path must be a valid directory', 'No error or error changed')

    #Test error on setting on Invalid archive path
    def test_invalidArchivePath(self):
        auditIns = audit(host=self.master)
        newPath = auditIns.getArchivePath() + 'test'
        rest = RestConnection(self.master)
        status, content = rest.setAuditSettings(archivePath=newPath)
        self.assertFalse(status, "Audit is able to set invalid path")
        self.assertEqual(content['errors']['archive_path'], 'The value of archive_path must be a valid directory', 'No error or error changed')

    #Test error on setting of Invalid log file in cluster
    def test_invalidLogPathCluster(self):
        auditIns = audit(host=self.master)
        newPath = auditIns.getAuditLogPath() + 'test'
        rest = RestConnection(self.master)
        status, content = rest.setAuditSettings(logPath=newPath)
        self.assertFalse(status, "Audit is able to set invalid path")
        self.assertEqual(content['errors']['log_path'], 'The value of log_path must be a valid directory', 'No error or error changed')

    #Test changing of log file path
    def test_changeLogPath(self):
        auditMaster = audit(host=self.servers[0])
        auditSecNode = audit(host=self.servers[1])
        #Capture original Audit Log Path
        originalPath = auditMaster.getAuditLogPath()

        #Create folders on CB server machines and change permission
        try:
            newPath = auditMaster.getAuditLogPath() + "/folder"

            for server in self.servers:
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
            for server in self.servers:
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

                if (result):
                    self.checkConfig(8220, server, expectedResults)
                else:
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
                if (result):
                    expectedResults = {'source':'internal', 'user':'couchbase'}
                    self.checkConfig(self.AUDITSHUTDOWN, self.master, expectedResults)
            finally:
                result = shell.start_couchbase()

        #Check for audit.log and for roll over file
        self.sleep(120, 'Waiting for server to start after shutdown')
        result = shell.file_exists(auditIns.pathLogFile, audit.AUDITLOGFILENAME)
        self.assertTrue(result, "Audit.log is not created when memcached server is killed or stopped")
        hostname = shell.execute_command("hostname")
        archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
        result = shell.file_exists(auditIns.archiveFilePath, archiveFile)
        self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed or stopped")

        #check for events of config enabled and config reload
        expectedResults = {'source':'internal', 'user':'couchbase'}
        self.checkConfig(self.AUDITCONFIGENABLED, self.master, expectedResults)

        expectedResults = {"archive_path":auditIns.getAuditConfigElement('archive_path'),
                           "auditd_enabled":auditIns.getAuditConfigElement('auditd_enabled'),
                           "descriptors_path":auditIns.getAuditConfigElement('descriptors_path'),
                           "log_path":auditIns.getAuditConfigElement('log_path'),
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

    #Check change to archive_log and checking for file roll over
    def test_ArchiveLogChangePath(self):
        auditIns = audit(host=self.master)
        origArchivePath = auditIns.getArchivePath()
        newPath = origArchivePath + "/archiveFolder"

        try:
            shell = RemoteMachineShellConnection(self.servers[0])
            try:
                shell.create_directory(newPath)
                command = 'chown couchbase:couchbase ' + newPath
                shell.execute_command(command)
            finally:
                shell.disconnect()

            auditIns.setAuditArchivePath(newPath)
            shell = RemoteMachineShellConnection(self.master)

            try:
                firstEventTime = self.getTimeStampForFile(auditIns)
                result = shell.kill_memcached()
                self.sleep(10)
                result = shell.file_exists(auditIns.pathLogFile, audit.AUDITLOGFILENAME)
                self.assertTrue(result, "Audit.log is not created when memcached server is killed")
                hostname = shell.execute_command("hostname")
                archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
                result = shell.file_exists(newPath, archiveFile)
                self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed")
            finally:
                shell.disconnect()

        finally:
            auditIns.setAuditArchivePath(origArchivePath)

    #Check change to archive_log in cluster and check on each node in the cluster
    def test_ArchiveLogChangePathCluster(self):
        auditMaster = audit(host=self.servers[0])
        auditSecNode = audit(host=self.servers[1])
        originalPath = auditMaster.getArchivePath()

        try:
            newPath = auditMaster.getAuditLogPath() + "/archivefoldercluster"

            for servers in self.servers:
                shell = RemoteMachineShellConnection(servers)
                try:
                    shell.create_directory(newPath)
                    command = 'chown couchbase:couchbase ' + newPath
                    shell.execute_command(command)
                finally:
                    shell.disconnect()

            source = 'ns_server'
            user = self.master.rest_username

            auditMaster.setAuditArchivePath(newPath)

            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                try:
                    tempAudit = audit(host=server)
                    firstEventTime = self.getTimeStampForFile(tempAudit)
                    result = shell.kill_memcached()
                    self.sleep(120)
                    result = shell.file_exists(auditMaster.pathLogFile, audit.AUDITLOGFILENAME)
                    self.assertTrue(result, "Audit.log is not created when memcached server is killed")
                    hostname = shell.execute_command("hostname")
                    archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
                    result = shell.file_exists(newPath, archiveFile)
                    self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed")
                finally:
                    shell.disconnect()
        finally:
            auditMaster.setAuditArchivePath(originalPath)

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
                result = shell.file_exists(auditIns.archiveFilePath, archiveFile)
                self.assertTrue(result, "Archive Audit.log is not created on time interval")
                result = shell.file_exists(auditIns.pathLogFile, auditIns.AUDITLOGFILENAME)
                self.assertTrue(result, "Audit.log is not created when memcached server is killed")
            finally:
                shell.disconnect()
        finally:
            auditIns.setAuditRotateInterval(originalInt)

    ''' Test roll over of audit.log as per rotate interval in a cluster'''
    def test_rotateIntervalCluster(self):
        intervalSec = self.input.param("intervalSec", None)
        auditIns = audit(host=self.master)
        originalInt = auditIns.getAuditRotateInterval()
        auditIns.setAuditRotateInterval(intervalSec)
        firstEventTime = []

        try:
            for i in range(len(self.servers)):
                auditTemp = audit(host=self.servers[i])
                firstEventTime.append(self.getTimeStampForFile(auditTemp))

            self.sleep(intervalSec + 20, 'Sleep for log roll over to happen')

            for i in range(len(self.servers)):
                shell = RemoteMachineShellConnection(self.servers[i])
                rest = RestConnection(self.servers[i])
                status, content = rest.validateLogin(self.master.rest_username, self.master.rest_password, True, getContent=True)
                self.sleep(120, "sleeping for log file creation")
                try:
                    hostname = shell.execute_command("hostname")
                    self.log.info ("print firstEventTime {0}".format(firstEventTime[i]))
                    archiveFile = hostname[0][0] + '-' + firstEventTime[i] + "-audit.log"
                    result = shell.file_exists(auditIns.archiveFilePath, archiveFile)
                    self.assertTrue(result, "Archive Audit.log is not created on time interval")
                    result = shell.file_exists(auditMaster.pathLogFile, auditIns.AUDITLOGFILENAME)
                    self.assertTrue(result, "Audit.log is not created when memcached server is killed")
                finally:
                    shell.disconnect()
        finally:
            auditIns.setAuditRotateInterval(originalInt)


    '''Boundary Condition for rotate interval'''
    def test_rotateIntervalShort(self):
        intervalSec = self.input.param("intervalSec", None)
        auditIns = audit(host=self.master)
        auditIns.setAuditRotateInterval(intervalSec)
        originalInt = auditIns.getAuditRotateInterval()
        status, content = auditIns.setAuditRotateInterval(intervalSec)
        self.assertFalse(status, "Audit log interval setting is <900 or > 604800")
        self.assertEqual(content['errors']['rotate_interval'], 'The value of rotate_interval must be in range from 900 to 604800')

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
            rest.create_bucket(expectedResults['name'], expectedResults['ram_quota'] / 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                       '11211', 'membase', 0, expectedResults['num_threads'], expectedResults['flush_enabled'], 'valueOnly')

            #Check on Events
            try:
                self.checkConfig(self.eventID, self.servers[0], expectedResults)
            except:
                self.log.info ("Issue reading the file at Node {0}".format(server.ip))


    #Add test case for MB-13511
    def test_fileRotate20MB(self):
        auditIns = audit(host=self.master)
        firstEventTime = auditIns.getTimeStampFirstEvent()

        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)

        filePath = auditIns.pathLogFile() + auditIns.AUDITLOGFILENAME
        fileSize = int(shell.get_data_file_size(filePath))
        originalFileSize = int (shell.get_data_file_size(filePath))
        while (number < 19922944):
            for i in range(1, 50):
                status, content = rest.validateLogin("Administrator", "password", True, getContent=True)
            number = int (shell.get_data_file_size(filePath))

        hostname = shell.execute_command("hostname")
        archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
        result = shell.file_exists(auditIns.archiveFilePath, archiveFile)
        self.assertTrue(result, "Archive Audit.log is not created on reaching 20MB threshhold")

    def test_clusterEndToEnd(self):
        rest = []
        for i in range(0, len(self.servers)):
            node1 = self.servers[i]
            restNode1 = RestConnection(node1)
            rest.append(restNode1)

        auditNodeFirst = audit(host=server[0])
        auditNodeSec = audit (host=server[1])
        origLogPath = auditNodeFirst.getAuditLogPath()

        try:
            # Create Events on both the nodes
            for server in self.servers:
                rest = RestConnection(sever)
                #Create an Event for Bucket Creation
                expectedResults = self.createBucketAudit(server, "Test Bucket" + server.ip)
                self.checkConfig(self.eventID, server, expectedResults)

            #Remove one node from the cluser
            self.cluster.rebalance(self.server, [], self.servers[1])

            #Change path on first cluster + Create Bucket Event
            newPath = auditNodeFirst.getAuditLogPath() + "changeClusterLogPath"
            self.createRemoteFolder(self.server[0], newPath)
            auditNodeFirst.setAuditLogPath(newPath)
            expectedResults = self.createBucketAudit(self.server[0], "TestBucketFirstNode")
            self.checkConfig(self.eventID, self.servers[0], expectedResults)

            #Add one node to the cluster
            self.createRemoteFolder(self.server[1], newPath)
            self.cluster.rebalance(self.server, self.servers[1], [])
            expectedResults = self.createBucketAudit(self.server[1], "TestBucketSecondNode")
            self.checkConfig(self.eventID, self.servers[1], expectedResults)

            #Change path on first cluster + Create Bucket Event
            auditNodeFirst.setAuditLogPath(origLogPath)
            expectedResults = self.createBucketAudit(self.server[0], "TestBucketFirstNode")
            self.checkConfig(self.eventID, self.servers[0], expectedResults)
        except:
            auditNodeFirst.setAuditLogPath(origLogPath)






