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
        self.assertFalse(auditIns.getAuditStatus(), "Audit is not disabled by default")

        #Validate that initial configuration is loaded with audit false
        expectedResults = {"archive_path":auditIns.getArchivePath(), "auditd_enabled":auditIns.getAuditStatus(),
                           "descriptors_path":auditIns.getAuditConfigElement('descriptors_path'),
                           "log_path":auditIns.getAuditLogPath(), "source":"internal",
                           "user":"couchbase", "rotate_interval":86400, "version":1}
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
                           "user":"couchbase", "rotate_interval":86400, "version":1}
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
            shell = RemoteMachineShellConnection(self.servers[0])
            try:
                shell.create_directory(newPath)
                command = 'chown couchbase:couchbase ' + newPath
                shell.execute_command(command)
            finally:
                shell.disconnect()

            shell = RemoteMachineShellConnection(self.servers[1])
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
            rest = RestConnection(self.master)
            expectedResults = {'max_nodes':1, "timeout":120, 'source':source, "user":user, 'ip':self.ipAddress, 'port':12345}
            rest.update_autofailover_settings(True, expectedResults['timeout'])

            #check for audit.log remotely
            shell = RemoteMachineShellConnection(self.servers[0])
            try:
                result = shell.file_exists(newPath, 'audit.log')
            finally:
                shell.disconnect()

            if (result):
                self.checkConfig(8220, self.master, expectedResults)
            else:
                self.assertTrue(result, 'Issue with file getting create in new directory')


            servs_inout = self.servers[1]
            rest = RestConnection(servs_inout)
            expectedResults = {'max_nodes':1, "timeout":120, 'source':source, "user":user, 'ip':self.ipAddress, 'port':12345}
            rest.update_autofailover_settings(True, expectedResults['timeout'])
            #check for audit.log remotely
            shell = RemoteMachineShellConnection(servs_inout)
            try:
                result = shell.file_exists(newPath, 'audit.log')
            finally:
                shell.disconnect()

            if (result):
                self.checkConfig(8220, servs_inout, expectedResults)
            else:
                self.assertTrue(result, 'Issue with file getting create in new directory')
        finally:
            auditMaster.setAuditLogPath(originalPath)

    #Check file rollover for different Server operations
    def test_cbServerOps(self):
        ops = self.input.param("ops", None)
        auditIns = audit(host=self.master)

        #Kill memcached to check for file roll over and new audit.log
        if (ops == "kill"):
            shell = RemoteMachineShellConnection(self.master)
        try:
            firstEventTime = audit.getTimeStampFirstEvent()
            result = shell.kill_memcached()
            self.sleep(10)
            result = shell.file_exists(auditIns.pathLogFile, audit.AUDITLOGFILENAME)
            self.assertTrue(result, "Audit.log is not created when memcached server is killed")
            hostname = shell.execute_command("hostname")
            archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
            result = shell.file_exists(auditIns.archiveFilePath, archiveFile)
            self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed")
        finally:
            shell.disconnect()

        #Stop CB Server to check for file roll over and new audit.log
        if (ops == 'shutdown'):
            shell = RemoteMachineShellConnection(self.master)
        try:
            result = shell.stop_couchbase()
            if (result):
                expectedResults = {'source':'internal', 'user':'couchbase'}
                self.checkConfig(4099, self.master, expectedResults)
        finally:
            result = shell.start_couchbase()
            shell.disconnect()

        expectedResults = {'source':'internal', 'user':'couchbase'}
        self.checkConfig(4097, self.master, expectedResults)

        expectedResults = {"archive_path":auditIns.getAuditConfigElement('archive_path'),
                           "auditd_enabled":auditIns.getAuditConfigElement('auditd_enabled'),
                           "descriptors_path":auditIns.getAuditConfigElement('descriptors_path'),
                           "log_path":auditIns.getAuditConfigElement('log_path'),
                           'source':'internal', 'user':'couchbase',
                           "rotate_interval":auditIns.getAuditConfigElement('rotate_interval'),
                           "version":1}
        self.checkConfig(4096, self.master, expectedResults)

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
                firstEventTime = auditIns.getTimeStampFirstEvent()
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

            shell = RemoteMachineShellConnection(self.servers[0])
            try:
                shell.create_directory(newPath)
                command = 'chown couchbase:couchbase ' + newPath
                shell.execute_command(command)
            finally:
                shell.disconnect()

            shell = RemoteMachineShellConnection(self.servers[1])
            try:
                shell.create_directory(newPath)
                command = 'chown couchbase:couchbase ' + newPath
                shell.execute_command(command)
            finally:
                shell.disconnect()

            source = 'ns_server'
            user = self.master.rest_username

            auditMaster.setAuditArchivePath(newPath)
            shell = RemoteMachineShellConnection(self.servers[0])
            try:
                firstEventTime = auditMaster.getTimeStampFirstEvent()
                result = shell.kill_memcached()
                self.sleep(10)
                result = shell.file_exists(auditMaster.pathLogFile, audit.AUDITLOGFILENAME)
                self.assertTrue(result, "Audit.log is not created when memcached server is killed")
                hostname = shell.execute_command("hostname")
                archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
                result = shell.file_exists(newPath, archiveFile)
                self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed")
            finally:
                shell.disconnect()


            auditSecNode.setAuditArchivePath(newPath)
            shell = RemoteMachineShellConnection(self.servers[1])
            try:
                firstEventTime = auditSecNode.getTimeStampFirstEvent()
                result = shell.kill_memcached()
                self.sleep(10)
                result = shell.file_exists(auditSecNode.pathLogFile, audit.AUDITLOGFILENAME)
                self.assertTrue(result, "Audit.log is not created when memcached server is killed")
                hostname = shell.execute_command("hostname")
                archiveFile = hostname[0][0] + '-' + firstEventTime + "-audit.log"
                result = shell.file_exists(newPath, archiveFile)
                self.assertTrue(result, "Archive Audit.log is not created when memcached server is killed")
            finally:
                shell.disconnect()

        finally:
            auditMaster.setAuditArchivePath(originalPath)
            auditSecNode.setAuditArchivePath(originalPath)