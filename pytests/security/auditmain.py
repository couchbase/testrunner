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
from testconstants import LINUX_DISTRIBUTION_NAME
from random import randint
from datetime import datetime
import time
import subprocess
import logger
import traceback
log = logger.Logger.get_logger()
import socket

class audit:
    AUDITLOGFILENAME = 'current-audit.log'
    AUDITCONFIGFILENAME = 'audit.json'
    AUDITDESCFILE = 'audit_events.json'
    WINLOGFILEPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/logs"
    LINLOGFILEPATH = "/opt/couchbase/var/lib/couchbase/logs"
    MACLOGFILEPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/logs"
    WINCONFIFFILEPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/config/"
    LINCONFIGFILEPATH = "/opt/couchbase/var/lib/couchbase/config/"
    MACCONFIGFILEPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/config/"
    DOWNLOADPATH = "/tmp/"

    def __init__(self,
                 eventID=None,
                 host=None,
                 method='REST'):

        rest = RestConnection(host)
        if (rest.is_enterprise_edition()):
            log.info ("Enterprise Edition, Audit is part of the test")
        else:
            raise Exception(" Install is not an enterprise edition, Audit requires enterprise edition.")
        self.method = method
        self.host = host
        self.nonroot = False
        self.slaveAddress = None
        shell = RemoteMachineShellConnection(self.host)
        self.info = shell.extract_remote_info()
        if self.info.distribution_type.lower() in LINUX_DISTRIBUTION_NAME and \
                                                    host.ssh_username != "root":
            self.nonroot = True
        shell.disconnect()
        self.pathDescriptor = self.getAuditConfigElement("descriptors_path") + "/"
        self.pathLogFile = self.getAuditLogPath()
        self.defaultFields = ['id', 'name', 'description']
        if eventID is not None:
            self.eventID = eventID
            self.eventDef = self.returnEventsDef()
        try:
            if "ip6" in self.host.ip or self.host.ip.startswith("["):
                self.slaveAddress = self.getLocalIPV6Address()
            else:
                self.slaveAddress = self.getLocalIPAddress()
        except Exception as ex:
            log.info ('Exception while generating ip address {0}'.format(ex))
            self.slaveAddress = '127.0.0.1'
    
    
    def getLocalIPAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]
        '''
        status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | awk '{print $2}'")
        return ipAddress
        '''

    def getLocalIPV6Address(self):
        result = socket.getaddrinfo(socket.gethostname(), 0, socket.AF_INET6)
        return result[0][4][0]

    def getAuditConfigPathInitial(self):
        shell = RemoteMachineShellConnection(self.host)
        os_type = shell.extract_remote_info().distribution_type
        dist_ver = (shell.extract_remote_info().distribution_version).rstrip()
        log.info ("OS type is {0}".format(os_type))
        if os_type == 'windows':
            auditconfigpath = audit.WINCONFIFFILEPATH
            self.currentLogFile = audit.WINLOGFILEPATH
        elif os_type == 'Mac':
            if ('10.12' == dist_ver):
                auditconfigpath = "/Users/admin/Library/Application Support/Couchbase/var/lib/couchbase/config/"
                self.currentLogFile = "/Users/admin/Library/Application Support/Couchbase/var/lib/couchbase/logs"
            else:
                auditconfigpath = audit.MACCONFIGFILEPATH
                self.currentLogFile = audit.MACLOGFILEPATH
        else:
            if self.nonroot:
                auditconfigpath = "/home/%s/cb%s" % (self.host.ssh_username,
                                                  audit.LINCONFIGFILEPATH)
                self.currentLogFile = "/home/%s/cb%s" % (self.host.ssh_username,
                                                      audit.LINLOGFILEPATH)
            else:
                auditconfigpath = audit.LINCONFIGFILEPATH
                self.currentLogFile = audit.LINLOGFILEPATH
        return auditconfigpath

    '''
    setAuditConfigPath - External function to set configPATH
    Parameters:
    configPath - path to config file
    Returns : None
    '''
    def setAuditConfigPath(self, configPath):
        self.auditConfigPath = configPath

    '''
    getAuditConfigPath
    Returns - Path to audit config file
    '''
    def getAuditConfigPath(self):
        return self.auditConfigPath

    '''
    readFile - copy file to local '/tmp' directory
    Parameters:
        pathAuditFile - remove file path
        fileName - file that needs to be copied
    Returns:
        None
    '''
    def getRemoteFile(self, host, remotepath, filename):
        shell = RemoteMachineShellConnection(host)
        try:
            sftp = shell._ssh_client.open_sftp()
            tempfile = str(remotepath + filename)
            tmpfile = audit.DOWNLOADPATH + filename
            log.info ("Value of remotepath is {0} and current Path - {1}".format(tempfile, tmpfile))
            sftp.get('{0}'.format(tempfile), '{0}'.format(tmpfile))
            sftp.close()
        except Exception as e:
            log.info (" Value of e is {0}".format(e))
            shell.disconnect()


    def readFile(self, pathAuditFile, fileName):
        self.getRemoteFile(self.host, pathAuditFile, fileName)

    '''
    writeFile - writing the config file
    Parameters:
        pathAuditFile - path to audit config
        fileName - name of audit config file
        lines - lines that need to be copied
    Returns:
        none
    '''
    def writeFile(self, pathAuditFile=None, fileName=None, lines=None):
        if (pathAuditFile is None):
            pathAuditFile = self.getAuditConfigPathInitial()
        if (fileName is None):
            fileName = audit.AUDITCONFIGFILENAME
        shell = RemoteMachineShellConnection(self.host)
        try:
            with open ("/tmp/audit.json", 'w') as outfile:
                json.dump(lines, outfile)
            result = shell.copy_file_local_to_remote('/tmp/audit.json', pathAuditFile + fileName)
        finally:
            shell.disconnect()

    '''
    returnEvent - reads actual audit event from audit.log file and returns last event
    Parameters:
        eventNumber - event number that needs to be queried
    Returns:
        dictionary of actual event from audit.log
    '''
    def returnEvent(self, eventNumber, audit_log_file=None, filtering=False):
        try:
            data = []
            if audit_log_file is None:
                audit_log_file = audit.AUDITLOGFILENAME
            self.readFile(self.pathLogFile, audit_log_file)
            with open(audit.DOWNLOADPATH + audit_log_file) as f:
                for line in f:
                    tempJson = json.loads(line)
                    if (tempJson['id'] == eventNumber):
                        data.append(json.loads(line))
            f.close()
            return data[len(data) - 1]
        except:
            log.info("ERROR ---- Event Not Found in audit.log file. Please check the log file")
            if filtering:
                return None

    '''
    getAuditConfigElement - get element of a configuration file
    Parameters
        element - element from audit config file
    Returns
        element of the config file or entire file if element == 'all'
    '''
    def getAuditConfigElement(self, element):
        data = []
        self.readFile(self.getAuditConfigPathInitial(), audit.AUDITCONFIGFILENAME)
        json_data = open (audit.DOWNLOADPATH + audit.AUDITCONFIGFILENAME)
        data = json.load(json_data)
        if (element == 'all'):
            return data
        else:
            return data[element]

    '''
    returnEventsDef - read event definition
    Parameters:None
    Returns:
        list of events from audit_events.json file
    '''
    def returnEventsDef(self):
        data = []
        self.readFile(self.pathDescriptor, audit.AUDITDESCFILE)
        json_data = open (audit.DOWNLOADPATH + audit.AUDITDESCFILE)
        data = json.load(json_data)
        return data

    '''
    getAuditLogPath - return value of log_path from REST API
    Returns:
        returns log_path from audit config file
    '''
    def getAuditLogPath(self):
        rest = RestConnection(self.host)
        content = rest.getAuditSettings()
        return content['logPath'] + "/"

    '''
    getAuditStatus - return value of audit status from REST API
    Returns:
        returns audit status from audit config file
    '''
    def getAuditStatus(self):
        rest = RestConnection(self.host)
        content = rest.getAuditSettings()
        return content['auditdEnabled']

    '''
    getAuditRotateInterval - return value of rotate Interval from REST API
    Returns:
        returns audit status from audit config file
    '''
    def getAuditRotateInterval(self):
        rest = RestConnection(self.host)
        content = rest.getAuditSettings()
        return content['rotateInterval']

    '''
    setAuditLogPath - set log_path via REST API
    Parameter:
        auditLogPath - path to log_path
    Returns:
        status - status rest command
    '''
    def setAuditLogPath(self, auditLogPath):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(logPath=auditLogPath)
        return status

    '''
    setAuditEnable - set audit_enabled via REST API
    Parameter:
        audit_enabled - true/false for setting audit status
    Returns:
        status - status rest command
    '''
    def setAuditEnable(self, auditEnable):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(enabled=auditEnable, logPath=self.currentLogFile)
        return status

    '''
    checkConfig - Wrapper around audit class
    Parameters:
        expectedResult - dictionary of fields and value for event
    '''
    def checkConfig(self, expectedResults, n1ql_audit=False):
        fieldVerification, valueVerification = self.validateEvents(expectedResults, n1ql_audit)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    '''
    setAuditRotateInterval - set rotate_internval via REST API
    Parameter:
        rotate_internval - log rotate interval
    Returns:
        status - status rest command
    '''
    def setAuditRotateInterval(self, rotateInterval):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(rotateInterval=rotateInterval, logPath=self.currentLogFile)
        return status

    '''
    getTimeStampFirstEvent - timestamp of first event in audit.log
    Returns:
        timestamp of first event in audit.log
    '''
    def getTimeStampFirstEvent(self):
        self.readFile(self.pathLogFile, audit.AUDITLOGFILENAME)
        with open(audit.DOWNLOADPATH + audit.AUDITLOGFILENAME) as f:
            data = line = f.readline()
        data = ((json.loads(line))['timestamp'])[:19]
        return data


    '''
    returnFieldsDef - returns event definition separated by sections
    Parameters:
        data - audit_events.json file in a list of dictionary
        eventNumber - event number that needs to be queried
    Returns:
        defaultFields - dictionary of default fields
        mandatoryFields - list is dictionary of mandatory fields
        mandatorySecLevel - list of dictionary containing 2nd level of mandatory fields
        optionalFields - list is dictionary of optional fields
        optionalSecLevel - list of dictionary containing 2nd level of optional fields
    '''
    def returnFieldsDef(self, data, eventNumber):
        defaultFields = {}
        mandatoryFields = []
        mandatorySecLevel = []
        optionalFields = []
        optionalSecLevel = []
        fields = ['mandatory_fields', 'optional_fields']
        for items in data['modules']:
            for particulars in items['events']:
                if particulars['id'] == eventNumber:
                    for key, value in list(particulars.items()):
                        if (key not in fields):
                            defaultFields[key] = value
                        elif key == 'mandatory_fields':
                            for items in particulars['mandatory_fields']:
                                #log.info("-->items:{},{}".format(type(items),items))
                                try:
                                  items = items.decode()
                                except AttributeError:
                                  pass
                                mandatoryFields.append(items)
                                if (isinstance((particulars['mandatory_fields'][items]), dict)):
                                    tempStr = items
                                    for secLevel in list(particulars['mandatory_fields'][items].items()):
                                        tempStr = tempStr + ":" + secLevel[0]
                                    mandatorySecLevel.append(tempStr)
                        elif key == 'optional_fields':
                            for items in particulars['optional_fields']:
                                optionalFields.append(items)
                                #log.info("-->items:{},{}".format(type(items),items))
                                try:
                                  items = items.decode()
                                except AttributeError:
                                  pass
                                if (isinstance((particulars['optional_fields'][items]), dict)):
                                    tempStr = items
                                    for secLevel in list(particulars['optional_fields'][items].items()):
                                        tempStr = tempStr + ":" + secLevel[0]
                                    optionalSecLevel.append(tempStr)

        #log.info ("Value of default fields is - {0}".format(defaultFields))
        #log.info ("Value of mandatory fields is {0}".format(mandatoryFields))
        #log.info ("Value of mandatory sec level is {0}".format(mandatorySecLevel))
        #log.info ("Value of optional fields i {0}".format(optionalFields))
        #log.info ("Value of optional sec level is {0}".format(optionalSecLevel))
        return defaultFields, mandatoryFields, mandatorySecLevel, optionalFields, optionalSecLevel

    '''
    returnFieldsDef - returns event definition separated by sections
    Parameters:
        data - event from audit.log file in a list of dictionary
        eventNumber - event number that needs to be queried
        module - Name of the module
        defaultFields - dictionary of default fields
        mandatoryFields - list is dictionary of mandatory fields
        mandatorySecLevel - list of dictionary containing 2nd level of mandatory fields
        optionalFields - list is dictionary of optional fields
        optionalSecLevel - list of dictionary containing 2nd level of optional fields
    Returns:
        Boolean - True if all field names match
    '''
    def validateFieldActualLog(self, data, eventNumber, module, defaultFields, mandatoryFields, manFieldSecLevel=None, optionalFields=None, optFieldSecLevel=None, method="Rest", n1ql_audit=False):
        flag = True
        for items in defaultFields:
            #log.info ("Default Value getting checked is - {0}".format(items))
            if items not in data:
                log.info (" Default value not matching with expected expected value is - {0}".format(items))
                flag = False
        for items in mandatoryFields:
            log.info ("Top Level Mandatory Field Default getting checked is - {0}".format(items))
            if items in data:
                if (isinstance ((data[items]), dict)):
                    for items1 in manFieldSecLevel:
                        tempStr = items1.split(":")
                        if tempStr[0] == items:
                            for items in data[items]:
                                #log.info ("Second Level Mandatory Field Default getting checked is - {0}".format(items))
                                if (items not in tempStr and method != 'REST'):
                                    #log.info (" Second level Mandatory field not matching with expected expected value is - {0}".format(items))
                                    flag = False
            elif 'common' in data and items in data['common']:
                if (isinstance ((data['common'][items]), dict)):
                    for items1 in manFieldSecLevel:
                        tempStr = items1.split(":")
                        if tempStr[0] == items:
                            for items in data['common'][items]:
                                #log.info ("Second Level Mandatory Field Default getting checked is - {0}".format(items))
                                if (items not in tempStr and method != 'REST'):
                                    #log.info (" Second level Mandatory field not matching with expected expected value is - {0}".format(items))
                                    flag = False
            else:
                flag = False
                if (method == 'REST' and items == 'sessionid'):
                    flag = True
                log.info (" Top level Mandatory field not matching with expected expected value is - {0}".format(items))
        for items in optionalFields:
            log.info ("Top Level Optional Field Default getting checked is - {0}".format(items))
            if items in data:
                if (isinstance ((data[items]), dict)):
                    for items1 in optFieldSecLevel:
                        tempStr = items1.split(":")
                        if tempStr[0] == items:
                            for items in data[items]:
                                #log.info ("Second Level Optional Field Default getting checked is - {0}".format(items))
                                if (items not in tempStr and method != 'REST'):
                                    log.info (" Second level Optional field not matching with expected expected value is - {0}".format(items))
                                    #flag = False
            else:
                #flag = False
                if (method == 'REST' and items == "sessionid"):
                    flag = True
                log.info (" Top level Optional field not matching with expected expected value is - {0}".format(items))
        if n1ql_audit:
            flag = True
        return flag

    '''
    validateData - validate data from audit.log with expected Result
    Parameters:
        data - event data from audit.log, based on eventID
        expectedResult - dictionary of expected Result to be validated
    Results:
        Boolean - True if data from audit.log matches with expectedResult
    '''

    def validateData(self, data, expectedResult, disable_hostname_verification=True):
        log.info (" Event from audit.log -- {0}".format(data))
        flag = True
        ignore = False
        for items in data:
            if items == 'timestamp':
                tempFlag = self.validateTimeStamp(data['timestamp'])
                if (tempFlag is False):
                    flag = False
            else:
                if (isinstance(data[items], dict)):
                    for seclevel in data[items]:
                        if (seclevel == 'port' and type(data[items][seclevel]) == str):
                            data[items][seclevel] = int(data[items][seclevel])
                        tempLevel = items + ":" + seclevel
                        if (tempLevel in list(expectedResult.keys())):
                            tempValue = expectedResult[tempLevel]
                        else:
                            if seclevel in list(expectedResult.keys()):
                                tempValue = expectedResult[seclevel]
                            else:
                                ignore = True
                                tempValue = data[items][seclevel]
                        if  (tempLevel == 'local:ip' and data[items][seclevel] == self.host.ip and ('ip' in expectedResult.keys())):
                            log.info ('Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(self.host.ip, data[items][seclevel], tempLevel))
                        elif tempLevel == 'local:port' and data[items][seclevel] == 8091 and ('port' in expectedResult.keys()):
                            log.info ('Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format('8091', data[items][seclevel], tempLevel))
                        elif tempLevel == 'remote:ip' and data[items][seclevel] == self.slaveAddress and ('ip' in expectedResult.keys()):
                            log.info ('Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(self.slaveAddress, data[items][seclevel], tempLevel))
                        elif (seclevel == 'port' and data[items][seclevel] >= 30000 and data[items][seclevel] <= 65535):
                            log.info ("Matching port is an ephemeral port -- actual port is {0}".format(data[items][seclevel]))
                        else:
                            if not ignore:
                                log.info('expected values - {0} -- actual value -- {1} - eventName - {2}'
                                         .format(tempValue, data[items][seclevel], seclevel))
                            if data[items][seclevel] != tempValue:
                                log.info('Mis-Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'
                                         .format(tempValue, data[items][seclevel], seclevel))
                                if tempLevel == 'local:ip' or tempLevel == 'remote:ip':
                                    if not disable_hostname_verification:
                                        flag = False
                                else:
                                    flag = False
                else:
                    if (items == 'port' and data[items] >= 30000 and data[items] <= 65535):
                        log.info ("Matching port is an ephemeral port -- actual port is {0}".format(data[items]))
                    elif (('ip' in expectedResult.keys()) or ('port' in expectedResult.keys())):
                        pass
                    else:
                        if items == "requestId" or items == 'clientContextId':
                            expectedResult[items] = data[items]
                        log.info ('expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items], data[items], items))
                        if (items == 'peername'):
                            if (expectedResult[items] not in data[items]):
                                flag = False
                                log.info ('Mis - Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items], data[items], items))
                        else:
                            if (data[items] != expectedResult[items]):
                                flag = False
                                log.info ('Mis - Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items], data[items], items))
            ignore = False
        return flag

    '''
    validateDate - validate date from audit.log and current date
    Parameters:
        actualDate - timestamp captured from audit.log for event
    Results:
        Boolean - True if difference of timestamp is < 30 seconds
    '''
    def validateTimeStamp(self, actualTime=None):
        try:
            date = actualTime[:10]
            hourMin = actualTime[11:16]
            tempTimeZone = actualTime[-6:]
            shell = RemoteMachineShellConnection(self.host)
            try:
                currDate = shell.execute_command('date +"%Y-%m-%d"')
                currHourMin = shell.execute_command('date +"%H:%M"')
                currTimeZone = shell.execute_command('date +%z')
            finally:
                shell.disconnect()
            log.info (" Matching expected date - currDate {0}; actual Date - {1}".format(currDate[0][0], date))
            log.info (" Matching expected time - currTime {0} ; actual Time - {1}".format(currHourMin[0][0], hourMin))
            if ((date != currDate[0][0])):
                log.info ("Mis-match in values for timestamp - date")
                return False
                #Compare time and minutes, will fail if time is 56 mins or above
            else:
                #log.info("-->{},{},{},{}".format(type(hourMin),hourMin,type(currHourMin),currHourMin))
                if ((int((hourMin.split(":"))[0])) != (int((currHourMin[0][0].split(":"))[0]))) or ((int((hourMin.split(":"))[1]) + 10) < (int((currHourMin[0][0].split(":"))[1]))):
                    log.info ("Mis-match in values for timestamp - time")
                    return False
                else:
                    tempTimeZone = tempTimeZone.replace(":", "")
                    if 'Z' in tempTimeZone:
                        tempTimeZone = "+0000"
                    if (tempTimeZone != currTimeZone[0][0]):
                        log.error("Mis-match in value of timezone; Expected: {0}  Actual: {1}".
                                  format(currTimeZone[0][0], tempTimeZone))
                        return False
        except Exception as e:
            log.info ("Value of exception is {0}".format(e))
            traceback.print_exc()
            return False


    '''
    validateEvents - external interface to validate event definition and value from audit.log
    Parameters:
        expectedResults - dictionary of keys as fields in audit.log and expected values for reach
    Returns:
        fieldVerification - Boolean - True if all matching fields have been found.
        valueVerification - Boolean - True if data matches with expected Results
    '''
    def validateEvents(self, expectedResults, disable_hostname_verification=True, n1ql_audit=False):
        defaultField, mandatoryFields, mandatorySecLevel, optionalFields, optionalSecLevel = self.returnFieldsDef(self.eventDef, self.eventID)
        actualEvent = self.returnEvent(self.eventID)
        fieldVerification = self.validateFieldActualLog(actualEvent, self.eventID, 'ns_server', self.defaultFields, mandatoryFields, \
                                                    mandatorySecLevel, optionalFields, optionalSecLevel, self.method, n1ql_audit)
        expectedResults = dict(list(defaultField.items()) + list(expectedResults.items()))
        valueVerification = self.validateData(actualEvent, expectedResults, disable_hostname_verification)
        return fieldVerification, valueVerification

    '''
    Make sure audit log is empty
    '''
    def validateEmpty(self):
        actualEvent = self.returnEvent(self.eventID, filtering=True)
        if actualEvent:
            return False, actualEvent
        else:
            return True, actualEvent

    def checkLastEvent(self):
        try:
            actualEvent = self.returnEvent(self.eventID)
            return self.validateTimeStamp(actualEvent['timestamp'])
        except:
            return False
