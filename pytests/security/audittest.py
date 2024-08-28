import json
import time
from threading import Thread, Event
from basetestcase import BaseTestCase
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import ReadDocumentException
from membase.api.exception import DesignDocCreationException
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
import testconstants
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH_RAW
from testconstants import MAC_COUCHBASE_BIN_PATH
from random import randint
from datetime import datetime
import subprocess
import logger
import urllib.request, urllib.parse, urllib.error
from security.auditmain import audit
from security.ldaptest import ldaptest
import socket

from pytests.security.x509_multiple_CA_util import x509main

log = logger.Logger.get_logger()
from ep_mc_bin_client import MemcachedClient


class auditTest(BaseTestCase):

    def setUp(self):
        super(auditTest, self).setUp()
        auditTemp = audit(host=self.master)
        self.disable_hostname_verification = self.input.param("disable_hostname_verification", True)
        try:
            if "ip6" in self.master.ip or self.master.ip.startswith("["):
                self.ipAddress = auditTemp.getLocalIPV6Address()
            else:
                self.ipAddress = auditTemp.getLocalIPAddress()
        except Exception as ex:
            log.info ('Exception while generating ip address {0}'.format(ex))
            self.ipAddress = '127.0.0.1'
        self.eventID = self.input.param('id', None)       
        currentState = auditTemp.getAuditStatus()
        self.log.info("Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
        if not currentState:
            self.log.info("Enabling Audit ")
            auditTemp.setAuditEnable('true')
            self.sleep(30)
        rest = RestConnection(self.master)
<<<<<<< HEAD   (85ac7b removed parition check from non rebalance test)
        self.setupLDAPSettings(rest)
=======
        #self.setupLDAPSettings(rest)
        param = {
            'hosts': '{0}'.format("172.23.120.175"),
            'port': '{0}'.format("389"),
            'encryption': '{0}'.format("None"),
            'bindDN': '{0}'.format("cn=admin,dc=couchbase,dc=com"),
            'bindPass': '{0}'.format("p@ssword"),
            'authenticationEnabled': '{0}'.format("true"),
            'userDNMapping': '{0}'.format('{"template":"cn=%u,ou=Users,dc=couchbase,dc=com"}')
        }
        rest.setup_ldap(param, '')
        # rbacmain().setup_auth_mechanism(self.servers,'ldap',rest)
        RbacBase().enable_ldap(rest)
>>>>>>> CHANGE (a57c88 Changing the LDAP config)

    def tearDown(self):
        super(auditTest, self).tearDown()

    def setupLDAPSettings (self, rest):
        api = rest.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({"enabled":'true',"admins":[],"roAdmins":[]})
        status, content, header = rest._http_request(api, 'POST', params)
        return status, content, header

    def set_user_role(self,rest,username,user_role='admin'):
        payload = "name=" + username + "&roles=" + user_role
        content = rest.set_user_roles(user_id=username, payload=payload)

    #Wrapper around auditmain
    def checkConfig(self, eventID, host, expectedResults, disable_hostname_verification=True, n1ql_audit=False):
        Audit = audit(eventID=self.eventID, host=host)
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults, disable_hostname_verification, n1ql_audit)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    #Check to make sure the audit code DOES NOT appear in the logs (for audit n1ql filtering)
    def checkFilter(self, eventID, host):
        Audit = audit(eventID=eventID, host=host)
        not_exists, entry = Audit.validateEmpty()
        self.assertTrue(not_exists, "There was an audit entry found. Audits for the code %s should not be logged. Here is the entry: %s" % (eventID, entry))


    #Tests to check for bucket events
    def test_bucketEvents(self):
        ops = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'
        rest = RestConnection(self.master)

        if (ops in ['create']):
            expectedResults = {'bucket_name': 'TestBucket', 'ram_quota': 268435456, 'num_replicas': 1,
                               'replica_index': False, 'eviction_policy': 'value_only', 'type': 'membase',
                               "autocompaction": 'false', "purge_interval": "undefined",
                               "flush_enabled": False, "num_threads": 3, "source": source,
                               "user": user, "local:ip": self.master.ip, "local:port": 8091, 'sessionid': '',
                               'conflict_resolution_type': 'seqno',
                               'storage_mode': self.bucket_storage, 'max_ttl': 400, 'compression_mode': 'passive',
                               'remote:ip': self.ipAddress}
            rest.create_bucket(bucket=expectedResults['bucket_name'],
                               ramQuotaMB=expectedResults['ram_quota'] // 1048576,
                               replicaNumber=expectedResults['num_replicas'],
                               proxyPort='11211', bucketType='membase', replica_index=0,
                               threadsNumber=expectedResults['num_threads'], flushEnabled=0, evictionPolicy='valueOnly',
                               maxTTL=expectedResults['max_ttl'],
                               storageBackend=self.bucket_storage)

        elif (ops in ['update']):
            expectedResults = {'bucket_name': 'TestBucket', 'ram_quota': 268435456, 'num_replicas': 1,
                               'replica_index': False, 'eviction_policy': 'value_only', 'type': 'membase',
                               "autocompaction": 'false', "purge_interval": "undefined", "flush_enabled": True,
                               "num_threads": 3, "source": source,
                               "user": user, "ip": self.ipAddress, "port": 57457, 'sessionid': '',
                               'storage_mode': self.bucket_storage, 'max_ttl': 400}
            rest.create_bucket(bucket=expectedResults['bucket_name'],
                               ramQuotaMB=expectedResults['ram_quota'] // 1048576,
                               replicaNumber=expectedResults['num_replicas'], proxyPort='11211', bucketType='membase',
                               replica_index=0, threadsNumber=expectedResults['num_threads'], flushEnabled=0,
                               evictionPolicy='valueOnly', maxTTL=expectedResults['max_ttl'],
                               storageBackend=self.bucket_storage)
            expectedResults = {'bucket_name': 'TestBucket', 'ram_quota': 268435456, 'num_replicas': 1,
                               'replica_index': True, 'eviction_policy': 'value_only', 'type': 'membase',
                               "autocompaction": 'false', "purge_interval": "undefined", "flush_enabled": True,
                               "num_threads": 3, "source": source,
                               "user": user, "ip": self.ipAddress, "port": 57457, 'storage_mode': self.bucket_storage,
                               'max_ttl': 200}
            rest.change_bucket_props(bucket=expectedResults['bucket_name'],
                                     ramQuotaMB=expectedResults['ram_quota'] // 1048576,
                                     replicaNumber=expectedResults['num_replicas'],
                                     proxyPort='11211', replicaIndex=1,
                                     maxTTL=expectedResults['max_ttl'])

        elif (ops in ['delete']):
            expectedResults = {'bucket_name': 'TestBucket', 'ram_quota': 268435456, 'num_replicas': 1,
                               'replica_index': True, 'eviction_policy': 'value_only', 'type': 'membase',
                               "autocompaction": 'false', "purge_interval": "undefined", "flush_enabled": False,
                               "num_threads": 3, "source": source,
                               "user": user, "ip": self.ipAddress, "port": 57457}
            rest.create_bucket(bucket=expectedResults['bucket_name'],
                               ramQuotaMB=expectedResults['ram_quota'] // 1048576,
                               replicaNumber=expectedResults['num_replicas'],
                               proxyPort='11211', bucketType='membase', replica_index=1,
                               threadsNumber=expectedResults['num_threads'], flushEnabled=0, evictionPolicy='valueOnly',
                               storageBackend=self.bucket_storage)
            rest.delete_bucket(expectedResults['bucket_name'])

        elif (ops in ['flush']):
            expectedResults = {'bucket_name': 'TestBucket', 'ram_quota': 256, 'num_replicas': 1, 'replica_index': True,
                               'eviction_policy': 'value_only', 'type': 'membase',
                               "autocompaction": 'false', "purge_interval": "undefined", "flush_enabled": True,
                               "num_threads": 3, "source": source,
                               "user": user, "ip": self.ipAddress, "port": 57457, 'storage_mode': self.bucket_storage}
            rest.create_bucket(bucket=expectedResults['bucket_name'], ramQuotaMB=expectedResults['ram_quota'],
                               replicaNumber=expectedResults['num_replicas'],
                               proxyPort='11211', bucketType='membase', replica_index=1,
                               threadsNumber=expectedResults['num_threads'], flushEnabled=1, evictionPolicy='valueOnly',
                               storageBackend=self.bucket_storage)
            self.sleep(10)
            rest.flush_bucket(expectedResults['bucket_name'])

        self.checkConfig(self.eventID, self.master, expectedResults, self.disable_hostname_verification)

    def test_bucket_select_audit(self):
        # security.audittest.auditTest.test_bucket_select_audit,default_bucket=false,id=20492
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='TestBucket', ramQuotaMB=100,
                           storageBackend=self.bucket_storage)
        time.sleep(30)
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('TestBucket')

        expectedResults = {"bucket":"TestBucket","description":"The specified bucket was selected","id":self.eventID,"name":"select bucket" \
                           ,"peername":"127.0.0.1:46539","real_userid":{"domain":"memcached","user":"@ns_server"},"sockname":"127.0.0.1:11209"}
        Audit = audit(eventID=self.eventID, host=self.master)
        actualEvent = Audit.returnEvent(self.eventID)
        Audit.validateData(actualEvent, expectedResults)



    def test_clusterOps(self):
        Audit = audit(eventID=self.eventID, host=self.master)
        ops = self.input.param('ops', None)
        servs_inout = self.servers[1:self.nodes_in + 1]
        source = 'ns_server'

        if (ops in ['addNodeKV']):
            self.cluster.rebalance(self.servers, servs_inout, [])
            print(servs_inout)
            print(servs_inout[0].ip)
            expectedResults = {"services":['kv'], 'port':8091, 'hostname':servs_inout[0].ip,
                               'groupUUID':"0", 'node':'ns_1@' + servs_inout[0].ip, 'source':source,
                               'user':self.master.rest_username, "ip":self.ipAddress, "remote:port":57457}

        if (ops in ['addNodeN1QL']):
           rest = RestConnection(self.master)
           rest.add_node(user=self.master.rest_username, password=self.master.rest_password, remoteIp=servs_inout[0].ip, services=['n1ql'])
           expectedResults = {"services":['n1ql'], 'port':8091, 'hostname':servs_inout[0].ip,
                               'groupUUID':"0", 'node':'ns_1@' + servs_inout[0].ip, 'source':source,
                               'user':self.master.rest_username, "ip":self.ipAddress, "remote:port":57457}

        if (ops in ['addNodeIndex']):
           rest = RestConnection(self.master)
           rest.add_node(user=self.master.rest_username, password=self.master.rest_password, remoteIp=servs_inout[0].ip, services=['index'])
           expectedResults = {"services":['index'], 'port':8091, 'hostname':servs_inout[0].ip,
                               'groupUUID':"0", 'node':'ns_1@' + servs_inout[0].ip, 'source':source,
                               'user':self.master.rest_username, "ip":self.ipAddress, "remote:port":57457}

        if (ops in ['removeNode']):
            self.cluster.rebalance(self.servers, [], servs_inout)
            shell = RemoteMachineShellConnection(self.master)
            os_type = shell.extract_remote_info().distribution_type
            log.info ("OS type is {0}".format(os_type))
            if os_type == 'windows':
                expectedResults = {"delta_recovery_buckets":"all", 'known_nodes':["ns_1@" + servs_inout[0].ip, "ns_1@" + self.master.ip], 'ejected_nodes':['ns_1@' + servs_inout[0].ip], 'source':'ns_server', \
                               'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}
            else:
                expectedResults = {"delta_recovery_buckets":"all", 'known_nodes':["ns_1@" + servs_inout[0].ip, "ns_1@" + self.master.ip], 'ejected_nodes':['ns_1@' + servs_inout[0].ip], 'source':'ns_server', \
                               'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}


        if (ops in ['rebalanceIn']):
            self.cluster.rebalance(self.servers, servs_inout, [])
            shell = RemoteMachineShellConnection(self.master)
            os_type = shell.extract_remote_info().distribution_type
            log.info ("OS type is {0}".format(os_type))
            if os_type == 'windows':
                expectedResults = {"delta_recovery_buckets":"all", 'known_nodes':["ns_1@" + servs_inout[0].ip, "ns_1@" + self.master.ip], 'ejected_nodes':[], 'source':'ns_server', \
                                'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}
            else:
                expectedResults = {"delta_recovery_buckets":"all", 'known_nodes':["ns_1@" + servs_inout[0].ip, "ns_1@" + self.master.ip], 'ejected_nodes':[], 'source':'ns_server', \
                                'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}

        if (ops in ['rebalanceOut']):
            self.cluster.rebalance(self.servers, [], servs_inout)
            shell = RemoteMachineShellConnection(self.master)
            os_type = shell.extract_remote_info().distribution_type
            log.info ("OS type is {0}".format(os_type))
            if os_type == 'windows':
                expectedResults = {"delta_recovery_buckets":"all", 'known_nodes':["ns_1@" + servs_inout[0].ip, "ns_1@" + self.master.ip], 'ejected_nodes':['ns_1@' + servs_inout[0].ip], 'source':'ns_server', \
                               'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}
            else:
                expectedResults = {"delta_recovery_buckets":"all", 'known_nodes':["ns_1@" + servs_inout[0].ip, "ns_1@" + self.master.ip], 'ejected_nodes':['ns_1@' + servs_inout[0].ip], 'source':'ns_server', \
                               'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}

        if (ops in ['failover']):
            type = self.input.param('type', None)
            self.cluster.failover(self.servers, servs_inout)
            self.cluster.rebalance(self.servers, [], [])
            expectedResults = {'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457, 'type':type, 'nodes':'[ns_1@' + servs_inout[0].ip + ']'}

        if (ops == 'nodeRecovery'):
            expectedResults = {'node':'ns_1@' + servs_inout[0].ip, 'type':'delta', 'source':source, 'user':self.master.rest_username, "ip":self.ipAddress, "port":57457}
            self.cluster.failover(self.servers, servs_inout)
            rest = RestConnection(self.master)
            rest.set_recovery_type(expectedResults['node'], 'delta')

        # Pending of failover - soft
        self.checkConfig(self.eventID, self.master, expectedResults)


    def test_settingsCluster(self):
        ops = self.input.param("ops", None)
        source = 'ns_server'
        user = self.master.rest_username
        password = self.master.rest_password
        rest = RestConnection(self.master)

        if (ops == 'memoryQuota'):
            expectedResults = {'memory_quota':512, 'source':source, 'user':user, 'ip':self.ipAddress, 'port':12345, 'cluster_name':'', 'index_memory_quota':512,'fts_memory_quota': 302}
            rest.init_cluster_memoryQuota(expectedResults['user'], password, expectedResults['memory_quota'])

        elif (ops == 'loadSample'):
            expectedResults = {'name':'gamesim-sample', 'source':source, "user":user, 'ip':self.ipAddress, 'port':12345}
            rest.addSamples()
            #Get a REST Command for loading sample

        elif (ops == 'enableAutoFailover'):
            expectedResults = {'max_nodes':1, "timeout":120, 'source':source, "user":user, 'ip':self.ipAddress, 'port':12345,'failover_server_group':False}
            rest.update_autofailover_settings(True, expectedResults['timeout'])

        elif (ops == 'disableAutoFailover'):
            expectedResults = {'max_nodes':1, "timeout":120, 'source':source, "user":user, 'ip':self.ipAddress, 'port':12345}
            rest.update_autofailover_settings(False, expectedResults['timeout'])

        elif (ops == 'resetAutoFailover'):
            expectedResults = {'source':source, "user":user, 'ip':self.ipAddress, 'port':12345}
            rest.reset_autofailover()

        elif (ops == 'enableClusterAlerts'):
            expectedResults = {"encrypt":False, "email_server:port":25, "host":"localhost", "email_server:user":"ritam", "alerts":["auto_failover_node", "auto_failover_maximum_reached"], \
                             "recipients":["ritam@couchbase.com"], "sender":"admin@couchbase.com", "source":"ns_server", "user":"Administrator", 'ip':self.ipAddress, 'port':1234}
            rest.set_alerts_settings('ritam@couchbase.com', 'admin@couchbase.com', 'ritam', 'password',)

        elif (ops == 'disableClusterAlerts'):
            rest.set_alerts_settings('ritam@couchbase.com', 'admin@couchbase.com', 'ritam', 'password',)
            expectedResults = {'source':source, "user":user, 'ip':self.ipAddress, 'port':1234}
            rest.disable_alerts()

        elif (ops == 'modifyCompactionSettingsPercentage'):
            expectedResults = {"parallel_db_and_view_compaction":False,
                               "database_fragmentation_threshold:percentage":50,
                               "view_fragmentation_threshold:percentage":50,
                               "purge_interval":3,
                               "source":"ns_server",
                               "user":"Administrator",
                               'source':source,
                               "user":user,
                               'ip':self.ipAddress,
                               'port':1234}
            rest.set_auto_compaction(dbFragmentThresholdPercentage=50, viewFragmntThresholdPercentage=50)

        elif (ops == 'modifyCompactionSettingsPercentSize'):
            expectedResults = {"parallel_db_and_view_compaction":False,
                               "database_fragmentation_threshold:percentage":50,
                               "database_fragmentation_threshold:size":10,
                               "view_fragmentation_threshold:percentage":50,
                               "view_fragmentation_threshold:size":10,
                               "purge_interval":3,
                               "source":"ns_server",
                               "user":"Administrator",
                               'source':source,
                               "user":user,
                               'ip':self.ipAddress,
                               'port':1234}
            rest.set_auto_compaction(dbFragmentThresholdPercentage=50,
                                     viewFragmntThresholdPercentage=50,
                                     dbFragmentThreshold=10,
                                     viewFragmntThreshold=10)

        elif (ops == 'modifyCompactionSettingsTime'):
            expectedResults = {"parallel_db_and_view_compaction": False,
                               "database_fragmentation_threshold:percentage": 50,
                               "database_fragmentation_threshold:size": 10,
                               "view_fragmentation_threshold:percentage": 50,
                               "view_fragmentation_threshold:size": 10,
                               "allowed_time_period:abort_outside": True,
                               "allowed_time_period:to_minute": 15,
                               "allowed_time_period:from_minute": 12,
                               "allowed_time_period:to_hour": 1,
                               "allowed_time_period:from_hour": 1,
                               "purge_interval": 3,
                               "source": "ns_server",
                               "user": "Administrator",
                               'source': source,
                               "user": user,
                               'ip': self.ipAddress,
                               'port': 1234,
                               }
            rest.set_auto_compaction(dbFragmentThresholdPercentage=50,
                                     viewFragmntThresholdPercentage=50,
                                     dbFragmentThreshold=10,
                                     viewFragmntThreshold=10,
                                     allowedTimePeriodFromHour=1,
                                     allowedTimePeriodFromMin=12,
                                     allowedTimePeriodToHour=1,
                                     allowedTimePeriodToMin=15,
                                     allowedTimePeriodAbort='true')

        elif (ops == "AddGroup"):
            expectedResults = {'group_name':'add group', 'source':source, 'user':user, 'ip':self.ipAddress, 'port':1234}
            rest.add_zone(expectedResults['group_name'])
            tempStr = rest.get_zone_uri()[expectedResults['group_name']]
            tempStr = (tempStr.split("/"))[4]
            expectedResults['uuid'] = tempStr

        elif (ops == "UpdateGroup"):
            expectedResults = {'group_name':'upGroup', 'source':source, 'user':user, 'ip':self.ipAddress, 'port':1234, 'nodes':[]}
            rest.add_zone(expectedResults['group_name'])
            rest.rename_zone(expectedResults['group_name'], 'update group')
            expectedResults['group_name'] = 'update group'
            tempStr = rest.get_zone_uri()[expectedResults['group_name']]
            tempStr = (tempStr.split("/"))[4]
            expectedResults['uuid'] = tempStr

        elif (ops == "UpdateGroupAddNodes"):
            sourceGroup = "Group 1"
            destGroup = 'destGroup'
            expectedResults = {'group_name':destGroup, 'source':source, 'user':user, 'ip':self.ipAddress, 'port':1234, 'nodes':['ns_1@' + self.master.ip], 'port':1234}
            #rest.add_zone(sourceGroup)
            rest.add_zone(destGroup)
            self.sleep(30)
            rest.shuffle_nodes_in_zones([self.master.ip], sourceGroup, destGroup)
            tempStr = rest.get_zone_uri()[expectedResults['group_name']]
            tempStr = (tempStr.split("/"))[4]
            expectedResults['uuid'] = tempStr

        elif (ops == "DeleteGroup"):
            expectedResults = {'group_name':'delete group', 'source':source, 'user':user, 'ip':self.ipAddress, 'port':1234}
            rest.add_zone(expectedResults['group_name'])
            tempStr = rest.get_zone_uri()[expectedResults['group_name']]
            tempStr = (tempStr.split("/"))[4]
            expectedResults['uuid'] = tempStr
            rest.delete_zone(expectedResults['group_name'])

        elif (ops == "regenCer"):
            expectedResults = {'source':source, 'user':user, 'ip':self.ipAddress, 'port':1234}
            rest.regenerate_cluster_certificate()

        elif (ops == 'renameNode'):
            rest.rename_node(self.master.ip, user, password)
            expectedResults = {"hostname":self.master.ip, "node":"ns_1@" + self.master.ip, "source":source, "user":user, "ip":self.ipAddress, "port":56845}

        try:
            self.checkConfig(self.eventID, self.master, expectedResults)
        finally:
            if (ops == "UpdateGroupAddNodes"):
                sourceGroup = "Group 1"
                destGroup = 'destGroup'
                rest.shuffle_nodes_in_zones([self.master.ip], destGroup, sourceGroup)

            rest = RestConnection(self.master)
            zones = rest.get_zone_names()
            for zone in zones:
                if zone != "Group 1":
                    rest.delete_zone(zone)



    def test_cbDiskConf(self):
        ops = self.input.param('ops', None)
        source = 'ns_server'
        user = self.master.rest_username
        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)
        os_type = shell.extract_remote_info().distribution_type
        if (os_type == 'Windows'):
          currentPath = "c:/Program Files/Couchbase/Server/var/lib/couchbase/data"
          newPath = "C:/tmp"
        else:
          currentPath = '/opt/couchbase/var/lib/couchbase/data'
          newPath = "/tmp"

        if (ops == 'indexPath'):
            try:
                expectedResults = {'node': 'ns_1@' + self.master.ip, 'source':source,
                                'user':user, 'ip':self.ipAddress, 'port':1234,
                                'index_path':newPath, 'db_path':currentPath,
                                'cbas_dirs':currentPath}

                rest.set_data_path(index_path=newPath)
                self.checkConfig(self.eventID, self.master, expectedResults)
            finally:
                rest.set_data_path(index_path=currentPath)


    def test_loginEvents(self):
        ops = self.input.param("ops", None)
        role = self.input.param("role", None)
        username = self.input.param('username', None)
        password = self.input.param('password', None)
        source = 'ns_server'
        rest = RestConnection(self.master)
        user = self.master.rest_username
        roles = []
        roles.append(role)

        if (ops in ['loginRoAdmin', 'deleteuser', 'passwordChange']):
                rest.create_ro_user(username, password)

        if (ops in ['loginAdmin', 'loginRoAdmin']):
            status, content = rest.validateLogin(username, password, True, getContent=True)
            sessionID = (((status['set-cookie']).split("="))[1]).split(";")[0]
            expectedResults = {'source':source, 'user':username, 'password':password, 'roles':roles, 'ip':self.ipAddress, "port":123456, 'sessionid':sessionID}

        elif (ops in ['deleteuser']):
            expectedResults = {"role":role, "real_userid:source":source, 'real_userid:user':user,
                               'ip':self.ipAddress, "port":123456, 'userid':username,'identity:source':'ro_admin','identity:user':'roAdmins'}
            rest.delete_ro_user()

        elif (ops in ['passwordChange']):
            expectedResults = {'real_userid:source':source, 'real_userid:user':user,
                               'password':password, 'role':role, 'ip':self.ipAddress, "port":123456,
                               'userid':username,'identity:source':'ro_admin','identity:user':'roAdmins'}
            rest.changePass_ro_user(username, password)

        elif (ops in ['invalidlogin']):
            status, content = rest.validateLogin(username, password, True, getContent=True)
            expectedResults = {'real_userid':username, 'password':password,
                               'ip':self.ipAddress, "port":123456,'source': 'rejected','user':username}

        # User must be pre-created in LDAP in advance
        elif ops in ['ldapLogin']:
            rest = RestConnection(self.master)
            self.set_user_role(rest, username)
            param = {
                'hosts': '{0}'.format("172.23.120.175"),
                'port': '{0}'.format("389"),
                'encryption': '{0}'.format("None"),
                'bindDN': '{0}'.format("cn=admin,dc=couchbase,dc=com"),
                'bindPass': '{0}'.format("p@ssword"),
                'authenticationEnabled': '{0}'.format("true"),
                'userDNMapping': '{0}'.format('{"template":"cn=%u,ou=Users,dc=couchbase,dc=com"}')
            }
            rest.setup_ldap(param, '')
            status, content = rest.validateLogin(username, password, True, getContent=True)
            sessionID = (((status['set-cookie']).split("="))[1]).split(";")[0]
            expectedResults = {'source':'external', 'user':username, 'password':password, 'roles':roles, 'ip':self.ipAddress, "port":123456, 'sessionid':sessionID}

        self.checkConfig(self.eventID, self.master, expectedResults)


    def test_checkCreateBucketCluster(self):
        ops = self.input.param("ops", None)
        source = 'ns_server'
        #auditTemp = audit(host=self.master)
        #auditTemp.setAuditEnable('true')
        for server in self.servers:
            user = server.rest_username
            rest = RestConnection(server)
            if (ops in ['create']):
                expectedResults = {'bucket_name':'TestBucket' + server.ip, 'ram_quota':104857600, 'num_replicas':1,
                                   'replica_index':False, 'eviction_policy':'value_only', 'type':'membase', \
                                   'auth_type':'sasl', "autocompaction":'false', "purge_interval":"undefined", \
                                    "flush_enabled":False, "num_threads":3, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':'', 'conflict_resolution_type':'seqno'}
                rest.create_bucket(expectedResults['bucket_name'], expectedResults['ram_quota'] // 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                   '11211', 'membase', 0, expectedResults['num_threads'], 0, 'valueOnly',
                                   storageBackend=self.bucket_storage)
                self.log.info ("value of server is {0}".format(server))
                self.checkConfig(self.eventID, server, expectedResults)

    def test_createBucketClusterNodeOut(self):
        ops = self.input.param("ops", None)
        nodesOut = self.input.param("nodes_out", 1)
        source = 'ns_server'
        user = self.master.rest_username

        firstNode = self.servers[0]
        secondNode = self.servers[1]
        auditFirstNode = audit(host=firstNode)
        auditFirstNode.setAuditEnable('true')
        auditSecondNode = audit(host=secondNode)

        origState = auditFirstNode.getAuditStatus()
        origLogPath = auditFirstNode.getAuditLogPath()
        origRotateInterval = auditFirstNode.getAuditRotateInterval()

        #Remove the node from cluster & check if there are any change to cluster
        self.cluster.rebalance(self.servers, [], self.servers[1:nodesOut + 1])
        self.assertEqual(auditFirstNode.getAuditStatus(), origState, "Issues with audit state after removing node")
        self.assertEqual(auditFirstNode.getAuditLogPath(), origLogPath, "Issues with audit log path after removing node")
        self.assertEqual(auditFirstNode.getAuditRotateInterval(), origRotateInterval, "Issues with audit rotate interval after removing node")

        restFirstNode = RestConnection(firstNode)
        if (ops in ['create']):
            expectedResults = {'bucket_name':'TestBucketRemNode', 'ram_quota':104857600, 'num_replicas':0,
                                'replica_index':False, 'eviction_policy':'value_only', 'type':'membase', \
                                'auth_type':'sasl', "autocompaction":'false', "purge_interval":"undefined", \
                                "flush_enabled":False, "num_threads":3, "source":source, \
                                "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':'', 'conflict_resolution_type':'seqno'}
            restFirstNode.create_bucket(expectedResults['bucket_name'], expectedResults['ram_quota'] // 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                '11211', 'membase', 0, expectedResults['num_threads'], 0, 'valueOnly',
                                storageBackend=self.bucket_storage)

            self.checkConfig(self.eventID, firstNode, expectedResults)

        #Add back the Node in Cluster
        self.cluster.rebalance(self.servers, self.servers[1:nodesOut + 1], [])
        self.assertEqual(auditSecondNode.getAuditStatus(), origState, "Issues with audit state after adding node")
        self.assertEqual(auditSecondNode.getAuditLogPath(), origLogPath, "Issues with audit log path after adding node")
        self.assertEqual(auditSecondNode.getAuditRotateInterval(), origRotateInterval, "Issues with audit rotate interval after adding node")

        for server in self.servers:
            user = server.rest_username
            rest = RestConnection(server)
            if (ops in ['create']):
                expectedResults = {'bucket_name':'TestBucket' + server.ip, 'ram_quota':104857600, 'num_replicas':1,
                                   'replica_index':False, 'eviction_policy':'value_only', 'type':'membase', \
                                   'auth_type':'sasl', "autocompaction":'false', "purge_interval":"undefined", \
                                    "flush_enabled":False, "num_threads":3, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':'', 'conflict_resolution_type':'seqno'}
                rest.create_bucket(expectedResults['bucket_name'], expectedResults['ram_quota'] // 1048576, expectedResults['auth_type'], 'password', expectedResults['num_replicas'], \
                                   '11211', 'membase', 0, expectedResults['num_threads'], 0, 'valueOnly',
                                   storageBackend=self.bucket_storage)

                self.checkConfig(self.eventID, server, expectedResults)


    def test_Backup(self):
         shell = RemoteMachineShellConnection(self.master)
         gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=100)
         self._load_all_buckets(self.master, gen_update, "create", 0, 1, 0, True, batch_size=20000,
                                                                        pause_secs=5, timeout_secs=180)
         self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
         info = shell.extract_remote_info()
         path = '/tmp/backup'
         #if info.type.lower() == "windows":
             #path = 'c:' + path
         shell.delete_files(path)
         create_dir = "mkdir " + path
         shell.execute_command(create_dir)
         shell.execute_cluster_backup(backup_location=path)
         expectedResults = {"peername":self.master.ip, "sockname":self.master.ip + ":11210", "source":"memcached", "user":"default", 'bucket':'default'}
         self.checkConfig(self.eventID, self.master, expectedResults)

    def test_Transfer(self):
         shell = RemoteMachineShellConnection(self.master)
         gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=100)
         self._load_all_buckets(self.master, gen_update, "create", 0, 1, 0, True, batch_size=20000,
                                                                        pause_secs=5, timeout_secs=180)
         self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
         source = "http://" + self.master.ip + ":8091"
         info = shell.extract_remote_info()
         path = '/tmp/backup'
         #if info.type.lower() == "windows":
         #    path = '/cygdrive/c' + path
         shell.delete_files(path)
         create_dir = "mkdir " + path
         shell.execute_command(create_dir)
         options = "-b default " + " -u " + self.master.rest_username + " -p " + self.master.rest_password
         shell.execute_cbtransfer(source, path, options)
         expectedResults = {"peername":self.master.ip, "sockname":self.master.ip + ":11210", "source":"memcached", "user":"default", 'bucket':'default'}
         self.checkConfig(self.eventID, self.master, expectedResults)

    #Need an implementation for cbreset_password
    def test_resetPass(self):
        shell = RemoteMachineShellConnection(self.master)
        info = shell.extract_remote_info()
        if info.type.lower() == "windows":
            command = "%scbreset_password.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH_RAW)
        else:
            command = "%scbreset_password" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        shell.delete_files(path)

    def test_AuthFailMemcache(self):
        shell = RemoteMachineShellConnection(self.master)
        os_type = shell.extract_remote_info().distribution_type
        log.info ("OS type is {0}".format(os_type))
        if os_type == 'windows':
     	     command = "%smcstat.exe" % (testconstants.WIN_COUCHBASE_BIN_PATH_RAW)
        else:
             command = "%smcstat" % (testconstants.LINUX_COUCHBASE_BIN_PATH)

        command = command + " -u foo -P bar"
        shell.execute_command(command)
        expectedResults = {"peername":'127.0.0.1', "sockname":'127.0.0.1' + ":11210", "source":"memcached", "user":"foo", "reason":"Unknown user"}
        self.checkConfig(self.eventID, self.master, expectedResults)


    def test_addLdapAdminRO(self):
        ops = self.input.param("ops", None)
        role = self.input.param("role", None)
        adminUser = self.input.param('adminUser', None)
        roAdminUser = self.input.param('roAdminUser', None)
        default = self.input.param('default', None)
        source = 'ns_server'
        rest = RestConnection(self.master)
        user = self.master.rest_username

        #User must be pre-created in LDAP in advance
        if (ops in ['ldapAdmin']) and (default is not None):
            rest.ldapUserRestOperation(True, adminUser=[[adminUser]], exclude='roAdmin')
            expectedResults = {"ro_admins":'default', "admins":[adminUser], "enabled":True, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':''}
        elif (ops in ['ldapROAdmin']) and (default is not None):
            rest.ldapUserRestOperation(True, ROadminUser=[[roAdminUser]], exclude='fullAdmin')
            expectedResults = {"admins":'default', "ro_admins":[roAdminUser], "enabled":True, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':''}
        elif (ops in ['ldapAdmin']):
            rest.ldapUserRestOperation(True, adminUser=[[adminUser]], exclude=None)
            expectedResults = {"ro_admins":[], "admins":[adminUser], "enabled":True, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':''}
        elif (ops in ['ldapROAdmin']):
            rest.ldapUserRestOperation(True, ROadminUser=[[roAdminUser]], exclude=None)
            expectedResults = {"admins":[], "ro_admins":[roAdminUser], "enabled":True, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':''}
        elif (ops in ['Both']):
            rest.ldapUserRestOperation(True, ROadminUser=[[roAdminUser]], adminUser=[[adminUser]], exclude=None)
            expectedResults = {"admins":[adminUser], "ro_admins":[roAdminUser], "enabled":True, "source":source, \
                                   "user":user, "ip":self.ipAddress, "port":57457, 'sessionid':''}

        self.checkConfig(self.eventID, self.master, expectedResults)



    def test_internalSettingsXDCR(self):
        ops = self.input.param("ops", None)
        value = self.input.param("value", None)
        rest = RestConnection(self.master)
        user = self.master.rest_username
        source = 'ns_server'
        input = self.input.param("input", None)

        replications = rest.get_replications()
        for repl in replications:
            src_bucket = repl.get_src_bucket()
            dst_bucket = repl.get_dst_bucket()
            rest.set_xdcr_param(src_bucket.name, dst_bucket.name, input, value)
        expectedResults = {"user":user, "local_cluster_name":self.master.ip+":8091", ops:value,
                               "source":source}

        self.checkConfig(self.eventID, self.master, expectedResults)

    def test_internalSettingLocal(self):
        ops = self.input.param("ops", None)
        if ":" in ops:
            ops = ops.replace(":", ",")
            ops = '{' + ops + '}'
        value = self.input.param("value", None)
        rest = RestConnection(self.master)
        user = self.master.rest_username
        source = 'ns_server'
        input = self.input.param("input", None)


        rest.set_internalSetting(input, value)
        expectedResults = {"user":user, ops:value,"source":source,"ip":self.ipAddress, "port":57457}

        self.checkConfig(self.eventID, self.master, expectedResults)

    def test_multiple_CA(self):
        ops = self.input.param("ops", None)
        self.x509 = x509main(host=self.master)
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        if ops == "load_cluster_CA":
            self.x509.upload_root_certs(server=self.master, root_ca_names=["clientroot"])
            content = self.x509.get_trusted_CAs(server=self.master)
            for ca_dict in content:
                subject = ca_dict["subject"]
                root_ca_name = subject.split("CN=")[1]
                if root_ca_name == "clientroot":
                    expires = ca_dict["notAfter"]
            expectedResults = {"description": "Upload cluster CA",
                               "expires": expires,
                               "local": {"ip": self.master.ip, "port": 8091}, "name": "upload cluster ca",
                               "real_userid": {"domain": "builtin", "user": "Administrator"},
                               "remote": {"ip": self.master.ip, "port": 35510},
                               "subject": "C=UA, O=MyCompany, CN=clientroot"}
        elif ops == "delete_cluster_CA":
            self.x509.upload_root_certs(server=self.master, root_ca_names=["clientroot"])
            ids = self.x509.get_ids_from_ca_names(ca_names=["clientroot"])
            self.x509.delete_trusted_CAs(ids=ids)
            expectedResults = {"description": "Delete cluster CA",
                               "local": {"ip": self.master.ip, "port": 8091}, "name": "delete cluster ca",
                               "real_userid": {"domain": "builtin", "user": "Administrator"},
                               "remote": {"ip": self.master.ip, "port": 62993},
                               "subject": "C=UA, O=MyCompany, CN=clientroot"}
        self.checkConfig(self.eventID, self.master, expectedResults)
        self.x509.teardown_certs(servers=self.servers)




