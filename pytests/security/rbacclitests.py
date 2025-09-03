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
from security.rbac_base import RbacBase
import socket
import random
import zlib
import subprocess
import urllib.request, urllib.parse, urllib.error


class rbacclitests(BaseTestCase):
    def setUp(self):
        self.times_teardown_called = 1
        super(rbacclitests, self).setUp()
        self.r = random.Random()
        self.vbucket_count = 1024
        self.shell = RemoteMachineShellConnection(self.master)
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        self.excluded_commands = self.input.param("excluded_commands", None)
        self.os = 'linux'
        self.cli_command_path = LINUX_COUCHBASE_BIN_PATH
        if type == 'windows':
            self.os = 'windows'
            self.cli_command_path = WIN_COUCHBASE_BIN_PATH
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'
            self.cli_command_path = MAC_COUCHBASE_BIN_PATH
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
        self.cli_command = self.input.param("cli_command", None)
        self.command_options = self.input.param("command_options", None)
        if self.command_options is not None:
            self.command_options = self.command_options.split(";")
        TestInputSingleton.input.test_params["default_bucket"] = False
        self.eventID = self.input.param('id', None)
        AuditTemp = audit(host=self.master)
        self.ipAddress = self.getLocalIPAddress()
        self.ldapUser = self.input.param('ldapUser', 'Administrator')
        self.ldapPass = self.input.param('ldapPass', 'password')
        self.source = self.input.param('source', 'ns_server')
        self.role = self.input.param('role', 'admin')
        if self.role in ['bucket_admin', 'views_admin']:
            self.role = self.role + "[*]"
        self.log.info (" value of self.role is {0}".format(self.role))
        if type == 'windows' and self.source == 'saslauthd':
            raise Exception(" Ldap Tests cannot run on windows");
        else:
            if self.source == 'saslauthd':
                rest = RestConnection(self.master)
                self.setupLDAPSettings(rest)
                #rest.ldapUserRestOperation(True, [[self.ldapUser]], exclude=None)
                self.set_user_role(rest, self.ldapUser, user_role=self.role)
        rest = RestConnection(self.master)
        param = {
            'hosts': '{0}'.format("172.23.124.20"),
            'port': '{0}'.format("389"),
            'encryption': '{0}'.format("None"),
            'bindDN': '{0}'.format("cn=admin,dc=couchbase,dc=com"),
            'bindPass': '{0}'.format("p@ssw0rd"),
            'authenticationEnabled': '{0}'.format("true"),
            'userDNMapping': '{0}'.format('{"template":"cn=%u,ou=Users,dc=couchbase,dc=com"}')
        }
        rest.setup_ldap(param, '')


    def tearDown(self):
        super(rbacclitests, self).tearDown()

    def getLocalIPAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        port = 80
        host_ip = socket.gethostbyname('www.couchbase.com')
        s.connect((host_ip,port))
        return s.getsockname()[0]
        '''
        status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        return ipAddress
        '''

    def setupLDAPSettings (self,rest):
        api = rest.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({"enabled":'true',"admins":[],"roAdmins":[]})
        status, content, header = rest._http_request(api, 'POST', params)
        return status, content, header

    def del_runCmd_value(self, output):
        if "runCmd" in output[0]:
            output = output[1:]
        return output

    def set_user_role(self,rest,username,user_role='admin'):
        payload = "name=" + username + "&roles=" + user_role
        content =  rest.set_user_roles(user_id=username, payload=payload)

    def _validate_roles(self, output, result):
        final_result = True
        for outputs in output:
            if result not in outputs:
                final_result = False
            else:
                final_result = True
        self.assertTrue(final_result, "Incorrect Message for the role")

    #Wrapper around auditmain
    def checkConfig(self, eventID, host, expectedResults):
        Audit = audit(eventID=eventID, host=host)
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    def _create_bucket(self, remote_client, bucket="default", bucket_type="couchbase", bucket_port=11211,
                        bucket_ramsize=200, bucket_replica=1, wait=False, enable_flush=None, enable_index_replica=None, \
                       user=None,password=None):
        options = "--bucket={0} --bucket-type={1} --bucket-ramsize={2} --bucket-replica={3}".\
            format(bucket, bucket_type, bucket_ramsize, bucket_replica)
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica), "")[enable_index_replica is None]
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --wait", "")[wait]
        cli_command = "bucket-create"
        if user is None:
            user = self.ldapUser

        if password is None:
            password = self.ldapPass

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=user, password=password)
        return output

    def testClusterEdit(self):
        options = "--server-add=http://{0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_couchbase_cli(cli_command='cluster-edit', options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)

    def testAddRemoveNodes(self):
        if self.role in ['replication_admin','views_admin[*]','bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin','cluster_admin']:
            result = 'SUCCESS'
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        force_failover = self.input.param("force_failover", False)
        nodes_readd = self.input.param("nodes_readd", 0)
        cli_command = self.input.param("cli_command", None)
        source = self.source
        remote_client = RemoteMachineShellConnection(self.master)
        for num in range(nodes_add):
            options = "--server-add=http://{0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command='server-add', options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        output, error = remote_client.execute_couchbase_cli(cli_command='rebalance', cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)

        if (cli_command == 'server-remove'):
            for num in range(nodes_rem):
                cli_command = "rebalance"
                options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
            self._validate_roles(output,result)


        if (cli_command in ["failover"]):
            cli_command = 'failover'
            for num in range(nodes_failover):
                self.log.info("failover node {0}".format(self.servers[nodes_add - nodes_rem - num].ip))
                options = "--server-failover={0}:8091".format(self.servers[nodes_add - nodes_rem - num].ip)
                options += " --force"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
            self._validate_roles(output,result)

        if (cli_command == "server-readd"):
            for num in range(nodes_readd):
                cli_command = 'failover'
                self.log.info("failover node {0}".format(self.servers[nodes_add - nodes_rem - num].ip))
                options = "--server-failover={0}:8091".format(self.servers[nodes_add - nodes_rem - num].ip)
                options += " --force"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
                self._validate_roles(output,result)
                self.log.info("add back node {0} to cluster".format(self.servers[nodes_add - nodes_rem - num ].ip))
                cli_command = "server-readd"
                options = "--server-add={0}:8091".format(self.servers[nodes_add - nodes_rem - num ].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
            self._validate_roles(output,result)
        remote_client.disconnect()


    def testBucketCreation(self):
        if self.role in ['replication_admin', 'views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin']:
            result = 'SUCCESS'
        bucket_name = self.input.param("bucket", "default")
        bucket_type = self.input.param("bucket_type", "couchbase")
        bucket_port = self.input.param("bucket_port", 11211)
        bucket_replica = self.input.param("bucket_replica", 1)
        bucket_password = self.input.param("bucket_password", None)
        bucket_ramsize = self.input.param("bucket_ramsize", 200)
        wait = self.input.param("wait", False)
        enable_flush = self.input.param("enable_flush", None)
        enable_index_replica = self.input.param("enable_index_replica", None)

        remote_client = RemoteMachineShellConnection(self.master)
        output = self._create_bucket(remote_client, bucket=bucket_name, bucket_type=bucket_type, bucket_port=bucket_port, \
                        bucket_ramsize=bucket_ramsize, bucket_replica=bucket_replica, wait=wait, enable_flush=enable_flush, enable_index_replica=enable_index_replica)
        self._validate_roles(output, result)
        remote_client.disconnect()


    def testBucketModification(self):
        if self.role in ['replication_admin', 'views_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin', 'bucket_admin[*]']:
            result = 'SUCCESS'
        cli_command = "bucket-edit"
        bucket_type = self.input.param("bucket_type", "couchbase")
        enable_flush = self.input.param("enable_flush", None)
        bucket_port_new = self.input.param("bucket_port_new", None)
        bucket_password_new = self.input.param("bucket_password_new", None)
        bucket_ramsize_new = self.input.param("bucket_ramsize_new", None)
        enable_flush_new = self.input.param("enable_flush_new", None)
        enable_index_replica_new = self.input.param("enable_index_replica_new", None)
        bucket_ramsize_new = self.input.param("bucket_ramsize_new", None)
        bucket = self.input.param("bucket", "default")
        bucket_ramsize = self.input.param("bucket_ramsize", 200)
        bucket_replica = self.input.param("bucket_replica", 1)
        enable_flush = self.input.param("enable_flush", None)
        enable_index_replica = self.input.param("enable_index_replica", None)
        wait = self.input.param("wait", False)

        remote_client = RemoteMachineShellConnection(self.master)

        self._create_bucket(remote_client, bucket, bucket_type=bucket_type, bucket_ramsize=bucket_ramsize,
                            bucket_replica=bucket_replica, wait=wait, enable_flush=enable_flush,
                            enable_index_replica=enable_index_replica, user="Administrator", password='password')

        cli_command = "bucket-edit"
        options = "--bucket={0}".format(bucket)
        options += (" --enable-flush={0}".format(enable_flush_new), "")[enable_flush_new is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica_new), "")[enable_index_replica_new is None]
        #options += (" --bucket-port={0}".format(bucket_port_new), "")[bucket_port_new is None]
        options += (" --bucket-ramsize={0}".format(bucket_ramsize_new), "")[bucket_ramsize_new is None]

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)

        cli_command = "bucket-flush --force"
        options = "--bucket={0}".format(bucket)
        if enable_flush_new is not None:
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)

        cli_command = "bucket-delete"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        expectedResults = {"bucket_name":"BBB", "source":self.source, "user":self.ldapUser, "ip":"127.0.0.1", "port":57457}
        self._validate_roles(output, result)

        remote_client.disconnect()

    def testSettingCompacttion(self):
        if self.role in ['replication_admin', 'views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin']:
            result = 'SUCCESS'
        cli_command = "bucket-edit"
        '''setting-compacttion OPTIONS:
        --compaction-db-percentage=PERCENTAGE     at which point database compaction is triggered
        --compaction-db-size=SIZE[MB]             at which point database compaction is triggered
        --compaction-view-percentage=PERCENTAGE   at which point view compaction is triggered
        --compaction-view-size=SIZE[MB]           at which point view compaction is triggered
        --compaction-period-from=HH:MM            allow compaction time period from
        --compaction-period-to=HH:MM              allow compaction time period to
        --enable-compaction-abort=[0|1]           allow compaction abort when time expires
        --enable-compaction-parallel=[0|1]        allow parallel compaction for database and view'''
        compaction_db_percentage = self.input.param("compaction-db-percentage", None)
        compaction_db_size = self.input.param("compaction-db-size", None)
        compaction_view_percentage = self.input.param("compaction-view-percentage", None)
        compaction_view_size = self.input.param("compaction-view-size", None)
        compaction_period_from = self.input.param("compaction-period-from", None)
        compaction_period_to = self.input.param("compaction-period-to", None)
        enable_compaction_abort = self.input.param("enable-compaction-abort", None)
        enable_compaction_parallel = self.input.param("enable-compaction-parallel", None)
        bucket = self.input.param("bucket", "default")
        output = self.input.param("output", '')
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "setting-compaction"
        options = (" --compaction-db-percentage={0}".format(compaction_db_percentage), "")[compaction_db_percentage is None]
        options += (" --compaction-db-size={0}".format(compaction_db_size), "")[compaction_db_size is None]
        options += (" --compaction-view-percentage={0}".format(compaction_view_percentage), "")[compaction_view_percentage is None]
        options += (" --compaction-view-size={0}".format(compaction_view_size), "")[compaction_view_size is None]
        options += (" --compaction-period-from={0}".format(compaction_period_from), "")[compaction_period_from is None]
        options += (" --compaction-period-to={0}".format(compaction_period_to), "")[compaction_period_to is None]
        options += (" --enable-compaction-abort={0}".format(enable_compaction_abort), "")[enable_compaction_abort is None]
        options += (" --enable-compaction-parallel={0}".format(enable_compaction_parallel), "")[enable_compaction_parallel is None]

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)
        remote_client.disconnect()



    def testSettingEmail(self):
        if self.role in ['replication_admin', 'views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin']:
            result = 'SUCCESS'
        setting_enable_email_alert = self.input.param("enable-email-alert", 1)
        setting_email_recipients = self.input.param("email-recipients", 'test@couchbase.com')
        setting_email_sender = self.input.param("email-sender", 'qe@couchbase.com')
        setting_email_user = self.input.param("email-user", 'ritam')
        setting_emaiL_password = self.input.param("email-password", 'password')
        setting_email_host = self.input.param("email-host", 'localhost')
        setting_email_port = self.input.param("email-port", '25')
        #setting_email_encrypt = self.input.param("enable-email-encrypt", 1)

        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "setting-alert"
        options = (" --enable-email-alert={0}".format(setting_enable_email_alert))
        options += (" --email-recipients={0}".format(setting_email_recipients))
        options += (" --email-sender={0}".format(setting_email_sender))
        options += (" --email-user={0}".format(setting_email_user))
        options += (" --email-password={0}".format(setting_emaiL_password))
        options += (" --email-host={0}".format(setting_email_host))
        options += (" --email-port={0}".format(setting_email_port))
        #options += (" --enable-email-encrypt={0}".format(setting_email_encrypt))
        options += (" --alert-auto-failover-node")
        options += (" --alert-auto-failover-max-reached")
        options += (" --alert-auto-failover-node-down")
        options += (" --alert-auto-failover-cluster-small")
        options += (" --alert-ip-changed")
        options += (" --alert-disk-space")
        options += (" --alert-meta-overhead")
        options += (" --alert-meta-oom")
        options += (" --alert-write-failed")

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)
        remote_client.disconnect()

    def testSettingNotification(self):
        if self.role in ['replication_admin', 'views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin']:
            result = 'SUCCESS'
        setting_enable_notification = self.input.param("enable-notification", 1)

        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "setting-notification"
        options = (" --enable-notification={0}".format(setting_enable_notification))

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)
        remote_client.disconnect()

    def testSettingFailover(self):
        if self.role in ['replication_admin', 'views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin']:
            result = 'SUCCESS'
        setting_enable_auto_failover = self.input.param("enable-auto-failover", 1)
        setting_auto_failover_timeout = self.input.param("auto-failover-timeout", 50)

        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "setting-autofailover"
        options = (" --enable-auto-failover={0}".format(setting_enable_auto_failover))
        options += (" --auto-failover-timeout={0}".format(setting_auto_failover_timeout))

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self._validate_roles(output,result)
        remote_client.disconnect()

    def testSSLManage(self):
        '''ssl-manage OPTIONS:
        --retrieve-cert=CERTIFICATE            retrieve cluster certificate AND save to a pem file
        --regenerate-cert=CERTIFICATE          regenerate cluster certificate AND save to a pem file'''
        xdcr_cert = self.input.param("xdcr-certificate", None)
        xdcr_cert = "/tmp/" + xdcr_cert
        cli_command = "ssl-manage"
        remote_client = RemoteMachineShellConnection(self.master)
        options = "--regenerate-cert={0}".format(xdcr_cert)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
        self.assertFalse(error, "Error thrown during CLI execution %s" % error)
        self.shell.execute_command("rm {0}".format(xdcr_cert))
        expectedResults = {"real_userid:source":self.source, "real_userid:user":self.ldapUser, "remote:ip":"127.0.0.1", "port":60035}
        self.checkConfig(8226, self.master, expectedResults)


    """ tests for the group-manage option. group creation, renaming and deletion are tested .
        These tests require a cluster of four or more nodes. """
    def testCreateRenameDeleteGroup(self):
        if self.role in ['replication_admin', 'views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin']:
            result = 'SUCCESS'

        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "group-manage"
        source = self.source
        user = self.ldapUser
        rest = RestConnection(self.master)

        if self.os == "linux":
            # create group
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
            self._validate_roles(output,result)

            if result != 'Forbidden':
                # rename group test
                options = " --rename=group3 --group-name=group2"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
                self._validate_roles(output,result)

                # delete group test
                options = " --delete --group-name=group3"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
                self._validate_roles(output,result)

        if self.os == "windows":
            # create group
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
            self._validate_roles(output,result)

            if result != 'Forbidden':
                # rename group test
                options = " --rename=group3 --group-name=group2"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
                self._validate_roles(output,result)

                # delete group test
                options = " --delete --group-name=group3"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="127.0.0.1:8091", user=self.ldapUser, password=self.ldapPass)
                self._validate_roles(output,result)

        remote_client.disconnect()



class XdcrCLITest(CliBaseTest):

    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(XdcrCLITest, self).setUp()
        self.ldapUser = self.input.param("ldapUser", 'Administrator')
        self.ldapPass = self.input.param('ldapPass', 'password')
        self.source = self.input.param('source', 'ns_server')
        self.__user = self.input.param("ldapUser", 'Administrator')
        self.__password = self.input.param('ldapPass', 'password')
        self.shell = RemoteMachineShellConnection(self.master)
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        self.role = self.input.param("role", "admin")
        if self.role in ['bucket_admin', 'views_admin']:
            self.role = self.role + "[*]"
        self.log.info (" value of self.role is {0}".format(self.role))
        if type == 'windows' and self.source == 'saslauthd':
            raise Exception(" Ldap Tests cannot run on windows");
        else:
            if self.source == 'saslauthd':
                rest = RestConnection(self.master)
                self.setupLDAPSettings(rest)
                self.set_user_role(rest, self.ldapUser, user_role=self.role)


    def tearDown(self):
        for server in self.servers:
            rest = RestConnection(server)
            rest.remove_all_remote_clusters()
            rest.remove_all_replications()
            rest.remove_all_recoveries()
        super(XdcrCLITest, self).tearDown()

    #Wrapper around auditmain
    def checkConfig(self, eventID, host, expectedResults):
        Audit = audit(eventID=eventID, host=host)
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    def setupLDAPSettings (self, rest):
        api = rest.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({"enabled":'true',"admins":[],"roAdmins":[]})
        status, content, header = rest._http_request(api, 'POST', params)
        return status, content, header

    def __execute_cli(self, cli_command, options, cluster_host="127.0.0.1:8091", user=None, password=None):
        if user is None:
            user = self.__user
            password = self.__password
        return self.shell.execute_couchbase_cli(
                                                cli_command=cli_command,
                                                options=options,
                                                cluster_host=cluster_host,
                                                user=user,
                                                password=password)
    def set_user_role(self,rest,username,user_role='admin'):
        payload = "name=" + username + "&roles=" + user_role
        rest.set_user_roles(user_id=username, payload=payload)

    def _validate_roles(self, output, result):
        final_result = True
        for outputs in output:
            if result not in outputs:
                final_result = False
            else:
                final_result = True
        self.assertTrue(final_result, "Incorrect Message for the role")

    def __xdcr_setup_create(self):
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'password': 'password'}]
        rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'roles': 'admin'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        # xdcr_hostname=the number of server in ini file to add to master as replication
        xdcr_cluster_name = self.input.param("xdcr-cluster-name", None)
        xdcr_hostname = self.input.param("xdcr-hostname", None)
        xdcr_username = self.input.param("xdcr-username", None)
        xdcr_password = self.input.param("xdcr-password", None)
        demand_encyrption = self.input.param("demand-encryption", 0)
        xdcr_cert = self.input.param("xdcr-certificate", None)
        wrong_cert = self.input.param("wrong-certificate", None)

        cli_command = "xdcr-setup"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name), "")[xdcr_cluster_name is None]
        print(("Value of xdcr_home is {0}".format(xdcr_hostname)))
        if xdcr_hostname is not None:
            RbacBase().create_user_source(testuser, 'builtin', self.servers[xdcr_hostname])
            RbacBase().add_user_role(rolelist, RestConnection(self.servers[xdcr_hostname]), 'builtin')
            options += " --xdcr-hostname={0}".format(self.servers[xdcr_hostname].ip)
        options += (" --xdcr-username={0}".format(xdcr_username), "")[xdcr_username is None]
        options += (" --xdcr-password={0}".format(xdcr_password), "")[xdcr_password is None]
        options += (" --xdcr-demand-encryption={0}".format(demand_encyrption))

        cluster_host = self.servers[xdcr_hostname].ip
        output, _ = self.__execute_cli(cli_command="ssl-manage", options="--retrieve-cert={0}".format(xdcr_cert), cluster_host=cluster_host, user='Administrator', password='password')
        options += (" --xdcr-certificate={0}".format(xdcr_cert), "")[xdcr_cert is None]
        #self.assertNotEqual(output[0].find("SUCCESS"), -1, "ssl-manage CLI failed to retrieve certificate")

        output, error = self.__execute_cli(cli_command=cli_command, options=options)
        return output, error, xdcr_cluster_name, xdcr_hostname, cli_command, options

    def testXDCRSetup(self):
        if self.role in ['views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin', 'replication_admin']:
            result = 'SUCCESS'
        output, _, xdcr_cluster_name, xdcr_hostname, cli_command, options = self.__xdcr_setup_create()
        self._validate_roles(output, result)

        if xdcr_cluster_name:
            options = options.replace("--create ", "--edit ")
            output, _ = self.__execute_cli(cli_command=cli_command, options=options)
            self._validate_roles(output, result)

        if not xdcr_cluster_name:
            options = "--delete --xdcr-cluster-name=\'{0}\'".format("remote cluster")
        else:
            options = "--delete --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name)
        output, _ = self.__execute_cli(cli_command=cli_command, options=options)
        self._validate_roles(output, result)



    def testXdcrReplication(self):
        if self.role in ['views_admin[*]', 'bucket_admin[*]']:
            result = "Forbidden"
        elif self.role in ['admin', 'cluster_admin', 'replication_admin']:
            result = 'SUCCESS'

        '''xdcr-replicate OPTIONS:
        --create                               create and start a new replication
        --delete                               stop and cancel a replication
        --list                                 list all xdcr replications
        --xdcr-from-bucket=BUCKET              local bucket name to replicate from
        --xdcr-cluster-name=CLUSTERNAME        remote cluster to replicate to
        --xdcr-to-bucket=BUCKETNAME            remote bucket to replicate to'''
        to_bucket = self.input.param("xdcr-to-bucket", None)
        from_bucket = self.input.param("xdcr-from-bucket", None)
        error_expected = self.input.param("error-expected", False)
        replication_mode = self.input.param("replication_mode", None)
        pause_resume = self.input.param("pause-resume", None)
        _, _, xdcr_cluster_name, xdcr_hostname, _, _ = self.__xdcr_setup_create()
        cli_command = "xdcr-replicate"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name), "")[xdcr_cluster_name is None]
        options += (" --xdcr-from-bucket=\'{0}\'".format(from_bucket), "")[from_bucket is None]
        options += (" --xdcr-to-bucket=\'{0}\'".format(to_bucket), "")[to_bucket is None]
        options += (" --xdcr-replication-mode=\'{0}\'".format(replication_mode), "")[replication_mode is None]
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        if from_bucket:
            bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                              replicas=self.num_replicas,
                                                              enable_replica_index=self.enable_replica_index)
            self.cluster.create_default_bucket(bucket_params)

        if to_bucket:
            bucket_params = self._create_bucket_params(server=self.servers[xdcr_hostname], size=self.bucket_size,
                                                              replicas=self.num_replicas,
                                                              enable_replica_index=self.enable_replica_index)
            self.cluster.create_default_bucket(bucket_params)

        output, _ = self.__execute_cli(cli_command, options)
        self._validate_roles(output, result)

        self.sleep(8)
        options = "--list"
        output, _ = self.__execute_cli(cli_command, options)
        for value in output:
            if value.startswith("stream id"):
                replicator = value.split(":")[1].strip()
                if pause_resume is not None:
                    # pause replication
                    options = "--pause"
                    options += (" --xdcr-replicator={0}".format(replicator))
                    output, _ = self.__execute_cli(cli_command, options)
                    self._validate_roles(output, result)

                    self.sleep(60)
                    # resume replication
                    options = "--resume"
                    options += (" --xdcr-replicator={0}".format(replicator))
                    output, _ = self.__execute_cli(cli_command, options)
                    self._validate_roles(output, result)

                options = "--delete"
                options += (" --xdcr-replicator={0}".format(replicator))
                output, _ = self.__execute_cli(cli_command, options)
                self._validate_roles(output, result)
