from membase.api.rest_client import RestHelper
import logging
import json

from lib.Cb_constants.CBServer import CbServer
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.membase.api.rest_client import RestConnection
from upgrade.newupgradebasetest import NewUpgradeBaseTest
from security.x509main import x509main
from security.ldapGroupBase import ldapGroupBase
from security.ldap_user import LdapUser
from security.rbacmain import rbacmain
from security.ntonencryptionBase import ntonencryptionBase

from lib.membase.helper.cluster_helper import ClusterOperationHelper
from pytests.security.auditmain import audit

log = logging.getLogger()


class SecurityUpgrade(NewUpgradeBaseTest):
    def setUp(self):
        log.info("==============  Security Upgrade setup has started ==============")
        super(SecurityUpgrade, self).setUp()
        self.initial_version = self.input.param("initial_version", '6.6.3-9799')
        self.upgrade_version = self.input.param("upgrade_version", "7.0.2-6578")

    def tearDown(self):
        ntonencryptionBase().disable_nton_cluster(self.servers)
        self._reset_original(self.servers)
        super(SecurityUpgrade, self).tearDown()

    def enable_audit(self):
        Audit = audit(host=self.master)
        currentState = Audit.getAuditStatus()
        self.log.info("Current status of audit on ip - {0} is {1}".
                      format(self.master.ip, currentState))
        if not currentState:
            self.log.info("Enabling Audit ")
            Audit.setAuditEnable('true')
            self.sleep(30)

    def check_rest_api(self, host):
        rest = RestConnection(host)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', ramQuotaMB=100)
            self.sleep(10)
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,
                                                            url='/pools/default',
                                                            port=18091, headers="",
                                                            client_cert=True, curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip,
                                                            url='/pools/default',
                                                            port=18091,
                                                            headers=' -u Administrator:password ',
                                                            client_cert=False,
                                                            curl=True)
        output = json.loads(output)
        self.log.info("Print output of command is {0}".format(output))
        self.assertEqual(output['rebalanceStatus'], 'none',
                         " The Web request has failed on port 18091 ")
        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip,
                                                            url='/pools/default',
                                                            port=18091, headers=None,
                                                            client_cert=True,
                                                            curl=True, verb='POST',
                                                            data='memoryQuota=400')
        else:
            output = x509main()._execute_command_clientcert(host.ip,
                                                            url='/pools/default', port=18091,
                                                            headers=' -u Administrator:password ',
                                                            client_cert=False,
                                                            curl=True, verb='POST',
                                                            data='memoryQuota=400')
        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")
        output = x509main()._execute_command_clientcert(host.ip,
                                                        url='/pools/default',
                                                        port=8091,
                                                        headers=" -u Administrator:password ",
                                                        client_cert=False,
                                                        curl=True, verb='GET', plain_curl=True)
        self.assertEqual(json.loads(output)['rebalanceStatus'], 'none',
                         " The Web request has failed on port 8091 ")
        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default',
                                                        port=8091,
                                                        headers=" -u Administrator:password ",
                                                        client_cert=True,
                                                        curl=True, verb='POST',
                                                        plain_curl=True, data='memoryQuota=400')
        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")

    def user_setup(self, user=None, group=None, auth_type=None, usrName=None,
                   grpUser=None, groupName=None, roles=None):
        self.auth_type = auth_type
        if user is not None:
            if self.auth_type == 'externalUser':
                ldapGroupBase().create_ldap_config(self.master)
                LdapUser('user3', 'password', self.master).user_setup()
                payload = 'name=' + usrName + '&roles=' + roles
                RestConnection(self.master).add_external_user(usrName, payload)
            elif self.auth_type == 'internalUser':
                payload = "name=" + usrName + "&roles=" + roles + "&password=password"
                RestConnection(self.master).add_set_builtin_user(usrName, payload)
        if group is not None:
            if self.auth_type == 'InternalGroup':
                ldapGroupBase().create_int_group(groupName, [grpUser], roles, [''],
                                                 self.master, user_creation=True)
            elif self.auth_type == 'ExternalGroup':
                LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"
                ldapGroupBase().create_group_ldap(groupName, [grpUser], self.master)
                group_dn = 'cn=' + groupName + ',' + LDAP_GROUP_DN
                ldapGroupBase().add_role_group(groupName, roles, group_dn, self.master)

    def get_current_roles(self, user, non_admin=False):
        status, content, header = rbacmain(self.master)._retrieve_user_roles()
        content = json.loads(content)
        final_role = ''
        for item in content:
            if item['id'] == user:
                for item_list in item['roles']:
                    final_role = item_list['role']
                    if non_admin:
                        final_role = item_list['role'] + "[" + item_list['bucket_name'] + \
                                     ":" + item_list['scope_name'] + ":" + \
                                     item_list['collection_name'] + "]"
        return user, final_role

    def validate_user_role(self, user, role=None, actualResult=None,
                           bucketName="default", scope="*", collection="*",
                           version="7.0"):
        if version == '7.0':
            expectedResult = role + "[" + bucketName + ":*:*]"
            if expectedResult == actualResult:
                print("Comparision Passed")
            else:
                print(actualResult)
                print(expectedResult)

    def setupnton(self, clusterencryption_level):
        ntonencryptionBase().setup_nton_cluster(self.servers,
                                                clusterEncryptionLevel=clusterencryption_level)

    def disablenton(self):
        ntonencryptionBase().disable_nton_cluster(self.servers)

    def validatenton(self, clusterencryption_level):
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        result = ntonencryptionBase().validate_results(self.servers, final_result,
                                                       clusterencryption_level)
        self.assertTrue(result, 'Issue with results with x509 enable for sets')

    def test(self):
        self.user_setup(user=True, auth_type='internalUser',
                        usrName='user2', roles='admin')
        self.user_setup(group=True, auth_type='InternalGroup', grpUser='intuser3',
                        roles='admin', groupName='extgrp1')
        user, final_role = self.get_current_roles('user2')
        user, final_role = self.get_current_roles('intuser3')

    def upgrade_all_nodes_user(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=100)
        self.cluster.rebalance(self.servers, servers_in, [])
        self.user_setup(user=True, auth_type='internalUser', usrName='user2',
                        roles='admin')
        self.user_setup(group=True, auth_type='InternalGroup', grpUser='intuser3',
                        roles='admin', groupName='extgrp1')
        self.user_setup(user=True, auth_type='internalUser', usrName='user4',
                        roles='data_writer[default]')
        self.user_setup(group=True, auth_type='InternalGroup', grpUser='intuser4',
                        roles='data_writer[default]', groupName='extgrp1')
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                             servers=self.servers)
        for threads in upgrade_threads:
            threads.join()
        user, final_role = self.get_current_roles('user2')
        user, final_role = self.get_current_roles('intuser3')
        user, final_role = self.get_current_roles('user4', non_admin=True)
        user, final_role = self.get_current_roles('intuser4', non_admin=True)
        self.validate_user_role('user4', 'data_writer', final_role)
        self.validate_user_role('intuser4', 'data_writer', final_role)

    def upgrade_all_nodes_nton(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=100)
        self.cluster.rebalance(self.servers, servers_in, [])
        self.setupnton("control")
        self.validatenton("control")
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                             servers=self.servers)
        for threads in upgrade_threads:
            threads.join()
        # self.setupnton("control")
        self.validatenton("control")

    def upgrade_all_nodes_nton_x509(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=100)
        self.cluster.rebalance(self.servers, servers_in, [])
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        self.setupnton("control")
        self.validatenton("control")
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                             servers=self.servers)
        for threads in upgrade_threads:
            threads.join()
        # self.setupnton("control")
        self.validatenton("control")
        for server in self.servers:
            # result = self._sdk_connection(host_ip=server.ip)
            # self.assertTrue(result, "Cannot create a security connection with server")
            # result = self._sdk_connection(host_ip=server.ip, sdk_version='vulcan')
            # self.assertTrue(result, "Cannot create a security connection with server")
            self.check_rest_api(server)

    def upgrade_half_nodes(self):
        serv_upgrade = self.servers[2:4]
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=100)
        self.cluster.rebalance(self.servers, servers_in, [])
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                             servers=serv_upgrade)
        for threads in upgrade_threads:
            threads.join()
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        for server in self.servers:
            self.check_rest_api(server)
            # result = self._sdk_connection(host_ip=server.ip)
            # self.assertFalse(result, "Can create a security connection with server")

    def upgrade_all_nodes_4_6_3(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=100)
        self.cluster.rebalance(self.servers, servers_in, [])
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=self.servers)
        for threads in upgrade_threads:
            threads.join()
        for server in self.servers:
            # result = self._sdk_connection(host_ip=server.ip)
            # self.assertTrue(result, "Cannot create a security connection with server")
            # result = self._sdk_connection(host_ip=server.ip, sdk_version='vulcan')
            # self.assertTrue(result, "Cannot create a security connection with server")
            self.check_rest_api(server)

    def test_x509_and_n2n_upgrade(self):
        """
        1. Set optionally x509 certs
        2. Set optionally n2n encryption level
        3. Upgrade the cluster
        4. Load bucket
        5. Set optionally x509 certs
        6. Set optionally n2n encryption level
        7. Load bucket
        """
        self.log.info("------------- test started-------------")
        self.pre_upgrade_x509 = self.input.param("pre_upgrade_x509", False)
        self.post_upgrade_x509 = self.input.param("post_upgrade_x509", True)
        self.pre_upgrade_n2n_level = self.input.param("pre_upgrade_n2n_level", None)
        self.post_upgrade_n2n_level = self.input.param("post_upgrade_n2n_level", "strict")

        if self.pre_upgrade_x509:
            self.log.info("------------- setting x509 pre-upgrade-------------")
            self.generate_x509_certs(self.servers)
            if self.setup_once:
                self.upload_x509_certs(self.servers)
        if self.pre_upgrade_n2n_level is not None:
            self.log.info("------------- setting n2n {0} pre-upgrade".
                          format(self.pre_upgrade_n2n_level))
            ntonencryptionBase().setup_nton_cluster(self.servers,
                                                    clusterEncryptionLevel=
                                                    self.pre_upgrade_n2n_level)

        self.log.info("-------------Performing upgrade to version------------- {0}".
                      format(self.upgrade_version))
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                             servers=self.servers)
        for threads in upgrade_threads:
            threads.join()
        self.log.info("Upgrade finished")

        self.log.info("------------- Loading bucket after upgrade -------------")
        gen = BlobGenerator('mike', 'mike-', self.value_size, start=0,
                            end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        if self.post_upgrade_x509:
            self.generate_x509_certs(self.servers)
            if self.setup_once:
                self.upload_x509_certs(self.servers)
        if self.post_upgrade_n2n_level:
            ntonencryptionBase().setup_nton_cluster(self.servers,
                                                    clusterEncryptionLevel=
                                                    self.post_upgrade_n2n_level)
            if self.post_upgrade_n2n_level == "strict":
                self.log.info("------------- setting use https to True -------------")
                CbServer.use_https = True
                status = ClusterOperationHelper.check_if_services_obey_tls\
                    (servers=self.servers)
                if not status:
                    self.fail("Services did not obey tls")

        self.log.info("------------- Loading bucket after upgrade & "
                      "changing security settings -------------")
        gen = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items,
                            end=2*self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
