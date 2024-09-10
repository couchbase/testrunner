import copy
import json
import logging
import random
import string

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509main import x509main as x509mainold
from pytests.security.x509_multiple_CA_util import x509main, Validation
from pytests.upgrade.newupgradebasetest import NewUpgradeBaseTest
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError, CouchbaseInputError

log = logging.getLogger()


class LdapContainer:
    """
    Some generic docker commands and a helper function to
    start an ldap container
    #TODo: support commands for all OS. For now, since the test runs only on
    #ToDO... particular set of centos VMs, it should be fine.
    """

    ldap_ca = "./pytests/security/x509_extension_files/ldap_container_ca.crt"
    container_name = "sumedh"
    hostname = "ldap.example.org"
    port = 389
    ssl_port = 636
    bindDN = "cn=admin,dc=example,dc=org"
    bindPass = "admin"
    ldap_user = "admin"

    @staticmethod
    def start_docker(shell):
        o, e = shell.execute_command("systemctl start docker")
        log.info('Output of starting containers is {0} and error message is {1}'.format(o, e))

    @staticmethod
    def stop_all_containers(shell):
        o, e = shell.execute_command("docker stop $(docker ps -a -q)")
        log.info('Output of stopping containers is {0} and error message is {1}'.format(o, e))

    @staticmethod
    def remove_all_containers(shell):
        o, e = shell.execute_command("docker rm $(docker ps -a -q)")
        log.info('Output of removing containers is {0} and error message is {1}'.format(o, e))

    def start_ldap_container(self, shell):
        o, e = shell.execute_command("docker run --name " + self.container_name +
                                     " --hostname " + self.hostname +
                                     " -p 389:389 -p 636:636 " +
                                     " --env LDAP_TLS_VERIFY_CLIENT=try" +
                                     " --detach osixia/openldap:1.5.0")
        log.info('Output of removing containers is {0} and error message is {1}'.format(o, e))


class MultipleCAUpgrade(NewUpgradeBaseTest):
    def setUp(self):
        self.test_setup_finished = False
        log.info("==============  Multiple CA Upgrade setup has started ==============")
        super(MultipleCAUpgrade, self).setUp()

        self.initial_version = self.input.param("initial_version", '6.6.3-9808')
        self.upgrade_version = self.input.param("upgrade_version", "7.1.0-1745")
        self.enc_key_mixed_mode = self.input.param("enc_key_mixed_mode", False)
        self.skip_rbac_internal_users_setup = self.input.param("skip_rbac_internal_users_setup",
                                                               False)
        self.skip_ldap_external_user_setup = self.input.param("skip_ldap_external_user_setup",
                                                              False)

        self.base_version = RestConnection(self.master).get_nodes_versions()[0]
        if self.enc_key_mixed_mode in ["True", True]:
            self.enc_key_mixed_mode = True
        else:
            self.enc_key_mixed_mode = False
        shell = RemoteMachineShellConnection(self.master)
        self.windows_test = False
        if shell.extract_remote_info().distribution_type == "windows":
            self.windows_test = True
        shell.disconnect()
        self.openssl_path = "/opt/couchbase/bin/openssl"
        self.inbox_folder_path = "/opt/couchbase/var/lib/couchbase/inbox/"
        if self.windows_test:
            self.openssl_path = "C:/Program Files/Couchbase/Server/bin/openssl"
            self.inbox_folder_path = "C:/Program Files/Couchbase/Server/var/lib/couchbase/inbox/"
        self.plain_passw_map = dict()

        self.load_sample_bucket(self.master, "travel-sample")
        if not self.skip_rbac_internal_users_setup:
            self.add_rbac_groups_roles(self.master)
        if not self.skip_ldap_external_user_setup:
            self.setup_ldap_config(server=self.master)
            self.add_ldap_user(self.master)
        self.test_setup_finished = True

    def tearDown(self):
        log.info("==============  Multiple CA Upgrade teardown has started ==============")
        ntonencryptionBase().disable_nton_cluster(self.servers)
        CbServer.use_https = False
        if self.test_setup_finished:
            self._install(self.servers)
        super(MultipleCAUpgrade, self).tearDown()
        log.info("==============  Multiple CA Upgrade teardown finished ==============")

    def load_sample_bucket(self, server, bucket_name="travel-sample"):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("""curl -v -u Administrator:password \
                             -X POST http://localhost:8091/sampleBuckets/install \
                          -d '["{0}"]'""".format(bucket_name))
        shell.disconnect()
        self.sleep(60)

    def add_rbac_groups_roles(self, server):
        """
        Adds group with RBAC roles and adds users to groups
        """
        rest = RestConnection(server)
        group_roles = ['admin', 'cluster_admin', 'ro_admin',
                    'bucket_admin[travel-sample]', 'bucket_full_access[travel-sample]',
                    'data_backup[travel-sample]']
        if self.base_version < "7.0":
            group_roles.extend(['security_admin', 'data_reader[travel-sample]',
                            'data_writer[travel-sample]', 'data_dcp_reader[travel-sample]',
                            'data_monitoring[travel-sample]'])
        else:
            group_roles.extend(['security_admin_local', 'security_admin_external',
                            'data_reader[travel-sample:_default:_default]',
                            'data_writer[travel-sample:_default:_default]',
                            'data_dcp_reader[travel-sample:_default:_default]',
                            'data_monitoring[travel-sample:_default:_default]'])
        groups = []
        for role in group_roles:
            group_name = "group_" + role.split("[",1)[0]
            groups.append(group_name)
            roles = role
            log.info("Group name -- {0} :: Roles -- {1}".format(group_name, roles))
            rest.add_group_role(group_name, group_name, roles, None)

        for group_name in groups:
            user_name = "user_" + group_name
            role = ''
            password = "password"
            payload = "name={0}&roles={1}&password={2}".format(user_name, role, password)
            rest.add_set_builtin_user(user_name, payload)

            log.info("Group name -- {0} :: User name -- {1}".format(group_name, user_name))
            rest.add_user_group(group_name, user_name)

    @staticmethod
    def get_ldap_params(hosts='172.23.120.175', port='389', encryption='None',
                        bindDN='cn=admin,dc=couchbase,dc=com', bindPass='p@ssw0rd',
                        serverCertValidation='false',
                        cacert=None,
                        authenticationEnabled='true',
                        userDNMapping='{"template":"cn=%u,ou=Users,dc=couchbase,dc=com"}'):
        param = {
            'hosts': '{0}'.format(hosts),
            'port': '{0}'.format(port),
            'encryption': '{0}'.format(encryption),
            'bindDN': '{0}'.format(bindDN),
            'bindPass': '{0}'.format(bindPass),
            'authenticationEnabled': '{0}'.format(authenticationEnabled),
            'userDNMapping': '{0}'.format(userDNMapping)
        }
        if encryption is not None:
            param['serverCertValidation'] = serverCertValidation
            if cacert is not None:
                param['cacert'] = cacert
        return param

    def setup_ldap_config(self, server, param=None):
        """
        Setups ldap configuration on couchbase
        :param: (optional) - ldap config parameters
        :server: server to make REST connection to setup ldap on CB cluster
        """
        if param is None:
            param = self.get_ldap_params()
        rest = RestConnection(server)
        rest.setup_ldap(param, '')

    def add_ldap_user(self, server, user_name="bjones"):
        """
        Add an ldap user to CB cluster with RBAC roles
        """
        roles = '''cluster_admin,ro_admin,
                bucket_admin[travel-sample],bucket_full_access[travel-sample],
                data_backup[travel-sample],'''
        if self.base_version < "7.0":
            roles = roles + '''security_admin,
                            data_reader[travel-sample],
                            data_writer[travel-sample],
                            data_dcp_reader[travel-sample],
                            data_monitoring[travel-sample]'''
        else:
            roles = roles + '''security_admin_local,security_admin_external,
                            data_reader[travel-sample:_default:_default],
                            data_writer[travel-sample:_default:_default],
                            data_dcp_reader[travel-sample:_default:_default],
                            data_monitoring[travel-sample:_default:_default]'''

        payload = "name={0}&roles={1}".format(user_name, roles)
        log.info("User name -- {0} :: Roles -- {1}".format(user_name, roles))
        rest = RestConnection(server)
        rest.add_external_user(user_name, payload)

    def make_rest_call_with_ldap_user(self, ldap_rest, api=None):
        """
        :ldap_rest" - Rest connection object ldap user's credentials
        :api: - (optional) String url to make rest GET request
        """
        if api is None:
            api = "https://%s:18091/nodes/self" % self.master.ip
        status, content, response = ldap_rest._http_request(api=api, timeout=10)
        return status, content, response

    def validate_rbac_users_post_upgrade(self, server):
        expected_user_roles = {'cbadminbucket': ['admin'],
                    'user_group_data_backup': ['data_backup[travel-sample]'],
                    'user_group_ro_admin': ['ro_admin'],
                    'user_group_admin': ['admin'],
                    'user_group_bucket_full_access': ['bucket_full_access[travel-sample]'],
                    'user_group_cluster_admin': ['cluster_admin'],
                    'user_group_bucket_admin': ['bucket_admin[travel-sample]'],
                    'bjones': [
                        'data_backup[travel-sample]',
                        'bucket_full_access[travel-sample]', 'bucket_admin[travel-sample]',
                        'security_admin_local', 'security_admin_external',
                        'ro_admin', 'cluster_admin']}
        if self.base_version < "7.0":
            expected_user_roles.update(
                    {'user_group_security_admin': ['security_admin_local', 'security_admin_external'],
                    'user_group_data_reader': ['data_reader[travel-sample][*][*]'],
                    'user_group_data_writer': ['data_writer[travel-sample][*][*]'],
                    'user_group_data_monitoring': ['data_monitoring[travel-sample][*][*]'],
                    'user_group_data_dcp_reader': ['data_dcp_reader[travel-sample][*][*]']})
            expected_user_roles['bjones'].extend(['data_writer[travel-sample][*][*]',
                        'data_reader[travel-sample][*][*]',
                        'data_monitoring[travel-sample][*][*]',
                        'data_dcp_reader[travel-sample][*][*]'])
        else:
            expected_user_roles.update(
                    {'user_group_security_admin_external': ['security_admin_external'],
                     'user_group_security_admin_local': ['security_admin_local'],
                    'user_group_data_reader': ['data_reader[travel-sample][_default][_default]'],
                    'user_group_data_writer': ['data_writer[travel-sample][_default][_default]'],
                    'user_group_data_monitoring': ['data_monitoring[travel-sample][_default][_default]'],
                    'user_group_data_dcp_reader': ['data_dcp_reader[travel-sample][_default][_default]']})
            expected_user_roles['bjones'].extend(['data_writer[travel-sample][_default][_default]',
                        'data_reader[travel-sample][_default][_default]',
                        'data_monitoring[travel-sample][_default][_default]',
                        'data_dcp_reader[travel-sample][_default][_default]'])

        rest = RestConnection(server)
        content = rest.retrieve_user_roles()
        observed_user_roles = {}
        for items in content:
            roles_list = []
            for role in items['roles']:
                roles_str = role['role']
                if 'bucket_name' in role:
                    roles_str = roles_str + "[" + role['bucket_name'] + "]"
                    if 'scope_name' in role:
                        roles_str = roles_str + "[" + role['scope_name'] + "]"
                        if 'collection_name' in role:
                            roles_str = roles_str + "[" + role['collection_name'] + "]"
                roles_list.append(roles_str)
                observed_user_roles[items['id']] = roles_list

        if len(observed_user_roles) != len(expected_user_roles):
            self.fail("Validation of RBAC user roles post upgrade failed. Mismatch in the number of users.")
        for user in expected_user_roles:
            if set(expected_user_roles[user]) != set(observed_user_roles[user]):
                self.fail("Validation of RBAC user roles post upgrade failed. Mismatch in the roles of {0}."
                          .format(user))
        self.log.info("Validation of RBAC user roles post upgrade -- SUCCESS")
        self.user_roles = observed_user_roles

    def validate_user_roles_sdk(self, server):
        """
        1. Create SDK connection
        2. Check whether users created before upgrade are able to write/read docs
            into the bucket post upgrade depending on their roles
        Written in accordance with python sdk version - 2.4
        """
        ntonencryptionBase().disable_nton_cluster(self.servers)
        CbServer.use_https = False

        for user in self.user_roles:
            # select users with data roles
            data_roles = [role for role in self.user_roles[user] if "data_" in role]
            if data_roles:
                self.log.info("USERNAME -- {0}".format(user))

                # create sdk connection
                bucket_name = "travel-sample"
                url = 'couchbase://{ip}/{name}'.format(ip=server.ip, name=bucket_name)
                if user == "bjones":
                    url = 'couchbase://{ip}/{name}?sasl_mech_force=PLAIN'.format(
                        ip=server.ip, name=bucket_name)
                bucket = Bucket(url, username=user, password="password")

                try:
                    # write/upsert into the doc
                    bucket.upsert('doc123', {'key123': 'value123'})
                except CouchbaseInputError:
                    if any("data_reader" in role for role in data_roles) or ("data_dcp_reader" in \
                        role for role in data_roles) or any("data_monitoring" in role for \
                        role in data_roles):
                        self.log.info("Write permission not granted as expected")
                    else:
                        self.fail("Validation failed. The user should have permission to "
                                  "write to the given bucket")
                except CouchbaseError:
                    if any("data_writer" in role for role in data_roles) or any("data_backup" \
                       in role for role in data_roles):
                        self.log.info("Write permission granted as expected")
                    else:
                        self.fail("Validation failed. The user should not have permission to "
                                  "write to the given bucket")
                else:
                    if any("data_writer" in role for role in data_roles) or any("data_backup" \
                        in role for role in data_roles):
                        self.log.info("Write permission granted as expected")
                    else:
                        self.fail("Validation failed. The user should not have permission to "
                                  "write to the given bucket")

                try:
                    # read the doc
                    bucket.get('airline_10')
                except CouchbaseInputError:
                    if any("data_writer" in role for role in data_roles) or any("data_monitoring" \
                       in role for role in data_roles):
                        self.log.info("Read permission not granted as expected")
                    else:
                        self.fail("Validation failed. The user should have permission to "
                                  "read the given bucket")
                except CouchbaseError:
                    if any("data_reader" in role for role in data_roles) or any("data_dcp_reader" \
                        in role for role in data_roles) or any("data_backup" in role for role \
                        in data_roles):
                        self.log.info("Read permission granted as expected")
                    else:
                        self.fail("Validation failed. The user should not have permission to "
                                  "read the given bucket")
                else:
                    if any("data_reader" in role for role in data_roles) or any("data_dcp_reader" \
                        in role for role in data_roles) or any("data_backup" in role for role \
                        in data_roles):
                        self.log.info("Read permission granted as expected")
                    else:
                        self.fail("Validation failed. The user should not have permission to "
                                  "read the given bucket")

    @staticmethod
    def copy_file_from_slave_to_server(server, src, dst):
        log.info("Copying file from slave to server {0}, src{1}, dst{2}".format(server, src, dst))
        shell = RemoteMachineShellConnection(server)
        shell.copy_file_local_to_remote(src, dst)
        shell.disconnect()

    def convert_to_pkcs8(self, node):
        """
        converts a pkcs#1 pkey to pkcs#8 encrypted pkey
        directly by executing openssl cmds on VM.
        """
        shell = RemoteMachineShellConnection(node)
        passw = ''.join(random.choice(string.ascii_uppercase + string.digits)
                        for _ in range(20))
        convert_cmd = self.openssl_path + " pkcs8 -in " + self.inbox_folder_path + "pkey.key" +\
                      " -passout pass:" + passw + " -topk8 -v2 aes256 -out " + \
                      self.inbox_folder_path + "enckey.key"
        output, error = shell.execute_command(convert_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        self.plain_passw_map[node.ip] = passw
        remove_cmd = "rm -rf " + self.inbox_folder_path + "pkey.key"
        output, error = shell.execute_command(remove_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        mv_command = "mv " + self.inbox_folder_path + "enckey.key " + \
                     self.inbox_folder_path + "pkey.key"
        output, error = shell.execute_command(mv_command)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        permissions_cmd = "chmod +777 " + self.inbox_folder_path + "pkey.key"
        output, error = shell.execute_command(permissions_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        shell.disconnect()

    def reload_node_cert_with_plain_password(self, node):
        params = dict()
        params["privateKeyPassphrase"] = dict()
        params["privateKeyPassphrase"]["type"] = "plain"
        params["privateKeyPassphrase"]["password"] = self.plain_passw_map[str(node.ip)]
        params = json.dumps(params)
        rest = RestConnection(node)
        status, content = rest.reload_certificate(params=params)
        if not status:
            msg = "Could not load reload node cert on %s; Failed with error %s" \
                  % (node.ip, content)
            raise Exception(msg)

    def auth(self, client_certs=None, api=None, servers=None):
        """
        Performs client-cert and Basic authentication via:
            1. Making a rest call to servers at api
            2. Making a sdk connection and doing some creates
        :client_certs: (list) - list of tuples. Each tuple being client cert,
                                client private ket
        :api: - full url to make a rest call
        :servers: a list of servers to make sdk connection/rest connection against
        """
        if client_certs is None:
            client_certs = list()
            client_certs.append(self.x509_new.get_client_cert(int_ca_name="i1_r1"))
            client_certs.append(self.x509_new.get_client_cert(int_ca_name="iclient1_r1"))
            client_certs.append(self.x509_new.get_client_cert(int_ca_name="iclient1_clientroot"))
        if servers is None:
            servers = [self.servers[0]]
        for client_cert_path_tuple in client_certs:
            for server in servers:
                if api is None:
                    api_ep = "https://" + server.ip + ":18091/pools/default/"
                else:
                    api_ep = api
                # 1) using client auth
                self.x509_new_validation = Validation(server=server,
                                                      cacert=x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME,
                                                      client_cert_path_tuple=client_cert_path_tuple)
                # 1a) rest api
                status, content, response = self.x509_new_validation.urllib_request(api=api_ep)
                if not status:
                    self.fail("Could not login using client cert auth {0}".format(content))
                # 1b) sdk
                client = self.x509_new_validation.sdk_connection()
                self.x509_new_validation.creates_sdk(client)

                # 2) using basic auth
                self.x509_new_validation = Validation(server=server,
                                                      cacert=x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME,
                                                      client_cert_path_tuple=None)
                # 2a) rest api
                status, content, response = self.x509_new_validation.urllib_request(api=api_ep)
                if not status:
                    self.fail("Could not login using basic auth {0}".format(content))

    def wait_for_rebalance_to_complete(self, task):
        status = task.result()
        if not status:
            self.fail("rebalance/failover failed")

    def test_multiple_CAs_offline(self):
        """
        1. Init Pre-neo cluster with x509 (single CA)
        2. Offline upgrade node one by one
        3. Optionally convert pkcs#1 key to encrypted pkcs#8 key in mixed mode cluster
        4. After cluster is fully upgraded, rotate certs to use pkcs#8 keys and multiple CAs
        """
        self.log.info("------------- test started-------------")
        self.generate_x509_certs(self.servers)
        self.upload_x509_certs(self.servers)
        ntonencryptionBase().setup_nton_cluster(servers=self.servers,
                                                clusterEncryptionLevel="all")

        for node in self.servers:
            self.log.info("-------------Performing upgrade on node {0} to version------------- {1}".
                          format(node, self.upgrade_version))
            upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                                 servers=[node])
            for threads in upgrade_threads:
                threads.join()
            self.log.info("Upgrade finished")
            if self.enc_key_mixed_mode:
                self.convert_to_pkcs8(node)
                self.reload_node_cert_with_plain_password(node)
        CbServer.use_https = True
        ntonencryptionBase().setup_nton_cluster(servers=self.servers,
                                                clusterEncryptionLevel="strict")
        self._reset_original(self.servers)
        self.x509_new = x509main(host=self.master, standard="pkcs8",
                                 encryption_type="aes256",
                                 passphrase_type="script")
        self.x509_new.generate_multiple_x509_certs(servers=self.servers)
        for server in self.servers:
            _ = self.x509_new.upload_root_certs(server)
        self.x509_new.upload_node_certs(servers=self.servers)
        self.x509_new.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509_new.upload_client_cert_settings(server=self.servers[0])
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                            self.servers[self.nodes_init:], [])

        self.wait_for_rebalance_to_complete(task)
        self.auth(servers=self.servers)
        self.validate_rbac_users_post_upgrade(self.master)
        self.validate_user_roles_sdk(self.master)

    def test_multiple_CAs_online(self):
        """
        1. Init Pre-neo cluster with x509 (single CA)
        2. Online upgrade by rebalance-in and rebalance-out
        3. Optionally convert pkcs#1 key to encrypted pkcs#8 key in mixed mode cluster
        4. After cluster is fully upgraded, rotate certs to use pkcs#8 keys and multiple CAs
        """
        self.log.info("------------- test started-------------")
        self.generate_x509_certs(self.servers)
        self.upload_x509_certs(self.servers)
        ntonencryptionBase().setup_nton_cluster(servers=self.servers,
                                                clusterEncryptionLevel="all")
        nodes_to_upgrade = self.servers[self.nodes_init:]
        nodes_to_upgrade = nodes_to_upgrade[:self.nodes_init]
        self.log.info("-------------Performing upgrade on node {0} to version------------- {1}".
                      format(nodes_to_upgrade, self.upgrade_version))
        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                             servers=nodes_to_upgrade)
        for threads in upgrade_threads:
            threads.join()

        nodes_in_cluster = self.servers[:self.nodes_init]
        old_nodes = copy.deepcopy(nodes_in_cluster)
        for node in nodes_to_upgrade:
            self.log.info("Rebalance-in a Neo node {0}".format(node))
            task = self.cluster.async_rebalance(nodes_in_cluster,
                                                [node], [],
                                                services=["kv,n1ql,cbas,eventing,fts,index"])
            self.wait_for_rebalance_to_complete(task)
            nodes_in_cluster.append(node)

            node_out = old_nodes[-1]
            self.log.info("Rebalance-out a pre-Neo node {0}".format(node_out))
            task = self.cluster.async_rebalance(nodes_in_cluster,
                                                [], [node_out])
            self.wait_for_rebalance_to_complete(task)
            old_nodes.remove(node_out)
            for enode in nodes_in_cluster:
                if enode.ip == node_out.ip:
                    nodes_in_cluster.remove(enode)

            self.master = copy.deepcopy(node)
            if self.enc_key_mixed_mode:
                self.convert_to_pkcs8(node)
                self.reload_node_cert_with_plain_password(node)

        CbServer.use_https = True
        ntonencryptionBase().setup_nton_cluster(servers=nodes_in_cluster,
                                                clusterEncryptionLevel="strict")
        self._reset_original(nodes_in_cluster)
        self.x509_new = x509main(host=self.master, standard="pkcs8",
                                 encryption_type="aes256",
                                 passphrase_type="script")
        self.x509_new.generate_multiple_x509_certs(servers=nodes_in_cluster)
        for server in nodes_in_cluster:
            _ = self.x509_new.upload_root_certs(server)
        self.x509_new.upload_node_certs(servers=nodes_in_cluster)
        self.x509_new.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509_new.upload_client_cert_settings(server=nodes_in_cluster[0])
        self.auth(servers=nodes_in_cluster)
        self.validate_rbac_users_post_upgrade(self.master)
        self.validate_user_roles_sdk(self.master)

    def test_upgrade_with_ldap_root_cert(self):
        """
        1. Setup a cluster with x509 certs with n2n encryption enabled.
        2. Start an ldap container with a root certificate.
        3. Setup an ldap client connection from CB cluster to ldap server.
        4. Create an ldap user.
        5. Upgrade the CB cluster offline.
        6. Add ldap's root cert to cluster's trusted CAs and make ldap
        client connection from CB to ldap server using cluster's trusted CAs
        7. Validate ldap user authentication works
        """
        self.log.info("------------- test started-------------")
        self.generate_x509_certs(self.servers)
        self.upload_x509_certs(self.servers)
        ntonencryptionBase().setup_nton_cluster(servers=self.servers,
                                                clusterEncryptionLevel="all")

        # setup and start ldap container
        self.log.info("Setting up ldap container")
        self.docker = LdapContainer()
        shell = RemoteMachineShellConnection(self.master)
        self.docker.start_docker(shell)
        self.docker.stop_all_containers(shell)
        self.docker.remove_all_containers(shell)
        self.docker.start_ldap_container(shell)
        shell.disconnect()

        # Setup ldap config and add an ldap user
        self.log.info("Setting up ldap config and creating ldap user")
        param = self.get_ldap_params(hosts='ldap.example.org', port='636', encryption='TLS',
                                     bindDN='cn=admin,dc=example,dc=org', bindPass="admin",
                                     serverCertValidation='false',
                                     userDNMapping='{"template":"cn=%u,dc=example,dc=org"}')
        self.setup_ldap_config(server=self.master, param=param)
        self.add_ldap_user(server=self.master, user_name=self.docker.ldap_user)

        for node in self.servers:
            self.log.info("-------------Performing upgrade on node {0} to version------------- {1}".
                          format(node, self.upgrade_version))
            upgrade_threads = self._async_update(upgrade_version=self.upgrade_version,
                                                 servers=[node])
            for threads in upgrade_threads:
                threads.join()
            self.log.info("Upgrade finished")

        # Add multiple CAs with strict level of n2n encryption
        CbServer.use_https = True
        ntonencryptionBase().setup_nton_cluster(servers=self.servers,
                                                clusterEncryptionLevel="strict")
        self._reset_original(self.servers)
        self.x509_new = x509main(host=self.master, standard="pkcs8",
                                 encryption_type="aes256",
                                 passphrase_type="script")
        self.x509_new.generate_multiple_x509_certs(servers=self.servers)
        for server in self.servers:
            _ = self.x509_new.upload_root_certs(server)
        self.x509_new.upload_node_certs(servers=self.servers)
        self.x509_new.delete_unused_out_of_the_box_CAs(server=self.master)

        # Upload ldap's root CA to cluster's trusted CAs
        self.log.info("Copying ldap CA to inbox/CA folder")
        # TODO: Change the hardcoded path (for now it's okay since the VM on which container is running is fixed )
        self.copy_file_from_slave_to_server(server=self.master, src=self.docker.ldap_ca,
                                            dst="/opt/couchbase/var/lib/couchbase/inbox/CA/ldap_container_ca.crt")
        self.log.info("Uploading ldap CA to CB")
        self.x509_new.upload_root_certs(self.master)

        self.log.info("Changing ldap config to use CB trusted CAs")
        param = self.get_ldap_params(hosts=self.docker.hostname, port=str(self.docker.ssl_port),
                                     encryption='TLS',
                                     bindDN=self.docker.bindDN, bindPass=self.docker.bindPass,
                                     serverCertValidation='true',
                                     userDNMapping='{"template":"cn=%u,dc=example,dc=org"}')
        self.setup_ldap_config(server=self.master, param=param)

        # Validate ldap user post upgrade
        ldap_rest = RestConnection(self.master)
        ldap_rest.username = self.docker.ldap_user
        ldap_rest.password = self.docker.bindPass
        status, content, response = self.make_rest_call_with_ldap_user(ldap_rest=ldap_rest)
        if not status:
            self.fail("Rest call with ldap user credentials failed with content "
                      "{0}, response{1}".format(content, response))
