import copy
import json
import logging
import random
import string

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509_multiple_CA_util import x509main, Validation
from pytests.upgrade.newupgradebasetest import NewUpgradeBaseTest

log = logging.getLogger()


class MultipleCAUpgrade(NewUpgradeBaseTest):
    def setUp(self):
        self.test_setup_finished = False
        log.info("==============  Multiple CA Upgrade setup has started ==============")
        super(MultipleCAUpgrade, self).setUp()
        self.initial_version = self.input.param("initial_version", '6.6.3-9799')
        self.upgrade_version = self.input.param("upgrade_version", "7.1.0-1745")
        self.enc_key_mixed_mode = self.input.param("enc_key_mixed_mode", False)
        if self.enc_key_mixed_mode in ["True", True]:
            self.enc_key_mixed_mode = True
        else:
            self.enc_key_mixed_mode = False
        self.load_sample_bucket(self.master, "travel-sample")
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
        self.add_rbac_groups_roles(self.master)
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
        rest = RestConnection(server)
        versions = rest.get_nodes_versions()
        if versions[0] < "7.0":
            group_roles = ['admin', 'cluster_admin', 'security_admin', 'ro_admin',
                           'bucket_admin[travel-sample]', 'bucket_full_access[travel-sample]',
                           'data_reader[travel-sample]', 'data_writer[travel-sample]', 'data_dcp_reader[travel-sample]', 'data_backup[travel-sample]', 'data_monitoring[travel-sample]']
        else:
            group_roles = ['admin', 'cluster_admin', 'security_admin_local', 'security_admin_external', 'ro_admin',
                           'bucket_admin[travel-sample]', 'bucket_full_access[travel-sample]',
                           'data_reader[travel-sample:_default:_default]', 'data_writer[travel-sample:_default:_default]', 'data_dcp_reader[travel-sample:_default:_default]', 'data_monitoring[travel-sample:_default:_default]',
                           'data_backup[travel-sample]']
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
