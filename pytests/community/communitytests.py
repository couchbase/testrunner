import json
import time
import unittest
import urllib.request, urllib.parse, urllib.error
import testconstants
from TestInput import TestInputSingleton

from community.community_base import CommunityBaseTest
from community.community_base import CommunityXDCRBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import RebalanceFailedException
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from scripts.install import InstallerJob
from testconstants import SHERLOCK_VERSION
from testconstants import COUCHBASE_FROM_WATSON, COUCHBASE_FROM_SPOCK,\
                          COUCHBASE_FROM_VULCAN
from testconstants import WIN_BACKUP_PATH, WIN_BACKUP_C_PATH, WIN_COUCHBASE_BIN_PATH
from testconstants import LINUX_COUCHBASE_BIN_PATH




class CommunityTests(CommunityBaseTest):
    def setUp(self):
        super(CommunityTests, self).setUp()
        self.command = self.input.param("command", "")
        self.zone = self.input.param("zone", 1)
        self.replica = self.input.param("replica", 1)
        self.command_options = self.input.param("command_options", '')
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 128)
        self.shutdown_zone = self.input.param("shutdown_zone", 1)
        self.do_verify = self.input.param("do-verify", True)
        self.num_node = self.input.param("num_node", 4)
        self.services = self.input.param("services", None)
        self.start_node_services = self.input.param("start_node_services", "kv")
        self.add_node_services = self.input.param("add_node_services", "kv")
        self.timeout = 6000
        self.user_add = self.input.param("user_add", None)
        self.user_role = self.input.param("user_role", None)


    def tearDown(self):
        super(CommunityTests, self).tearDown()

    def test_disabled_zone(self):
        disabled_zone = False
        zone_name = "group1"
        serverInfo = self.servers[0]
        self.rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone name 'group1'!")
            result = self.rest.add_zone(zone_name)
            print("result  ", result)
        except Exception as e :
            if e:
                print(e)
                disabled_zone = True
                pass
        if not disabled_zone:
            self.fail("CE version should not have zone feature")

    def check_audit_available(self):
        audit_available = False
        try:
            self.rest.getAuditSettings()
            audit_available = True
        except Exception as e :
            if e:
                print(e)
        if audit_available:
            self.fail("This feature 'audit' only available on "
                      "Enterprise Edition")

    def check_ldap_available(self):
        ldap_available = False
        self.rest = RestConnection(self.master)
        try:
            s, c, h = self.rest.clearLDAPSettings()
            if s:
                ldap_available = True
        except Exception as e :
            if e:
                print(e)
        if ldap_available:
            self.fail("This feature 'ldap' only available on "
                      "Enterprise Edition")

    def check_set_services(self):
        self.rest.force_eject_node()
        self.sleep(7, "wait for node reset done")
        try:
            status = self.rest.init_node_services(hostname=self.master.ip,
                                                 services=[self.services])
        except Exception as e:
            if e:
                print(e)
        if self.services == "kv":
            if status:
                self.log.info("CE could set {0} only service."
                                  .format(self.services))
            else:
                self.fail("Failed to set {0} only service."
                                   .format(self.services))
        elif self.services == "index,kv":
            if status:
                self.fail("CE does not support kv and index on same node")
            else:
                self.log.info("services enforced in CE")
        elif self.services == "kv,n1ql":
            if status:
                self.fail("CE does not support kv and n1ql on same node")
            else:
                self.log.info("services enforced in CE")
        elif self.services == "kv,eventing":
            if status:
                self.fail("CE does not support kv and eventing on same node")
            else:
                self.log.info("services enforced in CE")
        elif self.services == "index,n1ql":
            if status:
                self.fail("CE does not support index and n1ql on same node")
            else:
                self.log.info("services enforced in CE")
        elif self.services == "index,kv,n1ql":
            if status:
                self.log.info("CE could set all services {0} on same nodes."
                                                           .format(self.services))
            else:
                self.fail("Failed to set kv, index and query services on CE")
        elif self.version[:5] in COUCHBASE_FROM_WATSON:
            if self.version[:5] in COUCHBASE_FROM_VULCAN and "eventing" in self.services:
                if status:
                    self.fail("CE does not support eventing in vulcan")
                else:
                    self.log.info("services enforced in CE")
            elif self.services == "fts,index,kv":
                if status:
                    self.fail("CE does not support fts, index and kv on same node")
                else:
                    self.log.info("services enforced in CE")
            elif self.services == "fts,index,n1ql":
                if status:
                    self.fail("CE does not support fts, index and n1ql on same node")
                else:
                    self.log.info("services enforced in CE")
            elif self.services == "fts,kv,n1ql":
                if status:
                    self.fail("CE does not support fts, kv and n1ql on same node")
                else:
                    self.log.info("services enforced in CE")
            elif self.services == "fts,index,kv,n1ql":
                if status:
                    self.log.info("CE could set all services {0} on same nodes."
                                                           .format(self.services))
                else:
                    self.fail("Failed to set "
                              "fts, index, kv, and query services on CE")
        else:
            self.fail("some services don't support")

    def check_set_services_when_add_node(self):
        self.rest.force_eject_node()
        sherlock_services_in_ce = ["kv", "index,kv,n1ql"]
        watson_services_in_ce = ["kv", "index,kv,n1ql", "fts,index,kv,n1ql"]
        self.sleep(5, "wait for node reset done")
        try:
            self.log.info("Initialize node with services {0}"
                                  .format(self.start_node_services))
            status = self.rest.init_node_services(hostname=self.master.ip,
                                        services=[self.start_node_services])
            init_node = self.cluster.async_init_node(self.master,
                                            services = [self.start_node_services])
        except Exception as e:
            if e:
                print(e)
        if not status:
            if self.version not in COUCHBASE_FROM_WATSON and \
                         self.start_node_services not in sherlock_services_in_ce:
                self.log.info("initial services setting enforced in Sherlock CE")
            elif self.version in COUCHBASE_FROM_WATSON and \
                         self.start_node_services not in watson_services_in_ce:
                self.log.info("initial services setting enforced in Watson CE")

        elif status and init_node.result() != 0:
            add_node = False
            try:
                self.log.info("node with services {0} try to add"
                                  .format(self.add_node_services))
                add_node = self.cluster.rebalance(self.servers[:2],
                                                  self.servers[1:2], [],
                                      services = [self.add_node_services])
            except Exception:
                pass
            if add_node:
                self.get_services_map()
                list_nodes = self.get_nodes_from_services_map(get_all_nodes=True)
                map = self.get_nodes_services()
                if map[self.master.ip] == self.start_node_services and \
                    map[self.servers[1].ip] == self.add_node_services:
                    self.log.info("services set correctly when node added & rebalance")
                else:
                    self.fail("services set incorrectly when node added & rebalance. "
                        "cluster expected services: {0}; set cluster services {1} ."
                        "add node expected srv: {2}; set add node srv {3}"\
                        .format(map[self.master.ip], self.start_node_services, \
                         map[self.servers[1].ip], self.add_node_services))
            else:
                if self.version not in COUCHBASE_FROM_WATSON:
                    if self.start_node_services in ["kv", "index,kv,n1ql"] and \
                          self.add_node_services not in ["kv", "index,kv,n1ql"]:
                        self.log.info("services are enforced in CE")
                    elif self.start_node_services not in ["kv", "index,kv,n1ql"]:
                        self.log.info("services are enforced in CE")
                    else:
                        self.fail("maybe bug in add node")
                elif self.version in COUCHBASE_FROM_WATSON:
                    if self.start_node_services in ["kv", "index,kv,n1ql",
                         "fts,index,kv,n1ql"] and self.add_node_services not in \
                                    ["kv", "index,kv,n1ql", "fts,index,kv,n1ql"]:
                        self.log.info("services are enforced in CE")
                    elif self.start_node_services not in ["kv", "index,kv,n1ql",
                                                            "fts,index,kv,n1ql"]:
                        self.log.info("services are enforced in CE")
                    else:
                        self.fail("maybe bug in add node")
        else:
            self.fail("maybe bug in node initialization")

    def check_full_backup_only(self):
        """ for windows vm, ask IT to put uniq.exe at
            /cygdrive/c/Program Files (x86)/ICW/bin directory """

        self.remote = RemoteMachineShellConnection(self.master)
        """ put params items=0 in test param so that init items = 0 """
        self.remote.execute_command("{0}cbworkloadgen -n {1}:8091 -j -i 1000 " \
                                    "-u Administrator -p password" \
                                            .format(self.bin_path, self.master.ip))
        """ delete backup location before run backup """
        self.remote.execute_command("rm -rf {0}*".format(self.backup_location))
        output, error = self.remote.execute_command("ls -lh {0}"
                                                     .format(self.backup_location))
        self.remote.log_command_output(output, error)

        """ first full backup """
        self.remote.execute_command("{0}cbbackup http://{1}:8091 {2} -m full " \
                                    "-u Administrator -p password"\
                                    .format(self.bin_path,
                                            self.master.ip,
                                            self.backup_c_location))
        output, error = self.remote.execute_command("ls -lh {0}*/"
                                        .format(self.backup_location))
        self.remote.log_command_output(output, error)
        output, error = self.remote.execute_command("{0}cbtransfer -u Administrator "\
                                           "-p password {1}*/*-full/ " \
                                           "stdout: | grep set | uniq | wc -l"\
                                           .format(self.bin_path,
                                                   self.backup_c_location))
        self.remote.log_command_output(output, error)
        if int(output[0]) != 1000:
            self.fail("full backup did not work in CE. "
                      "Expected 1000, actual: {0}".format(output[0]))
        self.remote.execute_command("{0}cbworkloadgen -n {1}:8091 -j -i 1000 "\
                                    " -u Administrator -p password --prefix=t_"
                                    .format(self.bin_path, self.master.ip))
        """ do different backup mode """
        self.remote.execute_command("{0}cbbackup -u Administrator -p password "\
                                    "http://{1}:8091 {2} -m {3}"\
                                    .format(self.bin_path,
                                            self.master.ip,
                                            self.backup_c_location,
                                            self.backup_option))
        output, error = self.remote.execute_command("ls -lh {0}"
                                                     .format(self.backup_location))
        self.remote.log_command_output(output, error)
        output, error = self.remote.execute_command("{0}cbtransfer -u Administrator "\
                                           "-p password {1}*/*-{2}/ stdout: "\
                                           "| grep set | uniq | wc -l"\
                                           .format(self.bin_path,
                                                   self.backup_c_location,
                                                   self.backup_option))
        self.remote.log_command_output(output, error)
        if int(output[0]) == 2000:
            self.log.info("backup option 'diff' is enforced in CE")
        elif int(output[0]) == 1000:
            self.fail("backup option 'diff' is not enforced in CE. "
                      "Expected 2000, actual: {0}".format(output[0]))
        else:
            self.fail("backup failed to backup correct items")
        self.remote.disconnect()

    def check_ent_backup(self):
        """ for CE version from Watson, cbbackupmgr exe file should not in bin """
        command = "cbbackupmgr"
        self.remote = RemoteMachineShellConnection(self.master)
        self.log.info("check if {0} in {1} directory".format(command, self.bin_path))
        found = self.remote.file_exists(self.bin_path, command)
        if found:
            self.log.info("found {0} in {1} directory".format(command, self.bin_path))
            self.fail("CE from Watson should not contain {0}".format(command))
        elif not found:
            self.log.info("Ent. backup in CE is enforced, not in bin!")
        self.remote.disconnect()

    def check_memory_optimized_storage_mode(self):
        """ from Watson, CE should not have option 'memory_optimized' to set """
        self.rest.force_eject_node()
        self.sleep(5, "wait for node reset done")
        try:
            self.log.info("Initialize node with 'Memory Optimized' option")
            status = self.rest.set_indexer_storage_mode(
                            username=self.input.membase_settings.rest_username,
                            password=self.input.membase_settings.rest_password,
                                                storageMode='memory_optimized')
        except Exception as ex:
            if ex:
                print(ex)
        if not status:
            self.log.info("Memory Optimized setting enforced in CE "
                          "Could not set memory_optimized option")
        else:
            self.fail("Memory Optimzed setting does not enforced in CE "
                      "We could set this option in")

    def check_x509_cert(self):
        """ from Watson, X509 certificate only support in EE """
        api = self.rest.baseUrl + "pools/default/certificate?extended=true"
        self.log.info("request to get certificate at "
                      "'pools/default/certificate?extended=true' "
                      "should return False")
        try:
            status, content, header = self.rest._http_request(api, 'GET')
        except Exception as ex:
            if ex:
                print(ex)
        if status:
            self.fail("This X509 certificate feature only available in EE")
        elif not status:
            if "requires enterprise edition" in content:
                self.log.info("X509 cert is enforced in CE")

    def check_roles_base_access(self):
        """ from Watson, roles base access for admin should not in in CE """
        if self.user_add is None:
            self.fail("We need to pass user name (user_add) to run this test. ")
        if self.user_role is None:
            self.fail("We need to pass user roles (user_role) to run this test. ")
        api = self.rest.baseUrl + "settings/rbac/users/" + self.user_add
        self.log.info("url to run this test: %s" % api)
        """ add admin user """
        param = "name=%s&roles=%s" % (self.user_add, self.user_role)
        try:
            status, content, header = self.rest._http_request(api, 'PUT', param)
        except Exception as ex:
            if ex:
                print(ex)
        if status:
            self.fail("CE should not allow to add admin users")
        else:
            self.log.info("roles base is enforced in CE! ")

    def check_root_certificate(self):
        """ from watson, ce should not see root certificate
            manual test:
            curl -u Administrator:password -X GET
                            http://localhost:8091/pools/default/certificate """
        api = self.rest.baseUrl + "pools/default/certificate"
        try:
            status, content, header = self.rest._http_request(api, 'GET')
        except Exception as ex:
            if ex:
                print(ex)
        if status:
            self.fail("CE should not see root certificate!")
        elif "requires enterprise edition" in content:
            self.log.info("root certificate is enforced in CE! ")

    def check_settings_audit(self):
        """ from watson, ce should not set audit
            manual test:
            curl -u Administrator:password -X GET
                            http://localhost:8091/settings/audit """
        api = self.rest.baseUrl + "settings/audit"
        try:
            status, content, header = self.rest._http_request(api, 'GET')
        except Exception as ex:
            if ex:
                print(ex)
        if status:
            self.fail("CE should not allow to set audit !")
        elif "requires enterprise edition" in content:
            self.log.info("settings audit is enforced in CE! ")

    def check_infer(self):
        """ from watson, ce should not see infer
            manual test:
            curl -H "Content-Type: application/json" -X POST
                 -d '{"statement":"infer `bucket_name`;"}'
                       http://localhost:8093/query/service
            test params: new_services=kv-index-n1ql,default_bucket=False """
        self.rest.force_eject_node()
        self.sleep(7, "wait for node reset done")
        self.rest.init_node()
        bucket = "default"
        self.rest.create_bucket(bucket, ramQuotaMB=200)
        api = self.rest.query_baseUrl + "query/service"
        param = urllib.parse.urlencode({"statement":"infer `%s` ;" % bucket})
        try:
            status, content, header = self.rest._http_request(api, 'POST', param)
            json_parsed = json.loads(content)
        except Exception as ex:
            if ex:
                print(ex)
        if json_parsed["status"] == "success":
            self.fail("CE should not allow to run INFER !")
        elif json_parsed["status"] == "fatal":
            self.log.info("INFER is enforced in CE! ")

    def check_auto_complete(self):
        """ this feature has not complete to block in CE """

    """ Check new features from spock start here """
    def check_cbbackupmgr(self):
        """ cbbackupmgr should not available in CE from spock """
        if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
            file_name = "cbbackupmgr" + self.file_extension
            self.log.info("check if cbbackupmgr in bin dir in CE")
            result = self.remote.file_exists(self.bin_path, file_name)
            if result:
                self.fail("cbbackupmgr should not in bin dir of CE")
            else:
                self.log.info("cbbackupmgr is enforced in CE")
        self.remote.disconnect()

    def test_max_ttl_bucket(self):
        """
            From vulcan, EE bucket has has an option to set --max-ttl, not it CE.
            This test is make sure CE could not create bucket with option --max-ttl
            This test must pass default_bucket=False
        """
        if self.cb_version[:5] not in COUCHBASE_FROM_VULCAN:
            self.log.info("This test only for vulcan and later")
            return
        cmd = 'curl -X POST -u Administrator:password \
                                    http://{0}:8091/pools/default/buckets \
                                 -d name=bucket0 \
                                 -d maxTTL=100 \
                                 -d authType=sasl \
                                 -d ramQuotaMB=100 '.format(self.master.ip)
        if self.cli_test:
            cmd = "{0}couchbase-cli bucket-create -c {1}:8091 --username Administrator \
                --password password --bucket bucket0 --bucket-type couchbase \
                --bucket-ramsize 512 --bucket-replica 1 --bucket-priority high \
                --bucket-eviction-policy fullEviction --enable-flush 0 \
                --enable-index-replica 1 --max-ttl 200".format(self.bin_path,
                                                               self.master.ip)
        conn = RemoteMachineShellConnection(self.master)
        output, error = conn.execute_command(cmd)
        conn.log_command_output(output, error)
        mesg = "Max TTL is supported in enterprise edition only"
        if self.cli_test:
            mesg = "Maximum TTL can only be configured on enterprise edition"
        if output and mesg not in str(output[0]):
            self.fail("max ttl feature should not in Community Edition")
        buckets = RestConnection(self.master).get_buckets()
        if buckets:
            for bucket in buckets:
                self.log.info("bucekt in cluser: {0}".format(bucket.name))
                if bucket.name == "bucket0":
                    self.fail("Failed to enforce feature max ttl in CE.")
        conn.disconnect()

    def test_setting_audit(self):
        """
           CE does not allow to set audit from vulcan 5.5.0
        """
        if self.cb_version[:5] not in COUCHBASE_FROM_VULCAN:
            self.log.info("This test only for vulcan and later")
            return
        cmd = 'curl -X POST -u Administrator:password \
              http://{0}:8091/settings/audit \
              -d auditdEnabled=true '.format(self.master.ip)
        if self.cli_test:
            cmd = "{0}couchbase-cli setting-audit -c {1}:8091 -u Administrator \
                -p password --audit-enabled 1 --audit-log-rotate-interval 604800 \
                --audit-log-path /opt/couchbase/var/lib/couchbase/logs "\
                .format(self.bin_path, self.master.ip)

        conn = RemoteMachineShellConnection(self.master)
        output, error = conn.execute_command(cmd)
        conn.log_command_output(output, error)
        mesg = "This http API endpoint requires enterprise edition"
        if output and mesg not in str(output[0]):
            self.fail("setting-audit feature should not in Community Edition")
        conn.disconnect()

    def test_setting_autofailover_enterprise_only(self):
        """
           CE does not allow set auto failover if disk has issue
           and failover group from vulcan 5.5.0
        """
        if self.cb_version[:5] not in COUCHBASE_FROM_VULCAN:
            self.log.info("This test only for vulcan and later")
            return
        self.failover_disk_period = self.input.param("failover_disk_period", False)
        self.failover_server_group = self.input.param("failover_server_group", False)

        failover_disk_period = ""
        if self.failover_disk_period:
            if self.cli_test:
                failover_disk_period = "--failover-data-disk-period 300"
            else:
                failover_disk_period = "-d failoverOnDataDiskIssues[timePeriod]=300"
        failover_server_group = ""
        if self.failover_server_group and self.cli_test:
            failover_server_group = "--enable-failover-of-server-group 1"


        cmd = 'curl -X POST -u Administrator:password \
              http://{0}:8091/settings/autoFailover -d enabled=true -d timeout=120 \
              -d maxCount=1 \
              -d failoverOnDataDiskIssues[enabled]=true {1} \
              -d failoverServerGroup={2}'.format(self.master.ip, failover_disk_period,
                                                 self.failover_server_group)
        if self.cli_test:
            cmd = "{0}couchbase-cli setting-autofailover -c {1}:8091 \
                   -u Administrator -p password \
                   --enable-failover-on-data-disk-issues 1 {2} {3} "\
                  .format(self.bin_path, self.master.ip,
                          failover_disk_period,
                          failover_server_group)
        conn = RemoteMachineShellConnection(self.master)
        output, error = conn.execute_command(cmd)
        conn.log_command_output(output, error)
        mesg = "Auto failover on Data Service disk issues can only be " + \
               "configured on enterprise edition"
        if not self.cli_test:
            if self.failover_disk_period or \
                                   self.failover_server_group:
                if output and not error:
                    self.fail("setting autofailover disk issues feature\
                               should not in Community Edition")
        else:
            if self.failover_server_group:
                mesg = "--enable-failover-of-server-groups can only be " + \
                       "configured on enterprise edition"

        if output and mesg not in str(output[0]):
            self.fail("Setting EE autofailover features \
                       should not in Community Edition")
        else:
            self.log.info("EE setting autofailover are disable in CE")
        conn.disconnect()

    def test_set_bucket_compression(self):
        """
           CE does not allow to set bucket compression to bucket
           from vulcan 5.5.0.   Mode compression: off,active,passive
           Note: must set defaultbucket=False for this test
        """
        if self.cb_version[:5] not in COUCHBASE_FROM_VULCAN:
            self.log.info("This test only for vulcan and later")
            return
        self.compression_mode = self.input.param("compression_mode", "off")
        cmd = 'curl -X POST -u Administrator:password \
                                    http://{0}:8091/pools/default/buckets \
                                 -d name=bucket0 \
                                 -d compressionMode={1} \
                                 -d authType=sasl \
                                 -d ramQuotaMB=100 '.format(self.master.ip,
                                                            self.compression_mode)
        if self.cli_test:
            cmd = "{0}couchbase-cli bucket-create -c {1}:8091 --username Administrator \
                --password password --bucket bucket0 --bucket-type couchbase \
                --bucket-ramsize 512 --bucket-replica 1 --bucket-priority high \
                --bucket-eviction-policy fullEviction --enable-flush 0 \
                --enable-index-replica 1 --compression-mode {2}".format(self.bin_path,
                                                                 self.master.ip,
                                                                 self.compression_mode)
        conn = RemoteMachineShellConnection(self.master)
        output, error = conn.execute_command(cmd)
        conn.log_command_output(output, error)
        mesg = "Compression mode is supported in enterprise edition only"
        if self.cli_test:
            mesg = "Compression mode can only be configured on enterprise edition"
        if output and mesg not in str(output[0]):
            self.fail("Setting bucket compression should not in CE")
        conn.disconnect()


class CommunityXDCRTests(CommunityXDCRBaseTest):
    def setUp(self):
        super(CommunityXDCRTests, self).setUp()
        self.master = self._servers[0]
        self.cli_test = self._input.param("cli_test", False)
        self.bin_path = LINUX_COUCHBASE_BIN_PATH
        remote = RemoteMachineShellConnection(self.master)
        type = remote.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            self.is_linux = False
            self.backup_location = WIN_BACKUP_PATH
            self.backup_c_location = WIN_BACKUP_C_PATH
            self.bin_path = WIN_COUCHBASE_BIN_PATH
            self.file_extension = ".exe"
        else:
            self.is_linux = True
        self.cb_version = None
        if RestHelper(RestConnection(self.master)).is_ns_server_running():
            """ since every new couchbase version, there will be new features
                that test code will not work on previous release.  So we need
                to get couchbase version to filter out those tests. """
            self.cb_version = RestConnection(self.master).get_nodes_version()
        remote.disconnect()

    def tearDown(self):
        super(CommunityXDCRTests, self).tearDown()

    def test_xdcr_filter(self):
        filter_on = False
        serverInfo = self._servers[0]
        self.rest = RestConnection(serverInfo)
        self.rest.remove_all_replications()
        self.remote = RemoteMachineShellConnection(serverInfo)
        output, error = self.remote.execute_command('curl -X POST '
                                         '-u Administrator:password '
                     ' http://{0}:8091/controller/createReplication '
                     '-d fromBucket="default" '
                     '-d toCluster="cluster1" '
                     '-d toBucket="default" '
                     '-d replicationType="continuous" '
                     '-d filterExpression="some_exp"'
                                              .format(serverInfo.ip))
        if output:
            self.log.info(output[0])
        if output and "default" in output[0]:
            self.fail("XDCR Filter feature should not available in "
                      "Community Edition")
        self.remote.disconnect()

    def test_lww(self):
        server = self._servers[0]
        conn = RemoteMachineShellConnection(server)
        output, error = conn.execute_command('curl -X POST -u Administrator:password '
                                                    'http://{0}:8091/pools/default/buckets '
                                                    '-d name=default '
                                                    '-d conflictResolutionType=lww '
                                                    '-d authType=sasl '
                                                    '-d ramQuotaMB=100 '.format(server.ip))
        conn.log_command_output(output, error)
        if output and "Conflict resolution type 'lww' is supported only in enterprise edition"\
                  not in str(output[0]):
            self.fail("XDCR LWW feature should not be available in Community Edition")
        self.log.info("XDCR LWW feature not available in Community Edition as expected")
        conn.disconnect()

    def test_xdcr_compression(self):
        """
           flag --enable-compression should not work in CE from vulcan
        """
        self.compression_mode = self._input.param("compression_mode", "Auto")
        if self.cb_version[:5] not in COUCHBASE_FROM_VULCAN:
            self.log.info("This test only for vulcan and later")
            return
        self.log.info("Remove any existing replican")
        RestConnection(self.src_master).remove_all_replications()
        conn = RemoteMachineShellConnection(self.src_master)

        cmd = 'curl -X POST -u Administrator:password \
                              http://{0}:8091/controller/createReplication \
                                 -d fromBucket=default \
                                 -d toBucket=default \
                                 -d toCluster=cluster1 \
                                 -d replicationType=continuous \
                                 -d compressionType={1}'.format(self.src_master.ip,
                                                            self.compression_mode)
        if self.cli_test:
            cmd = "{0}couchbase-cli setting-xdcr -c {1}:8091 -u Administrator \
                   -p password --enable-compression 1 ".format(self.bin_path,
                                                                 self.src_master.ip)
        output, error = conn.execute_command(cmd)

        mesg = '"compressionType":"The value can be specified only in enterprise edition"'
        if self.cli_test:
            mesg = "ERROR: --enable-compression can only be configured on enterprise edition"
        if output and mesg not in str(output[0]):
            self.fail("Setting bucket compression should not in CE")
        conn.disconnect()