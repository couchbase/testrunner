import os

from remote.remote_util import RemoteMachineShellConnection
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection, RestHelper
from testconstants import LOG_FILE_NAMES
from couchbase_helper.document import View
from security.rbac_base import RbacBase
from collection.collections_stats import CollectionsStats


class CouchbaseCliTestWithCollections(CliBaseTest):
    def setUp(self):
        #TestInputSingleton.input.test_params["default_bucket"] = False
        super(CouchbaseCliTestWithCollections, self).setUp()

    def tearDown(self):
        super(CouchbaseCliTestWithCollections, self).tearDown()

    def test_cbstats_with_collection_status(self):
        if self.custom_scopes:
            self.create_scope_cluster_host()
        if self.custom_collections:
            self.create_collection(self.col_per_scope)
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%scbstats%s %s:11210 -u Administrator -p password -b %s all | grep collecion "\
                                  % (self.cli_command_path, self.cmd_ext,
                                     self.master.ip, "default")
        output, error = shell.execute_command(cmd)
        """ check collection enable """
        for x in output:
            if x.replace(" ", "") != "ep_collections_enabled:true":
                raise Exception("collection does not enable by default")

    def test_cbstats_with_collection(self):
        """
        1. Creates a bucket on the cluster
        2. Create scopes
        3. Create collections
        4. Verify scopes and collections with cbstats
        """
        if self.custom_scopes:
            self.create_scope()
        if self.custom_collections:
            self.create_collection(self.col_per_scope)
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%scbstats%s %s:11210 -u Administrator -p password -b %s "\
                                  % (self.cli_command_path, self.cmd_ext,
                                     self.master.ip, "default")
        if self.json_output:
            cmd += " -j "
        if self.check_scopes:
            cmd += " scopes"
        if self.check_scopes_details:
            cmd += " scopes-details"
        if self.check_collections:
            cmd += " collections "
        if self.check_collections_details:
            cmd += " collections-details "
        output, error = shell.execute_command(cmd)
        if not self.custom_scopes:
            if not self._check_output("_default", output):
                raise Exception("No _default scope in cluster")
        else:
            custom_scopes = self.get_bucket_scope()
            if not custom_scopes:
                self.sleep(10)
                custom_scopes = self.get_bucket_scope()
            if isinstance(custom_scopes, tuple):
                custom_scopes = custom_scopes[0]
            for scope in custom_scopes:
                if scope[:4] == "\x1b[6n":
                    scope = scope[4:]
                if not self._check_output(scope, output):
                    raise Exception("No scope: {0} in cluster".format(scope))
        if not self.custom_collections:
            if not self._check_output("_default", output):
                raise Exception("No _default collection in cluster")
        else:
            custom_collections = self.get_bucket_collection()
            if isinstance(custom_collections, tuple):
                custom_collections = self._extract_collection_names(custom_collections)
            for collection in custom_collections:
                if collection[:4] == "\x1b[6n":
                    collection = collection[4:]
                if not self._check_output(collection, output):
                    raise Exception("No collection: {0} in cluster".format(scope))

    def test_cbworkloadgen_with_collection(self):
        """
        1. Creates a bucket on the cluster
        2. Create scopes
        3. Create collections
        4. Run cbworkloadgen with option -c
        """
        if self.custom_scopes:
            self.create_scope()
        if self.custom_collections:
            self.create_collection(self.col_per_scope)
        scopes = self.get_bucket_scope()
        if scopes[0][:4] == "\x1b[6n":
            scopes[0] = scopes[0][4:]
        scopes_id = []
        for scope in scopes:
            if (scope[4:] == "_default" or scope == "_default") and self.custom_scopes:
                scopes.remove(scope)
                continue
            self.log.info("get scope id of scope: {0}".format(scope))
            scopes_id.append(self.get_scopes_id(scope))
        collections = self.get_bucket_collection()
        if isinstance(collections, tuple):
            remove_list = []
            collections = collections[0]
            for i in range(len(collections)):
                collections[i] = collections[i].replace("-", "")
                if ":" in collections[i]:
                    remove_list.append(collections[i])
            for item in remove_list:
                collections.remove(item)
        collections_id = []
        for collection in collections:
            if collection == "_default" and self.custom_collections:
                collections.remove(collection)
                continue
            collections_id.append(self.get_collections_id(scopes[0],collection))

        shell = RemoteMachineShellConnection(self.master)
        cmd = "%scbworkloadgen%s -n %s:8091 -u Administrator -p password -b %s "\
                                  % (self.cli_command_path, self.cmd_ext,
                                     self.master.ip, "default")
        if self.load_json_format:
            cmd += " -j "
        if self.cbwg_no_value_in_col_flag:
            cmd += " -c "
        if self.cbwg_no_value_in_col_flag:
            cmd += " -c  1234 "
        if self.load_to_default_scopes:
            cmd += " -c 0x0 "
        if self.load_to_scopes:
            cmd += " -c {0} ".format(scopes_id[0])
        if self.load_to_default_collections:
            cmd += " -c 0x0 "
        if self.load_to_collections:
            cmd += " -c {0} ".format(collections_id[0])
        output, error = shell.execute_command(cmd, use_channel=True)
        if error or "Error" in output:
            if (not self.cbwg_no_value_in_col_flag or not self.cbwg_invalid_value_in_col_flag)\
                and not self.should_fail:
                raise Exception("Failed to load data to cluster using cbworkloadgen")

    def test_create_sc_with_existing_sc(self):
        bucket_name = self.buckets[0].name
        self.create_scope()
        self.create_collection(self.col_per_scope)
        scopes = self.get_bucket_scope()
        for scope in scopes:
            if scope == "_default":
                scopes.remove(scope)
        collections = self.get_bucket_collection()
        for collection in collections:
            if collection == "_default":
                collections.remove(collection)
        scope_name = scopes[0]
        if self.create_existing_scope:
            if self.block_char:
                scope_name = "{0}testting".format(self.block_char)
            try:
                if self.use_rest:
                    self.rest.create_scope(bucket=bucket_name, scope=scope_name,
                                           params=None, num_retries=1)
                else:
                    self.cli_col.create_scope(bucket=bucket_name, scope=scope_name)
            except Exception as e:
                error_expected = False
                errors = ["already exists",
                          "First character must not be"]
                for error in errors:
                    if error in str(e):
                        error_expected = True
                if not error_expected:
                    raise Exception("Rest failed to block create scope with same name. Error: {0}"\
                                                                          .format(str(e)))
        if self.create_existing_collection:
            collection_name = collections[0]
            if self.block_char:
                collection_name = "{0}testting".format(self.block_char)
            try:
                if self.use_rest:
                    self.rest.create_collection(bucket=bucket_name, scope=scope_name,
                                                collection="mycollection_{0}_0".format(scope_name),
                                                params=None, num_retries=1)
                else:
                    self.cli_col.create_collection(bucket=bucket_name, scope=scope_name,
                                                   collection="mycollection_{0}_0".format(scope_name))
            except Exception as e:
                error_expected = False
                errors = ["already exists",
                          "First character must not be"]
                for error in errors:
                    if error in str(e):
                        error_expected = True
                if not error_expected:
                    raise Exception("Rest failed to block create collection with same name")

    def test_drop_sc(self):
        bucket_name = self.buckets[0].name
        self.create_scope()
        self.create_collection(self.col_per_scope)
        scopes = self.get_bucket_scope()
        for scope in scopes:
            if scope == "_default":
                scopes.remove(scope)
        collections = self.get_bucket_collection()
        for collection in collections:
            if collection == "_default":
                collections.remove(collection)
        scope_name = scopes[0]
        collection_name = collections[0]
        if self.drop_sc_default:
            scope_name = collection_name = "_default"
        if self.drop_collections:
            try:
                if self.use_rest:
                    self.rest_col.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                    collection="_{0}".format(bucket_name))
                else:
                    self.cli_col.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                  collection="_{0}".format(bucket_name))
            except Exception as e:
                if error in str(e):
                    raise Exception("Failed to drop collection with error: {0}".format(str(e)))
        if self.drop_scopes:
            try:
                if self.use_rest:
                    self.rest_col.delete_scope(bucket_name, scope_name)
                else:
                    self.cli_col.delete_scope(scope_name, bucket=bucket_name)
            except Exception as e:
                if error in str(e):
                    raise Exception("Failed to drop scope with error: {0}".format(str(e)))

    def test_drop_non_exist_sc(self):
        bucket_name = self.buckets[0].name
        scope_name = "scope123"
        collection_name = "collections123"
        if self.drop_collections:
            try:
                if self.use_rest:
                    self.rest_col.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                    collection="_{0}".format(bucket_name))
                else:
                    self.cli_col.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                  collection="_{0}".format(bucket_name))
            except Exception as e:
                if error in str(e):
                    self.log.info("Test failed as expected.")
        if self.drop_scopes:
            try:
                if self.use_rest:
                    self.rest_col.delete_scope(bucket_name, scope_name)
                else:
                    self.cli_col.delete_scope(scope_name, bucket=bucket_name)
            except Exception as e:
                if error in str(e):
                    self.log.info("Test failed as expected.")

    def test_cbcollectinfo_with_collection(self):
        """
            Load data to collection and run cbcollect_info
        """
        self.test_cbworkloadgen_with_collection()
        self.shell.delete_files("%s.zip" % (self.log_filename))
        """ This is the folder generated after unzip the log package """
        self.shell.delete_files("cbcollect_info*")

        cb_server_started = False
        if self.node_down:
            """ set autofailover to off """
            rest = RestConnection(self.master)
            rest.update_autofailover_settings(False, 60)
            if self.os == 'linux':
                output, error = self.shell.execute_command(
                                    "killall -9 memcached & killall -9 beam.smp")
        output, error = self.shell.execute_cbcollect_info("%s.zip"
                                                           % (self.log_filename))

        if self.os != "windows":
            if len(error) > 0:
                if self.node_down:
                    shell = RemoteMachineShellConnection(self.master)
                    shell.start_server()
                    self.sleep(15)
                    shell.disconnect()
                raise Exception("Command throw out error: %s " % error)

            for output_line in output:
                if output_line.find("ERROR") >= 0 or output_line.find("Error") >= 0:
                    if "from http endpoint" in output_line.lower():
                        continue

                    """ remove this code when bug in MB-45867 is fixed """
                    if "error occurred getting server guts" in output_line.lower() or \
                       "error: unable to retrieve statistics" in output_line.lower():
                        continue
                    """ *************************** """

                    if self.node_down:
                        shell = RemoteMachineShellConnection(self.master)
                        shell.start_server()
                        self.sleep(15)
                        shell.disconnect()
                    raise Exception("Command throw out error: %s " % output_line)
        try:
            if self.node_down:
                if self.os == 'linux':
                    self.shell.start_server()
                    rest = RestConnection(self.master)
                    if RestHelper(rest).is_ns_server_running(timeout_in_seconds=60):
                        cb_server_started = True
                    else:
                        self.fail("CB server failed to start")
            self.verify_results(self.log_filename)
        finally:
            if self.node_down and not cb_server_started:
                if self.os == 'linux':
                    self.shell.start_server()
                    rest = RestConnection(self.master)
                    if not RestHelper(rest).is_ns_server_running(timeout_in_seconds=60):
                        self.fail("CB server failed to start")

    def test_view_cbcollectinfo_with_collection(self):
        self.test_cbworkloadgen_with_collection()
        self.default_design_doc_name = "Doc1"
        self.view_name = self.input.param("view_name", "View")
        self.generate_map_reduce_error = self.input.param("map_reduce_error", False)
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.reduce_fn = "_count"
        expected_num_items = self.num_items
        if self.generate_map_reduce_error:
            self.reduce_fn = "_sum"
            expected_num_items = None

        view = View(self.view_name, self.default_map_func, self.reduce_fn, dev_view=False)
        self.cluster.create_view(self.master, self.default_design_doc_name, view,
                                 'default', self.wait_timeout * 2)
        query = {"stale": "false", "connection_timeout": 60000}
        try:
            self.cluster.query_view(self.master, self.default_design_doc_name, self.view_name, query,
                                expected_num_items, 'default', timeout=self.wait_timeout)
        except Exception as ex:
            if not self.generate_map_reduce_error:
                raise ex
        self.shell.execute_cbcollect_info("%s.zip" % (self.log_filename))
        self.verify_results(self.log_filename)

    def verify_results(self, output_file_name):
        try:
            os = "linux"
            zip_file = "%s.zip" % (output_file_name)
            info = self.shell.extract_remote_info()
            type = info.type.lower()
            if type == 'windows':
                os = "windows"

            if os == "linux":
                os_dist = self.shell.info.distribution_version.replace(" ", "").lower()
                if "debian" in os_dist:
                    self.shell.execute_command("apt update -y && apt install -y unzip")
                if "centos" in os_dist:
                    self.shell.execute_command("yum install -y unzip")
                command = "unzip %s" % (zip_file)
                output, error = self.shell.execute_command(command)
                self.sleep(2)
                if self.debug_logs:
                    self.shell.log_command_output(output, error)
                if len(error) > 0:
                    raise Exception("unable to unzip the files. Check unzip command output for help")

                command = "ls cbcollect_info*/"
                output, error = self.shell.execute_command(command)
                if self.debug_logs:
                    self.shell.log_command_output(output, error)
                if len(error) > 0:
                    raise Exception("unable to list the files. Check ls command output for help")
                missing_logs = False
                nodes_services = RestConnection(self.master).get_nodes_services()
                for node, services in list(nodes_services.items()):
                    for service in services:
                        if service.encode("ascii") == "fts" and \
                                     self.master.ip in node and \
                                    "fts_diag.json" not in LOG_FILE_NAMES:
                            LOG_FILE_NAMES.append("fts_diag.json")
                        if service.encode("ascii") == "index" and \
                                            self.master.ip in node:
                            if "indexer_mprof.log" not in LOG_FILE_NAMES:
                                LOG_FILE_NAMES.append("indexer_mprof.log")
                            if "indexer_pprof.log" not in LOG_FILE_NAMES:
                                LOG_FILE_NAMES.append("indexer_pprof.log")
                if self.debug_logs:
                    self.log.info('\nlog files sample: {0}'.format(LOG_FILE_NAMES))
                    self.log.info('\nlog files in zip: {0}'.format(output))

                for x in LOG_FILE_NAMES:
                    find_log = False
                    for output_line in output:
                        if output_line.find(x) >= 0:
                            find_log = True
                    if not find_log:
                        # missing syslog.tar.gz in mac as in ticket MB-9110
                        # need to remove 3 lines below if it is fixed in 2.2.1
                        # in mac os
                        if x == "syslog.tar.gz" and info.distribution_type.lower() == "mac":
                            missing_logs = False
                        else:
                            missing_logs = True
                            self.log.error("The log zip file miss %s" % (x))

                missing_buckets = False
                if not self.node_down:
                    for bucket in self.buckets:
                        command = "grep %s cbcollect_info*/stats.log" % (bucket.name)
                        output, error = self.shell.execute_command(command)
                        if self.debug_logs:
                            self.shell.log_command_output(output, error)
                        if len(error) > 0:
                            raise Exception("unable to grep key words. Check grep command output for help")
                        if len(output) == 0:
                            missing_buckets = True
                            self.log.error("%s stats are missed in stats.log" % (bucket.name))

                command = "du -s cbcollect_info*/*"
                output, error = self.shell.execute_command(command)
                if self.debug_logs:
                    self.shell.log_command_output(output, error)
                empty_logs = False
                if len(error) > 0:
                    raise Exception("unable to list file size. Check du command output for help")
                for output_line in output:
                    output_line = output_line.split()
                    file_size = int(output_line[0])
                    if "dist_cfg" in output_line[1]:
                        continue
                    if self.debug_logs:
                        print(("File size: ", file_size))
                    if file_size == 0:
                        if "kv_trace" in output_line[1] and self.node_down:
                            continue
                        else:
                            empty_logs = True
                            self.log.error("%s is empty" % (output_line[1]))

                if missing_logs:
                    raise Exception("Bad log file package generated. Missing logs")
                if missing_buckets:
                    raise Exception("Bad stats.log which miss some bucket information")
                if empty_logs:
                    raise Exception("Collect empty log files")
            elif os == "windows":
                # try to figure out what command works for windows for verification
                pass

        finally:
            self.shell.delete_files(zip_file)
            self.shell.delete_files("cbcollect_info*")


class XdcrCLITest(CliBaseTest):
    XDCR_SETUP_SUCCESS = {
                          "create": "SUCCESS: init/edit CLUSTERNAME",
                          "edit": "SUCCESS: init/edit CLUSTERNAME",
                          "delete": "SUCCESS: delete CLUSTERNAME"
    }
    XDCR_REPLICATE_SUCCESS = {
                          "create": "SUCCESS: start replication",
                          "delete": "SUCCESS: delete replication",
                          "pause": "SUCCESS: pause replication",
                          "resume": "SUCCESS: resume replication"
    }
    SSL_MANAGE_SUCCESS = {'retrieve': "SUCCESS: retrieve certificate to \'PATH\'",
                           'regenerate': "SUCCESS: regenerate certificate to \'PATH\'"
                           }

    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(XdcrCLITest, self).setUp()
        self.__user = "Administrator"
        self.__password = "password"
        self.log.info("===== Setup destination cluster =====")
        if self.dest_nodes and len(self.dest_nodes) > 1:
            self.cluster.async_rebalance(self.dest_nodes, self.dest_nodes[1:],
                                                                 []).result()

        XdcrCLITest.XDCR_SETUP_SUCCESS = {
                      "create": "SUCCESS: Cluster reference created",
                      "edit": "SUCCESS: Cluster reference edited",
                      "delete": "SUCCESS: Cluster reference deleted"
        }
        XdcrCLITest.XDCR_REPLICATE_SUCCESS = {
                      "create": "SUCCESS: XDCR replication created",
                      "delete": "SUCCESS: XDCR replication deleted",
                      "pause": "SUCCESS: XDCR replication paused",
                      "resume": "SUCCESS: XDCR replication resume"
        }
        XdcrCLITest.SSL_MANAGE_SUCCESS = \
                      {'retrieve': "SUCCESS: retrieve certificate to \'PATH\'",
                       'regenerate': 'SUCCESS: Certificate regenerate and copied to `PATH`'
                      }

    def tearDown(self):
        for server in self.servers:
            try:
                rest = RestConnection(server)
                rest.remove_all_replications()
                rest.remove_all_remote_clusters()
                rest.remove_all_recoveries()
            except Exception as e:
                if e:
                    print("Error: ", str(e))
        super(XdcrCLITest, self).tearDown()

    def __execute_cli(self, cli_command, options, cluster_host="localhost"):
        return self.shell.execute_couchbase_cli(
                                                cli_command=cli_command,
                                                options=options,
                                                cluster_host=cluster_host,
                                                user=self.__user,
                                                password=self.__password)

    def __xdcr_setup_create(self):
        # xdcr_hostname=the number of server in ini file to add to master as replication
        xdcr_cluster_name = self.input.param("xdcr-cluster-name", None)
        xdcr_hostname = self.input.param("xdcr-hostname", None)
        xdcr_username = self.input.param("xdcr-username", None)
        xdcr_password = self.input.param("xdcr-password", None)
        secure_connection = self.input.param("secure-connection", "none")
        xdcr_cert = self.input.param("xdcr-certificate", None)
        wrong_cert = self.input.param("wrong-certificate", None)

        cli_command = "xdcr-setup"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name),\
                                                "")[xdcr_cluster_name is None]
        if xdcr_hostname is not None:
            options += " --xdcr-hostname={0}".format(self.dest_nodes[0].ip)
        options += (" --xdcr-username={0}".format(xdcr_username), "")[xdcr_username is None]
        options += (" --xdcr-password={0}".format(xdcr_password), "")[xdcr_password is None]
        options += (" --xdcr-secure-connection={0}".format(secure_connection))

        if secure_connection == 'full' and xdcr_hostname is not None and xdcr_cert:
            if wrong_cert:
                cluster_host = "localhost"
            else:
                cluster_host = self.dest_master.ip
            cert_info = " --regenerate-cert cert.pem "
            output, _ = self.__execute_cli(cli_command="ssl-manage", options="{0} "\
                                     .format(cert_info), cluster_host=cluster_host)
            self.shell.copy_file_local_to_remote("cert.pem",
                                            self.root_path + "cert.pem")
            os.system("rm -f cert.pem")
            options += (" --xdcr-certificate={0}".format(xdcr_cert),\
                                               "")[xdcr_cert is None]
            msgs_check = ["-----END CERTIFICATE-----", "Certificate regenerate and copied"]
            self.assertTrue(self._check_output(msgs_check, output))

        output, error = self.__execute_cli(cli_command=cli_command, options=options)
        return output, error, xdcr_cluster_name, xdcr_hostname, cli_command, options

    def __verify_bucket_config(self, server, bucket_name, bucket_type,
                               memory_quota, eviction_policy, replica_count,
                               enable_index_replica, priority, enable_flush,
                               stdout, expect_error):
        self.assertTrue(self.verifyCommandOutput(stdout, expect_error,
                                                 "Bucket created"),
                                                 "Expected command to succeed")
        self.assertTrue(self.verifyBucketSettings(server, bucket_name,
                                                bucket_type, memory_quota,
                                                eviction_policy, replica_count,
                                                enable_index_replica, priority,
                                                enable_flush),
                                                "Bucket settings not set properly")

    def testXDCRSetup(self):
        error_expected_in_command = self.input.param("error-expected", None)
        output, _, xdcr_cluster_name, xdcr_hostname, cli_command, options = \
                                                         self.__xdcr_setup_create()
        if error_expected_in_command != "create":
            self.assertEqual(XdcrCLITest.XDCR_SETUP_SUCCESS["create"].replace("CLUSTERNAME",\
                              (xdcr_cluster_name, "")[xdcr_cluster_name is None]), output[0])
        else:
            output_error = self.input.param("output_error", "[]")
            if output_error.find("CLUSTERNAME") != -1:
                output_error = output_error.replace("CLUSTERNAME",\
                                               (xdcr_cluster_name,\
                                               "")[xdcr_cluster_name is None])
            if output_error.find("HOSTNAME") != -1:
                output_error = output_error.replace("HOSTNAME",\
                               (self.servers[xdcr_hostname].ip,\
                                     "")[xdcr_hostname is None])

            for element in output:
                self.log.info("element {0}".format(element))
                if "ERROR: unable to set up xdcr remote site remote (400) Bad Request" \
                        in element:
                    self.log.info("match {0}".format(element))
                    return True
                elif "Error: hostname (ip) is missing" in element:
                    self.log.info("match {0}".format(element))
                    return True
                else:
                    if "ERROR: _ - Error checking if target cluster supports SANs in cerificates." \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif "ERROR: --xdcr-hostname is required to create a cluster connections" \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif "ERROR: _ - Authentication failed. Verify username and password." \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif "ERROR: _ - certificate must be a single, PEM-encoded x509 certificate" \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif "ERROR: _ - Invalid remote cluster." \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
            self.assertFalse("output string did not match")

        # MB-8570 can't edit xdcr-setup through couchbase-cli
        if xdcr_cluster_name:
            options = options.replace("--create ", "--edit ")
            output, _ = self.__execute_cli(cli_command=cli_command, options=options)
            self.assertEqual(XdcrCLITest.XDCR_SETUP_SUCCESS["edit"]\
                                     .replace("CLUSTERNAME", (xdcr_cluster_name, "")\
                                            [xdcr_cluster_name is None]), output[0])
        if not xdcr_cluster_name:
            """ MB-8573 couchbase-cli: quotes are not supported when try to remove
                remote xdcr cluster that has white spaces in the name
            """
            options = "--delete --xdcr-cluster-name=\'{0}\'".format("remote cluster")
        else:
            options = "--delete --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name)
        output, _ = self.__execute_cli(cli_command=cli_command, options=options)
        if error_expected_in_command != "delete":
            self.assertEqual(XdcrCLITest.XDCR_SETUP_SUCCESS["delete"]\
                        .replace("CLUSTERNAME", (xdcr_cluster_name, "remote cluster")\
                                             [xdcr_cluster_name is None]), output[0])
        else:
            xdcr_cluster_name = "unknown"
            options = "--delete --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name)
            output, _ = self.__execute_cli(cli_command=cli_command, options=options)
            output_error = self.input.param("output_error", "[]")
            if output_error.find("CLUSTERNAME") != -1:
                output_error = output_error.replace("CLUSTERNAME", \
                                   (xdcr_cluster_name, "")[xdcr_cluster_name is None])
            if output_error.find("HOSTNAME") != -1:
                output_error = output_error.replace("HOSTNAME", \
                          (self.servers[xdcr_hostname].ip, "")[xdcr_hostname is None])
            expect_error = ("['ERROR: unable to delete xdcr remote site localhost "
                              "(404) Object Not Found', 'unknown remote cluster']")
            if output_error == expect_error:
                output_error = "ERROR: _ - unknown remote cluster"
            self.assertTrue(self._check_output(output_error, output))
            return

    def testXdcrReplication(self):
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
        checkpoint_interval = self.input.param("checkpoint_interval", None)
        source_nozzles = self.input.param("source_nozzles", None)
        target_nozzles = self.input.param("target_nozzles", None)
        filter_expression = self.input.param("filter_expression", None)
        from collection.collections_cli_client import CollectionsCLI

        if filter_expression:
            filter_expression = "REGEXP_CONTAINS(META().id, " + "\"" + filter_expression + "\")"
        timeout_perc_cap = self.input.param("timeout_perc_cap", None)
        _, _, xdcr_cluster_name, xdcr_hostname, _, _ = self.__xdcr_setup_create()
        cli_command = "xdcr-replicate"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name), "")[xdcr_cluster_name is None]
        options += (" --xdcr-from-bucket=\'{0}\'".format(from_bucket), "")[from_bucket is None]
        options += (" --xdcr-to-bucket=\'{0}\'".format(to_bucket), "")[to_bucket is None]
        options += (" --xdcr-replication-mode=\'{0}\'".format(replication_mode), "")[replication_mode is None]
        options += (" --source-nozzle-per-node=\'{0}\'".format(source_nozzles), "")[source_nozzles is None]
        options += (" --target-nozzle-per-node=\'{0}\'".format(target_nozzles), "")[target_nozzles is None]
        options += (" --filter-expression=\'{0}\'".format(filter_expression), "")[filter_expression is None]
        options += (" --checkpoint-interval=\'{0}\'".format(checkpoint_interval), "")[timeout_perc_cap is None]

        # Add built-in user to dest_nodes
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.dest_nodes[0])


        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.dest_nodes[0]), 'builtin')


        self.bucket_size = self._get_bucket_size(self.quota, 1)
        load_scope_id_src = ""
        load_collection_id_src = ""
        if from_bucket:
            bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                              replicas=self.num_replicas,
                                                              enable_replica_index=self.enable_replica_index)
            self.cluster.create_default_bucket(bucket_params)
            rest =  RestConnection(self.master)
            self.buckets = rest.get_buckets()
            self.cli_col = CollectionsCLI(self.master)
            if self.custom_scopes:
                self.create_scope()
            if self.custom_collections:
                self.create_collection(self.col_per_scope)
            self.sleep(10)
            scopes = self.get_bucket_scope()
            if scopes[0][:4] == "\x1b[6n":
                scopes[0] = scopes[0][4:]
            scopes_id = []
            for scope in scopes:
                if scope == "_default" and self.custom_scopes:
                    scopes.remove(scope)
                    continue
                self.log.info("get scope id of scope: {0}".format(scope))
                scopes_id.append(self.get_scopes_id(scope))
            if scopes_id:
                load_scope_id_src = scopes_id[0]
            collections = self.get_bucket_collection()
            if isinstance(collections, tuple):
                collections = self._extract_collection_names(collections)
            if collections[0][:4] == "\x1b[6n":
                collections[0] = collections[0][4:]
            collections_id = []
            for collection in collections:
                if collection == "_default":
                    if len(collections) > 1 and self.custom_collections:
                        collections.remove(collection)
                        continue
                    else:
                        collections_id.append("0x0")
                        break
                collections_id.append(self.get_collections_id(scopes[0], collection))
            collections_id = list(filter(None, collections_id))
            if collections_id:
                load_collection_id_src = collections_id[0]

            shell = RemoteMachineShellConnection(self.master)
            if self.custom_collections:
                col_cmd = " -c {0}".format(load_collection_id_src)
            else:
                col_cmd = " -c 0x0 "
            self.load_all_buckets(self.master, ratio=0.1,
                                             command_options=col_cmd)
        if to_bucket:
            bucket_params = self._create_bucket_params(server=self.dest_nodes[0], size=self.bucket_size,
                                                              replicas=self.num_replicas,
                                                              enable_replica_index=self.enable_replica_index)
            self.cluster.create_default_bucket(bucket_params)
            rest_rm =  RestConnection(self.dest_nodes[0])
            self.buckets = rest_rm.get_buckets()
            cli_col_rm = CollectionsCLI(self.dest_nodes[0])
            if self.custom_scopes:
                self.create_scope(2, rest_rm, cli_col_rm)
            if self.custom_collections:
                self.create_collection(self.col_per_scope, rest_rm, cli_col_rm)
        self.sleep(10)
        output, _ = self.__execute_cli(cli_command, options)
        if not error_expected:
            self.assertTrue(self._check_output(XdcrCLITest.XDCR_REPLICATE_SUCCESS["create"], output))
        else:
            return

        self.sleep(8)
        options = "--list"
        items_match = False
        output, _ = self.__execute_cli(cli_command, options)
        if output[0][:4] == "\x1b[6n":
            output[0] = output[0][4:]
        for value in output:
            if value.startswith("stream id"):
                replicator = value.split(":")[1].strip()
                if pause_resume is not None:
                    # pause replication
                    options = "--pause"
                    options += (" --xdcr-replicator={0}".format(replicator))
                    output, _ = self.__execute_cli(cli_command, options)
                    # validate output message
                    self.assertEqual(XdcrCLITest.XDCR_REPLICATE_SUCCESS["pause"], output[0])
                    self.sleep(20)
                    options = "--list"
                    output, _ = self.__execute_cli(cli_command, options)
                    # check if status of replication is "paused"
                    for value in output:
                        if value.startswith("status"):
                            self.assertEqual(value.split(":")[1].strip(), "paused")
                    self.sleep(20)
                    # resume replication
                    options = "--resume"
                    options += (" --xdcr-replicator={0}".format(replicator))
                    output, _ = self.__execute_cli(cli_command, options)
                    self.assertIn(XdcrCLITest.XDCR_REPLICATE_SUCCESS["resume"], output[0])
                    # check if status of replication is "running"
                    options = "--list"
                    output, _ = self.__execute_cli(cli_command, options)
                    for value in output:
                        if value.startswith("status"):
                            self.assertEqual(value.split(":")[1].strip(), "running")
                self.stat_col = CollectionsStats(self.dest_nodes[0])
                self.sleep(20, "wait for replication complete")
                server_set =  self.get_nodes_in_cluster()
                col_stats = self.get_collection_stats(cluster=server_set)
                items_in_cluster = 0
                for stats in col_stats:
                    if isinstance(col_stats, str):
                        col_stats = col_stats.split(",")
                    for stat in stats:
                        key, value = stat.split(" ")
                        if self.custom_scopes and not self.custom_collections:
                            break
                        if self.custom_collections or self.buckets[0].name == "default":
                            load_sc_item_id = load_scope_id_src + ":" + load_collection_id_src + ":items"
                            if load_sc_item_id in key:
                                items_in_cluster += int(value)
                if self.num_items == items_in_cluster:
                    items_match = True

                options = "--delete"
                options += (" --xdcr-replicator={0}".format(replicator))
                output, _ = self.__execute_cli(cli_command, options)
                self.assertIn(XdcrCLITest.XDCR_REPLICATE_SUCCESS["delete"], output[0])

        # Remove rbac users in dest_nodes
        role_del = ['cbadminbucket']
        RbacBase().remove_user_role(role_del, RestConnection(self.dest_nodes[0]))
        if not items_match:
            if self.custom_scopes and self.custom_collections:
                self.fail("Items in des cluster don't match with src")
            else:
                self.log.info("This is negative test which not create custom collections")
