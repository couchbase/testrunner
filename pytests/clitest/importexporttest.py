import copy
import json, filecmp, itertools
import os, shutil, ast
from threading import Thread

from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from couchbase_helper.stats_tools import StatsCommon
from couchbase_helper.cluster import Cluster
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator
from couchbase_helper.data_analysis_helper import DataCollector
from couchbase_cli import CouchbaseCLI
from security.rbac_base import RbacBase
from pprint import pprint
from testconstants import CLI_COMMANDS, COUCHBASE_FROM_WATSON,\
                          COUCHBASE_FROM_SPOCK, LINUX_COUCHBASE_BIN_PATH,\
                          WIN_COUCHBASE_BIN_PATH, COUCHBASE_FROM_SHERLOCK


class ImportExportTests(CliBaseTest):
    def setUp(self):
        super(ImportExportTests, self).setUp()
        self.cluster_helper = Cluster()
        self.ex_path = self.tmp_path + "export{0}/".format(self.master.ip)
        self.num_items = self.input.param("items", 1000)
        self.localhost = self.input.param("localhost", False)
        self.json_create_gen = JsonDocGenerator("imex", op_type="create",
                                       encoding="utf-8", start=0, end=self.num_items)
        self.json_delete_gen = JsonDocGenerator("imex", op_type="delete",
                                       encoding="utf-8", start=0, end=self.num_items)

    def tearDown(self):
        super(ImportExportTests, self).tearDown()
        self.import_back = self.input.param("import_back", False)
        ClusterOperationHelper.cleanup_cluster(self.servers, self.servers[0])
        if self.import_back:
            self.log.info("clean up server in import back tests")
            imp_servers = copy.deepcopy(self.servers[2:])
            BucketOperationHelper.delete_all_buckets_or_assert(imp_servers, self)
            ClusterOperationHelper.cleanup_cluster(imp_servers, imp_servers[0])
            ClusterOperationHelper.wait_for_ns_servers_or_assert(imp_servers, self)


    def test_check_require_import_flags(self):
        require_flags = ['Flag required, but not specified: -c/--cluster',
                         'Flag required, but not specified: -u/--username',
                         'Flag required, but not specified: -p/--password',
                         'Flag required, but not specified: -b/--bucket',
                         'Flag required, but not specified: -d/--dataset',
                         'Flag required, but not specified: -f/--format']

        cmd = "%s%s%s %s --no-ssl-verify"\
                    % (self.cli_command_path, "cbimport", self.cmd_ext, self.imex_type)
        output, error = self.shell.execute_command(cmd)
        num_require_flags = 6
        if self.imex_type == "csv":
            num_require_flags = 5
        self.log.info("output from command run %s " % output[:num_require_flags])
        self.assertEqual(require_flags[:num_require_flags], output[:num_require_flags],
                                       "Error in require flags of cbimport")

    def test_check_require_export_flags(self):
        require_flags = ['Flag required, but not specified: -c/--cluster',
                         'Flag required, but not specified: -u/--username',
                         'Flag required, but not specified: -p/--password',
                         'Flag required, but not specified: -b/--bucket',
                         'Flag required, but not specified: -f/--format',
                         'Flag required, but not specified: -o/--output']

        cmd = "%s%s%s %s --no-ssl-verify"\
                    % (self.cli_command_path, "cbexport", self.cmd_ext, "json")
        output, error = self.shell.execute_command(cmd)
        self.log.info("output from command run %s " % output[:6])
        self.assertEqual(require_flags[:6], output[:6],
                                       "Error in require flags of cbexport")

    def test_export_from_empty_bucket(self):
        options = {"load_doc": False, "bucket":"empty"}
        return self._common_imex_test("export", options)

    def test_export_from_sasl_bucket(self):
        options = {"load_doc": True, "docs":"1000"}
        return self._common_imex_test("export", options)

    def test_export_and_import_back(self):
        options = {"load_doc": True, "docs":"1000"}
        return self._common_imex_test("export", options)

    def test_export_with_localhost(self):
        """
           Set localhost param = True
           IP address will be replaced with localhost
        """
        options = {"load_doc": True, "docs":"1000"}
        return self._common_imex_test("export", options)

    def test_export_delete_expired_updated_data(self):
        """
           @delete_percent = percent keys deleted
           @udpated = update doc
           @update_field = [fields need to update] like update_field=['dept']
             When do update_field, "mutated" should not = 0

        """
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        delete_percent = self.input.param("delete_percent", None)
        updated = self.input.param("updated", False)
        update_field = self.input.param("update_field", None)
        path = self.input.param("path", None)
        self.ex_path = self.tmp_path + "export{0}/".format(self.master.ip)
        master = self.servers[0]
        server = copy.deepcopy(master)

        if username is None:
            username = server.rest_username
        if password is None:
            password = server.rest_password
        self.cli_command_path = "cd %s; ./" % self.cli_command_path
        self.buckets = RestConnection(server).get_buckets()
        total_items = self.num_items
        for bucket in self.buckets:
            doc_create_gen = copy.deepcopy(self.json_create_gen)
            self.cluster.load_gen_docs(self.master, bucket.name,
                                               doc_create_gen, bucket.kvs[1], "create", compression=self.sdk_compression)
            self.verify_cluster_stats(self.servers[:self.nodes_init], max_verify=total_items)
            if delete_percent is not None:
                self.log.info("Start to delete %s %% total keys" % delete_percent)
                doc_delete_gen = copy.deepcopy(self.json_delete_gen)
                doc_delete_gen.end = int(self.num_items) * int(delete_percent) / 100
                total_items -= doc_delete_gen.end
                self.cluster.load_gen_docs(self.master, bucket.name, doc_delete_gen,
                                                            bucket.kvs[1], "delete", compression=self.sdk_compression)
            if updated and update_field is not None:
                self.log.info("Start update data")
                doc_updated_gen = copy.deepcopy(self.json_create_gen)
                doc_updated_gen.update(fields_to_update=update_field)
                self.cluster.load_gen_docs(self.master, bucket.name, doc_updated_gen,
                                                             bucket.kvs[1], "update", compression=self.sdk_compression)
            self.verify_cluster_stats(self.servers[:self.nodes_init], max_verify=total_items)
            """ remove previous export directory at tmp dir and re-create it
                in linux:   /tmp/export
                in windows: /cygdrive/c/tmp/export """
            self.log.info("remove old export dir in %s" % self.tmp_path)
            self.shell.execute_command("rm -rf {0}export{1} "\
                                       .format(self.tmp_path, self.master.ip))
            self.log.info("create export dir in {0}".format(self.tmp_path))
            self.shell.execute_command("mkdir {0}export{1}"\
                                       .format(self.tmp_path, self.master.ip))
            """ /opt/couchbase/bin/cbexport json -c localhost -u Administrator
                              -p password -b default -f list -o /tmp/test4.zip """
            export_file = self.ex_path + bucket.name
            if self.cmd_ext:
                export_file = export_file.replace("/cygdrive/c", "c:")
            exe_cmd_str = "%s%s%s %s -c %s -u %s -p %s -b %s -f %s -o %s"\
                         % (self.cli_command_path, "cbexport", self.cmd_ext, self.imex_type,
                                     server.ip, username, password, bucket.name,
                                                    self.format_type, export_file)
            output, error = self.shell.execute_command(exe_cmd_str)
            self._check_output("successfully", output)
            if self.format_type == "list":
                self.log.info("remove [] in file")
                self.shell.execute_command("sed%s -i '/^\[\]/d' %s"
                                                 % (self.cmd_ext, export_file))
            output, _ = self.shell.execute_command("gawk%s 'END {print NR}' %s"
                                                     % (self.cmd_ext, export_file))
            self.assertTrue(int(total_items) == int(output[0]),
                                     "doc in bucket: %s != doc in export file: %s"
                                      % (total_items, output[0]))

    def test_export_from_dgm_bucket(self):
        """
           Need to add params below to test:
            format_type=list (or lines)
            dgm_run=True
            active_resident_threshold=xx
        """
        options = {"load_doc": True, "docs":"{0}".format(self.num_items)}
        return self._common_imex_test("export", options)

    def test_export_with_secure_connection(self):
        """
           Need to add params below to test:
            format_type=list (or lines)
            secure-conn=True (default False)
           Return: None
        """
        options = {"load_doc": True, "docs":"1000"}
        return self._common_imex_test("export", options)

    def test_import_to_dgm_bucket(self):
        """
           Need to add params below to test:
            format_type=list (or lines)
            imex_type=json (tab or csv)
            import_file=json_list_1000_lines (depend on above formats)
            dgm_run=True
            active_resident_threshold=xx
        """
        options = {"load_doc": True, "docs":"1000"}
        return self._common_imex_test("import", options)

    def test_imex_with_special_chars_in_password(self):
        """
           Import and export must accept special characters in password
           like: #, $
           This test needs to reset password back to original one.
           Need param:
             @param password
             @param test_type
             @format_type lines/list
             @param import_file
        """
        options = {"load_doc": True, "docs":"1000"}
        new_password = self.input.param("password", None)
        if "hash" in new_password:
            new_password = new_password.replace("hash", "#")
        if "bang" in new_password:
            new_password = new_password.replace("bang", "!")
        rest_password = self.master.rest_password
        command = "setting-cluster"
        password_changed = False
        try:
            self.log.info("Change password to new one")
            if self.input.param("password", None) is None:
                self.fail("Need to pass param 'password' to run this test")
            options_cli = "-u Administrator -p %s --cluster-password '%s' "\
                                        % (rest_password, new_password)
            output, _ = self.shell.couchbase_cli(command, self.master.ip, options_cli)
            if self._check_output("SUCCESS", output):
                self.log.info("Password was changed to %s " % new_password)
                password_changed = True
                self.master.rest_password = new_password
            else:
                self.fail("Fail to change password cluster to %s" % new_password)

            self._common_imex_test(self.test_type, options)
        finally:
            if password_changed:
                self.log.info("change password back to default in ini file")
                options = "-u Administrator -p '%s' --cluster-password '%s' "\
                                           % (new_password, rest_password)
                output, _ = self.shell.couchbase_cli(command, self.master.ip, options)
                if self._check_output("SUCCESS", output):
                    self.log.info("Password was changed back to %s " % rest_password)
                    self.master.rest_password = rest_password
                else:
                    self.fail("Fail to change password back to %s" % rest_password)

    def test_imex_during_rebalance(self):
        """ During rebalance, the test will execute the import/export command.
            Tests will execute in both path, absolute and bin path.
        """
        self._load_doc_data_all_buckets()
        task_reb = self.cluster.async_rebalance(self.servers, [self.servers[2]], [])
        while task_reb.state != "FINISHED":
            if len(self.buckets) >= 1:
                for bucket in self.buckets:
                    if self.test_type == "import":
                        self.test_type = "cbimport"
                        self._remote_copy_import_file(self.import_file)
                        format_flag = "-f"
                        field_separator_flag = ""
                        if self.imex_type == "csv":
                            format_flag = self.format_type = ""
                            if self.field_separator != "comma":
                                if self.field_separator == "tab":
                                    """ we test tab separator in this case """
                                    field_separator_flag = "--field-separator $'\\t' "
                                else:
                                    field_separator_flag = "--field-separator %s "\
                                                              % self.field_separator
                        key_gen = "key::%index%"
                        if self.cmd_ext:
                            self.des_file = self.des_file.replace("/cygdrive/c", "c:")
                        imp_cmd_str = "%s%s%s %s -c %s -u Administrator -p password"\
                                                    " -b %s -d %s%s %s %s -g %s %s "\
                              % (self.cli_command_path, self.test_type, self.cmd_ext,
                                     self.imex_type, self.servers[0].ip, bucket.name,
                                                   self.import_method, self.des_file,
                                              format_flag, self.format_type, key_gen,
                                                                field_separator_flag)
                        output, error = self.shell.execute_command(imp_cmd_str)
                        self.log.info("Output from execute command %s " % output)
                        if not self._check_output("successfully", output):
                            self.fail("Fail to import json file")
                    elif self.test_type == "export":
                        self.test_type = "cbexport"
                        self.shell.execute_command("rm -rf {0}export{1}"\
                                                   .format(self.tmp_path, self.master.ip))
                        self.shell.execute_command("mkdir {0}export{1}"\
                                                   .format(self.tmp_path, self.master.ip))
                        export_file = self.ex_path + bucket.name
                        if self.imex_type == "json":
                            if self.cmd_ext:
                                export_file = export_file.replace("/cygdrive/c", "c:")
                            exp_cmd_str = "%s%s%s %s -c %s -u Administrator -p password"\
                                                            " -b %s -f %s -o %s"\
                                  % (self.cli_command_path, self.test_type, self.cmd_ext,
                                         self.imex_type, self.servers[0].ip, bucket.name,
                                                               self.format_type, export_file)
                            output, error = self.shell.execute_command(exp_cmd_str)
                            self.log.info("Output from execute command %s " % output)
                            if not self._check_output("successfully", output):
                                self.fail("Fail to import json file")
        task_reb.result()

    def test_imex_non_default_port(self):
        options = {"load_doc": True, "docs":"10"}
        server = copy.deepcopy(self.servers[0])
        import_method = self.input.param("import_method", "file://")
        default_port = 8091
        new_port = 9000
        port_changed = False
        test_failed = False
        try:
            """ change default port from 8091 to 9000 """
            port_cmd = "%s%s%s %s -c %s:%s -u Administrator -p password --cluster-port=%s "\
                                    % (self.cli_command_path, "couchbase-cli", self.cmd_ext,
                                          "setting-cluster", server.ip, default_port, new_port)
            output, error = self.shell.execute_command(port_cmd)
            if self._check_output("SUCCESS", output):
                self.log.info("Port was changed from 8091 to 9000")
                port_changed = True
            else:
                self.fail("Fail to change port 8091 to 9000")
            if self.test_type == "import":
                self.test_type = "cbimport"
                self._remote_copy_import_file(self.import_file)
                if len(self.buckets) >= 1:
                    if self.imex_type == "json":
                        for bucket in self.buckets:
                            key_gen = "key::%index%"
                            """ ./cbimport json -c 12.11.10.132 -u Administrator -p password
                        -b default -d file:///tmp/export/default -f list -g key::%index%  """
                            imp_cmd_str = "%s%s%s %s -c %s:%s -u Administrator -p password "\
                                                                 "-b %s -d %s%s -f %s -g %s"\
                                     % (self.cli_command_path, self.test_type, self.cmd_ext,
                                           self.imex_type, server.ip, new_port, bucket.name,
                                    import_method, self.des_file, self.format_type, key_gen)
                            output, error = self.shell.execute_command(imp_cmd_str)
                            self.log.info("Output from execute command %s " % output)
            elif self.test_type == "export":
                self.test_type = "cbexport"
                if len(self.buckets) >= 1:
                    for bucket in self.buckets:
                        self.log.info("load json to bucket %s " % bucket.name)
                        load_cmd = "%s%s%s -n %s:%s -u Administrator -p password "\
                                                                 "-j -i %s -b %s "\
                            % (self.cli_command_path, "cbworkloadgen", self.cmd_ext,
                               server.ip, new_port, options["docs"], bucket.name)
                        self.shell.execute_command(load_cmd)
                self.shell.execute_command("rm -rf {0}export{1}"\
                                           .format(self.tmp_path, self.master.ip))
                self.shell.execute_command("mkdir {0}export{1}"\
                                           .format(self.tmp_path, self.master.ip))
                """ /opt/couchbase/bin/cbexport json -c localhost -u Administrator
                              -p password -b default -f list -o /tmp/test4.zip """
                if len(self.buckets) >= 1:
                    for bucket in self.buckets:
                        export_file = self.ex_path + bucket.name
                        if self.cmd_ext:
                            export_file = export_file.replace("/cygdrive/c", "c:")
                        exe_cmd_str = "%s%s%s %s -c %s:%s -u Administrator "\
                                             "-p password -b %s -f %s -o %s"\
                                    % (self.cli_command_path, self.test_type,
                                       self.cmd_ext, self.imex_type, server.ip,
                                       new_port, bucket.name, self.format_type,
                                                                   export_file)
                        self.shell.execute_command(exe_cmd_str)
                        self._verify_export_file(bucket.name, options)
        except Exception as e:
            if e:
                print("Exception throw: ", e)
            test_failed = True
        finally:
            if port_changed:
                self.log.info("change port back to default port 8091")
                port_cmd = "%s%s%s %s -c %s:%s -u Administrator -p password --cluster-port=%s "\
                        % (self.cli_command_path, "couchbase-cli", self.cmd_ext,
                           "setting-cluster", server.ip, new_port, default_port)
                output, error = self.shell.execute_command(port_cmd)
            if test_failed:
                self.fail("Test failed.  Check exception throw above.")

    def test_imex_flags(self):
        """ imex_type     = json
            cluster_flag  = -c
            user_flag     = -u
            password_flag = -p
            bucket_flag   = -b
            dataset_flag  = -d  // only import
            format_flag   = -f
            generate_flag = -g  // only import
            output_flag   = -o  // only export
            format_type = list/lines
            import_file = json_list_1000_lines
                                  =lines,....
            ./cbimport json -c 12.11.10.132 -u Administrator -p password
                -b default -d file:///tmp/export/default -f list -g key::%index% """
        server = copy.deepcopy(self.servers[0])
        self.sample_file = self.input.param("sample_file", None)
        self.cluster_flag = self.input.param("cluster_flag", "-c")
        self.user_flag = self.input.param("user_flag", "-u")
        self.password_flag = self.input.param("password_flag", "-p")
        self.bucket_flag = self.input.param("bucket_flag", "-b")
        self.dataset_flag = self.input.param("dataset_flag", "-d")
        self.format_flag = self.input.param("format_flag", "-f")
        self.generate_flag = self.input.param("generate_flag", "-g")
        self.output_flag = self.input.param("output_flag", "-o")
        data_path = self.tmp_path
        if self.cmd_ext:
            data_path = self.tmp_path_raw
        if self.test_type == "import":
            cmd = "cbimport"
            cmd_str = "%s%s%s %s %s %s %s Administrator %s password %s default %s "\
                                     "file://%sdefault  %s lines %s key::%%index%%"\
                            % (self.cli_command_path, cmd, self.cmd_ext,
                           self.imex_type, self.cluster_flag, server.ip,
                           self.user_flag, self.password_flag, self.bucket_flag,
                           self.dataset_flag, data_path, self.format_flag,
                           self.generate_flag)
        elif self.test_type == "export":
            cmd = "cbexport"
            self.shell.execute_command("rm -rf {0}export{1}"\
                                       .format(self.tmp_path, self.master.ip))
            self.shell.execute_command("mkdir {0}export{1}" \
                                       .format(self.tmp_path, self.master.ip))
            export_file = self.ex_path + "default"
            cmd_str = "%s%s%s %s %s %s %s Administrator %s password %s default "\
                                                             "  %s lines %s %s "\
                            % (self.cli_command_path, cmd, self.cmd_ext,
                           self.imex_type, self.cluster_flag, server.ip,
                           self.user_flag, self.password_flag, self.bucket_flag,
                           self.format_flag, self.output_flag, export_file)
        output, error = self.shell.execute_command(cmd_str)
        if self.imex_type == "":
            if "Unknown flag: -c" in output:
                self.log.info("%s detects missing 'json' option " % self.test_type)
            else:
                self.fail("%s could not detect missing 'json' option"
                                                             % self.test_type)
        if self.cluster_flag == "":
            if "Invalid subcommand `%s`" % server.ip in output \
                                  and "Required Flags:" in output:
                self.log.info("%s detected missing '-c or --clusger' flag"
                                                             % self.test_type)
            else:
                self.fail("%s could not detect missing '-c or --cluster' flag"
                                                              % self.test_type)
        if self.user_flag == "":
            if "Expected flag: Administrator" in output \
                             and "Required Flags:" in output:
                self.log.info("%s detected missing '-u or --username' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-u or --username' flag"
                                                              % self.test_type)
        if self.password_flag == "":
            if "Expected flag: password" in output \
                             and "Required Flags:" in output:
                self.log.info("%s detected missing '-p or --password' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-p or --password' flag"
                                                          % self.test_type)
        if self.bucket_flag == "":
            if "Expected flag: default" in output \
                             and "Required Flags:" in output:
                self.log.info("%s detected missing '-b or --bucket' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-b or --bucket' flag"
                                                          % self.test_type)
        if self.dataset_flag == "" and self.test_type == "import":
            if "Expected flag: file://%sdefault" % data_path in output \
                             and "Required Flags:" in output:
                self.log.info("%s detected missing '-d or --dataset' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-d or --dataset' flag"
                                                          % self.test_type)
        if self.format_flag == "":
            if "Expected flag: lines" in output \
                                 and "Required Flags:" in output:
                self.log.info("%s detected missing '-f or --format' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-f or --format' flag"
                                                          % self.test_type)
        if self.generate_flag == "" and self.test_type == "import":
            if "Expected flag: %index%" in output \
                             and "Required Flags:" in output:
                self.log.info("%s detected missing '-g or --generate' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-g or --generate' flag"
                                                          % self.test_type)
        if self.output_flag == "" and self.test_type == "export":
            if "Expected flag: /tmp/export/default" in output \
                             and "Required Flags:" in output:
                self.log.info("%s detected missing '-o or --output' flag"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect missing '-o or --output' flag"
                                                          % self.test_type)
        self.log.info("Output from execute command %s " % output)

    def test_imex_optional_flags(self):
        """ imex_type     = json
            threads_flag   = -t
            errors_flag    = -e
            logs_flag      = -l
            include_key_flag = --include-key """
        server = copy.deepcopy(self.servers[0])
        self.threads_flag = self.input.param("threads_flag", "")
        self.logs_flag = self.input.param("logs_flag", "")
        self.include_key_flag = self.input.param("include_key_flag", "")
        self.import_file = self.input.param("import_file", None)
        self.errors_flag = self.input.param("errors_flag", "")
        import_method = self.input.param("import_method", "file://")
        self.output_flag = self.input.param("output_flag", "-o")
        threads_flag = ""
        if self.threads_flag != "":
            threads_flag = "-t"
            if self.threads_flag == "empty":
                self.threads_flag = ""
        errors_flag = ""
        errors_path = ""
        if self.errors_flag != "":
            errors_flag = "-e"
            self.shell.execute_command("rm -rf %serrors" % self.tmp_path)
            self.shell.execute_command("mkdir %serrors" % self.tmp_path)
            if self.errors_flag == "empty":
                errors_path = ""
            elif self.errors_flag == "error":
                errors_path = self.errors_flag
                if "; ./" in self.cli_command_path:
                    self.shell.execute_command("rm -rf %serror"
                                % self.cli_command_path.replace("; ./", ""))
                else:
                    self.shell.execute_command("rm -rf %serror"
                                                   % self.cli_command_path)
            elif self.errors_flag == "relative_path":
                if self.os == 'windows':
                    self.log.info("skip relative path test for -e flag on windows")
                    return
                errors_path = "~/error"
                self.shell.execute_command("rm -rf ~/error")
            elif self.errors_flag == "absolute_path":
                errors_path = self.tmp_path_raw + "errors/" + self.errors_flag
        logs_flag = ""
        logs_path = ""
        if self.logs_flag != "":
            logs_flag = "-l"
            self.shell.execute_command("rm -rf %slogs" % self.tmp_path)
            self.shell.execute_command("mkdir %slogs" % self.tmp_path)
            if self.logs_flag == "empty":
                logs_path = ""
            elif self.logs_flag == "log":
                logs_path = self.logs_flag
                if "; ./" in self.cli_command_path:
                    self.shell.execute_command("rm -rf %slog"
                                % self.cli_command_path.replace("; ./", ""))
                else:
                    self.shell.execute_command("rm -rf %slog"
                                                    % self.cli_command_path)
            elif self.logs_flag == "relative_path":
                if self.os == 'windows':
                    self.log.info("skip relative path test for -l flag on windows")
                    return
                logs_path = "~/log"
                self.shell.execute_command("rm -rf ~/log")
            elif self.logs_flag == "absolute_path":
                logs_path = self.tmp_path_raw + "logs/" + self.logs_flag
        if self.cmd_ext:
            if logs_path and logs_path.startswith("/cygdrive/"):
                logs_path = logs_path.replace("/cygdrive/c", "c:")
        if self.test_type == "import":
            cmd = "cbimport"
            self._remote_copy_import_file(self.import_file)
            if self.imex_type == "json":
                for bucket in self.buckets:
                    """ ./cbimport json -c 12.11.10.132 -u Administrator -p password
                        -b default -d file:///tmp/export/default -f list -g %index%  """
                    des_file = self.des_file
                    if "/cygdrive/c" in des_file:
                        des_file = des_file.replace("/cygdrive/c", "c:")
                    imp_cmd_str = "%s%s%s %s -c %s -u Administrator -p password -b %s "\
                                  "-d %s%s -f %s -g key::%%index%% %s %s %s %s %s %s"\
                             % (self.cli_command_path, cmd, self.cmd_ext,
                                self.imex_type, server.ip, bucket.name,
                                import_method,
                                des_file,
                                self.format_type, threads_flag,
                                self.threads_flag,
                                errors_flag, errors_path,
                                logs_flag, logs_path)
                    self.log.info("command to run %s " % imp_cmd_str)
                    output, error = self.shell.execute_command(imp_cmd_str)
                    self.log.info("Output from execute command %s " % output)
                    error_check = self._check_output_option_flags(output,
                                                  errors_path, logs_path)
                    if logs_path:
                        self.shell.execute_command("rm -rf %s" % logs_path)
                    if errors_path:
                        self.shell.execute_command("rm -rf %s" % errors_path)
                    if error_check and not self._check_output("successfully", output):
                        self.fail("failed to run optional flags")
        elif self.test_type == "export":
            cmd = "cbexport"
            include_flag = ""
            if self.include_key_flag:
                include_flag = " --include-key"
            self.shell.execute_command("rm -rf {0}export{1}"\
                                       .format(self.tmp_path, self.master.ip))
            self.shell.execute_command("mkdir {0}export{1}"\
                                       .format(self.tmp_path, self.master.ip))
            if self.imex_type == "json":
                for bucket in self.buckets:
                    self.log.info("load json to bucket %s " % bucket.name)
                    load_cmd = "%s%s%s -n %s:8091 -u Administrator -p password -j "\
                                                                     "-i %s -b %s "\
                            % (self.cli_command_path, "cbworkloadgen", self.cmd_ext,
                                             server.ip, self.num_items, bucket.name)
                    self.shell.execute_command(load_cmd)
                    export_file = self.ex_path + bucket.name
                    export_file_cmd = export_file
                    if "/cygdrive/c" in export_file_cmd:
                        export_file_cmd = export_file_cmd.replace("/cygdrive/c", "c:")
                    cmd_str = "%s%s%s %s -c %s -u Administrator -p password -b %s "\
                                    "  -f %s %s %s %s %s %s %s %s %s"\
                                     % (self.cli_command_path, cmd, self.cmd_ext,
                                        self.imex_type, server.ip, bucket.name,
                                        self.format_type, self.output_flag,
                                        export_file_cmd,
                                        threads_flag, self.threads_flag,
                                        logs_flag, logs_path,
                                        include_flag, self.include_key_flag)
                    output, error = self.shell.execute_command(cmd_str)
                    self.log.info("Output from execute command %s " % output)
                    error_check = self._check_output_option_flags(output,
                                                              errors_path, logs_path)
                    if logs_path:
                        self.shell.execute_command("rm -rf %s" % logs_path)
                    if error_check and not self._check_output("successfully", output):
                        self.fail("failed to run optional flags")
                    if self.include_key_flag:
                        self.log.info("Verify export with --include-key flag")
                        output, _ = self.shell.execute_command("cat %s" % export_file)
                        if output:
                            for x in output:
                                eval_x = ast.literal_eval(x)
                                if not eval_x[self.include_key_flag].startswith("pymc"):
                                    self.fail("Flag %s failed to include key "
                                              % include_flag)
                                else:
                                    self.log.info("Data for %s flag is verified"
                                                  % include_flag)

    def test_import_invalid_folder_structure(self):
        """ not in 4.6 """
        options = {"load_doc": False}
        return self._common_imex_test("import", options)

    """ /opt/couchbase/bin/cbimport json -c 12.11.10.130:8091
       -u Administrator -p password  -b travel-sample
       -d /opt/couchbase/samples/travel-sample.zip -f sample """
    def test_import_invalid_json_sample(self):
        options = {"load_doc": False}
        return self._common_imex_test("import", options)

    def test_import_json_sample(self):
        """ test_import_json_sample
           -p default_bucket=False,imex_type=json,sample_file=travel-sample """
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        self.sample_file = self.input.param("sample_file", None)
        self.imex_type = self.input.param("imex_type", None)
        sample_file_path = self.sample_files_path + self.sample_file + ".zip"
        server = copy.deepcopy(self.servers[0])
        if username is None:
            username = server.rest_username
        if password is None:
            password = server.rest_password
        if self.sample_file is not None:
            cmd = "cbimport"
            imp_cmd_str = "{0}{1}{2} {3} -c {4} -u {5} -p {6} -b {7} -d file://{8} -f sample"\
                             .format(self.cli_command_path, cmd, self.cmd_ext, self.imex_type,
                                         server.ip, username, password, self.sample_file,
                                                                      sample_file_path)
            output, error = self.shell.execute_command(imp_cmd_str)
            if not self._check_output("SUCCESS", output):
                self.log.info("Output from command %s" % output)
                self.fail("Failed to load sample file %s" % self.sample_file)

    """ imex_type=json,format_type=list,import_file=json_list_1000_lines
                                  =lines,.... """
    def test_import_json_file(self):
        options = {"load_doc": False, "docs": "1000"}
        self.import_file = self.input.param("import_file", None)
        return self._common_imex_test("import", options)

    def test_import_json_generate_keys(self):
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_json_with_limit_n_docs(self):
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_json_with_skip_n_docs(self):
        """
           import docs with option to skip n docs
           flag --skip-docs
           :return: None
        """
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_json_with_skip_limit_n_docs(self):
        """
           import docs with option skip n docs and
           limit n docs to be imported
        """
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_csv_with_limit_n_rows(self):
        """
           import csv with option to limit number of rows
           flag --limit-rows
           :return: None
        """
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_csv_with_skip_n_rows(self):
        """
           import csv with option to limit number of rows
           flag --skip-rows
           :return: None
        """
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_csv_with_skip_limit_n_rows(self):
        """
           import csv with option skip n rows and
           limit n rows
           params:
             imex_type=json,format_type=lines,import_file=json_1000_lines,
           skip-docs=100,limit-docs=20,nodes_init=2,verify-data=True
        :return: None
        """
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_csv_file(self):
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_csv_with_infer_types(self):
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_csv_with_omit_empty(self):
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def test_import_with_secure_connection(self):
        """
            For csv, require param key-gen=False
        """
        options = {"load_doc": False, "docs": "1000"}
        return self._common_imex_test("import", options)

    def _common_imex_test(self, cmd, options):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        if password:
            if "hash" in password:
                password = password.replace("hash", "#")
            if "bang" in password:
                password = password.replace("bang", "!")
        path = self.input.param("path", None)
        self.pre_imex_ops_keys = 0
        self.short_flag = self.input.param("short_flag", True)
        import_method = self.input.param("import_method", "file://")
        if "url" in import_method:
            import_method = ""
        self.ex_path = self.tmp_path + "export{0}/".format(self.master.ip)
        master = self.servers[0]
        server = copy.deepcopy(master)

        if username is None:
            username = server.rest_username
        if password is None:
            password = server.rest_password
        if path is None:
            self.log.info("test with absolute path ")
        elif path == "local":
            self.log.info("test with local bin path ")
            self.cli_command_path = "cd %s; ./" % self.cli_command_path
        self.buckets = RestConnection(server).get_buckets()
        res_status = ""
        random_key = self.key_generator()
        kv_gen = BlobGenerator(random_key, "%s-" % random_key,
                                                   self.value_size,
                                                   start=0,
                                                   end=50000)
        url_format = ""
        secure_port = ""
        secure_conn = ""
        if self.secure_conn:
            # bin_path, cert_path, user, password, server_cert
            cacert = self.shell.get_cluster_certificate_info(self.cli_command_path,
                                                             self.tmp_path_raw,
                                                             "Administrator",
                                                             "password",
                                                             self.master)
            secure_port = "1"
            url_format = "s"
            if not self.no_cacert:
                secure_conn = "--cacert %s" % cacert
            if self.no_ssl_verify:
                secure_conn = "--no-ssl-verify"
        if "export" in cmd:
            cmd = "cbexport"
            if options["load_doc"]:
                if len(self.buckets) >= 1:
                    for bucket in self.buckets:
                        self.log.info("load json to bucket %s " % bucket.name)
                        load_cmd = "%s%s%s -n %s:8091 -u %s -p '%s' -j -i %s -b %s "\
                            % (self.cli_command_path, "cbworkloadgen", self.cmd_ext,
                               server.ip, username, password, options["docs"],
                               bucket.name)
                        if self.dgm_run and self.active_resident_threshold:
                            """ disable auto compaction so that bucket could
                                go into dgm faster.
                            """
                            self.rest.disable_auto_compaction()
                            self.log.info("**** Load bucket to %s of active resident"\
                                          % self.active_resident_threshold)
                            self.shell.execute_command("{0}{1}{2} bucket-edit -c {3}:8091 "\
                                                       " -u Administrator -p password "\
                                                       "--bucket {4} --bucket-ramsize 100"\
                                                       .format(self.cli_command_path,
                                                        "couchbase-cli", self.cmd_ext,
                                                        server.ip, bucket.name))
                            self._load_all_buckets(self.master, kv_gen, "create", 0)
                        self.log.info("load sample data to bucket")
                        self.shell.execute_command(load_cmd)
            """ remove previous export directory at tmp dir and re-create it
                in linux:   /tmp/export
                in windows: /cygdrive/c/tmp/export """
            self.log.info("remove old export dir in %s" % self.tmp_path)
            self.shell.execute_command("rm -rf {0}export{1}"\
                                       .format(self.tmp_path, self.master.ip))
            self.log.info("create export dir in %s" % self.tmp_path)
            self.shell.execute_command("mkdir {0}export{1}"\
                                       .format(self.tmp_path, self.master.ip))
            if self.check_preload_keys:
                for bucket in self.buckets:
                    self.cluster_helper.wait_for_stats([self.master], bucket.name, "",
                                                                "ep_queue_size", "==", 0)
                    self.pre_imex_ops_keys = \
                            RestConnection(self.master).get_active_key_count(bucket.name)
            """ /opt/couchbase/bin/cbexport json -c localhost -u Administrator
                              -p password -b default -f list -o /tmp/test4.zip """
            if len(self.buckets) >= 1:
                for bucket in self.buckets:
                    stats_all_buckets = {}
                    stats_all_buckets[bucket.name] = StatsCommon()
                    export_file = self.ex_path + bucket.name
                    if self.cmd_ext:
                        export_file = export_file.replace("/cygdrive/c", "c:")
                    if self.localhost:
                        server.ip = "localhost"
                    exe_cmd_str = "%s%s%s %s -c http%s://%s:%s8091 -u %s -p '%s' " \
                                  " -b %s -f %s %s -o %s -t 4"\
                                  % (self.cli_command_path, cmd, self.cmd_ext,
                                     self.imex_type, url_format, server.ip,
                                     secure_port, username, password, bucket.name,
                                     self.format_type, secure_conn, export_file)
                    if self.dgm_run:
                        res_status = stats_all_buckets[bucket.name].get_stats([self.master],
                                     bucket, '', 'vb_active_perc_mem_resident')[self.master]
                        while int(res_status) > self.active_resident_threshold:
                            self.sleep(5)
                            res_status = stats_all_buckets[bucket.name].get_stats([self.master],
                                         bucket, '',
                                         'vb_active_perc_mem_resident')[self.master]
                        if int(res_status) <= self.active_resident_threshold:
                            self.log.info("Clear terminal")
                            self.shell.execute_command('printf "\033c"')
                    output = ""
                    try:
                        output, error = self.shell.execute_command_raw(exe_cmd_str,
                                                                        timeout=60)
                    except Exception as e:
                        if not output:
                            self.fail("MB-31432 is fixed.  This must be other issue {0}"
                                                                             .format(e))
                    data_exported = True
                    if self.secure_conn:
                        if self.no_ssl_verify:
                            if not self._check_output("successfully", output):
                                data_exported = False
                                self.fail("Fail to export with no-ssl-verify flag")
                        elif self.no_cacert:
                            if self._check_output("successfully", output):
                                data_exported = False
                                self.fail("Secure connection works without cacert")
                        elif not self._check_output("successfully", output):
                            data_exported = False
                            self.fail("Failed export json in secure connection")
                    elif not self._check_output("successfully", output):
                        data_exported = False
                        self.fail("Failed to export json data")
                    if data_exported:
                        self._verify_export_file(bucket.name, options)

            if self.import_back:
                import_file = export_file
                import_servers = copy.deepcopy(self.servers)
                imp_rest = RestConnection(import_servers[2])
                import_shell = RemoteMachineShellConnection(import_servers[2])
                imp_rest.force_eject_node()
                self.sleep(2)

                imp_rest = RestConnection(import_servers[2])
                status = False
                info = imp_rest.get_nodes_self()
                if info.memoryQuota and int(info.memoryQuota) > 0:
                    self.quota = info.memoryQuota
                imp_rest.init_node()
                self.cluster.rebalance(import_servers[2:], [import_servers[3]], [])

                """ Add built-in user cbadminbucket to second cluster """
                self.log.info("add built-in user cbadminbucket to second cluster.")
                testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
                RbacBase().create_user_source(testuser, 'builtin', import_servers[2])
                """ Assign user to role """
                role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
                RbacBase().add_user_role(role_list, RestConnection(import_servers[2]), 'builtin')
                
                bucket_params=self._create_bucket_params(server=import_servers[2],
                                        size=250,
                                        replicas=self.num_replicas,
                                        enable_replica_index=self.enable_replica_index,
                                        eviction_policy=self.eviction_policy)
                self.cluster.create_default_bucket(bucket_params)
                imp_cmd_str = "%s%s%s %s -c %s -u %s -p '%s' -b %s "\
                                        "-d file://%s -f %s -g key::%%%s%%"\
                                        % (self.cli_command_path, "cbimport",
                                           self.cmd_ext, self.imex_type,
                                           import_servers[2].ip, username, password,
                                           "default", import_file, self.format_type,
                                           "index")
                output, error = import_shell.execute_command(imp_cmd_str)
                if self._check_output("error", output):
                    self.fail("Fail to run import back to bucket")
        elif "import" in cmd:
            cmd = "cbimport"
            if import_method != "":
                self.im_path = self.tmp_path + "import/"
                self.log.info("copy import file from local to remote")
                skip_lines = ""
                if self.skip_docs:
                    skip_lines = " --skip-docs %s " % self.skip_docs
                limit_lines = ""
                if self.limit_docs:
                    limit_lines = " --limit-docs %s " % self.limit_docs
                if self.limit_rows:
                    limit_lines = " --limit-rows %s " % self.limit_rows
                if self.skip_rows:
                    skip_lines = " --skip-rows %s " % self.skip_rows
                omit_empty = ""
                if self.omit_empty:
                    omit_empty = " --omit-empty "
                infer_types = ""
                if self.infer_types:
                    infer_types = " --infer-types "
                json_invalid_errors_file = ""
                if self.json_invalid_errors:
                    self.log.info("Remove old json invalid error file")
                    json_invalid_errors_file = "-e %sinvalid_error" % self.tmp_path
                    self.shell.execute_command("rm -rf %s"
                                                     % json_invalid_errors_file[3:])
                fx_generator = ""
                if self.fx_generator:
                    fx_generator = "::#%s#" % self.fx_generator.upper()
                if self.fx_generator and self.fx_gen_start:
                    fx_generator = "::#%s[%s]#" \
                                    % (self.fx_generator.upper(), self.fx_gen_start)
                output, error = self.shell.execute_command("ls %s " % self.tmp_path)
                if self._check_output("import", output):
                    self.log.info("remove %simport directory" % self.tmp_path)
                    self.shell.execute_command("rm -rf  %simport " % self.tmp_path)
                    output, error = self.shell.execute_command("ls %s " \
                                                                   % self.tmp_path)
                    if self._check_output("import", output):
                        self.fail("fail to delete import dir ")
                self.shell.execute_command("mkdir  %simport " % self.tmp_path)
                if self.import_file is not None:
                    src_file = "resources/imex/"+ self.import_file
                else:
                    self.fail("Need import_file param")
                des_file = self.im_path + self.import_file
                self.shell.copy_file_local_to_remote(src_file, des_file)
            else:
                des_file = self.import_file

            if len(self.buckets) >= 1:
                format_flag = "-f"
                field_separator_flag = ''
                if self.imex_type == "csv":
                    format_flag = ""
                    self.format_type = ""
                    if self.field_separator != "comma":
                        if self.field_separator == "tab":
                            """ we test tab separator in this case """
                            field_separator_flag = "--field-separator $'\\t' "
                        else:
                            field_separator_flag = "--field-separator %s " \
                                                            % self.field_separator
                for bucket in self.buckets:
                    key_gen = "-g %index%"
                    if self.key_gen:
                        key_gen = "-g key::%index%"
                    if self.field_substitutions:
                        key_gen = "-g key::%{0}%".format(self.field_substitutions)
                        options["field_substitutions"] = key_gen[3:]
                    """ ./cbimport json -c 12.11.10.132 -u Administrator -p password
                        -b default -d file:///tmp/export/default -f list -g key::%index%
                    """
                    if self.cmd_ext:
                        des_file = des_file.replace("/cygdrive/c", "c:")
                    imp_cmd_str = "%s%s%s %s -c http%s://%s:%s8091 -u %s -p '%s' " \
                                  "-b %s -d %s%s %s %s "\
                                  " %s%s %s %s %s %s %s %s %s"\
                                       % (self.cli_command_path, cmd, self.cmd_ext,
                                          self.imex_type,
                                          url_format, server.ip, secure_port,
                                          username, password,
                                          bucket.name,
                                          import_method, des_file,
                                          format_flag,
                                          self.format_type,
                                          key_gen, fx_generator,
                                          field_separator_flag,
                                          limit_lines, skip_lines,
                                          omit_empty, infer_types,
                                          secure_conn, json_invalid_errors_file)
                    print("\ncommand format: ", imp_cmd_str)
                    if self.dgm_run and self.active_resident_threshold:
                        """ disable auto compaction so that bucket could
                            go into dgm faster.
                        """
                        RestConnection(self.master).disable_auto_compaction()
                        self.log.info("**** Load bucket to %s of active resident"\
                                          % self.active_resident_threshold)
                        self.shell.execute_command("{0}{1}{2} bucket-edit -c {3}:8091 "\
                                                       " -u Administrator -p password "\
                                                       "--bucket {4} --bucket-ramsize 100"\
                                                       .format(self.cli_command_path,
                                                        "couchbase-cli", self.cmd_ext,
                                                        server.ip, bucket.name))
                        self._load_all_buckets(self.master, kv_gen, "create", 0)
                    self.cluster_helper.wait_for_stats([self.master], bucket.name, "",
                                                             "ep_queue_size", "==", 0)
                    if self.check_preload_keys:
                        self.cluster_helper.wait_for_stats([self.master], bucket.name, "",
                                                                 "ep_queue_size", "==", 0)
                        self.pre_imex_ops_keys = \
                            RestConnection(self.master).get_active_key_count(bucket.name)

                    self.log.info("Import data to bucket")
                    output, error = self.shell.execute_command(imp_cmd_str)
                    if error:
                        self.fail("\nFailed to run command %s\nError:\n %s"
                                  % (imp_cmd_str, error))
                    self.log.info("Output from execute command %s " % output)
                    """ Json `file:///root/json_list` imported to `http://host:8091`
                        successfully
                    """

                    data_loaded = False
                    if self.secure_conn:
                        if self.no_ssl_verify:
                            if self._check_output("successfully", output):
                                data_loaded = True
                            else:
                                self.fail("Fail to import with no-ssl-verify flag")
                        elif self.no_cacert:
                            if self._check_output("successfully", output):
                                self.fail("Secure connection works without cacert")
                        elif self._check_output("successfully", output):
                            data_loaded = True
                        else:
                            self.fail("Failed import data in secure connection")
                    elif "invalid" in self.import_file:
                        if self.json_invalid_errors:
                            output1, error1 = self.shell.execute_command("cat %s"
                                                    % json_invalid_errors_file[3:])
                            if output1:
                                self.log.info("\n** Invalid json line in error file **\n"
                                              "=> %s" % output1)
                                if '"name":: "pymc272"' not in output1[0]:
                                    self.fail("Failed to write error json line to file")
                    elif self._check_output("successfully", output):
                        data_loaded = True

                    if not data_loaded:
                        if self.secure_conn and not self.no_cacert:
                           self.fail("Failed to execute command")
                    self.sleep(5)
                    if data_loaded:
                        self._verify_import_data(options)

    def _verify_import_data(self, options):
        self.buckets = RestConnection(self.master).get_buckets()
        for bucket in self.buckets:
            keys = RestConnection(self.master).get_active_key_count(bucket.name)
        skip_lines = 0
        if self.import_file and self.import_file.startswith("csv"):
            skip_lines = 1
        limit_lines = ""
        data_import = ""
        if "docs" in options:
            data_import = int(options["docs"])
        if self.skip_docs:
            data_import = int(options["docs"]) - int(self.skip_docs)
            skip_lines += int(self.skip_docs)
        if self.limit_docs:
            data_import = int(self.limit_docs)
            limit_lines = int(self.limit_docs)
        if self.skip_rows:
            data_import = int(options["docs"]) - int(self.skip_rows)
            skip_lines += int(self.skip_rows)
        if self.limit_rows:
            data_import = int(self.limit_rows)
            limit_lines = int(self.limit_rows)
        if self.dgm_run:
            keys = int(keys) - int(self.pre_imex_ops_keys)

        if not self.json_invalid_errors:
            print("Total docs in bucket: ", keys)
            print("Docs need to import: ", data_import)

        if data_import != int(keys):
            if self.skip_docs:
                self.fail("Import failed to skip %s docs" % self.skip_docs)
            if self.limit_docs:
                self.fail("Import failed to limit %s docs" % self.limit_docs)
            if self.limit_rows:
                self.fail("Import failed to limit %s rows" % self.limit_rows)
            if self.skip_rows:
                self.fail("Import failed to skip %s rows" % self.limit_rows)
            else:
                self.fail("Import data does not match with bucket data")

        if self.verify_data:
            if self.field_substitutions:
                self.log.info("Check key format %s ")
                self.keys_check = []
                for i in range(10):
                    self.keys_check.append(self.rest.get_random_key("default"))
                if self.debug_logs:
                    print("keys:   ", self.keys_check)
                for x in self.keys_check:
                    if self.field_substitutions == "age":
                      if not x["key"].split("::")[1].isdigit():
                          self.fail("Field substitutions failed to work")
                    if self.field_substitutions == "name":
                        if not x["key"].split("::")[1].startswith("pymc"):
                            self.fail("Field substitutions failed to work")

            if self.imex_type == "json":
                export_file = self.tmp_path + "bucket_data"
                export_file_cmd = self.tmp_path_raw + "bucket_data"
                self.shell.execute_command("rm -rf %s " % export_file)
                cmd = "%scbexport%s %s -c %s -u %s -p '%s' -b %s -f %s -o %s"\
                              % (self.cli_command_path, self.cmd_ext,
                                 self.imex_type,
                                 self.master.ip, "cbadminbucket", "password",
                                 "default", self.format_type, export_file_cmd)
                output, error = self.shell.execute_command(cmd)
                self.shell.log_command_output(output, error)
            format_type = "json"
            if self.imex_type == "csv":
                if self.field_separator == "comma":
                    format_type = "csv_comma"
                else:
                    format_type = "csv_tab"
            with open("resources/imex/%s" % self.import_file) as f:
                if self.skip_docs and self.limit_docs:
                    self.log.info("Skip %s docs and import only %s docs after that"
                                               % (self.skip_docs, self.limit_docs))
                    src_data = list(itertools.islice(f, self.skip_docs,
                                                        skip_lines +
                                                        int(self.limit_docs)))
                    src_data = [s.strip() for s in src_data]
                    src_data = [x.replace(" ", "") for x in src_data]
                elif self.skip_rows and self.limit_rows:
                    self.log.info("Skip %s rows and import only %s rows after that"
                                  % (self.skip_rows, self.limit_rows))
                    src_data = list(itertools.islice(f, (self.skip_rows + 1),
                                                        skip_lines +
                                                        int(self.limit_rows)))
                elif self.skip_docs:
                    self.log.info("Get data from %dth line" % skip_lines)
                    src_data = f.read().splitlines()[skip_lines:]
                elif self.limit_docs:
                    self.log.info("Get limit data to %d lines" % limit_lines)
                    src_data = f.read().splitlines()[:limit_lines]
                elif self.limit_rows:
                    actual_lines = limit_lines + 1
                    self.log.info("Get limit data to %d lines" % limit_lines)
                    src_data = f.read().splitlines()[1:actual_lines]
                elif self.skip_rows:
                    self.log.info("Get data from %dth lines" % skip_lines)
                    src_data = f.read().splitlines()[skip_lines:]
                else:
                    self.log.info("Get data from source file")
                    src_data = f.read().splitlines()[skip_lines:]
            src_data = [x.replace(" ", "") for x in src_data]
            src_data = [x.rstrip(",") for x in src_data]
            src_data[0] = src_data[0].replace("[", "")
            src_data[len(src_data)-1] = src_data[len(src_data)-1].replace("]", "")

            if self.imex_type == "json":
                self.log.info("Copy bucket data from remote to local")
                if os.path.exists("/tmp/%s" % self.master.ip):
                    shutil.rmtree("/tmp/%s" % self.master.ip)
                os.makedirs("/tmp/%s" % self.master.ip)
                self.shell.copy_file_remote_to_local(export_file,
                                              "/tmp/%s/bucket_data" % self.master.ip)
                with open("/tmp/%s/bucket_data" % self.master.ip) as f:
                    bucket_data = f.read().splitlines()
                    bucket_data = [x.replace(" ", "") for x in bucket_data]
                    bucket_data = [x.rstrip(",") for x in bucket_data]
                    bucket_data[0] = bucket_data[0].replace("[", "")
                    bucket_data[len(bucket_data) - 1] = \
                    bucket_data[len(bucket_data) - 1].replace("]", "")
                if self.debug_logs:
                    print("\nsource data  \n", src_data)
                    print("\nbucket data  \n", bucket_data)
                self.log.info("Compare source data and bucket data")
                if sorted(src_data) == sorted(bucket_data):
                    self.log.info("Import data match bucket data")
                    if os.path.exists("/tmp/%s" % self.master.ip):
                        self.log.info("Remove data in slave")
                        shutil.rmtree("/tmp/%s" % self.master.ip)
                else:
                    self.fail("Import data does not match bucket data")
            elif self.imex_type == "csv":
                self.log.info("Verify csv import data")
                shell = RemoteMachineShellConnection(self.master)
                curl_cmd = "curl -g -X GET -u Administrator:password " \
                      "http://%s:8091/pools/default/buckets/default/docs?" \
                      "include_docs=false&skip=0" % self.master.ip
                output, error = shell.execute_command(curl_cmd)

                bucket_keys = ast.literal_eval(output[0])
                bucket_keys = bucket_keys["rows"]
                for x in range(0, len(src_data)):
                    if self.debug_logs:
                        print("source data:  ", src_data[x].split(",")[2])
                        print("bucket data:  \n", bucket_keys[x]["id"])
                    if not any(str(src_data[x].split(",")[2]) in\
                                                    k["id"] for k in bucket_keys):
                        self.fail("Failed to import key %s to bucket"
                                  % src_data[x])
                    curl_cmd = "curl -g -X GET -u Administrator:password " \
                            "http://%s:8091/pools/default/buckets/default/docs/%d" \
                               % (self.master.ip, x)
                    if self.omit_empty:
                        empty_data_keys = [2, 6, 100, 500, 750, 888]
                        if x in empty_data_keys:
                            output, error = shell.execute_command(curl_cmd)
                            if output:
                                key_value = output[0]
                                key_value = ast.literal_eval(key_value)
                                print("key value json  ", key_value["json"])
                                if "age" in key_value["json"]:
                                    if "body" in key_value["json"]:
                                        self.fail("Failed to omit empty value field")
                    if self.infer_types:
                        print_cmd = False
                        if self.debug_logs:
                            print_cmd = True
                        output, error = shell.execute_command(curl_cmd, debug=print_cmd)
                        if output:
                            key_value = output[0]
                            key_value = ast.literal_eval(key_value)
                            if isinstance( key_value["json"], str):
                                key_value["json"] = ast.literal_eval(key_value["json"])
                            if not isinstance( key_value["json"]["age"], int):
                                self.fail("Failed to put inferred type into docs %s"
                                          % src_data[x])

    def _verify_export_file(self, export_file_name, options):
        if not options["load_doc"]:
            if "bucket" in options and options["bucket"] == "empty":
                output, error = self.shell.execute_command("ls %s" % self.ex_path)
                if export_file_name in output[0]:
                    self.log.info("check if export file %s is empty"
                                                                % export_file_name)
                    output, error = self.shell.execute_command("cat %s%s"\
                                                 % (self.ex_path, export_file_name))
                    if output:
                        self.fail("file %s should be empty" % export_file_name)
                else:
                    self.fail("Fail to export.  File %s does not exist"
                                                            % export_file_name)
        elif options["load_doc"]:
            found = self.shell.file_exists(self.ex_path, export_file_name)
            if found:
                self.log.info("copy export file from remote to local")
                if os.path.exists("/tmp/export{0}".format(self.master.ip)):
                    shutil.rmtree("/tmp/export{0}".format(self.master.ip))
                os.makedirs("/tmp/export{0}".format(self.master.ip))
                self.shell.copy_file_remote_to_local(self.ex_path+export_file_name,
                                                    "/tmp/export{0}/".format(self.master.ip) \
                                                    + export_file_name)
                self.log.info("compare 2 json files")
                if self.format_type == "lines":
                    sample_file = open("resources/imex/json_%s_lines" % options["docs"])
                    samples = sample_file.read().splitlines()
                    samples = [x.replace(" ", "") for x in samples]
                    export_file = open("/tmp/export{0}/".format(self.master.ip)\
                                                             + export_file_name)

                    exports = export_file.read().splitlines()
                    for x in range(len(exports)):
                        tmp = exports[x].split(",")
                        """ add leading zero to name value
                            like pymc39 to pymc039
                        """
                        zero_fill = 0
                        if len(exports) >= 10 and len(exports) < 99:
                            zero_fill = 1
                        elif len(exports) >= 100 and len(exports) < 999:
                            zero_fill = 2
                        elif len(exports) >= 100 and len(exports) < 9999:
                            zero_fill = 3
                        tmp1 = tmp[0][13:-1].zfill(zero_fill)
                        tmp[0] = tmp[0][:13] + tmp1 + '"'
                        exports[x] = ",".join(tmp)

                    if self.debug_logs:
                        s = set(exports)
                        not_in_exports = [x for x in samples if x not in s]
                        print("\n data in sample not in exports  ", not_in_exports)
                        e = set(samples)
                        not_in_samples = [x for x in exports if x not in e]
                        print("\n data in exports not in samples  ", not_in_samples)
                    count = 0
                    self.log.info("Compare data with sample data")
                    for x in exports:
                        if x in samples:
                            count += 1
                    if count != len(samples):
                        self.fail("export and sample json count does not match")
                    elif not self.dgm_run:
                        if sorted(samples) == sorted(exports):
                            self.log.info("export and sample json mathch")
                        else:
                            self.fail("export and sample json does not match")
                    sample_file.close()
                    export_file.close()
                    self.log.info("remove file /tmp/export{0}".format(self.master.ip))
                    shutil.rmtree("/tmp/export{0}".format(self.master.ip))
                elif self.format_type == "list":
                    sample_file = open("resources/imex/json_list_%s_lines"\
                                                                 % options["docs"])
                    samples = sample_file.read()
                    samples = ast.literal_eval(samples)
                    samples.sort(key=lambda k: k['name'])
                    export_file = open("/tmp/export{0}/".format(self.master.ip)\
                                                             + export_file_name)
                    exports = export_file.read()
                    exports = ast.literal_eval(exports)
                    exports.sort(key=lambda k: k['name'])
                    """ convert index value from int to str in MH """
                    if exports and isinstance(exports[0]["index"], str):
                        for x in samples:
                            x["index"] = str(x["index"])

                    if self.debug_logs:
                        print("\nSample list data: %s" % samples)
                        print("\nExport list data: %s" % exports)
                    if samples == exports:
                        self.log.info("export and sample json files are matched")
                    else:
                        self.fail("export and sample json files did not match")
                    sample_file.close()
                    export_file.close()
                    self.log.info("remove file /tmp/export{0}".format(self.master.ip))
                    shutil.rmtree("/tmp/export{0}".format(self.master.ip))
            else:
                file_exist = True
                if self.secure_conn and self.no_cacert:
                    file_exist = False
                if file_exist:
                    self.fail("There is not export file '%s' in %s%s"\
                             % (export_file_name, self.ex_path, export_file_name))

    def _check_output(self, word_check, output):
        found = False
        if len(output) >=1 :
            for x in output:
                if word_check.lower() in x.lower():
                    self.log.info("Found \"%s\" in CLI output" % word_check)
                    found = True
                    break
        return found

    def _remote_copy_import_file(self, import_file):
        import_method = self.input.param("import_method", "file://")
        if "url" in import_method:
            import_method = ""
        if import_method != "":
            self.im_path = self.tmp_path + "import/"
            self.log.info("copy import file from local to remote")
            output, error = self.shell.execute_command("ls %s " % self.tmp_path)
            if self._check_output("import", output):
                self.log.info("remove %simport directory" % self.tmp_path)
                self.shell.execute_command("rm -rf  %simport " % self.tmp_path)
                output, error = self.shell.execute_command("ls %s " % self.tmp_path)
                if self._check_output("import", output):
                    self.fail("fail to delete import dir ")
            self.shell.execute_command("mkdir  %simport " % self.tmp_path)
            if import_file is not None:
                self.src_file = "resources/imex/"+ import_file
            else:
                self.fail("Need import_file param")
            self.des_file = self.im_path + import_file
            self.shell.copy_file_local_to_remote(self.src_file, self.des_file)
        else:
            self.des_file = self.import_file

    def _check_output_option_flags(self, output, errors_path, logs_path):
        error_check = True
        if  self.input.param("threads_flag", "") == "empty":
            error_check = False
            if "Expected argument for option: -t" in output:
                self.log.info("%s detected empty value of threads argument"
                                                          % self.test_type)
            else:
                self.fail("%s could not detect empty value of argument"
                                                       % self.test_type)
        elif self.threads_flag == "notnumber":
            error_check = False
            if output and "Unable to process value for flag: -t" in output[0]:
                self.log.info("%s detected incorrect value of threads argument"
                                                                 % self.test_type)
            else:
                self.fail("%s could not detect incorrect value of argument"
                                                            % self.test_type)
        if self.errors_flag == "empty":
            error_check = False
            if "Expected argument for option: -e" in output:
                self.log.info("%s detected empty value of error argument"
                                                        % self.test_type)
            else:
                self.fail("%s could not detect empty value of argument"
                                                     % self.test_type)
        elif self.errors_flag == "relative_path":
            output, error = self.shell.execute_command("ls %s " % self.root_path)
            if self._check_output(errors_path[2:], output):
                error_check = False
                self.log.info("%s error file created" % self.test_type)
            else:
                self.fail("%s failed to create error file in log flag"
                                                        % self.test_type)
        elif self.errors_flag == "absolute_path":
            output, error = self.shell.execute_command("ls %s " % errors_path)
            if self._check_output("error", output):
                error_check = False
                self.log.info("%s error file created" % self.test_type)
            else:
                self.fail("%s failed to create error file in log flag"
                                                    % self.test_type)
        elif self.errors_flag != "":
            output, error = self.shell.execute_command("ls %s " % errors_path)
            if self._check_output(errors_path, output):
                error_check = False
                self.log.info("%s error file created" % self.test_type)
            else:
                self.fail("%s failed to create error file in error flag"
                                                        % self.test_type)
        if self.logs_flag == "empty":
            error_check = False
            if "Expected argument for option: -l" in output:
                self.log.info("%s detected empty value of log argument"
                                                        % self.test_type)
            else:
                self.fail("%s could not detect empty value of logs argument"
                                                            % self.test_type)
        elif self.logs_flag == "relative_path":
            output, error = self.shell.execute_command("ls %s " % self.root_path)
            if self._check_output(logs_path[2:], output):
                error_check = False
                self.log.info("%s log file created" % self.test_type)
            else:
                self.fail("%s failed to create log file in log flag"
                                                        % self.test_type)
        elif self.logs_flag == "absolute_path":
            output, error = self.shell.execute_command("ls %s " % self.cli_command_path)
            if self._check_output("log", output):
                error_check = False
                self.log.info("%s log file created" % self.test_type)
            else:
                self.fail("%s failed to create log file in log flag"
                                                        % self.test_type)
        return error_check
