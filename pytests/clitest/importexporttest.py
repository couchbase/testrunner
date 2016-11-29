import copy
import json, filecmp
import os, shutil, ast
from threading import Thread

from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_cli import CouchbaseCLI
from pprint import pprint
from testconstants import CLI_COMMANDS, COUCHBASE_FROM_WATSON,\
                          COUCHBASE_FROM_SPOCK, LINUX_COUCHBASE_BIN_PATH,\
                          WIN_COUCHBASE_BIN_PATH, COUCHBASE_FROM_SHERLOCK


class ImportExportTests(CliBaseTest):
    def setUp(self):
        super(ImportExportTests, self).setUp()
        self.ex_path = self.tmp_path + "export/"
        self.num_items = self.input.param("items", 1000)
        self.field_separator = self.input.param("field_separator", "comma")

    def tearDown(self):
        super(ImportExportTests, self).tearDown()
        self.import_back = self.input.param("import_back", False)
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
        options = {"load_doc": True, "docs":"10"}
        return self._common_imex_test("export", options)

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
                            format_flag = ""
                            if self.field_separator != "comma":
                                field_separator_flag = "--field-separator %s " % self.field_separator
                        key_gen = "%index%"
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
                        self.shell.execute_command("rm -rf %sexport " % self.tmp_path)
                        self.shell.execute_command("mkdir %sexport " % self.tmp_path)
                        export_file = self.ex_path + bucket.name
                        if self.imex_type == "json":
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
                                          "cluster-edit", server.ip, default_port, new_port)
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
                            key_gen = "%index%"
                            """ ./cbimport json -c 12.11.10.132 -u Administrator -p password
                        -b default -d file:///tmp/export/default -f list -g %index%  """
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
                self.shell.execute_command("rm -rf %sexport " % self.tmp_path)
                self.shell.execute_command("mkdir %sexport " % self.tmp_path)
                """ /opt/couchbase/bin/cbexport json -c localhost -u Administrator
                              -p password -b default -f list -o /tmp/test4.zip """
                if len(self.buckets) >= 1:
                    for bucket in self.buckets:
                        export_file = self.ex_path + bucket.name
                        exe_cmd_str = "%s%s%s %s -c %s:%s -u Administrator "\
                                             "-p password -b %s -f %s -o %s"\
                                    % (self.cli_command_path, self.test_type,
                                       self.cmd_ext, self.imex_type, server.ip,
                                       new_port, bucket.name, self.format_type,
                                                                   export_file)
                        self.shell.execute_command(exe_cmd_str)
                        self._verify_export_file(bucket.name, options)
        except Exception, e:
            if e:
                print "Exception throw: ", e
            test_failed = True
        finally:
            if port_changed:
                self.log.info("change port back to default port 8091")
                port_cmd = "%s%s%s %s -c %s:%s -u Administrator -p password --cluster-port=%s "\
                        % (self.cli_command_path, "couchbase-cli", self.cmd_ext,
                           "cluster-edit", server.ip, new_port, default_port)
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
                        -b default -d file:///tmp/export/default -f list -g %index% """
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
        if self.test_type == "import":
            cmd = "cbimport"
            cmd_str = "%s%s%s %s %s %s %s Administrator %s password %s default %s "\
                                      "file://%sdefault  %s lines %s %%index%%"\
                            % (self.cli_command_path, cmd, self.cmd_ext,
                           self.imex_type, self.cluster_flag, server.ip,
                           self.user_flag, self.password_flag, self.bucket_flag,
                           self.dataset_flag, self.tmp_path, self.format_flag,
                           self.generate_flag)
        elif self.test_type == "export":
            cmd = "cbexport"
            self.shell.execute_command("rm -rf %sexport " % self.tmp_path)
            self.shell.execute_command("mkdir %sexport " % self.tmp_path)
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
            if "Expected flag: file://%sdefault" % self.tmp_path in output \
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
        self.errors_flag = self.input.param("errors_flag", "")
        self.logs_flag = self.input.param("logs_flag", "")
        self.include_key_flag = self.input.param("include_key_flag", "")
        self.import_file = self.input.param("import_file", None)
        self.format_type = self.input.param("format_type", None)
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
                errors_path = "~/error"
                self.shell.execute_command("rm -rf ~/error")
            elif self.errors_flag == "absolute_path":
                errors_path = self.tmp_path + "errors/" + self.errors_flag
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
                logs_path = "~/log"
                self.shell.execute_command("rm -rf ~/log")
            elif self.logs_flag == "absolute_path":
                logs_path = self.tmp_path + "logs/" + self.logs_flag
        if self.test_type == "import":
            cmd = "cbimport"
            self._remote_copy_import_file(self.import_file)
            if self.imex_type == "json":
                for bucket in self.buckets:
                    key_gen = "%index%"
                    """ ./cbimport json -c 12.11.10.132 -u Administrator -p password
                        -b default -d file:///tmp/export/default -f list -g %index%  """
                    imp_cmd_str = "%s%s%s %s -c %s -u Administrator -p password -b %s "\
                                  "-d %s%s -f %s -g %%index%% %s %s %s %s %s %s"\
                             % (self.cli_command_path, cmd, self.cmd_ext,
                                            self.imex_type, server.ip, bucket.name,
                                    import_method, self.des_file, self.format_type,
                                                   threads_flag, self.threads_flag,
                                                          errors_flag, errors_path,
                                                              logs_flag, logs_path)
                    self.log.info("command to run %s " % imp_cmd_str)
                    output, error = self.shell.execute_command(imp_cmd_str)
                    self.log.info("Output from execute command %s " % output)
                    error_check = self._check_output_option_flags(output,
                                                  errors_path, logs_path)
                    if error_check and not self._check_output("successfully", output):
                        self.fail("failed to run optional flags")
        elif self.test_type == "export":
            cmd = "cbexport"
            self.shell.execute_command("rm -rf %sexport " % self.tmp_path)
            self.shell.execute_command("mkdir %sexport " % self.tmp_path)
            if self.imex_type == "json":
                for bucket in self.buckets:
                    self.log.info("load json to bucket %s " % bucket.name)
                    load_cmd = "%s%s%s -n %s:8091 -u Administrator -p password -j "\
                                                                     "-i %s -b %s "\
                            % (self.cli_command_path, "cbworkloadgen", self.cmd_ext,
                                             server.ip, self.num_items, bucket.name)
                    self.shell.execute_command(load_cmd)
                    export_file = self.ex_path + bucket.name
                    cmd_str = "%s%s%s %s -c %s -u Administrator -p password -b %s "\
                                    "  -f %s %s %s %s %s %s %s "\
                                     % (self.cli_command_path, cmd, self.cmd_ext,
                                          self.imex_type, server.ip, bucket.name,
                                 self.format_type, self.output_flag, export_file,
                           threads_flag, self.threads_flag, logs_flag, logs_path)
                    output, error = self.shell.execute_command(cmd_str)
                    self.log.info("Output from execute command %s " % output)
                    error_check = self._check_output_option_flags(output,
                                                              errors_path, logs_path)
                    if error_check and not self._check_output("successfully", output):
                        self.fail("failed to run optional flags")

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
            imp_cmd_str = "%s%s%s %s -c %s -u %s -p %s -b %s -d %s -f sample"\
                             % (self.cli_command_path, cmd, self.cmd_ext, self.imex_type,
                                         server.ip, username, password, self.sample_file,
                                                                      sample_file_path)
            output, error = self.shell.execute_command(imp_cmd_str)
            if not self._check_output("SUCCESS", output):
                self.log.info("Output from command %s" % output)
                self.fail("Failed to load sample file %s" % self.sample_file)

    """ imex_type=json,format_type=list,import_file=json_list_1000_lines
                                  =lines,.... """
    def test_import_json_file(self):
        options = {"load_doc": False}
        self.import_file = self.input.param("import_file", None)
        return self._common_imex_test("import", options)

    def test_import_json_generate_keys(self):
        options = {"load_doc": False}
        self.import_file = self.input.param("import_file", None)
        return self._common_imex_test("import", options)

    """ not in 4.6 """
    def test_import_json_with_limit_first_10_lines(self):
        options = {"load_doc": False}
        return self._common_imex_test("import", options)

    def test_import_csv_file(self):
        options = {"load_doc": False}
        self.import_file = self.input.param("import_file", None)
        return self._common_imex_test("import", options)

    def _common_imex_test(self, cmd, options):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        path = self.input.param("path", None)
        self.short_flag = self.input.param("short_flag", True)
        import_method = self.input.param("import_method", "file://")
        if "url" in import_method:
            import_method = ""
        self.ex_path = self.tmp_path + "export/"
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
        if "export" in cmd:
            cmd = "cbexport"
            if options["load_doc"]:
                if len(self.buckets) >= 1:
                    for bucket in self.buckets:
                        self.log.info("load json to bucket %s " % bucket.name)
                        load_cmd = "%s%s%s -n %s:8091 -u %s -p %s -j -i %s -b %s "\
                            % (self.cli_command_path, "cbworkloadgen", self.cmd_ext,
                               server.ip, username, password, options["docs"],
                               bucket.name)
                        self.shell.execute_command(load_cmd)
            """ remove previous export directory at tmp dir and re-create it
                in linux:   /tmp/export
                in windows: /cygdrive/c/tmp/export """
            self.shell.execute_command("rm -rf %sexport " % self.tmp_path)
            self.shell.execute_command("mkdir %sexport " % self.tmp_path)
            """ /opt/couchbase/bin/cbexport json -c localhost -u Administrator
                              -p password -b default -f list -o /tmp/test4.zip """
            if len(self.buckets) >= 1:
                for bucket in self.buckets:
                    export_file = self.ex_path + bucket.name
                    exe_cmd_str = "%s%s%s %s -c %s -u %s -p %s -b %s -f %s -o %s"\
                         % (self.cli_command_path, cmd, self.cmd_ext, self.imex_type,
                                     server.ip, username, password, bucket.name,
                                                    self.format_type, export_file)
                    output, error = self.shell.execute_command(exe_cmd_str)
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
                self.cluster.create_default_bucket(import_servers[2], "250", self.num_replicas,
                                               enable_replica_index=self.enable_replica_index,
                                               eviction_policy=self.eviction_policy)
                imp_cmd_str = "%s%s%s %s -c %s -u %s -p %s -b %s -d file://%s -f %s -g %s"\
                              % (self.cli_command_path, "cbimport", self.cmd_ext, self.imex_type,
                                 import_servers[2].ip, username, password, "default",
                                 import_file, self.format_type, "index")
                output, error = import_shell.execute_command(imp_cmd_str)
                if self._check_output("error", output):
                    self.fail("Fail to run import back to bucket")
        elif "import" in cmd:
            cmd = "cbimport"
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
                if self.imex_type == "csv":
                    format_flag = ""
                for bucket in self.buckets:
                    key_gen = "%index%"
                    """ ./cbimport json -c 12.11.10.132 -u Administrator -p password
                    -b default -d file:///tmp/export/default -f list -g %index%  """
                    imp_cmd_str = "%s%s%s %s -c %s -u %s -p %s -b %s -d %s%s %s %s -g %s"\
                         % (self.cli_command_path, cmd, self.cmd_ext, self.imex_type,
                                          server.ip, username, password, bucket.name,
                                                             import_method, des_file,
                                              format_flag, self.format_type, key_gen)
                    print "command  ", imp_cmd_str
                    output, error = self.shell.execute_command(imp_cmd_str)
                    self.log.info("Output from execute command %s " % output)
                    """ Json `file:///root/json_list` imported to `http://host:8091` successfully """
                    json_loaded = False
                    if "invalid" in self.import_file:
                        if self._check_output("Json import failed:", output):
                            json_loaded = True
                    elif self._check_output("successfully", output):
                        json_loaded = True
                    if not json_loaded:
                        self.fail("Failed to execute command")

    def _verify_export_file(self, export_file_name, options):
        if not options["load_doc"]:
            if "bucket" in options and options["bucket"] == "empty":
                output, error = self.shell.execute_command("ls %s" % self.ex_path)
                if export_file_name in output[0]:
                    self.log.info("check if export file %s is empty" % export_file_name)
                    output, error = self.shell.execute_command("cat %s%s"\
                                                 % (self.ex_path, export_file_name))
                    if output:
                        self.fail("file %s should be empty" % export_file_name)
                else:
                    self.fail("Fail to export.  File %s does not exist" \
                                                            % export_file_name)
        elif options["load_doc"]:
            found = self.shell.file_exists(self.ex_path, export_file_name)
            if found:
                self.log.info("copy export file from remote to local")
                if os.path.exists("/tmp/export"):
                    shutil.rmtree("/tmp/export")
                os.makedirs("/tmp/export")
                self.shell.copy_file_remote_to_local(self.ex_path+export_file_name,\
                                                    "/tmp/export/"+export_file_name)
                self.log.info("compare 2 json files")
                if self.format_type == "lines":
                    sample_file = open("resources/imex/json_%s_lines" % options["docs"])
                    samples = sample_file.readlines()
                    export_file = open("/tmp/export/"+ export_file_name)
                    exports = export_file.readlines()
                    if sorted(samples) == sorted(exports):
                        self.log.info("export and sample json mathch")
                    else:
                        self.fail("export and sample json does not match")
                    sample_file.close()
                    export_file.close()
                elif self.format_type == "list":
                    sample_file = open("resources/imex/json_list_%s_lines" % options["docs"])
                    samples = sample_file.read()
                    samples = ast.literal_eval(samples)
                    samples.sort(key=lambda k: k['name'])
                    export_file = open("/tmp/export/"+ export_file_name)
                    exports = export_file.read()
                    exports = ast.literal_eval(exports)
                    exports.sort(key=lambda k: k['name'])

                    if samples == exports:
                        self.log.info("export and sample json files are matched")
                    else:
                        self.fail("export and sample json files did not match")
                    sample_file.close()
                    export_file.close()
            else:
                self.fail("There is not export file in %s%s"\
                                  % (self.ex_path, export_file_name))

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
            if "Unable to process value for flag: -t" in output:
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
            output, error = self.shell.execute_command("ls %s " % self.cli_command_path)
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
