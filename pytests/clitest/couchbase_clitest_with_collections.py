import copy, json, os, random
import string, re, time, sys

from ep_mc_bin_client import MemcachedClient, MemcachedError
from remote.remote_util import RemoteMachineShellConnection
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection



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
                raise("collection does not enable by default")

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
                raise("No _default scope in cluster")
        else:
            custom_scopes = self.get_bucket_scope()
            for scope in custom_scopes:
                if not self._check_output(scope, output):
                    raise("No scope: {0} in cluster".format(scope))
        if not self.custom_collections:
            if not self._check_output("_default", output):
                raise("No _default collection in cluster")
        else:
            custom_collections = self.get_bucket_collection()
            for collection in custom_collections:
                if not self._check_output(collection, output):
                    raise("No collection: {0} in cluster".format(scope))

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
        scopes_id = []
        for scope in scopes:
            if scope == "_default" and self.custom_scopes:
                scopes.remove(scope)
                continue
            self.log.info("get scope id of scope: {0}".format(scope))
            scopes_id.append(self.get_scopes_id(scope))
        collections = self.get_bucket_collection()
        collections_id = []
        for collection in collections:
            if collection == "_default" and self.custom_collections:
                scopes.remove(scope)
                continue
            collections_id.append(self.get_collections_id(scope[0],collection))

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
                raise("Failed to load data to cluster using cbworkloadgen")

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
                errors = ["Scope with this name already exists",
                          "First character must not be"]
                for error in errors:
                    if error in str(e):
                        error_expected = True
                if not error_expected:
                    raise("Rest failed to block create scope with same name")
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
                errors = ["Collection with this name already exists",
                          "First character must not be"]
                for error in errors:
                    if error in str(e):
                        error_expected = True
                if not error_expected:
                    raise("Rest failed to block create collection with same name")

