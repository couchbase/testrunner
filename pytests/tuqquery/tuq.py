import ast
import boto3
import concurrent.futures
import copy
import datetime
import json
import logging
import os
import pprint
import random
import re
import socket
import threading
import time
import traceback
import urllib.parse
import uuid

from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator

import httplib2
import testconstants
import collections
from couchbase_helper.documentgenerator import JsonDocGenerator
from couchbase_helper.documentgenerator import WikiJSONGenerator
from couchbase_helper.documentgenerator import NapaDataLoader
from couchbase_helper.documentgenerator import SDKDataLoader
from subprocess import Popen, PIPE
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.tuq_generators import JsonGenerator
from basetestcase import BaseTestCase
from membase.api.exception import (CBQError, InvalidArgumentException, ReadDocumentException,
                                   ServerUnavailableException)
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from security.rbac_base import RbacBase
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.documentgenerator import JSONNonDocGenerator
from deepdiff import DeepDiff
from fts.random_query_generator.rand_query_gen import FTSFlexQueryGenerator
from pytests.fts.fts_base import FTSIndex
from pytests.fts.random_query_generator.rand_query_gen import DATASET
from pytests.fts.fts_base import CouchbaseCluster
from collection.collections_n1ql_client import CollectionsN1QL
from collection.collections_stats import CollectionsStats
from membase.helper.cluster_helper import ClusterOperationHelper
from lib.Cb_constants.CBServer import CbServer

JOIN_INNER = "INNER"
JOIN_LEFT = "LEFT"
JOIN_RIGHT = "RIGHT"


# EXPLAIN-plan vocabulary for the plan helpers in QueryTests.
# Leaf scans; 'index' names the index ('#primary', '#sequentialscan' included).
INDEX_SCAN_OPERATORS = frozenset([
    'IndexScan3', 'IndexScan2', 'IndexScan', 'PrimaryScan3', 'PrimaryScan',
    'IndexCountScan2', 'IndexCountScan', 'IndexCountDistinctScan2',
])
# Scans that combine leaf scans.
COMPOSITE_SCAN_OPERATORS = frozenset([
    'IntersectScan', 'OrderedIntersectScan', 'UnionScan', 'DistinctScan',
])
SCAN_OPERATORS = INDEX_SCAN_OPERATORS | COMPOSITE_SCAN_OPERATORS
PRIMARY_INDEX = '#primary'
SEQUENTIAL_SCAN = '#sequentialscan'
# queryN1QLFeatCtrl bit that turns sequential scans off.
SEQSCAN_OFF_BIT = 0x4000
# A recovered panic ("Panic: ...").
PANIC_ERROR_CODE = 5001
# "code": 5001 in a query response, as JSON or as a Python dict repr.
PANIC_CODE_RE = re.compile(r"""['"]code['"]\s*:\s*{}\b""".format(PANIC_ERROR_CODE))
MIB = 1024 * 1024
# cbq-engine resident memory (bytes) in admin/vitals.
VITALS_RSS = 'process.rss'
# Removed by normalize_plan(): values that differ between nodes or runs for the same plan.
VOLATILE_KEYS = frozenset([
    'optimizer_estimates', 'cost', 'cardinality', 'fr_cost', 'size',
    'index_definition_checksum', 'index_id',
    '#time', '#time_normal', '#time_absolute', '#stats',
])
_FORMAT_FIELDS = ('index', 'keyspace', 'as', 'covers', 'spans', 'condition', 'filter')
_MAX_FIELD_LEN = 120

# DocLoader PolymorphicDoc shapes and their weights (update if DocLoader changes them).
POLYMORPHIC_DOC_TYPES = {
    'transaction': 40, 'assignedProduct': 20, 'profile': 20,
    'media_element': 12, 'mobile': 8, 'catalogItem': 6,
}
POLYMORPHIC_VALUE_TYPE = 'PolymorphicDoc'


class QueryTests(BaseTestCase):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp' \
                and str(self.__class__).find('upgrade_n1qlrbac') == -1 \
                and str(self.__class__).find("QueryCollectionsClusteropsTests") == -1 \
                and str(self.__class__).find('n1ql_upgrade') == -1 \
                and str(self.__class__).find('N1qlFTSIntegrationTest') == -1 \
                and str(self.__class__).find('N1qlFTSIntegrationPhase2Test') == -1 \
                and str(self.__class__).find('N1qlFTSIntegrationPhase2ClusteropsTest') == -1 \
                and str(self.__class__).find('QueryCollectionsEnd2EndTests') == -1 \
                and str(self.__class__).find('FlexIndexTests') == -1 \
                and str(self.__class__).find('AggregatePushdownRecoveryClass') == -1 \
                and str(self.__class__).find('ClusterOpsLargeMetaKV') == -1:
            self.skip_buckets_handle = True
        else:
            self.skip_buckets_handle = False
        super(QueryTests, self).setUp()
        if self.input.param("force_clean", False):
            self.skip_buckets_handle = False
            super(QueryTests, self).setUp()
        self.log.info("==============  QueryTests setup has started ==============")
        self.version = self.input.param("cbq_version", "sherlock")
        self.flat_json = self.input.param("flat_json", False)
        self.directory_flat_json = self.input.param("directory_flat_json", "/tmp/")
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        if self.input.param("start_cmd", True) and self.input.param("cbq_version", "sherlock") != 'sherlock':
            self._start_command_line_query(self.master, user=self.master.rest_username,
                                           password=self.master.rest_password)
        self.query_buckets = None
        self.test_buckets = self.input.param('test_buckets', 'default')
        if (self.test_buckets == "default") & (self.default_bucket == False) :
            self.test_buckets = None
        self.sample_bucket = self.input.param('sample_bucket', None)
        self.sample_bucket_index = self.input.param('sample_bucket_index', None)
        self.users = self.input.param("users", {})
        self.use_rest = self.input.param("use_rest", True)
        self.hint_index = self.input.param("hint", None)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.ipv6 = self.input.param("ipv6", False)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.analytics = self.input.param("analytics", False)
        self.query_context = self.input.param("query_context", None)
        self.dataset = getattr(self, 'dataset', self.input.param("dataset", "default"))
        self.primary_indx_type = self.input.param("primary_indx_type", 'GSI')
        self.index_type = self.input.param("index_type", 'GSI')
        self.skip_primary_index_for_collection = self.input.param("skip_primary_index_for_collection", False)
        self.skip_primary_index = self.input.param("skip_primary_index", False)
        self.primary_index_timeout = self.input.param("primary_index_timeout", 300)
        self.fail_on_primary_index_error = self.input.param("fail_on_primary_index_error", False)
        self.primary_indx_drop = self.input.param("primary_indx_drop", False)
        self.monitoring = self.input.param("monitoring", False)
        self.value_size = self.input.param("value_size", 0)
        self.isprepared = False
        self.named_prepare = self.input.param("named_prepare", None)
        self.udfs = self.input.param("udfs", False)
        self.encoded_prepare = self.input.param("encoded_prepare", False)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')
        if not self.capella_run:
            shell = RemoteMachineShellConnection(self.master)
            type = shell.extract_remote_info().distribution_type
            shell.disconnect()
        else:
            type = ""
        self.path = testconstants.LINUX_COUCHBASE_BIN_PATH
        self.array_indexing = self.input.param("array_indexing", False)
        self.load_sample = self.input.param("load_sample", False)
        self.gens_load = self.gen_docs(self.docs_per_day)
        self.log.info("--->gens_load is done...")
        self.skip_load = self.input.param("skip_load", False)
        self.skip_index = self.input.param("skip_index", False)
        self.plasma_dgm = self.input.param("plasma_dgm", False)
        self.use_server_groups = self.input.param("use_server_groups", True)
        self.server_group_map = {}
        if self.nodes_init == 1:
            self.server_grouping = "0"
        elif self.nodes_init == 2:
            self.server_grouping = "0:1"
        elif self.nodes_init == 3:
            self.server_grouping = "0:1:2"
        else:
            self.server_grouping = self.input.param("server_grouping", "0-1:2-3")
        self.DGM = self.input.param("DGM", False)
        self.covering_index = self.input.param("covering_index", False)
        self.cluster_ops = self.input.param("cluster_ops", False)
        self.server = self.master
        self.log.info("-->starting rest connection...")
        self.rest = RestConnection(self.server)
        self.username = self.rest.username
        self.password = self.rest.password
        self.cover = self.input.param("cover", False)
        self.bucket_name = self.input.param("bucket_name", "default")
        self.rbac_context = self.input.param("rbac_context", "")
        self.curl_path = "curl"
        self.scope = self.input.param("scope", "test")
        self.collections = self.input.param("collections", ["test1", "test2"])
        self.n1ql_certs_path = "/opt/couchbase/var/lib/couchbase/n1qlcerts"
        self.query_context = self.input.param("query_context", '')
        self.load_collections = self.input.param("load_collections", False)
        self.primary_index_collections = self.input.param("primary_index_collections", False)
        self.use_advice = self.input.param("use_advice", False)
        self.measure_code_coverage = self.input.param("measure_code_coverage", False)
        self.s3_bucket_cc_name = self.input.param("s3_bucket_cc_name", None)
        if type.lower() == 'windows':
            self.path = testconstants.WIN_COUCHBASE_BIN_PATH
            self.curl_path = "%scurl" % self.path
            self.n1ql_certs_path = "/cygdrive/c/Program\ Files/Couchbase/server/var/lib/couchbase/n1qlcerts"
        elif type.lower() == "mac":
            self.path = testconstants.MAC_COUCHBASE_BIN_PATH
        if self.primary_indx_type.lower() == "gsi":
            self.gsi_type = self.input.param("gsi_type", 'plasma')
        self.reload_data = self.input.param("reload_data", False)
        self.get_version = self.rest.get_nodes_versions()
        if self.use_server_groups and not CbServer.capella_run and "enterprise" in str(self.get_version):
            self._create_server_groups()
        if self.reload_data:
            self.log.info(f"--> reload_data: {self.reload_data}")
            if self.analytics:
                self.cluster.rebalance([self.master, self.cbas_node], [], [self.cbas_node], services=['cbas'])
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket=bucket, timeout=180000)
            # Adding sleep after flushing buckets (see CBQE-5838)
            self.sleep(10)
            self.gens_load = self.gen_docs(self.docs_per_day)
            verify_data = self.input.param("verify_data", False)
            if CbServer.capella_run:
                verify_data = False
            self.load(self.gens_load, batch_size=1000, flag=self.item_flag, verify_data=verify_data)
            if self.analytics:
                self.cluster.rebalance([self.master, self.cbas_node], [self.cbas_node], [], services=['cbas'])
        if not (hasattr(self, 'skip_generation') and self.skip_generation):
            self.full_list = self.generate_full_docs_list(self.gens_load)
        if self.input.param("gomaxprocs", None):
            self.configure_gomaxprocs()
        try:
            self.docs_per_day = int(self.docs_per_day)
        except ValueError:
            self.docs_per_day = 0
            pass
        if self.docs_per_day > 0:
            self.log.info("--> docs_per_day>0..generating TuqGenerators...")
            # generate_full_docs_list() is deterministic, and was previously called a
            # second time here - ~14s per setUp at doc-per-day=49 (job 86079, 50.8% of a
            # median test). Reuse the list built above when it exists. Sharing one list is
            # safe: TuqGenerators assigns self.full_set once (tuq_generators.py:18) and
            # only ever reads it (:330,:332,:481-491), and self.full_list is read-only too.
            full_docs_list = getattr(self, 'full_list', None)
            if full_docs_list is None:
                full_docs_list = self.generate_full_docs_list(self.gens_load)
            self.gen_results = TuqGenerators(self.log, full_docs_list)
            self.log.info("--> End: docs_per_day>0..generating TuqGenerators...")
        if str(self.__class__).find('QueriesUpgradeTests') == -1 and str(self.__class__).find('ClusterOpsLargeMetaKV') == -1 and str(self.__class__).find('FlexIndexTests') == -1:
            if not (self.analytics and self.skip_primary_index_for_collection):
                self.log.info("--> start: create_primary_index_for_3_0_and_greater...")
                self.create_primary_index_for_3_0_and_greater()
                self.log.info("--> End: create_primary_index_for_3_0_and_greater...")
        if self.analytics:
            self.setup_analytics()
        if self.monitoring:
            self.run_cbq_query('delete from system:prepareds')
            self.run_cbq_query('delete from system:completed_requests')
        self.log.info("==============  QueryTests setup has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryTests suite_setup has started ==============")
        try:
            changed = False
            self.collections_helper = CollectionsN1QL(self.master)
            # if os != 'windows':
            # self.sleep(10, 'sleep before load')
            if not self.skip_load:
                if self.flat_json:
                    self.log.info("-->gens_load flat_json")
                    self.load_directory(self.gens_load)
                else:
                    self.log.info("-->gens_load flat_json, batch_size=1000")
                    verify_data = self.input.param("verify_data", False)
                    if self.enforce_tls or CbServer.capella_run:
                        verify_data = False
                    self.load(self.gens_load, batch_size=1000, flag=self.item_flag, verify_data=verify_data)
            if not self.input.param("skip_build_tuq", True):
                self._build_tuq(self.master)
            self.skip_buckets_handle = True
            if self.analytics:
                if self.cbas_node is not self.master:
                    self.cluster.rebalance([self.master, self.cbas_node], [self.cbas_node], [], services=['cbas'])
                self.setup_analytics()
                # self.sleep(30, 'wait for analytics setup')
            if self.load_collections:
                self.collections_helper.create_scope(bucket_name="default",scope_name=self.scope)
                self.collections_helper.create_collection(bucket_name="default",scope_name=self.scope,collection_name=self.collections[0])
                self.collections_helper.create_collection(bucket_name="default",scope_name=self.scope,collection_name=self.collections[1])
                if self.analytics:
                    self.run_cbq_query(query="CREATE DATASET collection1 on default.test.test1")
                    self.run_cbq_query(query="CREATE DATASET collection2 on default.test.test2")
                if not self.use_advice:
                    if not self.analytics:
                        self.run_cbq_query(query="CREATE INDEX idx1 on default:default.{0}.{1}(name)".format(self.scope, self.collections[0]))
                        self.run_cbq_query(query="CREATE INDEX idx2 on default:default.{0}.{1}(name)".format(self.scope, self.collections[1]))
                        self.run_cbq_query(query="CREATE INDEX idx3 on default:default.test.{1}(nested)".format(self.scope, self.collections[0]))
                        self.run_cbq_query(query="CREATE INDEX idx4 on default:default.test.{1}(ALL numbers)".format(self.scope, self.collections[0]))
                        if self.primary_index_collections:
                            self.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.{0}.{1}".format(self.scope, self.collections[0]))
                            self.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.{0}.{1}".format(self.scope, self.collections[1]))
                        # No fixed sleep here: wait_for_all_indexes_online() already polls
                        # each index to 'online' (1s granularity, 600s budget).
                        self.wait_for_all_indexes_online()
                if self.analytics:
                    self.analytics = False
                    changed = True
                self.run_cbq_query(
                    query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
                self.run_cbq_query(
                    query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
                self.run_cbq_query(
                    query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[1]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" })'))
                self.run_cbq_query(
                    query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + ' (KEY, VALUE) VALUES ("key3", { "nested" : {"fields": "fake"}, "name" : "old hotel" })'))
                self.run_cbq_query(
                    query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + ' (KEY, VALUE) VALUES ("key4", { "numbers": [1,2,3,4] , "name" : "old hotel" })'))
                # No fixed sleep after the INSERTs: run_cbq_query sends
                # scan_consistency=REQUEST_PLUS by default (self.scan_consistency, set in
                # setUp), so the next query already waits for the index to reflect these
                # mutations. This blind 20s was ~20s of every collections suite_setUp.
                if changed:
                    self.analytics = True
        except Exception as ex:
            self.log.error('SUITE SETUP FAILED')
            self.log.info(ex)
            traceback.print_exc()
            self.tearDown()
        self.log.info("==============  QueryTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryTests tearDown has started ==============")
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        if self.analytics == True:
            bucket_username = "cbadminbucket"
            bucket_password = "password"
            data = 'use Default ;'
            for bucket in self.buckets:
                data += 'disconnect bucket {0} if connected;'.format(bucket.name)
                data += 'drop dataset {0} if exists;'.format(bucket.name + "_shadow")
                data += 'drop bucket {0} if exists;'.format(bucket.name)
            self.write_file("file.txt", data)
            url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
            cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
            os.system(cmd)
            # os.remove(filename)
        # Check for panics in query service logs
        if not getattr(self, 'capella_run', False):
            self.check_query_service_logs_for_panic()

        self.log.info("==============  QueryTests tearDown has completed ==============")
        super(QueryTests, self).tearDown()

    def check_query_service_logs_for_panic(self):
        """
        Check query service (n1ql/cbq-engine) logs for panic messages.
        Similar to check_gsi_logs_for_panic in gsi/newtuq.py.
        """
        panic_str = "panic"
        fatal_str = "fatal error"
        runtime_error_str = "runtime error"
        strings_to_monitor = [panic_str, fatal_str, runtime_error_str]
        fail_test = False
        panic_details = []

        # Get all n1ql nodes
        try:
            n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        except Exception as e:
            self.log.warning(f"Could not get n1ql nodes for panic check: {str(e)}")
            return

        if not n1ql_nodes:
            self.log.info("No n1ql nodes found, skipping query service panic check")
            return

        for server in n1ql_nodes:
            shell = None
            try:
                shell = RemoteMachineShellConnection(server)
                # Get the log directory path
                try:
                    _, log_dir = RestConnection(server).diag_eval(
                        'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
                except Exception as e:
                    # Fallback to default log location
                    log_dir = "/opt/couchbase/var/lib/couchbase/logs"
                    self.log.warning(f"Could not get log dir via diag_eval, using default: {log_dir}")

                query_log = str(log_dir) + '/query.log*'

                for string in strings_to_monitor:
                    # Use zgrep to search compressed and uncompressed logs
                    cmd = f'zgrep -i "{string}" {query_log} 2>/dev/null | grep -v "\"level\":\"info\"" | wc -l'
                    count, err = shell.execute_command(cmd)

                    if isinstance(count, list):
                        try:
                            count = int(count[0].strip())
                        except (ValueError, IndexError):
                            count = 0
                    else:
                        try:
                            count = int(str(count).strip())
                        except ValueError:
                            count = 0

                    if count > 0:
                        self.log.error(f"===== '{string}' OBSERVED {count} TIME(S) IN QUERY LOGS ON SERVER {server.ip} =====")
                        
                        # Get sample panic lines for debugging
                        sample_cmd = f'zgrep -i "{string}" {query_log} 2>/dev/null | grep -v "\"level\":\"info\"" | tail -5'
                        sample_output, _ = shell.execute_command(sample_cmd)
                        if sample_output:
                            self.log.error(f"Sample '{string}' entries from {server.ip}:")
                            for line in sample_output[:5]:
                                self.log.error(f"  {line}")
                            panic_details.append({
                                'server': server.ip,
                                'pattern': string,
                                'count': count,
                                'samples': sample_output[:5]
                            })
                        fail_test = True

            except Exception as e:
                self.log.warning(f"Error checking query logs on {server.ip}: {str(e)}")
            finally:
                if shell:
                    shell.disconnect()

        if fail_test:
            error_msg = f"PANIC/FATAL ERROR detected in query service logs! Details: {panic_details}"
            self.log.error(error_msg)
            # Don't fail the test immediately, just log the error
            # Uncomment the line below to fail tests on panic detection
            # self.fail(error_msg)

    def suite_tearDown(self):
        self.log.info("==============  QueryTests suite_tearDown has started ==============")
        if self.measure_code_coverage:
            self.log.info("In suite tearDown")
            test_cc_prefix = f"{self.s3_bucket_cc_name}/query{self.case_number}_{self._testMethodName}"
            self.log.info(f"test_cc_prefix is: {test_cc_prefix}")
            self.upload_coveragefiles_s3(test_cc_prefix, self.aws_access_key_id, self.aws_secret_access_key)
        if not self.input.param("skip_build_tuq", True):
            if hasattr(self, 'shell'):
                self._kill_all_processes_cbq()
        self.log.info("==============  QueryTests suite_tearDown has completed ==============")

    ##############################################################################################
    #
    #  Setup Helpers
    ##############################################################################################

    def log_config_info(self, query_node=None):
        try:
            current_indexes = []
            query_response = self.run_cbq_query("SELECT * FROM system:indexes WHERE indexes.bucket_id is missing", server=query_node)
            current_indexes = [(i['indexes']['name'],
                                i['indexes']['keyspace_id'],
                                frozenset([key.replace('`', '').replace('(', '').replace(')', '')
                                           for key in i['indexes']['index_key']]),
                                i['indexes']['state'],
                                i['indexes']['using']) for i in query_response['results']]
            # get all buckets
            query_response = self.run_cbq_query("SELECT * FROM system:keyspaces WHERE keyspaces.`bucket` is missing", server=query_node)
            buckets = [i['keyspaces']['name'] for i in query_response['results']]
            self.log.info("==============  System Config: ==============\n")
            for bucket in buckets:
                query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + bucket + "`", server=query_node)
                docs = query_response['results'][0]['$1']
                bucket_indexes = []
                for index in current_indexes:
                    if index[1] == bucket:
                        bucket_indexes.append(index[0])
                self.log.info("Bucket: " + bucket)
                self.log.info("Indexes: " + str(bucket_indexes))
                self.log.info("Docs: " + str(docs) + "\n")
            self.log.info("=============================================")
        except Exception as e:
            pass

    # Collect code coverage files and upload them to s3
    def upload_coveragefiles_s3(self, s3_bucket_cc_name_prefix, aws_access_key_id, aws_secret_access_key):
        self.log.info("Uploading files to s3")
        for node in self.servers[:self.nodes_init]:
            coverage_dir = os.path.join("/tmp", f"coverage_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}")
            os.makedirs(coverage_dir)
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()
            time.sleep(5)
            try:
                shell.get_all_files("/tmp/coverage", coverage_dir)
                s3_client = boto3.client('s3', region_name='us-west-1',
                                         aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key)
                dest_bucket = "qebucket"
                for path, subdirs, files in os.walk(coverage_dir):
                    for file in files:
                        self.log.info(f"Found file : {os.path.join(path, file)}")
                        s3_client.upload_file(Filename=os.path.join(path, file), Bucket=dest_bucket, Key=f"{s3_bucket_cc_name_prefix}_{node.ip}/{file}")
            except Exception as e:
                self.log.info("Failed to upload file")
            shell.start_couchbase()
            time.sleep(10)
            shell.disconnect()

    def fail_if_no_buckets(self):
        buckets = False
        for a_bucket in self.buckets:
            buckets = True
        if not buckets:
            self.fail('FAIL: This test requires buckets')

    def write_file(self, filename, data):
        f = open(filename, 'w')
        f.write(data)
        f.close()

    def setup_analytics(self):
        data = 'use Default;'
        self.log.info("No. of buckets : %s", len(self.buckets))
        bucket_username = "cbadminbucket"
        bucket_password = "password"
        for bucket in self.buckets:
            data += 'create bucket {0} with {{"bucket":"{0}","nodes":"{1}"}} ;'.format(bucket.name, self.cbas_node.ip)
            data += 'create shadow dataset {1} on {0}; '.format(bucket.name, bucket.name + "_shadow")
            data += 'connect bucket {0} with {{"username":"{1}","password":"{2}"}};'.format(bucket.name,
                                                                                            bucket_username,
                                                                                            bucket_password)
        self.write_file("file.txt", data)
        url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
        cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
        os.system(cmd)
        # os.remove(filename)

    def _load_emp_dataset_on_all_buckets(self, op_type="create", expiration=0, start=0,
                          end=1000):
        # Load Emp Dataset
        for bucket in self.buckets:
            retry = 0
            while retry <= 5:
                try:
                    self.cluster.bucket_flush(self.master, bucket)
                except Exception as e:
                    self.sleep(60)
                    retry += 1
                    continue
                else:
                    break
            if retry > 5:
                raise Exception("bucket flush failed after 5 attempts")

        if end > 0:
            self._kv_gen = JsonDocGenerator("emp_",
                                            encoding="utf-8",
                                            start=start,
                                            end=end)
            gen = copy.deepcopy(self._kv_gen)

            for bucket in self.buckets:
                self._load_bucket(bucket, self.servers[0], gen, op_type,
                                  expiration)

    def _load_emp_dataset(self, op_type="create", expiration=0, start=0,
                          end=1000):
        # Load Emp Dataset
        self.cluster.bucket_flush(self.master)

        if end > 0:
            self._kv_gen = JsonDocGenerator("emp_",
                                            encoding="utf-8",
                                            start=start,
                                            end=end)
            gen = copy.deepcopy(self._kv_gen)

            self._load_bucket(self.buckets[0], self.servers[0], gen, op_type,
                              expiration)

    def _load_napa_dataset(self, op_type="create", expiration=0, start=0,
                          end=1000):
        # Load Emp Dataset
        self.cluster.bucket_flush(self.master)
        # Adding sleep after flushing buckets (see CBQE-5838)
        self.sleep(30)

        if end > 0:
            self._kv_gen = NapaDataLoader("napa_",
                                            encoding="utf-8",
                                            start=start,
                                            end=end)
            gen = copy.deepcopy(self._kv_gen)

            self._load_bucket(self.buckets[0], self.servers[0], gen, op_type,
                              expiration)

    def _load_wiki_dataset(self, op_type="create", expiration=0, start=0,
                           end=1000):

        if end > 0:
            self._kv_gen = WikiJSONGenerator("wiki_",
                                             encoding="utf-8",
                                             start=start,
                                             end=end)
            gen = copy.deepcopy(self._kv_gen)

            self._load_bucket(self.buckets[0], self.servers[0], gen, op_type,
                              expiration)

    def create_fts_index(self, name, source_type='couchbase',
                         source_name=None, index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None, doc_count=1000, index_storage_type=None, cbcluster=None):
        """Create fts index/alias
        @param node: Node on which index is created
        @param name: name of the index/alias
        @param source_type : 'couchbase' or 'files'
        @param source_name : name of couchbase bucket or "" for alias
        @param index_type : 'fulltext-index' or 'fulltext-alias'
        @param index_params :  to specify advanced index mapping;
                                dictionary overriding params in
                                INDEX_DEFAULTS.BLEVE_MAPPING or
                                INDEX_DEFAULTS.ALIAS_DEFINITION depending on
                                index_type
        @param plan_params : dictionary overriding params defined in
                                INDEX_DEFAULTS.PLAN_PARAMS
        @param source_params: dictionary overriding params defined in
                                INDEX_DEFAULTS.SOURCE_CB_PARAMS or
                                INDEX_DEFAULTS.SOURCE_FILE_PARAMS
        @param source_uuid: UUID of the source, may not be used
        """
        if cbcluster:
            self.cbcluster = cbcluster
        else:
            self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self.keyword_analyzer_at_type_mapping = self.input.param("keyword_analyzer_at_type_mapping", False)
        if not self.custom_map:
            if self.keyword_analyzer_at_type_mapping:
                index_params = {
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "enabled": True,
                        "dynamic": True,
                        "default_analyzer": "keyword"
                    }
                }
            else:
                index_params = {
                    "default_analyzer": "keyword",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "enabled": True,
                        "dynamic": True,
                        "default_analyzer": "keyword"
                    }
                }

        if not plan_params:
            plan_params = {'numReplicas': 0}

        fts_index = FTSIndex(
            self.cbcluster,
            name,
            source_type,
            source_name,
            index_type,
            index_params,
            plan_params,
            source_params,
            source_uuid,
            self.dataset,
            index_storage_type
        )
        fts_index.create()

        self.wait_for_fts_indexing_complete(fts_index, doc_count)

        return fts_index

    def wait_for_fts_indexing_complete(self, fts_index, doc_count):
        indexed_doc_count = 0
        retry_count = self.input.param("fts_index_wait_retries", 24)
        while indexed_doc_count < doc_count and retry_count > 0:
            try:
                self.sleep(10)
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError as k:
                pass
            retry_count -= 1

        if indexed_doc_count != doc_count:
            self.fail(
                "FTS indexing did not complete. FTS index count : {0}, Bucket count : {1}".format(indexed_doc_count,
                                                                                                  doc_count))

    def _load_test_buckets(self, create_index=True):
        if self.get_bucket_from_name("beer-sample") is None:
            self.rest.load_sample("beer-sample")
            self.wait_for_buckets_status({"beer-sample": "healthy"}, 5, 120)
            self.wait_for_bucket_docs({"beer-sample": 7303}, 5, 120)

        if create_index:
            if not self.is_index_present("beer-sample", "beer_sample_code_idx"):
                self.run_cbq_query("create index beer_sample_code_idx on `beer-sample` (`beer-sample`.code)")
            if not self.is_index_present("beer-sample", "beer_sample_brewery_id_idx"):
                self.run_cbq_query("create index beer_sample_brewery_id_idx on `beer-sample` (`beer-sample`.brewery_id)")
            self.wait_for_all_indexes_online()

    def _create_user(self, user, bucket_name='default'):
        user_to_create = None
        rolelist = None
        user_role_map = [{
                'admin_user': [{'id': 'admin_user', 'name': 'admin_user', 'password': 'password'}],
                'rolelist': [{'id': 'admin_user', 'name': 'admin_user', 'roles': 'admin'}]
            },
            {
                'all_buckets_data_reader_search_admin': [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin', 'password': 'password'}],
                'rolelist': [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin', 'roles': 'query_select[*],fts_admin[*],query_external_access'}]
            },
            {
                'all_buckets_data_reader_search_reader':  [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader', 'password': 'password'}],
                'rolelist': [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader', 'roles': 'query_select[*],fts_searcher[*],query_external_access'}]
            },
            {
                'test_bucket_data_reader_search_admin': [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin', 'password': 'password'}],
                'rolelist': [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin', 'roles': 'query_select['+bucket_name+'],fts_admin['+bucket_name+'],query_external_access'}]
            },
            {
                'test_bucket_data_reader_null': [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'password': 'password'}],
                'rolelist': [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'roles': 'query_select['+bucket_name+'],query_external_access'}]
            },
            {
                'test_bucket_data_reader_search_reader': [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader', 'password': 'password'}],
                'rolelist': [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader', 'roles': 'query_select['+bucket_name+'],fts_searcher['+bucket_name+'],query_external_access'}]
            },
            {
                'all_buckets_data_reader_null': [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'password': 'password'}],
                'rolelist': [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'roles': 'query_select[*],query_external_access'}]
            },
            {
                'all_buckets_null_search_admin': [{'id': 'all_buckets_null_search_admin', 'name': 'all_buckets_null_search_admin', 'password': 'password'}],
                'rolelist': [{'id': 'all_buckets_null_search_admin', 'name': 'all_buckets_null_search_admin', 'roles': 'fts_admin[*],query_external_access'}]
            },
            {
                'all_buckets_null_null': [{'id': 'all_buckets_null_null', 'name': 'all_buckets_null_null', 'password': 'password'}],
                'rolelist': [{'id': 'all_buckets_null_null', 'name': 'all_buckets_null_null', 'roles': 'query_external_access'}]
            },
            {
                'all_buckets_null_search_reader': [{'id': 'all_buckets_null_search_reader', 'name': 'all_buckets_null_search_reader', 'password': 'password'}],
                'rolelist': [{'id': 'all_buckets_null_search_reader', 'name': 'all_buckets_null_search_reader', 'roles': 'fts_searcher[*],query_external_access'}]
            },
            {
                'test_bucket_null_search_admin': [{'id': 'test_bucket_null_search_admin', 'name': 'test_bucket_null_search_admin', 'password': 'password'}],
                'rolelist': [{'id': 'test_bucket_null_search_admin', 'name': 'test_bucket_null_search_admin', 'roles': 'fts_admin['+bucket_name+'],query_external_access'}]
            },
            {
                'test_bucket_null_search_reader': [{'id': 'test_bucket_null_search_reader', 'name': 'test_bucket_null_search_reader', 'password': 'password'}],
                'rolelist': [{'id': 'test_bucket_null_search_reader', 'name': 'test_bucket_null_search_reader', 'roles': 'fts_searcher['+bucket_name+'],query_external_access'}]
            }
        ]

        for user_info in user_role_map:
            if user in user_info.keys():
                user_to_create = user_info[user]
                rolelist = user_info['rolelist']
                break
        if user_to_create and rolelist:
            RbacBase().create_user_source(user_to_create, 'builtin', self.master)
            RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
            self.users[user] = {'username': user, 'password': 'password'}
        else:
            self.fail("{0} looks like an invalid user".format(user))

    def generate_random_queries(self, fields=None, num_queries=1, query_type=["match"],
                                seed=0):
        """
         Calls FTS-FLex Query Generator for employee dataset
         @param num_queries: number of queries to return
         @query_type: a list of different types of queries to generate
                      like: query_type=["match", "match_phrase","bool",
                                        "conjunction", "disjunction"]
        """
        self.query_gen = FTSFlexQueryGenerator(num_queries, query_type=query_type,
                                               seed=seed, dataset=self.dataset,
                                               fields=fields)

    def update_expected_fts_index_map(self, fts_index):
        if not fts_index.smart_query_fields:
            fts_index.smart_query_fields = DATASET.FIELDS["emp"]
        for f, v in fts_index.smart_query_fields.items():
            for field in v:
                field = self.query_gen.replace_underscores(field)
                if field not in self.expected_fts_index_map.keys():
                    self.expected_fts_index_map[field] = [fts_index.name]
                else:
                    self.expected_fts_index_map[field].append(fts_index.name)

        # for type field always have ftx index as expected index
        if "type" not in self.expected_fts_index_map.keys():
            self.expected_fts_index_map["type"] = [fts_index.name]
        else:
            self.expected_fts_index_map["type"].append(fts_index.name)
        self.log.info("expected_fts_index_map {0}".format(self.expected_fts_index_map))

    def get_index_storage_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'stats/storage'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        index_storage_stats = {}
        for index_stats in json_parsed:
            bucket = index_stats["Index"].split(":")[0]
            index_name = index_stats["Index"].split(":")[1]
            if not bucket in list(index_storage_stats.keys()):
                index_storage_stats[bucket] = {}
            index_storage_stats[bucket][index_name] = index_stats["Stats"]
        return index_storage_stats

    def get_dgm_for_plasma(self, indexer_nodes=None, memory_quota=256):
        """
        Internal Method to create OOM scenario
        :return:
        """

        def validate_disk_writes(indexer_nodes=None):
            if not indexer_nodes:
                indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            for node in indexer_nodes:
                indexer_rest = RestConnection(node)
                content = self.get_index_storage_stats()
                for index in list(content.values()):
                    for stats in list(index.values()):
                        if stats["MainStore"]["resident_ratio"] >= 1.00:
                            return False
            return True

        def kv_mutations(self, docs=1):
            if not docs:
                docs = self.docs_per_day
            gens_load = self.gen_docs(docs)
            self.full_docs_list = self.generate_full_docs_list(gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.load(gens_load, buckets=self.buckets, flag=self.item_flag,
                      verify_data=False, batch_size=1000)

        if self.gsi_type != "plasma":
            return
        if not self.plasma_dgm:
            return
        self.log.info("Trying to get all indexes in DGM...")
        self.log.info("Setting indexer memory quota to {0} MB...".format(memory_quota))
        node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(node)
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=memory_quota)
        cnt = 0
        docs = 50 + self.docs_per_day
        while cnt < 100:
            if validate_disk_writes(indexer_nodes):
                self.log.info("========== DGM is achieved ==========")
                return True
            kv_mutations(self, docs)
            self.sleep(30)
            cnt += 1
            docs += 20
        return False

    # This method is not being used
    def print_list_of_dicts(self, list_to_print, num_elements=10):
        print('\n\n')
        print('Printing a list...')
        for item in list_to_print:
            self.print_dict(item)
            num_elements = num_elements - 1
            if num_elements == 0:
                break

    # This method is only used by the function right above it, which is not being used
    def print_dict(self, dict_to_print):
        for k, v in dict_to_print.items():
            print((k, v))
        print('\n')

        def get_user_list(self):
            """
            :return:  a list of {'id': 'userid', 'name': 'some_name ,
            'password': 'passw0rd'}
            """

        user_list = []
        for user in self.inp_users:
            user_list.append({att: user[att] for att in ('id',
                                                         'name',
                                                         'password')})
        return user_list

    def get_user_role_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
         'roles': 'admin:fts_admin[default]'}
        """
        user_role_list = []
        for user in self.inp_users:
            user_role_list.append({att: user[att] for att in ('id',
                                                              'name',
                                                              'roles')})
        return user_role_list

    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin', self.master)
        self.log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def assign_role(self, rest=None, roles=None):
        if not rest:
            rest = RestConnection(self.master)
        # Assign roles to users
        if not roles:
            roles = self.roles
        RbacBase().add_user_role(roles, rest, 'builtin')
        for user_role in roles:
            self.log.info("SUCCESS: Role(s) %s assigned to %s"
                          % (user_role['roles'], user_role['id']))

    def does_test_meet_server_version(self, required_major_version=-1, required_minor_version1=-1,
                                      required_minor_version2=-1):
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        server_version = versions[0].split('-')[0]
        server_version_major = int(server_version.split(".")[0])
        server_version_minor1 = int(server_version.split(".")[1])
        server_version_minor2 = int(server_version.split(".")[2])

        if server_version_major >= required_major_version:
            if server_version_minor1 >= required_minor_version1:
                if server_version_minor2 >= required_minor_version2:
                    return True

        return False

    ##############################################################################################
    #
    #   Query Runner
    ##############################################################################################
    def query_runner(self, test_dict):
        test_results = dict()
        restore_indexes = self.get_parsed_indexes()
        res_dict = dict()
        res_dict['errors'] = []
        for test_name in sorted(test_dict.keys()):
            try:
                res_dict = dict()
                res_dict['errors'] = []
                index_list = test_dict[test_name].get('indexes', [
                    {'name': '#primary',
                     'bucket': 'default',
                     'fields': [],
                     'state': 'online',
                     'using': self.index_type.lower(),
                     'is_primary': True}])
                pre_queries = test_dict[test_name].get('pre_queries', [])
                queries = test_dict[test_name].get('queries', [])
                post_queries = test_dict[test_name].get('post_queries', [])
                asserts = test_dict[test_name].get('asserts', [])
                cleanups = test_dict[test_name].get('cleanups', [])
                server = test_dict[test_name].get('server', None)
                query_params = test_dict[test_name].get('query_params', {})

                # INDEX STAGE

                self.log.info("--> index stage")
                current_indexes = self.get_parsed_indexes()
                desired_indexes = self.parse_desired_indexes(index_list)
                desired_index_set = self.make_hashable_index_set(desired_indexes)
                current_index_set = self.make_hashable_index_set(current_indexes)

                # drop all undesired indexes

                self.log.info("--> drop all undesired indexes...")
                self.drop_undesired_indexes(desired_index_set, current_index_set, current_indexes)

                # create desired indexes

                self.log.info("--> create desired indexes...")
                current_indexes = self.get_parsed_indexes()
                current_index_set = self.make_hashable_index_set(current_indexes)
                self.create_desired_indexes(desired_index_set, current_index_set, desired_indexes)

                res_dict['pre_q_res'] = []
                res_dict['q_res'] = []
                res_dict['post_q_res'] = []
                res_dict['errors'] = []
                res_dict['cleanup_res'] = []

                # PRE_QUERIES STAGE

                self.log.info('Running Pre-query Stage')
                for func in pre_queries:
                    res = func(res_dict)
                    res_dict['pre_q_res'].append(res)

                # QUERIES STAGE

                self.log.info('Running Query Stage')
                for query in queries:
                    res = self.run_cbq_query(query, server=server, query_params=dict(query_params))
                    res_dict['q_res'].append(res)

                # POST_QUERIES STAGE

                self.log.info('Running Post-query Stage')
                for func in post_queries:
                    res = func(res_dict)
                    res_dict['post_q_res'].append(res)

                # ASSERT STAGE

                self.log.info('Running Assert Stage')
                for func in asserts:
                    res = func(res_dict)
                    self.log.info('Pass: ' + test_name)

                # CLEANUP STAGE

                self.log.info('Running Cleanup Stage')
                for func in cleanups:
                    res = func(res_dict)
                    res_dict['cleanup_res'].append(res)
            except Exception as e:
                self.log.info('Fail: ' + test_name)
                res_dict['errors'].append((test_name, e, traceback.format_exc(), res_dict))

            test_results[test_name] = res_dict

        # reset indexes

        self.log.info('Queries completed, restoring previous indexes')
        current_indexes = self.get_parsed_indexes()
        restore_index_set = self.make_hashable_index_set(restore_indexes)
        current_index_set = self.make_hashable_index_set(current_indexes)
        self.drop_undesired_indexes(restore_index_set, current_index_set, current_indexes)
        current_indexes = self.get_parsed_indexes()
        current_index_set = self.make_hashable_index_set(current_indexes)
        self.create_desired_indexes(restore_index_set, current_index_set, restore_indexes)

        # print errors

        errors = [error for key in list(test_results.keys()) for error in test_results[key]['errors']]
        has_errors = False
        if errors != []:
            has_errors = True
            error_string = '\n ************************ There are %s errors: ************************ \n \n' % (
                len(errors))
            for error in errors:
                error_string += '************************ Error in query: ' + str(
                    error[0]) + ' ************************ \n'
                error_string += str(error[2]) + '\n'
            error_string += '************************ End of Errors ************************ \n'
            self.log.error(error_string)

        # trigger failure

        self.assertEqual(has_errors, False)

    def wait_for_all_indexers_ready(self, indexer_port=9102, retries=60, delay=1):
        self.log.info("waiting for all indexers to be in active state...")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        i = 0
        for node in indexer_nodes:
            self.log.info("waiting for indexer on node: " + str(node.ip))
            node_rest = RestConnection(node)
            indexer_url = "http://" + str(node.ip) + ":" + str(indexer_port) + "/"
            node_ready = False

            while not node_ready:
                if i >= retries:
                    raise Exception(
                        'indexer was not found in Active state within ' + str(retries) + ' retries for node: ' + str(
                            node.ip))
                indexer_stats = node_rest.get_indexer_stats(baseUrl=indexer_url)
                self.log.info("iteration " + str(i) + " of " + str(retries))
                self.log.info("indexer state: " + str(indexer_stats['indexer_state']))
                if indexer_stats['indexer_state'] == "Active":
                    node_ready = True
                i += 1
                self.sleep(delay)
        self.log.info("indexers ready")

    def is_index_present(self, bucket_name, index_name, fields_set=None, using=None, status="online", server=None):
        query_response = self.run_cbq_query("SELECT * FROM system:indexes WHERE indexes.bucket_id is missing", server=server)
        if fields_set is None and using is None:
            if status is "any":
                desired_index = (index_name, bucket_name)
                current_indexes = [(i['indexes']['name'],
                                    i['indexes']['keyspace_id']) for i in query_response['results']]
            else:
                desired_index = (index_name, bucket_name, status)
                current_indexes = [(i['indexes']['name'],
                                    i['indexes']['keyspace_id'],
                                    i['indexes']['state']) for i in query_response['results']]
        else:
            if status is "any":
                desired_index = (index_name, bucket_name, frozenset([field for field in fields_set]), using)
                current_indexes = [(i['indexes']['name'],
                                    i['indexes']['keyspace_id'],
                                    frozenset([(
                                        key.replace('`', '').replace('(', '').replace(')', '').replace('meta.id',
                                                                                                       'meta().id'),
                                        j)
                                        for j, key in enumerate(i['indexes']['index_key'], 0)]),
                                    i['indexes']['using']) for i in query_response['results']]
            else:
                field_list = list(fields_set)
                i = 0
                for field in field_list:
                    temp_list = list(field)
                    if "INCLUDE MISSING" in temp_list[0]:
                        # NOTE: we don't need to replace INCLUDE MISSING with MISSING
                        # temp_list[0] = temp_list[0].replace("INCLUDE MISSING", "MISSING")
                        new_tuple = (temp_list[0], temp_list[1])
                        field_list[i] = new_tuple
                    i += 1
                desired_index = (index_name, bucket_name, frozenset([field for field in field_list]), status, using)
                current_indexes = [(i['indexes']['name'],
                                    i['indexes']['keyspace_id'],
                                    frozenset([(
                                        key.replace('`', '').replace('(', '').replace(')', '').replace('meta.id',
                                                                                                       'meta().id'),
                                        j)
                                        for j, key in enumerate(i['indexes']['index_key'], 0)]),
                                    i['indexes']['state'],
                                    i['indexes']['using']) for i in query_response['results']]

        if desired_index in current_indexes:
            return True
        else:
            self.log.info("waiting for: \n" + str(desired_index) + "\n")
            self.log.info("current indexes: \n" + str(current_indexes) + "\n")
            return False

    def wait_for_all_indexes_online(self, build_deferred=False):
        cur_indexes = self.get_parsed_indexes()
        for index in cur_indexes:
            self._wait_for_index_online(index['bucket'], index['name'], build_deferred=build_deferred)

    def wait_for_index_status_bulk(self, bucket_index_status_list):
        for item in bucket_index_status_list:
            bucket = item[0]
            index_name = item[1]
            index_status = item[2]
            self.wait_for_index_status(bucket, index_name, index_status)

    def drop_index(self, bucket, index):
        collection_name = self.get_collection_name(bucket)
        self.run_cbq_query("drop index %s ON `%s`" % (index, collection_name))
        self.wait_for_index_drop(bucket, index)

    def drop_primary_index(self, bucket, index):
        self.run_cbq_query("drop primary index on `%s`" % bucket)
        self.wait_for_index_drop(bucket, index)

    def drop_index_safe(self, bucket_name, index_name, is_primary=False):
        if self.is_index_present(bucket_name=bucket_name, index_name=index_name):
            if is_primary:
                self.drop_primary_index(bucket_name, index_name)
            else:
                self.drop_index(bucket_name, index_name)
            self.wait_for_index_drop(bucket_name, index_name)

    def drop_all_indexes(self, bucket=None, leave_primary=True):
        current_indexes = self.get_parsed_indexes()
        if bucket is not None:
            current_indexes = [index for index in current_indexes if index['bucket'] == bucket]
        if leave_primary:
            current_indexes = [index for index in current_indexes if index['is_primary'] is False]
        for index in current_indexes:
            bucket = index['bucket']
            index_name = index['name']
            self.run_cbq_query("drop index %s ON %s" % (index_name, bucket))
        for index in current_indexes:
            bucket = index['bucket']
            index_name = index['name']
            self.wait_for_index_drop(bucket, index_name)

    def wait_for_index_status(self, bucket_name, index_name, status):
        self.with_retry(lambda: self.is_index_present(bucket_name, index_name, status=status), eval=True, delay=1,
                        tries=30)

    def wait_for_index_present(self, bucket_name, index_name, fields_set, using):
        self.with_retry(lambda: self.is_index_present(bucket_name, index_name, fields_set, using), eval=True, delay=1,
                        tries=30)

    def wait_for_index_drop(self, bucket_name, index_name, fields_set=None, using=None):
        self.with_retry(
            lambda: self.is_index_present(bucket_name, index_name, fields_set=fields_set, using=using, status="any"),
            eval=False, delay=1, tries=30)

    def get_index_count(self, bucket, state):
        results = self.run_cbq_query(
            "SELECT  count(*) as num_indexes FROM system:indexes WHERE keyspace_id = '%s' and state = '%s'" % (
                bucket, state))
        num_indexes = results['results'][0]['num_indexes']
        return num_indexes

    def get_parsed_indexes(self):
        query_response = self.run_cbq_query("SELECT * FROM system:indexes WHERE indexes.bucket_id is missing")
        current_indexes = [{'name': i['indexes']['name'],
                            'bucket': i['indexes']['keyspace_id'],
                            'fields': frozenset([(key.replace('`', '').replace('(', '').replace(')', '').replace(
                                'meta.id', 'meta().id'), j)
                                for j, key in enumerate(i['indexes']['index_key'], 0)]),
                            'state': i['indexes']['state'],
                            'using': i['indexes']['using'],
                            'where': i['indexes'].get('condition', ''),
                            'is_primary': i['indexes'].get('is_primary', False)} for i in query_response['results']]
        return current_indexes

    def parse_desired_indexes(self, index_list):
        desired_indexes = [{'name': index['name'],
                            'bucket': index['bucket'],
                            'fields': frozenset([field for field in index['fields']]),
                            'state': index['state'],
                            'using': index['using'],
                            'where': index.get('where', ''),
                            'is_primary': index.get('is_primary', False)} for index in index_list]
        return desired_indexes

    def make_hashable_index_set(self, parsed_indexes):
        return frozenset([frozenset(list(index_dict.items())) for index_dict in parsed_indexes])

    def get_index_vars(self, index):
        name = index['name']
        keyspace = index['bucket']
        fields = index['fields']
        sorted_fields = [item[0] for item in tuple(sorted(fields, key=lambda item: item[1]))]
        joined_fields = ', '.join(sorted_fields)
        using = index['using']
        is_primary = index['is_primary']
        where = index['where']
        return name, keyspace, fields, joined_fields, using, is_primary, where

    def drop_undesired_indexes(self, desired_index_set, current_index_set, current_indexes):
        if desired_index_set != current_index_set:
            for current_index in current_indexes:
                if frozenset(list(current_index.items())) not in desired_index_set:
                    # drop index
                    name, keyspace, fields, joined_fields, using, is_primary, where = self.get_index_vars(current_index)
                    self.log.info("dropping index: %s %s %s" % (keyspace, name, using))
                    if is_primary:
                        self.run_cbq_query("DROP PRIMARY INDEX on %s USING %s" % (keyspace, using))
                    else:
                        self.run_cbq_query("DROP INDEX %s.%s USING %s" % (keyspace, name, using))
                    self.wait_for_index_drop(keyspace, name, fields, using)

    def create_desired_indexes(self, desired_index_set, current_index_set, desired_indexes):
        self.log.info(
            "-->Create desired indexes..desiredset={},currentset={},desiredindexes={}".format(desired_index_set,
                                                                                              current_index_set,
                                                                                              desired_indexes))
        if desired_index_set != current_index_set:
            for desired_index in desired_indexes:
                if frozenset(list(desired_index.items())) not in current_index_set:
                    name, keyspace, fields, joined_fields, using, is_primary, where = self.get_index_vars(desired_index)
                    self.log.info("creating index: %s %s %s" % (keyspace, name, using))
                    if is_primary:
                        init_time = time.time()
                        while True:
                            next_time = time.time()
                            try:
                                self.run_cbq_query("CREATE PRIMARY INDEX ON `%s` USING %s" % (keyspace, using))
                                break
                            except CBQError as ex:
                                if "#primary already exists" in str(ex):
                                    self.log.info("Primary index already exists")
                                    break
                                else:
                                    self.log.error(f"Fail to create index: {ex}")
                            if next_time - init_time > 600:
                                self.fail("Fail to create primary index before timeout")
                            time.sleep(2)
                    else:
                        if where != '':
                            self.run_cbq_query("CREATE INDEX %s ON %s(%s) WHERE %s  USING %s" % (
                                name, keyspace, joined_fields, where, using))
                        else:
                            self.run_cbq_query(
                                "CREATE INDEX %s ON %s(%s) USING %s" % (name, keyspace, joined_fields, using))
                    self.wait_for_index_present(keyspace, name, fields, using)

    ##############################################################################################
    #
    #   COMMON FUNCTIONS
    ##############################################################################################
    def _create_server_groups(self):
        if self.server_grouping:
            # ns_server refuses to change server groups while a rebalance runs.
            if self.rest._rebalance_progress_status() == 'running':
                self.rest.monitorRebalance(stop_if_loop=True)
            server_groups = self.server_grouping.split(":")
            self.log.info("server groups : %s", server_groups)

            zones = list(self.rest.get_zone_names().keys())

            # Delete Server groups
            for zone in zones:
                if zone != "Group 1":
                    nodes_in_zone = self.rest.get_nodes_in_zone(zone)
                    if nodes_in_zone:
                        self.rest.shuffle_nodes_in_zones(list(nodes_in_zone.keys()),
                                                         zone, "Group 1")
                    self.rest.delete_zone(zone)

            zones = list(self.rest.get_zone_names().keys())
            source_zone = zones[0]

            # Create Server groups
            for i in range(1, len(server_groups) + 1):
                server_grp_name = "ServerGroup_" + str(i)
                if not self.rest.is_zone_exist(server_grp_name):
                    self.rest.add_zone(server_grp_name)

            # Add nodes to Server groups
            i = 1
            for server_grp in server_groups:
                server_list = []
                server_grp_name = "ServerGroup_" + str(i)
                i += 1
                nodes_in_server_group = server_grp.split("-")
                for node in nodes_in_server_group:
                    self.rest.shuffle_nodes_in_zones(
                        [self.servers[int(node)].ip], source_zone,
                        server_grp_name)
                    server_list.append(self.servers[int(node)].ip + ":" + self.servers[int(node)].port)
                self.server_group_map[server_grp_name] = server_list

    def ExplainPlanHelper(self, res, debug=False):
        try:
            rv = res["results"][0]["plan"]
        except:
            rv = res["results"][0]
        if debug:
            print(rv)
        return rv

    def PreparePlanHelper(self, res):
        try:
            rv = res["results"][0]["plan"]
        except:
            rv = res["results"][0]["operator"]
        return rv

    # ------------------------------------------------------------------
    # Plan assertions, query-service restart and PolymorphicDoc dataset
    # ------------------------------------------------------------------

    @staticmethod
    def _walk_operators(plan):
        """Every operator dict in the plan, depth-first, in plan order."""
        if isinstance(plan, dict):
            if '#operator' in plan:
                yield plan
            for v in plan.values():
                yield from QueryTests._walk_operators(v)
        elif isinstance(plan, list):
            for item in plan:
                yield from QueryTests._walk_operators(item)

    @staticmethod
    def find_all_operators(plan, op_name):
        return [op for op in QueryTests._walk_operators(plan) if op.get('#operator') == op_name]

    @staticmethod
    def find_operators_using_index(plan, index_name):
        """Every operator on index_name, whatever its type (IndexScan3, PrimaryScan3, ...)."""
        return [op for op in QueryTests._walk_operators(plan) if op.get('index') == index_name]

    @staticmethod
    def find_scans(plan, include_composite=False):
        """Leaf scans in plan order; include_composite adds UnionScan, IntersectScan, ..."""
        wanted = SCAN_OPERATORS if include_composite else INDEX_SCAN_OPERATORS
        return [op for op in QueryTests._walk_operators(plan) if op.get('#operator') in wanted]

    @staticmethod
    def used_index_names(plan):
        return {op['index'] for op in QueryTests.find_scans(plan) if op.get('index')}

    @staticmethod
    def index_items_out(profile, index_name):
        """#itemsOut of every scan on index_name in profile['executionTimings']."""
        return [op.get('#stats', {}).get('#itemsOut')
                for op in QueryTests.find_operators_using_index(profile, index_name)]

    @staticmethod
    def index_spans(plan, index_name):
        """Spans of every scan on index_name, in plan order."""
        return [span for op in QueryTests.find_operators_using_index(plan, index_name)
                for span in op.get('spans') or []]

    @staticmethod
    def normalize_plan(plan):
        """Deep copy without VOLATILE_KEYS, so equal plans compare equal across nodes."""
        if isinstance(plan, dict):
            return {k: QueryTests.normalize_plan(v) for k, v in plan.items() if k not in VOLATILE_KEYS}
        if isinstance(plan, list):
            return [QueryTests.normalize_plan(item) for item in plan]
        return plan

    @staticmethod
    def format_plan(plan):
        """One line per operator, indented, for assertion messages."""
        lines = []

        def short(value):
            text = str(value)
            return text if len(text) <= _MAX_FIELD_LEN else text[:_MAX_FIELD_LEN - 3] + '...'

        def visit(node, depth):
            if isinstance(node, dict):
                if '#operator' in node:
                    parts = [node['#operator']] + ['{}={}'.format(f, short(node[f]))
                                                   for f in _FORMAT_FIELDS if f in node]
                    lines.append('  ' * depth + ' '.join(parts))
                    depth += 1
                for key, value in node.items():
                    if isinstance(value, (dict, list)) and key not in _FORMAT_FIELDS:
                        visit(value, depth)
            elif isinstance(node, list):
                for item in node:
                    visit(item, depth)

        visit(plan, 0)
        return '\n'.join(lines)

    @staticmethod
    def parse_polymorphic_value_type(value_type):
        """{type: weight} for 'PolymorphicDoc', 'PolymorphicDoc:t1,t2' or
        'PolymorphicDoc:t1=50,t2=50'. Raises ValueError on input DocLoader would silently
        treat as "every type" (empty filter, stray comma, unknown type)."""
        if value_type == POLYMORPHIC_VALUE_TYPE:
            return dict(POLYMORPHIC_DOC_TYPES)
        prefix = POLYMORPHIC_VALUE_TYPE + ':'
        if not value_type.startswith(prefix):
            raise ValueError("valueType must start with '{}': {!r}".format(prefix, value_type))
        suffix = value_type[len(prefix):]
        if ' ' in suffix:
            raise ValueError("valueType must not contain spaces (passed to a shell): {!r}".format(value_type))
        entries = suffix.split(',')
        if not suffix or any(not e for e in entries):
            raise ValueError("Empty type filter or stray comma in {!r}".format(value_type))
        weights = {}
        for entry in entries:
            name, sep, weight = entry.partition('=')
            if name not in POLYMORPHIC_DOC_TYPES:
                raise ValueError("Unknown type {!r} in {!r}; valid: {}".format(
                    name, value_type, sorted(POLYMORPHIC_DOC_TYPES)))
            if name in weights:
                raise ValueError("Type {!r} listed twice in {!r}".format(name, value_type))
            if sep and (not weight.isdigit() or int(weight) == 0):
                raise ValueError("Weight for {!r} must be a positive integer: {!r}".format(
                    name, value_type))
            weights[name] = int(weight) if sep else POLYMORPHIC_DOC_TYPES[name]
        return weights

    def get_n1ql_feat_ctrl(self):
        """Cluster-wide queryN1QLFeatCtrl (/settings/querySettings)."""
        status, settings = self.rest.get_query_settings(self.master)
        self.assertTrue(status, "Could not read /settings/querySettings: {}".format(settings))
        return int(settings['queryN1QLFeatCtrl'])

    def set_n1ql_feat_ctrl(self, value):
        """Set the cluster-wide queryN1QLFeatCtrl (every query node; survives a restart)."""
        params = urllib.parse.urlencode({'queryN1QLFeatCtrl': value})
        status, content, _ = self.rest._http_request(
            self.rest.baseUrl + 'settings/querySettings', 'POST', params)
        self.assertTrue(status, "Setting queryN1QLFeatCtrl={} failed: {}".format(value, content))
        self.log.info("queryN1QLFeatCtrl set to %s", value)

    def wait_feat_ctrl_bit(self, bit, enabled, timeout=60):
        """Wait until every query node reports n1ql-feat-ctrl with `bit` set (enabled) or clear."""
        deadline = time.time() + timeout
        while True:
            nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True) or []
            values = {n.ip: int(self.rest.get_query_admin_settings(n).get('n1ql-feat-ctrl', 0))
                      for n in nodes}
            if all(bool(v & bit) == enabled for v in values.values()):
                return
            if time.time() > deadline:
                self.fail("n1ql-feat-ctrl bit {:#x} is not {} on every query node "
                          "(per node: {})".format(bit, "set" if enabled else "clear", values))
            time.sleep(2)

    def set_feat_ctrl_bit(self, bit, enabled):
        """Set or clear one queryN1QLFeatCtrl bit, keeping the others, and wait for every
        query node to apply it."""
        current = self.get_n1ql_feat_ctrl()
        self.set_n1ql_feat_ctrl(current | bit if enabled else current & ~bit)
        self.wait_feat_ctrl_bit(bit, enabled)

    def disable_sequential_scan(self):
        """Turn sequential scans off on every query node; returns the previous queryN1QLFeatCtrl."""
        original = self.get_n1ql_feat_ctrl()
        self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, True)
        return original

    def seeded_rng(self, label, param='seed'):
        """random.Random from the `param` test parameter or a random seed (logged)."""
        value = self.input.param(param, None)
        if value is None and param != 'seed':
            value = self.input.param('seed', None)   # one seed= replays every test
        seed = int(value) if value is not None else random.randrange(1 << 30)
        self.log.info("%s: %s=%s (rerun with %s=%s for the same inputs)",
                      label, param, seed, param, seed)
        return random.Random(seed), seed

    @staticmethod
    def n1ql_literal(value):
        """A safe N1QL literal for a string or number."""
        return json.dumps(value)

    def get_query_nodes(self):
        return self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True) or []

    def get_index_nodes(self):
        return self.get_nodes_from_services_map(service_type="index", get_all_nodes=True) or []

    def assert_item_count(self, bucket, expected, scope='_default', collection='_default',
                           timeout=120):
        """The collection (not the bucket, which also counts _system) holds exactly
        `expected` items, from KV collection stats summed over the data nodes."""
        name = getattr(bucket, 'name', bucket)
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        stats = CollectionsStats(self.master)
        deadline = time.time() + timeout
        while True:
            count = stats.get_collection_item_count_cumulative(name, scope, collection,
                                                               node=kv_nodes)
            self.log.info("%s.%s.%s item count: %s (expected %s)", name, scope, collection,
                          count, expected)
            if count == expected or time.time() > deadline:
                break
            time.sleep(5)
        self.assertEqual(count, expected, "{}.{}.{} has {} items, expected {}".format(
            name, scope, collection, count, expected))

    def assert_index_plan(self, plan, index_name):
        """index_name is used, and there is no sequential or primary scan."""
        self.assert_index_used(plan, index_name)
        self.assert_no_sequential_scan(plan)
        self.assert_no_primary_scan(plan)

    def assert_mutations(self, result, expected, statement):
        count = result['metrics'].get('mutationCount')
        self.assertEqual(count, expected, "{} changed {} of {} documents".format(
            statement, count, expected))

    def assert_cbo_planned(self, plan, what):
        """CBO planned the query: its scans carry optimizer estimates (a rule-based plan has none)."""
        self.assertTrue(any('optimizer_estimates' in op for op in self.find_scans(plan)),
                        "{}: CBO did not plan the query (no optimizer_estimates):\n{}".format(
                            what, self.format_plan(plan)))

    def assert_cbo_estimates(self, profile, index_name, what, factor=10, slack=20):
        """Every scan on index_name in a profiled CBO run (profile=timings): the optimizer's
        row estimate (optimizer_estimates.cardinality) is within `factor` times the rows the
        scan returned (#stats.#itemsOut) either way, or within `slack` rows of it (small
        counts are noisy). Catches a bad selectivity estimate even when the index is right."""
        scans = self.find_operators_using_index(profile, index_name)
        self.assertTrue(scans, "{}: no scan on {} in the profile:\n{}".format(
            what, index_name, self.format_plan(profile)))
        for op in scans:
            estimate = (op.get('optimizer_estimates') or {}).get('cardinality')
            self.assertIsNotNone(estimate, "{}: scan on {} has no optimizer estimate:\n{}".format(
                what, index_name, self.format_plan(profile)))
            actual = op.get('#stats', {}).get('#itemsOut', 0)
            ratio = max(estimate, 1.0) / max(actual, 1)
            self.log.info("%s: %s estimated %.1f rows, returned %d", what, index_name, estimate, actual)
            self.assertTrue(abs(estimate - actual) <= slack or 1.0 / factor <= ratio <= factor,
                            "{}: CBO estimated {:.1f} rows from {}, the scan returned {} (more than "
                            "{}x off):\n{}".format(what, estimate, index_name, actual, factor,
                                                    self.format_plan(profile)))

    def wait_cluster_healthy(self, after, timeout=300):
        """All nodes_init nodes active and healthy, no rebalance running, the bucket
        present; fails naming what came before, instead of breaking the next test."""
        deadline = time.time() + timeout
        while True:
            # A new connection each time: it follows CbServer.use_https (strict encryption).
            rest = RestConnection(self.master)
            nodes = rest.get_nodes(get_all_nodes=True)
            states = {n.ip: (n.clusterMembership, n.status) for n in nodes}
            rebalance = rest._rebalance_progress_status()
            buckets = [b.name for b in rest.get_buckets()]
            if (len(nodes) == self.nodes_init and rebalance != 'running'
                    and all(state == ('active', 'healthy') for state in states.values())
                    and self.bucket_name in buckets):
                return
            if time.time() > deadline:
                self.fail("After {}: the cluster is not {} active, healthy nodes with bucket {} "
                          "(nodes {}, rebalance {}, buckets {})".format(
                              after, self.nodes_init, self.bucket_name, states, rebalance, buckets))
            time.sleep(5)

    def failover_and_recover(self, node, recovery_type='delta', graceful=True):
        """Fail node over (graceful by default) as a customer does for patching, then add
        it back with recovery_type and rebalance; waits for a healthy cluster. Uses the
        cluster failover/rebalance tasks. To keep queries running meanwhile, call it as the
        action of run_query_workload."""
        others = [s for s in self.servers[:self.nodes_init] if s.ip != node.ip]
        self.assertTrue(others, "No node left to fail {} over to".format(node.ip))
        rest = RestConnection(others[0])
        otp = [n.id for n in rest.node_statuses() if n.ip == node.ip]
        self.assertEqual(len(otp), 1, "Node {} not found in {}".format(
            node.ip, [n.ip for n in rest.node_statuses()]))
        def membership():
            return {n.ip: n.clusterMembership for n in rest.get_nodes(get_all_nodes=True)}

        self.cluster.failover(others, failover_nodes=[node], graceful=graceful)
        try:
            # The failover task only starts it: a graceful failover first moves the active
            # vBuckets off the node (it runs as a rebalance), then the node is failed over.
            self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                            "Failover of {} did not complete".format(node.ip))
            deadline = time.time() + 120
            while membership().get(node.ip) != 'inactiveFailed' and time.time() < deadline:
                time.sleep(2)
            states = membership()
            self.assertEqual(states.get(node.ip), 'inactiveFailed',
                             "{} is not failed over: {}".format(node.ip, states))
            self.log.info("%s failed over (graceful=%s)", node.ip, graceful)
        finally:
            # Also after a failed check: later tests need every node back in the cluster.
            if membership().get(node.ip) == 'inactiveFailed':
                rest.add_back_node(otp[0])
                rest.set_recovery_type(otp[0], recovery_type)
                try:
                    rebalanced = self.cluster.rebalance(others + [node], [], [])
                except InvalidArgumentException as ex:
                    # ns_server refuses delta recovery when it cannot reuse the node's
                    # data; a customer then adds the node back with full recovery.
                    if recovery_type == 'full' or 'deltaRecoveryNotPossible' not in str(ex):
                        raise
                    self.log.warning("%s: delta recovery not possible, adding it back with "
                                     "full recovery", node.ip)
                    recovery_type = 'full'
                    rest.set_recovery_type(otp[0], recovery_type)
                    rebalanced = self.cluster.rebalance(others + [node], [], [])
                self.assertTrue(rebalanced, "Rebalance after the {} recovery of {} failed".format(
                    recovery_type, node.ip))
                self.log.info("%s added back (%s recovery)", node.ip, recovery_type)
        self.wait_cluster_healthy("the {} recovery of {}".format(recovery_type, node.ip))

    def run_query_workload(self, action, run_query, what, nodes=None, allowed_codes=(),
                           recover_timeout=120, settle=5, seed=None):
        """Run action() (a restart, a kill, a failover, ...) while queries keep running in
        the background: one thread per node in `nodes` (default: every query node) calls
        run_query(server, n) in a loop, n = 0, 1, ...; run_query runs one query and checks
        its result (a wrong result fails the test). An error whose code is in
        allowed_codes is retried until that node's first success after action() returns;
        that success must come within recover_timeout, then the workload runs `settle`
        more seconds. Any other error, any error after the node recovered, a wrong result
        or no recovery fails the test. Returns {node_ip: {'ok': passed runs, 'allowed':
        Counter of allowed error codes, 'recovered': time of the first success after
        action() returned}}."""
        nodes = self.get_query_nodes() if nodes is None else nodes
        self.assertTrue(nodes, "{}: no query node to run the workload on".format(what))
        stop = threading.Event()
        action_end = []
        stats = {node.ip: {'ok': 0, 'allowed': collections.Counter(), 'recovered': None}
                 for node in nodes}

        def worker(server):
            mine = stats[server.ip]
            n = 0
            while not stop.is_set():
                try:
                    run_query(server, n)
                except CBQError as ex:
                    code = self.process_CBQE(ex).get('code')
                    if code not in allowed_codes or mine['recovered'] is not None:
                        raise
                    mine['allowed'][code] += 1
                    time.sleep(1)
                else:
                    mine['ok'] += 1
                    if action_end and mine['recovered'] is None:
                        mine['recovered'] = time.time()
                n += 1

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes))
        futures = {node.ip: pool.submit(worker, node) for node in nodes}
        try:
            action()
            action_end.append(time.time())
            while not any(f.done() for f in futures.values()):
                pending = [ip for ip, s in stats.items() if s['recovered'] is None]
                elapsed = time.time() - action_end[0]
                if (not pending and elapsed >= settle) or (pending and elapsed > recover_timeout):
                    break
                time.sleep(1)
        finally:
            stop.set()
            concurrent.futures.wait(list(futures.values()))
            pool.shutdown()
            for ip, future in futures.items():
                if future.exception():
                    self.log.error("%s: workload on %s failed: %s", what, ip, future.exception())
        for future in futures.values():
            future.result()
        self.log.info("%s: workload %s (seed=%s)", what, {
            ip: {'ok': s['ok'], 'allowed': dict(s['allowed']),
                 'recovered_after_s': None if s['recovered'] is None else
                 round(s['recovered'] - action_end[0], 1)} for ip, s in stats.items()}, seed)
        not_recovered = sorted(ip for ip, s in stats.items() if s['recovered'] is None)
        self.assertFalse(not_recovered, "{}: queries on {} did not succeed within {}s after it "
                         "(allowed errors {}) (seed={})".format(
                             what, not_recovered, recover_timeout,
                             {ip: dict(stats[ip]['allowed']) for ip in not_recovered}, seed))
        return stats

    def reassert_queries(self, checks, what, seed=None, nodes=None):
        """After a disturbance, on every node in `nodes` (default: every query node): the
        query service answers, and each check's query uses its index (no primary or
        sequential scan), has the same plan on every node and returns exactly its rows.
        checks: [(sql, index_name, expected_rows, query_params or None)]."""
        nodes = self.get_query_nodes() if nodes is None else nodes
        self.assertTrue(nodes, "{}: no query node to check".format(what))
        for node in nodes:
            self._wait_for_query_service_ready(node, timeout=120)
        for sql, index_name, expected, params in checks:
            for node in nodes:
                on_node = "{}: {} on {}".format(what, sql, node.ip)
                self.assert_index_plan(self.explain(sql, server=node, query_params=params),
                                       index_name)
                rows = self.run_cbq_query(sql, server=node,
                                          query_params=dict(params or {}))['results']
                self.assert_same_rows(rows, expected, on_node, seed)
            if len(nodes) > 1:
                self.assert_same_plan_across_nodes(sql, nodes=nodes)
        self.log.info("%s: %d queries re-checked (plan, rows) on %s", what, len(checks),
                      [n.ip for n in nodes])

    def restart_couchbase_server(self, node):
        """Restart couchbase-server on node; wait for ns_server, bucket warmup and the
        query service before the next node goes down."""
        shell = RemoteMachineShellConnection(node)
        try:
            shell.restart_couchbase()
        finally:
            shell.disconnect()
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self)
        for bucket in self.buckets:
            self.assertTrue(ClusterOperationHelper._wait_warmup_completed(
                self, [node], bucket.name), "Bucket {} on {} did not warm up after the "
                "restart".format(bucket.name, node.ip))
        self._wait_for_query_service_ready(node, timeout=300)
        self.wait_cluster_healthy("restarting couchbase-server on {}".format(node.ip))
        self.log.info("couchbase-server on %s restarted", node.ip)

    def cbq_pids(self):
        return {n.ip: self._cbq_engine_pid(n) for n in self.get_query_nodes()}

    def assert_cbq_not_restarted(self, pids_before, what):
        pids_after = self.cbq_pids()
        self.assertEqual(pids_after, pids_before,
                         "cbq-engine restarted (crashed) during {}: pids before {} after {}"
                         .format(what, pids_before, pids_after))

    def run_no_panic(self, query, step, **kwargs):
        """run_cbq_query; error 5001 fails the test naming the step, other errors are raised.
        The code is matched in the raw response: a request that panics mid-stream can return
        a truncated (non-JSON) body, which process_CBQE cannot parse."""
        try:
            return self.run_cbq_query(query, **kwargs)
        except CBQError as ex:
            text = str(ex)
            if PANIC_CODE_RE.search(text):
                errors = text[text.find('errors'):][:500]
                self.fail("{} panicked (error {}): {}".format(step, PANIC_ERROR_CODE, errors))
            raise

    def assert_union_scan(self, plan, index_names):
        """A UnionScan over exactly index_names, and no primary or sequential scan."""
        unions = self.find_all_operators(plan, 'UnionScan')
        self.assertTrue(unions, "No UnionScan in the plan:\n{}".format(self.format_plan(plan)))
        scanned = {op.get('index') for union in unions for op in self.find_scans(union)}
        self.assertEqual(scanned, set(index_names),
                         "UnionScan scans {} instead of {}. Plan:\n{}".format(
                             sorted(scanned), sorted(index_names), self.format_plan(plan)))
        self.assert_no_primary_scan(plan)
        self.assert_no_sequential_scan(plan)

    def require_two_nodes(self):
        """2+ query nodes (cross-node checks) and 2+ index nodes (index replica)."""
        query_nodes, index_nodes = self.get_query_nodes(), self.get_index_nodes()
        self.assertTrue(len(query_nodes) >= 2 and len(index_nodes) >= 2,
            "Needs 2+ query and 2+ index nodes (run with nodes_init=2 and "
            "services_init=kv:index:n1ql-kv:index:n1ql); found {} query, {} index".format(
                [n.ip for n in query_nodes], [n.ip for n in index_nodes]))

    @staticmethod
    def _canonical(value):
        """value with integral floats as ints (12.0 -> 12), as the query service returns them."""
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, dict):
            return {k: QueryTests._canonical(v) for k, v in value.items()}
        if isinstance(value, list):
            return [QueryTests._canonical(v) for v in value]
        return value

    def assert_same_rows(self, got, expected, what, seed=None, ordered=False):
        """got and expected hold the same rows (duplicates counted; in the same order
        when ordered): the count first, for a clear message, then the rows as JSON."""
        def dump(rows):
            dumped = [json.dumps(self._canonical(row), sort_keys=True) for row in rows]
            return dumped if ordered else sorted(dumped)
        got, expected = dump(got), dump(expected)
        suffix = " (seed={})".format(seed) if seed is not None else ""
        self.assertEqual(len(got), len(expected), "{}: {} rows, expected {}{}".format(
            what, len(got), len(expected), suffix))
        self.assertEqual(got, expected, "{}: wrong rows{}".format(what, suffix))

    def query_vitals(self, node):
        """admin/vitals of the query service on node, as a dict."""
        status, content = self.rest.get_query_vitals(node)
        self.assertTrue(status, "GET admin/vitals on {} failed: {}".format(node.ip, content))
        return json.loads(content)

    def query_rss_mb(self, node):
        """cbq-engine resident memory on node, in MB."""
        vitals = self.query_vitals(node)
        self.assertIn(VITALS_RSS, vitals, "admin/vitals on {} has no {}: {}".format(
            node.ip, VITALS_RSS, sorted(vitals)))
        return vitals[VITALS_RSS] / MIB

    def set_query_admin_setting(self, node, key, value):
        """POST {key: value} to the query service's admin/settings on one node
        (RestConnection.set_query_servicers posts {servicers: setting} there)."""
        response, content = self.rest.set_query_servicers(node, value, servicers=key)
        self.assertEqual(response.status, 200, "POST admin/settings {}={} on {} failed: {}"
                         .format(key, value, node.ip, content))

    def set_query_setting(self, key, value):
        """POST {key: value} to admin/settings on every query node; returns {ip: previous}
        for restore_query_setting."""
        before = {}
        for node in self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True):
            before[node.ip] = self.rest.get_query_admin_settings(node).get(key)
            self.set_query_admin_setting(node, key, value)
        return before

    def restore_query_setting(self, key, before):
        for node in self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True):
            if before.get(node.ip) is not None:
                self.set_query_admin_setting(node, key, before[node.ip])

    def query_log_line_count(self, node):
        """Number of lines in node's query.log, a mark for query_log_grep."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command("wc -l < {}/query.log".format(testconstants.LINUX_COUCHBASE_LOGS_PATH))
        finally:
            shell.disconnect()
        return int(out[0].strip())

    def query_log_grep(self, node, since, pattern):
        """query.log lines after line `since` that match the extended regex `pattern`."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command("tail -n +{} {}/query.log | grep -E '{}' | head -5"
                                           .format(since + 1, testconstants.LINUX_COUCHBASE_LOGS_PATH, pattern))
        finally:
            shell.disconnect()
        return [line for line in out if line.strip()]

    def assert_used_memory(self, result, quota_mb, what):
        """With a request memory_quota the metrics report usedMemory: above 0, below it."""
        used = result['metrics'].get('usedMemory')
        self.assertTrue(isinstance(used, (int, float)) and not isinstance(used, bool)
                        and 0 < used < quota_mb * MIB,
                        "{}: usedMemory {} is not between 0 and the {} MB quota".format(
                            what, used, quota_mb))

    def explain(self, query, server=None, use_cbo=None, query_params=None):
        """EXPLAIN plan (with CTE/subquery plans). Copies query_params: run_cbq_query
        modifies the dict it gets."""
        params = dict(query_params or {})
        if use_cbo is not None:
            params['use_cbo'] = use_cbo
        result = self.run_cbq_query("EXPLAIN {}".format(query), server=server,
                                    query_params=params)
        return self.plan_with_subqueries(result)

    def plan_with_subqueries(self, explain_result):
        """The EXPLAIN plan plus its '~subqueries' (CTE and subquery plans, which the main
        plan only shows as an ExpressionScan), so plan checks see their scans too."""
        plan = self.ExplainPlanHelper(explain_result)
        subqueries = explain_result["results"][0].get("~subqueries")
        if subqueries and isinstance(plan, dict):
            plan = dict(plan, **{"~subqueries": subqueries})
        return plan

    def assert_index_used(self, plan, index_name):
        self.assertIn(index_name, self.used_index_names(plan),
            "Expected index '{}' not used. Plan:\n{}".format(index_name, self.format_plan(plan)))

    def assert_no_primary_scan(self, plan):
        """No scan on '#primary' and no PrimaryScan (a sequential scan, also a
        PrimaryScan3, is left to assert_no_sequential_scan)."""
        primary = [op for op in self.find_scans(plan)
                   if op.get('index') == PRIMARY_INDEX
                   or (op['#operator'] in ('PrimaryScan3', 'PrimaryScan')
                       and op.get('index') != SEQUENTIAL_SCAN)]
        self.assertEqual(primary, [],
            "Unexpected primary scan. Plan:\n{}".format(self.format_plan(plan)))

    def assert_no_sequential_scan(self, plan):
        # Matched on the index name: a sequential scan can be an IndexScan3 or a PrimaryScan3.
        seqscans = self.find_operators_using_index(plan, SEQUENTIAL_SCAN)
        self.assertEqual(seqscans, [],
            "Unexpected sequential scan. Plan:\n{}".format(self.format_plan(plan)))

    def assert_same_plan_across_nodes(self, query, nodes=None, use_cbo=None):
        """Same normalized EXPLAIN plan on every query node (needs 2+ nodes)."""
        nodes = nodes if nodes is not None else (
            self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True) or [])
        self.assertGreaterEqual(len(nodes), 2,
            "assert_same_plan_across_nodes needs 2+ query nodes, found {}".format(
                [n.ip for n in nodes]))
        plans = [(node.ip, self.explain(query, server=node, use_cbo=use_cbo)) for node in nodes]
        base_ip, base_plan = plans[0]
        for ip, plan in plans[1:]:
            self.assertEqual(self.normalize_plan(base_plan), self.normalize_plan(plan),
                "Plan differs between query nodes {} and {}.\n{}:\n{}\n{}:\n{}".format(
                    base_ip, ip, base_ip, self.format_plan(base_plan), ip, self.format_plan(plan)))
        self.log.info("Same plan on %d query nodes: %s", len(plans), [ip for ip, _ in plans])

    def assert_cbo_result_parity(self, sql, expected, what, seed=None, nodes=None,
                                 query_params=None, profile_cbo=False):
        """On every node in `nodes` (default: every query node), the query with use_cbo
        true and with use_cbo false each returns exactly `expected` (rows computed from
        the data), so CBO and rule-based planning give the same rows. profile_cbo adds
        profile=timings to the CBO run. Returns {node_ip: result of the CBO run}."""
        nodes = self.get_query_nodes() if nodes is None else nodes
        cbo_results = {}
        for node in nodes:
            for use_cbo in (True, False):
                params = dict(query_params or {}, use_cbo=use_cbo)
                if use_cbo and profile_cbo:
                    params['profile'] = 'timings'
                result = self.run_cbq_query(sql, server=node, query_params=params)
                self.assert_same_rows(result['results'], expected, "{} on {} (use_cbo={})".format(
                    what, node.ip, use_cbo), seed)
                if use_cbo:
                    cbo_results[node.ip] = result
        return cbo_results

    def assert_no_cbo_index_regression(self, query):
        """If the non-CBO plan uses a secondary index, the CBO plan must not fall back
        to a primary or sequential scan."""
        fallback = {PRIMARY_INDEX, SEQUENTIAL_SCAN}
        plan_off = self.explain(query, use_cbo=False)
        plan_on = self.explain(query, use_cbo=True)
        off_idx, on_idx = self.used_index_names(plan_off), self.used_index_names(plan_on)
        self.log.info("CBO off uses %s; CBO on uses %s", sorted(off_idx), sorted(on_idx))
        if off_idx - fallback:
            self.assertFalse((on_idx & fallback) or not (on_idx - fallback),
                "CBO regressed to a primary/sequential scan (non-CBO used {}).\n"
                "CBO off:\n{}\nCBO on:\n{}".format(
                    sorted(off_idx - fallback), self.format_plan(plan_off), self.format_plan(plan_on)))

    def _wait_for_query_service_ready(self, node, timeout=60):
        """Wait for GET /admin/ping = 200 on node, then run SELECT 1 there. Only "port
        refused" (ServerUnavailableException) and a non-200 answer are waited out."""
        rest = RestConnection(node)
        api = rest.query_baseUrl + 'admin/ping'
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                ok, content, _ = rest._http_request(
                    api, 'GET', timeout=10, connect_timeout=max(1, deadline - time.time()))
            except ServerUnavailableException as ex:
                last = ex
                time.sleep(2)
                continue
            if ok:
                self.run_cbq_query('SELECT 1', server=node)
                return
            last = content
            time.sleep(2)
        self.fail("Query service on {} not ready within {}s (last: {})".format(
            node.ip, timeout, last))

    def _cbq_engine_pid(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command("pgrep -x cbq-engine")
        finally:
            shell.disconnect()
        return out[0].strip() if out else None

    def restart_query_service(self, node=None, timeout=120):
        """Kill cbq-engine on node (only the query service) and wait for a new one."""
        target = node or self.master
        before = self._cbq_engine_pid(target)
        if not before:
            self.fail("cbq-engine is not running on {}".format(target.ip))
        # kill -9 that pid: remote_util.terminate_process greps for the name and can kill its
        # own shell first once pids wrap (pid_max 32768), leaving cbq-engine running.
        shell = RemoteMachineShellConnection(target)
        try:
            shell.execute_command("kill -9 {}".format(before))
        finally:
            shell.disconnect()
        deadline = time.time() + timeout
        while time.time() < deadline:
            after = self._cbq_engine_pid(target)
            if after and after != before:
                self.log.info("cbq-engine on %s restarted: pid %s -> %s", target.ip, before, after)
                self._wait_for_query_service_ready(target, timeout=max(10, int(deadline - time.time())))
                return
            time.sleep(2)
        shell = RemoteMachineShellConnection(target)
        try:
            procs, _ = shell.execute_command("pgrep -a cbq-engine")
            babysitter, _ = shell.execute_command(
                "grep -i -E 'cbq|n1ql' {}/babysitter.log | tail -20".format(
                    testconstants.LINUX_COUCHBASE_LOGS_PATH))
        finally:
            shell.disconnect()
        self.fail("cbq-engine on {} did not restart within {}s (pid before {}); running now: "
                  "{}; babysitter.log: {}".format(target.ip, timeout, before, procs,
                                                  "\n".join(babysitter)))

    def _create_index(self, ddl, index_name, **options):
        """With one replica (a copy on each index node) plus extra WITH options."""
        ddl += ' WITH ' + json.dumps(dict({"num_replica": 1}, **options))
        self.run_cbq_query(ddl)
        self._wait_for_index_online(self.buckets[0], index_name)

    # --- index validation: item count, scan, explain; again after a query restart ---

    def _index_spec(self, name, scan_where, docs, entries=None, keyspace=None, replicas=1,
                    stats_keyspace=None):
        """What an index must hold, computed from the data: `docs` documents (what a scan
        through it counts) and `entries` index items (= docs unless it has an array key).
        scan_where selects every indexed document (leading key and partial condition)."""
        bucket = self.buckets[0].name
        return {'name': name, 'scan_where': scan_where, 'docs': docs,
                'entries': docs if entries is None else entries,
                'keyspace': keyspace or self.query_bucket, 'replicas': replicas,
                'stats_keyspaces': [stats_keyspace] if stats_keyspace else
                [bucket, 'default:{}._default._default'.format(bucket)]}

    def _collection_index_spec(self, scope, collection, keyspace, name, scan_where, docs,
                               entries=None, replicas=0):
        """_index_spec for an index (no replica by default) on scope.collection of the
        first bucket."""
        return self._index_spec(name, scan_where, docs, entries, keyspace=keyspace,
                                replicas=replicas,
                                stats_keyspace='default:{}.{}.{}'.format(
                                    self.buckets[0].name, scope, collection))

    def _index_item_counts(self, spec):
        """items_count of every instance (replica) of the index, summed over the index nodes
        (a partitioned instance is spread over several nodes)."""
        counts = {}
        for stats in (self.get_index_stats(perNode=True) or {}).values():
            stats = stats or {}
            for keyspace in spec['stats_keyspaces']:
                for name, index_stats in stats.get(keyspace, {}).items():
                    if re.sub(r' \(replica \d+\)$', '', name) == spec['name']:
                        counts[name] = counts.get(name, 0) + index_stats.get('items_count', 0)
        return counts

    def _check_indexes(self, specs, when):
        for spec in specs:
            what = "{} ({})".format(spec['name'], when)
            # 1. Every instance holds exactly the expected items (stats can lag the build).
            deadline = time.time() + 120
            while True:
                counts = self._index_item_counts(spec)
                ok = (len(counts) == spec['replicas'] + 1
                      and all(c == spec['entries'] for c in counts.values()))
                if ok or time.time() > deadline:
                    break
                time.sleep(3)
            self.assertTrue(ok, "{}: items_count per instance {}, expected {} on each of {} "
                            "instances".format(what, counts, spec['entries'], spec['replicas'] + 1))
            # 2. A scan through the index counts exactly the indexed documents.
            scan = "SELECT COUNT(1) AS n FROM {} USE INDEX ({}) WHERE {}".format(
                spec['keyspace'], spec['name'], spec['scan_where'])
            rows = self.run_cbq_query(scan)['results']
            self.assertEqual(rows, [{'n': spec['docs']}], "{}: scan counted {}, expected {} "
                             "documents ({} items)".format(what, rows, spec['docs'], spec['entries']))
            # 3. The scan is served by that index.
            self.assert_index_plan(self.explain(scan), spec['name'])

    def _validate_indexes(self, specs, restart=True):
        """Item count, scan count and plan of each index; then restart the query service
        on every query node and check them all again."""
        self._check_indexes(specs, "after create")
        if restart:
            for node in self.get_query_nodes():
                self.restart_query_service(node)
            self._check_indexes(specs, "after query-service restart")

    def load_polymorphic_docs(self, bucket, num_docs, value_type=POLYMORPHIC_VALUE_TYPE,
                              key_prefix='doc_', scope='_default', collection='_default'):
        """Load num_docs DocLoader PolymorphicDoc documents into one keyspace, through the
        same SDKDataLoader + magma DocLoader path BaseSecondaryIndexingTests uses for
        Hotel/Person. value_type is checked first (see parse_polymorphic_value_type)."""
        self.parse_polymorphic_value_type(value_type)
        if not self.use_magma_loader:
            self.fail("PolymorphicDoc is only available in the magma DocLoader; run with "
                      "use_magma_loader=True")
        bucket_name = getattr(bucket, 'name', bucket)
        self.log.info("Loading %s: bucket=%s scope=%s collection=%s keyPrefix=%s range=0..%d",
                      value_type, bucket_name, scope, collection, key_prefix, num_docs)
        gen = SDKDataLoader(num_ops=num_docs, percent_create=100, percent_update=0,
                            percent_delete=0, scope=scope, collection=collection,
                            json_template=value_type, output=True, key_prefix=key_prefix,
                            username=self.master.rest_username,
                            password=self.master.rest_password)
        self.cluster.async_load_gen_docs(self.master, bucket=bucket_name, generator=gen,
                                         use_magma_loader=True).result()

    def verify_polymorphic_distribution(self, bucket, num_docs, value_type=POLYMORPHIC_VALUE_TYPE,
                                        scope='_default', collection='_default',
                                        share_tolerance=0.03):
        """All num_docs documents have a requested _type and each type's share is near
        its weight; returns {type: count}. Uses an index on _type, so documents without
        _type are caught by the item count, not here."""
        weights = self.parse_polymorphic_value_type(value_type)
        keyspace = "`{}`.`{}`.`{}`".format(getattr(bucket, 'name', bucket), scope, collection)
        rows = self.run_cbq_query(
            "SELECT d._type AS t, COUNT(*) AS n FROM {} d WHERE d._type IS NOT MISSING "
            "GROUP BY d._type".format(keyspace))['results']
        counts = {row.get('t'): row['n'] for row in rows}
        total = sum(counts.values())
        self.log.info("PolymorphicDoc: %d docs with _type, counts per _type: %s", total, counts)
        self.assertEqual(total, num_docs,
            "Expected {} docs with a _type, found {}: {}. Stale DocLoader jar or failed "
            "load?".format(num_docs, total, counts))
        unexpected = set(counts) - set(weights)
        self.assertFalse(unexpected,
            "Unexpected _type values {} (requested {}). Is the jar built from DocLoader "
            "main with PolymorphicDoc?".format(sorted(unexpected, key=str), sorted(weights)))
        total_weight = float(sum(weights.values()))
        for doc_type, weight in weights.items():
            expected = weight / total_weight
            actual = counts.get(doc_type, 0) / float(num_docs)
            self.assertTrue(abs(actual - expected) <= share_tolerance,
                "_type {} is {:.1%} of docs, expected {:.1%} +/- {:.0%}: {}".format(
                    doc_type, actual, expected, share_tolerance, counts))
        return counts

    def gen_docs(self, docs_per_day=1, type='default', values_type=None, name='tuq', start=0, end=0):
        json_generator = JsonGenerator()
        generators = []
        self.log.info('Generating %s:%s data...' % (type, self.dataset))
        if type == 'default':
            if self.array_indexing:
                generators = json_generator.generate_docs_employee_array(docs_per_day, start)
            elif self.dataset == 'default':
                # not working
                self.log.info("-->start:generate_docs_employee for default...")
                generators = json_generator.generate_docs_employee(docs_per_day, start)
                self.log.info("-->end:generate_docs_employee for default...")
            elif self.dataset == 'sabre':
                # works
                generators = json_generator.generate_docs_sabre(docs_per_day, start)
            elif self.dataset == 'employee':
                # not working
                generators = json_generator.generate_docs_employee_data(docs_per_day=docs_per_day, start=start)
            elif self.dataset == 'simple':
                # not working
                generators = json_generator.generate_docs_employee_simple_data(docs_per_day=docs_per_day, start=start)
            elif self.dataset == 'sales':
                # not working
                generators = json_generator.generate_docs_employee_sales_data(docs_per_day=docs_per_day, start=start)
            elif self.dataset == 'bigdata':
                # not working
                generators = json_generator.generate_docs_bigdata(end=(1000 * docs_per_day), start=start,
                                                                  value_size=self.value_size)
            elif self.dataset == 'array':
                generators = json_generator.generate_all_type_documents_for_gsi(docs_per_day=docs_per_day, start=start)
            elif self.dataset == 'aggr':
                generators = json_generator.generate_doc_for_aggregate_pushdown(docs_per_day=docs_per_day, start=start)
            elif self.dataset == 'join':
                types = ['Engineer', 'Sales', 'Support']
                join_yr = [2010, 2011]
                join_mo = range(1, 12 + 1)
                join_day = range(1, 28 + 1)
                template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
                template += ' "job_title":"{4}", "tasks_ids":{5}}}'
                for info in types:
                    for year in join_yr:
                        for month in join_mo:
                            for day in join_day:
                                name = ["employee-%s" % (str(day))]
                                tasks_ids = ["test_task-%s" % day, "test_task-%s" % (day + 1)]
                                generators.append(DocumentGenerator("query-test-%s-%s-%s-%s" % (info, year, month, day),
                                                                    template, name, [year], [month], [day], [info],
                                                                    [tasks_ids],
                                                                    start=start, end=docs_per_day))
            else:
                self.fail("There is no dataset %s, please enter a valid one" % self.dataset)
        elif type == 'base64':
            if end == 0:
                end = self.num_items
            values = ["Engineer", "Sales", "Support"]
            generators = [JSONNonDocGenerator(name, values, start=start, end=end)]

        elif type == 'tasks':
            start, end = 0, (28 + 1)
            template = '{{ "task_name":"{0}", "project": "{1}"}}'
            generators.append(DocumentGenerator("test_task", template, ["test_task-%s" % i for i in range(0, 10)],
                                                ["CB"], start=start, end=10))
            generators.append(DocumentGenerator("test_task", template, ["test_task-%s" % i for i in range(10, 20)],
                                                ["MB"], start=10, end=20))
            generators.append(DocumentGenerator("test_task", template, ["test_task-%s" % i for i in range(20, end)],
                                                ["IT"], start=20, end=end))

        elif type == 'json_non_docs':
            if end == 0:
                end = self.num_items
            if values_type == 'string':
                values = ['Engineer', 'Sales', 'Support']
            elif values_type == 'int':
                values = [100, 200, 300, 400, 500]
            elif values_type == 'array':
                values = [[10, 20], [20, 30], [30, 40]]
            else:
                return []
            generators = [JSONNonDocGenerator(name, values, start=start, end=end)]

        elif type == 'nulls':
            if not end:
                end = self.num_items
            generators = []
            index = end // 3
            template = '{{ "feature_name":"{0}", "coverage_tests" : {{"P0":{1}, "P1":{2}, "P2":{3}}},'
            template += '"story_point" : {4},"jira_tickets": {5}}}'
            names = [str(i) for i in range(0, index)]
            rates = range(0, index)
            points = [[1, 2, 3], ]
            jira_tickets = ['[{"Number": 1, "project": "cb", "description": "test"},' + \
                            '{"Number": 2, "project": "mb", "description": "test"}]', ]
            generators.append(DocumentGenerator(name, template, names, rates, rates, rates, points, jira_tickets,
                                                start=start, end=index))
            template = '{{ "feature_name":"{0}", "coverage_tests" : {{"P0": null, "P1":null, "P2":null}},'
            template += '"story_point" : [1,2,null],"jira_tickets": {1}}}'
            jira_tickets = ['[{"Number": 1, "project": "cb", "description": "test"},' + \
                            '{"Number": 2, "project": "mb", "description": null}]', ]
            names = [str(i) for i in range(index, index + index)]
            generators.append(DocumentGenerator(name, template, names, jira_tickets, start=index, end=index + index))
            template = '{{ "feature_name":"{0}", "coverage_tests" : {{"P4": 2}},'
            template += '"story_point" : [null,null],"jira_tickets": {1}}}'
            names = [str(i) for i in range(index + index, end)]
            jira_tickets = ['[{"Number": 1, "project": "cb", "description": "test"},' + \
                            '{"Number": 2, "project": "mb"}]', ]
            generators.append(DocumentGenerator(name, template, names, jira_tickets, start=index + index, end=end))
        self.log.info('Completed Generating %s:%s data...' % (type, self.dataset))
        return generators

    def buckets_docs_ready(self, bucket_docs_map):
        ready = True
        rest_conn = RestConnection(self.master)
        bucket_docs_rest = rest_conn.get_buckets_itemCount()
        for bucket in list(bucket_docs_map.keys()):
            query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + bucket + "`")
            docs = query_response['results'][0]['$1']
            if docs != bucket_docs_map[bucket] and bucket_docs_rest[bucket] != bucket_docs_map[bucket]:
                self.log.info("Bucket Docs Not Ready For Bucket: " + str(bucket) + "... \n Expected: " + str(
                    bucket_docs_map[bucket]) + "\n Query: " + str(docs) + "\n Rest: " + str(bucket_docs_rest[bucket]))
                ready = False
        return ready

    def buckets_status_ready(self, bucket_status_map):
        ready = True
        rest_conn = RestConnection(self.master)
        for bucket in list(bucket_status_map.keys()):
            status = rest_conn.get_bucket_status(bucket)
            if status != bucket_status_map[bucket]:
                self.log.info(
                    "still waiting for bucket: " + bucket + " with status: " + str(status) + " to have " + str(
                        bucket_status_map[bucket]) + " status")
                ready = False
        return ready

    def bucket_deleted(self, bucket_name):
        query_response = self.run_cbq_query("select COUNT(*) from system:keyspaces where name == " + bucket_name)
        count = query_response['results'][0]['$1']
        if count != 0:
            self.log.info("Buckets still exists: " + bucket_name)
            return False
        if count == 0:
            return True

    def wait_for_buckets_status(self, bucket_status_map, delay, retries):
        self.with_retry(lambda: self.buckets_status_ready(bucket_status_map), eval=True, delay=delay, tries=retries)

    def wait_for_bucket_docs(self, bucket_doc_map, delay, retries):
        self.with_retry(lambda: self.buckets_docs_ready(bucket_doc_map), eval=True, delay=delay, tries=retries)

    def wait_for_bucket_delete(self, bucket_name, delay, retries):
        self.with_retry(lambda: self.bucket_deleted(bucket_name), delay=delay, tries=retries)

    def with_retry(self, func, eval=None, delay=5, tries=10, func_params=None):
        attempts = 0
        res = None
        while attempts < tries:
            attempts = attempts + 1
            try:
                res = func()
                if eval is None:
                    return res
                elif res == eval:
                    return res
                else:
                    self.sleep(delay, 'incorrect results, sleeping for %s' % delay)
            except Exception as ex:
                self.sleep(delay, 'exception returned: %s \n sleeping for %s' % (ex, delay))
        raise Exception('timeout, invalid results: %s' % res)

    def negative_common_body(self, queries_errors={}):
        if not queries_errors:
            self.fail("No queries to run!")
        check_code = False
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            for query_template, error_arg in queries_errors.items():
                if isinstance(error_arg, str):
                    error = error_arg
                else:
                    error, code = error_arg
                    check_code = True
                try:
                    query = self.gen_results.generate_query(query_template)
                    query_bucket = self.get_collection_name(bucket.name)
                    self.run_cbq_query(query.format(query_bucket))
                except CBQError as ex:
                    self.log.error(ex)
                    self.log.error(error)
                    self.assertTrue(str(ex).find(error) != -1,
                                    "Error is incorrect.Actual %s.\n Expected: %s.\n" % (
                                        str(ex).split(':')[-1], error))
                    if check_code:
                        self.assertTrue(str(ex).find(str(code)) != -1,
                                        "Error code is incorrect.Actual %s.\n Expected: %s.\n" % (str(ex), code))
                else:
                    self.fail("There were no errors. Error expected: %s" % error)

    def get_bucket_from_name(self, bucket_name):
        for bucket in self.buckets:
            if bucket.name == bucket_name:
                return bucket
        return None

    def delete_bucket(self, bucket):
        self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)

    def ensure_bucket_does_not_exist(self, bucket_name, using_rest=False):
        bucket = self.get_bucket_from_name(bucket_name)
        if bucket:
            if using_rest:
                rest = RestConnection(self.master)
                rest.delete_bucket(bucket_name)
            else:
                self.delete_bucket(bucket)
        self.wait_for_bucket_delete(bucket_name, 5, 10)

    def prepared_common_body(self, server=None):
        self.isprepared = True
        result_no_prepare = self.run_cbq_query(server=server)['results']
        if self.named_prepare:
            if 'concurrent' not in self.named_prepare:
                self.named_prepare = self.named_prepare + "_" + str(uuid.uuid4())[:4]
            query = "PREPARE %s from %s" % (self.named_prepare, self.query)
        else:
            query = "PREPARE %s" % self.query
        prepared = self.run_cbq_query(query=query, server=server)['results'][0]
        if self.encoded_prepare and len(self.servers) > 1:
            encoded_plan = prepared['encoded_plan']
            result_with_prepare = \
                self.run_cbq_query(query=prepared, is_prepared=True, encoded_plan=encoded_plan, server=server)[
                    'results']
        else:
            result_with_prepare = self.run_cbq_query(query=prepared, is_prepared=True, server=server)['results']
        if self.cover:
            self.assertTrue("IndexScan in %s" % result_with_prepare)
            self.assertTrue("covers in %s" % result_with_prepare)
            self.assertTrue("filter_covers in %s" % result_with_prepare)
            self.assertFalse('ERROR' in (str(word).upper() for word in result_with_prepare))
        msg = "Query result with prepare and without doesn't match.\nNo prepare: %s ... %s\nWith prepare: %s ... %s" \
              % (
                  result_no_prepare[:100], result_no_prepare[-100:], result_with_prepare[:100],
                  result_with_prepare[-100:])
        diffs = DeepDiff(result_no_prepare, result_with_prepare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        # self.assertTrue(sorted(result_no_prepare) == sorted(result_with_prepare), msg)

    def run_cbq_query_curl(self, query=None, server=None):
        if query is None:
            query = self.query
        if server is None:
            server = self.master

        shell = RemoteMachineShellConnection(server)
        cmd = (
                self.curl_path + " -u " + self.master.rest_username + ":" + self.master.rest_password + " http://" + server.ip + ":" + server.n1ql_port + "/query/service -d " \
                                                                                                                                                          "statement=" + query)

        output, error = shell.execute_command(cmd)
        json_output_str = ''
        for s in output:
            json_output_str += s
        return json.loads(json_output_str)

    def run_cbq_query(self, query=None, min_output_size=10, server=None, query_params={}, is_prepared=False, encoded_plan=None, username=None, password=None, use_fts_query_param=None, debug_query=True, query_context=None, txnid=None, txtimeout=None,atr_collection=None, is_analytics=False):
        if query is None:
            query = self.query
        if server is None:
            server = self.master
            if self.input.tuq_client and "client" in self.input.tuq_client:
                server = self.tuq_client
        cred_params = {'creds': []}
        rest = RestConnection(server)
        if username is None and password is None:
            username = rest.username
            password = rest.password
        cred_params['creds'].append({'user': username, 'pass': password})
        query_params.update(cred_params)
        if use_fts_query_param:
            query_params['use_fts'] = True
        if query_context:
            query_params['query_context'] = query_context
        else:
            query_params.pop('query_context', None)
        if txtimeout:
            query_params['txtimeout'] = txtimeout
        else:
            query_params.pop('txtimeout', None)
        if txnid:
            query_params['txid'] = ''"{0}"''.format(txnid)
        else:
            query_params.pop('txid', None)
        if atr_collection:
            query_params['atrcollection'] = atr_collection
        else:
            query_params.pop('atrcollection', None)

        if self.testrunner_client == 'python_sdk' and not is_prepared:
            try:
                from couchbase.n1ql import N1QLQuery, STATEMENT_PLUS, CONSISTENCY_REQUEST, MutationState
            except ImportError:
                print("Warning: failed to import couchbase lib")
            sdk_cluster = Cluster('couchbase://' + str(server.ip))
            authenticator = PasswordAuthenticator(username, password)
            sdk_cluster.authenticate(authenticator)
            for bucket in self.buckets:
                cb = sdk_cluster.open_bucket(bucket.name)

            sdk_query = N1QLQuery(query)

            # if is_prepared:
            #     sdk_query.adhoc = False

            if 'scan_consistency' in query_params:
                if query_params['scan_consistency'] == 'REQUEST_PLUS':
                    sdk_query.consistency = CONSISTENCY_REQUEST  # request_plus is currently mapped to the CONSISTENT_REQUEST constant in the Python SDK
                elif query_params['scan_consistency'] == 'STATEMENT_PLUS':
                    sdk_query.consistency = STATEMENT_PLUS
                else:
                    raise ValueError('Unknown consistency')
            # Python SDK returns results row by row, so we need to iterate through all the results
            row_iterator = cb.n1ql_query(sdk_query)
            content = []
            try:
                for row in row_iterator:
                    content.append(row)
                row_iterator.meta['results'] = content
                result = row_iterator.meta
            except Exception as e:
                # This will parse the resulting HTTP error and return only the dictionary containing the query results
                result = ast.literal_eval(str(e).split("value=")[1].split(", http_status")[0])

        elif self.use_rest:
            query_params.update({'scan_consistency': self.scan_consistency})
            if hasattr(self, 'query_params') and self.query_params:
                query_params = self.query_params
            if self.hint_index and (query.lower().find('select') != -1):
                from_clause = re.sub(r'let.*', '',
                                     re.sub(r'.*from', '', re.sub(r'where.*', '', query)))
                from_clause = re.sub(r'LET.*', '',
                                     re.sub(r'.*FROM', '', re.sub(r'WHERE.*', '', from_clause)))
                from_clause = re.sub(r'select.*', '', re.sub(r'order by.*', '',
                                                             re.sub(r'group by.*', '',
                                                                    from_clause)))
                from_clause = re.sub(r'SELECT.*', '', re.sub(r'ORDER BY.*', '',
                                                             re.sub(r'GROUP BY.*', '',
                                                                    from_clause)))
                hint = ' USE INDEX (%s using %s) ' % (self.hint_index, self.index_type)
                query = query.replace(from_clause, from_clause + hint)

            if not is_prepared and debug_query:
                self.log.info('RUN QUERY %s' % query)

            if self.analytics or is_analytics:
                if ';' not in query:
                    query = query + ";"
                if not self.udfs:
                    for bucket in self.buckets:
                        query = query.replace(bucket.name, bucket.name + "_shadow")
                result1 = RestConnection(self.cbas_node).execute_statement_on_cbas(query,
                                                                                   "immediate")
                try:
                    result = json.loads(result1)
                except Exception as ex:
                    self.log.error("CANNOT LOAD QUERY RESULT IN JSON: %s" % ex.message)
                    self.log.error("INCORRECT DOCUMENT IS: " + str(result1))
            else:
                result = rest.query_tool(query, self.n1ql_port, query_params=query_params,
                                         is_prepared=is_prepared, named_prepare=self.named_prepare,
                                         encoded_plan=encoded_plan, servers=self.servers, verbose=debug_query)
        else:
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside(
                    "$GOPATH/src/github.com/couchbase/query/" + \
                    "shell/cbq/cbq ", "", "", "", "", "", "")
            else:
                if not (self.isprepared):
                    query = query.replace('"', '\\"')
                    query = query.replace('`', '\\`')
                    if self.ipv6:
                        cmd = "%scbq  -engine=http://%s:%s/ -q -u %s -p %s" % (
                            self.path, server.ip, self.n1ql_port, username, password)
                    else:
                        cmd = "%scbq  -engine=http://%s:%s/ -q -u %s -p %s" % (
                            self.path, server.ip, server.port, username, password)

                    output = self.shell.execute_commands_inside(cmd, query, "", "", "", "", "")
                    if not (output[0] == '{'):
                        output1 = '{%s' % output
                    else:
                        output1 = output
                    try:
                        if "\x04" in output1:
                            output1 = output1.replace("\x04","")
                        result = json.loads(output1)
                    except Exception as ex:
                        self.log.error(f"CANNOT LOAD QUERY RESULT IN JSON: {str(ex)}")
                        self.log.error("INCORRECT DOCUMENT IS: " + str(output1))
        try:
            if isinstance(result, str) or 'errors' in result:
                raise CBQError(result, server.ip)
        except Exception as ex:
            self.log.error(f"PROBLEM WITH RESULT. TYPE IS: {type(result)} AND CONTENT IS: {result}")
            raise CBQError(result, server.ip)
        if 'metrics' in result and debug_query:
            self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])
        return result

    def build_url(self, version):
        info = self.shell.extract_remote_info()
        type = info.distribution_type.lower()
        if type in ["ubuntu", "centos", "red hat"]:
            url = "https://s3.amazonaws.com/packages.couchbase.com/releases/couchbase-query/dp1/"
            url += "couchbase-query_%s_%s_linux.tar.gz" % (version, info.architecture_type)
        # TODO for windows
        return url

    def _build_tuq(self, server):
        if self.version == "git_repo":
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                goroot = testconstants.LINUX_GOROOT
                gopath = testconstants.LINUX_GOPATH
            else:
                goroot = testconstants.WINDOWS_GOROOT
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if self.input.tuq_client and "goroot" in self.input.tuq_client:
                goroot = self.input.tuq_client["goroot"]
            cmd = "rm -rf {0}/src/github.com".format(gopath)
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) + \
                  ' export PATH=$PATH:$GOROOT/bin && go get github.com/couchbaselabs/tuqtng;' + \
                  'cd $GOPATH/src/github.com/couchbaselabs/tuqtng; go get -d -v ./...; cd .'
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) + \
                  ' export PATH=$PATH:$GOROOT/bin && cd $GOPATH/src/github.com/couchbaselabs/tuqtng; go build; cd .'
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) + \
                  ' export PATH=$PATH:$GOROOT/bin && cd $GOPATH/src/github.com/couchbaselabs/tuqtng/tuq_client; go build; cd .'
            self.shell.execute_command(cmd)
        else:
            cbq_url = self.build_url(self.version)
            # TODO for windows
            cmd = "cd /tmp; mkdir tuq;cd tuq; wget {0} -O tuq.tar.gz;".format(cbq_url)
            cmd += "tar -xvf tuq.tar.gz;rm -rf tuq.tar.gz"
            self.shell.execute_command(cmd)

    def _start_command_line_query(self, server, options='', user=None, password=None):
        out = ''
        if user and password:
            auth_row = '%s:%s@' % (user, password)
        os = self.shell.extract_remote_info().type.lower()
        if self.flat_json:
            if os == 'windows':
                gopath = testconstants.WINDOWS_GOPATH
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) + \
                      "./cbq-engine.exe -datastore=dir:%sdata >/dev/null 2>&1 &" % (self.directory_flat_json)
            else:
                gopath = testconstants.LINUX_GOPATH
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) + \
                      "./cbq-engine -datastore=dir:%s/data >n1ql.log 2>&1 &" % (self.directory_flat_json)
            out = self.shell.execute_command(cmd)
            self.log.info(out)
        elif self.version == "git_repo":
            if os != 'windows':
                gopath = testconstants.LINUX_GOPATH
            else:
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if os == 'windows':
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) + \
                      "./cbq-engine.exe -datastore http://%s%s:%s/ %s >/dev/null 2>&1 &" % (
                          ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            else:
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) + \
                      "./cbq-engine -datastore http://%s%s:%s/ %s >n1ql.log 2>&1 &" % (
                          ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            out = self.shell.execute_command(cmd)
        elif self.version == "sherlock":
            if self.services_init and self.services_init.find('n1ql') != -1:
                return
            if self.master.services and self.master.services.find('n1ql') != -1:
                return
            if os == 'windows':
                couchbase_path = testconstants.WIN_COUCHBASE_BIN_PATH
                cmd = "cd %s; " % (couchbase_path) + \
                      "./cbq-engine.exe -datastore http://%s%s:%s/ %s >/dev/null 2>&1 &" % (
                          ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            else:
                couchbase_path = testconstants.LINUX_COUCHBASE_BIN_PATH
                cmd = "cd %s; " % (couchbase_path) + \
                      "./cbq-engine -datastore http://%s%s:%s/ %s >/dev/null 2>&1 &" % (
                          ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            out = self.shell.execute_command(cmd)
            self.log.info(out)
        else:
            if os != 'windows':
                cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd /cygdrive/c/tuq;./cbq-engine.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" % (
                    server.ip, server.port)
            self.shell.execute_command(cmd)
        return out

    # This method has no usages anywhere
    def _set_env_variable(self, server):
        self.shell.execute_command(
            "export NS_SERVER_CBAUTH_URL=\"http://{0}:{1}/_cbauth\"".format(server.ip, server.port))
        self.shell.execute_command("export NS_SERVER_CBAUTH_USER=\"{0}\"".format(server.rest_username))
        self.shell.execute_command("export NS_SERVER_CBAUTH_PWD=\"{0}\"".format(server.rest_password))
        self.shell.execute_command(
            "export NS_SERVER_CBAUTH_RPC_URL=\"http://{0}:{1}/cbauth-demo\"".format(server.ip, server.port))
        self.shell.execute_command(
            "export CBAUTH_REVRPC_URL=\"http://{0}:{1}@{2}:{3}/query\"".format(server.rest_username,
                                                                               server.rest_password, server.ip,
                                                                               server.port))

    # This method has no usages anywhere
    def _parse_query_output(self, output):
        if output.find("cbq>") == 0:
            output = output[output.find("cbq>") + 4:].strip()
        if output.find("tuq_client>") == 0:
            output = output[output.find("tuq_client>") + 11:].strip()
        if output.find("cbq>") != -1:
            output = output[:output.find("cbq>")].strip()
        if output.find("tuq_client>") != -1:
            output = output[:output.find("tuq_client>")].strip()
        return json.loads(output)

    def _verify_results(self, actual_result, expected_result):
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]
        diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def _verify_results_old(self, actual_result, expected_result):
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]
            self.assertTrue(actual_result == expected_result, "Results are incorrect")
            return
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:100], extra[:100]))
            self.fail(
                "Results are incorrect.Actual num %s. Expected num: %s.\n" % (len(actual_result), len(expected_result)))
        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s Expected first and last 100: %s.\n  ... \n %s" \
              % (actual_result[:100], actual_result[-100:], expected_result[:100], expected_result[-100:])
        self.assertTrue(actual_result == expected_result, msg)

    def _verify_aggregate_query_results(self, result, query, bucket):
        def _result_compare(actual, expected):
            temp = list(expected)
            try:
                for elem in actual:
                    temp.remove(elem)
            except ValueError:
                return False
            return not temp

        self.restServer = self.get_nodes_from_services_map(service_type="n1ql")
        self.rest = RestConnection(self.restServer)
        self.rest.set_query_index_api_mode(1)
        primary_query = query % (bucket, "#primary")
        primary_result = self.run_cbq_query(primary_query)
        self.rest.set_query_index_api_mode(3)
        self.log.info(" Analyzing Expected Result")

        expected_result = [x['$1'] for x in primary_result["results"] if x['$1']]
        self.log.info(" Analyzing Actual Result")
        actual_result = [x['$1'] for x in result["results"] if x['$1']]
        if len(actual_result) != len(expected_result):
            self.log.error(f"Result sets do not match in length! actual: {len(actual_result)} , expected: {len(expected_result)}")
            return False
        diffs = DeepDiff(actual_result, expected_result, ignore_order=True, ignore_string_type_changes=True)
        if diffs:
            self.log.info(diffs)
            return False
        else:
            return True
        return False

    def check_missing_and_extra(self, actual, expected):
        missing, extra = [], []
        for item in actual:
            if not (item in expected):
                extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def sort_nested_list(self, result, key=None):
        actual_result = []
        for item in result:
            curr_item = {}
            for key, value in item.items():
                if isinstance(value, list) or isinstance(value, set):
                    if not isinstance(value, set) and key and isinstance(value[0], dict) and key in value:
                        curr_item[key] = sorted(value, key=lambda doc: (doc['task_name']))
                    else:
                        curr_item[key] = sorted(value, key=lambda doc: doc)
                else:
                    curr_item[key] = value
            actual_result.append(curr_item)
        return actual_result

    def configure_gomaxprocs(self):
        max_proc = self.input.param("gomaxprocs", None)
        cmd = "export GOMAXPROCS=%s" % max_proc
        for server in self.servers:
            shell_connection = RemoteMachineShellConnection(self.master)
            shell_connection.execute_command(cmd)

    def load_directory(self, generators_load):
        gens_load = []
        for generator_load in generators_load:
            gens_load.append(copy.deepcopy(generator_load))
        items = 0
        for gen_load in gens_load:
            items += (gen_load.end - gen_load.start)

        self.fail_if_no_buckets()
        for bucket in self.buckets:
            try:
                shell = RemoteMachineShellConnection(self.master)
                self.log.info(
                    "Delete directory's content %sdata/default/%s ..." % (self.directory_flat_json, bucket.name))
                o = shell.execute_command('rm -rf %sdata/default/*' % self.directory_flat_json)
                self.log.info("Create directory %sdata/default/%s..." % (self.directory_flat_json, bucket.name))
                o = shell.execute_command('mkdir -p %sdata/default/%s' % (self.directory_flat_json, bucket.name))
                self.log.info(
                    "Load %s documents to %sdata/default/%s..." % (items, self.directory_flat_json, bucket.name))
                for gen_load in gens_load:
                    gen_load.reset()
                    for i in range(gen_load.end):
                        key, value = next(gen_load)
                        out = shell.execute_command(
                            "echo '%s' > %sdata/default/%s/%s.json" % (value, self.directory_flat_json,
                                                                       bucket.name, key))
                self.log.info("LOAD IS FINISHED")
            except Exception as ex:
                self.log.info(ex)
                traceback.print_exc()
            finally:
                shell.disconnect()

    '''Two separate flags are used to control whether or not a primary index is created, one for tuq(skip_index)
       and one for newtuq(skip_primary_index) we should go back and merge these flags and fix the conf files'''

    def create_primary_index_for_3_0_and_greater(self):
        if self.skip_index or self.skip_primary_index:
            self.log.info("Not creating index")
            return
        if self.flat_json:
            return
        self.build_deferred_indexes()
        # ensure_primary_indexes_exist() enumerates every keyspace itself, so it does not
        # need re-running once per bucket; doing so multiplied the retry budget by
        # len(self.buckets). Bound the whole phase with a deadline so an index that can
        # never come online costs seconds, not the ~65 min per test seen in job 86079.
        started = time.time()
        deadline = started + self.primary_index_timeout
        try:
            while True:
                try:
                    self.ensure_primary_indexes_exist()
                    break
                except Exception as ex:
                    if time.time() >= deadline:
                        raise
                    self.sleep(1, 'ensure_primary_indexes_exist failed: %s, retrying' % ex)
            self.primary_index_created = True
            for bucket in self.buckets:
                self.log.info("-->waiting for indexes online, bucket:{}".format(bucket.name))
                self._wait_for_index_online(bucket.name, '#primary',
                                            timeout=max(1, deadline - time.time()),
                                            build_deferred=True)
        except Exception as ex:
            self.log.error("create_primary_index_for_3_0_and_greater gave up after %ss (buckets=%s): %s"
                           % (int(time.time() - started), [b.name for b in self.buckets], ex),
                           exc_info=True)
            if self.fail_on_primary_index_error:
                self.fail("setUp could not ensure primary indexes exist: %s" % ex)

    def ensure_primary_indexes_exist(self):
        self.log.info("--> start: ensure_primary_indexes_exist..")
        query_response = self.run_cbq_query("SELECT * FROM system:keyspaces WHERE keyspaces.`bucket` is missing")
        self.log.info("-->query_response:{}".format(query_response))
        buckets = [i['keyspaces']['name'] for i in query_response['results']]
        current_indexes = self.get_parsed_indexes()
        index_list = [{'name': '#primary',
                       'bucket': bucket,
                       'fields': [],
                       'state': 'online',
                       'using': self.index_type.lower(),
                       'is_primary': True} for bucket in buckets]
        desired_indexes = self.parse_desired_indexes(index_list)
        desired_index_set = self.make_hashable_index_set(desired_indexes)
        current_index_set = self.make_hashable_index_set(current_indexes)
        self.log.info("-->before create indexes: {},{},{}".format(index_list, desired_indexes, current_indexes))
        self.create_desired_indexes(desired_index_set, current_index_set, desired_indexes)
        self.log.info("--> end: ensure_primary_indexes_exist..")

    def build_deferred_indexes(self, primary_only=True, timeout=120):
        """Bring bucket-level indexes stuck in 'deferred' state online.

        ensure_primary_indexes_exist() asks for #primary in state 'online' and
        make_hashable_index_set() folds 'state' into the comparison, so a #primary that
        exists but is 'deferred' never matches. create_desired_indexes() then issues a
        CREATE PRIMARY INDEX that fails with 4300 (already exists) and falls through to
        wait_for_index_present(), which polls for status='online' and can never succeed
        because no code path issues BUILD INDEX.

        cbbackupmgr restore produces exactly this: it restores GSI definitions without
        building them. Issue the missing BUILD INDEX, bounded at timeout per index and
        always outside a retry loop. Best effort - this must never raise.

        primary_only=True (the default) is what setUp uses: it is the narrowest change
        that fixes the stall, and it cannot disturb suites that deliberately create
        deferred secondary indexes. Callers that own every index in the cluster - e.g. a
        restore test cleaning up after itself - can pass primary_only=False.
        """
        try:
            deferred = [index for index in self.get_parsed_indexes()
                        if index['state'] == 'deferred'
                        and (index['is_primary'] or not primary_only)]
        except Exception as ex:
            self.log.info("-->could not list indexes to build deferred indexes: {0}".format(ex))
            return
        for index in deferred:
            try:
                self.log.info("-->building deferred index {0} on bucket:{1}".format(
                    index['name'], index['bucket']))
                self._wait_for_index_online(index['bucket'], index['name'],
                                            timeout=timeout, build_deferred=True)
            except Exception as ex:
                self.log.warning("-->deferred index {0} on bucket:{1} did not come online: {2}".format(
                    index['name'], index['bucket'], ex))

    def _wait_for_index_online(self, bucket, index_name, timeout=600, build_deferred=False):
        end_time = time.time() + timeout
        while time.time() < end_time:
            query = "SELECT * FROM system:indexes where name='%s'" % index_name
            res = self.run_cbq_query(query)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    self.log.error(item)
                    continue
                bucket_name = ""
                if isinstance(bucket, str) or isinstance(bucket, str):
                    bucket_name = bucket
                else:
                    bucket_name = bucket.name
                if item['indexes']['keyspace_id'] == bucket_name:
                    if item['indexes']['state'] == "online":
                        return
                    if build_deferred and item['indexes']['state'] == "deferred":
                        self.run_cbq_query(query=f"BUILD INDEX ON `{item['indexes']['keyspace_id']}` (`{item['indexes']['name']}`) USING {item['indexes']['using']}")
            self.sleep(1, 'index is pending or not in the list. sleeping... (%s)' % [item['indexes'] for item in
                                                                                     res['results']])
        raise Exception('index %s is not online. last response is %s' % (index_name, res))

    def _debug_fts_request(self, request=""):
        cmd = "curl -XPOST -H \"Content-Type: application/json\" -u " + self.username + ":" + self.password + " " \
                                                                                                              "http://" + self.master.ip + ":8094/api/index/idx_beer_sample_fts/query -d " + request

        shell = RemoteMachineShellConnection(self.master)

        output, error = shell.execute_command(cmd)
        json_output_str = ''
        for s in output:
            json_output_str += s
        result = json.loads(json_output_str)
        return result

    ##############################################################################################
    #
    #   newtuq COMMON FUNCTIONS
    ##############################################################################################

    def run_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def run_query_with_subquery_select_from_template(self, query_template):
        subquery_template = re.sub(r'.*\$subquery\(', '', query_template)
        subquery_template = subquery_template[:subquery_template.rfind(')')]
        keys_num = int(re.sub(r'.*KEYS \$', '', subquery_template).replace('KEYS $', ''))
        subquery_full_list = self.generate_full_docs_list(gens_load=self.gens_load, keys=self._get_keys(keys_num))
        subquery_template = re.sub(r'USE KEYS.*', '', subquery_template)
        sub_results = TuqGenerators(self.log, subquery_full_list)
        self.query = sub_results.generate_query(subquery_template)
        expected_sub = sub_results.generate_expected_result()
        alias = re.sub(r',.*', '', re.sub(r'.*\$subquery\(.*\)', '', query_template))
        alias = re.sub(r'.*as', '', re.sub(r'FROM.*', '', alias)).strip()
        if not alias:
            alias = '$1'
        for item in self.gen_results.full_set:
            item[alias] = expected_sub[0]
        query_template = re.sub(r',.*\$subquery\(.*\).*%s' % alias, ',%s' % alias, query_template)
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def run_query_with_subquery_from_template(self, query_template):
        subquery_template = re.sub(r'.*\$subquery\(', '', query_template)
        subquery_template = subquery_template[:subquery_template.rfind(')')]
        subquery_full_list = self.generate_full_docs_list(gens_load=self.gens_load)
        sub_results = TuqGenerators(self.log, subquery_full_list)
        self.query = sub_results.generate_query(subquery_template)
        expected_sub = sub_results.generate_expected_result()
        alias = re.sub(r',.*', '', re.sub(r'.*\$subquery\(.*\)', '', query_template))
        alias = re.sub(r'.*as ', '', alias).strip()
        self.gen_results = TuqGenerators(self.log, expected_sub)
        query_template = re.sub(r'\$subquery\(.*\).*%s' % alias, ' %s' % alias, query_template)
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def _get_keys(self, key_num):
        keys = []
        for gen in self.gens_load:
            gen_copy = copy.deepcopy(gen)
            for i in range(gen_copy.end):
                key, _ = next(gen_copy)
                keys.append(key)
                if len(keys) == key_num:
                    return keys
        return keys

    def run_active_requests(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                result = self.run_cbq_query("select * from system:active_requests")
                self.assertTrue(result['metrics']['resultCount'] == 1)
                requestId = result['requestID']
                result = self.run_cbq_query(
                    'delete from system:active_requests where requestId  =  "%s"' % requestId)
                # time.sleep(20)

                retries = 3
                while retries > 0:
                    result = self.run_cbq_query(
                        'select * from system:active_requests  where requestId  =  "%s"' % requestId)
                    if result['metrics']['resultCount'] > 0:
                        self.sleep(5)
                        retries -= 1
                    else:
                        break;

                self.assertTrue(result['metrics']['resultCount'] == 0)
                result = self.run_cbq_query("select * from system:completed_requests")
                requestId = result['requestID']
                result = self.run_cbq_query(
                    'delete from system:completed_requests where requestId  =  "%s"' % requestId)
                # time.sleep(10)
                retries = 3
                while retries > 0:
                    result = self.run_cbq_query(
                        'select * from system:completed_requests where requestId  =  "%s"' % requestId)
                    if result['metrics']['resultCount'] > 0:
                        self.sleep(5)
                        retries -= 1
                    else:
                        break;

                self.assertTrue(result['metrics']['resultCount'] == 0)

    def debug_query(self, query, expected_result, result, function_name):
        print(("### " + function_name + " #### QUERY ::" + str(query) + "::"))
        print(("### " + function_name + " #### EXPECTED RESULT ::" + str(expected_result) + "::"))
        print(("### " + function_name + " #### FULL RESULT ::" + str(result) + "::"))

    def normalize_result(self, result):
        if len(result['results'][0]) == 0:
            return 'missing'
        return result['results'][0]['$1']

    def null_to_none(self, s):
        if s.lower() == 'null':
            return None
        return s

    def process_CBQE(self, s, index=0):
        '''
        return json object {'code':12345, 'msg':'error message'}
        '''
        content = ast.literal_eval(str(s).split("ERROR:")[1])
        json_parsed = json.loads(json.dumps(content))
        return json_parsed['errors'][index]

    ##############################################################################################
    #
    #   tuq_sanity.py helpers
    ##############################################################################################

    def expected_substr(self, a_string, start, index):
        if start is 0:
            substring = a_string[index:]
            if index >= len(a_string):
                return None
            elif index < -len(a_string):
                return None
            else:
                return substring
        if start is 1:
            substring = a_string[index - start:] if index > 0 else a_string[index:]
            if index >= len(a_string):
                return None
            elif index < -len(a_string):
                return None
            else:
                return substring

    def run_regex_query(self, word, substring, regex_type=''):
        self.query = "select REGEXP_POSITION%s('%s', '%s')" % (regex_type, word, substring)
        results = self.run_cbq_query()
        return results['results'][0]['$1']

    def run_position_query(self, word, substring, position_type=''):
        self.query = "select POSITION%s('%s', '%s')" % (position_type, word, substring)
        results = self.run_cbq_query()
        return results['results'][0]['$1']

    def check_explain_covering_index(self, index):
        for bucket in self.buckets:
            res = self.run_cbq_query()
            s = pprint.pformat(res, indent=4)
            if index in s:
                self.log.info("correct index used in json result ")
            else:
                self.log.error("correct index not used in json result ")
                self.fail("correct index not used in json result ")
            if 'covers' in s:
                self.log.info("covers key present in json result ")
            else:
                self.log.error("covers key missing from json result ")
                self.fail("covers key missing from json result ")
            if 'cover' in s:
                self.log.info("cover keyword present in json children ")
            else:
                self.log.error("cover keyword missing from json children ")
                self.fail("cover keyword missing from json children ")
            if 'IntersectScan' in s:
                self.log.error("This is a covered query, Intersect scan should not be used")

    ##############################################################################################
    #
    #   upgrade_n1qlrbac.py helpers
    ##############################################################################################
    def query_select_insert_update_delete_helper(self):
        self.create_users(users=[{'id': 'john_insert', 'name': 'johnInsert', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_update', 'name': 'johnUpdate', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_delete', 'name': 'johnDelete', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_select', 'name': 'johnSelect', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_select2', 'name': 'johnSelect2', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_rep', 'name': 'johnRep', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_bucket_admin', 'name': 'johnBucketAdmin', 'password': 'password'}])
        items = [("query_insert", 'john_insert'), ("query_update", 'john_update'), ("query_delete", 'john_delete'),
                 ("query_select", 'john_select'), ("bucket_admin", 'john_bucket_admin'),
                 ("query_select", 'john_select2')]
        for bucket in self.buckets:
            for item in items:
                self.query = "GRANT {0} on {2} to {1}".format(item[0], item[1], bucket.name)
                self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)

            self.query = "GRANT {0} to {1}".format("replication_admin", 'john_rep')
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)

    def query_select_insert_update_delete_helper_default(self):
        self.create_users(users=[{'id': 'john_insert', 'name': 'johnInsert', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_update', 'name': 'johnUpdate', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_delete', 'name': 'johnDelete', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_select', 'name': 'johnSelect', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_select2', 'name': 'johnSelect2', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_rep', 'name': 'johnRep', 'password': 'password'}])
        self.create_users(users=[{'id': 'john_bucket_admin', 'name': 'johnBucketAdmin', 'password': 'password'}])
        self.query = "GRANT {0} to {1}".format("replication_admin", 'john_rep')
        self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)

    def change_and_update_permission(self, query_type, permission, user, bucket, cmd, error_msg):
        if query_type == 'with_bucket':
            self.query = "GRANT {0} on {1} to {2}".format(permission, bucket, user)
        if query_type == 'without_bucket':
            self.query = "GRANT {0} to {1}".format(permission, user)
        if query_type in ['with_bucket', 'without_bucket']:
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), error_msg.format(bucket, user))
        self.log.info("Query executed successfully")

    def check_permissions_helper(self):
        for bucket in self.buckets:
            cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
                  "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"test\", { \"value1\": \"one1\" })'" % \
                  (self.curl_path, 'john_insert', 'password', self.master.ip, bucket.name)
            self.change_and_update_permission(None, None, 'johnInsert', bucket.name, cmd,
                                              "Unable to insert into {0} as user {1}")

            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'". \
                format('john_update', 'password', self.master.ip, bucket.name, new_name, old_name, self.curl_path)
            self.change_and_update_permission(None, None, 'johnUpdate', bucket.name, cmd,
                                              "Unable to update into {0} as user {1}")

            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=DELETE FROM {3} a WHERE name = '{4}''". \
                format('john_delete', 'password', self.master.ip, bucket.name, del_name, self.curl_path)
            self.change_and_update_permission(None, None, 'john_delete', bucket.name, cmd,
                                              "Unable to delete from {0} as user {1}")

            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'". \
                format('john_select2', 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission(None, None, 'john_select2', bucket.name, cmd,
                                              "Unable to select from {0} as user {1}")

    def create_and_verify_system_catalog_users_helper(self):
        self.create_users(users=[{'id': 'john_system', 'name': 'john', 'password': 'password'}])
        self.query = "GRANT {0} to {1}".format("query_system_catalog", 'john_system')
        self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        for bucket in self.buckets:
            cmds = ["{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:namespaces'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:datastores'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:indexes'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:completed_requests'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:active_requests'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:prepareds'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path),
                    "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:my_user_info'". \
                        format('john_system', 'password', self.master.ip, bucket.name, self.curl_path)]
            for cmd in cmds:
                self.change_and_update_permission(None, None, 'john_system', bucket.name, cmd,
                                                  "Unable to select from {0} as user {1}")

    def check_system_catalog_helper(self):
        """
        These test might fail for now as system catalog tables are not
        fully implemented based on query PM's doc.
        :return:
        """
        self.system_catalog_helper_delete_for_upgrade()
        self.system_catalog_helper_select_for_upgrade()

    def query_assert_success(self, query):
        self.query = query
        res = self.run_cbq_query(query=self.query)
        self.assertEqual(res['status'], 'success')

    def system_catalog_helper_select_for_upgrade(self):
        for query in ['select * from system:datastores', 'select * from system:namespaces',
                      'select * from system:keyspaces']:
            self.query_assert_success(query)
        self.query = 'create index idx1 on {0}(name)'.format(self.buckets[0].name)
        res = self.run_cbq_query(query=query)
        # self.sleep(10)
        for query in ['select * from system:indexes', 'select * from system:dual',
                      "prepare st1 from select * from {0} union select * from {0} union select * from {0}".format(
                          self.buckets[0].name),
                      'execute st1']:
            self.query_assert_success(query)

    def system_catalog_helper_delete_for_upgrade(self):
        self.queries = ['delete from system:datastores', 'delete from system:namespaces',
                        'delete from system:keyspaces',
                        'delete from system:indexes', 'delete from system:user_info', 'delete from system:nodes',
                        'delete from system:applicable_roles']
        for query in self.queries:
            try:
                self.run_cbq_query(query=query)
            except Exception as ex:
                self.log.error(ex)
                self.assertNotEqual(str(ex).find("'code': 11003"), -1)
        try:
            query = 'delete from system:dual'
            self.run_cbq_query(query=query)
        except Exception as ex:
            self.log.error(ex)
            self.assertNotEqual(str(ex).find("'code': 11000"), -1)

        queries = ['delete from system:completed_requests', 'delete from system:active_requests where state!="running"',
                   'delete from system:prepareds']
        for query in queries:
            res = self.run_cbq_query(query=query)
            self.assertEqual(res['status'], 'success')

        queries = ['select * from system:completed_requests', 'select * from system:active_requests',
                   'select * from system:prepareds']
        for query in queries:
            res = self.run_cbq_query(query=query)
            self.assertEqual(res['status'], 'success')

    def change_and_verify_pre_upgrade_ldap_users_permissions(self):
        for bucket in self.buckets:
            # change permission of john_bucketadmin1 and verify its able to execute the correct query.
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'". \
                format('bucket0', 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_select", 'bucket0', bucket.name, cmd,
                                              "Unable to select from {0} as user {1}")

            # change permission of john_bucketadminAll and verify its able to execute the correct query.
            cmd = "%s -u %s:%s http://%s:8093/query/service -d 'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" })'" \
                  % (self.curl_path, 'bucket0', 'password', self.master.ip, bucket.name)
            self.change_and_update_permission('with_bucket', "query_insert", 'bucket0', bucket.name, cmd,
                                              "Unable to insert into {0} as user {1}")

            # change permission of cluster_user and verify its able to execute the correct query.
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('bucket0', 'password', self.master.ip, bucket.name, new_name,
                                                 old_name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_update", 'bucket0', bucket.name, cmd,
                                              "Unable to update  {0} as user {1}")

            # change permission of bucket0 and verify its able to execute the correct query.
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d 'statement=DELETE FROM {3} a WHERE name = '{4}''". \
                format('bucket0', 'password', self.master.ip, bucket.name, del_name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_delete", 'bucket0', bucket.name, cmd,
                                              "Unable to delete from {0} as user {1}")

            # change permission of cbadminbucket user and verify its able to execute the correct query.
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'". \
                format('cbadminbucket', 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission('without_bucket', "query_system_catalog", 'cbadminbucket',
                                              'cbadminbucket', cmd,
                                              "Unable to select from system:keyspaces as user {0}")

    def create_ldap_auth_helper(self):
        """
        Helper function for creating ldap users pre-upgrade
        :return:
        """
        # not able to create bucket admin on passwordless bucket pre upgrade
        users = [
            {'id': 'john_bucketadminAll', 'name': 'john_bucketadminAll', 'password': 'password'},
            {'id': 'cluster_user', 'name': 'cluster_user', 'password': 'password'},
            {'id': 'read_user', 'name': 'read_user', 'password': 'password'},
            {'id': 'cadmin', 'name': 'cadmin', 'password': 'password'}, ]
        RbacBase().create_user_source(users, 'ldap', self.master)
        rolelist = [{'id': 'john_bucketadminAll', 'name': 'john_bucketadminAll', 'roles': 'bucket_admin[*]'},
                    {'id': 'cluster_user', 'name': 'cluster_user', 'roles': 'cluster_admin'},
                    {'id': 'read_user', 'name': 'read_user', 'roles': 'ro_admin'},
                    {'id': 'cadmin', 'name': 'cadmin', 'roles': 'admin'}]
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'ldap')

    def verify_pre_upgrade_users_permissions_helper(self, test=''):

        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'". \
            format('bucket0', 'password', self.master.ip, 'bucket0', self.curl_path)
        self.change_and_update_permission(None, None, 'bucket0', 'bucket0', cmd,
                                          "Unable to select from {0} as user {1}")

        if test == 'online_upgrade':
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'". \
                format('cbadminbucket', 'password', self.master.ip, 'default', self.curl_path)
        else:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'". \
                format('cbadminbucket', 'password', self.master.ip, 'bucket0', self.curl_path)

        self.change_and_update_permission(None, None, 'cbadminbucket', 'bucket0', cmd,
                                          "Unable to select from {0} as user {1}")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'". \
            format('cbadminbucket', 'password', self.master.ip, self.curl_path)
        self.change_and_update_permission(None, None, 'cbadminbucket', 'system:keyspaces', cmd,
                                          "Unable to select from {0} as user {1}")

        for bucket in self.buckets:
            cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
                  "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"5\", { \"value1\": \"one1\" })'" % \
                  (self.curl_path, 'bucket0', 'password', self.master.ip, bucket.name)

            self.change_and_update_permission(None, None, 'bucket0', bucket.name, cmd,
                                              "Unable to insert into {0} as user {1}")

            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'". \
                format('bucket0', 'password', self.master.ip, bucket.name, new_name, old_name, self.curl_path)
            self.change_and_update_permission(None, None, 'bucket0', bucket.name, cmd,
                                              "Unable to update into {0} as user {1}")

            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d 'statement=DELETE FROM {3} a WHERE name = '{4}''". \
                format('bucket0', 'password', self.master.ip, bucket.name, del_name, self.curl_path)
            self.change_and_update_permission(None, None, 'bucket0', bucket.name, cmd,
                                              "Unable to delete from {0} as user {1}")

    def use_pre_upgrade_users_post_upgrade(self):
        for bucket in self.buckets:
            cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
                  "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"test2\", { \"value1\": \"one1\" })'" % \
                  (self.curl_path, 'cbadminbucket', 'password', self.master.ip, bucket.name)
            self.change_and_update_permission(None, None, 'johnInsert', bucket.name, cmd,
                                              "Unable to insert into {0} as user {1}")

            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'". \
                format('cbadminbucket', 'password', self.master.ip, bucket.name, new_name, old_name, self.curl_path)
            self.change_and_update_permission(None, None, 'johnUpdate', bucket.name, cmd,
                                              "Unable to update into {0} as user {1}")

            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'". \
                format(bucket.name, 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission(None, None, bucket.name, bucket.name, cmd,
                                              "Unable to select from {0} as user {1}")

    def change_permissions_and_verify_pre_upgrade_users(self):
        for bucket in self.buckets:
            # change permission of john_cluster and verify its able to execute the correct query.
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'". \
                format(bucket.name, 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_select", bucket.name,
                                              bucket.name, cmd, "Unable to select from {0} as user {1}")

            # change permission of ro_non_ldap and verify its able to execute the correct query.
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('cbadminbucket', 'readonlypassword', self.master.ip, bucket.name,
                                                 new_name, old_name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_update", 'cbadminbucket',
                                              bucket.name, cmd, "Unable to update  {0} as user {1}")

            # change permission of john_admin and verify its able to execute the correct query.
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=DELETE FROM {3} a WHERE name = '{4}''". \
                format('cbadminbucket', 'password', self.master.ip, bucket.name, del_name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_delete", 'cbadminbucket',
                                              bucket.name, cmd, "Unable to update  {0} as user {1}")

            # change permission of bob user and verify its able to execute the correct query.

            self.change_and_update_permission('without_bucket', "query_system_catalog", 'cbadminbucket',
                                              bucket.name, cmd, "Unable to select from system:keyspaces as user {1}")

    def change_permissions_and_verify_new_users(self):
        for bucket in self.buckets:
            # change permission of john_insert and verify its able to execute the correct query.
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'". \
                format('john_insert', 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission('with_bucket', "bucket_admin", 'john_insert',
                                              bucket.name, cmd, "Unable to select from {0} as user {1}")

            # change permission of john_update and verify its able to execute the correct query.
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=INSERT INTO {3} values(\"k055\", 123  )' " \
                .format('john_update', 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_insert", 'john_update',
                                              bucket.name, cmd, "Unable to insert into {0} as user {1}")

            # change permission of john_select and verify its able to execute the correct query.
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('john_select', 'password', self.master.ip, bucket.name, new_name,
                                                 old_name, self.curl_path)
            self.change_and_update_permission('without_bucket', "cluster_admin", 'john_select',
                                              bucket.name, cmd, "Unable to update  {0} as user {1}")
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'". \
                format('john_select', 'password', self.master.ip, bucket.name, self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_select'))

            # change permission of john_select2 and verify its able to execute the correct query.
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d 'statement=DELETE FROM {3} a WHERE name = '{4}''". \
                format('john_select2', 'password', self.master.ip, bucket.name, del_name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_delete", 'john_select2',
                                              bucket.name, cmd, "Unable to delete from {0} as user {1}")

            # change permission of john_delete and verify its able to execute the correct query.
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'". \
                format('john_delete', 'password', self.master.ip, bucket.name, self.curl_path)
            self.change_and_update_permission('with_bucket', "query_select", 'john_delete',
                                              bucket.name, cmd, "Unable to select from {0} as user {1}")

    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin', self.master)
        self.log.info("SUCCESS: User(s) %s created" % ','.join([user['name'] for user in users]))

    def create_users_before_upgrade_non_ldap(self):
        """
        password needs to be added statically for these users
        on the specific machine where ldap is enabled.
        """
        cli_cmd = "{0}couchbase-cli -c {1}:8091 -u Administrator -p password".format(self.path, self.master.ip)
        cmds = [("create a read only user account",
                 cli_cmd + " user-manage --set --ro-username=ro_non_ldap --ro-password=readonlypassword"),
                ("create a bucket admin on bucket0 user account",
                 cli_cmd + " admin-role-manage --set-users=bob --set-names=Bob --roles=bucket_admin[bucket0]"),
                ("create a bucket admin on all buckets user account",
                 cli_cmd + " admin-role-manage --set-users=mary --set-names=Mary --roles=bucket_admin[*]"),
                ("create a cluster admin user account",
                 cli_cmd + "admin-role-manage --set-users=john_cluster --set-names=john_cluster --roles=cluster_admin"),
                ("create a admin user account",
                 cli_cmd + " admin-role-manage --set-users=john_admin --set-names=john_admin --roles=admin")]
        for cmd in cmds:
            self.log.info(cmd[0])
            self.shell.execute_command(cmd[1])
        users = [{'id': 'Bob', 'name': 'Bob', 'password': 'password', 'roles': 'admin'},
                 {'id': 'mary', 'name': 'Mary', 'password': 'password', 'roles': 'cluster_admin'},
                 {'id': 'john_cluster', 'name': 'john_cluster', 'password': 'password', 'roles': 'cluster_admin'},
                 {'id': 'ro_non_ldap', 'name': 'ro_non_ldap', 'password': 'readonlypassword', 'roles': 'ro_admin'},
                 {'id': 'john_admin', 'name': 'john_admin', 'password': 'password', 'roles': 'admin'}]

        RbacBase().create_user_source(users, 'ldap', self.master)
        RbacBase().add_user_role(users, RestConnection(self.master), 'ldap')

    def _perform_offline_upgrade(self):
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            self.upgrade_servers.append(server)
        upgrade_threads = self._async_update(self.upgrade_to, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        # self.sleep(20)
        self.add_built_in_server_user()
        # self.sleep(20)
        self.upgrade_servers = self.servers

    def _perform_online_upgrade_with_rebalance(self):
        self.nodes_upgrade_path = self.input.param("nodes_upgrade_path", "").split("-")
        for service in self.nodes_upgrade_path:
            nodes = self.get_nodes_from_services_map(service_type=service, get_all_nodes=True)

            self.log.info("----- Upgrading all {0} nodes -----".format(service))
            for node in nodes:
                node_rest = RestConnection(node)
                node_info = "{0}:{1}".format(node.ip, node.port)
                node_services_list = node_rest.get_nodes_services()[node_info]
                node_services = [",".join(node_services_list)]

                if "n1ql" in node_services_list:
                    n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
                    if len(n1ql_nodes) > 1:
                        for n1ql_node in n1ql_nodes:
                            if node.ip != n1ql_node.ip:
                                self.n1ql_node = n1ql_node
                                break

                self.log.info("Rebalancing the node out...")
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node])
                rebalance.result()
                active_nodes = []
                for active_node in self.servers:
                    if active_node.ip != node.ip:
                        active_nodes.append(active_node)
                self.log.info("Upgrading the node...")
                upgrade_th = self._async_update(self.upgrade_to, [node])
                for th in upgrade_th:
                    th.join()

                self.log.info("==== Upgrade Complete ====")
                self.log.info("Adding node back to cluster...")
                rebalance = self.cluster.async_rebalance(active_nodes, [node], [], services=node_services)
                rebalance.result()
                # self.sleep(60)
                node_version = RestConnection(node).get_nodes_versions()
                self.log.info("{0} node {1} Upgraded to: {2}".format(service, node.ip, node_version))

    def _perform_online_upgrade_with_failover(self):
        self.nodes_upgrade_path = self.input.param("nodes_upgrade_path", "").split("-")
        for service in self.nodes_upgrade_path:
            nodes = self.get_nodes_from_services_map(service_type=service, get_all_nodes=True)

            self.log.info("----- Upgrading all {0} nodes -----".format(service))
            for node in nodes:
                node_rest = RestConnection(node)
                node_info = "{0}:{1}".format(node.ip, node.port)
                node_services_list = node_rest.get_nodes_services()[node_info]
                node_services = [",".join(node_services_list)]

                self.log.info("Rebalancing the node out...")
                failover_task = self.cluster.async_failover([self.master], failover_nodes=[node], graceful=False)
                failover_task.result()
                active_nodes = []
                for active_node in self.servers:
                    if active_node.ip != node.ip:
                        active_nodes.append(active_node)
                self.log.info("Upgrading the node...")
                upgrade_th = self._async_update(self.upgrade_to, [node])
                for th in upgrade_th:
                    th.join()

                self.log.info("==== Upgrade Complete ====")
                # self.sleep(30)

                self.log.info("Adding node back to cluster...")
                rest = RestConnection(self.master)
                nodes_all = rest.node_statuses()
                for cluster_node in nodes_all:
                    if cluster_node.ip == node.ip:
                        self.log.info("Adding Back: {0}".format(node))
                        rest.add_back_node(cluster_node.id)
                        rest.set_recovery_type(otpNode=cluster_node.id, recoveryType="full")

                self.log.info("Adding node back to cluster...")
                rebalance = self.cluster.async_rebalance(active_nodes, [], [])
                rebalance.result()
                # self.sleep(60)
                node_version = RestConnection(node).get_nodes_versions()
                self.log.info("{0} node {1} Upgraded to: {2}".format(service, node.ip, node_version))

    ##############################################################################################
    #
    # n1ql_rbac_2.py helpers
    # Again very specific, some things are generalizable, perhaps rbac should have its own query base test
    #
    ##############################################################################################
    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin', self.master)
        self.log.info("SUCCESS: User(s) %s created" % ','.join([user['name'] for user in users]))

    def assign_role(self, rest=None, roles=None):
        if not rest:
            rest = RestConnection(self.master)
        # Assign roles to users
        if not roles:
            roles = self.roles
        RbacBase().add_user_role(roles, rest, 'builtin')
        for user_role in roles:
            self.log.info("SUCCESS: Role(s) %s assigned to %s"
                          % (user_role['roles'], user_role['id']))

    def delete_role(self, rest=None, user_ids=None):
        if not rest:
            rest = RestConnection(self.master)
        if not user_ids:
            user_ids = [user['id'] for user in self.roles]
        RbacBase().remove_user_role(user_ids, rest)
        self.sleep(20, "wait for user to get deleted...")
        self.log.info("user roles revoked for %s" % ", ".join(user_ids))

    def get_user_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
        'password': 'passw0rd'}
        """
        user_list = []
        for user in self.inp_users:
            user_list.append({att: user[att] for att in ('id', 'name', 'password')})
        return user_list

    def get_query_buckets(self, check_all_buckets=False, sample_buckets=None, deferred_bucket=None):
        """
        @summary: This method generate all bucket/scope/collection namespaces for query tests
        @param:
        check_all_buckets: If enabled this will check if all the buckets available in cluster are added to list for
        query buckets/scope/collection namespace
        sample_buckets: Provide the list of name of sample buckets for which queries would be running
        """
        collections_namespace = []
        bucket_list = []

        if self.test_buckets:
            query_bucket = self.test_buckets.split(',')

            for item in query_bucket:
                if ':' in item:
                    _, temp_split = item.split(':')
                    new_list = temp_split.split('.')
                    bucket_list.append(new_list[0])
                else:
                    bucket_list.append(item)
                collections_namespace.append(self.get_collection_name(item))

        if deferred_bucket:
            d_buckets = deferred_bucket.split(',')
            for item in d_buckets:
                if item not in bucket_list:
                    bucket_list.append(item)
                    collections_namespace.append(self.get_collection_name(item))
        if sample_buckets:
            if not isinstance(sample_buckets, list):
                sample_buckets = sample_buckets.split(',')
            for item in sample_buckets:
                if item not in bucket_list:
                    collections_namespace.append(self.get_collection_name("`{0}`".format(item)))
                    bucket_list.append(item)

        if check_all_buckets:
            for bucket in self.buckets:
                if bucket.name not in bucket_list:
                    collections_namespace.append(self.get_collection_name(bucket.name))

        self.log.info("xxxxxx - - - All buckets - - - xxxxxx")
        self.log.info(collections_namespace)
        return collections_namespace

    def get_collection_name(self, bucket_name=None):
        if not bucket_name:
            bucket_name = self.buckets[0]
        collection_list = bucket_name.split(':')
        cl_len = len(collection_list)
        if cl_len == 1:
            return collection_list[0]
        elif cl_len == 2:
            return '{0}:{1}'.format(collection_list[0], collection_list[1])
        elif cl_len == 3:
            return '{0}:{1}.{2}._default'.format(collection_list[0], collection_list[1], collection_list[2])
        elif cl_len == 4:
            return '{0}:{1}.{2}.{3}'.format(collection_list[0], collection_list[1],
                                            collection_list[2], collection_list[3])
        else:
            raise Exception("Invalid bucket name")

    def get_user_role_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
         'roles': 'admin:fts_admin[default]'}
        """
        user_role_list = []
        for user in self.inp_users:
            user_role_list.append({att: user[att] for att in ('id', 'name', 'roles')})
        return user_role_list

    def retrieve_roles(self):
        return self.retrieve_rbac('roles')

    def retrieve_users(self):
        return self.retrieve_rbac('users')

    def retrieve_rbac(self, type):
        if type == 'users':
            url = "settings/rbac/users"
            prepend = " Retrieve User Roles"
        if type == 'roles':
            url = "settings/rbac/roles"
            prepend = " Retrieve all User roles"
        rest = RestConnection(self.master)
        api = rest.baseUrl + url
        status, content, header = rest._http_request(api, 'GET')
        self.log.info("{3} - Status - {0} -- Content - {1} -- Header - {2}".format(status, content, header, prepend))
        return status, content, header

    def grant_role(self, role=None):
        self.log.info(self.roles)
        if not role:
            role = self.roles[0]['roles']
        if self.all_buckets:
            list = []
            for bucket in self.buckets:
                list.append(bucket.name)
            names = ','.join(list)
            self.query = "GRANT {0} on {1} to {2}".format(role, names, self.users[0]['id'])
            actual_result = self.run_cbq_query()
        elif self.udfs:
            roles = role.split(",")
            for role in roles:
                self.query = "GRANT {0} to {1}".format(role, self.users[0]['id'])
                actual_result = self.run_cbq_query()
        elif "," in role:
            roles = role.split(",")
            for role in roles:
                if "(" in role:
                    role1 = role.split("(")[0]
                    name = role.split("(")[1][:-1]
                    self.query = "GRANT {0} on {1} to {2}".format(role1, name, self.users[0]['id'])
                    actual_result = self.run_cbq_query()
                else:
                    self.query = "GRANT {0} to {1}".format(role, self.users[0]['id'])
                    actual_result = self.run_cbq_query()
        elif "(" in role:
            role1 = role.split("(")[0]
            name = role.split("(")[1][:-1]
            self.query = "GRANT {0} on {1} to {2}".format(role1, name, self.users[0]['id'])
            actual_result = self.run_cbq_query()
        else:
            self.query = "GRANT {0} to {1}".format(role, self.users[0]['id'])
            actual_result = self.run_cbq_query()
        msg = "Unable to grant role {0} to {1}".format(role, self.users[0]['id'])
        self.assertTrue(actual_result['status'] == 'success', msg)

    def revoke_role(self, role=None):
        if not role:
            role = self.roles[0]['roles']
            if self.all_buckets:
                role += "(`*`)"
        if "(" in role:
            role1 = role.split("(")[0]
            name = role.split("(")[1][:-1]
            self.query = "REVOKE {0} on {1} FROM {2}".format(role1, name, self.users[0]['id'])
        else:
            self.query = "REVOKE {0} FROM {1}".format(role, self.users[0]['id'])
        actual_result = self.run_cbq_query()
        msg = "Unable to revoke role {0} from {1}".format(role, self.users[0]['id'])
        self.assertTrue(actual_result['status'] == 'success', msg)

    def curl_with_roles(self, query):
        shell = RemoteMachineShellConnection(self.master)
        if '%' in query:
            query = query.replace("%", "%25")
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement={3}'". \
            format(self.users[0]['id'], self.users[0]['password'], self.master.ip, query, self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        new_list = [string.strip() for string in output]
        concat_string = ''.join(new_list)
        json_output = json.loads(concat_string)
        try:
            return json_output
        except ValueError:
            return error

    def system_catalog_helper_select(self, test, role=""):
        temp_name = self.bucket_name
        res = self.curl_with_roles('select * from system:datastores')
        self.assertTrue(res['metrics']['resultCount'] == 1)
        res = self.curl_with_roles('select * from system:namespaces')
        self.assertTrue(res['metrics']['resultCount'] == 1)
        res = self.curl_with_roles('select * from system:keyspaces')

        if role in ["query_update(default)", "query_delete(default)", "query_insert(default)","query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name)]:
            self.assertTrue(res['status'] == 'success')
        elif 'test)' in role and self.load_collections == True:
            self.assertEqual(res['metrics']['resultCount'], 7)
        elif 'test)' in role:
            self.assertEqual(res['metrics']['resultCount'], 2)
        elif role == 'query_select(default:default)' and self.load_collections == True:
            self.assertEqual(res['metrics']['resultCount'], 7)
        elif role == 'query_select(default)' and self.load_collections == True:
            self.assertEqual(res['metrics']['resultCount'], 3)
        elif (role.startswith("query_") or role.startswith("select") or role in ["bucket_full_access(default)",
                                                                                "query_delete(default)"]) and self.load_collections == True:
            self.assertEqual(res['metrics']['resultCount'], 7)
        elif role.startswith("query_") or role.startswith("select") or role in ["bucket_full_access(default)",
                                                                                "query_delete(default)"]:
            self.assertEqual(res['metrics']['resultCount'], 1)
        else:
            self.assertEqual(res['metrics']['resultCount'], 2)

        self.query = 'create primary index if not exists on `{0}`'.format(self.buckets[0].name)
        try:
            self.curl_with_roles(self.query)
        except Exception as ex:
            self.log.error(ex)

        if role not in ["query_insert(default)", "query_update(default)", "query_delete(default)","query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name)]:
            self.query = 'create primary index if not exists on `{0}`'.format(self.buckets[1].name)
            try:
                self.curl_with_roles(self.query)
            except Exception as ex:
                self.log.error(ex)

        if role not in ["views_admin(standard_bucket0)", "views_admin(default)", "query_insert(default)", "query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name),
                        "query_update(default)", "query_delete(default)"]:
            self.query = 'create index idx1 on `{0}`(name)'.format(self.buckets[0].name)
            res = self.curl_with_roles(self.query)
            # self.sleep(10)
            self.query = 'create index idx2 on `{0}`(name)'.format(self.buckets[1].name)
            self.curl_with_roles(self.query)
            # self.sleep(10)
            self.query = 'select * from system:indexes'
            res = self.curl_with_roles(self.query)

        if role in ["admin", "cluster_admin", "bucket_admin"]:
            self.assertTrue(res['metrics']['resultCount'] == 4)
        elif role in ["bucket_admin(default)", "bucket_admin(standard_bucket0)", "query_system_catalog", "ro_admin",
                      "replication_admin"]:
            self.assertTrue(res['status'] == 'success')

        self.query = 'select * from system:dual'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['metrics']['resultCount'] == 1)
        self.query = 'select * from system:user_info'
        res = self.curl_with_roles(self.query)

        if role == "admin":
            self.assertTrue(res['status'] == 'success')
        elif role == "cluster_admin":
            self.assertTrue(str(res).find("'code': 13014") != -1)

        self.query = 'select * from system:nodes'
        res = self.curl_with_roles(self.query)

        if role == "bucket_full_access(default)":
            self.assertTrue(res['status'] == 'stopped')
        elif role in ["select(standard_bucket0)","query_select(standard_bucket0)"]:
            self.assertTrue(str(res).find("'code': 13014") != -1)
        elif role in ["insert(default)", "query_insert(default)", "query_update(default)", "query_delete(default)","insert({0})".format(self.bucket_name), "query_insert({0})".format(self.bucket_name), "query_update({0})".format(self.bucket_name), "query_delete({0})".format(self.bucket_name),"insert(`{0}`)".format(self.bucket_name), "query_insert(`{0}`)".format(self.bucket_name), "query_update(`{0}`)".format(self.bucket_name), "query_delete(`{0}`)".format(self.bucket_name)]:
            self.assertTrue(res['status'] == 'fatal' or res['status'] == 'errors' )
        else:
            self.assertTrue(res['status'] == 'success')

        self.query = 'select * from system:applicable_roles'
        res = self.curl_with_roles(self.query)

        if role == "admin":
            self.assertTrue(res['status'] == 'success')
        elif role == "ro_admin":
            self.assertTrue(str(res).find("'code': 13014") != -1)
        elif role == "cluster_admin" or role == "bucket_admin(default)":
            self.assertTrue(str(res).find("'code': 13014") != -1)

        if role not in ["ro_admin", "replication_admin", "query_insert(default)", "query_delete(default)","query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name),
                        "query_update(default)", "bucket_full_access(default)", "query_system_catalog",
                        "views_admin(default)"]:
            if "`" in self.bucket_name:
                temp_name = self.bucket_name.split("`")[1]
            self.query = "prepare `st1{0}` from select * from {1} union select * from {1} union select * from {1}".format(temp_name,self.bucket_name)
            res = self.curl_with_roles(self.query)
            self.query = 'execute `st1{0}`'.format(temp_name)
            res = self.curl_with_roles(self.query)
            if role in ["bucket_admin(standard_bucket0)", "views_admin(standard_bucket0)", "replication_admin"]:
                self.assertTrue(str(res).find("'code': 4040") != -1)
            elif role == "select(default)" or role == "query_select(default)" or role == "query_select({0})".format(self.bucket_name) or role == "query_select(`{0}`)".format(self.bucket_name):
                self.assertTrue(res['metrics']['resultCount'] == 0)
            else:
                self.assertTrue(res['status'] == 'success')

            if role not in ["query_insert(default)", "query_delete(default)", "query_update(default)","query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name)]:
                self.query = "prepare `st2{0}` from select * from {1} union select * from " \
                             "standard_bucket0 union select * from {1}".format(temp_name, self.bucket_name)
                res = self.curl_with_roles(self.query)

                if role in ["bucket_admin(standard_bucket0)", "views_admin(standard_bucket0)",
                            "views_admin(default)", "views_admin", "bucket_admin(default)", "replication_admin",
                            "query_system_catalog", "select(default)", "query_select(default)","select({0})".format(self.bucket_name), "query_select({0})".format(self.bucket_name),"query_select({0})".format(self.rbac_context),"select(`{0}`)".format(self.bucket_name), "query_select(`{0}`)".format(self.bucket_name)]:
                    self.assertTrue(str(res).find("'code': 13014") != -1)
                else:
                    self.assertTrue(res['metrics']['resultCount'] > 0)

                self.query = 'execute `st2{0}`'.format(temp_name)
                res = self.curl_with_roles(self.query)
                if role in ["bucket_admin(standard_bucket0)", "views_admin(standard_bucket0)", "views_admin(default)",
                            "views_admin", "bucket_admin(default)", "replication_admin", "query_system_catalog",
                            "select(default)", "query_select(default)","select({0})".format(self.bucket_name), "query_select({0})".format(self.bucket_name),"query_select({0})".format(self.rbac_context),"select(`{0}`)".format(self.bucket_name), "query_select(`{0}`)".format(self.bucket_name)]:
                    self.assertTrue(str(res).find("'code': 4040") != -1)
                else:
                    self.assertTrue(res['status'] == 'success')

                self.query = 'select * from system:completed_requests'
                res = self.curl_with_roles(self.query)

                if role == "bucket_admin(standard_bucket0)":
                    self.assertTrue(res['metrics']['resultCount'] > 0)
                else:
                    self.assertTrue(res['status'] == 'success')

        if role not in ["query_insert(default)", "query_delete(default)", "query_update(default)","query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name),
                        "bucket_full_access(default)", "ro_admin"]:
            self.query = 'select * from system:prepareds'
            res = self.curl_with_roles(self.query)

            self.assertTrue(res['status'] == 'success')

            self.query = 'select * from system:active_requests'
            res = self.curl_with_roles(self.query)

            if role == "select(default)" or role == "query_select(default)" or role == "select({0})".format(self.bucket_name) or role == "query_select({0})".format(self.bucket_name) or role == "query_select({0})".format(self.rbac_context) or role == "select(`{0}`)".format(self.bucket_name) or role == "query_select(`{0}`)".format(self.bucket_name):
                self.assertTrue(res['status'] == 'success')
            else:
                self.assertTrue(res['metrics']['resultCount'] > 0)

            self.query = 'drop index {0}.idx1'.format(self.buckets[0].name)
            res = self.curl_with_roles(self.query)
            self.query = 'drop index {0}.idx2'.format(self.buckets[1].name)
            res = self.curl_with_roles(self.query)
            self.query = 'select * from system:indexes'
            res = self.curl_with_roles(self.query)

        if role == "views_admin(default)":
            self.assertTrue(res['status'] == 'success')
        elif role == 'query_select(default:default)' and self.load_collections == True:
            self.assertEqual(res['metrics']['resultCount'], 5)
        elif role == 'query_select(default)' and self.load_collections == True:
            self.assertEqual(res['metrics']['resultCount'], 3)
        elif role in ["query_select({0})".format(self.rbac_context)] and "test1" in self.bucket_name:
            self.assertTrue(res['metrics']['resultCount'] == 5)
        elif role in ["query_select({0})".format(self.rbac_context)]:
            self.assertTrue(res['metrics']['resultCount'] == 2)
        elif role in ["bucket_admin(standard_bucket0)", "bucket_admin(default)", "select(default)",
                      "query_select(default)", "query_select({0})".format(self.bucket_name), "query_select({0})".format(self.rbac_context),"select({0})".format(self.bucket_name), "query_select(`{0}`)".format(self.bucket_name),"select(`{0}`)".format(self.bucket_name)]:
            self.assertTrue(res['metrics']['resultCount'] == 1)
        elif role in ["query_insert(default)", "query_delete(default)", "query_update(default)","query_insert({0})".format(self.bucket_name),"query_update({0})".format(self.bucket_name),"query_delete({0})".format(self.bucket_name),"query_insert(`{0}`)".format(self.bucket_name),"query_update(`{0}`)".format(self.bucket_name),"query_delete(`{0}`)".format(self.bucket_name)]:
            self.assertTrue(res['metrics']['resultCount'] == 0)

    def try_query_assert(self, query, find_string):
        try:
            self.curl_with_roles(query)
        except Exception as ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find(find_string) != -1)

    def system_catalog_helper_insert(self, test, role=""):
        self.try_query_assert('insert into system:datastores values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:namespaces values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:keyspaces values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:indexes values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:dual values("k051", { "id":123  } )',
                              "System datastore error Mutations not allowed on system:dual.")
        self.try_query_assert('insert into system:user_info values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:nodes values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:applicable_roles values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:prepareds values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:completed_requests values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")
        self.try_query_assert('insert into system:active_requests values("k051", { "id":123  } )',
                              "System datastore :  Not implemented ")

    def system_catalog_helper_update(self, test, role=""):
        self.try_query_assert('update system:datastores use keys "%s" set name="%s"' % ("id", "test"), "'code': 11000")
        self.try_query_assert('update system:namespaces use keys "%s" set name="%s"' % ("id", "test"), "'code': 11003")
        self.try_query_assert('update system:keyspaces use keys "%s" set name="%s"' % ("id", "test"), "'code': 11003")
        self.try_query_assert('update system:indexes use keys "%s" set name="%s"' % ("id", "test"), "'code': 11003")
        self.try_query_assert('update system:dual use keys "%s" set name="%s"' % ("id", "test"), "'code': 11003")
        self.try_query_assert('update system:user_info use keys "%s" set name="%s"' % ("id", "test"), "'code': 5200")
        self.try_query_assert('update system:nodes use keys "%s" set name="%s"' % ("id", "test"), "'code': 11003}")
        # panic seen here as of now,hence commenting it out for now.
        self.try_query_assert('update system:applicable_roles use keys "%s" set name="%s"' % ("id", "test"),
                              "'code': 11000")
        self.try_query_assert('update system:active_requests use keys "%s" set name="%s"' % ("id", "test"),
                              "'code': 11000")
        self.try_query_assert('update system:completed_requests use keys "%s" set name="%s"' % ("id", "test"),
                              "'code': 11000")
        self.try_query_assert('update system:prepareds use keys "%s" set name="%s"' % ("id", "test"), "'code': 11000")

    # Query does not support drop these tables or buckets yet.We can add the test once it
    #  is supported.
    # Right now we cannot compare results in assert.
    # def system_catalog_helper_drop(self,query_params_with_roles,test = ""):
    #     self.query = 'drop system:datastores'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:namespaces'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:keyspaces'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:indexes'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:dual'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:user_info'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:nodes'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:applicable_roles'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:prepareds'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:completed_requests'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:active_requests'
    #     res = self.run_cbq_query()
    #     print res

    def query_with_roles(self, query, find_string):
        self.query = query
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find(find_string) != -1)

    def system_catalog_helper_delete(self, test, role="admin"):
        self.query_with_roles('delete from system:datastores', "'code': 11004")
        self.query_with_roles('delete from system:namespaces', "'code': 11004")
        # To be fixed in next version
        # self.query_with_roles('delete from system:keyspaces', "'code': 11003")
        self.query_with_roles('delete from system:indexes', "'code': 11004")
        self.query_with_roles('delete from system:dual', "'code': 11004")

        self.query_with_roles('delete from system:user_info', "'code': 11004")
        self.query_with_roles('delete from system:nodes', "'code': 11004")
        self.query_with_roles('delete from system:applicable_roles', "'code': 11004")
        self.query = 'delete from system:completed_requests'
        res = self.curl_with_roles(self.query)
        role_list = ["query_delete(default)", "query_delete(standard_bucket0)", "delete(default)",
                     "bucket_full_access(default)", "query_delete({0})".format(self.bucket_name),"query_delete({0})".format(self.rbac_context)]
        self.assertNotEqual(res['status'], 'success') if role in role_list else self.assertTrue(
            res['status'] == 'success')
        try:
            self.query = 'delete from system:active_requests'
            res = self.curl_with_roles(self.query)
            self.assertTrue(res['status'] == 'stopped')
        except:
            self.assertTrue(res['status'] == 'fatal' or res['status'] == 'errors')
        if role not in role_list:
            self.query = 'delete from system:prepareds'
            res = self.curl_with_roles(self.query)
            self.assertTrue(res['status'] == 'success')

    def select_my_user_info(self):
        self.query = 'select * from system:my_user_info'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['status'] == 'success')

    ##############################################################################################
    #
    #  tuq_curl.py and tuq_curl_whitelist.py helpers
    #
    ##############################################################################################

    '''Convert output of remote_util.execute_commands_inside to json'''

    def convert_to_json(self, output_curl):
        try:
            new_curl = "{" + output_curl
            if "\x04" in new_curl:
                new_curl = new_curl.replace("\x04", "")
            json_curl = json.loads(new_curl)
            return json_curl
        except Exception as ex:
            self.log.info(output_curl)

    '''Convert output of remote_util.execute_command to json
       (stripping all white space to match execute_command_inside output)'''

    def convert_list_to_json(self, output_of_curl):
        try:
            new_list = [string.replace(" ", "") for string in output_of_curl]
            concat_string = ''.join(new_list)
            json_output = json.loads(concat_string)
            return json_output
        except Exception as ex:
            self.log.info(output_of_curl)

    '''Convert output of remote_util.execute_command to json to match the output of run_cbq_query'''

    def convert_list_to_json_with_spacing(self, output_of_curl):
        try:
            new_list = [string.strip() for string in output_of_curl]
            concat_string = ''.join(new_list)
            json_output = json.loads(concat_string)
            return json_output
        except Exception as ex:
            self.log.info(output_of_curl)

    ##############################################################################################
    #
    #  tuq_ascdesc.py helper
    #
    ##############################################################################################
    def compare(self, test, query, expected_result_list, alias='_default'):
        actual_result_list = []
        actual_result = self.run_cbq_query(query)
        for i in range(0, 5):
            if test in ["test_asc_desc_composite_index", "test_meta", "test_asc_desc_array_index"]:
                actual_result_list.append(actual_result['results'][i][alias]['_id'])
            elif test in ["test_desc_isReverse_ascOrder"]:
                actual_result_list.append(actual_result['results'][i]['id'])
        if test != "do_not_test_against_hardcode":
            self.assertEqual(actual_result_list, expected_result_list)

        query = query.replace("from default:default._default._default as d",
                              "from default:default._default._default as d use index(`#primary`)")
        expected_result = self.run_cbq_query(query)
        self.assertEqual(actual_result['results'], expected_result['results'])

    ##############################################################################################
    #
    #  tuq_advancedcbqshell.py helpers
    #
    ##############################################################################################
    def execute_commands_inside(self, main_command, query, queries, bucket1, password, bucket2, source,
                                subcommands=[], min_output_size=0,
                                end_msg='', timeout=250):
        shell = RemoteMachineShellConnection(self.master)
        shell.extract_remote_info()
        filename = "/tmp/test2"
        iswin = False

        if shell.info.type.lower() == 'windows':
            iswin = True
            filename = "/cygdrive/c/tmp/test.txt"

        filedata = ""
        if not (query == ""):
            main_command = main_command + " -s=\"" + query + '"'
        elif (shell.remote and not (queries == "")):
            sftp = shell._ssh_client.open_sftp()
            filein = sftp.open(filename, 'w')
            for query in queries:
                filein.write(query)
                filein.write('\n')
            filein.close()
            fileout = sftp.open(filename, 'r')
            filedata = fileout.read()
            fileout.close()
        elif not (queries == ""):
            f = open(filename, 'w')
            for query in queries:
                f.write(query)
                f.write('\n')
            f.close()
            fileout = open(filename, 'r')
            filedata = fileout.read()
            fileout.close()

        # newdata = filedata.replace("bucketname", bucket2)
        newdata = filedata
        try:
            newdata = newdata.decode().replace("bucketname", bucket2)
        except AttributeError:
            pass
        newdata = newdata.replace("user", bucket1)
        newdata = newdata.replace("pass", password)
        newdata = newdata.replace("bucket1", bucket1)

        newdata = newdata.replace("user1", bucket1)
        newdata = newdata.replace("pass1", password)
        newdata = newdata.replace("bucket2", bucket2)
        newdata = newdata.replace("user2", bucket2)
        newdata = newdata.replace("pass2", password)

        if (shell.remote and not (queries == "")):
            f = sftp.open(filename, 'w')
            f.write(newdata)
            f.close()
        elif not (queries == ""):
            f = open(filename, 'w')
            f.write(newdata)
            f.close()
        if not (queries == ""):
            if (source):
                if iswin:
                    main_command = main_command + "  -s=\"\SOURCE " + 'c:\\\\tmp\\\\test.txt'
                else:
                    main_command = main_command + "  -s=\"\SOURCE " + filename + '"'
            else:
                if iswin:
                    main_command = main_command + " -f=" + 'c:\\\\tmp\\\\test.txt'
                else:
                    main_command = main_command + " -f=" + filename

        safe_cmd = main_command
        if self.password:
            safe_cmd = safe_cmd.replace("-p {0}".format(self.password), "-p *")
        self.log.info("running command on {0}: {1}".format(self.master.ip, safe_cmd))
        output = ""
        if shell.remote:
            stdin, stdout, stderro = shell._ssh_client.exec_command(main_command)
            # time.sleep(20)
            count = 0
            for line in stdout.readlines():
                if (count >= 0):
                    output += line.strip()
                    output = output.strip()
                    if "Inputwasnotastatement" in output:
                        output = "status:FAIL"
                        break
                    if "timeout" in output:
                        output = "status:timeout"
                else:
                    count += 1
            stdin.close()
            stdout.close()
            stderro.close()
        else:
            p = Popen(main_command, shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderro = p.communicate()
            output = stdout
            print(output)
            # time.sleep(1)
        if (shell.remote and not (queries == "")):
            sftp.remove(filename)
            sftp.close()
        elif not (queries == ""):
            os.remove(filename)

        return (output)

    ##############################################################################################
    #
    #  date_time_functions.py helpers
    #   These are very specific to this testing, should probably go back
    ##############################################################################################

    def _generate_date_part_millis_query(self, expression, part, timezone=None):
        if not timezone:
            query = 'SELECT DATE_PART_MILLIS({0}, "{1}")'.format(expression, part)
        else:
            query = 'SELECT DATE_PART_MILLIS({0}, "{1}", "{2}")'.format(expression, part, timezone)
        return query

    def _generate_date_format_str_query(self, expression, format):
        query = 'SELECT DATE_FORMAT_STR("{0}", "{1}")'.format(expression, format)
        return query

    def _generate_date_range_str_query(self, initial_date, final_date, part, increment=None):
        if increment is None:
            query = 'SELECT DATE_RANGE_STR("{0}", "{1}", "{2}")'.format(initial_date, final_date, part)
        else:
            query = 'SELECT DATE_RANGE_STR("{0}", "{1}", "{2}", {3})'.format(initial_date, final_date, part, increment)
        return query

    def _generate_date_range_millis_query(self, initial_millis, final_millis, part, increment=None):
        if increment is None:
            query = 'SELECT DATE_RANGE_MILLIS({0}, {1}, "{2}")'.format(initial_millis, final_millis, part)
        else:
            query = 'SELECT DATE_RANGE_MILLIS({0}, {1}, "{2}", {3})'.format(initial_millis, final_millis, part,
                                                                            increment)
        return query

    def _convert_to_millis(self, expression):
        query = 'SELECT STR_TO_MILLIS("{0}")'.format(expression)
        results = self.run_cbq_query(query)
        return results["results"][0]["$1"]

    def _is_date_part_present(self, expression):
        return (len(expression.split("-")) > 1)

    def _is_time_part_present(self, expression):
        return (len(expression.split(":")) > 1)

    ##############################################################################################
    #
    #  n1ql_options.py helpers
    #
    ##############################################################################################
    def curl_helper(self, statement):
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement={3}'". \
            format('Administrator', 'password', self.master.ip, statement, self.curl_path)
        return self.run_helper_cmd(cmd)

    def prepare_helper(self, statement):
        cmd = '{4} -u {0}:{1} http://{2}:8093/query/service -d \'prepared="{3}"&$type="Engineer"&$name="employee-4"\''. \
            format('Administrator', 'password', self.master.ip, statement, self.curl_path)
        return self.run_helper_cmd(cmd)

    def prepare_helper2(self, statement):
        cmd = '{4} -u {0}:{1} http://{2}:8093/query/service -d \'prepared="{3}"&args=["Engineer","employee-4"]\''. \
            format('Administrator', 'password', self.master.ip, statement, self.curl_path)
        return self.run_helper_cmd(cmd)

    def run_helper_cmd(self, cmd):
        shell = RemoteMachineShellConnection(self.master)
        output, error = shell.execute_command(cmd)
        new_list = [string.strip() for string in output]
        concat_string = ''.join(new_list)
        json_output = json.loads(concat_string)
        return json_output

    ##############################################################################################
    #
    #  n1ql_ro_user.py helpers
    #
    ##############################################################################################
    def _kill_all_processes_cbq(self):
        if hasattr(self, 'shell'):
            if self.input.tuq_client and "client" in self.input.tuq_client:
                self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
            else:
                self.shell = RemoteMachineShellConnection(self.master)
            o = self.shell.execute_command("ps -aef| grep cbq-engine")
            if len(o):
                for cbq_engine in o[0]:
                    if cbq_engine.find('grep') == -1:
                        pid = [item for item in cbq_engine.split(' ') if item][1]
                        self.shell.execute_command("kill -9 %s" % pid)

    ##############################################################################################
    #
    #   tuq_views_ops.py helpers
    ##############################################################################################

    def _compare_view_and_tool_result(self, view_result, tool_result, check_values=True):
        self.log.info("Comparing result ...")
        formated_tool_res = [{"key": [doc["join_yr"], doc["join_mo"]], "value": doc["name"]} for doc in tool_result]
        formated_view_res = [{"key": row["key"], "value": row["value"]} for row in view_result]
        msg = "Query results are not equal. Tool %s, view %s" % (len(formated_tool_res), len(formated_view_res))
        self.assertEqual(len(formated_tool_res), len(formated_view_res), msg)
        self.log.info("Length is equal")
        msg = "Query results sorting are not equal./n Actual %s, Expected %s" % (
            formated_tool_res[:100], formated_view_res[:100])
        self.assertEqual([row["key"] for row in formated_tool_res], [row["key"] for row in formated_view_res], msg)
        self.log.info("Sorting is equal")
        if check_values:
            formated_tool_res = sorted(formated_tool_res, key=lambda doc: (doc['key'], doc['value']))
            formated_view_res = sorted(formated_view_res, key=lambda doc: (doc['key'], doc['value']))
            msg = "Query results sorting are not equal. View but not tool has [%s]" % (
                [r for r in view_result if r in formated_tool_res])
            self.assertTrue(formated_tool_res == formated_view_res, msg)
            self.log.info("Items are equal")

    ##############################################################################################
    #
    #   tuq_tutorial.py helpers
    ##############################################################################################

    def _create_headers(self):
        authorization = ""
        return {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def _http_request(self, api, method='GET', params='', headers=None, timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError as e:
                        json_parsed = {"error": "status: {0}, content: {1}".format(response['status'], content)}
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    self.log.error(
                        '{0} error {1} reason: {2} {3}'.format(api, response['status'], reason, content.rstrip('\n')))
                    return False, content, response
            except socket.error as e:
                self.log.error("socket error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise Exception("nothing")
            # time.sleep(3)
    ##############################################################################################
    #
    #   tuq_subquery.py helpers
    ##############################################################################################

    def get_num_requests(self,index,num_nodes):
        num_requests = 0
        for i in range(num_nodes):
            cmd = f'curl -u {self.username}:{self.password} http://{self.servers[i].ip}:9102/api/v1/stats?skipEmpty=true'
            o = self.shell.execute_command(cmd)
            new_curl = json.dumps(o)
            formatted = json.loads(new_curl)
            actual_results = json.loads(formatted[0][0])
            if index in actual_results.keys():
                num_requests = actual_results[index]['num_requests'] + num_requests
        return num_requests

    ##############################################################################################
    #
    #   tuq_join.py helpers
    ##############################################################################################

    def _get_for_sort(self, doc):
        if not 'emp' in doc:
            return ''
        if 'name' in doc['emp']:
            return doc['emp']['name'], doc['emp']['join_yr'], \
                   doc['emp']['join_mo'], doc['emp']['job_title']
        else:
            return doc['emp']['task_name']

    def _delete_ids(self, result):
        for item in result:
            if 'emp' in item:
                del item['emp']['_id']
            else:
                None
            if 'tasks' in item:
                for task in item['tasks']:
                    if task and '_id' in task:
                        del task['_id']
                    else:
                        None

    def _generate_full_joined_docs_list(self, join_type=JOIN_INNER, particular_key=None):
        joined_list = []
        all_docs_list = self.generate_full_docs_list(self.gens_load)
        if join_type.upper() == JOIN_INNER:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys = [item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                for tasks_item in tasks_items:
                    item_to_add = copy.deepcopy(item)
                    item_to_add.update(tasks_item)
                    joined_list.append(item_to_add)
        elif join_type.upper() == JOIN_LEFT:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys = [item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                for key in keys:
                    item_to_add = copy.deepcopy(item)
                    if key in [doc["_id"] for doc in tasks_items]:
                        item_to_add.update([doc for doc in tasks_items if key == doc['_id']][0])
                        joined_list.append(item_to_add)
            joined_list.extend([{}] * self.gens_tasks[-1].end)
        elif join_type.upper() == JOIN_RIGHT:
            raise Exception("RIGHT JOIN doen't exists in current implementation")
        else:
            raise Exception("Unknown type of join")
        return joined_list

    def _generate_full_nested_docs_list(self, join_type=JOIN_INNER, particular_key=None):
        nested_list = []
        all_docs_list = self.generate_full_docs_list(self.gens_load)
        if join_type.upper() == JOIN_INNER:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys = [item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                if tasks_items:
                    nested_list.append({"items_nested": tasks_items, "item": item})
        elif join_type.upper() == JOIN_LEFT:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys = [item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                if tasks_items:
                    nested_list.append({"items_nested": tasks_items, "item": item})
            tasks_doc_list = self.generate_full_docs_list(self.gens_tasks)
            for item in tasks_doc_list:
                nested_list.append({"item": item})
        elif join_type.upper() == JOIN_RIGHT:
            raise Exception("RIGHT JOIN doen't exists in corrunt implementation")
        else:
            raise Exception("Unknown type of join")
        return nested_list

    ##############################################################################################
    #
    #   tuq_index.py helpers
    ##############################################################################################

    def run_intersect_scan_query(self, query_method):
        indexes = []
        query = None
        index_name_prefix = "inter_index_" + str(uuid.uuid4())[:4]
        index_fields = self.input.param("index_field", '').split(';')
        try:
            for bucket in self.buckets:
                for field in index_fields:
                    index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                    query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                        index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                    self.run_cbq_query(query=query)
                    self._wait_for_index_online(bucket.name, index_name)
                    indexes.append(index_name)

                fn = getattr(self, query_method)
                query = fn()
        except Exception as ex:
            self.log.info(ex)
        finally:
            return indexes, query

    def run_intersect_scan_explain_query(self, indexes_names, query_temp):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            if query_temp.find('%s') > 0:
                query_temp = query_temp % bucket.name
            self.log.info('-' * 100)
            query = 'EXPLAIN %s' % query_temp
            if query.find("CREATE INDEX") < 0:
                res = self.run_cbq_query(query=query)
                plan = self.ExplainPlanHelper(res)
                self.log.info(f"plan is: {plan}")
                result = plan["~children"][0]["~children"][0] if "~children" in plan["~children"][0] else \
                    plan["~children"][0]
                if result['#operator'] == 'IndexScan3':
                    self.assertTrue('inter_index' in result['index'])
                elif result['scans'][0]['#operator'] != 'DistinctScan':
                    if result["#operator"] != 'UnionScan':
                        if "ORDER BY" in query:
                            self.assertTrue(result["#operator"] == 'OrderedIntersectScan',
                                            "Index should be orderedintersect scan and is %s" % (plan))
                        else:
                            self.assertTrue(result["#operator"] == 'IntersectScan',
                                            "Index should be intersect scan and is %s" % (plan))
                    if result["#operator"] == 'UnionScan':
                        actual_indexes = [
                            scan['index'] if scan['#operator'] == 'IndexScan' else scan['scan']['index'] if scan[
                                                                                                                '#operator'] == 'DistinctScan' else
                            scan['index']
                            for results in result['scans'] for scan in results['scans']]
                    else:
                        actual_indexes = [
                            scan['index'] if scan['#operator'] == 'IndexScan' else scan['scan']['index'] if scan[
                                                                                                                '#operator'] == 'DistinctScan' else
                            scan['index']
                            for scan in result['scans']]
                    actual_indexes = [x.encode('UTF8') for x in actual_indexes]
                    self.log.info('actual indexes "{0}"'.format(actual_indexes))
                    self.log.info('compared against "{0}"'.format(indexes_names))
                    # self.assertTrue(set(actual_indexes) == set(indexes_names), "Indexes should be %s, but are: %s" % (indexes_names, actual_indexes))
                    diffs = DeepDiff(set(actual_indexes), set(indexes_names), ignore_order=True,
                                     ignore_string_type_changes=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            else:
                result = ""
            self.log.info('-' * 100)

    def _delete_indexes(self, indexes):
        count = 0
        for bucket in self.buckets:
            query = "DROP INDEX %s.%s USING %s" % (bucket.name, indexes[count], self.index_type)
            count = count + 1
            try:
                self.run_cbq_query(query=query)
            except:
                pass

    def _verify_view_is_present(self, view_name, bucket):
        if self.primary_indx_type.lower() == 'gsi':
            return
        ddoc, _ = RestConnection(self.master).get_ddoc(bucket.name, "ddl_%s" % view_name)
        self.assertTrue(view_name in ddoc["views"], "View %s wasn't created" % view_name)

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM system:indexes"
        res = self.run_cbq_query(query)
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                self.log.error(item)
                continue
            if item['indexes']['keyspace_id'] == bucket.name and item['indexes']['name'] == index_name:
                return True
        return False

    ##############################################################################################
    #
    #   tuq_dml.py helpers
    ##############################################################################################

    def _insert_gen_keys(self, num_docs, prefix='a1_'):
        def convert(data):
            if isinstance(data, str):
                return str(data)
            elif isinstance(data, collections.abc.Mapping):
                return dict(list(map(convert, iter(data.items()))))
            elif isinstance(data, collections.abc.Iterable):
                return type(data)(list(map(convert, data)))
            else:
                return data

        keys = []
        values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                break
            for i in range(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = next(gen)
                key = prefix + key
                value = convert(json.loads(value))
                for query_bucket in self.query_buckets:
                    self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (query_bucket, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(value)
        return keys, values

    def _keys_are_deleted(self, keys):
        TIMEOUT_DELETED = 300
        end_time = time.time() + TIMEOUT_DELETED
        # Checks for keys deleted ...
        while time.time() < end_time:
            for bucket in self.buckets:
                self.query = 'select meta(%s).id from %s' % (bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                found = False
                for key in keys:
                    if actual_result['results'].count({'id': key}) != 0:
                        found = True
                        break
                if not found:
                    return
            self.sleep(3)
        self.fail('Keys %s are still present' % keys)

    def _common_insert(self, keys, values):
        for bucket in self.buckets:
            for i in range(len(keys)):
                v = '"%s"' % values[i] if isinstance(values[i], str) else values[i]
                self.query = 'INSERT into %s (key , value) VALUES ("%s", "%s")' % (bucket.name, keys[i], values[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

    def _common_check(self, key, expected_item_value, upsert=False):
        clause = 'UPSERT' if upsert else 'INSERT'
        for bucket in self.buckets:
            inserted = expected_item_value
            if isinstance(expected_item_value, str):
                inserted = '"%s"' % expected_item_value
            if expected_item_value is None:
                inserted = 'null'
            self.query = '%s into %s (key , value) VALUES ("%s", %s)' % (clause, bucket.name, key, inserted)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select * from %s use keys ["%s"]' % (bucket.name, key)
            try:
                actual_result = self.run_cbq_query()
            except:
                pass
            self.sleep(15, 'Wait for index rebuild')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'].count({bucket.name: expected_item_value}), 1,
                             'Item did not appear')

    def _common_check_values(self, keys, expected_item_values, upsert=False):
        clause = 'UPSERT' if upsert else 'INSERT'
        for bucket in self.buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = '%s into %s (key , value) VALUES %s' % (clause, bucket.name, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select * from %s use keys ["%s"]' % (bucket.name, '","'.join(keys))
            try:
                self.run_cbq_query()
                self._wait_for_index_online(bucket.name, '#primary')
            except:
                pass
            self.sleep(15, 'Wait for index rebuild')
            actual_result = self.run_cbq_query()
            for value in expected_item_values:
                self.assertEqual(actual_result['results'].count({bucket.name: value}),
                                 expected_item_values.count(value),
                                 'Item did not appear')

    def load_with_dir(self, generators_load, exp=0, flag=0,
                      kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
                      timeout_secs=30, op_type='create', start_items=0):
        gens_load = {}
        for bucket in self.buckets:
            tmp_gen = []
            for generator_load in generators_load:
                tmp_gen.append(copy.deepcopy(generator_load))
            gens_load[bucket] = copy.deepcopy(tmp_gen)
        items = 0
        for gen_load in gens_load[self.buckets[0]]:
            items += (gen_load.end - gen_load.start)
        shell = RemoteMachineShellConnection(self.master)
        try:
            for bucket in self.buckets:
                self.log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
                self.log.info("Delete directory's content %s/data/default/%s ..." % (self.directory, bucket.name))
                shell.execute_command('rm -rf %s/data/default/*' % self.directory)
                self.log.info("Create directory %s/data/default/%s..." % (self.directory, bucket.name))
                shell.execute_command('mkdir -p %s/data/default/%s' % (self.directory, bucket.name))
                self.log.info("Load %s documents to %s/data/default/%s..." % (items, self.directory, bucket.name))
                for gen_load in gens_load:
                    for i in range(gen_load.end):
                        key, value = next(gen_load)
                        out = shell.execute_command("echo '%s' > %s/data/default/%s/%s.json" % (value, self.directory,
                                                                                                bucket.name, key))
                self.log.info("LOAD IS FINISHED")
        finally:
            shell.disconnect()
        self.num_items = items + start_items
        self.log.info("LOAD IS FINISHED")

    def _delete_ids(self, result):
        for item in result:
            if '_id' in item:
                del item['_id']
            else:
                None
            for bucket in self.buckets:
                if bucket.name in item and 'id' in item[bucket.name]:
                    del item[bucket.name]['_id']

    ##############################################################################################
    #
    #   tuq_concurrent.py helpers
    ##############################################################################################

    def query_thread(self, method_name):
        try:
            fn = getattr(self, method_name)
            fn()
        except Exception as ex:
            self.log.error(
                "***************ERROR*************\n At least one of query threads is crashed: {0}".format(ex))
            self.thread_crashed.set()
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()

    ##############################################################################################
    #
    #   tuq_cluster_ops.py helpers
    ##############################################################################################

    def _create_multiple_indexes(self, index_field):
        indexes = []
        self.assertTrue(self.buckets, 'There are no buckets! check your parameters for run')
        for bucket, query_bucket in zip(self.buckets, self.query_buckets):
            index_name = 'idx_%s_%s_%s' % (bucket.name, index_field, str(uuid.uuid4())[:4])
            query = "CREATE INDEX %s ON %s(%s) USING %s" % (index_name, query_bucket, ','.join(index_field.split(';')),
                                                            self.indx_type)
            self.run_cbq_query(query=query)

            if self.indx_type.lower() == 'gsi':
                self._wait_for_index_online(bucket.name, index_name)
        indexes.append(index_name)
        return indexes

    def _delete_multiple_indexes(self, indexes):
        for query_bucket in self.query_buckets:
            for index_name in set(indexes):
                try:
                    self.run_cbq_query(query="DROP INDEX %s ON %s" % (index_name, query_bucket))
                except:
                    pass

    ##############################################################################################
    #
    #   tuq_base64.py helpers
    ##############################################################################################

    def _generate_full_docs_list_base64(self, gens_load):
        all_docs_list = []
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                _, val = next(doc_gen)
                all_docs_list.append(val)
        return all_docs_list

    def _verify_results_base64(self, actual_result, expected_result):
        msg = "Results are incorrect. Actual num %s. Expected num: %s.\n" % (len(actual_result), len(expected_result))
        self.assertEqual(len(actual_result), len(expected_result), msg)
        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s Expected first and last 100: %s." \
              "\n  ... \n %s" % (
                  actual_result[:100], actual_result[-100:], expected_result[:100], expected_result[-100:])
        diffs = DeepDiff(actual_result, expected_result, ignore_order=True, ignore_string_type_changes=True)
        if diffs:
            self.assertTrue(False, diffs)

    ##############################################################################################
    #
    #   tuq_index.py helpers,   these are called in tuq_index.py, not standalone tests
    ##############################################################################################

    def run_test_case(self):
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN 'winter'" + \
                         " WHEN join_mo < 6 AND join_mo > 2 THEN 'spring' " + \
                         "WHEN join_mo < 9 AND join_mo > 5 THEN 'summer' " + \
                         "ELSE 'autumn' END AS period FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name'],
                doc['period']))

            expected_result = [{"name": doc['name'],
                                "period": ((('autumn', 'summer')[doc['join_mo'] in [6, 7, 8]],
                                            'spring')[doc['join_mo'] in [3, 4, 5]], 'winter')
                                [doc['join_mo'] in [12, 1, 2]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def run_test_group_by_aggr_fn(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % (bucket.name) + \
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " + \
                         "HAVING COUNT(tasks_points.task1) > 0 AND " + \
                         "(MIN(join_day)=1 OR MAX(join_yr=2011)) " + \
                         "ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()

            if self.analytics:
                self.query = "SELECT d.tasks_points.task1 AS task from %s d " % (bucket.name) + \
                             "WHERE d.join_mo>7 GROUP BY d.tasks_points.task1 " + \
                             "HAVING COUNT(d.tasks_points.task1) > 0 AND " + \
                             "(MIN(d.join_day)=1 OR MAX(d.join_yr=2011)) " + \
                             "ORDER BY d.tasks_points.task1"

            tmp_groups = {doc['tasks_points']["task1"] for doc in self.full_list}
            expected_result = [{"task": group} for group in tmp_groups
                               if [doc['tasks_points']["task1"]
                                   for doc in self.full_list].count(group) > 0 and \
                               (min([doc["join_day"] for doc in self.full_list
                                     if doc['tasks_points']["task1"] == group]) == 1 or \
                                max([doc["join_yr"] for doc in self.full_list
                                     if doc['tasks_points']["task1"] == group]) == 2011)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    ##############################################################################################
    #
    #   tuq_xdcr.py helpers
    #
    ##############################################################################################

    def run_common_body(self, index_list, queries_to_run):
        try:
            max_retry=5
            for idx in index_list:
                for retry in range(1,max_retry+1):
                    try:
                        self.run_cbq_query(query=idx[0])
                        break
                    except CBQError as ex:
                        if retry == max_retry:
                            self.log.error(f"Failed to create index: {ex}")
                        self.log.info(f"Attempt {retry} of {max_retry} failed. Trying again.")
                        self.sleep(2)

            for query in queries_to_run:
                query_results = self.run_cbq_query(query=query[0])
                self.assertEqual(query_results['metrics']['resultCount'], query[1])
        finally:
            for idx in index_list:
                drop_query = "DROP INDEX %s.%s" % (idx[1][0], idx[1][1])
                self.run_cbq_query(query=drop_query)

    ##############################################################################################
    #
    #   tuq_cluster_ops helpers
    #
    ##############################################################################################

    def run_queries_until_timeout(self, timeout=300):
        self.log.info("Running queries for %s seconds to ensure no issues" % timeout)
        init_time = time.time()
        check = False
        next_time = init_time
        query_bucket = self.get_collection_name(self.default_bucket_name)
        while not check:
            try:
                self.run_cbq_query("select * from {0} limit 10000".format(query_bucket))
                time.sleep(2)
                self.log.info("Query Succeeded")
                check = next_time - init_time > timeout
                next_time = time.time()
                self.fail = False
            except Exception as e:
                self.log.error("Query Failed")
                self.log.error(str(e))
                time.sleep(2)
                check = next_time - init_time > timeout
                if next_time - init_time > timeout:
                    self.log.error("Queries are failing after the interval, queries should have recovered by now!")
                    self.fail = True
                next_time = time.time()

        return

    ##############################################################################################
    #
    #   tuq_cbo_statistics helpers
    #
    ##############################################################################################

    def collect_stats(self, bucket):
        # Process index in default bucket
        query_idx = f'select raw name from system:indexes where state = "online" and keyspace_id = "{bucket}"'
        update_stats = f'update statistics for `{bucket}` INDEX(({query_idx}))'
        try:
            self.run_cbq_query(query=update_stats, server=self.master)
        except Exception as e:
            self.log.error("Update statistics error: {0}".format(e))

        # Process index in bucket scopes and collections
        query_ksp = f'select `scope`, name from system:keyspaces where `bucket` = "{bucket}"'
        keyspaces = self.run_cbq_query(query=query_ksp, server=self.master)
        for keyspace in keyspaces['results']:
            scope = keyspace['scope']
            collection = keyspace['name']
            query_idx = f'select raw name from system:indexes where state = "online" and bucket_id = "{bucket}" and scope_id = "{scope}" and keyspace_id = "{collection}"'
            update_stats = f'update statistics for `{bucket}`.{scope}.{collection} INDEX(({query_idx}))'
            try:
                self.run_cbq_query(query=update_stats, server=self.master)
            except Exception as e:
                self.log.error("Update statistics error: {0}".format(e))

    def delete_stats(self, bucket):
        # Process default bucket
        delete_stats = f'update statistics for `{bucket}` delete all'
        try:
            self.run_cbq_query(query=delete_stats, server=self.master)
        except Exception as e:
            self.log.error("Update statistics error: {0}".format(e))

        # Process bucket scopes and collections
        query_ksp = f'select `scope`, name from system:keyspaces where `bucket` = "{bucket}"'
        keyspaces = self.run_cbq_query(query=query_ksp, server=self.master)
        for keyspace in keyspaces['results']:
            scope = keyspace['scope']
            collection = keyspace['name']
            delete_stats = f'update statistics for `{bucket}`.{scope}.{collection} delete all'
            try:
                self.run_cbq_query(query=delete_stats, server=self.master)
            except Exception as e:
                self.log.error("Update statistics error: {0}".format(e))

    ##############################################################################################
    #
    #   UDF helpers
    #
    ##############################################################################################

    '''Create a library with functions, check to see that the library was created and the functions were created'''
    def create_library(self, library_name='', functions={}, function_names=[], replace= False, filename=None, error=False):
        created = False
        protocol = "http"
        if self.use_https:
            self.n1ql_port = CbServer.ssl_n1ql_port
            protocol = "https"
        url = f"{protocol}://{self.master.ip}:{self.n1ql_port}/evaluator/v1/libraries/{library_name}"
        self.shell.execute_command(f"{self.curl_path} -s -k -X DELETE {url} -u Administrator:password")
        data = f'{functions}'
        if filename:
            results = self.shell.execute_command(f"{self.curl_path} -s -k -X POST {url} -u Administrator:password -H 'content-type: application/json' -d @{filename}")
        else:
            results = self.shell.execute_command(f"{self.curl_path} -s -k -X POST {url} -u Administrator:password -H 'content-type: application/json' -d '{data}'")
        if error:
            self.assertTrue("syntax error" in str(results) or "SyntaxError" in str(results), f"The message is not correct, please check {results}")
        self.log.info(results)
        libraries = self.shell.execute_command(f"{self.curl_path} -s -k {url} -u Administrator:password")
        if library_name in str(libraries[0]):
            created = True
        else:
            self.log.error(f"The library {library_name} was not created: {libraries}")

        for function in function_names:
            if function in str(libraries[0]):
                created = True
            else:
                self.log.error(f"The function {function} was not created! {libraries}")
                created = False
                break
        return created

    '''Delete a library'''
    def delete_library(self, library_name =''):
        deleted = False
        url = f"http://{self.master.ip}:{self.n1ql_port}/evaluator/v1/libraries/{library_name}"
        curl_output = self.shell.execute_command(f"{self.curl_path} -X DELETE {url} -u Administrator:password ")
        libraries = self.shell.execute_command(f"{self.curl_path} {url} -u Administrator:password")
        if library_name not in str(libraries):
            deleted = True
        return deleted

##############################################################################################
#
#   tuq_xdcr.py helpers
#
##############################################################################################
# Tentative fixes for this implemented inside of tuq_xdcr itself, leaving these here until we have verified fix
# def _override_clusters_structure(self):
#     UpgradeTests._override_clusters_structure(self)
#
# def _create_buckets(self, nodes):
#     UpgradeTests._create_buckets(self, nodes)
#
# def _setup_topology_chain(self):
#     UpgradeTests._setup_topology_chain(self)
#
# def _set_toplogy_star(self):
#     UpgradeTests._set_toplogy_star(self)
#
# def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
#     UpgradeTests._join_clusters(self, src_cluster_name, src_master,
#                                 dest_cluster_name, dest_master)
#
# def _replicate_clusters(self, src_master, dest_cluster_name, buckets):
#     UpgradeTests._replicate_clusters(self, src_master, dest_cluster_name, buckets)
#
# def _get_bucket(self, bucket_name, server):
#     return UpgradeTests._get_bucket(self, bucket_name, server)
