__author__ = 'ashvinder'
import re
import os
import gc
import logger
import time
from TestInput import TestInputSingleton
from backup.backup_base import BackupBaseTest
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.documentgenerator import DocumentGenerator
from memcached.helper.kvstore import KVStore
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.data_analysis_helper import *
from memcached.helper.data_helper import VBucketAwareMemcached
from view.spatialquerytests import SimpleDataSet
from view.spatialquerytests import SpatialQueryTests
from membase.helper.spatial_helper import SpatialHelper
from couchbase_helper.cluster import Cluster
from membase.helper.bucket_helper import BucketOperationHelper
from couchbase_helper.document import DesignDocument, View
import copy


class IBRTests(BackupBaseTest):
    def setUp(self):
        super(IBRTests, self).setUp()
        self.num_mutate_items = self.input.param("mutate_items", 1000)
        gen_load = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0, 1, self.item_flag, True, batch_size=20000,
                               pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        #Take a full backup
        if not self.command_options:
            self.command_options = []
        options = self.command_options + [' -m full']
        self.total_backups = 1
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)


    def tearDown(self):
        super(IBRTests, self).tearDown()

    def restoreAndVerify(self, bucket_names, kvs_before, expected_error=None):
        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
        del kvs_before
        gc.collect()

        errors, outputs = self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)
        errors.extend(outputs)
        error_found = False
        if expected_error:
            for line in errors:
                if line.find(expected_error) != -1:
                    error_found = True
                    break

            self.assertTrue(error_found, "Expected error not found: %s" % expected_error)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        if expected_error:
            for bucket in self.buckets:
                bucket.kvs[1] = KVStore()
        self.verify_results(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def verify_dir_structure(self, total_backups, buckets, nodes):

        cmd = 'find ' + self.backup_location + ' -type f'
        if self.shell.info.type.lower() == 'windows':
            cmd = 'cmd.exe /C "dir /s /b C:\\tmp\\backup"'

        output, error = self.shell.execute_command(cmd)
        self.log.info("output = {0} error = {1}".format(output, error))

        if error:
            raise Exception('Got error {0}', format(error))

        expected_design_json = total_backups * buckets
        expected_data_cbb = total_backups * buckets * nodes
        expected_meta_json = total_backups * buckets * nodes
        expected_failover_json = total_backups * buckets * nodes

        timestamp = '\d{4}\-\d{2}\-\d{2}T\d+Z'
        pattern_mode = '(full|accu|diff)'
        timestamp_backup = timestamp + '\-' + pattern_mode
        pattern_bucket = 'bucket-\w+'
        pattern_node = 'node\-\d{1,3}\.\d{1,3}\.\d{1,3}.\d{1,3}.+'

        pattern_design_json = timestamp + '/|\\\\' + timestamp_backup + \
                                                    '/|\\\\' + pattern_bucket
        pattern_backup_files = pattern_design_json +  '/|\\\\' + pattern_node

        data_cbb = 0
        failover = 0
        meta_json = 0
        design_json = 0

        for line in output:
            if 'data-0000.cbb' in line:
                if re.search(pattern_backup_files, line):
                    data_cbb += 1
            if 'failover.json' in line:
                if re.search(pattern_backup_files, line):
                    failover += 1
            if self.cb_version[:5] != "4.5.1" and 'meta.json' in line:
                if re.search(pattern_backup_files, line):
                    meta_json += 1
            if 'design.json' in line:
                if re.search(pattern_design_json, line):
                    design_json += 1

        self.log.info("expected_data_cbb {0} data_cbb {1}"
                           .format(expected_data_cbb, data_cbb))
        self.log.info("expected_failover_json {0} failover {1}"
                      .format(expected_failover_json, failover))
        if self.cb_version[:5] != "4.5.1":
            self.log.info("expected_meta_json {0} meta_json {1}"
                        .format(expected_meta_json,  meta_json))
        """ add json support later in this test
            self.log.info("expected_design_json {0} design_json {1}"
                          .format(expected_design_json, design_json)) """

        if self.cb_version[:5] != "4.5.1":
            if data_cbb == expected_data_cbb and failover == expected_failover_json and \
                meta_json == expected_meta_json:
                # add support later in and design_json == expected_design_json:
                return True
        else:
            if data_cbb == expected_data_cbb and failover == expected_failover_json:
                return True

        return False

    def testFullBackupDirStructure(self):
        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Full Backup')

    def testMultipleFullBackupDirStructure(self):

        for count in range(10):
            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a incremental backup
            options = self.command_options + [' -m full']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.sleep(120)

        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Full Backup')


    def testIncrBackupDirStructure(self):
        # Update data
        gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                               pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        #Take a incremental backup
        options = self.command_options + [' -m accu']
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

        self.total_backups += 1

        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Incremental Backup')

    def testMultipleIncrBackupDirStructure(self):

        for count in range(10):
            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a incremental backup
            options = self.command_options + [' -m accu']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.log.info("sleeping for 30 secs")
            self.sleep(30)

        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Incremental Backup')

    def testMultipleDiffBackupDirStructure(self):

        for count in range(10):
            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a incremental backup
            options = self.command_options + [' -m diff']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.sleep(60)

        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Differential Backup')


    def testMultipleIncrDiffBackupDirStructure(self):

        for count in range(10):
            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a incremental backup
            options = self.command_options + [' -m accu']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.sleep(60)

            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a diff backup
            options = self.command_options + [' -m diff']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.sleep(60)

        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Combo Incr and Diff Backup')

    def testMultipleFullIncrDiffBackupDirStructure(self):

        for count in range(10):
            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a incremental backup
            options = self.command_options + [' -m accu']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.sleep(60)

            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a diff backup
            options = self.command_options + [' -m diff']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

            self.total_backups += 1
            self.sleep(60)

            # Update data
            gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                                   pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a full backup
            options = self.command_options + [' -m full']
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options, delete_backup=False)

            self.total_backups += 1
            self.sleep(60)

        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Combo Full,Incr and Diff Backups')

    def testDiffBackupDirStructure(self):
        # Update data
        gen_update = BlobGenerator('testdata', 'testdata-', self.value_size, end=5)
        self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000,
                               pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        #Take a diff backup
        options = self.command_options + [' -m diff']
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

        self.total_backups += 1
        if not self.verify_dir_structure(self.total_backups, len(self.buckets), len(self.servers)):
            raise Exception('Backup Directory Verification Failed for Differential Backup')

    def testIncrementalBackup(self):
        gen_extra = BlobGenerator('zoom', 'zoom-', self.value_size, end=self.num_items)
        self.log.info("Starting Incremental backup")

        extra_items_deleted_flag = 0

        if(self.doc_ops is not None):
            self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "update", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "delete", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                extra_items_deleted_flag = 1
            if("expire" in self.doc_ops):
                if extra_items_deleted_flag == 1:
                    self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                self._load_all_buckets(self.master, gen_extra, "update", self.expire_time, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)


        #Take a incremental backup
        options = self.command_options + [' -m accu']
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

        # Save copy of data
        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]

        # Delete all buckets
        self._all_buckets_delete(self.master)
        gc.collect()

        self._bucket_creation()
        self.sleep(20)
        self.restoreAndVerify(bucket_names, kvs_before)


    def testDifferentialBackup(self):

        gen_extra = BlobGenerator('zoom', 'zoom-', self.value_size, end=self.num_items)
        self.log.info("Starting Differential backup")

        extra_items_deleted_flag = 0

        if(self.doc_ops is not None):
            self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "update", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "delete", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                extra_items_deleted_flag = 1
            if("expire" in self.doc_ops):
                if extra_items_deleted_flag == 1:
                    self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                self._load_all_buckets(self.master, gen_extra, "update", self.expire_time, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)


        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        #Take a diff backup
        options = self.command_options + [' -m diff']
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

        # Save copy of data
        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]

        # Delete all buckets
        self._all_buckets_delete(self.master)
        gc.collect()

        self._bucket_creation()
        self.sleep(20)

        self.restoreAndVerify(bucket_names, kvs_before)

    def testFullBackup(self):
        # Save copy of data
        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]

        # Delete all buckets
        self._all_buckets_delete(self.master)
        gc.collect()

        self._bucket_creation()
        self.sleep(20)
        self.restoreAndVerify(bucket_names, kvs_before)

    def testIncrementalBackupConflict(self):
        gen_extra = BlobGenerator('zoom', 'zoom-', self.value_size, end=self.num_items)
        self.log.info("Starting Incremental backup")

        extra_items_deleted_flag = 0

        if(self.doc_ops is not None):
            self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "update", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "delete", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                extra_items_deleted_flag = 1
            if("expire" in self.doc_ops):
                if extra_items_deleted_flag == 1:
                    self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                self._load_all_buckets(self.master, gen_extra, "update", self.expire_time, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)

        #Take a incremental backup
        options = self.command_options + [' -m accu']
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)

        # Save copy of data
        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]

        # Delete all buckets
        self._all_buckets_delete(self.master)
        gc.collect()
        self.lww = self.num_mutate_items = self.input.param("lww_new", False)
        self._bucket_creation()
        self.sleep(20)
        expected_error = self.input.param("expected_error", None)
        self.restoreAndVerify(bucket_names, kvs_before, expected_error)

class IBRJsonTests(BackupBaseTest):
    def setUp(self):
        super(IBRJsonTests, self).setUp()
        self.num_mutate_items = self.input.param("mutate_items", 1000)
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('load_by_id_test', template, list(range(5)),\
                             ['james', 'john'], start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0, 1,\
                              self.item_flag, True, batch_size=20000,\
                                       pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        if self.test_with_view:
            view_list = []
            bucket = "default"
            if self.dev_view:
                prefix_ddoc="dev_ddoc"
            else:
                prefix_ddoc="ddoc"
            ddoc_view_map = self.bucket_ddoc_map.pop(bucket, {})
            for ddoc_count in range(self.num_ddocs):
                design_doc_name = prefix_ddoc + str(ddoc_count)
                view_list = self.make_default_views("views", self.num_views_per_ddoc)
                self.create_views(self.master, design_doc_name, view_list,\
                                             bucket, self.wait_timeout * 2)
                ddoc_view_map[design_doc_name] = view_list
            self.bucket_ddoc_map[bucket] = ddoc_view_map

        #Take a full backup
        if not self.command_options:
            self.command_options = []
        options = self.command_options + [' -m full']
        self.total_backups = 1
        self.shell.execute_cluster_backup(self.couchbase_login_info,\
                                               self.backup_location, options)
        self.sleep(2)

    def testFullBackup(self):
        # Save copy of data
        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]

        # Delete all buckets
        self._all_buckets_delete(self.master)
        gc.collect()

        self._bucket_creation()
        self.sleep(20)
        self.restoreAndVerify(bucket_names, kvs_before)


    def restoreAndVerify(self, bucket_names, kvs_before):
        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
        del kvs_before
        gc.collect()

        self.shell.restore_backupFile(self.couchbase_login_info,\
                                       self.backup_location, bucket_names)
        self.sleep(10)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.verify_results(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])
        """ add design doc and view """
        if self.test_with_view:
            result = False
            query = {"stale" : "false", "full_set" : "true", \
                                        "connection_timeout" : 60000}
            for bucket, ddoc_view_map in list(self.bucket_ddoc_map.items()):
                for ddoc_name, view_list in list(ddoc_view_map.items()):
                    for view in view_list:
                        try:
                            result = self.cluster.query_view(self.master,\
                                             ddoc_name, view.name, query,\
                                               self.num_items, timeout=10)
                        except Exception:
                            pass
                        if not result:
                            self.fail("There is no: View: {0} in Design Doc:"\
                                                        " {1} in bucket: {2}"\
                                        .format(view.name, ddoc_name, bucket))
            self.log.info("DDoc Data Validation Successful")

    def tearDown(self):
        super(IBRJsonTests, self).tearDown()

    def testMultipleBackups(self):
        if not self.command_options:
            self.command_options = []

        options = self.command_options

        if self.backup_type is not None:
            if "accu" in self.backup_type:
                options = self.command_options + [' -m accu']
            if "diff" in self.backup_type:
                options = self.command_options + [' -m diff']

        diff_backup = [" -m diff"]
        accu_backup = [" -m accu"]
        current_backup = [" -m diff"]

        for count in range(self.number_of_backups):
            if "mix" in self.backup_type:
                if current_backup == diff_backup:
                    current_backup = accu_backup
                    options = self.command_options + accu_backup
                elif current_backup == accu_backup:
                    current_backup = diff_backup
                    options = self.command_options + diff_backup


            # Update data
            template = '{{ "mutated" : {0}, "age": {0}, "first_name": "{1}" }}'
            gen_update = DocumentGenerator('load_by_id_test', template, list(range(5)),\
                                   ['james', 'john'], start=0, end=self.num_items)
            self._load_all_buckets(self.master, gen_update, "update", 0, 1,\
                                    self.item_flag, True, batch_size=20000,\
                                             pause_secs=5, timeout_secs=180)
            self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

            #Take a backup
            self.shell.execute_cluster_backup(self.couchbase_login_info,\
                                               self.backup_location, options)

        # Save copy of data
        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]

        # Delete all buckets
        self._all_buckets_delete(self.master)
        gc.collect()

        self._bucket_creation()
        self.sleep(20)

        self.restoreAndVerify(bucket_names, kvs_before)

class IBRSpatialTests(SpatialQueryTests):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        self.helper.setup_cluster()
        self.cluster = Cluster()
        self.default_bucket = self.input.param("default_bucket", True)
        self.sasl_buckets = self.input.param("sasl_buckets", 0)
        self.standard_buckets = self.input.param("standard_buckets", 0)
        self.memcached_buckets = self.input.param("memcached_buckets", 0)
        self.servers = self.helper.servers
        self.shell = RemoteMachineShellConnection(self.master)
        info = self.shell.extract_remote_info()
        self.os = info.type.lower()
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.command_options = self.input.param("command_options", '')



    def tearDown(self):
        self.helper.cleanup_cluster()

    def test_backup_with_spatial_data(self):
        num_docs = self.helper.input.param("num-docs", 5000)
        self.log.info("description : Make limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))
        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init(data_set)

        if not self.command_options:
            self.command_options = []
        options = self.command_options + [' -m full']

        self.total_backups = 1
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, options)
        time.sleep(2)

        self.buckets = RestConnection(self.master).get_buckets()
        bucket_names = [bucket.name for bucket in self.buckets]
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        gc.collect()

        self.helper._create_default_bucket()
        self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)

        SimpleDataSet(self.helper, num_docs)._create_views()
        self._query_test_init(data_set)
