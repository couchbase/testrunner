__author__ = 'ashvinder'
import re
import os
import gc
from backup.backup_base import BackupBaseTest
from couchbase.documentgenerator import BlobGenerator
from couchbase.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection, Bucket
from couchbase.data_analysis_helper import *


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

    def restoreAndVerify(self,bucket_names,kvs_before):
        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
        del kvs_before
        gc.collect()

        self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)

        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.verify_results(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def verify_dir_structure(self, total_backups, buckets, nodes):

        cmd = 'find ' + self.backup_location + ' -type f'
        if self.shell.info.type.lower() == 'windows':
            cmd = 'cmd.exe /C "dir /s /b C:\\tmp\\backup"'

        output, error = self.shell.execute_command(cmd)
        self.log.info("output = {0} error = {1}".format(output,error))

        if error:
            raise Exception('Got error {0}',format(error))

        expected_design_json = total_backups * buckets
        expected_data_cbb = total_backups * buckets * nodes
        expected_meta_json = total_backups * buckets * nodes
        expected_failover_json = total_backups * buckets * nodes

        timestamp = '\d{4}\-\d{2}\-\d{2}T\d+Z'
        pattern_mode = '(full|accu|diff)'
        timestamp_backup = timestamp + '\-' + pattern_mode
        pattern_bucket = 'bucket-\w+'
        pattern_node = 'node\-\d{1,3}\.\d{1,3}\.\d{1,3}.\d{1,3}.+'

        pattern_design_json = timestamp + '/|\\\\' + timestamp_backup + '/|\\\\' + pattern_bucket
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
            if 'meta.json' in line:
                if re.search(pattern_backup_files, line):
                    meta_json += 1
            if 'design.json' in line:
                if re.search(pattern_design_json, line):
                    design_json += 1

        self.log.info("expected_data_cbb {0} data_cbb {1}".format(expected_data_cbb, data_cbb))
        self.log.info("expected_failover_json {0} failover {1}".format(expected_failover_json, failover))
        self.log.info("expected_meta_json {0} meta_json {1}".format(expected_meta_json,  meta_json))
        self.log.info("expected_design_json {0} design_json {1}".format(expected_design_json, design_json))

        if data_cbb == expected_data_cbb and failover == expected_failover_json and \
            meta_json == expected_meta_json and design_json == expected_design_json:
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
            self.log.info("sleeping for 60 secs")
            self.sleep(60)

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
        gen_extra = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_mutate_items)
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
        self.restoreAndVerify(bucket_names, kvs_before)


    def testDifferentialBackup(self):

        gen_extra = BlobGenerator('testdata', 'testdata-', self.value_size, end=self.num_mutate_items)
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

        self.restoreAndVerify(bucket_names, kvs_before)
