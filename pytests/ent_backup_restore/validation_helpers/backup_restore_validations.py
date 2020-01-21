import json
import os
import logger
import math

from couchbase_helper.data_analysis_helper import DataCollector
from ent_backup_restore.validation_helpers.directory_structure_validations import DirectoryStructureValidations
from ent_backup_restore.validation_helpers.files_validations import BackupRestoreFilesValidations
from ent_backup_restore.validation_helpers.validation_base import BackupRestoreValidationBase
from remote.remote_util import RemoteMachineShellConnection


class BackupRestoreValidations(BackupRestoreValidationBase):
    def __init__(self, backupset, cluster, restore_cluster, bucket,
                 backup_validation_path, backups, num_items,
                 vbuckets):
        BackupRestoreValidationBase.__init__(self)
        self.backupset = backupset
        self.cluster = cluster
        self.restore_cluster = restore_cluster
        self.buckets = bucket
        self.backup_validation_path = backup_validation_path
        self.backups = backups
        self.num_items = num_items
        self.vbuckets = vbuckets
        self.log = logger.Logger.get_logger()

    def validate_backup_create(self):
        """
        Validates that the backup directory is created as expected
        Validates the backup metadata using backup-meta.json
        :return: status and message
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        info = remote_client.extract_remote_info().type.lower()
        if info == 'linux' or info == 'mac':
            command = "ls -R {0}/{1}".format(self.backupset.directory, self.backupset.name)
            o, e = remote_client.execute_command(command)
        elif info == 'windows':
            o = remote_client.list_files("{0}/{1}".format(self.backupset.directory, self.backupset.name))
        if not o and len(o) != 2 and o[1] != "backup-meta.json":
            return False, "Backup create did not create backup-meta file."
        remote_client.disconnect()
        files_validations = BackupRestoreFilesValidations(self.backupset)
        status, msg = files_validations.validate_backup_meta_json()
        if status:
            msg += "\nBackup create validation success."
        return status, msg

    def validate_backup(self):
        """
        Validates directory structure for backup
        :return: status and message
        """
        directory_validator = DirectoryStructureValidations(self.backupset)
        status, msg = directory_validator.validate_directory_structure()
        if not status:
            return status, msg
        return True, msg

    def validate_restore(self, backup_number, backup_vbucket_seqno, restored_vbucket_seqno, compare_uuid=False,
                         compare="==", get_replica=False, mode="memory"):
        """
        Validates ep-engine stats and restored data
        :param backup_number: backup end number
        :param backup_vbucket_seqno: vbucket map of all backups
        :param restored_vbucket_seqno: vbucket map of restored cluster
        :param compare_uuid: bool to decide whether to compare uuid or not
        :param compare: comparator function
        :param get_replica: bool to decide whether to compare replicas or not
        :param mode: where to get the items from - can be "disk" or "memory"
        :return: status and message
        """
        self.log.info("backup start: " + str(self.backupset.start))
        self.log.info("backup end: " + str(self.backupset.end))
        success_msg = ""
        if not self.backupset.force_updates:
            status, msg = self.compare_vbucket_stats(backup_vbucket_seqno[backup_number - 1], restored_vbucket_seqno,
                                                 compare_uuid=compare_uuid, seqno_compare=compare)
            if not status:
                return status, msg
            success_msg = "{0}\n".format(msg)
        for bucket in self.buckets:
            kv_file_name = "{0}-{1}-{2}-{3}.json".format(bucket.name, "key_value", "backup", backup_number)
            kv_file_path = os.path.join(self.backup_validation_path, kv_file_name)
            backedup_kv = {}
            if os.path.exists(kv_file_path):
                try:
                    with open(kv_file_path, 'r') as f:
                        backedup_kv = json.load(f)
                except Exception as e:
                    raise e
            data_collector = DataCollector()
            info, restored_data = data_collector.collect_data(self.restore_cluster,
                                     [bucket],
                                     userId=self.restore_cluster[0].rest_username,
                                     password=self.restore_cluster[0].rest_password,
                                     perNode=False,
                                     getReplica=get_replica,
                                     mode=mode)
            data = restored_data[bucket.name]
            for key in data:
                value = data[key]
                value = ",".join(value.split(',')[4:5])
                data[key] = value

            self.log.info("Compare backup data in bucket %s " % bucket.name)
            is_equal, not_equal, extra, not_present = \
                                        self.compare_dictionary(backedup_kv, data)
            status, msg = self.compare_dictionary_result_analyser(is_equal,
                                                                  not_equal,
                                                                  extra,
                                                                  not_present,
                                                "{0} Data".format(bucket.name))
            if not status:
                return status, msg
            success_msg += "{0}\n".format(msg)
        success_msg += "Data validation success"
        return True, success_msg

    def validate_backup_list(self, output, num_backup=1, bucket_items=None):
        """
        Validates list command output
        :param output: list command output
        :return: status and message

        TODO: this function validates list command output for 1 backup // 1 bucket only
         - need to be enhanced for multiple backups
        """
        backup_name = False
        bucket_name = False
        backup_folder_timestamp = False
        items_count = False
        shard_count = False
        items = 0
        for line in output:
            if self.backupset.name in line:
                backup_name = True
            if str(self.buckets[0].name) in line:
                bucket_name = True
            if self.backups[0] in line:
                backup_folder_timestamp = True
            if "+ data" in line:
                split = line.split(" ")
                split = [s for s in split if s]
                if int(split[1]) == self.num_items:
                    items_count = True
            if "shard" in line.lower():
                split = line.split(" ")
                split = [s for s in split if s]
                items += int(split[1])
        if items == self.num_items:
            shard_count = True
        if not backup_name:
            return False, "Expected Backup name not found in list command output"
        if not bucket_name:
            return False, "Expected Bucket name not found in list command output"
        if not backup_folder_timestamp:
            return False, "Expected folder timestamp not found in list command output"
        if not items_count:
            return False, "Items count mismatch in list command output"
        if not shard_count:
            return False, "Shard count mismatch in list command output"
        return True, "List command validation success"

    def validate_compact_lists(self, output_before_compact, output_after_compact, is_approx=False):
        size_match = True
        for line1, line2 in zip(output_before_compact, output_after_compact):
            split1 = line1.split(" ")
            split1 = [s for s in split1 if s]
            split2 = line2.split(" ")
            split2 = [s for s in split2 if s]
            size1 = self._convert_to_kb(split1[0], is_approx)
            size2 = self._convert_to_kb(split2[0], is_approx)
            if is_approx:
                if size1 != size2:
                    size_match = False
                    break
            else:
                if size1 < size2:
                    size_match = False
                    break
        if not size_match:
            return False, "Size comparison failed after compact - before: {0} after: {1}".format(line1, line2)
        return True, "Compact command validation success"

    def _convert_to_kb(self, input, is_approx=False):
        if "MB" in input:
            result = input[:-2]
            if is_approx:
                return math.ceil(float(result)) * 1000
            else:
                return float(result) * 1000
        if "KB" in input:
            result = input[:-2]
            if is_approx:
                return math.ceil(float(result))
            else:
                return float(result)
        if "B" in input:
            result = input[:-1]
            if is_approx:
                return math.ceil(float(result)) / 1000
            else:
                return float(result) / 1000

    def validate_merge(self, backup_validation_path):
        for bucket in self.buckets:
            if self.backupset.exclude_buckets and bucket.name in \
                    self.backupset.exclude_buckets:
                continue
            if self.backupset.include_buckets and bucket.name not in \
                    self.backupset.include_buckets:
                continue
            if self.backupset.deleted_buckets and bucket.name in \
                    self.backupset.deleted_buckets:
                continue
            if self.backupset.new_buckets and bucket.name in \
                    self.backupset.new_buckets:
                continue
            if self.backupset.flushed_buckets and bucket.name in \
                    self.backupset.flushed_buckets:
                continue
            if self.backupset.deleted_backups:
                continue
            start_file_name = "{0}-{1}-{2}.json".format(bucket.name, "range", self.backupset.start)
            start_file_path = os.path.join(self.backup_validation_path, start_file_name)
            start_json = {}
            if os.path.exists(start_file_path):
                try:
                    with open(start_file_path, 'r') as f:
                        start_json = json.load(f)
                        start_json = json.loads(start_json)
                except Exception as e:
                    raise e
            end_file_name = "{0}-{1}-{2}.json".format(bucket.name, "range", self.backupset.end)
            end_file_path = os.path.join(self.backup_validation_path, end_file_name)
            end_json = {}
            if os.path.exists(end_file_path):
                try:
                    with open(end_file_path, 'r') as f:
                        end_json = json.load(f)
                        end_json = json.loads(end_json)
                except Exception as e:
                    raise e
            merge_file_name = "{0}-{1}-{2}.json".format(bucket.name, "range", "merge")
            merge_file_path = os.path.join(self.backup_validation_path, merge_file_name)
            merge_json = {}
            if os.path.exists(merge_file_path):
                try:
                    with open(merge_file_path, 'r') as f:
                        merge_json_str = json.load(f)
                        merge_json = json.loads(merge_json_str)
                except Exception as e:
                    raise e

            if not start_json or not end_json or not merge_json:
                return

            for i in range(0, int(self.vbuckets)):
                start_seqno = start_json['{0}'.format(i)]['last']['log']['log'][0]['seqno']
                start_uuid =  start_json['{0}'.format(i)]['last']['log']['log'][0]['uuid']
                end_seqno = end_json['{0}'.format(i)]['current']['log']['log'][0]['seqno']
                end_uuid =  end_json['{0}'.format(i)]['current']['log']['log'][0]['uuid']
                merge_last_seqno = merge_json['{0}'.format(i)]['last']['log']['log'][0]['seqno']
                merge_last_uuid = merge_json['{0}'.format(i)]['last']['log']['log'][0]['uuid']
                merge_current_seqno = merge_json['{0}'.format(i)]['current']['log']['log'][0]['seqno']
                merge_current_uuid = merge_json['{0}'.format(i)]['current']['log']['log'][0]['uuid']
                if not merge_last_seqno >= start_seqno:
                    raise Exception("merge_last_seqno is not >= start_seqno for vbucket {0}".format(i))
                if not start_uuid == merge_last_uuid:
                    raise Exception("start_uuid did not match merge_last_uuid for vbucket {0}".format(i))
                if not merge_current_seqno >= end_seqno:
                    raise Exception("merge_current_seqno is not >= end_seqno for vbucket {0}".format(i))
                if not end_uuid == merge_current_uuid:
                    raise Exception("end_uuid did not match merge_current_uuid for vbucket {0}".format(i))

            with open(start_file_path, 'w') as f:
                json.dump(merge_json_str, f)

        self.log.info("Merge validation completed successfully")