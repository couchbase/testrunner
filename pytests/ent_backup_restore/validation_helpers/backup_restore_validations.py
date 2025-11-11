import json
import os
from os import listdir
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
                 vbuckets, objstore_provider):
        BackupRestoreValidationBase.__init__(self)
        self.backupset = backupset
        self.cluster = cluster
        self.restore_cluster = restore_cluster
        self.buckets = bucket
        self.backup_validation_path = backup_validation_path
        self.backups = backups
        self.num_items = num_items
        self.vbuckets = vbuckets
        self.objstore_provider = objstore_provider
        self.log = logger.Logger.get_logger()

    def validate_backup_create(self):
        """
        Validates that the backup directory is created as expected
        Validates the backup metadata using backup-meta.json
        :return: status and message
        """
        if self.objstore_provider:
            keys = self.objstore_provider.list_objects()

            # We expect to have seen three files created
            # .backup
            # .info
            # logs/backup-0.log
            # repo/backup-meta.json
            # README.md
            if len(keys) != 5 and len(keys) != 4:
                self.log.error(f"Expected 5 or 4 files, but got {len(keys)} files")
                self.log.error(f"Keys: {keys}")
                return False, "config did not create the expected number of files"

            if f"{self.backupset.directory}/{self.backupset.name}/backup-meta.json" not in keys:
                return False, "config did not create the backup-meta.json file in: " + f"{self.backupset.directory}/{self.backupset.name}"
        else:
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

        files_validations = BackupRestoreFilesValidations(self.backupset, self.objstore_provider)
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
                         compare="==", get_replica=False, mode="memory", backups_on_disk=None):
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
                            compare_uuid=compare_uuid, seqno_compare=compare, mapBucket = self.backupset.map_buckets)
            if not status:
                return status, msg
            success_msg = "{0}\n".format(msg)
        for bucket in self.buckets:
            kv_file_name = "{0}-{1}-{2}-{3}.json".format(bucket.name, "key_value", "backup", backup_number)
            kv_file_path = os.path.join(self.backup_validation_path, kv_file_name)
            if backup_number == self.backupset.end and backup_number == backups_on_disk:
                backup_files = [f"{self.backup_validation_path}/" + f for f in listdir(self.backup_validation_path) if f"{bucket.name}-key_value-backup" in f]
                backup_files.sort(key=os.path.getmtime)
                try:
                    kv_file_path = backup_files[-1]
                except IndexError as e:
                    if self.backupset.deleted_buckets:
                        return True, "Deleted bucket, will not check"
                    else:
                        raise e
            backedup_kv = {}
            if os.path.exists(kv_file_path):
                try:
                    with open(kv_file_path, 'r') as f:
                        backedup_kv = json.load(f)
                except Exception as e:
                    raise e
            else:
                raise Exception(f"{kv_file_path} is missing!")
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
                if '"b\'' in value:
                    value = ",".join(value.split(',')[4:8])
                else:
                    value = ",".join(value.split(',')[4:5])
                value = value.replace('""', '"')
                if value.startswith('"b\''):
                    value = value[3:-2]
                elif value.startswith("b"):
                    value = value.split(',')[0]
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
        bk_type = ["FULL"]
        items = 0
        if output and output[0]:
            bk_info = json.loads(output[0])
            bk_name = bk_info["name"]
            bk_info = bk_info["repos"]
        else:
            return False, "No output content"
                    
        self.log.info("list is deprecated from 7.0.0.  Use `info` instead")
        if self.backupset.name in bk_name: #bk_info[0]["backups"][0]["date"]:
            backup_name = True
        if str(self.buckets[0].name) in bk_info[0]["backups"][0]["buckets"][0]["name"]:
            bucket_name = True
        if self.backups[0] in bk_info[0]["backups"][0]["date"]:
            backup_folder_timestamp = True
        if self.num_items == bk_info[0]["backups"][0]["buckets"][0]["items"]:
            items_count = True
        if not backup_name:
            return False, "Expected Backup name not found in info command output. Expected: " + self.backupset.name + " Actual: " + bk_name
        if not bucket_name:
            return False, "Expected Bucket name not found in info command output. Expected: " + self.buckets[0].name + " Actual: " + bk_info[0]["backups"][0]["buckets"][0]["name"]
        if not backup_folder_timestamp:
            return False, "Expected folder timestamp not found in info command output. Expected: " + self.backups[0] + " Actual: " + bk_info[0]["backups"][0]["date"]
        if not items_count:
            return False, "Items count mismatch in info command output. Expected: " + self.num_items + " Actual: " + bk_info[0]["backups"][0]["buckets"][0]["items"]
        if bk_info[0]["backups"][0]["type"] not in bk_type:
            return False, "Backup complete does not have value"
        if not bk_info[0]["backups"][0]["complete"]:
            return False, "Backup type is not in info command output"
        return True, "Info command validation success"

    def validate_backup_info(self, output, scopes=None, collections=None,
                                         num_backup=1, bucket_items=None):
        """
        Validates info command output with --all and without --all
        :param output: info command output
        :return: status and message
        """
        backup_name = False
        bucket_name = False
        backup_folder_timestamp = False
        items_count = False
        first_backup_full = False
        after_first_backup_incr = False
        bk_type = ["FULL"]
        if collections is not None:
            col_dict = False
            for col in collections:
                if ":" in col:
                    col_dict = True
            if col_dict:
                collections = collections[1::2]
                collections = [x.replace("-", "") for x in collections]
                collections = [x.strip() for x in collections]
        items = 0
        if output and output[0]:
            if self.backupset.info_to_json:
                bk_info = json.loads(output[0])
                bk_name = bk_info["name"]
                bk_scopes = bk_info["backups"][0]["buckets"][0]["scopes"]
            else:
                return True, "For non-json output, the json data validation should be sufficient, and we just need to ensure there are no errors"
        else:
            return False, "No output content"
        if self.backupset.name in bk_name:
            backup_name = True
        if self.backupset.info_to_json:
            for idx, val in enumerate(bk_info["backups"]):
                if self.backups[idx] in bk_info["backups"][idx]["date"]:
                    backup_folder_timestamp = True
                if self.num_items == bk_info["backups"][idx]["buckets"][0]["items"]:
                    items_count = True
                if idx == 0:
                    if bk_info["backups"][idx]["type"] == "FULL":
                        first_backup_full = True
                if idx > 0:
                    if bk_info["backups"][idx]["type"] == "INCR":
                        after_first_backup_incr = True
                if idx > len(self.buckets) - 1:
                    continue
                elif str(self.buckets[idx].name) in bk_info["backups"][idx]["buckets"][0]["name"]:
                    bucket_name = True

            if scopes:
                found_scope = False
                found_collections = []
                scopes_collections = {}
                for scope_id in bk_scopes:
                    scopes_collections[scope_id] = []
                    if isinstance(scopes, list):
                        if bk_scopes[scope_id]["name"] not in scopes:
                            return False, "scope {0} not in backup repo"\
                                      .format(bk_scopes[scope_id]["name"])
                    elif isinstance(scopes, str):
                        if scopes in bk_scopes[scope_id]["name"]:
                            found_scope = True
                    if collections:
                        repo_collections = []
                        for collection in list(bk_scopes[scope_id]["collections"].values()):
                            scopes_collections[scope_id].append(collection["name"])
                        if isinstance(collections, list):
                            if len(collections) == 1:
                                if collections[0] in scopes_collections[scope_id]:
                                    found_collections.append(collections[0])
                            elif len(collections) > 1:
                                for col in collections:
                                    if col in scopes_collections[scope_id]:
                                        found_collections.append(col)
                        if self.backupset.load_to_collection:
                            if str(self.backupset.load_scope_id[2:]) in list(bk_scopes[scope_id]["collections"].keys()):
                                self.log.info("check items in collection")
                                print("\nitems number: ", bk_scopes[scope_id]["collections"][str(self.backupset.load_scope_id[2:])]["mutations"])
                                if bk_scopes[scope_id]["collections"][str(self.backupset.load_scope_id[2:])]["mutations"] != self.num_items:
                                    raise Exception("collection items not in backup")
                if len(found_collections) != len(collections):
                    raise Exception("collection may not in backup repo.  Found cols: {0} != check cols: {1}"\
                                                           .format(found_collections, collections))
                if not found_scope and isinstance(scopes, str):
                    return False, "scope {0} not in backup repo".format(scopes)

        if not backup_name:
            return False, "Expected Backup name not found in info command output"
        if not bucket_name:
            return False, "Expected Bucket name not found in info command output"
        if not backup_folder_timestamp:
            return False, "Expected folder timestamp not found in info command output"
        if not items_count:
            return False, "Items count mismatch in info command output"
        if not first_backup_full:
            return False, "First backup is not a full backup"
        if self.backupset.info_to_json:
            if len(bk_info["backups"]) >= 2 and not after_first_backup_incr:
                return False, "second backup is not a incr backup"
            if not bk_info["backups"][0]["complete"]:
                return False, "Backup type is not in info command output"
        return True, "Info command validation success"


    def validate_compact_lists(self, output_before_compact, output_after_compact, is_approx=False):
        size_match = True
        if output_before_compact and output_before_compact[0]:
            output_before_compact = json.loads(output_before_compact[0])
            output_before_compact = output_before_compact["repos"]
        else:
            return False, "No output_before_compact content"

        if output_after_compact and output_after_compact[0]:
            output_after_compact = json.loads(output_after_compact[0])
            output_after_compact = output_after_compact["repos"]
        else:
            return False, "No output_after_compact content"
        size1 = output_before_compact[0]["size"]
        size2 = output_after_compact[0]["size"]
        if is_approx:
            if size1 != size2:
                size_match = False
        else:
            if size1 < size2:
                size_match = False
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
        self.log.warning("MERGE VALIDATION IS CURRENTLY NON-FUNCTIONAL")
        return
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
