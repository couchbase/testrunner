import json
import os
import logger

from couchbase_helper.data_analysis_helper import DataCollector
from ent_backup_restore.validation_helpers.directory_structure_validations import DirectoryStructureValidations
from ent_backup_restore.validation_helpers.files_validations import BackupRestoreFilesValidations
from ent_backup_restore.validation_helpers.validation_base import BackupRestoreValidationBase
from remote.remote_util import RemoteMachineShellConnection


class BackupRestoreValidations(BackupRestoreValidationBase):
    def __init__(self, backupset, cluster, restore_cluster, bucket, backup_validation_path):
        BackupRestoreValidationBase.__init__(self)
        self.backupset = backupset
        self.cluster = cluster
        self.restore_cluster = restore_cluster
        self.buckets = bucket
        self.backup_validation_path = backup_validation_path
        self.log = logger.Logger.get_logger()

    def validate_backup_create(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -R {0}/{1}".format(self.backupset.directory, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if not o and len(o) != 2 and o[1] != "backup-meta.json":
            return False, "Backup create did not create backup-meta file."
        remote_client.disconnect()
        files_validations = BackupRestoreFilesValidations(self.backupset)
        status, msg = files_validations.validate_backup_meta_json()
        if status:
            msg += "\nBackup create validation success."
        return status, msg

    def validate_backup(self):
        directory_validator = DirectoryStructureValidations(self.backupset)
        status, msg = directory_validator.validate_directory_structure()
        if not status:
            return status, msg
        return True, msg

    def validate_restore(self, backup_number, backup_vbucket_seqno, restored_vbucket_seqno, compare_uuid=False,
                         compare="==", get_replica=False, mode="memory"):
        self.log.info("backup start: " + str(self.backupset.start))
        self.log.info("backup end: " + str(self.backupset.end))
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
                except Exception, e:
                    raise e
            data_collector = DataCollector()
            info, restored_data = data_collector.collect_data(self.restore_cluster, [bucket],
                                                              userId=self.restore_cluster[0].rest_username,
                                                              password=self.restore_cluster[0].rest_password,
                                                              perNode=False,
                                                              getReplica=get_replica, mode=mode)
            data = restored_data[bucket.name]
            for key in data:
                value = data[key]
                value = ",".join(value.split(',')[4:5])
                data[key] = value
            is_equal, not_equal, extra, not_present = self.compare_dictionary(backedup_kv, data)
            status, msg = self.compare_dictionary_result_analyser(is_equal, not_equal, extra, not_present,
                                                                  "{0} Data".format(bucket.name))
            if not status:
                return status, msg
            success_msg += "{0}\n".format(msg)
        success_msg += "Data validation success"
        return True, success_msg
