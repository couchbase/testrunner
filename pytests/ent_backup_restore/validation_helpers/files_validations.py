import json

from ent_backup_restore.validation_helpers.json_generator import JSONGenerator
from ent_backup_restore.validation_helpers.validation_base import BackupRestoreValidationBase
from remote.remote_util import RemoteMachineShellConnection


class BackupRestoreFilesValidations(BackupRestoreValidationBase):
    def __init__(self, backupset, objstore_provider):
        BackupRestoreValidationBase.__init__(self)
        self.backupset = backupset
        self.objstore_provider = objstore_provider


    def get_backup_meta_json(self):
        """
        Gets the actual backup metadata json after backup create
        :return: backup meta map
        """
        if self.objstore_provider:
            backup_meta = self.objstore_provider.get_json_object(f"{self.backupset.directory}/{self.backupset.name}/backup-meta.json")
        else:
            remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
            backup_meta_file_path = "{0}/{1}/backup-meta.json".format(self.backupset.directory, self.backupset.name)
            remote_client.copy_file_remote_to_local(backup_meta_file_path, "/tmp/backup-meta.json")
            remote_client.disconnect()
            backup_meta = json.load(open("/tmp/backup-meta.json"))

        return backup_meta


    def generate_backup_meta_json(self):
        """
        Generates expected backup metadata json
        :return: backup meta map
        """
        json_helper = JSONGenerator("backup-meta.json", self.backupset.__dict__)
        json_helper.generate_json()
        return json_helper.object

    def validate_backup_meta_json(self):
        """
        Validates backup metadata json
        :return: status and message
        """
        expected_meta_json = self.generate_backup_meta_json()
        expected_meta_json['auto_resolve_conflicts'] = [False]
        if self.backupset.exclude_buckets:
            exclude_data = {}
            exclude_data["bucket"] = self.backupset.exclude_buckets[0]
            expected_meta_json["exclude_data"].append(exclude_data)
        if self.backupset.disable_ft_alias or self.backupset.disable_ft_indexes:
            expected_meta_json['disable_ft_alias'] = True
        if self.backupset.disable_analytics:
            expected_meta_json['disable_analytics'] = True
        if self.objstore_provider and self.objstore_provider.schema_prefix() == "gs://":
            expected_meta_json['filter_vbuckets']['enabled'] = True
            expected_meta_json['filter_vbuckets']['list'] = list(range(64))
        actual_meta_json = self.get_backup_meta_json()
        is_equal, not_equal, extra, not_present = self.compare_dictionary(expected_meta_json, actual_meta_json)
        return self.compare_dictionary_result_analyser(is_equal, not_equal, extra, not_present, "Backup Meta data json")
