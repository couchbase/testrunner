import re

from ent_backup_restore.validation_helpers.json_generator import JSONGenerator
from ent_backup_restore.validation_helpers.validation_base import BackupRestoreValidationBase
from remote.remote_util import RemoteMachineShellConnection


class DirectoryStructureValidations(BackupRestoreValidationBase):
    def __init__(self, backupset):
        BackupRestoreValidationBase.__init__(self)
        self.backupset = backupset

    def get_directory_structure(self):
        """
        Gets the actual directory structure after backing up a cluster
        :return: directory map
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        backup_directory = "{0}/{1}".format(self.backupset.directory, self.backupset.name)
        command = "ls -lR {0}".format(backup_directory)
        output, error = remote_client.execute_command(command)
        if error:
            self.log.error("Error in check backup directory {0}".format(error))
        remote_client.disconnect()
        current_path = self.backupset.directory
        parent_path = self.backupset.name
        directory_structure = {self.backupset.name: {}}
        backups = {}
        for out in output:
            if not out or out.startswith("total"):
                continue
            if out.startswith(current_path):
                parent_path = out[len(self.backupset.directory) + 1:-1]
                continue
            split = out.split(" ")
            split = [s for s in split if s]
            parents = parent_path.split("/")
            parents = [p for p in parents if p]
            obj = directory_structure
            for parent in parents:
                if re.match("\d{4}-\d{2}-\d{2}T", parent):
                    parent = backups[parent]
                obj = obj[parent]
            if split[0].startswith("d"):
                if re.match("\d{4}-\d{2}-\d{2}T", split[-1]):
                    backup = "backup"
                    backups[split[-1]] = backup
                    split[-1] = backup
                obj[split[-1]] = {}
            else:
                obj[split[-1]] = split[-1]
        if "logs" in directory_structure[self.backupset.name]:
            directory_structure[self.backupset.name].pop("logs")
        return directory_structure

    def generate_directory_structure(self):
        """
        Generates the expected directory structure for the backup
        :return: directory map
        """
        json_helper = JSONGenerator("directory_structure.json", self.backupset.__dict__)
        json_helper.generate_json()
        backupset = json_helper.object[self.backupset.name]
        curr_backup_len = len(backupset) - 1
        if curr_backup_len != self.backupset.number_of_backups:
            for i in range(curr_backup_len, self.backupset.number_of_backups):
                backupset["backups" + str(i)] = backupset["backups"]
        for backup in backupset:
            b = backupset[backup]
            if not isinstance(b, dict):
                continue
            for bucket in b:
                buck = b[bucket]
                if not isinstance(buck, dict):
                    continue
                if self.backupset.disable_bucket_config and 'bucket-config.json' in buck:
                    del buck['bucket-config.json']
                if self.backupset.disable_gsi_indexes and 'gsi.json' in buck:
                    del buck['gsi.json']
                if self.backupset.disable_views and 'views.json' in buck:
                    del buck['views.json']
                if self.backupset.disable_data and 'data' in bucket:
                    del buck['data']
                else:
                    buck['data'] = {}
                    for i in range(0, int(self.backupset.threads)):
                        shard = 'shard_{0}.sqlite.0'.format(i)
                        buck['data'][shard] = shard
        return json_helper.object

    def validate_directory_structure(self):
        """
        Validates the directory structure of a backup
        :return:  status and message
        """
        expected_directory_structure = self.generate_directory_structure()
        actual_directory_structure = self.get_directory_structure()
        is_equal, not_equal, extra, not_present = self.compare_dictionary(expected_directory_structure,
                                                                          actual_directory_structure)
        return self.compare_dictionary_result_analyser(is_equal, not_equal, extra, not_present, "Directory Structure")
