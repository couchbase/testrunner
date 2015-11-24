import json
import re

from enterprise_backup_restore.validation_helpers.json_generator import JSONGenerator
from remote.remote_util import RemoteMachineShellConnection


class DirectoryValidationHelper:
    def __init__(self, backupset):
        self.backupset = backupset

    def validate_directory(self):
        expected_directory_structure = self.generate_directory_structure()
        actual_directory_structure = self.get_directory_structure()
        return self.compare_dictionary(expected_directory_structure, actual_directory_structure)

    @staticmethod
    def compare_dictionary(expected, actual, is_equal=None, not_equal=None, extra=None, not_present=None):
        if is_equal is None:
            is_equal = True
        if not not_equal:
            not_equal = {}
        if not extra:
            extra = {}
        if not not_present:
            not_present = {}
        if expected.__len__() < actual.__len__():
            is_equal = False
            extra_keys = set(actual) - set(expected)
            for key in extra_keys:
                if key not in extra:
                    extra[key] = []
                extra[key].append(actual[key])
        for expected_key in expected.keys():
            if expected_key in actual:
                if not isinstance(expected[expected_key], dict) and not isinstance(expected[expected_key], list):
                    if unicode(expected[expected_key]) != unicode(actual[expected_key]):
                        is_equal = False
                        if expected_key not in not_equal:
                            not_equal[expected_key] = {"expected": [], "actual": []}
                        not_equal[expected_key]["expected"].append(expected[expected_key])
                        not_equal[expected_key]["actual"].append(actual[expected_key])
                elif isinstance(expected[expected_key], list):
                    expected_list = expected[expected_key]
                    actual_list = actual[expected_key]
                    if set(expected_list) != set(actual_list):
                        is_equal = False
                        if expected_key not in not_equal:
                            not_equal[expected_key] = {"expected": [], "actual": []}
                        not_equal[expected_key]["expected"].extend(expected_list)
                        not_equal[expected_key]["actual"].extend(actual_list)
                elif isinstance(expected[expected_key], dict):
                    return DirectoryValidationHelper.compare_dictionary(expected[expected_key], actual[expected_key],
                                                                        is_equal, not_equal, extra, not_present)
            else:
                if expected_key not in not_present:
                    not_present[expected_key] = {"expected": []}
                not_present[expected_key]["expected"].append(expected[expected_key])
        return is_equal, not_equal, extra, not_present

    def get_directory_structure(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        backup_directory = "{0}/{1}".format(self.backupset.dir, self.backupset.name)
        command = "ls -lR {0}".format(backup_directory)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        current_path = self.backupset.dir
        parent_path = self.backupset.name
        current_backup = 1
        directory_structure = {self.backupset.name: {}}
        backups = {}
        for out in output:
            if not out or out.startswith("total"):
                continue
            if out.startswith(current_path):
                parent_path = out[len(self.backupset.dir) + 1:-1]
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
                    backup = "backup{0}".format(current_backup)
                    backups[split[-1]] = backup
                    split[-1] = backup
                    current_backup += 1
                obj[split[-1]] = {}
            else:
                obj[split[-1]] = split[-1]
        return directory_structure

    def generate_directory_structure(self):
        json_helper = JSONGenerator("directory_structure.json", self.backupset.__dict__)
        json_helper.generate()
        backupset = json_helper.object[self.backupset.name]
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
                        shard = 'shard_{0}.fdb'.format(i)
                        buck['data'][shard] = shard
        return json_helper.object

    def get_backup_meta_json(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        backup_meta_file_path = "{0}/{1}/backup-meta.json".format(self.backupset.dir, self.backupset.name)
        remote_client.copy_file_remote_to_local(backup_meta_file_path, "/tmp/backup-meta.json")
        backup_meta = json.load(open("/tmp/backup-meta.json"))
        return backup_meta


    def generate_backup_meta_json(self):
        json_helper = JSONGenerator("backup-meta.json", self.backupset.__dict__)
        json_helper.generate()
        return json_helper.object