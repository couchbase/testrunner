import re

from enterprise_backup_restore.validation_helpers.json_generator import JSONGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class DirectoryValidationHelper:
    def __init__(self, backupset):
        self.backupset = backupset

    def get_directory_structure(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        backup_directory = self.backupset.dir + self.backupset.name
        command = "ls -R {0}".format(backup_directory)
        output, error = remote_client.execute_command(command)
        directory_structure = {self.backupset.name: []}
        current_directory = self.backupset.name
        current_backup = 1
        for o in output:
            # continue if blank line
            if not o:
                continue
            # get the next directory if output line is the path of the next sub directory
            if o.startswith(backup_directory):
                subdirectory = o[len(backup_directory) + 1:]
                if subdirectory:
                    current_directory = subdirectory
                continue
            # Append the content of the directory to the list for the current sub directory
            files_in_current_directory = o.split("\t")
            for f in files_in_current_directory:
                # If the file is a backup folder, change the name from timestamp to readable format.
                # The output is sorted in the order of creation, hence timestamp can be exchanged to ordered name.
                if re.match("\d{4}-\d{2}-\d{2}T", f):
                    f = "backup{0}".format(current_backup)
                    current_backup += 1
                directory_structure[current_directory] = directory_structure[current_directory].append(f)
        return directory_structure

    def compare_directory_structure(self, expected, actual):
        len_expected = len(expected)
        len_actual = len(actual)
        if len_expected != len_actual:
            return False, "Directory structure not correct. Expected number of sub directories: {0} " \
                          "Actual number of sub directories: {1}".format(len_expected, len_actual)
        extra, not_present, different, same = self.compare_dictionary(expected, actual)
        if len(extra) != 0 or len(not_present) != 0 or len(different) != 0:
            return False, "Directory structure not same. \n" \
                          "Extra files: {0} \n" \
                          "Files not present but expected: {1} \n" \
                          "Files different from expected: {2} \n" \
                .format(", ".join(extra), ", ".join(not_present),
                        ", ".join(different))
        return True, "Directory structure are the same"

    def generate_directory_structure(self):
        rest = RestConnection(self.backupset.host)
        directory_structure = {self.backupset.name: []}
        buckets = rest.get_buckets()
        for i in range(1, self.backupset.number_of_backups + 1):
            directory_structure[self.backupset.name].append("backup{0}".format(i))
        directory_structure[self.self.backupset.name].append("backup-meta.json")
        for i in range(1, self.backupset.number_of_backups + 1):
            backup_folder_name = "backup{0}".format(i)
            directory_structure[backup_folder_name] = ["{0}-{1}".format(bucket.name, bucket.uuid) for bucket in buckets]
            directory_structure[backup_folder_name].append("plan.json")
            for bucket in buckets:
                # Skip buckets in exclude list or buckets not in include list
                if bucket.name in self.backupset.exclude_buckets:
                    continue
                if self.backupset.include_buckets and bucket.name not in self.backupset.include_buckets:
                    continue
                bucket_folder_name = "{0}/{1}-{2}".format(backup_folder_name, bucket.name, bucket.uuid)
                directory_structure[bucket_folder_name] = []
                if not self.backupset.disable_data:
                    directory_structure[bucket_folder_name].append("data")
                    directory_structure["{0}/{1}".format(bucket_folder_name, "data")] = ["shard_0.fdb", "shard_1.fdb",
                                                                                         "shard_2.fdb", "shard_3.fdb"]
                if not self.backupset.disable_ft_indexes:
                    directory_structure[bucket_folder_name].append("full-text.json")
                if not self.backupset.disable_gsi_indexes:
                    directory_structure[bucket_folder_name].append("gsi.json")
                directory_structure[bucket_folder_name].append("range.json")
                if not self.backupset.disable_views:
                    directory_structure[bucket_folder_name].append("views.json")
        return directory_structure

    def validate_directory(self):
        expected_directory_structure = self.generate_directory()
        actual_directory_structure = self.get_directory()
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
            extra_keys = set(actual) - set(expected)
            for key in extra_keys:
                if key not in extra:
                    extra[key] = []
                extra[key].append(expected[key])
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

    def get_directory(self):
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

    def generate_directory(self):
        json_helper = JSONGenerator("/pytests/enterprise_backup_restore/validation_helpers/"
                                    "directory_structure.json", self.backupset.__dict__)
        json_helper.generate()
        return json_helper.object