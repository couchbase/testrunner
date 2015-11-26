from unittest.util import safe_repr

from couchbase_helper.data_analysis_helper import DataAnalyzer, DataAnalysisResultAnalyzer
from enterprise_backup_restore.validation_helpers.directory_validation_helper import DirectoryValidationHelper
from remote.remote_util import RemoteMachineShellConnection


class ValidationBase():
    def __init__(self, backupset):
        self.backupset = backupset
        self.directory_validator = DirectoryValidationHelper(self.backupset)

    def validate_directory_structure(self):
        directory_validator = DirectoryValidationHelper(self.backupset)
        return directory_validator.validate_directory()

    def validate_json_files(self):
        return True, "Success"

    def validate_backup_file(self):
        return True, "Success"

    def validate_backup(self):
        directory_validator = DirectoryValidationHelper(self.backupset)
        is_equal, not_equal, extra, not_present = directory_validator.validate_directory()
        if not is_equal:
            message = ""
            if not_equal:
                message += "Files not equal: {0} \n".format(not_equal)
            if extra:
                message += "Files that are extra as compared to expected: {0}\n".format(extra)
            if not_present:
                message += "Files that are not present but are expected: {0}".format(not_present)
            return is_equal, message
        return True, "Backup Validation Success"

    def validate_restore(self):
        return True, "Restore validation success"

    def validate_backup_meta(self):
        meta_expected = self.directory_validator.generate_backup_meta_json()
        meta_actual = self.directory_validator.get_backup_meta_json()
        is_equal, not_equal, extra, not_present = self.directory_validator.compare_dictionary(meta_expected,
                                                                                              meta_actual)
        if not is_equal:
            message = ""
            if not_equal:
                message += "Meta_data not equal: {0} \n".format(not_equal)
            if extra:
                message += "Meta_data that are extra as compared to expected: {0}\n".format(extra)
            if not_present:
                message += "Meta_data that are not present but are expected: {0}".format(not_present)
            return is_equal, message
        return is_equal, "Meta data file validation success"

    def validate_backup_create(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -R {0}/{1}".format(self.backupset.dir, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if not o and len(o) != 2 and o[1] != "backup-meta.json":
            return False, "Backup create did not create backup-meta file."
        remote_client.disconnect()
        status, msg = self.validate_backup_meta()
        if status:
            msg += "\nBackup create validation success."
        return status, msg

    def validate_vbucket_stats(self, prev_vbuckets_stats, cur_vbuckets_stats):
        return

    def compare_vbucket_stats(self, prev_vbucket_stats, cur_vbucket_stats, compare_uuid=False):
        compare = "=="
        # if self.withMutationOps:
        #     compare = "<="
        comp_map = {}
        comp_map["abs_high_seqno"] = {'type': "long", 'operation': compare}
        comp_map["purge_seqno"] = {'type': "string", 'operation': compare}
        if compare_uuid:
            comp_map["uuid"] = {'type': "string", 'operation': "=="}
        else:
            comp_map["uuid"] = {'type': "string", 'operation': "filter"}
        data_analyzer = DataAnalyzer()
        result_analyzer = DataAnalysisResultAnalyzer()
        compare_vbucket_seqnos_result = data_analyzer.compare_stats_dataset(prev_vbucket_stats,
                                                                                     cur_vbucket_stats,
                                                                                     "vbucket_id",
                                                                                     comparisonMap=comp_map)

        isNotSame, summary, result = result_analyzer.analyze_all_result(compare_vbucket_seqnos_result,
                                                                             addedItems=False,
                                                                             deletedItems=False,
                                                                             updatedItems=False)
        if not isNotSame:
            msg = self._formatMessage(summary, "%s is not true" % safe_repr(summary))
            raise AssertionError(msg)
        return True, "End Verification for vbucket sequence numbers comparison "
