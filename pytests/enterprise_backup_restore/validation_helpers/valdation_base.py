from enterprise_backup_restore.validation_helpers.directory_validation_helper import DirectoryValidationHelper


class ValidationBase():
    def __init__(self, backupset):
        self.backupset = backupset

    def validate_directory_structure(self):
        directory_validator = DirectoryValidationHelper(self.backupset)
        return directory_validator.validate_directory()

    def validate_json_files(self):
        return True,"Success"

    def validate_backup_file(self):
        return True, "Success"

    def validate_backup(self):
        directory_validator = DirectoryValidationHelper(self.backupset)
        is_equal, not_equal, extra, not_present = directory_validator.validate_directory()
        if not is_equal:
            message =  ""
            if not_equal:
                message += "Files not equal: {0} \n".format(not_equal)
            if extra:
                message += "Files that are extra as compared to expected: {1}\n".format(extra)
            if not_present:
                message += "Files that are not present but are expected: {2}".format(not_present)
            return is_equal, message
        return True, "Backup Validation Success"

    def validate_restore(self):
        return True, "Restore validation success"