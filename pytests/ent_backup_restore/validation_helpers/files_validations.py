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
        actual_meta_json = self.get_backup_meta_json()

        # ---- Encryption at Rest (EaR) — Impl Backup_EaR_Implementation.md §5.3.1 ----
        # An EaR repo's backup-meta.json is still valid JSON, but it carries an
        # extra `encryption_options` block (+ is_encrypted:true) and stores the
        # MB-69422 filter fields as base64 ciphertext. Shape-validate the
        # encryption block, then strip the non-deterministic bits so the
        # structural compare below stays meaningful. Mode A (EaR off) is untouched.
        if getattr(self.backupset, "encrypted", False):
            status, msg = self._validate_meta_encryption_options(actual_meta_json)
            if not status:
                return status, msg
            # `is_encrypted` is the encryption gate; the template default is
            # False and the Backupset attr is `encrypted`, so map it in.
            expected_meta_json['is_encrypted'] = True
            # Treat the encrypted filter fields as opaque: drop their values from
            # both sides (they are base64 ciphertext, not exact-matchable).
            for f in ("include_data", "include_buckets", "exclude_data", "exclude_buckets"):
                expected_meta_json.pop(f, None)
                actual_meta_json.pop(f, None)
            # Remove the EaR-only key so it is not flagged as an "extra element".
            actual_meta_json.pop('encryption_options', None)

        is_equal, not_equal, extra, not_present = self.compare_dictionary(expected_meta_json, actual_meta_json)
        return self.compare_dictionary_result_analyser(is_equal, not_equal, extra, not_present, "Backup Meta data json")

    def _validate_meta_encryption_options(self, actual_meta_json):
        """Shape-validate the `encryption_options` block in an EaR repo's
        backup-meta.json (Impl §5.3.1). The crypto fields (salt/key/hash_salt)
        are non-deterministic, so we assert structure, not values.
        :return: (status, message)
        """
        if not actual_meta_json.get('is_encrypted'):
            return False, "Backup Meta data json: is_encrypted is not true for an EaR repo"
        opts = actual_meta_json.get('encryption_options')
        if not opts:
            return False, "Backup Meta data json: encryption_options block missing for an EaR repo"
        if isinstance(opts, list):
            opts = opts[0] if opts else {}
        protection = getattr(self.backupset, "protection", "passphrase")
        if opts.get('type') != protection:
            return False, "Backup Meta data json: encryption_options.type {0} != expected {1}" \
                .format(opts.get('type'), protection)
        expected_algo = getattr(self.backupset, "encryption_algo", None) or "AES256GCM"
        if opts.get('encrypt_algo') and opts.get('encrypt_algo') != expected_algo:
            return False, "Backup Meta data json: encrypt_algo {0} != expected {1}" \
                .format(opts.get('encrypt_algo'), expected_algo)
        if protection == "passphrase" and not opts.get('derivation_algo'):
            return False, "Backup Meta data json: derivation_algo missing for passphrase protection"
        return True, "encryption_options shape validated"
