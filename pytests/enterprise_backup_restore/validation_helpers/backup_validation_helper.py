from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached


class EnterpriseBackupValidationHelper:
    def __init__(self, backupset, buckets):
        self.backupset = backupset
        self.buckets = buckets
        self.data = {}
        for bucket in buckets:
            self.data[bucket.name] = {}
            self.data[bucket.name]["client"] = VBucketAwareMemcached(
                RestConnection(self.backupset.cluster_host), bucket)
            self.data[bucket.name]["bucket"] = bucket

    def validate_backup(self, previous_state):
        return

    def get_current_data(self, bucket):
        for data in self.data:
            client = data["client"]
            bucket = data["bucket"]
            kvs = bucket.kvs[1]
            v, d = kvs.key_set()
            keys = client.getMulti(v)
            valid_keys = {key: list(keys[key])[2:] for key in keys}
            if "current_data" in data:
                backup_number = "backup{0}".format(data["number_of_backups"])
                data[backup_number] = data["current_data"]
                data["number_of_backups"] += 1
            else:
                backup_number = "backup1"
                data[backup_number] = {}
                data["number_of_backups"] = 1
            data["current_data"] = valid_keys

    def get_backup_data(self):
        return
