import json
import os
import logger

from couchbase_helper.data_analysis_helper import DataAnalyzer, DataAnalysisResultAnalyzer, DataCollector
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class BackupRestoreValidationBase:
    def __init__(self):
        self.log = logger.Logger.get_logger()
        return

    @staticmethod
    def compare_vbucket_stats(prev_vbucket_stats, cur_vbucket_stats, compare_uuid=False, seqno_compare="=="):
        """
        Compares vbucket stats
        :param prev_vbucket_stats: old
        :param cur_vbucket_stats: new
        :param compare_uuid: bool to decide whether to compare uuid or not
        :param seqno_compare: comparator function
        :return: status and message
        """
        compare = "=="
        comp_map = {"abs_high_seqno": {'type': "long", 'operation': seqno_compare},
                    "purge_seqno": {'type': "string", 'operation': compare}}
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

        is_same, summary, result = result_analyzer.analyze_all_result(compare_vbucket_seqnos_result,
                                                                      addedItems=False,
                                                                      deletedItems=False,
                                                                      updatedItems=False)
        if not is_same:
            msg = "{0} is not true".format(summary)
            raise AssertionError(msg)
        return True, "End Verification for vbucket sequence numbers comparison "

    @staticmethod
    def compare_dictionary(expected, actual, is_equal=None, not_equal=None, extra=None, not_present=None):
        """
        Recursively compares the input maps and generates the diff results
        :param expected: expected map
        :param actual: actual map
        :param is_equal: bool which specifies if the maps are same
        :param not_equal: map for unequal elements
        :param extra: map for extra elements
        :param not_present: map for missing elements
        :return: is_equal, not_equal, extra and not_present
        """
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
        for expected_key in list(expected.keys()):
            if expected_key in actual:
                if not isinstance(expected[expected_key], dict) and not isinstance(expected[expected_key], list):
                    if str(expected[expected_key]) != str(actual[expected_key]):
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
                    return BackupRestoreValidationBase.compare_dictionary(expected[expected_key], actual[expected_key],
                                                                          is_equal, not_equal, extra, not_present)
            else:
                if expected_key not in not_present:
                    not_present[expected_key] = {"expected": []}
                not_present[expected_key]["expected"].append(expected[expected_key])
        return is_equal, not_equal, extra, not_present

    @staticmethod
    def compare_dictionary_result_analyser(is_equal, not_equal, extra, not_present, dict_type):
        """
        Analyses the result generated by compare_dictionary
        :param is_equal: bool which specifies if the maps are same
        :param not_equal: map for unequal elements
        :param extra: map for extra elements
        :param not_present: map for missing elements
        :param dict_type: type of input maps
        :return: status and message
        """
        if is_equal:
            return True, "Expected {0} and Actual {1} are equal".format(dict_type, dict_type)
        msg = ""
        if not_equal:
            msg += "{0} not equal. Unequal items: {1}".format(dict_type, not_equal)
        if extra:
            msg += "Extra elements found in the actual {0}. Extra elements: {1}".format(dict_type, extra)
        if not_present:
            msg += "Expected elements not found in the actual {0}. Missing elements: {1}".format(dict_type, not_present)
        return False, msg

    def store_latest(self, servers, buckets, backup_num, backup_validation_path, get_replica=False, mode="memory",
                     backup_type="backup"):
        """
        Collects the latest key value pairs from a cluster after a restore
        :param servers: servers in the cluster
        :param buckets: buckets in the cluster
        :param backup_num: current backup number
        :param backup_validation_path: where to store the data
        :param get_replica: bool to decide if replica data to be copied
        :param mode: where to get the items from - can be "disk" or "memory"
        :param backup_type: backup or restore
        """
        data_collector = DataCollector()
        info, complete_map = data_collector.collect_data(servers, buckets, userId=servers[0].rest_username,
                                                         password=servers[0].rest_password, perNode=False,
                                                         getReplica=get_replica, mode=mode)
        for bucket in list(complete_map.keys()):
            file_name = "{0}-{1}-{2}-{3}.json".format(bucket, "key_value", backup_type, backup_num)
            file_path = os.path.join(backup_validation_path, file_name)
            data = complete_map[bucket]
            for key in data:
                value = data[key]
                value = ",".join(value.split(',')[4:5])
                data[key] = value
            with open(file_path, 'w') as f:
                json.dump(data, f)

    def store_keys(self, servers, buckets, backup_num, backup_validation_path, mode="memory", backup_type="backup"):
        """
        Collects the latest keys from a cluster after a restore
        :param servers: servers in the cluster
        :param buckets: buckets in the cluster
        :param backup_num: current backup number
        :param backup_validation_path: where to store the data
        :param get_replica: bool to decide if replica data to be copied
        :param mode: where to get the items from - can be "disk" or "memory"
        :param backup_type: backup or restore
        """
        data_collector = DataCollector()
        info, complete_map = data_collector.collect_data(servers, buckets, userId=servers[0].rest_username,
                                                         password=servers[0].rest_password, perNode=False,
                                                         mode=mode)
        for bucket in list(complete_map.keys()):
            file_name = "{0}-{1}-{2}.json".format(bucket, "keys", backup_num)
            file_path = os.path.join(backup_validation_path, file_name)
            keys = list(complete_map[bucket].keys())
            with open(file_path, 'w') as f:
                json.dump({"keys": keys}, f)

    def store_range_json(self, buckets, backup_num, backup_validation_path, merge=False):
        rest_conn = RestConnection(self.backupset.cluster_host)
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        for bucket in buckets:
            bucket_stats = rest_conn.get_bucket_json(bucket)
            bucket_uuid = bucket_stats["uuid"]
            from_file_name = self.backupset.directory + "/" + self.backupset.name + "/" + \
                             self.backups[backup_num - 1] + "/" + bucket.name + "-" + bucket_uuid + "/" + "range.json"
            if merge:
                to_file_name = "{0}-{1}-{2}.json".format(bucket, "range", "merge")
            else:
                to_file_name = "{0}-{1}-{2}.json".format(bucket, "range", backup_num)
            to_file_path = os.path.join(backup_validation_path, to_file_name)
            output, error = shell.execute_command("cat " + from_file_name)
            output = [x.strip(' ') for x in output]
            if output:
                output = " ".join(output)
            with open(to_file_path, 'w') as f:
                json.dump(output, f)
        shell.disconnect()
