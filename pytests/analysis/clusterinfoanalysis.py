import logger
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.data_analysis_helper import *
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper


class DataAnalysisTests(BaseTestCase):
    """ Class for defining tests and methods for cluster data analysis """

    def setUp(self):
        super(DataAnalysisTests, self).setUp()
        self.services = self.services = self.get_services(self.servers, self.services_init)
        self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [], services = self.services)
        self.data_collector=DataCollector()
        self.data_analyzer=DataAnalyzer()
        credentials = self.input.membase_settings
        self.result_analyzer=DataAnalysisResultAnalyzer()
        self.log = logger.Logger.get_logger()
        credentials = self.input.membase_settings
        self.log.info("==============  DataAnalysisTests setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))
    def tearDown(self):
        super(DataAnalysisTests, self).tearDown()

    def test_check_http_access_log(self):
        """
            Test to check http access log
        """
        rest = RestConnection(self.master)
        log_path = rest.get_data_path().replace("data", "logs")
        remote_client = RemoteMachineShellConnection(self.master)
        output = remote_client.read_remote_file(log_path, "http_access.log")
        logic = self.verify_http_acesslog(output, [self.master.ip])
        self.assertTrue(logic, "search string not present in http_access.log")

    def test_node_services(self):
        """
            Test node services information
        """
        self.get_services_map()
        for key in self.services_map:
            self.log.info("{0} : {1}".format(key, self.services_map[key]))
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.log.info("node with n1ql service {0}:{1}".format(server.ip, server.port))
        servers = self.get_nodes_from_services_map(service_type = "kv", get_all_nodes = True)
        for server in servers:
            self.log.info("node with data service {0}:{1}".format(server.ip, server.port))

    def test_timeline_analysis_metadata(self):
        """
            Test comparison of meta data based on time
        """
        self.std = self.input.param("std", 1.0)
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        meta_data_store = self.get_meta_data_set_all(self.master)
        self.data_meta_data_analysis(self.master, meta_data_store)
        self.data_active_and_replica_analysis(self.master)

    def test_data_distribution(self):
        """
            Test to check for data distribution at vbucket level
        """
        self.std = self.input.param("std", 1.0)
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        self.data_distribution_analysis(self.num_items, self.std)

    def test_data_vb_num_distribution(self):
        """
            Test to check vbucket distribution for active and replica items
        """
        self.std = self.input.param("std", 1.0)
        self.total_vbuckets = self.input.param("total_vbuckets", 1.0)
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        self.vb_distribution_check(self.total_vbuckets, self.std)

    def test_data_analysis_disk_memory_comparison_all(self):
        """
            Method to show disk+memory vs memory comparison using cbtransfer functionality
            This will be done cluster level comparison
        """
        create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        update = BlobGenerator('loadOne', 'loadOne-', self.value_size, end=(self.num_items // 2 - 1))
        self._load_all_buckets(self.master, create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self.num_items=self.num_items - self.num_items // 2
        self._verify_stats_all_buckets(self.servers, timeout=120)
        self._wait_for_stats_all_buckets(self.servers)
        self._async_load_all_buckets(self.master, update, "update", 0)
        self._verify_stats_all_buckets(self.servers, timeout=120)
        self._wait_for_stats_all_buckets(self.servers)
        self.sleep(60)
        self.data_analysis_all()
        self.verify_unacked_bytes_all_buckets()

    def test_data_analysis_active_replica_comparison_all(self):
        """
            Method to show active vs replica comparison using cbtransfer functionality
            This will be done cluster level comparison
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        self.data_analysis_all_replica_active()

    def test_failoverlogs_extraction_equals(self):
        """
            Test to show usage of failover log collection via api
            and than comparison and running the logic for analysis
            This is done for cluster and node level as well
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        failovers_stats=self.get_failovers_logs(self.servers, self.buckets, perNode =  True)
        self.compare_failovers_logs(failovers_stats, self.servers, self.buckets, perNode =  True)
        failovers_stats=self.get_failovers_logs(self.servers, self.buckets, perNode =  False)
        self.compare_failovers_logs(failovers_stats, self.servers, self.buckets, perNode =  False)

    def test_vbucket_extraction_equals(self):
        """
            Test to show usage of vbucket information collection via api
            and than comparison and running the logic for analysis
            This is done for cluster and node level as well
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        vbucket_stats=self.get_vbucket_seqnos(self.servers, self.buckets, perNode =  True)
        self.compare_vbucket_seqnos(vbucket_stats, self.servers, self.buckets, perNode =  True)
        vbucket_stats=self.get_vbucket_seqnos(self.servers, self.buckets, perNode =  False)
        self.compare_vbucket_seqnos(vbucket_stats, self.servers, self.buckets, perNode =  False)

    def test_vbucket_uuid(self):
        """
            Test to show usage of vbucket information collection via api
            and than comparison and running the logic for analysis
            This is done for cluster and node level as well
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        vbucket_stats=self.get_vbucket_seqnos(self.servers, self.buckets, perNode =  True)
        logic, output = self.compare_per_node_maps(vbucket_stats)
        self.assertTrue(logic, output)

    def data_distribution_analysis(self, total_items = 0, std = 1.0):
        """
            Method to check data analysis
        """
        self.log.info(" Begin Verification for per Node data comparison")
        data, map=self.get_data_set_with_data_distribution_all(self.servers, self.buckets)
        for bucket in map:
            map1 = map[bucket]
            self.log.info(" Verification summary :: {0} ".format(map1))
            self.assertTrue(map1["total"] == total_items, "item do not match")
            self.assertTrue(map1["std"] >= 0.0 and map1["std"] < std, "std test failed, not within [0,1]")

    def vb_distribution_check(self, total_vbuckets = 0, std = 1.0):
        """
            Method to check data analysis
        """
        self.log.info(" Begin Verification for distribution analysis")
        self.vb_distribution_analysis(servers = self.servers, buckets = self.buckets, std = 1.0, total_vbuckets = self.total_vbuckets)

    def data_analysis_all(self):
        """
            Method to do disk vs memory data analysis using cb transfer
            This works at cluster level
            We do active data vs disk+memory comparison, replica data  vs disk+memory comparison
        """
        self.log.info(" Begin Verification for data comparison ")
        info, memory_dataset=self.data_collector.collect_data(self.servers, self.buckets, data_path = None, perNode = False, mode = "memory")
        info, memory_dataset=self.data_collector.collect_data(self.servers, self.buckets, data_path = None, perNode = False, mode = "memory")
        info, disk_dataset_active=self.data_collector.collect_data(self.servers, self.buckets, data_path = None, perNode = False, mode= "disk")
        info, disk_dataset_replica=self.data_collector.collect_data(self.servers, self.buckets, data_path = None, perNode = False, getReplica = True, mode = "disk")
        active_comparison_result=self.data_analyzer.compare_all_dataset(info, memory_dataset, disk_dataset_active)
        replica_comparison_result=self.data_analyzer.compare_all_dataset(info, memory_dataset, disk_dataset_replica)
        comparison_result=self.data_analyzer.compare_all_dataset(info, disk_dataset_active, disk_dataset_replica)
        logic, summary, output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems = False, addedItems = False, updatedItems = False)
        active_logic, active_summary, active_output = self.result_analyzer.analyze_all_result(active_comparison_result, deletedItems = False, addedItems = False, updatedItems = False)
        replica_logic, replica_summary, replica_output = self.result_analyzer.analyze_all_result(replica_comparison_result, deletedItems = False, addedItems = False, updatedItems = False)
        total_logic = logic and active_logic and replica_logic
        total_summary = " active vs replica  {0} \n memory vs active {1} \n memory vs replica {2}".format(summary, active_summary, replica_summary)
        total_output = " active vs replica  {0} \n memory vs active {1} \n memory vs replica {2}".format(output, active_output, replica_output)
        self.assertTrue(total_logic, total_summary+"\n"+total_output)
        self.log.info(" End Verification for data comparison ")


    def data_analysis_all_replica_active(self):
        """
            Method to do disk vs memory data analysis using cb transfer
            This works at cluster level
        """
        self.log.info(" Begin Verification for data comparison ")
        data_path="/opt/couchbase/var/lib/couchbase/data"
        info, memory_dataset_active=self.data_collector.collect_data(self.servers, self.buckets, data_path = data_path, perNode = False)
        info, memory_dataset_replica=self.data_collector.collect_data(self.servers, self.buckets, data_path = data_path, perNode = False, getReplica = True)
        comparison_result=self.data_analyzer.compare_all_dataset(info, memory_dataset_active, memory_dataset_replica)
        logic, summary, output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems = False, addedItems = False, updatedItems = False)
        self.log.info(" Verification summary :: {0} ".format(summary))
        self.assertTrue(logic, output)
        self.log.info(" End Verification for data comparison ")

    def get_failovers_logs(self, servers, buckets, perNode):
        """
            Method to get failovers logs from a cluster using cbstats
        """
        new_failovers_stats=self.data_collector.collect_failovers_stats(buckets, servers, perNode=perNode)
        return new_failovers_stats

    def compare_failovers_logs(self,prev_failovers_stats,servers,buckets,perNode=False,comp_map=None):
        """
            Method to compare failovers log information to a previously stored value
        """
        self.log.info(" Begin Verification for failovers logs comparison ")
        new_failovers_stats=self.get_failovers_logs(servers, buckets, perNode)
        compare_failovers_result=self.data_analyzer.compare_stats_dataset(prev_failovers_stats, new_failovers_stats, "vbucket_id", comp_map)
        isNotSame, summary, result=self.result_analyzer.analyze_all_result(compare_failovers_result, addedItems = False, deletedItems = False, updatedItems = False)
        self.log.info(" Verification summary for comparing Failovers logs :: {0} ".format(summary))
        self.assertTrue(isNotSame, result)
        self.log.info(" End Verification for failovers logs comparison ")

    def get_vbucket_seqnos(self, servers, buckets, perNode):
        """
            Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats=self.data_collector.collect_vbucket_stats(buckets, servers, collect_vbucket = False, collect_vbucket_seqno = True, collect_vbucket_details = False, perNode = perNode)
        return new_vbucket_stats

    def get_vbucket_seqnos(self, servers, buckets):
        """
            Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(buckets, servers, collect_vbucket = True, collect_vbucket_seqno = False, collect_vbucket_details = False, perNode = True)
        return self.get_active_replica_map(new_vbucket_stats)

    def compare_vbucket_seqnos(self,prev_vbucket_stats,servers,buckets,perNode=False,comp_map=None):
        """
            Method to compare vbucket information to a previously stored value
        """
        self.log.info(" Begin Verification for vbucket sequence numbers comparison ")
        new_vbucket_stats=self.get_vbucket_seqnos(servers, buckets, perNode)
        compare_failover_result=self.data_analyzer.compare_stats_dataset(prev_vbucket_stats, new_vbucket_stats, "vbucket_id", comp_map)
        isNotSame, summary, result=self.result_analyzer.analyze_all_result(compare_failover_result, addedItems = False, deletedItems = False, updatedItems = False)
        self.log.info(" Verification summary for comparing vbucket sequence numbers :: {0} ".format(summary))
        self.assertTrue(isNotSame, result)
        self.log.info(" End Verification for vbucket sequence numbers comparison ")

    def verify_http_acesslog(self,logs=[],verification_string = []):
        logic = True
        for check_string in verification_string:
            check_presence= False
            for val in logs:
                if check_string in val:
                    check_presence = True
            logic = logic and check_presence
        return logic

    def compare_per_node_maps(self, map1):
        map = {}
        nodeMap = {}
        logic = True
        output = ""
        print(map1)
        for bucket in list(map1.keys()):
            for node in list(map1[bucket].keys()):
                for vbucket in list(map1[bucket][node].keys()):
                    print(list(map1[bucket][node][vbucket].keys()))
                    uuid = map1[bucket][node][vbucket]['uuid']
                    if vbucket in list(map.keys()):
                        if  map[vbucket] != uuid:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. UUID {3}, Change node {4}. UUID {5}".format(bucket, vbucket, nodeMap[vbucket], map[vbucket], node, uuid)
                    else:
                        map[vbucket] = uuid
                        nodeMap[vbucket] = node
        return logic, output


