import logger
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from couchbase.documentgenerator import BlobGenerator
from couchbase.data_analysis_helper import *
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper

class DataAnalysisTests(BaseTestCase):
    """ Class for defining tests and methods for cluster data analysis """

    def setUp(self):
        super(DataAnalysisTests, self).setUp()
        self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [])
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

    def test_data_analysis_disk_memory_comparison_per_node(self):
        """
            Test to show disk vs memory comparison using cbtransfer functionality
            This will be done per node level comparison
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        self.data_analysis_per_node()

    def test_data_analysis_disk_memory_comparison_all(self):
        """
            Method to show disk vs memory comparison using cbtransfer functionality
            This will be done cluster level comparison
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        self.data_analysis_all()

    def test_data_analysis_active_replica_comparison_all(self):
        """
            Method to show active vs replica comparison using cbtransfer functionality
            This will be done cluster level comparison
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=10, timeout_secs=60)
        self._wait_for_stats_all_buckets(self.servers)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
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
        failovers_stats=self.get_failovers_logs(self.servers,self.buckets, perNode =  True)
        self.compare_failovers_logs(failovers_stats,self.servers,self.buckets, perNode =  True)
        failovers_stats=self.get_failovers_logs(self.servers,self.buckets,perNode =  False)
        self.compare_failovers_logs(failovers_stats,self.servers,self.buckets,perNode =  False)

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
        vbucket_stats=self.get_vbucket_seqnos(self.servers,self.buckets, perNode =  True)
        self.compare_vbucket_seqnos(vbucket_stats,self.servers,self.buckets, perNode =  True)
        vbucket_stats=self.get_vbucket_seqnos(self.servers,self.buckets, perNode =  False)
        self.compare_vbucket_seqnos(vbucket_stats,self.servers,self.buckets,perNode =  False)

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
        vbucket_stats=self.get_vbucket_seqnos(self.servers,self.buckets, perNode =  True)
        logic,output = self.compare_per_node_maps(vbucket_stats)
        self.assertTrue(logic, output)

    def data_analysis_per_node(self):
        """
            Method to do disk vs memory data analysis using cb transfer
            This works per node level
        """
        self.log.info(" Begin Verification for per Node data comparison ")
        data_path="/opt/couchbase/var/lib/couchbase/data"
        info,memory_dataset=self.data_collector.collect_data(self.servers,self.buckets,data_path = None, perNode = True)
        info,disk_dataset=self.data_collector.collect_data(self.servers,self.buckets,data_path = data_path, perNode = True)
        comparison_result=self.data_analyzer.compare_per_node_dataset(info, memory_dataset, disk_dataset)
        logic,summary,output = self.result_analyzer.analyze_per_node_result(comparison_result, deletedItems = False,addedItems = False,updatedItems = False)
        self.log.info(" Verification summary :: {0} ".format(summary))
        self.assertTrue(logic, output)
        self.log.info(" End Verification for per Node data comparison ")

    def data_analysis_all(self):
        """
            Method to do disk vs memory data analysis using cb transfer
            This works at cluster level
        """
        self.log.info(" Begin Verification for data comparison ")
        data_path="/opt/couchbase/var/lib/couchbase/data"
        info,memory_dataset=self.data_collector.collect_data(self.servers,self.buckets,data_path = None, perNode = False)
        info,disk_dataset=self.data_collector.collect_data(self.servers,self.buckets,data_path = data_path, perNode = False)
        comparison_result=self.data_analyzer.compare_all_dataset(info, memory_dataset, disk_dataset)
        logic,summary,output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems = False, addedItems = False, updatedItems = False)
        self.log.info(" Verification summary :: {0} ".format(summary))
        self.assertTrue(logic, output)
        self.log.info(" End Verification for data comparison ")

    def data_analysis_all_replica_active(self):
        """
            Method to do disk vs memory data analysis using cb transfer
            This works at cluster level
        """
        self.log.info(" Begin Verification for data comparison ")
        data_path="/opt/couchbase/var/lib/couchbase/data"
        info,memory_dataset_active=self.data_collector.collect_data(self.servers,self.buckets,data_path = data_path, perNode = False)
        info,memory_dataset_replica=self.data_collector.collect_data(self.servers,self.buckets,data_path = data_path, perNode = False, getReplica = True)
        comparison_result=self.data_analyzer.compare_all_dataset(info, memory_dataset_active, memory_dataset_replica)
        logic,summary,output = self.result_analyzer.analyze_all_result(comparison_result, deletedItems = False, addedItems = False, updatedItems = False)
        self.log.info(" Verification summary :: {0} ".format(summary))
        self.assertTrue(logic, output)
        self.log.info(" End Verification for data comparison ")

    def get_failovers_logs(self,servers,buckets,perNode):
        """
            Method to get failovers logs from a cluster using cbstats
        """
        new_failovers_stats=self.data_collector.collect_failovers_stats(buckets,servers,perNode=perNode)
        return new_failovers_stats

    def compare_failovers_logs(self,prev_failovers_stats,servers,buckets,perNode=False,comp_map=None):
        """
            Method to compare failovers log information to a previously stored value
        """
        self.log.info(" Begin Verification for failovers logs comparison ")
        new_failovers_stats=self.get_failovers_logs(servers,buckets,perNode)
        compare_failovers_result=self.data_analyzer.compare_stats_dataset(prev_failovers_stats,new_failovers_stats,"vbucket_id",comp_map)
        isNotSame,summary,result=self.result_analyzer.analyze_all_result(compare_failovers_result,addedItems = False, deletedItems = False, updatedItems = False)
        self.log.info(" Verification summary for comparing Failovers logs :: {0} ".format(summary))
        self.assertTrue(isNotSame, result)
        self.log.info(" End Verification for failovers logs comparison ")

    def get_vbucket_seqnos(self,servers,buckets,perNode):
        """
            Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats=self.data_collector.collect_vbucket_stats(buckets,servers,collect_vbucket = False,collect_vbucket_seqno = True,collect_vbucket_details = False, perNode = perNode)
        return new_vbucket_stats

    def get_vbucket_seqnos(self,servers,buckets):
        """
            Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(buckets,servers,collect_vbucket = True,collect_vbucket_seqno = False,collect_vbucket_details = False, perNode = True)
        return self.get_active_replica_map(new_vbucket_stats)

    def compare_vbucket_seqnos(self,prev_vbucket_stats,servers,buckets,perNode=False,comp_map=None):
        """
            Method to compare vbucket information to a previously stored value
        """
        self.log.info(" Begin Verification for vbucket sequence numbers comparison ")
        new_vbucket_stats=self.get_vbucket_seqnos(servers,buckets,perNode)
        compare_failover_result=self.data_analyzer.compare_stats_dataset(prev_vbucket_stats,new_vbucket_stats,"vbucket_id",comp_map)
        isNotSame,summary,result=self.result_analyzer.analyze_all_result(compare_failover_result,addedItems = False, deletedItems = False, updatedItems = False)
        self.log.info(" Verification summary for comparing vbucket sequence numbers :: {0} ".format(summary))
        self.assertTrue(isNotSame, result)
        self.log.info(" End Verification for vbucket sequence numbers comparison ")

    def compare_per_node_maps(self,map1):
        map = {}
        nodeMap = {}
        logic = True
        output = ""
        print map1
        for bucket in map1.keys():
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
                    print map1[bucket][node][vbucket].keys()
                    uuid = map1[bucket][node][vbucket]['uuid']
                    if vbucket in map.keys():
                        if  map[vbucket] != uuid:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2}. UUID {3}, Change node {4}. UUID {5}".format(bucket,vbucket,nodeMap[vbucket],map[vbucket],node,uuid)
                    else:
                        map[vbucket] = uuid
                        nodeMap[vbucket] = node
        return logic,output


