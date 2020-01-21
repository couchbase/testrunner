import os, time, datetime
import os.path
import uuid
from remote.remote_util import RemoteMachineShellConnection
from lib.mc_bin_client import MemcachedClient
from memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
# constants used in this file only
DELETED_ITEMS_FAILURE_ANALYSIS_FORMAT="\n1) Failure :: Deleted Items :: Expected {0}, Actual {1}"
DELETED_ITEMS_SUCCESS_ANALYSIS_FORMAT="\n1) Success :: Deleted Items "
ADDED_ITEMS_FAILURE_ANALYSIS_FORMAT="\n2) Failure :: Added Items :: Expected {0}, Actual {1}"
ADDED_ITEMS_SUCCESS_ANALYSIS_FORMAT="\n2) Success :: Added Items "
UPDATED_ITEMS_FAILURE_ANALYSIS_FORMAT="\n3) Failure :: Updated Items :: Expected {0}, Actual {1}"
UPDATED_ITEMS_SUCCESS_ANALYSIS_FORMAT="\n3) Success :: Updated Items"
ADD_ITEMS="addedItems"
DELETED_ITEMS="deletedItems"
UPDATED_ITEMS="updatedItems"
LOGICAL_RESULT="logicalresult"
RESULT="result"
MEMCACHED_PORT=11210

class DataAnalysisResultAnalyzer:
    """ Class containing methods to help analyze results for data analysis """

    def analyze_all_result(self,result,deletedItems = False ,addedItems = False,updatedItems = False):
        """
            Method to Generate & analyze result AND output the logical and analysis result
            This works on a bucket level only since we have already taken a union for all nodes
        """
        output=""
        summary=""
        logic=True
        for bucket in list(result.keys()):
            summary+="\n ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
            output+="\n ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
            output+="\n Analyzing for Bucket {0}".format(bucket)
            summary+="\n Analyzing for Bucket {0}".format(bucket)
            logicalresult=result[bucket][LOGICAL_RESULT]
            analysis=result[bucket][RESULT]
            l, o, s = self.analyze_result(analysis,logicalresult, \
                DELETED_ITEMS,deletedItems,DELETED_ITEMS_SUCCESS_ANALYSIS_FORMAT,DELETED_ITEMS_FAILURE_ANALYSIS_FORMAT)
            output+=o
            summary+=s
            logic=l and logic
            l, o, s = self.analyze_result(analysis,logicalresult, \
                ADD_ITEMS,addedItems,ADDED_ITEMS_SUCCESS_ANALYSIS_FORMAT,ADDED_ITEMS_FAILURE_ANALYSIS_FORMAT)
            output+=o
            summary+=s
            logic=l and logic
            l, o, s = self.analyze_result(analysis,logicalresult, \
                UPDATED_ITEMS,updatedItems,UPDATED_ITEMS_SUCCESS_ANALYSIS_FORMAT,UPDATED_ITEMS_FAILURE_ANALYSIS_FORMAT)
            output+=o
            summary+=s
            logic=l and logic
        return logic,summary,output

    def analyze_per_node_result(self,result,deletedItems = False,addedItems = False,updatedItems = False):
        """
            Method to Generate & analyze result AND output the logical and analysis result
            This works on a bucket, node level only
        """
        output=""
        summary=""
        logic=True
        for bucket in list(result.keys()):
            output+="\n ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
            summary+="\n ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
            output+="\n Analyzing for Bucket {0}".format(bucket)
            summary+="\n Analyzing for Bucket {0}".format(bucket)
            for node in list(result[bucket].keys()):
                output+="\n ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
                output+="\n Analyzing for Bucket {0}, node {1}".format(bucket,node)
                summary+="\n Analyzing for Bucket {0}, node {1}".format(bucket,node)
                logicalresult=result[bucket][node][LOGICAL_RESULT]
                analysis=result[bucket][node][RESULT]
                l, o, s = self.analyze_result(analysis,logicalresult, \
                    DELETED_ITEMS,deletedItems,DELETED_ITEMS_SUCCESS_ANALYSIS_FORMAT,DELETED_ITEMS_FAILURE_ANALYSIS_FORMAT)
                output+=o
                summary+=s
                logic=l and logic
                l, o, s = self.analyze_result(analysis,logicalresult, \
                    ADD_ITEMS,addedItems,ADDED_ITEMS_SUCCESS_ANALYSIS_FORMAT,ADDED_ITEMS_FAILURE_ANALYSIS_FORMAT)
                output+=o
                summary+=s
                logic=l and logic
                l, o, s = self.analyze_result(analysis,logicalresult, \
                    UPDATED_ITEMS,updatedItems,UPDATED_ITEMS_SUCCESS_ANALYSIS_FORMAT,UPDATED_ITEMS_FAILURE_ANALYSIS_FORMAT)
                output+=o
                summary+=s
                logic=l and logic
        return logic,summary,output

    def analyze_result(self,analysis,logicalresult,type,actual,successoutputformat,failureoutputformat):
        """ Helper for analyzing result """
        output=""
        summary=""
        logic=False
        lresult=logicalresult[type]
        result=analysis[type]
        if  actual == None:
            return True, "Not Applicable", "Not Applicable"
        if lresult == actual:
                logic=True
                summary+=successoutputformat
                output+=successoutputformat
        else:
            summary+=failureoutputformat.format(actual,lresult)
            output+=failureoutputformat.format(actual,lresult)

        if result != None and type == UPDATED_ITEMS:
            for key in list(result.keys()):
                output+="\n {0} : {1} ".format(key,result[key])
        else:
            for values in result:
                output+="\n {0}".format(values)

        return logic,output,summary

class DataAnalyzer(object):
    """ Class which defines logic for data comparison """

    def analyze_data_distribution(self,sourceMap):
        """
            Method to compare data sets and given as input of two bucket maps

            Paramters:

            sourceMap: input contains csv format of dataset

            Returns:

            Map per Bucket information of data: total, max per vbucket, min  per vbucket, mean, std
        """
        Result = {}
        for bucket in list(sourceMap.keys()):
            info = sourceMap[bucket]
            Result[bucket] = self.find_data_distribution(info)
        return Result

    def compare_all_dataset(self,headerInfo,sourceMap,targetMap,comparisonMap=None):
        """
            Method to compare data sets and given as input of two bucket maps

            Paramters:

            headerInfo: field names of values in input (comma seperated list)
            sourceMap: input 1 used for comparison
            targetMap: input 2 used for comparison
            The input is present in the format {bucket: {vbucket:[{key:value}]}}
            comparisonMap: logical comparison definitions for key, values

            Returns:

            for all buckets get result set as follows
            deletedItems = A-B, addedItems = B-A, updatedItems = Present in A and B, but have different values
            A  =  bucketmap1, B  =  bucketmap2
            The output has two Parts
            1) Logical: deletedItems = (True/False),addedItems = (True/False),updatedItems = (True/False)
                True indicates items present
                False indicates items not present
            2) Result Set: deletedItems,addedItems, updatedItems
               This output is for bucket -> vbucket level
        """
        Result = {}
        for bucket in list(sourceMap.keys()):
            info1 = sourceMap[bucket]
            info2 = targetMap[bucket]
            Result[bucket] = self.compare_data_maps(info1,info2,headerInfo,"key")
        return Result

    def compare_per_node_dataset(self,headerInfo,sourceMap,targetMap,comparisonMap=None):
        """
            Method to compare data sets and given as input of two compare_maps

            Paramters:

            headerInfo: field names of values in input (comma seperated list)
            sourceMap: input 1 used for comparison
            targetMap: input 2 used for comparison
            The input is present in the format {bucket: {vbucket:[{key:value}]}}
            comparisonMap: logical comparison definitions for key, values

            Returns:

            For all buckets get result set as follows:
            deletedItems = A-B, addedItems = B-A, updatedItems = Present in A and B, but have different values
            A  =  bucketmap1, B  =  bucketmap2

            The output has two Parts
            1) Logical: deletedItems = (True/False),addedItems = (True/False),updatedItems = (True/False)
                True indicates items present
                False indicates items not present
            2) Result Set: deletedItems,addedItems, updatedItems
                This output is for bucket -> node -> vbucket level
        """
        Result = {}
        for bucket in list(sourceMap.keys()):
            Result[bucket] = {}
        for bucket in list(sourceMap.keys()):
            for node in list(sourceMap[bucket].keys()):
                info1 = sourceMap[bucket][node]
                info2 = targetMap[bucket][node]
                Result[bucket][node] = self.compare_data_maps(info1,info2,headerInfo,"key")
        return Result

    def compare_stats_dataset(self,bucketmap1,bucketmap2,mainKey,comparisonMap=None):
        """
            Method to compare data sets and given as input of two bucket maps

            Paramters:

            bucketmap1: input 1 used for comparison
            bucketmap2: input 2 used for comparison
            mainKey: key name used in output
            The input is present in the format {bucket: {vbucket:[{key:value}]}}
            comparisonMap: logical comparison definitions for key, values

            Returns:

            for all buckets get result set as follows
            deletedItems = A-B, addedItems = B-A, updatedItems = Present in A and B, but have different values
            A  =  bucketmap1, B  =  bucketmap2
            The output has two Parts
            1) Logical: deletedItems = (True/False),addedItems = (True/False),updatedItems = (True/False)
                True indicates items present
                False indicates items not present
            2) Result Set: deletedItems,addedItems, updatedItems
               This output is for bucket -> vbucket level
        """
        Result = {}
        for bucket in list(bucketmap1.keys()):
            info1 = bucketmap1[bucket]
            info2 = bucketmap2[bucket]
            Result[bucket] = self.compare_maps(info1,info2,mainKey,comparisonMap)
        return Result

    def compare_per_node_stats_dataset(self,bucketmap1,bucketmap2,mainKey="key",comparisonMap=None):
        """
            Method to compare data sets and given as input of two compare_maps

            Paramters:

            bucketmap1: input 1 used for comparison
            bucketmap2: input 2 used for comparison
            mainKey: key name used in output
            the input is present in the format {bucket: {node:{vbucket:[{key:value}]}}
            comparisonMap: logical comparison definitions for key, values

            Returns:

            For all buckets get result set as follows:
            deletedItems = A-B, addedItems = B-A, updatedItems = Present in A and B, but have different values
            A  =  bucketmap1, B  =  bucketmap2

            The output has two Parts
            1) Logical: deletedItems = (True/False),addedItems = (True/False),updatedItems = (True/False)
                True indicates items present
                False indicates items not present
            2) Result Set: deletedItems,addedItems, updatedItems
                This output is for bucket -> node -> vbucket level
        """
        Result = {}
        for bucket in list(bucketmap1.keys()):
            map1 = bucketmap1[bucket]
            map2 = bucketmap2[bucket]
            NodeResult = {}
            if map1 !=  None:
                for node in list(map1.keys()):
                    info1 = map1[node]
                    info2 = map2[node]
                    NodeResult[node] = self.compare_maps(info1,info2,mainKey,comparisonMap)
            Result[bucket] = NodeResult
        return Result

    def compare_maps(self,info1,info2,mainKey="key",comparisonMap=None):
        """ Method to help comparison of stats datasets """
        updatedItemsMap = {}
        deletedItemsList = list(set(info1.keys()) - set(info2.keys()))
        addedItemsList = list(set(info2.keys()) - set(info1.keys()))
        for key in set(info1.keys()) & set(info2.keys()):
            data1 = info1[key]
            data2 = info2[key]
            isNotEqual = False
            reason = {}
            if len(list(data1.keys())) == len(list(data2.keys())):
                for vkey in list(data1.keys()):
                    if comparisonMap != None and vkey in list(comparisonMap.keys()):
                        self.compare_values(data1[vkey],data2[vkey],vkey,reason,comparisonMap[vkey])
                    elif data1[vkey] !=  data2[vkey]:
                        reason[vkey] = "Expected {0} :: Actual {1}".format(data1[vkey],data2[vkey])
            else:
                reason["number of key mismatch"] = "Key Mismatch :: Expected keys {0} \n Actual keys {1}".format(list(data1.keys()),list(data2.keys()))
            if len(reason) > 0:
                updatedItemsMap[key] = reason
        comparisonResult = {DELETED_ITEMS:deletedItemsList,ADD_ITEMS:addedItemsList,UPDATED_ITEMS:updatedItemsMap}
        logicalResult = {DELETED_ITEMS:(len(deletedItemsList) > 0),ADD_ITEMS:(len(addedItemsList) > 0),UPDATED_ITEMS:(len(updatedItemsMap) > 0)}
        return {LOGICAL_RESULT:logicalResult,RESULT:comparisonResult}

    def find_data_distribution(self,info):
        """ Method to extract data distribution from map info """
        distribution_map = {}
        for key in list(info.keys()):
            data = info[key].split(",")
            vbucket = data[len(data) - 1]
            if vbucket in list(distribution_map.keys()):
                distribution_map[vbucket] += 1
            else:
                distribution_map[vbucket] = 1
        array  =  []
        total  = 0
        for key in list(distribution_map.keys()):
            array.append(distribution_map[key])
        max_val  =  max(array)
        min_val =  min(array)
        total = sum(array)
        mean =  total / float(len(array))
        diffSum = 0
        for val in array:
            diffSum += pow((val - mean),2)
        std  =  (diffSum/len(array))**(.5)
        return {"max":max_val,"min":min_val, "total" : total, "mean" : mean, "std" :  std, "map" : distribution_map}

    def compare_analyze_active_replica_vb_nums(self, active_map, replica_map):
        active_maps = {}
        replica_maps = {}
        for bucket in list(active_map.keys()):
            active_maps[bucket] = self.analyze_vb_nums(active_map[bucket])
            replica_maps[bucket] = self.analyze_vb_nums(replica_map[bucket])
        return active_maps,replica_maps

    def analyze_vb_nums(self, map):
        array = []
        for machine in list(map.keys()):
            array.append(map[machine])
        total = sum(array)
        max_val = max(array)
        min_val = min(array)
        mean =  total / float(len(array))
        diffSum = 0
        for val in array:
            diffSum += pow((val - mean),2)
        std  =  (diffSum/len(array))**(.5)
        return {"max":max_val,"min":min_val, "total" : total, "mean" : mean, "std" :  std}

    def compare_data_maps(self,info1,info2,headerInfo,mainKey,comparisonMap=None):
        """ Method to help comparison of datasets """
        updatedItemsMap = {}
        deletedItemsList = list(set(info1.keys()) - set(info2.keys()))
        addedItemsList = list(set(info2.keys()) - set(info1.keys()))
        for key in set(info1.keys()) & set(info2.keys()):
            data1 = info1[key].split(",")
            data2 = info2[key].split(",")
            fields = headerInfo.split(",")
            reason = {}
            if len(data1) == len(data2):
                for i in range(len(data1)):
                    if comparisonMap != None and headerInfo[i] in list(comparisonMap.keys()):
                        self.compare_values(data1[i],data2[i],fields[i],reason,comparisonMap[headerInfo[i]])
                    elif data1[i] !=  data2[i]:
                        reason[fields[i]] = "Expected {0} :: Actual {1}".format(data1[i],data2[i])
            else:
                reason["number of value mismatch"] = "Number of values mismatch :: Expected values {0} \n Actual values {1}".format(data1,data2)
            if len(reason) > 0:
                updatedItemsMap[key] = reason
        comparisonResult = {DELETED_ITEMS:deletedItemsList,ADD_ITEMS:addedItemsList,UPDATED_ITEMS:updatedItemsMap}
        logicalResult = {DELETED_ITEMS:(len(deletedItemsList) > 0),ADD_ITEMS:(len(addedItemsList) > 0),UPDATED_ITEMS:(len(updatedItemsMap) > 0)}
        return {LOGICAL_RESULT:logicalResult,RESULT:comparisonResult}

    def compare_values(self,val1,val2,key,reason,logic):
        """ Helper method to compare values """
        isFail=True
        type=logic["type"]
        operation=logic["operation"]
        val1=self.convert_value(val1,type)
        val2=self.convert_value(val2,type)
        if operation == "filter":
            return
        elif operation == ">=":
            if val1 >= val2:
                isFail=False
        elif operation == "<=":
            if val1 <= val2:
                isFail=False
        elif operation == ">":
            if val1 > val2:
                isFail=False
        elif operation == "<":
            if val1 < val2:
                isFail=False
        elif operation == "==":
            if val1 == val2:
                isFail=False
        elif operation == "!=":
            if val1 != val2:
                isFail=False
        if isFail:
            reason[key] = "Condition Fail:: {0} {1} {2}".format(val1,operation,val2)

    def convert_value(self,val,type):
        """ Helper method to convert to a typical value """
        if val == None:
            return ""
        if type == "int":
            return int(val)
        elif type == "long":
            return int(val)
        elif type == "float":
            return float(val)
        elif type == "string":
            return val

class DataCollector(object):
    """ Helper Class to collect stats and data from clusters """

    def collect_data(self, servers, buckets, userId="Administrator", password="password",
                                             data_path = None, perNode = True,
                                             getReplica = False, mode = "memory"):
        """
            Method to extract all data information from memory or disk using cbtransfer
            The output is organized like { bucket :{ node { document-key : list of values }}}

            Paramters:

            servers: server information
            bucket: bucket information
            userId: user id of cb server
            password: password of cb server
            data_path: data path on servers, if given we will do cbtransfer on files
            perNode: if set we organize data for each bucket per node basis else we take a union

            Returns:

            If perNode flag is set we return data as follows
              {bucket {node { key: value list}}}
            else
              {bucket {key: value list}}
        """
        completeMap = {}
        for bucket in buckets:
            completeMap[bucket.name] = {}
        headerInfo = None
        for server in servers:
            if  mode  ==  "disk" and data_path == None:
                rest = RestConnection(server)
                data_path = rest.get_data_path()
            headerInfo = []
            bucketMap = {}
            if  server.ip == "127.0.0.1":
                headerInfo,bucketMap = self.get_local_data_map_using_cbtransfer(server,
                                                      buckets,
                                                      data_path=data_path,
                                                      userId=userId,
                                                      password=password,
                                                      getReplica = getReplica,
                                                      mode = mode)
            else:
                remote_client = RemoteMachineShellConnection(server)
                headerInfo,bucketMap = remote_client.get_data_map_using_cbtransfer(buckets,
                                                         data_path=data_path,
                                                         userId=userId,
                                                         password=password,
                                                         getReplica = getReplica,
                                                         mode = mode)
                remote_client.disconnect()
            for bucket in list(bucketMap.keys()):
                newMap = self.translateDataFromCSVToMap(0,bucketMap[bucket])
                if perNode:
                    completeMap[bucket][server.ip] = newMap
                else:
                    completeMap[bucket].update(newMap)
        return headerInfo,completeMap

    def collect_vbucket_stats(self, buckets, servers, collect_vbucket = True,
                                                      collect_vbucket_seqno = True,
                                                      collect_vbucket_details = True,
                                                      perNode = True):
        """
            Method to extract the vbuckets stats given by cbstats tool

            Paramters:

            buckets: bucket information
            servers: server information
            collect_vbucket: take vbucket type stats
            collect_vbucket_seqno: take vbucket-seqno type stats
            collect_vbucket_details: take vbucket-details type stats
            perNode: if True collects data per node else takes a union across nodes

            Returns:

            The output can be in two formats

            if we are doing per node data collection
            Vbucket Information :: {bucket { node : [vbucket_seqno {key:value}
                         U vbucket_details {key:value} U vbucket {key:value}]}}

            if we are not doing per node data collection
            Vbucket Information :: {bucket : [vbucket_seqno {key:value}
                          U vbucket_details {key:value} U vbucket {key:value}]}
        """
        bucketMap = {}
        vbucket = []
        vbucket_seqno = []
        vbucket_details = []
        for bucket in buckets:
            bucketMap[bucket.name] = {}
        for bucket in buckets:
            dataMap = {}
            for server in servers:
                map_data = {}
                client = MemcachedClientHelper.direct_client(server, bucket)
                if collect_vbucket:
                    vbucket=client.stats('vbucket')
                    self.createMapVbucket(vbucket,map_data)
                if collect_vbucket_seqno:
                    vbucket_seqno=client.stats('vbucket-seqno')
                    self.createMapVbucket(vbucket_seqno,map_data)
                if collect_vbucket_details:
                    vbucket_details=client.stats('vbucket-details')
                    self.createMapVbucket(vbucket_details,map_data)
                if perNode:
                    dataMap[server.ip] = map_data
                else:
                    dataMap.update(map_data)
            bucketMap[bucket.name] = dataMap
        return bucketMap

    def collect_failovers_stats(self,buckets,servers,perNode = True):
        """
            Method to extract the failovers stats given by cbstats tool

            Paramters:

            buckets: bucket informaiton
            servers: server information
            perNode: if set collect per node information else all

            Returns:

            Failover stats as follows:
            if not collecting per node :: {bucket : [{key:value}]}
            if collecting per node :: {bucket : {node:[{key:value}]}}
        """
        bucketMap = dict()
        for bucket in buckets:
            bucketMap[bucket.name] = dict()
            dataMap = dict()
            for server in servers:
                client = MemcachedClientHelper.direct_client(server, bucket)
                stats = client.stats('failovers')
                map_data = dict()
                num_map = dict()
                for o in list(stats.keys()):
                    tokens = o.split(":")
                    vb = tokens[0]
                    key = tokens[1]
                    value = stats[o].split()
                    num = 99999
                    if len(tokens) == 3:
                        vb = tokens[0]
                        num = int(tokens[1])
                        key = tokens[2]
                    if vb in list(map_data.keys()) and (num == num_map[vb] or num < num_map[vb]):
                        map_data[vb][key] = value[0]
                        num_map[vb] = num
                    elif vb in list(map_data.keys()) and key == "num_entries":
                        map_data[vb][key] = value[0]
                    elif vb not in list(map_data.keys()):
                        m = dict()
                        m[key] = value[0]
                        map_data[vb] = m
                        num_map[vb] = num
                if perNode:
                    dataMap[server.ip] = map_data
                else:
                    dataMap.update(map_data)
            bucketMap[bucket.name] = dataMap
        return bucketMap

    def collect_vbucket_num_stats(self,servers, buckets):
        """
            Method to extract the failovers stats given by cbstats tool

            Paramters:

            buckets: bucket informaiton
            servers: server information

            Returns:

            Failover stats as follows:
            if not collecting per node :: {bucket : [{key:value}]}
            if collecting per node :: {bucket : {node:[{key:value}]}}
        """
        active_bucketMap = {}
        replica_bucketMap = {}
        for bucket in buckets:
            active_map_data = {}
            replica_map_data = {}
            for server in servers:
                client = MemcachedClientHelper.direct_client(server, bucket)
                stats = client.stats('')
                for key in list(stats.keys()):
                    if key == 'vb_active_num':
                        active_map_data[server.ip] = int(stats[key])
                    if key == 'vb_replica_num':
                        replica_map_data[server.ip] = int(stats[key])
            active_bucketMap[bucket.name] = active_map_data
            replica_bucketMap[bucket.name] = replica_map_data
        return active_bucketMap,replica_bucketMap

    def collect_compare_dcp_stats(self, buckets, servers, perNode = True,
                                  stat_name = 'unacked_bytes', compare_value = 0,
                                  flow_control_buffer_size = 20971520, filter_list = []):
        """
            Method to extract the failovers stats given by cbstats tool

            Paramters:

            buckets: bucket informaiton
            servers: server information
            stat_name: stat we are searching to compare
            compare_value: the comparison value to be satisfied

            Returns:

            map of bucket informing if stat matching was satisfied/not satisfied

            example:: unacked_bytes in dcp
        """
        bucketMap = {}
        for bucket in buckets:
            bucketMap[bucket.name] = True
        for bucket in buckets:
            dataMap = {}
            for server in servers:
                client = MemcachedClientHelper.direct_client(server, bucket)
                stats = client.stats('dcp')
                map_data = {}
                for key in list(stats.keys()):
                    filter = False
                    if stat_name in key:
                        for filter_key in filter_list:
                            if filter_key in key:
                                filter = True
                        value = int(stats[key])
                        if not filter:
                            if value != compare_value:
                                if "eq_dcpq:mapreduce_view" in key:
                                    if value >= flow_control_buffer_size:
                                        bucketMap[bucket] = False
                                else:
                                    bucketMap[bucket] = False
        return bucketMap

    def collect_dcp_stats(self, buckets, servers, stat_names = [],
                                extra_key_condition = "replication"):
        """
            Method to extract the failovers stats given by cbstats tool

            Paramters:

            buckets: bucket informaiton
            servers: server information
            stat_names: stats we are searching to compare

            Returns:

            map of bucket informing map[bucket][vbucket id][stat name]

            example:: unacked_bytes in dcp
        """
        bucketMap = {}
        for bucket in buckets:
            bucketMap[bucket.name] = {}
        for bucket in buckets:
            dataMap = {}
            for server in servers:
                stats = MemcachedClientHelper.direct_client(server, bucket).stats('dcp')
                for key in list(stats.keys()):
                    for stat_name in stat_names:
                        if stat_name in key and extra_key_condition in key:
                            value = int(stats[key])
                            tokens = key.split(":")
                            vb_no = int(tokens[len(tokens) - 1].split("_")[1])
                            if vb_no not in dataMap:
                                dataMap[vb_no] = {}
                            dataMap[vb_no][stat_name] = value
            bucketMap[bucket.name] = dataMap
        return bucketMap

    def createMapVbucket(self,details,map_data):
        """ Helper method for vbucket information data collection """
        for o in list(details.keys()):
            tokens = o.split(":")
            if len(tokens) ==  2:
                vb = tokens[0]
                key = tokens[1]
                value = details[o].strip()
                if vb in list(map_data.keys()):
                    map_data[vb][key] = value
                else:
                    m = {}
                    m[key] = value
                    map_data[vb] = m
            elif len(tokens)  ==  1:
                vb = tokens[0]
                value = details[o].strip()
                if vb in list(map_data.keys()):
                    map_data[vb]["state"] = value
                else:
                    m = {}
                    m["state"] = value
                    map_data[vb] = m

    def translateDataFromCSVToMap(self,index,dataInCSV):
        """ Helper method to translate cbtransfer per line data into key: value pairs"""
        bucketMap = {}
        revIdIndex = 5
        for value in dataInCSV:
            values = value.split(",")
            if values[index] in list(bucketMap.keys()):
                prev_revId =  int(bucketMap[values[index]][revIdIndex])
                new_revId = int(values[revIdIndex])
                if prev_revId < new_revId:
                    bucketMap[values[index]] = value
            else:
                bucketMap[values[index]] = value
        return bucketMap

    def get_local_data_map_using_cbtransfer(self, server, buckets, data_path=None,
                                                  userId="Administrator", password="password",
                                                  getReplica=False, mode = "memory"):
        """ Get Local CSV information :: method used when running simple tests only """
        temp_path = "/tmp/"
        replicaOption = ""
        prefix = str(uuid.uuid1())
        fileName = prefix + ".csv"
        if getReplica:
             replicaOption = "  --source-vbucket-state=replica"
        source = "http://" + server.ip + ":"+server.port
        if mode == "disk":
            source = "couchstore-files://" + data_path
        elif mode == "backup":
            source = data_path
            fileName =  ""
        # Initialize Output
        bucketMap = {}
        headerInfo = ""
        # Iterate per bucket and generate maps
        for bucket in buckets:
            if data_path == None:
                options = " -b " + bucket.name + " -u " + userId + " -p " + password + \
                                                                   " --single-node"
            else:
                options = " -b " + bucket.name + " -u " + userId + " -p " + password + \
                                                                   " " + replicaOption
            suffix = "_" + bucket.name + "_N%2FA.csv"
            if mode == "memory" or mode == "backup":
               suffix = "_" + bucket.name + "_" + self.ip + "%3A"+server.port+".csv"
            genFileName = prefix + suffix
            csv_path = temp_path + fileName
            dest_path = temp_path+"/"+genFileName
            destination = "csv:" + csv_path
            bin_path=os.path.abspath(os.path.join(os.getcwd(), os.pardir))+ \
                                                   "/install/bin/cbtransfer"
            command = "{0} {1} {2} {3}".format(bin_path,source,destination,options)
            os.system(command)
            file_existed = os.path.isfile(dest_path)
            if file_existed:
                content = []
                headerInfo = ""
                with open(dest_path) as f:
                    headerInfo = f.readline()
                    content = f.readlines()
                bucketMap[bucket.name] = content
                os.remove(dest_path)
        return headerInfo, bucketMap

    def get_kv_dump_from_backup_file(self, server, cli_command, cmd_ext,
                                     backup_dir, master_key, buckets):
        """
            Extract key value from database file shard_0.sqlite.0
            Return: key, kv store name, status and value
        """
        conn = RemoteMachineShellConnection(server)
        backup_data = {}
        status = False
        now = datetime.datetime.now()
        for bucket in buckets:
            backup_data[bucket.name] = {}
            print("---- Collecting data in backup repo")
            if master_key == "random_keys":
                master_key = ".\{12\}$"
            dump_output = []
            for i in range(0, 1024):
                cmd2 = "{0}cbsqlitedump{1} "\
                       " -f {2}/backup/{3}*/{4}*/data/shard_{5}.sqlite.0 | grep -A 8 'Key: {6}' "\
                                                  .format(cli_command, cmd_ext,\
                                                   backup_dir, now.year, bucket.name,\
                                                   i, master_key)
                output, error = conn.execute_command(cmd2, debug=False)
                if output:
                    """ remove empty element """
                    output = [x.strip(' ') for x in output]
                    """ remove '--' element """
                    output = [ x for x in output if not "--" in x ]
                    key_ids       =  [x.split(":")[1].strip(' ') for x in output[0::9]]
                    key_partition =  [x.split(":")[1].strip(' ') for x in output[1::9]]
                    key_status    =  [x.split(":")[1].strip(' ') for x in output[4::9]]
                    key_value = []
                    for x in output[8::9]:
                        if x.split(":",1)[1].strip(' ').startswith("{"):
                            key_value.append(x.split(":",1)[1].strip())
                        else:
                            key_value.append(x.split(":")[-1].strip(' '))
                    for idx, key in enumerate(key_ids):
                        backup_data[bucket.name][key] = \
                           {"KV store name":key_partition[idx], "Status":key_status[idx],
                            "Value":key_value[idx]}
                    status = True

            if not backup_data[bucket.name]:
                print("Data base of bucket {0} is empty".format(bucket.name))
                return  backup_data, status
            print("---- Done extract data from backup files in backup repo of bucket {0}"\
                                                                   .format(bucket.name))
        return backup_data, status

    def get_views_definition_from_backup_file(self, server, backup_dir, buckets):
        """
            Extract key value from database file shard_0.fdb
            Return: key, kv store name, status and value
            /tmp/entbackup/backup/20*/default-*/views.json
        """
        conn = RemoteMachineShellConnection(server)
        backup_data = {}
        for bucket in buckets:
            backup_data[bucket.name] = {}
            output, error = conn.execute_command("ls %s/backup/20*/%s* "\
                                                        % (backup_dir, bucket.name))
            if "views.json" in output:
                cmd = "cat %s/backup/20*/%s*/views.json" % (backup_dir, bucket.name)
                views_output, error = conn.execute_command(cmd)
                views_output = [x.strip(' ') for x in views_output]
                if views_output:
                    views_output = " ".join(views_output)
                    return views_output
                else:
                    return False
