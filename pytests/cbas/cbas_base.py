import json
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from lib.couchbase_helper.analytics_helper import AnalyticsHelper
from couchbase_helper.documentgenerator import DocumentGenerator
import urllib.request, urllib.parse, urllib.error
from lib.membase.api.rest_client import RestConnection, RestHelper, Bucket
from lib.couchbase_helper.cluster import *
from lib.membase.helper.bucket_helper import BucketOperationHelper
from testconstants import FTS_QUOTA, CBAS_QUOTA, INDEX_QUOTA, MIN_KV_QUOTA
from threading import Thread
import threading
from security.rbac_base import RbacBase


class CBASBaseTest(BaseTestCase):
    def setUp(self, add_defualt_cbas_node = True):
        self.log = logger.Logger.get_logger()
        if self._testMethodDoc:
            self.log.info("\n\nStarting Test: %s \n%s"%(self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("\n\nStarting Test: %s"%(self._testMethodName))
        super(CBASBaseTest, self).setUp()
        self.cbas_node = self.input.cbas
        self.cbas_servers = []
        self.kv_servers = []
 
        for server in self.servers:
            if "cbas" in server.services:
                self.cbas_servers.append(server)
            if "kv" in server.services:
                self.kv_servers.append(server)
        
        self.analytics_helper = AnalyticsHelper()
        self._cb_cluster = self.cluster
        self.travel_sample_docs_count = 31591
        self.beer_sample_docs_count = 7303
        invalid_ip = '10.111.151.109'
        self.cb_bucket_name = self.input.param('cb_bucket_name', 'travel-sample')
        self.cbas_bucket_name = self.input.param('cbas_bucket_name', 'travel')
        self.cb_bucket_password = self.input.param('cb_bucket_password', None)
        self.expected_error = self.input.param("error", None)
        if self.expected_error:
            self.expected_error = self.expected_error.replace("INVALID_IP", invalid_ip)
            self.expected_error = self.expected_error.replace("PORT", self.master.port)
        self.cb_server_ip = self.input.param("cb_server_ip", None)
        self.cb_server_ip = self.cb_server_ip.replace('INVALID_IP', invalid_ip) if self.cb_server_ip is not None else None
        self.cbas_dataset_name = self.input.param("cbas_dataset_name", 'travel_ds')
        self.cbas_bucket_name_invalid = self.input.param('cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.cbas_dataset2_name = self.input.param('cbas_dataset2_name', None)
        self.skip_create_dataset = self.input.param('skip_create_dataset', False)
        self.disconnect_if_connected = self.input.param('disconnect_if_connected', False)
        self.cbas_dataset_name_invalid = self.input.param('cbas_dataset_name_invalid', self.cbas_dataset_name)
        self.skip_drop_connection = self.input.param('skip_drop_connection', False)
        self.skip_drop_dataset = self.input.param('skip_drop_dataset', False)
        self.query_id = self.input.param('query_id', None)
        self.mode = self.input.param('mode', None)
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size', 100)
        self.compiler_param = self.input.param('compiler_param', None)
        self.compiler_param_val = self.input.param('compiler_param_val', None)
        self.expect_reject = self.input.param('expect_reject', False)
        self.expect_failure = self.input.param('expect_failure', False)
        self.index_name = self.input.param('index_name', None)
        self.index_fields = self.input.param('index_fields', None)
        if self.index_fields:
            self.index_fields = self.index_fields.split("-")
        self.otpNodes = []

        self.rest = RestConnection(self.master)
        
        self.log.info("Setting the min possible memory quota so that adding mode nodes to the cluster wouldn't be a problem.")
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=MIN_KV_QUOTA)
        self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
        self.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=CBAS_QUOTA)
        
        # Drop any existing buckets and datasets
        if self.cbas_node:
            self.cleanup_cbas()
                    
        if not self.cbas_node and len(self.cbas_servers)>=1:
            self.cbas_node = self.cbas_servers[0]
            if "cbas" in self.master.services:
                self.cleanup_cbas()
            if add_defualt_cbas_node:
                if self.master.ip != self.cbas_node.ip:
                    self.otpNodes.append(self.add_node(self.cbas_node))
                else:
                    self.otpNodes = self.rest.node_statuses()
                ''' This cbas cleanup is actually not needed.
                    When a node is added to the cluster, it is automatically cleaned-up.'''
                self.cleanup_cbas()
                self.cbas_servers.remove(self.cbas_node)
        
        self.log.info("==============  CBAS_BASE setup was finished for test #{0} {1} ==============" \
                          .format(self.case_number, self._testMethodName))
    
    def create_default_bucket(self):
        node_info = self.rest.get_nodes_self()
        if node_info.memoryQuota and int(node_info.memoryQuota) > 0 :
            ram_available = node_info.memoryQuota
            
        self.bucket_size = ram_available - 1
        default_params=self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                         replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                         enable_replica_index=self.enable_replica_index,
                                                         eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_default_bucket(default_params)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                   num_replicas=self.num_replicas, bucket_size=self.bucket_size,
                                   eviction_policy=self.eviction_policy, lww=self.lww,
                                   type=self.bucket_type))
        if self.enable_time_sync:
            self._set_time_sync_on_buckets( ['default'] )
    
    def add_all_cbas_node_then_rebalance(self):
        if len(self.cbas_servers)>=1:
            for server in self.cbas_servers:
                '''This is the case when master node is running cbas service as well'''
                if self.master.ip != server.ip:
                    self.otpNodes.append(self.rest.add_node(user=server.rest_username,
                                               password=server.rest_password,
                                               remoteIp=server.ip,
                                               port=8091,
                                               services=server.services.split(",")))
    
            self.rebalance()
        else:
            self.log.info("No CBAS server to add in cluster")

        return self.otpNodes
    
    def rebalance(self, wait_for_completion=True):
        nodes = self.rest.node_statuses()
        started = self.rest.rebalance(otpNodes=[node.id for node in nodes])
        if started and wait_for_completion:
            result = self.rest.monitorRebalance()
            self.assertTrue(result, "Rebalance operation failed after adding %s cbas nodes,"%self.cbas_servers)
            self.log.info("successfully rebalanced cluster {0}".format(result))
        else:
            self.assertTrue(started, "Rebalance operation started and in progress,"%self.cbas_servers)
        
    def remove_all_cbas_node_then_rebalance(self,cbas_otpnodes=None, rebalance=True ):
        return self.remove_node(cbas_otpnodes, rebalance) 
        
    def add_node(self, node=None, services=None, rebalance=True, wait_for_rebalance_completion=True):
        if not node:
            self.fail("There is no node to add to cluster.")
        if not services:
            services = node.services.split(",")        
        otpnode = self.rest.add_node(user=node.rest_username,
                               password=node.rest_password,
                               remoteIp=node.ip,
                               port=8091,
                               services=services
                               )
        if rebalance:
            self.rebalance(wait_for_completion=wait_for_rebalance_completion)
        return otpnode
    
    def remove_node(self,otpnode=None, wait_for_rebalance=True):
        nodes = self.rest.node_statuses()
        '''This is the case when master node is running cbas service as well'''
        if len(nodes) <= len(otpnode):
            return
        
        helper = RestHelper(self.rest)
        try:
            removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                              ejectedNodes=[node.id for node in otpnode],
                                              wait_for_rebalance=wait_for_rebalance)
        except Exception as e:
            self.sleep(5, "First time rebalance failed on Removal. Wait and try again. THIS IS A BUG.")
            removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                              ejectedNodes=[node.id for node in otpnode],
                                              wait_for_rebalance=wait_for_rebalance)
        if wait_for_rebalance:
            self.assertTrue(removed, "Rebalance operation failed while removing %s,"%otpnode)
        
    def load_sample_buckets(self, servers=None, bucketName=None, total_items=None):
        """ Load the specified sample bucket in Couchbase """
        self.assertTrue(self.rest.load_sample(bucketName), "Failure while loading sample bucket: %s"%bucketName)
        
        """ check for load data into travel-sample bucket """
        if total_items:
            import time
            end_time = time.time() + 600
            while time.time() < end_time:
                self.sleep(10)
                num_actual = 0
                if not servers:
                    num_actual = self.get_item_count(self.master, bucketName)
                else:
                    for server in servers:
                        if "kv" in server.services:
                            num_actual += self.get_item_count(server, bucketName)
                if int(num_actual) == total_items:
                    self.log.info("%s items are loaded in the %s bucket" %(num_actual, bucketName))
                    break
                self.log.info("%s items are loaded in the %s bucket" %(num_actual, bucketName))
            if int(num_actual) != total_items:
                return False
        else:
            self.sleep(120)

        return True
    
    def create_bucket_on_cbas(self, cbas_bucket_name, cb_bucket_name,
                              cb_server_ip=None,
                              validate_error_msg=False,
                              username = None, password = None):
        """
        Creates a bucket on CBAS
        """
        return True
#         if cb_server_ip:
#             cmd_create_bucket = "create bucket " + cbas_bucket_name + " with {\"name\":\"" + cb_bucket_name + "\",\"nodes\":\"" + cb_server_ip + "\"};"
#         else:
#             '''DP3 doesn't need to specify cb server ip as cbas node is part of the cluster.'''
#             cmd_create_bucket = "create bucket " + cbas_bucket_name + " with {\"name\":\"" + cb_bucket_name + "\"};"
#         status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
#             cmd_create_bucket,username=username, password=password)
# 
#         if validate_error_msg:
#             return self.validate_error_in_response(status, errors)
#         else:
#             if status != "success":
#                 return False
#             else:
#                 return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value = None,
                                 validate_error_msg=False, username = None,
                                 password = None):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        cmd_create_dataset = "create dataset {0} on {1};".format(
            cbas_dataset_name, cbas_bucket_name)
        if where_field and where_value:
            cmd_create_dataset = "create dataset {0} on {1} WHERE `{2}`=\"{3}\";".format(
                cbas_dataset_name, cbas_bucket_name, where_field, where_value)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_create_dataset, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def connect_to_bucket(self, cbas_bucket_name, cb_bucket_password=None,
                          validate_error_msg=False, cb_bucket_username="Administrator",
                          username=None, password=None):
        """
        Connects to a CBAS bucket
        """
#         if cb_bucket_username and cb_bucket_password:
#             cmd_connect_bucket = "connect bucket " + cbas_bucket_name + " with {\"username\":\"" + cb_bucket_username + "\",\"password\":\"" + cb_bucket_password + "\"};"
#         else:
#             '''DP3 doesn't need to specify Username/Password as cbas node is part of the cluster.'''
#             cmd_connect_bucket = "connect bucket " + cbas_bucket_name
        cmd_connect_bucket = "connect link Local;"
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_connect_bucket, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def disconnect_from_bucket(self, cbas_bucket_name,
                               disconnect_if_connected=False,
                               validate_error_msg=False, username=None,
                               password=None):
        """
        Disconnects from a CBAS bucket
        """
#         if disconnect_if_connected:
#             cmd_disconnect_bucket = "disconnect bucket {0} if connected;".format(
#                 cbas_bucket_name)
#         else:
#             cmd_disconnect_bucket = "disconnect bucket {0};".format(
#                 cbas_bucket_name)
        cmd_disconnect_bucket = "disconnect link Local;"
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_disconnect_bucket, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_dataset(self, cbas_dataset_name, validate_error_msg=False,
                     username=None, password=None):
        """
        Drop dataset from CBAS
        """
        cmd_drop_dataset = "drop dataset {0};".format(cbas_dataset_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_drop_dataset, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_cbas_bucket(self, cbas_bucket_name, validate_error_msg=False, username=None, password=None):
        """
        Drop a CBAS bucket
        """
#         cmd_drop_bucket = "drop bucket {0};".format(cbas_bucket_name)
#         status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
#             cmd_drop_bucket, username=username, password=password)
#         if validate_error_msg:
#             return self.validate_error_in_response(status, errors)
#         else:
#             if status != "success":
#                 return False
#             else:
        return True

    def wait_for_ingestion_complete(self, cbas_dataset_names, num_items, timeout=300):
        
        total_items = 0
        for ds_name in cbas_dataset_names:
            total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]
        
        counter = 0
        while (timeout > counter):
            self.log.info("Total items in CB Bucket to be ingested in CBAS datasets %s"%num_items)
            if num_items <= total_items:
                self.log.info("Data ingestion completed in %s seconds."%counter)
                return True
            else:
                self.sleep(2)
                total_items = 0
                for ds_name in cbas_dataset_names:
                    total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]
                counter += 2
                
        return False
    
    def execute_statement_on_cbas(self, statement, server, mode=None):
        """
        Executes a statement on CBAS using the REST API through curl command
        """
        shell = RemoteMachineShellConnection(server)
        if mode:
            output, error = shell.execute_command(
                """curl -s --data pretty=true --data mode={2} --data-urlencode 'statement={1}' http://{0}:8095/analytics/service -v -u {3}:{4}""".format(
                    self.cbas_node.ip, statement, mode, self.cbas_node.rest_username, self.cbas_node.rest_password))
        else:
            output, error = shell.execute_command(
            """curl -s --data pretty=true --data-urlencode 'statement={1}' http://{0}:8095/analytics/service -v -u {2}:{3}""".format(
                self.cbas_node.ip, statement, self.cbas_node.rest_username, self.cbas_node.rest_password))
        response = ""
        for line in output:
            response = response + line
        response = json.loads(response)
        self.log.info(response)
        shell.disconnect()

        if "errors" in response:
            errors = response["errors"]
        else:
            errors = None

        if "results" in response:
            results = response["results"]
        else:
            results = None

        if "handle" in response:
            handle = response["handle"]
        else:
            handle = None

        return response["status"], response["metrics"], errors, results, handle

    def retrieve_result_using_handle(self, server, handle):
        """
        Retrieves result from the /analytics/results endpoint
        """
        shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(handle,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        shell.disconnect()

        return response

    def retrieve_request_status_using_handle(self, server, handle):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -v {0} -u {1}:{2}""".format(handle,
                                                self.cbas_node.rest_username,
                                                self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        shell.disconnect()

        status = ""
        handle = ""
        if 'status' in response:
            status = response['status']
        if 'handle' in response:
            handle = response['handle']

        self.log.info("status=%s, handle=%s"%(status, handle))
        return status, handle

    def validate_error_in_response(self, status, errors):
        """
        Validates if the error message in the response is same as the expected one.
        """
        if status != "success":
            actual_error = errors[0]["msg"]
            if self.expected_error not in actual_error:
                return False
            else:
                return True
        return False
    
    def cleanup_cbas(self):
        """
        Drops all connections, datasets and buckets from CBAS
        """
        try:
            # Disconnect from all connected buckets
            cmd_get_buckets = "select Name from Metadata.`Bucket`;"
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                cmd_get_buckets)
            if (results != None) & (len(results) > 0):
                for row in results:
                    self.disconnect_from_bucket(row['Name'],
                                                disconnect_if_connected=True)
                    self.log.info(
                        "********* Disconnected all buckets *********")
            else:
                self.log.info("********* No buckets to disconnect *********")

            # Drop all datasets
            cmd_get_datasets = "select DatasetName from Metadata.`Dataset` where DataverseName != \"Metadata\";"
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                cmd_get_datasets)
            if (results != None) & (len(results) > 0):
                for row in results:
                    self.drop_dataset(row['DatasetName'])
                    self.log.info("********* Dropped all datasets *********")
            else:
                self.log.info("********* No datasets to drop *********")

            # Drop all buckets
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                cmd_get_buckets)
            if (results != None) & (len(results) > 0):
                for row in results:
                    self.drop_cbas_bucket(row['Name'])
                    self.log.info("********* Dropped all buckets *********")
            else:
                self.log.info("********* No buckets to drop *********")
        except Exception as e:
            self.log.info(str(e))

    def perform_doc_ops_in_all_cb_buckets(self, num_items, operation, start_key=0, end_key=1000):
        """
        Create/Update/Delete docs in all cb buckets
        :param num_items: No. of items to be created/deleted/updated
        :param operation: String - "create","update","delete"
        :param start_key: Doc Key to start the operation with
        :param end_key: Doc Key to end the operation with
        :return:
        """
        age = list(range(70))
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        template = '{{ "number": {0}, "first_name": "{1}" , "mutated":0}}'
        gen_load = DocumentGenerator('test_docs', template, age, first,
                                     start=start_key, end=end_key)
        self.log.info("%s %s documents..." % (operation, num_items))
        try:
            self._load_all_buckets(self.master, gen_load, operation, 0)
            self._verify_stats_all_buckets(self.input.servers)
        except Exception as e:
            self.log.info(str(e))

    def get_num_items_in_cbas_dataset(self, dataset_name):
        """
        Gets the count of docs in the cbas dataset
        """
        total_items = -1
        mutated_items = -1
        cmd_get_num_items = "select count(*) from %s;" % dataset_name
        cmd_get_num_mutated_items = "select count(*) from %s where mutated>0;" % dataset_name

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_get_num_items)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.info("No. of items in CBAS dataset {0} : {1}".format(dataset_name, results[0]['$1']))
            total_items = results[0]['$1']

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_get_num_mutated_items)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.info("No. of items mutated in CBAS dataset {0} : {1}".format(dataset_name, results[0]['$1']))
            mutated_items = results[0]['$1']

        return total_items, mutated_items

    def validate_cbas_dataset_items_count(self, dataset_name, expected_count, expected_mutated_count=0):
        """
        Compares the count of CBAS dataset total and mutated items with the expected values.
        """
        count, mutated_count = self.get_num_items_in_cbas_dataset(dataset_name)
        tries = 12
        if expected_mutated_count:
            while (count != expected_count or mutated_count != expected_mutated_count) and tries > 0:
                self.sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(dataset_name)
                tries -= 1
        else :
            while count != expected_count and tries > 0:
                self.sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(
                    dataset_name)
                tries -= 1

        self.log.info("Expected Count: %s, Actual Count: %s" % (expected_count, count))
        self.log.info("Expected Mutated Count: %s, Actual Mutated Count: %s" % (expected_mutated_count, mutated_count))

        if count != expected_count:
            return False
        elif mutated_count == expected_mutated_count:
            return True
        else:
            return False

    def convert_execution_time_into_ms(self, time):
        """
        Converts the execution time into ms
        """
        match = re.match(r"([0-9]+.[0-9]+)([a-zA-Z]+)", time, re.I)
        if match:
            items = match.groups()

            if items[1] == "s":
                return float(items[0])*1000
            if items[1] == "ms":
                return float(items[0])
            if items[1] == "m":
                return float(items[0])*1000*60
            if items[1] == "h":
                return float(items[0])*1000*60*60
        else:
            return None

    def execute_statement_on_cbas_via_rest(self, statement, mode=None, rest=None, timeout=120, client_context_id=None, username=None, password=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        if not rest:
            rest = RestConnection(self.cbas_node)
        try:
            self.log.info("Running query on cbas: %s"%statement)
            response = rest.execute_statement_on_cbas(statement, mode, pretty,
                                                      timeout, client_context_id, username, password)
            response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
            else:
                errors = None

            if "results" in response:
                results = response["results"]
            else:
                results = None

            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None
            
            return response["status"], response[
                "metrics"], errors, results, handle

        except Exception as e:
            raise Exception(str(e))

    def async_query_execute(self, statement, mode, num_queries):
        """
        Asynchronously run queries
        """
        self.log.info("Executing %s queries concurrently", num_queries)

        cbas_base_url = "http://{0}:8095/analytics/service".format(
            self.cbas_node.ip)

        pretty = "true"
        tasks = []
        fail_count = 0
        failed_queries = []
        for count in range(0, num_queries):
            tasks.append(self._cb_cluster.async_cbas_query_execute(self.cbas_node,
                                                                   cbas_base_url,
                                                                   statement,
                                                                   mode,
                                                                   pretty))

        for task in tasks:
            task.result()
            if not task.passed:
                fail_count += 1

        if fail_count:
            self.fail("%s out of %s queries failed!" % (fail_count, num_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          % (num_queries - fail_count, num_queries))

    def delete_request(self, client_context_id):
        """
        Deletes a request from CBAS
        """
        rest = RestConnection(self.cbas_node)
        try:
            status = rest.delete_active_request_on_cbas(client_context_id)
            self.log.info (status)
            return status
        except Exception as e:
            raise Exception(str(e))

    def _run_concurrent_queries(self, query, mode, num_queries, rest=None):
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        # Run queries concurrently
        self.log.info("Running queries concurrently now...")
        threads = []
        for i in range(0, num_queries):
            threads.append(Thread(target=self._run_query,
                                  name="query_thread_{0}".format(i), args=(query, mode, rest)))
        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                self.sleep(5, "submitted {0} queries".format(i))
            thread.start()
        for thread in threads:
            thread.join()

        self.log.info(
            "%s queries submitted, %s failed, %s passed, %s rejected" % (
                num_queries, self.failed_count,
                self.success_count, self.rejected_count))
        self.assertTrue(self.failed_count+self.error_count == 0,
                        "Queries Failed:%s , Queries Error Out:%s"%(self.failed_count, self.error_count))

    def _run_query(self, query, mode, rest=None, validate_item_count=False, expected_count=0):
        # Execute query (with sleep induced)
        name = threading.currentThread().getName();
        client_context_id = name

        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
                query, mode=mode, rest=rest, timeout=3600,
                client_context_id=client_context_id)
            # Validate if the status of the request is success, and if the count matches num_items
            if mode == "immediate":
                if status == "success":
                    if validate_item_count:
                        if results[0]['$1'] != expected_count:
                            self.log.info("Query result : %s", results[0]['$1'])
                            self.log.info(
                                "********Thread %s : failure**********",
                                name)
                            self.failed_count += 1
                        else:
                            self.log.info(
                                "--------Thread %s : success----------",
                                name)
                            self.success_count += 1
                    else:
                        self.log.info("--------Thread %s : success----------",
                                      name)
                        self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1

            elif mode == "async":
                if status == "running" and handle:
                    self.log.info("--------Thread %s : success----------", name)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1

            elif mode == "deferred":
                if status == "success" and handle:
                    self.log.info("--------Thread %s : success----------", name)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1
        except Exception as e:
            if str(e) == "Request Rejected":
                self.log.info("Error 503 : Request Rejected")
                self.rejected_count += 1
            elif str(e) == "Capacity cannot meet job requirement":
                self.log.info(
                    "Error 500 : Capacity cannot meet job requirement")
                self.rejected_count += 1
            else:
                self.error_count +=1
                self.log.error(str(e))

    def verify_index_created(self, index_name, index_fields, dataset):
        result = True

        statement = "select * from Metadata.`Index` where DatasetName='{0}' and IsPrimary=False".format(
            dataset)
        status, metrics, errors, content, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        if status != "success":
            result = False
            self.log.info("Index not created. Metadata query status = %s",
                          status)
        else:
            index_found = False
            for index in content:
                if index["Index"]["IndexName"] == index_name:
                    index_found = True
                    field_names = []
                    for index_field in index_fields:
                        if "meta()".lower() in index_field:
                            field_names.append(index_field.split(".")[1])
                        else:
                            field_names.append(str(index_field.split(":")[0]))
                            field_names.sort()
                    self.log.info(field_names)
                    actual_field_names = []
                    for index_field in index["Index"]["SearchKey"]:
                        if isinstance(index_field, list):
                            index_field = ".".join(index_field)
                        actual_field_names.append(str(index_field))
                        actual_field_names.sort()

                    actual_field_names.sort()
                    self.log.info(actual_field_names)
                    if field_names != actual_field_names:
                        result = False
                        self.log.info("Index fields not correct")
                    break
            result &= index_found
        return result, content

    def _create_user_and_grant_role(self, username, role, source='builtin'):
        user = [{'id':username,'password':'password','name':'Some Name'}]
        response = RbacBase().create_user_source(user, source, self.master)
        user_role_list = [{'id':username,'name':'Some Name','roles':role}]
        response = RbacBase().add_user_role(user_role_list, self.rest, source)

    def _drop_user(self, username):
        user = [username]
        response = RbacBase().remove_user_role(user, self.rest)


