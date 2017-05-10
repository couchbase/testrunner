import json
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from lib.couchbase_helper.analytics_helper import *
from couchbase_helper.documentgenerator import DocumentGenerator
import urllib
from lib.membase.api.rest_client import RestConnection
from lib.couchbase_helper.cluster import *


class CBASBaseTest(BaseTestCase):
    def setUp(self):
        super(CBASBaseTest, self).setUp()
        self.cbas_node = self.input.cbas
        self.analytics_helper = AnalyticsHelper()
        self._cb_cluster = Cluster()
        invalid_ip = '10.111.151.109'
        self.cb_bucket_name = TestInputSingleton.input.param('cb_bucket_name',
                                                             'travel-sample')
        self.cbas_bucket_name = TestInputSingleton.input.param(
            'cbas_bucket_name', 'travel')
        self.cb_bucket_password = TestInputSingleton.input.param(
            'cb_bucket_password', '')
        self.expected_error = TestInputSingleton.input.param("error", None)
        if self.expected_error:
            self.expected_error = self.expected_error.replace("INVALID_IP",invalid_ip)
            self.expected_error = self.expected_error.replace("PORT",self.master.port)
        self.cb_server_ip = TestInputSingleton.input.param("cb_server_ip",
                                                           self.master.ip)
        self.cb_server_ip = self.cb_server_ip.replace('INVALID_IP',invalid_ip)
        self.cbas_dataset_name = TestInputSingleton.input.param(
            "cbas_dataset_name", 'travel_ds')
        self.cbas_bucket_name_invalid = self.input.param(
            'cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.cbas_dataset2_name = self.input.param('cbas_dataset2_name', None)
        self.skip_create_dataset = self.input.param('skip_create_dataset',
                                                    False)
        self.disconnect_if_connected = self.input.param(
            'disconnect_if_connected', False)
        self.cbas_dataset_name_invalid = self.input.param(
            'cbas_dataset_name_invalid', self.cbas_dataset_name)
        self.skip_drop_connection = self.input.param('skip_drop_connection',
                                                     False)
        self.skip_drop_dataset = self.input.param('skip_drop_dataset', False)

        self.query_id = self.input.param('query_id',None)
        self.mode = self.input.param('mode',None)
        self.num_concurrent_queries = self.input.param('num_queries',5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size', 100)
        self.compiler_param = self.input.param('compiler_param', None)
        self.compiler_param_val = self.input.param('compiler_param_val', None)
        self.expect_reject = self.input.param('expect_reject', False)
        self.expect_failure = self.input.param('expect_failure', False)

        # Drop any existing buckets and datasets
        self.cleanup_cbas()

    def tearDown(self):
        super(CBASBaseTest, self).tearDown()

    def load_sample_buckets(self, server, bucketName):
        """
        Load the specified sample bucket in Couchbase
        """
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("""curl -v -u Administrator:password -X POST http://{0}:8091/sampleBuckets/install -d '["{1}"]'""".format(server.ip, bucketName))
        shell.disconnect()
        self.sleep(5)

    def create_bucket_on_cbas(self, cbas_bucket_name, cb_bucket_name,
                              cb_server_ip,
                              validate_error_msg=False):
        """
        Creates a bucket on CBAS
        """
        cmd_create_bucket = "create bucket " + cbas_bucket_name + " with {\"name\":\"" + cb_bucket_name + "\",\"nodes\":\"" + cb_server_ip + "\"};"
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_create_bucket)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value = None,
                                 validate_error_msg=False):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        cmd_create_dataset = "create shadow dataset {0} on {1};".format(
            cbas_dataset_name, cbas_bucket_name)
        if where_field and where_value:
            cmd_create_dataset = "create shadow dataset {0} on {1} WHERE `{2}`=\"{3}\";".format(
                cbas_dataset_name, cbas_bucket_name, where_field, where_value)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_create_dataset)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def connect_to_bucket(self, cbas_bucket_name, cb_bucket_password="",
                          validate_error_msg=False, cb_bucket_username=""):
        """
        Connects to a CBAS bucket
        """
        if cb_bucket_username=="":
            cb_bucket_username="cbadminbucket"
        if cb_bucket_password=="":
            cb_bucket_password="password"
        cmd_connect_bucket = "connect bucket " + cbas_bucket_name + " with {\"username\":\"" + cb_bucket_username + "\",\"password\":\"" + cb_bucket_password + "\"};"
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_connect_bucket)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def disconnect_from_bucket(self, cbas_bucket_name,
                               disconnect_if_connected=False,
                               validate_error_msg=False):
        """
        Disconnects from a CBAS bucket
        """
        if disconnect_if_connected:
            cmd_disconnect_bucket = "disconnect bucket {0} if connected;".format(
                cbas_bucket_name)
        else:
            cmd_disconnect_bucket = "disconnect bucket {0};".format(
                cbas_bucket_name)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_disconnect_bucket)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_dataset(self, cbas_dataset_name, validate_error_msg=False):
        """
        Drop dataset from CBAS
        """
        cmd_drop_dataset = "drop dataset {0};".format(cbas_dataset_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_drop_dataset)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_cbas_bucket(self, cbas_bucket_name, validate_error_msg=False):
        """
        Drop a CBAS bucket
        """
        cmd_drop_bucket = "drop bucket {0};".format(cbas_bucket_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            cmd_drop_bucket)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def execute_statement_on_cbas(self, statement, server, mode=None):
        """
        Executes a statement on CBAS using the REST API through curl command
        """
        shell = RemoteMachineShellConnection(server)
        if mode:
            output, error = shell.execute_command(
                """curl -s --data pretty=true --data mode={2} --data-urlencode 'statement={1}' http://{0}:8095/analytics/service -v""".format(
                    self.cbas_node.ip, statement, mode))
        else:
            output, error = shell.execute_command(
            """curl -s --data pretty=true --data-urlencode 'statement={1}' http://{0}:8095/analytics/service -v""".format(
                self.cbas_node.ip, statement))
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

        output, error = shell.execute_command("""curl -v {0}""".format(handle))

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
            """curl -v {0}""".format(handle))

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

        self.log.info("status=%s, handle=%s"%(status,handle))
        return status, handle

    def validate_error_in_response(self, status, errors):
        """
        Validates if the error message in the response is same as the expected one.
        """
        if status != "success":
            actual_error = errors[0]["msg"]
            if self.expected_error != actual_error:
                return False
            else:
                return True

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
            self.log.info(e.message)

    def perform_doc_ops_in_all_cb_buckets(self, num_items, operation,start_key=0, end_key=1000):
        """
        Create/Update/Delete docs in all cb buckets
        :param num_items: No. of items to be created/deleted/updated
        :param operation: String - "create","update","delete"
        :param start_key: Doc Key to start the operation with
        :param end_key: Doc Key to end the operation with
        :return:
        """
        age = range(70)
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        template = '{{ "number": {0}, "first_name": "{1}" , "mutated":0}}'
        gen_load = DocumentGenerator('test_docs', template, age, first,
                                     start=start_key, end=end_key)
        self.log.info("%s %s documents..." % (operation, num_items))
        try:
            self._load_all_buckets(self.master, gen_load, operation, 0)
            self._verify_stats_all_buckets(self.input.servers)
        except Exception as e:
            self.log.info(e.message)

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
            self.log.info("No. of items in CBAS dataset {0} : {1}".format(dataset_name,results[0]['$1']))
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

    def execute_statement_on_cbas_via_rest(self, statement, mode=None, rest=None, timeout=70, client_context_id=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        if not rest:
            rest = RestConnection(self.master)
        try:
            response = rest.execute_statement_on_cbas(statement, mode, pretty,
                                                      timeout, client_context_id)
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

        except Exception,e:
            raise Exception(str(e))

    def async_query_execute(self, statement, mode, num_queries):
        """
        Asynchronously run queries
        """
        self.log.info("Executing %s queries concurrently",num_queries)

        cbas_base_url = "http://{0}:8095/analytics/service".format(
            self.cbas_node.ip)

        pretty = "true"
        tasks = []
        fail_count = 0
        failed_queries = []
        for count in range(0, num_queries):
            tasks.append(self._cb_cluster.async_cbas_query_execute(self.master,
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
        rest = RestConnection(self.master)
        try:
            status = rest.delete_active_request_on_cbas(client_context_id)
            self.log.info (status)
            return status
        except Exception, e:
            raise Exception(str(e))
