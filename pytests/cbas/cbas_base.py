import json
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from lib.couchbase_helper.analytics_helper import *


class CBASBaseTest(BaseTestCase):
    def setUp(self):
        super(CBASBaseTest, self).setUp()
        self.cbas_node = self.input.cbas
        self.analytics_helper = AnalyticsHelper()
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
        self.sleep(10)

    def create_bucket_on_cbas(self, cbas_bucket_name, cb_bucket_name,
                              cb_server_ip,
                              validate_error_msg=False):
        """
        Creates a bucket on CBAS
        """
        cmd_create_bucket = "create bucket " + cbas_bucket_name + " with {\"bucket\":\"" + cb_bucket_name + "\",\"nodes\":\"" + cb_server_ip + "\"};"
        status, metrics, errors, results = self.execute_statement_on_cbas(
            cmd_create_bucket, self.master)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 validate_error_msg=False):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        cmd_create_dataset = "create shadow dataset {0} on {1};".format(
            cbas_dataset_name, cbas_bucket_name)
        status, metrics, errors, results = self.execute_statement_on_cbas(
            cmd_create_dataset, self.master)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def connect_to_bucket(self, cbas_bucket_name, cb_bucket_password="",
                          validate_error_msg=False):
        """
        Connects to a CBAS bucket
        """
        cmd_connect_bucket = "connect bucket " + cbas_bucket_name + " with {\"password\":\"" + cb_bucket_password + "\"};"
        status, metrics, errors, results = self.execute_statement_on_cbas(
            cmd_connect_bucket, self.master)
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

        status, metrics, errors, results = self.execute_statement_on_cbas(
            cmd_disconnect_bucket, self.master)
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
        status, metrics, errors, results = self.execute_statement_on_cbas(
            cmd_drop_dataset, self.master)
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
        status, metrics, errors, results = self.execute_statement_on_cbas(
            cmd_drop_bucket, self.master)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def execute_statement_on_cbas(self, statement, server):
        """
        Executes a statement on CBAS using the REST API
        """
        shell = RemoteMachineShellConnection(server)
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

        return response["status"], response["metrics"], errors, results

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
            cmd_get_buckets = "select BucketName from Metadata.`Bucket`;"
            status, metrics, errors, results = self.execute_statement_on_cbas(
                cmd_get_buckets, self.master)
            if (results != None) & (len(results) > 0):
                for row in results:
                    self.disconnect_from_bucket(row['BucketName'],
                                                disconnect_if_connected=True)
                    self.log.info(
                        "********* Disconnected all buckets *********")
            else:
                self.log.info("********* No buckets to disconnect *********")

            # Drop all datasets
            cmd_get_datasets = "select DatasetName from Metadata.`Dataset` where DataverseName != \"Metadata\";"
            status, metrics, errors, results = self.execute_statement_on_cbas(
                cmd_get_datasets, self.master)
            if (results != None) & (len(results) > 0):
                for row in results:
                    self.drop_dataset(row['DatasetName'])
                    self.log.info("********* Dropped all datasets *********")
            else:
                self.log.info("********* No datasets to drop *********")

            # Drop all buckets
            status, metrics, errors, results = self.execute_statement_on_cbas(
                cmd_get_buckets, self.master)
            if (results != None) & (len(results) > 0):
                for row in results:
                    self.drop_cbas_bucket(row['BucketName'])
                    self.log.info("********* Dropped all buckets *********")
            else:
                self.log.info("********* No buckets to drop *********")
        except Exception as e:
            self.log.info(e.message)
