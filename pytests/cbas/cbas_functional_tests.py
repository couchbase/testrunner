from cbas_base import *


class CBASFunctionalTests(CBASBaseTest):
    def setUp(self):
        super(CBASFunctionalTests, self).setUp()

    def tearDown(self):
        super(CBASFunctionalTests, self).tearDown()

    def test_create_bucket_on_cbas(self):
        # Delete Default bucket and load travel-sample bucket
        self.cluster.bucket_delete(server=self.master, bucket="default")
        self.load_sample_buckets(server=self.master, bucketName="travel-sample")

        # Create bucket on CBAS
        if not self.expected_error:
            result = self.create_bucket_on_cbas(
                cbas_bucket_name=self.cbas_bucket_name,
                cb_bucket_name=self.cb_bucket_name,
                cb_server_ip=self.cb_server_ip,
                cb_bucket_password=self.cb_bucket_password)
        else:
            result = self.create_bucket_on_cbas(
                cbas_bucket_name=self.cbas_bucket_name,
                cb_bucket_name=self.cb_bucket_name,
                cb_server_ip=self.cb_server_ip,
                cb_bucket_password=self.cb_bucket_password,
                validate_error_msg=True)
        if not result:
            self.fail("Test failed")

    def test_create_another_bucket_on_cbas(self):
        # Delete Default bucket and load travel-sample bucket
        self.cluster.bucket_delete(server=self.master, bucket="default")
        self.load_sample_buckets(server=self.master, bucketName="travel-sample")

        # Create first bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip,
                                   cb_bucket_password=self.cb_bucket_password)

        # Create another bucket on CBAS
        if not self.expected_error:
            result = self.create_bucket_on_cbas(
                cbas_bucket_name=self.cbas_bucket_name,
                cb_bucket_name=self.cb_bucket_name,
                cb_server_ip=self.cb_server_ip,
                cb_bucket_password=self.cb_bucket_password)
        else:
            result = self.create_bucket_on_cbas(
                cbas_bucket_name=self.cbas_bucket_name,
                cb_bucket_name=self.cb_bucket_name,
                cb_server_ip=self.cb_server_ip,
                cb_bucket_password=self.cb_bucket_password,
                validate_error_msg=True)
        if not result:
            self.fail("Test failed")

    def test_create_dataset_on_bucket(self):
        # Delete Default bucket and load travel-sample bucket
        self.cluster.bucket_delete(server=self.master, bucket="default")
        self.load_sample_buckets(server=self.master, bucketName="travel-sample")

        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip,
                                   cb_bucket_password=self.cb_bucket_password)

        # Create dataset on the CBAS bucket
        cbas_bucket_name_for_ds = self.input.param('cbas_bucket_name_for_ds',
                                                   self.cbas_bucket_name)
        if not self.expected_error:
            result = self.create_dataset_on_bucket(
                cbas_bucket_name=cbas_bucket_name_for_ds,
                cbas_dataset_name=self.cbas_dataset_name)
        else:
            result = self.create_dataset_on_bucket(
                cbas_bucket_name=cbas_bucket_name_for_ds,
                cbas_dataset_name=self.cbas_dataset_name,
                validate_error_msg=True)

        if not result:
            self.fail("Test failed")

    def test_create_another_dataset_on_bucket(self):
        # Delete Default bucket and load travel-sample bucket
        self.cluster.bucket_delete(server=self.master, bucket="default")
        self.load_sample_buckets(server=self.master, bucketName="travel-sample")

        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip,
                                   cb_bucket_password=self.cb_bucket_password)

        # Create first dataset on the CBAS bucket
        cbas_bucket_name_for_ds = self.input.param('cbas_bucket_name_for_ds',
                                                   self.cbas_bucket_name)
        self.create_dataset_on_bucket(cbas_bucket_name=cbas_bucket_name_for_ds,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Create another dataset on the CBAS bucket
        cbas_dataset2_name = self.input.param('cbas_dataset2_name', None)
        if not self.expected_error:
            result = self.create_dataset_on_bucket(
                cbas_bucket_name=cbas_bucket_name_for_ds,
                cbas_dataset_name=cbas_dataset2_name)
        else:
            result = self.create_dataset_on_bucket(
                cbas_bucket_name=cbas_bucket_name_for_ds,
                cbas_dataset_name=cbas_dataset2_name, validate_error_msg=True)
        if not result:
            self.fail("Test failed")
