#!/usr/bin/python3
import os
import json
import logging
import uuid
import re
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient

from . import provider

class AZURE(provider.Provider):
    def __init__(self, access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory, repo_name, teardown_bucket=False):
        """Create a new AZURE provider which allows interaction with AZURE masked behind the common 'Provider' interface. All
        required parameters should be those parse€d from the ini.
        """
        super().__init__(access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory)
        logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)
        logging.getLogger('azure.mgmt.resource').setLevel(logging.WARNING)
        logging.getLogger('azure.storage.blob').setLevel(logging.WARNING)
        logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.ERROR)

        if "BACKUP_RESTORE_AZURE_TENANT_ID" in os.environ:
             os.environ["AZURE_TENANT_ID"] = os.environ["BACKUP_RESTORE_AZURE_TENANT_ID"]
        if "BACKUP_RESTORE_AZURE_CLIENT_ID" in os.environ:
             os.environ["AZURE_CLIENT_ID"] = os.environ["BACKUP_RESTORE_AZURE_CLIENT_ID"]
        if "BACKUP_RESTORE_AZURE_CLIENT_SECRET" in os.environ:
             os.environ["AZURE_CLIENT_SECRET"] = os.environ["BACKUP_RESTORE_AZURE_CLIENT_SECRET"]
        self.credential = DefaultAzureCredential()

        if "BACKUP_RESTORE_AZURE_SUBSCRIPTION_KEY" in os.environ:
            self.subscription_id = os.environ["BACKUP_RESTORE_AZURE_SUBSCRIPTION_KEY"]
        else:
            raise Exception("""The environment variable 'BACKUP_RESTORE_AZURE_SUBSCRIPTION_KEY' needs to be set.
                            It sets the Azure AZURE_SUBSCRIPTION_KEY environment variable.
                            Its value is the Azure subscription ID, which can be found in:
                            Azure protal --> Subscriptions --> Overview --> Subscription ID""")
        self.region = region
        self.resource_group_name = "backuprestoretests" + str(uuid.uuid4())
        self.storage_name = "storage" +  str(uuid.uuid4())[:8]
        self.container_name = "container" +  str(uuid.uuid4())[:8]
        self.bucket = self.storage_name
        self.resource_management_client = ResourceManagementClient(self.credential, self.subscription_id)
        self.not_found_error = re.compile("container .* not found")

    def __del__(self):
        super().__del__()
        if hasattr(self, "resource_management_client"):
            if self.resource_management_client.resource_groups.check_existence(self.resource_group_name):
                delete_async_operation = self.resource_management_client \
                    .resource_groups.begin_delete(self.resource_group_name)
                delete_async_operation.wait()

    def schema_prefix(self):
        """See super class"""
        return 'az://'

    def setup(self):
        """See super class"""
        kwargs = {}

        if self.region:
            kwargs['location'] = self.region

        try:
            self.__create_storage_container__(self.region)

        except Exception as error:
            if "ResourceExistsError" not in str(error):
                raise error
            self.log.info("Bucket already exists, this is fine.")

    def teardown(self, info, remote_client):
        """See super class"""
        # Delete all the remaining objects
        self.log.info("Beginning AZURE teardown...")
        self.remove_bucket()

        # Remove the staging directory because cbbackupmgr has validation to ensure that are unique to each archive
        self._remove_staging_directory(info, remote_client)

    def remove_bucket(self):
        """See super class"""
        self.delete_objects()

    def get_json_object(self, key):
        """See super class"""
        keyContent = None
        storageStreamDownloaderObj = self.container_client.download_blob(key)
        keyContent = json.loads(storageStreamDownloaderObj.readall())
        return keyContent

    def list_objects(self, prefix=None):
        """See super class"""
        blobs_list = self.container_client.list_blobs(name_starts_with=prefix)
        return [key.name for key in blobs_list]

    def delete_objects(self, prefix=None):
        """See super class"""
        objects = list(key for key in self.list_objects(prefix))
        for blob in objects:
           self.container_client.delete_blob(blob)

    def num_multipart_uploads(self):
        """ See super class
        """
        return None

    def __delete_test_resource_group__(self):
        delete_async_operation = self.resource_management_client. \
            resource_groups.begin_delete(self.resource_group_name)
        delete_async_operation.wait()

    def __create_storage_container__(self, region):
        if self.resource_management_client.resource_groups.check_existence(self.resource_group_name):
            self.__delete_test_resource_group__()

        self.log.info("Creating Azure resoce group, storage and container")
        self.log.info(f"Resource group name: {self.resource_group_name}")
        self.log.info(f"Subscription ID: {self.subscription_id}")
        self.log.info(f"Region: {region}")
        self.log.info(f"Storage name: {self.storage_name}")
        self.log.info(f"Container name: {self.container_name}")

        # Obtain the management object for resources.
        self.resource_management_client = ResourceManagementClient(self.credential, self.subscription_id)
        RESOURCE_GROUP_NAME = self.resource_group_name
        LOCATION = region

        # Step 1: Provision the resource group.

        rg_result = self.resource_management_client.resource_groups.create_or_update(RESOURCE_GROUP_NAME,
            { "location": LOCATION })

        self.log.info(f"Provisioned resource group {rg_result.name}")

        # For details on the previous code, see Example: Provision a resource group
        # at https://docs.microsoft.com/azure/developer/python/azure-sdk-example-resource-group


        # Step 2: Provision the storage account, starting with a management object.
        storage_client = StorageManagementClient(self.credential, self.subscription_id)
        STORAGE_ACCOUNT_NAME = self.storage_name
        poller = storage_client.storage_accounts.begin_create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME,
            {
                "location" : LOCATION,
                "kind": "StorageV2",
                "sku": {"name": "Standard_LRS"}
            }
        )
        # Long-running operations return a poller object; calling poller.result()
        # waits for completion.
        account_result = poller.result()
        self.log.info(f"Provisioned storage account {account_result.name}")

        # Step 3: Retrieve the account's primary access key and generate a connection string.
        access_keys = storage_client.storage_accounts.list_keys(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)
        self.access_key_id = access_keys.keys[0].value

        # Step 4: Provision the blob container in the account (this call is synchronous)
        blob_service_client = BlobServiceClient.from_connection_string(self.__get_connection_string__())
        self.container_client = blob_service_client.create_container(self.container_name)
        return

    def __get_connection_string__(self):
        return f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={self.storage_name};AccountKey={self.access_key_id}"

provider.Provider.register(AZURE)
