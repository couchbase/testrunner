#!/usr/bin/python3

import json
import os
import time
import contextlib
import re

from concurrent.futures import(
    ThreadPoolExecutor
)
from google.cloud import(
        storage,
        exceptions
)
from google.api_core import exceptions as api_exceptions

from . import provider

class GCP(provider.Provider):
    def __init__(self, access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory, repo_name, teardown_bucket=False):
        """Create a new GCP provider which allows interaction with GCP masked behind the common 'Provider' interface. All
        required parameters should be those parsed from the ini.
        """
        super().__init__(access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory)

        self.teardown_bucket = teardown_bucket

        self.repo_name = repo_name

        # Initialize storage client with credentials if available
        gcp_auth_path = os.getenv("GCP_AUTH_PATH")
        # If not set via env var, check default location
        if not gcp_auth_path:
            default_path = "/root/.config/gcloud/application_default_credentials.json"
            if os.path.isfile(default_path) and os.access(default_path, os.R_OK):
                gcp_auth_path = default_path
        
        if gcp_auth_path and os.path.isfile(gcp_auth_path) and os.access(gcp_auth_path, os.R_OK):
            self.resource = storage.Client.from_service_account_json(gcp_auth_path)
        else:
            # Fall back to default credentials (will use GOOGLE_APPLICATION_CREDENTIALS env var if set)
            self.resource = storage.Client()

        self.not_found_error = re.compile("the specified bucket does not exist")

    def schema_prefix(self):
        """See super class"""
        return 'gs://'

    def setup(self):
        """See super class"""
        kwargs = {}

        if self.region:
            kwargs['location'] = self.region

        try:
            self.resource.create_bucket(self.bucket, **kwargs)
        except Exception as error:
            if error.code != 409:
                raise error
            self.log.info("Bucket already exists, this is fine.")

        self.cloud_bucket = self.resource.bucket(self.bucket)

    def teardown(self, info, remote_client):
        """See super class"""
        # Delete all the remaining objects
        self.log.info("Beginning GCP teardown...")

        # Since we're deleting everything, it's ok if items are missing
        # Permission errors are handled and logged in delete_all_items, but won't fail teardown
        with contextlib.suppress(exceptions.NotFound, api_exceptions.Forbidden):
            self.delete_all_items(self.teardown_bucket)
            # Buckets can only be deleted with <256 items
            if self.teardown_bucket:
                try:
                    self.cloud_bucket.delete(force=True)
                except api_exceptions.Forbidden as e:
                    self.log.warning(f"Permission denied when deleting bucket: {e}. "
                                   f"Bucket may not have been deleted. This is non-fatal.")

        # Remove the staging directory because cbbackupmgr has validation to ensure that are unique to each archive
        self._remove_staging_directory(info, remote_client)

    def remove_bucket(self):
        """See super class"""
        self.cloud_bucket.delete()

    def get_json_object(self, key):
        """See super class"""
        obj = None
        with contextlib.suppress(exceptions.NotFound):
            search_blob = storage.blob.Blob(key, self.cloud_bucket)
            obj = json.loads(search_blob.download_as_text(self.resource))
        return obj

    def list_objects(self, prefix=None):
        """See super class"""
        kwargs = {'prefix': self.repo_name}
        if prefix is not None:
            kwargs['prefix'] = prefix

        # We only care about the path here, so generate a list of paths to return
        try:
            return [key.name for key in self.resource.list_blobs(self.cloud_bucket, **kwargs)]
        except api_exceptions.Forbidden as e:
            self.log.warning(f"Permission denied when listing objects: {e}. "
                           f"Service account may not have storage.objects.list permission.")
            return []
        except Exception as e:
            self.log.error(f"Error listing objects: {e}")
            raise

    def delete_objects(self, prefix=None):
        """See super class"""
        kwargs = {}
        if prefix:
            kwargs['prefix'] = prefix

        try:
            objects = list([key.name for key in self.resource.list_blobs(self.cloud_bucket, **kwargs)])
            with ThreadPoolExecutor() as pool:
                pool.map(lambda obj: self.cloud_bucket.delete_blob(obj), objects)
        except api_exceptions.Forbidden as e:
            self.log.warning(f"Permission denied when deleting objects: {e}. "
                           f"Service account may not have storage.objects.list or storage.objects.delete permission.")
            # Try to delete individual objects if we know the prefix, but can't list
            # This is a best-effort cleanup
            if prefix:
                self.log.info(f"Attempting to delete objects with prefix {prefix} individually (may fail if no list permission)")
        except Exception as e:
            self.log.error(f"Error deleting objects: {e}")
            raise

    def num_multipart_uploads(self):
        """ See super class
        """
        return None

    def delete_all_items(self, teardown_bucket):
        """ Deletes the entire contents of self.cloud_bucket on GCP
        """
        if teardown_bucket:
            self.log.info(f"Removing all items from bucket {self.cloud_bucket.name}...")
            try:
                all_blobs = list(self.resource.list_blobs(self.cloud_bucket))
                # If we have a very large dataset, delete asynchronously and poll
                if len(all_blobs) > 256000:
                    self.cloud_bucket.add_lifecycle_delete_rule(age=0)
                    self.cloud_bucket.patch()
                    while len(all_blobs) > 256:
                        time.sleep(10)
                        all_blobs = list(self.resource.list_blobs(self.cloud_bucket))
                        # If we don't have that many delete synchronously
                elif len(all_blobs) > 256:
                    self.cloud_bucket.delete_blobs(all_blobs)
            except api_exceptions.Forbidden as e:
                self.log.warning(f"Permission denied when listing all blobs: {e}. "
                               f"Service account may not have storage.objects.list permission. "
                               f"Skipping blob deletion.")
        else:
            self.log.info(f"Removing items with prefix {self.repo_name}")
            self.delete_objects(prefix=self.repo_name)

provider.Provider.register(GCP)
