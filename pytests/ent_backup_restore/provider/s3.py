#!/usr/bin/python3

import json
import re

import boto3
import botocore

from . import provider

class S3(provider.Provider):
    def __init__(self, access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory):
        """Create a new S3 provider which allows interaction with S3 masked behind the common 'Provider' interface. All
        required parameters should be those parsed from the ini.
        """
        super().__init__(access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory)

        # boto3 will raise an exception if given an empty string as the endpoint_url so we must construct a kwargs
        # dictionary and conditionally populate it.
        kwargs = {}
        if self.access_key_id:
            kwargs['aws_access_key_id'] = self.access_key_id
        if self.cacert:
            kwargs['verify'] = self.cacert
        if self.endpoint != '':
            kwargs['endpoint_url'] = self.endpoint
        if self.no_ssl_verify:
            # Supplying no_ssl_verify will override the cacert value if supplied e.g. they are mutually exclusive
            kwargs['verify'] = False
        if self.region:
            kwargs['region_name'] = self.region
        if self.secret_access_key:
            kwargs['aws_secret_access_key'] = self.secret_access_key

        self.resource = boto3.resource('s3', **kwargs)

    def schema_prefix(self):
        """See super class"""
        return 's3://'

    def setup(self):
        """See super class"""
        configuration = {}

        if self.region:
            configuration['LocationConstraint'] = self.region

        try:
            self.resource.create_bucket(Bucket=self.bucket, CreateBucketConfiguration=configuration)
        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code != 'BucketAlreadyExists':
                raise error

    def teardown(self, info, remote_client):
        """See super class"""
        bucket = self.resource.Bucket(self.bucket)

        # Delete all the remaining objects
        try:
            for obj in bucket.objects.all():
                obj.delete()
        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                # Some tests remove the bucket after it's created/cleaned, if the bucket doesn't exist then all we need
                # to do is clean the staging directory.
                self._remove_staging_directory(info, remote_client)
                return

            raise error_code


        # Abort all the remaining multipart uploads. We ignore any 'NoSuchUpload' errors because we don't care if the
        # upload doesn't exist; we are trying to remove it.
        for upload in bucket.multipart_uploads.all():
            try:
                upload.abort()
            except botocore.exceptions.ClientError as error:
                error_code = error.response['Error']['Code']
                if error_code != "NoSuchUpload":
                    raise error

        # Remove the staging directory because cbbackupmgr has validation to ensure that are unique to each archive
        self._remove_staging_directory(info, remote_client)

    def remove_bucket(self):
        """See super class"""
        self.resource.Bucket(self.bucket).delete()

    def get_json_object(self, key):
        """See super class"""
        obj = None
        try:
            obj = json.loads(self.resource.Object(self.bucket, key).get()['Body'].read())
        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code not in ('NoSuchKey', 'KeyNotFound'):
                raise error_code
        return obj

    def list_objects(self, prefix=None):
        """See super class"""
        keys = []

        kwargs = {}
        if prefix:
            kwargs['Prefix'] = prefix

        for obj in self.resource.Bucket(self.bucket).objects.filter(**kwargs):
            keys.append(obj.key)

        return keys

    def delete_objects(self, prefix):
        """See super class"""
        kwargs = {}
        if prefix:
            kwargs['Prefix'] = prefix

        for obj in self.resource.Bucket(self.bucket).objects.filter(**kwargs):
            obj.delete()

    def list_backups(self, archive, repo):
        """See super class"""
        pattern = re.compile("([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3])_([0-5][0-9])_([0-5][0-9]|60)(\.[0-9]+)?(([Zz])|([\+|\-]([01][0-9]|2[0-3])_[0-5][0-9]))")

        backups = []
        for obj in self.resource.Bucket(self.bucket).objects.filter(Prefix=f"{archive}/{repo}"):
            res = pattern.search(obj.key)

            if res and res.group() not in backups:
                backups.append(res.group())

        return backups


    def num_multipart_uploads(self):
        return sum(1 for _ in self.resource.Bucket(self.bucket).multipart_uploads.all())

provider.Provider.register(S3)
