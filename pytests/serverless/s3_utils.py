"""
s3_utils.py:

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 30/11/22 4:52 pm

"""
import os.path

import boto3
import logger
from time import sleep


class S3Utils(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, s3_bucket='gsi-onprem-test', region='us-west-1'):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket = s3_bucket
        self.region = region
        self.s3 = boto3.client(service_name='s3', region_name=region,
                               aws_access_key_id=self.aws_access_key_id,
                               aws_secret_access_key=self.aws_secret_access_key)
        self.log = logger.Logger.get_logger()

    def create_s3_folder(self, folder='indexing'):
        response = self.s3.put_object(Bucket=self.s3_bucket, Body='',  Key=f'{folder}/')
        self.log.info(response)

    def delete_s3_folder(self, folder='indexing'):
        if folder is None:
            return
        folder = f"{folder}/"
        result = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=folder)

        paths_to_delete = []
        if 'Contents' not in result:
            return
        for item in result['Contents']:
            paths_to_delete.append({"Key": item["Key"]})
        #paths_to_delete.append({"Key": folder})

        response = self.s3.delete_objects(Bucket=self.s3_bucket, Delete={"Objects": paths_to_delete})

        self.log.info(response)

    def check_s3_folder_exist(self, folder='indexing'):
        folder_to_delete = f"{folder}/"
        result = self.s3.list_objects_v2(Bucket=self.s3_bucket, Delimiter='/*')
        if 'Contents' not in result:
            return True
        result_folder = [item['Key'] for item in result['Contents']]
        return folder_to_delete in result_folder

    def check_s3_cleanup(self, folder='indexing', bucket=None, recheck=False):
        if not bucket:
            bucket = self.s3_bucket
        result = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/*')
        folder_path_expected = f"{folder}/plasma_storage_v1/"
        self.log.info(f"Expected folder list {folder_path_expected}")
        folder_list_on_aws = []
        self.log.info(f"Result from the s3 list_objects_v2 API call:{result}")
        for item in result['Contents']:
            if folder_path_expected in item['Key']:
                folder_list_on_aws.append(item['Key'])
        if len(folder_list_on_aws) > 1:
            if not recheck:
                self.log.info("Waiting for janitor to kick clean up")
                sleep(60)
                self.check_s3_cleanup(folder=folder, bucket=bucket, recheck=True)
            else:
                self.log.info(folder_list_on_aws)
                raise Exception("Bucket is not cleaned up after rebalance.")

    def download_file(self, object_name, filename, force_download=False):
        self.log.info("Downloading file from S3")
        if os.path.exists(filename) and not force_download:
            self.log.info("File exist in the local setup, skipping downloading")
            return
        self.s3.download_file(self.s3_bucket, object_name, filename)
