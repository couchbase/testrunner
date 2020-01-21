#!/usr/bin/env/python
import os
import sys
import logging
import boto3
from botocore.exceptions import ClientError
import threading

class AWSS3Util:
    def __init__(self):
        logging.info("init")

    def create_bucket(bucket_name, region=None):
        try:
            if region is None:
                s3_client = boto3.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def list_buckets(bucket_name=None, region=None):
        s3 = boto3.client('s3')
        response = s3.list_buckets()

        print('Existing buckets:')
        for bucket in response['Buckets']:
            print('{}'.format(bucket["Name"]))


    def upload_file(file_name, bucket, object_name=None, metadata=None, callback=None):
        if object_name is None:
            object_name = file_name

        s3 = boto3.client('s3')
        try:
            if metadata is None and callback is None:
                response = s3.upload_file(file_name, bucket, object_name)
            elif metadata is not None and callback is None:
                response = s3.upload_file(file_name, bucket, object_name, ExtraArgs={'Metadata': metadata})
            elif metadata is None and callback is not None:
                response = s3.upload_file(file_name, bucket, object_name,
                                          Callback = callback)
            else:
                response = s3.upload_file(file_name, bucket, object_name, ExtraArgs={'Metadata': metadata},
                                          Callback = callback)

        except ClientError as e:
            logging.error(e)
            return False
        return True


    def upload_file_obj(file_name, bucket, object_name=None, metadata=None,callback=None):
        if object_name is None:
            object_name = file_name
        try:
            s3 = boto3.client('s3')
            with open(file_name, "rb") as f:
                if metadata is None and callback is None:
                    s3.upload_file(f, bucket, object_name)
                elif metadata is not None and callback is None:
                    s3.upload_file(f, bucket, object_name, ExtraArgs={'Metadata': metadata})
                elif metadata is not None and callback is not None:
                    s3.upload_file(f, bucket, object_name, CallBack=callback)
                else:
                    s3.upload_file(f, bucket, object_name, ExtraArgs={'Metadata': metadata}, CallBack = callback)
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def download_file(file_name, bucket, object_name=None):
        s3 = boto3.client('s3')
        s3.download_file(bucket, object_name, file_name)


    def download_file_obj(file_name, bucket, object_name=None):
        s3 = boto3.client('s3')
        with open(file_name, 'wb') as f:
            s3.download_fileobj(bucket, object_name, f)

    if __name__ == "__main__":
        list_buckets()
        upload_file("aws_s3_util.py", "cb-logs-qe", "test/aws_s3_util.py")


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()






