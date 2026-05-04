from icebergLib.iceberg_base import IcebergBase
from botocore.exceptions import ClientError


class S3TablesCatalog:
    """
    AWS S3 Tables Catalog provisioning helpers for Iceberg tables.
    Ported from TAF icebergLib.
    """

    def __init__(self, state: IcebergBase):
        self.state = state

    def delete_s3_table(self):
        """Delete an S3 Table and its bucket using the AWS S3 Tables API."""
        try:
            self.state.s3tables_boto3_client.delete_table(
                tableBucketARN=self.state.s3_table_bucket_arn,
                namespace=self.state.database_name,
                name=self.state.table_name
            )
            print(f"Table {self.state.database_name}.{self.state.table_name} deleted successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NotFoundException':
                print(f"Table {self.state.database_name}.{self.state.table_name} does not exist, skipping table delete.")
            else:
                raise e

        try:
            self.state.s3tables_boto3_client.delete_namespace(
                tableBucketARN=self.state.s3_table_bucket_arn,
                namespace=self.state.database_name
            )
            print(f"Namespace {self.state.database_name} deleted successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NotFoundException':
                print(f"Namespace {self.state.database_name} does not exist, skipping namespace delete.")
            else:
                raise e

        try:
            self.state.s3tables_boto3_client.delete_table_bucket(
                tableBucketARN=self.state.s3_table_bucket_arn)
            print("Table bucket deleted successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NotFoundException':
                print("Table bucket does not exist, skipping bucket delete.")
            else:
                raise e

    def create_s3_table_bucket(self):
        """Create S3 Tables bucket, namespace, and table."""
        try:
            response = self.state.s3tables_boto3_client.create_table_bucket(
                name=self.state.iceberg_bucket)
            self.state.s3_table_bucket_arn = response['arn']
            print(f"Table bucket created successfully. ARN: {self.state.s3_table_bucket_arn}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                # Bucket already exists, get its ARN
                response = self.state.s3tables_boto3_client.get_table_bucket(
                    tableBucketARN=f"arn:aws:s3tables:{self.state.iceberg_region}:{self.state.aws_account_id}:bucket/{self.state.iceberg_bucket}"
                )
                self.state.s3_table_bucket_arn = response['arn']
                print(f"Table bucket already exists. ARN: {self.state.s3_table_bucket_arn}")
            else:
                raise e

        try:
            self.state.s3tables_boto3_client.create_namespace(
                tableBucketARN=self.state.s3_table_bucket_arn, namespace=[self.state.database_name])
            print(f"Namespace {self.state.database_name} created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                print(f"Namespace {self.state.database_name} already exists.")
            else:
                raise e

        try:
            self.state.s3tables_boto3_client.create_table(
                tableBucketARN=self.state.s3_table_bucket_arn, namespace=self.state.database_name,
                name=self.state.table_name, format="ICEBERG")
            print(f"Table {self.state.table_name} created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                print(f"Table {self.state.table_name} already exists.")
            else:
                raise e
