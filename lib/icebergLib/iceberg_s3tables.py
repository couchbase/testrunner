from icebergLib.iceberg_base import IcebergBase
from botocore.exceptions import ClientError
import time


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

        self.grant_lakeformation_permissions()

    def grant_lakeformation_permissions(self):
        """Grant Lake Formation permissions so the active principal can read the S3 Tables catalog."""
        principal_arn = self.state.get_lakeformation_principal_arn()
        if not principal_arn or not self.state.lakeformation_boto3_client:
            print("Lake Formation client or principal ARN unavailable, skipping permission grant.")
            return

        database_resource = {
            'Database': {
                'CatalogId': self.state.aws_account_id,
                'Name': self.state.database_name
            }
        }
        table_resource = {
            'Table': {
                'CatalogId': self.state.aws_account_id,
                'DatabaseName': self.state.database_name,
                'Name': self.state.table_name
            }
        }
        table_wildcard_resource = {
            'Table': {
                'CatalogId': self.state.aws_account_id,
                'DatabaseName': self.state.database_name,
                'TableWildcard': {}
            }
        }

        try:
            self.state.lakeformation_boto3_client.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal_arn},
                Resource=database_resource,
                Permissions=['DESCRIBE'],
                PermissionsWithGrantOption=[]
            )
            print(f"Granted Lake Formation permissions ['DESCRIBE'] on {database_resource} to {principal_arn}.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"Lake Formation permissions ['DESCRIBE'] on {database_resource} already exist for {principal_arn}.")
            else:
                raise e

        # LF metadata for newly created S3 Tables can take a few seconds to become visible.
        # Retry table-level grant to avoid failing suite_setUp on propagation races.
        max_attempts = 8
        retry_delay_sec = 2
        for attempt in range(1, max_attempts + 1):
            try:
                self.state.lakeformation_boto3_client.grant_permissions(
                    Principal={'DataLakePrincipalIdentifier': principal_arn},
                    Resource=table_resource,
                    Permissions=['DESCRIBE', 'SELECT'],
                    PermissionsWithGrantOption=[]
                )
                print(f"Granted Lake Formation permissions ['DESCRIBE', 'SELECT'] on {table_resource} to {principal_arn}.")
                return
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error'].get('Message', '')
                if error_code == 'AlreadyExistsException':
                    print(f"Lake Formation permissions ['DESCRIBE', 'SELECT'] on {table_resource} already exist for {principal_arn}.")
                    return
                if error_code == 'InvalidInputException' and 'Table not found' in error_message and attempt < max_attempts:
                    print(
                        f"Lake Formation table metadata not ready yet for {self.state.database_name}.{self.state.table_name} "
                        f"(attempt {attempt}/{max_attempts}); retrying in {retry_delay_sec}s..."
                    )
                    time.sleep(retry_delay_sec)
                    continue
                if error_code == 'InvalidInputException' and 'Table not found' in error_message:
                    break
                raise e

        # Fallback: grant table wildcard permissions so newly visible tables in the namespace are still accessible.
        try:
            self.state.lakeformation_boto3_client.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal_arn},
                Resource=table_wildcard_resource,
                Permissions=['DESCRIBE', 'SELECT'],
                PermissionsWithGrantOption=[]
            )
            print(
                f"Granted Lake Formation fallback permissions ['DESCRIBE', 'SELECT'] on "
                f"{table_wildcard_resource} to {principal_arn}."
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(
                    f"Lake Formation fallback permissions ['DESCRIBE', 'SELECT'] on "
                    f"{table_wildcard_resource} already exist for {principal_arn}."
                )
            else:
                raise e
