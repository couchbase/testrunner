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

    def _lakeformation_catalog_id(self):
        """Return the Glue catalog ID holding this run's S3 Tables namespace.

        Table buckets are federated into Glue as <account>:s3tablescatalog/<bucket>,
        the same identifier the query catalog uses as its warehouse. Falls back to the
        account ID when the bucket ARN is unavailable.
        """
        arn = self.state.s3_table_bucket_arn
        if not arn:
            return self.state.aws_account_id
        try:
            # arn:aws:s3tables:<region>:<account>:bucket/<bucket-name>
            bucket_name = arn.split(":")[5].split("/")[1]
        except IndexError:
            print(f"Could not parse table bucket name from ARN {arn}, using account ID as catalog")
            return self.state.aws_account_id
        return f"{self.state.aws_account_id}:s3tablescatalog/{bucket_name}"

    def grant_lakeformation_permissions(self):
        """Grant Lake Formation permissions so the active principal can read the S3 Tables catalog."""
        principal_arn = self.state.get_lakeformation_principal_arn()
        if not principal_arn or not self.state.lakeformation_boto3_client:
            print("Lake Formation client or principal ARN unavailable, skipping permission grant.")
            return

        # S3 Tables REST (sigv4_signing_name=s3tables) does not use the Glue Data Catalog,
        # so LF grants with account ID as CatalogId will fail. Skip LF grants entirely for
        # REST — the IAM role's direct S3 Tables permissions are sufficient for queries.
        if getattr(self.state, 'sigv4_signing_name', None) == 's3tables':
            print("S3 Tables REST catalog detected — skipping Lake Formation grants "
                  "(IAM role has direct S3 Tables access).")
            return

        # S3 Tables namespaces are federated into Glue under a per-bucket catalog
        # (<account>:s3tablescatalog/<bucket>), not the account's default catalog.
        # Granting against the account ID looks for icebergdb in the default catalog,
        # where it only exists if the AWS_GLUE suite happens to have left one behind.
        catalog_id = self._lakeformation_catalog_id()

        database_resource = {
            'Database': {
                'CatalogId': catalog_id,
                'Name': self.state.database_name
            }
        }
        table_resource = {
            'Table': {
                'CatalogId': catalog_id,
                'DatabaseName': self.state.database_name,
                'Name': self.state.table_name
            }
        }
        table_wildcard_resource = {
            'Table': {
                'CatalogId': catalog_id,
                'DatabaseName': self.state.database_name,
                'TableWildcard': {}
            }
        }

        # Retry database grant — S3 Tables LF metadata can take time to propagate
        max_db_attempts = 8
        for db_attempt in range(1, max_db_attempts + 1):
            try:
                self.state.lakeformation_boto3_client.grant_permissions(
                    Principal={'DataLakePrincipalIdentifier': principal_arn},
                    Resource=database_resource,
                    Permissions=['DESCRIBE'],
                    PermissionsWithGrantOption=[]
                )
                print(f"Granted Lake Formation permissions ['DESCRIBE'] on {database_resource} to {principal_arn}.")
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'AlreadyExistsException':
                    print(f"Lake Formation permissions ['DESCRIBE'] on {database_resource} already exist for {principal_arn}.")
                    break
                elif e.response['Error']['Code'] == 'InvalidInputException' and 'Database not found' in str(e):
                    if db_attempt < max_db_attempts:
                        print(f"LF database not yet visible in catalog {catalog_id} "
                              f"(attempt {db_attempt}/{max_db_attempts}), retrying in 5s...")
                        import time
                        time.sleep(5)
                    else:
                        print(f"Database {self.state.database_name} never became visible in Glue "
                              f"catalog {catalog_id}. If this persists, check that S3 Tables "
                              f"integration with AWS analytics services is enabled for this "
                              f"account/region — without it the federated catalog is never created.")
                        raise e
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
