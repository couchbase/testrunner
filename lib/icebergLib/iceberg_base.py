import time
import uuid
import boto3
import os
from google.auth import default
from google.auth.transport.requests import Request


class IcebergBase:
    """
    Base class for Iceberg catalog configuration and credential management.
    Adapted for testrunner from TAF icebergLib.
    """

    def __init__(self, aws_access_key=None, aws_secret_key=None, aws_session_token=None,
                 gcs_credentials=None, catalog_type=None, aws_region=None, database_name=None,
                 table_name=None, iceberg_bucket=None, gcs_project_id=None, gcs_bucket_location=None,
                 nessie_server=None, nessie_uri=None, aws_role_arn=None):
        # Common
        self.database_name = database_name or "icebergdb"
        self.table_name = table_name or "hotel"
        self.iceberg_bucket = iceberg_bucket or f"tuqquery-iceberg-{str(int(time.time()))}-{uuid.uuid4().hex[:8]}"
        self.catalog_type = catalog_type
        self.aws_account_id = os.environ.get("AWS_ACCOUNT_ID") or self._get_aws_account_id()

        # AWS Glue / S3 Tables
        self.s3_warehouse_path = f"s3://{self.iceberg_bucket}"
        self.iceberg_region = aws_region or "us-east-1"
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.aws_role_arn = aws_role_arn

        # S3 Tables
        self.s3_table_bucket_arn = None

        # Nessie
        self.nessie_server = nessie_server
        self.nessie_uri = nessie_uri or (f"http://{nessie_server}:19120/iceberg" if nessie_server else None)

        # GCP BigLake
        self.gcs_project_id = gcs_project_id or os.environ.get("GCS_PROJECT_ID")
        self.gcs_credentials = gcs_credentials
        self.gcs_bucket_path = f"gs://{self.iceberg_bucket}"
        self.gcs_bucket_location = gcs_bucket_location or "us-east1"

        # Set up environment variables for AWS/GCP
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        os.environ["AWS_CONFIG_FILE"] = "/dev/null"
        if self.aws_access_key:
            os.environ["AWS_ACCESS_KEY_ID"] = str(self.aws_access_key)
        if self.aws_secret_key:
            os.environ["AWS_SECRET_ACCESS_KEY"] = str(self.aws_secret_key)
        if self.aws_session_token:
            os.environ["AWS_SESSION_TOKEN"] = str(self.aws_session_token)
        if self.iceberg_region:
            os.environ["AWS_REGION"] = str(self.iceberg_region)
        if self.gcs_credentials:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(self.gcs_credentials)

        # Setup boto3 clients if AWS credentials provided
        if self.aws_access_key and self.aws_secret_key:
            boto3.setup_default_session(
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token,
                region_name=self.iceberg_region
            )

            self.glue_boto3_client = boto3.client('glue', region_name=self.iceberg_region)
            self.s3tables_boto3_client = boto3.client('s3tables', region_name=self.iceberg_region)
            self.lakeformation_boto3_client = boto3.client('lakeformation', region_name=self.iceberg_region)
            self.s3_resource = boto3.resource('s3', region_name=self.iceberg_region)
            self.s3_client = boto3.client('s3', region_name=self.iceberg_region)
        else:
            self.glue_boto3_client = None
            self.s3tables_boto3_client = None
            self.lakeformation_boto3_client = None
            self.s3_resource = None
            self.s3_client = None

    def gcp_access_token(self):
        """Return GCP access token for cloud-platform scope (e.g. BigLake REST catalog)."""
        credentials, _ = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        credentials.refresh(Request())
        return credentials.token

    def _get_aws_account_id(self):
        try:
            import boto3
            return boto3.client('sts').get_caller_identity()['Account']
        except Exception:
            return None

    def _normalize_iam_principal_arn(self, arn):
        if not arn:
            return None
        if arn.startswith("arn:aws:sts::") and ":assumed-role/" in arn:
            account_id = arn.split(":")[4]
            role_name = arn.split(":assumed-role/")[1].split("/")[0]
            return f"arn:aws:iam::{account_id}:role/{role_name}"
        return arn

    def get_lakeformation_principal_arn(self):
        if self.aws_role_arn:
            return self.aws_role_arn
        try:
            import boto3
            caller_arn = boto3.client('sts').get_caller_identity()['Arn']
            return self._normalize_iam_principal_arn(caller_arn)
        except Exception:
            return None

    def create_s3_bucket(self):
        """Create S3 bucket if it doesn't exist."""
        try:
            if self.iceberg_region == 'us-east-1':
                self.s3_resource.Bucket(self.iceberg_bucket).create()
            else:
                location = {'LocationConstraint': self.iceberg_region}
                self.s3_resource.Bucket(self.iceberg_bucket).create(
                    CreateBucketConfiguration=location)
            print(f"S3 bucket {self.iceberg_bucket} created successfully.")
            return True
        except Exception as e:
            print(f"Error while creating S3 bucket {self.iceberg_bucket}: {str(e)}")
            return False

    def delete_s3_bucket(self):
        """Delete S3 bucket if it exists (empties first)."""
        try:
            if self.iceberg_bucket in self.list_s3_buckets():
                if self._empty_s3_bucket():
                    self.s3_resource.Bucket(self.iceberg_bucket).delete()
                    print(f"S3 bucket {self.iceberg_bucket} deleted successfully.")
                    return True
        except Exception as e:
            print(f"Error while deleting S3 bucket {self.iceberg_bucket}: {str(e)}")
        return False

    def _empty_s3_bucket(self):
        """Empty all objects in S3 bucket before deletion."""
        try:
            bucket_resource = self.s3_resource.Bucket(self.iceberg_bucket)
            response = bucket_resource.object_versions.all().delete()
            status = True
            for item in response:
                if item.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                    status = False
            return status
        except Exception as e:
            print(f"Error emptying S3 bucket {self.iceberg_bucket}: {str(e)}")
            return False

    def list_s3_buckets(self):
        """List all S3 buckets."""
        try:
            response = self.s3_client.list_buckets()
            if response:
                return [x["Name"] for x in response.get('Buckets', [])]
            return []
        except Exception as e:
            print(f"Error listing S3 buckets: {str(e)}")
            return []
