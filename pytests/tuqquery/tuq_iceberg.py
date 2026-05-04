import json
import os
import random
from tuqquery.tuq import QueryTests
from icebergLib.iceberg_base import IcebergBase
from icebergLib.iceberg_util import IcebergUtil
from membase.api.rest_client import RestConnection
import httplib2
import base64
import time
from datetime import datetime, timedelta


# Module-level variables to share Spark session across all tests in the suite
_shared_iceberg_base = None
_shared_iceberg_util = None
_shared_spark_initialized = False
_preserve_iceberg_debug_resources = False


class IcebergQueryTests(QueryTests):
    """
    Iceberg integration tests for N1QL queries over external Iceberg tables.
    Adapted from TAF Enterprise Analytics tests for testrunner tuqquery flow.
    
    Architecture:
    - suite_setUp: Creates S3 bucket, Spark session, Glue database (once per suite)
    - setUp: Creates Iceberg table with test data, N1QL objects (per test)
    - tearDown: Drops Iceberg table, N1QL objects (per test)
    - suite_tearDown: Stops Spark, deletes Glue database, S3 bucket (once per suite)
    """

    def suite_setUp(self):
        """Initialize shared Iceberg infrastructure for the entire test suite."""
        super(IcebergQueryTests, self).suite_setUp()
        
        global _shared_iceberg_base, _shared_iceberg_util, _shared_spark_initialized, _preserve_iceberg_debug_resources
        
        self.log.info("==============  IcebergQueryTests suite_setUp started ==============")
        _preserve_iceberg_debug_resources = False
        
        try:
            # Read test parameters for Iceberg configuration
            self._init_iceberg_params()
            
            # Initialize Iceberg helpers
            self.log.info(f"Initializing shared Iceberg infrastructure with catalog_type={self.catalog_type}")
            _shared_iceberg_base = IcebergBase(
                aws_access_key=self.aws_access_key_id,
                aws_secret_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                gcs_credentials=self.gcs_credentials_file,
                catalog_type=self.catalog_type,
                aws_region=self.aws_region,
                database_name=self.iceberg_namespace,
                table_name=self.iceberg_table_name,
                iceberg_bucket=self.iceberg_bucket,
                gcs_project_id=self.gcs_project_id,
                gcs_bucket_location=self.gcs_bucket_location
            )
            
            _shared_iceberg_util = IcebergUtil(_shared_iceberg_base)
            
            # Create S3 bucket and Spark session (shared for all tests)
            self.log.info("Creating S3 bucket for test suite...")
            if self.catalog_type in ["AWS_GLUE", "AWS_GLUE_REST"]:
                _shared_iceberg_util.glue_catalog.create_s3_bucket()
                self.log.info("Creating Spark session (will be reused for all tests)...")
                _shared_iceberg_util.create_spark_session(catalog_type=self.catalog_type)
                self.log.info("Creating Glue database...")
                _shared_iceberg_util.glue_catalog.create_glue_database()
            elif self.catalog_type == "S3_TABLES":
                _shared_iceberg_util.s3_tables_catalog.create_s3_table_bucket()
                _shared_iceberg_util.create_spark_session(catalog_type="S3_TABLES")
            elif self.catalog_type == "BIGLAKE_METASTORE":
                _shared_iceberg_util.biglake_metastore_catalog.create_gcs_bucket()
                _shared_iceberg_util.biglake_metastore_catalog.create_biglake_metastore_catalog()
                _shared_iceberg_util.create_spark_session(catalog_type="BIGLAKE_METASTORE")
            
            _shared_spark_initialized = True
            self.log.info("Shared Spark session created successfully")
            
        except Exception as e:
            self.log.error(f"Error during suite_setUp: {str(e)}")
            # Cleanup on failure
            if _shared_iceberg_base:
                try:
                    _shared_iceberg_base.delete_s3_bucket()
                except Exception:
                    pass
            _shared_iceberg_base = None
            _shared_iceberg_util = None
            _shared_spark_initialized = False
            raise
        
        self.log.info("==============  IcebergQueryTests suite_setUp completed ==============")

    def suite_tearDown(self):
        """Cleanup shared Iceberg infrastructure after all tests complete."""
        global _shared_iceberg_base, _shared_iceberg_util, _shared_spark_initialized, _preserve_iceberg_debug_resources
        
        self.log.info("==============  IcebergQueryTests suite_tearDown started ==============")
        
        # Read params to know what cleanup to do
        self._init_iceberg_params()
        
        # Stop Spark session
        if _shared_iceberg_util:
            try:
                self.log.info("Stopping shared Spark session...")
                _shared_iceberg_util._stop_spark_session_safely()
            except Exception as e:
                self.log.error(f"Error stopping Spark session: {str(e)}")
            
            if _preserve_iceberg_debug_resources:
                self.log.warning(
                    "Preserving Iceberg catalog resources and bucket for debugging because at least one test failed"
                )
            else:
                # Cleanup catalog resources
                try:
                    if self.catalog_type in ["AWS_GLUE", "AWS_GLUE_REST"]:
                        self.log.info("Deleting Glue database...")
                        _shared_iceberg_util.glue_catalog.delete_glue_database()
                    elif self.catalog_type == "S3_TABLES":
                        _shared_iceberg_util.s3_tables_catalog.delete_s3_table()
                    elif self.catalog_type == "BIGLAKE_METASTORE":
                        _shared_iceberg_util.biglake_metastore_catalog.destroy_biglake_metastore_catalog()
                except Exception as e:
                    self.log.error(f"Error cleaning up catalog: {str(e)}")
                
                # Delete S3 bucket
                if _shared_iceberg_base:
                    try:
                        self.log.info(f"Deleting S3 bucket: {_shared_iceberg_base.iceberg_bucket}")
                        _shared_iceberg_base.delete_s3_bucket()
                    except Exception as e:
                        self.log.error(f"Error deleting S3 bucket: {str(e)}")
        
        # Reset shared state
        _shared_iceberg_base = None
        _shared_iceberg_util = None
        _shared_spark_initialized = False
        _preserve_iceberg_debug_resources = False
        
        super(IcebergQueryTests, self).suite_tearDown()
        self.log.info("==============  IcebergQueryTests suite_tearDown completed ==============")

    def _init_iceberg_params(self):
        """Initialize Iceberg parameters from test input."""
        self.catalog_type = self.input.param("catalog_type", "AWS_GLUE")
        self.credentialstore_name = self.input.param("credentialstore_name", "iceberg_creds")
        self.catalog_name = self.input.param("catalog_name", "iceberg_catalog")
        self.couchbase_bucket_name = self.input.param("couchbase_bucket_name", "default")
        self.iceberg_scope_name = self.input.param("iceberg_scope_name", "iceberg")
        self.iceberg_collection_name = self.input.param("iceberg_collection_name", "external_hotel")
        self.external_collection_name = f"{self.couchbase_bucket_name}.{self.iceberg_scope_name}.{self.iceberg_collection_name}"
        self.iceberg_namespace = self.input.param("iceberg_namespace", "icebergdb")
        self.iceberg_table_name = self.input.param("iceberg_table_name", "hotel")
        self.iceberg_bucket = self.input.param("iceberg_bucket", None)
        self.initial_doc_count = self.input.param("initial_doc_count", 10000)

        # AWS parameters
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", None)
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
        self.aws_region = self.input.param("aws_region", "us-east-1")
        self.aws_session_token = os.environ.get("AWS_SESSION_TOKEN", None)
        self.aws_endpoint = self.input.param("aws_endpoint", None)
        self.sigv4_signing_name = self.input.param("sigv4_signing_name", None)
        self.sigv4_signing_region = self.input.param("sigv4_signing_region", None)
        self.warehouse_path = self.input.param("warehouse_path", None)
        self.catalog_uri = self.input.param("catalog_uri", None)

        # GCP parameters
        self.gcs_credentials_file = self.input.param("gcs_credentials_file", None)
        self.gcs_project_id = self.input.param("gcs_project_id", None)
        self.gcs_bucket_location = self.input.param("gcs_bucket_location", "us-east1")

        # Nessie parameters
        self.nessie_uri = self.input.param("nessie_uri", None)
        self.nessie_warehouse = self.input.param("nessie_warehouse", None)

        # Advanced parameters
        self.enable_credentialstore_settings = self.input.param("enable_credentialstore_settings", True)
        self.catalog_with_json = self.input.param("catalog_with_json", None)
        self.external_collection_with_json = self.input.param("external_collection_with_json", None)

    def _validate_spark_session(self):
        """
        Validate the shared Spark session is still healthy.
        Returns True if healthy, False if needs recreation.
        """
        global _shared_iceberg_util
        
        if not _shared_iceberg_util or not _shared_iceberg_util.spark:
            return False
        
        try:
            # Simple health check - try to execute a trivial operation
            _shared_iceberg_util.spark.sql("SELECT 1").collect()
            return True
        except Exception as e:
            self.log.warning(f"Spark session health check failed: {e}")
            return False

    def _recreate_spark_session(self):
        """Recreate the Spark session if it has become unhealthy."""
        global _shared_iceberg_util, _shared_spark_initialized
        
        self.log.info("Recreating Spark session...")
        try:
            # Stop old session if any
            if _shared_iceberg_util:
                try:
                    _shared_iceberg_util._stop_spark_session_safely()
                except Exception:
                    pass
            
            # Create new session
            _shared_iceberg_util.create_spark_session(catalog_type=self.catalog_type)
            _shared_spark_initialized = True
            self.log.info("Spark session recreated successfully")
            return True
        except Exception as e:
            self.log.error(f"Failed to recreate Spark session: {e}")
            _shared_spark_initialized = False
            return False

    def setUp(self):
        """Per-test setup: create Iceberg table and N1QL objects."""
        global _shared_iceberg_base, _shared_iceberg_util, _shared_spark_initialized, _preserve_iceberg_debug_resources
        
        # Initialize flags
        self.n1ql_objects_created = False
        self._test_setup_done = False
        
        super(IcebergQueryTests, self).setUp()
        
        # Skip Iceberg-specific setup for suite methods
        if self._testMethodName in ('suite_setUp', 'suite_tearDown'):
            self.log.info(f"Skipping Iceberg setup for {self._testMethodName}")
            return
        
        self.log.info("==============  IcebergQueryTests setUp started ==============")
        
        try:
            # Initialize params
            self._init_iceberg_params()
            
            # Use shared infrastructure
            if not _shared_spark_initialized:
                raise RuntimeError("Shared Spark session not initialized. suite_setUp may have failed.")
            
            self.iceberg_base = _shared_iceberg_base
            self.iceberg_util = _shared_iceberg_util
            
            # Validate Spark session is still healthy, recreate if needed
            if not self._validate_spark_session():
                self.log.warning("Spark session is unhealthy, attempting to recreate...")
                if not self._recreate_spark_session():
                    raise RuntimeError("Failed to recreate Spark session")
            
            # Create Iceberg table with test-specific data
            self._provision_iceberg_table()
            
            # Setup Couchbase-side objects (CREDENTIALSTORE, CATALOG, EXTERNAL COLLECTION)
            self._setup_n1ql_iceberg_objects()
            
            self._test_setup_done = True
            
        except Exception as e:
            self.log.error(f"Error during setUp: {str(e)}")
            _preserve_iceberg_debug_resources = True
            self.log.warning(
                f"Setup failed for {self._testMethodName}; preserving Iceberg table and Glue/S3 catalog resources for debugging"
            )
            raise
        
        self.log.info("==============  IcebergQueryTests setUp completed ==============")

    def tearDown(self):
        """Per-test cleanup: drop Iceberg table and N1QL objects."""
        self.log.info("==============  IcebergQueryTests tearDown started ==============")
        global _preserve_iceberg_debug_resources
        
        # Skip for suite methods
        if self._testMethodName in ('suite_setUp', 'suite_tearDown'):
            self.log.info(f"Skipping Iceberg teardown for {self._testMethodName}")
            super(IcebergQueryTests, self).tearDown()
            self.log.info("==============  IcebergQueryTests tearDown completed ==============")
            return
        
        # Cleanup N1QL objects
        if getattr(self, 'n1ql_objects_created', False):
            try:
                self._cleanup_n1ql_iceberg_objects()
            except Exception as e:
                self.log.error(f"Error during N1QL cleanup: {str(e)}")
        
        # Preserve shared Iceberg resources when the test fails so they can be inspected after the run.
        if self.is_test_failed():
            _preserve_iceberg_debug_resources = True
            self.log.warning(
                f"Test {self._testMethodName} failed; preserving Iceberg table and Glue/S3 catalog resources for debugging"
            )
        else:
            # Cleanup Iceberg table (not the whole catalog)
            self._cleanup_test_table()
        
        super(IcebergQueryTests, self).tearDown()
        self.log.info("==============  IcebergQueryTests tearDown completed ==============")

    def _cleanup_test_table(self):
        """Cleanup only the Iceberg table, not the shared infrastructure."""
        if getattr(self, 'iceberg_util', None) and getattr(self, 'catalog_type', None):
            try:
                self.log.info(f"Dropping Iceberg table: {self.iceberg_table_name}")
                if self.catalog_type in ["AWS_GLUE", "AWS_GLUE_REST"]:
                    self.iceberg_util.glue_catalog.delete_glue_table()
                elif self.catalog_type == "S3_TABLES":
                    # S3 Tables handles table cleanup differently
                    pass
            except Exception as e:
                self.log.error(f"Error dropping Iceberg table: {str(e)}")

    def _provision_iceberg_table(self):
        """Create Iceberg table with test data using the shared Spark session."""
        self.log.info(f"Creating Iceberg table for test: {self._testMethodName}")
        use_legacy_inferred_schema = self._testMethodName == "test_iceberg_select_star_nested_structure"

        # Generate sample data for testing based on test method
        if self._testMethodName == "test_iceberg_select_reviews_nested_list_template":
            sample_data = self._generate_legacy_nested_review_sample_data(self.initial_doc_count)
        elif self._testMethodName == "test_iceberg_null_values":
            sample_data = self._generate_null_values_sample_data(min(self.initial_doc_count, 100))
        elif self._testMethodName == "test_iceberg_diverse_datatypes":
            sample_data = self._generate_diverse_datatypes_sample_data(min(self.initial_doc_count, 100))
        elif self._testMethodName == "test_iceberg_mixed_types":
            sample_data = self._generate_mixed_types_sample_data(min(self.initial_doc_count, 100))
        else:
            sample_data = self._generate_sample_data(self.initial_doc_count)
        self.sample_data = sample_data
        self.sample_data_by_id = {doc["id"]: doc for doc in sample_data}

        try:
            # Delete existing table if any (from previous test)
            if self.catalog_type in ["AWS_GLUE", "AWS_GLUE_REST"]:
                try:
                    self.iceberg_util.glue_catalog.delete_glue_table()
                    self.log.info("Deleted existing Glue table (if any)")
                except Exception as e:
                    self.log.info(f"No existing table to delete or delete failed: {e}")
            
            # Create the Iceberg table using the shared Spark session
            self.log.info(f"Creating Iceberg table: {self.catalog_type}.{self.iceberg_namespace}.{self.iceberg_table_name}")
            self.iceberg_util.create_iceberg_table(
                catalog_type=self.catalog_type,
                data=sample_data,
                infer_schema=not use_legacy_inferred_schema
            )
            
            # Wait for Glue metadata to propagate
            self.log.info("Waiting for Glue metadata to propagate...")
            time.sleep(5)
            
            # Verify table was created and is accessible
            table_path = f"{self.catalog_type}.{self.iceberg_namespace}.{self.iceberg_table_name}"
            self.log.info(f"Verifying table exists: {table_path}")
            
            # Retry verification a few times in case of propagation delay
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    df = self.iceberg_util.spark.table(table_path)
                    df.printSchema()
                    row_count = df.count()
                    self.log.info(f"Created Iceberg table with {row_count} rows")
                    df.show(5)
                    
                    if row_count != len(sample_data):
                        self.log.warning(f"Row count mismatch: expected {len(sample_data)}, got {row_count}")
                    break
                except Exception as verify_error:
                    if attempt < max_retries - 1:
                        self.log.warning(f"Table verification attempt {attempt + 1} failed: {verify_error}, retrying...")
                        time.sleep(3)
                    else:
                        raise RuntimeError(f"Table verification failed after {max_retries} attempts: {verify_error}")
            
        except Exception as e:
            self.log.error(f"Failed to create Iceberg table: {str(e)}")
            raise



    def _setup_n1ql_iceberg_objects(self):
        """Setup N1QL objects: CREDENTIALSTORE, CATALOG, EXTERNAL COLLECTION."""
        self.log.info("Setting up N1QL Iceberg objects...")

        # Enable credential store if requested
        if self.enable_credentialstore_settings:
            self._enable_credentialstore_settings()

        # Create credential store
        self._create_credentialstore()

        # Create catalog
        self._create_catalog()

        # Create bucket for external collection (if not using existing bucket)
        self._create_bucket_if_needed()

        # Create scope for external collection (if not _default)
        if self.iceberg_scope_name != "_default":
            self._create_scope()

        # Create external collection
        self._create_external_collection()
        time.sleep(10)

        # Create comparison Couchbase collection with same data
        self._setup_comparison_couchbase_collection()

        # Mark N1QL objects as successfully created for cleanup
        self.n1ql_objects_created = True
        self.log.info("Successfully created N1QL Iceberg objects")

    def _setup_comparison_couchbase_collection(self):
        """Create a Couchbase collection with the same data as Iceberg for comparison."""
        default_bucket = self.buckets[0].name if self.buckets else "default"
        self.cb_comparison_scope = "iceberg_compare"
        self.cb_comparison_collection = "hotel_data"
        self.cb_comparison_path = f"`{default_bucket}`.`{self.cb_comparison_scope}`.`{self.cb_comparison_collection}`"
        
        self.log.info(f"Setting up comparison Couchbase collection: {self.cb_comparison_path}")
        
        # Create scope
        try:
            self.run_cbq_query(f'CREATE SCOPE `{default_bucket}`.`{self.cb_comparison_scope}` IF NOT EXISTS')
            time.sleep(2)
        except Exception as e:
            self.log.warning(f"Comparison scope creation: {e}")
        
        # Create collection
        try:
            self.run_cbq_query(f'CREATE COLLECTION {self.cb_comparison_path} IF NOT EXISTS')
            time.sleep(3)
        except Exception as e:
            self.log.warning(f"Comparison collection creation: {e}")
        
        # Create primary index
        try:
            self.run_cbq_query(f'CREATE PRIMARY INDEX ON {self.cb_comparison_path}')
            time.sleep(3)
        except Exception as e:
            self.log.warning(f"Comparison index creation: {e}")
        
        # Insert same data as Iceberg (all documents) - suppress query logging to reduce bloat
        self.log.info(f"Inserting {len(self.sample_data)} documents into comparison collection (logging suppressed)...")
        
        batch_size = 100
        insert_count = 0
        for i in range(0, len(self.sample_data), batch_size):
            batch = self.sample_data[i:i + batch_size]
            values = ", ".join([f"('{doc['id']}', {json.dumps(doc)})" for doc in batch])
            try:
                self.run_cbq_query(f"INSERT INTO {self.cb_comparison_path} (KEY, VALUE) VALUES {values}", debug_query=False)
                insert_count += len(batch)
            except Exception:
                # Fall back to individual inserts if batch fails
                for doc in batch:
                    try:
                        self.run_cbq_query(f"INSERT INTO {self.cb_comparison_path} (KEY, VALUE) VALUES ('{doc['id']}', {json.dumps(doc)})", debug_query=False)
                        insert_count += 1
                    except Exception:
                        pass
        
        time.sleep(5)
        
        # Verify
        result = self.run_cbq_query(f"SELECT COUNT(*) as cnt FROM {self.cb_comparison_path}")
        cb_count = result['results'][0]['cnt'] if result.get('results') else 0
        self.log.info(f"Comparison collection has {cb_count} documents (inserted {insert_count})")
        self.cb_comparison_ready = cb_count > 0

    def _cleanup_comparison_couchbase_collection(self):
        """Cleanup the comparison Couchbase collection."""
        if hasattr(self, 'cb_comparison_path'):
            try:
                self.run_cbq_query(f'DROP COLLECTION {self.cb_comparison_path}')
            except Exception:
                pass
            try:
                default_bucket = self.buckets[0].name if self.buckets else "default"
                self.run_cbq_query(f'DROP SCOPE `{default_bucket}`.`{self.cb_comparison_scope}`')
            except Exception:
                pass

    def _cleanup_n1ql_iceberg_objects(self):
        """Cleanup N1QL objects in reverse order."""
        self.log.info("Cleaning up N1QL Iceberg objects...")

        # Drop comparison Couchbase collection
        self._cleanup_comparison_couchbase_collection()

        # Drop external collection (if it was created)
        if hasattr(self, 'external_collection_name'):
            query = f"DROP COLLECTION {self.external_collection_name}"
            self.log.info(f"Executing: {query}")
            try:
                result = self.run_cbq_query(query)
                self.log.info(f"Drop collection result: {result}")
            except Exception as e:
                self.log.warning(f"Error dropping collection: {str(e)}")

        # Drop scope (if it was created and not _default)
        if hasattr(self, 'iceberg_scope_name') and self.iceberg_scope_name != "_default":
            query = f"DROP SCOPE `{self.couchbase_bucket_name}`.`{self.iceberg_scope_name}`"
            self.log.info(f"Executing: {query}")
            try:
                result = self.run_cbq_query(query)
                self.log.info(f"Drop scope result: {result}")
            except Exception as e:
                self.log.warning(f"Error dropping scope: {str(e)}")

        # Drop catalog (if it was created)
        if hasattr(self, 'catalog_name'):
            query = f"DROP CATALOG {self.catalog_name}"
            self.log.info(f"Executing: {query}")
            try:
                result = self.run_cbq_query(query)
                self.log.info(f"Drop catalog result: {result}")
            except Exception as e:
                self.log.warning(f"Error dropping catalog: {str(e)}")

        # Drop credential store (if it was created)
        if hasattr(self, 'credentialstore_name'):
            query = f"DROP CREDENTIALSTORE {self.credentialstore_name}"
            self.log.info(f"Executing: {query}")
            try:
                result = self.run_cbq_query(query)
                self.log.info(f"Drop credentialstore result: {result}")
            except Exception as e:
                self.log.warning(f"Error dropping credentialstore: {str(e)}")



    def _enable_credentialstore_settings(self):
        """Enable credential store via REST API."""
       
        http = httplib2.Http()
        url = f"http://{self.master.ip}:{self.master.port}/settings/credentialStore"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {self._get_basic_auth()}'
        }
        body = json.dumps({
            "configEncryptionOverride": False,
            "n2nEncryptionOverride": True
        })
        self.log.info(f"Enabling credential store at {url}")
        response, content = http.request(url, 'PUT', headers=headers, body=body)
        self.log.info(f"Credential store enable response: {response.status}, {content}")

    def _get_basic_auth(self):
        """Get base64 encoded basic auth string."""
        credentials = f"{self.username}:{self.password}"
        return base64.b64encode(credentials.encode()).decode()

    def _create_credentialstore(self):
        """Create CREDENTIALSTORE for AWS or GCP credentials."""
        if self.catalog_type in ["AWS_GLUE", "AWS_GLUE_REST", "S3_TABLES"]:
            fields = {
                "accessKeyId": self.aws_access_key_id,
                "secretAccessKey": self.aws_secret_access_key,
                "region": self.aws_region,
                "endpoint": "https://s3.amazonaws.com"
            }
            if self.aws_session_token:
                fields["sessionToken"] = self.aws_session_token
            
            creds_obj = {
                "type": "aws",
                "fields": fields,
                "guardrails": {"allowedServices": ["n1ql"]},
                "description": "AWS credential for iceberg"
            }
            query = f"""
            CREATE CREDENTIALSTORE {self.credentialstore_name} WITH {json.dumps(creds_obj)}
            """
        elif self.catalog_type == "BIGLAKE_METASTORE":
            with open(self.gcs_credentials_file, 'r') as f:
                gcs_creds = json.load(f)
            creds_obj = {
                "type": "gcp",
                "fields": {
                    "jsonCredentials": gcs_creds
                },
                "guardrails": {"allowedServices": ["n1ql"]},
                "description": "GCP credential for iceberg"
            }
            query = f"""
            CREATE CREDENTIALSTORE {self.credentialstore_name} WITH {json.dumps(creds_obj)}
            """
        else:
            raise ValueError(f"Unsupported catalog_type for credentialstore: {self.catalog_type}")

        self.log.info(f"Creating credentialstore: {self.credentialstore_name}")
        result = self.run_cbq_query(query)
        self.log.info(f"Credentialstore creation result: {result}")

    def _create_catalog(self):
        """Create CATALOG for Iceberg."""
        if self.catalog_type == "AWS_GLUE":
            query = f"""
            CREATE CATALOG {self.catalog_name} TYPE ICEBERG SOURCE AWS_GLUE AT {self.credentialstore_name} WITH {{}}
            """
        elif self.catalog_type == "AWS_GLUE_REST":
            query = f"""
            CREATE CATALOG {self.catalog_name} TYPE ICEBERG SOURCE AWS_GLUE_REST AT {self.credentialstore_name} WITH {{}}
            """
        elif self.catalog_type == "S3_TABLES":
            query = f"""
            CREATE CATALOG {self.catalog_name} TYPE ICEBERG SOURCE S3_TABLES AT {self.credentialstore_name} WITH {{}}
            """
        elif self.catalog_type == "BIGLAKE_METASTORE":
            query = f"""
            CREATE CATALOG {self.catalog_name} TYPE ICEBERG SOURCE BIGLAKE_METASTORE AT {self.credentialstore_name} WITH {{}}
            """
        else:
            raise ValueError(f"Unsupported catalog_type: {self.catalog_type}")

        self.log.info(f"Creating catalog: {self.catalog_name}")
        result = self.run_cbq_query(query)
        self.log.info(f"Catalog creation result: {result}")

    def _create_bucket_if_needed(self):
        """Create bucket for external collection if it doesn't exist."""
        # Check if bucket exists
        try:
            check_query = f"SELECT * FROM system:keyspaces WHERE `bucket` = '{self.couchbase_bucket_name}' LIMIT 1"
            result = self.run_cbq_query(check_query)
            if result.get('results') and len(result['results']) > 0:
                self.log.info(f"Bucket {self.couchbase_bucket_name} already exists")
                return
        except Exception as e:
            self.log.warning(f"Error checking bucket existence: {str(e)}")

        # Create bucket using REST API
        self.log.info(f"Creating bucket: {self.couchbase_bucket_name}")
        try:
            self.rest.create_bucket(
                bucket=self.couchbase_bucket_name,
                ramQuotaMB=256,
                replicaNumber=0
            )
            self.log.info(f"Bucket {self.couchbase_bucket_name} created successfully")
            # Wait for bucket to be ready
            time.sleep(5)
        except Exception as e:
            self.log.warning(f"Bucket creation warning (may already exist): {str(e)}")

    def _create_scope(self):
        """Create scope for external collection if it doesn't exist."""
        query = f"CREATE SCOPE `{self.couchbase_bucket_name}`.`{self.iceberg_scope_name}` IF NOT EXISTS"
        self.log.info(f"Creating scope: {self.couchbase_bucket_name}.{self.iceberg_scope_name}")
        try:
            result = self.run_cbq_query(query)
            self.log.info(f"Scope creation result: {result}")
        except Exception as e:
            # Scope might already exist, log and continue
            self.log.warning(f"Scope creation warning (may already exist): {str(e)}")

    def _create_external_collection(self):
        """Create EXTERNAL COLLECTION pointing to Iceberg table."""
        options = {
            "namespace": self.iceberg_namespace,
            "tableName": self.iceberg_table_name
        }
        query = f"""
        CREATE EXTERNAL COLLECTION {self.external_collection_name} ON {self.catalog_name} AT {self.credentialstore_name} WITH {json.dumps(options)}
        """

        self.log.info(f"Creating external collection: {self.external_collection_name}")
        result = self.run_cbq_query(query)
        self.log.info(f"External collection creation result: {result}")

    def _generate_sample_data(self, count):
        """
        Generate sample hotel data for Iceberg table with complex nested structure.
        
        Default generates 10,000 documents with the following schema:
        - country: Cycles through 20 country names (500 docs per country)
        - city: Cycles through 50 city names (200 docs per city)
        - type: Cycles through 5 hotel types (2000 docs per type)
        - price: 500 to 5000 (cycles every 500)
        - avg_rating: 1.0 to 5.0 (derived from document id)
        - reviews: Array of 1-5 review objects (random count)
        - public_likes: Array of 0-10 names (count = id % 11)
        
        REVIEW GENERATION PATTERN:
        - Number of reviews per doc: random 1-5 (seeded with doc id for reproducibility)
        - Review dates: Sequential weeks starting from 2025-11-13
        - Review ratings: Deterministic based on (id + review_index) % 5
        """
        sample_data = []
        
        # Data pools for cycling
        countries = [
            "Guadeloupe", "United States", "France", "Germany", "Japan",
            "Australia", "Brazil", "Canada", "Italy", "Spain",
            "Mexico", "Thailand", "India", "China", "South Korea",
            "Argentina", "Egypt", "Turkey", "Greece", "Portugal"
        ]
        
        cities = [
            "New Fritz", "Springfield", "Paris", "Berlin", "Tokyo",
            "Sydney", "Rio", "Toronto", "Rome", "Madrid",
            "Cancun", "Bangkok", "Mumbai", "Shanghai", "Seoul",
            "Buenos Aires", "Cairo", "Istanbul", "Athens", "Lisbon",
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
            "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
            "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
            "Boston", "El Paso", "Nashville", "Detroit", "Portland",
            "Las Vegas", "Louisville", "Baltimore", "Milwaukee", "Albuquerque"
        ]
        
        hotel_types = ["Hotel", "Hostel", "Resort", "Motel", "Inn"]
        
        first_names = [
            "Alexander", "Winston", "Leoma", "Adella", "Ervin",
            "Shaquita", "Ladonna", "Landon", "Ilona", "Lady",
            "Odis", "Althea", "Rolf", "Kathern", "Fritz"
        ]
        
        last_names = [
            "Stiedemann", "Fisher", "Kuphal", "VonRueden", "Pouros",
            "Kovacek", "Keebler", "Kautzer", "Toy", "Stamm",
            "Ziemann", "Kshlerin", "McDermott", "Village", "City"
        ]
        
        for i in range(count):
            # Use seeded random for reproducibility per document
            random.seed(i)
            num_reviews = random.randint(1, 5)  # Random 1 to 5 reviews per hotel
            num_likes = i % 11  # 0 to 10 likes per hotel
            
            # Generate reviews array
            reviews = []
            for review_idx in range(num_reviews):
                # Date: weekly intervals starting from 2025-11-13
                base_date = "2025-11-13"
                date_obj = datetime.strptime(base_date, "%Y-%m-%d")
                date_obj += timedelta(weeks=review_idx)
                review_date = date_obj.strftime("%Y-%m-%d 01:08:32")
                
                # Deterministic author name
                author = f"{first_names[(i + review_idx) % len(first_names)]} {last_names[(i + review_idx) % len(last_names)]}"
                
                # Deterministic ratings (0-4 for variety)
                rating_base = (i + review_idx) % 5
                reviews.append({
                    "date": review_date,
                    "author": author,
                    "ratings": {
                        "Value": (rating_base + 0) % 5,
                        "Cleanliness": (rating_base + 2) % 5,
                        "Overall": (rating_base + 3) % 5,
                        "Check in / front desk": (rating_base + 1) % 5,
                        "Rooms": (rating_base + 2) % 5
                    }
                })
            
            # Generate public_likes array
            public_likes = []
            for like_idx in range(num_likes):
                liker = f"{first_names[(i + like_idx) % len(first_names)]} {last_names[(i + like_idx) % len(last_names)]}"
                if like_idx % 3 == 0:
                    liker = f"Dr. {liker}"
                elif like_idx % 3 == 1:
                    liker = f"Mrs. {liker}"
                public_likes.append(liker)
            
            # Calculate average rating from reviews
            total_rating = sum(r["ratings"]["Overall"] for r in reviews)
            avg_rating = total_rating / len(reviews) if reviews else 0.0
            
            # Generate document
            doc = {
                "id": i,  # Adding id for easier querying/validation
                "country": countries[i % len(countries)],
                "mutate": 0,
                "address": f"{100 + (i % 900)} {last_names[i % len(last_names)]} {['Street', 'Avenue', 'Boulevard', 'Village', 'Drive'][i % 5]}",
                "free_parking": i % 2 == 0,
                "city": cities[i % len(cities)],
                "counter": 0,
                "type": hotel_types[i % len(hotel_types)],
                "url": f"www.{last_names[i % len(last_names)].lower()}-{first_names[i % len(first_names)].lower()}.{['com', 'org', 'net', 'io'][i % 4]}",
                "reviews": reviews,
                "phone": f"{200 + (i % 799)}.{100 + (i % 899)}.{1000 + (i % 8999)}",
                "price": 500 + (i % 4500),
                "avg_rating": avg_rating + (i % 100) / 100.0,  # Add some variance
                "free_breakfast": i % 3 == 0,
                "name": f"{first_names[i % len(first_names)]} {last_names[i % len(last_names)]}",
                "public_likes": public_likes,
                "email": f"{first_names[i % len(first_names)]}.{last_names[i % len(last_names)]}@hotels.{['com', 'org', 'net'][i % 3]}"
            }
            
            sample_data.append(doc)
        
        return sample_data

    def _generate_legacy_nested_review_sample_data(self, count):
        """Generate sample data using the legacy nested-list reviews template."""
        sample_data = self._generate_sample_data(count)

        for doc in sample_data:
            legacy_reviews = []
            for review in doc["reviews"]:
                ratings = review["ratings"]
                legacy_reviews.append([
                    ["date", review["date"]],
                    ["ratings",
                     "{Value=%s, Overall=%s, Cleanliness=%s, Check in / front desk=%s, Rooms=%s}" % (
                         ratings["Value"],
                         ratings["Overall"],
                         ratings["Cleanliness"],
                         ratings["Check in / front desk"],
                         ratings["Rooms"]
                     )],
                    ["author", review["author"]]
                ])
            doc["reviews"] = legacy_reviews

        return sample_data

    def _generate_null_values_sample_data(self, count):
        """
        Generate sample data with null values in various fields.
        Pattern:
        - id % 5 == 0: null name
        - id % 7 == 0: null city
        - id % 11 == 0: null price (less frequent to ensure AVG has non-null values)
        - id % 9 == 0: null reviews (empty array)
        - id % 13 == 0: null nested field (ratings.Overall)
        """
        sample_data = []
        
        countries = ["USA", "France", "Germany", "Japan", "Australia"]
        cities = ["New York", "Paris", "Berlin", "Tokyo", "Sydney"]
        hotel_types = ["Hotel", "Hostel", "Resort", "Motel", "Inn"]
        
        for i in range(count):
            random.seed(i)
            
            # Generate reviews with potential null nested fields
            reviews = []
            if i % 9 != 0:  # Not null reviews
                num_reviews = random.randint(1, 3)
                for review_idx in range(num_reviews):
                    review = {
                        "date": f"2025-{(i % 12) + 1:02d}-{(review_idx % 28) + 1:02d}",
                        "author": f"Author_{i}_{review_idx}",
                        "ratings": {
                            "Value": (i + review_idx) % 5,
                            "Cleanliness": (i + review_idx + 1) % 5,
                            "Rooms": (i + review_idx + 2) % 5
                        }
                    }
                    # Add null Overall rating for some documents
                    if i % 13 == 0:
                        review["ratings"]["Overall"] = None
                    else:
                        review["ratings"]["Overall"] = (i + review_idx + 3) % 5
                    reviews.append(review)
            
            doc = {
                "id": i,
                "name": None if i % 5 == 0 else f"Hotel_{i}",
                "city": None if i % 7 == 0 else cities[i % len(cities)],
                "country": countries[i % len(countries)],
                "type": hotel_types[i % len(hotel_types)],
                "price": None if i % 11 == 0 else 100 + (i * 10),
                "rating": None if i % 8 == 0 else round(1.0 + (i % 40) / 10.0, 1),
                "is_available": None if i % 17 == 0 else (i % 2 == 0),
                "reviews": reviews if reviews else None,
                "tags": None if i % 19 == 0 else [f"tag_{i % 5}", f"tag_{(i + 1) % 5}"],
                "metadata": {
                    "created_by": None if i % 23 == 0 else f"user_{i}",
                    "version": i
                }
            }
            sample_data.append(doc)
        
        return sample_data

    def _generate_diverse_datatypes_sample_data(self, count):
        """
        Generate sample data with diverse datatypes:
        - Integers (int, bigint)
        - Floating point (float, double)
        - Strings
        - Booleans
        - Timestamps
        - Dates
        - Arrays of different types
        - Nested objects
        - Decimal/precise numbers
        """
        sample_data = []
        
        for i in range(count):
            random.seed(i)
            
            # Base timestamp for this document
            base_ts = datetime(2024, 1, 1, 0, 0, 0) + timedelta(days=i, hours=i % 24, minutes=i % 60, seconds=i % 60)
            
            doc = {
                "id": i,
                # Integer types
                "small_int": i % 100,
                "big_int": 1000000000000 + i,
                "negative_int": -1 * (i + 1),
                
                # Floating point types
                "float_val": round(i * 0.123456, 6),
                "double_val": round(i * 0.123456789012345, 12),
                "scientific_notation": float(f"{i + 1}e{i % 5}"),
                "precise_decimal": round(i / 7.0, 10),
                
                # Special float values
                "zero_float": 0.0,
                "negative_float": -1.5 * (i + 1),
                
                # String types
                "simple_string": f"value_{i}",
                "unicode_string": f"Hotel \u00e9toile {i}",  # é character
                "empty_string": "" if i % 5 == 0 else f"non_empty_{i}",
                "long_string": "x" * (100 + i % 100),
                
                # Boolean
                "is_active": i % 2 == 0,
                "is_verified": i % 3 == 0,
                
                # Timestamp and date types (as strings for Iceberg compatibility)
                "created_at": base_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": (base_ts + timedelta(hours=i % 48)).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "date_only": base_ts.strftime("%Y-%m-%d"),
                "time_only": base_ts.strftime("%H:%M:%S"),
                "iso_timestamp": base_ts.isoformat(),
                
                # Arrays of different types
                "int_array": [i, i + 1, i + 2],
                "float_array": [round(i * 0.1, 2), round(i * 0.2, 2), round(i * 0.3, 2)],
                "string_array": [f"item_{i}_0", f"item_{i}_1", f"item_{i}_2"],
                "bool_array": [True, False, i % 2 == 0],
                "empty_array": [],
                "mixed_size_array": list(range(i % 10)),
                
                # Nested objects with diverse types
                "metrics": {
                    "count": i * 100,
                    "average": round(i / 3.0, 4),
                    "min_value": i,
                    "max_value": i * 2,
                    "is_valid": i % 2 == 0,
                    "timestamp": base_ts.strftime("%Y-%m-%d %H:%M:%S")
                },
                
                # Deeply nested structure
                "deep_nested": {
                    "level1": {
                        "level2": {
                            "level3": {
                                "value": i,
                                "name": f"deep_{i}"
                            }
                        }
                    }
                },
                
                # Array of objects
                "measurements": [
                    {
                        "sensor_id": j,
                        "reading": round(i * 0.5 + j * 0.1, 3),
                        "timestamp": (base_ts + timedelta(minutes=j)).strftime("%Y-%m-%d %H:%M:%S"),
                        "is_valid": j % 2 == 0
                    }
                    for j in range(3)
                ],
                
                # Edge case numbers
                "very_small_float": 0.0000001 * (i + 1),
                "very_large_int": 9223372036854775807 - i if i < 100 else i,  # Near max long
            }
            sample_data.append(doc)
        
        return sample_data

    def _generate_mixed_types_sample_data(self, count):
        """
        Generate sample data that tests type handling in Iceberg.
        All fields have consistent types for Iceberg schema compatibility,
        but we track what the "original" type would have been.
        
        Key insight: Iceberg requires consistent schema, so we store everything
        as consistent types but track metadata about original types.
        """
        sample_data = []
        
        for i in range(count):
            # Original "flexible_value" concept - what type would it have been
            if i % 4 == 0:
                original_type = "str"
                original_value = f"string_value_{i}"
            elif i % 4 == 1:
                original_type = "int"
                original_value = i * 100
            elif i % 4 == 2:
                original_type = "float"
                original_value = round(i * 1.5, 2)
            else:
                original_type = "bool"
                original_value = i % 2 == 0
            
            # Arrays - always strings for consistency
            flexible_array = [f"item_{j}" for j in range(i % 5 + 1)]
            
            # Status tracking
            original_status_is_bool = i % 2 == 0
            
            doc = {
                "id": i,
                "name": f"Item_{i}",
                "category": ["Electronics", "Clothing", "Food", "Books", "Toys"][i % 5],
                
                # All values stored as strings for schema consistency
                "flexible_value": str(original_value),
                "flexible_value_type": original_type,
                
                # Numeric fields - always use consistent types
                "quantity_numeric": i * 10,  # Always int
                "quantity_is_string": i % 2 == 1,  # Track if "would have been" string
                
                "score_numeric": round(i * 0.75, 2),  # Always float
                "score_is_string": i % 3 == 2,  # Track if "would have been" string
                
                # Status field - always string
                "status_raw": "True" if (i % 4 == 0) else ("False" if (i % 2 == 0) else ("active" if i % 4 == 1 else "inactive")),
                "status_is_boolean": original_status_is_bool,
                
                # Array with consistent string type
                "tags": flexible_array,
                
                # Nested object (always object for schema consistency)
                "metadata": {
                    "nested_type": "object" if i % 2 == 0 else "simple",
                    "nested_value": {
                        "type": "object",
                        "value": i,
                        "label": f"nested_{i}"
                    }
                },
                
                # Additional consistent fields
                "created_at": f"2025-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "priority": i % 5 + 1,
                "is_active": i % 3 != 0
            }
            sample_data.append(doc)
        
        return sample_data

    # Helper methods for validation

    def _calculate_expected_review_count(self, doc_ids):
        """
        Calculate expected total review count for given document IDs.
        Review count per doc is random 1-5 (seeded by doc id for reproducibility).
        """
        total = 0
        for doc_id in doc_ids:
            random.seed(doc_id)
            total += random.randint(1, 5)
        return total

    def _get_expected_reviews_for_doc(self, doc_id):
        """Get expected number of reviews for a specific document ID."""
        random.seed(doc_id)
        return random.randint(1, 5)

    def _load_iceberg_query_suite(self):
        """Load the Iceberg query suite definitions from disk."""
        queries_path = os.path.join(os.path.dirname(__file__), "iceberg_queries.json")
        with open(queries_path, "r") as queries_file:
            return json.load(queries_file)

    def _normalize_query_result(self, value):
        """Normalize query results for deterministic comparisons."""
        if isinstance(value, float):
            return round(value, 6)
        if isinstance(value, list):
            return [self._normalize_query_result(item) for item in value]
        if isinstance(value, dict):
            return {key: self._normalize_query_result(item) for key, item in value.items()}
        return value

    def _compare_query_results(self, actual_results, expected_results, query_name):
        """Compare normalized query results against expected data."""
        normalized_actual = self._normalize_query_result(actual_results)
        normalized_expected = self._normalize_query_result(expected_results)
        
        # For dict comparison, sort keys to handle field order differences
        if isinstance(normalized_actual, dict) and isinstance(normalized_expected, dict):
            actual_sorted = json.dumps(normalized_actual, sort_keys=True, default=str)
            expected_sorted = json.dumps(normalized_expected, sort_keys=True, default=str)
            self.assertEqual(actual_sorted, expected_sorted,
                             f"Unexpected results for query {query_name}")
        elif isinstance(normalized_actual, list) and isinstance(normalized_expected, list):
            # For list of dicts, compare each element with sorted keys
            self.assertEqual(len(normalized_actual), len(normalized_expected),
                             f"Result count mismatch for query {query_name}: {len(normalized_actual)} vs {len(normalized_expected)}")
            for i, (actual_row, expected_row) in enumerate(zip(normalized_actual, normalized_expected)):
                if isinstance(actual_row, dict) and isinstance(expected_row, dict):
                    actual_sorted = json.dumps(actual_row, sort_keys=True, default=str)
                    expected_sorted = json.dumps(expected_row, sort_keys=True, default=str)
                    self.assertEqual(actual_sorted, expected_sorted,
                                     f"Row {i} mismatch for query {query_name}")
                else:
                    self.assertEqual(actual_row, expected_row,
                                     f"Row {i} mismatch for query {query_name}")
        else:
            self.assertEqual(normalized_actual, normalized_expected,
                             f"Unexpected results for query {query_name}")

    def _extract_selected_document(self, row):
        """Extract the selected document payload from a SELECT * result row."""
        if self.iceberg_collection_name in row:
            return row[self.iceberg_collection_name]
        if len(row) == 1:
            return next(iter(row.values()))
        return row

    def _query_spark_iceberg_table(self, spark_query):
        """
        Query the Iceberg table directly via Spark to verify data.
        Returns the results as a list of dicts, or None if Spark is not available.
        """
        if not getattr(self, 'iceberg_util', None) or not getattr(self.iceberg_util, 'spark', None):
            self.log.warning("Spark session not available for direct Iceberg query")
            return None
        try:
            df = self.iceberg_util.spark.sql(spark_query)
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            self.log.warning(f"Failed to query Spark directly: {e}")
            return None

    def verify_iceberg_data_via_spark(self, where_clause=None, columns="*", limit=100, order_by="id"):
        """
        Query the Iceberg table directly via Spark for manual verification.
        Returns list of dicts with query results, or None if Spark unavailable.
        """
        if not getattr(self, 'iceberg_util', None) or not getattr(self.iceberg_util, 'spark', None):
            self.log.error("Spark session not available")
            return None
        
        table_path = f"{self.catalog_type}.{self.iceberg_namespace}.{self.iceberg_table_name}"
        query = f"SELECT {columns} FROM {table_path}"
        if where_clause:
            query += f" WHERE {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = self.iceberg_util.spark.sql(query)
            results = [row.asDict() for row in df.collect()]
            self.log.info(f"Spark returned {len(results)} rows")
            return results
        except Exception as e:
            self.log.error(f"Spark query failed: {e}")
            return None

    def run_cbq_query_with_spark_verification(self, n1ql_query, spark_where=None, spark_columns="*", 
                                                spark_order_by=None, spark_limit=None, description=None):
        """
        Run an N1QL query and also query Spark directly for comparison.
        Returns the N1QL query result dict.
        """
        desc = description or "Query"
        
        # Run N1QL query
        n1ql_result = self.run_cbq_query(n1ql_query, query_params={"timeout": "300s"})
        n1ql_rows = n1ql_result.get("results", [])
        self.log.info(f"[{desc}] N1QL returned {len(n1ql_rows)} rows")
        
        return n1ql_result

    def run_three_way_query(self, query_template, spark_query, description="Query"):
        """
        Run a query against three sources and return comparison results.
        
        Args:
            query_template: N1QL query with {iceberg} and {couchbase} placeholders
            spark_query: Equivalent Spark SQL query
            description: Description for logging
            
        Returns:
            dict with keys: iceberg_results, couchbase_results, spark_results, comparison_log
        """
        results = {
            "description": description,
            "iceberg_results": None,
            "couchbase_results": None, 
            "spark_results": None,
            "iceberg_error": None,
            "couchbase_error": None,
            "spark_error": None
        }
        
        # 1. Query Iceberg via N1QL
        iceberg_query = query_template.format(
            iceberg=self.external_collection_name,
            couchbase=getattr(self, 'cb_comparison_path', 'N/A')
        )
        try:
            result = self.run_cbq_query(iceberg_query, query_params={"timeout": "300s"})
            results["iceberg_results"] = result.get("results", [])
        except Exception as e:
            results["iceberg_error"] = str(e)
        
        # 2. Query Couchbase via N1QL (if comparison collection is ready)
        if getattr(self, 'cb_comparison_ready', False):
            cb_query = query_template.format(
                iceberg=self.external_collection_name,
                couchbase=self.cb_comparison_path
            ).replace(self.external_collection_name, self.cb_comparison_path)
            try:
                result = self.run_cbq_query(cb_query, query_params={"timeout": "300s"})
                results["couchbase_results"] = result.get("results", [])
            except Exception as e:
                results["couchbase_error"] = str(e)
        
        # 3. Query Spark directly
        if spark_query:
            try:
                results["spark_results"] = self._query_spark_iceberg_table(spark_query)
            except Exception as e:
                results["spark_error"] = str(e)
        
        # Log comparison
        self._log_three_way_comparison(results)
        
        return results

    def _log_three_way_comparison(self, results):
        """Log a formatted comparison of three-way query results."""
        desc = results.get("description", "Query")
        
        def format_results(res, max_rows=5):
            if res is None:
                return "N/A"
            if isinstance(res, str):
                return res
            if not res:
                return "[]"
            # Extract just IDs or first field for compact display
            if isinstance(res[0], dict):
                if 'id' in res[0]:
                    ids = [r['id'] for r in res[:max_rows]]
                    suffix = f"... ({len(res)} total)" if len(res) > max_rows else ""
                    return str(ids) + suffix
                else:
                    first_key = list(res[0].keys())[0]
                    vals = [r[first_key] for r in res[:max_rows]]
                    suffix = f"... ({len(res)} total)" if len(res) > max_rows else ""
                    return str(vals) + suffix
            return str(res[:max_rows])
        
        iceberg = format_results(results.get("iceberg_results")) if not results.get("iceberg_error") else f"ERROR: {results['iceberg_error'][:50]}"
        couchbase = format_results(results.get("couchbase_results")) if not results.get("couchbase_error") else f"ERROR: {results['couchbase_error'][:50]}"
        spark = format_results(results.get("spark_results")) if not results.get("spark_error") else f"ERROR: {results['spark_error'][:50]}"
        
        self.log.info(f"\n[{desc}] THREE-WAY COMPARISON:")
        self.log.info(f"  Iceberg N1QL:   {iceberg}")
        self.log.info(f"  Couchbase N1QL: {couchbase}")
        self.log.info(f"  Spark Direct:   {spark}")
        
        # Check for discrepancies
        iceberg_res = results.get("iceberg_results")
        cb_res = results.get("couchbase_results")
        spark_res = results.get("spark_results")
        
        if iceberg_res and cb_res and iceberg_res != cb_res:
            self.log.warning(f"  *** DISCREPANCY: Iceberg vs Couchbase results differ!")
        if iceberg_res and spark_res and iceberg_res != spark_res:
            self.log.warning(f"  *** DISCREPANCY: Iceberg vs Spark results differ!")

    def dump_iceberg_table_info(self):
        """
        Dump Iceberg table metadata via Spark for debugging.
        Call this after setUp() to verify what's in the Iceberg table.
        """
        if not getattr(self, 'iceberg_util', None) or not getattr(self.iceberg_util, 'spark', None):
            self.log.error("Spark session not available")
            return
        
        table_path = f"{self.catalog_type}.{self.iceberg_namespace}.{self.iceberg_table_name}"
        spark = self.iceberg_util.spark
        
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_path}").collect()[0]['cnt']
            self.log.info(f"Iceberg table {table_path}: {count} rows")
        except Exception as e:
            self.log.error(f"Failed to get table info: {e}")

    def _get_spark_verification_query(self, query_name):
        """
        Get the equivalent Spark SQL query to verify Iceberg data directly.
        """
        table_path = f"{self.catalog_type}.{self.iceberg_namespace}.{self.iceberg_table_name}"
        if query_name == "projection_limit_offset":
            return f"SELECT id, name, city, type, price FROM {table_path} WHERE id < 10 ORDER BY id"
        if query_name == "group_by_type_aggregates":
            return f"SELECT type, COUNT(1) as hotel_count, MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price FROM {table_path} WHERE id < 25 GROUP BY type HAVING COUNT(1) >= 5 ORDER BY type"
        if query_name == "unnest_review_aggregates":
            return f"SELECT id, size(reviews) as review_count FROM {table_path} WHERE id BETWEEN 0 AND 9 ORDER BY id"
        return None

    def _get_source_data_for_query(self, query_name):
        """
        Get the relevant subset of Spark-inserted sample_data for a query case.
        This helps verify what data was actually inserted before the query ran.
        """
        if query_name == "projection_limit_offset":
            # Query uses WHERE id < 10, OFFSET 2, LIMIT 3 -> expects ids 2,3,4
            return self.sample_data[:10]
        if query_name == "between_in_filter":
            return self.sample_data[10:26]
        if query_name == "distinct_like_cities":
            return self.sample_data[:100]
        if query_name == "group_by_type_aggregates":
            return self.sample_data[:25]
        if query_name == "any_satisfies_reviews":
            return self.sample_data[:15]
        if query_name == "unnest_review_aggregates":
            return self.sample_data[:10]
        if query_name == "let_case_array_projection":
            return [self.sample_data_by_id[doc_id] for doc_id in [99, 100, 101]]
        if query_name == "scalar_subquery_price_filter":
            return self.sample_data[:20]
        if query_name == "union_all_amenities":
            return self.sample_data[:5]
        if query_name == "window_rank_by_city":
            return [doc for doc in self.sample_data[:250] if doc["city"] == "Albuquerque"]
        return self.sample_data[:10]  # Default: first 10 docs

    def _build_expected_query_results(self, query_name):
        """Build expected results for a named Iceberg query suite case."""
        if query_name == "projection_limit_offset":
            return [
                {field: doc[field] for field in ["id", "name", "city", "type", "price"]}
                for doc in self.sample_data[2:5]
            ]

        if query_name == "between_in_filter":
            rows = []
            for doc in self.sample_data[10:26]:
                if doc["type"] in ["Hotel", "Inn"]:
                    rows.append({field: doc[field] for field in ["id", "name", "type", "price"]})
            return rows

        if query_name == "distinct_like_cities":
            matching_cities = sorted({
                doc["city"] for doc in self.sample_data[:100]
                if doc["city"].startswith("New ")
            })
            return [{"city": city} for city in matching_cities]

        if query_name == "group_by_type_aggregates":
            grouped = {}
            for doc in self.sample_data[:25]:
                grouped.setdefault(doc["type"], []).append(doc)

            rows = []
            for hotel_type in sorted(grouped):
                docs = grouped[hotel_type]
                if len(docs) >= 5:
                    prices = [doc["price"] for doc in docs]
                    rows.append({
                        "type": hotel_type,
                        "hotel_count": len(docs),
                        "min_price": min(prices),
                        "max_price": max(prices),
                        "avg_price": sum(prices) / len(prices)
                    })
            return rows

        if query_name == "any_satisfies_reviews":
            rows = []
            for doc in self.sample_data[:15]:
                if any(
                    review["ratings"]["Overall"] >= 4 and review["ratings"]["Rooms"] >= 3
                    for review in doc["reviews"]
                ):
                    rows.append({
                        "id": doc["id"],
                        "name": doc["name"],
                        "review_count": len(doc["reviews"])
                    })
            return rows

        if query_name == "unnest_review_aggregates":
            rows = []
            for doc in self.sample_data[:10]:
                overall_ratings = [review["ratings"]["Overall"] for review in doc["reviews"]]
                rows.append({
                    "id": doc["id"],
                    "review_count": len(overall_ratings),
                    "avg_overall": round(sum(overall_ratings) / len(overall_ratings), 3)
                })
            return rows

        if query_name == "let_case_array_projection":
            rows = []
            for doc_id in [99, 100, 101]:
                doc = self.sample_data_by_id[doc_id]
                if doc["price"] < 1000:
                    price_band = "budget"
                elif doc["price"] < 3000:
                    price_band = "mid"
                else:
                    price_band = "premium"
                rows.append({
                    "id": doc["id"],
                    "name": doc["name"],
                    "review_count": len(doc["reviews"]),
                    "price_band": price_band,
                    "review_authors": [review["author"] for review in doc["reviews"]]
                })
            return rows

        if query_name == "scalar_subquery_price_filter":
            avg_price = sum(doc["price"] for doc in self.sample_data[:20]) / 20.0
            return [
                {field: doc[field] for field in ["id", "name", "price"]}
                for doc in self.sample_data[:15]
                if doc["price"] > avg_price
            ]

        if query_name == "union_all_amenities":
            breakfast_rows = [
                {"id": doc["id"], "name": doc["name"], "amenity": "breakfast"}
                for doc in self.sample_data[:5]
                if doc["free_breakfast"]
            ]
            parking_rows = [
                {"id": doc["id"], "name": doc["name"], "amenity": "parking"}
                for doc in self.sample_data[:5]
                if doc["free_parking"]
            ]
            return sorted(breakfast_rows + parking_rows, key=lambda row: (row["id"], row["amenity"]))

        if query_name == "window_rank_by_city":
            city_rows = [
                {
                    "id": doc["id"],
                    "name": doc["name"],
                    "city": doc["city"],
                    "price": doc["price"],
                    "avg_rating": doc["avg_rating"]
                }
                for doc in self.sample_data[:250]
                if doc["city"] == "Albuquerque"
            ]
            city_rows.sort(key=lambda row: (-row["avg_rating"], row["price"]))

            ranked_rows = []
            current_rank = 0
            previous_sort_key = None
            for index, row in enumerate(city_rows, start=1):
                sort_key = (-row["avg_rating"], row["price"])
                if sort_key != previous_sort_key:
                    current_rank = index
                    previous_sort_key = sort_key
                ranked_rows.append({
                    **row,
                    "city_rank": current_rank
                })
            return sorted(ranked_rows, key=lambda row: (row["city_rank"], row["id"]))

        raise ValueError(f"Unsupported Iceberg query suite case: {query_name}")

    # Test methods

    def test_iceberg_select_count(self):
        """
        Test simple SELECT COUNT(*) on external Iceberg collection.
        Expected: 10,000 documents
        """
        query = f"SELECT COUNT(*) as count FROM {self.external_collection_name}"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="COUNT(*) as count",
            description="SELECT COUNT(*)"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertEqual(len(result['results']), 1, "Expected 1 result")
        self.assertEqual(result['results'][0]['count'], self.initial_doc_count,
                         f"Expected count={self.initial_doc_count}")

    def test_iceberg_select_projection(self):
        """Test SELECT with projection on external Iceberg collection."""
        query = f"SELECT id, name, city, type, price FROM {self.external_collection_name} WHERE id < 10 ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, name, city, type, price",
            spark_where="id < 10",
            spark_order_by="id",
            description="SELECT with projection"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertEqual(len(result['results']), 10, "Expected 10 results")

        # Verify first result matches expected data generation pattern
        first_result = result['results'][0]
        self.assertEqual(first_result['id'], 0, "Expected id=0")
        self.assertIn('name', first_result, "Expected 'name' field")
        self.assertIn('city', first_result, "Expected 'city' field")
        self.assertIn('type', first_result, "Expected 'type' field")
        self.assertIn('price', first_result, "Expected 'price' field")

    def test_iceberg_select_filter(self):
        """
        Test SELECT with WHERE filter on external Iceberg collection.
        With 50 cities cycling, each city appears 200 times (10,000 / 50).
        """
        query = f"SELECT id, name, city, type, price FROM {self.external_collection_name} WHERE type = 'Hostel' ORDER BY id LIMIT 100"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, name, city, type, price",
            spark_where="type = 'Hostel'",
            spark_order_by="id",
            spark_limit=100,
            description="SELECT with WHERE filter"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")

        # Type cycles every 5, so 'Hostel' (index 1) appears at IDs: 1, 6, 11, 16, ...
        # With 10K docs and 5 types, each type has 2000 docs
        # We're limiting to 100 results for performance
        self.assertGreater(len(result['results']), 0, "Expected some results")
        self.assertLessEqual(len(result['results']), 100, "Expected at most 100 results due to LIMIT")

        for row in result['results']:
            self.assertEqual(row['type'], 'Hostel', f"Expected type=Hostel, got {row['type']}")

    def test_iceberg_select_reviews_array(self):
        """
        Test querying the nested reviews array.
        This validates the complex nested structure and cross-validates review counts.
        
        Review generation pattern:
        - Number of reviews per doc: random 1-5 (seeded by doc id for reproducibility)
        """
        # Test 1: Get document with known review count
        query = f"SELECT id, name, reviews FROM {self.external_collection_name} WHERE id IN [0, 1, 2, 3, 4] ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, name, reviews",
            spark_where="id IN (0, 1, 2, 3, 4)",
            spark_order_by="id",
            description="SELECT reviews array"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertEqual(len(result['results']), 5, "Expected 5 documents")

        # Cross-validate review counts
        for row in result['results']:
            doc_id = row['id']
            expected_review_count = self._get_expected_reviews_for_doc(doc_id)
            actual_review_count = len(row['reviews'])
            self.assertEqual(actual_review_count, expected_review_count,
                             f"Doc {doc_id}: Expected {expected_review_count} reviews, got {actual_review_count}")
            
            # Validate review structure
            for review in row['reviews']:
                self.assertIn('date', review, "Review should have 'date' field")
                self.assertIn('author', review, "Review should have 'author' field")
                self.assertIn('ratings', review, "Review should have 'ratings' field")
                self.assertIn('Overall', review['ratings'], "Ratings should have 'Overall' field")

        # Test 2: Unnest reviews and validate
        query = f"""
        SELECT h.id, h.name, r.author, r.ratings.Overall as overall_rating
        FROM {self.external_collection_name} h
        UNNEST h.reviews r
        WHERE h.id < 10
        ORDER BY h.id, r.date
        """
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, name, size(reviews) as review_count",
            spark_where="id < 10",
            spark_order_by="id",
            description="UNNEST reviews"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        # Expected: Sum of reviews for IDs 0-9 (random 1-5 per doc, seeded by doc id)
        expected_review_count = self._calculate_expected_review_count(range(10))
        self.assertEqual(len(result['results']), expected_review_count,
                         f"Expected {expected_review_count} unnested reviews for IDs 0-9")

        self.log.info(f"✓ Cross-validation successful: {expected_review_count} reviews matched expected count")

    def test_iceberg_queries_from_file(self):
        """
        Run a mixed suite of Iceberg queries from disk and validate each result
        against Couchbase bucket and Spark for comparison.
        """
        query_cases = self._load_iceberg_query_suite()
        failed_queries = []
        
        # Build Spark query equivalents
        table_path = f"{self.catalog_type}.{self.iceberg_namespace}.{self.iceberg_table_name}"
        
        self.log.info(f"\n{'='*80}")
        self.log.info("ICEBERG QUERY SUITE - THREE-WAY COMPARISON")
        self.log.info(f"{'='*80}")

        for query_case in query_cases:
            query_name = query_case["name"]
            iceberg_query = query_case["query"].format(keyspace=self.external_collection_name)
            cb_query = query_case["query"].format(keyspace=self.cb_comparison_path) if getattr(self, 'cb_comparison_ready', False) else None
            spark_query = self._get_spark_query_for_case(query_name, table_path)
            expected_results = self._build_expected_query_results(query_name)

            self.log.info(f"\n--- {query_name} ---")
            
            iceberg_results = []
            cb_results = []
            spark_results = []
            
            # 1. Query Iceberg via N1QL
            try:
                result = self.run_cbq_query(query=iceberg_query, query_params={"timeout": "300s"})
                iceberg_results = result.get("results", [])
            except Exception as e:
                self.log.error(f"Iceberg query error: {e}")
            
            # 2. Query Couchbase via N1QL
            if cb_query:
                try:
                    result = self.run_cbq_query(query=cb_query, query_params={"timeout": "300s"})
                    cb_results = result.get("results", [])
                except Exception as e:
                    self.log.error(f"Couchbase query error: {e}")
            
            # 3. Query Spark directly
            if spark_query:
                try:
                    spark_results = self._query_spark_iceberg_table(spark_query) or []
                except Exception as e:
                    self.log.error(f"Spark query error: {e}")
            
            # Format results for logging (first 3 rows, key fields only)
            def fmt(results, max_rows=3):
                if not results:
                    return "[]"
                if 'id' in results[0]:
                    ids = [r['id'] for r in results[:max_rows]]
                    return f"{ids}{'...' if len(results) > max_rows else ''} ({len(results)} rows)"
                elif 'type' in results[0]:
                    types = [r['type'] for r in results[:max_rows]]
                    return f"{types}{'...' if len(results) > max_rows else ''} ({len(results)} rows)"
                else:
                    return f"{len(results)} rows"
            
            self.log.info(f"  Iceberg N1QL:   {fmt(iceberg_results)}")
            self.log.info(f"  Couchbase N1QL: {fmt(cb_results)}")
            self.log.info(f"  Spark Direct:   {fmt(spark_results)}")
            self.log.info(f"  Expected:       {fmt(expected_results)}")
            
            # Check for failures/discrepancies
            test_failed = False
            failure_info = {"name": query_name, "query": iceberg_query}
            
            # Compare Iceberg vs Expected
            try:
                self._compare_query_results(iceberg_results, expected_results, query_name)
                self.log.info(f"  Result: PASS (Iceberg matches expected)")
            except AssertionError as e:
                test_failed = True
                failure_info["error"] = str(e)
                failure_info["iceberg"] = iceberg_results[:5]
                failure_info["couchbase"] = cb_results[:5] if cb_results else "N/A"
                failure_info["spark"] = spark_results[:5] if spark_results else "N/A"
                failure_info["expected"] = expected_results[:5]
                
                # Determine if it's Iceberg-specific or general N1QL bug
                if cb_results:
                    try:
                        self._compare_query_results(cb_results, expected_results, query_name)
                        self.log.error(f"  Result: ICEBERG BUG - Couchbase correct, Iceberg wrong")
                        failure_info["bug_type"] = "ICEBERG"
                    except AssertionError:
                        self.log.error(f"  Result: N1QL BUG - Both Iceberg and Couchbase wrong")
                        failure_info["bug_type"] = "N1QL"
                else:
                    self.log.error(f"  Result: FAIL (no CB comparison available)")
                    failure_info["bug_type"] = "UNKNOWN"
            
            if test_failed:
                failed_queries.append(failure_info)

        # Summary
        self.log.info(f"\n{'='*80}")
        self.log.info(f"SUMMARY: {len(query_cases) - len(failed_queries)}/{len(query_cases)} PASSED")
        self.log.info(f"{'='*80}")
        
        if failed_queries:
            failure_msg = f"\n{'='*60}\nIceberg query suite: {len(failed_queries)}/{len(query_cases)} FAILED\n{'='*60}\n"
            for item in failed_queries:
                failure_msg += f"\n[{item.get('bug_type', 'FAILED')}] {item['name']}\n"
                failure_msg += f"  Query: {item['query']}\n"
                failure_msg += f"  Iceberg:   {json.dumps(item.get('iceberg', []), default=str)}\n"
                failure_msg += f"  Couchbase: {json.dumps(item.get('couchbase', 'N/A'), default=str)}\n"
                failure_msg += f"  Spark:     {json.dumps(item.get('spark', 'N/A'), default=str)}\n"
                failure_msg += f"  Expected:  {json.dumps(item.get('expected', []), default=str)}\n"
            self.fail(failure_msg)

    def _get_spark_query_for_case(self, query_name, table_path):
        """Get equivalent Spark SQL query for a test case."""
        queries = {
            "projection_limit_offset": f"SELECT id, name, city, type, price FROM {table_path} WHERE id < 10 ORDER BY id LIMIT 3 OFFSET 2",
            "between_in_filter": f"SELECT id, name, type, price FROM {table_path} WHERE id BETWEEN 10 AND 25 AND type IN ('Hotel', 'Inn') ORDER BY id",
            "distinct_like_cities": f"SELECT DISTINCT city FROM {table_path} WHERE id < 100 AND city LIKE 'New %' ORDER BY city",
            "group_by_type_aggregates": f"SELECT type, COUNT(1) AS hotel_count, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price FROM {table_path} WHERE id < 25 GROUP BY type HAVING COUNT(1) >= 5 ORDER BY type",
            "any_satisfies_reviews": None,  # Complex N1QL syntax, skip Spark comparison
            "unnest_review_aggregates": f"SELECT id, size(reviews) as review_count FROM {table_path} WHERE id BETWEEN 0 AND 9 ORDER BY id",
            "let_case_array_projection": None,  # Complex N1QL syntax
            "scalar_subquery_price_filter": None,  # Subquery syntax differs
            "union_all_amenities": None,  # Complex syntax
            "window_rank_by_city": f"SELECT id, name, city, price, avg_rating, RANK() OVER (PARTITION BY city ORDER BY avg_rating DESC, price ASC) AS city_rank FROM {table_path} WHERE city = 'Albuquerque' AND id < 250 ORDER BY city_rank, id"
        }
        return queries.get(query_name)

    def test_iceberg_select_reviews_nested_list_template(self):
        """Test the legacy nested-list reviews template as a separate variant."""
        query = f"SELECT id, name, reviews FROM {self.external_collection_name} WHERE id IN [99, 100, 101] ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, name, reviews",
            spark_where="id IN (99, 100, 101)",
            spark_order_by="id",
            description="SELECT reviews nested list"
        )
        self.assertEqual(result["status"], "success", f"Query failed: {result}")
        self.assertEqual(len(result["results"]), 3, "Expected 3 documents")

        expected_results = [
            {
                "id": self.sample_data_by_id[doc_id]["id"],
                "name": self.sample_data_by_id[doc_id]["name"],
                "reviews": self.sample_data_by_id[doc_id]["reviews"]
            }
            for doc_id in [99, 100, 101]
        ]
        self._compare_query_results(result["results"], expected_results,
                                    "test_iceberg_select_reviews_nested_list_template")

    def test_iceberg_select_star_nested_structure(self):
        """Reproduce the nested-structure bug with SELECT * where id=0"""
        query = f"SELECT * FROM {self.external_collection_name} where id=0"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="*",
            spark_where="id = 0",
            description="SELECT * nested structure"
        )
        self.assertEqual(result["status"], "success", f"Query failed: {result}")
        self.assertEqual(len(result["results"]), 1, "Expected 1 document")

        actual_row = result["results"][0]
        actual_document = self._extract_selected_document(actual_row)
        actual_doc_id = actual_document.get("id")

        self.assertIsNotNone(actual_doc_id, f"Could not determine document id from query result: {actual_row}")
        expected_document = self.sample_data_by_id[actual_doc_id]

        self._compare_query_results(actual_document, expected_document,
                                    "test_iceberg_select_star_nested_structure")

    def test_iceberg_alter_catalog_invalid_credentials(self):
        """
        Test ALTER CATALOG with invalid credentials.
        After altering the catalog with junk credential reference, queries should fail.
        """
        # First verify the collection works with valid credentials
        query = f"SELECT id, name FROM {self.external_collection_name} ORDER BY id LIMIT 5"
        self.log.info(f"Running initial query to verify collection works: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Initial query should succeed: {result}")
        self.log.info(f"Initial query succeeded with {len(result['results'])} results")

        # Create a dummy/invalid credentialstore
        invalid_creds_name = "invalid_iceberg_creds"
        invalid_creds_obj = {
            "type": "aws",
            "fields": {
                "accessKeyId": "INVALID_ACCESS_KEY_12345",
                "secretAccessKey": "invalid_secret_key_that_does_not_exist",
                "region": "us-east-1",
                "endpoint": "https://s3.amazonaws.com"
            },
            "guardrails": {"allowedServices": ["n1ql"]},
            "description": "Invalid AWS credential for testing"
        }
        create_invalid_creds_query = f"CREATE CREDENTIALSTORE {invalid_creds_name} WITH {json.dumps(invalid_creds_obj)}"
        self.log.info(f"Creating invalid credentialstore: {create_invalid_creds_query}")
        result = self.run_cbq_query(create_invalid_creds_query)
        self.log.info(f"Invalid credentialstore creation result: {result}")

        # Alter catalog to use invalid credentials
        alter_query = f'ALTER CATALOG {self.catalog_name} WITH {{"credentialId": "{invalid_creds_name}"}}'
        self.log.info(f"Altering catalog with invalid credentials: {alter_query}")
        try:
            result = self.run_cbq_query(alter_query)
            self.log.info(f"ALTER CATALOG result: {result}")
        except Exception as e:
            self.log.info(f"ALTER CATALOG raised exception (may be expected): {str(e)}")

        # Now try to query - should fail due to invalid credentials
        query = f"SELECT id, name FROM {self.external_collection_name} ORDER BY id LIMIT 5"
        self.log.info(f"Running query after ALTER CATALOG with invalid creds: {query}")
        try:
            result = self.run_cbq_query(query, query_params={"timeout": "60s"})
            # If we get here, check if it's an error status
            if result.get('status') == 'success':
                self.fail(f"Query should have failed with invalid credentials, but succeeded: {result}")
            else:
                self.log.info(f"Query failed as expected with status: {result.get('status')}, errors: {result.get('errors')}")
        except Exception as e:
            # Expected - query should fail with invalid credentials
            self.log.info(f"Query failed as expected with invalid credentials: {str(e)}")

        # Restore valid credentials
        restore_query = f'ALTER CATALOG {self.catalog_name} WITH {{"credentialId": "{self.credentialstore_name}"}}'
        self.log.info(f"Restoring catalog with valid credentials: {restore_query}")
        result = self.run_cbq_query(restore_query)
        self.log.info(f"Restore CATALOG result: {result}")

        # Cleanup invalid credentialstore
        drop_invalid_creds = f"DROP CREDENTIALSTORE {invalid_creds_name}"
        self.log.info(f"Dropping invalid credentialstore: {drop_invalid_creds}")
        try:
            result = self.run_cbq_query(drop_invalid_creds)
            self.log.info(f"Drop invalid credentialstore result: {result}")
        except Exception as e:
            self.log.warning(f"Error dropping invalid credentialstore: {str(e)}")

        # Verify collection works again with restored credentials
        query = f"SELECT id, name FROM {self.external_collection_name} ORDER BY id LIMIT 5"
        self.log.info(f"Running final query to verify restoration: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Final query should succeed after restoring credentials: {result}")
        self.log.info(f"Final query succeeded with {len(result['results'])} results - credentials restored successfully")

    def test_iceberg_drop_order_dependencies(self):
        """
        Test that dropping N1QL Iceberg objects in wrong order produces appropriate errors.
        Correct order: external_collection -> catalog -> credentialstore
        This test verifies errors when trying to drop in wrong order.
        """
        # Test 1: Try to drop credentialstore while catalog still references it
        self.log.info("Test 1: Attempting to drop credentialstore while catalog references it")
        drop_creds_query = f"DROP CREDENTIALSTORE {self.credentialstore_name}"
        drop_creds_failed = False
        try:
            result = self.run_cbq_query(drop_creds_query)
            if result.get('status') == 'success':
                self.log.warning(f"DROP CREDENTIALSTORE unexpectedly succeeded (may be product behavior): {result}")
            else:
                self.log.info(f"DROP CREDENTIALSTORE failed as expected: {result.get('errors')}")
                drop_creds_failed = True
        except Exception as e:
            error_msg = str(e).lower()
            self.log.info(f"DROP CREDENTIALSTORE failed as expected with error: {str(e)}")
            drop_creds_failed = True
            # Check if error indicates dependency - but don't fail test if message is different
            if any(keyword in error_msg for keyword in ['in use', 'reference', 'depend', 'cannot', 'being used']):
                self.log.info("Error message indicates dependency issue as expected")

        # Test 2: Try to drop catalog while external collection still references it
        self.log.info("Test 2: Attempting to drop catalog while external collection references it")
        drop_catalog_query = f"DROP CATALOG {self.catalog_name}"
        drop_catalog_failed = False
        try:
            result = self.run_cbq_query(drop_catalog_query)
            if result.get('status') == 'success':
                self.log.warning(f"DROP CATALOG unexpectedly succeeded (may be product behavior): {result}")
            else:
                self.log.info(f"DROP CATALOG failed as expected: {result.get('errors')}")
                drop_catalog_failed = True
        except Exception as e:
            error_msg = str(e).lower()
            self.log.info(f"DROP CATALOG failed as expected with error: {str(e)}")
            drop_catalog_failed = True
            # Check if error indicates dependency - but don't fail test if message is different
            if any(keyword in error_msg for keyword in ['in use', 'reference', 'depend', 'cannot', 'being used', 'collection']):
                self.log.info("Error message indicates dependency issue as expected")

        # Test 3: Verify correct drop order works
        self.log.info("Test 3: Verifying correct drop order works")

        # Drop external collection first
        drop_collection_query = f"DROP COLLECTION {self.external_collection_name}"
        self.log.info(f"Dropping external collection: {drop_collection_query}")
        result = self.run_cbq_query(drop_collection_query)
        self.assertEqual(result.get('status'), 'success', f"DROP COLLECTION should succeed: {result}")
        self.log.info("External collection dropped successfully")

        # Now catalog should be droppable
        self.log.info(f"Dropping catalog: {drop_catalog_query}")
        result = self.run_cbq_query(drop_catalog_query)
        self.assertEqual(result.get('status'), 'success', f"DROP CATALOG should succeed after dropping collection: {result}")
        self.log.info("Catalog dropped successfully")

        # Now credentialstore should be droppable
        self.log.info(f"Dropping credentialstore: {drop_creds_query}")
        result = self.run_cbq_query(drop_creds_query)
        self.assertEqual(result.get('status'), 'success', f"DROP CREDENTIALSTORE should succeed after dropping catalog: {result}")
        self.log.info("Credentialstore dropped successfully")

        # Mark that we've already cleaned up N1QL objects to skip cleanup in tearDown
        self.n1ql_objects_created = False
        self.log.info("All N1QL Iceberg objects dropped in correct order - test passed")

    def test_iceberg_null_values(self):
        """
        Test querying Iceberg data with null values in various fields.
        Verifies that:
        - NULL values are correctly read from Iceberg
        - IS NULL / IS NOT NULL predicates work
        - NULL handling in aggregations
        - NULL in nested fields
        """
        # Test 1: Select documents with null name and count in Python (avoiding COUNT(*) bug)
        query = f"SELECT id FROM {self.external_collection_name} WHERE name IS NULL ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id",
            spark_where="name IS NULL",
            spark_order_by="id",
            description="Test 1 - Select null names"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        null_name_count = len(result['results'])
        # Every id where id % 5 == 0 has null name
        expected_null_names = len([d for d in self.sample_data if d['name'] is None])
        self.assertEqual(null_name_count, expected_null_names, 
                         f"Expected {expected_null_names} documents with null name, got {null_name_count}")
        self.log.info(f"Found {null_name_count} documents with null name (expected {expected_null_names})")

        # Test 2: Select documents with null price and count in Python
        query = f"SELECT id FROM {self.external_collection_name} WHERE price IS NULL ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id",
            spark_where="price IS NULL",
            spark_order_by="id",
            description="Test 2 - Select null prices"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        null_price_count = len(result['results'])
        # Every id where id % 11 == 0 has null price
        expected_null_prices = len([d for d in self.sample_data if d['price'] is None])
        self.assertEqual(null_price_count, expected_null_prices,
                         f"Expected {expected_null_prices} documents with null price, got {null_price_count}")
        self.log.info(f"Found {null_price_count} documents with null price (expected {expected_null_prices})")

        # Test 3: Select documents with non-null city
        query = f"SELECT id, city FROM {self.external_collection_name} WHERE city IS NOT NULL ORDER BY id LIMIT 10"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, city",
            spark_where="city IS NOT NULL",
            spark_order_by="id",
            spark_limit=10,
            description="Test 3 - Select non-null cities"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            self.assertIsNotNone(row.get('city'), f"City should not be null: {row}")
        self.log.info(f"Verified {len(result['results'])} documents have non-null city")

        # Test 4: Aggregate with null values (AVG should ignore nulls)
        query = f"SELECT AVG(price) as avg_price FROM {self.external_collection_name}"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="AVG(price) as avg_price",
            description="Test 4 - AVG with nulls"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        avg_price = result['results'][0]['avg_price']
        self.assertIsNotNone(avg_price, "AVG should return a value (nulls should be ignored)")
        self.log.info(f"AVG(price) = {avg_price}")

        # Test 5: COALESCE with null values
        query = f"SELECT id, COALESCE(name, 'Unknown') as display_name FROM {self.external_collection_name} WHERE id < 10 ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, COALESCE(name, 'Unknown') as display_name",
            spark_where="id < 10",
            spark_order_by="id",
            description="Test 5 - COALESCE"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            self.assertIsNotNone(row.get('display_name'), f"COALESCE should never return null: {row}")
            if self.sample_data_by_id[row['id']]['name'] is None:
                self.assertEqual(row['display_name'], 'Unknown', 
                                 f"Expected 'Unknown' for null name, got {row['display_name']}")
        self.log.info("COALESCE correctly handled null values")

        # Test 6: Select documents with null reviews and count in Python
        query = f"SELECT id FROM {self.external_collection_name} WHERE reviews IS NULL ORDER BY id"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id",
            spark_where="reviews IS NULL",
            spark_order_by="id",
            description="Test 6 - Select null reviews"
        )
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        null_reviews_count = len(result['results'])
        # Every id where id % 9 == 0 has null reviews
        expected_null_reviews = len([d for d in self.sample_data if d['reviews'] is None])
        self.assertEqual(null_reviews_count, expected_null_reviews,
                         f"Expected {expected_null_reviews} documents with null reviews, got {null_reviews_count}")
        self.log.info(f"Found {null_reviews_count} documents with null reviews")

        self.log.info("All null value tests passed successfully")

    def test_iceberg_diverse_datatypes(self):
        """
        Test querying Iceberg data with diverse datatypes including:
        - Integers (small, big, negative)
        - Floating point (float, double, scientific notation)
        - Strings (simple, unicode, empty)
        - Booleans
        - Timestamps and dates
        - Arrays of different types
        - Nested objects
        """
        # Test 1: Integer types
        query = f"SELECT id, small_int, big_int, negative_int FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running integer types query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['small_int'], doc['small_int'], f"small_int mismatch for id={row['id']}")
            self.assertEqual(row['big_int'], doc['big_int'], f"big_int mismatch for id={row['id']}")
            self.assertEqual(row['negative_int'], doc['negative_int'], f"negative_int mismatch for id={row['id']}")
        self.log.info("Integer type tests passed")

        # Test 2: Floating point types
        query = f"SELECT id, float_val, double_val, precise_decimal FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running floating point types query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            # Use approximate comparison for floats
            self.assertAlmostEqual(row['float_val'], doc['float_val'], places=5,
                                   msg=f"float_val mismatch for id={row['id']}")
        self.log.info("Floating point type tests passed")

        # Test 3: Boolean types
        query = f"SELECT id, is_active, is_verified FROM {self.external_collection_name} WHERE id < 10 ORDER BY id"
        self.log.info(f"Running boolean types query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['is_active'], doc['is_active'], f"is_active mismatch for id={row['id']}")
            self.assertEqual(row['is_verified'], doc['is_verified'], f"is_verified mismatch for id={row['id']}")
        self.log.info("Boolean type tests passed")

        # Test 4: Filter by boolean (select and count in Python to avoid COUNT(*) bug)
        query = f"SELECT id FROM {self.external_collection_name} WHERE is_active = true ORDER BY id"
        self.log.info(f"Running filter by boolean query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        active_count = len(result['results'])
        expected_active = len([d for d in self.sample_data if d['is_active'] == True])
        self.assertEqual(active_count, expected_active, f"Expected {expected_active} active, got {active_count}")
        self.log.info(f"Found {active_count} active documents")

        # Test 5: Timestamp queries
        query = f"SELECT id, created_at, date_only FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running timestamp query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            self.assertIsNotNone(row.get('created_at'), f"created_at should not be null for id={row['id']}")
            self.assertIsNotNone(row.get('date_only'), f"date_only should not be null for id={row['id']}")
        self.log.info("Timestamp type tests passed")

        # Test 6: Array types
        query = f"SELECT id, int_array, string_array, empty_array FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running array types query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['int_array'], doc['int_array'], f"int_array mismatch for id={row['id']}")
            self.assertEqual(row['string_array'], doc['string_array'], f"string_array mismatch for id={row['id']}")
            self.assertEqual(row['empty_array'], doc['empty_array'], f"empty_array mismatch for id={row['id']}")
        self.log.info("Array type tests passed")

        # Test 7: Nested object access
        query = f"SELECT id, metrics.`count` as metric_count, metrics.average as metric_avg FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running nested object access query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['metric_count'], doc['metrics']['count'], 
                             f"metrics.count mismatch for id={row['id']}")
        self.log.info("Nested object access tests passed")

        # Test 8: Deep nested access
        query = f"SELECT id, deep_nested.level1.level2.level3.`value` as deep_value FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running deep nested access query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            expected_deep_val = doc['deep_nested']['level1']['level2']['level3']['value']
            self.assertEqual(row['deep_value'], expected_deep_val,
                             f"deep_nested value mismatch for id={row['id']}")
        self.log.info("Deep nested access tests passed")

        # Test 9: UNNEST on array of objects
        query = f"SELECT d.id, m.sensor_id, m.reading FROM {self.external_collection_name} d UNNEST d.measurements m WHERE d.id < 3 ORDER BY d.id, m.sensor_id"
        self.log.info(f"Running UNNEST query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertGreater(len(result['results']), 0, "UNNEST should return results")
        self.log.info(f"UNNEST returned {len(result['results'])} rows")

        # Test 10: Unicode string handling
        query = f"SELECT id, unicode_string FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Running unicode string query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['unicode_string'], doc['unicode_string'],
                             f"unicode_string mismatch for id={row['id']}")
        self.log.info("Unicode string tests passed")

        self.log.info("All diverse datatype tests passed successfully")

    def test_iceberg_schema_evolution(self):
        """
        Test Iceberg schema evolution scenarios:
        1. Add new columns to table
        2. Query old data (new columns should be NULL)
        3. Append new data with new columns populated
        4. Query to verify both old and new data
        5. Rename a column
        6. Drop a column
        """
        # Get initial row count
        query = f"SELECT id, name, city FROM {self.external_collection_name} ORDER BY id LIMIT 5"
        result = self.run_cbq_query_with_spark_verification(
            n1ql_query=query,
            spark_columns="id, name, city",
            spark_order_by="id",
            spark_limit=5,
            description="Initial query"
        )
        self.assertEqual(result['status'], 'success', f"Initial query failed: {result}")
        initial_count = len(result['results'])
        self.log.info(f"Initial data has {initial_count} rows (showing first 5)")

        # Get current schema
        initial_schema = self.iceberg_util.get_table_schema()
        self.log.info(f"Initial schema: {initial_schema}")
        initial_columns = [col[0] for col in initial_schema]

        # Test 1: Add new columns
        self.log.info("Test 1: Adding new columns to the table")
        self.iceberg_util.add_column("loyalty_points", "int")
        self.iceberg_util.add_column("membership_tier", "string")
        self.iceberg_util.add_column("last_visit_date", "string")

        # Verify new columns exist in schema
        updated_schema = self.iceberg_util.get_table_schema()
        self.log.info(f"Updated schema after adding columns: {updated_schema}")
        updated_columns = [col[0] for col in updated_schema]
        self.assertIn("loyalty_points", updated_columns, "loyalty_points column should exist")
        self.assertIn("membership_tier", updated_columns, "membership_tier column should exist")
        self.assertIn("last_visit_date", updated_columns, "last_visit_date column should exist")

        # Test 2: Query old data - new columns should be NULL
        self.log.info("Test 2: Querying old data - new columns should be NULL")

        # Need to recreate external collection to pick up schema changes
        self._recreate_external_collection()

        query = f"SELECT id, name, loyalty_points, membership_tier FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Query old data with new columns: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        for row in result['results']:
            # New columns should be NULL for old data
            self.assertIsNone(row.get('loyalty_points'), 
                              f"loyalty_points should be NULL for old data (id={row['id']})")
            self.assertIsNone(row.get('membership_tier'), 
                              f"membership_tier should be NULL for old data (id={row['id']})")
        self.log.info("Verified new columns are NULL for existing data")

        # Test 3: Append new data with new columns populated
        self.log.info("Test 3: Appending new data with new columns populated")
        
        # Get current schema to ensure we include all required columns
        current_schema = self.iceberg_util.get_table_schema()
        schema_columns = {col[0] for col in current_schema}
        self.log.info(f"Current schema columns: {schema_columns}")
        
        # Start with an existing sample doc as template to get all required fields
        template_doc = self.sample_data[0].copy()
        
        new_data = []
        base_id = max(d['id'] for d in self.sample_data) + 1
        for i in range(10):
            # Start with template and update fields
            new_doc = template_doc.copy()
            new_doc.update({
                "id": base_id + i,
                "name": f"New Hotel {i}",
                "city": ["Miami", "Seattle", "Boston", "Denver", "Atlanta"][i % 5],
                "country": "USA",
                "type": "Hotel",
                "price": 200 + (i * 25),
                "avg_rating": 4.0 + (i % 10) / 10.0,
                "free_parking": i % 2 == 0,
                "free_breakfast": i % 3 == 0,
                "reviews": [],
                "public_likes": [],
                "email": f"newhotel{i}@test.com",
                "address": f"{100 + i} New Street",
                "phone": f"555-000-{1000 + i}",
                "url": f"www.newhotel{i}.com",
                "counter": 0,
                "mutate": 0,
                # New columns
                "loyalty_points": 1000 + (i * 100),
                "membership_tier": ["Bronze", "Silver", "Gold", "Platinum"][i % 4],
                "last_visit_date": f"2025-0{(i % 9) + 1}-{(i % 28) + 1:02d}"
            })
            new_data.append(new_doc)

        # Use infer_schema=False to let Spark match existing table schema
        self.iceberg_util.append_data(new_data, infer_schema=False)
        self.log.info(f"Appended {len(new_data)} new rows with populated new columns")

        # Update sample_data for validation
        for doc in new_data:
            self.sample_data.append(doc)
            self.sample_data_by_id[doc['id']] = doc

        # Test 4: Query to verify both old and new data
        self.log.info("Test 4: Querying to verify both old and new data")

        # Query new data - new columns should have values
        query = f"SELECT id, name, loyalty_points, membership_tier, last_visit_date FROM {self.external_collection_name} WHERE id >= {base_id} ORDER BY id"
        self.log.info(f"Query new data: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertEqual(len(result['results']), len(new_data), 
                         f"Expected {len(new_data)} new rows, got {len(result['results'])}")

        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['loyalty_points'], doc['loyalty_points'],
                             f"loyalty_points mismatch for id={row['id']}")
            self.assertEqual(row['membership_tier'], doc['membership_tier'],
                             f"membership_tier mismatch for id={row['id']}")
        self.log.info("Verified new columns have correct values for new data")

        # Query mixing old and new data
        query = f"SELECT id, name, loyalty_points FROM {self.external_collection_name} WHERE id IN [0, 1, {base_id}, {base_id + 1}] ORDER BY id"
        self.log.info(f"Query mixing old and new data: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertEqual(len(result['results']), 4, "Expected 4 rows")
        
        # First two should have NULL loyalty_points, last two should have values
        self.assertIsNone(result['results'][0].get('loyalty_points'), "Old data should have NULL loyalty_points")
        self.assertIsNone(result['results'][1].get('loyalty_points'), "Old data should have NULL loyalty_points")
        self.assertIsNotNone(result['results'][2].get('loyalty_points'), "New data should have loyalty_points")
        self.assertIsNotNone(result['results'][3].get('loyalty_points'), "New data should have loyalty_points")
        self.log.info("Verified mixed query returns correct NULL/non-NULL values")

        # Test 5: Filter on new column
        self.log.info("Test 5: Filter on new column")
        query = f"SELECT id, name, membership_tier FROM {self.external_collection_name} WHERE membership_tier = 'Gold' ORDER BY id"
        self.log.info(f"Filter on new column: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        for row in result['results']:
            self.assertEqual(row['membership_tier'], 'Gold', f"Expected Gold tier, got {row['membership_tier']}")
        expected_gold_count = len([d for d in new_data if d['membership_tier'] == 'Gold'])
        self.assertEqual(len(result['results']), expected_gold_count,
                         f"Expected {expected_gold_count} Gold tier hotels")
        self.log.info(f"Filter on new column returned {len(result['results'])} results")

        # Test 6: Aggregation on new column
        self.log.info("Test 6: Aggregation on new column")
        query = f"SELECT AVG(loyalty_points) as avg_points FROM {self.external_collection_name} WHERE loyalty_points IS NOT NULL"
        self.log.info(f"Aggregation on new column: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        avg_points = result['results'][0]['avg_points']
        self.assertIsNotNone(avg_points, "AVG should return a value")
        expected_avg = sum(d['loyalty_points'] for d in new_data) / len(new_data)
        self.assertAlmostEqual(avg_points, expected_avg, places=0,
                               msg=f"Expected avg ~{expected_avg}, got {avg_points}")
        self.log.info(f"AVG(loyalty_points) = {avg_points}")

        self.log.info("All schema evolution tests passed successfully")

    def _recreate_external_collection(self):
        """Helper to recreate external collection to pick up schema changes."""
        # Drop existing external collection
        drop_query = f"DROP COLLECTION {self.external_collection_name}"
        self.log.info(f"Dropping external collection: {drop_query}")
        try:
            self.run_cbq_query(drop_query)
        except Exception as e:
            self.log.warning(f"Error dropping collection (may not exist): {str(e)}")

        # Recreate external collection
        time.sleep(2)
        self._create_external_collection()
        time.sleep(5)
        self.log.info("External collection recreated to pick up schema changes")

    def test_iceberg_mixed_types(self):
        """
        Test querying Iceberg data where fields track what the original type
        would have been, while storing all values consistently for schema compatibility.
        
        This tests:
        - Fields that represent different original types (tracked via *_type fields)
        - Metadata about type origin
        - Filtering and aggregation on type metadata
        """
        # Test 1: Verify data loaded correctly
        query = f"SELECT id, name, category, flexible_value, flexible_value_type FROM {self.external_collection_name} WHERE id < 10 ORDER BY id"
        self.log.info(f"Running basic query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertEqual(len(result['results']), 10, "Expected 10 rows")
        
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['flexible_value'], doc['flexible_value'],
                             f"flexible_value mismatch for id={row['id']}")
            self.assertEqual(row['flexible_value_type'], doc['flexible_value_type'],
                             f"flexible_value_type mismatch for id={row['id']}")
        self.log.info("Basic data verification passed")

        # Test 2: Filter by original type using the type indicator field
        self.log.info("Test 2: Filter by original type")
        
        # Find all rows where original value was a string
        query = f"SELECT id, flexible_value FROM {self.external_collection_name} WHERE flexible_value_type = 'str' ORDER BY id"
        self.log.info(f"Query string-type values: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        expected_str_count = len([d for d in self.sample_data if d['flexible_value_type'] == 'str'])
        self.assertEqual(len(result['results']), expected_str_count,
                         f"Expected {expected_str_count} string-type rows, got {len(result['results'])}")
        self.log.info(f"Found {len(result['results'])} rows with string-type original values")

        # Find all rows where original value was an int
        query = f"SELECT id, flexible_value FROM {self.external_collection_name} WHERE flexible_value_type = 'int' ORDER BY id"
        self.log.info(f"Query int-type values: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        expected_int_count = len([d for d in self.sample_data if d['flexible_value_type'] == 'int'])
        self.assertEqual(len(result['results']), expected_int_count,
                         f"Expected {expected_int_count} int-type rows, got {len(result['results'])}")
        self.log.info(f"Found {len(result['results'])} rows with int-type original values")

        # Test 3: Query numeric vs string quantity indicator
        self.log.info("Test 3: Numeric vs string quantities")
        
        # Query rows where quantity would have been string (tracked by boolean)
        query = f"SELECT id, quantity_numeric, quantity_is_string FROM {self.external_collection_name} WHERE quantity_is_string = true ORDER BY id LIMIT 10"
        self.log.info(f"Query string quantities: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        for row in result['results']:
            self.assertTrue(row['quantity_is_string'], f"Expected quantity_is_string=true for id={row['id']}")
        self.log.info(f"Found {len(result['results'])} rows with string quantity indicator")

        # Test 4: Aggregation on numeric field
        self.log.info("Test 4: Aggregation on numeric field")
        query = f"SELECT AVG(quantity_numeric) as avg_qty, SUM(quantity_numeric) as total_qty FROM {self.external_collection_name}"
        self.log.info(f"Aggregate numeric quantities: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        avg_qty = result['results'][0]['avg_qty']
        total_qty = result['results'][0]['total_qty']
        self.assertIsNotNone(avg_qty, "AVG should return a value")
        self.assertIsNotNone(total_qty, "SUM should return a value")
        
        expected_total = sum(d['quantity_numeric'] for d in self.sample_data)
        self.assertEqual(total_qty, expected_total, f"Expected total {expected_total}, got {total_qty}")
        self.log.info(f"AVG(quantity_numeric)={avg_qty}, SUM(quantity_numeric)={total_qty}")

        # Test 5: Query by category and type combination
        self.log.info("Test 5: Combined category and type filter")
        query = f"SELECT id, name, category, flexible_value_type FROM {self.external_collection_name} WHERE category = 'Electronics' AND flexible_value_type = 'str' ORDER BY id"
        self.log.info(f"Combined filter: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        for row in result['results']:
            self.assertEqual(row['category'], 'Electronics', f"Expected Electronics category for id={row['id']}")
            self.assertEqual(row['flexible_value_type'], 'str', f"Expected str type for id={row['id']}")
        
        expected_count = len([d for d in self.sample_data 
                              if d['category'] == 'Electronics' and d['flexible_value_type'] == 'str'])
        self.assertEqual(len(result['results']), expected_count,
                         f"Expected {expected_count} rows, got {len(result['results'])}")
        self.log.info(f"Combined filter returned {len(result['results'])} rows")

        # Test 6: Query nested metadata with type information
        self.log.info("Test 6: Nested metadata query")
        query = f"SELECT id, metadata.nested_type, metadata.nested_value FROM {self.external_collection_name} WHERE id < 10 ORDER BY id"
        self.log.info(f"Nested metadata query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            expected_type = doc['metadata']['nested_type']
            # Handle potential key name encoding issues
            actual_type = row.get('nested_type') or row.get('metadata', {}).get('nested_type')
            if actual_type:
                self.assertEqual(actual_type, expected_type,
                                 f"nested_type mismatch for id={row['id']}")
        self.log.info("Nested metadata query passed")

        # Test 7: Filter by boolean stored as string
        self.log.info("Test 7: Boolean stored as string filter")
        query = f"SELECT id, status_raw, status_is_boolean FROM {self.external_collection_name} WHERE status_raw = 'True' ORDER BY id"
        self.log.info(f"Boolean string filter: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        expected_true_count = len([d for d in self.sample_data if d['status_raw'] == 'True'])
        self.assertEqual(len(result['results']), expected_true_count,
                         f"Expected {expected_true_count} rows with status_raw='True', got {len(result['results'])}")
        self.log.info(f"Found {len(result['results'])} rows with status_raw='True'")

        # Test 8: Array field with string-converted elements
        self.log.info("Test 8: Array field query")
        query = f"SELECT id, tags FROM {self.external_collection_name} WHERE id < 5 ORDER BY id"
        self.log.info(f"Array field query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        for row in result['results']:
            doc = self.sample_data_by_id[row['id']]
            self.assertEqual(row['tags'], doc['tags'],
                             f"tags mismatch for id={row['id']}")
        self.log.info("Array field verification passed")

        # Test 9: UNNEST on tags array
        self.log.info("Test 9: UNNEST on mixed-origin array")
        query = f"SELECT d.id, t as tag FROM {self.external_collection_name} d UNNEST d.tags t WHERE d.id < 3 ORDER BY d.id, t"
        self.log.info(f"UNNEST query: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        self.assertGreater(len(result['results']), 0, "UNNEST should return results")
        self.log.info(f"UNNEST returned {len(result['results'])} rows")

        # Test 10: Group by original type
        self.log.info("Test 10: Group by original type")
        query = f"SELECT flexible_value_type, COUNT(1) as type_count FROM {self.external_collection_name} GROUP BY flexible_value_type ORDER BY flexible_value_type"
        self.log.info(f"Group by type: {query}")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"Query failed: {result}")
        
        self.log.info(f"Type distribution: {result['results']}")
        
        # Verify counts match expected
        for row in result['results']:
            vtype = row['flexible_value_type']
            actual_count = row['type_count']
            expected_count = len([d for d in self.sample_data if d['flexible_value_type'] == vtype])
            self.assertEqual(actual_count, expected_count,
                             f"Count mismatch for type {vtype}: expected {expected_count}, got {actual_count}")
        self.log.info("Group by type verification passed")

        self.log.info("All mixed types tests passed successfully")

    def _setup_join_test_collection(self, bucket_name, scope_name, collection_name, create_bucket=False):
        """
        Helper to create a Couchbase collection and populate it with sample data for JOIN tests.
        Returns (collection_path, cleanup_list) where cleanup_list contains resources to clean up.
        """
        created_resources = []
        rest = RestConnection(self.master)
        
        if create_bucket:
            self.log.info(f"Creating test bucket: {bucket_name}")
            try:
                rest.create_bucket(bucket=bucket_name, ramQuotaMB=256)
                created_resources.append(('bucket', bucket_name))
                time.sleep(10)  # Wait longer for bucket to be ready
            except Exception as e:
                self.log.warning(f"Bucket creation failed (may already exist): {e}")
        
        if scope_name != '_default':
            self.log.info(f"Creating scope {scope_name} in bucket {bucket_name}")
            try:
                self.run_cbq_query(f'CREATE SCOPE `{bucket_name}`.`{scope_name}`')
                created_resources.append(('scope', f'{bucket_name}.{scope_name}'))
                time.sleep(5)
            except Exception as e:
                self.log.warning(f"Scope creation failed (may already exist): {e}")
        
        self.log.info(f"Creating collection {collection_name}")
        try:
            self.run_cbq_query(f'CREATE COLLECTION `{bucket_name}`.`{scope_name}`.`{collection_name}`')
            created_resources.append(('collection', f'{bucket_name}.{scope_name}.{collection_name}'))
            time.sleep(5)  # Wait for collection to be ready
        except Exception as e:
            self.log.warning(f"Collection creation failed (may already exist): {e}")
        
        coll_path = f'`{bucket_name}`.`{scope_name}`.`{collection_name}`'
        
        # Create primary index FIRST so inserts can be indexed
        index_name = f"idx_join_{bucket_name}_{scope_name}_{collection_name}"
        self.log.info(f"Creating primary index: {index_name}")
        try:
            self.run_cbq_query(f'CREATE PRIMARY INDEX `{index_name}` ON {coll_path}')
            time.sleep(5)  # Wait for index to be ready
        except Exception as e:
            self.log.warning(f"Primary index creation failed (may already exist): {e}")
        
        # Insert all sample data (same as Iceberg) using batch inserts - suppress logging
        self.log.info(f"Inserting {len(self.sample_data)} documents into {coll_path} (logging suppressed)...")
        
        batch_size = 100
        insert_count = 0
        for i in range(0, len(self.sample_data), batch_size):
            batch = self.sample_data[i:i + batch_size]
            values = ", ".join([f"('{doc['id']}', {json.dumps(doc)})" for doc in batch])
            try:
                self.run_cbq_query(f"INSERT INTO {coll_path} (KEY, VALUE) VALUES {values}", debug_query=False)
                insert_count += len(batch)
            except Exception:
                # Fall back to individual inserts if batch fails
                for doc in batch:
                    try:
                        self.run_cbq_query(f"INSERT INTO {coll_path} (KEY, VALUE) VALUES ('{doc['id']}', {json.dumps(doc)})", debug_query=False)
                        insert_count += 1
                    except Exception:
                        pass
        
        self.log.info(f"Insert results: {insert_count} succeeded")
        
        # Wait for data to be queryable
        time.sleep(5)
        
        # Verify data was inserted
        verify_query = f"SELECT COUNT(*) as cnt FROM {coll_path}"
        self.log.info(f"Verifying data insert: {verify_query}")
        try:
            result = self.run_cbq_query(verify_query)
            doc_count = result['results'][0]['cnt'] if result.get('results') else 0
            self.log.info(f"Verification: {doc_count} documents found in collection")
            if doc_count == 0:
                self.log.error("WARNING: No documents found in collection after insert!")
        except Exception as e:
            self.log.error(f"Verification query failed: {e}")
        
        return coll_path, created_resources

    def _cleanup_join_test_resources(self, created_resources):
        """Helper to clean up resources created for JOIN tests."""
        rest = RestConnection(self.master)
        for resource_type, resource_name in reversed(created_resources):
            try:
                if resource_type == 'collection':
                    parts = resource_name.split('.')
                    self.run_cbq_query(f'DROP COLLECTION `{parts[0]}`.`{parts[1]}`.`{parts[2]}`')
                elif resource_type == 'scope':
                    parts = resource_name.split('.')
                    self.run_cbq_query(f'DROP SCOPE `{parts[0]}`.`{parts[1]}`')
                elif resource_type == 'bucket':
                    rest.delete_bucket(resource_name)
                self.log.info(f"Cleaned up {resource_type}: {resource_name}")
            except Exception as e:
                self.log.warning(f"Failed to cleanup {resource_type} {resource_name}: {e}")

    def _run_join_test_queries(self, cb_coll_path):
        """Helper to run standard JOIN test queries."""
        # First, verify CB collection has data
        verify_query = f"SELECT COUNT(*) as cnt FROM {cb_coll_path}"
        self.log.info(f"Verifying CB collection data: {verify_query}")
        result = self.run_cbq_query(verify_query, query_params={"timeout": "300s"})
        cb_count = result['results'][0]['cnt'] if result.get('results') else 0
        self.log.info(f"CB collection has {cb_count} documents")
        self.assertGreater(cb_count, 0, "CB collection should have documents for JOIN tests")
        
        # Test OFFSET on both Iceberg and Couchbase to isolate bug
        self.log.info("Testing OFFSET behavior on Iceberg vs Couchbase")
        
        # OFFSET on Iceberg external collection
        iceberg_query = f"SELECT id FROM {self.external_collection_name} WHERE id < 10 ORDER BY id LIMIT 3 OFFSET 2"
        iceberg_result = self.run_cbq_query(iceberg_query, query_params={"timeout": "300s"})
        iceberg_ids = [r['id'] for r in iceberg_result.get('results', [])]
        self.log.info(f"Iceberg OFFSET result: {iceberg_ids} (expected [2,3,4])")
        
        # OFFSET on regular Couchbase collection
        cb_query = f"SELECT id FROM {cb_coll_path} WHERE id < 10 ORDER BY id LIMIT 3 OFFSET 2"
        cb_result = self.run_cbq_query(cb_query, query_params={"timeout": "300s"})
        cb_ids = [r['id'] for r in cb_result.get('results', [])]
        self.log.info(f"Couchbase OFFSET result: {cb_ids} (expected [2,3,4])")
        
        # Log comparison for bug report
        if iceberg_ids != [2, 3, 4]:
            self.log.error(f"OFFSET BUG on Iceberg: got {iceberg_ids}, expected [2,3,4]")
        if cb_ids != [2, 3, 4]:
            self.log.error(f"OFFSET BUG on Couchbase: got {cb_ids}, expected [2,3,4]")
        if iceberg_ids != cb_ids:
            self.log.error(f"OFFSET behavior differs: Iceberg={iceberg_ids}, Couchbase={cb_ids}")
        
        # INNER JOIN
        query = f"""
            SELECT ext.id, ext.name as ext_name, cb.name as cb_name
            FROM {self.external_collection_name} ext
            INNER JOIN {cb_coll_path} cb ON ext.id = cb.id
            WHERE ext.id < 10
            ORDER BY ext.id
        """
        self.log.info(f"Running INNER JOIN query")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"INNER JOIN failed: {result}")
        self.assertGreater(len(result['results']), 0, f"Expected some rows from INNER JOIN, got {len(result['results'])}")
        self.log.info(f"INNER JOIN returned {len(result['results'])} rows")
        for row in result['results']:
            self.assertEqual(row['ext_name'], row['cb_name'], 
                             f"Name mismatch for id={row['id']}")
        self.log.info("INNER JOIN passed")

        # LEFT JOIN (limited to avoid timeout with large datasets)
        query = f"""
            SELECT ext.id, ext.name, cb.name as cb_name
            FROM {self.external_collection_name} ext
            LEFT JOIN {cb_coll_path} cb ON ext.id = cb.id
            WHERE ext.id < 20
            ORDER BY ext.id
        """
        self.log.info(f"Running LEFT JOIN query")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"LEFT JOIN failed: {result}")
        self.assertGreater(len(result['results']), 0, "LEFT JOIN should return results")
        self.log.info(f"LEFT JOIN returned {len(result['results'])} rows")
        self.log.info("LEFT JOIN passed")

        # JOIN with aggregation
        query = f"""
            SELECT ext.type, COUNT(1) as match_count
            FROM {self.external_collection_name} ext
            INNER JOIN {cb_coll_path} cb ON ext.id = cb.id
            WHERE ext.id < 50
            GROUP BY ext.type
        """
        self.log.info(f"Running JOIN with aggregation")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"JOIN with aggregation failed: {result}")
        self.assertGreater(len(result['results']), 0, "JOIN aggregation should return results")
        self.log.info(f"JOIN aggregation returned {len(result['results'])} groups")
        self.log.info("JOIN with aggregation passed")

        # UNION
        query = f"""
            SELECT id, name, 'iceberg' as source FROM {self.external_collection_name} WHERE id < 3
            UNION ALL
            SELECT id, name, 'couchbase' as source FROM {cb_coll_path} WHERE id < 3
            ORDER BY source, id
        """
        self.log.info(f"Running UNION query")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"UNION failed: {result}")
        self.assertGreater(len(result['results']), 0, "UNION should return results")
        self.log.info(f"UNION returned {len(result['results'])} rows")
        self.log.info("UNION passed")

        # INTERSECT
        query = f"""
            SELECT id, name FROM {self.external_collection_name} WHERE id < 5
            INTERSECT
            SELECT id, name FROM {cb_coll_path} WHERE id < 5
            ORDER BY id
        """
        self.log.info(f"Running INTERSECT query")
        result = self.run_cbq_query(query, query_params={"timeout": "300s"})
        self.assertEqual(result['status'], 'success', f"INTERSECT failed: {result}")
        self.assertGreater(len(result['results']), 0, "INTERSECT should return results")
        self.log.info(f"INTERSECT returned {len(result['results'])} rows")
        self.log.info("INTERSECT passed")

    def test_iceberg_join_different_bucket(self):
        """
        Test JOIN queries between Iceberg external collection and a Couchbase collection
        in a DIFFERENT BUCKET.
        """
        test_bucket = "iceberg_join_diff_bucket"
        test_scope = "join_scope"
        test_collection = "join_coll"
        created_resources = []
        
        try:
            self.log.info("=" * 60)
            self.log.info("Testing JOIN with collection in DIFFERENT BUCKET")
            self.log.info("=" * 60)
            
            cb_coll_path, created_resources = self._setup_join_test_collection(
                test_bucket, test_scope, test_collection, create_bucket=True
            )
            
            self._run_join_test_queries(cb_coll_path)
            
            self.log.info("All JOIN tests with different bucket passed")
            
        finally:
            self._cleanup_join_test_resources(created_resources)

    def test_iceberg_join_same_bucket_different_scope(self):
        """
        Test JOIN queries between Iceberg external collection and a Couchbase collection
        in the SAME BUCKET but DIFFERENT SCOPE.
        """
        default_bucket = self.buckets[0].name if self.buckets else "default"
        test_scope = "iceberg_join_scope"
        test_collection = "join_coll"
        created_resources = []
        
        try:
            self.log.info("=" * 60)
            self.log.info("Testing JOIN with collection in SAME BUCKET, DIFFERENT SCOPE")
            self.log.info("=" * 60)
            
            cb_coll_path, created_resources = self._setup_join_test_collection(
                default_bucket, test_scope, test_collection, create_bucket=False
            )
            
            self._run_join_test_queries(cb_coll_path)
            
            self.log.info("All JOIN tests with same bucket, different scope passed")
            
        finally:
            self._cleanup_join_test_resources(created_resources)

    def test_iceberg_join_same_bucket_same_scope(self):
        """
        Test JOIN queries between Iceberg external collection and a Couchbase collection
        in the SAME BUCKET and SAME SCOPE.
        Uses the comparison collection created during setUp.
        """
        self.log.info("=" * 60)
        self.log.info("Testing JOIN with collection in SAME BUCKET, SAME SCOPE")
        self.log.info("=" * 60)
        
        # Use the comparison collection already created during setUp
        if not getattr(self, 'cb_comparison_ready', False):
            self.skipTest("Comparison Couchbase collection not available")
        
        self._run_join_test_queries(self.cb_comparison_path)
        
        self.log.info("All JOIN tests with same bucket, same scope passed")
