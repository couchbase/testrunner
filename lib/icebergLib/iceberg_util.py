# CRITICAL: Fix hostname resolution before importing pyspark
# The JVM calls InetAddress.getLocalHost() which fails on hosts with unresolvable hostnames
# We must set environment variables AND add hostname to /etc/hosts or use Java properties
import os
import re
import socket
import subprocess
import time
import gc
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType

def _get_local_ip():
    """Get local IP address, fallback to 127.0.0.1 if resolution fails."""
    try:
        # Try to get IP by connecting to an external address (doesn't actually connect)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"

def _hostname_in_hosts_file(hostname):
    """Check if hostname already exists in /etc/hosts."""
    try:
        with open("/etc/hosts", "r") as f:
            for line in f:
                # Skip comments and empty lines
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Split line into IP and hostnames
                parts = line.split()
                if len(parts) >= 2:
                    # Check if hostname matches any of the hostnames on this line
                    hostnames_on_line = parts[1:]
                    if hostname in hostnames_on_line:
                        return True
        return False
    except Exception:
        return False

def _ensure_hostname_resolvable():
    """Ensure the local hostname can be resolved. Add to /etc/hosts if needed."""
    hostname = socket.gethostname()
    try:
        socket.gethostbyname(hostname)
        # Hostname resolves, nothing to do
        return
    except socket.gaierror:
        # Hostname doesn't resolve, check if it's already in /etc/hosts
        if _hostname_in_hosts_file(hostname):
            print(f"Hostname {hostname} already in /etc/hosts but not resolving - DNS issue?")
            return
        
        # Try to add it to /etc/hosts
        try:
            local_ip = _get_local_ip()
            hosts_entry = f"{local_ip} {hostname}\n"
            with open("/etc/hosts", "a") as f:
                f.write(hosts_entry)
            print(f"Added {hostname} -> {local_ip} to /etc/hosts")
        except PermissionError:
            print(f"Warning: Cannot write to /etc/hosts. Hostname {hostname} may not resolve.")
        except Exception as e:
            print(f"Warning: Failed to update /etc/hosts: {e}")

# Try to fix hostname resolution at OS level
_ensure_hostname_resolvable()

# Set Spark environment variables
local_ip = _get_local_ip()
os.environ["SPARK_LOCAL_IP"] = local_ip
os.environ["SPARK_LOCAL_HOSTNAME"] = local_ip  # Override hostname with IP

# Pass Java system properties to handle hostname resolution in JVM
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f"--conf spark.driver.host={local_ip} "
    f"--conf spark.driver.bindAddress={local_ip} "
    f"--conf spark.local.ip={local_ip} "
    "--conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 "
    "--driver-java-options \"-Djava.net.preferIPv4Stack=true\" "
    "pyspark-shell"
)

from icebergLib.iceberg_base import IcebergBase
from icebergLib.iceberg_glue import GlueCatalog
from icebergLib.iceberg_s3tables import S3TablesCatalog
from icebergLib.iceberg_biglake_metastore import BigLakeMetastoreCatalog
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from typing import Any, List, Dict


class IcebergUtil:
    """
    Main utility class for Iceberg catalog operations with PySpark.
    Supports AWS Glue, S3 Tables, and BigLake Metastore catalogs.
    """

    def __init__(self, state: IcebergBase):
        self.state = state
        self.spark = None
        self.glue_catalog = GlueCatalog(self.state)
        self.s3_tables_catalog = S3TablesCatalog(self.state)
        self.biglake_metastore_catalog = BigLakeMetastoreCatalog(self.state)

    def _stop_spark_session_safely(self):
        """
        Safely stop any existing Spark session and clean up resources.
        This is more aggressive than just calling stop() to ensure the JVM
        releases all resources before a new session is created.
        """
        import time
        import gc
        
        stopped = False
        
        # First, try to stop our own spark reference
        if self.spark is not None:
            try:
                print("Stopping self.spark session...")
                self.spark.stop()
                self.spark = None
                stopped = True
            except Exception as e:
                print(f"Warning: Error stopping self.spark: {e}")
                self.spark = None
        
        # Then try to stop any active session (might be different from self.spark)
        try:
            existing_session = SparkSession.getActiveSession()
            if existing_session:
                print("Stopping existing active Spark session...")
                existing_session.stop()
                stopped = True
        except Exception as e:
            print(f"Warning: Error getting/stopping active session: {e}")
        
        # Also check SparkSession._instantiatedSession (internal reference)
        try:
            if hasattr(SparkSession, '_instantiatedSession') and SparkSession._instantiatedSession is not None:
                print("Stopping SparkSession._instantiatedSession...")
                SparkSession._instantiatedSession.stop()
                SparkSession._instantiatedSession = None
                stopped = True
        except Exception as e:
            print(f"Warning: Error stopping _instantiatedSession: {e}")
        
        # Clear the Py4J gateway client to force recreation on next session
        try:
            from py4j.java_gateway import JavaGateway
            from pyspark import SparkContext
            if SparkContext._gateway is not None:
                print("Clearing SparkContext._gateway...")
                try:
                    SparkContext._gateway.shutdown()
                except Exception as e:
                    print(f"Warning: Error shutting down SparkContext._gateway: {e}")
                SparkContext._gateway = None
            if SparkContext._jvm is not None:
                SparkContext._jvm = None
            # Clear active context reference
            if SparkContext._active_spark_context is not None:
                try:
                    SparkContext._active_spark_context.stop()
                except Exception as e:
                    print(f"Warning: Error stopping SparkContext._active_spark_context: {e}")
                SparkContext._active_spark_context = None
        except Exception as e:
            print(f"Warning: Error clearing Py4J gateway: {e}")
        
        # Force garbage collection to help clean up JVM references
        gc.collect()
        
        # If we stopped anything, wait for JVM to fully release resources
        if stopped:
            print("Waiting for Spark/JVM resources to be released...")
            time.sleep(5)  # Increased wait time
            gc.collect()
        
        # Kill any lingering Java processes from this Spark session
        self._kill_orphan_spark_processes()

    def _kill_orphan_spark_processes(self):
        """
        Kill any orphaned Java processes from previous Spark sessions.
        This is a last resort to ensure clean state for new sessions.
        """
        import subprocess
        import os
        
        try:
            # Find Java/Spark processes (PySpark spawns a 'java' process for the JVM)
            result = subprocess.run(
                ["pgrep", "-f", "SparkSubmit|spark-submit|pyspark|org.apache.spark|py4j"],
                capture_output=True,
                text=True
            )
            pids = result.stdout.strip().split('\n')
            current_pid = os.getpid()
            
            for pid in pids:
                if pid and pid.strip():
                    try:
                        pid_int = int(pid.strip())
                        # Don't kill ourselves or parent process
                        if pid_int != current_pid and pid_int != os.getppid():
                            print(f"Killing orphan Spark process: {pid_int}")
                            os.kill(pid_int, 9)  # SIGKILL
                    except (ValueError, ProcessLookupError, PermissionError) as e:
                        print(f"Warning: Could not kill process {pid}: {e}")
        except FileNotFoundError:
            # pgrep not available (Windows?)
            pass
        except Exception as e:
            print(f"Warning: Error killing orphan processes: {e}")

    def create_iceberg_table(self, catalog_type, data, partitioned_field=[], schema=None, filter_by=None, infer_schema=True, write_format="parquet"):
        """Create an Iceberg table with provided data."""
        if infer_schema and schema is None and isinstance(data, list) and data and isinstance(data[0], dict):
            schema = self.infer_schema_from_list(data)

        df = self.spark.createDataFrame(data, schema=schema)

        # BigLake does not support field names with special characters — sanitize them
        if catalog_type == "BIGLAKE_METASTORE":
            def sanitize_col_name(name, seen):
                base = re.sub(r'[^a-zA-Z0-9_]', '_', name)
                candidate = base
                i = 1
                while candidate in seen:
                    candidate = f"{base}_{i}"
                    i += 1
                seen.add(candidate)
                return candidate
            seen = set()
            for col_name in df.columns:
                safe_name = sanitize_col_name(col_name, seen)
                if safe_name != col_name:
                    df = df.withColumnRenamed(col_name, safe_name)
            # Also sanitize nested struct fields via schema casting
            def sanitize_schema(schema):
                seen_fields = set()
                fields = []
                for field in schema.fields:
                    safe_name = sanitize_col_name(field.name, seen_fields)
                    if isinstance(field.dataType, StructType):
                        fields.append(StructField(safe_name, sanitize_schema(field.dataType), field.nullable))
                    elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                        fields.append(StructField(safe_name, ArrayType(sanitize_schema(field.dataType.elementType), field.dataType.containsNull), field.nullable))
                    else:
                        fields.append(StructField(safe_name, field.dataType, field.nullable))
                return StructType(fields)
            safe_schema = sanitize_schema(df.schema)
            df = df.sparkSession.createDataFrame(df.rdd, safe_schema)

        df.printSchema()

        if filter_by:
            print(f"Filter by: {filter_by}")
            df = df.filter(filter_by)

        table_path = f"{catalog_type}.{self.state.database_name}.{self.state.table_name}"
        if catalog_type == "BIGLAKE_METASTORE":
            writer = df.writeTo(table_path).using("iceberg").tableProperty("write-format", write_format)
        else:
            writer = df.writeTo(table_path).using("iceberg").tableProperty(
                "format-version", "2").tableProperty("write-format", write_format)

        if partitioned_field:
            print(f"Partitioning by field: {partitioned_field}")
            writer = writer.partitionedBy(*partitioned_field)

        if catalog_type == "BIGLAKE_METASTORE":
            # BigLake REST catalog does not support createOrReplace — drop and recreate
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {table_path}")
            except Exception as e:
                print(f"Warning: Failed to drop table {table_path} before recreate (may not exist yet): {e}")
            writer.create()
        elif catalog_type in ["NESSIE_REST", "NESSIE"]:
            # Nessie requires the namespace to exist before createOrReplace
            try:
                self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_type}.{self.state.database_name}")
            except Exception as e:
                print(f"Warning: Failed to create namespace {catalog_type}.{self.state.database_name}: {e}")
            writer.createOrReplace()
        else:
            writer.createOrReplace()
        print(f"Data successfully written to Iceberg Table via {catalog_type}!")

    def infer_spark_type(self, value: Any) -> DataType:
        """Infer Spark SQL type from a Python value."""
        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, list):
            if not value:
                return ArrayType(StringType())
            if all(isinstance(v, dict) for v in value):
                return ArrayType(self.infer_spark_schema(value[0]))
            else:
                return ArrayType(self.infer_spark_type(value[0]))
        elif isinstance(value, dict):
            return self.infer_spark_schema(value)
        else:
            return StringType()

    def infer_spark_schema(self, data: Dict[str, Any]) -> StructType:
        """Recursively infer Spark StructType schema from a Python dict."""
        fields = []
        for key, value in data.items():
            spark_type = self.infer_spark_type(value)
            fields.append(StructField(key, spark_type, True))
        return StructType(fields)

    def infer_schema_from_list(self, data_list: List[Dict[str, Any]]) -> StructType:
        """Infer Spark schema from a list of Python dictionaries."""
        if not data_list:
            return StructType([])
        return self.infer_spark_schema(data_list[0])

    def _clear_py4j_state(self):
        """Clear all Py4J/Spark global state to force fresh connection."""
        from pyspark import SparkContext
        
        # Clear SparkContext gateway and JVM references
        try:
            if hasattr(SparkContext, '_gateway') and SparkContext._gateway is not None:
                try:
                    SparkContext._gateway.shutdown()
                except Exception as e:
                    print(f"Warning: Error shutting down SparkContext._gateway: {e}")
                SparkContext._gateway = None

            if hasattr(SparkContext, '_jvm'):
                SparkContext._jvm = None

            if hasattr(SparkContext, '_active_spark_context') and SparkContext._active_spark_context is not None:
                try:
                    SparkContext._active_spark_context.stop()
                except Exception as e:
                    print(f"Warning: Error stopping SparkContext._active_spark_context: {e}")
                SparkContext._active_spark_context = None
            
            # Clear SparkSession references
            if hasattr(SparkSession, '_instantiatedSession'):
                SparkSession._instantiatedSession = None
            
            # Force garbage collection
            gc.collect()
            
        except Exception as e:
            print(f"Warning: Error clearing Py4J state: {e}")

    def create_spark_session(self, catalog_type=None):
        """Create a Spark session configured for the specified catalog type."""
        # Aggressively stop any existing Spark session to ensure fresh configuration
        self._stop_spark_session_safely()

        # Wait until no active session exists (getOrCreate would return old session otherwise)
        max_wait = 30
        start = time.time()
        while True:
            try:
                active = SparkSession.getActiveSession()
                if active is None:
                    break
                print("Waiting for old Spark session to fully terminate...")
                active.stop()
            except Exception:
                break
            if time.time() - start > max_wait:
                raise RuntimeError("Spark session still active after timeout; aborting to avoid stale config")
            time.sleep(2)
            gc.collect()
        
        # Retry logic for session creation (in case JVM needs time to fully terminate)
        max_retries = 3
        retry_delay = 5
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return self._create_spark_session_internal(catalog_type)
            except ConnectionRefusedError as e:
                last_error = e
                print(f"ConnectionRefusedError on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    print(f"Cleaning up Py4J state and retrying in {retry_delay} seconds...")
                    # Clear all Py4J state before retry
                    self._clear_py4j_state()
                    self._kill_orphan_spark_processes()
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
            except Exception as e:
                # For Py4J errors, also retry
                if "Py4J" in str(type(e).__name__) or "py4j" in str(e).lower():
                    last_error = e
                    print(f"Py4J error on attempt {attempt + 1}/{max_retries}: {e}")
                    if attempt < max_retries - 1:
                        print(f"Cleaning up and retrying in {retry_delay} seconds...")
                        self._clear_py4j_state()
                        self._kill_orphan_spark_processes()
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    continue
                # For other errors, don't retry
                raise
        
        raise last_error or Exception("Failed to create Spark session after retries")
    
    def _create_spark_session_internal(self, catalog_type=None):
        """Internal method to actually create the Spark session."""
        # Force clear any cached session so getOrCreate() always creates a fresh one
        try:
            if SparkSession._instantiatedSession is not None:
                SparkSession._instantiatedSession.stop()
                SparkSession._instantiatedSession = None
        except Exception as e:
            print(f"Warning: Could not clear _instantiatedSession: {e}")
        try:
            from pyspark import SparkContext
            if SparkContext._active_spark_context is not None:
                try:
                    SparkContext._active_spark_context.stop()
                except Exception as e:
                    print(f"Warning: Error stopping SparkContext._active_spark_context: {e}")
                SparkContext._active_spark_context = None
        except Exception as e:
            print(f"Warning: Error clearing SparkContext references: {e}")

        builder = (
            SparkSession.builder
            .appName(f"IcebergTest-{catalog_type}")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.local.ip", "127.0.0.1")
            .config("spark.driver.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.files.maxPartitionBytes", "134217728")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.task.maxFailures", "1")
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4,"
                "software.amazon.awssdk:bundle:2.29.26,"
                "software.amazon.awssdk:url-connection-client:2.29.26,"
                "org.apache.iceberg:iceberg-gcp-bundle:1.6.1",
            )
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog_type}", "org.apache.iceberg.spark.SparkCatalog")
        )

        if catalog_type == "AWS_GLUE" or catalog_type == "AWS_GLUE_REST":
            print(f"Configuring Spark session for {catalog_type}...")
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.s3_warehouse_path)
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config(f"spark.sql.catalog.{catalog_type}.client.region", self.state.iceberg_region)
            )
        elif catalog_type == "S3_TABLES":
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.s3_table_bucket_arn)
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config(f"spark.sql.catalog.{catalog_type}.client.region", self.state.iceberg_region)
            )
        elif catalog_type == "BIGLAKE_METASTORE":
            # Ensure GOOGLE_APPLICATION_CREDENTIALS is set so google.auth.default() works
            if self.state.gcs_credentials and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(self.state.gcs_credentials)
            gcp_token = self.state.gcp_access_token()
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.type", "rest")
                .config(f"spark.sql.catalog.{catalog_type}.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.gcs_bucket_path)
                .config(f"spark.sql.catalog.{catalog_type}.header.x-goog-user-project", self.state.gcs_project_id)
                .config(f"spark.sql.catalog.{catalog_type}.header.Authorization", f"Bearer {gcp_token}")
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                .config(f"spark.sql.catalog.{catalog_type}.rest-metrics-reporting-enabled", "false")
                .config("spark.sql.defaultCatalog", catalog_type)
            )
        elif catalog_type in ["NESSIE_REST", "NESSIE"]:
            # Both NESSIE and NESSIE_REST use the native Nessie API (/api/v2) for Spark
            nessie_api_uri = f"http://{self.state.nessie_server}:19120/api/v2"
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.type", "nessie")
                .config(f"spark.sql.catalog.{catalog_type}.uri", nessie_api_uri)
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.s3_warehouse_path)
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config(f"spark.sql.catalog.{catalog_type}.s3.region", self.state.iceberg_region)
            )
        else:
            raise ValueError(
                f"Unsupported catalog_type: {catalog_type}. Use 'AWS_GLUE', 'AWS_GLUE_REST', 'S3_TABLES', 'BIGLAKE_METASTORE', 'NESSIE_REST' or 'NESSIE'.")

        self.spark = builder.getOrCreate()
        return self.spark

    def setup_glue_catalog(self, data, partitioned_field=[], filter_by=None, infer_schema=True):
        """Setup AWS Glue catalog with S3 bucket, database, and load data."""
        self.glue_catalog.create_s3_bucket()
        self.create_spark_session(catalog_type=self.state.catalog_type)
        self.glue_catalog.create_glue_database()
        self.glue_catalog.delete_glue_table()
        self.create_iceberg_table(
            catalog_type=self.state.catalog_type,
            data=data, partitioned_field=partitioned_field, filter_by=filter_by, infer_schema=infer_schema)

        df = self.spark.table(
            f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}")
        df.printSchema()
        print(f"Number of rows read: {df.count()}")
        df.show(5)

    def destroy_glue_catalog(self):
        """Destroy Glue catalog resources."""
        self.glue_catalog.delete_s3_bucket()
        self.glue_catalog.delete_glue_table()
        self.glue_catalog.delete_glue_database()

    def setup_s3_tables_catalog(self, data, partitioned_field=[], filter_by=None, infer_schema=True):
        """Setup S3 Tables catalog and load data."""
        self.s3_tables_catalog.create_s3_table_bucket()
        self.create_spark_session(catalog_type="S3_TABLES")
        self.create_iceberg_table(
            catalog_type=self.state.catalog_type,
            data=data, partitioned_field=partitioned_field, filter_by=filter_by, infer_schema=infer_schema)

        df = self.spark.table(
            f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}")
        df.printSchema()
        print(f"Number of rows read: {df.count()}")
        df.show(5)

    def destroy_s3_tables_catalog(self):
        """Destroy S3 Tables catalog resources."""
        self.s3_tables_catalog.delete_s3_table()

    def setup_biglake_metastore_catalog(self, data, partitioned_field=[], schema=None, filter_by=None):
        """Setup BigLake Metastore catalog and load data."""
        self.biglake_metastore_catalog.create_gcs_bucket()
        self.biglake_metastore_catalog.create_biglake_metastore_catalog()
        self.create_spark_session(catalog_type="BIGLAKE_METASTORE")

        self.spark.sql("SHOW CATALOGS;").show(truncate=False)
        self.spark.sql(f"USE {self.state.catalog_type};")

        self.spark.sql(f"DROP TABLE IF EXISTS {self.state.database_name}.{self.state.table_name};")
        self.spark.sql(f"DROP NAMESPACE IF EXISTS {self.state.database_name} CASCADE;")

        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)

        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.state.database_name};")
        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)

        self.create_iceberg_table(
            catalog_type=self.state.catalog_type,
            data=data, partitioned_field=partitioned_field, schema=schema, filter_by=filter_by)

        self.spark.sql(f"SHOW TABLES IN {self.state.database_name};").show(truncate=False)
        self.spark.sql(f"DESCRIBE {self.state.database_name}.{self.state.table_name}").show(truncate=False)
        self.spark.sql(
            f"SELECT COUNT(*) FROM {self.state.database_name}.{self.state.table_name}").show(truncate=False)

    def destroy_biglake_metastore_catalog(self):
        """Destroy BigLake Metastore catalog resources."""
        self.create_spark_session(catalog_type="BIGLAKE_METASTORE")
        self.spark.sql("SHOW CATALOGS;").show(truncate=False)
        self.spark.sql(f"USE {self.state.catalog_type};")

        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)
        self.spark.sql(f"SHOW TABLES IN {self.state.database_name};").show(truncate=False)

        self.spark.sql(f"DROP TABLE IF EXISTS {self.state.database_name}.{self.state.table_name};")
        self.spark.sql(f"DROP NAMESPACE IF EXISTS {self.state.database_name} CASCADE;")
        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)

        self.biglake_metastore_catalog.delete_biglake_metastore_catalog()
        self.biglake_metastore_catalog.delete_gcs_bucket()

    def add_column(self, column_name, column_type="string"):
        """
        Add a new column to the Iceberg table.
        
        Args:
            column_name: Name of the new column
            column_type: Spark SQL type (string, int, long, double, boolean, etc.)
        """
        table_path = f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}"
        alter_sql = f"ALTER TABLE {table_path} ADD COLUMN {column_name} {column_type}"
        print(f"Adding column: {alter_sql}")
        self.spark.sql(alter_sql)
        print(f"Column {column_name} ({column_type}) added successfully")

    def drop_column(self, column_name):
        """
        Drop a column from the Iceberg table.
        
        Args:
            column_name: Name of the column to drop
        """
        table_path = f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}"
        alter_sql = f"ALTER TABLE {table_path} DROP COLUMN {column_name}"
        print(f"Dropping column: {alter_sql}")
        self.spark.sql(alter_sql)
        print(f"Column {column_name} dropped successfully")

    def rename_column(self, old_name, new_name):
        """
        Rename a column in the Iceberg table.
        
        Args:
            old_name: Current column name
            new_name: New column name
        """
        table_path = f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}"
        alter_sql = f"ALTER TABLE {table_path} RENAME COLUMN {old_name} TO {new_name}"
        print(f"Renaming column: {alter_sql}")
        self.spark.sql(alter_sql)
        print(f"Column renamed from {old_name} to {new_name}")

    def append_data(self, data, infer_schema=True):
        """
        Append new data to the existing Iceberg table.
        
        Args:
            data: List of dictionaries to append
            infer_schema: Whether to infer schema from data
        """
        from pyspark.sql.types import ArrayType, StringType, StructType
        schema = None
        if infer_schema and isinstance(data, list) and data and isinstance(data[0], dict):
            schema = self.infer_schema_from_list(data)

        table_path = f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}"

        # When infer_schema is disabled, use existing table schema to avoid
        # inference failures for empty arrays/null-heavy payloads.
        if schema is None and not infer_schema:
            try:
                schema = self.spark.table(table_path).schema
            except Exception:
                schema = None

        # If inferred schema has empty-list fields (ArrayType(StringType)), patch from table schema
        if schema is not None:
            try:
                table_schema = self.spark.table(table_path).schema
                table_field_map = {f.name.lower(): f for f in table_schema.fields}
                fixed = []
                for field in schema.fields:
                    tfield = table_field_map.get(field.name.lower())
                    if (tfield and isinstance(field.dataType, ArrayType)
                            and isinstance(field.dataType.elementType, StringType)
                            and not isinstance(tfield.dataType.elementType, StringType)):
                        fixed.append(tfield)
                    else:
                        fixed.append(field)
                schema = StructType(fixed)
            except Exception:
                pass

        df = self.spark.createDataFrame(data, schema=schema)
        df.writeTo(table_path).append()
        print(f"Appended {len(data)} rows to {table_path}")

    def get_table_schema(self):
        """
        Get the current schema of the Iceberg table.
        
        Returns:
            List of (column_name, column_type) tuples
        """
        table_path = f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}"
        df = self.spark.table(table_path)
        return [(field.name, str(field.dataType)) for field in df.schema.fields]
