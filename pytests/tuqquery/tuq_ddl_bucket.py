from tuqquery.tuq import QueryTests
from lib.membase.api.exception import CBQError


class QueryBucketDDLTests(QueryTests):
    """
    Test class for bucket DDL operations.
    Tests CREATE BUCKET/DATABASE statements with various configurations.
    """

    def setUp(self):
        super(QueryBucketDDLTests, self).setUp()
        self.log.info("==============  QueryBucketDDLTests setup has started ==============")
        # No data loading needed as specified
        self.log.info("==============  QueryBucketDDLTests setup has completed ==============")

    def suite_setUp(self):
        super(QueryBucketDDLTests, self).suite_setUp()
        self.log.info("==============  QueryBucketDDLTests suite_setup has started ==============")
        self.log.info("==============  QueryBucketDDLTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryBucketDDLTests tearDown has started ==============")
        super(QueryBucketDDLTests, self).tearDown()
        self.log.info("==============  QueryBucketDDLTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryBucketDDLTests suite_tearDown has started ==============")
        super(QueryBucketDDLTests, self).suite_tearDown()
        self.log.info("==============  QueryBucketDDLTests suite_tearDown has completed ==============")

    def _get_kv_nodes_count(self):
        """Helper to get the number of kv nodes in the cluster."""
        # If self.input.servers is available, use it, else fallback to rest API
        try:
            if hasattr(self, "input") and hasattr(self.input, "servers"):
                # Only count kv nodes
                return len([s for s in self.input.servers if hasattr(s, "services") and ("kv" in getattr(s, "services", ["kv"]))])
        except Exception:
            pass
        # Fallback: try to get from rest API
        try:
            nodes = self.rest.get_nodes()
            return len([n for n in nodes if "kv" in n.get("services", [])])
        except Exception:
            # Default to 1 if cannot determine
            return 1

    def _expected_ram_quota(self, mb):
        """Return expected ramQuota in bytes, accounting for number of kv nodes."""
        kv_nodes = self._get_kv_nodes_count()
        return mb * 1024 * 1024 * kv_nodes

    def _assert_ram_quota(self, bucket_info, expected_mb, msg):
        expected = self._expected_ram_quota(expected_mb)
        actual = bucket_info['quota']['ram']
        self.assertEqual(actual, expected, f"{msg} (expected {expected}, got {actual})")

    def _cleanup_test_buckets(self):
        """Clean up test buckets created during tests"""
        test_buckets = [ "default",
            "test_bucket", "test_bucket2", "test_bucket3", "test_bucket4",
            "test_bucket5", "test_bucket6", "test_bucket7", "test_bucket8",
            "test_bucket9", "test_bucket10", "test_bucket11", "test_bucket12",
            "test_bucket13", "test_bucket14", "test_bucket15", "test_bucket16",
            "test_bucket17", "test_bucket18", "test_bucket19", "test_bucket20",
            "test_bucket21", "test_bucket22", "test_bucket23", "test_bucket24",
            "test_bucket25", "test_bucket26", "test_bucket27", "test_bucket28",
            "test_bucket29", "test_bucket30", "test_bucket31", "test_bucket32",
            "test_bucket33", "test_bucket34", "test_bucket35", "test_bucket36",
            "test_bucket37", "test_bucket38", "test_bucket39", "test_bucket40",
            "test_bucket41", "test_bucket42", "test_bucket43", "test_bucket44",
            "test_bucket45", "test_bucket46", "test_bucket47", "test_bucket48",
            "test_bucket49", "test_bucket50", "test_bucket51", "test_bucket52",
            "test_bucket53", "test_bucket54", "test_bucket55", "test_bucket56",
            "test_bucket57", "test_bucket58", "test_bucket59", "test_bucket60",
            "test_bucket61", "test_bucket62", "test_bucket63", "test_bucket64",
            "test_bucket65", "test_bucket66", "test_bucket67", "test_bucket68",
            "test_bucket69", "test_bucket70", "test_bucket71", "test_bucket72",
            "test_bucket73", "test_bucket74", "test_bucket75", "test_bucket76",
            "test_bucket77", "test_bucket78", "test_bucket79", "test_bucket80",
            "test_bucket81", "test_bucket82", "test_bucket83", "test_bucket84",
            "test_bucket85", "test_bucket86", "test_bucket87", "test_bucket88",
            "test_bucket89", "test_bucket90", "test_bucket91", "test_bucket92",
            "test_bucket93", "test_bucket94", "test_bucket95", "test_bucket96",
            "test_bucket97", "test_bucket98", "test_bucket99", "test_bucket100",
            "test_db", "test_db2", "test_db3", "test_db4", "test_db5",
            "test_db6", "test_db7", "test_db8", "test_db9", "test_db10"
        ]

        for bucket_name in test_buckets:
            try:
                self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")
            except Exception as e:
                self.log.info(f"Error dropping bucket {bucket_name}: {e}")

    def test_create_bucket_basic(self):
        """Test basic CREATE BUCKET without any options"""
        try:
            # Test CREATE BUCKET with default settings
            self.run_cbq_query("CREATE BUCKET test_bucket WITH {'ramQuota': 256}")

            # Verify bucket was created
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket'")
            self.assertEqual(len(result['results']), 1, "Bucket should be created")

            # Verify default ramQuota is 100 (as per documentation)
            bucket_info = self.rest.get_bucket_json("test_bucket")
            self._assert_ram_quota(bucket_info, 256, "Default ramQuota should be 100MB")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")
            self.assertEqual(bucket_info['numVBuckets'], 128, "numVBuckets should be 1024")
            self.assertEqual(bucket_info['replicaNumber'], 1, "replicaNumber should be 1")
            self.assertEqual(bucket_info['evictionPolicy'], 'valueOnly', "evictionPolicy should be valueOnly")
            self.assertEqual(bucket_info['compressionMode'], 'passive', "compressionMode should be passive")
            self.assertEqual(bucket_info['maxTTL'], 0, "maxTTL should be 0")
            self.assertEqual(bucket_info['conflictResolutionType'], 'seqno', "conflictResolutionType should be seqno")
            self.assertEqual(bucket_info['threadsNumber'], 3, "threadsNumber should be 3")
            self.assertNotIn('flush', bucket_info['controllers'], "flush field should not be defined")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket")

    def test_create_database_basic(self):
        """Test basic CREATE DATABASE without any options"""
        try:
            # Test CREATE DATABASE with default settings
            self.run_cbq_query("CREATE DATABASE test_db WITH {'ramQuota': 256}")

            # Verify database was created
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_db'")
            self.assertEqual(len(result['results']), 1, "Database should be created")

            # Verify default ramQuota is 100 (as per documentation)
            bucket_info = self.rest.get_bucket_json("test_db")
            self._assert_ram_quota(bucket_info, 256, "Default ramQuota should be 100MB")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")
            self.assertEqual(bucket_info['numVBuckets'], 128, "numVBuckets should be 1024")
            self.assertEqual(bucket_info['replicaNumber'], 1, "replicaNumber should be 1")
            self.assertEqual(bucket_info['evictionPolicy'], 'valueOnly', "evictionPolicy should be valueOnly")
            self.assertEqual(bucket_info['compressionMode'], 'passive', "compressionMode should be passive")
            self.assertEqual(bucket_info['maxTTL'], 0, "maxTTL should be 0")
            self.assertEqual(bucket_info['conflictResolutionType'], 'seqno', "conflictResolutionType should be seqno")
            self.assertEqual(bucket_info['threadsNumber'], 3, "threadsNumber should be 3")
            self.assertNotIn('flush', bucket_info['controllers'], "flush field should not be defined")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_db")

    def test_create_bucket_with_ramquota(self):
        """Test CREATE BUCKET with custom ramQuota"""
        try:
            # Test with custom ramQuota
            self.run_cbq_query("CREATE BUCKET test_bucket2 WITH {'ramQuota': 256}")

            # Verify bucket was created with correct ramQuota
            bucket_info = self.rest.get_bucket_json("test_bucket2")
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be 256MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket2")

    def test_create_bucket_with_storage_backend_magma(self):
        """Test CREATE BUCKET with storageBackend magma"""
        try:
            # Test with storageBackend magma
            self.run_cbq_query("CREATE BUCKET test_bucket3 WITH {'storageBackend': 'magma'}")

            # Verify bucket was created with magma storage
            bucket_info = self.rest.get_bucket_json("test_bucket3")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")

            # Verify default ramQuota is 1024 for magma (as per documentation)
            self._assert_ram_quota(bucket_info, 1024, "Default ramQuota should be 1024MB for magma")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket3")

    def test_create_bucket_with_storage_backend_couchstore(self):
        """Test CREATE BUCKET with storageBackend couchstore"""
        try:
            # Test with storageBackend couchstore
            self.run_cbq_query("CREATE BUCKET test_bucket4 WITH {'storageBackend': 'couchstore'}")

            # Verify bucket was created with couchstore storage
            bucket_info = self.rest.get_bucket_json("test_bucket4")
            self.assertEqual(bucket_info['storageBackend'], 'couchstore', "storageBackend should be couchstore")

            # Verify default ramQuota is 100 for couchstore
            self._assert_ram_quota(bucket_info, 100, "Default ramQuota should be 100MB for couchstore")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket4")

    def test_create_bucket_with_multiple_options(self):
        """Test CREATE BUCKET with multiple configuration options"""
        try:
            # Test with multiple options
            self.run_cbq_query("CREATE BUCKET test_bucket5 WITH {'ramQuota': 512, 'storageBackend': 'magma', 'replicaNumber': 1}")

            # Verify bucket was created with correct options
            bucket_info = self.rest.get_bucket_json("test_bucket5")
            self._assert_ram_quota(bucket_info, 512, "ramQuota should be 512MB")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")
            self.assertEqual(bucket_info['replicaNumber'], 1, "replicaNumber should be 1")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket5")

    def test_create_bucket_with_replica_number(self):
        """Test CREATE BUCKET with replicaNumber option"""
        try:
            # Test with replicaNumber
            self.run_cbq_query("CREATE BUCKET test_bucket6 WITH {'storageBackend': 'couchstore', 'replicaNumber': 2}")

            # Verify bucket was created with correct replica number
            bucket_info = self.rest.get_bucket_json("test_bucket6")
            self.assertEqual(bucket_info['replicaNumber'], 2, "replicaNumber should be 2")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket6")

    def test_create_bucket_with_eviction_policy(self):
        """Test CREATE BUCKET with evictionPolicy option"""
        try:
            # Test with evictionPolicy
            self.run_cbq_query("CREATE BUCKET test_bucket7 WITH {'storageBackend': 'couchstore', 'evictionPolicy': 'fullEviction'}")

            # Verify bucket was created with correct eviction policy
            bucket_info = self.rest.get_bucket_json("test_bucket7")
            self.assertEqual(bucket_info['evictionPolicy'], 'fullEviction', "evictionPolicy should be fullEviction")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket7")

    def test_create_bucket_with_flush_enabled(self):
        """Test CREATE BUCKET with flushEnabled option"""
        try:
            # Test with flushEnabled
            self.run_cbq_query("CREATE BUCKET test_bucket8 WITH {'storageBackend': 'couchstore', 'flushEnabled': 1}")

            # Verify bucket was created with flush enabled
            bucket_info = self.rest.get_bucket_json("test_bucket8")
            self.assertIn('flush', bucket_info['controllers'], "'flush' field should be defined in bucket info")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket8")

    def test_create_bucket_with_compression_mode(self):
        """Test CREATE BUCKET with compressionMode option"""
        try:
            # Test with compressionMode
            self.run_cbq_query("CREATE BUCKET test_bucket9 WITH {'storageBackend': 'couchstore', 'compressionMode': 'active'}")

            # Verify bucket was created with correct compression mode
            bucket_info = self.rest.get_bucket_json("test_bucket9")
            self.assertEqual(bucket_info['compressionMode'], 'active', "compressionMode should be active")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket9")

    def test_create_bucket_with_max_ttl(self):
        """Test CREATE BUCKET with maxTTL option"""
        try:
            # Test with maxTTL
            self.run_cbq_query("CREATE BUCKET test_bucket10 WITH {'storageBackend': 'couchstore', 'maxTTL': 3600}")

            # Verify bucket was created with correct maxTTL
            bucket_info = self.rest.get_bucket_json("test_bucket10")
            self.assertEqual(bucket_info['maxTTL'], 3600, "maxTTL should be 3600")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket10")

    def test_create_bucket_with_lww_conflict_resolution(self):
        """Test CREATE BUCKET with conflict resolution type"""
        try:
            # Test with conflict resolution type
            self.run_cbq_query("CREATE BUCKET test_bucket11 WITH {'storageBackend': 'couchstore', 'conflictResolutionType': 'lww'}")

            # Verify bucket was created with LWW conflict resolution
            bucket_info = self.rest.get_bucket_json("test_bucket11")
            self.assertEqual(bucket_info['conflictResolutionType'], 'lww', "conflictResolutionType should be lww")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket11")

    def test_create_bucket_with_invalid_num_vbuckets(self):
        """Test CREATE BUCKET with invalid numVBuckets option (256 is not allowed, only 128 and 1024 are valid)"""
        with self.assertRaises(CBQError, msg="Should fail for invalid numVBuckets value 256"):
            self.run_cbq_query("CREATE BUCKET test_bucket12 WITH {'storageBackend': 'couchstore', 'numVBuckets': 256}")
        # Clean up in case bucket was created (should not be)
        self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket12")

    def test_create_bucket_with_bucket_type(self):
        """Test CREATE BUCKET with bucketType option"""
        try:
            # Test with bucketType
            self.run_cbq_query("CREATE BUCKET test_bucket13 WITH {'storageBackend': 'couchstore', 'bucketType': 'ephemeral'}")

            # Verify bucket was created with correct bucket type
            bucket_info = self.rest.get_bucket_json("test_bucket13")
            self.assertEqual(bucket_info['bucketType'], 'ephemeral', "bucketType should be ephemeral")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket13")

    def test_create_bucket_with_replica_index(self):
        """Test CREATE BUCKET with replicaIndex option"""
        try:
            # Test with replicaIndex
            self.run_cbq_query("CREATE BUCKET test_bucket14 WITH {'storageBackend': 'couchstore', 'replicaIndex': 0}")

            # Verify bucket was created with correct replica index setting
            bucket_info = self.rest.get_bucket_json("test_bucket14")
            self.assertEqual(bucket_info['replicaIndex'], 0, "replicaIndex should be 0")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket14")

    def test_create_bucket_with_threads_number(self):
        """Test CREATE BUCKET with threadsNumber option"""
        try:
            # Test with threadsNumber (3 or 8)
            self.run_cbq_query("CREATE BUCKET test_bucket15 WITH {'ramQuota': 256, 'threadsNumber': 3}")

            # Verify bucket was created with correct threads number
            bucket_info = self.rest.get_bucket_json("test_bucket15")
            self.assertEqual(bucket_info['threadsNumber'], 3, "threadsNumber should be 3")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket15")

    def test_create_bucket_with_all_options(self):
        """Test CREATE BUCKET with all available options"""
        try:
            # Test with all options
            self.run_cbq_query("""
                CREATE BUCKET test_bucket16 WITH {
                    'ramQuota': 1024,
                    'storageBackend': 'couchstore',
                    'replicaNumber': 1,
                    'evictionPolicy': 'fullEviction',
                    'flushEnabled': 0,
                    'compressionMode': 'passive',
                    'maxTTL': 7200,
                    'conflictResolutionType': 'seqno',
                    'bucketType': 'couchbase',
                    'replicaIndex': 1,
                    'threadsNumber': 3
                }
            """)

            # Verify bucket was created with all correct options
            bucket_info = self.rest.get_bucket_json("test_bucket16")
            self._assert_ram_quota(bucket_info, 1024, "ramQuota should be 1024MB")
            self.assertEqual(bucket_info['storageBackend'], 'couchstore', "storageBackend should be couchstore")
            self.assertEqual(bucket_info['replicaNumber'], 1, "replicaNumber should be 1")
            self.assertEqual(bucket_info['evictionPolicy'], 'fullEviction', "evictionPolicy should be fullEviction")
            self.assertNotIn('flush', bucket_info['controllers'], "flush field should not be defined")
            self.assertEqual(bucket_info['compressionMode'], 'passive', "compressionMode should be passive")
            self.assertEqual(bucket_info['maxTTL'], 7200, "maxTTL should be 7200")
            self.assertEqual(bucket_info['conflictResolutionType'], 'seqno', "conflictResolutionType should be seqno")
            self.assertEqual(bucket_info['numVBuckets'], 1024, "numVBuckets should be 1024")
            self.assertEqual(bucket_info['bucketType'], 'membase', "bucketType should be membase")
            self.assertEqual(bucket_info['replicaIndex'], True, "replicaIndex should be True")
            self.assertEqual(bucket_info['threadsNumber'], 3, "threadsNumber should be 3")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket16")

    def test_create_bucket_duplicate_name(self):
        """Test CREATE BUCKET with duplicate name should fail"""
        try:
            # Create first bucket
            self.run_cbq_query("CREATE BUCKET test_bucket17 WITH {'storageBackend': 'couchstore'}")

            # Try to create bucket with same name - should fail
            with self.assertRaises(CBQError):
                self.run_cbq_query("CREATE BUCKET test_bucket17 WITH {'storageBackend': 'couchstore'}")

        finally:
                self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket17")

    def test_create_bucket_if_not_exists(self):
        """Test CREATE BUCKET IF NOT EXISTS"""
        try:
            # Create first bucket
            self.run_cbq_query("CREATE BUCKET test_bucket18 WITH {'storageBackend': 'couchstore'}")

            # Try to create bucket with same name using IF NOT EXISTS - should not fail
            self.run_cbq_query("CREATE BUCKET IF NOT EXISTS test_bucket18 WITH {'storageBackend': 'couchstore'}")

            # Verify bucket still exists
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket18'")
            self.assertEqual(len(result['results']), 1, "Bucket should still exist")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket18")

    def test_create_bucket_invalid_ramquota(self):
        """Test CREATE BUCKET with invalid ramQuota should fail"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket19 WITH {'ramQuota': -1}")

    def test_create_bucket_invalid_storage_backend(self):
        """Test CREATE BUCKET with invalid storageBackend should fail"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket20 WITH {'storageBackend': 'invalid'}")

    def test_create_bucket_invalid_replica_number(self):
        """Test CREATE BUCKET with invalid replicaNumber should fail"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket21 WITH {'replicaNumber': 5}")

    def test_create_bucket_invalid_eviction_policy(self):
        """Test CREATE BUCKET with invalid evictionPolicy should fail"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket22 WITH {'evictionPolicy': 'invalid'}")

    def test_create_bucket_invalid_compression_mode(self):
        """Test CREATE BUCKET with invalid compressionMode should fail"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket23 WITH {'compressionMode': 'invalid'}")

    def test_create_bucket_invalid_bucket_type(self):
        """Test CREATE BUCKET with invalid bucketType should fail"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket24 WITH {'bucketType': 'invalid'}")

    def test_create_bucket_magma_minimum_ram(self):
        """Test CREATE BUCKET with magma storage and minimum ram requirement"""
        try:
            # Test with minimum ram for magma (should be at least 256MB)
            self.run_cbq_query("CREATE BUCKET test_bucket25 WITH {'storageBackend': 'magma', 'ramQuota': 256}")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket25")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be 256MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket25")

    def test_create_bucket_couchstore_minimum_ram(self):
        """Test CREATE BUCKET with couchstore storage and minimum ram requirement"""
        try:
            # Test with minimum ram for couchstore (should be at least 100MB)
            self.run_cbq_query("CREATE BUCKET test_bucket26 WITH {'storageBackend': 'couchstore', 'ramQuota': 100}")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket26")
            self.assertEqual(bucket_info['storageBackend'], 'couchstore', "storageBackend should be couchstore")
            self._assert_ram_quota(bucket_info, 100, "ramQuota should be 100MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket26")

    def test_create_bucket_with_special_characters_in_name(self):
        """Test CREATE BUCKET with special characters in name"""
        try:
            # Test with underscore
            self.run_cbq_query("CREATE BUCKET test_bucket_27 WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket_27'")
            self.assertEqual(len(result['results']), 1, "Bucket with underscore should be created")

            # Test with hyphen
            self.run_cbq_query("CREATE BUCKET `test-bucket-28` WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test-bucket-28'")
            self.assertEqual(len(result['results']), 1, "Bucket with hyphen should be created")

            # Test with period
            self.run_cbq_query("CREATE BUCKET `test.bucket.29` WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test.bucket.29'")
            self.assertEqual(len(result['results']), 1, "Bucket with period should be created")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket_27")
            self.run_cbq_query("DROP BUCKET IF EXISTS `test-bucket-28`")
            self.run_cbq_query("DROP BUCKET IF EXISTS `test.bucket.29`")

    def test_create_bucket_with_numbers_in_name(self):
        """Test CREATE BUCKET with numbers in name"""
        try:
            # Test with numbers
            self.run_cbq_query("CREATE BUCKET test_bucket_30 WITH {'storageBackend': 'couchstore'}")
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket_30'")
            self.assertEqual(len(result['results']), 1, "Bucket with numbers should be created")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket_30")

    def test_create_bucket_with_quoted_name(self):
        """Test CREATE BUCKET with quoted name"""
        try:
            # Test with quoted name
            self.run_cbq_query('CREATE BUCKET "test_bucket_31" WITH {"storageBackend": "couchstore"}')
            result = self.run_cbq_query('SELECT * FROM system:buckets WHERE name = "test_bucket_31"')
            self.assertEqual(len(result['results']), 1, "Bucket with quoted name should be created")

        finally:
            self.run_cbq_query('DROP BUCKET IF EXISTS "test_bucket_31"')

    def test_create_bucket_with_backtick_name(self):
        """Test CREATE BUCKET with backtick quoted name"""
        try:
            # Test with backtick quoted name
            self.run_cbq_query("CREATE BUCKET `test_bucket_32` WITH {'storageBackend': 'couchstore'}")
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket_32'")
            self.assertEqual(len(result['results']), 1, "Bucket with backtick quoted name should be created")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS `test_bucket_32`")

    def test_create_bucket_case_sensitivity(self):
        """Test CREATE BUCKET case sensitivity"""
        try:
            # Test with different cases
            self.run_cbq_query("CREATE BUCKET Test_Bucket_33 WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)
            self.run_cbq_query("CREATE BUCKET TEST_BUCKET_34 WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)
            self.run_cbq_query("CREATE BUCKET test_bucket_35 WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)

            # Verify all buckets were created
            result = self.run_cbq_query("SELECT name FROM system:buckets WHERE name IN ['Test_Bucket_33', 'TEST_BUCKET_34', 'test_bucket_35']")
            self.assertEqual(len(result['results']), 3, "All case variants should be created as separate buckets")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS Test_Bucket_33")
            self.run_cbq_query("DROP BUCKET IF EXISTS TEST_BUCKET_34")
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket_35")

    def test_create_bucket_with_empty_options(self):
        """Test CREATE BUCKET with empty options object"""
        try:
            # Test with empty options
            self.run_cbq_query("CREATE BUCKET test_bucket36 WITH {}")

            # Verify bucket was created with defaults
            bucket_info = self.rest.get_bucket_json("test_bucket36")
            self._assert_ram_quota(bucket_info, 100, "Default ramQuota should be 100MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket36")

    def test_create_bucket_with_nested_json_options(self):
        """Test CREATE BUCKET with nested JSON in options"""
        try:
            # Test with nested JSON (should work as long as valid options are provided)
            self.run_cbq_query("CREATE BUCKET test_bucket37 WITH {'ramQuota': 256, 'storageBackend': 'magma'}")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket37")
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be 256MB")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket37")

    def test_create_bucket_with_whitespace_in_options(self):
        """Test CREATE BUCKET with whitespace in options"""
        try:
            # Test with whitespace in options
            self.run_cbq_query("CREATE BUCKET test_bucket38 WITH { 'ramQuota' : 256 , 'storageBackend' : 'magma' }")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket38")
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be 256MB")
            self.assertEqual(bucket_info['storageBackend'], 'magma', "storageBackend should be magma")

        finally:
                self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket38")

    def test_create_bucket_with_comments_in_options(self):
        """Test CREATE BUCKET with comments in options (should pass)"""
        try:
            # Test with comments in options
            self.run_cbq_query("CREATE BUCKET test_bucket39 WITH { /* comment */ 'ramQuota': 256 }")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket39")
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be 256MB")
        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket39")

    def test_create_bucket_with_trailing_comma_in_options(self):
        """Test CREATE BUCKET with trailing comma in options (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket40 WITH {'ramQuota': 256,}")

    def test_create_bucket_with_missing_quotes_in_options(self):
        """Test CREATE BUCKET with missing quotes in options (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket41 WITH {ramQuota: 256}")

    def test_create_bucket_with_invalid_json_syntax(self):
        """Test CREATE BUCKET with invalid JSON syntax (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket42 WITH {'ramQuota': 256, 'invalid'}")

    def test_create_bucket_with_duplicate_options(self):
        """Test CREATE BUCKET with duplicate options (last one should win)"""
        try:
            # Test with duplicate options
            self.run_cbq_query("CREATE BUCKET test_bucket43 WITH {'ramQuota': 100, 'ramQuota': 512}")

            # Verify bucket was created with last value
            bucket_info = self.rest.get_bucket_json("test_bucket43")
            self._assert_ram_quota(bucket_info, 512, "Last ramQuota value should be used")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket43")

    def test_create_bucket_with_unknown_options(self):
        """Test CREATE BUCKET with unknown options (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket44 WITH {'ramQuota': 256, 'unknownOption': 'value'}")

    def test_create_bucket_with_zero_ramquota(self):
        """Test CREATE BUCKET with zero ramQuota (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket45 WITH {'ramQuota': 0}")

    def test_create_bucket_with_very_large_ramquota(self):
        """Test CREATE BUCKET with very large ramQuota"""
        try:
            # Test with large ramQuota
            self.run_cbq_query("CREATE BUCKET test_bucket46 WITH {'ramQuota': 2048}")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket46")
            self._assert_ram_quota(bucket_info, 2048, "ramQuota should be 2048MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket46")

    def test_create_bucket_with_fractional_ramquota(self):
        """Test CREATE BUCKET with fractional ramQuota (should be rounded)"""
        try:
            # Test with fractional ramQuota
            self.run_cbq_query("CREATE BUCKET test_bucket47 WITH {'ramQuota': 256.5}")

            # Verify bucket was created (should be rounded down)
            bucket_info = self.rest.get_bucket_json("test_bucket47")
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be rounded down to 256MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket47")

    def test_create_bucket_with_string_ramquota(self):
        """Test CREATE BUCKET with string ramQuota (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket48 WITH {'ramQuota': '256'}")

    def test_create_bucket_with_boolean_ramquota(self):
        """Test CREATE BUCKET with boolean ramQuota (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket49 WITH {'ramQuota': true}")

    def test_create_bucket_with_null_ramquota(self):
        """Test CREATE BUCKET with null ramQuota (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket50 WITH {'ramQuota': null}")

    def test_create_bucket_with_array_ramquota(self):
        """Test CREATE BUCKET with array ramQuota (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket51 WITH {'ramQuota': [256]}")

    def test_create_bucket_with_object_ramquota(self):
        """Test CREATE BUCKET with object ramQuota (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket52 WITH {'ramQuota': {'value': 256}}")

    def test_create_bucket_with_negative_replica_number(self):
        """Test CREATE BUCKET with negative replicaNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket53 WITH {'replicaNumber': -1}")

    def test_create_bucket_with_fractional_replica_number(self):
        """Test CREATE BUCKET with fractional replicaNumber (should be rounded)"""
        try:
            # Test with fractional replicaNumber
            self.run_cbq_query("CREATE BUCKET test_bucket54 WITH {'storageBackend': 'couchstore', 'replicaNumber': 1.5}")

            # Verify bucket was created (should be rounded down)
            bucket_info = self.rest.get_bucket_json("test_bucket54")
            self.assertEqual(bucket_info['replicaNumber'], 1, "replicaNumber should be rounded down to 1")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket54")

    def test_create_bucket_with_string_replica_number(self):
        """Test CREATE BUCKET with string replicaNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket55 WITH {'storageBackend': 'couchstore', 'replicaNumber': '1'}")

    def test_create_bucket_with_boolean_replica_number(self):
        """Test CREATE BUCKET with boolean replicaNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket56 WITH {'storageBackend': 'couchstore', 'replicaNumber': true}")

    def test_create_bucket_with_null_replica_number(self):
        """Test CREATE BUCKET with null replicaNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket57 WITH {'storageBackend': 'couchstore', 'replicaNumber': null}")

    def test_create_bucket_with_array_replica_number(self):
        """Test CREATE BUCKET with array replicaNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket58 WITH {'storageBackend': 'couchstore', 'replicaNumber': [1]}")

    def test_create_bucket_with_object_replica_number(self):
        """Test CREATE BUCKET with object replicaNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket59 WITH {'storageBackend': 'couchstore', 'replicaNumber': {'value': 1}}")

    def test_create_bucket_with_negative_max_ttl(self):
        """Test CREATE BUCKET with negative maxTTL (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket60 WITH {'storageBackend': 'couchstore', 'maxTTL': -1}")

    def test_create_bucket_with_fractional_max_ttl(self):
        """Test CREATE BUCKET with fractional maxTTL (should be rounded)"""
        try:
            # Test with fractional maxTTL
            self.run_cbq_query("CREATE BUCKET test_bucket61 WITH {'storageBackend': 'couchstore', 'maxTTL': 3600.5}")

            # Verify bucket was created (should be rounded down)
            bucket_info = self.rest.get_bucket_json("test_bucket61")
            self.assertEqual(bucket_info['maxTTL'], 3600, "maxTTL should be rounded down to 3600")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket61")

    def test_create_bucket_with_string_max_ttl(self):
        """Test CREATE BUCKET with string maxTTL (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket62 WITH {'storageBackend': 'couchstore', 'maxTTL': '3600'}")

    def test_create_bucket_with_boolean_max_ttl(self):
        """Test CREATE BUCKET with boolean maxTTL (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket63 WITH {'storageBackend': 'couchstore', 'maxTTL': true}")

    def test_create_bucket_with_null_max_ttl(self):
        """Test CREATE BUCKET with null maxTTL (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket64 WITH {'storageBackend': 'couchstore', 'maxTTL': null}")

    def test_create_bucket_with_array_max_ttl(self):
        """Test CREATE BUCKET with array maxTTL (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket65 WITH {'storageBackend': 'couchstore', 'maxTTL': [3600]}")

    def test_create_bucket_with_object_max_ttl(self):
        """Test CREATE BUCKET with object maxTTL (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket66 WITH {'storageBackend': 'couchstore', 'maxTTL': {'value': 3600}}")

    def test_create_bucket_with_negative_num_vbuckets(self):
        """Test CREATE BUCKET with negative numVBuckets (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket67 WITH {'storageBackend': 'couchstore', 'numVBuckets': -1}")

    def test_create_bucket_with_fractional_num_vbuckets(self):
        """Test CREATE BUCKET with fractional numVBuckets (should be rounded)"""
        try:
            # Test with fractional numVBuckets
            self.run_cbq_query("CREATE BUCKET test_bucket68 WITH {'storageBackend': 'couchstore', 'numVBuckets': 128.5}")

            # Verify bucket was created (should be rounded down)
            bucket_info = self.rest.get_bucket_json("test_bucket68")
            self.assertEqual(bucket_info['numVBuckets'], 128, "numVBuckets should be rounded down to 128")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket68")

    def test_create_bucket_with_string_num_vbuckets(self):
        """Test CREATE BUCKET with string numVBuckets (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket69 WITH {'storageBackend': 'couchstore', 'numVBuckets': '128'}")

    def test_create_bucket_with_boolean_num_vbuckets(self):
        """Test CREATE BUCKET with boolean numVBuckets (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket70 WITH {'storageBackend': 'couchstore', 'numVBuckets': true}")

    def test_create_bucket_with_null_num_vbuckets(self):
        """Test CREATE BUCKET with null numVBuckets (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket71 WITH {'storageBackend': 'couchstore', 'numVBuckets': null}")

    def test_create_bucket_with_array_num_vbuckets(self):
        """Test CREATE BUCKET with array numVBuckets (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket72 WITH {'storageBackend': 'couchstore', 'numVBuckets': [128]}")

    def test_create_bucket_with_object_num_vbuckets(self):
        """Test CREATE BUCKET with object numVBuckets (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket73 WITH {'storageBackend': 'couchstore', 'numVBuckets': {'value': 128}}")

    def test_create_bucket_with_negative_threads_number(self):
        """Test CREATE BUCKET with negative threadsNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket74 WITH {'storageBackend': 'couchstore', 'threadsNumber': -1}")

    def test_create_bucket_with_fractional_threads_number(self):
        """Test CREATE BUCKET with fractional threadsNumber (should be rounded)"""
        try:
            # Test with fractional threadsNumber
            self.run_cbq_query("CREATE BUCKET test_bucket75 WITH {'storageBackend': 'couchstore', 'threadsNumber': 3.5}")

            # Verify bucket was created (should be rounded down)
            bucket_info = self.rest.get_bucket_json("test_bucket75")
            self.assertEqual(bucket_info['threadsNumber'], 3, "threadsNumber should be rounded down to 3")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket75")

    def test_create_bucket_with_string_threads_number(self):
        """Test CREATE BUCKET with string threadsNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket76 WITH {'storageBackend': 'couchstore', 'threadsNumber': '3'}")

    def test_create_bucket_with_boolean_threads_number(self):
        """Test CREATE BUCKET with boolean threadsNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket76 WITH {'storageBackend': 'couchstore', 'threadsNumber': true}")

    def test_create_bucket_with_null_threads_number(self):
        """Test CREATE BUCKET with null threadsNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket77 WITH {'storageBackend': 'couchstore', 'threadsNumber': null}")

    def test_create_bucket_with_array_threads_number(self):
        """Test CREATE BUCKET with array threadsNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket78 WITH {'storageBackend': 'couchstore', 'threadsNumber': [3]}")

    def test_create_bucket_with_object_threads_number(self):
        """Test CREATE BUCKET with object threadsNumber (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket79 WITH {'storageBackend': 'couchstore', 'threadsNumber': {'value': 3}}")

    def test_create_bucket_with_invalid_eviction_policy_values(self):
        """Test CREATE BUCKET with various invalid evictionPolicy values"""
        invalid_policies = ['invalid', 'none', 'random', 'lru', 'fifo']
        for i, policy in enumerate(invalid_policies):
            with self.assertRaises(CBQError):
                self.run_cbq_query(f"CREATE BUCKET test_bucket80_{i} WITH {{'storageBackend': 'couchstore', 'evictionPolicy': '{policy}'}}")

    def test_create_bucket_with_invalid_compression_mode_values(self):
        """Test CREATE BUCKET with various invalid compressionMode values"""
        invalid_modes = ['invalid', 'none', 'fast', 'best', 'default']
        for i, mode in enumerate(invalid_modes):
            with self.assertRaises(CBQError):
                self.run_cbq_query(f"CREATE BUCKET test_bucket81_{i} WITH {{'storageBackend': 'couchstore', 'compressionMode': '{mode}'}}")

    def test_create_bucket_with_invalid_bucket_type_values(self):
        """Test CREATE BUCKET with various invalid bucketType values"""
        invalid_types = ['invalid', 'memory', 'disk', 'hybrid', 'persistent']
        for i, bucket_type in enumerate(invalid_types):
            with self.assertRaises(CBQError):
                self.run_cbq_query(f"CREATE BUCKET test_bucket82_{i} WITH {{'storageBackend': 'couchstore', 'bucketType': '{bucket_type}'}}")

    def test_create_bucket_with_invalid_storage_backend_values(self):
        """Test CREATE BUCKET with various invalid storageBackend values"""
        invalid_backends = ['invalid', 'memory', 'disk', 'ssd', 'hdd']
        for i, backend in enumerate(invalid_backends):
            with self.assertRaises(CBQError):
                self.run_cbq_query(f"CREATE BUCKET test_bucket83_{i} WITH {{'storageBackend': '{backend}'}}")

    def test_create_bucket_with_invalid_conflict_resolution_values(self):
        """Test CREATE BUCKET with various invalid conflictResolutionType values"""
        invalid_resolutions = ['invalid', 'timestamp', 'sequence', 'manual', 'auto']
        for i, resolution in enumerate(invalid_resolutions):
            with self.assertRaises(CBQError):
                self.run_cbq_query(f"CREATE BUCKET test_bucket84_{i} WITH {{'storageBackend': 'couchstore', 'conflictResolutionType': '{resolution}'}}")

    def test_create_bucket_with_mixed_valid_invalid_options(self):
        """Test CREATE BUCKET with mix of valid and invalid options"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket85 WITH {'storageBackend': 'couchstore', 'ramQuota': 256, 'invalidOption': 'value'}")

    def test_create_bucket_with_nested_invalid_options(self):
        """Test CREATE BUCKET with nested invalid options"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket86 WITH {'storageBackend': 'couchstore', 'ramQuota': 256, 'nested': {'invalid': 'value'}}")

    def test_create_bucket_with_empty_string_values(self):
        """Test CREATE BUCKET with empty string values for string options"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket87 WITH {'storageBackend': ''}")

    def test_create_bucket_with_whitespace_only_string_values(self):
        """Test CREATE BUCKET with whitespace-only string values"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET test_bucket88 WITH {'storageBackend': '   '}")

    def test_create_bucket_with_very_long_name(self):
        """Test CREATE BUCKET with very long name (should work within limits)"""
        try:
            # Test with a long but valid name
            long_name = "a" * 100  # 100 character name
            self.run_cbq_query(f"CREATE BUCKET {long_name} WITH {{'storageBackend': 'couchstore'}}")

            # Verify bucket was created
            result = self.run_cbq_query(f"SELECT * FROM system:buckets WHERE name = '{long_name}'")
            self.assertEqual(len(result['results']), 1, "Long bucket name should be created")

        finally:
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {long_name}")

    def test_create_bucket_with_unicode_name(self):
        """Test CREATE BUCKET with unicode characters in name (should fail)"""
        unicode_name = "test_bucket_测试_89"
        with self.assertRaises(CBQError):
            self.run_cbq_query(f"CREATE BUCKET `{unicode_name}` WITH {{'storageBackend': 'couchstore'}}")

    def test_create_bucket_with_reserved_keywords(self):
        """Test CREATE BUCKET with reserved keywords as name"""
        try:
            # Test with reserved keyword (should work with proper quoting)
            self.run_cbq_query('CREATE BUCKET `select` WITH {"storageBackend": "couchstore"}')

            # Verify bucket was created
            result = self.run_cbq_query('SELECT * FROM system:buckets WHERE name = "select"')
            self.assertEqual(len(result['results']), 1, "Reserved keyword bucket name should be created")

        finally:
            self.run_cbq_query('DROP BUCKET IF EXISTS `select`')

    def test_create_bucket_with_special_sql_characters(self):
        """Test CREATE BUCKET with special SQL characters in name"""
        try:
            # Test with special characters
            special_name = "test_bucket_`~!@#$%^&*()_+-={}[]|\\:;\"'<>?,./"
            self.run_cbq_query(f'CREATE BUCKET `{special_name}` WITH {{"storageBackend": "couchstore"}}')

            # Verify bucket was created
            result = self.run_cbq_query(f'SELECT * FROM system:buckets WHERE name = `{special_name}`')
            self.assertEqual(len(result['results']), 1, "Special character bucket name should be created")

        finally:
            self.run_cbq_query(f'DROP BUCKET IF EXISTS `{special_name}`')

    def test_create_bucket_with_leading_numbers(self):
        """Test CREATE BUCKET with leading numbers in name"""
        try:
            # Test with leading numbers
            self.run_cbq_query("CREATE BUCKET `123test_bucket` WITH {'storageBackend': 'couchstore'}")

            # Verify bucket was created
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = '123test_bucket'")
            self.assertEqual(len(result['results']), 1, "Leading number bucket name should be created")

        finally:
            self.sleep(2)
            self.run_cbq_query("DROP BUCKET IF EXISTS `123test_bucket`")

    def test_create_bucket_with_leading_underscore(self):
        """Test CREATE BUCKET with leading underscore in name"""
        try:
            # Test with leading underscore
            self.run_cbq_query("CREATE BUCKET _test_bucket_90 WITH {'storageBackend': 'couchstore'}")

            # Verify bucket was created
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = '_test_bucket_90'")
            self.assertEqual(len(result['results']), 1, "Leading underscore bucket name should be created")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS _test_bucket_90")

    def test_create_bucket_with_leading_hyphen(self):
        """Test CREATE BUCKET with leading hyphen in name"""
        try:
            # Test with leading hyphen
            self.run_cbq_query("CREATE BUCKET `-test_bucket_91` WITH {'storageBackend': 'couchstore'}")

            # Verify bucket was created
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = '-test_bucket_91'")
            self.assertEqual(len(result['results']), 1, "Leading hyphen bucket name should be created")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS `-test_bucket_91`")

    def test_create_bucket_with_leading_period(self):
        """Test CREATE BUCKET with leading period in name (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET .test_bucket_92 WITH {'storageBackend': 'couchstore'}")

    def test_create_bucket_with_empty_name(self):
        """Test CREATE BUCKET with empty name (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET '' WITH {'storageBackend': 'couchstore'}")

    def test_create_bucket_with_whitespace_only_name(self):
        """Test CREATE BUCKET with whitespace-only name (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET '   ' WITH {'storageBackend': 'couchstore'}")

    def test_create_bucket_with_null_name(self):
        """Test CREATE BUCKET with null name (should fail)"""
        with self.assertRaises(CBQError):
            self.run_cbq_query("CREATE BUCKET null WITH {'storageBackend': 'couchstore'}")

    def test_create_bucket_with_very_large_ramquota(self):
        """Test CREATE BUCKET with very large ramQuota (should work within system limits)"""
        try:
            # Test with very large ramQuota
            self.run_cbq_query("CREATE BUCKET test_bucket93 WITH {'ramQuota': 8192}")

            # Verify bucket was created
            bucket_info = self.rest.get_bucket_json("test_bucket93")
            self._assert_ram_quota(bucket_info, 8192, "ramQuota should be 8192MB")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket93")

    def test_create_bucket_with_minimum_valid_values(self):
        """Test CREATE BUCKET with minimum valid values for all options"""
        try:
            # Test with minimum valid values
            self.run_cbq_query("""
                CREATE BUCKET test_bucket94 WITH {
                    'ramQuota': 100,
                    'storageBackend': 'couchstore',
                    'replicaNumber': 0,
                    'evictionPolicy': 'valueOnly',
                    'flushEnabled': 0,
                    'compressionMode': 'passive',
                    'maxTTL': 0,
                    'conflictResolutionType': 'seqno',
                    'bucketType': 'couchbase',
                    'replicaIndex': 0,
                    'threadsNumber': 3,
                    'numVBuckets': 1024
                }
            """)

            # Verify bucket was created with minimum values
            bucket_info = self.rest.get_bucket_json("test_bucket94")
            self._assert_ram_quota(bucket_info, 100, "ramQuota should be 100MB")
            self.assertEqual(bucket_info['storageBackend'], 'couchstore', "storageBackend should be couchstore")
            self.assertEqual(bucket_info['replicaNumber'], 0, "replicaNumber should be 0")
            self.assertEqual(bucket_info['evictionPolicy'], 'valueOnly', "evictionPolicy should be valueOnly")
            self.assertNotIn('flush', bucket_info['controllers'], "flush field should not be defined")
            self.assertEqual(bucket_info['compressionMode'], 'passive', "compressionMode should be passive")
            self.assertEqual(bucket_info['maxTTL'], 0, "maxTTL should be 0")
            self.assertEqual(bucket_info['conflictResolutionType'], 'seqno', "conflictResolutionType should be seqno")
            self.assertEqual(bucket_info['numVBuckets'], 1024, "numVBuckets should be 1024")
            self.assertEqual(bucket_info['bucketType'], 'membase', "bucketType should be membase")
            self.assertEqual(bucket_info['replicaIndex'], False, "replicaIndex should be False")
            self.assertEqual(bucket_info['threadsNumber'], 3, "threadsNumber should be 3")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket94")

    def test_create_bucket_with_maximum_valid_values(self):
        """Test CREATE BUCKET with maximum valid values for all options"""
        try:
            # Test with maximum valid values
            self.run_cbq_query("""
                CREATE BUCKET test_bucket95 WITH {
                    'ramQuota': 1024    ,
                    'storageBackend': 'magma',
                    'replicaNumber': 3,
                    'evictionPolicy': 'noEviction',
                    'flushEnabled': 1,
                    'compressionMode': 'active',
                    'maxTTL': 2147483647,
                    'conflictResolutionType': 'lww',
                    'bucketType': 'ephemeral',
                    'threadsNumber': 8,
                    'numVBuckets': 128
                }
            """)

            # Verify bucket was created with maximum values
            bucket_info = self.rest.get_bucket_json("test_bucket95")
            self._assert_ram_quota(bucket_info, 1024, "ramQuota should be 1024MB")
            self.assertEqual(bucket_info['replicaNumber'], 3, "replicaNumber should be 3")
            self.assertEqual(bucket_info['evictionPolicy'], 'noEviction', "evictionPolicy should be noEviction")
            self.assertIn('flush', bucket_info['controllers'], "'flush' field should be defined in bucket info")
            self.assertEqual(bucket_info['compressionMode'], 'active', "compressionMode should be active")
            self.assertEqual(bucket_info['maxTTL'], 2147483647, "maxTTL should be 2147483647")
            self.assertEqual(bucket_info['conflictResolutionType'], 'lww', "conflictResolutionType should be lww")
            self.assertEqual(bucket_info['numVBuckets'], 128, "numVBuckets should be 128")
            self.assertEqual(bucket_info['bucketType'], 'ephemeral', "bucketType should be ephemeral")
            self.assertEqual(bucket_info['threadsNumber'], 8, "threadsNumber should be 8")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket95")

    def test_create_multiple_buckets_concurrently(self):
        """Test creating multiple buckets concurrently"""
        try:
            # Create multiple buckets concurrently
            bucket_names = [f"concurrent_bucket_{i}" for i in range(5)]

            for bucket_name in bucket_names:
                self.run_cbq_query(f"CREATE BUCKET {bucket_name}")

            # Verify all buckets were created
            for bucket_name in bucket_names:
                result = self.run_cbq_query(f"SELECT * FROM system:buckets WHERE name = '{bucket_name}'")
                self.assertEqual(len(result['results']), 1, f"Bucket {bucket_name} should be created")

        finally:
            # Clean up all buckets
            for bucket_name in bucket_names:
                self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")

    def test_create_bucket_and_verify_system_catalog(self):
        """Test that created bucket appears in system catalog"""
        try:
            # Create bucket
            self.run_cbq_query("CREATE BUCKET test_bucket96 WITH {'storageBackend': 'couchstore'}")

            # Verify in system:buckets
            result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket96'")
            self.assertEqual(len(result['results']), 1, "Bucket should appear in system:buckets")

            # Verify in system:buckets (if available)
            try:
                result = self.run_cbq_query("SELECT * FROM system:buckets WHERE name = 'test_bucket96'")
                self.assertEqual(len(result['results']), 1, "Bucket should appear in system:buckets")
            except CBQError:
                # system:buckets might not be available in all versions
                self.log.info("system:buckets catalog not available")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket96")

    def test_create_bucket_and_insert_data(self):
        """Test that created bucket can be used for data operations"""
        try:
            # Create bucket
            self.run_cbq_query("CREATE BUCKET test_bucket97 WITH {'storageBackend': 'couchstore'}")

            # Insert data
            self.run_cbq_query("INSERT INTO test_bucket97 (KEY, VALUE) VALUES ('key1', {'name': 'test'})")

            # Query data
            result = self.run_cbq_query("SELECT * FROM test_bucket97")
            self.assertEqual(len(result['results']), 1, "Should be able to query data from created bucket")
            self.assertEqual(result['results'][0]['test_bucket97']['name'], 'test', "Data should be correctly inserted")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket97")

    def test_create_bucket_and_create_index(self):
        """Test that created bucket can be used for index operations"""
        try:
            # Create bucket
            self.run_cbq_query("CREATE BUCKET test_bucket98 WITH {'storageBackend': 'couchstore'}")

            # Insert some data
            self.run_cbq_query("INSERT INTO test_bucket98 (KEY, VALUE) VALUES ('key1', {'name': 'test1'})")
            self.run_cbq_query("INSERT INTO test_bucket98 (KEY, VALUE) VALUES ('key2', {'name': 'test2'})")

            # Create index
            self.run_cbq_query("CREATE INDEX idx_name ON test_bucket98(name)")

            # Verify index was created
            result = self.run_cbq_query("SELECT * FROM system:indexes WHERE keyspace_id = 'test_bucket98'")
            self.assertEqual(len(result['results']), 1, "Index should be created on the bucket")

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket98")

    def test_create_bucket_and_create_scope_collection(self):
        """Test that created bucket can be used for scope/collection operations"""
        try:
            # Create bucket
            self.run_cbq_query("CREATE BUCKET test_bucket99 WITH {'storageBackend': 'couchstore'}")
            self.sleep(2)

            # Create scope
            self.run_cbq_query("CREATE SCOPE test_bucket99.test_scope")
            self.sleep(2)

            # Create collection
            self.run_cbq_query("CREATE COLLECTION test_bucket99.test_scope.test_collection")
            self.sleep(2)

        finally:
            self.run_cbq_query("DROP BUCKET IF EXISTS test_bucket99")

    def test_create_bucket_rbac(self):
        """
        Test that a regular user with admin privilege can create a bucket.
        """
        username = "admin_user"
        password = "password123"
        bucket_name = "rbac_test_bucket"

        # Create user with admin role using N1QL statement
        self.run_cbq_query(f'CREATE USER {username} PASSWORD "{password}"')
        self.run_cbq_query(f"GRANT admin TO {username}")

        try:
            # Try to create a bucket as the bucket_admin user
            self.run_cbq_query(
                f"CREATE BUCKET {bucket_name} WITH {{'storageBackend': 'couchstore'}}",
                username=username,
                password=password
            )
            self.sleep(2)

            # Verify bucket was created (do not specify user)
            result = self.run_cbq_query(
                f"SELECT * FROM system:buckets WHERE name = '{bucket_name}'"
            )
            self.assertEqual(len(result['results']), 1, "Bucket should be created by bucket_admin user")

        finally:
            # Cleanup: drop the bucket and remove the user
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")
            self.run_cbq_query(f"DROP USER {username}")

    def test_create_bucket_rbac_negative(self):
        """
        Test that a user with ro_admin privilege cannot create a bucket.
        """
        username = "ro_admin_user"
        password = "password123"
        bucket_name = "rbac_negative_test_bucket"

        # Create user with ro_admin role using N1QL statement
        self.run_cbq_query(f'CREATE USER {username} PASSWORD "{password}"')
        self.run_cbq_query(f"GRANT ro_admin TO {username}")

        try:
            # Try to create a bucket as the ro_admin user, should fail
            with self.assertRaises(CBQError):
                self.run_cbq_query(
                    f"CREATE BUCKET {bucket_name} WITH {{'storageBackend': 'couchstore'}}",
                    username=username,
                    password=password
                )
            # Also verify that the bucket was not created
            result = self.run_cbq_query(
                f"SELECT * FROM system:buckets WHERE name = '{bucket_name}'"
            )
            self.assertEqual(len(result['results']), 0, "Bucket should not be created by ro_admin user")
        finally:
            # Cleanup: drop the bucket (if created) and remove the user
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")
            self.run_cbq_query(f"DROP USER {username}")

    def test_alter_bucket_basic(self):
        """
        Test ALTER BUCKET with various valid and invalid options.
        """
        bucket_name = "alter_test_bucket"
        try:
            # Create a bucket with initial options
            self.run_cbq_query(
                f"CREATE BUCKET {bucket_name} WITH {{'storageBackend': 'couchstore', 'ramQuota': 128, 'replicaNumber': 1, 'evictionPolicy': 'valueOnly', 'compressionMode': 'passive', 'maxTTL': 0, 'threadsNumber': 3}}"
            )
            self.sleep(2)

            # 1. Alter ramQuota
            self.run_cbq_query(
                f"ALTER BUCKET {bucket_name} WITH {{'ramQuota': 256}}"
            )
            bucket_info = self.rest.get_bucket_json(bucket_name)
            self._assert_ram_quota(bucket_info, 256, "ramQuota should be updated to 256MB")

            # 2. Alter replicaNumber
            self.run_cbq_query(
                f"ALTER BUCKET {bucket_name} WITH {{'replicaNumber': 2}}"
            )
            bucket_info = self.rest.get_bucket_json(bucket_name)
            self.assertEqual(bucket_info['replicaNumber'], 2, "replicaNumber should be updated to 2")

            # 3. Alter evictionPolicy
            self.run_cbq_query(
                f"ALTER BUCKET {bucket_name} WITH {{'evictionPolicy': 'fullEviction'}}"
            )
            bucket_info = self.rest.get_bucket_json(bucket_name)
            self.assertEqual(bucket_info['evictionPolicy'], 'fullEviction', "evictionPolicy should be updated to fullEviction")

            # 4. Alter compressionMode
            self.run_cbq_query(
                f"ALTER BUCKET {bucket_name} WITH {{'compressionMode': 'active'}}"
            )
            bucket_info = self.rest.get_bucket_json(bucket_name)
            self.assertEqual(bucket_info['compressionMode'], 'active', "compressionMode should be updated to active")

            # 5. Alter maxTTL
            self.run_cbq_query(
                f"ALTER BUCKET {bucket_name} WITH {{'maxTTL': 3600}}"
            )
            bucket_info = self.rest.get_bucket_json(bucket_name)
            self.assertEqual(bucket_info['maxTTL'], 3600, "maxTTL should be updated to 3600")

            # 6. Alter threadsNumber
            self.run_cbq_query(
                f"ALTER BUCKET {bucket_name} WITH {{'threadsNumber': 8}}"
            )
            bucket_info = self.rest.get_bucket_json(bucket_name)
            self.assertEqual(bucket_info['threadsNumber'], 8, "threadsNumber should be updated to 8")

            # 7. Try to alter forbidden fields: bucketType, storageBackend, replicaIndex, conflictResolutionType
            for field, value in [
                ("bucketType", "ephemeral"),
                ("storageBackend", "magma"),
                ("replicaIndex", True),
                ("conflictResolutionType", "lww"),
            ]:
                with self.assertRaises(CBQError, msg=f"Should not be able to alter {field}"):
                    self.run_cbq_query(
                        f"ALTER BUCKET {bucket_name} WITH {{{repr(field)}: {repr(value)}}}"
                    )

        finally:
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")

    def test_alter_database_basic(self):
        """
        Test ALTER DATABASE with valid options.
        """
        db_name = "alter_test_db"
        try:
            # Create a database with initial options
            self.run_cbq_query(
                f"CREATE DATABASE {db_name} WITH {{'ramQuota': 512, 'replicaNumber': 1, 'evictionPolicy': 'valueOnly', 'compressionMode': 'passive', 'maxTTL': 0, 'threadsNumber': 3}}"
            )
            self.sleep(2)

            # Alter multiple options at once
            self.run_cbq_query(
                f"ALTER DATABASE {db_name} WITH {{'ramQuota': 256, 'replicaNumber': 2, 'evictionPolicy': 'fullEviction', 'compressionMode': 'active', 'maxTTL': 7200, 'threadsNumber': 8}}"
            )
            db_info = self.rest.get_bucket_json(db_name)
            self._assert_ram_quota(db_info, 256, "ramQuota should be updated to 256MB")
            self.assertEqual(db_info['replicaNumber'], 2, "replicaNumber should be updated to 2")
            self.assertEqual(db_info['evictionPolicy'], 'fullEviction', "evictionPolicy should be updated to fullEviction")
            self.assertEqual(db_info['compressionMode'], 'active', "compressionMode should be updated to active")
            self.assertEqual(db_info['maxTTL'], 7200, "maxTTL should be updated to 7200")
            self.assertEqual(db_info['threadsNumber'], 8, "threadsNumber should be updated to 8")

            # Try to alter forbidden fields
            for field, value in [
                ("bucketType", "ephemeral"),
                ("storageBackend", "couchstore"),
                ("replicaIndex", True),
                ("conflictResolutionType", "lww"),
            ]:
                with self.assertRaises(CBQError, msg=f"Should not be able to alter {field}"):
                    self.run_cbq_query(
                        f"ALTER DATABASE {db_name} WITH {{{repr(field)}: {repr(value)}}}"
                    )

        finally:
            self.run_cbq_query(f"DROP DATABASE IF EXISTS {db_name}")

    def test_bucket_uuid_in_system_catalog(self):
        """
        Test that bucket UUID is correctly set and consistent across system catalogs.
        """
        bucket_name = "uuid_test_bucket"
        bucket_name2 = "uuid_test_bucket2"
        scope_name = "uuid_test_scope"
        collection_name = "uuid_test_collection"

        try:
            # Create bucket
            self.run_cbq_query(f"CREATE BUCKET {bucket_name} WITH {{'storageBackend': 'couchstore'}}")
            self.sleep(2)

            # Create scope
            self.run_cbq_query(f"CREATE SCOPE {bucket_name}.{scope_name}")
            self.sleep(2)

            # Create collection
            self.run_cbq_query(f"CREATE COLLECTION {bucket_name}.{scope_name}.{collection_name}")
            self.sleep(2)

            # Get bucket UUID from system:buckets
            result = self.run_cbq_query(f"SELECT uuid FROM system:buckets WHERE name = '{bucket_name}'")
            self.assertEqual(len(result['results']), 1, "Bucket should exist in system:buckets")
            bucket_uuid = result['results'][0]['uuid']
            self.assertIsNotNone(bucket_uuid, "Bucket UUID should not be null")
            self.assertIsInstance(bucket_uuid, str, "Bucket UUID should be a string")
            self.assertGreater(len(bucket_uuid), 0, "Bucket UUID should not be empty")

            # Verify UUID format (should be a 32-character hex string)
            import re
            uuid_pattern = re.compile(r'^[a-f0-9]{32}$')
            self.assertTrue(uuid_pattern.match(bucket_uuid), f"Bucket UUID should be a 32-character hex string, got: {bucket_uuid}")

            # Verify UUID in system:scopes
            result = self.run_cbq_query(f"SELECT bucket_uuid FROM system:scopes WHERE `bucket` = '{bucket_name}' AND name = '{scope_name}'")
            self.assertEqual(len(result['results']), 1, "Scope should exist in system:scopes")
            scope_bucket_uuid = result['results'][0]['bucket_uuid']
            self.assertEqual(scope_bucket_uuid, bucket_uuid, "Scope bucket_uuid should match bucket UUID")

            # Verify UUID in system:keyspaces for the bucket
            result = self.run_cbq_query(f"SELECT bucket_uuid FROM system:keyspaces WHERE name = '{bucket_name}'")
            self.assertEqual(len(result['results']), 1, "Bucket should exist in system:keyspaces")
            keyspace_bucket_uuid = result['results'][0]['bucket_uuid']
            self.assertEqual(keyspace_bucket_uuid, bucket_uuid, "Keyspace bucket_uuid should match bucket UUID")

            # Verify UUID in system:keyspaces for the collection
            result = self.run_cbq_query(f"SELECT bucket_uuid FROM system:keyspaces WHERE name = '{collection_name}' AND `scope` = '{scope_name}'")
            self.assertEqual(len(result['results']), 1, "Collection should exist in system:keyspaces")
            collection_bucket_uuid = result['results'][0]['bucket_uuid']
            self.assertEqual(collection_bucket_uuid, bucket_uuid, "Collection bucket_uuid should match bucket UUID")

            # Test with multiple buckets to ensure UUIDs are unique
            self.run_cbq_query(f"CREATE BUCKET {bucket_name2} WITH {{'storageBackend': 'couchstore'}}")
            self.sleep(2)

            result = self.run_cbq_query(f"SELECT uuid FROM system:buckets WHERE name = '{bucket_name2}'")
            self.assertEqual(len(result['results']), 1, "Second bucket should exist in system:buckets")
            bucket_uuid2 = result['results'][0]['uuid']

            # UUIDs should be different
            self.assertNotEqual(bucket_uuid, bucket_uuid2, "Different buckets should have different UUIDs")

            # Verify both UUIDs are valid
            self.assertTrue(uuid_pattern.match(bucket_uuid2), f"Second bucket UUID should be a 32-character hex string, got: {bucket_uuid2}")

        finally:
            # Clean up
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name2}")

    def test_bucket_uuid_persistence(self):
        """
        Test that bucket UUID remains consistent after bucket operations.
        """
        bucket_name = "uuid_persistence_test_bucket"

        try:
            # Create bucket
            self.run_cbq_query(f"CREATE BUCKET {bucket_name} WITH {{'storageBackend': 'couchstore'}}")
            self.sleep(2)

            # Get initial UUID
            result = self.run_cbq_query(f"SELECT uuid FROM system:buckets WHERE name = '{bucket_name}'")
            initial_uuid = result['results'][0]['uuid']

            # Alter bucket
            self.run_cbq_query(f"ALTER BUCKET {bucket_name} WITH {{'ramQuota': 256}}")
            self.sleep(2)

            # Verify UUID remains the same
            result = self.run_cbq_query(f"SELECT uuid FROM system:buckets WHERE name = '{bucket_name}'")
            altered_uuid = result['results'][0]['uuid']
            self.assertEqual(altered_uuid, initial_uuid, "Bucket UUID should remain consistent after ALTER")

            # Insert some data
            self.run_cbq_query(f"INSERT INTO {bucket_name} (KEY, VALUE) VALUES ('test_key', {{'test': 'value'}})")

            # Verify UUID still remains the same
            result = self.run_cbq_query(f"SELECT uuid FROM system:buckets WHERE name = '{bucket_name}'")
            data_uuid = result['results'][0]['uuid']
            self.assertEqual(data_uuid, initial_uuid, "Bucket UUID should remain consistent after data operations")

        finally:
            self.run_cbq_query(f"DROP BUCKET IF EXISTS {bucket_name}")