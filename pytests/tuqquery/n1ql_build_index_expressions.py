from .tuq import QueryTests


class QueryBuildIndexExpressionsTests(QueryTests):

    def setUp(self):
        super(QueryBuildIndexExpressionsTests, self).setUp()
        self.log.info("==============  QueryBuildIndexExpressionsTests setup has started ==============")
        self.can_teardown = True
        self.indexes = ["ix1", "ix2", "ix3", "ix4", "ix5", "ix6", "ix7", "ix8", "ix9", "ix10"]
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.query_bucket = self.query_buckets[0]
        for index in self.indexes:
            self.run_cbq_query("CREATE index %s on %s(join_yr) WITH {'defer_build':true}" % (index, self.query_bucket))
        wait_for_index_list = [(self.query_bucket, index, 'deferred') for index in self.indexes]
        self.wait_for_index_status_bulk(wait_for_index_list)
        self.log.info("==============  QueryBuildIndexExpressionsTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryBuildIndexExpressionsTests, self).suite_setUp()
        self.log.info("==============  QueryBuildIndexExpressionsTests suite_setup has started ==============")
        self.log.info("==============  QueryBuildIndexExpressionsTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryBuildIndexExpressionsTests tearDown has started ==============")
        if hasattr(self, 'can_teardown'):
            self.drop_all_indexes(bucket=self.query_bucket, leave_primary=True)
        self.log.info("==============  QueryBuildIndexExpressionsTests tearDown has completed ==============")
        super(QueryBuildIndexExpressionsTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryBuildIndexExpressionsTests suite_tearDown has started ==============")
        self.log.info("==============  QueryBuildIndexExpressionsTests suite_tearDown has completed ==============")
        super(QueryBuildIndexExpressionsTests, self).suite_tearDown()

    def test_build_index_with_identifiers(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}(ix1, ix2, ix3, ix4, ix5)".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix1', 'ix2', 'ix3', 'ix4', 'ix5']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)
        results = self.run_cbq_query("BUILD index on {0}(ix9, ix6, ix6, ix7, ix6, ix6, ix8, ix8, ix8, ix9)".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix6', 'ix7', 'ix8', 'ix9']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 9)
        results = self.run_cbq_query("BUILD index on {0}(ix10, ix10, ix10, ix10, ix10, ix10, ix10, ix10, ix10, ix10)".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix10']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 10)

    def test_build_index_with_strings(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}('ix1', 'ix2', 'ix3', 'ix4', 'ix5')".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix1', 'ix2', 'ix3', 'ix4', 'ix5']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)
        results = self.run_cbq_query("BUILD index on {0}('ix9', 'ix6', 'ix6', 'ix7', 'ix6', 'ix6', 'ix8', 'ix8', "
                                     "'ix8', 'ix9')".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix6', 'ix7', 'ix8', 'ix9']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 9)
        results = self.run_cbq_query("BUILD index on {0}('ix10', 'ix10', 'ix10', 'ix10', 'ix10', 'ix10', 'ix10', "
                                     "'ix10', 'ix10', 'ix10')".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix10']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 10)

    def test_build_index_with_arrays(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}(['ix1'], ['ix2'], ['ix3'], ['ix4'], ['ix5'])".format(
            self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix1', 'ix2', 'ix3', 'ix4', 'ix5']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)
        results = self.run_cbq_query("BUILD index on {0}(['ix9', 'ix6', 'ix6'], ['ix7'], "
                                     "['ix6', 'ix6', 'ix8', 'ix8', 'ix8', 'ix9'])".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix6', 'ix7', 'ix8', 'ix9']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 9)
        results = self.run_cbq_query("BUILD index on {0}(['ix10', 'ix10', 'ix10', 'ix10', 'ix10', 'ix10'], "
                                     "['ix10', 'ix10', 'ix10', 'ix10'])".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix10']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 10)

    def test_build_index_with_subqueries(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}("
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name = 'ix1'), "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name = 'ix2'), "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name = 'ix3'), "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name = 'ix4'), "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name = 'ix5'))".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix1', 'ix2', 'ix3', 'ix4', 'ix5']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)
        results = self.run_cbq_query("BUILD index on {0}((SELECT RAW name FROM system:indexes WHERE keyspace_id = "
                                     "'{0}' AND state = 'deferred' and name in ['ix9', 'ix6']), "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name in ['ix7']), "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name in ['ix6', 'ix8', 'ix9']))".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix6', 'ix7', 'ix8', 'ix9']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 9)
        results = self.run_cbq_query("BUILD index on {0}((select raw 'ix10' from default limit 10))".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix10']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 10)

    def test_build_index_with_mixed_types(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}("
                                     "ix1, "
                                     "'ix2', "
                                     "['ix3'], "
                                     "['ix4'], "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name = 'ix5'))".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix1', 'ix2', 'ix3', 'ix4', 'ix5']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)
        results = self.run_cbq_query("BUILD index on {0}("
                                     "ix6, 'ix9', 'ix6', ix9, "
                                     "['ix7', 'ix6', 'ix7', 'ix10'], "
                                     "(SELECT RAW name FROM system:indexes WHERE keyspace_id = '{0}' AND state = "
                                     "'deferred' and name in ['ix6', 'ix8', 'ix9']))".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix6', 'ix7', 'ix8', 'ix9', 'ix10']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 10)

    def test_build_index_many_expressions(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}((select raw 'ix10' from default))".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix10']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 1)

    def test_build_index_invalid_index(self):
        initial_num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertTrue(initial_num_indexes_online == 0 or initial_num_indexes_online == 1)
        results = self.run_cbq_query("BUILD index on {0}(ix1, ix2, ix3, ix4, ix5)".format(self.query_bucket))
        build_index_status = results['status']
        self.assertEqual(build_index_status, 'success')
        for index in ['ix1', 'ix2', 'ix3', 'ix4', 'ix5']:
            self.wait_for_index_status(self.query_bucket, index, "online")
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)

        # index already built
        try:
            results = self.run_cbq_query("BUILD index on {0}(ix6, ix6, ix6, ix1)".format(self.query_bucket))
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue('Index ix1 is already built' in str(ex))

        build_index_status = results['status']
        self.log.info(str(build_index_status))
        self.assertEqual(build_index_status, 'success')
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)

        try:
            results = self.run_cbq_query("BUILD index on {0}(['ix6', 'ix6', 'ix6', 'ix1'])".format(self.query_bucket))
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue('Index ix1 is already built' in str(ex))

        build_index_status = results['status']
        self.log.info(str(build_index_status))
        self.assertEqual(build_index_status, 'success')
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)

        # index doesnt exist
        try:
            results = self.run_cbq_query("BUILD index on {0}(ix6, ix6, ix6, ix16)".format(self.query_bucket))
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue('GSI index ix16 not found' in str(ex))

        build_index_status = results['status']
        self.log.info(str(build_index_status))
        self.assertEqual(build_index_status, 'success')
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)

        try:
            results = self.run_cbq_query("BUILD index on {0}(['ix6', 'ix6', 'ix6', 'ix16'])".format(self.query_bucket))
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue('GSI index ix16 not found' in str(ex))

        build_index_status = results['status']
        self.log.info(str(build_index_status))
        self.assertEqual(build_index_status, 'success')
        num_indexes_online = self.get_index_count(self.query_bucket, 'online')
        self.assertEqual(num_indexes_online, initial_num_indexes_online + 5)





