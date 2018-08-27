from tuq import QueryTests


class QueryMiscTests(QueryTests):

    def setUp(self):
        super(QueryMiscTests, self).setUp()
        self.log.info("==============  QueriesIndexTests setup has started ==============")
        self.log.info("==============  QueriesIndexTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryMiscTests, self).suite_setUp()
        self.log.info("==============  QueriesIndexTests suite_setup has started ==============")
        self.log.info("==============  QueriesIndexTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueriesIndexTests tearDown has started ==============")
        self.log.info("==============  QueriesIndexTests tearDown has completed ==============")
        super(QueryMiscTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueriesIndexTests suite_tearDown has started ==============")
        self.log.info("==============  QueriesIndexTests suite_tearDown has completed ==============")
        super(QueryMiscTests, self).suite_tearDown()

    '''MB-30946: Empty array from index scan not working properly when backfill is used'''
    def test_empty_array_low_scancap(self):
        createdIndex = False
        createdBucket = False
        try:
            temp_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)
            createdBucket = True
            self.query = 'INSERT INTO temp_bucket VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]}),' \
                         'VALUES(UUID(),{"severity":"low","deferred":[]});'
            res = self.run_cbq_query()

            self.query = 'CREATE INDEX ix1 ON temp_bucket(severity,deferred);'
            res = self.run_cbq_query()

            self._wait_for_index_online("temp_bucket", "ix1")
            createdIndex = True

            self.query = 'SELECT META().id, deferred FROM temp_bucket WHERE severity = "low";'
            expect_res1 = self.run_cbq_query()
            self.assertEqual(len(expect_res1['results']), 10)
            for item in expect_res1['results']:
                self.assertEqual(item["deferred"], [])

            self.query = 'SELECT META().id, deferred FROM temp_bucket WHERE severity = "low" AND EVERY v IN deferred SATISFIES v != "X" END;'
            expect_res2 = self.run_cbq_query()
            self.assertEqual(len(expect_res2['results']), 10)
            for item in expect_res2['results']:
                self.assertEqual(item["deferred"], [])

            self.query = 'SELECT META().id, deferred FROM temp_bucket WHERE severity = "low";'
            actual_res1 = self.run_cbq_query(query_params={"scan_cap": 2})
            self.assertEqual(actual_res1['results'], expect_res1['results'])

            self.query = 'SELECT META().id, deferred FROM temp_bucket WHERE severity = "low" AND EVERY v IN deferred SATISFIES v != "X" END;'
            actual_res2 = self.run_cbq_query(query_params={"scan_cap": 2})
            self.assertEqual(actual_res2['results'], expect_res2['results'])
        finally:
            if createdIndex:
                self.query = 'DROP INDEX temp_bucket.ix1'
                self.run_cbq_query()
                self.wait_for_index_drop("temp_bucket", "ix1", ["severity", "deferred"], self.gsi_type)
            if createdBucket:
                self.cluster.bucket_delete(self.master, "temp_bucket")

    '''MB-28636: query with OrderedIntersect scan returns empty result set intermittently'''
    def test_orderintersectscan_nonempty_results(self):
        createdIndexes = {}
        createdBucket = False
        try:
            temp_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)
            createdBucket = True

            self.query = 'CREATE primary index ON temp_bucket'
            self.run_cbq_query()
            self._wait_for_index_online("temp_bucket", "#primary")
            createdIndexes["#primary"] = []

            self.query = 'INSERT INTO temp_bucket VALUES(UUID(), {"CUSTOMER_ID":551,"MSISDN":UUID(), "ICCID":UUID()})'
            self.run_cbq_query()

            self.query = 'INSERT INTO temp_bucket (KEY UUID(), VALUE d) SELECT {"CUSTOMER_ID":551,"MSISDN":UUID(),"ICCID":UUID()} AS d  FROM temp_bucket WHERE CUSTOMER_ID == 551;'
            for i in range(0, 8):
                self.run_cbq_query()
            self.query = 'select * from temp_bucket'
            res = self.run_cbq_query()
            self.assertEqual(len(res['results']), 256)

            self.query = 'CREATE INDEX `xi1` ON `temp_bucket`(`CUSTOMER_ID`,`MSISDN`);'
            self.run_cbq_query()
            self._wait_for_index_online("temp_bucket", "xi1")
            createdIndexes["xi1"] = ['CUSTOMER_ID', 'MSISDN']

            self.query = 'CREATE INDEX `ai_SIM` ON `temp_bucket`(distinct pairs({`CUSTOMER_ID`, `ICCID`, `MSISDN` }));'
            self.run_cbq_query()
            self._wait_for_index_online("temp_bucket", "ai_SIM")
            createdIndexes["ai_SIM"] = ['distinct pairs{"CUSTOMER_ID": CUSTOMER_ID, "ICCID": ICCID, "MSISDN": MSISDN}']

            self.query = 'select TIM_ID, MSISDN from temp_bucket WHERE CUSTOMER_ID = 551 ORDER BY MSISDN ASC LIMIT 2 OFFSET 0 '
            for i in range(0, 100):
                res = self.run_cbq_query()
                self.assertEqual(len(res['results']), 2)
        finally:
            for index in createdIndexes.keys():
                if index == "#primary":
                    self.query = "DROP primary index on temp_bucket"
                else:
                    self.query = 'DROP INDEX temp_bucket.'+str(index)
                self.run_cbq_query()
                self.wait_for_index_drop("temp_bucket", index, createdIndexes[index], self.gsi_type)
            if createdBucket:
                self.cluster.bucket_delete(self.master, "temp_bucket")