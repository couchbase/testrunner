from .tuq import QueryTests
import threading
import time
import json


class QueryMiscTests(QueryTests):

    def setUp(self):
        super(QueryMiscTests, self).setUp()
        self.log.info("==============  QueryMiscTests setup has started ==============")
        self.log.info("==============  QueryMiscTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryMiscTests, self).suite_setUp()
        self.log.info("==============  QueryMiscTests suite_setup has started ==============")
        self.log.info("==============  QueryMiscTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryMiscTests tearDown has started ==============")
        self.log.info("==============  QueryMiscTests tearDown has completed ==============")
        super(QueryMiscTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryMiscTests suite_tearDown has started ==============")
        self.log.info("==============  QueryMiscTests suite_tearDown has completed ==============")
        super(QueryMiscTests, self).suite_tearDown()

    '''MB-32120'''
    def test_xattrs_use_keys(self):
        queries = dict()

        assert1 = lambda x: self.assertEqual(x['q_res'][0]['status'], 'success')
        assert2 = lambda x: self.assertEqual(x['q_res'][0]['metrics']['resultCount'], 0)
        assert3 = lambda x: self.assertEqual(x['q_res'][0]['results'], [])
        asserts = [assert1, assert2, assert3]

        queries["a"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS []"], "asserts": asserts}
        queries["b"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\"\"]"], "asserts": asserts}
        queries["c"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\" \"]"], "asserts": asserts}
        queries["d"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\"\", \"\"]"], "asserts": asserts}
        queries["e"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\" \", \"\"]"], "asserts": asserts}
        queries["f"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\" \", \" \"]"], "asserts": asserts}
        queries["g"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\"xxxx\"]"], "asserts": asserts}
        queries["h"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\"xxxx\", \"yyyy\"]"], "asserts": asserts}
        queries["i"] = {"queries": ["SELECT META().xattrs._sync FROM default USE KEYS [\"xxxx\", \"xxxx\"]"], "asserts": asserts}

        self.query_runner(queries)


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
            for index in list(createdIndexes.keys()):
                if index == "#primary":
                    self.query = "DROP primary index on temp_bucket"
                else:
                    self.query = 'DROP INDEX temp_bucket.'+str(index)
                self.run_cbq_query()
                self.wait_for_index_drop("temp_bucket", index, createdIndexes[index], self.gsi_type)
            if createdBucket:
                self.cluster.bucket_delete(self.master, "temp_bucket")

    def test_intersectscan_thread_growth(self):
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

            self.query = 'CREATE INDEX ix1 ON `temp_bucket`(a)'
            self.run_cbq_query()
            self._wait_for_index_online("temp_bucket", "ix1")
            createdIndexes["ix1"] = ['a']

            self.query = 'CREATE INDEX ix2 ON `temp_bucket`(b)'
            self.run_cbq_query()
            self._wait_for_index_online("temp_bucket", "ix2")
            createdIndexes["ix2"] = ['b']

            for i in range(0, 20000):
                self.query = 'INSERT INTO `temp_bucket` (KEY, VALUE) VALUES ("'+str(i)+'", {"a":'+str(i)+', "b":'+str(i)+'})'
                self.run_cbq_query()
            self.query = 'select * from temp_bucket'
            res = self.run_cbq_query()
            self.assertEqual(len(res['results']), 20000)

            self.query = 'explain select * from `temp_bucket` where a > 0 and b > 0 limit 1000'
            res = self.run_cbq_query()
            self.assertTrue("IntersectScan" in str(res['results'][0]['plan']))
            self.assertTrue("ix1" in str(res['results'][0]['plan']))
            self.assertTrue("ix2" in str(res['results'][0]['plan']))

            self.log.info("priming query engine")
            self.query = 'select * from `temp_bucket` where a > 0 and b > 0 limit 1000'
            for i in range(0, 10):
                res = self.run_cbq_query()
                self.assertEqual(len(res['results']), 1000)
            port_for_version = "8093"
            cbversion = self.cb_version.split(".")
            major_version = cbversion[0]
            minor_version = cbversion[1]
            if (int(major_version) < 5) or (int(major_version) == 5 and int(minor_version) < 5):
                port_for_version = "6060"
            curl_cmd = "curl -u Administrator:password http://localhost:"+port_for_version+"/debug/pprof/goroutine?debug=2"
            curl_output = self.shell.execute_command(curl_cmd)
            pprof_list = curl_output[0]
            count_intersect = 0
            for item in pprof_list:
                if "Intersect" in str(item) or "intersect" in str(item):
                    count_intersect = count_intersect + 1
            self.log.info("number of intersect threads after primed: "+str(count_intersect))

            self.sleep(10)

            # run query 1000 times now
            for i in range(0, 1000):
                res = self.run_cbq_query()
                self.assertEqual(len(res['results']), 1000)

            curl_output = self.shell.execute_command(curl_cmd)
            pprof_list = curl_output[0]
            count_intersect_a = 0
            for item in pprof_list:
                if "Intersect" in str(item) or "intersect" in str(item):
                    count_intersect_a = count_intersect_a + 1

            self.log.info("number of intersect threads A: "+str(count_intersect_a))
            self.assertEqual(count_intersect_a, 0)

            self.sleep(60)

            curl_output = self.shell.execute_command(curl_cmd)
            pprof_list = curl_output[0]
            count_intersect_b = 0
            for item in pprof_list:
                if "Intersect" in str(item) or "intersect" in str(item):
                    count_intersect_b = count_intersect_b + 1
            self.log.info("number of intersect threads B: "+str(count_intersect_b))
            self.assertEqual(count_intersect_b, 0)
        finally:
            for index in list(createdIndexes.keys()):
                if index == "#primary":
                    self.query = "DROP primary index on temp_bucket"
                else:
                    self.query = 'DROP INDEX temp_bucket.'+str(index)
                self.run_cbq_query()
                self.wait_for_index_drop("temp_bucket", index, createdIndexes[index], self.gsi_type)
            if createdBucket:
                self.cluster.bucket_delete(self.master, "temp_bucket")

    '''MB-31600 Indexing meta().id for binary data was broken, the index would contain no data'''
    '''bug has not been fixed, will fail until then'''
    def test_indexing_meta(self):
        self.fail_if_no_buckets()
        idx_list = []
        item_count = 10
        for bucket in self.buckets:
            if bucket.name == "default":
                self.cluster.bucket_flush(self.master, bucket=bucket, timeout=180000)
        bucket_doc_map = {"default": 0}
        bucket_status_map = {"default": "healthy"}
        self.wait_for_buckets_status(bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
        self.shell.execute_cbworkloadgen("Administrator", "password", item_count, 100, 'default', 1024, '')
        bucket_doc_map = {"default": 10}
        self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
        try:
            self.run_cbq_query(query="CREATE INDEX idx1 on default(meta().id)")
            self._wait_for_index_online("default", "idx1")
            self.sleep(10)
            idx_list.append('idx1')

            i = 0
            found = False
            while i < 10:
                for server in self.servers:
                    curl_output = self.shell.execute_command("%s -u Administrator:password http://%s:9102/stats"
                                                         % (self.curl_path, server.ip))
                    # The above command returns a tuple, we want the first element of that tuple
                    expected_curl = self.convert_list_to_json(curl_output[0])
                    self.log.info(str(expected_curl))

                    if 'default:idx1:items_count' in expected_curl or i == 9:
                        self.assertEqual(expected_curl['default:idx1:items_count'], item_count)
                        found = True
                        break
                if found:
                    break
                i += 1
                self.sleep(3)

            self.run_cbq_query(query="CREATE INDEX idx2 on default(meta().cas)")
            self._wait_for_index_online("default", "idx2")
            self.sleep(10)
            idx_list.append('idx2')

            i = 0
            found = False
            while i < 10:
                for server in self.servers:
                    curl_output = self.shell.execute_command("%s -u Administrator:password http://%s:9102/stats"
                                                             % (self.curl_path, server.ip))
                    # The above command returns a tuple, we want the first element of that tuple
                    expected_curl = self.convert_list_to_json(curl_output[0])
                    self.log.info(str(expected_curl))

                    if 'default:idx2:items_count' in expected_curl or i == 9:
                        self.assertEqual(expected_curl['default:idx2:items_count'], item_count)
                        found = True
                        break
                if found:
                    break
                i += 1
                self.sleep(3)

            self.run_cbq_query(query="CREATE INDEX idx3 on default(meta().expiration)")
            self._wait_for_index_online("default", "idx3")
            self.sleep(10)
            idx_list.append('idx3')

            i = 0
            found = False
            while i < 10:
                for server in self.servers:
                    curl_output = self.shell.execute_command("%s -u Administrator:password http://%s:9102/stats"
                                                             % (self.curl_path, server.ip))
                    # The above command returns a tuple, we want the first element of that tuple
                    expected_curl = self.convert_list_to_json(curl_output[0])
                    self.log.info(str(expected_curl))
                    if 'default:idx3:items_count' in expected_curl or i == 9:
                        self.assertEqual(expected_curl['default:idx3:items_count'], item_count)
                        found = True
                        break
                if found:
                    break
                i += 1
                self.sleep(3)

        finally:
            for idx in idx_list:
                drop_query = "DROP INDEX default.%s" % (idx)
                self.run_cbq_query(query=drop_query)

    def test_indexer_endpoints(self):
        curl = "curl "
        credentials = [("full", "-u Administrator:password "), ("none", " ")]
        protocols = [("insecure", " ", "http://", ":9102"), ("secure", "-k ", "https://", ":19102")]
        ips = [self.master.ip]
        endpoints = ["/stats",
                     "/listMetadataTokens",
                     "/debug/pprof/",
                     "/debug/pprof/goroutine",
                     "/debug/pprof/block",
                     "/debug/pprof/heap",
                     "/debug/pprof/threadcreate",
                     "/debug/pprof/profile",
                     "/debug/pprof/cmdline",
                     "/debug/pprof/symbol",
                     "/debug/pprof/trace",
                     "/debug/vars",
                     "/registerRebalanceToken",
                     "/listRebalanceTokens",
                     "/cleanupRebalance",
                     "/moveIndex",
                     "/moveIndexInternal",
                     "/nodeuuid",
                     "/api/indexes",
                     "/settings",
                     "/internal/settings",
                     "/triggerCompaction",
                     "/settings/runtime/freeMemory",
                     "/settings/runtime/forceGC",
                     "/plasmaDiag",
                     "/listMetadataTokens",
                     "/stats",
                     "/stats/mem",
                     "/stats/storage/mm",
                     "/stats/storage",
                     "/stats/reset",
                     "/createIndex",
                     "/createIndexRebalance",
                     "/dropIndex",
                     "/buildIndex",
                     "/getLocalIndexMetadata",
                     "/getIndexMetadata",
                     "/restoreIndexMetadata",
                     "/getIndexStatus",
                     "/getIndexStatement",
                     "/planIndex",
                     "/settings/storageMode",
                     "/api/index",
                     "/api",
                     "/listCreateTokens"]
        for ip in ips:
            for endpoint in endpoints:
                for credential in credentials:
                    # secure and insecure with credentials should return same data
                    # secure and insecure without credentials should return same error
                    self.log.info("\n hitting endpoint with credentials: "+credential[0])
                    endpoint_responses = []
                    for protocol in protocols:
                        self.log.info("\n using: "+protocol[0])
                        cmd = curl + credential[1] + protocol[1] + protocol[2] + ip + protocol[3] + endpoint
                        curl_output = self.shell.execute_command(cmd)
                        endpoint_responses.append(curl_output)
                        self.log.info("\n"+str(curl_output)+"\n")
                    for response in endpoint_responses:
                        if credential == "full":
                            self.assertTrue('No web credentials found in request.' not in response[0] and '401 Unauthorized' not in response[0])
                        if credential == "none":
                            self.assertTrue('No web credentials found in request.' in response[0] or '401 Unauthorized' in response[0])

    '''https://issues.couchbase.com/browse/CBSE-6593'''
    def test_query_cpu_max_utilization(self):
        try:
            self.cluster.bucket_flush(self.master, bucket="default", timeout=180000)
            self.query = "create index idx1 on default(ln,lk)"
            self.run_cbq_query()
            self._wait_for_index_online("default", "idx1")
            createdIndex = True

            thread_list = []
            for i in range(0, 250):
                t = threading.Thread(target=self.run_insert_query)
                t.daemon = True
                t.start()
                thread_list.append(t)

            for t in thread_list:
                t.join()

            bucket_doc_map = {"default": 250000}
            self.wait_for_bucket_docs(bucket_doc_map, 5, 120)

            end_time = time.time() + 60
            cpu_rdy = False
            while time.time() < end_time:
                cluster_stats = self.rest.get_cluster_stats()
                node_stats = cluster_stats[str(self.master.ip) + ":8091"]
                cpu_utilization = node_stats['cpu_utilization']
                self.log.info("waiting for cpu utilization (" + str(cpu_utilization) + ") < 20.00")
                if cpu_utilization < 20.00:
                    cpu_rdy = True
                    break
            self.assertTrue(cpu_rdy)
            self.log.info("cpu ready")

            for i in range(0, 20):
                t = threading.Thread(target=self.run_select_queries)
                t.daemon = True
                t.start()
                thread_list.append(t)

            end_time = time.time() + 65
            cpu_stats = []
            while time.time() < end_time:
                cluster_stats = self.rest.get_cluster_stats()
                node_stats = cluster_stats[str(self.master.ip) + ":8091"]
                cpu_utilization = node_stats['cpu_utilization']
                self.log.info("**** CPU Utilization is " + str(cpu_utilization) + "****")
                cpu_stats.append(cpu_utilization)
                self.sleep(1)

            for t in thread_list:
                t.join()

            response, content = self.rest.get_query_vitals(self.master)
            query_vitals = json.loads(content)
            self.log.info("query vitals: " + str(query_vitals))
            self.log.info("cpu utilization stats: " + str(cpu_stats))
            self.assertTrue(query_vitals['request.per.sec.1min'] > 3)
            for cpu_utilization in cpu_stats:
                self.assertTrue(cpu_utilization < 99.99)
        finally:
            if createdIndex:
                self.drop_index("default", "idx1")
                self.wait_for_index_drop("default", "idx1")
            self.cluster.bucket_flush(self.master, bucket="default", timeout=180000)

    def run_insert_query(self):
        values = 'VALUES(UUID(),{"ln":"null","lk":"null"}),'*999
        values = values + 'VALUES(UUID(),{"ln":"null","lk":"null"});'
        query = 'INSERT INTO default ' + values
        self.run_cbq_query(query=query)

    def run_select_queries(self):
        end_time = time.time() + 60
        query = 'SELECT default.* FROM default WHERE ln = "null" AND lk != "null"'
        while time.time() < end_time:
            res = self.run_cbq_query(query=query)