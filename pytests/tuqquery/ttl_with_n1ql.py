import sys
import time

from couchbase.cluster import Cluster
try:
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import ClusterOptions
except ImportError:
    from couchbase.cluster import PasswordAuthenticator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests


class QueryExpirationTests(QueryTests):

    def setUp(self):
        super(QueryExpirationTests, self).setUp()
        self.log.info("==============  QueryExpirationTests setup has started ==============")
        self.index_to_be_created = self.input.param("index_to_be_created", '')
        self.query_to_be_run = self.input.param("query_to_be_run", '')
        self.log.info("==============  QueryExpirationTests setup has completed ==============")
        self.log_config_info()
        self.index_stat = 'items_count'
        self.exp_index = 'idx_expire'
        self.default_bucket_name = self.input.param('bucket_name', 'default')
        self.cb_rest = RestConnection(self.master)
        authenticator = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        try:
            self.cb_cluster = Cluster('couchbase://{0}'.format(self.master.ip))
            self.cb_cluster.authenticate(authenticator)
        except Exception as e:
            self.cb_cluster = Cluster('couchbase://{0}'.format(self.master.ip), ClusterOptions(authenticator))

    def tearDown(self):
        self.log.info("==============  QueryExpirationTests tearDown has started ==============")
        super(QueryExpirationTests, self).tearDown()
        self.log.info("==============  QueryExpirationTests tearDown has completed ==============")

    def test_insert_with_ttl(self):
        """
        @summary: Inserting docs with expiration value and checking if doc expired after passage of expiration time
        1. Start a cluster with some docs with no expiration value. Create Indexes, one primary and another on
         expiration value, to index docs in bucket.
        2. Create some docs with expiration, say 10 sec
        3. sleep for time so that doc expires (15 sec)
        4. Validate that doc has expired and indexers have also catch up with expired doc
        5. Validate that KV stats also have vb_active_expired equals to no. of expired docs

        @Note: -i b/resources/tmp.ini -p doc-per-day=10,nodes_init=1,initial_index_number=1,primary_index_type=GSI
         -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_insert_with_ttl
        """
        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)

        num_iter = 10
        expiration_time = 10
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)

        inserted_docs = []
        # creating index on meta.expiration
        expiration_index_query = "CREATE INDEX {1} ON {0} ( META().expiration )".format(query_default_bucket,
                                                                                        self.exp_index)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        cb = self.cb_cluster.open_bucket(self.default_bucket_name)
        indexed_item = 0
        item_count_before_inserts = \
            self.cb_rest.get_index_stats()[self.default_bucket_name][self.exp_index][self.index_stat]
        for num in range(num_iter):
            doc_id = "k{0}".format(num)
            inserted_docs.append(doc_id)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query_1 = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                             ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query_1)

            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)

            doc_id = doc_id + "_" + str(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query_2 = 'INSERT INTO {0}  VALUES ({1}, {{"expiration": {2}}})'.format(query_default_bucket,
                                                                                           doc_body,
                                                                                           expiration_time)
            self.run_cbq_query(insert_query_2)

            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)

            indexed_item += 2

        # Validating docs expiration setting
        self.sleep(1.5 * expiration_time, "Waiting for docs to get expired")
        for doc_id in inserted_docs:
            doc = cb.get(doc_id, quiet=True)
            self.assertEqual(doc.value, None,
                             "Inserted doc hasn't expired after passage of expiration time")
        select_query = "SELECT * FROM {0}".format(query_default_bucket)
        self.run_cbq_query(select_query)
        # Getting index count after expiration of document
        result = self._is_expected_index_count(self.default_bucket_name, self.exp_index,
                                               self.index_stat, item_count_before_inserts)
        self.assertTrue(result, "Expired docs are still available with indexer")
        kv_stat_result = self._is_expected_kv_expired_stats(bucket, num_iter * 2)
        self.assertTrue(kv_stat_result, "KV vb_active_expired stat is not matching with actual no. of expired docs")

    def test_insert_with_ttl_with_nested_select(self):
        """
        @summary: Inserting docs with expiration using queries with nested select query bringing data from travel-sample
        1. Load beer-sample and create empty bucket named 'default'
        2. create index on meta().expiration and primary index for beer-sample
        3. Inserting beer-sample docs into default and update the meta().expiration for them.
        4. Validate that docs in default bucket are expired after passage of expiry time.
        5. Update the expiration for docs in beer-sample.
        6. Insert beer-sample docs into default bucket with retaining expiration time from beer-sample.

        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_insert_with_ttl_with_nested_select
        """
        expiration_time = 10
        # Loading beer-sample bucket
        result_count = 7303
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)

        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_default_bucket)
        self.run_cbq_query(expiration_index_query)

        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        # Inserting docs into default from beer-sample
        insert_query = 'INSERT INTO {0} (KEY doc_keys, VALUE doc, OPTIONS {{"expiration": {1}}})  ' \
                       'SELECT META(b).id AS doc_keys, b AS doc  FROM {2} AS b'.format(query_default_bucket,
                                                                                       expiration_time,
                                                                                       query_sample_bucket)
        self.run_cbq_query(insert_query)

        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=result_count)
        self.assertTrue(result, "Indexer not able to index all the items")

        self.sleep(expiration_time + 10, "Allowing time to all the docs to get expired")
        select_query = "Select * FROM {0}".format(query_default_bucket)
        results = self.run_cbq_query(select_query)
        self.assertEqual(len(results['results']), 0, "Docs with expiry value are not expired even after"
                                                     " passage of expiration time")

        kv_stat_result = self._is_expected_kv_expired_stats(bucket, result_count)
        self.assertTrue(kv_stat_result, "KV vb_active_expired stat is not matching with actual no. of expired docs")
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "Indexer has indexes for expired docs")

        # Inserting docs with expiration time with nested select
        update_expiration_on_beer_sample = 'UPDATE {0} AS b SET META(b).expiration={1}'.format(query_sample_bucket,
                                                                                               expiration_time * 3)
        self.run_cbq_query(update_expiration_on_beer_sample)

        insert_query_with_exp = 'INSERT INTO {0} (KEY doc_keys, VALUE doc, OPTIONS {{"expiration": exptime}})' \
                                '  SELECT META(t).id AS doc_keys, t AS doc, META(t).expiration AS exptime' \
                                '  FROM {1} AS t'.format(query_default_bucket, query_sample_bucket)
        self.run_cbq_query(insert_query_with_exp)
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=result_count)
        self.assertTrue(result, "Indexer not able to index all the items")

        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "All docs didn't expired after the passage of time")
        results = self.run_cbq_query(select_query)
        self.assertEqual(len(results['results']), 0, "Docs with expiry value are not expired even after"
                                                     " passage of expiration time")

        kv_stat_result = self._is_expected_kv_expired_stats(bucket, result_count * 2)
        self.assertTrue(kv_stat_result, "KV vb_active_expired stat is not matching with actual no. of expired docs")
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "Indexer has indexes for expired docs")

    def test_insert_with_valid_invalid_ttl(self):
        """
        @summary: testing for different values of ttl
        1. Create a bucket named default.
        2. Insert docs into bucket with invalid expiration value.
        3. Check for Insert with some unusual valid expiration value. (Check for all the variation of Insert statement)

        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_insert_with_invalid_ttl
        """
        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_default_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        # Insert query with invalid ttl values
        invalid_expiration_time_list = [-1, '"ten"', [10, 10], sys.float_info.max, sys.maxsize + 1]
        for expiration_time, num in zip(invalid_expiration_time_list, list(range(len(invalid_expiration_time_list)))):
            doc_id = "K{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)

            select_exp_query = "SELECT META().expiration FROM {0}" \
                               " WHERE META().id = '{1}'".format(query_default_bucket, doc_id)
            result = self.run_cbq_query(select_exp_query)
            self.assertEqual(result['results'][0]['expiration'], 0, "Expiration is set to an invalid value")

        # Running an insert with expired expiration epoch time
        expiration_time = 30 * 24 * 60 * 60 + 1
        doc_body = '"12345", {"id":"12345"}'
        insert_query = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                       ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
        self.run_cbq_query(insert_query)
        doc_value = self.run_cbq_query(f"select * from {query_default_bucket} where id = '12345'")
        self.assertEqual(doc_value['results'], [], "Insert doc with expiration epoch value is still available in db")

        # Insert Queries with some valid values
        valid_expiration_time_list = [100000.4, sys.maxsize, int(time.time() + 120)]
        for expiration_time, num in zip(valid_expiration_time_list, list(range(len(valid_expiration_time_list)))):
            doc_id = "M{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)

            select_query = "SELECT META().expiration FROM {0}" \
                           " WHERE META().id = '{1}'".format(query_default_bucket, doc_id)
            result = self.run_cbq_query(select_query)
            self.assertNotEqual(int(result['results'][0]['expiration']), 0, "Expiration is not set to a valid value")

        # Loading beer-sample bucket
        result_count = 7303
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)
        num_docs = self.get_item_count(self.master, bucket)
        self.log.info(f'num_docs is {num_docs}')
        get_doc_ids_query = 'SELECT META().id FROM {0}'.format(query_sample_bucket)
        result = self.run_cbq_query(get_doc_ids_query)
        doc_ids_for_new_insertion = ['k00', 'k01', 'k02', 'k03']
        for item in result['results']:
            doc_ids_for_new_insertion.append(item['id'])

        # Insert Queries with 0 ttl
        query_with_zero_ttl = ['INSERT INTO {0} (KEY, VALUE) VALUES'
                               ' ("k00", {{"id":"k00"}})'.format(query_default_bucket),
                               'INSERT INTO {0} (KEY, VALUE) VALUES ("k01", {{"id":"k01"}},'
                               ' {{"expiration":0}})'.format(query_default_bucket),
                               'INSERT INTO {0} VALUES ("k02", {{"id":"k02"}},'
                               ' {{"expiration":0}})'.format(query_default_bucket),
                               'INSERT INTO {0}  VALUES ("k03", {{"id":"k03"}})'.format(query_default_bucket),
                               'INSERT INTO {0} (KEY doc_key, VALUE doc, OPTIONS {{"expiration": 0}})'
                               ' SELECT META(t).id AS doc_key, t AS doc'
                               ' FROM {1} AS t'.format(query_default_bucket, query_sample_bucket)]
        for query in query_with_zero_ttl:
            self.run_cbq_query(query)

        self.sleep(30)
        select_exp_query = "SELECT META().id, META().expiration FROM {0}".format(query_default_bucket)
        result = self.run_cbq_query(select_exp_query)
        for item in result['results']:
            if item['id'] in doc_ids_for_new_insertion:
                self.assertEqual(item['expiration'], 0, "Expiration is not set to 0 for new insertions")
        self.run_cbq_query('select * from {0}'.format(query_default_bucket))
        self.sleep(10)
        new_num_docs = self.get_item_count(self.master, bucket)
        self.assertEqual(new_num_docs, num_docs + len(query_with_zero_ttl) - 1 + result_count,
                         "Some of newly inserted docs are missing")

    def test_upsert_with_ttl(self):
        """
        @summary: Upserting docs with expiration value and checking if doc expired after passage of expiration time
        1. Create a default bucket. Create Indexes, one primary and another on expiration value
        2. Upsert(new insert) some docs with expiration, say 10 sec
        3. Upsert(update) docs to have new modified expiration
        4. Validate that doc has expired and indexers have also catch up with expired doc
        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_upsert_with_ttl
        """
        num_iter = 10
        expiration_time = 10

        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        cb = self.cb_cluster.open_bucket(self.default_bucket_name)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_default_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        inserted_docs = []
        for num in range(num_iter):
            doc_id = "k{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            inserted_docs.append(doc_id)
            # UPSERT for creating doc first time
            upsert_query_1 = 'UPSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                             ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(upsert_query_1)
            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)
            # UPSERT for updating above creating doc
            upsert_query_2 = 'UPSERT INTO {0}  VALUES ({1},' \
                             ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time * 2)
            self.run_cbq_query(upsert_query_2)

            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)

        # Validating docs expiration setting
        self.sleep(expiration_time * 2 + 10, "Waiting for docs to get expired")
        for doc_id in inserted_docs:
            doc = cb.get(doc_id, quiet=True)
            self.assertEqual(doc.value, None,
                             "Inserted doc hasn't expired after passage of expiration time")
        select_query = "SELECT * FROM {0}".format(query_default_bucket)
        self.run_cbq_query(select_query)
        # Getting index count after expiration of document
        result = self._is_expected_index_count(self.default_bucket_name, self.exp_index, self.index_stat, 0)
        self.assertTrue(result, "Expired docs are still available with indexer")
        kv_stat_result = self._is_expected_kv_expired_stats(bucket, num_iter)
        self.assertTrue(kv_stat_result, "KV vb_active_expired stat is not matching with actual no. of expired docs")

    def test_upsert_with_ttl_with_nested_select(self):
        """
        @summary: Upserting docs with expiration value with a nested select and checking if doc expired
        after passage of expiration time
        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_upsert_with_ttl_with_nested_select
        """
        expiration_time = 60

        # Loading beer-sample bucket
        result_count = 7303
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)

        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_default_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        upsert_query = 'UPSERT INTO {0} (KEY doc_keys, VALUE doc, OPTIONS {{"expiration": {1}}})  ' \
                       'SELECT META(b).id AS doc_keys, b AS doc  FROM {2} AS b'.format(query_default_bucket,
                                                                                       expiration_time,
                                                                                       query_sample_bucket)
        self.run_cbq_query(upsert_query)

        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, max_try=50, expected_stat_value=result_count)
        self.assertTrue(result, "Indexer not able to index all the items")
        self.sleep(expiration_time + 10, "Allowing time to all the docs to get expired")
        select_query = "Select * FROM {0}".format(query_default_bucket)
        results = self.run_cbq_query(select_query)
        self.assertEqual(len(results['results']), 0, "Docs with expiry value are not expired even after"
                                                     " passage of expiration time")
        kv_stat_result = self._is_expected_kv_expired_stats(bucket, result_count)
        self.assertTrue(kv_stat_result, "KV vb_active_expired stat is not matching with actual no. of expired docs")
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, max_try = 50, expected_stat_value=0)
        self.assertTrue(result, "Indexer has indexes for expired docs")

        # Inserting docs with expiration time with nested select
        update_expiration_on_beer_sample = 'UPDATE {0} AS b SET META(b).expiration = {1}'.format(query_sample_bucket, expiration_time)
        result = self.run_cbq_query(update_expiration_on_beer_sample)
        self.log.info(result)

        upsert_query_with_exp = 'UPSERT INTO {0} (KEY doc_keys, VALUE doc, OPTIONS {{"expiration": exptime}})' \
                                '  SELECT META(t).id AS doc_keys, t AS doc, META(t).expiration AS exptime' \
                                '  FROM {1} AS t'.format(query_default_bucket, query_sample_bucket)
        result = self.run_cbq_query(upsert_query_with_exp)
        self.log.info(result)

        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, max_try = 50, expected_stat_value=result_count)
        self.assertTrue(result, "Indexer not able to index all the items")
        self.sleep(expiration_time + 10, "Allowing time to all the docs to get expired")
        select_query = "Select * FROM {0}".format(query_default_bucket)
        results = self.run_cbq_query(select_query)
        self.assertEqual(len(results['results']), 0, "Docs with expiry value are not expired even after"
                                                     " passage of expiration time")

        kv_stat_result = self._is_expected_kv_expired_stats(bucket, result_count * 2)
        self.assertTrue(kv_stat_result, "KV vb_active_expired stat is not matching with actual no. of expired docs")
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                               stat_name=self.index_stat, max_try = 50, expected_stat_value=0)
        self.assertTrue(result, "Indexer has indexes for expired docs")

    def test_upsert_with_valid_invalid_ttl(self):
        """
        @summary: testing upsert for different values of ttl
        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_upsert_with_invalid_ttl
        """
        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_default_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        # Upsert query with invalid ttl values
        invalid_expiration_time_list = [-1, '"ten"', [10, 10], sys.float_info.max, sys.maxsize + 1]
        for expiration_time, num in zip(invalid_expiration_time_list, list(range(len(invalid_expiration_time_list)))):
            doc_id = "K{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'UPSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)

            select_exp_query = "SELECT META().expiration FROM {0}" \
                               " WHERE META().id = '{1}'".format(query_default_bucket, doc_id)
            result = self.run_cbq_query(select_exp_query)
            self.assertEqual(result['results'][0]['expiration'], 0, "Expiration is set to an invalid value")

        # Running an upsert with expired expiration epoch time
        expiration_time = 30 * 24 * 60 * 60 + 1
        doc_body = '"12345", {"id":"12345"}'
        insert_query = 'UPSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                       ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
        self.run_cbq_query(insert_query)
        doc_value = self.run_cbq_query(f"select * from {query_default_bucket} where id = '12345'")
        self.assertEqual(doc_value['results'], [], "Insert doc with expiration epoch value is still available in db")

        # Insert Queries with some valid values
        valid_expiration_time_list = [100000.4, sys.maxsize, int(time.time() + 120)]
        for expiration_time, num in zip(valid_expiration_time_list, list(range(len(valid_expiration_time_list)))):
            doc_id = "K{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'UPSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)

            select_query = "SELECT META().expiration FROM {0}" \
                           " WHERE META().id = '{1}'".format(query_default_bucket, doc_id)
            result = self.run_cbq_query(select_query)
            self.assertNotEqual(int(result['results'][0]['expiration']), 0, "Expiration is not set to a valid value")

        # Loading beer-sample bucket
        result_count = 7303
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)
        num_docs = self.get_item_count(self.master, bucket)
        get_doc_ids_query = 'SELECT META().id FROM {0}'.format(query_sample_bucket)
        result = self.run_cbq_query(get_doc_ids_query)
        doc_ids_for_new_insertion = ['k00', 'k01', 'k02', 'k03']
        for item in result['results']:
            doc_ids_for_new_insertion.append(item['id'])

        # Insert Queries with 0 ttl
        query_with_zero_ttl = ['UPSERT INTO {0} (KEY, VALUE) VALUES'
                               ' ("k00", {{"id":"k00"}})'.format(query_default_bucket),
                               'UPSERT INTO {0} (KEY, VALUE) VALUES ("k01", {{"id":"k01"}},'
                               ' {{"expiration":0}})'.format(query_default_bucket),
                               'UPSERT INTO {0} VALUES ("k02", {{"id":"k02"}},'
                               ' {{"expiration":0}})'.format(query_default_bucket),
                               'UPSERT INTO {0}  VALUES '
                               '("k03", {{"id":"k03"}})'.format(query_default_bucket),
                               'UPSERT INTO {0} (KEY doc_key, VALUE doc, OPTIONS {{"expiration": 0}})'
                               ' SELECT META(t).id AS doc_key, '
                               't AS doc FROM {1} AS t'.format(query_default_bucket, query_sample_bucket)]
        for query in query_with_zero_ttl:
            self.run_cbq_query(query)

        self.sleep(30)
        select_exp_query = "SELECT META().id, META().expiration FROM {0}".format(query_default_bucket)
        result = self.run_cbq_query(select_exp_query)
        for item in result['results']:
            if item['id'] in doc_ids_for_new_insertion:
                self.assertEqual(item['expiration'], 0, "Expiration is not set to 0 for new insertions")

        self.run_cbq_query('select * from {0}'.format(query_default_bucket))
        self.sleep(10)
        new_num_docs = self.get_item_count(self.master, bucket)
        self.assertEqual(new_num_docs, num_docs + len(query_with_zero_ttl) - 1 + result_count,
                         "Some of newly inserted docs are missing")

    def test_update_scenarios_for_ttl(self):
        """
        @summary:
        1. Load beer-sample bucket and create Index on meta().expiration
        2. Run a update query on beer-sample bucket (just a quick sanity)
        """
        expiration_time = 30
        # Loading beer-sample bucket
        result_count = 7303
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)

        bucket = self.rest.get_bucket(self.sample_bucket)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_sample_bucket)
        self.run_cbq_query(expiration_index_query)

        # Running an update query on few documents(test10)
        update_count = 20
        update_query = 'UPDATE {0} AS b SET b.type = "Brewery"' \
                       ' WHERE b.type = "brewery" LIMIT {1}'.format(query_sample_bucket, update_count)
        self.run_cbq_query(update_query)
        check_update_query = 'SELECT COUNT(*) FROM {0} WHERE type = "Brewery"'.format(query_sample_bucket)
        result = self.run_cbq_query(check_update_query)
        self.assertEqual(result['results'][0]['$1'], update_count,
                         "No. of updated documents is not matching with expected values.")

        # Updating expiration time of all documents in bucket (test11)
        update_expiry_query = 'UPDATE {0} AS b SET META(b).expiration = {1}'.format(query_sample_bucket,
                                                                                    expiration_time)
        self.run_cbq_query(update_expiry_query)
        checking_expiry_set_query = 'SELECT COUNT(*) FROM {0} WHERE META().expiration > 0'.format(query_sample_bucket)
        result = self.run_cbq_query(checking_expiry_set_query)
        self.assertEqual(result['results'][0]['$1'], result_count,
                         "No. of updated documents is not matching with expected values.")

        # Updating some documents with expiration value without retaining expiration value(test9)
        self.run_cbq_query(update_query)
        check_update_query = 'SELECT COUNT(*) FROM {0} WHERE META().expiration = 0'.format(query_sample_bucket)
        result = self.run_cbq_query(check_update_query)
        self.assertEqual(result['results'][0]['$1'], update_count,
                         "No. of updated documents is not matching with expected values.")

        # Updating some document's expiration value back to 0 from valid expiration value(test12)
        update_expiry_to_zero = 'UPDATE {0} AS b SET META(b).expiration = 0 ' \
                                'WHERE b.type = "beer" LIMIT {1}'.format(query_sample_bucket, update_count)
        self.run_cbq_query(update_expiry_to_zero)
        check_update_query = 'SELECT COUNT(*) FROM {0} WHERE type = "beer"' \
                             ' AND META().expiration = 0'.format(query_sample_bucket)
        result = self.run_cbq_query(check_update_query)
        self.assertEqual(result['results'][0]['$1'], update_count,
                         "No. of updated documents is not matching with expected values.")

        # updating document field while retaining their expiration values (test13)
        update_field_with_expiry_query = 'UPDATE {0} AS b SET b.type = "Brewery",' \
                                         ' META(b).expiration = META(b).expiration' \
                                         ' WHERE b.type = "brewery" LIMIT {1}'.format(query_sample_bucket,
                                                                                      update_count)
        self.run_cbq_query(update_field_with_expiry_query)
        self.sleep(15)
        check_update_query = 'SELECT COUNT(`type`) FROM {0} where `type` = "Brewery"'.format(query_sample_bucket)
        result = self.run_cbq_query(check_update_query)
        self.assertEqual(result['results'][0]['$1'], update_count * 3,  # for previous 2 updates + this update
                         "No. of updated documents is not matching with expected values.")
        result = self._is_expected_index_count(self.sample_bucket, self.exp_index, self.index_stat, update_count * 2)
        self.assertTrue(result, "Indexer not able to index all the items")

    def test_doc_ttl_with_bucket_ttl(self):
        """
        @summary: Testing bucket ttl with doc ttl.
            - Case 1 : bucket ttl < doc ttl --> bucket ttl overrides doc ttl
            - Case 2 : doc ttl < bucket ttl --> bucket ttl doesn't overrides doc ttl
            - Case 3 : doc ttl = bucket ttl --> bucket ttl doesn't overrides doc ttl
        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_doc_ttl_with_bucket_ttl
        """
        bucket_ttl = 10
        expiration_time = 10 * 24 * 60 * 60
        num_iter = 10

        # creating default bucket
        default_params = self._create_bucket_params(server=self.master, size=256,
                                                    replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                    enable_replica_index=self.enable_replica_index,
                                                    eviction_policy=self.eviction_policy, lww=self.lww,
                                                    maxttl=bucket_ttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)
        bucket = self.rest.get_bucket(self.default_bucket_name)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_default_bucket = self.get_collection_name(self.default_bucket_name)

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_default_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_default_bucket)
        self.run_cbq_query(primary_index_query)

        # Inserting docs with ttl higher than bucket ttl and check if the ttl is set to bucket ttl
        cb = self.cb_cluster.open_bucket(bucket_name=self.default_bucket_name)

        for num in range(num_iter):
            doc_id = "k{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)
            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)
        # checking for doc to expire
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                      stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "Indexer still has indexes for expired docs")
        # Changing bucket ttl to a higher value
        bucket_ttl = 5 * 60
        expiration_time = 30
        self.rest.change_bucket_props(bucket=self.default_bucket_name, maxTTL=bucket_ttl)

        # inserting docs with ttl < bucket ttl
        for num in range(num_iter):
            doc_id = "k{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'UPSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)
            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)
        # Validating if the doc ttl is not overridden by bucket ttl
        get_expiration_query = 'SELECT META().expiration FROM {0}'.format(query_default_bucket)
        results = self.run_cbq_query(get_expiration_query)['results']
        for result in results:
            # checking if bucket ttl doesn't override doc ttl. Keeping a buffer range of 10 sec
            self.assertNotEqual(result['expiration'], 0, "Bucket ttl has overridden doc ttl")
        # checking for doc to expire
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                      stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

        # inserting docs with ttl = bucket ttl
        self.rest.change_bucket_props(bucket=self.default_bucket_name, maxTTL=expiration_time)
        for num in range(num_iter):
            doc_id = "k{0}".format(num)
            doc_body = '"{0}", {{"id":"{0}"}}'.format(doc_id)
            insert_query = 'UPSERT INTO {0} (KEY, VALUE) VALUES ({1},' \
                           ' {{"expiration": {2}}})'.format(query_default_bucket, doc_body, expiration_time)
            self.run_cbq_query(insert_query)
            # Checking if doc is created through sdk
            doc_value = cb.get(doc_id).value
            self.assertEqual(doc_value['id'], doc_id)
        # Validating if the doc ttl is not overridden by bucket ttl
        get_expiration_query = 'SELECT META().expiration FROM {0}'.format(query_default_bucket)
        results = self.run_cbq_query(get_expiration_query)['results']
        for result in results:
            # checking if bucket ttl doesn't override doc ttl. Keeping a buffer range of 10 sec
            self.assertNotEqual(result['expiration'], 0, "Bucket ttl has overridden doc ttl")
        # checking for doc to expire
        result = self._is_expected_index_count(bucket_name=self.default_bucket_name, idx_name=self.exp_index,
                                      stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

    def test_merge_update_ttl(self):
        """
        @summary: Testing ttl with MERGE clause
        -i b/resources/tmp.ini -p default_bucket=False,nodes_init=1,initial_index_number=1,primary_indx_type=GSI,
        bucket_size=100 -t tuqquery.ttl_with_n1ql.QueryExpirationTests.test_merge_update_ttl
        """
        expiration_time = 60 * 60
        # Loading beer-sample bucket
        result_count = 31591
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)
        bucket = self.rest.get_bucket(self.sample_bucket)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_sample_bucket)
        self.run_cbq_query(expiration_index_query)

        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_sample_bucket)
        self.run_cbq_query(primary_index_query)

        merge_update_query = '''MERGE INTO {0} AS route
                                USING {0} AS airport
                                ON route.sourceairport = airport.faa
                                WHEN MATCHED THEN
                                    UPDATE SET route.old_equipment = route.equipment,
                                        route.equipment = "12345",
                                        route.updated = true,
                                        META(route).expiration = {1}
                                WHERE airport.country = "France"
                                    AND route.airline = "BA"
                                    AND CONTAINS(route.equipment, "319")'''.format(query_sample_bucket, expiration_time)
        self.run_cbq_query(merge_update_query)

        # Validating if the expiration is set
        get_exp_query = 'SELECT META().expiration, updated, equipment FROM {0}' \
                        ' WHERE equipment = "12345"'.format(query_sample_bucket)
        results = self.run_cbq_query(get_exp_query)['results']
        count = 0
        for item in results:
            self.assertNotEqual(item['expiration'], 0, "Failed to set expiry through MERGE clause")
            self.assertTrue(item['updated'], "Doc field is not updated")
            self.assertEqual(item['equipment'], "12345", "Item value is not matching")
            count += 1

        # Changing expiration time again
        expiration_time = 10
        merge_update_query = '''MERGE INTO {0} AS route
                                USING {0} AS airport
                                ON route.sourceairport = airport.faa
                                WHEN MATCHED THEN
                                    UPDATE SET route.old_equipment = route.equipment,
                                        route.equipment = "719",
                                        route.updated = true,
                                        META(route).expiration = {1}
                                WHERE airport.country = "France"
                                    AND route.airline = "BA"
                                    AND CONTAINS(route.equipment, "12345")'''.format(query_sample_bucket,
                                                                                     expiration_time)
        self.run_cbq_query(merge_update_query)

        # Validating expiration of document
        result = self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=result_count - count)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

    def test_merge_insert_ttl(self):
        """
        @summary: Testing ttl with MERGE clause
        """
        expiration_time = 10
        # Loading beer-sample bucket
        result_count = 31591
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)
        bucket = self.rest.get_bucket(self.sample_bucket)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_sample_bucket)
        self.run_cbq_query(expiration_index_query)

        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_sample_bucket)
        self.run_cbq_query(primary_index_query)

        merge_upsert_query = '''MERGE INTO {0} AS target
                                USING [
                                    {{"iata":"DSA", "name": "Doncaster Sheffield Airport"}},
                                    {{"iata":"VLY", "name": "Anglesey Airport / Maes Awyr Mon"}}
                                ] AS source
                                ON target.faa = source.iata
                                WHEN MATCHED THEN
                                    UPDATE SET target.old_name = target.airportname,
                                             target.airportname = source.name,
                                             target.updated = true,
                                             Meta(target).expiration = Meta(target).expiration
                                WHEN NOT MATCHED THEN
                                    INSERT (KEY UUID(),
                                            VALUE {{ "faa": source.iata,
                                                    "airportname": source.name,
                                                    "type": "airport",
                                                    "inserted": true}},
                                          OPTIONS {{"expiration": {1}}})'''.format(query_sample_bucket, expiration_time)
        self.run_cbq_query(merge_upsert_query)

        # Validating Merge UPSERT query
        doc_count_query = "SELECT COUNT(*) FROM {0}".format(query_sample_bucket)
        results = self.run_cbq_query(doc_count_query)['results']
        self.assertEqual(results[0]['$1'], result_count + 1, "MERGE failed to insert new doc")
        check_update_query = "SELECT airportname, META().expiration FROM {0}" \
                             " WHERE updated = true".format(query_sample_bucket)
        results = self.run_cbq_query(check_update_query)['results'][0]
        self.assertEqual(results['expiration'], 0, "MERGE failed to retain doc expiry")

        # checking if newly inserted doc is expired
        result = self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=result_count)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

    def test_delete_with_ttl(self):
        """
        @summary: Testing delete query with docs for which expiration is set.
        """
        expiration_time = 2 * 90
        limit_count = 10

        # Loading travel-sample bucket
        result_count = 31591
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)
        bucket = self.rest.get_bucket(self.sample_bucket)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_sample_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_sample_bucket)
        self.run_cbq_query(primary_index_query)

        # Setting Expiry value for all docs
        update_expiry_query = "UPDATE {0} as b SET META(b).expiration = {1}".format(query_sample_bucket,
                                                                                    expiration_time)
        self.run_cbq_query(update_expiry_query)

        # Running a nested SELECT Query
        nested_select_query = 'SELECT count(t1.city) FROM {0} t1 WHERE t1.type = "landmark" AND t1.city IN' \
                              ' (SELECT RAW city FROM {0} WHERE type = "airport" AND' \
                              ' META().expiration > 0)'.format(query_sample_bucket)
        result = self.run_cbq_query(nested_select_query)['results'][0]['$1']
        self.assertEqual(result, 2776, "Nested SELECT query didn't return as expected")
        # Running DELETE Queries
        delete_query_with_limit = 'DELETE FROM {0} as d LIMIT {1}'.format(query_sample_bucket, limit_count)
        self.run_cbq_query(delete_query_with_limit)
        count_query = "SELECT COUNT(*) FROM {}".format(query_sample_bucket)
        count, max_retry = 0, 15
        while count < max_retry:
            results = self.run_cbq_query(count_query)['results']
            if results[0]['$1'] == result_count - 10:
                break
            count += 1
        self.assertEqual(results[0]['$1'], result_count - 10, "DELETE failed to delete docs")
        result =self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                              stat_name=self.index_stat, max_try=50, expected_stat_value=result_count - 10)
        self.assertTrue(result, "Indexer still has indexes for expired docs")
        result_count = result_count - 10

        update_expiry_query = "UPDATE {0} as b SET META(b).expiration = {1}" \
                              " WHERE type = 'airline'".format(query_sample_bucket, 10)
        self.run_cbq_query(update_expiry_query)
        doc_updated_query = "SELECT COUNT(*) FROM {0} as b where META(b).expiration > 0 and "\
                            "type = 'airline'".format(query_sample_bucket)
        num_of_doc_updated = self.run_cbq_query(doc_updated_query)['results'][0]['$1']
        delete_on_expiry_query = "DELETE FROM {0} as b WHERE META(b).expiration > 0 and" \
                                 " type = 'airline'".format(query_sample_bucket)
        self.run_cbq_query(delete_on_expiry_query)
        result = self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                               stat_name=self.index_stat,
                                               expected_stat_value=result_count - num_of_doc_updated)
        self.assertTrue(result, "Indexer still has indexes for expired docs")
        result_count = result_count - num_of_doc_updated

        # Delete with Nested Select
        # Below query will result in deletion of 8 documents
        nested_delete_query = """
                                DELETE FROM {0} as t 
                                WHERE META(t).id IN (
                                                    SELECT RAW META(d).id FROM {0} as d
                                                    WHERE d.type = 'hotel' AND d.reviews IS NOT null
                                                        AND ANY hotel_review in d.reviews 
                                                                satisfies hotel_review.ratings.Overall = 3.0
                                                                    AND hotel_review.ratings.Location = 1 END)
                                                                    """.format(query_sample_bucket)
        self.run_cbq_query(nested_delete_query)
        result = self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                      stat_name=self.index_stat, max_try= 50, expected_stat_value=result_count - 8)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

        # Waiting for all docs to expire
        self.log.info("Checking if all docs has expired")
        result = self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=0, sleep_time=10)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

    def test_ttl_update_for_user_with_query_update_permission(self):
        """
        @summary: Update doc expiration from a user who doesn't have Query delete permission
        """
        cluster = Cluster('couchbase://{0}'.format(self.master.ip))
        authenticator = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        cluster.authenticate(authenticator)

        # Loading travel-sample bucket
        result_count = 7303
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: 'healthy'}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: result_count}, 5, 120)
        self._wait_for_index_online(self.sample_bucket, self.sample_bucket_index)
        bucket = self.rest.get_bucket(self.sample_bucket)
        self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        query_sample_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))

        # creating index on meta.expiration and a primary index
        expiration_index_query = "CREATE INDEX {0} ON {1} ( META().expiration )".format(self.exp_index,
                                                                                        query_sample_bucket)
        self.run_cbq_query(expiration_index_query)
        primary_index_query = "CREATE PRIMARY INDEX idx on {0} USING GSI".format(query_sample_bucket)
        self.run_cbq_query(primary_index_query)

        # creating a user with Query Update rbac permission only
        payload = "name=test&roles=query_select[{0}],query_update[{0}]&password=password".format(self.sample_bucket)
        self.rest.add_set_builtin_user("test", payload)

        # updating expiration using test user
        delete_query = "DELETE FROM {0}".format(query_sample_bucket)
        try:
            self.run_cbq_query(delete_query, username="test", password="password")
        except Exception as err:
            result = "User does not have credentials to run DELETE queries" in str(err)
            self.assertTrue(result, "DELETE Query failed for non-credential reasons. Check error msg.")
            self.log.info(str(err))
        update_expiry_query = "UPDATE {0} SET META().expiration = 2".format(query_sample_bucket)
        self.run_cbq_query(update_expiry_query, username="test", password="password")
        result = self._is_expected_index_count(bucket_name=self.sample_bucket, idx_name=self.exp_index,
                                               stat_name=self.index_stat, expected_stat_value=0)
        self.assertTrue(result, "Indexer still has indexes for expired docs")

    def _is_expected_kv_expired_stats(self, bucket='default', expected_val=None, max_retry=60, sleep_time=2):
        """
        @summary: This method returns no. of doc expired as reflected in KV stats
        """
        nodes = self.get_kv_nodes(self.servers, self.master)
        active_expired_stats = 0
        count = 0
        while count < max_retry:
            for node in nodes:
                shell = RemoteMachineShellConnection(node)
                output, error = shell.execute_cbstats(bucket, 'all', print_results=False)
                if not error:
                    for stat in output:
                        if 'vb_active_expired' in stat:
                            value = stat.split(':')
                            active_expired_stats += int(value[-1].strip())
            if active_expired_stats == expected_val:
                self.log.info("Expected: {0}, Actual: {1}".format(expected_val, active_expired_stats))
                return True
            self.sleep(sleep_time, "Waiting before rechecking if stat matches expected value")
            count += 1
        self.log.info("Expected: {0}, Actual: {1}".format(expected_val, active_expired_stats))
        return False

    def _is_expected_index_count(self, bucket_name, idx_name, stat_name, expected_stat_value,
                                 index_node=None, max_try=60, sleep_time=2):
        if not index_node:
            index_node = self.master
        rest = RestConnection(index_node)
        count = 0
        curr_stat_value = None
        while count < max_try:
            self.run_cbq_query('select * from `{0}`'.format(bucket_name))
            curr_stat_value = rest.get_index_stats()[bucket_name][idx_name][stat_name]
            if curr_stat_value == expected_stat_value:
                self.log.info("Expected: {0}, Actual: {1}".format(expected_stat_value, curr_stat_value))
                return True
            self.sleep(sleep_time, f"Wait before rechecking if index[{idx_name}] stat[{stat_name}] current [{curr_stat_value}] matches expected [{expected_stat_value}]")
            count += 1
        self.log.info("Expected: {0}, Actual: {1}".format(expected_stat_value, curr_stat_value))
        return False
