
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff

class QueryAdviseTests(QueryTests):

    def setUp(self):
        super(QueryAdviseTests, self).setUp()
        self.log.info("==============  QuerySanityTests setup has started ==============")
        self.index_to_be_created = self.input.param("index_to_be_created", '')
        if self.load_sample:
            self.rest.load_sample("travel-sample")
            init_time = time.time()
            while True:
                next_time = time.time()
                query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + self.bucket_name + "`")
                if query_response['results'][0]['$1'] == 31591:
                    break
                if next_time - init_time > 600:
                    break
                time.sleep(1)

            self.wait_for_all_indexes_online()
            list_of_indexes = self.run_cbq_query(query="select raw name from system:indexes WHERE indexes.bucket_id is missing")

            for index in list_of_indexes['results']:
                if index == "#primary":
                    continue
                else:
                    self.run_cbq_query(query="drop index `travel-sample`.`%s`" % index)

        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAdviseTests, self).suite_setUp()
        self.log.info("==============  QuerySanityTests suite_setup has started ==============")
        self.log.info("==============  QuerySanityTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QuerySanityTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QuerySanityTests tearDown has completed ==============")
        super(QueryAdviseTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QuerySanityTests suite_tearDown has started ==============")
        self.log.info("==============  QuerySanityTests suite_tearDown has completed ==============")
        super(QueryAdviseTests, self).suite_tearDown()

    def get_index_statements(self, advise_results):
        indexes = []
        for index_type in advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes'].keys():
            for index in advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes'][index_type]:
                indexes.append(index['index_statement'])
        return indexes
    # Use advise statement on an update statement
    def test_update_advise(self):
        try:
            results_simple = self.run_cbq_query(query="advise UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
            try:
                results_with_advise_index = self.run_cbq_query(query="UPDATE `{0}` SET city = 'SF' WHERE lower(city) = 'san francisco'".format(self.bucket_name), server=self.master)
                self.assertEqual(results_with_advise_index['status'], 'success')
                self.assertEqual(results_with_advise_index['metrics']['mutationCount'], 938)
            finally:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name,index_name))

    # Use advise on an update statement but the bucket doesn't exist
    def test_update_fake(self):
        try:
            results_fake_field = self.run_cbq_query(query="advise UPDATE `fakebucket` SET fakey_field = 'fake' WHERE fakey_field = 'faker'", server=self.master)
            fake_field_indexes = self.get_index_statements(results_fake_field)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        self.assertTrue(len(fake_field_indexes) > 0, "No advise generated for an index with no created bucket!")

    def test_delete_advise(self):
        try:
            results_simple = self.run_cbq_query(query="advise DELETE FROM `{0}` WHERE country = 'France'".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
            try:
                results_with_advise_index = self.run_cbq_query(query="DELETE FROM `{0}` WHERE country = 'France'".format(self.bucket_name), server=self.master)
                self.assertEqual(results_with_advise_index['status'], 'success')
                self.assertEqual(results_with_advise_index['metrics']['mutationCount'], 770)
            finally:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name, index_name))

    def test_delete_fake(self):
        try:
            results_fake_field = self.run_cbq_query(query="advise DELETE FROM `fake_bucket` WHERE fake_field = 'Fake'", server=self.master)
            fake_field_indexes = self.get_index_statements(results_fake_field)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        self.assertTrue(len(fake_field_indexes) > 0, "No advise generated for an index with no created bucket!")

    def test_delete(self):
        try:
            results_simple = self.run_cbq_query(query="advise DELETE FROM `{0}` WHERE ((free_breakfast=true OR city is null)) OR (id <= '549')".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
        try:
            results_with_advise_index = self.run_cbq_query(query="DELETE FROM `{0}` WHERE ((free_breakfast=true OR city is null)) OR (id <= '549')".format(self.bucket_name), server=self.master)
            self.assertEqual(results_with_advise_index['status'], 'success')
            self.assertEqual(results_with_advise_index['metrics']['mutationCount'], 31591)
        finally:
            for index in simple_indexes:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name, index_name))

    def test_advise_index(self):
        try:
            results_update = self.run_cbq_query(query="advise index UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_update)
            self.assertTrue(len(simple_indexes) > 0, "No advise generated for an index with no created bucket!")

            results_select = self.run_cbq_query(query="advise index SELECT city FROM `{0}` WHERE lower(city) = 'sanfrancisco'".format(self.bucket_name), server=self.master)
            select_indexes = self.get_index_statements(results_select)
            self.assertTrue(len(simple_indexes) > 0, "No advise generated for an index with no created bucket!")

        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()

    def test_advise_merge_fake(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="ADVISE MERGE INTO shellTest t USING [ {'id':'21728', 'vacancy': true} , {'id':'21730', 'vacancy': true} ] s ON t.id || 123 = s.id WHEN MATCHED THEN UPDATE SET t.old_vacancy = t.vacancy, t.vacancy = s.vacancy RETURNING meta(t).id",
                server=self.master)
            fake_field_indexes = self.get_index_statements(results_fake_field)
            for fake_indexes in fake_field_indexes:
                self.assertTrue('`id`||123' in fake_indexes, "Index generated does not contain the correct fields: {0}".format(fake_indexes))
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        self.assertTrue(len(fake_field_indexes) > 0, "No advise generated for an index with no created bucket!")

    def test_meta_cas(self):
        try:
            results_simple = self.run_cbq_query(query="advise select * from `{0}` t where meta(t).cas > 10 limit 100".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
            try:
                results_with_advise_index = self.run_cbq_query(
                    query="select * from `{0}` t where meta(t).cas > 10 limit 100".format(self.bucket_name))
                self.assertEqual(results_with_advise_index['status'], 'success')
                self.assertEqual(results_with_advise_index['metrics']['resultCount'], 100)
            finally:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name, index_name))

    def test_meta_expiration(self):
        try:
            results_simple = self.run_cbq_query(query="advise select * from `{0}` t where meta(t).expiration = 0 limit 100".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
            try:
                results_with_advise_index = self.run_cbq_query(
                    query="select * from `{0}` t where meta(t).expiration = 0 limit 100".format(self.bucket_name))
                self.assertEqual(results_with_advise_index['status'], 'success')
                self.assertEqual(results_with_advise_index['metrics']['resultCount'], 100)
            finally:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name, index_name))

    def test_meta_expiration_cas_and(self):
        try:
            results_simple = self.run_cbq_query(query="advise select meta().cas, meta().expiration from `{0}` t where meta(t).expiration = 0  and meta(t).cas > 10 limit 100".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
            try:
                results_with_advise_index = self.run_cbq_query(
                    query="select meta().cas, meta().expiration from `{0}` t where meta(t).expiration = 0 and meta(t).cas > 10 limit 100".format(self.bucket_name))
                self.assertEqual(results_with_advise_index['status'], 'success')
                self.assertEqual(results_with_advise_index['metrics']['resultCount'], 100)
            finally:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name, index_name))

    def test_meta_expiration_cas_or(self):
        try:
            results_simple = self.run_cbq_query(query="advise select meta().cas, meta().expiration from `{0}` t where meta(t).expiration = 1 OR meta(t).cas > 10 limit 100".format(self.bucket_name), server=self.master)
            simple_indexes = self.get_index_statements(results_simple)
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
        try:
            results_with_advise_index = self.run_cbq_query(
                query="select meta().cas, meta().expiration from `{0}` t where meta(t).expiration = 1 OR meta(t).cas > 10 limit 100".format(
                    self.bucket_name))
            self.assertEqual(results_with_advise_index['status'], 'success')
            self.assertEqual(results_with_advise_index['metrics']['resultCount'], 100)
        finally:
            for index in simple_indexes:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name, index_name))

    # Negative test, cannot advise on a query with a syntax error
    def test_syntax_error(self):
        try:
            self.run_cbq_query(
                query="ADVISE select * from `{0}` wear x = y".format(self.bucket_name),server=self.master)
            self.log.error("Advise statement contains syntax error, should've thrown an error")
            self.fail()
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))

    # Test to make sure advise recognizes that the current indexes are sufficient
    def test_optimal_index(self):
        self.run_cbq_query(query="CREATE INDEX idx on `{0}`(type)".format(self.bucket_name), server=self.master)
        self.wait_for_all_indexes_online()
        try:
            results_field = self.run_cbq_query(
                query="ADVISE select * from `{0}` where type = 'hotel'".format(self.bucket_name), server=self.master)
            self.assertEqual(results_field['results'][0]['advice']['adviseinfo']['current_indexes'][0]['index_status'], 'SAME AS THE INDEX WE CAN RECOMMEND')
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()
        finally:
            self.run_cbq_query("DROP INDEX `{0}`.idx".format(self.bucket_name))

    # Test to see if advise will recommend a better single index, instead of two
    def test_suboptimal_indexes(self):
        self.run_cbq_query(query="CREATE INDEX idx on `{0}`(type)".format(self.bucket_name), server=self.master)
        self.run_cbq_query(query="CREATE INDEX idx2 on `{0}`(callsign)".format(self.bucket_name), server=self.master)
        self.wait_for_all_indexes_online()
        try:
            results_field = self.run_cbq_query(
                query="ADVISE select type, callsign from `{0}` where type = 'airline' and callsign = 'MILE-AIR'".format(self.bucket_name), server=self.master)
            self.assertEqual(results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], "CREATE INDEX adv_callsign_type ON `{0}`(`callsign`) WHERE `type` = 'airline'".format(self.bucket_name))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()
        finally:
            self.run_cbq_query("DROP INDEX `{0}`.idx".format(self.bucket_name))
            self.run_cbq_query("DROP INDEX `{0}`.idx2".format(self.bucket_name))

    # Test an advise on a skip range scan
    def test_skip_range_key(self):
        self.run_cbq_query(query="CREATE INDEX idx on `{0}`(a,b,d,e)".format(self.bucket_name), server=self.master)
        self.wait_for_all_indexes_online()
        try:
            results_field = self.run_cbq_query(
                query="ADVISE select a,d from `{0}` where a>0 and d>0".format(self.bucket_name), server=self.master)
            self.assertTrue('THIS IS AN OPTIMAL COVERING INDEX' in results_field['results'][0]['advice']['adviseinfo']['current_indexes'][0]['index_status'], "Index should've been optimal: {0}".format(results_field['results'][0]['advice']['adviseinfo']['current_indexes'][0]['index_status']))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()
        finally:
            self.run_cbq_query("DROP INDEX `{0}`.idx".format(self.bucket_name))

    # Test an advise on a query with aggregates in it
    def test_aggregate_query(self):
        try:
            results_field = self.run_cbq_query(
                query="advise select sum(a), min(d) from `{0}` where a > 0 and d >0".format(self.bucket_name), server=self.master)
            self.assertTrue(results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == 'CREATE INDEX adv_d_a ON `{0}`(`d`,`a`)'.format(self.bucket_name) or results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == 'CREATE INDEX adv_a_d ON `{0}`(`a`,`d`)'.format(self.bucket_name))
            self.assertEqual(results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_property'], 'FULL GROUPBY & AGGREGATES pushdown')
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # Test an advise on a subquery
    def test_advise_subquery(self):
        try:
            results_field = self.run_cbq_query(
                query="advise select t3.type from (select t2.type from `{0}` t2 where t2.type = 'airline') t3".format(self.bucket_name), server=self.master)
            self.assertEqual(results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], 'CREATE INDEX adv_type ON `{0}`(`type`)'.format(self.bucket_name))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # Test a join that just requires one index
    def test_advise_join(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="advise select t1.type from `{0}` t1 INNER JOIN `{0}` t2 ON (t1.type = t2.type) limit 10".format(self.bucket_name), server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], 'CREATE INDEX adv_type ON `{0}`(`type`)'.format(self.bucket_name))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # Test a join that requires multiple indexes
    def test_advise_join_multiple_indexes(self):
        self.run_cbq_query(query="CREATE INDEX idx on `{0}`(type)".format(self.bucket_name), server=self.master)
        self.wait_for_all_indexes_online()
        try:
            results_fake_field = self.run_cbq_query(
                query="advise select t1.type from `{0}` t1 INNER JOIN `{0}` t2 ON (t1.type = t2.type) INNER JOIN `fake_bucket` t3 ON (t2.callsign = t3.callsign) limit 10".format(self.bucket_name), server=self.master)
            self.assertTrue( 'CREATE INDEX adv_type_callsign ON `{0}`(`type`,`callsign`)'.format(self.bucket_name) in str(results_fake_field), "The index is not found in the advise: {0}".format(str(results_fake_field)))
            self.assertTrue('CREATE INDEX adv_callsign ON `fake_bucket`(`callsign`)' in str(results_fake_field), "The index is not found in the advise: {0}".format(str(results_fake_field)))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()
        finally:
            self.run_cbq_query(query="DROP INDEX`{0}`.idx".format(self.bucket_name), server=self.master)

    # Should not get an index recommendation for system:keyspaces
    def test_advise_keyspace(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="advise select * from system:keyspaces".format(self.bucket_name), server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'], 'No index recommendation at this time.')
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # There is a new field called keyspace alias, we should look to see if it exists
    def test_advise_alias(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="advise select * from `{0}` t where t.type = 'airline'".format(self.bucket_name), server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['keyspace_alias'], 'travel-sample_t')
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # This query used to not be properly covered, now it should recommend a covering index
    def test_join_covering(self):
        field_list = ['`primary_key_id`','`int_field1`','`bool_field1`','`decimal_field1`']
        try:
            results_fake_field = self.run_cbq_query(
                query="advise SELECT t_1.int_field1 , t_1.decimal_field1 , t_1.primary_key_id , t_1.bool_field1 FROM bucket_01 t_1 INNER JOIN bucket_04 t_4 ON ( t_1.primary_key_id = t_4.primary_key_id ) INNER JOIN bucket_04 t_5 ON ( t_1.primary_key_id = t_5.primary_key_id );", server=self.master)
            for field in field_list:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'].format(field))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # This query used to throw an error, now it should provide the correct advise
    def test_advise_no_error(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="advise SELECT lang, SUM(purchaseLittle) as totalLittle, SUM(purchaseGreat) AS totalGreat, SUM(purchaseSome) AS totalSome, SUM(purchaseGreat) / SUM(purchaseLittle) AS difference FROM default s UNNEST SPLIT(s.LanguageWorkedWith, ';') lang LET purchaseLittle = CASE WHEN s.PurchaseWhat LIKE 'I have little%' THEN 1 ELSE 0 END, purchaseGreat = CASE WHEN s.PurchaseWhat LIKE 'I have a great%' THEN 1 ELSE 0 END, purchaseSome = CASE WHEN s.PurchaseWhat LIKE 'I have some%' THEN 1 ELSE 0 END WHERE s.PurchaseWhat != 'NA' AND lang != 'NA' group by lang order by SUM(purchaseGreat) / SUM(purchaseLittle) desc", server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], "CREATE INDEX adv_ALL_split_s_LanguageWorkedWith_PurchaseWhat ON `default`(ALL split((`LanguageWorkedWith`), ';'),`PurchaseWhat`)")
            results_fake_field = self.run_cbq_query(
                query='advise SELECT RAW email FROM sample_data data UNNEST data.`identity`.`Contact`.`Emails` email WHERE (data.`type`="links") AND (data.`owner`="AA") AND email LIKE $pfx GROUP BY email HAVING COUNT(meta(data).id) > 20', server=self.master)
            self.assertTrue(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_ALL_identity_Contact_Emails_type_owner ON `sample_data`(ALL ((`identity`).`Contact`).`Emails`,`type`,`owner`)" or results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_ALL_identity_Contact_Emails_owner_type ON `sample_data`(ALL ((`identity`).`Contact`).`Emails`,`owner`,`type`)")
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_param_advise(self):
        try:
            results_fake_field = self.run_cbq_query(query="advise select name, pro_account from default where country=$1 and name is not null order by country desc", server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], "CREATE INDEX adv_countryDESC_name_pro_account ON `default`(`country` DESC,`name`,`pro_account`)")
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_property'], "ORDER pushdown")
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_is_missing(self):
        try:
            results_fake_field = self.run_cbq_query(query="advise SELECT name, val FROM bucket1 where type = 'configurations' and (components in ['x'] or components is missing)", server=self.master)
            self.assertTrue(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_type_components_name_val ON `bucket1`(`type`,`components`,`name`,`val`)" or results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_type_components_val_name ON `bucket1`(`type`,`components`,`val`,`name`)")
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    # Bugged test case, need to revisit when MB-39786 is fixed
    def test_no_dup_keys(self):
        try:
            results_fake_field = self.run_cbq_query(query="ADVISE SELECT a, b, c, sum(d), avg(d) FROM tutorial t WHERE t.d = 10 and t.b > 20 and t.c < 30 and t.d between 40 and 50 and (ANY x in t.p satisfies x = 100 end) GROUP BY a, b, c ORDER BY a, b DESC LIMIT 1 OFFSET 1", server=self.master)
            self.assertTrue(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_type_components_name_val ON `bucket1`(`type`,`components`,`name`,`val`)" or results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_type_components_val_name ON `bucket1`(`type`,`components`,`val`,`name`)")
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_unnest(self):
        try:
            results_fake_field = self.run_cbq_query(query="advise SELECT * FROM product UNNEST product.categories AS cat WHERE LOWER(cat) = 'golf' LIMIT 10 OFFSET 10", server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], "CREATE INDEX adv_ALL_categories_lower_cat ON `product`(ALL ARRAY lower(`cat`) FOR cat IN `categories` END)")
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_merge_bug(self):
        try:
            results_fake_field = self.run_cbq_query(query='advise merge into shellTest a2 using shellTest a1 on a1.c12=a2.c22 and a1.test_id = "advise" when matched then update set a2.type = "matched";', server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], "CREATE INDEX adv_test_id ON `shellTest`(`test_id`)")
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][1]['index_statement'], "CREATE INDEX adv_c22 ON `shellTest`(`c22`)")

        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_array_func(self):
        field_list =['`roomType`','DISTINCT ARRAY [`s`.`level`, `s`.`size`, `s`.`num`] FOR `s` IN `rooms` END','`guestCode`','`startTime`','`endTime`','array_distinct(ifmissing((array_star(`rooms`).`num`), []))']
        try:
            results_fake_field = self.run_cbq_query(query="advise SELECT META(p).id, ARRAY_DISTINCT(IFMISSING(rooms[*].num,[])) FROM `travel-sample` AS p WHERE type = 'hotel' AND (guestCode = IFNULL($guestCode, '') OR guestCode = '') AND (roomType = IFNULL($roomType,'R') OR roomType = IFNULL($roomType,'D') ) AND ($checkinTime BETWEEN startTime AND endTime) AND (ANY s IN rooms SATISFIES [s.`level`,s.size, s.num] = [$level, $size, $num] END)", server=self.master)
            self.log.info("Advised index is {0}".format(results_fake_field))
            for field in field_list:
                self.assertTrue(field in results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], "The field is missing from the recommended index: {0}, Index advised: {1}".format(field,results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement']))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_is_missing(self):
        try:
            results_fake_field = self.run_cbq_query(query="advise select sum(d) from default where a = 10 and b is missing group by c", server=self.master)
            self.assertEqual(results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], "CREATE INDEX adv_a_b_c_d ON `default`(`a`,`b`,`c`,`d`)")
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_user_generated_advise(self):
        field_list =['`status`','`type`','DISTINCT ARRAY `v`.`key` FOR `v` IN `operationContext` END','`userRef`.`name`','`startTime`']
        try:
            results_fake_field = self.run_cbq_query(query='ADVISE SELECT result.* FROM (SELECT meta().id, startTime FROM `processMonitoring` WHERE type = "ProcessMonitorDTO"  AND status = "Running" AND userRef.name LIKE "fir%" AND ANY v IN operationContext SATISFIES v.`key` IN ["SITE_ID::ISite_100","SITE_ID::ISite_101"] END INTERSECT SELECT meta().id, startTime FROM `processMonitoring`  WHERE type = "ProcessMonitorDTO"  AND status = "Running" AND userRef.name LIKE "fir%" AND ANY v IN operationContext SATISFIES v.`key` ="FWAId::IFrameworkAgreement_100" END) AS result ORDER BY startTime ASC LIMIT 10 OFFSET 0', server=self.master)
            for field in field_list:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'covering_indexes'][0]['index_statement'],
                                "The field is missing from the recommended index: {0}".format(field))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_user_generated_advise_2(self):
        field_list1 =['`type`','`type_`','`effectiveDate`','`policy`.`id`']
        field_list2 =['`type_`','`policy`.`id`','to_number(`loadingPremium`)','to_number(`installmentPremium`)']
        try:
            results_fake_field = self.run_cbq_query(query="ADVISE select q1.PolID as PolicyID, SUM(((q1.POLoadPrem) + (q1.POInstPrem) + (q1.PCOLoadPrem) + (q1.PCOInstPrem)) * (q1.FreqValue)) as TotalPremium from (select meta(pol).id as PolID, TO_NUMBER(pol.paymentOption.frequency) as Freq, TO_NUMBER(po.installmentPremium) as POInstPrem, TO_NUMBER(po.loadingPremium) as POLoadPrem, TO_NUMBER(pco.installmentPremium) as PCOInstPrem, TO_NUMBER(pco.loadingPremium) as PCOLoadPrem, CASE WHEN pol.paymentOption.frequency == 'ANNUAL' THEN 1 WHEN pol.paymentOption.frequency == 'SEMI-ANNUAL' THEN 2 WHEN pol.paymentOption.frequency == 'QUARTERLY' THEN 4 WHEN pol.paymentOption.frequency == 'MONTHLY' THEN 12 WHEN pol.paymentOption.frequency == 'SINGLE' THEN 0.1 END AS FreqValue from data td join data pol on td.policy.id = meta(pol).id join data po on td.policy.id = po.policy.id join data pco on td.policy.id = pco.policy.id where td.type_ = 'TransactionDetail' and pol.type_ = 'Policy' and po.type_ = 'ProductOption' and pco.type_ = 'ProductComponentOption' and td.type = 'NEWCONTRACTPROPOSAL' and td.effectiveDate = '2019-12-13' and pol.status = 'INFORCE' ) q1 group by q1.PolID", server=self.master)
            self.log.info("Advised index is {0}".format(results_fake_field))
            for field in field_list1:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'],
                                "The field is missing from the recommended index: {0} , Index Advised {1}".format(field,results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement']))
            for field in field_list2:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][1]['index_statement'],
                                "The field is missing from the recommended index: {0}, Index Advised {1}".format(field, results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][1]['index_statement']))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_user_generated_advise_3(self):
        field_list1 =['ALL `details`','`type`','`storeId`','lower((`drawer`.`name`))']
        field_list2 =['ALL ARRAY lower((`currencyDetails`.`currencyCode`)) FOR currencyDetails IN `details` END','`type`','`storeId`']
        field_list3 =['ALL `details`','`type`','`storeId`','lower((`transactionUser`.`name`))']
        field_list4 =['ALL `details`','`type`','`storeId`']
        try:
            results_fake_field = self.run_cbq_query(query="ADVISE SELECT parent.id, parent.createdDate, parent.drawer.name, parent.transactionUser.name as userName, currencyDetails.currencyCode, currencyDetails.totalValue, store[0].name AS storeName FROM TransactionManager AS parent UNNEST parent.details AS currencyDetails LET store = ARRAY node FOR node IN parent.nodeStructure WHEN node.nodeType= 'STORE' END WHERE parent.type='PICK_UP' AND (LOWER(parent.drawer.name) LIKE 'sal%' OR LOWER(currencyDetails.currencyCode) LIKE 'sal%' OR LOWER(parent.transactionUser.name) LIKE 'sal%' OR ARRAY  node FOR node IN store WHEN node.nodeType='STORE' AND LOWER(node.name) LIKE 'sal%'  END) AND parent.storeId IN ['S::1','S::2','S::3','S::4','S::5','S::6','S::7','S::31','NODE::7070483e-f2c8-4b33-9360-eeec26e38a38','NODE::7070483e-f2c8-4b33-9360-eeec26e38a39']", server=self.master)
            self.log.info("Advised index is {0}".format(results_fake_field))
            for field in field_list1:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'],
                                "The field is missing from the recommended index: {0}, Index Advised {1}".format(field, results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement']))
            for field in field_list2:
                self.assertTrue(field in                                
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][1]['index_statement'],
                                "The field is missing from the recommended index: {0}, Index Advised {1}".format(field, results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][1]['index_statement']))
            for field in field_list3:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][0]['index_statement'],
                                "The field is missing from the recommended index: {0}, Index Advised {1}".format(field, results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][0]['index_statement']))

            for field in field_list4:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][0]['index_statement'],
                                "The field is missing from the recommended index: {0}, Index Advised {1}".format(field, results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][0]['index_statement']))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_user_generated_advise_4(self):
        field_list1 =['ALL `orderItemsId`','`type`','`toGoDetails`.`desiredDate`','`hasBeenPrinted`']
        try:
            results_fake_field = self.run_cbq_query(query='ADVISE SELECT RAW SUM(oi.quantity) FROM `ipratico_store` AS o UNNEST o.orderItemsId AS oiId JOIN `ipratico_store` AS oi ON KEYS oiId JOIN `ipratico_store` AS p ON KEYS oi.saleItem.productId WHERE o.type = "order" AND SOME c IN o.channels SATISFIES c = "lct_123" END AND o.toGoDetails.desiredDate >= "2020-01-01" AND o.toGoDetails.desiredDate < "2020-01-05" AND o.closedDate IS NOT VALUED AND (o.hasBeenPrinted = false OR o.hasBeenPrinted IS NOT VALUED) AND p.productCategoryId = "product_category:5f0d574d-38be-4fd7-bf75-05bd4d946268"', server=self.master)
            for field in field_list1:
                self.assertTrue(field in
                                results_fake_field['results'][0]['advice']['adviseinfo']['recommended_indexes'][
                                    'indexes'][0]['index_statement'],
                                "The field is missing from the recommended index: {0}".format(field))
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_advice_collections(self):
        try:
            results_field = self.run_cbq_query(
                query="ADVISE SELECT * FROM default:default.test.test1 b WHERE b.name = 'new hotel'", server=self.master)
            self.assertEqual(results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], 'CREATE INDEX adv_name ON `default`:`default`.`test`.`test1`(`name`)')
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_advice_collections_fake(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="ADVISE SELECT * FROM default:default.fakescope.fakecollection b WHERE b.name = 'new hotel'",
                server=self.master)
            fake_field_indexes = self.get_index_statements(results_fake_field)
            for index in fake_field_indexes:
                self.assertEqual(index, 'CREATE INDEX adv_name ON `default`:`default`.`fakescope`.`fakecollection`(`name`)')
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        self.assertTrue(len(fake_field_indexes) > 0, "No advise generated for an index with no created bucket!")

    def test_advice_collections_query_context(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="ADVISE SELECT * FROM fakecollection b WHERE b.name = 'new hotel'",
                server=self.master, query_context='default:default.fakescope')
            fake_field_indexes = self.get_index_statements(results_fake_field)
            for index in fake_field_indexes:
                self.assertEqual(index, 'CREATE INDEX adv_name ON `default`:`default`.`fakescope`.`fakecollection`(`name`)')
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        self.assertTrue(len(fake_field_indexes) > 0, "No advise generated for an index with no created bucket!")

    def test_advise_merge_fake_collections(self):
        try:
            results_fake_field = self.run_cbq_query(
                query="ADVISE MERGE INTO default:default.fake.fakecollection  USING [ {'id':'21728', 'vacancy': true} , {'id':'21730', 'vacancy': true} ] s ON fakecollection.id || 123 = s.id WHEN MATCHED THEN UPDATE SET fakecollection.old_vacancy = fakecollection.vacancy, fakecollection.vacancy = s.vacancy RETURNING meta(fakecollection).id",
                server=self.master)
            fake_field_indexes = self.get_index_statements(results_fake_field)
            for fake_indexes in fake_field_indexes:
                self.assertTrue("`default`:`default`.`fake`.`fakecollection`" in fake_indexes and '`id`||123' in fake_indexes, "Index generated does not contain the correct fields: {0}".format(fake_indexes))
        except Exception as e:
            self.log.error("Advise statement failed: {0}".format(e))
            self.fail()
        self.assertTrue(len(fake_field_indexes) > 0, "No advise generated for an index with no created bucket!")

    # Test an advise on a subquery
    def test_advise_subquery_collections(self):
        try:
            results_field = self.run_cbq_query(
                query="advise select t3.type from (select t2.type from fakecollection t2 where t2.type = 'airline') t3".format(self.bucket_name), query_context="default:default.fakescope", server=self.master)
            self.assertEqual(results_field['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], 'CREATE INDEX adv_type ON `default`:`default`.`fakescope`.`fakecollection`(`type`)')
        except Exception as e:
            self.log.info("Advise statement failed: {0}".format(e))
            self.fail()

    def test_advise_context_join(self):
        results = self.run_cbq_query(query='advise select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"', query_context='default:default.test2')
        for result in results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes']:
            self.assertTrue('CREATE INDEX adv_name ON `default`:`default`.`test2`.`test2`(`name`)' in str(result) or 'CREATE INDEX adv_name ON `default`:`default`.`test`.`test1`(`name`)' in str(result), "Correct index not being recommended {0}".format(result))
        results2 = self.run_cbq_query(query='advise select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"', query_context='default:default.test')
        for result in results2['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes']:
            self.assertTrue("CREATE INDEX adv_name ON `default`:`default`.`test`.`test2`(`name`)" in str(result) or "CREATE INDEX adv_name ON `default`:`default`.`test`.`test1`(`name`)" in str(result), "Correct index not being recommended {0}".format(result))

    def test_advise_join_full_path(self):
        results = self.run_cbq_query(
            query='advise select * from default:default.test.test1 t1 INNER JOIN default:default.test2.test2 t2 ON t1.name = t2.name where t1.name = "new hotel"')
        for result in results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes']:
            self.assertTrue('CREATE INDEX adv_name ON `default`:`default`.`test2`.`test2`(`name`)' in str(
                result) or 'CREATE INDEX adv_name ON `default`:`default`.`test`.`test1`(`name`)' in str(result),
                            "Correct index not being recommended {0}".format(result))

    def test_advise_context_switch(self):
        results = self.run_cbq_query(query='advise select * from test1 where name = "new hotel"', query_context='default:default.test')
        self.assertEqual(results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], 'CREATE INDEX adv_name ON `default`:`default`.`test`.`test1`(`name`)')
        results = self.run_cbq_query(query='advise select * from test1 where name = "new hotel"', query_context='default:default.test2')
        self.assertEqual(results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], 'CREATE INDEX adv_name ON `default`:`default`.`test2`.`test1`(`name`)')

    def test_advise_set_operator(self):
        SETOPERATORS = ["UNION", "INTERSECT", "EXCEPT","UNION ALL", "INTERSECT ALL", "EXCEPT ALL"]
        for setop in SETOPERATORS:
            query = f"ADVISE SELECT col1, col2 FROM t WHERE a = 1 AND b = 2 {setop} SELECT col1, col2 FROM t WHERE c = 3 AND d = 4"
            results = self.run_cbq_query(query=query)
            indexes = self.get_index_statements(results)
            self.assertTrue("CREATE INDEX adv_a_b ON `t`(`a`,`b`)" in indexes or "CREATE INDEX adv_b_a ON `t`(`b`,`a`)" in indexes)
            self.assertTrue("CREATE INDEX adv_c_d ON `t`(`c`,`d`)" in indexes or "CREATE INDEX adv_d_c ON `t`(`d`,`c`)" in indexes)

    def test_advise_in_in(self):
        query = "SELECT 1 FROM default WHERE ANY a IN a IN aa SATISFIES true END"
        result = self.run_cbq_query(f'ADVISE {query}')
        self.log.info(f"Advise result is: {result['results']}")
        self.assertTrue(result['results'][0]['query'], query)

    def test_advise_regexp_contains(self):
        # MB-63577
        advise_query = 'ADVISE SELECT meta().id FROM default WHERE type = "xx" AND (REGEXP_CONTAINS (c1, "^[0-9;]*$") OR  c1 IS MISSING  OR  c1 is null)'
        result = self.run_cbq_query(advise_query)
        self.log.info(f"Advise result is: {result['results']}")

    def test_advise_nested_subquery(self):
        query = "SELECT (SELECT 3 ) AS a3, (SELECT 1 AS a21, (SELECT 2 AS a22 ) AS a22 ) AS a2"
        result = self.run_cbq_query(f'ADVISE {query}')
        self.log.info(f"Advise result is: {result['results']}")

        subqueries = ["select 3", "select 2 as `a22`", "select 1 as `a21`, (select 2 as `a22`) as `a22`"]
        # check main query
        self.assertEqual(query, result['results'][0]['query'], f"Main query {query} not found")
        # check nested subqueries
        for subquery in result['results'][0]['~subqueries']:
            self.assertTrue(subquery['subquery'] in subqueries, f"Subquery {subquery} not found")

    def test_advise_covered_index_join(self):
        # create mb66635 collection
        self.run_cbq_query('CREATE COLLECTION default._default.mb66635 IF NOT EXISTS')
        insert1 = 'UPSERT INTO mb66635 (KEY,VALUE) VALUES("test11_advise", {"c11": 1, "c12": 10, "a11": [ 1, 2, 3, 4 ], "type": "left", "test_id": "advise"}), VALUES("test12_advise", {"c11": 2, "c12": 20, "a11": [ 3, 3, 5, 10 ], "type": "left", "test_id": "advise"}), VALUES("test13_advise", {"c11": 3, "c12": 30, "a11": [ 3, 4, 20, 40 ], "type": "left", "test_id": "advise"}), VALUES("test14_advise", {"c11": 4, "c12": 40, "a11": [ 30, 30, 30 ], "type": "left", "test_id": "advise"})'
        insert2 = 'UPSERT INTO mb66635 (KEY,VALUE) VALUES("test21_advise", {"c21": 1, "c22": 10, "a21": [ 1, 10, 20], "a22": [ 1, 2, 3, 4 ], "type": "right", "test_id": "advise"}), VALUES("test22_advise", {"c21": 2, "c22": 20, "a21": [ 2, 3, 30], "a22": [ 3, 5, 10, 3 ], "type": "right", "test_id": "advise"}), VALUES("test23_advise", {"c21": 2, "c22": 21, "a21": [ 2, 20, 30], "a22": [ 3, 3, 5, 10 ], "type": "right", "test_id": "advise"}), VALUES("test24_advise", {"c21": 3, "c22": 30, "a21": [ 3, 10, 30], "a22": [ 3, 4, 20, 40 ], "type": "right", "test_id": "advise"}), VALUES("test25_advise", {"c21": 3, "c22": 31, "a21": [ 3, 20, 40], "a22": [ 4, 3, 40, 20 ], "type": "right", "test_id": "advise"}), VALUES("test26_advise", {"c21": 3, "c22": 32, "a21": [ 4, 14, 24], "a22": [ 40, 20, 4, 3 ], "type": "right", "test_id": "advise"}), VALUES("test27_advise", {"c21": 5, "c22": 50, "a21": [ 5, 15, 25], "a22": [ 1, 2, 3, 4 ], "type": "right", "test_id": "advise"}), VALUES("test28_advise", {"c21": 6, "c22": 60, "a21": [ 6, 16, 26], "a22": [ 3, 3, 5, 10 ], "type": "right", "test_id": "advise"}), VALUES("test29_advise", {"c21": 7, "c22": 70, "a21": [ 7, 17, 27], "a22": [ 30, 30, 30 ], "type": "right", "test_id": "advise"}), VALUES("test30_advise", {"c21": 8, "c22": 80, "a21": [ 8, 18, 28], "a22": [ 30, 30, 30 ], "type": "right", "test_id": "advise"})'
        self.run_cbq_query(insert1, query_context='default:default._default')
        self.run_cbq_query(insert2, query_context='default:default._default')

        # update statistics
        update_stats = 'UPDATE STATISTICS FOR mb66635(c11, c12, c21, c22, DISTINCT a11, DISTINCT a21, DISTINCT a22, type, test_id)'
        self.run_cbq_query(update_stats, query_context='default:default._default')

        advise_query = 'ADVISE select a1.c12, a2.c22 from mb66635 a1 join mb66635 a2 on a1.c11=a2.c21 and a2.test_id = "advise" where a1.test_id = "advise" and a1.c12 < 40'
        result = self.run_cbq_query(advise_query, query_context='default:default._default')
        self.log.info(f"Advise result is: {result['results']}")

        # Check we get proper covered index recommendations
        self.assertEqual(result['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'], 
                        "CREATE INDEX adv_c12_test_id_c11 ON `default`:`default`.`_default`.`mb66635`(`c12`,`c11`) WHERE `test_id` = 'advise'")
        self.assertEqual(result['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][1]['index_statement'],
                        "CREATE INDEX adv_c21_test_id_c22 ON `default`:`default`.`_default`.`mb66635`(`c21`,`c22`) WHERE `test_id` = 'advise'")
        
