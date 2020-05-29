from pytests.tuqquery.tuq import QueryTests


class TokenTests(QueryTests):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(TokenTests, self).setUp()
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')
        if not self.sample_bucket:
            self.sample_bucket = 'beer-sample'
        self.query_bucket = self.get_query_buckets(sample_buckets=[self.sample_bucket])[0]

    def tearDown(self):
        # server = self.master
        # shell = RemoteMachineShellConnection(server)
        #  shell.execute_command("""curl -X DELETE -u Administrator:password 
        #  http://{0}:8091/pools/default/buckets/beer-sample""".format(server.ip))
        # self.sleep(20)
        super(TokenTests, self).tearDown()

    def test_tokens_secondary_indexes(self):
        self.rest.load_sample(self.sample_bucket)
        self.sleep(20)
        created_indexes = []
        self.query = 'create primary index on ' + self.query_bucket
        self.run_cbq_query()
        self.query = 'create index idx1 on ' + self.query_bucket + '(description,name )'
        self.run_cbq_query()
        self.query = 'create index idx2 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx3 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"lower","names":true,"specials":false}) ' \
                     'END ,description,name ) '
        self.run_cbq_query()
        self.query = 'create index idx4 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false,"specials":true}) ' \
                     'END ,description,name ) '
        self.run_cbq_query()
        self.query = 'create index idx5 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false}) END ,' \
                     'description,name ) '
        self.run_cbq_query()
        self.query = 'create index idx6 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx7 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx8 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"":""}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx9 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"random"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx10 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"names":"random"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx11 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"specials":"random"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx12 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description) END )'
        self.run_cbq_query()
        self.query = 'create index idx13 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"lower"}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx14 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper"}) END )'
        self.run_cbq_query()
        self.query = 'create index idx15 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"lower","names":true,"specials":false}) ' \
                     'END  ) '
        self.run_cbq_query()
        self.query = 'create index idx16 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false,"specials":true}) ' \
                     'END  ) '
        self.run_cbq_query()
        self.query = 'create index idx17 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false}) END )'
        self.run_cbq_query()
        self.query = 'create index idx18 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{}) END )'
        self.run_cbq_query()
        self.query = 'create index idx19 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"":""}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx20 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"case":"random"}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx21 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"names":"random"}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx22 on ' + self.query_bucket + \
                     '( DISTINCT ARRAY v FOR v in tokens(description,{"specials":"random"}) END  )'
        self.run_cbq_query()

        for i in range(1, 22):
            index = 'idx{0}'.format(i)
            created_indexes.append(index)

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' b where any v in tokens(description) satisfies v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)

        self.assertTrue(actual_result['results'])
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx2")

        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
            'cover ((distinct (array `v` for `v` in tokens((`b`.`description`)) end)))'))

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(reverse(description)) satisfies v = "nedlog" END ' \
                     'order by meta().id limit 10 '
        expected_result = self.run_cbq_query()

        self.query = 'select name from ' + self.query_bucket + \
                     ' b where any v in tokens(reverse(description)) satisfies v = "nedlog" END order by meta().id ' \
                     'limit 10 '
        actual_result = self.run_cbq_query()
        # self.assertTrue(str(actual_result['results'])=="[{u'name': u'21A IPA'}, {u'name': u'Amendment Pale Ale'},
        # {u'name': u'Double Trouble IPA'}, {u'name': u'South Park Blonde'}, {u'name': u'Restoration Pale Ale'},
        # {u'name': u'S.O.S'}, {u'name': u'Satsuma Harvest Wit'}, {u'name': u'Adnams Explorer'}, {u'name': u'Shock
        # Top'}, {u'name': u'Anniversary Maibock'}]" )
        self.assertTrue((actual_result['results']) == (expected_result['results']))

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' b where any v in tokens(description,{"case":"lower","names":true,"specials":false}) ' \
                     ' satisfies v = "brewery" END limit 10 '
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
            'cover ((distinct (array `v` for `v` in tokens((`b`.`description`), {"case": "lower", "names": true, '
            '"specials": false}) end)))'))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx3")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(description,{"case":"lower","names":true,' \
                     '"specials":false}) satisfies v = "brewery" END order by meta().id limit 10 '

        expected_result = self.run_cbq_query()

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx15`) where any v in tokens(description,{"case":"lower","names":true,' \
                     '"specials":false}) satisfies v = "brewery" END order by meta().id limit 10 '
        actual_result = self.run_cbq_query()

        self.assertTrue((actual_result['results']) == (expected_result['results']))

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' b use index(`idx14`) where any v in tokens(description,{"case":"upper","names":false,' \
                     '"specials":true}) satisfies v = "BREWERY" END order by meta().id limit 10 '
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
            'cover ((distinct (array `v` for `v` in tokens((`b`.`description`), {"case": "upper", "names": false, '
            '"specials": true}) end)))'))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == "idx4")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx16`) where any v in tokens(description,{"case":"upper","names":false,' \
                     '"specials":true}) satisfies v = "BREWERY" END order by meta().id limit 10 '
        actual_result = self.run_cbq_query()
        self.assertTrue((actual_result['results']) == (expected_result['results']))

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END ' \
                     'limit 10 '
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx5")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx17`) where any v in tokens(description,{"case":"upper","names":false}) ' \
                     'satisfies v = "GOLDEN" END limit 10 '
        actual_result = self.run_cbq_query()

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(description,{"case":"upper","names":false}) ' \
                     'satisfies v = "GOLDEN" END limit 10 '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx7")
        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx18`) where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        actual_result = self.run_cbq_query()

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' where any v in tokens(description,{"":""}) satisfies v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx8")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx19`)  where any v in tokens(description,{"":""}) satisfies v = "golden" END ' \
                     'order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`)  where any v in tokens(description,{"":""}) satisfies v = "golden" END ' \
                     'order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END '
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['scan']['index'] == "idx9")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx20`) where any v in tokens(description,{"case":"random"}) ' \
                     'satisfies  v = "golden" END order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(description,{"case":"random"}) satisfies  v = ' \
                     '"golden"  END  order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' where any v in tokens(description,{"specials":"random"}) satisfies v = "brewery" END order by ' \
                     'name '
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx11")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx22`) where any v in tokens(description,{"specials":"random"}) satisfies  v = ' \
                     '"golden"  END  order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(description,{"specials":"random"}) satisfies  v = ' \
                     '"golden"  END order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])

        self.query = 'explain select name from ' + self.query_bucket + \
                     ' where any v in tokens(description,{"names":"random"}) satisfies v = "brewery" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx10")

        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`idx21`) where any v in tokens(description,{"names":"random"}) satisfies  v = ' \
                     '"golden"  END  limit 10 '
        actual_result = self.run_cbq_query()
        self.query = 'select name from ' + self.query_bucket + \
                     ' use index(`#primary`) where any v in tokens(description,{"names":"random"}) satisfies  v = ' \
                     '"golden"  END limit 10 '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])
        for idx in created_indexes:
            self.query = "DROP INDEX %s ON %s USING %s" % (idx, self.query_bucket, self.index_type)
            self.run_cbq_query()

    '''This test is specific to beer-sample bucket'''

    def test_tokens_simple_syntax(self):
        self.rest.load_sample(self.sample_bucket)
        bucket_doc_map = {self.sample_bucket: 7303}
        bucket_status_map = {self.sample_bucket: "healthy"}
        self.wait_for_buckets_status(bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
        self._wait_for_index_online(self.sample_bucket, "beer_primary")
        self.sleep(10)
        created_indexes = []
        try:
            idx1 = "idx_suffixes"
            idx2 = "idx_tokens"
            idx3 = "idx_pairs"
            idx4 = "idx_addresses"
            self.query = 'CREATE INDEX {0} ON '.format(idx1) + self.query_bucket + '( DISTINCT SUFFIXES( name ) )'
            self.run_cbq_query()
            self._wait_for_index_online(self.sample_bucket, idx1)
            created_indexes.append(idx1)
            self.query = "explain select * from " + self.query_bucket + " where name like '%Cafe%'"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['scan']['index'], idx1)
            self.query = 'CREATE INDEX {0} ON '.format(idx2) + self.query_bucket + '( DISTINCT TOKENS( description ) )'
            self.run_cbq_query()
            self._wait_for_index_online(self.sample_bucket, idx2)
            created_indexes.append(idx2)
            self.query = "explain select * from " + self.query_bucket + " where contains_token(description,'Great')"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['scan']['index'], idx2)
            self.query = "CREATE INDEX {0} ON ".format(idx3) + self.query_bucket + "( DISTINCT PAIRS( SELF ) )"
            self.run_cbq_query()
            self._wait_for_index_online(self.sample_bucket, idx3)
            created_indexes.append(idx3)
            self.query = "explain select * from " + self.query_bucket + " where name like 'A%' and abv > 6"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue("idx_suffixes" in str(plan))
            self.query = "CREATE INDEX {0} ON ".format(idx4) + self.query_bucket + "( ALL address )"
            self.run_cbq_query()
            self._wait_for_index_online(self.sample_bucket, idx4)
            created_indexes.append(idx4)
            self.query = "explain select min(addr) from " + self.query_bucket + " unnest address as addr"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['index'], idx4)
            self.query = "explain select count(a) from " + self.query_bucket + " unnest address as a"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['index'], idx4)
            self.query = "explain select * from " + self.query_bucket + " where any place in address satisfies " \
                                                                        "place LIKE '100 %' end"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(idx4 in str(plan))
            self.assertTrue(idx3 in str(plan))
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s ON %s" % (idx, self.query_bucket)
                self.run_cbq_query()
            self.rest.delete_bucket(self.query_bucket)

    def test_dynamicindex_limit(self):
        self.rest.load_sample(self.sample_bucket)
        self.sleep(20)
        created_indexes = []
        try:
            idx1 = "idx_abv"
            idx2 = "dynamic"
            self.query = "CREATE INDEX idx_abv ON " + self.query_bucket + "( abv )"
            self.run_cbq_query()
            created_indexes.append(idx1)
            self.query = "CREATE INDEX dynamic ON " + self.query_bucket + "( DISTINCT PAIRS( SELF ) )"
            self.run_cbq_query()
            created_indexes.append(idx2)
            self.query = "Explain select * from " + self.query_bucket + " where abv > 5 LIMIT 10"
            res = self.run_cbq_query()
            plan = self.ExplainPlanHelper(res)
            self.assertTrue(plan['~children'][0]['~children'][0]['limit'] == '10')
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s ON %s" % (idx, self.query_bucket)
                self.run_cbq_query()
