from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.tuqquery.tuq import QueryTests

class TokenTests(QueryTests):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(TokenTests, self).setUp()
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')

    def tearDown(self):
        server = self.master
        shell = RemoteMachineShellConnection(server)
        #  shell.execute_command("""curl -X DELETE -u Administrator:password http://{0}:8091/pools/default/buckets/beer-sample""".format(server.ip))
        self.sleep(20)
        super(TokenTests, self).tearDown()

    def test_tokens_secondary_indexes(self):
        self.rest.load_sample("beer-sample")
        self.sleep(20)
        created_indexes = []
        self.query = 'create primary index on `beer-sample`'
        self.run_cbq_query()
        self.query = 'create index idx1 on `beer-sample`(description,name )'
        self.run_cbq_query()
        self.query = 'create index idx2 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx3 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"lower","names":true,"specials":false}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx4 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false,"specials":true}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx5 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx6 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx7 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx8 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"":""}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx9 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"random"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx10 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"names":"random"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx11 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"specials":"random"}) END ,description,name )'
        self.run_cbq_query()
        self.query = 'create index idx12 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description) END )'
        self.run_cbq_query()
        self.query = 'create index idx13 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"lower"}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx14 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper"}) END )'
        self.run_cbq_query()
        self.query = 'create index idx15 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"lower","names":true,"specials":false}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx16 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false,"specials":true}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx17 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"upper","names":false}) END )'
        self.run_cbq_query()
        self.query = 'create index idx18 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{}) END )'
        self.run_cbq_query()
        self.query = 'create index idx19 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"":""}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx20 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"case":"random"}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx21 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"names":"random"}) END  )'
        self.run_cbq_query()
        self.query = 'create index idx22 on `beer-sample`( DISTINCT ARRAY v FOR v in tokens(description,{"specials":"random"}) END  )'
        self.run_cbq_query()

        for i in range(1, 22):
            index = 'idx{0}'.format(i)
            created_indexes.append(index)

        self.query = 'explain select name from `beer-sample` where any v in tokens(description) satisfies v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)

        self.assertTrue(actual_result['results'])
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx2")

        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ('cover ((distinct (array `v` for `v` in tokens((`beer-sample`.`description`)) end)))'))

        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(reverse(description)) satisfies v = "nedlog" END order by meta().id limit 10'
        expected_result = self.run_cbq_query()


        self.query = 'select name from `beer-sample` where any v in tokens(reverse(description)) satisfies v = "nedlog" END order by meta().id limit 10'
        actual_result = self.run_cbq_query()
        #self.assertTrue(str(actual_result['results'])=="[{u'name': u'21A IPA'}, {u'name': u'Amendment Pale Ale'}, {u'name': u'Double Trouble IPA'}, {u'name': u'South Park Blonde'}, {u'name': u'Restoration Pale Ale'}, {u'name': u'S.O.S'}, {u'name': u'Satsuma Harvest Wit'}, {u'name': u'Adnams Explorer'}, {u'name': u'Shock Top'}, {u'name': u'Anniversary Maibock'}]" )
        self.assertTrue((actual_result['results'])== (expected_result['results']))


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"case":"lower","names":true,"specials":false}) satisfies v = "brewery" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ('cover ((distinct (array `v` for `v` in tokens((`beer-sample`.`description`), {"case": "lower", "names": true, "specials": false}) end)))'))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx3")

        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"case":"lower","names":true,"specials":false}) satisfies v = "brewery" END order by meta().id limit 10'

        expected_result = self.run_cbq_query()

        self.query = 'select name from `beer-sample` use index(`idx15`) where any v in tokens(description,{"case":"lower","names":true,"specials":false}) satisfies v = "brewery" END order by meta().id limit 10'
        actual_result = self.run_cbq_query()

        self.assertTrue((actual_result['results'])== (expected_result['results']) )

        self.query = 'explain select name from `beer-sample` use index(`idx14`) where any v in tokens(description,{"case":"upper","names":false,"specials":true}) satisfies v = "BREWERY" END order by meta().id limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ('cover ((distinct (array `v` for `v` in tokens((`beer-sample`.`description`), {"case": "upper", "names": false, "specials": true}) end)))'))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == "idx4")

        self.query = 'select name from `beer-sample` use index(`idx16`) where any v in tokens(description,{"case":"upper","names":false,"specials":true}) satisfies v = "BREWERY" END order by meta().id limit 10'
        actual_result = self.run_cbq_query()
        self.assertTrue((actual_result['results'])== (expected_result['results']))

        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx5")


        self.query = 'select name from `beer-sample` use index(`idx17`) where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END limit 10'
        actual_result = self.run_cbq_query()

        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx7")
        self.query = 'select name from `beer-sample` use index(`idx18`) where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        actual_result = self.run_cbq_query()

        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])

        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"":""}) satisfies v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx8")

        self.query = 'select name from `beer-sample` use index(`idx19`)  where any v in tokens(description,{"":""}) satisfies v = "golden" END order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`)  where any v in tokens(description,{"":""}) satisfies v = "golden" END order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END '
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['scan']['index'] == "idx9")

        self.query = 'select name from `beer-sample` use index(`idx20`) where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END  order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])

        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"specials":"random"}) satisfies v = "brewery" END order by name'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx11")

        self.query = 'select name from `beer-sample` use index(`idx22`) where any v in tokens(description,{"specials":"random"}) satisfies  v = "golden"  END  order by name'
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"specials":"random"}) satisfies  v = "golden"  END order by name'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"names":"random"}) satisfies v = "brewery" END limit 10'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx10")

        self.query = 'select name from `beer-sample` use index(`idx21`) where any v in tokens(description,{"names":"random"}) satisfies  v = "golden"  END  limit 10'
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"names":"random"}) satisfies  v = "golden"  END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])
        for idx in created_indexes:
            self.query = "DROP INDEX %s.%s USING %s" % ("`beer-sample`", idx, self.index_type)
            actual_result = self.run_cbq_query()

    '''This test is specific to beer-sample bucket'''
    def test_tokens_simple_syntax(self):
        self.rest.load_sample("beer-sample")
        bucket_doc_map = {"beer-sample": 7303}
        bucket_status_map = {"beer-sample": "healthy"}
        self.wait_for_buckets_status(bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
        self._wait_for_index_online("beer-sample", "beer_primary")
        self.sleep(10)
        created_indexes = []
        try:
            idx1 = "idx_suffixes"
            idx2 = "idx_tokens"
            idx3 = "idx_pairs"
            idx4 = "idx_addresses"
            self.query = 'CREATE INDEX {0} ON `beer-sample`( DISTINCT SUFFIXES( name ) )'.format(idx1)
            self.run_cbq_query()
            self._wait_for_index_online("beer-sample", "beer_primary")
            created_indexes.append(idx1)
            self.query = "explain select * from `beer-sample` where name like '%Cafe%'"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['scan']['index'], idx1)
            self.query = 'CREATE INDEX {0} ON `beer-sample`( DISTINCT TOKENS( description ) )'.format(idx2)
            self.run_cbq_query()
            self._wait_for_index_online("beer-sample", "beer_primary")
            created_indexes.append(idx2)
            self.query = "explain select * from `beer-sample` where contains_token(description,'Great')"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['scan']['index'], idx2)
            self.query = "CREATE INDEX {0} ON `beer-sample`( DISTINCT PAIRS( SELF ) )".format(idx3)
            self.run_cbq_query()
            self._wait_for_index_online("beer-sample", "beer_primary")
            created_indexes.append(idx3)
            self.query = "explain select * from `beer-sample` where name like 'A%' and abv > 6"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue("idx_suffixes" in str(plan))
            self.query = "CREATE INDEX {0} ON `beer-sample`( ALL address )".format(idx4)
            self.run_cbq_query()
            self._wait_for_index_online("beer-sample", "beer_primary")
            created_indexes.append(idx4)
            self.query = "explain select min(addr) from `beer-sample` unnest address as addr"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['index'], idx4)
            self.query = "explain select count(a) from `beer-sample` unnest address as a"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertEqual(plan['~children'][0]['index'], idx4)
            self.query = "explain select * from `beer-sample` where any place in address satisfies " \
                         "place LIKE '100 %' end"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(idx4 in str(plan))
            self.assertTrue(idx3 in str(plan))
        finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `beer-sample`.%s" % (idx)
                    self.run_cbq_query()
                self.rest.delete_bucket("beer-sample")

    def test_dynamicindex_limit(self):
        self.rest.load_sample("beer-sample")
        self.sleep(20)
        created_indexes = []
        try:
            idx1 = "idx_abv"
            idx2 = "dynamic"
            self.query = "CREATE INDEX idx_abv ON `beer-sample`( abv )"
            self.run_cbq_query()
            created_indexes.append(idx1)
            self.query = "CREATE INDEX dynamic ON `beer-sample`( DISTINCT PAIRS( SELF ) )"
            self.run_cbq_query()
            created_indexes.append(idx2)
            self.query = "Explain select * from `beer-sample` where abv > 5 LIMIT 10"
            res = self.run_cbq_query()
            plan = self.ExplainPlanHelper(res)
            self.assertTrue(plan['~children'][0]['~children'][0]['limit']=='10')
        finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `beer-sample`.%s" % ( idx)
                    self.run_cbq_query()
