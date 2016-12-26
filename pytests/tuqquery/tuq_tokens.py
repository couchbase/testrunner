from lib import testconstants
from lib.membase.api.exception import CBQError
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.basetestcase import BaseTestCase
from tuqquery.tuq import ExplainPlanHelper


class TokenTests(BaseTestCase):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(TokenTests, self).setUp()
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')

    # def suite_setUp(self):
    #     super(TokenTests, self).suite_setUp()

    def tearDown(self):
        server = self.master
        shell = RemoteMachineShellConnection(server)
        #  shell.execute_command("""curl -X DELETE -u Administrator:password http://{0}:8091/pools/default/buckets/beer-sample""".format(server.ip))
        self.sleep(20)
        super(TokenTests, self).tearDown()

    # def suite_tearDown(self):
    #     super(TokenTests, self).suite_tearDown()

    def load_sample_buckets(self, bucketName="beer-sample" ):
        """
        Load the specified sample bucket in Couchbase
        """
        #self.cluster.bucket_delete(server=self.master, bucket="default")
        server = self.master
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("""curl -v -u Administrator:password \
                             -X POST http://{0}:8091/sampleBuckets/install \
                          -d '["{1}"]'""".format(server.ip, bucketName))
        self.sleep(30)

        shell.disconnect()

    def test_tokens_secondary_indexes(self):
        self.load_sample_buckets()
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


        self.query = 'explain select name from `beer-sample` where any v in tokens(description) satisfies v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)

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
        plan = ExplainPlanHelper(actual_result)
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
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ('cover ((distinct (array `v` for `v` in tokens((`beer-sample`.`description`), {"case": "upper", "names": false, "specials": true}) end)))'))
        self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == "idx4")

        self.query = 'select name from `beer-sample` use index(`idx16`) where any v in tokens(description,{"case":"upper","names":false,"specials":true}) satisfies v = "BREWERY" END order by meta().id limit 10'
        actual_result = self.run_cbq_query()
        self.assertTrue((actual_result['results'])== (expected_result['results']))

        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END limit 10'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx5")


        self.query = 'select name from `beer-sample` use index(`idx17`) where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END limit 10'
        actual_result = self.run_cbq_query()

        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"case":"upper","names":false}) satisfies v = "GOLDEN" END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx7")
        self.query = 'select name from `beer-sample` use index(`idx18`) where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        actual_result = self.run_cbq_query()

        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{}) satisfies  v = "golden" END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])

        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"":""}) satisfies v = "golden" END limit 10'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx8")

        self.query = 'select name from `beer-sample` use index(`idx19`)  where any v in tokens(description,{"":""}) satisfies v = "golden" END order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`)  where any v in tokens(description,{"":""}) satisfies v = "golden" END order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END '
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['scan']['index'] == "idx9")

        self.query = 'select name from `beer-sample` use index(`idx20`) where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END order by name '
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"case":"random"}) satisfies  v = "golden"  END  order by name '
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])

        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"specials":"random"}) satisfies v = "brewery" END order by name'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx11")

        self.query = 'select name from `beer-sample` use index(`idx22`) where any v in tokens(description,{"specials":"random"}) satisfies  v = "golden"  END  order by name'
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"specials":"random"}) satisfies  v = "golden"  END order by name'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])


        self.query = 'explain select name from `beer-sample` where any v in tokens(description,{"names":"random"}) satisfies v = "brewery" END limit 10'
        actual_result = self.run_cbq_query()
        plan = ExplainPlanHelper(actual_result)
        self.assertTrue("covers" in str(plan))
        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == "idx10")

        self.query = 'select name from `beer-sample` use index(`idx21`) where any v in tokens(description,{"names":"random"}) satisfies  v = "golden"  END  limit 10'
        actual_result = self.run_cbq_query()
        self.query = 'select name from `beer-sample` use index(`#primary`) where any v in tokens(description,{"names":"random"}) satisfies  v = "golden"  END limit 10'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])











    def run_cbq_query(self, query=None, min_output_size=10, server=None):
        if query is None:
            query = self.query
        if server is None:
           server = self.master
           if server.ip == "127.0.0.1":
            self.n1ql_port = server.n1ql_port
        else:
            if server.ip == "127.0.0.1":
                self.n1ql_port = server.n1ql_port
            if self.input.tuq_client and "client" in self.input.tuq_client:
                server = self.tuq_client
        if self.n1ql_port == None or self.n1ql_port == '':
            self.n1ql_port = self.input.param("n1ql_port", 8093)
            if not self.n1ql_port:
                self.log.info(" n1ql_port is not defined, processing will not proceed further")
                raise Exception("n1ql_port is not defined, processing will not proceed further")
        query_params = {}
        cred_params = {'creds': []}
        for bucket in self.buckets:
            if bucket.saslPassword:
                cred_params['creds'].append({'user': 'local:%s' % bucket.name, 'pass': bucket.saslPassword})
        query_params.update(cred_params)
        query_params.update({'scan_consistency': self.scan_consistency})
        self.log.info('RUN QUERY %s' % query)

        result = RestConnection(server).query_tool(query, self.n1ql_port, query_params=query_params)
        if isinstance(result, str) or 'errors' in result:
            raise CBQError(result, server.ip)
        self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])
        return result
