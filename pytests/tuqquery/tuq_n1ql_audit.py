from membase.api.rest_client import RestConnection
from security.rbac_base import RbacBase
from security.audittest import auditTest
from tuq import QueryTests
from membase.api.exception import CBQError
import logger
log = logger.Logger.get_logger()

class QueryN1QLAuditTests(auditTest,QueryTests):
    def setUp(self):
        super(QueryN1QLAuditTests, self).setUp()
        self.log.info("==============  QueryN1QLAuditTests setup has started ==============")
        self.log.info("==============  QueryN1QLAuditTests setup has completed ==============")
        self.log_config_info()


    def suite_setUp(self):
        super(QueryN1QLAuditTests, self).suite_setUp()
        self.log.info("==============  QueryN1QLAuditTests suite_setup has started ==============")
        self.rest.load_sample("travel-sample")
        testuser = [{'id': 'no_select', 'name': 'no_select', 'password': 'password'},
                    {'id': 'query', 'name': 'query', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)

        no_select_permissions = 'query_update[*]:query_insert[*]:query_delete[*]:query_manage_index[*]:query_system_catalog'
        query_permissions = 'bucket_full_access[*]:query_select[*]:query_update[*]:' \
                           'query_insert[*]:query_delete[*]:query_manage_index[*]:' \
                           'query_system_catalog'

        role_list = [{'id': 'no_select', 'name': 'no_select', 'roles': '%s' % no_select_permissions},
                     {'id': 'query', 'name': 'query', 'roles': '%s' % query_permissions}]
        RbacBase().add_user_role(role_list, self.rest, 'builtin')
        self.log.info("==============  QueryN1QLAuditTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryN1QLAuditTests tearDown has started ==============")
        self.log.info("==============  QueryN1QLAuditTests tearDown has completed ==============")
        super(QueryN1QLAuditTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryN1QLAuditTests suite_tearDown has started ==============")
        self.log.info("==============  QueryN1QLAuditTests suite_tearDown has completed ==============")
        super(QueryN1QLAuditTests, self).suite_tearDown()

    def test_queryEvents(self):
        query_type = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'

        if (query_type =='create_index'):
            self.run_cbq_query(query="CREATE INDEX idx on default(fake)")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'CREATE INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'CREATE INDEX idx on default(fake)',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL CREATE INDEX statement was executed'}

        elif (query_type =='build_index'):
            self.run_cbq_query(query="CREATE INDEX idx3 on default(fake2) WITH {'defer_build':true}")
            self.run_cbq_query(query="BUILD INDEX on default(idx3)")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'BUILD INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'BUILD INDEX on default(idx3)',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL BUILD INDEX statement was executed'}

        elif(query_type == 'drop_index'):
            self.run_cbq_query(query='CREATE INDEX idx2 on default(fake1)')
            self.run_cbq_query(query='DROP INDEX default.idx2')
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'DROP INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'DROP INDEX default.idx2',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL DROP INDEX statement was executed'}

        elif(query_type == 'primary_index'):
            self.run_cbq_query(query="CREATE PRIMARY INDEX on default")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'CREATE PRIMARY INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'CREATE PRIMARY INDEX on default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL CREATE PRIMARY INDEX statement was executed'}

        elif(query_type == 'select'):
            self.run_cbq_query(query="SELECT * FROM default LIMIT 100")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'SELECT * FROM default LIMIT 100',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL SELECT statement was executed'}

        elif(query_type == 'explain'):
            self.run_cbq_query(query="EXPLAIN SELECT * FROM default")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'EXPLAIN statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'EXPLAIN SELECT * FROM default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL EXPLAIN statement was executed'}

        elif(query_type == 'prepare'):
            self.run_cbq_query(query="prepare a1 from select * from default")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'PREPARE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'prepare a1 from select * from default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL PREPARE statement was executed'}

        elif(query_type == 'adhoc_false'):
            self.run_cbq_query(query="prepare a1 from select * from default")
            self.run_cbq_query(query="execute a1")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': False,
                               'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'execute a1',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL SELECT statement was executed'}

        elif(query_type == 'unrecognized'):
            try:
                self.run_cbq_query(query="selec * fro default")
            except CBQError:
                expectedResults = {'node': '127.0.0.1:8091', 'status': 'fatal', 'isAdHoc': True,
                                   'name': 'UNRECOGNIZED statement', 'real_userid': {'source': source, 'user': user},
                                   'statement': 'selec * fro default', 'userAgent': 'Python-httplib2/$Rev: 259 $',
                                   'id': self.eventID, 'description': 'An unrecognized statement was received by the N1QL query engine'}

        elif(query_type == 'insert'):
            self.run_cbq_query(query='INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })')
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'INSERT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL INSERT statement was executed'}

        elif(query_type == 'upsert'):
            self.run_cbq_query(query='UPSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })')
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'UPSERT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'UPSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL UPSERT statement was executed'}

        elif(query_type == 'delete'):
            self.run_cbq_query(query='DELETE FROM `travel-sample` WHERE type = "hotel"')
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'DELETE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'DELETE FROM `travel-sample` WHERE type = "hotel"',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL DELETE statement was executed'}

        elif(query_type == 'update'):
            self.run_cbq_query(query='UPDATE `travel-sample` SET foo = 5')
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'UPDATE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'UPDATE `travel-sample` SET foo = 5',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL UPDATE statement was executed'}

        elif(query_type == 'merge'):
            self.run_cbq_query(query='MERGE INTO `travel-sample` t USING [{"id":"21728"},{"id":"21730"}] source '
                                     'ON KEY "hotel_"|| source.id WHEN MATCHED THEN UPDATE SET t.old_vacancy = t.vacancy'
                                     ', t.vacancy = false RETURNING meta(t).id, t.old_vacancy, t.vacancy')
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'MERGE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'MERGE INTO `travel-sample` t USING [{"id":"21728"},{"id":"21730"}] source '
                                     'ON KEY "hotel_"|| source.id WHEN MATCHED THEN UPDATE SET t.old_vacancy = t.vacancy'
                                     ', t.vacancy = false RETURNING meta(t).id, t.old_vacancy, t.vacancy',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL MERGE statement was executed'}

        elif(query_type == 'grant'):
            self.run_cbq_query(query="GRANT query_external_access TO query")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'GRANT ROLE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'GRANT query_external_access TO query',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL GRANT ROLE statement was executed'}

        elif(query_type == 'revoke'):
            self.run_cbq_query(query="REVOKE query_system_catalog FROM query")
            expectedResults = {'node': '127.0.0.1:8091', 'status': 'success', 'isAdHoc': True,
                               'name': 'REVOKE ROLE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'REVOKE query_system_catalog FROM query',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL REVOKE ROLE statement was executed'}

        elif(query_type == 'no_select'):
            cbqpath = '%scbq' % self.path + " -e %s:%s -u 'no_select' -p 'password' -q " % (self.master.ip, self.n1ql_port)
            query = 'select * from default limit 100'
            self.shell.execute_commands_inside(cbqpath, query, '', '', '', '', '')
            expectedResults ={'node': '127.0.0.1:8091', 'status': 'fatal', 'isAdHoc': True,
                              'statement': 'select * from default limit 100;',
                              'description': 'An unrecognized statement was received by the N1QL query engine',
                              'real_userid': {'source': 'local', 'user': 'no_select'},
                              'userAgent': 'Go-http-client/1.1 (CBQ/2.0)',
                              'id': self.eventID, 'name': 'UNRECOGNIZED statement'}

        self.checkConfig(self.eventID, self.master, expectedResults, n1ql_audit=True)
