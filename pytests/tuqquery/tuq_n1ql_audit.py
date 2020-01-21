from membase.api.rest_client import RestHelper
from security.rbac_base import RbacBase
from security.audittest import auditTest
from .tuq import QueryTests
from membase.api.exception import CBQError
import logger
log = logger.Logger.get_logger()

class QueryN1QLAuditTests(auditTest, QueryTests):
    def setUp(self):
        super(QueryN1QLAuditTests, self).setUp()
        self.log.info("==============  QueryN1QLAuditTests setup has started ==============")
        self.audit_codes = [28672, 28673, 28674, 28675, 28676, 28677, 28678, 28679, 28680, 28681,
                            28682, 28683, 28684, 28685, 28686, 28687, 28688]
        self.unauditedID = self.input.param("unauditedID", "")
        self.audit_url = "http://%s:%s/settings/audit" % (self.master.ip, self.master.port)
        self.filter = self.input.param("filter", False)
        self.log.info("==============  QueryN1QLAuditTests setup has completed ==============")
        self.log_config_info()
        self.set_audit()


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

        role_list = [{'id': 'no_select', 'name': 'no_select', 'roles': '%s' % no_select_permissions, 'password':'password'},
                     {'id': 'query', 'name': 'query', 'roles': '%s' % query_permissions, 'password':'password'}]
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
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'CREATE INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'CREATE INDEX idx on default(join_day)',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL CREATE INDEX statement was executed'}

        elif(query_type =='alter_index'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="CREATE INDEX idx4 on default(join_day) WITH {'nodes':['%s:%s']}" % (self.servers[0].ip, self.servers[0].port))
            self.run_cbq_query(query="ALTER INDEX default.idx4 WITH {'action':'move','nodes':['%s:%s']}" % (self.servers[1].ip, self.servers[1].port))
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'ALTER INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': "ALTER INDEX default.idx4 WITH {'action':'move','nodes':['%s:%s']}" % (self.servers[1].ip, self.servers[1].port),
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL ALTER INDEX statement was executed'}

        elif (query_type =='build_index'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="CREATE INDEX idx3 on default(join_yr) WITH {'defer_build':true}")
            self.run_cbq_query(query="BUILD INDEX on default(idx3)")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'BUILD INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'BUILD INDEX on default(idx3)',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL BUILD INDEX statement was executed'}

        elif(query_type == 'drop_index'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='CREATE INDEX idx2 on default(fake1)')
            self.run_cbq_query(query='DROP INDEX default.idx2')
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'DROP INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'DROP INDEX default.idx2',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL DROP INDEX statement was executed'}

        elif(query_type == 'primary_index'):
            if self.filter:
                self.audit_codes.remove(self.eventID)
                self.set_audit(set_disabled=True)

            self.run_cbq_query(query="CREATE PRIMARY INDEX on default")
            if self.filter:
                self.run_cbq_query(query="delete from default limit 1")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'CREATE PRIMARY INDEX statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'CREATE PRIMARY INDEX on default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL CREATE PRIMARY INDEX statement was executed'}

        elif(query_type == 'select'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="SELECT * FROM default LIMIT 100")
            expectedResults = {'node':'%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'SELECT * FROM default LIMIT 100',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL SELECT statement was executed'}

        elif(query_type == 'explain'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="EXPLAIN SELECT * FROM default")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'EXPLAIN statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'EXPLAIN SELECT * FROM default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL EXPLAIN statement was executed'}

        elif(query_type == 'prepare'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="prepare a1 from select * from default")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'PREPARE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'prepare a1 from select * from default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL PREPARE statement was executed'}

        elif(query_type == 'adhoc_false'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="prepare a1 from select * from default")
            self.run_cbq_query(query="execute a1")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': False,
                               'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'prepare a1 from select * from default',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID, 'preparedId': 'a1',
                               'description': 'A N1QL SELECT statement was executed'}

        elif(query_type == 'unrecognized'):
            if self.filter:
                self.execute_filtered_query()
            try:
                self.run_cbq_query(query="selec * fro default")
            except CBQError:
                expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'fatal', 'isAdHoc': True,
                                   'name': 'UNRECOGNIZED statement', 'real_userid': {'source': source, 'user': user},
                                   'statement': 'selec * fro default', 'userAgent': 'Python-httplib2/$Rev: 259 $',
                                   'id': self.eventID, 'description': 'An unrecognized statement was received by the N1QL query engine'}

        elif(query_type == 'insert'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })')
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'INSERT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL INSERT statement was executed'}

        elif(query_type == 'upsert'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='UPSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })')
            expectedResults = {'node':'%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'UPSERT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'UPSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                       '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                       '[ "11", "12", "13" ] })',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL UPSERT statement was executed'}

        elif(query_type == 'delete'):
            if self.filter:
                self.audit_codes.remove(self.eventID)
                self.set_audit(set_disabled=True)
                try:
                    self.run_cbq_query(query="selec * fro default", server=self.servers[1])
                except CBQError:
                    self.log.info("Query is unrecognized (expected)")
            self.run_cbq_query(query='DELETE FROM `travel-sample` WHERE type = "hotel"', server=self.servers[1])
            expectedResults = {'node': '%s:%s' % (self.servers[1].ip, self.servers[1].port), 'status': 'success', 'isAdHoc': True,
                               'name': 'DELETE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'DELETE FROM `travel-sample` WHERE type = "hotel"',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL DELETE statement was executed'}

        elif(query_type == 'update'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='UPDATE `travel-sample` SET foo = 5')
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'UPDATE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'UPDATE `travel-sample` SET foo = 5',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL UPDATE statement was executed'}

        elif(query_type == 'merge'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='MERGE INTO `travel-sample` t USING [{"id":"21728"},{"id":"21730"}] source '
                                     'ON KEY "hotel_"|| source.id WHEN MATCHED THEN UPDATE SET t.old_vacancy = t.vacancy'
                                     ', t.vacancy = false RETURNING meta(t).id, t.old_vacancy, t.vacancy')
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'MERGE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'MERGE INTO `travel-sample` t USING [{"id":"21728"},{"id":"21730"}] source '
                                     'ON KEY "hotel_"|| source.id WHEN MATCHED THEN UPDATE SET t.old_vacancy = t.vacancy'
                                     ', t.vacancy = false RETURNING meta(t).id, t.old_vacancy, t.vacancy',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL MERGE statement was executed'}

        elif(query_type == 'grant'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="GRANT query_external_access TO query")
            expectedResults = {'node':'%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'GRANT ROLE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'GRANT query_external_access TO query',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL GRANT ROLE statement was executed'}

        elif(query_type == 'revoke'):
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="REVOKE query_system_catalog FROM query")
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'REVOKE ROLE statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'REVOKE query_system_catalog FROM query',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL REVOKE ROLE statement was executed'}

        elif(query_type == 'no_select'):
            cbqpath = '%scbq' % self.path + " -e %s:%s -u 'no_select' -p 'password' -q " % (self.master.ip, self.n1ql_port)
            query = 'select * from default limit 100'
            self.shell.execute_commands_inside(cbqpath, query, '', '', '', '', '')
            expectedResults ={'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'stopped', 'isAdHoc': True,
                              'statement': 'select * from default limit 100;',
                              'description': 'A N1QL SELECT statement was executed',
                              'real_userid': {'source': 'local', 'user': 'no_select'},
                              'userAgent': 'Go-http-client/1.1 (CBQ/2.0)',
                              'id': self.eventID, 'name': 'SELECT statement'}
        
        if query_type == 'delete':
            self.checkConfig(self.eventID, self.servers[1], expectedResults, n1ql_audit=True)
            if self.filter:
                self.checkFilter(self.unauditedID, self.servers[1])
        else:
            self.checkConfig(self.eventID, self.master, expectedResults, n1ql_audit=True)
            if self.filter:
                self.checkFilter(self.unauditedID, self.master)


    def test_user_filter(self):
        self.set_audit(disable_user=True)
        cbqpath = '%scbq' % self.path + " -e %s:%s -u 'no_select' -p 'password' -q " % (self.master.ip, self.n1ql_port)
        query = 'select * from default limit 100'
        self.shell.execute_commands_inside(cbqpath, query, '', '', '', '', '')
        self.checkFilter(self.unauditedID, self.master)

    def test_setting_propagation(self):
        self.set_audit(set_disabled=True)
        audit_url = "http://%s:%s/settings/audit" % (self.servers[1].ip, self.servers[1].port)
        curl_output = self.shell.execute_command("%s -u Administrator:password %s" % (self.curl_path, audit_url))
        expected_curl = self.convert_list_to_json(curl_output[0])
        self.assertEqual(expected_curl['disabled'], self.audit_codes)

    def set_audit(self, set_disabled=False, disable_user=False):
        if set_disabled:
            curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabled=%s' %s"
                                                    % (self.curl_path, 'true', ','.join(map(str, self.audit_codes)), self.audit_url))
        elif disable_user:
            curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabledUsers=%s' %s"
                                                    % (self.curl_path, 'true', 'no_select/local', self.audit_url))
        else:
            curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabled=' %s"
                                                    % (self.curl_path, 'true', self.audit_url))
        if "errors" in str(curl_output):
            self.log.error("Auditing settings were not set correctly")
        self.sleep(10)

    def execute_filtered_query(self):
        self.audit_codes.remove(self.eventID)
        self.set_audit(set_disabled=True)
        self.run_cbq_query(query="delete from default limit 1")