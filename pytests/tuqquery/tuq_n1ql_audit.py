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
        if not self.sample_bucket:
            self.sample_bucket = 'travel-sample'
        self.query_buckets = self.get_query_buckets(sample_buckets=[self.sample_bucket])

    def suite_setUp(self):
        super(QueryN1QLAuditTests, self).suite_setUp()
        self.log.info("==============  QueryN1QLAuditTests suite_setup has started ==============")
        if not self.sample_bucket:
            self.sample_bucket = 'travel-sample'
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_all_indexes_online()
        testuser = [{'id': 'no_select', 'name': 'no_select', 'password': 'password'},
                    {'id': 'query', 'name': 'query', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)

        no_select_permissions = 'query_update[*]:query_insert[*]:query_delete[*]:query_manage_index[' \
                                '*]:query_system_catalog '
        query_permissions = 'bucket_full_access[*]:query_select[*]:query_update[*]:' \
                            'query_insert[*]:query_delete[*]:query_manage_index[*]:' \
                            'query_system_catalog'

        role_list = [
            {'id': 'no_select', 'name': 'no_select', 'roles': '%s' % no_select_permissions, 'password': 'password'},
            {'id': 'query', 'name': 'query', 'roles': '%s' % query_permissions, 'password': 'password'}]
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

        if query_type == 'create_index':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="CREATE INDEX idx on " + self.query_buckets[0] + "(join_day)")
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'CREATE INDEX statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'CREATE INDEX idx on ' + self.query_buckets[0] + '(join_day)',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL CREATE INDEX statement was executed'}

        elif query_type == 'alter_index':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(
                query="CREATE INDEX idx4 on " + self.query_buckets[0] + "(join_day) WITH {'nodes':['%s:%s']}" % (
                    self.servers[0].ip, self.servers[0].port))
            self.run_cbq_query(
                query="ALTER INDEX idx4 ON " + self.query_buckets[0] + " WITH {'action':'move','nodes':['%s:%s']}" % (
                    self.servers[1].ip, self.servers[1].port))
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'ALTER INDEX statement', 'real_userid': {'source': source, 'user': user},
                                'statement': "ALTER INDEX idx4 ON " + self.query_buckets[0] + " WITH {'action':'move',"
                                                                                              "'nodes':['%s:%s']}" % (
                                    self.servers[1].ip, self.servers[1].port),
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL ALTER INDEX statement was executed'}

        elif query_type == 'build_index':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(
                query="CREATE INDEX idx3 on " + self.query_buckets[0] + "(join_yr) WITH {'defer_build':true}")
            self.run_cbq_query(query="BUILD INDEX on " + self.query_buckets[0] + "(idx3)")
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'BUILD INDEX statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'BUILD INDEX on ' + self.query_buckets[0] + '(idx3)',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL BUILD INDEX statement was executed'}

        elif query_type == 'drop_index':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='CREATE INDEX idx2 on ' + self.query_buckets[0] + '(fake1)')
            self.run_cbq_query(query='DROP INDEX idx2 ON ' + self.query_buckets[0])
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'DROP INDEX statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'DROP INDEX idx2 ON ' + self.query_buckets[0],
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL DROP INDEX statement was executed'}

        elif query_type == 'primary_index':
            if self.filter:
                self.audit_codes.remove(self.eventID)
                self.set_audit(set_disabled=True)

            self.run_cbq_query(query="CREATE PRIMARY INDEX on " + self.query_buckets[0])
            if self.filter:
                self.run_cbq_query(query="delete from " + self.query_buckets[0] + " limit 1")
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'CREATE PRIMARY INDEX statement',
                                'real_userid': {'source': source, 'user': user},
                                'statement': 'CREATE PRIMARY INDEX on ' + self.query_buckets[0],
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL CREATE PRIMARY INDEX statement was executed'}

        elif query_type == 'select':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="SELECT * FROM " + self.query_buckets[0] + " LIMIT 100")
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'SELECT * FROM ' + self.query_buckets[0] + ' LIMIT 100',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL SELECT statement was executed'}

        elif query_type == 'explain':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="EXPLAIN SELECT * FROM " + self.query_buckets[0])
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'EXPLAIN statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'EXPLAIN SELECT * FROM ' + self.query_buckets[0],
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL EXPLAIN statement was executed'}

        elif query_type == 'prepare':
            prepared_name = self.gen_vacant_prepared_name("a")
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="prepare {0} from select * from {1}".format(prepared_name, self.query_buckets[0]))
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'PREPARE statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'prepare {0} from select * from {1}'.format(prepared_name,
                                                                                         self.query_buckets[0]),
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL PREPARE statement was executed'}

        elif query_type == 'adhoc_false':
            prepared_name = self.gen_vacant_prepared_name("a")
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='prepare {0}'.format(prepared_name) + ' from INFER ' + self.query_buckets[
                0] + ' WITH {"sample_size":10000,"num_sample_values":1,"similarity_metric":0.0}')
            self.run_cbq_query(query="execute {0}".format(prepared_name))
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': False,
                                'name': 'INFER statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'prepare {0}'.format(prepared_name) + ' from INFER ' + self.query_buckets[
                                    0] + ' WITH {"sample_size":10000,"num_sample_values":1,"similarity_metric":0.0}',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'preparedId': '{0}'.format(prepared_name),
                                'description': 'A N1QL INFER statement was executed'}

        elif query_type == 'unrecognized':
            if self.filter:
                self.execute_filtered_query()
            try:
                self.run_cbq_query(query="selec * fro " + self.query_buckets[0])
            except CBQError:
                expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'fatal',
                                    'isAdHoc': True,
                                    'name': 'UNRECOGNIZED statement', 'real_userid': {'source': source, 'user': user},
                                    'statement': 'selec * fro ' + self.query_buckets[0],
                                    'userAgent': 'Python-httplib2/0.13.1 (gzip)',
                                    'id': self.eventID,
                                    'description': 'An unrecognized statement was received by the N1QL query engine'}

        elif query_type == 'insert':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(
                query='INSERT INTO ' + self.query_buckets[0] + ' ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                                               '"order", "customer_id":"24601", "total_price": 30.3, '
                                                               '"lineitems": '
                                                               '[ "11", "12", "13" ] })')
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'INSERT statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'INSERT INTO ' + self.query_buckets[0] +
                                             ' ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", '
                                             '"type": '
                                             '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                             '[ "11", "12", "13" ] })',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL INSERT statement was executed'}

        elif query_type == 'upsert':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(
                query='UPSERT INTO ' + self.query_buckets[0] + ' ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                                               '"order", "customer_id":"24601", "total_price": 30.3, '
                                                               '"lineitems": '
                                                               '[ "11", "12", "13" ] })')
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'UPSERT statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'UPSERT INTO ' + self.query_buckets[0] +
                                             ' ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", '
                                             '"type": '
                                             '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                             '[ "11", "12", "13" ] })',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL UPSERT statement was executed'}

        elif query_type == 'delete':
            if self.filter:
                self.audit_codes.remove(self.eventID)
                self.set_audit(set_disabled=True)
                try:
                    self.run_cbq_query(query="selec * fro " + self.query_buckets[0] + "", server=self.servers[1])
                except CBQError:
                    self.log.info("Query is unrecognized (expected)")
            self.run_cbq_query(query='DELETE FROM ' + self.query_buckets[1] + ' WHERE type = "hotel"', server=self.servers[1])
            expected_results = {'node': '%s:%s' % (self.servers[1].ip, self.servers[1].port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'DELETE statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'DELETE FROM ' + self.query_buckets[1] + ' WHERE type = "hotel"',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL DELETE statement was executed'}

        elif query_type == 'update':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='UPDATE ' + self.query_buckets[1] + ' SET foo = 5')
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'UPDATE statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'UPDATE ' + self.query_buckets[1] + ' SET foo = 5',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL UPDATE statement was executed'}

        elif query_type == 'merge':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query='MERGE INTO ' + self.query_buckets[1] + ' t USING [{"id":"21728"},{"id":"21730"}] source '
                                     'ON KEY "hotel_"|| source.id WHEN MATCHED THEN UPDATE SET t.old_vacancy = '
                                     't.vacancy '
                                     ', t.vacancy = false RETURNING meta(t).id, t.old_vacancy, t.vacancy')
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'MERGE statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'MERGE INTO ' + self.query_buckets[1] + ' t USING [{"id":"21728"},{"id":"21730"}] '
                                             'source '
                                             'ON KEY "hotel_"|| source.id WHEN MATCHED THEN UPDATE SET t.old_vacancy '
                                             '= t.vacancy '
                                             ', t.vacancy = false RETURNING meta(t).id, t.old_vacancy, t.vacancy',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL MERGE statement was executed'}

        elif query_type == 'grant':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="GRANT query_external_access TO query")
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'GRANT ROLE statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'GRANT query_external_access TO query',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL GRANT ROLE statement was executed'}

        elif query_type == 'revoke':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(query="REVOKE query_system_catalog FROM query")
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                                'isAdHoc': True,
                                'name': 'REVOKE ROLE statement', 'real_userid': {'source': source, 'user': user},
                                'statement': 'REVOKE query_system_catalog FROM query',
                                'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                                'description': 'A N1QL REVOKE ROLE statement was executed'}

        elif query_type == 'no_select':
            cbqpath = '%scbq' % self.path + " -e %s:%s -u 'no_select' -p 'password' -q " % (
                self.master.ip, self.n1ql_port)
            query = 'select * from ' + self.query_buckets[0] + ' limit 100'
            self.shell.execute_commands_inside(cbqpath, query, '', '', '', '', '')
            expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'errors',
                                'isAdHoc': True,
                                'statement': 'select * from ' + self.query_buckets[0] + ' limit 100;',
                                'description': 'A N1QL SELECT statement was executed',
                                'real_userid': {'source': 'local', 'user': 'no_select'},
                                'userAgent': 'Go-http-client/1.1 (CBQ/2.0)',
                                'id': self.eventID, 'name': 'SELECT statement'}
        else:
            self.fail("Unexpected query_type value")
        if query_type == 'delete':
            self.checkConfig(self.eventID, self.servers[1], expected_results, n1ql_audit=True)
            if self.filter:
                self.checkFilter(self.unauditedID, self.servers[1])
        else:
            self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=True)
            if self.filter:
                self.checkFilter(self.unauditedID, self.master)

    def test_audit_create_scope_event(self):
        query_type = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'

        self.run_cbq_query(query="CREATE SCOPE default:default.test2")
        self.sleep(10)
        expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                            'isAdHoc': True,
                            'name': 'CREATE SCOPE statement', 'real_userid': {'source': source, 'user': user},
                            'statement': "CREATE SCOPE default:default.test2",
                            'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                            'description': 'A N1QL CREATE SCOPE statement was executed'}

        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=True)

    def test_audit_drop_scope_event(self):
        query_type = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'
        try:
            self.run_cbq_query(query="CREATE SCOPE default:default.test2")
            self.sleep(10)
        except Exception as e:
            self.log.info("scope already exists")
        self.run_cbq_query(query="DROP SCOPE default:default.test2")
        expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                            'isAdHoc': True,
                            'name': 'DROP SCOPE statement', 'real_userid': {'source': source, 'user': user},
                            'statement': "DROP SCOPE default:default.test2",
                            'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                            'description': 'A N1QL DROP SCOPE statement was executed'}

        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=True)

    def test_audit_create_collection_event(self):
        query_type = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'
        try:
            self.run_cbq_query(query="CREATE SCOPE default:default.test2")
            self.sleep(10)
        except Exception as e:
            self.log.info("scope already exists")
        self.run_cbq_query(query="CREATE COLLECTION default:default.test2.test1")
        self.sleep(10)
        expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                            'isAdHoc': True,
                            'name': 'CREATE COLLECTION statement', 'real_userid': {'source': source, 'user': user},
                            'statement': "CREATE COLLECTION default:default.test2.test1",
                            'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                            'description': 'A N1QL CREATE COLLECTION statement was executed'}

        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=True)

    def test_audit_drop_collection_event(self):
        query_type = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'
        try:
            self.run_cbq_query(query="CREATE SCOPE default:default.test2")
            self.sleep(10)
            self.run_cbq_query(query="CREATE COLLECTION default:default.test2.test1")
            self.sleep(10)
        except Exception as e:
            self.log.info("scope/collection already exists")

        self.run_cbq_query(query="DROP COLLECTION default:default.test2.test1")
        self.sleep(10)
        expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                            'isAdHoc': True,
                            'name': 'DROP COLLECTION statement', 'real_userid': {'source': source, 'user': user},
                            'statement': "DROP COLLECTION default:default.test2.test1",
                            'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                            'description': 'A N1QL DROP COLLECTION statement was executed'}

        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=True)

    def test_audit_query_context(self):
        query_type = self.input.param("ops", None)
        user = self.master.rest_username
        source = 'ns_server'
        self.run_cbq_query(query="CREATE SCOPE default:default.test")
        self.sleep(10)
        self.run_cbq_query(query="CREATE COLLECTION default:default.test.test1")
        self.run_cbq_query(query="CREATE COLLECTION default:default.test.test2")
        self.sleep(10)
        self.run_cbq_query(query="CREATE INDEX idx1 on default:default.test.test1(name)")
        self.sleep(10)
        self.run_cbq_query(
            query='INSERT INTO default:default.test.test1 (KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "old hotel" })')
        self.sleep(10)
        self.run_cbq_query(query="select name from test1 where name = 'old hotel'", query_context='default:default.test')
        expected_results = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success',
                            'isAdHoc': True,
                            'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                            'statement': "select name from test1 where name = 'old hotel'",
                            'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                            'description': 'A N1QL SELECT statement was executed', 'queryContext':'default:default.test' }

        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=True)

    def gen_vacant_prepared_name(self, prefix):
        vacant_prepared_name = "a"
        for i in range(1, 10000):
            query = "select count(*) from system:prepareds where name='{0}'".format(vacant_prepared_name + str(i))
            result = self.run_cbq_query(query)["results"][0]["$1"]
            if int(result) == 0:
                return vacant_prepared_name + str(i)
        raise Exception("Cannot generate vacant name for prepared statement!")

    def test_user_filter(self):
        self.set_audit(disable_user=True)
        cbqpath = '%scbq' % self.path + " -e %s:%s -u 'no_select' -p 'password' -q " % (self.master.ip, self.n1ql_port)
        query = 'select * from ' + self.query_buckets[0] + ' limit 100'
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
            curl_output = self.shell.execute_command(
                "%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabled=%s' %s"
                % (self.curl_path, 'true', ','.join(map(str, self.audit_codes)), self.audit_url))
        elif disable_user:
            curl_output = self.shell.execute_command(
                "%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabledUsers=%s' %s"
                % (self.curl_path, 'true', 'no_select/local', self.audit_url))
        else:
            curl_output = self.shell.execute_command(
                "%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabled=' %s"
                % (self.curl_path, 'true', self.audit_url))
        if "errors" in str(curl_output):
            self.log.error("Auditing settings were not set correctly")
        self.sleep(10)

    def execute_filtered_query(self):
        self.audit_codes.remove(self.eventID)
        self.set_audit(set_disabled=True)
        self.run_cbq_query(query="delete from " + self.query_buckets[0] + " limit 1")
