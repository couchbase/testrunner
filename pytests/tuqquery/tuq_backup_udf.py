from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from membase.api.exception import CBQError
from collection.collections_n1ql_client import CollectionsN1QL

class QueryBackupUDFTests(QueryTests):

    def setUp(self):
        super(QueryBackupUDFTests, self).setUp()
        self.log.info("==============  QueryFilterTests setup has started ==============")
        self.include = self.input.param("include", "bucket2")
        self.exclude = self.input.param("exclude", "bucket1")
        self.map = self.input.param("map", "bucket1=bucket1b")
        self.compact = self.input.param("compact", False)
        self.disable = self.input.param("disable", None)
        self.role = self.input.param("role", None)

        self.backup_user = self.username
        self.backup_password = self.password

        self.log.info("==============  QueryBackupUDFTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryBackupUDFTests, self).suite_setUp()
        self.log.info("==============  QueryBackupUDFTests suite_setup has started ==============")
        bucket1 = self.get_bucket_from_name("bucket1")
        if not bucket1:
            self.rest.create_bucket(bucket="bucket1", ramQuotaMB=200, replicaNumber=0)
        bucket2 = self.get_bucket_from_name("bucket2")
        if not bucket2:
            self.rest.create_bucket(bucket="bucket2", ramQuotaMB=200, replicaNumber=0)
        self.collections_helper = CollectionsN1QL(self.master)
        self.collections_helper.create_scope(bucket_name="bucket1",scope_name="scope1")
        self.collections_helper.create_collection(bucket_name="bucket1",scope_name="scope1",collection_name="collection1")
        self.collections_helper.create_scope(bucket_name="bucket2",scope_name="scope2a")
        self.collections_helper.create_scope(bucket_name="bucket2",scope_name="scope2b")
        self.collections_helper.create_collection(bucket_name="bucket2",scope_name="scope2a",collection_name="collection2a")
        self.collections_helper.create_collection(bucket_name="bucket2",scope_name="scope2b",collection_name="collection2b")
        # add JS library
        functions = 'function adder(a, b, c) { for (i=0; i< b; i++){a = a + c;} return a; }'
        function_names = ["adder"]
        self.log.info("Create math library")
        self.create_library("math", functions, function_names)
        self.log.info("==============  QueryBackupUDFTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryBackupUDFTests tearDown has started ==============")
        self.log.info("==============  QueryBackupUDFTests tearDown has completed ==============")
        super(QueryBackupUDFTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryBackupUDFTests suite_tearDown has started ==============")
        self.log.info("==============  QueryBackupUDFTests suite_tearDown has completed ==============")
        super(QueryBackupUDFTests, self).suite_tearDown()

    def backup_config(self, archive="/backup-1", repo="my_backup"):
        shell = RemoteMachineShellConnection(self.master)
        output = shell.execute_command(f"{self.path}/cbbackupmgr remove -a {archive} -r {repo}")
        output = shell.execute_command(f"{self.path}/cbbackupmgr config -a {archive} -r {repo}")

    def backup_info(self, archive="/backup-1", repo="my_backup"):
        shell = RemoteMachineShellConnection(self.master)
        output = shell.execute_command(f"{self.path}/cbbackupmgr info -a {archive} -r {repo} -j")
        return self.convert_list_to_json(output[0])

    def backup_merge(self, archive="/backup-1", repo="my_backup", start=1, end=2):
        shell = RemoteMachineShellConnection(self.master)
        output = shell.execute_command(f"{self.path}/cbbackupmgr merge -a {archive} -r {repo} --start {start} --end {end}")
        return output

    def backup(self, archive="/backup-1", repo="my_backup"):
        shell = RemoteMachineShellConnection(self.master)
        output = shell.execute_command(f"{self.path}/cbbackupmgr backup -a {archive} -r {repo} -c http://{self.master.ip}:8091 -u {self.backup_user} -p {self.backup_password}")
        if self.compact:
            self.log.info("Compacting backup")
            compact = shell.execute_command(f"{self.path}/cbbackupmgr compact -a {archive} -r {repo} --backup latest")
            self.log.info(compact)
        return output

    def restore(self, archive="/backup-1", repo="my_backup", disable=None, start=None, end=None, include=None, exclude=None, map=None):
        bar_command=f"{self.path}/cbbackupmgr restore -a {archive} -r {repo} -c http://{self.master.ip}:8091 -u {self.backup_user} -p {self.backup_password}"
        if include:
            bar_command += f" --include-data {include}"
        if exclude:
            bar_command += f" --exclude-data {exclude}"
        if map:
            bar_command += f" --map-data {map} --auto-create-buckets"
        if start:
            bar_command += f" --start {start}"
        if end:
            bar_command += f" --end {end}"
        if disable:
            bar_command += f" --disable-{disable}-query"
        shell = RemoteMachineShellConnection(self.master)
        output = shell.execute_command(bar_command)
        return output

    def drop_udf(self):
        result = self.run_cbq_query("select f.identity.name, f.identity.type, f.identity.`bucket`, f.identity.`scope` from system:functions as f")
        udfs = result['results']
        for udf in udfs:
            if udf['type'] == 'scope':
                self.run_cbq_query(f"DROP FUNCTION default:{udf['bucket']}.{udf['scope']}.{udf['name']}")
            else:
                self.run_cbq_query(f"DROP FUNCTION {udf['name']}")
    
    def create_udf(self):
        self.drop_udf()
        UDFS = [
            "func_global",
            "bucket2.scope2a.scope2a_func",
            "bucket2.scope2b.scope2b_func"
        ]
        for udf in UDFS:
            try:
                self.run_cbq_query(f"CREATE FUNCTION default:{udf}() {{ 0 }}")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 10102)
        self.run_cbq_query('CREATE FUNCTION default:bucket1.scope1.scope1_func(a,b,c) LANGUAGE JAVASCRIPT AS "adder" AT "math"')

    def test_backup_info(self):
        self.create_udf()
        self.backup_config()
        self.backup()
        info = self.backup_info()
        global_count = info['backups'][0]['query_udfs']
        self.assertEqual(global_count, 1)
        scope_count = {}
        for bucket in info['backups'][0]['buckets']:
            scope_count[bucket['name']] = bucket['query_udfs']
        self.assertEqual(scope_count['bucket1'], 1)
        self.assertEqual(scope_count['bucket2'], 2)

    def test_restore_all(self):
        if self.role:
            self.users = [{"id": "jackDoe", "name": "Jack Downing", "password": "password1"}]
            self.create_users()
            self.backup_user, self.backup_password = self.users[0]['id'], self.users[0]['password']
            self.run_cbq_query(query=f"GRANT {self.role} on bucket1,bucket2,default to {self.backup_user}")
            self.run_cbq_query(query=f"GRANT query_system_catalog to {self.backup_user}")

        self.create_udf()
        self.backup_config()
        self.backup()
        self.drop_udf()
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        self.log.info(result['results'])
        self.assertEqual(result['results'], [])
        output = self.restore(disable=self.disable)
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        expected_udf = [{'name': 'func_global'}, {'name': 'scope1_func'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}]
        if self.disable == "bucket":
            expected_udf = [{'name': 'func_global'}]
        elif self.disable == "cluster":
             expected_udf = [{'name': 'scope1_func'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}]
        self.assertEqual(result['results'], expected_udf)

    def test_restore_include(self):
        self.create_udf()
        self.backup_config()
        self.backup()
        self.drop_udf()
        result = self.run_cbq_query("select f.identity.name from system:functions as f")
        self.log.info(f"result is {result['results']}")
        self.assertEqual(result['results'], [])
        output = self.restore(include=self.include)
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        expected_udf = {
            'bucket1': [{'name': 'func_global'}, {'name': 'scope1_func'}],
            'bucket2': [{'name': 'func_global'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}],
            'bucket1.scope1': [{'name': 'func_global'}, {'name': 'scope1_func'}],
            'bucket2.scope2a': [{'name': 'func_global'}, {'name': 'scope2a_func'}],
            'bucket2.scope2b': [{'name': 'func_global'}, {'name': 'scope2b_func'}],
            'bucket2.scope2a,bucket2.scope2b': [{'name': 'func_global'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}],
            'bucket1.scope1.collection1': [{'name': 'func_global'}]
        }
        self.include = self.include.strip('"')
        self.assertEqual(result['results'], expected_udf[self.include])

    def test_restore_exclude(self):
        self.create_udf()
        self.backup_config()
        self.backup()
        self.drop_udf()
        result = self.run_cbq_query("select f.identity.name from system:functions as f")
        self.log.info(f"result is {result['results']}")
        self.assertEqual(result['results'], [])
        output = self.restore(exclude=self.exclude)
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        expected_udf = {
            'bucket1': [{'name': 'func_global'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}],
            'bucket2': [{'name': 'func_global'}, {'name': 'scope1_func'}],
            'bucket1.scope1': [{'name': 'func_global'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}],
            'bucket2.scope2a': [{'name': 'func_global'}, {'name': 'scope1_func'}, {'name': 'scope2b_func'}],
            'bucket2.scope2b': [{'name': 'func_global'}, {'name': 'scope1_func'}, {'name': 'scope2a_func'}],
            'bucket2.scope2a,bucket2.scope2b': [{'name': 'func_global'}, {'name': 'scope1_func'}],
            'bucket1.scope1.collection1': [{'name': 'func_global'}, {'name': 'scope1_func'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}]
        }
        self.exclude = self.exclude.strip('"')
        self.assertEqual(result['results'], expected_udf[self.exclude])

    def test_restore_map(self):
        self.map = self.map.replace(":","=")
        self.create_udf()
        self.backup_config()
        self.backup()
        self.drop_udf()
        result = self.run_cbq_query("select f.identity.name from system:functions as f")
        self.log.info(f"result is {result['results']}")
        self.assertEqual(result['results'], [])
        output = self.restore(map=self.map)
        result = self.run_cbq_query("select f.identity.name, f.identity.`bucket`, f.identity.`scope` from system:functions as f order by f.identity.name")
        expected_udf = {
            'bucket1=bucket1b': [
                {'name': 'func_global'},
                {'bucket': 'bucket1b', 'name': 'scope1_func', 'scope': 'scope1'},
                {'bucket': 'bucket2', 'name': 'scope2a_func', 'scope': 'scope2a'}, 
                {'bucket': 'bucket2', 'name': 'scope2b_func', 'scope': 'scope2b'}
            ],
            'bucket1.scope1=bucket1.scope1b': [
                {'name': 'func_global'},
                {'bucket': 'bucket1', 'name': 'scope1_func', 'scope': 'scope1b'},
                {'bucket': 'bucket2', 'name': 'scope2a_func', 'scope': 'scope2a'}, 
                {'bucket': 'bucket2', 'name': 'scope2b_func', 'scope': 'scope2b'} 
            ],
            'bucket1.scope1.collection1=bucket1.scope1.collection1b': [
                {'name': 'func_global'},
                {'bucket': 'bucket1', 'name': 'scope1_func', 'scope': 'scope1'},
                {'bucket': 'bucket2', 'name': 'scope2a_func', 'scope': 'scope2a'}, 
                {'bucket': 'bucket2', 'name': 'scope2b_func', 'scope': 'scope2b'}
            ]
        }
        self.map = self.map.strip('"')
        self.assertEqual(result['results'], expected_udf[self.map])

    def test_restore_incremental(self):
        self.backup_config()
        self.drop_udf()
        # Full backuo 1
        self.backup()
        self.create_udf()
        # Incremental backuo 2
        self.backup()
        result = self.run_cbq_query("CREATE FUNCTION default:bucket1.scope1.scope1b_func() { 0 }")
        # Incremental backup 3
        self.backup()
        # Restore Incremental 3 only
        self.drop_udf()
        output = self.restore(start=3, end=3)
        result = self.run_cbq_query("select f.identity.name, f.identity.`bucket`, f.identity.`scope` from system:functions as f order by f.identity.name")
        expected_udf = [
            {'name': 'func_global'}, 
            {'bucket': 'bucket1', 'name': 'scope1_func', 'scope': 'scope1'}, 
            {'bucket': 'bucket1', 'name': 'scope1b_func', 'scope': 'scope1'}, 
            {'bucket': 'bucket2', 'name': 'scope2a_func', 'scope': 'scope2a'}, 
            {'bucket': 'bucket2', 'name': 'scope2b_func', 'scope': 'scope2b'}
        ]
        self.assertEqual(result['results'], expected_udf)
        # Restore Incremental 2 only
        self.drop_udf()
        output = self.restore(start=2, end=2)
        result = self.run_cbq_query("select f.identity.name, f.identity.`bucket`, f.identity.`scope` from system:functions as f order by f.identity.name")
        expected_udf = [
            {'name': 'func_global'}, 
            {'bucket': 'bucket1', 'name': 'scope1_func', 'scope': 'scope1'}, 
            {'bucket': 'bucket2', 'name': 'scope2a_func', 'scope': 'scope2a'}, 
            {'bucket': 'bucket2', 'name': 'scope2b_func', 'scope': 'scope2b'}
        ]
        self.assertEqual(result['results'], expected_udf)

    def test_restore_merge(self):
        self.backup_config()
        self.drop_udf()
        # Full backuo 1
        self.backup()
        self.create_udf()
        # Incremental backuo 2
        self.backup()
        result = self.run_cbq_query("CREATE FUNCTION default:bucket1.scope1.scope1b_func() { 0 }")
        # Incremental backup 3
        self.backup()
        # Merge backups
        output = self.backup_merge(start=1, end=3)
        info = self.backuo_info()
        self.assertEqual(info['backups'][0]['type'], 'MERGE-FULL')
        # Restore merged backup
        self.drop_udf()
        output = self.restore(start=1, end=1)
        result = self.run_cbq_query("select f.identity.name, f.identity.`bucket`, f.identity.`scope` from system:functions as f order by f.identity.name")
        expected_udf = [
            {'name': 'func_global'}, 
            {'bucket': 'bucket1', 'name': 'scope1_func', 'scope': 'scope1'}, 
            {'bucket': 'bucket1', 'name': 'scope1b_func', 'scope': 'scope1'}, 
            {'bucket': 'bucket2', 'name': 'scope2a_func', 'scope': 'scope2a'}, 
            {'bucket': 'bucket2', 'name': 'scope2b_func', 'scope': 'scope2b'}
        ]
        self.assertEqual(result['results'], expected_udf)

    def test_restore_replace(self):
        self.create_udf()
        result = self.run_cbq_query("execute function default:bucket2.scope2a.scope2a_func()")
        self.assertEqual(result['results'], [0])

        self.backup_config()
        self.backup()
        self.drop_udf()
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        self.log.info(result['results'])
        self.assertEqual(result['results'], [])

        result = self.run_cbq_query("CREATE FUNCTION default:bucket2.scope2a.scope2a_func() { 10 }")
        result = self.run_cbq_query("execute function default:bucket2.scope2a.scope2a_func()")
        self.assertEqual(result['results'], [10])

        output = self.restore()
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        expected_udf = [{'name': 'func_global'}, {'name': 'scope1_func'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}]
        self.assertEqual(result['results'], expected_udf)

        result = self.run_cbq_query("execute function default:bucket2.scope2a.scope2a_func()")
        self.assertEqual(result['results'], [0])

    def test_restore_empty_bucket(self):
        self.create_udf()
        self.run_cbq_query("drop function default:bucket1.scope1.scope1_func")
        self.backup_config()
        self.backup()
        self.drop_udf()
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        self.log.info(result['results'])
        self.assertEqual(result['results'], [])
        output = self.restore(disable=self.disable)
        result = self.run_cbq_query("select f.identity.name from system:functions as f order by f.identity.name")
        expected_udf = [{'name': 'func_global'}, {'name': 'scope2a_func'}, {'name': 'scope2b_func'}]
        self.assertEqual(result['results'], expected_udf)

