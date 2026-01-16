import paramiko
from membase.api.rest_client import RestConnection
import testconstants
import time
import json
import logger
import os
from subprocess import Popen, PIPE
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import re

log = logger.Logger.get_logger()


class AdvancedQueryTests(QueryTests):
    def setUp(self):
        super(AdvancedQueryTests, self).setUp()
        self.use_rest = False
        self.cbqpath = '{0}cbq -quiet -u {1} -p {2} -e=localhost:8093 '.format(self.path, self.username, self.password)

    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        super(AdvancedQueryTests, self).tearDown()

    def test_url(self):
        '''
        Description: This test will ensure that the commandline cbq command can and will connect to the valid URLs
        and will through an error if the URL is invalid.

        Steps:
        1. Create list of URLs that should work and a list of URLs the should not work
        2. Send cbq command to remote shell on each server. The command will use a URL with the -e parameter

        Author: Korrigan Clark
        Date Modified: 26/07/2017
        '''
        ###
        prefixes = ['http://', 'https://', 'couchbase://', 'couchbases://']
        ips = ['localhost', '127.0.0.1'] + [str(server.ip) for server in self.servers]
        ports = [':8091', ':8093', ':18091', ':18093']

        pass_urls = []

        # creates url, port tuples that should be valid.
        # port will be used to verify it connected to the proper endpoint
        for prefix in prefixes:
            for ip in ips:
                pass_urls.append((ip, '8091'))
                if prefix == 'couchbase://':
                    pass_urls.append((prefix+ip, '8091'))
                if prefix == 'couchbases://':
                    pass_urls.append((prefix+ip, '18091'))
                for port in ports:
                    if prefix == 'http://' and port in ['8091', '8093']:
                        pass_urls.append((prefix+ip+port, port))
                    if prefix == 'https://' and port in ['18091', '18093']:
                        pass_urls.append((prefix+ip+port, port))

        fail_urls = []

        # creates urls that should not work, either wrong prefix/prot combo or invalid url
        for prefix in prefixes:
            for ip in ips:
                for port in ports:
                    if prefix == 'http://' and port in ['18091', '18093']:
                        fail_urls.append(prefix+ip+port)
                        fail_urls.append(prefix+ip+port+'!')
                        fail_urls.append(prefix+ip+'!'+port)
                    if prefix == 'https://' and port in ['8091', '8093']:
                        fail_urls.append(prefix+ip+port)
                        fail_urls.append(prefix+ip+port+'!')
                        fail_urls.append(prefix+ip+'!'+port)
                    if prefix == 'couchbase://':
                        fail_urls.append(prefix+ip+port)
                    if prefix == 'couchbases://':
                        fail_urls.append(prefix+ip+port)

        # run through all servers and try to connect cbq to the given url
        for server in self.servers:
            for bucket in self.buckets:
                shell = RemoteMachineShellConnection(server)
                try:
                    for url in pass_urls:
                        cmd = self.path+'cbq  -u=Administrator -p=password -e='+url[0]+' -no-ssl-verify=true'
                        o = shell.execute_commands_inside(cmd, '', ['select * from system:nodes;', '\quit;'], '', '', '', '')
                        self.assertTrue(url[1] in o)

                    for url in fail_urls:
                        cmd = self.path+'cbq  -u=Administrator -p=password -e='+url+' -no-ssl-verify=true'
                        o = shell.execute_commands_inside(cmd, '', ['select * from system:nodes;', '\quit;'], '', '', '', '')
                        self.assertTrue('status:FAIL' in o)
                finally:
                    shell.disconnect()

    def test_ipv6(self):
        prefixes = ['http://', 'https://', 'couchbase://', 'couchbases://']
        ips = ['[::1]']
        ports = [':8091', ':8093', ':18091', ':18093']

        pass_urls = []

        # creates url, port tuples that should be valid.
        # port will be used to verify it connected to the proper endpoint
        for prefix in prefixes:
            for ip in ips:
                pass_urls.append((ip, '8091'))
                if prefix == 'couchbase://':
                    pass_urls.append((prefix+ip, '8091'))
                if prefix == 'couchbases://':
                    pass_urls.append((prefix+ip, '18091'))
                for port in ports:
                    if prefix == 'http://' and port in ['8091', '8093']:
                        pass_urls.append((prefix+ip+port, port))
                    if prefix == 'https://' and port in ['18091', '18093']:
                        pass_urls.append((prefix+ip+port, port))

        # run through all servers and try to connect cbq to the given url
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            try:
                for url in pass_urls:
                    cmd = self.path+'cbq  -u=Administrator -p=password -e='+url[0]+' -no-ssl-verify=true'
                    o = shell.execute_commands_inside(cmd, '', ['select * from system:nodes;', '\quit;'], '', '', '', '')
                    self.assertTrue(url[1] in o)
            finally:
                shell.disconnect()

    def test_engine_postive(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = self.execute_commands_inside(self.cbqpath, '\quit', '', '', '', '', '', '')
                    if self.analytics:
                        self.query = '\quit'
                        self.run_cbq_query()
                    self.assertTrue(o is '')
                finally:
                    shell.disconnect()


    def test_shell_error(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = self.execute_commands_inside(self.cbqpath, '\quit1', '', '', '', '', '')
                    if self.analytics:
                        self.query = '\quit1'
                        self.run_cbq_query()
                    self.assertTrue("Command does not exist" in o)
                finally:
                    shell.disconnect()

    def test_engine_ne(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = self.execute_commands_inside('%s/cbq  -q -ne' % (self.path), 'select * from %s' % bucket.name, '', '', '', '', '')
                    self.assertTrue("Not connected to any cluster" in o)
                    o = self.execute_commands_inside('%s/cbq -q -ne' % (self.path), '\SET', '', '', '', '', '')
                    if self.analytics:
                        self.query = '\SET'
                        self.run_cbq_query()
                    self.assertTrue("histfileValue" in o)
                finally:
                    shell.disconnect()

    # rest parameter timeout, give with - option for rest parameters
    def test_timeout(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            username = self.rest.username
            password = self.rest.password
            try:
                queries = ['\set -timeout "10ms";', "select * from default;"]
                o = self.execute_commands_inside(self.cbqpath,
                                                  '', queries, '', '', '', '')
                if self.analytics:
                    self.query = '\set -timeout "10ms"'
                    self.run_cbq_query()
                    self.query = 'select * from default'
                    self.run_cbq_query()
                self.assertTrue("timeout" in o or "Timeout" in o)
            finally:
                shell.disconnect()


    # difference combinations of username/password and creds
    def check_onesaslbucket_auth(self):
        # No longer valid test as sasl password is no longer used
        return


    def check_multiple_saslbuckets_auth(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            queries = ['\set -creds bucket0:pass,bucket1:pass;', 'create primary index on bucket0;', 'create primary index on bucket1;', 'select count(*) from bucket0  union all select count(*) from bucket1;']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path), '', queries, 'bucket1', 'password', 'bucket0', '' )
            self.assertTrue("requestID" in o)
            queries = ['SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path), '', queries, 'bucket1', 'password', 'bucket0', '' )
            self.assertTrue("AuthorizationFailed" in o)
            queries = ['\set -creds bucket0:pass,bucket1:pass;', 'SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0" limit 10;']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '' )

            self.assertTrue("requestID" in o)

            queries = ['\set -creds Administrator:pass;', 'select * from bucket1 union all select * from bucket2 limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds user:pass;', 'SELECT buck.email FROM  bucket1 buck LEFT JOIN bucket2 on keys "query-testemployee10153.1877827-0";']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password123', 'bucket1', ''  )

            self.assertTrue("AuthorizationFailed" in o)


            queries = ['\set -creds Administrator:pass;', 'select * from bucketname union all select * from default limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds user:pass;', 'select * from bucketname union all select * from default limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds wrong:pass1,user:pass;', 'drop primary index on bucket1;', 'drop primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '' )

            self.assertTrue("AuthorizationFailed" in o)

            queries = ['\set -creds user1:pass1,'':pass2;', 'create primary index on bucket1;', 'create primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '')

            self.assertTrue("Usernamemissingin" in o)
            queries = ['\set -creds '':pass1,'':pass2;', 'drop primary index on bucket1;', 'drop primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, 'bucket0', 'password', 'bucket1', '' )

            self.assertTrue("Usernamemissingin" in o)


    def test_version(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            o = self.execute_commands_inside('%s/cbq --version' % (self.path), '', '', '', '', '', '' )
            o = self.execute_commands_inside('%s/cbq -s="\HELP VERSION"' % (self.path), '', '', '', '', '', '' )
            print(o)

    def test_exit_on_error(self):
        for bucket in self.buckets:
            try:
                o = self.shell.execute_command('%s/cbq  -q -u %s -p %s -exit-on-error -s="\set 1" '
                                         '-s="select * from default limit 1"'
                                         % (self.path, self.username, self.password))
                self.assertTrue("Exitingonfirsterrorencountered")
            finally:
                self.shell.disconnect()

    def test_pretty_false(self):
        shell = RemoteMachineShellConnection(self.master)
        queries = ['\SET -pretty true;',
                   'select * from default limit 5;']
        pretty = self.execute_commands_inside(self.cbqpath, '', queries, '', '', '', '')
        try:
            pretty_json = json.loads(pretty)
        except Exception as ex:
            self.log.error("Incorrect JSON object is "+str(pretty)+"")
        pretty_size = pretty_json['metrics']['resultSize']
        queries = ['\SET -pretty false;',
                   'select * from default limit 5;']
        ugly = self.execute_commands_inside(self.cbqpath, '', queries, '', '', '', '')
        ugly_json = json.loads(ugly)
        ugly_size = ugly_json['metrics']['resultSize']
        self.assertTrue(pretty_size > ugly_size)

    def test_connect_disconnect(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\connect http://localhost:8091;', 'drop primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                # wrong disconnect
                queries = ['\disconnect http://localhost:8091;', 'create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("Too many input arguments to command" in o)
                #wrong port
                queries = ['\connect http://localhost:8097;', 'create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("Unable to connect to endpoint" in o)
                #wrong url including http
                queries = ['\connect http://localhost345:8097;', 'create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("Unable to connect to endpoint" in o)
                #wrong url not including http
                queries = ['\connect localhost3458097;', 'create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("Unable to connect to endpoint" in o)
                queries = ['\disconnect', 'drop primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("Too many input arguments to command" in o)
                queries = ['\disconnect', 'create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("Too many input arguments to command" in o)
                queries = ['\connect http://localhost:8091;', 'create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                self.assertTrue("The index #primary already exists." in o)

    def test_history(self):
         for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            type2 = shell.extract_remote_info().distribution_type
            queries = []
            queries2 = []
            queries3 = []
            queries5 = []
            queries6 = []
            for bucket in self.buckets:
                if type2.lower() == 'windows':
                    queries = ["\set histfile c:\\tmp\\history.txt;"]
                    queries2 = ["\Alias p c:\\tmp\\history2.txt;"]
                    queries3 = ["\set $a c:\\tmp\\history3.txt;"]
                    queries5 = ['\set $a "\\abcde";']
                    queries6 = ["\set $a '\\abcde';"]
                elif type2.lower() == "linux":
                    queries = ["\set histfile /tmp/history;"]
                    queries2 = ["\Alias p /tmp/history2;"]
                    queries3 = ["\set $a /tmp/history3;"]
                    queries5 = ['\set $a "/abcde";']
                    queries6 = ["\set $a /abcde;"]

                queries.extend(['\ALIAS tempcommand create primary index on bucketname;', '\\\\tempcommand;', '\ALIAS tempcommand2 select * from bucketname limit 1;', '\\\\tempcommand2;', '\ALIAS;', '\echo \\\\tempcommand;', '\echo \\\\tempcommand2;', '\echo histfile;'])
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, '' )
                if type2.lower() == "linux":
                    self.assertTrue('/tmp/history' in o)

                queries2.extend(["\set histfile \\\\p;", "\echo histfile;", "\set histfile '\\\\p';", "\echo histfile;"])
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries2, '', '', bucket.name, '' )

                if type2.lower() == "linux":
                    self.assertTrue('/tmp/history2' in o)
                    self.assertTrue('\\p' in o)

                queries3.extend(["\set histfile $a;", "\echo histfile;"])
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries3, '', '', bucket.name, '' )


                queries4 = ["\push histfile newhistory.txt;", "\echo histfile;", '\ALIAS tempcommand create primary index on bucketname;', '\\\\tempcommand;', '\ALIAS tempcommand2 select * from bucketname limit 1;', '\\\\tempcommand2;', '\ALIAS;', '\echo \\\\tempcommand;', '\echo \\\\tempcommand2;', '\echo histfile;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries4, '', '', bucket.name, '' )


                queries5.append("\echo $a;")
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries5, '', '', bucket.name, '' )


                queries6.append("\echo $a;")
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries6, '', '', bucket.name, '' )

    def test_alias_and_echo(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            type2 = shell.extract_remote_info().distribution_type
            for bucket in self.buckets:
                queries = ["\ALIAS tempcommand create primary index on bucketname;", "\\\\tempcommand;", '\ALIAS tempcommand2 select *,email from bucketname limit 10;', "\\\\tempcommand2;", '\ALIAS;', '\echo \\\\tempcommand1;', '\echo \\\\tempcommand2;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, '' )
                self.assertTrue("ERROR 141 : Alias does not exist  tempcommand1" in o)
                queries = ['\ALIAS tempcommand drop primary index on bucketname;', '\\\\tempcommand;', '\ALIAS tempcommand create primary index on bucketname;', '\ALIAS tempcommand2 drop primary index on bucketname;', '\\\\tempcommand;', '\\\\tempcommand2;', '\ALIAS;', '\echo \\\\tempcommand;', '\echo \\\\tempcommand2;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, '' )
                queries = ['\\UNALIAS tempcommand drop primary index on bucketname;', '\\\\tempcommand;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, '' )
                if type2.lower() == "windows":
                   print(o)
                else:
                    self.assertTrue("Alias does not exist" in o)
                queries = ['\\UNALIAS tempcommand;', '\\\\tempcommand;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, '' )
                self.assertTrue("Alias does not exist" in o)

    def test_positional_params(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\SET -args [7, 0,1,2011];', 'prepare temp from SELECT tasks_points.task1 AS task from bucketname WHERE join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4)) ORDER BY tasks_points.task1 ;', 'execute temp;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '')
                # Test neeeds to be finished
    
    def test_prepared_auto_execute(self):
        for bucket in self.buckets:
            queries = ['\set -auto_execute true;','\set -$a 1;','prepare p1 from select $a;','execute p1;','\set -auto_execute false;','execute p1;','\set -auto_execute true;','execute p1;']
            o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '')
            self.assertTrue('"errors"' not in str(o), f"Errors should not be present, please check {o}")
            self.assertTrue('[{"$1": 1}]' in str(o), f"we expect these results from the queries, please check {o}")

    def test_query_context(self):
        queries = ['\SET -query_context default.test;',
                   'select name from test1 b where b.name="old hotel"']
        o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
        self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o,
                        "Results are incorrect : {0}".format(o))

    def test_query_collection(self):
        queries = ['select name from default:default.test.test1 b where b.name="old hotel"']
        o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
        self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o,
                        "Results are incorrect : {0}".format(o))

    def test_context_join(self):
        queries = ['\SET -query_context default:default.test;',
                   'select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"']
        o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
        self.assertTrue('{"t1": {"name": "new hotel","type": "hotel"},"t2": {"name": "new hotel","type": "hotel"}}' in o,
                        "Results are incorrect : {0}".format(o))

    def test_ddl_collections(self):
        queries = ['\SET -query_context default:default.test;',
                   'INSERT INTO {0}'.format(self.collections[0]) + '(KEY, VALUE) VALUES ("key10", { "type" : "hotel", "name" : "new hotel" })']
        o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
        self.assertTrue('"mutationCount": 1' in o)

    def test_batch_queries(self):
        queries = ['\SET -query_context default.test;',
                   'select name from test1 b where b.name="old hotel";','select name from test2 b1 where b1.name="new hotel";']
        o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
        self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o,
                        "Results are incorrect : {0}".format(o))
        self.assertTrue('{"name": "new hotel"}' in o,
                        "Results are incorrect : {0}".format(o))

    def test_named_params(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\SET -$join_day 2;', '\SET -$project "AB";', 'prepare temp from select name, tasks_ids,join_day from bucketname where join_day>=$join_day and tasks_ids[0] IN (select ARRAY_AGG(DISTINCT task_name) as names from bucketname d use keys ["test_task-1", "test_task-2"] where project!=$project)[0].names;', 'execute temp;', '\quit;']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
                # Test needs to be finished

    def test_push_pop_set(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                i=1
                pushqueries=['\set -$project "AB";', '\push -$project "CD";', '\push -$project "EF";', '\push -$project "GH";', 'select $project;']
                o = self.execute_commands_inside(self.cbqpath, '', pushqueries, '', '', bucket.name, '' )
                self.assertTrue('{"$1": "GH"}' in o)
                pushqueries.append('\pop;')
                pushqueries.append('select $project;')
                o = self.execute_commands_inside(self.cbqpath, '', pushqueries, '', '', bucket.name, '' )
                self.assertTrue('{"$1": "EF"}' in o)

                popqueries=['\pop;', 'select $project;']
                o = self.execute_commands_inside(self.cbqpath, '', popqueries, '', '', bucket.name, '' )
                self.assertTrue('Error evaluating projection' in o)

                popqueries.extend(['\push -$project "CD";', '\push -$project "EF";', '\push -$project "GH";', '\pop -$project;', '\pop;', 'select $project;'])
                o = self.execute_commands_inside(self.cbqpath, '', popqueries, '', '', bucket.name, '' )
                self.assertTrue('{"$1": "CD"}' in o)
                popqueries.append('\pop -$project;')
                popqueries.append('select $project;')
                self.assertTrue('Error evaluating projection' in o)
                popqueries.extend(['\set -$project "AB";', '\push -$project "CD";', '\push -$project "EF";', '\pop;', '\\unset -$project;', 'select $project;'])
                o = self.execute_commands_inside(self.cbqpath, '', popqueries, '', '', bucket.name, '' )
                self.assertTrue('Error evaluating projection' in o)


                while(i<15):
                    pushqueries.append('\SET -args [7, 0,1,2011];')
                    pushqueries.append('\push;')
                    pushqueries.append('\SET -$join_day %s;' %i)
                    pushqueries.append('\push -$join_day %s;' %i)
                    pushqueries.append('\push -args [8,1,2,2011];')
                    pushqueries.append('select $join_day;');
                    pushqueries.append('\SET -$project "AB";')
                    pushqueries.append('\push;')
                    pushqueries.append('\push  -$project "CD";')
                    pushqueries.append('select  $project;')
                    pushqueries.append('prepare temp from select  tasks_points.task1 AS task from bucketname where join_day>=$join_day and  join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4));')
                    pushqueries.append('execute temp;')
                    pushqueries.append('\set;')
                    i=i+1
                    o = self.execute_commands_inside(self.cbqpath, '', pushqueries, '', '', bucket.name, '' )
                i=1
                popqueries =[]
                while(i<10):
                    popqueries.append('\SET;')
                    popqueries.append('\pop;')
                    popqueries.append('\pop -args;')
                    popqueries.append('\pop -$join_day;')
                    popqueries.append('select $join_day;');
                    popqueries.append('\pop -$project;')
                    popqueries.append('\SET;')
                    popqueries.append('prepare temp from select tasks_points.task1 AS task from bucketname where join_day>=$join_day and  join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4));')
                    popqueries.append('execute temp;')
                    i=i+1
                    o = self.execute_commands_inside(self.cbqpath, '', popqueries, '', '', bucket.name, '' )

    def test_redirect(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\\redirect abc;', 'create primary index on bucketname;', 'select name,tasks_points.task1,skills from bucketname;', '\\redirect off;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, '' )
                sftp = shell._ssh_client.open_sftp()
                fileout = sftp.open("abc", 'r')
                filedata = fileout.read()
                print(filedata)
                queries = ['drop primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path), '', queries, '', '', bucket.name, True )
                print(o)
