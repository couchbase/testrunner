import paramiko
from membase.api.rest_client import RestConnection
import testconstants
import time
import json
import logger
import os
from subprocess import Popen, PIPE
from remote.remote_util import RemoteMachineShellConnection
from tuq import QueryTests

log = logger.Logger.get_logger()

class AdvancedQueryTests(QueryTests):
    def setUp(self):
        super(AdvancedQueryTests, self).setUp()
        self.use_rest = False
        self.cbqpath = '%scbq -quiet -u %s -p %s' % (self.path, self.username, self.password)


    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        super(AdvancedQueryTests, self).tearDown()


    def test_url(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = shell.execute_commands_inside('%s/cbq  -u=Administrator -p=password http://localhost:8091@' % (self.path),'','','','','','')
                    self.assertTrue('status:FAIL' in o)
                    o = shell.execute_commands_inside('%s/cbq  -u=Administrator -p=password http://localhost:8091:' % (self.path),'','','','','','')
                    self.assertTrue('status:FAIL' in o)
                    o = shell.execute_commands_inside('%s/cbq  -u=Administrator -p=password http://localhost:8091[' % (self.path),'','','','','','')
                    self.assertTrue('status:FAIL' in o)
                    o = shell.execute_commands_inside('%s/cbq  -u=Administrator -p=password http://localhost:8091]' % (self.path),'','','','','','')
                    self.assertTrue('status:FAIL' in o)
                    o = shell.execute_commands_inside('%s/cbq  -u=Administrator -p=password http://localhost:8091:' % (self.path),'','','','','','')
                    self.assertTrue('status:FAIL' in o)
                finally:
                    shell.disconnect()

    def test_engine_postive(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = self.execute_commands_inside(self.cbqpath,'\quit','','','','','','')
                    if self.analytics:
                        self.query = '\quit'
                        o = self.run_cbq_query()
                        print o
                    self.assertTrue(o is '')
                finally:
                    shell.disconnect()


    def test_shell_error(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = self.execute_commands_inside(self.cbqpath,'\quit1','','','','','')
                    if self.analytics:
                        self.query = '\quit1'
                        o = self.run_cbq_query()
                        print o
                    print o
                    self.assertTrue("Command does not exist" in o)
                finally:
                    shell.disconnect()

    def test_engine_ne(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = self.execute_commands_inside('%s/cbq  -q -ne' % (self.path),'select * from %s' % bucket.name,'','','','','')
                    self.assertTrue("Not connected to any cluster" in o)
                    o = self.execute_commands_inside('%s/cbq -q -ne' % (self.path),'\SET','','','','','')
                    if self.analytics:
                        self.query = '\SET'
                        o = self.run_cbq_query()
                        print o
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
                queries = ['\set -timeout "10ms";',"select * from default;"]
                o = self.execute_commands_inside(self.cbqpath,
                                                  '',queries,'','','','')
                if self.analytics:
                    self.query = '\set -timeout "10ms"'
                    self.run_cbq_query()
                    self.query = 'select * from default'
                    o = self.run_cbq_query()
                    print o
                self.assertTrue("timeout" in o)
            finally:
                shell.disconnect()

    def test_exit_on_error(self):
        shell = RemoteMachineShellConnection(self.master)
        queries = ['\set 1']
        o = self.execute_commands_inside(self.cbqpath + ' -exit-on-error', '', queries, '',
                                          '', '', '')
        import pdb; pdb.set_trace()
        print o

    # difference combinations of username/password and creds
    def check_onesaslbucket_auth(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    if (bucket.saslPassword != ''):
                        print('sasl')
                        o = shell.execute_commands_inside('%s/cbq -c %s:%s -q' % (self.path,bucket.name,bucket.saslPassword),'CREATE PRIMARY INDEX ON %s USING GSI' %bucket.name,'','','','','')
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -c %s:%s -q' % (self.path,bucket.name,bucket.saslPassword),'select *,join_day from %s limit 10'%bucket.name,'','','','','')
                        if self.analytics:
                            self.query = 'select join_day from %s limit 10'%bucket.name
                            o = self.run_cbq_query()
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -c %s:%s -q' % (self.path,bucket.name,'wrong'),'select * from %s limit 10'%bucket.name,'','','','','')
                        print o
                        self.assertTrue("AuthorizationFailed"  in o)

                        o = shell.execute_commands_inside('%s/cbq -c %s:%s -q' % (self.path,'','wrong'),'select * from %s limit 10'%bucket.name,'','','','','')
                        self.assertEqual('FAIL',o[7:])
                        o = shell.execute_commands_inside('%s/cbq -c %s:%s -q' % (self.path,'wrong',bucket.saslPassword),'select * from %s limit 10'%bucket.name,'','','','','')
                        self.assertTrue("AuthorizationFailed"  in o)

                        queries = ['\set -creds user:pass;','select *,join_day from bucketname limit 10;']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,bucket.saslPassword,bucket.name,'' )
                        self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','select * from bucketname union all select * from default limit 10;']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'Administrator','password',bucket.name,'' )
                        self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'Administrator','password',bucket.name,'' )
                        self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,bucket.saslPassword,bucket.name,'' )
                        self.assertTrue("requestID" in o)

                        queries = ['select count(*) from bucketname  union all select count(*) from default;']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'',bucket.saslPassword,bucket.name,''  )
                        self.assertTrue("AuthorizationFailed"  in o)

                        queries = ['\set -creds user:pass;','select *,email,join_day from bucketname;']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'Administrator','password',bucket.name,'' )
                        self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','create primary index on default;','select email,join_day from bucketname union all select email,join_day from default;']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,bucket.saslPassword,bucket.name,'' )
                        self.assertTrue("requestID" in o)

                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'wrong','wrong',bucket.name,'' )
                        self.assertTrue("AuthorizationFailed"  in o)
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'wrong',bucket.saslPassword,bucket.name,'' )
                        self.assertTrue("AuthorizationFailed"  in o)
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,'wrong',bucket.name,'' )
                        self.assertTrue("AuthorizationFailed"  in o)
                        o = shell.execute_commands_inside('%s/cbq -q -u=%s -p=%s' % (self.path,'Administrator','password'),'select * from %s limit 10;' %bucket.name,'','','','','' )
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -q -u=%s -p=%s' % (self.path,bucket.name,bucket.saslPassword),'select * from %s limit 10;' %bucket.name,'','','','','' )
                        self.assertTrue("requestID" in o)
                        print('nonsasl')
                        o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,'Administrator','password'),'select * from default limit 10;','','','','','' )
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,bucket.name,bucket.saslPassword),'select * from default limit 10;' ,'','','','','' )
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -q ' % (self.path),'select * from default limit 10;','','','','','' )
                        self.assertTrue("requestID" in o)
                        break;

                finally:
                    shell.disconnect()


    def check_multiple_saslbuckets_auth(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            queries = ['\set -creds bucket0:pass,bucket1:pass;','create primary index on bucket0;','create primary index on bucket1;','select count(*) from bucket0  union all select count(*) from bucket1;']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path),'',queries,'bucket1','password','bucket0','' )
            self.assertTrue("requestID" in o)
            queries = ['SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path),'',queries,'bucket1','password','bucket0','' )
            self.assertTrue("AuthorizationFailed" in o)
            queries = ['\set -creds bucket0:pass,bucket1:pass;','SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0" limit 10;']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )

            self.assertTrue("requestID" in o)

            queries = ['\set -creds Administrator:pass;','select * from bucket1 union all select * from bucket2 limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds user:pass;','SELECT buck.email FROM  bucket1 buck LEFT JOIN bucket2 on keys "query-testemployee10153.1877827-0";']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password123','bucket1',''  )

            self.assertTrue("AuthorizationFailed" in o)


            queries = ['\set -creds Administrator:pass;','select * from bucketname union all select * from default limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds user:pass;','select * from bucketname union all select * from default limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds wrong:pass1,user:pass;','drop primary index on bucket1;','drop primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0' ,'password','bucket1','' )

            self.assertTrue("AuthorizationFailed" in o)

            queries = ['\set -creds user1:pass1,'':pass2;','create primary index on bucket1;','create primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','')

            self.assertTrue("Usernamemissingin" in o)
            queries = ['\set -creds '':pass1,'':pass2;','drop primary index on bucket1;','drop primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )

            self.assertTrue("Usernamemissingin" in o)


    def test_version(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            o = self.execute_commands_inside('%s/cbq --version' % (self.path),'','','','','','' )
            print o
            o = self.execute_commands_inside('%s/cbq -s="\HELP VERSION"' % (self.path),'','','','','','' )
            print o

    def test_pretty_false(self):
        shell = RemoteMachineShellConnection(self.master)
        queries = ['\SET -pretty true;',
                   'select * from default limit 5;']
        pretty = self.execute_commands_inside(self.cbqpath,'',queries,'','','','')
        pretty_json = json.loads(pretty)
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
                queries = ['\connect http://localhost:8091;','drop primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                # wrong disconnect
                queries = ['\disconnect http://localhost:8091;','create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                self.assertTrue("Too many input arguments to command" in o)
                #wrong port
                queries = ['\connect http://localhost:8097;','create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                self.assertTrue("Connection failed" in o)
                #wrong url including http
                queries = ['\connect http://localhost345:8097;','create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                self.assertTrue("Connection failed" in o)
                #wrong url not including http
                queries = ['\connect localhost3458097;','create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                self.assertTrue("no such host" in o)
                queries = ['\disconnect','drop primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                self.assertTrue("Too many input arguments to command" in o)
                queries = ['\disconnect','create primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                self.assertTrue("Too many input arguments to command" in o)
                queries = ['\connect http://localhost:8091;','create primary index on bucketname;','drop primary index on bucketname;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                print o
                self.assertTrue("GSI CreatePrimaryIndex() - cause: Index #primary already exists." in o)

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

                queries.extend(['\ALIAS tempcommand create primary index on bucketname;','\\\\tempcommand;','\ALIAS tempcommand2 select * from bucketname limit 1;','\\\\tempcommand2;','\ALIAS;','\echo \\\\tempcommand;','\echo \\\\tempcommand2;','\echo histfile;'])
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                if type2.lower() == "linux":
                    self.assertTrue('/tmp/history' in o)

                queries2.extend(["\set histfile \\\\p;","\echo histfile;","\set histfile '\\\\p';","\echo histfile;"])
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries2,'','',bucket.name,'' )

                if type2.lower() == "linux":
                    self.assertTrue('/tmp/history2' in o)
                    self.assertTrue('\\p' in o)

                queries3.extend(["\set histfile $a;","\echo histfile;"])
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries3,'','',bucket.name,'' )


                queries4 = ["\push histfile newhistory.txt;","\echo histfile;",'\ALIAS tempcommand create primary index on bucketname;','\\\\tempcommand;','\ALIAS tempcommand2 select * from bucketname limit 1;','\\\\tempcommand2;','\ALIAS;','\echo \\\\tempcommand;','\echo \\\\tempcommand2;','\echo histfile;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries4,'','',bucket.name,'' )


                queries5.append("\echo $a;")
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries5,'','',bucket.name,'' )


                queries6.append("\echo $a;")
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries6,'','',bucket.name,'' )

    def test_alias_and_echo(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            type2 = shell.extract_remote_info().distribution_type
            for bucket in self.buckets:
                queries = ["\ALIAS tempcommand create primary index on bucketname;","\\\\tempcommand;",'\ALIAS tempcommand2 select *,email from bucketname limit 10;',"\\\\tempcommand2;",'\ALIAS;','\echo \\\\tempcommand1;','\echo \\\\tempcommand2;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("ERROR 141 : Alias does not exist  tempcommand1" in o)
                queries = ['\ALIAS tempcommand drop primary index on bucketname;','\\\\tempcommand;','\ALIAS tempcommand create primary index on bucketname;','\ALIAS tempcommand2 drop primary index on bucketname;','\\\\tempcommand;','\\\\tempcommand2;','\ALIAS;','\echo \\\\tempcommand;','\echo \\\\tempcommand2;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                queries = ['\UNALIAS tempcommand drop primary index on bucketname;','\\\\tempcommand;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                if type2.lower() == "windows":
                   print o
                else:
                    self.assertTrue("Alias does not exist" in o)
                queries = ['\UNALIAS tempcommand;','\\\\tempcommand;']
                o = self.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("Alias does not exist" in o)

    def test_positional_params(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\SET -args [7, 0,1,2011];','prepare temp from SELECT tasks_points.task1 AS task from bucketname WHERE join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4)) ORDER BY tasks_points.task1 ;','execute temp;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name ,'')
                # Test neeeds to be finished

    def test_named_params(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\SET -$join_day 2;','\SET -$project "AB";','prepare temp from select name, tasks_ids,join_day from bucketname where join_day>=$join_day and tasks_ids[0] IN (select ARRAY_AGG(DISTINCT task_name) as names from bucketname d use keys ["test_task-1", "test_task-2"] where project!=$project)[0].names;','execute temp;']
                o = self.execute_commands_inside(self.cbqpath,'',queries,'','',bucket.name,'' )
                # Test needs to be finished

    def test_push_pop_set(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                i=1
                pushqueries=['\set -$project "AB";','\push -$project "CD";','\push -$project "EF";','\push -$project "GH";','select $project;']
                o = self.execute_commands_inside(self.cbqpath,'',pushqueries,'','',bucket.name,'' )
                self.assertTrue('{"$1": "GH"}' in o)
                pushqueries.append('\pop;')
                pushqueries.append('select $project;')
                o = self.execute_commands_inside(self.cbqpath,'',pushqueries,'','',bucket.name,'' )
                self.assertTrue('{"$1": "EF"}' in o)

                popqueries=['\pop;','select $project;']
                o = self.execute_commands_inside(self.cbqpath,'',popqueries,'','',bucket.name,'' )
                self.assertTrue('Error evaluating projection' in o)

                popqueries.extend(['\push -$project "CD";','\push -$project "EF";','\push -$project "GH";','\pop -$project;','\pop;','select $project;'])
                o = self.execute_commands_inside(self.cbqpath,'',popqueries,'','',bucket.name,'' )
                self.assertTrue('{"$1": "CD"}' in o)
                popqueries.append('\pop -$project;')
                popqueries.append('select $project;')
                self.assertTrue('Error evaluating projection' in o)
                popqueries.extend(['\set -$project "AB";','\push -$project "CD";','\push -$project "EF";','\pop;','\unset -$project;','select $project;'])
                o = self.execute_commands_inside(self.cbqpath,'',popqueries,'','',bucket.name,'' )
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
                    o = self.execute_commands_inside(self.cbqpath,'',pushqueries,'','',bucket.name,'' )
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
                    o = self.execute_commands_inside(self.cbqpath,'',popqueries,'','',bucket.name,'' )

    def test_redirect(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\\redirect abc;','create primary index on bucketname;','select name,tasks_points.task1,skills from bucketname;','\\redirect off;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                sftp = shell._ssh_client.open_sftp()
                fileout = sftp.open("abc",'r')
                filedata = fileout.read()
                print filedata
                queries = ['drop primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,True )
                print o


    def execute_commands_inside(self, main_command, query, queries, bucket1, password, bucket2, source,
                                subcommands=[], min_output_size=0,
                                end_msg='', timeout=250):
        shell = RemoteMachineShellConnection(self.master)
        shell.extract_remote_info()
        filename = "/tmp/test2"
        iswin = False

        if shell.info.type.lower() == 'windows':
            iswin = True
            filename = "/cygdrive/c/tmp/test.txt"

        filedata = ""
        if not (query == ""):
            main_command = main_command + " -s=\"" + query + '"'
        elif (shell.remote and not (queries == "")):
            sftp = shell._ssh_client.open_sftp()
            filein = sftp.open(filename, 'w')
            for query in queries:
                filein.write(query)
                filein.write('\n')
            fileout = sftp.open(filename, 'r')
            filedata = fileout.read()
            fileout.close()
        elif not (queries == ""):
            f = open(filename, 'w')
            for query in queries:
                f.write(query)
                f.write('\n')
            f.close()
            fileout = open(filename, 'r')
            filedata = fileout.read()
            fileout.close()

        newdata = filedata.replace("bucketname", bucket2)
        newdata = newdata.replace("user", bucket1)
        newdata = newdata.replace("pass", password)
        newdata = newdata.replace("bucket1", bucket1)

        newdata = newdata.replace("user1", bucket1)
        newdata = newdata.replace("pass1", password)
        newdata = newdata.replace("bucket2", bucket2)
        newdata = newdata.replace("user2", bucket2)
        newdata = newdata.replace("pass2", password)

        if (shell.remote and not (queries == "")):
            f = sftp.open(filename, 'w')
            f.write(newdata)
            f.close()
        elif not (queries == ""):
            f = open(filename, 'w')
            f.write(newdata)
            f.close()
        if not (queries == ""):
            if (source):
                if iswin:
                    main_command = main_command + "  -s=\"\SOURCE " + 'c:\\\\tmp\\\\test.txt'
                else:
                    main_command = main_command + "  -s=\"\SOURCE " + filename + '"'
            else:
                if iswin:
                    main_command = main_command + " -f=" + 'c:\\\\tmp\\\\test.txt'
                else:
                    main_command = main_command + " -f=" + filename

        log.info("running command on {0}: {1}".format(self.master.ip, main_command))
        output = ""
        if shell.remote:
            stdin, stdout, stderro = shell._ssh_client.exec_command(main_command)
            time.sleep(20)
            count = 0
            for line in stdout.readlines():
                if (count >= 0):
                    output += line.strip()
                    output = output.strip()
                    if "Inputwasnotastatement" in output:
                        output = "status:FAIL"
                        break
                    if "timeout" in output:
                        output = "status:timeout"
                else:
                    count += 1
            stdin.close()
            stdout.close()
            stderro.close()
        else:
            p = Popen(main_command, shell=True, stdout=PIPE, stderr=PIPE)
            stdout, stderro = p.communicate()
            output = stdout
            print output
            time.sleep(1)
        if (shell.remote and not (queries == "")):
            sftp.remove(filename)
            sftp.close()
        elif not (queries == ""):
            os.remove(filename)

        return (output)