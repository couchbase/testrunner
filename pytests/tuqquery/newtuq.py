import logging
import json
import copy
import re
import os

import testconstants
import time
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from membase.api.exception import CBQError, ReadDocumentException
from membase.api.rest_client import RestConnection

class QueryTests(BaseTestCase):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(QueryTests, self).setUp()
        self.version = self.input.param("cbq_version", "sherlock")
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        if not self._testMethodName == 'suite_setUp' and self.input.param("cbq_version", "sherlock") != 'sherlock':
            self._start_command_line_query(self.master)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.analytics = self.input.param("analytics", False)
        self.dataset = self.input.param("dataset", "default")
        self.primary_indx_type = self.input.param("primary_indx_type", 'GSI')
        self.index_type = self.input.param("index_type", 'GSI')
        self.primary_indx_drop = self.input.param("primary_indx_drop", False)
        self.monitoring = self.input.param("monitoring", False)
        self.isprepared = False
        self.named_prepare = self.input.param("named_prepare", None)
        self.skip_primary_index = self.input.param("skip_primary_index", False)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        self.path = testconstants.LINUX_COUCHBASE_BIN_PATH
        if type.lower() == 'windows':
            self.path = testconstants.WIN_COUCHBASE_BIN_PATH
        elif type.lower() == "mac":
            self.path = testconstants.MAC_COUCHBASE_BIN_PATH
        self.threadFailure = False
        if self.primary_indx_type.lower() == "gsi":
            self.gsi_type = self.input.param("gsi_type", 'plasma')
        else:
            self.gsi_type = None
        if self.input.param("reload_data", False):
            if self.analytics:
                self.cluster.rebalance([self.master, self.cbas_node], [], [self.cbas_node], services=['cbas'])
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket=bucket,
                                          timeout=self.wait_timeout * 5)
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.load(self.gens_load, flag=self.item_flag)
            if self.analytics:
                self.cluster.rebalance([self.master, self.cbas_node], [self.cbas_node], [], services=['cbas'])
        self.gens_load = self.generate_docs(self.docs_per_day)
        if self.input.param("gomaxprocs", None):
            self.configure_gomaxprocs()
        self.gen_results = TuqGenerators(self.log, self.generate_full_docs_list(self.gens_load))
        if (self.analytics == False):
                self.create_primary_index_for_3_0_and_greater()
        if (self.analytics):
            self.setup_analytics()
            self.sleep(30, 'wait for analytics setup')

    def suite_setUp(self):
        try:
            self.load(self.gens_load, flag=self.item_flag)
            if not self.input.param("skip_build_tuq", True):
                self._build_tuq(self.master)
            self.skip_buckets_handle = True
            if (self.analytics):
                self.cluster.rebalance([self.master, self.cbas_node], [self.cbas_node], [], services=['cbas'])
                self.setup_analytics()
                self.sleep(30, 'wait for analytics setup')
        except:
            self.log.error('SUITE SETUP FAILED')
            self.tearDown()

    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        if self.analytics:
            bucket_username = "cbadminbucket"
            bucket_password = "password"
            data = 'use Default ;'
            for bucket in self.buckets:
                data += 'disconnect bucket {0} if connected;'.format(bucket.name)
                data += 'drop dataset {0} if exists;'.format(bucket.name+ "_shadow")
                data += 'drop bucket {0} if exists;'.format(bucket.name)
            filename = "file.txt"
            f = open(filename, 'w')
            f.write(data)
            f.close()
            url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
            cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
            os.system(cmd)
            os.remove(filename)
        super(QueryTests, self).tearDown()

    def suite_tearDown(self):
        if not self.input.param("skip_build_tuq", False):
            if hasattr(self, 'shell'):
                self.shell.execute_command("killall /tmp/tuq/cbq-engine")
                self.shell.execute_command("killall tuqtng")
                self.shell.disconnect()

##############################################################################################
#
#  Setup Helpers
##############################################################################################

    def setup_analytics(self):
        data = 'use Default;'
        bucket_username = "cbadminbucket"
        bucket_password = "password"
        for bucket in self.buckets:
            data += 'create bucket {0} with {{"bucket":"{0}","nodes":"{1}"}} ;'.format(
                bucket.name, self.master.ip)
            data += 'create shadow dataset {1} on {0}; '.format(bucket.name,
                                                                bucket.name + "_shadow")
            data += 'connect bucket {0} with {{"username":"{1}","password":"{2}"}};'.format(
                bucket.name, bucket_username, bucket_password)
        filename = "file.txt"
        f = open(filename, 'w')
        f.write(data)
        f.close()
        url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
        cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
        os.system(cmd)
        os.remove(filename)

    def run_active_requests(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                result = self.run_cbq_query("select * from system:active_requests")
                self.assertTrue(result['metrics']['resultCount'] == 1)
                requestId = result['requestID']
                result = self.run_cbq_query(
                    'delete from system:active_requests where requestId  =  "%s"' % requestId)
                time.sleep(20)
                result = self.run_cbq_query(
                    'select * from system:active_requests  where requestId  =  "%s"' % requestId)
                self.assertTrue(result['metrics']['resultCount'] == 0)
                result = self.run_cbq_query("select * from system:completed_requests")
                requestId = result['requestID']
                result = self.run_cbq_query(
                    'delete from system:completed_requests where requestId  =  "%s"' % requestId)
                time.sleep(10)
                result = self.run_cbq_query(
                    'select * from system:completed_requests where requestId  =  "%s"' % requestId)
                self.assertTrue(result['metrics']['resultCount'] == 0)

##############################################################################################
#
#   COMMON FUNCTIONS
##############################################################################################

    def run_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def run_query_with_subquery_select_from_template(self, query_template):
        subquery_template = re.sub(r'.*\$subquery\(', '', query_template)
        subquery_template = subquery_template[:subquery_template.rfind(')')]
        keys_num = int(re.sub(r'.*KEYS \$', '', subquery_template).replace('KEYS $', ''))
        subquery_full_list = self.generate_full_docs_list(gens_load=self.gens_load, keys=self._get_keys(keys_num))
        subquery_template = re.sub(r'USE KEYS.*', '', subquery_template)
        sub_results = TuqGenerators(self.log, subquery_full_list)
        self.query = sub_results.generate_query(subquery_template)
        expected_sub = sub_results.generate_expected_result()
        alias = re.sub(r',.*', '', re.sub(r'.*\$subquery\(.*\)', '', query_template))
        alias = re.sub(r'.*as', '', re.sub(r'FROM.*', '', alias)).strip()
        if not alias:
            alias = '$1'
        for item in self.gen_results.full_set:
            item[alias] = expected_sub[0]
        query_template = re.sub(r',.*\$subquery\(.*\).*%s' % alias, ',%s' % alias, query_template)
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def run_query_with_subquery_from_template(self, query_template):
        subquery_template = re.sub(r'.*\$subquery\(', '', query_template)
        subquery_template = subquery_template[:subquery_template.rfind(')')]
        subquery_full_list = self.generate_full_docs_list(gens_load=self.gens_load)
        sub_results = TuqGenerators(self.log, subquery_full_list)
        self.query = sub_results.generate_query(subquery_template)
        expected_sub = sub_results.generate_expected_result()
        alias = re.sub(r',.*', '', re.sub(r'.*\$subquery\(.*\)', '', query_template))
        alias = re.sub(r'.*as ', '', alias).strip()
        self.gen_results = TuqGenerators(self.log, expected_sub)
        query_template = re.sub(r'\$subquery\(.*\).*%s' % alias, ' %s' % alias, query_template)
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def negative_common_body(self, queries_errors={}):
        if not queries_errors:
            self.fail("No queries to run!")
        for bucket in self.buckets:
            for query_template, error in queries_errors.items():
                try:
                    query = self.gen_results.generate_query(query_template)
                    actual_result = self.run_cbq_query(query.format(bucket.name))
                except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find(error) != -1,
                                    "Error is incorrect.Actual %s.\n Expected: %s.\n" %(
                                                                str(ex).split(':')[-1], error))
                else:
                    self.fail("There were no errors. Error expected: %s" % error)

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
        query_params = {}
        cred_params = {'creds': []}
        rest = RestConnection(server)
        username = rest.username
        password = rest.password
        cred_params['creds'].append({'user': username, 'pass': password})
        for bucket in self.buckets:
            if bucket.saslPassword:
                cred_params['creds'].append({'user': 'local:%s' % bucket.name, 'pass': bucket.saslPassword})
        query_params.update(cred_params)
        if self.use_rest:
            query_params.update({'scan_consistency': self.scan_consistency})
            self.log.info('RUN QUERY %s' % query)

            if self.analytics:
                query = query + ";"
                for bucket in self.buckets:
                    query = query.replace(bucket.name, bucket.name+"_shadow")
                result = RestConnection(self.cbas_node).execute_statement_on_cbas(query, "immediate")
                result = json.loads(result)

            else :
                result = rest.query_tool(query, self.n1ql_port, query_params=query_params)


        else:
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside("$GOPATH/src/github.com/couchbase/query/" +\
                                                            "shell/cbq/cbq ", "", "", "", "", "", "")
            else:
                os = self.shell.extract_remote_info().type.lower()
                if not(self.isprepared):
                    query = query.replace('"', '\\"')
                    query = query.replace('`', '\\`')

                cmd =  "%s/cbq  -engine=http://%s:%s/ -q -u %s -p %s" % (self.path, server.ip, server.port, username, password)

                output = self.shell.execute_commands_inside(cmd, query, "", "", "", "", "")
                if not(output[0] == '{'):
                    output1 = '{'+str(output)
                else:
                    output1 = output
                result = json.loads(output1)
        if isinstance(result, str) or 'errors' in result:
            raise CBQError(result, server.ip)
        self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])
        return result

    def build_url(self, version):
        info = self.shell.extract_remote_info()
        type = info.distribution_type.lower()
        if type in ["ubuntu", "centos", "red hat"]:
            url = "https://s3.amazonaws.com/packages.couchbase.com/releases/couchbase-query/dp1/"
            url += "couchbase-query_%s_%s_linux.tar.gz" %(
                                version, info.architecture_type)
        #TODO for windows
        return url

    def _build_tuq(self, server):
        if self.version == "git_repo":
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                goroot = testconstants.LINUX_GOROOT
                gopath = testconstants.LINUX_GOPATH
            else:
                goroot = testconstants.WINDOWS_GOROOT
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if self.input.tuq_client and "goroot" in self.input.tuq_client:
                goroot = self.input.tuq_client["goroot"]
            cmd = "rm -rf {0}/src/github.com".format(gopath)
            self.shell.execute_command(cmd)
            cmd= 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) +\
                ' export PATH=$PATH:$GOROOT/bin && ' +\
                'go get github.com/couchbaselabs/tuqtng;' +\
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng; ' +\
                'go get -d -v ./...; cd .'
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) +\
                ' export PATH=$PATH:$GOROOT/bin && ' +\
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng; go build; cd .'
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) +\
                ' export PATH=$PATH:$GOROOT/bin && ' +\
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng/tuq_client; go build; cd .'
            self.shell.execute_command(cmd)
        else:
            cbq_url = self.build_url(self.version)
            #TODO for windows
            cmd = "cd /tmp; mkdir tuq;cd tuq; wget {0} -O tuq.tar.gz;".format(cbq_url)
            cmd += "tar -xvf tuq.tar.gz;rm -rf tuq.tar.gz"
            self.shell.execute_command(cmd)

    def _start_command_line_query(self, server):
        if self.version == "git_repo":
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                gopath = testconstants.LINUX_GOPATH
            else:
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if os == 'windows':
                cmd = "cd %s/src/github.com/couchbase/query/server/main; " % (gopath) +\
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd %s/src/github.com/couchbase/query//server/main; " % (gopath) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" %(
                                                                server.ip, server.port)
            self.shell.execute_command(cmd)
        elif self.version == "sherlock":
            if self.services_init.find('n1ql') != -1:
                return
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                couchbase_path = testconstants.LINUX_COUCHBASE_BIN_PATH
            else:
                couchbase_path = testconstants.WIN_COUCHBASE_BIN_PATH
            if self.input.tuq_client and "sherlock_path" in self.input.tuq_client:
                couchbase_path = "%s/bin" % self.input.tuq_client["sherlock_path"]
            if os == 'windows':
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" %(
                                                                server.ip, server.port)
                n1ql_port = self.input.param("n1ql_port", None)
                if server.ip == "127.0.0.1" and server.n1ql_port:
                    n1ql_port = server.n1ql_port
                if n1ql_port:
                    cmd = "cd %s; " % (couchbase_path) +\
                './cbq-engine -datastore http://%s:%s/ -http=":%s">n1ql.log 2>&1 &' %(
                                                                server.ip, server.port, n1ql_port)
            self.shell.execute_command(cmd)
        else:
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd /cygdrive/c/tuq;./cbq-engine.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            self.shell.execute_command(cmd)

    def _parse_query_output(self, output):
        if output.find("cbq>") == 0:
            output = output[output.find("cbq>") + 4:].strip()
        if output.find("tuq_client>") == 0:
            output = output[output.find("tuq_client>") + 11:].strip()
        if output.find("cbq>") != -1:
            output = output[:output.find("cbq>")].strip()
        if output.find("tuq_client>") != -1:
            output = output[:output.find("tuq_client>")].strip()
        return json.loads(output)

    def generate_docs(self, num_items, start=0):
        try:
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)
        except:
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs_sabre(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sabre(docs_per_day, start)

    def generate_docs_employee(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_data(docs_per_day = docs_per_day, start = start)

    def generate_docs_simple(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_simple_data(docs_per_day = docs_per_day, start = start)

    def generate_docs_sales(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_sales_data(docs_per_day = docs_per_day, start = start)

    def generate_docs_bigdata(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=(1000*docs_per_day), start=start, value_size=self.value_size)


    def _verify_results(self, actual_result, expected_result, missing_count = 1, extra_count = 1):
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:missing_count], extra[:extra_count]))
            self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" +\
        "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                          msg % (actual_result[:100], actual_result[-100:],
                                 expected_result[:100], expected_result[-100:]))

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                 extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def sort_nested_list(self, result):
        actual_result = []
        for item in result:
            curr_item = {}
            for key, value in item.items():
                if isinstance(value, list) or isinstance(value, set):
                    curr_item[key] = sorted(value)
                else:
                    curr_item[key] = value
            actual_result.append(curr_item)
        return actual_result

    def configure_gomaxprocs(self):
        max_proc = self.input.param("gomaxprocs", None)
        cmd = "export GOMAXPROCS=%s" % max_proc
        for server in self.servers:
            shell_connection = RemoteMachineShellConnection(self.master)
            shell_connection.execute_command(cmd)

    def create_primary_index_for_3_0_and_greater(self):
        self.log.info("CREATE PRIMARY INDEX using %s" % self.primary_indx_type)
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        if versions[0].startswith("4") or versions[0].startswith("3") or versions[0].startswith("5"):
            for bucket in self.buckets:
                if self.primary_indx_drop:
                    self.log.info("Dropping primary index for %s using %s ..." % (bucket.name, self.primary_indx_type))
                    self.query = "DROP PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
                    #self.run_cbq_query()
                    self.sleep(3, 'Sleep for some time after index drop')
                self.query = 'select * from system:indexes where name="#primary" and keyspace_id = "%s"' % bucket.name
                res = self.run_cbq_query()
                self.sleep(10)
                if self.monitoring:
                    self.query = "delete from system:completed_requests"
                    self.run_cbq_query()
                if not self.skip_primary_index:
                    if (res['metrics']['resultCount'] == 0):
                        self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
                        self.log.info("Creating primary index for %s ..." % bucket.name)
                        try:
                            self.run_cbq_query()
                            self.primary_index_created = True
                            if self.primary_indx_type.lower() == 'gsi':
                                self._wait_for_index_online(bucket, '#primary')
                        except Exception as ex:
                            self.log.info(str(ex))

    def _wait_for_index_online(self, bucket, index_name, timeout=6000):
        end_time = time.time() + timeout
        while time.time() < end_time:
            query = "SELECT * FROM system:indexes where name='%s'" % index_name
            res = self.run_cbq_query(query)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    self.log.error(item)
                    continue
                if item['indexes']['keyspace_id'] == bucket.name:
                    if item['indexes']['state'] == "online":
                        return
            self.sleep(5, 'index is pending or not in the list. sleeping... (%s)' % [item['indexes'] for item in res['results']])
        raise Exception('index %s is not online. last response is %s' % (index_name, res))


    def _get_keys(self, key_num):
        keys = []
        for gen in self.gens_load:
            gen_copy = copy.deepcopy(gen)
            for i in range(gen_copy.end):
                key, _ = next(gen_copy)
                keys.append(key)
                if len(keys) == key_num:
                    return keys
        return keys
