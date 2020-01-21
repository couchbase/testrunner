import paramiko
import logger
import json
import uuid
import math
import re
import time
import testconstants
import datetime
import time
from datetime import date
from couchbase_helper.tuq_generators import TuqGenerators
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import CBQError, ReadDocumentException
from membase.api.rest_client import RestConnection

class AnalyticsHelper():
    def __init__(self, version = None, master = None, shell = None, use_rest = None, max_verify = 0, buckets = [],
        item_flag = 0,  analytics_port = 8095, n1ql_port = 8093, full_docs_list = [], log = None, input = None,database = None):
        self.version = version
        self.shell = shell
        self.n1ql_port = n1ql_port
        self.max_verify = max_verify
        self.buckets = buckets
        self.item_flag = item_flag
        self.analytics_port = analytics_port
        self.input = input
        self.log = log
        self.use_rest = True
        self.full_docs_list = full_docs_list
        self.master = master
        self.database = database
        if self.full_docs_list and len(self.full_docs_list) > 0:
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)


    def killall_tuq_process(self):
        self.shell.execute_command("killall cbq-engine")
        self.shell.execute_command("killall tuqtng")
        self.shell.execute_command("killall indexer")

    def run_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_analytics_query()
        return actual_result, expected_result

    def run_analytics_query(self, query=None, min_output_size=10, server=None, query_params = {}, is_prepared=False, scan_consistency = None, scan_vector = None, verbose= False):
        if query is None:
            query = self.query
        if server is None:
           server = self.master
           if server.ip == "127.0.0.1":
            self.analytics_port = server.analytics_port
        else:
            if server.ip == "127.0.0.1":
                self.analytics_port = server.analytics_port
            if self.input.tuq_client and "client" in self.input.tuq_client:
                server = self.tuq_client
        if self.analytics_port == None or self.analytics_port == '':
            self.analytics_port = self.input.param("analytics_port", 8095)
            if not self.analytics_port:
                self.log.info(" analytics_port is not defined, processing will not proceed further")
                raise Exception("analytics_port is not defined, processing will not proceed further")
        cred_params = {'creds': []}
        for bucket in self.buckets:
            if bucket.saslPassword:
                cred_params['creds'].append({'user': 'local:%s' % bucket.name, 'pass': bucket.saslPassword})
        query_params.update(cred_params)
        if self.use_rest:
            query_params = {}
            if scan_consistency:
                query_params['scan_consistency']=  scan_consistency
            if scan_vector:
                query_params['scan_vector']=  str(scan_vector).replace("'", '"')
            if verbose:
                self.log.info('RUN QUERY %s' % query)
            query = query + ";"
            if "USE INDEX" in query:
                query = query.replace("USE INDEX(`#primary` USING GSI)"," ")
            for bucket in self.buckets:
                query = query.replace(bucket.name+" ",bucket.name+"_shadow ")


            self.log.info(" CBAS QUERY :: {0}".format(query))
            result = RestConnection(server).analytics_tool(query, self.analytics_port, query_params=query_params, verbose = verbose)

        if isinstance(result, str) or 'errors' in result:
            error_result = str(result)
            length_display = len(error_result)
            if length_display > 500:
                error_result = error_result[:500]
            raise CBQError(error_result, server.ip)
        self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])
        return result



    def _verify_results(self, actual_result, expected_result, missing_count = 1, extra_count = 1):
        self.log.info(" Analyzing Actual Result")
        actual_result = self._gen_dict(actual_result)
        self.log.info(" Analyzing Expected Result")
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            raise Exception("Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        msg = "The number of rows match but the results mismatch, please check"
        if actual_result != expected_result:
            raise Exception(msg)

    def _verify_results_rqg_new(self, n1ql_result = [], sql_result = [], hints = ["a1"]):
        new_n1ql_result = []
        for result in n1ql_result:
            if result != {}:
                for key in list(result.keys()):
                    if key.find('_shadow') != -1:
                        new_n1ql_result.append(result[key])
                    else:
                        new_n1ql_result.append(result)
                        break
        n1ql_result = new_n1ql_result
        if self._is_function_in_result(hints):
            return self._verify_results_rqg_for_function(n1ql_result, sql_result)
        check = self._check_sample(n1ql_result, hints)
        actual_result = n1ql_result
        if actual_result == [{}]:
            actual_result = []
        if check:
            actual_result = self._gen_dict(n1ql_result)
        actual_result = sorted(actual_result)
        expected_result = sorted(sql_result)
        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect.Actual num %s. Expected num: %s.:: %s \n" % (
                                            len(actual_result), len(expected_result), extra_msg))
        msg = "The number of rows match but the results mismatch, please check"
        if self._sort_data(actual_result) != self._sort_data(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception(msg+"\n "+extra_msg)
        
    def _verify_results_rqg(self, n1ql_result = [], sql_result = [], hints = ["a1"]):
        new_n1ql_result = []
        for result in n1ql_result:
            if result != {}:
                new_n1ql_result.append(result)
        n1ql_result = new_n1ql_result
        if self._is_function_in_result(hints):
            return self._verify_results_rqg_for_function(n1ql_result, sql_result)
        check = self._check_sample(n1ql_result, hints)
        actual_result = n1ql_result
        if actual_result == [{}]:
            actual_result = []
        if check:
            actual_result = self._gen_dict(n1ql_result)
        actual_result = sorted(actual_result)
        expected_result = sorted(sql_result)
        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect.Actual num %s. Expected num: %s.:: %s \n" % (
                                            len(actual_result), len(expected_result), extra_msg))
        msg = "The number of rows match but the results mismatch, please check"
        if self._sort_data(actual_result) != self._sort_data(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception(msg+"\n "+extra_msg)

    def _sort_data(self, result):
        new_data =[]
        for data in result:
            new_data.append(sorted(data))
        return new_data

    def _verify_results_crud_rqg(self, n1ql_result = [], sql_result = [], hints = ["primary_key_id"]):
        new_n1ql_result = []
        for result in n1ql_result:
            if result != {}:
                new_n1ql_result.append(result)
        n1ql_result = new_n1ql_result
        if self._is_function_in_result(hints):
            return self._verify_results_rqg_for_function(n1ql_result, sql_result)
        check = self._check_sample(n1ql_result, hints)
        actual_result = n1ql_result
        if actual_result == [{}]:
            actual_result = []
        if check:
            actual_result = self._gen_dict(n1ql_result)
        actual_result = sorted(actual_result)
        expected_result = sorted(sql_result)
        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect.Actual num %s. Expected num: %s.:: %s \n" % (
                                            len(actual_result), len(expected_result), extra_msg))
        if not self._result_comparison_analysis(actual_result,expected_result) :
            msg = "The number of rows match but the results mismatch, please check"
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception(msg+"\n "+extra_msg)

    def _get_failure_message(self, expected_result, actual_result):
        if expected_result == None:
            expected_result = []
        if actual_result == None:
            actual_result = []
        len_expected_result = len(expected_result)
        len_actual_result = len(actual_result)
        len_expected_result = min(5,len_expected_result)
        len_actual_result = min(5,len_actual_result)
        extra_msg = "mismatch in results :: expected :: {0}, actual :: {1} ".format(expected_result[0:len_expected_result], actual_result[0:len_actual_result])
        return extra_msg

    def _result_comparison_analysis(self, expected_result, actual_result):
        expected_map ={}
        actual_map ={}
        for data in expected_result:
            primary=None
            for key in list(data.keys()):
                keys = key
                if keys.encode('ascii') == "primary_key_id":
                    primary = keys
            expected_map[data[primary]] = data
        for data in actual_result:
            primary=None
            for key in list(data.keys()):
                keys = key
                if keys.encode('ascii') == "primary_key_id":
                    primary = keys
            actual_map[data[primary]] = data
        check = True
        for key in list(expected_map.keys()):
            if sorted(actual_map[key]) != sorted(expected_map[key]):
                check= False
        return check

    def _analyze_for_special_case_using_func(self, expected_result, actual_result):
        if expected_result == None:
            expected_result = []
        if actual_result == None:
            actual_result = []
        if len(expected_result) == 1:
            value = list(expected_result[0].values())[0]
            if value == None or value == 0:
                expected_result = []
        if len(actual_result) == 1:
            value = list(actual_result[0].values())[0]
            if value == None or value == 0:
                actual_result = []
        return expected_result, actual_result

    def _is_function_in_result(self, result):
        if result == "FUN":
            return True
        return False

    def _verify_results_rqg_for_function(self, n1ql_result = [], sql_result = [], hints = ["a1"]):
        actual_count = -1
        expected_count = -1
        actual_result = n1ql_result
        sql_result, actual_result= self._analyze_for_special_case_using_func(sql_result, actual_result)
        if len(sql_result) != len(actual_result):
            msg = "the number of results do not match :: expected = {0}, actual = {1}".format(len(n1ql_result), len(sql_result))
            extra_msg = self._get_failure_message(sql_result, actual_result)
            raise Exception(msg+"\n"+extra_msg)
        n1ql_result = self._gen_dict_n1ql_func_result(n1ql_result)
        n1ql_result = sorted(n1ql_result)
        sql_result = self._gen_dict_n1ql_func_result(sql_result)
        sql_result = sorted(sql_result)
        if  len(sql_result) == 0 and len(actual_result) == 0:
            return
        if sql_result != n1ql_result:
            max = 2
            if len(sql_result) < 5:
                max = len(sql_result)
            msg = "mismatch in results :: expected [0:{0}]:: {1}, actual [0:{0}]:: {2} ".format(max, sql_result[0:max], n1ql_result[0:max])
            raise Exception(msg)

    def _convert_to_number(self, val):
        if not isinstance(val, str):
            return val
        value = -1
        try:
            if value == '':
                return 0
            value = int(val.split("(")[1].split(")")[0])
        except Exception as ex:
            self.log.info(ex)
        finally:
            return value

    def analyze_failure(self, actual, expected):
        missing_keys =[]
        different_values = []
        for key in list(expected.keys()):
            if key not in list(actual.keys()):
                missing_keys.append(key)
            if expected[key] != actual[key]:
                different_values.append("for key {0}, expected {1} \n actual {2}".
                    format(key, expected[key], actual[key]))
        self.log.info(missing_keys)
        if(len(different_values) > 0):
            self.log.info(" number of such cases {0}".format(len(different_values)))
            self.log.info(" example key {0}".format(different_values[0]))

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

    def build_url(self, version):
        info = self.shell.extract_remote_info()
        type = info.distribution_type.lower()
        if type in ["ubuntu", "centos", "red hat"]:
            url = "https://s3.amazonaws.com/packages.couchbase.com/releases/couchbase-query/dp1/"
            url += "couchbase-query_%s_%s_linux.tar.gz" %(
                                version, info.architecture_type)
        #TODO for windows
        return url

    def _restart_indexer(self):
        couchbase_path = "/opt/couchbase/var/lib/couchbase"
        cmd = "rm -f {0}/meta;rm -f /tmp/log_upr_client.sock".format(couchbase_path)
        self.shell.execute_command(cmd)

    def _start_command_line_query(self, server):
        self.shell = RemoteMachineShellConnection(server)
        self._set_env_variable(server)
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
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                couchbase_path = testconstants.LINUX_COUCHBASE_BIN_PATH
            else:
                couchbase_path = testconstants.WIN_COUCHBASE_BIN_PATH
            if self.input.tuq_client and "sherlock_path" in self.input.tuq_client:
                couchbase_path = "%s/bin" % self.input.tuq_client["sherlock_path"]
                print("PATH TO SHERLOCK: %s" % couchbase_path)
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

    def drop_primary_index(self, using_gsi = True, server = None):
        if server == None:
            server = self.master
        self.log.info("CHECK FOR PRIMARY INDEXES")
        for bucket in self.buckets:
            self.query = "DROP PRIMARY INDEX ON {0}".format(bucket.name)
            if using_gsi:
                self.query += " USING GSI"
            if not using_gsi:
                self.query += " USING VIEW "
            self.log.info(self.query)
            try:
                check = self._is_index_in_list(bucket.name, "#primary", server = server)
                if check:
                    self.run_analytics_query(server = server)
            except Exception as ex:
                self.log.error('ERROR during index creation %s' % str(ex))

    def create_primary_index(self, using_gsi = True, server = None):
        if server == None:
            server = self.master
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % (bucket.name)
            if using_gsi:
                self.query += " USING GSI"
                # if gsi_type == "memdb":
                #     self.query += " WITH {'index_type': 'memdb'}"
            if not using_gsi:
                self.query += " USING VIEW "
            try:
                check = self._is_index_in_list(bucket.name, "#primary", server = server)
                if not check:
                    self.run_analytics_query(server = server)
                    check = self.is_index_online_and_in_list(bucket.name, "#primary", server = server)
                    if not check:
                        raise Exception(" Timed-out Exception while building primary index for bucket {0} !!!".format(bucket.name))
                else:
                    raise Exception(" Primary Index Already present, This looks like a bug !!!")
            except Exception as ex:
                self.log.error('ERROR during index creation %s' % str(ex))
                raise ex

    def verify_index_with_explain(self, actual_result, index_name, check_covering_index= False):
        check = True
        if check_covering_index:
            if "covering" in str(actual_result):
                check = True
            else:
                check = False
        if index_name in str(actual_result):
            return True and check
        return False

    def run_query_and_verify_result(self, server = None, query = None, timeout = 120.0, max_try = 1,
     expected_result = None, scan_consistency = None, scan_vector = None, verify_results = True):
        check = False
        init_time = time.time()
        try_count = 0
        while not check:
            next_time = time.time()
            try:
                actual_result = self.run_analytics_query(query = query, server = server,
                 scan_consistency = scan_consistency, scan_vector = scan_vector)
                if verify_results:
                    self._verify_results(actual_result['results'], expected_result)
                else:
                    return "ran query with success and validated results" , True
                check = True
            except Exception as ex:
                if (next_time - init_time > timeout or try_count >= max_try):
                    return ex, False
            finally:
                try_count += 1
        return "ran query with success and validated results" , check


    def run_cbq_query(self, query=None, min_output_size=10, server=None, query_params = {}, is_prepared=False, scan_consistency = None, scan_vector = None, verbose= True):
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
            self.n1ql_port = self.input.param("n1ql_port", 90)
            if not self.n1ql_port:
                self.log.info(" n1ql_port is not defined, processing will not proceed further")
                raise Exception("n1ql_port is not defined, processing will not proceed further")
        cred_params = {'creds': []}
        for bucket in self.buckets:
            if bucket.saslPassword:
                cred_params['creds'].append({'user': 'local:%s' % bucket.name, 'pass': bucket.saslPassword})
        query_params.update(cred_params)
        if self.use_rest:
            query_params = {}
            if scan_consistency:
                query_params['scan_consistency']=  scan_consistency
            if scan_vector:
                query_params['scan_vector']=  str(scan_vector).replace("'", '"')
            if verbose:
                self.log.info('RUN QUERY %s' % query)
            result = RestConnection(server).query_tool(query, self.n1ql_port, query_params=query_params, is_prepared = is_prepared, verbose = verbose)
        else:
            # if self.version == "git_repo":
            #     output = self.shell.execute_commands_inside("$GOPATH/src/github.com/couchbaselabs/tuqtng/" +\
            #                                                 "tuq_client/tuq_client " +\
            #                                                 "-engine=http://%s:8093/" % server.ip,
            #                                            subcommands=[query,],
            #                                            min_output_size=20,
            #                                            end_msg='tuq_client>')
            # else:
            #os = self.shell.extract_remote_info().type.lower()
            shell = RemoteMachineShellConnection(server)
            #query = query.replace('"', '\\"')
            #query = query.replace('`', '\\`')
            #if os == "linux":
            cmd = "%s/cbq  -engine=http://%s:8093/" % (testconstants.LINUX_COUCHBASE_BIN_PATH,server.ip)
            output = shell.execute_commands_inside(cmd,query,"","","","","")
            print("--------------------------------------------------------------------------------------------------------------------------------")
            print(output)
            result = json.loads(output)
            print(result)
            result = self._parse_query_output(output)
        if isinstance(result, str) or 'errors' in result:
            error_result = str(result)
            length_display = len(error_result)
            if length_display > 500:
                error_result = error_result[:500]
            raise CBQError(error_result, server.ip)
        self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])
        return result

    # def is_index_online_and_in_list(self, bucket, index_name, server=None, timeout=600.0):
    #     check = self._is_index_in_list(bucket, index_name, server = server)
    #     init_time = time.time()
    #     while not check:
    #         time.sleep(1)
    #         check = self._is_index_in_list(bucket, index_name, server = server)
    #         next_time = time.time()
    #         if check or (next_time - init_time > timeout):
    #             return check
    #     return check
    #
    # def is_index_ready_and_in_list(self, bucket, index_name, server=None, timeout=600.0):
    #     query = "SELECT * FROM system:indexes where name = \'{0}\'".format(index_name)
    #     if server == None:
    #         server = self.master
    #     init_time = time.time()
    #     check = False
    #     while not check:
    #         res = self.run_analytics_query(query=query, server=server)
    #         for item in res['results']:
    #             if 'keyspace_id' not in item['indexes']:
    #                 check = False
    #             elif item['indexes']['keyspace_id'] == str(bucket) \
    #                     and item['indexes']['name'] == index_name \
    #                     and item['indexes']['state'] == "online":
    #                 check = True
    #         time.sleep(1)
    #         next_time = time.time()
    #         check = check or (next_time - init_time > timeout)
    #     return check

    # def is_index_online_and_in_list_bulk(self, bucket, index_names = [], server = None, index_state = "online", timeout = 600.0):
    #     check, index_names = self._is_index_in_list_bulk(bucket, index_names, server = server, index_state = index_state)
    #     init_time = time.time()
    #     while not check:
    #         check, index_names = self._is_index_in_list_bulk(bucket, index_names, server = server, index_state = index_state)
    #         next_time = time.time()
    #         if check or (next_time - init_time > timeout):
    #             return check
    #     return check
    #
    # def gen_build_index_query(self, bucket = "default", index_list = []):
    #     return "BUILD INDEX on {0}({1}) USING GSI".format(bucket,",".join(index_list))
    #
    # def gen_query_parameter(self, scan_vector = None, scan_consistency = None):
    #     query_params = {}
    #     if scan_vector:
    #         query_params.update("scan_vector", scan_vector)
    #     if scan_consistency:
    #         query_params.update("scan_consistency", scan_consistency)
    #     return query_params

    # def _is_index_in_list(self, bucket, index_name, server = None, index_state = ["pending", "building", "deferred"]):
    #     query = "SELECT * FROM system:indexes where name = \'{0}\'".format(index_name)
    #     if server == None:
    #         server = self.master
    #     res = self.run_cbq_query(query = query, server = server)
    #     for item in res['results']:
    #         if 'keyspace_id' not in item['indexes']:
    #             return False
    #         if item['indexes']['keyspace_id'] == str(bucket) and item['indexes']['name'] == index_name and item['indexes']['state'] not in index_state:
    #             return True
    #     return False
    #
    # def _is_index_in_list_bulk(self, bucket, index_names = [], server = None, index_state = ["pending","building"]):
    #     query = "SELECT * FROM system:indexes"
    #     if server == None:
    #         server = self.master
    #     res = self.run_cbq_query(query = query, server = server)
    #     index_count=0
    #     found_index_list = []
    #     for item in res['results']:
    #         if 'keyspace_id' not in item['indexes']:
    #             return False
    #         for index_name in index_names:
    #             if item['indexes']['keyspace_id'] == str(bucket) and item['indexes']['name'] == index_name and item['indexes']['state'] not in index_state:
    #                 found_index_list.append(index_name)
    #     if len(found_index_list) == len(index_names):
    #         return True, []
    #     return False, list(set(index_names) - set(found_index_list))
    #
    # def gen_index_map(self, server = None):
    #     query = "SELECT * FROM system:indexes"
    #     if server == None:
    #         server = self.master
    #     res = self.run_cbq_query(query = query, server = server)
    #     index_map = {}
    #     for item in res['results']:
    #         bucket_name = item['indexes']['keyspace_id'].encode('ascii','ignore')
    #         if bucket_name not in index_map.keys():
    #             index_map[bucket_name] = {}
    #         index_name = str(item['indexes']['name'])
    #         index_map[bucket_name][index_name] = {}
    #         index_map[bucket_name][index_name]['state'] = item['indexes']['state']
    #     return index_map
    #
    # def get_index_count_using_primary_index(self, buckets, server = None):
    #     query = "SELECT COUNT(*) FROM {0}"
    #     map= {}
    #     if server == None:
    #         server = self.master
    #     for bucket in buckets:
    #         res = self.run_cbq_query(query = query.format(bucket.name), server = server)
    #         map[bucket.name] = int(res["results"][0]["$1"])
    #     return map
    #
    # def get_index_count_using_index(self, bucket, index_name,server=None):
    #     query = 'SELECT COUNT(*) FROM {0} USE INDEX ({1})'.format(bucket.name, index_name)
    #     if not server:
    #         server = self.master
    #     res = self.run_cbq_query(query=query, server=server)
    #     return int(res['results'][0]['$1'])

    def _gen_dict(self, result):
        result_set = []
        if result != None and len(result) > 0:
                for val in result:
                    for key in list(val.keys()):
                        result_set.append(val[key])
        return result_set

    def _gen_dict_n1ql_func_result(self, result):
        result_set = [val[key] for val in result for key in list(val.keys())]
        new_result_set = []
        if len(result_set) > 0:
            for value in result_set:
                if isinstance(value, float):
                    new_result_set.append(round(value, 0))
                else:
                    new_result_set.append(value)
        else:
            new_result_set = result_set
        return new_result_set

    def _check_sample(self, result, expected_in_key = None):
        if expected_in_key == "FUN":
            return False
        if expected_in_key == None or len(expected_in_key) == 0:
            return False
        if result != None and len(result) > 0:
            sample=result[0]
            for key in list(sample.keys()):
                for sample in expected_in_key:
                    if key in sample:
                        return True
        return False

    def old_gen_dict(self, result):
        result_set = []
        map = {}
        duplicate_keys = []
        try:
            if result != None and len(result) > 0:
                for val in result:
                    for key in list(val.keys()):
                        result_set.append(val[key])
            for val in result_set:
                if val["_id"] in list(map.keys()):
                    duplicate_keys.append(val["_id"])
                map[val["_id"]] = val
            keys = list(map.keys())
            keys.sort()
        except Exception as ex:
            self.log.info(ex)
            raise
        if len(duplicate_keys) > 0:
            raise Exception(" duplicate_keys {0}".format(duplicate_keys))
        return map

