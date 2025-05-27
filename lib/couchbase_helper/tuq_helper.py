import json
import ast
import testconstants
import time
from couchbase_helper.tuq_generators import TuqGenerators
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
import traceback
from deepdiff import DeepDiff
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

from lib.Cb_constants.CBServer import CbServer


class N1QLHelper():
    def __init__(self, version=None, master=None, shell=None,  max_verify=0, buckets=[], item_flag=0,
                 n1ql_port=8093, full_docs_list=[], log=None, input=None, database=None, use_rest=None):
        self.version = version
        self.shell = shell
        self.max_verify = max_verify
        self.buckets = buckets
        self.item_flag = item_flag
        self.n1ql_port = n1ql_port
        self.input = input
        self.log = log
        self.use_rest = use_rest
        self.full_docs_list = full_docs_list
        self.master = master
        self.database = database
        if self.full_docs_list and len(self.full_docs_list) > 0:
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)

    def create_collection(self, server=None, keyspace="default", bucket_name="", scope_name="", collection_name="",
                          poll_interval=1, timeout=30):
        if not bucket_name:
            raise Exception("Bucket name cannot be empty!")

        if not scope_name:
            scope_name = "_default"

        query = f"create collection {keyspace}:{bucket_name}.{scope_name}.{collection_name}"
        try:
            self.run_cbq_query(query=query, server=server)
        except CBQError as e:
            raise e
        collection_created = self.wait_till_collection_created(server=server, bucket_name=bucket_name, scope_name=scope_name,
                                          collection_name=collection_name, poll_interval=poll_interval, timeout=timeout)
        return collection_created

    def create_scope(self, server=None, keyspace="default", bucket_name="", scope_name="", poll_interval=1, timeout=30):
        if not bucket_name:
            raise Exception("Bucket name cannot be empty!")

        query = f"create scope {keyspace}:{bucket_name}.{scope_name}"
        self.run_cbq_query(server=server, query=query)
        scope_created = self.wait_till_scope_created(server=server, bucket_name=bucket_name, scope_name=scope_name,
                                                     poll_interval=poll_interval, timeout=timeout)
        return scope_created

    def delete_collection(self, server=None, keyspace="", bucket_name="", scope_name="", collection_name="",
                          poll_interval=1, timeout=30):
        objects = RestConnection(server).get_scope_collections(bucket_name, scope_name)
        if not collection_name in objects:
            self.log.info(f"Cannot find specified collection {keyspace}:{bucket_name}.{scope_name}.{collection_name}. Nothing to delete, returning.")
            return

        query = f"drop collection {keyspace}:{bucket_name}.{scope_name}.{collection_name}"
        self.run_cbq_query(server=server, query=query)
        self.wait_till_collection_dropped(server, bucket_name, scope_name, collection_name, poll_interval, timeout)

    def check_if_scope_exists(self, server=None, namespace=None, bucket=None, scope=None):
        scope_query = f"select count(*) as cnt from system:scopes where `namespace`='{namespace}' and `bucket`='{bucket}' and name='{scope}'"
        res = self.run_cbq_query(query=scope_query, server=server)
        return res['results'][0]['cnt'] == 1

    def delete_scope(self, server=None, keyspace="", bucket_name="", scope_name="", poll_interval=1, timeout=30):
        scope_exists = self.check_if_scope_exists(server=server, namespace=keyspace, bucket=bucket_name, scope=scope_name)
        if scope_exists:
            query = f"drop scope {keyspace}:{bucket_name}.{scope_name}"
            self.run_cbq_query(query=query, server=server)
            self.wait_till_scope_dropped(server, bucket_name, scope_name, poll_interval, timeout)
        else:
            self.log.info(f"Cannot find specified scope {keyspace}:{bucket_name}.{scope_name}. Nothing to delete, returning.")

    def wait_till_scope_dropped(self, server=None, bucket_name="", scope_name="", poll_interval=1, timeout=30):
        start_time = time.time()
        scope_dropped = False
        while time.time() < start_time + timeout:
            # todo: get_bucket_scopes(bucket_name) function seems suspicious, need to investigate
            objects = RestConnection(server).get_bucket_scopes(bucket_name)
            if scope_name in objects:
                time.sleep(poll_interval)
            else:
                scope_dropped = True
                break
        return scope_dropped

    def wait_till_collection_dropped(self, server=None, bucket_name="", scope_name="", collection_name="",
                                     poll_interval=1, timeout=30):
        start_time = time.time()
        collection_dropped = False
        while time.time() < start_time + timeout:
            # todo: get_scope_collections(bucket_name, scope_name) function seems suspicious, need to investigate
            objects = RestConnection(server).get_scope_collections(bucket_name, scope_name)
            if collection_name in objects:
                time.sleep(poll_interval)
            else:
                collection_dropped = True
                break
        return collection_dropped

    def wait_till_scope_created(self, server=None, bucket_name="", scope_name="", poll_interval=1, timeout=30):
        start_time = time.time()
        scope_created = False
        while time.time() < start_time + timeout:
            # todo: get_bucket_scopes(bucket_name) function seems suspicious, need to investigate
            objects = RestConnection(server).get_bucket_scopes(bucket_name)
            if not scope_name in objects:
                time.sleep(poll_interval)
            else:
                scope_created = True
                break
        return scope_created

    def wait_till_collection_created(self, server=None, bucket_name=None, scope_name=None, collection_name=None,
                                     poll_interval=1, timeout=30):
        start_time = time.time()
        collection_created = False
        while time.time() < start_time + timeout:
            # todo: get_scope_collections(bucket_name, scope_name) function seems suspicious, need to investigate
            objects = RestConnection(server).get_scope_collections(bucket_name, scope_name)
            if not collection_name in objects:
                time.sleep(poll_interval)
            else:
                collection_created = True
                break
        return collection_created

    def killall_tuq_process(self):
        self.shell.execute_command("killall cbq-engine")
        self.shell.execute_command("killall tuqtng")
        self.shell.execute_command("killall indexer")

    def run_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def drop_all_indexes(self, bucket=None, leave_primary=True):
        current_indexes = self.get_parsed_indexes()
        if bucket is not None:
            current_indexes = [index for index in current_indexes if index['bucket'] == bucket]
        if leave_primary:
            current_indexes = [index for index in current_indexes if index['is_primary'] is False]
        for index in current_indexes:
            bucket = index['bucket'].replace("()","")
            index_name = index['name']
            if index['using'] != 'fts':
                self.run_cbq_query("drop index {0}.`{1}`".format(bucket, index_name))
        for index in current_indexes:
            bucket = index['bucket']
            index_name = index['name']
            if index['using'] != 'fts':
                self.wait_for_index_drop(bucket, index_name)

    def drop_all_indexes_on_keyspace(self, bucket=None, leave_primary=True):
        current_indexes = self.get_parsed_indexes_online()
        if bucket is not None:
            current_indexes = [index for index in current_indexes if index['bucket'] == bucket]
        if leave_primary:
            current_indexes = [index for index in current_indexes if index['is_primary'] is False]
        for index in current_indexes:
            keyspace = index['keyspace']
            index_name = index['name']
            self.run_cbq_query("drop index %s ON %s" % (index_name, keyspace))
        time.sleep(120)
        for index in current_indexes:
            bucket = index['bucket']
            index_name = index['name']
            self.wait_for_index_drop(bucket, index_name)

    def wait_for_index_drop(self, bucket_name, index_name, fields_set=None, using=None):
        self.with_retry(lambda: self.is_index_present(bucket_name, index_name, fields_set=fields_set, using=using, status="any"), eval=False, delay=1, tries=30)

    def with_retry(self, func, eval=None, delay=5, tries=10, func_params=None):
        attempts = 0
        while attempts < tries:
            attempts = attempts + 1
            try:
                res = func()
                if eval is None:
                    return res
                elif res == eval:
                    return res
                else:
                    time.sleep(delay)
            except Exception as ex:
                time.sleep (delay)
        raise Exception('timeout, invalid results: %s' % res)

    def is_index_present(self, bucket_name, index_name, fields_set=None, using=None, status="online"):
        query_response = self.run_cbq_query("SELECT * FROM system:indexes")
        self.log.info(f'{query_response}')
        if fields_set is None and using is None:
            if status is "any":
                desired_index = (index_name, bucket_name)
                current_indexes = [(i['indexes']['name'],
                                    i['indexes']['keyspace_id']) for i in query_response['results']]
            else:
                desired_index = (index_name, bucket_name, status)
                current_indexes = [(i['indexes']['name'],
                                i['indexes']['keyspace_id'],
                                i['indexes']['state']) for i in query_response['results']]
        else:
            if status is "any":
                desired_index = (index_name, bucket_name, frozenset([field for field in fields_set]), using)
                current_indexes = [(i['indexes']['name'],
                                    i['indexes']['keyspace_id'],
                                    frozenset([(key.replace('`', '').replace('(', '').replace(')', '').replace('meta.id', 'meta().id'), j)
                                               for j, key in enumerate(i['indexes']['index_key'], 0)]),
                                    i['indexes']['using']) for i in query_response['results']]
            else:
                desired_index = (index_name, bucket_name, frozenset([field for field in fields_set]), status, using)
                current_indexes = [(i['indexes']['name'],
                                i['indexes']['keyspace_id'],
                                frozenset([(key.replace('`', '').replace('(', '').replace(')', '').replace('meta.id', 'meta().id'), j)
                                           for j, key in enumerate(i['indexes']['index_key'], 0)]),
                                i['indexes']['state'],
                                i['indexes']['using']) for i in query_response['results']]

        if desired_index in current_indexes:
            return True
        else:
            self.log.info("waiting for: \n" + str(desired_index) + "\n")
            self.log.info("current indexes: \n" + str(current_indexes) + "\n")
            return False

    def run_cbq_query(self, query=None, min_output_size=10, server=None, query_params={}, is_prepared=False,
                      scan_consistency=None, scan_vector=None, scan_vectors=None, verbose=True, timeout=None,
                      rest_timeout=None, query_context=None,txnid=None,txtimeout=None, use_sdk = False):
        if query is None:
            query = self.query
        if server is None:
            server = self.master
            if server.ip == "127.0.0.1":
                self.n1ql_port = server.n1ql_port
        else:
            if server.ip == "127.0.0.1":
                self.n1ql_port = server.n1ql_port
            if self.input and self.input.tuq_client and "client" in self.input.tuq_client:
                server = self.tuq_client

        rest = RestConnection(server)

        if self.n1ql_port is None or self.n1ql_port == '':
            self.n1ql_port = rest.query_port
            if not self.n1ql_port:
                self.log.info(" n1ql_port is not defined, processing will not proceed further")
                raise Exception("n1ql_port is not defined, processing will not proceed further")

        result = ""
        if use_sdk and ("prepare" not in query.lower()) and ("insert" not in query.lower()):
            from couchbase.n1ql import N1QLQuery, STATEMENT_PLUS, CONSISTENCY_REQUEST, MutationState
            sdk_cluster = Cluster('couchbase://' + str(server.ip))
            authenticator = PasswordAuthenticator(rest.username, rest.password)
            sdk_cluster.authenticate(authenticator)
            for bucket in self.buckets:
                cb = sdk_cluster.open_bucket(bucket.name)

            sdk_query = N1QLQuery(query)

            if 'scan_consistency' in query_params:
                if query_params['scan_consistency'] == 'REQUEST_PLUS':
                    sdk_query.consistency = CONSISTENCY_REQUEST  # request_plus is currently mapped to the CONSISTENT_REQUEST constant in the Python SDK
                elif query_params['scan_consistency'] == 'STATEMENT_PLUS':
                    sdk_query.consistency = STATEMENT_PLUS
                else:
                    raise ValueError('Unknown consistency')
            # Python SDK returns results row by row, so we need to iterate through all the results
            row_iterator = cb.n1ql_query(sdk_query)
            content = []
            try:
                for row in row_iterator:
                    content.append(row)
                row_iterator.meta['results'] = content
                result = row_iterator.meta
            except Exception as e:
                # This will parse the resulting HTTP error and return only the dictionary containing the query results
                result = ast.literal_eval(str(e).split("value=")[1].split(", http_status")[0])
        elif self.use_rest:
            query_params = {}
            if txtimeout:
                query_params['txtimeout'] = txtimeout
            if txnid:
                query_params['txid'] = ''"{0}"''.format(txnid)
            if scan_consistency:
                query_params['scan_consistency']= scan_consistency
            if scan_vector:
                query_params['scan_vector']= str(scan_vector).replace("'", '"')
            if scan_vectors:
                query_params['scan_vectors'] = str(scan_vectors).replace("'", '"')
            if timeout:
                query_params['timeout'] = timeout
            if query_context:
                query_params['query_context'] = query_context
            if verbose:
                self.log.info('RUN QUERY %s' % query)
            result = RestConnection(server).query_tool(query, self.n1ql_port, query_params=query_params,
                                                       is_prepared=is_prepared, verbose=verbose, timeout=rest_timeout)
        else:
            if not self.shell:
                shell = RemoteMachineShellConnection(server)
            cmd = f"{testconstants.LINUX_COUCHBASE_BIN_PATH}/cbq  -engine=http://{server.ip}:8093/ -u {rest.username} -p {rest.password} "
            if CbServer.use_https:
                cmd = f"{testconstants.LINUX_COUCHBASE_BIN_PATH}/cbq  -engine=https://{server.ip}:18093/ -u {rest.username} -p {rest.password} "
            query = query.replace('"', '\\"')
            if "#primary" in query:
                query = query.replace("'#primary'", '\\"#primary\\"')
            if not self.shell:
                output = shell.execute_commands_inside(cmd, query, "", "", "", "", "")
            else:
                output = self.shell.execute_commands_inside(cmd, query, "", "", "", "", "")
            output = output[output.find('{"requestID":'):]
            try:
                result = json.loads(output)
            except Exception as ex:
                self.log.error(f"CANNOT LOAD QUERY RESULT IN JSON: {ex}" )
                self.log.error("INCORRECT DOCUMENT IS: " + str(output))
        if isinstance(result, str) or 'errors' in result:
            error_result = str(result)
            length_display = len(error_result)
            if length_display > 500:
                error_result = error_result[:500]
            raise CBQError(error_result, server.ip)

        self.log.info(f"TOTAL ELAPSED TIME: {result['metrics']['elapsedTime']}")
        return result

    def wait_for_all_indexes_online(self,verbose=True):
        cur_indexes = self.get_parsed_indexes()
        for index in cur_indexes:
            self.log.info("Indexes {0}".format(index))
            self._wait_for_index_online(index['bucket'], index['name'],verbose=verbose)

    def get_parsed_indexes(self):
        query_response = self.run_cbq_query("SELECT * FROM system:indexes where bucket_id is missing and name not like 'default%'")
        current_indexes = [{'name': i['indexes']['name'],
                            'bucket': i['indexes']['keyspace_id'],
                            'fields': frozenset([(key.replace('`', '').replace('(', '').replace(')', '').replace('meta.id', 'meta().id'), j)
                                                 for j, key in enumerate(i['indexes']['index_key'], 0)]),
                            'state': i['indexes']['state'],
                            'using': i['indexes']['using'],
                            'where': i['indexes'].get('condition', ''),
                            'is_primary': i['indexes'].get('is_primary', False)} for i in query_response['results']]
        return current_indexes

    def get_parsed_indexes_online(self):
        query_response = self.run_cbq_query("SELECT * FROM system:indexes")
        current_indexes = [{'name': i['indexes']['name'],
                            'keyspace': i['indexes']['namespace_id'] + ":" + i['indexes']['bucket_id'] + "." +
                            i['indexes']['scope_id'] + "." +
                            i['indexes']['keyspace_id'],
                            'bucket': i['indexes']['keyspace_id'],
                            'fields': frozenset([(key.replace('`', '').replace('(', '').replace(')', '').replace(
                                'meta.id', 'meta().id'), j)
                                for j, key in enumerate(i['indexes']['index_key'], 0)]),
                            'state': i['indexes']['state'],
                            'using': i['indexes']['using'],
                            'where': i['indexes'].get('condition', ''),
                            'is_primary': i['indexes'].get('is_primary', False)} for i in query_response['results']]
        return current_indexes

    def _wait_for_index_online(self, bucket, index_name, timeout=300, poll_interval=1, verbose=True):
        end_time = time.time() + timeout
        res = {}
        while time.time() < end_time:
            query = "SELECT * FROM system:indexes where name='%s'" % index_name
            res = self.run_cbq_query(query,verbose=verbose)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    self.log.error(item)
                    continue
                bucket_name = ""
                if isinstance(bucket, str) or isinstance(bucket, unicode):
                    bucket_name = bucket
                else:
                    bucket_name = bucket.name
                if item['indexes']['keyspace_id'] == bucket_name:
                    if item['indexes']['state'] == "online":
                        return
            time.sleep(poll_interval)
        raise Exception('index %s is not online. last response is %s' % (index_name, res))


    def _verify_results(self, actual_result, expected_result, missing_count = 1, extra_count = 1):
        self.log.info(" Analyzing Actual Result")
        actual_result = self._gen_dict(actual_result)
        self.log.info(" Analyzing Expected Result")
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            raise Exception("Results are incorrect.Actual num %s. Expected num: %s.\n" % (len(actual_result), len(expected_result)))
        msg = "The number of rows match but the results mismatch, please check"
        diffs = DeepDiff(actual_result, expected_result, ignore_order=True, ignore_numeric_type_changes=True)
        if diffs:
            self.log.info("-->actual vs expected diffs found:{}".format(diffs))
            raise Exception(msg)

    def _verify_results_rqg(self, subquery, aggregate=False, n1ql_result=[], sql_result=[], hints=["a1"],
                            aggregate_pushdown=False, window_function_test=False, delta=0, use_fts=False):
        new_n1ql_result = []
        keyspace = '_default'
        for result in n1ql_result:
            if result != {}:
                if list(result.keys()) == [keyspace]:
                    new_n1ql_result.append(result[keyspace])
                else:
                    new_n1ql_result.append(result)

        n1ql_result = new_n1ql_result

        if self._is_function_in_result(hints):
            return self._verify_results_rqg_for_function(n1ql_result, sql_result, aggregate_pushdown=aggregate_pushdown)

        check = self._check_sample(n1ql_result, hints)
        actual_result = n1ql_result

        if actual_result == [{}]:
            actual_result = []
        if check and not use_fts:
            actual_result = self._gen_dict(n1ql_result)

        expected_result = sql_result

        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect. Actual num %s. Expected num: %s. :: %s \n" % (len(actual_result), len(expected_result), extra_msg))

        msg = "The number of rows match but the results mismatch, please check"
        if subquery:
            for x, y in zip(actual_result, expected_result):
                if aggregate:
                    productId = x['ABC'][0]['$1']
                else:
                    productId = x['ABC'][0]['productId']
                if(productId != y['ABC']) or \
                                x['datetime_field1'] != y['datetime_field1'] or \
                                x['primary_key_id'] != y['primary_key_id'] or \
                                x['varchar_field1'] != y['varchar_field1'] or \
                                x['decimal_field1'] != y['decimal_field1'] or \
                                x['char_field1'] != y['char_field1'] or \
                                x['int_field1'] != y['int_field1'] or \
                                x['bool_field1'] != y['bool_field1']:
                    print("actual_result is %s" % actual_result)
                    print("expected result is %s" % expected_result)
                    extra_msg = self._get_failure_message(expected_result, actual_result)
                    raise Exception(msg+"\n "+extra_msg)
        elif window_function_test:
            for x, y in zip(actual_result, expected_result):
                if not x['wf']:
                    x['wf'] = 0
                if not y['wf']:
                    y['wf'] = 0
                if type(x['wf']) == list and type(y['wf']) == str:
                    diffs = DeepDiff(x['wf'], eval(y['wf']), ignore_order=True, ignore_numeric_type_changes=True)
                    if diffs:
                        diff = 1
                    else:
                        diff = 0
                else:
                    max_val = max([x['wf'], y['wf']])
                    min_val = min([x['wf'], y['wf']])
                    diff = max_val - min_val
                if x['char_field1'] != y['char_field1'] or x['decimal_field1'] != y['decimal_field1'] or diff > delta:
                    print("actual_result is %s" % x)
                    print("expected result is %s" % y)
                    extra_msg = self._get_failure_message(expected_result, actual_result)
                    raise Exception(msg + "\n " + extra_msg)
        else:
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True, ignore_numeric_type_changes=True, ignore_string_case=True)
            if diffs:
                self.log.info("-->actual vs expected diffs found:{}".format(diffs))
                raise Exception("-->actual vs expected diffs found:{}".format(diffs))

    def _sort_data(self, result):
        new_data = []
        for data in result:
            new_data.append(sorted(data))
        return new_data

    def _verify_results_crud_rqg(self, n1ql_result=[], sql_result=[], hints=["primary_key_id"]):
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

        expected_result = sql_result

        if len(actual_result) != len(expected_result):
            extra_msg = self._get_failure_message(expected_result, actual_result)
            raise Exception("Results are incorrect. Actual num %s. Expected num: %s.:: %s \n" % (len(actual_result), len(expected_result), extra_msg))
        diffs = DeepDiff(actual_result, expected_result, ignore_order=True, ignore_numeric_type_changes=True)
        if diffs:
            self.log.info("-->actual vs expected diffs found:{}".format(diffs))
            raise Exception("-->actual vs expected diffs found:{}".format(diffs))

    def _get_failure_message(self, expected_result, actual_result):
        if expected_result is None:
            expected_result = []
        if actual_result is None:
            actual_result = []
        len_expected_result = len(expected_result)
        len_actual_result = len(actual_result)
        len_expected_result = min(5, len_expected_result)
        len_actual_result = min(5, len_actual_result)
        extra_msg = "mismatch in results :: expected :: {0}, actual :: {1} ".format(expected_result[0:len_expected_result], actual_result[0:len_actual_result])
        return extra_msg

    def _result_comparison_analysis(self, expected_result, actual_result):
        expected_map = {}
        actual_map = {}
        for data in expected_result:
            primary=None
            for key in list(data.keys()):
                keys = key
                if keys.encode('ascii') == "primary_key_id":
                    primary = keys
            expected_map[data[primary]] = data
        for data in actual_result:
            primary = None
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
        if expected_result is None:
            expected_result = []
        if actual_result is None:
            actual_result = []
        if len(expected_result) == 1:
            value = list(expected_result[0].values())[0]
            if value is None or value == 0:
                expected_result = []
        if len(actual_result) == 1:
            value = list(actual_result[0].values())[0]
            if value is None or value == 0:
                actual_result = []
        return expected_result, actual_result

    def _is_function_in_result(self, result):
        if result == "FUN":
            return True
        return False

    def _verify_results_rqg_for_function(self, n1ql_result=[], sql_result=[], hints=["a1"], aggregate_pushdown=False):
        if not aggregate_pushdown:
            sql_result, n1ql_result = self._analyze_for_special_case_using_func(sql_result, n1ql_result)
        if len(sql_result) != len(n1ql_result):
            msg = "the number of results do not match :: sql = {0}, n1ql = {1}".format(len(sql_result), len(n1ql_result))
            extra_msg = self._get_failure_message(sql_result, n1ql_result)
            raise Exception(msg+"\n"+extra_msg)
        n1ql_result = self._gen_dict_n1ql_func_result(n1ql_result)
        sql_result = self._gen_dict_n1ql_func_result(sql_result)
        if len(sql_result) == 0 and len(n1ql_result) == 0:
            return
        diffs = DeepDiff(n1ql_result, sql_result, ignore_order=True, ignore_numeric_type_changes=True)
        if diffs:
            self.log.info("-->actual vs expected diffs found:{0}".format(diffs))
            raise Exception("mismatch in results:{0}".format(diffs))

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
        missing_keys = []
        different_values = []
        for key in list(expected.keys()):
            if key not in list(actual.keys()):
                missing_keys.append(key)
            if expected[key] != actual[key]:
                different_values.append("for key {0}, expected {1} \n actual {2}".format(key, expected[key], actual[key]))
        self.log.info(missing_keys)
        if len(different_values) > 0:
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
            url += "couchbase-query_%s_%s_linux.tar.gz" % (version, info.architecture_type)
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
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd %s/src/github.com/couchbase/query//server/main; " % (gopath) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" % (server.ip, server.port)
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
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" % (server.ip, server.port)
                n1ql_port = self.input.param("n1ql_port", None)
                if server.ip == "127.0.0.1" and server.n1ql_port:
                    n1ql_port = server.n1ql_port
                if n1ql_port:
                    cmd = "cd %s; " % (couchbase_path) +\
                './cbq-engine -datastore http://%s:%s/ -http=":%s">n1ql.log 2>&1 &' % (server.ip, server.port, n1ql_port)
            self.shell.execute_command(cmd)
        else:
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
            else:
                cmd = "cd /cygdrive/c/tuq;./cbq-engine.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" % (server.ip, server.port)
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
        for _ in self.servers:
            shell_connection = RemoteMachineShellConnection(self.master)
            shell_connection.execute_command(cmd)

    def drop_primary_index(self, using_gsi = True, server = None):
        if server is None:
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
                    self.run_cbq_query(server=server)
            except Exception as ex:
                self.log.error('ERROR during index creation %s' % str(ex))

    def create_primary_index(self, using_gsi=True, server=None, num_index_replicas=0):
        if server is None:
            server = self.master
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % bucket.name
            if num_index_replicas > 0:
                self.query += f' with {{"num_replica": {num_index_replicas} }}'
            if self.use_rest:
                try:
                    self.log.info("Check if index existed in {0} on server {1}".format(bucket.name, server.ip))
                    check = self._is_index_in_list(bucket.name, "#primary", server=server)
                    if not check:
                        self.log.info("Create primary index")
                        self.run_cbq_query(server=server, query_params={'timeout': '900s'})
                        self.log.info("Check if index is online")
                        check = self.is_index_online_and_in_list(bucket.name, "#primary", server=server)
                        if not check:
                            raise Exception(" Timed-out Exception while building primary index for bucket {0} !!!".format(bucket.name))
                    else:
                        raise Exception(" Primary Index Already present, This looks like a bug !!!")
                except Exception as ex:
                    self.log.error('ERROR during index creation %s' % str(ex))
                    raise ex

    def create_partitioned_primary_index(self, using_gsi=True, server=None):
        if server is None:
            server = self.master
        for bucket in self.buckets:
            self.query = "CREATE PRIMARY INDEX ON %s " % bucket.name
            if using_gsi:
                self.query += " PARTITION BY HASH(meta().id) USING GSI"
            if not using_gsi:
                self.query += " USING VIEW "
            if self.use_rest:
                try:
                    check = self._is_index_in_list(bucket.name, "#primary",
                                                   server=server)
                    if not check:
                        self.run_cbq_query(server=server,
                                           query_params={'timeout': '900s'})
                        check = self.is_index_online_and_in_list(bucket.name,
                                                                 "#primary",
                                                                 server=server)
                        if not check:
                            raise Exception(
                                " Timed-out Exception while building primary index for bucket {0} !!!".format(
                                    bucket.name))
                    else:
                        raise Exception(
                            " Primary Index Already present, This looks like a bug !!!")
                except Exception as ex:
                    self.log.error('ERROR during index creation %s' % str(ex))
                    raise ex

    def verify_index_with_explain(self, actual_result, index_name, check_covering_index=False):
        check = True
        if check_covering_index:
            if "covering" in str(actual_result):
                check = True
            else:
                check = False
        if index_name in str(actual_result):
            return True and check
        return False

    def verify_explain(self, actual_result, keyword="", present=True):
        if keyword in str(actual_result) and present:
            return True
        elif keyword not in str(actual_result) and not present:
            return True
        else:
            return False

    def run_query_and_verify_result(self, server=None, query=None, timeout=120.0, max_try=1, expected_result=None,
                                    scan_consistency=None, scan_vector=None, verify_results=True):
        check = False
        init_time = time.time()
        try_count = 0
        while not check:
            next_time = time.time()
            try:
                actual_result = self.run_cbq_query(query=query, server=server, scan_consistency=scan_consistency,
                                                   scan_vector=scan_vector)
                #self.log.info("-->actual_result={}".format(actual_result))
                if verify_results:
                    #self._verify_results(sorted(actual_result['results']), sorted(expected_result))
                    self._verify_results(actual_result['results'], expected_result)
                else:
                    return "ran query with success and validated results", True
                check = True
            except Exception as ex:
                traceback.print_exc()
                if next_time - init_time > timeout or try_count >= max_try:
                    return ex, False
            finally:
                try_count += 1
        return "ran query with success and validated results", check

    def get_index_names(self, server=None):
        query = "select distinct(name) from system:indexes where `using`='gsi'"
        index_names = []
        if server is None:
            server = self.master
        res = self.run_cbq_query(query=query, server=server)
        for item in res['results']:
            index_names.append(item['name'])
        return index_names

    def is_index_online_and_in_list(self, bucket, index_name, server=None, timeout=600.0):
        check = self._is_index_in_list(bucket, index_name, server=server)
        init_time = time.time()
        while not check:
            time.sleep(1)
            check = self._is_index_in_list(bucket, index_name, server=server)
            next_time = time.time()
            if check or (next_time - init_time > timeout):
                return check
        return check

    def is_index_ready_and_in_list(self, bucket, index_name, server=None, timeout=600.0):
        query = "SELECT * FROM system:indexes where name = \'{0}\'".format(index_name)
        if server is None:
            server = self.master
        init_time = time.time()
        check = False
        while not check:
            res = self.run_cbq_query(query=query, server=server)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    check = False
                elif item['indexes']['keyspace_id'] == str(bucket) \
                        and item['indexes']['name'] == index_name \
                        and item['indexes']['state'] == "online":
                    check = True
            time.sleep(1)
            next_time = time.time()
            check = check or (next_time - init_time > timeout)
        return check

    def is_index_online_and_in_list_bulk(self, bucket, index_names=[], server=None, index_state="online", timeout=600.0):
        check, index_names = self._is_index_in_list_bulk(bucket, index_names, server=server, index_state=index_state)
        init_time = time.time()
        while not check:
            check, index_names = self._is_index_in_list_bulk(bucket, index_names, server=server, index_state=index_state)
            next_time = time.time()
            if check or (next_time - init_time > timeout):
                return check
        return check

    def gen_build_index_query(self, bucket="default", index_list=[]):
        return "BUILD INDEX on {0}({1}) USING GSI".format(bucket, ",".join(index_list))

    def gen_query_parameter(self, scan_vector=None, scan_consistency=None):
        query_params = {}
        if scan_vector:
            query_params.update("scan_vector", scan_vector)
        if scan_consistency:
            query_params.update("scan_consistency", scan_consistency)
        return query_params

    def _is_index_in_list(self, bucket, index_name, server=None, index_state=["pending", "building", "deferred"]):
        query = "SELECT * FROM system:indexes where name = \'{0}\'".format(index_name)
        if server is None:
            server = self.master
        try:
            res = self.run_cbq_query(query=query, server=server)
            if res and res['results']:
                for item in res['results']:
                    if item['indexes'] and 'keyspace_id' not in item['indexes']:
                        return False
                    if item['indexes']['keyspace_id'] == str(bucket) and item['indexes']['name'] == index_name and item['indexes']['state'] not in index_state:
                        return True
            else:
                self.log.error("Fail to get index list.  List output: {0}".format(res))
        except Exception as e:
            self.log.error("Query index in list throws exception: {0}".format(str(e)))
            raise Exception(str(e))
        return False

    def _is_index_in_list_bulk(self, bucket, index_names=[], server=None, index_state=["pending","building"]):
        query = "SELECT * FROM system:indexes"
        if server is None:
            server = self.master
        res = self.run_cbq_query(query=query, server=server)
        found_index_list = []
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                return False
            for index_name in index_names:
                if item['indexes']['keyspace_id'] == str(bucket) and item['indexes']['name'] == index_name and item['indexes']['state'] not in index_state:
                    found_index_list.append(index_name)
        if len(found_index_list) == len(index_names):
            return True, []
        return False, list(set(index_names) - set(found_index_list))

    def gen_index_map(self, server=None):
        query = "SELECT * FROM system:indexes"
        if server is None:
            server = self.master
        res = self.run_cbq_query(query=query, server=server)
        index_map = {}
        for item in res['results']:
            bucket_name = item['indexes']['keyspace_id'].encode('ascii', 'ignore')
            if bucket_name not in list(index_map.keys()):
                index_map[bucket_name] = {}
            index_name = str(item['indexes']['name'])
            index_map[bucket_name][index_name] = {}
            index_map[bucket_name][index_name]['state'] = item['indexes']['state']
        return index_map

    def get_index_count_using_primary_index(self, buckets, server=None):
        query = "SELECT COUNT(*) FROM {0}"
        map= {}
        if server is None:
            server = self.master
        for bucket in buckets:
            res = self.run_cbq_query(query=query.format(bucket.name), server=server)
            map[bucket.name] = int(res["results"][0]["$1"])
        return map

    def get_index_count_using_index(self, bucket, index_name, server=None):
        query = 'SELECT COUNT(*) FROM {0} USE INDEX ({1})'.format(bucket.name, index_name)
        if not server:
            server = self.master
        res = self.run_cbq_query(query=query, server=server)
        return int(res['results'][0]['$1'])

    def get_index_count_using_index_collection(self, keyspace, index_name, server=None):
        query = 'SELECT COUNT(*) FROM {0} USE INDEX ({1})'.format(keyspace, index_name)
        if not server:
            server = self.master
        res = self.run_cbq_query(query=query, server=server)
        return int(res['results'][0]['$1'])

    def get_collection_itemCount(self, keyspace, server=None):
        query = 'SELECT COUNT(*) FROM {0}'.format(keyspace)
        if not server:
            server = self.master
        res = self.run_cbq_query(query=query, server=server)
        return int(res['results'][0]['$1'])

    def _gen_dict(self, result):
        result_set = []
        if result is not None and len(result) > 0:
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
                elif value == 'None':
                    new_result_set.append(None)
                else:
                    new_result_set.append(value)
        else:
            new_result_set = result_set
        return new_result_set

    def _check_sample(self, result, expected_in_key=None):
        if expected_in_key == "FUN":
            return False
        if expected_in_key is None or len(expected_in_key) == 0:
            return False
        if result is not None and len(result) > 0:
            sample = result[0]
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
            if result is not None and len(result) > 0:
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

    def _set_env_variable(self, server):
        self.shell.execute_command("export NS_SERVER_CBAUTH_URL=\"http://{0}:{1}/_cbauth\"".format(server.ip, server.port))
        self.shell.execute_command("export NS_SERVER_CBAUTH_USER=\"{0}\"".format(server.rest_username))
        self.shell.execute_command("export NS_SERVER_CBAUTH_PWD=\"{0}\"".format(server.rest_password))
        self.shell.execute_command("export NS_SERVER_CBAUTH_RPC_URL=\"http://{0}:{1}/cbauth-demo\"".format(server.ip, server.port))
        self.shell.execute_command("export CBAUTH_REVRPC_URL=\"http://{0}:{1}@{2}:{3}/query\"".format(server.rest_username, server.rest_password, server.ip, server.port))

    def verify_indexes_redistributed(self, map_before_rebalance, map_after_rebalance, stats_map_before_rebalance,
                                     stats_map_after_rebalance, nodes_in, nodes_out, swap_rebalance=False,
                                     use_https=False, item_count_increase=False, per_node=False,
                                     skip_array_index_item_count=False, indexes_changed=False):
        # verify that number of indexes before and after rebalance are same
        no_of_indexes_before_rebalance = 0
        no_of_indexes_after_rebalance = 0
        for bucket in map_before_rebalance:
            no_of_indexes_before_rebalance += len(map_before_rebalance[bucket])
        for bucket in map_after_rebalance:
            no_of_indexes_after_rebalance += len(map_after_rebalance[bucket])
        self.log.info("Number of indexes before rebalance : {0}".format(no_of_indexes_before_rebalance))
        self.log.info("Number of indexes after rebalance  : {0}".format(no_of_indexes_after_rebalance))

        # verify that index names before and after rebalance are same
        index_names_before_rebalance = []
        index_names_after_rebalance = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                index_names_before_rebalance.append(index)
        for bucket in map_after_rebalance:
            for index in map_after_rebalance[bucket]:
                index_names_after_rebalance.append(index)
        self.log.info("Index names before rebalance : {0}".format(sorted(index_names_before_rebalance)))
        self.log.info("Index names after rebalance  : {0}".format(sorted(index_names_after_rebalance)))
        if no_of_indexes_before_rebalance != no_of_indexes_after_rebalance and not indexes_changed:
            self.log.info(f"some indexes are missing after rebalance")
            self.log.info(f"indexes before rebalance : {index_names_before_rebalance}")
            self.log.info(f"indexes after rebalance : {index_names_after_rebalance}")
            raise Exception("some indexes are missing after rebalance")
        if not indexes_changed and sorted(index_names_before_rebalance) != sorted(index_names_after_rebalance):
            self.log.info("number of indexes are same but index names don't match")
            raise Exception("number of indexes are same but index names don't match")
        # verify that rebalanced out nodes are not present
        host_names_before_rebalance = []
        host_names_after_rebalance = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                if type(map_before_rebalance[bucket][index]['hosts']) is list:
                    for host in map_before_rebalance[bucket][index]['hosts']:
                        host_names_before_rebalance.append(host)
                else:
                    host_names_before_rebalance.append(map_before_rebalance[bucket][index]['hosts'])
        indexer_nodes_before_rebalance = sorted(set(host_names_before_rebalance))
        for bucket in map_after_rebalance:
            for index in map_after_rebalance[bucket]:
                if type(map_after_rebalance[bucket][index]['hosts']) is list:
                    for host in map_after_rebalance[bucket][index]['hosts']:
                        host_names_after_rebalance.append(host)
                else:
                    host_names_after_rebalance.append(map_after_rebalance[bucket][index]['hosts'])
        indexer_nodes_after_rebalance = sorted(set(host_names_after_rebalance))
        indexer_nodes_after_rebalance = [item.split(":")[0] for item in indexer_nodes_after_rebalance]
        indexer_nodes_before_rebalance = [item.split(":")[0] for item in indexer_nodes_before_rebalance]
        self.log.info("Host names of indexer nodes before rebalance : {0}".format(indexer_nodes_before_rebalance))
        self.log.info("Host names of indexer nodes after rebalance  : {0}".format(indexer_nodes_after_rebalance))
        # indexes need to redistributed in case of rebalance out, not necessarily in case of rebalance in
        if nodes_out and indexer_nodes_before_rebalance == indexer_nodes_after_rebalance:
            self.log.info("Even after rebalance some of rebalanced out nodes still have indexes")
            raise Exception("Even after rebalance some of rebalanced out nodes still have indexes")
        for node_out in nodes_out:
            if node_out in indexer_nodes_after_rebalance:
                self.log.info("rebalanced out node still present after rebalance {0} : {1}".format(node_out,
                                                                                                   indexer_nodes_after_rebalance))
                raise Exception("rebalanced out node still present after rebalance")
        if swap_rebalance:
            for node_in in nodes_in:
                ip_address = f"{node_in.ip}"
                if ip_address not in indexer_nodes_after_rebalance:
                    self.log.info("swap rebalanced in node is not distributed any indexes")
                    raise Exception("swap rebalanced in node is not distributed any indexes")


        # verify that index status before and after rebalance are same
        index_state_before_rebalance = {}
        index_state_after_rebalance = {}
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                index_state_before_rebalance[index] = map_before_rebalance[bucket][index]["status"]
        for bucket in map_after_rebalance:
            for index in map_after_rebalance[bucket]:
                index_state_after_rebalance[index] = map_after_rebalance[bucket][index]["status"]
        self.log.info("index status of indexes rebalance {0}".format(index_state_before_rebalance))
        self.log.info("index status of indexes rebalance {0}".format(index_state_after_rebalance))

        index_distribution_map_before_rebalance = {}
        index_distribution_map_after_rebalance = {}
        for node in host_names_before_rebalance:
            index_distribution_map_before_rebalance[node] = index_distribution_map_before_rebalance.get(node, 0) + 1
        for node in host_names_after_rebalance:
            index_distribution_map_after_rebalance[node] = index_distribution_map_after_rebalance.get(node, 0) + 1
        self.log.info("Distribution of indexes before rebalance")
        for k, v in index_distribution_map_before_rebalance.items():
            print(k, v)
        self.log.info("Distribution of indexes after rebalance")
        for k, v in index_distribution_map_after_rebalance.items():
            print(k, v)
        # verify that items_count before and after rebalance are same
        if not indexes_changed:
            self.validate_item_count_data_size(map_before_rebalance=map_before_rebalance, map_after_rebalance=map_after_rebalance,
                                              stats_map_before_rebalance=stats_map_before_rebalance,
                                              stats_map_after_rebalance=stats_map_after_rebalance,
                                              item_count_increase=item_count_increase,
                                              per_node=per_node, skip_array_index_item_count=skip_array_index_item_count)

            diffs = DeepDiff(index_state_before_rebalance, index_state_after_rebalance, ignore_order=True)
            if diffs:
                self.log.info(diffs)
                raise Exception("index status mismatch")

            # Rebalance is not guaranteed to achieve a balanced cluster.
            # The indexes will be distributed in a manner to satisfy the resource requirements of each index.
            # Hence printing the index distribution just for logging/debugging purposes


    def validate_item_count_data_size(self, map_before_rebalance, map_after_rebalance, stats_map_before_rebalance,
                                     stats_map_after_rebalance, item_count_increase=False, per_node=False,
                                      skip_array_index_item_count=False):
        items_count_before_rebalance = {}
        items_count_after_rebalance = {}
        if not per_node:
            for bucket in stats_map_before_rebalance:
                for index in stats_map_before_rebalance[bucket]:
                    items_count_before_rebalance[index] = stats_map_before_rebalance[bucket][index][
                        "items_count"]
            for bucket in stats_map_after_rebalance:
                for index in stats_map_after_rebalance[bucket]:
                    items_count_after_rebalance[index] = stats_map_after_rebalance[bucket][index]["items_count"]
            self.log.info("item_count of indexes before rebalance {0}".format(items_count_before_rebalance))
            self.log.info("item_count of indexes after rebalance {0}".format(items_count_after_rebalance))
            diffs = DeepDiff(items_count_before_rebalance, items_count_after_rebalance, ignore_order=True)
            if diffs:
                self.log.info(diffs)
                if not item_count_increase:
                    raise Exception("items_count mismatch")
        else:
            items_count_before_rebalance, data_size_before_rebalance, partitioned_indexes_dict_item_count, \
            partitioned_indexes_dict_data_size, array_indexes_dict_item_count_before, \
            array_indexes_dict_data_size_before = self._create_dicts(map_before_rebalance, stats_map_before_rebalance)
            items_count_after_rebalance, data_size_after_rebalance, partitioned_indexes_dict_item_count_after, \
            partitioned_indexes_dict_data_size_after, array_indexes_dict_item_count_after, \
            array_indexes_dict_data_size_after = self._create_dicts(map_after_rebalance, stats_map_after_rebalance)
            self.log.info("Compare items count for indexes before and after rebalance")
            self._find_differences(items_count_before_rebalance, items_count_after_rebalance, item_count_increase=item_count_increase)
            self.log.info("Compare data size for indexes before and after rebalance")
            # TODO Remove after MB-58829 is fixed
            try:
                self._find_differences(data_size_before_rebalance, data_size_after_rebalance, item_count_increase=item_count_increase, delta_allowed=0.25)
                raise Exception("Ignoring data size change exception.")
            except:
                pass
            self.log.info("Compare items count for partitioned indexes before and after rebalance")
            self._find_differences(partitioned_indexes_dict_item_count, partitioned_indexes_dict_item_count_after, item_count_increase=item_count_increase)
            self.log.info("Compare data size for partitioned indexes before and after rebalance")
            # TODO Remove after MB-58829 is fixed
            try:
                self._find_differences(partitioned_indexes_dict_data_size, partitioned_indexes_dict_data_size_after, item_count_increase=item_count_increase, delta_allowed=0.25)
                raise Exception("Ignoring data size change exception.")
            except:
                pass
            if not skip_array_index_item_count:
                self.log.info("Compare items count for array indexes before and after rebalance")
                self._find_differences(array_indexes_dict_item_count_before, array_indexes_dict_item_count_after,
                                       item_count_increase=item_count_increase)
                self.log.info("Compare data size for array indexes before and after rebalance")
                # TODO Remove after MB-58829 is fixed
                try:
                    self._find_differences(array_indexes_dict_data_size_before, array_indexes_dict_data_size_after,
                                       item_count_increase=item_count_increase, delta_allowed=0.25)
                    raise Exception("Ignoring data size change exception.")
                except:
                    pass

    def _find_differences(self, count_before, count_after, item_count_increase=False, delta_allowed=0.0):
        """
        finds differences in the index metadata before and after rebalance.
        """
        diffs = DeepDiff(count_before, count_after,
                         ignore_order=True)
        if diffs:
            self.log.info(f"Diffs {diffs}")
            if not item_count_increase:
                for key in diffs['values_changed']:
                    if diffs['values_changed'][key]['new_value'] > diffs['values_changed'][key]['old_value']:
                        maxval = diffs['values_changed'][key]['new_value']
                        minval = diffs['values_changed'][key]['old_value']
                    else:
                        maxval = diffs['values_changed'][key]['old_value']
                        minval = diffs['values_changed'][key]['new_value']
                    if (maxval - minval) / diffs['values_changed'][key]['old_value'] <= delta_allowed:
                        self.log.info(
                            f"Ignoring change of up to 25% increase/decrease in data size. Percentage change {100 * (maxval - minval) / diffs['values_changed'][key]['old_value']}")
                    else:
                        self.log.error(
                            f"Index data sizes differ by more than {delta_allowed*100} percent before and after rebalance.{diffs}. Percentage change {100 * (maxval - minval) / diffs['values_changed'][key]['old_value']}")
                        raise Exception("Item count/data size change of more than 25%")
            else:
                for key in diffs['values_changed']:
                    if diffs['values_changed'][key]['new_value'] <= diffs['values_changed'][key]['old_value']:
                        raise Exception(
                            f"Item count/data size increased for indexes post rebalance. Difference {diffs}")

    def _create_dicts(self, map_metadata, map_stats):
        """
        forms a map of index metadata and the index names.
        also forms a map for partitioned and array index maps separately.
        """
        partitioned_indexes_list, partitioned_indexes_dict_item_count, partitioned_indexes_dict_data_size = [], {}, {}
        items_count_dict, data_size_dict = {}, {}
        array_index_list = []
        array_indexes_dict_item_count, array_indexes_dict_data_size = {}, {}
        for bucket in map_metadata:
            for index in map_metadata[bucket]:
                if map_metadata[bucket][index]['partitioned']:
                    partitioned_indexes_list.append(index)
                if "array" in map_metadata[bucket][index]['definition']:
                    array_index_list.append(index)
        for host in map_stats:
            for bucket in map_stats[host]:
                for index in map_stats[host][bucket]:
                    if index not in partitioned_indexes_list and index not in array_index_list:
                        items_count_dict[index] = map_stats[host][bucket][index][
                            "items_count"]
                        data_size_dict[index] = map_stats[host][bucket][index][
                            "data_size"]
                    elif index in partitioned_indexes_list:
                        if index not in partitioned_indexes_dict_item_count:
                            partitioned_indexes_dict_item_count[index] = \
                                map_stats[host][bucket][index][
                                    "items_count"]
                        else:
                            partitioned_indexes_dict_item_count[index] += \
                                map_stats[host][bucket][index][
                                    "items_count"]
                        if index not in partitioned_indexes_dict_data_size:
                            partitioned_indexes_dict_data_size[index] = \
                                map_stats[host][bucket][index]["data_size"]
                        else:
                            partitioned_indexes_dict_data_size[index] += \
                                map_stats[host][bucket][index]["data_size"]
                    elif index in array_index_list:
                        if index not in array_indexes_dict_item_count:
                            array_indexes_dict_item_count[index] = \
                                map_stats[host][bucket][index][
                                    "items_count"]
                        else:
                            array_indexes_dict_item_count[index] += \
                                map_stats[host][bucket][index][
                                    "items_count"]
                        if index not in array_indexes_dict_data_size:
                            array_indexes_dict_data_size[index] = \
                                map_stats[host][bucket][index]["data_size"]
                        else:
                            array_indexes_dict_data_size[index] += \
                                map_stats[host][bucket][index]["data_size"]
        return items_count_dict, data_size_dict, partitioned_indexes_dict_item_count, \
               partitioned_indexes_dict_data_size, array_indexes_dict_item_count, array_indexes_dict_data_size

    def verify_replica_indexes(self, index_names, index_map, num_replicas, expected_nodes=None, dropped_replica=False, replicaId=None):
        # 1. Validate count of no_of_indexes
        # 2. Validate index names
        # 3. Validate index replica have the same id
        # 4. Validate index replicas are on different hosts

        nodes = []
        skip_replica_check = False
        for index_name in index_names:
            index_host_name, index_id = self.get_index_details_using_index_name(index_name, index_map)
            nodes.append(index_host_name)

            for i in range(0, num_replicas):
                if dropped_replica:
                    if not i+1 == replicaId:
                        index_replica_name = index_name + " (replica {0})".format(str(i+1))
                    else:
                        skipped_index = index_name + " (replica {0})".format(str(i+1))
                        skip_replica_check = True
                else:
                    index_replica_name = index_name + " (replica {0})".format(str(i + 1))
                try:
                    if not skip_replica_check:
                        index_replica_hostname, index_replica_id = self.get_index_details_using_index_name(
                            index_replica_name, index_map)
                except Exception as ex:
                    self.log.info(str(ex))
                    raise Exception(str(ex))

                if not skip_replica_check:
                    self.log.info("Hostnames : %s , %s" % (index_host_name, index_replica_hostname))
                    self.log.info("Index IDs : %s, %s" % (index_id, index_replica_id))

                    nodes.append(index_replica_hostname)

                    if index_id != index_replica_id:
                        self.log.info("Index ID for main index and replica indexes not same")
                        raise Exception("index id different for replicas")

                    if index_host_name == index_replica_hostname:
                        self.log.info("Index hostname for main index and replica indexes are same")
                        raise Exception("index hostname same for replicas")
                else:
                    try:
                        index_replica_hostname, index_replica_id = self.get_index_details_using_index_name(
                            skipped_index, index_map)
                    except Exception as ex:
                        self.log.info(str(ex))
                        continue
                    raise Exception("Replica is still present when it should have been dropped")

        if expected_nodes:
            expected_nodes = expected_nodes.sort()
            nodes = nodes.sort()
            if not expected_nodes == nodes:
                raise Exception("Replicas not created on expected hosts")

    def verify_replica_indexes_build_status(self, index_map, num_replicas, defer_build=False):

        index_names = self.get_index_names()

        for index_name in index_names:
            index_status, index_build_progress = self.get_index_status_using_index_name(index_name, index_map)
            if not defer_build and index_status != "Ready":
                self.log.info("Expected %s status to be Ready, but it is %s" % (index_name, index_status))
                raise Exception("Index status incorrect")
            elif defer_build and index_status != "Created":
                self.log.info(
                    "Expected %s status to be Created, but it is %s" % (index_name, index_status))
                raise Exception("Index status incorrect")
            else:
                self.log.info("index_name = %s, defer_build = %s, index_status = %s" % (index_name, defer_build, index_status))

            for i in range(1, num_replicas+1):
                index_replica_name = index_name + " (replica {0})".format(str(i))
                try:
                    index_replica_status, index_replica_progress = self.get_index_status_using_index_name(index_replica_name, index_map)
                except Exception as ex:
                    self.log.info(str(ex))
                    raise Exception(str(ex))

                if not defer_build and index_replica_status != "Ready":
                    self.log.info("Expected %s status to be Ready, but it is %s" % (index_replica_name, index_replica_status))
                    raise Exception("Index status incorrect")
                elif defer_build and index_replica_status != "Created":
                    self.log.info("Expected %s status to be Created, but it is %s" % (index_replica_name, index_replica_status))
                    raise Exception("Index status incorrect")
                else:
                    self.log.info("index_name = %s, defer_build = %s, index_replica_status = %s" % (index_replica_name, defer_build, index_status))

    def get_index_details_using_index_name(self, index_name, index_map):
        for key in index_map.keys():
            if index_name in list(index_map[key].keys()):
                return index_map[key][index_name]['hosts'], index_map[key][index_name]['id']
        raise Exception("Index does not exist - {0}".format(index_name))

    def get_index_status_using_index_name(self, index_name, index_map):
        for key in index_map.keys():
            if index_name in list(index_map[key].keys()):
                return index_map[key][index_name]['status'], \
                       index_map[key][index_name]['progress']
            else:
                raise Exception("Index does not exist - {0}".format(index_name))
