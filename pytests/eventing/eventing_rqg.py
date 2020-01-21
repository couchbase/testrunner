import os, re
import zipfile
import datetime
import logging
from threading import Thread
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_ERROR
from pytests.eventing.eventing_base import EventingBaseTest, log
from lib.couchbase_helper.tuq_helper import N1QLHelper
from string import Template

log = logging.getLogger()

class EventingRQG(EventingBaseTest):
    def setUp(self):
        super(EventingRQG, self).setUp()
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.n1ql_helper = N1QLHelper(shell=self.shell,
                                      max_verify=self.max_verify,
                                      buckets=self.buckets,
                                      item_flag=self.item_flag,
                                      n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list,
                                      log=self.log, input=self.input,
                                      master=self.master,
                                      use_rest=True
                                      )
        self.number_of_handler = self.input.param('number_of_handler', 5)
        self.number_of_queries = self.input.param('number_of_queries', None)
        self.template_file=self.input.param('template_file', 'b/resources/rqg/simple_table_db/query_tests_using_templates/query_10000_fields.txt.zip')

    having_map = {"STRING_FIELD ": "email ", "NUMERIC_FIELD ": "age ", "UPPER_BOUND_VALUE": "8",
                  "LOWER_BOUND_VALUE": "0", "NUMERIC_FIELD_LIST": "age", "STRING_FIELD_LIST": "email",
                  "( LIST )": "[1,2,3]"}

    update_map = {"STRING_FIELD ": "email ", "NUMERIC_FIELD ": "age ", "UPPER_BOUND_VALUE": "8",
                  "LOWER_BOUND_VALUE": "0", "NUMERIC_FIELD_LIST": "age", "STRING_FIELD_LIST": "email",
                  "( LIST )": "[1,2,3]","STRING_FIELD,NUMERIC_FIELD,DATETIME_FIELD":"email=\"update@a.c\",age=4,created='2010-09-15 00:00:00'"}

    join_map = {"PREVIOUS_TABLE.FIELD":"src_bucket.email","CURRENT_TABLE.FIELD":"_bucket.email","STRING_FIELD ": "email ", "NUMERIC_FIELD ": "age ", "UPPER_BOUND_VALUE": "8",
                  "LOWER_BOUND_VALUE": "0", "NUMERIC_FIELD_LIST": "age", "STRING_FIELD_LIST": "email",
                  "( LIST )": "[1,2,3]"}
    field_map = {"NUMERIC_VALUE":"0","STRING_VALUES":"\"a@b.c\""}

    def tearDown(self):
        super(EventingRQG, self).tearDown()

    def test_random_n1ql(self):
        test_file_path = self.unzip_template(
            self.template_file)
        with open(test_file_path) as f:
            query_list = f.readlines()
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
        log.info(len(query_list))
        k = self.number_of_handler
        if self.number_of_queries is None:
            s = len(query_list)
        else:
            s = self.number_of_queries
        for j in range(0, s, k):
            try:
                threads = []
                for i in range(j, j + k):
                    if i >= s:
                        break
                    threads.append(Thread(target=self.create_function_and_deploy, args={query_list[i]}))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                key = datetime.datetime.now().time()
                self.sleep(10)
                query = "insert into src_bucket (KEY, VALUE) VALUES (\"" + str(key) + "\",\"doc created\")"
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
                self.eventing_stats()
            except Exception as e:
                log.error(e)
            finally:
                self.undeploy_delete_all_functions()
                self.delete_temp_handler_code()
        self.verify_n1ql_stats(s)

    def test_queries(self):
        test_file_path = self.template_file
        with open(test_file_path) as f:
            query_list = f.readlines()
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
        k = self.number_of_handler
        if self.number_of_queries is None:
            s = len(query_list)
        else:
            s = self.number_of_queries
        log.info(s)
        for j in range(0, s, k):
            try:
                threads = []
                for i in range(j, j + k):
                    if i >= s:
                        break
                    threads.append(Thread(target=self.create_function_and_deploy, args=(query_list[i], False)))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                key = datetime.datetime.now().time()
                query = "insert into src_bucket (KEY, VALUE) VALUES (\""+str(key)+"\",{\"email\":\"a@b.c\"})"
                self.log.info("insert doc:{}".format(query))
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
                self.eventing_stats()
            except Exception as e:
                log.error(e)
            finally:
                self.undeploy_delete_all_functions()
        self.delete_temp_handler_code()
        self.verify_n1ql_stats(s)


    def create_function_and_deploy(self, query, replace=True):
        log.info("creating handler code for :{}".format(query))
        if replace:
            file_path = self.generate_eventing_file(self._convert_template_n1ql(query))
        else:
            file_path = self.generate_eventing_file(query)
        self.sleep(10)
        ts = datetime.datetime.now().strftime('%m%d%y%H%M%S%f')
        body = self.create_save_function_body(self.function_name + str(ts), file_path,
                                              dcp_stream_boundary="from_now", worker_count=1, execution_timeout=60)
        self.deploy_function(body)


    def _convert_template_n1ql(self, query):
        n1ql = str(query).replace("BUCKET_NAME", self.src_bucket_name)
        n1ql = str(n1ql).replace("TRUNCATE", "TRUNC")
        for k, v in list(self.field_map.items()):
            n1ql = str(n1ql).replace(k, v)
        if "HAVING" in n1ql:
            for k, v in list(self.having_map.items()):
                n1ql=str(n1ql).replace(k, v)
            group_fields = re.search(r'GROUP BY(.*?)HAVING', n1ql).group(1)
            n1ql = n1ql.replace("GROUPBY_FIELDS", group_fields)
        elif "GROUP BY" in n1ql:
            for k, v in list(self.having_map.items()):
                n1ql=str(n1ql).replace(k, v)
            group_fields = re.search(r'GROUP BY(.*?);', n1ql).group(1)
            n1ql = n1ql.replace("GROUPBY_FIELDS", group_fields)
        if "UPDATE" in n1ql:
            for k, v in list(self.update_map.items()):
                n1ql = str(n1ql).replace(k, v)
            n1ql = n1ql.replace(self.src_bucket_name, self.dst_bucket_name)
        if "JOIN" in n1ql:
            for k, v in list(self.join_map.items()):
                n1ql = str(n1ql).replace(k, v)
        return n1ql

    def generate_eventing_file(self, query):
        h_code = self.input.param('handler_code', 'n1ql_with_exec')
        if h_code == "n1ql_with_exec":
            handler_code = HANDLER_CODE.N1QL_TEMP
        else:
            handler_code = HANDLER_CODE.N1QL_TEMP_WITHOUT_EXEC
        try:
            if not os.path.exists(HANDLER_CODE.N1QL_TEMP_PATH):
                os.makedirs(HANDLER_CODE.N1QL_TEMP_PATH)
        except OSError as err:
            print(err)
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, handler_code)
        fh = open(abs_file_path, "r")
        code = Template(fh.read()).substitute(n1ql=query)
        fh.close()
        ts = datetime.datetime.now().strftime('%m%d%y%H%M%S%f')
        temp_file_path = HANDLER_CODE.N1QL_TEMP_PATH + "f_" + ts + ".js"
        abs_file_path = os.path.join(script_dir, temp_file_path)
        fw = open(abs_file_path, "w+")
        fw.write(code)
        fw.close()
        return temp_file_path


    def unzip_template(self, template_path):
        if "zip" not in template_path:
            return template_path
        tokens = template_path.split("/")
        file_name = tokens[len(tokens) - 1]
        output_path = template_path.replace(file_name, "")
        with zipfile.ZipFile(template_path, "r") as z:
            z.extractall(output_path)
        template_path = template_path.replace(".zip", "")
        return template_path


    def delete_temp_handler_code(self, path=HANDLER_CODE.N1QL_TEMP_PATH):
        log.info("deleting all the handler codes")
        script_dir = os.path.dirname(__file__)
        dirPath = os.path.join(script_dir, path)
        fileList = os.listdir(dirPath)
        for fileName in fileList:
            os.remove(dirPath + "/" + fileName)


    def verify_n1ql_stats(self, total_query):
        n1ql_query = "select failed_query.query from dst_bucket where failed_query is not null"
        failed = self.n1ql_helper.run_cbq_query(query=n1ql_query, server=self.n1ql_node)
        n1ql_query = "select passed_query.query from dst_bucket where passed_query is not null"
        passed = self.n1ql_helper.run_cbq_query(query=n1ql_query, server=self.n1ql_node)
        log.info("passed: {}".format(len(passed["results"])))
        log.info("failed: {}".format(len(failed["results"])))
        assert len(passed["results"]) + len(failed["results"]) == total_query
        assert len(failed["results"]) == 0, "failed queries are {0}".format(failed["results"])
