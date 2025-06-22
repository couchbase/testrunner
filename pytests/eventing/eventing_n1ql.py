import copy
import datetime

from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_ERROR
from pytests.eventing.eventing_base import EventingBaseTest, log
from lib.couchbase_helper.tuq_helper import N1QLHelper
from pytests.security.rbacmain import rbacmain
from lib.remote.remote_util import RemoteMachineShellConnection
import json

class EventingN1QL(EventingBaseTest):
    def setUp(self):
        super(EventingN1QL, self).setUp()
        self.buckets = self.rest.get_buckets()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.handler_code=self.input.param('handler_code', 'bucket_op')

    def tearDown(self):
        super(EventingN1QL, self).tearDown()

    def test_delete_from_n1ql_from_update(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_DELETE_UPDATE, worker_count=3)
        try:
            self.deploy_function(body)
        except Exception as ex:
            if "ERR_INTER_BUCKET_RECURSION" not in str(ex):
                self.fail("recursive mutations are allowed through n1ql")

    def test_n1ql_prepare_statement(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        query = "PREPARE test from DELETE from " + self.src_bucket_name + " where mutated=0"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_PREPARE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        query = "drop primary index on " + self.src_bucket_name
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)


    def test_n1ql_DML(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_DML, dcp_stream_boundary="from_now",
                                              execution_timeout=15)
        self.deploy_function(body)
        query = "UPDATE "+self.src_bucket_name+" set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.verify_eventing_results(self.function_name, 6, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_n1ql_DDL(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_DDL, dcp_stream_boundary="from_now",
                                              execution_timeout=15)
        self.deploy_function(body)
        #create a mutation via N1QL
        query = "UPDATE "+self.src_bucket_name+" set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        #verify deployment should fail
        self.verify_eventing_results(self.function_name, 3, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_recursive_mutation_n1ql(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.RECURSIVE_MUTATION,
                                              dcp_stream_boundary="from_now", execution_timeout=15)
        self.deploy_function(body)
        # create a mutation via N1QL
        query = "UPDATE " + self.src_bucket_name + " set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # verify deployment should fail
        self.verify_eventing_results(self.function_name, 0)
        self.undeploy_and_delete_function(body)

    def test_grant_revoke(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.GRANT_REVOKE, dcp_stream_boundary="from_now",
                                              execution_timeout=15)
        self.deploy_function(body)
        #create a mutation via N1QL
        query = "UPDATE "+self.src_bucket_name+" set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        #verify deployment should fail
        self.verify_eventing_results(self.function_name, 2, skip_stats_validation=True)
        rbacmain(self.master)._check_user_permission(self.master.rest_username, self.master.rest_password, "admin")
        self.undeploy_and_delete_function(body)

    def test_n1ql_curl(self):
        self.rest.create_whitelist(self.master, {"all_access": True})
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.CURL, dcp_stream_boundary="from_now",
                                              execution_timeout=15)
        self.deploy_function(body)
        # create a mutation via N1QL
        query = "UPDATE "+self.src_bucket_name+" set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.verify_eventing_results(self.function_name, 1, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_anonymous(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.ANONYMOUS, dcp_stream_boundary="from_now"
                                              )
        self.deploy_function(body)
        #create a mutation via N1QL
        query = "UPDATE "+self.src_bucket_name+" set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        #verify that n1ql query will fail
        self.verify_eventing_results(self.function_name, 2, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_recursion_function(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.RECURSION_FUNCTION,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        # create a mutation via N1QL
        query = "UPDATE " + self.src_bucket_name + " set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # verify that n1ql query will fail
        self.verify_eventing_results(self.function_name, 2, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_global_variable(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        try:
            body = self.create_save_function_body(self.function_name, HANDLER_CODE_ERROR.GLOBAL_VARIABLE,
                                              dcp_stream_boundary="from_now")
        except Exception as e:
            if "ERR_HANDLER_COMPILATION" not in str(e):
                self.fail("Deployment is expected to be failed but no message of failure")

    def test_empty_handler(self):
        try:
            body = self.create_save_function_body(self.function_name, HANDLER_CODE_ERROR.EMPTY,
                                              dcp_stream_boundary="from_now")
            self.deploy_function(body, deployment_fail=True)
        except Exception as e:
            if "ERR_HANDLER_COMPILATION" not in str(e):
                self.fail("Function deployment succeeded with empty handler")

    def test_without_update_delete(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        try:
            body = self.create_save_function_body(self.function_name, HANDLER_CODE_ERROR.RANDOM,
                                              dcp_stream_boundary="from_now")
            self.deploy_function(body, deployment_fail=True)
        except Exception as e:
            self.log.info(e)
            assert "ERR_HANDLER_COMPILATION" in str(e) and "handler must have at least OnUpdate() or OnDelete() function" in str(e), True


    def test_anonymous_with_cron_timer(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ERROR.ANONYMOUS_CRON_TIMER,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body, deployment_fail=True)
        # TODO : more assertion needs to be validate after MB-27155

    def test_anonymous_with_doc_timer(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ERROR.ANONYMOUS_DOC_TIMER,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body, deployment_fail=True)
        # TODO : more assertion needs to be validate after MB-27155

    def test_n1ql_iterator(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_ITERATOR, dcp_stream_boundary="from_now", execution_timeout=15)
        self.deploy_function(body)
        query = "UPDATE "+self.src_bucket_name+" set mutated=1 where mutated=0 limit 1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    # This was moved from base class to here because http://ci-eventing.northscale.in/ was failing as it could not find
    # from pytests.security.rbacmain import rbacmain
    def verify_user_noroles(self, username):
        status, content, header=rbacmain(self.master)._retrieve_user_roles()
        res = json.loads(content)
        userExist=False
        for ele in res:
            log.debug("user {0}".format(ele["name"]))
            log.debug(ele["name"] == username)
            if ele["name"] == username:
                log.debug("user roles {0}".format(ele["roles"]))
                if not ele["roles"]:
                    log.info("user {0} has no roles".format(username))
                    userExist=True
                    break
        if not userExist:
            raise Exception("user {0} roles are not empty".format(username))

    def test_n1ql_iterators_with_break_and_continue(self):
        values = ['1', '10']
        # create 100 non json docs
        # number of docs is intentionally reduced as handler code runs 1 n1ql queries/mutation
        gen_load_non_json = JSONNonDocGenerator('non_json_docs', values, start=0, end=100)
        gen_load_non_json_del = copy.deepcopy(gen_load_non_json)
        # load the data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   'create', compression=self.sdk_compression)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_ITERATORS, execution_timeout=60)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, 100)
        # delete all the docs
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json_del, self.buckets[0].kvs[1],
                                   'delete', compression=self.sdk_compression)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)
        # delete all the primary indexes
        self.n1ql_helper.drop_primary_index(using_gsi=True, server=self.n1ql_server)

    def test_n1ql_with_multiple_queries(self):
        self.load_sample_buckets(self.server,"travel-sample")
        body = self.create_save_function_body(self.function_name,"handler_code/n1ql_op_with_multiple_queries.js",
                                              dcp_stream_boundary="from_now", execution_timeout=15)
        self.deploy_function(body)
        key = datetime.datetime.now().time()
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"" + str(key) + "\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 9, skip_stats_validation=True)
        query = "delete from src_bucket where META().id='"+str(key)+"'"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_n1ql_with_iterator_break_continue(self):
        self.load_sample_buckets(self.server,"travel-sample")
        body = self.create_save_function_body(self.function_name,"handler_code/n1ql_op_iterator.js",
                                              dcp_stream_boundary="from_now", execution_timeout=15)
        self.deploy_function(body)
        key = datetime.datetime.now().time()
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"" + str(key) + "\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 917, skip_stats_validation=True)
        query = "delete from src_bucket where META().id='"+str(key)+"'"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


    def test_n1ql_close_before_complete(self):
        self.load_sample_buckets(self.server,"travel-sample")
        body = self.create_save_function_body(self.function_name,"handler_code/n1ql_op_close.js",
                                              dcp_stream_boundary="from_now", execution_timeout=15)
        self.deploy_function(body)
        key = datetime.datetime.now().time()
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"" + str(key) + "\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 10, skip_stats_validation=True)
        query = "delete from src_bucket where META().id='"+str(key)+"'"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_n1ql_variable_substitution(self):
        self.load_sample_buckets(self.server,"travel-sample")
        body = self.create_save_function_body(self.function_name,"handler_code/n1ql_op_with_variable_substitution.js",
                                              dcp_stream_boundary="from_now", execution_timeout=15)
        self.deploy_function(body)
        key = datetime.datetime.now().time()
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"" + str(key) + "\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 9, skip_stats_validation=True)
        query = "delete from src_bucket where META().id='"+str(key)+"'"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_n1ql_timeout(self):
        self.load_sample_buckets(self.server,"travel-sample")
        body = self.create_save_function_body(self.function_name,"handler_code/n1ql_op_timeout.js",
                                              dcp_stream_boundary="from_now", execution_timeout=10)
        self.deploy_function(body)
        key = datetime.datetime.now().time()
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"" + str(key) + "\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.sleep(30)
        stats = self.rest.get_all_eventing_stats()
        log.info("Stats {0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        if stats[0]["failure_stats"]["timeout_count"] !=1:
            if stats[0]["lcb_exception_stats"]["201"]==1:
                pass
            else:
                raise Exception("Timeout not happened for the long running query")
        elif stats[0]["failure_stats"]["timeout_count"] ==1:
            pass
        self.undeploy_and_delete_function(body)


    def test_n1ql_slow_queries(self):
        self.load_sample_buckets(self.server, "travel-sample")
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,"handler_code/n1ql_op_slow_handler.js",
                                              dcp_stream_boundary="everything", execution_timeout=60)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, 2016, skip_stats_validation=True)
        gen_load_del = copy.deepcopy(self.gens_load)
        # delete json documents
        self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_n1ql_with_set_expiry(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        self.n1ql_helper.drop_primary_index(using_gsi=True, server=self.n1ql_server)
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_server)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, "handler_code/n1ql_op_to_set_expiry.js",
                                              dcp_stream_boundary="everything", execution_timeout=60)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, 2016, skip_stats_validation=True)
        body1 = self.create_save_function_body(self.function_name+"_check", "handler_code/check_for_expiry.js",
                                              dcp_stream_boundary="everything", execution_timeout=60,multi_dst_bucket=True)
        self.deploy_function(body1)
        self.verify_eventing_results(self.function_name, 2016,bucket=self.dst_bucket_name1, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.undeploy_and_delete_function(body1)

    def test_n1ql_with_ttl(self):
        dst_bucket = self.rest.get_bucket(self.dst_bucket_name)
        self.shell.execute_cbepctl(dst_bucket, "", "set flush_param", "exp_pager_stime", 5)
        query = "SELECT COUNT(*) from dst_bucket"
        self.load(self.gens_load, buckets=[self.src_bucket[0], dst_bucket], flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)

        # Deploying eventing function
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_TTL_UPDATE, worker_count=3)
        try:
            self.deploy_function(body)
        except Exception as ex:
            if "Can not execute DML query on bucket" not in str(ex):
                self.fail("recursive mutations are allowed through n1ql")
        count = 0
        while count < 20:
            count += 1
            result = self.n1ql_helper.run_cbq_query(query, server=self.n1ql_server)['results'][0]['$1']
            if result == 0:
                self.log.info("Eventing is able to set expiration values in dst_bucket")
                break
            self.sleep(timeout=2, message="Waiting for docs to get expired")
        self.assertNotEqual(count, 20, "All docs didn't expired in dst_bucket. Check eventing logs for details.")
        self.undeploy_and_delete_function(body)

    #MB-42513
    def test_delete_query(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_DELETE_QUERY_TEST, worker_count=3)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_n1ql_gc_rebalance(self):
        self.load_sample_buckets(self.server, "travel-sample")
        worker_count = self.input.param('worker_count', 12)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=worker_count)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    # MB-59344
    def test_n1ql_queries_after_internal_password_rotation(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.rest.refresh_credentials()
        self.load_data_to_collection(self.docs_per_day * self.num_docs * 2, "src_bucket._default._default")
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs * 2)
        self.undeploy_and_delete_function(body)
