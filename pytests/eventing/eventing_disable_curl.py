import json
import logging

from pytests.eventing.eventing_base import EventingBaseTest
from lib.membase.api.rest_client import RestConnection
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL

log = logging.getLogger()


class EventingDisableCurl(EventingBaseTest):
    def setUp(self):
        super(EventingDisableCurl, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.handler_code = self.input.param('handler_code', 'bucket_op_curl')
        self.curl_username = self.input.param('curl_user', None)
        self.curl_password = self.input.param('curl_password', None)
        self.auth_type = self.input.param('auth_type', 'no-auth')
        self.url = self.input.param('path', None)
        if self.handler_code == 'bucket_op_curl':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL
        elif self.handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_ON_UPDATE
        self._create_test_collections()
        self.bucket_op_func_names = []
        self.curl_func_names = []

    def tearDown(self):
        super(EventingDisableCurl, self).tearDown()

    def _create_test_collections(self):
        """
          coll0, coll1  — source/destination collections for bucket-op functions
          coll2, coll3  — source/destination collections for curl functions
        """
        for bucket in [self.src_bucket_name, self.dst_bucket_name]:
            try:
                self.collection_rest.create_scope(bucket, "scope0")
            except Exception:
                pass  # scope already exists from previous tests
            for coll in ["coll0", "coll1", "coll2", "coll3"]:
                try:
                    self.collection_rest.create_collection(bucket, "scope0", coll)
                except Exception:
                    pass  # collection already exists from previous tests

    def _list_name(self, name):
        """
        Return the function name as it appears in the list API response
        """
        scope = self.function_scope
        if scope.get("bucket") == "*":
            return name
        return "{0}/{1}/{2}".format(scope["bucket"], scope["scope"], name)

    def create_bucket_op_functions(self):
        saved_is_curl = self.is_curl
        self.is_curl = False
        meta_ns = "{0}._default._default".format(self.metadata_bucket_name)
        for i in range(2):
            name = "{0}_bucket_op_{1}".format(self.function_name, i)[:90]
            src_ns = "{0}.scope0.coll{1}".format(self.src_bucket_name, i)
            dst_binding = "dst_bucket.{0}.scope0.coll{1}.rw".format(self.dst_bucket_name, i)
            self.create_function_with_collection(
                name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE,
                src_namespace=src_ns, meta_namespace=meta_ns,
                collection_bindings=[dst_binding], is_curl=False)
            self.bucket_op_func_names.append(name)
        self.is_curl = saved_is_curl

    def create_curl_functions(self):
        saved_is_curl = self.is_curl
        meta_ns = "{0}._default._default".format(self.metadata_bucket_name)
        for idx, coll in enumerate(["coll2", "coll3"]):
            name = "{0}_curl_{1}".format(self.function_name, idx)[:90]
            src_ns = "{0}.scope0.{1}".format(self.src_bucket_name, coll)
            dst_binding = "dst_bucket.{0}.scope0.{1}.rw".format(self.dst_bucket_name, coll)
            self.create_function_with_collection(
                name, self.handler_code,
                src_namespace=src_ns, meta_namespace=meta_ns,
                collection_bindings=[dst_binding], is_curl=True)
            self.curl_func_names.append(name)
        self.is_curl = saved_is_curl

    def test_list_functions_curl_binding(self):
        """
        api/v1/list/functions?curl_binding=<bool>
          curl_binding=true  → only the 2 curl functions
          curl_binding=false → only the 2 bucket-op functions
          no filter          → all 4
        """
        self.create_bucket_op_functions()
        self.create_curl_functions()

        all_functions = self.get_list_eventing_functions()
        self.assertEqual(len(all_functions), 4,
                         "Expected 4 total functions, got: {}".format(all_functions))

        expected_curl = sorted(self._list_name(n) for n in self.curl_func_names)
        actual_curl = sorted(self.get_list_eventing_functions(curl_binding=True))
        self.assertEqual(actual_curl, expected_curl,
                         "curl_binding=true mismatch. Got: {}".format(actual_curl))

        expected_bo = sorted(self._list_name(n) for n in self.bucket_op_func_names)
        actual_bo = sorted(self.get_list_eventing_functions(curl_binding=False))
        self.assertEqual(actual_bo, expected_bo,
                         "curl_binding=false mismatch. Got: {}".format(actual_bo))
        self.undeploy_delete_all_functions()

    def test_list_deployed_functions_with_curl_binding(self):
        """
        api/v1/list/functions?deployed=true&curl_binding=<bool>
        Should return intersection of curl and deployed functions
        """
        self.create_bucket_op_functions()
        self.create_curl_functions()

        self.deploy_handler_by_name(self.bucket_op_func_names[0])
        self.deploy_handler_by_name(self.curl_func_names[0])

        deployed_curl = self.get_list_eventing_functions(curl_binding=True, deployed=True)
        self.assertEqual(deployed_curl, [self._list_name(self.curl_func_names[0])],
                         "deployed+curl_binding=true mismatch. Got: {}".format(deployed_curl))

        deployed_bo = self.get_list_eventing_functions(curl_binding=False, deployed=True)
        self.assertEqual(deployed_bo, [self._list_name(self.bucket_op_func_names[0])],
                         "deployed+curl_binding=false mismatch. Got: {}".format(deployed_bo))
        self.undeploy_delete_all_functions()

    def test_disable_curl_binding_negative(self):
        """
        disable_curl_binding=true should not be allowed when deployed functions have curl bindings
        """
        self.create_curl_functions()
        self.deploy_handler_by_name(self.curl_func_names[0])

        status, response = self.set_curl_binding_config(True)
        self.assertFalse(status,
                         "Expected failure when a curl function is deployed. Got: {}".format(response))
        self.assertEqual(response.get("name"), "ERR_INVALID_REQUEST",
                         "Expected ERR_INVALID_REQUEST, got: {}".format(response))
        self.assertEqual(response.get("code"), 63,
                         "Expected error code 63, got: {}".format(response))
        self.undeploy_delete_all_functions()

    def test_disable_curl_binding_positive(self):
        """
        Test disable_curl_binding=true:
        Must succeed when no deployed function has a curl binding
        After disabling, deploying a curl function must be rejected with the following:
        ERR_INVALID_REQUEST ("curl binding not allowed")
        """
        self.create_bucket_op_functions()
        self.create_curl_functions()
        self.deploy_handler_by_name(self.bucket_op_func_names[0])

        status, response = self.set_curl_binding_config(True)
        self.assertTrue(status,
                        "Expected disable_curl_binding to succeed. Got: {}".format(response))
        self.assertEqual(response.get("code"), 0,
                         "Expected success code 0, got: {}".format(response))

        try:
            self.deploy_handler_by_name(self.curl_func_names[0])
            self.fail("Expected deploy of curl function to be rejected when curl binding is disabled")
        except Exception as e:
            raw = e.args[0]
            err = json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
            self.assertEqual(err.get("name"), "ERR_INVALID_REQUEST",
                             "Expected ERR_INVALID_REQUEST on deploy, got: {}".format(err))
            self.assertEqual(err.get("description"), "curl binding not allowed",
                             "Expected 'curl binding not allowed', got: {}".format(err))
            self.log.info("Deploy correctly rejected: {}".format(err))
        self.undeploy_delete_all_functions()