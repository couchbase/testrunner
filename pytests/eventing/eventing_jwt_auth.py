from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_ANALYTICS, HANDLER_CODE_ONDEPLOY
from pytests.security.jwt_utils import JWTUtils
from lib.membase.api.rest_client import RestConnection
from security.rbac_base import RbacBase
from pytests.eventing.eventing_base import EventingBaseTest

import logging
import json
import base64
import urllib.parse

log = logging.getLogger()


class EventingJWTAuth(EventingBaseTest):
    def setUp(self):
        super(EventingJWTAuth, self).setUp()

        # JWT Configuration
        self.jwt_algorithm = self.input.param('jwt_algorithm', 'ES256')
        self.jwt_issuer = self.input.param('jwt_issuer', 'custom-issuer')
        self.jwt_audience = self.input.param('jwt_audience', 'cb-cluster')
        self.jwt_user = self.input.param('jwt_user', 'jwt_user')
        self.jwt_group = self.input.param('jwt_group', 'admin')
        self.jwt_roles = self.input.param('jwt_roles', 'admin')
        self.jit_provisioning = self.input.param('jit_provisioning', True)
        self.jwt_ttl = self.input.param('jwt_ttl', 3600)
        self.logsize = self.input.param("size", None)
        self.aggregate= self.input.param("aggregate", False)


        # Initialize JWT utilities
        self.jwt_utils = JWTUtils(log=self.log)

        # Handler Codes
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert.js"
        elif handler_code == 'heavy_logger':
            self.handler_code = 'handler_code/heavy_logger.js'
        elif handler_code == "subdoc":
            self.handler_code = "handler_code/ABO/subdoc_array_and_field_op.js"
        elif handler_code == "bucket_cache":
            self.handler_code = "handler_code/ABO/cache_compare_values.js"
        elif handler_code == "xattrs":
            self.handler_code = "handler_code/xattrs_handler1.js"
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == "sbm":
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_SBM
        elif handler_code == "timers":
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_TIMERS
        elif handler_code == "curl":
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_CURL
        elif handler_code == "base64":
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_BASE64
        elif handler_code == "n1ql":
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_N1QL
        elif handler_code == "crc":
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_CRC
        elif handler_code == "analytics_and_counters":
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
        elif handler_code == "ondeploy":
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP

    def tearDown(self):
        super(EventingJWTAuth, self).tearDown()

    def setup_jwt_config(self):
        """
        Setup JWT configuration on the cluster
        """
        log.info("Setting up JWT configuration")

        # Generate key pair
        log.info(f"Generating {self.jwt_algorithm} key pair")
        private_key, public_key = self.jwt_utils.generate_key_pair(self.jwt_algorithm, key_size=2048)
        self.private_key = private_key
        self.public_key = public_key

        # Create group with eventing permissions
        log.info(f"Creating group {self.jwt_group} with eventing permissions")
        self.create_jwt_group()

        # Create external user
        log.info(f"Creating external user {self.jwt_user}")
        self.create_jwt_user()

        # Configure JWT on cluster
        log.info("Configuring JWT on cluster")
        self.configure_jwt_on_cluster(public_key)

        # Generate JWT token
        log.info("Generating JWT token")
        self.jwt_token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer,
            user_name=self.jwt_user,
            algorithm=self.jwt_algorithm,
            private_key=private_key,
            token_audience=[self.jwt_audience],
            user_groups=[self.jwt_group],
            ttl=self.jwt_ttl
        )
        log.info("JWT token generated successfully")

        return self.jwt_token

    def create_jwt_group(self):
        """
        Create a group with eventing permissions (Admin/Eventing Manage Scope) using existing REST method
        """
        # Use existing add_group_role method from on_prem_rest_client
        status, content = self.rest.add_group_role(
            group_name=self.jwt_group,
            description="Group for testing JWT permissions",
            roles=self.jwt_roles
        )
        if not status:
            raise Exception(f"Failed to create group {self.jwt_group}: {content}")
        log.info(f"Group {self.jwt_group} created successfully")

    def create_jwt_user(self):
        """
        Create external user and assign to group using existing REST method
        """
        # Use existing add_external_user method from on_prem_rest_client
        payload = urllib.parse.urlencode({
            "name": self.jwt_user,
            "groups": self.jwt_group
        })

        _ = self.rest.add_external_user(self.jwt_user, payload)
        log.info(f"External user {self.jwt_user} created successfully")

    def configure_jwt_on_cluster(self, public_key):
        """
        Configure JWT authentication on cluster using existing REST method
        """
        jwt_config = self.jwt_utils.get_jwt_config(
            issuer_name=self.jwt_issuer,
            algorithm=self.jwt_algorithm,
            pub_key=public_key,
            token_audience=[self.jwt_audience],
            token_group_matching_rule=[f"^.{self.jwt_user}$ {self.jwt_group}"],
            jit_provisioning=self.jit_provisioning
        )
        status, content, _ = self.rest.create_jwt_with_config(jwt_config)
        if not status:
            raise Exception(f"Failed to configure JWT: {content}")
        log.info("JWT configured successfully")

    def test_eventing_jwt_auth_sanity(self):
        '''
        Create and deploy eventing function with JWT authentication
        Load Docs
        Verify that mutations are processed
        Pause function with JWT authentication
        Resume function with JWT authentication
        Delete docs and verify delete mutations are processed
        Undeploy and delete function with JWT authentication
        '''
        # Setup JWT configuration
        self.setup_jwt_config()

        # Create function body
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)

        # Deploy function using JWT authentication (uses base class method with jwt_token parameter)
        log.info("Deploying eventing function with JWT authentication")
        self.deploy_function(body, jwt_token=self.jwt_token)

        # Load data
        log.info("Loading data to source bucket")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")

        # Verify mutations are processed
        log.info("Verifying mutations are processed")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)

        # Pause function using JWT authentication
        log.info("Pausing eventing function with JWT authentication")
        self.pause_function(body, jwt_token=self.jwt_token)

        # Resume function using JWT authentication
        log.info("Resuming eventing function with JWT authentication")
        self.resume_function(body, jwt_token=self.jwt_token)

        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.sleep(10)  # Wait for deletions to be processed
        self.verify_doc_count_collections("default.scope0.collection1", 0)

        # Undeploy and delete function using JWT authentication
        log.info("Undeploying eventing function with JWT authentication")
        self.undeploy_function(body, jwt_token=self.jwt_token)

        log.info("Deleting eventing function with JWT authentication")
        self.delete_function(body, jwt_token=self.jwt_token)


    def test_eventing_jwt_auth_export_import(self):
        '''
        Test export and import functionality with JWT authentication
        Create and deploy function
        Export function with JWT authentication
        Delete function
        Import function with JWT authentication
        Verify function is restored
        '''
        # Setup JWT configuration
        self.setup_jwt_config()

        # Create function body
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token )

        # Export function with JWT authentication
        log.info("Exporting eventing function with JWT authentication")
        exported_function = self.export_eventing_function(self.function_name, jwt_token=self.jwt_token)
        log.info(f"Exported function: {exported_function}")

        # Verify export contains expected fields
        if 'appname' not in exported_function:
            raise Exception("Exported function missing appname field")

        # Undeploy and delete original function
        log.info("Undeploying and deleting original function")
        self.delete_function(body, jwt_token=self.jwt_token)

        # Import function with JWT authentication
        log.info("Importing eventing function with JWT authentication")
        import_body = json.dumps([exported_function])
        self.import_eventing_function(import_body, jwt_token=self.jwt_token)

        # Verify function is restored and operational
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body, jwt_token=self.jwt_token)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_function(body, jwt_token=self.jwt_token)

        log.info("Export/Import test completed successfully")
        self.delete_function(body, jwt_token=self.jwt_token)

    def test_eventing_jwt_auth_unauthorized(self):
        '''
        Test that invalid JWT token is rejected
        Setup JWT configuration
        Attempt to deploy function with invalid JWT token
        Verify operation fails with authorization error
        '''
        # Setup JWT configuration
        self.setup_jwt_config()

        # Create function body
        body = self.create_save_function_body(self.function_name, self.handler_code)

        # Attempt to deploy with invalid token
        log.info("Attempting to deploy with invalid JWT token")
        invalid_token = "invalid.jwt.token"

        try:
            self.deploy_function(body, jwt_token=invalid_token)
            raise Exception("Expected deployment to fail with invalid JWT token, but it succeeded")
        except Exception as e:
            log.info(f"Deployment correctly failed with invalid token: {e}")
            assert "ERR_UNAUTHENTICATED" in str(e) and "unauthenticated User" in str(e), True
        log.info("Unauthorized test completed successfully")


    def test_expired_jwt_token(self):
        '''
        Test that expired JWT token is rejected
        Setup JWT configuration with short TTL
        Generate JWT token
        Attempt to deploy function with expired JWT token
        Verify operation fails with authorization error
        '''
        # Setup JWT configuration with short TTL
        self.jwt_ttl = 30  # 30 seconds TTL for quick expiration
        self.setup_jwt_config()

        # Create function body
        body = self.create_save_function_body(self.function_name, self.handler_code)

        # Wait for token to expire
        log.info("Waiting for JWT token to expire")
        self.sleep(60)

        # Attempt to deploy with expired token
        log.info("Attempting to deploy with expired JWT token")
        try:
            self.deploy_function(body, jwt_token=self.jwt_token)
            raise Exception("Expected deployment to fail with expired JWT token, but it succeeded")
        except Exception as e:
            log.info(f"Deployment correctly failed with expired token: {e}")
            assert "ERR_UNAUTHENTICATED" in str(e) and "unauthenticated User" in str(e), True
        log.info("Expired token test completed successfully")


    def test_diff_deployment_configurations_with_jwt(self):
        '''
        Test updating function configuration using JWT authentication
        '''
        self.setup_jwt_config()

        # Create and deploy function with JWT
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")

        # Delete initial depcfg and create a new one
        del body['depcfg']['buckets']
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append(
            {"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name, "scope_name": "_default",
             "collection_name": "_default", "access": "rw"})

        # Update and get the configuration to verify changes are applied
        self.update_eventing_config_per_function(body['depcfg'], self.function_name, jwt_token=self.jwt_token)
        log.info(self.get_eventing_config_per_function(self.function_name, jwt_token=self.jwt_token))

        # Deloy and verify if mutations are processed
        self.deploy_function(body, jwt_token=self.jwt_token)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)

        # Cleanup
        self.undeploy_function(body, jwt_token=self.jwt_token)
        self.delete_function(body, jwt_token=self.jwt_token)


    def test_editing_function_settings_with_jwt(self):
        '''
        Test changing eventing function settings using JWT authentication
        '''
        # Setup JWT configuration
        self.setup_jwt_config()

        # Create and deploy function with JWT
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)

        # Load data to source bucket
        log.info("Loading data to source bucket")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")

        # Set deployment and processing status to deploy the function and change other settings as well
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        body['settings']['log_level'] = 'DEBUG'
        body['settings']['worker_count'] = 4
        if 'num_timer_partitions' not in body['settings']:
            body['settings']['num_timer_partitions'] = 2

        self.set_function_settings(body['appname'], body['settings'], jwt_token=self.jwt_token)

        # Verify mutations are processed
        log.info("Verifying mutations are processed after settings changes")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)

        # Undeploy and delete function with JWT
        log.info("Undeploying and deleting function with JWT")
        self.undeploy_function(body, jwt_token=self.jwt_token)
        self.delete_function(body, jwt_token=self.jwt_token)


    def test_eventing_applogs_with_jwt_auth(self):
        '''
        Test /getAppLog endpoint with JWT authentication
        '''
        # Setup JWT configuration
        self.setup_jwt_config()

        # Necessary Initial Config
        self.load_sample_buckets(self.server, "travel-sample")
        self.src_bucket_name="travel-sample"
        self.function_scope = {"bucket": "*", "scope": "*"}
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['depcfg']['source_scope'] = "_default"
        body['depcfg']['source_collection'] = "_default"

        # Create, Delpoy the function and verify if mutations are processed
        self.rest.create_function_with_jwt(body['appname'], body, self.jwt_token, function_scope=self.function_scope)
        self.deploy_function(body, jwt_token=self.jwt_token)
        self.verify_doc_count_collections("dst_bucket._default._default", 31591)

        # Get AppLogs using the /getAppLog endpoint using JWT auth
        applogs = self.get_app_logs(self.function_name, size=self.logsize, aggregate=self.aggregate, jwt_token=self.jwt_token)
        if len(applogs) == 0:
            raise Exception("No app logs found")
        else:
            log.info("App logs found, count: {}".format(len(applogs)))
        self.undeploy_function(body, jwt_token=self.jwt_token)
        self.delete_function(body, jwt_token=self.jwt_token)


    def test_coesistence_of_basic_and_jwt_auth(self):
        '''
        Perform few Lifecycle Operations with JWT and others with Basic Auth
        '''
        # Setup JWT configuration
        self.setup_jwt_config()
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)

        # Deploy function using JWT authentication (uses base class method with jwt_token parameter)
        log.info("Deploying eventing function with JWT authentication")
        self.deploy_function(body, jwt_token=self.jwt_token)

        # Load data
        log.info("Loading data to source bucket")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")

        # Verify mutations are processed
        log.info("Verifying mutations are processed")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)

        # Pause function using Basic authentication
        log.info("Pausing eventing function with Basic authentication")
        self.pause_function(body)

        # Resume function using Basic authentication
        log.info("Resuming eventing function with Basic authentication")
        self.resume_function(body)

        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.sleep(10)  # Wait for deletions to be processed
        self.verify_doc_count_collections("default.scope0.collection1", 0)

        # Undeploy and delete function using JWT authentication
        log.info("Undeploying eventing function with Basic authentication")
        self.undeploy_function(body)

        log.info("Deleting eventing function with JWT authentication")
        self.delete_function(body, jwt_token=self.jwt_token)


    def test_eventing_jwt_dropping_function_scope_when_handler_is_deployed(self):
        '''
        Internal Undeployment Scenario:
        Dropping the function scope and checking if the function is getting undeployed
        '''
        # Setup JWT configuration
        self.setup_jwt_config()
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)
        self.deploy_function(body, jwt_token=self.jwt_token)
        # Drop function scope
        self.rest.delete_bucket(self.src_bucket_name)
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body, jwt_token=self.jwt_token)


    def test_eventing_jwt_dropping_metadata_keyspace_when_handler_is_deployed(self):
        '''
        Internal Undeployment Scenario:
        Dropping the metadata keyspace and checking if the function is getting undeployed
        '''
        # Setup JWT configuration
        self.setup_jwt_config()
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)
        self.deploy_function(body, jwt_token=self.jwt_token)
        # Drop metadata keyspace
        self.rest.delete_bucket(self.metadata_bucket_name)
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body, jwt_token=self.jwt_token)

    def test_jwt_permitted_users_and_user_groups(self):
        '''
        Only Admin and Eventing Admin should be permitted to perform Lifecyle Ops
        Eventing Manage Functions should be allowed to perform lifecycle operations only on the specified scope
        Other combined users (not the above 3 roles) should not be permitted to perform lifecycle operations
        '''
        # Setup JWT configuration
        self.setup_jwt_config()
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token)

        if self.jwt_roles in ["admin", "eventing_admin"]:
            log.info(f"Testing with role {self.jwt_roles} which should have permissions to create and deploy functions")
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
            self.deploy_function(body, jwt_token=self.jwt_token)
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
            self.pause_function(body, jwt_token=self.jwt_token)
            self.resume_function(body, jwt_token=self.jwt_token)
            self.undeploy_function(body, jwt_token=self.jwt_token)
            self.delete_function(body, jwt_token=self.jwt_token)
            log.info(f"Test passed for role {self.jwt_roles}")

        else:
            log.info(
                f"Testing with role {self.jwt_roles} which should NOT have permissions to create and deploy functions")
            try:
                self.deploy_function(body, jwt_token=self.jwt_token)
                raise Exception(
                    "Expected function deployment to fail for user with insufficient permissions, but it succeeded")
            except Exception as e:
                log.info(f"Deployment correctly failed for user with insufficient permissions: {e}")
                assert "ERR_FORBIDDEN" in str(e) and "Forbidden" in str(e), True
                log.info("Negative test for user with insufficient permissions completed successfully")

    def test_jwt_with_diff_handler_codes(self):

        # Setup JWT configuration
        self.setup_jwt_config()

        #Import Travel Sample
        self.load_sample_buckets(self.server, "travel-sample")

        # Create function body with diff handler codes
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=self.jwt_token, src_binding = True)

        log.info("Deploying eventing function with JWT authentication")
        self.deploy_function(body, jwt_token=self.jwt_token)

        log.info("Loading data to source bucket")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")

        handler_code_name = self.input.param('handler_code', 'bucket_op')

        log.info("Verifying mutations are processed")
        if handler_code_name == "sbm":
            self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs * 2)
        elif handler_code_name == "xattrs":
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs * 2)
        else:
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)

        log.info("Pausing eventing function with JWT authentication")
        self.pause_function(body, jwt_token=self.jwt_token)

        log.info("Resuming eventing function with JWT authentication")
        self.resume_function(body, jwt_token=self.jwt_token)

        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.sleep(10)  # Wait for deletions to be processed

        if handler_code_name != "sbm":
            self.verify_doc_count_collections("default.scope0.collection1", 0)
        else:
            self.verify_doc_count_collections("default.scope0.collection0", 0)

        log.info("Undeploying eventing function with JWT authentication")
        self.undeploy_function(body, jwt_token=self.jwt_token)

        log.info("Deleting eventing function with JWT authentication")
        self.delete_function(body, jwt_token=self.jwt_token)