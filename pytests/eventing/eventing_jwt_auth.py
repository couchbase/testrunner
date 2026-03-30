from pytests.eventing.eventing_constants import HANDLER_CODE
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
        self.jit_provisioning = self.input.param('jit_provisioning', True)
        self.jwt_ttl = self.input.param('jwt_ttl', 3600)

        # Initialize JWT utilities
        self.jwt_utils = JWTUtils(log=self.log)

        # Handler Codes
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert.js"

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
            description="JWT Eventing Admin Group",
            roles="admin"
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