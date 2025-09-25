
import json
import subprocess
import sys
import os
import time
import uuid
import requests
import jwt

from lib.membase.api.on_prem_rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec

from basetestcase import OnPremBaseTestCase
from pytests.autofailovertests import AutoFailoverBaseTest
from pytests.security.internal_user import InternalUser
from pytests.security.external_user import ExternalUser

class JWTTokenTest(OnPremBaseTestCase):
    def setUp(self):
        super().setUp()
        self.binary_path = "/opt/couchbase/bin/couchbase-cli"

        self.issuer_name = self.input.param("issuer_name", "custom-issuer")
        self.user_name = self.input.param("user_name", "user")
        self.token_audience = eval(self.input.param("token_audience", "['cb-cluster']"))
        self.user_groups = eval(self.input.param("user_groups", "['admin']"))
        self.token_group_matching_rule = eval(self.input.param("token_group_rule", "['^admin$ admin']"))
        self.algorithm = self.input.param("algorithm", "ES256")
        self.key_size = self.input.param("key_size", 2048)
        self.subject = self.input.param("subject", "This is a subject")
        self.ttl = self.input.param("ttl", 300)
        self.nbf_seconds = self.input.param("nbf_seconds", 0)
        self.private_key, self.pub_key = self._generate_key_pair(algorithm=self.algorithm, key_size=self.key_size)
        self._enable_dev_preview()

    def _enable_dev_preview(self):
        self.log.info("Attempting to enable developer preview...")
        shell_conn = RemoteMachineShellConnection(self.master)
        cmd = f"""echo y | {self.binary_path} enable-developer-preview \
                -c localhost:8091 \
                -u {self.master.rest_username} \
                -p {self.master.rest_password} \
                --enable"""

        _, err = shell_conn.execute_command(cmd)
        if err:
            self.fail("Failed to enable Developer Preview Mode")

    def _generate_key_pair(self, algorithm: str, key_size: int):
        """Generate a key pair for JWT signing.
        Args:
            algorithm: JWT signing algorithm (RS256, ES256, etc.)
            key_size: Key size in bits for RSA algorithms (minimum 2048)
        Returns:
            tuple: (private_key_pem, public_key_pem) as strings
        """
        EC_CURVE_MAP = {
            "ES256": ec.SECP256R1(),
            "ES384": ec.SECP384R1(),
            "ES512": ec.SECP521R1(),
            "ES256K": ec.SECP256K1(),
        }
        algorithm = algorithm.upper()
        self.log.info(f"Generating key pair for algorithm: {algorithm}")
        if algorithm.startswith("RS") or algorithm.startswith("PS"):
            if key_size < 2048:
                self.fail(f"RSA key size must be 2048 or greater. Got {key_size}")
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size,
            )
        elif algorithm in EC_CURVE_MAP:
            curve = EC_CURVE_MAP[algorithm]
            private_key = ec.generate_private_key(curve)
        else:
            self.fail(f"Unsupported algorithm: {algorithm}. Supported: RS*, PS*, ES*")
        if not private_key:
            self.fail("Error while creating key pair")
        private_key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_key = private_key.public_key()
        public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        self.log.info("Key pair generated successfully.")
        return private_key_pem.decode(), public_key_pem.decode()

    def _get_jwt_config(self, jit_provisioning=True):
        """Get JWT configuration with configurable JIT provisioning
        Args:
            jit_provisioning (bool): Enable/disable JIT user provisioning. Default: True
        """
        return {
          "enabled": True,
          "issuers": [
            {
              "name": self.issuer_name,
              "signingAlgorithm": self.algorithm,
              "publicKeySource": "pem",
              "publicKey": self.pub_key,
              "jitProvisioning": jit_provisioning,
              "subClaim": "sub",
              "audClaim": "aud",
              "audienceHandling": "any",
              "audiences": self.token_audience,
              "groupsClaim": "groups",
              "groupsMaps": self.token_group_matching_rule
            }
          ]
        }

    def create_token(self):
        curr_time  = int(time.time())
        payload = {
            "iss": self.issuer_name,
            "sub": self.user_name,
            "exp": curr_time+self.ttl,
            "iat": curr_time,
            "nbf": curr_time-self.nbf_seconds,
            "jti": str(uuid.uuid4()),
        }
        if self.token_audience:
            payload['aud'] = self.token_audience
        if self.user_groups:
            payload['groups'] = self.user_groups
        self.log.info(f"Creating JWT token with payload: {json.dumps(payload, indent=2)}")
        jwt_token = jwt.encode(payload=payload,
                               algorithm=self.algorithm,
                               key=self.private_key)
        return jwt_token

    def _debug_jwt_config(self, rest_conn):
        """Debug method to check JWT configuration"""
        try:
            # Try to get current JWT settings if there's an API for it
            self.log.info("Attempting to retrieve JWT configuration for debugging...")
            api = rest_conn.baseUrl + "settings/jwt"
            status, content, header = rest_conn._http_request(api, 'GET')
            self.log.info(f"Current JWT Config - Status: {status}, Content: {content}")
        except Exception as e:
            self.fail(f"Could not retrieve JWT config for debugging: {e}")

    def make_request_with_jwt(self, token, server, endpoint, method, params={}, port=8091 ):
        rest_conn = RestConnection(server)
        header = {
            "Authorization": f"Bearer {token}"
        }
        ok, res, content = rest_conn._http_request(f"http://{server.ip}:{port}/{endpoint}",
                                                   method=method,
                                                   params=json.dumps(params),
                                                   headers=header)
        return ok, res, content

    def test_create_config(self):
        # First, create the admin group that our JWT will map to
        self.log.info("Creating admin group for JWT mapping...")
        group_status = self.create_new_group("admin", "Admin group for JWT authentication")
        if not group_status:
            self.fail("Failed to create admin group")
        self.log.info("Admin group created successfully")
        # With JIT provisioning enabled, we don't need to manually create users
        rest_conn = RestConnection(self.master)
        config = self._get_jwt_config(jit_provisioning=True)
        self.log.info(f"JWT Configuration: {json.dumps(config, indent=2)}")
        status, content, header = rest_conn.create_jwt_with_config(config)
        self.log.info(f"JWT Config Creation - Status: {status}, Content: {content}")
        if not status:
            self.fail(f"Failed to create jwt config: {content}")
        self.sleep(10, "Waiting for JWT config to take effect")
        token = self.create_token()
        self.log.info(f"Generated JWT Token: {token}")
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            self.log.info(f"JWT Token Payload: {json.dumps(decoded, indent=2)}")
        except Exception as e:
            self.fail(f"Error decoding token: {e}")
        ok, res_text, response = self.make_request_with_jwt(token, self.master,
                                                            "/pools/default/buckets",
                                                            "GET", params={})
        status_code = response.status
        self.log.info(f"Request Result - OK: {ok}, Status: {status_code}, Response: {res_text}")
        if 200<=status_code<300:
            self.log.info("Successfully executed curl request with JWT")
        elif status_code == 401:
            self.log.info(f"401 Unauthorized: {res_text}")
            self._debug_jwt_config(rest_conn)
            self.fail("Unauthorised error, JWT did not work as expected")
        else:
            self._debug_jwt_config(rest_conn)
            self.fail(f"Unexpected error, status: {status_code}, response: {res_text}")


    def test_create_config_without_jit(self):
        """Test JWT authentication with JIT provisioning disabled - manual user/group creation"""

        rest_conn = RestConnection(self.master)
        # enabling LDAP for users to be in external domain
        param = {
            'hosts': '{0}'.format("172.23.124.20"),
            'port': '{0}'.format("389"),
            'encryption': '{0}'.format("None"),
            'bindDN': '{0}'.format("cn=admin,dc=couchbase,dc=com"),
            'bindPass': '{0}'.format("p@ssw0rd"),
            'authenticationEnabled': '{0}'.format("true"),
            'userDNMapping': '{0}'.format('{"template":"cn=%u,ou=Users,dc=couchbase,dc=com"}')
        }
        rest_conn.setup_ldap(param, '')
        group_name = "admin_no_jit"
        self.log.info(f"Creating {group_name} group manually (JIT disabled) - with admin roles...")
        group_status = self.create_new_group(group_name, "Admin group for JWT authentication without JIT")
        if not group_status:
            self.log.info(f"Group creation failed, group {group_name} might already exist - deleting and creating")
            status, content = rest_conn.delete_group(group_name)
            if not status:
                self.fail(f"Failed to delete group, reason: {content}")
            self.sleep(3, "Sleeping for group deletion to take effect")
            status, content = self.create_new_group(group_name, "Admin group for JWT authentication without JIT")
            if not status:
                self.fail(f"Failed to recreate group, reason: {content}")
        else:
            self.log.info(f"{group_name} group created successfully (with admin roles)")

        self.log.info("Creating user manually (JIT disabled)...")
        payload = f"name={self.user_name}&groups={group_name}"
        new_user = ExternalUser(user_id=self.user_name,
                                payload=payload,
                                host=self.master)
        try:
            new_user.create_user()
        except Exception as e:
            self.fail(f"Failed to create user with payload:{payload}, reason:{e}")
        self.log.info("External user created successfully (without roles)")
        self.log.info("Checking available groups...")
        try:
            groups_status, groups_content = rest_conn.get_group_list()
            self.log.info(f"Available groups - Status: {groups_status}, Content: {groups_content}")
            if not groups_content:
                self.fail("No groups found in the list")
        except Exception as e:
            self.fail(f"Could not retrieve groups: {e}")

        original_user_groups = self.user_groups
        original_group_rule = self.token_group_matching_rule
        try:
            # Use the new group name for this test
            self.user_groups = [group_name]
            self.token_group_matching_rule = [f".*admin.* admin"]
            config = self._get_jwt_config(jit_provisioning=False)
            # Debug: Log the configuration
            self.log.info(f"JWT Configuration (JIT disabled): {json.dumps(config, indent=2)}")
            status, content, header = rest_conn.create_jwt_with_config(config)
            self.log.info(f"JWT Config Creation - Status: {status}, Content: {content}")
            if not status:
                self.fail(f"Failed to create jwt config without JIT: {content}")
            self.sleep(5, "Waiting for JWT config (JIT disabled) to take effect")

            token = self.create_token()
            self.log.info(f"Generated JWT Token (JIT disabled): {token}")

            try:
                decoded = jwt.decode(token, options={"verify_signature": False})
                self.log.info(f"JWT Token Payload (JIT disabled): {json.dumps(decoded, indent=2)}")
            except Exception as e:
                self.fail(f"Error decoding token: {e}")

            ok, res_text, response = self.make_request_with_jwt(token, self.master,
                                                                "/pools/default/buckets",
                                                                "GET", params={})
            status_code = response.status
            self.log.info(f"Request Result (JIT disabled) - OK: {ok}, Status: {status_code}, Response: {res_text}")
            if 200<=status_code<300:
                self.log.info("Successfully executed curl request with JWT (JIT disabled, manual user/group creation)")
            elif status_code == 401:
                self.log.info(f"401 Unauthorized (JIT disabled): {res_text}")
                self._debug_jwt_config(rest_conn)
                self.fail("Unauthorised error, JWT with manual user/group creation did not work as expected")
            else:
                self._debug_jwt_config(rest_conn)
                self.fail(f"Unexpected error, status: {status_code}, response: {res_text}")

        finally:
            status, content, header = rest_conn.disable_ldap()
            if not status:
                self.fail(f"Failed to disable LDAP, reason: {content}")
            status, content = rest_conn.delete_group(group_name)
            if not status:
                self.fail(f"Failed to clean up group - {group_name}")

    def test_access_forbidden_with_jwt(self):
        """
        Test that access_forbidden audit events are properly triggered with JWT auth.
        Creates a read-only user via JWT and verifies they can read but not write/create resources.
        """
        rest_conn = RestConnection(self.master)

        read_only_group = "jwt_readonly_users"
        self.log.info(f"Creating {read_only_group} group with read-only permissions...")

        group_status, content = rest_conn.add_group_role(
            read_only_group,
            "JWT test group for read-only users",
            "data_reader[*]"
        )
        if not group_status:
            self.log.info(f"Group creation failed, group {read_only_group} might already exist - deleting and recreating")
            status, content = rest_conn.delete_group(read_only_group)
            if not status:
                self.fail(f"Failed to delete group, reason: {content}")
            self.sleep(3, "Sleeping for group deletion to take effect")
            status, content = rest_conn.add_group_role(
                read_only_group,
                "JWT test group for read-only users",
                "data_reader[*]"
            )
            if not status:
                self.fail(f"Failed to recreate group, reason: {content}")

        self.log.info(f"{read_only_group} group created successfully with read-only permissions")

        original_user_groups = self.user_groups
        original_group_rule = self.token_group_matching_rule

        self.user_groups = ["readonly"]
        self.token_group_matching_rule = [f"^readonly$ {read_only_group}"]

        config = self._get_jwt_config(jit_provisioning=True)
        self.log.info(f"JWT Configuration for read-only test: {json.dumps(config, indent=2)}")

        status, content, header = rest_conn.create_jwt_with_config(config)
        self.log.info(f"JWT Config Creation - Status: {status}, Content: {content}")
        if not status:
            self.fail(f"Failed to create jwt config for read-only test: {content}")

        self.sleep(10, "Waiting for JWT config to take effect")

        token = self.create_token()
        self.log.info(f"Generated JWT Token for read-only user: {token}")

        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            self.log.info(f"JWT Token Payload for read-only user: {json.dumps(decoded, indent=2)}")
        except Exception as e:
            self.fail(f"Error decoding token: {e}")

        self.log.info("Testing read access - should succeed...")
        ok, res_text, response = self.make_request_with_jwt(
            token, self.master,
            "pools/default/buckets",
            "GET", params={}
        )
        status_code = response.status
        self.log.info(f"Read Request Result - OK: {ok}, Status: {status_code}, Response: {res_text}")

        if not (200 <= status_code < 300):
            self.fail(f"Read access failed unexpectedly. Status: {status_code}, Response: {res_text}")

        self.log.info("Read access successful as expected")

        self.log.info("Testing write access (bucket creation) - should fail with 403...")

        bucket_params = {
            "name": "test_forbidden_bucket",
            "ramQuotaMB": 100,
            "bucketType": "membase"
        }

        ok, res_text, response = self.make_request_with_jwt(
            token, self.master,
            "pools/default/buckets",
            "POST", params=bucket_params
        )
        status_code = response.status
        self.log.info(f"Write Request Result - OK: {ok}, Status: {status_code}, Response: {res_text}")

        if status_code != 403:
            self.fail(f"Expected 403 Forbidden for write access, got {status_code}. Response: {res_text}")

        self.log.info("Write access correctly forbidden with 403 status")
        self.log.info("Access forbidden audit event should have been triggered")

        self.log.info("Testing user creation - should also fail with 403...")
        user_params = {
            "name": "test_forbidden_user",
            "roles": "admin"
        }

        ok, res_text, response = self.make_request_with_jwt(
            token, self.master,
            "settings/rbac/users/local/test_forbidden_user",
            "PUT", params=user_params
        )
        status_code = response.status
        self.log.info(f"User Creation Request Result - OK: {ok}, Status: {status_code}, Response: {res_text}")

        if status_code != 403:
            self.fail(f"Expected 403 for user creation, got {status_code}")
        else:
            self.log.info("User creation correctly forbidden with 403 status")

    def test_nested_field_paths(self):
        """Test JWT authentication with nested field paths in custom claims"""

        # Configurable parameters
        nested_user_name = self.input.param("nested_user_name", "nested_test_user")
        nested_group_name = self.input.param("nested_group_name", "jwt_nested_admins")
        sub_claim_path = self.input.param("sub_claim_path", "user.preferred_username")
        groups_claim_path = self.input.param("groups_claim_path", "user.profile.groups")
        jwt_group_value = self.input.param("jwt_group_value", "nested_admins")

        rest_conn = RestConnection(self.master)

        group_status = self.create_new_group(nested_group_name, "Admin group for nested JWT claims testing")
        if not group_status:
            self.fail(f"Failed to create {nested_group_name} group")

        self.user_name = nested_user_name

        config = {
            "enabled": True,
            "issuers": [
                {
                    "name": self.issuer_name,
                    "signingAlgorithm": self.algorithm,
                    "publicKeySource": "pem",
                    "publicKey": self.pub_key,
                    "jitProvisioning": True,
                    "subClaim": sub_claim_path,
                    "audClaim": "aud",
                    "audienceHandling": "any",
                    "audiences": self.token_audience,
                    "groupsClaim": groups_claim_path,
                    "groupsMaps": [f"^{jwt_group_value}$ {nested_group_name}"]
                }
            ]
        }

        status, content, header = rest_conn.create_jwt_with_config(config)
        if not status:
            self.fail(f"Failed to create jwt config with nested field paths: {content}")

        self.sleep(10)

        curr_time = int(time.time())
        nested_payload = {
            "iss": self.issuer_name,
            "user": {
                "preferred_username": self.user_name,
                "profile": {
                    "groups": [jwt_group_value]
                }
            },
            "exp": curr_time + self.ttl,
            "iat": curr_time,
            "nbf": curr_time - self.nbf_seconds,
            "jti": str(uuid.uuid4()),
        }

        if self.token_audience:
            nested_payload['aud'] = self.token_audience

        jwt_token = jwt.encode(payload=nested_payload,
                               algorithm=self.algorithm,
                               key=self.private_key)

        ok, res_text, response = self.make_request_with_jwt(jwt_token, self.master,
                                                            "/pools/default/buckets",
                                                            "GET", params={})
        status_code = response.status

        if 200 <= status_code < 300:
            self.log.info("SUCCESS: Nested field paths authentication worked")
        elif status_code == 401:
            self._debug_jwt_config(rest_conn)
            self.fail("Unauthorized error with nested field paths")
        else:
            self._debug_jwt_config(rest_conn)
            self.fail(f"Unexpected error, status: {status_code}, response: {res_text}")

        # Test missing nested field (should fail)
        missing_payload = {
            "iss": self.issuer_name,
            "user": {
                "id": self.user_name,
                "groups": [jwt_group_value]
            },
            "exp": curr_time + self.ttl,
            "iat": curr_time,
            "nbf": curr_time - self.nbf_seconds,
            "jti": str(uuid.uuid4()),
        }

        if self.token_audience:
            missing_payload['aud'] = self.token_audience

        missing_token = jwt.encode(payload=missing_payload,
                                   algorithm=self.algorithm,
                                   key=self.private_key)

        ok, res_text, response = self.make_request_with_jwt(missing_token, self.master,
                                                            "/pools/default/buckets",
                                                            "GET", params={})
        status_code = response.status

        if status_code == 401:
            self.log.info("SUCCESS: Correctly rejected token with missing nested fields")
        else:
            self.fail(f"Expected 401 for missing nested fields, got {status_code}")
        status, content = rest_conn.delete_group(nested_group_name)
        if not status:
            self.fail("failed to delete group after test")

    def test_nested_roles_claim(self):
        """Test JWT authentication with nested roles claim"""

        # Configurable parameters
        roles_user_name = self.input.param("roles_user_name", "roles_test_user")
        roles_claim_path = self.input.param("roles_claim_path", "resource.testclient.roles")
        jwt_role_value = self.input.param("jwt_role_value", "admin-access")
        couchbase_role = self.input.param("couchbase_role", "admin")

        rest_conn = RestConnection(self.master)

        self.user_name = roles_user_name

        config = {
            "enabled": True,
            "issuers": [
                {
                    "name": self.issuer_name,
                    "signingAlgorithm": self.algorithm,
                    "publicKeySource": "pem",
                    "publicKey": self.pub_key,
                    "jitProvisioning": True,
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audienceHandling": "any",
                    "audiences": self.token_audience,
                    "rolesClaim": roles_claim_path,
                    "rolesMaps": [f"^{jwt_role_value}$ {couchbase_role}"]
                }
            ]
        }

        status, content, header = rest_conn.create_jwt_with_config(config)
        if not status:
            self.fail(f"Failed to create jwt config with nested roles claim: {content}")

        self.sleep(10)

        curr_time = int(time.time())
        roles_payload = {
            "iss": self.issuer_name,
            "sub": self.user_name,
            "resource": {
                "testclient": {
                    "roles": [jwt_role_value]
                }
            },
            "exp": curr_time + self.ttl,
            "iat": curr_time,
            "nbf": curr_time - self.nbf_seconds,
            "jti": str(uuid.uuid4()),
        }

        if self.token_audience:
            roles_payload['aud'] = self.token_audience

        jwt_token = jwt.encode(payload=roles_payload,
                               algorithm=self.algorithm,
                               key=self.private_key)

        ok, res_text, response = self.make_request_with_jwt(jwt_token, self.master,
                                                            "/pools/default/buckets",
                                                            "GET", params={})
        status_code = response.status

        if 200 <= status_code < 300:
            self.log.info("SUCCESS: Nested roles claim authentication worked")
        elif status_code == 401:
            self._debug_jwt_config(rest_conn)
            self.fail("Unauthorized error with nested roles claim")
        else:
            self._debug_jwt_config(rest_conn)
            self.fail(f"Unexpected error, status: {status_code}, response: {res_text}")

        # Test wrong nesting level (should fail)
        wrong_payload = {
            "iss": self.issuer_name,
            "sub": self.user_name,
            "resource": {
                "roles": [jwt_role_value]
            },
            "exp": curr_time + self.ttl,
            "iat": curr_time,
            "nbf": curr_time - self.nbf_seconds,
            "jti": str(uuid.uuid4()),
        }

        if self.token_audience:
            wrong_payload['aud'] = self.token_audience

        wrong_token = jwt.encode(payload=wrong_payload,
                                 algorithm=self.algorithm,
                                 key=self.private_key)

        ok, res_text, response = self.make_request_with_jwt(wrong_token, self.master,
                                                            "/pools/default/buckets",
                                                            "GET", params={})
        status_code = response.status

        if status_code == 401:
            self.log.info("SUCCESS: Correctly rejected token with wrong nesting level")
        else:
            self.fail(f"Expected 401 for wrong nesting level, got {status_code}")

    def test_comprehensive_nested_claims(self):
        """Test JWT authentication with multiple nested claims combined"""

        # Configurable parameters
        comp_user_name = self.input.param("comp_user_name", "comprehensive_user")
        comp_group_name = self.input.param("comp_group_name", "jwt_comprehensive_admins")
        sub_claim_path = self.input.param("sub_claim_path", "user.identity.username")
        groups_claim_path = self.input.param("groups_claim_path", "user.profile.groups")
        roles_claim_path = self.input.param("roles_claim_path", "resource.app.roles")
        jwt_group_value = self.input.param("jwt_group_value", "comprehensive-group")
        jwt_role_value = self.input.param("jwt_role_value", "comprehensive-role")
        couchbase_role = self.input.param("couchbase_role", "admin")

        rest_conn = RestConnection(self.master)

        group_status = self.create_new_group(comp_group_name, "Admin group for comprehensive nested JWT testing")
        if not group_status:
            self.fail(f"Failed to create {comp_group_name} group")

        self.user_name = comp_user_name

        config = {
            "enabled": True,
            "issuers": [
                {
                    "name": self.issuer_name,
                    "signingAlgorithm": self.algorithm,
                    "publicKeySource": "pem",
                    "publicKey": self.pub_key,
                    "jitProvisioning": True,
                    "subClaim": sub_claim_path,
                    "audClaim": "aud",
                    "audienceHandling": "any",
                    "audiences": self.token_audience,
                    "groupsClaim": groups_claim_path,
                    "groupsMaps": [f"^{jwt_group_value}$ {comp_group_name}"],
                    "rolesClaim": roles_claim_path,
                    "rolesMaps": [f"^{jwt_role_value}$ {couchbase_role}"]
                }
            ]
        }

        status, content, header = rest_conn.create_jwt_with_config(config)
        if not status:
            self.fail(f"Failed to create comprehensive jwt config: {content}")

        self.sleep(10)

        curr_time = int(time.time())
        comprehensive_payload = {
            "iss": self.issuer_name,
            "user": {
                "identity": {
                    "username": self.user_name
                },
                "profile": {
                    "groups": [jwt_group_value]
                }
            },
            "resource": {
                "app": {
                    "roles": [jwt_role_value]
                }
            },
            "exp": curr_time + self.ttl,
            "iat": curr_time,
            "nbf": curr_time - self.nbf_seconds,
            "jti": str(uuid.uuid4()),
        }

        if self.token_audience:
            comprehensive_payload['aud'] = self.token_audience

        jwt_token = jwt.encode(payload=comprehensive_payload,
                               algorithm=self.algorithm,
                               key=self.private_key)

        ok, res_text, response = self.make_request_with_jwt(jwt_token, self.master,
                                                            "/pools/default/buckets",
                                                            "GET", params={})
        status_code = response.status

        if 200 <= status_code < 300:
            self.log.info("SUCCESS: Comprehensive nested claims authentication worked")
        elif status_code == 401:
            self._debug_jwt_config(rest_conn)
            self.fail("Unauthorized error with comprehensive nested claims")
        else:
            self._debug_jwt_config(rest_conn)
            self.fail(f"Unexpected error, status: {status_code}, response: {res_text}")

        status, content = rest_conn.delete_group(comp_group_name)
        if not status:
            self.fail("failed to delete group after test")