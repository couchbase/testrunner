import os
import re
import base64
import urllib
import requests
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from multiprocessing.dummy import Pool as ThreadPool

from lib import testconstants
from basetestcase import BaseTestCase
from lib.Cb_constants.CBServer import CbServer
from pytests.security.rbac_base import RbacBase
from pytests.security.saml_util import SAMLUtils
from lib.membase.api.rest_client import RestConnection
from pytests.security.x509_multiple_CA_util import x509main
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.x509main import x509main as x509main_client
from pytests.security.ntonencryptionBase import ntonencryptionBase


class SAMLTest(BaseTestCase):

    def setUp(self):
        super(SAMLTest, self).setUp()
        self.okta_account = os.getenv("OKTA_ACCOUNT")
        self.okta_token = os.getenv("OKTA_TOKEN")
        self.okta_app_id = os.getenv("OKTA_APP_ID")
        self.log.info(f"OKTA_ACCOUNT: {self.okta_account}")
        self.log.info(f"OKTA_TOKEN: {self.okta_token}")
        self.log.info(f"OKTA_APP_ID: {self.okta_app_id}")

        self.saml_util = SAMLUtils(self.log, self.okta_account)

        try:
            self.okta_app_id, self.idp_metadata = self.saml_util.update_okta_app_ip(self.okta_token,
                                                                                    self.okta_app_id,
                                                                                    self.master.ip)
            self.saml_util.upload_idp_metadata(self.okta_app_id, self.idp_metadata)
        except Exception as e:
            self.fail(f"Failed as could not update app/app metadata for the reason: {e}")
        self.rest = RestConnection(self.master)
        self.remote_connection = RemoteMachineShellConnection(self.master)
        self.saml_user = self.input.param("saml_user", "samridh.anand@couchbase.com")
        self.saml_passcode = self.input.param("saml_passcode", "Password@123")

    def teardown(self):
        self.log.info("Teardown Start")
        self.log.info("Teardown End")

    def suite_tearDown(self):
        self.log.info("Suite Teardown Start")
        self.log.info("Suite Teardown End")

    def login_sso(self, https=False):
        """
        Initiates an SSO login using direct browser-style API calls
        Step 1: GET /saml/auth - Initiate SAML auth and get initial data
        Step 2: POST to Okta - Send SAML request and authenticate to get SAML response
        Step 3: POST /saml/consume - Send SAML response to Couchbase and get session cookie
        Step 4: GET / - Verify login with session cookie
        """
        try:
            # Step 1: Initiate SAML auth (direct browser style)
            self.log.info('Step 1: Initiate SAML auth')
            auth_result = self.saml_util.direct_saml_auth(self.master)

            if len(auth_result) == 3:
                # Got redirect response
                action, cookie_string, cookies = auth_result
                saml_request = None  # Will be extracted from Okta response
                self.log.info(f"Got redirect to IdP: {action}")
            elif len(auth_result) == 4:
                # Got direct form response
                action, saml_request, cookie_string, cookies = auth_result
                self.log.info(f"Got form action: {action}")
                self.log.info(f"Got SAML request: {saml_request[:100]}...")

            self.log.info(f"Initial cookies: {cookie_string}")
            # Step 2: Authenticate with IdP and get SAML response
            self.log.info('Step 2: Authenticate with IdP')
            if saml_request:
                # Direct form submission case
                SAMLResponse = self.saml_util.direct_idp_authenticate(
                    action, saml_request, self.saml_user, self.saml_passcode,
                    self.master.ip, cookie_string
                )
            else:
                # Redirect case - need to follow redirect first
                self.log.info(f"Following redirect to: {action}")
                # This is more complex and may need additional handling
                # For now, let's assume we have the SAML request from the redirect
                raise NotImplementedError("Redirect case not fully implemented - please use form case")

            self.log.info(f"Got SAML response: {SAMLResponse[:100]}...")
            # Step 3: Send SAML response to Couchbase and get session cookie
            self.log.info('Step 3: Send SAML response to Couchbase')
            session_cookie_name, session_cookie_value = self.saml_util.direct_saml_consume(
                self.master, SAMLResponse, cookie_string
            )
            self.log.info(f"session_cookie_name: {session_cookie_name}")
            self.log.info(f"session_cookie_value: {session_cookie_value}")
            # Step 4: Verify login
            self.log.info('Step 4: Verify SSO login')
            is_logged_in = self.saml_util.verify_direct_login(
                self.master, session_cookie_name, session_cookie_value
            )

            if is_logged_in:
                self.log.info("✅ SSO Login Success - Direct Browser Flow")
            else:
                self.log.error("❌ SSO Login Failed - Direct Browser Flow")
                raise Exception("SSO Login Failed - Direct Browser Flow")
        except Exception as e:
            self.log.error(f"❌ Direct browser SSO login failed: {str(e)}")
            raise Exception

    def get_saml_response_from_okta(self):
        saml_response = None
        cookie_string = None
        auth_result = self.saml_util.direct_saml_auth(self.master)
        if len(auth_result) == 3:
            action, cookie_string, cookies = auth_result
            saml_request = None
            self.log.info(f"Got redirect to IdP: {action}")
        elif len(auth_result) == 4:
            action, saml_request, cookie_string, cookies = auth_result
            self.log.info(f"Got form action: {action}")
            self.log.info(f"Got SAML request: {saml_request[:100]}...")
        self.log.info(f"Initial cookies: {cookie_string}")
        self.log.info('Authenticate with IdP')
        if saml_request:
            saml_response = self.saml_util.direct_idp_authenticate(
                action, saml_request, self.saml_user, self.saml_passcode,
                self.master.ip, cookie_string
            )
            return saml_response, cookie_string
        else:
            self.log.info(f"Following redirect to: {action}")
            raise NotImplementedError("Redirect case not fully implemented - please use form case")

    def _login_sso_original(self, https=False):
        """
        Original SSO login method (kept as fallback)
        Step 1: Initiate single sign on
        Step 2: Redirect to the IdP
        Step 3: SSO user authentication via the IdP
        Step 4: Get the SAML response from the IdP
        Step 5: Send the SAML response to Couchbase and set a session for the SSO user
        Step 6: Use the session and verify SSO login
        """
        # Step 1: Initiate single sign on
        action, SAMLRequest = self.saml_util.saml_auth_url(self.master)
        self.log.info("action: {0}".format(action))
        self.log.info("SAMLRequest: {0}".format(SAMLRequest))
        # Step 2: Redirect to the IdP
        self.log.info('Redirect to the IdP')
        state_token, cookie_string, j_session_id = self.saml_util.idp_redirect(action,
                                                                               SAMLRequest)
        self.log.info("state_token: {0}".format(state_token))
        self.log.info("cookie_string: {0}".format(cookie_string))
        self.log.info("j_session_id: {0}".format(j_session_id))
        # Step 3: SSO user authentication via the IdP
        self.log.info('SSO user authentication via the IdP')
        next_url, j_session_id = self.saml_util.idp_login(self.saml_user, self.saml_passcode,
                                                          state_token,
                                                          cookie_string, j_session_id)
        self.log.info("next_url: {0}".format(next_url))
        self.log.info("j_session_id: {0}".format(j_session_id))
        # Step 4: Get the SAML response from the IdP
        self.log.info('Get the SAML response from the IdP')
        SAMLResponse = self.saml_util.get_saml_response(next_url, cookie_string, j_session_id)
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # Step 5: Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
        session_cookie_name, session_cookie_value = self.saml_util \
            .saml_consume_url(self.master, cookie_string, SAMLResponse, https=https)
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        # Step 6: Use the session and verify SSO login
        self.log.info('Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")

    def validate_saml_logs(self, start_time, end_time):
        logs_dir = testconstants.LINUX_COUCHBASE_LOGS_PATH

        def check_logs(grep_output_list):
            for line in grep_output_list:
                # eg: 2021-07-12T04:03:45
                timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
                match_obj = timestamp_regex.search(line)
                if not match_obj:
                    print("%s does not match any timestamp" % line)
                    return True
                timestamp = match_obj.group()
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
                if start_time <= timestamp <= end_time:
                    return True
            return False

        log_files = self.remote_connection.execute_command(
            "ls " + os.path.join(logs_dir, "info.log"))[0]
        log_file = log_files[0]
        log_file = log_file.strip("\n")
        grep_for_str = "SAML settings were modified"
        cmd_to_run = "grep -r '%s' %s" \
                     % (grep_for_str, log_file)
        grep_output = self.remote_connection.execute_command(cmd_to_run)[0]
        return check_logs(grep_output)

    def test_sso_login(self):
        """
        Tests a single sign on login to Couchbase
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on and verify login
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # STEP 1: Enable SAML
        self.log.info("STEP 1: Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # STEP 2: Add the SSO user as an external user to Couchbase
        self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})

        try:
            content = self.rest.add_external_user(self.saml_user, body)
            self.log.info("External user add response: {0}".format(content))

            if not content or content.strip() == "":
                self.log.warning(
                    "External user addition returned empty response - user might already exist or API failed")
                # Try to verify the user exists
                try:
                    users_response = RbacBase().get_all_users(self.rest)
                    self.log.info(f"Checking if user {self.saml_user} exists in current users...")
                    if self.saml_user in str(users_response):
                        self.log.info(f"✅ User {self.saml_user} found in existing users")
                    else:
                        self.log.error(
                            f"❌ User {self.saml_user} NOT found in existing users - this will cause SAML authentication to fail")
                        self.fail(f"Failed to add external user {self.saml_user} - SAML authentication will not work")
                except Exception as verify_error:
                    self.log.warning(f"Could not verify user existence: {verify_error}")
                    self.fail(f"Could not verify users exitence due to exception: {verify_error}")
            else:
                self.log.info(f"✅ Successfully added external user: {self.saml_user}")

        except Exception as e:
            self.log.error(f"Failed to add external user {self.saml_user}: {str(e)}")
            self.fail(f"External user addition failed: {str(e)}")
        # STEP 3: Initiate single sign on and verify login
        self.log.info("STEP 3: Initiate single sign on and verify login")
        self.login_sso()

    def test_sso_login_negative(self):
        """
        A user should not be able to use SSO after SAML has been disabled
        STEP 1: Disable  SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Verify that the initiation to the IdP fails
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # set "enabled" to "false" to disable SAML
        body = {
            "enabled": "false",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # STEP 1: Enable SAML
        self.log.info("STEP 1: Disable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # STEP 2: Add the SSO user as an external user to Couchbase
        self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(
            self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # STEP 3 & 4: Initiate single sign on
        self.log.info('STEP 3: Initiate single sign on')
        try:
            self.login_sso()
            self.log.info("Test should have failed")
            self.fail("Login should not be possible")
        except Exception as e:
            self.log.info(f"Login initiation to the IdP failed as expected: {e}")

    def test_enable_saml_unauthz(self):
        """
        A couchbase user without the role of Full Administrator should not be able to enable SAML
        based authentication for the Web UI
        STEP 1: Enable SAML as a non-admin
        STEP 2: Verify that the SAML was not enabled
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Include few non-admin roles
        self.log.info("Add few non-admin users")
        roles = ['cluster_admin', 'ro_admin', 'ro_security_admin']
        for role in roles:
            user_name = "user_" + role
            role = role
            password = "password"
            payload = "name={0}&roles={1}&password={2}".format(user_name, role, password)
            self.log.info("User name -- {0} :: role -- {1}".format(user_name, role))
            self.rest.add_set_builtin_user(user_name, payload)
            serverinfo_dict = {"ip": self.master.ip, "port": self.master.port,
                               "username": user_name, "password": password}
            rest_non_admin = RestConnection(serverinfo_dict)
            body = {
                "enabled": "true",
                "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
                "idpMetadataOrigin": "upload",
                "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
                "spAssertionDupeCheck": "disabled",
                "spBaseURLScheme": "http",
                "spCertificate": self.saml_util.saml_resources["spCertificate"],
                "spKey": self.saml_util.saml_resources["spKey"]
            }
            # Enable SAML and verify that the user does not have permissions to modify SAML settings
            self.log.info("Enable SAML and verify that the user does not have permissions to "
                          "modify SAML settings")
            status, content, header = rest_non_admin.modify_saml_settings(body)
            if not status:
                self.log.info("SAML enable failed as expected")
            else:
                self.fail("User should be unauthorized to enable SAML")

    def test_login_with_incomplete_saml_response(self):
        """
        Passing an incomplete SAML response to CB Server should fail and not set a session for
        the SSO user
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Send the incomplete SAML response to Couchbase and set a session for the SSO user
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # STEP 1: Enable SAML
        self.log.info("STEP 1: Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # STEP 2: Add the SSO user as an external user to Couchbase
        self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(
            self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        self.log.info('Step 1: Initiate SAML auth')
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info(f"Got SAML response: {SAMLResponse[:100]}...")
        try:
            self.saml_util.direct_saml_consume(self.master, SAMLResponse[:-1], cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e}")
        else:
            self.fail("SSO session creation should have failed for incomplete SAML response")

    def test_SSO_no_UI(self):
        """
        If the UI is disabled in Couchbase, then SAML based authentication should not be available
        STEP 1: Disable  UI
        Step 2: Enable SAML
        STEP 3: Add the SSO user as an external user to Couchbase
        STEP 4: Initiate single sign on
        Step 5: Verify that the initiation to the IdP fails
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # STEP 1: Disable UI
        self.log.info("STEP 1: Disable UI")
        cmd = "/opt/couchbase/bin/couchbase-cli setting-security -c localhost -u Administrator -p" \
              " password --set --disable-http-ui 1 "
        o, err = self.remote_connection.execute_command(cmd, debug=True)
        if err:
            self.fail(f"Failed to disable UI: {err}")
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # STEP 2: Enable SAML
        self.log.info("STEP 2: Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # STEP 3: Add the SSO user as an external user to Couchbase
        self.log.info("STEP 3: Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(
            self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # STEP 4 & 5: Initiate single sign on
        self.log.info('STEP 4: Initiate single sign on')
        try:
            self.login_sso()
            self.fail("Login initiation to the IdP should have failed after disabling UI")
        except Exception as e:
            self.log.info(f"Login initiation to the IdP failed as expected: {e}")
        finally:
            self.log.info("Enable UI back")
            cmd = "/opt/couchbase/bin/couchbase-cli setting-security -c localhost -u Administrator -p" \
                  " password --set --disable-http-ui 0 "
            o, err = self.remote_connection.execute_command(cmd, debug=True)
            if err:
                self.fail(f"Failed to enable UI: {err}")

    def test_SSO_with_certificates_configured(self):
        """
        Users should be able to use all the existing authentication mechanisms available along with
        SSO. This test checks the working of client certificate authentication along with SSO.
        """
        self.x509 = x509main(host=self.master)
        self.x509_client = x509main_client(host=self.master)
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.test_sso_login()

    def test_SSO_with_LDAP(self):
        """
        Test SSO when LDAP is enabled.
        As both are external authentication domains, verify both work simultaneously
        STEP 1: Enable LDAP
        STEP 2: Authenticate an LDAP user
        STEP 3: Enable SAML
        STEP 4: Login via SSO
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("STEP 1: Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user("user1", body)
        self.log.info("Content: {0}".format(content))
        # STEP 1: Setup and enable LDAP
        self.log.info("STEP 2: Setup and enable LDAP")
        rest = RestConnection(self.master)
        param = {
            'hosts': '{0}'.format("172.23.124.20"),
            'port': '{0}'.format("389"),
            'encryption': '{0}'.format("None"),
            'bindDN': '{0}'.format("cn=admin,dc=couchbase,dc=com"),
            'bindPass': '{0}'.format("p@ssw0rd"),
            'authenticationEnabled': '{0}'.format("true"),
            'userDNMapping': '{0}'.format('{"template":"uid=%u,ou=Users,dc=couchbase,dc=com"}')
        }
        rest.setup_ldap(param, '')
        # STEP 2: Authenticate an LDAP user
        self.log.info("STEP 3: Authenticate an LDAP user")
        cmd = "curl -u user1:password " + self.master.ip + ":8091/pools/default"
        self.remote_connection.execute_command(cmd, debug=True)
        # STEP 3: Enable SAML and Login via SSO
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Adding "usernameAttribute" field to the body as "given_name"
        # "given_name" maps to "user1" in Okta IdP
        # this is done to check the integrity when LDAP also has a user "user1"
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "usernameAttribute": "given_name"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # STEP 4: Initiate single sign on and verify
        try:
            self.login_sso()
        except Exception as e:
            self.fail(f"Failed to login via SSO: {e}")
        finally:
            self.rest.delete_external_user("user1")

    def test_SAML_group_attributes(self):
        """
        SAML SSO must be able to assert attributes for users allowing for group membership to be
        configured at the SSO provider.
        Note:
            IdP has a group configured named "Group_SAML_Test"
            The group includes the user "dave"
            No external user "dave" created
        STEP 1: Configure SAML settings in CB Server to enable group attributes
        STEP 2: Initiate SSO
        STEP 3: Verify it fails as no group created in CB Server
        STEP 4: Create a group named "Group_SAML_Test" in CB Server
        STEP 5: Initiate SSO
        STEP 6: Verify SSO login is a success
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Adding "groupsAttribute", "groupsAttributeSep", "groupsFilterRE" field to the body
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "groupsAttribute": "groups",
            "groupsAttributeSep": ",",
            "groupsFilterRE": "Group_SAML_Test"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Ensure no group named Group_SAML_Test in CB server
        self.rest.delete_group("Group_SAML_Test")
        # Ensure no external user present
        try:
            self.rest.delete_external_user(self.saml_user)
        except Exception as e:
            self.log.error(f"Extrernal user deletion failed for the reason: {e}")
        # Initiate single sign on
        try:
            # Should fail as no group named Group_SAML_Test created
            # Send the SAML response to Couchbase and set a session for the SSO user
            self.login_sso()
        except Exception as e:
            self.log.info("SSO failed as expected as no group created")
        else:
            self.fail("SSO should have failed as no group created")
        # Create group "Group_SAML_Test" in CB Server
        group_role = "admin"
        group_name = "Group_SAML_Test"
        self.log.info("Group name -- {0} :: Roles -- {1}".format(group_name, group_role))
        self.rest.add_group_role(group_name, group_name, group_role, None)
        try:
            self.login_sso()
        except Exception as e:
            self.fail(f"Test should have passed as group is created: {e}")

    def test_SAML_role_attributes(self):
        """
        SAML SSO must be able to assert role attributes for users.
        Note:
            IdP has a group configured named "ro_admin"
            The group includes the user "dave"
        STEP 1: Configure SAML settings in CB Server to enable role attributes
        STEP 2: Initiate SSO
        STEP 3: Verify it fails as no group created in CB Server
        STEP 4: Create a group named "Group_SAML_Test" in CB Server
        STEP 5: Initiate SSO
        STEP 6: Verify SSO login is a success
        """
        saml_user = self.saml_user
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # IdP has a group configured named "ro_admin"
        # The group includes the saml user
        # No external user needs to be created
        # Adding "rolesAttribute", "rolesAttributeSep", "rolesFilterRE" field to the body
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "rolesAttribute": "roles",
            "rolesAttributeSep": ",",
            "rolesFilterRE": "ro_admin"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Ensure no external user present
        try:
            self.rest.delete_external_user(saml_user)
        except Exception as e:
            self.log.error(f"Extrernal user deletion failed for the reason: {e}")
        self.log.info('Step 1: Initiate SAML auth')
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()

        self.log.info(f"Got SAML response: {SAMLResponse[:100]}...")
        # Step 3: Send SAML response to Couchbase and get session cookie
        self.log.info('Step 3: Send SAML response to Couchbase')
        session_cookie_name, session_cookie_value = self.saml_util.direct_saml_consume(
            self.master, SAMLResponse, cookie_string
        )
        self.log.info(f"session_cookie_name: {session_cookie_name}")
        self.log.info(f"session_cookie_value: {session_cookie_value}")

        self.log.info('Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")
        # Verify saml user has permission of a Read only Admin
        response = self.saml_util.sso_user_permission_details(self.master, session_cookie_name,
                                                              session_cookie_value)
        response = response.json()
        assert [{'role': 'ro_admin'}] == response['roles']
        assert saml_user == response['id']
        assert 'external' == response['domain']
        self.log.info("Verified that saml user has permission of a Read only Admin")

    def test_input_combinations(self):
        """
        1. Invalid/wrong input
        2. Expired values of certificates/assertions
        3. Invalid credentials
        4. Unexpected Request Parameters: Send unexpected or malformed parameters in the SAML
           request to check the robustness of error handling.
        """
        # Case 1: Invalid input
        # "spAssertionDupeCheck": "enabled",
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "enabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        try:
            # STEP 1: Enable SAML
            self.log.info("STEP 1: Enable SAML")
            status, content, header = self.rest.modify_saml_settings(body)
            self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                          format(status, content, header))
            self.log.info("Get current SAML settings")
            status, content, header = self.rest.get_saml_settings()
            self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                          format(status, content, header))
            # STEP 2: Add the SSO user as an external user to Couchbase
            self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
            body = urllib.parse.urlencode({"roles": "admin"})
            content = self.rest.add_external_user(self.saml_user, body)
            self.log.info("Content: {0}".format(content))
            # STEP 3: Initiate single sign on and verify login
            self.log.info("STEP 3: Initiate single sign on and verify login")
            self.login_sso()
        except Exception as e:
            self.log.info(f"Fails as expected: {e}")
        else:
            self.fail("Should have failed for providing an invalid input")
        # Case 2: Expired certificate
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))

        def generate_valid_pair_but_expired_certificate():
            # Generate a private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            # Set the expiration date in the past (e.g., 30 days ago)
            expiration_date = datetime.utcnow() - timedelta(days=30)
            # Create a valid certificate
            valid_certificate = x509.CertificateBuilder().subject_name(x509.Name([
                x509.NameAttribute(x509.NameOID.COUNTRY_NAME, u"US"),
                x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, u"California"),
                x509.NameAttribute(x509.NameOID.LOCALITY_NAME, u"San Francisco"),
                x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, u"My Organization"),
                x509.NameAttribute(x509.NameOID.COMMON_NAME, u"example.com"),
            ])).issuer_name(x509.Name([
                x509.NameAttribute(x509.NameOID.COUNTRY_NAME, u"US"),
                x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, u"California"),
                x509.NameAttribute(x509.NameOID.LOCALITY_NAME, u"San Francisco"),
                x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, u"My Organization"),
                x509.NameAttribute(x509.NameOID.COMMON_NAME, u"example.com"),
            ])).not_valid_before(datetime.utcnow() - timedelta(days=365)).not_valid_after(
                expiration_date).serial_number(x509.random_serial_number()).public_key(
                private_key.public_key()).add_extension(
                x509.SubjectAlternativeName([x509.DNSName(u"example.com")]),
                critical=False,
            ).sign(private_key, hashes.SHA256(), default_backend())
            return valid_certificate, private_key

        expired_cert, expired_key = generate_valid_pair_but_expired_certificate()
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "enabled",
            "spBaseURLScheme": "http",
            "spCertificate": expired_cert,
            "spKey": expired_key
        }
        try:
            # STEP 1: Enable SAML
            self.log.info("STEP 1: Enable SAML")
            status, content, header = self.rest.modify_saml_settings(body)
            self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                          format(status, content, header))
            self.log.info("Get current SAML settings")
            status, content, header = self.rest.get_saml_settings()
            self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                          format(status, content, header))
            # STEP 2: Add the SSO user as an external user to Couchbase
            self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
            body = urllib.parse.urlencode({"roles": "admin"})
            content = self.rest.add_external_user(self.saml_user, body)
            self.log.info("Content: {0}".format(content))
            # STEP 3: Initiate single sign on and verify login
            self.log.info("STEP 3: Initiate single sign on and verify login")
            self.login_sso()
        except Exception as e:
            self.log.info(f"Fails as expected: {e}")
        else:
            self.fail("Should have failed for providing an expired certificate")
        # Case 3: Malformed input
        # spBaseURLScheme: <script>alert('Malicious Script');</script>
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "enabled",
            "spBaseURLScheme": "<script>alert('Malicious Script');</script>",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        try:
            # STEP 1: Enable SAML
            self.log.info("STEP 1: Enable SAML")
            status, content, header = self.rest.modify_saml_settings(body)
            self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                          format(status, content, header))
            self.log.info("Get current SAML settings")
            status, content, header = self.rest.get_saml_settings()
            self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                          format(status, content, header))
            # STEP 2: Add the SSO user as an external user to Couchbase
            self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
            body = urllib.parse.urlencode({"roles": "admin"})
            content = self.rest.add_external_user(self.saml_user, body)
            self.log.info("Content: {0}".format(content))
            # STEP 3: Initiate single sign on and verify login
            self.log.info("STEP 3: Initiate single sign on and verify login")
            self.login_sso()
        except Exception as e:
            self.log.info(f"Fails as expected: {e}")
        else:
            self.fail("Should have failed for providing a malformed input")

    def test_duplicate_saml_response_enabled(self):
        """
        STEP 1: CB SAML settings: SAML dupe check enabled
                spAssertionDupeCheck: global or local
        STEP 2: Initiate SSO login
        STEP 3: Store the prev SAML response
        Step 4: Replay the SAML response
        Step 5: Should not authenticate, throws error
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Adding spAssertionDupeCheck field to the body
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "global"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        session_cookie_name, session_cookie_value = self.saml_util.direct_saml_consume(
            self.master, SAMLResponse, cookie_string
        )
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        # Use the session and verify SSO login
        self.log.info('Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")
        # Store the previous SAML response
        prev_SAMLResponse = SAMLResponse
        # Send the previous SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the previous SAML response to Couchbase and set a session for '
                      'the SSO user')
        try:
            self.saml_util.direct_saml_consume(self.master, prev_SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} as it duplicate SAML assertion "
                          "was passed")
        else:
            self.fail("SSO session creation should have failed due to duplicate SAML assertion")

    def test_duplicate_saml_response_disabled(self):
        """
        STEP 1: CB SAML settings: SAML dupe check disabled
                spAssertionDupeCheck: disabled
        STEP 2: Initiate SSO login
        STEP 3: Store the prev SAML response
        Step 4: Replay the SAML response
        Step 5: Should authenticate, no error
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Adding spAssertionDupeCheck field to the body
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        session_cookie_name, session_cookie_value = self.saml_util.direct_saml_consume(
            self.master, SAMLResponse, cookie_string
        )
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        # Use the session and verify SSO login
        self.log.info('Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")
        prev_SAMLResponse = SAMLResponse
        # Send the previous SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the previous SAML response to Couchbase and set a session for the SSO '
                      'user')
        session_cookie_name, session_cookie_value = self.saml_util.direct_saml_consume(self.master,
                                                                                       prev_SAMLResponse,
                                                                                       cookie_string)
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        # Use the session and verify SSO login
        self.log.info('Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")

    def test_login_with_invalid_saml_response(self):
        """
        Pass an invalid SAML response to CB Server should fail and not set a session for the SSO
        user
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Send the invalid SAML response to Couchbase and set a session for the SSO user
                Done by removing a tag
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # replace a particular tag to make the SAML response invalid
        substr = "</saml2:AttributeStatement>"
        new_saml_response_xml = saml_response_xml.replace(substr, "")
        # encode back the modified SAML response
        new_SAMLResponse = new_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the invalid SAML response to Couchbase and set a session for the SSO '
                      'user')
        try:
            self.saml_util.direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e}")
        else:
            self.fail("SSO session creation should have failed for incomplete SAML response")

    def test_oversize_payload(self):
        """
        Pass an oversize valid SAML Response to CB server and verify that the authentication fails
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Send the oversize SAML response to Couchbase and set a session for the SSO user
                Done by adding additional tags
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find the <saml2:AttributeStatement> element
        attribute_statement = root.find(".//saml2:AttributeStatement",
                                        namespaces={"saml2":
                                                        "urn:oasis:names:tc:SAML:2.0:assertion"})
        # Define the additional attribute values
        # https://issues.couchbase.com/browse/MB-59583
        # 1000 = 243.376 kiloBytes: PASS
        # 2500 = 597.388 kiloBytes/432.55 KB(http://bytesizematters.com/): PASS
        # 5000 = 1187.38 kiloBytes: PASS
        # 7500 = 1777.38 kiloBytes: PASS
        # 9000 =  2131.376 kiloBytes: PASS
        # 10000 = 2367.38 kiloBytes: FAIL
        additional_attributes = self.input.param("additional_attributes", 10000)
        for i in range(additional_attributes):
            attribute = ET.Element("saml2:Attribute",
                                   {"Name": "role_" + str(i),
                                    "Format":
                                        "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified"})
            attribute_value = ET.Element("saml2:AttributeValue")
            attribute_value.text = "role_value_" + str(i)
            attribute.append(attribute_value)
            attribute_statement.append(attribute)
        # Serialize the modified XML back to a string
        oversize_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                     ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = oversize_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        self.log.info("Length: {0} kiloBytes".format(len(new_SAMLResponse) / 1000))
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            self.saml_util.direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for oversize SAML response")
        else:
            self.fail("SSO session creation should have failed for oversize SAML response")

    def test_issuer_validation(self):
        """
        Manipulate the issuer value and verify that the CB Server rejects the SAML Response
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Modify the Issuer value
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()

        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find all Issuer elements and update their text
        for issuer_element in root.iter("{urn:oasis:names:tc:SAML:2.0:assertion}Issuer"):
            old_value = issuer_element.text
            new_value = old_value + "0"
            issuer_element.text = new_value
        # Serialize the modified XML back to a string
        new_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = new_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for invalid Issuer")
        else:
            self.fail("SSO session creation should have failed for invalid Issuer")

    def test_audience_restriction(self):
        """
        Manipulate the audience restriction value and verify that the CB Server rejects the SAML
        Response
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Modify the Audience restriction value
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find the AudienceRestriction element and update its value
        audience_element = root.find('.//{urn:oasis:names:tc:SAML:2.0:assertion}Audience')
        if audience_element is not None:
            audience_element.text = "https://www.example.com/"
        # Serialize the modified XML back to a string
        new_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = new_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for invalid audience")
        else:
            self.fail("SSO session creation should have failed for invalid audience")

    def test_xml_signature_wrapping_attack(self):
        """
        Modify XML
            Add an attribute with its corresponding signature but a valid one.
            Generate a new digital signature for the malicious element. This signature should
            look valid, even though the element is not part of the original SAML assertion.
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Add a new valid XML tag and its digital signature
        Step 8: Verify that CB throws error and no session is set
        ToDo: Covers a simple case, can be extended to cover sophisticated signature wrapping attack
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "true",
            "spVerifyAssertionEnvelopSig": "true"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Add a new XML tag
        new_tag = ET.SubElement(root, 'NewTag')
        new_tag.text = 'This is a new tag'
        # Serialize the modified XML
        modified_xml_str = ET.tostring(root, encoding='utf-8').decode('utf-8')
        # Generate a digital signature
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        signature_data = modified_xml_str.encode('utf-8')
        signature = private_key.sign(
            signature_data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        # Add the digital signature to the XML
        signature_element = ET.SubElement(root, 'DigitalSignature')
        signature_element.text = signature.hex()
        # Serialize the modified XML back to a string, manually preserving namespaces
        modified_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                     ET.tostring(root,
                                                 method="xml",
                                                 encoding="utf-8",
                                                 short_empty_elements=False).decode()
        # encode back the modified SAML response
        new_SAMLResponse = modified_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for invalid audience")
        else:
            self.fail("SSO session creation should have failed for invalid audience")

    def test_invalid_signature(self):
        """
        Replace the certificate in the SAML Response with a signature that is not signed by a
        real Certificate Authority, by using a self-signed certificate. CB Server should reject it
        due to the untrusted certificate.
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Replace the certificate by a self-signed certificate
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()

        def generate_self_signed_certificate():
            key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Example Organization"),
                x509.NameAttribute(NameOID.COMMON_NAME, "example.com")])
            cert = x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(
                key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(
                datetime.utcnow()).not_valid_after(datetime.utcnow() + timedelta(days=365)).sign(
                key, hashes.SHA256(), default_backend())
            # Serialize the certificate to PEM format
            cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM)
            return cert_pem

        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Generate a self-signed certificate
        self_signed_certificate = generate_self_signed_certificate()
        # Find the X.509 certificate element
        x509_certificate_elements = root.findall(
            ".//{http://www.w3.org/2000/09/xmldsig#}X509Certificate")
        for x509_certificate_element in x509_certificate_elements:
            # Update the X.509 certificate value
            x509_certificate_element.text = self_signed_certificate.decode()
        # Serialize the modified XML back to a string, manually preserving namespaces
        modified_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                     ET.tostring(root,
                                                 method="xml",
                                                 encoding="utf-8",
                                                 short_empty_elements=False).decode()
        # encode back the modified SAML response
        new_SAMLResponse = modified_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for invalid audience")
        else:
            self.fail("SSO session creation should have failed for invalid audience")

    def test_network_delay(self):
        """
        Simulate network delay by adding time before sending the SAML Response
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Add time
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "global",
            "spVerifyAssertionSig": "true",
            "spVerifyAssertionEnvelopSig": "true"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Add time
        # 5: Fail = session was set
        # 10: Pass = session was not set
        # 15: Pass = session was not set
        self.sleep(60 * 10, "Stale the SAML response")
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for oversize SAML response")
        else:
            self.fail("SSO session creation should have failed for oversize SAML response")

    def test_invalid_data(self):
        """
        Remove an important XML element
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Remove an XML element
        Step 8: Verify that CB throws error and no session is set
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find and remove the saml2:NameID tag
        name_id_tag = root.find(".//{urn:oasis:names:tc:SAML:2.0:assertion}NameID")
        if name_id_tag is not None:
            parent = root.find(".//{urn:oasis:names:tc:SAML:2.0:assertion}Subject")
            parent.remove(name_id_tag)
        # Serialize the modified XML back to a string
        invalid_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                    ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = invalid_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            self.saml_util.direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for invalid SAML response")
        else:
            self.fail("SSO session creation should have failed for invalid SAML response")

    def test_session(self):
        """
        Confirm that these timestamps are set correctly to prevent unauthorized access to the
        session.
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Verify that the session timestamps are set correctly
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find the Conditions element
        conditions_element = root.find('.//saml2:Conditions', namespaces=namespaces)
        # Get NotBefore and NotOnOrAfter values
        not_before = conditions_element.get('NotBefore')
        not_on_or_after = conditions_element.get('NotOnOrAfter')
        from datetime import datetime
        current_time = datetime.utcnow()
        # Parse timestamps from the SAML assertion
        not_before_time = datetime.strptime(not_before, "%Y-%m-%dT%H:%M:%S.%fZ")
        not_on_or_after_time = datetime.strptime(not_on_or_after, "%Y-%m-%dT%H:%M:%S.%fZ")
        # Check if the current time is within the validity period
        output = not_before_time <= current_time <= not_on_or_after_time
        if output:
            self.log.info("SAML assertion is valid")
        else:
            self.fail("SAML assertion is invalid")

    def test_duplicate_assertion(self):
        """
        Replay Old SAML Assertion: Reuse a previously intercepted SAML Assertion within a fresh SAML
        Response and check if it's rejected.
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Send this SAML response to CB server
        Step 8: Initiate another SSO
        Step 9: Get the second SAML response
        Step 10: Replace second SAML response's assertion with the first one's
        Step 11: Verify the SSO fails
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "global",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))

        SAMLResponse1, cookie_string = self.get_saml_response_from_okta()

        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        session_cookie_name, session_cookie_value = self.saml_util \
            .direct_saml_consume(self.master, SAMLResponse1, cookie_string)
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        # convert to xml
        saml_response_xml1 = base64.b64decode(SAMLResponse1)
        saml_response_xml1 = saml_response_xml1.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root1 = ET.fromstring(saml_response_xml1)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find the SAML Assertion element
        saml_assertion1 = root1.find('.//saml2:Assertion',
                                     namespaces=namespaces)
        if saml_assertion1 is None:
            raise ValueError("SAML Assertion not found in the XML content.")
        SAMLResponse2, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse2))
        # convert to xml
        saml_response_xml2 = base64.b64decode(SAMLResponse2)
        saml_response_xml2 = saml_response_xml2.decode("utf-8")
        # Parse the XML content of the second SAML Response
        root2 = ET.fromstring(saml_response_xml2)
        # Find the SAML Assertion element in the second SAML Response
        saml_assertion2 = root2.find('.//saml2:Assertion', namespaces=namespaces)
        if saml_assertion2 is None:
            raise ValueError("SAML Assertion not found in the second SAML Response.")
        # Replace the SAML Assertion in the first SAML Response with the one from the second SAML
        # Response
        root2.remove(saml_assertion2)
        root2.append(saml_assertion1)
        # Serialize the modified XML back to a string
        invalid_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                    ET.tostring(root2, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = invalid_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the modified SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for a duplicate SAML response")
        else:
            self.fail("SSO session creation should have failed for a duplicate SAML response")

    def test_SAML_user_integrity(self):
        """
        Test SAML user integrity when:
        i>   SSO user is added
        ii>  SSO user permissions are modified
        iii> SSO user is deleted
        STEP 1: User is present in IdP but not in CB Server
        STEP 2: Add user to CB Sever and now do SSO
        STEP 3: Modify SSO user permission and verify the role
        STEP 4: Delete the SSO user in CB Server and verify the session is deactivated after
                the session timeout
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Ensure no external user present
        try:
            self.rest.delete_external_user(self.saml_user)
        except Exception as e:
            pass
        # STEP 1: User is present in IdP but not in CB Server
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))

        SAMLResponse, cookie_string= self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            self.saml_util.direct_saml_consume(self.master, SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} as no user added")
        else:
            self.fail("SSO session creation should have failed as no user added")
        # STEP 2: Add user to CB Sever and now do SSO
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        SAMLResponse, cookie_string= self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        session_cookie_name, session_cookie_value = self.saml_util \
            .direct_saml_consume(self.master, SAMLResponse, cookie_string)
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))
        # Use the session and verify SSO login
        self.log.info('Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")

        # STEP 3: Modify SSO user permission and verify the role
        self.log.info("Modify the user's permission")
        body = urllib.parse.urlencode({"roles": "ro_admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Use the same cookie and check the SSO user permission
        response = self.saml_util.sso_user_permission_details(self.master, session_cookie_name,
                                                              session_cookie_value)
        response = response.json()
        assert [{'role': 'ro_admin'}] == response['roles']
        assert self.saml_user == response['id']
        assert 'external' == response['domain']
        # STEP 4: Delete the SSO user in CB Server and verify the session is deactivated after
        # the session timeout
        self.log.info("Delete the SSO user from CB Server")
        content = self.rest.delete_external_user(self.saml_user)
        self.log.info("Content: {0}".format(content))
        # Set the browser session to 1 minute
        self.log.info("Set the browser session to 1 minute")
        status, content, header = self.rest.set_ui_session_timeout("60")
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.sleep(60, "wait until the session cookie expires")
        # Check if the cookie is still valid
        self.log.info('Use the session and to check if it is still valid')
        response = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                   session_cookie_value)
        assert not response
        self.log.info("Verified that the session cookie is invalid")

    def test_on_behalf_authentication(self):
        """
        Validate a user can do on-behalf-of authentication for an SSO user
        STEP 1: Enable SAML
        STEP 2: Add SSO user as an external user
        STEP 3: Perform on behalf of authentication
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Authenticate on behalf of
        self.log.info("Authenticate on behalf of")
        self.saml_util.cb_auth_on_behalf_of(self.master, self.saml_user,
                                            self.rest.username, self.rest.password)

    def test_tamper_signature(self):
        """
        Tamper with the Signature: within the <ds:Signature>
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Initiate another SSO
        Step 8: Get the second SAML response
        Step 9: Replace second SAML response's assertion with the first one's
        Step 10: Verify the SSO fails
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "true",
            "spVerifyAssertionEnvelopSig": "true"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find the SignatureValue element in the XML
        signature_value = root.find('.//ds:SignatureValue', namespaces=namespaces)
        if signature_value is None:
            raise ValueError("SignatureValue element not found in the XML.")
        # Tamper with the signature value (change some characters)
        original_signature = signature_value.text
        tampered_signature = original_signature[:10] + 'X' + original_signature[11:]
        # Update the SignatureValue with the tampered value
        signature_value.text = tampered_signature
        # Serialize the modified XML back to a string
        invalid_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                    ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = invalid_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            self.saml_util.direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for tampered sign SAML response")
        else:
            self.fail("SSO session creation should have failed for tampered sign invalid SAML "
                      "response")

    def test_xml_injection1(self):
        """
        Insert malicious XML tags or entities in the SAML response
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Create the malicious attribute
        malicious_attribute = ET.Element("malicious_attribute")
        script_element = ET.SubElement(malicious_attribute, "script")
        script_element.text = "alert('Malicious Script');"
        # Append the malicious attribute to the root element
        root.append(malicious_attribute)
        # Append the malicious attribute to the root element
        root.append(malicious_attribute)
        # Serialize the modified XML back to a string
        modified_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                     ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = modified_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util.direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
            self.log.info('Use the session and verify SSO login')
            is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                           session_cookie_value)
            if is_logged_in:
                self.log.info("SSO Log in Success")
            else:
                self.fail("SSO Log in Failed")
        except Exception as e:
            self.fail(f"Cb failed to consume SAML response, {str(e)}")

    def test_xml_injection2(self):
        """
        Insert malicious XML tags or entities in the SAML response
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Create the malicious attribute
        malicious_attribute = ET.Element("malicious_attribute")
        script_element = ET.SubElement(malicious_attribute, "erl_code")
        script_element.text = "io:format(\"This is an XML injection in Erlang\")"
        # Append the malicious attribute to the root element
        root.append(malicious_attribute)
        # Serialize the modified XML back to a string
        modified_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                     ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = modified_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
            # Use the session and verify SSO login
            self.log.info('Use the session and verify SSO login')
            is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                           session_cookie_value)
            if is_logged_in:
                self.log.info("SSO Log in Success")
            else:
                self.fail("SSO Log in Failed")
        except Exception as e:
            self.fail(f"Cb failed to consume SAML response, {str(e)}")

    def test_xml_injection3(self):
        """
        Insert malicious XML tags or entities in the SAML response
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Create the malicious attribute
        malicious_attribute = ET.Element("malicious_attribute")
        script_element = ET.SubElement(malicious_attribute, "erl_script")
        script_element.text = """io:format("Malicious Erlang Code Executed!~n"),os:cmd("echo 'This is a test' > /tmp/malicious.txt")"""
        # Append the malicious attribute to the root element
        root.append(malicious_attribute)
        # Serialize the modified XML back to a string
        modified_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + ET.tostring(root,
                                                                                            encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = modified_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
            self.log.info('Use the session and verify SSO login')
            is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                           session_cookie_value)
            if is_logged_in:
                self.log.info("SSO Log in Success")
            else:
                self.fail("SSO Log in Failed")
        except Exception as e:
            self.fail(f"Cb failed to consume SAML response, {str(e)}")

    def test_xml_injection4(self):
        """
        Insert malicious XML tags or entities in the SAML response
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "disabled",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))

        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Create the malicious attribute
        malicious_attribute = ET.Element("malicious_attribute")
        malicious_attribute.text = """
            <![CDATA[<!DOCTYPE foo [ <!ELEMENT foo ANY ><!ENTITY xxe SYSTEM "file:///etc/passwd" >]><malicious_element>&xxe;</malicious_element>]]>
            <![CDATA[<IMG SRC=http://www.exmaple.com/logo.gif onmouseover=javascript:alert('Attack');>]]>
        """
        # Append the malicious attribute to the root element
        root.append(malicious_attribute)
        # Append the malicious attribute to the root element
        root.append(malicious_attribute)
        # Serialize the modified XML back to a string
        modified_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + ET.tostring(root,
                                                                                            encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = modified_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the SAML response to Couchbase and set a session for the SSO user
        try:
            self.log.info('Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
            # Use the session and verify SSO login
            self.log.info('Use the session and verify SSO login')
            is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                           session_cookie_value)
            if is_logged_in:
                self.log.info("SSO Log in Success")
            else:
                self.fail("SSO Log in Failed")
        except Exception as e:
            self.fail(f"Cb failed to consume SAML response, {str(e)}")

    def test_unsigned_response_check(self):
        """
        STEP 0: IdP
            Assertion Signature = Unsigned
            Assertion Encryption = Unencrypted
        STEP 1: CB SAML settings: Validate assertion signature check enabled
            spVerifyAssertionSig: true
            Upload modified XML doc
        STEP 2: Initiate SSO login
        STEP 3: Fails with the message
            SAML assertion validation failed
            {envelope, error, no signature}
        """
        try:
            self.okta_app_id, self.idp_metadata = self.saml_util.change_assertion_signed(self.okta_token,
                                                                                    self.okta_app_id,
                                                                                     False)
            self.saml_util.upload_idp_metadata(self.okta_app_id, self.idp_metadata)
        except Exception as e:
            self.fail("Failed as could not update app/app metadata")
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Adding spAssertionDupeCheck field to the body
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spVerifyAssertionSig": "true"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e}")
        else:
            self.fail("SSO session creation should have failed for incomplete SAML response")

        try:
            self.okta_app_id, self.idp_metadata = self.saml_util.change_assertion_signed(self.okta_token,
                                                                                    self.okta_app_id,
                                                                                     True)
            self.saml_util.upload_idp_metadata(self.okta_app_id, self.idp_metadata)
        except Exception as e:
            self.fail(f"Failed to update app/app metadata: {e}")

    def test_unsigned_response_uncheck(self):
        """
        STEP 0: IdP
            Assertion Signature = Unsigned
            Assertion Encryption = Unencrypted
        STEP 1: CB SAML settings: Validate assertion signature check disabled
            spVerifyAssertionSig: false
            Upload modified XML doc
        STEP 2: Initiate SSO login
        STEP 3: Authenticates, no error
        """
        try:
            self.okta_app_id, self.idp_metadata = self.saml_util.change_assertion_signed(self.okta_token,
                                                                                    self.okta_app_id,
                                                                                     False)
            self.saml_util.upload_idp_metadata(self.okta_app_id, self.idp_metadata)
        except Exception as e:
            self.fail("Failed as could not update app/app metadata")
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Adding spAssertionDupeCheck field to the body
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spVerifyAssertionSig": "false"
        }
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("STEP 2: Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # Send the SAML response to Couchbase and set a session for the SSO user
        try:
            self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
            session_cookie_name, session_cookie_value = self.saml_util \
                .direct_saml_consume(self.master, SAMLResponse, cookie_string)
            self.log.info("session_cookie_name: {0}".format(session_cookie_name))
            self.log.info("session_cookie_value: {0}".format(session_cookie_value))
            # Use the session and verify SSO login
            self.log.info('Use the session and verify SSO login')
            is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                           session_cookie_value)
            if is_logged_in:
                self.log.info("SSO Log in Success")
            else:
                self.fail("SSO Log in Failed")
        except Exception as e:
            self.fail("SSO Failed due to the reason: {e}")

        try:
            self.okta_app_id, self.idp_metadata = self.saml_util.change_assertion_signed(self.okta_token,
                                                                                    self.okta_app_id,
                                                                                     True)
            self.saml_util.upload_idp_metadata(self.okta_app_id, self.idp_metadata)
        except Exception as e:
            self.fail(f"Failed to update app/app metadata: {e}")

    def test_custom_url_node_address(self):
        """
        spBaseURLType: custom
        spCustomBaseURL: http://10.123.233.104:8091
        STEP 1: sudo ip addr add 10.123.233.104/24 dev eth0
        STEP 2: ip addr show eth0
        STEP 3: curl -v -X PUT -u Administrator:password http://10.123.233.103:8091/node/controller/setupAlternateAddresses/external -d hostname=10.123.233.104
        STEP 4: curl -v -X GET -u Administrator:password http://10.123.233.103:8091/pools/default/nodeServices | jq
        STEP 5: CB SAML Settings: alt node address
        STEP 6: 10.123.233.103 -> 10.123.233.104 (4 places) in IdP
        STEP 7: Altered IdP metadata in CB SAML settings
        """
        self.alternate_ip_address = self.input.param("alternate_ip_address", "10.123.233.104")
        # Add an alternate IP address to the network interface
        cmd = "sudo ip addr add " + self.alternate_ip_address + "/24 dev eth0"
        self.log.info("Add an alternate IP address to the network interface")
        o, e = self.remote_connection.execute_command(cmd, debug=True)
        self.log.info("Output: {0} :: Error: {1}".format(o, e))
        # Show information about the network interfaces
        cmd = "sudo ip addr add " + self.alternate_ip_address + "/24 dev eth0"
        self.log.info("Show information about the network interfaces")
        o, e = self.remote_connection.execute_command(cmd, debug=True)
        self.log.info("Output: {0} :: Error: {1}".format(o, e))
        # Set up alternate node address in CB server
        self.rest.set_alternate_address(self.alternate_ip_address, alternate_ports={},
                                        network_type="external")
        # Display the node service information
        response = self.rest.get_pools_default()
        verify_alternate_ip_address = response["nodes"][0]["alternateAddresses"]["external"][
            "hostname"]
        self.log.info(verify_alternate_ip_address)
        if self.alternate_ip_address != verify_alternate_ip_address:
            self.fail("Error in setting up the alternate IP address")
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spBaseURLType": "custom"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Initiate single sign on and verify login
        self.log.info("Initiate single sign on and verify login")
        self.login_sso()

    def test_alternate_url_node_address(self):
        """
        spBaseURLType: custom
        spCustomBaseURL: http://10.123.233.104:8091
        STEP 1: sudo ip addr add 10.123.233.104/24 dev eth0
        STEP 2: ip addr show eth0
        STEP 3: curl -v -X PUT -u Administrator:password http://10.123.233.103:8091/node/controller/setupAlternateAddresses/external -d hostname=10.123.233.104
        STEP 4: curl -v -X GET -u Administrator:password http://10.123.233.103:8091/pools/default/nodeServices | jq
        STEP 5: CB SAML Settings: alt node address
        STEP 6: 10.123.233.103 -> 10.123.233.104 (4 places) in IdP
        STEP 7: Altered IdP metadata in CB SAML settings
        """
        self.alternate_ip_address = self.input.param("alternate_ip_address", "10.123.233.104")
        # Add an alternate IP address to the network interface
        cmd = "sudo ip addr add " + self.alternate_ip_address + "/24 dev eth0"
        self.log.info("Add an alternate IP address to the network interface")
        o, e = self.remote_connection.execute_command(cmd, debug=True)
        self.log.info("Output: {0} :: Error: {1}".format(o, e))
        # Show information about the network interfaces
        cmd = "sudo ip addr add " + self.alternate_ip_address + "/24 dev eth0"
        self.log.info("Show information about the network interfaces")
        o, e = self.remote_connection.execute_command(cmd, debug=True)
        self.log.info("Output: {0} :: Error: {1}".format(o, e))
        # Set up alternate node address in CB server
        self.rest.set_alternate_address(self.alternate_ip_address, alternate_ports={},
                                        network_type="external")
        # Display the node service information
        response = self.rest.get_pools_default()
        verify_alternate_ip_address = response["nodes"][0]["alternateAddresses"]["external"][
            "hostname"]
        self.log.info(verify_alternate_ip_address)
        if self.alternate_ip_address != verify_alternate_ip_address:
            self.fail("Error in setting up the alternate IP address")
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spBaseURLType": "alternate"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Initiate single sign on and verify login
        self.log.info("Initiate single sign on and verify login")
        self.login_sso()

    def test_log(self):
        """
        - All events wrt configuring SAML settings must be audited
        - Logged in audit.log
        - "id": 8273
        - "name": "modify SAML configuration"
        - "description": "SAML settings were modified"
        - Logs should not leak any sensitive and unintended information
        - If the UI is disabled after SAML access is enabled, a warning message must be written in
          the log explaining that SAML UI auth is enabled, but the UI is disabled.
        """
        # Log SAML settings modification
        start_time = datetime.now()
        self.test_sso_login()
        end_time = datetime.now()
        self.log.info("Start time: {0}".format(start_time))
        self.log.info("End time: {0}".format(end_time))
        validate_result = self.validate_saml_logs(start_time, end_time)
        self.log.info("Validation result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any saml logs in the specified interval")
        # Log SAML UI auth is enabled, but the UI is disabled
        start_time = datetime.now()
        self.test_SSO_no_UI()
        end_time = datetime.now()
        self.log.info("Start time: {0}".format(start_time))
        self.log.info("End time: {0}".format(end_time))
        validate_result = self.validate_saml_logs(start_time, end_time)
        self.log.info("Validation result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any saml logs in the specified interval")

    def test_high_quantity_saml_request(self):
        """
        Send large amount of saml requests to CB server - sequentially
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        no_of_iters = self.input.param("no_of_iters", 1)
        req_no = 0
        try:
            while req_no < no_of_iters:
                self.log.info("Request number: {0}".format(req_no))
                # Initiate single sign on and verify login
                self.log.info("Initiate single sign on and verify login")
                self.login_sso()
                req_no = req_no + 1
        except requests.ConnectionError as er:
            self.log.info(er)
            self.log.info("High quantity SAML requests fails as expected")
        else:
            self.fail("High quantity SAML requests should have given ConnectionError")

    def test_saml_over_https(self):
        """
        Test SSO over HTTPS
        1. Chose SP base URL scheme = https
        2. Update values in IdP to match https and 18091
        3. Initiate SSO and verify it passes
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "https",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Initiate single sign on and verify login
        self.log.info("Initiate single sign on and verify login fails over HTTP")
        try:
            self.login_sso(https=False)
        except AssertionError:
            self.log.info("Login initiation to the IdP failed over HTTP as expected")
        else:
            self.fail("Login initiation to the IdP should have failed over HTTP")
        self.log.info("Initiate single sign on and verify login is success over HTTPS")
        self.login_sso(https=True)

    def test_saml_response_without_assertion(self):
        """
        SAML Response Without Assertion: Send a SAML response without any assertion data and
        validate that the SP appropriately handles this scenario.
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Initiate another SSO
        Step 8: Get the second SAML response
        Step 9: Replace second SAML response's assertion with the first one's
        Step 10: Verify the SSO fails
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"],
            "spAssertionDupeCheck": "global",
            "spVerifyAssertionSig": "false",
            "spVerifyAssertionEnvelopSig": "false"
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Initiate single sign on
        self.log.info('Initiate single sign on')
        action, SAMLRequest = self.saml_util.saml_auth_url(self.master)
        self.log.info("action: {0}".format(action))
        self.log.info("SAMLRequest: {0}".format(SAMLRequest))

        SAMLResponse, cookie_string = self.get_saml_response_from_okta()
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # convert to xml
        saml_response_xml = base64.b64decode(SAMLResponse)
        saml_response_xml = saml_response_xml.decode("utf-8")
        # Define namespaces
        namespaces = {
            "saml2p": "urn:oasis:names:tc:SAML:2.0:protocol",
            "saml2": "urn:oasis:names:tc:SAML:2.0:assertion",
            "ds": "http://www.w3.org/2000/09/xmldsig#",
            "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xs": "http://www.w3.org/2001/XMLSchema",
        }
        # Load the SAML Response XML
        root = ET.fromstring(saml_response_xml)
        # Register the namespaces
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        # Find the SAML Assertion element
        saml_assertion = root.find('.//saml2:Assertion', namespaces=namespaces)
        if saml_assertion is None:
            raise ValueError("SAML Assertion not found in the XML content.")
        # Remove the SAML assertion from the SAML response
        root.remove(saml_assertion)
        # Serialize the modified XML back to a string
        invalid_saml_response_xml = '<?xml version="1.0" encoding="UTF-8"?>' + \
                                    ET.tostring(root, encoding="utf-8").decode()
        # encode back the modified SAML response
        new_SAMLResponse = invalid_saml_response_xml.encode("utf-8")
        new_SAMLResponse = base64.b64encode(new_SAMLResponse)
        # Send the oversize SAML response to Couchbase and set a session for the SSO user
        self.log.info('Send the SAML response to Couchbase and set a session for the SSO user')
        try:
            self.saml_util.direct_saml_consume(self.master, new_SAMLResponse, cookie_string)
        except Exception as e:
            self.log.info(f"SSO session creation failed as expected: {e} for am invalid SAML response")
        else:
            self.fail("SSO session creation should have failed for an invalid SAML response")

    def test_sso_with_encryption(self):
        ntonencryptionBase().setup_nton_cluster(servers=self.servers, ntonStatus="enable",
                                                clusterEncryptionLevel="all")
        self.test_sso_login()
        CbServer.use_https = True
        ntonencryptionBase().setup_nton_cluster(servers=self.servers, ntonStatus="enable",
                                                clusterEncryptionLevel="strict")
        self.test_saml_over_https()

    def test_saml_volume(self):
        """
        Send SAML requests in parallel
        """
        self.log.info("Delete current SAML settings")
        status, content, header = self.rest.delete_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        body = {
            "enabled": "true",
            "idpMetadata": self.saml_util.idp_metadata["idpMetadata"],
            "idpMetadataOrigin": "upload",
            "idpMetadataTLSCAs": self.saml_util.saml_resources["idpMetadataTLSCAs"],
            "spAssertionDupeCheck": "disabled",
            "spBaseURLScheme": "http",
            "spCertificate": self.saml_util.saml_resources["spCertificate"],
            "spKey": self.saml_util.saml_resources["spKey"]
        }
        # Enable SAML
        self.log.info("Enable SAML")
        status, content, header = self.rest.modify_saml_settings(body)
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        self.log.info("Get current SAML settings")
        status, content, header = self.rest.get_saml_settings()
        self.log.info("Status: {0} --- Content: {1} --- Header: {2}".
                      format(status, content, header))
        # Add the SSO user as an external user to Couchbase
        self.log.info("Add the SSO user as an external user to Couchbase")
        body = urllib.parse.urlencode({"roles": "admin"})
        content = self.rest.add_external_user(self.saml_user, body)
        self.log.info("Content: {0}".format(content))
        # Step 1: Initiate single sign on
        self.log.info('Initiate single sign on')
        action, SAMLRequest = self.saml_util.saml_auth_url(self.master)
        self.log.info("action: {0}".format(action))
        self.log.info("SAMLRequest: {0}".format(SAMLRequest))
        # Step 2: Redirect to the IdP
        self.log.info('Redirect to the IdP')
        state_token, cookie_string, j_session_id = self.saml_util.idp_redirect(action,
                                                                               SAMLRequest)
        self.log.info("state_token: {0}".format(state_token))
        self.log.info("cookie_string: {0}".format(cookie_string))
        self.log.info("j_session_id: {0}".format(j_session_id))
        # Step 3: SSO user authentication via the IdP
        self.log.info('SSO user authentication via the IdP')
        next_url, j_session_id = self.saml_util.idp_login(self.saml_user, self.saml_passcode,
                                                          state_token,
                                                          cookie_string, j_session_id)
        self.log.info("next_url: {0}".format(next_url))
        self.log.info("j_session_id: {0}".format(j_session_id))
        # Step 4: Get the SAML response from the IdP
        self.log.info('Get the SAML response from the IdP')
        SAMLResponse = self.saml_util.get_saml_response(next_url, cookie_string, j_session_id)
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))
        # Specify the number of parallel requests
        num_requests = self.input.param("num_requests", 110)
        # Using ThreadPool to send requests concurrently
        pool = ThreadPool(num_requests)
        # Map the function to the list of usernames
        pool.map(lambda _: self.saml_util.saml_consume_url(self.master,
                                                           cookie_string, SAMLResponse),
                 range(num_requests))
        # Close the pool and wait for the worker threads to finish
        pool.close()
        pool.join()