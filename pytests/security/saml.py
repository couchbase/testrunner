import urllib
from basetestcase import BaseTestCase
from pytests.security.saml_util import SAMLUtils
from lib.membase.api.rest_client import RestConnection


class SAMLTest(BaseTestCase):
    def setUp(self):
        super(SAMLTest, self).setUp()
        self.saml_util = SAMLUtils(self.log)
        self.rest = RestConnection(self.master)
        self.saml_user = self.input.param("saml_user", "qe.security.testing@couchbase.com")
        self.saml_passcode = self.input.param("saml_passcode", "Password@123")

    def test_sso_login(self):
        """
        Tests a single sign on login to Couchbase
        STEP 1: Enable SAML
        STEP 2: Add the SSO user as an external user to Couchbase
        STEP 3: Initiate single sign on
        Step 4: Redirect to the IdP
        Step 5: SSO user authentication via the IdP
        Step 6: Get the SAML response from the IdP
        Step 7: Send the SAML response to Couchbase and set a session for the SSO user
        Step 8: Use the session and verify SSO login
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
            "idpMetadata": self.saml_util.saml_resources["idpMetadata"],
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
            "qe.security.testing@couchbase.com", body)
        self.log.info("Content: {1}".format(status, content, header))

        # STEP 3: Initiate single sign on
        self.log.info('STEP 3: Initiate single sign on')
        action, SAMLRequest = self.saml_util.saml_auth_url(self.master)
        self.log.info("action: {0}".format(action))
        self.log.info("SAMLRequest: {0}".format(SAMLRequest))

        # Step 4: Redirect to the IdP
        self.log.info('Step 4: Redirect to the IdP')
        state_token, cookie_string, j_session_id = self.saml_util.idp_redirect(action,
                                                                               SAMLRequest)
        self.log.info("state_token: {0}".format(state_token))
        self.log.info("cookie_string: {0}".format(cookie_string))
        self.log.info("j_session_id: {0}".format(j_session_id))

        # Step 5: SSO user authentication via the IdP
        self.log.info('Step 5: SSO user authentication via the IdP')
        next_url, j_session_id = self.saml_util.idp_login(self.saml_user, self.saml_passcode,
                                                          state_token,
                                                          cookie_string, j_session_id)
        self.log.info("next_url: {0}".format(next_url))
        self.log.info("j_session_id: {0}".format(j_session_id))

        # Step 6: Get the SAML response from the IdP
        self.log.info('Step 6: Get the SAML response from the IdP')
        SAMLResponse = self.saml_util.get_saml_response(next_url, cookie_string, j_session_id)
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))

        # Step 7: Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Step 7: Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
        session_cookie_name, session_cookie_value = self.saml_util \
            .saml_consume_url(self.master, cookie_string, SAMLResponse)
        self.log.info("session_cookie_name: {0}".format(session_cookie_name))
        self.log.info("session_cookie_value: {0}".format(session_cookie_value))

        # Step 8: Use the session and verify SSO login
        self.log.info('Step 8: Use the session and verify SSO login')
        is_logged_in = self.saml_util.verify_sso_login(self.master, session_cookie_name,
                                                       session_cookie_value)
        if is_logged_in:
            self.log.info("SSO Log in Success")
        else:
            self.fail("SSO Log in Failed")

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
            "idpMetadata": self.saml_util.saml_resources["idpMetadata"],
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
            "qe.security.testing@couchbase.com", body)
        self.log.info("Content: {1}".format(status, content, header))

        # STEP 3 & 4: Initiate single sign on
        self.log.info('STEP 3: Initiate single sign on')
        try:
            self.saml_util.saml_auth_url(self.master)
        except AssertionError:
            self.log.info("Login initiation to the IdP failed as expected")
        else:
            self.fail("Login initiation to the IdP should have failed after disabling SAML")

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
        roles = ['cluster_admin', 'ro_admin', 'security_admin_local']
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
                "idpMetadata": self.saml_util.saml_resources["idpMetadata"],
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
            "idpMetadata": self.saml_util.saml_resources["idpMetadata"],
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
            "qe.security.testing@couchbase.com", body)
        self.log.info("Content: {1}".format(status, content, header))

        # STEP 3: Initiate single sign on
        self.log.info('STEP 3: Initiate single sign on')
        action, SAMLRequest = self.saml_util.saml_auth_url(self.master)
        self.log.info("action: {0}".format(action))
        self.log.info("SAMLRequest: {0}".format(SAMLRequest))

        # Step 4: Redirect to the IdP
        self.log.info('Step 4: Redirect to the IdP')
        state_token, cookie_string, j_session_id = self.saml_util.idp_redirect(action,
                                                                               SAMLRequest)
        self.log.info("state_token: {0}".format(state_token))
        self.log.info("cookie_string: {0}".format(cookie_string))
        self.log.info("j_session_id: {0}".format(j_session_id))

        # Step 5: SSO user authentication via the IdP
        self.log.info('Step 5: SSO user authentication via the IdP')
        next_url, j_session_id = self.saml_util.idp_login(self.saml_user, self.saml_passcode,
                                                          state_token,
                                                          cookie_string, j_session_id)
        self.log.info("next_url: {0}".format(next_url))
        self.log.info("j_session_id: {0}".format(j_session_id))

        # Step 6: Get the SAML response from the IdP
        self.log.info('Step 6: Get the SAML response from the IdP')
        SAMLResponse = self.saml_util.get_saml_response(next_url, cookie_string, j_session_id)
        self.log.info("SAMLResponse: {0}".format(SAMLResponse))

        # Step 7 & 8: Send the SAML response to Couchbase and set a session for the SSO user
        self.log.info('Step 7: Send the SAML response to Couchbase and set a session for the SSO '
                      'user')
        try:
            self.saml_util.saml_consume_url(self.master, cookie_string, SAMLResponse[:-1])
        except KeyError:
            self.log.info("SSO session creation failed as expected")
        else:
            self.fail("SSO session creation should have failed for incomplete SAML response")
