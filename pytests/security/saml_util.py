import re
import json
import base64
import requests
import random
import string
import urllib.parse
import six.moves.urllib.parse
from couchbase.bucket import Bucket

class SAMLUtils:
    def __init__(self, logger, okta_account):
        self.qe_server_ip = "172.23.105.178"
        self.log = logger
        self.saml_resources = None
        self.idp_metadata = None
        self.idp_id = None
        self.initialize_file_paths()
        self.app_name = "sso_try"

        # Ensure okta_account URL ends with a slash for proper URL construction
        if okta_account and not okta_account.endswith('/'):
            okta_account = okta_account + '/'
        self.okta_account = okta_account

    def get_app_name(self):
        chars = string.ascii_letters + string.digits
        return ''.join(random.choices(chars, k=10))

    def initialize_file_paths(self):
        bucket_name = "saml_tests_resources"
        ip = self.qe_server_ip
        username = "saml_test_user"
        password = "password"
        url = 'couchbase://{ip}/{name}'.format(ip=ip, name=bucket_name)
        bucket = Bucket(url, username=username, password=password)
        self.saml_resources = bucket.get("saml_resources_new").value
        self.idp_metadata = bucket.get("idpMetadata_new").value
        self.idp_id = bucket.get("idp_id_new").value

    def upload_idp_metadata(self, idp_id, idp_metadata):
        bucket_name = "saml_tests_resources"
        ip = self.qe_server_ip
        username = "saml_test_user"
        password = "password"
        url = 'couchbase://{ip}/{name}'.format(ip=ip, name=bucket_name)
        bucket = Bucket(url, username=username, password=password)
        bucket.upsert('idpMetadata_new', {'idpMetadata': idp_metadata})
        bucket.upsert('idp_id_new', {'idp_id': idp_id})

    def saml_auth_url(self, cluster):
        """
        Initiates UI/SAML login (SSO)
        If SAML is disabled, it should return an error
        If SAML is enabled, it should redirect the user to Identity Provider's (IDP) web page
        """
        url = "http://" + cluster.ip + ":8091" + "/saml/auth"
        self.log.info(url)
        resp = requests.get(url)
        assert resp
        assert resp.status_code == 200
        action = SAMLRequest = ""
        for line in resp.content.decode().split("\n"):
            if "<form id=\"saml-req-form\" method=\"post\" action=" in line:
                action = line[47:-2]
            elif "<input type=\"hidden\" name=\"SAMLRequest\" value=" in line:
                SAMLRequest = line[47:-4]
        return action, SAMLRequest

    def idp_redirect(self, action, SAMLRequest):
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        resp = requests.post(action,
                             data="SAMLRequest={0}&RelayState={1}".format(SAMLRequest, ""),
                             headers=header,
                             timeout=300, verify=False, allow_redirects=False)
        self.log.info("Redirecting to IdP...")
        self.log.info("Response: {0}".format(resp.content))
        assert resp.status_code == 200
        # Extract state token more robustly
        content = resp.content.decode()
        state_token = ""

        # Look for: var stateToken = 'TOKEN';

        pattern = r"var stateToken = '([^']+)'"
        match = re.search(pattern, content)
        if match:
            state_token = match.group(1)
        else:
            # Fallback: look for stateToken: 'TOKEN'
            pattern = r"stateToken:\s*'([^']+)'"
            match = re.search(pattern, content)
            if match:
                state_token = match.group(1)

        if state_token:
            self.log.info(f"✅ Successfully extracted state token (length: {len(state_token)})")
            self.log.info(f"State token starts with: {state_token[:20]}...")
        else:
            self.log.error("❌ Failed to extract state token from response")
            self.log.error("Searching for stateToken patterns in response...")
            if "var stateToken =" in content:
                self.log.error("Found 'var stateToken =' in response")
            if "stateToken:" in content:
                self.log.error("Found 'stateToken:' in response")
            if "stateToken=" in content:
                self.log.error("Found 'stateToken=' in response")
            # Show first 1000 chars of response for debugging
            self.log.error(f"Response preview: {content[:1000]}")
            raise Exception("Could not extract state token from Okta response")
        cookie_dict = {}
        cookie_string = ""
        for i in resp.cookies:
            cookie_dict[i.name] = i.value
            cookie_string = cookie_string + i.name + "=" + i.value + "; "
        cookie_string = cookie_string[:-2]
        j_session_id = cookie_dict["JSESSIONID"]
        return state_token, cookie_string, j_session_id

    def idp_login(self, identifier, passcode, state_token, cookie_string, j_session_id):
        """
        Authentication of an SSO user via a IdP
        """
        # Use IDX API with proper stateHandle - it's required
        url = f"{self.okta_account}idp/idx/identify"
        body = {"identifier": identifier,
                "credentials": {"passcode": passcode},
                "stateHandle": state_token}

        self.log.info(f"Attempting IDX API authentication for user: {identifier}")
        self.log.info(f"State token length: {len(state_token) if state_token else 0}")
        self.log.info(f"State token starts with: {state_token[:10] if state_token else 'None'}...")
        self.log.info(f"Using state token: {state_token[:50]}..." if state_token and len(
            state_token) > 50 else f"Using state token: {state_token}")

        # Validate state token format
        if not state_token:
            raise Exception("State token is empty - check state token parsing in idp_redirect")
        if state_token.startswith("Token="):
            self.log.warning("State token starts with 'Token=' - removing prefix")
            state_token = state_token[6:]  # Remove "Token=" prefix
            body["stateHandle"] = state_token

        self.log.info(f"Cookie string: {cookie_string}")
        self.log.info(f"Request body: {json.dumps(body)}")

        header = {'Cookie': cookie_string,
                  'Content-Type': "application/json",
                  'Connection': "keep-alive",
                  'Accept-Encoding': 'gzip, deflate, br',
                  'Accept': '*/*',
                  'JSESSIONID': j_session_id
                  }
        resp = requests.post(url, data=json.dumps(body), headers=header)
        self.log.info("Logging in to IdP...")
        self.log.info("Response: {0}".format(resp.content))
        self.log.info("Response status code: {0}".format(resp.status_code))
        self.log.info("Response headers: {0}".format(resp.headers))

        response_text = resp.content.decode()

        # Check if we got a JSON error response first (IDX API)
        try:
            error_response = json.loads(response_text)
            if error_response.get("messages"):
                messages = error_response["messages"]["value"]
                for msg in messages:
                    if msg.get("class") == "ERROR":
                        error_text = msg.get("message", "Unknown error")
                        self.log.error(f"IDX API error: {error_text}")

                        if "session.expired" in error_text or "expired" in error_text.lower():
                            self.log.error(
                                "Session expired error - this indicates the state token is invalid or expired")
                            self.log.error("Possible causes:")
                            self.log.error("1. State token parsing issue")
                            self.log.error("2. Too much time elapsed between token extraction and use")
                            self.log.error("3. State token was corrupted during parsing")

                        raise Exception(f"Authentication failed: {error_text}")
        except json.JSONDecodeError:
            # Not JSON, probably HTML response - continue with HTML parsing
            pass

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {response_text}"

        # Parse HTML response for redirect URL
        if "success-redirect" not in response_text:
            self.log.error("Response doesn't contain expected success-redirect")
            self.log.error(f"Response content: {response_text[:500]}...")
            raise Exception(f"Unexpected response format - no success redirect found")

        try:
            next_url = response_text[response_text.index("success-redirect\",\"href\":\"") + 26:
                                     response_text.index("success-redirect\",\"href\":\"") + 134]

            # Extract JSESSIONID from response headers
            if "set-cookie" in resp.headers:
                j_session_id = resp.headers["set-cookie"][resp.headers["set-cookie"].index("JSESSIONID=") +
                                                          11: resp.headers["set-cookie"].index(
                    "JSESSIONID=") + 43]

            self.log.info(f"Extracted next_url: {next_url}")
            self.log.info(f"Extracted j_session_id: {j_session_id}")

            return next_url, j_session_id
        except (ValueError, KeyError) as e:
            self.log.error(f"Failed to parse response: {e}")
            self.log.error(f"Response content: {response_text}")
            raise Exception(f"Failed to extract redirect URL from response")

    def get_saml_response(self, next_url, cookie_string, j_session_id):
        """
        A SAML Response is sent by the Identity Provider to the Service Provider(Couchbase),
        if the user succeeded in the authentication process
        """
        header = {
            'Cookie': cookie_string,
            'Content-Type': "application/json",
            'Connection': "keep-alive",
            'Accept': '*/*',
            'JSESSIONID': j_session_id
        }
        resp = requests.get(next_url, headers=header)
        SAMLResponse = ""
        for line in resp.content.decode().split("\n"):
            if "<input name=\"SAMLResponse\" type=\"hidden\" value=\"" in line:
                SAMLResponse = line[52:-3]
        SAMLResponse = SAMLResponse.replace("&#x2b;", "+")
        SAMLResponse = SAMLResponse.replace("&#x3d;", "=")
        return SAMLResponse

    def saml_consume_url(self, cluster, cookie_string, SAMLResponse, https=False):
        """
        Sends SAML assertion to couchbase.
        Usually contains the SAML auth response.
        Ns server validates the response and creates a cookie for the user,
        if SAML assertion is valid.
        """
        url = "http://" + cluster.ip + ":8091" + "/saml/consume"
        if https:
            url = "https://" + cluster.ip + ":18091" + "/saml/consume"
        header = {
            'Cookie': cookie_string,
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        SAMLResponse = six.moves.urllib.parse.quote(SAMLResponse)
        resp = requests.post(url,
                             data="SAMLResponse={0}".format(SAMLResponse),
                             headers=header,
                             timeout=300, verify=False, allow_redirects=False)
        try:
            error_msg = self.get_saml_error_message(cluster, requests, resp, header)
            self.log.error("*****+++++******")
            self.log.error("SAML error message received: {0}".format(error_msg))
            self.log.error("*****+++++******")
            raise Exception(f"SAML authentication failed: {error_msg}")
        except json.decoder.JSONDecodeError:
            self.log.info("No SAML error message found")
        except KeyError:
            self.log.error("SAML error occurred but could not extract error message")
            raise Exception("SAML authentication failed - check server logs for details")

        if "Set-Cookie" not in resp.headers:
            self.log.error(f"❌ No Set-Cookie header in response. Status: {resp.status_code}")
            self.log.error(f"Response headers: {dict(resp.headers)}")
            raise Exception(f"SAML consume failed - no session cookie received. Status: {resp.status_code}")

        session_cookie = resp.headers["Set-Cookie"].strip().split(";")[0].split("=")
        return session_cookie[0], session_cookie[1]

    def verify_sso_login(self, cluster, session_cookie_name, session_cookie_value):
        """
        Use the cookie, created by ns server on validation of SAML assertion, to access
        pools/default
        """
        cookies = {session_cookie_name: session_cookie_value}
        headers = {
            'Host': cluster.ip + ":8091",
            'ns-server-ui': 'yes',
            'Accept': 'application/json,text/plain, */*'
        }
        response = requests.get("http://" + cluster.ip + ":8091" + "/pools/default",
                                cookies=cookies,
                                headers=headers, verify=False)
        if response:
            return True
        else:
            return False

    def sso_user_permission_details(self, cluster, session_cookie_name, session_cookie_value):
        """
        /whoami returns details about the user
        """
        cookies = {session_cookie_name: session_cookie_value}
        headers = {
            'Host': cluster.ip + ":8091",
            'ns-server-ui': 'yes',
            'Accept': 'application/json,text/plain, */*'
        }
        response = requests.get("http://" + cluster.ip + ":8091" + "/whoami",
                                cookies=cookies,
                                headers=headers, verify=False)
        return response

    def saml_deauth(self, cluster):
        """
        GET /saml/deauth
        Initiates UI/SAML logout (SLO) (called by couchbase UI).
        If SAML is disabled, it should return an error.
        If SAML is enabled, it performs local logout, and then redirects the user to IDP endpoint
        with SAML logout request message, which performs the logout at the IDP side.
        IDP is supposed to redirect the user back (to GET or POST /saml/logout).
        """
        url = "http://" + cluster.ip + ":8091" + "/saml/deauth"
        self.log.info(url)
        resp = requests.get(url)
        assert resp
        assert resp.status_code == 200
        action = SAMLRequest = ""
        for line in resp.content.decode().split("\n"):
            if "<form id=\"saml-req-form\" method=\"post\" action=" in line:
                action = line[47:-2]
            elif "<input type=\"hidden\" name=\"SAMLRequest\" value=" in line:
                SAMLRequest = line[47:-4]
        return action, SAMLRequest

    def saml_logout(self, cluster, SAMLResponse):
        """
        Sends either logout response or logout request to couchbase
        Usually IDP redirects user's browser to that endpoint
        If logout is initiated by couchbase (by GET /saml/deauth),
        /saml/logout will contain a SAML logout response.
        If logout is initiated by IDP, /saml/logout will contain the SAML logout request.
        Couchbase is supposed to call IdP's logout binding with a logout response then.
        """
        url = "http://" + cluster.ip + ":8091" + "/saml/logout"
        header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        SAMLResponse = six.moves.urllib.parse.quote(SAMLResponse)
        requests.post(url,
                      data="SAMLResponse={0}".format(SAMLResponse),
                      headers=header,
                      timeout=300, verify=False, allow_redirects=False)

    def saml_metadata(self, cluster):
        """
        Returns Service Provider's (SP) metadata in XML format.
        """
        url = "http://" + cluster.ip + ":8091" + "/saml/metadata"
        self.log.info(url)
        resp = requests.get(url)
        assert resp
        assert resp.status_code == 200
        return resp.content

    def cb_auth_on_behalf_of(self, cluster, sso_user, non_sso_user_username, non_sso_password):
        """
        Do on-behalf-of authentication
        """
        sso_user = base64.b64encode('{}:{}'.format(sso_user, "external").encode()).decode()
        non_sso_user = base64.b64encode('{}:{}'
                                        .format(non_sso_user_username, non_sso_password)
                                        .encode()).decode()
        url = "http://" + cluster.ip + ":8091" + "/whoami"
        header = {
            'cb-on-behalf-of': sso_user,
            'Authorization': 'Basic ' + non_sso_user
        }
        response = requests.get(url, headers=header, timeout=300, verify=False,
                                allow_redirects=False)
        response = response.json()
        assert [{'role': 'admin'}] == response['roles']
        assert sso_user == response['id']
        assert 'external' == response['domain']

    def get_saml_error_message(self, cluster, session, consume_resp, consume_header):
        """
        Get the SAML error message
        Note: can be accessed only once
        """
        redirect_path = consume_resp.headers['Location']
        # Define a regular expression pattern to match the message ID
        pattern = re.compile(r"samlErrorMsgId=([a-fA-F0-9-]+)")
        # Use the pattern to search for a match in the URL
        match = pattern.search(redirect_path)
        # If a match is found, extract the message ID
        error_id = ""
        if match:
            error_id = match.group(1)
        else:
            self.log.info("ERROR ID not found")
        # extracting error msg from server
        response = session.get("http://" + cluster.ip + ":8091" + '/saml/error',
                               headers=consume_header,
                               params={'id': error_id})
        error_msg = response.json()['error']
        return error_msg

    def delete_okta_applications(self, okta_token):
        self.log.info("Deleting OKTA Application")
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }
        resp = requests.get(self.okta_account + "api/v1/apps",
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        app_list = json.loads(resp.content.decode())
        for app in app_list:
            app_label = app["label"]
            if app_label == self.app_name:
                resp = requests.post(
                    self.okta_account + "api/v1/apps/" + app["id"] + "/lifecycle/deactivate",
                    headers=header,
                    timeout=300, verify=False, allow_redirects=False)
                assert resp.status_code == 200
                resp = requests.delete(
                    self.okta_account + "api/v1/apps/" + app["id"],
                    headers=header,
                    timeout=300, verify=False, allow_redirects=False)
                assert resp.status_code == 204

    def create_okta_application(self, okta_token, cluster):
        self.log.info("Creating OKTA Application")
        header = {
            'Content-Type': 'application/json',
            'Authorization': 'SSWS ' + okta_token
        }
        body = {
            "label": self.app_name,
            "accessibility": {
                "selfService": False,
                "errorRedirectUrl": None,
                "loginRedirectUrl": None
            },
            "visibility": {
                "autoSubmitToolbar": False,
                "hide": {
                    "iOS": False,
                    "web": False
                }
            },
            "features": [],
            "signOnMode": "SAML_2_0",
            "credentials": {
                "userNameTemplate": {
                    "template": "${source.login}",
                    "type": "BUILT_IN"
                },
                "signing": {}
            },
            "settings": {
                "app": {},
                "notifications": {
                    "vpn": {
                        "network": {
                            "connection": "DISABLED"
                        },
                        "message": None,
                        "helpUrl": None
                    }
                },
                "manualProvisioning": False,
                "implicitAssignment": False,
                "signOn": {
                    "defaultRelayState": "",
                    "ssoAcsUrl": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "idpIssuer": "http://www.okta.com/${org.externalKey}",
                    "audience": "http://" + cluster.ip + ":8091" + "/saml/metadata",
                    "recipient": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "destination": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "subjectNameIdTemplate": "${user.userName}",
                    "subjectNameIdFormat": "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent",
                    "responseSigned": True,
                    "assertionSigned": True,
                    "signatureAlgorithm": "RSA_SHA256",
                    "digestAlgorithm": "SHA256",
                    "honorForceAuthn": True,
                    "authnContextClassRef": "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport",
                    "spIssuer": None,
                    "requestCompressed": False,
                    "attributeStatements": [
                        {
                            "type": "EXPRESSION",
                            "name": "email",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.email"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "given_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.firstName"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "family_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.lastName"]
                        },
                        {
                            "type": "GROUP",
                            "name": "groups",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        },
                        {
                            "type": "GROUP",
                            "name": "roles",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        }
                    ],
                    "inlineHooks": [],
                    "allowMultipleAcsEndpoints": False,
                    "acsEndpoints": [],
                    "samlSignedRequestEnabled": False,
                    "slo": {
                        "enabled": False
                    },
                }
            }
        }
        resp = requests.post(self.okta_account + "api/v1/apps",
                             data=json.dumps(body),
                             headers=header,
                             timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        resp_content = json.loads(resp.content.decode())
        okta_app_id = resp_content["id"]
        idp_metadata_url = resp_content["_links"]["metadata"]["href"]
        resp = requests.get(idp_metadata_url,
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        idp_metadata = resp.content.decode()
        return okta_app_id, idp_metadata

    def assign_user(self, okta_token, okta_app_id):

        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }

        user_email = "samridh.anand@couchbase.com"

        # First, find the user by email to get their Okta user ID
        self.log.info(f"Looking up user by email: {user_email}")
        search_url = f"{self.okta_account}api/v1/users?q={user_email}"
        search_resp = requests.get(search_url, headers=header, timeout=300, verify=False)

        if search_resp.status_code != 200:
            self.log.error(f"Failed to search for user: {search_resp.status_code} - {search_resp.text}")
            raise Exception(f"User lookup failed: {search_resp.text}")

        users = search_resp.json()
        if not users:
            self.log.error(f"User {user_email} not found in Okta")
            # Try to create the user first
            self.log.info(f"Attempting to create user: {user_email}")
            create_user_body = {
                "profile": {
                    "firstName": "Samridh",
                    "lastName": "Anand",
                    "email": user_email,
                    "login": user_email
                }
            }
            create_resp = requests.post(
                f"{self.okta_account}api/v1/users?activate=true",
                data=json.dumps(create_user_body),
                headers=header,
                timeout=300, verify=False
            )
            if create_resp.status_code in [200, 201]:
                self.log.info("User created successfully")
                user_id = create_resp.json()["id"]
            else:
                self.log.error(f"Failed to create user: {create_resp.status_code} - {create_resp.text}")
                raise Exception(f"User creation failed: {create_resp.text}")
        else:
            user_id = users[0]["id"]
            self.log.info(f"Found user ID: {user_id}")

        # Now assign the user to the application
        self.log.info(f"Assigning user {user_id} to application {okta_app_id}")
        body = {"id": user_id,
                "scope": "USER",
                "credentials":
                    {"userName": user_email}}
        resp = requests.post(
            self.okta_account + "api/v1/apps/" + okta_app_id + "/users",
            data=json.dumps(body),
            headers=header,
            timeout=300, verify=False, allow_redirects=False)

        if resp.status_code == 200:
            self.log.info("User assigned to application successfully")
        else:
            self.log.error(f"Failed to assign user: {resp.status_code} - {resp.text}")
            raise Exception(f"User assignment failed: {resp.text}")

    def update_okta_application(self, okta_token, cluster):
        self.log.info("Updating OKTA Application")
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }
        body = {
            "label": self.app_name,
            "accessibility": {
                "selfService": False,
                "errorRedirectUrl": None,
                "loginRedirectUrl": None
            },
            "visibility": {
                "autoSubmitToolbar": False,
                "hide": {
                    "iOS": False,
                    "web": False
                }
            },
            "features": [],
            "signOnMode": "SAML_2_0",
            "credentials": {
                "userNameTemplate": {
                    "template": "${source.login}",
                    "type": "BUILT_IN"
                },
                "signing": {}
            },
            "settings": {
                "app": {},
                "notifications": {
                    "vpn": {
                        "network": {
                            "connection": "DISABLED"
                        },
                        "message": None,
                        "helpUrl": None
                    }
                },
                "manualProvisioning": False,
                "implicitAssignment": False,
                "signOn": {
                    "defaultRelayState": "",
                    "ssoAcsUrl": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "idpIssuer": "http://www.okta.com/${org.externalKey}",
                    "audience": "http://" + cluster.ip + ":8091" + "/saml/metadata",
                    "recipient": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "destination": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "subjectNameIdTemplate": "${user.userName}",
                    "subjectNameIdFormat": "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent",
                    "responseSigned": True,
                    "assertionSigned": False,
                    "signatureAlgorithm": "RSA_SHA256",
                    "digestAlgorithm": "SHA256",
                    "honorForceAuthn": True,
                    "authnContextClassRef": "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport",
                    "spIssuer": None,
                    "requestCompressed": False,
                    "attributeStatements": [
                        {
                            "type": "EXPRESSION",
                            "name": "email",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.email"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "given_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.firstName"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "family_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.lastName"]
                        },
                        {
                            "type": "GROUP",
                            "name": "groups",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        },
                        {
                            "type": "GROUP",
                            "name": "roles",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        }
                    ],
                    "inlineHooks": [],
                    "allowMultipleAcsEndpoints": False,
                    "acsEndpoints": [],
                    "samlSignedRequestEnabled": False,
                    "slo": {
                        "enabled": False
                    }
                }
            }
        }
        resp = requests.put(self.okta_account + "api/v1/apps/" + self.idp_id["idp_id"],
                            data=json.dumps(body),
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        resp_content = json.loads(resp.content.decode())
        okta_app_id = resp_content["id"]
        idp_metadata_url = resp_content["_links"]["metadata"]["href"]
        resp = requests.get(idp_metadata_url,
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        idp_metadata = resp.content.decode()
        return okta_app_id, idp_metadata

    def reset_okta_application(self, cluster, okta_token):
        self.log.info("Reset OKTA Application")
        header = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Authorization': 'SSWS ' + okta_token
        }
        body = {
            "label": self.app_name,
            "accessibility": {
                "selfService": False,
                "errorRedirectUrl": None,
                "loginRedirectUrl": None
            },
            "visibility": {
                "autoSubmitToolbar": False,
                "hide": {
                    "iOS": False,
                    "web": False
                }
            },
            "features": [],
            "signOnMode": "SAML_2_0",
            "credentials": {
                "userNameTemplate": {
                    "template": "${source.login}",
                    "type": "BUILT_IN"
                },
                "signing": {}
            },
            "settings": {
                "app": {},
                "notifications": {
                    "vpn": {
                        "network": {
                            "connection": "DISABLED"
                        },
                        "message": None,
                        "helpUrl": None
                    }
                },
                "manualProvisioning": False,
                "implicitAssignment": False,
                "signOn": {
                    "defaultRelayState": "",
                    "ssoAcsUrl": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "idpIssuer": "http://www.okta.com/${org.externalKey}",
                    "audience": "http://" + cluster.ip + ":8091" + "/saml/metadata",
                    "recipient": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "destination": "http://" + cluster.ip + ":8091" + "/saml/consume",
                    "subjectNameIdTemplate": "${user.userName}",
                    "subjectNameIdFormat": "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent",
                    "responseSigned": True,
                    "assertionSigned": True,
                    "signatureAlgorithm": "RSA_SHA256",
                    "digestAlgorithm": "SHA256",
                    "honorForceAuthn": True,
                    "authnContextClassRef": "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport",
                    "spIssuer": None,
                    "requestCompressed": False,
                    "attributeStatements": [
                        {
                            "type": "EXPRESSION",
                            "name": "email",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.email"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "given_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.firstName"]
                        },
                        {
                            "type": "EXPRESSION",
                            "name": "family_name",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "values": ["user.lastName"]
                        },
                        {
                            "type": "GROUP",
                            "name": "groups",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        },
                        {
                            "type": "GROUP",
                            "name": "roles",
                            "namespace": "urn:oasis:names:tc:SAML:2.0:attrname-format:unspecified",
                            "filterType": "REGEX",
                            "filterValue": ".*"
                        }
                    ],
                    "inlineHooks": [],
                    "allowMultipleAcsEndpoints": False,
                    "acsEndpoints": [],
                    "samlSignedRequestEnabled": False,
                    "slo": {
                        "enabled": False
                    }
                }
            }
        }
        resp = requests.put(self.okta_account + "api/v1/apps/" + self.idp_id["idp_id"],
                            data=json.dumps(body),
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        resp_content = json.loads(resp.content.decode())
        okta_app_id = resp_content["id"]
        idp_metadata_url = resp_content["_links"]["metadata"]["href"]
        resp = requests.get(idp_metadata_url,
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)
        assert resp.status_code == 200
        idp_metadata = resp.content.decode()
        return okta_app_id, idp_metadata

    def direct_saml_auth(self, cluster):
        """
        Direct browser-style SAML auth initiation
        Follows the exact browser flow: GET /saml/auth with proper headers and cookies
        """
        url = f"http://{cluster.ip}:8091/saml/auth"

        # Use browser-like headers
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Referer': f'http://{cluster.ip}:8091/ui/index.html',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        self.log.info(f"Direct SAML auth to: {url}")
        resp = requests.get(url, headers=headers, allow_redirects=False)

        self.log.info(f"Response status: {resp.status_code}")
        self.log.info(f"Response headers: {dict(resp.headers)}")

        if resp.status_code == 302:
            # Follow redirect to get to Okta
            redirect_location = resp.headers.get('Location', '')
            self.log.info(f"Redirected to: {redirect_location}")

            # Extract cookies from initial response
            cookies = {}
            cookie_string = ""
            if resp.cookies:
                for cookie in resp.cookies:
                    cookies[cookie.name] = cookie.value
                    cookie_string += f"{cookie.name}={cookie.value}; "
                cookie_string = cookie_string.rstrip("; ")

            return redirect_location, cookie_string, cookies

        elif resp.status_code == 200:
            # Parse the HTML response to extract the form action and SAMLRequest
            content = resp.content.decode()

            action = ""
            saml_request = ""

            # Extract form action
            action_match = re.search(r'<form[^>]*action="([^"]*)"', content)
            if action_match:
                action = action_match.group(1)

            # Extract SAMLRequest value
            saml_match = re.search(r'<input[^>]*name="SAMLRequest"[^>]*value="([^"]*)"', content)
            if saml_match:
                saml_request = saml_match.group(1)

            # Extract cookies
            cookies = {}
            cookie_string = ""
            if resp.cookies:
                for cookie in resp.cookies:
                    cookies[cookie.name] = cookie.value
                    cookie_string += f"{cookie.name}={cookie.value}; "
                cookie_string = cookie_string.rstrip("; ")

            self.log.info(f"Extracted action: {action}")
            self.log.info(f"Extracted SAMLRequest: {saml_request[:100]}...")

            return action, saml_request, cookie_string, cookies
        else:
            raise Exception(f"Unexpected response status: {resp.status_code} - {resp.text}")

    def direct_idp_authenticate(self, action_url, saml_request, username, password, cluster_ip, initial_cookies=""):
        """
        Direct browser-style IdP authentication
        Posts SAMLRequest to Okta and handles the full authentication flow to get SAMLResponse
        """
        self.log.info(f"Direct IdP authentication to: {action_url}")
        self.log.info(f"Username: {username}")

        # First, post the SAML request to Okta
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': f'http://{cluster_ip}:8091',
            'priority': 'u=0, i',
            'referer': f'http://{cluster_ip}:8091/',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-platform-version': '"15.6.1"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'cross-site',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        # Add initial cookies if provided
        if initial_cookies:
            headers['Cookie'] = initial_cookies

        # Prepare form data
        form_data = {
            'SAMLRequest': saml_request,
            'RelayState': ''
        }

        self.log.info("Posting SAML request to IdP...")
        resp = requests.post(action_url, data=form_data, headers=headers, allow_redirects=False)

        self.log.info(f"IdP response status: {resp.status_code}")

        if resp.status_code == 200:
            # We got the login page, now need to authenticate
            content = resp.text

            # Extract any state tokens or session info from the page
            # Look for authentication form or direct SAML response
            if 'SAMLResponse' in content:
                # Direct SAML response - extract it
                saml_response_match = re.search(r'<input[^>]*name="SAMLResponse"[^>]*value="([^"]*)"', content)
                if saml_response_match:
                    saml_response = saml_response_match.group(1)
                    # URL decode the response
                    saml_response = urllib.parse.unquote(saml_response)
                    self.log.info("✅ Got SAML response directly")
                    return saml_response

            # Extract cookies from response for authentication
            auth_cookies = {}
            cookie_string = initial_cookies
            if resp.cookies:
                for cookie in resp.cookies:
                    auth_cookies[cookie.name] = cookie.value
                    if cookie_string:
                        cookie_string += f"; {cookie.name}={cookie.value}"
                    else:
                        cookie_string = f"{cookie.name}={cookie.value}"

            # Now try to authenticate using the Okta Classic API
            return self._authenticate_with_okta_classic(username, password, cookie_string, content, action_url)

        elif resp.status_code in [302, 303]:
            # Follow redirect
            redirect_location = resp.headers.get('Location', '')
            self.log.info(f"Following redirect to: {redirect_location}")

            # Update cookies from redirect
            cookie_string = initial_cookies
            if resp.cookies:
                for cookie in resp.cookies:
                    if cookie_string:
                        cookie_string += f"; {cookie.name}={cookie.value}"
                    else:
                        cookie_string = f"{cookie.name}={cookie.value}"

            # Follow the redirect
            redirect_headers = headers.copy()
            redirect_headers['Cookie'] = cookie_string
            redirect_headers['referer'] = action_url

            redirect_resp = requests.get(redirect_location, headers=redirect_headers, allow_redirects=False)

            if redirect_resp.status_code == 200 and 'SAMLResponse' in redirect_resp.text:
                # Extract SAML response
                saml_response_match = re.search(r'<input[^>]*name="SAMLResponse"[^>]*value="([^"]*)"',
                                                redirect_resp.text)
                if saml_response_match:
                    saml_response = saml_response_match.group(1)
                    saml_response = urllib.parse.unquote(saml_response)
                    self.log.info("✅ Got SAML response from redirect")
                    return saml_response

            # If no direct SAML response, try authentication
            return self._authenticate_with_okta_classic(username, password, cookie_string, redirect_resp.text,
                                                        action_url)

        else:
            raise Exception(f"IdP request failed: {resp.status_code} - {resp.text}")

    def _authenticate_with_okta_classic(self, username, password, cookie_string, page_content, app_url=None):
        """
        Authenticate using Okta's classic authentication flow
        """
        self.log.info("Attempting Okta classic authentication...")

        # Try the classic authn API
        auth_url = f"{self.okta_account}api/v1/authn"

        auth_body = {
            "username": username,
            "password": password,
            "options": {
                "multiOptionalFactorEnroll": False,
                "warnBeforePasswordExpired": False
            }
        }

        auth_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Cookie': cookie_string,
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
        auth_resp = requests.post(auth_url, data=json.dumps(auth_body), headers=auth_headers)

        self.log.info(f"Auth response status: {auth_resp.status_code}")

        if auth_resp.status_code == 200:
            auth_data = auth_resp.json()
            self.log.info(f"Auth status: {auth_data.get('status')}")

            if auth_data.get('status') == 'SUCCESS':
                session_token = auth_data.get('sessionToken')
                if session_token:
                    # Now get the SAML response using the session token
                    return self._get_saml_response_with_session_token(session_token, cookie_string, app_url)

        # If classic API fails, try alternative approaches
        raise Exception("Authentication failed - unable to get SAML response")

    def _get_saml_response_with_session_token(self, session_token, cookie_string, app_url=None):
        """
        Get SAML response using session token
        """
        # Use provided app URL or fallback to hardcoded one
        if not app_url:
            app_url = f"{self.okta_account}app/integrator-2810815_ssoapp_2/exkv5dd7sul27aH7b697/sso/saml"

        self.log.info(f"Using app URL: {app_url}")

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cookie': cookie_string,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        # Add session token as parameter
        params = {'sessionToken': session_token}

        self.log.info("Getting SAML response with session token...")
        resp = requests.get(app_url, headers=headers, params=params, allow_redirects=False)

        self.log.info(f"SAML app response status: {resp.status_code}")
        self.log.info(f"Response headers: {dict(resp.headers)}")

        if resp.status_code == 200:
            content = resp.text
            self.log.info(f"Response content length: {len(content)}")
            self.log.info(f"Content preview: {content[:500]}...")

            # Check if SAMLResponse is in the content
            if 'SAMLResponse' in content:
                self.log.info("Found SAMLResponse in content, attempting to extract...")

                # Try multiple patterns to extract SAML response
                patterns = [
                    r'<input[^>]*name=["\']?SAMLResponse["\']?[^>]*value=["\']([^"\']*)["\'][^>]*>',
                    r'<input[^>]*value=["\']([^"\']*)["\'][^>]*name=["\']?SAMLResponse["\']?[^>]*>',
                    r'name=["\']SAMLResponse["\'][^>]*value=["\']([^"\']*)["\']',
                    r'value=["\']([^"\']*)["\'][^>]*name=["\']SAMLResponse["\']',
                    r'"SAMLResponse"[^"]*"([^"]*)"',
                    r'SAMLResponse["\'\s]*[=:]["\'\s]*([^"\'\\s><]+)'
                ]

                for i, pattern in enumerate(patterns):
                    match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                    if match:
                        saml_response = match.group(1)
                        if saml_response and len(saml_response) > 10:  # Basic validation
                            saml_response = urllib.parse.unquote(saml_response)

                            # Decode HTML entities (same as original get_saml_response method)
                            saml_response = saml_response.replace("&#x2b;", "+")
                            saml_response = saml_response.replace("&#x3d;", "=")

                            self.log.info(f"✅ Got SAML response with pattern {i}: {saml_response[:100]}...")
                            return saml_response
                        else:
                            self.log.warning(f"Pattern {i} matched but result too short: '{saml_response}'")
                            raise Exception(f"Pattern matched but result too short")

                # If no patterns worked, log more details
                self.log.error("No SAML response extraction patterns worked")
                self.log.error(f"Full content: {content}")
                raise Exception("No SAML response could be extracted using any available patterns")
            else:
                self.log.warning("SAMLResponse not found in content")
                self.log.warning(f"Content contains: {[word for word in content.split() if 'saml' in word.lower()]}")
                raise Exception("SAMLResponse not found in the response content")

        elif resp.status_code in [302, 303]:
            # Follow redirect
            redirect_location = resp.headers.get('Location', '')
            self.log.info(f"Following SAML redirect to: {redirect_location}")

            redirect_resp = requests.get(redirect_location, headers=headers, allow_redirects=False)
            self.log.info(f"Redirect response status: {redirect_resp.status_code}")

            if redirect_resp.status_code == 200:
                content = redirect_resp.text
                self.log.info(f"Redirect content length: {len(content)}")

                if 'SAMLResponse' in content:
                    patterns = [
                        r'<input[^>]*name=["\']?SAMLResponse["\']?[^>]*value=["\']([^"\']*)["\'][^>]*>',
                        r'<input[^>]*value=["\']([^"\']*)["\'][^>]*name=["\']?SAMLResponse["\']?[^>]*>',
                        r'name=["\']SAMLResponse["\'][^>]*value=["\']([^"\']*)["\']',
                        r'value=["\']([^"\']*)["\'][^>]*name=["\']SAMLResponse["\']'
                    ]

                    for i, pattern in enumerate(patterns):
                        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                        if match:
                            saml_response = match.group(1)
                            if saml_response and len(saml_response) > 10:
                                saml_response = urllib.parse.unquote(saml_response)
                                self.log.info(f"✅ Got SAML response from redirect with pattern {i}")
                                return saml_response

        raise Exception(
            f"Failed to get SAML response with session token. Status: {resp.status_code}, Content preview: {content[:200] if 'content' in locals() else 'No content'}")

    def direct_saml_consume(self, cluster, saml_response, cookies=""):
        """
        Direct browser-style SAML consume
        Posts SAMLResponse to /saml/consume and gets session cookie
        """
        url = f"http://{cluster.ip}:8091/saml/consume"

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'null',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        # Add cookies if provided
        if cookies:
            headers['Cookie'] = cookies

        # Prepare form data - SAMLResponse should already be URL encoded
        form_data = {
            'SAMLResponse': saml_response,
            'RelayState': ''
        }

        self.log.info(f"Posting SAML response to: {url}")
        resp = requests.post(url, data=form_data, headers=headers, allow_redirects=False, verify=False)

        self.log.info(f"Consume response status: {resp.status_code}")
        self.log.info(f"Consume response headers: {dict(resp.headers)}")

        if resp.status_code in [302, 303]:
            if 'Set-Cookie' in resp.headers:
                # Extract session cookie
                cookie_header = resp.headers['Set-Cookie']

                # Parse the session cookie
                cookie_match = re.search(r'([^=]+)=([^;]+)', cookie_header)
                if cookie_match:
                    session_cookie_name = cookie_match.group(1)
                    session_cookie_value = cookie_match.group(2)

                    self.log.info(f"✅ Got session cookie: {session_cookie_name}={session_cookie_value}")
                return session_cookie_name, session_cookie_value

            elif 'Location' in resp.headers and 'samlErrorMsgId' in resp.headers['Location']:
                # SAML validation error - extract error message
                error_message = self.get_saml_error_message(cluster, requests, resp, headers)
                raise Exception(f"SAML authentication failed. The error message in SAML consume: {error_message}")

        elif resp.status_code == 200:
            # Check if we got cookies in a different way
            if resp.cookies:
                for cookie in resp.cookies:
                    self.log.info(f"✅ Got session cookie: {cookie.name}={cookie.value}")
                    return cookie.name, cookie.value

        raise Exception(f"Failed to get session cookie from consume response: {resp.status_code} - {resp.text}")

    def verify_direct_login(self, cluster, session_cookie_name, session_cookie_value):
        """
        Direct browser-style login verification
        GET / with session cookie to verify authentication
        """
        url = f"http://{cluster.ip}:8091/"

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': f'{session_cookie_name}={session_cookie_value}',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        self.log.info(f"Verifying login to: {url}")
        resp = requests.get(url, headers=headers, verify=False)

        self.log.info(f"Verification response status: {resp.status_code}")

        if resp.status_code == 200:
            # Check if we're logged in (not redirected to login page)
            content = resp.text.lower()
            if 'login' not in content or 'dashboard' in content or 'cluster' in content:
                self.log.info("✅ Login verification successful")
                return True

        self.log.error("❌ Login verification failed")
        return False

    def update_okta_app_ip(self, okta_token, okta_app_id, new_cluster_ip):
        """
        Simple function to just update the IP addresses in an existing Okta SAML app
        This avoids recreating the app and dealing with policies
        """
        self.log.info(f"Updating Okta app {okta_app_id} to use IP: {new_cluster_ip}")
        self.log.info(f"🔗 Using Okta account URL: {self.okta_account}")

        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "SSWS " + okta_token
        }
        # Step 1: Get current app configuration
        api_url = self.okta_account + f"api/v1/apps/{okta_app_id}"
        self.log.info(f"🌐 API URL: {api_url}")
        resp = requests.get(api_url,
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)

        if resp.status_code != 200:
            self.log.error(f"Failed to get app {okta_app_id}: {resp.status_code}")
            self.log.error(f"Response: {resp.text}")
            raise Exception(f"Failed to get Okta app: {resp.status_code}")

        app_config = resp.json()
        self.log.info(f"Current app: {app_config.get('label', 'Unknown')}")
        # Step 2: Update only the IP addresses in SAML URLs
        if 'settings' in app_config and 'signOn' in app_config['settings']:
            sign_on = app_config['settings']['signOn']

            # Update all the SAML URLs with new IP
            saml_urls = {
                'ssoAcsUrl': f"http://{new_cluster_ip}:8091/saml/consume",
                'audience': f"http://{new_cluster_ip}:8091/saml/metadata",
                'recipient': f"http://{new_cluster_ip}:8091/saml/consume",
                'destination': f"http://{new_cluster_ip}:8091/saml/consume"
            }

            # Log what we're changing
            self.log.info(f"🔧 Updating SAML URLs for IP: {new_cluster_ip}")
            for key, new_url in saml_urls.items():
                old_url = sign_on.get(key, 'Not set')
                sign_on[key] = new_url
                self.log.info(f"  {key}: {old_url} -> {new_url}")

            # Log the critical recipient URL specifically
            self.log.info(f"🎯 Recipient URL set to: {saml_urls['recipient']}")
        else:
            raise Exception("❌ Could not find signOn settings in app config - URLs not updated!")

        # Step 3: Update the app
        update_url = self.okta_account + f"api/v1/apps/{okta_app_id}"
        self.log.info(f"🚀 Updating app via: {update_url}")
        resp = requests.put(update_url,
                            data=json.dumps(app_config),
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)

        if resp.status_code != 200:
            self.log.error(f"❌ Failed to update app: {resp.status_code}")
            self.log.error(f"Response: {resp.text}")
            raise Exception(f"Failed to update Okta app IP addresses: {resp.status_code}")

        self.log.info(f"✅ Successfully updated Okta app {okta_app_id} to use IP {new_cluster_ip}")

        # Step 4: Get updated metadata URL
        updated_app = resp.json()
        metadata_url = updated_app.get('_links', {}).get('metadata', {}).get('href')

        if metadata_url:
            self.log.info(f"Updated metadata URL: {metadata_url}")

            # Optionally fetch the updated metadata
            try:
                header = {
                    "Authorization": "SSWS " + okta_token
                }
                metadata_resp = requests.get(metadata_url, headers=header, timeout=300, verify=False)
                if metadata_resp.status_code == 200:
                    metadata_content = metadata_resp.text
                    self.log.info("Successfully retrieved updated IdP metadata")
                    return okta_app_id, metadata_content
                else:
                    self.log.warning(f"Could not fetch metadata: {metadata_resp.status_code}")
                    raise Exception("Failed to complete operation of fetching metadata")
            except Exception as e:
                self.log.warning(f"Could not fetch metadata for the reason: {e}")
                raise Exception("Failed to complete operation")

        return okta_app_id, None

    def change_assertion_signed(self, okta_token, okta_app_id, value=True):
        """
        Change the assertionSigned setting in an Okta SAML app
        This function modifies only the assertionSigned property
        """
        self.log.info(f"Changing assertionSigned to {value} for Okta app {okta_app_id}")

        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "SSWS " + okta_token
        }
        resp = requests.get(self.okta_account + f"api/v1/apps/{okta_app_id}",
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)

        if resp.status_code != 200:
            self.log.error(f"Failed to get app {okta_app_id}: {resp.status_code}")
            self.log.error(f"Response: {resp.text}")
            raise Exception(f"Failed to get Okta app: {resp.status_code}")

        app_config = resp.json()
        self.log.info(f"Current app: {app_config.get('label', 'Unknown')}")
        if 'settings' in app_config and 'signOn' in app_config['settings']:
            sign_on = app_config['settings']['signOn']
            old_value = sign_on.get('assertionSigned', 'Not set')
            sign_on['assertionSigned'] = value
            self.log.info(f"  assertionSigned: {old_value} -> {value}")

        resp = requests.put(self.okta_account + f"api/v1/apps/{okta_app_id}",
                            data=json.dumps(app_config),
                            headers=header,
                            timeout=300, verify=False, allow_redirects=False)

        if resp.status_code != 200:
            self.log.error(f"Failed to update assertionSigned: {resp.status_code}")
            self.log.error(f"Response: {resp.text}")
            raise Exception(f"Failed to update Okta app assertionSigned: {resp.status_code}")

        self.log.info(f"Successfully updated assertionSigned to {value} for app {okta_app_id}")

        updated_app = resp.json()
        metadata_url = updated_app.get('_links', {}).get('metadata', {}).get('href')

        if metadata_url:
            self.log.info(f"Updated metadata URL: {metadata_url}")

            try:
                header = {
                    "Authorization": "SSWS " + okta_token
                }
                metadata_resp = requests.get(metadata_url, headers=header, timeout=300, verify=False)
                if metadata_resp.status_code == 200:
                    metadata_content = metadata_resp.text
                    self.log.info("Successfully retrieved updated IdP metadata")
                    return okta_app_id, metadata_content
                else:
                    self.log.warning(f"Could not fetch metadata: {metadata_resp.status_code}")
                    raise Exception("Failed to complete operation of fetching metadata")
            except Exception as e:
                self.log.warning(f"Could not fetch metadata for the reason: {e}")
                raise Exception("Failed to complete operation")

        return okta_app_id, None