import re
import json
import base64
import requests
import six.moves.urllib.parse
from couchbase.bucket import Bucket


class SAMLUtils:
    def __init__(self, logger):
        self.log = logger
        self.saml_resources = None
        self.initialize_file_paths()

    def initialize_file_paths(self):
        bucket_name = "saml_tests_resources"
        ip = "172.23.124.12"
        username = "saml_test_user"
        password = "password"
        url = 'couchbase://{ip}/{name}'.format(ip=ip, name=bucket_name)
        bucket = Bucket(url, username=username, password=password)
        self.saml_resources = bucket.get("saml_resources").value

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
        assert resp.status_code == 200
        state_token = ""
        for line in resp.content.decode().split("\n"):
            if "stateToken=" in line:
                state_token = line[94:-1]
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
        url = "https://dev-82235514.okta.com/idp/idx/identify"
        body = {"identifier": identifier,
                "credentials": {"passcode": passcode},
                "stateHandle": state_token}
        header = {'Cookie': cookie_string,
                  'Content-Type': "application/json",
                  'Connection': "keep-alive",
                  'Accept-Encoding': 'gzip, deflate, br',
                  'Accept': '*/*',
                  'JSESSIONID': j_session_id
                  }
        resp = requests.post(url, data=json.dumps(body), headers=header)
        assert resp.status_code == 200
        next_url = resp.content.decode()[resp.content.decode().index("success-redirect\","
                                                                     "\"href\":\"") + 26:
                                         resp.content.decode().index("success-redirect\","
                                                                     "\"href\":\"") + 134]
        j_session_id = resp.headers["set-cookie"][resp.headers["set-cookie"].index("JSESSIONID=") +
                                                  11: resp.headers["set-cookie"].index(
            "JSESSIONID=") + 43]
        return next_url, j_session_id

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
            self.log.info("*****+++++******")
            self.log.info("SAML error message received: {0}".format(error_msg))
            self.log.info("*****+++++******")
        except json.decoder.JSONDecodeError:
            self.log.info("No SAML error message found")
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
