import json
import urllib.parse

from membase.api.rest_client import RestConnection
from pytests.security.jwt_utils import JWTUtils
from .xdcrnewbasetests import XDCRNewBaseTest


class XDCRJWTAuth(XDCRNewBaseTest):
    """
    Verifies that XDCR's REST admin surface needs no XDCR-specific code for
    JWT bearer-token auth, since it is entirely delegated to cbauth — see
    docs/superpowers/specs/2026-07-29-xdcr-jwt-auth-test-coverage-design.md.
    """

    def setUp(self):
        super(XDCRJWTAuth, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)

        self.jwt_issuer = self._input.param('jwt_issuer', 'xdcr-test-issuer')
        self.jwt_algorithm = self._input.param('jwt_algorithm', 'ES256')
        self.jwt_audience = self._input.param('jwt_audience', 'cb-cluster')
        self.jwt_utils = JWTUtils(log=self.log)
        self.private_key, self.pub_key = self.jwt_utils.generate_key_pair(self.jwt_algorithm)

        self._jwt_groups_created = []
        self._jwt_users_created = []

    def tearDown(self):
        for user_name in self._jwt_users_created:
            self.jwt_utils.delete_external_user(self.src_rest, user_name)
        for group_name in self._jwt_groups_created:
            try:
                self.src_rest.delete_group(group_name)
            except Exception as e:
                self.log.warning('Error deleting group {0}: {1}'.format(group_name, e))
        try:
            self.jwt_utils.disable_jwt(self.src_rest)
        except Exception as e:
            self.log.warning('Error disabling JWT config: {0}'.format(e))
        super(XDCRJWTAuth, self).tearDown()

    def _add_remote_cluster_via_jwt(self, token, name):
        params = urllib.parse.urlencode({
            'hostname': '{0}:{1}'.format(self.dest_master.ip, self.dest_master.port),
            'username': self.dest_master.rest_username,
            'password': self.dest_master.rest_password,
            'name': name,
        })
        return self.jwt_utils.verify_token_rest(
            self.src_rest, token, endpoint='pools/default/remoteClusters',
            method='POST', params=params)

    def _modify_remote_cluster_via_jwt(self, token, name):
        params = urllib.parse.urlencode({
            'hostname': '{0}:{1}'.format(self.dest_master.ip, self.dest_master.port),
            'username': self.dest_master.rest_username,
            'password': self.dest_master.rest_password,
            'name': name,
        })
        endpoint = 'pools/default/remoteClusters/{0}'.format(urllib.parse.quote(name, safe=''))
        return self.jwt_utils.verify_token_rest(
            self.src_rest, token, endpoint=endpoint, method='POST', params=params)

    def _remove_remote_cluster_via_jwt(self, token, name):
        endpoint = 'pools/default/remoteClusters/{0}'.format(urllib.parse.quote(name, safe=''))
        return self.jwt_utils.verify_token_rest(self.src_rest, token, endpoint=endpoint, method='DELETE')

    def _create_replication_via_jwt(self, token, remote_cluster_name, bucket_name='default'):
        params = urllib.parse.urlencode({
            'replicationType': 'continuous',
            'fromBucket': bucket_name,
            'toBucket': bucket_name,
            'toCluster': remote_cluster_name,
            'type': 'xmem',
        })
        ok, status_code, content = self.jwt_utils.verify_token_rest(
            self.src_rest, token, endpoint='controller/createReplication',
            method='POST', params=params)
        repl_id = None
        if ok and status_code and 200 <= int(status_code) < 300:
            repl_id = json.loads(content)['id']
        return ok, status_code, repl_id

    def _replication_settings_endpoint(self, repl_id):
        return 'settings/replications/{0}'.format(urllib.parse.quote(repl_id, safe=''))

    def _get_replication_settings_via_jwt(self, token, repl_id):
        return self.jwt_utils.verify_token_rest(
            self.src_rest, token, endpoint=self._replication_settings_endpoint(repl_id), method='GET')

    def _set_replication_setting_via_jwt(self, token, repl_id, param, value):
        params = urllib.parse.urlencode({param: value})
        return self.jwt_utils.verify_token_rest(
            self.src_rest, token, endpoint=self._replication_settings_endpoint(repl_id),
            method='POST', params=params)

    def _delete_replication_via_jwt(self, token, bucket_name='default'):
        replication = self.src_rest.get_replication_for_buckets(bucket_name, bucket_name)
        endpoint = replication['cancelURI'].lstrip('/')
        return self.jwt_utils.verify_token_rest(self.src_rest, token, endpoint=endpoint, method='DELETE')

    def _create_basic_auth_replication(self, remote_name):
        try:
            self.src_rest.add_remote_cluster(
                self.dest_master.ip, self.dest_master.port,
                self.dest_master.rest_username, self.dest_master.rest_password, remote_name)
        except Exception as e:
            self.fail('Basic-Auth scaffolding: failed to create remote cluster reference: {0}'.format(e))
        return self.src_rest.start_replication('continuous', 'default', remote_name)

    def _provision_jwt_user(self, group_name, role, jwt_group_claim, user_name, ttl=300):
        status, content = self.src_rest.add_group_role(group_name, 'XDCR JWT test group', role)
        if not status:
            self.fail('Failed to create group {0}: {1}'.format(group_name, content))
        self._jwt_groups_created.append(group_name)

        config = self.jwt_utils.get_jwt_config(
            issuer_name=self.jwt_issuer, algorithm=self.jwt_algorithm, pub_key=self.pub_key,
            token_audience=[self.jwt_audience],
            token_group_matching_rule=['^{0}$ {1}'.format(jwt_group_claim, group_name)],
            jit_provisioning=True)
        self.jwt_utils.put_jwt_config(self.src_rest, config)
        self._jwt_users_created.append(user_name)

        return self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer, user_name=user_name, algorithm=self.jwt_algorithm,
            private_key=self.private_key, token_audience=[self.jwt_audience],
            user_groups=[jwt_group_claim], ttl=ttl)

    def test_jwt_admin_full_replication_lifecycle(self):
        """
        A JIT-provisioned JWT user mapped to replication_admin drives the
        entire XDCR REST admin surface: create RCR, create replication,
        read/write settings, pause, resume, delete replication, delete RCR.
        Every call must succeed — this is the direct verification of the
        1-pager's claim that cbauth's generic proxy handling is sufficient.
        """
        token = self._provision_jwt_user('xdcr_admin_group', 'replication_admin',
                                          'xdcr_admins', 'xdcr_jwt_admin_user')
        remote_name = 'jwt_remote_c2'

        ok, status_code, content = self._add_remote_cluster_via_jwt(token, remote_name)
        self.jwt_utils.assert_auth_succeeds(ok, status_code,
            'JWT admin should be able to create a remote cluster reference: {0}'.format(content))

        ok, status_code, content = self._modify_remote_cluster_via_jwt(token, remote_name)
        self.jwt_utils.assert_auth_succeeds(ok, status_code,
            'JWT admin should be able to edit a remote cluster reference: {0}'.format(content))

        ok, status_code, repl_id = self._create_replication_via_jwt(token, remote_name)
        self.jwt_utils.assert_auth_succeeds(ok, status_code, 'JWT admin should be able to create a replication')
        self.assertIsNotNone(repl_id, 'Expected a replication id from createReplication response')

        ok, status_code, content = self._get_replication_settings_via_jwt(token, repl_id)
        self.jwt_utils.assert_auth_succeeds(ok, status_code, 'JWT admin should be able to read replication settings')

        ok, status_code, content = self._set_replication_setting_via_jwt(token, repl_id, 'pauseRequested', 'true')
        self.jwt_utils.assert_auth_succeeds(ok, status_code, 'JWT admin should be able to pause a replication')
        self.assertTrue(self.src_rest.is_replication_paused_by_id(repl_id),
                         'Replication should be paused after JWT-authenticated pause call')

        ok, status_code, content = self._set_replication_setting_via_jwt(token, repl_id, 'pauseRequested', 'false')
        self.jwt_utils.assert_auth_succeeds(ok, status_code, 'JWT admin should be able to resume a replication')
        self.assertFalse(self.src_rest.is_replication_paused_by_id(repl_id),
                          'Replication should be resumed after JWT-authenticated resume call')

        ok, status_code, content = self._delete_replication_via_jwt(token)
        self.jwt_utils.assert_auth_succeeds(ok, status_code,
            'JWT admin should be able to delete a replication: {0}'.format(content))

        ok, status_code, content = self._remove_remote_cluster_via_jwt(token, remote_name)
        self.jwt_utils.assert_auth_succeeds(ok, status_code,
            'JWT admin should be able to delete a remote cluster reference: {0}'.format(content))

    def test_jwt_user_without_xdcr_permission_gets_forbidden(self):
        """
        A JIT-provisioned JWT user mapped to a role with zero XDCR
        permissions (data_reader) must get 403 on every XDCR admin call,
        even against a replication that already exists.
        """
        remote_name = 'jwt_remote_c2_negative'
        repl_id = self._create_basic_auth_replication(remote_name)

        token = self._provision_jwt_user('xdcr_no_access_group', 'data_reader[*]',
                                          'no_xdcr_access', 'xdcr_jwt_no_access_user')

        whoami = self.jwt_utils.get_user_info_from_whoami(self.src_rest, token)
        self.jwt_utils.assert_external_identity(whoami, 'xdcr_jwt_no_access_user')

        ok, status_code, content = self._add_remote_cluster_via_jwt(token, 'jwt_remote_should_fail')
        self.jwt_utils.assert_unauthorized_status(status_code,
            'JWT user without XDCR role should not create a remote cluster reference, got {0}'.format(status_code))

        ok, status_code, content = self._get_replication_settings_via_jwt(token, repl_id)
        self.jwt_utils.assert_unauthorized_status(status_code,
            'JWT user without XDCR role should not read replication settings, got {0}'.format(status_code))

        ok, status_code, content = self._set_replication_setting_via_jwt(token, repl_id, 'pauseRequested', 'true')
        self.jwt_utils.assert_unauthorized_status(status_code,
            'JWT user without XDCR role should not pause a replication, got {0}'.format(status_code))

        ok, status_code, content = self._delete_replication_via_jwt(token)
        self.jwt_utils.assert_unauthorized_status(status_code,
            'JWT user without XDCR role should not delete a replication, got {0}'.format(status_code))

    def _assert_malformed_token_rejected_on_endpoint(self, token, endpoint, token_type):
        """
        Like JWTUtils.verify_malformed_token_rejection, but against a caller-supplied
        endpoint instead of the generic pools/default/buckets default -- used here so
        rejection is proven against XDCR's own REST surface (settings/replications/<id>),
        not just any cbauth-guarded endpoint. jwt_utils.py is intentionally left
        untouched (no endpoint param added there); this is inline test-local logic.
        """
        ok, status_code, content = self.jwt_utils.verify_token_rest(
            self.src_rest, token, endpoint=endpoint, method='GET')
        self.assertFalse(ok and self.jwt_utils._is_success_status(status_code),
            'Expected malformed token ({0}) to be rejected but request succeeded'.format(token_type))
        if status_code is not None:
            self.assertIn(int(status_code), (400, 401),
                'Unexpected status for malformed token ({0}): {1}'.format(token_type, status_code))
        self.log.info('Malformed token ({0}) correctly rejected, status={1}'.format(token_type, status_code))

    def test_malformed_expired_tampered_jwt_rejected(self):
        """
        Expired tokens, tokens with a tampered payload, and tokens with a
        dropped signature must all be rejected (400/401) against a real
        XDCR-specific REST endpoint (settings/replications/<id>), proving
        rejection on XDCR's own admin surface rather than any generic
        cbauth-guarded endpoint.
        """
        remote_name = 'jwt_remote_c2_tamper'
        repl_id = self._create_basic_auth_replication(remote_name)
        replication_settings_endpoint = self._replication_settings_endpoint(repl_id)

        tamper_user_name = 'xdcr_jwt_tamper_user'
        tamper_group_claim = 'xdcr_admins_tamper'

        expired_token = self._provision_jwt_user('xdcr_admin_group_tamper', 'replication_admin',
                                                  tamper_group_claim, tamper_user_name, ttl=2)
        # cbauth's JWT validation applies a default expiryLeewayS of 15s (confirmed
        # live via GET /settings/jwt), so a 5s wait is not enough to clear it and the
        # "expired" token is still accepted. Mirror gsi_jwt_auth.py's proven pattern
        # (short ttl, wait 20s) and wait comfortably past ttl + the 15s leeway.
        self.sleep(20, 'Waiting for token to expire past the default 15s leeway')
        self._assert_malformed_token_rejected_on_endpoint(expired_token, replication_settings_endpoint, 'expired')

        valid_token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer, user_name=tamper_user_name, algorithm=self.jwt_algorithm,
            private_key=self.private_key, token_audience=[self.jwt_audience],
            user_groups=[tamper_group_claim], ttl=300)

        ok, status_code, content = self._get_replication_settings_via_jwt(valid_token, repl_id)
        self.jwt_utils.assert_auth_succeeds(ok, status_code,
            'Valid token should authenticate successfully before tampering')

        tampered_payload_token = self.jwt_utils.build_tampered_payload_token(
            valid_token, {'sub': 'someone_else'})
        self._assert_malformed_token_rejected_on_endpoint(
            tampered_payload_token, replication_settings_endpoint, 'tampered-payload')

        dropped_sig_token = self.jwt_utils.build_tampered_header_token(
            valid_token, {'alg': 'none'}, drop_signature=True)
        self._assert_malformed_token_rejected_on_endpoint(
            dropped_sig_token, replication_settings_endpoint, 'dropped-signature')

    def test_basic_auth_and_jwt_coexist_across_replication_lifecycle(self):
        """
        Create a replication with Basic Auth, pause/resume it with JWT,
        delete it (and its RCR) with Basic Auth. No XDCR-specific
        session/header assumption should break when the auth mechanism
        changes mid-lifecycle.
        """
        remote_name = 'jwt_remote_c2_coexist'
        repl_id = self._create_basic_auth_replication(remote_name)

        token = self._provision_jwt_user('xdcr_admin_group_coexist', 'replication_admin',
                                          'xdcr_admins_coexist', 'xdcr_jwt_coexist_user')

        ok, status_code, content = self._set_replication_setting_via_jwt(token, repl_id, 'pauseRequested', 'true')
        self.jwt_utils.assert_auth_succeeds(ok, status_code, 'JWT should be able to pause a Basic-Auth-created replication')
        self.assertTrue(self.src_rest.is_replication_paused_by_id(repl_id),
            'Replication should be paused after JWT-authenticated pause call')

        ok, status_code, content = self._set_replication_setting_via_jwt(token, repl_id, 'pauseRequested', 'false')
        self.jwt_utils.assert_auth_succeeds(ok, status_code, 'JWT should be able to resume a Basic-Auth-created replication')
        self.assertFalse(self.src_rest.is_replication_paused_by_id(repl_id),
            'Replication should be resumed after JWT-authenticated resume call')

        replication = self.src_rest.get_replication_for_buckets('default', 'default')
        self.src_rest.stop_replication(replication['cancelURI'])
        self.src_rest.remove_remote_cluster(remote_name)

    def test_jwt_config_enabled_does_not_break_p2p_replication(self):
        """
        Enabling a JWT issuer on the cluster must not perturb XDCR's P2P
        path (cbauth's SetRequestAuthVia), which never uses JWT. This is a
        regression check, not a JWT feature test: the token this method
        provisions is never used to drive the replication itself — only
        ordinary Basic-Auth-backed setup_xdcr_and_load()/perform_update_delete()/verify_results()
        run, exactly as any other XDCR regression test.
        """
        self._provision_jwt_user('xdcr_admin_group_p2p', 'replication_admin',
                                  'xdcr_admins_p2p', 'xdcr_jwt_p2p_smoke_user')

        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()
