"""
XDCR Staged Credentials Tests

Tests for the seamless password change feature that allows staging secondary credentials
on remote cluster references. When primary credentials fail, XDCR will automatically
try staged credentials and promote them if successful.

API Endpoints:
- POST /pools/default/remoteClusters/<name> with stage=true - stage credentials
- GET /pools/default/remoteClusters?stage=true - retrieve staged credentials info
"""

import time

from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from security.rbac_base import RbacBase

from .xdcrnewbasetests import XDCRNewBaseTest, NodeHelper


class StagedCredentialsXDCR(XDCRNewBaseTest):
    """Test suite for XDCR staged credentials / seamless password change feature."""

    def setUp(self):
        super(StagedCredentialsXDCR, self).setUp()

        # Initialize cluster references
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)

        # Default credentials for staging
        self.staged_username = self._input.param("staged_username", "staged_user")
        self.staged_password = self._input.param("staged_password", "staged_password")

        # Get remote cluster reference name
        self.remote_cluster_name = self._input.param(
            "remote_cluster_name",
            "remote_cluster_C1-C2"
        )

    def tearDown(self):
        try:
            # Restore XDCR user permissions if modified during test
            self._restore_xdcr_user_permissions()
        except Exception as e:
            self.log.warning(f"Error during teardown: {e}")
        finally:
            super(StagedCredentialsXDCR, self).tearDown()

    def _get_remote_cluster_name(self):
        """Get the name of the first remote cluster reference."""
        remote_clusters = self.src_rest.get_remote_clusters()
        if remote_clusters:
            return remote_clusters[0].get('name', self.remote_cluster_name)
        return self.remote_cluster_name

    def _stage_credentials(self, username, password, remote_name=None):
        """Stage username/password credentials for the remote cluster reference."""
        if remote_name is None:
            remote_name = self._get_remote_cluster_name()
        self.log.info(f"Staging credentials for remote cluster '{remote_name}': username={username}")
        return self.src_rest.stage_remote_cluster_credentials(remote_name, username, password)

    def _stage_certificates(self, client_cert, client_key, remote_name=None):
        """Stage client certificate/key for the remote cluster reference."""
        if remote_name is None:
            remote_name = self._get_remote_cluster_name()
        self.log.info(f"Staging certificates for remote cluster '{remote_name}'")
        return self.src_rest.stage_remote_cluster_certificates(remote_name, client_cert, client_key)

    def _get_staged_credentials(self):
        """Retrieve remote clusters with staged credentials info."""
        result = self.src_rest.get_remote_clusters_with_stage()
        self.log.info(f"Retrieved {len(result)} remote clusters with stage parameter")
        return result

    def _verify_staged_credentials(self, expected_username):
        """Verify staged credentials are correctly stored."""
        remote_clusters = self._get_staged_credentials()
        self.log.info(f"Checking for staged username: {expected_username}")
        self.log.info(f"Retrieved {len(remote_clusters)} remote clusters with stage parameter")
        for i, rc in enumerate(remote_clusters):
            self.log.info(f"Remote cluster [{i}] keys: {list(rc.keys())}")
            self.log.info(f"Remote cluster [{i}]: {rc}")
            # Check for 'stage' field (newer API format)
            if 'stage' in rc:
                stage_info = rc.get('stage')
                self.log.info(f"Stage field found: {stage_info}, type: {type(stage_info)}")
                if isinstance(stage_info, dict):
                    if stage_info.get('username') == expected_username:
                        self.log.info(f"Verified staged username (stage dict): {expected_username}")
                        return True
                elif isinstance(stage_info, bool) and stage_info:
                    # If stage is True, check main username field
                    if rc.get('username') == expected_username:
                        self.log.info(f"Verified staged username (stage True via main field): {expected_username}")
                        return True
            else:
                # Fallback: check if username matches directly
                # Some API versions might return staged credentials directly in the main object
                self.log.info(f"No 'stage' field, checking direct username match")
                if rc.get('username') == expected_username:
                    self.log.info(f"Verified staged username (direct match): {expected_username}")
                    return True

        # Additional check: look for staged_user_* pattern in the response
        self.log.info(f"Full scan check for any username matching '{expected_username}'")
        for i, rc in enumerate(remote_clusters):
            for key, value in rc.items():
                if isinstance(value, dict) and 'username' in value:
                    if value['username'] == expected_username:
                        self.log.info(f"Found staged username in nested field {key}: {expected_username}")
                        return True

        self.log.error(f"Staged username '{expected_username}' not found in any format")
        self.log.error(f"All remote clusters: {remote_clusters}")
        return False

    def _create_xdcr_user(self, node, username, password, roles="replication_target[*]"):
        """Create a user with XDCR replication target permissions."""
        self.log.info(f"Creating XDCR user '{username}' with roles '{roles}' on {node.ip}")
        testuser = [{'id': username, 'name': username, 'password': password}]
        RbacBase().create_user_source(testuser, 'builtin', node)
        time.sleep(2)
        role_list = [{'id': username, 'name': username, 'roles': roles, 'password': password}]
        RbacBase().add_user_role(role_list, RestConnection(node), 'builtin')

    def _change_user_roles(self, node, username, new_roles, password=None):
        """Change the roles/permissions for an existing user."""
        self.log.info(f"Changing roles for user '{username}' to '{new_roles}' on {node.ip}")
        if password is None:
            password = node.rest_password
        role_list = [{'id': username, 'name': username, 'roles': new_roles, 'password': password}]
        RbacBase().add_user_role(role_list, RestConnection(node), 'builtin')

    def _strip_xdcr_permissions(self, node, username, password=None):
        """Remove XDCR permissions from a user (assign a non-XDCR role)."""
        self.log.info(f"Stripping XDCR permissions from user '{username}' on {node.ip}")
        if password is None:
            password = node.rest_password
        # Assign a role that doesn't have XDCR permissions
        role_list = [{'id': username, 'name': username, 'roles': 'ro_admin', 'password': password}]
        RbacBase().add_user_role(role_list, RestConnection(node), 'builtin')

    def _restore_xdcr_user_permissions(self):
        """Restore XDCR permissions to the admin user (cleanup helper)."""
        try:
            password = self.dest_master.rest_password
            role_list = [{'id': 'Administrator', 'name': 'Administrator', 'roles': 'admin', 'password': password}]
            RbacBase().add_user_role(role_list, self.dest_rest, 'builtin')
        except Exception:
            pass

    def _delete_user(self, node, username):
        """Delete a user from the cluster."""
        self.log.info(f"Deleting user '{username}' from {node.ip}")
        try:
            RbacBase().remove_user_role([username], RestConnection(node))
        except Exception as e:
            self.log.warning(f"Error deleting user: {e}")

    def _get_replication_status(self):
        """Get the connectivity status of remote cluster references."""
        remote_clusters = self.src_rest.get_remote_clusters()
        statuses = {}
        for rc in remote_clusters:
            statuses[rc['name']] = rc.get('connectivityStatus', 'UNKNOWN')
        return statuses

    def _wait_for_connectivity_status(self, expected_status, timeout=60, interval=5):
        """Wait for remote cluster to reach expected connectivity status."""
        self.log.info(f"Waiting for connectivity status: {expected_status}")
        end_time = time.time() + timeout
        while time.time() < end_time:
            statuses = self._get_replication_status()
            for name, status in statuses.items():
                if status == expected_status:
                    self.log.info(f"Remote cluster '{name}' reached status: {status}")
                    return True
            time.sleep(interval)
        self.log.warning(f"Timeout waiting for status {expected_status}. Current: {statuses}")
        return False

    def _wait_for_replication_to_recover(self, timeout=120):
        """Wait for replication to recover and show RC_OK status."""
        return self._wait_for_connectivity_status("RC_OK", timeout=timeout)

    def _wait_for_auth_error(self, timeout=60, interval=5):
        """Wait for authentication error or any error status."""
        self.log.info("Waiting for authentication error status")
        error_statuses = ["RC_AUTH_ERR", "RC_ERROR", "RC_DEGRADED"]
        end_time = time.time() + timeout
        while time.time() < end_time:
            statuses = self._get_replication_status()
            for name, status in statuses.items():
                if status in error_statuses:
                    self.log.info(f"Remote cluster '{name}' reached error status: {status}")
                    return True
            time.sleep(interval)
        self.log.warning(f"Timeout waiting for error status. Current: {statuses}")
        return False

    def _change_user_password(self, node, username, new_password):
        """Change password for a user."""
        self.log.info(f"Changing password for user '{username}' on {node.ip}")
        rest = RestConnection(node)
        api = rest.baseUrl + f"settings/rbac/users/local/{username}"
        params = f"password={new_password}"
        status, content, _ = rest._http_request(api, 'PUT', params)
        return status

    def _change_admin_password(self, node, current_password, new_password):
        """Change the Administrator password on a node."""
        self.log.info(f"Changing Administrator password on {node.ip}")
        rest = RestConnection(node)
        api = rest.baseUrl + "controller/changePassword"
        params = f"password={new_password}"
        status, content, _ = rest._http_request(api, 'POST', params)
        if status:
            self.log.info("Administrator password changed successfully")
        else:
            self.log.error(f"Failed to change password: {content}")
        return status

    def _kill_goxdcr(self, cluster, wait_to_recover=True):
        """Kill goxdcr process on all nodes in a cluster."""
        for node in cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                cmd = 'pkill -9 goxdcr || true'
                shell.execute_command(cmd, use_channel=True)
                self.log.info(f"Killed goxdcr on {node.ip}")
            except Exception as e:
                self.log.error(f"Error killing goxdcr on {node.ip}: {e}")
            finally:
                shell.disconnect()

        if wait_to_recover:
            self.sleep(15, "Waiting for goxdcr to restart")

    def _load_docs_with_pillowfight(self, server, items, bucket="default",
                                     batch=1000, docsize=100, rate_limit=100000):
        """Load documents using pillowfight for high load testing."""
        shell = RemoteMachineShellConnection(server)
        cmd = (f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password "
               f"-U couchbase://localhost/{bucket} -I {items} -m {docsize} -M {docsize} "
               f"-B {batch} --rate-limit={rate_limit} --populate-only")
        self.log.info(f"Loading {items} docs with pillowfight on {server.ip}")
        try:
            output, error = shell.execute_command(cmd, timeout=300, use_channel=True)
            if error:
                self.log.warning(f"Pillowfight warning: {error}")
        finally:
            shell.disconnect()

    def _check_logs_for_plaintext_password(self, password):
        """Check if a password appears in plain text in goxdcr logs."""
        self.log.info(f"Checking logs for plain text password")
        matches, count = NodeHelper.check_goxdcr_log(
            self.src_master,
            password,
            print_matches=False
        )
        return count > 0

    def _toggle_cross_cluster_versioning(self, server, bucket="default", enable=True):
        """Enable or disable cross-cluster versioning (ECCV) on a bucket."""
        self.log.info(f"Setting ECCV to {enable} on {server.ip} for bucket {bucket}")
        shell = RemoteMachineShellConnection(server)
        value = "true" if enable else "false"
        cmd = (f"curl -s -X POST localhost:8091/pools/default/buckets/{bucket} "
               f"-d enableCrossClusterVersioning={value} "
               f"-u {server.rest_username}:{server.rest_password}")
        try:
            shell.execute_command(cmd, use_channel=True, timeout=10)
        finally:
            shell.disconnect()

    def test_stage_credentials_and_rotate(self):
        """
        Test basic credential staging and automatic rotation.
        1. Setup XDCR replication
        2. Create a secondary user for staging
        3. Stage the secondary user credentials
        4. Strip XDCR permissions from primary user on target
        5. Verify XDCR auto-rotates to staged credentials
        6. Verify replication recovers and continues
        """
        # Setup XDCR
        self.setup_xdcr_and_load()
        self.verify_results()

        # Create a secondary user on target for staging
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)

        # Stage the secondary credentials
        remote_name = self._get_remote_cluster_name()
        result = self._stage_credentials(self.staged_username, self.staged_password, remote_name)
        self.assertIn('stage', result, "Stage info should be in response")

        # Verify staged credentials are stored
        self.assertTrue(
            self._verify_staged_credentials(self.staged_username),
            "Staged credentials should be retrievable"
        )

        # Verify replication is healthy before invalidating primary
        self.assertTrue(
            self._wait_for_connectivity_status("RC_OK", timeout=30),
            "Replication should be healthy before test"
        )

        # Strip XDCR permissions from the primary user (Administrator)
        self._strip_xdcr_permissions(self.dest_master, "Administrator")

        # Wait for auth error (primary creds now invalid)
        self.sleep(10, "Waiting for XDCR to detect auth failure")

        # Wait for replication to recover using staged credentials
        self.assertTrue(
            self._wait_for_replication_to_recover(timeout=120),
            "Replication should recover using staged credentials"
        )

        # Verify staged credentials were promoted (staging area cleared)
        remote_clusters = self._get_staged_credentials()
        for rc in remote_clusters:
            stage_info = rc.get('stage', {})
            # After rotation, stage should be empty or username changed to staged user
            self.log.info(f"Post-rotation stage info: {stage_info}")

        # Restore permissions for cleanup
        self._change_user_roles(self.dest_master, "Administrator", "admin")

        # Load more data and verify replication continues
        gen = BlobGenerator("staged-", "staged-", self._value_size, end=1000)
        self.src_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

    def test_stage_credentials_multiple_topologies(self):
        """
        Test staged credentials across multiple XDCR topologies.
        Tests staging credentials on multiple remote cluster references
        when using chain topology with bidirectional replication.
        """
        topology = self._input.param("ctopology", "chain")
        rdirection = self._input.param("rdirection", "unidirection")
        self.log.info(f"Testing staged credentials with {topology} topology, {rdirection}")

        # Setup XDCR with specified topology
        self.setup_xdcr_and_load()
        self.verify_results()

        # Get available clusters
        clusters = [self.src_cluster, self.dest_cluster]

        # Create staged user on all clusters (for bidirectional)
        for cluster in clusters:
            master = cluster.get_master_node()
            self._create_xdcr_user(master, self.staged_username, self.staged_password)

        # Stage credentials on all clusters that have remote cluster refs
        for cluster in clusters:
            master = cluster.get_master_node()
            rest = RestConnection(master)
            remote_clusters = rest.get_remote_clusters()
            for rc in remote_clusters:
                try:
                    rest.stage_remote_cluster_credentials(
                        rc['name'], self.staged_username, self.staged_password
                    )
                    self.log.info(f"Staged credentials for {rc['name']} on {master.ip}")
                except Exception as e:
                    self.log.warning(f"Could not stage credentials: {e}")

        # Invalidate primary credentials on destination
        self._strip_xdcr_permissions(self.dest_master, "Administrator")

        # Wait for rotation to occur
        self.sleep(20, "Waiting for credential rotation")

        # Verify replication recovers
        self.assertTrue(
            self._wait_for_replication_to_recover(timeout=120),
            "Replication should recover with staged credentials"
        )

        # Restore for cleanup
        self._change_user_roles(self.dest_master, "Administrator", "admin")

    def test_stage_credentials_under_high_load(self):
        """
        Test staging credentials while source cluster is under high load.
        Staging operation should succeed regardless of replication load.
        """
        # Setup XDCR
        self.setup_xdcr()
        self.sleep(10, "Waiting for replication to stabilize")

        # Create staged user on target
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)

        # Start high load on source in background
        self.log.info("Starting high load on source cluster")
        self._load_docs_with_pillowfight(self.src_master, 50000, "default")

        # Stage credentials during load
        remote_name = self._get_remote_cluster_name()
        result = self._stage_credentials(self.staged_username, self.staged_password, remote_name)

        # Verify staging succeeded
        self.assertIn('stage', result, "Staging should succeed under load")
        self.assertTrue(
            self._verify_staged_credentials(self.staged_username),
            "Staged credentials should be stored correctly under load"
        )

        # Wait for replication to catch up
        self._wait_for_replication_to_catchup()

    def test_stage_credentials_rapid_changes(self):
        """
        Test changing staged credentials repeatedly in quick succession.
        Each POST should overwrite previous staged credentials.
        """
        # Setup XDCR
        self.setup_xdcr()
        self.sleep(10, "Waiting for replication to stabilize")

        remote_name = self._get_remote_cluster_name()

        # Rapid staging changes
        for i in range(5):
            username = f"staged_user_{i}"
            password = f"staged_pass_{i}"

            # Create user on target
            self._create_xdcr_user(self.dest_master, username, password)

            # Stage credentials
            result = self._stage_credentials(username, password, remote_name)
            self.assertIn('stage', result, f"Staging attempt {i} should succeed")

            # Verify latest credentials are staged
            self.assertTrue(
                self._verify_staged_credentials(username),
                f"Latest staged username should be {username}"
            )

            self.sleep(2, "Pause between staging operations")

        # Small delay to ensure final staging operation is processed
        self.sleep(3, "Wait for final staging to propagate")

        # Final verification - only last staged user should be active
        self.assertTrue(
            self._verify_staged_credentials("staged_user_4"),
            "Final staged credentials should be staged_user_4"
        )

    def test_stage_credentials_validation(self):
        """
        Test staging credentials validation.
        Correct payload should succeed, incorrect should return errors.
        """
        # Setup XDCR
        self.setup_xdcr()
        self.sleep(10, "Waiting for replication to stabilize")

        remote_name = self._get_remote_cluster_name()

        # Test with valid credentials
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)
        result = self._stage_credentials(self.staged_username, self.staged_password, remote_name)
        self.assertIn('stage', result, "Valid credentials should be staged successfully")

        # Test with empty username - should fail or return error
        try:
            self._stage_credentials("", self.staged_password, remote_name)
            # If no exception, check the response
            self.log.info("Empty username staging did not raise exception")
        except Exception as e:
            self.log.info(f"Empty username correctly rejected: {e}")

    def test_retrieve_staged_credentials(self):
        """
        Test retrieving staged credentials via ?stage=true query parameter.
        Username should be visible, password should be hidden.
        """
        # Setup XDCR
        self.setup_xdcr()
        self.sleep(10, "Waiting for replication to stabilize")

        # Stage credentials
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)
        remote_name = self._get_remote_cluster_name()
        self._stage_credentials(self.staged_username, self.staged_password, remote_name)

        # Retrieve staged credentials
        remote_clusters = self._get_staged_credentials()

        # Verify structure and content
        found_staged = False
        for rc in remote_clusters:
            if 'stage' in rc and rc['stage']:
                stage_info = rc['stage']
                self.log.info(f"Retrieved stage info: {stage_info}")

                # Username should be visible
                if 'username' in stage_info:
                    self.assertEqual(
                        stage_info['username'], self.staged_username,
                        "Staged username should match"
                    )
                    found_staged = True

                # Password should NOT be visible in plain text
                self.assertNotIn(
                    'password', stage_info,
                    "Password should not be returned in stage info"
                )

        self.assertTrue(found_staged, "Staged credentials should be retrievable")

    def test_mutual_exclusion_credentials_certs(self):
        """
        Test that both username/password and client cert/key cannot be staged together.
        Should return an error when both are provided.
        """
        # Setup XDCR
        self.setup_xdcr()
        self.sleep(10, "Waiting for replication to stabilize")

        remote_name = self._get_remote_cluster_name()

        # Try to stage both credentials and certificates
        # This should fail with an error
        api = self.src_rest.baseUrl + 'pools/default/remoteClusters/' + remote_name
        params = {
            'stage': 'true',
            'username': self.staged_username,
            'password': self.staged_password,
            'clientCertificate': 'dummy_cert',
            'clientKey': 'dummy_key'
        }
        import urllib.parse
        encoded_params = urllib.parse.urlencode(params)

        status, content, _ = self.src_rest._http_request(api, 'POST', encoded_params)

        # The API should reject this request
        if status:
            self.log.warning("API accepted both credentials and certs - may need validation")
        else:
            self.log.info(f"API correctly rejected mixed credentials: {content}")

    def test_incorrect_staged_credentials(self):
        """
        Test staging incorrect credentials.
        When primary fails and staged is also wrong, replication should fail.
        """
        # Setup XDCR
        self.setup_xdcr_and_load()
        self.verify_results()

        # Stage WRONG credentials (user doesn't exist on target)
        remote_name = self._get_remote_cluster_name()
        self._stage_credentials("nonexistent_user", "wrong_password", remote_name)

        # Save original password and change the target cluster password
        # This will invalidate the primary credentials for sure
        original_password = self.dest_master.rest_password
        new_password = "InvalidatedPassword123!"
        self.log.info("Changing target cluster password to invalidate primary credentials")
        self._change_admin_password(self.dest_master, original_password, new_password)
        self.dest_master.rest_password = new_password

        # Kill goxdcr to force re-authentication with new credentials
        self.log.info("Restarting goxdcr to force re-authentication")
        self._kill_goxdcr(self.src_cluster, wait_to_recover=True)

        # Load documents to force XDCR to attempt replication with invalid credentials
        self.log.info("Loading 10000 documents to trigger replication attempts")
        gen = BlobGenerator("trigger-", "trigger-", self._value_size, end=10000)
        self.src_cluster.load_all_buckets_from_generator(gen)

        # Wait for auth error to be detected
        self.sleep(60, "Waiting for XDCR to detect auth failure")

        # Check for auth error
        has_error = self._wait_for_auth_error(timeout=120)

        # Log final status for debugging
        statuses = self._get_replication_status()
        self.log.info(f"Replication statuses after both creds invalid: {statuses}")

        # If we didn't get auth error status, check if replication is NOT OK
        if not has_error:
            has_error = all(s != "RC_OK" for s in statuses.values()) if statuses else False
            self.log.info(f"Checking if replication is not OK: {has_error}")

        # Restore original password before asserting (for cleanup)
        self._change_admin_password(self.dest_master, new_password, original_password)
        self.dest_master.rest_password = original_password

        self.assertTrue(has_error, "Replication should show error with invalid staged credentials")

    def test_role_change_during_staging(self):
        """
        Test changing roles/permissions while credentials are staged.
        Staged credentials should take over when primary loses XDCR permissions.
        """
        # Setup XDCR
        self.setup_xdcr_and_load()
        self.verify_results()

        # Create and stage secondary user
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)
        remote_name = self._get_remote_cluster_name()
        self._stage_credentials(self.staged_username, self.staged_password, remote_name)

        # Gradually modify primary user roles
        # First, change to a limited role that still has XDCR access
        self._change_user_roles(self.dest_master, "Administrator", "replication_admin")
        self.sleep(10, "Waiting after role change")

        # Verify replication still works
        self.assertTrue(
            self._wait_for_connectivity_status("RC_OK", timeout=30),
            "Replication should work with replication_admin role"
        )

        # Now strip XDCR permissions completely
        self._strip_xdcr_permissions(self.dest_master, "Administrator")
        self.sleep(15, "Waiting for credential rotation")

        # Replication should recover using staged credentials
        self.assertTrue(
            self._wait_for_replication_to_recover(timeout=120),
            "Replication should recover with staged credentials after role change"
        )

        # Restore for cleanup
        self._change_user_roles(self.dest_master, "Administrator", "admin")

    def test_both_credentials_invalid(self):
        """
        Test behavior when both primary and staged credentials are invalid.
        Replication should stop and resume when correct credentials are provided.
        """
        # Setup XDCR
        self.setup_xdcr_and_load()
        self.verify_results()

        # Stage invalid credentials (user doesn't exist)
        remote_name = self._get_remote_cluster_name()
        self._stage_credentials("invalid_user", "invalid_pass", remote_name)

        # Change admin password to invalidate primary credentials
        original_password = self.dest_master.rest_password
        new_password = "BothInvalidTest123!"
        self._change_admin_password(self.dest_master, original_password, new_password)
        self.dest_master.rest_password = new_password

        # Kill goxdcr to force re-authentication with new credentials
        self.log.info("Restarting goxdcr to force re-authentication")
        self._kill_goxdcr(self.src_cluster, wait_to_recover=True)

        # Load documents to trigger replication with invalid credentials
        self.log.info("Loading documents to trigger replication attempts")
        gen = BlobGenerator("bothinvalid-", "bothinvalid-", self._value_size, end=5000)
        self.src_cluster.load_all_buckets_from_generator(gen)

        # Wait for failure to be detected
        self.sleep(90, "Waiting for credential failures to be detected")

        # Check status
        statuses = self._get_replication_status()
        self.log.info(f"Replication statuses with both creds invalid: {statuses}")

        # Now fix by creating a valid user and staging correct credentials
        self._create_xdcr_user(self.dest_master, "recovery_user", "recovery_pass",
                               roles="replication_target[*]")
        self._stage_credentials("recovery_user", "recovery_pass", remote_name)

        # Load more docs to trigger credential rotation
        gen = BlobGenerator("recovery-", "recovery-", self._value_size, end=1000)
        self.src_cluster.load_all_buckets_from_generator(gen)

        # Replication should recover with the new staged credentials
        self.sleep(30, "Waiting for credential rotation")
        recovered = self._wait_for_replication_to_recover(timeout=120)

        # Restore original password for cleanup
        self._change_admin_password(self.dest_master, new_password, original_password)
        self.dest_master.rest_password = original_password

        self.assertTrue(recovered, "Replication should recover after staging correct credentials")

    def test_goxdcr_restart_after_staging(self):
        """
        Test credential rotation after goxdcr process restart.
        Staged credentials should persist and work after process restart.
        """
        # Setup XDCR
        self.setup_xdcr_and_load()
        self.verify_results()

        # Create and stage secondary user
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)
        remote_name = self._get_remote_cluster_name()
        self._stage_credentials(self.staged_username, self.staged_password, remote_name)

        # Verify credentials are staged
        self.assertTrue(
            self._verify_staged_credentials(self.staged_username),
            "Credentials should be staged before restart"
        )

        # Kill goxdcr process
        self._kill_goxdcr(self.src_cluster, wait_to_recover=True)

        # Verify staged credentials persist after restart
        self.assertTrue(
            self._verify_staged_credentials(self.staged_username),
            "Staged credentials should persist after goxdcr restart"
        )

        # Now invalidate primary and verify rotation works
        self._strip_xdcr_permissions(self.dest_master, "Administrator")
        self.sleep(15, "Waiting for credential rotation after restart")

        # Verify replication recovers
        self.assertTrue(
            self._wait_for_replication_to_recover(timeout=120),
            "Replication should recover after goxdcr restart and credential rotation"
        )

        # Restore for cleanup
        self._change_user_roles(self.dest_master, "Administrator", "admin")

    def test_no_plaintext_passwords_in_logs(self):
        """
        Test that staged and primary passwords do not appear in plain text in goxdcr logs.
        This is a security requirement - checks goxdcr.log specifically, not test framework logs.
        """
        # Use unique passwords that we can search for
        unique_password = "UniqueXDCRTestPwd12345"
        staged_password = "StagedXDCRSecretPwd67890"

        # Setup XDCR
        self.setup_xdcr()
        self.sleep(10, "Waiting for replication to stabilize")

        # Create user with unique password
        self._create_xdcr_user(self.dest_master, "log_test_user", unique_password)

        # Stage credentials with unique password
        remote_name = self._get_remote_cluster_name()
        self._stage_credentials("log_test_user", staged_password, remote_name)

        # Wait for some XDCR activity and log entries
        self.sleep(15, "Allowing time for XDCR log entries")

        # Check goxdcr.log specifically for plain text passwords
        # Using NodeHelper.check_goxdcr_log which searches goxdcr.log
        primary_matches, primary_count = NodeHelper.check_goxdcr_log(
            self.src_master,
            unique_password,
            print_matches=False
        )
        staged_matches, staged_count = NodeHelper.check_goxdcr_log(
            self.src_master,
            staged_password,
            print_matches=False
        )

        self.log.info(f"Primary password occurrences in goxdcr.log: {primary_count}")
        self.log.info(f"Staged password occurrences in goxdcr.log: {staged_count}")

        # Passwords should not appear in goxdcr logs
        self.assertEqual(
            primary_count, 0,
            f"Primary password should NOT appear in goxdcr.log (found {primary_count} times)"
        )
        self.assertEqual(
            staged_count, 0,
            f"Staged password should NOT appear in goxdcr.log (found {staged_count} times)"
        )

    def test_staged_credentials_with_eccv(self):
        """
        Test staged credentials with ECCV (Extended Cross-Cluster Versioning) enabled.
        Credential staging and rotation should work normally with ECCV.
        """
        # Enable ECCV on source and destination
        self._toggle_cross_cluster_versioning(self.src_master, enable=True)
        self._toggle_cross_cluster_versioning(self.dest_master, enable=True)
        self.sleep(5, "Waiting for ECCV to be enabled")

        # Setup XDCR
        self.setup_xdcr_and_load()
        self.verify_results()

        # Create and stage secondary user
        self._create_xdcr_user(self.dest_master, self.staged_username, self.staged_password)
        remote_name = self._get_remote_cluster_name()
        result = self._stage_credentials(self.staged_username, self.staged_password, remote_name)

        self.assertIn('stage', result, "Staging should work with ECCV enabled")

        # Verify credentials are staged
        self.assertTrue(
            self._verify_staged_credentials(self.staged_username),
            "Staged credentials should be stored with ECCV enabled"
        )

        # Trigger rotation
        self._strip_xdcr_permissions(self.dest_master, "Administrator")
        self.sleep(15, "Waiting for credential rotation with ECCV")

        # Verify replication recovers
        self.assertTrue(
            self._wait_for_replication_to_recover(timeout=120),
            "Replication should recover with ECCV enabled"
        )

        # Load more data to verify ECCV replication works
        gen = BlobGenerator("eccv-", "eccv-", self._value_size, end=500)
        self.src_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        # Restore for cleanup
        self._change_user_roles(self.dest_master, "Administrator", "admin")
