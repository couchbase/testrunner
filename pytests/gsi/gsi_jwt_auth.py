"""
JWT-based authentication tests for GSI indexer REST endpoints (port 9102).

Endpoints under test:
  /stats, /_prometheusMetrics, /getIndexStatus, /settings

Auth methods tested:
  - PEM key pair (locally generated RSA key)
  - JWKS JSON (local RSA key exported as JWK set)
  - External IDP via Keycloak (realm: cb-index, JWKS URI)

Cluster topologies:
  - Single indexer node
  - Multiple indexer nodes (JWT config propagates cluster-wide via ns_server/cbauth)

Negative scenarios:
  - Expired JWT
  - JWT after auth disabled (logout)
  - Tampered JWT payload / alg:none bypass
"""
import json
import logging
import os
import time

import paramiko
import requests
import urllib3
from couchbase_helper.documentgenerator import SDKDataLoader
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from jwt.algorithms import RSAAlgorithm

from membase.api.rest_client import RestConnection
from pytests.gsi.base_gsi import BaseSecondaryIndexingTests
from pytests.security.jwt_utils import JWTUtils

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
log = logging.getLogger(__name__)

# All 4 indexer endpoints that should honour JWT bearer auth
INDEXER_ENDPOINTS = ["stats", "_prometheusMetrics", "getIndexStatus", "settings"]


class GSIJWTAuthTests(BaseSecondaryIndexingTests):
    """JWT authentication tests for GSI indexer REST endpoints (port 9102)."""

    def setUp(self):
        super().setUp()
        self.jwt_utils = JWTUtils(log=log)
        self.jwt_issuer = self.input.param("jwt_issuer", "gsi-jwt-issuer")
        self.jwt_algorithm = self.input.param("jwt_algorithm", "RS256")
        self.jwt_audience = self.input.param("jwt_audience", "cb-cluster")
        self.jwt_user = self.input.param("jwt_user", "gsi_jwt_user")
        self.jwt_ttl = self.input.param("jwt_ttl", 300)
        # Keycloak IDP params — defaults point at the shared QA IDP
        self.keycloak_ip = os.environ.get('KEYCLOAK_IDP_IP', '')
        self.keycloak_port = self.input.param("keycloak_port", 8444)
        self.keycloak_realm = self.input.param("keycloak_realm", "cb-index")
        self.keycloak_client_id = self.input.param("keycloak_client_id", "test-client")
        self.keycloak_client_secret = self.input.param("keycloak_client_secret", "")
        self.keycloak_username = self.input.param("keycloak_username", "jit_user")
        self.keycloak_password = self.input.param("keycloak_password", "password")
        self.keycloak_ssh_user = self.input.param("keycloak_ssh_user", "root")
        self.keycloak_ssh_pass = self.input.param("keycloak_ssh_pass", "couchbase")
        self.keycloak_container = self.input.param("keycloak_container", "keycloak-persistent")
        self.keycloak_compose_dir = self.input.param("keycloak_compose_dir", "/data/keycloak")

    def tearDown(self):
        try:
            self.jwt_utils.disable_jwt(self.rest)
            self.jwt_utils.delete_external_user(self.rest, self.jwt_user)
            if hasattr(self, "keycloak_username") and self.keycloak_username != self.jwt_user:
                self.jwt_utils.delete_external_user(self.rest, self.keycloak_username)
        except Exception as e:
            log.warning(f"tearDown JWT cleanup error: {e}")
        super().tearDown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _indexer_request(self, server, endpoint, token):
        """HTTP GET to an indexer endpoint on port 9102 with JWT bearer header."""
        rest = RestConnection(server)
        api = rest.index_baseUrl + endpoint.lstrip('/')
        headers = {'Authorization': f'Bearer {token}', 'Accept': '*/*'}
        _, content, response = rest._http_request(api, headers=headers)
        return int(response['status']), content.decode('utf-8', errors='replace')

    def _assert_stats_response(self, server, token):
        """
        /stats → flat JSON dict of indexer metrics.
        Always contains 'indexer_state' (e.g. "Active") and 'memory_quota'
        regardless of how many indexes exist.
        """
        status, body = self._indexer_request(server, "stats", token)
        self.assertEqual(200, status,
            f"/stats on {server.ip}: expected 200, got {status}. Body: {body[:300]}")
        data = json.loads(body)
        self.assertIsInstance(data, dict, "/stats must return a JSON object")
        self.assertIn("indexer_state", data, "/stats missing 'indexer_state'")
        self.assertIn(data["indexer_state"], {"Active", "Pause", "Bootstrap", "Warmup", "ShuttingDown"},
            f"/stats 'indexer_state' has unexpected value: {data['indexer_state']}")
        self.assertIn("memory_quota", data, "/stats missing 'memory_quota'")
        log.info(f"/stats {server.ip}: indexer_state={data['indexer_state']}, "
                 f"memory_quota={data['memory_quota']}")

    def _assert_prometheus_response(self, server, token):
        """
        /_prometheusMetrics → Prometheus text exposition format.
        Lines start with '# HELP'/'# TYPE' (metadata) or 'indexer_' (metric name).
        """
        status, body = self._indexer_request(server, "_prometheusMetrics", token)
        self.assertEqual(200, status,
            f"/_prometheusMetrics on {server.ip}: expected 200, got {status}. Body: {body[:300]}")
        lines = body.splitlines()
        self.assertGreater(len(lines), 0, "/_prometheusMetrics returned empty body")
        # At least one line must be a HELP/TYPE comment or an indexer_ metric
        has_prometheus_content = any(
            l.startswith("# HELP") or l.startswith("# TYPE") or l.startswith("indexer_")
            for l in lines
        )
        self.assertTrue(has_prometheus_content,
            f"/_prometheusMetrics body does not look like Prometheus format. "
            f"First 3 lines: {lines[:3]}")
        log.info(f"/_prometheusMetrics {server.ip}: {len(lines)} lines")

    def _assert_get_index_status_response(self, server, token):
        """
        /getIndexStatus → JSON with top-level keys 'code' and 'status'.
        code must be 'success'; status must be a list (entries have 'name', 'bucket').
        After _create_test_index(), the list must contain at least one entry.
        """
        status, body = self._indexer_request(server, "getIndexStatus", token)
        self.assertEqual(200, status,
            f"/getIndexStatus on {server.ip}: expected 200, got {status}. Body: {body[:300]}")
        data = json.loads(body)
        self.assertIn("code", data, "/getIndexStatus missing 'code'")
        self.assertEqual("success", data["code"],
            f"/getIndexStatus 'code' expected 'success', got '{data['code']}'")
        self.assertIn("status", data, "/getIndexStatus missing 'status'")
        self.assertIsInstance(data["status"], list, "/getIndexStatus 'status' must be a list")
        self.assertGreater(len(data["status"]), 0,
            f"/getIndexStatus on {server.ip}: expected indexes after hotel data load, got empty list")
        # Verify each index entry has the mandatory fields
        for entry in data["status"]:
            for field in ("name", "bucket", "status"):
                self.assertIn(field, entry,
                    f"/getIndexStatus entry missing field '{field}': {entry}")
        log.info(f"/getIndexStatus {server.ip}: code={data['code']}, "
                 f"indexes={len(data['status'])}")

    def _assert_settings_response(self, server, token):
        """
        /settings → JSON dict of indexer settings.
        'indexer.settings.storage_mode' is always present (plasma or forestdb).
        """
        status, body = self._indexer_request(server, "settings", token)
        self.assertEqual(200, status,
            f"/settings on {server.ip}: expected 200, got {status}. Body: {body[:300]}")
        data = json.loads(body)
        self.assertIsInstance(data, dict, "/settings must return a JSON object")
        self.assertIn("indexer.settings.storage_mode", data,
            "/settings missing 'indexer.settings.storage_mode'")
        self.assertIn(data["indexer.settings.storage_mode"], {"plasma", "forestdb", "memory_optimized"},
            f"/settings unexpected storage_mode: {data['indexer.settings.storage_mode']}")
        log.info(f"/settings {server.ip}: "
                 f"storage_mode={data['indexer.settings.storage_mode']}")

    def _verify_all_endpoints(self, server, token):
        """Run per-endpoint response assertions for all 4 indexer endpoints."""
        self._assert_stats_response(server, token)
        self._assert_prometheus_response(server, token)
        self._assert_get_index_status_response(server, token)
        self._assert_settings_response(server, token)

    def _load_hotel_data(self):
        """Load Hotel dataset into default bucket and create hotel indexes."""
        buckets = [b.name for b in self.rest.get_buckets()]
        if "default" not in buckets:
            self.rest.create_bucket(bucket="default", ramQuotaMB=256,
                                    replicaNumber=1, storageBackend="magma")
            self.sleep(5, "waiting for default bucket")
        gen = SDKDataLoader(num_ops=1000, percent_create=100, percent_update=0, percent_delete=0,
                            scope="_default", collection="_default", json_template="Hotel",
                            output=True, username=self.username, password=self.password)
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen, batch_size=1000,
                                                        dataset="Hotel")
        for task in tasks:
            task.result()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        namespace = "default:default._default._default"
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        create_queries = self.gsi_util_obj.get_create_index_list(
            definition_list=query_definitions, namespace=namespace)
        for query in create_queries:
            self.run_cbq_query(query=query, server=query_node)
        self.sleep(10, "waiting for hotel indexes to build")
        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=query_definitions, namespace=namespace, limit=10)
        total_results = 0
        for query in select_queries:
            result = self.run_cbq_query(query=query, server=query_node)
            count = result.get('metrics', {}).get('resultCount', 0)
            log.info(f"Hotel scan count: {count}")
            total_results += count
        self.assertGreater(total_results, 0,
            "Hotel dataset loaded but all select queries returned 0 results — bucket may be empty")

    def _get_indexer_nodes(self, all_nodes=True):
        return self.get_nodes_from_services_map(service_type="index", get_all_nodes=all_nodes)

    # --- JWT configuration helpers ---

    def _setup_pem_jwt(self):
        """Configure cluster with RSA PEM public key, return (token, private_key)."""
        private_key, public_key = self.jwt_utils.generate_key_pair(self.jwt_algorithm)
        # jit_provisioning=False: user must exist in Couchbase RBAC with admin role
        self.jwt_utils.create_external_user(self.rest, self.jwt_user, roles="admin")
        jwt_config = self.jwt_utils.get_jwt_config(
            issuer_name=self.jwt_issuer,
            algorithm=self.jwt_algorithm,
            pub_key=public_key,
            token_audience=[self.jwt_audience],
            jit_provisioning=False,
        )
        self.jwt_utils.put_jwt_config(self.rest, jwt_config)
        token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer,
            user_name=self.jwt_user,
            algorithm=self.jwt_algorithm,
            private_key=private_key,
            token_audience=[self.jwt_audience],
            ttl=self.jwt_ttl,
        )
        return token, private_key

    def _setup_jwks_json_jwt(self):
        """Configure cluster with JWKS JSON key source, return (token, private_key)."""
        private_key, public_key = self.jwt_utils.generate_key_pair(self.jwt_algorithm)
        pub_key_obj = load_pem_public_key(public_key.encode())
        jwk_dict = json.loads(RSAAlgorithm.to_jwk(pub_key_obj))
        jwks = {"keys": [jwk_dict]}
        self.jwt_utils.create_external_user(self.rest, self.jwt_user, roles="admin")
        jwt_config = self.jwt_utils.get_jwt_config(
            issuer_name=self.jwt_issuer,
            algorithm=self.jwt_algorithm,
            token_audience=[self.jwt_audience],
            jit_provisioning=False,
            public_key_source="jwks",
            jwks_json=jwks,
        )
        self.jwt_utils.put_jwt_config(self.rest, jwt_config)
        token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer,
            user_name=self.jwt_user,
            algorithm=self.jwt_algorithm,
            private_key=private_key,
            token_audience=[self.jwt_audience],
            ttl=self.jwt_ttl,
        )
        return token, private_key

    def _ensure_keycloak_running(self, timeout=90):
        """Check if Keycloak is reachable; if not, start it via SSH and wait."""
        check_url = (f"https://{self.keycloak_ip}:{self.keycloak_port}"
                     f"/realms/{self.keycloak_realm}/.well-known/openid-configuration")

        def _is_up():
            try:
                r = requests.get(check_url, timeout=5, verify=False)
                return r.status_code == 200
            except Exception:
                return False

        if _is_up():
            log.info(f"Keycloak already running at {self.keycloak_ip}:{self.keycloak_port}")
            return

        log.info(f"Keycloak not reachable at {self.keycloak_ip}:{self.keycloak_port}, starting via SSH...")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.keycloak_ip, username=self.keycloak_ssh_user,
                       password=self.keycloak_ssh_pass, timeout=10)
        cmd = (f"docker start {self.keycloak_container} 2>/dev/null "
               f"|| (cd {self.keycloak_compose_dir} && docker compose up -d)")
        _, stdout, stderr = client.exec_command(cmd)
        log.info(f"Keycloak start output: {stdout.read().decode().strip()}")
        err = stderr.read().decode().strip()
        if err:
            log.warning(f"Keycloak start stderr: {err}")
        client.close()

        log.info(f"Waiting up to {timeout}s for Keycloak to be ready...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            if _is_up():
                log.info(f"Keycloak started successfully at {self.keycloak_ip}:{self.keycloak_port}")
                return
            time.sleep(5)
        self.fail(f"Keycloak did not start within {timeout}s at {self.keycloak_ip}:{self.keycloak_port}")

    def _configure_idp_jwt(self):
        """Configure JWT issuer using Keycloak JWKS URI (jitProvisioning=True)."""
        issuer_name = (f"https://{self.keycloak_ip}:{self.keycloak_port}"
                       f"/realms/{self.keycloak_realm}")
        jwks_uri = f"{issuer_name}/protocol/openid-connect/certs"
        jwt_config = {
            "enabled": True,
            "issuers": [{
                "name": issuer_name,
                "signingAlgorithm": "RS256",
                "audClaim": "azp",
                "audienceHandling": "any",
                "audiences": [self.keycloak_client_id],
                "subClaim": "preferred_username",
                "publicKeySource": "jwks_uri",
                "jwksUri": jwks_uri,
                "jwksUriTlsVerifyPeer": False,  # test IDP uses self-signed cert
                "jitProvisioning": True,         # roles come from Keycloak claims
            }]
        }
        self.jwt_utils.put_jwt_config(self.rest, jwt_config)
        log.info(f"Configured IDP JWT issuer: {issuer_name}")
        return issuer_name

    def _get_idp_token(self):
        """Obtain a JWT access token from Keycloak using password grant flow."""
        url = (f"https://{self.keycloak_ip}:{self.keycloak_port}"
               f"/realms/{self.keycloak_realm}/protocol/openid-connect/token")
        data = {
            "grant_type": "password",
            "scope": "openid",
            "client_id": self.keycloak_client_id,
            "username": self.keycloak_username,
            "password": self.keycloak_password,
        }
        if self.keycloak_client_secret:
            data["client_secret"] = self.keycloak_client_secret
        resp = requests.post(url, data=data, timeout=30, verify=False)
        self.assertEqual(200, resp.status_code,
            f"IDP token request failed: {resp.status_code} {resp.text}")
        return resp.json()["access_token"]

    # ------------------------------------------------------------------
    # Tests — PEM key pair
    # ------------------------------------------------------------------

    def test_pem_jwt_indexer_endpoints(self):
        """
        PEM-based JWT across all indexer nodes:
        - Configure cluster with RSA public key (PEM), jitProvisioning=False
        - Load Hotel dataset and create hotel indexes
        - Verify all 4 indexer endpoints return 200 with valid JWT on every indexer node
          (validates JWT config propagation from master to each indexer via ns_server)
        """
        token, _ = self._setup_pem_jwt()
        self._load_hotel_data()
        nodes = self._get_indexer_nodes()
        self.assertGreater(len(nodes), 0, "No indexer nodes found in cluster")
        for node in nodes:
            log.info(f"Verifying PEM JWT endpoints on {node.ip}")
            self._verify_all_endpoints(node, token)

    # ------------------------------------------------------------------
    # Tests — JWKS JSON key
    # ------------------------------------------------------------------

    def test_jwks_json_jwt_indexer_endpoints(self):
        """
        JWKS JSON key source across all indexer nodes:
        - Build a JWK set from the locally generated RSA public key
        - Configure cluster with publicKeySource=jwks
        - Load Hotel dataset and create hotel indexes
        - Verify all 4 endpoints return 200 with JWT on every indexer node
        """
        token, _ = self._setup_jwks_json_jwt()
        self._load_hotel_data()
        nodes = self._get_indexer_nodes()
        self.assertGreater(len(nodes), 0, "No indexer nodes found in cluster")
        for node in nodes:
            log.info(f"Verifying JWKS-JSON JWT endpoints on {node.ip}")
            self._verify_all_endpoints(node, token)

    # ------------------------------------------------------------------
    # Tests — External IDP (Keycloak, realm: cb-index)
    # ------------------------------------------------------------------

    def test_idp_jwt_indexer_endpoints(self):
        """
        Keycloak IDP token across all indexer nodes:
        - Configure cluster with Keycloak JWKS URI (realm cb-index, jitProvisioning=True)
        - Pre-create user with admin role (JIT provisioning grants auth but not authz)
        - Load Hotel dataset and create hotel indexes
        - Obtain access_token from IDP via password grant
        - Verify all 4 indexer endpoints return 200 with IDP-issued token on every indexer node
        """
        self._ensure_keycloak_running()
        self._configure_idp_jwt()
        self.jwt_utils.create_external_user(self.rest, self.keycloak_username, roles="admin")
        self._load_hotel_data()
        token = self._get_idp_token()
        nodes = self._get_indexer_nodes()
        self.assertGreater(len(nodes), 0, "No indexer nodes found in cluster")
        for node in nodes:
            log.info(f"Verifying IDP JWT endpoints on {node.ip}")
            self._verify_all_endpoints(node, token)

    # ------------------------------------------------------------------
    # Negative scenarios (combined)
    # ------------------------------------------------------------------

    def test_negative_jwt_scenarios(self):
        """
        All negative JWT auth scenarios in one test:
        1. Expired JWT (ttl=1s, wait past 15s leeway) → 401/403 on all endpoints
        2. JWT disabled → previously valid token rejected on all endpoints
        3. Tampered tokens (payload change + alg:none bypass) → rejected on all endpoints
        """
        private_key, public_key = self.jwt_utils.generate_key_pair(self.jwt_algorithm)
        self.jwt_utils.create_external_user(self.rest, self.jwt_user, roles="admin")
        jwt_config = self.jwt_utils.get_jwt_config(
            issuer_name=self.jwt_issuer,
            algorithm=self.jwt_algorithm,
            pub_key=public_key,
            token_audience=[self.jwt_audience],
            jit_provisioning=False,
        )
        self.jwt_utils.put_jwt_config(self.rest, jwt_config)
        valid_token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer,
            user_name=self.jwt_user,
            algorithm=self.jwt_algorithm,
            private_key=private_key,
            token_audience=[self.jwt_audience],
            ttl=self.jwt_ttl,
        )
        # Create expired token immediately so it's already ticking while hotel data loads
        expired_token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer,
            user_name=self.jwt_user,
            algorithm=self.jwt_algorithm,
            private_key=private_key,
            token_audience=[self.jwt_audience],
            ttl=1,
        )
        self._load_hotel_data()
        nodes = self._get_indexer_nodes()
        self.assertGreater(len(nodes), 0, "No indexer nodes found in cluster")
        node = nodes[0]

        # --- Scenario 1: Expired token ---
        # expiryLeewayS defaults to 15s; wait 20s past token creation to clear the leeway
        log.info("Waiting 20s for JWT to expire past the default 15s leeway")
        self.sleep(20, "waiting for JWT expiry + leeway")
        for ep in INDEXER_ENDPOINTS:
            status, _ = self._indexer_request(node, ep, expired_token)
            self.assertIn(status, [401, 403],
                f"Expired JWT should be rejected on /{ep}, got {status}")
        log.info("Expired token correctly rejected on all endpoints")

        # --- Scenario 2: JWT disabled ---
        status, _ = self._indexer_request(node, "stats", valid_token)
        self.assertEqual(200, status, "Valid token must work before JWT is disabled")
        self.jwt_utils.disable_jwt(self.rest)
        self.sleep(3, "allow JWT config change to propagate to cbauth")
        for ep in INDEXER_ENDPOINTS:
            status, _ = self._indexer_request(node, ep, valid_token)
            self.assertIn(status, [401, 403],
                f"Token should be rejected after JWT disabled on /{ep}, got {status}")
        log.info("Token correctly rejected after JWT disabled")

        # Re-enable JWT for tampered token scenario
        self.jwt_utils.put_jwt_config(self.rest, jwt_config)
        self.sleep(3, "allow JWT config to re-enable")

        # --- Scenario 3: Tampered tokens ---
        tampered_payload = self.jwt_utils.build_tampered_payload_token(
            valid_token, {"sub": "evil_user", "groups": ["admin"]}
        )
        tampered_alg_none = self.jwt_utils.build_tampered_header_token(
            valid_token, {"alg": "none"}, drop_signature=True
        )
        for tampered, label in [
            (tampered_payload, "payload_tampered"),
            (tampered_alg_none, "alg_none_bypass"),
        ]:
            for ep in INDEXER_ENDPOINTS:
                status, _ = self._indexer_request(node, ep, tampered)
                self.assertIn(status, [400, 401, 403],
                    f"{label} should be rejected on /{ep}, got {status}")
            log.info(f"Tampered token ({label}) correctly rejected on all endpoints")
