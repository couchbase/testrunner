import base64
import json
import secrets
import time
import urllib.parse
import uuid
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec


# Common REST endpoints used with JWT Bearer-token requests.
ENDPOINT_WHOAMI = "whoami"
ENDPOINT_POOLS_DEFAULT = "pools/default"
ENDPOINT_POOLS_BUCKETS = "pools/default/buckets"
ENDPOINT_SETTINGS_JWT = "settings/jwt"


class JWTUtils:
    """Utility class for JWT token generation, configuration, and REST verification.

    Generic across services (Eventing, GSI, N1QL, ...) - any module test suite can
    import this directly: `from pytests.security.jwt_utils import JWTUtils`.
    """

    EC_CURVE_MAP = {
        "ES256": ec.SECP256R1(),
        "ES384": ec.SECP384R1(),
        "ES512": ec.SECP521R1(),
        "ES256K": ec.SECP256K1(),
    }

    def __init__(self, log=None):
        """Initialize JWTUtils
        Args:
            log: Logger instance for logging messages (optional)
        """
        self.log = log

    # ------------------------------------------------------------------
    # Key / secret generation
    # ------------------------------------------------------------------

    def generate_key_pair(self, algorithm: str, key_size: int = 2048):
        """Generate a key pair for JWT signing.
        Args:
            algorithm: JWT signing algorithm (RS256, ES256, etc.)
            key_size: Key size in bits for RSA algorithms (minimum 2048)
        Returns:
            tuple: (private_key_pem, public_key_pem) as strings
        """
        algorithm = algorithm.upper()
        if self.log:
            self.log.info(f"Generating key pair for algorithm: {algorithm}")

        if algorithm.startswith("RS") or algorithm.startswith("PS"):
            if key_size < 2048:
                raise ValueError(f"RSA key size must be 2048 or greater. Got {key_size}")
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size,
            )
        elif algorithm in self.EC_CURVE_MAP:
            curve = self.EC_CURVE_MAP[algorithm]
            private_key = ec.generate_private_key(curve)
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}. Supported: RS*, PS*, ES*")

        if not private_key:
            raise RuntimeError("Error while creating key pair")

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
        if self.log:
            self.log.info("Key pair generated successfully.")
        return private_key_pem.decode(), public_key_pem.decode()

    @staticmethod
    def generate_hmac_secret(byte_length: int = 32) -> str:
        """Generate a cryptographically random HMAC shared secret as a hex string."""
        return secrets.token_hex(byte_length)

    # ------------------------------------------------------------------
    # Config builders
    # ------------------------------------------------------------------

    @staticmethod
    def _build_jwt_issuer_entry(name, algorithm, sub_claim="sub", aud_claim="aud",
                                audiences=None, audience_handling="any",
                                groups_claim="groups", group_maps=None,
                                jit_provisioning=True, public_key_source="pem",
                                pub_key=None, jwks_uri=None, jwks_json=None,
                                shared_secret=None):
        """Build a single issuer entry for the JWT config payload.

        Supports HMAC (sharedSecret) or asymmetric algorithms via
        publicKeySource "pem" / "jwks" / "jwks_uri".
        """
        issuer = {
            "name": name,
            "signingAlgorithm": algorithm,
            "subClaim": sub_claim,
            "audClaim": aud_claim,
            "audienceHandling": audience_handling,
            "audiences": audiences or [],
            "jitProvisioning": jit_provisioning,
        }
        if group_maps is not None:
            issuer["groupsClaim"] = groups_claim
            issuer["groupsMaps"] = group_maps

        if algorithm.upper().startswith("HS"):
            if not shared_secret:
                raise ValueError("shared_secret is required for HMAC algorithm")
            issuer["sharedSecret"] = shared_secret
            return issuer

        issuer["publicKeySource"] = public_key_source
        if public_key_source == "jwks_uri":
            if not jwks_uri:
                raise ValueError("jwks_uri is required when public_key_source='jwks_uri'")
            issuer["jwksUri"] = jwks_uri
        elif public_key_source == "jwks":
            if not jwks_json:
                raise ValueError("jwks_json is required when public_key_source='jwks'")
            issuer["jwks"] = jwks_json
        else:
            if not pub_key:
                raise ValueError("pub_key is required when public_key_source='pem'")
            issuer["publicKey"] = pub_key

        return issuer

    def get_jwt_config(self, issuer_name, algorithm, pub_key=None, token_audience=None,
                       token_group_matching_rule=None, jit_provisioning=True,
                       public_key_source="pem", jwks_uri=None, jwks_json=None,
                       shared_secret=None):
        """Get single-issuer JWT configuration with configurable JIT provisioning.
        Args:
            issuer_name: Name of the JWT issuer
            algorithm: JWT signing algorithm
            pub_key: Public key in PEM format (required when public_key_source="pem")
            token_audience: List of audiences for the token
            token_group_matching_rule: List of group matching rules
            jit_provisioning (bool): Enable/disable JIT user provisioning. Default: True
            public_key_source: "pem" (default), "jwks", or "jwks_uri"
            jwks_uri: JWKS URI (required when public_key_source="jwks_uri")
            jwks_json: JWKS dict (required when public_key_source="jwks")
            shared_secret: HMAC shared secret (required for HS256/HS384/HS512)
        Returns:
            dict: JWT configuration dictionary
        """
        issuer = self._build_jwt_issuer_entry(
            name=issuer_name,
            algorithm=algorithm,
            audiences=token_audience,
            group_maps=token_group_matching_rule,
            jit_provisioning=jit_provisioning,
            public_key_source=public_key_source,
            pub_key=pub_key,
            jwks_uri=jwks_uri,
            jwks_json=jwks_json,
            shared_secret=shared_secret,
        )
        return self.get_jwt_config_from_issuers([issuer])

    @staticmethod
    def get_jwt_config_from_issuers(issuers):
        """Build the top-level JWT config payload from a list of issuer entries."""
        return {"enabled": True, "issuers": issuers or []}

    def get_multi_issuer_jwt_config(self, issuers_list):
        """
        Build JWT configuration with multiple issuers.

        Args:
            issuers_list: List of dicts, each containing:
                - name (str): Issuer name
                - algorithm (str): JWT signing algorithm
                - audiences (list): List of audiences
                - group_maps (list, optional): List of group matching rules
                - jit_provisioning (bool, optional): Default True
                - public_key_source (str, optional): "pem" (default) / "jwks" / "jwks_uri"
                - pub_key / jwks_uri / jwks_json / shared_secret: matching the source above

        Returns:
            dict: JWT configuration dictionary with multiple issuers
        """
        issuers = []
        for issuer in issuers_list or []:
            issuers.append(
                self._build_jwt_issuer_entry(
                    name=issuer["name"],
                    algorithm=issuer["algorithm"],
                    audiences=issuer.get("audiences"),
                    group_maps=issuer.get("group_maps"),
                    jit_provisioning=issuer.get("jit_provisioning", True),
                    public_key_source=issuer.get("public_key_source", "pem"),
                    pub_key=issuer.get("pub_key"),
                    jwks_uri=issuer.get("jwks_uri"),
                    jwks_json=issuer.get("jwks_json"),
                    shared_secret=issuer.get("shared_secret"),
                )
            )
        return self.get_jwt_config_from_issuers(issuers)

    # ------------------------------------------------------------------
    # Token creation
    # ------------------------------------------------------------------

    def create_token(self, issuer_name, user_name, algorithm, private_key,
                     token_audience=None, user_groups=None, ttl=300,
                     nbf_seconds=0):
        """Create a JWT token with the specified parameters
        Args:
            issuer_name: Name of the JWT issuer
            user_name: Username for the token subject
            algorithm: JWT signing algorithm
            private_key: Private key for signing the token
            token_audience: List of audiences for the token (optional)
            user_groups: List of user groups (optional)
            ttl: Time to live in seconds (default: 300)
            nbf_seconds: Not before offset in seconds (default: 0)
        Returns:
            str: Encoded JWT token
        """
        curr_time = int(time.time())
        payload = {
            "iss": issuer_name,
            "sub": user_name,
            "exp": curr_time + ttl,
            "iat": curr_time,
            "nbf": curr_time - nbf_seconds,
            "jti": str(uuid.uuid4()),
        }
        if token_audience:
            payload['aud'] = token_audience
        if user_groups:
            payload['groups'] = user_groups

        if self.log:
            self.log.info(f"Creating JWT token with payload: {json.dumps(payload, indent=2)}")

        jwt_token = jwt.encode(payload=payload,
                               algorithm=algorithm,
                               key=private_key)
        return jwt_token

    # ------------------------------------------------------------------
    # Token introspection / tampering (for negative tests)
    # ------------------------------------------------------------------

    @staticmethod
    def _pad_b64(s):
        """Pad a base64url string to a multiple of 4 for decoding."""
        return s + "=" * ((4 - len(s) % 4) % 4)

    @staticmethod
    def get_payload_from_token(token):
        """Decode JWT payload (no signature verification). Returns dict of claims."""
        parts = token.split(".")
        if len(parts) < 2:
            raise ValueError("Token must have at least 2 parts")
        payload_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(parts[1]))
        return json.loads(payload_bytes.decode("utf-8"))

    @staticmethod
    def get_header_from_token(token):
        """Decode JWT header (contains alg, typ, kid). Returns dict."""
        parts = token.split(".")
        if len(parts) < 1:
            raise ValueError("Token must have at least 1 part")
        header_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(parts[0]))
        return json.loads(header_bytes.decode("utf-8"))

    @staticmethod
    def get_kid_from_token(token):
        """Extract Key ID (kid) from JWT header. Returns kid string or None."""
        return JWTUtils.get_header_from_token(token).get("kid")

    @staticmethod
    def build_tampered_payload_token(valid_token, payload_overrides):
        """Build a token with modified payload and the original (now-invalid) signature."""
        parts = valid_token.split(".")
        if len(parts) != 3:
            raise ValueError("Token must have 3 parts")
        payload_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(parts[1]))
        payload_dict = json.loads(payload_bytes.decode("utf-8"))
        payload_dict.update(payload_overrides or {})
        tampered_b64 = base64.urlsafe_b64encode(
            json.dumps(payload_dict, separators=(",", ":")).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        return f"{parts[0]}.{tampered_b64}.{parts[2]}"

    @staticmethod
    def build_tampered_header_token(valid_token, header_overrides, drop_signature=False):
        """Build a token with a modified header (e.g. alg=none), optionally dropping the signature."""
        parts = valid_token.split(".")
        if len(parts) != 3:
            raise ValueError("Token must have 3 parts")
        header_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(parts[0]))
        header_dict = json.loads(header_bytes.decode("utf-8"))
        header_dict.update(header_overrides or {})
        tampered_b64 = base64.urlsafe_b64encode(
            json.dumps(header_dict, separators=(",", ":")).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        if drop_signature:
            return f"{tampered_b64}.{parts[1]}."
        return f"{tampered_b64}.{parts[1]}.{parts[2]}"

    # ------------------------------------------------------------------
    # REST verification (generic Bearer-token requests)
    # ------------------------------------------------------------------

    @staticmethod
    def status_code_from_header(header):
        """Extract the HTTP status code from a testrunner REST response object."""
        if header is None:
            return None
        try:
            return header['status']
        except (TypeError, KeyError):
            pass
        return getattr(header, 'status', None) or getattr(header, 'status_code', None)

    @staticmethod
    def _is_success_status(status_code):
        if status_code is None:
            return False
        try:
            return 200 <= int(status_code) < 300
        except (TypeError, ValueError):
            return False

    def verify_token_rest(self, rest_connection, token, endpoint=ENDPOINT_POOLS_BUCKETS,
                          method="GET", params=""):
        """
        Make a JWT Bearer-token REST request against any endpoint.

        Args:
            rest_connection: RestConnection instance (must support request_with_jwt_bearer)
            token: JWT bearer token to use
            endpoint: REST endpoint path (default: pools/default/buckets)
            method: HTTP method (default: GET)
            params: request body / query string

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        status, content, header = rest_connection.request_with_jwt_bearer(token, endpoint, method, params)
        status_code = self.status_code_from_header(header)
        return status, status_code, content

    def verify_token_whoami_rest(self, rest_connection, token):
        """
        Verify a JWT token against /whoami - accessible to any authenticated user,
        so it validates authentication independent of RBAC roles.

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        return self.verify_token_rest(rest_connection, token, endpoint=ENDPOINT_WHOAMI)

    def get_user_info_from_whoami(self, rest_connection, token):
        """
        Get user information from /whoami endpoint.

        Returns:
            dict or None
        """
        ok, status_code, content = self.verify_token_whoami_rest(rest_connection, token)
        if not ok or not self._is_success_status(status_code):
            return None
        try:
            parsed = self.parse_jwt_config_content(content)
            user_id = parsed.get("id")
            domain = parsed.get("domain")
            if user_id and domain and domain != "anonymous":
                return parsed
            return None
        except Exception as e:
            if self.log:
                self.log.warning(f"get_user_info_from_whoami: parse failed - {e}")
            return None

    # ------------------------------------------------------------------
    # /settings/jwt configuration (GET / PUT / disable)
    # ------------------------------------------------------------------

    @staticmethod
    def parse_jwt_config_content(content):
        """Parse a JWT config REST response body (JSON bytes/string or dict) into a dict."""
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")
        if isinstance(content, str):
            return json.loads(content)
        return content

    def put_jwt_config(self, rest_connection, config):
        """
        PUT JWT configuration to the cluster.

        Returns:
            tuple: (status_code, content)
        """
        if self.log:
            self.log.info(f"JWT config payload: {json.dumps(config, indent=2)}")
        status, content, header = rest_connection.create_jwt_with_config(config)
        status_code = self.status_code_from_header(header)
        if not status:
            raise Exception(f"Failed to PUT /settings/jwt. status={status_code} content={content}")
        return status_code, content

    def get_jwt_config_settings(self, rest_connection, expected_status_code=200):
        """
        GET JWT configuration from the cluster.

        Args:
            rest_connection: RestConnection instance
            expected_status_code: Expected HTTP status code (default: 200). If None, no check.

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        status, content, header = rest_connection.get_jwt_config()
        status_code = self.status_code_from_header(header)
        if expected_status_code is not None and status_code is not None:
            assert int(status_code) == int(expected_status_code), (
                f"Expected GET /settings/jwt status {expected_status_code}, got {status_code}"
            )
        return status, status_code, content

    def disable_jwt(self, rest_connection):
        """
        Disable JWT authentication on the cluster without discarding issuer config.
        Uses GET-then-PUT so the payload includes required fields (e.g. issuers).
        """
        try:
            ok, _, content = self.get_jwt_config_settings(rest_connection, expected_status_code=None)
            if ok and content:
                config = self.parse_jwt_config_content(content)
                config["enabled"] = False
                if "issuers" not in config:
                    config["issuers"] = []
                self.put_jwt_config(rest_connection, config)
                return
        except Exception as e:
            if self.log:
                self.log.warning(f"GET before disable failed: {e}")
        self.put_jwt_config(rest_connection, {"enabled": False, "issuers": []})

    # ------------------------------------------------------------------
    # External RBAC user helpers
    # ------------------------------------------------------------------

    def create_external_user(self, rest_connection, user_name, roles=None, groups=None):
        """
        Create an external RBAC user (required for non-JIT-provisioned JWT issuers).

        Pass `roles` to assign roles directly, or `groups` to assign via group
        membership (the group must already exist with its own roles).
        """
        payload_dict = {"name": user_name}
        if roles is not None:
            payload_dict["roles"] = roles
        if groups is not None:
            payload_dict["groups"] = groups
        payload = urllib.parse.urlencode(payload_dict)
        result = rest_connection.add_external_user(user_name, payload)
        if self.log:
            self.log.info(f"Created external user {user_name} (roles={roles}, groups={groups})")
        return result

    @staticmethod
    def get_external_user(rest_connection, user_name):
        """Get external user info from RBAC, or None if not found."""
        return rest_connection.get_external_user(user_name)

    def delete_external_user(self, rest_connection, user_name):
        """Delete an external user from RBAC. Safe to call even if it doesn't exist."""
        if not user_name:
            return None
        try:
            return rest_connection.delete_external_user(user_name)
        except Exception as e:
            if self.log:
                self.log.warning(f"Error deleting external user {user_name}: {e}")
            return None

    # ------------------------------------------------------------------
    # Assertions
    # ------------------------------------------------------------------

    @staticmethod
    def assert_auth_fails(status_code, message):
        """Assert that authentication failed (non-2xx or no status)."""
        assert status_code is None or not JWTUtils._is_success_status(status_code), message

    @staticmethod
    def assert_auth_succeeds(ok, status_code, message):
        """Assert that authentication succeeded."""
        assert ok and JWTUtils._is_success_status(status_code), message

    @staticmethod
    def assert_unauthorized_status(status_code, message):
        """Assert that status code indicates unauthorized (401 or 403)."""
        if status_code is not None:
            assert int(status_code) in (401, 403), message

    @staticmethod
    def assert_success_status(status_code, message):
        """Assert that status code indicates HTTP success (2xx)."""
        assert JWTUtils._is_success_status(status_code), message

    @staticmethod
    def assert_external_identity(whoami, username, domain="external", prefix=""):
        """Assert /whoami identity fields (id and domain)."""
        p = f"{prefix}: " if prefix else ""
        assert whoami is not None, f"{p}Should get user info from /whoami"
        assert whoami.get("id") == username, (
            f"{p}Expected user id '{username}', got '{whoami.get('id')}'"
        )
        assert whoami.get("domain") == domain, (
            f"{p}Expected domain '{domain}', got '{whoami.get('domain')}'"
        )

    @staticmethod
    def assert_role_present(whoami, role_name):
        """Assert that role_name is present in /whoami roles. Returns full role list."""
        roles = whoami.get("roles", [])
        role_names = [r.get("role") for r in roles if isinstance(r, dict)]
        assert role_name in role_names, f"Expected '{role_name}' role in {role_names}"
        return role_names

    def verify_malformed_token_rejection(self, rest_connection, token, token_type):
        """
        Verify that a malformed/invalid token is rejected with 400 or 401.

        Args:
            rest_connection: RestConnection instance
            token: The malformed token string
            token_type: Description of the token type for logging/errors
        """
        ok, status_code, content = self.verify_token_rest(rest_connection, token)
        assert not (ok and self._is_success_status(status_code)), (
            f"Expected malformed token ({token_type}) to be rejected but request succeeded"
        )
        if status_code is not None:
            assert int(status_code) in (400, 401), (
                f"Unexpected status for malformed token ({token_type}): {status_code}"
            )
        if self.log:
            self.log.info(f"Malformed token ({token_type}) correctly rejected, status={status_code}")
