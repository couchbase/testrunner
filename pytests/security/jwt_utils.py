import time
import uuid
import jwt
import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec


class JWTUtils:
    """Utility class for JWT token generation and configuration"""

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

    def get_jwt_config(self, issuer_name, algorithm, pub_key, token_audience, 
                       token_group_matching_rule, jit_provisioning=True):
        """Get JWT configuration with configurable JIT provisioning
        Args:
            issuer_name: Name of the JWT issuer
            algorithm: JWT signing algorithm
            pub_key: Public key in PEM format
            token_audience: List of audiences for the token
            token_group_matching_rule: List of group matching rules
            jit_provisioning (bool): Enable/disable JIT user provisioning. Default: True
        Returns:
            dict: JWT configuration dictionary
        """
        return {
            "enabled": True,
            "issuers": [
                {
                    "name": issuer_name,
                    "signingAlgorithm": algorithm,
                    "publicKeySource": "pem",
                    "publicKey": pub_key,
                    "jitProvisioning": jit_provisioning,
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audienceHandling": "any",
                    "audiences": token_audience,
                    "groupsClaim": "groups",
                    "groupsMaps": token_group_matching_rule
                }
            ]
        }

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
