import datetime
import ipaddress
import json

import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID

# Key algorithms covering the two most common real-world Couchbase PKI shapes:
# RSA 2048 (still the default for legacy/enterprise AD CS setups) and ECDSA
# P-256 (default for Vault PKI, cert-manager, and cloud-native private CAs).
KEY_ALGORITHMS = {"rsa2048", "ecdsa_p256"}

POLICY_MODES = {"Disabled", "Permissive", "Require"}
SCOPES = {"clientAuth", "nodeToNode"}
CACHE_STATUS_VALUES = {
    "active", "expired", "notYetValid", "untrusted", "invalid", "notLoaded",
}
RELOAD_RESULT_VALUES = {
    "loaded", "failed", "notAttempted", "uploaded", "notDownloaded",
    "checksumMismatch", "readError",
}
DIAGNOSTIC_STATUS_VALUES = {"valid", "revoked", "undetermined", "failed"}


class CRLUtils:
    """
    Utility class for CRL (Certificate Revocation List) test automation.
    See CRL_INFO.md for the full REST/API reference this class implements
    against.

    Covers in-memory CA/leaf-cert generation and CRL building (via
    `cryptography`, RFC 5280), plus a small set of convenience helpers on top
    of RestConnection's native CRL methods (get_crl_settings, post_crl_settings,
    get_crl_files, upload_crl_file, delete_crl_file, get_diagnostics_status,
    post_diagnostics_status, post_diagnostics_validate, reload_crl,
    cbauth_crls_validate — call those directly on a RestConnection instance,
    e.g. `rest.get_crl_settings()`, the same way JWT tests call
    `self.rest.get_jwt_config()`. This class does not wrap them.
    """

    def __init__(self, log=None):
        self.log = log

    # ── Crypto: CA / leaf certs ──────────────────────────────────────────────

    @staticmethod
    def _generate_private_key(key_algorithm):
        """Return a private key for one of KEY_ALGORITHMS."""
        if key_algorithm == "rsa2048":
            return rsa.generate_private_key(public_exponent=65537, key_size=2048)
        if key_algorithm == "ecdsa_p256":
            return ec.generate_private_key(ec.SECP256R1())
        raise ValueError(
            f"Unsupported key_algorithm: {key_algorithm!r}, expected one of {KEY_ALGORITHMS}"
        )

    @staticmethod
    def generate_ca(cn, key_algorithm="rsa2048", valid_days=3650):
        """
        Generate a self-signed CA cert/key pair, in memory.

        Args:
            key_algorithm: one of KEY_ALGORITHMS — "rsa2048" (default, matches
                legacy/enterprise AD CS) or "ecdsa_p256" (matches Vault PKI,
                cert-manager, cloud-native private CAs)

        Returns:
            tuple: (ca_cert: x509.Certificate, ca_key)
        """
        key = CRLUtils._generate_private_key(key_algorithm)
        subject = issuer = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, cn)]
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=False, content_commitment=False,
                    key_encipherment=False, data_encipherment=False,
                    key_agreement=False, key_cert_sign=True, crl_sign=True,
                    encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        return cert, key

    @staticmethod
    def generate_leaf_cert(ca_cert, ca_key, cn, key_algorithm="rsa2048",
                            valid_days=825, extended_key_usage=None,
                            crl_distribution_url=None, dns_names=None):
        """
        Generate a leaf cert signed by ca_cert/ca_key, in memory.

        Args:
            key_algorithm: one of KEY_ALGORITHMS — "rsa2048" (default) or
                "ecdsa_p256". Independent of ca_key's own algorithm — a CA can
                sign leaf certs of either algorithm.
            extended_key_usage: list of x509.oid.ExtendedKeyUsageOID, defaults
                to CLIENT_AUTH
            crl_distribution_url: optional http(s) URL to embed as a
                CRLDistributionPoints extension (informational only —
                Couchbase does not auto-fetch from this, see CRL_INFO.md)
            dns_names: optional list of SAN DNS names (needed for node certs)

        Returns:
            tuple: (cert: x509.Certificate, key, serial: int)
        """
        if extended_key_usage is None:
            extended_key_usage = [x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]
        key = CRLUtils._generate_private_key(key_algorithm)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
        now = datetime.datetime.now(datetime.timezone.utc)
        serial = x509.random_serial_number()
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(serial)
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True, content_commitment=False,
                    key_encipherment=True, data_encipherment=False,
                    key_agreement=False, key_cert_sign=False, crl_sign=False,
                    encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(x509.ExtendedKeyUsage(extended_key_usage), critical=False)
        )
        if dns_names:
            names = []
            for name in dns_names:
                try:
                    names.append(x509.IPAddress(ipaddress.ip_address(name)))
                except ValueError:
                    names.append(x509.DNSName(name))
            builder = builder.add_extension(x509.SubjectAlternativeName(names), critical=False)
        if crl_distribution_url:
            dp = x509.DistributionPoint(
                full_name=[x509.UniformResourceIdentifier(crl_distribution_url)],
                relative_name=None, reasons=None, crl_issuer=None,
            )
            builder = builder.add_extension(x509.CRLDistributionPoints([dp]), critical=False)
        cert = builder.sign(ca_key, hashes.SHA256())
        return cert, key, serial

    @staticmethod
    def build_crl(ca_cert, ca_key, revoked_serials=None, this_update=None,
                  next_update=None, crl_number=None, expired=False):
        """
        Build and sign a CRL for ca_cert/ca_key.

        Args:
            revoked_serials: list of int serial numbers to mark revoked
            this_update / next_update: datetime, defaults to now / now+30d
            crl_number: optional int, adds a CRLNumber extension
            expired: if True and next_update not given, sets next_update to
                30 days in the past (server rejects genuinely-expired CRLs at
                upload time unless allow_expired_crls is set via diag/eval —
                see CRL_INFO.md)

        Returns:
            bytes: PEM-encoded CRL
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        if this_update is None:
            this_update = (
                now - datetime.timedelta(days=30) if expired
                else now - datetime.timedelta(days=1)
            )
        if next_update is None:
            next_update = (
                now - datetime.timedelta(days=1) if expired
                else now + datetime.timedelta(days=30)
            )
        builder = x509.CertificateRevocationListBuilder().issuer_name(
            ca_cert.subject
        ).last_update(this_update).next_update(next_update)
        for serial in (revoked_serials or []):
            revoked = (
                x509.RevokedCertificateBuilder()
                .serial_number(serial)
                .revocation_date(now)
                .build()
            )
            builder = builder.add_revoked_certificate(revoked)
        if crl_number is not None:
            builder = builder.add_extension(
                x509.CRLNumber(crl_number), critical=False
            )
        crl = builder.sign(private_key=ca_key, algorithm=hashes.SHA256())
        return crl.public_bytes(serialization.Encoding.PEM)

    @staticmethod
    def cert_to_pem(cert):
        return cert.public_bytes(serialization.Encoding.PEM)

    @staticmethod
    def key_to_pem(key):
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    # ── Convenience helpers ──────────────────────────────────────────────────

    @staticmethod
    def parse_content(content):
        """Return parsed JSON, tolerating bytes/str/dict input."""
        if content is None:
            return None
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8", "replace")
        if isinstance(content, str):
            try:
                return json.loads(content)
            except ValueError:
                return content
        return content

    @staticmethod
    def revoke_and_upload(rest, ca_cert, ca_key, serials, filename,
                           timeout=300, **crl_kwargs):
        """
        Convenience: build_crl() revoking `serials` (int or list of int) then
        rest.upload_crl_file() it. The single most-used helper across CRL tests.

        Returns (status_bool, content).
        """
        if isinstance(serials, int):
            serials = [serials]
        pem = CRLUtils.build_crl(ca_cert, ca_key, revoked_serials=serials, **crl_kwargs)
        status, content, _ = rest.upload_crl_file(filename, pem, timeout=timeout)
        return status, content

    @staticmethod
    def perform_mtls_handshake(host, port, client_cert_path, client_key_path,
                                ca_cert_path, path="/whoami", timeout=30):
        """
        Perform a real mTLS handshake using plain `requests` with a client cert.

        Args:
            host/port: target node, e.g. "18091" for the TLS mgmt port
            client_cert_path/client_key_path: filesystem paths to PEM files
            ca_cert_path: filesystem path to the CA cert PEM to verify against

        Returns:
            requests.Response on a successful handshake, or raises
            requests.exceptions.SSLError on TLS-layer rejection (e.g. a
            revoked cert — callers should catch this explicitly, since a
            revoked-cert rejection is a connection-level failure, not an
            HTTP status code — see CRL_INFO.md).
        """
        url = "https://{0}:{1}{2}".format(host, port, path)
        return requests.get(
            url, cert=(client_cert_path, client_key_path), verify=ca_cert_path,
            timeout=timeout,
        )

    # ── Assert helpers ───────────────────────────────────────────────────────

    @staticmethod
    def assert_settings_equal(actual, expected_subset):
        """Assert every key/value in expected_subset matches actual."""
        for key, value in expected_subset.items():
            if actual.get(key) != value:
                raise AssertionError(
                    f"CRL settings mismatch for {key!r}: "
                    f"expected {value!r}, got {actual.get(key)!r}"
                )

    @staticmethod
    def assert_diagnostics_entry(entry, expected_status=None, expected_source=None):
        """Assert a single diagnostics/status file entry matches expectations."""
        if expected_status is not None and entry.get("cacheStatus") != expected_status:
            raise AssertionError(
                f"Expected cacheStatus={expected_status!r}, "
                f"got {entry.get('cacheStatus')!r}"
            )
        if expected_source is not None and entry.get("source") != expected_source:
            raise AssertionError(
                f"Expected source={expected_source!r}, got {entry.get('source')!r}"
            )
