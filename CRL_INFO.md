# CRL (Certificate Revocation List) — QA Automation Reference

**Audience:** anyone building CRL test automation in this repo. This is the concrete, up-to-date
reference for the REST contract and the helper classes already available — `RestConnection`'s CRL
methods, `pytests/security/crl_utils.py::CRLUtils`, and `pytests/security/crl_base.py::CRLBase`.

**Sources of truth (ns-server erlang):**

| Area | File |
|---|---|
| REST handlers | `apps/ns_server/src/menelaus_web_crl.erl` |
| Route table / RBAC permissions | `apps/ns_server/src/menelaus_web.erl` (search `"crl"`) |
| Config owner / defaults / file lifecycle | `apps/ns_server/src/cb_crl_manager.erl` |
| Runtime revocation check (`verify_fun`) | `apps/ns_server/src/cb_crl.erl` |
| CRL cache | `apps/ns_server/src/cb_crl_cache.erl` |
| Internal cbauth integration endpoint | `apps/ns_server/src/menelaus_cbauth.erl` (`handle_crls_validate_post/1`) |

---

## 1. REST API endpoints

| Method | Path | RestConnection method | RBAC permission |
|---|---|---|---|
| GET | `/settings/crl` | `rest.get_crl_settings()` | `{[admin, security], read}` |
| POST | `/settings/crl` | `rest.post_crl_settings(payload)` | `{[admin, security], write}` |
| GET | `/settings/crl/files` | `rest.get_crl_files()` | `{[admin, security], read}` |
| POST | `/settings/crl/files` | `rest.upload_crl_file(filename, content_bytes, timeout=300)` | `{[admin, security], write}` |
| DELETE | `/settings/crl/files/:filename` | `rest.delete_crl_file(filename)` | `{[admin, security], write}` |
| GET | `/settings/crl/diagnostics/status` | `rest.get_diagnostics_status(nodes=None)` | `{[admin, security], read}` |
| POST | `/settings/crl/diagnostics/status` | `rest.post_diagnostics_status(nodes=None)` | `{[admin, security], read}` |
| POST | `/settings/crl/diagnostics/validate` | `rest.post_diagnostics_validate(policy=None, certs=None)` | `{[admin, security], read}` (note: read perm despite POST — bypasses configured policy) |
| POST | `/node/controller/reloadCrl` | `rest.reload_crl()` | `{[admin, security], write}` |
| POST | `/_cbauth/crlsValidate` | `rest.cbauth_crls_validate(certs, scope)` | `{[admin, internal], all}` — internal only |

Every method returns `(status_bool, content, response)`.

**Feature gating**, applied to every endpoint above except `/_cbauth/crlsValidate`:
1. Enterprise Edition only — Community Edition gets nothing. `CRLBase._require_crl_supported()`
   enforces this before any test body runs (`self.rest.is_enterprise_edition()`).
2. Cluster compat version Totoro (8.1.0)+ — below that, every endpoint returns **HTTP 404** with
   body `"CRL feature not yet enabled in this cluster"`. Not separately gated in `CRLBase` — this
   automation assumes it always runs against Totoro+ clusters; write a dedicated test with a
   deliberately older cluster if you need to verify this 404 behavior itself.

---

## 2. `GET`/`POST /settings/crl` — settings schema

```json
{
  "policyPerScope": { "clientAuth": "Disabled", "nodeToNode": "Disabled" },
  "dirPollIntervalMs": 60000,
  "directory": "<data-dir>/inbox/crls",
  "checkIntermediateCerts": false,
  "urls": [],
  "urlPollIntervalMs": 3600000
}
```

| Field | Type | Default | Validation |
|---|---|---|---|
| `policyPerScope` | object `{scope: mode}` | `{"clientAuth":"Disabled","nodeToNode":"Disabled"}` | Partial — supplying only one scope leaves the other untouched. Scopes: `clientAuth`, `nodeToNode`. Modes: `Disabled`, `Permissive`, `Require` (no `Strict` — see §3) |
| `directory` | string | `<data-dir>/inbox/crls` | POSTing `""` resets to default |
| `dirPollIntervalMs` | integer | `60000` | `1000 ≤ x ≤ 86400000` (1s–24h) |
| `checkIntermediateCerts` | boolean | `false` | When `false`, self-signed/root certs are never CRL-checked |
| `urls` | array of strings | `[]` | Each must be `http`/`https`; capped at 100; duplicates silently deduplicated |
| `urlPollIntervalMs` | integer | `3600000` | `1000 ≤ x ≤ 86400000` (1s–24h) |

Any field not in this table is rejected (400), not silently ignored. On successful POST, the
handler returns the **full merged config**, not an echo of what was posted. POST failure:
`{"error": "<reason>"}`, HTTP 400.

```python
status, content, _ = rest.post_crl_settings({"policyPerScope": {"clientAuth": "Require"}})
```

---

## 3. `GET`/`POST /settings/crl/files` — file lifecycle

**Upload:** `rest.upload_crl_file(filename, content_bytes, timeout=300)` — multipart/form-data
under the hood. Filename must be 1-255 chars of `[a-zA-Z0-9._-]`, not `.`/`..`. Server-side CRL
parse/signature validation happens synchronously — invalid CRLs are rejected with 400 and a
reason (`"CRL validation failed: ..."`, `"Failed to decode CRL: ..."`, `"Invalid CRL"`, etc.).
Response on success: the full updated file list.

`timeout` matters — large CRLs (200k+ revoked entries) can take up to ~10 minutes server-side
(observed: ~2.6-2.9ms per revoked entry, ~586s for 200k entries). Pass a larger value for
large-CRL tests; the default 300s is fine for normal-sized CRLs.

**List:** `rest.get_crl_files()` — array of
`{filename, checksum, uploadTimestamp, entries: [{issuer, thisUpdate, nextUpdate, crlNumber}]}`.
`crlNumber` is `null` (not omitted) when the CRL has no CRL Number extension.

**Delete:** `rest.delete_crl_file(filename)` — `{}`/200 on success, `{"error": "CRL file not
found"}`/404 if the filename doesn't exist.

```python
crl_pem = CRLUtils.build_crl(ca_cert, ca_key, revoked_serials=[serial], crl_number=1)
status, content, _ = rest.upload_crl_file("revoked.pem", crl_pem)
status, files, _ = rest.get_crl_files()
status, content, _ = rest.delete_crl_file("revoked.pem")
```

---

## 4. `POST /settings/crl/diagnostics/validate` — admin diagnostic endpoint

Bypasses the configured policy entirely; evaluates against a caller-supplied test policy instead.
`rest.post_diagnostics_validate(policy=None, certs=None)`.

- `policy`: `"Permissive"` or `"Require"` (defaults to `"Require"`; `"Disabled"` is rejected —
  nothing to test with a disabled policy)
- `certs`: optional list of PEM strings or base64-encoded DER strings (≤100). Omit for
  cluster-cert mode — checks every node's own stored client+node cert chains.

**Status vocabulary — 4 values only:** `"valid"`, `"revoked"`, `"undetermined"`, `"failed"`, each
with an optional free-text `details` string. Do not assert on any other status string — there is
no 8-value enum here despite what older PRD drafts may say.

A self-signed root cert is always `"valid"` with `details: "self-signed root; not CRL-checked"`.

---

## 5. `GET`/`POST /settings/crl/diagnostics/status`

`rest.get_diagnostics_status(nodes=None)` / `rest.post_diagnostics_status(nodes=None)` (use POST
when the node list is too long for a query string). Response — object keyed by node hostname,
each value an array of per-file status objects:

```json
{
  "172.23.220.125:8091": [
    {
      "filename": "ca1.crl",
      "source": "uploaded",
      "cacheStatus": "active",
      "entries": [{"issuer": "...", "status": "active", "thisUpdate": "...",
                   "nextUpdate": "...", "checksum": "...", "crlNumber": 1}],
      "lastReload": {"result": "loaded", "time": "...", "errors": []}
    }
  ]
}
```

`source` enum: `localDir | uploaded | generated | url`.
`cacheStatus`/`entries[].status` enum: `active | expired | notYetValid | untrusted | invalid | notLoaded`.
`lastReload.result` enum: `loaded | failed | notAttempted | uploaded | notDownloaded | checksumMismatch | readError`.

Per-node RPC failures (timeout/down) degrade gracefully — that node's entry becomes
`{"error": "<reason>"}` rather than failing the whole request, when nodes are explicitly named.
Unknown hostnames fail the whole request with 400.

---

## 6. `POST /node/controller/reloadCrl`

`rest.reload_crl()` — no body. Forces immediate reload on the **local node only** (not
cluster-wide). Returns the same per-file status shape as one entry of `diagnostics/status`.

---

## 7. `POST /_cbauth/crlsValidate` (internal)

`rest.cbauth_crls_validate(certs, scope)` — used by cbauth-registered GO services (query,
analytics, indexer, goxdcr). Validates against the **currently configured** policy (unlike
`diagnostics/validate`, which bypasses it). Requires `{[admin, internal], all}` — Administrator
creds satisfy it.

- `certs`: list of base64-encoded DER strings, leaf first, chain after (≤100)
- `scope`: `"clientAuth"` or `"nodeToNode"`

Response: `{"statuses": [{"status": ..., "subject": ..., "details": ..., "expiration": ...}, ...]}`
— same 4-value status set as §4.

---

## 8. What's explicitly NOT implemented (don't write assertions expecting these)

- `"Strict"` policy mode — only `Disabled`/`Permissive`/`Require` exist.
- `serverDiagnostics`, `Xdcr`, `sdkServerValidation` as first-class `policyPerScope` scopes — only
  `clientAuth`/`nodeToNode` are real enforced scopes.
- Audit events for any CRL lifecycle action (upload/delete/policy change) — zero audit event IDs.
- CRL expiration health-warning alerts.
- A per-connection/per-cert decision cache — only loaded-CRL entries are cached, not revocation
  decisions. Don't be surprised by unoptimized latency on the revocation check path.
- Dry-run impact analysis, active session termination on revoke, CRL Distribution Point
  auto-fetch — all future-phase, not built.

---

## 9. `CRLUtils` — helper API reference

`pytests/security/crl_utils.py`. All methods are static — instantiate with `CRLUtils(log=self.log)`
only if you want `self.log` available for future extension; none of the current methods use it.

**Crypto (in-memory, `cryptography` library — RFC 5280 X.509/CRL construction, no network/SSH):**

| Method | Returns | Notes |
|---|---|---|
| `generate_ca(cn, key_algorithm="rsa2048", valid_days=3650)` | `(ca_cert, ca_key)` | `key_algorithm`: `"rsa2048"` or `"ecdsa_p256"` — the two most common real-world Couchbase customer PKI key types (RSA for legacy/enterprise AD CS, ECDSA for Vault PKI/cert-manager/cloud-native private CAs) |
| `generate_leaf_cert(ca_cert, ca_key, cn, key_algorithm="rsa2048", valid_days=825, extended_key_usage=None, crl_distribution_url=None, dns_names=None)` | `(cert, key, serial)` | `key_algorithm` is independent of the CA's own algorithm. `dns_names` for node certs (SAN). `serial` is what you later revoke |
| `build_crl(ca_cert, ca_key, revoked_serials=None, this_update=None, next_update=None, crl_number=None, expired=False)` | `bytes` (PEM) | `expired=True` sets `next_update` 30 days in the past — note the server rejects genuinely-expired CRLs at upload time unless `allow_expired_crls` is set via `diag_eval` |
| `cert_to_pem(cert)` / `key_to_pem(key)` | `bytes` | PEM serialization |

**Convenience helpers:**

| Method | Notes |
|---|---|
| `revoke_and_upload(rest, ca_cert, ca_key, serials, filename, timeout=300, **crl_kwargs)` | `build_crl()` + `rest.upload_crl_file()` in one call — the single most-used helper across CRL tests. `serials` can be a single int or a list |
| `perform_mtls_handshake(host, port, client_cert_path, client_key_path, ca_cert_path, path="/whoami", timeout=30)` | Real mTLS via plain `requests`. Raises `requests.exceptions.SSLError` on TLS-layer rejection (revoked cert) — this is a connection-level failure, not an HTTP status code, so catch it explicitly |
| `parse_content(content)` | Tolerant JSON parse of bytes/str/dict |
| `assert_settings_equal(actual, expected_subset)` | Raises `AssertionError` on any mismatched key |
| `assert_diagnostics_entry(entry, expected_status=None, expected_source=None)` | Raises `AssertionError` on mismatch |

---

## 10. `CRLBase` — test base class reference

`pytests/security/crl_base.py :: CRLBase(BaseTestCase)`. Extend this for any CRL test class —
`setUp`/`tearDown` handle all the boilerplate:

**`setUp()` provides:**
- `self.crl_utils` — a `CRLUtils` instance
- `self.rest` — a `RestConnection(self.master)`
- `self.ca_cert`, `self.ca_key` — a freshly generated `TestCA1` (RSA 2048), already trusted on the
  cluster (written to the node's real `inbox/CA` directory and loaded via
  `rest.load_trusted_CAs()`)
- Fails the test immediately (`self.fail(...)`) if the cluster isn't Enterprise Edition

**`tearDown()` provides** (each step in its own try/except, so one failure doesn't block the rest):
- Deletes every CRL file tracked via `self._track_uploaded_file(filename)`
- Resets `policyPerScope` to `Disabled` for both scopes
- Disables `clientCertAuth`
- Deletes every RBAC user created via `self._create_rbac_test_user(username, role)`

**Other available helpers:**
- `self._trust_ca_on_cluster(ca_cert, server=None)` — trust an additional CA (e.g. a second test
  CA, or trusting the default `TestCA1` on a different node in a multi-node test). Resolves the
  real install path via `x509main(host=server).install_path` (OS-detected, or the node's actual
  configured data directory via `diag_eval` for Linux) — **do not** guess at shell attributes for
  this; `x509main` already solves it correctly.
- `self._create_rbac_test_user(username, role, password="Couchbase@1234")` → `(username, password)`

---

## 11. Automation helper blueprints

**Generate a CA and a leaf cert, revoke it, confirm rejection:**

```python
import os
import tempfile

import requests

from pytests.security.crl_base import CRLBase


class MyCRLTest(CRLBase):
    def test_revoke_client_cert(self):
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "my-test-client"
        )
        username, password = self._create_rbac_test_user("my-test-client", ["ro_admin"])

        self.rest.client_cert_auth(
            state="enable",
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}],
        )

        self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Require"}})

        # Baseline: upload a non-revoking CRL, confirm the cert authenticates.
        status, _ = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], "baseline.pem", crl_number=1
        )
        self._track_uploaded_file("baseline.pem")

        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        ca_path = self._write_temp_pem(self.crl_utils.cert_to_pem(self.ca_cert))

        response = self.crl_utils.perform_mtls_handshake(
            self.master.ip, 18091, cert_path, key_path, ca_path
        )
        assert response.status_code == 200

        # Revoke, confirm rejection.
        status, _ = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, "baseline.pem", crl_number=2
        )
        self.rest.reload_crl()

        try:
            self.crl_utils.perform_mtls_handshake(
                self.master.ip, 18091, cert_path, key_path, ca_path
            )
            raise AssertionError("expected SSLError, connection succeeded instead")
        except requests.exceptions.SSLError:
            pass  # expected — revoked cert rejected at the TLS layer

    def _write_temp_pem(self, pem_bytes):
        import tempfile, os
        fd, path = tempfile.mkstemp(suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(pem_bytes)
        self.addCleanup(os.remove, path)
        return path
```

**Settings round-trip:**

```python
status, settings, _ = self.rest.get_crl_settings()
status, updated, _ = self.rest.post_crl_settings({"checkIntermediateCerts": True})
self.crl_utils.assert_settings_equal(
    self.crl_utils.parse_content(updated), {"checkIntermediateCerts": True}
)
```

**RSA vs ECDSA coverage:**

```python
from pytests.security.crl_utils import KEY_ALGORITHMS  # {"rsa2048", "ecdsa_p256"}

for key_algorithm in KEY_ALGORITHMS:
    ca_cert, ca_key = self.crl_utils.generate_ca(f"TestCA-{key_algorithm}", key_algorithm=key_algorithm)
    # ... exercise the same flow for each algorithm
```

---

## 12. Test resource inventory — quick cheat-sheet

| Need | Use |
|---|---|
| A trusted test CA, already loaded on the cluster | `self.ca_cert`/`self.ca_key` from `CRLBase.setUp()` |
| A second/custom CA (e.g. multi-CA tests) | `self.crl_utils.generate_ca(cn, key_algorithm=...)` + `self._trust_ca_on_cluster(ca_cert)` |
| A leaf cert to revoke | `self.crl_utils.generate_leaf_cert(ca_cert, ca_key, cn, ...)` |
| A CRL (empty or revoking specific serials) | `self.crl_utils.build_crl(...)` or the combined `revoke_and_upload(...)` |
| Real mTLS handshake (success or revoked-rejection) | `self.crl_utils.perform_mtls_handshake(...)` |
| An RBAC user matching a cert's CN | `self._create_rbac_test_user(username, role)` |
| Cleanup tracking for uploaded files | `self._track_uploaded_file(filename)` — auto-deleted in `tearDown` |
| Node-to-node PKI setup (custom node/client certs) | `x509main`/`x509_multiple_CA_util.py` (existing, proven — not CRL-specific) |

---

## 13. Design notes worth knowing

**Why RSA 2048 + ECDSA P-256, not real Vault/AWS-PCA/cert-manager integration?** Couchbase's CRL
verification is pure RFC 5280 X.509 logic — it can't distinguish which CA vendor issued a
cert/CRL. What actually varies across real customer PKIs *and matters for correctness* is key
algorithm and chain depth, not vendor/provenance. Standing up real external CA infrastructure for
test fixtures would be a large ops lift for no additional correctness signal.

**The CA-install-path pitfall:** the install path where a CA must be placed for
`load_trusted_CAs()` to pick it up is **not** a fixed constant across every server build/OS — use
`x509main(host=server).install_path` (which itself resolves via `diag_eval` on Linux, not a
hardcoded path) rather than assuming `/opt/couchbase/var/lib/couchbase/`. A prior version of this
automation guessed at nonexistent shell-connection attributes for this and silently wrote the CA
to the wrong location — the upload then failed with `"CRL issuer not trusted"` since the server
never actually loaded it. `CRLBase._trust_ca_on_cluster` already avoids this; don't reintroduce the
guess if you're writing your own variant.

**Why no `RestConnection` wrapper/shim object for CRL calls?** Every CRL REST method lives directly
on `RestConnection` (same convention as JWT's `create_jwt_with_config`/`get_jwt_config`/etc.) — call
them straight off whatever `RestConnection` instance your test already has (`self.rest` in
`CRLBase`). There is no separate wrapper class to construct.
