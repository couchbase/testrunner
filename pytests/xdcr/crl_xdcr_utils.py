"""Helpers for the XDCR CRL suite.

Kept separate from crlXDCR.py so the test module carries test logic only.
"""
import datetime
import os
import re
import shlex
import tempfile
import time

from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from xdcr.xdcrnewbasetests import NodeHelper

INBOX_SUBDIR = "inbox"
CLIENT_CHAIN_FILE = "client_chain.pem"
CLIENT_KEY_FILE = "client_pkey.key"
CA_SUBDIR = "CA"
CA_FILE = "ca.pem"

# goxdcr's own leading log timestamp. A multi-line log entry's
# CONTINUATION fragment (starting 'map[...]', 'sendPeerToPeerReq(...)',
# etc.) has no such prefix, and must be skipped rather than have its own
# leading token read as a time (M8).
_TIMESTAMP_PREFIX = re.compile(r"^\d{4}-\d\d-\d\dT")


def _parse_iso_timestamp(token):
    """Parse an ISO-8601 timestamp into a timezone-AWARE datetime.

    Accepts both spellings this suite has to reconcile:
    `2026-09-14T06:00:14.123Z` (UTC, what `_read_node_started_utc` produces)
    and `2026-09-13T23:09:54.969-07:00` (local time plus offset, what goxdcr
    actually writes). A token with no zone at all is read as UTC.

    Python 3.10's `datetime.fromisoformat` does not accept a trailing 'Z'
    (that arrived in 3.11), so the 'Z' is rewritten to '+00:00' first --
    do not "simplify" that away, the suite runs on 3.10.13.

    Returns None when `token` is not a timestamp at all; callers decide
    whether that is benign (a continuation fragment) or a hard error.
    """
    token = token.strip()
    if token.endswith("Z") or token.endswith("z"):
        token = token[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(token)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _line_is_at_or_after(line, since):
    """True when `line`'s own leading timestamp is at or after `since`.

    Compares ABSOLUTE INSTANTS, never strings. The obvious shortcut -- a
    lexicographic `line.split()[0] >= since` -- is wrong and silently
    returns 0 matches forever on any node whose clock is not set to UTC,
    which is what made every "the phrase must appear" assertion in this
    suite fail on the QE fleet while the product was behaving correctly:
    goxdcr timestamps a line in the NODE's own zone (`/etc/timezone` is
    `US/Pacific` there, so `2026-09-13T23:09:54.969-07:00`) while `since`
    is UTC (`2026-09-14T06:00:14.123Z`). Those two name the SAME instant,
    but '2026-09-13...' sorts below '2026-09-14...', so the line -- and
    every other line in the window -- was discarded.

    A continuation fragment (no leading timestamp) is not a log line and
    returns False, as before (M8). A line that DOES start with something
    timestamp-shaped but cannot be parsed raises: silently dropping such a
    line is precisely the failure mode above, and a count that quietly
    goes to zero reads as a product bug rather than a fixture bug.

    `since` may be an ISO-8601 string or an already-parsed aware datetime
    (`goxdcr_log_count` parses it once, up front, so a malformed value
    fails even on a run where no line matches the pattern at all).
    """
    if not _TIMESTAMP_PREFIX.match(line):
        return False
    token = line.split(" ", 1)[0]
    stamp = _parse_iso_timestamp(token)
    if stamp is None:
        raise ValueError(
            "could not parse the leading timestamp {0!r} of goxdcr log line "
            "{1!r} -- refusing to silently drop it from the `since` "
            "window".format(token, line[:200]))
    since_stamp = since if isinstance(
        since, datetime.datetime) else _parse_iso_timestamp(since)
    if since_stamp is None:
        raise ValueError(
            "could not parse `since` value {0!r} as an ISO-8601 "
            "timestamp".format(since))
    return stamp >= since_stamp


def _inbox_path(shell):
    """Absolute inbox path on a Linux node."""
    return "/opt/couchbase/var/lib/couchbase/" + INBOX_SUBDIR


def install_internal_client_cert(server, chain_pem, key_pem, reload=True):
    """Install an internal client certificate into `server`'s inbox.

    This is the certificate XDCR presents on its intra-cluster surfaces. It is
    NOT the node certificate: x509main handles chain.pem/pkey.key, this handles
    client_chain.pem/client_pkey.key, and the two are reloaded by different
    REST endpoints.

    Both files must be mode 600 and owned by `couchbase`, or the reload is
    rejected.

    Args:
        chain_pem: PEM bytes or str of the client cert (leaf first, CA after)
        key_pem: PEM bytes or str of the matching private key
        reload: call POST /node/controller/reloadClientCertificate afterwards

    Returns:
        True when the certificate is in place (and reloaded, if asked).
    """
    if isinstance(chain_pem, bytes):
        chain_pem = chain_pem.decode()
    if isinstance(key_pem, bytes):
        key_pem = key_pem.decode()

    shell = RemoteMachineShellConnection(server)
    try:
        inbox = _inbox_path(shell)
        shell.create_directory(inbox)
        for filename, content in ((CLIENT_CHAIN_FILE, chain_pem),
                                  (CLIENT_KEY_FILE, key_pem)):
            local = tempfile.NamedTemporaryFile(
                mode="w", suffix=".pem", delete=False)
            try:
                local.write(content)
                local.close()
                shell.copy_file_local_to_remote(
                    local.name, "{0}/{1}".format(inbox, filename))
            finally:
                os.unlink(local.name)
        shell.execute_command(
            "chmod 600 {0}/{1} {0}/{2} && chown couchbase:couchbase "
            "{0}/{1} {0}/{2}".format(inbox, CLIENT_CHAIN_FILE, CLIENT_KEY_FILE))
    finally:
        shell.disconnect()

    if reload:
        status, _, _ = RestConnection(server).reload_client_certificate()
        return bool(status)
    return True


def install_ca_cert(server, ca_pem, load=True):
    """Install a trusted CA certificate into `server`'s inbox/CA folder.

    `RestConnection.load_trusted_CAs()` loads every .pem file it finds in
    inbox/CA -- it reads a directory, not a single file -- so the CA must be
    written there before that call is made. An empty (or missing) inbox/CA
    directory fails *silently*: loadTrustedCAs still returns success, the
    cluster simply ends up trusting nothing, and every certificate presented
    afterwards is rejected by a handshake failure that gives no hint the CA
    was never installed. Call this before load_trusted_CAs(), not after.

    Unlike the private key in install_internal_client_cert, ca.pem is a
    public certificate: mode 644, not 600.

    Args:
        ca_pem: PEM bytes or str of the CA certificate
        load: call POST /node/controller/loadTrustedCAs afterwards

    Returns:
        True when the CA certificate is in place (and loaded, if asked).
    """
    if isinstance(ca_pem, bytes):
        ca_pem = ca_pem.decode()

    shell = RemoteMachineShellConnection(server)
    try:
        inbox = _inbox_path(shell)
        ca_dir = "{0}/{1}".format(inbox, CA_SUBDIR)
        # sftp.mkdir is not recursive: ensure the parent exists first in case
        # this runs before any internal client cert has been installed.
        shell.create_directory(inbox)
        shell.create_directory(ca_dir)
        local = tempfile.NamedTemporaryFile(
            mode="w", suffix=".pem", delete=False)
        try:
            local.write(ca_pem)
            local.close()
            shell.copy_file_local_to_remote(
                local.name, "{0}/{1}".format(ca_dir, CA_FILE))
        finally:
            os.unlink(local.name)
        shell.execute_command(
            "chmod 644 {0}/{1} && chown couchbase:couchbase {0}/{1}".format(
                ca_dir, CA_FILE))
    finally:
        shell.disconnect()

    if load:
        status, _ = RestConnection(server).load_trusted_CAs()
        return bool(status)
    return True


def delete_inbox_contents(server):
    """Best-effort removal of the three files this suite writes into
    `server`'s inbox: the internal client cert pair (CLIENT_CHAIN_FILE,
    CLIENT_KEY_FILE) and everything under inbox/CA.

    Mirrors the ordering `pytests/security/x509_multiple_CA_util.py`'s
    `X509main.teardown_certs` uses for the security suite's own PKI
    teardown: wipe the inbox FIRST, before the REST calls
    (regenerateCertificate/deleteTrustedCA) that follow this in
    `crlXDCR.py`'s `_reset_node_pki`. Doing it in the other order leaves a
    stale client_chain.pem behind, signed by a CA the REST call just
    deleted -- exactly the cert this node keeps presenting on
    intra-cluster mTLS afterward (a node restart, or a later suite
    enabling node encryption, then finds nobody trusts it).

    `load_trusted_CAs` loads EVERY .pem file it finds in inbox/CA (see
    `install_ca_cert`), and `pytests/security/crl_base.py`'s
    `_trust_ca_on_cluster` writes its own crl_test_ca.pem into that same
    directory. Leaving our ca.pem behind after the REST delete_trusted_CA
    call means a later run of the security suite silently re-trusts a CA
    this suite's REST call had just removed -- and, symmetrically, leaves
    that suite's own CA to leak into ours on the next run. Wiping all of
    inbox/CA here (not just our own file) closes both directions.

    Unlike x509_multiple_CA_util's teardown, this removes only the FILES
    -- never the `inbox` directory (or `inbox/CA`) itself. Other suites
    and ns_server both expect `inbox` to exist as a directory.

    Best-effort: an unreachable node must not abort the caller's sweep
    across the rest of the fleet, so this connects with
    `exit_on_failure=False` -- the default would otherwise SIGKILL this
    entire process on one unreachable node (see
    `RemoteMachineShellConnection.ssh_connect_with_retries`). This still
    RAISES on failure, so the caller (`_reset_node_pki`) can log it, the
    same way it already logs a failed `refresh_certificate()` call.
    """
    shell = RemoteMachineShellConnection(server, exit_on_failure=False)
    try:
        inbox = _inbox_path(shell)
        ca_dir = "{0}/{1}".format(inbox, CA_SUBDIR)
        shell.execute_command(
            "rm -f {0}/{1} {0}/{2}; rm -rf {3}/*".format(
                inbox, CLIENT_CHAIN_FILE, CLIENT_KEY_FILE, ca_dir))
    finally:
        shell.disconnect()


def goxdcr_log_count(server, pattern, since=None):
    """Count matches of `pattern` across goxdcr.log* on `server`.

    NodeHelper.check_goxdcr_log cannot be used for counting. It returns
    len(lines) from the grep output, which collapses to 1 when zgrep reports
    'Binary file ... matches' on a compressed rotated log, and it greps a glob
    so every line arrives filename-prefixed once the log has rotated. Both
    distort a count, and the second breaks any positional parse of the line.

    `zgrep -c` reports per-file counts as 'file:count' (or a bare count for a
    single file); summing $NF handles both.

    goxdcr.log is never truncated between tests or runs, so an all-time count
    (the `since=None` default) spans the machine's entire history, not just
    the current test -- residue from an earlier run/probe can fail a
    "must not appear" assertion forever, or silently pass a "must appear"
    assertion while the current test does nothing at all. `assert_no_cert_material_logged`
    is the one caller that deliberately wants the all-time count regardless
    (certificate material must never be logged at any point), so its default
    stays unchanged.

    Args:
        since: optional ISO-8601 timestamp string, e.g.
            '2026-09-01T21:27:00.727Z'. When given, only lines whose OWN
            leading timestamp is at or after `since` AS AN INSTANT are
            counted -- see `_line_is_at_or_after`, and do not revert that
            to a string comparison: goxdcr timestamps its lines in the
            NODE's own timezone, not UTC, so the two spellings of one
            instant do not sort alike.
    """
    shell = RemoteMachineShellConnection(server)
    try:
        log_dir = NodeHelper.get_goxdcr_log_dir(server)
        if since is None:
            cmd = ("zgrep -c -- {0} {1}/goxdcr.log* 2>/dev/null "
                   "| awk -F: '{{s += $NF}} END {{print s + 0}}'").format(
                       shlex.quote(pattern), log_dir)
            output, _ = shell.execute_command(cmd)
            if not output:
                return 0
            try:
                return int(str(output[0]).strip())
            except ValueError:
                return 0

        # `since` needs each matching line's own leading timestamp, not just
        # a count, so this greps for lines rather than -c. Two of zgrep's
        # multi-file/rotated-log quirks would otherwise corrupt that just as
        # they would a naive count: `-h` drops the filename prefix zgrep adds
        # once more than one goxdcr.log* file matches, so there is no
        # 'file:line' prefix to strip back off before reading the leading
        # timestamp; `--text` disables grep's binary-content heuristic so a
        # compressed rotated log always comes back as real lines instead of
        # collapsing to a single 'Binary file ... matches' placeholder with
        # no timestamp to compare.
        # Parse `since` BEFORE reading the log, not per line: a malformed
        # value must fail the test outright, including on a run where the
        # pattern matches nothing at all. Deferring it into the loop would
        # let a "must be 0" assertion pass vacuously on a bad `since` --
        # the same shape of silent-zero bug this window filter already had.
        since_stamp = _parse_iso_timestamp(since)
        if since_stamp is None:
            raise ValueError(
                "could not parse `since` value {0!r} as an ISO-8601 "
                "timestamp".format(since))

        cmd = "zgrep -h --text -- {0} {1}/goxdcr.log* 2>/dev/null".format(
            shlex.quote(pattern), log_dir)
        output, _ = shell.execute_command(cmd)
        if not output:
            return 0
        count = 0
        for line in output:
            # M8 (continuation fragments) and the timezone comparison both
            # live in `_line_is_at_or_after` -- see there.
            if _line_is_at_or_after(str(line), since_stamp):
                count += 1
        return count
    finally:
        shell.disconnect()


def wait_for_goxdcr_phrase(server, pattern, timeout=180, interval=10, since=None):
    """Poll goxdcr.log until `pattern` appears, or `timeout` expires.

    The 'possible certificate revocation' line is emitted only after retry
    exhaustion — roughly 50s after the first failure on 8.1, never on the first
    error. Asserting immediately after the trigger reads a log that is correct
    but not yet written.

    Args:
        since: forwarded to `goxdcr_log_count` -- see there. Omitting it
            (the default) means a phrase already sitting in the log from
            before this poll started satisfies the wait immediately, which
            is almost never what a caller wants; pass the test's start time
            to require a fresh occurrence.

    Returns the match count (0 on timeout), so a caller can assert on it.
    """
    end = time.time() + timeout
    while True:
        count = goxdcr_log_count(server, pattern, since=since)
        if count > 0:
            return count
        if time.time() >= end:
            return 0
        time.sleep(interval)


def goxdcr_pid(server):
    """PID of the goxdcr process on `server`, or None.

    R4 requires a rotated certificate to take effect with no process restart,
    so the hot-reload tests compare this before and after. Without it, a test
    that restarts goxdcr passes while proving the opposite of the requirement.
    """
    shell = RemoteMachineShellConnection(server)
    try:
        output, _ = shell.execute_command("pgrep -f '[g]oxdcr'")
        if not output:
            return None
        try:
            return int(str(output[0]).strip())
        except ValueError:
            return None
    finally:
        shell.disconnect()
