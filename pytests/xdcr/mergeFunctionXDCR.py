"""
[MB-72963] XDCR CCR merge-function name validation.

goxdcr builds the js-evaluator URL for a custom-conflict-resolution merge
function by concatenating the user-supplied `mergeFunctionMapping` value
straight onto a base URL:

    http://<sourceKVHost>:<xdcrRestPort>/evaluator/v1/libraries/<fname>

Before the fix, `fname` was unvalidated, so a name carrying path
separators could steer that GET at an endpoint the caller was not
entitled to. goxdcr commit ca1a34e adds `base.IsValidUDFName`, called at
the top of `ResolverSvc.CheckMergeFunction`
(service_impl/resolver_service.go), which enforces the Eventing function
naming convention -- first character alphanumeric, remainder
alphanumeric, `_` or `-` -- and rejects anything else with

    The passed UDF '<fname>' has an invalid name

.ini: 2 clusters (C1, C2), 1 node each. No data is loaded; every test
here is replication-settings validation and finishes in seconds.

VERSION SENSITIVITY, deliberately ungated: the rejection tests assert the
post-fix behaviour unconditionally. The fix first shipped in
8.0.3-5884. On any earlier build -- or a branch the backport never
reached -- `test_invalid_merge_function_name_is_rejected_at_create` and
`..._on_settings_change` FAIL rather than skip, which is the intended
signal for a missing backport. Read a failure here as "this build has no
fname validation", not as a broken test.

WHY THE POSITIVE CONTROL IS LOOSELY ASSERTED: a name that passes
validation falls through to the HTTP request, and the error it then
produces depends on whether ResolverSvc is running. On 8.0.x
`resolver_svc.Start()` is commented out at
replication_manager/replication_manager.go:175, so `functionUrl` is empty
and the failure reads `Get "/<fname>": unsupported protocol scheme ""`.
If that call is ever restored the same case reports
`CheckMergeFunction received http.Status 404 ...` instead. Neither is a
regression in name validation, so
`test_valid_merge_function_name_passes_name_validation` asserts only that
the invalid-name error is ABSENT -- it does not pin the downstream text.
"""

import json
import urllib.parse

from lib.membase.api.rest_client import RestConnection

from .xdcrnewbasetests import NodeHelper, XDCRNewBaseTest


class MergeFunctionValidationXDCRTests(XDCRNewBaseTest):
    """Validation of the per-replication `mergeFunctionMapping` setting."""

    # The product error added by MB-72963. Matched as a substring so the
    # quoted fname in the middle of the message does not have to be
    # reproduced exactly.
    INVALID_NAME_ERR = "has an invalid name"

    # Rejected by IsValidUDFName. The first two are the traversal shapes
    # the ticket is actually about; the rest pin the boundaries of the
    # naming convention (leading non-alphanumeric, and the punctuation
    # classes a path or query could otherwise smuggle in).
    INVALID_FNAMES = [
        "../../../../pools/default",
        "../settings/replications",
        "_leadingUnderscore",
        "my.func",
        "func!",
        "my func",
    ]

    # Accepted by IsValidUDFName. None of these exist as a library, so
    # creation still fails -- but it must fail PAST the name check.
    VALID_FNAMES = [
        "noSuchFunc123",
        "my_func-name2",
    ]

    # goxdcr special-cases this one name: CheckMergeFunction's error is
    # swallowed and InitDefaultFunc() runs instead, so the replication is
    # created. base.DefaultMergeFunc in constant.go.
    DEFAULT_MERGE_FUNC = "defaultLWW"

    def setUp(self):
        super(MergeFunctionValidationXDCRTests, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)

        # Remote-cluster refs only -- NOT setup_xdcr(), which would also
        # create the replications. A replication already existing for the
        # bucket pair makes createReplication fail as a duplicate, and a
        # rejection test would then pass without the validator ever
        # running. Tests that need a live replication create their own.
        self.set_xdcr_topology()

    def tearDown(self):
        super(MergeFunctionValidationXDCRTests, self).tearDown()

    # --- Helpers ---------------------------------------------------------

    def _remote_ref_name(self):
        """Name of the C1->C2 remote-cluster ref created by
        set_xdcr_topology()."""
        refs = self.src_cluster.get_remote_clusters()
        self.assertTrue(
            refs, "no remote-cluster ref on C1; cannot create replications "
                  "by ref name")
        return refs[0].get_name()

    def _src_bucket_name(self):
        buckets = self.src_cluster.get_buckets()
        self.assertTrue(buckets, "C1 has no buckets")
        return buckets[0].name

    def _merge_mapping(self, fname):
        """`mergeFunctionMapping` wire form. Only the bucket-level key is
        accepted -- goxdcr rejects a mapping with any other key, or an
        empty value, before the name is ever looked at
        (base.BucketMergeFunctionKey)."""
        return json.dumps({"default": fname})

    def _create_replication_raw(self, fname, from_bucket=None):
        """POST controller/createReplication and return (ok, content).

        Deliberately not RestConnection.start_replication: that helper
        retries three times and raises, discarding the response body,
        while these tests have to match the product's error text. Note
        _http_request returns (False, content, response) for a non-2xx
        rather than raising, so a rejection arrives as ok=False.
        """
        from_bucket = from_bucket or self._src_bucket_name()
        api = self.src_rest.baseUrl + "controller/createReplication"
        params = {
            "replicationType": "continuous",
            "fromBucket": from_bucket,
            "toBucket": from_bucket,
            "toCluster": self._remote_ref_name(),
            "type": "xmem",
            "mergeFunctionMapping": self._merge_mapping(fname),
        }
        self.log.info("createReplication on C1 with mergeFunctionMapping "
                      "for fname={0!r}".format(fname))
        ok, content, _ = self.src_rest._http_request(
            api, "POST", urllib.parse.urlencode(params))
        self.log.info("createReplication result: ok={0} body={1}".format(
            ok, content))
        return ok, self._as_text(content)

    def _set_merge_function_raw(self, repl_id, fname):
        """POST settings/replications/<id> -- the second REST entry point
        into CheckMergeFunction -- and return (ok, content)."""
        api = "{0}settings/replications/{1}".format(
            self.src_rest.baseUrl, urllib.parse.quote(repl_id, safe=""))
        params = {"mergeFunctionMapping": self._merge_mapping(fname)}
        self.log.info("settings/replications on {0} with fname={1!r}".format(
            repl_id, fname))
        ok, content, _ = self.src_rest._http_request(
            api, "POST", urllib.parse.urlencode(params))
        self.log.info("settings/replications result: ok={0} body={1}".format(
            ok, content))
        return ok, self._as_text(content)

    def _as_text(self, content):
        if isinstance(content, bytes):
            return content.decode("utf-8", "replace")
        return str(content)

    def _replication_count(self):
        return len(self.src_rest.get_replications())

    def _create_default_merge_func_replication(self):
        """Create the one replication these tests are allowed to have,
        using the special-cased default merge function. Returns its id."""
        ok, content = self._create_replication_raw(self.DEFAULT_MERGE_FUNC)
        self.assertTrue(
            ok, "createReplication with the default merge function {0!r} was "
                "rejected: {1}".format(self.DEFAULT_MERGE_FUNC, content))
        repl_id = json.loads(content)["id"]
        self.log.info("created replication {0}".format(repl_id))
        return repl_id

    # --- Tests -----------------------------------------------------------

    def test_invalid_merge_function_name_is_rejected_at_create(self):
        """Every name outside the Eventing naming convention is refused by
        createReplication, and no replication is left behind.

        Also asserts the rejection is recorded in goxdcr.log, which is
        what distinguishes a goxdcr validation failure from ns_server
        refusing the request before it ever reaches XDCR.
        """
        before = self._replication_count()
        failures = []
        for fname in self.INVALID_FNAMES:
            ok, content = self._create_replication_raw(fname)
            if ok:
                failures.append(
                    "{0!r}: ACCEPTED, expected rejection".format(fname))
                continue
            if self.INVALID_NAME_ERR not in content:
                failures.append(
                    "{0!r}: rejected, but not for an invalid name: {1}".format(
                        fname, content))
        self.assertEqual(
            [], failures,
            "mergeFunctionMapping name validation failed for {0} of {1} "
            "cases:\n  {2}".format(
                len(failures), len(self.INVALID_FNAMES),
                "\n  ".join(failures)))

        self.assertEqual(
            before, self._replication_count(),
            "a rejected createReplication still created a replication")

        matches, count = NodeHelper.check_goxdcr_log(
            self.src_master, self.INVALID_NAME_ERR)
        self.assertGreater(
            count, 0,
            "createReplication was rejected but goxdcr.log on {0} has no "
            "{1!r} entry; the rejection may not have come from goxdcr".format(
                self.src_master.ip, self.INVALID_NAME_ERR))

    def test_valid_merge_function_name_passes_name_validation(self):
        """A conventionally-valid name must get PAST the name check.

        These libraries do not exist, so creation still fails -- the point
        is that it fails downstream of validation rather than being
        refused as a bad name. Without this control, a validator that
        rejected everything would pass the rejection test above.
        """
        failures = []
        for fname in self.VALID_FNAMES:
            ok, content = self._create_replication_raw(fname)
            if ok:
                # Legal for the name check to pass and the library to
                # exist; nothing to assert against.
                self.log.info(
                    "{0!r} was accepted outright".format(fname))
                continue
            if self.INVALID_NAME_ERR in content:
                failures.append(
                    "{0!r}: rejected as an invalid name, but it conforms to "
                    "the convention: {1}".format(fname, content))
            else:
                self.log.info(
                    "{0!r} passed name validation and failed downstream, as "
                    "expected: {1}".format(fname, content))
        self.assertEqual(
            [], failures,
            "name validation rejected {0} conforming name(s):\n  {1}".format(
                len(failures), "\n  ".join(failures)))

    def test_default_merge_function_is_accepted(self):
        """The default merge function stays usable.

        goxdcr tolerates CheckMergeFunction failing for
        base.DefaultMergeFunc specifically -- it creates the function
        instead -- so this is the one path where an error must NOT block
        creation. A validator tightened past the convention would break
        it here first.
        """
        repl_id = self._create_default_merge_func_replication()
        self.assertIn(
            "/", repl_id,
            "unexpected replication id {0!r}".format(repl_id))

    def test_invalid_merge_function_name_is_rejected_on_settings_change(self):
        """The same validation guards the settings-change entry point.

        createReplication is not the only way into CheckMergeFunction:
        POST settings/replications/<id> reaches the same line, and a fix
        applied to only one path would leave the other exploitable.
        """
        repl_id = self._create_default_merge_func_replication()

        failures = []
        for fname in self.INVALID_FNAMES:
            ok, content = self._set_merge_function_raw(repl_id, fname)
            if ok:
                failures.append(
                    "{0!r}: ACCEPTED, expected rejection".format(fname))
            elif self.INVALID_NAME_ERR not in content:
                failures.append(
                    "{0!r}: rejected, but not for an invalid name: {1}".format(
                        fname, content))
        self.assertEqual(
            [], failures,
            "settings-change name validation failed for {0} of {1} "
            "cases:\n  {2}".format(
                len(failures), len(self.INVALID_FNAMES),
                "\n  ".join(failures)))
