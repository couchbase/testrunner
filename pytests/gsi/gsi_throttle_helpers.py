"""KV rate-limiting (8.5+) stats capture and settings verification for GSI tests."""

import os
import time

import logger
from TestInput import TestInputSingleton
from lib.metering_throttling import kv_throttle_stats, cbstats_throttle_stats
from membase.api.rest_client import RestConnection


class GSIThrottleHelpers:

    def __init__(self, test):
        self.test = test
        self.input = test.input
        # recovery_gsi replaces test.log in setUp
        self.log = logger.Logger.get_logger()
        self.enabled = self.input.param("collect_throttle_stats", False)
        self.stats = None
        self.before = {}
        self.test_start = time.time()

    @staticmethod
    def scaled_timeout(timeout):
        return timeout * TestInputSingleton.input.param(
            "throttle_timeout_multiplier", 1)

    def setup(self):
        if not self.enabled:
            return
        try:
            kv_nodes = self.kv_nodes()
            if not kv_nodes:
                self.test.fail("collect_throttle_stats is set but no KV nodes "
                               "could be resolved - nothing could be measured")
            source = self.input.param("throttle_stats_source", "prometheus")
            reader = {"prometheus": kv_throttle_stats,
                      "cbstats": cbstats_throttle_stats}.get(source)
            if reader is None:
                self.test.fail(f"Unknown throttle_stats_source '{source}', "
                               f"expected 'prometheus' or 'cbstats'")
            master = self.test.master
            self.stats = reader(kv_nodes, master.rest_username,
                                master.rest_password)
            self.verify_settings(phase="setUp")
            for name in self.bucket_names():
                self.before[name] = self.stats.get_bucket_stats(name)
        except Exception as exc:
            self.test.fail(f"Could not start throttle stat capture: {exc!r}")

    def teardown(self):
        if not self.enabled or self.stats is None:
            return
        try:
            # also covers nodes rebalanced in mid-test
            self.verify_settings(phase="tearDown")
        finally:
            try:
                self.log_deltas()
            except Exception as exc:
                self.log.error(f"Throttle stat capture failed at teardown: "
                               f"{exc!r}")

    def apply_bucket_settings(self):
        """For buckets created outside _create_bucket, e.g. by cbbackupmgr restore."""
        params = RestConnection.get_conf_bucket_throttle_params()
        if not params:
            return
        rest = RestConnection(self.test.master)
        for name in self.bucket_names():
            try:
                rest.change_bucket_props(
                    bucket=name,
                    throttleReserved=params.get("throttleReserved"),
                    throttleHardLimit=params.get("throttleHardLimit"))
                self.log.info(
                    f"Applied bucket throttle settings to {name}: {params}")
            except Exception as exc:
                self.log.error(f"Could not apply bucket throttle settings to "
                               f"{name}: {exc!r}")

    def kv_nodes(self):
        # the services map is None mid-rebalance
        try:
            nodes = self.test.get_nodes_from_services_map(service_type="kv",
                                                          get_all_nodes=True)
        except Exception as exc:
            self.log.warning(
                f"Could not resolve KV nodes for throttle stats: {exc!r}")
            return []
        return nodes or []

    def bucket_names(self):
        # test.buckets is empty or stale for default_bucket=false rows
        try:
            return [bucket.name for bucket in
                    RestConnection(self.test.master).get_buckets()]
        except Exception as exc:
            self.log.warning(f"Could not list buckets for throttle stats "
                             f"({exc!r}), using test.buckets")
            return [bucket.name for bucket in (self.test.buckets or [])]

    def verify_settings(self, phase=""):
        expected_reserved = self.input.param("bucket_throttle_reserved", None)
        expected_hard_limit = self.input.param("bucket_throttle_hard_limit",
                                               None)
        expected_node = {
            param: self.input.param(param, None)
            for param in RestConnection.NODE_THROTTLE_PARAMS
            if self.input.param(param, None) is not None
        }

        if expected_reserved is not None or expected_hard_limit is not None:
            for name in self.bucket_names():
                reserved, hard_limit = self.stats.get_bucket_limits(name)
                if expected_reserved is not None:
                    self.test.assertEqual(
                        reserved, expected_reserved,
                        f"[{phase}] throttleReserved on {name} is "
                        f"{reserved}, conf asked for {expected_reserved}")
                if expected_hard_limit is not None:
                    self.test.assertEqual(
                        hard_limit, expected_hard_limit,
                        f"[{phase}] throttleHardLimit on {name} is "
                        f"{hard_limit}, conf asked for {expected_hard_limit}")

        if expected_node:
            kv_nodes = self.kv_nodes()
            if not kv_nodes:
                self.log.warning(f"[{phase}] no KV nodes resolvable, skipping "
                                 f"node throttle verification")
                return
            for server in kv_nodes:
                actual = RestConnection(server).get_node_throttle_settings()
                for param, expected in expected_node.items():
                    self.test.assertEqual(
                        str(actual.get(param)).lower(), str(expected).lower(),
                        f"[{phase}] {param} on {server.ip} is "
                        f"{actual.get(param)}, conf asked for {expected}")

    def log_deltas(self):
        logs_folder = self.input.param("logs_folder", "/tmp")
        summary = os.path.join(logs_folder, "throttle_stats.log")
        names = self.bucket_names()
        dropped = [name for name in self.before if name not in names]
        if dropped:
            self.log.info(f"THROTTLE DELTA skipped for dropped bucket(s) "
                          f"{dropped}")
        for name in names:
            before = self.before.get(name)
            if before is None:
                # bucket created by the test
                before = {metric: 0 for metric in kv_throttle_stats.METRICS}
                before['ts'] = self.test_start
            after = self.stats.get_bucket_stats(name)
            delta = kv_throttle_stats.delta(before, after)
            line = (f"{self.test._testMethodName}\t{name}\t"
                    f"ru={delta['kv_ru_total']}\twu={delta['kv_wu_total']}\t"
                    f"throttled={delta['kv_throttle_count_total']}\t"
                    f"rejected={delta['kv_reject_count_total']}\t"
                    f"waits={delta['kv_throttle_duration_seconds_count']}\t"
                    f"wait_mean={delta['throttle_wait_mean']}s\t"
                    f"elapsed={delta['elapsed']}s\t"
                    f"units_per_sec={delta['units_per_sec']}\t"
                    f"hard_limit={delta['kv_throttle_hard_limit']}"
                    + ("\trecreated=True" if delta['bucket_recreated'] else ""))
            self.log.info(f"THROTTLE DELTA {line}")
            with open(summary, "a") as handle:
                handle.write(line + "\n")
