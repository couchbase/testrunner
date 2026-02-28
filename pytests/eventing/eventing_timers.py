import json
import os
import logging
import time
import re


from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest, log

log = logging.getLogger()


class EventingTimers(EventingBaseTest):
    def setUp(self):
        super(EventingTimers, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.handler_code = self.input.param('handler_code', None)
        self.num_timer_partitions = self.input.param('num_timer_partitions', 2)
        self.worker_count = self.input.param('worker_count', 1)
        self.gap_days = self.input.param('gap_days', 25)
        self.monitor_checks = self.input.param('monitor_checks', 180)
        self.monitor_interval = self.input.param('monitor_interval', 30)

    def tearDown(self):
        super(EventingTimers, self).tearDown()

    def test_null_refrence(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, "handler_code/null_timer_refrence.js",
                                              worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_different_context_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_timer_creation_error(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_timer_execution_time(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_multiple_timers_with_same_callback_reference(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 1)
        self.undeploy_and_delete_function(body)

    def test_timer_span_gap_recovery(self):
        """
        Test timer span gap recovery after large time gap injection.

        This test validates the fix for timer partition stuck bug where timer partitions
        get permanently stuck when there's a large gap between span.start and current time.

        Bug: The iterator's ShrinkSpan was only called when root keys existed, so empty
        timeslots made no persistent progress. The 60s scan timeout would kill the scan,
        and the next cycle would restart from the same span.start position, creating a livelock.

        Fix: ShrinkSpan is now called unconditionally after every timeslot advance, and also
        before timeout exits, ensuring progress is always persisted.

        Steps:
        1. Deploy a function with timer chaining
        2. Wait for bootstrap and insert trigger document to start timer chain
        3. Create primary index on metadata bucket
        4. Discover timer span document prefix
        5. Pause function
        6. Inject a large gap (25 days) by setting span.sta to (now - 25 days) via N1QL
        7. Resume function
        8. Monitor span.sta advancement over multiple scan cycles
        9. Verify span.sta advances beyond the injection point within expected time
        """
        # Step 1: Create and deploy timer function
        log.info("[Step 1] Creating and deploying timer function...")
        body = self.create_save_function_body(
            self.function_name,
            "handler_code/timer_span_gap_recovery.js",
            worker_count=self.worker_count
        )
        if 'num_timer_partitions' not in body['settings']:
            body['settings']['num_timer_partitions'] = self.num_timer_partitions
        self.deploy_function(body)
        self.sleep(30, "Waiting 30 seconds for function to bootstrap...")

        # Step 2: Insert trigger document to start timer chain
        log.info("[Step 2] Inserting trigger document to start timer chain...")
        trigger_query = 'INSERT INTO `{}` (KEY, VALUE) VALUES ("timer_gap_test_trigger", {{"trigger": true}})'.format(
            self.src_bucket_name)
        self.n1ql_helper.run_cbq_query(query=trigger_query, server=self.n1ql_server)
        log.info("Trigger document inserted")
        self.sleep(30, "Waiting 30 seconds for timer chain to start...")

        # Step 3: Create primary index on metadata bucket
        log.info("[Step 3] Creating primary index on metadata bucket...")
        try:
            index_query = "CREATE PRIMARY INDEX IF NOT EXISTS ON `{}`".format(self.metadata_bucket_name)
            self.n1ql_helper.run_cbq_query(query=index_query, server=self.n1ql_server)
            self.sleep(5, "Waiting 5 seconds for primary index creation...")
        except Exception as e:
            log.info("Index may already exist: {}".format(e))

        # Step 4: Discover timer span prefix
        log.info("[Step 4] Discovering timer span prefix...")
        span_prefix = self._discover_timer_span_prefix(self.metadata_bucket_name)
        log.info("Discovered timer span prefix: {}".format(span_prefix))

        # Step 5: Verify span documents exist
        log.info("[Step 5] Verifying span documents...")
        span_count = self._get_span_document_count(self.metadata_bucket_name, span_prefix)
        if span_count == 0:
            raise Exception("No timer span documents found. Function may not have bootstrapped properly.")
        log.info("Found {} timer span documents".format(span_count))

        # Get baseline span state
        baseline_min_sta = self._get_min_span_sta(self.metadata_bucket_name, span_prefix)
        log.info("Baseline MIN(sta): {}".format(baseline_min_sta))

        # Step 6: Pause function to inject gap safely
        log.info("[Step 6] Pausing function to inject gap safely...")
        self.pause_function(body)
        self.sleep(15, "Waiting 15 seconds after pausing function...")

        # Step 7: Inject large gap
        log.info("[Step 7] Injecting {}-day gap into timer spans...".format(self.gap_days))
        gap_seconds = 3600 * 24 * self.gap_days
        mutated_count = self._inject_span_gap(self.metadata_bucket_name, span_prefix, gap_seconds)
        if mutated_count == 0:
            raise Exception("Failed to inject gap. No span documents were updated.")
        log.info("Injected gap into {} span documents (sta = now - {} seconds)".format(
            mutated_count, gap_seconds))

        # Verify gap was injected
        after_injection_min_sta = self._get_min_span_sta(self.metadata_bucket_name, span_prefix)
        current_time = int(time.time())
        gap_to_now = current_time - after_injection_min_sta
        log.info("After injection MIN(sta): {} (gap to now: ~{} seconds)".format(
            after_injection_min_sta, gap_to_now))

        # Step 8: Resume function
        log.info("[Step 8] Resuming function...")
        self.resume_function(body)
        self.sleep(10, "Waiting 10 seconds after resuming function...")

        # Step 9: Monitor span.sta advancement
        log.info("[Step 9] Monitoring span.sta advancement (expecting steady progress with fix)...")
        log.info("Check | MIN(sta)   | MAX(sta)   | Gap(s) | Status")
        log.info("------|------------|------------|--------|------------------")

        advancement_count = 0
        prev_min_sta = after_injection_min_sta

        for check in range(1, self.monitor_checks + 1):
            self.sleep(self.monitor_interval, "Waiting {} seconds before next check...".format(self.monitor_interval))

            current_min_sta = self._get_min_span_sta(self.metadata_bucket_name, span_prefix)
            try:
                current_max_sta = self._get_max_span_sta(self.metadata_bucket_name, span_prefix)
            except:
                current_max_sta = current_min_sta

            now_ts = int(time.time())
            gap = now_ts - current_min_sta
            status = "STUCK"

            if current_min_sta > prev_min_sta:
                status = "ADVANCING"
                advancement_count += 1

            log.info("{:5d} | {:10d} | {:10d} | {:6d} | {}".format(
                check, current_min_sta, current_max_sta, gap, status))

            prev_min_sta = current_min_sta

            # If gap is small enough (< 60s), we've successfully recovered
            if gap < 60:
                log.info("=" * 70)
                log.info("SUCCESS: Gap closed to {} seconds after {} checks".format(gap, check))
                log.info("Timer partitions successfully recovered from {}-day gap".format(self.gap_days))
                log.info("=" * 70)
                self.undeploy_and_delete_function(body)
                return

            # If max(sta) has progressed past current time (with 2 min tolerance), we've successfully recovered
            if current_max_sta > now_ts - 120:
                log.info("=" * 70)
                log.info("SUCCESS: MAX(sta) progressed past current time ({} > {}) after {} checks".format(
                    current_max_sta, now_ts - 120, check))
                log.info("Timer partitions successfully recovered from {}-day gap".format(self.gap_days))
                log.info("=" * 70)
                self.undeploy_and_delete_function(body)
                return

        # Analyze results after monitoring period
        log.info("Monitoring complete: {}/{} checks showed advancement".format(
            advancement_count, self.monitor_checks - 1))

        final_gap = int(time.time()) - prev_min_sta

        # Clean up
        self.undeploy_and_delete_function(body)

        # Assert based on results
        if advancement_count == 0:
            raise Exception(
                "FAILURE: Timer partitions are stuck. MIN(sta) never advanced from {}. "
                "This indicates the bug is present (ShrinkSpan not called for empty timeslots).".format(
                    after_injection_min_sta))
        elif advancement_count < (self.monitor_checks - 1) // 2:
            raise Exception(
                "PARTIAL FAILURE: Timer partitions show inconsistent advancement. "
                "Only {} out of {} checks advanced. Expected continuous advancement with the fix.".format(
                    advancement_count, self.monitor_checks - 1))
        elif final_gap >= self.gap_seconds // 2:
            raise Exception(
                "FAILURE: Gap reduction insufficient. Started with {} seconds gap, "
                "ended with {} seconds gap. Expected >50% reduction within {} seconds.".format(
                    self.gap_seconds, final_gap, self.monitor_checks * self.monitor_interval))
        else:
            log.info("SUCCESS: Gap reduced from {} to {} seconds (>50% reduction)".format(
                self.gap_seconds, final_gap))
            log.info("Timer partitions are recovering as expected")

    def _discover_timer_span_prefix(self, bucket):
        """Discover timer span document prefix from metadata bucket"""
        query = "SELECT RAW meta().id FROM `{}` WHERE meta().id LIKE '%:tm:%:sp' LIMIT 1".format(bucket)
        result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        if len(result.get("results", [])) == 0:
            raise Exception("No span documents found")
        span_key = result["results"][0]
        # Extract prefix from span key format: "<prefix>:tm:<partition>:sp"
        tm_index = span_key.find(":tm:")
        if tm_index == -1:
            raise Exception("Invalid span key format: {}".format(span_key))
        return span_key[:tm_index]

    def _get_span_document_count(self, bucket, prefix):
        """Get count of timer span documents"""
        query = "SELECT COUNT(*) AS cnt FROM `{}` WHERE meta().id LIKE '{}:tm:%:sp'".format(bucket, prefix)
        result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        if len(result.get("results", [])) == 0:
            return 0
        return result["results"][0].get("cnt", 0)

    def _get_min_span_sta(self, bucket, prefix):
        """Get minimum sta value from timer span documents"""
        query = "SELECT MIN(sta) AS min_sta FROM `{}` WHERE meta().id LIKE '{}:tm:%:sp'".format(bucket, prefix)
        result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        if len(result.get("results", [])) == 0 or result["results"][0].get("min_sta") is None:
            raise Exception("No results or null min_sta")
        return result["results"][0]["min_sta"]

    def _get_max_span_sta(self, bucket, prefix):
        """Get maximum sta value from timer span documents"""
        query = "SELECT MAX(sta) AS max_sta FROM `{}` WHERE meta().id LIKE '{}:tm:%:sp'".format(bucket, prefix)
        result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        if len(result.get("results", [])) == 0 or result["results"][0].get("max_sta") is None:
            raise Exception("No results or null max_sta")
        return result["results"][0]["max_sta"]

    def _inject_span_gap(self, bucket, prefix, gap_seconds):
        """Inject a time gap into timer span documents"""
        query = "UPDATE `{}` SET sta = CEIL((ROUND(NOW_MILLIS()/1000) - {}) / 7) * 7 WHERE meta().id LIKE '{}:tm:%:sp'".format(
            bucket, gap_seconds, prefix)
        result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        metrics = result.get("metrics", {})
        return metrics.get("mutationCount", 0)