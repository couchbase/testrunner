import json
import time
from .tuq import QueryTests
from membase.api.exception import CBQError
import os


class ConversationalSessionTests(QueryTests):
    """
    MB-70633: Conversational Database Session tests.
    Covers BEGIN CHAT / USING AI / END CHAT syntax against travel-sample.
    Sessions S-01 through S-11 from the test case master.
    """

    def setUp(self):
        super(ConversationalSessionTests, self).setUp()
        self.log.info("==============  ConversationalSessionTests setup has started ==============")
        self.natural_capella_user = os.environ.get("NATURAL_CAPELLA_USER", None)
        self.natural_capella_password = os.environ.get("NATURAL_CAPELLA_PASSWORD", None)
        # Keep this None-safe: the REST-based chat lifecycle tests below need no AI
        # credentials, so setUp must not blow up when the env vars are absent.
        # Tests that do need Capella call _require_ai_creds() and skip instead.
        if self.natural_capella_user and self.natural_capella_password:
            self.natural_cred = self.natural_capella_user + ":" + self.natural_capella_password
        else:
            self.natural_cred = None
        self.natural_orgid = os.environ.get("NATURAL_ORGID", None)
        # Resources created by the REST-based tests, cleaned up in tearDown.
        self._chats = []
        self._users_created = []
        self.natural_context = self.input.param(
            "natural_context",
            "travel-sample.inventory.airport,travel-sample.inventory.airline,"
            "travel-sample.inventory.hotel,travel-sample.inventory.landmark,"
            "travel-sample.inventory.route"
        )
        self.cbqpath = '{0}cbq -quiet -u {1} -p {2} -e=localhost:{3} '.format(
            self.path, self.username, self.password, self.n1ql_port)
        if self.load_sample:
            self.rest.load_sample("travel-sample")
            init_time = time.time()
            while True:
                next_time = time.time()
                query_response = self.run_cbq_query(
                    "SELECT COUNT(*) FROM `travel-sample`"
                )
                doc_count = query_response['results'][0]['$1']
                self.log.info("travel-sample doc count: %d" % doc_count)
                if doc_count == 31591:
                    break
                if next_time - init_time > 600:
                    self.log.warning("travel-sample load timed out at %d docs" % doc_count)
                    break
                self.sleep(2)
            self.wait_for_all_indexes_online()
        self.log.info("==============  ConversationalSessionTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(ConversationalSessionTests, self).suite_setUp()
        self.log.info("==============  ConversationalSessionTests suite_setup has started ==============")
        self.log.info("==============  ConversationalSessionTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  ConversationalSessionTests tearDown has started ==============")
        # The chat cache is capped at NumCPU*2, so leaked chats make later tests fail
        # with E_NL_CHAT_CACHE_FULL. End every chat this test created.
        for chat_id in getattr(self, '_chats', []):
            try:
                self._rest_chat('END CHAT WITH {"chatId": "%s"}' % chat_id)
            except Exception as exc:
                self.log.warning("cleanup: END CHAT %s failed: %s" % (chat_id, exc))
        for user_id in getattr(self, '_users_created', []):
            try:
                self.rest.delete_builtin_user(user_id)
            except Exception as exc:
                self.log.warning("cleanup: delete user %s failed: %s" % (user_id, exc))
        super(ConversationalSessionTests, self).tearDown()
        self.log.info("==============  ConversationalSessionTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  ConversationalSessionTests suite_tearDown has started ==============")
        super(ConversationalSessionTests, self).suite_tearDown()
        self.log.info("==============  ConversationalSessionTests suite_tearDown has completed ==============")

    # ------------------------------------------------------------------ helpers

    def _cbq_exec(self, cmds):
        """Run all cmds together via execute_commands_inside and return parsed JSON."""
        output = self.execute_commands_inside(self.cbqpath, '', cmds, '', '', '', '')
        self.log.info("cbq output: %r" % (output[:500] if output else output,))
        # cbq appends \x04 (EOF) after the JSON — strip before parsing
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        output = output.strip("\x04 \n\r\t") if output else output
        try:
            return json.loads(output)
        except (json.JSONDecodeError, ValueError):
            idx = output.find('{') if output else -1
            if idx >= 0:
                try:
                    return json.loads(output[idx:].strip('\x04 \n\r\t'))
                except (json.JSONDecodeError, ValueError):
                    pass
        return {}

    def _require_ai_creds(self):
        """Skip a test that needs live Capella conversational AI when creds are absent."""
        if not self.natural_cred or not self.natural_orgid:
            self.skipTest("Capella AI credentials not set; export NATURAL_CAPELLA_USER, "
                          "NATURAL_CAPELLA_PASSWORD and NATURAL_ORGID to run this test")

    def _begin_chat(self):
        """Start a conversational session via BEGIN CHAT; returns the chatId."""
        self._require_ai_creds()
        cmds = []
        if self.natural_cred:
            cmds.append('\\set -natural_cred %s;' % self.natural_cred)
        if self.natural_orgid:
            cmds.append('\\set -natural_orgid %s;' % self.natural_orgid)
        if self.natural_context:
            cmds.append('\\set -natural_context "%s";' % self.natural_context)
        cmds.append('BEGIN CHAT;')
        result = self._cbq_exec(cmds)
        self.log.info("BEGIN CHAT result: %s" % result)
        self.assertEqual(result.get('status'), 'success',
                         "BEGIN CHAT failed: %s" % result)
        if result.get('results'):
            chat_id = (result['results'][0].get('chatId') or
                       result['results'][0].get('chatid'))
        else:
            chat_id = result.get('chatId') or result.get('chatid')
        self.assertIsNotNone(chat_id, "BEGIN CHAT did not return a chatId")
        self.log.info("BEGIN CHAT -> chatId=%s" % chat_id)
        return chat_id

    def _end_chat(self, chat_id):
        """End the active conversational session."""
        cmds = []
        if self.natural_cred:
            cmds.append('\\set -natural_cred %s;' % self.natural_cred)
        if self.natural_orgid:
            cmds.append('\\set -natural_orgid %s;' % self.natural_orgid)
        if self.natural_context:
            cmds.append('\\set -natural_context "%s";' % self.natural_context)
        cmds.append('END CHAT WITH {"chatId": "%s"};' % chat_id)
        result = self._cbq_exec(cmds)
        self.assertEqual(result.get('status'), 'success',
                         "END CHAT failed: %s" % result)
        self.log.info("END CHAT completed for chatId=%s" % chat_id)

    def _using_ai(self, natural_language, chat_id):
        """Execute a USING AI query, passing chatId via WITH clause."""
        cmds = []
        if self.natural_cred:
            cmds.append('\\set -natural_cred %s;' % self.natural_cred)
        if self.natural_orgid:
            cmds.append('\\set -natural_orgid %s;' % self.natural_orgid)
        if self.natural_context:
            cmds.append('\\set -natural_context "%s";' % self.natural_context)
        stmt = 'USING AI WITH {"chatId": "%s"} "%s";' % (chat_id,
                                                         natural_language.replace('"', '\\"'),)
        cmds.append(stmt)
        result = self._cbq_exec(cmds)
        self.assertEqual(result.get('status'), 'success',
                         "USING AI query failed for [%s]: %s" % (natural_language, result))
        self.log.info("USING AI [%s] -> generated: %s" % (
            natural_language, result.get('generated_statement', 'N/A')))
        if 'results' in result:
            result['results'] = [self._unwrap_row(r) for r in result['results'] if r is not None]
        return result

    def _unwrap_row(self, row):
        """Unwrap {'alias': {doc}} -> {doc} when AI generates SELECT * AS alias queries."""
        if isinstance(row, dict) and len(row) == 1:
            val = next(iter(row.values()))
            if isinstance(val, dict):
                return val
        return row

    def _get_scalar(self, row):
        """Extract a scalar aggregate value from a result row (handles varied aliases)."""
        if row is None:
            return None
        if not isinstance(row, dict):
            return row
        for key in ('count', 'hotel_count', 'airline_count', 'airport_count',
                    'route_count', 'eat_count', 'drink_count', 'total_hotels', '$1'):
            if key in row:
                return row[key]
        return next(iter(row.values()), None)

    # ---------------------------------------------- REST chat-lifecycle helpers
    # BEGIN/END/PAUSE/ALTER CHAT and system:natural_chats are plain REST statements:
    # they only need a natural_context request parameter, not the cbq shell and not
    # Capella credentials (only USING AI actually calls the LLM). The helpers below
    # drive that path so the regression tests are hermetic and always runnable.

    # The plain 'default' bucket, deliberately not self.natural_context (travel-sample).
    # Without a natural_context, BEGIN CHAT fails with 19215 / cause "empty path".
    _REST_CTX = "default"

    def _rest_chat(self, statement, username=None, password=None, natural_context=None):
        """Run a chat statement over REST, returning the raw response dict."""
        params = {"natural_context": natural_context or self._REST_CTX}
        return self.run_cbq_query(statement, query_params=params,
                                  username=username, password=password)

    def _rest_begin_chat(self, timeout=None, username=None, password=None,
                         natural_context=None):
        """BEGIN CHAT over REST; records the chatId for tearDown and returns it."""
        if timeout is None:
            stmt = 'BEGIN CHAT'
        elif isinstance(timeout, str):
            stmt = 'BEGIN CHAT WITH {"timeout": "%s"}' % timeout
        else:
            stmt = 'BEGIN CHAT WITH {"timeout": %s}' % json.dumps(timeout)
        result = self._rest_chat(stmt, username=username, password=password,
                                 natural_context=natural_context)
        chat_id = result.get('chatId')
        self.assertIsNotNone(chat_id, "BEGIN CHAT did not return a chatId: %s" % result)
        self._chats.append(chat_id)
        return chat_id

    def _end_rest_chat(self, chat_id, username=None, password=None):
        """END CHAT and stop tracking the chat, so tearDown does not re-end it."""
        try:
            self._rest_chat('END CHAT WITH {"chatId": "%s"}' % chat_id,
                            username=username, password=password)
        finally:
            if chat_id in self._chats:
                self._chats.remove(chat_id)

    def _chat_ids(self, username=None, password=None):
        """chatIds visible in system:natural_chats for the given user (admin by default)."""
        result = self.run_cbq_query("SELECT RAW chatId FROM system:natural_chats",
                                    username=username, password=password)
        return result['results']

    def _chat_field(self, chat_id, field):
        """Read one field of a chat row from system:natural_chats, or None if absent."""
        result = self.run_cbq_query(
            "SELECT RAW `%s` FROM system:natural_chats WHERE chatId = '%s'" % (field, chat_id))
        rows = result['results']
        return rows[0] if rows else None

    def _expect_error(self, statement, code, msg_substring=None, username=None,
                      password=None, natural_context=None):
        """Assert a chat statement fails with the given error code (and message text).

        run_cbq_query raises CBQError on a N1QL error, so a missing exception means
        the statement unexpectedly succeeded - that must fail the test explicitly.
        """
        try:
            result = self._rest_chat(statement, username=username, password=password,
                                     natural_context=natural_context)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], code,
                             "Expected error %s for [%s], got %s" % (code, statement, error))
            if msg_substring:
                self.assertIn(msg_substring, str(error),
                              "Expected %r in error for [%s], got %s"
                              % (msg_substring, statement, error))
            return error
        self.fail("Expected error %s for [%s] but it succeeded: %s"
                  % (code, statement, result))

    def _prom_metric(self, metric_name):
        """Value of an n1ql metric from :8093/_prometheusMetrics, or None if absent.

        Matches the metric name exactly - a substring match would let n1ql_chats
        collide with any n1ql_chats_* metric - so None means genuinely absent,
        which is different from a present-but-zero counter.
        """
        status, content = self.rest.get_rest_endpoint_data(
            '_prometheusMetrics', ip=self.master.ip, port=self.n1ql_port)
        self.assertTrue(status, "Failed to scrape _prometheusMetrics on %s" % self.master.ip)
        if isinstance(content, bytes):
            content = content.decode('utf8', 'replace')
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            name = line.split('{')[0].split(' ')[0]
            if name == metric_name:
                try:
                    return int(float(line.split()[-1]))
                except (ValueError, IndexError):
                    continue
        return None

    def _create_chat_user(self, user_id, roles='query_system_catalog,query_manage_system_catalog',
                          password='password1'):
        """Create a local user with the given roles, tracked for deletion in tearDown.

        Roles are comma separated: this goes straight to
        PUT /settings/rbac/users/local/<id>, which rejects the colon separated form
        that RbacBase only rewrites for roles carrying [bucket] parameters.
        Returns (user_id, password).
        """
        # Drop a leftover from an earlier run so the test is rerun-safe.
        try:
            self.rest.delete_builtin_user(user_id)
        except Exception:
            pass
        payload = "name=%s&roles=%s&password=%s" % (user_id, roles, password)
        self.rest.add_set_builtin_user(user_id, payload)
        self._users_created.append(user_id)
        self.sleep(3, "Waiting for user %s to be usable" % user_id)
        return user_id, password

    # ------------------------------------------------------------------ S-01

    def test_s01_hotel_country_city_pronoun_chain(self):
        """
        TC-01-01 to TC-01-04  |  S-01: Hotel Country → City → Pronoun chain
        Validates: basic context carryover and pronoun resolution across 4 turns.
        """
        chat_id = self._begin_chat()
        try:
            # TC-01-01: hotels grouped by country — must return exactly 3 rows
            result = self._using_ai("How many hotels are there in each country?", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 3,
                             "TC-01-01 FAIL: Expected 3 country rows, got %d" % len(rows))
            country_map = {r['country']: r.get('hotel_count', r.get('count', r.get('$1')))
                           for r in rows}
            self.assertEqual(country_map.get('United Kingdom'), 416,
                             "TC-01-01 FAIL: UK hotel_count expected 416, got %s" %
                             country_map.get('United Kingdom'))
            self.assertEqual(country_map.get('United States'), 361,
                             "TC-01-01 FAIL: US hotel_count expected 361, got %s" %
                             country_map.get('United States'))
            self.assertEqual(country_map.get('France'), 140,
                             "TC-01-01 FAIL: France hotel_count expected 140, got %s" %
                             country_map.get('France'))

            # TC-01-02: hotels in Half Moon Bay — context must carry 'hotel' entity; expect 1 row
            result = self._using_ai("How many of those are in Half Moon Bay?", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 1,
                             "TC-01-02 FAIL: Expected 1 row, got %d. "
                             "0 rows = context lost; >1 = no city filter applied." % len(rows))
            count_val = self._get_scalar(rows[0])
            self.assertEqual(count_val, 1,
                             "TC-01-02 FAIL: Expected count=1, got %s. "
                             "City filter 'Half Moon Bay' not applied." % count_val)

            # TC-01-03: pronoun 'its' resolves to the Half Moon Bay hotel; expect name field
            result = self._using_ai("What is its name?", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 1,
                             "TC-01-03 FAIL: Expected 1 row, got %d. "
                             "Pronoun 'its' likely not resolved (returns all 917 hotels)." % len(rows))
            name = rows[0].get('name')
            self.assertEqual(name, 'Pacific Victorian Bed and Breakfast',
                             "TC-01-03 FAIL: Expected 'Pacific Victorian Bed and Breakfast', got '%s'" % name)

            # TC-01-04: pronoun 'it' — full detail row for the same hotel
            result = self._using_ai("Can you give me more details about it?", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 1,
                             "TC-01-04 FAIL: Expected 1 row, got %d. "
                             "Context chain broken — pronoun 'it' not resolved." % len(rows))
            hotel = rows[0]
            self.assertEqual(hotel.get('name'), 'Pacific Victorian Bed and Breakfast',
                             "TC-01-04 FAIL: Wrong hotel name: %s" % hotel.get('name'))
            self.assertEqual(hotel.get('city'), 'Half Moon Bay',
                             "TC-01-04 FAIL: Wrong city: %s" % hotel.get('city'))
            self.assertEqual(hotel.get('country'), 'United States',
                             "TC-01-04 FAIL: Wrong country: %s" % hotel.get('country'))
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-02

    def test_s02_uk_hotels_progressive_filter(self):
        """
        TC-02-01 to TC-02-04  |  S-02: UK Hotels Progressive Filter
        Validates: each turn accumulates an additional WHERE predicate without dropping prior ones.
        """
        chat_id = self._begin_chat()
        try:
            # TC-02-01: all UK hotels — expect 416
            result = self._using_ai("Show me all hotels in the United Kingdom", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 416,
                             "TC-02-01 FAIL: Expected 416 UK hotels, got %d" % count)
            for row in result['results'][:5]:
                self.assertEqual(row.get('country'), 'United Kingdom',
                                 "TC-02-01 FAIL: Non-UK row returned: %s" % row)

            # TC-02-02: UK + free_breakfast=TRUE — expect 306
            result = self._using_ai("Which of those offer free breakfast?", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 306,
                             "TC-02-02 FAIL: Expected 306 rows (UK+breakfast), got %d. "
                             "679 = no country filter (context lost); other values = wrong filter." % count)

            # TC-02-03: UK + free_breakfast + pets_ok — aggregate; expect count=158
            result = self._using_ai("How many of those also allow pets?", chat_id)
            rows = result['results']
            val = self._get_scalar(rows[0])
            self.assertEqual(val, 158,
                             "TC-02-03 FAIL: Expected 158, got %s. "
                             "207 = free_breakfast dropped; higher = country filter dropped." % val)

            # TC-02-04: add city='London' — expect 30 rows
            result = self._using_ai("Show me just the ones in London", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 30,
                             "TC-02-04 FAIL: Expected 30 rows (London+UK+breakfast+pets), got %d. "
                             "37 = pets_ok dropped; 50 = free_breakfast dropped." % count)
            for row in result['results'][:5]:
                self.assertEqual(row.get('city'), 'London',
                                 "TC-02-04 FAIL: Non-London row returned: %s" % row)
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-03

    def test_s03_sf_hotels_global_pivot(self):
        """
        TC-03-01 to TC-03-04  |  S-03: SF Hotels + Global Pivot
        Validates: progressive amenity filters and explicit global pivot resets context.
        """
        chat_id = self._begin_chat()
        try:
            # TC-03-01: SF hotels — expect 132
            result = self._using_ai("List hotels in San Francisco", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 132,
                             "TC-03-01 FAIL: Expected 132 SF hotels, got %d" % count)

            # TC-03-02: SF + free_breakfast — expect 95
            result = self._using_ai("Which of these have free breakfast?", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 95,
                             "TC-03-02 FAIL: Expected 95 rows (SF+breakfast), got %d. "
                             "679 = city filter dropped (context lost)." % count)

            # TC-03-03: SF + free_breakfast + free_parking — expect 28
            result = self._using_ai("Do any of those also have free parking?", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 28,
                             "TC-03-03 FAIL: Expected 28 rows (SF+breakfast+parking), got %d. "
                             "60 or 133 = city or breakfast filter dropped." % count)

            # TC-03-04: global pivot — ALL hotels; expect 917 with NO city filter
            result = self._using_ai("How many hotels are in the database in total?", chat_id)
            rows = result['results']
            total = self._get_scalar(rows[0])
            self.assertEqual(total, 917,
                             "TC-03-04 FAIL: Expected 917 (global total), got %s. "
                             "132 = SF city filter bleeding from prior turns." % total)
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-04

    def test_s04_sfo_airlines_jfk_aircraft_distance(self):
        """
        TC-04-01 to TC-04-04  |  S-04: SFO Airlines → JFK → Aircraft → Distance
        Validates: cross-collection context and pronoun resolution for route queries.
        """
        chat_id = self._begin_chat()
        try:
            # TC-04-01: airlines from SFO — expect 42
            result = self._using_ai("How many airlines fly out of San Francisco airport?", chat_id)
            rows = result['results']
            val = self._get_scalar(rows[0])
            self.assertEqual(val, 42,
                             "TC-04-01 FAIL: Expected 42 airlines from SFO, got %s" % val)

            # TC-04-02: SFO → JFK airlines — verify context maintained (result count > 0)
            result = self._using_ai("Which ones also fly to JFK?", chat_id)
            count = result['metrics']['resultCount']
            self.assertGreater(count, 0, "TC-04-02 FAIL: No airlines returned for SFO→JFK query")
            self.log.info("TC-04-02: Got %d airlines for SFO→JFK (AI may use city or FAA code)" % count)

            # TC-04-03: 'they' + 'that route' → verify equipment field present
            result = self._using_ai("What aircraft do they use on that route?", chat_id)
            count = result['metrics']['resultCount']
            self.assertGreater(count, 0, "TC-04-03 FAIL: No rows returned for aircraft query")
            for row in result['results']:
                self.assertIn('equipment', row,
                              "TC-04-03 FAIL: 'equipment' field missing from row: %s" % row)
            self.log.info("TC-04-03: Got %d rows with equipment field" % count)

            # TC-04-04: 'it' → verify distance field present
            result = self._using_ai("How far is it?", chat_id)
            count = result['metrics']['resultCount']
            self.assertGreater(count, 0, "TC-04-04 FAIL: No rows returned for distance query")
            for row in result['results']:
                self.assertIsNotNone(row.get('distance'),
                                     "TC-04-04 FAIL: 'distance' field missing from row: %s" % row)
            self.log.info("TC-04-04: Got %d rows with distance field" % count)
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-05

    def test_s05_airport_aggregation_france_faa(self):
        """
        TC-05-01 to TC-05-03  |  S-05: Airport Aggregation → France → FAA filter
        Validates: aggregation drill-down context and LIKE predicate carryover.
        """
        chat_id = self._begin_chat()
        try:
            # TC-05-01: airports by country — expect 3 rows
            result = self._using_ai(
                "How many airports are in the database, grouped by country?", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 3,
                             "TC-05-01 FAIL: Expected 3 rows, got %d" % len(rows))
            country_map = {r['country']: r.get('airport_count', r.get('count', r.get('$1')))
                           for r in rows}
            self.assertEqual(country_map.get('United States'), 1560,
                             "TC-05-01 FAIL: US airport_count expected 1560, got %s" %
                             country_map.get('United States'))
            self.assertEqual(country_map.get('France'), 221,
                             "TC-05-01 FAIL: France airport_count expected 221, got %s" %
                             country_map.get('France'))
            self.assertEqual(country_map.get('United Kingdom'), 187,
                             "TC-05-01 FAIL: UK airport_count expected 187, got %s" %
                             country_map.get('United Kingdom'))

            # TC-05-02: French airports — context must carry 'airport' entity; expect 221
            result = self._using_ai("Show me the airports in France", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 221,
                             "TC-05-02 FAIL: Expected 221 French airports, got %d. "
                             "1968 = no country filter (context lost)." % count)

            # TC-05-03: French airports with FAA LIKE 'L%' — expect 16
            result = self._using_ai("Which of those have a FAA code starting with L?", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 16,
                             "TC-05-03 FAIL: Expected 16 rows (France + faa LIKE 'L%%'), got %d. "
                             "221 = FAA filter missing; large value = country filter missing." % count)
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-07

    def test_s07_mixed_sqlpp_using_ai(self):
        """
        TC-07-01 to TC-07-03  |  S-07: Mixed SQL++ + USING AI
        Validates: AI query context reflects intermediate explicit SQL++ DML within same session.
        Cleanup: UNSET alias on all US hotels after test.
        """
        chat_id = self._begin_chat()
        try:
            # TC-07-01: query 5 US hotels — alias field present (null before update)
            result = self._using_ai(
                "Show me the name and alias for any 5 hotels in the United States", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 5,
                             "TC-07-01 FAIL: Expected 5 rows, got %d" % len(rows))
            for row in rows:
                self.assertIn('name', row,
                              "TC-07-01 FAIL: 'name' field missing from row: %s" % row)
                self.assertIn('alias', row,
                              "TC-07-01 FAIL: 'alias' field missing from row: %s" % row)

            # TC-07-02: explicit SQL++ UPDATE within the chat session
            update_result = self.run_cbq_query(
                "UPDATE `travel-sample`.`inventory`.`hotel` AS h "
                "SET h.alias = h.name "
                "WHERE h.country = 'United States'"
            )
            self.assertEqual(update_result['status'], 'success',
                             "TC-07-02 FAIL: UPDATE failed: %s" % update_result)
            mutation_count = update_result['metrics'].get('mutationCount', 0)
            self.assertEqual(mutation_count, 361,
                             "TC-07-02 FAIL: Expected mutationCount=361, got %d" % mutation_count)

            # TC-07-03: re-query — alias must equal name (AI context sees the explicit UPDATE)
            result = self._using_ai(
                "Show me the name and alias for any 5 hotels in the United States again", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 5,
                             "TC-07-03 FAIL: Expected 5 rows, got %d" % len(rows))
            for row in rows:
                self.assertIsNotNone(row.get('alias'),
                                     "TC-07-03 FAIL: alias is null — AI ignored intermediate UPDATE: %s" % row)
                self.assertEqual(row.get('alias'), row.get('name'),
                                 "TC-07-03 FAIL: alias != name. Row: %s" % row)
        finally:
            self._end_chat(chat_id)
            self.run_cbq_query(
                "UPDATE `travel-sample`.`inventory`.`hotel` AS h "
                "UNSET h.alias "
                "WHERE h.country = 'United States'"
            )

    # ------------------------------------------------------------------ S-08

    def test_s08_transaction_context_rollback(self):
        """
        TC-08-01 to TC-08-05  |  S-08: Transaction Context ROLLBACK
        Validates: read-your-writes within BEGIN WORK, and full isolation after ROLLBACK WORK.
        """
        chat_id = self._begin_chat()
        txid = None
        try:
            # TC-08-01: BEGIN WORK — must return a txid
            begin_result = self.run_cbq_query("BEGIN WORK")
            self.assertEqual(begin_result['status'], 'success',
                             "TC-08-01 FAIL: BEGIN WORK failed: %s" % begin_result)
            txid = begin_result['results'][0].get('txid')
            self.assertIsNotNone(txid, "TC-08-01 FAIL: txid not returned by BEGIN WORK")
            self.log.info("BEGIN WORK txid=%s" % txid)

            # TC-08-02: UPDATE within transaction
            update_result = self.run_cbq_query(
                "UPDATE `travel-sample`.`inventory`.`hotel` AS h "
                "SET h.alias = 'TEMP_TEST' "
                "WHERE h.country = 'United States'",
                txnid=txid
            )
            self.assertEqual(update_result['status'], 'success',
                             "TC-08-02 FAIL: Transactional UPDATE failed: %s" % update_result)
            mutation_count = update_result['metrics'].get('mutationCount', 0)
            self.assertEqual(mutation_count, 361,
                             "TC-08-02 FAIL: Expected mutationCount=361, got %d" % mutation_count)

            # TC-08-03: USING AI within same transaction — read-your-writes via \\set -txid
            cmds = []
            if self.natural_cred:
                cmds.append('\\set -natural_cred %s;' % self.natural_cred)
            if self.natural_orgid:
                cmds.append('\\set -natural_orgid %s;' % self.natural_orgid)
            if self.natural_context:
                cmds.append('\\set -natural_context "%s";' % self.natural_context)
            cmds.append('\\set -txid %s;' % txid)
            cmds.append('USING AI WITH {"chatId": "%s"} "Show me the alias field for any 5 hotels in the United States";' % chat_id)
            ai_result = self._cbq_exec(cmds)
            self.assertEqual(ai_result['status'], 'success',
                             "TC-08-03 FAIL: USING AI within transaction failed: %s" % ai_result)
            rows = [self._unwrap_row(r) for r in ai_result.get('results', [])]
            self.assertEqual(len(rows), 5,
                             "TC-08-03 FAIL: Expected 5 rows, got %d" % len(rows))
            for row in rows:
                alias = row.get('alias') if isinstance(row, dict) else row  # scalar if AI used SELECT RAW
                self.assertEqual(alias, 'TEMP_TEST',
                                 "TC-08-03 FAIL: Expected alias='TEMP_TEST' (read-your-writes), "
                                 "got '%s'. Transaction mutation not visible." % alias)

            # TC-08-04: ROLLBACK WORK
            rollback_result = self.run_cbq_query(
                "ROLLBACK WORK",
                txnid=txid
            )
            self.assertEqual(rollback_result['status'], 'success',
                             "TC-08-04 FAIL: ROLLBACK WORK failed: %s" % rollback_result)
            txid = None

            # TC-08-05: USING AI after rollback — alias must NOT be TEMP_TEST (isolation)
            result = self._using_ai(
                "Show me the alias field for any 5 hotels in the United States", chat_id)
            rows = result['results']
            self.assertEqual(len(rows), 5,
                             "TC-08-05 FAIL: Expected 5 rows, got %d" % len(rows))
            for row in rows:
                alias = row.get('alias') if isinstance(row, dict) else row
                self.assertNotEqual(alias, 'TEMP_TEST',
                                    "TC-08-05 FAIL: alias='TEMP_TEST' visible after ROLLBACK — "
                                    "dirty read / isolation failure detected.")
        finally:
            if txid:
                try:
                    self.run_cbq_query("ROLLBACK WORK", txnid=txid)
                except Exception as exc:
                    self.log.warning("ROLLBACK WORK failed for txid=%s: %s", txid, exc)
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-09

    def test_s09_context_reset_after_end_chat(self):
        """
        TC-09-01 to TC-09-02  |  S-09: Context Reset After END CHAT
        Chat A establishes London hotel context; Chat B is a fresh session.
        Validates: prior session context does NOT bleed into the new session.
        """
        # Chat A
        chat_id_a = self._begin_chat()
        try:
            result = self._using_ai("How many hotels are in London?", chat_id_a)
            rows = result['results']
            count_a = self._get_scalar(rows[0])
            self.assertEqual(count_a, 67,
                             "TC-09-01 FAIL: Expected 67 London hotels, got %s" % count_a)
            self.log.info("TC-09-01 PASS: Chat A returned hotel_count=67 for London; session ended.")
        finally:
            self._end_chat(chat_id_a)

        # Chat B — brand-new session; 'those' has no prior context
        chat_id = self._begin_chat()
        try:
            result = self._using_ai("How many of those have free breakfast?", chat_id)
            rows = result.get('results', [])
            if not rows:
                self.log.info("TC-09-02 PASS: clarification response (no rows): %s" % result)
            else:
                count_b = self._get_scalar(rows[0])
                self.assertNotEqual(count_b, 50,
                                    "TC-09-02 FAIL: Received 50 — CRITICAL session isolation failure. "
                                    "London hotel context leaked from Chat A into Chat B. "
                                    "Expected 679 (global free_breakfast) or a clarification response.")
                if isinstance(count_b, int):
                    self.assertEqual(count_b, 679,
                                     "TC-09-02 FAIL: Expected 679 (global free_breakfast count with no "
                                     "city context), got %s. Session context isolation broken." % count_b)
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-10

    def test_s10_empty_result_edge_case(self):
        """
        TC-10-01 to TC-10-02  |  S-10: Empty Result Edge Case
        Validates: graceful handling of 0-row first turn, and city context retained on follow-up.
        """
        chat_id = self._begin_chat()
        try:
            # TC-10-01: Miami hotels — travel-sample has none; expect 0 rows, no crash
            result = self._using_ai("Show me hotels in Miami", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 0,
                             "TC-10-01 FAIL: Expected 0 Miami hotels (none in travel-sample), "
                             "got %d" % count)

            # TC-10-02: follow-up on empty context — city='Miami' must persist; result still 0
            result = self._using_ai("Which of those have free breakfast?", chat_id)
            count = result['metrics']['resultCount']
            self.assertEqual(count, 0,
                             "TC-10-02 FAIL: Expected 0 rows (Miami city context retained), "
                             "got %d. 679 = city context dropped; system error = crash on "
                             "empty prior context." % count)
        finally:
            self._end_chat(chat_id)

    # ------------------------------------------------------------------ S-11

    def test_s11_french_airlines_cross_collection_join(self):
        """
        TC-11-01 to TC-11-03  |  S-11: French Airlines Cross-Collection JOIN
        Validates: AI correctly JOINs airline → route collections using established context,
        and correctly swaps the airport on implicit 'What about from Los Angeles?'.
        """
        chat_id = self._begin_chat()
        try:
            # TC-11-01: French airlines — expect 21
            result = self._using_ai("How many airlines are based in France?", chat_id)
            rows = result['results']
            val = self._get_scalar(rows[0])
            self.assertEqual(val, 21,
                             "TC-11-01 FAIL: Expected 21 French airlines, got %s" % val)

            # TC-11-02: French airline routes from SFO — cross-collection JOIN with context
            result = self._using_ai(
                "How many routes do they operate from San Francisco airport?", chat_id)
            stmt = (result.get('generated_statement') or '').lower()
            rows = result['results']
            route_count = self._get_scalar(rows[0]) if rows else 0
            self.assertGreater(route_count, 0,
                               "TC-11-02 FAIL: Expected >0 routes for French airlines from SFO, got %s" %
                               route_count)
            self.assertTrue("route" in stmt and ("france" in stmt or "country" in stmt),
                            "TC-11-02 FAIL: Generated query appears to have lost French airline context: %s" %
                            result.get('generated_statement'))
            self.assertTrue("san francisco" in stmt or "sfo" in stmt,
                            "TC-11-02 FAIL: Generated query does not include SFO/San Francisco constraint: %s" %
                            result.get('generated_statement'))

            # TC-11-03: same French airlines, switch airport to LAX with context retained
            result = self._using_ai("What about from Los Angeles?", chat_id)
            stmt = (result.get('generated_statement') or '').lower()
            rows = result['results']
            route_count = self._get_scalar(rows[0]) if rows else 0
            self.assertGreater(route_count, 0,
                               "TC-11-03 FAIL: Expected >0 routes for French airlines from LAX, got %s" %
                               route_count)
            self.assertTrue("route" in stmt and ("france" in stmt or "country" in stmt),
                            "TC-11-03 FAIL: Generated query appears to have lost French airline context: %s" %
                            result.get('generated_statement'))
            self.assertTrue("los angeles" in stmt or "lax" in stmt,
                            "TC-11-03 FAIL: Generated query does not switch airport to Los Angeles/LAX: %s" %
                            result.get('generated_statement'))
        finally:
            self._end_chat(chat_id)

    # ============================================================================
    # REST-based chat lifecycle regressions (no AI credentials required).
    # BEGIN/END/PAUSE/ALTER CHAT and system:natural_chats are plain REST statements,
    # so these run anywhere - no cbq shell, no Capella, no travel-sample.
    # ============================================================================

    def test_n1ql_chats_metric_increments(self):
        """
        MB-71826: a n1ql_chats counter is exposed on the query service's
        /_prometheusMetrics endpoint and increments once per successful BEGIN CHAT.
        Note the metric lives on the query port (8093), not 9499 as the MB states -
        9499 is a cluster_run-only port and is refused on an installed build.
        """
        before = self._prom_metric('n1ql_chats')
        self.assertIsNotNone(
            before, "MB-71826: n1ql_chats is absent from :%s/_prometheusMetrics "
                    "(pre-fix builds do not expose it)" % self.n1ql_port)

        chat_id = self._rest_begin_chat()
        after = self._prom_metric('n1ql_chats')
        self.assertEqual(after - before, 1,
                         "MB-71826: expected n1ql_chats +1 after BEGIN CHAT, got %s -> %s"
                         % (before, after))

        # Only BEGIN CHAT increments; END CHAT must leave the counter alone.
        self._end_rest_chat(chat_id)
        after_end = self._prom_metric('n1ql_chats')
        self.assertEqual(after_end, after,
                         "MB-71826: END CHAT must not increment n1ql_chats, got %s -> %s"
                         % (after, after_end))

    def test_chat_timeout_accepted_values(self):
        """
        MB-71372: chats carry a configurable inactivity timeout, surfaced as a Go
        duration string in system:natural_chats. The default is 60 minutes.
        """
        chat_id = self._rest_begin_chat()
        self.assertEqual(self._chat_field(chat_id, 'inactivityTimeout'), '1h0m0s',
                         "MB-71372: default inactivityTimeout should be 1h0m0s")

        # A JSON number is interpreted as seconds, a string as a Go duration.
        chat_secs = self._rest_begin_chat(timeout=3600)
        self.assertEqual(self._chat_field(chat_secs, 'inactivityTimeout'), '1h0m0s',
                         "MB-71372: timeout=3600 (seconds) should be 1h0m0s")

        chat_dur = self._rest_begin_chat(timeout="90m")
        self.assertEqual(self._chat_field(chat_dur, 'inactivityTimeout'), '1h30m0s',
                         "MB-71372: timeout='90m' should be 1h30m0s")

    def test_chat_timeout_below_minimum_rejected(self):
        """
        MB-71372: the inactivity timeout can only be raised - the minimum equals the
        60 minute default - and non-numeric/non-duration values are rejected outright.
        """
        for timeout in ('300', '60'):
            self._expect_error('BEGIN CHAT WITH {"timeout": %s}' % timeout, 19246,
                               'inactivity timeout must be at least 1h0m0s')
        self._expect_error('BEGIN CHAT WITH {"timeout": "5m"}', 19246,
                           'inactivity timeout must be at least 1h0m0s')
        self._expect_error('BEGIN CHAT WITH {"timeout": true}', 19246,
                           'invalid timeout type: bool')

    def test_alter_and_update_chat_timeout(self):
        """
        MB-71372: an existing chat's timeout can be changed with ALTER CHAT or by
        UPDATEing system:natural_chats. inactivityTimeout is the only updatable field.
        """
        chat_id = self._rest_begin_chat()

        self._rest_chat('ALTER CHAT WITH {"chatId": "%s", "timeout": "2h"}' % chat_id)
        self.assertEqual(self._chat_field(chat_id, 'inactivityTimeout'), '2h0m0s',
                         "MB-71372: ALTER CHAT should set inactivityTimeout to 2h0m0s")

        # The WITH clause and its timeout key are both mandatory for ALTER CHAT.
        self._expect_error('ALTER CHAT WITH {"chatId": "%s"}' % chat_id, 19213)

        self.run_cbq_query("UPDATE system:natural_chats SET inactivityTimeout = '1h40m' "
                           "WHERE chatId = '%s'" % chat_id)
        self.assertEqual(self._chat_field(chat_id, 'inactivityTimeout'), '1h40m0s',
                         "MB-71372: UPDATE should set inactivityTimeout to 1h40m0s")

        # Any other field is not updatable.
        try:
            self.run_cbq_query("UPDATE system:natural_chats SET summary = 'nope' "
                               "WHERE chatId = '%s'" % chat_id)
            self.fail("MB-71372: updating a field other than inactivityTimeout should fail")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 5130,
                             "MB-71372: expected 5130 invalid field update, got %s" % error)

    def test_chat_not_visible_to_other_user(self):
        """
        MB-71373: system:natural_chats is filtered by chat ownership - another
        non-admin user simply does not see the rows (no error) - while an
        administrator can see every user's chat.
        """
        user_a, pwd_a = self._create_chat_user('chat_user_a')
        user_b, pwd_b = self._create_chat_user('chat_user_b')

        chat_id = self._rest_begin_chat(username=user_a, password=pwd_a)

        self.assertIn(chat_id, self._chat_ids(username=user_a, password=pwd_a),
                      "MB-71373: the owner must see their own chat")
        self.assertNotIn(chat_id, self._chat_ids(username=user_b, password=pwd_b),
                         "MB-71373: another user must not see someone else's chat")
        # The admin bypass is the actual fix: pre-fix an admin saw no chats at all.
        self.assertIn(chat_id, self._chat_ids(),
                      "MB-71373: an administrator must see every user's chat")

    def test_chat_statements_denied_for_non_owner(self):
        """
        MB-71373: chat statements issued by a non-owner are rejected with 19241,
        and mutations through system:natural_chats affect no rows.
        """
        user_a, pwd_a = self._create_chat_user('chat_user_a')
        user_b, pwd_b = self._create_chat_user('chat_user_b')

        chat_id = self._rest_begin_chat(username=user_a, password=pwd_a)
        denied = 'The non-admin user(s) provided in the request are not part of the chat session'

        for statement in ('PAUSE CHAT WITH {"chatId": "%s"}' % chat_id,
                          'ALTER CHAT WITH {"chatId": "%s", "timeout": "2h"}' % chat_id,
                          'END CHAT WITH {"chatId": "%s"}' % chat_id):
            self._expect_error(statement, 19241, denied,
                               username=user_b, password=pwd_b)

        # UPDATE/DELETE are filtered rather than denied: no rows, no error.
        result = self.run_cbq_query(
            "UPDATE system:natural_chats SET inactivityTimeout = '2h' WHERE chatId = '%s'"
            % chat_id, username=user_b, password=pwd_b)
        self.assertEqual(result['metrics'].get('mutationCount', 0), 0,
                         "MB-71373: a non-owner UPDATE must not mutate the chat")

        result = self.run_cbq_query(
            "DELETE FROM system:natural_chats WHERE chatId = '%s'" % chat_id,
            username=user_b, password=pwd_b)
        self.assertEqual(result['metrics'].get('mutationCount', 0), 0,
                         "MB-71373: a non-owner DELETE must not remove the chat")
        self.assertIn(chat_id, self._chat_ids(),
                      "MB-71373: the chat must still exist after a non-owner DELETE")

    def test_admin_can_manage_any_chat(self):
        """
        MB-71373: an administrator may manage any user's chat. This is the behaviour
        the fix introduced - before it, an admin was denied with 19241 like any
        other non-owner.
        """
        user_a, pwd_a = self._create_chat_user('chat_user_a')
        chat_id = self._rest_begin_chat(username=user_a, password=pwd_a)

        self._end_rest_chat(chat_id)
        self.assertNotIn(chat_id, self._chat_ids(),
                         "MB-71373: an admin END CHAT should remove another user's chat")

    def test_natural_context_more_than_four_keyspaces(self):
        """
        MB-70908: the four-keyspace cap on natural_context was removed. Before the
        fix a fifth keyspace failed with 19215 wrapping 19223 'Too many keyspaces
        specified.'; the list length is now unbounded.
        """
        scope = 'mb70908'
        collections = ['c1', 'c2', 'c3', 'c4', 'c5']
        self.run_cbq_query('CREATE SCOPE default.%s IF NOT EXISTS' % scope)
        self.sleep(5, "Waiting for scope to be created")
        try:
            for name in collections:
                self.run_cbq_query('CREATE COLLECTION default.%s.%s IF NOT EXISTS'
                                   % (scope, name))
            self.sleep(5, "Waiting for collections to be created")

            paths = ['default.%s.%s' % (scope, name) for name in collections]
            # 3 keyspaces was always allowed; 5 and 8 were rejected before the fix.
            for count, context in ((3, ','.join(paths[:3])),
                                   (5, ','.join(paths)),
                                   (8, ','.join(paths + paths[:3]))):
                chat_id = self._rest_begin_chat(natural_context=context)
                self.assertIsNotNone(
                    chat_id, "MB-70908: BEGIN CHAT with %d keyspaces should succeed" % count)
                self._end_rest_chat(chat_id)
        finally:
            self.run_cbq_query('DROP SCOPE default.%s IF EXISTS' % scope)
