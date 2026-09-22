import re
import time
import requests
from lib.Cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
import logger

class metering(object):
    def __init__(self, server, username, password):
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        url = f"https://{server}:{CbServer.ssl_port}/pools/default"
        response = requests.get(url, auth=self.auth, verify=False)
        self.nodes = response.json()['nodes']
        self.log = logger.Logger.get_logger()

    @staticmethod
    def __meter_pattern(unit, bucket, service, unbilled='', variant=''):
        meter_service = ''
        meter_unbilled = ''
        meter_variant = ''
        if unbilled != '':
            meter_unbilled = f',unbilled="{unbilled}"'
        if variant != '':
            meter_variant = f',variant="{variant}"'
        if service != '':
            meter_service = f',for="{service}"'

        return f'meter_{unit}_total{{bucket="{bucket}"{meter_service}{meter_unbilled}{meter_variant}}} (\d+)'

    @staticmethod
    def __credit_pattern(unit, bucket, service, unbilled='', variant=''):
        meter_service = ''
        meter_unbilled = ''
        meter_variant = ''
        if unbilled != '':
            meter_unbilled = f',unbilled="{unbilled}"'
        if variant != '':
            meter_variant = f',variant="{variant}"'
        if service != '':
            meter_service = f',for="{service}"'

        return f'credit_{unit}_total{{bucket="{bucket}"{meter_service}{meter_unbilled}{meter_variant}}} (\d+)'

    def get_credit_unit(self, bucket='default', unit = 'ru', service='kv', unbilled='', variant=''):
        total = 0
        credit_pattern = re.compile(metering.__credit_pattern(unit, bucket, service, unbilled, variant), re.MULTILINE)
        for node in self.nodes:
            if service in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth=self.auth, verify=False)
                if credit_pattern.search(response.text):
                    for credit in credit_pattern.findall(response.text):
                        total += int(credit)
        self.log.info(f'CREDIT {service} UNITS for {bucket}: {total} {unit}')
        return total

    def get_query_cu(self, bucket='default', unbilled='true', variant='eval'):
        cu = 0
        qry_cu_pattern = re.compile(metering.__meter_pattern('cu', bucket, 'n1ql', unbilled, variant))
        for node in self.nodes:
            if 'n1ql' in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth=self.auth, verify=False)
                if qry_cu_pattern.search(response.text):
                    cu += int(qry_cu_pattern.findall(response.text)[0])
        self.log.info(f'N1QL UNITS for {bucket}: {cu} CU')
        return cu

    def get_index_rwu(self, bucket='default', unbilled = '', variant = ''):
        ru, wu = 0, 0
        idx_ru_pattern = re.compile(metering.__meter_pattern('ru', bucket, 'index'))
        idx_wu_pattern = re.compile(metering.__meter_pattern('wu', bucket, 'index', unbilled, variant))
        for node in self.nodes:
            if 'index' in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth=self.auth, verify=False)
                if idx_ru_pattern.search(response.text):
                    ru += int(idx_ru_pattern.findall(response.text)[0])
                if idx_wu_pattern.search(response.text):
                    wu += int(idx_wu_pattern.findall(response.text)[0])
        self.log.info(f'INDEX UNITS for {bucket}: {ru} RU, {wu} WU')
        return ru, wu

    def get_fts_rwu(self, bucket='default', unbilled = '', variant = ''):
        ru, wu = 0, 0
        fts_ru_pattern = re.compile(metering.__meter_pattern('ru', bucket, 'fts', unbilled, variant))
        fts_wu_pattern = re.compile(metering.__meter_pattern('wu', bucket, 'fts', unbilled, variant))
        for node in self.nodes:
            if 'fts' in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth=self.auth, verify=False)
                if fts_ru_pattern.search(response.text):
                    ru += int(fts_ru_pattern.findall(response.text)[0])
                if fts_wu_pattern.search(response.text):
                    wu += int(fts_wu_pattern.findall(response.text)[0])
        self.log.info(f'FTS UNITS for {bucket}: {ru} RU, {wu} WU')
        return ru, wu

    def get_kv_rwu(self, bucket='default', unbilled ='', variant =''):
        ru, wu = 0, 0
        kv_ru_pattern = re.compile(metering.__meter_pattern('ru', bucket, 'kv', unbilled, variant))
        kv_wu_pattern = re.compile(metering.__meter_pattern('wu', bucket, 'kv', unbilled, variant))
        for node in self.nodes:
            if 'kv' in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth=self.auth, verify=False)
                if kv_ru_pattern.search(response.text):
                    ru += int(kv_ru_pattern.findall(response.text)[0])
                if kv_wu_pattern.search(response.text):
                    wu += int(kv_wu_pattern.findall(response.text)[0])
        self.log.info(f'KV UNITS for {bucket}: {ru} RU, {wu} WU')
        return ru, wu

    def assert_query_billing_unit(self, result, expected, unit="ru", service="kv"):
        if 'billingUnits' in result.keys():
            if unit in result['billingUnits'].keys():
                if service in result['billingUnits'][unit].keys():
                    actual = result['billingUnits'][unit][service]
                    # With indexer service there is a 10% variation do to collatejson encoding, so we want to account for that
                    if service == "index":
                        if (actual/expected) >= .85 and (actual/expected) <= 1.15:
                            return True, ''
                        else:
                            return False, f'Expected {expected} {service} {unit} unit but got {actual}'
                    else:
                        if (actual == expected):
                            return True, ''
                        else:
                            return False, f'Expected {expected} {service} {unit} unit but got {actual}'
                else:
                    return False, f"result['billingUnits'][{unit}] does not contain {service}, result['billingUnits'][{unit}] is: {result['billingUnits'][unit]}"
            else:
                return False, f"result['billingUnits'] does not contain {unit}, result['billingUnits'] is: {result['billingUnits']}"
        else:
            return False, f'result does not contain billingUnits, result is: {result}'

# Throttling limit name for each service are:
# - dataThrottleLimit
# - indexThrottleLimit
# - searchThrottleLimit
# - queryThrottleLimit
# - sgwReadThrottleLimit
# - sgwWriteThrottleLimit
# - dataStorageLimit
# - indexStorageLimit
# - searchStorageLimit
class throttling(object):
    def __init__(self, server, username, password):
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        self.url_bucket_throttle = f"https://{server}:{CbServer.ssl_port}/pools/default/buckets"
        self.url_cluster_throttle = f"https://{server}:{CbServer.ssl_port}/settings/serverless"
        self.url_query_settings = f"https://{server}:{CbServer.ssl_n1ql_port}/admin/settings"
        url = f"https://{server}:{CbServer.ssl_port}/pools/default"
        response = requests.get(url, auth=self.auth, verify=False)
        self.nodes = response.json()['nodes']
        self.log = logger.Logger.get_logger()

    def get_query_settings(self, setting='node-quota'):
        response = requests.get(self.url_query_settings, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to get query settings: {response.text}')
        settings = response.json()
        self.log.info(f'RETRIEVED {setting}: {settings[setting]}')
        return settings[setting]

    def set_query_settings(self, setting='node-quota', value=256):
        data = {}
        data[setting] = value
        response = requests.post(self.url_query_settings, data=f'{{"{setting}":{value}}}', auth=self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to set query settings: {response.text}')

    def get_bucket_limit(self, bucket = 'default', service='dataThrottleLimit'):
        response = requests.get(self.url_bucket_throttle + f"/{bucket}", auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to get bucket throttle limit: {response.text}')
        throttle_limits = response.json()
        self.log.info(f'RETRIEVED {service} for {bucket}: {throttle_limits[service]}')
        return throttle_limits[service]

    def set_bucket_limit(self, bucket= 'default', value=5000, service='dataThrottleLimit'):
        data = {}
        data[service] = value
        self.log.info(f'SETTING {service} for {bucket} to {value}')
        response = requests.post(self.url_bucket_throttle + f"/{bucket}", data=data, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to set bucket throttle limit: {response.text}')

    def get_cluster_limit(self, service='dataThrottleLimit'):
        response = requests.get(self.url_cluster_throttle, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to get cluster throttle limit: {response.text}')
        throttle_limits = response.json()
        self.log.info(f'RETRIEVED {service} for CLUSTER: {throttle_limits[service]}')
        return throttle_limits[service]

    def set_cluster_limit(self, value=5000, service='dataThrottleLimit'):
        data = {}
        data[service] = value
        self.log.info(f'setting {value} as throttling limit for CLUSTER for service: {service}')
        response = requests.post(self.url_cluster_throttle, data=data, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to set cluster throttle limit: {response.text}')

    def get_metrics(self, bucket='default', service='kv'):
        throttle_count_total, throttle_seconds_total = 0, 0
        throttle_seconds_pattern = re.compile(f'throttle_seconds_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        throttle_count_pattern = re.compile(f'throttle_count_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        for node in self.nodes:
            if service in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth = self.auth, verify=False)
                if response.status_code not in (200,201):
                    self.fail(f'Fail to get throttle metrics: {response.text}')
                if throttle_seconds_pattern.search(response.text):
                    throttle_seconds_total += int(throttle_seconds_pattern.findall(response.text)[0])
                if throttle_count_pattern.search(response.text):
                    throttle_count_total += int(throttle_count_pattern.findall(response.text)[0])
        self.log.info(f'{service.upper()} THROTTLE for {bucket}: {throttle_count_total} count, {throttle_seconds_total} seconds')
        return throttle_count_total, throttle_seconds_total

    def get_reject_count(self, bucket='default', service='kv'):
        reject_count_total = 0
        reject_count_pattern = re.compile(f'reject_count_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        for node in self.nodes:
            if service in node['services']:
                url = f"https://{node['hostname'][:-5]}:{CbServer.ssl_port}/metrics"
                response = requests.get(url, auth = self.auth, verify=False)
                if response.status_code not in (200,201):
                    self.fail(f'Fail to get throttle metrics: {response.text}')
                if reject_count_pattern.search(response.text):
                    reject_count_total += int(reject_count_pattern.findall(response.text)[0])
        self.log.info(f'{service.upper()} THROTTLE REJECTION for {bucket}: {reject_count_total} count')
        return reject_count_total


class KVRateLimiting(object):
    """Drive and observe KV rate-limiting on one cluster.

    The REST surface itself is on RestConnection:

        set_node_throttle_settings()   get_node_throttle_settings()
        set_bucket_throttle_limits()   get_bucket_throttle_limits()

    plus apply_conf_node_throttle_settings() / apply_bucket_throttle_params()
    for the conf driven path. This class is what sits on top of those and is
    not REST:

      - fan-out of *explicit* values across the KV nodes of a cluster. The
        conf driven entry point already fans out, but a test that changes
        limits mid-run has values in hand rather than in the conf, and node
        settings have to land on every data node.
      - the throttle counters, which come from cbstats. They are not on the
        REST API, and they are not the Prometheus meter_* series either -
        those belong to the serverless metering feature above and are not
        published on an on-prem (configProfile "default") cluster. Verified
        on 8.5.0-1257: 224 metric families exposed, not one of them a meter.
        TAF's reference suite, pytests/epengine/rate_limiting_tests.py, reads
        cbstats, and the feature spec's observability section is headed
        "KV STATS" and names ru_total rather than meter_ru_total.
      - counter deltas, the counter-to-rate conversion any limit derived from
        an observed workload needs, and a teardown that puts the cluster back
        the way it was found.

    `kv_nodes` must be the data service nodes: both the global memcached
    settings and the throttle counters are per node, so settings are fanned
    out across all of them and counters summed across all of them.
    """

    NODE_PARAMS = ("throttle_enabled", "node_capacity", "read_unit_size",
                   "write_unit_size")

    COUNTERS = {
        "ru_total": "ru",
        "wu_total": "wu",
        "throttle_count_total": "throttle_count",
        "reject_count_total": "reject_count",
        "throttle_duration": "throttle_duration",
        "throttle_seconds_total": "throttle_duration",
    }

    ZERO_COUNTERS = {"ru": 0, "wu": 0, "throttle_count": 0,
                     "reject_count": 0, "throttle_duration": 0}

    # What an unlimited 8.5 bucket reports for throttle_hard_limit (2^64 - 1),
    # and therefore what clearing a limit has to put back. Clearing it to 0
    # instead would pin the bucket at no throughput at all
    UNLIMITED_UNITS = 18446744073709551615

    # Used only to explain a zero reading: if the expected counters are
    # missing, the throttle-shaped names that *are* present are worth more
    # than a bare zero
    _COUNTER_HINT = re.compile(
        r"throttle|reject|_ru_|_wu_|ru_total|wu_total|unit", re.IGNORECASE)

    def __init__(self, kv_nodes, log=None):
        if not kv_nodes:
            raise ValueError("KVRateLimiting needs at least one KV node")
        self.kv_nodes = list(kv_nodes)
        self.log = log or logger.Logger.get_logger()

    # -- configuration ----------------------------------------------------

    def set_node_settings(self, throttle_enabled=None, node_capacity=None,
                          read_unit_size=None, write_unit_size=None):
        """Set node level throttle params, with explicit values, on every KV
        node of the cluster."""
        params = dict()
        if throttle_enabled is not None:
            params["throttle_enabled"] = throttle_enabled
        if node_capacity is not None:
            params["node_capacity"] = int(node_capacity)
        if read_unit_size is not None:
            params["read_unit_size"] = int(read_unit_size)
        if write_unit_size is not None:
            params["write_unit_size"] = int(write_unit_size)
        if not params:
            return

        for node in self.kv_nodes:
            RestConnection(node).set_node_throttle_settings(**params)
        self.log.info("Node throttle params {0} set on {1} KV node(s)"
                      .format(params, len(self.kv_nodes)))

    def get_node_settings(self):
        """Node level throttle params as the first KV node reports them"""
        return RestConnection(self.kv_nodes[0]).get_node_throttle_settings()

    def set_bucket_limits(self, bucket, throttle_reserved=None,
                          throttle_hard_limit=None, throttle_enabled=None):
        """Set the throttle limits of an existing bucket.

        0 is a legal value for both limits and means something quite
        different from 'unset', hence 'is not None' on the way in.
        """
        RestConnection(self.kv_nodes[0]).set_bucket_throttle_limits(
            bucket, throttle_reserved=throttle_reserved,
            throttle_hard_limit=throttle_hard_limit,
            throttle_enabled=throttle_enabled)
        self.log.info("Bucket {0} throttle limits set: reserved={1} "
                      "hard_limit={2}".format(bucket, throttle_reserved,
                                              throttle_hard_limit))

    def get_bucket_limits(self, bucket='default'):
        """Throttle limits the cluster reports for a bucket"""
        return RestConnection(
            self.kv_nodes[0]).get_bucket_throttle_limits(bucket)

    def reset(self, bucket_names=(), restore=None):
        """Put the cluster back: clear bucket limits, restore node settings.

        `restore` is a get_node_settings() snapshot taken before the test
        touched anything; without one the master toggle is simply turned off.

        Meant for tearDown, so it never raises - a cluster left throttled
        slows down every suite that runs on it next and is worth a loud log,
        but it must not mask the failure that brought us here.
        """
        for bucket_name in bucket_names:
            try:
                self.set_bucket_limits(
                    bucket_name, throttle_reserved=0,
                    throttle_hard_limit=self.UNLIMITED_UNITS)
            except Exception as err:
                self.log.error("Could not clear throttle limits on bucket "
                               "{0}: {1}".format(bucket_name, err))

        settings = dict(restore or {})
        # Never leave the feature on because it was found on by accident; an
        # explicit snapshot value is honoured, anything else defaults to off
        settings.setdefault("throttle_enabled", False)
        if settings.get("throttle_enabled"):
            self.log.warning("Restoring throttle_enabled=true: this cluster "
                             "already had KV throttling on before the test")
        if restore is not None and "node_capacity" not in restore:
            self.log.warning("node_capacity was unset before this test and "
                             "cannot be unset again through this API; it is "
                             "left at the value the test configured")
        try:
            self.set_node_settings(**settings)
        except Exception as err:
            self.log.error("Could not restore node throttle settings - this "
                           "cluster may be left throttled: {0}".format(err))

    def get_counters(self, bucket_name):
        """Throttle counters for one bucket, summed over the KV nodes.

        Returns ru, wu, throttle_count, reject_count and throttle_duration.
        A counter the build does not expose reads as 0, so a caller asserting
        on a delta is asserting on something it actually observed.
        """
        totals = dict(self.ZERO_COUNTERS)
        for stats in self._all_stats(bucket_name):
            for key, value in self._parse_stats(stats).items():
                totals[key] += value
        self.log.info("KV throttle counters for bucket {0}: {1}"
                      .format(bucket_name, totals))
        return totals

    def available_counters(self, bucket_name):
        """Throttle-shaped stat names cbstats actually returns for a bucket.

        Only used to explain a zero reading, so that the real names can be
        read off one failure instead of costing a separate investigation.
        """
        seen = dict()
        for stats in self._all_stats(bucket_name):
            for name, value in stats.items():
                if self._COUNTER_HINT.search(name):
                    seen[name] = value
        return seen

    def _all_stats(self, bucket_name):
        """cbstats 'all' for the bucket, once per KV node.

        One shell connection per node per call, always closed.
        """
        for node in self.kv_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                yield Cbstats(shell).all_stats(bucket_name)
            finally:
                shell.disconnect()

    @classmethod
    def _parse_stats(cls, stats):
        found = dict(cls.ZERO_COUNTERS)
        for name, key in cls.COUNTERS.items():
            if name not in stats:
                continue
            try:
                value = float(str(stats[name]).strip())
            except (TypeError, ValueError):
                continue
            # Everything but throttle_duration is a whole-unit counter; keep
            # those as ints so the logs read as counts rather than floats
            found[key] += int(value) if value.is_integer() else value
        return found

    @staticmethod
    def counters_delta(before, after):
        """after - before, per counter.

        Clamped at 0: memcached restarting mid-window resets the counters,
        and a negative 'consumption' would silently poison a derived limit.
        """
        return dict((key, max(0, after.get(key, 0) - before.get(key, 0)))
                    for key in set(before) | set(after))

    @staticmethod
    def units_per_sec(delta, elapsed_secs):
        """Observed RU+WU rate over a window, in units/sec.

        throttle_reserved and throttle_hard_limit are rates while ru_total
        and wu_total are monotonic counters, so this is the conversion any
        limit derived from an observed workload has to go through.
        """
        if elapsed_secs <= 0:
            return 0
        return (delta.get("ru", 0) + delta.get("wu", 0)) / float(elapsed_secs)


# On-prem KV rate-limiting (8.5+): kv_* series per bucket, no service label.
# kv_throttle_duration_seconds_* stays 0 for DCP before 8.5.0-1248 (MB-74217).
class kv_throttle_stats(object):

    COUNTERS = ("kv_ru_total", "kv_wu_total",
                "kv_throttle_count_total", "kv_reject_count_total",
                "kv_throttle_duration_seconds_count",
                "kv_throttle_duration_seconds_sum")
    LIMITS = ("kv_throttle_hard_limit", "kv_throttle_reserved")
    METRICS = COUNTERS + LIMITS

    # "no limit" (2^64-1) arrives in exponent form, so compare by magnitude
    UNLIMITED_ABOVE = 10 ** 18

    @staticmethod
    def is_unlimited(hard_limit):
        return hard_limit > kv_throttle_stats.UNLIMITED_ABOVE

    def __init__(self, servers, username, password):
        """servers: KV nodes to scrape; counters are summed across them."""
        if not isinstance(servers, (list, tuple, set)):
            servers = [servers]
        self.servers = list(servers)
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        self.log = logger.Logger.get_logger()

    def __url(self, server):
        ip = server.ip if hasattr(server, 'ip') else server
        if CbServer.use_https:
            return f"https://{ip}:{CbServer.ssl_port}/metrics"
        return f"http://{ip}:{CbServer.port}/metrics"

    @staticmethod
    def __pattern(metric, bucket):
        return re.compile(r'^%s\{bucket="%s"\} ([0-9.e+-]+)$'
                          % (metric, re.escape(bucket)), re.MULTILINE)

    def get_bucket_stats(self, bucket):
        bucket_name = bucket.name if hasattr(bucket, 'name') else bucket
        stats = {metric: 0 for metric in kv_throttle_stats.METRICS}
        for server in self.servers:
            response = requests.get(self.__url(server), auth=self.auth,
                                    verify=False)
            if response.status_code not in (200, 201):
                raise Exception(f'Failed to read metrics from '
                                f'{self.__url(server)}: {response.text}')
            for metric in kv_throttle_stats.METRICS:
                found = kv_throttle_stats.__pattern(
                    metric, bucket_name).findall(response.text)
                if not found:
                    continue
                value = int(float(found[0]))
                if metric in kv_throttle_stats.LIMITS:
                    # per-bucket setting, not a per-node quantity
                    stats[metric] = value
                else:
                    stats[metric] += value
        stats['ts'] = time.time()
        self.log.info(f'KV THROTTLE STATS for {bucket_name}: {stats}')
        return stats

    @staticmethod
    def delta(before, after):
        elapsed = after['ts'] - before['ts']
        # counters going backwards means the bucket was recreated mid-test
        recreated = any(after[metric] < before[metric]
                        for metric in kv_throttle_stats.COUNTERS)
        result = {metric: after[metric] - (0 if recreated else before[metric])
                  for metric in kv_throttle_stats.COUNTERS}
        result['bucket_recreated'] = recreated
        result['elapsed'] = round(elapsed, 3)
        for metric in kv_throttle_stats.LIMITS:
            result[metric] = after[metric]
        waits = result['kv_throttle_duration_seconds_count']
        result['throttle_wait_mean'] = round(
            result['kv_throttle_duration_seconds_sum'] / waits, 4) if waits else 0
        if elapsed > 0:
            result['ru_per_sec'] = round(result['kv_ru_total'] / elapsed, 2)
            result['wu_per_sec'] = round(result['kv_wu_total'] / elapsed, 2)
            result['units_per_sec'] = round(
                (result['kv_ru_total'] + result['kv_wu_total']) / elapsed, 2)
        else:
            result['ru_per_sec'] = result['wu_per_sec'] = 0
            result['units_per_sec'] = 0
        return result

    def get_bucket_limits(self, bucket):
        stats = self.get_bucket_stats(bucket)
        return stats['kv_throttle_reserved'], stats['kv_throttle_hard_limit']


# Same counters over cbstats, as a drop-in for kv_throttle_stats.
class cbstats_throttle_stats(kv_throttle_stats):

    def __init__(self, servers, username=None, password=None):
        kv_throttle_stats.__init__(self, servers, username, password)

    def get_bucket_stats(self, bucket):
        bucket_name = bucket.name if hasattr(bucket, 'name') else bucket
        stats = {metric: 0 for metric in kv_throttle_stats.METRICS}
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            try:
                raw = Cbstats(shell).all_stats(bucket_name)
            finally:
                shell.disconnect()
            for metric in kv_throttle_stats.METRICS:
                cbstats_name = metric[len("kv_"):]
                if cbstats_name not in raw:
                    continue
                value = int(float(raw[cbstats_name]))
                if metric in kv_throttle_stats.LIMITS:
                    stats[metric] = value
                else:
                    stats[metric] += value
        stats['ts'] = time.time()
        self.log.info(f'KV THROTTLE STATS (cbstats) for {bucket_name}: '
                      f'{stats}')
        return stats
