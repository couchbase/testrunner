import re
import requests
from lib.Cb_constants.CBServer import CbServer

class metering(object):
    def __init__(self, server, username, password):
        self.url_query_metering = f"https://{server}:{CbServer.ssl_n1ql_port}/_metering"
        self.url_index_metering = f"https://{server}:{CbServer.ssl_index_port}/_metering"
        self.url_fts_metering = f"https://{server}:{CbServer.ssl_fts_port}/_metering"
        self.url_metrics = f"https://{server}:{CbServer.ssl_port}/metrics"

        self.auth = requests.auth.HTTPBasicAuth(username, password)

    def get_query_cu(self, bucket='default', variant='eval', unbilled='true'):
        cu = 0
        qry_cu_pattern = re.compile(f'meter_cu_total{{bucket="{bucket}",for="n1ql",unbilled="{unbilled}",variant="{variant}"}} (\d+)')
        response = requests.get(self.url_query_metering, auth=self.auth, verify=False)
        if qry_cu_pattern.search(response.text):
            cu = int(qry_cu_pattern.findall(response.text)[0])
        return cu

    def get_index_rwu(self, bucket='default', unbilled = '', variant = ''):
        ru, wu = 0, 0
        idx_ru_pattern = re.compile(f'meter_ru_total{{bucket="{bucket}",for="index",unbilled="{unbilled}",variant="{variant}"}} (\d+)')
        idx_wu_pattern = re.compile(f'meter_wu_total{{bucket="{bucket}",for="index",unbilled="{unbilled}",variant="{variant}"}} (\d+)')
        response = requests.get(self.url_index_metering, auth=self.auth, verify=False)
        if idx_ru_pattern.search(response.text):
            ru = int(idx_ru_pattern.findall(response.text)[0])
        if idx_wu_pattern.search(response.text):
            wu = int(idx_wu_pattern.findall(response.text)[0])
        return ru, wu

    def get_fts_rwu(self, bucket='default', unbilled = '', variant = ''):
        ru, wu = 0, 0
        fts_ru_pattern = re.compile(f'meter_ru_total{{bucket="{bucket}",for="fts",unbilled="{unbilled}",variant="{variant}"}} (\d+)')
        fts_wu_pattern = re.compile(f'meter_wu_total{{bucket="{bucket}",for="fts",unbilled="{unbilled}",variant="{variant}"}} (\d+)')
        response = requests.get(self.url_fts_metering, auth=self.auth, verify=False)
        if fts_ru_pattern.search(response.text):
            ru = int(fts_ru_pattern.findall(response.text)[0])
        if fts_wu_pattern.search(response.text):
            wu = int(fts_wu_pattern.findall(response.text)[0])
        return ru, wu

    def get_kv_rwu(self, bucket='default'):
        ru, wu = 0, 0
        response = requests.get(self.url_metrics, auth=self.auth, verify=False)
        kv_ru_pattern = re.compile(f'meter_ru_total{{bucket="{bucket}"}} (\d+)')
        kv_wu_pattern = re.compile(f'meter_wu_total{{bucket="{bucket}"}} (\d+)')
        if kv_ru_pattern.search(response.text):
            ru = int(kv_ru_pattern.findall(response.text)[0])
        if kv_wu_pattern.search(response.text):
            wu = int(kv_wu_pattern.findall(response.text)[0])
        return ru, wu

    def assert_query_billing_unit(self, result, expected, unit="ru", service="kv"):
        if 'billingUnits' in result.keys():
            if unit in result['billingUnits'].keys():
                if service in result['billingUnits'][unit].keys():
                    actual = result['billingUnits'][unit][service]
                    self.assertEqual(actual, expected, f'Expected {expected} {service} {unit} unit but got {actual}')
                else:
                    self.fail(f"result['billingUnits'][{unit}] does not contain {service}, result['billingUnits'][{unit}] is: {result['billingUnits'][unit]}")
            else:
                self.fail(f"result['billingUnits'] does not contain {unit}, result['billingUnits'] is: {result['billingUnits']}")
        else:
            self.fail(f'result does not contain billingUnits, result is: {result}')

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
        self.url_metrics = f"https://{server}:{CbServer.ssl_port}/metrics"
        self.url_bucket_throttle = f"https://{server}:{CbServer.ssl_port}/pools/default/buckets"
        self.url_cluster_throttle = f"https://{server}:{CbServer.ssl_port}/internalSettings"

    def get_bucket_limit(self, bucket = 'default', service='dataThrottleLimit'):
        response = requests.get(self.url_bucket_throttle + f"/{bucket}", auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to get bucket throttle limit: {response.text}')
        throttle_limits = response.json()
        return throttle_limits[service]

    def set_bucket_limit(self, bucket= 'default', value=5000, service='dataThrottleLimit'):
        data = {}
        data[service] = value
        response = requests.post(self.url_bucket_throttle + f"/{bucket}", data=data, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to set bucket throttle limit: {response.text}')

    def get_cluster_limit(self, service='dataThrottleLimit'):
        response = requests.get(self.url_cluster_throttle, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to get cluster throttle limit: {response.text}')
        throttle_limits = response.json()
        return throttle_limits[service]

    def set_cluster_limit(self, value=5000, service='dataThrottleLimit'):
        data = {}
        data[service] = value
        response = requests.post(self.url_cluster_throttle, data=data, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to set cluster throttle limit: {response.text}')

    def get_metrics(self, bucket='default', service='kv'):
        throttle_count_total, throttle_seconds_total = 0, 0
        throttle_seconds_pattern = re.compile(f'throttle_seconds_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        throttle_count_pattern = re.compile(f'throttle_count_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        response = requests.get(self.url_metrics, auth = self.auth, verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to get throttle metrics: {response.text}')
        if throttle_seconds_pattern.search(response.text):
            throttle_seconds_total = int(throttle_seconds_pattern.findall(response.text)[0])
        if throttle_count_pattern.search(response.text):
            throttle_count_total = int(throttle_count_pattern.findall(response.text)[0])
        return throttle_count_total, throttle_seconds_total
