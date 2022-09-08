import requests

from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase


class QueryBaseServerless(ServerlessBaseTestCase):
    def setUp(self):
        super(QueryBaseServerless, self).setUp()

    def tearDown(self):
        super(QueryBaseServerless, self).tearDown()

    def run_query(self, query, database_obj, query_params=None, use_sdk=False, **kwargs):
        if use_sdk:
            cluster = self.get_sdk_cluster(database_obj.id)
            row_iter = cluster.query(query)
            return row_iter.rows()
        else:
            if not query_params:
                query_params = {'query_context': "default:{}".format(database_obj.id)}
                for key, value in kwargs.items():
                    if key == "txtimeout":
                        query_params['txtimeout'] = value
                    if key == "txnid":
                        query_params['txid'] = ''"{0}"''.format(value)
                    if key == "scan_consistency":
                        query_params['scan_consistency'] = value
                    if key == "scan_vector":
                        query_params['scan_vector'] = str(value).replace("'", '"')
                    if key == "timeout":
                        query_params['timeout'] = value
            api = "https://{}:18093/query/service".format(database_obj.nebula)
            query_params["statement"] = query
            resp = requests.post(api, params=query_params, auth=(database_obj.access_key, database_obj.secret_key))
            resp.raise_for_status()
            return resp.json()
