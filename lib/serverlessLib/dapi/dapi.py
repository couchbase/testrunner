import logging
import requests
import base64
import json

log = logging.getLogger()

class RestfulDAPI:
    def __init__(self, args):
        self.endpoint = "https://" + args.get("dapi_endpoint")
        self.endpoint_v1 = "https://" + args.get("dapi_endpoint") + "/v1"
        self.username = args.get("access_token")
        self.password = args.get("access_secret")
        self.header = self._create_headers(self.username, self.password)
        self._log = logging.getLogger(__name__)

    def _create_headers(self, username=None, password=None, contentType='application/json', connection='close'):
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        return {'Content-Type': contentType,
                'Authorization': 'Basic %s' % authorization,
                'Connection': connection,
                'Accept': '*/*'}

    def _urllib_request(self, api, method='GET', headers=None,
                        params={}, timeout=300, verify=False):
        session = requests.Session()
        headers = headers or self.header
        params = json.dumps(params)
        try:
            if method == "GET":
                resp = session.get(api, params=params, headers=headers,
                                   timeout=timeout, verify=verify)
            elif method == "POST":
                resp = session.post(api, data=params, headers=headers,
                                    timeout=timeout, verify=verify)
            elif method == "DELETE":
                resp = session.delete(api, data=params, headers=headers,
                                      timeout=timeout, verify=verify)
            elif method == "PUT":
                resp = session.put(api, data=params, headers=headers,
                                   timeout=timeout, verify=verify)
            return resp
        except requests.exceptions.HTTPError as errh:
            self._log.error("HTTP Error {0}".format(errh))
        except requests.exceptions.ConnectionError as errc:
            self._log.error("Error Connecting {0}".format(errc))
        except requests.exceptions.Timeout as errt:
            self._log.error("Timeout Error: {0}".format(errt))
        except requests.exceptions.RequestException as err:
            self._log.error("Something else: {0}".format(err))
        except Exception as err:
            self._log.error("Something else: {0}".format(err))

    def check_dapi_health(self):
        url = self.endpoint + "/health"
        return self._urllib_request(url)

    def insert_doc(self, doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" \
            + collection + "/docs/" + doc_id
        return self._urllib_request(url, method="POST", params=params)

    def get_doc(self, doc_id, scope, collection):
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" \
              + collection + "/docs/" + doc_id
        return self._urllib_request(url)

    def upsert_doc(self, existing_doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" \
            + collection + "/docs/" + existing_doc_id + "?upsert=true"
        return self._urllib_request(url, method="PUT", params=params)

    def delete_doc(self, existing_doc_id, scope, collection):
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" \
            + collection + "/docs/" + existing_doc_id
        return self._urllib_request(url, method="DELETE")

    def get_scope_list(self):
        url = self.endpoint_v1 + "/scopes"
        return self._urllib_request(url)

    def get_scope_detail(self, scopeName):
        url = self.endpoint_v1 + "/scopes/" + scopeName
        return self._urllib_request(url)

    def get_collection_detail(self, scopeName, collectionName):
        url = self.endpoint_v1 + "/scopes/" + scopeName + "/collections/" + collectionName
        return self._urllib_request(url)

    def get_collection_list(self, scope="_default"):
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections"
        return self._urllib_request(url)

    def get_document_list(self, scope="_default", collection="_default"):
        authorization = base64.b64encode('{}:{}'.format(self.username, self.password).encode()).decode()
        header = {'Authorization': 'Basic %s' % authorization,
                  'Accept': '*/*'}
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/docs"
        return self._urllib_request(url, headers=header)

    def get_subdoc(self, doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/subdocs/" + doc_id
        return self._urllib_request(url, method="POST", params=params)

    def insert_subdoc(self, doc_id, doc_content, scope, collection):
        params = doc_content
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/subdocs/" + doc_id
        return self._urllib_request(url, method="POST", params=params)

    def execute_query(self, query, scope):
        params = query
        url = self.endpoint_v1 + "/scopes/" + scope + "/query?"
        return self._urllib_request(url, method="POST", params=params)

    def create_scope(self, scope_name):
        params = scope_name
        url = self.endpoint_v1 + "/scopes"
        return self._urllib_request(url, method="POST", params=params)

    def create_collection(self, scope, collection_name):
        params = collection_name
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections"
        return self._urllib_request(url, method="POST", params=params)

    def delete_collection(self, scope, collection):
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection
        return self._urllib_request(url, method="DELETE")

    def delete_scope(self, scope):
        url = self.endpoint_v1 + "/scopes/" + scope
        return self._urllib_request(url, method="DELETE")

    def get_bulk_document(self, scope, collection, document_ids=()):
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/docs?ids="
        url = url + ','.join(document_ids)
        return self._urllib_request(url)

    def insert_bulk_document(self, scope, collection, key_documents):
        param = key_documents
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/docs"
        return self._urllib_request(url, method="POST", params=param)

    def delete_bulk_document(self, scope, collection, document_ids=[]):
        param = document_ids
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/docs"
        return self._urllib_request(url, method="DELETE", params=param)

    def update_bulk_document(self, scope, collection, key_documents):
        param = key_documents
        url = self.endpoint_v1 + "/scopes/" + scope + "/collections/" + collection + "/docs"
        return self._urllib_request(url, method="PUT", params=param)

    def get_bucket_info(self, bucket_name):
        url = self.endpoint_v1 + "/buckets/" + bucket_name
        return self._urllib_request(url)

    def get_bucket_list(self):
        url = self.endpoint_v1 + "/buckets"
        return self._urllib_request(url)