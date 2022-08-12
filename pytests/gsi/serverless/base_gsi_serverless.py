import logging
import requests

from gsi.serverless.base_query_serverless import QueryBaseServerless
from couchbase_helper.query_definitions import SQLDefinitionGenerator

log = logging.getLogger(__name__)


class BaseGSIServerless(QueryBaseServerless):

    def setUp(self):
        super(BaseGSIServerless, self).setUp()
        query_definition_generator = SQLDefinitionGenerator()
        self.dataset = self.input.param("dataset", "default")
        if self.dataset == "default" or self.dataset == "employee":
            self.query_definitions = query_definition_generator.generate_employee_data_query_definitions()
        if self.dataset == "simple":
            self.query_definitions = query_definition_generator.generate_simple_data_query_definitions()
        if self.dataset == "sabre":
            self.query_definitions = query_definition_generator.generate_sabre_data_query_definitions()
        if self.dataset == "bigdata":
            self.query_definitions = query_definition_generator.generate_big_data_query_definitions()
        if self.dataset == "array":
            self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()

    def tearDown(self):
        super(BaseGSIServerless, self).tearDown()

    def create_index(self, database_obj, query_statement):
        api = "https://{}:18093/query/service".format(database_obj.nebula)
        payload = {"query_context": "default:{}".format(database_obj.id),
                   "statement": query_statement}
        resp = requests.post(api, params=payload, auth=(database_obj.access_key, database_obj.secret_key))
        resp.raise_for_status()
        return resp.json()

    def get_indexer_metadata(self, database_obj, indexer_node):
        endpoint = "https://{}:18091/indexStatus".format(indexer_node)
        resp = requests.get(endpoint, auth=(database_obj.admin_username, database_obj.admin_password), verify=False)
        resp.raise_for_status()
        return resp.json()
