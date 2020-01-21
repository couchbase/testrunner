import copy
import re
from tuqquery.tuq import QueryTests
from couchbase_helper.documentgenerator import DocumentGenerator


JOIN_INNER = "INNER"
JOIN_LEFT = "LEFT"

class JoinQueryTests(QueryTests):
    def setUp(self):
        super(JoinQueryTests, self).setUp()
        self.type_join = self.input.param("type_join", JOIN_INNER)
        self.full_list = self.generate_full_docs_list(self.gens_load)
        self.additional_set = self.generate_docs_additional()
        self.load(self.additional_set)
        
    def suite_setUp(self):
        super(JoinQueryTests, self).suite_setUp()

    def tearDown(self):
        super(JoinQueryTests, self).tearDown()

    def suite_tearDown(self):
        super(JoinQueryTests, self).suite_tearDown()


##############################################################################################
#
#   SIMPLE CHECKS
##############################################################################################
    def test_simple_join_keys(self):
        for bucket in self.buckets:
            query_template = "SELECT employee.$str0, employee.$list_str0, new_project " +\
            "FROM %s as employee %s JOIN default.project as new_project " % (bucket.name, self.type_join) +\
            "ON KEYS employee.$list_str0"
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)


##############################################################################################
#
#   COMMON FUNCTIONS
##############################################################################################

    def run_join_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        self.gen_results.full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
        where_clause = re.findall(r'WHERE.*', query_template)[0]
        query_template = re.sub(r'JOIN.*', '', query_template) + where_clause
        self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def generate_docs_additional(self):
        generators = []
        start, end = 0, 0
        join_keys = self.gen_results.type_args['list_str'][0]
        key_prefixes = list({doc[join_keys] for doc in self.gen_results.full_list})[:-1]
        template = '{{ "add_name":"{0}", "project": "{1}"}}'
        for key_prefix in key_prefixes:
            generators.append(DocumentGenerator(key_prefix, template,
                                                ["test-%s" % i for i in range(0, 10)],
                                                ["Project%s" % start],
                                                start=start, end=start+10))
            start, end = end, end+10
        return generators

    def _generate_full_joined_docs_list(self, join_type=JOIN_INNER,
                                        particular_key=None):
        join_keys = re.sub(r'.*ON KEYS', '', self.query).split('.')[-1].strip()
        joining_key = re.sub(r'ON KEYS.*', '', re.sub(r'JOIN.*', '', self.query)).strip().split(' ')[0].split('.')[-1]
        joining_key_alias = re.sub(r'ON KEYS.*', '', re.sub(r'JOIN.*', '', self.query)).strip().split(' ')[-1]
        joined_list = []
        all_docs_list = copy.deepcopy(self.full_list)
        if join_type.upper() == JOIN_INNER:
            for item in all_docs_list:
                keys = item[join_keys]
                if particular_key is not None:
                    keys=[item[join_keys][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                for tasks_item in tasks_items:
                    item_to_add = copy.deepcopy(item)
                    item_to_add.update({joining_key_alias: tasks_item[joining_key]})
                    joined_list.append(item_to_add)
        elif join_type.upper() == JOIN_LEFT:
            for item in all_docs_list:
                keys = item[join_keys]
                if particular_key is not None:
                    keys=[item[join_keys][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                for key in keys:
                    item_to_add = copy.deepcopy(item)
                    if key in [doc["_id"] for doc in tasks_items]:
                        item_to_add.update([{joining_key_alias: doc[joining_key]} for doc in tasks_items if key == doc['_id']][0])
                        joined_list.append(item_to_add)
            joined_list.extend([{}] * self.gens_tasks[-1].end)
        else:
            raise Exception("Unknown type of join")
        return joined_list