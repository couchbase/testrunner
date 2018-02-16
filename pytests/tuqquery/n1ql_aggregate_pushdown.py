import logging

from couchbase_helper.tuq_helper import N1QLHelper
from tuq import QueryTests


log = logging.getLogger(__name__)

AGGREGATE_FUNCTIONS = ["SUM", "MIN", "MAX", "COUNT", "COUNTN", "AVG"]

class AggregatePushdownClass(QueryTests):
    def setUp(self):
        super(AggregatePushdownClass, self).setUp()
        self.n1ql_helper = N1QLHelper(master=self.master)
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")

    def tearDown(self):
        super(AggregatePushdownClass, self).tearDown()

    def test_array_aggregate_non_array_group_non_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            for create_def in index_name_def["create_definitions"]:
                result = self.run_cbq_query(create_def)
                create_index_count += 1
            array_field = index_name_def["fields"][0]
            non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
            for aggr_func in AGGREGATE_FUNCTIONS:
                select_non_array_where_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                select_array_where_clause = "SELECT " + aggr_func + "(d.{0}) from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
                for non_array_first_field in non_array_fields:
                    for non_array_second_field in non_array_fields:
                        if non_array_second_field == non_array_first_field:
                            query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      non_array_first_field["where_clause"],
                                                                                      non_array_first_field["name"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                                                  array_field["where_clause"],
                                                                                  non_array_first_field["name"])
                                                ]
                        else:
                            query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      non_array_first_field["where_clause"],
                                                                                      non_array_first_field["name"]),
                                                 select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      non_array_first_field["where_clause"],
                                                                                      non_array_second_field["name"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                                                  array_field["where_clause"],
                                                                                  non_array_first_field["name"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                                                  array_field["where_clause"],
                                                                                  non_array_second_field["name"]),
                                                 select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      non_array_second_field["where_clause"],
                                                                                      non_array_first_field["name"]),
                                                 select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      non_array_second_field["where_clause"],
                                                                                      non_array_second_field["name"])
                                                 ]
                        for bucket in self.buckets:
                            for query_template in query_definitions:
                                for index_name in index_name_def["index_names"]:
                                    query = query_template % (bucket.name, index_name)
                                    result = self.run_cbq_query(query)
                                    query_count += 1
                                    explain_verification = self._verify_aggregate_explain_results(query_template,
                                                                                                  index_name,
                                                                                                  index_name_def["fields"],
                                                                                                  bucket.name)
                                    if explain_verification is False:
                                        failed_queries_in_explain.append(query)
                                        log.info("Query {0} failed in explain".format(query))
                                    query_verification = self._verify_aggregate_query_results(result, query_template,
                                                                                              bucket.name)
                                    if query_verification is not True:
                                        failed_queries_in_result.append(query)
                                        log.info(query_verification)

            for drop_def in index_name_def["drop_definitions"]:
                result = self.run_cbq_query(drop_def)
        log.info("create_index_count: " + str(create_index_count))
        log.info("query_count: " + str(query_count))

    def _create_array_index_definitions(self):
        array_fields = [{"name": "travel_history", "where_clause": "UNNEST d.`travel_history` AS t where t > 'I'"},
                        {"name": "travel_details", "where_clause": "UNNEST d.`travel_details` AS t where t.country > 'I'"},
                        {"name": "credit_cards", "where_clause": "UNNEST d.`credit_cards` AS t where t < 99999"}]
        non_array_fields = [{"name": "name", "where_clause": "name > 'J'"},
                            {"name": "age", "where_clause": "age < 55"},
                            {"name": "email", "where_clause": "email is not null"}]
        create_index_definitions = []
        for arr_field in array_fields:
            arr_field_defn = "ALL ARRAY t FOR t in `{0}` END".format(arr_field["name"])
            for first_field in non_array_fields[:-1]:
                for second_field in non_array_fields[non_array_fields.index(first_field)+1:]:
                    if first_field["name"] == second_field["name"]:
                        continue
                    index_names_defn = {}
                    index_names = ["{0}_{1}_{2}".format(first_field["name"], second_field["name"], arr_field["name"]),
                                   "{0}_{1}_{2}".format(first_field["name"], arr_field["name"], second_field["name"]),
                                   "{0}_{1}_{2}".format(arr_field["name"], first_field["name"], second_field["name"])]
                    index_names_defn["index_names"] = index_names
                    index_names_defn["fields"] = [arr_field, first_field, second_field]
                    create_index_clause = "CREATE INDEX {0} on %s ({1}, {2}, {3})"
                    temp_create_defns = [create_index_clause.format(index_names[0], first_field["name"], second_field["name"], arr_field_defn),
                                         create_index_clause.format(index_names[1], first_field["name"], arr_field_defn, second_field["name"]),
                                         create_index_clause.format(index_names[2], arr_field_defn, first_field["name"], second_field["name"])]
                    drop_index_clause = "DROP INDEX %s.{0}"
                    temp_delete_defns = [drop_index_clause.format(index_names[0]),
                                         drop_index_clause.format(index_names[1]),
                                         drop_index_clause.format(index_names[2])]
                    index_names_defn["create_definitions"] = []
                    index_names_defn["drop_definitions"] = []
                    create_index_definitions.append(index_names_defn)
                    for bucket in self.buckets:
                        bucket_name = bucket.name
                        for definition in temp_create_defns:
                            index_names_defn["create_definitions"].append(definition % bucket_name)
                        for definition in temp_delete_defns:
                            index_names_defn["drop_definitions"].append(definition % bucket_name)
                    yield index_names_defn

    def _verify_aggregate_explain_results(self, query, index_name, index_fields, bucket, allow_pushdown=True):
        for field in index_fields:
            if field["name"] in query.split("where")[1].split("GROUP")[0] or \
                            field["name"] in \
                            query.split("USE INDEX")[1].split(")")[1].split("GROUP")[0]:
                if not index_name.startswith(field["name"]):
                    allow_pushdown = False
            if "UNNEST" not in query:
                if "DISTINCT" not in index_name:
                    for aggr_func in ["SUM", "COUNT", "COUNTN", "AVG"]:
                        if aggr_func in query:
                            allow_pushdown = False
                            break

        explain_query = "EXPLAIN " + query
        explain_query = explain_query % (bucket, index_name)
        explain_result = self.run_cbq_query(explain_query)
        if allow_pushdown:
            return "index_group_aggs" in str(explain_result)
        else:
            return "index_group_aggs" not in str(explain_result)

    def _verify_aggregate_query_results(self, result, query, bucket):
        primary_query = query % (bucket, "#primary")
        actual_result = sorted(self.run_cbq_query(primary_query)["results"])
        expected_result = sorted(result["results"])

        def _gen_dict(res):
            result_set = []
            if res is not None and len(res) > 0:
                for val in res:
                    for key in val.keys():
                        result_set.append(val[key])
            return result_set
        log.info(" Analyzing Actual Result")
        actual_result = _gen_dict(actual_result)
        log.info(" Analyzing Expected Result")
        expected_result = _gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            return False
        if actual_result != expected_result:
            return False
        return True
