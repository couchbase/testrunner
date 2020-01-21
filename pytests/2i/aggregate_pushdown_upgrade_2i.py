import logging
import random

from membase.api.rest_client import RestConnection, RestHelper
from .int64_upgrade_2i import UpgradeSecondaryIndexInt64

log = logging.getLogger(__name__)
INT64_VALUES = [-9223372036854775808, 9223372036854775807, 9223372036854770000, 9000000000000000000,
                -9000000000000000000, -9223372036854770000, 5464748874972.17865, -5464748874972.17865,
                -2147483648, 2147483647, -2147483600, 2147483600, 32767, -32768, 30000, -30000, 100, -100, 0,
                110000000003421999, 9223372036852775807, 9007199254740991]
AGGREGATE_FUNCTIONS = ["SUM", "MIN", "MAX", "COUNT", "COUNTN", "AVG"]

class UpgradeSecondaryIndexAggrPushdown(UpgradeSecondaryIndexInt64):
    def setUp(self):
        self.queries = []
        super(UpgradeSecondaryIndexAggrPushdown, self).setUp()

    def tearDown(self):
        super(UpgradeSecondaryIndexAggrPushdown, self).tearDown()

    def _create_indexes(self):
        index_defn = ["CREATE INDEX index_int_num_name on %s(int_num, name)",
                      "CREATE INDEX index_name_int_num on %s(name, int_num)",
                      "CREATE INDEX index_long_num_name on %s(long_num, name)",
                      "CREATE INDEX index_name_long_num on %s(name, long_num)",
                      "CREATE INDEX index_int_arr_name on %s(ALL ARRAY t for t in int_arr END, name)",
                      "CREATE INDEX index_name_int_arr on %s(name, ALL ARRAY t for t in int_arr END)",
                      "CREATE INDEX index_long_arr_name on %s(ALL ARRAY t for t in long_arr END, name)",
                      "CREATE INDEX index_name_long_arr on %s(name, ALL ARRAY t for t in long_arr END)",
                      "CREATE INDEX index_int_num_int_arr_name on %s(int_num, ALL ARRAY t for t in int_arr END, name)",]
        for index in index_defn:
            for bucket in self.buckets:
                query = index % bucket.name
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

    def _query_index(self, phase):
        where_clauses = ["int_num = {0}".format(random.randint(-100, 100)),
                            "int_num > {0}".format(random.randint(-100, 100)),
                            "int_num < {0}".format(random.randint(-100, 100)),
                            "int_num > {0} AND int_num < {1}".format(random.randint(-100, 0), random.randint(0, 100)),
                            "int_num BETWEEN {0} AND {1}".format(random.randint(-100, 0), random.randint(0, 100))]

        where_clause_name = ["name = 'Kala'",
                             "name > 'C'",
                             "name < 'K'",
                             "name > 'C' AND name < 'K'",
                             "name BETWEEN 'C' AND 'K'",
                             "name is not null"]
        where_clause_int_arr = ["UNNEST d.`int_arr` AS t where t is not null",
                                  "UNNEST d.`int_arr` AS t where t = {0}".format(random.randint(-100, 100)),
                                  "UNNEST d.`int_arr` AS t where t > {0}".format(random.randint(-100, 100)),
                                  "UNNEST d.`int_arr` AS t where t < {0}".format(random.randint(-100, 100)),
                                  "UNNEST d.`int_arr` AS t where t > {0} and t < {1}".format(random.randint(-100, 0), random.randint(0, 100)),
                                  "UNNEST d.`int_arr` AS t where t between {0} and {1}".format(random.randint(-100, 0), random.randint(0, 100))]
        where_clauses.extend(where_clause_name)
        if phase not in list(self.query_results.keys()):
            self.query_results[phase] = {}
        phase_results = []
        for aggr_fun in AGGREGATE_FUNCTIONS:
            for where_clause in where_clauses:
                self.queries.append("SELECT " + aggr_fun + "(int_num) from default where " + where_clause + " group by name")
                self.queries.append("SELECT " + aggr_fun + "(int_num) from default where " + where_clause + " group by int_num")
                self.queries.append("SELECT " + aggr_fun + "(name) from default where " + where_clause + " group by name")
                self.queries.append("SELECT " + aggr_fun + "(name) from default where " + where_clause + " group by int_num")
            for where_clause in where_clause_int_arr:
                for group_by in ["d.name", "t", "d.int_num"]:
                    self.queries.append("SELECT " + aggr_fun + "(t) from default d " + where_clause + " group by " + group_by)
                    self.queries.append("SELECT " + aggr_fun + "(d.int_num) from default d " + where_clause + " group by " + group_by)
        for query in self.queries:
            res = {}
            res["query"] = query
            res["query_res"] = self.n1ql_helper.run_cbq_query(query, server=self.n1ql_node)["results"]
            query = "EXPLAIN " + query
            res["explain_res"] = self.n1ql_helper.run_cbq_query(query, server=self.n1ql_node)["results"]
            phase_results.append(res)
        self.query_results[phase] = phase_results

    def _verify_post_upgrade_results(self):
        wrong_results = []
        wrong_explain_results = []
        for i in range(len(self.query_results["pre_upgrade"])):
            if sorted(self.query_results["pre_upgrade"][i]["query_res"]) != sorted(self.query_results["post_upgrade"][i]["query_res"]):
                    wrong_results.append(self.query_results["post_upgrade"][i]["query"])
            if "index_group_aggs" not in self.query_results["post_upgrade"][i]["explain_res"]:
                wrong_explain_results.append(self.query_results["post_upgrade"][i]["query"])
        self.assertEqual(len(wrong_results), 0, str(wrong_results))
        if len(wrong_explain_results) != 0:
            log.warning("Explain failed for following queries: {0}".format(wrong_explain_results))

    def _query_for_long_num(self):
        for num in INT64_VALUES:
            where_clauses = ["long_num = {0}".format(num),
                            "long_num > {0}".format(num),
                            "long_num < {0}".format(num),
                            "long_num > {0} AND long_num < {1}".format(num-1, num+1),
                            "long_num BETWEEN {0} AND {1}".format(num-1, num+1)]
            where_clause_array = ["UNNEST d.`long_arr` AS t where t is not null",
                                  "UNNEST d.`long_arr` AS t where t = {0}".format(num),
                                  "UNNEST d.`long_arr` AS t where t > {0}".format(num),
                                  "UNNEST d.`long_arr` AS t where t < {0}".format(num),
                                  "UNNEST d.`long_arr` AS t where t > {0} and t < {1}".format(num-1, num+1),
                                  "UNNEST d.`long_arr` AS t where t between {0} and {1}".format(num - 1, num + 1)]

        where_clause_name = ["name = 'Kala'",
                             "name > 'C'",
                             "name < 'K'",
                             "name > 'C' AND name < 'K'",
                             "name BETWEEN 'C' AND 'K'",
                             "name is not null"]
        where_clauses.extend(where_clause_name)

        wrong_results = []
        for aggr_fun in AGGREGATE_FUNCTIONS:
            for where_clause in where_clauses:
                for group_by in ["name", "long_num"]:
                    query = "SELECT " + aggr_fun + "(long_num) from default where " + where_clause + " group by " + group_by
                    query_results = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)["results"]
                    primary_query = query.replace("default", "default use index(`#primary`)")
                    primary_results = self.n1ql_helper.run_cbq_query(query=primary_query, server=self.n1ql_node)["results"]
                    if len(query_results) == len(primary_results):
                        if sorted(query_results) != sorted(primary_results):
                            log.warning("Query results don't match for query {0}".format(query))
                            wrong_results.append(query)
                    else:
                        log.warning("Query count doesn't match for query {0}".format(query))
                        wrong_results.append(query)

        for aggr_fun in AGGREGATE_FUNCTIONS:
            for where_clause in where_clause_array:
                for group_by in ["d.name", "t"]:
                    query = "SELECT " + aggr_fun + "(t) from default d " + where_clause + " group by " + group_by
                    query_results = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)[0]
                    primary_query = query.replace("default d", "default d use index(`#primary`)")
                    primary_results = self.n1ql_helper.run_cbq_query(query=primary_query, server=self.n1ql_node)[0]
                    if len(query_results) == len(primary_results):
                        if sorted(query_results) != sorted(primary_results):
                            log.warning("Query results don't match for query {0}".format(query))
                            wrong_results.append(query)
                    else:
                        log.warning("Query count doesn't match for query {0}".format(query))
                        wrong_results.append(query)

        self.assertEqual(len(wrong_results), 0, wrong_results)

    def _verify_aggregate_query_results(self, result, query, bucket):
        def _gen_dict(res):
            result_set = []
            if res is not None and len(res) > 0:
                for val in res:
                    for key in list(val.keys()):
                        result_set.append(val[key])
            return result_set

        self.restServer = self.get_nodes_from_services_map(service_type="n1ql")
        self.rest = RestConnection(self.restServer)
        self.rest.set_query_index_api_mode(1)
        primary_query = query % (bucket, "#primary")
        primary_result = self.run_cbq_query(primary_query)
        self.rest.set_query_index_api_mode(3)
        self.log.info(" Analyzing Actual Result")

        actual_result = _gen_dict(sorted(primary_result["results"]))
        self.log.info(" Analyzing Expected Result")
        expected_result = _gen_dict(sorted(result["results"]))
        if len(actual_result) != len(expected_result):
            return False
        if actual_result != expected_result:
            return False
        return True
