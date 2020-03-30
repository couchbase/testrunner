import itertools
import logging

from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection
from .tuq import QueryTests


log = logging.getLogger(__name__)

AGGREGATE_FUNCTIONS = ["SUM", "MIN", "MAX", "COUNT", "COUNTN", "AVG"]
DISTINCT_AGGREGATE_FUNCTIONS = ["SUM", "COUNT", "AVG"]

class AggregatePushdownClass(QueryTests):
    def setUp(self):
        super(AggregatePushdownClass, self).setUp()
        self.n1ql_helper = N1QLHelper(master=self.master)
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.array_type = self.input.param("array_type", "all")
        self.aggr_distinct = self.input.param("aggr_distinct", False)

    def tearDown(self):
        super(AggregatePushdownClass, self).tearDown()

    def test_array_aggregate_non_array_group_non_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        select_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0}) from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
                    else:
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_non_array_group_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0}) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{1}` AS t where d.{2} GROUP BY t"
                        select_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0}) from %s d USE INDEX (`%s`) {1}  GROUP BY t"
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(d.{0}) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{1}` AS t where d.{2} GROUP BY t"
                        select_array_where_clause = "SELECT " + aggr_func + "(d.{0}) from %s d USE INDEX (`%s`) {1}  GROUP BY t"

                    for non_array_first_field in non_array_fields:
                        for non_array_second_field in non_array_fields:
                            if non_array_second_field == non_array_first_field:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"])]
                            else:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_second_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_second_field["where_clause"])]
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_non_array_multiple_group(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0}) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{1}` AS t where d.{2} GROUP BY "
                        select_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0}) from %s d USE INDEX (`%s`) {1} GROUP BY "
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(d.{0}) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{1}` AS t where d.{2} GROUP BY "
                        select_array_where_clause = "SELECT " + aggr_func + "(d.{0}) from %s d USE INDEX (`%s`) {1}  GROUP BY "

                    for non_array_first_field in non_array_fields:
                        for non_array_second_field in non_array_fields:
                            if non_array_second_field == non_array_first_field:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"])]
                            else:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_second_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_second_field["where_clause"])]
                            for query_template in query_definitions:
                                query_template += [", ".join(tup[0]["name"], tup[1]["name"], tup[2]["name"])
                                                   for tup in itertools.permutations(index_name_def["fields"])]

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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_non_array_without_group_by(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1}"
                        select_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0}) from %s d USE INDEX (`%s`) {1}"
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1}"
                        select_array_where_clause = "SELECT " + aggr_func + "(d.{0}) from %s d USE INDEX (`%s`) {1}"
                    for non_array_first_field in non_array_fields:
                        for non_array_second_field in non_array_fields:
                            if non_array_second_field == non_array_first_field:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"])]
                            else:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          non_array_second_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          non_array_second_field["where_clause"])]
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_array_group_non_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT t) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{0}` AS t where d.{1} GROUP BY d.{2}"
                        select_array_where_clause = "SELECT " + aggr_func + "(DISTINCT t) from %s d USE INDEX (`%s`) {0}  GROUP BY d.{1}"
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{0}` AS t where d.{1} GROUP BY d.{2}"
                        select_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`) {0}  GROUP BY d.{1}"
                    for non_array_first_field in non_array_fields:
                        for non_array_second_field in non_array_fields:
                            if non_array_second_field == non_array_first_field:
                                query_definitions = [select_non_array_where_clause.format(array_field["name"],
                                                                          non_array_first_field["where_clause"],
                                                                          non_array_first_field["name"]),
                                                     select_array_where_clause.format(array_field["where_clause"],
                                                                          non_array_first_field["name"])]
                            else:
                                query_definitions = [select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_first_field["where_clause"],
                                                                                          non_array_first_field["name"]),
                                                     select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_first_field["where_clause"],
                                                                                          non_array_second_field["name"]),
                                                     select_array_where_clause.format(array_field["where_clause"],
                                                                                      non_array_first_field["name"]),
                                                     select_array_where_clause.format(array_field["where_clause"],
                                                                                      non_array_second_field["name"]),
                                                     select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_second_field["where_clause"],
                                                                                          non_array_first_field["name"]),
                                                     select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_second_field["where_clause"],
                                                                                          non_array_second_field["name"])]
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_array_group_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT t) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{1}` AS t where d.{2} GROUP BY t"
                        select_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`) {1}  GROUP BY t"
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{1}` AS t where d.{2} GROUP BY t"
                        select_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`) {1}  GROUP BY t"
                    for non_array_first_field in non_array_fields:
                        for non_array_second_field in non_array_fields:
                            if non_array_second_field == non_array_first_field:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"])]
                            else:
                                query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_second_field["where_clause"]),
                                                     select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                          array_field["name"],
                                                                                          non_array_second_field["where_clause"])]
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_array_without_group_by(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT t) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{0}` AS t where d.{1}"
                        select_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`) {0}"
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT t) from %s d USE INDEX (`%s`)" \
                                                                            "UNNEST d.`{0}` AS t where d.{1}"
                        select_array_where_clause = "SELECT " + aggr_func + "(t) from %s d USE INDEX (`%s`) {0}"
                    for non_array_first_field in non_array_fields:
                        for non_array_second_field in non_array_fields:
                            if non_array_second_field == non_array_first_field:
                                query_definitions = [select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(array_field["where_clause"])]
                            else:
                                query_definitions = [select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_first_field["where_clause"]),
                                                     select_array_where_clause.format(array_field["where_clause"]),
                                                     select_non_array_where_clause.format(array_field["name"],
                                                                                          non_array_second_field["where_clause"])]
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_aggregate_expressions(self):
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
                else:
                    aggregate_functions = AGGREGATE_FUNCTIONS
                for aggr_func in aggregate_functions:
                    if self.aggr_distinct:
                        select_non_array_where_clause = "SELECT " + aggr_func + "(DISTINCT {0} * 2) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        select_array_where_clause = "SELECT " + aggr_func + "(DISTINCT d.{0} * 2) from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
                    else:
                        select_non_array_where_clause = "SELECT " + aggr_func + "({0} * 6) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        select_array_where_clause = "SELECT " + aggr_func + "(d.{0} * 7) from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_non_array_group_non_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    select_non_array_where_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    select_array_where_clause = "SELECT DISTINCT d.{0} from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
                else:
                    select_non_array_where_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    select_array_where_clause = "SELECT d.{0} from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
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
                                                 select_non_array_where_clause.format(non_array_second_field["name"],
                                                      non_array_first_field["where_clause"],
                                                      non_array_second_field["name"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                      array_field["where_clause"],
                                                      non_array_first_field["name"]),
                                                 select_array_where_clause.format(non_array_second_field["name"],
                                                      array_field["where_clause"],
                                                      non_array_second_field["name"]),
                                                 select_non_array_where_clause.format(non_array_first_field["name"],
                                                                      non_array_second_field["where_clause"],
                                                                      non_array_first_field["name"]),
                                                 select_non_array_where_clause.format(non_array_second_field["name"],
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_group_array(self):
        create_index_count = 0
        query_count = 0
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    create_index_count += 1
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    select_non_array_where_clause = "SELECT DISTINCT t from %s d USE INDEX (`%s`)" \
                                                                        "UNNEST d.`{1}` AS t where d.{2} GROUP BY t"
                    select_array_where_clause = "SELECT t from %s d USE INDEX (`%s`) {1}  GROUP BY t"
                else:
                    select_non_array_where_clause = "SELECT t from %s d USE INDEX (`%s`)" \
                                                                        "UNNEST d.`{1}` AS t where d.{2} GROUP BY t"
                    select_array_where_clause = "SELECT t from %s d USE INDEX (`%s`) {1}  GROUP BY t"
                for non_array_first_field in non_array_fields:
                    for non_array_second_field in non_array_fields:
                        if non_array_second_field == non_array_first_field:
                            query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["name"],
                                                                                      non_array_first_field["where_clause"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                                                  array_field["where_clause"])]
                        else:
                            query_definitions = [select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["name"],
                                                                                      non_array_first_field["where_clause"]),
                                                 select_non_array_where_clause.format(non_array_first_field["name"],
                                                                                      array_field["name"],
                                                                                      non_array_first_field["where_clause"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                                                  array_field["where_clause"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                                                  array_field["where_clause"]),
                                                 select_non_array_where_clause.format(non_array_second_field["name"],
                                                                                      array_field["name"],
                                                                                      non_array_second_field["where_clause"]),
                                                 select_non_array_where_clause.format(non_array_second_field["name"],
                                                                                      array_field["name"],
                                                                                      non_array_second_field["where_clause"])]
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def test_array_groupby_expressions(self):
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                array_field = index_name_def["fields"][0]
                non_array_fields = [index_name_def["fields"][1], index_name_def["fields"][2]]
                if self.aggr_distinct:
                    select_non_array_where_clause = "SELECT (DISTINCT {0} * 2) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    select_array_where_clause = "SELECT (DISTINCT d.{0} * 2) from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
                else:
                    select_non_array_where_clause = "SELECT ({0} * 6) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    select_array_where_clause = "SELECT (d.{0} * 7) from %s d USE INDEX (`%s`) {1}  GROUP BY d.{2}"
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
                                                 select_non_array_where_clause.format(non_array_second_field["name"],
                                                      non_array_first_field["where_clause"],
                                                      non_array_second_field["name"]),
                                                 select_array_where_clause.format(non_array_first_field["name"],
                                                      array_field["where_clause"],
                                                      non_array_first_field["name"]),
                                                 select_array_where_clause.format(non_array_second_field["name"],
                                                      array_field["where_clause"],
                                                      non_array_second_field["name"]),
                                                 select_non_array_where_clause.format(non_array_first_field["name"],
                                                                      non_array_second_field["where_clause"],
                                                                      non_array_first_field["name"]),
                                                 select_non_array_where_clause.format(non_array_second_field["name"],
                                                                      non_array_second_field["where_clause"],
                                                                      non_array_second_field["name"])
                                                 ]
                        for bucket in self.buckets:
                            for query_template in query_definitions:
                                for index_name in index_name_def["index_names"]:
                                    query = query_template % (bucket.name, index_name)
                                    result = self.run_cbq_query(query)
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
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Aggregate Pushdown Query Results fails: {0}".format(failed_queries_in_result))
        log.info("Failed Aggregate Queries in Explain: {0}".format(failed_queries_in_explain))

    def _create_array_index_definitions(self):
        array_fields = [{"name": "passwords", "where_clause": "UNNEST d.`passwords` AS t where t is not null"},
                        {"name": "transactions", "where_clause": "UNNEST d.`transactions` AS t where t > 99999"},
                        {"name": "travel_history", "where_clause": "UNNEST d.`travel_history` AS t where t = 'India'"},
                        {"name": "credit_cards", "where_clause": "UNNEST d.`credit_cards` AS t where t < 10000"}]
        non_array_fields = [{"name": "name", "where_clause": "name = 'Kala'"},
                        {"name": "age", "where_clause": "age < 55"},
                        {"name": "debt", "where_clause": "debt > -500000"},
                        {"name": "address", "where_clause": "address.country = 'India'"}]
        create_index_definitions = []
        for arr_field in array_fields:
            if self.array_type == "all":
                arr_field_defn = "ALL ARRAY t FOR t in `{0}` END".format(arr_field["name"])
            else:
                arr_field_defn = "DISTINCT ARRAY t FOR t in `{0}` END".format(arr_field["name"])
            for first_field in non_array_fields[:-1]:
                for second_field in non_array_fields[non_array_fields.index(first_field)+1:]:
                    if first_field["name"] == second_field["name"]:
                        continue
                    index_names_defn = {}

                    index_names = ["{0}_{1}_{2}".format(first_field["name"], second_field["name"], arr_field["name"]),
                                   "{0}_{1}_{2}".format(first_field["name"], arr_field["name"], second_field["name"]),
                                   "{0}_{1}_{2}".format(arr_field["name"], first_field["name"], second_field["name"])]
                    if self.array_type == "distinct":
                        index_names = ["{0}_distinct".format(inx) for inx in index_names]
                    index_names_defn["index_names"] = index_names
                    index_names_defn["fields"] = [arr_field, first_field, second_field]
                    create_index_clause = "CREATE INDEX {0} on %s({1}, {2}, {3})"
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
        explain_query = "EXPLAIN " + query
        explain_query = explain_query % (bucket, index_name)
        explain_result = self.run_cbq_query(explain_query)
        if "UNNEST" in query:
            if not index_name.startswith(index_fields[0]["name"]):
                allow_pushdown = False
            else:
                if "where t" not in query:
                    allow_pushdown = False
        else:
            where_field = ""
            for field in index_fields:
                if field["name"] in query.split("where")[1].split("GROUP")[0]:
                    where_field = field["name"]
            if "GROUP BY" in query:
                if not index_name.startswith(where_field):
                    allow_pushdown = False
            else:
                if not index_name.startswith(where_field):
                    if where_field in query.split("SELECT")[1].split("from")[0]:
                        allow_pushdown = False
                    else:
                        if index_name.find(where_field) > index_name.find(index_fields[0]["name"]):
                            allow_pushdown = False
            if "distinct" not in index_name:
                aggr_func = query.split("SELECT ")[1].split("(")[0]
                if aggr_func in ["SUM", "COUNT", "COUNTN", "AVG"]:
                    allow_pushdown = False

        if allow_pushdown:
            check = "index_group_aggs" in str(explain_result)
        else:
            check = "index_group_aggs" not in str(explain_result)
        return check
