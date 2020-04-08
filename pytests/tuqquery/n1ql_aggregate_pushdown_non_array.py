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
        self.aggr_distinct = self.input.param("aggr_distinct", False)
        self.order_by = self.input.param("order_by", False)
        self.having = self.input.param("having", False)
        self.offset_limit = self.input.param("offset_limit", False)
        self.big_int = self.input.param("big_int", False)

    def tearDown(self):
        super(AggregatePushdownClass, self).tearDown()

    def test_aggregate_group_by_leading(self):
        self.fail_if_no_buckets()
        if self.big_int:
            self.run_cbq_query(query="UPDATE default set age = 2147483750 where age = 60 returning age")
            self.run_cbq_query(query="UPDATE default set age = 2147483900 where age = 63 returning age")
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            select_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        else:
                            select_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        if self.order_by:
                            if self.offset_limit:
                                select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                            else:
                                select_clause += " ORDER BY {3}"
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"], index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                        elif self.having:
                            if self.offset_limit:
                                select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                            else:
                                select_clause += " HAVING {3} is not null"
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"], index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                        else:
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_aggregate_group_by_first_non_leading(self):
        self.fail_if_no_buckets()
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            select_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        else:
                            select_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        if self.order_by:
                            if self.offset_limit:
                                select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                            else:
                                select_clause += " ORDER BY {3}"
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"], index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                        elif self.having:
                            if self.offset_limit:
                                select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                            else:
                                select_clause += " HAVING {3} is not null"
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"], index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                        else:
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_aggregate_group_by_second_non_leading(self):
        self.fail_if_no_buckets()
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            select_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        else:
                            select_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        if self.order_by:
                            if self.offset_limit:
                                select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                            else:
                                select_clause += " ORDER BY {3}"
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"], index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                        elif self.having:
                            if self.offset_limit:
                                select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                            else:
                                select_clause += " HAVING {3} is not null"
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"], index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                        else:
                            query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                      index_fields[0]["name"])
                                                 for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_aggregate_without_group_by(self):
        self.fail_if_no_buckets()
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            select_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1}"
                        else:
                            select_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1}"
                        query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"])
                                             for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_aggregate_on_expression(self):
        self.fail_if_no_buckets()
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            select_clause = "SELECT " + aggr_func + "(DISTINCT {0}*5) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        else:
                            select_clause = "SELECT " + aggr_func + "({0}*2) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"], tup[0]["name"])
                                             for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_aggregate_group_by_expression(self):
        self.fail_if_no_buckets()
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            select_clause = "SELECT " + aggr_func + "(DISTINCT {0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}*2"
                        else:
                            select_clause = "SELECT " + aggr_func + "({0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}*2"
                        query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"], tup[0]["name"])
                                             for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_aggregate_on_constant(self):
        self.fail_if_no_buckets()
        if self.aggr_distinct:
            aggregate_functions = DISTINCT_AGGREGATE_FUNCTIONS
        else:
            aggregate_functions = AGGREGATE_FUNCTIONS
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    for aggr_func in aggregate_functions:
                        if self.aggr_distinct:
                            if self.big_int:
                                select_clause = "SELECT " + aggr_func + "(DISTINCT 2147483650) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                            else:
                                select_clause = "SELECT " + aggr_func + "(DISTINCT 55) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        else:
                            if self.big_int:
                                select_clause = "SELECT " + aggr_func + "(2147483750) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                            else:
                                select_clause = "SELECT " + aggr_func + "(67) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                        query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"], tup[0]["name"])
                                             for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))
        self.log.info("Dont worry this test actually does something")


    def test_aggregate_unsupported_methods(self):
        self.fail_if_no_buckets()
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]

                    if self.aggr_distinct:
                        select_clause = "SELECT  ARRAY_AGG(DISTINCT {0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    else:
                        select_clause = "SELECT ARRAY_AGG({0}) from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                              index_fields[0]["name"])
                                         for tup in itertools.permutations(index_fields)]
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_group_by_no_aggregate(self):
        self.fail_if_no_buckets()
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    if self.aggr_distinct:
                        select_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    else:
                        select_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    if self.order_by:
                        if self.offset_limit:
                            select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                        else:
                            select_clause += " ORDER BY {3}"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    elif self.having:
                        if self.offset_limit:
                            select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                        else:
                            select_clause += " HAVING {3} is not null"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    else:
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_group_by_first_non_leading(self):
        self.fail_if_no_buckets()
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    if self.aggr_distinct:
                        select_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    else:
                        select_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    if self.order_by:
                        if self.offset_limit:
                            select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                        else:
                            select_clause += " ORDER BY {3}"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    elif self.having:
                        if self.offset_limit:
                            select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                        else:
                            select_clause += " HAVING {3} is not null"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    else:
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_group_by_second_non_leading(self):
        self.fail_if_no_buckets()
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    if self.aggr_distinct:
                        select_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    else:
                        select_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    if self.order_by:
                        if self.offset_limit:
                            select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                        else:
                            select_clause += " ORDER BY {3}"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    elif self.having:
                        if self.offset_limit:
                            select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                        else:
                            select_clause += " HAVING {3} is not null"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                                 index_field["name"],
                                                                                 index_field["name"])
                                    query_definitions.append(query_definition)
                    else:
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_group_by_expression(self):
        self.fail_if_no_buckets()
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    if self.aggr_distinct:
                        select_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}*2"
                    else:
                        select_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}*2"
                    for tup in itertools.permutations(index_fields):
                        for index_field in index_fields:
                            if tup[0]['name'] == index_field['name']:
                                query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                         index_field["name"], index_field["name"])
                                query_definitions.append(query_definition)
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_mixed_data(self):
        self.fail_if_no_buckets()
        result = self.run_cbq_query(query = 'UPDATE default set age = "string" where age = 63 returning age')
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    if self.aggr_distinct:
                        select_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    else:
                        select_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    if self.order_by:
                        select_clause += " ORDER BY {3}"
                        if self.offset_limit:
                            select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    elif self.having:
                        select_clause += " HAVING {3} is not null"
                        if self.offset_limit:
                            select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    else:
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], tup[1]["where_clause"],
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))

    def test_where_no_results(self):
        self.fail_if_no_buckets()
        failed_queries_in_explain = []
        failed_queries_in_result = []
        for index_name_def in self._create_array_index_definitions():
            try:
                for create_def in index_name_def["create_definitions"]:
                    result = self.run_cbq_query(create_def)
                    query_definitions = []
                    index_name = index_name_def["index_name"]
                    index_fields = index_name_def["fields"]
                    if self.aggr_distinct:
                        select_clause = "SELECT DISTINCT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    else:
                        select_clause = "SELECT {0} from %s USE INDEX (`%s`) where {1} GROUP BY {2}"
                    if self.order_by:
                        select_clause += " ORDER BY {3}"
                        if self.offset_limit:
                            select_clause += " ORDER BY {3} LIMIT 10 OFFSET 200"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], "name = 'ajay'",
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    elif self.having:
                        select_clause += " HAVING {3} is not null"
                        if self.offset_limit:
                            select_clause += " HAVING {3} is not null LIMIT 10 OFFSET 200"
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], "name = 'ajay'",
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    else:
                        for tup in itertools.permutations(index_fields):
                            for index_field in index_fields:
                                if tup[0]['name'] == index_field['name']:
                                    query_definition = select_clause.format(tup[0]["name"], "name = 'ajay'",
                                                                             index_field["name"], index_field["name"])
                                    query_definitions.append(query_definition)
                    for bucket in self.buckets:
                        for query_definition in query_definitions:
                            query = query_definition % (bucket.name, index_name)
                            result = self.run_cbq_query(query)
                            explain_verification = self._verify_aggregate_explain_results(query,
                                                                                          index_name,
                                                                                          index_fields)
                            if not explain_verification:
                                failed_queries_in_explain.append(query)
                            query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                                      bucket.name)
                            if not query_verification:
                                failed_queries_in_result.append(query)
            finally:
                for drop_def in index_name_def["drop_definitions"]:
                    result = self.run_cbq_query(drop_def)
        self.assertEqual(len(failed_queries_in_result), 0,
                         "Following Queries failed in result: {0}".format(failed_queries_in_result))
        if failed_queries_in_explain:
            log.info("Following queries failed in explain: {0}".format(failed_queries_in_explain))
    def _create_array_index_definitions(self):
        index_fields = [{"name": "name", "where_clause": "name = 'Kala'"},
                        {"name": "age", "where_clause": "age < 85"},
                        {"name": "debt", "where_clause": "debt > -500000"}]
        for first_field in index_fields:
            for second_field in index_fields:
                if first_field == second_field:
                    continue
                for third_field in index_fields:
                    if second_field == third_field or first_field == third_field:
                        continue
                    index_names_defn = {}
                    index_name = "{0}_{1}_{2}".format(first_field["name"], second_field["name"], third_field["name"])
                    index_names_defn["index_name"] = index_name
                    index_names_defn["fields"] = [first_field, second_field, third_field]
                    create_index_clause = "CREATE INDEX {0} on %s({1}, {2}, {3})".format(
                        index_name, first_field["name"], second_field["name"], third_field["name"])
                    drop_index_clause = "DROP INDEX %s.{0}".format(index_name)
                    index_names_defn["create_definitions"] = [(create_index_clause % bucket.name) for bucket in self.buckets]
                    index_names_defn["drop_definitions"] = [(drop_index_clause % bucket.name) for bucket in self.buckets]
                    yield index_names_defn

    def _verify_aggregate_explain_results(self, query, index_name, index_fields, allow_pushdown=True):
        explain_query = "EXPLAIN " + query
        explain_result = self.run_cbq_query(explain_query)
        where_field = ""
        for field in index_fields:
            if field["name"] in query.split("where")[1].split("GROUP")[0]:
                where_field = field["name"]
        if "GROUP BY" in query:
            if not index_name.startswith(where_field):
                allow_pushdown = False
            if self.aggr_distinct:
                for i in range(len(index_fields)):
                    if index_fields[i]["name"] in query.split("GROUP BY")[1]:
                        if i > 1:
                            allow_pushdown = False
                            break
        else:
            if not index_name.startswith(where_field):
                if where_field in query.split("SELECT")[1].split("from")[0]:
                    allow_pushdown = False
                else:
                    if index_name.find(where_field) > index_name.find(index_fields[0]["name"]):
                        allow_pushdown = False
        if allow_pushdown:
            check = "index_group_aggs" in str(explain_result)
        else:
            check = "index_group_aggs" not in str(explain_result)
        return check