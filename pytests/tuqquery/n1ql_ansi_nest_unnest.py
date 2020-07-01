import logging
import random

from couchbase_helper.tuq_generators import JsonGenerator
from couchbase_helper.query_definitions import QueryDefinition, SQLDefinitionGenerator
from membase.api.rest_client import RestConnection
from .tuq import QueryTests

log = logging.getLogger(__name__)
ARRAY_FIELDS = ["transactions", "travel_history", "credit_cards", "password"]


class QueryANSINestUnnestTests(QueryTests):
    def setUp(self):
        super(QueryANSINestUnnestTests, self).setUp()
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.default_query_bucket = self.query_buckets[0]
        self.standard_query_bucket = self.query_buckets[1]

    def suite_setUp(self):
        super(QueryANSINestUnnestTests, self).suite_setUp()
        self._create_indexes()
        self._load_buckets()

    def tearDown(self):
        super(QueryANSINestUnnestTests, self).tearDown()

    def test_basic_join_with_unnest(self):
        for arr_name in ARRAY_FIELDS:
            query = "select t from {0} d UNNEST d.`{2}` as t INNER JOIN {1}" \
                    " s ON (d.age == s.age)".format(self.default_query_bucket, self.standard_query_bucket, arr_name)
            self.run_cbq_query(query=query)

    def test_basic_join_non_covering_with_unnest(self):
        for arr_name in ARRAY_FIELDS:
            query = "select t from {0} d UNNEST d.`{2}` as t INNER JOIN {1} s on (s.age == d.age OR" \
                    " s.name == d.name)".format(self.default_query_bucket, self.standard_query_bucket, arr_name)
            self.run_cbq_query(query=query)

    def test_ROJ_with_unnest(self):
        for arr_name in ARRAY_FIELDS:
            query = "select t from {0} d RIght outer JOIN {1} s ON (d.age == s.age) UNNEST" \
                    " d.`{2}` as t".format(self.default_query_bucket, self.standard_query_bucket, arr_name)
            self.run_cbq_query(query=query)

    def test_ROJ_chain_with_unnest(self):
        for arr_name in ARRAY_FIELDS:
            query = "select t from {0} d RIGHT OUTER JOIN {1} s  ON (s.age > d.age) INNER JOIN " \
                    "{0} d1 ON (s.age > d1.age) UNNEST d.`{2}` as t limit 1000".format(self.default_query_bucket,
                                                                                       self.standard_query_bucket,
                                                                                       arr_name)
            self.run_cbq_query(query=query.format(arr_name))

    def test_LOJ_with_unnest(self):
        query = "select d.business_name from {0} d UNNEST d.travel_history as t LEFT OUTER JOIN {1}" \
                " s ON (t == s.home)".format(self.default_query_bucket, self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_left_hand_array_unnest(self):
        query = "select * from {1} s UNNEST s.travel_history as noarray  INNER JOIN {0} d ON" \
                " (noarray.country > d.home)".format(self.default_query_bucket, self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_full_array_with_unnest(self):
        query = "CREATE INDEX i1 on {0}( `travel_history`)".format(self.default_query_bucket)
        self.run_cbq_query(query)
        query = "CREATE INDEX i2 on {0}( `travel_history`)".format(self.standard_query_bucket)
        self.run_cbq_query(query)
        query = "select * from {1} s unnest s.travel_history as t INNER JOIN {0} d ON " \
                "(t < d.travel_history)".format(self.default_query_bucket, self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_array_one_to_one_right_unnest(self):
        query = "select * from {0} d unnest d.`passwords` INNER JOIN {1} s ON (d.age < s.age AND ANY var IN " \
                "s.`travel_history` SATISFIES var = 'India' END) WHERE d.weight > 55".format(self.default_query_bucket,
                                                                                             self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_array_one_to_one_left_unnest(self):
        query = "select * from {1} s unnest s.`passwords` INNER JOIN {0} d ON (d.age < s.age AND ANY var IN " \
                "s.`travel_history` SATISFIES var = 'India' END) WHERE d.weight > 55".format(self.default_query_bucket,
                                                                                             self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_array_indexing_left_hand_array_unnest_IN(self):
        query = "select * from {0} d unnest d.passwords INNER JOIN {1} s ON (s.home IN (ALL ARRAY t for t in " \
                "d.travel_history END))".format(self.default_query_bucket, self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_ansi_nest_basic_left(self):
        query = "SELECT * FROM {0} d NEST {0} s ON (s.home in d.travel_history)".format(self.default_query_bucket)
        self.run_cbq_query(query)

    def test_ansi_nest_basic_left_UNNEST(self):
        query = "SELECT * FROM {0} d unnest d.travel_history as t NEST {0} s ON " \
                "(s.home = t)".format(self.default_query_bucket)
        self.run_cbq_query(query)

    def test_ansi_nest_basic_right_IN(self):
        query = "SELECT * FROM {0} s NEST {0} d ON (s.home in d.travel_history)".format(self.default_query_bucket)
        self.run_cbq_query(query)

    def test_ansi_nest_basic_right(self):
        query = "SELECT * FROM {0} d NEST {0} s ON (any t in s.travel_history satisfies " \
                "t = d.home end)".format(self.default_query_bucket)
        self.run_cbq_query(query)

    def test_ansi_nest_with_ansi_join(self):
        query = "SELECT * FROM {0} d NEST {0} d1 ON (any t in d1.travel_history satisfies t = d.home end) JOIN " \
                "{1} s ON (s.home == d.home)".format(self.default_query_bucket, self.standard_query_bucket)
        self.run_cbq_query(query)

    def test_ansi_nest_ansi_join_unnest(self):
        query = "SELECT * FROM {0} d unnest d.travel_history as t NEST {0} d1 ON (t = d1.home) INNER JOIN {1} s " \
                "ON (t == s.home)".format(self.default_query_bucket, self.standard_query_bucket)
        self.run_cbq_query(query)

    def _load_buckets(self):
        """
        1. Remove existing buckets
        2. Create 2 buckets and load documents
        3. Create full_doc_list for both buckets
        :return:
        """
        rest = RestConnection(self.master)
        json_generator = JsonGenerator()
        self.standard_gens_load = json_generator.generate_doc_for_aggregate_pushdown(docs_per_day=self.docs_per_day,
                                                                                     start=0)
        self.standard_full_docs_list = self.generate_full_docs_list(self.standard_gens_load)
        self.default_gens_load = json_generator.generate_doc_for_aggregate_pushdown(docs_per_day=self.docs_per_day,
                                                                                    start=0)
        self.default_full_docs_list = self.generate_full_docs_list(self.default_gens_load)
        for bucket in self.buckets:
            rest.flush_bucket(bucket.name)
            count = 0
            while rest.get_bucket_status(bucket.name) != "healthy" and count < 10:
                log.info(
                    "Bucket {0} Status is {1}. Sleeping...".format(bucket.name, rest.get_bucket_status(bucket.name)))
                count += 1
                self.sleep(15)
            if bucket.name.startswith("standard"):
                self.load(self.standard_gens_load, flag=self.item_flag, buckets=[bucket], verify_data=False)
            if bucket.name.startswith("default"):
                self.load(self.default_gens_load, flag=self.item_flag, buckets=[bucket], verify_data=False)

    def _create_indexes(self):
        groups = self.input.param("groups", "all").split(":")
        query_gen = QueryDefs()
        self.query_definitions = query_gen.generate_query_definition_for_aggr_data()
        self.query_definitions = query_gen.filter_by_group(groups, self.query_definitions)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for query_defn in self.query_definitions:
                create_query = query_defn.generate_index_create_query(namespace=query_bucket)
                self.run_cbq_query(create_query)


class QueryDefs(SQLDefinitionGenerator):
    def generate_query_definition_for_aggr_data(self):
        definitions_list = []
        index_name_prefix = "back_detail_" + str(random.randint(100000, 999999))
        # Primary Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_primary_index", index_fields=[],
                            query_template="SELECT * FROM %s", groups=["full_data_set", "primary"],
                            index_where_clause=""))
        # simple index on string
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_name", index_fields=["name"],
                            query_template="SELECT name FROM %s where name = 'Ciara'", groups=["all", "simple_index"],
                            index_where_clause=" name = 'Ciara' "))
        # simple index on int
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_business_name", index_fields=["business_name"],
                            query_template="SELECT business_name FROM %s where business_name < 'V' and business_name > 'C'",
                            groups=["all", "simple_index"],
                            index_where_clause=" business_name < 'V' and business_name > 'C' "))
        # simple index on int
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_home", index_fields=["home"],
                            query_template="SELECT home FROM %s where home = 'India'", groups=["all", "simple_index"],
                            index_where_clause=" home = 'India' "))
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_age", index_fields=["age"],
                            query_template="SELECT age FROM %s where age > 50 and age < 70",
                            groups=["all", "simple_index"], index_where_clause=" age > 50 and age < 70 "))
        # simple index on float
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_weight", index_fields=["weight"],
                            query_template="SELECT weight FROM %s where weight > 65.5", groups=["all", "simple_index"],
                            index_where_clause=" weight > 65.5 "))
        index_name_prefix = "back_detail_" + str(random.randint(100000, 999999))
        # simple index on negative int
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_debt", index_fields=["debt"],
                            query_template="SELECT debt FROM %s where debt < -300000", groups=["all", "simple_index"],
                            index_where_clause=" debt < -300000 "))
        # simple array index on int+str
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_password",
                            index_fields=["ALL ARRAY t FOR t in `password` END"],
                            query_template="SELECT password FROM %s where ANY t IN `password` SATISFIES t > 3000 END",
                            groups=["all", "array", "simple_index"],
                            index_where_clause=" ANY t IN `password` SATISFIES t > 3000 END "))
        # simple index on int
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_transactions",
                            index_fields=["ALL ARRAY t FOR t in `transactions` END"],
                            query_template="SELECT transactions FROM %s where ANY t IN `transactions` SATISFIES t > 3000 END",
                            groups=["all", "array", "simple_index"],
                            index_where_clause=" ANY t IN `transactions` SATISFIES t > 3000 END "))
        # simple index on object
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_address", index_fields=["address"],
                            query_template="SELECT address FROM %s where address.country = 'India'",
                            groups=["all", "simple_index"], index_where_clause=" address.country = 'India' "))
        # simple array index on str
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_travel_history",
                            index_fields=["ALL ARRAY t FOR t in `travel_history` END"],
                            query_template="SELECT travel_history FROM %s where ANY t IN `travel_history` SATISFIES t > 'B' END",
                            groups=["all", "array", "simple_index"],
                            index_where_clause=" ANY t IN `travel_history` SATISFIES t > 'B' END "))
        # simple index on int
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_credit_cards",
                            index_fields=["ALL ARRAY t FOR t in `credit_cards` END"],
                            query_template="SELECT credit_cards FROM %s where ANY t IN `credit_cards` SATISFIES t > 3000 END",
                            groups=["all", "array", "simple_index"],
                            index_where_clause=" ANY t IN `credit_cards` SATISFIES t > 3000 END "))
        return definitions_list
