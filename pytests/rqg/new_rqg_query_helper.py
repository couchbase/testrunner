import random
import copy
import string
import pprint
from base_query_helper import BaseRQGQueryHelper

'''
N1QL PARSE ORDER
1.  with
2.  from
3.  let
4.  where
5.  group
6.  letting
7.  having
8.  select
9.  order by
10. offset
11. limit

RQG RESERVED KEYWORD LIST

*CLAUSE KEYWORD*
WITH_START
WITH_END
FROM_START
FROM_END
LET_START
LET_END
WHERE_START
WHERE_END
GROUPBY_START
GROUPBY_END
LETTING_START
LETTING_END
HAVING_START
HAVING_END
SELECT_START
SELECT_END
ORDERBY_START
ORDERBY_END
OFFSET_START
OFFSET_END
LIMIT_START
LIMIT_END

*INTERNAL KEYWORDS*
CTE_START
CTE_END
WITH_CLAUSE_SUBQUERY
WITH_CLAUSE_CONSTANT
'''


class RQGQueryHelperNew(BaseRQGQueryHelper):

    ''' Dispatcher function. Uses test_name parameter to identify the way how templates
        will be transformed into SQL and N1QL queries.
        from let where group by letting haVING SELECT ORDER BY'''
    def _get_conversion_func(self, test_name):
        if test_name == 'group_by_alias':
            return self._convert_sql_template_for_group_by_aliases
        elif test_name == 'skip_range_key_scan':
            return self._convert_sql_template_for_skip_range_scan
        elif test_name == 'common_table_expression':
            return self._convert_sql_template_for_common_table_expression
        else:
            print("Unknown test name")
            exit(1)

    def _convert_sql_template_for_common_table_expression(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        template_map = self._extract_clauses(query_template)

        query_template_map = self._convert_clauses_n1ql(conversion_map, template_map)
        query_template_map['SQL'] = query_template_map['N1QL'].replace(" RAW ", " ")

        query_map = {"n1ql": query_template_map['N1QL'],  "sql": query_template_map['N1QL'],
                     "bucket": str(",".join(table_map.keys())),
                     "expected_result": None, "indexes": {}}

        return query_map

    def _convert_clauses_n1ql(self, conversion_map, template_map):
        template_map = self._convert_with_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map)
        template_map["N1QL"] = self._combine_converted_clauses(template_map)
        return template_map

    def _combine_converted_clauses(self, template_map):
        clause_order = ["WITH_CLAUSE", "SELECT_CLAUSE", "FROM_CLAUSE", "LET_CLAUSE", "WHERE_CLAUSE", "GROUPBY_CLAUSE", "LETTING_CLAUSE",
                        "HAVING_CLAUSE", "ORDERBY_CLAUSE", "OFFSET_CLAUSE", "LIMIT_CLAUSE"]
        query = ""
        for clause in clause_order:
            converted_clause = template_map.get(clause, "")
            if converted_clause != "":
                query += converted_clause + " "
        return query

    def _convert_select_clause_template_n1ql(self, conversion_map, template_map):
        select_template = template_map['SELECT_TEMPLATE'][0]
        select_expression = select_template.split("SELECT")[1].strip()
        select_clause = "SELECT"
        from_info = template_map['FROM_FIELD']

        if select_expression == "FIELDS":
            random_select_fields = self._get_random_select_fields(from_info, conversion_map, template_map)
        else:
            print("Unknown select type")
            exit(1)

        for field in random_select_fields:
            select_clause += " " + field + ","

        select_clause = self._remove_trailing_substring(select_clause.strip(), ",")
        template_map['SELECT_CLAUSE'] = select_clause
        return template_map

    def _get_random_select_fields(self, from_info, conversion_map, template_map):
        from_field = from_info[0]
        from_type = from_info[1]
        table_map = conversion_map.get("table_map", {})
        random_fields = []
        if from_type == "BUCKET":
            all_fields = table_map[from_field]["fields"].keys()
            random_fields = self._random_sample(all_fields)
        elif from_type == "WITH_ALIAS":
            all_fields = template_map['WITH_FIELDS'][from_field]
            all_fields = [field_tuple[0] for field_tuple in all_fields]
            if len(all_fields) == 0:
                random_fields = [from_field]
            else:
                random_fields = self._random_sample(all_fields)
                random_fields = [from_field + "." + field for field in random_fields]
        else:
            print("Unknown from type")
            exit(1)

        return random_fields

    def _random_sample(self, list):
        size_of_sample = random.choice(range(1, len(list) + 1))
        random_sample = [list[i] for i in random.sample(xrange(len(list)), size_of_sample)]
        return random_sample

    def _convert_where_clause_template_n1ql(self, conversion_map, template_map):
        where_clause = "WHERE"

        from_info = template_map['FROM_FIELD']
        from_field = from_info[0]
        from_type = from_info[1]

        comparator = random.choice(['<', '>', '='])

        if from_type == "BUCKET":
            # need to add random field selection from bucket
            table_map = conversion_map.get("table_map", {})
            all_fields = table_map[from_field]["fields"].keys()
            random_field = random.choice(all_fields)
            where_clause += " " + random_field + " " + comparator + " " + str(self._random_constant(random_field))

        elif from_type == "WITH_ALIAS":
            with_alias_fields = template_map['WITH_FIELDS'][from_field]
            random_with_field_info = random.choice(with_alias_fields)
            random_with_field = random_with_field_info[0]
            where_clause += " " + from_field + "." + random_with_field + " " + comparator + " " + str(self._random_constant(random_with_field))
        else:
            print("Unknown from expression type")
            exit(1)

        template_map['WHERE_CLAUSE'] = where_clause
        return template_map

    def _random_constant(self, field=None):
        if field:
            if field == "int_field1":
                random_constant = random.randrange(36787, 99912344, 1000000)
            elif field == "bool_field1":
                random_constant = random.choice([True, False])
            elif field == "char_field1":
                random_constant = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(1))
                random_constant = "'%s'" %random_constant
            elif field == "datetime_field1":
                random_constant = "'%s'" % self._random_datetime()#= "'%s'" % random.choice(["1999-01-01 00:00:00", "2007-6-15 00:00:00", "2014-12-31 00:00:00"])
            elif field == "decimal_field1":
                random_constant = random.randrange(16, 9971, 10)
            elif field == "primary_key_id":
                random_constant = "'%s'" % random.randrange(1, 9999, 10)
            elif field == "varchar_field1":
                random_constant = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(5))
                random_constant = "'%s'" %random_constant
            else:
                print("Unknown field type")
                exit(1)
        else:
            constant_type = random.choice(["STRING", "INTEGER", "DECIMAL", "BOOLEAN"])
            if constant_type == "STRING":
                random_constant = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                random_constant = "'%s'" %random_constant
            elif constant_type == "INTEGER":
                random_constant = random.randrange(36787, 99912344, 1000000)
            elif constant_type == "DECIMAL":
                random_constant = float((random.randrange(700, 997400))/100)
            elif constant_type == "BOOLEAN":
                random_constant = random.choice([True, False])
            else:
                print("Unknown constant type")
                exit(1)
        return random_constant

    def _convert_from_clause_template_n1ql(self, conversion_map, template_map):
        from_template = template_map['FROM_TEMPLATE'][0]
        from_expression = from_template.split("FROM")[1].strip()
        from_clause = "FROM"
        if from_expression == "BUCKET_NAME":
            table_name = conversion_map.get("table_name", "simple_table")
            from_clause += " " + table_name
            template_map['FROM_FIELD'] = (table_name, "BUCKET")
        elif from_expression == "WITH_CLAUSE_ALIAS":
            with_clause_aliases = template_map['WITH_EXPRESSIONS'].keys()
            with_alias = random.choice(with_clause_aliases)
            from_clause += " " + with_alias
            template_map['FROM_FIELD'] = (with_alias, "WITH_ALIAS")
        template_map['FROM_CLAUSE'] = from_clause
        return template_map

    def _convert_with_clause_template_n1ql(self, conversion_map, template_map):
        with_template = template_map['WITH_TEMPLATE'][0]
        template_map['WITH_EXPRESSION_TEMPLATES'] = {}
        start_sep = "CTE_START"
        end_sep = "CTE_END"
        tmp = with_template.split(start_sep)

        for substring in tmp:
            if end_sep in substring:
                random_alias = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                template_map['WITH_EXPRESSION_TEMPLATES'][random_alias] = substring.split(end_sep)[0].strip()

        template_map['WITH_EXPRESSIONS'] = {}
        template_map['WITH_FIELDS'] = {}
        for with_expression_alias in template_map['WITH_EXPRESSION_TEMPLATES'].keys():
            template_map = self._convert_with_expression(with_expression_alias, template_map, conversion_map)

        converted_with_clause = "WITH"
        for converted_with_expression_alias in template_map['WITH_EXPRESSIONS'].keys():
            expression = template_map['WITH_EXPRESSIONS'][converted_with_expression_alias]
            converted_with_clause += " " + converted_with_expression_alias + " as " + "(" + expression + "),"

        with_clause = self._remove_trailing_substring(converted_with_clause.strip(), ",")
        template_map['WITH_CLAUSE'] = with_clause
        return template_map

    def _remove_trailing_substring(self, string, ending):
        if string.endswith(ending):
            return string[:-len(ending)]
        else:
            return string

    def _convert_with_expression(self, alias, template_map, conversion_map):
        expression = template_map['WITH_EXPRESSION_TEMPLATES'][alias]
        if expression == "WITH_CLAUSE_SUBQUERY":
            query_template = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM BUCKET_NAME FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
            with_template_map = self._extract_clauses(query_template)
            with_template_map = self._convert_from_clause_template_n1ql(conversion_map, with_template_map)
            with_template_map = self._convert_where_clause_template_n1ql(conversion_map, with_template_map)
            with_template_map = self._convert_select_clause_template_n1ql(conversion_map, with_template_map)
            with_expression = self._combine_converted_clauses(with_template_map)
        else:
            print("Unknown with expression template")
            exit(1)

        template_map['WITH_EXPRESSIONS'][alias] = with_expression
        template_map['WITH_FIELDS'][alias] = self._extract_fields_from_clause("SELECT", with_expression, with_template_map, conversion_map)
        return template_map

    def _extract_fields_from_clause(self, clause_type, expression, template_map, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        table_fields = table_map[table_name]["fields"].keys()

        if expression.find(clause_type) == -1:
            return []

        if clause_type == "SELECT":
            expression_fields_string = expression.split("SELECT")[1].split("FROM")[0].strip()
            #pprint.pprint(expression_fields_string)
        else:
            print("Unknown clause type")
            exit(1)

        return self._extract_raw_fields_from_string(table_fields, expression_fields_string)

    def _extract_raw_fields_from_string(self, raw_field_list, string):
        raw_fields = []
        for raw_field in raw_field_list:
            if raw_field.find('char') == 0:
                idx = self.find_char_field(string)
                if idx > -1:
                    raw_fields.append((raw_field, idx))
            else:
                idx = string.find(raw_field)
                if idx > -1:
                    raw_fields.append((raw_field, idx))
        return raw_fields

    def _extract_clauses(self, query_template):
        with_sep = ("WITH_TEMPLATE", "WITH_START", "WITH_END")
        from_sep = ("FROM_TEMPLATE", "FROM_START", "FROM_END")
        let_sep = ("LET_TEMPLATE", "LET_START", "LET_END")
        where_sep = ("WHERE_TEMPLATE", "WHERE_START", "WHERE_END")
        groupby_sep = ("GROUPBY_TEMPLATE", "GROUPBY_START", "GROUPBY_END")
        letting_sep = ("LETTING_TEMPLATE", "LETTING_START", "LETTING_END")
        having_sep = ("HAVING_TEMPLATE", "HAVING_START", "HAVING_END")
        select_sep = ("SELECT_TEMPLATE", "SELECT_START", "SELECT_END")
        orderby_sep = ("ORDERBY_TEMPLATE", "ORDERBY_START", "ORDERBY_END")
        offset_sep = ("OFFSET_TEMPLATE", "OFFSET_START", "OFFSET_END")
        limit_sep = ("LIMIT_TEMPLATE", "LIMIT_START", "LIMIT_END")

        clause_seperators = [with_sep, from_sep, let_sep, where_sep, groupby_sep,
                             letting_sep, having_sep, select_sep, orderby_sep, offset_sep, limit_sep]
        parsed_clauses = dict()
        parsed_clauses['RAW_QUERY_TEMPLATE'] = query_template
        for clause_seperator in clause_seperators:
            clause = clause_seperator[0]
            start_sep = clause_seperator[1]
            end_sep = clause_seperator[2]
            result = []
            tmp = query_template.split(start_sep)
            for substring in tmp:
                if end_sep in substring:
                    result.append(substring.split(end_sep)[0].strip())
            parsed_clauses[clause] = result
        return parsed_clauses

    def _convert_sql_template_for_skip_range_scan(self, n1ql_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        aggregate_pushdown = "secondary"
        sql, table_map = self._convert_sql_template_to_value(sql=n1ql_template, table_map=table_map, table_name=table_name, aggregate_pushdown=aggregate_pushdown, ansi_joins=False)
        n1ql = self._gen_sql_to_nql(sql, ansi_joins=False)
        sql = self._convert_condition_template_to_value_datetime(sql, table_map, sql_type="sql")
        n1ql = self._convert_condition_template_to_value_datetime(n1ql, table_map, sql_type="n1ql")
        sql_map = self._divide_sql(n1ql)

        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = {"n1ql": n1ql,
               "sql": sql,
               "bucket": str(",".join(table_map.keys())),
               "expected_result": None,
               "indexes": {}
               }

        table_name = random.choice(table_map.keys())
        map["bucket"] = table_name
        table_fields = table_map[table_name]["fields"].keys()

        aggregate_pushdown_index_name, create_aggregate_pushdown_index_statement = self._create_skip_range_key_scan_index(table_name, table_fields, sql_map)
        map = self.aggregate_special_convert(map)
        map["indexes"][aggregate_pushdown_index_name] = {"name": aggregate_pushdown_index_name,
                                                         "type": "GSI",
                                                         "definition": create_aggregate_pushdown_index_statement
                                                         }
        return map

    def _create_skip_range_key_scan_index(self, table_name, table_fields, sql_map):
        where_condition = sql_map["where_condition"]
        select_from = sql_map["select_from"]
        group_by = sql_map["group_by"]
        select_from_fields = []
        where_condition_fields = []
        groupby_fields = []
        aggregate_pushdown_fields_in_order = []
        skip_range_scan_index_fields_in_order = []

        for field in table_fields:
            if field.find('char') == 0:
                if select_from:
                    idx = self.find_char_field(select_from)
                    if idx > -1:
                        select_from_fields.append((idx, field))
                if where_condition:
                    idx = self.find_char_field(where_condition)
                    if idx > -1:
                        where_condition_fields.append((idx, field))
                if group_by:
                    idx = self.find_char_field(group_by)
                    if idx > -1:
                        groupby_fields.append((idx, field))
            else:
                if select_from:
                    idx = select_from.find(field)
                    if idx > -1:
                        select_from_fields.append((idx, field))
                if where_condition:
                    idx = where_condition.find(field)
                    if idx > -1:
                        where_condition_fields.append((idx, field))
                if group_by:
                    idx = group_by.find(field)
                    if idx > -1:
                        groupby_fields.append((idx, field))

        select_from_fields.sort(key=lambda tup: tup[0])
        where_condition_fields.sort(key=lambda tup: tup[0])
        groupby_fields.sort(key=lambda tup: tup[0])

        leading_key = random.choice(where_condition_fields)
        skip_range_scan_index_fields_in_order.append(leading_key[1])
        where_condition_fields.remove(leading_key)
        all_fields = select_from_fields + where_condition_fields + groupby_fields

        for i in range(0, len(all_fields)):
            num_random_fields = random.choice([1, 2, 3])
            for j in range(0, num_random_fields):
                random_field = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
                skip_range_scan_index_fields_in_order.append(random_field)
            next_field = random.choice(all_fields)
            all_fields.remove(next_field)
            skip_range_scan_index_fields_in_order.append(next_field[1])

        aggregate_pushdown_index_name = "{0}_aggregate_pushdown_index_{1}".format(table_name, self._random_int())

        create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) USING GSI".format(aggregate_pushdown_index_name, table_name, self._convert_list(skip_range_scan_index_fields_in_order, "numeric"))
        return aggregate_pushdown_index_name, create_aggregate_pushdown_index

    ''' Main function to convert templates into SQL and N1QL queries for GROUP BY clause field aliases '''
    def _convert_sql_template_for_group_by_aliases(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        n1ql_template_map = self._divide_sql(query_template)

        sql_template_map = copy.copy(n1ql_template_map)
        sql_template_map = self._add_field_aliases_to_sql_select_clause(sql_template_map)
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'where_condition')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'order_by')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'group_by')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'having')

        sql_table, table_map = self._gen_select_tables_info(sql_template_map["from_fields"], table_map, False)
        converted_sql_map, converted_n1ql_map = self._apply_group_by_aliases(sql_template_map, n1ql_template_map, table_map)
        n1ql, sql, table_map = self._convert_sql_n1ql_templates_to_queries(converted_n1ql_map=converted_n1ql_map,
                                                                                         converted_sql_map=converted_sql_map,
                                                                                         table_map=table_map,
                                                                                         table_name=table_name)
        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = { "n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(table_map.keys())),
                "expected_result": None,
                "indexes": {} }
        return map

    ''' Function builds whole SQL and N1QL queries after applying field aliases to all available query clauses. '''
    def _convert_sql_n1ql_templates_to_queries(self, converted_n1ql_map={}, converted_sql_map={}, table_map={}, table_name="simple_table"):

        new_sql = "SELECT " + converted_sql_map['select_from'] + " FROM " + table_name
        if converted_sql_map['where_condition']:
            new_sql += " WHERE " + converted_sql_map['where_condition']
        if converted_sql_map['group_by']:
            new_sql += " GROUP BY " + converted_sql_map['group_by']
        if converted_sql_map['having']:
            new_sql += " HAVING " + converted_sql_map['having']
        if converted_sql_map['order_by']:
            new_sql += " ORDER BY " + converted_sql_map['order_by']

        new_n1ql = "SELECT " + converted_n1ql_map['select_from'] + " FROM " + table_name
        if converted_n1ql_map['where_condition']:
            new_n1ql += " WHERE " + converted_n1ql_map['where_condition']
        if converted_n1ql_map['group_by']:
            new_n1ql += " GROUP BY " + converted_n1ql_map['group_by']
        if converted_n1ql_map['having']:
            new_n1ql += " HAVING " + converted_n1ql_map['having']
        if converted_n1ql_map['order_by']:
            new_n1ql += " ORDER BY " + converted_n1ql_map['order_by']

        return new_n1ql, new_sql, table_map

    ''' Function constructs temp dictionaries for SQL and N1QL query clauses and passes them to 
        transform function. '''
    def _apply_group_by_aliases(self, sql_template_map={}, n1ql_template_map={}, table_map={}):

        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint"], table_map)

        converted_sql_map = copy.deepcopy(sql_template_map)
        converted_n1ql_map = copy.deepcopy(n1ql_template_map)

        if "BOOL_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(bool_field_names,
                                                                                    "BOOL_ALIAS",
                                                                                    "BOOL_FIELD",
                                                                                    n1ql_template_map,
                                                                                    converted_n1ql_map,
                                                                                    converted_sql_map)
        if "STRING_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(string_field_names,
                                                                                 "STRING_ALIAS",
                                                                                 "STRING_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)
        if "NUMERIC_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(numeric_field_names,
                                                                                 "NUMERIC_ALIAS",
                                                                                 "NUMERIC_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)
        if "DATETIME_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(datetime_field_names,
                                                                                 "DATETIME_ALIAS",
                                                                                 "DATETIME_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)

        return converted_sql_map, converted_n1ql_map

    def _add_field_aliases_to_sql_select_clause(self, sql_map):
        select_from = sql_map['select_from']

        select_from = self._add_field_alias(select_from, "NUMERIC_FIELD", "NUMERIC_ALIAS")
        select_from = self._add_field_alias(select_from, "VARCHAR_FIELD", "VARCHAR_ALIAS")
        select_from = self._add_field_alias(select_from, "BOOL_FIELD", "BOOL_ALIAS")
        select_from = self._add_field_alias(select_from, "DATETIME_FIELD", "DATETIME_ALIAS")

        sql_map['select_from'] = select_from
        return sql_map

    def _add_field_alias(self, select_from, field_token, alias_token):
        if not (field_token + " " + alias_token) in select_from:
            if alias_token in select_from:
                select_from = select_from.replace(alias_token, field_token + " " + alias_token)
            else:
                select_from = select_from.replace(field_token, field_token + " " + alias_token)
        return select_from

    def _cleanup_field_aliases_from_sql_clause(self, sql_map={}, clause_name=''):
        if clause_name in sql_map.keys():
            clause_str = sql_map[clause_name]

            clause_str = clause_str.replace("NUMERIC_FIELD NUMERIC_ALIAS", "NUMERIC_FIELD").\
                replace("BOOL_FIELD BOOL_ALIAS", "BOOL_FIELD").replace("STRING_FIELD STRING_ALIAS", "STRING_FIELD").\
                replace("DATETIME_FIELD DATETIME_ALIAS", "DATETIME_FIELD")

            clause_str = clause_str.replace("NUMERIC_ALIAS", "NUMERIC_FIELD").replace("BOOL_ALIAS", "BOOL_FIELD").\
                replace("STRING_ALIAS", "STRING_FIELD").replace("DATETIME_ALIAS", "DATETIME_FIELD")

            sql_map[clause_name] = clause_str

        return sql_map


    ''' Function substitutes XXX_FIELD and XXX_ALIAS placeholders with real field names and their aliases.
        Additional transformations to stay compatible with common SQL and N1QL syntax.
        Additional transformations to produce the same fields aliases usage for SELECT clause in SQL and N1QL queries.'''
    def normalize_field_aliases(self, field_names, alias_token, field_token, n1ql_map, converted_n1ql_map, converted_sql_map):
        field_name = random.choice(field_names)

        group_by_alias = alias_token in n1ql_map["group_by"]
        select_alias = (field_token+" "+alias_token) in n1ql_map["select_from"]

        if alias_token in n1ql_map["group_by"]:
            converted_n1ql_map['group_by'] = converted_n1ql_map['group_by'].replace(alias_token, field_name[:-1])

        if (field_token+" "+alias_token) in n1ql_map["select_from"]:
            converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
            converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
        elif field_token in n1ql_map["select_from"]:
            converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token, field_name)
            converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, field_name)
        else: #BOOL_ALIAS
            if group_by_alias:
                converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(alias_token, field_name[:-1])
                converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
            else:
                converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(alias_token, field_name)
                converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, field_name)

        if alias_token in n1ql_map['order_by']:
            if group_by_alias or select_alias:
                converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(alias_token, field_name[:-1])
            else:
                converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(alias_token, field_name)

        if alias_token in n1ql_map['having']:
            if group_by_alias:
                converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(alias_token, field_name[:-1])
            else:
                converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(alias_token, field_name)

        converted_n1ql_map['group_by'] = converted_n1ql_map['group_by'].replace(field_token, field_name)
        converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token, field_name)
        converted_n1ql_map['where_condition'] = converted_n1ql_map['where_condition'].replace(field_token, field_name)
        converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(field_token, field_name)
        converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(field_token, field_name)

        converted_sql_map['group_by'] = converted_sql_map['group_by'].replace(field_token, field_name)
        converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token, field_name)
        converted_sql_map['where_condition'] = converted_sql_map['where_condition'].replace(field_token, field_name)
        converted_sql_map['order_by'] = converted_sql_map['order_by'].replace(field_token, field_name)
        converted_sql_map['having'] = converted_sql_map['having'].replace(field_token, field_name)

        converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(alias_token, field_name[:-1])

        return converted_n1ql_map, converted_sql_map

