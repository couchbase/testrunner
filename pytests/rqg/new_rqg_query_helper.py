import random
import copy
import string
from base_query_helper import BaseRQGQueryHelper


class RQGQueryHelperNew(BaseRQGQueryHelper):

    ''' Dispatcher function. Uses test_name parameter to identify the way how templates
        will be transformed into SQL and N1QL queries.
        from let where group by letting haVING SELECT ORDER BY'''
    def _get_conversion_func(self, test_name):
        if test_name == 'group_by_alias':
            return self._convert_sql_template_for_group_by_aliases
        elif test_name == 'skip_range_key_scan':
            return self._convert_sql_template_for_skip_range_scan
        else:
            print("Unknown test name")
            exit(1)

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

