query:
 	START_MAIN select END_MAIN ;

select:
#	SELECT OUTER_BUCKET_NAME.* FROM BUCKET_NAME WHERE subquery_fields_comparisons |
    SELECT OUTER_BUCKET_NAME.* FROM BUCKET_NAME WHERE subquery_condition_exists |
    SELECT OUTER_BUCKET_NAME.* FROM BUCKET_NAME WHERE subquery_condition_exists_limit_offset limit 2 offset 1;
	#SELECT OUTER_BUCKET_NAME.* FROM BUCKET_NAME WHERE subquery_agg_exists |
    #SELECT OUTER_BUCKET_NAME.* FROM BUCKET_NAME  WHERE subquery_in;

subquery_fields_comparisons:
	INNER_SUBQUERY_FIELDS comparison_operator START_FIELDS_COMPARISON_SUBQUERY ( rule_subquery_fields_comparisons ) END_FIELDS_COMPARISON_SUBQUERY ;

subquery_condition_exists:
	exists_operator_type START_EXISTS_SUBQUERY ( rule_subquery_exists ) END_EXISTS_SUBQUERY ;

subquery_condition_exists_limit_offset:
	exists_operator_type START_EXISTS_SUBQUERY ( rule_subquery_exists_limit_offset ) END_EXISTS_SUBQUERY ;

subquery_agg_exists:
	INNER_SUBQUERY_AGG_FIELD comparison_operator START_AGG_SUBQUERY ( rule_subquery_agg_exists ) END_AGG_SUBQUERY ;

subquery_in:
	INNER_SUBQUERY_IN_FIELD in_operator START_IN_SUBQUERY ( rule_subquery_in ) END_IN_SUBQUERY ;

subquery_selects:
	subquery_agg_exists  | subquery_condition_exists | subquery_in | subquery_fields_comparisons;

subquery_where_condition:
	MYSQL_OPEN_PAR complex_condition MYSQL_CLOSED_PAR | subquery_selects ;

rule_subquery_exists:
	SELECT * FROM BUCKET_NAME WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON  MYSQL_OPEN_PAR subquery_where_condition MYSQL_CLOSED_PAR;

rule_subquery_exists_limit_offset:
    SELECT * FROM BUCKET_NAME WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON  MYSQL_OPEN_PAR subquery_where_condition MYSQL_CLOSED_PAR limit 10 offset 0 ;


rule_subquery_fields_comparisons:
	SELECT OUTER_SUBQUERY_FIELDS FROM BUCKET_NAME WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON complex_condition ;

rule_subquery_in:
	SELECT RAW OUTER_SUBQUERY_IN_FIELD FROM BUCKET_NAME WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON subquery_where_condition ;

rule_subquery_agg_exists:
	SELECT RAW select_from_with_aggregate FROM BUCKET_NAME WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON subquery_where_condition ;

outer_inner_table_primary_key_comparison:
	AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON;

in_operator:
	IN;

comparison_operator:
	EQUALS | NOT_EQUALS;

exists_operator_type:
	EXISTS | NOT EXISTS;

direction:
	ASC | DESC;

select_from:
	*   ;

select_from_with_aggregate:
	aggregate_function(OUTER_SUBQUERY_AGG_FIELD) AS OUTER_SUBQUERY_AGG_FIELD | SUM(OUTER_SUBQUERY_AGG_FIELD) AS OUTER_SUBQUERY_AGG_FIELD | MAX(OUTER_SUBQUERY_AGG_FIELD) AS OUTER_SUBQUERY_AGG_FIELD| MIN(OUTER_SUBQUERY_AGG_FIELD) AS OUTER_SUBQUERY_AGG_FIELD | COUNT(*) AS OUTER_SUBQUERY_AGG_FIELD;

aggregate_function:
    AVG | STDDEV | VARIANCE | STDDEV_SAMP | STDDEV_POP | VARIANCE_POP | VARIANCE_SAMP | MEAN ;

complex_condition:
	NOT (condition) | (condition) AND (condition) | (condition) OR (condition) | (condition) AND (condition) OR (condition) AND (condition) | condition | (complex_condition) AND (complex_condition) | (complex_condition) OR (complex_condition) | NOT (complex_condition);

condition:
	numeric_condition | string_condition | bool_condition | (string_condition AND numeric_condition) |
	(numeric_condition OR string_condition) | (bool_condition AND numeric_condition) |  (bool_condition OR numeric_condition) |
	 (bool_condition AND numeric_condition) | (bool_condition OR string_condition) |
	 (bool_condition AND string_condition) | (numeric_condition AND string_condition AND bool_condition);

field:
	NUMERIC_FIELD | STRING_FIELD;

non_string_field:
	NUMERIC_FIELD;

# NUMERIC RULES

numeric_condition:
	numeric_field < numeric_value |
	numeric_field = numeric_value |
	numeric_field > numeric_value |
	numeric_field  >= numeric_value |
	numeric_field  <= numeric_value |
	(numeric_condition) AND (numeric_condition)|
	(numeric_condition) OR (numeric_condition)|
	NOT (numeric_condition) |
	numeric_between_condition |
	numeric_is_not_null |
	numeric_not_equals_condition |
	numeric_is_null |
	numeric_in_conidtion ;

numeric_equals_condition:
	numeric_field = numeric_value ;

numeric_not_equals_condition:
	numeric_field != numeric_value ;

numeric_in_conidtion:
	numeric_field IN ( numeric_field_list );

numeric_between_condition:
	NUMERIC_FIELD BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

numeric_not_between_condition:
	NUMERIC_FIELD NOT BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

numeric_is_not_null:
	NUMERIC_FIELD IS NOT NULL;

numeric_is_missing:
	NUMERIC_FIELD IS MISSING;

numeric_is_not_missing:
	NUMERIC_FIELD IS NOT MISSING;

numeric_is_valued:
	NUMERIC_FIELD IS VALUED;

numeric_is_not_valued:
	NUMERIC_FIELD IS NOT VALUED;

numeric_is_null:
	NUMERIC_FIELD IS NULL;

numeric_field_list:
	LIST;

numeric_field:
	NUMERIC_FIELD;

numeric_value:
	NUMERIC_VALUE;

# STRING RULES

string_condition:
	string_field < string_values |
	string_field > string_values |
	string_field  >= string_values |
	string_field  <= string_values |
	(string_condition) AND (string_condition) |
	(string_condition) OR (string_condition) |
	string_not_between_condition |
	NOT (string_condition) |
	string_is_not_null |
	string_is_null |
	string_not_equals_condition |
	string_in_conidtion |
	string_like_condition |
	string_equals_condition |
	string_not_like_condition ;

string_equals_condition:
	string_field = string_values;

string_not_equals_condition:
	string_field != string_values | string_field <> string_values ;

string_between_condition:
	string_field BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

string_not_between_condition:
	string_field NOT BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

string_is_not_null:
	string_field IS NOT NULL;

string_in_conidtion:
	string_field IN ( string_field_list );

string_is_null:
	string_field IS NULL;

string_like_condition:
	string_field LIKE 'STRING_VALUES%' | string_field LIKE '%STRING_VALUES' | string_field LIKE STRING_VALUES | string_field LIKE '%STRING_VALUES%';

string_not_like_condition:
	string_field NOT LIKE 'STRING_VALUES%' | string_field NOT LIKE '%STRING_VALUES' | string_field NOT LIKE STRING_VALUES |  string_field NOT LIKE '%STRING_VALUES%';

string_field_list:
	LIST;

string_is_missing:
	STRING_FIELD IS MISSING;

string_is_not_missing:
	STRING_FIELD IS NOT MISSING;

string_is_valued:
	STRING_FIELD IS VALUED;

string_is_not_valued:
	STRING_FIELD IS NOT VALUED;

string_field:
	STRING_FIELD;

string_values:
	STRING_VALUES;

# BOOLEAN RULES

bool_condition:
	bool_field |
	NOT (bool_field) |
	bool_equals_condition |
	bool_not_equals_condition ;

bool_equals_condition:
	bool_field = bool_value;

bool_not_equals_condition:
	bool_field != bool_value ;

bool_field:
	BOOL_FIELD;

bool_value:
	true | false;

field_list:
	NUMERIC_FIELD_LIST | STRING_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST, BOOL_FIELD_LIST;
