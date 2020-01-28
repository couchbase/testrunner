# min,max...(leading key), count(leading key),composite filters on any keys which are covered
# no group by,no or clause ,no not clause,no in cluase, no intersect scan

query:
 	select ;

select:
	SELECT select_from FROM BUCKET_NAME WHERE complex_condition ORDER BY select_from nulls_first_last |
	SELECT select_from FROM BUCKET_NAME WHERE complex_condition GROUP BY field_list |
	SELECT select_from FROM BUCKET_NAME WHERE complex_condition |
	SELECT sel_from FROM BUCKET_NAME WHERE numeric_condition |
	SELECT sel_from FROM BUCKET_NAME WHERE string_condition |
	SELECT sel_from FROM BUCKET_NAME WHERE bool_condition;

nulls_first_last:
    | ASC NULLS FIRST | DESC NULLS LAST ;

create_index:
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(FIELD_LIST) WHERE complex_condition |
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(complex_condition) |
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(USER_FIELD_LIST);

sel_from:
	COUNT(*) | COUNT( field ) | MIN( non_string_field ) | COUNT( DISTINCT 1 ) | COUNT( DISTINCT field ) | COUNT( 1 );

select_from:
	COUNT(*) |  COUNT( field ) | SUM( non_string_field ) | SUM(DISTINCT non_string_field ) | aggregate_function( non_string_field ) | aggregate_function(non_string_field ) |  MAX( non_string_field ) | MIN( non_string_field );

aggregate_function:
    AVG | STDDEV | VARIANCE | STDDEV_SAMP | STDDEV_POP | VARIANCE_POP | VARIANCE_SAMP | MEAN ;

complex_condition:
	NOT (condition) | (condition) AND (condition) | (condition) OR (condition) | (condition) AND (condition) OR (condition) AND (condition) | condition;

condition:
	numeric_condition | string_condition | bool_condition | (string_condition AND numeric_condition) |
	(numeric_condition OR string_condition) | (bool_condition AND numeric_condition) |  (bool_condition OR numeric_condition) |
	 (bool_condition AND numeric_condition) | (bool_condition OR string_condition) |
	 (bool_condition AND string_condition) | (numeric_condition AND string_condition AND bool_condition);

field:
	NUMERIC_FIELD | STRING_FIELD;

non_string_field:
	NUMERIC_FIELD;

simple_condition:
    numeric_condition | string_condition | bool_condition;


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
	numeric_in_condition ;

numeric_equals_condition:
	numeric_field = numeric_value ;

numeric_not_equals_condition:
	numeric_field != numeric_value ;

numeric_in_condition:
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
	bool_field = true|
	NOT (bool_field = true) |
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
