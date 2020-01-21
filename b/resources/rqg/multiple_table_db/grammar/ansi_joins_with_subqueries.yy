query:
 	select ;

select:
	SELECT select_from FROM from_bucket WHERE complex_condition;

nulls_first_last:
    ASC NULLS FIRST | DESC NULLS LAST ;

direction:
	ASC | DESC;

select_from:
	OUTER_BUCKET_NAME.* ;

from_bucket:
	SUBQUERY_START (SELECT select_from FROM BUCKET_NAME WHERE complex_condition) ALIAS SUBQUERY_END INNER JOIN SUBQUERY_START (SELECT select_from FROM BUCKET_NAME WHERE complex_condition) ALIAS SUBQUERY_END ON ( ansi_joins_complex_condition ) |
	SUBQUERY_START (SELECT select_from FROM BUCKET_NAME WHERE complex_condition) ALIAS SUBQUERY_END LEFT JOIN SUBQUERY_START (SELECT select_from FROM BUCKET_NAME WHERE complex_condition) ALIAS SUBQUERY_END ON ( ansi_joins_complex_condition ) ;

#JOIN RULES
ansi_joins_complex_condition:
    join_condition | join_condition | join_condition | join_condition | join_condition;

join_condition:
	join_numeric_condition | join_string_condition | (join_string_condition AND join_numeric_condition) |
	(join_numeric_condition OR join_string_condition) | (join_string_condition AND join_numeric_condition) |
	 (join_string_condition AND join_numeric_condition) | (join_numeric_condition OR join_string_condition) |
	 (join_numeric_condition AND join_string_condition) | (join_numeric_condition AND join_string_condition AND join_numeric_condition) |
	 (join_numeric_condition AND join_string_condition) |
	 (join_string_condition AND join_numeric_condition) | (join_string_condition AND join_numeric_condition) |
	 (join_numeric_condition AND join_string_condition) |
	 (join_numeric_condition AND join_string_condition) |
	 (join_string_condition AND join_numeric_condition) | (join_string_condition AND join_numeric_condition) | (join_string_condition AND join_string_condition) |
	 (join_numeric_condition AND join_numeric_condition);

#JOIN NUMERIC RULES

join_numeric_condition:
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
    previous_numeric_field = current_numeric_field |
	previous_numeric_field < current_numeric_field |
	previous_numeric_field = current_numeric_field |
	previous_numeric_field > current_numeric_field |
	previous_numeric_field  >= current_numeric_field |
	previous_numeric_field  <= current_numeric_field |
	join_numeric_not_equals_condition;

join_numeric_not_equals_condition:
	previous_numeric_field != current_numeric_field ;

previous_numeric_field:
	PREVIOUS_TABLE.NUMERIC_FIELD;

current_numeric_field:
	CURRENT_TABLE.NUMERIC_FIELD;

#JOIN STRING RULES

join_string_condition:
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
    previous_string_field = current_string_field |
	previous_string_field < current_string_field |
	previous_string_field > current_string_field |
	previous_string_field  >= current_string_field |
	previous_string_field  <= current_string_field |
	join_string_not_equals_condition;

join_string_not_equals_condition:
	previous_string_field != current_string_field | previous_string_field <> current_string_field ;


previous_string_field:
	PREVIOUS_TABLE.STRING_FIELD;

current_string_field:
	CURRENT_TABLE.STRING_FIELD;

#JOIN BOOL RULES

join_bool_condition:
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_equals_condition |
	join_bool_not_equals_condition ;

join_bool_equals_condition:
	previous_bool_field = current_bool_field;

join_bool_not_equals_condition:
	previous_bool_field != current_bool_field ;

previous_bool_field:
	PREVIOUS_TABLE.BOOL_FIELD;

current_bool_field:
	CURRENT_TABLE.BOOL_FIELD;

complex_condition:
	NOT (condition) | (condition) AND (condition) | (condition) OR (condition) | condition | condition | condition | condition | condition | condition |
	condition | condition | condition | condition | condition | condition  | condition | condition | condition ;

condition:
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	numeric_condition | string_condition | bool_condition | numeric_condition | string_condition | bool_condition |
	(string_condition AND numeric_condition) |
	(numeric_condition OR string_condition) | (bool_condition AND numeric_condition) |
	(bool_condition AND numeric_condition) | (bool_condition OR string_condition) | (bool_condition AND string_condition);

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
	NULL_NUM_FIELD IS NULL or NULL_NUM_FIELD IS MISSING;

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
	NULL_STR_FIELD IS NULL OR NULL_STR_FIELD IS MISSING;

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


