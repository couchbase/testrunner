query:
 	select ;

create_index:
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(NUMERIC_FIELD)  |
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(STRING_FIELD) |
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(BOOL_FIELD) |
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(NUMERIC_FIELD,STRING_FIELD,BOOL_FIELD);

select:
	SELECT sel_from FROM BUCKET_NAME WHERE numeric_condition |
	SELECT sel_from FROM BUCKET_NAME WHERE string_condition |
	SELECT sel_from FROM BUCKET_NAME WHERE bool_condition;

sel_from:
	COUNT( field ) | MIN( non_string_field ) | MAX( non_string_field ) ;

complex_condition:
	(condition) AND (condition) | condition;

condition:
	numeric_condition | string_condition | bool_condition | (string_condition AND numeric_condition) |
	 (bool_condition AND numeric_condition) |(bool_condition AND numeric_condition)  |
	 (bool_condition AND string_condition) | (numeric_condition AND string_condition AND bool_condition);

field:
	NUMERIC_FIELD | STRING_FIELD | BOOL_FIELD;

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
	numeric_between_condition |
	numeric_is_null ;

numeric_equals_condition:
	numeric_field = numeric_value ;

numeric_not_equals_condition:
	numeric_field != numeric_value ;

numeric_in_condition:
	numeric_field IN ( numeric_field_list );

numeric_between_condition:
	NUMERIC_FIELD BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

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
	string_is_not_null |
	string_is_null |
	string_like_condition |
	string_equals_condition ;

string_equals_condition:
	string_field = string_values;

string_not_equals_condition:
	string_field != string_values | string_field <> string_values ;

string_between_condition:
	string_field BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

string_is_not_null:
	string_field IS NOT NULL;

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
