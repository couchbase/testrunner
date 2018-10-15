query:
 	START_MAIN select END_MAIN ;
select:
		SELECT OUTERBUCKET.*,(SELECT SUBTABLE.productId FROM SUBTABLE WHERE SUBTABLE.primary_key_id=OUTERBUCKET.primary_key_id ORDER BY SUBTABLE.price asc_nulls_first limit 1) ABC  FROM OUTERBUCKET WHERE subquery_condition_exists |
		SELECT OUTERBUCKET.*,(SELECT SUBTABLE.productId FROM SUBTABLE WHERE SUBTABLE.primary_key_id=OUTERBUCKET.primary_key_id ORDER BY SUBTABLE.price desc_nulls_last limit 1) ABC  FROM OUTERBUCKET WHERE subquery_condition_exists |
		SELECT OUTERBUCKET.*,(SELECT sum(SUBTABLE.productId) as productId FROM SUBTABLE WHERE SUBTABLE.primary_key_id=OUTERBUCKET.primary_key_id GROUP BY  SUBTABLE.price  ORDER BY SUBTABLE.price nulls_first_last limit 1) ABC  FROM OUTERBUCKET WHERE subquery_condition_exists |
		SELECT OUTERBUCKET.*,(SELECT max(SUBTABLE.productId) as productId  FROM SUBTABLE WHERE SUBTABLE.primary_key_id=OUTERBUCKET.primary_key_id GROUP BY  SUBTABLE.price  ORDER BY SUBTABLE.price nulls_first_last limit 1) ABC  FROM OUTERBUCKET WHERE subquery_condition_exists |
		SELECT OUTERBUCKET.*,(SELECT min(SUBTABLE.productId) as productId FROM SUBTABLE WHERE SUBTABLE.primary_key_id=OUTERBUCKET.primary_key_id GROUP BY  SUBTABLE.price  ORDER BY  SUBTABLE.price nulls_first_last limit 1) ABC  FROM OUTERBUCKET WHERE subquery_condition_exists |
		SELECT OUTERBUCKET.*,(SELECT count(SUBTABLE.productId) as productId FROM SUBTABLE WHERE SUBTABLE.primary_key_id=OUTERBUCKET.primary_key_id GROUP BY  SUBTABLE.price  ORDER BY SUBTABLE.price nulls_first_last limit 1) ABC  FROM OUTERBUCKET WHERE subquery_condition_exists ;

asc_nulls_first:
    ASC | ASC NULLS FIRST ;

desc_nulls_last:
    DESC | DESC NULLS LAST ;

nulls_first_last:
    | ASC NULLS FIRST | DESC NULLS LAST ;

subquery_condition_exists:
	exists_operator_type START_EXISTS_SUBQUERY ( rule_subquery_exists ) END_EXISTS_SUBQUERY ;
exists_operator_type:
	EXISTS | NOT EXISTS;
rule_subquery_exists:
    SELECT * FROM BUCKET_NAME  WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON  MYSQL_OPEN_PAR subquery_where_condition MYSQL_CLOSED_PAR;
	#SELECT * FROM BUCKET_NAME use_key_conditions WHERE AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON  MYSQL_OPEN_PAR subquery_where_condition MYSQL_CLOSED_PAR;
subquery_where_condition:
	MYSQL_OPEN_PAR complex_condition MYSQL_CLOSED_PAR  ;
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



