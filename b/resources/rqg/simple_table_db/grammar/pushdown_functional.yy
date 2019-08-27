query:
 	select;

select:
	SELECT sel_from FROM BUCKET_NAME WHERE complex_condition group_by_order_by |
	SELECT sel_from FROM BUCKET_NAME WHERE numeric_condition group_by_order_by |
	SELECT sel_from FROM BUCKET_NAME WHERE string_condition group_by_order_by |
	SELECT sel_from FROM BUCKET_NAME WHERE bool_condition group_by_order_by |

	SELECT sel_from_no_group_by FROM BUCKET_NAME WHERE complex_condition maybe_limit_offset |
    SELECT sel_from_no_group_by FROM BUCKET_NAME WHERE numeric_condition maybe_limit_offset |
    SELECT sel_from_no_group_by FROM BUCKET_NAME WHERE string_condition maybe_limit_offset |
    SELECT sel_from_no_group_by FROM BUCKET_NAME WHERE bool_condition maybe_limit_offset ;

maybe_limit_offset:
    |
    limit 10 offset 4;

sel_from_no_group_by:
    sel_agg_a, COUNT(1) |
    sel_agg_a, sel_agg_b |
    sel_agg_a, sel_agg_b, sel_agg_c ;

sel_from:
    sel_agg_a |
    sel_agg_a, sel_agg_b |
    sel_agg_a, sel_agg_b, sel_agg_c |
    sel_agg_a, sel_non_agg |
    sel_non_agg, sel_agg_a |
    sel_agg_a, sel_non_agg, sel_agg_b |
    sel_agg_a, sel_agg_b, sel_non_agg |
    sel_non_agg, sel_agg_a, sel_agg_b |
    sel_agg_a, sel_non_agg AS A, sel_non_agg AS B |
    sel_non_agg AS A, sel_non_agg AS B, sel_agg_a |
    sel_non_agg AS A, sel_agg_a, sel_non_agg AS B ;

sel_agg_a:
    agg( agg_expression ) ;

sel_agg_b:
    agg( agg_expression ) ;

sel_agg_c:
    agg( agg_expression ) ;

agg_expression:
    extra_expression_a num_func( non_string_field ) extra_expression_b |
    extra_expression_a special_num_funcs extra_expression_b |
    extra_expression_a string_func( string_field ) extra_expression_b |
    extra_expression_a special_string_func extra_expression_b |
    extra_expression_a special_date_func extra_expression_b ;

sel_non_agg:
    GROUPBY_FIELD;

any_field:
    NUMERIC_FIELD | STRING_FIELD | BOOL_FIELD | DATETIME_FIELD;

agg:
    MIN |
    MAX |
    SUM |
    COUNT |
    aggregate_function ;

aggregate_function:
    AVG | MEAN ;

num_func:
    ABS |
    CEIL |
    COS |
    DEGREES |
    RADIANS |
    SIGN |
    SIN |
    TAN |
    FLOOR ;

# special cases: ATAN2, E, PI, POWER, ROUND, TRUNC, RANDOM, ACOS, ASIN, ATAN,

special_num_funcs:
    SQRT( ABS( non_string_field ) ) |
    LN( ABS( non_string_field ) ) |
    LOG( ABS( non_string_field ) ) |
    EXP( COS( non_string_field ) ) ;

string_func:
    LENGTH ;

special_string_func:
    POSITION( string_field , search_string ) |
    POSITION( LOWER( string_field ) , search_string ) |
    POSITION( LOWER( string_field ) , LOWER( search_string ) ) |
    POSITION( string_field , UPPER( search_string ) ) |
    POSITION( UPPER( string_field ) , search_string ) |
    POSITION( UPPER( string_field ) , UPPER( search_string ) ) ;

search_string:
    "a" |
    "e" |
    "i" |
    "o" |
    "u" |
    "r" |
    "s" |
    "t" |
    "l" |
    "n" ;

special_date_func:
    DATE_PART_STR( datetime_field , date_part ) ;

date_part:
    "DAY" |
    "MONTH" |
    "YEAR" ;

extra_expression_a:
    2 + |
    2 - |
    2 * |
     |
    2 / ;

extra_expression_b:
    + 2 |
    - 2 |
    * 2 |
     |
    / 2 ;

complex_condition:
	(condition) AND (condition) | condition;

condition:
	numeric_condition | string_condition | bool_condition | (string_condition AND numeric_condition) | (bool_condition AND numeric_condition) |
	 (bool_condition AND numeric_condition) | (bool_condition AND string_condition) | (numeric_condition AND string_condition AND bool_condition);

field:
	NUMERIC_FIELD | STRING_FIELD;

non_string_field:
	NUMERIC_FIELD;

simple_condition:
    numeric_condition | string_condition | bool_condition;

group_by_order_by:
    GROUP BY field_list;

# NUMERIC RULES

numeric_condition:
	numeric_field < numeric_value |
	numeric_field = numeric_value |
	numeric_field > numeric_value |
	numeric_field  >= numeric_value |
	numeric_field  <= numeric_value |
	(numeric_condition) AND (numeric_condition) |
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
	string_not_between_condition |
	string_is_not_null |
	string_is_null |
	string_not_equals_condition |
	string_in_condition |
	string_equals_condition;

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

string_in_condition:
	string_field IN ( string_field_list );

string_is_null:
	string_field IS NULL;

string_like_condition:
	string_field LIKE 'STRING_VALUES%' | string_field LIKE STRING_VALUES;

string_not_like_condition:
	string_field NOT LIKE 'STRING_VALUES%' | string_field NOT LIKE STRING_VALUES;

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

datetime_field:
    DATETIME_FIELD ;

field_list:
	NUMERIC_FIELD_LIST | STRING_FIELD_LIST | BOOL_FIELD_LIST | DATETIME_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST, BOOL_FIELD_LIST;
