query:
 	select;

select:
	SELECT sel_field_groupby FROM BUCKET_NAME WHERE primary_condition GROUP BY primary_group_by_expression |
    SELECT sel_field_no_groupby FROM BUCKET_NAME WHERE primary_condition;

sel_field_groupby:
    COUNT(1), agg_expression |
    agg_expression, multi_agg ;

sel_field_no_groupby:
    COUNT(1), agg_expression |
    agg_expression, multi_agg ;

multi_agg:
    agg_expression |
    agg_expression, multi_agg |
    agg_expression, multi_agg |
    agg_expression, multi_agg ;

agg_expression:
    string_agg_exp |
    numerical_agg_exp ;

string_agg_exp:
    string_agg( PRIMARY_KEY ) |
    string_agg( string_func_1 ) |
    string_agg( string_func_2 ) |
    string_agg( string_func_3 ) ;

string_agg:
    MIN |
    MAX |
    COUNT ;

string_func_1:
    REVERSE( PRIMARY_KEY ) |
    UPPER( PRIMARY_KEY ) |
    LOWER( PRIMARY_KEY ) ;

string_func_2:
    REPEAT( PRIMARY_KEY, n ) ;

string_func_3:
    REPLACE( PRIMARY_KEY, sample_string, sample_string ) ;
    SUBSTR( PRIMARY_KEY, lower_n, upper_n) ;

numerical_agg_exp:
    numerical_agg( extra_expression_a numerical_func_1 extra_expression_b ) |
    numerical_agg( extra_expression_a numerical_func_11 extra_expression_b ) ;

numerical_func_1:
    LENGTH( PRIMARY_KEY ) |
    LENGTH( string_func_1 ) |
    LENGTH( string_func_2 ) ;
    LENGTH( string_func_3 ) ;

numerical_func_11:
    LENGTH( PRIMARY_KEY ) |
    LENGTH( string_func_1 ) |
    LENGTH( string_func_2 ) |
    LENGTH( string_func_3 ) |
    func( extra_expression_a numerical_func_1 extra_expression_b ) ;

primary_group_by_expression:
    group_by_field |
    group_by_field, group_by_field |
    group_by_field, group_by_field, group_by_field ;

group_by_field:
    PRIMARY_KEY |
    string_func_1 |
    string_func_2 |
    string_func_3 |
    numerical_func_1 |
    numerical_func_11 ;

func:
    ABS |
    CEIL |
    COS |
    DEGREES |
    RADIANS |
    SIGN |
    SIN |
    TAN |
    FLOOR ;

numerical_agg:
    MIN |
    MAX |
    SUM |
    AVG |
    COUNT ;

n:
    0 |
    1 |
    2 |
    3 |
    4 |
    5 ;

lower_n:
    1 |
    2 |
    3 ;

upper_n:
    3 |
    4 |
    5 |
    6 ;

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

sample_string:
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

primary_condition:
	PRIMARY_KEY < string_values |
	PRIMARY_KEY > string_values |
	PRIMARY_KEY  >= string_values |
	PRIMARY_KEY  <= string_values |
	(primary_condition) AND (primary_condition) |
	(primary_condition) OR (primary_condition) |
	string_between_condition |
	string_not_between_condition |
	NOT (primary_condition) |
	string_is_not_null |
	string_is_null |
	string_not_equals_condition |
	string_in_condition |
	string_like_condition |
	string_not_like_condition |
	string_equals_condition ;

string_equals_condition:
	PRIMARY_KEY = string_values;

string_not_equals_condition:
	PRIMARY_KEY != string_values | PRIMARY_KEY <> string_values ;

string_between_condition:
	PRIMARY_KEY BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

string_not_between_condition:
	PRIMARY_KEY NOT BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

string_is_not_null:
	PRIMARY_KEY IS NOT NULL;

string_in_condition:
	PRIMARY_KEY IN ( string_field_list );

string_is_null:
	PRIMARY_KEY IS NULL;

string_like_condition:
	PRIMARY_KEY LIKE 'STRING_VALUES%' | PRIMARY_KEY LIKE '%STRING_VALUES' | PRIMARY_KEY LIKE STRING_VALUES | PRIMARY_KEY LIKE '%STRING_VALUES%';

string_not_like_condition:
	PRIMARY_KEY NOT LIKE 'STRING_VALUES%' | PRIMARY_KEY NOT LIKE '%STRING_VALUES' | PRIMARY_KEY NOT LIKE STRING_VALUES |  PRIMARY_KEY NOT LIKE '%STRING_VALUES%';

string_field_list:
	LIST;

string_is_missing:
	PRIMARY_KEY IS MISSING;

string_is_not_missing:
	PRIMARY_KEY IS NOT MISSING;

string_is_valued:
	PRIMARY_KEY IS VALUED;

string_is_not_valued:
	PRIMARY_KEY IS NOT VALUED;

string_values:
	STRING_VALUES;

