query:
 	select ;

select:
        SELECT select_from FROM BUCKET_NAME WHERE complex_condition ORDER BY NUMERIC_FIELD |
        SELECT select_from FROM BUCKET_NAME WHERE complex_condition ORDER BY STRING_FIELD |
        SELECT select_from FROM BUCKET_NAME WHERE complex_condition ORDER BY BOOL_FIELD , NUMERIC_FIELD ;


select_from:
	*  | field | DISTINCT( field );

complex_condition:
	NOT ( condition ) | ( condition ) AND ( condition ) |
	( condition) OR ( condition ) |
	( condition ) AND ( condition ) OR ( condition ) AND ( condition ) |
	condition;

condition:
	( numeric_condition )  |
	( bool_condition )  ;


logical_operator:
	AND | OR  ;

field:
	NUMERIC_FIELD | STRING_FIELD | BOOL_FIELD;

# NUMERIC RULES

numeric_condition:
	numeric_field < numeric_value |
	numeric_field = numeric_value |
	numeric_field > numeric_value |
	numeric_field  >= numeric_value |
	numeric_field  <= numeric_value |
	( numeric_condition ) AND ( numeric_condition ) |
	( numeric_condition ) OR ( numeric_condition )|
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
	TRUNCATE( 1000 * maths_operator( NUMERIC_VALUE ), 0 ) | POWER( NUMERIC_VALUE , 2 ) | ( NUMERIC_VALUE numeric_operator digit ) ;

maths_operator:
	SIN | LN | LOG | ROUND | SIGN | SQRT | TAN | COS | SQRT | TAN | FLOOR | ABS | CEIL | FLOOR | ASIN | ACOS | ATAN ;

abs_maths_operator:
	 ASIN | ACOS | ATAN ;

number:
    -1 | -0.8 | -0.6 | -0.4 | -0.2 | 0.2 | 0.4 | 0.6 | 0.8 | 1 ;

numeric_operator:
	+ | - | * | / | %;

digit:
	2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;

# BOOLEAN RULES

bool_condition:
	bool_field |
	NOT ( bool_field ) |
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