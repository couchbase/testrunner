query:
 	select ;

select:
	SELECT * FROM BUCKET_NAME WHERE complex_condition ORDER BY field direction |
	SELECT select_from FROM BUCKET_NAME WHERE complex_condition ORDER BY ORDER_BY_SEL_VAL direction ;

create_index:
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(field direction) WHERE complex_condition ;

direction:
	ASC | DESC | ASC NULLS FIRST | DESC NULLS LAST ;

select_from:
	field | DISTINCT( field );

complex_condition:
	NOT ( condition ) | ( condition ) AND ( condition ) |
	( condition) OR ( condition ) |
	( condition ) AND ( condition ) OR ( condition ) AND ( condition ) |
	condition;

condition:
	( numeric_condition ) |
	( string_condition )  |
	( bool_condition )  |
	( datetime_condition ) |
	NOT ( condition ) |
	( condition ) logical_operator ( condition ) ;


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
	NUMERIC_VALUE | TRUNCATE( 1000 * maths_operator( NUMERIC_VALUE ), 0 ) | POWER( NUMERIC_VALUE , 2 ) | ( NUMERIC_VALUE numeric_operator digit ) ;

maths_operator:
	SIN | LN | TAN | COS | SQRT | TAN | FLOOR | RADIANS | ABS | CEIL | FLOOR;

abs_maths_operator:
	 ASIN | ACOS | ATAN ;

numeric_operator:
	+ | - | * | / | %;

digit:
	2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | ROUND(PI());

# DATETIME RULES

datetime_condition:
	datetime_field < datetime_values |
	datetime_field > datetime_values |
	datetime_field  >= datetime_values |
	datetime_field  <= datetime_values |
	( datetime_condition ) AND ( datetime_condition ) |
	( datetime_condition ) OR ( datetime_condition ) |
	datetime_not_between_condition |
	NOT ( datetime_condition ) |
	datetime_not_equals_condition |
	datetime_in_conidtion |
	datetime_equals_condition ;

datetime_equals_condition:
	datetime_field = datetime_values;

datetime_not_equals_condition:
	datetime_field != datetime_values | datetime_field <> datetime_values ;

datetime_between_condition:
	datetime_field BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

datetime_not_between_condition:
	datetime_field NOT BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

datetime_in_conidtion:
	datetime_field IN ( datetime_field_list );

datetime_field_list:
	DATETIME_LIST;

datetime_field:
	DATETIME_FIELD;

datetime_values:
	DATETIME_VALUE |
	DATETIME_VALUE_ADD_HOUR |
	DATETIME_VALUE_ADD_SECOND |
	DATETIME_VALUE_ADD_MINUTE |
	DATETIME_VALUE_ADD_DAY |
	DATETIME_VALUE_ADD_MONTH |
	DATETIME_VALUE_SUB_YEAR |
	DATETIME_VALUE_SUB_HOUR |
	DATETIME_VALUE_SUB_SECOND |
	DATETIME_VALUE_SUB_MINUTE |
	DATETIME_VALUE_SUB_DAY |
	DATETIME_VALUE_SUB_MONTH |
	DATETIME_VALUE_SUB_YEAR ;

# STRING RULES

string_condition:
	string_field < string_values |
	string_field > string_values |
	string_field  >= string_values |
	string_field  <= string_values |
	( string_condition ) AND ( string_condition ) |
	( string_condition ) OR ( string_condition ) |
	string_not_between_condition |
	NOT ( string_condition ) |
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
	STRING_VALUES |  string_function( STRING_VALUES ) | SUBSTR( STRING_VALUES, SUBSTR_INDEX ) | CONCAT( STRING_VALUES , characters ) | REPLACE( STRING_VALUES , characters , characters  ) ;

string_function:
	UPPER | LOWER | LTRIM |  RTRIM | TRIM   ;

characters:
	"a" | "b" | "c" | "c" | "d" ;

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