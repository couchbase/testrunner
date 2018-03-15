query:
 	select ;

select:
	SELECT select_from FROM BUCKET_NAME WHERE complex_condition ORDER BY field ;

select_from:
	*  | field | DISTINCT( field );

complex_condition:
	NOT ( condition ) | ( condition ) AND ( condition ) |
	( condition) OR ( condition ) |
	( condition ) AND ( condition ) OR ( condition ) AND ( condition ) |
	condition;

condition:
	( string_condition )  |
	( bool_condition )  ;

logical_operator:
	AND | OR  ;

field:
	NUMERIC_FIELD | STRING_FIELD | BOOL_FIELD;

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
    string_not_like_condition |
    string_length_condition |
    string_contains_condition ;
    
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
	string_field IS MISSING;

string_is_not_missing:
	string_field IS NOT MISSING;

string_is_valued:
	string_field IS VALUED;

string_is_not_valued:
	string_field IS NOT VALUED;

string_length_condition:
	LENGTH ( string_field ) > digit | LENGTH ( string_field ) < digit | LENGTH ( string_field ) = digit;
	
string_contains_condition:
	CONTAINS ( string_field , string_values ) ;

string_position_condition:
    POSITION ( string_field , STRING_VALUES ) > digit | POSITION ( string_field , STRING_VALUES ) < digit | POSITION ( string_field , STRING_VALUES ) = digit ;

digit:
	2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;

string_field:
	STRING_FIELD;

string_values:
	STRING_VALUES |  string_function( STRING_VALUES ) | SUBSTR( STRING_VALUES, SUBSTR_INDEX ) | CONCAT( STRING_VALUES , characters ) | REPLACE( STRING_VALUES , characters , characters  ) ;

string_function:
	UPPER | LOWER | LTRIM |  RTRIM | TRIM ;

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