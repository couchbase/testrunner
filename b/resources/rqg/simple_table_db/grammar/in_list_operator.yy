query:
 	select ;

select:
	SELECT GROUPBY_FIELDS FROM BUCKET_NAME WHERE complex_condition GROUP BY NUMERIC_FIELD_LIST HAVING numeric_condition |
	SELECT GROUPBY_FIELDS FROM BUCKET_NAME WHERE complex_condition GROUP BY STRING_FIELD_LIST HAVING string_condition  |
	SELECT GROUPBY_FIELDS FROM BUCKET_NAME WHERE complex_condition GROUP BY NUMERIC_FIELD_LIST,STRING_FIELD_LIST HAVING (string_condition) AND  (numeric_condition);

complex_condition:
	condition ;

and_or:
    AND | OR ;

condition:
	numeric_condition | string_condition | (string_condition AND numeric_condition) | (numeric_condition OR string_condition) ;

field:
	NUMERIC_FIELD | STRING_FIELD;

non_string_field:
	NUMERIC_FIELD;

numeric_in_conidtion:
	numeric_field in_not_in ( numeric_field_list );

numeric_field_list:
	LIST;

string_field_list:
	LIST;

numeric_field:
	NUMERIC_FIELD;

numeric_condition:
	numeric_in_conidtion ;

string_condition:
	string_in_conidtion ;

string_in_conidtion:
	string_field in_not_in ( string_field_list );

in_not_in:
    IN | NOT IN ;

string_field:
	STRING_FIELD;

string_values:
	STRING_VALUES;

field_list:
	NUMERIC_FIELD_LIST | STRING_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST ;


