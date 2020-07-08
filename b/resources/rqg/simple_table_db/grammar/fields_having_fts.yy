query:
 	select ;

select:
	SELECT GROUPBY_FIELDS FROM BUCKET_NAME WHERE complex_condition GROUP BY NUMERIC_FIELD_LIST HAVING numeric_condition |
	SELECT GROUPBY_FIELDS FROM BUCKET_NAME WHERE complex_condition GROUP BY STRING_FIELD_LIST HAVING string_condition  |
	SELECT GROUPBY_FIELDS FROM BUCKET_NAME WHERE complex_condition GROUP BY NUMERIC_FIELD_LIST,STRING_FIELD_LIST HAVING (string_condition) AND  (numeric_condition);
	SELECT numeric_aggregate_method  FROM BUCKET_NAME WHERE complex_condition GROUP BY NUMERIC_FIELD_LIST HAVING numeric_condition |
	SELECT generic_aggregate_method  FROM BUCKET_NAME WHERE complex_condition GROUP BY NUMERIC_FIELD_LIST HAVING numeric_condition ;
	SELECT numeric_aggregate_method  FROM BUCKET_NAME WHERE complex_condition GROUP BY STRING_FIELD_LIST HAVING numeric_condition |
	SELECT generic_aggregate_method  FROM BUCKET_NAME WHERE complex_condition GROUP BY STRING_FIELD_LIST HAVING string_condition ;

numeric_aggregate_method:
	COUNT(*) AS AGGREGATE_FIELD | COUNT(FIELD) AS AGGREGATE_FIELD | aggregate_function(FIELD) AS AGGREGATE_FIELD | SUM(FIELD) AS AGGREGATE_FIELD | numeric_aggregate_method, numeric_aggregate_method ;

aggregate_function:
    AVG | STDDEV | VARIANCE | STDDEV_SAMP | STDDEV_POP | VARIANCE_POP | VARIANCE_SAMP | MEAN ;

generic_aggregate_method:
	MAX(FIELD) AS AGGREGATE_FIELD | MIN(FIELD) AS AGGREGATE_FIELD | generic_aggregate_method, generic_aggregate_method ;

direction:
	ASC | DESC;


complex_condition:
	(condition) AND (condition) | (condition) OR (condition) | condition;

condition:
	numeric_condition | string_condition |(string_condition AND numeric_condition) |
	(numeric_condition OR string_condition);

field:
	NUMERIC_FIELD | STRING_FIELD;

non_string_field:
	NUMERIC_FIELD;

# NUMERIC RULES

numeric_condition:
    numeric_equals_condition |
    numeric_closed_range |
	numeric_between_condition;

numeric_equals_condition:
	numeric_field = numeric_value ;

numeric_between_condition:
	NUMERIC_FIELD BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

numeric_closed_range:
    CLOSED_RANGE_NUMERIC_FIELD > LOWER_BOUND_VALUE and SAME_CLOSED_RANGE_NUMERIC_FIELD < UPPER_BOUND_VALUE |
    CLOSED_RANGE_NUMERIC_FIELD >= LOWER_BOUND_VALUE and SAME_CLOSED_RANGE_NUMERIC_FIELD <= UPPER_BOUND_VALUE |
    CLOSED_RANGE_NUMERIC_FIELD >= LOWER_BOUND_VALUE and SAME_CLOSED_RANGE_NUMERIC_FIELD < UPPER_BOUND_VALUE |
    CLOSED_RANGE_NUMERIC_FIELD > LOWER_BOUND_VALUE and SAME_CLOSED_RANGE_NUMERIC_FIELD <= UPPER_BOUND_VALUE;

numeric_field_list:
	LIST;

numeric_field:
	NUMERIC_FIELD;

numeric_value:
	NUMERIC_VALUE;

# STRING RULES

string_condition:
	string_like_condition |
	string_equals_condition;

string_equals_condition:
	string_field = string_values;

string_between_condition:
	string_field BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE;

string_like_condition:
	string_field LIKE 'STRING_VALUES%' | string_field LIKE STRING_VALUES;

string_field_list:
	LIST;

string_field:
	STRING_FIELD;

string_values:
	STRING_VALUES;


field_list:
	NUMERIC_FIELD_LIST | STRING_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST | NUMERIC_FIELD_LIST, STRING_FIELD_LIST;
