query:
 	select ;

select:
	SELECT select_from FROM BUCKET_NAME WHERE ANY v IN field_list SATISFIES v = 3 END; ;

create_index:
	CREATE INDEX INDEX_NAME ON BUCKET_NAME(DISTINCT ARRAY v FOR v in field_list END);

select_from:
	*  | field_list | DISTINCT(field);

field:
	NUMERIC_FIELD ;

numeric_field:
	NUMERIC_FIELD;


numeric_field_list:
	LIST;

field_list:
	NUMERIC_FIELD_LIST ;

