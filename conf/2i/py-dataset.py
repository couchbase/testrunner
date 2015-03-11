2i.indexscans_2i.SecondaryIndexingScanTests:
# Basic Tests with varying buckets, create index, explain query, query, drop
	test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:equals:orderby:range,dataset=default,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=1000
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:equals:orderby:range,dataset=sabre,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:notequals:range,dataset=sabre,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:greater_than:no_orderby_groupby:range,dataset=sabre,use_gsi_for_primary=True,use_gsi_for_secondary=True,,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=single,dataset=sabre,use_gsi_for_primary=True,use_gsi_for_secondary=True,,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=composite:and:orderby:range,dataset=sabre,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:full,dataset=big_data,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:range,dataset=big_data,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:full,dataset=big_data,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200,value_size=10000
    test_multi_create_query_explain_drop_index_with_index_where_clause,groups=simple:range,dataset=big_data,use_gsi_for_primary=True,use_gsi_for_secondary=True,doc-per-day=200,value_size=10000