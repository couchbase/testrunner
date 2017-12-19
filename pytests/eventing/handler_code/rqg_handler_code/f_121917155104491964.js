function OnUpdate(doc, meta) {
    var query=SELECT * FROM src_bucket WHERE (BOOL_FIELD = false) AND ((STRING_FIELD LIKE STRING_VALUES AND NUMERIC_FIELD > NUMERIC_VALUE)) OR ((NUMERIC_FIELD = NUMERIC_VALUE AND STRING_FIELD != STRING_VALUES AND NOT (BOOL_FIELD))) AND ((BOOL_FIELD = false AND NUMERIC_FIELD IS NOT NULL));

    try{
    query.execQuery();
    var key = Date.now();
    dst_bucket[key]={'passed_query' : query};
    }
    catch(e){
    var key = Date.now();
    dst_bucket[key]={'failed_query' : query};
    }
}