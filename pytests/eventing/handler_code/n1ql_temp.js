function OnUpdate(doc, meta) {
    var query=$n1ql
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