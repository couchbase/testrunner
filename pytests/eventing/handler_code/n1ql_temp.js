function OnUpdate(doc, meta) {
    try{
        var query=$n1ql
        //query.execQuery();
        for(var raw of query){
        }
        var key = Date.now()+Math.random();
        dst_bucket[key]={'passed_query' : query};
    }
    catch(e){
        var key = Date.now()+Math.random();
        dst_bucket[key]={'failed_query' : query};
    }
}