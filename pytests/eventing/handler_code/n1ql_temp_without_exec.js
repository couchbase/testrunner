function OnUpdate(doc, meta) {
    try{
        var query=$n1ql
        var key = Date.now()+Math.random();
        dst_bucket[key]={'passed_query' : query};
    }
    catch(e){
        var key = Date.now()+Math.random();
        dst_bucket[key]={'failed_query' : query};
    }
}