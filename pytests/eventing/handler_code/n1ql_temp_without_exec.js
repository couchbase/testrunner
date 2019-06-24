function OnUpdate(doc, meta) {
    try{
        var query=$n1ql
        var key = Date.now()+Math.random();
        dst_bucket[key]={'passed_query' : '$n1ql'};
    }
    catch(e){
        var key = Date.now()+Math.random();
        dst_bucket[key]={'failed_query' : '$n1ql'};
    }
}