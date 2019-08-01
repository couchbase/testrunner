function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket1 limit 1;
        for(var row of query){
        }
    } catch (e) {
        log(e);
        if(e["message"] == "Unable to schedule query : Operation not supported\n"){
            dst_bucket[meta.id]=e;
        }
    }
}
function OnDelete(meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket1 limit 1;
        for(var row of query){
        }
    } catch (e) {
        log(e);
         if(e["message"] == "Unable to schedule query : Operation not supported\n"){
            delete dst_bucket[meta.id];
        }
    }
}