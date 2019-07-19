function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket1 limit 1;
        for(var row of query){
        }
    } catch (e) {
        log(e);
        if(e["message"] == "N1QL: Unable to schedule N1QL query"){
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
         if(e["message"] == "N1QL: Unable to schedule N1QL query"){
            delete dst_bucket[meta.id];
        }
    }
}