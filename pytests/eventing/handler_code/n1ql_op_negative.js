function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket1 limit 1;
        for(var row of query){
        }
    } catch (e) {
        var obj=e["message"];
        log("errors:",obj);
        dst_bucket[meta.id]="caught the error";
    }
}
function OnDelete(meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket1 limit 1;
        for(var row of query){
        }
    } catch (e) {
        var obj=e["message"];
        log("errors:",obj);
        delete dst_bucket[meta.id];
    }
}