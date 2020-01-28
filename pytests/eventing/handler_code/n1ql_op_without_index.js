function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket where mutated = 0;
        for(var row of query){
        }
    } catch (e) {
        log(e);
        dst_bucket[meta.id]="caught the error";
    }
}
function OnDelete(meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket where mutated = 0;
        for(var row of query){
        }
    } catch (e) {
        log(e);
        delete dst_bucket[meta.id];
    }
}