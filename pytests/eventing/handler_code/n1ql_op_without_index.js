function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket where mutated = 0;
        for(var row of query){
        }
    } catch (e) {
        log(e);
        var obj=JSON.parse(e["message"]);
        if(obj["errors"][0]["code"]==4000){
            dst_bucket[meta.id]=JSON.parse(e["message"]);
        }
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
        var obj=JSON.parse(e["message"]);
        if(obj["errors"][0]["code"]==4000){
            delete dst_bucket[meta.id];
        }
    }
}