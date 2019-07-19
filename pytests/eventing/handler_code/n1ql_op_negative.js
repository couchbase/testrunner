function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try {
        var query=select * from src_bucket1 limit 1;
        for(var row of query){
        }
    } catch (e) {
        var obj=JSON.parse(e["message"]);
        if(obj["errors"][0]["code"]==12003){
            dst_bucket[meta.id]=JSON.parse(e["message"]);
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
        var obj=JSON.parse(e["message"]);
        if(obj["errors"][0]["code"]==12003){
            delete dst_bucket[meta.id];
        }
    }
}