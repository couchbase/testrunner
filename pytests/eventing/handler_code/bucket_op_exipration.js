function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
    var query=select Raw(meta().expiration) from src_bucket where meta().id=$meta.id;
    for(var row of query){
        log(row);
        var expiration=row;
    }
    if (meta.expiration == expiration){
        dst_bucket[meta.id]=meta.expiration;
    }
    }catch(e){
        log(e);
    }
}

function OnDelete(meta) {
    try{
    log(meta.expiration);
    if (meta.expiration != undefined){
        delete dst_bucket[meta.id];
    }
    }catch(e){
        log(e);
    }
}
