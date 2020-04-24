function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
    var query=select Raw(meta().cas) from src_bucket where meta().id=$meta.id;
    for(var row of query){
        log(row);
        var cas=row;
    }
    if (meta.cas == cas){
        dst_bucket[meta.id]=meta.cas;
    }
    }catch(e){
        log(e);
    }
}

function OnDelete(meta) {
    try{
    log(meta.cas);
    if (meta.cas != undefined){
        delete dst_bucket[meta.id];
    }
    }catch(e){
        log(e);
    }
}