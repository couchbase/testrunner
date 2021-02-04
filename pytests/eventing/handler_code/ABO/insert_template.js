function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    try{
        var id={"id":'$key'+'_'+meta.id}
        var result1= couchbase.insert(dst_bucket,id,doc);
        log(result1);
    }catch(e){
        log("error:",e);
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var doc={"id":'$key'+'_'+meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
}
