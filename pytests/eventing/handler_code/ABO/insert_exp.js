function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    try{
    var result = couchbase.get(src_bucket,meta);
    log(result);
    if(result.success && result.meta.cas != undefined && result.meta.expiry_date != undefined &&
        JSON.stringify(result.doc) === JSON.stringify(doc)){
            var result1= couchbase.insert(dst_bucket,meta,doc);
    }
    log(result1);
    }catch(e){
        log("error:",e);
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var doc={"id":meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
}
