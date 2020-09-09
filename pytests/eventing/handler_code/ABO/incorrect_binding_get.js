function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    try{
    var r1 = couchbase.get(src_bucket1,doc);
    }catch(e){
        log("error get:",e);
        if(e=="ReferenceError: src_bucket1 is not defined"){
            try{
            var r2 = couchbase.insert(dst_bucket,meta,doc);
            log(r2);
            }catch(e1){
                log(e1);
            }
        }
    }
}

function OnDelete(meta, options) {
    log("Doc created/updated", meta.id);
    try{
    var r = couchbase.get(src_bucket,meta.id);
    }catch(e){
        log("error delete:",e);
        if(e["message"]=="2nd argument should be object"){
            var r1 = couchbase.get(src_bucket,meta);
            log(r1);
            if(r1.error.name == "LCB_KEY_ENOENT"){
                var r2 = couchbase.delete(dst_bucket,{"id":meta.id});
                log(r2);
            }
        }
    }
}
