function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var meta_data={"id": meta.id};
    var result1 = couchbase.replace(dst_bucket,meta,doc);
    log(result1);
    if(!result1.success && result1.error.key_not_found && result1.error.name == "LCB_KEY_ENOENT"){
        couchbase.insert(dst_bucket,meta_data,doc);
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var result = couchbase.get(dst_bucket,meta);
    log(result);
    if(result.success && result.meta.cas != undefined){
            var meta_data = {"id": meta.id,"cas":"12324"};
            var result1 = couchbase.replace(dst_bucket,meta_data,"replace");
            log(result1);
            if(!result1.success && result1.error.cas_mismatch && result1.error.name == "LCB_KEY_EEXISTS"){
                var meta_del={"id":meta.id}
                couchbase.delete(dst_bucket,meta_del);
            }
    }
}