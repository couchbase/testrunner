function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var success = false;
    while(!success){
        try{
        var result= couchbase.insert(dst_bucket,meta,doc);
        log(result);
        success=result.success;
        if(result.error != undefined && result.error.name == "LCB_KEY_EEXISTS"){
            break;
            }
        }catch(e){
        log("error:",e);
        }
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var doc={"id":meta.id}
    var success = false;
    while(!success){
    try{
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
    success=result.success;
    if(result.error != undefined && result.error.name == "LCB_KEY_ENOENT"){
            break;
            }
    }catch(e){
        log("error:",e);
        }
    }
}
