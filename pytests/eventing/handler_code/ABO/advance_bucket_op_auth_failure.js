function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var result1= couchbase.insert(dst_bucket,meta,doc);
    log(result1);

}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    try {
        var result = couchbase.get(dst_bucket,meta);
    } catch (e) {
        log(e);
        if(e.message.name == "LCB_ERR_AUTHENTICATION_FAILURE"){
            var doc={"id":meta.id}
            var result = couchbase.delete(dst_bucket,doc);
            log(result);
        }
    }
}