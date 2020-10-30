function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    if(options.expired){
        var doc={"id":meta.id}
        var result = couchbase.insert(dst_bucket,meta,"expired");
        log(result);
    }
}
