function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var result = couchbase.increment(src_bucket,{"id":"counter"});
    log(result);
    if(!result.success && result.error.name=="LCB_DELTA_BADVAL")
        var result1 = couchbase.insert(dst_bucket,meta,doc);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var result = couchbase.decrement(src_bucket,{"id":"counter"});
    log(result);
    if(!result.success && result.error.name=="LCB_DELTA_BADVAL")
        var result1 = couchbase.delete(dst_bucket,{"id":meta.id});

}
