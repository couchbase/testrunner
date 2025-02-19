function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var result1= couchbase.insert(dst_bucket,meta,doc);
    log(result1);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var doc={"id":meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
}

function OnDeploy(action) {
    var randomId = "doc_" + Math.random().toString();
    var meta = { id: randomId };
    dst_bucket[meta.id] = 'adding docs';
    delete dst_bucket[meta.id];
    log("Operation complete for doc with id:", meta.id);
}