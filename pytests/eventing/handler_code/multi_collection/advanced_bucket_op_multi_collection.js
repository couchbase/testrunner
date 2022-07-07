function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var doc_meta = {"id":meta.id,
    "keyspace": {"scope_name":meta.keyspace.scope_name,
    "collection_name":meta.keyspace.collection_name}};
    var result = couchbase.insert(dst_bucket, doc_meta, doc);
    log(result);

}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var doc_meta = {"id":meta.id,
    "keyspace": {"scope_name":meta.keyspace.scope_name,
    "collection_name":meta.keyspace.collection_name}};
    var result = couchbase.delete(dst_bucket, doc_meta);
    log(result);
}