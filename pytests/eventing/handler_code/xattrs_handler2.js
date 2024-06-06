function OnUpdate(doc, meta) {
    var meta = {"id": meta.id};
    couchbase.insert(dst_bucket, meta, doc);
    var failed=false
    try{
    couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.upsert("path",1 , { "xattrs": true })]);
    }
    catch (e) {
        failed=true
    }
    if (!failed){
        dst_bucket[meta.id+"11111"]="success"
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}