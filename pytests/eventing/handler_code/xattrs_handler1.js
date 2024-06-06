function OnUpdate(doc, meta, xattrs) {
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
    var doc={"id":meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    doc={"id":meta.id +"11111"}
    var result = couchbase.delete(dst_bucket,doc);
}