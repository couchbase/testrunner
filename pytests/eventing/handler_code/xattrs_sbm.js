function OnUpdate(doc, meta, xattrs) {
    var meta = {"id": meta.id};
    var failed=false
    try{
    couchbase.mutateIn(src_bucket, meta, [couchbase.MutateInSpec.upsert("path",1 , { "xattrs": true })]);
    var res=couchbase.lookupIn(src_bucket, meta, [couchbase.LookupInSpec.get("path", { "xattrs": true })]);
    if (!res.doc[0].success){
        failed=true;
    }}
    catch (e) {
        failed=true
    }
    if (!failed){
        src_bucket[meta.id+"test"]="success"
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}