function OnUpdate(doc, meta) {
    var result= couchbase.insert(dst_bucket,meta,doc);
    log(result);
    //cache the document
    var a=couchbase.get(dst_bucket, {id: meta.id}, {"cache": true});
    //compare cached document with document in kv bucket
    var b=couchbase.get(dst_bucket, {id: meta.id}, {"cache": true});
    var c=couchbase.get(dst_bucket, {id: meta.id});
    if(JSON.stringify(b['doc'])==JSON.stringify(c['doc']))
    {
        var result1= couchbase.insert(dst_bucket1,meta,doc);
        log(result1);
    }
}

function OnDelete(meta, options) {
    var doc={"id":meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
    var result1 = couchbase.delete(dst_bucket1,doc);
    log(result1);
}