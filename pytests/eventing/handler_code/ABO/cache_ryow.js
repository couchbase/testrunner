function OnUpdate(doc, meta) {
    var result= couchbase.insert(dst_bucket,meta,doc);
    log(result);
    //cache the document
    var a=couchbase.get(dst_bucket, {id: meta.id}, {"cache": true});
    //compare cached document with document in kv bucket
    var b=couchbase.get(dst_bucket, {id: meta.id}, {"cache": true});
    //modify the document in bucket
    var result1= couchbase.upsert(dst_bucket,meta,{'hello': 'world'});
    log(result1);
    var c=couchbase.get(dst_bucket, {id: meta.id}, {"cache": true});
    var d=couchbase.get(dst_bucket, {id: meta.id});
    //read your own write phenomenon document will get updated in bucket as well as in cache
    if(JSON.stringify(c)==JSON.stringify(d))
    {
        var result2= couchbase.insert(dst_bucket1,meta,{'hello': 'world'});
        log(result2);
    }
}

function OnDelete(meta, options) {
    var doc={"id":meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
    var result1 = couchbase.delete(dst_bucket1,doc);
    log(result1);
}