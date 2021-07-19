function OnUpdate(doc, meta) {
    var value = {'big number': '1'.repeat(10*1024*1024), 'overflow': false};
    for(var i=1;i<=8;i++)
    {
        var result= couchbase.insert(dst_bucket, {id: meta.id + i.toString()}, value);
        log(result);
        couchbase.get(dst_bucket, {id: meta.id + i.toString()}, {"cache": true});
    }
    var query=UPDATE dst_bucket SET overflow = true;
    log(query,meta.id);
    var check = false;
    for(var i=1;i<=8;i++)
    {
        var a=couchbase.get(dst_bucket, {id: meta.id + i.toString()}, {"cache": true});
        log(a.doc.overflow);
        if(a.doc.overflow == true)
        {
            check=true;
            break;
        }
    }
    if(check)
    {
        var result1= couchbase.insert(dst_bucket1,meta,doc);
        log(result1);
    }


}

function OnDelete(meta, options) {
    for(var i=1;i<=8;i++)
    {
        var result = couchbase.delete(dst_bucket,{id:meta.id + i.toString()});
        log(result);
    }
    var doc={"id":meta.id}
    var result1 = couchbase.delete(dst_bucket1,doc);
    log(result1);
}