function OnUpdate(doc, meta) {
    var value = {'big number': '1'.repeat(10*1024*1024), 'underflow': false};
    for(var i=1;i<=6;i++)
    {
        var result= couchbase.insert(dst_bucket, {id: meta.id + i.toString()}, value);
        log(result);
        couchbase.get(dst_bucket, {id: meta.id + i.toString()}, {"cache": true});
    }
    var query=UPDATE default.scope0.collection1 SET underflow = true;
    log(query,meta.id);
    var check = false;
    for(var i=1;i<=6;i++)
    {
        var a=couchbase.get(dst_bucket, {id: meta.id + i.toString()}, {"cache": true});
        log(a.doc.underflow);
        if(a.doc.underflow == true)
        {
            check=true;
            break;
        }
    }
    if(!check)
    {
        var result1= couchbase.insert(dst_bucket1,meta,doc);
        log(result1);
    }


}

function OnDelete(meta, options) {
    for(var i=1;i<=6;i++)
    {
        var result = couchbase.delete(dst_bucket,{id:meta.id + i.toString()});
        log(result);
    }
    var doc={"id":meta.id}
    var result1 = couchbase.delete(dst_bucket1,doc);
    log(result1);
}