function OnUpdate(doc, meta) {
    try{
    log("Doc created/updated", meta.id);
    var r1 = couchbase.upsert(dst_bucket,meta,["ad"]);
    log(r1);
    if(r1.success){
            var r3 = couchbase.upsert(dst_bucket,meta,null);
            log(r3);
            if(r3.success){
                var r4 = couchbase.upsert(dst_bucket,meta,true);
                log(r4);
                if(r4.success){
                    var r5 = couchbase.upsert(dst_bucket,meta,123456);
                    log(r5);
                    if(r5.success){
                        var r6 = couchbase.upsert(dst_bucket,meta,new Object());
                        log(r6);
			            couchbase.upsert(dst_bucket1,meta,{"upset done": true});
                    }
                }
            }
    }
    }catch(e){
        log(e);
    }
}