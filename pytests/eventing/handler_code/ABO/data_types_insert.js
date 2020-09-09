function OnUpdate(doc, meta) {
    try{
    log("Doc created/updated", meta.id);
    var r1 = couchbase.insert(dst_bucket,{"id":meta.id+"_arr"},["ad"]);
    log(r1);
    if(r1.success){
        var r2 = couchbase.insert(dst_bucket,{"id":meta.id+"_un"},undefined);
        log(r2);
        if(r2.success){
            var r3 = couchbase.insert(dst_bucket,{"id":meta.id+"_null"},null);
            log(r3);
            if(r3.success){
                var r4 = couchbase.insert(dst_bucket,{"id":meta.id+"_boo"},true);
                log(r4);
                if(r4.success){
                    var r5 = couchbase.insert(dst_bucket,{"id":meta.id+"_int"},123456);
                    log(r5);
                    if(r5.success){
                        var r6 = couchbase.insert(dst_bucket,{"id":meta.id+"_obj"},new Object());
                        log(r6);
                        if(r6.success){
                            var x = (-5 >>> 0);
                            var r7 = couchbase.insert(dst_bucket,{"id":meta.id+"_bin"},x);
                            log(r7);
                        }
                    }
                }
            }
        }
    }
    }catch(e){
        log(e);
    }
}
