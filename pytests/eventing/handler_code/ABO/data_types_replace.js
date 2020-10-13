function OnUpdate(doc, meta) {
    try{
    log("Doc created/updated", meta.id);
    var r1 = couchbase.insert(dst_bucket,{"id":meta.id},["ad"]);
    log(r1);
    if(r1.success){
            var r3 = couchbase.replace(dst_bucket,{"id":meta.id,"cas":r1.meta.cas},null);
            log(r3);
            if(r3.success){
                var r4 = couchbase.replace(dst_bucket,{"id":meta.id,"cas":r3.meta.cas},true);
                log(r4);
                if(r4.success){
                    var r5 = couchbase.replace(dst_bucket,{"id":meta.id,"cas":r4.meta.cas},123456);
                    log(r5);
                    if(r5.success){
                        var r6 = couchbase.replace(dst_bucket,{"id":meta.id,"cas":r5.meta.cas},new Object());
                        log(r6);
                        couchbase.insert(dst_bucket1,{"id":meta.id},"data types");
                    }
                }
            }
    }
    }catch(e){
        log(e);
    }
}