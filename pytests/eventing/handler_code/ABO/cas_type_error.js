function OnUpdate(doc, meta) {
    try{
    log("Doc created/updated", meta.id);
    var r1 = couchbase.insert(dst_bucket,{"id":meta.id},"{initial doc}");
    log(r1);
    if(r1.success){
        var r2 = couchbase.replace(dst_bucket,{"id":meta.id,"cas":1233},"replace");
        log(r2);
        }
    }catch(e){
        log("error:",e);
        if(e["message"]=="cas should be a string")
            var result = couchbase.insert(dst_bucket1,{"id": meta.id},doc);
    }
}