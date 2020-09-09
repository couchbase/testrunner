function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    try{
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 180);
    var meta_data={"id": meta.id,"expiry_date": 123 };
    var result = couchbase.insert(dst_bucket,meta_data,doc);
    log(result);
    }catch(e){
        log("error:",e);
        if(e["message"]=="expiry should be a data object")
            meta_data={"id": meta.id,"expiry_date": expiry };
            couchbase.insert(dst_bucket,meta_data,doc);
            couchbase.insert(dst_bucket1,meta_data,doc);
    }
}