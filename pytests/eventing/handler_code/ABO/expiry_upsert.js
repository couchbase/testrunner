function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 180);
    var meta_data={"id": meta.id,"expiry_date": expiry };
    var result = couchbase.upsert(dst_bucket,meta_data,doc);
    log(result);
}
