function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var result = couchbase.insert(dst_bucket,meta,doc);
    log(result);
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 180);
    var meta_data={"id": meta.id,"expiry_date": expiry,"cas":result.meta.cas};
    var result1 = couchbase.replace(dst_bucket,meta_data,doc);
    log(result1);
}
