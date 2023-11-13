function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 50);

    var req = {"id": meta.id, "expiry_date": expiry};
    couchbase.touch(src_bucket, req);
}
