function OnUpdate(doc, meta) {
    log("meta:", JSON.stringify(meta));
    dst_bucket[meta.id] = doc;
}
function OnDelete(meta, options) {
}
