function OnUpdate(doc, meta) {
    log('document', doc);
    dst_bucket[meta.id + "11111111"] = doc;
}
function OnDelete(meta) {
    delete dst_bucket[meta.id + "11111111"];
}