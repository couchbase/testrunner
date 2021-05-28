function OnUpdate(doc, meta) {
    dst_bucket1[meta.id] = doc;
}
function OnDelete(meta) {
    delete dst_bucket1[meta.id];
}