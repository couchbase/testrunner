function OnUpdate(doc, meta) {
    log('docId:', doc);
    dst_bucket[meta.id]=doc;
}
function OnDelete(meta) {
}