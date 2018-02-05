function OnUpdate(doc, meta) {
    var doc_id = meta.id;
    log('reading document from dst_bucket : ', doc_id);
    dst_bucket[meta.id + "11111111"] = dst_bucket[doc_id];
}