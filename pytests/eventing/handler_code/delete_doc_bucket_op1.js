function OnUpdate(doc, meta) {
    var doc_id = meta.id;
    log('creating document for : ', doc);
    dst_bucket[doc_id] = doc; // SET operation
}
function OnDelete(meta) {
    log('deleting document', meta.id);
    delete dst_bucket[meta.id]; // DELETE operation
}