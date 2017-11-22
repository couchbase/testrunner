function OnUpdate(doc, meta) {
    var doc_id = meta.id; // GET operation
    log('creating document for : ', doc);
    // Use the first alias for doc create
    dst_bucket[doc_id] = {'doc_id' : doc_id}; // SET operation
}

function OnDelete(meta) {
    log('deleting document', meta.id);
    // Use the second alias for doc delete
    delete dst_bucket1[meta.id]; // DELETE operation
}