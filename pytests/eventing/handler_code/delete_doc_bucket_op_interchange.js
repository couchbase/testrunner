function OnUpdate(doc, meta) {
    var doc_id = meta.id;
    log('creating document for : ', doc);
    src_bucket[doc_id+"_1"] = {'doc_id' : doc_id}; // SET operation
}

// This is intentionally left blank


























function OnDelete(meta) {
    log('deleting document', meta.id);
    delete src_bucket[meta.id+"_1"]; // DELETE operation
}