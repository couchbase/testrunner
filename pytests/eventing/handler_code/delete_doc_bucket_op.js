function OnUpdate(doc, meta) {
    var doc_id = meta.id;
    log("creating document for : ", doc);
    dst_bucket[doc_id] = {"doc_id" : doc_id}; // SET operation
    var val = dst_bucket[meta.id];
    // Explicit validation for GET
    if (val === null || val === undefined) {
       throw new Error("GET failed: document not found for key " + meta.id);
    }
    log("GET operation successful for key:", meta.id);
}

// This is intentionally left blank


























function OnDelete(meta) {
    log("deleting document", meta.id);
    delete dst_bucket[meta.id]; // DELETE operation
}