function OnUpdate(doc, meta) {
    var doc_id = meta.id;
    log('creating document for : ', doc);
    while(true){
        try{
           dst_bucket[doc_id] = {'doc_id' : doc_id}; // SET operation
           break;
        }catch(e){
            log(e);
        }
    }
}

// This is intentionally left blank


























function OnDelete(meta) {
    log('deleting document', meta.id);
    while(true){
        try{
           delete dst_bucket[meta.id]; // DELETE operation
           break;
        }catch(e){
            log(e);
        }
    }
}