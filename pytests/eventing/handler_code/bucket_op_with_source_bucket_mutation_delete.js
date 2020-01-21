function OnUpdate(doc, meta) {
    log('document', doc);
    try {
        src_bucket[meta.id + "_sbm"] = doc;
    } catch(e) {
        log(e);
    }
}

function OnDelete(meta){
    try {
        delete src_bucket[meta.id + "_sbm"];
    } catch(e) {
        log(e);
    }
}
