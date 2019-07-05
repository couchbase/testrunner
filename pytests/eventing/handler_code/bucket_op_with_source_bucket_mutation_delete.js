function OnUpdate(doc, meta) {
    log('document', doc);
    while(true){
    try {
        src_bucket[meta.id + "_sbm"] = doc;
        break;
    } catch(e) {
        log(e);
    }
    }
}

function OnDelete(meta){
    while(true){
    try {
        delete src_bucket[meta.id + "_sbm"];
        break;
    } catch(e) {
        log(e);
    }
    }
}
