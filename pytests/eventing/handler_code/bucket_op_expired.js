function OnUpdate(doc, meta) {
    log('docId', meta.id);
    dst_bucket[meta.id]=doc;
}
function OnDelete(meta, options) {
    if(options.expired){
    log('expired doc', meta.id);
    delete dst_bucket[meta.id];
    }
}