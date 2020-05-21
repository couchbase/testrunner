function OnUpdate(doc, meta) {
    log('docId', meta.id);
    dst_bucket[meta.id]=doc;
}
function OnDelete(meta, options) {
    if(!options.expired){
        delete dst_bucket[meta.id];
    }
}