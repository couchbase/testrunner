function OnUpdate(doc, meta) {
    log('docId', meta.id);
    dst_bucket[meta.id]=doc;
}
function OnDelete(meta, options) {
    if(typeof(options.expired)=="boolean"){
        delete dst_bucket[meta.id];
    }
}