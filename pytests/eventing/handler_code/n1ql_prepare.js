function OnUpdate(doc, meta) {
    try{
        log('document', doc);
        var query=Execute test;
    }catch(e){
        log(e);
    }
}
function OnDelete(meta) {
    try{
        dst_bucket[meta.id]='deleted';
    }catch(e){
    log(e);
    }
}