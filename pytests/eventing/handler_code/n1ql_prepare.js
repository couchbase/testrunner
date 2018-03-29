function OnUpdate(doc, meta) {
    log('document', doc);
    var query=Execute test;
//    query.execQuery();
}
function OnDelete(meta) {
    dst_bucket[meta.id]='deleted';
}