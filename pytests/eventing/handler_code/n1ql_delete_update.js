function OnUpdate(doc, meta) {
    log('document', doc);
    var query=DELETE from src_bucket where mutated=0;
    query.execQuery();
}
function OnDelete(meta) {
    dst_bucket[meta.id]='deleted';
}