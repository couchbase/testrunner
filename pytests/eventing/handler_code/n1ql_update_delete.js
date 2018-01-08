function OnUpdate(doc, meta) {
    var sel=SELECT *  from src_bucket where mutated=0 limit 1;
    key = meta.id+"_update"
    dst_bucket[key]=sel.execQuery();
}


function OnDelete(meta) {
    var sel=SELECT *  from src_bucket where mutated=0 limit 1;
    key = meta.id+"_delete"
    dst_bucket[key]=sel.execQuery();
}