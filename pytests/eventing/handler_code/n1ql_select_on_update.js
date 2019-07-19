function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = select * from src_bucket limit 1;
    for(var raw of query){
        log(raw);
    }
}