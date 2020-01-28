function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
    var i=0;
    var query = SELECT * From `travel-sample`;
    for(var q of query){
        dst_bucket[i] = q;
        i++;
    }
    query.close();
    }
    catch(e){
        log(e);
    }
}


function OnDelete(meta) {
    log('docId', meta.id);
    try{
    var i=0;
    var query = SELECT * From `travel-sample`;
    for(var q of query){
        delete dst_bucket[i];
        i++;
    }
    query.close();
    }
    catch(e){
        log(e);
    }
}