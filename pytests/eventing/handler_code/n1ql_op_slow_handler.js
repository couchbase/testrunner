function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
    var i=0;
    var query = SELECT * From `travel-sample` limit 1500;
    for(var q of query){
        i++;
    }
    if(i==1500){
        dst_bucket[meta.id]="query returns 1500 doc";
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
    var query = SELECT * From `travel-sample` limit 1500;
    for(var q of query){
        i++;
    }
    if(i==1500){
        delete dst_bucket[meta.id];
    }
    query.close();
    }
    catch(e){
        log(e);
    }
}