function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
    var doc=dst_bucket[meta.id];
    log('doc:',doc);
    if(typeof(doc)=="undefined"){
        dst_bucket[meta.id]="no doc found";
    }
    }catch(e){
        log('error:',e);
    }
}