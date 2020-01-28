function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
    var doc=dst_bucket[meta.id];
    log('doc:',doc);
    }catch(e){
        log('error:',e);
        //var obj=JSON.parse(e);
        //log(obj);
        if(e["message"]["name"]=="LCB_KEY_ENOENT"){
            dst_bucket[meta.id]=e;
        }
    }
}