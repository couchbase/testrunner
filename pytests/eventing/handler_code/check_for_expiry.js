function OnUpdate(doc, meta) {
    log('docId', meta.id);
    var res=SELECT META().id, META().expiration FROM dst_bucket where META().id=$meta.id;
    for(var r of res){
        if(r["expiration"] !=0){
            dst_bucket1[meta.id]="has expiry"
        }
    }
}
function OnDelete(meta) {
}