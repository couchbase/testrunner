function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    try{
    var result = couchbase.get(src_bucket,meta);
    log(result);
    var id={"id":random_gen()}
    if(result.success && result.meta.cas != undefined &&
        JSON.stringify(result.doc) === JSON.stringify(doc)){
            var result1= couchbase.insert(dst_bucket,id,doc);
    }
    log(result1);
    }catch(e){
        log("error:",e);
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var doc={"id":meta.id}
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
}

function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}