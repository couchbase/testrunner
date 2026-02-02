function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var val = src_bucket[meta.id];
    // Explicit validation for GET
    if (val === null || val === undefined) {
       throw new Error("GET failed: document not found for key " + meta.id);
    }
    log("GET operation successful for key:", meta.id);
    try{
        var time_rand = random_gen();
        var result= couchbase.insert(src_bucket,{"id":meta.id + time_rand},doc);
        log(result);
    }catch(e){
        log(e);
    }
}

function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}