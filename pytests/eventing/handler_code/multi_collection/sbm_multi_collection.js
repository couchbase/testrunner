function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    try{
        var time_rand = random_gen();
        var doc_meta = {"id":meta.id + time_rand,
        "keyspace": {"scope_name":meta.keyspace.scope_name,
        "collection_name":meta.keyspace.collection_name}}
        var result = couchbase.insert(src_bucket, doc_meta, doc);
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