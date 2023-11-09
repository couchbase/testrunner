function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var new_meta = {"id": meta.id + random_gen()};
    couchbase.insert(src_bucket,new_meta,doc,{"self_recursion": false});
}

function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}