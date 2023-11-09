function OnUpdate(doc, meta) {
    if(!doc.count) {
        doc = {"count": 0};
    }
    if(doc.count < 1000) {
        doc.count++;
        var new_meta = {"id": random_gen()};
        couchbase.insert(src_bucket,new_meta,doc,{"self_recursion": true});
    }
    else if(doc.count < 2000) {
        doc.count++;
        couchbase.upsert(src_bucket,meta,doc,{"self_recursion": true});
    }
    else if(doc.count < 3000) {
        doc.count++;
        couchbase.replace(src_bucket,meta,doc,{"self_recursion": true});
    }
    else {
        couchbase.delete(src_bucket,{"id": meta.id});
    }
}

function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}