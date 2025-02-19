function OnUpdate(meta, options) {
    log("Doc updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
    var randomId = "doc_" + Math.random().toString()
    var meta = { id: randomId };  // Set the random id to meta.id
    src_bucket[meta.id] = 'source bucket mutation';
    var doc = src_bucket[meta.id];
    log("Operation complete for doc with id:", meta.id);
}
