function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 3);

    var context = {docID : meta.id, scope_name: meta.keyspace.scope_name, collection_name: meta.keyspace.collection_name, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
}

function timerCallback(context) {
    var result= couchbase.insert(dst_bucket,{
    "id":context.docID, "keyspace": {"scope_name": context.scope_name,
    "collection_name": context.collection_name}},context.random_text);
    log(result);
}

function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);

    var context = {docID : meta.id,
    scope_name: meta.keyspace.scope_name,
    collection_name: meta.keyspace.collection_name};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}

function NDtimerCallback(context) {
    var meta={"id":context.docID,
    "keyspace": {"scope_name": context.scope_name,
    "collection_name": context.collection_name}}
    var result = couchbase.delete(dst_bucket,meta);
    log(result);
}