function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 3);

    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
}

function timerCallback(context) {
    var result= couchbase.insert(dst_bucket,{"id":context.docID},context.random_text);
    log(result);
}

function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);

    var context = {docID : meta.id };
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}

function NDtimerCallback(context) {
    var meta={"id":context.docID}
    var result = couchbase.delete(dst_bucket,meta);
    log(result);
}

