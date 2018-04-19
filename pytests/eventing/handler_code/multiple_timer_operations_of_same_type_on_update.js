function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback,  expiry, meta.id);
    cronTimer(NDtimerCallback1,  expiry, meta.id);
}

function NDtimerCallback(docid) {
    dst_bucket[docid] = 'from NDtimerCallback';
}

function NDtimerCallback1(docid) {
    var query = UPSERT INTO dst_bucket1 ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback1');
//    query.execQuery();
}
