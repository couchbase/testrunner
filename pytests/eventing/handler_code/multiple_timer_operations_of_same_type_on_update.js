function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, meta.id, expiry);
    cronTimer(NDtimerCallback1, meta.id, expiry);
}

function NDtimerCallback(docid, expiry) {
    dst_bucket[docid] = 'from NDtimerCallback';
}

function NDtimerCallback1(docid, expiry) {
    var query = UPSERT INTO dst_bucket1 ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback1');
//    query.execQuery();
}
