function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback,  expiry, meta.id);
    cronTimer(NDtimerCallback,  expiry, meta.id);
}

function timerCallback(docid) {
    dst_bucket[docid] = 'from docTimerCallback';
}

function NDtimerCallback(docid) {
    dst_bucket1[docid] = 'from cronTimerCallback';
}
