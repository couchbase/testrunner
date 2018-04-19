function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 30;
    docTimer(timerCallback,  expiry, meta.id);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 30;
    cronTimer(NDtimerCallback,  expiry, meta.id);
}
function NDtimerCallback(docid) {
    delete dst_bucket[docid];
}
function timerCallback(docid) {
    dst_bucket[docid] = 'from timerCallback';
}