function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(timerCallback,  expiry, meta.id);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback,  expiry, meta.id);
}
function NDtimerCallback(docid) {
    delete dst_bucket[docid];
}
function timerCallback(docid) {
    // read the source bucket data from handler code and copy it to destination bucket
    dst_bucket[docid] = src_bucket[docid];
}