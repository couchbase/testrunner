function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(timerCallback, meta.id, expiry);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, meta.id, expiry);
}
function NDtimerCallback(docid, expiry) {
    delete dst_bucket[docid];
}
function timerCallback(docid, expiry) {
    // read the source bucket data from handler code and copy it to destination bucket
    dst_bucket[docid] = src_bucket[docid];
}