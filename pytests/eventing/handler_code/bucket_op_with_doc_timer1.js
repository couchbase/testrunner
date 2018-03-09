function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    docTimer(doctimerCallback, meta.id, expiry);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    cronTimer(NDtimerCallback, meta.id, expiry);
}
function NDtimerCallback(docid, expiry) {
    delete dst_bucket[docid];
}
function doctimerCallback(docid, expiry) {
    dst_bucket[docid] = 'from doctimerCallback';
}