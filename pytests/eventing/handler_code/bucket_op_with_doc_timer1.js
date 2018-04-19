function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    docTimer(doctimerCallback,  expiry, meta.id);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    cronTimer(NDtimerCallback,  expiry, meta.id);
}
function NDtimerCallback(docid) {
    delete dst_bucket[docid];
}
function doctimerCallback(docid) {
    dst_bucket[docid] = 'from doctimerCallback';
}