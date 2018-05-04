function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    docTimer(doctimerCallback,  expiry, meta.id);
}
function doctimerCallback(docid) {
    dst_bucket[docid] = 'from doctimerCallback';
}