function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(function(docid) {
    dst_bucket[docid] = 'from NDtimerCallback';
    }, expiry, meta.id);
}