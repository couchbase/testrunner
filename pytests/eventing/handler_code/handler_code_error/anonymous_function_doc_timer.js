function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(function(docid, expiry) {
    dst_bucket[docid] = 'from NDtimerCallback';
    }, meta.id, expiry);
}