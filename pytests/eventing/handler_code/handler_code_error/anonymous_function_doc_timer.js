function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);
    var context = {docID : meta.id};

    createTimer(function(context) {
    dst_bucket[context.docID] = 'from NDtimerCallback';
    }, expiry, meta.id);
}
