function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    var context = {docID : meta.id};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function timerCallback(context) {
    dst_bucket[context.docID] = 'from timerCallback';
}