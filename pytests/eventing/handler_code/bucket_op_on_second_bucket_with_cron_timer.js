function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    var context = {docID : meta.id};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function NDtimerCallback(context) {
    dst_bucket1[context.docID] = 'from NDtimerCallback';
}
