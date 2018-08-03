function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);
    var context = {docID : meta.id};

    createTimer(NDtimerCallback,  expiry, meta.id, context);
    createTimer(timerCallback,  expiry, meta.id, context);
}

function timerCallback(context) {
    dst_bucket[context.docid] = 'from docTimerCallback';
}

function NDtimerCallback(context) {
    dst_bucket1[context.docid] = 'from cronTimerCallback';
}
