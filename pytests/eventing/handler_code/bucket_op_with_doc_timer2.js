function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);

    var context = {docID : meta.id};
    createTimer(doctimerCallback,  expiry, meta.id, context);
}

function doctimerCallback(context) {
    dst_bucket[context.docID] = 'from doctimerCallback';
}