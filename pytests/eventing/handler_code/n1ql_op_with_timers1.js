function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);

    var context = {docID : meta.id};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);

    var context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}
function NDtimerCallback(context) {
    var docID = context.docid;
    var query = DELETE FROM dst_bucket2 where meta().id = $docID;
//    query.execQuery();
}
function timerCallback(context) {
    var docID = context.docid;
    var query = INSERT INTO dst_bucket2 ( KEY, VALUE ) VALUES ( $docID ,'timerCallback');
//    query.execQuery();
}