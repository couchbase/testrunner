function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    var context = {docID : meta.id};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function timerCallback(context) {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( UUID() ,'timerCallback');
//    query.execQuery();
}