function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    var context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}
function NDtimerCallback(context) {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback');
//    query.execQuery();
}