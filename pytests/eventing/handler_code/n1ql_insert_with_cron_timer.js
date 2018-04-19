function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback,  expiry, meta.id);
}
function NDtimerCallback(docid) {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback');
//    query.execQuery();
}