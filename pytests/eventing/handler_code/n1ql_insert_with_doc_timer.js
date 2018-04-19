function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback,  expiry, meta.id);
}
function timerCallback(docid) {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( UUID() ,'timerCallback');
//    query.execQuery();
}