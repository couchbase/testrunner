function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    docTimer(timerCallback,  expiry, meta.id);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    cronTimer(NDtimerCallback,  expiry, meta.id);
}
function NDtimerCallback(docid) {
    var query = DELETE FROM dst_bucket2 where meta().id = $docid;
//    query.execQuery();
}
function timerCallback(docid) {
    var query = INSERT INTO dst_bucket2 ( KEY, VALUE ) VALUES ( $docid ,'timerCallback');
//    query.execQuery();
}