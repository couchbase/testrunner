function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    docTimer(timerCallback, meta.id, expiry);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    cronTimer(NDtimerCallback, meta.id, expiry);
}
function NDtimerCallback(docid, expiry) {
    var query = DELETE FROM dst_bucket where meta().id = $docid;
//    query.execQuery();
}
function timerCallback(docid, expiry) {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docid ,'timerCallback');
//    query.execQuery();
}