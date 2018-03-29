function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid, expiry) {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( UUID() ,'timerCallback');
//    query.execQuery();
}