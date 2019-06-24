function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);
    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);
    var context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}
function NDtimerCallback(context) {
    var docID = context.docID;
    while (true) {
    try {
        var query = DELETE FROM dst_bucket where meta().id = $docID;
        break;
    } catch (e) {
        log(e);
        }
    }
}

function timerCallback(context) {
    var docID = context.docID;
    while (true) {
    try {
        var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docID ,'timerCallback');
        break;
    } catch (e) {
        log(e);
        }
    }
}