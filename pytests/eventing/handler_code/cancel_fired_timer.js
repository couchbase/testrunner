function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);
    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
    log("create timer for:",meta.id);
}
function OnDelete(meta) {
    try{
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);
    var context = {docID : meta.id};
    var cancelled=cancelTimer(NDtimerCallback,meta.id);
    log("Timer cancelled? :",cancelled);
    if(!cancelled){
        delete dst_bucket[meta.id];
    }
    }catch(e){
        log(e)
    }
}

function NDtimerCallback(context) {
    log("firing delete timer:",context.docID);
}

function timerCallback(context) {
    log("firing update timer:",context.docID);
    dst_bucket[context.docID] = context.random_text;
}
