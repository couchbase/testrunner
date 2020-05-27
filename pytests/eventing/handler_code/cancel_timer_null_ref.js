function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);
    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
    log("create timer for:",meta.id);
}
function OnDelete(meta) {
    try{
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);
    var context = {docID : meta.id};
    var cancelled=cancelTimer(NDtimerCallback,null);
    }catch(e){
        log(e);
        if(e instanceof EventingError && e["message"]=="Second argument to cancelTimer must be a string"){
            log("cancellation failed with:",e)
            dst_bucket[meta.id]=e;
        }
    }
}

function NDtimerCallback(context) {
    log("firing delete timer:",context.docID);
}

function timerCallback(context) {
    log("firing update timer:",context.docID);
}
