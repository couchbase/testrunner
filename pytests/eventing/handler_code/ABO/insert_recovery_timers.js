function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    var context = {docID : meta.id };
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}

function NDtimerCallback(context) {
   log("Doc deleted/expired", context.docID);
    var doc={"id":context.docID}
    var success = false;
    while(!success){
    try{
    var result = couchbase.delete(dst_bucket,doc);
    log(result);
    success=result.success;
    if(result.error != undefined && result.error.name == "LCB_KEY_ENOENT"){
            break;
            }
    }catch(e){
        log("error:",e);
        }
    }
}

function timerCallback(context) {
   var success = false;
   while(!success){
        try{
        var result= couchbase.insert(dst_bucket,{"id":context.docID},"timer fired");
        log(result);
        success=result.success;
        if(result.error != undefined && result.error.name == "LCB_KEY_EEXISTS"){
            break;
            }
        }catch(e){
        log("error:",e);
        }
   }
}
