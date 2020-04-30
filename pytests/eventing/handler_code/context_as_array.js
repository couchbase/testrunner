function OnUpdate(doc,meta) {
    try{
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 10);
    var context = [1, 2, 4];
    var timers=createTimer(timerCallback,  expiry, null, context);
    }catch(e){
        log("error in onUpdate:",e);
    }
}
function timerCallback(context) {
    try{
    dst_bucket[Date.now().toString()]=context;
    }catch(e){
        log("Error in timer:",e);
    }
}
