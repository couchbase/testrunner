function OnUpdate(doc,meta) {
    try{
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 10);
    var context = true;
    var timers=createTimer(timerCallback,  expiry, null, context);
    }catch(e){
        log(e);
    }
}
function timerCallback(context) {
    try{
    dst_bucket[Date.now().toString()]=context;
    }catch(e){
        log(e);
    }
}
