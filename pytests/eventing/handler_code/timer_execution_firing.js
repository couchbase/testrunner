function OnUpdate(doc,meta) {
    try{
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);
    var context = {id : meta.id,expected_time: expiry};
    var timers=createTimer(timerCallback,  expiry, null, context);
    }catch(e){
        log("error in onUpdate:",e);
    }
}
function timerCallback(context) {
    try{
    var expected_time=context.expected_time;
    var now = new Date().getTime();
    var total = (now - new Date(expected_time).getTime()) ;
    var totalD =  Math.abs(Math.floor(total/1000));
    log("totalD",totalD);
    if(now < new Date(expected_time){
        log("timer executed before time");
        return;
    }
    if(now > new Date(expected_time) && totalD <= 90 ){
        dst_bucket[context.id]=totalD;
    }
    }catch(e){
        log("Error in timer:",e);
    }
}
