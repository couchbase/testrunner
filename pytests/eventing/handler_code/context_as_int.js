function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 10);
    var context = 12324343545345345345345344334534533224342;
    var timers=createTimer(timerCallback,  expiry, null, context);
}
function timerCallback(context) {
    dst_bucket[Date.now().toString()]=context;
}
