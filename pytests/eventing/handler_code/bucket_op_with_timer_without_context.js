function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);
    createTimer(timerCallback, expiry, meta.id);
}

function timerCallback() {
    var key = Date.now()+ Math.random();
    dst_bucket[key]='From timerCallback without context';
}