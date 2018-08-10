function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);
    createTimer(timerCallback, expiry, meta.id);
}

function timerCallback() {
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var key = Math.round((new Date()).getTime() / 1000) + rand;
    dst_bucket[key]='From timerCallback without context';
}