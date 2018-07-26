function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 180);
    var time_rand = random_gen();
    var context = {docID : meta.id};
    createTimer(DocTimerCallback, expiry, time_rand+'', context);
}
function DocTimerCallback(context) {
    try {
        dst_bucket[context.docID] = 'from DocTimerCallback';
    } catch(e) {
        //var time_rand = random_gen();
        //dst_bucket[time_rand] = 'from DocTimerCallback';
    }
}
function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}