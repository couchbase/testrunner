function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 180;
    var time_rand = random_gen();
    docTimer(DocTimerCallback, expiry, time_rand+'');
}
function DocTimerCallback(docid) {
    try {
        dst_bucket[docid] = 'from DocTimerCallback';
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