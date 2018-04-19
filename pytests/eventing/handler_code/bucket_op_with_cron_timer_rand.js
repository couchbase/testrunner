function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 300;
    var time_rand = random_gen();
    cronTimer(NDtimerCallback, expiry, time_rand+'');
}
function NDtimerCallback(docid) {
    try {
        dst_bucket[docid] = 'from NDtimerCallback';
    } catch(e) {
        //var time_rand = random_gen();
        //dst_bucket[time_rand] = 'from NDtimerCallback';
    }
}
function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}