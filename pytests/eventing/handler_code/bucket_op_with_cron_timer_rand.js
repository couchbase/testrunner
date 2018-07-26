function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 300);

    var context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, time_rand+'', context);
}
function NDtimerCallback(context) {
    try {
        dst_bucket[context.docID] = 'from NDtimerCallback';
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