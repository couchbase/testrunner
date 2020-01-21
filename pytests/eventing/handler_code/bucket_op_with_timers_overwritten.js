function OnUpdate(doc,meta) {
    var expiry1 = new Date();
    expiry1.setSeconds(expiry1.getSeconds() + 30);

    var expiry2 = new Date();
    expiry2.setSeconds(expiry2.getSeconds() + 120);

    var expiry3 = new Date();
    expiry3.setSeconds(expiry3.getSeconds() + 210);

    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry1, meta.id, context);
    createTimer(timerCallback,  expiry2, meta.id, context);
    createTimer(timerCallback,  expiry3, meta.id, context);
}
function timerCallback(context) {
    var time_rand = random_gen();
    var doc_id = context.docID+'_'+time_rand;
    dst_bucket[doc_id] = context.random_text;
}

function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}