function OnUpdate(doc, meta) {
    log('document', doc);
    try {
        var time_rand = random_gen();
        src_bucket[meta.id + time_rand] = doc;
    } catch(e) {
        log(e);
        dst_bucket[meta.id]=doc;
    }
}
function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}
