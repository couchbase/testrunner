function OnUpdate(doc, meta) {
    try
    {
        var expiry = new Date();
        expiry.setSeconds(expiry.getSeconds() + 30);
        var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh"};
        createTimer(timerCallback, expiry, meta.id,context);
    }
    catch(e)
    {
        log(e);
        if(e instanceof EventingError && e["message"]=="The context payload size is more than the configured size:20 bytes")
        {
            dst_bucket[meta.id]=e;
        }
    }
}

function timerCallback(context) {
}

function OnDelete(meta) {
    log('deleting document', meta.id);
    delete dst_bucket[meta.id];
}