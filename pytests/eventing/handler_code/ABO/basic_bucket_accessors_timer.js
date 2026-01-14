function OnUpdate(doc, meta) {
    try {
        // GET test
        if (src_bucket[meta.id] === undefined) {
            throw new Error("GET failed in OnUpdate for key " + meta.id);
        }
        log("GET successful in OnUpdate for key " + meta.id);

        var expiry = new Date();
        expiry.setSeconds(expiry.getSeconds() + 5);

        var context = {
            docID: meta.id,
            random_text: "test_text"
        };

        createTimer(timerCallback, expiry, meta.id, context);

    } catch (e) {
        log(e.message);
        throw e;
    }
}

function OnDelete(meta, options) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);
    createTimer(NDtimerCallback, expiry, meta.id, { docID: meta.id });
    delete dst_bucket[meta.id];
}

function NDtimerCallback(context) {
    delete dst_bucket[context.docID];
}

function timerCallback(context) {
    dst_bucket[context.docID] = context.random_text;
}
