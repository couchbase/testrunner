function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}
function OnDeploy(action) {
    var success = 0;
    // Heavy task with a small timeout value
    setTimeout(() => {
        let sum = 0;
        for (let i = 0; i < 10_000_000_000; i++) {
            sum += i;
        }
        dst_bucket["timeout_success"] = 'Success: Timeout operation completed.';
        success++;
    }, 1); // Small timeout value - 1ms
    return success;
}

