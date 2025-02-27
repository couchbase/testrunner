function OnUpdate(doc, meta, xattrs) {
    log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
    log("OnDeploy function run", action);
    log("Testing Analytics inside Eventing")
    var randomId = "doc_" + Math.random().toString()
    var meta = { id: randomId };  // Set the random id to meta.id
    try {
        var count = 0;
        const limit = 2;
        var query_analytics = couchbase.analyticsQuery('SELECT * FROM `travel-sample`.`inventory`.`airline` LIMIT 2');
        for (let row of query_analytics) {
            log("Query Results:", row);
            ++count;
        }
        if (count === limit) {
            dst_bucket[meta.id] = 'analytics_query_executed';
        }
    } catch (e) {
        log("Error executing Analytics query:", e);
    }
}