// Timer callback — runs analytics filtered query after 30s delay
function analyticsFilteredTimerCallback(context) {
    var step = "timer_callback";
    try {
        log("Analytics filtered timer fired for doc:", context.docID);

        step = "analytics_filtered";
        var filteredIt = couchbase.analyticsQuery(
            "SELECT name, country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country LIMIT 10;",
            { "country": "United States" }
        );
        var rows = [];
        for (let row of filteredIt) {
            rows.push(row);
        }

        log("Analytics filtered query (via timer) returned", rows.length, "rows");

        step = "write_result";
        dst_bucket[context.docID] = {
            doc_id: context.docID,
            query_type: "filtered_with_timer",
            row_count: rows.length,
            data: rows,
            triggered_by: "timer",
            success: (rows.length > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics filtered timer test complete for:", context.docID);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}

function OnUpdate(doc, meta) {
    try {
        var expiry = new Date();
        expiry.setSeconds(expiry.getSeconds() + 30);

        var context = {
            docID: meta.id,
            queryType: "analytics_filtered_with_timer"
        };

        createTimer(analyticsFilteredTimerCallback, expiry, meta.id, context);
        log("Analytics filtered timer (30s) scheduled for doc:", meta.id);

    } catch (e) {
        log("Error scheduling analytics filtered timer:", e);
    }
}
