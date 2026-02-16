// Timer callback — runs analytics COUNT(DISTINCT) query after 30s delay
function analyticsCountDistinctTimerCallback(context) {
    var step = "timer_callback";
    try {
        log("Analytics COUNT(DISTINCT) timer fired for doc:", context.docID);

        step = "analytics_count_distinct";
        var countDistinctIt = couchbase.analyticsQuery(
            "SELECT COUNT(DISTINCT country) AS distinct_country_count FROM `travel-sample`.`inventory`.`airline`;"
        );
        var result = null;
        for (let row of countDistinctIt) {
            result = row;
        }

        var distinctCount = result ? result.distinct_country_count : 0;
        log("Analytics COUNT(DISTINCT) (via timer) returned:", distinctCount);

        step = "write_result";
        dst_bucket[context.docID] = {
            doc_id: context.docID,
            query_type: "count_distinct_with_timer",
            distinct_country_count: distinctCount,
            data: result,
            triggered_by: "timer",
            success: (distinctCount > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics COUNT(DISTINCT) timer test complete for:", context.docID);

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
            queryType: "analytics_count_distinct_with_timer"
        };

        createTimer(analyticsCountDistinctTimerCallback, expiry, meta.id, context);
        log("Analytics COUNT(DISTINCT) timer (30s) scheduled for doc:", meta.id);

    } catch (e) {
        log("Error scheduling analytics COUNT(DISTINCT) timer:", e);
    }
}
