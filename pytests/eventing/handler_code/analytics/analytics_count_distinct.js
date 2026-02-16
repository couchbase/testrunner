function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var countDistinctIt = couchbase.analyticsQuery(
            "SELECT COUNT(DISTINCT country) AS distinct_country_count FROM `travel-sample`.`inventory`.`airline`;"
        );
        var result = null;
        for (let row of countDistinctIt) {
            result = row;
        }

        log("COUNT(DISTINCT) result:", JSON.stringify(result));

        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "count_distinct",
            distinct_country_count: result ? result.distinct_country_count : 0,
            data: result,
            success: (result && result.distinct_country_count > 0),
            processed_at: new Date().toISOString()
        };

        log("COUNT(DISTINCT) test complete for:", meta.id);

    } catch (e) {
        log("Error:", e);
    }
}