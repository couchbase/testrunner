function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics JOIN query ---
        step = "analytics_joins";
        log("Processing doc:", meta.id);

        var joinIt = couchbase.analyticsQuery(
            "SELECT r.airline, a.name AS airline_name, COUNT(*) AS route_count " +
            "FROM `travel-sample`.`inventory`.`route` r " +
            "JOIN `travel-sample`.`inventory`.`airline` a ON r.airline = a.iata " +
            "GROUP BY r.airline, a.name ORDER BY route_count DESC LIMIT 5;"
        );
        var rows = [];
        for (let row of joinIt) {
            rows.push(row);
        }

        log("JOIN query returned", rows.length, "rows");

        // --- Step 2: cURL enrichment for top analytics results ---
        step = "curl_enrichment";
        var enrichedResults = [];
        for (var i = 0; i < Math.min(rows.length, 2); i++) {
            var airline = rows[i];
            var request = {
                path: '/post',
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    airline_code: airline.airline,
                    airline_name: airline.airline_name,
                    route_count: airline.route_count,
                    request_type: "enrichment"
                })
            };

            try {
                var response = curl("POST", server, request);
                enrichedResults.push({
                    airline: airline.airline_name,
                    status: response.status,
                    enrichment: response.body
                });
            } catch (curlErr) {
                enrichedResults.push({
                    airline: airline.airline_name,
                    error: curlErr.toString()
                });
            }
        }

        log("cURL enrichment done for", enrichedResults.length, "airlines");

        // --- Step 3: Write result ---
        step = "write_result";
        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "joins_with_curl",
            row_count: rows.length,
            enriched_results: enrichedResults,
            data: rows,
            coexistence: (rows.length > 0 && enrichedResults.length > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics JOIN + cURL test complete for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
