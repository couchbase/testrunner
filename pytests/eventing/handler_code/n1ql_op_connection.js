function OnUpdate(doc, meta) {
    try{
        let query1 = SELECT * FROM `travel-sample` limit 10;
        let query2 = SELECT * FROM `travel-sample` limit 10;
        let query3 = SELECT * FROM `travel-sample` limit 10;
        let query4 = SELECT * FROM `travel-sample` limit 10;
        let query5 = SELECT * FROM `travel-sample` limit 10;
        let query6 = SELECT * FROM `travel-sample` limit 10;
        let query7 = SELECT * FROM `travel-sample` limit 10;
        let query8 = SELECT * FROM `travel-sample` limit 10;
        let query9 = SELECT * FROM `travel-sample` limit 10;
        let query10 = SELECT * FROM `travel-sample` limit 10;
        let query11 = SELECT * FROM `travel-sample` limit 10;
    } catch (e) {
        log (e);
        if (e.message === 'Connection pool maximum capacity reached') {
            dst_bucket[meta.id] = 'yes';
        } else {
            log (e);
        }
    }
}

function OnDelete(meta){
    try{
        let query1 = SELECT * FROM `travel-sample` limit 10;
        let query2 = SELECT * FROM `travel-sample` limit 10;
        let query3 = SELECT * FROM `travel-sample` limit 10;
        let query4 = SELECT * FROM `travel-sample` limit 10;
        let query5 = SELECT * FROM `travel-sample` limit 10;
        let query6 = SELECT * FROM `travel-sample` limit 10;
        let query7 = SELECT * FROM `travel-sample` limit 10;
        let query8 = SELECT * FROM `travel-sample` limit 10;
        let query9 = SELECT * FROM `travel-sample` limit 10;
        let query10 = SELECT * FROM `travel-sample` limit 10;
        let query11 = SELECT * FROM `travel-sample` limit 10;
    } catch (e) {
        log (e);
        if (e.message === 'Connection pool maximum capacity reached') {
            delete dst_bucket[meta.id];
        } else {
            log (e);
        }
    }
}