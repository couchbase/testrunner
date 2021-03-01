function OnUpdate(doc, meta) {
    try{
    var query1=SELECT * FROM `travel-sample` limit 1;
    var query2=SELECT * FROM `travel-sample` limit 1;
    var query3=SELECT * FROM `travel-sample` limit 1;
    var query4=SELECT * FROM `travel-sample` limit 1;
    var query5=SELECT * FROM `travel-sample` limit 1;
    var query6=SELECT * FROM `travel-sample` limit 1;
    var query7=SELECT * FROM `travel-sample` limit 1;
    var query8=SELECT * FROM `travel-sample` limit 1;
    var query9=SELECT * FROM `travel-sample` limit 1;
    var query10=SELECT * FROM `travel-sample` limit 1;
    var query11=SELECT * FROM `travel-sample` limit 1;
    var query12=SELECT * FROM `travel-sample` limit 1;
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
    var query1=SELECT * FROM `travel-sample` limit 1;
    var query2=SELECT * FROM `travel-sample` limit 1;
    var query3=SELECT * FROM `travel-sample` limit 1;
    var query4=SELECT * FROM `travel-sample` limit 1;
    var query5=SELECT * FROM `travel-sample` limit 1;
    var query6=SELECT * FROM `travel-sample` limit 1;
    var query7=SELECT * FROM `travel-sample` limit 1;
    var query8=SELECT * FROM `travel-sample` limit 1;
    var query9=SELECT * FROM `travel-sample` limit 1;
    var query10=SELECT * FROM `travel-sample` limit 1;
    var query11=SELECT * FROM `travel-sample` limit 1;
    var query12=SELECT * FROM `travel-sample` limit 1;
    } catch (e) {
        log (e);
        if (e.message === 'Connection pool maximum capacity reached') {
            delete dst_bucket[meta.id];
        } else {
            log (e);
        }
    }
}