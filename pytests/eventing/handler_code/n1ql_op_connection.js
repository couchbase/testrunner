function OnUpdate(doc, meta) {
    try{
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
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
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    SELECT * FROM `travel-sample` limit 1;
    } catch (e) {
        log (e);
        if (e.message === 'Connection pool maximum capacity reached') {
            delete dst_bucket[meta.id];
        } else {
            log (e);
        }
    }
}