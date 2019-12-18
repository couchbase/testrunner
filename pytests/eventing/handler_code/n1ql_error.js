function OnUpdate(doc, meta) {
    try {
     select * from test;
    }
    catch (e) {
        log('error:', e);
        if(e instanceof N1QLError){
            dst_bucket[meta.id]="N1QLError";
        }

    }
}