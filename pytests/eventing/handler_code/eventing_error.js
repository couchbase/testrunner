function OnUpdate(doc, meta) {
    try {
      let symbol3 = Symbol('foo');
      dst_bucket[meta.id]=symbol3;
    }
    catch (e) {
        log('error:', e);
        if(e instanceof EventingError){
            dst_bucket[meta.id]="EventingError";
        }

    }
}