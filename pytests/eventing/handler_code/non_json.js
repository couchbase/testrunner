function OnUpdate(doc, meta) {
    log('docId', meta.id);
    try{
        dst_bucket[meta.id]=undefined;
    }catch(e){
        log(e);
        if(e["message"]=="Invalid data type for value - \"undefined\" is not a valid type"){
            dst_bucket[meta.id+"_undefine"]=e;
        }
    }
     try{
        dst_bucket[meta.id]=Object;
    }catch(e){
        log(e);
        if(e["message"]=="Invalid data type for value - \"function\" is not a valid type"){
            dst_bucket[meta.id+"_object"]=e;
        }
    }
    try{
        let symbol3 = Symbol('foo');
        dst_bucket[meta.id]=symbol3;
    }catch(e){
        log(e);
        if(e["message"]=="Invalid data type for value - \"symbol\" is not a valid type"){
            dst_bucket[meta.id+"_symbol"]=e;
        }
    }
}