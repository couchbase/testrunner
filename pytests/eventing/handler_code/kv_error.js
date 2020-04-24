function OnUpdate(doc, meta) {
    try{
    dst_bucket[meta.id]=doc;
    }catch(e){
        if(e instanceof KVError){
            log("KVError:",e);
        }
    }
}