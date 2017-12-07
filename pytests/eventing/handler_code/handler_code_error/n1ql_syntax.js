function OnUpdate(doc, meta){
    try{
    var sel=SELECT from src_bucket where mutated=0 limit 1;
    sel.execQuery();
    }
    catch(e){
    dst_bucket["select"]=e;
    }
}