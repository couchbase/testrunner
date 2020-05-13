function OnUpdate(doc,meta) {
   var src_doc= doc;
   if(src_doc["updated_field"]==undefined){
    src_doc["updated_field"]="a";
   }
   else if(src_doc["updated_field"]=="a"){
        log("Run time recursion from SBM for id:",meta.id);
   }
   src_bucket[meta.id]=src_doc;

}