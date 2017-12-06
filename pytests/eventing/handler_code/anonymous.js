function OnUpdate(doc, meta) {
    var anon = function () {
         var sel = SELECT  *  from src_bucket where mutated=0 limit 1;
         dst_bucket['select'] = sel.execQuery();
   };
    anon();

     (function () {
         var sel = SELECT  *  from src_bucket where mutated=0 limit 1;
         dst_bucket['select2'] = sel.execQuery();
   })();
}