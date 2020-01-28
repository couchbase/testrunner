function OnUpdate(doc, meta){
var sel=SELECT *  from src_bucket where mutated=0 limit 1;
for(var row of sel){
    dst_bucket["select"]=row;
}

var ins=INSERT into dst_bucket (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" });
dst_bucket["insert"]=ins;

var up=UPDATE dst_bucket set name='update' where name='new hotel';
dst_bucket["update"]=up;

var del=DELETE from dst_bucket where name="update";
dst_bucket["delete"]=del;

var ups=UPSERT INTO `dst_bucket` (KEY, VALUE)
VALUES ("key1", { "type" : "upsert", "name" : "new hotel" })
RETURNING *;
dst_bucket["upsert"]=ups;
}