function OnUpdate(doc, meta) {
    var create_secondary=create index i1 on `src_bucket`(join_yr) with {"defer_build":false,"num_replica":1};
    dst_bucket["secondary"]=create_secondary.execQuery();

    var drop_secondary=drop index src_bucket.i1;
    dst_bucket["drop_secondary"]=drop_secondary.execQuery();

    var drop_primary=drop primary index on src_bucket;
    dst_bucket["drop_primary"]=drop_primary.execQuery();
}
