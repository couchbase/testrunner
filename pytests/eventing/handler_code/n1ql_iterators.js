function OnUpdate(doc, meta) {
    log('document : ', meta.id);
    // We have intentionally left new lines to test multi line n1ql support in eventing
    var query_result = SELECT

            *



                       FROM src_bucket




                                                       LIMIT 10;
    var res1 = test_continue(query_result);
    var query_result1 = SELECT

            *



                       FROM src_bucket




                                                       LIMIT 10;
    var res2 = test_break(query_result1);
    var query_result2 = SELECT

            *



                       FROM src_bucket




                                                       LIMIT 10;
    var res3 = test_labelled_continue(query_result2);
    var query_result3 = SELECT

            *



                       FROM src_bucket




                                                       LIMIT 10;
    var res4 = test_labelled_break(query_result3);
    if (res1 && res2 && res3 && res4){
        dst_bucket[meta.id] = doc;
    }
}
function OnDelete(meta) {
    // We have intentionally scrambled the query
    var query_result = SELECT
                              *
                                   FROM
                                            metadata
                                                        LIMIT
                                                                10
                                                        ;
    var res1 = test_try_catch_throw(query_result);
    if (res1){
        delete dst_bucket[meta.id];
    }
}

function test_continue(query_result){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    for(var row of nums) {
      for(var row of nums) {
        count++;
        if(count == 5 || count == 7){
          continue;
        }
        ++count;
      }
    }
    var count1 = 0;
    for(var row of query_result) {
      for(var row of query_result) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue;
        }
        ++count1;
      }
    }
    log("test_continue :: count : ",count);
    log("test_continue :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_break(query_result){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    for(var row of nums) {
        for(var row of nums) {
            count++;
            if(count == 5 || count == 7){
                break;
            }
            ++count;
        }
    }
    var count1 = 0;
    for(var row of query_result) {
        for(var row of query_result) {
            count1++;
            if(count1 == 5 || count1 == 7){
                break;
            }
            ++count1;
        }
    }
    log("test_break :: count : ",count);
    log("test_break :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_labelled_continue(query_result){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    list: for(var row of nums) {
      for(var row of nums) {
        count++;
        if(count == 5 || count == 7){
          continue list;
        }
        ++count;
      }
    }
    var count1 = 0;
    list: for(var row of query_result) {
      for(var row of query_result) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue list;
        }
        ++count1;
      }
    }
    log("test_labelled_continue :: count : ",count);
    log("test_labelled_continue :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_labelled_break(query_result){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    list: for(var row of nums) {
        for(var row of nums) {
            count++;
            if(count == 5 || count == 7){
                break list;
            }
            ++count;
        }
    }
    var count1 = 0;
    list: for(var row of query_result) {
        for(var row of query_result) {
            count1++;
            if(count1 == 5 || count1 == 7){
                break list;
            }
            ++count1;
        }
    }
    log("test_labelled_break :: count : ",count);
    log("test_labelled_break :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_try_catch_throw(query_result){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    for(var row of nums) {
        try{
            for(var row of nums) {
                ++count;
                if(count == 5 || count == 7){
                    throw 'Error';
                }
            }
        } catch(e) {
            ++count;
		}
	}
    var count1 = 0;
    for(var row of query_result) {
        try{
            for(var row of query_result) {
                ++count1;
                if(count1 == 5 || count1 == 7){
                    throw 'Error';
                }
            }
        } catch(e) {
            ++count1;
		}
	}
    log("test_try_catch_throw :: count : ",count);
    log("test_try_catch_throw :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}