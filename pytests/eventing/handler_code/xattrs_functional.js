function callerFunction(operation,meta, path, value, xattrs) {
    switch (operation) {
        case "insert":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.insert(path, value, { "xattrs": xattrs })]);
        case "upsert":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.upsert(path, value, { "xattrs": xattrs })]);
        case "replace":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.replace(path, value, { "xattrs": xattrs })]);
        case "remove":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.remove(path, { "xattrs": xattrs })]);
        case "get":
            return couchbase.lookupIn(dst_bucket, meta, [couchbase.LookupInSpec.get(path, { "xattrs": xattrs })]);
        case "arrayAppend":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.arrayAppend(path, value, { "xattrs": xattrs })]);
        case "arrayPrepend":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.arrayPrepend(path, value, { "xattrs": xattrs })]);
        case "arrayInsert":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.arrayInsert(path, value, { "xattrs": xattrs })]);
        case "arrayAddUnique":
            return couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.arrayAddUnique(path, value, { "xattrs": xattrs })]);
    }
}

function OnUpdate(doc, meta, xattrs) {
    var meta = {"id": meta.id};
    couchbase.insert(dst_bucket, meta, doc);
    // Positive scenarios
    const positiveTestCases = [
        // MutateInSpec operations with xattrs: true
        { operation: "insert", path: "testField", value: 123, xattrs: true },
        { operation: "insert", path: 123, value: "test", xattrs: true },
        { operation: "upsert", path: "testField", value: 3.14, xattrs: true },
        { operation: "upsert", path: [89,89], value: 5, xattrs: true },
        { operation: "replace", path: "testField", value: "hello", xattrs: true },
        { operation: "upsert", path: "testField", value: [1,2,3], xattrs: true },
        { operation: "replace", path: "testField", value: {"a":1,"b":2}, xattrs: true },
        { operation: "remove", path: "testField", value: null, xattrs: true },
        { operation: "insert", path: "testField", value: null, xattrs: true },
        { operation: "upsert", path: "testField", value: new Date(), xattrs: true },
        { operation: "replace", path: "testField", value: true, xattrs: true },
        // LookupInSpec operation with xattrs: true
        { operation: "get", path: "testField", value: null, xattrs: true },
        // Array operations with xattrs: true
        { operation: "upsert", path: "arrayTest", value: [], xattrs: true },
        { operation: "arrayAppend", path: "arrayTest", value: Infinity, xattrs: true },
        { operation: "arrayPrepend", path: "arrayTest", value: 1.618, xattrs: true },
        { operation: "arrayInsert", path: "arrayTest[0]", value: "first", xattrs: true },
        { operation: "arrayAddUnique", path: "arrayTest", value: true, xattrs: true },
        { operation: "arrayAppend", path: "arrayTest", value: [1,2,3,4], xattrs: true },
        { operation: "arrayPrepend", path: "arrayTest", value: {"a":1,"b":2}, xattrs: true },
        { operation: "arrayInsert", path: "arrayTest[0]", value: new Date(), xattrs: true },
        { operation: "arrayAppend", path: "arrayTest", value: NaN, xattrs: true },
        { operation: "upsert", path: "testArray", value: [], xattrs: true }
    ];

    // Negative scenarios
    const negativeTestCases = [
        // Invalid Path (String, int, float)
        { operation: "remove", path: "invalidField", value: null, xattrs: true },
        { operation: "replace", path: 3.14, value: "new value", xattrs: true },

        // Invalid Types (Undefined, Infinity, Function, NaN, Date, Symbol, BigInt)
        { operation: "insert", path: "invalidField", value: undefined, xattrs: true },
        { operation: "replace", path: "invalidField", value: function() {}, xattrs: true },
        { operation: "replace", path: "invalidField", value: Symbol("test"), xattrs: true },

        { operation: "get", path: "invalidFieldxyz", value: null, xattrs: true },

        { operation: "arrayAppend", path: 123, value: "test", xattrs: true },
        { operation: "arrayPrepend", path: "invalidField", value: null, xattrs: true },
        { operation: "arrayInsert", path: [89,89], value: 5, xattrs: true },
        { operation: "arrayAddUnique", path: 3.14, value: "new value", xattrs: true },
        { operation: "arrayAppend", path: "testArray", value: Symbol("test"), xattrs: true },
        { operation: "arrayPrepend", path: "testArray", value: BigInt(42), xattrs: true },
        { operation: "arrayInsert", path: "testArray[-9]", value: 1, xattrs: true },
        { operation: "arrayAppend", path: "testArray", value:undefined, xattrs: true },
        { operation: "arrayInsert", path: "testArray[2]", value: function() {}, xattrs: true },

    ];
    var failed=false
    // Run positive test cases
    positiveTestCases.forEach(testCase => {
        try {
            var res=callerFunction(testCase.operation,meta, testCase.path, testCase.value, testCase.xattrs);
            if ((!res.success )||(testCase.operation=="get" && !res.doc[0].success)){
                failed=true
            }
             res=callerFunction(testCase.operation,meta, testCase.path, testCase.value, !testCase.xattrs);
            if ((!res.success )){
                failed=true
            }
        } catch (error) {
            log("Positive test failed error occurred:", testCase.operation, "with value:", testCase.value, "- Error:", error,"Result: ",res);
            failed=true
        }
    });

    // Run negative test cases
    negativeTestCases.forEach(testCase => {
        try {
            var res=callerFunction(testCase.operation,meta, testCase.path, testCase.value, testCase.xattrs);
            if ((res.success && testCase.operation!="get")|| (testCase.operation=="get" && res.doc[0].success)){
                log("Negative passed was expected to fail result: ",res , " for operation: ", testCase.operation, "with value:", testCase.value)
            }
        } catch (error) {
            log("Negative test failed as expected");
        }
    });
    if (!failed){
        dst_bucket[meta.id+"test"]="success"
    }
}
function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}



