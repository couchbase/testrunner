function OnUpdate(doc, meta) {
    var sampleASCIIString="";
    for (let i = 0; i < 128; i++) {
        sampleASCIIString += String.fromCharCode(i);
    }
    var failWholeTest=false;
    var result = couchbase.crc_64_go_iso(sampleASCIIString);
    if (result != "681110f096387b8c") {
        failWholeTest=true
        log("Incorrect result for ASCII string");
    }

    result = couchbase.crc_64_go_iso(1);
    if (result != "4320000000000000") {
        failWholeTest=true
        log("Incorrect result for integer")
    }

    result = couchbase.crc_64_go_iso([1,2,3,4]);
    if (result != "3b8002901291a293") {
        failWholeTest=true
        log("Incorrect result for array");
    }

    result = couchbase.crc_64_go_iso({"my_key":"my_value"});
    if (result != "756fe1c0128e6be0") {
        failWholeTest=true
        log("Incorrect result for JSON")
    }

    result = couchbase.crc_64_go_iso(null);
    if (result != "3eeef9ddb0000000") {
        failWholeTest=true
        log("Incorrect result for null");
    }

    result = couchbase.crc_64_go_iso(false);
    if (result != "32cc7ee410300000") {
        failWholeTest=true
         log("Incorrect result for boolean");
    }

    result = couchbase.crc_64_go_iso(undefined);
    if (result != "fcd0dd899002d36d") {
        failWholeTest=true
         log("Incorrect result for undefined");
    }

    // JSON stringify of any Symbol gives undefined, so CRC of Symbol is equivalent to CRC of undefined
    result = couchbase.crc_64_go_iso(Symbol("hey"));
    if (result != "fcd0dd899002d36d") {
        failWholeTest=true
         log("Incorrect result for symbol");
    }

    result = couchbase.crc_64_go_iso(new Date("2024-05-10"))
    if (result != "508adbb2e62fe363") {
        failWholeTest=true
         log("Incorrect result for date");
    }

    // BigInt datatype gives TypeError when passed as an argument to crc function
    try {
        couchbase.s(12345678901234567890123456123456n);
    } catch (err) {
        if (!(err instanceof TypeError)) {
           failWholeTest=true
         log("Test should FAIl for BigInt");
        }
    }
    validateBucketGet(meta);
    if (!failWholeTest){
        dst_bucket[meta.id]="success";
    }


}

function OnDelete(meta, options) {
    delete dst_bucket[meta.id];
    log("Doc deleted/expired", meta.id);
}

function validateBucketGet(meta) {
    var val = src_bucket[meta.id];

    if (val === null || val === undefined) {
        throw new Error("GET failed: document not found for key " + meta.id);
    }

    log("Bucket GET successful for key:", meta.id);
    return val;
}