import json
from .tuq import QueryTests

class QueryInferTests(QueryTests):
    def setUp(self):
        super(QueryInferTests, self).setUp()
        self.log.info("==============  QueryInferTests setup has started ==============")
        # Optionally load test data here if needed
        self.log.info("==============  QueryInferTests setup has completed ==============")

    def suite_setUp(self):
        super(QueryInferTests, self).suite_setUp()
        self.log.info("==============  QueryInferTests suite_setup has started ==============")
        self.log.info("==============  QueryInferTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryInferTests tearDown has started ==============")
        # Optionally clean up test data here if needed
        super(QueryInferTests, self).tearDown()
        self.log.info("==============  QueryInferTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryInferTests suite_tearDown has started ==============")
        super(QueryInferTests, self).suite_tearDown()
        self.log.info("==============  QueryInferTests suite_tearDown has completed ==============")

    def _load_test_data(self):
        # Insert documents with arrays and nested objects for inference
        docs = []
        # Document with array of arrays
        docs.append({
            "id": "array-arrays",
            "parent-to-an-array-of-arrays": {
                "array-of-arrays": [
                    [
                        {"c1": x, "c2": str(x)}
                        for x in range(11)
                    ] for y in range(7)
                ]
            }
        })
        # Document with deep nesting
        docs.append({
            "id": "deep-nesting",
            "a": 1,
            "b": {
                "c": 1,
                "d": {
                    "e": 1,
                    "f": {
                        "g": 2
                    }
                }
            }
        })
        # Document with nested arrays
        docs.append({
            "id": "nested-arrays",
            "arr": [
                {
                    "x": [
                        {
                            "y": [
                                {"z": i}
                                for i in range(11)
                            ]
                        } for j in range(6)
                    ]
                } for k in range(4)
            ]
        })

        # Insert documents into the default bucket
        for doc in docs:
            self.run_cbq_query(
                f'INSERT INTO default (KEY, VALUE) VALUES ("{doc["id"]}", {json.dumps(doc)})'
            )

    def test_infer_array_sample_size(self):
        """
        Test INFER with array_sample_size option to limit array samples in type inference.
        """
        query = '''
        INFER {"parent-to-an-array-of-arrays":
            { "array-of-arrays": ARRAY ARRAY {"c1":x, "c2": TO_STR(x)} FOR x IN ARRAY_RANGE(0,10) END FOR y IN ARRAY_RANGE(0,6) END }
        } WITH {"array_sample_size":2}
        '''
        result = self.run_cbq_query(query)
        self.log.info("INFER result with array_sample_size=2: %s", result['results'])

        infer_info = result['results'][0][0]['properties']['parent-to-an-array-of-arrays']['properties']['array-of-arrays']
        self.assertIn('sampleSize', infer_info, "sampleSize attribute should be present")
        self.assertEqual(infer_info['sampleSize'], 2, "sampleSize should be 2")

        # Check that samples array contains 2 array elements of size 2
        self.assertIn('samples', infer_info, "samples attribute should be present")
        samples = infer_info['samples'][0]
        self.assertIsInstance(samples, list, "samples should be an array")
        self.assertEqual(len(samples), 2, "samples should contain 2 array elements")
        
        for i, sample in enumerate(samples):
            self.assertIsInstance(sample, list, f"sample[{i}] should be an array")
            self.assertEqual(len(sample), 2, f"sample[{i}] should contain 2 elements")
            for j, element in enumerate(sample):
                self.assertIsInstance(element, dict, f"sample[{i}][{j}] should be a dictionary")
                self.assertIn('c1', element, f"sample[{i}][{j}] should have 'c1' key")
                self.assertIn('c2', element, f"sample[{i}][{j}] should have 'c2' key")
                self.assertIsInstance(element['c1'], int, f"sample[{i}][{j}]['c1'] should be an integer")
                self.assertIsInstance(element['c2'], str, f"sample[{i}][{j}]['c2'] should be a string")

    def test_infer_max_nesting_depth(self):
        """
        Test INFER with max_nesting_depth option to limit nesting depth in type inference.
        """
        query = '''
        INFER {"a":1, "b":{"c":1, "d":{"e":1}}} WITH {"max_nesting_depth":1}
        '''
        result = self.run_cbq_query(query)
        self.log.info("INFER result with max_nesting_depth=2: %s", result['results'])
        # Check that nestingDepth attribute is present and correct
        props = result['results'][0][0]['properties']
        self.assertEqual(props['a']['nestingDepth'], 0)
        self.assertEqual(props['b']['nestingDepth'], 0)
        self.assertEqual(props['b']['properties']['c']['nestingDepth'], 1)
        self.assertEqual(props['b']['properties']['d']['nestingDepth'], 1)
        # "e" should not be present because max_nesting_depth=1 (0,1 allowed, >1 not)
        self.assertNotIn('e', props['b']['properties']['d']['properties'])

    def test_infer_both_options(self):
        """
        Test INFER with both array_sample_size and max_nesting_depth options together.
        """
        query = '''
        INFER {
            "arr": ARRAY {"x": ARRAY {"y": ARRAY {"z": i} FOR i IN ARRAY_RANGE(0,10) END} FOR j IN ARRAY_RANGE(0,5) END} FOR k IN ARRAY_RANGE(0,3) END
        } WITH {"array_sample_size": 2, "max_nesting_depth": 4}
        '''
        result = self.run_cbq_query(query)
        self.log.info("INFER result with both options: %s", result['results'])
        arr_info = result['results'][0][0]['properties']['arr']
        self.assertIn('sampleSize', arr_info)
        self.assertEqual(arr_info['sampleSize'], 2)
        self.assertEqual(arr_info['nestingDepth'], 0)
        # Check that nested arrays have correct nestingDepth and are trimmed
        x_info = arr_info['items']['properties']['x']
        self.assertEqual(x_info['nestingDepth'], 2)
        y_info = x_info['items']['properties']['y']
        self.assertEqual(y_info['nestingDepth'], 4)
        # "z" should not be present because max_nesting_depth=4
        self.assertEqual(y_info['items'], {})
        # check that the samples array contains 2 array elements of size 2
        self.assertIn('samples', arr_info)
        samples = arr_info['samples'][0]
        self.assertIsInstance(samples, list, "samples should be an array")
        self.assertEqual(len(samples), 2, "samples should contain 2 array elements")
        for i, sample in enumerate(samples):
            self.assertIsInstance(sample['x'], list, f"sample[{i}] should be an array")
            self.assertEqual(len(sample['x']), 2, f"sample[{i}] should contain 2 elements")

    def test_infer_parameters(self):
        """
        Test INFER with undefined named or positional parameter
        Ensure INFER does not panic, but returns error.
        """
        # Test with undefined positional parameter
        query_positional = "INFER $1"
        try:
            result = self.run_cbq_query(query_positional)
            # We expect an error, so if no exception, the test should fail
            self.fail("INFER with undefined positional parameter should return error, but returned: %s" % result)
        except Exception as ex:
            # Should return error message about missing/incomplete parameter(s)
            error_msg = str(ex)
            self.assertTrue("No value for positional parameter" in error_msg or
                            f"Expected error about missing parameter, got: {error_msg}")

        # Test with undefined named parameter
        query_named = "INFER $bucket"
        try:
            result = self.run_cbq_query(query_named)
            self.fail("INFER with undefined named parameter should return error, but returned: %s" % result)
        except Exception as ex:
            error_msg = str(ex)
            self.assertTrue("No value for named parameter" in error_msg or
                            f"Expected error about missing parameter, got: {error_msg}")