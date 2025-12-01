from .tuq import QueryTests
import hashlib
import json
import binascii
import subprocess

class QueryHashBytesTests(QueryTests):
    def setUp(self):
        super(QueryHashBytesTests, self).setUp()
        self.bucket = "default"
        self.algorithm = self.input.param("algorithm", "MD5")
        self.polynomial = self.input.param("polynomial", "IEEE")
        self.value = self.input.param("value", "Hello World!")

    def suite_setUp(self):
        super(QueryHashBytesTests, self).suite_setUp()

    def tearDown(self):
        super(QueryHashBytesTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryHashBytesTests, self).suite_tearDown()

    def hashbytes(self, value, algorithm):
        if type(value) == dict:
            string = json.dumps(value, separators=(',', ':'), sort_keys=True)
        elif type(value) in [int, float]:
            string = str(value)
        else:
            string = value
        if algorithm.upper() == "MD4":
            cmd = subprocess.getoutput(f'/bin/echo -n {string} | openssl md4 -provider legacy')
            return cmd.replace("MD4(stdin)= ", "")
        if algorithm.upper() == "MD5":
            return hashlib.md5(string.encode()).hexdigest()
        if algorithm.upper() == "SHA224":
            return hashlib.sha224(string.encode()).hexdigest()
        if algorithm.upper() == "SHA256":
            return hashlib.sha256(string.encode()).hexdigest()
        if algorithm.upper() == "SHA384":
            return hashlib.sha384(string.encode()).hexdigest()
        if algorithm.upper() == "SHA512":
            return hashlib.sha512(string.encode()).hexdigest()
        if algorithm.upper() == "CRC32":
            return hex(binascii.crc32(string.encode()))[2:]
        
    def test_hash_default(self):
        expected_hash = self.hashbytes(self.value, "sha256")
        
        result = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}")')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

        result = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", {{}})')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

        result = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", {{"invalid":"abc"}})')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_string(self):
        expected_hash = self.hashbytes(self.value, self.algorithm)
        
        result = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", "{self.algorithm.lower()}" )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

        result = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", "{self.algorithm.upper()}" )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

        result = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", {{"algorithm": "{self.algorithm.lower()}"}} )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_poly(self):
        poly_map = {
            "IEEE": "0xedb88320",
            "Castagnoli": "0x82f63b78",
            "Koopman": "0xeb31d82e"
        }
        result1 = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", {{"algorithm": "crc32", "polynomial": "{self.polynomial}"}} )')
        result2 = self.run_cbq_query(f'SELECT RAW hashbytes("{self.value}", {{"algorithm": "crc32", "polynomial": "{poly_map[self.polynomial]}"}} )')
        self.log.info(f"Hash using {self.polynomial}: {result1['results'][0]}")
        self.log.info(f"Hash using {poly_map[self.polynomial]}: {result2['results'][0]}")
        self.assertEqual(result1['results'][0], result2['results'][0])

    def test_hash_null(self):
        expected_hash = None
        result = self.run_cbq_query(f'SELECT RAW hashbytes(null)')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_number(self):
        expected_hash = self.hashbytes(self.value, self.algorithm)
        
        result = self.run_cbq_query(f'SELECT RAW hashbytes({self.value}, "{self.algorithm.lower()}" )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_json(self):
        value = json.loads(self.value)
        expected_hash = self.hashbytes(value, self.algorithm)
        
        result = self.run_cbq_query(f'SELECT RAW hashbytes({value}, "{self.algorithm.upper()}" )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_missing(self):
        expected_hash = None
        result = self.run_cbq_query('SELECT RAW hashbytes(t.b) FROM [{"a":1}] as t')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_bool(self):
        expected_hash = self.hashbytes("true", self.algorithm)
        result = self.run_cbq_query(f'SELECT RAW hashbytes(t.bool, "{self.algorithm}") FROM [{{"bool":True}}] as t')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_param_invalid(self):
        result = self.run_cbq_query('SELECT RAW hashbytes("Hello World!", "invalid" )')
        self.assertEqual(result['results'][0], None)

        result = self.run_cbq_query('SELECT RAW hashbytes("Hello World!", {"algorithm":"invalid"})')
        self.assertEqual(result['results'][0], None)

        result = self.run_cbq_query('SELECT RAW hashbytes("Hello World!", 12345 )')
        self.assertEqual(result['results'][0], None)

        result = self.run_cbq_query('SELECT RAW hashbytes("Hello World!", "SHA512/220" )')
        self.assertEqual(result['results'][0], None)

    def test_hash_sha512_224(self):
        expected_hash = "ba0702dd8dd23280b617ef288bcc7e276060b8ebcddf28f8e4356eae"
        result = self.run_cbq_query(f'SELECT RAW hashbytes("Hello World!", "SHA512/224" )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)

    def test_hash_sha512_256(self):
        expected_hash = "f371319eee6b39b058ec262d4e723a26710e46761301c8b54c56fa722267581a"
        result = self.run_cbq_query(f'SELECT RAW hashbytes("Hello World!", "SHA512/256" )')
        actual_hash = result['results'][0]
        self.assertEqual(expected_hash, actual_hash)
