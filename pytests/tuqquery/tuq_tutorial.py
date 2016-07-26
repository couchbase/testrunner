import pprint
import logger
import copy
import urllib
import httplib2
import socket
import requests, json
import testconstants
import time
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection


class TuqTutorialTests(BaseTestCase):

    def _create_headers(self):
        authorization = ""
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def _http_request(self, api, method='GET', params='', headers=None, timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError as e:
                        json_parsed = {}
                        json_parsed["error"] = "status: {0}, content: {1}".format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    self.log.error('{0} error {1} reason: {2} {3}'.format(api, response['status'], reason, content.rstrip('\n')))
                    return False, content, response
            except socket.error as e:
                self.log.error("socket error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise Exception("nothing")
            time.sleep(3)

    def test_expample_1(self):
        """
           html encode for some spectial characters:
           char   html
           %       %25
           """
        api = "http://query.pub.couchbase.com/query"
        queries = {"1":"SELECT 'Hello World' AS Greeting",
                  "2":"SELECT * FROM tutorial WHERE fname = 'Ian'",
                  "3":"SELECT children[0].fname AS cname FROM tutorial WHERE fname='Dave'",
                  "4":"SELECT META(tutorial) AS meta FROM tutorial",
                  "5":"SELECT fname, age, age/7 AS age_dog_years FROM tutorial WHERE fname = 'Dave'",
                  "6":"SELECT fname, age, ROUND(age/7) AS age_dog_years FROM tutorial WHERE fname = 'Dave'",
                  "7":"SELECT fname || ' ' || lname AS full_name FROM tutorial",
                  "8":"SELECT fname, age FROM tutorial WHERE age > 30",
                  "9":"SELECT fname, email FROM tutorial WHERE email LIKE '%25@yahoo.com'",
                  "10":"SELECT DISTINCT orderlines[0].productId FROM orders",
                  "11":"SELECT fname, children FROM tutorial WHERE children IS NULL",
                  "12":"SELECT fname, children FROM tutorial WHERE ANY child IN tutorial.children SATISFIES child.age > 10  END",
                  "13":"SELECT fname, email, children FROM tutorial WHERE ARRAY_LENGTH(children) > 0 AND email LIKE '%25@gmail.com'",
                  "14":"SELECT fname, email FROM tutorial USE KEYS ['dave', 'ian']",
                  "15":"SELECT children[0:2] FROM tutorial WHERE children[0:2] IS NOT MISSING",
                  "16":"SELECT fname || ' ' || lname AS full_name, email, children[0:2] AS offsprings FROM tutorial WHERE email LIKE '%25@yahoo.com' OR ANY child IN tutorial.children SATISFIES child.age > 10 END",
                  "17":"SELECT fname, age FROM tutorial ORDER BY age",
                  "18":"SELECT fname, age FROM tutorial ORDER BY age LIMIT 2",
                  "19":"SELECT COUNT(*) AS count FROM tutorial",
                  "20":"SELECT relation, COUNT(*) AS count FROM tutorial GROUP BY relation",
                  "21":"SELECT relation, COUNT(*) AS count FROM tutorial GROUP BY relation HAVING COUNT(*) > 1",
                  "22":"SELECT ARRAY child.fname FOR child IN tutorial.children END AS children_names FROM tutorial WHERE children IS NOT NULL",
                  "23":"SELECT t.relation, COUNT(*) AS count, AVG(c.age) AS avg_age FROM tutorial t UNNEST t.children c WHERE c.age > 10 GROUP BY t.relation HAVING COUNT(*) > 1 ORDER BY avg_age DESC LIMIT 1 OFFSET 1",
                  "24":"SELECT usr.personal_details, orders FROM users_with_orders usr USE KEYS 'Elinor_33313792' JOIN orders_with_users orders ON KEYS ARRAY s.order_id FOR s IN usr.shipped_order_history END",
                  "25":"SELECT usr.personal_details, orders FROM users_with_orders usr USE KEYS 'Tamekia_13483660' LEFT JOIN orders_with_users orders ON KEYS ARRAY s.order_id FOR s IN usr.shipped_order_history END",
                  "26":"SELECT usr.personal_details, orders FROM users_with_orders usr USE KEYS 'Elinor_33313792' NEST orders_with_users orders ON KEYS ARRAY s.order_id FOR s IN usr.shipped_order_history END",
                  "27":"SELECT * FROM tutorial AS contact UNNEST contact.children WHERE contact.fname = 'Dave'",
                  "28":"SELECT  u.personal_details.display_name name, s AS order_no, o.product_details FROM users_with_orders u USE KEYS 'Aide_48687583' UNNEST u.shipped_order_history s JOIN users_with_orders o ON KEYS s.order_id",
                  "29":"EXPLAIN INSERT INTO tutorial (KEY, VALUE) VALUES ('baldwin', {'name':'Alex Baldwin', 'type':'contact'})",
                  "30":"EXPLAIN DELETE FROM tutorial t USE KEYS 'baldwin' RETURNING t",
                  "31":"EXPLAIN UPDATE tutorial USE KEYS \"baldwin\" SET type = \"actor\" RETURNING tutorial.type",
                  "32":"SELECT COUNT(*) AS product_count FROM product",
                  "33":"SELECT * FROM product UNNEST product.categories AS cat WHERE LOWER(cat) in [\"golf\"] LIMIT 10 OFFSET 10"}

        results = {"1":"[{u'Greeting': u'Hello World'}]",
                   "2":"[{u'tutorial': {u'title': u'Mr.', u'age': 56, u'email': u'ian@gmail.com', u'lname': u'Taylor', u'relation': u'cousin', u'fname': u'Ian', u'hobbies': [u'golf', u'surfing'], u'type': u'contact', u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Abama'}, {u'gender': u'm', u'age': 21, u'fname': u'Bebama'}]}}]",
                   "3":"[{u'cname': u'Aiden'}]",
                   "4":"[{u'meta': {u'id': u'dave'}}, {u'meta': {u'id': u'earl'}}, {u'meta': {u'id': u'fred'}}, {u'meta': {u'id': u'harry'}}, {u'meta': {u'id': u'ian'}}, {u'meta': {u'id': u'jane'}}]",
                   "5":"[{u'age_dog_years': 6.571428571428571, u'age': 46, u'fname': u'Dave'}]",
                   "6":"[{u'age_dog_years': 7, u'age': 46, u'fname': u'Dave'}]",
                   "7":"[{u'full_name': u'Dave Smith'}, {u'full_name': u'Earl Johnson'}, {u'full_name': u'Fred Jackson'}, {u'full_name': u'Harry Jackson'}, {u'full_name': u'Ian Taylor'}, {u'full_name': u'Jane Edwards'}]",
                   "8":"[{u'age': 46, u'fname': u'Dave'}, {u'age': 46, u'fname': u'Earl'}, {u'age': 56, u'fname': u'Ian'}, {u'age': 40, u'fname': u'Jane'}]",
                   "9":"[{u'email': u'harry@yahoo.com', u'fname': u'Harry'}]",
                   "10":"[{u'productId': u'coffee01'}, {u'productId': u'tea111'}]",
                   "11":"[{u'children': None, u'fname': u'Fred'}]",
                   "12":"[{u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Aiden'}, {u'gender': u'f', u'age': 2, u'fname': u'Bill'}], u'fname': u'Dave'}, {u'children': [{u'gender': u'f', u'age': 17, u'fname': u'Xena'}, {u'gender': u'm', u'age': 2, u'fname': u'Yuri'}], u'fname': u'Earl'}, {u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Abama'}, {u'gender': u'm', u'age': 21, u'fname': u'Bebama'}], u'fname': u'Ian'}]",
                   "13":"[{u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Aiden'}, {u'gender': u'f', u'age': 2, u'fname': u'Bill'}], u'fname': u'Dave', u'email': u'dave@gmail.com'}, {u'children': [{u'gender': u'f', u'age': 17, u'fname': u'Xena'}, {u'gender': u'm', u'age': 2, u'fname': u'Yuri'}], u'fname': u'Earl', u'email': u'earl@gmail.com'}, {u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Abama'}, {u'gender': u'm', u'age': 21, u'fname': u'Bebama'}], u'fname': u'Ian', u'email': u'ian@gmail.com'}]",
                   "14":"[{u'email': u'dave@gmail.com', u'fname': u'Dave'}, {u'email': u'ian@gmail.com', u'fname': u'Ian'}]",
                   "15":"[{u'$1': [{u'gender': u'm', u'age': 17, u'fname': u'Aiden'}, {u'gender': u'f', u'age': 2, u'fname': u'Bill'}]}, {u'$1': [{u'gender': u'f', u'age': 17, u'fname': u'Xena'}, {u'gender': u'm', u'age': 2, u'fname': u'Yuri'}]}, {u'$1': None}, {u'$1': [{u'gender': u'm', u'age': 17, u'fname': u'Abama'}, {u'gender': u'm', u'age': 21, u'fname': u'Bebama'}]}]",
                   "16":"[{u'email': u'dave@gmail.com', u'full_name': u'Dave Smith', u'offsprings': [{u'gender': u'm', u'age': 17, u'fname': u'Aiden'}, {u'gender': u'f', u'age': 2, u'fname': u'Bill'}]}, {u'email': u'earl@gmail.com', u'full_name': u'Earl Johnson', u'offsprings': [{u'gender': u'f', u'age': 17, u'fname': u'Xena'}, {u'gender': u'm', u'age': 2, u'fname': u'Yuri'}]}, {u'email': u'harry@yahoo.com', u'full_name': u'Harry Jackson'}, {u'email': u'ian@gmail.com', u'full_name': u'Ian Taylor', u'offsprings': [{u'gender': u'm', u'age': 17, u'fname': u'Abama'}, {u'gender': u'm', u'age': 21, u'fname': u'Bebama'}]}]",
                   "17":"[{u'age': 18, u'fname': u'Fred'}, {u'age': 20, u'fname': u'Harry'}, {u'age': 40, u'fname': u'Jane'}, {u'age': 46, u'fname': u'Dave'}, {u'age': 46, u'fname': u'Earl'}, {u'age': 56, u'fname': u'Ian'}]",
                   "18":"[{u'age': 18, u'fname': u'Fred'}, {u'age': 20, u'fname': u'Harry'}]",
                   "19":"[{u'count': 6}]",
                   "20":"[{u'count': 1, u'relation': u'parent'}, {u'count': 2, u'relation': u'cousin'}, {u'count': 2, u'relation': u'friend'}, {u'count': 1, u'relation': u'coworker'}]",
                   "21":"[{u'count': 2, u'relation': u'cousin'}, {u'count': 2, u'relation': u'friend'}]",
                   "22":"[{u'children_names': [u'Aiden', u'Bill']}, {u'children_names': [u'Xena', u'Yuri']}, {u'children_names': [u'Abama', u'Bebama']}]",
                   "23":"[{u'count': 2, u'avg_age': 17, u'relation': u'friend'}]",
                   "24":"[{u'orders': {u'doc_type': u'order', u'user_id': u'Elinor_33313792', u'shipping_details': {u'shipping_type': u'Express', u'shipping_status': u'Delivered', u'shipping_charges': 5}, u'order_details': {u'order_id': u'T103929516925', u'order_datetime': u'Wed Jun  6 18:53:39 2012'}, u'product_details': {u'currency': u'EUR', u'sale_price': 303, u'list_price': 318, u'pct_discount': 5, u'product_id': u'P3109994453'}, u'payment_details': {u'payment_mode': u'Debit Card', u'total_charges': 308}}, u'personal_details': {u'first_name': u'Elinor', u'last_name': u'Ritchie', u'display_name': u'Elinor Ritchie', u'age': 60, u'state': u'Arizona', u'email': u'Elinor.Ritchie@snailmail.com'}}, {u'orders': {u'doc_type': u'order', u'user_id': u'Elinor_33313792', u'shipping_details': {u'shipping_type': u'Regular', u'shipping_status': u'Delivered', u'shipping_charges': 2}, u'order_details': {u'order_id': u'T573145204032', u'order_datetime': u'Thu Aug 11 18:53:39 2011'}, u'product_details': {u'currency': u'GBP', u'sale_price': 567, u'list_price': 666, u'pct_discount': 15, u'product_id': u'P9315874155'}, u'payment_details': {u'payment_mode': u'NetBanking', u'total_charges': 569}}, u'personal_details': {u'first_name': u'Elinor', u'last_name': u'Ritchie', u'display_name': u'Elinor Ritchie', u'age': 60, u'state': u'Arizona', u'email': u'Elinor.Ritchie@snailmail.com'}}]",
                   "25":"[{u'personal_details': {u'first_name': u'Tamekia', u'last_name': u'Akin', u'display_name': u'Tamekia Akin', u'age': 39, u'state': u'Massachusetts', u'email': u'Tamekia.Akin@snailmail.com'}}]",
                   "26":"[{u'orders': [{u'doc_type': u'order', u'user_id': u'Elinor_33313792', u'shipping_details': {u'shipping_type': u'Express', u'shipping_status': u'Delivered', u'shipping_charges': 5}, u'order_details': {u'order_id': u'T103929516925', u'order_datetime': u'Wed Jun  6 18:53:39 2012'}, u'product_details': {u'currency': u'EUR', u'sale_price': 303, u'list_price': 318, u'pct_discount': 5, u'product_id': u'P3109994453'}, u'payment_details': {u'payment_mode': u'Debit Card', u'total_charges': 308}}, {u'doc_type': u'order', u'user_id': u'Elinor_33313792', u'shipping_details': {u'shipping_type': u'Regular', u'shipping_status': u'Delivered', u'shipping_charges': 2}, u'order_details': {u'order_id': u'T573145204032', u'order_datetime': u'Thu Aug 11 18:53:39 2011'}, u'product_details': {u'currency': u'GBP', u'sale_price': 567, u'list_price': 666, u'pct_discount': 15, u'product_id': u'P9315874155'}, u'payment_details': {u'payment_mode': u'NetBanking', u'total_charges': 569}}], u'personal_details': {u'first_name': u'Elinor', u'last_name': u'Ritchie', u'display_name': u'Elinor Ritchie', u'age': 60, u'state': u'Arizona', u'email': u'Elinor.Ritchie@snailmail.com'}}]",
                   "27":"[{u'contact': {u'title': u'Mr.', u'age': 46, u'email': u'dave@gmail.com', u'lname': u'Smith', u'relation': u'friend', u'fname': u'Dave', u'hobbies': [u'golf', u'surfing'], u'type': u'contact', u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Aiden'}, {u'gender': u'f', u'age': 2, u'fname': u'Bill'}]}, u'children': {u'gender': u'm', u'age': 17, u'fname': u'Aiden'}}, {u'contact': {u'title': u'Mr.', u'age': 46, u'email': u'dave@gmail.com', u'lname': u'Smith', u'relation': u'friend', u'fname': u'Dave', u'hobbies': [u'golf', u'surfing'], u'type': u'contact', u'children': [{u'gender': u'm', u'age': 17, u'fname': u'Aiden'}, {u'gender': u'f', u'age': 2, u'fname': u'Bill'}]}, u'children': {u'gender': u'f', u'age': 2, u'fname': u'Bill'}}]",
                   "28":"[{u'name': u'Aide Swank', u'order_no': {u'order_id': u'T638751835595', u'order_datetime': u'Sat Jan  7 22:00:11 2012'}, u'product_details': {u'currency': u'USD', u'sale_price': 134, u'list_price': 178, u'pct_discount': 25, u'product_id': u'P8360066417'}}, {u'name': u'Aide Swank', u'order_no': {u'order_id': u'T870351974549', u'order_datetime': u'Fri Jan 20 22:00:11 2012'}, u'product_details': {u'currency': u'USD', u'sale_price': 171, u'list_price': 179, u'pct_discount': 5, u'product_id': u'P8589655279'}}, {u'name': u'Aide Swank', u'order_no': {u'order_id': u'T769348087819', u'order_datetime': u'Sun Feb 19 22:00:11 2012'}, u'product_details': {u'currency': u'EUR', u'sale_price': 548, u'list_price': 608, u'pct_discount': 10, u'product_id': u'P1512413007'}}, {u'name': u'Aide Swank', u'order_no': {u'order_id': u'T183428307793', u'order_datetime': u'Mon Oct 15 22:00:11 2012'}, u'product_details': {u'currency': u'USD', u'sale_price': 649, u'list_price': 683, u'pct_discount': 5, u'product_id': u'P3705096321'}}, {u'name': u'Aide Swank', u'order_no': {u'order_id': u'T703068425987', u'order_datetime': u'Sun Aug  5 22:00:11 2012'}, u'product_details': {u'currency': u'EUR', u'sale_price': 56, u'list_price': 62, u'pct_discount': 10, u'product_id': u'P7771903542'}}]",
                   "29":"[{u'text': u\"INSERT INTO tutorial (KEY, VALUE) VALUES (\'baldwin\', {\'name\':\'Alex Baldwin\', \'type\':\'contact\'})\", u'plan': {u'#operator': u'Sequence', u'~children': [{u'#operator': u'ValueScan', u'values': u'[[\"baldwin\", {\"name\": \"Alex Baldwin\", \"type\": \"contact\"}]]'}, {u'#operator': u'Parallel', u'maxParallelism': 1, u'~child': {u'#operator': u'Sequence', u'~children': [{u'alias': u'tutorial', u'#operator': u'SendInsert', u'namespace': u'default', u'limit': None, u'keyspace': u'tutorial'}, {u'#operator': u'Discard'}]}}]}}]",
                   "30":"[{u'text': u\"DELETE FROM tutorial t USE KEYS 'baldwin' RETURNING t\", u'plan': {u'#operator': u'Sequence', u'~children': [{u'keys': u'\"baldwin\"', u'#operator': u'KeyScan'}, {u'#operator': u'Parallel', u'maxParallelism': 1, u'~child': {u'#operator': u'Sequence', u'~children': [{u'keyspace': u'tutorial', u'#operator': u'DummyFetch', u'namespace': u'default', u'as': u't'}, {u'alias': u't', u'#operator': u'SendDelete', u'namespace': u'default', u'limit': None, u'keyspace': u'tutorial'}, {u'#operator': u'InitialProject', u'result_terms': [{u'expr': u'`t`'}]}, {u'#operator': u'FinalProject'}]}}]}}]",
                   "31":"[{u'text': u'UPDATE tutorial USE KEYS \"baldwin\" SET type = \"actor\" RETURNING tutorial.type', u'plan': {u'#operator': u'Sequence', u'~children': [{u'keys': u'\"baldwin\"', u'#operator': u'KeyScan'}, {u'#operator': u'Parallel', u'maxParallelism': 1, u'~child': {u'#operator': u'Sequence', u'~children': [{u'keyspace': u'tutorial', u'#operator': u'Fetch', u'namespace': u'default'}, {u'#operator': u'Clone'}, {u'#operator': u'Set', u'set_terms': [{u'path': u'(`tutorial`.`type`)', u'value': u'\"actor\"'}]}, {u'alias': u'tutorial', u'#operator': u'SendUpdate', u'namespace': u'default', u'limit': None, u'keyspace': u'tutorial'}, {u'#operator': u'InitialProject', u'result_terms': [{u'expr': u'(`tutorial`.`type`)'}]}, {u'#operator': u'FinalProject'}]}}]}}]",
                   "32":"[{u'product_count': 900}]",
                   "33":"[{u'product': {u'dateAdded': u'2014-04-06T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B008KYGUZW/ref=s9_hps_bw_g200_ir011?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'plum', u'unitPrice': 54, u'imageURL': u'http://ecx.images-amazon.com/images/I/41dEDQdCohL._SL300_.jpg', u'reviewList': [u'review167', u'review1056', u'review1995', u'review3122', u'review4301', u'review4719', u'review4931', u'review5857', u'review7365', u'review8122'], u'productId': u'product851', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Wellzher 2012 Lite Stand Golf Bag'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-08-09T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B006LMH2Q6/ref=s9_hps_bw_g200_ir012?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'lime', u'unitPrice': 99.99, u'imageURL': u'http://ecx.images-amazon.com/images/I/41hQ6iO%2ByEL._SL300_.jpg', u'reviewList': [u'review796', u'review1173', u'review2655', u'review3040', u'review3798', u'review4851', u'review8107'], u'productId': u'product852', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Nike Golf Xtreme Sport IV Golf Bag'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-09-08T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B001IDTC4S/ref=s9_hps_bw_g200_ir013?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'blue', u'unitPrice': 74.99, u'imageURL': u'http://ecx.images-amazon.com/images/I/31SSqAW36aL._SL300_.jpg', u'reviewList': [u'review113', u'review1469', u'review2146', u'review2955', u'review5689', u'review7193', u'review8319', u'review9225'], u'productId': u'product853', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Golf Girl LEFTY Junior Club Set for Kids Ages 4-7 w/Pink Stand Bag'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-08-09T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B009FVFAFA/ref=s9_hps_bw_g200_ir014?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'blue', u'unitPrice': 123.03, u'imageURL': u'http://ecx.images-amazon.com/images/I/51HkYiWWPLL._SL300_.jpg', u'reviewList': [u'review316', u'review414', u'review1380', u'review1453', u'review2348', u'review3087', u'review4327', u'review4983', u'review6913', u'review6925', u'review7370'], u'productId': u'product854', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Callaway XTT Xtreme Stand Bag'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-06-10T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B0055NW7OE/ref=s9_hps_bw_g200_ir015?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'white', u'unitPrice': 3.99, u'imageURL': u'http://ecx.images-amazon.com/images/I/41rziEf82GL._SL300_.jpg', u'reviewList': [u'review124', u'review1717', u'review2290', u'review2874', u'review3258', u'review4423', u'review4495', u'review5614', u'review5628', u'review7158', u'review8649'], u'productId': u'product855', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Pride Golf Tee - 3-1/4 inch Deluxe Tee - 75 Count,Black)'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-09-08T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B0000AUBX7/ref=s9_hps_bw_g200_ir016?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'gold', u'unitPrice': 13.78, u'imageURL': u'http://ecx.images-amazon.com/images/I/31tiAJYeukL._SL300_.jpg', u'reviewList': [u'review627', u'review1204', u'review3645', u'review4455', u'review7062', u'review7846', u'review7923', u'review8343', u'review8757'], u'productId': u'product856', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Winn Excel Medium Midsize Grip'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-10-08T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B0036URAJ0/ref=s9_hps_bw_g200_ir017?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'red', u'unitPrice': 10.37, u'imageURL': u'http://ecx.images-amazon.com/images/I/41yclC%2ByfwL._SL300_.jpg', u'reviewList': [u'review81', u'review3843'], u'productId': u'product857', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Nitro Blaster 12CT White Golf Ball'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2014-01-06T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B004GCIJCO/ref=s9_hps_bw_g200_ir018?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'sky blue', u'unitPrice': 55.99, u'imageURL': u'http://ecx.images-amazon.com/images/I/21dOq2ky4ML._SL300_.jpg', u'reviewList': [u'review122', u'review246', u'review314', u'review329', u'review373', u'review1652', u'review3678', u'review4296', u'review7123', u'review7742', u'review8504', u'review9200'], u'productId': u'product858', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Nike Golf Victory Red Pro Hybrids'}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-11-07T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B0047N02D6/ref=s9_hps_bw_g200_ir019?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'azure', u'unitPrice': 79.95, u'imageURL': u'http://ecx.images-amazon.com/images/I/31w1kosf1hL._SL300_.jpg', u'reviewList': [u'review603', u'review2444', u'review2624', u'review2996', u'review3364', u'review4540', u'review5403', u'review6323', u'review6700', u'review7022', u'review7469', u'review8273'], u'productId': u'product859', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u\"Callaway Men's RAZR X Hybrids\"}, u'cat': u'Golf'}, {u'product': {u'dateAdded': u'2013-08-09T15:52:19Z', u'description': u'This product is available on <a target=\"_blank\" href=\"http://www.amazon.com/gp/product/B001P303YY/ref=s9_hps_bw_g200_ir020?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-10&pf_rd_r=AF4356F23718450681C3&pf_rd_t=101&pf_rd_p=1532316542&pf_rd_i=3410851\">Amazon.com</a>.', u'color': u'plum', u'unitPrice': 14.99, u'imageURL': u'http://ecx.images-amazon.com/images/I/51mMk8BipAL._SL300_.jpg', u'reviewList': [u'review249', u'review1001', u'review1423', u'review4515', u'review4890', u'review4932', u'review5528', u'review5818', u'review8843', u'review8962'], u'productId': u'product860', u'type': u'product', u'dateModified': u'2014-05-06T15:52:19Z', u'categories': [u'Golf'], u'name': u'Dixon Earth Golf Balls (One Dozen)'}, u'cat': u'Golf'}]"}
        errors = []
        status = []
        run_keys = ["33"]
        """ curl http://query.pub.couchbase.com/query -d "statement=SELECT 'Hello World' AS Greeting" | python -c 'import sys, json; print json.load(sys.stdin)'
            output:  [{u'Greeting': u'Hello World'}] """
        for key in queries.keys():
            if run_keys:
                for run_key in run_keys:
                    if run_key == key:
                        self.log.info("run query %s: %s " % (key, queries[key]))
                        params = "statement=%s" % queries[key]
                        status, content, header = self._http_request(api, 'POST', params)
                        json_parsed = json.loads(content)
                        if "errors" in json_parsed.keys():
                            errors.append(json_parsed["errors"])
                            print "error  ", json_parsed["errors"]
                        if "results" in json_parsed.keys():
                            print "KEY ****** :  ", key
                            print "result from web:  ", json_parsed["results"]
                            print "result must be:   ", results[key]
                            if str(json_parsed["results"]) == results[key].strip():
                                print "test passed"
                            else:
                                print "ERROR **************"
            else:
                self.log.info("run query %s: %s " % (key, queries[key]))
                params = "statement=%s" % queries[key]
                status, content, header = self._http_request(api, 'POST', params)
                json_parsed = json.loads(content)
                if "errors" in json_parsed.keys():
                    errors.append(json_parsed["errors"])
                    print "error  ", json_parsed["errors"]
                if "results" in json_parsed.keys():
                    print "KEY ****** :  ", key
                    print "result from web:  ", json_parsed["results"]
                    print "result must be:   ", results[key]
                    if str(json_parsed["results"]) == results[key].strip():
                        print "test passed"
                    else:
                        print "ERROR **************"
