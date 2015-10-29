import json
import requests
import sys
import os
from elasticsearch import *
#from fts_base import *
import logger
import collections
import threading
from pyes import *

class ES_Index_Generator(object):

    def __init__(self,mapping,mapping_name):
        self.__mapping={
                       "string" : {
                           "tag" : [] ,
                           "analyzer":[],
                           "index":[]
                       },
                       "number":  {
                           "tag" : [] ,
                           "analyzer":[],
                           "index":[]
                       }
        }

    def get_mapping(self):
        return self.__mapping

    def set_mapping(self,mapping):

        data=json.load(mapping)
        if data["mapping"]["string"].keys():
           '''
           docmapping.add_property(
           StringField(name="parsedtext", store=True, term_vector="with_
           positions_offsets", index="analyzed"))
           '''
           for name,term_vector,index in  \
                   map(data["mapping"]["string"]["tag"],data["mapping"]
                   ["string"]["analyzer"],data["mapping"]["string"]["index"]):
               mapstr = "name="+name+", store=True, term_vector= "+term_vector+", index="+index
               docmapping.add_property(StringField(mapstr))

        if data["mapping"]["int"].keys():
            pass




class ES_Query:

    def __init__(self):
        self.__query_type_store={}
        self.__query={'query': {} }
        self.__wildcard_store=[]
        self.__query_type_store['match']=\
            collections.namedtuple('match','query boost freuency')
        self.__query_type_store['match_phrase']=\
            collections.namedtuple('match_phrase','query analyzer')
        self.__query_type_store['fuzzy'] = \
            collections.namedtuple('fuzzy','fuzzy_query')
        self.__query_type_store['wildcard'] = \
            collections.namedtuple('wildcard','tag wildcard_value boost')

    def get_query_namedtuple(self,query_type):
        try:
          return self.__query_type_store[query_type]
        except KeyError as e:
            raise (e.message)

    def get_query_builder(self,querylist,query_type):
        '''
        the input querylist  will be a namedtuple , querytype will contain the query .
         we parse the namedtuple to extract required fields .
        :param querylist:
        :param query_type:
        :return:The query string returned will be passed as search parameter
        '''
        if query_type == 'match':
            '''
            https://www.elastic.co/guide/en/elasticsearch/
                reference/current/query-dsl-match-query.html
            {
                    "match" : {
                        "message" : "this is a test"
                    }
                }
            '''
            self.__query['query']={
                'match':{ querylist.query['tag'] : querylist.query['query_string']},
            }

        elif query_type == 'match_phrase':
            '''
              {
                "match_phrase" : {
                    "message" : {
                        "query" : "this is a test",
                        "analyzer" : "my_analyzer"
                    }
                }
            }
            '''
            self.__query={
                'match':{ querylist.query['tag'] : querylist.query['query_string']},
                'analyzer' : querylist.analyzer
            }

        elif query_type == 'range':
            '''
            {
                "range" : {
                    "age" : {
                        "gte" : 10,
                        "lte" : 20,
                        "boost" : 2.0
                    }
                }
            }

            for extreme cases check with sys.maxint sys.minint
            '''
            self.__query = {
                "range" : {
                    querylist.tag : {
                        "gte" : querylist.gte,
                        "lte" : querylist.lte,
                        "boost" : querylist.boost
                    }
                }
            }

        elif query_type == 'fuzzy':
            '''
            The fuzzy query generates all possible matching terms that are within
             the maximum edit distance
             specified in fuzziness and then checks the term dictionary
             to find out which of those generated
             terms actually exist in the index.
                        {
                "fuzzy" : {
                    "user" : {
                        "value" :         "ki",
                        "boost" :         1.0,
                        "fuzziness" :     2,
                        "prefix_length" : 0,
                        "max_expansions": 100
                    }
                }
            }
            '''
            self.__query = {
                "fuzzy" : {
                    "query":querylist.fuzzy_query
                    }
                }

        elif query_type == 'regex':

            '''
                        {
                "regexp": {
                    "username": {
                        "value": "john~athon<1-5>",
                        "flags": "COMPLEMENT|INTERVAL"
                    }
                }
            }
            '''
            self.__query = {
                "regexp" : {
                    querylist.parameter : {
                        "value" : querylist.regex_value,
                        "boost" : querylist.boost,
                        "flags" : querylist.flags
                    }
                }
            }

        elif query_type == 'invalid':
             '''
             invalid query string  there is nothing called matches
             '''
             self.__query = {
              'query': {
                'filtered': {
                  'query': {
                    'matches': {'para': 'languages'}
                  }
                  }
                }
             }

        elif query_type == 'wildcard':
             '''
             wildcards are *, which matches any character sequence
             (including the empty one), and ?,
             which matches any single character
                     {
                "wildcard" : { "user" : { "value" : "ki*y", "boost" : 2.0 } }
             }
             '''
             self.__query =   {
                "wildcard" : {  querylist.tag :{ "value" : querylist.wildcard_value ,
                                                 "boost" : querylist.boost } }
             }

        elif query_type == "term":
             '''
                         {
                "terms" : {
                    "tags" : [ "blue", "pill" ],
                    "minimum_should_match" : 1
                }
             }
             '''
             self.query = {
            "terms" : {
                querylist.tags : querylist.tag_values,
                "minimum_should_match" : 1
            }}


        return self.__query

    def query_wildcard_generator(self,text):
        #case 1 start from 0 , select end point

        end=len(text)
        increment = (end - 0) / 2
        if end != -1:
           self.__wildcard_store.append(text[0]+'*'+text[end-1])
           self.__wildcard_store.append(text[0:increment]+'*'+text[end-1])
           self.__wildcard_store.append('*'+text[increment:end-1])
           self.__wildcard_store.append(text[0]+'?'+text[end-1]) #mostly -ve case
           self.__wildcard_store.append(text[0:increment]+'?'+text[increment+2:end-1])
           self.__wildcard_store.append('?'+text[1:end-1])

        return self.__wildcard_store

    def query_conjuncdisjunc_generator(self,text):
        pass


class Elastic_Search_Base(object):

      def __init__(self,hosts=[]):
         #host is in the form IP address
         self.__host=hosts
         self.__instance_id=None
         self.__document={}
         self.__mapping={}
         self.__host=[]
         self. __increment_id=0
         self.__STATUSOK=200
         self.__bulk_insert_count=0
         self.__indices=[]
         self.__index_types={}
         self.__port=9200 #port is constant
         self.__connection_url=None

      def check_ES_connection(self):
         """
         make sure ES is up and running
         check the service is running , if not abort the test
         """
         result=""
         if not self.__host:
           self.__connection_url='http://127.0.0.1:9200'
         else:
           self.__connection_url='http://'+self.host[0]+':'+str(self.port)
         try:
           print self.__connection_url
           result=requests.get(self.__connection_url,timeout=4)
         except requests.exceptions:
           return False
         if result.status_code == self.__STATUSOK:
             return True
         else:
            return False


      def connect(self):
         """

         :type self: object
         """
         try:
           """
           make sure ES is up and running
           """
           res = requests.get('http://127.0.0.1:9200')
           #host is in the form [{'host': 'localhost', 'port': 9200}]
           if not self.__host:
             #if host not defined use localhost and port 9200
             self.__instance_id = Elasticsearch()
             #print self._instance_id
           else:
            self.__instance_id = Elasticsearch(self.__connection_url)
         except Elasticsearch.ConnectionError:
           raise ElasticsearchException('Failed to connect to ES running,service is down')
         except ElasticsearchException as e:
           raise ('some exception happened')

      def disconnect(self):
         """
         need to destroy the connection.Kill all process running on port 9200
         """
         rest_disconnect="curl -XPOST 'http://"+self.__host+":9200/_shutdown'"
         os.system(rest_disconnect)

      def delete_index(self,index_name):
          # Delete index if already found one
          try:
            self.__instance_id.delete_index(index_name)
          except ElasticsearchException as e:
            raise ElasticsearchException('Problem appears when try to '
                                         'delete the index=%s %s',index_name,e.message)

      def delete_indices(self):
          '''
          Delete all indices present
          :return:Nothing
          '''
          try:
            for index_name in self.__indices:
               self.__instance_id.delete_index(index_name)
          except ElasticsearchException as e:
            raise ElasticsearchException('Problem appears when try to '
                                         'delete the index=%s %s',index_name,e.message)

      def get_instance(self):
          return self.__instance_id

      def create_empty_index(self,index_name):
          try:
            self.__instance_id.indices.create(index_name)
            self.__instance_id.cluster.health(wait_for_status="yellow")
          except:
            logger.warn('some exception happened while creating empty index')

      """
      API to store data , loaddata will update individual document
      if you need to load bulk data use the other API,
      id used in index will be autogenerated
      """
      def load_data(self,index_name,index_type,document_json):
          #if not index_name or not index_type or not document_json == 0:
              #raise Exception('indexName or indexType or document can not be NULL')

          if not self.__instance_id.indices.exists(index_name):
               self.__indices.append(index_name)
          self.__document=document_json

          try :
               result=self.__instance_id.index(index = index_name,
                            doc_type = index_type,
                            id = self.__increment_id,
                            body = document_json)
               if result['created'] == False:
                   raise Exception('Insertion failed for index= %s',indexName)
               self.__increment_id += 1
          except ElasticsearchException ,e:
                raise ElasticsearchException(e.message)

      """
         If you want load bulk data , iterator or file use this API
        Please mention the input type either file or document
      """
      def load_data_bulk(self,index_name,index_type,bulk_data=[]):
           if not index_name:
              raise Exception('indexName  can not be NULL')
           if not self.__instance_id.indices.exists(index_name):
               self.__indices.append(index_name)
           try:
               """
               we specify refresh here because
               to ensure that our data will be immediately available.
               The bulkdata can come from any where , either from JSONLoadGenerator.
               The loaddatatype are two type now
               either J=JSON or F=FILE
               """
               self.__instance_id.bulk(index = index_name, body = bulk_data,
                                       doc_type = index_type, refresh = True)
           except ElasticsearchException,e:
               """
               All exception related logs are from https://elasticsearch-py.
               readthedocs.org/en/master/exceptions.html
               """
               raise e

      def  if_insert_index_success_count(self,index_name,count=0):
               """
               This procedure will return the number of inserts , to validate for debug purpose
               :param indexName:
               :return: true or false
               """
               try:
                  result = self.__instance_id.count(index_name)
                  if result['count'] == count:
                      self.__bulkInsertCount = 0
                      return True
                  else:
                      return False
               except NotFoundError,e:
                   raise e

      def  count_documents_inserted(self):
          """
          This function is also for debugging , it will return the number of documents
          :return: count of the documents inserted
          """
          return self.__increment_id + self.__bulk_insert_count

      def   update_index(self,index_name):
           '''
           This procedure will refresh index when insert is performed .
           Need to call this API to take search in effect.
           :param index_name:
           :return:
           '''
           try:
               self.__instance_id.indices.refresh(index_name)
           except   ElasticsearchException ,e:
               raise e

      def   retrieve_single_doc(self,indexName,indexType,id):
            '''
            This function will return you document or documents .
             Mostly for debugging purpose to check if its there
            Its needed before you search .
            :param indexName:
            :param indexType:
            :param id:
            :return:
            '''
            results={}
            try:
                results=self.__instance_id.get(index=indexName,doc_type=indexType,id=id)
                if results == None:
                    warning("Retrieve failed from index = %s ",indexName)
            except ElasticsearchException,e:
                  raise ElasticSearchException(e.message)
            return results

      def retrieve_multiple_doc(self,indexName,indexType):
           """
            This function will return you document or documents .
             Mostly for debugging purpose to check if its there
            Its needed before you search .
            :param indexName:
            :return:
           """
           result=False
           try:
              """
                Any details of this API check https://elasticsearch-py.readthedocs.org/en/master/api.html
              """
              results=self.__instance_id.mget(index=indexName,body={"ids" : ["0"]},refresh=True)
              if results == None:
                   warning("Retrieve failed from index = %s ",indexName)
           except ElasticsearchException,e:
               raise ElasticsearchException(e.message)
           return results

      def  search(self,index_name,query):
           """
           This function will be used for search . based on the query
           :param index_name:
           :param query:
           :return: the search result . The search result will be matched
           """
           try:
               return self.__instance_id.search(body=query,index=index_name)
           except ElasticsearchException ,e:
               raise e


      def get_count_index(self,index_name):
         try:
               counts=self.__instance_id.count(index=index_name)
               if not counts:
                  return 0
               else:
                  return counts['count']
         except ElasticsearchException ,e:
               raise e


      def get_index_status(self,index_name):
           """
          This API used to get index status .
          :param indexName:
          :return: status of Index as JSON string
           """
           try:
               return self.__instance_id.indices.status(index_name)
           except ElasticsearchException ,e:
               raise e


      def get_indices(self):
          """
          Return all the indices created
          :return: List of all indices
          """
          return self.__indices

      def index_alias(self,index_name):
          index_alias = INDEX_DEFAULTS().ALIAS_DEFINITION
          index_alias['indexUUID']=index_name

      def get_mapping(self,index_name,index_type):
          return self.__instance_id.indices.get_mapping(index_name, index_type)

      def delete_mapping(self,index_name,index_type):
           self.__instance_id.indices.delete_mapping(index_name, index_type)

      def set_ES_mapping(self,indexName,indexType,mapping):
           """
            Simple mapping looks like :
            More from : https://lingohub.com/blog/2014/07/elasticsearch-tutorial-part-elasticsearch-data-mapping/
                        http://codeslashslashcomment.com/2012/09/01/
                        search-query-suggestions-using-elasticsearch-via-shingle-filter-and-facets/


            'properties': {
            'description': {
                'type': 'string',
                'analyzer': 'snowball',
            },
            'sku': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            'price': {
                'type': 'integer',
                'index': 'not_analyzed',
            },
            'category': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            "birthdate": {
               "type": "date",
               "format": "dateOptionalTime"
            },
        }
           :param indexName:
           :param indexType:
           :param mapping:
           :return:
           """
           self.__instance_id.indices.put_mapping(index_name ,index_type, mapping)

      def update(self,index_name,index_type,document_json):
         if not (bool(index_name) ^ bool(index_type) ^ bool(document_json)):
              raise Exception('indexName or indexType or document can not be NULL')
