import uuid
FULL_SCAN_TEMPLATE = "SELECT {0} FROM %s"
RANGE_SCAN_TEMPLATE = "SELECT {0} FROM %s WHERE {1}"
FULL_SCAN_GROUP_BY_TEMPLATE = "SELECT {0} FROM %s GROUP by {2}"
RANGE_SCAN_GROUP_BY_TEMPLATE = "SELECT {0} FROM %s WHERE {1} GROUP BY {2}"
FULL_SCAN_ORDER_BY_TEMPLATE = "SELECT {0} FROM %s ORDER by {2}"
RANGE_SCAN_ORDER_BY_TEMPLATE = "SELECT {0} FROM %s where {1} ORDER BY {2}"
FULL_SCAN_COUNT_TEMPLATE = "SELECT count(*) FROM %s"
RANGE_SCAN_COUNT_TEMPLATE = "SELECT count(*) FROM %s WHERE {1}"
RANGE_SCAN_JOIN_TEMPLATE = "SELECT s1.{0},s2.{1} FROM %s as s1 JOIN %s as s2"
INDEX_CREATION_TEMPLATE =  "CREATE INDEX %s ON %s(%s)"
INDEX_DROP_TEMPLATE = "DROP INDEX %s.%s"
SIMPLE_INDEX="simple"
COMPOSITE_INDEX="composite"
GROUP_BY="groupby"
ORDER_BY="orderby"
RANGE_SCAN="range"
FULL_SCAN="full"
JOIN = "join"
EQUALS = "equals"
NOTEQUALS ="notequals"
NO_ORDERBY_GROUPBY="no_orderby_groupby"
GREATER_THAN="greater_than"
AND = "and"
OR = "or"
class QueryDefinition(object):
	def __init__(self, name = "default", index_name = "Random", index_fields = [], index_creation_template = INDEX_CREATION_TEMPLATE,
		index_drop_template = INDEX_DROP_TEMPLATE, query_template = "", groups = []):
		self.name = str(uuid.uuid4()).replace("-","")
		self.index_name = index_name
		self.index_fields = index_fields
		self.index_creation_template = index_creation_template
		self.index_drop_template = index_drop_template
		self.query_template = query_template
		self.groups = groups

	def generate_index_create_query(self, bucket = "default", use_gsi_for_secondary = True,
	 deploy_node_info = None, defer_build = None ):
		query = "CREATE INDEX %s ON %s(%s)" % (self.index_name,bucket, ",".join(self.index_fields))
		if use_gsi_for_secondary:
			query += " USING GSI "
		deployment_plan = {}
		if deploy_node_info  != None:
			deployment_plan["nodes"] = deploy_node_info
		if defer_build != None:
			deployment_plan["defer_build"] = defer_build
		if len(deployment_plan) != 0:
			query += " WITH " + str(deployment_plan)
		return query

	def generate_index_drop_query(self, bucket = "default", use_gsi_for_secondary = True):
		query =  "DROP INDEX %s.%s" % (bucket, self.index_name)
		if use_gsi_for_secondary:
			query += " USING GSI"
		return query

	def generate_query(self, bucket):
		if "join" in self.groups:
			return self.query_template % (bucket,bucket)
		return self.query_template % bucket

	def generate_query_with_explain(self, bucket):
		return ("EXPLAIN "+self.query_template) % bucket

	def add_group(self, group):
		self.groups.append(group)

class SQLDefinitionGenerator:
	def generate_simple_data_query_definitions(self):
		definitions_list = []
		index_name_prefix = "simple"+str(uuid.uuid4()).replace("-","")
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
				index_fields = ["job_title"],
				query_template = FULL_SCAN_TEMPLATE.format("*","name IS NOT NULL"),
				groups = [SIMPLE_INDEX, FULL_SCAN, "simple","isnotnull",NO_ORDERBY_GROUPBY]))
		return definitions_list

	def generate_employee_data_query_definitions(self):
		definitions_list = []
		index_name_prefix = "employee"+str(uuid.uuid4()).replace("-","")
		#emit_fields = "name, job_title, join_yr, join_mo, join_day"
		emit_fields = "*"
		and_conditions = ["job_title == \"Sales\"","job_title != \"Sales\""]
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_ORDER_BY_TEMPLATE.format(emit_fields,"job_title IS NOT NULL","job_title"),
							 groups = [SIMPLE_INDEX, FULL_SCAN, ORDER_BY, "employee","isnotnull"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\""),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, EQUALS,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" ORDER BY job_title "),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, ORDER_BY, EQUALS,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title != \"Sales\""),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, NOTEQUALS,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" or job_title == \"Engineer\""),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, OR,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_yr",
							 index_fields = ["join_yr"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "join_yr > 2010 and join_yr < 2014"),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, AND,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_yr",
							 index_fields = ["join_yr"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "join_yr > 1999"),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, GREATER_THAN,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title_join_yr",
							 index_fields = ["join_yr","job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" and join_yr > 2010 and join_yr < 2014"),
							 groups = [COMPOSITE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, EQUALS,AND,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title_join_yr",
							 index_fields = ["join_yr","job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" or join_yr > 2010 and join_yr < 2014 ORDER BY job_title"),
							 groups = [COMPOSITE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, EQUALS,OR,"employee"]))
		return definitions_list

	def filter_by_group(self, groups = [], query_definitions = []):
		new_query_definitions = {}
		for query_definition in query_definitions:
			count = 0
			for group in query_definition.groups:
				for group_name in groups:
					if group_name == group:
						count += 1
			if count == len(groups) and query_definition.name not in new_query_definitions.keys():
				new_query_definitions[query_definition.name] = query_definition
		return new_query_definitions.values()

	def _create_condition(self, fields = [], begin_range = [],
		begin_condition = None, end_range= [], end_condition = None):
		index = 0
		list = []
		for field in fields:
			condition_list=[]
			if begin_condition != None:
				condition_list.append (" ({0} {1} {2})".format(field, begin_condition,
					begin_range[index]))
			if end_condition != None:
				condition_list.append (" ({0} {1} {2})".format(field, end_condition, end_range[index]))
			list.append(" and ".join(condition_list))
			index +=1
		return " and ".join(list)
