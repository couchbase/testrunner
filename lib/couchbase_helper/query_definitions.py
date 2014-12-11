import uuid
FULL_SCAN_TEMPLATE = "Select {0} from %s"
RANGE_SCAN_TEMPLATE = "Select {0} from %s where {1}"
FULL_SCAN_GROUP_BY_TEMPLATE = "Select {0} from %s Group by {2}"
RANGE_SCAN_GROUP_BY_TEMPLATE = "Select {0} from %s where {1} Group by {2}"
FULL_SCAN_ORDER_BY_TEMPLATE = "Select {0} from %s Order by {2}"
RANGE_SCAN_ORDER_BY_TEMPLATE = "Select {0} from %s where {1} Order by {2}"
FULL_SCAN_COUNT_TEMPLATE = "Select count(*) from %s"
RANGE_SCAN_COUNT_TEMPLATE = "Select count(*) from %s where {1}"
RANGE_SCAN_JOIN_TEMPLATE = "Select s1.{0},s2.{1} from %s as s1 join %s as s2"
INDEX_CREATION_TEMPLATE =  "CREATE INDEX %s ON %s(%s) "
INDEX_DROP_TEMPLATE = "DROP INDEX %s.%s"
SIMPLE_INDEX="simple"
COMPOSITE_INDEX="composite"
GROUP_BY="groupby"
ORDER_BY="orderby"
RANGE_SCAN="range"
FULL_SCAN="full"
JOIN = "join"
class QueryDefinition(object):
	def __init__(self, index_name = "Random", index_fields = [], index_creation_template = INDEX_CREATION_TEMPLATE,
		index_drop_template = INDEX_DROP_TEMPLATE, query_template = "", groups = []):
		self.index_name = index_name
		self.index_fields = index_fields
		self.index_creation_template = index_creation_template
		self.index_drop_template = index_drop_template
		self.query_template = query_template
		self.groups = groups

	def generate_index_create_query(self, bucket = "default"):
		return "CREATE INDEX %s ON %s(%s) " % (self.index_name,bucket, ",".join(self.index_fields))

	def generate_index_drop_query(self, bucket = "default"):
		return "DROP INDEX %s.%s" % (bucket, self.index_name)

	def generate_query(self, bucket):
		if "join" in groups:
			return self.query_template % (bucket,bucket)
		return self.query_template % bucket

	def generate_query_with_explain(self, bucket):
		return ("EXPLAIN "+self.query_template) % bucket

	def add_group(self, group):
		self.groups.append(group)

class SQLDefinitionGenerator:
	def generate_employee_data_sql_definitions(self):
		definitions_list = []
		index_name_prefix = "employee"+str(uuid.uuid4()).replace("-","")
		emit_fields = "name, job_title, join_yr, join_mo, join_day"
		and_conditions = ["job_title = \"^System \"","job_title = \"^Senior \"",
		"job_title = \"^System \"","job_title = \"^UI \""]
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
				index_fields = ["job_title"],
				query_template = FULL_SCAN_TEMPLATE.format(emit_fields,"job_title is not null"),
				groups = [SIMPLE_INDEX, FULL_SCAN, "employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
				index_fields = ["job_title"],
				query_template = FULL_SCAN_GROUP_BY_TEMPLATE.format(emit_fields,"job_title is not null","job_title"),
				groups = [SIMPLE_INDEX, RANGE_SCAN, GROUP_BY, "employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = FULL_SCAN_ORDER_BY_TEMPLATE.format(emit_fields,"job_title is not null","job_title"),
							 groups = [SIMPLE_INDEX, RANGE_SCAN, ORDER_BY, "employee"]))
		for condition in and_conditions:
			definitions_list.append(
				QueryDefinition(
					index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields,"job_title is not null and %s" % condition),
							 groups = [SIMPLE_INDEX,RANGE_SCAN, "employee"]))
			definitions_list.append(
				QueryDefinition(
					index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_GROUP_BY_TEMPLATE.format(emit_fields,"job_title is not null and %s" % condition,"job_title"),
							 groups = [SIMPLE_INDEX, RANGE_SCAN, GROUP_BY, "employee"]))
			definitions_list.append(
				QueryDefinition(
					index_name=index_name_prefix+"job_title",
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_ORDER_BY_TEMPLATE.format(emit_fields,"job_title is not null and %s" % condition,"job_title"),
							 groups = [SIMPLE_INDEX, RANGE_SCAN, ORDER_BY, "employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_month_join_yr_join_day",
							 index_fields = ["join_mon", "join_yr", "join_day"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields,
							 	self._create_condition(["join_yr","join_mon","join_day"],[2008,0,1],"<=",[2008,7,1],">=")+" order by join_yr"),
							 groups = [COMPOSITE_INDEX, RANGE_SCAN, "employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_month_join_yr_join_day",
							 index_fields =["join_mon", "join_yr", "join_day"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields,
							 	self._create_condition(["join_yr","join_mon","join_day"],[2008,0,1],"<=",[],None)+" order by join_yr"),
							 groups = [COMPOSITE_INDEX, RANGE_SCAN, ORDER_BY, "employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_month_join_yr_join_day",
							 index_fields = ["join_mon", "join_yr", "join_day"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields,
							 	self._create_condition(["join_yr","join_mon","join_day"],[],None,[2008,7,1],">=")+" order by join_yr"),
							 groups = [COMPOSITE_INDEX, RANGE_SCAN,  ORDER_BY,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_month_join_yr_join_day",
							 index_fields = ["join_mon", "join_yr", "join_day"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields,
							 	self._create_condition(["join_yr","join_mon","join_day"],[2008,7,1],"<=",[2008,7,1],">=")+" order by join_yr"),
							 groups = [COMPOSITE_INDEX, RANGE_SCAN,  ORDER_BY,"employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"join_month_join_yr_join_day",
							 index_fields = ["join_mon", "join_yr", "join_day"],
							 query_template = RANGE_SCAN_TEMPLATE.format(emit_fields,
							 	self._create_condition(["join_yr","join_mon","join_day"],[1999,7,1],"<=",[2030,7,1],">=")+" order by join_yr"),
							 groups = [COMPOSITE_INDEX, RANGE_SCAN,  ORDER_BY, "employee"]))
		definitions_list.append(
			QueryDefinition(
				index_name=index_name_prefix+"composite_range_join_{0}_{1}".format(index_name_prefix,"name_job_title"),
							 index_fields = ["job_title"],
							 query_template = RANGE_SCAN_JOIN_TEMPLATE.format("name", emit_fields),
							 groups = [COMPOSITE_INDEX, RANGE_SCAN, JOIN, "employee"]))
		return definitions_list

	def filter_by_group(self, groups = [], query_definitions = []):
		new_query_definitions = {}
		for group in groups:
			for query_definition in query_definitions:
				if group in query_definition.groups:
					if query_definition.index_name not in new_query_definitions.keys():
						new_query_definitions[query_definition.index_name] = query_definition
		return new_query_definitions.values()

	def _create_condition(self, fields = [], begin_range = [],
		begin_condition = None, end_range= [], end_condition = None):
		index = 0
		list = []
		for field in fields:
			condition_list=[]
			if begin_condition != None:
				list.append (" ({0} {1} {2})".format(field, begin_condition,
					begin_range[index]))
			if end_condition != None:
				list.append (" ({0} {1} {2})".format(field, end_condition, end_range[index]))
			list.append(" and ".join(condition_list))
			index +=1
		return " and ".join(list)
