#/usr/bin/python
import random
import string
import json

#/route -> schedule, hotel -> review, hotel -> public_likes

cfg = {
       "nqueries": 		7,
       "sql1name": 		["route", "hotel"],
       "sql1":			["routeflds", "hotelflds"],
       "routeflds":   	["sourceairport", "destinationairport", "airlineid", "airline"],
       "hotelflds":   	["address", "city", "country", "name", "title", "url"],
       "route":			["schedule"],
       "hotel":			["reviews", "public_likes"],
       "schedule": 		["sourceairport", "destinationairport", "airlineid", "airline"],
       "reviews":		["address", "city", "country", "name"],
	   "public_likes":	["address", "city", "country", "name"],
      }

objectElements = {
		"schedule":		["day", "flight", "utc"],
		"reviews":		["author", "date"],
        "public_likes":	["public_likes"],
		}


fieldMap = {
		"author":		["\"Barton Marks\"", "\"Blaise O'Connell IV\"", "\"Davon Price\"", "\"Domenick Waters DVM\"", "\"Earlene Feest\"", "\"Joannie Barrows DDS\"", "\"Marianna Schmeler\"", "\"Nedra Cronin\"", "\"Ozella Sipes\""],
		"date":			["\"2012-01-14 02:15:51 +0300\"", "\"2015-11-28 13:29:30 +0300\"", "\"2018-11-28 13:29:30 +0300\""],
		"public_likes": ["\"Blake Homenick\"", "\"Christy Okuneva\"", "\"Corrine Hilll\"", "\"Ephraim Hane\"", "\"Geoffrey Glover\"", "\"Julius Tromp I\"", "\"Nash Kihn\""],
		"day":			["0", "1", "2", "3", "4", "5", "6"],
		"flight":		["\"2L024\"", "\"3F054\"", "\"AF198\"", "\"AY076\"", "\"BW629\"", "\"CA100\"", "\"XQ403\"", "\"XQ995\""],
		"utc":			["\"06:01:00\"", "\"11:46:00\"", "\"22:15:00\""],
		}

class ArrayGenerator(object):
	def common_field(self,i, table2):
		li = []
		for f1 in cfg[cfg["sql1"][i]]:
			for f2 in cfg[table2]:
				if f1 == f2:
					li.append(f1)
		return li


	def projection(self,t, fields):
		projs = random.sample(fields, random.randint(1, len(fields)))
		projs.insert(0, "id")
		newprojs = []
		for p in projs:
			p = t + "." + p
			newprojs.append(p)
		seperator = ", "
		return seperator.join(projs), "DISTINCT " + seperator.join(newprojs)


	def betweenclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		start = random.randint(0, len(fields) - 2)
		end = random.randint(start, len(fields) - 1)
		s1 = table + "." + field + " between " + fields[start] + " AND " + fields[end]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " between " + fields[start] + " AND " + fields[end] + " END"
		else :
			s2 += alias + "." + field + " between " + fields[start] + " AND " + fields[end] + " END"
		array.append(s2)
		return array

	def likeclause_leading(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		l = random.randint(1, len(fields[idx]) - 1)
		if l == 1:
			l = l + 1
		prex = fields[idx][:l] + "%"
		s1 = table + "." + field + " like " + prex + "\""
		array.append(s1)
		s2 = ""
		if field == "public_likes":
			s2 += alias + " like " + str(prex) + "\" END"
		else :
			s2 += alias + "." + field + " like " + str(prex) + "\" END"
		array.append(s2)
		return array


	def likeclause_noleading(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		l = random.randint(1, len(fields[idx]))
		if l == 1:
			l = l + 1
		prex =  "%" + fields[idx][:l] + "%"
		s1 = table + "." + field + " like " + prex
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " like " + prex + "\" END"
		else :
			s2 += alias + "." + field + " like " + prex + "\" END"
		array.append(s2)
		return array


	def equclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		s1 = table + "." + field + " = "  + fields[idx]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " = " + fields[idx] + " END"
		else :
			s2 += alias + "." + field + " = " + fields[idx] + " END"
		array.append(s2)
		return array

	def multiequclause(self, table, field, field2, alias):
		array = []
		if random.randint(0, 100) < 80:
			conjunction = " AND "
		else:
			conjunction = " OR "
		fields = fieldMap[field]
		fields2 = fieldMap[field2]
		idx = random.randint(0, len(fields) - 1)
		idx2 = random.choice([i for i in range(0,len(fields) - 1) if i != idx])
		s1 = table + "." + field + " = "  + fields[idx]
		array.append(s1)
		s2 = ""
		s2 += alias + "." + field + " = " + fields[idx] + conjunction + alias + "." + field2 + " = " + fields2[idx2] + " END"
		array.append(s2)
		return array

	def lessclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		s1 = table + "." + field + " < "  + fields[idx]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " < " + fields[idx] + " END"
		else :
			s2 += alias + "." + field + " < " + fields[idx] + " END"
		array.append(s2)
		return array

	def largerclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		s1 = table + "." + field + " > "  + fields[idx]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " > " + fields[idx] + " END"
		else :
			s2 += alias + "." + field + " > " + fields[idx] + " END"
		array.append(s2)
		return array

	def less_array(self,table, field1, field2, alias):
		array = []
		fields_1 = fieldMap[field1]
		fields_2 = fieldMap[field2]
		idx_1 = random.randint(0, len(fields_1) - 1)
		idx_2 = random.randint(0, len(fields_2) - 1)
		s1 = table + "." + field1 + " < "  + fields_1[idx_1] + " OR (" + table + "." + field1 + " = "  + fields_1[idx_1] + " AND " + table + "." + field2 + " < " + fields_2[idx_2] + ")"
		array.append(s1)
		s2 = "[" + alias + "." + field1 + ", " + alias + "." + field2  + "] < [" + fields_1[idx_1] + ", " + fields_2[idx_2] + "]" + " END"
		array.append(s2)
		return array

	def larger_array(self,table, field1, field2, alias):
		array = []
		fields_1 = fieldMap[field1]
		fields_2 = fieldMap[field2]
		idx_1 = random.randint(0, len(fields_1) - 1)
		idx_2 = random.randint(0, len(fields_2) - 1)
		s1 = table + "." + field1 + " > "  + fields_1[idx_1] + " OR (" + table + "." + field1 + " = "  + fields_1[idx_1] + " AND " + table + "." + field2 + " > " + fields_2[idx_2] + ")"
		array.append(s1)
		s2 = "[" + alias + "." + field1 + ", " + alias + "." + field2  + "] > [" + fields_1[idx_1] + ", " + fields_2[idx_2] + "]" + " END"
		array.append(s2)
		return array


	def inclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		sub = random.sample(fields, random.randint(1, len(fields)))
		seperator = ", "
		l = seperator.join(sub)
		s1 = table + "." + field + " in (" + l + ")"
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " in [" + l + "] END"
		else :
			s2 += alias + "." + field + " in [" + l + "] END"
		array.append(s2)
		return array

	def withinclause(self, table, field, alias):
		array = []
		object = {}
		for entry in fieldMap:
			if entry != field :
				fields = fieldMap[entry]
				if random.randint(0, 100) < 50:
					object[entry] = eval("[" + ','.join(random.sample(fields, random.randint(1, len(fields)))) + "]")
		if random.randint(0,100) < 80:
			fields = fieldMap[field]
			sub = random.sample(fields, random.randint(1, len(fields)))
			seperator = ","
			l = seperator.join(sub)
			object[field] = eval("[" + l + "]")
		else:
			fields = fieldMap[field]
			sub = random.sample(fields, random.randint(1, len(fields)))
			seperator = ", "
			l = seperator.join(sub)
			object[field] = {"nest": eval("[" + l + "]")}
		s1 =""
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " within " + json.dumps(object) + " END"
		else :
			s2 += alias + "." + field + " within " + json.dumps(object) + " END"
		array.append(s2)
		return array

	def generate_statement(self,i, table2, cond, array):
		stmts = []
		if "schedule" in array[0]:
			table1 = cfg["sql1name"][0]
			fields = cfg[cfg["sql1"][0]]
		else:
			table1 = cfg["sql1name"][1]
			fields = cfg[cfg["sql1"][1]]
		num = random.randint(0,len(fields)-1)
		proj, newproj = self.projection(table1, fields)
		alias = table2[:1]
		s1 = ""
		s1 += "SELECT " + newproj
		s1 += " FROM " + table1
		s1 += " JOIN " + table2
		s1 += " ON " + table1 + "." + cond + " = " + table2 + "." + cond
		s1 += " AND " + table1 + ".id" " = " +  table2 + ".id"
		s1 += " WHERE " + array[0] + ";"

		proj_field_list = proj.split(",")
		order_by_list = ""
		i = 0

		# Randomly add ASC or DESC to the order by fields
		for field in proj_field_list:
			if random.randint(0, 100) < 20:
				if random.randint(0,100) < 30:
					new_proj_field = field + " ASC"
				else:
					new_proj_field = field + " DESC"
				if i == 0:
					order_by_list += new_proj_field
				else:
					order_by_list += (", " + new_proj_field)
				i +=1
			else:
				if i == 0:
					order_by_list += field
				else:
					order_by_list += (", " + field)
				i += 1
		if random.randint(0, 100) < 80:
			array_type = random.choice(["ANY ","SOME "])
		else:
			array_type = "EVERY "
		if table1 == "route" and random.randint(0,100) < 30:
			s2 = "SELECT " + proj + " FROM `travel-sample` WHERE " + array_type + alias + " IN " + table2 + " SATISFIES " + array[1] + " AND `type` = \"" + table1 + "\" AND stops = 0 ORDER BY " + order_by_list
		else:
			s2 = "SELECT " + proj + " FROM `travel-sample` WHERE " + array_type + alias + " IN " + table2 + " SATISFIES " + array[1] + " AND `type` = \"" + table1 + "\" ORDER BY " + order_by_list
		return s1, s2


	def generate_query_pairs(self):
		global cnt
		query_array = []
		for i in range(0, len(cfg["sql1name"])):
			t1 = cfg["sql1name"][i]
			for t2 in cfg[t1]:
				joinconds = self.common_field(i, t2)
				array_condition = []
				alias = t2[:1]
				for cond in joinconds:
					idx = random.randint(0, len(objectElements[t2]) - 1)
					idxfield = objectElements[t2][idx]
					if not (t2 == 'public_likes' or t2 == 'reviews'):
						idx2 = random.choice([i for i in range(0,(len(objectElements[t2]) - 1)) if i != idx])
						idxfield2 = objectElements[t2][idx2]
					if random.randint(0,100) < 40 or t2 == 'public_likes' or t2 == 'reviews':
						cond_equal = self.equclause(t2, idxfield, alias)
					else:
						cond_equal = self.multiequclause(t2, idxfield, idxfield2, alias)
					array_condition.append(cond_equal)
					cond_in = self.inclause(t2, idxfield, alias)
					array_condition.append(cond_in)
					cond_within = self.withinclause(t2, idxfield, alias)
					array_condition.append(cond_within)
					cond_btw = self.betweenclause(t2, idxfield, alias)
					array_condition.append(cond_btw)
					if idxfield != "day":
						cond_like = self.likeclause_leading(t2, idxfield, alias)
						array_condition.append(cond_like)
					if t2 != "public_likes":
						idx2 = random.randint(0, len(objectElements[t2]) - 1)
						idxfield2 = objectElements[t2][idx2]
						cond_larger_array = self.larger_array(t2, idxfield, idxfield2, alias)
						cond_less_array = self.less_array(t2, idxfield, idxfield2, alias)
						array_condition.append(cond_larger_array)
						array_condition.append(cond_less_array)
					for j in range(0, len(array_condition)):
						s1, s2 = self.generate_statement(i, t2, cond, array_condition[j])
						query_array.append(s2)
						cnt = cnt + 1
		return query_array

cnt = 0
def main():
	print("Here")
	final_list = []
	for i in range(cfg["nqueries"]):
		query_array = ArrayGenerator().generate_query_pairs()
		final_list += query_array
	global cnt
	print(cnt)


if __name__ == "__main__":
    random.seed()
    main()

