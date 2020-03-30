import copy
from .documentgenerator import  DocumentGenerator
import re
import datetime
import json
import random, string
import os
import logger

from .data import COUNTRIES, COUNTRY_CODE, FIRST_NAMES, LAST_NAMES

log = logger.Logger.get_logger()

class TuqGenerators(object):

    def __init__(self, log, full_set):
        self.log = log
        self.full_set = full_set
        self.query = None
        self.type_args = {}
        self.nests = self._all_nested_objects(full_set[0])
        self.type_args['str'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], str)]
        self.type_args['int'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], int)]
        self.type_args['float'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], float)]
        self.type_args['bool'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], bool)]
        self.type_args['list_str'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], str)]
        self.type_args['list_int'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], int)]
        self.type_args['list_obj'] = [attr[0] for attr in full_set[0].items()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], dict)]
        self.type_args['obj'] = [attr[0] for attr in full_set[0].items()
                             if isinstance(attr[1], dict)]
        for obj in self.type_args['obj']:
            self.type_args['_obj%s_str' % (self.type_args['obj'].index(obj))] = [attr[0] for attr in full_set[0][obj].items()
                                                                                    if isinstance(attr[1], str)]
            self.type_args['_obj%s_int'% (self.type_args['obj'].index(obj))] = [attr[0] for attr in full_set[0][obj].items()
                                                                                    if isinstance(attr[1], int)]
        for obj in self.type_args['list_obj']:
            self.type_args['_list_obj%s_str' % (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].items()
                                                                                    if isinstance(attr[1], str) or isinstance(attr[1], str)]
            self.type_args['_list_obj%s_int'% (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].items()
                                                                                    if isinstance(attr[1], int)]
        for i in range(2, 5):
            self.type_args['nested_%sl' % i] = [attr for attr in self.nests if len(attr.split('.')) == i]
        for i in range(2, 5):
            self.type_args['nested_list_%sl' % i] = [attr[0] for attr in self.nests.items() if len(attr[0].split('.')) == i and isinstance(attr[1], list)]
        self._clear_current_query()

    def generate_query(self, template):
        query = template
        for name_type, type_arg in self.type_args.items():
            for attr_type_arg in type_arg:
                query = query.replace('$%s%s' % (name_type, type_arg.index(attr_type_arg)), attr_type_arg)
        for expr in [' where ', ' select ', ' from ', ' order by', ' limit ', 'end',
                     ' offset ', ' count(' , 'group by', 'unnest', 'min', 'satisfies']:
            query = query.replace(expr, expr.upper())
        self.log.info("Generated query to be run: '''%s'''" % query)
        self.query = query
        return query

    def generate_expected_result(self, print_expected_result = True):
        try:
            self._create_alias_map()
            from_clause = self._format_from_clause()
            log.info("FROM clause ===== is %s" % from_clause)
            where_clause = self._format_where_clause(from_clause)
            log.info("WHERE clause ===== is %s" % where_clause)
            unnest_clause = self._format_unnest_clause(from_clause)
            log.info("UNNEST clause ===== is %s" % unnest_clause)
            select_clause = self._format_select_clause(from_clause)
            log.info("SELECT clause ===== is %s" % select_clause)
            result = self._filter_full_set(select_clause, where_clause, unnest_clause)
            result = self._order_results(result)
            result = self._limit_and_offset(result)
            if print_expected_result:
                log.info("Expected result is %s ..." % str(result[:15]))
            return result
        finally:
            self._clear_current_query()

    def _all_nested_objects(self, d):
        def items():
            for key, value in list(d.items()):
                if isinstance(value, dict):
                    for subkey, subvalue in list(self._all_nested_objects(value).items()):
                        yield key + "." + subkey, subvalue
                else:
                    yield key, value
        return dict(items())

    def _create_alias_map(self):
        query_dict = self.query.split()
        for word in query_dict:
            if word.upper() == 'AS':
                self.aliases[query_dict[query_dict.index(word) + 1]] = query_dict[query_dict.index(word) - 1]

    def _format_where_clause(self, from_clause=None):
        if self.query.find('WHERE') == -1:
            return None
        clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*WHERE', '', self.query))
        clause = re.sub(r'GROUP BY.*', '', clause)
        attributes = self.get_all_attributes()
        conditions = clause.replace('IS NULL', 'is None')
        conditions = conditions.replace('IS NOT NULL', 'is not None')
        satisfy_expr = self.format_satisfy_clause()
        if satisfy_expr:
            conditions = re.sub(r'ANY.*END', '', conditions).strip()
        regex = re.compile("[\w']+\.[\w']+")
        atts = regex.findall(conditions)
        for att in atts:
            parent, child = att.split('.')
            if parent in attributes:
                conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                ' doc["%s"]["%s"] ' % (parent, child))
            else:
                if parent not in self.aliases:
                    conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                ' doc["%s"] ' % (child))
                elif self.aliases[parent] in attributes:
                    conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                    ' doc["%s"]["%s"] ' % (self.aliases[parent], child))
                else:
                    conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                    ' doc["%s"] ' % (child))
        for attr in attributes:
            conditions = conditions.replace(' %s ' % attr, ' doc["%s"] ' % attr)
        if satisfy_expr:
            if conditions:
                for join in ["AND", "OR"]:
                    present = conditions.find(join)
                    if present > -1:
                        conditions = conditions.replace(join, join.lower())
                        if present > 0:
                            conditions += '' + satisfy_expr
                            break
                        else:
                            conditions = satisfy_expr + ' ' + conditions
                            break
            else:
                conditions += '' + satisfy_expr
        if from_clause and from_clause.find('.') != -1:
            sub_attrs = [att for name, group in self.type_args.items()
                         for att in group if att not in attributes]
            for attr in sub_attrs:
                conditions = conditions.replace(' %s ' % attr, ' doc["%s"] ' % attr)
            conditions = conditions.replace('doc[', 'doc["%s"][' % from_clause.split('.')[-1])
        conditions = conditions.replace(' = ', ' == ')
        return conditions

    def _format_from_clause(self):
        clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*FROM', '', self.query)).strip()
        clause = re.sub(r'WHERE.*', '', re.sub(r'GROUP BY.*', '', clause)).strip()
        clause = re.sub(r'SELECT.*', '', clause).strip()
        if len(clause.split()) == 2:
            self.aliases[clause.split()[1]] = clause.split()[0]
        return clause

    def _format_unnest_clause(self, from_clause):
        if from_clause.find('UNNEST') == -1:
            return None
        clause = re.sub(r'.*UNNEST', '', from_clause)
        attr = clause.split()
        if len(attr) == 1:
            clause = 'doc["%s"]' % attr[0]
        elif len(attr) == 2:
            attributes = self.get_all_attributes()
            if attr[0].find('.') != -1:
                splitted = attr[0].split('.')
                if splitted[0] not in attributes:
                    alias = [attr[0].split('.')[1],]
                    clause = 'doc["%s"]' % attr[1]
                    for inner in splitted[2:]:
                        alias.append(inner)
                    self.aliases[attr[1]] = tuple(alias)
                    return clause
                parent, child = attr[0].split('.')
                if parent in attributes:
                    clause = 'doc["%s"]["%s"]' % (parent, child)
                    self.aliases[attr[1]] = (parent, child)
                else:
                    if parent not in self.aliases:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[1]] = child
                    elif self.aliases[parent] in attributes:
                        clause = 'doc["%s"]["%s"]' % (self.aliases[parent], child)
                        self.aliases[attr[1]] = (self.aliases[parent], child)
                    else:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[1]] = child
            else:
                clause = 'doc["%s"]' % attr[0]
                self.aliases[attr[1]] = attr[0]
        elif len(attr) == 3 and ('as' in attr or 'AS' in attr):
            attributes = self.get_all_attributes()
            if attr[0].find('.') != -1:
                parent, child = attr[0].split('.')
                if parent in attributes:
                    clause = 'doc["%s"]["%s"]' % (parent, child)
                    self.aliases[attr[2]] = (parent, child)
                else:
                    if parent not in self.aliases:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[2]] = child
                    elif self.aliases[parent] in attributes:
                        clause = 'doc["%s"]["%s"]' % (self.aliases[parent], child)
                        self.aliases[attr[2]] = (self.aliases[parent], child)
                    else:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[2]] = child
            else:
                clause = 'doc["%s"]' % attr[0]
                self.aliases[attr[2]] = attr[0]
        return clause

    def _format_select_clause(self, from_clause=None):
        select_clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*SELECT', '', self.query)).strip()
        select_clause = re.sub(r'WHERE.*', '', re.sub(r'FROM.*', '', select_clause)).strip()
        select_attrs = select_clause.split(',')
        if from_clause and from_clause.find('UNNEST') != -1:
            from_clause = re.sub(r'UNNEST.*', '', from_clause).strip()
        condition = '{'
        #handle aliases
        for attr_s in select_attrs:
            attr = attr_s.split()
            if re.match(r'COUNT\(.*\)', attr[0]):
                    attr[0] = re.sub(r'\)', '', re.sub(r'.*COUNT\(', '', attr[0])).strip()
                    self.aggr_fns['COUNT'] = {}
                    if attr[0].upper() == 'DISTINCT':
                        attr = attr[1:]
                        self.distinct= True
                    if attr[0].find('.') != -1:
                        parent, child = attr[0].split('.')
                        attr[0] = child
                    if attr[0] in self.aliases:
                        attr[0] = self.aliases[attr[0]]
                    self.aggr_fns['COUNT']['field'] = attr[0]
                    self.aggr_fns['COUNT']['alias'] = ('$1', attr[-1])[len(attr) > 1]
                    if attr[0] == '*':
                        condition += '"%s" : doc,' % attr[-1]
                    continue
            elif re.match(r'MIN\(.*\)', attr[0]):
                    attr[0] = re.sub(r'\)', '', re.sub(r'.*MIN\(', '', attr[0])).strip()
                    self.aggr_fns['MIN'] = {}
                    if attr[0].find('.') != -1:
                        parent, child = attr[0].split('.')
                        attr[0] = child
                    if attr[0] in self.aliases:
                        attr[0] = self.aliases[attr[0]]
                    self.aggr_fns['MIN']['field'] = attr[0]
                    self.aggr_fns['MIN']['alias'] = ('$1', attr[-1])[len(attr) > 1]
                    self.aliases[('$1', attr[-1])[len(attr) > 1]] = attr[0]
                    condition += '"%s": doc["%s"]' % (self.aggr_fns['MIN']['alias'], self.aggr_fns['MIN']['field'])
                    continue
            elif attr[0].upper() == 'DISTINCT':
                attr = attr[1:]
                self.distinct= True
            if attr[0] == '*':
                condition += '"*" : doc,'
            elif len(attr) == 1:
                if attr[0].find('.') != -1:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[0], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
                    elif attr[0].split('.')[1] == '*':
                        condition = 'doc["%s"]' % (attr[0].split('.')[0])
                        return condition
                    else:
                        if attr[0].split('.')[0] not in self.get_all_attributes() and\
                                        from_clause.find(attr[0].split('.')[0]) != -1:
                            condition += '"%s" : doc["%s"],' % (attr[0].split('.')[1], attr[0].split('.')[1])
                            continue
                        else:
                            condition += '"%s" : {%s : doc["%s"]["%s"]},' % (attr[0].split('.')[0], attr[0].split('.')[1],
                                                                          attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[0], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
                    else:
                        if attr[0] in self.aliases:
                            value = self.aliases[attr[0]]
                            if len(value) > 1:
                                condition += '"%s" : doc["%s"]' % (attr[0], value[0])
                                for inner in value[1:]:
                                    condition += '["%s"]' % (inner)
                                condition += ','
                        else:
                            condition += '"%s" : doc["%s"],' % (attr[0], attr[0])
            elif len(attr) == 2:
                if attr[0].find('.') != -1:
                    condition += '"%s" : doc["%s"]["%s"],' % (attr[1], attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    condition += '"%s" : doc["%s"],' % (attr[1], attr[0])
                self.aliases[attr[1]] = attr[0]
            elif len(attr) == 3 and ('as' in attr or 'AS' in attr):
                if attr[0].find('.') != -1:
                    condition += '"%s" : doc["%s"]["%s"],' % (attr[2], attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[2], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
                    else:
                        condition += '"%s" : doc["%s"],' % (attr[2], attr[0])
        condition += '}'
        if from_clause and from_clause.find('.') != -1:
            condition = condition.replace('doc[', 'doc["%s"][' % from_clause.split('.')[-1])
        return condition

    def _filter_full_set(self, select_clause, where_clause, unnest_clause):
        diff = self._order_clause_greater_than_select(select_clause)
        if diff and not self._is_parent_selected(select_clause, diff) and not 'MIN' in self.query:
            if diff[0].find('][') == -1:
                select_clause = select_clause[:-1] + ','.join(['"%s" : %s' %([at.replace('"','') for at in re.compile('"\w+"').findall(attr)][0],
                                                                            attr) for attr in self._order_clause_greater_than_select(select_clause)]) + '}'
            else:
                for attr in self._order_clause_greater_than_select(select_clause):
                    select_clause = select_clause[:-1]
                    for at in re.compile('"\w+"').findall(attr):
                        if attr.find('][') != -1:
                            attrs_split = [at.replace('"','') for at in re.compile('"\w+"').findall(attr)]
                            select_clause = select_clause + '"%s" : {"%s" : %s},' %(attrs_split[0], attrs_split[1], attr)
                        else:
                            select_clause = select_clause + '"%s" : %s,' %([at.replace('"','') for at in re.compile('"\w+"').findall(attr)][0], attr)
                    select_clause = select_clause + '}'

        log.info("-->select_clause:{}; where_clause={}".format(select_clause, where_clause))
        if where_clause:
            where_clause = where_clause.replace('if  t  >  "', 'if  str(t)  >  "') # to fix the type error between int, str comparison
            log.info("-->where_clause={}".format(where_clause))
            result = [eval(select_clause) for doc in self.full_set if eval(where_clause)]
        else:
            result = [eval(select_clause) for doc in self.full_set]
        if self.distinct:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        if unnest_clause:
            unnest_attr = unnest_clause[5:-2]
            if unnest_attr in self.aliases:
                def res_generator():
                    for doc in result:
                        doc_temp = copy.deepcopy(doc)
                        del doc_temp[unnest_attr]
                        for item in eval(unnest_clause):
                            doc_to_append = copy.deepcopy(doc_temp)
                            doc_to_append[unnest_attr] = copy.deepcopy(item)
                            yield doc_to_append
                result = list(res_generator())
            else:
                result = [item for doc in result for item in eval(unnest_clause)]
        if self._create_groups()[0]:
            result = self._group_results(result)
        if self.aggr_fns:
            if not self._create_groups()[0] or len(result) == 0:
                for fn_name, params in self.aggr_fns.items():
                    if fn_name == 'COUNT':
                        result = [{params['alias'] : len(result)}]
        return result

    def _order_clause_greater_than_select(self, select_clause):
        order_clause = self._get_order_clause()
        if not order_clause:
            return None
        order_clause = order_clause.replace(',"', '"')
        diff = set(order_clause.split(',')) - set(re.compile('doc\["[\w\']+"\]').findall(select_clause))
        diff = [attr.replace(",",'"') for attr in diff if attr != '']
        for k, v in self.aliases.items():
            if k.endswith(','):
                self.aliases[k[:-1]] = v
                del self.aliases[k]
        if not set(diff) - set(['doc["%s"]' % alias for alias in self.aliases]):
            return None
        else:
            diff = list(set(diff) - set(['doc["%s"]' % alias for alias in self.aliases]))
        if diff:
            self.attr_order_clause_greater_than_select = [re.sub(r'"\].*', '', re.sub(r'doc\["', '', attr)) for attr in diff]
            self.attr_order_clause_greater_than_select = [attr for attr in self.attr_order_clause_greater_than_select if attr]
            return list(diff)
        return None

    def _get_order_clause(self):
        if self.query.find('ORDER BY') == -1:
            return None
        order_clause = re.sub(r'LIMIT.*', '', re.sub(r'.*ORDER BY', '', self.query)).strip()
        order_clause = re.sub(r'OFFSET.*', '', order_clause).strip()
        condition = ""
        order_attrs = order_clause.split(',')
        for attr_s in order_attrs:
            attr = attr_s.split()
            if attr[0] in iter(self.aliases.values()):
                    condition += 'doc["%s"],' % (self.get_alias_for(attr[0]))
                    continue
            if attr[0].find('MIN') != -1:
                if 'MIN' not in self.aggr_fns:
                    self.aggr_fns['MIN'] = {}
                    attr[0]= attr[0][4:-1]
                    self.aggr_fns['MIN']['field'] = attr[0]
                    self.aggr_fns['MIN']['alias'] = '$gr1'
                else:
                    if 'alias' in self.aggr_fns['MIN']:
                        condition += 'doc["%s"],' % self.aggr_fns['MIN']['alias']
                        continue
            if attr[0].find('.') != -1:
                attributes = self.get_all_attributes()
                if attr[0].split('.')[0] in self.aliases and (not self.aliases[attr[0].split('.')[0]] in attributes) or\
                   attr[0].split('.')[0] in attributes:
                    condition += 'doc["%s"]["%s"],' % (attr[0].split('.')[0],attr[0].split('.')[1])
                else:
                    if attr[0].split('.')[0].find('[') != -1:
                        ind = attr[0].split('.')[0].index('[')
                        condition += 'doc["%s"]%s["%s"],' % (attr[0].split('.')[0][:ind], attr[0].split('.')[0][ind:],
                                                             attr[0].split('.')[1])
                    else:
                        condition += 'doc["%s"],' % attr[0].split('.')[1]
            else:
                if attr[0].find('[') != -1:
                    ind = attr[0].index('[')
                    condition += 'doc["%s"]%s,' % (attr[0].split('.')[0][:ind], attr[0].split('.')[0][ind:])
                else:
                    condition += 'doc["%s"],' % attr[0]
        log.info("ORDER clause ========= is %s" % condition)
        return condition

    def _order_results(self, result):
        order_clause = self._get_order_clause()
        key = None
        reverse = False
        if order_clause:
            all_order_clause = re.sub(r'LIMIT.*', '', re.sub(r'.*ORDER BY', '', self.query)).strip()
            all_order_clause = re.sub(r'OFFSET.*', '', all_order_clause).strip()
            order_attrs = all_order_clause.split(',')
            for attr_s in order_attrs:
                attr = attr_s.split()
                if len(attr) == 2 and attr[1].upper() == 'DESC':
                    reverse = True
            for att_name in re.compile('"[\w\']+"').findall(order_clause):
                if att_name[1:-1] in iter(self.aliases.values()):
                    order_clause = order_clause.replace(att_name[1:-1],
                                                        self.get_alias_for(att_name[1:-1]))
                if self.aggr_fns and att_name[1:-1] in [params['field'] for params in self.aggr_fns.values()]:
                    order_clause = order_clause.replace(att_name[1:-1],
                                                        [params['alias'] for params in self.aggr_fns.values()
                                                         if params['field'] == att_name[1:-1]][0])
            if order_clause.find(',"') != -1:
                order_clause = order_clause.replace(',"', '"')
            key = lambda doc: eval(order_clause)
        try:
            result = sorted(result, key=key, reverse=reverse)
        except:
            return result
        if self.attr_order_clause_greater_than_select and not self.parent_selected:
            for doc in result:
                for attr in self.attr_order_clause_greater_than_select:
                    if attr.find('.') != -1:
                        attr = attr.split('.')[0]
                    if attr in doc:
                        del doc[attr]
                    elif '$gr1' in doc:
                        del doc['$gr1']
        return result

    def _limit_and_offset(self, result):
        limit_clause = offset_clause = None
        if self.query.find('LIMIT') != -1:
            limit_clause = re.sub(r'OFFSET.*', '', re.sub(r'.*LIMIT', '', self.query)).strip()
        if self.query.find('OFFSET') != -1:
            offset_clause = re.sub(r'.*OFFSET', '', self.query).strip()
        if offset_clause:
            result = result[int(offset_clause):]
        if limit_clause:
            result = result[:int(limit_clause)]
        return result

    def _create_groups(self):
        if self.query.find('GROUP BY') == -1:
            return 0, None
        group_clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*GROUP BY', '', self.query)).strip()
        if not group_clause:
            return 0, None
        attrs = group_clause.split(',')
        attrs = [attr.strip() for attr in attrs]
        if len(attrs) == 2:
            groups = set([(doc[attrs[0]],doc[attrs[1]])  for doc in self.full_set])
        elif len(attrs) == 1:
            if attrs[0].find('.') != -1:
                if len(attrs[0].split('.')) > 2:
                    groups = set([doc[attrs[0].split('.')[1]][attrs[0].split('.')[2]]
                              for doc in self.full_set])
                else:
                    groups = set([doc[attrs[0].split('.')[0]][attrs[0].split('.')[1]]
                              for doc in self.full_set])
            else:
                groups = set([doc[attrs[0]]  for doc in self.full_set])
        return attrs, groups

    def _group_results(self, result):
        attrs, groups = self._create_groups()
        for fn_name, params in self.aggr_fns.items():
            if fn_name == 'COUNT':
                result = [{attrs[0] : group[0], attrs[1] : group[1],
                                params['alias'] : len([doc for doc in result
                                if doc[attrs[0]]==group[0] and doc[attrs[1]]==group[1]])}
                          for group in groups]
                result = [doc for doc in result if doc[params['alias']] > 0]
            if fn_name == 'MIN':
                if isinstance(list(groups)[0], tuple):
                    result = [{attrs[0] : group[0], attrs[1] : group[1],
                                    params['alias'] : min([doc[params['field']] for doc in result
                                    if doc[attrs[0]]==group[0] and doc[attrs[1]]==group[1]])}
                              for group in groups]
                else:
                    if attrs[0] in iter(self.aliases.values()):
                        attrs[0] = self.get_alias_for(attrs[0]).replace(',', '')
                    result = [{attrs[0] : group,
                                params['alias'] : min([doc[params['alias']] for doc in result
                                if doc[attrs[0]]==group])}
                          for group in groups]
        else:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        return result

    def get_alias_for(self, value_search):
        for key, value in self.aliases.items():
            if value == value_search:
                return key
        return ''

    def get_all_attributes(self):
        return [att for name, group in self.type_args.items()
                for att in group if not name.startswith('_')]

    def _is_parent_selected(self, clause, diff):
        self.parent_selected = len([select_el for select_el in re.compile('doc\["[\w\']+"\]').findall(clause)
                for diff_el in diff if diff_el.find(select_el) != -1]) > 0
        return self.parent_selected

    def format_satisfy_clause(self):
        if self.query.find('ANY') == -1 and self.query.find('EVERY') == -1:
            return ''
        satisfy_clause = re.sub(r'.*ANY', '', re.sub(r'END.*', '', self.query)).strip()
        satisfy_clause = re.sub(r'.*ALL', '', re.sub(r'.*EVERY', '', satisfy_clause)).strip()
        if not satisfy_clause:
            return ''
        main_attr = re.sub(r'SATISFIES.*', '', re.sub(r'.*IN', '', satisfy_clause)).strip()
        attributes = self.get_all_attributes()
        if main_attr in attributes:
            main_attr = 'doc["%s"]' % (main_attr)
        else:
            if main_attr.find('.') != -1:
                parent, child = main_attr.split('.')
                if parent in self.aliases and self.aliases[parent] in attributes:
                    main_attr = 'doc["%s"]["%s"]' % (self.aliases[parent], child)
                else:
                    main_attr = 'doc["%s"]' % (child)
        var = "att"
        if self.query.find('ANY') != -1:
            var = re.sub(r'.*ANY', '', re.sub(r'IN.*', '', self.query)).strip()
            result_clause = 'len([{0} for {1} in {2} if '.format(var, var, main_attr)
        satisfy_expr = re.sub(r'.*SATISFIES', '', re.sub(r'END.*', '', satisfy_clause)).strip()
        for expr in satisfy_expr.split():
            if expr.find('.') != -1:
                result_clause += ' {0}["{1}"] '.format(var, expr.split('.')[1])
            elif expr.find('=') != -1:
                result_clause += ' == '
            elif expr.upper() in ['AND', 'OR', 'NOT']:
                result_clause += expr.lower()
            else:
                result_clause += ' %s ' % expr
        result_clause += ']) > 0'
        return result_clause

    def _clear_current_query(self):
        self.distinct = False
        self.aggr_fns = {}
        self.aliases = {}
        self.attr_order_clause_greater_than_select = []
        self.parent_selected = False

class JsonGenerator:

    def generate_docs_employee(self, docs_per_day = 1, start=0, isShuffle = False):
        generators = []
        types = self._shuffle(['Engineer', 'Sales', 'Support'],isShuffle)
        join_yr = self._shuffle([2010, 2011],isShuffle)
        join_mo = self._shuffle(range(1, 12 + 1),isShuffle)
        join_day = self._shuffle(range(1, 28 + 1),isShuffle)
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "email":"{4}", "job_title":"{5}", "test_rate":{8}, "skills":{9},'
        template += '"VMs": {10},'
        template += ' "tasks_points" : {{"task1" : {6}, "task2" : {7}}}}}'
        count = 1
        for info in types:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        random.seed(count)
                        count+=1
                        prefix = "employee"+str(random.random()*100000)
                        name = ["employee-%s" % (str(day))]
                        email = ["%s-mail@couchbase.com" % (str(day))]
                        vms = [{"RAM": month, "os": "ubuntu",
                                "name": "vm_%s" % month, "memory": month},
                               {"RAM": month, "os": "windows",
                                "name": "vm_%s"% (month + 1), "memory": month}
                             ]
                        generators.append(DocumentGenerator("query-test" + prefix,
                                               template,
                                               name, [year], [month], [day],
                                               email, [info], list(range(1,10)), list(range(1,10)),
                                               [float("%s.%s" % (month, month))],
                                               [["skill%s" % y for y in join_yr]],
                                               [vms],
                                               start=start, end=docs_per_day))
        return generators

    def generate_docs_employee_array(self, docs_per_day = 1, start=0, isShuffle = False):
        generators = []
        #simple array
        department = self._shuffle(['Developer', 'Support','HR','Tester','Manager'],isShuffle)
        sport = ['Badminton','Cricket','Football','Basketball','American Football','ski']
        dance = ['classical','bollywood','salsa','hip hop','contemporary','bhangra']
        join_yr = self._shuffle([2010, 2011,2012,2013,2014,2015,2016],isShuffle)
        join_mo = self._shuffle(range(1, 12 + 1),isShuffle)
        join_day = self._shuffle(range(1, 28 + 1),isShuffle)
        engineer = ["Query","Search","Indexing","Storage","Android","IOS"]
        marketing = ["East","West","North","South","International"]
        cities = ['Mumbai','Delhi','New York','San Francisco']
        streets = ['21st street','12th street','18th street']
        countries = ['USA','INDIA','EUROPE']
        template = '{{ "name":{0}  , "department": "{1}" , "join_yr":{2},'
        template += ' "email":"{3}", "hobbies": {{ "hobby" : {4} }},'
        template += ' "tasks":  {5},  '
        template += '"VMs": {6} , '
        template += '"address" : {7} }}'
        count = 1

        for dept in department:
            for month in join_mo:
                for day in join_day:
                    random.seed(count)
                    count += 1
                    prefix = "employee" + str(random.random() * 100000)
                    # array of  single objects
                    name = [{"FirstName": "employeefirstname-%s" % (str(day))},
                            {"MiddleName": "employeemiddlename-%s" % (str(day))},
                            {"LastName": "employeelastname-%s" % (str(day))}]

                    # array inside array inside object
                    sportValue = random.sample(sport, 3)
                    danceValue = random.sample(dance, 3)
                    hobbies = [{"sports": sportValue}, {"dance": danceValue},"art"]
                    email = ["%s-mail@couchbase.com" % (str(day))]
                    joining = random.sample(join_yr,3)
                    # array inside array
                    enggValue = random.sample(engineer, 2)
                    marketingValue = [{"region1" :random.choice(marketing),"region2" :random.choice(marketing)},{"region2" :random.choice(marketing)}]
                    taskValue = [{"Developer": enggValue,"Marketing": marketingValue},"Sales","QA"]
                    # array of multiple objects
                    vms = [{"RAM": month, "os": "ubuntu",
                            "name": "vm_%s" % month, "memory": month},
                           {"RAM": month, "os": "windows",
                            "name": "vm_%s" % (month + 1), "memory": month},
                           {"RAM": month, "os": "centos", "name": "vm_%s" % (month + 2), "memory": month},
                           {"RAM": month, "os": "macos", "name": "vm_%s" % (month + 3), "memory": month}
                           ]

                    addressvalue = [[ {"city": random.choice(cities)},{"street":random.choice(streets)}],[{"apartment":123,"country":random.choice(countries)}]]

                    generators.append(DocumentGenerator("query-test" + prefix,
                                                        template,
                                                        [name], [dept], [[ y for y in joining]],
                                                        email, [hobbies],
                                                        [taskValue],
                                                        [vms],[addressvalue],
                                                        start=start, end=docs_per_day))

        return generators

    def generate_docs_sabre(self, docs_per_day=1, start=0, isShuffle=False, years=2, indexes=[1,4,8]):
        generators = []
        all_airports = ["ABR", "ABI", "ATL","BOS", "BUR", "CHI", "MDW", "DAL", "SFO", "SAN", "SJC", "LGA", "JFK", "MSP",
                        "MSQ", "MIA", "LON", "DUB"]
        dests = [all_airports[i] for i in indexes]
        join_yr = self._shuffle(range(2010, 2010 + years), isShuffle)
        join_mo = self._shuffle(range(1, 12 + 1),isShuffle)
        join_day = self._shuffle(range(1, 28 + 1),isShuffle)
        template = '{{ "Amount":{0}, "CurrencyCode":"{1}",'
        template += ' "TotalTax":{{"DecimalPlaces" : {2}, "Amount" : {3}, "CurrencyCode" : "{4}"}},'
        template += ' "Tax":{5}, "FareBasisCode":{6}, "PassengerTypeQuantity":{7}, "TicketType":"{8}",'
        template += '"SequenceNumber": {9},'
        template += ' "DirectionInd" : "{10}",  "Itinerary" : {11}, "Destination" : "{12}",'
        template += '"join_yr":{13}, "join_mo":{14}, "join_day":{15}, "Codes":{16}}}'
        count = 1
        for dest in dests:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        random.seed(count)
                        count +=1
                        prefix = '%s_%s-%s-%s' % (dest, year, month, day)
                        amount = [float("%s.%s" % (month, month))]
                        currency = [("USD", "EUR")[month in [1,3,5]]]
                        decimal_tax = [1,2]
                        amount_tax = [day]
                        currency_tax = currency
                        taxes = [{"DecimalPlaces": 2, "Amount": float(amount_tax[0])/3,
                                  "TaxCode": "US1", "CurrencyCode": currency},
                                 {"DecimalPlaces": 2, "Amount": float(amount_tax[0])/4,
                                  "TaxCode": "US2", "CurrencyCode": currency},
                                 {"DecimalPlaces": 2, "Amount": amount_tax[0] - float(amount_tax[0])/4-\
                                  float(amount_tax[0])/3,
                                  "TaxCode": "US2", "CurrencyCode": currency}]

                        fare_basis = [{"content": "XA21A0NY", "DepartureAirportCode": dest,
                                       "BookingCode": "X", "ArrivalAirportCode": "MSP"},
                                      {"content": "XA21A0NY", "DepartureAirportCode": "MSP",
                                       "AvailabilityBreak": True, "BookingCode": "X",
                                       "ArrivalAirportCode": "BOS"}]
                        pass_amount = [day]
                        ticket_type = [("eTicket", "testType")[month in [1,3,5]]]
                        sequence = [year]
                        direction = [("oneWay", "return")[month in [2,6,10]]]
                        itinerary = {"OriginDestinationOptions":
                                     {"OriginDestinationOption": [
                                       {"FlightSegment": [
                                         {"TPA_Extensions":
                                           {"eTicket": {"Ind": True}},
                                           "MarketingAirline": {"Code": dest},
                                           "StopQuantity": month,
                                           "DepartureTimeZone": {"GMTOffset": -7},
                                           "OperatingAirline": {"Code": "DL",
                                                                "FlightNumber": year + month},
                                           "DepartureAirport": {"LocationCode": "SFO"},
                                           "ArrivalTimeZone": {"GMTOffset": -5},
                                           "ResBookDesigCode": "X",
                                           "FlightNumber": year + day,
                                           "ArrivalDateTime": "2014-07-12T06:07:00",
                                           "ElapsedTime": 212,
                                           "Equipment": {"AirEquipType": 763},
                                           "DepartureDateTime": "2014-07-12T00:35:00",
                                           "MarriageGrp": "O",
                                           "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}},
                                        {"TPA_Extensions":
                                           {"eTicket": {"Ind": False}},
                                           "MarketingAirline": {"Code": dest},
                                           "StopQuantity": month,
                                           "DepartureTimeZone": {"GMTOffset": -7},
                                           "OperatingAirline": {"Code": "DL",
                                                                "FlightNumber": year + month + 1},
                                           "DepartureAirport": {"LocationCode": random.sample(all_airports, 1)},
                                           "ArrivalTimeZone": {"GMTOffset": -3},
                                           "ResBookDesigCode": "X",
                                           "FlightNumber": year + day,
                                           "ArrivalDateTime": "2014-07-12T06:07:00",
                                           "ElapsedTime": 212,
                                           "Equipment": {"AirEquipType": 764},
                                           "DepartureDateTime": "2014-07-12T00:35:00",
                                           "MarriageGrp": "1",
                                           "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}}],
                                    "ElapsedTime": 619},
                                   {"FlightSegment": [
                                         {"TPA_Extensions":
                                           {"eTicket": {"Ind": True}},
                                           "MarketingAirline": {"Code": dest},
                                           "StopQuantity": month,
                                           "DepartureTimeZone": {"GMTOffset": -7},
                                           "OperatingAirline": {"Code": "DL",
                                                                "FlightNumber": year + month},
                                           "DepartureAirport": {"LocationCode": random.sample(all_airports, 1)},
                                           "ArrivalTimeZone": {"GMTOffset": -5},
                                           "ResBookDesigCode": "X",
                                           "FlightNumber": year + day,
                                           "ArrivalDateTime": "2014-07-12T06:07:00",
                                           "ElapsedTime": 212,
                                           "Equipment": {"AirEquipType": 763},
                                           "DepartureDateTime": "2014-07-12T00:35:00",
                                           "MarriageGrp": "O",
                                           "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}},
                                        {"TPA_Extensions":
                                           {"eTicket": {"Ind": False}},
                                           "MarketingAirline": {"Code": dest},
                                           "StopQuantity": month,
                                           "DepartureTimeZone": {"GMTOffset": -7},
                                           "OperatingAirline": {"Code": "DL",
                                                                "FlightNumber": year + month + 1},
                                           "DepartureAirport": {"LocationCode": random.sample(all_airports, 1)},
                                           "ArrivalTimeZone": {"GMTOffset": -3},
                                           "ResBookDesigCode": "X",
                                           "FlightNumber": year + day,
                                           "ArrivalDateTime": "2014-07-12T06:07:00",
                                           "ElapsedTime": 212,
                                           "Equipment": {"AirEquipType": 764},
                                           "DepartureDateTime": "2014-07-12T00:35:00",
                                           "MarriageGrp": "1",
                                           "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}}]}]},
                                     "DirectionInd": "Return"}
                        generators.append(DocumentGenerator(prefix, template,
                                               amount, currency, decimal_tax, amount_tax, currency_tax,
                                               [taxes], [fare_basis], pass_amount, ticket_type, sequence,
                                               direction, [itinerary], [dest], [year], [month], [day],
                                               [[dest, dest]], start=start, end=docs_per_day))
        return generators

    def generate_docs_sales(self, key_prefix = "sales_dataset", test_data_type = True, start=0, docs_per_day=None, isShuffle = False):
        generators = []
        if end is None:
            end = self.docs_per_day
        join_yr = self._shuffle(list(range(2008, 2008 + self.years)),isShuffle)
        join_mo = self._shuffle(list(range(1, self.months + 1)),isShuffle)
        join_day = self._shuffle(list(range(1, self.days + 1)),isShuffle)
        count = 1
        if test_data_type:
            template = '{{ "join_yr" : {0}, "join_mo" : {1}, "join_day" : {2},'
            template += ' "sales" : {3}, "delivery_date" : "{4}", "is_support_included" : {5},'
            template += ' "is_high_priority_client" : {6}, "client_contact" :  "{7}",'
            template += ' "client_name" : "{8}", "client_reclaims_rate" : {9}}}'
            sales = self._shuffle([200000, 400000, 600000, 800000],isShuffle)

            is_support = self._shuffle(['true', 'false'],isShuffle)
            is_priority = self._shuffle(['true', 'false'],isShuffle)
            contact = "contact_"+str(random.random()*10000000)
            name ="name_"+str(random.random()*100000)
            rate = [x * 0.1 for x in range(0, 10)]
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        random.seed(count)
                        count +=1
                        prefix = "prefix_"+str(random.random()*100000)
                        delivery = str(datetime.date(year, month, day))
                        generators.append(DocumentGenerator(key_prefix + prefix,
                                                  template,
                                                  [year], [month], [day],
                                                  sales, [delivery], is_support,
                                                  is_priority, [contact],
                                                  [name], rate,
                                                  start=start, end=end))
        return generators

    def generate_docs_bigdata(self, key_prefix = "big_dataset", value_size = 1024, start=0, docs_per_day=1, end=None):
        if end is None:
            end = docs_per_day
        age = list(range(start, end))
        name = ['a' * value_size,]
        template = '{{ "age": {0}, "name": "{1}" }}'

        gen_load = DocumentGenerator(key_prefix, template, age, name, start=start,
                                     end=end)
        return [gen_load]

    def generate_docs_simple(self, key_prefix ="simple_dataset", start=0, docs_per_day = 1000, isShuffle = False):
        end = docs_per_day
        age = self._shuffle(list(range(start, end)), isShuffle)
        name = [key_prefix + '-' + str(i) for i in self._shuffle(range(start, end), isShuffle)]
        template = '{{ "age": {0}, "name": "{1}" }}'
        gen_load = DocumentGenerator(key_prefix, template, age, name, start=start, end=end)
        return [gen_load]

    def generate_docs_array(self, key_prefix="array_dataset", start=0, docs_per_day=1, isShuffle=False):
        COUNTRIES = ["India", "US", "UK", "Japan", "France", "Germany", "China", "Korea", "Canada", "Cuba",
             "West Indies", "Australia", "New Zealand", "Nepal", "Sri Lanka", "Pakistan", "Mexico",
             "belgium", "Netherlands", "Brazil", "Costa Rica", "Cambodia", "Fiji", "Finland", "haiti",
             "Hong Kong", "Iceland", "Iran", "Iraq", "Italy", "Greece", "Jamaica", "Kenya", "Kuwait", "Macau",
             "Spain","Morocco", "Maldives", "Norway"]

        COUNTRY_CODE = ["Ind123", "US123", "UK123", "Jap123", "Fra123", "Ger123", "Chi123", "Kor123", "Can123",
                "Cub123", "Wes123", "Aus123", "New123", "Nep123", "Sri123", "Pak123", "Mex123", "bel123",
                "Net123", "Bra123", "Cos123", "Cam123", "Fij123", "Fin123", "hai123", "Hon123", "Ice123",
                "Ira123", "Ira123", "Ita123", "Gre123", "Jam123", "Ken123", "Kuw123", "Mac123", "Spa123",
                "Mor123", "Mal123", "Nor123"]
        end = docs_per_day
        generators = []
        template = '{{"name": "{0}", "email": "{1}", \
                   "countries": {2}, "code": {3}}}'
        for i in range(start, end):
            countries = []
            codes = []
            name = ["Passenger-{0}".format(i)]
            email = ["passenger_{0}@abc.com".format(i)]
            start_pnt = random.randint(0, len(COUNTRIES)-2)
            end_pnt = random.randint(start_pnt, len(COUNTRIES)-1)
            cnt = COUNTRIES[start_pnt:end_pnt]
            countries.append(cnt)
            cde = COUNTRY_CODE[start_pnt:end_pnt]
            codes.append(cde)
            prefix = "{0}-{1}".format(key_prefix,i)
            generators.append(DocumentGenerator(prefix, template,
                                                name, email, countries, codes,  start=start, end=end))
        return generators

    def generate_all_type_documents_for_gsi(self, start=0, docs_per_day=10):
        """
        Document fields:
        name: String
        age: Number
        email: Alphanumeric + Special Character
        premium_customer: Boolean or <NULL>
        Address: Object
                {Line 1: Alphanumeric + Special Character
                Line 2: Alphanumeric + Special Character or <NULL>
                City: String
                Country: String
                postal_code: Number
                }
        travel_history: Array of string - Duplicate elements ["India", "US", "UK", "India"]
        travel_history_code: Array of alphanumerics - Duplicate elements
        booking_history: Array of objects
                        {source:
                         destination:
                          }
        credit_cards: Array of numbers
        secret_combination: Array of mixed data types
        countries_visited: Array of strings - non-duplicate elements

        :param start:
        :param docs_per_day:
        :param isShuffle:
        :return:
        """
        generators = []
        bool_vals = [True, False]
        template = r'{{ "name":"{0}", "email":"{1}", "age":{2}, "premium_customer":{3}, ' \
                   '"address":{4}, "travel_history":{5}, "travel_history_code":{6}, "travel_details":{7},' \
                   '"booking":{8}, "credit_cards":{9}, "secret_combination":{10}, "countries_visited":{11}, ' \
                   '"question_values":{12}}}'
        for i in range(docs_per_day):
            name = random.choice(FIRST_NAMES)
            age = random.randint(25, 70)
            last_name = random.choice(LAST_NAMES)
            dob = "{0}-{1}-{2}".format(random.randint(1970, 1999),
                                       random.randint(1, 28), random.randint(1, 12))
            email = "{0}.{1}.{2}@abc.com".format(name, last_name, dob.split("-")[1])
            premium_customer = random.choice(bool_vals)
            address = {}
            address["line_1"] = "Street No. {0}".format(random.randint(100, 200))
            address["line_2"] = "null"
            if not random.choice(bool_vals):
                address["address2"] = "Building {0}".format(random.randint(1, 6))
            address["city"] = "Bangalore"
            address["contact"] = "{0} {1}".format(name, last_name)
            address["country"] = "India"
            address["postal_code"] = "{0}".format(random.randint(560071, 560090))
            credit_cards = [random.randint(-1000000, 9999999) for i in range(random.randint(3, 7))]
            secret_combo = [''.join(random.choice(string.ascii_lowercase) for i in range(7)),
                            random.randint(1000000, 9999999)]
            travel_history = [random.choice(COUNTRIES[:9]) for i in range(1, 11)]
            travel_history_code = [COUNTRY_CODE[COUNTRIES.index(i)] for i in travel_history]
            travel_details = [{"country": travel_history[i], "code": travel_history_code[i]}
                              for i in range(len(travel_history))]
            countries_visited = list(set(travel_history))
            booking = {"source": random.choice(COUNTRIES), "destination": random.choice(COUNTRIES)}
            confirm_question_values = [random.choice(bool_vals) for i in range(5)]
            prefix = "airline_record_" + str(random.random()*100000)
            generators.append(DocumentGenerator(prefix, template, [name], [email], [age], [premium_customer],
                                                [address], [travel_history], [travel_history_code], [travel_details],
                                                [booking], [credit_cards], [secret_combo], [countries_visited],
                                                [confirm_question_values], start=start, end=1))
        return generators

    def generate_doc_for_aggregate_pushdown(self, start=0, docs_per_day=10):
        generators = []
        bool_vals = [True, False]
        template = r'{{ "name":"{0}", "business_name": "{1}", "age":{2}, "weight": {3}, "debt":{4}, "passwords":{5}, ' \
                   r'"transactions":{6}, "address":{7}, "travel_history":{8}, "credit_cards":{9}}}'
        for i in range(docs_per_day):
            name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            business_name = name + "_" + last_name
            age = random.randint(25, 70)
            weight = round(random.uniform(50.1, 115.5), 2)
            debt = random.randint(-999999, -100000)
            passwords = []
            for i in range(random.randint(10, 50)):
                passwords.append(random.randint(-10000, 99999))
                passwords.append(''.join(random.choice(string.ascii_lowercase) for _ in range(9)))
            transactions = [random.randint(1000, 9999999) for _ in range(random.randint(10, 50))]
            address = {}
            address["country"] = random.choice(COUNTRIES)
            address["postal_code"] = random.randint(200000, 800000)
            travel_history = [random.choice(COUNTRIES[:9]) for i in range(1, 11)]
            credit_cards = [random.randint(-1000000, 9999999) for i in range(random.randint(3, 7))]
            prefix = "bank_record_" + str(random.random()*100000)
            generators.append(DocumentGenerator(prefix, template, [name], [business_name], [age], [weight], [debt],
                                                [passwords], [transactions], [address], [travel_history],
                                                [credit_cards], start=start, end=1))
        return generators

    def generate_docs_employee_data(self, key_prefix ="employee_dataset", start=0, docs_per_day = 1, isShuffle = False):
        generators = []
        count = 1
        sys_admin_info = {"title" : "System Administrator and heliport manager",
                              "desc" : "...Last but not least, as the heliport manager, you will help maintain our growing fleet of remote controlled helicopters, that crash often due to inexperienced pilots.  As an independent thinker, you may be free to replace some of the technologies we currently use with ones you feel are better. If so, you should be prepared to discuss and debate the pros and cons of suggested technologies with other stakeholders",
                              "type" : "admin"}
        ui_eng_info = {"title" : "UI Engineer",
                           "desc" : "Couchbase server UI is one of the crown jewels of our product, which makes the Couchbase NoSQL database easy to use and operate, reports statistics on real time across large clusters, and much more. As a Member of Couchbase Technical Staff, you will design and implement front-end software for cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure.",
                            "type" : "ui"}
        senior_arch_info = {"title" : "Senior Architect",
                               "desc" : "As a Member of Technical Staff, Senior Architect, you will design and implement cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure. More specifically, you will bring Unix systems and server tech kung-fu to the team.",
                               "type" : "arch"}
        data_sets = self._shuffle([sys_admin_info, ui_eng_info, senior_arch_info],isShuffle)
        if end is None:
            end = self.docs_per_day
        join_yr = self._shuffle(list(range(2008, 2008 + self.years)),isShuffle)
        join_mo = self._shuffle(list(range(1, self.months + 1)),isShuffle)
        join_day = self._shuffle(list(range(1, self.days + 1)),isShuffle)
        name = ["employee-%s-%s" % (key_prefix, str(i)) for i in range(start, end)]
        email = ["%s-mail@couchbase.com" % str(i) for i in range(start, end)]
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "email":"{4}", "job_title":"{5}", "type":"{6}", "desc":"{7}"}}'
        for info in data_sets:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        random.seed(count)
                        prefix = str(random.random()*100000)
                        generators.append(DocumentGenerator(key_prefix + prefix,
                                               template,
                                               name, [year], [month], [day],
                                               email, [info["title"]],
                                               [info["type"]], [info["desc"]],
                                               start=start, end=docs_per_day))
        return generators

    def generate_docs_using_monster(self,
            executatble_path = None, key_prefix=  "", bag_dir = "lib/couchbase_helper/monster/bags",
            pod_name = None, num_items = 1, seed = None):
        "This method runs monster tool using localhost, creates a map of json based on a pattern"
        list = []
        command = executatble_path
        dest_path = "/tmp/{0}.txt".format(int(random.random()*1000))
        if pod_name == None:
            return list
        else:
            pod_path = "lib/couchbase_helper/monster/prod/%s" % pod_name
        command += " -bagdir {0}".format(bag_dir)
        if seed != None:
            command += " -s {0}".format(seed)
        command += " -n {0}".format(num_items)
        command += " -o {0}".format(dest_path)
        if pod_path != None:
            command += " {0}".format(pod_path)
        print("Will run the following command: {0}".format(command))
        # run command and generate temp file
        os.system(command)
        # read file and generate list
        with open(dest_path) as f:
            i= 1
            for line in f.readlines():
                key = "{0}{1}".format(key_prefix,i)
                data = json.loads(line[:len(line)-1])
                data["_id"] = key
                data["mutate"] = 0
                list.append(data)
                i+=1
        os.remove(dest_path)
        return list

    def _shuffle(self, data, isShuffle):
        if isShuffle:
            if not isinstance(data, list):
                data = [x for x in data]
            random.shuffle(data)
            return data
        return data
