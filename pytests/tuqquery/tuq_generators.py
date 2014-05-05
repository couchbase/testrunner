import re

class TuqGenerators(object):
    
    def __init__(self, log, full_set):
        self.log = log
        self.full_set = full_set
        self.query = None
        self.type_args = {}
        self.type_args['str'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], unicode)]
        self.type_args['int'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], int)]
        self.type_args['float'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], float)]
        self.type_args['bool'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], bool)]
        self.type_args['list_str'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], unicode)]
        self.type_args['list_obj'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], dict)]
        self.type_args['obj'] = [attr[0] for attr in full_set[0].iteritems()
                             if isinstance(attr[1], dict)]
        for obj in self.type_args['obj']:
            self.type_args['_obj%s_str' % (self.type_args['obj'].index(obj))] = [attr[0] for attr in full_set[0][obj].iteritems()
                                                                                    if isinstance(attr[1], str)]
            self.type_args['_obj%s_int'% (self.type_args['obj'].index(obj))] = [attr[0] for attr in full_set[0][obj].iteritems()
                                                                                    if isinstance(attr[1], int)]
        for obj in self.type_args['list_obj']:
            self.type_args['_list_obj%s_str' % (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].iteritems()
                                                                                    if isinstance(attr[1], str)]
            self.type_args['_list_obj%s_int'% (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].iteritems()
                                                                                    if isinstance(attr[1], int)]
        self.distict = False
        self.aggr_fns = {}
        self.aliases = {}
        self.attr_order_clause_greater_than_select = []

    def generate_query(self, template):
        query = template
        for name_type, type_arg in self.type_args.iteritems():
            for attr_type_arg in type_arg:
                query = query.replace('$%s%s' % (name_type, type_arg.index(attr_type_arg)), attr_type_arg)
        for expr in [' where ', ' select ', ' from ', ' order by', ' limit ', ' offset ', ' count(' , 'group by', 'unnest']:
            query = query.replace(expr, expr.upper())
        self.log.info("Generated query to be run: '''%s'''" % query)
        self.query = query
        return query

    def generate_expected_result(self):
        self._create_alias_map()
        where_clause = self._format_where_clause()
        from_clause = self._format_from_clause()
        unnest_clause = self._format_unnest_clause(from_clause)
        select_clause = self._format_select_clause()
        result = self._filter_full_set(select_clause, where_clause, unnest_clause)
        result = self._order_results(result)
        result = self._limit_and_offset(result)
        return result

    def _create_alias_map(self):
        query_dict = self.query.split()
        for word in query_dict:
            if word.upper() == 'AS':
                self.aliases[query_dict[query_dict.index(word) + 1]] = query_dict[query_dict.index(word) - 1]

    def _format_where_clause(self):
        if self.query.find('WHERE') == -1:
            return None
        clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*WHERE', '', self.query))
        attributes = [attr for name, group in self.type_args.iteritems()
                      for attr in group if not name.startswith('_')]
        conditions = clause.replace('IS NULL', 'is None')
        conditions = conditions.replace('IS NOT NULL', 'is not None')
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
            conditions = conditions.replace(' %s ' % attr, ' doc["%s"] ')
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
            attributes = [att for name, group in self.type_args.iteritems()
                      for att in group if not name.startswith('_')]
            if attr[0].find('.') != -1:
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
            attributes = [att for name, group in self.type_args.iteritems()
                      for att in group if not name.startswith('_')]
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

    def _format_select_clause(self):
        select_clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*SELECT', '', self.query)).strip()
        select_clause = re.sub(r'WHERE.*', '', re.sub(r'FROM.*', '', select_clause)).strip()
        select_attrs = select_clause.split(',')
        condition = '{'
        #handle aliases
        for attr_s in select_attrs:
            attr = attr_s.split()
            if re.match(r'COUNT\(.*\)', attr[0]):
                    attr[0] = re.sub(r'\)', '', re.sub(r'.*COUNT\(', '', attr[0])).strip()
                    self.aggr_fns['COUNT'] = {}
                    if attr[0].upper() == 'DISTINCT':
                        attr = attr[1:]
                        self.distict= True
                    if attr[0].find('.') != -1:
                        parent, child = attr[0].split('.')
                        attr[0] = child
                    if attr[0] in self.aliases:
                        attr[0] = self.aliases[attr[0]]
                    self.aggr_fns['COUNT']['field'] = attr[0]
                    self.aggr_fns['COUNT']['alias'] = ('$1', attr[-1])[len(attr) > 1]
            elif attr[0].upper() == 'DISTINCT':
                attr = attr[1:]
                self.distict= True
            if len(attr) == 1:
                if attr[0].find('.') != -1:
                    condition += '"%s" : doc["%s"]["%s"],' % (attr[0], attr[0].split('.')[0], attr[0].split('.')[1])
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
                    condition += '"%s" : doc["%s"],' % (attr[2], attr[0])
        condition += '}'
        return condition

    def _filter_full_set(self, select_clause, where_clause, unnest_clause):
        if self._order_clause_greater_than_select(select_clause):
            select_clause = select_clause[:-1] + ','.join(['"%s" : {"%s" : %s}' %([at.replace('"','') for at in re.compile('"\w+"').findall(attr)][0],
                                                                                  [at.replace('"','') for at in re.compile('"\w+"').findall(attr)][1],
                                                attr) for attr in self._order_clause_greater_than_select(select_clause)]) + '}'
        if where_clause:
            result = [eval(select_clause) for doc in self.full_set if eval(where_clause)]
        else:
            result = [eval(select_clause) for doc in self.full_set]
        if self.distict:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        if unnest_clause:
            result = [item for doc in self.full_set for item in eval(unnest_clause)]
        if self.aggr_fns:
            for fn_name, params in self.aggr_fns.iteritems():
                if fn_name == 'COUNT':
                    result = [{params['alias'] : len(result)}]
        return result

    def _order_clause_greater_than_select(self, select_clause):
        order_clause = self._get_order_clause()
        if not order_clause:
            return None
        diff = set(order_clause.split(',')) - set(re.compile('doc\["[\w\']+"\]').findall(select_clause))
        diff = [attr for attr in diff if attr != '']
        if diff:
            self.attr_order_clause_greater_than_select = [re.sub(r'"\].*', '', re.sub(r'doc\["', '', attr)) for attr in diff]
            return diff
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
            if attr[0].find('.') != -1:
                condition += 'doc["%s"]["%s"],' % (attr[0].split('.')[0],attr[0].split('.')[1])
            else:
                condition += 'doc["%s"],' % attr[0]
        return condition

    def _order_results(self, result):
        order_clause = self._get_order_clause()
        key = None
        reverse = False
        if order_clause:
            order_attrs = order_clause.split(',')
            for attr_s in order_attrs:
                attr = attr_s.split()
                if len(attr) == 2 and attr[1].upper() == 'DESC':
                    reverse = True
            key = lambda doc: eval(order_clause)
        result = sorted(result, key=key, reverse=reverse)
        if self.attr_order_clause_greater_than_select:
            for doc in result:
                for attr in self.attr_order_clause_greater_than_select:
                    if attr.find('.') != -1:
                        attr = attr.split('.')[0]
                    del doc[attr]
        return result

    def _limit_and_offset(self, result):
        limit_clause = offset_clause = None
        if self.query.find('LIMIT') != -1:
            limit_clause = re.sub(r'OFFSET.*', '', re.sub(r'.*LIMIT', '', self.query)).strip()
        if self.query.find('OFFSET') != -1:
            offset_clause = re.sub(r'.*OFFSET', '', self.query).strip()
        if limit_clause:
            result = result[:int(limit_clause)]
        if offset_clause:
            result = result[int(limit_clause):]
        return result
