import re
import logger

log = logger.Logger.get_logger()

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
                                                                                    if isinstance(attr[1], str) or isinstance(attr[1], unicode)]
            self.type_args['_list_obj%s_int'% (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].iteritems()
                                                                                    if isinstance(attr[1], int)]
        self._clear_current_query()

    def generate_query(self, template):
        query = template
        for name_type, type_arg in self.type_args.iteritems():
            for attr_type_arg in type_arg:
                query = query.replace('$%s%s' % (name_type, type_arg.index(attr_type_arg)), attr_type_arg)
        for expr in [' where ', ' select ', ' from ', ' order by', ' limit ', 'end',
                     ' offset ', ' count(' , 'group by', 'unnest', 'min', 'satisfies']:
            query = query.replace(expr, expr.upper())
        self.log.info("Generated query to be run: '''%s'''" % query)
        self.query = query
        return query

    def generate_expected_result(self):
        try:
            self._create_alias_map()
            where_clause = self._format_where_clause()
            log.info("WHERE clause ===== is %s" % where_clause)
            from_clause = self._format_from_clause()
            log.info("FROM clause ===== is %s" % from_clause)
            unnest_clause = self._format_unnest_clause(from_clause)
            log.info("UNNEST clause ===== is %s" % unnest_clause)
            select_clause = self._format_select_clause()
            log.info("SELECT clause ===== is %s" % select_clause)
            result = self._filter_full_set(select_clause, where_clause, unnest_clause)
            result = self._order_results(result)
            result = self._limit_and_offset(result)
            log.info("Expected result is %s ..." % str(result[:15]))
            return result
        finally:
            self._clear_current_query()

    def _create_alias_map(self):
        query_dict = self.query.split()
        for word in query_dict:
            if word.upper() == 'AS':
                self.aliases[query_dict[query_dict.index(word) + 1]] = query_dict[query_dict.index(word) - 1]

    def _format_where_clause(self):
        if self.query.find('WHERE') == -1:
            return None
        clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*WHERE', '', self.query))
        clause = re.sub(r'GROUP BY.*', '', clause)
        attributes = self.get_all_attributes()
        conditions = clause.replace('IS NULL', 'is None')
        conditions = conditions.replace('IS NOT NULL', 'is not None')
        satisfy_expr = self.format_satisfy_clause()
        if satisfy_expr:
            conditions = re.sub(r'ANY.*END', '', clause)
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
            conditions += '' + satisfy_expr
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
                    if attr[0] == '*':
                        condition += '"%s" : doc,' % attr[-1]
                    continue
            elif attr[0].upper() == 'DISTINCT':
                attr = attr[1:]
                self.distict= True
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
                        condition += '"%s" : {%s : doc["%s"]["%s"]},' % (attr[0].split('.')[0], attr[0].split('.')[1],
                                                                         attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[0], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
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
        return condition

    def _filter_full_set(self, select_clause, where_clause, unnest_clause):
        diff = self._order_clause_greater_than_select(select_clause)
        if diff and not self._is_parent_selected(select_clause, diff):
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
        if where_clause:
            result = [eval(select_clause) for doc in self.full_set if eval(where_clause)]
        else:
            result = [eval(select_clause) for doc in self.full_set]
        if self.distict:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        if unnest_clause:
            result = [item for doc in self.full_set for item in eval(unnest_clause)]
        if self._create_groups()[0]:
            result = self._group_results(result)
        if self.aggr_fns:
            if not self._create_groups()[0]:
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
        if not set(diff) - set(['doc["%s"]' % alias for alias in self.aliases]):
            return None
        else:
            diff = list(set(diff) - set(['doc["%s"]' % alias for alias in self.aliases]))
        if diff:
            self.attr_order_clause_greater_than_select = [re.sub(r'"\].*', '', re.sub(r'doc\["', '', attr)) for attr in diff]
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
            if attr[0] in self.aliases.itervalues():
                    condition += 'doc["%s"],' % (self.get_alias_for(attr[0]))
                    continue
            if attr[0].find('MIN') != -1:
                self.aggr_fns['MIN'] = {}
                attr[0]= attr[0][4:-1]
                self.aggr_fns['MIN']['field'] = attr[0]
                self.aggr_fns['MIN']['alias'] = '$gr1'
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
                if att_name[1:-1] in self.aliases.itervalues():
                    order_clause = order_clause.replace(att_name[1:-1],
                                                        self.get_alias_for(att_name[1:-1]))
                if self.aggr_fns and att_name[1:-1] in [params['field'] for params in self.aggr_fns.itervalues()]:
                    order_clause = order_clause.replace(att_name[1:-1],
                                                        [params['alias'] for params in self.aggr_fns.itervalues()
                                                         if params['field'] == att_name[1:-1]][0])
            key = lambda doc: eval(order_clause)
        result = sorted(result, key=key, reverse=reverse)
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
        if limit_clause:
            result = result[:int(limit_clause)]
        if offset_clause:
            result = result[int(limit_clause):]
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
                groups = set([doc[attrs[0].split('.')[0]][attrs[0].split('.')[1]]
                              for doc in self.full_set])
            else:
                groups = set([doc[attrs[0]]  for doc in self.full_set])
        return attrs, groups

    def _group_results(self, result):
        attrs, groups = self._create_groups()
        for fn_name, params in self.aggr_fns.iteritems():
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
                    if attrs[0] in self.aliases.itervalues():
                        attrs[0] = self.get_alias_for(attrs[0])
                    result = [{attrs[0] : group,
                                params['alias'] : min([doc[params['field']] for doc in result
                                if doc[attrs[0]]==group])}
                          for group in groups]
        else:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        return result

    def get_alias_for(self, value_search):
        for key, value in self.aliases.iteritems():
            if value == value_search:
                return key
        return ''

    def get_all_attributes(self):
        return [att for name, group in self.type_args.iteritems()
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
        if self.query.find('ANY') != -1:
            result_clause = 'len([att for att in %s if ' % main_attr
        satisfy_expr = re.sub(r'.*SATISFIES', '', re.sub(r'END.*', '', satisfy_clause)).strip()
        for expr in satisfy_expr.split():
            if expr.find('.') != -1:
                result_clause += ' att["%s"] ' % expr.split('.')[1]
            elif expr.find('=') != -1:
                result_clause += ' == '
            elif expr.upper() in ['AND', 'OR', 'NOT']:
                result_clause += expr.lower()
            else:
                result_clause += ' %s ' % expr
        result_clause += ']) > 0'
        return result_clause

    def _clear_current_query(self):
        self.distict = False
        self.aggr_fns = {}
        self.aliases = {}
        self.attr_order_clause_greater_than_select = []
        self.parent_selected = False
