
from __future__ import print_function

from collections import OrderedDict
import sys, os

from .schema import Schema, ResultSet


class SQLResultSet(ResultSet):

	def __init__(self, schema, name, select_columns=None, dict_record=False):
		super(SQLResultSet, self).__init__(
			schema, name, select_columns=select_columns, dict_record=dict_record
		)

	def update_operator(self, left, op, right):
		update_operators = {
			'=': lambda l, r: u'%s=%s' % (l, r),
			':': lambda l, r: u'%s=%s' % (l, r),
			'+': lambda l, r: u'%s=%s+%s' % (l, l, r),
			'||': lambda l, r: u'%s=%s' % (l, self.schema.render_concat(l, r)),
			'-': lambda l, r: u'%s=%s-%s' % (l, l, r),
			'*': lambda l, r: u'%s=%s*%s' % (l, l, r),
			'/': lambda l, r: u'%s=%s/%s' % (l, l, r),
		}
		if len(op) == 2 and op[1] == '=':
			op = op[0]
		if op in update_operators:
			param = self.schema.render_paramplace
			return update_operators[op](left, param), [right]
		return '', []


	def create(self, **kw):
		new_key, auto_increment = self.primary_key(kw)
		fields, values = self.fields_sanitize(False, kw)

		insert = u'insert into %s (%s) values(%s)\n' % (
			self.schema.render_name(self.entity['table']), 
			','.join(fields), 
			','.join([self.schema.render_paramplace] * len(values))
		)
		pk_fields = self.entity['primary_key']
		new_key = self.schema.perform_insert(
			insert, values, pk_fields, self.entity['table'], 
			None if auto_increment else new_key)

		try:
			return self.find(*[new_key]).__iter__().next()
		except:
			return self.find(*[new_key]).__iter__().__next__()

	def do_filter(self, filter=None):
		select_from = u'select %s\nfrom %s\n' % (
			','.join([
				self.schema.render_name(field) \
				for field in self.select_columns
			]),
			self.schema.render_name(self.entity['table'])
		)
		where, values = self.render_where(filter or self.filter)

		cursor = self.schema.db_execute(select_from + where, values)

		for record in cursor.fetchall():
			yield(record)

	def update(self, **kw):
		assignments, values = list(), list()

		for field, value in zip(*self.fields_sanitize(True, kw)):
			assignment, value = self.update_operator(field, *value)
			assignments.append(assignment)
			values += value

		update = 'update %s\nset%s ' % (
			self.schema.render_name(self.entity['table']),
			u',\n'.join(assignments)
		)
		where, values2 = self.render_where()
		sql = update + where

		self.schema.db_execute(sql, values + values2)

		return self

	def render_pk_in(self, args):
		sql = ''
		values = []
		pk = self.entity['primary_key']
		len_pk = len(pk)
		args = [
			(list(arg) if isinstance(arg, (list, tuple)) else [arg]) \
			for arg in args \
			if len_pk == 1 or \
				isinstance(arg, (list, tuple)) and len(arg) == len_pk
		]
		if args:
			if len_pk > 1:
				one_rec = u' and '.join([
					u'%s=%s' % (
						self.schema.render_name(field), 
						self.schema.render_paramplace, 
					) \
					for field in pk
				])
				sql = u'((%s))' % u') or \n('.join([one_rec] * len(args))
				values = reduce(lambda a, b,: a+b, args)
			else:
				sql = u'(%s in (%s))' % (
					self.schema.render_name(pk[0]), 
					','.join([self.schema.render_paramplace] * len(args))
				)
				values = [arg[0] for arg in args]
		return sql, values

	def render_where(self, filter=None):
		args, kw = filter or self.filter
		in_sql, values = self.render_pk_in(args)
		sql = [in_sql]
		for field, value in kw.items():
			if field not in self.entity['fields']:
				continue
			op, arg = '=', value
			if isinstance(value, (list, tuple)) and len(value) == 2:
				op, arg = value
			if isinstance(arg, (list, tuple)):
				if not self.operator_has_list(op):
					continue
				arg = [
					_arg for _arg in arg if not isinstance(_arg, (list, tuple))
				]
			else:
				arg = [arg]
			exp = u'(%s%s(%s))' % (
				self.schema.render_name(field), op, 
				','.join([self.schema.render_paramplace] * len(arg))
			)
			sql.append(exp)
			values += arg

		sql = u' and '.join([part for part in sql if part])
		where = (u'where (%s)' % sql) if sql else u''

		return where, values

	def find(self, *args, **kw):
		self.filter = args, kw
		return self

	def next(self):
		select = self.iterator.next()
		return self.do_next(select)

	def __next__(self):
		select = self.iterator.__next__()
		return self.do_next(select)


class SQLSchema(Schema):

	rs_class = SQLResultSet

	prelude = ""
	postfix = ""
	table_sufix = ""
	query_prefix = ""

	dump_extension = '.sql'

	_type_conv = dict()
	connection = None
	inline_domains = False
	default_delimiter = ";"
	oncreate_inline = True

	@staticmethod
	def render_string(value):
		return "'%s'" % value.replace("'", "''")

	@staticmethod
	def render_datetime(value, format='%Y-%m-%d %H:%M:%S'):
		return render_string(value.strftime(format))

	@staticmethod
	def render_date(value):
		return render_datetime(value, '%Y-%m-%d')

	@staticmethod
	def render_time(value):
		return render_datetime(value, '%H:%M:%S')

	@staticmethod
	def render_number(value):
		return str(value)

	@staticmethod
	def render_bool(value):
		return str(int(value))

	type_render = dict(
		integer='render_number', 
		tinyint='render_number', 
		smallint='render_number', 
		enum='render_string', 
		float='render_number', 
		decimal='render_number', 
		numeric='render_number', 
		char='render_string', 
		varchar='render_string', 
		text='render_string', 
		mediumtext='render_string', 
		date='render_date', 
		time='render_time', 
		datetime='render_datetime', 
		timestamp='render_datetime', 
		boolean='render_bool', 
	)

	inline_timestamps = True

	attr_render = dict(
		data_type=(None, lambda c, x: type_render.get(x)),
		is_nullable=('required', lambda c, x: not int(x)),
		default_value=('default', None), 
		size=('size', None), 
		extra=("domain", lambda c, x: x['list']),
		set_on_create=lambda c, x: int(x),
		set_on_update=lambda c, x: int(x),
		is_auto_increment=lambda c, x: int(x), 
	)

	render_pk = True
	inline_fk = True
	inline_domain = False
	triggers = list()
	trigger_actions = dict()
	trigger_templates = dict()

	check_constraints = list()
	fk_constraints = list()

	render_paramplace = '?'

	@staticmethod
	def render_concat(left, right):
		return '%s || %s' % (left, right)

	def type_conv(self, attrs, entity, name):
		data_type = self._type_conv.get(
			(attrs['data_type'], name), 
			self._type_conv.get(attrs['data_type'], attrs['data_type'])
		)
		return data_type

	def render_default(self):
		return ' default (%(default_value)s)'

	def render_name(self, name):
		return '[%s]' % name

	def render_autoincrement(self, attrs, entity, name):
		if 'is_auto_increment' in attrs and entity['primary_key'] != (name,):
			print(
				'autoincrement on %(table)s not the primary key' % entity, 
				file=sys.stderr)
			return dict(), None
		return attrs, ''

	def render_field(self, name, attrs, entity, prefix='\t'):
		attrs = dict(attrs)
		fields = entity['fields'].keys()

		attrs, res_autoincrement = self.render_autoincrement(
			attrs, entity, name)

		if 'data_type' not in attrs:
			raise Exception('no data type for %s.%s' % (name, entity['table']))

		attrs['data_type'] = self.type_conv(attrs, entity, name)
		_type_render = self.type_render[attrs['data_type']]
		_type_render = getattr(self, _type_render)

		res = u'%s %%(data_type)s' % self.render_name(name)

		if 'extra' in attrs:
			if 'list' in attrs['extra']:
				domain = ','.join([
					_type_render(part) for part in attrs['extra']['list']
				])
				if self.inline_domains:
					res += u'(%s)' % domain
				else:
					check = u'check (%s in (%s))' % (
						self.render_name(name), 
						domain
					)
					self.check_constraints.append(check)

		if 'size' in attrs:
			res += '(%(size)d)'

		getdate = self.getdate.get(attrs['data_type'], u'')
		params = dict(
			field=name, 
			getdate=getdate, 
			getdate_tr=getdate.replace('%', '%%'), 
			table=entity['table'], 
			default_delimiter=self.default_delimiter, 
		)

		if 'set_on_create' in attrs and name not in entity['primary_key']:
			trigger_actions = dict()
			trigger_templates = dict()
			triggers = list()

			if getattr(self, 'trigger_field_action_before', None):
				event = ('before', 'insert')
				trigger_actions.setdefault(event, list())
				trigger_actions[event].append(
					self.trigger_field_action_before['insert'] % params)
			if getattr(self, 'trigger_field_action_after', None):
				event = ('after', 'insert')
				trigger_actions.setdefault(event, list())
				trigger_actions[event].append(
					self.trigger_field_action_after % params)
			if getattr(self, 'on_create_trigger_template', None):
				stage, text = self.on_create_trigger_template
				trigger_templates[(stage, 'insert')] = text % params
			if getattr(self, 'on_create_trigger', None):
				triggers.append(self.on_create_trigger % params)
			if getattr(self, 'oncreate_inline', '') and self.inline_timestamps:
				attrs['default_rendered'] = True
				attrs['default_value'] = self.getdate[attrs['data_type']]
			else:
				for event, action in trigger_actions.items():
					self.trigger_actions.setdefault(event, list())
					self.trigger_actions[event] += action
				for event, template in trigger_templates.items():
					self.trigger_templates[event] = template
				self.triggers += triggers

		if 'default_value' in attrs:
			if not attrs.get('default_rendered'):
				attrs['default_value'] = _type_render(attrs['default_value'])
			res += self.render_default()

		if 'is_nullable' in attrs:
			if not attrs['is_nullable']:
				res += ' not'
			res += ' null'

		if 'set_on_update' in attrs and name not in entity['primary_key']:
			trigger_actions = dict()
			trigger_templates = dict()
			triggers = list()

			other_fields = ', '.join([
				'%s' % self.render_name(field) \
				for field in fields if field != name
			])
			where_pk = ' and '.join([
				'%s.%s=%s' % (
					self.render_name('new'), 
					self.render_name(field), 
					self.render_name(field)
				) \
				for field in entity['primary_key']
			])

			params.update(
				where_pk=where_pk, 
				other_fields=other_fields, 
			)
			if getattr(self, 'trigger_field_action_before', None):
				event = ('before', 'update')
				trigger_actions.setdefault(event, list())
				trigger_actions[event].append(
					self.trigger_field_action_before['update'] % params)
			if getattr(self, 'trigger_field_action_after', None):
				event = ('after', 'update')
				trigger_actions.setdefault(event, list())
				trigger_actions[event].append(
					self.trigger_field_action_after % params)
			if getattr(self, 'on_update_trigger_template', None):
				stage, text = self.on_update_trigger_template
				trigger_templates[(stage, 'update')] = text % params
			if getattr(self, 'on_update_trigger', None):
				triggers.append(self.on_update_trigger % params)
			if getattr(self, 'onupdate_inline', '') and self.inline_timestamps:
				res += self.onupdate_inline % params
			else:
				for event, action in trigger_actions.items():
					self.trigger_actions.setdefault(event, list())
					self.trigger_actions[event] += action
				for event, template in trigger_templates.items():
					self.trigger_templates[event] = template
				self.triggers += triggers

		if res_autoincrement:
			res += u' %s' % res_autoincrement

		return prefix + res % attrs

	def get_entity(self, name, prefix='\t', with_fk=True):

		self.this_render_pk = self.render_pk
		self.triggers = list()
		self.trigger_actions = dict()
		self.trigger_templates = dict()
		self.check_constraints = list()

		entity = super(SQLSchema, self).get_entity(name)

		fields = [
			self.render_field(name, attrs, entity, prefix=prefix) \
			for name, attrs in entity['fields'].items()
		]
		constraints = [
			u'%s %s' % (self.render_name('check%d' % c), check) \
			for c, check in enumerate(self.check_constraints)
		]
		if self.this_render_pk:
			constraints.append(
				'%s primary key (%s)' % (
					self.render_name('pk_%s' % entity['table']), 
					', '.join([
						'%s' % self.render_name(column) \
						for column in entity['primary_key']
					])
				)
			)
		for c, unique in enumerate(entity['unique_constraints']):
			name, columns = unique
			if name is None:
				name = '%s_unique%d' % (entity['table'], c)
			constraints.append(
				'%s unique (%s)' % (
					self.render_name(name), 
					', '.join([
						'%s' % self.render_name(column) for column in columns
					])
				)
			)
		for name, other, ours, remotes, extra in entity['belongs_to']:
			other_entity = self.entities.get(other)
			other_table = other_entity.get('table')
			if not other_table:
				print('foreign key %s references inexistent teble %s' % (
					name, other), file=sys.stderr)
				continue

			constraint = '%s foreign key (%s) references %s (%s)' % (
					self.render_name(u'fk_%s_%s' % (entity['table'], name)), 
					', '.join(['%s' % self.render_name(our) for our in ours]), 
					self.render_name(other_table), 
					', '.join([
						'%s' % self.render_name(remote) for remote in remotes
					])
				)
			if not with_fk:
				continue
			if not self.inline_fk:
				self.fk_constraints.append(
					'ALTER TABLE %s ADD CONSTRAINT %s' % (
						self.render_name(entity['table']), constraint
					)
				)
			else:
				constraints.append(constraint)

		named_constraints = [
			'%sconstraint %s' % (prefix, constraint) \
			for constraint in constraints
		]

		create = [
			"create table %s (\n%s\n) %s" % (
				self.render_name(entity['table']), 
				',\n'.join(fields + named_constraints), 
				self.table_sufix
			)
		]

		trigger_format = getattr(self, 'trigger_format', '%s')
		c = 0
		for key, trigger in self.trigger_templates.items():
			content = self.trigger_actions.get(key, list())
			trigger = trigger % dict(c=c, content='\n'.join(content))
			trigger = trigger_format % trigger
			create.append(trigger)

		for c1, trigger in enumerate(self.triggers):
			trigger = trigger % dict(c=c+c1)
			trigger = trigger_format % trigger
			create.append(trigger)

		return create


	def ddl_list(self, prefix='\t', only_tables=None, with_fk=True):

		self.fk_constraints = list()

		if only_tables:
			with_fk=False
		entities = [self.prelude]
		for name in self.entities.keys():
			if only_tables and name not in only_tables:
				continue
			entities += self.get_entity(name, prefix=prefix, with_fk=with_fk)
		entities += self.fk_constraints + [self.postfix]

		return entities


	def ddl(self, pm_location, prefix='\t', only_tables=None, with_fk=True):
		entities = self.ddl_list(
			prefix=prefix, only_tables=only_tables, with_fk=with_fk)

		return self.script_combine(entities)


	def script_combine(self, statements):
		return '\n'.join([
			u'%s%s' % (statement, self.default_delimiter) \
			for statement in statements if statement.strip()
		])


	def db_executefile(self, filename):
		script = open(filename, 'r').read()
		return self.db_executescript(script)


	def db_executelist(self, statements):
		script = self.script_combine(statements)
		self.db_executescript(script)


	def load_ddl(
			self, pm_location, only_tables=None, with_fk=True, with_prefix=[]):
		super(SQLSchema, self).load_ddl(
			pm_location, only_tables=only_tables, with_fk=with_fk)
		ddl = self.ddl_list(with_fk=with_fk, only_tables=only_tables)
		self.db_executelist(with_prefix + ddl)
		#schema.db_commit()

