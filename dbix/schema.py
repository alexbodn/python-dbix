
from __future__ import print_function

from collections import OrderedDict
import sys
import json
from datetime import datetime

from .perlconv import schemaconv


class ResultSet(object):

	filter = [], {}


	def __init__(self, schema, name, select_columns=None, dict_record=False):
		self.schema = schema
		self.entity = schema.entities[name]
		if hasattr(schema, 'data'):
			self.data = schema.data[name]
		self.set_columns(select_columns)
		self.dict_record = dict_record


	def set_columns(self, select_columns):
		self.select_columns = [
			column for column in select_columns \
			if column in self.entity['fields']
		] if select_columns else self.entity['fields'].keys()
		field_names = list(self.entity['fields'].keys())
		self.select_columns_ix = [
			field_names.index(column) for column in self.select_columns
		]


	def query_operator(self, left, op, right):
		query_operators = {
			'=': lambda l, r: l == r,
			'!=': lambda l, r: l != r,
			'<>': lambda l, r: l != r,
			'<': lambda l, r: l < r,
			'<=': lambda l, r: l <= r,
			'>': lambda l, r: l > r,
			'>=': lambda l, r: l >= r,
			'~': lambda l, r: 0, # re.match
			'in': lambda l, r: l in r,
		}
		return op in query_operators and query_operators[op](left, right)


	def update_operator(self, left, op, right):
		update_operators = {
			'=': lambda l, r: r,
			':': lambda l, r: r,
			'+': lambda l, r: l + r,
			'||': lambda l, r: u'%s%s' % (l, r),
			'-': lambda l, r: l - r,
			'*': lambda l, r: l * r,
			'/': lambda l, r: l / r,
		}
		if len(op) == 2 and op[1] == '=':
			op = op[0]
		if op in update_operators:
			return update_operators[op](left, right)
		return left


	def operator_has_list(self, op):
		return op.lower() in ['in']


	def fields_sanitize(self, with_op, kw):
		fields = list()
		values = list()
		for key, value in kw.items():
			if key not in self.entity['fields']:
				continue
			if with_op and not isinstance(value, (list, tuple)):
				value = ['=', value]
			if isinstance(value, (list, tuple)):
				if not with_op or len(value) != 2:
					continue
			fields.append(self.schema.render_name(key))
			#fields.append(key) #################
			values.append(value)

		return fields, values


	def field_init(self, field, value, pk=None):
		entityfield = self.entity['fields'][field]
		data_type = entityfield['data_type']
		auto_increment = False
		if value:
			return value, False, False
		elif 'default_value' in entityfield:
			value = entityfield['default_value']
		elif entityfield.get('set_on_create'):
			value = datetime.utcnow()
			if data_type in ('date', 'time'):
				value = getattr(value, data_type)()
		auto_increment = \
			pk and entityfield.get('is_auto_increment') and len(pk) == 1
		nullable = entityfield.get('is_nullable', 1)

		return value, nullable, auto_increment


	def primary_key(self, kw):
		key = list()
		auto_increment = False
		pk = self.entity['primary_key']
		for field in pk:
			value, nullable, auto_increment = self.field_init(
				field, kw.get(field), pk=pk)
			if value is None and not auto_increment:
				raise Exception(
					'missing value for field %s in primary key of %s' % (
						field, self.entity['table']
					)
				)
			key.append(value)
		return key, auto_increment


	def create(self, **kw):
		key, auto_increment = self.primary_key(kw)
		record = list()
		for field in self.entity['fields']:
			value, nullable, auto_increment = self.field_init(
				field, kw.get(field))
			if value is None and not nullable:
				raise Exception(
					'missing value for field %s in %s' % (
						field, self.entity['table']
					)
				)
			record.append(value)
		pk = self.entity['primary_key']
		if auto_increment:
			kw[pk[0]] = (self.data.keys() or [0])[-1] + 1
			key = [kw[pk[0]]]
		key = tuple(key) if len(pk) > 1 else key[0]
		if key in self.data:
			print(
				'key %s already in %s' % (repr(key), self.entity['table']),
				file=sys.stderr
			)
			return
		self.data[key] = record

		try:
			return self.find(*[key]).__iter__().next()
		except:
			return self.find(*[key]).__iter__().__next__()


	def do_filter(self, filter=None):
		args, kw = (filter or self.filter)
		keys = args or self.data.keys()
		for key in keys:
			record = self.data[key]
			for c, field in enumerate(self.entity['fields']):
				if field not in kw:
					continue
				op = ['=', kw[field]]
				if isinstance(kw[field], (list, tuple)) and len(kw[field]) == 2:
					op = list(kw[field])
				if not self.query_operator(record[c], *op):
					break
			else:
				yield(key)


	def update(self, **kw):

		fields, values = self.fields_sanitize(True, kw)
		kw = dict(zip(fields, values))
		for key in self.do_filter():
			for c, field in enumerate(self.entity['fields']):
				entityfield = self.entity['fields'][field]
				if field not in kw and entityfield.get('set_on_update'):
					data_type = entityfield['data_type']
					kw[field] = datetime.utcnow()
					if data_type in ('date', 'time'):
						kw[field] = getattr(kw[field], data_type)()
				if field in kw:
					value = kw[field]
					op = ['=', value]
					if isinstance(value, (list, tuple)) and len(value) == 2:
						op = list(value)
					self.data[key][c] = self.update_operator(
						self.data[key][c], *op)


	def find(self, *args, **kw):
		self.filter = args, kw
		return self


	def __iter__(self):
		self.iterator = self.do_filter()
		return self


	def do_next(self, select):
		if self.dict_record:
			return dict(zip(self.select_columns, select))
		return select


	def do_next_key(self, key):
		record = self.data[key]
		select = tuple([record[c] for c in self.select_columns_ix])
		return self.do_next(select)


	def next(self):
		key = self.iterator.next()
		return self.do_next_key(key)


	def __next__(self):
		key = self.iterator.__next__()
		return self.do_next_key(key)


	def get_description(self):
		return [(field, ) for field in self.select_columns]

	description = property(get_description, )


class Schema(object):

	rs_class = ResultSet

	dump_extension = '.json'


	@staticmethod
	def isfalse(x):
		false = set([False, 0, 'false', 'no', 'not', '0'])
		res = 0
		try:
			res = x in false
		except:
			try:
				res = x.lower() in false
			except:
				pass
		return int(res)

	type_converter = dict(
		integer=lambda x: int(x), 
		tinyint=lambda x: int(x), 
		smallint=lambda x: int(x), 
		enum=lambda x: x, 
		float=lambda x: float(x), 
		decimal=lambda x: Decimal(x), 
		numeric=lambda x: Decimal(x), 
		char=lambda x: x, 
		varchar=lambda x: x, 
		text=lambda x: x, 
		mediumtext=lambda x: x, 
		date=lambda x: datetime.strptime(x, '%Y-%m-%d').date(), 
		time=lambda x: datetime.strptime(x, '%H:%M:%f').time(), 
		datetime=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%f'), 
		boolean=lambda x: Schema.isfalse(x) or 1, 
	)

	field_attr = dict(
		data_type0=lambda c, x: Schema.type_converter.get(x, (None,))[0],
		data_type=lambda c, x: x,
		is_nullable=lambda c, x: int(x),
		default_value=lambda c, x: c(x),
		size=lambda c, x: int(x),
		is_auto_increment=lambda c, x: int(x),
		set_on_create=lambda c, x: int(x),
		set_on_update=lambda c, x: int(x),
	)


	def new_entity(self, name):
		entity = dict(
			table = None, 
			fields = OrderedDict(), 
			primary_key = list(), 
			unique_constraints = list(), 
			belongs_to = list(), 
			has_many = list(), 
			components = list(), 
			parent_column = None, 
		)
		self.data.setdefault(name, OrderedDict())
		self.entities.setdefault(name, entity)


	def __init__(self):
		self.init()


	def init(self):
		self.data = dict()
		self.dbname = 'self'
		self.entities = OrderedDict()
		return True


	def render_name(self, name):
		return name


	def get_entity(self, entityname, attr_conv=dict(), only_in_conv=False):

		if entityname not in self.entities.keys():
			return dict()
		entity = self.entities[entityname]
		entity['fields'] = dict([
			(name, dict([
				(
					attr_conv.get(key,(0,0))[0] if attr_conv.get(key,(0,0))[0] else key, 
					attr_conv.get(key,(0,0))[1](0, val) if attr_conv.get(key,(0,0))[1] else val
				) \
				for (key, val) in entity['fields'][name].items() \
				if key in attr_conv or not only_in_conv
			])) for (name, attrs) in entity['fields'].items()
		])

		if 'components' in entity:
			del entity['components']

		return entity


	def load_ddl(self, pm_location, only_tables=None, with_fk=True):
		schemaconv(pm_location, self, with_dict=True)


	def ddl(self, pm_location, prefix='\t', only_tables=None, with_fk=True):
		self.load_ddl(pm_location, only_tables=only_tables, with_fk=with_fk)

		return json.dumps(self.entities, indent=4)


	def db_create(self, dbname):
		return True


	def db_drop(self, dbname):
		if dbname == self.dbname:
			self.init()
		return True


	def db_connect(self, dbname):
		self.dbname = dbname
		return True


	def db_disconnect(self):
		self.init()


	def db_commit(self):
		pass


	def db_rollback(self):
		pass


	def db_name(self):
		return self.dbname


	def db_list(self):
		return [self.dbname]


	def resultset(self, name, select_columns=None, dict_record=False):
		return self.rs_class(
			self, name, select_columns=select_columns, dict_record=dict_record)

