
from __future__ import print_function
from future.utils import with_metaclass

import sys, os

from .perlconv import treeconv


class BuilderMetaClass(type):

	@property
	def load_namespaces(self):
		if not self.__load_namespaces__:
			return
		pytext = treeconv(
			os.path.dirname(self.__sourcepath__), 
			self.__sourcepath__, 
			with_dict=True)
		exec(pytext, dict(schema=self.schema))
		#print(pytext, file=sys.stdout)

class BuilderMixin(with_metaclass(BuilderMetaClass, object)):

	@classmethod
	def register(cls):

		cls.schema.new_entity(cls.__name__)

	@classmethod
	def table(cls, name):
		cls.register()
		cls.schema.entities[cls.__name__]['table'] = name

	@classmethod
	def add_columns(cls, **columns):

		cls.register()
		for column, attrs in columns.items():

			if 'data_type' not in attrs:
				print(
					'missing data type for %s' % cls.__name__, file=sys.stderr)
			data_type = attrs.get('data_type')
			if data_type not in cls.schema.type_converter:
				print(
					'unrecognized data type %s %s' % (cls.__name__, data_type), 
					file=sys.stderr)
			converter = cls.schema.type_converter.get(data_type)

			kwargs = dict()
			for key in attrs:
				arg = attrs[key]
				if converter and cls.schema.field_attr.get(key):
					arg = cls.schema.field_attr[key](converter, arg)
				kwargs[key] = arg

			cls.schema.entities[cls.__name__]['fields'][column] = kwargs

	@classmethod
	def set_primary_key(cls, *columns):
		cls.register()
		cls.schema.entities[cls.__name__]['primary_key'] = columns

	@classmethod
	def add_unique_constraint(cls, *columns):
		cls.register()
		if len(columns) != 2 or type(columns[1]) != list:
			columns = (None, columns[0])
		cls.schema.entities[cls.__name__]['unique_constraints'].append(columns)

	@classmethod
	def belongs_to(cls, name, other, field, extra=dict()):
		cls.register()
		ours = list()
		remotes = list()
		if type(field) is dict:
			for k, v in field.items():
				ours.append(v.split('.')[-1])
				remotes.append(k.split('.')[-1])
		else:
			ours = [field]
			remotes = [field]
		cls.schema.entities[cls.__name__]['belongs_to'].append(
			(name, other, ours, remotes, extra))

	@classmethod
	def has_many(cls, name, other, field=None):
		cls.register()
		cls.schema.entities[cls.__name__]['has_many'].append(
			(name, other, field))

	@classmethod
	def load_components(cls, *args):
		cls.register()
		cls.schema.entities[cls.__name__]['components'] = args

	@classmethod
	def parent_column(cls, name):
		cls.register()
		cls.schema.entities[cls.__name__]['parent_column'] = name

	@classmethod
	# it is assumed that the schema creator has checked it in the perl code
	def schema_sanity_checker(cls, *args):
		return True

