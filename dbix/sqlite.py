
from .sqlschema import SQLSchema, SQLResultSet

import sqlite3
import os
from datetime import datetime


class SQLITEResultSet(SQLResultSet):

	def perform_insert(self, script, param, pk_fields, table, new_key):

		self.schema.db_execute(script, param)
		if new_key:
			return new_key
		script = u'select %sfrom %s\nwhere rowid=last_insert_rowid()' % (
			u','. join ([
				self.schema.render_name(field) for field in pk_fields
			]), 
			self.schema.render_name(table)
		)
		res = self.schema.db_execute(script)

		return res.fetchone()


class SQLITE(SQLSchema):

	rs_class = SQLITEResultSet

	_type_conv = dict(
		enum='varchar', 
		boolean='integer', 
		datetime='timestamp', 
		tinyint='integer', 
		mediumtext='text', 
	)

	prelude = """
	PRAGMA recursive_triggers=1;
	"""
	postfix = """
	PRAGMA foreign_keys = ON;
	"""
	query_prefix = """
	--PRAGMA recursive_triggers=1;
	"""

	getdate = dict(
		timestamp="strftime('%Y-%m-%d %H:%M:%f', 'now')",
		date="strftime('%Y-%m-%d', 'now')",
		time="strftime('%H:%M:%S.%f', 'now')",
	)

	deferred_fk = "DEFERRABLE INITIALLY DEFERRED"

	on_update_trigger = """
		create trigger [tr_%(table)s%%(c)d] 
		after update 
		of %(other_fields)s 
		on [%(table)s] for each row 
--		when ([new].[%(field)s]=[old].[%(field)s])
		begin
			update [%(table)s] 
			set [%(field)s]=%(getdate_tr)s
			where %(where_pk)s;
		end;
		"""

	dbsuffix = '.sqlite'
	path = None

	def __init__(self, **kw):
		super(SQLITE, self).__init__()
		self.type_render['integer primary key autoincrement'] = \
			self.type_render['integer']
		self.dbsuffixlen = len(self.dbsuffix)

		path = kw.get('path')
		if os.path.exists(path):
			self.path = os.path.abspath(path)

	def render_autoincrement(self, attrs, entity, name):
		attrs, _ = super(SQLITE, self).render_autoincrement(attrs, entity, name)
		if attrs.get('is_auto_increment'):
			attrs['data_type'] = 'integer primary key autoincrement'
			self.this_render_pk = False
		return attrs, ''

	def fk_disable(self):
		return ["PRAGMA foreign_keys = OFF"]

	def fk_enable(self):
		return ["PRAGMA foreign_keys = ON"]

	def db_filename(self, dbname):
		if dbname == ':memory:':
			return dbname
		return os.path.join(self.path, dbname + self.dbsuffix)

	def isdba(self, **kw):
		if self.dbname == ':memory:':
			return True
		return self.path and os.access(self.path, os.W_OK)

	def db_create(self, dbname):
		if dbname == ':memory:':
			return True
		path = self.db_filename(dbname)
		if os.path.exists(path):
			return False
		open(path, 'w').write('')
		return os.path.exists(path)

	def db_drop(self, dbname):
		if not self.isdba():
			return
		if dbname == self.dbname:
			self.db_disconnect()
		if dbname == ':memory:':
			return True
		path = self.db_filename(dbname)
		if os.path.exists(path):
			os.remove(path)
		return not os.path.exists(path)

	def db_connect(self, dbname):
		try:
			path = self.db_filename(dbname)
			self.connection = sqlite3.connect(
				path, 
				detect_types=sqlite3.PARSE_DECLTYPES, 
			)
			self.dbname = dbname
			return True
		except:
			self.connection = None
			self.dbname = None
			return False

	def db_disconnect(self):
		if not self.connection:
			return
		self.connection.close()
		self.connection = None
		self.dbname = None

	def db_commit(self):
		if not self.connection:
			return
		self.connection.commit()

	def db_rollback(self):
		if not self.connection:
			return
		self.connection.rollback()

	def db_name(self):
		return self.dbname

	def db_list(self):
		if self.dbname == ':memory:':
			return [self.dbname]
		return [
			db[:-self.dbsuffixlen] for db in os.listdir(self.path) \
			if db.endswith(self.dbsuffix)
		]

	def db_execute(self, script, param=list()):
		if not self.connection:
			return
		cur = self.db_cursor()
		cur.executescript(self.query_prefix)
		cur.execute(script, param)
		return cur

	def db_executemany(self, script, param=list()):
		if not self.connection:
			return
		cur = self.db_cursor()
		cur.executescript(self.query_prefix)
		cur.executemany(script, param)
		return cur

	def db_executescript(self, script):
		if not self.connection:
			return
		cur = self.db_cursor()
		cur.executescript(self.query_prefix + ';' + script)
		return cur

	def db_now(self):
		snow = super(SQLITE, self).db_now()
		return datetime.strptime(snow, '%Y-%m-%d %H:%M:%S.%f')


	def db_cursor(self):
		if not self.connection:
			return
		return self.connection.cursor()

