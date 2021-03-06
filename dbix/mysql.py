
from .sqlschema import SQLSchema, SQLResultSet

import MySQLdb as mysqldb
#import mysql.connector as mysqldb


class MYSQLResultSet(SQLResultSet):

	def __enter__(self):
		ret = super(MYSQLResultSet, self).__enter__()
		if not self.schema.fk_enabled_status:
			self.schema.fk_disable()
		self.schema.fk_enabled_status += 1
		return ret

	def __exit__(self, exc_type, exc_value, traceback):
		self.schema.fk_enabled_status -= 1
		if not self.schema.fk_enabled_status:
			self.schema.fk_enable()
		return super(MYSQLResultSet, self).__exit__(
			exc_type, exc_value, traceback)

	def perform_insert(self, script, param, pk_fields, table, new_key):
		self.schema.db_execute(script, param)
		if new_key:
			return new_key
		script = u'select %s=last_insert_id()' % \
			self.schema.render_name(pk_fields[0])
		res = self.schema.db_execute(script)
		return res.fetchone()


class MYSQL(SQLSchema):

	engine = 'InnoDB'
#	engine = 'MyISAM'
	inline_fk = engine != 'InnoDB'

	rs_class = MYSQLResultSet

	inline_domains = True
	inline_timestamps = False

	_type_conv = {
		'datetime': 'timestamp', 
		#'datetime': 'timestamp(6)', 
	}

	getdate = {
		'timestamp': "now()",
		'timestamp(6)': "now(6)",
		'datetime': "now()",
		'datetime(6)': "now(6)",
		'date': "now()",
		'time': "now()",
	}

	render_paramplace = '%s'

	oncreate_inline = " DEFAULT %(getdate)s"

	on_create_trigger_template = (
		'before', 
		"""
		create trigger `tr_%(table)s%%(c)d_insert` 
		before insert 
		on `%(table)s` for each row 
		begin
			%%(content)s
		end; 
		"""
	)

	onupdate_inline = " ON UPDATE %(getdate)s"

	on_update_trigger_template = (
		'before', 
		"""
		create trigger `tr_%(table)s%%(c)d_update` 
		before update 
		on `%(table)s` for each row 
		begin
			%%(content)s
		end; 
		"""
	)

	trigger_field_action_before = dict(
		update="""
		if `new`.`%(field)s`=`old`.`%(field)s` then
			set `new`.`%(field)s`=%(getdate_tr)s;
		end if;
		""",
		insert="""
		if `new`.`%(field)s` is null then
			set `new`.`%(field)s`=%(getdate_tr)s;
		end if;
		""",
	)

	_trigger_format = """
		delimiter $$
		%s
		$$
		delimiter ;
		"""

	auto_increment = " AUTO_INCREMENT"

	table_sufix = "ENGINE=%s" % engine

	fk_enabled_status = 0


	@staticmethod
	def render_number(value):
		return u"'%s'" % value

	@staticmethod
	def render_concat(left, right):
		return 'concat(%s, %s)' % (left, right)

	def render_default(self):
		return ' DEFAULT %(default_value)s'

	def render_name(self, name):
		return '`%s`' % name

	def render_autoincrement(self, attrs, entity, name):
		attrs, __ = super(MYSQL, self).render_autoincrement(
			attrs, entity, name)
		if attrs.get('is_auto_increment'):
			return attrs, 'AUTO_INCREMENT'
		return attrs, ''

	def render_unique_column(self, name, entity):
		column = super(MYSQL, self).render_unique_column(name, entity)
		attrs = entity['fields'][name]
		if attrs['data_type'] in ('text', 'blob') and 'size' not in attrs:
			column += '(1024)'
		return column

	def fk_disable(self):
		self.db_execute("SET FOREIGN_KEY_CHECKS=0")

	def fk_enable(self):
		self.db_execute("SET FOREIGN_KEY_CHECKS=1")

	def type_conv(self, attrs, entity, name):
		data_type = super(MYSQL, self).type_conv(attrs, entity, name)
		timestamp_attrs = ('set_on_create', 'set_on_update')
		if 1 in [attrs.get(parm, 0) for parm in timestamp_attrs]:
			if 'hastimestamps' in entity:
				if data_type == 'timestamp':
					data_type = 'datetime'
				elif data_type == 'timestamp(6)':
					data_type = 'datetime(6)'
				self.inline_timestamps = False
			else:
				entity['hastimestamps'] = 1
				self.inline_timestamps = True
		return data_type

	def __init__(self, **connectparams):
		super(MYSQL, self).__init__()
		self.type_render['timestamp(6)'] = self.type_render['timestamp']
		self.type_render['datetime(6)'] = self.type_render['datetime']

		self.connectparams_user = dict(
			host=connectparams.get('host', 'localhost'),
			user=connectparams.get('user'),
			passwd=connectparams.get('password'),
		)
		self.connectparams_dba = dict(
			host=connectparams.get('host', 'localhost'),
			db='mysql',
			user=connectparams.get('user_dba'),
			passwd=connectparams.get('password_dba'),
		)

	def isdba(self):
		return self.connectparams_dba.get('user') \
			and self.connectparams_dba.get('passwd')

	def db_create(self, dbname):
		if not self.isdba():
			return
		conn = mysqldb.connect(**self.connectparams_dba)
		cur = conn.cursor()
		connectparams = dict(
			db=dbname,
			user=self.connectparams_user.get('user'),
		)
		cur.execute(
			"""
			CREATE DATABASE %(db)s;
			""" % connectparams
		)
		try:
			cur.execute(
				"""
				SET GLOBAL log_bin_trust_function_creators=1;
			"""
			)
		except:
			pass
		conn.commit()
		cur.execute(
			"""
			GRANT ALL PRIVILEGES ON `%(db)s`.* TO '%(user)s';
			""" % connectparams
		)
		conn.commit()
		conn.close()
		dbs = self.db_list()
		return dbs and dbname in dbs

	def db_drop(self, dbname):
		if not self.isdba():
			return
		dbs = self.db_list()
		if dbs and dbname not in dbs:
			return True
		if dbname == self.dbname:
			self.db_disconnect()
		conn = mysqldb.connect(**self.connectparams_dba)
		cur = conn.cursor()
		cur.execute("DROP DATABASE %(db)s;" % dict(db=dbname))
		conn.commit()
		conn.close()
		dbs = self.db_list()
		return dbs and dbname not in dbs

	def db_connect(self, dbname):
		try:
			connectparams = dict(db=dbname)
			connectparams.update(self.connectparams_user)
			self.connection = mysqldb.connect(**connectparams)
			self.connection.autocommit(False)
			self.dbname = dbname
			return True
		except:
			self.db_reset()
			return False

	def db_disconnect(self):
		if not self.connection:
			return
		self.connection.close()
		self.db_reset()

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
		try:
			conn = mysqldb.connect(**self.connectparams_dba)
			cur = conn.cursor()
			cur.execute("SHOW DATABASES;")
			res = [row[0] for row in cur.fetchall()]
			if not self.connection:
				conn.close()
			return res
		except:
		    return None

	def db_execute(self, script, param=list()):
		self.pre_execute(script, param)
		cur = self.db_cursor()
		if self.query_prefix:
			script = self.query_prefix + script
		cur.execute(script, param)
		return cur

	def db_executemany(self, script, param=list()):
		cur = self.db_cursor()
		if self.query_prefix:
			script = self.query_prefix + script
		cur.executemany(script, param)
		return cur


	def db_executescript(self, script):
		self.db_execute(script)
		return self.db_execute("select 0=1;")


	def db_executelist(self, statements):
		cur = self.db_cursor()
		for script in statements:
			script = script.strip()
			if not script or script == self.default_delimiter:
				continue
			if self.query_prefix:
				cur.execute(self.query_prefix)
			print(script)
			cur.execute(script)
		return cur

